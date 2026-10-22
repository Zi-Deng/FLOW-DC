"""Frozen adaptive-150 design. Public-host and controlled evidence stay separate."""

import itertools
import math
import random
import statistics
from pathlib import Path

from .common import OPTIONS, file_hash, read_json, require, source_identity, write_new
from .design import activity_intervals, weighted_quantile
from .onset import Policy as OnsetPolicy

PROFILE = "bounded-adaptive-ablation-v1"
METHODS = ("ratio-v2", "gradient-candidate-v2")
VARIANTS = (
    "full",
    "no-startup",
    "short-refresh",
    "no-utilization-gate",
    "no-elapsed-smoothing",
    "no-method-feedback",
)
ROWS = 32768
SEED = 20261021
LIMITS = dict(
    wall_seconds=28800,
    max_windows=8,
    window_seconds=3600,
    budget_su=300,
    max_requests=1200000,
    body_bytes=200 * 2**30,
    artifact_bytes=256 * 2**30,
    free_bytes=40 * 2**30,
    metadata_bytes=4 * 2**30,
    audit_seconds=1200,
    collection_seconds=900,
    max_cooldowns=3,
    volume_gib=384,
)
PLANNED = ("planned_window_end", "planned_body_budget")


def terminal_fault(event):
    """Refusal evidence has priority even after a planned window stop."""
    if event.get("event") == "headers" and event.get("status") in (401, 403, 429, 503):
        return {
            401: "access_refusal",
            403: "access_refusal",
            429: "explicit_429",
            503: "service_unavailable",
        }[event["status"]]
    return (event.get("fatal_reason") or event.get("reason") or "fatal_stop") if event.get("fatal") else None


def recovered(value, baseline):
    return (
        value.get("passed") is True
        and value.get("reason") is None
        and value.get("requests") == 128
        and value.get("valid", 0) >= 0.95 * 128
        and value.get("ttfb_p50_s") is not None
        and value["ttfb_p50_s"] <= 1.5 * baseline
    )


def grid():
    return [
        dict(startup_growth_factor=a, baseline_max_age_s=b, target_delay_ratio=c)
        for a, b, c in itertools.product((1.25, 1.5), (10.0, 120.0), (2.0, 3.0))
    ]


def schedule():
    rng = random.Random(SEED)
    runs = [
        dict(
            run_id=f"capacity-{n + 1}",
            phase="capacity",
            method="fixed-v1",
            arm=f"capacity-{cap}",
            cap=cap,
            workers=cap,
            block=0,
            slice="capacity",
            network_seconds=180,
            budget_seconds=600,
        )
        for n, cap in enumerate((256, 512))
    ]
    for block in range(2):
        cells = [(m, i) for m in METHODS for i in range(8) if bin(i).count("1") % 2 == block]
        rng.shuffle(cells)
        for m, i in cells:
            runs.append(
                dict(
                    run_id=f"screen-{m}-{i}",
                    phase="screen",
                    method=m,
                    arm=f"{m}-{i}",
                    grid=i,
                    block=block,
                    slice=f"tuning-{i}",
                    network_seconds=120,
                    budget_seconds=420,
                )
            )
    arms = (
        "ratio-v1",
        "gradient-candidate-v1",
        "balanced-ratio-v2",
        "balanced-gradient-candidate-v2",
        "high-ratio-v2",
        "high-gradient-candidate-v2",
    )
    for block in range(3):
        order = list(arms)
        rng.shuffle(order)
        for arm in order:
            method = arm.removeprefix("balanced-").removeprefix("high-")
            runs.append(
                dict(
                    run_id=f"validation-{block + 1}-{arm}",
                    phase="validation",
                    method=method,
                    arm=arm,
                    block=block + 1,
                    slice=f"validation-{block + 1}",
                    network_seconds=180,
                    budget_seconds=600,
                )
            )
    return runs


def prepare(study, source, previous):
    import polars as pl

    study, source, previous = map(Path, (study, source, previous))
    require(not study.exists(), "study exists; preserve attempts")
    original = read_json(previous / "population-record.json")
    require(file_hash(previous / "population.parquet") == original["population_sha256"], "population changed")
    frame = pl.read_parquet(previous / "population.parquet").sort("url")
    require(frame["url"].n_unique() == frame.height, "duplicate population URLs")
    names = ["capacity", *[f"tuning-{n}" for n in range(8)], *[f"validation-{n + 1}" for n in range(3)]]
    needed = len(names) * ROWS + 300 + 3 * 128
    require(frame.height >= needed, "insufficient disjoint allocation")
    indices = random.Random(SEED).sample(range(frame.height), needed)
    study.mkdir(parents=True, mode=0o700)
    (study / "slices").mkdir()
    for n, name in enumerate(names):
        frame[indices[n * ROWS : (n + 1) * ROWS]].write_parquet(study / "slices" / f"{name}.parquet")
    at = len(names) * ROWS
    frame[indices[at : at + 300]].write_parquet(study / "probe.parquet")
    for n in range(3):
        frame[indices[at + 300 + n * 128 : at + 300 + (n + 1) * 128]].write_parquet(
            study / f"recovery-{n}.parquet"
        )
    write_new(study / "schedule.json", {"seed": SEED, "runs": schedule()})
    files = [p for p in study.rglob("*") if p.is_file()]
    write_new(
        study / "protocol.json",
        dict(
            schema="flowdc-adaptive-ablation-v1",
            source=source_identity(source),
            limits=LIMITS,
            rows=ROWS,
            grid=grid(),
            population=original,
            reuse="Methods share each tuning-grid manifest; different grids use disjoint slices. Validation arms share their block manifest; blocks are disjoint. Capacity, tuning and validation populations are disjoint. No cache busting.",
            files={str(p.relative_to(study)): file_hash(p) for p in files},
        ),
    )
    return read_json(study / "protocol.json")


def execution_source(study):
    path = Path(study) / "lifecycle-source-amendment.json"
    return read_json(path)["source"] if path.exists() else read_json(Path(study) / "protocol.json")["source"]


def check_lifecycle_source(study, protocol, actual):
    path = Path(study) / "lifecycle-source-amendment.json"
    if not path.exists():
        require(protocol["source"] == actual, "frozen source changed")
        return
    amendment = read_json(path)
    require(
        amendment["schema"] == "flowdc-lifecycle-source-amendment-v1"
        and amendment["parent_protocol_sha256"] == file_hash(Path(study) / "protocol.json")
        and amendment["parent_source"] == protocol["source"]
        and amendment["source"] == actual,
        "lifecycle source amendment changed",
    )
    allowed = {
        "bin/download_batch.py",
        "benchmark/real_server_study/ablation.py",
        "benchmark/real_server_study/ablation_analysis.py",
        "benchmark/real_server_study/ablation_collect.py",
        "benchmark/real_server_study/ablation_run.py",
        "benchmark/real_server_study/requirements.txt",
    }
    before, after = protocol["source"]["files"], actual["files"]
    require(
        set(before) == set(after) and all(before[k] == after[k] for k in before if k not in allowed),
        "lifecycle amendment changed control/measurement safeguards",
    )
    permitted = amendment["permitted_run_ids"]
    require(
        len(permitted) == 9
        and len(set(permitted)) == 9
        and set(permitted) <= {r["run_id"] for r in schedule() if r["phase"] == "validation"},
        "lifecycle continuation scope changed",
    )
    qualification = amendment["native_qualification"]
    require(
        qualification["status"] == "qualified"
        and qualification["public_requests"] == 0
        and qualification["credited_rows"] >= 20000
        and qualification["native_seconds"] <= 510
        and amendment["qualified_native_sha256"] == after["bin/download_batch.py"],
        "native lifecycle qualification missing or mismatched",
    )


def check(study, source):
    study = Path(study)
    p = read_json(study / "protocol.json")
    require(
        p["schema"] == "flowdc-adaptive-ablation-v1" and p["limits"] == LIMITS and p["grid"] == grid(),
        "study contract changed",
    )
    check_lifecycle_source(study, p, source_identity(source))
    for name, digest in p["files"].items():
        path = Path(name)
        require(
            not path.is_absolute() and ".." not in path.parts and not (study / path).is_symlink(),
            "unsafe input",
        )
        require(file_hash(study / path) == digest, "frozen input changed")
    require(read_json(study / "schedule.json")["runs"] == schedule(), "schedule changed")
    return p


def config(study, run):
    study = Path(study)
    m = run["method"]
    opts = dict(OPTIONS.get(m, {}))
    cap = run.get("cap")
    if m in METHODS:
        opts = (
            grid()[run["grid"]]
            if run["phase"] == "screen"
            else read_json(study / "selection.json")["choices"][run["arm"]]["options"]
        )
    return dict(
        input=str(study / "slices" / f"{run['slice']}.parquet"),
        input_format="parquet",
        url="url",
        label=None,
        output=str(study / "runs" / run["run_id"] / "native"),
        output_format="webdataset",
        concurrent_downloads=run.get("workers", 512),
        http_connection_limit=256,
        timeout=30,
        enable_paarc=True,
        control_method=m,
        method_options=opts,
        C_min=cap or 2,
        C_init=cap or 4,
        C_max=cap or 512,
        max_retry_attempts=1,
        research_profile=True,
        research_workload=PROFILE,
        create_tar=True,
        compress_tar=False,
        create_overview=True,
        force_overwrite=False,
    )


def exposure(events, phase, credits, baseline):
    starts = [e["monotonic_s"] for e in events if e["event"] == "dispatch"]
    if not starts:
        return dict(qualified=False, balanced=False, window_complete=False)
    origin = min(starts)
    lo, hi = (20, 60) if phase == "screen" else (30, 90)
    intervals = activity_intervals(events, origin, warmup=0)
    weights = []
    covered = dwell = longest = streak = 0.0
    for begin, end, active, backlog in intervals:
        length = max(0, min(end, origin + hi) - max(begin, origin + lo))
        if length <= 0:
            continue
        if backlog:
            covered += length
            weights.append((active, length))
        if backlog and active >= 150:
            dwell += length
            streak += length
            longest = max(longest, streak)
        else:
            streak = 0
    ends = [
        e for e in events if e["event"] == "network_end" and origin + lo < e["monotonic_s"] <= origin + hi
    ]
    delays = [e["ttfb_s"] for e in ends if e.get("ttfb_s") is not None]
    median = statistics.median(delays) if delays else None
    eligible = {e["token"] for e in ends if e.get("status") == 200}
    byte_count = sum(credits.get(url, 0) for url in eligible)
    warnings = []
    was = False
    for e in events:
        if e["event"] == "sample":
            if e.get("warning") and not was and e.get("cap") is not None:
                warnings.append(e)
            was = bool(e.get("warning"))
    responses = [
        any(
            e["event"] == "sample"
            and w["monotonic_s"] < e["monotonic_s"] <= w["monotonic_s"] + 2.2
            and e.get("cap") is not None
            and e["cap"] < w["cap"]
            for e in events
        )
        for w in warnings
    ]
    complete = covered >= hi - lo - 0.05
    return dict(
        warning_response="not_exercised" if not warnings else "passed" if all(responses) else "failed",
        window_complete=complete,
        qualified=complete and dwell >= 0.9 * (hi - lo) and longest >= 30,
        balanced=complete and median is not None and median <= 3 * baseline,
        active_median=weighted_quantile(weights, 0.5) if weights else None,
        active_p95=weighted_quantile(weights, 0.95) if weights else None,
        seconds_at_150=dwell,
        longest_at_150=longest,
        backlogged_seconds=covered,
        peak=max((e.get("inflight", 0) for e in events), default=0),
        window_goodput_Bps=byte_count / (hi - lo),
        median_ttfb_s=median,
        p95_ttfb_s=sorted(delays)[math.ceil(0.95 * len(delays)) - 1] if delays else None,
    )


def select(records):
    choices = {}
    for m in METHODS:
        for label in ("balanced", "high"):
            rows = [
                r
                for r in records
                if r["phase"] == "screen"
                and r["method"] == m
                and r["protocol_window_complete"]
                and not r.get("stop_reason")
                and r["exposure"].get("warning_response") != "failed"
                and r["exposure"].get("balanced" if label == "balanced" else "qualified")
                and r["verification"]["credited_rows"] / max(1, r["verification"]["dispatched_terminal_rows"])
                >= 0.99
            ]
            if not rows:
                continue
            best = max(r["exposure"]["window_goodput_Bps"] for r in rows)
            near = [r for r in rows if r["exposure"]["window_goodput_Bps"] >= 0.95 * best]
            winner = min(
                near, key=lambda r: (r["exposure"]["active_median"], r["exposure"]["p95_ttfb_s"], r["run_id"])
            )
            choices[f"{label}-{m}"] = dict(
                options=grid()[winner["grid"]], screen_run=winner["run_id"], criterion=label
            )
    return dict(
        choices=choices,
        screen_records={r["run_id"]: r["record_sha256"] for r in records if r["phase"] == "screen"},
    )


class Policy(OnsetPolicy):
    """3x warning, 5x rolling hard latency stop; explicit refusal remains immediate."""

    def __init__(self, baseline, rate=None):
        super().__init__(baseline, rate)
        self.initial_baseline = baseline
        self.warning = False

    def header(self, now, status, retry_after=None):
        super().header(now, status, retry_after)
        fault = terminal_fault(dict(event="headers", status=status))
        if fault:
            self.fatal = True
            self.fatal_reason = self.fatal_reason or fault
            self.reason = self.fatal_reason

    def tick(self, now):
        self.baseline = self.initial_baseline * 5 / 3
        super().tick(now)
        recent = [delay for at, delay in self.completed if now - 10 < at <= now]
        self.warning = len(recent) >= 20 and statistics.median(recent) >= 3 * self.initial_baseline
        self.baseline = self.initial_baseline
