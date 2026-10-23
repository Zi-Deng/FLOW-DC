"""Frozen calibration/transfer design; historical protocols remain immutable files."""

import math
import random
from pathlib import Path

import polars as pl

from .common import file_hash, read_json, require, source_identity, utc, write_new

METHODS = ("ratio-v2", "gradient-candidate-v2", "img2dataset-1.47.0")
SOURCES = ("openimages-cvdf", "inaturalist-open", "taibif-moth")
TARGETS = SOURCES[1:]
TOOL_SETTINGS = ((1, 32), (1, 128), (1, 256), (8, 32))
PROFILES = ((2.0, 1.5), (3.0, 1.5), (3.0, 2.0), (4.5, 2.0))
PROFILE = "bounded-public-transfer-v1"
SEED = 20261023
LIMITS = dict(
    wall_seconds=3 * 86400, dispatch_seconds=3 * 86400 - 14400,
    max_windows=72, window_seconds=14400, cleanup_seconds=600,
    budget_su=2400, max_requests=40000000, body_bytes=4 * 2**40,
    archive_bytes=3584 * 2**30, free_bytes=50 * 2**30,
    job_seconds=12600, native_seconds=10800, audit_seconds=1200,
    collection_seconds=900, metadata_bytes=4 * 2**30, local_peak_bytes=12 * 2**30,
    max_rows=524288, job_body_bytes=128 * 2**30,
    target_http_seconds=900, minimum_http_seconds=600, corrections=2,
)


def ceiling(source):
    require(source in SOURCES, "unknown public source")
    return 192 if source == "taibif-moth" else 256


def tool_settings(source):
    cap = ceiling(source)
    return [(p, min(t, cap // p)) for p, t in TOOL_SETTINGS]


def prepare(study, source, populations, provenance, initial_rows):
    study = Path(study)
    require(not study.exists(), "new study required; preserve old first attempts")
    require(set(populations) == set(provenance) == set(initial_rows) == set(SOURCES), "source inventory differs")
    study.mkdir(parents=True, mode=0o700)
    for name in ("pools", "gates", "runs", "slices", "analysis"):
        (study / name).mkdir()
    hashes, counts, partitions = {}, {}, {}
    for key in SOURCES:
        frame = pl.read_parquet(populations[key])
        columns = ["url", "record_id", "author", "license", "landing_url"]
        frame = frame.select(columns).cast({c: pl.String for c in columns})
        require(frame["url"].n_unique() == frame.height and not any(frame.null_count().row(0)), "invalid metadata population")
        from .public_guard import checked_url
        for url in frame["url"]:
            checked_url(url, key)
        # One third reserved exclusively for confirmation; repeated temporal blocks
        # use the same held-out manifest, not new URLs selected after outcomes.
        room = min(LIMITS["max_rows"], (frame.height - 32) // 3)
        require(type(initial_rows[key]) is int and 1 <= initial_rows[key] <= room, "initial size exceeds disjoint pool")
        frame = frame.sample(fraction=1, shuffle=True, seed=SEED)
        frame = frame.with_row_index("study_row_id").with_columns(
            (pl.lit(key + "-") + pl.col("study_row_id").cast(pl.String)).alias("study_row_id"))
        path = study / "pools" / (key + ".parquet")
        frame.write_parquet(path)
        hashes[key], counts[key] = file_hash(path), frame.height
        partitions[key] = dict(sizing=32, calibration=32 + room, evaluation=32 + 2 * room, capacity=room)
    record = dict(
        schema="flowdc-adaptive-transfer-study-v1", utc=utc(), seed=SEED,
        source=source_identity(source), limits=LIMITS, methods=METHODS,
        profiles=PROFILES, tool_settings={s: tool_settings(s) for s in SOURCES},
        population_sha256=hashes, population_rows=counts, partitions=partitions,
        provenance=provenance, initial_rows=initial_rows, blocks=6,
        timing="manager submission through independently verified returned artifacts",
        interpretation="real-source transfer case studies; no guaranteed speed or refusal avoidance")
    write_new(study / "protocol.json", record)
    return record


def manifest(study, source, phase, rows, block=0):
    study = Path(study)
    protocol = read_json(study / "protocol.json")
    part = protocol["partitions"][source]
    require(phase in ("sizing", "calibration", "evaluation") and 0 < rows <= part["capacity"], "invalid manifest partition")
    frame = pl.read_parquet(study / "pools" / (source + ".parquet")).slice(part[phase], rows)
    require(frame.height == rows, "source population exhausted")
    # Same content and ordering across methods in a block; independently seeded
    # URL order across temporal blocks, without changing the held-out population.
    return frame.sample(fraction=1, shuffle=True, seed=SEED + block) if phase == "evaluation" else frame


def configuration(directory, cell, frame):
    directory = Path(directory).resolve()
    require(cell["workers"] == 1 and cell["method"] in METHODS, "single-worker study required")
    cap = ceiling(cell["source"])
    if cell["method"] == METHODS[2]:
        setting = tuple(cell["tool_setting"])
        require(setting in tool_settings(cell["source"]), "unfrozen tool setting")
        processes, threads = setting
        return dict(
            url_list=str(directory / "input.parquet"), input_format="parquet", url_col="url",
            output_folder=str(directory / "native"), output_format="webdataset",
            processes_count=processes, thread_count=threads, timeout=30, retries=0,
            max_shard_retry=0, resize_mode="no", disable_all_reencoding=True,
            extract_exif=False, encode_format="jpg", compute_hash="sha256",
            distributor="multiprocessing", number_sample_per_shard=min(4096, max(1, math.ceil(frame.height / processes))),
            user_agent_token="flowdc-public-study", save_additional_columns=[c for c in frame.columns if c != "url"])
    require(type(cell["profile"]) is int and 0 <= cell["profile"] < len(PROFILES), "unfrozen PAARC profile")
    ratio, growth = PROFILES[cell["profile"]]
    return dict(
        input=str(directory / "input.parquet"), input_format="parquet", output=str(directory / "native"),
        output_format="webdataset", url="url", label=None, concurrent_downloads=cap,
        timeout=30, enable_paarc=True, control_method=cell["method"],
        method_options=dict(target_delay_ratio=ratio, startup_growth_factor=growth, baseline_max_age_s=120.0),
        C_min=2, C_init=4, C_max=cap, max_retry_attempts=1, research_profile=True,
        research_workload=PROFILE, create_tar=True, compress_tar=False,
        create_overview=True, force_overwrite=False)


def calibration_cells(source, rows, development=False):
    choices = [(m, i) for m in (METHODS if development else METHODS[2:]) for i in range(4)]
    random.Random(SEED + SOURCES.index(source)).shuffle(choices)
    return [dict(run_id=f"calibration-{source}-{m}-c{i}", phase="calibration",
                 source=source, workers=1, rows=rows, method=m, profile=i,
                 tool_setting=tool_settings(source)[i], candidate=i)
            for m, i in choices]


def select_configuration(records, method):
    require(len(records) == 4 and {r["candidate"] for r in records} == set(range(4)), "candidate inventory incomplete")
    complete = [r for r in records if r["status"] == "complete" and r["method"] == method
                and r.get("http_span_s", 0) >= LIMITS["minimum_http_seconds"]]
    require(complete, "no duration-qualified calibration candidate")
    coverage = max(r["coverage"] for r in complete)
    eligible = [r for r in complete if r["coverage"] >= coverage - .01]
    best = max(r["goodput_Bps"] for r in eligible)
    tied = [r for r in eligible if r["goodput_Bps"] >= .98 * best]
    selected = min(tied, key=lambda r: (r["mean_http_concurrency"], r["candidate"]))
    return dict(candidate=selected["candidate"], run_id=selected["run_id"],
                goodput_Bps=selected["goodput_Bps"], coverage=selected["coverage"])


def schedule(selected, sizes, sources):
    cells = []
    for source in sources:
        roles = [("ratio", METHODS[0], selected["development"][METHODS[0]]["candidate"]),
                 ("gradient", METHODS[1], selected["development"][METHODS[1]]["candidate"]),
                 ("transferred", METHODS[2], selected["development"][METHODS[2]]["candidate"]),
                 ("target_calibrated", METHODS[2], selected[source]["candidate"])]
        alias = roles[2][2] == roles[3][2]
        if alias:
            roles.pop()
        random.Random(SEED + SOURCES.index(source)).shuffle(roles)
        for block in range(6):
            ordered = roles[block % len(roles):] + roles[:block % len(roles)]
            for position, (arm, method, candidate) in enumerate(ordered):
                cells.append(dict(run_id=f"evaluation-{source}-b{block}-{arm}", phase="evaluation",
                    family=source, block=block, position=position, source=source, workers=1,
                    rows=sizes[source], method=method, arm=arm, profile=candidate,
                    tool_setting=tool_settings(source)[candidate], candidate=candidate,
                    reference_aliases=["target_calibrated"] if alias and arm == "transferred" else []))
    return cells


def forecast(record, rows):
    require(record["status"] == "complete" and record.get("phase") != "evaluation", "forecast requires pre-confirmation measurements")
    factor = rows / record["rows"]
    # Ordered physical clocks, unlike independent affine fits from two short runs.
    http = record["http_span_s"] * factor
    native = max(http, record["native_seconds"] * max(1, factor))
    total = max(native, record["elapsed_s"] * max(1, factor))
    return dict(http=http, native=native, total=total, body=record["body_bytes"] * factor)


def sizing(record, capacity):
    target = math.ceil(record["rows"] * LIMITS["target_http_seconds"] / record["http_span_s"])
    require(target <= capacity and target <= LIMITS["max_rows"], "ten-minute workload exceeds qualified disjoint population")
    require(forecast(record, target)["body"] * 1.25 <= LIMITS["job_body_bytes"], "forecast body exceeds finite workload")
    return target


def freeze(study, selected, sizes, sources, records, remaining_seconds):
    study = Path(study)
    require(not any(r.get("phase") == "evaluation" for r in records), "confirmation already started")
    cells = schedule(selected, sizes, sources)
    predictions = []
    for cell in cells:
        candidates = [r for r in records if r["source"] == cell["source"] and r["status"] == "complete"]
        require(candidates, "missing target sizing evidence")
        # PAARC transfer can be slower than target tool calibration: reserve the
        # worst selected-method development ratio as well as target setup cost.
        scaling = 1.0
        if cell["method"] != METHODS[2]:
            own = next(r for r in records if r["run_id"] == selected["development"][cell["method"]]["run_id"])
            tool = next(r for r in records if r["run_id"] == selected["development"][METHODS[2]]["run_id"])
            scaling = max(1.0, own["elapsed_s"] / tool["elapsed_s"])
        predicted = max(forecast(r, cell["rows"])["total"] for r in candidates) * scaling
        require(predicted < LIMITS["job_seconds"] - 1800,
                "forecast leaves no return/verification reserve")
        seconds = min(LIMITS["job_seconds"], max(3600, math.ceil(2 * predicted / 300) * 300))
        cell["job_seconds"] = seconds
        predictions.append(dict(run_id=cell["run_id"], seconds=predicted,
                                body=max(forecast(r, cell["rows"])["body"] for r in candidates)))
    # Use the same deadline for all arms on one target.
    for source in sources:
        seconds = max(c["job_seconds"] for c in cells if c["source"] == source)
        for c in cells:
            if c["source"] == source:
                c["job_seconds"] = seconds
    total = 1.5 * sum(p["seconds"] + 300 for p in predictions) + 86400 + 3600
    body = 1.25 * sum(p["body"] for p in predictions)
    used = sum(r.get("body_bytes", LIMITS["job_body_bytes"]) for r in records)
    require(total < remaining_seconds and used + body < LIMITS["body_bytes"], "measured confirmation forecast exceeds finite campaign")
    require(used + body < LIMITS["archive_bytes"], "calibration plus confirmation exceeds retained storage")
    value = dict(schema="flowdc-transfer-confirmation-v1", utc=utc(), blocks=6,
                 selected=selected, source_rows=sizes, sources=sources, cells=cells,
                 forecasts=predictions, expected_seconds=total, expected_body_bytes=body,
                 protocol_sha256=file_hash(study / "protocol.json"))
    write_new(study / "freeze.json", value)
    return value
