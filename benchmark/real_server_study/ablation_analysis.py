"""One bounded receipt audit, lossless metadata bundling and private report."""

import io
import json
import subprocess
import tarfile
import time
from pathlib import Path

from . import ablation
from .common import file_hash, lock, read_json, replace_derived, require, utc
from .onset_run import audit_events


def closed_archive(target, receipt):
    """Recover only derived archive bytes; never replay acquisition."""
    partial = target.with_name(target.name + ".partial")
    for path in (target, receipt, partial):
        require(not path.is_symlink(), "archive cache symlink")
    if receipt.exists():
        value = read_json(receipt)
        candidate = target if target.exists() else partial
        require(
            candidate.is_file()
            and candidate.stat().st_size == value["bytes"]
            and file_hash(candidate) == value["sha256"],
            "receipted archive changed",
        )
        if candidate == partial:
            partial.replace(target)
        return value
    if target.exists():
        require(not (target.parent / "record.json").exists(), "unreceipted finalized archive")
        target.unlink()  # Derived interrupted bytes; journals remain untouched.
    if partial.exists():
        require(partial.is_file(), "invalid archive temporary")
        partial.unlink()
    return None


def seal(directory):
    """Bundle journals once; retained native files and third-party payloads stay put."""
    directory = Path(directory)
    target = directory / "metadata.tar.gz"
    receipt = directory / "metadata-bundle.json"
    previous = closed_archive(target, receipt)
    if previous:
        return previous
    partial = target.with_name(target.name + ".partial")
    paths = [p for p in directory.rglob("*") if p.is_file() and p.suffix in (".json", ".jsonl", ".log")]
    require(not any(p.is_symlink() for p in paths), "metadata symlink")
    manifest = {}
    with tarfile.open(partial, "x:gz", compresslevel=1) as archive:
        for p in sorted(paths):
            name = str(p.relative_to(directory))
            before = p.stat()
            manifest[name] = dict(bytes=before.st_size, sha256=file_hash(p))
            archive.add(p, arcname=name, recursive=False)
            after = p.stat()
            require(
                (before.st_size, before.st_mtime_ns, before.st_ctime_ns)
                == (after.st_size, after.st_mtime_ns, after.st_ctime_ns),
                "metadata changed while sealing",
            )
        raw = json.dumps(manifest, sort_keys=True).encode()
        member = tarfile.TarInfo("bundle-manifest.json")
        member.size = len(raw)
        archive.addfile(member, io.BytesIO(raw))
    value = dict(
        path=target.name, sha256=file_hash(partial), bytes=partial.stat().st_size, files=len(manifest)
    )
    replace_derived(receipt, value)
    partial.replace(target)
    return value


def audit(study, source, seconds=1200):
    study = Path(study)
    ablation.check(study, source)
    require(0 < seconds <= ablation.LIMITS["audit_seconds"], "audit limit")
    began = time.monotonic()
    deadline = began + seconds
    out = study / "analysis"
    out.mkdir(exist_ok=True)
    with lock(study / "study.lock"):
        result = []
        cache = read_json(out / "audit-units.json") if (out / "audit-units.json").exists() else {}
        for run in ablation.schedule():
            directory = study / "runs" / run["run_id"]
            path = directory / "record.json"
            if not path.exists():
                result.append(
                    dict(run, status="interrupted" if directory.exists() else "unstarted", audited=False)
                )
                continue
            row = read_json(path)
            require(all(row[k] == v for k, v in run.items()), "schedule identity changed")
            if row.get("status") in ("no_candidate", "alias"):
                result.append(dict(row, audited=True))
                continue
            try:
                require(time.monotonic() < deadline, "audit deadline")
                binding = read_json(directory / "attempt.json")
                require(
                    binding["protocol_sha256"] == file_hash(study / "protocol.json", deadline),
                    "protocol changed",
                )
                require(
                    binding["slice_sha256"]
                    == file_hash(study / "slices" / f"{run['slice']}.parquet", deadline),
                    "input changed",
                )
                if run["phase"] == "validation":
                    require(
                        binding["selection_sha256"] == file_hash(study / "selection.json", deadline),
                        "selection changed",
                    )
                digest = file_hash(path, deadline)
                archive = directory / "native.tar"
                stamp = (
                    [
                        getattr(archive.stat(), k)
                        for k in ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")
                    ]
                    if row["protocol_window_complete"]
                    else None
                )
                previous = cache.get(run["run_id"])
                reused = (
                    previous and previous["record_sha256"] == digest and previous["archive_stat"] == stamp
                )
                for name, expected in row["evidence"].items():
                    relative = Path(name)
                    require(
                        not relative.is_absolute()
                        and ".." not in relative.parts
                        and not (directory / relative).is_symlink(),
                        "unsafe evidence",
                    )
                    if name != "native.tar" or not reused:
                        require(
                            file_hash(directory / relative, deadline) == expected, "evidence changed: " + name
                        )
                if row["protocol_window_complete"]:
                    verification = read_json(directory / "verification.json")
                    credits = verification.pop("credited_url_bytes")
                    require(
                        verification.pop("observations") == row["observations"]
                        and verification == row["verification"],
                        "verification receipt changed",
                    )
                    require(
                        sum(credits.values()) == verification["credited_bytes"], "credit arithmetic changed"
                    )
                    events, invalid = audit_events(directory / "request-audit.jsonl")
                    require(not invalid, "malformed audit")
                    require(
                        not any(ablation.terminal_fault(e) for e in events),
                        "terminal public fault in credited window",
                    )
                    require(
                        row["exposure"]
                        == ablation.exposure(
                            events,
                            run["phase"],
                            credits,
                            read_json(study / "probe-summary.json")["ttfb_p50_s"],
                        ),
                        "exposure changed",
                    )
                    require(
                        row["goodput_Bps"] == verification["credited_bytes"] / row["elapsed_s"],
                        "timing arithmetic changed",
                    )
                cache[run["run_id"]] = dict(record_sha256=digest, archive_stat=stamp, utc=utc())
                result.append(
                    dict(
                        row, audited=True, status="complete" if row["protocol_window_complete"] else "stopped"
                    )
                )
            except TimeoutError:
                result.append(dict(run, status="audit_pending", audited=False))
            except ValueError as error:
                if str(error) != "audit deadline":
                    raise
                result.append(dict(run, status="audit_pending", audited=False))
        replace_derived(out / "audit-units.json", cache)
        # Selection is recomputed from all original screening records, including
        # adverse ones, so the audit cannot silently select a different winner.
        if (study / "selection.json").exists():
            records = [
                {**read_json(p), "record_sha256": file_hash(p)}
                for p in study.glob("runs/screen-*/record.json")
            ]
            require(
                read_json(study / "selection.json") == ablation.select(records), "selection receipt changed"
            )
        lab = []
        from .ablation_controlled import cases

        for cell in cases():
            path = study / "controlled" / cell["run_id"] / "record.json"
            if not path.exists():
                lab.append(dict(cell, status="unstarted"))
                continue
            row = read_json(path)
            try:
                for name, expected in row["evidence"].items():
                    relative = Path(name)
                    require(
                        not relative.is_absolute()
                        and ".." not in relative.parts
                        and not (path.parent / relative).is_symlink(),
                        "unsafe controlled evidence",
                    )
                    require(
                        file_hash(path.parent / relative, deadline) == expected, "controlled evidence changed"
                    )
                lab.append(dict(row, audited=True))
            except TimeoutError:
                lab.append(dict(cell, status="audit_pending", audited=False))
        value = dict(
            schema="flowdc-adaptive-ablation-results-v1",
            protocol_sha256=file_hash(study / "protocol.json"),
            record_inventory=record_inventory(study),
            runs=result,
            controlled=lab,
            audit_elapsed_s=time.monotonic() - began,
            audit_pending=sum(r["status"] == "audit_pending" for r in result)
            + sum(r.get("status") == "audit_pending" for r in lab),
            limitations="One retained-payload decode/verification per credited run; offline audit rechecks digests and arithmetic. No independent reproduction or global optimum claim.",
        )
        replace_derived(out / "results.json", value)
    return value


def record_inventory(study):
    """Bind derived audit results to the current raw first-attempt inventory."""
    from .ablation_controlled import cases

    result = {}
    for parent, runs in (("runs", ablation.schedule()), ("controlled", cases())):
        for run in runs:
            directory = Path(study) / parent / run["run_id"]
            path = directory / "record.json"
            result[f"{parent}/{run['run_id']}"] = (
                file_hash(path) if path.exists() else ("interrupted" if directory.exists() else "unstarted")
            )
    return result


def results_current(study, value):
    """Older audit receipts must also agree with every current raw record."""
    if value.get("protocol_sha256") != file_hash(Path(study) / "protocol.json"):
        return False
    if "record_inventory" in value:
        return value["record_inventory"] == record_inventory(study)
    from .ablation_controlled import cases

    for parent, runs, key in (("runs", ablation.schedule(), "runs"), ("controlled", cases(), "controlled")):
        rows = value.get(key, [])
        by_id = {r["run_id"]: r for r in rows}
        if len(rows) != len(runs) or set(by_id) != {r["run_id"] for r in runs}:
            return False
        for run in runs:
            row = by_id[run["run_id"]]
            directory = Path(study) / parent / run["run_id"]
            path = directory / "record.json"
            if not path.exists():
                if row.get("status") != ("interrupted" if directory.exists() else "unstarted"):
                    return False
            elif row.get("status") == "audit_pending" and not row.get("audited"):
                continue  # No numeric credit is drawn from this entry.
            else:
                raw = read_json(path)
                status = raw.get("status")
                if parent == "runs" and status not in ("alias", "no_candidate"):
                    status = "complete" if raw.get("protocol_window_complete") else "stopped"
                if (
                    row.get("status") != status
                    or not row.get("audited")
                    or any(row.get(k) != v for k, v in raw.items() if k != "status")
                ):
                    return False
    return True


def report(study, output=None):
    from .analyze import summary

    study = Path(study)
    out = Path(output) if output else study.parent / "final-report"
    out.mkdir(parents=True, exist_ok=True)
    cached = (
        read_json(study / "analysis/results.json") if (study / "analysis/results.json").exists() else None
    )
    if cached is not None and results_current(study, cached):
        value = cached
    else:
        from .ablation_controlled import cases

        def inventory(parent, run):
            directory = study / parent / run["run_id"]
            path = directory / "record.json"
            row = read_json(path) if path.exists() else {}
            status = (
                (
                    row.get("status")
                    if row.get("status") in ("alias", "no_candidate", "timeout", "failed")
                    else "audit_pending"
                )
                if path.exists()
                else ("interrupted" if directory.exists() else "unstarted")
            )
            return dict(
                run,
                audited=False,
                status=status,
                **({"alias_of": row["alias_of"]} if status == "alias" else {}),
            )

        runs = [inventory("runs", r) for r in ablation.schedule()]
        lab = [inventory("controlled", r) for r in cases()]
        value = dict(
            protocol_sha256=file_hash(study / "protocol.json"),
            runs=runs,
            controlled=lab,
            audit_pending=sum(r["status"] == "audit_pending" for r in runs + lab),
            limitations="No current aggregate audit is available; numeric outcomes are withheld. Status-only inventory, not efficacy evidence.",
        )
    require(value["protocol_sha256"] == file_hash(study / "protocol.json"), "report protocol mismatch")
    rows = {r["run_id"]: r for r in value["runs"]}

    def resolved(r):
        if r.get("status") == "alias":
            return rows.get(r["alias_of"], {})
        return r

    def eligible(r):
        return r.get("audited") and r.get("protocol_window_complete") and r.get("status") == "complete"

    contrasts = {}
    evaluation_contrasts = {}
    qualification = {}
    for method, old in zip(ablation.METHODS, ("ratio-v1", "gradient-candidate-v1")):
        for kind in ("balanced", "high"):
            differences = []
            evaluation_differences = []
            successes = 0
            complete_windows = 0
            for b in range(1, 4):
                a = resolved(rows[f"validation-{b}-{kind}-{method}"])
                base = rows[f"validation-{b}-{old}"]
                if eligible(a):
                    complete_windows += 1
                    successes += bool(a["exposure"]["qualified"])
                if eligible(a) and eligible(base):
                    differences.append(a["goodput_Bps"] - base["goodput_Bps"])
                    if a["exposure"]["window_complete"] and base["exposure"]["window_complete"]:
                        evaluation_differences.append(
                            a["exposure"]["window_goodput_Bps"] - base["exposure"]["window_goodput_Bps"]
                        )
            contrasts[f"{kind}-{method}"] = summary(differences)
            evaluation_contrasts[f"{kind}-{method}"] = summary(evaluation_differences)
            qualification[f"{kind}-{method}"] = dict(
                qualified_windows=successes,
                complete_windows=complete_windows,
                required=3,
                replicated_150=successes == 3,
            )
    value.update(
        paired_differences_Bps=contrasts,
        paired_evaluation_differences_Bps=evaluation_contrasts,
        qualification=qualification,
    )
    capacity = [r for r in value["runs"] if r["phase"] == "capacity" and eligible(r)]
    value["selection"] = read_json(study / "selection.json") if (study / "selection.json").exists() else None
    replace_derived(out / "results.json", value)
    replace_derived(
        out / "claims.json",
        dict(
            actual_150={
                arm: dict(
                    status="supported_in_three_windows" if q["replicated_150"] else "not_established",
                    evidence=[f"validation-{b}-{arm}" for b in range(1, 4)],
                )
                for arm, q in qualification.items()
            },
            optimum="Not established globally: finite exploratory grid, three held-out blocks, one host and cache/load history.",
            superiority="Adaptivity alone does not establish empirical superiority; paired outcomes and costs must be assessed.",
            protocol_sha256=value["protocol_sha256"],
        ),
    )
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(2, 1, figsize=(7, 6), layout="constrained")
    screened = [r for r in value["runs"] if r["phase"] == "screen" and eligible(r) and r.get("exposure")]
    for method in ablation.METHODS:
        rr = [r for r in screened if r["method"] == method]
        axes[0].plot(
            [r["grid"] for r in rr], [r["exposure"].get("active_median") or 0 for r in rr], "o", label=method
        )
        axes[1].plot(
            [r["grid"] for r in rr], [(r.get("goodput_Bps") or 0) / 1e6 for r in rr], "o", label=method
        )
    axes[0].axhline(150, color="gray", linestyle="--", linewidth=1)
    axes[0].set(ylabel="Actual HTTP median", xlabel="Frozen grid index")
    axes[1].set(ylabel="Verified end-to-end MB/s", xlabel="Frozen grid index")
    for ax in axes:
        ax.grid(alpha=0.2)
        if ax.get_legend_handles_labels()[0]:
            ax.legend(fontsize=8, frameon=False)
    fig.savefig(out / "screening.pdf")
    plt.close(fig)
    esc = lambda x: "".join(
        {
            "_": r"\_",
            "%": r"\%",
            "&": r"\&",
            "#": r"\#",
            "{": r"\{",
            "}": r"\}",
            "\\": r"\textbackslash{}",
        }.get(c, c)
        for c in str(x)
    )
    num = lambda x, scale=1: "--" if x is None else f"{x / scale:.3f}"
    lines = [
        r"\documentclass[11pt]{article}",
        r"\usepackage[margin=.8in]{geometry}",
        r"\usepackage[T1]{fontenc}\usepackage{newtxtext,newtxmath,booktabs,longtable,graphicx,xcolor,hyperref}",
        r"\definecolor{navy}{HTML}{18394A}\hypersetup{colorlinks=true,urlcolor=navy}",
        r"\setlength{\emergencystretch}{2em}\setlength{\parindent}{0pt}\setlength{\parskip}{6pt}\begin{document}",
        r"\title{\textcolor{navy}{FLOW-DC adaptive-concurrency ablation}}\author{Private exploratory evidence report}\date{\today}\maketitle",
        r"\section{Outcome}",
        f"Public matrix: 36 planned slots, including {sum(bool(r.get('audited')) and r.get('status') == 'complete' for r in value['runs'])} audited complete acquisitions. Alias and no-candidate slots are not downloads. Audit pending: {value['audit_pending']}.",
        "The 150-request target refers to actual requests from headers sent through body completion. It requires at least 90 percent of the evaluation window at or above 150, one continuous 30-second span, and queued demand throughout. A permit cap or isolated peak is insufficient.",
    ]
    for arm, q in qualification.items():
        lines.append(
            esc(arm)
            + f": {q['qualified_windows']} qualified among {q['complete_windows']} audited complete held-out windows; three qualified windows required. Replicated target {'met' if q['replicated_150'] else 'not established'}."
        )
    from collections import Counter

    counts = Counter(r["status"] for r in value["runs"])
    lines.append("First-attempt status counts: " + esc(dict(sorted(counts.items()))) + ".")
    if value["selection"] and not any(k.startswith("high-") for k in value["selection"]["choices"]):
        lines.append(
            "No feasible high-concurrency selection was frozen from screening. Its validation slots remain no-candidate or unstarted rather than causing additional downloads."
        )
    if len(capacity) == 2 and not any(r["exposure"]["qualified"] for r in capacity):
        lines.append(
            "Neither fixed pipeline diagnostic sustained the 150-request criterion in this client/storage configuration. Therefore the target shortfall cannot be attributed to controller tuning alone. This does not establish a server quota or prove that the server cannot sustain150 under another client configuration."
        )
    lines += [
        r"\section{Prospective design}",
        "Two redesigned methods start at four permits, minimum two, with an internal ceiling of 512 and a common HTTP/TCP ceiling of 256. Multiplicative startup, elapsed smoothing, utilization gating, refreshed delay baseline and method feedback are separately exercised in 24 normalized controlled cases. These cases use known image bytes and capacity four--one--four; they do not prove public-host capacity.",
        "The public matrix contains two pipeline diagnostics, 16 full-factorial screening runs and 18 held-out validation slots. Grid factors are startup multiplier 1.25/1.5, baseline age 10/120 seconds and target delay ratio 2/3. Screening and validation populations are disjoint; the two methods share the same manifest within each grid cell, while different grid cells use disjoint tuning slices. Validation arms share their block manifest, and blocks use disjoint slices. Grid trends are descriptive because configuration and slice both vary across grid indices. There is no cache busting. Validation has three independent initializations per arm. Identical balanced and high selections alias one execution and are never extra replicates.",
        "Screening evaluation is seconds 20--60 after first dispatch; capacity and validation use seconds 30--90. Network windows are 120/180 seconds, followed by a bounded 30-second drain. Body allowance can terminate a window early. Original manifests contain 32,768 rows; nondispatched rows remain unattempted, and partial or unknown outcomes are not credited.",
        "The selected 120-second baseline age exceeds both primary evaluation intervals. Those intervals assess cold-start behavior before active age-based refresh; they do not establish long-run post-refresh stability. The short-refresh controlled cases exercise probing separately. A failed processing/verification attempt also reduces the available tuning evidence; selections are conditional on eligible completed cells, not proven optima even over every scheduled grid cell.",
        "Balanced selection requires median first-byte delay no more than three times the initial low-load baseline. Both selection classes require at least 99 percent credited among dispatched terminal rows and no failed warning response. Among settings within five percent of the best verified evaluation-window goodput, select the lowest actual median, then p95 delay, then fixed identifier. This is conditional selection, not a global optimum.",
        r"\section{All first-attempt statuses}\small\begin{longtable}{p{.42\linewidth}lrrrr}\toprule Run & Status & Median & Peak & 150 s & MB/s\\\midrule\endhead",
    ]
    for r in value["runs"]:
        e = r.get("exposure", {})
        label = r["run_id"].replace("gradient-candidate-", "Gradient-").replace("ratio-", "Ratio-")
        lines.append(
            esc(label)
            + " & "
            + esc(r["status"])
            + f" & {num(e.get('active_median'))} & {e.get('peak', '--')} & {num(e.get('seconds_at_150'))} & {num(r.get('goodput_Bps'), 1e6)} "
            + r"\\"
        )
    lines += [
        r"\bottomrule\end{longtable}\normalsize",
        r"\section{Screening factor key}",
        r"\begin{center}\begin{tabular}{rrrr}\toprule Grid & Startup multiplier & Refresh (s) & Delay target\\\midrule",
    ]
    for i, factors in enumerate(ablation.grid()):
        lines.append(
            f"{i} & {factors['startup_growth_factor']} & {factors['baseline_max_age_s']:.0f} & {factors['target_delay_ratio']:.0f} "
            + r"\\"
        )
    lines += [
        r"\bottomrule\end{tabular}\end{center}",
        r"\includegraphics[width=\linewidth]{screening.pdf}",
        r"\section{Validation and uncertainty}",
        "End-to-end goodput here starts immediately before native subprocess launch and ends after independent verification and payload compaction. It excludes admission scans, metadata sealing, VM staging and local reporting; those costs remain in campaign wall time and operational exposure. Selection uses the separate fixed evaluation-window boundary. Requests are not independent replicates. Intervals below are ordinary exploratory paired t intervals with no multiplicity adjustment; n=3 gives limited precision.",
    ]
    for arm, s in contrasts.items():
        ci = "unavailable" if not s["ci"] else "[" + ", ".join(num(x, 1e6) for x in s["ci"]) + "] MB/s"
        lines.append(
            esc(arm)
            + f": {s['n']} complete matched blocks; mean difference versus its v1 method {num(s['mean'], 1e6)} MB/s; 95\\% interval {ci}."
        )
    for arm, result in evaluation_contrasts.items():
        ci = (
            "unavailable"
            if not result["ci"]
            else "[" + ", ".join(num(x, 1e6) for x in result["ci"]) + "] MB/s"
        )
        lines.append(
            esc(arm)
            + f": evaluation-window difference, {result['n']} complete matched blocks; mean {num(result['mean'], 1e6)} MB/s; ordinary 95\\% interval {ci}."
        )
    if value["selection"]:
        lines.append(r"\section{Frozen selected configurations}")
        for arm, choice in value["selection"]["choices"].items():
            lines.append(
                esc(arm)
                + f": startup multiplier {choice['options']['startup_growth_factor']}; baseline age {choice['options']['baseline_max_age_s']:.0f} seconds; target delay ratio {choice['options']['target_delay_ratio']}. Selected from grid {choice['screen_run'].rsplit('-', 1)[-1]}; the exact source identifier is retained in results.json."
            )
    lines += [r"\section{Accountability and adverse outcomes}"]
    for r in value["runs"]:
        if not r.get("verification"):
            continue
        v = r["verification"]
        label = esc(r["run_id"].replace("gradient-candidate-", "Gradient-"))
        lines.append(
            label
            + f": original rows {v['original_rows']}; dispatched {v['dispatched_rows']}; terminal {v['dispatched_terminal_rows']}; dispatched administrative censoring {v['administratively_censored_dispatched_rows']}; intents without HTTP dispatch {v['attempted_rows'] - v['dispatched_rows']}; independently credited {v['credited_rows']}; undecodable native outputs {v['decode_failures']}; native disposition totals "
            + esc(v["dispositions"])
            + "."
        )
    lines += [
        r"\section{Latency and evaluation-window outcomes}",
        "Times are application first-body delays. Warning labels are pass/fail or not seen; an unobserved warning is not evidence of successful warning response. Only audited protocol-complete acquisitions appear below.",
        r"\small\begin{longtable}{p{.42\linewidth}rrrl}\toprule Run & p50 ms & p95 ms & Eval MB/s & Warning\\\midrule\endhead",
    ]
    for row in value["runs"]:
        if not eligible(row):
            continue
        exposure = row["exposure"]
        warning = {"passed": "pass", "failed": "fail", "not_exercised": "not seen"}.get(
            exposure.get("warning_response"), "unknown"
        )
        label = row["run_id"].replace("gradient-candidate-", "Gradient-").replace("ratio-", "Ratio-")
        lines.append(
            esc(label)
            + f" & {num(exposure.get('median_ttfb_s'), 0.001)} & {num(exposure.get('p95_ttfb_s'), 0.001)} & {num(exposure.get('window_goodput_Bps'), 1e6)} & {warning} "
            + r"\\"
        )
    lines += [r"\bottomrule\end{longtable}\normalsize"]
    for r in value["runs"]:
        if r.get("stop_reason") or r.get("fatal_reason"):
            lines.append(
                esc(r["run_id"])
                + ": "
                + esc(r.get("stop_reason") or r.get("fatal_reason"))
                + ". Its partial output is not included as a complete-run efficacy observation."
            )
    packet = study.parent
    amendment = (
        read_json(packet / "window-count-amendment.json")
        if (packet / "window-count-amendment.json").exists()
        else {}
    )
    recovery_path = packet / "provider-window-recovery-amendment.json"
    provider_recovery = read_json(recovery_path) if recovery_path.exists() else {}
    max_windows = provider_recovery.get(
        "amended_max_windows", amendment.get("amended_max_windows", ablation.LIMITS["max_windows"])
    )
    closures = sorted(packet.glob("window-[0-9][0-9].json"))
    for name in ("acquisition-ended.json", "recovery-skipped.json"):
        path = study.parent / name
        if path.exists():
            lines.append(esc(name) + ": " + esc(read_json(path).get("reason", "finite campaign stop")) + ".")
    if (packet / "complete.json").exists():
        complete = read_json(packet / "complete.json")
        lines.append(
            f"Recorded campaign wall time: {num(complete.get('wall_seconds'), 3600)} hours. The declared 8-hour study ended with missing validation acquisitions; this is an incomplete efficacy study, not a submission-ready result."
        )
    if closures:
        last = read_json(closures[-1])
        lines += [
            r"\section{Operational exposure and cleanup}",
            f"Closed VM windows: {len(closures)} of {max_windows}. New conservative journal exposure: {num(last.get('new_exposure_su'))} SUs; this is not exact provider billing. ",
            "Fresh offload and original-network verification: "
            + esc(last.get("fresh_offload_and_network_verified", False))
            + " at "
            + esc(last["utc"])
            + ". Retained payloads remain on the campaign-owned volume.",
        ]
    if (packet / "worker-environment-observation.json").exists():
        lines.append(
            "Execution used one Jetstream2 downloader guest with eight visible CPUs and approximately 29 GiB RAM. The bounded native unit imposed CPUQuota 400 percent, MemoryMax 24 GiB and 4096 file descriptors. The controlled origin ran on that worker; public acquisition was not TaskVine distributed execution. Manager/origin operational roles were present, but this study does not establish multi-worker scaling."
        )
    if amendment:
        lines.append(
            "Three setup windows were retained; the window-count amendment preserved the original eight-hour deadline and 300-SU ceiling. No controller, input or selection rule changed."
        )
    if provider_recovery:
        lines.append(
            "A later provider observation error on the manager closed the connection before validation. Recovery required fresh cleanup and guest proof of zero validation attempts and no terminal study stop; one additional operational window was allowed within the same deadline and spending caps. No scientific download was repeated."
        )
    if (packet / "processing-amendment.json").exists():
        lines.append(
            "One screen exhausted its 420-second processing limit during independent verification. It remains a failed, unselectable first attempt. A separately labeled offline diagnostic checked its retained archive without downloading again; unfinished screens then received a prospective 600-second processing ceiling with unchanged network/evaluation windows. The offline diagnostic consumes the existing aggregate audit allowance."
        )
    if (packet / "post-stop-quiescence.json").exists():
        stop = read_json(packet / "post-stop-quiescence.json").get("ended") or {}
        lines.append(
            "The held-out stage stopped at "
            + esc(stop.get("run_id", "unknown case"))
            + " because of "
            + esc(stop.get("reason", "a retained execution failure"))
            + ". This failed first attempt is not a complete-run observation; remaining validation acquisitions are unstarted. A full 180-second timer fired, but native processing exceeded its deadline. No new public requests were issued to replace it."
        )
    if (packet / "stop-diagnosis-pause.json").exists():
        lines.append(
            "An inspection pause of the local coordinator overlapped a mounting SSH call and exhausted its timeout. That operational window was retained and freshly cleaned; no new scientific acquisition occurred in it. The final window performs offline audit and metadata collection only."
        )
    if (packet / "artifact-admission-amendment.json").exists():
        lines.append(
            "Read-only parallel logical-byte accounting replaced repeated whole-tree Python scans. Completed archive digests could seed the existing unchanged-file audit cache. Scheduling reserves were adjusted within the original hard limits. Metadata transfer and local extraction overlap supervised cleanup only after local archive checksum verification."
        )
    lines += [
        r"\section{Controlled mechanism observations}\small\begin{longtable}{p{.63\linewidth}rr}\toprule Case & Verified rows & Realized\\\midrule\endhead"
    ]
    for r in value["controlled"]:
        lines.append(
            esc(r["run_id"].replace("gradient-candidate-", "Gradient-"))
            + f" & {r.get('verified_rows', '--')} & {r.get('realized_stimulus', False)} "
            + r"\\"
        )
    lines += [
        r"\bottomrule\end{longtable}\normalsize",
        r"\section{Observed control decisions}",
        "Counts below describe named trace decisions in one run per cell. Baseline reset may reflect a new minimum or a sample gap; it is not an active baseline probe. Ratio's utilization-hold label can also mean an unchanged rounded proposal. Counts alone do not isolate a causal effect or prove that every mechanism was exercised.",
        r"\small\begin{longtable}{p{.49\linewidth}rrrrr}\toprule Case & Startup & Probe & Sparse & Hard & Rows\\\midrule\endhead",
    ]
    for row in value["controlled"]:
        reasons = row.get("control_reasons", {}) if row.get("audited") else {}
        lines.append(
            esc(row["run_id"].replace("gradient-candidate-", "Gradient-"))
            + " & "
            + " & ".join(
                str(reasons.get(k, 0)) if row.get("audited") else "--"
                for k in ("startup_increase", "baseline_probe", "sparse_hold", "hard_queue_decrease")
            )
            + f" & {row.get('verified_rows', '--')} "
            + r"\\"
        )
    gate = read_json(study / "host-gate.json") if (study / "host-gate.json").exists() else {}
    lines += [
        r"\bottomrule\end{longtable}\normalsize",
        r"\section{Stops, limits and provenance}",
        "Three-times baseline is a warning; five-times baseline over three qualified ten-second bins stops an arm. A latency-only stop permits a predefined cooldown and low-load recovery, with at most three cooldowns. Access refusals, 429/503, TLS or integrity failures end public testing. Latency growth alone is not proof of a server rate-limit mechanism.",
        f"The live envelope is eight hours, at most {max_windows} one-hour VM windows, 300 conservative SUs, 1.2 million public request intents, 200 GiB bodies and 256 GiB artifacts. No favorable retries replace first attempts. Unknown, interrupted, unstarted, aliased and audit-pending slots remain visible.",
        "Public images are drawn from the frozen TaiBIF moth population at moth.tbn.org.tw/images/. Dataset, license, sampling and URL provenance are retained in protocol.json and the frozen manifests. "
        + (
            "The previously authorized host-specific certificate validity-period exception was used; certificate-chain and hostname verification remained enabled. Certificate, peer and robots observations are retained in host-gate.json."
            if gate.get("ignore_time")
            else "Certificate and robots observations, when acquired, are retained in host-gate.json."
        ),
        esc(value["limitations"]),
        "Protocol SHA-256: "
        + r"\texttt{"
        + value["protocol_sha256"]
        + r"}. Machine-readable results, exact configurations, selection receipts and retained request/control journals accompany this report.",
        r"\end{document}",
    ]
    lines = [
        line + r"\par"
        if line and not line.startswith("\\") and " & " not in line and not line.endswith(r"\\")
        else line
        for line in lines
    ]
    (out / "report.tex").write_text("\n".join(lines) + "\n")
    with (out / "latex.log").open("wb") as log:
        for _ in range(2):
            subprocess.run(
                ["pdflatex", "-interaction=nonstopmode", "-halt-on-error", "report.tex"],
                cwd=out,
                stdout=log,
                stderr=log,
                check=True,
                timeout=60,
            )
    replace_derived(
        out / "report-manifest.json",
        dict(
            utc=utc(),
            generator_sha256=file_hash(Path(__file__)),
            measured_source_head=read_json(study / "protocol.json").get("source", {}).get("head"),
            operational_receipts={
                p.name: file_hash(p)
                for p in packet.glob("*.json")
                if p.name.startswith(
                    (
                        "window-",
                        "processing-amendment",
                        "artifact-admission",
                        "provider-window-recovery",
                        "offline-recovery",
                        "worker-environment",
                    )
                )
            },
            files={
                p.name: file_hash(p)
                for p in out.iterdir()
                if p.suffix in (".pdf", ".tex", ".json") and p.name != "report-manifest.json"
            },
        ),
    )
    return out / "report.pdf"
