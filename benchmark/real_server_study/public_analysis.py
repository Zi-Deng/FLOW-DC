"""Small pilot summaries from verified first attempts, including incomplete cells."""

import argparse
import json
import math
import statistics
from pathlib import Path

from .common import file_hash, replace_derived, require
from .public_batch import METHODS, SOURCES, inventory


def report(study):
    study = Path(study)
    records, interrupted = inventory(study)
    for row in records:
        directory = study / "runs" / row["run_id"]
        for name, digest in row["evidence"].items():
            require(Path(name).name == name, "unsafe record evidence")
            require(file_hash(directory / name) == digest, "collected evidence changed")
    contrasts = []
    summaries = []
    evaluation = [r for r in records if r["phase"] == "evaluation"]
    for source in SOURCES:
        for method in METHODS:
            cells = [r for r in evaluation if r["source"] == source and r["method"] == method]
            complete = [r for r in cells if r["status"] == "complete"]
            summaries.append(
                dict(
                    source=source,
                    method=method,
                    planned=4,
                    attempted=len(cells),
                    complete=len(complete),
                    full_success=sum(r["full_success"] for r in complete),
                    mean_goodput_Bps=statistics.mean(r["goodput_Bps"] for r in complete)
                    if complete
                    else None,
                    mean_elapsed_s=statistics.mean(r["elapsed_s"] for r in complete) if complete else None,
                    mean_coverage=statistics.mean(r["coverage"] for r in complete) if complete else None,
                    missing=4 - len(complete),
                )
            )
        for method in METHODS[:2]:
            for reference in METHODS[2:]:
                pairs = []
                for block in range(4):
                    by = {
                        r["method"]: r
                        for r in evaluation
                        if r["source"] == source and r["index"] == block and r["status"] == "complete"
                    }
                    if method in by and reference in by:
                        a, b = by[method], by[reference]
                        require(a["input_sha256"] == b["input_sha256"], "paired inputs differ")
                        # Remote contents may differ; preserve rather than filter them.
                        common = set(a["verification"]["payloads"]) & set(b["verification"]["payloads"])
                        changed = sum(
                            a["verification"]["payloads"][i]["sha256"]
                            != b["verification"]["payloads"][i]["sha256"]
                            for i in common
                        )
                        pairs.append(
                            dict(
                                block=block,
                                log_ratio=math.log(a["goodput_Bps"] / b["goodput_Bps"])
                                if min(a["goodput_Bps"], b["goodput_Bps"]) > 0
                                else None,
                                changed_common_payloads=changed,
                            )
                        )
                logs = [p["log_ratio"] for p in pairs if p["log_ratio"] is not None]
                interval = None
                if len(logs) >= 2:
                    from scipy.stats import t

                    mean = statistics.mean(logs)
                    half = float(t.ppf(0.975, len(logs) - 1)) * statistics.stdev(logs) / math.sqrt(len(logs))
                    interval = [math.exp(mean - half), math.exp(mean + half)]
                contrasts.append(
                    dict(
                        source=source,
                        method=method,
                        reference=reference,
                        pairs=pairs,
                        geometric_goodput_ratio=math.exp(statistics.mean(logs)) if logs else None,
                        ordinary_95_interval=interval,
                        interpretation="Exploratory complete-pair interval; no multiplicity adjustment, population-generalization or confirmatory claim",
                    )
                )
    result = dict(
        schema="flowdc-public-pilot-results-v1",
        protocol_sha256=file_hash(study / "protocol.json"),
        records=records,
        interrupted=interrupted,
        summaries=summaries,
        contrasts=contrasts,
        missing_evaluation=32 - sum(r["status"] == "complete" for r in evaluation),
        limits="Four blocks/source. External state/cache/content not controlled. No proven universal optimum or superiority. Tool concurrency includes connection setup; native dispatch begins at headers sent. Tool latency safeguards not established.",
    )
    (study / "analysis").mkdir(exist_ok=True)
    replace_derived(study / "analysis/results.json", result)
    lines = [
        "# Real-world download pilot",
        "",
        result["limits"],
        "",
        "| Source | Method | Complete / planned | Mean MiB/s | Mean seconds | Mean coverage |",
        "|---|---|---:|---:|---:|---:|",
    ]
    for row in summaries:

        def fmt(key, scale=1, row=row):
            return "—" if row[key] is None else f"{row[key] / scale:.3f}"

        lines.append(
            f"| {row['source']} | {row['method']} | {row['complete']}/4 | {fmt('mean_goodput_Bps', 2**20)} | {fmt('mean_elapsed_s')} | {fmt('mean_coverage')} |"
        )
    lines += [
        "",
        f"Missing or incomplete evaluation cells: {result['missing_evaluation']}. All first attempts remain in results.json.",
        "",
        "Primary goodput counts decoded, archive-verified original-row bytes over job launch through verification. It excludes batch-level staging and metadata transfer, which must be reported separately. Archived original bytes remain on the retained worker volume; this local package contains verified metadata and archive digests.",
    ]
    replace_derived(study / "analysis/REPORT.md", "\n".join(lines) + "\n")
    return dict(status="report_complete", runs=len(records), missing_evaluation=result["missing_evaluation"])


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("study", type=Path)
    a = p.parse_args()
    print(json.dumps(report(a.study)))
