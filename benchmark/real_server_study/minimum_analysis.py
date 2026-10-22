"""Measured-only summaries of frozen first attempts; missing cells stay visible."""

import argparse
import csv
import json
import math
import statistics
import time
from pathlib import Path

from .common import file_hash, read_json, require, replace_derived, utc
from .minimum_study import METHODS


def contrast(pairs):
    if not pairs:
        return dict(
            blocks=0, ratio=None, interval95=None, p_greater=None, interpretation="no complete matched pairs"
        )
    values = [math.log(a / b) for a, b in pairs]
    mean = statistics.mean(values)
    result = dict(
        blocks=len(values),
        ratio=math.exp(mean),
        interval95=None,
        p_greater=None,
        pairs=[a / b for a, b in pairs],
        interpretation="descriptive paired case study",
    )
    if len(values) > 1:
        from scipy.stats import t

        error = statistics.stdev(values) / math.sqrt(len(values))
        width = float(t.ppf(0.975, len(values) - 1)) * error
        result["interval95"] = [math.exp(mean - width), math.exp(mean + width)]
        # Conditional normal-log-ratio interval. Six blocks remain a small sample.
        if len(values) >= 6:
            result["p_greater"] = (
                float(t.sf(mean / error, len(values) - 1)) if error else (0.0 if mean > 0 else 1.0)
            )
            result["interpretation"] = (
                "small-sample paired t inference assuming independent normal log ratios"
            )
    return result


def summarize(study):
    study = Path(study)
    deadline = time.monotonic() + 1200
    records = {p.parent.name: read_json(p) for p in (study / "runs").glob("*/record.json")}
    frozen = read_json(study / "freeze.json") if (study / "freeze.json").exists() else None
    cells = frozen["cells"] if frozen else []
    inventory = []
    payloads = {}
    for run_id, record in records.items():
        directory = study / "runs" / run_id
        for name, digest in record.get("evidence", {}).items():
            require(
                not Path(name).is_absolute() and ".." not in Path(name).parts, "unsafe evidence reference"
            )
            require(file_hash(directory / name, deadline) == digest, "collected evidence changed")
        if record["status"] == "complete":
            certificates = [read_json(p) for p in directory.glob("*/verification.json")]
            require(len(certificates) == record["workers"], "missing partition verification")
            require(
                sum(v["original_rows"] for v in certificates) == record["original_rows"],
                "original denominator differs",
            )
            require(
                sum(v["credited_bytes"] for v in certificates) == record["credited_bytes"],
                "credited byte total differs",
            )
            require(
                math.isclose(
                    record["goodput_Bps"], record["credited_bytes"] / record["elapsed_s"], rel_tol=1e-12
                ),
                "primary timing arithmetic differs",
            )
            merged = {}
            for certificate in certificates:
                require(not (set(merged) & set(certificate["payloads"])), "partition overlap")
                merged.update(certificate["payloads"])
            payloads[run_id] = merged
    for cell in cells:
        record = records.get(cell["run_id"])
        inventory.append({**cell, **(record or {"status": "not_started"})})
    comparisons = {}
    for family in ("E1", "E2", "E3"):
        for method in METHODS[:2]:
            pairs = []
            changed = []
            selected = [r for r in inventory if r["family"] == family and r["method"] == method]
            for r in selected:
                other = next(
                    (
                        x
                        for x in inventory
                        if x["family"] == family and x["block"] == r["block"] and x["method"] == METHODS[2]
                    ),
                    None,
                )
                if r["status"] != "complete" or not other or other["status"] != "complete":
                    continue
                require(
                    file_hash(study / "runs" / r["run_id"] / "input.parquet")
                    == file_hash(study / "runs" / other["run_id"] / "input.parquet"),
                    "paired work differs",
                )
                a, b = payloads[r["run_id"]], payloads[other["run_id"]]
                differences = [key for key in a.keys() & b.keys() if a[key]["sha256"] != b[key]["sha256"]]
                if differences:
                    changed.append(dict(block=r["block"], rows=len(differences)))
                    continue
                pairs.append((r["goodput_Bps"], other["goodput_Bps"]))
            comparisons[family + ":" + method] = {**contrast(pairs), "changed_content_blocks": changed}
    primary = [comparisons["E1:" + m] for m in METHODS[:2]]
    if all(p["p_greater"] is not None for p in primary):
        ordered = sorted(primary, key=lambda r: r["p_greater"])
        floor = 0.0
        for index, entry in enumerate(ordered):
            floor = max(floor, min(1.0, (2 - index) * entry["p_greater"]))
            entry["p_holm"] = floor
    groups = []
    for family in ("E1", "E2", "E3"):
        for method in METHODS:
            rows = [r for r in inventory if r["family"] == family and r["method"] == method]
            complete = [r for r in rows if r["status"] == "complete"]
            groups.append(
                dict(
                    family=family,
                    method=method,
                    planned=len(rows),
                    complete=len(complete),
                    mean_goodput_MBps=statistics.mean(r["goodput_Bps"] / 1e6 for r in complete)
                    if complete
                    else None,
                    mean_elapsed_s=statistics.mean(r["elapsed_s"] for r in complete) if complete else None,
                    mean_coverage=statistics.mean(r["coverage"] for r in complete) if complete else None,
                    mean_job_concurrency=statistics.mean(r["mean_job_concurrency"] for r in complete)
                    if complete
                    else None,
                )
            )
    result = dict(
        schema="flowdc-minimum-measured-analysis-v1",
        utc=utc(),
        evaluation_frozen=frozen is not None,
        planned_jobs=len(cells),
        complete_jobs=sum(r["status"] == "complete" for r in inventory),
        engineering_and_calibration_jobs=sum(r.get("phase") != "evaluation" for r in records.values()),
        groups=groups,
        comparisons=comparisons,
        inventory=inventory,
        limitations=[
            "Only retained first attempts; incomplete cases excluded from complete-pair intervals and remain visible.",
            "Two AWS-hosted collections, not independent infrastructures; no injected capacity changes.",
            "Matched TaskVine sharding, not img2dataset native Ray/Spark comparison.",
            "Intervals conditional on small-sample log-ratio assumptions; no causal mechanism or universal claim.",
            "Common concurrency integrates connection-start through body/failure end over the entire manager job.",
        ],
    )
    destination = study / "analysis"
    destination.mkdir(exist_ok=True)
    replace_derived(destination / "results.json", result)
    with (destination / "groups.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(groups[0]))
        writer.writeheader()
        writer.writerows(groups)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("study", type=Path)
    args = parser.parse_args()
    result = summarize(args.study)
    print(
        json.dumps(
            {
                k: result[k]
                for k in ("evaluation_frozen", "planned_jobs", "complete_jobs", "groups", "comparisons")
            }
        )
    )
