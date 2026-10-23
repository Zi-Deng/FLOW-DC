"""Transfer-study analysis: all first attempts and ordinary paired intervals."""

import argparse
import csv
import json
import math
import statistics
import time
from pathlib import Path

from .common import file_hash, read_json, require, replace_derived, utc
from .minimum_study import METHODS, SOURCES, TARGETS


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



def paired_difference(values):
    if not values:
        return dict(mean=None, interval95=None, blocks=0)
    mean = statistics.mean(values)
    interval = None
    if len(values) > 1:
        from scipy.stats import t
        width = float(t.ppf(.975, len(values)-1)) * statistics.stdev(values) / math.sqrt(len(values))
        interval = [mean-width, mean+width]
    return dict(mean=mean, interval95=interval, blocks=len(values))


def summarize(study):
    study = Path(study)
    deadline = time.monotonic() + 1200
    records = {p.parent.name: read_json(p) for p in (study / "runs").glob("*/record.json")}
    frozen = read_json(study / "freeze.json") if (study / "freeze.json").exists() else None
    cells = frozen["cells"] if frozen else []
    payloads = {}
    for run_id, record in records.items():
        directory = study / "runs" / run_id
        for name, digest in record.get("evidence", {}).items():
            require(not Path(name).is_absolute() and ".." not in Path(name).parts, "unsafe evidence reference")
            require(file_hash(directory / name, deadline) == digest, "collected evidence changed")
        certificates = [read_json(p) for p in directory.glob("*/verification.json")]
        if record.get("credited_bytes") is not None:
            require(len(certificates) == record["workers"], "missing partition verification")
            require(sum(v["original_rows"] for v in certificates) == record["original_rows"], "original denominator differs")
            require(sum(v["credited_bytes"] for v in certificates) == record["credited_bytes"], "credited bytes differ")
            require(math.isclose(sum(v["credited_rows"] for v in certificates) / record["original_rows"], record["coverage"]), "coverage differs")
            merged = {}
            for certificate in certificates:
                require(not set(merged).intersection(certificate["payloads"]), "partition overlap")
                merged.update(certificate["payloads"])
            payloads[run_id] = merged
            if record["status"] == "complete":
                require(math.isclose(record["goodput_Bps"], record["credited_bytes"] / record["elapsed_s"], rel_tol=1e-12), "primary timing differs")
            else:
                require(record.get("goodput_Bps") is None, "partial run cannot have complete-run goodput")
    inventory = [{**cell, **records.get(cell["run_id"], dict(status="not_started"))} for cell in cells]
    comparisons, groups = {}, []
    for source in TARGETS:
        subset = [r for r in inventory if r["source"] == source]
        for method in METHODS[:2]:
            for reference in ("transferred", "target_calibrated"):
                pairs, changed, coverage = [], [], []
                for row in [r for r in subset if r["method"] == method]:
                    other = next((r for r in subset if r.get("block") == row["block"] and
                                  (r.get("arm") == reference or reference in r.get("reference_aliases", []))), None)
                    if row["status"] != "complete" or not other or other["status"] != "complete":
                        continue
                    a_dir, b_dir = study / "runs" / row["run_id"], study / "runs" / other["run_id"]
                    require(file_hash(a_dir / "input.parquet") == file_hash(b_dir / "input.parquet"), "matched work differs")
                    a, b = payloads[row["run_id"]], payloads[other["run_id"]]
                    differences = [k for k in a.keys() & b.keys() if a[k]["sha256"] != b[k]["sha256"]]
                    if differences:
                        changed.append(dict(block=row["block"], rows=len(differences)))
                        continue
                    pairs.append((row["goodput_Bps"], other["goodput_Bps"]))
                    coverage.append(row["coverage"] - other["coverage"])
                comparisons[source + ":" + method + ":" + reference] = {
                    **contrast(pairs), "changed_content_blocks": changed,
                    "coverage_difference": paired_difference(coverage), "all_six_pairs_complete": len(pairs) == 6}
        for arm in ("ratio", "gradient", "transferred", "target_calibrated"):
            selected = [r for r in subset if r.get("arm") == arm or arm in r.get("reference_aliases", [])]
            completed = [r for r in selected if r["status"] == "complete"]
            groups.append(dict(source=source, arm=arm, planned=len(selected),
                complete=len(completed), stopped=sum(r["status"] == "stopped" for r in selected),
                mean_goodput_MBps=statistics.mean(r["goodput_Bps"] / 1e6 for r in completed) if completed else None,
                mean_elapsed_s=statistics.mean(r["elapsed_s"] for r in completed) if completed else None,
                mean_coverage=statistics.mean(r["coverage"] for r in completed) if completed else None))
    # Keep all four planned hypotheses in the correction, including unavailable
    # sources/pairs. Missing evidence is not a reason to reduce multiplicity.
    primary = [comparisons[s + ":" + m + ":transferred"] for s in TARGETS for m in METHODS[:2]]
    ordered = sorted(primary, key=lambda r: r["p_greater"] if r["p_greater"] is not None else 1)
    floor = 0.0
    for i, entry in enumerate(ordered):
        raw = entry["p_greater"] if entry["p_greater"] is not None else 1
        floor = max(floor, min(1, (4-i) * raw))
        entry["p_holm"] = floor if entry["p_greater"] is not None else None
        ci = entry["coverage_difference"]["interval95"]
        entry["practical_speed_claim_supported"] = bool(entry["all_six_pairs_complete"] and
            entry["ratio"] >= 1.1 and entry["p_holm"] < .05 and ci and ci[0] >= -.01)
    dispositions = {p.stem: read_json(p) for p in (study / "gates").glob("*-disposition.json")}
    # A stop before selection must not make the unstarted study disappear as
    # "zero planned jobs". These are design maxima, not invented frozen cells.
    scope = []
    for source in SOURCES:
        for phase, maximum in (("sizing", 3), ("calibration", 12 if source == SOURCES[0] else 4),
                               ("evaluation", 0 if source == SOURCES[0] else 24)):
            if not maximum:
                continue
            observed = [r for r in records.values() if r["source"] == source and r.get("phase") == phase]
            scope.append(dict(source=source, phase=phase, maximum_jobs=maximum,
                attempted=len(observed), complete=sum(r["status"] == "complete" for r in observed),
                stopped_or_failed=sum(r["status"] != "complete" for r in observed),
                status="not_started" if not observed else "attempted"))
    result = dict(schema="flowdc-adaptive-transfer-analysis-v1", utc=utc(),
        evaluation_frozen=frozen is not None, planned_jobs=len(cells),
        scientific_status=("not_frozen" if frozen is None else
            "complete" if cells and all(r["status"] == "complete" for r in inventory) else "incomplete"),
        declared_scope=scope, attempted_jobs=len(records),
        complete_jobs=sum(r["status"] == "complete" for r in inventory), groups=groups,
        comparisons=comparisons, inventory=inventory,
        development_and_calibration=[r for r in records.values() if r.get("phase") != "evaluation"],
        source_dispositions=dispositions,
        limitations=["First attempts retained, stopped/unknown cases absent from complete-run intervals.",
                     "Six temporal blocks on a repeated manifest; remote caches and background traffic are uncontrolled.",
                     "Ordinary95% intervals assume independent normal run-level log ratios; primary p values use Holm across four contrasts.",
                     "No guaranteed throttling avoidance, optimal configuration or native Ray/Spark scaling claim."])
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
    print(json.dumps({k: result[k] for k in ("evaluation_frozen", "planned_jobs", "complete_jobs", "groups", "comparisons", "source_dispositions")}))
