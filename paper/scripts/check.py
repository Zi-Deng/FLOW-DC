#!/usr/bin/env python3
"""Check the manuscript artifact, not scientific completion of its experiments."""

import argparse
import hashlib
import json
import math
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def require(ok, message):
    if not ok:
        raise SystemExit(message)


def close(a, b):
    return math.isclose(a, b, rel_tol=1e-10, abs_tol=1e-8)


def main():
    evidence = json.loads((ROOT / "provenance/evidence.json").read_text())
    for name, record in evidence["inputs"].items():
        require(
            hashlib.sha256((ROOT / "data" / name).read_bytes()).hexdigest() == record["packaged_sha256"],
            "packaged numerical input differs from its evidence binding",
        )
    parser = argparse.ArgumentParser()
    parser.parse_args()
    m = json.loads((ROOT / "data/measured.json").read_text())
    require(len(m["cvdf"]) == 12, "Expected four CVDF blocks for each of three methods")
    for i in range(4):
        rows = [r for r in m["cvdf"] if r["index"] == i]
        require(len(rows) == 3 and len({r["label"] for r in rows}) == 3, "Invalid matched block")
        require(len({r["input_sha256"] for r in rows}) == 1, "Mismatched input hash")
        require(len({r["credited_bytes"] for r in rows}) == 1, "Mismatched credited bytes")
        for r in rows:
            require(r["coverage"] == 1 and r["evidence_kind"] == "measured", "Invalid measured CVDF scope")
            require(close(r["credited_bytes"] / r["elapsed_s"], r["goodput_Bps"]), "CVDF goodput arithmetic")
            require(close(r["native_seconds"] + r["verification_s"], r["elapsed_s"]), "CVDF timing sum")
    author = json.loads((ROOT / "data/author-concurrency.json").read_text())
    supplied = {r["concurrency_limit"]: r for r in author["conditions"]}
    require(set(supplied) == {96, 384}, "Author configuration inventory differs")
    require(
        supplied[96]["goodput_MBps"] == [6.12, 6.33, 6.74, 6.26]
        and supplied[96]["verified_by_author"] is True,
        "Author measurements altered",
    )
    require(
        supplied[384]["status"] == "operator_stopped" and supplied[384]["reported_run_count"] == 5,
        "Stopped author outcomes must remain five stopped runs",
    )
    require(
        supplied[384]["goodput_MBps"] is None and supplied[384]["elapsed_s"] is None,
        "Stopped author runs gained completion metrics",
    )
    require(
        supplied[384]["mean_active"] is None
        and supplied[384]["observed_simultaneous_requests_approx"] == 2
        and supplied[384]["http_errors_reported"] is False
        and supplied[384]["server_rate_limit_confirmed"] is False,
        "Author slowdown interpretation changed",
    )
    summaries = json.loads((ROOT / "data/summaries.json").read_text())
    require(
        close(summaries["author_concurrency"]["c96_mean_MBps"], 6.3625)
        and summaries["author_concurrency"]["pooled_with_matched_comparison"] is False,
        "Author summary arithmetic or machine grouping differs",
    )
    table = (ROOT / "sections/cvdf-table.tex").read_text()
    require(
        "96 & 6.36 & N/A & N/A & N/A" in table and "384 & N/A & N/A & N/A" in table,
        "Unavailable table values were imputed",
    )
    require("multicolumn" not in table, "Obsolete machine-group table headings")
    status = [r["status"] for r in m["taibif_validation"]]
    require(
        len(status) == 12
        and status.count("complete") == 3
        and status.count("stopped") == 2
        and status.count("no_candidate") == 4
        and status.count("unstarted") == 3,
        "TaiBIF inventory altered",
    )
    for r in m["taibif_validation"]:
        if r["status"] == "complete":
            require(
                close(r["verification"]["credited_bytes"] / r["elapsed_s"], r["goodput_Bps"]),
                "TaiBIF arithmetic",
            )
        else:
            require(r.get("goodput_Bps") is None, "Incomplete run gained a complete-run goodput")
    for r in m["taibif_timing"]:
        require(
            close(r["native_seconds"] + r["postprocessing_seconds"], r["total_seconds"]),
            "Timing reconciliation",
        )
        require(close(r["credited_bytes"] / r["total_seconds"] / 1e6, r["end_to_end_MBps"]), "Timing goodput")
        require(
            close(r["credited_bytes"] / r["network_span_s"] / 1e6, r["full_http_interval_MBps"]),
            "HTTP interval goodput",
        )
    engineering = json.loads((ROOT / "data/engineering-campaign.json").read_text())
    require(engineering["collected_analysis_verified"] is True, "Unverified engineering collection")
    for r in engineering["records"]:
        require(r["phase"] == "engineering", "Unexpected efficacy relabeling")
        if r["status"] == "complete":
            require(close(r["credited_bytes"] / r["elapsed_s"], r["goodput_Bps"]), "Engineering arithmetic")
            require(len(r["participants"]) == r["workers"], "Missing worker participation")
        else:
            require(r.get("goodput_Bps") is None, "Stopped engineering gained goodput")
    src = (ROOT / "FLOW-DC.tex").read_text()
    body = src + "\n" + "\n".join(p.read_text() for p in (ROOT / "sections").glob("*.tex"))
    require(
        not re.search(
            r"Lorem ipsum|TODO|TBD|PLACEHOLDER|Ratio-v2|Gradient-v2|Tuned Fixed|Gradient2", body, re.I
        ),
        "Obsolete label or placeholder in main article",
    )
    abstract = src.split(r"\begin{abstract}")[1].split(r"\end{abstract}")[0]
    require(150 <= len(abstract.split()) <= 250, "Abstract length outside 150--250 words")
    require(
        not re.search(r"\\(?:cite|footnote|gen)\b", abstract),
        "Abstract has citation, footnote or generated result",
    )
    keys = set(re.findall(r"@\w+\{([^,]+),", (ROOT / "references.bib").read_text()))
    original = set(json.loads((ROOT / "provenance/original-reference-keys.json").read_text()))
    require(len(original) == 29 and original <= keys, "Original bibliography work missing")
    citations = set()
    for block in re.findall(r"\\cite\{([^}]+)\}", body):
        citations.update(block.split(","))
    require(citations == keys, "Uncited or unresolved bibliography entry")
    require(len(keys) == 43, "Unexpected reference count")
    figures = json.loads((ROOT / "figures/manifest.json").read_text())
    require(len(figures) == 8, "Figure inventory incomplete")
    for r in figures:
        p = ROOT / r["file"]
        require(p.exists(), "Missing figure")
        require(hashlib.sha256(p.read_bytes()).hexdigest() == r["sha256"], "Figure checksum mismatch")
        for i in r["inputs"]:
            require((ROOT / i).exists(), "Missing figure input")
        require(
            r["evidence_kind"]
            in ("conceptual", "measured", "measured-engineering", "measured-and-author-reported"),
            "Unclassified performance figure",
        )
        if r["evidence_kind"] == "measured-and-author-reported":
            require(
                r["file"] == "figures/cvdf-comparison.pdf" and "data/author-concurrency.json" in r["inputs"],
                "Missing author-reported figure provenance",
            )
    results = {}
    for name in ["FLOW-DC", "supplement"]:
        log = (ROOT / f"build/{name}.log").read_text()
        require(
            not re.search(r"undefined|multiply defined|Overfull|^!", log, re.M),
            f"LaTeX unresolved or overflow: {name}",
        )
        pdf = ROOT / f"{name}.pdf"
        info = subprocess.check_output(["pdfinfo", str(pdf)], text=True)
        n = int(re.search(r"Pages:\s+(\d+)", info).group(1))
        results[name] = n
        text = subprocess.check_output(["pdftotext", str(pdf), "-"], text=True)
        require("prototype" not in text.lower(), f"Unexpected prototype banner: {name}")
        require(
            "Generated scenario; not an experimental measurement." not in text,
            f"Obsolete scenario remains: {name}",
        )
        require(
            "6.36" in text and "384" in text and "different machine" in text.lower(),
            f"Additional observations or machine distinction missing: {name}",
        )
        if name == "FLOW-DC":
            require(text.lower().count("different machine") == 1, "Expected one concise machine disclosure")
    require(results["FLOW-DC"] <= 12, "Main manuscript exceeds 12 pages")
    require(
        len(re.findall(r"\\bibitem\{", (ROOT / "build/FLOW-DC.bbl").read_text())) == 43,
        "Rendered references incomplete",
    )
    # Recompute paired intervals from run-level inputs independently of plotting.
    import statistics

    from scipy.stats import t

    reference = {int(r["index"]): r for r in m["cvdf"] if r["label"] == "img2dataset"}
    for item in summaries["cvdf_contrasts"]:
        logs = [
            math.log(r["goodput_Bps"] / reference[int(r["index"])]["goodput_Bps"])
            for r in m["cvdf"]
            if r["label"] == item["method"]
        ]
        mean = statistics.mean(logs)
        half = float(t.ppf(0.975, len(logs) - 1)) * statistics.stdev(logs) / math.sqrt(len(logs))
        for key, value in [
            ("geometric_ratio", math.exp(mean)),
            ("lower", math.exp(mean - half)),
            ("upper", math.exp(mean + half)),
        ]:
            require(close(item[key], value), "Paired interval differs: " + key)
    report = dict(
        status="passed",
        pages=results,
        references=43,
        original_references_retained=29,
        figures=8,
        measured_cvdf_runs=12,
        taibif_validation_dispositions=12,
        author_reported_c96_measurements=4,
        author_reported_c384_stopped=5,
        engineering_attempts=len(engineering["records"]),
        engineering_completed=sum(r["status"] == "complete" for r in engineering["records"]),
        scope="Artifact arithmetic, structure, provenance and typesetting; no new acquisition or raw payload verification.",
    )
    (ROOT / "provenance/validation.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
