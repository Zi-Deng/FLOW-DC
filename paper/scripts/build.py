#!/usr/bin/env python3
"""Build and check the current paper from explicit source inputs in an isolated tree."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SECTIONS = ["introduction", "background", "architecture", "evaluation", "applications", "engineering-results"]
INPUTS = [
    "FLOW-DC.tex",
    "supplement.tex",
    "references.bib",
    "requirements.txt",
    "requirements-lock.txt",
    "claim-evidence.csv",
    "provenance/original-reference-keys.json",
    "provenance/evidence.json",
    *[f"sections/{s}.tex" for s in SECTIONS],
    *[
        f"data/{s}"
        for s in [
            "measured.json",
            "engineering-campaign.json",
            "author-concurrency.json",
            "run-inventory.csv",
            "experiment-status.csv",
        ]
    ],
    *[
        f"scripts/{s}"
        for s in ["build.py", "generate.py", "engineering_campaign.py", "check.py", "export_single_file.py"]
    ],
    "Makefile",
    "README.md",
]


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def execute(command, cwd):
    subprocess.run(command, cwd=cwd, check=True)


def stage(destination):
    destination.mkdir(parents=True)
    for name in INPUTS:
        target = destination / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / name, target)
    (destination / "figures").mkdir()


def compile_paper(work):
    execute([sys.executable, "scripts/generate.py"], work)
    for name in ["FLOW-DC", "supplement"]:
        execute(
            [
                "latexmk",
                "-norc",
                "-pdf",
                "-interaction=nonstopmode",
                "-halt-on-error",
                "-no-shell-escape",
                "-outdir=build",
                name + ".tex",
            ],
            work,
        )
        shutil.copy2(work / f"build/{name}.pdf", work / f"{name}.pdf")
    execute([sys.executable, "scripts/check.py"], work)
    execute([sys.executable, "scripts/export_single_file.py"], work)
    # LuaLaTeX compiles only the supplied standalone source, without companion files.
    standalone = work / "standalone"
    standalone.mkdir()
    shutil.copy2(work / "FLOW-DC-single-file.tex", standalone / "FLOW-DC-single-file.tex")
    execute(
        [
            "latexmk",
            "-norc",
            "-lualatex",
            "-interaction=nonstopmode",
            "-halt-on-error",
            "-no-shell-escape",
            "FLOW-DC-single-file.tex",
        ],
        standalone,
    )
    check_standalone(work, standalone)


def check_standalone(work, standalone):
    """Check compiled reference numbers, page count and extracted figure bytes."""
    name = "FLOW-DC-single-file"
    log = (standalone / (name + ".log")).read_text()
    if re.search(r"undefined|multiply defined|Overfull|^!", log, re.M):
        raise ValueError("Standalone article has unresolved references or typesetting errors")
    modular_aux = (work / "build/FLOW-DC.aux").read_text()
    standalone_aux = (standalone / (name + ".aux")).read_text()
    for pattern in (r"\\bibcite\{([^{}]+)\}\{(\d+)\}", r"\\newlabel\{([^{}]+)\}\{\{([^{}]*)\}"):
        expected, actual = [dict(re.findall(pattern, text)) for text in (modular_aux, standalone_aux)]
        if not expected or expected != actual:
            raise ValueError("Standalone citation/reference numbering differs from the modular article")

    def pages(path):
        info = subprocess.check_output(["pdfinfo", str(path)], text=True)
        return int(re.search(r"Pages:\s+(\d+)", info).group(1))

    if pages(work / "FLOW-DC.pdf") != pages(standalone / (name + ".pdf")):
        raise ValueError("Standalone page count differs from the modular article")
    assets = sorted((standalone / (name + "-assets")).glob("*.pdf"))
    if len(assets) != 7 or any(sha(p) != sha(work / "figures" / p.name) for p in assets):
        raise ValueError("Standalone figures differ from the modular article")
    (work / "provenance/standalone-validation.json").write_text(
        json.dumps(
            {
                "status": "passed",
                "pages": pages(work / "FLOW-DC.pdf"),
                "figures": len(assets),
                "reference_numbering_matches": True,
                "scope": "Fresh inline source; matching compiled reference numbers, pages and exact figure bytes",
            },
            indent=2,
        )
        + "\n"
    )


def build(output):
    output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="flowdc-paper-") as temp:
        work = Path(temp) / "source"
        stage(work)
        # Bind packaging and receipts to the exact staged originals, before generation.
        inputs = {name: (work / name).read_bytes() for name in INPUTS}
        compile_paper(work)
        if any((ROOT / name).read_bytes() != raw for name, raw in inputs.items()):
            raise ValueError("Manuscript inputs changed during the build; outputs were not published")
        for name in ["FLOW-DC.pdf", "supplement.pdf", "FLOW-DC-single-file.tex"]:
            shutil.copy2(work / name, output / name)
        shutil.copy2(work / "standalone/FLOW-DC-single-file.pdf", output / "FLOW-DC-single-file.pdf")
        shutil.copytree(work / "figures", output / "figures", dirs_exist_ok=True)
        (output / "data").mkdir(exist_ok=True)
        shutil.copy2(work / "data/summaries.json", output / "data/summaries.json")
        (output / "tables").mkdir(exist_ok=True)
        for name in ("numbers.tex", "cvdf-table.tex", "engineering-status.tex"):
            shutil.copy2(work / "sections" / name, output / "tables" / name)
        shutil.copytree(work / "build", output / "logs", dirs_exist_ok=True)
        shutil.copy2(work / "provenance/validation.json", output / "validation.json")
        shutil.copy2(work / "provenance/standalone-validation.json", output / "standalone-validation.json")
        with zipfile.ZipFile(output / "FLOW-DC-source.zip", "w", zipfile.ZIP_DEFLATED) as archive:
            for name in sorted(INPUTS):
                info = zipfile.ZipInfo(name, date_time=(2026, 1, 1, 0, 0, 0))
                info.compress_type = zipfile.ZIP_DEFLATED
                archive.writestr(info, inputs[name])
        receipt = {
            "status": "passed",
            "inputs": {name: hashlib.sha256(raw).hexdigest() for name, raw in inputs.items()},
            "outputs": {
                name: sha(output / name)
                for name in [
                    "FLOW-DC.pdf",
                    "supplement.pdf",
                    "FLOW-DC-single-file.tex",
                    "FLOW-DC-single-file.pdf",
                    "FLOW-DC-source.zip",
                    "standalone-validation.json",
                    "data/summaries.json",
                    "figures/manifest.json",
                    "tables/numbers.tex",
                    "tables/cvdf-table.tex",
                    "tables/engineering-status.tex",
                ]
            },
            "scope": "Current source and packaged measured summaries; no new image acquisition",
        }
        (output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")


def reproduce():
    with tempfile.TemporaryDirectory(prefix="flowdc-paper-reproduction-") as temp:
        a, b, extracted = [Path(temp) / n for n in ["first", "second", "extracted"]]
        build(a)
        build(b)
        for name in ["FLOW-DC.pdf", "supplement.pdf", "FLOW-DC-single-file.tex", "FLOW-DC-source.zip"]:
            if sha(a / name) != sha(b / name):
                raise ValueError(f"nondeterministic artifact: {name}")
        with zipfile.ZipFile(a / "FLOW-DC-source.zip") as archive:
            archive.extractall(extracted)
        execute(
            [sys.executable, "scripts/build.py", "all", "--build-dir", str(Path(temp) / "zip-output")],
            extracted,
        )
        if sha(a / "FLOW-DC.pdf") != sha(Path(temp) / "zip-output/FLOW-DC.pdf"):
            raise ValueError("source ZIP does not reproduce the current article")
    print("Two isolated builds and the source ZIP reproduced the current paper.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("target", choices=["all", "check", "single-file", "source", "reproduce"])
    parser.add_argument("--build-dir", type=Path, default=ROOT.parent / "build/paper")
    args = parser.parse_args()
    if args.target == "reproduce":
        reproduce()
    else:
        build(args.build_dir.resolve())


if __name__ == "__main__":
    main()
