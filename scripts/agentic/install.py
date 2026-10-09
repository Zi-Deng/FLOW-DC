#!/usr/bin/env python3
"""Install the single current workflow; do not overwrite existing project files."""

import argparse
import shutil
from pathlib import Path

from workflow import ROOT, WorkflowError


def payload(root=ROOT):
    root = Path(root)
    directories = ["scripts/agentic", ".agents/skills", ".agentic", "docs/agent-workflow"]
    result = [
        p.relative_to(root)
        for d in directories
        for p in (root / d).rglob("*")
        if p.is_file() and "__pycache__" not in p.parts
    ]
    result += [
        Path(p)
        for p in [
            "scripts/check_repository.py",
            "scripts/new-task.sh",
            "scripts/finish-task.sh",
            "Makefile",
            "requirements-dev.txt",
            "pyproject.toml",
            ".github/workflows/agentic-quality.yml",
        ]
    ]
    return sorted(set(result))


def install(source, target):
    source, target = Path(source), Path(target)
    files = payload(source)
    conflicts = [str(p) for p in files if (target / p).exists()]
    if conflicts:
        raise WorkflowError(
            "Existing files require explicit project reconciliation: " + ", ".join(conflicts[:10])
        )
    for name in files:
        destination = target / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source / name, destination)
    return len(files)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("target", type=Path)
    args = parser.parse_args()
    print(f"Installed {install(ROOT, args.target)} current workflow files")
