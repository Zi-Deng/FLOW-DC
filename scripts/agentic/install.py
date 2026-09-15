#!/usr/bin/env python3
"""Preview or install workflow files without overwriting an existing project."""

import argparse
import hashlib
import json
import os
import stat
import sys
from pathlib import Path

from tasks import atomic_json
from workflow import WorkflowError

ROOT = Path(__file__).resolve().parents[2]
PATHS = [
    ".agentic",
    ".agents/skills",
    ".github/ISSUE_TEMPLATE/change.yml",
    ".github/pull_request_template.md",
    ".github/agents/independent-reviewer.agent.md",
    ".github/copilot-instructions.md",
    ".github/workflows/agentic-quality.yml",
    ".github/workflows/copilot-review.yml",
    "scripts/agentic",
    "scripts/new-task.sh",
    "scripts/cleanup-task.sh",
    "scripts/finish-task.sh",
    "tests/agentic",
    "docs/agent-workflow",
    "AGENTS.md",
]

EXCLUDED = {
    "__pycache__",
    ".agentic-local",
    "memory",
    "archives",
    "sessions",
    ".venv",
    ".pytest_cache",
    ".ruff_cache",
    ".mypy_cache",
    "data",
    "checkpoints",
    "weights",
    "wandb",
}


def payload(source):
    result = []

    def visit(entry):
        relative = entry.relative_to(source)
        if EXCLUDED.intersection(relative.parts) or entry.suffix in {".pyc", ".pyo"}:
            return
        if relative.as_posix() == ".agentic/template-origin.json":
            return
        if entry.is_symlink():
            raise WorkflowError(f"Source symlink is not supported: {entry}")
        if entry.is_dir():
            for child in sorted(entry.iterdir()):
                visit(child)
        elif entry.is_file():
            result.append(relative)
        else:
            raise WorkflowError(f"Missing or unsupported installer source: {entry}")

    for relative in PATHS:
        path = source / relative
        visit(path)
    return result


def install(source, target, apply=False):
    if ".." in Path(target).parts:
        raise WorkflowError("Installation target must not contain '..' components")
    source, target = Path(source).resolve(), Path(target).absolute()
    if any(path.is_symlink() for path in (target, *target.parents)):
        raise WorkflowError("Installation target must not contain symlink components")
    if target == source or target.is_relative_to(source):
        raise WorkflowError("Install into a separate repository or staging directory")
    files = payload(source)
    contents = {relative: (source / relative).read_bytes() for relative in files}
    modes = {relative: stat.S_IMODE((source / relative).stat().st_mode) for relative in files}
    conflicts, pending, identical = [], [], []
    for relative in files:
        destination = target / relative
        if any(p.is_symlink() for p in [destination, *destination.parents] if p != target.parent):
            conflicts.append(str(relative) + " (symlink)")
        elif any(parent.exists() and not parent.is_dir() for parent in destination.parents):
            conflicts.append(str(relative) + " (non-directory ancestor)")
        elif destination.exists():
            if destination.is_file() and destination.read_bytes() == contents[relative]:
                identical.append(str(relative))
            else:
                conflicts.append(str(relative))
        else:
            pending.append(relative)
    manifest = target / ".agentic/template-origin.json"
    if manifest.is_symlink():
        conflicts.append(".agentic/template-origin.json (symlink)")
    elif manifest.exists():
        try:
            prior = json.loads(manifest.read_text())
            valid = (
                isinstance(prior, dict)
                and set(prior) == {"schema_version", "source", "files"}
                and prior["schema_version"] == 1
                and prior["source"] == "agentic-github-template"
                and isinstance(prior["files"], dict)
            )
        except (OSError, ValueError):
            valid = False
        if not valid:
            conflicts.append(".agentic/template-origin.json (unrecognized existing content)")
    result = {
        "target": str(target),
        "conflicts": conflicts,
        "identical": identical,
        "new_files": [str(p) for p in pending],
        "applied": False,
        "next": (
            "Merge /memory/ and /.agentic-local/ into .gitignore; adapt AGENTS, domain rubric and project CI. "
            "The payload does not install Makefile, requirements-dev.txt, requirements-test.txt, "
            "pyproject.toml or scripts/check_repository.py. Integrate equivalent project validation "
            "and CI targets before running the installed make commands. See docs/agent-workflow/SETUP.md."
        ),
    }
    if apply:
        if conflicts:
            raise WorkflowError(
                "Installation refused before any writes; reconcile these paths: " + ", ".join(conflicts)
            )
        for relative in pending:
            destination = target / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            with destination.open("xb") as stream:
                stream.write(contents[relative])
                stream.flush()
                os.fchmod(stream.fileno(), modes[relative])
                os.fsync(stream.fileno())
        atomic_json(
            manifest,
            {
                "schema_version": 1,
                "source": "agentic-github-template",
                "files": {str(p): hashlib.sha256(contents[p]).hexdigest() for p in files},
            },
        )
        result["applied"] = True
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("target")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    try:
        print(json.dumps(install(ROOT, args.target, args.apply), indent=2))
        return 0
    except (WorkflowError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
