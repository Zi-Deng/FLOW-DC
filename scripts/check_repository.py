#!/usr/bin/env python3
"""Check current skills, configuration, CI and documentation links."""

import json
import re
import subprocess
import sys
from pathlib import Path
from urllib.parse import unquote

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts/agentic"))
from workflow import configuration  # noqa: E402


def validate_skills(root):
    skills = list((root / ".agents/skills").glob("*/SKILL.md"))
    assert len(skills) == 8
    for path in skills:
        text = path.read_text()
        assert text.startswith("---\n")
        front, _, body = text[4:].partition("\n---\n")
        fields = yaml.safe_load(front)
        assert fields["name"] == path.parent.name and fields["description"] and body.strip()
        meta = yaml.safe_load((path.parent / "agents/openai.yaml").read_text())
        assert "$" + fields["name"] in meta["interface"]["default_prompt"]
        assert meta.get("policy", {}).get("allow_implicit_invocation", True) is True


def main():
    validate_skills(ROOT)
    config = configuration(ROOT)
    assert json.loads((ROOT / ".agentic/schemas/review-report.json").read_text())["type"] == "object"
    jobs = []
    for p in (ROOT / ".github/workflows").glob("*.yml"):
        value = yaml.load(p.read_text(), Loader=yaml.BaseLoader)
        assert "pull_request_target" not in value["on"] and value["permissions"]["contents"] == "read"
        for name, job in value["jobs"].items():
            assert "timeout-minutes" in job
            if name in config["required_checks"]:
                jobs.append(name)
            for step in job.get("steps", []):
                if "uses" in step:
                    assert re.fullmatch(r"[^@]+@[0-9a-f]{40}", step["uses"])
                    if step["uses"].startswith("actions/checkout@"):
                        assert step["with"]["persist-credentials"] == "false"
    assert sorted(jobs) == sorted(config["required_checks"])
    names = (
        subprocess.check_output(
            ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=ROOT
        )
        .decode()
        .split("\0")
    )
    files = sorted({ROOT / n for n in names if n and (ROOT / n).is_file()})
    paths = [p for p in files if p.suffix == ".md" and p.name != "TASK_SUMMARY.md"]
    for p in paths:
        for link in re.findall(r"\[[^\]]*\]\(([^)]+)\)", p.read_text()):
            if re.match(r"[a-z]+://|mailto:|#", link):
                continue
            target = unquote(link.split("#", 1)[0].strip("<>"))
            assert not target or (p.parent / target).exists(), f"{p}: missing {target}"
    for path in files:
        name = path.relative_to(ROOT).as_posix()
        assert not name.startswith(
            ("memory/", "build/", "dist/", "runs/", "extra-files/", ".agentic-local/")
        ), name
        assert path.suffix not in (".pdf", ".zip", ".whl", ".pyc", ".tar", ".gz"), f"generated output: {name}"
        if path.name == "TASK_SUMMARY.md":
            continue  # Exact original prompt and exhaustive path inventory are intentional.
        try:
            text = path.read_text()
        except UnicodeError:
            continue
        assert not re.search(r"/(?:home/zi|mnt/storage/github|mnt/storage/flowdc-memory-data)/", text), (
            f"private path: {name}"
        )
        assert not re.search(
            r"(?:gh[pousr]_[A-Za-z0-9]{30,}|sk-ant-[A-Za-z0-9_-]{20,}|-----BEGIN (?:OPENSSH|RSA|EC) PRIVATE KEY-----)",
            text,
        ), f"credential pattern: {name}"
        if name.startswith(("src/", "examples/", "docs/", "paper/")):
            assert not re.search(
                r"(?:bin/(?:download_batch|TaskvineFLOWDC|flowdc_experiment)|requirements-dev\.txt)", text
            ), f"stale interface: {name}"
    for directory in ("bin", "benchmark", "playground", "files", "jupyter", "archives"):
        assert not (ROOT / directory).exists(), f"retired tree remains: {directory}"
    from importlib.util import module_from_spec, spec_from_file_location

    spec = spec_from_file_location("paper_build_inputs", ROOT / "paper/scripts/build.py")
    paper = module_from_spec(spec)
    spec.loader.exec_module(paper)
    assert all((ROOT / "paper" / name).is_file() for name in paper.INPUTS), "missing paper input"
    assert not (ROOT / "AGENT.md").exists(), "use the existing AGENTS.md"
    runtime = sum(len(p.read_text().splitlines()) for p in (ROOT / "scripts/agentic").glob("*.py"))
    tests = sum(len(p.read_text().splitlines()) for p in (ROOT / "tests/agentic").glob("*.py"))
    assert runtime <= 2500 and tests <= 1200, "Workflow growth exceeded the maintainer budget"
    print(
        f"Current config, eight skills, CI, links and size budgets verified: {runtime}runtime/{tests}test lines"
    )


if __name__ == "__main__":
    main()
