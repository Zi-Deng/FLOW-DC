#!/usr/bin/env python3
"""Check current skills, configuration, CI and documentation links."""

import json
import re
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
    paths = [
        ROOT / "README.md",
        ROOT / "AGENTS.md",
        *(ROOT / "docs/agent-workflow").glob("*.md"),
        *(ROOT / ".agents/skills").glob("*/SKILL.md"),
    ]
    for p in paths:
        for link in re.findall(r"\[[^\]]*\]\(([^)]+)\)", p.read_text()):
            if re.match(r"[a-z]+://|mailto:|#", link):
                continue
            target = unquote(link.split("#", 1)[0].strip("<>"))
            assert not target or (p.parent / target).exists(), f"{p}: missing {target}"
    runtime = sum(len(p.read_text().splitlines()) for p in (ROOT / "scripts/agentic").glob("*.py"))
    tests = sum(len(p.read_text().splitlines()) for p in (ROOT / "tests/agentic").glob("*.py"))
    assert runtime <= 2500 and tests <= 1200, "Workflow growth exceeded the maintainer budget"
    print(
        f"Current config, eight skills, CI, links and size budgets verified: {runtime}runtime/{tests}test lines"
    )


if __name__ == "__main__":
    main()
