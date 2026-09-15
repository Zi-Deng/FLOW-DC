#!/usr/bin/env python3
"""Validate FLOW-DC workflow configuration, skills and maintained workflow links."""

import json
import re
import sys
from pathlib import Path
from urllib.parse import unquote

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts/agentic"))
from workflow import configuration  # noqa: E402

SKILLS = {
    "agentic-workflow",
    "agentic-capture",
    "agentic-plan",
    "agentic-prepare",
    "agentic-implement",
    "agentic-review",
    "agentic-repair",
    "agentic-finish",
}


def validate_skills(root):
    for name in sorted(SKILLS):
        directory = root / ".agents/skills" / name
        skill = directory / "SKILL.md"
        metadata = directory / "agents/openai.yaml"
        for path in (directory, skill, metadata):
            assert not path.is_symlink(), path
        text = skill.read_text()
        assert text.startswith("---\n"), skill
        front, separator, body = text[4:].partition("\n---\n")
        assert separator and body.strip(), skill
        fields = yaml.safe_load(front)
        assert isinstance(fields, dict) and fields.get("name") == name, skill
        assert isinstance(fields.get("description"), str) and fields["description"].strip(), skill
        invocation = yaml.safe_load(metadata.read_text())
        assert isinstance(invocation, dict), metadata
        interface = invocation.get("interface", {})
        for key in ("display_name", "short_description", "default_prompt"):
            assert isinstance(interface.get(key), str) and interface[key].strip(), metadata
        assert re.search(rf"\${re.escape(name)}(?![a-z0-9-])", interface["default_prompt"]), metadata
        policy = invocation.get("policy", {})
        assert isinstance(policy, dict), metadata
        assert policy.get("allow_implicit_invocation", True) is True, metadata


def main():
    validate_skills(ROOT)
    config = configuration(ROOT)
    required = config["required_checks"]
    assert set(required) == {"flowdc-tests", "agentic-quality"} and len(required) == 2
    schema = json.loads((ROOT / ".agentic/schemas/executor-result.json").read_text())
    assert schema["type"] == "object" and schema["additionalProperties"] is False
    assert set(schema["required"]) == set(schema["properties"]) == {"status", "summary", "checks", "blockers"}
    assert schema["properties"]["status"] == {
        "type": "string",
        "enum": ["completed", "checkpoint", "blocked"],
    }
    assert schema["properties"]["summary"] == {"type": "string"}
    for key in ("checks", "blockers"):
        assert schema["properties"][key] == {"type": "array", "items": {"type": "string"}}
    observed_jobs = []
    for path in (ROOT / ".github").rglob("*.yml"):
        # BaseLoader retains the Actions key `on` rather than YAML 1.1's boolean True.
        value = yaml.load(path.read_text(), Loader=yaml.BaseLoader)
        assert isinstance(value, dict), path
        if path.parent.name == "workflows":
            assert "on" in value and "jobs" in value, path
            assert "pull_request_target" not in value["on"], path
            assert value["permissions"]["contents"] == "read", path
            for name, job in value["jobs"].items():
                assert "timeout-minutes" in job, path
                if name in required:
                    observed_jobs.append(name)
                    assert "pull_request" in value["on"] and "push" in value["on"], path
                    assert "if" not in job and "continue-on-error" not in job, path
                    assert job.get("name", name) == name, path
                for step in job.get("steps", []):
                    if "uses" in step:
                        assert re.fullmatch(r"[^@]+@[0-9a-f]{40}", step["uses"]), step["uses"]
                        if step["uses"].startswith("actions/checkout@"):
                            assert step["with"]["persist-credentials"] == "false", path
    assert sorted(observed_jobs) == sorted(required), "Required check names must match unique CI jobs"
    paths = [
        ROOT / "README.md",
        ROOT / "AGENTS.md",
        *(ROOT / "docs/agent-workflow").rglob("*.md"),
        *(ROOT / ".agents/skills").rglob("*.md"),
    ]
    errors = []
    for path in paths:
        text = path.read_text()
        for link in re.findall(r"\[[^\]]*\]\(([^)]+)\)", text):
            if re.match(r"[a-z]+://|mailto:|#", link):
                continue
            target = unquote(link.split("#", 1)[0].strip("<>"))
            if target and not (path.parent / target).exists():
                errors.append(f"{path.relative_to(ROOT)}: missing {target}")
    if errors:
        raise SystemExit("\n".join(errors))
    print(
        "Eight skills, runtime configuration, result schema, CI check names, YAML and workflow links validated"
    )


if __name__ == "__main__":
    main()
