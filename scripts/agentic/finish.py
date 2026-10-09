#!/usr/bin/env python3
"""Prepare a current-head human merge command; never merge automatically."""

import argparse
import hashlib
import json
import shlex
import sys
from pathlib import Path

from workflow import Repo, WorkflowError, configuration, run


def preflight(repo, pr, directory):
    meta = json.loads((directory / "review.json").read_text())
    remote = repo.api(f"pulls/{pr}")
    raw = (directory / "report.json").read_bytes()
    if (
        meta.get("status") != "completed"
        or meta.get("pr") != pr
        or meta.get("repository") != repo.name
        or not meta.get("publication")
    ):
        raise WorkflowError("A completed published review is required")
    if hashlib.sha256(raw).hexdigest() != meta.get("report_sha256"):
        raise WorkflowError("Review report changed")
    if remote["head"]["sha"] != meta["head"] or remote["base"]["sha"] != meta["base"]:
        raise WorkflowError("Review head/base is stale")
    if remote.get("mergeable") is not True or remote.get("draft") or remote["state"] != "open":
        raise WorkflowError("PR must be open, ready and mergeable")
    published = repo.api(f"pulls/{pr}/reviews/{meta['publication']['id']}")
    marker = f"<!-- flowdc-review:{meta['head']}:{meta['report_sha256']} -->"
    if (
        published.get("commit_id") != meta["head"]
        or published.get("state") != "COMMENTED"
        or marker not in published.get("body", "")
    ):
        raise WorkflowError("Bound model COMMENT review unavailable")
    checks = json.loads(
        run(
            ["gh", "pr", "checks", str(pr), "--repo", repo.name, "--required", "--json", "name,bucket,state"]
        ).stdout
    )
    names = {x["name"] for x in checks}
    if not set(configuration()["required_checks"]).issubset(names) or any(
        x["bucket"] != "pass" or x["state"] != "SUCCESS" for x in checks
    ):
        raise WorkflowError("Current required checks must succeed")
    report = json.loads(raw)
    return {
        "head": meta["head"],
        "findings": report["findings"],
        "human_action": "Assess every material finding and scientific evidence before deciding to merge.",
        "command": shlex.join(
            [
                "gh",
                "pr",
                "merge",
                str(pr),
                "--repo",
                repo.name,
                "--squash",
                "--match-head-commit",
                meta["head"],
            ]
        ),
        "cleanup": "After human merge, preserve ignored artifacts before removing the worktree. No automatic deletion.",
    }


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("pr", type=int)
    p.add_argument("--review-directory", required=True, type=Path)
    a = p.parse_args()
    try:
        print(json.dumps(preflight(Repo(), a.pr, a.review_directory), indent=2))
        return 0
    except (WorkflowError, OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
