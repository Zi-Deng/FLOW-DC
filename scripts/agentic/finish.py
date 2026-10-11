#!/usr/bin/env python3
"""Prepare a current-head human merge command; never merge automatically."""

import argparse
import hashlib
import json
import shlex
import sys
from pathlib import Path

from workflow import Repo, WorkflowError, configuration, run


def preflight(repo, pr, directory, *, final_repair_delta=False):
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
    final_head = remote["head"]["sha"]
    delta = None
    if remote["base"]["sha"] != meta["base"]:
        raise WorkflowError("Review head/base is stale")
    if final_head != meta["head"]:
        if not final_repair_delta:
            raise WorkflowError("Review head/base is stale")
        attempts = [
            json.loads(path.read_text()) for path in (repo.state / "reviews").glob(f"pr{pr}-*/review.json")
        ]
        if sum(x.get("attempts", 0) for x in attempts) != 2:
            raise WorkflowError("Final repair delta handoff requires the two-invocation ceiling")
        launched = [x for x in attempts if x.get("attempts", 0)]
        if max(launched, key=lambda x: x.get("started_at", 0)).get("head") != meta["head"]:
            raise WorkflowError("Use the last completed review for final repair delta assessment")
        repo.fetch(f"refs/pull/{pr}/head")
        if repo.git("rev-parse", "FETCH_HEAD") != final_head:
            raise WorkflowError("PR head changed during final delta preparation")
        ancestor = run(
            ["git", "merge-base", "--is-ancestor", meta["head"], final_head], cwd=repo.root, check=False
        )
        if ancestor.returncode:
            raise WorkflowError("Final repair must descend from the reviewed head")
        delta = repo.git("diff", "--stat", f"{meta['head']}..{final_head}")
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
        "head": final_head,
        "reviewed_head": meta["head"],
        "final_repair_delta": delta,
        "status": "requires_human_delta_assessment" if delta is not None else "requires_human_assessment",
        "findings": report["findings"],
        "human_action": "Assess every material finding, scientific evidence and any unreviewed final repair delta before deciding to merge.",
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
                final_head,
            ]
        ),
        "cleanup": "After human merge, preserve ignored artifacts before removing the worktree. No automatic deletion.",
    }


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("pr", type=int)
    p.add_argument("--review-directory", required=True, type=Path)
    p.add_argument(
        "--final-repair-delta",
        action="store_true",
        help="Prepare an explicitly unreviewed final-repair delta for human assessment after two invocations",
    )
    a = p.parse_args()
    try:
        print(
            json.dumps(
                preflight(Repo(), a.pr, a.review_directory, final_repair_delta=a.final_repair_delta), indent=2
            )
        )
        return 0
    except (WorkflowError, OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
