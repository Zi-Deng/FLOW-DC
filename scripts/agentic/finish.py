#!/usr/bin/env python3
"""Prepare readiness; only the human-run entrypoint requests merge and cleanup."""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import uuid
from pathlib import Path
from urllib.parse import urlparse

import pipeline
from sessions import session_uuid
from tasks import TaskStore, digest, plain_path
from workflow import Repo, WorkflowError, cleanup_task, configuration, positive, run, sha

import archives

NOTE = (
    "This is an operator assessment, not human approval or cryptographic proof. "
    "--match-head-commit pins only head; server rules, checks and merge queues "
    "remain authoritative for the base and final merge."
)


def server_view(repo, number):
    query = """
    query($owner:String!,$name:String!,$number:Int!,$cursor:String) {
      repository(owner:$owner,name:$name) {
        pullRequest(number:$number) {
          headRefOid baseRefOid mergeQueueEntry { id }
          reviewThreads(first:100,after:$cursor) {
            nodes { id isResolved }
            pageInfo { hasNextPage endCursor }
          }
        }
      }
    }
    """
    owner, name = repo.name.split("/")
    cursor = None
    cursors = set()
    seen = set()
    threads = []
    binding = None
    while True:
        try:
            result = run(
                ["gh", "api", "graphql", "--input", "-"],
                cwd=repo.root,
                check=False,
                input=json.dumps(
                    {
                        "query": query,
                        "variables": {
                            "owner": owner,
                            "name": name,
                            "number": positive(number),
                            "cursor": cursor,
                        },
                    }
                ),
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise WorkflowError("GitHub thread/queue query could not complete") from exc
        if result.returncode:
            raise WorkflowError(f"GitHub thread/queue query failed (gh exit {result.returncode})")
        try:
            response = json.loads(result.stdout)
            if not isinstance(response, dict) or response.get("errors"):
                raise WorkflowError("GitHub could not establish thread and merge-queue state")
            current = response["data"]["repository"]["pullRequest"]
            observed = (sha(current["headRefOid"]), sha(current["baseRefOid"]))
            queue = current["mergeQueueEntry"]
            connection = current["reviewThreads"]
            nodes = connection["nodes"]
            page = connection["pageInfo"]
            has_next = page["hasNextPage"]
            next_cursor = page["endCursor"]
        except (ValueError, KeyError, TypeError) as exc:
            raise WorkflowError("Malformed GitHub thread/queue response") from exc
        if (
            not isinstance(nodes, list)
            or not isinstance(has_next, bool)
            or (next_cursor is not None and not isinstance(next_cursor, str))
            or (
                queue is not None
                and (
                    not isinstance(queue, dict)
                    or not isinstance(queue.get("id"), str)
                    or not queue["id"].strip()
                )
            )
        ):
            raise WorkflowError("Malformed GitHub thread/queue response")
        if binding is not None and observed != binding:
            raise WorkflowError("PR changed during thread pagination")
        binding = observed
        for thread in nodes:
            if (
                not isinstance(thread, dict)
                or not isinstance(thread.get("id"), str)
                or not thread["id"].strip()
                or thread["id"] in seen
                or not isinstance(thread.get("isResolved"), bool)
            ):
                raise WorkflowError("Invalid or duplicate review-thread result")
            seen.add(thread["id"])
            threads.append(thread)
        if not has_next:
            return {
                "head_sha": binding[0],
                "base_sha": binding[1],
                "queued": queue is not None,
                "threads": threads,
            }
        if not next_cursor or not next_cursor.strip() or next_cursor in cursors:
            raise WorkflowError("Review-thread pagination did not advance")
        cursors.add(next_cursor)
        cursor = next_cursor


def required_checks(repo, number):
    try:
        result = run(
            [
                "gh",
                "pr",
                "checks",
                str(number),
                "--repo",
                repo.name,
                "--required",
                "--json",
                "name,bucket,state",
            ],
            cwd=repo.root,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise WorkflowError("GitHub required-check query could not complete") from exc
    try:
        checks = json.loads(result.stdout)
    except (ValueError, TypeError) as exc:
        raise WorkflowError(f"Malformed required-check response (gh exit {result.returncode})") from exc
    if not isinstance(checks, list) or any(
        not isinstance(item, dict)
        or not isinstance(item.get("name"), str)
        or not item["name"].strip()
        or not isinstance(item.get("bucket"), str)
        or item["bucket"] not in {"pass", "fail", "pending", "skipping", "cancel"}
        for item in checks
    ):
        raise WorkflowError(f"Malformed required-check response (gh exit {result.returncode})")
    problems = ", ".join(f"{item['name']}: {item['bucket']}" for item in checks if item["bucket"] != "pass")
    if result.returncode:
        reason = {8: "pending", 1: "failing or unavailable"}.get(result.returncode, "unavailable")
        detail = f": {problems}" if problems else ""
        raise WorkflowError(f"Required checks are {reason} (gh exit {result.returncode}){detail}")
    expected = set(configuration(repo.root)["required_checks"])
    if not expected or not checks or not expected.issubset({item["name"] for item in checks}):
        raise WorkflowError("Required check configuration is absent or does not match policy")
    if problems:
        raise WorkflowError("Every required check must pass: " + problems)
    return checks


def public_links(value):
    return (
        isinstance(value, list)
        and bool(value)
        and all(
            isinstance(item, str)
            and urlparse(item).scheme == "https"
            and bool(urlparse(item).netloc)
            and urlparse(item).username is None
            for item in value
        )
    )


def validate_assessment(repo, state, assessment):
    verified = pipeline.validate_designated(repo, state)
    pr = verified["pr"]
    required = {
        "head_sha",
        "base_sha",
        "review_id",
        "feedback_digest",
        "pr_digest",
        "summary",
        "acceptance",
        "records",
        "limitations",
        "domain_evidence",
    }
    if not isinstance(assessment, dict) or not required.issubset(assessment):
        raise WorkflowError("Finish assessment is missing required fields")
    if (
        assessment["head_sha"] != pr["head"]["sha"]
        or assessment["base_sha"] != pr["base"]["sha"]
        or positive(assessment["review_id"]) != verified["designation"]["review_id"]
        or assessment["pr_digest"] != pipeline.pr_digest(pr)
    ):
        raise WorkflowError("Assessment does not describe the current reviewed PR")
    for name in ("summary", "limitations", "domain_evidence"):
        if not isinstance(assessment[name], str) or not assessment[name].strip():
            raise WorkflowError(f"Assessment requires an explicit {name}")
    acceptance = assessment["acceptance"]
    if not isinstance(acceptance, list) or not acceptance:
        raise WorkflowError("Assessment requires criterion-to-evidence coverage")
    criteria = set()
    for item in acceptance:
        if (
            not isinstance(item, dict)
            or not isinstance(item.get("criterion"), str)
            or not item["criterion"].strip()
            or item["criterion"] in criteria
            or item.get("supported") is not True
            or not public_links(item.get("evidence"))
        ):
            raise WorkflowError("Acceptance evidence is missing, duplicated or unsupported")
        criteria.add(item["criterion"])
    feedback = pipeline.collect_feedback(repo, state["pr"])
    if assessment["feedback_digest"] != digest(feedback):
        raise WorkflowError("Public feedback changed; reassess the complete discussion")
    expected = pipeline.relevant_records(feedback, assessment["head_sha"])
    records = assessment["records"]
    if not isinstance(records, list) or any(not isinstance(item, dict) for item in records):
        raise WorkflowError("Invalid finding assessment records")
    mapped = {item.get("id"): item for item in records}
    if len(mapped) != len(records) or set(mapped) != set(expected):
        raise WorkflowError("Assess every published review and inline record, including old heads")
    for identifier, item in mapped.items():
        disposition = item.get("disposition")
        if (
            item.get("digest") != expected[identifier]["digest"]
            or disposition not in {"no-action", "fixed", "rebutted", "deferred"}
            or not isinstance(item.get("rationale"), str)
            or not item["rationale"].strip()
            or not public_links(item.get("evidence"))
        ):
            raise WorkflowError("A finding is unresolved or lacks a current evidenced disposition")
        if disposition == "deferred":
            match = re.fullmatch(
                r"https://github\.com/([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)/issues/([1-9][0-9]*)",
                item.get("follow_up", ""),
            )
            if (
                not match
                or not isinstance(item.get("acceptance_source"), str)
                or not item["acceptance_source"].strip()
            ):
                raise WorkflowError("Deferral requires a follow-up and explicit acceptance source")
            follow_up = json.loads(
                run(["gh", "api", f"repos/{match[1]}/issues/{match[2]}"], cwd=repo.root).stdout
            )
            if follow_up.get("state") != "open" or "pull_request" in follow_up:
                raise WorkflowError("Deferred finding must reference an open follow-up issue")
    executor = state.get("executor")
    if not isinstance(executor, dict):
        raise WorkflowError("Finish requires the saved original executor UUID and a completed phase")
    session_uuid(executor.get("uuid"))
    runs = executor.get("runs")
    if (
        not isinstance(runs, list)
        or not runs
        or not isinstance(runs[-1], dict)
        or runs[-1].get("status") != "completed"
        or runs[-1].get("incomplete") is not False
        or runs[-1].get("contract_digest") != digest(state["approval"]["contract"])
    ):
        raise WorkflowError("Executor phase remains incomplete for this contract")
    checks = required_checks(repo, state["pr"])
    server = server_view(repo, state["pr"])
    if (
        server["head_sha"] != assessment["head_sha"]
        or server["base_sha"] != assessment["base_sha"]
        or pr.get("mergeable") is not True
    ):
        raise WorkflowError("PR changed or mergeability is unknown/conflicting")
    if any(not thread["isResolved"] for thread in server["threads"]):
        raise WorkflowError("Review conversations remain unresolved")
    final = pipeline.validate_designated(repo, state)
    if (
        pipeline.pr_digest(final["pr"]) != assessment["pr_digest"]
        or digest(pipeline.collect_feedback(repo, state["pr"])) != assessment["feedback_digest"]
    ):
        raise WorkflowError("PR discussion or evidence changed during finish validation")
    return {"pr": final["pr"], "checks": checks, "server": server}


def cleanup_identity(repo, state, head, *, merged, allow_missing=False):
    repo.assert_main()
    registration = state["workspace"]
    branch = registration["branch"]
    issue = positive(state["approval"]["issue"])
    if not re.fullmatch(rf"issue-{issue}-[a-z0-9]+(?:-[a-z0-9]+)*", branch):
        raise WorkflowError("Cleanup branch differs from the registered issue")
    path = plain_path(repo.worktree_root() / branch)
    if registration["worktree"] != str(path) or registration["base"] != repo.base:
        raise WorkflowError("Cleanup workspace identity changed")
    pr = repo.pr(state["pr"])
    if (
        not pr["head"].get("repo")
        or pr["head"]["repo"]["full_name"] != repo.name
        or pr["head"]["ref"] != branch
        or pr["head"]["sha"] != head
        or pr["base"]["ref"] != repo.base
        or (merged and not pr.get("merged"))
        or (not merged and (pr.get("merged") or pr.get("state") != "open"))
    ):
        raise WorkflowError("PR no longer matches the pinned cleanup identity or merge state")
    registered = [item for item in repo.worktrees() if item.get("branch") == f"refs/heads/{branch}"]
    local = run(["git", "-C", repo.root, "rev-parse", "--verify", f"refs/heads/{branch}"], check=False)
    if local.returncode == 0 and local.stdout.strip() != head:
        raise WorkflowError("Local branch tip changed; preserve its commits")
    if path.exists():
        if (
            len(registered) != 1
            or Path(registered[0]["worktree"]).resolve() != path
            or registered[0].get("HEAD") != head
            or "locked" in registered[0]
            or run(["git", "-C", path, "rev-parse", "HEAD"]).stdout.strip() != head
        ):
            raise WorkflowError("Cleanup worktree is moved, locked or at a different tip")
    elif registered or not allow_missing:
        raise WorkflowError("Registered cleanup worktree is missing")
    return path, pr


def inventory(path):
    if not path.exists():
        return {"roots": [], "artifacts": [], "blockers": []}
    raw = run(
        [
            "git",
            "-C",
            path,
            "status",
            "--porcelain=v1",
            "-z",
            "--ignored=matching",
            "--untracked-files=all",
        ]
    ).stdout
    records = iter(raw.rstrip("\0").split("\0") if raw else [])
    roots, blockers = [], []
    for record in records:
        code, name = record[:2], record[3:]
        if "R" in code or "C" in code:
            next(records, None)
        if code == "!!":
            roots.append(archives.relative_name(name.rstrip("/")))
        else:
            blockers.append({"status": code, "path": name})
    minimal = []
    for root in sorted(set(roots), key=lambda value: (value.count("/"), value)):
        if not any(root == parent or root.startswith(parent + "/") for parent in minimal):
            minimal.append(root)
    artifacts = []
    if not blockers:
        for root in minimal:
            manifest = archives.snapshot(path / root)
            artifacts.append(
                {
                    "path": root,
                    "kind": manifest[""]["kind"],
                    "entries": len(manifest),
                    "bytes": sum(item.get("size", 0) for item in manifest.values()),
                }
            )
    return {"roots": minimal, "artifacts": artifacts, "blockers": blockers}


def human_command(repo, number, head, base):
    return shlex.join(
        [
            "bash",
            str(repo.main / "scripts/finish-task.sh"),
            str(number),
            "--head",
            head,
            "--base",
            base,
        ]
    )


def prepare_finish(repo, number, assessment_file, ready=False):
    number = positive(number)
    raw = Path(assessment_file).read_bytes()
    if len(raw) > 1_000_000:
        raise WorkflowError("Finish assessment exceeds its size budget")
    assessment = json.loads(raw)
    store = TaskStore(repo)
    with store.locked(f"issue-{number}") as state:
        old = state.get("finish")
        if old and old.get("status") not in {"prepared", "queued", "unmerged"}:
            raise WorkflowError("Finishing already started; use the saved human retry command")
        if old and (
            old.get("local_cleanup")
            or "archive_result" in old
            or "merge_commit_sha" in old
            or archives.exists(plain_path(old["archive"]))
        ):
            raise WorkflowError("Existing archive or cleanup evidence requires the saved recovery record")
        validated = validate_assessment(repo, state, assessment)
        with repo.lock():
            path, _ = cleanup_identity(repo, state, assessment["head_sha"], merged=False)
            observed = inventory(path)
        if observed["blockers"]:
            return {"incomplete": True, "qualified": False, "inventory": observed, "command": None}
        pr = validated["pr"]
        if ready and pr.get("draft"):
            result = run(
                ["gh", "pr", "ready", str(state["pr"]), "--repo", repo.name],
                cwd=repo.root,
                check=False,
            )
            pr = repo.pr(state["pr"])
            if pr.get("draft"):
                raise WorkflowError(f"PR remains draft after ready request ({result.returncode})")
            validated = validate_assessment(repo, state, assessment)
            pr = validated["pr"]
        destination = plain_path(repo.main / ".agentic-local/archives" / f"issue-{number}-{uuid.uuid4().hex}")
        state["finish"] = {
            "status": "prepared",
            "assessment": assessment,
            "assessment_digest": digest(assessment),
            "contract_digest": digest(state["approval"]["contract"]),
            "head_sha": assessment["head_sha"],
            "base_sha": assessment["base_sha"],
            "review_id": assessment["review_id"],
            "archive": str(destination),
            "inventory": observed,
            "local_cleanup": False,
            "remote_branch": "pending",
            "note": NOTE,
        }
        store.save(state)
        return {
            "qualified": True,
            "ready": not pr.get("draft"),
            "pr": state["pr_url"],
            "review": state["designated_review"]["url"],
            "head_sha": assessment["head_sha"],
            "base_sha": assessment["base_sha"],
            "inventory": observed,
            "archive": str(destination),
            "cwd": str(repo.main),
            "command": human_command(repo, number, assessment["head_sha"], assessment["base_sha"]),
            "note": NOTE,
        }


def delete_remote(repo, state, head):
    branch = state["workspace"]["branch"]
    ref = f"refs/heads/{branch}"

    def observed():
        words = run(["git", "-C", repo.root, "ls-remote", "--heads", "origin", ref]).stdout.split()
        if not words:
            return None
        if len(words) != 2 or words[1] != ref:
            raise WorkflowError("Unexpected remote-ref response")
        return sha(words[0])

    current = observed()
    if current is None:
        return "absent"
    if current != head:
        return "advanced"
    run(
        [
            "git",
            "-C",
            repo.root,
            "push",
            f"--force-with-lease={ref}:{head}",
            "origin",
            f":{ref}",
        ],
        check=False,
    )
    current = observed()
    if current is None:
        return "deleted"
    if current != head:
        return "advanced"
    raise WorkflowError("Remote deletion is incomplete; retry with the original expected SHA")


def finish_task(repo, number, head, base):
    """Called by the human entrypoint; tests use only disposable Git/GitHub doubles."""
    number, head, base = positive(number), sha(head), sha(base)
    store = TaskStore(repo)
    with store.locked(f"issue-{number}") as state:
        saved = state.get("finish")
        if (
            not saved
            or saved["head_sha"] != head
            or saved["base_sha"] != base
            or saved["assessment_digest"] != digest(saved["assessment"])
            or saved["contract_digest"] != digest(state["approval"]["contract"])
        ):
            raise WorkflowError("Human command does not match the saved finish assessment")
        retry = human_command(repo, number, head, base)
        pr = repo.pr(state["pr"])
        if not pr.get("merged"):
            server = server_view(repo, state["pr"])
            if server["queued"]:
                saved["status"] = "queued"
                store.save(state)
                return {
                    "incomplete": True,
                    "merged": False,
                    "queued": True,
                    "archived": False,
                    "retry": retry,
                }
            validated = validate_assessment(repo, state, saved["assessment"])
            if validated["pr"].get("draft"):
                raise WorkflowError("Mark the qualifying PR ready before running the human command")
            with repo.lock():
                path, _ = cleanup_identity(repo, state, head, merged=False)
                if inventory(path)["blockers"]:
                    raise WorkflowError("Worktree contains tracked changes or unexpected untracked files")
            saved["status"] = "merge-requested"
            store.save(state)
            result = None
            failure = None
            try:
                result = run(
                    [
                        "gh",
                        "pr",
                        "merge",
                        str(state["pr"]),
                        "--repo",
                        repo.name,
                        "--squash",
                        "--match-head-commit",
                        head,
                    ],
                    cwd=repo.root,
                    check=False,
                )
            except (OSError, WorkflowError, subprocess.SubprocessError) as exc:
                failure = str(exc)
            saved["merge_result"] = {
                "returncode": result.returncode if result is not None else None,
                "error": failure,
            }
            store.save(state)
            # A CLI error or timeout is not evidence that GitHub did not merge.
            try:
                pr = repo.pr(state["pr"])
            except (OSError, WorkflowError, ValueError, subprocess.SubprocessError) as exc:
                saved["error"] = str(exc)
                store.save(state)
                return {"incomplete": True, "merged": None, "queued": None, "archived": False, "retry": retry}
            if not pr.get("merged"):
                server = server_view(repo, state["pr"])
                saved["status"] = "queued" if server["queued"] else "unmerged"
                store.save(state)
                return {
                    "incomplete": True,
                    "merged": False,
                    "queued": server["queued"],
                    "archived": False,
                    "retry": retry,
                }
        # After a verified merge, issue closure and changes to the base tip are
        # expected. Cleanup remains bound to the saved head, repository and branch.
        saved["status"] = "merged"
        store.save(state)
        try:
            with repo.lock():
                path, pr = cleanup_identity(repo, state, head, merged=True, allow_missing=True)
                saved["merge_commit_sha"] = sha(pr["merge_commit_sha"])
                destination = plain_path(saved["archive"])
                allowed = plain_path(repo.main / ".agentic-local/archives")
                if destination.parent != allowed:
                    raise WorkflowError("Archive destination is outside canonical private archives")
                observed = inventory(path)
                if observed["blockers"]:
                    raise WorkflowError("Merged worktree contains changed or unexpected files")
                if not path.exists() and not archives.exists(destination) and saved["inventory"]["roots"]:
                    raise WorkflowError("Worktree disappeared before its artifacts were archived")
                saved["status"] = "archiving"
                store.save(state)

                def guard():
                    cleanup_identity(repo, state, head, merged=True, allow_missing=True)
                    if path.exists() and inventory(path)["blockers"]:
                        raise WorkflowError("Concurrent worktree changes stop archival")

                saved["archive_result"] = archives.archive(
                    path,
                    destination,
                    {"repository": repo.name, "issue": number, "pr": state["pr"], "head_sha": head},
                    observed["roots"],
                    guard,
                )
                store.save(state)
            # Keep the existing low-level cleanup guards as the final authority.
            cleanup_task(repo, state["pr"], expected_sha=head)
            saved["local_cleanup"] = True
            saved["status"] = "local-cleaned"
            store.save(state)
            with repo.lock():
                cleanup_identity(repo, state, head, merged=True, allow_missing=True)
                saved["remote_branch"] = delete_remote(repo, state, head)
            saved["status"] = (
                "complete" if saved["remote_branch"] in {"deleted", "absent"} else "remote-preserved"
            )
            saved.pop("error", None)
            store.save(state)
        except (
            WorkflowError,
            OSError,
            ValueError,
            KeyError,
            TypeError,
            subprocess.SubprocessError,
            KeyboardInterrupt,
        ) as exc:
            saved["error"] = str(exc) or "Finishing interrupted"
            store.save(state)
        return {
            "merged": True,
            "queued": False,
            "archived": saved.get("archive_result", {}).get("complete", False),
            "archive": saved["archive"],
            "journal": str(Path(saved["archive"]) / "journal.json"),
            "local_cleanup": saved["local_cleanup"],
            "remote_branch": saved["remote_branch"],
            "incomplete": saved["status"] != "complete",
            "error": saved.get("error"),
            "retry": retry,
            "note": NOTE,
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("issue")
    parser.add_argument("--head", required=True)
    parser.add_argument("--base", required=True)
    args = parser.parse_args()
    try:
        if os.environ.get("AGENTIC_EXECUTOR_ROLE"):
            raise WorkflowError("Only the human runs the finishing entrypoint")
        result = finish_task(Repo(), args.issue, args.head, args.base)
        print(json.dumps(result, indent=2))
        return 1 if result.get("incomplete") else 0
    except (WorkflowError, OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
