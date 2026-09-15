#!/usr/bin/env python3
"""Small, explicit issue-to-PR operations; no autonomous merge or shell evaluation."""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
from pathlib import Path


class WorkflowError(Exception):
    """An unmet precondition; leave existing work intact."""


def run(argv, *, cwd=None, input=None, check=True, env=None, timeout=120):
    result = subprocess.run(
        [str(x) for x in argv],
        cwd=cwd,
        input=input,
        text=True,
        encoding="utf-8",
        capture_output=True,
        env=env,
        timeout=timeout,
        check=False,
    )
    if check and result.returncode:
        # Never print stdin: it can contain credentials or private review text.
        raise WorkflowError(f"{argv[0]} {argv[1]} failed ({result.returncode}): {result.stderr.strip()}")
    return result


def positive(value):
    if not re.fullmatch(r"[1-9][0-9]*", str(value)):
        raise WorkflowError("Issue/PR number must be a positive integer")
    return int(value)


def sha(value):
    if not re.fullmatch(r"[0-9a-f]{40}", value):
        raise WorkflowError("Expected a complete 40-character Git commit SHA")
    return value


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


class Repo:
    def __init__(self, path=None):
        self.root = Path(run(["git", "rev-parse", "--show-toplevel"], cwd=path).stdout.strip()).resolve()
        common = self.git("rev-parse", "--path-format=absolute", "--git-common-dir")
        self.common = Path(common).resolve()
        self.main = self.common.parent
        self._info = None

    def git(self, *args, check=True):
        return run(["git", "-C", self.root, *args], check=check).stdout.strip()

    @property
    def info(self):
        if self._info is None:
            remote = self.git("remote", "get-url", "origin")
            self._info = json.loads(
                run(
                    [
                        "gh",
                        "repo",
                        "view",
                        remote,
                        "--json",
                        "nameWithOwner,defaultBranchRef,isPrivate",
                    ],
                    cwd=self.root,
                ).stdout
            )
            if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", self._info["nameWithOwner"]):
                raise WorkflowError("Invalid GitHub repository identity")
            if not self._info.get("defaultBranchRef"):
                raise WorkflowError("The GitHub repository has no default branch yet")
        return self._info

    @property
    def name(self):
        return self.info["nameWithOwner"]

    @property
    def base(self):
        return self.info["defaultBranchRef"]["name"]

    def api(self, suffix, *, data=None, paginate=False, method=None, page_key=None):
        args = ["gh", "api", f"repos/{self.name}/{suffix}"]
        if paginate:
            args += ["--paginate", "--slurp"]
        if data is not None:
            args += ["--method", method or "POST", "--input", "-"]
        elif method:
            args += ["--method", method]
        out = run(args, cwd=self.root, input=json.dumps(data) if data is not None else None).stdout
        result = json.loads(out) if out.strip() else None
        return (
            [item for page in result for item in (page[page_key] if page_key else page)]
            if paginate
            else result
        )

    def fetch(self, *refs):
        # Per-command helper supports private Actions snapshots without persisting a token.
        return run(
            [
                "git",
                "-C",
                self.root,
                "-c",
                "credential.helper=",
                "-c",
                "credential.https://github.com.helper=!gh auth git-credential",
                "fetch",
                "--no-tags",
                "origin",
                *refs,
            ]
        ).stdout

    def pr(self, number):
        return self.api(f"pulls/{positive(number)}")

    def assert_main(self, *, clean=True):
        if self.root != self.main or self.git("branch", "--show-current") != self.base:
            raise WorkflowError("Run this operation from the main checkout on its default branch")
        if clean and self.git("status", "--porcelain"):
            raise WorkflowError("Main checkout is dirty; preserve and reconcile its changes first")

    def worktrees(self):
        raw = run(["git", "-C", self.root, "worktree", "list", "--porcelain", "-z"]).stdout
        trees = []
        for block in raw.strip("\0").split("\0\0"):
            trees.append(
                dict(line.split(" ", 1) if " " in line else (line, "") for line in block.split("\0"))
            )
        return trees

    def worktree_root(self):
        path = Path(os.environ.get("WT_ROOT", str(self.main.parent / (self.main.name + "-worktrees"))))
        if not path.is_absolute():
            raise WorkflowError("WT_ROOT must be an absolute path")
        if path.is_symlink():
            raise WorkflowError("WT_ROOT must not be a symlink")
        path = path.resolve()
        if path == self.main or path.is_relative_to(self.main):
            raise WorkflowError("Worktrees must live outside the main checkout")
        return path

    @contextlib.contextmanager
    def lock(self):
        with (self.common / "agentic-operation.lock").open("a") as stream:
            try:
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise WorkflowError("Another local workflow operation is running") from exc
            yield


def configuration(root):
    result = json.loads((Path(root) / ".agentic/config.json").read_text(encoding="utf-8"))
    if (
        not isinstance(result, dict)
        or type(result.get("schema_version")) is not int
        or result["schema_version"] != 1
    ):
        raise WorkflowError("Unsupported .agentic/config.json schema")
    result.setdefault("max_diff_bytes", None)
    result.setdefault("managed_max_prompt_bytes", 300_000)
    for key in ("max_diff_bytes", "managed_max_prompt_bytes"):
        value = result[key]
        if key == "max_diff_bytes" and value is None:
            continue
        if type(value) is not int or value <= 0:
            suffix = " or null (unlimited)" if key == "max_diff_bytes" else ""
            raise WorkflowError(f"{key} must be a positive integer{suffix}")
    for key in (
        "review_timeout_seconds",
        "review_max_ai_credits",
        "max_source_file_bytes",
        "max_snapshot_bytes",
    ):
        if type(result.get(key)) is not int or result[key] <= 0:
            raise WorkflowError(f"{key} is required and must be a positive integer")
    for key in ("managed_timeout_seconds", "managed_max_output_bytes"):
        if key in result and (type(result[key]) is not int or result[key] <= 0):
            raise WorkflowError(f"{key} must be a positive integer when supplied")
    for key in ("openai_model", "copilot_model", "domain_rubric"):
        if not isinstance(result.get(key), str) or not result[key].strip():
            raise WorkflowError(f"{key} is required and must be a nonempty string")
    checks = result.get("required_checks")
    if (
        not isinstance(checks, list)
        or not checks
        or any(not isinstance(item, str) or not item.strip() for item in checks)
        or len(set(checks)) != len(checks)
    ):
        raise WorkflowError("required_checks must be a nonempty list of unique check names")
    return result


def new_task(repo, number, slug, base=None):
    number = positive(number)
    if not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", slug) or len(slug) > 80:
        raise WorkflowError("Slug must be lowercase words/digits joined by hyphens, at most 80 characters")
    with repo.lock():
        repo.assert_main()
        issue = repo.api(f"issues/{number}")
        if issue["state"] != "open" or "pull_request" in issue:
            raise WorkflowError("Task must reference an open issue, not a pull request")
        base = base or repo.base
        run(["git", "check-ref-format", "--branch", base])
        branch = f"issue-{number}-{slug}"
        path = repo.worktree_root() / branch
        if path.exists() or path.is_symlink():
            raise WorkflowError(f"Worktree path already exists: {path}")
        if any(t.get("branch") == f"refs/heads/{branch}" for t in repo.worktrees()):
            raise WorkflowError("Task branch is already checked out")
        repo.git("fetch", "--prune", "origin")
        repo.git("rev-parse", "--verify", f"refs/remotes/origin/{base}^{{commit}}")
        path.parent.mkdir(parents=True, exist_ok=True)
        exists = run(
            ["git", "-C", repo.root, "show-ref", "--verify", "--quiet", f"refs/heads/{branch}"], check=False
        )
        if exists.returncode == 0:
            repo.git("worktree", "add", str(path), branch)
        else:
            remote = run(
                ["git", "-C", repo.root, "show-ref", "--verify", "--quiet", f"refs/remotes/origin/{branch}"],
                check=False,
            )
            if remote.returncode == 0:
                repo.git("worktree", "add", "--track", "-b", branch, str(path), f"origin/{branch}")
            else:
                repo.git("worktree", "add", "-b", branch, str(path), f"origin/{base}")
        # A failed push deliberately leaves the task worktree available for recovery.
        run(["git", "-C", path, "push", "-u", "origin", branch])
        return {"issue": number, "branch": branch, "worktree": str(path), "base": base}


def cleanup_task(repo, number, expected_sha=None):
    with repo.lock():
        repo.assert_main()
        pr = repo.pr(number)
        branch = pr["head"]["ref"]
        tip = sha(pr["head"]["sha"])
        if expected_sha is not None and tip != sha(expected_sha):
            raise WorkflowError("Merged PR head differs from the pinned cleanup head")
        if not pr.get("merged"):
            raise WorkflowError("Cleanup requires a verified MERGED pull request")
        if not pr["head"].get("repo") or pr["head"]["repo"]["full_name"] != repo.name:
            raise WorkflowError("Cleanup refuses fork PRs or deleted head repositories")
        if pr["base"]["ref"] != repo.base or branch == repo.base:
            raise WorkflowError("Cleanup requires a task merged into this repository's default branch")
        if not re.fullmatch(r"issue-[1-9][0-9]*-[a-z0-9]+(?:-[a-z0-9]+)*", branch):
            raise WorkflowError("Cleanup only handles issue-N-slug task branches")
        path = repo.worktree_root() / branch
        if path.is_symlink():
            raise WorkflowError("Refusing a symlink worktree")
        registered = [t for t in repo.worktrees() if t.get("branch") == f"refs/heads/{branch}"]
        if registered and (len(registered) != 1 or Path(registered[0]["worktree"]).resolve() != path):
            raise WorkflowError("Task branch is attached at an unexpected location")
        local = run(["git", "-C", repo.root, "rev-parse", "--verify", f"refs/heads/{branch}"], check=False)
        if local.returncode == 0 and local.stdout.strip() != tip:
            raise WorkflowError("Local branch tip differs from merged PR head; preserve these commits")
        if path.exists():
            if not registered:
                raise WorkflowError("Path exists but is not the registered task worktree")
            status = run(
                ["git", "-C", path, "status", "--porcelain", "--ignored", "--untracked-files=all"]
            ).stdout
            if status:
                raise WorkflowError(
                    "Task worktree contains changed, untracked or ignored files; archive them first"
                )
            if registered[0].get("HEAD") != tip or "locked" in registered[0]:
                raise WorkflowError("Worktree tip changed or worktree is locked")
            repo.git("worktree", "remove", str(path))
        if local.returncode == 0:
            # Force deletion is bounded by remote merged state AND exact local tip equality.
            repo.git("branch", "-D", branch)
        return {"cleaned": branch, "remote_branch_deleted": False}


def draft_pr(repo, title, body):
    if os.environ.get("AGENTIC_EXECUTOR_ROLE"):
        raise WorkflowError("Managed executors prepare PR text; the coordinator publishes it")
    branch = repo.git("branch", "--show-current")
    match = re.fullmatch(r"issue-([1-9][0-9]*)-[a-z0-9-]+", branch)
    if repo.root == repo.main or not match:
        raise WorkflowError("Create a draft PR from an issue task worktree")
    if repo.git("status", "--porcelain"):
        raise WorkflowError("Commit the intended changes before opening the draft PR")
    text = Path(body).read_text(encoding="utf-8")
    if not re.search(rf"(?im)^Fixes #{match[1]}\s*$", text):
        raise WorkflowError(f"PR body must include its own line: Fixes #{match[1]}")
    existing = json.loads(
        run(
            ["gh", "pr", "list", "--repo", repo.name, "--head", branch, "--state", "open", "--json", "url"]
        ).stdout
    )
    repo.git("push", "-u", "origin", branch)
    if existing:
        if len(existing) != 1:
            raise WorkflowError("Multiple open PRs match this branch")
        run(
            [
                "gh",
                "pr",
                "edit",
                existing[0]["url"],
                "--repo",
                repo.name,
                "--title",
                title,
                "--body-file",
                Path(body).resolve(),
            ]
        )
        return {"existing_pr": existing[0]["url"], "updated": True}
    return {
        "pr": run(
            [
                "gh",
                "pr",
                "create",
                "--repo",
                repo.name,
                "--draft",
                "--base",
                repo.base,
                "--head",
                branch,
                "--title",
                title,
                "--body-file",
                Path(body).resolve(),
            ]
        ).stdout.strip()
    }


def ruleset(repo, checks):
    if not checks or any(not c.strip() for c in checks):
        raise WorkflowError("At least one observed check name is required")
    return {
        "name": "agentic-default-branch",
        "target": "branch",
        "enforcement": "active",
        "bypass_actors": [],
        "conditions": {"ref_name": {"include": [f"refs/heads/{repo.base}"], "exclude": []}},
        "rules": [
            {"type": "deletion"},
            {"type": "non_fast_forward"},
            {"type": "required_linear_history"},
            {
                "type": "pull_request",
                "parameters": {
                    "required_approving_review_count": 0,
                    "dismiss_stale_reviews_on_push": True,
                    "require_code_owner_review": False,
                    "require_last_push_approval": False,
                    "required_review_thread_resolution": True,
                },
            },
            {
                "type": "required_status_checks",
                "parameters": {
                    "required_status_checks": [{"context": c, "integration_id": 15368} for c in checks],
                    "strict_required_status_checks_policy": True,
                },
            },
        ],
    }


def merge_preflight(repo, number, reviewed_sha):
    repo.assert_main()
    reviewed_sha = sha(reviewed_sha)
    pr = repo.pr(number)
    if pr["state"] != "open" or pr.get("draft") or pr.get("merged"):
        raise WorkflowError("PR must be open and ready for review")
    if pr["base"]["ref"] != repo.base or pr["head"]["sha"] != reviewed_sha:
        raise WorkflowError("PR base or head changed; review the current artifact")
    if pr.get("mergeable") is not True:
        raise WorkflowError("Mergeability is unknown or conflicting; wait or repair")
    review_list = repo.api(f"pulls/{number}/reviews", paginate=True)
    if not any(
        r.get("commit_id") == reviewed_sha and r.get("state") in {"COMMENTED", "APPROVED"}
        for r in review_list
    ):
        raise WorkflowError("No published review exists for this exact head")
    # --required must fail closed if no required checks are configured.
    checks = json.loads(
        run(
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
            ]
        ).stdout
    )
    expected = set(configuration(repo.root)["required_checks"])
    if not checks or not expected.issubset({c["name"] for c in checks}):
        raise WorkflowError("Required check configuration is missing or does not match the observed names")
    if any(c["bucket"] != "pass" for c in checks):
        raise WorkflowError("Every required check must pass; skipped/neutral/pending is insufficient")
    if repo.pr(number)["head"]["sha"] != reviewed_sha:
        raise WorkflowError("Head changed during preflight")
    return {
        "reviewed_sha": reviewed_sha,
        "human_checks": "Read every finding, resolve conversations, confirm domain evidence and approve the merge yourself.",
        "command": shlex.join(
            [
                "gh",
                "pr",
                "merge",
                str(number),
                "--repo",
                repo.name,
                "--squash",
                "--match-head-commit",
                reviewed_sha,
            ]
        ),
        "note": "Preflight does not merge and cannot attest to a human decision. Server rules remain authoritative.",
    }


def launch(repo, role, task, execute=False, managed=False):
    if os.environ.get("AGENTIC_EXECUTOR_ROLE"):
        raise WorkflowError("An already-running executor must not launch another executor")
    if managed:
        from sessions import managed_launch

        return managed_launch(repo, role, task, execute=execute)
    cfg = configuration(repo.root)
    if not re.fullmatch(r"gpt-[a-zA-Z0-9.-]+", cfg["openai_model"]):
        raise WorkflowError("Non-review roles must use an explicitly configured OpenAI GPT model")
    if role in {"implement", "repair"}:
        if repo.root == repo.main or not re.fullmatch(
            r"issue-[1-9][0-9]*-[a-z0-9-]+", repo.git("branch", "--show-current")
        ):
            raise WorkflowError("Implementation and repair require an issue task worktree")
    prompt = (repo.root / f".agentic/prompts/{role}.md").read_text(encoding="utf-8")
    prompt += f"\nTask identifier: {task}\nRepository: {repo.root}\n"
    args = [
        "codex",
        "--model",
        cfg["openai_model"],
        "--sandbox",
        "workspace-write" if role in {"implement", "repair"} else "read-only",
        "--ask-for-approval",
        "on-request",
        "--cd",
        str(repo.root),
        prompt,
    ]
    if execute:
        return subprocess.call(args)
    return {"command": shlex.join(args), "model": cfg["openai_model"]}


def main():
    from tasks import COMMANDS, add_commands, dispatch

    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    add_commands(sub)
    sub.add_parser("doctor")
    sub.add_parser("memory-init")
    new = sub.add_parser("new-task")
    new.add_argument("issue")
    new.add_argument("slug")
    new.add_argument("base", nargs="?")
    clean = sub.add_parser("cleanup-task")
    clean.add_argument("pr")
    draft = sub.add_parser("draft-pr")
    draft.add_argument("--title", required=True)
    draft.add_argument("--body-file", required=True)
    rule = sub.add_parser("ruleset")
    rule.add_argument("--check", action="append", required=True)
    merge = sub.add_parser("merge-preflight")
    merge.add_argument("pr")
    merge.add_argument("--reviewed-sha", required=True)
    agent = sub.add_parser("launch")
    agent.add_argument("role", choices=["draft", "plan", "implement", "repair"])
    agent.add_argument("task")
    agent.add_argument("--execute", action="store_true")
    agent.add_argument("--managed", action="store_true")
    args = parser.parse_args()
    try:
        repo = Repo()
        if args.command == "doctor":
            result = {
                "root": str(repo.root),
                "main": str(repo.main),
                "tools": {tool: shutil.which(tool) for tool in ["git", "gh", "codex", "copilot"]},
                "git_clean": not bool(repo.git("status", "--porcelain")),
                "github_authenticated": run(["gh", "auth", "status"], check=False).returncode == 0
                if shutil.which("gh")
                else False,
                "config": configuration(repo.root),
            }
            print(json.dumps(result, indent=2))
            return 0 if all(result["tools"].values()) and result["github_authenticated"] else 1
        if args.command in COMMANDS:
            result = dispatch(repo, args)
        elif args.command == "memory-init":
            if repo.git("ls-files", "memory"):
                raise WorkflowError("memory is already tracked; ignoring it cannot remove it from history")
            ignored = run(["git", "-C", repo.root, "check-ignore", "memory/probe.md"], check=False)
            if ignored.returncode:
                raise WorkflowError("Add /memory/ to .gitignore before creating private memory")
            (repo.root / "memory").mkdir(exist_ok=True)
            readme = repo.root / "memory/README.md"
            if not readme.exists():
                readme.write_text(
                    "# Private project memory\n\nGit-ignored context. Never store credentials here.\n"
                )
            result = {"memory": str(readme.parent)}
        elif args.command == "new-task":
            result = new_task(repo, args.issue, args.slug, args.base)
        elif args.command == "cleanup-task":
            result = cleanup_task(repo, args.pr)
        elif args.command == "draft-pr":
            result = draft_pr(repo, args.title, args.body_file)
        elif args.command == "ruleset":
            result = ruleset(repo, args.check)
        elif args.command == "merge-preflight":
            result = merge_preflight(repo, positive(args.pr), args.reviewed_sha)
        elif args.command == "launch":
            result = launch(repo, args.role, args.task, args.execute, args.managed)
            if isinstance(result, int):
                return result
        print(json.dumps(result, indent=2))
        return 1 if isinstance(result, dict) and result.get("incomplete") else 0
    except (WorkflowError, OSError, ValueError, subprocess.TimeoutExpired) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    # Helpers must share this module's exception class and low-level interfaces.
    sys.modules["workflow"] = sys.modules[__name__]
    sys.exit(main())
