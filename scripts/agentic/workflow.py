#!/usr/bin/env python3
"""Small GitHub workflow helpers. Implementation stays in the working agent."""

import argparse
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


class WorkflowError(RuntimeError):
    pass


def run(args, *, cwd=None, check=True, **kwargs):
    result = subprocess.run(args, cwd=cwd, capture_output=True, text=True, timeout=120, **kwargs)
    if check and result.returncode:
        raise WorkflowError(f"{Path(str(args[0])).name} failed (exit {result.returncode})")
    return result


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")
    path.chmod(0o600)


def configuration(root=ROOT):
    value = json.loads((Path(root) / ".agentic/config.json").read_text())
    for name, low, high in (
        ("review_timeout_seconds", 1, 900),
        ("review_max_turns", 1, 100),
        ("max_snapshot_bytes", 1, 12_000_000),
        ("max_source_file_bytes", 1, 500_000),
        ("review_max_ai_credits", 1, 400),
        ("review_max_estimated_usd", 1, 10),
    ):
        if type(value.get(name)) is not int or not low <= value[name] <= high:
            raise WorkflowError(f"Invalid finite workflow setting: {name}")
    if value.get("review_provider") not in {"claude-code", "copilot"}:
        raise WorkflowError("Unknown reviewer provider")
    return value


class Repo:
    def __init__(self, root="."):
        self.root = Path(run(["git", "rev-parse", "--show-toplevel"], cwd=root).stdout.strip())
        url = self.git("remote", "get-url", "origin")
        match = re.fullmatch(r"(?:https://github\.com/|git@github\.com:)([\w.-]+/[\w.-]+?)(?:\.git)?", url)
        if not match:
            raise WorkflowError("Expected a GitHub origin")
        self.name = match[1]
        self.base = "main"
        first = self.git("worktree", "list", "--porcelain").splitlines()[0]
        self.main = Path(first.removeprefix("worktree "))
        self.state = self.main / ".agentic-local"

    def git(self, *args):
        return run(["git", *args], cwd=self.root).stdout.strip()

    def api(self, suffix, *, data=None, method=None):
        args = ["gh", "api", f"repos/{self.name}/{suffix}", "--method", method or ("POST" if data else "GET")]
        if data is None:
            return json.loads(run(args).stdout or "null")
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json") as body:
            json.dump(data, body)
            body.flush()
            return json.loads(run([*args, "--input", body.name]).stdout or "null")

    def fetch(self, ref):
        self.git("fetch", "origin", ref)


def new_task(repo, issue, slug):
    if issue <= 0 or not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", slug):
        raise WorkflowError("Use a positive issue and lowercase slug")
    branch = f"issue-{issue}-{slug}"
    path = repo.main.parent / (repo.main.name + "-worktrees") / branch
    registered = repo.git("worktree", "list", "--porcelain")
    if f"worktree {path}\n" in registered:
        return {"branch": branch, "worktree": str(path), "reused": True}
    if path.exists():
        raise WorkflowError("Existing unregistered path; preserve it")
    repo.fetch(repo.base)
    path.parent.mkdir(parents=True, exist_ok=True)
    repo.git("worktree", "add", "-b", branch, str(path), "FETCH_HEAD")
    return {"branch": branch, "worktree": str(path)}


def draft_pr(repo, title, body_file):
    branch = repo.git("branch", "--show-current")
    match = re.fullmatch(r"issue-([1-9][0-9]*)-[a-z0-9-]+", branch)
    text = Path(body_file).read_text()
    if not match or not re.search(rf"(?im)^Fixes #{match[1]}\s*$", text):
        raise WorkflowError("Use an issue branch and standalone Fixes #N in the body")
    if repo.git("status", "--porcelain"):
        raise WorkflowError("Commit intended changes before publishing")
    existing = json.loads(
        run(
            [
                "gh",
                "pr",
                "list",
                "--repo",
                repo.name,
                "--head",
                branch,
                "--state",
                "open",
                "--json",
                "number,url",
            ]
        ).stdout
    )
    if len(existing) > 1:
        raise WorkflowError("Multiple open PRs for the branch")
    repo.git("push", "-u", "origin", branch)
    args = ["gh", "pr"]
    if existing:
        args += ["edit", str(existing[0]["number"])]
    else:
        args += ["create", "--draft", "--base", repo.base, "--head", branch]
    output = run(
        [*args, "--repo", repo.name, "--title", title, "--body-file", str(Path(body_file).resolve())]
    )
    return {
        "url": existing[0]["url"] if existing else output.stdout.strip(),
        "head": repo.git("rev-parse", "HEAD"),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    new = sub.add_parser("new-task")
    new.add_argument("issue", type=int)
    new.add_argument("slug")
    draft = sub.add_parser("draft-pr")
    draft.add_argument("--title", required=True)
    draft.add_argument("--body-file", required=True)
    selection = sub.add_parser("review-selection")
    for item in ("review-provider", "review-model", "review-effort"):
        selection.add_argument("--" + item)
    selection.add_argument("--save", action="store_true")
    login = sub.add_parser("claude-login-setup")
    login.add_argument("--paid-usage-disabled", action="store_true")
    login.add_argument("--renew", action="store_true")
    login.add_argument("--retain-capability", action="store_true", help=argparse.SUPPRESS)
    review = sub.add_parser("review")
    review.add_argument("pr", type=int)
    review.add_argument("--issue", type=int)
    review.add_argument("--plan-comment", type=int)
    review.add_argument("--publish", action="store_true")
    review.add_argument(
        "--fresh", action="store_true", help="Explicit new bounded attempt after diagnosis/delta"
    )
    for item in ("review-provider", "review-model", "review-effort"):
        review.add_argument("--" + item)
    publish = sub.add_parser("publish-review")
    publish.add_argument("directory")
    sub.add_parser("doctor")
    args = parser.parse_args()
    try:
        repo = Repo()
        if args.command == "new-task":
            value = new_task(repo, args.issue, args.slug)
        elif args.command == "draft-pr":
            value = draft_pr(repo, args.title, args.body_file)
        elif args.command == "claude-login-setup":
            import claude_auth

            value = claude_auth.login(paid_usage_disabled=args.paid_usage_disabled)
        elif args.command in {"review-selection", "doctor"}:
            import providers

            value = providers.selection(
                repo,
                configuration(),
                **(
                    {k: getattr(args, k) for k in ("review_provider", "review_model", "review_effort")}
                    if args.command == "review-selection"
                    else {}
                ),
            )
            if args.command == "review-selection" and args.save:
                write_json(repo.state / "review-selection.json", value)
        else:
            import review as reviewer

            if args.command == "publish-review":
                value = reviewer.publish(repo, Path(args.directory))
            else:
                value = reviewer.execute(
                    repo,
                    args.pr,
                    configuration(),
                    issue=args.issue,
                    plan=args.plan_comment,
                    fresh=args.fresh,
                    **{k: getattr(args, k) for k in ("review_provider", "review_model", "review_effort")},
                )
                if args.publish and value["status"] == "completed":
                    value["publication"] = reviewer.publish(repo, Path(value["directory"]))
        print(json.dumps(value, indent=2))
        return 1 if value.get("status") == "failed" else 0
    except (WorkflowError, OSError, ValueError, subprocess.TimeoutExpired) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.modules["workflow"] = sys.modules[__name__]
    sys.exit(main())
