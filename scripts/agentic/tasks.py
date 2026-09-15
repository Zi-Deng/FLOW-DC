"""Coordinator-owned contracts, recoverable publications and task registration."""

from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import fcntl
import hashlib
import json
import os
import re
import stat
import subprocess
import tempfile
import uuid
from pathlib import Path

from workflow import WorkflowError, new_task, positive, run

COMMANDS = {
    "capture",
    "plan",
    "approve-plan",
    "prepare",
    "task-status",
    "recover-executor",
    "bind-pr",
    "publish-pr",
    "feedback",
    "respond",
    "task-review",
    "finish-prepare",
}


def digest(value):
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def plain_path(path):
    """Reject symlink ancestors and nonregular record files before state access."""
    path = Path(path).absolute()
    for parent in path.parents:
        if parent.is_symlink():
            raise WorkflowError(f"State path contains a symlink: {parent}")
        if parent.exists() and not parent.is_dir():
            raise WorkflowError(f"State ancestor is not a directory: {parent}")
    if path.is_symlink():
        raise WorkflowError(f"State path is a symlink: {path}")
    return path


def atomic_json(path, value):
    path = plain_path(path)
    if path.exists() and not path.is_file():
        raise WorkflowError("State destination is not a regular file")
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, temporary = tempfile.mkstemp(prefix=".pending-", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(value, stream, indent=2, ensure_ascii=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def operation_key(value):
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,79}", value):
        raise WorkflowError("Operation key must be 1–80 lowercase letters, digits or hyphens")
    return value


class TaskStore:
    """Local bookkeeping, never a substitute for current Git/GitHub evidence."""

    def __init__(self, repo):
        if os.environ.get("AGENTIC_EXECUTOR_ROLE"):
            raise WorkflowError("Managed executors must not invoke coordinator operations")
        repo.assert_main()
        ignored = run(
            ["git", "-C", repo.main, "check-ignore", ".agentic-local/probe"],
            check=False,
        )
        if ignored.returncode or repo.git("ls-files", ".agentic-local"):
            raise WorkflowError("Control .agentic-local must be ignored and untracked")
        self.repo = repo
        self.directory = plain_path(repo.main / ".agentic-local/tasks")
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)

    def path(self, key):
        return plain_path(self.directory / f"{operation_key(key)}.json")

    def read(self, key):
        path = self.path(key)
        if not path.exists():
            return {
                "schema_version": 1,
                "repository": self.repo.name,
                "key": key,
                "operations": {},
            }
        if not path.is_file():
            raise WorkflowError("Task record is not a regular file")
        value = json.loads(path.read_text(encoding="utf-8"))
        if (
            value.get("schema_version") != 1
            or value.get("repository") != self.repo.name
            or value.get("key") != key
            or not isinstance(value.get("operations"), dict)
        ):
            raise WorkflowError("Task record identity or schema differs")
        return value

    def save(self, value):
        if value.get("repository") != self.repo.name:
            raise WorkflowError("Task record belongs to another repository")
        atomic_json(self.path(value["key"]), value)

    @contextlib.contextmanager
    def locked(self, key):
        path = plain_path(self.directory / f"{operation_key(key)}.lock")
        fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "a") as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                raise WorkflowError("Task lock is not a regular file")
            try:
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise WorkflowError("Another local operation owns this task") from exc
            try:
                yield self.read(key)
            finally:
                fcntl.flock(stream, fcntl.LOCK_UN)


def body_text(path):
    text = Path(path).read_text(encoding="utf-8")
    if not text.strip() or len(text.encode("utf-8")) > 60000:
        raise WorkflowError("Publication body must be nonempty and at most 60000 bytes")
    return text


def receipt(item):
    result = {"id": positive(item["id"]), "url": item["html_url"]}
    if "number" in item:
        result["number"] = positive(item["number"])
    return result


def publish_once(store, state, label, endpoint, listing, payload, *, retry_confirmed_absent=False):
    """Journal before writing; reconcile uncertain creates without blind retries."""
    repo = store.repo
    fingerprint = digest({"endpoint": endpoint, "payload": payload})
    operations = state["operations"]
    if label not in operations:
        operations[label] = {
            "digest": fingerprint,
            "marker": f"<!-- agentic-operation:{uuid.uuid4()} -->",
            "phase": "prepared",
        }
        store.save(state)
    operation = operations[label]
    if operation["digest"] != fingerprint:
        raise WorkflowError("Publication key already binds different content")
    posted = dict(payload)
    posted["body"] = payload["body"].rstrip() + "\n\n" + operation["marker"] + "\n"

    def reconcile():
        matches = [
            item
            for item in repo.api(listing, paginate=True)
            if operation["marker"] in (item.get("body") or "")
        ]
        if len(matches) > 1:
            raise WorkflowError("Multiple publications carry this operation marker")
        if not matches:
            return None
        item = matches[0]
        if item.get("body") != posted["body"] or ("title" in posted and item.get("title") != posted["title"]):
            raise WorkflowError("Published content changed; reconcile it explicitly")
        if operation.get("receipt") and operation["receipt"]["id"] != positive(item["id"]):
            raise WorkflowError("Publication ID changed")
        operation.update(phase="published", receipt=receipt(item))
        store.save(state)
        return operation["receipt"]

    existing = reconcile()
    if existing:
        return existing
    if operation["phase"] == "published":
        raise WorkflowError("Recorded publication is no longer observable; do not recreate it")
    if operation["phase"] == "inflight" and not retry_confirmed_absent:
        raise WorkflowError(
            "Publication outcome is ambiguous. Investigate before explicitly retrying "
            "with --retry-confirmed-absent"
        )
    repo.assert_main()
    operation["phase"] = "inflight"
    store.save(state)
    try:
        created = repo.api(endpoint, data=posted)
        result = receipt(created)
    except (WorkflowError, OSError, ValueError, KeyError, subprocess.SubprocessError) as exc:
        existing = reconcile()
        if existing:
            return existing
        raise WorkflowError("Publication outcome is ambiguous; the operation remains incomplete") from exc
    operation.update(phase="published", receipt=result)
    store.save(state)
    return result


def capture(repo, key, title, body_file, retry_confirmed_absent=False):
    operation_key(key)
    if not title.strip() or len(title) > 256:
        raise WorkflowError("Issue title must contain 1–256 characters")
    store = TaskStore(repo)
    with store.locked("capture-" + digest(key)) as state:
        return publish_once(
            store,
            state,
            "capture",
            "issues",
            "issues?state=all&per_page=100",
            {"title": title, "body": body_text(body_file)},
            retry_confirmed_absent=retry_confirmed_absent,
        )


def issue_contract(repo, number, comment):
    number, comment = positive(number), positive(comment)
    issue = repo.api(f"issues/{number}")
    if issue.get("state") != "open" or "pull_request" in issue:
        raise WorkflowError("Contract requires an open issue, not a pull request")
    plan = repo.api(f"issues/comments/{comment}")
    if plan.get("issue_url", "").rstrip("/") != issue["url"].rstrip("/"):
        raise WorkflowError("Plan comment does not belong to this issue")
    return {
        "issue": number,
        "plan_comment": comment,
        "issue_digest": digest({"title": issue["title"], "body": issue.get("body") or ""}),
        "plan_digest": digest({"id": comment, "body": plan.get("body") or ""}),
    }


def verify_contract(repo, state):
    approval = state.get("approval")
    if not approval:
        raise WorkflowError("Record explicit human approval before preparing or executing")
    observed = issue_contract(repo, approval["issue"], approval["plan_comment"])
    if observed != approval["contract"]:
        raise WorkflowError("Approved issue or plan changed; reconcile the contract")
    return observed


def plan(repo, number, body_file, retry_confirmed_absent=False):
    number = positive(number)
    store = TaskStore(repo)
    text = body_text(body_file)
    with store.locked(f"issue-{number}") as state:
        issue = repo.api(f"issues/{number}")
        if issue.get("state") != "open" or "pull_request" in issue:
            raise WorkflowError("Planning requires an open issue")
        result = publish_once(
            store,
            state,
            "plan-" + digest(text),
            f"issues/{number}/comments",
            f"issues/{number}/comments?per_page=100",
            {"body": text},
            retry_confirmed_absent=retry_confirmed_absent,
        )
        state["proposed_plan_comment"] = result["id"]
        store.save(state)
        return result


def approve_plan(repo, number, comment, approval_source, supersede=False):
    number, comment = positive(number), positive(comment)
    if not approval_source.strip() or len(approval_source) > 2000:
        raise WorkflowError("State the source of the existing human authorization")
    store = TaskStore(repo)
    with store.locked(f"issue-{number}") as state:
        contract = issue_contract(repo, number, comment)
        previous = state.get("approval")
        if state.get("workspace") and not previous:
            raise WorkflowError("Registered task has no original approval; recover its record")
        changed = previous is not None and previous["contract"] != contract
        if changed and not supersede:
            raise WorkflowError("Approved task contract cannot be silently replaced; use --supersede")
        if changed:
            state.setdefault("approval_history", []).append(previous)
            state.pop("designated_review", None)
            state.pop("finish", None)
            state["preparation"] = "incomplete"
            state["contract_generation"] = state.get("contract_generation", 1) + 1
        state["approval"] = {
            "issue": number,
            "plan_comment": comment,
            "contract": contract,
            "source": approval_source,
            "recorded_at": dt.datetime.now(dt.UTC).isoformat(),
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        }
        store.save(state)
        return state["approval"]


def workspace(repo, state):
    registered = state.get("workspace")
    if not registered:
        raise WorkflowError("Task has no registered workspace")
    number = positive(state["approval"]["issue"])
    branch = registered["branch"]
    if not re.fullmatch(rf"issue-{number}-[a-z0-9]+(?:-[a-z0-9]+)*", branch):
        raise WorkflowError("Registered branch differs from the task")
    expected = plain_path(repo.worktree_root() / branch)
    if registered["worktree"] != str(expected) or registered["base"] != repo.base:
        raise WorkflowError("Registered workspace path or base differs")
    trees = [t for t in repo.worktrees() if t.get("branch") == f"refs/heads/{branch}"]
    if (
        len(trees) != 1
        or Path(trees[0]["worktree"]).resolve() != expected
        or "locked" in trees[0]
        or not expected.is_dir()
    ):
        raise WorkflowError("Task worktree registration is missing, moved or locked")
    actual = run(["git", "-C", expected, "branch", "--show-current"]).stdout.strip()
    if actual != branch:
        raise WorkflowError("Task worktree is on a different branch")
    return expected


def prepare(repo, number, slug):
    number = positive(number)
    if not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", slug) or len(slug) > 80:
        raise WorkflowError("Slug must be lowercase hyphenated words, at most 80 characters")
    store = TaskStore(repo)
    with store.locked(f"issue-{number}") as state:
        verify_contract(repo, state)
        branch = f"issue-{number}-{slug}"
        expected = plain_path(repo.worktree_root() / branch)
        registration = {
            "branch": branch,
            "worktree": str(expected),
            "base": repo.base,
        }
        if state.get("workspace") and state["workspace"] != registration:
            raise WorkflowError("Task is already assigned to another workspace")
        state["workspace"] = registration
        state["preparation"] = "incomplete"
        store.save(state)
        trees = [t for t in repo.worktrees() if t.get("branch") == f"refs/heads/{branch}"]
        if not expected.exists() and not trees:
            state["creation_attempted"] = True
            store.save(state)
            new_task(repo, number, slug)
        with repo.lock():
            path = workspace(repo, state)
            upstream = run(
                ["git", "-C", path, "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"],
                check=False,
            )
            initial_upstream = (
                state.get("creation_attempted") and upstream.stdout.strip() == f"origin/{repo.base}"
            )
            if upstream.returncode or initial_upstream:
                # A newly created branch initially tracks the base until its first push.
                run(["git", "-C", path, "push", "-u", "origin", branch])
            elif upstream.stdout.strip() != f"origin/{branch}":
                raise WorkflowError("Task branch tracks an unexpected upstream")
            verify_contract(repo, state)
            state.pop("creation_attempted", None)
            state["preparation"] = "prepared"
            store.save(state)
            return {
                **registration,
                "status": run(["git", "-C", path, "status", "--porcelain"]).stdout,
            }


def task_status(repo, number):
    number = positive(number)
    store = TaskStore(repo)
    with store.locked(f"issue-{number}") as state:
        observed = {"issue": repo.api(f"issues/{number}").get("state")}
        if state.get("workspace"):
            try:
                path = workspace(repo, state)
                observed["head"] = run(["git", "-C", path, "rev-parse", "HEAD"]).stdout.strip()
                observed["status"] = run(["git", "-C", path, "status", "--porcelain"]).stdout
            except WorkflowError as exc:
                observed["workspace_observation"] = str(exc)
        return {"record": state, "observed": observed}


def add_commands(sub):
    from pipeline import add_commands as add_pipeline_commands

    add_pipeline_commands(sub)
    capture_parser = sub.add_parser("capture")
    capture_parser.add_argument("--key", required=True)
    capture_parser.add_argument("--title", required=True)
    capture_parser.add_argument("--body-file", required=True)
    capture_parser.add_argument("--retry-confirmed-absent", action="store_true")
    plan_parser = sub.add_parser("plan")
    plan_parser.add_argument("issue")
    plan_parser.add_argument("--body-file", required=True)
    plan_parser.add_argument("--retry-confirmed-absent", action="store_true")
    approval = sub.add_parser("approve-plan")
    approval.add_argument("issue")
    approval.add_argument("--plan-comment", required=True)
    approval.add_argument(
        "--supersede",
        action="store_true",
        help="Explicitly replace the approved contract, retaining history and executor identity",
    )
    approval.add_argument(
        "--approval-source",
        required=True,
        help="Operator assertion identifying existing human approval, never inferred from public text",
    )
    preparation = sub.add_parser("prepare")
    preparation.add_argument("issue")
    preparation.add_argument("slug")
    status = sub.add_parser("task-status")
    status.add_argument("issue")
    recovery = sub.add_parser("recover-executor")
    recovery.add_argument("issue")
    recovery.add_argument("--session", required=True)
    recovery.add_argument("--record-file", required=True)
    recovery.add_argument("--recovery-source", required=True)
    recovery.add_argument("--confirm-stopped", action="store_true", required=True)
    finishing = sub.add_parser("finish-prepare")
    finishing.add_argument("issue")
    finishing.add_argument("--assessment-file", required=True)
    finishing.add_argument("--ready", action="store_true")


def dispatch(repo, args: argparse.Namespace):
    if args.command == "capture":
        return capture(repo, args.key, args.title, args.body_file, args.retry_confirmed_absent)
    if args.command == "plan":
        return plan(repo, args.issue, args.body_file, args.retry_confirmed_absent)
    if args.command == "approve-plan":
        return approve_plan(repo, args.issue, args.plan_comment, args.approval_source, args.supersede)
    if args.command == "prepare":
        return prepare(repo, args.issue, args.slug)
    if args.command == "task-status":
        return task_status(repo, args.issue)
    if args.command == "recover-executor":
        from sessions import recover_executor

        return recover_executor(
            repo,
            args.issue,
            args.session,
            args.record_file,
            args.recovery_source,
            args.confirm_stopped,
        )
    if args.command == "finish-prepare":
        from finish import prepare_finish

        return prepare_finish(repo, args.issue, args.assessment_file, args.ready)
    from pipeline import dispatch as dispatch_pipeline

    if args.command in COMMANDS:
        return dispatch_pipeline(repo, args)
    raise WorkflowError("Unknown task operation")
