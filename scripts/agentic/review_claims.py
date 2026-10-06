"""Exclusive material claims shared by independently prepared v7 batches.

Claims are append-only local accounting. A crash consumes the claim; deleting a
batch ledger never permits another dispatch. Only an explicitly validated stopped
ancestor can name the previous claim for a new finite successor allocation.
"""

import contextlib
import fcntl
import os
import stat

from claude_reporting_execution import exclusive
from reporting_activation import read
from tasks import digest, plain_path, private_directory
from workflow import WorkflowError


def root(repo):
    return plain_path(repo.main / ".agentic-local/review-claims-v1")


def record(batch, unit, binding, previous=None):
    identity = {
        key: batch["binding"][key]
        for key in ("repository", "pr", "issue", "plan_comment", "head_sha", "base_sha", "merge_base_sha")
    }
    keys = sorted(digest([identity, batch["contract_digest"], key]) for key in unit["required_ids"])
    if not keys or len(set(keys)) != len(keys):
        raise WorkflowError("Exclusive claims require unique original material obligations")
    return {
        "schema_version": 1,
        "keys": keys,
        "batch_sha256": digest(batch),
        "authorization_digest": digest(batch["authorization"]),
        "reservation_digest": digest(binding),
        "unit": unit["id"],
        "previous_digest": previous,
    }


@contextlib.contextmanager
def locked(repo):
    directory = root(repo)
    private_directory(directory)
    fd = os.open(
        plain_path(directory / "claims.lock"), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600
    )
    with os.fdopen(fd, "a") as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise WorkflowError("Global claim lock is not a regular file")
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise WorkflowError("Another batch claim is active") from None
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


def history(repo, key):
    directory = plain_path(root(repo) / key)
    if not directory.exists():
        return directory, []
    files = []
    for path in directory.iterdir():
        if len(files) >= 256:
            raise WorkflowError("Excessive global claim history")
        files.append(path)
    files.sort()
    if (
        not files
        or len(files) > 256
        or [p.name for p in files] != [f"{i:04d}.json" for i in range(1, len(files) + 1)]
    ):
        raise WorkflowError("Missing, ambiguous or excessive global claim history")
    records = [read(plain_path(path)) for path in files]
    for index, value in enumerate(records):
        if value.get("previous_digest") != (digest(records[index - 1]) if index else None):
            raise WorkflowError("Global claim history lost its predecessor")
    return directory, records


def reserve(repo, batch, unit, binding, *, previous=None, successor=None):
    if previous is not None:
        raise WorkflowError("Successor claims require the separate versioned continuation lifecycle")
    if successor is not None:
        import review_continuation
        from review_batch_v7 import load

        if digest(load(successor)) != digest(batch):
            raise WorkflowError("Successor batch binding differs")
        previous = review_continuation.previous(repo, successor, batch, unit)
    value = record(batch, unit, binding, previous)
    with locked(repo):
        targets = []
        for key in value["keys"]:
            directory, old = history(repo, key)
            if (digest(old[-1]) if old else None) != previous:
                raise WorkflowError("Material already claimed by another or uncertain batch")
            if len(old) >= 256:
                raise WorkflowError("Finite global claim history exhausted")
            targets.append((directory, len(old) + 1))
        # Validate the whole assignment before the first write. A partial write
        # remains consumed; neither the ledger nor a second manifest can reset it.
        for directory, number in targets:
            private_directory(directory)
            exclusive(directory / f"{number:04d}.json", value, limit=2_000_000)
    return value


def verify(repo, batch, unit, row):
    value = row.get("claim")
    if type(value) is not dict or digest(value) != digest(
        record(batch, unit, row["binding"], value.get("previous_digest"))
    ):
        raise WorkflowError("Batch reservation lost its exact material claim")
    for key in value["keys"]:
        _, old = history(repo, key)
        if not old or digest(old[-1]) != digest(value):
            raise WorkflowError("Current global material claim differs or is missing")
    return value
