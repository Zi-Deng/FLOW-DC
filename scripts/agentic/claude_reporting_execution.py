"""Prospective native dispatch bindings; no authorization or provider invocation."""

from __future__ import annotations

import hashlib
import os
import uuid
from pathlib import Path

from tasks import digest, plain_path
from workflow import WorkflowError

FILENAME = "reporting-execution.json"


def require_activation(repo, meta):
    # Deliberately closed until the separate purpose-10/11 lifecycle is wired.
    # The old nine-attempt ledger must never authorize this reporting policy.
    raise WorkflowError("Prospective v7 dispatch requires the separate reporting activation implementation")


def binding(directory, meta, session_id, prompt):
    from claude_reporting_policy import validate
    from review import RESULT_FIELDS
    from review_prompt import native

    validate(meta["review_policy"])
    if (
        meta.get("schema_version") != 7
        or type(session_id) is not str
        or str(uuid.UUID(session_id)) != session_id
        or prompt != native(directory, meta)
    ):
        raise WorkflowError("Reporting invocation identity or prompt differs")
    return {
        "schema_version": 1,
        "input_digest": digest({k: v for k, v in meta.items() if k not in RESULT_FIELDS}),
        "policy_digest": digest(meta["review_policy"]),
        "reporting_digest": digest(meta["review_policy"]["reporting"]),
        "session_id": session_id,
        "prompt_sha256": hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
        "wrapper_invocations": 1,
    }


def exclusive(path, value):
    """No replacement even after a crash or competing entrypoint's reservation."""
    from claude_reporting import _json_bytes

    raw = _json_bytes(value, 10000)
    try:
        fd = os.open(plain_path(path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        raise WorkflowError("Prior reporting attempt cannot be repeated") from None
    with os.fdopen(fd, "wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    parent = os.open(Path(path).parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(parent)
    finally:
        os.close(parent)


def reserve(directory, meta, session_id, prompt, attempt):
    record = binding(directory, meta, session_id, prompt)
    # The first exclusive file is the dispatch claim. Failure between writes
    # leaves it consumed/uncertain, never a new available invocation.
    exclusive(Path(directory) / FILENAME, record)
    exclusive(Path(directory) / "attempt.json", {**attempt, "execution_sha256": digest(record)})
    return record


def retained(directory, meta):
    """Read only, bounded and typed; no credential store or provider access."""
    from review import exact_reporting_bytes
    from review_coverage import strict_json
    from review_prompt import native

    path = plain_path(Path(directory) / FILENAME)
    if not path.exists():
        return None
    try:
        record = strict_json(exact_reporting_bytes(path, 10000).decode("utf-8"))
        expected = binding(directory, meta, record.get("session_id"), native(directory, meta))
        if digest(record) != digest(expected):
            raise ValueError
    except (ValueError, TypeError, KeyError, AttributeError):
        raise WorkflowError("Reporting execution binding changed") from None
    return record


def validate_capture(directory, meta, capture):
    from review import read_result_artifact

    record = retained(directory, meta)
    if record is None and "execution" not in capture:
        # Offline schema-7 storage fixtures remain recoverable as before; they
        # cannot acquire current readiness or authorize a provider invocation.
        return
    if record is None or digest(capture.get("execution")) != digest(record):
        raise WorkflowError("Reporting capture lost or changed its execution binding")
    attempt = read_result_artifact(directory, "attempt.json", meta)
    expected = {
        "schema_version": 7,
        "input_digest": record["input_digest"],
        "policy_digest": record["policy_digest"],
        "status": "started",
        "requests": 1,
        "execution_sha256": digest(record),
    }
    finished = {
        **expected,
        "status": "finished",
        "cli_version": capture["provider_version"],
        "reasons": capture["diagnostics"]["reasons"],
    }
    if digest(attempt) not in {digest(expected), digest(finished)}:
        raise WorkflowError("Reporting attempt differs from its exact capture")
