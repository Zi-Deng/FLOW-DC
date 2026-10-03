"""Private, manually supplied subscription credentials. Never log secret material."""

from __future__ import annotations

import datetime as dt
import getpass
import hashlib
import json
import os
import stat
import sys
import tempfile
import warnings
from pathlib import Path

from workflow import WorkflowError

RECEIPT_DAYS = 7
MAX_TOKEN_BYTES = 16384


def default_path():
    return Path.home() / ".config/flowdc-agentic/claude-review-token"


def guarded_path(path, *, create=False):
    path = Path(path).absolute()
    if ".." in path.parts or any(p.is_symlink() for p in (path, *path.parents)):
        raise WorkflowError("Credential path must not contain symlinks or parent traversal")
    for parent in path.parents:
        marker = parent / ".git"
        if (
            marker.is_file()
            or marker.is_symlink()
            or (marker / "HEAD").is_file()
            or ((parent / "HEAD").is_file() and (parent / "objects").is_dir())
        ):
            raise WorkflowError("Subscription credentials and receipt must remain outside Git checkouts")
    if create:
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = path.parent.stat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700:
        raise WorkflowError("Dedicated credential directory must be owned by you with mode 0700")
    return path


def read_private(path, *, limit=MAX_TOKEN_BYTES):
    path = guarded_path(path)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(fd)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_nlink != 1
        ):
            raise WorkflowError("Credential files must be single-link owner-only regular files (0600)")
        with os.fdopen(fd, "rb", closefd=False) as stream:
            value = stream.read(limit + 1)
        if not value or len(value) > limit:
            raise WorkflowError("Credential file is empty or exceeds its bound")
        return value
    finally:
        os.close(fd)


def write_private(path, data, *, replace=False):
    path = guarded_path(path, create=True)
    if path.exists():
        read_private(path)
        if not replace:
            raise WorkflowError("Existing credential/receipt requires explicit replacement")
    fd, name = tempfile.mkstemp(prefix=".subscription-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            os.fchmod(stream.fileno(), 0o600)
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        if replace:
            guarded_path(path)
            if path.exists():
                read_private(path)
            os.replace(name, path)
        else:
            os.link(name, path)  # Atomic no-clobber, even if another setup won the race.
        parent_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)
    finally:
        Path(name).unlink(missing_ok=True)


def validate_token(raw):
    try:
        token = raw.decode("ascii")
    except UnicodeError:
        raise WorkflowError("Unsupported subscription token format") from None
    if (
        len(raw) > MAX_TOKEN_BYTES
        or not token.startswith("sk-ant-oat01-")
        or len(token) < 24
        or any(c.isspace() or ord(c) < 33 for c in token)
    ):
        raise WorkflowError("Use a dedicated subscription token generated manually with claude setup-token")
    return token


def receipt_path(path):
    return Path(str(path) + ".receipt.json")


def setup(path=None, *, replace=False, paid_usage_disabled=False):
    if not paid_usage_disabled:
        raise WorkflowError("Confirm paid usage credits/extra usage are disabled before setup")
    path = guarded_path(path or default_path(), create=True)
    if not sys.stdin.isatty():
        raise WorkflowError("Subscription setup requires a terminal with hidden input")
    with warnings.catch_warnings():
        warnings.simplefilter("error", getpass.GetPassWarning)
        try:
            token = getpass.getpass("Dedicated claude setup-token value (hidden): ")
        except (getpass.GetPassWarning, EOFError):
            raise WorkflowError("Hidden credential input unavailable") from None
    raw = token.encode("utf-8")
    validate_token(raw)
    now = dt.datetime.now(dt.UTC)
    receipt = {
        "schema_version": 1,
        "assertion": "operator-confirms-paid-usage-credits-and-extra-usage-disabled",
        "billing_mode": "included-max-subscription-only",
        "token_sha256": hashlib.sha256(raw).hexdigest(),
        "recorded_at": now.isoformat(),
        "expires_at": (now + dt.timedelta(days=RECEIPT_DAYS)).isoformat(),
    }
    # A partial setup cannot activate: read() requires both matching records.
    write_private(path, raw, replace=replace)
    write_private(receipt_path(path), json.dumps(receipt).encode("utf-8"), replace=replace)
    return {
        "configured": True,
        "receipt_valid_days": RECEIPT_DAYS,
        "operator_assertion_not_billing_guarantee": True,
    }


def read(path=None, *, now=None):
    try:
        path = path or default_path()
        raw = read_private(path)
        token = validate_token(raw)
        from review_coverage_v2 import strict_json

        receipt = strict_json(read_private(receipt_path(path)).decode("utf-8"))
        if not isinstance(receipt, dict) or set(receipt) != {
            "schema_version",
            "assertion",
            "billing_mode",
            "token_sha256",
            "recorded_at",
            "expires_at",
        }:
            raise ValueError
        if (
            type(receipt["schema_version"]) is not int
            or receipt["schema_version"] != 1
            or receipt["assertion"] != "operator-confirms-paid-usage-credits-and-extra-usage-disabled"
            or receipt["billing_mode"] != "included-max-subscription-only"
            or receipt["token_sha256"] != hashlib.sha256(raw).hexdigest()
        ):
            raise ValueError
        start, end = (dt.datetime.fromisoformat(receipt[k]) for k in ("recorded_at", "expires_at"))
        current = now or dt.datetime.now(dt.UTC)
        if (
            not start.tzinfo
            or not end.tzinfo
            or not start <= current < end
            or end - start > dt.timedelta(days=RECEIPT_DAYS)
        ):
            raise ValueError
        return token
    except (OSError, ValueError, TypeError, KeyError, UnicodeError, WorkflowError):
        raise WorkflowError(
            "Claude activation requires a protected subscription token and current matching disabled-paid-usage receipt"
        ) from None


def status():
    try:
        read()
        return []
    except WorkflowError:
        return ["dedicated_subscription_token_or_current_disabled_paid_usage_receipt_unavailable"]
