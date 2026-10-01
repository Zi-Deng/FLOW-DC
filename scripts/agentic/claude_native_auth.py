"""Guarded native Max registration and access-only invocation snapshots.

Native setup is deliberately gated on verified callback isolation. Owner-writable
provenance is accounting, not server attestation or a cryptographic billing proof.
No caller may import the ordinary login or manufacture a registration from it.
"""

from __future__ import annotations

import contextlib
import copy
import fcntl
import hashlib
import json
import math
import os
import stat
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

from review_coverage_v2 import strict_json
from workflow import WorkflowError

MODE = "native-max-access-only-v1"
MAX_BYTES = 131072
RECEIPT_SECONDS = 7 * 86400
REFRESH_MARGIN = 300
CLOCK_ALLOWANCE = 60
ERROR = "Dedicated native Max registration, credentials or current paid-usage receipt are invalid"


def default_root():
    return Path.home() / ".config/flowdc-agentic/claude-review-login"


def new_binding(registration_id=None):
    return {
        "schema_version": 1,
        "mode": MODE,
        "registration_id": registration_id or str(uuid.uuid4()),
        "generation_id": str(uuid.uuid4()),
    }


def validate_binding(value):
    if (
        not isinstance(value, dict)
        or set(value) != {"schema_version", "mode", "registration_id", "generation_id"}
        or type(value["schema_version"]) is not int
        or value["schema_version"] != 1
        or value["mode"] != MODE
    ):
        raise WorkflowError("Missing or incompatible native authentication binding; prepare a fresh packet")
    for field in ("registration_id", "generation_id"):
        try:
            parsed = uuid.UUID(value[field])
            if parsed.version != 4 or str(parsed) != value[field]:
                raise ValueError
        except (ValueError, TypeError, AttributeError):
            raise WorkflowError("Invalid opaque authentication binding") from None
    return value


def finite(value):
    return type(value) in {int, float} and abs(value) < 1e16 and math.isfinite(value)


def native_records(credentials, config, timeout, *, now=None):
    """Project real reader fields, never infer subscriptionType from status/config.

    dF/gF return claudeAiOauth if accessToken exists; ult reads its subscriptionType.
    gD compares millisecond expiry to Date.now()+300000. Absent refreshToken makes
    da return no_refresh_token before lock/network. These are pinned internals.
    """
    now = time.time() if now is None else now
    try:
        if not finite(now) or now <= 0 or type(timeout) is not int or not 0 < timeout <= 900:
            raise ValueError
        if not isinstance(credentials, dict) or set(credentials) != {"claudeAiOauth"}:
            raise ValueError
        record = credentials["claudeAiOauth"]
        if not isinstance(record, dict) or record.get("subscriptionType") != "max":
            raise ValueError
        access = record.get("accessToken")
        if not isinstance(access, str) or not 16 <= len(access) <= 16384 or not access.isascii():
            raise ValueError
        if any(c.isspace() or ord(c) < 33 for c in access):
            raise ValueError
        expiry = record.get("expiresAt")
        if not finite(expiry) or expiry / 1000 - now <= timeout + REFRESH_MARGIN + CLOCK_ALLOWANCE:
            raise ValueError
        # Implausible units/far-future values must not authorize a call.
        if expiry / 1000 - now > 366 * 86400:
            raise ValueError
        scopes = record.get("scopes")
        if (
            not isinstance(scopes, list)
            or not scopes
            or any(not isinstance(s, str) or len(s) > 128 for s in scopes)
            or len(set(scopes)) != len(scopes)
            or not {"user:profile", "user:inference"}.issubset(scopes)
        ):
            raise ValueError
        if not isinstance(config, dict) or {
            "primaryApiKey",
            "apiKeyHelper",
            "user_oauth",
            "profiles",
            "env",
            "oauthToken",
            "forceLoginMethod",
            "forceLoginOrgUUID",
            "policyHelper",
            "policyHelpers",
        }.intersection(config):
            raise ValueError
        account = config.get("oauthAccount")
        if not isinstance(account, dict) or account.get("hasExtraUsageEnabled") is not False:
            raise ValueError
        identity = {}
        for key in ("accountUuid", "organizationUuid"):
            value = account.get(key)
            if not isinstance(value, str) or str(uuid.UUID(value)) != value:
                raise ValueError
            identity[key] = value
        projected = {
            key: copy.deepcopy(record[key])
            for key in ("accessToken", "expiresAt", "scopes", "subscriptionType")
        }
        # No refresh field, cached rate/feature settings, or unneeded account data.
        return {"claudeAiOauth": projected}, {"oauthAccount": identity}
    except (ValueError, TypeError, KeyError, AttributeError):
        raise WorkflowError(ERROR) from None


def _directory(path, *, create=False):
    """Walk from / with O_NOFOLLOW: no check-then-follow path traversal."""
    path = Path(path).absolute()
    if ".." in path.parts:
        raise WorkflowError(ERROR)
    fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in path.parts[1:]:
            try:
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            except FileNotFoundError:
                if not create:
                    raise
                info = os.fstat(fd)
                if info.st_uid not in {0, os.getuid()} or (
                    info.st_mode & 0o022 and not (info.st_uid == 0 and info.st_mode & stat.S_ISVTX)
                ):
                    raise WorkflowError(ERROR) from None
                os.mkdir(part, 0o700, dir_fd=fd)
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = child
        info = os.fstat(fd)
        if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700:
            raise WorkflowError(ERROR)
        return fd
    except BaseException:
        os.close(fd)
        raise


def _outside_git(root):
    # Inspect only ancestry, never ordinary Claude configuration. Real Git
    # markers and unreadable ancestry are refused, including linked worktrees.
    for parent in (root, *root.parents):
        try:
            marker = parent / ".git"
            info = marker.lstat()
            if stat.S_ISDIR(info.st_mode):
                (marker / "HEAD").lstat()
        except FileNotFoundError:
            pass
        else:
            raise WorkflowError("Native login must be outside every Git checkout")
        if (parent / "HEAD").is_file() and (parent / "objects").is_dir():
            raise WorkflowError("Native login must be outside every Git checkout")


def _regular(fd):
    info = os.fstat(fd)
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) != 0o600
        or info.st_nlink != 1
        or info.st_size > MAX_BYTES
    ):
        raise WorkflowError(ERROR)
    return info


class Store:
    def __init__(self, root, fd, lock):
        self.root, self.fd, self.lock = root, fd, lock

    def check_location(self):
        current = _directory(self.root)
        try:
            before, after = os.fstat(self.fd), os.fstat(current)
            if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
                raise WorkflowError("Dedicated registration directory changed")
            fd = os.open("lock", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=current)
            try:
                before, after = _regular(self.lock), _regular(fd)
                if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
                    raise WorkflowError("Dedicated registration lock changed")
            finally:
                os.close(fd)
        finally:
            os.close(current)

    @contextlib.contextmanager
    def parent(self, name):
        parts = Path(name).parts
        if not parts or Path(name).is_absolute() or any(p in {".", ".."} for p in parts):
            raise WorkflowError(ERROR)
        fd = os.dup(self.fd)
        try:
            for part in parts[:-1]:
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                info = os.fstat(child)
                if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700:
                    os.close(child)
                    raise WorkflowError(ERROR)
                os.close(fd)
                fd = child
            yield fd, parts[-1]
        finally:
            os.close(fd)

    def raw(self, name):
        with self.parent(name) as (parent, leaf):
            try:
                fd = os.open(leaf, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
            except FileNotFoundError:
                raise
            except OSError:
                raise WorkflowError(ERROR) from None
            try:
                before = _regular(fd)
                raw = os.read(fd, MAX_BYTES + 1)
                after = os.fstat(fd)
                if (
                    len(raw) > MAX_BYTES
                    or not raw
                    or (before.st_mtime_ns, before.st_size) != (after.st_mtime_ns, after.st_size)
                ):
                    raise WorkflowError(ERROR)
                return raw
            finally:
                os.close(fd)

    def read(self, name):
        try:
            value = strict_json(self.raw(name).decode("utf-8"))
            if not isinstance(value, dict):
                raise ValueError
            return value
        except (ValueError, UnicodeError, RecursionError):
            raise WorkflowError(ERROR) from None

    def write(self, name, value, *, replace=False):
        raw = json.dumps(value, allow_nan=False).encode("utf-8")
        if len(raw) > MAX_BYTES:
            raise WorkflowError(ERROR)
        with self.parent(name) as (parent, leaf):
            try:
                previous = self.raw(name)
            except FileNotFoundError:
                previous = None
            if previous is not None and not replace:
                raise WorkflowError("Native registration already exists; explicit renewal required")
            temporary = ".write-" + str(uuid.uuid4())
            fd = os.open(
                temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=parent
            )
            try:
                with os.fdopen(fd, "wb") as stream:
                    stream.write(raw)
                    stream.flush()
                    os.fsync(stream.fileno())
                if replace and previous is not None:
                    if self.raw(name) != previous:
                        raise WorkflowError("Native registration changed concurrently")
                    os.rename(temporary, leaf, src_dir_fd=parent, dst_dir_fd=parent)
                else:
                    os.link(temporary, leaf, src_dir_fd=parent, dst_dir_fd=parent, follow_symlinks=False)
                os.fsync(parent)
            finally:
                try:
                    os.unlink(temporary, dir_fd=parent)
                except FileNotFoundError:
                    pass


@contextlib.contextmanager
def store(root=None, *, create=False):
    root = Path(root or default_root()).absolute()
    lock = fd = None
    try:
        _outside_git(root)
        parent_fd = _directory(root.parent, create=create)
        os.close(parent_fd)
        fd = _directory(root, create=create)
        lock = os.open(
            "lock",
            os.O_RDWR | (os.O_CREAT if create else 0) | os.O_NOFOLLOW | os.O_NONBLOCK,
            0o600,
            dir_fd=fd,
        )
        _regular(lock)
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise WorkflowError("Dedicated native registration is busy") from None
        yield Store(root, fd, lock)
    except (OSError, ValueError, TypeError, KeyError, AttributeError, UnicodeError, RecursionError):
        raise WorkflowError(ERROR) from None
    finally:
        if lock is not None:
            os.close(lock)
        if fd is not None:
            os.close(fd)


def _digest(raw):
    # Private store only: never return a credential/account-derived public digest.
    return hashlib.sha256(raw).hexdigest()


def _load(storage, timeout, *, now=None, renewal=False):
    from review_policy import PROVIDERS

    storage.check_location()
    now = time.time() if now is None else now
    registration = storage.read("registration.json")
    completion = storage.read("setup-attempt.json")
    if type(completion.get("schema_version")) is not int or completion != {
        "schema_version": 1,
        "authentication": registration.get("authentication"),
        "status": "completed",
    }:
        raise WorkflowError("Native login setup is incomplete; activation refused")
    if set(registration) != {
        "authentication",
        "cli",
        "native_exit",
        "interactive",
        "account",
        "files",
        "lineage",
        "retained_capability_generations",
    }:
        raise WorkflowError(ERROR)
    binding = validate_binding(registration["authentication"])
    if (
        registration["cli"] != PROVIDERS["claude-code"]["cli"]
        or type(registration["native_exit"]) is not int
        or registration["native_exit"] != 0
        or registration["interactive"] is not True
    ):
        raise WorkflowError(ERROR)
    lineage = registration["lineage"]
    if not isinstance(lineage, list) or len(lineage) > 100:
        raise WorkflowError(ERROR)
    generations = {binding["generation_id"]}
    for previous in lineage:
        validate_binding(previous)
        if (
            previous["registration_id"] != binding["registration_id"]
            or previous["generation_id"] in generations
        ):
            raise WorkflowError(ERROR)
        generations.add(previous["generation_id"])
    retained = registration["retained_capability_generations"]
    if (
        not isinstance(retained, list)
        or any(not isinstance(item, str) for item in retained)
        or len(set(retained)) != len(retained)
        or not set(retained).issubset(generations - {binding["generation_id"]})
    ):
        raise WorkflowError(ERROR)
    prefix = "generations/" + binding["generation_id"] + "/config/"
    expected = {prefix + ".credentials.json", prefix + ".claude.json"}
    if set(registration["files"]) != expected:
        raise WorkflowError(ERROR)
    for name, checksum in registration["files"].items():
        if _digest(storage.raw(name)) != checksum:
            raise WorkflowError(ERROR)
    credentials, config = (storage.read(prefix + name) for name in (".credentials.json", ".claude.json"))
    receipt = storage.read("receipt.json")
    if set(receipt) != {
        "schema_version",
        "authentication",
        "account",
        "paid_usage_disabled",
        "recorded_at",
        "expires_at",
    }:
        raise WorkflowError(ERROR)
    if (
        type(receipt["schema_version"]) is not int
        or receipt["schema_version"] != 1
        or receipt["authentication"] != binding
        or receipt["account"] != registration["account"]
        or receipt["paid_usage_disabled"] is not True
    ):
        raise WorkflowError(ERROR)
    start, end = receipt["recorded_at"], receipt["expires_at"]
    if (
        not all(finite(v) for v in (start, end, now))
        or start > now
        or not renewal
        and now >= end
        or end <= start
        or end - start > RECEIPT_SECONDS
    ):
        raise WorkflowError(ERROR)
    snapshot, identity = native_records(credentials, config, timeout, now=start if renewal else now)
    if identity != registration["account"]:
        raise WorkflowError(ERROR)
    return registration, snapshot, identity


def current_binding(timeout=900, *, root=None):
    with store(root) as storage:
        registration, _, _ = _load(storage, timeout)
        return copy.deepcopy(registration["authentication"])


def bind(policy):
    if policy["provider"] != "claude-code":
        return policy
    return {**policy, "authentication": current_binding(policy["budget"]["timeout_seconds"])}


def capability_lineage(observed, current, timeout, *, root=None):
    """Explicit same-account renewal retains observation provenance, never rewrites it."""
    validate_binding(observed)
    validate_binding(current)
    if observed == current:
        return True
    with store(root) as storage:
        registration, _, _ = _load(storage, timeout)
        return (
            registration["authentication"] == current
            and observed in registration["lineage"]
            and observed["generation_id"] in registration["retained_capability_generations"]
        )


def status(timeout=900):
    try:
        binding = current_binding(timeout)
        return {"authentication": binding, "blockers": []}
    except WorkflowError:
        return {"mode": MODE, "blockers": ["guarded_native_login_and_current_account_bound_receipt_required"]}


@contextlib.contextmanager
def snapshot(policy, *, root=None):
    """Hold registration lock through capture. Never commit ephemeral auth back."""
    binding = validate_binding(policy.get("authentication"))
    timeout = policy["budget"]["timeout_seconds"]
    with store(root) as storage:
        registration, credentials, identity = _load(storage, timeout)
        if registration["authentication"] != binding:
            raise WorkflowError("Prepared credential generation changed; explicitly prepare a fresh packet")
        wall, monotonic = time.time(), time.monotonic()
        with tempfile.TemporaryDirectory(prefix="agentic-native-auth-") as temporary:
            home = Path(temporary)
            from review_claude import environment

            env = environment(home)
            config = Path(env["CLAUDE_CONFIG_DIR"])
            for name, record in ((".credentials.json", credentials), (".claude.json", identity)):
                fd = os.open(config / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                with os.fdopen(fd, "w") as stream:
                    json.dump(record, stream, allow_nan=False)

            def recheck():
                delta = (time.time() - wall) - (time.monotonic() - monotonic)
                if not finite(delta) or abs(delta) > 1:
                    raise WorkflowError("Clock changed during native credential preparation")
                current, fresh, account = _load(storage, timeout)
                if current != registration or fresh != credentials or account != identity:
                    raise WorkflowError("Native registration changed during preparation")
                for name, record in ((".credentials.json", credentials), (".claude.json", identity)):
                    with store(config, create=True) as ephemeral:
                        if ephemeral.read(name) != record:
                            raise WorkflowError("Native authentication snapshot changed before launch")

            yield env, recheck


def setup(repo, *, root=None, renew=False, paid_usage_disabled=False, retain_capability=False):
    if retain_capability and not renew:
        raise WorkflowError("Explicit capability retention requires verified same-account renewal")
    if not paid_usage_disabled or not all(stream.isatty() for stream in (sys.stdin, sys.stdout, sys.stderr)):
        raise WorkflowError(
            "Native login requires a private real terminal and explicit paid-usage-disabled assertion"
        )
    import review_claude
    import review_cli
    from review_policy import PROVIDERS

    # This gate runs before any credential read, filesystem creation or auth call.
    binary = review_cli.executable(repo, "claude-code")
    review_claude.native_setup_controls(binary)
    with store(root, create=not renew) as storage:
        previous = None
        if renew:
            # Renewal can repair an expired token/receipt, but never ignore lineage
            # or a changed account. No native refresh material is copied to the new login.
            previous, _, _ = _load(storage, 900, renewal=True)
        elif list(storage.root.iterdir()) != [storage.root / "lock"]:
            raise WorkflowError(
                "Existing or partial native setup requires inspection; no automatic replacement"
            )
        binding = new_binding(previous["authentication"]["registration_id"] if previous else None)
        storage.write(
            "setup-attempt.json",
            {"schema_version": 1, "authentication": binding, "status": "started"},
            replace=renew,
        )
        generation = storage.root / "generations" / binding["generation_id"]
        for name in ("home", "config", "workspace"):
            fd = _directory(generation / name, create=True)
            os.close(fd)
        env = review_claude.environment(generation / "home", config=generation / "config")
        # Terminal streams intentionally inherited: OAuth URLs/codes are never captured.
        args = [binary, "--safe-mode", "--restricted", "--setting-sources", "", "auth", "login", "--claudeai"]
        storage.check_location()
        if review_cli.executable(repo, "claude-code") != binary:
            raise WorkflowError("Native login executable changed before launch")
        review_claude.native_setup_controls(binary)
        try:
            result = subprocess.run(
                args, cwd=generation / "workspace", env=env, close_fds=True, umask=0o077, check=False
            )
        except (OSError, KeyboardInterrupt):
            raise WorkflowError(
                "Native login interrupted or failed; private partial state retained"
            ) from None
        if result.returncode:
            raise WorkflowError("Native login failed; private partial state retained")
        prefix = "generations/" + binding["generation_id"] + "/config/"
        files = {
            prefix + name: _digest(storage.raw(prefix + name))
            for name in (".credentials.json", ".claude.json")
        }
        _, identity = native_records(
            storage.read(prefix + ".credentials.json"), storage.read(prefix + ".claude.json"), 900
        )
        if previous and previous["account"] != identity:
            raise WorkflowError("Native account changed; activation and prior lineage cannot carry forward")
        lineage = [*previous["lineage"], previous["authentication"]] if previous else []
        registration = {
            "authentication": binding,
            "cli": copy.deepcopy(PROVIDERS["claude-code"]["cli"]),
            "native_exit": 0,
            "interactive": True,
            "account": identity,
            "files": files,
            "lineage": lineage,
            "retained_capability_generations": [
                *previous["retained_capability_generations"],
                previous["authentication"]["generation_id"],
            ]
            if previous and retain_capability
            else [],
        }
        now = time.time()
        receipt = {
            "schema_version": 1,
            "authentication": binding,
            "account": identity,
            "paid_usage_disabled": True,
            "recorded_at": now,
            "expires_at": now + RECEIPT_SECONDS,
        }
        storage.write("registration.json", registration, replace=renew)
        storage.write("receipt.json", receipt, replace=renew)
        storage.write(
            "setup-attempt.json",
            {"schema_version": 1, "authentication": binding, "status": "completed"},
            replace=True,
        )
        return {
            "authentication": binding,
            "receipt_valid_days": 7,
            "operator_assertion_not_billing_guarantee": True,
            "live_diagnostics_established": False,
        }
