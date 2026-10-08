"""Bounded CI diagnostic transport under a trusted-owner, quiescent boundary."""

import argparse
import datetime
import hashlib
import json
import os
import re
import signal
import stat
import subprocess
import time
from pathlib import Path

MIB = 1024 * 1024
LIMITS = {
    "request.json": 16 * MIB,
    "summary.json": 15 * MIB,
    "worker-0.jsonl": 16 * MIB,
    "worker-1.jsonl": 16 * MIB,
    "worker-0.log": 32 * MIB,
    "worker-1.log": 32 * MIB,
}
MANIFEST_LIMIT = MIB
AGGREGATE_LIMIT = 128 * MIB
SECONDS = 60


class DiagnosticError(ValueError):
    """Bounded refusal with a fixed reason."""


class Expired(Exception):
    """Cannot be handled as a per-file refusal."""


class Deadline:
    def __init__(self, seconds):
        self.start = time.monotonic()
        self.end = self.start + seconds
        self.utc = datetime.datetime.now(datetime.UTC).isoformat()

    def check(self):
        if time.monotonic() >= self.end:
            raise Expired("deadline")

    def remaining(self):
        self.check()
        return self.end - time.monotonic()


def directory(path):
    raw = str(path)
    if (
        not raw.startswith("/")
        or any(c in raw for c in ("\0", "\n", "\r"))
        or any(p in ("", ".", "..") for p in raw.split("/")[1:])
    ):
        raise DiagnosticError("path")
    fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in raw.split("/")[1:]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = child
            info = os.fstat(fd)
            if info.st_mode & 0o022 and not info.st_mode & stat.S_ISVTX:
                raise DiagnosticError("ownership")
        return fd
    except BaseException:
        os.close(fd)
        raise


def owned(fd):
    info = os.fstat(fd)
    if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o700:
        raise DiagnosticError("ownership")
    return info.st_dev, info.st_ino


def staging(path, source):
    raw = str(path)
    path = Path(path)
    temporary = Path(os.environ["RUNNER_TEMP"])
    if str(path) != raw or path.parent != temporary or path == source or path.is_relative_to(source):
        raise DiagnosticError("boundary")
    fd = directory(raw)
    try:
        owned(fd)
    except BaseException:
        os.close(fd)
        raise
    return fd


def prepare(source, clock):
    clock.check()
    run, attempt = os.environ["GITHUB_RUN_ID"], os.environ["GITHUB_RUN_ATTEMPT"]
    if any(not re.fullmatch(r"[1-9][0-9]{0,19}", v) for v in (run, attempt)):
        raise DiagnosticError("metadata")
    temporary = Path(os.environ["RUNNER_TEMP"])
    if temporary == source or temporary.is_relative_to(source):
        raise DiagnosticError("boundary")
    fd = directory(os.environ["RUNNER_TEMP"])
    name = f"issue31-ci-{run}-{attempt}"
    try:
        clock.check()
        os.mkdir(name, 0o700, dir_fd=fd)
        parent = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
        try:
            owned(parent)
            clock.check()
            os.mkdir("suite-tmp", 0o700, dir_fd=parent)
            clock.check()
        finally:
            os.close(parent)
    finally:
        os.close(fd)
    return temporary / name


def signature(info):
    return (
        info.st_dev,
        info.st_ino,
        info.st_size,
        info.st_mode,
        info.st_nlink,
        info.st_mtime_ns,
        info.st_ctime_ns,
    )


def regular(info):
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.geteuid():
        raise DiagnosticError("type")


def git(source, clock, *args):
    clock.check()
    value = subprocess.check_output(["git", *args], cwd=source, timeout=min(10, clock.remaining()))
    clock.check()
    return value


def source_identity(source, clock):
    """Independently mirror source() coverage; never import or discover tests."""
    files = {}
    boundaries = {}

    def parents(path, include_self=False):
        """Check lexical directories; do not resolve away a symlink."""
        relative = path.relative_to(source)
        current = source
        parts = relative.parts if include_self else relative.parts[:-1]
        for part in (None, *parts):
            clock.check()
            if part is not None:
                current = current / part
            try:
                info = current.lstat()
            except FileNotFoundError:
                if include_self:
                    return
                raise
            if not stat.S_ISDIR(info.st_mode):
                raise DiagnosticError("source_changed")
            identity = (info.st_dev, info.st_ino, info.st_mode)
            if current in boundaries and boundaries[current] != identity:
                raise DiagnosticError("source_changed")
            boundaries[current] = identity

    parents(source, include_self=True)

    def add(path):
        clock.check()
        parents(path)
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            before = os.fstat(fd)
            if not stat.S_ISREG(before.st_mode):
                raise DiagnosticError("type")
            digest = hashlib.sha256()
            while True:
                clock.check()
                block = os.read(fd, 65536)
                if not block:
                    break
                digest.update(block)
            if signature(before) != signature(os.fstat(fd)) or signature(before) != signature(path.lstat()):
                raise DiagnosticError("source_changed")
            files[str(path.relative_to(source))] = digest.hexdigest()
        finally:
            os.close(fd)

    for name in (
        "scripts/agentic",
        "tests/agentic",
        ".agentic",
        ".agents/skills",
        ".github",
        "docs/agent-workflow",
    ):
        base = source / name
        parents(base, include_self=True)
        if base.is_symlink():
            raise DiagnosticError("source_changed")
        for path in base.rglob("*"):
            clock.check()
            if any(p in {"__pycache__", ".ruff_cache"} for p in path.parts):
                continue
            if path.is_symlink():
                raise DiagnosticError("source_changed")
            if path.is_file():
                add(path)
            elif not path.is_dir():
                raise DiagnosticError("type")
    for name in ("AGENTS.md", "Makefile", "scripts/check_repository.py"):
        path = source / name
        if path.exists():
            add(path)
    checkout = None
    if (source / ".git").is_symlink():
        raise DiagnosticError("source_changed")
    if (source / ".git").exists():
        checkout = git(source, clock, "rev-parse", "HEAD").decode().strip()
        for raw in git(source, clock, "ls-files", "-z").split(b"\0"):
            if raw:
                add(source / os.fsdecode(raw))
    for path, identity in boundaries.items():
        clock.check()
        info = path.lstat()
        if (info.st_dev, info.st_ino, info.st_mode) != identity:
            raise DiagnosticError("source_changed")
    clock.check()
    return dict(checkout=checkout, files=files)


def select(suite_fd, clock):
    candidates = []
    with os.scandir(suite_fd) as entries:
        for index, entry in enumerate(entries):
            clock.check()
            if index >= 128 or len(os.fsencode(entry.name)) > 255:
                raise DiagnosticError("selection_limit")
            if entry.name.startswith("agentic-check-"):
                candidates.append(entry.name)
    if not candidates:
        raise DiagnosticError("selection_missing")
    if len(candidates) != 1:
        raise DiagnosticError("selection_ambiguous")
    name = candidates[0]
    if not name.removeprefix("agentic-check-"):
        raise DiagnosticError("selection_unsafe")
    fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=suite_fd)
    try:
        owned(fd)
    except BaseException:
        os.close(fd)
        raise
    return name, fd


def request_binding(raw, expected):
    if raw is None:
        return "absent"

    def pairs(items):
        value = {}
        for k, v in items:
            if k in value:
                raise ValueError("duplicate")
            value[k] = v
        return value

    def nonfinite(value):
        raise ValueError("nonfinite")

    try:
        value = json.loads(raw, object_pairs_hook=pairs, parse_constant=nonfinite)
        limits = value["execution_limits"]
        wanted = dict(
            schema_version=1,
            profile="issue31-suite1800-v1",
            cap_seconds=1800,
            seconds=1800,
            text_limit=32 * 1024 * 1024,
            evidence_limit=16 * 1024 * 1024,
        )
        if (
            type(value["version"]) is not int
            or value["version"] != 3
            or type(value["jobs"]) is not int
            or value["jobs"] != 2
            or type(limits) is not dict
            or set(limits) != set(wanted)
            or any(type(limits[k]) is not type(v) or limits[k] != v for k, v in wanted.items())
            or type(value["evidence_limit"]) is not int
            or value["evidence_limit"] != wanted["evidence_limit"]
        ):
            return "mismatch"
        if type(value["source"]) is not dict or value["source"] != expected:
            return "mismatch"
        return "matched"
    except (ValueError, TypeError, KeyError, UnicodeError, RecursionError):
        return "invalid"


def collect(parent, source, identity):
    try:
        return _collect(parent, source, identity, Deadline(SECONDS))
    except Expired:
        return 1


def _collect(parent, source, identity, clock):
    clock.check()
    value = metadata(identity)
    parent_fd = staging(parent, source)
    suite_fd = runner_fd = output_fd = None
    rows = [dict(name=n, state="refused", bytes=None, sha256=None, reason="runner") for n in LIMITS]
    errors = []
    total = 0
    runner_name = None
    binding = "unavailable"
    request = None
    try:
        parent_id = owned(parent_fd)
        clock.check()
        os.mkdir("diagnostics", 0o700, dir_fd=parent_fd)
        output_fd = os.open("diagnostics", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent_fd)
        output_id = owned(output_fd)
        try:
            suite_fd = os.open("suite-tmp", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent_fd)
            suite_id = owned(suite_fd)
            runner_name, runner_fd = select(suite_fd, clock)
            runner_id = owned(runner_fd)
        except (OSError, DiagnosticError) as exc:
            errors.append(str(exc) if isinstance(exc, DiagnosticError) else "selection_unsafe")
            for opened in (runner_fd, suite_fd):
                if opened is not None:
                    os.close(opened)
            runner_fd = suite_fd = None
            runner_name = None
        expected = None
        try:
            expected = source_identity(source, clock)
            if expected["checkout"] != value["tested_checkout_sha"]:
                errors.append("request_mismatch")
        except (OSError, DiagnosticError, subprocess.SubprocessError):
            errors.append("source_changed")
        for row in rows:
            clock.check()
            if runner_fd is None:
                continue
            name = row["name"]
            limit = LIMITS[name]
            fd = dest = None
            try:
                fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=runner_fd)
                before = os.fstat(fd)
                regular(before)
                if before.st_size > limit:
                    row.update(state="oversize", reason="size")
                    errors.append("size")
                    continue
                clock.check()
                dest = os.open(
                    name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=output_fd
                )
                h = hashlib.sha256()
                count = 0
                captured = bytearray() if name == "request.json" else None
                while True:
                    clock.check()
                    data = os.read(fd, min(65536, limit - count + 1))
                    clock.check()
                    if not data:
                        break
                    if count + len(data) > limit or total + len(data) > AGGREGATE_LIMIT - MANIFEST_LIMIT:
                        raise DiagnosticError("size")
                    view = memoryview(data)
                    while view:
                        clock.check()
                        written = os.write(dest, view)
                        clock.check()
                        view = view[written:]
                    count += len(data)
                    total += len(data)
                    h.update(data)
                    if captured is not None:
                        captured.extend(data)
                if (
                    signature(before) != signature(os.fstat(fd))
                    or signature(before) != signature(os.stat(name, dir_fd=runner_fd, follow_symlinks=False))
                    or count != before.st_size
                ):
                    raise DiagnosticError("changed")
                row.update(state="copied", bytes=count, sha256=h.hexdigest(), reason="exact")
                if captured is not None:
                    request = bytes(captured)
            except FileNotFoundError:
                row.update(state="absent", reason="missing")
                errors.append("missing")
            except (OSError, DiagnosticError) as exc:
                reason = str(exc) if isinstance(exc, DiagnosticError) else "io"
                row.update(reason=reason)
                errors.append(reason)
            finally:
                for opened in (fd, dest):
                    if opened is not None:
                        os.close(opened)
        clock.check()
        binding = request_binding(request, expected) if expected is not None else "unavailable"
        if binding == "matched" and expected["checkout"] != value["tested_checkout_sha"]:
            binding = "mismatch"
        if binding != "matched":
            errors.append(
                "request_invalid" if binding in {"absent", "invalid", "unavailable"} else "request_mismatch"
            )
        try:
            if expected != source_identity(source, clock):
                binding = "mismatch"
                errors.append("source_changed")
            verify = staging(parent, source)
            try:
                if owned(verify) != parent_id:
                    errors.append("changed")
            finally:
                os.close(verify)
            for name, opened, owner_id, parent_handle in (("diagnostics", output_fd, output_id, parent_fd),):
                current = os.stat(name, dir_fd=parent_handle, follow_symlinks=False)
                if (
                    (current.st_dev, current.st_ino) != owner_id
                    or owned(opened) != owner_id
                    or not stat.S_ISDIR(current.st_mode)
                    or stat.S_IMODE(current.st_mode) != 0o700
                    or current.st_uid != os.geteuid()
                ):
                    errors.append("changed")
            if suite_fd is not None:
                current = os.stat("suite-tmp", dir_fd=parent_fd, follow_symlinks=False)
                if (
                    (current.st_dev, current.st_ino) != suite_id
                    or owned(suite_fd) != suite_id
                    or not stat.S_ISDIR(current.st_mode)
                    or stat.S_IMODE(current.st_mode) != 0o700
                    or current.st_uid != os.geteuid()
                ):
                    errors.append("changed")
            if runner_fd is not None:
                current = os.stat(runner_name, dir_fd=suite_fd, follow_symlinks=False)
                if (
                    (current.st_dev, current.st_ino) != runner_id
                    or owned(runner_fd) != runner_id
                    or not stat.S_ISDIR(current.st_mode)
                    or stat.S_IMODE(current.st_mode) != 0o700
                    or current.st_uid != os.geteuid()
                ):
                    errors.append("changed")
        except (OSError, DiagnosticError, subprocess.SubprocessError):
            errors.append("boundary")
        clock.check()
        result = dict(
            schema_version=1,
            **value,
            utc_start=clock.utc,
            utc_end=datetime.datetime.now(datetime.UTC).isoformat(),
            elapsed_seconds=time.monotonic() - clock.start,
            cap_seconds=SECONDS,
            aggregate_limit_bytes=AGGREGATE_LIMIT,
            files=rows,
            collection_errors=sorted(set(errors)),
            runner_name=runner_name,
            request_binding=binding,
        )
        raw = (json.dumps(result, indent=2, ensure_ascii=False) + "\n").encode()
        clock.check()
        if len(raw) > MANIFEST_LIMIT or total + len(raw) > AGGREGATE_LIMIT:
            raise DiagnosticError("manifest")
        fd = os.open(
            "manifest.json", os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=output_fd
        )
        try:
            view = memoryview(raw)
            while view:
                clock.check()
                count = os.write(fd, view)
                clock.check()
                view = view[count:]
        finally:
            os.close(fd)
        clock.check()
        return 1 if errors else 0
    finally:
        for fd in (runner_fd, suite_fd, output_fd, parent_fd):
            if fd is not None:
                os.close(fd)


def main():
    clock = Deadline(SECONDS)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepare", action="store_true")
    parser.add_argument("--staging")
    parser.add_argument("--test-status")
    parser.add_argument("--clean-status")
    args = parser.parse_args()
    if args.prepare:
        clock.end = clock.start + 30

    def expired(signum, frame):
        raise Expired("deadline")

    previous = signal.signal(signal.SIGALRM, expired)
    try:
        signal.setitimer(signal.ITIMER_REAL, clock.remaining())
        if args.prepare:
            parent = prepare(Path.cwd(), clock)
            clock.check()
            print(parent)
            return 0
        if args.staging is None or str(Path(args.staging)) != args.staging:
            raise DiagnosticError("path")
        identity = dict(
            repository=os.environ["GITHUB_REPOSITORY"],
            check="agentic-quality",
            event=os.environ["GITHUB_EVENT_NAME"],
            pr_head_sha=os.environ["REVIEW_HEAD_SHA"],
            pr_base_sha=os.environ["REVIEW_BASE_SHA"],
            tested_checkout_sha=git(Path.cwd(), clock, "rev-parse", "HEAD").decode().strip(),
            run_id=int(os.environ["GITHUB_RUN_ID"]),
            run_attempt=int(os.environ["GITHUB_RUN_ATTEMPT"]),
            profile=os.environ["AGENTIC_SUITE_PROFILE"],
            test_status=args.test_status,
            clean_status=args.clean_status,
        )
        clock.check()
        return _collect(Path(args.staging), Path.cwd(), identity, clock)
    except (Expired, OSError, ValueError, KeyError, subprocess.SubprocessError):
        return 1
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def metadata(value):
    keys = {
        "repository",
        "check",
        "event",
        "pr_head_sha",
        "pr_base_sha",
        "tested_checkout_sha",
        "run_id",
        "run_attempt",
        "profile",
        "test_status",
        "clean_status",
    }
    if type(value) is not dict or set(value) != keys:
        raise DiagnosticError("metadata")
    if any(type(value[k]) is not str or len(value[k]) > 128 for k in keys - {"run_id", "run_attempt"}):
        raise DiagnosticError("metadata")
    if (
        value["repository"] != "Zi-Deng/FLOW-DC"
        or value["check"] != "agentic-quality"
        or value["event"] != "pull_request"
        or value["profile"] != "issue31-suite1800-v1"
    ):
        raise DiagnosticError("metadata")
    if any(
        not re.fullmatch("[0-9a-f]{40}", value[k])
        for k in ("pr_head_sha", "pr_base_sha", "tested_checkout_sha")
    ):
        raise DiagnosticError("metadata")
    if any(type(value[k]) is not int or not 0 < value[k] < 10**20 for k in ("run_id", "run_attempt")):
        raise DiagnosticError("metadata")
    if any(
        value[k] not in {"success", "failure", "cancelled", "skipped"}
        for k in ("test_status", "clean_status")
    ):
        raise DiagnosticError("metadata")
    return value.copy()


if __name__ == "__main__":
    raise SystemExit(main())
