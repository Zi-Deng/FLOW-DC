#!/usr/bin/env python3
"""Private Jetstream2 setup, preflight and bounded pilot control (Python 3.12+, Linux)."""

import argparse
import errno
import importlib.util
import ipaddress
import json
import math
import os
import re
import selectors
import shutil
import signal
import stat
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit
from uuid import UUID

if __name__ == "__main__":
    sys.modules["flowdc_ops"] = sys.modules[__name__]

# Direct scripts already include their directory; embedded library imports must
# not alter the caller's module search path.

SCHEMA_VERSION = 1
DIRECTORIES = ("inventory", "releases", "runs")
DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
MAX_BYTES = 262144
PROBE_SECONDS = 5
CLOUD_SECONDS = 20
INVENTORY_SECONDS = 90
MAX_FLEET_SERVERS = 64
PROFILE_FIELDS = {
    "schema_version",
    "expected_project_id",
    "region",
    "auth_url",
    "credential_file",
    "wrapper_path",
    "openstack_client",
    "intended_server_ids",
    "known_seed_id",
}
STATES = {
    "ACTIVE",
    "SHUTOFF",
    "SHELVED",
    "SHELVED_OFFLOADED",
    "BUILD",
    "ERROR",
    "PAUSED",
    "SUSPENDED",
    "RESCUE",
    "REBOOT",
    "HARD_REBOOT",
    "REBUILD",
    "MIGRATING",
    "RESIZE",
    "VERIFY_RESIZE",
    "REVERT_RESIZE",
    "DELETED",
    "SOFT_DELETED",
    "UNKNOWN",
}
PLAN_STATES = {"ACTIVE", "SHUTOFF", "SHELVED", "SHELVED_OFFLOADED"}

# This program is fixed; no profile values are inserted into shell source. Only
# the explicitly selected, operator-trusted OpenRC is sourced. The Python caller
# validates and pins wrapper/credential files, supplies a minimal environment and
# captures all child output. Do not invoke this internal wrapper as a general CLI.
WRAPPER = r"""#!/bin/bash -p
set +x +v
set -eu -o pipefail
[[ $# -ge 3 && $# -le 4 ]] || exit 64
readonly flowdc_credential=$1 flowdc_client=$2 flowdc_action=$3 flowdc_id=${4-}
case "$flowdc_action" in
  context) command=(configuration show --mask -f json -c region_name -c auth_url -c auth.auth_url);;
  project) command=(token issue -f json -c project_id);;
  regions) command=(region list -f json -c Region);;
  server) command=(server show "$flowdc_id" -f json -c id -c project_id -c status -c flavor -c addresses -c image -c key_name);;
  flavor) command=(flavor show "$flowdc_id" -f json -c id -c vcpus -c ram -c disk);;
  quota) command=(quota show --compute -f json -c Resource -c Limit);;
  networks) command=(network list --project "$flowdc_id" --long -f json -c ID -c Status -c Subnets);;
  *) exit 64;;
esac
case "$flowdc_action" in
  server|flavor|networks) [[ "$flowdc_id" =~ ^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$ ]] || exit 64;;
  *) [[ $# -eq 3 ]] || exit 64;;
esac
readonly -a command
for flowdc_var in ${!OS_@}; do unset "$flowdc_var"; done
unset BASH_ENV ENV
# A downloaded OpenRC is trusted code. Suppress even accidental prints or tracing
# in that file, then turn tracing off again before returning to the caller.
{ source "$flowdc_credential"; set +x +v; } >/dev/null 2>&1
[[ ${OS_AUTH_TYPE-} == v3applicationcredential ]] || exit 65
[[ -n ${OS_APPLICATION_CREDENTIAL_ID-} && -n ${OS_APPLICATION_CREDENTIAL_SECRET-} ]] || exit 65
[[ -n ${OS_AUTH_URL-} && -n ${OS_REGION_NAME-} ]] || exit 65
# Reject unrelated scope/cloud/endpoint overrides instead of silently mixing them.
for flowdc_var in ${!OS_@}; do
  case "$flowdc_var" in
    OS_AUTH_TYPE|OS_APPLICATION_CREDENTIAL_ID|OS_APPLICATION_CREDENTIAL_SECRET|OS_AUTH_URL|OS_REGION_NAME|OS_INTERFACE|OS_IDENTITY_API_VERSION) ;;
    *) exit 65;;
  esac
done
export OS_INTERFACE=public OS_IDENTITY_API_VERSION=3
export OS_COMPUTE_API_VERSION=2.1
exec "$flowdc_client" --os-interface public --os-compute-api-version 2.1 "${command[@]}"
"""


class OpsError(Exception):
    """An error with public, fixed text, never raw filesystem/provider diagnostics."""

    def __init__(self, code, message, action, exit_code=2):
        super().__init__(message)
        self.code = code
        self.message = message
        self.action = action
        self.exit_code = exit_code


def invalid_path():
    return OpsError(
        "unsafe_path",
        "A managed path is unsafe or has an unexpected type.",
        "Use absolute, separate roots outside Git, without links or parent traversal; "
        "inspect existing paths manually before retrying.",
    )


def private_metadata(info, *, directory=False, executable=False):
    expected = 0o700 if directory or executable else 0o600
    correct_type = stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)
    if not correct_type or (not directory and info.st_nlink != 1):
        raise invalid_path()
    if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != expected:
        raise OpsError(
            "unsafe_permissions",
            "An existing managed path is not owner-private with the required permissions.",
            "Inspect ownership and permissions manually: managed directories require 0700 "
            "and files 0600 (the js2 wrapper requires 0700). Existing paths are never chmodded automatically.",
        )


def absolute_path(value):
    # Do not resolve(): resolving would silently accept a symlink or '..'.
    path = Path(value)
    if path.anchor != "/" or path == Path("/") or ".." in path.parts or "\x00" in value:
        raise invalid_path()
    return path


def inspect_ancestor(fd, root_owner):
    info = os.fstat(fd)
    # A root/owner-controlled sticky directory (e.g. /tmp) is a valid ancestor.
    # Filesystem root ownership may be remapped inside an OS user namespace.
    # An untrusted writable ancestor could otherwise replace a private root.
    if info.st_uid not in (0, root_owner, os.geteuid()) or (
        info.st_mode & 0o022 and not info.st_mode & stat.S_ISVTX
    ):
        raise invalid_path()
    try:
        marker = os.stat(".git", dir_fd=fd, follow_symlinks=False)
    except FileNotFoundError:
        return
    if stat.S_ISDIR(marker.st_mode):
        git_fd = os.open(".git", DIRECTORY_FLAGS, dir_fd=fd)
        try:
            try:
                os.stat("HEAD", dir_fd=git_fd, follow_symlinks=False)
            except FileNotFoundError:
                # An empty .git directory is not a Git repository (some sandbox
                # mounts provide one at /tmp). Do not read repository contents.
                return
        finally:
            os.close(git_fd)
    raise OpsError(
        "git_workspace",
        "Private operations state cannot be placed inside a Git working tree.",
        "Choose roots outside all Git working trees, such as the default home directories.",
    )


@contextmanager
def private_directory(path, *, create=False, private=True):
    """Walk using directory descriptors; reject links at every component.

    Only newly created directories get chmod, including missing parents. Keeping
    descriptors open avoids following a path swapped to a symlink between checks.
    """
    fd = os.open("/", DIRECTORY_FLAGS)
    try:
        root_owner = os.fstat(fd).st_uid
        inspect_ancestor(fd, root_owner)
        for part in path.parts[1:]:
            created = False
            if create:
                try:
                    os.mkdir(part, 0o700, dir_fd=fd)
                    created = True
                except FileExistsError:
                    pass
            try:
                child = os.open(part, DIRECTORY_FLAGS, dir_fd=fd)
            except OSError as exc:
                if isinstance(exc, FileNotFoundError):
                    raise
                if exc.errno in (errno.ENOTDIR, errno.ELOOP):
                    raise invalid_path() from None
                raise
            os.close(fd)
            fd = child
            if created:
                os.fchmod(fd, 0o700)
            inspect_ancestor(fd, root_owner)
        if private:
            private_metadata(os.fstat(fd), directory=True)
        yield fd
    finally:
        os.close(fd)


def inspect_child(fd, name, *, directory=False, executable=False, required=False):
    try:
        info = os.stat(name, dir_fd=fd, follow_symlinks=False)
    except FileNotFoundError:
        if required:
            raise OpsError(
                "concurrent_change",
                "A managed path disappeared during initialization.",
                "Stop concurrent changes to the selected roots, inspect the setup, and retry.",
                exit_code=1,
            ) from None
        return False
    private_metadata(info, directory=directory, executable=executable)
    return True


def preflight_root(path, *, directories=(), files=()):
    try:
        with private_directory(path) as fd:
            for name in directories:
                inspect_child(fd, name, directory=True)
            for name in files:
                inspect_child(fd, name, executable=name == "js2")
    except FileNotFoundError:
        pass


def create_profile(fd, value):
    """Exclusive creation; existing profiles are checked but never opened/read."""
    name = "profile.json"
    try:
        output = os.open(
            name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=fd
        )
    except FileExistsError:
        inspect_child(fd, name, required=True)
        return "preserved"
    with os.fdopen(output, "w", encoding="utf-8") as stream:
        os.fchmod(stream.fileno(), 0o600)
        json.dump(value, stream, indent=2, allow_nan=False)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    return "created"


def read_bounded_file(fd):
    """Read through EOF, retaining at most one byte beyond the size limit."""
    content = bytearray()
    while len(content) <= MAX_BYTES:
        chunk = os.read(fd, min(65536, MAX_BYTES + 1 - len(content)))
        if not chunk:
            break
        content.extend(chunk)
    return bytes(content)


def create_wrapper(fd):
    try:
        output = os.open("js2", os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o700, dir_fd=fd)
    except FileExistsError:
        with open_private_at(fd, "js2", executable=True) as existing:
            if read_bounded_file(existing) != WRAPPER.encode():
                raise OpsError(
                    "wrapper_conflict",
                    "Existing js2 differs from this tool's trusted wrapper.",
                    "Preserve the existing file; choose a new private config root or inspect it manually.",
                ) from None
        return "preserved"
    with os.fdopen(output, "w") as stream:
        os.fchmod(stream.fileno(), 0o700)
        stream.write(WRAPPER)
        stream.flush()
        os.fsync(stream.fileno())
    return "created"


def outcome(operation, status, *, checks=(), errors=(), next_actions=(), data=None):
    return {
        "schema_version": SCHEMA_VERSION,
        "operation": operation,
        "status": status,
        "checks": list(checks),
        "errors": list(errors),
        "next_actions": list(next_actions),
        "data": {} if data is None else data,
    }


def initialize(args):
    # The CLI is single-threaded. Restore even after an error for in-process use.
    previous_mask = os.umask(0o077)
    try:
        return initialize_private(args)
    finally:
        os.umask(previous_mask)


def initialize_private(args):
    state_root = absolute_path(args.state_root)
    config_root = absolute_path(args.config_root)
    if state_root == config_root or state_root in config_root.parents or config_root in state_root.parents:
        raise invalid_path()
    # Detect existing conflicts before creating anything. A later concurrent change
    # can still fail; retain any new private setup files for operator inspection.
    preflight_root(state_root, directories=DIRECTORIES)
    preflight_root(config_root, files=("profile.json", "js2"))
    checks = []
    with private_directory(state_root, create=True) as fd:
        for name in DIRECTORIES:
            try:
                os.mkdir(name, 0o700, dir_fd=fd)
            except FileExistsError:
                state = "preserved"
            else:
                # Open with O_NOFOLLOW before setting permissions on our new inode.
                child = os.open(name, DIRECTORY_FLAGS, dir_fd=fd)
                try:
                    os.fchmod(child, 0o700)
                finally:
                    os.close(child)
                state = "created"
            inspect_child(fd, name, directory=True, required=True)
            checks.append({"name": name, "state": state})
    with private_directory(config_root, create=True) as fd:
        wrapper_state = create_wrapper(fd)
        profile_state = create_profile(
            fd,
            {
                "schema_version": SCHEMA_VERSION,
                "expected_project_id": None,
                "region": None,
                "auth_url": None,
                "credential_file": None,
                "wrapper_path": str(config_root / "js2"),
                "openstack_client": None,
                "intended_server_ids": [],
                "known_seed_id": None,
            },
        )
    checks.append({"name": "profile", "state": profile_state})
    checks.append({"name": "wrapper", "state": wrapper_state})
    checks.append({"name": "cloud_readiness", "state": "not_assessed"})
    return outcome(
        "init",
        "ok",
        checks=checks,
        next_actions=[
            "Edit profile.json privately after manual enrollment and verification of project, region, "
            "credential/client paths, and intended server UUIDs; no credentials have been read or created.",
            "Run doctor with this profile and state root. No cloud or SSH readiness has been established.",
        ],
        data={"state_root": str(state_root), "config_root": str(config_root)},
    )


def schema_error(name="document"):
    return OpsError(
        "invalid_schema",
        f"Invalid {name} schema or value.",
        "Use the versioned examples in docs/jetstream2; remove unknown fields and verify all values.",
    )


def fields(value, required, optional=(), name="document"):
    if (
        not isinstance(value, dict)
        or not set(required) <= value.keys()
        or value.keys() - set(required) - set(optional)
    ):
        raise schema_error(name)
    return value


def version(value):
    if type(value.get("schema_version")) is not int or value["schema_version"] != SCHEMA_VERSION:
        raise schema_error()


def uuid_value(value):
    if not isinstance(value, str) or not re.fullmatch(
        r"[0-9a-fA-F]{32}|[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}", value
    ):
        raise schema_error()
    return str(UUID(value))


def identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}", value):
        raise schema_error()
    return value


def https_url(value):
    if not isinstance(value, str) or len(value) > 2048 or any(ord(c) < 33 for c in value):
        raise schema_error()
    try:
        url = urlsplit(value)
        if (
            url.scheme != "https"
            or not url.hostname
            or url.username
            or url.password
            or url.query
            or url.fragment
        ):
            raise ValueError
        _ = url.port
    except (ValueError, OverflowError):
        raise schema_error() from None
    return value.rstrip("/")


def integer(value, low=0, high=2**53):
    if type(value) is not int or not low <= value <= high:
        raise schema_error()
    return value


def number(value):
    if type(value) not in (int, float):
        raise schema_error()
    try:
        if not math.isfinite(value) or value <= 0:
            raise schema_error()
    except OverflowError:
        raise schema_error() from None
    return value


def timestamp(value):
    if not isinstance(value, str) or len(value) > 40:
        raise schema_error()
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if result.tzinfo is None:
            raise ValueError
        return result.astimezone(UTC)
    except (ValueError, OverflowError):
        raise schema_error() from None


def uuid_list(value, maximum=MAX_FLEET_SERVERS):
    if not isinstance(value, list) or len(value) > maximum:
        raise schema_error()
    result = [uuid_value(item) for item in value]
    if len(set(result)) != len(result):
        raise schema_error()
    return result


@contextmanager
def open_private_at(parent, name, *, executable=False):
    # O_NONBLOCK prevents a FIFO swap from hanging before fstat rejects its type.
    try:
        fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=parent)
    except OSError as exc:
        if exc.errno in (errno.ELOOP, errno.ENOTDIR):
            raise invalid_path() from None
        raise
    try:
        private_metadata(os.fstat(fd), executable=executable)
        yield fd
    finally:
        os.close(fd)


@contextmanager
def private_file(value, *, executable=False):
    path = absolute_path(value)
    with private_directory(path.parent) as parent:
        with open_private_at(parent, path.name, executable=executable) as fd:
            yield fd


def parse_json(raw):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError
            result[key] = value
        return result

    def reject(value):
        raise ValueError

    try:
        return json.loads(raw, object_pairs_hook=unique, parse_constant=reject)
    except (ValueError, UnicodeError, RecursionError):
        raise schema_error() from None


def read_document(path):
    try:
        with private_file(path) as fd:
            with os.fdopen(os.dup(fd), "rb") as stream:
                raw = stream.read(MAX_BYTES + 1)
    except FileNotFoundError:
        raise OpsError(
            "missing_file",
            "A requested private input file is missing.",
            "Initialize or provide the named private input file, then retry.",
            3,
        ) from None
    if len(raw) > MAX_BYTES:
        raise schema_error()
    return parse_json(raw)


def load_profile(path):
    value = fields(read_document(path), PROFILE_FIELDS, name="profile")
    version(value)
    value["intended_server_ids"] = uuid_list(value["intended_server_ids"])
    for key in ("expected_project_id", "known_seed_id"):
        if value[key] is not None:
            value[key] = uuid_value(value[key])
    if value["known_seed_id"] and value["known_seed_id"] not in value["intended_server_ids"]:
        raise schema_error("profile")
    if value["region"] is not None:
        identifier(value["region"])
    if value["auth_url"] is not None:
        value["auth_url"] = https_url(value["auth_url"])
    for key in ("credential_file", "wrapper_path", "openstack_client"):
        if value[key] is not None:
            if not isinstance(value[key], str):
                raise schema_error("profile")
            absolute_path(value[key])
    return value


def validate_client(path):
    if not path:
        raise OpsError(
            "client_missing",
            "OpenStack client is not configured.",
            "Have the coordinator install the administration environment and record its absolute client path.",
            3,
        )
    value = absolute_path(path)
    with private_directory(value.parent, private=False) as parent:
        info = os.stat(value.name, dir_fd=parent, follow_symlinks=False)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid not in (0, os.geteuid())
            or info.st_mode & 0o022
            or not info.st_mode & 0o111
        ):
            raise invalid_path()
    if not os.access(value, os.X_OK):
        raise invalid_path()


def validate_wrapper(fd):
    if read_bounded_file(fd) != WRAPPER.encode():
        raise OpsError(
            "wrapper_conflict",
            "The configured wrapper does not match this tool's fixed program.",
            "Use js2 from init for this tool version; preserve and inspect any conflicting file.",
        )
    os.lseek(fd, 0, os.SEEK_SET)


def safe_environment(*, local=False):
    env = {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8", "PYTHONNOUSERSITE": "1"}
    # Local session probes need their actual bus/agent addresses. Cloud commands
    # receive none of these and no inherited OS_*, tracing or Python overrides.
    if local:
        for key in (
            "SSH_AUTH_SOCK",
            "XDG_RUNTIME_DIR",
            "DBUS_SESSION_BUS_ADDRESS",
            "HOME",
            "USER",
            "LOGNAME",
        ):
            if key in os.environ:
                env[key] = os.environ[key]
    return env


def run_bounded(argv, *, timeout, local=False, pass_fds=(), classify_errors=False, on_dispatch=None):
    if timeout <= 0:
        raise OpsError(
            "probe_timeout", "The probe time budget expired.", "Inspect access manually and retry.", 1
        )
    # This single-threaded CLI must own reaping; an inherited SIG_IGN or a
    # custom handler could release the child PID before group cleanup.
    if signal.getsignal(signal.SIGCHLD) != signal.SIG_DFL:
        raise OpsError(
            "probe_child_management",
            "The process environment does not permit exclusive child management.",
            "Run the CLI with the default SIGCHLD disposition and no external child reaper.",
            1,
        )
    try:
        process = subprocess.Popen(
            argv,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=safe_environment(local=local),
            cwd="/",
            start_new_session=True,
            pass_fds=pass_fds,
        )
    except OSError:
        raise OpsError(
            "probe_start_failed",
            "A required executable could not be started.",
            "Inspect its installation and access permissions manually.",
            1,
        ) from None
    output = bytearray()
    diagnostics = bytearray()
    total = 0
    deadline = time.monotonic() + timeout
    try:
        # The child may execute immediately. Durable callers must already retain intent.
        if on_dispatch is not None:
            on_dispatch()
        with selectors.DefaultSelector() as selector:
            for stream in (process.stdout, process.stderr):
                os.set_blocking(stream.fileno(), False)
                selector.register(stream, selectors.EVENT_READ)
            while selector.get_map():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise OpsError(
                        "probe_timeout",
                        "A subprocess exceeded its time budget.",
                        "Inspect the local tool or provider access manually before retrying.",
                        1,
                    )
                for key, _ in selector.select(min(remaining, 0.1)):
                    chunk = os.read(key.fileobj.fileno(), 8192)
                    if not chunk:
                        selector.unregister(key.fileobj)
                        continue
                    total += len(chunk)
                    if total > MAX_BYTES:
                        raise OpsError(
                            "probe_output_limit",
                            "A subprocess exceeded its output budget.",
                            "Inspect the provider/tool privately; raw output was discarded.",
                            1,
                        )
                    if key.fileobj is process.stdout:
                        output.extend(chunk)
                    elif classify_errors:
                        diagnostics.extend(chunk)
            # Observe exit without releasing the PID. Even after pipe EOF,
            # descendants may remain in this child's process group.
            while os.waitid(os.P_PID, process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT) is None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise OpsError(
                        "probe_timeout",
                        "A subprocess exceeded its time budget.",
                        "Inspect the local tool or provider access manually before retrying.",
                        1,
                    )
                time.sleep(min(remaining, 0.01))
    finally:
        # Signal before wait() reaps the leader, so its PID cannot be recycled
        # between reaping and killpg(). This also terminates quiet descendants.
        try:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            try:
                code = process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                raise OpsError(
                    "probe_cleanup_timeout",
                    "Subprocess cleanup did not complete within its time budget.",
                    "Inspect local child processes manually; do not assume the probe has stopped.",
                    1,
                ) from None
        finally:
            process.stdout.close()
            process.stderr.close()
    if code and classify_errors:
        # Return only a fixed category, never stderr or the rejected request.
        diagnostic = bytes(diagnostics).lower()
        if b"forbidden" in diagnostic or b"unauthorized" in diagnostic or b"http 403" in diagnostic:
            return code, b"permission"
        if b"quota" in diagnostic or b"overlimit" in diagnostic:
            return code, b"quota"
        return code, b"provider"
    return code, bytes(output)


def readiness_files(profile):
    checks = []
    for key in ("credential_file", "wrapper_path", "openstack_client"):
        if not profile[key]:
            checks.append({"name": key, "state": "missing"})
            continue
        try:
            if key == "openstack_client":
                validate_client(profile[key])
            elif key == "credential_file":
                # Metadata only: do not open/read the credential for doctor.
                path = absolute_path(profile[key])
                with private_directory(path.parent) as parent:
                    info = os.stat(path.name, dir_fd=parent, follow_symlinks=False)
                    private_metadata(info)
                    if info.st_size == 0:
                        checks.append({"name": key, "state": "empty"})
                        continue
            else:
                with private_file(profile[key], executable=True) as fd:
                    validate_wrapper(fd)
        except FileNotFoundError:
            state = "missing"
        except (OpsError, OSError):
            state = "unsafe_or_unavailable"
        else:
            state = "ok"
        checks.append({"name": key, "state": state})
    return checks


def doctor(args):
    profile = load_profile(args.profile)
    checks = readiness_files(profile)
    try:
        with private_directory(absolute_path(args.state_root)) as fd:
            for name in DIRECTORIES:
                if not inspect_child(fd, name, directory=True):
                    raise FileNotFoundError
    except (OpsError, OSError):
        checks.append({"name": "state_root", "state": "missing_or_unsafe"})
    else:
        checks.append({"name": "state_root", "state": "ok"})
    configured = all(profile[key] is not None for key in ("expected_project_id", "region", "auth_url"))
    checks.append({"name": "profile_context", "state": "ok" if configured else "pending"})
    checks.append(
        {"name": "intended_servers", "state": "ok" if profile["intended_server_ids"] else "pending"}
    )
    checks.append({"name": "python", "state": "ok" if sys.version_info >= (3, 12) else "unsupported"})
    tools = {
        name: shutil.which("/bin/bash" if name == "bash" else name)
        for name in ("bash", "ssh", "ssh-add", "systemctl", "loginctl")
    }
    checks.extend({"name": name, "state": "ok" if path else "missing"} for name, path in tools.items())
    probes = (
        ("ssh_agent", "ssh-add", ["-l"]),
        ("systemd_user", "systemctl", ["--user", "is-system-running"]),
        ("lingering", "loginctl", ["show-user", str(os.getuid()), "--property=Linger", "--value"]),
    )
    for name, tool, arguments in probes:
        state = "tool_missing"
        if tools[tool]:
            try:
                code, raw = run_bounded([tools[tool], *arguments], timeout=PROBE_SECONDS, local=True)
            except OpsError as exc:
                state = exc.code
            else:
                if name == "ssh_agent":
                    state = {0: "ok", 1: "no_identities", 2: "unavailable"}.get(code, "probe_failed")
                elif name == "systemd_user":
                    if raw.strip() == b"degraded":
                        state = "degraded"
                    else:
                        state = (
                            "ok" if code == 0 and raw.strip() == b"running" else "not_running_or_unavailable"
                        )
                else:
                    state = "ok" if code == 0 and raw.strip() == b"yes" else "disabled_or_unavailable"
        checks.append({"name": name, "state": state})
    pending = any(check["state"] != "ok" for check in checks)
    return outcome(
        "doctor",
        "pending" if pending else "ok",
        checks=checks,
        next_actions=[
            "For missing/unsafe paths, run init or inspect the configured paths and owner-private permissions. "
            "Bash must be executable at /bin/bash, the interpreter used by inventory. "
            "Install missing tools only through the coordinator's separate setup operation.",
            "For missing credentials, enroll manually through Horizon Identity > Application Credentials for the correct allocation. "
            "Keep the downloaded OpenRC private; doctor does not validate its contents or authentication.",
            "An absent/empty SSH agent says nothing about direct key-file or other SSH authentication. "
            "Inspect ssh-add -l privately and arrange guest access manually; no guest connection was attempted.",
            "Inspect systemctl --user is-system-running and loginctl show-user UID --property=Linger manually. "
            "A degraded user manager remains pending; inspect systemctl --user --failed for failed units. "
            "Discuss persistence with the operator if needed; this tool does not start services or enable lingering.",
            "Fill verified project/site/UUID context, then run inventory. A local doctor pass is not cloud or guest readiness.",
        ],
        data={"cloud_readiness": "not_assessed", "guest_readiness": "not_assessed"},
    ), 3 if pending else 0


def cloud_query(profile, action, resource=None, *, deadline):
    validate_client(profile["openstack_client"])
    with private_file(profile["wrapper_path"], executable=True) as wrapper:
        validate_wrapper(wrapper)
        with private_file(profile["credential_file"]) as credential:
            argv = [
                "/bin/bash",
                "-p",
                f"/proc/self/fd/{wrapper}",
                f"/proc/self/fd/{credential}",
                profile["openstack_client"],
                action,
            ]
            if resource is not None:
                argv.append(resource)
            code, raw = run_bounded(
                argv, timeout=min(CLOUD_SECONDS, deadline - time.monotonic()), pass_fds=(wrapper, credential)
            )
    if code:
        raise OpsError(
            "provider_failed",
            "Read-only discovery failed; provider output was suppressed.",
            "Inspect application-credential validity, allocation access, OpenRC format and client compatibility privately.",
            1,
        )
    try:
        return parse_json(raw)
    except OpsError:
        raise OpsError(
            "provider_schema",
            "Provider returned an unsupported response.",
            "Verify the installed OpenStack client and provider response format privately.",
            1,
        ) from None


def reference_id(value, *, image=False):
    if image and value in (None, "", "N/A (booted from volume)"):
        return None
    if isinstance(value, dict):
        value = value.get("id")
    elif isinstance(value, str) and value.endswith(")"):
        # OSC with compute microversion 2.1 prints 'display name (id)'. Retain
        # only the identifier, never the untrusted display name.
        match = re.search(r"\(([^()]*)\)$", value)
        value = match[1] if match else None
    return uuid_value(value) if image else identifier(value)


def address_list(value):
    if not isinstance(value, dict) or len(value) > 32:
        raise schema_error()
    result = []
    for addresses in value.values():
        if not isinstance(addresses, list) or len(addresses) > 32:
            raise schema_error()
        for address in addresses:
            if not isinstance(address, str):
                raise schema_error()
            try:
                result.append(str(ipaddress.ip_address(address)))
            except ValueError:
                raise schema_error() from None
    return sorted(set(result))


def server_summary(value, expected_id, project):
    if not isinstance(value, dict):
        raise schema_error()
    resource = uuid_value(value.get("id"))
    tenant = uuid_value(value.get("project_id"))
    if resource != expected_id or tenant != project:
        raise OpsError(
            "server_context_mismatch",
            "A server UUID or project differs from the intended scope.",
            "Verify the intended UUIDs and allocation in Horizon; do not plan from this snapshot.",
            3,
        )
    state = value.get("status")
    if not isinstance(state, str) or state not in STATES or "key_name" not in value or "image" not in value:
        raise schema_error()
    if value["key_name"] is not None and not isinstance(value["key_name"], str):
        raise schema_error()
    return {
        "id": resource,
        "project_id": tenant,
        "status": state,
        "flavor_id": reference_id(value.get("flavor")),
        "addresses": address_list(value.get("addresses")),
        "image_id": reference_id(value["image"], image=True),
        "keypair_present": bool(value["key_name"]),
    }


def flavor_summary(value, expected_id):
    if not isinstance(value, dict) or value.get("id") != expected_id:
        raise schema_error()
    return {
        "id": expected_id,
        "vcpus": integer(value.get("vcpus"), 1),
        "ram_mb": integer(value.get("ram"), 1),
        "disk_gb": integer(value.get("disk")),
    }


def quota_summary(value):
    if not isinstance(value, list) or len(value) > 100:
        raise schema_error()
    result = {}
    for row in value:
        if not isinstance(row, dict):
            raise schema_error()
        key = row.get("Resource")
        if key in ("cores", "ram", "instances"):
            if key in result:
                raise schema_error()
            result[key] = integer(row.get("Limit"), -1)
    fields(result, ("cores", "ram", "instances"))
    return result


def network_summary(value):
    if not isinstance(value, list) or len(value) > 100:
        raise schema_error()
    result = []
    seen = set()
    for row in value:
        if not isinstance(row, dict):
            raise schema_error()
        resource = uuid_value(row.get("ID"))
        if resource in seen or row.get("Status") not in ("ACTIVE", "DOWN", "BUILD", "ERROR"):
            raise schema_error()
        seen.add(resource)
        result.append(
            {"id": resource, "status": row["Status"], "subnet_ids": uuid_list(row.get("Subnets"), 100)}
        )
    return result


def inventory(args):
    profile = load_profile(args.profile)
    checks = readiness_files(profile)
    errors = []
    next_actions = [
        "Verify missing facts against Horizon and the selected allocation; fill the profile and rerun inventory.",
        "A complete snapshot records cloud metadata only. Guest access, services, routes and billing remain unverified.",
    ]
    data = {
        "observed_at": datetime.now(UTC).isoformat(),
        "complete": False,
        "context_verified": False,
        "context": None,
        "intended_server_ids": profile["intended_server_ids"],
        "known_seed_id": profile["known_seed_id"],
        "servers": [],
        "quota": None,
        "networks": None,
        "guest_readiness": "not_assessed",
    }
    exit_code = 3 if any(check["state"] != "ok" for check in checks) else 0

    def finish():
        data["complete"] = exit_code == 0
        return outcome(
            "inventory",
            {0: "ok", 1: "error", 2: "invalid", 3: "pending"}[exit_code],
            checks=checks,
            errors=errors,
            next_actions=next_actions,
            data=data,
        ), exit_code

    if exit_code:
        next_actions.append(
            "Complete manual enrollment and configure private OpenRC, generated js2 and administration client paths; run doctor."
        )
        return finish()
    deadline = time.monotonic() + INVENTORY_SECONDS
    local_failure = False

    def probe(name, action, transform, resource=None):
        nonlocal exit_code, local_failure
        if local_failure:
            return None
        transforming = False
        try:
            raw = cloud_query(profile, action, resource, deadline=deadline)
            transforming = True
            value = transform(raw)
        except (OpsError, OSError, KeyError, TypeError, ValueError) as exc:
            message = "Read-only observation failed or did not match the requested scope."
            if not transforming and (
                isinstance(exc, OSError) or isinstance(exc, OpsError) and exc.exit_code == 2
            ):
                local_failure = True
                code = exc.code if isinstance(exc, OpsError) else "local_io_error"
                exit_code = 2 if isinstance(exc, OpsError) else 1
                message = "Local input revalidation failed; further discovery was stopped."
                next_actions.insert(
                    0,
                    "Inspect the local wrapper, credential and client paths, contents and owner-private permissions. "
                    "Preserve existing files, stop concurrent changes, then rerun doctor and inventory.",
                )
            elif isinstance(exc, OpsError) and exc.exit_code == 3:
                code = exc.code
                if not exit_code:
                    exit_code = 3
            else:
                code = exc.code if isinstance(exc, OpsError) and exc.exit_code == 1 else "provider_schema"
                exit_code = 1
            checks.append({"name": name, "state": code})
            errors.append(
                {
                    "code": code,
                    "message": message,
                }
            )
            return None
        checks.append({"name": name, "state": "ok"})
        return value

    def context(raw):
        # OSC can report env-based auth at top level or flatten a nested auth
        # mapping. Select only these safe URL columns and reject disagreement.
        if not isinstance(raw, dict):
            raise schema_error()
        urls = {https_url(raw[key]) for key in ("auth_url", "auth.auth_url") if raw.get(key) is not None}
        if len(urls) != 1:
            raise schema_error()
        return {"region": identifier(raw["region_name"]), "auth_url": urls.pop()}

    site = probe("site", "context", context)
    project = probe("project", "project", lambda raw: uuid_value(raw["project_id"]))

    def regions(raw):
        if not isinstance(raw, list) or not 1 <= len(raw) <= 100:
            raise schema_error()
        result = [identifier(row["Region"]) for row in raw]
        if len(result) != len(set(result)):
            raise schema_error()
        return result

    observed_regions = probe("regions", "regions", regions)
    if site is None or project is None or observed_regions is None:
        return finish()
    data["context"] = {"project_id": project, **site}
    expected = {
        "project_id": profile["expected_project_id"],
        "region": profile["region"],
        "auth_url": profile["auth_url"],
    }
    if site["region"] not in observed_regions or data["context"] != expected:
        exit_code = 3
        checks.append({"name": "context", "state": "unverified_or_mismatched"})
        return finish()
    data["context_verified"] = True
    checks.append({"name": "context", "state": "ok"})
    if not profile["intended_server_ids"]:
        exit_code = 3
        checks.append({"name": "intended_servers", "state": "missing"})
        return finish()
    flavors = {}
    for index, server_id in enumerate(profile["intended_server_ids"]):
        server = probe(
            f"server_{index + 1}",
            "server",
            lambda raw, sid=server_id: server_summary(raw, sid, project),
            server_id,
        )
        if server is None:
            continue
        flavor_id = server.pop("flavor_id")
        if flavor_id not in flavors:
            flavors[flavor_id] = probe(
                f"flavor_{index + 1}",
                "flavor",
                lambda raw, fid=flavor_id: flavor_summary(raw, fid),
                flavor_id,
            )
        flavor = flavors[flavor_id]
        if flavor is not None:
            data["servers"].append({**server, "flavor": flavor})
    data["quota"] = probe("quota", "quota", quota_summary)
    data["networks"] = probe("networks", "networks", network_summary, UUID(project).hex)
    return finish()


def validate_context(value):
    fields(value, ("project_id", "region", "auth_url"))
    return {
        "project_id": uuid_value(value["project_id"]),
        "region": identifier(value["region"]),
        "auth_url": https_url(value["auth_url"]),
    }


def validate_spec(path):
    spec = fields(read_document(path), ("schema_version", "context", "vms"), name="pilot specification")
    version(spec)
    spec["context"] = validate_context(spec["context"])
    if not isinstance(spec["vms"], list) or len(spec["vms"]) != 3:
        raise schema_error("pilot specification")
    roles, ids = set(), set()
    now = datetime.now(UTC)
    for vm in spec["vms"]:
        fields(vm, ("role", "id", "active_seconds", "rate"))
        if not isinstance(vm["role"], str) or vm["role"] not in ("manager", "origin", "worker"):
            raise schema_error("pilot specification")
        vm["id"] = uuid_value(vm["id"])
        if vm["role"] in roles or vm["id"] in ids:
            raise schema_error("pilot specification")
        roles.add(vm["role"])
        ids.add(vm["id"])
        integer(vm["active_seconds"], 1, 7200)
        if vm["rate"] is not None:
            rate = fields(vm["rate"], ("su_per_hour", "source", "observed_at", "verified", "flavor_id"))
            number(rate["su_per_hour"])
            rate["source"] = https_url(rate["source"])
            if timestamp(rate["observed_at"]) > now:
                raise schema_error("rate")
            identifier(rate["flavor_id"])
            if type(rate["verified"]) is not bool:
                raise schema_error("rate")
    return spec


def validate_snapshot(path):
    snapshot = fields(
        read_document(path),
        ("schema_version", "operation", "status", "checks", "errors", "next_actions", "data"),
    )
    version(snapshot)
    if snapshot["operation"] != "inventory" or snapshot["status"] not in (
        "ok",
        "pending",
        "error",
        "invalid",
    ):
        raise schema_error("inventory")
    if not all(isinstance(snapshot[key], list) for key in ("checks", "errors", "next_actions")):
        raise schema_error("inventory")
    for check in snapshot["checks"]:
        fields(check, ("name", "state"))
        if not all(isinstance(value, str) for value in check.values()):
            raise schema_error("inventory")
    for error in snapshot["errors"]:
        fields(error, ("code", "message"))
        if not all(isinstance(value, str) for value in error.values()):
            raise schema_error("inventory")
    if not all(isinstance(value, str) for value in snapshot["next_actions"]):
        raise schema_error("inventory")
    data = fields(
        snapshot["data"],
        (
            "observed_at",
            "complete",
            "context_verified",
            "context",
            "intended_server_ids",
            "known_seed_id",
            "servers",
            "quota",
            "networks",
            "guest_readiness",
        ),
    )
    timestamp(data["observed_at"])
    if (
        type(data["complete"]) is not bool
        or type(data["context_verified"]) is not bool
        or data["guest_readiness"] != "not_assessed"
    ):
        raise schema_error("inventory")
    if data["context"] is not None:
        data["context"] = validate_context(data["context"])
    data["intended_server_ids"] = uuid_list(data["intended_server_ids"])
    if data["known_seed_id"] is not None:
        data["known_seed_id"] = uuid_value(data["known_seed_id"])
        if data["known_seed_id"] not in data["intended_server_ids"]:
            raise schema_error("inventory")
    if not isinstance(data["servers"], list) or len(data["servers"]) > MAX_FLEET_SERVERS:
        raise schema_error("inventory")
    seen = set()
    for server in data["servers"]:
        fields(server, ("id", "project_id", "status", "addresses", "image_id", "keypair_present", "flavor"))
        server["id"] = uuid_value(server["id"])
        server["project_id"] = uuid_value(server["project_id"])
        if server["id"] in seen or server["id"] not in data["intended_server_ids"]:
            raise schema_error("inventory")
        seen.add(server["id"])
        if (
            not isinstance(server["status"], str)
            or server["status"] not in STATES
            or type(server["keypair_present"]) is not bool
            or data["context"] is None
            or server["project_id"] != data["context"]["project_id"]
        ):
            raise schema_error("inventory")
        if server["image_id"] is not None:
            uuid_value(server["image_id"])
        address_list({"addresses": server["addresses"]})
        flavor = fields(server["flavor"], ("id", "vcpus", "ram_mb", "disk_gb"))
        identifier(flavor["id"])
        integer(flavor["vcpus"], 1)
        integer(flavor["ram_mb"], 1)
        integer(flavor["disk_gb"])
    if data["quota"] is not None:
        fields(data["quota"], ("cores", "ram", "instances"))
        for value in data["quota"].values():
            integer(value, -1)
    if data["networks"] is not None:
        if not isinstance(data["networks"], list) or len(data["networks"]) > 100:
            raise schema_error("inventory")
        for net in data["networks"]:
            fields(net, ("id", "status", "subnet_ids"))
        network_summary(
            [
                {"ID": net["id"], "Status": net["status"], "Subnets": net["subnet_ids"]}
                for net in data["networks"]
            ]
        )
    if data["complete"] and (
        snapshot["status"] != "ok"
        or snapshot["errors"]
        or not snapshot["checks"]
        or any(check["state"] != "ok" for check in snapshot["checks"])
        or not data["context_verified"]
        or data["context"] is None
        or not seen
        or seen != set(data["intended_server_ids"])
        or data["quota"] is None
        or data["networks"] is None
    ):
        raise schema_error("inventory")
    return data


def plan(args):
    spec = validate_spec(args.spec)
    saved = validate_snapshot(args.inventory)
    now = datetime.now(UTC)
    age = (now - timestamp(saved["observed_at"])).total_seconds()
    checks = [
        {"name": "inventory_complete", "state": "ok" if saved["complete"] else "pending"},
        {"name": "inventory_fresh", "state": "ok" if 0 <= age <= 86400 else "stale_or_future"},
        {
            "name": "context",
            "state": "ok"
            if saved["context"] == spec["context"] and saved["context_verified"]
            else "mismatch",
        },
    ]
    inventory_usable = all(check["state"] == "ok" for check in checks)
    servers = {server["id"]: server for server in saved["servers"]}
    costs = []
    for vm in spec["vms"]:
        server = servers.get(vm["id"])
        state = "ok"
        if vm["id"] not in saved["intended_server_ids"] or server is None:
            state = "not_observed_or_allowlisted"
        elif server["status"] not in PLAN_STATES:
            state = "incompatible_vm_state"
        rate = vm["rate"]
        if rate is None or rate["verified"] is not True:
            checks.append({"name": f"{vm['role']}_rate", "state": "missing_or_unverified"})
        elif server is None or rate["flavor_id"] != server["flavor"]["id"]:
            checks.append({"name": f"{vm['role']}_rate", "state": "flavor_mismatch"})
        else:
            estimate = rate["su_per_hour"] * (vm["active_seconds"] / 3600)
            number(estimate)
            if inventory_usable and state == "ok":
                costs.append(
                    {
                        "role": vm["role"],
                        "id": vm["id"],
                        "active_seconds": vm["active_seconds"],
                        "su_per_hour": rate["su_per_hour"],
                        "estimated_su": estimate,
                        "rate_source": rate["source"],
                        "rate_observed_at": rate["observed_at"],
                    }
                )
            checks.append({"name": f"{vm['role']}_rate", "state": "ok"})
        checks.append({"name": vm["role"], "state": state})
    pending = any(check["state"] != "ok" for check in checks)
    total = sum(row["estimated_su"] for row in costs) if not pending else None
    if total is not None:
        number(total)
    return outcome(
        "plan",
        "pending" if pending else "ok",
        checks=checks,
        next_actions=[
            "Resolve named prerequisites using a fresh complete inventory and verified per-VM rates with source, time and matching flavor.",
            "This validates a bounded specification only. No resource was activated, and guest/network readiness is not inferred.",
            "Activation is not guaranteed. Stopping guest services does not establish that billing stops; verify provider lifecycle/billing rules manually.",
            "pilot lifecycle commands require separate explicit preparation and operational authorization; guest deployment and experiments are not implemented.",
        ],
        data={
            "validated": not pending,
            "context": spec["context"],
            "inventory_observed_at": saved["observed_at"],
            "vms": costs,
            "total_estimated_su": total,
            "arithmetic": "SU/hour * active_seconds / 3600; sum per VM",
            "guest_readiness": "not_assessed",
            "resources_activated": 0,
        },
    ), 3 if pending else 0


def check_output(value):
    path = absolute_path(value)
    with private_directory(path.parent) as parent:
        try:
            os.stat(path.name, dir_fd=parent, follow_symlinks=False)
        except FileNotFoundError:
            return
    raise OpsError(
        "output_exists",
        "Requested output already exists; replacement is refused.",
        "Choose a fresh filename in an existing owner-private directory; preserve the previous result.",
    )


def save_output(value, result):
    path = absolute_path(value)
    with private_directory(path.parent) as parent:
        try:
            fd = os.open(
                path.name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=parent
            )
        except FileExistsError:
            raise OpsError(
                "output_exists",
                "Requested output already exists; replacement is refused.",
                "Choose a fresh owner-private output path.",
            ) from None
        with os.fdopen(fd, "w") as stream:
            os.fchmod(stream.fileno(), 0o600)
            json.dump(result, stream, indent=2, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())


class JsonParser(argparse.ArgumentParser):
    def error(self, message):
        # argparse's default error can echo secrets supplied as unknown arguments.
        raise OpsError(
            "invalid_arguments", "Invalid command arguments.", "Run --help for supported arguments."
        )

    def print_help(self, file=None):
        print(json.dumps(outcome("help", "ok", data={"help": self.format_help()})), file=file)


def parser():
    result = JsonParser(description=__doc__, allow_abbrev=False)
    commands = result.add_subparsers(dest="command", required=True)
    init = commands.add_parser(
        "init", help="Prepare private local directories and a profile.", allow_abbrev=False
    )
    init.add_argument("--state-root", default=str(Path.home() / ".local/share/flowdc-ops"))
    init.add_argument("--config-root", default=str(Path.home() / ".config/flowdc"))
    doc = commands.add_parser(
        "doctor", help="Inspect local readiness without reading credentials.", allow_abbrev=False
    )
    doc.add_argument("--profile", default=str(Path.home() / ".config/flowdc/profile.json"))
    doc.add_argument("--state-root", default=str(Path.home() / ".local/share/flowdc-ops"))
    inv = commands.add_parser(
        "inventory", help="Capture scoped read-only cloud observations.", allow_abbrev=False
    )
    inv.add_argument("--profile", required=True)
    inv.add_argument("--output")
    pilot = commands.add_parser(
        "plan", help="Validate an offline three-VM pilot specification.", allow_abbrev=False
    )
    pilot.add_argument("--spec", required=True)
    pilot.add_argument("--inventory", required=True)
    pilot.add_argument("--output")
    siblings = (
        "flowdc_pilot",
        "flowdc_pilot_cli",
        "flowdc_pilot_journal",
        "flowdc_pilot_provider",
        "flowdc_pilot_supervisor",
    )
    if all(importlib.util.find_spec(name) is not None for name in siblings):
        from flowdc_pilot_cli import arguments

        arguments(commands)
    else:
        unavailable = commands.add_parser("pilot", help="Pilot modules are not installed.")
        unavailable.add_argument("pilot_args", nargs=argparse.REMAINDER)
        unavailable.set_defaults(pilot_unavailable=True)
    return result


def main(argv=None):
    operation = "cli"
    try:
        args = parser().parse_args(argv)
        operation = args.command
        if getattr(args, "output", None):
            check_output(args.output)
        if args.command == "init":
            result, exit_code = initialize(args), 0
        elif args.command == "pilot":
            if getattr(args, "pilot_unavailable", False):
                raise OpsError(
                    "pilot_unavailable",
                    "Pilot modules are not installed.",
                    "Use the complete reviewed release for pilot commands.",
                    3,
                )
            from flowdc_pilot_cli import run

            result, exit_code = run(args)
        else:
            result, exit_code = {"doctor": doctor, "inventory": inventory, "plan": plan}[args.command](args)
        if getattr(args, "output", None):
            save_output(args.output, result)
    except OpsError as exc:
        result = outcome(
            operation,
            "invalid" if exc.exit_code == 2 else "pending" if exc.exit_code == 3 else "error",
            errors=[{"code": exc.code, "message": exc.message}],
            next_actions=[exc.action],
        )
        exit_code = exc.exit_code
    except OSError:
        result = outcome(
            operation,
            "error",
            errors=[{"code": "local_io_error", "message": "Local filesystem operation failed."}],
            next_actions=[
                "Inspect available disk space, access permissions and any partially created setup files. "
                "Existing files were not replaced; retry only after resolving the local problem."
            ],
        )
        exit_code = 1
    print(json.dumps(result, allow_nan=False))
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
