#!/usr/bin/env python3
"""Private local setup for FLOW-DC operations (Python 3.12+, Linux).

This first implementation checkpoint exposes init only. Cloud access and execution
are deliberately absent; later commands will use the same JSON outcome contract.
"""

import argparse
import errno
import json
import os
import stat
import sys
from contextlib import contextmanager
from pathlib import Path

SCHEMA_VERSION = 1
DIRECTORIES = ("inventory", "releases", "runs")
DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC


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


def private_metadata(info, *, directory=False):
    expected = 0o700 if directory else 0o600
    correct_type = stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)
    if not correct_type or (not directory and info.st_nlink != 1):
        raise invalid_path()
    if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != expected:
        raise OpsError(
            "unsafe_permissions",
            "An existing managed path is not owner-private with the required permissions.",
            "Inspect ownership and permissions manually: managed directories require 0700 "
            "and profile files 0600. Existing paths are never chmodded automatically.",
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
def private_directory(path, *, create=False):
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
        private_metadata(os.fstat(fd), directory=True)
        yield fd
    finally:
        os.close(fd)


def inspect_child(fd, name, *, directory=False, required=False):
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
    private_metadata(info, directory=directory)
    return True


def preflight_root(path, *, directories=(), files=()):
    try:
        with private_directory(path) as fd:
            for name in directories:
                inspect_child(fd, name, directory=True)
            for name in files:
                inspect_child(fd, name)
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
    preflight_root(config_root, files=("profile.json",))
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
        profile_state = create_profile(
            fd,
            {
                "schema_version": SCHEMA_VERSION,
                "expected_project_id": None,
                "region": None,
                "credential_file": None,
                "wrapper_path": None,
                "openstack_client": None,
                "intended_server_ids": [],
                "known_seed_id": None,
            },
        )
    checks.append({"name": "profile", "state": profile_state})
    checks.append({"name": "cloud_readiness", "state": "not_assessed"})
    return outcome(
        "init",
        "ok",
        checks=checks,
        next_actions=[
            "Edit profile.json privately after manual enrollment and verification of project, region, "
            "credential/client paths, and intended server UUIDs; no credentials have been read or created.",
            "This checkpoint implements init only. Wrapper creation, doctor, inventory, and plan "
            "are pending implementation; no cloud or SSH readiness has been established.",
        ],
        data={"state_root": str(state_root), "config_root": str(config_root)},
    )


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
    return result


def main(argv=None):
    operation = "cli"
    try:
        args = parser().parse_args(argv)
        operation = args.command
        result = initialize(args)
        exit_code = 0
    except OpsError as exc:
        result = outcome(
            operation,
            "invalid" if exc.exit_code == 2 else "error",
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
