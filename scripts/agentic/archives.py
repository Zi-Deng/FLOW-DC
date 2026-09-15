"""Journaled ignored-artifact transfers; never follow links or overwrite payloads."""

from __future__ import annotations

import contextlib
import ctypes
import errno
import hashlib
import json
import os
import shutil
import stat
import uuid
from pathlib import Path

from workflow import WorkflowError

DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW


def relative_name(value, *, empty=False):
    if empty and value == "":
        return value
    if (
        not isinstance(value, str)
        or value.startswith("/")
        or any(part in {"", ".", ".."} for part in value.split("/"))
    ):
        raise WorkflowError("Invalid archive-relative path")
    return value


@contextlib.contextmanager
def directory(path, *, create=False):
    """Open each directory component without dereferencing symlinks."""
    path = Path(path)
    if not path.is_absolute() or ".." in path.parts:
        raise WorkflowError("Archive operations require absolute paths without traversal")
    fd = os.open("/", DIRECTORY_FLAGS)
    try:
        for part in path.parts[1:]:
            if create:
                try:
                    os.mkdir(part, 0o700, dir_fd=fd)
                except FileExistsError:
                    pass
            try:
                child = os.open(part, DIRECTORY_FLAGS, dir_fd=fd)
            except OSError as exc:
                if exc.errno in {errno.ELOOP, errno.ENOTDIR}:
                    raise WorkflowError("Archive path contains a symlink or non-directory") from exc
                raise
            os.close(fd)
            fd = child
        yield fd
    finally:
        os.close(fd)


def exists(path):
    path = Path(path)
    try:
        with directory(path.parent) as parent:
            os.stat(path.name, dir_fd=parent, follow_symlinks=False)
        return True
    except FileNotFoundError:
        return False


def stamp(value):
    return [value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns]


def snapshot(path):
    """Record content and identities, detecting changes observed during traversal."""
    result = {}

    def visit(parent, name, relative):
        before = os.stat(name, dir_fd=parent, follow_symlinks=False)
        item = {"mode": stat.S_IMODE(before.st_mode), "stamp": stamp(before)}
        if stat.S_ISLNK(before.st_mode):
            item.update(kind="symlink", target=os.readlink(name, dir_fd=parent))
        elif stat.S_ISREG(before.st_mode):
            fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
            try:
                if stamp(os.fstat(fd)) != stamp(before):
                    raise WorkflowError("Archive source changed while opening a file")
                checksum = hashlib.sha256()
                while chunk := os.read(fd, 1024 * 1024):
                    checksum.update(chunk)
                if stamp(os.fstat(fd)) != stamp(before):
                    raise WorkflowError("Archive file changed while reading")
            finally:
                os.close(fd)
            item.update(kind="file", size=before.st_size, sha256=checksum.hexdigest())
        elif stat.S_ISDIR(before.st_mode):
            fd = os.open(name, DIRECTORY_FLAGS, dir_fd=parent)
            try:
                if stamp(os.fstat(fd)) != stamp(before):
                    raise WorkflowError("Archive directory changed while opening")
                names = sorted(os.listdir(fd))
                for child in names:
                    visit(fd, child, f"{relative}/{child}" if relative else child)
                if sorted(os.listdir(fd)) != names or stamp(os.fstat(fd)) != stamp(before):
                    raise WorkflowError("Archive directory changed during traversal")
            finally:
                os.close(fd)
            item["kind"] = "directory"
        else:
            raise WorkflowError("Unsupported archive object: only files, directories and symlinks")
        if stamp(os.stat(name, dir_fd=parent, follow_symlinks=False)) != stamp(before):
            raise WorkflowError("Archive source changed during inspection")
        result[relative] = item

    path = Path(path)
    with directory(path.parent) as parent:
        visit(parent, path.name, "")
    return result


def content(manifest):
    return {
        name: {key: value for key, value in item.items() if key != "stamp"} for name, item in manifest.items()
    }


def subtree(manifest, name):
    if not name:
        return manifest
    prefix = name + "/"
    return {
        "" if key == name else key[len(prefix) :]: value
        for key, value in manifest.items()
        if key == name or key.startswith(prefix)
    }


def verify_content(path, expected):
    observed = snapshot(path)
    if content(observed) != content(expected):
        raise WorkflowError("Archive verification failed; retained source and destination need inspection")
    return observed


def rename_noreplace(source, destination):
    """Linux atomic no-replace rename. Unsupported platforms fail closed."""
    source, destination = Path(source), Path(destination)
    libc = ctypes.CDLL(None, use_errno=True)
    rename = getattr(libc, "renameat2", None)
    if rename is None:
        raise WorkflowError("Atomic no-replace rename is unavailable on this platform")
    rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    rename.restype = ctypes.c_int
    with directory(source.parent) as src, directory(destination.parent, create=True) as dst:
        if rename(src, os.fsencode(source.name), dst, os.fsencode(destination.name), 1):
            code = ctypes.get_errno()
            if code == errno.EEXIST:
                raise WorkflowError("Archive destination collision; neither side was overwritten")
            if code in {errno.ENOSYS, errno.EINVAL, errno.ENOTSUP}:
                raise WorkflowError("Filesystem lacks atomic no-replace rename")
            raise OSError(code, os.strerror(code))
        os.fsync(src)
        os.fsync(dst)


def copy_tree(source, destination):
    """Copy into a fresh destination using descriptor-relative, exclusive creation."""

    def copy_node(src_parent, src_name, dst_parent, dst_name):
        info = os.stat(src_name, dir_fd=src_parent, follow_symlinks=False)
        if stat.S_ISLNK(info.st_mode):
            os.symlink(os.readlink(src_name, dir_fd=src_parent), dst_name, dir_fd=dst_parent)
        elif stat.S_ISREG(info.st_mode):
            src = os.open(src_name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=src_parent)
            try:
                if not stat.S_ISREG(os.fstat(src).st_mode) or stamp(os.fstat(src)) != stamp(info):
                    raise WorkflowError("Copy source changed")
                dst = os.open(
                    dst_name,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                    0o600,
                    dir_fd=dst_parent,
                )
                with os.fdopen(dst, "wb") as target, os.fdopen(os.dup(src), "rb") as origin:
                    shutil.copyfileobj(origin, target, 1024 * 1024)
                    target.flush()
                    os.fchmod(target.fileno(), stat.S_IMODE(info.st_mode))
                    os.utime(target.fileno(), ns=(info.st_atime_ns, info.st_mtime_ns))
                    os.fsync(target.fileno())
                if stamp(os.fstat(src)) != stamp(info):
                    raise WorkflowError("Copy source changed while reading")
            finally:
                os.close(src)
        elif stat.S_ISDIR(info.st_mode):
            os.mkdir(dst_name, 0o700, dir_fd=dst_parent)
            src = os.open(src_name, DIRECTORY_FLAGS, dir_fd=src_parent)
            dst = os.open(dst_name, DIRECTORY_FLAGS, dir_fd=dst_parent)
            try:
                if stamp(os.fstat(src)) != stamp(info):
                    raise WorkflowError("Copy directory changed")
                for child in sorted(os.listdir(src)):
                    copy_node(src, child, dst, child)
                os.fchmod(dst, stat.S_IMODE(info.st_mode))
                os.utime(dst, ns=(info.st_atime_ns, info.st_mtime_ns))
                os.fsync(dst)
            finally:
                os.close(src)
                os.close(dst)
        else:
            raise WorkflowError("Unsupported copy source")
        os.fsync(dst_parent)

    source, destination = Path(source), Path(destination)
    with directory(source.parent) as src, directory(destination.parent, create=True) as dst:
        copy_node(src, source.name, dst, destination.name)


def save_journal(destination, journal):
    with directory(destination) as parent:
        temporary = ".journal-" + uuid.uuid4().hex
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600, dir_fd=parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                json.dump(journal, stream, indent=2)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            try:
                old = os.stat("journal.json", dir_fd=parent, follow_symlinks=False)
                if not stat.S_ISREG(old.st_mode):
                    raise WorkflowError("Archive journal is not a regular file")
            except FileNotFoundError:
                pass
            os.replace(temporary, "journal.json", src_dir_fd=parent, dst_dir_fd=parent)
            os.fsync(parent)
        finally:
            try:
                os.unlink(temporary, dir_fd=parent)
            except FileNotFoundError:
                pass


def load_journal(destination):
    with directory(destination) as parent:
        fd = os.open("journal.json", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        with os.fdopen(fd, "r", encoding="utf-8") as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                raise WorkflowError("Archive journal is not regular")
            return json.load(stream)


def initialize(source, destination, identity, roots):
    source, destination = Path(source), Path(destination)
    if exists(destination):
        try:
            journal = load_journal(destination)
        except FileNotFoundError as exc:
            raise WorkflowError("Archive destination already exists without its journal") from exc
        if (
            journal.get("schema_version") != 1
            or journal.get("identity") != identity
            or journal.get("source") != str(source)
            or journal.get("destination") != str(destination)
        ):
            raise WorkflowError("Archive journal identity differs")
        for entry in journal["entries"]:
            relative_name(entry["root"])
            if entry["root"].split("/")[0] == ".git":
                raise WorkflowError("Git metadata cannot be an archive root")
            for name in entry["manifest"]:
                relative_name(name, empty=True)
        return journal
    entries = []
    for root in roots:
        relative_name(root)
        if root.split("/")[0] == ".git":
            raise WorkflowError("Git metadata cannot be an archive root")
        entries.append({"root": root, "manifest": snapshot(source / root), "phase": "planned", "deleted": []})
    journal = {
        "schema_version": 1,
        "identity": identity,
        "source": str(source),
        "destination": str(destination),
        "entries": entries,
        "complete": False,
    }
    staging = destination.parent / (".initializing-" + uuid.uuid4().hex)
    with directory(staging.parent, create=True) as parent:
        os.mkdir(staging.name, 0o700, dir_fd=parent)
        os.fsync(parent)
    save_journal(staging, journal)
    # A crash before this rename leaves only a private initialization record.
    # No source artifact has moved, and retries never overwrite that record.
    rename_noreplace(staging, destination)
    return journal


def copy_path(destination, value):
    parts = relative_name(value).split("/")
    if len(parts) != 2 or parts[0] != ".copies" or len(parts[1]) != 32:
        raise WorkflowError("Invalid retained-copy path")
    if any(character not in "0123456789abcdef" for character in parts[1]):
        raise WorkflowError("Invalid retained-copy identifier")
    return Path(destination) / value


def remove_source(source, target, entry, destination, journal):
    manifest = entry["manifest"]
    verify_content(target, manifest)
    if not entry["deleted"] and entry.get("delete_pending") is None:
        if snapshot(source) != manifest:
            raise WorkflowError("Source changed after copy; retaining both sides")
    names = sorted(manifest, key=lambda name: (name.count("/") + bool(name), name), reverse=True)
    for name in names:
        path = source / name if name else source
        archived = target / name if name else target
        if name in entry["deleted"]:
            if exists(path):
                raise WorkflowError("A removed source path reappeared; preserve the new data")
            continue
        verify_content(archived, subtree(manifest, name))
        if not exists(path):
            if entry.get("delete_pending") != name:
                raise WorkflowError("Source disappeared outside its recorded removal step")
        else:
            expected = manifest[name]
            if expected["kind"] == "directory":
                with directory(path) as fd:
                    observed = os.fstat(fd)
                    if (
                        stamp(observed)[:2] != expected["stamp"][:2]
                        or stat.S_IMODE(observed.st_mode) != expected["mode"]
                        or os.listdir(fd)
                    ):
                        raise WorkflowError("Source directory changed during archival")
            elif snapshot(path) != {"": expected}:
                raise WorkflowError("Source file changed during archival")
            entry["delete_pending"] = name
            save_journal(destination, journal)
            with directory(path.parent) as parent:
                if expected["kind"] == "directory":
                    os.rmdir(path.name, dir_fd=parent)
                else:
                    os.unlink(path.name, dir_fd=parent)
                os.fsync(parent)
        entry["deleted"].append(name)
        entry["delete_pending"] = None
        save_journal(destination, journal)


def archive(source, destination, identity, roots, guard):
    source, destination = Path(source), Path(destination)
    guard()
    journal = initialize(source, destination, identity, roots)
    for entry in journal["entries"]:
        guard()
        origin = source / entry["root"]
        target = destination / "payload" / entry["root"]
        expected = entry["manifest"]
        with directory(target.parent, create=True):
            pass
        if entry["phase"] in {"planned", "moving"}:
            if exists(target):
                if exists(origin) or entry["phase"] != "moving":
                    raise WorkflowError("Archive destination collision; preserve both sides")
                observed = verify_content(target, expected)
                if observed[""]["stamp"][:2] != expected[""]["stamp"][:2]:
                    raise WorkflowError("Moved archive identity differs")
                entry["phase"] = "done"
                save_journal(destination, journal)
            else:
                if snapshot(origin) != expected:
                    raise WorkflowError("Archive source changed before transfer")
                entry["phase"] = "moving"
                save_journal(destination, journal)
                try:
                    rename_noreplace(origin, target)
                except OSError as exc:
                    if exc.errno != errno.EXDEV:
                        raise
                    entry["phase"] = "copying"
                    save_journal(destination, journal)
                else:
                    verify_content(target, expected)
                    if exists(origin):
                        raise WorkflowError("Source reappeared after rename")
                    entry["phase"] = "done"
                    save_journal(destination, journal)
        if entry["phase"] == "copying":
            if exists(target):
                raise WorkflowError("Archive destination collision")
            if snapshot(origin) != expected:
                raise WorkflowError("Copy source changed; retaining all recovery data")
            previous = entry.get("copy")
            copied = None
            if previous and exists(copy_path(destination, previous)):
                candidate = copy_path(destination, previous)
                try:
                    copied = verify_content(candidate, expected)
                except WorkflowError:
                    entry.setdefault("retained_copies", []).append(previous)
            if copied is None:
                entry["copy"] = ".copies/" + uuid.uuid4().hex
                save_journal(destination, journal)
                candidate = copy_path(destination, entry["copy"])
                copy_tree(origin, candidate)
                copied = verify_content(candidate, expected)
            if snapshot(origin) != expected:
                raise WorkflowError("Source changed during copying; neither side will be removed")
            entry["copy_manifest"] = copied
            entry["phase"] = "promoting"
            save_journal(destination, journal)
        if entry["phase"] == "promoting":
            candidate = copy_path(destination, entry["copy"])
            if exists(target):
                if exists(candidate):
                    raise WorkflowError("Archive promotion collision")
                observed = verify_content(target, expected)
                if observed[""]["stamp"][:2] != entry["copy_manifest"][""]["stamp"][:2]:
                    raise WorkflowError("Promoted copy identity differs")
            else:
                verify_content(candidate, expected)
                rename_noreplace(candidate, target)
            entry["phase"] = "removing"
            save_journal(destination, journal)
        if entry["phase"] == "removing":
            guard()
            remove_source(origin, target, entry, destination, journal)
            entry["phase"] = "done"
            save_journal(destination, journal)
        if entry["phase"] != "done":
            raise WorkflowError("Unsupported archive recovery phase")
        verify_content(target, expected)
        if exists(origin):
            raise WorkflowError("Archived source path now contains new data")
    guard()
    journal["complete"] = True
    save_journal(destination, journal)
    return {
        "directory": str(destination),
        "journal": str(destination / "journal.json"),
        "complete": True,
        "retained_copies": [
            str(copy_path(destination, name))
            for entry in journal["entries"]
            for name in entry.get("retained_copies", [])
        ],
    }
