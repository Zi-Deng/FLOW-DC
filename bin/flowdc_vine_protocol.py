"""Bounded retained TaskVine returns; transport success never grants useful credit."""

import hashlib
import io
import json
import os
import stat
import tarfile
from pathlib import Path

SCHEMA = "flowdc-vine-return-v1"
MAX_RETURN = 192 * 1024 * 1024
MAX_FILES = 8192


def require(condition, message):
    if not condition:
        raise ValueError(message)


def encode(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def parse(raw):
    def pairs(items):
        value = {}
        for key, item in items:
            require(key not in value, "duplicate return JSON key")
            value[key] = item
        return value

    return json.loads(raw, object_pairs_hook=pairs, parse_constant=lambda _: require(False, "nonfinite JSON"))


def write_new(path, value):
    with Path(path).open("xb") as stream:
        stream.write(encode(value))
        stream.flush()
        os.fsync(stream.fileno())


def safe_name(name):
    require(
        isinstance(name, str)
        and name
        and "\\" not in name
        and "\x00" not in name
        and not name.startswith("/")
        and all(p not in ("", ".", "..") for p in name.split("/")),
        "unsafe returned member",
    )
    return name


def pack_return(directory, output):
    """Retain native archives, journals, outcomes and logs, excluding staged secrets."""
    directory, output = Path(directory), Path(output)
    files, total = {}, 0
    for path in sorted(directory.rglob("*")):
        info = path.lstat()
        require(not stat.S_ISLNK(info.st_mode), "symlink in worker return")
        if stat.S_ISDIR(info.st_mode):
            continue
        require(stat.S_ISREG(info.st_mode), "nonregular worker return")
        name = safe_name(path.relative_to(directory).as_posix())
        total += info.st_size
        require(
            total <= MAX_RETURN - 8 * 1024 * 1024 and len(files) < MAX_FILES - 1,
            "worker return bound exceeded",
        )
        raw = path.read_bytes()
        files[name] = {"bytes": len(raw), "sha256": digest(raw)}
    write_new(directory / "return-index.json", {"schema": SCHEMA, "files": files})
    with output.open("xb") as stream:
        with tarfile.open(fileobj=stream, mode="w", dereference=True) as archive:
            for name in [*files, "return-index.json"]:
                archive.add(directory / name, arcname=name, recursive=False)
        stream.flush()
        os.fsync(stream.fileno())
    require(output.stat().st_size <= MAX_RETURN, "oversize return archive")
    return digest(output.read_bytes())


def unpack_return(archive_path, destination):
    """Independent safe extraction into a new directory; reject before mutation."""
    path, destination = Path(archive_path), Path(destination)
    require(
        not path.is_symlink() and path.is_file() and path.stat().st_size <= MAX_RETURN,
        "missing/oversize return archive",
    )
    raw = path.read_bytes()
    files, end = {}, 0
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:") as archive:
        for member in archive:
            name = safe_name(member.name)
            require(
                member.isreg()
                and not member.issparse()
                and name not in files
                and len(files) < MAX_FILES
                and 0 <= member.size <= MAX_RETURN,
                "invalid returned archive member",
            )
            files[name] = archive.extractfile(member).read()
            require(len(files[name]) == member.size, "truncated return")
            end = member.offset_data + ((member.size + 511) // 512) * 512
    require(len(raw) % 512 == 0 and len(raw) - end >= 1024 and not any(raw[end:]), "unclosed return archive")
    index = parse(files.pop("return-index.json"))
    require(index["schema"] == SCHEMA and set(index["files"]) == set(files), "return membership mismatch")
    require(
        all(
            index["files"][name] == {"bytes": len(value), "sha256": digest(value)}
            for name, value in files.items()
        ),
        "return digest mismatch",
    )
    require({"identity.json", "receipt.json"} <= set(files), "missing identity/receipt")
    identity, receipt = parse(files["identity.json"]), parse(files["receipt.json"])
    require(
        all(
            parent.as_posix() not in files
            for name in files
            for parent in Path(name).parents
            if parent.as_posix() != "."
        ),
        "conflicting returned paths",
    )
    destination.mkdir(parents=False, exist_ok=False)
    for name, content in files.items():
        target = destination / name
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("xb") as stream:
            stream.write(content)
    write_new(destination / "return-index.json", index)
    return identity, receipt, digest(raw)
