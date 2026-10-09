"""Bounded retained TaskVine returns; transport success never grants useful credit."""

import hashlib
import json
import os
import shutil
import stat
import tarfile
from pathlib import Path
from flowdc_research_profile import LEGACY, from_record, workload

SCHEMA = "flowdc-vine-return-v1"
MAX_RETURN = LEGACY.max_return_bytes
MAX_FILES = LEGACY.max_files


def require(condition, message):
    if not condition:
        raise ValueError(message)


def encode(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def stream_digest(stream):
    checksum = hashlib.sha256()
    size = 0
    while chunk := stream.read(1024 * 1024):
        checksum.update(chunk)
        size += len(chunk)
    return {"bytes": size, "sha256": checksum.hexdigest()}


class BoundedWriter:
    def __init__(self, stream, maximum):
        self.stream, self.maximum, self.written = stream, maximum, 0

    def write(self, raw):
        require(self.written + len(raw) <= self.maximum, "worker return bound exceeded")
        count = self.stream.write(raw)
        self.written += count
        return count


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


def pack_return(directory, output, *, research_workload=None):
    """Retain native archives, journals, outcomes and logs, excluding staged secrets."""
    directory, output = Path(directory), Path(output)
    limits = workload(research_workload)
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
            total <= limits.max_return_bytes - 8 * 1024 * 1024 and len(files) < limits.max_files - 1,
            "worker return bound exceeded",
        )
        with path.open("rb") as stream:
            files[name] = stream_digest(stream)
        require(files[name]["bytes"] == info.st_size, "worker file changed during hashing")
    index = {"schema": SCHEMA, "files": files}
    if research_workload is not None:
        index.update(schema="flowdc-vine-return-v2", workload=limits.record())
    require(len(encode(index)) <= limits.max_metadata_bytes, "return index bound exceeded")
    write_new(directory / "return-index.json", index)
    with output.open("xb") as stream:
        with tarfile.open(fileobj=BoundedWriter(stream, limits.max_return_bytes), mode="w|", dereference=True) as archive:
            for name in [*files, "return-index.json"]:
                archive.add(directory / name, arcname=name, recursive=False)
        stream.flush()
        os.fsync(stream.fileno())
    require(output.stat().st_size <= limits.max_return_bytes, "oversize return archive")
    with output.open("rb") as stream:
        return stream_digest(stream)["sha256"]


def unpack_return(archive_path, destination, *, research_workload=None):
    """Independent safe extraction into a new directory; reject before mutation."""
    path, destination = Path(archive_path), Path(destination)
    limits = workload(research_workload)
    require(
        not path.is_symlink() and path.is_file() and path.stat().st_size <= limits.max_return_bytes,
        "missing/oversize return archive",
    )
    files, end, metadata = {}, 0, {}
    with tarfile.open(path, mode="r:") as archive:
        for member in archive:
            name = safe_name(member.name)
            require(
                member.isreg()
                and not member.issparse()
                and name not in files
                and len(files) < limits.max_files
                and 0 <= member.size <= limits.max_return_bytes,
                "invalid returned archive member",
            )
            with archive.extractfile(member) as stream:
                files[name] = stream_digest(stream)
            require(files[name]["bytes"] == member.size, "truncated return")
            if name in ("return-index.json", "identity.json", "receipt.json"):
                require(member.size <= limits.max_metadata_bytes, "return metadata bound exceeded")
                metadata[name] = parse(archive.extractfile(member).read())
            end = member.offset_data + ((member.size + 511) // 512) * 512
    size = path.stat().st_size
    require(size % 512 == 0 and size - end >= 1024, "unclosed return archive")
    with path.open("rb") as stream:
        stream.seek(end)
        while chunk := stream.read(1024 * 1024):
            require(not any(chunk), "unclosed return archive")
    index = metadata["return-index.json"]
    files.pop("return-index.json")
    expected_schema = SCHEMA if research_workload is None else "flowdc-vine-return-v2"
    require(index["schema"] == expected_schema and set(index["files"]) == set(files), "return membership mismatch")
    if research_workload is not None:
        require(from_record(index.get("workload")) == limits, "return workload mismatch")
    require(index["files"] == files, "return digest mismatch")
    require({"identity.json", "receipt.json"} <= set(files), "missing identity/receipt")
    identity, receipt = metadata["identity.json"], metadata["receipt.json"]
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
    extracted = set()
    with tarfile.open(path, mode="r:") as archive:
        for member in archive:
            if member.name == "return-index.json":
                continue
            name = safe_name(member.name)
            require(name in files and name not in extracted and member.isreg()
                    and not member.issparse() and member.size == files[name]["bytes"],
                    "return changed during extraction")
            extracted.add(name)
            target = destination / name
            target.parent.mkdir(parents=True, exist_ok=True)
            with archive.extractfile(member) as source, target.open("xb") as stream:
                shutil.copyfileobj(source, stream, 1024 * 1024)
            with target.open("rb") as stream:
                require(stream_digest(stream) == files[member.name], "return changed during extraction")
    require(extracted == set(files), "return changed during extraction")
    write_new(destination / "return-index.json", index)
    with path.open("rb") as stream:
        return identity, receipt, stream_digest(stream)["sha256"]
