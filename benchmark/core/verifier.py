"""Independent verification of bounded native FLOW-DC/img2dataset artifacts.

No native success counter or filename extension grants useful-byte credit. A
structurally invalid required artifact earns no credit; native files are untouched.
"""

import io
import hashlib
import re
import stat
import tarfile
from collections import Counter
from collections.abc import Mapping
from pathlib import Path

import polars as pl

from .truth import (
    LEGACY,
    PROVENANCE,
    digest,
    encode,
    initial_outcomes,
    parse,
    require,
    validate_partition,
    truth_workload,
)

MAX_ARTIFACT_BYTES = LEGACY.max_artifact_bytes


def file_info(path, maximum):
    path = Path(path).absolute()
    for parent in (*reversed(path.parents), path):
        require(not parent.is_symlink(), f"symlink artifact: {parent}")
    info = path.stat()
    require(stat.S_ISREG(info.st_mode) and info.st_size <= maximum, "invalid/oversized artifact")
    return info


def read_file(path, maximum=MAX_ARTIFACT_BYTES):
    file_info(path, maximum)
    return Path(path).read_bytes()


def identity(info):
    # Reading can update atime; only mutation/identity fields bind the artifact.
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns


def file_digest(path, maximum):
    info = file_info(path, maximum)
    checksum = hashlib.sha256()
    with Path(path).open('rb') as stream:
        while chunk := stream.read(1024 * 1024):
            checksum.update(chunk)
    require(identity(Path(path).stat()) == identity(info), 'artifact changed during hashing')
    return checksum.hexdigest(), info.st_size


class ArchiveMembers(Mapping):
    """Validated offsets; hold at most one finite member body per caller read."""
    def __init__(self, path, info, entries):
        self.path, self.info, self.entries = Path(path), info, entries

    def __len__(self):
        return len(self.entries)

    def __iter__(self):
        return iter(self.entries)

    def __getitem__(self, key):
        offset, size = self.entries[key]
        require(identity(self.path.stat()) == identity(self.info), 'archive changed during verification')
        with self.path.open('rb') as stream:
            stream.seek(offset)
            raw = stream.read(size)
        require(len(raw) == size and identity(self.path.stat()) == identity(self.info), 'archive truncated/changed')
        return raw


def safe_name(name):
    require(
        isinstance(name, str)
        and name
        and "\\" not in name
        and "\x00" not in name
        and not name.startswith("/")
        and all(p not in ("", ".", "..") for p in name.split("/")),
        "unsafe archive member",
    )
    return name


def archive_members(path, limits=LEGACY):
    """Reject compressed, unclosed, concatenated, duplicate and nonregular tars."""
    info = file_info(path, limits.max_artifact_bytes)
    members, end = {}, 0
    with tarfile.open(path, mode="r:") as archive:
        for item in archive:
            safe_name(item.name)
            require(
                item.isreg() and not item.issparse() and item.name not in members,
                "duplicate or nonregular archive member",
            )
            require(
                0 <= item.size <= limits.max_object_bytes and len(members) < 2 * limits.max_rows + 2,
                "oversized archive member/count",
            )
            require(item.offset_data + item.size <= info.st_size, "truncated archive member")
            members[item.name] = (item.offset_data, item.size)
            end = item.offset_data + ((item.size + 511) // 512) * 512
    require(
        info.st_size % 512 == 0 and info.st_size - end >= 1024,
        "archive lacks closure or contains trailing data",
    )
    with Path(path).open('rb') as stream:
        stream.seek(end)
        while chunk := stream.read(1024 * 1024):
            require(not any(chunk), "archive contains trailing data")
    require(identity(Path(path).stat()) == identity(info), 'archive changed during verification')
    return ArchiveMembers(path, info, members)


def _payload(raw, specification):
    require(specification is not None, "no successful payload exists in independent catalog")
    require(
        len(raw) == specification["bytes"] and digest(raw) == specification["sha256"],
        "payload differs from independent original bytes",
    )


def _metadata(actual, expected):
    require(isinstance(actual, dict), "invalid row metadata")
    require(encode({name: actual[name] for name in expected}) == encode(expected), "row metadata mismatch")


def _flowdc(output, truth, outcomes):
    limits = truth_workload(truth)
    index_raw = read_file(output / "outcome-index.json", limits.max_metadata_bytes)
    index = parse(index_raw)
    final = parse(read_file(output / ".flowdc/final.json"))
    require(index["schema_version"] == 2 and final["schema_version"] == 2, "unsupported native schema")
    require(final["completion_boundary"] == "verified_uncompressed_archive", "incorrect native boundary")
    require(final["outcome_index_sha256"] == digest(index_raw), "native index binding mismatch")
    # The native product appends .tar; do not replace dots in a caller's basename.
    tar_path = output.parent / (output.name + ".tar")
    archive_sha, archive_bytes = file_digest(tar_path, limits.max_artifact_bytes)
    require(
        final["archive"]["sha256"] == archive_sha and final["archive"]["bytes"] == archive_bytes,
        "native archive binding mismatch",
    )
    members = archive_members(tar_path, limits)
    prefix = output.name + "/"
    require(members.get(prefix + "outcome-index.json") == index_raw, "archived index mismatch")
    require(isinstance(parse(members[prefix + "overview.json"]), dict), "invalid archived overview")
    expected_names = {prefix + "outcome-index.json", prefix + "overview.json"}
    expected = {row["row_id"]: row for row in truth["rows"] if row["eligible"]}
    seen = set()
    for native in index["rows"]:
        key = native["row_id"]
        require(key in expected and key not in seen, "unexpected/duplicate native row")
        seen.add(key)
        row, outcome = expected[key], outcomes[key]
        disposition = native["disposition"]
        require(disposition in ("verified", "failed", "unattempted"), "invalid native disposition")
        outcome.update(
            native_disposition=disposition,
            native_attempt_intents=native["attempt_intents"],
            native_attempt_information_uncertain=native["attempt_information_uncertain"],
        )
        if disposition != "verified":
            outcome.update(
                disposition="failed" if disposition == "failed" else "missing", error=native["error"]
            )
            continue
        payload_name, metadata_name = (prefix + safe_name(native[k]) for k in ("payload", "metadata"))
        require(
            payload_name != metadata_name and not expected_names.intersection((payload_name, metadata_name)),
            "duplicate row artifact destination",
        )
        expected_names.update((payload_name, metadata_name))
        payload = members[payload_name]
        _payload(payload, row["expected_payload"])
        _metadata(
            parse(members[metadata_name]),
            {
                "schema_version": 2,
                "key": key,
                "row_id": key,
                "url": row["metadata"]["url"],
                "row": row["metadata"],
                "source_manifest": truth["manifest_sha256"],
                "source_position": row["position"],
                "source_rows": truth["original_rows"],
                "payload": native["payload"],
                "payload_bytes": len(payload),
                "payload_sha256": digest(payload),
            },
        )
        require(
            native["payload_bytes"] == len(payload) and native["payload_sha256"] == digest(payload),
            "native row payload claim mismatch",
        )
        outcome.update(disposition="verified", useful_bytes=len(payload), payload_sha256=digest(payload))
    require(set(members) == expected_names, "unexpected/missing archive member")
    return {"native_rows": len(seen), "archives": [tar_path.name]}


def _img2dataset(output, truth, outcomes):
    limits = truth_workload(truth)
    require(output.is_dir() and not output.is_symlink(), "missing/unsafe native output")
    shards = sorted(output.glob("*.parquet"))
    require(bool(shards), "missing native parquet outcome sidecars")
    require(len(shards) <= limits.max_rows, "too many native shards")
    expected = {row["row_id"]: row for row in truth["rows"] if row["eligible"]}
    seen, native_keys, expected_tars, stats_records = set(), set(), set(), []
    for shard in shards:
        require(re.fullmatch(r"[0-9]+", shard.stem), "invalid shard name")
        frame = pl.read_parquet(io.BytesIO(read_file(shard, limits.max_metadata_bytes)))
        require(frame.height <= limits.max_rows, "oversized shard sidecar")
        tar_path = shard.with_suffix(".tar")
        expected_tars.add(tar_path.name)
        members = archive_members(tar_path, limits)
        expected_names = set()
        statuses = Counter()
        for native in frame.iter_rows(named=True):
            key = native[PROVENANCE[3]]
            require(key in expected and key not in seen, "unexpected/duplicate native row")
            seen.add(key)
            row, outcome = expected[key], outcomes[key]
            required = dict(row["metadata"])
            required.update(
                zip(
                    PROVENANCE,
                    (
                        truth["manifest_sha256"],
                        row["position"],
                        truth["original_rows"],
                        key,
                        digest(encode(row["metadata"])),
                    ),
                    strict=True,
                )
            )
            _metadata(native, required)
            native_key, status = native["key"], native["status"]
            require(
                isinstance(native_key, str)
                and re.fullmatch(r"[0-9]+", native_key)
                and native_key not in native_keys,
                "invalid/duplicate native key",
            )
            native_keys.add(native_key)
            require(status in ("success", "failed_to_download", "failed_to_resize"), "invalid native status")
            statuses[status] += 1
            outcome.update(
                native_disposition=status,
                native_attempts=None,
                native_attempt_attribution="unavailable per row",
            )
            if status != "success":
                outcome.update(disposition="failed", error=native["error_message"])
                continue
            # encode_format=jpg is an output key convention even for original PNG
            # streams when all reencoding is disabled. Hash the bytes, not suffixes.
            payload_name, metadata_name = native_key + ".jpg", native_key + ".json"
            expected_names.update((payload_name, metadata_name))
            payload = members[payload_name]
            _payload(payload, row["expected_payload"])
            _metadata(parse(members[metadata_name]), native)
            require(native["sha256"] == digest(payload), "native digest differs from payload")
            outcome.update(disposition="verified", useful_bytes=len(payload), payload_sha256=digest(payload))
        require(set(members) == expected_names, "unexpected/missing archive member")
        stats = parse(read_file(shard.with_name(shard.stem + "_stats.json")))
        for field, value in {
            "count": frame.height,
            "successes": statuses["success"],
            "failed_to_download": statuses["failed_to_download"],
            "failed_to_resize": statuses["failed_to_resize"],
        }.items():
            require(type(stats[field]) is int and stats[field] == value, "native shard stats mismatch")
        stats_records.append(stats)
    require({p.name for p in output.glob("*.tar")} == expected_tars, "unexpected/missing shard archive")
    require(
        {p.stem.removesuffix("_stats") for p in output.glob("*_stats.json")} == {p.stem for p in shards},
        "unexpected/missing shard stats",
    )
    return {"native_rows": len(seen), "archives": sorted(expected_tars), "native_stats": stats_records}


def verify_native(tool, output, truth):
    """Repeated offline verification is pure; no native reports are rewritten."""
    require(tool in ("flowdc", "img2dataset"), "unsupported native tool")
    outcomes = initial_outcomes(truth)
    details, errors = {}, []
    try:
        validate_partition(truth)
        details = (_flowdc if tool == "flowdc" else _img2dataset)(Path(output), truth, outcomes)
    except (ValueError, OSError, KeyError, TypeError, tarfile.TarError, pl.exceptions.PolarsError) as exc:
        errors.append(f"{type(exc).__name__}: {exc}")
        for row in outcomes.values():
            if row["disposition"] == "verified":
                row.update(disposition="invalid_artifact", useful_bytes=0, error=errors[-1])
    return {
        "rows": list(outcomes.values()),
        "artifacts_valid": not errors,
        "errors": errors,
        "native_details": details,
    }
