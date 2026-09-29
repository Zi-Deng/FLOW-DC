"""Independent verification of bounded native FLOW-DC/img2dataset artifacts.

No native success counter or filename extension grants useful-byte credit. A
structurally invalid required artifact earns no credit; native files are untouched.
"""

import io
import re
import stat
import tarfile
from collections import Counter
from pathlib import Path

import polars as pl

from .truth import (
    MAX_BYTES,
    MAX_ROWS,
    PROVENANCE,
    digest,
    encode,
    initial_outcomes,
    parse,
    require,
    validate_partition,
)

MAX_ARTIFACT_BYTES = 128 * 1024 * 1024


def read_file(path):
    path = Path(path).absolute()
    for parent in (*reversed(path.parents), path):
        require(not parent.is_symlink(), f"symlink artifact: {parent}")
    info = path.stat()
    require(stat.S_ISREG(info.st_mode) and info.st_size <= MAX_ARTIFACT_BYTES, "invalid/oversized artifact")
    return path.read_bytes()


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


def archive_members(path):
    """Reject compressed, unclosed, concatenated, duplicate and nonregular tars."""
    raw = read_file(path)
    members, end = {}, 0
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:") as archive:
        for item in archive:
            safe_name(item.name)
            require(
                item.isreg() and not item.issparse() and item.name not in members,
                "duplicate or nonregular archive member",
            )
            require(
                0 <= item.size <= MAX_BYTES and len(members) < 2 * MAX_ROWS + 2,
                "oversized archive member/count",
            )
            with archive.extractfile(item) as stream:
                content = stream.read()
            require(len(content) == item.size, "truncated archive member")
            members[item.name] = content
            end = item.offset_data + ((item.size + 511) // 512) * 512
    require(
        len(raw) % 512 == 0 and len(raw) - end >= 1024 and not any(raw[end:]),
        "archive lacks closure or contains trailing data",
    )
    return members


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
    index_raw = read_file(output / "outcome-index.json")
    index = parse(index_raw)
    final = parse(read_file(output / ".flowdc/final.json"))
    require(index["schema_version"] == 2 and final["schema_version"] == 2, "unsupported native schema")
    require(final["completion_boundary"] == "verified_uncompressed_archive", "incorrect native boundary")
    require(final["outcome_index_sha256"] == digest(index_raw), "native index binding mismatch")
    # The native product appends .tar; do not replace dots in a caller's basename.
    tar_path = output.parent / (output.name + ".tar")
    archive_raw = read_file(tar_path)
    require(
        final["archive"]["sha256"] == digest(archive_raw) and final["archive"]["bytes"] == len(archive_raw),
        "native archive binding mismatch",
    )
    members = archive_members(tar_path)
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
    require(output.is_dir() and not output.is_symlink(), "missing/unsafe native output")
    shards = sorted(output.glob("*.parquet"))
    require(bool(shards), "missing native parquet outcome sidecars")
    require(len(shards) <= MAX_ROWS, "too many native shards")
    expected = {row["row_id"]: row for row in truth["rows"] if row["eligible"]}
    seen, native_keys, expected_tars, stats_records = set(), set(), set(), []
    for shard in shards:
        require(re.fullmatch(r"[0-9]+", shard.stem), "invalid shard name")
        frame = pl.read_parquet(io.BytesIO(read_file(shard)))
        require(frame.height <= MAX_ROWS, "oversized shard sidecar")
        tar_path = shard.with_suffix(".tar")
        expected_tars.add(tar_path.name)
        members = archive_members(tar_path)
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
