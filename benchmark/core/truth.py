"""Bounded original-row contract, independent of either downloader's counters.

Version 1 deliberately accepts only a restricted metadata schema. It does not
repair or reinterpret the general FLOW-DC typed-metadata path (issue #25).
"""

import hashlib
import io
import json
import math
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

import polars as pl

SCHEMA = "flowdc-known-truth-v1"
MAX_ROWS = 256
MAX_BYTES = 64 * 1024 * 1024
PROVENANCE = (
    "__flowdc_manifest__",
    "__flowdc_position__",
    "__flowdc_source_rows__",
    "__flowdc_row_id__",
    "__flowdc_row_digest__",
)
NATIVE_RESERVED = {
    "key",
    "caption",
    "width",
    "height",
    "original_width",
    "original_height",
    "status",
    "error_message",
    "exif",
    "md5",
    "sha256",
    "sha512",
}


def encode(value):
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False) + "\n"
    ).encode()


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def parse(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, "duplicate JSON key")
            result[key] = value
        return result

    def nonfinite(_):
        raise ValueError("nonfinite JSON number")

    return json.loads(raw, object_pairs_hook=pairs, parse_constant=nonfinite)


def valid_url(value):
    # Do not normalize the original metadata. Whitespace-bearing URLs are common
    # skipped rows for both native tools, including leading/trailing whitespace.
    if not isinstance(value, str) or not value or any(c.isspace() for c in value):
        return False
    try:
        parsed = urlsplit(value)
        return (
            parsed.scheme in ("http", "https")
            and bool(parsed.hostname)
            and parsed.port != 0
            and parsed.username is None
            and parsed.password is None
        )
    except ValueError:
        return False


def validate_metadata(frame):
    require(
        "url" in frame.columns and frame.schema["url"] == pl.String,
        "known-truth input requires a String column named url",
    )
    require(0 < frame.height <= MAX_ROWS, "fixture must contain 1..256 original rows")
    for name, dtype in frame.schema.items():
        require(
            re.fullmatch(r"[a-zA-Z][a-zA-Z0-9_]*", name) is not None and name not in NATIVE_RESERVED,
            f"reserved/unsupported metadata column: {name}",
        )
        require(
            dtype in (pl.String, pl.Boolean, pl.Int64, pl.Float64, pl.Null),
            f"unsupported research metadata dtype: {name}: {dtype}",
        )
    for row in frame.iter_rows(named=True):
        for name, value in row.items():
            if type(value) is int:
                require(abs(value) <= 2**53 - 1, f"unsafe JSON integer: {name}")
            if type(value) is float:
                require(math.isfinite(value), f"nonfinite metadata: {name}")
        require(len(encode(row)) <= 64 * 1024, "row metadata exceeds 64 KiB")


@dataclass(frozen=True)
class Truth:
    raw_manifest: bytes
    frame: pl.DataFrame
    record: dict

    @classmethod
    def load(cls, manifest, catalog):
        """Validate everything in memory, before creating output or starting HTTP.

        Catalog values are predeclared {bytes, sha256} or null for objects with no
        successful payload. The catalog is supplied independently of native output.
        """
        raw = Path(manifest).read_bytes()
        frame = pl.read_parquet(io.BytesIO(raw))
        validate_metadata(frame)
        require(isinstance(catalog, dict), "catalog must be an object")
        for url, spec in catalog.items():
            require(valid_url(url), "invalid catalog URL")
            if spec is not None:
                require(isinstance(spec, dict) and set(spec) == {"bytes", "sha256"}, "invalid object truth")
                require(
                    type(spec["bytes"]) is int and 0 < spec["bytes"] <= MAX_BYTES,
                    "expected payload must have positive bounded integer length",
                )
                require(
                    isinstance(spec["sha256"], str) and re.fullmatch(r"[0-9a-f]{64}", spec["sha256"]),
                    "invalid expected payload hash",
                )
        source = digest(raw)
        rows = []
        for position, metadata in enumerate(frame.iter_rows(named=True)):
            url = metadata["url"]
            eligible = valid_url(url)
            require(not eligible or url in catalog, "eligible URL absent from independent catalog")
            rows.append(
                {
                    "row_id": digest(encode([source, position])),
                    "position": position,
                    "metadata": metadata,
                    "eligible": eligible,
                    "expected_payload": catalog.get(url) if eligible else None,
                }
            )
        require(
            sum((row["expected_payload"] or {}).get("bytes", 0) for row in rows) <= MAX_BYTES,
            "fixture exceeds 64 MiB expected row payload",
        )
        record = {
            "schema": SCHEMA,
            "manifest_sha256": source,
            "original_rows": frame.height,
            "metadata_schema": {name: str(dtype) for name, dtype in frame.schema.items()},
            "catalog": catalog,
            "catalog_sha256": digest(encode(catalog)),
            "rows": rows,
        }
        return cls(raw, frame, record)

    def write(self, directory):
        """Create a new retained fixture; refuse collisions without deleting anything."""
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=False)
        (directory / "original.parquet").write_bytes(self.raw_manifest)
        (directory / "truth.json").write_bytes(encode(self.record))
        source, rows = self.record["manifest_sha256"], self.record["rows"]
        frame = self.frame.with_columns(
            pl.lit(source).alias(PROVENANCE[0]),
            pl.Series(PROVENANCE[1], [row["position"] for row in rows], dtype=pl.Int64),
            pl.lit(len(rows), dtype=pl.Int64).alias(PROVENANCE[2]),
            pl.Series(PROVENANCE[3], [row["row_id"] for row in rows]),
            pl.Series(PROVENANCE[4], [digest(encode(row["metadata"])) for row in rows]),
        ).filter(pl.Series([row["eligible"] for row in rows]))
        frame.write_parquet(directory / "eligible.parquet")
        return directory / "eligible.parquet"


def initial_outcomes(truth):
    return {
        row["row_id"]: {
            "row_id": row["row_id"],
            "position": row["position"],
            "disposition": "missing" if row["eligible"] else "skipped",
            "useful_bytes": 0,
            "error": None if row["eligible"] else "invalid URL",
        }
        for row in truth["rows"]
    }


def partition_truth(parent, identifiers):
    """An explicitly scoped view; the original scientific denominator never changes.

    Retain the full independent parent catalog and row membership, not a native
    output-derived denominator. Parent order is authoritative for reconciliation.
    """
    require(parent.get("schema") == SCHEMA and "scope" not in parent, "invalid partition parent")
    require(
        type(parent["original_rows"]) is int
        and 1 <= parent["original_rows"] <= MAX_ROWS
        and len(parent["rows"]) == parent["original_rows"],
        "invalid parent denominator",
    )
    require(parent["catalog_sha256"] == digest(encode(parent["catalog"])), "parent catalog mismatch")
    for position, row in enumerate(parent["rows"]):
        require(
            row["position"] == position
            and row["row_id"] == digest(encode([parent["manifest_sha256"], position]))
            and row["eligible"] == valid_url(row["metadata"]["url"])
            and row["expected_payload"] == parent["catalog"].get(row["metadata"]["url"]),
            "invalid parent row identity/truth",
        )
    identifiers = list(identifiers)
    require(bool(identifiers) and len(set(identifiers)) == len(identifiers), "empty/duplicate partition rows")
    rows = [row for row in parent["rows"] if row["row_id"] in identifiers]
    require(
        len(rows) == len(identifiers) and all(row["eligible"] for row in rows),
        "partition contains unexpected/ineligible logical rows",
    )
    # A serialization copy prevents later caller mutation from changing the view.
    return parse(
        encode(
            {
                **parent,
                "rows": rows,
                "scope": "partition_only",
                "partition_rows": len(rows),
                "parent_truth_sha256": digest(encode(parent)),
                "parent_truth": parent,
            }
        )
    )


def validate_partition(truth):
    if "scope" not in truth:
        require("parent_truth" not in truth and "partition_rows" not in truth, "unscoped partition")
        return
    expected = partition_truth(truth["parent_truth"], [row["row_id"] for row in truth["rows"]])
    require(encode(expected) == encode(truth), "partition parent/membership binding mismatch")
