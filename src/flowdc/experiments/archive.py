"""Independent tool-ledger and archive validation, with common byte accounting."""

from __future__ import annotations

import io
import tarfile
from pathlib import Path
from uuid import uuid4

import polars as pl

from .. import integrity
from .common import require


def seal_tool(original, tool_input, tool_output, destination):
    from PIL import Image

    expected = {r["study_row_id"]: r for r in tool_input.iter_rows(named=True)}
    native_rows = {r["__key__"]: r for r in original.iter_rows(named=True)}
    ledgers = sorted(tool_output.glob("*.parquet"))
    require(ledgers, "comparator row ledger missing")
    ledger = pl.concat([pl.read_parquet(p) for p in ledgers]).to_dicts()
    require(
        len(ledger) == len(expected) and {r["study_row_id"] for r in ledger} == set(expected),
        "comparator original-row denominator differs",
    )
    by_id = {r["study_row_id"]: r for r in ledger}
    for row in ledger:
        require(
            all(row.get(k) == v for k, v in expected[row["study_row_id"]].items()),
            "comparator original metadata differs",
        )
        require(
            row["status"] in ["success", "failed_to_download", "failed_to_resize"],
            "unknown comparator disposition",
        )
    destination.mkdir(parents=True, exist_ok=True)
    rows = {}
    seen = set()
    with tarfile.open(destination / "native.tar", "x", format=tarfile.PAX_FORMAT) as final:
        for source in sorted(tool_output.glob("*.tar")):
            with tarfile.open(source, "r:") as archive:
                members = archive.getmembers()
                names = {m.name: m for m in members}
                require(
                    len(names) == len(members)
                    and all(
                        m.isfile() and not m.name.startswith("/") and ".." not in Path(m.name).parts
                        for m in members
                    ),
                    "unsafe or duplicate comparator archive member",
                )
                wanted = set()
                for member in members:
                    if not member.name.endswith(".json"):
                        continue
                    require(member.size <= 1024 * 1024, "tool metadata is oversized")
                    metadata = integrity.parse(archive.extractfile(member).read())
                    key = metadata.get("study_row_id")
                    require(
                        key in by_id and key not in seen and metadata == by_id[key],
                        "tool archive and ledger differ",
                    )
                    require(member.name == str(metadata["key"]) + ".json", "tool archive key differs")
                    seen.add(key)
                    wanted.add(member.name)
                    if metadata["status"] != "success":
                        continue
                    image_name = str(metadata["key"]) + ".jpg"
                    wanted.add(image_name)
                    require(
                        image_name in names and 0 < names[image_name].size <= 64 * 2**20,
                        "tool image missing or oversized",
                    )
                    body = archive.extractfile(names[image_name]).read()
                    require(integrity.digest(body) == metadata.get("sha256"), "tool image checksum differs")
                    with Image.open(io.BytesIO(body)) as image:
                        image.verify()
                    native = native_rows[key]
                    payload = key + ".jpg"
                    sidecar = key + ".json"
                    common = {
                        "schema_version": 2,
                        "key": key,
                        "row_id": key,
                        "url": native["url"],
                        "class_name": None,
                        "source_manifest": native["__flowdc_manifest__"],
                        "source_position": native["__flowdc_position__"],
                        "source_rows": native["__flowdc_source_rows__"],
                        "row": integrity.row_metadata(native),
                        "payload": payload,
                        "payload_bytes": len(body),
                        "payload_sha256": integrity.digest(body),
                    }
                    encoded = integrity.encode(common)
                    for name, data in [(payload, body), (sidecar, encoded)]:
                        info = tarfile.TarInfo("native/" + name)
                        info.size = len(data)
                        info.mode = 0o600
                        info.mtime = 0
                        final.addfile(info, io.BytesIO(data))
                    rows[key] = {
                        "row_id": key,
                        "source_manifest": common["source_manifest"],
                        "source_position": common["source_position"],
                        "disposition": "verified",
                        "payload": payload,
                        "metadata": sidecar,
                        "payload_bytes": len(body),
                        "payload_sha256": integrity.digest(body),
                        "metadata_bytes": len(encoded),
                        "metadata_sha256": integrity.digest(encoded),
                    }
                require(set(names) == wanted, "orphan or missing comparator archive member")
        require(
            all(r["status"] != "success" or r["study_row_id"] in seen for r in ledger),
            "successful tool row has no archive evidence",
        )
        for key, row in native_rows.items():
            if key not in rows:
                rows[key] = {
                    "row_id": key,
                    "source_manifest": row["__flowdc_manifest__"],
                    "source_position": row["__flowdc_position__"],
                    "disposition": "failed",
                    "payload": None,
                    "metadata": None,
                    "payload_bytes": 0,
                }
        counts = {
            k: sum(r["disposition"] == k for r in rows.values())
            for k in ["verified", "failed", "skipped", "unattempted"]
        }
        index = {
            "schema_version": 2,
            "run_id": uuid4().hex,
            "original_rows": len(rows),
            "counts": counts,
            "rows": list(rows.values()),
            "verified_payload_bytes": sum(r["payload_bytes"] for r in rows.values()),
        }
        overview = {"method": "img2dataset", "version": "1.47.0", "run_complete": counts["failed"] == 0}
        for name, value in [("outcome-index.json", index), ("overview.json", overview)]:
            raw = integrity.encode(value)
            (destination / name).write_bytes(raw)
            info = tarfile.TarInfo("native/" + name)
            info.size = len(raw)
            info.mode = 0o600
            info.mtime = 0
            final.addfile(info, io.BytesIO(raw))
    with (destination / "native.tar").open("rb") as stream:
        integrity.verify_archive(stream, index, "native")
    return overview
