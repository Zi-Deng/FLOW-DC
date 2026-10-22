"""Verify retained public bytes; no assertion of independently known remote truth."""

import json
import sys
from pathlib import Path

import polars as pl

from benchmark.core.verifier import archive_members, file_digest

from .common import decode_image, read_json, require, sha256


def verify(directory, source, method, started, ended):
    directory, source = Path(directory), Path(source)
    frame = pl.read_parquet(directory / "input.parquet")
    expected = {r["study_row_id"]: r for r in frame.to_dicts()}
    payloads, archives = {}, {}
    sys.path.insert(0, str(source / "bin"))
    from flowdc_research_profile import EXTERNAL

    if method != "img2dataset-1.47.0":
        from .verify import verify as native_verify

        native = native_verify(
            directory, directory / "input.parquet", source, started, ended, credit_urls=True
        )
        index = read_json(directory / "native/outcome-index.json")
        for row in index["rows"]:
            if row["disposition"] != "verified":
                continue
            original = frame.row(row["source_position"], named=True)
            if original["url"] in native["credited_url_bytes"]:
                payloads[original["study_row_id"]] = dict(
                    url=original["url"], bytes=row["payload_bytes"], sha256=row["payload_sha256"]
                )
        archives["native.tar"] = dict(
            bytes=(directory / "native.tar").stat().st_size, sha256=native["archive_sha256"]
        )
        return dict(
            credited_bytes=native["credited_bytes"],
            credited_rows=native["credited_rows"],
            original_rows=frame.height,
            coverage=native["credited_rows"] / frame.height,
            full_success=native["native_run_complete"] and native["decode_failures"] == 0,
            decode_failures=native["decode_failures"],
            payloads=payloads,
            archives=archives,
            remote_truth="Retained-byte integrity and decode; no independent publisher byte truth",
        )
    seen, failures, decode_failures = set(), 0, 0
    metadata_files = sorted((directory / "native").glob("*.parquet"))
    require(metadata_files, "missing tool row ledger")
    ledger = pl.concat([pl.read_parquet(p) for p in metadata_files]).to_dicts()
    require(
        len(ledger) == len(expected) and {r["study_row_id"] for r in ledger} == set(expected),
        "tool original denominator incomplete",
    )
    for item in ledger:
        require(
            all(item.get(k) == v for k, v in expected[item["study_row_id"]].items()),
            "tool ledger original metadata changed",
        )
        require(
            item["status"] in ("success", "failed_to_download", "failed_to_resize"), "unknown tool row status"
        )
        if item["status"] != "success":
            failures += 1
            seen.add(item["study_row_id"])
    successful = {r["study_row_id"]: r for r in ledger if r["status"] == "success"}
    for archive in sorted((directory / "native").glob("*.tar")):
        digest, size = file_digest(archive, EXTERNAL.max_artifact_bytes)
        archives[str(archive.relative_to(directory))] = dict(bytes=size, sha256=digest)
        members = archive_members(archive, EXTERNAL)
        wanted = set()
        for name in members:
            if not name.endswith(".json"):
                continue
            item = json.loads(members[name])
            row_id = item.get("study_row_id")
            require(row_id in successful and row_id not in seen, "unknown/duplicate tool row")
            row = expected[row_id]
            require(all(item.get(k) == v for k, v in row.items()), "tool original row metadata changed")
            require(item == successful[row_id], "tool archive/ledger mismatch")
            seen.add(row_id)
            wanted.add(name)
            require(name == str(item["key"]) + ".json", "tool key/metadata mismatch")
            status = item.get("status")
            require(
                status in ("success", "failed_to_download", "failed_to_resize"), "unknown tool row status"
            )
            if status != "success":
                failures += 1
                continue
            payload_name = str(item["key"]) + ".jpg"
            wanted.add(payload_name)
            raw = members[payload_name]
            require(
                0 < len(raw) <= EXTERNAL.max_object_bytes and item.get("sha256") == sha256(raw),
                "tool payload digest mismatch",
            )
            try:
                decode_image(raw)
            except Exception:
                decode_failures += 1
                continue
            payloads[row_id] = dict(url=row["url"], bytes=len(raw), sha256=sha256(raw))
        require(set(members) == wanted, "orphan/missing tool archive member")
    require(archives and seen == set(expected), "tool original denominator incomplete")
    return dict(
        credited_bytes=sum(p["bytes"] for p in payloads.values()),
        credited_rows=len(payloads),
        original_rows=frame.height,
        coverage=len(payloads) / frame.height,
        full_success=failures == 0 and decode_failures == 0,
        decode_failures=decode_failures,
        payloads=payloads,
        archives=archives,
        remote_truth="Retained-byte integrity and decode; no independent publisher byte truth",
    )


def main():
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--directory", type=Path, required=True)
    p.add_argument("--source", type=Path, required=True)
    p.add_argument("--method", required=True)
    p.add_argument("--started", type=float, required=True)
    p.add_argument("--ended", type=float, required=True)
    a = p.parse_args()
    print(json.dumps(verify(a.directory, a.source, a.method, a.started, a.ended), allow_nan=False))


if __name__ == "__main__":
    main()
