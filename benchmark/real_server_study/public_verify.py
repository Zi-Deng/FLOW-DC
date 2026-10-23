"""Verify retained public bytes; no assertion of independently known remote truth."""

import json
import sys
import tarfile
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
    from flowdc_research_profile import EXTERNAL, workload

    # Historical pilots keep their original limits. Long jobs bind the complete
    # finite profile in attempt.json, independently of the native output report.
    attempt = read_json(directory / "attempt.json") if (directory / "attempt.json").exists() else {}
    limits = workload(attempt["workload"]) if "workload" in attempt else EXTERNAL

    if method != "img2dataset-1.47.0":
        from .verify import verify as native_verify

        native = native_verify(
            directory, directory / "input.parquet", source, started, ended, credit_urls=True,
            workload_name=limits.name,
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
        digest, size = file_digest(archive, limits.max_artifact_bytes)
        archives[str(archive.relative_to(directory))] = dict(bytes=size, sha256=digest)
        members = archive_members(archive, limits)
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
                0 < len(raw) <= limits.max_object_bytes and item.get("sha256") == sha256(raw),
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



def verify_partial(directory, source, method):
    """Credit only sealed, independently checked row outputs after a stop.

    No reconciliation writes, invented dispositions or normal-completion claim.
    Original rows without verifiable bytes remain uncredited, including rows
    never dispatched and shards whose publication was interrupted.
    """
    from .common import encode, file_hash
    directory, source = Path(directory), Path(source)
    frame = pl.read_parquet(directory / "input.parquet")
    expected = {r["study_row_id"]: r for r in frame.to_dicts()}
    require(len(expected) == frame.height, "duplicate original row identity")
    payloads, artifacts, incomplete = {}, {}, []
    sys.path.insert(0, str(source / "bin"))
    from flowdc_research_profile import workload
    limits = workload(read_json(directory / "attempt.json")["workload"])

    def safe(path):
        require(path.is_relative_to(directory) and not any(p.is_symlink() for p in (path, *path.parents)
                if p.is_relative_to(directory)), "unsafe partial artifact")
        return path

    def retained(path):
        safe(path)
        digest, count = file_digest(path, limits.max_artifact_bytes)
        artifacts[str(path.relative_to(directory))] = dict(sha256=digest, bytes=count)

    if method != "img2dataset-1.47.0":
        native = directory / "native"
        owner_path = native / ".flowdc/owner.json"
        if owner_path.exists():
            owner = read_json(safe(owner_path))
            manifest = file_hash(directory / "input.parquet")
            require(owner["schema_version"] == 2 and owner["manifest"]["sha256"] == manifest
                    and owner["manifest"]["original_rows"] == frame.height, "partial ownership differs")
            cfg = owner["config"]["values"]
            require(cfg["control_method"] == method and cfg["research_workload"] == limits.name
                    and cfg["max_retry_attempts"] == 1, "partial method/profile differs")
            sources = owner["config"]["source_sha256"]
            require(set(sources) == {"download_batch.py", "single_download.py", "flowdc_integrity.py",
                                    "flowdc_methods.py", "flowdc_research_profile.py"}
                    and all(file_hash(source / "bin" / name) == digest for name, digest in sources.items()),
                    "partial source provenance differs")
            require(len(owner["rows"]) == frame.height, "partial owner denominator differs")
            retained(owner_path)
            for position, original in enumerate(frame.to_dicts()):
                row_id = sha256(encode([manifest, position]))
                owned = owner["rows"][position]
                require(owned["row_id"] == row_id and owned["source_position"] == position
                        and owned["row"] == original and owned["source_manifest"] == manifest,
                        "partial original metadata differs")
                commit_path = native / ".flowdc/commits" / (row_id + ".json")
                if not commit_path.exists():
                    continue
                commit = read_json(safe(commit_path))
                require(commit["run_id"] == owner["run_id"] and commit["row_id"] == row_id
                        and type(commit["attempt"]) is int and commit["attempt"] == 1,
                        "partial commit identity differs")
                attempt = native / ".flowdc/attempts" / row_id / str(commit["attempt"])
                intent = read_json(safe(attempt / "intent.json"))
                require(intent == {k: commit[k] for k in ("run_id", "row_id", "attempt")}, "partial intent differs")
                require(read_json(safe(attempt / "ready.json")) == commit, "partial ready/commit mismatch")
                raw_path, metadata_path = safe(attempt / "payload"), safe(attempt / "metadata")
                require(raw_path.stat().st_size <= limits.max_object_bytes, "partial object exceeds limit")
                raw, metadata = raw_path.read_bytes(), metadata_path.read_bytes()
                require(len(raw) == commit["payload_bytes"] and sha256(raw) == commit["payload_sha256"]
                        and len(metadata) == commit["metadata_bytes"] and sha256(metadata) == commit["metadata_sha256"],
                        "partial committed digest mismatch")
                info = json.loads(metadata)
                require(info["row"] == original and info["source_manifest"] == manifest
                        and info["source_position"] == position and info["source_rows"] == frame.height
                        and info["row_id"] == row_id and info["payload_sha256"] == sha256(raw)
                        and info["payload_bytes"] == len(raw) and info["url"] == original["url"],
                        "partial committed metadata mismatch")
                try:
                    decode_image(raw)
                except Exception:
                    incomplete.append(str(raw_path.relative_to(directory)))
                    continue
                payloads[original["study_row_id"]] = dict(url=original["url"], bytes=len(raw), sha256=sha256(raw))
                for path in (commit_path, attempt / "intent.json", attempt / "ready.json", metadata_path, raw_path):
                    retained(path)
    else:
        seen = set()
        for ledger in sorted((directory / "native").glob("*.parquet")):
            archive = ledger.with_suffix(".tar")
            if not archive.exists():
                incomplete.append(str(ledger.relative_to(directory)))
                continue
            try:
                rows = pl.read_parquet(safe(ledger)).to_dicts()
                with safe(archive).open("rb") as stream:
                    stream.seek(max(0, archive.stat().st_size - 1024))
                    require(stream.read() == bytes(1024), "partial tar lacks closing blocks")
                members = archive_members(archive, limits)
            except (OSError, ValueError, tarfile.TarError, pl.exceptions.PolarsError):
                incomplete.append(str(archive.relative_to(directory)))
                continue
            retained(ledger)
            retained(archive)
            for row in rows:
                key = row.get("study_row_id")
                require(key in expected and all(row.get(k) == v for k, v in expected[key].items()),
                        "partial tool metadata differs")
                require(key not in seen and row.get("status") in ("success", "failed_to_download", "failed_to_resize"),
                        "duplicate/unknown partial tool disposition")
                seen.add(key)
                if row.get("status") != "success":
                    continue
                require(key not in payloads, "duplicate partial tool credit")
                stem = str(row["key"])
                info = json.loads(members[stem + ".json"])
                require(info == row, "partial tool archive/ledger mismatch")
                raw = members[stem + ".jpg"]
                require(0 < len(raw) <= limits.max_object_bytes and sha256(raw) == row["sha256"],
                        "partial tool payload digest mismatch")
                try:
                    decode_image(raw)
                except Exception:
                    incomplete.append(stem)
                    continue
                payloads[key] = dict(url=expected[key]["url"], bytes=len(raw), sha256=sha256(raw))
    return dict(credited_bytes=sum(p["bytes"] for p in payloads.values()), credited_rows=len(payloads),
                original_rows=frame.height, coverage=len(payloads) / frame.height,
                full_success=False, partial=True, uncredited_rows=frame.height - len(payloads),
                payloads=payloads, archives=artifacts, incomplete_artifacts=incomplete,
                remote_truth="Retained committed/sealed bytes only; no remote truth or completion inferred")

def main():
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--directory", type=Path, required=True)
    p.add_argument("--source", type=Path, required=True)
    p.add_argument("--method", required=True)
    p.add_argument("--started", type=float, required=True)
    p.add_argument("--ended", type=float, required=True)
    p.add_argument("--partial", action="store_true")
    a = p.parse_args()
    result = verify_partial(a.directory, a.source, a.method) if a.partial else verify(a.directory, a.source, a.method, a.started, a.ended)
    print(json.dumps(result, allow_nan=False))


if __name__ == "__main__":
    main()
