"""Bounded archive inspection and content evidence; never extract downloaded tar files."""

import gzip
import io
import sys
import tarfile
from collections import Counter
from pathlib import PurePosixPath

from flowdc_experiment_data import (
    ARCHIVE_MEMBER_LIMIT,
    LIMIT,
    ExperimentError,
    digest,
    encode,
    parse,
    require,
)
from flowdc_integrity import row_identity, verify_archive


def members(raw, maximum, count=ARCHIVE_MEMBER_LIMIT):
    require(len(raw) <= maximum, "artifact_size_limit")
    if raw.startswith(b"\x1f\x8b"):
        with gzip.GzipFile(fileobj=io.BytesIO(raw)) as compressed:
            raw = compressed.read(maximum + 1)
        require(len(raw) <= maximum, "artifact_size_limit")
    result = {}
    total = 0
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:") as archive:
        for index, item in enumerate(archive):
            path = PurePosixPath(item.name)
            require(index < count and item.name not in result, "archive_member_limit_or_duplicate")
            require(
                not path.is_absolute()
                and ".." not in path.parts
                and path.parts
                and not any(c in item.name for c in ("\\", "\x00")),
                "unsafe_archive_path",
            )
            require(item.isfile() or item.isdir(), "unsafe_archive_type")
            total += item.size
            require(0 <= item.size <= maximum and total <= maximum, "artifact_size_limit")
            if item.isdir():
                result[item.name] = None
                continue
            stream = archive.extractfile(item)
            content = stream.read(item.size + 1)
            require(len(content) == item.size, "artifact_truncated")
            result[item.name] = content
    return result


def bundle(files):
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w") as archive:
        for path, content in sorted(files.items()):
            item = tarfile.TarInfo(path)
            item.size, item.mode, item.mtime = len(content), 0o600, 0
            archive.addfile(item, io.BytesIO(content))
    return output.getvalue()


def validate_case(raw, case, partitions, maximum):
    files = members(raw, maximum)
    require(files.get("tasks.json") is not None, "task_evidence_missing")
    tasks = parse(files["tasks.json"])
    require(
        tasks.get("submitted") == len(partitions) and len(tasks.get("tasks", [])) == len(partitions),
        "task_count_mismatch",
    )
    require(
        all(
            t.get("successful") is True and t.get("exit_code") == 0 and t.get("log_truncated") is False
            for t in tasks["tasks"]
        ),
        "task_failed",
    )
    require(len({t.get("id") for t in tasks["tasks"]}) == len(partitions), "duplicate_task")
    require(files.get("resolved-config.json") is not None, "resolved_config_missing")
    require(
        all(files.get(f"task-{task['id']}.log") is not None for task in tasks["tasks"]), "task_log_missing"
    )
    resolved = parse(files["resolved-config.json"])
    require(resolved.get("enable_paarc") is case["config"]["enable_paarc"], "paarc_mode_mismatch")
    reports = []
    for part in partitions:
        filename = "output_" + part["name"].removesuffix(".parquet") + ".tar.gz"
        require(files.get(filename) is not None, "partition_output_missing")
        # Schema 2 adds one metadata member per row plus an outcome index.
        content = members(files[filename], maximum, count=2 * ARCHIVE_MEMBER_LIMIT)
        overviews = [value for key, value in content.items() if PurePosixPath(key).name == "overview.json"]
        require(len(overviews) == 1 and overviews[0] is not None, "overview_missing")
        overview = parse(overviews[0])
        summary = overview.get("summary", {})
        require(
            summary.get("total_urls") == part["rows"]
            and summary.get("successful_downloads") == part["rows"]
            and summary.get("failed_downloads") == 0
            and summary.get("shutdown_requested") is False,
            "downloader_incomplete",
        )
        require(
            overview.get("script_inputs", {}).get("enable_paarc") is case["config"]["enable_paarc"],
            "paarc_mode_mismatch",
        )
        if overview.get("report_schema_version") == 2:
            indices = [
                (key, value)
                for key, value in content.items()
                if PurePosixPath(key).name == "outcome-index.json"
            ]
            require(len(indices) == 1 and indices[0][1] is not None, "outcome_index_missing")
            index_name, index_raw = indices[0]
            index = parse(index_raw)
            rows = index.get("rows", [])
            require(
                index.get("manifest", {}).get("sha256") == part.get("manifest_sha256"),
                "partition_manifest_mismatch",
            )
            if "row_ids" in part:
                require(
                    sorted(row.get("row_id", "") for row in rows) == sorted(part["row_ids"]),
                    "partition_identity_mismatch",
                )
            require(
                index.get("schema_version") == 2
                and index.get("original_rows") == part["rows"]
                and len(rows) == part["rows"],
                "outcome_denominator_mismatch",
            )
            require(all(row.get("disposition") == "verified" for row in rows), "unverified_outcome")
            require(len({row.get("row_id") for row in rows}) == len(rows), "duplicate_row_identity")
            require(
                index.get("counts") == {"verified": len(rows), "failed": 0, "skipped": 0, "unattempted": 0},
                "outcome_count_mismatch",
            )
            prefix = str(PurePosixPath(index_name).parent)
            try:
                verify_archive(files[filename], index, prefix)
            except Exception:
                require(False, "archive_integrity_mismatch")
            hashes = []
            for row in rows:
                metadata = parse(content[prefix + "/" + row["metadata"]])
                require(
                    metadata.get("row_id")
                    == row_identity(metadata.get("source_manifest"), metadata.get("source_position")),
                    "row_provenance_mismatch",
                )
                require(
                    metadata.get("source_manifest") == row.get("source_manifest")
                    and metadata.get("source_position") == row.get("source_position"),
                    "row_provenance_mismatch",
                )
                hashes.append(digest(content[prefix + "/" + row["payload"]]))
            require(
                sum(row["payload_bytes"] for row in rows)
                == index.get("verified_payload_bytes")
                == summary.get("verified_payload_bytes"),
                "payload_byte_mismatch",
            )
        else:
            # Preserve historical schema-1 interpretation and member bounds.
            require(len(content) <= ARCHIVE_MEMBER_LIMIT, "archive_member_limit_or_duplicate")
            hashes = [
                digest(value)
                for key, value in content.items()
                if value is not None and PurePosixPath(key).name != "overview.json"
            ]
        require(len(hashes) == part["rows"], "image_count_mismatch")
        if "expected_sha256" in part:
            require(Counter(hashes) == Counter(part["expected_sha256"]), "image_hash_mismatch")
        reports.append(
            {
                "partition": part["name"],
                "archive_sha256": digest(files[filename]),
                "observed_sha256": sorted(hashes),
                "expected_hashes_verified": "expected_sha256" in part,
                "overview": overview,
            }
        )
    return reports


def validate_origin(raw, cases, worker, maximum):
    logs = members(raw, maximum)
    require(logs.get("origin.jsonl") is not None, "origin_evidence_missing")
    rows = [parse(line) for line in logs["origin.jsonl"].splitlines()]
    workers = [worker] if isinstance(worker, str) else worker
    require(
        isinstance(workers, list) and len(workers) in (1, 2, 4) and len(set(workers)) == len(workers),
        "invalid_worker_sources",
    )
    require(all(row.get("source") in workers for row in rows), "origin_source_mismatch")
    for case in cases:
        prefix = "/" + case["name"] + "/"
        selected = [row for row in rows if row.get("path", "").startswith(prefix)]
        require(len(selected) == 65, "fixture_request_count_mismatch")
        for index in range(64):
            statuses = [row["status"] for row in selected if row["path"] == f"{prefix}{index}.png"]
            require(statuses == ([503, 200] if index == 0 else [200]), "fixture_retry_mismatch")
    require(len(rows) == 130, "fixture_request_count_mismatch")
    return {"requests": len(rows), "injected_failures_recovered": 2}


def main():
    # Validation is a separate bounded child so decompression/hash work cannot
    # indefinitely postpone the parent runner's cloud-stop path.
    header = sys.stdin.buffer.readline(LIMIT + 1)
    require(len(header) <= LIMIT and header.endswith(b"\n"), "validation_header_limit")
    value = parse(header)
    maximum = value["maximum"]
    require(type(maximum) is int and 0 < maximum <= 1073741824)
    raw = sys.stdin.buffer.read(maximum + 1)
    require(len(raw) <= maximum, "artifact_size_limit")
    if value["kind"] == "distributed":
        from flowdc_experiment_research import verify_return

        result = verify_return(
            members(raw, maximum),
            value["case"],
            value["truth"],
            value["source"],
            value["environment"],
            value["cohort"],
        )
    elif value["kind"] == "controlled-origin":
        files = members(raw, maximum)
        rows = [parse(line) for line in files["origin.jsonl"].splitlines()]
        require(
            bool(rows) and all(row["sequence"] == i for i, row in enumerate(rows, 1)),
            "origin_sequence_mismatch",
        )
        arrivals = {row["request_id"] for row in rows if row["phase"] == "arrival"}
        responses = {row["request_id"] for row in rows if row["phase"] == "response"}
        require(arrivals == responses, "origin_unclosed_requests")
        result = {"requests": len(arrivals), "scope": "origin clock only; no cross-machine subtraction"}
    elif value["kind"] == "case":
        result = validate_case(raw, value["case"], value["partitions"], maximum)
    elif value["kind"] == "origin":
        result = validate_origin(raw, value["cases"], value["worker"], maximum)
    else:
        require(value["kind"] == "worker")
        role = value.get("role", "worker")
        require(role in ("worker", "worker-2", "worker-3", "worker-4"), "invalid_worker_role")
        files = members(raw, maximum)
        require(files.get(role + ".log") is not None, "worker_log_missing")
        if "cohort" in value:
            from flowdc_experiment_research import verify_worker_launch

            verify_worker_launch(
                parse(files["owned-worker.json"]), value["cohort"], role, value["case"], value["slot"]
            )
        result = {"worker_log_present": True}
    sys.stdout.buffer.write(encode({"result": result}))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        code = str(exc) if isinstance(exc, ExperimentError) else "artifact_validation_failed"
        sys.stdout.buffer.write(encode({"error": code}))
        sys.exit(1)
