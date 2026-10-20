"""Read-only verification of external acquisitions; no remote-byte truth claim."""
import argparse
import json
import math
import sys
from collections import Counter
from pathlib import Path

import polars as pl
from .common import decode_image, encode, file_hash, read_json, require, sha256


def observations(run_dir, started=None, ended=None, initial=4, strict=False):
    native = Path(run_dir) / "native"
    overview = read_json(native / "overview.json") if strict else None
    files = (overview["http_observations"]["files"] if strict else
             [{"path": str(p.relative_to(native))} for p in native.glob(".flowdc/attempts/*/*/measurement.json")])
    statuses, kinds, errors, reasons = Counter(), Counter(), Counter(), Counter()
    ttfb, body, limits = [], [], []
    observed_bytes, overload = 0, 0
    if strict:
        require(overview["http_observations"]["complete"] and not overview["http_observations"]["missing_attempts"],
                "missing HTTP observations")
        listed = {f["path"] for f in files}
        actual = {str(p.relative_to(native)) for p in native.glob(".flowdc/attempts/*/*/measurement.json")}
        require(listed == actual and len(listed) == len(files), "HTTP observation inventory mismatch")
    for spec in files:
        relative = Path(spec["path"])
        require(not relative.is_absolute() and ".." not in relative.parts, "unsafe observation path")
        path = native / relative
        if strict:
            require(not path.is_symlink() and path.stat().st_size == spec["bytes"] and file_hash(path) == spec["sha256"],
                    "HTTP observation hash mismatch")
        record = read_json(path)
        if strict:
            require(record["schema"] == "flowdc-http-observation-v1" and record["measurement_version"] == "4-output-independent-delay-signals",
                    "unsupported HTTP observation")
            require(record["run_id"] == overview["output_integrity"]["run_id"] and
                    record["row_id"] == path.parent.parent.name and record["attempt"] == int(path.parent.name),
                    "HTTP observation identity mismatch")
            require(read_json(path.with_name("intent.json")) == {k: record[k] for k in ("run_id", "row_id", "attempt")},
                    "attempt intent mismatch")
        observed_bytes += record.get("observed_response_body_bytes") or 0
        status, kind = record["status"], record["failure_kind"]
        statuses[str(status)] += 1
        kinds[str(kind)] += 1
        overload += bool(status in (408, 429) or (type(status) is int and status >= 500) or kind == "transport")
        if record["latency_eligible"]:
            require(status == 200 and all(type(record[k]) in (int, float) and math.isfinite(record[k])
                    and record[k] > 0 for k in ("ttfb", "body_delay")) and record["body_delay"] >= record["ttfb"],
                    "invalid eligible HTTP measurement")
            ttfb.append(record["ttfb"])
            body.append(record["body_delay"])
    for path in native.glob(".flowdc/attempts/*/*/result.json"):
        error = read_json(path).get("error")
        if error:
            errors[str(error)] += 1
    traces = (overview["control_trajectories"] if strict else
              [{"path": str(p.relative_to(native))} for p in native.glob(".flowdc/control/*.jsonl")])
    for spec in traces:
        relative = Path(spec["path"])
        require(not relative.is_absolute() and ".." not in relative.parts, "unsafe trace path")
        path = native / relative
        if strict:
            require(spec["closed"] and file_hash(path) == spec["sha256"] and path.stat().st_size == spec["bytes"],
                    "unclosed or modified control trace")
        events = [json.loads(line) for line in path.read_text().splitlines()]
        if strict:
            require(len(events) == spec["records"] and events[0]["event"] == "start"
                    and events[-1]["event"] == "end" and
                    all(e["run_id"] == spec["run_id"] and e["invocation_id"] == spec["invocation_id"] for e in events),
                    "invalid control trace identity/closure")
        for event in events:
            if "reason" in event:
                reasons[event["reason"]] += 1
                limits.append((event["monotonic_s"], event["limit_before"], event["limit"], event["reason"]))
    limits.sort()
    weighted, previous, limit = 0.0, started, initial
    if started is not None and ended is not None:
        for instant, before, after, reason in limits:
            require(started <= instant <= ended, "control event outside acquisition clock")
            weighted += (instant - previous) * limit
            previous, limit = instant, after
        weighted += (ended - previous) * limit
    return {"attempts": sum(statuses.values()), "overload_attempts": overload, "observed_body_bytes": observed_bytes,
            "statuses": dict(statuses), "failure_kinds": dict(kinds), "errors": dict(errors),
            "tls_errors": sum(v for k, v in errors.items() if "CERTIFICATE_VERIFY_FAILED" in k or "SSLCertVerificationError" in k),
            "eligible_ttfb_s": ttfb, "eligible_body_delay_s": body, "control_reasons": dict(reasons),
            "decisions": len(limits), "time_weighted_mean_limit": weighted / (ended - started)
            if started is not None and ended > started else None,
            "decreases": sum(after < before for _, before, after, _ in limits),
            "decreases_by_reason": dict(Counter(reason for _, before, after, reason in limits if after < before)),
            "fraction_decisions_at_256": sum(after == 256 for _, _, after, _ in limits) / len(limits) if limits else None}


def verify(run_dir, slice_path, source, started=None, ended=None, credit_urls=False, planned_window=False):
    source = Path(source)
    sys.path.insert(0, str(source))
    sys.path.insert(0, str(source / "bin"))
    import download_batch
    import flowdc_integrity
    from flowdc_research_profile import EXTERNAL, workload
    raw_config = read_json(Path(run_dir) / "flowdc-config.json")
    RESEARCH = workload(raw_config.get("research_workload")) if planned_window else EXTERNAL
    require(not planned_window or RESEARCH.name == "bounded-adaptive-ablation-v1", "planned-window profile required")

    from benchmark.core.verifier import archive_members, file_digest, read_file

    run_dir, slice_path = Path(run_dir), Path(slice_path)
    native = run_dir / "native"
    final_raw = read_file(native / ".flowdc/final.json")
    final = json.loads(final_raw)
    index_raw = read_file(native / "outcome-index.json", RESEARCH.max_metadata_bytes)
    index = json.loads(index_raw)
    overview = read_json(native / "overview.json")
    owner = read_json(native / ".flowdc/owner.json")
    manifest = file_hash(slice_path)
    frame = pl.read_parquet(slice_path)
    expected_ids = {sha256(encode([manifest, i])): (i, row) for i, row in enumerate(frame.to_dicts())}
    require(final["schema_version"] == index["schema_version"] == owner["schema_version"] == 2, "unsupported schema")
    require(final["run_id"] == index["run_id"] == owner["run_id"], "run identity mismatch")
    require(final["completion_boundary"] == "verified_uncompressed_archive" and not final.get("artifact_error"), "invalid final boundary")
    require(final["outcome_index_sha256"] == sha256(index_raw), "outcome index binding mismatch")
    require(overview["output_integrity"]["completion_record_sha256"] == sha256(final_raw), "final record binding mismatch")
    for field, digest_field in (("http_observations", "http_observations_sha256"),
                                ("control_trajectories", "control_trajectories_sha256")):
        require(sha256(encode(overview[field])) == final[digest_field], "observation/trace binding mismatch")
    require(owner["manifest"]["sha256"] == index["manifest"]["sha256"] == final["manifest"]["sha256"] == manifest,
            "source manifest binding mismatch")
    old_argv = sys.argv
    try:
        sys.argv = ["download_batch.py", "--config", str(run_dir / "flowdc-config.json")]
        cfg = download_batch.normalize_config(download_batch.parse_args())
    finally:
        sys.argv = old_argv
    require(owner["config"] == flowdc_integrity.effective_config(cfg), "effective configuration/source mismatch")
    require(index["original_rows"] == len(index["rows"]) == frame.height, "original denominator mismatch")
    require({r["row_id"] for r in index["rows"]} == set(expected_ids), "original row identity mismatch")
    archive_sha, archive_bytes = file_digest(run_dir / "native.tar", RESEARCH.max_artifact_bytes)
    require(final["archive"]["sha256"] == archive_sha and final["archive"]["bytes"] == archive_bytes
            and final["archive"]["verified"], "archive binding mismatch")
    members = archive_members(run_dir / "native.tar", RESEARCH)
    expected_names = {"native/outcome-index.json", "native/overview.json"}
    require(members["native/outcome-index.json"] == index_raw, "archived index mismatch")
    require(isinstance(json.loads(members["native/overview.json"]), dict), "invalid archived overview")
    credited, credited_rows, decode_failures = 0, 0, 0
    verified_bytes = 0
    credited_urls = {}
    for row in index["rows"]:
        position, metadata = expected_ids[row["row_id"]]
        require(row["source_position"] == position and row["source_manifest"] == manifest, "row provenance mismatch")
        require(row["disposition"] in (("verified", "failed", "unattempted") if planned_window else ("verified", "failed")) and not row["attempt_information_uncertain"],
                "unknown/unattempted row evidence")
        require((0 if planned_window and row["disposition"] == "unattempted" else 1) <= row["attempt_intents"] <= cfg.max_retry_attempts, "invalid attempt count")
        if row["disposition"] != "verified":
            continue
        payload_name, metadata_name = "native/" + row["payload"], "native/" + row["metadata"]
        require(payload_name != metadata_name and not expected_names.intersection((payload_name, metadata_name)), "duplicate row destinations")
        expected_names.update((payload_name, metadata_name))
        raw = members[payload_name]
        require(len(raw) == row["payload_bytes"] and sha256(raw) == row["payload_sha256"], "payload mismatch")
        actual = json.loads(members[metadata_name])
        wanted = {"schema_version": 2, "row_id": row["row_id"], "key": row["row_id"], "url": metadata["url"],
                  "row": metadata, "source_manifest": manifest, "source_position": position, "source_rows": frame.height,
                  "payload": row["payload"], "payload_bytes": len(raw), "payload_sha256": sha256(raw)}
        require(all(actual.get(k) == v for k, v in wanted.items()), "archived metadata mismatch")
        verified_bytes += len(raw)
        try:
            decode_image(raw)
        except Exception:
            decode_failures += 1
            continue
        credited += len(raw)
        credited_rows += 1
        if credit_urls:
            credited_urls[metadata['url']] = len(raw)
    require(set(members) == expected_names, "unexpected/missing archive members")
    counts = Counter(r["disposition"] for r in index["rows"])
    require(all(index["counts"][k] == counts[k] for k in ("verified", "failed", "skipped", "unattempted")), "disposition count mismatch")
    require(verified_bytes == index["verified_payload_bytes"] == final["useful_final_payload_bytes"], "useful byte mismatch")
    marker=read_json(run_dir/"planned-stop.json") if planned_window else None
    require(final["run_complete"] == (counts["failed"] == 0 and counts["unattempted"] == 0 and (not planned_window or marker["reason"] == "input_exhausted")), "native completion/count mismatch")
    obs = observations(run_dir, started, ended, cfg.C_init, strict=True)
    require(obs["attempts"] == sum(r["attempt_intents"] for r in index["rows"]), "attempt denominator mismatch")
    if not planned_window:
        require(not obs["failure_kinds"].get("unknown"), "unclassified acquisition failure")
    audit = [json.loads(line) for line in (run_dir / "request-audit.jsonl").read_text().splitlines()]
    require(audit and audit[0]["event"] == "start" and audit[-1]["event"] == "closed"
            and audit[-1]["active"] == 0 and (audit[-1]["stop_reason"] in (None, "planned_window_end", "planned_body_budget") if planned_window else not audit[-1]["stop_reason"]), "audit not normally closed")
    active, dispatched_tokens, ended_tokens = set(), set(), set()
    previous = -1
    for event in audit:
        require(event["monotonic_s"] >= previous, "audit clock reversed")
        previous = event["monotonic_s"]
        if event["event"] == "dispatch":
            token = event["token"]
            require(token not in dispatched_tokens and len(active) < cfg.C_max, "duplicate/excess dispatch")
            active.add(token); dispatched_tokens.add(token)
            require(event["inflight"] == len(active), "dispatch inflight mismatch")
        elif event["event"] == "network_end":
            token = event["token"]
            require(token not in ended_tokens and event["dispatched"] == (token in active), "network closure mismatch")
            active.discard(token); ended_tokens.add(token)
            require(event["inflight"] == len(active), "completion inflight mismatch")
    require(not active and dispatched_tokens <= ended_tokens, "unclosed network requests")
    attempted = Counter(e["url"] for e in audit if e["event"] == "attempt")
    dispatched = Counter(e["url"] for e in audit if e["event"] == "dispatch")
    require(sum(attempted.values()) == obs["attempts"] and (set(attempted) <= set(frame["url"]) if planned_window else set(attempted) == set(frame["url"])), "request audit denominator mismatch")
    if planned_window:
        marker=read_json(run_dir/"planned-stop.json")
        require(marker["reason"] == (audit[-1]["stop_reason"] or "input_exhausted") and marker["protocol_sha256"] == read_json(run_dir/"attempt.json")["protocol_sha256"], "planned-stop binding changed")
        for record in index["rows"]:
            error=str(record.get("error") or "")
            if record["disposition"] == "failed" and "study dispatch stopped:" in error:
                require(any(reason in error for reason in ("planned_window_end","planned_body_budget")), "unplanned guard rejection")
        unknown=[]
        for spec in overview["http_observations"]["files"]:
            measurement=read_json(native/spec["path"])
            if measurement["failure_kind"] == "unknown":unknown.append(measurement)
        failures={r["row_id"]:str(r.get("error") or "") for r in index["rows"]}
        require(all(any("study dispatch stopped: "+reason in failures[r["row_id"]] for reason in ("planned_window_end","planned_body_budget")) for r in unknown), "unknown failure is not planned administrative censoring")
    require(all(n <= cfg.max_retry_attempts and dispatched[url] <= n for url, n in attempted.items()), "unexpected request replay")
    result = {"native_run_complete": final["run_complete"], "final_useful_payload_bytes": final["useful_final_payload_bytes"],
            "flowdc_verified_payload_bytes": verified_bytes, "dispositions": dict(counts),
            "row_errors": dict(Counter(str(r.get("error")) for r in index["rows"] if r["disposition"] != "verified")),
            "row_final_status": dict(Counter(str(r.get("status_code")) for r in index["rows"])),
            "credited_bytes": credited, "credited_rows": credited_rows, "decode_failures": decode_failures,
            **({"attempted_rows":len(attempted), "dispatched_rows":len(dispatched), "original_rows":frame.height, "protocol_window_complete":True} if planned_window else {}),
            "archive_mismatch": 0, "archive_sha256": archive_sha, "observations": obs}
    if credit_urls:
        result['credited_url_bytes'] = credited_urls
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--slice", required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--started", type=float, required=True)
    parser.add_argument("--ended", type=float, required=True)
    parser.add_argument("--partial", action="store_true")
    parser.add_argument("--credit-urls", action="store_true")
    parser.add_argument("--planned-window", action="store_true")
    args = parser.parse_args()
    result = observations(args.run_dir, args.started, args.ended) if args.partial else verify(args.run_dir, args.slice, args.source, args.started, args.ended, args.credit_urls, args.planned_window)
    # stdout is a bounded JSON interface; native artifacts are never changed.
    print(json.dumps(result, allow_nan=False))


if __name__ == "__main__":
    main()
