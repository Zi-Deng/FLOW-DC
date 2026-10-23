"""Finite transfer-study queue; immutable first attempts, source-local suspension."""

import datetime as dt
import time
from pathlib import Path

from .common import file_hash, read_json, require, utc, write_new
from .cooldown import deadline, user_seconds
from .minimum_study import (LIMITS, METHODS, SOURCES, TARGETS, calibration_cells,
                            freeze, select_configuration, sizing, tool_settings)


def records(study):
    result = {p.parent.name: read_json(p) for p in (study / "runs").glob("*/record.json")}
    unfinished = [p.name for p in (study / "runs").iterdir() if p.is_dir() and p.name not in result]
    require(not unfinished, "unfinished first attempt; no replacement dispatch")
    return result


def source_available(study, source, history):
    protocol = read_json(study / "protocol.json")
    provenance = protocol["provenance"][source]
    if (study / "gates" / (source + "-probe-failure.json")).exists():
        return False
    if provenance.get("media_rights_qualified") is not True or provenance.get("access_qualified") is not True:
        path = study / "gates" / (source + "-disposition.json")
        if not path.exists():
            write_new(path, dict(source=source, status="not_qualified", reason=provenance.get("limitation", "media/access qualification missing"), utc=utc()))
        return False
    stops = [r for r in history.values() if r["source"] == source and r["status"] != "complete"]
    if stops:
        # Refusals retire the source in this finite execution. A future separately
        # scheduled acquisition needs a bounded health check;
        # nothing automatically probes/replays a known failing configuration.
        path = study / "gates" / (source + "-disposition.json")
        if not path.exists():
            seconds = user_seconds(protocol)
            server_until = max([e.get("server_retry_after_until_utc", 0)
                                for r in stops for e in r.get("stop_evidence", [])] + [0])
            user_until = deadline(seconds)
            write_new(path, dict(source=source, status="suspended", utc=utc(),
                first_attempts=[r["run_id"] for r in stops],
                user_cooldown_seconds=seconds, user_cooldown_until_utc=user_until,
                server_retry_after_until_utc=server_until,
                earliest_reconsideration_utc=max(time.time(), user_until, server_until),
                reason="Stopped first attempt; preserve partial evidence, no automatic refusal retry"))
        return False
    return True


def dispatch(cell):
    return {**cell, "job_seconds": cell.get("job_seconds", LIMITS["job_seconds"])}


def target_size(study, source, history):
    path = study / "gates" / (source + "-size.json")
    if path.exists():
        return read_json(path)["rows"], None
    protocol = read_json(study / "protocol.json")
    count = protocol["initial_rows"][source]
    basis = []
    for step in range(LIMITS["corrections"] + 1):
        cell = dict(run_id=f"sizing-{source}-{step}", phase="sizing", source=source,
                    workers=1, rows=count, method=METHODS[2], profile=3,
                    tool_setting=tool_settings(source)[3], candidate=3)
        record = history.get(cell["run_id"])
        if record is None:
            return None, dispatch(cell)
        require(record["rows"] == count and record["status"] == "complete", "sizing identity/closure differs")
        basis.append(record["run_id"])
        if record["http_span_s"] >= LIMITS["minimum_http_seconds"]:
            write_new(path, dict(rows=count, basis=basis, actual_http_seconds=record["http_span_s"]))
            return count, None
        if step < LIMITS["corrections"]:
            count = sizing(record, protocol["partitions"][source]["capacity"])
    raise ValueError("two sizing corrections exhausted without ten-minute acquisition")


def next_cell(study, remaining_seconds, lifecycle_seconds_per_job=0, calibration_ready=True):
    study = Path(study)
    history = records(study)
    available = {s for s in SOURCES if source_available(study, s, history)}
    frozen_path = study / "freeze.json"
    if frozen_path.exists():
        frozen = read_json(frozen_path)
        require(frozen["protocol_sha256"] == file_hash(study / "protocol.json"), "frozen protocol changed")
        chosen = next((c for c in frozen["cells"] if c["source"] in available and c["run_id"] not in history), None)
        if chosen and chosen["block"] >= 3:
            prior = [r for r in history.values() if r.get("phase") == "evaluation"]
            if prior:
                first = min(dt.datetime.fromisoformat(r["started_utc"].replace("Z", "+00:00")) for r in prior)
                next_day = dt.datetime.combine(first.date() + dt.timedelta(days=1), dt.time(), dt.UTC).timestamp()
                if time.time() < next_day:
                    return dict(phase="wait", until_utc=next_day, reason="confirmation spans two UTC dates")
        return dispatch(chosen) if chosen else None
    if SOURCES[0] not in available:
        return None
    sizes = {}
    for source in SOURCES:
        if source not in available:
            continue
        size, pending = target_size(study, source, history)
        if pending:
            return pending
        sizes[source] = size
        cells = calibration_cells(source, size, development=source == SOURCES[0])
        for cell in cells:
            if cell["run_id"] not in history:
                return dispatch(cell)
        # Global PAARC selection happens before the target calibration/outcomes.
        if source == SOURCES[0]:
            path = study / "gates/development-selection.json"
            if not path.exists():
                selection = {m: select_configuration([history[c["run_id"]] for c in cells if c["method"] == m], m) for m in METHODS}
                write_new(path, selection)
    selected = dict(development=read_json(study / "gates/development-selection.json"))
    targets = [s for s in TARGETS if s in sizes]
    if not targets:
        return None
    for source in targets:
        cells = calibration_cells(source, sizes[source])
        selected[source] = select_configuration([history[c["run_id"]] for c in cells], METHODS[2])
    frozen = freeze(study, selected, sizes, targets, list(history.values()), remaining_seconds)
    return dispatch(frozen["cells"][0])
