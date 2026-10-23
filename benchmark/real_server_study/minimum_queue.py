"""Finite queue: stopped runs remain outcomes; only scoped blockers retire work."""

import datetime as dt
import time
from pathlib import Path

from .common import file_hash, read_json, require, utc, write_new
from .cooldown import user_seconds
from .minimum_recovery import stop_scope
from .minimum_study import (LIMITS, METHODS, SOURCES, TARGETS, calibration_cells,
                            freeze, select_configuration, sizing, tool_settings)


def records(study):
    result = {p.parent.name: read_json(p) for p in (study / "runs").glob("*/record.json")}
    unfinished = [p.name for p in (study / "runs").iterdir() if p.is_dir() and p.name not in result]
    require(not unfinished, "unfinished first attempt; no replacement dispatch")
    return result


def note(study, name, value):
    path = study / "gates" / (name + ".json")
    if not path.exists():
        write_new(path, dict(utc=utc(), **value))
    return read_json(path)


def source_state(study, source, history):
    protocol = read_json(study / "protocol.json")
    provenance = protocol["provenance"][source]
    if (study / "gates" / (source + "-probe-failure.json")).exists():
        return dict(status="suspended", reason="initial health probe failed")
    if provenance.get("media_rights_qualified") is not True or provenance.get("access_qualified") is not True:
        return note(study, source + "-disposition", dict(source=source, status="not_qualified",
            reason=provenance.get("limitation", "media/access qualification missing")))
    for row in history.values():
        if row["source"] != source or row["status"] == "complete":
            continue
        scope = stop_scope(row)
        require(scope != "campaign", "campaign-scoped failure: " + row["run_id"])
        seconds = user_seconds(protocol)
        server_until = max([e.get("server_retry_after_until_utc", 0) for e in row.get("stop_evidence", [])] + [0])
        closed = dt.datetime.fromisoformat(row["closed_utc"].replace("Z", "+00:00")).timestamp() if row.get("closed_utc") else time.time()
        decision = note(study, "stop-" + row["run_id"], dict(source=source, run_id=row["run_id"],
            stop_scope=scope, status="stopped", user_cooldown_seconds=seconds,
            server_retry_after_until_utc=server_until, until_utc=max(server_until, closed + seconds if seconds else 0)))
        if scope == "source":
            return note(study, source + "-disposition", dict(source=source, status="suspended",
                reason="source-scoped stop", first_attempts=[row["run_id"]],
                user_cooldown_seconds=seconds, user_cooldown_until_utc=closed + seconds if seconds else 0,
                server_retry_after_until_utc=server_until,
                earliest_reconsideration_utc=max(time.time(), decision["until_utc"])))
        if time.time() < decision["until_utc"]:
            return dict(status="wait", phase="wait", source=source, until_utc=decision["until_utc"],
                        reason="server Retry-After or explicit user pause")
        service_error = any(e.get("status_counts", {}).get("503", 0) for e in row.get("stop_evidence", []))
        if service_error:
            path = study / "gates" / ("recovery-" + row["run_id"] + ".json")
            if not path.exists():
                return dict(status="recovery_probe", phase="recovery_probe", source=source,
                            run_id="recovery-" + row["run_id"], after_run=row["run_id"], job_seconds=180, rows=4)
            recovery = read_json(path)
            require(recovery["source"] == source and recovery["after_run"] == row["run_id"], "recovery identity changed")
            if recovery["status"] != "passed":
                return note(study, source + "-disposition", dict(source=source, status="suspended",
                    reason="four-object recovery check failed", first_attempts=[row["run_id"]]))
    return dict(status="available")


def source_available(study, source, history):
    return source_state(study, source, history)["status"] == "available"


def dispatch(cell):
    return {**cell, "job_seconds": cell.get("job_seconds", LIMITS["job_seconds"])}


def target_size(study, source, history):
    path = study / "gates" / (source + "-size.json")
    if path.exists():
        return read_json(path)["rows"], None
    protocol = read_json(study / "protocol.json")
    count = protocol["initial_rows"][source]
    candidates = [2, 1, 0] if source == SOURCES[0] else [3, 2, 1, 0]
    index = corrections = 0
    basis = []
    reason = "all sizing configurations stopped"
    while index < len(candidates):
        candidate = candidates[index]
        cell = dict(run_id=f"sizing-{source}-c{candidate}-s{corrections}", phase="sizing", source=source,
                    workers=1, rows=count, method=METHODS[2], profile=candidate,
                    tool_setting=tool_settings(source)[candidate], candidate=candidate)
        record = history.get(cell["run_id"])
        if record is None:
            return None, dispatch(cell)
        require(record["rows"] == count, "sizing identity differs")
        basis.append(record["run_id"])
        if record["status"] != "complete":
            index += 1
            continue
        if record["http_span_s"] >= LIMITS["minimum_http_seconds"]:
            write_new(path, dict(rows=count, basis=basis, actual_http_seconds=record["http_span_s"]))
            return count, None
        if corrections >= LIMITS["corrections"]:
            reason = "two size corrections exhausted without ten-minute acquisition"
            break
        try:
            count = sizing(record, protocol["partitions"][source]["capacity"])
        except ValueError as error:
            reason = str(error)
            break
        corrections += 1
    write_new(path, dict(rows=None, basis=basis, status="dependency_blocked", reason=reason))
    return None, None


def unavailable(reason, rows=()):
    return dict(status="unavailable", reason=reason, attempts=[r["run_id"] for r in rows])


def selection(cells, history, method):
    rows = [history[c["run_id"]] for c in cells if c["method"] == method and c["run_id"] in history]
    return select_configuration(rows, method) if len(rows) == 4 else unavailable("source/prerequisite prevented full candidate inventory", rows)


def next_cell(study, remaining_seconds, lifecycle_seconds_per_job=0, calibration_ready=True):
    study = Path(study)
    history = records(study)
    states = {s: source_state(study, s, history) for s in SOURCES}
    waits = []

    def admit(source, cell):
        state = states[source]
        if state["status"] == "available":
            return dispatch(cell)
        if state["status"] == "recovery_probe":
            return state
        if state["status"] == "wait":
            waits.append(state)
        return None

    frozen_path = study / "freeze.json"
    if frozen_path.exists():
        frozen = read_json(frozen_path)
        require(frozen["protocol_sha256"] == file_hash(study / "protocol.json"), "frozen protocol changed")
        prior = [r for r in history.values() if r.get("phase") == "evaluation"]
        next_day = 0
        if prior:
            first = min(dt.datetime.fromisoformat(r["started_utc"].replace("Z", "+00:00")) for r in prior)
            next_day = dt.datetime.combine(first.date() + dt.timedelta(days=1), dt.time(), dt.UTC).timestamp()
        for cell in frozen["cells"]:
            if cell["run_id"] in history:
                continue
            if states[cell["source"]]["status"] in ("suspended", "not_qualified"):
                note(study, "blocked-" + cell["run_id"], dict(run_id=cell["run_id"], status="dependency_blocked",
                     reason=states[cell["source"]]["reason"]))
                continue
            if cell["block"] >= 3 and time.time() < next_day:
                waits.append(dict(phase="wait", until_utc=next_day, reason="confirmation spans two UTC dates"))
                continue
            chosen = admit(cell["source"], cell)
            if chosen:
                return chosen
        return min(waits, key=lambda x: x["until_utc"]) if waits else None

    sizes, selected = {}, {}
    development_path = study / "gates/development-selection.json"
    for source in SOURCES:
        state = states[source]
        usable = state["status"] in ("available", "wait", "recovery_probe")
        size_path = study / "gates" / (source + "-size.json")
        if usable or size_path.exists():
            size, pending = target_size(study, source, history)
            if pending:
                chosen = admit(source, pending)
                if chosen:
                    return chosen
                continue
        else:
            size = None
        cells = calibration_cells(source, size, development=source == SOURCES[0]) if size else []
        if size:
            # Target sizing is independent; target calibration waits for the
            # recorded development disposition, even if all methods unavailable.
            if source in TARGETS and not development_path.exists():
                continue
            todo = next((c for c in cells if c["run_id"] not in history), None)
            if todo:
                chosen = admit(source, todo)
                if chosen:
                    return chosen
                if state["status"] == "wait":
                    continue
            if usable:
                sizes[source] = size
        if source == SOURCES[0]:
            selected["development"] = {m: selection(cells, history, m) for m in METHODS}
            if not development_path.exists():
                write_new(development_path, selected["development"])
        else:
            selected[source] = selection(cells, history, METHODS[2])
    if waits:
        return min(waits, key=lambda x: x["until_utc"])
    require(development_path.exists(), "development disposition missing")
    selected["development"] = read_json(development_path)
    targets = [s for s in TARGETS if s in sizes]
    frozen = freeze(study, selected, sizes, targets, list(history.values()), remaining_seconds)
    return next_cell(study, remaining_seconds) if frozen["cells"] else None
