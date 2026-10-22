"""Finite first-attempt queue; only calibration determines size/replication."""

import math
from pathlib import Path

from .common import read_json, require, write_new
from .minimum_study import LIMITS, METHODS, SOURCES, TOOL_SETTINGS, freeze, select_tool


def candidates(source, phase, rows, workers=1):
    choices = [(METHODS[0], TOOL_SETTINGS[1]), (METHODS[1], TOOL_SETTINGS[1])]
    choices += [(METHODS[2], setting) for setting in TOOL_SETTINGS]
    for index, (method, setting) in enumerate(choices):
        yield dict(
            run_id=f"{phase}-{source}-w{workers}-n{rows}-c{index}",
            phase=phase,
            source=source,
            workers=workers,
            rows=rows,
            method=method,
            tool_setting=setting,
        )


def next_cell(study, remaining_seconds, lifecycle_seconds_per_job):
    study = Path(study)
    records = {p.parent.name: read_json(p) for p in (study / "runs").glob("*/record.json")}
    incomplete = [p.name for p in (study / "runs").iterdir() if p.is_dir() and p.name not in records]
    require(not incomplete, "unfinished first attempt; no replacement dispatch")
    require(
        all(r["status"] == "complete" for r in records.values()), "stopped first attempt requires disposition"
    )
    if (study / "freeze.json").exists():
        cells = read_json(study / "freeze.json")["cells"]
        return next((cell for cell in cells if cell["run_id"] not in records), None)
    # Two short sizes allow fixed setup cost to be separated from per-row work.
    for source in SOURCES:
        for workers in (1, 2) if source == SOURCES[0] else (1,):
            for rows in (512, 2048):
                for cell in candidates(source, "engineering", rows, workers):
                    if cell["run_id"] not in records:
                        return cell
    sizes = {}
    for source in SOURCES:
        path = study / "gates" / (source + "-size.json")
        if not path.exists():
            observed = [
                r
                for r in records.values()
                if r["source"] == source and r["phase"] == "engineering" and r["rows"] == 2048
            ]
            fastest = max(r["rows"] / r["http_span_s"] for r in observed)
            rows = int(math.ceil(fastest * LIMITS["target_http_seconds"] / 1024) * 1024)
            require(rows <= LIMITS["max_rows"], "ten-minute workload exceeds finite row ceiling")
            write_new(
                path, dict(rows=rows, basis=[r["run_id"] for r in observed], fastest_rows_per_s=fastest)
            )
        sizes[source] = read_json(path)["rows"]
        for cell in candidates(source, "calibration", sizes[source]):
            if cell["run_id"] not in records:
                return cell
    selected = {}
    for source in SOURCES:
        observed = [
            r
            for r in records.values()
            if r["source"] == source and r["phase"] == "calibration" and r["workers"] == 1
        ]
        selected[source] = select_tool(observed)
    for method in METHODS:
        cell = dict(
            run_id="calibration-inaturalist-open-w2-" + method,
            phase="calibration",
            source=SOURCES[0],
            workers=2,
            rows=sizes[SOURCES[0]],
            method=method,
            tool_setting=selected[SOURCES[0]],
        )
        if cell["run_id"] not in records:
            return cell
    calibration = [r for r in records.values() if r["phase"] == "calibration"]
    # At most one size correction per source/worker condition, using the frozen
    # selected methods. Corrections remain calibration; never replace old records.
    for source in SOURCES:
        chosen = [
            r
            for r in calibration
            if r["source"] == source and (r["method"] != METHODS[2] or r["tool_setting"] == selected[source])
        ]
        fastest = max(r["rows"] / r["http_span_s"] for r in chosen)
        target = int(math.ceil(fastest * 720 / 1024) * 1024)
        if min(r["http_span_s"] for r in chosen) < 600:
            require(
                source == SOURCES[0] and target <= LIMITS["max_rows"],
                "long qualification needs an amendment beyond three correction jobs",
            )
            sizes[source] = target
            for method in METHODS:
                cell = dict(
                    run_id="correction-inaturalist-open-" + method,
                    phase="calibration",
                    source=source,
                    workers=2,
                    rows=target,
                    method=method,
                    tool_setting=selected[source],
                )
                if cell["run_id"] not in records:
                    return cell
            calibration = [r for r in calibration if r["source"] != source or r["workers"] != 2]
            calibration += [r for r in records.values() if r["run_id"].startswith("correction-")]
    used = dict(
        requests=sum(r["rows"] for r in records.values()),
        body_bytes=sum(r["body_bytes"] for r in records.values()),
        retained_bytes=sum(p.stat().st_size for p in (study / "runs").rglob("*") if p.is_file()),
        lifecycle_seconds_per_job=lifecycle_seconds_per_job,
    )
    calibration = [
        r for r in calibration if r["method"] != METHODS[2] or r["tool_setting"] == selected[r["source"]]
    ]
    record = freeze(study, calibration, selected, sizes, remaining_seconds, used)
    return record["cells"][0]
