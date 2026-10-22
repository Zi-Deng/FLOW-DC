"""Finite first-attempt queue; only calibration determines size/replication."""

from pathlib import Path

from .common import read_json, require, write_new
from .minimum_study import LIMITS, METHODS, SOURCES, TOOL_SETTINGS, feasible, freeze, select_tool, sizing


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


def dispatch_cell(cell, records):
    if cell is None:
        return None
    if cell["phase"] == "engineering":
        seconds = 600
    else:
        basis = [
            r
            for r in records.values()
            if r["source"] == cell["source"]
            and r["workers"] == cell["workers"]
            and r["method"] == cell["method"]
            and (r["method"] != METHODS[2] or list(r["tool_setting"]) == list(cell["tool_setting"]))
        ]
        require(basis, "missing matching job-duration observation")
        r = max(basis, key=lambda r: r["rows"])
        feasible([r], cell["rows"])
        seconds = min(
            LIMITS["job_seconds"], max(600, int(1.5 * r["elapsed_s"] * cell["rows"] / r["rows"] + 120))
        )
    return {**cell, "job_seconds": seconds}


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
        return dispatch_cell(next((cell for cell in cells if cell["run_id"] not in records), None), records)
    # Two short sizes allow fixed setup cost to be separated from per-row work.
    for source in SOURCES:
        for workers in (1, 2) if source == SOURCES[0] else (1,):
            for rows in (512, 2048):
                for cell in candidates(source, "engineering", rows, workers):
                    if cell["run_id"] not in records:
                        return dispatch_cell(cell, records)
    sizes = {}
    for source in SOURCES:
        path = study / "gates" / (source + "-size.json")
        if not path.exists():
            observed = [
                r
                for r in records.values()
                if r["source"] == source
                and r["phase"] == "engineering"
                and r["rows"] == 2048
                and r["workers"] == 1
            ]
            rows = sizing(observed, workers=1)
            write_new(path, dict(rows=rows, basis=[r["run_id"] for r in observed], workers=1))
        sizes[source] = read_json(path)["rows"]
        for cell in candidates(source, "calibration", sizes[source]):
            if cell["run_id"] not in records:
                return dispatch_cell(cell, records)
    selected = {}
    for source in SOURCES:
        observed = [
            r
            for r in records.values()
            if r["source"] == source
            and r["phase"] == "calibration"
            and r["workers"] == 1
            and not r["run_id"].startswith("correction-")
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
            return dispatch_cell(cell, records)
    calibration = [
        r
        for r in records.values()
        if r["phase"] == "calibration" and not r["run_id"].startswith("correction-")
    ]
    # Persist a single three-job correction decision. Later observations never
    # change the rows assigned to these first attempts.
    source = SOURCES[0]
    chosen = [r for r in calibration if r["source"] == source and r["workers"] == 2]
    correction = study / "gates" / "correction-size.json"
    if correction.exists() or min(r["http_span_s"] for r in chosen) < 600:
        if not correction.exists():
            target = sizing(chosen, workers=2)
            feasible(
                [
                    r
                    for r in calibration
                    if r["source"] == source
                    and r["workers"] == 1
                    and (r["method"] != METHODS[2] or r["tool_setting"] == selected[source])
                ],
                target,
            )
            write_new(correction, dict(rows=target, basis=[r["run_id"] for r in chosen]))
        sizes[source] = read_json(correction)["rows"]
        for method in METHODS:
            cell = dict(
                run_id="correction-inaturalist-open-" + method,
                phase="calibration",
                source=source,
                workers=2,
                rows=sizes[source],
                method=method,
                tool_setting=selected[source],
            )
            if cell["run_id"] not in records:
                return dispatch_cell(cell, records)
        corrected = [r for r in records.values() if r["run_id"].startswith("correction-")]
        require(
            min(r["http_span_s"] for r in corrected) >= 600,
            "bounded correction did not qualify ten-minute HTTP",
        )
        calibration = [r for r in calibration if r["source"] != source or r["workers"] != 2] + corrected
    require(
        all(
            r["http_span_s"] >= 600
            for r in calibration
            if r["source"] == SOURCES[1]
            and (r["method"] != METHODS[2] or r["tool_setting"] == selected[SOURCES[1]])
        ),
        "CVDF long acquisition needs a separately scoped sizing correction",
    )
    gates = [read_json(study / "gates" / (source + ".json")) for source in SOURCES]
    used = dict(
        requests=sum(g["requests"] for g in gates) + sum(r["rows"] for r in records.values()),
        body_bytes=sum(g["probe_body_bytes"] for g in gates) + sum(r["body_bytes"] for r in records.values()),
        retained_bytes=sum(p.stat().st_size for p in (study / "runs").rglob("*") if p.is_file()),
        lifecycle_seconds_per_job=lifecycle_seconds_per_job,
    )
    calibration = [
        r for r in calibration if r["method"] != METHODS[2] or r["tool_setting"] == selected[r["source"]]
    ]
    record = freeze(study, calibration, selected, sizes, remaining_seconds, used)
    return dispatch_cell(record["cells"][0], records)
