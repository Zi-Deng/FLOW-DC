"""Finite first-attempt queue; only calibration determines size/replication."""

from pathlib import Path

from .common import read_json, require, write_new
from .minimum_study import (
    LIMITS,
    METHODS,
    SOURCES,
    TOOL_SETTINGS,
    estimate,
    feasible,
    freeze,
    modeled,
    select_tool,
    sizing,
    schedule,
)


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
            and r["phase"] in ("engineering", "calibration")
            and r["workers"] == cell["workers"]
            and r["method"] == cell["method"]
            and (r["method"] != METHODS[2] or list(r["tool_setting"]) == list(cell["tool_setting"]))
        ]
        require(basis, "missing matching job-duration observation")
        r = max(basis, key=lambda r: r["rows"])
        feasible([r], cell["rows"])
        seconds = min(
            LIMITS["job_seconds"], max(600, int(1.25 * estimate(r, cell["rows"], "elapsed_s") + 60))
        )
    return {**cell, "job_seconds": seconds}


def campaign_forecast(study, records, sizes, remaining_seconds, lifecycle_seconds):
    path = study / "gates" / "campaign-feasibility.json"
    if path.exists():
        require(read_json(path)["accepted"], "retained campaign forecast refused")
        return
    engineering = [r for r in records.values() if r["phase"] == "engineering" and r["rows"] == 2048]
    selected = {}
    evaluation_rows = dict(sizes)
    for source in SOURCES:
        one = [r for r in engineering if r["source"] == source and r["workers"] == 1]
        reference = max(r["coverage"] for r in one)
        tools = [r for r in one if r["method"] == METHODS[2] and r["coverage"] >= reference - 0.01]
        require(tools, "no engineering coverage-eligible comparator")
        selected[source] = max(
            tools, key=lambda r: r["body_bytes"] / estimate(r, sizes[source], "elapsed_s")
        )["tool_setting"]
    two = [
        r
        for r in engineering
        if r["source"] == SOURCES[0]
        and r["workers"] == 2
        and (r["method"] != METHODS[2] or r["tool_setting"] == selected[SOURCES[0]])
    ]
    evaluation_rows[SOURCES[0]] = max(sizes[SOURCES[0]], sizing(two, workers=2))
    cells = [c for source in SOURCES for c in candidates(source, "calibration", sizes[source])]
    cells += [
        dict(
            source=SOURCES[0], workers=2, method=m, rows=sizes[SOURCES[0]], tool_setting=selected[SOURCES[0]]
        )
        for m in METHODS
    ]
    cells += schedule(selected, evaluation_rows, 3)
    seconds = body = 0.0
    for cell in cells:
        basis = [
            r
            for r in engineering
            if r["source"] == cell["source"]
            and r["workers"] == cell["workers"]
            and r["method"] == cell["method"]
            and (r["method"] != METHODS[2] or r["tool_setting"] == list(cell["tool_setting"]))
        ]
        require(len(basis) == 1, "ambiguous initial forecast")
        r = basis[0]
        feasible([r], cell["rows"])
        seconds += estimate(r, cell["rows"], "elapsed_s")
        body += r["body_bytes"] * cell["rows"] / r["rows"]
    # Reserve the complete allowed three-job correction scope, plus final
    # collection/cleanup. This early forecast is revisited after calibration.
    total = (
        1.25 * (seconds + len(cells) * lifecycle_seconds)
        + 3 * (LIMITS["job_seconds"] + lifecycle_seconds)
        + 10800
    )
    requests = (
        sum(r["rows"] for r in records.values())
        + sum(c["rows"] for c in cells)
        + 3 * max(evaluation_rows.values())
        + 64
    )
    accepted = (
        total <= remaining_seconds
        and requests <= LIMITS["max_requests"]
        and 1.25 * body + 3 * LIMITS["job_body_bytes"] <= LIMITS["body_bytes"]
    )
    write_new(
        path,
        dict(
            accepted=accepted,
            forecast_seconds=total,
            remaining_seconds=remaining_seconds,
            lifecycle_seconds_per_job=lifecycle_seconds,
            forecast_body_bytes=body,
            forecast_requests=requests,
            anticipated_tool_settings=selected,
            anticipated_evaluation_rows=evaluation_rows,
            basis="affine engineering estimates; final method selection still uses long calibration",
        ),
    )
    require(accepted, "remaining calibration plus27 evaluation jobs cannot fit; stop before long calibration")


def next_cell(study, remaining_seconds, lifecycle_seconds_per_job, calibration_ready=True):
    study = Path(study)
    records = {p.parent.name: read_json(p) for p in (study / "runs").glob("*/record.json")}
    incomplete = [p.name for p in (study / "runs").iterdir() if p.is_dir() and p.name not in records]
    require(not incomplete, "unfinished first attempt; no replacement dispatch")
    require(
        all(r["status"] == "complete" for r in records.values()), "stopped first attempt requires disposition"
    )
    records = {r["run_id"]: r for r in modeled(list(records.values()))}
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
    if not calibration_ready:
        return dict(phase="observe_cleanup")
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
    campaign_forecast(study, records, sizes, remaining_seconds, lifecycle_seconds_per_job)
    for source in SOURCES:
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
    eligible = [
        r for r in calibration if r["method"] != METHODS[2] or r["tool_setting"] == selected[r["source"]]
    ]
    needs = [
        source for source in SOURCES if any(r["http_span_s"] < 600 for r in eligible if r["source"] == source)
    ]
    correction = study / "gates" / "correction-size.json"
    if correction.exists() or needs:
        if not correction.exists():
            require(len(needs) == 1, "both sources need corrections beyond the three-job budget")
            source = needs[0]
            workers = 2 if source == SOURCES[0] else 1
            chosen = [r for r in eligible if r["source"] == source]
            target = max(
                sizing([r for r in chosen if r["workers"] == w], workers=w)
                for w in sorted({r["workers"] for r in chosen})
            )
            feasible(chosen, target)
            write_new(
                correction,
                dict(source=source, workers=workers, rows=target, basis=[r["run_id"] for r in chosen]),
            )
        decision = read_json(correction)
        source, workers = decision["source"], decision["workers"]
        sizes[source] = decision["rows"]
        for method in METHODS:
            cell = dict(
                run_id="correction-" + source + "-" + method,
                phase="calibration",
                source=source,
                workers=workers,
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
        calibration = [r for r in calibration if r["source"] != source or r["workers"] != workers] + corrected
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
