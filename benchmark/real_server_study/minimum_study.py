"""Frozen design and admission arithmetic for the minimum measured manuscript."""

import math
import random
from pathlib import Path

import polars as pl

from .common import file_hash, read_json, require, source_identity, utc, write_new

METHODS = ("ratio-v2", "gradient-candidate-v2", "img2dataset-1.47.0")
SOURCES = ("inaturalist-open", "openimages-cvdf")
TOOL_SETTINGS = ((1, 32), (1, 192), (8, 24))
OPTIONS = dict(startup_growth_factor=1.5, baseline_max_age_s=120.0, target_delay_ratio=2.0)
PROFILE = "bounded-public-long-v1"
SEED = 20261022
LIMITS = dict(
    wall_seconds=86400,
    dispatch_seconds=75600,
    max_windows=64,
    window_seconds=3600,
    cleanup_seconds=600,
    budget_su=1000,
    max_requests=16000000,
    body_bytes=3 * 2**40,
    archive_bytes=3584 * 2**30,
    free_bytes=50 * 2**30,
    job_seconds=2400,
    native_seconds=1800,
    audit_seconds=1200,
    collection_seconds=900,
    metadata_bytes=4 * 2**30,
    local_peak_bytes=12 * 2**30,
    max_rows=524288,
    job_body_bytes=64 * 2**30,
    target_http_seconds=720,
    minimum_http_seconds=600,
    corrections=3,
)


def prepare(study, source, populations, provenance):
    study = Path(study)
    require(not study.exists(), "new study required; do not replay first attempts")
    study.mkdir(parents=True, mode=0o700)
    for name in ("pools", "gates", "runs", "slices", "analysis"):
        (study / name).mkdir()
    hashes, population_rows = {}, {}
    for source_id in SOURCES:
        frame = pl.read_parquet(populations[source_id])
        columns = ["url", "record_id", "author", "license", "landing_url"]
        frame = frame.select(columns).cast({key: pl.String for key in columns})
        require(
            frame.height >= LIMITS["max_rows"] + 3 * 2048 + 2080 and frame["url"].n_unique() == frame.height,
            "insufficient unique source population",
        )
        require(not any(frame.null_count().row(0)), "missing source attribution")
        # A seeded shuffle, independent of future HTTP outcomes. Calibration is
        # one shared prefix; expansion never consumes evaluation rows.
        frame = frame.sample(fraction=1, shuffle=True, seed=SEED)
        frame = frame.with_row_index("study_row_id").with_columns(
            (pl.lit(source_id + "-") + pl.col("study_row_id").cast(pl.String)).alias("study_row_id")
        )
        path = study / "pools" / (source_id + ".parquet")
        frame.write_parquet(path)
        hashes[source_id] = file_hash(path)
        population_rows[source_id] = frame.height
    write_new(
        study / "protocol.json",
        dict(
            schema="flowdc-minimum-real-study-v1",
            utc=utc(),
            source=source_identity(source),
            limits=LIMITS,
            methods=METHODS,
            sources=SOURCES,
            options=OPTIONS,
            tool_settings=TOOL_SETTINGS,
            population_sha256=hashes,
            population_rows=population_rows,
            provenance=provenance,
            timing="manager submission through verified returned artifacts; no pooling with worker-only pilot",
            interpretation="paired real-source practical case studies, not universal or causal superiority",
        ),
    )


def manifest(study, source, phase, rows, block=0):
    require(source in SOURCES and 1 <= rows <= LIMITS["max_rows"], "invalid source/row budget")
    starts = {
        "probe": 0,
        "engineering": 32,
        "calibration": 2080,
        "evaluation": 2080 + LIMITS["max_rows"] + block * rows,
    }
    require(
        phase in starts and (phase != "probe" or rows == 32) and (phase != "engineering" or rows <= 2048),
        "invalid manifest phase",
    )
    frame = pl.read_parquet(Path(study) / "pools" / (source + ".parquet")).slice(starts[phase], rows)
    require(frame.height == rows, "source population exhausted")
    return frame


def configuration(directory, cell, frame):
    directory = Path(directory).resolve()
    workers = cell["workers"]
    require(workers in (1, 2) and cell["method"] in METHODS, "unsupported comparison")
    cap = 192 // workers
    if cell["method"] == METHODS[2]:
        setting = tuple(cell["tool_setting"])
        require(setting in TOOL_SETTINGS, "unfrozen tool setting")
        processes, threads = setting
        return dict(
            url_list=str(directory / "input.parquet"),
            input_format="parquet",
            url_col="url",
            output_folder=str(directory / "native"),
            output_format="webdataset",
            processes_count=processes,
            thread_count=threads // workers,
            timeout=30,
            retries=0,
            max_shard_retry=0,
            resize_mode="no",
            disable_all_reencoding=True,
            extract_exif=False,
            encode_format="jpg",
            compute_hash="sha256",
            distributor="multiprocessing",
            number_sample_per_shard=min(4096, max(1, math.ceil(frame.height / processes))),
            user_agent_token="flowdc-public-study",
            save_additional_columns=[c for c in frame.columns if c != "url"],
        )
    return dict(
        input=str(directory / "input.parquet"),
        input_format="parquet",
        output=str(directory / "native"),
        output_format="webdataset",
        url="url",
        label=None,
        concurrent_downloads=cap,
        timeout=30,
        enable_paarc=True,
        control_method=cell["method"],
        method_options=OPTIONS,
        C_min=2,
        C_init=4,
        C_max=cap,
        max_retry_attempts=1,
        research_profile=True,
        research_workload=PROFILE,
        create_tar=True,
        compress_tar=False,
        create_overview=True,
        force_overwrite=False,
    )


def select_tool(records):
    require(len(records) == 5 and all(r["status"] == "complete" for r in records), "calibration incomplete")
    reference = max(r["coverage"] for r in records)
    eligible = [r for r in records if r["method"] == METHODS[2] and r["coverage"] >= reference - 0.01]
    require(eligible, "no coverage-eligible comparator")
    best = max(eligible, key=lambda r: (r["goodput_Bps"], -TOOL_SETTINGS.index(tuple(r["tool_setting"]))))
    return best["tool_setting"]


def modeled(records):
    """Separate setup from per-row cost; evaluation outcomes never fit models."""
    result = []
    for record in records:
        same = [
            r
            for r in records
            if r["source"] == record["source"]
            and r["workers"] == record["workers"]
            and r["method"] == record["method"]
            and (r["method"] != METHODS[2] or list(r["tool_setting"]) == list(record["tool_setting"]))
        ]
        engineering = {r["rows"]: r for r in same if r["phase"] == "engineering"}
        models = {}
        if 512 in engineering and 2048 in engineering:
            small, large = engineering[512], engineering[2048]
            for field in ("elapsed_s", "native_seconds", "http_span_s"):
                slope = max(0.0, (large[field] - small[field]) / 1536)
                intercept = max(0.0, small[field] - slope * 512)
                calibration = [r for r in same if r["phase"] == "calibration"]
                if calibration:
                    row_count = max(r["rows"] for r in calibration)
                    value = max(r[field] for r in calibration if r["rows"] == row_count)
                    slope = max(0.0, (value - intercept) / row_count)
                models[field] = dict(intercept=intercept, slope=slope)
        result.append({**record, "forecast": models})
    return result


def estimate(record, rows, field):
    model = record.get("forecast", {}).get(field)
    if model:
        return model["intercept"] + model["slope"] * rows
    return record.get(field, record["elapsed_s"]) * rows / record["original_rows"]


def feasible(records, rows):
    """Reject a workload before dispatch when a measured candidate cannot fit."""
    require(records and 1 <= rows <= LIMITS["max_rows"], "invalid workload forecast")
    for r in records:
        scale = rows / r["original_rows"]
        require(
            1.25 * r["body_bytes"] * scale <= LIMITS["job_body_bytes"], "long workload exceeds body budget"
        )
        require(
            1.25 * estimate(r, rows, "native_seconds") < LIMITS["native_seconds"],
            "long workload exceeds native processing deadline",
        )
        require(
            1.25 * estimate(r, rows, "elapsed_s") + 60 < LIMITS["job_seconds"],
            "long workload exceeds complete-job deadline",
        )


def sizing(records, workers=1):
    require(records and all(r["status"] == "complete" for r in records), "sizing observations incomplete")
    require(all(r["workers"] == workers for r in records), "sizing must use matching worker count")
    slopes = [
        r.get("forecast", {}).get("http_span_s", {}).get("slope", r["http_span_s"] / r["original_rows"])
        for r in records
    ]
    require(min(slopes) > 0, "short HTTP observations cannot identify a positive per-row cost")
    rate = 1 / min(slopes)
    target = int(math.ceil(rate * LIMITS["target_http_seconds"] / 1024) * 1024)
    body_cap = min(
        int(0.8 * LIMITS["job_body_bytes"] * r["original_rows"] / r["body_bytes"]) for r in records
    )
    rows = min(target, body_cap, LIMITS["max_rows"])
    require(
        min(estimate(r, rows, "http_span_s") for r in records) >= LIMITS["minimum_http_seconds"],
        "finite bounds cannot support ten-minute acquisition",
    )
    feasible(records, rows)
    return rows


def schedule(selected, rows, primary_blocks=3):
    require(primary_blocks in (3, 6), "unapproved replication")
    rng = random.Random(SEED)
    order = list(METHODS)
    rng.shuffle(order)
    cells = []
    for block in range(primary_blocks):
        conditions = [("E1", SOURCES[0], 1)]
        if block < 3:
            conditions += [("E2", SOURCES[1], 1), ("E3", SOURCES[0], 2)]
        rng.shuffle(conditions)
        for family, source, workers in conditions:
            for method in order[block % 3 :] + order[: block % 3]:
                cells.append(
                    dict(
                        run_id=f"{family}-b{block + 1}-{method}",
                        family=family,
                        source=source,
                        workers=workers,
                        phase="evaluation",
                        block=block,
                        method=method,
                        rows=rows[source],
                        tool_setting=selected[source],
                    )
                )
    return cells


def freeze(study, calibration, selected, rows, remaining_seconds, used):
    """Choose replication only from calibration, before evaluation files exist."""
    study = Path(study)
    require(not list((study / "runs").glob("E*")), "evaluation already started")
    by_condition = {
        (r["source"], r["workers"], r["method"]): r for r in calibration if r["status"] == "complete"
    }
    for blocks in (6, 3):
        cells = schedule(selected, rows, blocks)
        populations = read_json(study / "protocol.json")["population_rows"]
        if any(
            2080 + LIMITS["max_rows"] + rows[s] * (blocks if s == SOURCES[0] else 3) > populations[s]
            for s in SOURCES
        ):
            continue
        required_s, body = 0.0, 0.0
        for cell in cells:
            r = by_condition[(cell["source"], cell["workers"], cell["method"])]
            feasible([r], cell["rows"])
            ratio = cell["rows"] / r["original_rows"]
            required_s += estimate(r, cell["rows"], "elapsed_s")
            body += r["body_bytes"] * ratio
        # Lifecycle forecast comes from live setup, never hidden in goodput.
        overhead = used["lifecycle_seconds_per_job"] * len(cells)
        forecast = 1.25 * (required_s + overhead) + 10800
        requests = sum(c["rows"] for c in cells)
        if (
            forecast <= remaining_seconds
            and used["body_bytes"] + 1.25 * body <= LIMITS["body_bytes"]
            and used["requests"] + requests <= LIMITS["max_requests"]
            and used["retained_bytes"] + 1.25 * body <= LIMITS["archive_bytes"]
        ):
            record = dict(
                utc=utc(),
                blocks=blocks,
                cells=cells,
                source_rows=rows,
                selected=selected,
                forecast_seconds=forecast,
                forecast_body_bytes=body,
                basis_runs=[r["run_id"] for r in calibration],
                margin=1.25,
                forecast_models={r["run_id"]: r.get("forecast", {}) for r in calibration},
            )
            write_new(study / "freeze.json", record)
            return record
    raise ValueError("neither 27 nor36 long evaluation jobs fits the remaining approved envelope")
