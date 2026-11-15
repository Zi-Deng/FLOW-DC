"""One acquisition pipeline for local and staged TaskVine jobs."""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import tarfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import aiohttp
import polars as pl
from tqdm.asyncio import tqdm

from . import integrity
from .config import DownloadConfig as Config
from .config import normalize_config
from .controllers import CandidateController, ControllerManager, ControlTrace
from .http import HTTP_MEASUREMENT_VERSION, OUTPUT_CTX, HTTPTraceConfig, download_single
from .http import HTTP_TRACE_CTX as TRACE_CTX
from .manifests import load_manifest, resolve_worker_count, validate_and_load
from .naming import SequentialNamer, render_filename
from .reports import generate_overview_report

shutdown_flag = False


def _signal_handler(sig, frame):
    global shutdown_flag
    print("\n[Shutdown] Interrupt received. Attempting graceful shutdown...")
    shutdown_flag = True


def _setup_signal_handlers():
    """Register signal handlers. Called from main() to avoid side effects on import."""
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)


def _now() -> float:
    """Current time in seconds (monotonic for intervals, time.time for timestamps)."""
    return time.time()


def _monotonic() -> float:
    """Monotonic time for duration measurements."""
    return time.monotonic()


def _is_connection_error(msg: Any) -> bool:
    """Check if an error message indicates a connection-level failure."""
    if msg is None:
        return False
    s = str(msg).lower()
    patterns = [
        "connection reset by peer",
        "server disconnected",
        "connection refused",
        "cannot connect",
        "connection aborted",
        "broken pipe",
        "timeout",
        "timed out",
    ]
    return any(p in s for p in patterns)


def _is_retryable(status_code: int | None, error: Any) -> bool:
    """Determine if a request failure is retryable."""
    if status_code == 429 or status_code == 408:
        return True
    if status_code is not None and status_code >= 500:
        return True
    return _is_connection_error(error)


async def controller_loop(manager: ControllerManager) -> None:
    """
    BBR-consistent control loop with per-host adaptive timing.
    """
    host_next_step: dict[str, float] = {}  # host -> next step time

    while not shutdown_flag:
        now = time.monotonic()
        controllers = await manager.all_controllers()

        for ctrl in controllers:
            host = ctrl.host
            next_time = host_next_step.get(host, 0)

            if now >= next_time:
                try:
                    snap = await ctrl.step_interval()

                    # Use the controller's state-aware, sample-aware scheduling helper.
                    interval = ctrl._calculate_control_interval(snap)

                    host_next_step[host] = now + interval

                except Exception as e:
                    if hasattr(manager, "failure"):
                        manager.failure = e
                        return  # New methods fail closed at subsequent HTTP dispatch.
                    print(f"[PAARC] Error for {host}: {e}")
                    host_next_step[host] = now + 0.2  # Fallback

        # Sleep until next host needs attention (or minimum check interval)
        if host_next_step:
            next_wakeup = min(host_next_step.values())
            sleep_time = max(0.01, next_wakeup - time.monotonic())
        else:
            sleep_time = 0.2

        await asyncio.sleep(sleep_time)


@dataclass
class DownloadOutcome:
    """Result of a single download attempt."""

    key: str
    url: str
    success: bool
    file_path: str | None
    class_name: str | None
    status_code: int | None
    error: str | None
    bytes_downloaded: int = 0


async def download_one(
    *,
    row: dict,
    cfg: Config,
    session: aiohttp.ClientSession,
    total_bytes: list[int],
    manager: ControllerManager | None,
    sequential_namer: SequentialNamer,
    global_written_paths: dict[str, str],
    store=None,
) -> DownloadOutcome:
    """Execute a single download with PAARC control."""
    url = str(row[cfg.url_column]).strip()
    key = str(row.get("__key__", ""))
    class_name = (
        str(row[cfg.label_column])
        if cfg.label_column is not None and row.get(cfg.label_column) is not None
        else None
    )

    try:
        directory = store.begin(key) if store is not None else None
    except (OSError, ValueError) as exc:
        # Only a durable row rejection can become an ordinary failed outcome.
        # A journal/storage failure must abort this invocation before HTTP; if
        # swallowed here it can leave an unchanged budget and retry forever.
        if store is None or not store.fs.exists(f".flowdc/rejections/{key}.json"):
            raise
        return DownloadOutcome(key, url, False, None, class_name, None, str(exc))
    # Determine filename
    if store is not None:
        filename_override = Path(store.rows[key]["payload"]).name
    elif cfg.naming_mode == "sequential":
        filename_override = await sequential_namer.filename_for(key, url)
    else:
        filename_override = render_filename(cfg.file_name_pattern, url, key)

    # Get host controller if PAARC enabled
    ctrl: CandidateController | None = None
    if manager is not None:
        ctrl = await manager.get_controller(url)

    # Acquire concurrency slot
    if ctrl is not None:
        await ctrl.semaphore.acquire()

    # Set up tracing context
    trace_dict: dict[str, Any] = {}
    if manager is not None and hasattr(manager, "check_health"):
        trace_dict["dispatch_check"] = manager.check_health
    if manager is not None:

        async def redirect_admit(destination: str) -> None:
            nonlocal ctrl
            next_ctrl = await manager.get_controller(destination)
            if next_ctrl is not ctrl:
                # Release before acquiring: reciprocal A->B / B->A redirects
                # must not hold both authorities' permits and deadlock.
                if ctrl is not None:
                    await ctrl.semaphore.release()
                    ctrl = None
                await next_ctrl.semaphore.acquire()
                ctrl = next_ctrl
            if ctrl.smoother is not None:
                await ctrl.smoother.acquire()

        trace_dict["redirect_admit"] = redirect_admit
    trace_token = TRACE_CTX.set(trace_dict)
    output_token = OUTPUT_CTX.set((store, directory, key) if store is not None else None)

    try:
        # Apply smoothing if available
        if ctrl is not None and ctrl.smoother is not None:
            await ctrl.smoother.acquire()

        # Execute download
        k, file_path, cls, err, status, retry_after_sec = await download_single(
            url=url,
            key=key,
            class_name=class_name,
            output_dir=cfg.output_dir,
            output_format=cfg.output_format,
            session=session,
            timeout=cfg.timeout_s,
            filename=filename_override,
            # Do not credit the shared tally until the final lookup succeeds.
            total_bytes=[],
        )

        # A missing output or failed size lookup is a local failed acquisition,
        # not a successful zero-byte output. Preserve the completed HTTP timing.
        bytes_dl = 0
        if err is None:
            try:
                if not file_path:
                    raise OSError("No saved output path")
                if store is None:
                    bytes_dl = os.path.getsize(file_path)
                else:
                    record = store.fs.json(f".flowdc/commits/{key}.json")
                    bytes_dl = record["payload_bytes"]
            except Exception as exc:
                err = f"Output size verification failed: {exc}"
                trace_dict["failure_kind"] = "local"
            else:
                total_bytes.append(bytes_dl)

        if store is not None:
            store.measure(directory, key, trace_dict, status, HTTP_MEASUREMENT_VERSION)
        if store is not None and err is not None:
            store.fail(
                directory,
                key,
                error=err,
                status=status,
                retryable=_is_retryable(status, err)
                and trace_dict.get("failure_kind") not in ("local", "unknown"),
                observed_bytes=trace_dict.get("observed_response_body_bytes", 0),
                latency_eligible=trace_dict.get("latency_eligible", False),
                body_complete=trace_dict.get("body_completed_at") is not None,
            )
        # Record metrics
        failure_kind = trace_dict.get("failure_kind")
        is_unknown_error = failure_kind == "unknown"
        is_conn_error = failure_kind == "transport" or (failure_kind is None and _is_connection_error(err))
        is_local_error = failure_kind in ("local", "admission") or (
            err is not None and not is_conn_error and not is_unknown_error and status in (None, 200)
        )
        if manager is not None:
            # Each redirected hop acquires its destination's permit. Final
            # response feedback belongs to the controller actually holding it.
            # A normalized trace URL (default port / IDNA) may otherwise select
            # another legacy host key even without a redirect. Only fall back to
            # the target lookup when admission failed before acquiring a permit.
            feedback_ctrl = (
                ctrl
                if ctrl is not None
                else await manager.get_controller(trace_dict.get("feedback_url", url))
            )
            await feedback_ctrl.metrics.record(
                status_code=status,
                ttfb=trace_dict.get("ttfb"),
                bytes_downloaded=bytes_dl,
                is_conn_error=is_conn_error,
                retry_after_sec=retry_after_sec,
                acquisition_success=(err is None),
                is_local_error=is_local_error,
                is_unknown_error=is_unknown_error,
                latency_eligible=trace_dict.get("latency_eligible"),
                dispatch_at=trace_dict.get("t0"),
            )

        # Track written paths for collision detection
        if err is None and file_path:
            prev = global_written_paths.get(file_path)
            if prev is not None and prev != key:
                print(f"[Warning] Filename collision: {file_path}")
            else:
                global_written_paths[file_path] = key

        return DownloadOutcome(
            key=str(k),
            url=url,
            success=(err is None),
            file_path=file_path,
            class_name=cls,
            status_code=status,
            error=str(err) if err is not None else None,
            bytes_downloaded=bytes_dl,
        )

    finally:
        OUTPUT_CTX.reset(output_token)
        TRACE_CTX.reset(trace_token)
        if ctrl is not None:
            await ctrl.semaphore.release()


async def download_batch_bounded(
    *,
    cfg: Config,
    session: aiohttp.ClientSession,
    df: pl.DataFrame,
    manager: ControllerManager | None,
    sequential_namer: SequentialNamer,
    global_written_paths: dict[str, str],
    effective_workers: int,
    store=None,
) -> dict[str, DownloadOutcome]:
    """
    Bounded batch download scheduler.

    Uses a queue with N worker tasks for bounded parallelism.

    Args:
        effective_workers: Number of worker coroutines (auto-detected or configured)
    """
    integrity.require(
        "__key__" in df.columns and df["__key__"].null_count() == 0 and df["__key__"].n_unique() == df.height,
        "duplicate or missing internal row identity",
    )
    q: asyncio.Queue[dict] = asyncio.Queue()
    for row in df.iter_rows(named=True):
        q.put_nowait(row)

    total_bytes: list[int] = []
    outcomes: dict[str, DownloadOutcome] = {}
    pbar = tqdm(total=df.height, desc="Downloading", unit="url")

    async def worker():
        while not shutdown_flag:
            try:
                row = q.get_nowait()
            except asyncio.QueueEmpty:
                return

            out = await download_one(
                row=row,
                cfg=cfg,
                session=session,
                total_bytes=total_bytes,
                manager=manager,
                sequential_namer=sequential_namer,
                global_written_paths=global_written_paths,
                store=store,
            )

            outcomes[out.key] = out
            q.task_done()
            pbar.update(1)

    workers = [asyncio.create_task(worker()) for _ in range(max(1, effective_workers))]

    async def cancel_on_shutdown():
        while not shutdown_flag:
            await asyncio.sleep(0.1)
        for task in workers:
            task.cancel()

    shutdown_task = asyncio.create_task(cancel_on_shutdown())
    try:
        await asyncio.gather(*workers)
    except asyncio.CancelledError:
        if not shutdown_flag:
            raise
    finally:
        shutdown_task.cancel()
        for task in workers:
            task.cancel()
        await asyncio.gather(shutdown_task, *workers, return_exceptions=True)
        pbar.close()

    return outcomes


def integrity_report(cfg, store, snapshot, elapsed, report_factory):
    outcomes = {}
    for row in snapshot["rows"]:
        outcomes[row["row_id"]] = DownloadOutcome(
            row["row_id"],
            str(store.rows[row["row_id"]]["url"]),
            row["disposition"] == "verified",
            row["payload"],
            None,
            row.get("status_code"),
            row["error"],
            row["payload_bytes"],
        )
    report = report_factory(cfg, outcomes, elapsed)
    from .controllers import control_records, method_record

    report["control_method"] = method_record(cfg)
    report["control_trajectories"] = control_records(store)
    report["http_observations"] = store.measurements()
    counts = snapshot["counts"]
    report["report_schema_version"] = integrity.SCHEMA
    report["summary"].update(
        total_urls=snapshot["original_rows"],
        successful_downloads=counts["verified"],
        failed_downloads=counts["failed"],
        skipped_rows=counts["skipped"],
        unattempted_rows=counts["unattempted"],
        unattempted_or_cancelled_urls=counts["unattempted"],
        verified_payload_bytes=snapshot["verified_payload_bytes"],
        unique_content_bytes=snapshot["unique_content_bytes"],
        artifact_file_bytes=snapshot["artifact_file_bytes"],
        observed_response_body_bytes=snapshot["observed_response_body_bytes"],
        observed_bytes_complete=snapshot["observed_bytes_complete"],
    )
    boundary = (
        "verified_uncompressed_archive"
        if cfg.research_profile
        else (
            "verified_archive"
            if cfg.output_format == "webdataset" and cfg.create_tar
            else "committed_local_files"
        )
    )
    report["output_integrity"] = {
        "schema_version": integrity.SCHEMA,
        "run_id": store.owner["run_id"],
        "manifest": snapshot["manifest"],
        "counts": counts,
        "completion_boundary": boundary,
        "run_complete": False,
        "useful_final_payload_bytes": 0,
        "outcome_index": "outcome-index.json",
        "outcome_index_sha256": integrity.digest(integrity.encode(snapshot)),
        "byte_semantics": "integer payload bytes; observed response bodies are decoded application bytes, not wire bytes",
        "artifact_file_bytes_scope": "committed payload and row-metadata files plus published archive, excluding reports and staging",
        "successful_downloads_semantics": "rows with verified committed local files",
        "control_trajectories_sha256": integrity.digest(integrity.encode(report["control_trajectories"])),
        "http_observations_sha256": integrity.digest(integrity.encode(report["http_observations"])),
        "display_mb_divisor": 1000000,
    }
    if cfg.resume or cfg.reconcile:
        report["summary"].update(elapsed_sec=None, avg_speed_MBps=None)
        report["output_integrity"]["elapsed_scope"] = (
            "not reconstructed across interruptions or offline reconciliation"
        )
    return report


def finalize_run(cfg, store, snapshot, elapsed, report_factory):
    store.fs.atomic(
        ".flowdc/final.json", {"run_complete": False, "run_id": store.owner["run_id"]}, replace=True
    )
    report = integrity_report(cfg, store, snapshot, elapsed, report_factory)
    store.emit("outcome-index.json", integrity.encode(snapshot))
    if cfg.create_overview:
        store.emit("overview.json", integrity.encode(report))
    tar_time, artifact_error = 0.0, None
    if cfg.create_tar and not shutdown_flag:
        try:
            started = _monotonic()
            path, size, sha256 = store.make_archive(snapshot, report, compress=cfg.compress_archive)
            tar_time = _monotonic() - started
            report["summary"]["tar_path"] = path
            report["summary"]["artifact_file_bytes"] += size
            report["output_integrity"]["archive"] = {
                "path": path,
                "bytes": size,
                "sha256": sha256,
                "verified": True,
            }
        except (OSError, ValueError, tarfile.TarError) as exc:
            artifact_error = str(exc)
            report["output_integrity"]["artifact_error"] = artifact_error
    boundary_ok = (
        report["output_integrity"]["completion_boundary"] == "committed_local_files"
        or "archive" in report["output_integrity"]
    )
    if boundary_ok:
        report["output_integrity"]["useful_final_payload_bytes"] = snapshot["verified_payload_bytes"]
    complete = (
        boundary_ok
        and not artifact_error
        and not shutdown_flag
        and not (snapshot["counts"]["failed"] or snapshot["counts"]["unattempted"])
    )
    report["output_integrity"]["run_complete"] = complete
    final_record = dict(report["output_integrity"])
    if cfg.create_overview:
        # Two reports and the completion record cannot be published atomically.
        # Reports reference the expected last record; only that record confirms
        # final useful-byte credit. A crash/report failure cannot leave a report
        # falsely declaring a completed run before the last publication.
        report["output_integrity"].update(
            run_complete=None,
            useful_final_payload_bytes=None,
            completion_record=".flowdc/final.json",
            completion_record_sha256=integrity.digest(integrity.encode(final_record)),
        )
        store.emit("overview.json", integrity.encode(report))
        store.emit(store.root.name + "_overview.json", integrity.encode(report), external=True)
    store.fs.atomic(".flowdc/final.json", final_record, replace=True)
    report["output_integrity"] = final_record
    print(f"Download time:         {elapsed:.2f}s")
    print(f"Tar creation time:     {tar_time:.2f}s")
    print(f"Total time:            {elapsed + tar_time:.2f}s")
    print(f"Reconciled rows:        {snapshot['counts']}")
    print(f"Completion record:     {store.root / '.flowdc/final.json'}")
    if artifact_error:
        raise integrity.IntegrityError("Archive/report finalization failed: " + artifact_error)
    return report


async def run_acquisition(cfg, *, report_factory=None):
    """Shared acquisition/reconciliation runner for both controller variants."""
    global shutdown_flag
    shutdown_flag = False
    cfg = normalize_config(cfg)
    if report_factory is None:

        def report_factory(config, outcomes, elapsed):
            return generate_overview_report(
                cfg=config, df_total=len(outcomes), outcomes=outcomes, elapsed_sec=elapsed
            )

    if cfg.reconcile:
        # Offline mode needs only owned output. It neither loads input nor opens
        # an HTTP session; effective acquisition settings come from ownership.
        from dataclasses import fields, replace

        with integrity.RunStore(cfg.output_dir) as store:
            integrity.require(
                store.owner["config"]["implementation"] == "flowdc-package-v1",
                "unsupported historical implementation: use its original runtime for recovery",
            )
            accepted = {f.name for f in fields(cfg)} - {
                "input_path",
                "output_dir",
                "force",
                "resume",
                "reconcile",
            }
            cfg = replace(cfg, **{k: v for k, v in store.owner["config"]["values"].items() if k in accepted})
            if store.fs.exists("overview.json"):
                previous = store.fs.json("overview.json")
                if previous.get("output_integrity", {}).get("run_id") == store.owner["run_id"]:
                    original_factory = report_factory

                    def report_factory(config, outcomes, elapsed):
                        report = original_factory(config, outcomes, elapsed)
                        for field in ("controller_variant", "gradient_summary", "gradient_summary_scope"):
                            if field in previous:
                                report[field] = previous[field]
                        return report

            snapshot = store.reconcile()
            return finalize_run(cfg, store, snapshot, 0.0, report_factory)
    prepared = load_manifest(cfg)
    df, manifest = prepared
    rows = integrity.plan_rows(df, cfg, render_filename)
    # Finish all pure validation and provenance checks before destructive consent.
    validate_and_load(cfg, prepared=prepared)
    config = integrity.effective_config(cfg)
    store = integrity.RunStore(
        cfg.output_dir, manifest=manifest, config=config, rows=None if cfg.resume else rows
    )
    manager, ctrl_task, trajectory = None, None, None
    started = _monotonic()
    try:
        integrity.require(store.owner["rows"] == rows, "resume row provenance or publication plan mismatch")
        snapshot = store.reconcile()
        keys = store.eligible(snapshot)
        if keys and not shutdown_flag:
            current_df = df.filter(pl.col("__key__").is_in(keys))
            effective_workers = resolve_worker_count(cfg, current_df)
            trajectory = ControlTrace(store, cfg)
            manager = ControllerManager(cfg, trajectory.emit)
            ctrl_task = asyncio.create_task(controller_loop(manager))
            connector = aiohttp.TCPConnector(
                limit=cfg.http_connection_limit or max(50, int(effective_workers * 1.1)), ttl_dns_cache=300
            )
            async with aiohttp.ClientSession(
                connector=connector,
                timeout=aiohttp.ClientTimeout(total=max(1, cfg.timeout_s * 2)),
                headers={"User-Agent": "FLOW-DC/3.0"},
                trace_configs=[HTTPTraceConfig()],
            ) as session:
                namer, written = SequentialNamer(), {}
                while keys and not shutdown_flag:
                    selected = set(keys)
                    attempt = (
                        max(row["attempt_intents"] for row in snapshot["rows"] if row["row_id"] in selected)
                        + 1
                    )
                    print(f"[Attempt {attempt}] Processing {len(keys)} URLs...")
                    await download_batch_bounded(
                        cfg=cfg,
                        session=session,
                        df=current_df,
                        manager=manager,
                        sequential_namer=namer,
                        global_written_paths=written,
                        effective_workers=effective_workers,
                        store=store,
                    )
                    if shutdown_flag:
                        # The common final boundary reconciles all rows below.
                        # Repeating this full archive/journal scan after a planned
                        # stop adds substantial cost on the research volume.
                        break
                    snapshot = store.reconcile()
                    keys = store.eligible(snapshot)
                    if keys and not shutdown_flag:
                        await asyncio.sleep(cfg.retry_backoff_s)
                        current_df = df.filter(pl.col("__key__").is_in(keys))
        snapshot = store.reconcile()
        if ctrl_task is not None:
            ctrl_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await ctrl_task
            ctrl_task = None
        if trajectory is not None:
            # Include the final short interval and close retained trajectories
            # before publishing the result boundary.
            if manager is not None:
                manager.check_health()
                for controller in await manager.all_controllers():
                    await controller.step_interval()
            trajectory.close(complete=not shutdown_flag)
        return finalize_run(cfg, store, snapshot, _monotonic() - started, report_factory)
    finally:
        if ctrl_task is not None:
            ctrl_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await ctrl_task
        try:
            if trajectory is not None:
                trajectory.close()
        finally:
            store.close()
