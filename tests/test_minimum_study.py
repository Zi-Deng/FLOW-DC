"""Offline regressions for long public studies; never scientific image requests."""

import io
import json
import multiprocessing
import os
import subprocess
import sys
import tarfile
import tempfile
import time
import unittest
from unittest.mock import patch
from pathlib import Path

import polars as pl

from benchmark.real_server_study import minimum_study as design
from benchmark.real_server_study.common import write_new
from benchmark.real_server_study.minimum_native import pack, unpack
from benchmark.real_server_study.public_guard import SharedBodyBudget
from benchmark.real_server_study.public_launch import merge_audits
from benchmark.real_server_study.public_metrics import transport


def reserve(path):
    return SharedBodyBudget(path, 10).update(reserve=6)


def native_shard(arguments):
    import email.message
    from PIL import Image

    wrapped, row = arguments
    buf = io.BytesIO()
    Image.new("RGB", (8, 8), "green").save(buf, format="JPEG")
    raw = buf.getvalue()

    class Response(io.BytesIO):
        code = 200
        headers = email.message.Message()

    with patch("urllib.request.OpenerDirector.open", side_effect=lambda *args, **kwargs: Response(raw)):
        return wrapped(row)


class MinimumStudyTests(unittest.TestCase):
    def test_exhausted_cloud_envelope_records_blocker_without_activation(self):
        from types import SimpleNamespace
        from unittest.mock import Mock

        sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bin"))
        import flowdc_ops
        from benchmark.real_server_study import minimum_cloud

        for exhausted_windows in (False, True):
            with (
                self.subTest(exhausted_windows=exhausted_windows),
                tempfile.TemporaryDirectory() as directory,
            ):
                packet = Path(directory)
                write_new(packet / "operator.json", {})
                write_new(packet / "deployment.json", dict(limits=design.LIMITS))
                if exhausted_windows:
                    for number in range(design.LIMITS["max_windows"]):
                        write_new(packet / f"window-{number:02}.json", dict(cleaned=True))
                journal = SimpleNamespace(read=lambda: dict(profile_path="test", vms={}))
                controller = Mock()
                with (
                    patch.object(
                        minimum_cloud.public_cloud,
                        "authorize",
                        return_value=(journal, dict(clock=dict(utc=time.time() - 86400))),
                    ),
                    patch.object(flowdc_ops, "load_profile", return_value={}),
                    patch.object(minimum_cloud, "exposure", return_value=0),
                    patch.object(minimum_cloud.cloud, "controller_for", return_value=controller),
                ):
                    result = minimum_cloud.run(packet)
                self.assertEqual(result["status"], "blocked")
                self.assertFalse(result["collected"])
                self.assertTrue((packet / "complete.json").exists())
                controller.call.assert_not_called()

    def test_affine_planning_separates_sixty_second_startup(self):
        records = [
            dict(
                run_id=str(n),
                phase="engineering",
                source=design.SOURCES[0],
                method=design.METHODS[0],
                workers=1,
                rows=n,
                original_rows=n,
                status="complete",
                elapsed_s=60 + n / 150,
                native_seconds=40 + n / 150,
                http_span_s=5 + n / 150,
                body_bytes=n * 1000,
            )
            for n in (512, 2048)
        ]
        fitted = design.modeled(records)
        rows = design.sizing([fitted[-1]])
        self.assertGreaterEqual(design.estimate(fitted[-1], rows, "http_span_s"), 720)
        self.assertLess(design.estimate(fitted[-1], rows, "elapsed_s"), 800)
        self.assertAlmostEqual(fitted[-1]["forecast"]["elapsed_s"]["intercept"], 60)
        # Confirmation outcomes cannot affect future planning models.
        confirmation = {**records[-1], "phase": "evaluation", "elapsed_s": 99999}
        self.assertEqual(design.modeled(records + [confirmation])[0]["forecast"], fitted[0]["forecast"])

    def test_queue_uses_matching_workers_and_persists_correction_size(self):
        from benchmark.real_server_study.minimum_queue import next_cell

        with tempfile.TemporaryDirectory() as directory:
            study = Path(directory)
            (study / "runs").mkdir()
            (study / "gates").mkdir()
            write_new(study / "protocol.json", dict(population_rows={s: 2000000 for s in design.SOURCES}))
            for source in design.SOURCES:
                write_new(study / "gates" / (source + ".json"), dict(requests=32, probe_body_bytes=3200))
            sizes = {}
            for index in range(46):
                if index == 30:
                    self.assertEqual(next_cell(study, 86400, 120, False), dict(phase="observe_cleanup"))
                    # An insufficient envelope refuses before spending on any
                    # long calibration, retaining the refusal receipt.
                    from benchmark.real_server_study.minimum_queue import campaign_forecast

                    records = [json.loads(p.read_text()) for p in (study / "runs").glob("*/record.json")]
                    with tempfile.TemporaryDirectory() as failed:
                        failure_study = Path(failed)
                        (failure_study / "gates").mkdir()
                        with self.assertRaises(ValueError):
                            campaign_forecast(
                                failure_study,
                                {r["run_id"]: r for r in design.modeled(records)},
                                {s: 15360 for s in design.SOURCES},
                                1000,
                                120,
                            )
                        self.assertFalse(
                            json.loads((failure_study / "gates/campaign-feasibility.json").read_text())[
                                "accepted"
                            ]
                        )
                cell = next_cell(study, 86400, 120)
                self.assertNotEqual(cell["phase"], "evaluation")
                rows = cell["rows"]
                if cell["phase"] == "engineering":
                    span = rows / (20 if cell["workers"] == 1 else 30)
                    elapsed = span + 10
                    self.assertEqual(cell["job_seconds"], 600)
                elif cell["run_id"].startswith("correction-"):
                    sizes.setdefault("correction", rows)
                    self.assertEqual(rows, sizes["correction"])
                    # Different correction rates must not resize later arms.
                    span = 610 + 20 * (index % 3)
                    elapsed = span + 100
                else:
                    sizes.setdefault(cell["source"], rows)
                    self.assertEqual(rows, 15360)  # w1 rate20, never w2 rate30.
                    span = 768 if cell["workers"] == 1 else 500
                    elapsed = span + 80
                run = study / "runs" / cell["run_id"]
                run.mkdir()
                write_new(
                    run / "record.json",
                    dict(
                        **cell,
                        status="complete",
                        original_rows=rows,
                        http_span_s=span,
                        elapsed_s=elapsed,
                        native_seconds=span,
                        body_bytes=rows * 1000,
                        coverage=1,
                        goodput_Bps=rows * 1000 / elapsed,
                    ),
                )
            cell = next_cell(study, 86400, 120)
            self.assertEqual(cell["phase"], "evaluation")
            frozen = json.loads((study / "freeze.json").read_text())
            self.assertEqual(frozen["source_rows"][design.SOURCES[0]], sizes["correction"])
            self.assertEqual(len(frozen["cells"]), 36)

    def test_sizing_refuses_native_deadline_body_and_worker_mismatch(self):
        record = dict(
            status="complete",
            workers=1,
            original_rows=2048,
            http_span_s=100,
            elapsed_s=110,
            native_seconds=105,
            body_bytes=2048000,
        )
        self.assertEqual(design.sizing([record]), 15360)
        for change in (dict(workers=2), dict(native_seconds=500), dict(body_bytes=20 * 2**30)):
            with self.assertRaises(ValueError):
                design.sizing([{**record, **change}])

    def test_measured_analysis_keeps_missing_and_changed_content_visible(self):
        from benchmark.real_server_study.minimum_analysis import summarize

        with tempfile.TemporaryDirectory() as directory:
            study = Path(directory)
            cells = []
            for block in range(3):
                for method in design.METHODS:
                    cell = dict(run_id=f"E1-{block}-{method}", family="E1", block=block, method=method)
                    cells.append(cell)
                    if block == 2 and method == design.METHODS[0]:
                        continue  # The planned first attempt remains unstarted.
                    run = study / "runs" / cell["run_id"]
                    (run / "worker").mkdir(parents=True)
                    (run / "input.parquet").write_bytes(b"same frozen work")
                    seconds = 10 if method == design.METHODS[2] else 5
                    write_new(
                        run / "record.json",
                        dict(
                            **cell,
                            phase="evaluation",
                            status="complete",
                            workers=1,
                            original_rows=1,
                            credited_bytes=100,
                            elapsed_s=seconds,
                            goodput_Bps=100 / seconds,
                            coverage=1,
                            mean_job_concurrency=1,
                        ),
                    )
                    digest = "changed" if block == 1 and method == design.METHODS[0] else "original"
                    write_new(
                        run / "worker/verification.json",
                        dict(
                            original_rows=1,
                            credited_bytes=100,
                            payloads={"row": dict(sha256=digest, bytes=100)},
                        ),
                    )
            write_new(study / "freeze.json", dict(cells=cells))
            result = summarize(study)
            self.assertEqual((result["planned_jobs"], result["complete_jobs"]), (9, 8))
            ratio = result["comparisons"]["E1:" + design.METHODS[0]]
            self.assertEqual(ratio["blocks"], 1)
            self.assertEqual(ratio["ratio"], 2)
            self.assertEqual(ratio["changed_content_blocks"], [dict(block=1, rows=1)])
            self.assertIsNone(ratio["interval95"])
            grad = result["comparisons"]["E1:" + design.METHODS[1]]
            self.assertEqual(grad["blocks"], 3)
            self.assertEqual(grad["interval95"], [2, 2])
            self.assertIsNone(grad["p_greater"])

    def isolated_spawn(self):
        if os.environ.get("FLOWDC_MINIMUM_TEST_CHILD") == "1":
            return False
        # multiprocessing's persistent resource tracker otherwise leaks into
        # later subreaper/owned-worker tests in unittest discovery.
        env = dict(
            os.environ, FLOWDC_MINIMUM_TEST_CHILD="1", PYTHONPATH=str(Path(__file__).resolve().parents[1])
        )
        result = subprocess.run(
            [sys.executable, str(Path(__file__).resolve()), "MinimumStudyTests." + self._testMethodName],
            env=env,
            capture_output=True,
            text=True,
            timeout=45,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return True

    def test_shared_reservations_across_spawned_processes(self):
        if self.isolated_spawn():
            return
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "counter"
            counter = SharedBodyBudget.create(path, 10)
            with multiprocessing.get_context("spawn").Pool(2) as pool:
                values = pool.map(reserve, [path, path])
            self.assertEqual(sum(not value[2] for value in values), 1)
            self.assertEqual(counter.update(release=6, received=3), (3, 0, "body_budget"))
            with self.assertRaises(ValueError):
                counter.update(release=1)

    def test_static_partition_and_native_tool_shards(self):
        frame = pl.DataFrame(
            {"url": [str(i) for i in range(100)], "study_row_id": [str(i) for i in range(100)]}
        )
        for workers in (1, 2):
            for method in design.METHODS:
                cfg = design.configuration(
                    "/tmp/example", dict(workers=workers, method=method, tool_setting=[8, 24]), frame
                )
                if method == design.METHODS[2]:
                    self.assertEqual(cfg["thread_count"] * cfg["processes_count"] * workers, 192)
                    self.assertGreaterEqual(
                        (100 + cfg["number_sample_per_shard"] - 1) // cfg["number_sample_per_shard"], 8
                    )
                    self.assertEqual((cfg["retries"], cfg["max_shard_retry"]), (0, 0))
                else:
                    self.assertEqual(cfg["C_max"] * workers, 192)
                    self.assertEqual(cfg["research_workload"], design.PROFILE)
                    self.assertEqual(cfg["method_options"], design.OPTIONS)
            parts = [frame.gather_every(workers, offset=i) for i in range(workers)]
            self.assertEqual(pl.concat(parts)["study_row_id"].n_unique(), frame.height)

    def test_matched_schedule_balances_order_and_work(self):
        selected = {s: [8, 24] for s in design.SOURCES}
        rows = {s: 131072 for s in design.SOURCES}
        for blocks, total in ((3, 27), (6, 36)):
            cells = design.schedule(selected, rows, blocks)
            self.assertEqual(len(cells), total)
            self.assertEqual(len({c["run_id"] for c in cells}), total)
            for family in ("E1", "E2", "E3"):
                chosen = [c for c in cells if c["family"] == family]
                for block in range(blocks if family == "E1" else 3):
                    group = [c for c in chosen if c["block"] == block]
                    self.assertEqual({c["method"] for c in group}, set(design.METHODS))
                    self.assertEqual(len({c["rows"] for c in group}), 1)

    def test_forecast_refuses_insufficient_budget_and_existing_results(self):
        with tempfile.TemporaryDirectory() as directory:
            study = Path(directory)
            (study / "runs").mkdir()
            write_new(study / "protocol.json", dict(population_rows={s: 2000000 for s in design.SOURCES}))
            calibration = []
            for source, workers in ((design.SOURCES[0], 1), (design.SOURCES[1], 1), (design.SOURCES[0], 2)):
                for method in design.METHODS:
                    calibration.append(
                        dict(
                            source=source,
                            workers=workers,
                            method=method,
                            status="complete",
                            original_rows=1000,
                            elapsed_s=1000,
                            body_bytes=1000,
                            run_id=f"{source}-{workers}-{method}",
                        )
                    )
            used = dict(body_bytes=0, retained_bytes=0, requests=0, lifecycle_seconds_per_job=120)
            selected = {s: [8, 24] for s in design.SOURCES}
            rows = {s: 1000 for s in design.SOURCES}
            with self.assertRaises(ValueError):
                design.freeze(study, calibration, selected, rows, 1000, used)
            record = design.freeze(study, calibration, selected, rows, 52000, used)
            self.assertEqual(record["blocks"], 3)
            (study / "runs/E1-started").mkdir()
            with self.assertRaises(ValueError):
                design.freeze(study, calibration, selected, rows, 86400, used)

    def test_pack_keeps_journals_and_unpack_rejects_traversal(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run = root / "run"
            (run / "native/.flowdc").mkdir(parents=True)
            (run / "native.tar").write_bytes(b"retained archive")
            (run / "native/photo").write_bytes(b"loose duplicate")
            (run / "native/photo.json").write_text("{}")
            (run / "native/.flowdc/measurement.json").write_text("{}")
            write_new(
                run / "native/outcome-index.json",
                dict(rows=[dict(disposition="verified", payload="photo", metadata="photo.json")]),
            )
            archive = root / "return.tar"
            pack(run, archive)
            unpack(archive, root / "returned", time.monotonic() + 10)
            self.assertTrue((root / "returned/native/.flowdc/measurement.json").exists())
            self.assertFalse((root / "returned/native/photo").exists())
            self.assertTrue((run / "native/photo").exists())  # no premature deletion
            with tarfile.open(root / "bad.tar", "w") as tar:
                info = tarfile.TarInfo("../escape")
                info.size = 1
                tar.addfile(info, io.BytesIO(b"x"))
            with self.assertRaises(ValueError):
                unpack(root / "bad.tar", root / "bad-return", time.monotonic() + 10)

    def test_merged_common_boundary_and_duplicate_refusal(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = []
            for i in range(2):
                events = [dict(event="start", monotonic_s=1)]
                events += [
                    dict(event="attempt", monotonic_s=2 + i, token=str(i), url=str(i)),
                    dict(event="http_start", monotonic_s=2 + i, token=str(i)),
                    dict(event="dispatch", monotonic_s=2 + i, token=str(i), inflight=1),
                    dict(event="http_first_byte", monotonic_s=3 + i, token=str(i)),
                    dict(event="network_end", monotonic_s=5 + i, token=str(i), inflight=0, dispatched=True),
                    dict(
                        event="closed",
                        monotonic_s=7,
                        active=0,
                        body_bytes=5,
                        logical_attempts=1,
                        stop_reason=None,
                    ),
                ]
                path = root / str(i)
                path.write_text("".join(json.dumps(e) + "\n" for e in events))
                paths.append(path)
            output = root / "merged"
            with output.open("w") as stream:
                merge_audits(paths, stream, 2)
            metrics = transport(output, 2)
            self.assertEqual(
                (metrics["attempts"], metrics["common_peak"], metrics["request_seconds"]), (2, 2, 6)
            )
            self.assertEqual(metrics["mean_http_concurrency"], 1.5)
            with (root / "bad").open("w") as stream, self.assertRaises(ValueError):
                merge_audits([paths[0], paths[0]], stream, 2)

    def test_actual_spawned_tool_shards_share_budget_and_verify_original_rows(self):
        try:
            import importlib

            native = importlib.import_module("img2dataset.main")
            from img2dataset.reader import Reader
        except ImportError:
            self.skipTest("optional pinned tool runtime")
        if self.isolated_spawn():
            return
        from benchmark.real_server_study.public_launch import GuardedDownloader
        from benchmark.real_server_study.public_verify import verify

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "native").mkdir()
            (root / "temp").mkdir()
            frame = pl.DataFrame(
                dict(
                    url=[f"https://open-images-dataset.s3.amazonaws.com/train/{i}.jpg" for i in range(24)],
                    study_row_id=[str(i) for i in range(24)],
                    author=["test"] * 24,
                    license=["CC-BY"] * 24,
                    landing_url=["test"] * 24,
                    record_id=[str(i) for i in range(24)],
                )
            )
            frame.write_parquet(root / "input.parquet")
            reader = Reader(
                str(root / "input.parquet"),
                "parquet",
                "url",
                None,
                None,
                None,
                [c for c in frame.columns if c != "url"],
                6,
                set(),
                str(root / "temp"),
            )
            resizer = native.Resizer(
                image_size=256,
                resize_mode="no",
                resize_only_if_bigger=False,
                upscale_interpolation="lanczos",
                downscale_interpolation="area",
                encode_quality=95,
                encode_format="jpg",
                skip_reencode=False,
                disable_all_reencoding=True,
                min_image_size=0,
                max_image_area=float("inf"),
                max_aspect_ratio=float("inf"),
                blurrer=None,
            )
            downloader = native.Downloader(
                sample_writer_class=native.WebDatasetSampleWriter,
                resizer=resizer,
                thread_count=2,
                save_caption=False,
                extract_exif=False,
                output_folder=str(root / "native"),
                column_list=reader.column_list,
                timeout=30,
                number_sample_per_shard=6,
                oom_shard_count=5,
                compute_hash="sha256",
                verify_hash_type=None,
                encode_format="jpg",
                retries=0,
                user_agent_token="flowdc-public-study",
                disallowed_header_directives=[],
            )
            counter = SharedBodyBudget.create(root / "budget", 2**20)
            wrapped = GuardedDownloader(
                downloader,
                "openimages-cvdf",
                frame["url"].to_list(),
                str(root / "audit"),
                dict(baseline_s=10, ignore_time=False),
                2,
                2**20,
                6,
                str(root / "budget"),
            )
            with multiprocessing.get_context("spawn").Pool(2) as pool:
                results = pool.map(native_shard, [(wrapped, row) for row in reader])
            self.assertTrue(all(r[0] for r in results))
            with (root / "request-audit.jsonl").open("w") as out:
                merge_audits(sorted(root.glob("tool-request-*.jsonl")), out, 4)
            observed = transport(root / "request-audit.jsonl", 4)
            self.assertEqual(observed["attempts"], 24)
            self.assertEqual(counter.update()[0], observed["body_bytes"])
            verified = verify(root, Path(__file__).resolve().parents[1], "img2dataset-1.47.0", 0, 1)
            self.assertEqual(verified["coverage"], 1)
            self.assertEqual(verified["credited_bytes"], observed["body_bytes"])

    def test_flow_guard_complete_archive_and_common_timing(self):
        import contextlib
        import threading
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        from PIL import Image
        from benchmark.real_server_study import public_launch, public_verify

        repo = Path(__file__).resolve().parents[1]
        sys.path.insert(0, str(repo / "bin"))
        raw = io.BytesIO()
        Image.new("RGB", (8, 8), "blue").save(raw, format="JPEG")
        body = raw.getvalue()

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as directory:
                study = Path(directory)
                run = study / "runs/fixture"
                run.mkdir(parents=True)
                (study / "gates").mkdir()
                frame = pl.DataFrame(
                    dict(
                        url=[f"http://127.0.0.1:{server.server_port}/{i}.jpg" for i in range(16)],
                        study_row_id=[str(i) for i in range(16)],
                        author=["test"] * 16,
                        license=["CC-BY"] * 16,
                        landing_url=["test"] * 16,
                        record_id=[str(i) for i in range(16)],
                    )
                )
                frame.write_parquet(run / "input.parquet")
                cell = dict(
                    run_id="fixture",
                    workers=1,
                    method="ratio-v2",
                    source="openimages-cvdf",
                    workload=design.PROFILE,
                )
                cfg = design.configuration(run, cell, frame)
                write_new(run / "config.json", cfg)
                write_new(run / "flowdc-config.json", cfg)
                write_new(run / "attempt.json", cell)
                write_new(study / "protocol.json", {})
                write_new(study / "execution.json", dict(source_root=str(repo)))
                write_new(study / "gates/openimages-cvdf.json", dict(baseline_s=10, ignore_time=False))
                before = time.monotonic()
                with (
                    patch.object(
                        sys, "argv", ["public_launch", "--study", str(study), "--run-id", "fixture"]
                    ),
                    patch.object(public_launch, "guest_check"),
                    patch("benchmark.real_server_study.public_guard.checked_url", side_effect=lambda u, s: u),
                    contextlib.redirect_stdout(io.StringIO()),
                ):
                    public_launch.main()
                after = time.monotonic()
                observed = transport(run / "request-audit.jsonl", 192)
                self.assertEqual(observed["attempts"], 16)
                verified = public_verify.verify(run, repo, "ratio-v2", before, after)
                self.assertEqual(verified["coverage"], 1)
                self.assertEqual(verified["credited_bytes"], 16 * len(body))
                self.assertGreater(observed["common_ttfb_p50_s"], 0)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
