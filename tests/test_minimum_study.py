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
