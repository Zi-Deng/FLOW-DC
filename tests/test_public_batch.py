"""Offline software checks; these fixtures are not scientific acquisitions."""

import email.message
import io
import pickle
import tempfile
import time
import unittest
import urllib.request
from pathlib import Path
from unittest.mock import patch

import polars as pl
from PIL import Image

from benchmark.real_server_study import public_batch as batch
from benchmark.real_server_study import public_guard as guard
from benchmark.real_server_study import public_verify as verifier
from benchmark.real_server_study.common import write_new
from benchmark.real_server_study.public_launch import GuardedDownloader

URL = "https://open-images-dataset.s3.amazonaws.com/train/a.jpg"


class Response(io.BytesIO):
    code = 200

    def __init__(self, data, status=200):
        super().__init__(data)
        self.code = status
        self.headers = email.message.Message()


class PublicBatchTests(unittest.TestCase):
    def test_order_disjoint_pools_and_schedule(self):
        ordered = batch.order()
        self.assertEqual(len(ordered), 4)
        for position in range(4):
            self.assertEqual({row[position] for row in ordered}, set(batch.METHODS))
        with tempfile.TemporaryDirectory() as d:
            study = Path(d)
            (study / "pools").mkdir()
            (study / "gates").mkdir()
            pl.DataFrame({"url": [str(i) for i in range(13344)]}).write_parquet(
                study / "pools/openimages-cvdf.parquet"
            )
            parts = [batch.pool_slice(study, "openimages-cvdf", "preflight")]
            parts += [batch.pool_slice(study, "openimages-cvdf", "qualification", i) for i in range(4)]
            parts += [batch.pool_slice(study, "openimages-cvdf", "calibration", i) for i in range(2)]
            parts += [batch.pool_slice(study, "openimages-cvdf", "tuning")]
            parts += [batch.pool_slice(study, "openimages-cvdf", "evaluation", i) for i in range(4)]
            combined = pl.concat(parts)
            self.assertEqual(combined.height, 13344)
            self.assertEqual(combined["url"].n_unique(), 13344)
            write_new(study / "gates/openimages-cvdf-size.json", {"rows": 512})
            write_new(
                study / "gates/openimages-cvdf-selection.json", {"selected": dict.fromkeys(batch.METHODS, 0)}
            )
            cells = list(batch.planned_cells(study, "openimages-cvdf"))
            self.assertEqual(len(cells), 38)
            self.assertEqual(sum(c["phase"] == "evaluation" for c in cells), 16)

    def test_url_and_refusal_guards(self):
        self.assertEqual(guard.checked_url(URL, "openimages-cvdf"), URL)
        for url in (
            URL.replace("https:", "http:"),
            URL + "?x=1",
            URL.replace("train/", "other/"),
            URL.replace("s3.amazonaws.com", "s3.amazonaws.com.evil.test"),
        ):
            with self.assertRaises(ValueError):
                guard.checked_url(url, "openimages-cvdf")
        for status in (301, 401, 403, 429, 503):
            policy = guard.PublicPolicy(0.1)
            policy.header(1, status, "3600")
            self.assertTrue(policy.fatal)
            self.assertGreater(policy.cooldown_until, time.time() + 3500)
        policy = guard.PublicPolicy(0.1)
        for i in range(100):
            policy.header(i, 404 if i < 6 else 200)
        self.assertEqual(policy.reason, "dead_link_fraction")
        policy = guard.PublicPolicy(0.1)
        for i in range(100):
            policy.network_end(i, None, {"failure_kind": "transport" if i < 3 else None})
        self.assertEqual(policy.reason, "transport_fraction")

    def test_tool_bytes_duplicates_and_no_redirect(self):
        stream = io.StringIO()
        g = guard.ToolGuard("openimages-cvdf", [URL], stream, 0.1, body_limit=10)
        g.opener = type("Opener", (), {"open": lambda _, request, timeout: Response(b"abc")})()
        g.emit("start")
        restore = g.install()
        try:
            with urllib.request.urlopen(URL) as r:
                self.assertEqual(r.read(), b"abc")
            with self.assertRaises(ValueError):
                urllib.request.urlopen(URL)
        finally:
            restore()
            g.close()
        self.assertEqual(g.body_bytes, 3)
        self.assertEqual(len(g.active), 0)
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "audit"
            path.write_text(stream.getvalue())
            a = batch.audit_transport(path)
            self.assertEqual((a["attempts"], a["peak"]), (1, 1))
            self.assertLessEqual(a["mean_active"], 1)
            lines = stream.getvalue().splitlines()
            lines.insert(-1, lines[-2])
            path.write_text("\n".join(lines))
            with self.assertRaises(ValueError):
                batch.audit_transport(path)
        self.assertIsNone(guard.NoRedirect().redirect_request(None, None, 302, "", {}, "https://evil.test"))

    def test_tool_body_budget_stops_new_work(self):
        g = guard.ToolGuard("openimages-cvdf", [URL], io.StringIO(), 0.1, body_limit=2)
        g.opener = type("Opener", (), {"open": lambda _, request, timeout: Response(b"abc")})()
        restore = g.install()
        try:
            with self.assertRaises(RuntimeError):
                with urllib.request.urlopen(URL) as r:
                    r.read()
        finally:
            restore()
        self.assertEqual(g.body_bytes, 2)
        self.assertEqual(g.policy.reason, "body_budget")

    def test_tuning_coverage_and_no_favorable_replacement(self):
        rows = [
            dict(method=m, setting=s, status="complete", coverage=0.99 if s < 3 else 0.8, goodput_Bps=s + 1)
            for m in batch.METHODS
            for s in range(4)
        ]
        self.assertEqual(set(batch.select(rows).values()), {2})
        rows[0]["status"] = "stopped"
        with self.assertRaises(ValueError):
            batch.select(rows)
        with tempfile.TemporaryDirectory() as d:
            study = Path(d)
            (study / "runs/interrupted").mkdir(parents=True)
            (study / "gates").mkdir()
            pl.DataFrame({"url": [URL]}).write_parquet(study / "runs/interrupted/input.parquet")
            used = batch.usage(study)
            self.assertEqual(used["body_bytes"], 2**30)
            self.assertEqual(used["interrupted"], ["interrupted"])

    def test_configuration_native_safeguards_and_tool_output(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)
            pl.DataFrame({"url": [URL], "study_row_id": ["a"]}).write_parquet(p / "input.parquet")
            for method in batch.METHODS:
                c = batch.configuration(p, dict(method=method, setting=3, phase="qualification"), "/source")
                if method == "img2dataset-1.47.0":
                    self.assertEqual(c["thread_count"], 4)
                    self.assertTrue(c["disable_all_reencoding"])
                    self.assertEqual(c["max_shard_retry"], 0)
                else:
                    self.assertEqual(c["C_max"], 4)
                    self.assertEqual(c["max_retry_attempts"], 1)

    def test_branch_qualification_requires_both_workflows(self):
        from benchmark.real_server_study import public_cloud
        from benchmark.real_server_study.common import file_hash

        with tempfile.TemporaryDirectory() as d:
            packet = Path(d)
            write_new(packet / "deployment.json", {"source": {"head": "fixed-head"}})
            write_new(packet / "review.json", {"head": "fixed-head", "status": "completed"})
            write_new(packet / "report.json", {"findings": []})
            write_new(
                packet / "qualification.json",
                dict(
                    head="fixed-head",
                    review_file=str(packet / "review.json"),
                    report_file=str(packet / "report.json"),
                    review_sha256=file_hash(packet / "review.json"),
                    report_sha256=file_hash(packet / "report.json"),
                    ci_run_ids=[1, 2],
                ),
            )

            def outcome(name):
                import json

                return json.dumps(dict(headSha="fixed-head", conclusion="success", name=name))

            with patch.object(
                public_cloud.subprocess,
                "check_output",
                side_effect=[outcome("FLOW-DC regression tests"), outcome("agentic workflow tests")],
            ):
                public_cloud.qualification(packet)
            with (
                patch.object(
                    public_cloud.subprocess, "check_output", return_value=outcome("FLOW-DC regression tests")
                ),
                self.assertRaises(ValueError),
            ):
                public_cloud.qualification(packet)

    def test_source_rights_gate_precedes_image_probe(self):
        with tempfile.TemporaryDirectory() as d:
            study = Path(d)
            (study / "gates").mkdir()
            write_new(
                study / "protocol.json",
                {
                    "provenance": {
                        "taibif-moth": {"media_rights_qualified": False, "limitation": "unqualified"}
                    }
                },
            )
            with (
                patch("benchmark.real_server_study.prepare.guest_check"),
                patch.object(batch.socket, "create_connection") as connection,
                self.assertRaises(ValueError),
            ):
                batch.host_gate(study, "taibif-moth")
            connection.assert_not_called()

    def test_real_tool_adapter_offline_shard_and_failure_ledger(self):
        try:
            import importlib

            native = importlib.import_module("img2dataset.main")
            from img2dataset.reader import Reader
        except ImportError:
            self.skipTest("optional pinned tool environment")
        with tempfile.TemporaryDirectory() as d:
            directory = Path(d)
            (directory / "native").mkdir()
            (directory / "temp").mkdir()
            frame = pl.DataFrame(
                {
                    "url": [URL, URL.replace("a.jpg", "b.jpg")],
                    "study_row_id": ["a", "b"],
                    "author": ["A", "B"],
                    "license": ["CC-BY", "CC-BY"],
                    "landing_url": ["x", "y"],
                    "record_id": ["1", "2"],
                }
            )
            frame.write_parquet(directory / "input.parquet")
            buf = io.BytesIO()
            Image.new("RGB", (8, 8), "blue").save(buf, format="JPEG")
            raw = buf.getvalue()
            cfg = batch.configuration(
                directory, dict(method="img2dataset-1.47.0", setting=0, phase="tuning"), "/source"
            )
            # Exercise the pinned native Reader/Downloader/Resizer/Writer. Only
            # the HTTP opener is mocked: no local server or scientific data.
            reader = Reader(
                str(directory / "input.parquet"),
                "parquet",
                "url",
                None,
                None,
                None,
                cfg["save_additional_columns"],
                512,
                set(),
                str(directory / "temp"),
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
                thread_count=1,
                save_caption=False,
                extract_exif=False,
                output_folder=str(directory / "native"),
                column_list=reader.column_list,
                timeout=30,
                number_sample_per_shard=512,
                oom_shard_count=5,
                compute_hash="sha256",
                verify_hash_type=None,
                encode_format="jpg",
                retries=0,
                user_agent_token="flowdc-public-study",
                disallowed_header_directives=[],
            )
            wrapped = GuardedDownloader(
                downloader,
                "openimages-cvdf",
                frame["url"].to_list(),
                str(directory / "request-audit.jsonl"),
                dict(baseline_s=0.1, ignore_time=False),
            )
            pickle.loads(pickle.dumps(wrapped))  # The actual native distributor uses spawn.

            def opened(request, timeout):
                if request.full_url.endswith("b.jpg"):
                    import urllib.error

                    raise urllib.error.HTTPError(
                        request.full_url, 404, "missing", email.message.Message(), io.BytesIO()
                    )
                return Response(raw)

            with patch.object(
                guard.urllib.request,
                "build_opener",
                return_value=type("Opener", (), {"open": staticmethod(opened)})(),
            ):
                for shard in reader:
                    self.assertTrue(wrapped(shard)[0])
            value = verifier.verify(
                directory, Path(__file__).resolve().parents[1], "img2dataset-1.47.0", 0, 1
            )
            self.assertEqual(value["credited_rows"], 1)
            self.assertEqual(value["coverage"], 0.5)
            self.assertEqual(value["credited_bytes"], len(raw))
            self.assertFalse(value["full_success"])
            self.assertEqual(batch.audit_transport(directory / "request-audit.jsonl")["attempts"], 2)
            ledger = next((directory / "native").glob("*.parquet"))
            changed = pl.read_parquet(ledger).with_columns(pl.lit("tampered").alias("author"))
            changed.write_parquet(ledger)
            with self.assertRaises(ValueError):
                verifier.verify(directory, Path(__file__).resolve().parents[1], "img2dataset-1.47.0", 0, 1)


if __name__ == "__main__":
    unittest.main()
