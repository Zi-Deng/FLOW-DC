"""Frozen matrices, first attempts, missing outcomes and run-level inference."""

import io
import json
import tempfile
import unittest
from pathlib import Path

from PIL import Image

from flowdc.experiments.protocol import prepare, validate
from flowdc.experiments.runner import analyze, run, select, verify
from tests.test_acquisition import Handler


class ImageHandler(Handler):
    def do_GET(self):
        type(self).requests.append(self.path)
        raw = io.BytesIO()
        Image.new("RGB", (8, 8), "blue").save(raw, format="JPEG")
        self.send_response(200)
        self.end_headers()
        self.wfile.write(raw.getvalue())


class ExperimentTests(unittest.TestCase):
    # Reuse only the loopback fixture setup, not acquisition test methods.
    def setUp(self):
        import threading
        from http.server import ThreadingHTTPServer

        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        ImageHandler.requests = []
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), ImageHandler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def definition(self):
        import polars as pl

        source = self.root / "population.parquet"
        pl.DataFrame({"url": [self.url + f"/{i}.jpg" for i in range(8)]}).write_parquet(source)
        protocol = {
            "name": "loopback-test",
            "source": "loopback",
            "blocks": 2,
            "conditions": [{"rows": 4, "workers": 1}],
            "methods": [{"method": "paarc-ratio"}, {"method": "paarc"}],
            "seed": 54,
            "timeout_s": 30,
            "body_limit_bytes": 2**20,
            "access_qualified": True,
            "media_rights_qualified": True,
        }
        path = self.root / "protocol.json"
        path.write_text(json.dumps(protocol))
        return source, path, protocol

    def test_frozen_blocks_first_attempts_and_missing_baseline(self):
        source, path, _ = self.definition()
        study = self.root / "study"
        frozen = prepare(path, source, study)
        self.assertEqual(len(frozen["cells"]), 4)
        import polars as pl

        ids = [set(pl.read_parquet(study / f"inputs/b{i}-c0.parquet")["__key__"]) for i in range(2)]
        self.assertFalse(ids[0] & ids[1])
        first = run(study, limit=1)
        self.assertEqual(first[0]["status"], "complete", first)
        self.assertEqual(first[0]["coverage"], 1.0)
        self.assertGreater(first[0]["credited_bytes"], 4)
        before = (study / "runs" / first[0]["run_id"] / "record.json").read_bytes()
        remaining = run(study)
        self.assertEqual(len(remaining), 3)
        self.assertTrue(all(r["status"] == "complete" for r in remaining), remaining)
        self.assertEqual((study / "runs" / first[0]["run_id"] / "record.json").read_bytes(), before)
        result = analyze(study)
        self.assertEqual(len(result["inventory"]), 4)
        self.assertTrue(all(r["pairs"] == 0 and r["geometric_ratio"] is None for r in result["contrasts"]))
        self.assertFalse(run(study))
        self.assertEqual(verify(study)["verified_completed_attempts"], 4)

    def test_access_unstarted_and_tampering(self):
        source, path, protocol = self.definition()
        protocol["access_qualified"] = False
        path.write_text(json.dumps(protocol))
        study = self.root / "study"
        prepare(path, source, study)
        with self.assertRaises(ValueError):
            run(study)
        self.assertEqual({r["status"] for r in analyze(study)["inventory"]}, {"unstarted"})
        (study / "inputs/b0-c0.parquet").write_bytes(b"changed")
        with self.assertRaises(ValueError):
            analyze(study)

    def test_interrupted_attempt_is_not_replayed(self):
        source, path, _ = self.definition()
        study = self.root / "study"
        frozen = prepare(path, source, study)
        directory = study / "runs" / frozen["cells"][0]["run_id"]
        directory.mkdir()
        self.assertEqual(analyze(study)["inventory"][0]["status"], "interrupted")
        self.assertEqual(len(run(study)), 3)
        self.assertFalse((directory / "record.json").exists())

    def test_explicit_methods_and_unknown_fields(self):
        _, _, protocol = self.definition()
        for item in [
            dict(protocol, extra=1),
            dict(protocol, methods=[{"method": "fixed-v1"}]),
            dict(protocol, conditions=[{"rows": None, "workers": 1}]),
            dict(protocol, ignore_time=True),
            dict(protocol, baseline_s=float("inf")),
            dict(protocol, conditions=[{"rows": 2, "workers": 65}]),
        ]:
            with self.assertRaises(ValueError):
                validate(item)

    def test_calibration_selection_and_excluded_confirmation(self):
        import polars as pl

        source, path, protocol = self.definition()
        protocol.update(
            blocks=1,
            phase="calibration",
            selection={"minimum_http_s": 0.001, "coverage_tolerance": 0.01, "goodput_tie_fraction": 0.02},
        )
        path.write_text(json.dumps(protocol))
        study = self.root / "calibration"
        prepare(path, source, study)
        run(study)
        result = select(study)
        self.assertTrue(all(r["configuration"] is not None for r in result["selected"]), result)
        protocol.pop("selection")
        protocol["phase"] = "evaluation"
        path.write_text(json.dumps(protocol))
        confirmation = self.root / "confirmation"
        prepare(path, source, confirmation, exclude_study=study)
        a = set(pl.read_parquet(study / "inputs/b0-c0.parquet")["url"])
        b = set(pl.read_parquet(confirmation / "inputs/b0-c0.parquet")["url"])
        self.assertFalse(a & b)

    def test_runner_lock_and_positive_limit(self):
        import fcntl

        source, path, _ = self.definition()
        study = self.root / "study"
        prepare(path, source, study)
        with self.assertRaises(ValueError):
            run(study, limit=0)
        with (study / ".run.lock").open("a+b") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaisesRegex(ValueError, "another runner"):
                run(study)
