"""Local HTTP, row identity, archive verification and interruption recovery."""

import asyncio
import json
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

import polars as pl

from flowdc import downloader, integrity
from flowdc.config import DownloadConfig
from flowdc.http import parse_retry_after
from flowdc.manifests import load_manifest, validate_and_load
from flowdc.naming import render_filename
from flowdc.partition import partition_manifest
from flowdc.verification import verify_output


class Handler(BaseHTTPRequestHandler):
    requests = []

    def do_GET(self):
        type(self).requests.append(self.path)
        if self.path == "/redirect":
            self.send_response(302)
            self.send_header("Location", "/ok")
            self.end_headers()
            return
        if self.path == "/retry" and type(self).requests.count("/retry") == 1:
            self.send_response(429)
            self.send_header("Retry-After", "1")
            self.end_headers()
            return
        if self.path == "/missing":
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"complete fixture body")

    def log_message(self, *args):
        pass


class AcquisitionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        Handler.requests = []
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def config(self, urls, **kwargs):
        path = self.root / "input.parquet"
        pl.DataFrame({"url": urls}, schema={"url": pl.String}).write_parquet(path)
        return DownloadConfig(
            input_path=str(path), output_dir=str(self.root / "output"), retry_backoff_s=0.01, **kwargs
        )

    def run_config(self, cfg):
        return asyncio.run(downloader.run_acquisition(cfg))

    def test_duplicate_invalid_and_failed_rows_keep_denominator_and_archive(self):
        cfg = self.config(
            [self.url + "/ok", self.url + "/ok", None, "not a URL", self.url + "/missing"], max_attempts=1
        )
        report = self.run_config(cfg)
        self.assertEqual(
            report["output_integrity"]["counts"], {"verified": 2, "failed": 1, "skipped": 2, "unattempted": 0}
        )
        self.assertFalse(report["output_integrity"]["run_complete"])
        index = integrity.parse((Path(cfg.output_dir) / "outcome-index.json").read_bytes())
        self.assertEqual(len({r["row_id"] for r in index["rows"]}), 5)
        self.assertEqual(index["verified_payload_bytes"], 2 * len(b"complete fixture body"))
        self.assertEqual(index["unique_content_bytes"], len(b"complete fixture body"))
        self.assertTrue(verify_output(cfg.output_dir)["archive_verified"])

    def test_redirect_and_retry_after_then_success(self):
        cfg = self.config([self.url + "/redirect", self.url + "/retry"], max_attempts=2)
        started = time.monotonic()
        report = self.run_config(cfg)
        self.assertGreaterEqual(time.monotonic() - started, 0.85)
        self.assertTrue(report["output_integrity"]["run_complete"])
        self.assertEqual(Handler.requests.count("/retry"), 2)
        self.assertIn("/ok", Handler.requests)
        self.assertIsNone(parse_retry_after("-2"))
        self.assertIsNone(parse_retry_after("not a date"))

    def test_verification_is_read_only_and_detects_tampered_payload(self):
        cfg = self.config([self.url + "/ok"])
        self.run_config(cfg)
        root = Path(cfg.output_dir)
        final = root / ".flowdc/final.json"
        before = final.read_bytes()
        verify_output(root)
        self.assertEqual(final.read_bytes(), before)
        index = integrity.parse((root / "outcome-index.json").read_bytes())
        payload = root / index["rows"][0]["payload"]
        payload.write_bytes(b"corruption")
        with self.assertRaises(ValueError):
            verify_output(root)
        self.assertEqual(final.read_bytes(), before)

    def test_resume_does_not_redownload_and_rejects_changed_configuration(self):
        cfg = self.config([self.url + "/ok"])
        self.run_config(cfg)
        requests = len(Handler.requests)
        self.run_config(replace(cfg, resume=True))
        self.assertEqual(len(Handler.requests), requests)
        final = Path(cfg.output_dir) / ".flowdc/final.json"
        before = final.read_bytes()
        with self.assertRaises(ValueError):
            self.run_config(replace(cfg, resume=True, timeout_s=31))
        self.assertEqual(final.read_bytes(), before)

    def test_historical_schema_two_verifies_read_only_and_cannot_resume(self):
        cfg = self.config([self.url + "/ok"])
        self.run_config(cfg)
        root = Path(cfg.output_dir)
        owner_path = root / ".flowdc/owner.json"
        owner = integrity.parse(owner_path.read_bytes())
        values = owner["config"]["values"]
        for old, new in [
            ("max_retry_attempts", "max_attempts"),
            ("url_col", "url_column"),
            ("label_col", "label_column"),
        ]:
            values[old] = values.pop(new)
        owner["config"].update(
            implementation="base",
            source_sha256={
                n: "a" * 64 for n in ("download_batch.py", "single_download.py", "flowdc_integrity.py")
            },
        )
        owner_path.write_bytes(integrity.encode(owner))
        before = {str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*") if p.is_file()}
        self.assertEqual(verify_output(root)["counts"]["verified"], 1)
        with self.assertRaises(ValueError):
            self.run_config(replace(cfg, resume=True))
        self.assertEqual(
            before, {str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*") if p.is_file()}
        )
        with integrity.RunStore(root, allow_historical=True) as store:
            with self.assertRaises(ValueError):
                store.reconcile()
            with self.assertRaises(ValueError):
                store.begin(owner["rows"][0]["row_id"])

    def test_ready_record_recovers_without_new_http_attempt(self):
        cfg = self.config([self.url + "/ok"])
        frame, manifest = load_manifest(cfg)
        validate_and_load(cfg, prepared=(frame, manifest))
        rows = integrity.plan_rows(frame, cfg, render_filename)

        class Crash(BaseException):
            pass

        def fault(step):
            if step == "ready":
                raise Crash()

        with integrity.RunStore(
            cfg.output_dir, manifest=manifest, config=integrity.effective_config(cfg), rows=rows, fault=fault
        ) as store:
            key = rows[0]["row_id"]
            attempt = store.begin(key)
            with self.assertRaises(Crash):
                store.publish(attempt, key, b"committed body")
        with integrity.RunStore(cfg.output_dir) as store:
            snapshot = store.reconcile()
            self.assertEqual(snapshot["counts"]["verified"], 1)
            self.assertEqual(snapshot["rows"][0]["attempt_intents"], 1)
            self.assertFalse(store.eligible(snapshot))
        self.assertFalse(Handler.requests)

    def test_partition_preserves_every_original_row_and_partial_provenance_refuses(self):
        cfg = self.config([None, self.url + "/ok", self.url + "/ok"])
        parts = partition_manifest(cfg.input_path, self.root / "parts", groups=2, method="rows")
        rows = pl.concat([pl.read_parquet(p) for p in parts])
        self.assertEqual(rows.height, 3)
        self.assertEqual(rows["__key__"].n_unique(), 3)
        with self.assertRaises(FileExistsError):
            partition_manifest(cfg.input_path, self.root / "parts")
        broken = self.root / "broken.parquet"
        rows.drop("__flowdc_row_digest__").write_parquet(broken)
        with self.assertRaises(ValueError):
            load_manifest(replace(cfg, input_path=str(broken)))

    def test_overwrite_confirmation_and_invalid_input_preserve_existing_files(self):
        cfg = self.config([self.url + "/ok"])
        output = Path(cfg.output_dir)
        output.mkdir()
        sentinel = output / "keep"
        sentinel.write_text("keep")
        with patch("builtins.input", return_value=""), self.assertRaises(SystemExit):
            validate_and_load(cfg)
        self.assertEqual(sentinel.read_text(), "keep")
        pl.DataFrame({"wrong": [1]}).write_parquet(cfg.input_path)
        with self.assertRaises(ValueError):
            validate_and_load(replace(cfg, force=True))
        self.assertTrue(sentinel.exists())

    def test_symlink_output_refuses_before_http(self):
        cfg = self.config([self.url + "/ok"])
        target = self.root / "target"
        target.mkdir()
        Path(cfg.output_dir).symlink_to(target, target_is_directory=True)
        with self.assertRaises((ValueError, OSError)):
            self.run_config(cfg)
        self.assertFalse(Handler.requests)

    def test_cli_override_and_offline_reconcile_without_input(self):
        cfg = self.config([self.url + "/ok"])
        self.run_config(cfg)
        Path(cfg.input_path).unlink()
        requests = len(Handler.requests)
        p = subprocess.run(
            [sys.executable, "-m", "flowdc", "reconcile", "--output-dir", cfg.output_dir],
            capture_output=True,
            text=True,
        )
        self.assertEqual(p.returncode, 0, p.stderr)
        self.assertEqual(len(Handler.requests), requests)

    def test_cli_recovers_before_overview_publication_from_another_directory(self):
        cfg = self.config([self.url + "/ok"], create_overview=False)
        frame, manifest = load_manifest(cfg)
        validate_and_load(cfg, prepared=(frame, manifest))
        rows = integrity.plan_rows(frame, cfg, render_filename)

        class Crash(BaseException):
            pass

        def fault(step):
            if step == "ready":
                raise Crash()

        with integrity.RunStore(
            cfg.output_dir,
            manifest=manifest,
            config=integrity.effective_config(cfg),
            rows=rows,
            fault=fault,
            input_path=cfg.input_path,
        ) as store:
            key = rows[0]["row_id"]
            with self.assertRaises(Crash):
                store.publish(store.begin(key), key, b"complete fixture body")
        self.assertFalse((Path(cfg.output_dir) / "overview.json").exists())
        elsewhere = self.root / "elsewhere"
        elsewhere.mkdir()
        for command in ("resume", "reconcile"):
            if command == "reconcile":
                Path(cfg.input_path).unlink()
            result = subprocess.run(
                [sys.executable, "-m", "flowdc", command, "--output-dir", cfg.output_dir],
                cwd=elsewhere,
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(result.stdout)["output_integrity"]["counts"]["verified"], 1)
        self.assertFalse(Handler.requests)

    def test_resume_accepts_explicit_source_when_no_path_was_recorded(self):
        cfg = self.config([self.url + "/ok"])
        self.run_config(cfg)
        owner_path = Path(cfg.output_dir) / ".flowdc/owner.json"
        owner = integrity.parse(owner_path.read_bytes())
        owner.pop("input_path")
        owner_path.write_bytes(integrity.encode(owner))
        args = [sys.executable, "-m", "flowdc", "resume", "--output-dir", cfg.output_dir]
        missing = subprocess.run(args, capture_output=True, text=True)
        self.assertEqual(missing.returncode, 2)
        self.assertIn("--input-path", missing.stderr)
        recovered = subprocess.run([*args, "--input-path", cfg.input_path], capture_output=True, text=True)
        self.assertEqual(recovered.returncode, 0, recovered.stderr)
        self.assertTrue(json.loads(recovered.stdout)["output_integrity"]["run_complete"])

    def test_overwrite_cannot_delete_working_directory_ancestor(self):
        from contextlib import chdir

        cfg = self.config([self.url + "/ok"])
        ancestor = self.root / "ancestor"
        cwd = ancestor / "project"
        cwd.mkdir(parents=True)
        sentinel = ancestor / "keep"
        sentinel.write_text("preserve")
        with chdir(cwd), self.assertRaises(ValueError):
            validate_and_load(replace(cfg, output_dir=str(ancestor), force=True))
        self.assertEqual(sentinel.read_text(), "preserve")


class ConfigurationTests(unittest.TestCase):
    def test_strict_fields_and_single_defaults(self):
        base = {"input_path": "input.csv", "output_dir": "output"}
        self.assertEqual(DownloadConfig.from_dict(base).method, "paarc-grad")
        for extra in [
            {"timeout": 3},
            {"C_max": 4},
            {"enable_paarc": False},
            {"method": "ratio-v1"},
            {"method_options": {"ablation": "no-startup"}},
            {"force": "yes"},
            {"workers": True},
            {"timeout_s": float("nan")},
        ]:
            with self.subTest(extra=extra), self.assertRaises(ValueError):
                DownloadConfig.from_dict({**base, **extra})
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(json.dumps({**base, "timeout_s": 3}))
            self.assertEqual(DownloadConfig.read(path, timeout_s=4).timeout_s, 4)
