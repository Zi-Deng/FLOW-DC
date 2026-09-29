"""Second-review regressions and disposition evidence using owned local fixtures."""

import asyncio
import importlib
import json
import sys
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import test_integrity_review as fixtures

base, gradient, protocol, single = fixtures.base, fixtures.gradient, fixtures.protocol, fixtures.single


class SecondReviewRegressions(unittest.TestCase):
    setUp = fixtures.ReviewRegressions.setUp
    setup_run = fixtures.ReviewRegressions.setup_run
    report_factory = staticmethod(fixtures.ReviewRegressions.report_factory)
    parse_adapter = fixtures.ReviewRegressions.parse_adapter

    def test_adapter_throughput_uses_exact_bytes_and_benchmark_binary_units(self):
        for display in ({}, {"downloaded_mb": 1.0}, {"downloaded_mb": 1234.0}):
            with self.subTest(display=display):
                result = self.parse_adapter(
                    {"elapsed_sec": 3, "verified_payload_bytes": 1_000_001, **display}
                )
                self.assertEqual(result.total_bytes_downloaded, 1_000_001)
                self.assertEqual(result.throughput_mbps, 1_000_001 / 1_048_576 / 3)
                self.assertEqual(result.extra_metrics["throughput_bytes_divisor"], 1_048_576)
                self.assertEqual(result.extra_metrics["throughput_unit"], "MiB/s")

    def test_new_export_collision_has_integrity_diagnostic_and_preserves_foreign_file(self):
        for external in (False, True):
            with self.subTest(external=external):
                cfg, _, store = self.setup_run(name=f"race-{external}")
                target = (self.root if external else Path(cfg.output_folder)) / "report.json"

                def create_conflict(step, target=target):
                    if step == "export_intent":
                        target.write_bytes(b"foreign output")

                store.fault = create_conflict
                with self.assertRaisesRegex(protocol.IntegrityError, "export changed during publication"):
                    store.emit(target.name, b"new report", external=external)
                self.assertEqual(target.read_bytes(), b"foreign output")
                index = store.fs.json(".flowdc/exports.json")
                history = index[("external:" if external else "internal:") + target.name]
                self.assertEqual(store.fs.read(history[0]["stage"]), b"new report")

    def test_cross_entrypoint_reconciliation_refuses_before_mutating_reports(self):
        for owner, caller in ((base, gradient), (gradient, base)):
            with self.subTest(owner=owner.__name__):
                cfg, frame, store = self.setup_run(name=owner.__name__, module=owner)
                for key in frame["__key__"]:
                    store.publish(store.begin(key), key, b"verified payload")
                factory = self.report_factory
                if owner is gradient:

                    def factory(config, outcomes, elapsed):
                        return gradient.generate_overview_report(
                            cfg=config,
                            df_total=len(outcomes),
                            outcomes=outcomes,
                            elapsed_sec=elapsed,
                            gradient_summary={},
                        )

                report = base.finalize_run(cfg, store, store.reconcile(), 1, factory)
                self.assertEqual(report["paarc_version"], "2.0.0-gradient" if owner is gradient else "2.0.0")
                store.close()
                before = {p: p.read_bytes() for p in self.root.rglob("*") if p.is_file()}
                request = caller.Config(input_path="unused", output_folder=cfg.output_folder, reconcile=True)
                with (
                    patch.object(base.aiohttp, "ClientSession", side_effect=AssertionError("HTTP forbidden")),
                    self.assertRaisesRegex(protocol.IntegrityError, "reconcile controller variant mismatch"),
                ):
                    if caller is gradient:
                        with patch.object(gradient, "parse_args", return_value=request):
                            asyncio.run(gradient.main())
                    else:
                        asyncio.run(base.run_acquisition(request))
                self.assertEqual({p: p.read_bytes() for p in self.root.rglob("*") if p.is_file()}, before)

    def test_retry_progress_does_not_scan_eligible_list_for_each_row(self):
        class NoLinearMembership(list):
            def __contains__(self, item):
                raise AssertionError("linear eligible-list membership per manifest row")

        cfg, _, store = self.setup_run()
        store.close()
        eligible = protocol.RunStore.eligible

        def selected(instance, snapshot):
            return NoLinearMembership(eligible(instance, snapshot))

        fetch = AsyncMock(return_value=(b"payload", 200, None, None))
        with (
            patch.object(protocol.RunStore, "eligible", selected),
            patch.object(single, "download_via_http_get", fetch),
        ):
            report = asyncio.run(base.run_acquisition(replace(cfg, resume=True)))
        self.assertEqual(fetch.await_count, 2)
        self.assertEqual(report["summary"]["successful_downloads"], 2)

    def test_symlink_ancestor_diagnostic_names_component_without_creating_output(self):
        actual = self.root / "actual"
        actual.mkdir()
        link = self.root / "linked-scratch"
        link.symlink_to(actual, target_is_directory=True)
        with self.assertRaisesRegex(OSError, "linked-scratch"):
            protocol.Files(link / "new-output", create=True)
        self.assertTrue(link.is_symlink())
        self.assertEqual(list(actual.iterdir()), [])

    def test_cleared_rejection_stays_terminal_and_resume_preserves_verified_row(self):
        cfg, frame, store = self.setup_run()
        good, rejected = frame["__key__"].to_list()
        store.publish(store.begin(good), good, b"preserved verified bytes")
        target = Path(cfg.output_folder) / store.rows[rejected]["payload"]
        target.write_bytes(b"foreign file")
        with self.assertRaisesRegex(protocol.IntegrityError, "existing target"):
            store.begin(rejected)
        rejection = store.fs.read(f".flowdc/rejections/{rejected}.json")
        target.unlink()  # Simulate the operator removing their own fixture file.
        store.close()
        with patch.object(base.aiohttp, "ClientSession", side_effect=AssertionError("HTTP forbidden")):
            report = asyncio.run(base.run_acquisition(replace(cfg, resume=True)))
        self.assertEqual(report["summary"]["successful_downloads"], 1)
        with protocol.RunStore(cfg.output_folder) as reopened:
            snapshot = reopened.reconcile()
            failed = next(row for row in snapshot["rows"] if row["row_id"] == rejected)
            self.assertEqual((failed["disposition"], failed["attempt_intents"]), ("failed", 0))
            self.assertFalse(failed["retryable"])
            self.assertEqual(reopened.fs.read(f".flowdc/rejections/{rejected}.json"), rejection)
            self.assertEqual(reopened.fs.read(reopened.rows[good]["payload"]), b"preserved verified bytes")

    def test_url_derived_filename_still_needs_validation_before_http(self):
        with patch.object(single, "download_via_http_get", side_effect=AssertionError("HTTP forbidden")):
            result = asyncio.run(
                single.download_single(
                    "http://fixture.invalid/%2fescape.jpg",
                    "row",
                    None,
                    str(self.root),
                    "webdataset",
                    None,
                    1,
                )
            )
        self.assertIsNotNone(result[3])
        self.assertEqual(list(self.root.iterdir()), [])

    def test_explicit_research_and_compression_config_matches_taskvine_archive(self):
        with patch.dict(sys.modules, {"ndcctools": MagicMock(), "ndcctools.taskvine": MagicMock()}):
            vine = importlib.import_module("TaskvineFLOWDC")
        manager, task = MagicMock(), MagicMock()
        with patch.object(vine.vine, "Task", return_value=task):
            count = vine.submit_tasks(
                manager,
                str(fixtures.ROOT / "bin/download_batch.py"),
                str(fixtures.ROOT / "bin/single_download.py"),
                {"part.parquet": "declared"},
                {"research_profile": True, "compress_tar": True},
                str(self.root),
            )
        self.assertEqual(count, 1)
        config_path = self.root / "config_part.json"
        self.assertTrue(json.loads(config_path.read_text())["compress_tar"])
        with patch.object(sys, "argv", ["downloader", "--config", str(config_path)]):
            cfg = base.normalize_config(base.parse_args())
        self.assertFalse(cfg.compress_tar)
        self.assertTrue(cfg.create_tar)
        self.assertEqual(cfg.output_format, "webdataset")
        self.assertIn("output_part.tar", [call.args[1] for call in task.add_output.call_args_list])


if __name__ == "__main__":
    unittest.main()
