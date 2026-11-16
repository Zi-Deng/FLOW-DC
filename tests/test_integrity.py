"""Retained identity, publication, no-follow recovery and archive contracts."""

import asyncio
import hashlib
import io
import tarfile
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import AsyncMock, patch

import polars as pl

from flowdc import downloader as base
from flowdc import http as single
from flowdc import integrity as protocol
from flowdc.config import DownloadConfig, normalize_config
from flowdc.manifests import load_manifest, validate_and_load
from flowdc.naming import render_filename


class Interrupted(BaseException):
    pass


class ArchiveLookupTests(unittest.TestCase):
    class CountedRow(dict):
        metadata_reads = 0

        def __getitem__(self, key):
            if key == "metadata":
                self.metadata_reads += 1
            return super().__getitem__(key)

    def fixture(self, count, *, compressed=False, binding=None, shadow=False, json_members=True):
        rows, members = ([], [])
        for i in range(count):
            payload = protocol.encode({"sample": i})
            name = f"{i}.json" if json_members else f"{i}.bin"
            metadata_name = f"{i}.metadata.json" if json_members else f"{i}.metadata"
            metadata = protocol.encode(
                {"row_id": i, "payload_sha256": protocol.digest(payload), **(binding or {})}
            )
            rows.append(
                self.CountedRow(
                    disposition="verified",
                    row_id=i,
                    payload=name,
                    payload_bytes=len(payload),
                    payload_sha256=protocol.digest(payload),
                    metadata=metadata_name,
                    metadata_bytes=len(metadata),
                    metadata_sha256=protocol.digest(metadata),
                )
            )
            members.extend(((name, payload), (metadata_name, metadata)))
        for disposition in ("failed", "skipped", "unattempted"):
            rows.append(self.CountedRow(disposition=disposition, metadata=f"{disposition}.json"))
        if shadow:
            rows.insert(0, self.CountedRow(disposition="failed", metadata="0.metadata.json", row_id=-1))
        snapshot = {"rows": rows}
        members.extend((("outcome-index.json", protocol.encode(snapshot)), ("overview.json", b"{}")))
        stream = io.BytesIO()
        with tarfile.open(fileobj=stream, mode="w:gz" if compressed else "w") as archive:
            for name, content in members:
                item = tarfile.TarInfo("output/" + name)
                item.size = len(content)
                archive.addfile(item, io.BytesIO(content))
        return (stream.getvalue(), snapshot)

    def test_many_rows_use_bounded_lookup_work_with_json_payloads_and_mixed_dispositions(self):
        for compressed in (False, True):
            with self.subTest(compressed=compressed):
                raw, snapshot = self.fixture(96, compressed=compressed)
                self.assertTrue(protocol.verify_archive(raw, snapshot, "output"))
                self.assertLessEqual(
                    sum(row.metadata_reads for row in snapshot["rows"]), 2 * len(snapshot["rows"])
                )

    def test_first_metadata_match_in_nonverified_row_remains_authoritative(self):
        raw, snapshot = self.fixture(2, shadow=True)
        with self.assertRaisesRegex(protocol.IntegrityError, "archive row metadata mismatch"):
            protocol.verify_archive(raw, snapshot, "output")

    def test_wrong_metadata_identity_or_payload_binding_is_rejected_after_digest_check(self):
        for binding in ({"row_id": -1}, {"payload_sha256": "0" * 64}):
            with self.subTest(binding=binding):
                raw, snapshot = self.fixture(2, binding=binding)
                with self.assertRaisesRegex(protocol.IntegrityError, "archive row metadata mismatch"):
                    protocol.verify_archive(raw, snapshot, "output")

    def test_non_json_members_do_not_require_nonverified_metadata_paths(self):
        raw, snapshot = self.fixture(2, json_members=False)
        snapshot["rows"][-1].pop("metadata")
        with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
            members = [(item.name, archive.extractfile(item).read()) for item in archive]
        stream = io.BytesIO()
        with tarfile.open(fileobj=stream, mode="w") as archive:
            for name, content in members:
                if name == "output/outcome-index.json":
                    content = protocol.encode(snapshot)
                item = tarfile.TarInfo(name)
                item.size = len(content)
                archive.addfile(item, io.BytesIO(content))
        self.assertTrue(protocol.verify_archive(stream.getvalue(), snapshot, "output"))


class ProtocolTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def setup_run(self, values=None, **options):
        source = self.root / "input.parquet"
        if values is None:
            values = {"url": ["http://fixture.invalid/a.jpg", "http://fixture.invalid/b.jpg"]}
        pl.DataFrame(values).write_parquet(source)
        cfg = normalize_config(
            DownloadConfig(
                input_path=str(source),
                output_dir=str(self.root / "output"),
                naming_mode="row_id",
                output_format="webdataset",
                compress_archive=False,
                retry_backoff_s=0.01,
                **options,
            )
        )
        frame, manifest = load_manifest(cfg)
        (self.root / "output").mkdir()
        store = protocol.RunStore(
            cfg.output_dir,
            manifest=manifest,
            config=protocol.effective_config(cfg),
            rows=protocol.plan_rows(frame, cfg, render_filename),
        )
        self.addCleanup(store.close)
        return (cfg, frame, store)

    def test_parent_identity_survives_reordering_and_partitioning(self):
        cfg, frame, store = self.setup_run(
            {
                "url": [None, "http://fixture.invalid/a", "http://fixture.invalid/a"],
                "__key__": ["external", "same", "same"],
            }
        )
        parent = frame["__key__"].to_list()
        partition = frame.reverse().head(2)
        stamped = protocol.stamp_frame(partition, "a" * 64)
        self.assertEqual(stamped["__key__"].to_list(), parent[::-1][:2])
        self.assertEqual(stamped[protocol.PROVENANCE[2]].to_list(), [3, 3])
        self.assertEqual(stamped[protocol.EXTERNAL_KEY].to_list(), ["same", "same"])
        self.assertEqual(
            store.reconcile()["counts"], {"verified": 0, "failed": 0, "skipped": 1, "unattempted": 2}
        )
        for changed in (
            partition.with_columns(pl.lit("tampered").alias("url")),
            pl.concat([partition, partition]),
            partition.drop(protocol.PROVENANCE[2]),
        ):
            with self.assertRaises(protocol.IntegrityError):
                protocol.stamp_frame(changed, "a" * 64)
            changed.write_parquet(self.root / "conflicting.parquet")
            sentinel = Path(cfg.output_dir) / "user-file"
            sentinel.write_bytes(b"preserve")
            with self.assertRaises(ValueError):
                validate_and_load(replace(cfg, input_path=str(self.root / "conflicting.parquet"), force=True))
            self.assertEqual(sentinel.read_bytes(), b"preserve")

    def test_empty_and_all_invalid_inputs_reconcile_without_http(self):
        for values in ([], [None, "", " ", "ftp://fixture.invalid/a", "http://"]):
            with self.subTest(values=values):
                source, output = (
                    self.root / "empty.parquet",
                    self.root / ("empty" if not values else "invalid"),
                )
                pl.DataFrame({"url": values}, schema={"url": pl.String}).write_parquet(source)
                cfg = DownloadConfig(input_path=str(source), output_dir=str(output), research_profile=True)
                with patch.object(
                    base.aiohttp, "ClientSession", side_effect=AssertionError("HTTP forbidden")
                ):
                    report = asyncio.run(base.run_acquisition(cfg))
                self.assertTrue(report["output_integrity"]["run_complete"])
                self.assertEqual(report["summary"]["total_urls"], len(values))
                self.assertEqual(report["summary"]["skipped_rows"], len(values))
                self.assertEqual(report["summary"]["verified_payload_bytes"], 0)

    def test_collision_planning_rejects_all_affected_rows(self):
        cfg, frame, _ = self.setup_run(
            {"url": ["http://a.invalid/same.jpg", "http://b.invalid/same.jpg", "http://c.invalid/same.json"]}
        )
        planned = protocol.plan_rows(
            frame, replace(cfg, naming_mode="url_based", file_name_pattern="{segment_last}"), render_filename
        )
        self.assertEqual([r["initial_disposition"] for r in planned], ["failed"] * 3)
        self.assertTrue(all("collision" in r["error"] for r in planned))
        safe = protocol.plan_rows(frame.head(2), cfg, render_filename)
        self.assertEqual(len({r["payload"] for r in safe}), 2)
        self.assertTrue(all(r["initial_disposition"] is None for r in safe))

    def test_reserved_labels_and_encoded_separators_are_rejected(self):
        for label in (".", "..", "/tmp", "a/b", "a\\b", "%2fetc", "%252e%252e", ".flowdc", "overview.json"):
            frame = protocol.stamp_frame(
                pl.DataFrame({"url": ["http://fixture.invalid/a.jpg"], "label": [label]}), "a" * 64
            )
            cfg = DownloadConfig(input_path="unused", output_dir="unused", label_column="label")
            rows = protocol.plan_rows(frame, cfg, render_filename)
            self.assertEqual(rows[0]["initial_disposition"], "failed", label)
        for name in ("overview.json", "outcome-index.json", ".flowdc"):
            cfg = DownloadConfig(
                input_path="unused",
                output_dir="unused",
                output_format="webdataset",
                naming_mode="url_based",
                file_name_pattern=name,
            )
            rows = protocol.plan_rows(frame, cfg, lambda pattern, url, key: pattern)
            self.assertEqual(rows[0]["initial_disposition"], "failed", name)

    def test_symlink_parent_and_existing_targets_are_preserved(self):
        cfg, frame, store = self.setup_run()
        key = frame["__key__"][0]
        path = Path(cfg.output_dir) / store.rows[key]["payload"]
        sentinel = self.root / "sentinel"
        sentinel.write_bytes(b"user evidence")
        path.symlink_to(sentinel)
        with self.assertRaises(protocol.IntegrityError):
            store.begin(key)
        self.assertEqual(sentinel.read_bytes(), b"user evidence")
        self.assertEqual(store.reconcile()["counts"]["failed"], 1)
        outside = self.root / "outside"
        outside.mkdir()
        (Path(cfg.output_dir) / "escape").symlink_to(outside, target_is_directory=True)
        with self.assertRaises(OSError):
            store.fs.write("escape/nope", b"bad")
        self.assertEqual(list(outside.iterdir()), [])

    def test_each_publication_cut_is_recoverable_without_double_credit(self):
        for step in (
            "intent",
            "payload_staged",
            "metadata_staged",
            "ready",
            "payload_published",
            "metadata_published",
            "committed",
        ):
            with self.subTest(step=step), tempfile.TemporaryDirectory(dir=self.root) as tmp:
                output = Path(tmp) / "out"
                output.mkdir()
                frame = protocol.stamp_frame(
                    pl.DataFrame({"url": ["http://fixture.invalid/a.jpg"]}), "a" * 64
                )
                cfg = DownloadConfig(
                    input_path="unused",
                    output_dir=str(output),
                    naming_mode="row_id",
                    output_format="webdataset",
                )

                def fault(actual, step=step):
                    if actual == step:
                        raise Interrupted()

                with protocol.RunStore(
                    output,
                    manifest={"sha256": "a" * 64, "original_rows": 1},
                    config=protocol.effective_config(cfg),
                    rows=protocol.plan_rows(frame, cfg, render_filename),
                    fault=fault,
                ) as store:
                    key = frame["__key__"][0]
                    with self.assertRaises(Interrupted):
                        directory = store.begin(key)
                        store.publish(directory, key, b"truth")
                with protocol.RunStore(output) as recovered:
                    first = recovered.reconcile()
                    self.assertEqual(first, recovered.reconcile())
                    expected = step in ("ready", "payload_published", "metadata_published", "committed")
                    row = first["rows"][0]
                    self.assertEqual(row["disposition"], "verified" if expected else "failed")
                    self.assertEqual(row["attempt_intents"], 1)
                    self.assertEqual(first["verified_payload_bytes"], 5 if expected else 0)
                    if not expected:
                        self.assertTrue(row["attempt_information_uncertain"])
                        self.assertEqual(recovered.eligible(first), [key])
                        next_directory = recovered.begin(key)
                        recovered.publish(next_directory, key, b"truth")
                        self.assertEqual(recovered.reconcile()["rows"][0]["attempt_intents"], 2)

    def test_existing_identical_unowned_file_is_not_recovered_as_owned(self):
        cfg, frame, store = self.setup_run()
        key = frame["__key__"][0]
        directory = store.begin(key)

        def stop(step):
            if step == "ready":
                raise Interrupted()

        store.fault = stop
        with self.assertRaises(Interrupted):
            store.publish(directory, key, b"same bytes")
        path = Path(cfg.output_dir) / store.rows[key]["payload"]
        path.write_bytes(b"same bytes")
        store.fault = lambda _: None
        result = store.reconcile()
        self.assertEqual(result["counts"]["failed"], 1)
        self.assertEqual(result["verified_payload_bytes"], 0)
        self.assertEqual(path.read_bytes(), b"same bytes")
        self.assertFalse(result["rows"][0]["retryable"])

    def test_corrupt_and_truncated_ownership_fail_closed(self):
        cfg, _, store = self.setup_run()
        original = (Path(cfg.output_dir) / ".flowdc/owner.json").read_bytes()
        store.close()
        for raw in (b'{"schema_version":', original.replace(b'"schema_version":2', b'"schema_version":999')):
            (Path(cfg.output_dir) / ".flowdc/owner.json").write_bytes(raw)
            with self.assertRaises(protocol.IntegrityError):
                protocol.RunStore(cfg.output_dir)
            with self.assertRaises(protocol.IntegrityError):
                validate_and_load(replace(cfg, force=True))
            self.assertEqual((Path(cfg.output_dir) / ".flowdc/owner.json").read_bytes(), raw)

    def test_unknown_intent_and_torn_temporary_are_not_zero_attempts(self):
        _, frame, store = self.setup_run()
        key = frame["__key__"][0]
        store.fs.mkdir(f".flowdc/attempts/{key}/1")
        store.fs.write(f".flowdc/attempts/{key}/1/intent.json.writing-torn", b'{"')
        result = store.reconcile()["rows"][0]
        self.assertEqual(result["disposition"], "failed")
        self.assertEqual(result["attempt_intents"], 1)
        self.assertTrue(result["attempt_information_uncertain"])
        self.assertFalse(result["retryable"])

    def test_million_bytes_duplicate_content_and_archive_truth(self):
        _, frame, store = self.setup_run()
        body = b"a" * 500000
        for key in frame["__key__"]:
            store.publish(store.begin(key), key, body)
        snapshot = store.reconcile()
        self.assertEqual(snapshot["verified_payload_bytes"], 1000000)
        self.assertEqual(snapshot["unique_content_bytes"], 500000)
        path, size, sha = store.make_archive(snapshot, {"summary": {}}, compress=False)
        raw = Path(path).read_bytes()
        self.assertEqual(size, len(raw))
        self.assertEqual(sha, hashlib.sha256(raw).hexdigest())
        with tarfile.open(path) as archive:
            payloads = [m for m in archive if m.name.endswith(".jpg")]
            self.assertEqual(len(payloads), 2)
            for item in payloads:
                self.assertEqual(archive.extractfile(item).read(), body)
        self.assertTrue(protocol.verify_archive(raw, snapshot, store.root.name))
        self.assertEqual(snapshot, store.reconcile())

    def test_archive_missing_extra_duplicate_corrupt_and_truncated_members_fail(self):
        _, frame, store = self.setup_run()
        key = frame["__key__"][0]
        store.publish(store.begin(key), key, b"truth")
        snapshot = store.reconcile()
        path, _, _ = store.make_archive(snapshot, {}, compress=False)
        raw = Path(path).read_bytes()
        with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
            members = [(m.name, archive.extractfile(m).read()) for m in archive]
        for mode in ("missing", "extra", "duplicate", "corrupt", "truncated", "tail"):
            selected = list(members)
            if mode == "missing":
                selected.pop(0)
            elif mode == "extra":
                selected.append(("out/unexpected", b"extra"))
            elif mode == "duplicate":
                selected.append(selected[0])
            elif mode == "corrupt":
                selected[0] = (selected[0][0], b"corrupt")
            stream = io.BytesIO()
            with tarfile.open(fileobj=stream, mode="w") as archive:
                for name, content in selected:
                    item = tarfile.TarInfo(name)
                    item.size = len(content)
                    archive.addfile(item, io.BytesIO(content))
            candidate = stream.getvalue()
            if mode == "truncated":
                candidate = candidate[:600]
            if mode == "tail":
                candidate += b"garbage" + b"\x00" * 1024
            with self.subTest(mode=mode), self.assertRaises((ValueError, tarfile.TarError)):
                protocol.verify_archive(candidate, snapshot, store.root.name)

    def test_resume_skips_committed_rows_and_retains_attempt_budget(self):
        source, output = (self.root / "resume.parquet", self.root / "resume")
        pl.DataFrame(
            {"url": ["http://fixture.invalid/one.jpg", "http://fixture.invalid/two.jpg"]}
        ).write_parquet(source)
        cfg = DownloadConfig(
            input_path=str(source),
            output_dir=str(output),
            create_tar=False,
            workers=1,
            retry_backoff_s=0.01,
            max_attempts=2,
        )
        calls = []

        async def fetch(session, url, timeout):
            calls.append(url)
            if url.endswith("two.jpg") and calls.count(url) == 1:
                base.shutdown_flag = True
                return (None, 503, "HTTP 503", None)
            return (b"payload", 200, None, None)

        with (
            patch.object(base, "shutdown_flag", False),
            patch.object(single, "download_via_http_get", side_effect=fetch),
        ):
            first = asyncio.run(base.run_acquisition(cfg))
            self.assertEqual(first["summary"]["successful_downloads"], 1)
            base.shutdown_flag = False
            second = asyncio.run(base.run_acquisition(replace(cfg, resume=True)))
        self.assertEqual(second["summary"]["successful_downloads"], 2)
        self.assertEqual(calls.count("http://fixture.invalid/one.jpg"), 1)
        self.assertEqual(calls.count("http://fixture.invalid/two.jpg"), 2)
        with protocol.RunStore(output) as store:
            self.assertEqual([row["attempt_intents"] for row in store.reconcile()["rows"]], [1, 2])
        before = (output / ".flowdc/owner.json").read_bytes()
        with self.assertRaises(protocol.IntegrityError):
            asyncio.run(base.run_acquisition(replace(cfg, resume=True, timeout_s=99)))
        self.assertEqual((output / ".flowdc/owner.json").read_bytes(), before)
        pl.DataFrame({"url": ["http://fixture.invalid/changed"]}).write_parquet(source)
        with self.assertRaises(protocol.IntegrityError):
            asyncio.run(base.run_acquisition(replace(cfg, resume=True)))
        self.assertEqual((output / ".flowdc/owner.json").read_bytes(), before)

    def test_offline_reconciliation_does_not_need_source_or_http(self):
        cfg, frame, store = self.setup_run(create_tar=False)
        for key in frame["__key__"]:
            store.publish(store.begin(key), key, b"truth")
        store.close()
        Path(cfg.input_path).unlink()
        with patch.object(base.aiohttp, "ClientSession", side_effect=AssertionError("HTTP forbidden")):
            report = asyncio.run(base.run_acquisition(replace(cfg, reconcile=True)))
            first = (Path(cfg.output_dir) / "outcome-index.json").read_bytes()
            asyncio.run(base.run_acquisition(replace(cfg, reconcile=True)))
            self.assertEqual((Path(cfg.output_dir) / "outcome-index.json").read_bytes(), first)
        self.assertTrue(report["output_integrity"]["run_complete"])
        self.assertEqual(report["summary"]["successful_downloads"], 2)

    def test_report_and_archive_failures_never_complete(self):
        cfg, frame, store = self.setup_run()
        for key in frame["__key__"]:
            store.publish(store.begin(key), key, b"truth")

        def factory(c, outcomes, elapsed):
            return base.generate_overview_report(
                cfg=c, df_total=len(outcomes), outcomes=outcomes, elapsed_sec=elapsed
            )

        with patch.object(store, "make_archive", side_effect=OSError("archive failure")):
            with self.assertRaises(protocol.IntegrityError):
                base.finalize_run(cfg, store, store.reconcile(), 0, factory)
        self.assertFalse(store.fs.json(".flowdc/final.json")["run_complete"])
        with patch.object(store, "emit", side_effect=OSError("report failure")):
            with self.assertRaises(OSError):
                base.finalize_run(cfg, store, store.reconcile(), 0, factory)
        self.assertFalse(store.fs.json(".flowdc/final.json")["run_complete"])
        atomic = store.fs.atomic

        def fail_last_record(path, value, **kwargs):
            if path == ".flowdc/final.json" and value.get("run_complete") is True:
                raise OSError("completion record failure after external report")
            return atomic(path, value, **kwargs)

        with patch.object(store.fs, "atomic", side_effect=fail_last_record):
            with self.assertRaisesRegex(OSError, "completion record failure"):
                base.finalize_run(replace(cfg, create_tar=False), store, store.reconcile(), 0, factory)
        external = protocol.parse((self.root / "output_overview.json").read_bytes())
        self.assertIsNone(external["output_integrity"]["run_complete"])
        self.assertIsNone(external["output_integrity"]["useful_final_payload_bytes"])
        self.assertFalse(store.fs.json(".flowdc/final.json")["run_complete"])
        self.assertNotEqual(
            external["output_integrity"]["completion_record_sha256"],
            protocol.digest(store.fs.read(".flowdc/final.json")),
        )

    def test_unknown_journal_rows_fail_closed_and_managed_tar_excludes_unowned_files(self):
        cfg, frame, store = self.setup_run(create_tar=False)
        for key in frame["__key__"]:
            store.publish(store.begin(key), key, b"truth")
        store.fs.write("unowned.txt", b"preserve but do not archive")
        path, _, _ = store.make_archive(store.reconcile(), {}, compress=False)
        store.close()
        with tarfile.open(path) as archive:
            names = archive.getnames()
            self.assertEqual(len(names), 6)
            self.assertFalse(any(".flowdc" in name or "unowned.txt" in name for name in names))
        with protocol.RunStore(cfg.output_dir) as reopened:
            reopened.fs.mkdir(".flowdc/attempts/" + "a" * 64)
            with self.assertRaisesRegex(protocol.IntegrityError, "unknown row"):
                reopened.reconcile()
            self.assertEqual(reopened.fs.read("unowned.txt"), b"preserve but do not archive")

    def test_exhausted_attempts_do_not_restart_on_resume(self):
        cfg, frame, store = self.setup_run(max_attempts=2, create_tar=False)
        for key in frame["__key__"]:
            for _ in range(2):
                store.fail(store.begin(key), key, error="HTTP 503", status=503, retryable=True)
        self.assertEqual(store.eligible(store.reconcile()), [])
        store.close()
        with patch.object(
            base.aiohttp, "ClientSession", side_effect=AssertionError("exhausted attempts cannot dispatch")
        ):
            report = asyncio.run(base.run_acquisition(replace(cfg, resume=True)))
        self.assertEqual(report["summary"]["failed_downloads"], 2)
        self.assertFalse(report["output_integrity"]["run_complete"])

    def test_export_interruptions_and_unowned_report_preserve_evidence(self):
        cfg, _, store = self.setup_run()
        store.emit("overview.json", b"first")

        def fault(step):
            if step == "export_published":
                raise Interrupted()

        store.fault = fault
        with self.assertRaises(Interrupted):
            store.emit("overview.json", b"second")
        store.fault = lambda _: None
        store.emit("overview.json", b"third")
        self.assertEqual(store.fs.read("overview.json"), b"third")
        unowned = self.root / "output_overview.json"
        unowned.write_bytes(b"user file")
        with self.assertRaises(protocol.IntegrityError):
            store.emit(unowned.name, b"replacement", external=True)
        self.assertEqual(unowned.read_bytes(), b"user file")

    def test_force_replaces_only_prior_owned_exports_after_validation(self):
        source, output = (self.root / "force.parquet", self.root / "force")
        pl.DataFrame({"url": ["http://fixture.invalid/a.jpg"]}).write_parquet(source)
        cfg = DownloadConfig(
            input_path=str(source),
            output_dir=str(output),
        )
        for body, force in ((b"first", False), (b"second", True)):
            with patch.object(
                single, "download_via_http_get", AsyncMock(return_value=(body, 200, None, None))
            ):
                report = asyncio.run(base.run_acquisition(replace(cfg, force=force)))
            self.assertEqual(report["summary"]["verified_payload_bytes"], len(body))
        self.assertEqual(next((output / "output").glob("*.jpg")).read_bytes(), b"second")
