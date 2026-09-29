"""Identity, publication and recovery contract; temporary fixtures, no cloud calls."""

import asyncio
import hashlib
import io
import selectors
import signal
import subprocess
import sys
import tarfile
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import AsyncMock, patch

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bin"))
import download_batch as base  # noqa: E402
import download_batch_gradient as gradient  # noqa: E402
import flowdc_integrity as protocol  # noqa: E402
import single_download as single  # noqa: E402


class BaseRegressions(unittest.TestCase):
    def test_invalid_rows_keep_original_denominator_and_ids(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "input.parquet"
            pl.DataFrame(
                {"url": [None, "", "not a URL", "http://fixture.invalid/a", "http://fixture.invalid/a"]}
            ).write_parquet(path)
            cfg = base.Config(input_path=str(path), output_folder=str(Path(tmp) / "output"))
            frame = base.validate_and_load(cfg)
            self.assertEqual(frame.height, 5)
            self.assertEqual(frame["__key__"].n_unique(), 5)
            self.assertEqual(frame["__flowdc_position__"].to_list(), list(range(5)))

    def test_invalid_input_does_not_delete_existing_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            path, output = Path(tmp) / "input.parquet", Path(tmp) / "output"
            pl.DataFrame({"wrong_column": ["value"]}).write_parquet(path)
            output.mkdir()
            sentinel = output / "owned-by-user"
            sentinel.write_bytes(b"keep")
            cfg = base.Config(input_path=str(path), output_folder=str(output), force_overwrite=True)
            with self.assertRaises(ValueError):
                base.validate_and_load(cfg)
            self.assertTrue(sentinel.exists(), "invalid input must be rejected before deletion")
            self.assertEqual(sentinel.read_bytes(), b"keep")

    def test_helper_collision_never_overwrites_existing_payload(self):
        async def exercise(root):
            for payload in (b"first body", b"second body"):
                with patch.object(
                    single, "download_via_http_get", AsyncMock(return_value=(payload, 200, None, None))
                ):
                    result = await single.download_single(
                        "http://fixture.invalid/a",
                        payload.hex(),
                        None,
                        str(root),
                        "webdataset",
                        None,
                        1,
                        filename="same.jpg",
                    )
                if payload == b"first body":
                    self.assertIsNone(result[3])
                else:
                    self.assertIsNotNone(result[3])
            self.assertEqual((root / "same.jpg").read_bytes(), b"first body")

        with tempfile.TemporaryDirectory() as tmp:
            asyncio.run(exercise(Path(tmp)))


class Interrupted(BaseException):
    pass


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
        cfg = base.normalize_config(
            base.Config(
                input_path=str(source),
                output_folder=str(self.root / "output"),
                naming_mode="row_id",
                output_format="webdataset",
                compress_tar=False,
                enable_paarc=False,
                retry_backoff_sec=0,
                **options,
            )
        )
        frame, manifest = base.load_manifest(cfg)
        (self.root / "output").mkdir()
        store = protocol.RunStore(
            cfg.output_folder,
            manifest=manifest,
            config=protocol.effective_config(cfg),
            rows=protocol.plan_rows(frame, cfg, base.render_filename),
        )
        self.addCleanup(store.close)
        return cfg, frame, store

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
            sentinel = Path(cfg.output_folder) / "user-file"
            sentinel.write_bytes(b"preserve")
            with self.assertRaises(protocol.IntegrityError):
                base.validate_and_load(
                    replace(cfg, input_path=str(self.root / "conflicting.parquet"), force_overwrite=True)
                )
            self.assertEqual(sentinel.read_bytes(), b"preserve")

    def test_empty_and_all_invalid_inputs_reconcile_without_http(self):
        for values in ([], [None, "", " ", "ftp://fixture.invalid/a", "http://"]):
            with self.subTest(values=values):
                source, output = (
                    self.root / "empty.parquet",
                    self.root / ("empty" if not values else "invalid"),
                )
                pl.DataFrame({"url": values}, schema={"url": pl.String}).write_parquet(source)
                cfg = base.Config(input_path=str(source), output_folder=str(output), research_profile=True)
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
            frame,
            replace(cfg, naming_mode="url_based", file_name_pattern="{segment_last}"),
            base.render_filename,
        )
        self.assertEqual([r["initial_disposition"] for r in planned], ["failed"] * 3)
        self.assertTrue(all("collision" in r["error"] for r in planned))
        safe = protocol.plan_rows(frame.head(2), cfg, base.render_filename)
        self.assertEqual(len({r["payload"] for r in safe}), 2)
        self.assertTrue(all(r["initial_disposition"] is None for r in safe))

    def test_reserved_labels_and_encoded_separators_are_rejected(self):
        for label in (".", "..", "/tmp", "a/b", "a\\b", "%2fetc", "%252e%252e", ".flowdc", "overview.json"):
            frame = protocol.stamp_frame(
                pl.DataFrame({"url": ["http://fixture.invalid/a.jpg"], "label": [label]}), "a" * 64
            )
            cfg = base.Config(input_path="unused", output_folder="unused", label_col="label")
            rows = protocol.plan_rows(frame, cfg, base.render_filename)
            self.assertEqual(rows[0]["initial_disposition"], "failed", label)
        for name in ("overview.json", "outcome-index.json", ".flowdc"):
            cfg = base.Config(
                input_path="unused",
                output_folder="unused",
                output_format="webdataset",
                naming_mode="url_based",
                file_name_pattern=name,
            )
            rows = protocol.plan_rows(frame, cfg, lambda pattern, url, key: pattern)
            self.assertEqual(rows[0]["initial_disposition"], "failed", name)

    def test_symlink_parent_and_existing_targets_are_preserved(self):
        cfg, frame, store = self.setup_run()
        key = frame["__key__"][0]
        path = Path(cfg.output_folder) / store.rows[key]["payload"]
        sentinel = self.root / "sentinel"
        sentinel.write_bytes(b"user evidence")
        path.symlink_to(sentinel)
        with self.assertRaises(protocol.IntegrityError):
            store.begin(key)
        self.assertEqual(sentinel.read_bytes(), b"user evidence")
        self.assertEqual(store.reconcile()["counts"]["failed"], 1)
        outside = self.root / "outside"
        outside.mkdir()
        (Path(cfg.output_folder) / "escape").symlink_to(outside, target_is_directory=True)
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
                cfg = base.Config(
                    input_path="unused",
                    output_folder=str(output),
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
                    rows=protocol.plan_rows(frame, cfg, base.render_filename),
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
        path = Path(cfg.output_folder) / store.rows[key]["payload"]
        path.write_bytes(b"same bytes")
        store.fault = lambda _: None
        result = store.reconcile()
        self.assertEqual(result["counts"]["failed"], 1)
        self.assertEqual(result["verified_payload_bytes"], 0)
        self.assertEqual(path.read_bytes(), b"same bytes")
        self.assertFalse(result["rows"][0]["retryable"])

    def test_corrupt_and_truncated_ownership_fail_closed(self):
        cfg, _, store = self.setup_run()
        original = (Path(cfg.output_folder) / ".flowdc/owner.json").read_bytes()
        store.close()
        for raw in (b'{"schema_version":', original.replace(b'"schema_version":2', b'"schema_version":999')):
            (Path(cfg.output_folder) / ".flowdc/owner.json").write_bytes(raw)
            with self.assertRaises(protocol.IntegrityError):
                protocol.RunStore(cfg.output_folder)
            with self.assertRaises(protocol.IntegrityError):
                base.validate_and_load(replace(cfg, force_overwrite=True))
            self.assertEqual((Path(cfg.output_folder) / ".flowdc/owner.json").read_bytes(), raw)

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
        body = b"a" * 500_000
        for key in frame["__key__"]:
            store.publish(store.begin(key), key, body)
        snapshot = store.reconcile()
        self.assertEqual(snapshot["verified_payload_bytes"], 1_000_000)
        self.assertEqual(snapshot["unique_content_bytes"], 500_000)
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
                candidate += b"garbage" + b"\0" * 1024
            with self.subTest(mode=mode), self.assertRaises((ValueError, tarfile.TarError)):
                protocol.verify_archive(candidate, snapshot, store.root.name)

    def test_real_child_kill_recovers_publication(self):
        output = self.root / "child"
        output.mkdir()
        code = """
import sys, time
from pathlib import Path
sys.path.insert(0, sys.argv[1])
import polars as pl
import download_batch as b
import flowdc_integrity as p
root = sys.argv[2]
f = p.stamp_frame(pl.DataFrame({'url':['http://fixture.invalid/a.jpg']}), 'a'*64)
c = b.Config(input_path='unused', output_folder=root, naming_mode='row_id', output_format='webdataset')
def fault(step):
    if step == 'payload_published':
        print('ready to kill', flush=True)
        time.sleep(60)
with p.RunStore(root, manifest={'sha256':'a'*64,'original_rows':1}, config=p.effective_config(c), rows=p.plan_rows(f,c,b.render_filename), fault=fault) as s:
    k = f['__key__'][0]
    s.publish(s.begin(k), k, b'child truth')
"""
        child = subprocess.Popen(
            [sys.executable, "-B", "-c", code, str(ROOT / "bin"), str(output)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(child.stdout, selectors.EVENT_READ)
                self.assertTrue(selector.select(10), "child did not reach publication")
            self.assertEqual(child.stdout.readline(), b"ready to kill\n")
            child.kill()
            child.communicate(timeout=5)
            self.assertEqual(child.returncode, -signal.SIGKILL)
        finally:
            if child.poll() is None:
                child.kill()
            child.communicate(timeout=5)
        with protocol.RunStore(output) as store:
            snapshot = store.reconcile()
            self.assertEqual(snapshot["counts"]["verified"], 1)
            self.assertEqual(snapshot["verified_payload_bytes"], len(b"child truth"))
            self.assertEqual(snapshot, store.reconcile())

    def test_resume_skips_committed_rows_and_retains_attempt_budget(self):
        source, output = self.root / "resume.parquet", self.root / "resume"
        pl.DataFrame(
            {"url": ["http://fixture.invalid/one.jpg", "http://fixture.invalid/two.jpg"]}
        ).write_parquet(source)
        cfg = base.Config(
            input_path=str(source),
            output_folder=str(output),
            create_tar=False,
            enable_paarc=False,
            concurrent_downloads=1,
            retry_backoff_sec=0,
            max_retry_attempts=2,
        )
        calls = []

        async def fetch(session, url, timeout):
            calls.append(url)
            if url.endswith("two.jpg") and calls.count(url) == 1:
                base.shutdown_flag = True
                return None, 503, "HTTP 503", None
            return b"payload", 200, None, None

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
            asyncio.run(base.run_acquisition(replace(cfg, resume=True, timeout_sec=99)))
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
            first = (Path(cfg.output_folder) / "outcome-index.json").read_bytes()
            asyncio.run(base.run_acquisition(replace(cfg, reconcile=True)))
            self.assertEqual((Path(cfg.output_folder) / "outcome-index.json").read_bytes(), first)
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
        store.close()
        path = base.create_tar(cfg.output_folder, compress=False)
        with tarfile.open(path) as archive:
            names = archive.getnames()
            self.assertEqual(len(names), 6)
            self.assertFalse(any(".flowdc" in name or "unowned.txt" in name for name in names))
        with protocol.RunStore(cfg.output_folder) as reopened:
            reopened.fs.mkdir(".flowdc/attempts/" + "a" * 64)
            with self.assertRaisesRegex(protocol.IntegrityError, "unknown row"):
                reopened.reconcile()
            self.assertEqual(reopened.fs.read("unowned.txt"), b"preserve but do not archive")
    def test_managed_metadata_and_stat_failures_keep_body_observation(self):
        for module in (base, gradient):
            for failure in ("metadata", "stat"):
                with (
                    self.subTest(module=module.__name__, failure=failure),
                    tempfile.TemporaryDirectory(dir=self.root) as tmp,
                ):
                    output = Path(tmp) / "out"
                    output.mkdir()
                    frame = protocol.stamp_frame(
                        pl.DataFrame({"url": ["http://fixture.invalid/a.jpg"]}), "a" * 64
                    )
                    cfg = base.Config(
                        input_path="unused",
                        output_folder=str(output),
                        output_format="webdataset",
                        naming_mode="row_id",
                    )
                    with protocol.RunStore(
                        output,
                        manifest={"sha256": "a" * 64, "original_rows": 1},
                        config=protocol.effective_config(cfg),
                        rows=protocol.plan_rows(frame, cfg, base.render_filename),
                    ) as store:

                        async def exercise(module=module, failure=failure, frame=frame, cfg=cfg):
                            manager = module.HostControllerManager(module.PAARCConfig())

                            async def fetch(*_):
                                single.HTTP_TRACE_CTX.get().update(
                                    ttfb=0.2,
                                    latency_eligible=True,
                                    body_completed_at=1,
                                    observed_response_body_bytes=5,
                                )
                                return b"truth", 200, None, None

                            verify = store.fs.verify

                            def fail_stat(path, size, sha):
                                if path.endswith("/payload"):
                                    self.assertEqual(store.fs.read(path), b"truth")
                                    raise OSError("injected verification stat failure")
                                return verify(path, size, sha)

                            def fault(step):
                                if step == "payload_staged" and failure == "metadata":
                                    store.fs.mkdir(store.attempts(frame["__key__"][0])[0] + "/metadata")

                            store.fault = fault
                            with (
                                patch.object(single, "download_via_http_get", side_effect=fetch),
                                patch.object(
                                    store.fs, "verify", side_effect=fail_stat if failure == "stat" else verify
                                ),
                            ):
                                out = await base.download_one(
                                    row=frame.row(0, named=True),
                                    cfg=cfg,
                                    session=None,
                                    total_bytes=[],
                                    manager=manager,
                                    sequential_namer=base.SequentialNamer(),
                                    global_written_paths={},
                                    store=store,
                                )
                            self.assertFalse(out.success)
                            self.assertEqual(out.bytes_downloaded, 0)
                            metrics = await (
                                await manager.get_controller("http://fixture.invalid")
                            ).metrics.finish_interval()
                            self.assertEqual(metrics["n_samples"], 1)
                            self.assertEqual(metrics["n_local_failures"], 1)
                            self.assertEqual(metrics["bytes"], 0)
                            self.assertFalse(metrics["has_overload"])

                        asyncio.run(exercise())
                        result = store.reconcile()
                        self.assertEqual(result["counts"]["failed"], 1)
                        self.assertEqual(result["verified_payload_bytes"], 0)
                        self.assertEqual(result["observed_response_body_bytes"], 5)

    def test_cancellation_leaves_uncertain_attempt_and_releases_permit(self):
        cfg, frame, store = self.setup_run(create_tar=False)

        async def exercise():
            started = asyncio.Event()
            manager = base.HostControllerManager(base.PAARCConfig())

            async def stall(*_):
                started.set()
                await asyncio.Future()

            with patch.object(single, "download_via_http_get", side_effect=stall):
                task = asyncio.create_task(
                    base.download_one(
                        row=frame.row(0, named=True),
                        cfg=cfg,
                        session=None,
                        total_bytes=[],
                        manager=manager,
                        sequential_namer=base.SequentialNamer(),
                        global_written_paths={},
                        store=store,
                    )
                )
                await asyncio.wait_for(started.wait(), 2)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
            self.assertEqual((await manager.get_controller("http://fixture.invalid")).semaphore.inflight, 0)

        asyncio.run(exercise())
        row = store.reconcile()["rows"][0]
        self.assertEqual(row["disposition"], "failed")
        self.assertTrue(row["attempt_information_uncertain"])
        self.assertEqual(row["attempt_intents"], 1)

    def test_exhausted_attempts_do_not_restart_on_resume(self):
        cfg, frame, store = self.setup_run(max_retry_attempts=2, create_tar=False)
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
        source, output = self.root / "force.parquet", self.root / "force"
        pl.DataFrame({"url": ["http://fixture.invalid/a.jpg"]}).write_parquet(source)
        cfg = base.Config(input_path=str(source), output_folder=str(output), enable_paarc=False)
        for body, force in ((b"first", False), (b"second", True)):
            with patch.object(
                single, "download_via_http_get", AsyncMock(return_value=(body, 200, None, None))
            ):
                report = asyncio.run(base.run_acquisition(replace(cfg, force_overwrite=force)))
            self.assertEqual(report["summary"]["verified_payload_bytes"], len(body))
        self.assertEqual(next((output / "output").glob("*.jpg")).read_bytes(), b"second")

    def test_real_partition_cli_preserves_parent_rows_and_metadata(self):
        source, output = self.root / "parent.parquet", self.root / "partitions"
        pl.DataFrame(
            {"url": [None, "http://a.invalid/a", "http://b.invalid/b"], "label": ["x", "x", "y"]}
        ).write_parquet(source)
        for method in ("simple", "host", "greedy"):
            selected = output / method
            result = subprocess.run(
                [
                    sys.executable,
                    "-B",
                    str(ROOT / "bin/SplitParquet.py"),
                    "--parquet",
                    str(source),
                    "--url_col",
                    "url",
                    "--groups",
                    "2",
                    "--output_folder",
                    str(selected),
                    "--method",
                    method,
                    "--grouping_col",
                    "label",
                ],
                capture_output=True,
                timeout=10,
            )
            self.assertEqual(result.returncode, 0, result.stderr.decode())
            rows = pl.concat([pl.read_parquet(p) for p in sorted(selected.glob("*.parquet"))])
            self.assertEqual(rows.height, 3)
            stamped = protocol.stamp_frame(rows, "b" * 64).sort(protocol.PROVENANCE[1])
            self.assertEqual(stamped["label"].to_list(), ["x", "x", "y"])
            self.assertEqual(stamped[protocol.PROVENANCE[1]].to_list(), [0, 1, 2])
            self.assertEqual(
                stamped[protocol.PROVENANCE[0]].unique().to_list(),
                [hashlib.sha256(source.read_bytes()).hexdigest()],
            )

    def test_schema_two_experiment_parser_and_integer_adapter(self):
        from flowdc_experiment_artifacts import bundle, validate_case

        from benchmark.core.flowdc_adapter import FlowDCAdapter, FlowDCConfig
        from benchmark.core.metrics import ResourceMetrics

        cfg, frame, store = self.setup_run()
        for key in frame["__key__"]:
            store.publish(store.begin(key), key, b"a" * 500_000)
        snapshot = store.reconcile()

        def factory(c, outcomes, elapsed):
            return base.generate_overview_report(cfg=c, df_total=2, outcomes=outcomes, elapsed_sec=1)

        overview = base.integrity_report(cfg, store, snapshot, 1, factory)
        path, _, _ = store.make_archive(snapshot, overview, compress=True)
        case = {"name": "case", "config": {"enable_paarc": False}}
        parts = [
            {
                "name": "part-000.parquet",
                "rows": 2,
                "manifest_sha256": snapshot["manifest"]["sha256"],
                "row_ids": frame["__key__"].to_list(),
                "expected_sha256": [hashlib.sha256(b"a" * 500_000).hexdigest()] * 2,
            }
        ]
        files = {
            "output_part-000.tar.gz": Path(path).read_bytes(),
            "tasks.json": protocol.encode(
                {
                    "submitted": 1,
                    "tasks": [{"id": 1, "successful": True, "exit_code": 0, "log_truncated": False}],
                }
            ),
            "resolved-config.json": protocol.encode({"enable_paarc": False}),
            "task-1.log": b"done",
        }
        self.assertEqual(len(validate_case(bundle(files), case, parts, 8_000_000)), 1)
        report_path = self.root / "report.json"
        report_path.write_bytes(protocol.encode(overview))
        adapter_cfg = FlowDCConfig("unused", str(self.root), "url", None, 2, 5, False)
        adapted = FlowDCAdapter(ROOT)._parse_overview(report_path, adapter_cfg, 0, ResourceMetrics())
        self.assertEqual(adapted.total_bytes_downloaded, 1_000_000)
        # Historical MB fields retain their historical interpretation.
        overview.pop("report_schema_version")
        report_path.write_bytes(protocol.encode(overview))
        historical = FlowDCAdapter(ROOT)._parse_overview(report_path, adapter_cfg, 0, ResourceMetrics())
        self.assertEqual(historical.total_bytes_downloaded, 1_048_576)

    def test_taskvine_stages_new_dependency_and_forwards_research_profile(self):
        import importlib
        from unittest.mock import MagicMock

        with patch.dict(sys.modules, {"ndcctools": MagicMock(), "ndcctools.taskvine": MagicMock()}):
            vine = importlib.import_module("TaskvineFLOWDC")
            manager, task = MagicMock(), MagicMock()
            with patch.object(vine.vine, "Task", return_value=task):
                count = vine.submit_tasks(
                    manager,
                    str(ROOT / "bin/download_batch.py"),
                    str(ROOT / "bin/single_download.py"),
                    {"part.parquet": "declared"},
                    {"research_profile": True},
                    str(self.root),
                )
            self.assertEqual(count, 1)
            manager.declare_file.assert_any_call(str(ROOT / "bin/flowdc_integrity.py"))
            self.assertIn("flowdc_integrity.py", [call.args[1] for call in task.add_input.call_args_list])
            self.assertIn("output_part.tar", [call.args[1] for call in task.add_output.call_args_list])
            config = vine.create_partition_config({"research_profile": True}, "part", "output")
            self.assertTrue(config["research_profile"])

    def test_both_entrypoints_parse_recovery_and_reject_overwrite(self):
        for module in (base, gradient):
            with patch.object(sys, "argv", ["downloader", "--output", "unused", "--reconcile"]):
                cfg = module.parse_args()
            self.assertTrue(cfg.reconcile)
            with self.assertRaises(ValueError):
                base.normalize_config(replace(cfg, force_overwrite=True))
            with patch.object(
                sys, "argv", ["downloader", "--input", "unused", "--output", "unused", "--research_profile"]
            ):
                cfg = base.normalize_config(module.parse_args())
            self.assertEqual(
                (cfg.naming_mode, cfg.output_format, cfg.compress_tar), ("row_id", "webdataset", False)
            )


if __name__ == "__main__":
    unittest.main()
