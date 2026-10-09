"""Finite workload boundaries, historical interpretation and explicit authority."""
import copy
import dataclasses
import json
import sys
import tempfile
import unittest
from pathlib import Path
from uuid import uuid4

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bin"))
sys.path.insert(0, str(ROOT))
from flowdc_research_profile import BodyBudget, LEGACY, RESEARCH, from_record, workload
from benchmark.core.truth import Truth, digest, encode, partition_truth, truth_workload
from benchmark.core.study import make_plan, tuning_catalog, validate_plan, freeze_protocol
from benchmark.core.controlled_origin import scenario
from flowdc_shared_state import Ledger
from download_batch import Config, load_manifest, normalize_config


class WorkloadTests(unittest.TestCase):
    def test_profile_record_cannot_change_limits_or_relabel_legacy(self):
        self.assertEqual(workload(), LEGACY)
        self.assertEqual(from_record(RESEARCH.record()), RESEARCH)
        bad = {**RESEARCH.record(), "max_rows": 10**9}
        with self.assertRaises(ValueError):
            from_record(bad)
        with self.assertRaises(ValueError):
            truth_workload({"schema": "flowdc-known-truth-v1", "workload": RESEARCH.record()})

    def test_larger_truth_requires_explicit_profile_and_preserves_parent_denominator(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "input.parquet"
            url = "http://fixture.test/image"
            pl.DataFrame({"url": [url] * 257, "label": ["duplicate"] * 257}).write_parquet(path)
            catalog = {url: {"bytes": 8, "sha256": digest(b"original")}}
            with self.assertRaises(ValueError):
                Truth.load(path, catalog)
            truth = Truth.load(path, catalog, research_workload=RESEARCH.name)
            self.assertEqual(truth.record["original_rows"], 257)
            scoped = partition_truth(truth.record, [truth.record["rows"][0]["row_id"]])
            self.assertEqual(scoped["original_rows"], 257)
            self.assertEqual(scoped["partition_rows"], 1)
            self.assertEqual(truth_workload(scoped), RESEARCH)
            # No large allocation is necessary to exercise the aggregate
            # independent catalog budget: duplicate original rows each count.
            with self.assertRaises(ValueError):
                Truth.load(path, {url: {"bytes": 3 * 2**20, "sha256": digest(b"original")}},
                           research_workload=RESEARCH.name)

    def test_invalid_metadata_and_rows_fail_before_forced_output_handling(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); input_path = root / "input.parquet"; output = root / "output"
            output.mkdir(); marker = output / "keep"; marker.write_text("original")
            config = normalize_config(Config(str(input_path), str(output), research_profile=True,
                research_workload=RESEARCH.name, control_method="fixed-v1", force_overwrite=True))
            pl.DataFrame({"url": ["http://fixture.test/image"], "nested": [[1, 2]]}).write_parquet(input_path)
            with self.assertRaises(ValueError):
                load_manifest(config)
            pl.DataFrame({"url": ["http://fixture.test/image"] * (RESEARCH.max_rows + 1)}).write_parquet(input_path)
            with self.assertRaises(ValueError):
                load_manifest(config)
            self.assertEqual(marker.read_text(), "original")

    def test_shared_profile_rows_and_reopen_are_bound(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder) / "ledger"
            binding = {"run_id": uuid4().hex, "source_sha256": "a" * 64,
                       "config_sha256": "b" * 64, "method": "fixed-v1"}
            ledger = Ledger(root, binding, research_workload=RESEARCH.name)
            rows = [digest(str(i).encode()) for i in range(257)]
            ledger.enroll(uuid4().hex, rows)
            self.assertEqual(len(next(iter(ledger.current()["scopes"].values()))["rows"]), 257)
            ledger.close()
            with self.assertRaises(ValueError):
                Ledger(root, binding, reopen=True)
            ledger = Ledger(root, binding, reopen=True, research_workload=RESEARCH.name)
            self.assertEqual(ledger.current()["phase"], "fenced")
            ledger.close()

    def test_five_method_plan_tuning_and_maintainer_provenance(self):
        plan = make_plan(seed=42, rows=1024, research_workload=RESEARCH.name)
        self.assertEqual(len(plan["cells"]), 90)
        self.assertIn("gradient2-application-delay-v1", plan["methods"])
        self.assertEqual(plan["families"], ["drop-recovery", "mixed-sizes", "sustained-overload"])
        validate_plan(plan)
        catalog = tuning_catalog(research_workload=RESEARCH.name)
        self.assertEqual({len(x) for x in catalog["candidates"].values()}, {8})
        decisions = {"approved_plan_sha256": digest(encode(plan)),
                     "provenance": "Maintainer accepted provisional defaults on October 9, 2026.",
                     "constraints": "Coverage loss <= .01; p95 delay increase <= .10.",
                     "estimand": "Verified original-row bytes / common end-to-end seconds.",
                     "repetition_rule": "Six paired pilot blocks; precision-derived confirmation frozen later.",
                     "approval_authority": "maintainer-provisional"}
        frozen = freeze_protocol(plan, decisions, source_sha256="a" * 64, environment_sha256="b" * 64)
        self.assertIn("advisor decisions pending", frozen["authority"])
        changed = copy.deepcopy(plan); changed["workload"]["max_rows"] += 1
        with self.assertRaises(ValueError):
            validate_plan(changed)

    def test_sparse_is_separate_from_interruption_and_overload_windows_are_explicit(self):
        payloads = {"JPEG": b"jpeg", "PNG": b"png"}
        sparse = scenario("sparse", payloads, rows=128, research_workload=RESEARCH.name)
        self.assertEqual(len(sparse["assignments"]), 128)
        self.assertTrue(all(x["responses"] == [{"status": 200}] for x in sparse["objects"].values()))
        self.assertEqual(sparse["overload_windows"], [])
        for name, interval in [("transient-overload", [10, 11]), ("sustained-overload", [10, 300])]:
            plan=scenario(name, payloads, research_workload=RESEARCH.name)
            self.assertEqual(plan["overload_windows"], [interval])
            self.assertEqual(plan["schema"], "flowdc-origin-scenario-v3")
            self.assertEqual(plan["schedule"][:2], [[0,4],[10,1]])
            self.assertEqual(plan["queue_bound"], 2)

    def test_overload_window_serves_low_load_and_rejects_only_queue_overflow(self):
        import concurrent.futures
        import threading
        import urllib.error
        import urllib.request
        from benchmark.core.controlled_origin import ControlledOrigin
        with tempfile.TemporaryDirectory() as folder:
            plan=scenario('sustained-overload',{'JPEG':b'known bytes','PNG':b'known bytes'},research_workload=RESEARCH.name)
            # Shorten only the fixture clock; exercise real HTTP admission in
            # the active overload window, not a synthetic response function.
            plan['schedule']=[[0,1]];plan['overload_windows']=[[0,10]]
            for spec in plan['objects'].values():spec.update(service_s=.5,tail_s=0)
            with ControlledOrigin(Path(folder),plan) as origin:
                url=origin.base_url+next(iter(plan['objects']))
                def fetch():
                    try:
                        with urllib.request.urlopen(url,timeout=5) as response:
                            response.read();return response.status
                    except urllib.error.HTTPError as error:return error.code
                self.assertEqual(fetch(),200)
                barrier=threading.Barrier(5)
                def concurrent_fetch():barrier.wait(timeout=5);return fetch()
                with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
                    statuses=list(pool.map(lambda _:concurrent_fetch(),range(5)))
                self.assertEqual(statuses.count(200),3)
                self.assertEqual(statuses.count(503),2)
            responses=[e for e in origin.events if e['phase']=='response']
            self.assertTrue(all(e['status']==503 for e in responses if e['overload_stimulus']))
            self.assertTrue(all(not e['overload_stimulus'] for e in responses if e['status']==200))

    def test_observed_byte_budget_counts_failed_attempts_and_limits_each_object(self):
        limits = dataclasses.replace(RESEARCH, max_object_bytes=8, max_payload_bytes=5, max_attempts=2)
        budget = BodyBudget(limits)
        budget.charge(5, 5)
        budget.charge(4, 4)
        with self.assertRaises(ValueError):
            budget.charge(2, 6)
        self.assertEqual(budget.observed_bytes, 9)
        with self.assertRaises(ValueError):
            budget.charge(1, 9)


if __name__ == "__main__":
    unittest.main()
