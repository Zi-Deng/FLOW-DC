"""V1 origin/scheduling/inference fixtures; real HTTP checks use benchmark/study.py."""

import copy
import json
import math
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from benchmark import study as harness  # noqa: E402
from benchmark.core.controlled_origin import (  # noqa: E402
    SCENARIOS,
    ServiceModel,
    audit_events,
    public_scenario,
    scenario,
)
from benchmark.core.study import (  # noqa: E402
    LEGACY_METHODS as METHODS,
    authorize_plan,
    freeze_protocol,
    make_plan,
    method_configs,
    paired_summary,
    precision_plan,
    t_critical,
    tuning_catalog,
    validate_plan,
)
from benchmark.core.truth import digest, encode  # noqa: E402


class OriginModelTests(unittest.TestCase):
    def test_capacity_drop_is_nonpreemptive_and_recovery_releases_fifo_queue(self):
        events = []
        model = ServiceModel([[0, 2], [1, 1], [3, 3]], 2, events.append)
        self.assertEqual(model.arrive(0, 1, "/a"), "service")
        self.assertEqual(model.arrive(0, 2, "/b"), "service")
        self.assertEqual(model.arrive(0, 3, "/c"), "queued")
        self.assertEqual(model.arrive(0, 4, "/d"), "queued")
        self.assertEqual(model.arrive(0, 5, "/e"), "rejected")
        model.advance(1)
        self.assertEqual(model.active, {1, 2})  # Issued service survives capacity reduction.
        model.finish(1.1, 1)
        self.assertEqual(model.active, {2})
        self.assertEqual(list(model.queue), [3, 4])
        model.finish(1.2, 2)
        self.assertEqual(model.active, {3})
        model.advance(3)
        self.assertEqual(model.active, {3, 4})
        self.assertEqual([e["request_id"] for e in events if e["phase"] == "service_start"], [1, 2, 3, 4])
        self.assertEqual([e["slots"] for e in events if e["phase"] == "capacity"], [2, 1, 3])

    def test_stop_cancels_queue_without_starting_it_and_state_is_per_origin(self):
        first, second = ServiceModel([[0, 1]], 2, lambda _: None), ServiceModel([[0, 2]], 0, lambda _: None)
        first.arrive(0, 1, "/a")
        first.arrive(0, 2, "/b")
        first.stop(1)
        first.finish(1.1, 1)
        self.assertEqual(first.states[2], "cancelled")
        self.assertEqual(first.arrive(2, 3, "/c"), "rejected")
        self.assertEqual(second.arrive(2, 1, "/a"), "service")
        self.assertEqual(second.states, {1: "service"})

    def test_independent_event_replay_detects_overcap_and_missing_response(self):
        events = []

        def emit(record):
            events.append(
                {
                    "sequence": len(events) + 1,
                    "origin_monotonic_ns": int(record["origin_elapsed_s"] * 1e9),
                    **record,
                }
            )

        model = ServiceModel([[0, 1]], 0, emit)
        model.arrive(0, 1, "/a")
        model.finish(1, 1)
        emit({"phase": "response", "origin_elapsed_s": 1, "request_id": 1})
        snapshot = {"instrumented": True, "requests": 1, "responses": 1}
        self.assertEqual(audit_events(events, snapshot)["status"], "verified")
        with self.assertRaisesRegex(ValueError, "unaccounted"):
            audit_events(events[:-1], snapshot)
        bad = copy.deepcopy(events)
        bad[0]["slots"] = 0
        with self.assertRaisesRegex(ValueError, "capacity"):
            audit_events(bad, snapshot)

    def test_scenario_catalog_is_independent_and_within_budget(self):
        payloads = {"JPEG": b"j" * 10, "PNG": b"p" * 20, "largePNG": b"p" * 10000}
        for name in SCENARIOS:
            plan = scenario(name, payloads)
            public = public_scenario(plan)
            self.assertLessEqual(len(plan["assignments"]), 256)
            self.assertNotIn("payload", next(iter(public["objects"].values())))
            self.assertTrue(
                all(
                    spec["sha256"] == digest(plan["objects"][path]["payload"])
                    for path, spec in public["objects"].items()
                )
            )
        balanced = scenario("balanced", payloads, rows=100)
        skewed = scenario("skewed", payloads, rows=100)
        self.assertEqual(sum(r["origin"] for r in balanced["assignments"]), 50)
        self.assertEqual(sum(r["origin"] for r in skewed["assignments"]), 10)
        with self.assertRaises(ValueError):
            scenario("mixed-sizes", {**payloads, "largePNG": b"x" * 1_000_000}, rows=256)


class PlansAndInferenceTests(unittest.TestCase):
    def test_blocked_order_reproducible_complete_and_namespace_isolation(self):
        plan = make_plan(seed=42)
        self.assertEqual(plan, make_plan(seed=42))
        self.assertNotEqual(plan["cells"], make_plan(seed=43)["cells"])
        self.assertEqual(len(plan["cells"]), 72)
        for block in range(6):
            for family in plan["families"]:
                cells = [
                    cell for cell in plan["cells"] if cell["block"] == block and cell["scenario"] == family
                ]
                self.assertEqual({cell["method"] for cell in cells}, set(METHODS))
                self.assertEqual(len({cell["fixture_seed"] for cell in cells}), 1)
        tuning = make_plan(seed=42, namespace="tuning")
        self.assertNotEqual(tuning["cells"][0]["fixture_seed"], plan["cells"][0]["fixture_seed"])
        self.assertTrue(
            set(cell["cell_id"] for cell in plan["cells"]).isdisjoint(
                cell["cell_id"] for cell in tuning["cells"]
            )
        )
        edited = copy.deepcopy(plan)
        edited["cells"].reverse()
        with self.assertRaisesRegex(ValueError, "order"):
            validate_plan(edited)

    def test_proposed_candidates_are_fixed_before_evaluation_and_config_hashes_enforced(self):
        catalog = tuning_catalog()
        self.assertEqual(catalog["status"], "proposal_only")
        for method, candidates in catalog["candidates"].items():
            self.assertEqual(len(candidates), 8)
            for candidate in candidates:
                configs = method_configs(legacy=True)
                configs[method] = candidate["config"]
                plan = make_plan(seed=1, configurations=configs)
                validate_plan(plan)
                self.assertEqual(candidate["sha256"], digest(encode(candidate["config"])))
        plan["cells"][0]["config_sha256"] = "f" * 64
        with self.assertRaises(ValueError):
            validate_plan(plan)

    def test_confirmatory_gate_requires_explicit_provenance_bound_to_source_environment_plan(self):
        plan = make_plan(seed=7, purpose="confirmatory")
        identity = {"source_sha256": "a" * 64, "environment_sha256": "b" * 64}
        for protocol in (None, {"approved": True}, {"status": "frozen"}):
            with self.assertRaises(ValueError):
                authorize_plan(plan, protocol, **identity)
        decisions = {
            "approved_plan_sha256": digest(encode(plan)),
            "provenance": "test fixture only; no real advisor approval",
            "constraints": "test-only explicit constraints",
            "estimand": "test-only paired run differences",
            "repetition_rule": "test-only fixed block count before execution",
        }
        frozen = freeze_protocol(plan, decisions, **identity)
        authorize_plan(plan, frozen, **identity)
        with self.assertRaisesRegex(ValueError, "mismatch"):
            authorize_plan(plan, frozen, **{**identity, "source_sha256": "c" * 64})
        with self.assertRaises(ValueError):
            authorize_plan(make_plan(seed=8, purpose="confirmatory"), frozen, **identity)
        with self.assertRaises(ValueError):
            authorize_plan(
                make_plan(seed=7, purpose="engineering", namespace="engineering", blocks=2), None, **identity
            )

    def test_student_t_reference_quantiles_and_paired_ci(self):
        # Two-sided 95% quantiles: df=1,2,5,30.
        for n, expected in ((2, 12.706204736), (3, 4.302652730), (6, 2.570581836), (31, 2.042272456)):
            self.assertAlmostEqual(t_critical(n), expected, places=8)
        pairs = [{"reference": 100, "candidate": 100 + difference} for difference in (0, 1, -2, 2, -3, 6)]
        result = paired_summary(pairs)
        self.assertEqual(result["status"], "estimated")
        self.assertAlmostEqual(result["mean_difference"], 2 / 3)
        self.assertAlmostEqual(result["half_width"], 2.570581835636312 * math.sqrt(10.266666666666667 / 6))
        self.assertEqual(precision_plan(result)["required_total_pairs"], 6)

    def test_zero_failed_censored_undefined_and_insufficient_cases_are_not_dropped(self):
        zeros = [{"reference": 0, "candidate": 0, "reference_status": "nonzero_exit"} for _ in range(6)]
        result = paired_summary(zeros)
        self.assertEqual(result["planned_pairs"], 6)
        self.assertEqual(result["mean_difference"], 0)
        self.assertEqual(precision_plan(result)["status"], "zero_reference_mean")
        self.assertEqual(paired_summary(zeros[:5])["status"], "insufficient_pairs")
        for update, status in (
            ({"censored": True}, "censored_cells_present"),
            ({"candidate": None}, "undefined_cells_present"),
        ):
            pairs = copy.deepcopy(zeros)
            pairs[0].update(update)
            summary = paired_summary(pairs)
            self.assertEqual(summary["status"], status)
            self.assertEqual(len(summary["pairs"]), 6)
            self.assertIsNone(summary["ci"])
            self.assertEqual(precision_plan(summary)["status"], status)
        variable = paired_summary([{"reference": 1, "candidate": i * 1000} for i in range(6)])
        self.assertEqual(precision_plan(variable, max_pairs=10)["status"], "infeasible_within_bound")


class HarnessRecoveryTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.plan = make_plan(
            seed=10, blocks=1, families=["steady"], namespace="engineering", purpose="engineering", rows=4
        )
        self.environment = {"source_files_sha256": {"file": "a" * 64}, "packages": "test-only"}
        self.env_patch = patch.object(harness, "environment_record", return_value=self.environment)
        self.env_patch.start()
        self.addCleanup(self.env_patch.stop)

    def native(self, useful=0, status="nonzero_exit"):
        return {
            "native": {
                "status": status,
                "elapsed_ns": 1_000_000_000,
                "useful_payload_bytes": useful,
                "run_complete": status == "complete",
            },
            "origin_work": [],
        }

    def test_failed_cells_retained_resume_never_repeats_and_rerun_has_distinct_identity(self):
        with patch.object(harness, "execute_cell", return_value=self.native()) as execute:
            first = harness.run_cell(self.plan, self.root, 0)
            self.assertEqual(first["native"]["status"], "nonzero_exit")
            resumed = harness.run_cell(self.plan, self.root, 0, resume=True)
            self.assertEqual(resumed["status"], "already_recorded")
            self.assertEqual(execute.call_count, 1)
            execute.return_value = self.native(100, "complete")
            retry = harness.run_cell(self.plan, self.root, 0, resume=True, rerun=True)
            self.assertNotEqual(first["attempt_id"], retry["attempt_id"])
        summary = harness.summarize(self.plan, self.root)
        self.assertEqual(len(summary["planned_cells"]), 4)
        first_cell = summary["planned_cells"][0]
        self.assertEqual(len(first_cell["attempts"]), 2)
        self.assertEqual(first_cell["attempts"][0]["native"]["useful_payload_bytes"], 0)
        self.assertEqual(sum(cell["status"] == "pending" for cell in summary["planned_cells"]), 3)
        with self.assertRaisesRegex(ValueError, "collision"):
            harness.run_cell(self.plan, self.root, 0)

    def test_interrupted_setup_and_conflicting_source_are_retained_and_refused(self):
        with patch.object(harness, "execute_cell", side_effect=KeyboardInterrupt):
            record = harness.run_cell(self.plan, self.root, 0)
        self.assertEqual(record["status"], "interrupted")
        self.environment["source_files_sha256"]["file"] = "b" * 64
        with self.assertRaisesRegex(ValueError, "mismatch"):
            harness.run_cell(self.plan, self.root, 0, resume=True)

    def test_crash_recovery_does_not_rerun_uncertain_process_ownership(self):
        with patch.object(harness, "execute_cell", return_value=self.native()):
            record = harness.run_cell(self.plan, self.root, 0)
        attempt = self.root / "engineering" / self.plan["cells"][0]["cell_id"] / record["attempt_id"]
        record["status"], record["native"] = "started", None
        (attempt / "record.json").write_bytes(encode(record))
        (attempt / "run").mkdir()
        with patch.object(harness, "execute_cell") as execute:
            harness.run_cell(self.plan, self.root, 0, resume=True)
            self.assertEqual(json.loads((attempt / "record.json").read_bytes())["status"], "interrupted")
            with self.assertRaisesRegex(ValueError, "ownership uncertain"):
                harness.run_cell(self.plan, self.root, 0, resume=True, rerun=True)
            execute.assert_not_called()

    def test_cannot_bypass_planned_order_or_live_work_in_an_earlier_failed_cell(self):
        with patch.object(harness, "execute_cell", return_value=self.native()) as execute:
            with self.assertRaisesRegex(ValueError, "frozen ordering"):
                harness.run_cell(self.plan, self.root, 1)
            execute.assert_not_called()
            first = harness.run_cell(self.plan, self.root, 0, resume=True)
            attempt = self.root / "engineering" / self.plan["cells"][0]["cell_id"] / first["attempt_id"]
            (attempt / "run").mkdir()
            (attempt / "run/result.json").write_bytes(encode({"status": "cleanup_failed"}))
            (attempt / "run/process-owner.json").write_bytes(encode({"pgid": 12345}))
            with patch("benchmark.core.lifecycle._group_running", return_value=True):
                with self.assertRaisesRegex(ValueError, "active/uncertain"):
                    harness.run_cell(self.plan, self.root, 1, resume=True)
            self.assertEqual(execute.call_count, 1)


if __name__ == "__main__":
    unittest.main()
