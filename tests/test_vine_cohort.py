"""Finite owner launch contracts and independent dispatch audits (not a native gate)."""

import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bin"))
from flowdc_experiment_research import verify_worker_launch
from flowdc_vine_cohort import cohort, dispatch_audit, validate


class CohortTests(unittest.TestCase):
    def test_unique_slots_and_native_dispatch_ceiling_are_separate_from_admission(self):
        plan = cohort(2, replacements=(0,))
        self.assertNotEqual(plan["slots"][0]["feature"], plan["slots"][1]["feature"])
        dispatches = [{"task_id": key} for key in (1, 1, 1, 2, 2)]
        audit = dispatch_audit(plan, [1, 2], dispatches)
        self.assertTrue(audit["within_bound"])
        self.assertEqual([row["dispatch_limit"] for row in audit["tasks"]], [3, 2])
        self.assertFalse(dispatch_audit(plan, [1, 2], [*dispatches, {"task_id": 1}])["within_bound"])
        with self.assertRaisesRegex(ValueError, "unknown task"):
            dispatch_audit(plan, [1, 2], [{"task_id": 3}])

    def test_contract_refuses_duplicate_features_external_owners_and_guest_replacement(self):
        for kind in ("duplicate", "external", "guest-replacement", "unbounded", "boolean", "extra"):
            plan = cohort(2)
            if kind == "duplicate":
                plan["slots"][1]["feature"] = plan["slots"][0]["feature"]
            if kind == "external":
                plan["owner"] = "arbitrary-external-workers"
            if kind == "guest-replacement":
                plan["owner"] = "prepared-guest-service-v1"
                plan["slots"][0]["launch_limit"] = 2
            if kind in ("unbounded", "boolean"):
                plan["slots"][0]["launch_limit"] = 0 if kind == "unbounded" else True
            if kind == "extra":
                plan["ignore_launch_limit"] = True
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                validate(plan, 2)

    def test_guest_launch_receipt_binds_case_role_slot_and_nonrestarting_single_shot(self):
        plan = cohort(2, owner="prepared-guest-service-v1")
        receipt = {
            "schema": "flowdc-owned-worker-launch-v1",
            "role": "worker-3",
            "case": "case",
            "cohort": plan,
            "feature": plan["slots"][1]["feature"],
            "single_shot": True,
            "restart": "no",
            "launch_index": 0,
        }
        verify_worker_launch(receipt, plan, "worker-3", "case", 1)
        for field, value in (
            ("role", "worker"),
            ("case", "other"),
            ("single_shot", False),
            ("restart", "always"),
            ("launch_index", 1),
            ("feature", plan["slots"][0]["feature"]),
        ):
            bad = copy.deepcopy(receipt)
            bad[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                verify_worker_launch(bad, plan, "worker-3", "case", 1)
