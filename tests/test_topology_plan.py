"""Offline sizing retains missing facts and preserves immutable example identities."""

import copy
import json
import tempfile
import unittest
from pathlib import Path

from benchmark.topology_plan import example, generate, sizing


class TopologyPlanTests(unittest.TestCase):
    def test_examples_preserve_accounts_across_shapes_and_do_not_invent_allowance_or_cost(self):
        previous = {}
        for count in (1, 2, 4):
            spec, _ = example(count)
            current = {vm["id"]: vm for vm in spec["vms"]}
            self.assertEqual({key: current[key] for key in previous}, previous)
            previous = current
            result = sizing(spec)
            self.assertFalse(result["activation_ready"])
            self.assertIsNone(result["conditional_total_su"])
            self.assertEqual(len(result["per_vm"]), count + 2)
            self.assertTrue(all(row["remaining_allowance_seconds"] is None for row in result["per_vm"]))
            self.assertEqual(result["maximum_stop_after_seconds"], {1: 1020, 2: 980, 4: 900}[count])

    def test_nonprefix_selection_preserves_input_and_scales_stop_budget(self):
        spec, _ = example(4)
        before = copy.deepcopy(spec)
        worker = next(vm["id"] for vm in spec["vms"] if vm["role"] == "worker-3")
        result = sizing(spec, [worker], 1200)
        self.assertEqual(spec, before)
        self.assertEqual(result["selection"]["worker_ids"], [worker])
        self.assertEqual([row["role"] for row in result["per_vm"]], ["manager", "worker-3", "origin"])
        self.assertEqual(result["maximum_stop_after_seconds"], 420)
        for window in (600, 780, 1801, True):
            with self.subTest(window=window), self.assertRaises(ValueError):
                sizing(spec, [worker], window)
        with self.assertRaises(ValueError):
            sizing(spec, [worker, worker])
        spec["vms"][0]["active_seconds"] = 900
        with self.assertRaises(ValueError):
            sizing(spec, [worker], 1200)

    def test_generator_refuses_collision_and_never_creates_runtime_state(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "examples"
            generate(output)
            before = {p.name: p.read_bytes() for p in output.iterdir()}
            self.assertEqual(len(before), 13)
            self.assertTrue(json.loads(before["README.json"])["fixture_only"])
            with self.assertRaises(FileExistsError):
                generate(output)
            self.assertEqual(before, {p.name: p.read_bytes() for p in output.iterdir()})
            self.assertFalse(list(output.rglob("*.sqlite3")))
