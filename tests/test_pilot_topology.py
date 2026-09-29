"""Versioned topology, crash-safe migration and every-VM cleanup; all fixture-only."""

import copy
import json
import os
import sqlite3
import sys
import tempfile
import unittest
from dataclasses import asdict
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bin"))
import flowdc_ops as ops
from flowdc_pilot_journal import allowance_binding_sha256, register
from flowdc_pilot_supervisor import Supervisor
from flowdc_pilot_supervisor import request as supervise_request
from flowdc_pilot_topology import apply, preview, sha, source_digest, transition
from flowdc_topology import from_legacy, role_names, roles
from test_flowdc_pilot_lifecycle import FakeClock, FakeProvider, access, spec, synthetic_service


def expanded(selected, network, workers):
    selected, network = from_legacy(selected), copy.deepcopy(network)
    network["schema_version"] = 2
    for i in range(2, workers + 1):
        vm_id, role = str(uuid4()), f"worker-{i}"
        selected["vms"].append({"id": vm_id, "role": role, "active_seconds": 1800, "rate": None})
        selected["topology"]["workers"].append(vm_id)
        interface = copy.deepcopy(network["interfaces"]["worker"])
        interface.update(port_id=str(uuid4()), fixed_ip=f"10.0.0.{20 + i}")
        network["interfaces"][role] = interface
    return selected, network


class TopologyTests(unittest.TestCase):
    def setUp(self):
        mask = os.umask(0o077)
        self.addCleanup(os.umask, mask)
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        config = self.root / "config"
        config.mkdir()
        profile = config / "profile.json"
        profile.write_text("{}")
        self.journal = register(profile, self.root / "state", spec(), access())
        self.clock = FakeClock()

        def settled(record):
            record["service"] = synthetic_service()
            record["events"].append({"kind": "historical", "data": {"keep": True}})
            for i, vm in enumerate(record["vms"].values()):
                vm["account"]["consumed"] = 123.125 + i
                vm["observed"] = {"state": "SHELVED_OFFLOADED", "clock": asdict(self.clock())}

        self.journal.change(settled)
        self.before = self.journal.read()

    def request(self, workers=4):
        before = self.journal.read()
        selected, network = expanded(before["spec"], before["access"], workers)
        return {
            "schema_version": 1,
            "migration_id": str(uuid4()),
            "registration_id": before["registration_id"],
            "expected_state_sha256": sha(before),
            "expected_binding_sha256": allowance_binding_sha256(before),
            "expected_source_sha256": source_digest(),
            "spec": selected,
            "access": network,
            "new_worker_ids": sorted({v["id"] for v in selected["vms"]} - set(before["vms"])),
        }

    def test_legacy_interpretation_and_three_four_six_vm_validation(self):
        self.assertEqual(roles(spec()), ("manager", "worker", "origin"))
        for count in (1, 2, 4):
            selected, network = expanded(spec(), access(), count)
            self.assertEqual(roles(selected), role_names(count))
            self.assertEqual(len(ops.validate_spec_value(selected)["vms"]), count + 2)
            from flowdc_pilot_provider import validate_access_value

            self.assertEqual(len(validate_access_value(network)["interfaces"]), count + 2)
        selected, _ = expanded(spec(), access(), 4)
        selected["topology"]["workers"].reverse()
        with self.assertRaises(ValueError):
            roles(selected)
        for count in (0, 3, 5, True):
            with self.assertRaises(ValueError):
                role_names(count)

    def test_preview_is_read_only_and_apply_preserves_every_old_account_and_event(self):
        request = self.request()
        description = preview(self.journal, request)
        self.assertTrue(description["would_change"])
        self.assertEqual(self.journal.read(), self.before)
        receipt, changed = apply(self.journal, request)
        self.assertTrue(changed)
        after = self.journal.read()
        self.assertEqual(after["schema_version"], 3)
        self.assertEqual(after["events"][:-1], self.before["events"])
        self.assertEqual({key: after["vms"][key] for key in self.before["vms"]}, self.before["vms"])
        self.assertTrue(all(vm["account"]["limit"] <= 7200 for vm in after["vms"].values()))
        backup = json.loads((self.journal.root / receipt["backup"]).read_text())
        self.assertEqual(backup["record"], self.before)
        self.assertEqual(backup["record_sha256"], sha(self.before))
        self.assertEqual(apply(self.journal, request), (receipt, False))
        with sqlite3.connect(self.journal.root / "pilot.sqlite3") as db:
            self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], 3)

    def test_rejects_stale_identity_missing_history_active_uncertain_and_replay_conflicts(self):
        request = self.request()
        for kind in ("state", "source", "reassign", "delete", "active", "uncertain", "unrecorded"):
            candidate, value = copy.deepcopy(self.before), copy.deepcopy(request)
            if kind == "state":
                value["expected_state_sha256"] = "0" * 64
            if kind == "source":
                value["expected_source_sha256"] = "0" * 64
            if kind == "reassign":
                value["spec"]["vms"][0]["id"] = str(uuid4())
            if kind == "delete":
                value["spec"]["vms"].pop(0)
            if kind == "active":
                candidate["desired"] = "run"
            if kind == "uncertain":
                next(iter(candidate["vms"].values()))["account"]["uncertain"] = True
            if kind == "unrecorded":
                value["new_worker_ids"] = []
            with self.subTest(kind=kind), self.assertRaises(ops.OpsError):
                transition(candidate, value)
            self.assertEqual(self.journal.read(), self.before)
        apply(self.journal, request)
        with self.assertRaisesRegex(ops.OpsError, ""):
            apply(self.journal, {**request, "new_worker_ids": []})

    def test_crash_cuts_are_idempotent_and_never_reset_consumption(self):
        request = self.request(2)
        for point in ("backup_durable", "before_commit"):

            def fault(at, target=point):
                if at == target:
                    raise RuntimeError("crash cut")

            with self.assertRaisesRegex(RuntimeError, "crash cut"):
                apply(self.journal, request, fault=fault)
            self.assertEqual(self.journal.read(), self.before)
        with self.assertRaisesRegex(RuntimeError, "crash cut"):
            apply(
                self.journal,
                request,
                fault=lambda at: (
                    (_ for _ in ()).throw(RuntimeError("crash cut")) if at == "after_commit" else None
                ),
            )
        after = self.journal.read()
        self.assertFalse(apply(self.journal, request)[1])
        self.assertEqual(self.journal.read(), after)
        vm_id = next(iter(self.before["vms"]))
        self.journal.change(lambda value: value["vms"][vm_id]["account"].update(consumed=500))
        self.assertFalse(apply(self.journal, request)[1])
        self.assertEqual(self.journal.read()["vms"][vm_id]["account"]["consumed"], 500)

    def test_existing_grant_receipts_and_consumption_survive_migration(self):
        from test_flowdc_pilot_allowance import GrantTransitionTests

        granted = GrantTransitionTests()
        granted.setUp()
        self.addCleanup(granted.doCleanups)
        granted.apply()
        self.journal = granted.journal
        before = self.journal.read()
        apply(self.journal, self.request(4))
        after = self.journal.read()
        self.assertEqual(after["events"][:-1], before["events"])
        self.assertEqual({key: after["vms"][key] for key in before["vms"]}, before["vms"])

    def test_partial_unshelve_cleanup_covers_every_uuid(self):
        apply(self.journal, self.request(4))
        provider = FakeProvider()
        provider.states = dict.fromkeys(self.journal.read()["vms"], "SHELVED_OFFLOADED")
        provider.fail_unshelve = list(provider.states)[-1]
        supervisor = Supervisor(self.journal, provider, clock=self.clock)
        with self.journal.supervisor_lock():
            supervisor.recover()
            supervise_request(self.journal, "start", clock=self.clock)
            for _ in range(150):
                supervisor.tick()
                self.clock.advance()
                if self.journal.read()["desired"] == "idle":
                    break
        after = self.journal.read()
        self.assertEqual(after["desired"], "idle")
        self.assertIn(("unshelve", provider.fail_unshelve), provider.actions)
        self.assertTrue(
            all(
                vm["observed"]["state"] == "SHELVED_OFFLOADED" and not vm["account"]["obligation"]
                for vm in after["vms"].values()
            )
        )

    def test_mock_network_setup_and_cleanup_include_every_selected_port(self):
        from test_flowdc_pilot_network import NetworkBackend

        apply(self.journal, self.request(4))
        record = self.journal.read()
        provider = NetworkBackend()
        original_port = copy.deepcopy(next(iter(provider.ports.values())))
        for vm in record["spec"]["vms"]:
            interface = record["access"]["interfaces"][vm["role"]]
            provider.ports[interface["port_id"]] = {
                **copy.deepcopy(original_port),
                "id": interface["port_id"],
                "device_id": vm["id"],
                "fixed_ips": [{"ip_address": interface["fixed_ip"], "subnet_id": interface["subnet_id"]}],
            }
        self.journal.change(
            lambda record: record.update(desired="run", window={"seconds": 1800, "inspection": True})
        )
        # Per role: original snapshot, group, three rules per peer, attach,
        # and configuration receipt; plus bounded route/final readiness steps.
        maximum_steps = 6 * (5 + 3 * 5) + 10
        for _ in range(maximum_steps):
            provider.network_step(self.journal, rollback=False)
            if self.journal.read()["network"]["ready"]:
                break
        self.assertTrue(self.journal.read()["network"]["ready"])
        self.assertEqual(set(self.journal.read()["network"]["original"]), set(role_names(4)))
        self.journal.change(lambda record: record.update(desired="stop"))
        for _ in range(100):
            provider.network_step(self.journal, rollback=True)
            if self.journal.read()["network"]["rolled_back"]:
                break
        self.assertTrue(self.journal.read()["network"]["rolled_back"])
        self.assertTrue(
            all(port["security_group_ids"] == [provider.original] for port in provider.ports.values())
        )
        self.assertEqual(set(provider.groups), {provider.original})

    def test_old_sqlite_interpreter_refuses_new_schema_and_supervisor_lock_blocks_apply(self):
        request = self.request(2)
        with self.journal.supervisor_lock(), self.assertRaises(ops.OpsError):
            apply(self.journal, request)
        apply(self.journal, request)
        import importlib.util

        path = Path(__file__).parent / "fixtures/pilot_v1_connection.py"
        module_spec = importlib.util.spec_from_file_location("topology_old_connection", path)
        old = importlib.util.module_from_spec(module_spec)
        module_spec.loader.exec_module(old)
        with self.assertRaises(ops.OpsError) as caught, old.connection(self.journal):
            self.fail("old code opened a new-schema journal")
        self.assertEqual(caught.exception.code, "unsupported_journal")
        self.assertEqual(self.journal.read()["schema_version"], 3)

    def test_every_selected_vm_is_accounted_and_confirmed_offloaded(self):
        apply(self.journal, self.request(4))
        provider = FakeProvider()
        provider.states = dict.fromkeys(self.journal.read()["vms"], "SHELVED_OFFLOADED")
        supervisor = Supervisor(self.journal, provider, clock=self.clock)
        with self.journal.supervisor_lock():
            supervisor.recover()
            supervise_request(self.journal, "start", clock=self.clock)
            for _ in range(100):
                supervisor.tick()
                self.clock.advance()
                if all(
                    vm["observed"] and vm["observed"]["state"] == "ACTIVE"
                    for vm in self.journal.read()["vms"].values()
                ):
                    break
            self.assertEqual(
                {vm for action, vm in provider.actions if action == "unshelve"}, set(provider.states)
            )
            supervise_request(self.journal, "stop", clock=self.clock)
            for _ in range(120):
                supervisor.tick()
                self.clock.advance()
                if self.journal.read()["desired"] == "idle":
                    break
        after = self.journal.read()
        self.assertEqual(after["desired"], "idle")
        self.assertEqual(len(after["vms"]), 6)
        self.assertTrue(
            all(
                vm["observed"]["state"] == "SHELVED_OFFLOADED" and not vm["account"]["obligation"]
                for vm in after["vms"].values()
            )
        )
        self.assertTrue(all(vm["account"]["consumed"] > 0 for vm in after["vms"].values()))


if __name__ == "__main__":
    unittest.main()
