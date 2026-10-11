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
from unittest.mock import patch
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bin"))
import flowdc_ops as ops
from flowdc_pilot_journal import allowance_binding_sha256, register
from flowdc_pilot_supervisor import Supervisor
from flowdc_pilot_supervisor import request as supervise_request
from flowdc_pilot_topology import apply, preview, sha, source_digest, transition
from flowdc_topology import from_legacy, role_names, roles, selected_roles, selection
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

    def test_four_one_two_selection_preserves_all_enrolled_accounts(self):
        apply(self.journal, self.request(4))
        registered = copy.deepcopy(self.journal.read()["spec"])
        workers = registered["topology"]["workers"]
        provider = FakeProvider()
        provider.states = dict.fromkeys(self.journal.read()["vms"], "SHELVED_OFFLOADED")
        supervisor = Supervisor(self.journal, provider, clock=self.clock)
        with self.journal.supervisor_lock():
            supervisor.recover()
            for chosen in (workers, workers[2:3], [workers[1], workers[3]]):
                before = self.journal.read()
                actions_start = len(provider.actions)
                ids = {registered["topology"]["manager"], registered["topology"]["origin"], *chosen}
                supervise_request(self.journal, "start", clock=self.clock, window=1200, worker_ids=chosen)
                again = self.journal.read()
                supervise_request(
                    self.journal, "start", clock=self.clock, window=1200, worker_ids=list(reversed(chosen))
                )
                self.assertEqual(self.journal.read(), again)
                with self.assertRaises(ops.OpsError):
                    supervise_request(
                        self.journal, "start", clock=self.clock, window=1200, worker_ids=[workers[0]]
                    )
                for _ in range(100):
                    supervisor.tick()
                    self.clock.advance()
                    current = self.journal.read()
                    if all(
                        current["vms"][key]["observed"]
                        and current["vms"][key]["observed"]["state"] == "ACTIVE"
                        for key in ids
                    ):
                        break
                self.assertEqual(
                    {vm_id for action, vm_id in provider.actions[actions_start:] if action == "unshelve"}, ids
                )
                self.assertTrue(all(provider.states[key] == "ACTIVE" for key in ids))
                from flowdc_experiment_transport import ready, remaining
                from flowdc_pilot_cli import status

                with patch("flowdc_pilot_cli.sample_clock", self.clock):
                    value, code = status(self.journal)
                self.assertEqual(code, 0)
                self.assertEqual(set(value["data"]["selected_ids"]), ids)
                self.assertTrue(ready(value["data"]))
                self.assertGreater(remaining(value["data"]), 0)
                for key in set(before["vms"]) - ids:
                    self.assertEqual(current["vms"][key]["account"], before["vms"][key]["account"])
                    self.assertEqual(provider.states[key], "SHELVED_OFFLOADED")
                self.clock.advance(30)
                supervise_request(self.journal, "stop", clock=self.clock)
                for _ in range(140):
                    supervisor.tick()
                    self.clock.advance()
                    if self.journal.read()["desired"] == "idle":
                        break
                after = self.journal.read()
                self.assertEqual(after["desired"], "idle")
                self.assertEqual(after["spec"], registered)
                self.assertEqual(set(after["vms"]), set(before["vms"]))
                self.assertEqual(after["events"][: len(before["events"])], before["events"])
                for key in before["vms"]:
                    self.assertEqual(
                        after["vms"][key]["account"]["limit"], before["vms"][key]["account"]["limit"]
                    )
                    self.assertFalse(after["vms"][key]["account"]["obligation"])
                    self.assertEqual(after["vms"][key]["observed"]["state"], "SHELVED_OFFLOADED")
                    if key in ids:
                        self.assertGreater(
                            after["vms"][key]["account"]["consumed"],
                            before["vms"][key]["account"]["consumed"],
                        )
                    else:
                        self.assertEqual(after["vms"][key]["account"], before["vms"][key]["account"])

    def test_exhausted_unselected_account_is_preserved_but_uncertainty_blocks(self):
        apply(self.journal, self.request(4))
        workers = self.journal.read()["spec"]["topology"]["workers"]
        self.journal.change(
            lambda r: r["vms"][workers[0]]["account"].update(
                consumed=r["vms"][workers[0]]["account"]["limit"]
            )
        )
        provider = FakeProvider()
        provider.states = dict.fromkeys(self.journal.read()["vms"], "SHELVED_OFFLOADED")
        supervisor = Supervisor(self.journal, provider, clock=self.clock)
        with self.journal.supervisor_lock():
            supervisor.recover()
            before = self.journal.read()
            supervise_request(self.journal, "start", window=1200, clock=self.clock, worker_ids=[workers[2]])
            for _ in range(20):
                supervisor.tick()
                self.clock.advance()
            self.assertEqual(
                self.journal.read()["vms"][workers[0]]["account"], before["vms"][workers[0]]["account"]
            )
            self.assertNotIn(("unshelve", workers[0]), provider.actions)
            supervise_request(self.journal, "stop", clock=self.clock)
            for _ in range(100):
                supervisor.tick()
                self.clock.advance()
                if self.journal.read()["desired"] == "idle":
                    break
            self.journal.change(lambda r: r["vms"][workers[0]]["account"].update(uncertain=True))
            before = self.journal.read()
            with self.assertRaises(ops.OpsError):
                supervise_request(
                    self.journal, "start", window=1200, clock=self.clock, worker_ids=[workers[2]]
                )
            self.assertEqual(self.journal.read(), before)

    def test_invalid_or_unenrolled_selection_cannot_change_any_account(self):
        apply(self.journal, self.request(4))
        supervisor = Supervisor(self.journal, FakeProvider(), clock=self.clock)
        with self.journal.supervisor_lock():
            supervisor.recover()
            before = self.journal.read()
            workers = before["spec"]["topology"]["workers"]
            for chosen in (
                [],
                workers[:3],
                [workers[0], workers[0]],
                [str(uuid4())],
                [before["spec"]["topology"]["manager"]],
                "worker",
                [None],
            ):
                with self.subTest(chosen=chosen), self.assertRaises(ops.OpsError):
                    supervise_request(self.journal, "start", clock=self.clock, worker_ids=chosen)
                self.assertEqual(self.journal.read(), before)

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

    def test_subset_network_never_attaches_unselected_ports_and_refuses_activation(self):
        from flowdc_pilot_journal import fresh_network
        from flowdc_pilot_provider import Provider
        from test_flowdc_pilot_network import NetworkBackend

        apply(self.journal, self.request(4))
        record = self.journal.read()
        provider = NetworkBackend()
        original = copy.deepcopy(next(iter(provider.ports.values())))
        for vm in record["spec"]["vms"]:
            interface = record["access"]["interfaces"][vm["role"]]
            provider.ports[interface["port_id"]] = dict(
                copy.deepcopy(original),
                id=interface["port_id"],
                device_id=vm["id"],
                fixed_ips=[{"ip_address": interface["fixed_ip"], "subnet_id": interface["subnet_id"]}],
            )
        workers = record["spec"]["topology"]["workers"]
        for chosen in (workers[2:3], [workers[1], workers[3]], workers):
            self.journal.change(
                lambda r, chosen=chosen: r.update(
                    selection=selection(r["spec"], chosen),
                    desired="run",
                    window={"seconds": 1200, "inspection": True},
                    network=fresh_network(),
                )
            )
            record = self.journal.read()
            expected = set(selected_roles(record))
            for _ in range(150):
                provider.network_step(self.journal, rollback=False)
                if self.journal.read()["network"]["ready"]:
                    break
            record = self.journal.read()
            self.assertTrue(record["network"]["ready"])
            self.assertEqual(set(record["network"]["original"]), expected)
            for role, interface in record["access"]["interfaces"].items():
                if role not in expected:
                    self.assertEqual(
                        provider.ports[interface["port_id"]]["security_group_ids"], [provider.original]
                    )
                else:
                    group = provider.groups[record["network"]["seen_groups"][role]]
                    provider.verify_ingress(record, role, group)
                    actual = {
                        rule["remote_ip_prefix"] for rule in group["rules"] if rule["direction"] == "ingress"
                    }
                    peers = {"manager"} if role.startswith("worker") else expected - {role}
                    wanted = {record["access"]["interfaces"][peer]["fixed_ip"] + "/32" for peer in peers}
                    if role == "manager":
                        wanted.add(record["access"]["operator_cidr"])
                    self.assertEqual(actual, wanted)
                    self.assertTrue(all(rule["protocol"] == "tcp" for rule in group["rules"]))
            self.assertEqual(sum(len(provider.groups[record["network"]["seen_groups"][role]]["rules"])
                for role in expected), 3 * len(chosen) + 3)
            if len(chosen) < len(workers):
                excluded = next(worker for worker in workers if worker not in chosen)
                with self.assertRaises(ops.OpsError) as caught:
                    Provider({}).lifecycle(record, excluded, "unshelve")
                self.assertEqual(caught.exception.code, "vm_not_selected")
            self.journal.change(lambda r: r.update(desired="stop"))
            for _ in range(100):
                provider.network_step(self.journal, rollback=True)
                if self.journal.read()["network"]["rolled_back"]:
                    break
            self.assertTrue(self.journal.read()["network"]["rolled_back"])
            self.assertTrue(
                all(port["security_group_ids"] == [provider.original] for port in provider.ports.values())
            )

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
