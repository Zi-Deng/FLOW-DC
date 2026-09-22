"""Exercise the real network state machine against a synthetic Neutron backend."""

import copy
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bin"))

import flowdc_ops as ops
from flowdc_pilot_journal import register
from flowdc_pilot_provider import Provider, validate_access
from test_flowdc_pilot_lifecycle import NETWORK, PORT_IDS, PROJECT, ROLES, SUBNET, VM_IDS, access, spec


class NetworkBackend(Provider):
    def __init__(self):
        super().__init__({}, before_activation=lambda: None)
        self.groups = {}
        self.fips = {}
        self.ports = {}
        self.calls = []
        self.counter = 100
        self.fail_action = None
        self.lose_response = None
        self.forbid_route = False
        self.external = "88888888-8888-4888-8888-888888888888"
        self.router = "99999999-9999-4999-8999-999999999999"
        self.router_port = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        self.original = "66666666-6666-4666-8666-666666666666"
        self.groups[self.original] = {
            "id": self.original,
            "name": "shared-original",
            "description": "shared",
            "project_id": PROJECT,
            "rules": [],
        }
        for index, (role, vm_id) in enumerate(zip(ROLES, VM_IDS, strict=True)):
            interface = access()["interfaces"][role]
            self.ports[PORT_IDS[index]] = {
                "id": PORT_IDS[index],
                "device_id": vm_id,
                "project_id": PROJECT,
                "network_id": NETWORK,
                "port_security_enabled": True,
                "allowed_address_pairs": [],
                "fixed_ips": [{"ip_address": interface["fixed_ip"], "subnet_id": SUBNET}],
                "security_group_ids": [self.original],
            }

    def new_id(self):
        self.counter += 1
        return f"77777777-7777-4777-8777-{self.counter:012d}"

    def context(self, record):
        pass

    def call(self, action, *args, mutation=False, on_dispatch=None):
        self.validate_call(action, args)
        if on_dispatch is not None:
            on_dispatch()
        self.calls.append((action, args, mutation))
        if action == self.fail_action:
            raise ops.OpsError("network_quota_pending", "fixed", "fixed", 3)
        if action == "groups":
            return [{"ID": key, "Name": value["name"]} for key, value in self.groups.items()]
        if action == "group":
            return copy.deepcopy(self.groups[args[0]])
        if action == "group_create":
            key = self.new_id()
            self.groups[key] = {
                "id": key,
                "name": args[0],
                "description": args[0],
                "project_id": PROJECT,
                "rules": [],
            }
        elif action == "rule":
            self.groups[args[0]]["rules"].append(
                {
                    "direction": "ingress",
                    "protocol": args[1],
                    "remote_ip_prefix": args[2],
                    "ethertype": "IPv4",
                    "port_range_min": 22 if args[3] == "22" else None,
                    "port_range_max": 22 if args[3] == "22" else None,
                }
            )
        elif action == "port":
            return copy.deepcopy(self.ports[args[0]])
        elif action == "ports":
            return [{"ID": key} for key, port in self.ports.items() if port["device_id"] == args[0]]
        elif action == "network":
            if args[0] == self.external:
                return {"id": self.external, "router:external": True}
            return {"id": NETWORK, "project_id": PROJECT}
        elif action == "subnet":
            return {"id": SUBNET, "network_id": NETWORK, "project_id": PROJECT, "host_routes": []}
        elif action == "attach":
            self.ports[args[0]]["security_group_ids"] = list(args[1:])
        elif action == "group_ports":
            return [
                {"ID": key} for key, value in self.ports.items() if args[0] in value["security_group_ids"]
            ]
        elif action == "group_delete":
            del self.groups[args[0]]
        elif action == "floating":
            if self.forbid_route:
                raise ops.OpsError("route_unverified", "fixed", "fixed", 3)
            return [{"ID": key} for key in self.fips]
        elif action == "floating_show":
            return copy.deepcopy(self.fips[args[0]])
        elif action == "router":
            return {
                "id": self.router,
                "project_id": PROJECT,
                "external_gateway_info": {"network_id": self.external},
            }
        elif action == "router_ports":
            self.ports[self.router_port] = {
                "id": self.router_port,
                "network_id": NETWORK,
                "device_id": self.router,
                "fixed_ips": [{"subnet_id": SUBNET}],
                "security_group_ids": [],
            }
            return [{"ID": self.router_port}]
        elif action == "floating_create":
            resource = self.new_id()
            self.fips[resource] = {
                "id": resource,
                "project_id": PROJECT,
                "description": args[0],
                "port_id": args[1],
                "fixed_ip_address": args[2],
                "floating_network_id": args[3],
            }
        elif action == "floating_delete":
            del self.fips[args[0]]
        else:
            raise AssertionError(action)
        if action == self.lose_response:
            raise ops.OpsError("lost_response", "fixed", "fixed", 3)
        return None


class NetworkTests(unittest.TestCase):
    def setUp(self):
        mask = os.umask(0o077)
        self.addCleanup(os.umask, mask)
        temp = tempfile.TemporaryDirectory(prefix="flowdc-network-test-")
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        config = self.root / "config"
        config.mkdir(mode=0o700)
        profile = config / "profile.json"
        profile.write_text("{}")
        self.journal = register(profile, self.root / "state", spec(), access())
        self.journal.change(
            lambda record: record.update(desired="run", window={"seconds": 1800, "inspection": True})
        )
        self.provider = NetworkBackend()

    def setup_network(self):
        for _ in range(60):
            self.provider.network_step(self.journal, rollback=False)
            if self.journal.read()["network"]["ready"]:
                return
        self.fail("network setup never finished")

    def rollback(self):
        self.journal.change(lambda record: record.update(desired="stop"))
        for _ in range(20):
            self.provider.network_step(self.journal, rollback=True)
            if self.journal.read()["network"]["rolled_back"]:
                return
        self.fail("rollback never finished")

    def test_creation_dispatch_boundary_and_recovered_rollback(self):
        from contextlib import nullcontext

        for mode in ("client", "credential", "guard", "dispatched"):
            with self.subTest(mode=mode):
                self.journal.change(lambda r: r["network"]["intents"].clear())
                self.journal.change(lambda r: r.update(desired="run"))
                provider = Provider({"openstack_client": "/absent", "credential_file": "/absent"})
                provider.activating = True
                provider.before_activation = lambda: None
                marker = "flowdc-" + self.journal.read()["network"]["generation"] + "-manager"
                error = ops.OpsError("provider_timeout", "fixed", "fixed", 3)
                if mode == "guard":
                    provider.before_activation = None
                with (
                    patch.object(
                        ops, "validate_client", side_effect=FileNotFoundError() if mode == "client" else None
                    ),
                    patch.object(
                        ops,
                        "private_file",
                        side_effect=PermissionError() if mode == "credential" else None,
                        return_value=nullcontext(123),
                    ),
                    patch.object(ops, "run_bounded", side_effect=error) as runner,
                ):
                    with self.assertRaises((ops.OpsError, OSError)):
                        provider.network_intent(self.journal, "group-manager", "group_create", marker)
                intent = self.journal.read()["network"]["intents"]["group-manager"]
                self.assertEqual(intent["not_sent"], mode != "dispatched")
                self.assertEqual(runner.call_count, int(mode == "dispatched"))
                if mode != "dispatched":
                    self.rollback()
                    self.assertTrue(self.journal.read()["network"]["rolled_back"])
                else:
                    with self.assertRaises(ops.OpsError) as caught:
                        self.rollback()
                    self.assertEqual(caught.exception.code, "unresolved_network_creation")
                    self.assertFalse(self.journal.read()["network"]["rolled_back"])

    def test_unsent_checkpoint_failure_preserves_original_and_ambiguous_intent(self):
        provider = Provider({"openstack_client": "/absent", "credential_file": "/absent"})
        marker = "flowdc-" + self.journal.read()["network"]["generation"] + "-manager"
        change = self.journal.change
        calls = 0

        def busy_result(update):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise ops.OpsError("pilot_state_busy", "fixed", "fixed", 3)
            return change(update)

        original = FileNotFoundError("synthetic")
        with (
            patch.object(self.journal, "change", side_effect=busy_result),
            patch.object(ops, "validate_client", side_effect=original),
        ):
            with self.assertRaises(FileNotFoundError) as caught:
                provider.network_intent(self.journal, "group-manager", "group_create", marker)
        self.assertIs(caught.exception, original)
        self.assertNotIn("not_sent", self.journal.read()["network"]["intents"]["group-manager"])

    def test_project_filters_use_compact_uuid_with_positive_results(self):
        self.configure_floating()
        self.setup_network()
        for value in (*self.provider.groups.values(), *self.provider.fips.values()):
            value["project_id"] = PROJECT.replace("-", "")
        record = self.journal.read()
        self.assertEqual(len(self.provider.owned_groups(record)), 3)
        self.assertEqual(len(self.provider.floating(record)), 1)
        for action, args, _ in self.provider.calls:
            if action in ("groups", "floating"):
                self.assertEqual(args, (PROJECT.replace("-", ""),))
        self.rollback()

    def test_cleanup_does_not_depend_on_missing_or_drifted_port(self):
        record = self.journal.read()
        for action, state in (("shelve", "ACTIVE"), ("offload", "SHELVED")):
            with (
                self.subTest(action=action),
                patch.object(
                    ops, "cloud_query", return_value={"id": VM_IDS[0], "project_id": PROJECT, "status": state}
                ),
                patch.object(self.provider, "port", side_effect=ops.invalid_path()) as port,
                patch.object(self.provider, "call") as call,
            ):
                self.provider.lifecycle(record, VM_IDS[0], action)
                port.assert_not_called()
                call.assert_called_once_with(action, VM_IDS[0], mutation=True)

    def test_setup_restricts_selected_ports_and_rollback_preserves_shared_group(self):
        shared = copy.deepcopy(self.provider.groups[self.provider.original])
        self.setup_network()
        for role, interface in access()["interfaces"].items():
            group_id = self.provider.ports[interface["port_id"]]["security_group_ids"][0]
            rules = self.provider.groups[group_id]["rules"]
            self.assertEqual(len(rules), 7 if role == "manager" else 6)
            for rule in rules:
                self.assertTrue(rule["remote_ip_prefix"].endswith("/32"))
                if rule["remote_ip_prefix"] == access()["operator_cidr"]:
                    self.assertEqual((role, rule["protocol"], rule["port_range_min"]), ("manager", "tcp", 22))
        self.rollback()
        self.assertEqual(self.provider.groups, {self.provider.original: shared})
        self.assertTrue(
            all(
                port["security_group_ids"] == [self.provider.original]
                for port in self.provider.ports.values()
            )
        )

    def test_rules_changed_after_setup_are_rejected_before_activation(self):
        self.setup_network()
        record = self.journal.read()
        resource = record["network"]["seen_groups"]["manager"]
        self.provider.verify_ingress(record, "manager", self.provider.groups[resource])
        self.provider.groups[resource]["rules"].append(
            {
                "direction": "ingress",
                "protocol": "tcp",
                "remote_ip_prefix": "0.0.0.0/0",
                "port_range_min": 80,
                "port_range_max": 80,
                "ethertype": "IPv4",
            }
        )
        with self.assertRaises(ops.OpsError):
            self.provider.verify_ingress(record, "manager", self.provider.groups[resource])

    def test_compact_project_uuid_is_verified_by_identity(self):
        for port in self.provider.ports.values():
            port["project_id"] = PROJECT.replace("-", "")
        self.setup_network()
        self.rollback()

    def test_routes_are_checked_before_any_mutation(self):
        self.provider.forbid_route = True
        with self.assertRaises(ops.OpsError):
            self.provider.network_step(self.journal, rollback=False)
        self.assertFalse(any(mutation for _, _, mutation in self.provider.calls))

    def test_wrong_selected_port_or_additional_interface_never_mutates(self):
        self.provider.ports[PORT_IDS[0]]["device_id"] = VM_IDS[1]
        with self.assertRaises(ops.OpsError):
            for _ in range(3):
                self.provider.network_step(self.journal, rollback=False)
        self.assertFalse(any(mutation for _, _, mutation in self.provider.calls))

    def test_lost_creation_response_is_discovered_and_never_duplicated(self):
        self.provider.lose_response = "group_create"
        with self.assertRaises(ops.OpsError):
            for _ in range(4):
                self.provider.network_step(self.journal, rollback=False)
        self.provider.lose_response = None
        self.setup_network()
        self.assertEqual(sum(action == "group_create" for action, _, _ in self.provider.calls), 3)
        self.rollback()

    def test_unknown_creation_result_without_resource_is_not_replayed(self):
        self.provider.fail_action = "group_create"
        with self.assertRaises(ops.OpsError):
            for _ in range(4):
                self.provider.network_step(self.journal, rollback=False)
        self.provider.fail_action = None
        with self.assertRaises(ops.OpsError):
            self.provider.network_step(self.journal, rollback=False)
        self.assertEqual(sum(action == "group_create" for action, _, _ in self.provider.calls), 1)

    def test_external_attachment_change_blocks_rollback(self):
        self.setup_network()
        self.provider.ports[PORT_IDS[0]]["security_group_ids"].append(self.provider.original)
        with self.assertRaises(ops.OpsError):
            self.rollback()
        self.assertFalse(any(action == "group_delete" for action, _, _ in self.provider.calls))

    def test_unrelated_vm_using_owned_group_prevents_group_deletion(self):
        self.setup_network()
        group = self.provider.ports[PORT_IDS[0]]["security_group_ids"][0]
        unrelated = copy.deepcopy(self.provider.ports[PORT_IDS[0]])
        unrelated["device_id"] = PROJECT
        self.provider.ports[PROJECT] = unrelated
        with self.assertRaises(ops.OpsError):
            self.rollback()
        self.assertIn(group, self.provider.groups)
        self.assertEqual(self.provider.ports[PROJECT], unrelated)

    def configure_floating(self):
        facts = access()
        facts["route"] = {
            "mode": "floating",
            "external_network_id": self.provider.external,
            "router_id": self.provider.router,
            "operator_route_verified": True,
        }
        config = self.root / "floating-config"
        config.mkdir(mode=0o700)
        profile = config / "profile.json"
        profile.write_text("{}")
        self.journal = register(profile, self.root / "floating-state", spec(), facts)
        self.journal.change(
            lambda record: record.update(desired="run", window={"seconds": 1800, "inspection": True})
        )

    def test_only_one_manager_floating_entry_is_created_and_removed(self):
        self.configure_floating()
        self.setup_network()
        self.assertEqual(len(self.provider.fips), 1)
        value = next(iter(self.provider.fips.values()))
        self.assertEqual(value["port_id"], PORT_IDS[0])
        self.rollback()
        self.assertEqual(self.provider.fips, {})
        self.assertEqual(sum(action == "floating_create" for action, _, _ in self.provider.calls), 1)

    def test_existing_manager_floating_entry_is_reused_and_never_deleted(self):
        self.configure_floating()
        resource = self.provider.new_id()
        original = {
            "id": resource,
            "project_id": PROJECT,
            "description": "operator-owned",
            "port_id": PORT_IDS[0],
            "fixed_ip_address": "10.0.0.10",
            "floating_network_id": self.provider.external,
        }
        self.provider.fips[resource] = copy.deepcopy(original)
        self.setup_network()
        self.rollback()
        self.assertEqual(self.provider.fips, {resource: original})
        self.assertFalse(
            any(action.startswith("floating_") and mutation for action, _, mutation in self.provider.calls)
        )

    def test_lost_floating_response_reconciles_by_recorded_marker(self):
        self.configure_floating()
        self.provider.lose_response = "floating_create"
        with self.assertRaises(ops.OpsError):
            self.setup_network()
        self.provider.lose_response = None
        self.setup_network()
        self.assertEqual(sum(action == "floating_create" for action, _, _ in self.provider.calls), 1)
        self.rollback()

    def test_existing_worker_public_entry_refuses_setup_before_mutation(self):
        self.configure_floating()
        resource = self.provider.new_id()
        self.provider.fips[resource] = {
            "id": resource,
            "project_id": PROJECT,
            "description": "existing",
            "port_id": PORT_IDS[1],
        }
        with self.assertRaises(ops.OpsError):
            self.provider.network_step(self.journal, rollback=False)
        self.assertFalse(any(mutation for _, _, mutation in self.provider.calls))

    def test_access_facts_reject_public_peer_broad_operator_and_unknown_fields(self):
        path = self.root / "access.json"
        import json

        for change in (
            lambda value: value.update(operator_cidr="0.0.0.0/0"),
            lambda value: value["interfaces"]["worker"].update(fixed_ip="8.8.8.8"),
            lambda value: value.update(unknown=True),
        ):
            value = access()
            change(value)
            path.write_text(json.dumps(value))
            with self.assertRaises(ops.OpsError):
                validate_access(str(path))

    def test_adapter_validates_all_argv_before_starting_subprocess(self):
        provider = Provider({})
        for action, args in (
            ("delete", [VM_IDS[0]]),
            ("shelve", ["--all"]),
            ("attach", [PORT_IDS[0], "bad"]),
            ("rule", [PROJECT, "tcp", "0.0.0.0/0", "22", "bad"]),
            ("group_create", ["unrelated"]),
        ):
            with self.subTest(action=action), self.assertRaises(ops.OpsError):
                provider.call(action, *args, mutation=True)


if __name__ == "__main__":
    unittest.main()
