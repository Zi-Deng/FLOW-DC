"""Fixed OpenStack lifecycle/network adapter, separate from the read-only wrapper.

Every public step has a single 20-second deadline shared by its verification and
mutation subprocesses. Only the supervisor constructs this adapter for mutations.
"""

import ipaddress
import time
from contextlib import contextmanager
from uuid import UUID

import flowdc_ops as ops
from flowdc_pilot_journal import failure

# No arbitrary command is accepted. Arguments come from validated identities and
# fixed rules below. OpenRC remains trusted code; no credential output is retained.
LIFECYCLE_WRAPPER = r"""#!/bin/bash -p
set +x +v
set -eu -o pipefail
[[ $# -ge 3 ]] || exit 64
readonly flowdc_credential=$1 flowdc_client=$2 flowdc_action=$3
shift 3
case "$flowdc_action" in
  unshelve) [[ $# == 1 ]] || exit 64; command=(server unshelve "$1");;
  shelve) [[ $# == 1 ]] || exit 64; command=(server shelve "$1");;
  offload) [[ $# == 1 ]] || exit 64; command=(server shelve --offload "$1");;
  port) command=(port show "$1" -f json);;
  ports) command=(port list --server "$1" -f json -c ID);;
  network) command=(network show "$1" -f json);;
  subnet) command=(subnet show "$1" -f json);;
  router) command=(router show "$1" -f json);;
  router_ports) command=(port list --device-id "$1" -f json -c ID);;
  groups) command=(security group list --project "$1" -f json -c ID -c Name);;
  group_ports) command=(port list --security-group "$1" -f json -c ID);;
  group) command=(security group show "$1" -f json);;
  group_create) command=(security group create --description "$1" "$1" -f json);;
  group_delete) command=(security group delete "$1");;
  rule) command=(security group rule create --ingress --ethertype IPv4 --protocol "$2" --remote-ip "$3" --description "$5" "$1" -f json)
    [[ "$4" == any ]] || command+=(--dst-port "$4");;
  attach) flowdc_port=$1; shift; command=(port set --no-security-group)
    for flowdc_group in "$@"; do command+=(--security-group "$flowdc_group"); done
    command+=("$flowdc_port");;
  floating) command=(floating ip list --project "$1" --long -f json -c ID);;
  floating_show) command=(floating ip show "$1" -f json);;
  floating_create) command=(floating ip create --description "$1" --port "$2" --fixed-ip-address "$3" "$4" -f json);;
  floating_delete) command=(floating ip delete "$1");;
  *) exit 64;;
esac
readonly -a command
""" + ops.WRAPPER.split("readonly -a command\n", 1)[1]


def validate_access(path):
    return validate_access_value(ops.read_document(path))


def validate_access_value(value):
    value = ops.fields(value, ("schema_version", "operator_cidr", "route", "interfaces"))
    ops.version(value)
    try:
        operator = ipaddress.ip_network(value["operator_cidr"], strict=True)
        if operator.version != 4 or operator.prefixlen != 32:
            raise ValueError
        route = ops.fields(
            value["route"], ("mode", "external_network_id", "router_id", "operator_route_verified")
        )
        if route["operator_route_verified"] is not True or route["mode"] not in ("private", "floating"):
            raise ValueError
        if route["mode"] == "floating":
            route["external_network_id"] = ops.uuid_value(route["external_network_id"])
            route["router_id"] = ops.uuid_value(route["router_id"])
        elif route["external_network_id"] is not None or route["router_id"] is not None:
            raise ValueError
        ops.fields(value["interfaces"], ("manager", "worker", "origin"))
        ports, addresses, networks, subnets = set(), set(), set(), set()
        for interface in value["interfaces"].values():
            ops.fields(interface, ("port_id", "network_id", "subnet_id", "fixed_ip"))
            for key in ("port_id", "network_id", "subnet_id"):
                interface[key] = ops.uuid_value(interface[key])
            address = ipaddress.ip_address(interface["fixed_ip"])
            if (
                address.version != 4
                or not address.is_private
                or address.is_unspecified
                or address.is_loopback
            ):
                raise ValueError
            ports.add(interface["port_id"])
            addresses.add(interface["fixed_ip"])
            networks.add(interface["network_id"])
            subnets.add(interface["subnet_id"])
        if len(ports) != 3 or len(addresses) != 3 or len(networks) != 1 or len(subnets) != 1:
            raise ValueError
    except (TypeError, ValueError):
        raise failure("access_facts_required", invalid=True) from None
    return value


def field(value, key):
    if not isinstance(value, dict) or key not in value:
        raise failure("provider_schema")
    return value[key]


def ids(rows):
    if not isinstance(rows, list) or len(rows) > 128:
        raise failure("provider_collection_limit")
    result = [ops.uuid_value(field(row, "ID")) for row in rows]
    if len(set(result)) != len(result):
        raise failure("provider_duplicate_identity")
    return result


class Provider:
    def __init__(self, profile, *, before_activation=None):
        self.profile = profile
        self.before_activation = before_activation
        self.activating = False
        self.verified_record = None
        self.deadline = 0

    @contextmanager
    def step(self):
        self.deadline = time.monotonic() + ops.CLOUD_SECONDS
        self.verified_record = None
        try:
            yield
        except (KeyError, TypeError, ValueError, AttributeError):
            raise failure("provider_schema") from None
        finally:
            self.verified_record = None

    def validate_call(self, action, args):
        single_ids = {
            "unshelve",
            "shelve",
            "offload",
            "port",
            "ports",
            "network",
            "subnet",
            "router",
            "router_ports",
            "groups",
            "group",
            "group_ports",
            "group_delete",
            "floating",
            "floating_show",
            "floating_delete",
        }

        def marker(value):
            import re

            if not re.fullmatch(r"flowdc-[0-9a-f-]{36}-(manager|worker|origin|entry)", value):
                raise failure("invalid_adapter_argument", invalid=True)

        if action in single_ids and len(args) == 1:
            ops.uuid_value(args[0])
        elif action == "group_create" and len(args) == 1:
            marker(args[0])
        elif action == "attach" and 1 <= len(args) <= 17:
            for value in args:
                ops.uuid_value(value)
        elif action == "rule" and len(args) == 5:
            ops.uuid_value(args[0])
            network = ipaddress.ip_network(args[2], strict=True)
            if (
                args[1] not in ("tcp", "udp", "icmp")
                or args[3] not in ("22", "any")
                or network.version != 4
                or network.prefixlen != 32
            ):
                raise failure("invalid_adapter_argument", invalid=True)
            marker(args[4])
        elif action == "floating_create" and len(args) == 4:
            marker(args[0])
            ops.uuid_value(args[1])
            if ipaddress.ip_address(args[2]).version != 4:
                raise failure("invalid_adapter_argument", invalid=True)
            ops.uuid_value(args[3])
        else:
            raise failure("invalid_adapter_action", invalid=True)

    def call(self, action, *args, mutation=False, on_dispatch=None):
        self.validate_call(action, args)
        mutating = action in {
            "unshelve",
            "shelve",
            "offload",
            "group_create",
            "group_delete",
            "rule",
            "attach",
            "floating_create",
            "floating_delete",
        }
        if mutating:
            # Caller flags cannot turn a mutation into an unguarded read.
            mutation = True
            record = self.verified_record
            if record is None:
                raise failure("verified_adapter_scope_required")
            network = record["network"]
            if action in ("unshelve", "shelve", "offload") and args[0] not in record["vms"]:
                raise failure("vm_not_allowlisted")
            if action in ("rule", "group_delete") and args[0] not in network["seen_groups"].values():
                raise failure("network_resource_not_owned")
            if action == "attach":
                role = next(
                    (
                        role
                        for role, interface in record["access"]["interfaces"].items()
                        if interface["port_id"] == args[0]
                    ),
                    None,
                )
                if role is None or list(args[1:]) not in (
                    network["original"].get(role),
                    [network["seen_groups"].get(role)],
                ):
                    raise failure("selected_attachment_not_authorized")
            marker = "flowdc-" + network["generation"] + "-"
            if action == "group_create" and args[0] not in [
                marker + role for role in ("manager", "worker", "origin")
            ]:
                raise failure("network_resource_not_owned")
            if action == "floating_create":
                interface = record["access"]["interfaces"]["manager"]
                if list(args) != [
                    marker + "entry",
                    interface["port_id"],
                    interface["fixed_ip"],
                    record["access"]["route"]["external_network_id"],
                ]:
                    raise failure("floating_entry_not_authorized")
            if action == "floating_delete" and args[0] != network.get("floating_id"):
                raise failure("network_resource_not_owned")
        if mutation and (self.activating or action == "unshelve"):
            if self.before_activation is None:
                raise failure("supervisor_guard_required")
            self.before_activation()
        ops.validate_client(self.profile["openstack_client"])
        # Bash receives fixed source, never interpolated profile/provider values.
        # Unlike memfd_create this also works with Python builds lacking that API.
        with ops.private_file(self.profile["credential_file"]) as credential:
            code, raw = ops.run_bounded(
                [
                    "/bin/bash",
                    "-p",
                    "-c",
                    LIFECYCLE_WRAPPER,
                    "flowdc-lifecycle",
                    f"/proc/self/fd/{credential}",
                    self.profile["openstack_client"],
                    action,
                    *args,
                ],
                timeout=min(ops.CLOUD_SECONDS, self.deadline - time.monotonic()),
                pass_fds=(credential,),
                classify_errors=True,
                on_dispatch=on_dispatch,
            )
        if code:
            codes = {b"quota": "network_quota_pending", b"permission": "provider_permission_pending"}
            raise failure(codes.get(raw, "provider_request_failed"))
        return None if mutation else ops.parse_json(raw)

    def context(self, record):
        expected = record["spec"]["context"]
        if (
            self.profile["expected_project_id"] != expected["project_id"]
            or self.profile["region"] != expected["region"]
            or self.profile["auth_url"] != expected["auth_url"]
            or not set(record["vms"]).issubset(self.profile["intended_server_ids"])
        ):
            raise failure("context_binding_mismatch")
        context = ops.cloud_query(self.profile, "context", deadline=self.deadline)
        urls = [context[key] for key in ("auth_url", "auth.auth_url") if context.get(key)]
        project = ops.cloud_query(self.profile, "project", deadline=self.deadline)
        if (
            field(context, "region_name") != expected["region"]
            or not urls
            or any(ops.https_url(url) != expected["auth_url"] for url in urls)
            or ops.uuid_value(field(project, "project_id")) != expected["project_id"]
        ):
            raise failure("cloud_context_mismatch")
        self.verified_record = record

    def server(self, record, vm_id):
        if vm_id not in record["vms"]:
            raise failure("vm_not_allowlisted")
        value = ops.cloud_query(self.profile, "server", vm_id, deadline=self.deadline)
        if (
            ops.uuid_value(field(value, "id")) != vm_id
            or ops.uuid_value(field(value, "project_id")) != record["spec"]["context"]["project_id"]
        ):
            raise failure("server_identity_mismatch")
        state = field(value, "status")
        if state not in ops.STATES:
            raise failure("provider_schema")
        return state

    def port(self, record, role):
        interface = record["access"]["interfaces"][role]
        vm_id = next(key for key, vm in record["vms"].items() if vm["role"] == role)
        value = self.call("port", interface["port_id"])
        if (
            field(value, "id") != interface["port_id"]
            or field(value, "device_id") != vm_id
            or ops.uuid_value(field(value, "project_id")) != record["spec"]["context"]["project_id"]
            or field(value, "network_id") != interface["network_id"]
            or field(value, "port_security_enabled") is not True
            or field(value, "allowed_address_pairs") != []
            or field(value, "fixed_ips")
            != [{"ip_address": interface["fixed_ip"], "subnet_id": interface["subnet_id"]}]
        ):
            raise failure("selected_port_identity_mismatch")
        groups = field(value, "security_group_ids")
        if not isinstance(groups, list) or len(groups) > 16:
            raise failure("port_security_groups_unsupported")
        for group in groups:
            ops.uuid_value(group)
        return value

    def preflight(self, record):
        with self.step():
            self.context(record)
            for vm_id in record["vms"]:
                if self.server(record, vm_id) != "SHELVED_OFFLOADED":
                    raise failure("initial_offload_required")

    def observe(self, record, vm_id):
        with self.step():
            self.context(record)
            return self.server(record, vm_id)

    def verify_ingress(self, record, role, group):
        marker = "flowdc-" + record["network"]["generation"] + "-" + role
        if (
            group.get("id") != record["network"]["seen_groups"].get(role)
            or group.get("description") != marker
            or ops.uuid_value(field(group, "project_id")) != record["spec"]["context"]["project_id"]
        ):
            raise failure("owned_group_identity_changed")
        expected = {("tcp", record["access"]["operator_cidr"], 22, 22)} if role == "manager" else set()
        for peer, interface in record["access"]["interfaces"].items():
            if peer != role:
                expected.update(
                    (protocol, interface["fixed_ip"] + "/32", None, None)
                    for protocol in ("tcp", "udp", "icmp")
                )
        ingress = [rule for rule in field(group, "rules") if rule.get("direction") == "ingress"]
        actual = {
            (
                rule.get("protocol"),
                rule.get("remote_ip_prefix"),
                rule.get("port_range_min"),
                rule.get("port_range_max"),
            )
            for rule in ingress
        }
        if (
            actual != expected
            or len(ingress) != len(expected)
            or any(
                rule.get("ethertype") != "IPv4"
                or rule.get("remote_group_id")
                or rule.get("remote_address_group_id")
                for rule in ingress
            )
        ):
            raise failure("owned_group_rules_changed")

    def lifecycle(self, record, vm_id, action):
        if action not in ("unshelve", "shelve", "offload"):
            raise failure("unsupported_lifecycle_action")
        self.activating = action == "unshelve"
        with self.step():
            self.context(record)
            state = self.server(record, vm_id)
            if action == "unshelve":
                role = record["vms"][vm_id]["role"]
                port = self.port(record, role)
                if port["security_group_ids"] != [record["network"]["seen_groups"].get(role)]:
                    raise failure("activation_network_changed")
                self.verify_ingress(record, role, self.call("group", record["network"]["seen_groups"][role]))
            # Context and server identity remain mandatory for cleanup. Port
            # drift must not prevent shelving this authenticated allowlisted VM;
            # network rollback separately verifies its own ownership/attachments.
            allowed = {
                "unshelve": {"SHELVED_OFFLOADED"},
                "shelve": {"ACTIVE", "SHUTOFF", "ERROR", "PAUSED", "SUSPENDED"},
                "offload": {"SHELVED"},
            }
            if state not in allowed[action]:
                raise failure("lifecycle_state_pending")
            self.call(action, vm_id, mutation=True)

    def topology(self, record, role):
        """Inspect all selected attachments/routes before changing this interface."""
        interface = record["access"]["interfaces"][role]
        vm_id = next(key for key, vm in record["vms"].items() if vm["role"] == role)
        if ids(self.call("ports", vm_id)) != [interface["port_id"]]:
            raise failure("additional_interfaces_require_manual_checkpoint")
        port = self.port(record, role)
        network = self.call("network", interface["network_id"])
        subnet = self.call("subnet", interface["subnet_id"])
        project = record["spec"]["context"]["project_id"]
        if (
            field(network, "id") != interface["network_id"]
            or ops.uuid_value(field(network, "project_id")) != project
            or field(subnet, "id") != interface["subnet_id"]
            or field(subnet, "network_id") != interface["network_id"]
            or ops.uuid_value(field(subnet, "project_id")) != project
        ):
            raise failure("selected_network_identity_mismatch")
        if field(subnet, "host_routes") != []:
            raise failure("nonstandard_routes_require_manual_checkpoint")
        for group in port["security_group_ids"]:
            self.call("group", group)  # Inspect existing rules; originals are never edited.
        return port

    def owned_groups(self, record):
        project = record["spec"]["context"]["project_id"]
        prefix = "flowdc-" + record["network"]["generation"] + "-"
        rows = self.call("groups", UUID(project).hex)
        ids(rows)
        result = {}
        for row in rows:
            known_role = next(
                (
                    role
                    for role, resource in record["network"]["seen_groups"].items()
                    if resource == row["ID"]
                ),
                None,
            )
            if known_role is not None and row.get("Name") != prefix + known_role:
                raise failure("owned_group_identity_changed")
            if row.get("Name") not in [prefix + role for role in ("manager", "worker", "origin")]:
                continue
            value = self.call("group", row["ID"])
            if (
                value.get("description") != row["Name"]
                or ops.uuid_value(field(value, "project_id")) != project
                or value.get("id") != row["ID"]
            ):
                raise failure("network_ownership_ambiguous")
            role = row["Name"][len(prefix) :]
            if role in result:
                raise failure("duplicate_owned_network_resource")
            if "group-" + role not in record["network"]["intents"]:
                raise failure("unrecorded_network_resource")
            previous = record["network"]["seen_groups"].get(role)
            if previous is not None and previous != value["id"]:
                raise failure("owned_group_identity_changed")
            result[role] = value
        return result

    def floating(self, record):
        project = record["spec"]["context"]["project_id"]
        result = []
        for resource in ids(self.call("floating", UUID(project).hex)):
            value = self.call("floating_show", resource)
            if value.get("id") != resource or ops.uuid_value(field(value, "project_id")) != project:
                raise failure("floating_identity_mismatch")
            result.append(value)
        return result

    def network_intent(self, journal, key, action, *args):
        """Never blindly retry create after a lost response, even if list is empty."""
        record = journal.read()
        if key in record["network"]["intents"]:
            raise failure("ambiguous_network_response_reconcile_manually")

        def intent(current):
            if current["desired"] != "run":
                raise failure("stop_requested")
            current["network"]["intents"][key] = {"action": action, "args": list(args)}
            current["network"]["rolled_back"] = False
            journal.event(current, "network_intent", {"key": key})

        self.verified_record = journal.change(intent)
        possibly_dispatched = False

        def dispatched():
            nonlocal possibly_dispatched
            possibly_dispatched = True

        try:
            self.call(action, *args, mutation=True, on_dispatch=dispatched)
        except (ops.OpsError, OSError) as exc:
            code = exc.code if isinstance(exc, ops.OpsError) else "local_provider_io_error"
            try:
                journal.change(
                    lambda current: current["network"]["intents"][key].update(
                        error=code, not_sent=not possibly_dispatched
                    )
                )
            except (ops.OpsError, OSError):
                # Preserve the original cause; the durable intent remains conservative.
                pass
            raise

    def network_step(self, journal, *, rollback):
        record = journal.read()
        self.activating = not rollback
        with self.step():
            self.context(record)
            groups = self.owned_groups(record)

            def seen(current):
                current["network"]["seen_groups"].update(
                    {role: group["id"] for role, group in groups.items()}
                )

            record = journal.change(seen)
            self.verified_record = record
            if rollback:
                return self.rollback_step(journal, record, groups)
            if not record["network"].get("route_checked"):
                self.route_step(journal, record, inspect_only=True)
                return
            # One role/action per iteration, always inspected before mutation.
            for role in ("manager", "worker", "origin"):
                if role in record["network"].get("configured", []):
                    continue
                port = self.topology(record, role)
                if role not in record["network"]["original"]:

                    def save(current, role=role, port=port):
                        current["network"]["original"][role] = sorted(port["security_group_ids"])

                    journal.change(save)
                    return
                marker = "flowdc-" + record["network"]["generation"] + "-" + role
                if role not in groups:
                    self.network_intent(journal, "group-" + role, "group_create", marker)
                    return
                group = groups[role]
                rules = field(group, "rules")
                if not isinstance(rules, list) or len(rules) > 64:
                    raise failure("provider_schema")
                desired = [("tcp", record["access"]["operator_cidr"], "22")] if role == "manager" else []
                for peer, interface in record["access"]["interfaces"].items():
                    if peer != role:
                        desired += [
                            (protocol, interface["fixed_ip"] + "/32", "any")
                            for protocol in ("tcp", "udp", "icmp")
                        ]
                for protocol, remote, ports in desired:
                    matches = [
                        rule
                        for rule in rules
                        if rule.get("direction") == "ingress"
                        and rule.get("protocol") == protocol
                        and rule.get("remote_ip_prefix") == remote
                        and rule.get("ethertype") == "IPv4"
                        and rule.get("port_range_min") == (22 if ports == "22" else None)
                        and rule.get("port_range_max") == (22 if ports == "22" else None)
                    ]
                    if not matches:
                        self.network_intent(
                            journal,
                            f"rule-{role}-{protocol}-{remote}",
                            "rule",
                            group["id"],
                            protocol,
                            remote,
                            ports,
                            marker,
                        )
                        return
                # Reject extra ingress (including remote groups); don't trust a name alone.
                ingress = [rule for rule in rules if rule.get("direction") == "ingress"]
                if len(ingress) != len(desired):
                    raise failure("owned_group_rules_changed")
                current = sorted(port["security_group_ids"])
                if current != [group["id"]]:
                    if current != record["network"]["original"][role]:
                        raise failure("port_attachments_changed")
                    self.network_intent(journal, "attach-" + role, "attach", port["id"], group["id"])
                    return
                journal.change(
                    lambda current, role=role: current["network"].setdefault("configured", []).append(role)
                )
                return
            self.route_step(journal, record)

    def route_step(self, journal, record, *, inspect_only=False):
        route = record["access"]["route"]
        manager = record["access"]["interfaces"]["manager"]
        floating = self.floating(record)
        selected_ports = {v["port_id"] for v in record["access"]["interfaces"].values()}
        attached = [v for v in floating if v.get("port_id") in selected_ports]
        if any(v.get("port_id") != manager["port_id"] for v in attached) or len(attached) > 1:
            raise failure("unexpected_public_entrypoints")
        if route["mode"] == "floating":
            external = self.call("network", route["external_network_id"])
            router = self.call("router", route["router_id"])
            if (
                external.get("id") != route["external_network_id"]
                or external.get("router:external") is not True
                or router.get("id") != route["router_id"]
                or ops.uuid_value(field(router, "project_id")) != record["spec"]["context"]["project_id"]
                or field(router, "external_gateway_info").get("network_id") != route["external_network_id"]
            ):
                raise failure("external_route_unverified")
            connected = False
            for port_id in ids(self.call("router_ports", route["router_id"])):
                port = self.call("port", port_id)
                if (
                    port.get("network_id") == manager["network_id"]
                    and port.get("device_id") == route["router_id"]
                ):
                    connected = connected or any(
                        ip.get("subnet_id") == manager["subnet_id"] for ip in port.get("fixed_ips", [])
                    )
            if not connected:
                raise failure("router_not_connected")
            if not attached:
                if inspect_only:
                    journal.change(lambda current: current["network"].update(route_checked=True))
                    return
                marker = "flowdc-" + record["network"]["generation"] + "-entry"
                self.network_intent(
                    journal,
                    "floating",
                    "floating_create",
                    marker,
                    manager["port_id"],
                    manager["fixed_ip"],
                    route["external_network_id"],
                )
                return
            if (
                attached[0].get("floating_network_id") != route["external_network_id"]
                or attached[0].get("fixed_ip_address") != manager["fixed_ip"]
            ):
                raise failure("existing_entrypoint_mismatch")
        elif attached:
            raise failure("private_route_has_public_entrypoint")

        if inspect_only:
            journal.change(lambda current: current["network"].update(route_checked=True))
            return

        def ready(current):
            if "floating" in current["network"]["intents"]:
                marker = "flowdc-" + current["network"]["generation"] + "-entry"
                if not any(v.get("description") == marker for v in attached):
                    raise failure("floating_creation_identity_unresolved")
                current["network"]["seen_floating"] = True
                current["network"]["floating_id"] = attached[0]["id"]
            if current["desired"] == "run":
                current["network"]["ready"] = True
                current["network"]["rolled_back"] = False

        journal.change(ready)

    def rollback_step(self, journal, record, groups):
        # Delete only a floating IP with our durable creation intent and marker.
        marker = "flowdc-" + record["network"]["generation"] + "-entry"
        floating = self.floating(record)
        if any(
            v["id"] == record["network"].get("floating_id") and v.get("description") != marker
            for v in floating
        ):
            raise failure("owned_floating_identity_changed")
        owned = [v for v in floating if v.get("description") == marker]
        if len(owned) > 1 or (owned and "floating" not in record["network"]["intents"]):
            raise failure("floating_ownership_ambiguous")
        if owned:
            if record["network"].get("floating_id", owned[0]["id"]) != owned[0]["id"]:
                raise failure("owned_floating_identity_changed")
            self.verified_record = journal.change(
                lambda current: current["network"].update(seen_floating=True, floating_id=owned[0]["id"])
            )
            manager = record["access"]["interfaces"]["manager"]
            if (
                owned[0].get("port_id") != manager["port_id"]
                or owned[0].get("fixed_ip_address") != manager["fixed_ip"]
            ):
                raise failure("owned_floating_attachment_changed")
            journal.change(
                lambda current: journal.event(current, "floating_delete_intent", {"id": owned[0]["id"]})
            )
            self.call("floating_delete", owned[0]["id"], mutation=True)
            return
        for role, original in record["network"]["original"].items():
            port = self.port(record, role)
            current = sorted(port["security_group_ids"])
            group = groups.get(role)
            if current != original:
                if group is None or current != [group["id"]]:
                    raise failure("rollback_attachment_conflict")
                journal.change(
                    lambda current, role=role: journal.event(current, "restore_port_intent", {"role": role})
                )
                self.call("attach", port["id"], *original, mutation=True)
                return
            if group:
                if ids(self.call("group_ports", group["id"])):
                    raise failure("owned_group_attached_elsewhere")
                journal.change(
                    lambda current, group=group: journal.event(
                        current, "group_delete_intent", {"id": group["id"]}
                    )
                )
                self.call("group_delete", group["id"], mutation=True)
                return
        # An unobserved create could still arrive late: never declare rollback done.
        if any(
            key.startswith("group-")
            and not record["network"]["intents"][key].get("not_sent")
            and key[6:] not in record["network"].get("seen_groups", [])
            for key in record["network"]["intents"]
        ):
            raise failure("unresolved_network_creation")
        if (
            "floating" in record["network"]["intents"]
            and not record["network"]["intents"]["floating"].get("not_sent")
            and not record["network"].get("seen_floating")
        ):
            raise failure("unresolved_floating_creation")

        def done(current):
            current["network"]["rolled_back"] = True
            current["network"]["ready"] = False
            # Retain prior intent history in events, use a fresh token for next setup.

        journal.change(done)
