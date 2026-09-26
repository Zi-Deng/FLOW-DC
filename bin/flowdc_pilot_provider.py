"""Fixed OpenStack lifecycle/network adapter, separate from the read-only wrapper.

Every public step has a single 20-second deadline shared by its verification and
mutation subprocesses. Only the supervisor constructs this adapter for mutations.
"""

import ipaddress
import os
import stat
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
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


# The only SDK operation is shelve_offload_server. Keep the program in this
# immutable module so the installed release still consists of six modules.
OFFLOAD_PROGRAM = r"""
import contextlib
import json
import logging
import os
import sys
import time
from pathlib import Path
from urllib.parse import urlsplit
from uuid import UUID

def main():
    dispatched = False
    try:
        mode, prefix, project, region, auth_url, vm_id, deadline = sys.argv[1:]
        if (mode not in ("check", "offload") or sys.version_info < (3, 12)
                or sys.prefix != prefix or sys.base_prefix == sys.prefix
                or sys.executable != str(Path(prefix) / "bin/python")
                or not sys.flags.isolated or not sys.flags.no_user_site):
            return "offload_runtime_unsupported", "prerequisite", False
        import requests
        from keystoneauth1.identity.v3 import ApplicationCredential
        from keystoneauth1.session import Session
        from openstack.config.cloud_region import CloudRegion
        from openstack.connection import Connection
        from openstack.compute.v2._proxy import Proxy
        if not callable(getattr(Proxy, "shelve_offload_server", None)):
            return "offload_runtime_unsupported", "prerequisite", False
        if mode == "check":
            return "offload_runtime_ready", "ok", False
        parsed = urlsplit(auth_url)
        if (parsed.scheme != "https" or not parsed.hostname or parsed.username
                or parsed.password or parsed.query or parsed.fragment
                or os.environ.get("OS_AUTH_URL", "").rstrip("/") != auth_url.rstrip("/")
                or os.environ.get("OS_REGION_NAME") != region
                or os.environ.get("OS_AUTH_TYPE") != "v3applicationcredential"):
            return "cloud_context_mismatch", "identity", False
        UUID(project)
        UUID(vm_id)
        remaining = float(deadline) - time.monotonic()
        if not 0 < remaining <= 20:
            return "probe_timeout", "timeout", False
        auth = ApplicationCredential(
            auth_url=auth_url,
            application_credential_id=os.environ["OS_APPLICATION_CREDENTIAL_ID"],
            application_credential_secret=os.environ["OS_APPLICATION_CREDENTIAL_SECRET"],
        )
        # Prevent reauthentication replay, HTTP/connection retries, redirects,
        # proxy/netrc/environment overrides and logging of request bodies.
        class SingleRequestSession(Session):
            def request(self, *args, **kwargs):
                self.timeout = float(deadline) - time.monotonic()
                if self.timeout <= 0:
                    raise TimeoutError
                kwargs.update(allow_reauth=False, connect_retries=0,
                              status_code_retries=0, redirect=False, log=False)
                return super().request(*args, **kwargs)
        transport = requests.Session()
        transport.trust_env = False
        session = SingleRequestSession(auth=auth, session=transport, verify=True,
                                       timeout=remaining, connect_retries=0, redirect=False)
        # Direct CloudRegion construction never invokes a clouds.yaml, vendor
        # profile, environment or auth-cache loader. Connection constructs the
        # network/image mixins even for compute; supply their required keys
        # explicitly, with those unused facilities disabled.
        config = CloudRegion(name="flowdc-offload", session=session, auth_plugin=auth,
            config={"region_name": region, "interface": "public", "verify": True,
                    "compute_api_version": "2.1", "connect_retries": 0,
                    "status_code_retries": 0, "secgroup_source": None,
                    "image_api_use_tasks": False}, cache_auth=False)
        if str(UUID(session.get_project_id())) != project:
            return "cloud_context_mismatch", "identity", False
        connection = Connection(config=config)
        compute = connection.compute
        server = compute.get_server(vm_id)
        if str(UUID(server.id)) != vm_id or str(UUID(server.project_id)) != project:
            return "server_identity_mismatch", "identity", False
        if server.status != "SHELVED":
            return "lifecycle_state_pending", "state", False
        if time.monotonic() >= float(deadline):
            return "probe_timeout", "timeout", False
        dispatched = True
        compute.shelve_offload_server(vm_id)
        return "offload_acknowledged", "ok", True
    except (ImportError, AttributeError):
        return "offload_runtime_unsupported", "prerequisite", dispatched
    except TimeoutError:
        return "probe_timeout", "timeout", dispatched
    except Exception as exc:
        status = getattr(exc, "status_code", None) or getattr(exc, "http_status", None)
        category = "unknown"
        if type(status) is int:
            if status in (401, 403):
                category = "permission"
            elif status == 409:
                category = "conflict"
            elif status in (429, 500, 502, 503, 504):
                category = "transient"
        code = "provider_permission_pending" if category == "permission" else "provider_request_failed"
        return code, category, dispatched

logging.disable(logging.CRITICAL)
with open(os.devnull, "w") as sink, contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
    code, category, dispatched = main()
print(json.dumps({"code": code, "category": category, "dispatch_possible": dispatched}))
"""

# Reuse the exact OpenRC allowlist, suppress its output, and pass only nonsecret
# bindings/program text in argv. Credentials remain in the child's environment.
OFFLOAD_WRAPPER = (
    r"""set +x +v
set -eu -o pipefail
readonly flowdc_credential=$1 flowdc_python=$2 flowdc_program=$3
shift 3
"""
    + ops.WRAPPER.split("readonly -a command\n", 1)[1].split('exec "$flowdc_client"', 1)[0]
    + r"""
exec "$flowdc_python" -I -B -c "$flowdc_program" "$@"
"""
)


def offload_runtime(client):
    """Validate a supported venv, retaining the lexical symlink interpreter path."""
    try:
        ops.validate_client(client)
        path = ops.absolute_path(client)
        if path.name != "openstack" or path.parent.name != "bin":
            raise ValueError
        prefix = path.parent.parent
        python = path.parent / "python"
        # Match the ancestor walk's root-owner treatment in a user namespace.
        owners = (0, os.stat("/").st_uid, os.geteuid())

        def read_runtime_file(value):
            with ops.private_directory(value.parent, private=False) as parent:
                fd = os.open(value.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
                try:
                    info = os.fstat(fd)
                    if (
                        not stat.S_ISREG(info.st_mode)
                        or info.st_uid not in owners
                        or info.st_mode & 0o022
                        or info.st_size > ops.MAX_BYTES
                    ):
                        raise ValueError
                    return ops.read_bounded_file(fd).decode("utf-8")
                finally:
                    os.close(fd)

        if read_runtime_file(path).splitlines()[0] != "#!" + str(python):
            raise ValueError
        config = {}
        for line in read_runtime_file(prefix / "pyvenv.cfg").splitlines():
            key, separator, value = line.partition("=")
            if separator:
                if key.strip() in config:
                    raise ValueError
                config[key.strip()] = value.strip()
        if config.get("include-system-site-packages") != "false":
            raise ValueError
        version = tuple(int(part) for part in config["version"].split(".")[:2])
        if len(version) != 2 or version < (3, 12):
            raise ValueError
        with ops.private_directory(
            prefix / "lib" / f"python{version[0]}.{version[1]}" / "site-packages", private=False
        ):
            pass
        home = ops.absolute_path(config["home"])
        with ops.private_directory(home, private=False):
            pass
        current = python
        for _ in range(8):
            with ops.private_directory(current.parent, private=False) as parent:
                info = os.stat(current.name, dir_fd=parent, follow_symlinks=False)
                if info.st_uid not in owners:
                    raise ValueError
                if not stat.S_ISLNK(info.st_mode):
                    if (
                        not stat.S_ISREG(info.st_mode)
                        or info.st_mode & 0o022
                        or not info.st_mode & 0o111
                        or not os.access(current, os.X_OK)
                        or current.parent != home
                    ):
                        raise ValueError
                    return str(python), str(prefix)
                target = Path(os.readlink(current.name, dir_fd=parent))
                current = ops.absolute_path(str(target if target.is_absolute() else current.parent / target))
        raise ValueError
    except (ops.OpsError, OSError, ValueError, KeyError, IndexError, UnicodeError):
        raise failure("offload_runtime_unsupported") from None


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
        self.started = None
        self.phase = self.role = "unknown"

    @contextmanager
    def step(self, phase="unknown", role="unknown"):
        self.started = time.monotonic()
        self.deadline = self.started + ops.CLOUD_SECONDS
        self.phase, self.role = phase, role
        self.verified_record = None
        try:
            yield
        except (KeyError, TypeError, ValueError, AttributeError):
            error = failure("provider_schema")
            self.diagnose(error)
            raise error from None
        except (ops.OpsError, OSError) as exc:
            if not hasattr(exc, "diagnostic"):
                self.diagnose(exc)
            raise
        finally:
            self.verified_record = None
            self.started = None
            self.phase = self.role = "unknown"

    def diagnose(self, error, action="unknown", args=(), dispatch_possible=None):
        role = self.role
        record = self.verified_record
        if record and args and args[0] is not None:
            for vm_id, vm in record["vms"].items():
                selected = record["access"]["interfaces"][vm["role"]]["port_id"]
                group = record["network"]["seen_groups"].get(vm["role"])
                if args[0] in (vm_id, selected, group):
                    role = vm["role"]
                    break
        code = getattr(error, "code", "")
        category = getattr(error, "provider_category", None)
        if category is None:
            category = {
                "probe_timeout": "timeout",
                "probe_start_failed": "local",
                "probe_child_management": "local",
                "probe_cleanup_timeout": "local",
                "probe_output_limit": "output_limit",
                "offload_runtime_unsupported": "prerequisite",
                "provider_permission_pending": "permission",
                "network_quota_pending": "quota",
                "cloud_context_mismatch": "identity",
                "context_binding_mismatch": "identity",
                "server_identity_mismatch": "identity",
                "lifecycle_state_pending": "state",
            }.get(code, "local" if isinstance(error, OSError) else "unknown")
        now = time.monotonic()
        error.diagnostic = ops.safe_diagnostic(
            {
                "phase": self.phase,
                "role": role,
                "action": action,
                "category": category,
                "dispatch_possible": getattr(error, "dispatch_possible", dispatch_possible),
                "elapsed_seconds": now - self.started if self.started is not None else None,
                "remaining_seconds": self.deadline - now if self.started is not None else None,
            }
        )

    def query(self, action, resource=None):
        dispatched = False

        def sent():
            nonlocal dispatched
            dispatched = True

        try:
            return ops.cloud_query(self.profile, action, resource, deadline=self.deadline, on_dispatch=sent)
        except (ops.OpsError, OSError) as exc:
            self.diagnose(exc, action, (resource,), dispatched)
            raise

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
        dispatched = False

        def sent():
            nonlocal dispatched
            dispatched = True
            if on_dispatch is not None:
                on_dispatch()

        try:
            return self._call(action, *args, mutation=mutation, on_dispatch=sent)
        except (ops.OpsError, OSError) as exc:
            self.diagnose(exc, action, args, dispatched)
            raise

    def _call(self, action, *args, mutation=False, on_dispatch=None):
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
        if action == "offload":
            return self.offload(args[0], on_dispatch=on_dispatch)
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
            error = failure(codes.get(raw, "provider_request_failed"))
            error.provider_category = {
                b"permission": "permission",
                b"quota": "quota",
                b"conflict": "conflict",
                b"transient": "transient",
            }.get(raw, "unknown")
            raise error
        return None if mutation else ops.parse_json(raw)

    def sdk_result(self, code, raw, *, checking=False):
        if code:
            raise failure("offload_runtime_unsupported" if checking else "provider_request_failed")
        try:
            result = ops.fields(ops.parse_json(raw), ("code", "category", "dispatch_possible"))
            combinations = {
                "offload_runtime_unsupported": ({"prerequisite"}, {False, True}),
                "cloud_context_mismatch": ({"identity"}, {False}),
                "server_identity_mismatch": ({"identity"}, {False}),
                "lifecycle_state_pending": ({"state"}, {False}),
                "probe_timeout": ({"timeout"}, {False, True}),
                "provider_permission_pending": ({"permission"}, {False, True}),
                "provider_request_failed": ({"conflict", "transient", "unknown"}, {False, True}),
            }
            expected = "offload_runtime_ready" if checking else "offload_acknowledged"
            combinations[expected] = ({"ok"}, {not checking})
            categories, dispatched = combinations[result["code"]]
            if (
                result["category"] not in categories
                or type(result["dispatch_possible"]) is not bool
                or result["dispatch_possible"] not in dispatched
            ):
                raise ValueError
        except (ops.OpsError, KeyError, TypeError, ValueError):
            raise failure("offload_runtime_unsupported" if checking else "provider_schema") from None
        expected = "offload_runtime_ready" if checking else "offload_acknowledged"
        if result["code"] != expected:
            error = failure(result["code"])
            error.provider_category = result["category"]
            error.dispatch_possible = result["dispatch_possible"]
            raise error

    def runtime_check(self):
        """Offline prerequisite check: never open the credential or a cloud session."""
        with self.step("runtime_check"):
            python, prefix = offload_runtime(self.profile["openstack_client"])
            code, raw = ops.run_bounded(
                [python, "-I", "-B", "-c", OFFLOAD_PROGRAM, "check", prefix, "", "", "", "", "0"],
                timeout=self.deadline - time.monotonic(),
            )
            self.sdk_result(code, raw, checking=True)

    def offload(self, vm_id, *, on_dispatch=None):
        if self.verified_record is None or vm_id not in self.verified_record["vms"]:
            raise failure("verified_adapter_scope_required")
        python, prefix = offload_runtime(self.profile["openstack_client"])
        expected = self.verified_record["spec"]["context"]
        with ops.private_file(self.profile["credential_file"]) as credential:
            code, raw = ops.run_bounded(
                [
                    "/bin/bash",
                    "-p",
                    "-c",
                    OFFLOAD_WRAPPER,
                    "flowdc-offload",
                    f"/proc/self/fd/{credential}",
                    python,
                    OFFLOAD_PROGRAM,
                    "offload",
                    prefix,
                    expected["project_id"],
                    expected["region"],
                    expected["auth_url"],
                    vm_id,
                    str(self.deadline),
                ],
                timeout=self.deadline - time.monotonic(),
                pass_fds=(credential,),
                on_dispatch=on_dispatch,
            )
        # Even a successful SDK return is only acknowledgement. The supervisor
        # retains its durable intent/obligation until a later fresh observation.
        self.sdk_result(code, raw)

    def read_batch(self, requests):
        """Join at most four read-only probes under the current step deadline.

        Workers only return facts. Validation and every mutation stay on the
        actor thread. Cancellation only stops probes that have not started. The
        executor context joins all running probes, whose runners own and reap
        their children under the shared deadline; later batches are not submitted.
        """
        allowed = {"group", "ports", "port", "network", "subnet", "floating_show"}
        for action, *args in requests:
            if action not in allowed:
                raise failure("invalid_adapter_action", invalid=True)
            self.validate_call(action, args)
        results = []
        with ThreadPoolExecutor(max_workers=4) as executor:
            for offset in range(0, len(requests), 4):
                if time.monotonic() >= self.deadline:
                    raise failure("probe_timeout")
                futures = [executor.submit(self.call, *request) for request in requests[offset : offset + 4]]
                try:
                    results.extend(future.result() for future in futures)
                finally:
                    for future in futures:
                        future.cancel()
        if time.monotonic() >= self.deadline:
            raise failure("probe_timeout")
        return results

    def context(self, record):
        expected = record["spec"]["context"]
        if (
            self.profile["expected_project_id"] != expected["project_id"]
            or self.profile["region"] != expected["region"]
            or self.profile["auth_url"] != expected["auth_url"]
            or not set(record["vms"]).issubset(self.profile["intended_server_ids"])
        ):
            raise failure("context_binding_mismatch")
        context = self.query("context")
        urls = [context[key] for key in ("auth_url", "auth.auth_url") if context.get(key)]
        project = self.query("project")
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
        value = self.query("server", vm_id)
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
        return self.validate_port(record, role, self.call("port", interface["port_id"]))

    def validate_port(self, record, role, value):
        interface = record["access"]["interfaces"][role]
        vm_id = next(key for key, vm in record["vms"].items() if vm["role"] == role)
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
        with self.step("preflight"):
            self.context(record)
            for vm_id in record["vms"]:
                if self.server(record, vm_id) != "SHELVED_OFFLOADED":
                    raise failure("initial_offload_required")

    def verify_idle(self, record):
        """Fresh, read-only offload and rollback proof for idle maintenance."""
        self.preflight(record)
        with self.step("idle_verification"):
            self.context(record)
            if self.owned_groups(record):
                raise failure("maintenance_network_rollback_required")
            marker = "flowdc-" + record["network"]["generation"] + "-entry"
            if any(value.get("description") == marker for value in self.floating(record)):
                raise failure("maintenance_network_rollback_required")
        for role in ("manager", "worker", "origin"):
            with self.step("idle_verification", role):
                self.context(record)
                port = self.topology(record, role)
                original = record["network"]["original"].get(role)
                if original is not None and sorted(port["security_group_ids"]) != sorted(original):
                    raise failure("maintenance_network_rollback_required")

    def observe(self, record, vm_id):
        role = record["vms"].get(vm_id, {}).get("role", "unknown")
        with self.step("cleanup" if record["desired"] == "stop" else "observation", role):
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
        role = record["vms"].get(vm_id, {}).get("role", "unknown")
        with self.step("activation" if action == "unshelve" else "cleanup", role):
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
                error = failure("lifecycle_state_pending")
                self.diagnose(error, action, (vm_id,), False)
                raise error
            self.call(action, vm_id, mutation=True)

    def topology(self, record, role):
        """Inspect all selected attachments/routes before changing this interface."""
        interface = record["access"]["interfaces"][role]
        vm_id = next(key for key, vm in record["vms"].items() if vm["role"] == role)
        ports, port, network, subnet = self.read_batch(
            [
                ("ports", vm_id),
                ("port", interface["port_id"]),
                ("network", interface["network_id"]),
                ("subnet", interface["subnet_id"]),
            ]
        )
        if ids(ports) != [interface["port_id"]]:
            raise failure("additional_interfaces_require_manual_checkpoint")
        port = self.validate_port(record, role, port)
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
        # Inspect existing rules; originals are never edited.
        self.read_batch([("group", group) for group in port["security_group_ids"]])
        return port

    def owned_groups(self, record):
        project = record["spec"]["context"]["project_id"]
        prefix = "flowdc-" + record["network"]["generation"] + "-"
        rows = self.call("groups", UUID(project).hex)
        ids(rows)
        result = {}
        selected = []
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
            selected.append(row)
        values = self.read_batch([("group", row["ID"]) for row in selected])
        for row, value in zip(selected, values, strict=True):
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
        resources = ids(self.call("floating", UUID(project).hex))
        values = self.read_batch([("floating_show", resource) for resource in resources])
        for resource, value in zip(resources, values, strict=True):
            if value.get("id") != resource or ops.uuid_value(field(value, "project_id")) != project:
                raise failure("floating_identity_mismatch")
        return values

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
        with self.step("network_rollback" if rollback else "network_setup"):
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
        # Read every selected port under this step's shared deadline. Join and
        # validate the complete batch on the actor before deciding on a mutation;
        # already restored roles must still be freshly verified on every step.
        roles = list(record["network"]["original"])
        values = self.read_batch(
            [("port", record["access"]["interfaces"][role]["port_id"]) for role in roles]
        )
        ports = {
            role: self.validate_port(record, role, value) for role, value in zip(roles, values, strict=True)
        }
        for role, original in record["network"]["original"].items():
            port = ports[role]
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
