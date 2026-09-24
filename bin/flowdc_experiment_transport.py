"""Verified installed-controller and strict SSH boundaries for experiments."""

import ipaddress
import shlex
import shutil
import sys
import time
from pathlib import Path

import flowdc_ops as ops
from flowdc_experiment_data import ROLES, ExperimentError, digest, parse, read_file, require
from flowdc_experiment_process import execute
from flowdc_pilot_cli import (
    require_persistent_session,
    service_unit,
    trusted_bytes,
    verify_release,
    verify_unit_origin,
)
from flowdc_pilot_journal import Journal, allowance
from flowdc_pilot_supervisor import sample_clock


def binding(record):
    return {key: record[key] for key in ("registration_id", "spec", "access", "service")}


class Controller:
    def __init__(self, root, expected):
        self.root, self.expected = Path(root), expected
        self.journal = Journal(root)
        self.record = self.journal.read()
        require(binding(self.record) == expected, "registration_changed")
        service = self.record["service"]
        require(service is not None, "supervisor_not_installed")
        verify_release(service, self.root)
        self.command = [
            service["interpreter"],
            "-E",
            "-s",
            "-B",
            str(Path(service["release"]) / "flowdc_ops.py"),
            "pilot",
        ]

    def call(self, action, seconds=10, window=1800):
        require(action in ("status", "start", "stop", "reconcile"))
        require(binding(self.journal.read()) == self.expected, "registration_changed")
        verify_release(self.record["service"], self.root)
        argv = [*self.command, action, "--state-root", str(self.root)]
        if action == "start":
            argv += ["--window-seconds", str(window)]
        code, raw = execute(argv, seconds=seconds)
        value = parse(raw)
        require(isinstance(value, dict), "controller_response_invalid")
        if value.get("errors"):
            diagnostic = value["errors"][0].get("code", "controller_failed")
            require(
                isinstance(diagnostic, str) and diagnostic.replace("_", "").isalnum(),
                "controller_response_invalid",
            )
            raise ExperimentError(diagnostic)
        require(
            code in (0, 3)
            and value.get("data", {}).get("registration_id") == self.expected["registration_id"],
            "controller_response_invalid",
        )
        return value["data"]

    def preflight(self, window):
        verify_unit_origin(reloaded=True)
        expected = service_unit(self.record["service"], self.root)
        unit = Path.home() / ".config/systemd/user/flowdc-pilot.service"
        require(trusted_bytes(unit) == expected, "installed_unit_changed")
        require_persistent_session()
        value = self.call("status")
        require(idle(value), "controller_not_idle")
        if not clean(value):
            code, raw = execute(
                [sys.executable, str(Path(__file__).resolve()), str(self.root), "verify-idle"], seconds=90
            )
            if code:
                raise ExperimentError(parse(raw).get("error", "fresh_idle_verification_failed"))
            self.call("reconcile")
            deadline = time.monotonic() + 30
            while not clean(value):
                value = self.call("status", seconds=min(10, deadline - time.monotonic()))
                require(value.get("desired") in ("idle", "stop"), "controller_not_idle")
                if clean(value):
                    break
                require(time.monotonic() < deadline, "fresh_idle_verification_timeout")
                time.sleep(0.2)
        for vm in value["vms"]:
            try:
                allowance(vm["account"]).activation_intent(
                    sample_clock(), window_seconds=window, inspection=True
                )
            except ValueError:
                raise ExperimentError("insufficient_allowance") from None
        return value

    def addresses(self, seconds):
        # Provider use is read-only and isolated under the caller's deadline.
        argv = [sys.executable, str(Path(__file__).resolve()), str(self.root)]
        code, raw = execute(argv, seconds=seconds)
        value = parse(raw)
        if code:
            raise ExperimentError(value.get("error", "registered_route_unavailable"))
        require(set(value) == set(ROLES), "registered_route_unavailable")
        for address in value.values():
            require(ipaddress.ip_address(address).version == 4, "registered_route_unavailable")
        return value


def idle(value):
    """Durable idle/accounting gate, independent of expired provider observations."""
    return (
        value.get("supervisor_ready") is True
        and value.get("desired") == "idle"
        and value.get("checkpoint") is None
        and value.get("network_rolled_back") is True
        and len(value.get("vms", [])) == 3
        and {v.get("role") for v in value["vms"]} == set(ROLES)
        and all(
            v.get("phase") == "offloaded"
            and v.get("account", {}).get("obligation") is False
            and v.get("account", {}).get("uncertain") is False
            for v in value["vms"]
        )
    )


def clean(value):
    return (
        value.get("supervisor_ready") is True
        and value.get("desired") == "idle"
        and value.get("checkpoint") is None
        and value.get("network_rolled_back") is True
        and len(value.get("vms", [])) == 3
        and {v.get("role") for v in value["vms"]} == set(ROLES)
        and all(
            v.get("observation_fresh") is True
            and v.get("provider_state") == "SHELVED_OFFLOADED"
            and v.get("phase") == "offloaded"
            and v.get("account", {}).get("obligation") is False
            and v.get("account", {}).get("uncertain") is False
            for v in value["vms"]
        )
    )


def ready(value):
    return (
        value.get("supervisor_ready") is True
        and value.get("desired") == "run"
        and value.get("checkpoint") is None
        and value.get("network_ready") is True
        and len(value.get("vms", [])) == 3
        and all(
            v.get("observation_fresh") is True and v.get("provider_state") == "ACTIVE" for v in value["vms"]
        )
    )


def remaining(value):
    left = []
    for vm in value["vms"]:
        account = allowance(vm["account"])
        require(account.obligation and not account.uncertain, "account_not_active")
        left.append(account.shutdown_at_consumed - account.consumed - 180)
    return min(left)


def ssh_preflight(spec, registered):
    for tool in ("ssh", "ssh-keygen"):
        require(shutil.which(tool) is not None, "local_ssh_required")
    # Validate private metadata without copying or hashing the private key.
    key = ops.absolute_path(spec["identity_file"])
    with ops.private_directory(key.parent, private=False) as parent, ops.open_private_at(parent, key.name):
        pass
    hosts = read_file(spec["known_hosts"], maximum=262144, private=True)
    for vm in registered["spec"]["vms"]:
        code, raw = execute(
            ["ssh-keygen", "-F", "flowdc-" + vm["role"], "-f", spec["known_hosts"]], seconds=5
        )
        require(code == 0 and raw.strip(), "enrolled_host_key_missing")
    return digest(hosts)


def ssh_config(spec, registered, addresses):
    # Reject SSH configuration metacharacters in referenced filenames. These are
    # local references only; private-key bytes never enter staging or public JSON.
    for key in ("identity_file", "known_hosts"):
        require(not any(c in spec[key] for c in '\\\n\r\x00"%'), "unsafe_ssh_reference")
    require({vm["role"] for vm in registered["spec"]["vms"]} == set(ROLES), "registration_changed")
    lines = [
        "Host *",
        "  BatchMode yes",
        "  StrictHostKeyChecking yes",
        "  IdentitiesOnly yes",
        "  PasswordAuthentication no",
        "  KbdInteractiveAuthentication no",
        "  ForwardAgent no",
        "  ClearAllForwardings yes",
        "  PermitLocalCommand no",
        "  GlobalKnownHostsFile /dev/null",
        "  UpdateHostKeys no",
        "  ConnectionAttempts 1",
        "  ConnectTimeout 10",
        "  ServerAliveInterval 5",
        "  ServerAliveCountMax 2",
        "  LogLevel ERROR",
        f'  UserKnownHostsFile "{spec["known_hosts"]}"',
        f'  IdentityFile "{spec["identity_file"]}"',
        f"  User {spec['user']}",
    ]
    for role in ROLES:
        address = str(ipaddress.IPv4Address(addresses[role]))
        lines += [f"Host {role}", f"  HostName {address}", f"  HostKeyAlias flowdc-{role}"]
        if role != "manager":
            lines.append("  ProxyJump manager")
    return ("\n".join(lines) + "\n").encode()


class Transport:
    def __init__(self, store, selected, manifest, addresses):
        self.store, self.selected, self.manifest = store, selected, manifest
        spec = manifest["spec"]
        require(
            ssh_preflight(spec["ssh"], manifest["binding"]) == manifest["known_hosts_sha256"],
            "enrolled_host_keys_changed",
        )
        self.configuration = ssh_config(spec["ssh"], manifest["binding"], addresses)
        store.write(selected, "ssh.conf", self.configuration, replace=True)
        self.path = store.root / "runs" / selected / "ssh.conf"
        self.helper = store.read(selected, "guest.py")
        require(digest(self.helper) == manifest["files"]["guest.py"]["sha256"], "staged_content_changed")

    def call(self, role, action, case="all", *, seconds, data=b"", extra=(), maximum=262144):
        require(role in ROLES and action in ("probe", "deploy", "launch", "status", "stop", "collect"))
        require(self.store.read(self.selected, "ssh.conf") == self.configuration, "ssh_configuration_changed")
        spec = self.manifest["spec"]
        require(
            digest(read_file(spec["ssh"]["known_hosts"], 262144, private=True))
            == self.manifest["known_hosts_sha256"],
            "enrolled_host_keys_changed",
        )
        root = spec["guest"]["root"] if action == "probe" else spec["guest"]["root"] + "/" + self.selected
        remote = [
            spec["guest"]["python"],
            "-c",
            self.helper.decode(),
            action,
            root,
            role,
            case,
            *map(str, extra),
        ]
        code, raw = execute(
            ["ssh", "-F", str(self.path), "--", role, shlex.join(remote)],
            seconds=seconds,
            data=data,
            maximum=maximum,
        )
        require(code == 0, "guest_operation_failed")
        return raw if action == "collect" else parse(raw)


def read_addresses(root):
    from flowdc_pilot_provider import Provider

    record = Journal(root).read()
    interfaces = record["access"]["interfaces"]
    addresses = {role: interfaces[role]["fixed_ip"] for role in ROLES}
    if record["access"]["route"]["mode"] == "floating":
        provider = Provider(ops.load_profile(record["profile_path"]))
        with provider.step():
            provider.context(record)
            rows = provider.floating(record)
        selected = [row for row in rows if row.get("port_id") == interfaces["manager"]["port_id"]]
        require(
            len(selected) == 1 and selected[0].get("fixed_ip_address") == addresses["manager"],
            "registered_route_unavailable",
        )
        addresses["manager"] = str(ipaddress.IPv4Address(selected[0]["floating_ip_address"]))
    return addresses


if __name__ == "__main__":
    from flowdc_experiment_data import encode

    try:
        if len(sys.argv) == 3 and sys.argv[2] == "verify-idle":
            from flowdc_pilot_provider import Provider

            record = Journal(sys.argv[1]).read()
            require(
                record["desired"] == "idle"
                and not record["checkpoint"]
                and all(
                    not allowance(v["account"]).obligation and not allowance(v["account"]).uncertain
                    for v in record["vms"].values()
                ),
                "controller_not_idle",
            )
            Provider(ops.load_profile(record["profile_path"])).verify_idle(record)
            sys.stdout.buffer.write(encode({"idle_verified": True}))
        else:
            sys.stdout.buffer.write(encode(read_addresses(sys.argv[1])))
    except Exception as exc:
        code = (
            exc.code
            if isinstance(exc, ops.OpsError)
            else str(exc)
            if isinstance(exc, ExperimentError)
            else "provider_read_failed"
        )
        sys.stdout.buffer.write(encode({"error": code}))
        sys.exit(1)
