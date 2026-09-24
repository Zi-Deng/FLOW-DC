"""Experiment CLI/storage/transport evidence using temporary state and fake infrastructure."""

import contextlib
import io
import json
import os
import signal
import subprocess
import sys
import tarfile
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bin"))
import flowdc_experiment as cli
import flowdc_experiment_data as data
import flowdc_experiment_transport as transport
import flowdc_ops as ops
from flowdc_experiment_artifacts import bundle, members
from flowdc_experiment_process import execute
from flowdc_pilot_journal import register
from test_flowdc_pilot_lifecycle import access, spec

REPO = Path(__file__).resolve().parents[1]


class ExperimentTests(unittest.TestCase):
    def setUp(self):
        mask = os.umask(0o077)
        self.addCleanup(os.umask, mask)
        temporary = tempfile.TemporaryDirectory(prefix="flowdc-experiment-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        config = self.root / "config"
        config.mkdir(mode=0o700)
        profile = config / "profile.json"
        profile.write_text("{}")
        self.state = self.root / "state"
        self.journal = register(profile, self.state, spec(), access())
        (self.state / "runs").mkdir(mode=0o700, exist_ok=True)
        self.key = config / "identity"
        subprocess.run(
            ["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(self.key)],
            check=True,
            capture_output=True,
            timeout=10,
        )
        public = self.key.with_suffix(".pub").read_text().split()[:2]
        self.hosts = config / "known_hosts"
        self.hosts.write_text("".join(f"flowdc-{vm['role']} {' '.join(public)}\n" for vm in spec()["vms"]))
        cases = []
        for name, paarc in (("paarc-on", True), ("paarc-off", False)):
            path = config / f"{name}.json"
            path.write_text(json.dumps({"enable_paarc": paarc, "C_init": 2, "C_min": 1, "C_max": 4}))
            cases.append({"name": name, "config": str(path)})
        self.value = {
            "schema_version": 1,
            "state_root": str(self.state),
            "registration_id": self.journal.read()["registration_id"],
            "source": {
                "repository": str(REPO),
                "revision": subprocess.check_output(
                    ["git", "-C", str(REPO), "rev-parse", "HEAD"], text=True
                ).strip(),
            },
            "ssh": {"user": "test", "identity_file": str(self.key), "known_hosts": str(self.hosts)},
            "guest": {
                "root": "/home/test/experiments",
                "python": "/home/test/venv/bin/python",
                "worker": "/home/test/venv/bin/vine_worker",
            },
            "fixture": True,
            "cases": cases,
            "bounds": {"stop_seconds": 1},
        }
        self.path = config / "experiment.json"
        self.store = data.Store(self.state)

    def prepare(self):
        self.path.write_bytes(data.encode(self.value))
        result = subprocess.run(
            [sys.executable, str(REPO / "bin/flowdc_experiment.py"), "prepare", "--spec", str(self.path)],
            capture_output=True,
            timeout=15,
        )
        self.assertEqual(result.stderr, b"")
        self.assertEqual(result.returncode, 0, result.stdout)
        selected = json.loads(result.stdout)["data"]["run_id"]
        return selected, *cli.load(self.store, selected)

    def invoke(self, action, selected):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = cli.main([action, "--state-root", str(self.state), "--run-id", selected])
        return code, json.loads(output.getvalue())

    def test_public_prepare_status_unique_private_and_offline(self):
        before = self.journal.read()
        selected, manifest, _ = self.prepare()
        second, _, _ = self.prepare()
        self.assertNotEqual(selected, second)
        self.assertEqual(self.journal.read(), before)
        self.assertEqual(manifest["source"]["commit"], self.value["source"]["revision"])
        self.assertEqual(self.invoke("status", selected)[0], 0)
        self.assertEqual(len(manifest["partitions"]["paarc-on"]), 2)
        for path in (self.state / "runs" / selected).iterdir():
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        raw = self.store.read(selected, "bundle.tar")
        self.assertNotIn(self.key.read_bytes(), raw)
        self.assertNotIn(b"PRIVATE KEY", raw)
        self.assertNotIn(b".git/", raw)

    def test_tampered_staged_content_manifest_and_input_symlink_refused(self):
        selected, manifest, _ = self.prepare()
        filename = next(iter(manifest["files"].values()))["local"]
        self.store.write(selected, filename, b"tampered", replace=True)
        self.assertEqual(self.invoke("run", selected)[1]["error"], "staged_content_changed")
        self.store.write(selected, "manifest.json", b"{}", replace=True)
        self.assertEqual(self.invoke("status", selected)[1]["error"], "manifest_changed")
        self.path.unlink()
        self.path.symlink_to(self.hosts)
        with self.assertRaises(OSError):
            data.read_file(self.path)

    def test_schema_rejects_shell_paths_overrides_duplicates_and_legacy(self):
        for update in ({"command": "echo secret"}, {"schema_version": True}, {"fixture": "yes"}):
            with self.subTest(update=update), self.assertRaises(data.ExperimentError):
                data.specification(dict(self.value, **update))
        for config in (
            {"enable_paarc": True, "ulimit_nofile": "1; false"},
            {"enable_paarc": True, "enable_polite_controller": True},
            {"enable_paarc": "false"},
            {"enable_paarc": True, "beta": float("nan")},
        ):
            with self.assertRaises(data.ExperimentError):
                data.case_config(config)
        with self.assertRaises(data.ExperimentError):
            data.parse(b'{"x":1,"x":2}')
        with self.assertRaises(data.ExperimentError):
            data.guest_path("/home/test/../escape")

    def test_supplied_partitions_validate_real_parquet_and_rows(self):
        import polars as pl

        path = self.root / "input.parquet"
        pl.DataFrame({"url": ["https://example.invalid/image.png"]}).write_parquet(path)
        self.value.pop("fixture")
        self.value["partitions"] = [{"path": str(path), "rows": 1}]
        selected, manifest, _ = self.prepare()
        self.assertEqual(manifest["partitions"]["paarc-on"][0]["rows"], 1)
        self.assertEqual(self.invoke("status", selected)[0], 0)
        with self.assertRaisesRegex(data.ExperimentError, "invalid_partition"):
            data.parquet_rows(b"invalid", "url")
        self.value["partitions"][0]["rows"] = 2
        self.path.write_bytes(data.encode(self.value))
        with self.assertRaisesRegex(data.ExperimentError, "partition_row_count_mismatch"):
            cli.prepare(self.path)

    def test_lock_collision_and_outside_git_paths(self):
        selected, _, _ = self.prepare()
        with self.store.lock(), self.assertRaisesRegex(data.ExperimentError, "experiment_busy"):
            with self.store.lock():
                pass
        self.store.claim(selected)
        with self.assertRaisesRegex(data.ExperimentError, "another_experiment_incomplete"):
            self.store.claim("exp-" + "1" * 32)
        with self.assertRaises(ops.OpsError):
            with data.Store(REPO).directory():
                pass

    def test_strict_ssh_configuration_and_missing_host_keys(self):
        registered = transport.binding(self.journal.read())
        addresses = {role: entry["fixed_ip"] for role, entry in access()["interfaces"].items()}
        raw = transport.ssh_config(self.value["ssh"], registered, addresses).decode()
        for value in (
            "StrictHostKeyChecking yes",
            "BatchMode yes",
            "ProxyJump manager",
            "PermitLocalCommand no",
            "IdentitiesOnly yes",
            "ForwardAgent no",
        ):
            self.assertIn(value, raw)
        self.assertNotIn("ProxyCommand", raw)
        self.hosts.write_text("")
        with self.assertRaisesRegex(data.ExperimentError, "enrolled_host_key_missing"):
            transport.ssh_preflight(self.value["ssh"], registered)

    def infrastructure(self, manifest, failure=None):
        calls, captures = [], {}
        self.fake_captures = captures
        test = self

        class Controller:
            def __init__(self, root, expected):
                self.running = False

            def preflight(self, window):
                calls.append("preflight")
                if failure == "allowance":
                    raise data.ExperimentError("insufficient_allowance")

            def call(self, action, **kwargs):
                calls.append(action)
                if action == "start":
                    self.running = True
                    if failure == "lost-start":
                        raise data.ExperimentError("subprocess_timeout")
                if action == "stop" and failure != "cleanup":
                    self.running = False
                return {"running": self.running}

            def addresses(self, seconds):
                return {role: entry["fixed_ip"] for role, entry in access()["interfaces"].items()}

        class Transport:
            def __init__(self, *args):
                pass

            def call(self, role, action, case="all", **kwargs):
                calls.append(f"{role}:{action}:{case}")
                if failure == "signal" and action == "deploy":
                    os.kill(os.getpid(), signal.SIGTERM)
                if failure == "dependency" and action == "probe":
                    raise data.ExperimentError("guest_operation_failed")
                if action == "deploy":
                    captures.update(members(kwargs["data"], data.LIMIT))
                if action == "status":
                    return {"ActiveState": "inactive", "Result": "success", "ExecMainStatus": "0"}
                if action == "collect":
                    if role == "worker":
                        return bundle({"worker.log": b"fake worker boundary"})
                    if role == "origin":
                        rows = []
                        for name in (c["name"] for c in manifest["cases"]):
                            for i in range(64):
                                for status in [503, 200] if i == 0 else [200]:
                                    rows.append(
                                        {
                                            "path": f"/{name}/{i}.png",
                                            "status": status,
                                            "source": access()["interfaces"]["worker"]["fixed_ip"],
                                        }
                                    )
                        return bundle({"origin.jsonl": b"".join(data.encode(r) for r in rows)})
                    outputs = {
                        "tasks.json": data.encode(
                            {
                                "submitted": 2,
                                "tasks": [
                                    {"id": i, "successful": True, "exit_code": 0, "log_truncated": False}
                                    for i in (1, 2)
                                ],
                            }
                        )
                    }
                    mode = next(c["config"]["enable_paarc"] for c in manifest["cases"] if c["name"] == case)
                    outputs["resolved-config.json"] = data.encode({"enable_paarc": mode})
                    outputs.update({f"task-{i}.log": b"fake task boundary" for i in (1, 2)})
                    for group in range(2):
                        content = {
                            f"output/image-{i}.png": captures[f"images/{i}.png"]
                            for i in range(group * 32, group * 32 + 32)
                        }
                        content["output/overview.json"] = data.encode(
                            {
                                "script_inputs": {"enable_paarc": mode},
                                "summary": {
                                    "total_urls": 32,
                                    "successful_downloads": 32,
                                    "failed_downloads": 0,
                                    "shutdown_requested": False,
                                },
                            }
                        )
                        if failure == "hash":
                            content["output/image-0.png"] = b"corrupt"
                        outputs[f"output_part-{group:03}.tar.gz"] = bundle(content)
                    if failure == "missing":
                        outputs.pop("output_part-001.tar.gz")
                    return bundle(outputs)
                return {"ok": True}

        stack = contextlib.ExitStack()
        stack.enter_context(patch.object(cli, "Controller", Controller))
        stack.enter_context(patch.object(cli, "Transport", Transport))
        stack.enter_context(patch.object(cli, "ready", lambda v: v["running"]))
        stack.enter_context(patch.object(cli, "clean", lambda v: not v["running"]))
        stack.enter_context(patch.object(cli, "remaining", lambda v: 1000))
        test.addCleanup(stack.close)
        return calls

    def test_success_replay_refusal_and_repeated_collection(self):
        selected, manifest, _ = self.prepare()
        calls = self.infrastructure(manifest)
        code, outcome = self.invoke("run", selected)
        self.assertEqual(code, 0, outcome)
        self.assertEqual(outcome["data"]["workload"], "passed")
        self.assertEqual(outcome["data"]["cleanup"], "verified")
        self.assertLess(calls.index("origin:stop:all"), calls.index("stop"))
        before = list(calls)
        self.assertEqual(self.invoke("run", selected)[1]["error"], "run_cannot_be_replayed")
        self.assertEqual(self.invoke("collect", selected)[0], 0)
        self.assertEqual(calls, before)

    def test_lost_ack_signal_dependencies_missing_output_and_hash_failures_cleanup(self):
        for failure in ("lost-start", "signal", "dependency", "missing", "hash"):
            with self.subTest(failure=failure):
                selected, manifest, _ = self.prepare()
                calls = self.infrastructure(manifest, failure)
                code, outcome = self.invoke("run", selected)
                self.assertNotEqual(code, 0, outcome)
                self.assertIn("stop", calls)
                self.assertEqual(outcome["data"]["cleanup"], "verified")
                self.assertNotEqual(outcome["data"]["workload"], "passed")
                self.assertIsNone(self.store.owner())

    def test_cleanup_uncertainty_blocks_competing_run_and_recovers(self):
        selected, manifest, _ = self.prepare()
        self.infrastructure(manifest, "cleanup")
        code, outcome = self.invoke("run", selected)
        self.assertEqual(code, 3)
        self.assertEqual(outcome["data"]["workload"], "passed")
        self.assertEqual(outcome["data"]["cleanup"], "uncertain")
        self.assertEqual(self.store.owner(), selected)
        second, _, _ = self.prepare()
        self.assertEqual(self.invoke("run", second)[1]["error"], "another_experiment_incomplete")
        self.infrastructure(manifest)
        self.assertEqual(self.invoke("stop", selected)[0], 0)
        self.assertIsNone(self.store.owner())

    def test_insufficient_allowance_never_starts(self):
        selected, manifest, _ = self.prepare()
        calls = self.infrastructure(manifest, "allowance")
        self.assertEqual(self.invoke("run", selected)[1]["error"], "insufficient_allowance")
        self.assertEqual(calls, ["preflight"])

    def test_remaining_work_below_old_cutoff_is_admitted_after_slow_start(self):
        selected, manifest, _ = self.prepare()
        self.infrastructure(manifest)
        real_clock = time.monotonic
        offset = [0]

        def became_ready(value):
            offset[0] = 400
            return value["running"]

        with (
            patch.object(cli.time, "monotonic", lambda: real_clock() + offset[0]),
            patch.object(cli, "ready", became_ready),
            patch.object(cli, "remaining", lambda value: 460),
        ):
            code, outcome = self.invoke("run", selected)
        self.assertEqual(code, 0, outcome)

    def test_work_budget_expiration_and_pre_activation_cancel(self):
        selected, manifest, _ = self.prepare()
        calls = self.infrastructure(manifest)
        with patch.object(cli, "remaining", lambda value: 100):
            code, outcome = self.invoke("run", selected)
        self.assertEqual(code, 3, outcome)
        self.assertIn("insufficient_work_time", outcome["data"]["errors"])
        self.assertIn("stop", calls)
        self.assertNotIn("manager:deploy:all", calls)
        selected, _, _ = self.prepare()
        self.store.claim(selected)
        self.assertEqual(self.invoke("stop", selected)[0], 0)
        self.assertIsNone(self.store.owner())
        self.assertEqual(self.invoke("run", selected)[1]["error"], "run_cannot_be_replayed")

    def test_partial_collection_retries_preserve_prior_evidence(self):
        selected, manifest, _ = self.prepare()
        self.infrastructure(manifest, "missing")
        self.assertEqual(self.invoke("run", selected)[0], 3)
        first = self.store.json(selected, "state.json")["collected"]["paarc-on"]["file"]
        old = self.store.read(selected, first)
        guest_files = dict(self.fake_captures)
        self.infrastructure(manifest)
        self.fake_captures.update(guest_files)
        # Original failed-run diagnostics stay visible even after missing outputs arrive.
        self.assertEqual(self.invoke("collect", selected)[0], 3)
        updated = self.store.json(selected, "state.json")
        self.assertEqual(updated["workload"], "passed")
        self.assertTrue(updated["collected"]["paarc-on"]["valid"])
        self.assertNotEqual(updated["collected"]["paarc-on"]["file"], first)
        self.assertEqual(self.store.read(selected, first), old)

    def test_two_cli_processes_cancel_only_the_active_run(self):
        selected, _, _ = self.prepare()
        other, _, _ = self.prepare()
        driver = self.root / "fake_cli.py"
        entered = self.root / "guest-entered"
        driver.write_text(
            "import sys\nfrom pathlib import Path\n"
            + f"sys.path.insert(0, {str(REPO / 'bin')!r})\n"
            + "import flowdc_experiment as cli\nfrom flowdc_experiment_process import execute\n"
            + f"ENTERED = Path({str(entered)!r})\n"
            + r"""
class Controller:
    def __init__(self, *args): self.active = False
    def preflight(self, window): pass
    def call(self, action, **kwargs):
        if action == "start": self.active = True
        if action == "stop": self.active = False
        return {"active": self.active}
    def addresses(self, seconds):
        return {"manager": "10.0.0.10", "worker": "10.0.0.11", "origin": "10.0.0.12"}
class Transport:
    def __init__(self, *args): pass
    def call(self, role, action, *args, **kwargs):
        if action == "probe":
            ENTERED.write_text("entered")
            execute([sys.executable, "-c", "import time; time.sleep(60)"], seconds=60)
        return {}
cli.Controller = Controller
cli.Transport = Transport
cli.ready = lambda value: value["active"]
cli.clean = lambda value: not value["active"]
cli.remaining = lambda value: 1000
sys.exit(cli.main())
"""
        )
        args = [sys.executable, str(driver), "run", "--state-root", str(self.state), "--run-id", selected]
        runner = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            deadline = time.monotonic() + 10
            while not entered.exists() and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertTrue(entered.exists())
            wrong = subprocess.run(
                [*args[:2], "stop", "--state-root", str(self.state), "--run-id", other],
                capture_output=True,
                timeout=10,
            )
            self.assertEqual(json.loads(wrong.stdout)["error"], "another_experiment_incomplete")
            started = time.monotonic()
            stopped = subprocess.run(
                [*args[:2], "stop", "--state-root", str(self.state), "--run-id", selected],
                capture_output=True,
                timeout=10,
            )
            self.assertEqual(stopped.returncode, 0, stopped.stdout)
            output, stderr = runner.communicate(timeout=5)
            self.assertEqual(stderr, b"")
            self.assertEqual(runner.returncode, 3, output)
            self.assertIn("cancellation_requested", json.loads(output)["data"]["errors"])
            self.assertLess(time.monotonic() - started, 10)
            self.assertIsNone(self.store.owner())
        finally:
            if runner.poll() is None:
                runner.kill()
            runner.communicate()

    def test_failed_guest_cleanup_retains_owner_but_stops_cloud(self):
        selected, manifest, _ = self.prepare()
        calls = self.infrastructure(manifest)
        base = cli.Transport

        class BrokenStop(base):
            def call(self, role, action, case="all", **kwargs):
                if action == "stop":
                    raise data.ExperimentError("guest_operation_failed")
                return super().call(role, action, case, **kwargs)

        with patch.object(cli, "Transport", BrokenStop):
            code, result = self.invoke("run", selected)
        self.assertEqual(code, 3)
        self.assertIn("stop", calls)
        self.assertEqual(result["data"]["cleanup"], "verified")
        self.assertEqual(result["data"]["guest_cleanup"], "uncertain")
        self.assertEqual(self.store.owner(), selected)
        self.assertEqual(self.invoke("stop", selected)[0], 0)
        self.assertIsNone(self.store.owner())

    def test_interrupted_collection_after_artifact_write_preserves_snapshot(self):
        selected, manifest, state = self.prepare()
        self.infrastructure(manifest)
        self.fake_captures.update(members(self.store.read(selected, "bundle.tar"), data.LIMIT))
        boundary = cli.Transport()
        original_write = self.store.write

        def interrupted(selected, filename, raw, **kwargs):
            original_write(selected, filename, raw, **kwargs)
            if filename.startswith("artifacts-"):
                raise SystemExit("simulated process loss")

        with patch.object(self.store, "write", interrupted), self.assertRaises(SystemExit):
            cli.collect_outputs(self.store, selected, manifest, state, boundary, time.monotonic() + 20)
        snapshots = list((self.state / "runs" / selected).glob("artifacts-*.tar"))
        self.assertEqual(len(snapshots), 1)
        old = snapshots[0].read_bytes()
        restored = self.store.json(selected, "state.json")
        self.assertEqual(restored["collected"], {})
        cli.collect_outputs(self.store, selected, manifest, restored, boundary, time.monotonic() + 20)
        self.assertEqual(restored["workload"], "passed")
        self.assertEqual(snapshots[0].read_bytes(), old)
        self.assertNotEqual(restored["collected"]["paarc-on"]["file"], snapshots[0].name)

    def test_existing_role_host_aliases_are_accepted(self):
        public = " ".join(self.key.with_suffix(".pub").read_text().split()[:2])
        self.hosts.write_text("".join(f"flowdc-{role} {public}\n" for role in data.ROLES))
        registered = transport.binding(self.journal.read())
        self.assertEqual(
            transport.ssh_preflight(self.value["ssh"], registered), data.digest(self.hosts.read_bytes())
        )

    def test_floating_lookup_uses_real_provider_context_and_deadline(self):
        from uuid import uuid4

        from test_flowdc_pilot_lifecycle import PROJECT, VM_IDS

        route = access()
        route["route"].update(mode="floating", external_network_id=str(uuid4()), router_id=str(uuid4()))
        root = self.root / "floating-state"
        profile_path = self.root / "profile.json"
        profile_path.write_text("{}")
        journal = register(profile_path, root, spec(), route)
        client = self.root / "client"
        client.write_text("#!/bin/sh\nexit 1\n")
        client.chmod(0o700)
        wrapper = self.root / "wrapper"
        wrapper.write_text(ops.WRAPPER)
        wrapper.chmod(0o700)
        credential = self.root / "credential"
        credential.write_text("synthetic")
        profile = {
            "expected_project_id": PROJECT,
            "region": "test-region",
            "auth_url": "https://cloud.example.test/v3",
            "intended_server_ids": VM_IDS,
            "openstack_client": str(client),
            "wrapper_path": str(wrapper),
            "credential_file": str(credential),
        }
        floating_id = str(uuid4())
        calls = []

        def boundary(argv, *, timeout, **kwargs):
            self.assertGreater(timeout, 0)
            action = argv[7] if "flowdc-lifecycle" in argv else argv[5]
            calls.append(action)
            replies = {
                "context": {"region_name": "test-region", "auth_url": profile["auth_url"]},
                "project": {"project_id": PROJECT},
                "floating": [{"ID": floating_id}],
                "floating_show": {
                    "id": floating_id,
                    "project_id": PROJECT,
                    "port_id": route["interfaces"]["manager"]["port_id"],
                    "fixed_ip_address": route["interfaces"]["manager"]["fixed_ip"],
                    "floating_ip_address": "192.0.2.20",
                },
            }
            return 0, data.encode(replies[action])

        before = journal.read()
        with (
            patch.object(ops, "load_profile", return_value=profile),
            patch.object(ops, "run_bounded", boundary),
        ):
            result = transport.read_addresses(root)
        self.assertEqual(result["manager"], "192.0.2.20")
        self.assertEqual(calls, ["context", "project", "floating", "floating_show"])
        self.assertEqual(journal.read(), before)

    def test_observed_538_second_startup_with_deployment_and_two_cases(self):
        selected, manifest, _ = self.prepare()
        calls = self.infrastructure(manifest)
        real_clock = time.monotonic
        offset = [0]
        status_calls = [0]
        base_transport = cli.Transport

        class SlowTransport(base_transport):
            def call(self, role, action, case="all", **kwargs):
                if action in ("probe", "deploy"):
                    offset[0] += 5
                if action == "status":
                    offset[0] += 30
                return super().call(role, action, case, **kwargs)

        def startup(value):
            status_calls[0] += 1
            if status_calls[0] == 1:
                offset[0] = 538
            return value["running"]

        with (
            patch.object(cli.time, "monotonic", lambda: real_clock() + offset[0]),
            patch.object(cli, "ready", startup),
            patch.object(cli, "Transport", SlowTransport),
        ):
            code, outcome = self.invoke("run", selected)
        self.assertEqual(code, 0, outcome)
        self.assertIn("manager:launch:paarc-off", calls)

    def test_actual_controller_status_uses_immutable_release(self):
        from dataclasses import asdict

        from flowdc_pilot_cli import MODULES, service_unit
        from flowdc_pilot_supervisor import sample_clock

        content = [(REPO / "bin" / name).read_bytes() for name in MODULES]
        checksum = data.digest(b"".join(content))
        release = self.state / "releases" / ("pilot-" + checksum)
        release.mkdir(mode=0o700, parents=True)
        for name, raw in zip(MODULES, content, strict=True):
            (release / name).write_bytes(raw)
        interpreter = str(Path(sys.executable).resolve())
        service = {
            "unit": "flowdc-pilot.service",
            "release": str(release),
            "digest": checksum,
            "interpreter": interpreter,
            "interpreter_digest": data.digest(Path(interpreter).read_bytes()),
        }
        clock = asdict(sample_clock())

        def update(record):
            record.update(service=service, heartbeat=clock)
            for vm in record["vms"].values():
                vm.update(phase="offloaded", observed={"state": "SHELVED_OFFLOADED", "clock": clock})

        self.journal.change(update)
        record = self.journal.read()
        bound = transport.binding(record)
        unit = self.root / ".config/systemd/user/flowdc-pilot.service"
        unit.parent.mkdir(mode=0o700, parents=True)
        unit.write_bytes(service_unit(service, self.state))
        with (
            patch.dict(os.environ, {"HOME": str(self.root)}),
            patch.object(transport, "verify_unit_origin"),
            patch.object(transport, "require_persistent_session"),
            self.journal.supervisor_lock(),
        ):
            controller = transport.Controller(self.state, bound)
            value = controller.preflight(1800)
            self.assertTrue(transport.clean(value))
            stale = dict(value)
            stale["vms"] = [
                dict(vm, observation_fresh=False, provider_state="UNKNOWN") for vm in value["vms"]
            ]
            sequence = iter([stale, {}, value])
            calls = []

            def call(action, **kwargs):
                calls.append(action)
                return next(sequence)

            with (
                patch.object(controller, "call", call),
                patch.object(transport, "execute", return_value=(0, b"{}")) as proof,
            ):
                self.assertTrue(transport.clean(controller.preflight(1800)))
                self.assertEqual(calls, ["status", "reconcile", "status"])
                self.assertIn("verify-idle", proof.call_args.args[0])
            self.journal.change(
                lambda record: [vm["account"].update(consumed=6000) for vm in record["vms"].values()]
            )
            with self.assertRaisesRegex(data.ExperimentError, "insufficient_allowance"):
                controller.preflight(1800)
            (release / MODULES[0]).write_text("tampered")
            with self.assertRaises(ops.OpsError) as raised:
                controller.call("start")
            self.assertEqual(raised.exception.code, "installed_release_changed")


class ArchiveAndProcessTests(unittest.TestCase):
    def test_unsafe_archives_and_size_limit(self):
        for filename, kind in (
            ("../escape", tarfile.REGTYPE),
            ("/absolute", tarfile.REGTYPE),
            ("link", tarfile.SYMTYPE),
            ("pipe", tarfile.FIFOTYPE),
        ):
            raw = io.BytesIO()
            with tarfile.open(fileobj=raw, mode="w") as archive:
                item = tarfile.TarInfo(filename)
                item.type = kind
                archive.addfile(item)
            with self.assertRaises(data.ExperimentError):
                members(raw.getvalue(), 100000)
        with self.assertRaisesRegex(data.ExperimentError, "artifact_size_limit"):
            members(bundle({"file": b"x"}), 1)

    def test_subprocess_timeout_output_limit_and_no_secret_stderr(self):
        start = time.monotonic()
        with self.assertRaisesRegex(data.ExperimentError, "subprocess_timeout"):
            execute([sys.executable, "-c", "import time; time.sleep(10)"], seconds=0.1)
        self.assertLess(time.monotonic() - start, 2)
        with self.assertRaisesRegex(data.ExperimentError, "subprocess_output_limit"):
            execute([sys.executable, "-c", "print('x'*1000)"], seconds=2, maximum=10)
        code, raw = execute(
            [sys.executable, "-c", "import sys; print('secret-marker',file=sys.stderr)"], seconds=2
        )
        self.assertEqual((code, raw), (0, b""))


if __name__ == "__main__":
    unittest.main()
