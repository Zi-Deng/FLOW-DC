"""Experiment CLI/storage/transport evidence using temporary state and fake infrastructure."""

import contextlib
import io
import json
import os
import shutil
import signal
import subprocess
import sys
import sysconfig
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
from flowdc_experiment_process import CANCEL_CHECK, execute
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

    def test_cloud_stop_precedes_slow_cleanup_bookkeeping(self):
        selected, manifest, state = self.prepare()
        elapsed, dispatched = [0], []
        original = cli.save

        def slow_save(*args, **kwargs):
            elapsed[0] += 2
            return original(*args, **kwargs)

        class Controller:
            def call(self, action, seconds):
                if seconds <= 0:
                    raise data.ExperimentError("deadline_expired")
                dispatched.append(action)
                return {}

        with patch.object(cli.time, "monotonic", lambda: elapsed[0]), patch.object(cli, "save", slow_save):
            cli.cleanup(self.store, selected, manifest, state, Controller())
        self.assertEqual(dispatched, ["stop"])
        self.assertEqual(state["cleanup"], "uncertain")

    def test_partition_reads_use_remaining_aggregate_staging_budget(self):
        self.value.pop("fixture")
        partitions = []
        for index in range(3):
            path = self.root / f"large-{index}.parquet"
            path.write_bytes(b"x" * (100000 if index == 0 else 600000))
            partitions.append({"path": str(path), "rows": 1})
        self.value["partitions"] = partitions
        self.path.write_bytes(data.encode(self.value))
        real_read, attempts = cli.read_file, []

        def observed_read(path, maximum=data.LIMIT, **kwargs):
            if str(path).endswith(".parquet"):
                attempts.append(maximum)
            return real_read(path, maximum, **kwargs)

        with (
            patch.object(cli, "LIMIT", 2 * 1048576),
            patch.object(cli, "read_file", observed_read),
            patch.object(cli, "parquet_rows", return_value=1),
        ):
            with self.assertRaisesRegex(data.ExperimentError, "staging_size_limit"):
                cli.prepare(self.path)
        self.assertEqual(len(attempts), 2)
        self.assertLess(attempts[0], 524288)
        self.assertEqual(attempts[0] - attempts[1], 100000)

    def test_partition_validation_cached_per_distinct_url_column(self):
        import polars as pl

        path = self.root / "input.parquet"
        pl.DataFrame(
            {"url": ["https://example.invalid/a"], "alternate": ["https://example.invalid/b"]}
        ).write_parquet(path)
        self.value.pop("fixture")
        self.value["partitions"] = [{"path": str(path), "rows": 1}]
        self.path.write_bytes(data.encode(self.value))
        with patch.object(cli, "parquet_rows", wraps=cli.parquet_rows) as validate:
            cli.prepare(self.path)
        self.assertEqual(validate.call_count, 1)
        case = self.value["cases"][1]
        Path(case["config"]).write_bytes(data.encode({"enable_paarc": False, "url_col": "alternate"}))
        with patch.object(cli, "parquet_rows", wraps=cli.parquet_rows) as validate:
            cli.prepare(self.path)
        self.assertEqual({c.args[1] for c in validate.call_args_list}, {"url", "alternate"})

    def test_completed_worker_and_origin_validation_survives_interruption(self):
        for boundary in ("origin_acquire", "terminal_save"):
            with self.subTest(boundary=boundary):
                selected, manifest, state = self.prepare()
                self.infrastructure(manifest)
                self.fake_captures.update(members(self.store.read(selected, "bundle.tar"), data.LIMIT))
                transport = cli.Transport()
                original_call, original_save = transport.call, cli.save

                def call(role, action, *args, boundary=boundary, original_call=original_call, **kwargs):
                    if role == "origin" and boundary == "origin_acquire":
                        raise SystemExit("process loss")
                    return original_call(role, action, *args, **kwargs)

                def save(store, selected, current, *args, original_save=original_save):
                    if current["workload"] == "passed":
                        raise SystemExit("process loss")
                    return original_save(store, selected, current, *args)

                with (
                    patch.object(transport, "call", call),
                    patch.object(cli, "save", save),
                    self.assertRaises(SystemExit),
                ):
                    cli.collect_outputs(
                        self.store, selected, manifest, state, transport, time.monotonic() + 20
                    )
                restored = self.store.json(selected, "state.json")
                self.assertTrue(restored["collected"]["paarc-off-worker"]["valid"])
                if boundary == "terminal_save":
                    self.assertTrue(restored["collected"]["origin"]["valid"])
                    cli.collect_outputs(self.store, selected, manifest, restored, None, time.monotonic() + 20)
                    self.assertEqual(restored["workload"], "passed")

    def test_nonrun_interrupt_returns_fixed_json_and_restores_umask(self):
        selected, _, _ = self.prepare()
        previous = os.umask(0o027)
        try:
            with patch.object(cli, "load", side_effect=KeyboardInterrupt):
                code, result = self.invoke("status", selected)
            self.assertEqual(code, 2)
            self.assertEqual(result["error"], "experiment_interrupted")
            self.assertEqual(os.umask(0o027), 0o027)
            with self.store.lock():
                pass
        finally:
            os.umask(previous)

    def test_cleanup_uses_configured_budget_for_slow_outstanding_stops(self):
        selected, manifest, state = self.prepare()
        manifest["spec"]["bounds"]["stop_seconds"] = 120
        self.store.claim(selected)
        state["services"] = [
            {"role": "manager", "case": "previous", "stopped": True},
            {"role": "worker", "case": "previous", "stopped": True},
            {"role": "origin", "case": "all"},
            {"role": "manager", "case": "current"},
            {"role": "worker", "case": "current"},
        ]
        elapsed, calls = [0], []

        class Controller:
            def call(self, action, **kwargs):
                calls.append(action)
                return {}

        class SlowStop:
            def call(self, role, action, case, seconds):
                calls.append((role, case))
                elapsed[0] += min(seconds, 15)
                if seconds < 15:
                    raise data.ExperimentError("subprocess_timeout")

        with (
            patch.object(cli.time, "monotonic", lambda: elapsed[0]),
            patch.object(cli, "clean", return_value=True),
        ):
            cli.cleanup(self.store, selected, manifest, state, Controller(), SlowStop())
        self.assertEqual(calls[0], "stop")
        self.assertEqual(state["guest_cleanup"], "verified")
        self.assertEqual(state["cleanup"], "verified")
        self.assertEqual(elapsed[0], 45)
        self.assertIsNone(self.store.owner())
        self.assertNotIn(("manager", "previous"), calls)

    def test_expired_case_reports_specific_deadline_before_status(self):
        selected, manifest, _ = self.prepare()
        self.infrastructure(manifest)
        real_clock, offset = time.monotonic, [0]
        base = cli.Transport

        class ExpiredLaunch(base):
            def call(self, role, action, case="all", **kwargs):
                if action == "launch" and role == "worker":
                    offset[0] += 400
                if action == "status" and kwargs["seconds"] <= 0:
                    raise data.ExperimentError("deadline_expired")
                return super().call(role, action, case, **kwargs)

        with (
            patch.object(cli.time, "monotonic", lambda: real_clock() + offset[0]),
            patch.object(cli, "Transport", ExpiredLaunch),
        ):
            code, result = self.invoke("run", selected)
        self.assertEqual(code, 3)
        self.assertIn("case_deadline_expired", result["data"]["errors"])
        self.assertNotIn("deadline_expired", result["data"]["errors"])

    def test_partition_row_admission_matches_nested_archive_bound(self):
        value = dict(self.value)
        value.pop("fixture")
        value["partitions"] = [{"path": str(self.path), "rows": 4093}]
        data.specification(value)
        value["partitions"][0]["rows"] = 4094
        with self.assertRaises(data.ExperimentError):
            data.specification(value)

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

        from flowdc_pilot_cli import MODULES, service_unit, trusted_bytes
        from flowdc_pilot_supervisor import sample_clock

        content = [(REPO / "bin" / name).read_bytes() for name in MODULES]
        checksum = data.digest(b"".join(content))
        release = self.state / "releases" / ("pilot-" + checksum)
        release.mkdir(mode=0o700, parents=True)
        for name, raw in zip(MODULES, content, strict=True):
            (release / name).write_bytes(raw)
        # Model a setup-python cache whose writable ancestor is not trusted.
        cache = self.root / "tool-cache"
        cache.mkdir(mode=0o700)
        cache.chmod(0o777)
        cached_python = cache / "python"
        shutil.copyfile(Path(sys.executable).resolve(), cached_python)
        cached_python.chmod(0o700)
        with self.assertRaises(ops.OpsError):
            trusted_bytes(cached_python, executable=True)
        # Copy the runnable installation layout, not just its executable: hosted
        # Python uses a shared library and discovers stdlib relative to its prefix.
        prefix = self.root / "private-python"
        (prefix / "bin").mkdir(mode=0o700, parents=True)
        library = prefix / "lib"
        library.mkdir(mode=0o700)
        version = f"python{sys.version_info.major}.{sys.version_info.minor}"
        shutil.copytree(
            sysconfig.get_path("stdlib"),
            library / version,
            ignore=shutil.ignore_patterns("site-packages", "dist-packages", "__pycache__"),
        )
        if sysconfig.get_config_var("Py_ENABLE_SHARED"):
            for shared in Path(sysconfig.get_config_var("LIBDIR")).glob(f"lib{version}.so*"):
                shutil.copyfile(shared, library / shared.name)
        private_python = prefix / "bin" / "python"
        shutil.copyfile(cached_python, private_python)
        private_python.chmod(0o700)
        interpreter = str(private_python)
        self.assertEqual(trusted_bytes(private_python, executable=True), cached_python.read_bytes())
        runtime = subprocess.run(
            [
                interpreter,
                "-E",
                "-s",
                "-B",
                "-c",
                "import json, sys, encodings, hashlib, selectors; "
                "print(json.dumps([sys.prefix, encodings.__file__, list(sys.version_info[:2])]))",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        self.assertEqual(runtime.returncode, 0, runtime.stderr)
        runtime_prefix, encoding_path, runtime_version = json.loads(runtime.stdout)
        self.assertEqual(Path(runtime_prefix), prefix)
        self.assertTrue(Path(encoding_path).is_relative_to(prefix))
        self.assertGreaterEqual(runtime_version, [3, 12])
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
            original_interpreter = private_python.read_bytes()
            private_python.write_bytes(b"tampered")
            with self.assertRaises(ops.OpsError) as raised:
                controller.call("start")
            self.assertEqual(raised.exception.code, "installed_interpreter_changed")
            private_python.write_bytes(original_interpreter)
            (release / MODULES[0]).write_text("tampered")
            with self.assertRaises(ops.OpsError) as raised:
                controller.call("start")
            self.assertEqual(raised.exception.code, "installed_release_changed")


class ArchiveAndProcessTests(unittest.TestCase):
    def test_cancellation_after_stdout_eof(self):
        started = time.monotonic()

        def cancel():
            if time.monotonic() - started >= 0.2:
                raise data.ExperimentError("cancellation_requested")

        token = CANCEL_CHECK.set(cancel)
        try:
            with self.assertRaisesRegex(data.ExperimentError, "cancellation_requested"):
                execute(
                    [sys.executable, "-c", "import os,time; os.close(1); time.sleep(2)"],
                    seconds=3,
                )
        finally:
            CANCEL_CHECK.reset(token)
        self.assertLess(time.monotonic() - started, 1.5)

    def test_deadline_after_stdout_eof(self):
        started = time.monotonic()
        with self.assertRaisesRegex(data.ExperimentError, "subprocess_timeout"):
            execute(
                [sys.executable, "-c", "import os,time; os.close(1); time.sleep(2)"],
                seconds=0.2,
            )
        self.assertLess(time.monotonic() - started, 1.5)

    def test_group_cleanup_precedes_reaping_exited_leader(self):
        real_killpg = os.killpg
        observed = []

        def kill_owned_group(pid, sig):
            # WNOWAIT proves the real child remains waitable at signaling time.
            status = os.waitid(os.P_PID, pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
            observed.append(status)
            return real_killpg(pid, sig)

        with patch("flowdc_experiment_process.os.killpg", kill_owned_group):
            code, output = execute([sys.executable, "-c", "print('done'); raise SystemExit(7)"], seconds=3)
        self.assertEqual((code, output), (7, b"done\n"))
        self.assertEqual(len(observed), 1)
        self.assertEqual(observed[0].si_status, 7)

    def test_nondefault_child_management_refused_before_spawn(self):
        for disposition in (signal.SIG_IGN, lambda *args: None):
            with (
                patch("flowdc_experiment_process.signal.getsignal", return_value=disposition),
                patch(
                    "flowdc_experiment_process.subprocess.Popen",
                    side_effect=AssertionError("unexpected child spawn"),
                ) as spawn,
                self.assertRaisesRegex(data.ExperimentError, "subprocess_child_management"),
            ):
                execute([sys.executable, "-c", "pass"], seconds=1)
            spawn.assert_not_called()

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
