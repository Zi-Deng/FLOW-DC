"""Offline operations CLI tests; all writes stay in disposable temporary roots."""

import contextlib
import copy
import importlib.util
import io
import json
import os
import stat
import subprocess
import sys
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / "bin/flowdc_ops.py"
SPEC = importlib.util.spec_from_file_location("flowdc_ops", SCRIPT)
ops = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ops)


class InitTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="flowdc-ops-")
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.state = self.root / "state"
        self.config = self.root / "config"
        self.arguments = ["init", "--state-root", str(self.state), "--config-root", str(self.config)]

    def run_cli(self, *arguments, env=None):
        result = subprocess.run(
            [sys.executable, str(SCRIPT), *arguments], capture_output=True, text=True, timeout=10, env=env
        )
        self.assertEqual(result.stderr, "", result.stderr)
        value = json.loads(result.stdout)
        self.assertEqual(value["schema_version"], 1)
        self.assertEqual(
            set(value), {"schema_version", "operation", "status", "checks", "errors", "next_actions", "data"}
        )
        return result.returncode, value

    def initialize(self):
        code, value = self.run_cli(*self.arguments)
        self.assertEqual(code, 0, value)
        self.assertEqual(value["status"], "ok")
        return value

    def assert_invalid(self, error="unsafe_path"):
        code, value = self.run_cli(*self.arguments)
        self.assertEqual(code, 2, value)
        self.assertEqual(value["errors"][0]["code"], error)
        self.assertTrue(value["next_actions"])

    def test_init_creates_private_layout_and_unconfigured_profile(self):
        value = self.initialize()
        for directory in (self.state, self.config, *(self.state / name for name in ops.DIRECTORIES)):
            self.assertEqual(stat.S_IMODE(directory.stat().st_mode), 0o700)
        profile = self.config / "profile.json"
        self.assertEqual(stat.S_IMODE(profile.stat().st_mode), 0o600)
        self.assertEqual(
            json.loads(profile.read_text()),
            {
                "schema_version": 1,
                "expected_project_id": None,
                "region": None,
                "auth_url": None,
                "credential_file": None,
                "wrapper_path": str(self.config / "js2"),
                "openstack_client": None,
                "intended_server_ids": [],
                "known_seed_id": None,
            },
        )
        self.assertIn({"name": "cloud_readiness", "state": "not_assessed"}, value["checks"])
        self.assertTrue(value["next_actions"])

    def test_repeat_preserves_contents_inode_and_mtime_without_reading_profile(self):
        self.initialize()
        profile = self.config / "profile.json"
        profile.write_text("operator-edited-secret-marker")
        previous = profile.stat()
        artifact = self.state / "runs" / "keep"
        artifact.write_text("previous run")
        stdout = io.StringIO()
        with patch.object(Path, "read_text", side_effect=AssertionError("must not read existing files")):
            with contextlib.redirect_stdout(stdout):
                self.assertEqual(ops.main(self.arguments), 0)
        current = profile.stat()
        self.assertEqual((previous.st_ino, previous.st_mtime_ns), (current.st_ino, current.st_mtime_ns))
        self.assertEqual(profile.read_text(), "operator-edited-secret-marker")
        self.assertEqual(artifact.read_text(), "previous run")
        self.assertNotIn("secret-marker", stdout.getvalue())
        self.assertIn({"name": "profile", "state": "preserved"}, json.loads(stdout.getvalue())["checks"])

    def test_default_locations_use_home(self):
        code, value = self.run_cli("init", env={**os.environ, "HOME": str(self.root)})
        self.assertEqual(code, 0, value)
        self.assertTrue((self.root / ".local/share/flowdc-ops/inventory").is_dir())
        self.assertTrue((self.root / ".config/flowdc/profile.json").is_file())

    def test_parent_permissions_unchanged_and_missing_parents_private(self):
        self.root.chmod(0o755)
        self.state = self.root / "nested" / "state"
        self.arguments[2] = str(self.state)
        self.initialize()
        self.assertEqual(stat.S_IMODE(self.root.stat().st_mode), 0o755)
        self.assertEqual(stat.S_IMODE(self.state.parent.stat().st_mode), 0o700)

    def test_creation_modes_and_umask_restoration(self):
        for mask in (0, 0o777):
            with self.subTest(mask=mask), tempfile.TemporaryDirectory(dir=self.root) as directory:
                args = ["init", "--state-root", f"{directory}/state", "--config-root", f"{directory}/config"]
                old = os.umask(mask)
                try:
                    with contextlib.redirect_stdout(io.StringIO()):
                        self.assertEqual(ops.main(args), 0)
                    observed_mask = os.umask(mask)
                    self.assertEqual(observed_mask, mask)
                finally:
                    os.umask(old)
                self.assertEqual(stat.S_IMODE(Path(directory, "state").stat().st_mode), 0o700)
                self.assertEqual(stat.S_IMODE(Path(directory, "config/profile.json").stat().st_mode), 0o600)

    def test_rejects_symlink_root_without_writing_target(self):
        target = self.root / "target"
        target.mkdir(mode=0o700)
        self.state.symlink_to(target, target_is_directory=True)
        self.assert_invalid()
        self.assertEqual(list(target.iterdir()), [])
        self.assertFalse(self.config.exists())

    def test_rejects_symlink_ancestor_and_dangling_link(self):
        target = self.root / "target"
        target.mkdir(mode=0o700)
        link = self.root / "link"
        link.symlink_to(target, target_is_directory=True)
        self.arguments[2] = str(link / "state")
        self.assert_invalid()
        self.assertEqual(list(target.iterdir()), [])
        self.arguments[2] = str(self.state)
        self.state.symlink_to(self.root / "missing")
        self.assert_invalid()

    def test_rejects_symlink_child_before_other_creation(self):
        self.state.mkdir(mode=0o700)
        (self.state / "runs").symlink_to(self.root / "missing")
        self.assert_invalid()
        self.assertFalse((self.state / "inventory").exists())
        self.assertFalse(self.config.exists())

    def test_rejects_symlink_profile_and_preserves_target(self):
        self.config.mkdir(mode=0o700)
        target = self.root / "target"
        target.write_text("keep me")
        (self.config / "profile.json").symlink_to(target)
        self.assert_invalid()
        self.assertEqual(target.read_text(), "keep me")
        self.assertFalse(self.state.exists())

    def test_rejects_nonregular_profile(self):
        self.config.mkdir(mode=0o700)
        profile = self.config / "profile.json"
        for kind in ("directory", "fifo"):
            with self.subTest(kind=kind):
                if kind == "directory":
                    profile.mkdir(mode=0o700)
                else:
                    os.mkfifo(profile, 0o600)
                self.assert_invalid()
                if kind == "directory":
                    profile.rmdir()
                else:
                    profile.unlink()
        self.assertFalse(self.state.exists())

    def test_rejects_hardlinked_profile(self):
        self.config.mkdir(mode=0o700)
        target = self.root / "target"
        target.write_text("private")
        target.chmod(0o600)
        (self.config / "profile.json").hardlink_to(target)
        self.assert_invalid()
        self.assertEqual(target.read_text(), "private")

    def test_rejects_nonprivate_root_and_keeps_mode(self):
        self.state.mkdir(mode=0o755)
        self.assert_invalid("unsafe_permissions")
        self.assertEqual(stat.S_IMODE(self.state.stat().st_mode), 0o755)
        self.assertEqual(list(self.state.iterdir()), [])

    def test_rejects_nonprivate_profile_and_keeps_mode(self):
        self.initialize()
        profile = self.config / "profile.json"
        profile.chmod(0o644)
        self.assert_invalid("unsafe_permissions")
        self.assertEqual(stat.S_IMODE(profile.stat().st_mode), 0o644)

    def test_rejects_wrong_owner(self):
        actual = self.root.stat()
        wrong_owner = os.stat_result(
            (
                actual.st_mode,
                actual.st_ino,
                actual.st_dev,
                actual.st_nlink,
                os.geteuid() + 1,
                actual.st_gid,
                actual.st_size,
                actual.st_atime,
                actual.st_mtime,
                actual.st_ctime,
            )
        )
        with self.assertRaises(ops.OpsError) as caught:
            ops.private_metadata(wrong_owner, directory=True)
        self.assertEqual(caught.exception.code, "unsafe_permissions")

    def test_rejects_untrusted_writable_ancestor_without_chmod(self):
        self.root.chmod(0o777)
        self.assert_invalid()
        self.assertEqual(stat.S_IMODE(self.root.stat().st_mode), 0o777)
        self.assertFalse(self.state.exists())

    def test_rejects_file_as_state_root(self):
        self.state.write_text("keep")
        self.assert_invalid()
        self.assertEqual(self.state.read_text(), "keep")

    def test_rejects_git_checkout_roots(self):
        for kind in ("directory", "worktree_file"):
            with self.subTest(kind=kind):
                marker = self.root / ".git"
                if kind == "directory":
                    marker.mkdir()
                    (marker / "HEAD").write_text("ref: refs/heads/main\n")
                else:
                    marker.write_text("gitdir: /not/read")
                self.assert_invalid("git_workspace")
                if kind == "directory":
                    (marker / "HEAD").unlink()
                    marker.rmdir()
                else:
                    marker.unlink()
        self.assertFalse(self.state.exists())

    def test_empty_git_marker_is_not_a_checkout(self):
        (self.root / ".git").mkdir()
        self.initialize()

    def test_rejects_relative_traversal_and_overlapping_roots(self):
        for path in (
            "relative",
            "/",
            f"{self.root}/../escape",
            str(self.config),
            str(self.config / "state"),
            f"/{self.config}",
        ):
            with self.subTest(path=path):
                self.arguments[2] = path
                self.assert_invalid()
        self.assertFalse(self.config.exists())

    def test_disappearing_existing_profile_is_an_operational_error(self):
        self.initialize()
        stdout = io.StringIO()
        with patch.object(ops, "preflight_root"):
            original_stat = ops.os.stat

            def disappeared(path, *args, **kwargs):
                if path == "profile.json":
                    raise FileNotFoundError
                return original_stat(path, *args, **kwargs)

            with patch.object(ops.os, "stat", side_effect=disappeared):
                with contextlib.redirect_stdout(stdout):
                    self.assertEqual(ops.main(self.arguments), 1)
        self.assertEqual(json.loads(stdout.getvalue())["errors"][0]["code"], "concurrent_change")

    def test_paths_with_shell_text_remain_literal_data(self):
        self.arguments[2] = str(self.root / "state; $(touch should-not-exist)")
        self.initialize()
        self.assertTrue(Path(self.arguments[2]).is_dir())
        self.assertEqual(
            sorted(path.name for path in self.root.iterdir()), ["config", "state; $(touch should-not-exist)"]
        )

    def test_errors_and_help_are_json_and_do_not_echo_argument_values(self):
        for args in ([], ["inventory"], ["--token=secret-marker"], ["init", "--state", "secret-marker"]):
            with self.subTest(args=args):
                code, value = self.run_cli(*args)
                self.assertEqual(code, 2)
                self.assertEqual(value["errors"][0]["code"], "invalid_arguments")
                self.assertNotIn("secret-marker", json.dumps(value))
        code, value = self.run_cli("--help")
        self.assertEqual(code, 0)
        self.assertEqual(value["operation"], "help")
        self.assertIn("init", value["data"]["help"])

    def test_local_io_error_suppresses_raw_exception_and_restores_umask(self):
        old = os.umask(0o027)
        stdout = io.StringIO()
        try:
            with patch.object(ops.os, "mkdir", side_effect=OSError("provider-secret-marker")):
                with contextlib.redirect_stdout(stdout):
                    self.assertEqual(ops.main(self.arguments), 1)
            self.assertEqual(os.umask(0o027), 0o027)
        finally:
            os.umask(old)
        value = json.loads(stdout.getvalue())
        self.assertEqual(value["errors"][0]["code"], "local_io_error")
        self.assertNotIn("secret-marker", stdout.getvalue())


PROJECT = "11111111-1111-4111-8111-111111111111"
SERVERS = [f"22222222-2222-4222-8222-{index:012d}" for index in (1, 2, 3)]
IMAGE = "33333333-3333-4333-8333-333333333333"
NETWORK = "44444444-4444-4444-8444-444444444444"
FLAVOR = "55555555-5555-4555-8555-555555555555"
CONTEXT = {"project_id": PROJECT, "region": "test-region", "auth_url": "https://cloud.example.test/v3"}

# This fake administration client is reached through the real generated Bash
# wrapper. Fixtures contain synthetic values only, and no network call is made.
FAKE_CLIENT = r"""
import json
import os
from pathlib import Path
import sys
import time
root = Path(__file__).parent
fixture = json.loads((root / 'fixtures.json').read_text())
args = sys.argv[1:]
if args[:4] != ['--os-interface', 'public', '--os-compute-api-version', '2.1']:
    sys.exit(70)
args = args[4:]
key = ' '.join(args[:2])
with (root / 'calls.jsonl').open('a') as out:
    out.write(json.dumps(args) + '\n')
if any(k in os.environ for k in ('OS_TOKEN', 'OS_CLOUD', 'BASH_ENV', 'PYTHONPATH')):
    sys.exit(71)
if os.environ.get('OS_APPLICATION_CREDENTIAL_SECRET') != 'synthetic-openrc-secret':
    sys.exit(72)
mode = fixture.get('failure', {})
if mode.get('key') == key:
    action = mode['action']
    if action == 'fail':
        print('provider-secret-marker', file=sys.stderr)
        print('provider-secret-marker')
        sys.exit(9)
    if action == 'sleep':
        time.sleep(10)
    if action == 'oversize':
        print('provider-secret-marker' * 40000)
        sys.exit(0)
    if action == 'malformed':
        print('provider-secret-marker')
        sys.exit(0)
value = fixture['responses'][key]
if key in ('server show', 'flavor show'):
    value = value[args[2]]
if fixture.get('column_contract'):
    # OSC 10.3 environment-based configuration uses top-level auth_url. Its
    # default network headers omit Status until --long is requested.
    columns = [args[index + 1] for index, arg in enumerate(args) if arg == '-c']
    if key == 'configuration show':
        value = {key: item for key, item in value.items() if key in columns}
    if key == 'network list' and '--long' not in args:
        value = [{key: item for key, item in row.items() if key != 'Status'} for row in value]
print(json.dumps(value))
"""


class PreflightTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="flowdc-preflight-")
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.state, self.config = self.root / "state", self.root / "config"
        self.init_args = ["init", "--state-root", str(self.state), "--config-root", str(self.config)]
        self.invoke(self.init_args, 0)
        self.profile_path = self.config / "profile.json"
        self.profile = json.loads(self.profile_path.read_text())
        self.credential = self.config / "openrc.sh"
        self.credential.write_text(
            "set -x\nprintf 'source-secret-marker\\n'\n"
            "export OS_AUTH_TYPE=v3applicationcredential\n"
            "export OS_APPLICATION_CREDENTIAL_ID=synthetic-credential\n"
            "export OS_APPLICATION_CREDENTIAL_SECRET=synthetic-openrc-secret\n"
            "export OS_AUTH_URL=https://cloud.example.test/v3\nexport OS_REGION_NAME=test-region\n"
        )
        self.credential.chmod(0o600)
        self.client = self.root / "fake-openstack"
        self.client.write_text(f"#!{sys.executable}\n" + FAKE_CLIENT)
        self.client.chmod(0o700)
        self.profile.update(
            expected_project_id=PROJECT,
            region=CONTEXT["region"],
            auth_url=CONTEXT["auth_url"],
            intended_server_ids=SERVERS.copy(),
            known_seed_id=SERVERS[0],
            credential_file=str(self.credential),
            openstack_client=str(self.client),
        )
        self.write_json(self.profile_path, self.profile)
        self.fixture = {
            "responses": {
                "configuration show": {
                    "region_name": CONTEXT["region"],
                    "auth.auth_url": CONTEXT["auth_url"],
                    "auth.password": "provider-secret-marker",
                },
                "token issue": {"project_id": PROJECT.replace("-", ""), "id": "token-secret-marker"},
                "region list": [{"Region": CONTEXT["region"]}],
                "server show": {
                    sid: {
                        "id": sid,
                        "project_id": PROJECT.replace("-", ""),
                        "status": "SHELVED_OFFLOADED",
                        "flavor": f"ignored display text ({FLAVOR})",
                        "image": f"ignored image ({IMAGE})",
                        "addresses": {"ignored network text": ["192.0.2.5", "2001:db8::1"]},
                        "key_name": "keypair-secret-marker",
                        "adminPass": "provider-secret-marker",
                    }
                    for sid in SERVERS
                },
                "flavor show": {FLAVOR: {"id": FLAVOR, "vcpus": 2, "ram": 4096, "disk": 20}},
                "quota show": [
                    {"Resource": key, "Limit": count}
                    for key, count in (("cores", 20), ("ram", 65536), ("instances", 10))
                ],
                "network list": [{"ID": NETWORK, "Status": "ACTIVE", "Subnets": []}],
            }
        }
        self.write_fixture()
        self.inventory_path = self.state / "inventory" / "snapshot.json"
        self.spec_path = self.config / "pilot.json"
        self.spec = {
            "schema_version": 1,
            "context": CONTEXT.copy(),
            "vms": [
                {
                    "role": role,
                    "id": sid,
                    "active_seconds": 7200,
                    "rate": {
                        "su_per_hour": rate,
                        "source": "https://billing.example.test/rates",
                        "observed_at": datetime.now(UTC).isoformat(),
                        "verified": True,
                        "flavor_id": FLAVOR,
                    },
                }
                for role, sid, rate in zip(
                    ("manager", "origin", "worker"), SERVERS, (1.25, 2, 3), strict=True
                )
            ],
        }
        self.write_json(self.spec_path, self.spec)

    def write_json(self, path, value):
        path.write_text(json.dumps(value))
        path.chmod(0o600)

    def write_fixture(self):
        self.write_json(self.root / "fixtures.json", self.fixture)

    def invoke(self, args, expected):
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            code = ops.main(args)
        self.assertEqual(stderr.getvalue(), "")
        self.assertEqual(code, expected, stdout.getvalue())
        self.assertNotIn("secret-marker", stdout.getvalue())
        self.assertNotIn("synthetic-openrc-secret", stdout.getvalue())
        value = json.loads(stdout.getvalue())
        self.assertTrue(value["next_actions"])
        return value

    def capture(self, expected=0, save=True):
        args = ["inventory", "--profile", str(self.profile_path)]
        if save:
            args.extend(["--output", str(self.inventory_path)])
        return self.invoke(args, expected)

    def pilot(self, expected=0, output=None):
        args = ["plan", "--inventory", str(self.inventory_path), "--spec", str(self.spec_path)]
        if output:
            args.extend(["--output", str(output)])
        return self.invoke(args, expected)

    def calls(self):
        path = self.root / "calls.jsonl"
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def test_coordinator_osc_10_3_top_level_auth_url(self):
        self.fixture["column_contract"] = True
        self.fixture["responses"]["configuration show"] = {
            "region_name": CONTEXT["region"],
            "auth_url": CONTEXT["auth_url"],
            "application_credential_secret": "provider-secret-marker",
        }
        self.write_fixture()
        result = self.capture()
        self.assertEqual(result["data"]["context"], CONTEXT)
        self.assertTrue(result["data"]["complete"])

    def test_coordinator_network_status_requires_long(self):
        self.fixture["column_contract"] = True
        self.write_fixture()
        result = self.capture()
        self.assertEqual(result["data"]["networks"], [{"id": NETWORK, "status": "ACTIVE", "subnet_ids": []}])
        self.assertTrue(result["data"]["complete"])

    def test_coordinator_22_server_inventory_and_three_server_pilot(self):
        fleet = [f"22222222-2222-4222-8222-{index:012d}" for index in range(1, 23)]
        self.profile["intended_server_ids"] = fleet
        self.write_json(self.profile_path, self.profile)
        template = self.fixture["responses"]["server show"][SERVERS[0]]
        self.fixture["responses"]["server show"] = {sid: {**template, "id": sid} for sid in fleet}
        self.write_fixture()
        result = self.capture()
        self.assertEqual(len(result["data"]["servers"]), 22)
        self.assertTrue(result["data"]["complete"])
        self.assertEqual(sum(call[:2] == ["flavor", "show"] for call in self.calls()), 1)
        self.assertEqual(self.pilot()["data"]["total_estimated_su"], 12.5)
        self.spec["vms"].append({**self.spec["vms"][0], "id": fleet[3]})
        self.write_json(self.spec_path, self.spec)
        self.pilot(2)

    def test_full_inventory_uses_only_fixed_read_only_argv_and_minimizes_fields(self):
        with patch.dict(
            os.environ,
            {
                "OS_TOKEN": "inherited-secret-marker",
                "OS_CLOUD": "wrong-cloud",
                "BASH_ENV": "/should/not/source",
                "PYTHONPATH": "/should/not/load",
            },
        ):
            result = self.capture()
        self.assertTrue(result["data"]["complete"])
        self.assertTrue(result["data"]["context_verified"])
        self.assertEqual(result["data"]["context"], CONTEXT)
        self.assertEqual(result["data"]["intended_server_ids"], SERVERS)
        self.assertEqual(result["data"]["guest_readiness"], "not_assessed")
        self.assertEqual(json.loads(self.inventory_path.read_text()), result)
        self.assertEqual(stat.S_IMODE(self.inventory_path.stat().st_mode), 0o600)
        expected = [
            [
                "configuration",
                "show",
                "--mask",
                "-f",
                "json",
                "-c",
                "region_name",
                "-c",
                "auth_url",
                "-c",
                "auth.auth_url",
            ],
            ["token", "issue", "-f", "json", "-c", "project_id"],
            ["region", "list", "-f", "json", "-c", "Region"],
        ]
        for sid in SERVERS:
            expected.append(
                [
                    "server",
                    "show",
                    sid,
                    "-f",
                    "json",
                    "-c",
                    "id",
                    "-c",
                    "project_id",
                    "-c",
                    "status",
                    "-c",
                    "flavor",
                    "-c",
                    "addresses",
                    "-c",
                    "image",
                    "-c",
                    "key_name",
                ]
            )
            if sid == SERVERS[0]:
                expected.append(
                    [
                        "flavor",
                        "show",
                        FLAVOR,
                        "-f",
                        "json",
                        "-c",
                        "id",
                        "-c",
                        "vcpus",
                        "-c",
                        "ram",
                        "-c",
                        "disk",
                    ]
                )
        expected.extend(
            [
                ["quota", "show", "--compute", "-f", "json", "-c", "Resource", "-c", "Limit"],
                [
                    "network",
                    "list",
                    "--project",
                    PROJECT.replace("-", ""),
                    "--long",
                    "-f",
                    "json",
                    "-c",
                    "ID",
                    "-c",
                    "Status",
                    "-c",
                    "Subnets",
                ],
            ]
        )
        self.assertEqual(self.calls(), expected)

    def test_wrapper_creation_mode_repeat_and_conflict(self):
        wrapper = self.config / "js2"
        before = wrapper.stat()
        self.assertEqual(stat.S_IMODE(before.st_mode), 0o700)
        self.invoke(self.init_args, 0)
        self.assertEqual(before.st_mtime_ns, wrapper.stat().st_mtime_ns)
        wrapper.write_text("operator script")
        result = self.invoke(self.init_args, 2)
        self.assertEqual(result["errors"][0]["code"], "wrapper_conflict")
        self.assertEqual(wrapper.read_text(), "operator script")

    def test_wrapper_refuses_arbitrary_commands_and_extra_arguments(self):
        for action in ("server delete", "delete", "$(touch bad)"):
            with self.subTest(action=action):
                with self.assertRaises(ops.OpsError):
                    ops.cloud_query(self.profile, action, deadline=ops.time.monotonic() + 2)
        self.assertEqual(self.calls(), [])

    def test_missing_credential_is_saved_as_pending_without_cloud_calls(self):
        self.credential.unlink()
        result = self.capture(3)
        self.assertFalse(result["data"]["complete"])
        self.assertEqual(self.calls(), [])
        self.assertEqual(json.loads(self.inventory_path.read_text())["status"], "pending")

    def test_unsafe_prerequisites_never_start_provider(self):
        for key in ("credential_file", "wrapper_path", "openstack_client"):
            path = Path(self.profile[key])
            before = path.stat().st_mode
            with self.subTest(key=key):
                path.chmod(0o777)
                self.capture(3, save=False)
                path.chmod(stat.S_IMODE(before))
        self.assertEqual(self.calls(), [])

    def test_openrc_scope_overrides_are_rejected_and_sanitized(self):
        with self.credential.open("a") as stream:
            stream.write("export OS_TOKEN=provider-secret-marker\n")
        result = self.capture(1)
        self.assertFalse(result["data"]["complete"])
        self.assertEqual(self.calls(), [])

    def test_wrong_project_or_site_stops_before_server_discovery(self):
        mutations = (
            ("expected_project_id", IMAGE),
            ("region", "other-region"),
            ("auth_url", "https://other.example.test/v3"),
        )
        for key, value in mutations:
            profile = {**self.profile, key: value}
            self.write_json(self.profile_path, profile)
            with self.subTest(key=key):
                result = self.capture(3, save=False)
                self.assertFalse(result["data"]["context_verified"])
                self.assertFalse(result["data"]["complete"])
        self.assertTrue(
            all(
                call[:2] in (["configuration", "show"], ["token", "issue"], ["region", "list"])
                for call in self.calls()
            )
        )

    def test_initial_context_discovery_is_pending_without_guessing_scope(self):
        self.profile.update(
            expected_project_id=None, region=None, auth_url=None, intended_server_ids=[], known_seed_id=None
        )
        self.write_json(self.profile_path, self.profile)
        result = self.capture(3)
        self.assertEqual(result["data"]["context"], CONTEXT)
        self.assertEqual(result["data"]["servers"], [])
        self.assertEqual(len(self.calls()), 3)

    def test_missing_intended_set_is_pending(self):
        self.profile.update(intended_server_ids=[], known_seed_id=None)
        self.write_json(self.profile_path, self.profile)
        result = self.capture(3)
        self.assertTrue(result["data"]["context_verified"])
        self.assertFalse(result["data"]["complete"])

    def test_provider_failures_are_incomplete_and_never_expose_raw_output(self):
        for action in ("fail", "oversize", "malformed"):
            self.fixture["failure"] = {"key": "quota show", "action": action}
            self.write_fixture()
            with self.subTest(action=action):
                result = self.capture(1, save=False)
                self.assertFalse(result["data"]["complete"])
                self.assertTrue(result["errors"])

    def test_timeout_is_bounded_and_partial_snapshot_is_labeled(self):
        self.fixture["failure"] = {"key": "quota show", "action": "sleep"}
        self.write_fixture()
        with patch.object(ops, "CLOUD_SECONDS", 0.2):
            started = ops.time.monotonic()
            result = self.capture(1)
        self.assertLess(ops.time.monotonic() - started, 4)
        self.assertFalse(result["data"]["complete"])
        self.assertIn("probe_timeout", [error["code"] for error in result["errors"]])

    def test_wrong_or_duplicate_server_uuid_and_project_are_incomplete(self):
        original = copy.deepcopy(self.fixture)
        for key, value in (("id", SERVERS[1]), ("project_id", IMAGE)):
            self.fixture = copy.deepcopy(original)
            self.fixture["responses"]["server show"][SERVERS[0]][key] = value
            self.write_fixture()
            with self.subTest(key=key):
                result = self.capture(3, save=False)
                self.assertFalse(result["data"]["complete"])
                self.assertEqual(len(result["data"]["servers"]), 2)

    def test_missing_server_does_not_disappear_silently(self):
        del self.fixture["responses"]["server show"][SERVERS[0]]
        self.write_fixture()
        result = self.capture(1)
        self.assertFalse(result["data"]["complete"])
        self.assertEqual(len(result["data"]["servers"]), 2)

    def test_unsupported_provider_shapes_never_make_complete_snapshot(self):
        original = copy.deepcopy(self.fixture)
        for key, value in (
            ("region list", [{"Region": "other-region"}]),
            ("quota show", []),
            ("network list", [{"ID": NETWORK, "Status": "$(secret-marker)", "Subnets": []}]),
        ):
            self.fixture = copy.deepcopy(original)
            self.fixture["responses"][key] = value
            self.write_fixture()
            with self.subTest(key=key):
                result = self.capture(3 if key == "region list" else 1, save=False)
                self.assertFalse(result["data"]["complete"])

    def test_nonobject_context_is_a_sanitized_provider_error(self):
        for value in (None, [], "provider-secret-marker", 1):
            self.fixture["responses"]["configuration show"] = value
            self.write_fixture()
            with self.subTest(value=value):
                result = self.capture(1, save=False)
                self.assertFalse(result["data"]["complete"])
                self.assertIn({"name": "site", "state": "provider_schema"}, result["checks"])

    def test_duplicate_or_invalid_profile_uuid_is_rejected_before_provider(self):
        oversized = [f"22222222-2222-4222-8222-{index:012d}" for index in range(1, 66)]
        for ids in ([SERVERS[0]] * 3, ["$(secret-marker)"], oversized):
            self.write_json(self.profile_path, {**self.profile, "intended_server_ids": ids})
            with self.subTest(ids=ids):
                self.capture(2, save=False)
        self.assertEqual(self.calls(), [])

    def test_unknown_profile_fields_versions_and_nonfinite_json_rejected(self):
        for profile in (
            {**self.profile, "shell": "secret-marker"},
            {**self.profile, "schema_version": True},
            {**self.profile, "region": "$(secret-marker)"},
            {**self.profile, "auth_url": "https://user:secret-marker@example.test"},
        ):
            self.write_json(self.profile_path, profile)
            self.capture(2, save=False)
        for raw in (
            '{"schema_version":1,"schema_version":1}',
            '{"schema_version":NaN}',
            '{"schema_version":Infinity}',
        ):
            self.profile_path.write_text(raw)
            self.capture(2, save=False)
        self.assertEqual(self.calls(), [])

    def test_snapshot_output_never_overwrites_and_precedes_provider_calls(self):
        self.inventory_path.write_text("keep")
        before = self.inventory_path.stat()
        result = self.capture(2)
        self.assertEqual(result["errors"][0]["code"], "output_exists")
        self.assertEqual(self.calls(), [])
        self.assertEqual(self.inventory_path.read_text(), "keep")
        self.assertEqual(self.inventory_path.stat().st_mtime_ns, before.st_mtime_ns)
        self.inventory_path.unlink()
        self.inventory_path.symlink_to(self.root / "missing")
        self.capture(2)
        self.assertFalse((self.root / "missing").exists())

    def test_inventory_without_output_does_not_create_snapshot(self):
        self.capture(save=False)
        self.assertEqual(list((self.state / "inventory").iterdir()), [])

    def test_plan_happy_path_arithmetic_private_output_and_no_execution(self):
        self.capture()
        before = {
            path: (path.read_bytes(), path.stat().st_mtime_ns)
            for path in (self.spec_path, self.inventory_path)
        }
        calls = self.calls()
        output = self.state / "runs" / "plan.json"
        with patch.object(ops.subprocess, "Popen", side_effect=AssertionError("offline plan cannot execute")):
            result = self.pilot(output=output)
        self.assertTrue(result["data"]["validated"])
        self.assertEqual(result["data"]["total_estimated_su"], 12.5)
        self.assertEqual([vm["estimated_su"] for vm in result["data"]["vms"]], [2.5, 4.0, 6.0])
        self.assertEqual(result["data"]["resources_activated"], 0)
        self.assertEqual(stat.S_IMODE(output.stat().st_mode), 0o600)
        self.assertEqual(json.loads(output.read_text()), result)
        self.assertEqual(calls, self.calls())
        for path, expected in before.items():
            self.assertEqual((path.read_bytes(), path.stat().st_mtime_ns), expected)
        self.pilot(2, output=output)

    def test_invalid_roles_counts_ids_and_budgets_are_rejected(self):
        self.capture()
        variations = []
        for key, value in (
            ("role", "other"),
            ("role", "worker"),
            ("id", SERVERS[2]),
            ("active_seconds", 7201),
            ("active_seconds", 0),
            ("active_seconds", True),
            ("active_seconds", 1.5),
        ):
            spec = copy.deepcopy(self.spec)
            spec["vms"][0][key] = value
            variations.append(spec)
        variations += [{**self.spec, "vms": self.spec["vms"][:2]}, {**self.spec, "vms": self.spec["vms"] * 2}]
        for spec in variations:
            with self.subTest(spec=spec):
                self.write_json(self.spec_path, spec)
                self.pilot(2)

    def test_rate_numbers_and_provenance_are_strict(self):
        self.capture()
        for key, values in (
            ("su_per_hour", (0, -1, True, "2", float("nan"), float("inf"), 1e308)),
            ("source", ("", "unverified", "https://user:secret-marker@example.test")),
            ("verified", (1, "true")),
            ("observed_at", ("unknown", "2099-01-01T00:00:00Z")),
        ):
            for value in values:
                spec = copy.deepcopy(self.spec)
                spec["vms"][0]["rate"][key] = value
                self.write_json(self.spec_path, spec)
                with self.subTest(key=key, value=value):
                    self.pilot(2)

    def test_missing_or_unverified_rate_and_flavor_mismatch_are_pending(self):
        self.capture()
        for rate in (
            None,
            {**self.spec["vms"][0]["rate"], "verified": False},
            {**self.spec["vms"][0]["rate"], "flavor_id": "different-flavor"},
        ):
            spec = copy.deepcopy(self.spec)
            spec["vms"][0]["rate"] = rate
            self.write_json(self.spec_path, spec)
            result = self.pilot(3)
            self.assertFalse(result["data"]["validated"])
            self.assertIsNone(result["data"]["total_estimated_su"])
            self.assertEqual([vm["role"] for vm in result["data"]["vms"]], ["origin", "worker"])

    def test_stale_future_incomplete_and_wrong_context_snapshots_are_pending(self):
        original = self.capture()
        for key, value in (
            ("observed_at", (datetime.now(UTC) - timedelta(hours=25)).isoformat()),
            ("observed_at", (datetime.now(UTC) + timedelta(hours=1)).isoformat()),
            ("complete", False),
        ):
            saved = copy.deepcopy(original)
            saved["data"][key] = value
            self.write_json(self.inventory_path, saved)
            result = self.pilot(3)
            self.assertIsNone(result["data"]["total_estimated_su"])
            self.assertEqual(result["data"]["vms"], [])
        self.write_json(self.inventory_path, original)
        self.spec["context"]["region"] = "other-region"
        self.write_json(self.spec_path, self.spec)
        self.pilot(3)

    def test_invalid_and_out_of_scope_snapshot_facts_rejected(self):
        original = self.capture()
        mutations = [
            lambda d: d["servers"].pop(),
            lambda d: d["servers"].append(d["servers"][0]),
            lambda d: d["servers"][0].update(project_id=IMAGE),
            lambda d: d["servers"][0].update(id=IMAGE),
            lambda d: d["servers"][0].update(status="$(secret-marker)"),
            lambda d: d["servers"][0]["flavor"].update(vcpus=True),
            lambda d: d.update(complete=1),
            lambda d: d.update(context_verified=False),
            lambda d: d.update(quota=None),
            lambda d: d.update(secret="secret-marker"),
        ]
        for mutate in mutations:
            saved = copy.deepcopy(original)
            mutate(saved["data"])
            self.write_json(self.inventory_path, saved)
            self.pilot(2)

    def test_unallowlisted_spec_uuid_and_incompatible_vm_state_are_pending(self):
        original = self.capture()
        self.spec["vms"][0]["id"] = IMAGE
        self.write_json(self.spec_path, self.spec)
        self.pilot(3)
        self.spec["vms"][0]["id"] = SERVERS[0]
        self.write_json(self.spec_path, self.spec)
        original["data"]["servers"][0]["status"] = "ERROR"
        self.write_json(self.inventory_path, original)
        self.pilot(3)

    def test_unknown_nested_spec_fields_are_invalid(self):
        self.capture()
        for part in ("top", "context", "vm", "rate"):
            spec = copy.deepcopy(self.spec)
            target = {
                "top": spec,
                "context": spec["context"],
                "vm": spec["vms"][0],
                "rate": spec["vms"][0]["rate"],
            }[part]
            target["unexpected"] = "secret-marker"
            self.write_json(self.spec_path, spec)
            self.pilot(2)

    def test_unsafe_and_missing_offline_inputs_are_json_errors(self):
        self.capture()
        self.spec_path.chmod(0o644)
        self.pilot(2)
        self.spec_path.unlink()
        self.pilot(3)
        self.spec_path.symlink_to(self.inventory_path)
        self.pilot(2)

    def doctor(
        self,
        expected,
        *,
        ssh_code=0,
        systemd=(0, b"running\n"),
        linger=(0, b"yes\n"),
        missing=(),
        bash_interpreter=True,
    ):
        def which(name):
            if name == "/bin/bash":
                return name if bash_interpreter and "bash" not in missing else None
            return None if name in missing else f"/fake/{name}"

        def probe(argv, **kwargs):
            if argv[0].endswith("ssh-add"):
                self.assertEqual(argv[1:], ["-l"])
                return ssh_code, b"ssh-key-secret-marker"
            if argv[0].endswith("systemctl"):
                self.assertEqual(argv[1:], ["--user", "is-system-running"])
                return systemd
            self.assertEqual(argv[1:], ["show-user", str(os.getuid()), "--property=Linger", "--value"])
            return linger

        with (
            patch.object(ops.shutil, "which", side_effect=which),
            patch.object(ops, "run_bounded", side_effect=probe),
        ):
            with patch.object(ops, "cloud_query", side_effect=AssertionError("doctor is local")):
                return self.invoke(
                    ["doctor", "--profile", str(self.profile_path), "--state-root", str(self.state)], expected
                )

    def test_doctor_happy_path_checks_metadata_without_reading_credentials(self):
        original = ops.open_private_at

        @contextlib.contextmanager
        def guarded(parent, name, **kwargs):
            self.assertNotEqual(name, self.credential.name, "doctor must not open the credential")
            with original(parent, name, **kwargs) as fd:
                yield fd

        with patch.object(ops, "open_private_at", side_effect=guarded):
            result = self.doctor(0)
        self.assertEqual(result["data"]["cloud_readiness"], "not_assessed")
        self.assertEqual(self.calls(), [])

    def test_doctor_missing_tools_agent_and_failed_systemd_probes(self):
        for ssh_code, state in ((1, "no_identities"), (2, "unavailable"), (4, "probe_failed")):
            result = self.doctor(3, ssh_code=ssh_code)
            self.assertIn({"name": "ssh_agent", "state": state}, result["checks"])
            self.assertIn("direct key-file", " ".join(result["next_actions"]))
        self.doctor(3, systemd=(1, b"provider-secret-marker"))
        self.doctor(3, linger=(0, b"no\n"))
        result = self.doctor(3, missing=("ssh", "ssh-add", "systemctl", "loginctl", "bash"))
        self.assertIn({"name": "ssh", "state": "missing"}, result["checks"])

    def test_doctor_missing_insecure_empty_and_symlink_credentials_are_pending(self):
        self.credential.chmod(0o644)
        result = self.doctor(3)
        self.assertIn({"name": "credential_file", "state": "unsafe_or_unavailable"}, result["checks"])
        self.credential.chmod(0o600)
        self.credential.write_text("")
        self.doctor(3)
        self.credential.unlink()
        self.doctor(3)
        self.credential.symlink_to(self.root / "missing")
        self.doctor(3)

    def test_doctor_timeout_is_pending_without_raw_diagnostics(self):
        with patch.object(ops.shutil, "which", return_value="/fake/tool"):
            error = ops.OpsError("probe_timeout", "safe", "safe", 1)
            with patch.object(ops, "run_bounded", side_effect=error):
                result = self.invoke(
                    ["doctor", "--profile", str(self.profile_path), "--state-root", str(self.state)], 3
                )
        self.assertIn({"name": "ssh_agent", "state": "probe_timeout"}, result["checks"])

    def test_review_doctor_rejects_path_only_bash(self):
        # PATH has /fake/bash, but the interpreter used by cloud_query is absent.
        result = self.doctor(3, bash_interpreter=False)
        self.assertIn({"name": "bash", "state": "missing"}, result["checks"])
        self.assertIn("/bin/bash", " ".join(result["next_actions"]))

    def test_review_cleanup_wait_timeout_is_json_error_and_closes_streams(self):
        original_popen = ops.subprocess.Popen
        children = []

        def popen(*args, **kwargs):
            child = original_popen(*args, **kwargs)
            original_wait = child.wait
            children.append((child, original_wait))
            waited = False

            def wait(timeout):
                nonlocal waited
                if not waited:
                    waited = True
                    return original_wait(timeout=timeout)
                # Deterministically simulate delayed cleanup after a successful
                # probe. The real fixture child is already reaped; none hangs.
                self.assertEqual(timeout, 1)
                raise subprocess.TimeoutExpired(["cleanup-secret-marker"], timeout)

            child.wait = wait
            return child

        try:
            with patch.object(ops.subprocess, "Popen", side_effect=popen):
                result = self.capture(1)
            self.assertFalse(result["data"]["complete"])
            self.assertTrue(result["errors"])
            self.assertTrue(all(error["code"] == "probe_cleanup_timeout" for error in result["errors"]))
            self.assertTrue(all(child.stdout.closed and child.stderr.closed for child, _ in children))
        finally:
            for child, original_wait in children:
                child.wait = original_wait
                child.stdout.close()
                child.stderr.close()
                original_wait(timeout=1)

    def test_review_flavor_name_id_mismatch_stays_incomplete(self):
        for server in self.fixture["responses"]["server show"].values():
            server["flavor"] = "m3.small"
        flavor = self.fixture["responses"]["flavor show"][FLAVOR]
        self.fixture["responses"]["flavor show"] = {"m3.small": flavor}
        self.write_fixture()
        result = self.capture(1)
        self.assertFalse(result["data"]["complete"])
        self.assertEqual(result["data"]["servers"], [])
        self.assertIn({"name": "flavor_1", "state": "provider_schema"}, result["checks"])
        self.assertEqual(sum(call[:3] == ["flavor", "show", "m3.small"] for call in self.calls()), 1)

    def test_bounded_runner_caps_stderr_and_kills_pipe_holding_descendants(self):
        with self.assertRaises(ops.OpsError) as caught:
            ops.run_bounded([sys.executable, "-c", "import sys; sys.stderr.write('x' * 300000)"], timeout=2)
        self.assertEqual(caught.exception.code, "probe_output_limit")
        started = ops.time.monotonic()
        with self.assertRaises(ops.OpsError) as caught:
            ops.run_bounded(
                [sys.executable, "-c", "import os,time; pid=os.fork(); time.sleep(10) if pid == 0 else None"],
                timeout=0.1,
            )
        self.assertEqual(caught.exception.code, "probe_timeout")
        self.assertLess(ops.time.monotonic() - started, 2)

    def test_conflicting_safe_auth_url_columns_are_rejected(self):
        self.fixture["responses"]["configuration show"]["auth_url"] = "https://other.example.test/v3"
        self.write_fixture()
        result = self.capture(1)
        self.assertFalse(result["data"]["complete"])
        self.assertIsNone(result["data"]["context"])

    def test_failed_shared_flavor_is_not_retried_or_marked_complete(self):
        self.fixture["failure"] = {"key": "flavor show", "action": "fail"}
        self.write_fixture()
        result = self.capture(1)
        self.assertEqual(result["data"]["servers"], [])
        self.assertFalse(result["data"]["complete"])
        self.assertEqual(sum(call[:2] == ["flavor", "show"] for call in self.calls()), 1)

    def test_expired_overall_inventory_budget_starts_no_process(self):
        with patch.object(ops, "INVENTORY_SECONDS", 0):
            with patch.object(ops.subprocess, "Popen", side_effect=AssertionError("expired budget")):
                result = self.capture(1)
        self.assertFalse(result["data"]["complete"])
        self.assertEqual(self.calls(), [])

    def test_output_race_refuses_replacement_and_unsafe_parent(self):
        output = self.state / "runs" / "plan.json"
        ops.check_output(str(output))
        output.write_text("concurrent result")
        with self.assertRaises(ops.OpsError) as caught:
            ops.save_output(str(output), {"status": "ok"})
        self.assertEqual(caught.exception.code, "output_exists")
        self.assertEqual(output.read_text(), "concurrent result")
        output.parent.chmod(0o755)
        with self.assertRaises(ops.OpsError):
            ops.save_output(str(output.parent / "new.json"), {})
        self.assertFalse((output.parent / "new.json").exists())

    def test_public_examples_are_valid_synthetic_inputs_with_pending_rates(self):
        directory = SCRIPT.parents[1] / "docs/jetstream2"
        self.write_json(self.profile_path, json.loads((directory / "profile.example.json").read_text()))
        self.write_json(self.spec_path, json.loads((directory / "pilot.example.json").read_text()))
        profile = ops.load_profile(str(self.profile_path))
        spec = ops.validate_spec(str(self.spec_path))
        self.assertEqual(len(profile["intended_server_ids"]), 3)
        self.assertEqual({vm["role"] for vm in spec["vms"]}, {"manager", "origin", "worker"})
        self.assertTrue(all(vm["rate"] is None for vm in spec["vms"]))

    def test_extreme_offset_datetime_is_a_json_error(self):
        self.capture()
        self.spec["vms"][0]["rate"]["observed_at"] = "0001-01-01T00:00:00+23:00"
        self.write_json(self.spec_path, self.spec)
        self.pilot(2)


if __name__ == "__main__":
    unittest.main()
