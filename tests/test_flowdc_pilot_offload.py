"""Actual provider subprocess with an isolated, credential-free fake SDK venv."""

import copy
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bin"))
import flowdc_ops as ops
import flowdc_pilot_cli as cli
import test_flowdc_pilot_lifecycle as lifecycle
from flowdc_pilot_journal import register
from flowdc_pilot_provider import Provider
from test_flowdc_pilot_lifecycle import PROJECT, VM_IDS, access, spec

FAKE_SDK = r"""
import json
import os
import time
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[3]

def fixture():
    return json.loads((ROOT / "fixture.json").read_text())

def record(value):
    with (ROOT / "requests.jsonl").open("a") as stream:
        stream.write(json.dumps(value) + "\n")

class ApplicationCredential:
    def __init__(self, **kwargs):
        assert kwargs == {
            "auth_url": "https://cloud.example.test/v3",
            "application_credential_id": "test-credential",
            "application_credential_secret": "secret-canary-offload",
        }

class Session:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
    def get_project_id(self):
        return fixture()["auth_project"]
    def request(self, *args, **kwargs):
        assert kwargs == {"allow_reauth": False, "connect_retries": 0,
                          "status_code_retries": 0, "redirect": False, "log": False}

class RequestsSession:
    trust_env = True

class CloudRegion:
    def __init__(self, **kwargs):
        assert kwargs["cache_auth"] is False
        assert kwargs["config"]["interface"] == "public"
        assert kwargs["config"]["compute_api_version"] == "2.1"
        assert kwargs["config"]["region_name"] == "test-region"
        assert kwargs["config"]["connect_retries"] == 0
        assert kwargs["config"]["status_code_retries"] == 0
        self.kwargs = kwargs

class Proxy:
    def shelve_offload_server(self, resource):
        pass

class Connection:
    def __init__(self, *, config):
        self.compute = self
        self.session = config.kwargs["session"]
        assert self.session.kwargs["session"].trust_env is False
        assert self.session.kwargs["verify"] is True
        assert self.session.kwargs["connect_retries"] == 0
        assert self.session.kwargs["redirect"] is False
    def get_server(self, resource):
        return SimpleNamespace(**fixture()["server"])
    def shelve_offload_server(self, resource):
        self.session.request("/synthetic", "POST")
        record({"operation": "offload", "id": resource})
        value = fixture()
        if value.get("lost_ack"):
            os._exit(7)
        if value.get("delay"):
            time.sleep(value["delay"])
        if value.get("error"):
            exc = RuntimeError("secret-canary-offload https://private.example/?token=canary")
            exc.http_status = value["error"]
            raise exc
"""


class OffloadTests(unittest.TestCase):
    def setUp(self):
        mask = os.umask(0o077)
        self.addCleanup(os.umask, mask)
        temp = tempfile.TemporaryDirectory(prefix="flowdc-sdk-test-")
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.runtime = self.root / "venv"
        # Explicit trusted test interpreter; never substitutes a production
        # runtime or installs packages. Python's root owner may be namespaced.
        interpreter = Path("/usr/bin/python3.12")
        info = interpreter.stat()
        self.assertIn(info.st_uid, (0, os.stat("/").st_uid))
        self.assertFalse(info.st_mode & 0o022)
        subprocess.run(
            [str(interpreter), "-I", "-B", "-m", "venv", "--without-pip", str(self.runtime)],
            check=True,
            capture_output=True,
            timeout=10,
        )
        packages = self.runtime / "lib/python3.12/site-packages"
        modules = {
            "fake_cloud.py": FAKE_SDK,
            "requests/__init__.py": "from fake_cloud import RequestsSession as Session\n",
            "keystoneauth1/__init__.py": "",
            "keystoneauth1/session.py": "from fake_cloud import Session\n",
            "keystoneauth1/identity/__init__.py": "",
            "keystoneauth1/identity/v3.py": "from fake_cloud import ApplicationCredential\n",
            "openstack/__init__.py": "",
            "openstack/connection.py": "from fake_cloud import Connection\n",
            "openstack/config/__init__.py": "",
            "openstack/config/cloud_region.py": "from fake_cloud import CloudRegion\n",
            "openstack/compute/__init__.py": "",
            "openstack/compute/v2/__init__.py": "",
            "openstack/compute/v2/_proxy.py": "from fake_cloud import Proxy\n",
        }
        for name, source in modules.items():
            path = packages / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(source)
        self.client = self.runtime / "bin/openstack"
        # Emulate the installed client's no-op for an already shelved server.
        self.client.write_text(f"#!{self.runtime}/bin/python\nimport sys\nsys.exit(0)\n")
        self.client.chmod(0o700)
        self.credential = self.root / "credential.sh"
        self.credential.write_text(
            "export OS_AUTH_TYPE=v3applicationcredential\n"
            "export OS_APPLICATION_CREDENTIAL_ID=test-credential\n"
            "export OS_APPLICATION_CREDENTIAL_SECRET=secret-canary-offload\n"
            "export OS_AUTH_URL=https://cloud.example.test/v3\n"
            "export OS_REGION_NAME=test-region\n"
        )
        self.profile = {
            "openstack_client": str(self.client),
            "credential_file": str(self.credential),
            "expected_project_id": PROJECT,
            "region": "test-region",
            "auth_url": "https://cloud.example.test/v3",
            "intended_server_ids": VM_IDS,
        }
        profile = self.root / "profile.json"
        profile.write_text("{}")
        self.journal = register(profile, self.root / "state", spec(), access())
        self.record = self.journal.read()
        self.provider = Provider(self.profile)
        self.fixture = {
            "auth_project": PROJECT,
            "server": {"id": VM_IDS[0], "project_id": PROJECT, "status": "SHELVED"},
        }
        self.save_fixture()

    def save_fixture(self):
        (self.runtime / "fixture.json").write_text(json.dumps(self.fixture))

    def requests(self):
        path = self.runtime / "requests.jsonl"
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def query(self, profile, action, *args, **kwargs):
        if action == "context":
            return {"region_name": "test-region", "auth_url": "https://cloud.example.test/v3"}
        if action == "project":
            return {"project_id": PROJECT}
        if action == "server":
            return {"id": VM_IDS[0], "project_id": PROJECT, "status": "SHELVED"}
        raise AssertionError(action)

    def dispatch(self):
        with patch.object(ops, "cloud_query", side_effect=self.query):
            self.provider.lifecycle(self.record, VM_IDS[0], "offload")

    def test_verified_shelved_vm_dispatches_one_sdk_offload(self):
        before = copy.deepcopy(self.journal.read())
        self.dispatch()
        self.assertEqual(self.requests(), [{"operation": "offload", "id": VM_IDS[0]}])
        self.assertEqual(self.journal.read(), before)

    def test_wrong_registered_context_or_vm_never_starts_child(self):
        for field, value in (
            ("region", "wrong"),
            ("expected_project_id", VM_IDS[1]),
            ("intended_server_ids", VM_IDS[1:]),
        ):
            with self.subTest(field=field), patch.dict(self.profile, {field: value}):
                with patch.object(ops.subprocess, "Popen") as spawn, self.assertRaises(ops.OpsError):
                    self.dispatch()
                spawn.assert_not_called()
        with patch.object(ops, "cloud_query", side_effect=self.query), self.assertRaises(ops.OpsError):
            self.provider.lifecycle(self.record, PROJECT, "offload")
        self.assertEqual(self.requests(), [])

    def test_child_revalidates_authenticated_project_identity_and_state(self):
        original = copy.deepcopy(self.fixture)
        variants = [
            {"auth_project": VM_IDS[1]},
            {"server": dict(original["server"], id=VM_IDS[1])},
            {"server": dict(original["server"], project_id=VM_IDS[1])},
            {"server": dict(original["server"], status="ACTIVE")},
            {"server": dict(original["server"], status="SHELVED_OFFLOADED")},
        ]
        for variant in variants:
            with self.subTest(variant=variant):
                self.fixture = dict(original, **variant)
                self.save_fixture()
                with self.assertRaises(ops.OpsError) as caught:
                    self.dispatch()
                self.assertIs(caught.exception.diagnostic["dispatch_possible"], False)
                self.assertEqual(self.requests(), [])

    def test_openrc_region_auth_url_and_overrides_are_refused(self):
        original = self.credential.read_text()
        for extra in (
            "OS_REGION_NAME=wrong",
            "OS_AUTH_URL=http://cloud.example.test/v3",
            "OS_CLOUD=secret-canary-offload",
            "OS_ENDPOINT_OVERRIDE=https://private.example",
        ):
            with self.subTest(extra=extra):
                self.credential.write_text(original + "\nexport " + extra + "\n")
                with self.assertRaises(ops.OpsError) as caught:
                    self.dispatch()
                self.assertNotIn("secret-canary", str(caught.exception))
                self.assertEqual(self.requests(), [])

    def test_runtime_check_is_offline_and_preserves_venv_symlink_semantics(self):
        self.assertTrue((self.runtime / "bin/python").is_symlink())
        self.credential.unlink()
        with patch.object(ops, "private_file", side_effect=AssertionError("credential forbidden")):
            self.provider.runtime_check()
        self.assertEqual(self.requests(), [])

    def test_runtime_check_cli_never_requires_journal_or_claims_cloud_readiness(self):
        with (
            patch.object(cli.ops, "load_profile", return_value=self.profile),
            patch.object(cli, "Journal", side_effect=AssertionError("journal forbidden")),
        ):
            args = ops.parser().parse_args(
                ["pilot", "runtime-check", "--profile", str(self.root / "profile.json")]
            )
            value, code = cli.run(args)
        self.assertEqual(code, 0)
        self.assertEqual(value["data"]["cloud_readiness"], "not_assessed")
        self.assertEqual(self.requests(), [])

    def test_hostile_parent_python_and_cloud_configuration_are_not_loaded(self):
        malicious = self.root / "malicious"
        malicious.mkdir()
        sentinel = self.root / "executed"
        (malicious / "sitecustomize.py").write_text(f"open({str(sentinel)!r}, 'w').close()\n")
        (malicious / "clouds.yaml").write_text("secret-canary-offload")
        with patch.dict(
            os.environ,
            {
                "PYTHONPATH": str(malicious),
                "PYTHONHOME": str(malicious),
                "OS_CLIENT_CONFIG_FILE": str(malicious / "clouds.yaml"),
                "OS_CLOUD": "untrusted",
                "HTTPS_PROXY": "https://private.example",
            },
        ):
            self.dispatch()
        self.assertFalse(sentinel.exists())
        self.assertEqual(len(self.requests()), 1)

    def test_ack_and_lost_response_retain_obligation_until_fresh_offloaded_observation(self):
        for lost in (False, True):
            with self.subTest(lost=lost):
                case = lifecycle.LifecycleTests()
                case.setUp()
                try:
                    case.start()
                    case.tick(5)
                    lifecycle.request(case.journal, "stop", clock=case.clock)
                    self.fixture["lost_ack"] = lost
                    self.save_fixture()
                    case.supervisor.provider = self.provider
                    state = ["SHELVED"]

                    def fresh_query(profile, action, resource=None, *, state=state, **kwargs):
                        if action == "server":
                            return {
                                "id": resource,
                                "project_id": PROJECT,
                                "status": state[0] if resource == VM_IDS[0] else "SHELVED_OFFLOADED",
                            }
                        return self.query(profile, action, resource, **kwargs)

                    with patch.object(ops, "cloud_query", side_effect=fresh_query):
                        case.tick()
                        current = case.journal.read()["vms"][VM_IDS[0]]
                        self.assertTrue(current["account"]["obligation"])
                        self.assertEqual(current["cleanup_intent"]["action"], "offload")
                        self.assertEqual(current["observed"]["state"], "SHELVED")
                        # No task metadata is present. A later fresh observation
                        # alone supplies completion evidence after either result.
                        state[0] = "SHELVED_OFFLOADED"
                        case.clock.advance(12)
                        case.tick(3)
                    self.assertFalse(case.journal.read()["vms"][VM_IDS[0]]["account"]["obligation"])
                finally:
                    case.doCleanups()

    def test_unsafe_runtime_and_unsupported_sdk_refuse_before_dispatch(self):
        cfg = self.runtime / "pyvenv.cfg"
        original = cfg.read_text()
        for change in (
            "writable_cfg",
            "system_packages",
            "wrong_home",
            "client_shebang",
            "writable_packages",
        ):
            with self.subTest(change=change):
                cfg.write_text(original)
                cfg.chmod(0o600)
                self.client.write_text(f"#!{self.runtime}/bin/python\n")
                packages = self.runtime / "lib/python3.12/site-packages"
                packages.chmod(0o700)
                if change == "writable_cfg":
                    cfg.chmod(0o660)
                elif change == "system_packages":
                    cfg.write_text(
                        original.replace(
                            "include-system-site-packages = false", "include-system-site-packages = true"
                        )
                    )
                elif change == "wrong_home":
                    cfg.write_text(original.replace("home = /usr/bin", "home = /tmp"))
                elif change == "client_shebang":
                    self.client.write_text("#!/usr/bin/env python\n")
                else:
                    packages.chmod(0o770)
                with (
                    patch.object(ops.subprocess, "Popen") as spawn,
                    self.assertRaises(ops.OpsError) as caught,
                ):
                    self.dispatch()
                self.assertEqual(caught.exception.code, "offload_runtime_unsupported")
                spawn.assert_not_called()
        packages.chmod(0o700)
        (packages / "openstack/compute/v2/_proxy.py").write_text("class Proxy: pass\n")
        with self.assertRaises(ops.OpsError) as caught:
            self.provider.runtime_check()
        self.assertEqual(caught.exception.code, "offload_runtime_unsupported")
        self.assertEqual(self.requests(), [])

    def test_group_writable_resolved_interpreter_is_refused_without_execution(self):
        base = self.root / "base"
        base.mkdir()
        executable = base / "python3.12"
        shutil.copyfile("/usr/bin/python3.12", executable)
        executable.chmod(0o775)
        python = self.runtime / "bin/python"
        python.unlink()
        python.symlink_to(executable)
        cfg = self.runtime / "pyvenv.cfg"
        cfg.write_text(cfg.read_text().replace("home = /usr/bin", f"home = {base}"))
        with patch.object(ops.subprocess, "Popen") as spawn, self.assertRaises(ops.OpsError) as caught:
            self.dispatch()
        self.assertEqual(caught.exception.code, "offload_runtime_unsupported")
        self.assertIs(caught.exception.diagnostic["dispatch_possible"], False)
        spawn.assert_not_called()

    def test_permission_conflict_transient_unknown_and_lost_ack_are_sanitized(self):
        for error, category in (
            (403, "permission"),
            (409, "conflict"),
            (503, "transient"),
            (418, "unknown"),
            ("HTTP 409 secret-canary", "unknown"),
            (None, "unknown"),
        ):
            with self.subTest(error=error):
                path = self.runtime / "requests.jsonl"
                path.unlink(missing_ok=True)
                self.fixture.update(error=error, lost_ack=error is None)
                self.save_fixture()
                with self.assertRaises(ops.OpsError) as caught:
                    self.dispatch()
                detail = caught.exception.diagnostic
                self.assertEqual(
                    (detail["phase"], detail["action"], detail["role"]), ("cleanup", "offload", "manager")
                )
                self.assertEqual(detail["category"], category)
                self.assertIs(detail["dispatch_possible"], True)
                self.assertNotIn("canary", json.dumps(detail))
                self.assertNotIn("https", json.dumps(detail))
                self.assertEqual(len(self.requests()), 1)

    def test_timeout_reaps_sdk_child_and_credentials_never_enter_argv(self):
        self.fixture["delay"] = 10
        self.save_fixture()
        children = []
        original = ops.subprocess.Popen

        def spawn(argv, **kwargs):
            self.assertNotIn("secret-canary-offload", repr(argv))
            child = original(argv, **kwargs)
            children.append(child)
            return child

        with (
            patch.object(ops, "CLOUD_SECONDS", 0.5),
            patch.object(ops.subprocess, "Popen", side_effect=spawn),
        ):
            with self.assertRaises(ops.OpsError) as caught:
                self.dispatch()
        self.assertEqual(caught.exception.code, "probe_timeout")
        self.assertEqual(len(self.requests()), 1)
        self.assertEqual(len(children), 1)
        self.assertIsNotNone(children[0].returncode)
        self.assertTrue(children[0].stdout.closed)
        self.assertTrue(children[0].stderr.closed)

    def test_malformed_sdk_success_is_not_acknowledged(self):
        for value in (
            {"code": "offload_acknowledged", "category": "ok", "dispatch_possible": False},
            {"code": "offload_runtime_ready", "category": "ok", "dispatch_possible": False},
            {"code": "secret-canary-offload", "category": "ok", "dispatch_possible": True},
        ):
            with self.subTest(value=value), self.assertRaises(ops.OpsError) as caught:
                self.provider.sdk_result(0, json.dumps(value).encode())
            self.assertEqual(caught.exception.code, "provider_schema")


if __name__ == "__main__":
    unittest.main()
