"""Native-login lifecycle uses fake native records, never workstation credentials."""

import copy
import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from test_workflow import workflow

# isort: split
import claude_native_auth as auth


class NativeAuthTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "login"
        self.now = time.time()
        self.credentials = {
            "claudeAiOauth": {
                "accessToken": "fixture-access-never-real",
                "refreshToken": "fixture-refresh-never-real",
                "expiresAt": (self.now + 7200) * 1000,
                "scopes": ["user:profile", "user:inference"],
                "subscriptionType": "max",
            }
        }
        self.config = {
            "oauthAccount": {
                "accountUuid": "11111111-1111-4111-8111-111111111111",
                "organizationUuid": "22222222-2222-4222-8222-222222222222",
                "hasExtraUsageEnabled": False,
            },
            "hasCompletedOnboarding": True,
        }

    def test_access_only_projection_omits_refresh_and_persistent_customization(self):
        self.config["history"] = "private history"
        snapshot, identity = auth.native_records(self.credentials, self.config, 900, now=self.now)
        self.assertNotIn("refreshToken", snapshot["claudeAiOauth"])
        self.assertEqual(snapshot["claudeAiOauth"]["accessToken"], "fixture-access-never-real")
        self.assertNotIn("history", identity)
        self.assertEqual(identity["oauthAccount"]["accountUuid"], self.config["oauthAccount"]["accountUuid"])

    def test_native_reader_requires_genuine_max_not_status_fallback(self):
        for kind in (None, "pro", "team", "enterprise", "console", "unknown"):
            data = copy.deepcopy(self.credentials)
            data["claudeAiOauth"]["subscriptionType"] = kind
            config = {**self.config, "subscriptionType": "max", "hasAvailableSubscription": True}
            with self.subTest(kind=kind), self.assertRaises(workflow.WorkflowError):
                auth.native_records(data, config, 900, now=self.now)
        for key in ("accessToken", "subscriptionType", "expiresAt", "scopes"):
            data = copy.deepcopy(self.credentials)
            del data["claudeAiOauth"][key]
            with self.subTest(key=key), self.assertRaises(workflow.WorkflowError):
                auth.native_records(data, self.config, 900, now=self.now)

    def test_full_timeout_refresh_margin_clock_allowance_is_strict(self):
        for timeout in (300, 900):
            for seconds in (-1, 0, timeout + 359, timeout + 360):
                data = copy.deepcopy(self.credentials)
                data["claudeAiOauth"]["expiresAt"] = (self.now + seconds) * 1000
                with self.subTest(seconds=seconds), self.assertRaises(workflow.WorkflowError):
                    auth.native_records(data, self.config, timeout, now=self.now)
            data["claudeAiOauth"]["expiresAt"] = (self.now + timeout + 361) * 1000
            auth.native_records(data, self.config, timeout, now=self.now)
        for expiry in (None, True, float("nan"), float("inf"), self.now + 7200):
            data["claudeAiOauth"]["expiresAt"] = expiry
            with self.subTest(expiry=expiry), self.assertRaises(workflow.WorkflowError):
                auth.native_records(data, self.config, 900, now=self.now)

    def test_alternate_auth_account_and_paid_usage_are_rejected_without_secret_output(self):
        for key in ("primaryApiKey", "apiKeyHelper", "user_oauth", "profiles", "env"):
            config = {**self.config, key: "SECRET-SENTINEL"}
            with self.assertRaises(workflow.WorkflowError) as error:
                auth.native_records(self.credentials, config, 900, now=self.now)
            self.assertNotIn("SECRET-SENTINEL", str(error.exception))
        for field, value in (
            ("accountUuid", None),
            ("organizationUuid", "bad"),
            ("hasExtraUsageEnabled", True),
        ):
            config = copy.deepcopy(self.config)
            config["oauthAccount"][field] = value
            with self.assertRaises(workflow.WorkflowError):
                auth.native_records(self.credentials, config, 900, now=self.now)

    def test_setup_requires_real_terminal_and_assertion_before_writes(self):
        with patch.object(auth.sys.stdin, "isatty", return_value=False):
            with self.assertRaises(workflow.WorkflowError):
                auth.setup(None, root=self.root, paid_usage_disabled=True)
        self.assertFalse(self.root.exists())
        with self.assertRaises(workflow.WorkflowError):
            auth.setup(None, root=self.root)

    def test_immutable_authentication_payload_rejects_legacy_and_malformed(self):
        binding = auth.new_binding()
        self.assertEqual(auth.validate_binding(binding), binding)
        for field in binding:
            bad = dict(binding)
            bad.pop(field)
            with self.subTest(field=field), self.assertRaises(workflow.WorkflowError):
                auth.validate_binding(bad)
        for value in (
            {},
            None,
            {**binding, "mode": "subscription-token"},
            {**binding, "generation_id": "secret"},
        ):
            with self.assertRaises(workflow.WorkflowError):
                auth.validate_binding(value)
        self.assertNotIn("fixture", json.dumps(binding))

    def test_private_store_refuses_symlinks_hardlinks_fifo_and_concurrent_operations(self):
        with auth.store(self.root, create=True) as store:
            store.write("record.json", {"ok": True})
            self.assertEqual(store.read("record.json"), {"ok": True})
            with self.assertRaises(workflow.WorkflowError):
                with auth.store(self.root):
                    pass
            os.link(self.root / "record.json", self.root / "other")
            with self.assertRaises(workflow.WorkflowError):
                store.read("record.json")
            (self.root / "other").unlink()
            (self.root / "record.json").unlink()
            os.mkfifo(self.root / "record.json", 0o600)
            with self.assertRaises(workflow.WorkflowError):
                store.read("record.json")
            (self.root / "record.json").unlink()
            (self.root / "record.json").symlink_to(self.root / "lock")
            with self.assertRaises(workflow.WorkflowError):
                store.read("record.json")

    def register_fixture(self):
        # Fake native child output + registration for deterministic storage tests.
        # It is never installed in the real dedicated root or used for inference.
        from review_policy import PROVIDERS

        binding = auth.new_binding()
        prefix = "generations/" + binding["generation_id"] + "/config/"
        with auth.store(self.root, create=True) as storage:
            (self.root / prefix).mkdir(mode=0o700, parents=True)
            (self.root / "generations").chmod(0o700)
            (self.root / "generations" / binding["generation_id"]).chmod(0o700)
            storage.write(prefix + ".credentials.json", self.credentials)
            storage.write(prefix + ".claude.json", self.config)
            _, account = auth.native_records(self.credentials, self.config, 900, now=self.now)
            registration = {
                "authentication": binding,
                "cli": PROVIDERS["claude-code"]["cli"],
                "native_exit": 0,
                "interactive": True,
                "account": account,
                "lineage": [],
                "retained_capability_generations": [],
                "files": {
                    prefix + name: auth._digest(storage.raw(prefix + name))
                    for name in (".credentials.json", ".claude.json")
                },
            }
            receipt = {
                "schema_version": 1,
                "authentication": binding,
                "account": account,
                "paid_usage_disabled": True,
                "recorded_at": self.now,
                "expires_at": self.now + auth.RECEIPT_SECONDS,
            }
            storage.write("registration.json", registration)
            storage.write(
                "setup-attempt.json", {"schema_version": 1, "authentication": binding, "status": "completed"}
            )
            storage.write("receipt.json", receipt)
        return binding

    def policy(self, binding):
        from review_policy import choices, policy

        return {**policy(choices("claude-code"), {}), "authentication": binding}

    def test_snapshot_real_store_no_refresh_commitback_or_inherited_auth_and_cleanup(self):
        binding = self.register_fixture()
        with auth.store(self.root) as storage:
            registration = storage.raw("registration.json")
            before = {name: storage.raw(name) for name in storage.read("registration.json")["files"]}
        homes = []
        for failure in (None, RuntimeError, KeyboardInterrupt):
            with patch.dict(
                os.environ,
                {
                    "ANTHROPIC_API_KEY": "hostile",
                    "CLAUDE_CODE_OAUTH_TOKEN": "injected",
                    "CLAUDE_CONFIG_DIR": "/never-read-ordinary",
                },
            ):
                try:
                    with auth.snapshot(self.policy(binding), root=self.root) as (env, recheck):
                        home = Path(env["HOME"])
                        homes.append(home)
                        self.assertNotIn("ANTHROPIC_API_KEY", env)
                        self.assertNotIn("CLAUDE_CODE_OAUTH_TOKEN", env)
                        credential = Path(env["CLAUDE_CONFIG_DIR"]) / ".credentials.json"
                        value = json.loads(credential.read_bytes())
                        self.assertNotIn("refreshToken", value["claudeAiOauth"])
                        self.assertNotIn("fixture-refresh", credential.read_text())
                        recheck()
                        with self.assertRaises(workflow.WorkflowError):
                            auth.current_binding(root=self.root)
                        # A native process may mutate only its disposable snapshot.
                        credential.write_text("changed-native-snapshot")
                        if failure:
                            raise failure
                except (RuntimeError, KeyboardInterrupt):
                    if failure is None:
                        raise
        self.assertTrue(all(not home.exists() for home in homes))
        with auth.store(self.root) as storage:
            self.assertEqual(storage.raw("registration.json"), registration)
            self.assertEqual({name: storage.raw(name) for name in before}, before)

    def test_current_binding_generation_tamper_stale_receipt_and_account_mismatch_refuse(self):
        binding = self.register_fixture()
        self.assertEqual(auth.current_binding(root=self.root), binding)
        wrong = {**binding, "generation_id": auth.new_binding()["generation_id"]}
        with self.assertRaisesRegex(workflow.WorkflowError, "generation changed"):
            with auth.snapshot(self.policy(wrong), root=self.root):
                pass
        with auth.store(self.root) as storage:
            receipt = storage.read("receipt.json")
            for changes in (
                {"expires_at": self.now - 1},
                {"recorded_at": self.now + 3600},
                {"expires_at": self.now + auth.RECEIPT_SECONDS + 1},
                {"paid_usage_disabled": False},
                {"account": {}},
                {"authentication": wrong},
            ):
                storage.write("receipt.json", {**receipt, **changes}, replace=True)
                with self.assertRaises(workflow.WorkflowError):
                    auth._load(storage, 900)
            storage.write("receipt.json", receipt, replace=True)
            name = next(iter(storage.read("registration.json")["files"]))
            storage.write(name, {"tampered": True}, replace=True)
            with self.assertRaises(workflow.WorkflowError):
                auth._load(storage, 900)

    def test_immediate_prelaunch_lifetime_and_clock_are_rechecked(self):
        binding = self.register_fixture()
        with auth.snapshot(self.policy(binding), root=self.root) as (_, recheck):
            with patch.object(auth.time, "time", return_value=self.now + 4000):
                with self.assertRaisesRegex(workflow.WorkflowError, "Clock changed"):
                    recheck()
            with patch.object(auth, "_load", side_effect=workflow.WorkflowError("expiry check")):
                with self.assertRaisesRegex(workflow.WorkflowError, "expiry check"):
                    recheck()

    def test_provenance_missing_or_bad_cli_cannot_establish_native_login(self):
        self.register_fixture()
        with auth.store(self.root) as storage:
            registration = storage.read("registration.json")
            for changes in (
                {"native_exit": 1},
                {"native_exit": False},
                {"interactive": False},
                {"cli": {}},
                {"files": {}},
                {"account": {}},
            ):
                storage.write("registration.json", {**registration, **changes}, replace=True)
                with self.assertRaises(workflow.WorkflowError):
                    auth._load(storage, 900)

    def test_real_git_and_symlinked_parent_are_refused_before_creation(self):
        base = Path(self.temp.name)
        (base / ".git").write_text("gitdir: /fixture-only")
        with self.assertRaises(workflow.WorkflowError):
            with auth.store(self.root, create=True):
                pass
        self.assertFalse(self.root.exists())
        (base / ".git").unlink()
        self.root.symlink_to(base / "target")
        with self.assertRaises(workflow.WorkflowError):
            with auth.store(self.root, create=True):
                pass
        self.assertFalse((base / "target").exists())

    def test_endpoint_permission_error_and_setup_callback_block_before_auth(self):
        from unittest.mock import Mock

        import review_claude

        path = Mock()
        path.lstat.side_effect = PermissionError("private-policy-path")
        with patch.object(review_claude, "MANAGED_PATHS", (path,)):
            with self.assertRaisesRegex(workflow.WorkflowError, "cannot be inspected"):
                review_claude.managed_controls()
        with (
            patch.object(auth.sys.stdin, "isatty", return_value=True),
            patch.object(auth.sys.stdout, "isatty", return_value=True),
            patch.object(auth.sys.stderr, "isatty", return_value=True),
            patch("review_cli.executable", return_value="/verified/claude"),
            patch.object(review_claude, "managed_controls"),
            patch.object(review_claude, "check_controls"),
            patch.object(auth.subprocess, "run") as child,
        ):
            with self.assertRaisesRegex(workflow.WorkflowError, "callback policy isolation"):
                auth.setup(None, root=self.root, paid_usage_disabled=True)
        child.assert_not_called()
        self.assertFalse(self.root.exists())

    def test_guarded_manual_setup_and_renewal_keep_generations_and_never_import_login(self):
        import subprocess

        def native_login(args, **kwargs):
            self.assertEqual(args[-3:], ["auth", "login", "--claudeai"])
            self.assertEqual(kwargs["umask"], 0o077)
            self.assertTrue(kwargs["close_fds"])
            self.assertNotIn("capture_output", kwargs)
            self.assertNotIn("CLAUDE_CODE_OAUTH_TOKEN", kwargs["env"])
            config = Path(kwargs["env"]["CLAUDE_CONFIG_DIR"])
            for name, data in ((".credentials.json", self.credentials), (".claude.json", self.config)):
                path = config / name
                path.write_text(json.dumps(data))
                path.chmod(0o600)
            return subprocess.CompletedProcess(args, 0)

        ordinary = Path(self.temp.name) / "ordinary"
        ordinary.mkdir()
        (ordinary / ".credentials.json").write_text("DO-NOT-READ-OR-IMPORT")
        with (
            patch.object(auth.sys.stdin, "isatty", return_value=True),
            patch.object(auth.sys.stdout, "isatty", return_value=True),
            patch.object(auth.sys.stderr, "isatty", return_value=True),
            patch("review_cli.executable", return_value="/verified/claude"),
            patch("review_claude.native_setup_controls"),
            patch.object(auth.subprocess, "run", side_effect=native_login) as child,
            patch.dict(os.environ, {"CLAUDE_CONFIG_DIR": str(ordinary)}),
        ):
            first = auth.setup(None, root=self.root, paid_usage_disabled=True)["authentication"]
            self.assertEqual(auth.current_binding(root=self.root), first)
            old = self.root / "generations" / first["generation_id"] / "config/.credentials.json"
            old_bytes = old.read_bytes()
            second = auth.setup(
                None, root=self.root, renew=True, paid_usage_disabled=True, retain_capability=True
            )["authentication"]
            self.assertEqual(second["registration_id"], first["registration_id"])
            self.assertNotEqual(second["generation_id"], first["generation_id"])
            self.assertEqual(old.read_bytes(), old_bytes)
            self.assertEqual(auth.current_binding(root=self.root), second)
            self.assertTrue(auth.capability_lineage(first, second, 900, root=self.root))
            with auth.store(self.root) as storage:
                registration = storage.read("registration.json")
                registration["retained_capability_generations"] = []
                storage.write("registration.json", registration, replace=True)
            self.assertFalse(auth.capability_lineage(first, second, 900, root=self.root))
            self.config["oauthAccount"]["accountUuid"] = "33333333-3333-4333-8333-333333333333"
            with self.assertRaisesRegex(workflow.WorkflowError, "account changed"):
                auth.setup(None, root=self.root, renew=True, paid_usage_disabled=True)
            with self.assertRaisesRegex(workflow.WorkflowError, "incomplete"):
                auth.current_binding(root=self.root)
            self.assertEqual(child.call_count, 3)  # Fake native login calls; zero inference.
        self.assertEqual((ordinary / ".credentials.json").read_text(), "DO-NOT-READ-OR-IMPORT")
