"""Current behavior tests; historical protocol implementations are intentionally retired."""

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts/agentic"))
import claude_auth  # noqa: E402
import finish  # noqa: E402
import providers  # noqa: E402
import review  # noqa: E402
from workflow import Repo, WorkflowError, configuration, write_json  # noqa: E402


def report():
    return {
        "summary": "No supported major defect found in the examined code.",
        "findings": [],
        "inspected": ["scripts/agentic/review.py"],
        "limitations": ["Static subset inspection; no tests executed."],
    }


class SelectionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.repo = SimpleNamespace(state=Path(self.temp.name))
        self.config = configuration(ROOT)

    def test_provider_switch_selects_own_default(self):
        selected = providers.selection(self.repo, self.config, review_provider="copilot")
        self.assertEqual(selected, {"provider": "copilot", "model": "claude-opus-5", "effort": "default"})

    def test_explicit_models_and_saved_selection(self):
        write_json(
            self.repo.state / "review-selection.json",
            {"provider": "copilot", "model": "gpt-6-astra", "effort": "high"},
        )
        self.assertEqual(providers.selection(self.repo, self.config)["model"], "gpt-6-astra")
        self.assertEqual(
            providers.selection(self.repo, self.config, review_provider="claude-code")["model"],
            "claude-opus-5-5",
        )

    def test_bad_selections_fail_before_invocation(self):
        for kwargs in [
            {"review_provider": "unknown"},
            {"review_model": "auto"},
            {"review_model": "x;rm"},
            {"review_effort": "default"},
        ]:
            with self.subTest(kwargs=kwargs), self.assertRaises(WorkflowError):
                providers.selection(self.repo, self.config, **kwargs)

    def test_claude_only_static_tools_and_no_resume(self):
        args = providers.command(
            "claude",
            providers.selection(self.repo, self.config),
            self.config,
            Path("/settings"),
            Path("/mcp"),
            "review",
            review.SCHEMA,
        )
        self.assertEqual(args[args.index("--tools") + 1], "Read,Grep,Glob")
        self.assertIn("--restricted", args)
        self.assertIn("--safe-mode", args)
        self.assertNotIn("--dangerously-skip-permissions", args)
        self.assertNotIn("--resume", args)

    def test_copilot_tools_and_credit_limit(self):
        args = providers.command(
            "copilot",
            providers.selection(self.repo, self.config, review_provider="copilot"),
            self.config,
            Path("/s"),
            Path("/m"),
            "review",
            review.SCHEMA,
        )
        self.assertIn("--available-tools=view,grep,glob", args)
        self.assertIn("--allow-tool=read", args)
        self.assertIn("--deny-tool=shell,write,url", args)
        self.assertEqual(args[args.index("--max-ai-credits") + 1], "400")
        with patch.dict(
            os.environ,
            {
                "COPILOT_GITHUB_TOKEN": "synthetic",
                "PATH": "/custom/node:/usr/bin",
                "SSL_CERT_FILE": "/trusted/ca.pem",
            },
        ):
            with providers.provider_environment({"provider": "copilot"}, 900) as env:
                self.assertIn("/custom/node", env["PATH"])
                self.assertEqual(env["SSL_CERT_FILE"], "/trusted/ca.pem")


class AuthTests(unittest.TestCase):
    def setUp(self):
        self.now = 1_000_000
        self.credentials = {
            "claudeAiOauth": {
                "accessToken": "synthetic-known-truth-token",
                "refreshToken": "must-not-be-copied",
                "expiresAt": (self.now + 3600) * 1000,
                "subscriptionType": "max",
                "scopes": ["user:profile", "user:inference"],
            }
        }
        self.config = {
            "oauthAccount": {
                "hasExtraUsageEnabled": False,
                "accountUuid": "fixture-account",
                "organizationUuid": "fixture-org",
            }
        }
        self.receipt = {"paid_usage_disabled": True, "recorded_at": self.now}

    def validate(self):
        return claude_auth.validate(self.credentials, self.config, self.receipt, 900, now=self.now)

    def test_access_only_projection(self):
        credentials, config = self.validate()
        self.assertNotIn("refreshToken", credentials["claudeAiOauth"])
        self.assertEqual(set(config["oauthAccount"]), {"accountUuid", "organizationUuid"})

    def test_paid_and_api_alternatives_rejected(self):
        self.config["oauthAccount"]["hasExtraUsageEnabled"] = True
        with self.assertRaises(WorkflowError):
            self.validate()
        self.config["oauthAccount"]["hasExtraUsageEnabled"] = False
        self.config["primaryApiKey"] = "synthetic"
        with self.assertRaises(WorkflowError):
            self.validate()

    def test_wrong_plan_expiry_and_receipt_rejected(self):
        self.credentials["claudeAiOauth"]["subscriptionType"] = "pro"
        with self.assertRaises(WorkflowError):
            self.validate()
        self.credentials["claudeAiOauth"]["subscriptionType"] = "max"
        self.credentials["claudeAiOauth"]["expiresAt"] = (self.now + 900) * 1000
        with self.assertRaises(WorkflowError):
            self.validate()
        self.credentials["claudeAiOauth"]["expiresAt"] = (self.now + 3600) * 1000
        self.receipt["recorded_at"] = self.now - 8 * 86400
        with self.assertRaises(WorkflowError):
            self.validate()

    def test_private_permissions(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "credential.json"
            write_json(path, {"value": "fixture"})
            self.assertEqual(claude_auth.private_json(path), {"value": "fixture"})
            path.chmod(0o644)
            with self.assertRaises(WorkflowError):
                claude_auth.private_json(path)

    def test_environment_does_not_inherit_keys(self):
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "synthetic-secret", "GH_TOKEN": "synthetic-gh"}):
            env = claude_auth.environment(Path("/private"), Path("/private/config"))
        self.assertNotIn("ANTHROPIC_API_KEY", env)
        self.assertNotIn("GH_TOKEN", env)


class SnapshotTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "repo"
        self.root.mkdir()

        def git(*args):
            return subprocess.check_output(
                ["git", *args], cwd=self.root, stderr=subprocess.DEVNULL, text=True
            ).strip()

        self.git = git
        git("init", "-b", "main")
        git("config", "user.name", "Fixture")
        git("config", "user.email", "fixture@example.invalid")
        git("remote", "add", "origin", "https://github.com/fixture/project.git")
        (self.root / "a.py").write_text("answer = 42\n")
        (self.root / 'café "quoted".md').write_text("known unicode bytes\n")
        (self.root / "binary.bin").write_bytes(b"\xff\x00")
        (self.root / "link").symlink_to("/etc/passwd")
        git("add", ".")
        git("commit", "-m", "fixture")
        self.head = git("rev-parse", "HEAD")
        self.repo = Repo(self.root)
        self.output = Path(self.temp.name) / "snapshot"
        self.config = configuration(ROOT)

    def test_snapshot_is_committed_text_and_omits_symlinks_binary(self):
        (self.root / "a.py").write_text("changed privately\n")
        data = review.snapshot(self.repo, self.head, self.output, self.config)
        self.assertEqual((self.output / "a.py").read_text(), "answer = 42\n")
        self.assertEqual((self.output / 'café "quoted".md').read_text(), "known unicode bytes\n")
        self.assertEqual(set(data["omitted"]), {"binary.bin", "link"})
        self.assertFalse((self.output / "link").exists())

    def test_finite_snapshot_and_path_refusal(self):
        self.config["max_snapshot_bytes"] = 1
        with self.assertRaises(WorkflowError):
            review.snapshot(self.repo, self.head, self.output, self.config)
        for path in ["../private", "/etc/passwd", "a\nb"]:
            with self.subTest(path=path), self.assertRaises(WorkflowError):
                review.safe_path(path)


class ExecutionTests(unittest.TestCase):
    def test_timeout_kills_owned_process(self):
        result, raw = providers.capture(
            [sys.executable, "-c", "import time;time.sleep(30)"], cwd=ROOT, env=os.environ.copy(), seconds=0.2
        )
        self.assertEqual(result["reason"], "timeout")
        self.assertNotEqual(result["exit_status"], 0)
        self.assertLess(result["elapsed_seconds"], 3)
        self.assertEqual(raw, b"")

    @unittest.skipUnless(sys.platform == "linux", "process-group fixture uses /proc")
    def test_cleanup_kills_term_ignoring_child_after_parent_exits(self):
        with tempfile.TemporaryDirectory() as tmp:
            pidfile = Path(tmp) / "child.pid"
            script = (
                "import os,signal,time,pathlib\np=os.fork()\nif p==0:\n signal.signal(signal.SIGTERM,signal.SIG_IGN)\n pathlib.Path("
                + repr(str(pidfile))
                + ").write_text(str(os.getpid()))\n time.sleep(30)\nelse: time.sleep(30)"
            )
            result, _ = providers.capture(
                [sys.executable, "-c", script], cwd=ROOT, env=os.environ.copy(), seconds=0.4
            )
            pid = int(pidfile.read_text())
            state = Path(f"/proc/{pid}/stat")
            for _ in range(100):
                alive = state.exists() and state.read_text().split()[2] != "Z"
                if not alive:
                    break
                time.sleep(0.01)
            if alive:
                os.kill(pid, 9)
            self.assertFalse(alive)
            self.assertEqual(result["reason"], "timeout")

    def test_output_limit_and_normal_completion(self):
        result, raw = providers.capture(
            [sys.executable, "-c", 'print("x"*10000)'],
            cwd=ROOT,
            env=os.environ.copy(),
            seconds=2,
            output_limit=100,
        )
        self.assertEqual(result["reason"], "output_limit")
        self.assertEqual(raw, b"")
        result, raw = providers.capture(
            [sys.executable, "-c", 'print("ok")'], cwd=ROOT, env=os.environ.copy(), seconds=2
        )
        self.assertEqual(result["exit_status"], 0)
        self.assertEqual(raw, b"ok\n")

    def test_malformed_and_invented_locations_refused(self):
        with self.assertRaises(WorkflowError):
            review.validate_report({"summary": "no report"})
        value = report()
        value["findings"] = [
            {"severity": "P1", "path": "../secret", "line": 1, "claim": "c", "evidence": "e", "fix": "f"}
        ]
        with self.assertRaises(WorkflowError):
            review.validate_report(value)


class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.head = "a" * 40
        self.base = "b" * 40
        raw = json.dumps(report(), indent=2) + "\n"
        (self.directory / "report.json").write_text(raw)
        self.meta = {
            "status": "completed",
            "pr": 1,
            "repository": "fixture/project",
            "head": self.head,
            "base": self.base,
            "policy": {"provider": "claude-code", "model": "claude-opus-5-5", "effort": "medium"},
            "report_sha256": hashlib.sha256(raw.encode()).hexdigest(),
        }
        write_json(self.directory / "review.json", self.meta)
        self.calls = []
        self.published = []

        def api(path, **kwargs):
            self.calls.append((path, kwargs))
            if path == "pulls/1":
                return {"head": {"sha": self.head}, "base": {"sha": self.base}}
            if path == "issues/2":
                return {"body": "different issue"}
            if "reviews?" in path:
                return self.published
            if kwargs:
                data = kwargs["data"]
                self.published = [
                    {
                        "id": 2,
                        "html_url": "https://github.com/fixture/project/pull/1#review2",
                        "body": data["body"],
                    }
                ]
                return self.published[0]
            raise AssertionError(path)

        self.repo = SimpleNamespace(name="fixture/project", api=api)

    def test_comment_publication_is_idempotent_and_not_approval(self):
        first = review.publish(self.repo, self.directory)
        meta = json.loads((self.directory / "review.json").read_text())
        del meta["publication"]
        write_json(self.directory / "review.json", meta)
        second = review.publish(self.repo, self.directory)
        self.assertEqual(json.loads((self.directory / "review.json").read_text())["publication"], first)
        posts = [kwargs for _, kwargs in self.calls if kwargs]
        self.assertEqual(len(posts), 1)
        self.assertEqual(posts[0]["data"]["event"], "COMMENT")
        self.assertEqual(first["url"], second["url"])
        self.assertTrue(second["reused"])

    def test_stale_and_changed_reports_refused(self):
        self.head = "c" * 40
        with self.assertRaises(WorkflowError):
            review.publish(self.repo, self.directory)
        self.head = "a" * 40
        (self.directory / "report.json").write_text("{}")
        with self.assertRaises(WorkflowError):
            review.publish(self.repo, self.directory)

    def test_repeat_execution_reuses_report_without_provider_call(self):
        path = self.directory / "reviews" / f"pr1-{self.head[:12]}-fixture"
        path.mkdir(parents=True)
        meta = {**self.meta, "directory": str(path)}
        meta["task_key"] = review.task_key(self.repo, None, None)
        write_json(path / "review.json", meta)
        (path / "report.json").write_bytes((self.directory / "report.json").read_bytes())
        write_json(
            path.with_name(f"pr1-{self.head[:12]}-zz-failed") / "review.json", {**meta, "status": "failed"}
        )
        self.repo.state = self.directory
        with patch.object(providers, "invoke", side_effect=AssertionError("no paid replay")):
            result = review.execute(self.repo, 1, configuration(ROOT))
        self.assertEqual(result["directory"], str(path))
        with (
            patch.object(review, "prepare", side_effect=WorkflowError("different contract")),
            self.assertRaisesRegex(WorkflowError, "different contract"),
        ):
            review.execute(self.repo, 1, configuration(ROOT), issue=2)
        meta["status"] = "failed"
        write_json(path / "review.json", meta)
        with self.assertRaises(WorkflowError):
            review.execute(self.repo, 1, configuration(ROOT))

    def test_finish_never_merges_and_refuses_missing_publication(self):
        with self.assertRaises(WorkflowError):
            finish.preflight(self.repo, 1, self.directory)

    def test_finish_prepares_command_for_realistic_comment_state(self):
        self.meta["publication"] = {"id": 2, "url": "fixture"}
        write_json(self.directory / "review.json", self.meta)
        marker = f"<!-- flowdc-review:{self.head}:{self.meta['report_sha256']} -->"
        self.repo.api = lambda path: (
            {
                "head": {"sha": self.head},
                "base": {"sha": self.base},
                "mergeable": True,
                "draft": False,
                "state": "open",
            }
            if path == "pulls/1"
            else {"commit_id": self.head, "state": "COMMENTED", "body": marker}
        )
        checks = [
            {"name": name, "bucket": "pass", "state": "SUCCESS"}
            for name in configuration(ROOT)["required_checks"]
        ]
        with patch.object(finish, "run", return_value=SimpleNamespace(stdout=json.dumps(checks))):
            value = finish.preflight(self.repo, 1, self.directory)
        self.assertIn("--match-head-commit " + self.head, value["command"])

    def test_invocation_error_and_interrupt_record_status_without_retry(self):
        self.repo.state = self.directory
        for exception, status in [(TypeError("bad receipt"), "failed"), (KeyboardInterrupt(), "stopped")]:
            with (
                patch.object(review, "prepare", return_value=(self.directory, dict(self.meta))),
                patch.object(providers, "invoke", side_effect=exception) as invoke,
            ):
                if status == "stopped":
                    with self.assertRaises(KeyboardInterrupt):
                        review.execute(self.repo, 1, configuration(ROOT), fresh=True)
                else:
                    self.assertEqual(
                        review.execute(self.repo, 1, configuration(ROOT), fresh=True)["status"], status
                    )
                self.assertEqual(invoke.call_count, 1)
            self.assertEqual(json.loads((self.directory / "review.json").read_text())["status"], status)


if __name__ == "__main__":
    unittest.main()
