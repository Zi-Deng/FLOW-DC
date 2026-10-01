"""Positive/negative native stream evidence; fixtures cannot establish activation."""

import contextlib
import copy
import json
import os
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import patch

from test_workflow import GitFixture, review, workflow

# isort: split
import claude_native_auth
import claude_telemetry as telemetry
import review_claude
import review_coverage as coverage
from claude_fixtures import AUTHENTICATION, native_events, native_stream


class ClaudeTelemetryTests(GitFixture):
    def setUp(self):
        super().setUp()
        binding = patch.object(claude_native_auth, "current_binding", return_value=AUTHENTICATION)
        binding.start()
        self.addCleanup(binding.stop)
        self.commit_task()
        self.directory = review.prepare(self.repo, 31, 12, 1234, review_provider="claude-code")
        self.packet = self.directory / "packet"
        self.policy = review.verify_packet(self.directory)["review_policy"]
        self.rows = native_events(self.packet, self.packet, "fixture-session")

    def evaluate(self, rows, **kwargs):
        raw = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows)
        body, diag = telemetry.capture(
            raw, self.packet, self.packet, self.policy, "fixture-session", **kwargs
        )
        return coverage.assess(self.packet, body, diag, policy=self.policy), diag, body

    def test_successful_native_results_qualify_with_exact_source_ranges(self):
        self.assertFalse((self.packet / ".github/agents").exists())
        assessment, diag, body = self.evaluate(self.rows)
        self.assertTrue(assessment["qualified"])
        self.assertTrue(all(diag["capability"].values()))
        self.assertEqual(body, self.rows[-1]["result"])
        self.assertEqual(diag["usage"]["counters"]["estimated_usd"], 0.01)

    def test_terminal_bytes_are_not_synthesized_or_extracted(self):
        report = " \r\n\x1b\x07café\\n\r\n"
        self.rows[-1]["result"] = report
        result, _, body = self.evaluate(self.rows)
        self.assertEqual(body.encode(), report.encode())
        self.assertFalse(result["qualified"])
        self.rows[-1].pop("result")
        self.rows.insert(
            -1,
            {
                "type": "assistant",
                "session_id": "fixture-session",
                "message": {"model": self.policy["model"], "content": [{"type": "text", "text": report}]},
            },
        )
        self.assertEqual(self.evaluate(self.rows)[2], "")

    def test_native_partial_report_recovers_and_publishes_exact_bytes_without_inference(self):
        body = " \r\n\x1b\x07Native partial café\\n\r\n"
        self.rows[-1]["result"] = body
        _, diag, _ = self.evaluate(self.rows)
        meta = review.verify_packet(self.directory)
        review.save_result(self.directory, meta, body, diag, "2.1.282")
        (self.directory / "review-result.json").unlink()
        with patch.object(
            review_claude.review_process, "capture", side_effect=AssertionError("Recovery cannot infer")
        ):
            review.recover_review(self.repo, self.directory)
            review.publish(self.repo, self.directory)
        self.assertEqual((self.directory / "review.md").read_bytes(), body.encode())
        expected = review.publication_body(self.directory)
        self.assertIn(body, expected)
        self.assertIn("Independent Claude Code", expected)
        self.assertIn("INCOMPLETE", expected)
        self.assertEqual(self.posts[-1][1]["body"], expected)
        self.assertFalse(review.coverage_ready(self.directory))
        with self.assertRaises(workflow.WorkflowError):
            review.qualification(self.directory, require=True)

    def test_wrong_identity_forbidden_tools_and_ui_only_results_never_qualify(self):
        variants = []
        for field, value in [
            ("session_id", "other"),
            ("model", "claude-opus-5"),
            ("tools", ["Read", "Grep", "Glob", "Bash"]),
            ("mcp_servers", [{"name": "hidden"}]),
            ("permissionMode", "bypassPermissions"),
        ]:
            rows = copy.deepcopy(self.rows)
            rows[0][field] = value
            variants.append(rows)
        rows = copy.deepcopy(self.rows)
        rows[1]["message"]["content"][0]["name"] = "Agent"
        variants.append(rows)
        rows = copy.deepcopy(self.rows)
        rows[1]["parent_tool_use_id"] = "delegated"
        variants.append(rows)
        rows = copy.deepcopy(self.rows)
        block = rows[2]["message"]["content"][0]
        rows[2]["tool_use_result"] = {"content": block.pop("content")}
        variants.append(rows)
        for index, rows in enumerate(variants):
            with self.subTest(index=index):
                self.assertFalse(self.evaluate(rows)[0]["qualified"])

    def test_malformed_duplicate_missing_error_quota_budget_and_unknown_usage_fail(self):
        variants = [self.rows[:-1], self.rows + [self.rows[-1]], self.rows[:2] + self.rows[3:]]
        for field, value in [
            ("is_error", True),
            ("subtype", "error_max_budget_usd"),
            ("total_cost_usd", 11),
            ("total_cost_usd", None),
            ("modelUsage", {"another-model": {}}),
        ]:
            rows = copy.deepcopy(self.rows)
            rows[-1][field] = value
            variants.append(rows)
        variants.append(
            self.rows[:-1]
            + [
                {
                    "type": "rate_limit_event",
                    "session_id": "fixture-session",
                    "rate_limit_info": {"status": "rejected"},
                }
            ]
            + self.rows[-1:]
        )
        variants.append(
            self.rows
            + [{"type": "unknown-secret-event", "session_id": "fixture-session", "content": "secret-data"}]
        )
        for index, rows in enumerate(variants):
            with self.subTest(index=index):
                result, diag, _ = self.evaluate(rows)
                self.assertFalse(result["qualified"])
                self.assertNotIn("unknown-secret-event", json.dumps(diag))
                self.assertNotIn("secret-data", json.dumps(diag))
        for raw in [b"\xff", b'{"type":', b'{"type":"result","type":"user"}']:
            _, diag = telemetry.capture(raw, self.packet, self.packet, self.policy, "fixture-session")
            self.assertTrue(diag["reasons"])

    def test_read_mismatch_and_incomplete_content_do_not_earn_source_credit(self):
        for content in [
            "",
            "1\twrong source",
            "ordinary unnumbered output",
            "1\tRead-only tool capability fixture\n[truncated]",
        ]:
            rows = copy.deepcopy(self.rows)
            rows[2]["message"]["content"][0]["content"] = content
            result, diag, _ = self.evaluate(rows)
            self.assertFalse(result["qualified"])
            self.assertFalse(diag["capability"]["view"])

    def test_bounds_and_failed_process_are_incomplete(self):
        with patch.object(coverage, "MAX_EVENTS", 2):
            self.assertIn("event_limit_exceeded", self.evaluate(self.rows)[1]["reasons"])
        with patch.object(coverage, "MAX_TOOL_RECORDS", 1):
            self.assertFalse(self.evaluate(self.rows)[0]["qualified"])
        self.assertFalse(self.evaluate(self.rows, exit_code=1)[0]["qualified"])
        self.assertFalse(self.evaluate(self.rows, failure="provider_timeout")[0]["qualified"])

    def test_claude_invocation_is_absolute_bounded_fresh_and_minimal(self):
        homes = []
        token = "fake-subscription-test-token"

        @contextlib.contextmanager
        def fake_snapshot(policy):
            # Invocation test double only. Real private snapshot/cleanup is tested separately.
            with tempfile.TemporaryDirectory() as temporary:
                env = review_claude.environment(Path(temporary))
                (Path(env["CLAUDE_CONFIG_DIR"]) / ".credentials.json").write_text(
                    json.dumps({"claudeAiOauth": {"accessToken": token}})
                )
                yield env, lambda: None

        def capture(args, **kwargs):
            self.assertEqual(args[0], "/verified/2.1.282/claude")
            self.assertEqual(kwargs["timeout"], 900)
            env = kwargs["env"]
            self.assertNotIn("CLAUDE_CODE_OAUTH_TOKEN", env)
            credential = json.loads((Path(env["CLAUDE_CONFIG_DIR"]) / ".credentials.json").read_bytes())
            self.assertEqual(credential["claudeAiOauth"]["accessToken"], token)
            self.assertNotIn("refreshToken", credential["claudeAiOauth"])
            self.assertNotIn("ANTHROPIC_API_KEY", env)
            self.assertNotIn("CLAUDE_CODE_USE_BEDROCK", env)
            self.assertNotIn("HTTP_PROXY", env)
            self.assertNotIn("CLAUDE_CODE_MAX_RETRIES", env)
            self.assertNotIn(token, json.dumps(args))
            self.assertIn("--safe-mode", args)
            self.assertIn("--restricted", args)
            self.assertIn("--no-session-persistence", args)
            self.assertNotIn("--resume", args)
            settings = json.loads(Path(args[args.index("--settings") + 1]).read_bytes())
            self.assertTrue(settings["disableAllHooks"])
            self.assertFalse(settings["switchModelsOnFlag"])
            self.assertEqual(settings["fallbackModel"], [])
            homes.append(Path(env["HOME"]))
            return subprocess.CompletedProcess(
                args, 0, native_stream(self.packet, kwargs["cwd"], args[args.index("--session-id") + 1]), b""
            )

        with (
            patch.dict(
                os.environ,
                {"ANTHROPIC_API_KEY": "hostile", "CLAUDE_CODE_USE_BEDROCK": "1", "HTTP_PROXY": "hostile"},
            ),
            patch.object(review_claude, "preflight", return_value="/verified/2.1.282/claude"),
            patch.object(claude_native_auth, "snapshot", side_effect=fake_snapshot),
            patch.object(review_claude.review_cli, "executable", return_value="/verified/2.1.282/claude"),
            patch("review_diagnostics.require_activation"),
            patch.object(review_claude.review_process, "capture", side_effect=capture) as process,
        ):
            report = review.review(self.repo, self.directory)
        process.assert_called_once()
        self.assertTrue(report.is_file())
        self.assertTrue(review.coverage_ready(self.directory))
        self.assertTrue(all(not path.exists() for path in homes))
        self.assertNotIn(token, (self.directory / "diagnostics.json").read_text())

    def test_missing_prerequisites_prevent_inference(self):
        with (
            patch.object(
                review_claude,
                "preflight",
                side_effect=workflow.WorkflowError("Missing protected subscription receipt"),
            ),
            patch.object(review_claude.review_process, "capture") as process,
        ):
            with self.assertRaisesRegex(workflow.WorkflowError, "receipt"):
                review.review(self.repo, self.directory)
        process.assert_not_called()
        self.assertFalse((self.directory / "attempt.json").exists())
        diag = json.loads((self.directory / "diagnostics.json").read_bytes())
        self.assertEqual(diag["adapter"], self.policy["adapter"])

    def test_legacy_token_policy_cannot_execute_or_be_relabelled(self):
        legacy = dict(self.policy)
        legacy.pop("authentication")
        with patch.object(review_claude.review_process, "capture") as process:
            with self.assertRaisesRegex(workflow.WorkflowError, "authentication binding"):
                review_claude.preflight(self.repo, legacy)
        process.assert_not_called()

    def test_settings_cannot_enable_hooks_or_fallback(self):
        for key, value in [
            ("disableAllHooks", False),
            ("switchModelsOnFlag", True),
            ("fallbackModel", ["other"]),
            ("availableModels", ["claude-opus-5-5", "other"]),
        ]:
            settings = review_claude.trusted_settings(self.policy)
            settings[key] = value
            with self.subTest(key=key), self.assertRaisesRegex(workflow.WorkflowError, "isolation policy"):
                review_claude.check_controls("/not-opened", settings)

    def test_native_line_boundaries_credit_only_exact_returned_lines(self):
        files = {"example.txt": "first\r\n\tcafé\r\nlast\r\n"}
        args = {"file_path": "example.txt", "offset": 2, "limit": 1}
        spans, _, reason = telemetry.observation("Read", args, "2:\tcafé", self.packet, files)
        self.assertIsNone(reason)
        self.assertEqual([(s["start_line"], s["end_line"]) for s in spans], [(2, 2)])
        for content in ("2:café", "2:\tcafe\u0301", "2:\tcaf", "1:first", "2:\tcafé\n[truncated]", ""):
            with self.subTest(content=content):
                self.assertFalse(telemetry.observation("Read", args, content, self.packet, files)[0])

    def test_event_order_and_missing_numerical_usage_remain_incomplete(self):
        variants = [self.rows[1:2] + self.rows[:1] + self.rows[2:], self.rows + self.rows[2:3]]
        for field, value in [
            ("duration_ms", None),
            ("num_turns", -1),
            ("usage", {"input_tokens": True, "output_tokens": 1}),
        ]:
            rows = copy.deepcopy(self.rows)
            rows[-1][field] = value
            variants.append(rows)
        for rows in variants:
            self.assertFalse(self.evaluate(rows)[0]["qualified"])

    def test_deduplicated_step_inputs_and_terminal_output_accounting(self):
        messages = [row["message"] for row in self.rows if row["type"] == "assistant"]
        for index, message in enumerate(messages):
            message.update(
                id=f"private-message-{index}",
                usage={
                    "input_tokens": 11,
                    "cache_read_input_tokens": 3,
                    "cache_creation_input_tokens": 2,
                    "output_tokens": 1,
                },
            )
        self.rows.insert(2, copy.deepcopy(self.rows[1]))
        result, diag, _ = self.evaluate(self.rows)
        self.assertTrue(result["qualified"])
        counters = diag["usage"]["counters"]
        self.assertEqual(counters["model_steps_observed"], len(messages))
        self.assertEqual(counters["assistant_input_tokens_observed"], 11 * len(messages))
        self.assertEqual(counters["output_tokens"], 20)
        self.assertEqual(diag["usage"]["models"][self.policy["model"]]["output_tokens"], 20)
        self.assertNotIn("private-message-", json.dumps(diag))
        self.rows[2]["message"]["usage"]["input_tokens"] = 99
        self.assertIn("inconsistent_assistant_usage", self.evaluate(self.rows)[1]["reasons"])

    def test_builtin_declarations_do_not_authorize_agent_tools_or_customizations(self):
        self.rows[0]["agents"] = ["Explore", "Plan"]
        self.assertTrue(self.evaluate(self.rows)[0]["qualified"])
        self.rows[0]["agents"].append("custom-unknown")
        self.assertFalse(self.evaluate(self.rows)[0]["qualified"])

    def test_expected_refusal_is_only_diagnostic_evidence_and_never_normal_coverage(self):
        outside = str(self.parent / "harmless-outside.txt")
        call = {
            "type": "assistant",
            "session_id": "fixture-session",
            "message": {
                "model": self.policy["model"],
                "content": [
                    {"type": "tool_use", "id": "refusal", "name": "Read", "input": {"file_path": outside}}
                ],
            },
        }
        result = {
            "type": "user",
            "session_id": "fixture-session",
            "message": {
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": "refusal",
                        "is_error": True,
                        "content": "Permission to use Read has been denied",
                    }
                ]
            },
        }
        rows = self.rows[:-1] + [call, result] + self.rows[-1:]
        raw = "\n".join(json.dumps(row) for row in rows)
        body, diag = telemetry.capture(
            raw, self.packet, self.packet, self.policy, "fixture-session", refusal_path=outside
        )
        self.assertEqual(diag["telemetry"]["controlled_refusals"], 1)
        self.assertFalse(coverage.assess(self.packet, body, diag, policy=self.policy)["qualified"])
        result["message"]["content"][0]["content"] = "File does not exist"
        raw = "\n".join(json.dumps(row) for row in rows)
        _, diag = telemetry.capture(
            raw, self.packet, self.packet, self.policy, "fixture-session", refusal_path=outside
        )
        self.assertEqual(diag["telemetry"]["controlled_refusals"], 0)
        self.assertIn("controlled_refusal_not_observed", diag["reasons"])
