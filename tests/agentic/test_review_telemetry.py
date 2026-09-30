"""Synthetic same-request CLI/session correlation; no paid or live model calls."""

import copy
import json
import os
import sys
import tempfile
import unittest
from unittest.mock import patch

from test_workflow import GitFixture, review

# isort: split
import review_coverage as coverage
import review_process
import review_telemetry as telemetry
from review_fixtures import events


class TelemetryTests(GitFixture):
    def setUp(self):
        super().setUp()
        self.commit_task()
        self.directory = review.prepare(self.repo, 31, 12, 1234)
        self.packet = self.directory / "packet"
        self.state = self.parent / "fresh-state"
        self.session = "00000000-0000-4000-8000-000000000029"
        self.event_path = self.state / "session-state" / self.session / "events.jsonl"
        self.event_path.parent.mkdir(parents=True)
        self.rows = events(self.packet)
        self.rows[0]["data"]["sessionId"] = self.session

    def evaluate(self, rows=None, stdout=None, **kwargs):
        self.event_path.write_text("\n".join(json.dumps(row) for row in (rows or self.rows)) + "\n")
        if stdout is None:
            # Representative CLI stdout intentionally lacks toolCallId/arguments.
            stdout = [
                {
                    "type": "tool.execution_start",
                    "data": {"toolName": "view", "command": "secret-looking-not-retained"},
                },
                {"type": "tool.execution_complete", "data": {"toolName": "view", "success": True}},
                {"type": "result", "exitCode": 0, "result": self.rows[-2]["data"]["content"]},
            ]
        raw = "\n".join(json.dumps(row) for row in stdout)
        body, diag = telemetry.capture(
            raw, self.state, self.session, self.packet, self.packet, version="1.0.83", **kwargs
        )
        return coverage.assess(self.packet, body, diag), diag

    def test_full_correlated_session_qualifies_idless_stdout_without_fabricating_ids(self):
        result, diag = self.evaluate()
        self.assertTrue(result["qualified"])
        self.assertEqual(diag["telemetry"]["source"], "session-state")
        self.assertEqual(diag["telemetry"]["stdout_shapes"]["tool.execution_start"]["call_id"], 0)
        self.assertGreater(diag["telemetry"]["session_shapes"]["tool.execution_start"]["call_id"], 0)
        self.assertNotIn("secret-looking-not-retained", json.dumps(diag))
        self.assertNotIn(self.session, json.dumps(diag))

    def test_session_identity_ambiguity_symlink_and_missing_events_fail_closed(self):
        rows = copy.deepcopy(self.rows)
        rows[0]["data"]["sessionId"] = "wrong-session"
        self.assertFalse(self.evaluate(rows)[0]["qualified"])
        other = self.event_path.parent.parent / "unexpected"
        other.mkdir()
        self.assertFalse(self.evaluate()[0]["qualified"])
        other.rmdir()
        self.event_path.unlink()
        self.event_path.symlink_to(self.packet / "issue.txt")
        with self.assertRaises(OSError):
            telemetry.session_events(self.state, self.session)
        self.event_path.unlink()
        body, diag = telemetry.capture(
            '{"type":"result","exitCode":0}',
            self.state,
            self.session,
            self.packet,
            self.packet,
            version="1.0.83",
        )
        self.assertFalse(coverage.assess(self.packet, body, diag)["qualified"])
        self.assertEqual(diag["telemetry"]["source"], "unavailable")

    def test_terminal_exit_code_malformed_chunked_or_unknown_stream_remains_incomplete(self):
        for code in (1, None, True, "0"):
            result, diag = self.evaluate(stdout=[{"type": "result", "exitCode": code}])
            self.assertFalse(result["qualified"])
            self.assertIn("provider_terminal_failure", diag["reasons"])
        for variant in ("chunked", "unknown", "uncorrelated", "forbidden"):
            rows = copy.deepcopy(self.rows)
            if variant == "chunked":
                rows[-2]["data"]["chunkCount"] = 2
            elif variant == "unknown":
                rows.insert(1, {"type": "arbitrary-secret-event-name", "data": {}})
            elif variant == "uncorrelated":
                del rows[1]["data"]["toolCallId"]
            else:
                rows[1]["data"]["toolName"] = "bash"
            result, diag = self.evaluate(rows)
            self.assertFalse(result["qualified"], variant)
            self.assertNotIn("arbitrary-secret-event-name", json.dumps(diag))
        self.assertFalse(self.evaluate(stdout=[])[0]["qualified"])

    def test_event_and_diagnostic_caps_remain_visible(self):
        with patch.object(coverage, "MAX_TOOL_RECORDS", 1):
            result, diag = self.evaluate()
        self.assertFalse(result["qualified"])
        self.assertIn("tool_record_limit_exceeded", diag["reasons"])
        with patch.object(coverage, "MAX_DIAGNOSTIC_BYTES", 1):
            result, diag = self.evaluate()
        self.assertFalse(result["qualified"])
        self.assertIn("diagnostic_limit_exceeded", diag["reasons"])

    def test_known_actual_usage_projection_keeps_numbers_without_private_metadata(self):
        value = {
            "totalNanoAiu": 24,
            "totalPremiumRequestCost": 2.5,
            "tokenDetails": {"input": {"tokenCount": 100}},
            "modelMetrics": {
                "claude-opus-5": {
                    "requests": {"count": 1, "cost": 2.5},
                    "usage": {"inputTokens": 100, "outputTokens": 40},
                    "cacheExpiresAt": "private",
                }
            },
            "agentMetrics": {"private-agent": {"secret": "not-retained"}},
            "sessionStartTime": "private",
        }
        clean = coverage.sanitize_usage(value)
        self.assertEqual(clean["counters"]["totalNanoAiu"], 24)
        self.assertEqual(clean["counters"]["token_input"], 100)
        self.assertEqual(clean["models"]["claude-opus-5"]["requests_cost"], 2.5)
        self.assertNotIn("private", json.dumps(clean))

    def test_root_agent_selection_and_system_message_are_narrow_and_private(self):
        selection = {
            "type": "subagent.selected",
            "data": {
                "agentName": "independent-reviewer",
                "agentDisplayName": "private-display-name",
                "tools": ["view", "grep", "glob"],
            },
        }
        message = {"type": "system.message", "data": {"role": "system", "content": "private-system-text"}}
        rows = self.rows[:1] + [selection, message] + self.rows[1:]
        result, diag = self.evaluate(rows)
        self.assertTrue(result["qualified"])
        self.assertIn("subagent.selected", diag["telemetry"]["session_shapes"])
        self.assertNotIn("private-", json.dumps(diag))
        variants = []
        for tools in (None, ["*"], ["view", "grep", "bash"], ["view", "view", "glob"]):
            event = copy.deepcopy(selection)
            event["data"]["tools"] = tools
            variants.append(event)
        for key, value in (
            ("agentName", "other"),
            ("parentToolCallId", "parent"),
            ("toolCallId", "delegate"),
        ):
            event = copy.deepcopy(selection)
            event["data"][key] = value
            variants.append(event)
        variants.extend(
            [
                {**selection, "agentId": "child"},
                {**message, "agentId": "child"},
                {"type": "hook.start", "data": {}},
                {"type": "subagent.deselected", "data": {}},
            ]
        )
        for event in variants:
            with self.subTest(event=event):
                self.assertFalse(self.evaluate(self.rows[:1] + [event] + self.rows[1:])[0]["qualified"])
                terminal = {"type": "result", "exitCode": 0, "result": self.rows[-2]["data"]["content"]}
                self.assertFalse(self.evaluate(stdout=[event, terminal])[0]["qualified"])

    def test_warning_diagnostics_distinguish_categories_without_retaining_payloads(self):
        for category in ("subscription", "policy", "mcp", "private-category"):
            warning = {
                "type": "session.warning",
                "data": {
                    "warningType": category,
                    "message": "private-message",
                    "url": "https://private-url/credential",
                    "remediation": {"private-key": "private-value"},
                },
            }
            terminal = {"type": "result", "exitCode": 0, "result": self.rows[-2]["data"]["content"]}
            for source in ("stdout", "session"):
                with self.subTest(category=category, source=source):
                    result, diag = (
                        self.evaluate(stdout=[warning, terminal])
                        if source == "stdout"
                        else self.evaluate(self.rows[:1] + [warning] + self.rows[1:])
                    )
                    self.assertFalse(result["qualified"])
                    row = diag["telemetry"]["warnings"][source]
                    self.assertEqual(row["count"], 1)
                    self.assertEqual(row["message_string"], 1)
                    self.assertEqual(row["url_string"], 1)
                    self.assertEqual(row["remediation_present"], 1)
                    key = category if category != "private-category" else "other"
                    self.assertEqual(row["categories"][key], 1)
                    self.assertNotIn("private-", json.dumps(diag))
                    telemetry.validate_summary(diag["telemetry"])

    def test_warning_projection_is_fixed_size_and_never_weakens_isolation(self):
        warnings = [
            {"type": "session.warning", "data": {"warningType": f"private-{i}", "message": "secret"}}
            for i in range(100)
        ]
        result, diag = self.evaluate(self.rows[:1] + warnings + self.rows[1:])
        self.assertFalse(result["qualified"])
        row = diag["telemetry"]["warnings"]["session"]
        self.assertEqual(row["categories"]["other"], 100)
        self.assertLess(len(json.dumps(diag["telemetry"]["warnings"])), 1000)
        telemetry.validate_summary(diag["telemetry"])
        self.assertNotIn("warnings", self.evaluate()[1]["telemetry"])
        for field in ("agentId", "parentToolCallId", "mcpServerName"):
            warning = copy.deepcopy(warnings[0])
            if field == "agentId":
                warning[field] = "private-child"
            else:
                warning["data"][field] = "private-child"
            result, diag = self.evaluate(self.rows[:1] + [warning] + self.rows[1:])
            self.assertFalse(result["qualified"])
            self.assertIn("delegated_or_mcp_event", diag["reasons"])
            self.assertNotIn("private-", json.dumps(diag))

    def test_warning_shapes_and_summary_tampering_remain_fail_closed(self):
        for data in (
            None,
            [],
            {},
            {"warningType": 42},
            {"warningType": "policy", "message": [], "url": 7, "private-key": "secret"},
        ):
            warning = {"type": "session.warning", "data": data}
            result, diag = self.evaluate(self.rows[:1] + [warning] + self.rows[1:])
            self.assertFalse(result["qualified"])
            row = diag["telemetry"]["warnings"]["session"]
            self.assertEqual(row["count"], 1)
            self.assertEqual(row["message_string"], 0)
            mixed = isinstance(data, dict) and data.get("warningType") == "policy"
            self.assertEqual(row["categories"]["policy" if mixed else "missing_or_invalid"], 1)
            self.assertEqual(row["url_string"], 0)
            self.assertEqual(row["url_present"], int(mixed))
            self.assertEqual(row["extra_fields"], int(mixed))
            self.assertEqual(row["remediation_present"], 0)
            self.assertNotIn("secret", json.dumps(diag))
            telemetry.validate_summary(diag["telemetry"])
        for key, value in (("message", "secret"), ("count", True), ("url_string", 2)):
            bad = copy.deepcopy(diag["telemetry"])
            bad["warnings"]["session"][key] = value
            with self.assertRaises(coverage.WorkflowError):
                telemetry.validate_summary(bad)
        bad = copy.deepcopy(diag["telemetry"])
        bad["warnings"]["session"]["categories"]["private-category"] = 1
        with self.assertRaises(coverage.WorkflowError):
            telemetry.validate_summary(bad)

    def test_managed_settings_diagnostics_never_grant_policy_credit(self):
        terminal = {"type": "result", "exitCode": 0, "result": self.rows[-2]["data"]["content"]}
        for data in (
            {"source": "none", "failClosed": False},
            {"source": "server", "failClosed": True, "settings": {"secret": "credential"}},
            {"source": "secret-source", "managedKeys": ["secret-key"]},
            None,
        ):
            event = {"type": "session.managed_settings_resolved", "ephemeral": True, "data": data}
            result, diag = self.evaluate(stdout=[event, terminal])
            self.assertFalse(result["qualified"])
            self.assertIn("unsupported_stdout_event", diag["reasons"])
            row = diag["telemetry"]["managed_settings"]["stdout"]
            self.assertEqual(row["count"], 1)
            self.assertNotIn("secret", json.dumps(diag))
            self.assertNotIn("credential", json.dumps(diag))
            telemetry.validate_summary(diag["telemetry"])
        row["settings"] = "secret"
        with self.assertRaises(coverage.WorkflowError):
            telemetry.validate_summary(diag["telemetry"])

    def test_unknown_event_names_are_bounded_digests_and_never_qualify(self):
        events = [{"type": f"secret-type-{i}", "data": {"content": "private"}} for i in range(100)]
        result, diag = self.evaluate(self.rows[:1] + events + self.rows[1:])
        self.assertFalse(result["qualified"])
        unknown = diag["telemetry"]["unknown_types"]["session"]
        self.assertEqual(sum(unknown.values()), 100)
        self.assertEqual(len(unknown), 65)
        self.assertEqual(unknown[coverage.checksum("secret-type-0")], 1)
        self.assertNotIn("secret-type", json.dumps(diag))
        self.assertNotIn("private", json.dumps(diag))
        telemetry.validate_summary(diag["telemetry"])
        unknown["secret-raw-name"] = 1
        with self.assertRaises(coverage.WorkflowError):
            telemetry.validate_summary(diag["telemetry"])


class CaptureTests(unittest.TestCase):
    def test_owned_process_capture_is_bounded_and_preserves_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            result = review_process.capture(
                [sys.executable, "-c", "import os;os.write(1,b'\\r\\n\\x1b\\x07');os.write(2,b'withheld')"],
                cwd=directory,
                env=os.environ.copy(),
                timeout=5,
            )
            self.assertEqual(result.stdout, b"\r\n\x1b\x07")
            self.assertEqual(result.stderr, b"")
            self.assertIsNone(result.failure_reason)
            result = review_process.capture(
                [sys.executable, "-c", "import os;os.write(1,b'x'*4096)"],
                cwd=directory,
                env=os.environ.copy(),
                timeout=5,
                limit=32,
            )
            self.assertEqual(result.failure_reason, "stream_limit_exceeded")
            self.assertLessEqual(len(result.stdout), 32)
            result = review_process.capture(
                [sys.executable, "-c", "import time;time.sleep(5)"],
                cwd=directory,
                env=os.environ.copy(),
                timeout=0.1,
            )
            self.assertEqual(result.failure_reason, "provider_timeout")
            self.assertNotEqual(result.returncode, 0)
