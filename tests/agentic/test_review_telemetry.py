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
