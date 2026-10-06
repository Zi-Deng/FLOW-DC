"""Narrow native estimate compatibility; synthetic data only, no live qualification."""

import copy
import json
import unittest
from unittest.mock import patch

import claude_native_auth
import claude_telemetry_v8 as telemetry
import review_coverage as coverage
import test_claude_reporting as reporting_fixtures
import test_claude_v7 as legacy_tests
from claude_fixtures import AUTHENTICATION, native_events
from test_claude_reporting import AUXILIARY, LIMITS
from test_workflow import GitFixture, review, workflow


def opened(kind="thinking"):
    reasons = set()
    stream = telemetry.PartialStream("model", reasons)
    stream.observe({"type": "message_start", "message": {"id": "m", "model": "model"}})
    stream.observe({"type": "content_block_start", "index": 0, "content_block": {"type": kind}})
    return stream, reasons


def delta(value, **extra):
    return {
        "type": "content_block_delta",
        "index": 0,
        "delta": {
            "type": "thinking_delta",
            "thinking": "PRIVATE_SENTINEL",
            "estimated_tokens": value,
            **extra,
        },
    }


class EstimateTests(unittest.TestCase):
    def test_observed_estimate_classes_preserve_valid_framing(self):
        for value in (None, 0, 7, 9007199254740991, 9007199254740991.0, 7.0, -0.0):
            with self.subTest(value=value):
                stream, reasons = opened()
                stream.observe(delta(value))
                stream.observe({"type": "content_block_stop", "index": 0})
                stream.observe({"type": "message_stop"})
                self.assertEqual(reasons, set())
                self.assertIsNone(stream.active)
                self.assertEqual(stream.observation()["total"], 0)
                self.assertNotIn("PRIVATE_SENTINEL", json.dumps(stream.observation()))

    def test_invalid_estimates_and_other_extensions_still_refuse(self):
        for value in (
            True,
            False,
            "1",
            [],
            {},
            -1,
            0.5,
            9007199254740992,
            10**1000,
            float("nan"),
            float("inf"),
        ):
            stream, reasons = opened()
            stream.observe(delta(value))
            self.assertEqual(reasons, {"unsupported_partial_stream"})
            self.assertEqual(stream.observation()["counts"], {"delta_fields": 1})
        for mutate in (
            lambda e: e["delta"].update(unknown="PRIVATE_SENTINEL"),
            lambda e: e["delta"].pop("thinking"),
            lambda e: e["delta"].update(thinking=1),
            lambda e: e.update(index=1),
            lambda e: e["delta"].update(type="signature_delta", signature="PRIVATE_SENTINEL"),
        ):
            stream, reasons = opened()
            event = delta(None)
            mutate(event)
            stream.observe(event)
            self.assertEqual(reasons, {"unsupported_partial_stream"})
            stream.observe(delta(1))
            self.assertEqual(reasons, {"unsupported_partial_stream"})
            self.assertNotIn("PRIVATE_SENTINEL", json.dumps(stream.observation()))

    def test_non_json_numeric_objects_are_never_rendered_or_admitted(self):
        class HostileInt(int):
            def __repr__(self):
                raise AssertionError("PRIVATE-EXCEPTION")

            def __str__(self):
                raise AssertionError("PRIVATE-EXCEPTION")

            def __le__(self, other):
                raise AssertionError("PRIVATE-EXCEPTION")

        for value in (HostileInt(1), object()):
            stream, reasons = opened()
            stream.observe(delta(value))
            self.assertEqual(reasons, {"unsupported_partial_stream"})
            record = stream.observation()
            self.assertEqual(record["samples"][0]["delta_shape"]["estimated_tokens_class"], "unavailable")
            self.assertNotIn("PRIVATE", json.dumps(record))
        for kind, payload in (
            ("text", {"type": "text_delta", "text": "private"}),
            ("thinking", {"type": "signature_delta", "signature": "private"}),
        ):
            stream, reasons = opened(kind)
            stream.observe(
                {"type": "content_block_delta", "index": 0, "delta": {**payload, "estimated_tokens": None}}
            )
            self.assertEqual(reasons, {"unsupported_partial_stream"})
            self.assertEqual(stream.observation()["counts"], {"delta_fields": 1})

    def test_frozen_v7_keeps_rejecting_extension(self):
        import claude_telemetry_v7_observed as old

        stream = old.PartialStream("model", set())
        stream.observe({"type": "message_start", "message": {"id": "m", "model": "model"}})
        stream.observe({"type": "content_block_start", "index": 0, "content_block": {"type": "thinking"}})
        stream.observe(delta(None))
        self.assertEqual(stream.reasons, {"unsupported_partial_stream"})
        self.assertEqual(stream.observation()["total"], 1)


class CaptureTests(GitFixture):
    def setUp(self):
        super().setUp()
        binding = patch.object(claude_native_auth, "current_binding", return_value=AUTHENTICATION)
        binding.start()
        self.addCleanup(binding.stop)
        self.commit_task()
        self.directory = review.prepare(
            self.repo,
            31,
            12,
            1234,
            review_provider="claude-code",
            reporting={"max_turns": 80, "limits": LIMITS},
        )
        self.packet = self.directory / "packet"
        self.meta = review.verify_packet(self.directory)
        self.policy = self.meta["review_policy"]
        rows = native_events(self.packet, self.packet, "session")
        self.body = " \r\n" + rows[-1]["result"] + "\r\n"
        value = json.loads(self.body)
        reporting_rows = reporting_fixtures.ReportingTests().native_rows()[3:]
        reporting_rows[2]["event"]["delta"]["partial_json"] = self.body
        reporting_rows[3]["message"]["content"][0]["input"] = value
        reporting_rows[-1].update(rows[-1], result=AUXILIARY, structured_output=value)
        rows[0]["tools"].append("StructuredOutput")
        self.rows = rows[:-1] + reporting_rows
        for i, row in enumerate(self.rows):
            row["uuid"] = f"original_{i}"

    def evaluate(self, rows=None):
        return telemetry.capture(
            "\n".join(json.dumps(r) for r in (self.rows if rows is None else rows)),
            self.packet,
            self.packet,
            self.policy,
            "session",
        )

    def test_exact_extension_full_capture_has_no_inspection_or_usage_credit(self):
        baseline = self.evaluate()
        self.assertTrue(
            coverage.assess(self.packet, baseline[0], baseline[1], policy=self.policy)["qualified"]
        )
        events = [
            {"type": "message_start", "message": {"id": "thought", "model": self.policy["model"]}},
            {"type": "content_block_start", "index": 0, "content_block": {"type": "thinking"}},
            *[delta(v) for v in (None, 2, None)],
            {"type": "content_block_stop", "index": 0},
            {"type": "message_stop"},
        ]
        rows = copy.deepcopy(self.rows)
        rows[1:1] = [
            {"type": "stream_event", "session_id": "session", "uuid": f"thought{i}", "event": e}
            for i, e in enumerate(events)
        ]
        body, diagnostic, proof = self.evaluate(rows)
        self.assertEqual(body, baseline[0])
        self.assertEqual(proof, baseline[2])
        self.assertEqual(diagnostic["events"], baseline[1]["events"])
        self.assertEqual(diagnostic["usage"], baseline[1]["usage"])
        self.assertEqual(diagnostic["telemetry"]["partial_stream"]["total"], 0)
        self.assertEqual(diagnostic["schema_version"], 10)
        self.assertTrue(coverage.assess(self.packet, body, diagnostic, policy=self.policy)["qualified"])
        self.assertNotIn("PRIVATE_SENTINEL", json.dumps((diagnostic, proof)))
        # Mutation of payload type still refuses after the estimate-field guard.
        rows[3]["event"]["delta"]["thinking"] = {}
        self.assertIn("unsupported_partial_stream", self.evaluate(rows)[1]["reasons"])

    def test_unaffected_capture_equivalence_and_impossible_shape_refusal(self):
        import claude_reporting_policy as old_policy
        import claude_telemetry_v7_observed as old

        raw = "\n".join(json.dumps(row) for row in self.rows)
        previous = {**self.policy, "adapter": old_policy.ADAPTER}
        foreign = json.dumps(
            {
                "type": "stream_event",
                "session_id": "foreign",
                "parent_tool_use_id": "delegated",
                "event": {"type": "message_start", "message": {"id": "private", "model": "wrong"}},
            }
        )
        for value in (raw, foreign + "\n" + raw, "{\n" + raw, b"\xff", "\ud800", 123):
            for count, size in ((coverage.MAX_EVENTS, coverage.MAX_STREAM_BYTES), (5, 1000)):
                with (
                    patch.object(coverage, "MAX_EVENTS", count),
                    patch.object(coverage, "MAX_STREAM_BYTES", size),
                ):
                    expected = old.capture(value, self.packet, self.packet, previous, "session")
                    body, diagnostics, proof = telemetry.capture(
                        value, self.packet, self.packet, self.policy, "session"
                    )
                    diagnostics.update(schema_version=9, adapter=old_policy.ADAPTER)
                    self.assertEqual((body, diagnostics, proof), expected)
        body, diagnostics, _ = self.evaluate()
        for record in (None, {"schema_version": 1}, {"schema_version": 2}):
            changed = copy.deepcopy(diagnostics)
            if record is None:
                del changed["telemetry"]["partial_stream"]
            else:
                changed["telemetry"]["partial_stream"] = record
            with self.assertRaises(workflow.WorkflowError):
                coverage.assess(self.packet, body, changed, policy=self.policy)
        stream = old.PartialStream("model", set())
        stream.observe({"type": "message_start", "message": {"id": "m", "model": "model"}})
        stream.observe({"type": "content_block_start", "index": 0, "content_block": {"type": "thinking"}})
        stream.observe(delta(None))
        changed = copy.deepcopy(diagnostics["telemetry"])
        changed["partial_stream"] = stream.observation()
        with self.assertRaises(workflow.WorkflowError):
            telemetry.validate_summary(changed)

    # Run existing non-version-specific trust invariants against the current capture.
    test_no_reporting_source_credit = (
        legacy_tests.ClaudeV7Tests.test_successful_reporting_alone_never_earns_inspection
    )
    test_controls = legacy_tests.ClaudeV7Tests.test_full_controls_still_reject_with_valid_reporting
    test_capture_recovery = (
        legacy_tests.ClaudeV7Tests.test_capture_recovery_replays_proof_without_inference_and_binds_auxiliary
    )
    test_proof_mutations = legacy_tests.ClaudeV7Tests.test_policy_and_proof_mutations_fail_closed
    test_missing_report = legacy_tests.ClaudeV7Tests.test_missing_report_is_captured_as_incomplete_without_auxiliary_substitution
    test_foreign_and_unknown = legacy_tests.ClaudeV7Tests.test_foreign_partial_stream_and_forbidden_calls_cannot_hide_behind_projection
    test_all_artifact_bindings = (
        legacy_tests.ClaudeV7Tests.test_each_reporting_artifact_and_bound_capture_is_revalidated
    )


class GuardTests(unittest.TestCase):
    def test_every_guard_has_a_distinct_safe_classification_and_preserves_state(self):
        import copy

        import claude_partial_observation as observation

        start = {"type": "message_start", "message": {"id": "m", "model": "model"}}
        block = {"type": "content_block_start", "index": 0, "content_block": {"type": "text"}}
        stop = {"type": "content_block_stop", "index": 0}
        end = {"type": "message_stop"}
        tool = {"type": "tool_use", "id": "t", "name": "Read", "input": {}}
        cases = [
            ("event_object", [], None),
            ("message_inactive", [start], start),
            ("message_object", [], {"type": "message_start", "message": []}),
            ("message_id", [], {"type": "message_start", "message": {"id": [], "model": "model"}}),
            ("message_unique", [start, end], start),
            ("message_model", [], {"type": "message_start", "message": {"id": "m", "model": None}}),
            ("message_active", [], end),
            ("event_kind", [start], {"type": "unrecognized-private-kind"}),
            ("block_index", [start], {**block, "index": True}),
            ("block_unique", [start, block, stop], block),
            ("block_object", [start], {**block, "content_block": []}),
            ("tool_name", [start], {**block, "content_block": {**tool, "name": "Bash"}}),
            ("tool_id", [start], {**block, "content_block": {**tool, "id": ""}}),
            ("tool_input", [start], {**block, "content_block": {**tool, "input": {"secret": "secret"}}}),
            ("block_kind", [start], {**block, "content_block": {"type": "unknown-secret"}}),
            ("block_open", [start], stop),
            ("delta_object", [start, block], {"type": "content_block_delta", "index": 0, "delta": []}),
            (
                "delta_kind",
                [start, block],
                {"type": "content_block_delta", "index": 0, "delta": {"type": "unknown-secret"}},
            ),
            (
                "delta_fields",
                [start, block],
                {
                    "type": "content_block_delta",
                    "index": 0,
                    "delta": {"type": "text_delta", "text": "secret", "secret": "secret"},
                },
            ),
            (
                "delta_text",
                [start, block],
                {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": 123}},
            ),
            ("message_delta_object", [start], {"type": "message_delta", "delta": [], "usage": {}}),
            ("message_usage_object", [start], {"type": "message_delta", "delta": {}, "usage": []}),
            ("message_blocks_closed", [start, block], end),
        ]
        self.assertEqual({row[0] for row in cases}, observation.PREDICATES)
        for predicate, prefix, rejected in cases:
            with self.subTest(predicate=predicate):
                reasons = set()
                stream = telemetry.PartialStream("model", reasons)
                for event in prefix:
                    stream.observe(event)
                self.assertEqual(reasons, set())
                before = copy.deepcopy((stream.active, stream.messages, stream.blocks))
                stream.observe(rejected)
                self.assertEqual((stream.active, stream.messages, stream.blocks), before)
                value = stream.observation()
                self.assertEqual(value["counts"], {predicate: 1})
                self.assertEqual(value["samples"][0]["predicate"], predicate)
                self.assertFalse(value["samples"][0]["evaluation_error"])
                self.assertEqual(value["samples"][0]["message_active"], stream.active is not None)
                self.assertEqual(reasons, {"unsupported_partial_stream"})
                telemetry.partial_observation.validate(value, len(prefix) + 1)
                self.assertNotIn("secret", str(value))
                # Invalid events don't reset framing or prevent a later valid message.
                if stream.active is not None:
                    for index, kind in list(stream.blocks.items()):
                        if kind is not None:
                            stream.observe({"type": "content_block_stop", "index": index})
                    stream.observe(end)
                stream.observe({"type": "message_start", "message": {"id": "next", "model": "model"}})
                stream.observe(end)
                self.assertIsNone(stream.active)
                self.assertEqual(stream.observation(), value)

    def test_unhashable_shapes_are_classified_at_the_actual_evaluation_boundary(self):
        start = {"type": "message_start", "message": {"id": "m", "model": "model"}}
        block = {"type": "content_block_start", "index": 0, "content_block": {"type": "text"}}
        cases = [
            ("event_kind", [], {"type": []}),
            ("block_kind", [], {**block, "content_block": {"type": {}}}),
            ("delta_kind", [block], {"type": "content_block_delta", "index": 0, "delta": {"type": []}}),
        ]
        for predicate, prefix, event in cases:
            with self.subTest(predicate=predicate):
                reasons = set()
                stream = telemetry.PartialStream("model", reasons)
                for row in [start, *prefix, event]:
                    stream.observe(row)
                self.assertEqual(stream.observation()["counts"], {predicate: 1})
                self.assertTrue(stream.observation()["samples"][0]["evaluation_error"])
                self.assertEqual(reasons, {"unsupported_partial_stream"})

    def test_valid_text_thinking_redacted_and_tool_blocks_remain_supported(self):
        stream = telemetry.PartialStream("model", set())
        stream.observe({"type": "message_start", "message": {"id": "m", "model": "model"}})
        for index, (block, deltas) in enumerate(
            [
                ({"type": "text"}, [{"type": "text_delta", "text": "\ud800private"}]),
                (
                    {"type": "thinking"},
                    [
                        {"type": "thinking_delta", "thinking": "private"},
                        {"type": "signature_delta", "signature": "private"},
                    ],
                ),
                ({"type": "redacted_thinking"}, []),
                (
                    {"type": "tool_use", "name": "Read", "id": "t", "input": {}},
                    [{"type": "input_json_delta", "partial_json": "private"}],
                ),
            ]
        ):
            stream.observe({"type": "content_block_start", "index": index, "content_block": block})
            for delta in deltas:
                stream.observe({"type": "content_block_delta", "index": index, "delta": delta})
            stream.observe({"type": "content_block_stop", "index": index})
        stream.observe({"type": "message_delta", "delta": {}, "usage": {}})
        stream.observe({"type": "message_stop"})
        self.assertEqual(stream.reasons, set())
        self.assertEqual(stream.observation()["total"], 0)
        self.assertIsNone(stream.active)

    def test_observation_bounds_privacy_and_snapshot_copy(self):
        import json

        import claude_partial_observation as observation

        stream = telemetry.PartialStream("model", set())
        secret = "\ud800\u202ePRIVATE-token/path/header/prompt/signature-" * 10000
        huge = {secret: [float("nan"), float("inf"), {"private": secret}]}
        for _ in range(observation.MAX_REJECTIONS + 5):
            stream.observe({"type": "message_start", "message": {"id": [], "model": huge}, secret: huge})
        record = stream.observation()
        telemetry.partial_observation.validate(record, observation.MAX_REJECTIONS)
        self.assertEqual(record["total"], observation.MAX_REJECTIONS)
        self.assertTrue(record["count_saturated"])
        self.assertTrue(record["samples_overflow"])
        self.assertEqual(len(record["samples"]), observation.MAX_SAMPLES)
        encoded = json.dumps(record, allow_nan=False)
        self.assertLess(len(encoded), 10000)
        for text in ("PRIVATE", "private", "token", "header", "signature", "\\ud800", "\\u202e"):
            self.assertNotIn(text, encoded)
        record["counts"].clear()
        record["samples"][0]["predicate"] = "mutated"
        self.assertEqual(stream.observation()["counts"], {"message_id": observation.MAX_REJECTIONS})
        self.assertEqual(stream.observation()["samples"][0]["predicate"], "message_id")
