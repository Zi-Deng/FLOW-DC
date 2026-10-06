"""Safe rejected-stream classification, never provider qualification."""

import unittest

import claude_telemetry_v7_observed as telemetry


class PartialObservationTests(unittest.TestCase):
    def test_rejected_event_identifies_guard_without_retaining_payload(self):
        reasons = set()
        stream = telemetry.PartialStream("model", reasons)
        stream.observe({"type": "message_start", "message": {"id": "private-id", "model": "wrong-secret"}})
        observed = stream.observation()
        self.assertEqual(observed["counts"], {"message_model": 1})
        self.assertEqual(reasons, {"unsupported_partial_stream"})
        self.assertNotIn("wrong-secret", str(observed))
        self.assertNotIn("private-id", str(observed))

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
                observation.validate(value, len(prefix) + 1)
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
        observation.validate(record, observation.MAX_REJECTIONS)
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

    def test_nested_record_refuses_tampering_and_unbounded_fields(self):
        import copy

        import claude_partial_observation as observation
        from workflow import WorkflowError

        stream = telemetry.PartialStream("model", set())
        stream.observe(None)
        value = stream.observation()
        variants = []
        for key, replacement in [
            ("schema_version", True),
            ("schema_version", 2),
            ("total", True),
            ("total", -1),
            ("total", float("nan")),
            ("counts", {"private": 1}),
            ("counts", {"event_object": True}),
            ("counts", {"event_object": observation.MAX_REJECTIONS + 1}),
            ("counts", {}),
            ("samples", value["samples"] * 33),
            ("samples_overflow", True),
            ("count_saturated", True),
            ("private", "private"),
        ]:
            variants.append({**copy.deepcopy(value), key: replacement})
        for key, replacement in [
            ("predicate", []),
            ("predicate", "private"),
            ("evaluation_error", 1),
            ("index_open", True),
            ("index_known", True),
            ("private", "private"),
        ]:
            changed = copy.deepcopy(value)
            changed["samples"][0][key] = replacement
            variants.append(changed)
        for changed in variants:
            with self.subTest(changed=changed):
                with self.assertRaises(WorkflowError):
                    observation.validate(changed, 1)
        with self.assertRaises(WorkflowError):
            observation.validate(value, 0)

    def test_conditional_native_thinking_extension_stays_rejected_not_assumed_historical(self):
        # The pinned coalescer may add estimated_tokens. That static possibility
        # does not identify the unretained actual purpose-13 shape or authorize it.
        stream = telemetry.PartialStream("model", set())
        stream.observe({"type": "message_start", "message": {"id": "m", "model": "model"}})
        stream.observe({"type": "content_block_start", "index": 0, "content_block": {"type": "thinking"}})
        stream.observe(
            {
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "thinking_delta", "thinking": "private", "estimated_tokens": None},
            }
        )
        self.assertEqual(stream.reasons, {"unsupported_partial_stream"})
        self.assertEqual(stream.observation()["counts"], {"delta_fields": 1})
        self.assertNotIn("private", str(stream.observation()))
        self.assertNotIn("estimated_tokens", str(stream.observation()))
