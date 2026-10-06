"""Content-free delta classification; frozen acceptance remains authoritative."""

import copy
import json
import unittest

import claude_telemetry_v7_observed as telemetry


def rejected(delta):
    reasons = set()
    stream = telemetry.PartialStream("model", reasons)
    stream.observe({"type": "message_start", "message": {"id": "m", "model": "model"}})
    stream.observe({"type": "content_block_start", "index": 0, "content_block": {"type": "thinking"}})
    stream.observe({"type": "content_block_delta", "index": 0, "delta": delta})
    return stream, reasons


class StructuralObservationTests(unittest.TestCase):
    def test_estimated_extension_and_unknown_extra_are_distinct_but_both_refuse(self):
        observations = []
        for extra in ({"estimated_tokens": None}, {"private-extra": "private-value"}):
            stream, reasons = rejected({"type": "thinking_delta", "thinking": "private-thought", **extra})
            self.assertEqual(reasons, {"unsupported_partial_stream"})
            record = stream.observation()
            self.assertEqual(record["counts"], {"delta_fields": 1})
            self.assertEqual(record["schema_version"], 2)
            observations.append(record["samples"][0]["delta_shape"])
            self.assertNotIn("private", json.dumps(record))
        self.assertEqual(observations[0]["estimated_tokens_class"], "null")
        self.assertEqual(observations[0]["other_fields"], 0)
        self.assertEqual(observations[1]["estimated_tokens_class"], "absent")
        self.assertEqual(observations[1]["other_fields"], 1)

    def test_types_bounds_privacy_and_incomplete_overflow(self):
        import claude_partial_observation_v2 as codec

        cases = [
            (None, "null"),
            (True, "boolean"),
            ("sensitive", "string"),
            (["sensitive"], "array"),
            ({"sensitive": "secret"}, "object"),
            (float("nan"), "nonfinite"),
            (float("inf"), "nonfinite"),
            (-1, "negative"),
            (0.5, "fractional"),
            (0, "integer_safe"),
            (9007199254740991, "integer_safe"),
            (10**1000, "above_safe"),
        ]
        for value, expected in cases:
            stream, reasons = rejected(
                {"type": "thinking_delta", "thinking": "secret", "estimated_tokens": value}
            )
            record = stream.observation()
            codec.validate(record, 3)
            self.assertEqual(record["samples"][0]["delta_shape"]["estimated_tokens_class"], expected)
            self.assertEqual(reasons, {"unsupported_partial_stream"})
            self.assertNotIn("sensitive", json.dumps(record))
        stream, _ = rejected({"type": "thinking_delta", "estimated_tokens": None})
        self.assertFalse(stream.observation()["samples"][0]["delta_shape"]["payload_present"])
        delta = {
            "type": "thinking_delta",
            "thinking": "secret",
            **{f"\ud800secret{i}": "secret" for i in range(10000)},
        }
        for _ in range(20005):
            stream.observe({"type": "content_block_delta", "index": 0, "delta": delta})
        record = stream.observation()
        codec.validate(record, 20000)
        self.assertTrue(record["samples_overflow"])
        self.assertTrue(record["count_saturated"])
        self.assertTrue(record["samples"][1]["delta_shape"]["other_fields_overflow"])
        self.assertEqual(record["samples"][1]["delta_shape"]["other_fields"], 8)
        raw = json.dumps(record, sort_keys=True, separators=(",", ":")).encode()
        self.assertLess(len(raw), codec.MAX_BYTES)
        self.assertNotIn(b"secret", raw)
        record["samples"][0]["delta_shape"]["block_kind"] = "secret"
        self.assertEqual(stream.observation()["samples"][0]["delta_shape"]["block_kind"], "thinking")

    def test_strict_tamper_validation_and_old_schema_retains_meaning(self):
        import claude_partial_observation_v2 as codec
        import claude_telemetry_v7_observed_v1 as old
        from workflow import WorkflowError

        stream, _ = rejected({"type": "thinking_delta", "thinking": "secret", "estimated_tokens": 1})
        record = stream.observation()
        codec.validate(record, 3)
        for key, value in (
            ("block_kind", "text"),
            ("delta_kind", []),
            ("type_present", False),
            ("payload_present", 1),
            ("other_fields", True),
            ("other_fields", 9),
            ("other_fields_overflow", True),
            ("estimated_tokens_class", "absent"),
            ("private", "secret"),
        ):
            changed = copy.deepcopy(record)
            changed["samples"][0]["delta_shape"][key] = value
            with self.subTest(key=key, value=value), self.assertRaises(WorkflowError):
                codec.validate(changed, 3)
        changed = copy.deepcopy(record)
        changed["samples"][0]["delta_shape"].update(
            estimated_tokens_present=False, estimated_tokens_class="absent"
        )
        with self.assertRaises(WorkflowError):
            codec.validate(changed, 3)
        for version in (True, 0, 3, "2"):
            with self.assertRaises(WorkflowError):
                codec.validate({**record, "schema_version": version}, 3)
        original = old.PartialStream("model", set())
        original.observe(None)
        before = original.observation()
        codec.validate(before, 1)
        self.assertEqual(before, original.observation())

    def test_current_all_guard_state_and_capture_equivalence_includes_extension(self):
        import claude_telemetry_v7 as frozen

        for delta in (
            {"type": "thinking_delta", "thinking": "x"},
            {"type": "thinking_delta", "thinking": "x", "estimated_tokens": None},
            {"type": "secret"},
            [],
            {"type": []},
        ):
            current, reasons = rejected(delta)
            original_reasons = set()
            original = frozen.PartialStream("model", original_reasons)
            for event in [
                {"type": "message_start", "message": {"id": "m", "model": "model"}},
                {"type": "content_block_start", "index": 0, "content_block": {"type": "thinking"}},
                {"type": "content_block_delta", "index": 0, "delta": delta},
                {"type": "content_block_stop", "index": 0},
                {"type": "message_stop"},
            ]:
                original.observe(event)
            current.observe({"type": "content_block_stop", "index": 0})
            current.observe({"type": "message_stop"})
            self.assertEqual(reasons, original_reasons)
            self.assertEqual(
                (current.active, current.messages, current.blocks),
                (original.active, original.messages, original.blocks),
            )

    def test_hostile_values_are_unavailable_without_rendering_or_traversal(self):
        import claude_partial_observation_v2 as codec

        class Hostile:
            def __repr__(self):
                raise AssertionError("private representation must not execute")

            def __str__(self):
                raise AssertionError("private rendering must not execute")

        class HostileDict(dict):
            def __iter__(self):
                raise AssertionError("private iteration must not execute")

        for value in (Hostile(), HostileDict(secret=Hostile())):
            stream, reasons = rejected(
                {"type": "thinking_delta", "thinking": "secret", "estimated_tokens": value}
            )
            record = stream.observation()
            codec.validate(record, 3)
            self.assertEqual(record["samples"][0]["delta_shape"]["estimated_tokens_class"], "unavailable")
            self.assertEqual(reasons, {"unsupported_partial_stream"})
            self.assertNotIn("secret", json.dumps(record))
