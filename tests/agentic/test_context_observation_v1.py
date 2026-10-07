"""Synthetic protocol steps; no native qualification, account or provider access."""

import copy
import hashlib
import json
import unittest

import claude_context_observation_v1 as observation
from workflow import WorkflowError


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def bindings():
    result = {key: hashlib.sha256(key.encode()).hexdigest() for key in observation.BINDINGS}
    result["descriptor_sha256"] = hashlib.sha256(canonical(observation.DESCRIPTOR)).hexdigest()
    return result


def assistant(ordinal, identity, values):
    return {
        "ordinal": ordinal,
        "kind": "assistant",
        "message": {
            "id": identity,
            "content": [{"type": "text", "text": "synthetic private content"}],
            "usage": dict(zip(observation.COUNTERS, values, strict=True)),
        },
    }


def fixture():
    first = assistant(1, "synthetic-first", (10, 20, 30))
    duplicate = copy.deepcopy(first)
    duplicate["ordinal"] = 2
    return [
        first,
        duplicate,
        {"ordinal": 3, "kind": "mandatory_read"},
        assistant(4, "synthetic-final", (11, 21, 31)),
        {"ordinal": 5, "kind": "structured_output"},
        {"ordinal": 6, "kind": "terminal_success"},
    ]


def counters():
    return {
        "model_steps_observed": 2,
        "assistant_input_tokens_observed": 21,
        "assistant_cache_read_input_tokens_observed": 41,
        "assistant_cache_creation_input_tokens_observed": 61,
    }


def correlation():
    return {"event_count": 6, "mandatory_read_ordinals": [3], "output_ordinal": 5}


def sealed():
    reducer = observation.Observer()
    for event in fixture():
        reducer.feed(event)
    return reducer.seal(bindings(), counters(), correlation())


class ContextObservationTests(unittest.TestCase):
    def test_maximum_cache_sums_dedup_and_private_content(self):
        raw = sealed()
        summary = observation.replay(raw, bindings(), counters(), correlation())
        self.assertEqual(summary["response_count"], 2)
        self.assertEqual(summary["sums"], dict(zip(observation.COUNTERS, (21, 41, 61), strict=True)))
        self.assertEqual(summary["max_observed_input"], 63)
        self.assertEqual(summary["final_observed_input"], 63)
        self.assertNotEqual(summary["max_observed_input"], sum(summary["sums"].values()))
        document = json.loads(raw)
        self.assertEqual([row["ordinal"] for row in document["responses"]], [1, 4])
        for forbidden in (b"synthetic", b"content", b"message", b"session", b"output_tokens"):
            self.assertNotIn(forbidden, raw)
        self.assertLessEqual(len(raw), 65536)

    def test_conflicting_identity_including_same_counters_different_content(self):
        for key in ("usage", "content"):
            with self.subTest(key=key):
                events = fixture()
                if key == "usage":
                    events[1]["message"][key]["input_tokens"] += 1
                else:
                    events[1]["message"][key][0]["text"] = "different"
                reducer = observation.Observer()
                reducer.feed(events[0])
                with self.assertRaisesRegex(
                    WorkflowError, "^Incomplete or inconsistent context observation$"
                ):
                    reducer.feed(events[1])
                # A caught error cannot be repaired into a successful observation.
                with self.assertRaises(WorkflowError):
                    reducer.feed(fixture()[1])
                with self.assertRaises(WorkflowError):
                    reducer.seal(bindings(), counters(), correlation())

    def test_missing_unknown_types_and_safe_integer_bounds(self):
        bad_values = [None, True, False, 1.0, "1", -1, observation.SAFE_INTEGER + 1]
        for field in observation.COUNTERS:
            for value in bad_values:
                with self.subTest(field=field, value=value):
                    event = fixture()[0]
                    event["message"]["usage"][field] = value
                    with self.assertRaises(WorkflowError):
                        observation.Observer().feed(event)
            event = fixture()[0]
            del event["message"]["usage"][field]
            with self.assertRaises(WorkflowError):
                observation.Observer().feed(event)
        for mutation in (
            lambda e: e["message"]["usage"].update(unknown=0),
            lambda e: e["message"].update(id=""),
            lambda e: e.update(ordinal=True),
            lambda e: e.update(extra=0),
            lambda e: e["message"]["usage"].update(output_tokens=True),
        ):
            event = fixture()[0]
            mutation(event)
            with self.assertRaises(WorkflowError):
                observation.Observer().feed(event)
        reducer = observation.Observer()
        reducer.feed(assistant(1, "safe", (observation.SAFE_INTEGER, 0, 0)))
        with self.assertRaises(WorkflowError):
            observation.Observer().feed(assistant(1, "overflow", (observation.SAFE_INTEGER, 1, 0)))

    def test_response_limit_and_duplicate_does_not_consume_slot(self):
        reducer = observation.Observer()
        for index in range(400):
            reducer.feed(assistant(index + 1, str(index), (1, 0, 0)))
        duplicate = assistant(401, "399", (1, 0, 0))
        reducer.feed(duplicate)
        with self.assertRaises(WorkflowError):
            reducer.feed(assistant(402, "new", (1, 0, 0)))

    def test_full_response_envelope_and_oversized_correlation_refusal(self):
        reducer = observation.Observer()
        reducer.feed({"ordinal": 1, "kind": "mandatory_read"})
        for index in range(400):
            reducer.feed(assistant(index + 2, str(index), (index, 1, 2)))
        reducer.feed({"ordinal": 402, "kind": "structured_output"})
        reducer.feed({"ordinal": 403, "kind": "terminal_success"})
        aggregate = {
            "model_steps_observed": 400,
            "assistant_input_tokens_observed": sum(range(400)),
            "assistant_cache_read_input_tokens_observed": 400,
            "assistant_cache_creation_input_tokens_observed": 800,
        }
        link = {"event_count": 403, "mandatory_read_ordinals": [1], "output_ordinal": 402}
        raw = reducer.seal(bindings(), aggregate, link)
        self.assertLessEqual(len(raw), 65536)
        self.assertEqual(observation.replay(raw, bindings(), aggregate, link)["response_count"], 400)
        reducer = observation.Observer()
        for ordinal in range(1, 15001):
            reducer.feed({"ordinal": ordinal, "kind": "mandatory_read"})
        reducer.feed(assistant(15001, "last", (1, 0, 0)))
        reducer.feed({"ordinal": 15002, "kind": "structured_output"})
        reducer.feed({"ordinal": 15003, "kind": "terminal_success"})
        aggregate = {
            "model_steps_observed": 1,
            "assistant_input_tokens_observed": 1,
            "assistant_cache_read_input_tokens_observed": 0,
            "assistant_cache_creation_input_tokens_observed": 0,
        }
        link = {
            "event_count": 15003,
            "mandatory_read_ordinals": list(range(1, 15001)),
            "output_ordinal": 15002,
        }
        with self.assertRaises(WorkflowError):
            reducer.seal(bindings(), aggregate, link)

    def test_gaps_reordering_decrease_compaction_and_terminal_failure(self):
        for kind in ("compaction", "dropped", "unsupported", "terminal_failure", "quota_exhausted"):
            with self.subTest(kind=kind), self.assertRaises(WorkflowError):
                observation.Observer().feed({"ordinal": 1, "kind": kind})
        for ordinal in (0, 2, 20001):
            with self.assertRaises(WorkflowError):
                observation.Observer().feed({"ordinal": ordinal, "kind": "other"})
        reducer = observation.Observer()
        reducer.feed(fixture()[0])
        with self.assertRaises(WorkflowError):
            reducer.feed(assistant(2, "decrease", (10, 19, 30)))
        reducer = observation.Observer()
        reducer.feed(fixture()[0])
        with self.assertRaises(WorkflowError):
            reducer.feed(fixture()[0])
        with self.assertRaises(WorkflowError):
            observation.Observer().feed({"ordinal": 1, "kind": "terminal_success"})

    def test_correlation_requires_new_response_after_last_mandatory_read(self):
        # Replaying an earlier duplicate after a Read cannot supply a new response.
        events = fixture()
        events[3] = copy.deepcopy(events[0])
        events[3]["ordinal"] = 4
        reducer = observation.Observer()
        for event in events:
            reducer.feed(event)
        one = {
            "model_steps_observed": 1,
            **{f"assistant_{k}_observed": v for k, v in zip(observation.COUNTERS, (10, 20, 30), strict=True)},
        }
        with self.assertRaises(WorkflowError):
            reducer.seal(bindings(), one, correlation())
        raw = sealed()
        for field, value in (
            ("event_count", 7),
            ("mandatory_read_ordinals", []),
            ("mandatory_read_ordinals", [4]),
            ("output_ordinal", 4),
        ):
            expected = correlation()
            expected[field] = value
            with self.subTest(field=field, value=value), self.assertRaises(WorkflowError):
                observation.replay(raw, bindings(), counters(), expected)
        reducer = observation.Observer()
        for event in fixture()[:-1]:
            reducer.feed(event)
        with self.assertRaises(WorkflowError):
            reducer.seal(bindings(), counters(), correlation())
        reducer.feed(fixture()[-1])
        with self.assertRaises(WorkflowError):
            reducer.feed({"ordinal": 7, "kind": "other"})

    def test_every_binding_and_diagnostic_sum_is_independently_checked(self):
        raw = sealed()
        for key in observation.BINDINGS:
            changed = bindings()
            changed[key] = "f" * 64
            with self.subTest(binding=key), self.assertRaises(WorkflowError):
                observation.replay(raw, changed, counters(), correlation())
        for key in counters():
            for value in (None, True, counters()[key] + 1):
                changed = counters()
                changed[key] = value
                with self.subTest(counter=key, value=value), self.assertRaises(WorkflowError):
                    observation.replay(raw, bindings(), changed, correlation())
        for value in ({}, {**bindings(), "extra_sha256": "0" * 64}):
            with self.assertRaises(WorkflowError):
                observation.replay(raw, value, counters(), correlation())

    def test_torn_noncanonical_unknown_and_numeric_tampering(self):
        raw = sealed()
        for bad in (
            raw[:-1],
            raw + b"\n",
            b"{}",
            b"x" * 65537,
            raw.decode(),
            b'{"a":1,"a":2}',
            b"[" * 2000 + b"]" * 2000,
        ):
            with self.assertRaises(WorkflowError):
                observation.replay(bad, bindings(), counters(), correlation())
        for mutate in (
            lambda d: d.update(schema_version=True),
            lambda d: d.update(extra=0),
            lambda d: d["responses"][0].update(ordinal=4),
            lambda d: d["responses"][0].update(input_tokens=999),
            lambda d: d["summary"].update(max_observed_input=123),
            lambda d: d["responses"].reverse(),
            lambda d: d["correlation"].update(extra=0),
        ):
            document = json.loads(raw)
            mutate(document)
            with self.assertRaises(WorkflowError):
                observation.replay(canonical(document), bindings(), counters(), correlation())

    def test_later_capture_completion_binding(self):
        raw = sealed()
        capture = hashlib.sha256(b"synthetic capture").hexdigest()
        receipt = observation.completion(raw, capture)
        self.assertEqual(
            observation.replay_completed(raw, receipt, capture, bindings(), counters(), correlation())[
                "max_observed_input"
            ],
            63,
        )
        for key in receipt:
            changed = receipt.copy()
            changed[key] = True if key == "schema_version" else "0" * 64
            with self.subTest(key=key), self.assertRaises(WorkflowError):
                observation.replay_completed(raw, changed, capture, bindings(), counters(), correlation())
        with self.assertRaises(WorkflowError):
            observation.replay_completed(raw, receipt, "f" * 64, bindings(), counters(), correlation())
