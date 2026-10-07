"""Public synthetic fixtures and frozen native parser checks; no live authority."""

import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import claude_context_observation_v1 as observation
import claude_reporting_policy_v8
import claude_telemetry_v8
import diagnostic_tool_contract
import reporting_activation_v4
import reporting_diagnostic_v2
import review_capacity_native_v1 as capacity
import review_coverage
import review_policy
import review_report_material_v1 as material
from claude_fixtures import AUTHENTICATION, native_events
from workflow import WorkflowError


def policy():
    return claude_reporting_policy_v8.build(
        {**review_policy.policy(review_policy.choices("claude-code"), {}), "authentication": AUTHENTICATION},
        max_turns=400,
        limits={
            "events": 20000,
            "fragment_bytes": 10000,
            "proof_bytes": 2000000,
            "report_bytes": 10000,
            "terminal_bytes": 60000,
        },
    )


def additional():
    return [{"id": "a" * 24, "artifact": "guidance.txt", "start_line": 1, "end_line": 1}], {
        "guidance.txt": b"Public guidance\n"
    }


def catalog():
    return [
        {
            "id": "component-a",
            "items": [{"id": "b" * 24, "artifact": "source.txt", "start_line": 1, "end_line": 1}],
            "files": {"source.txt": b"Public source\n"},
        }
    ]


def stream(packet):
    rows = native_events(packet, packet, "synthetic-session")
    rows[0]["tools"].append("StructuredOutput")
    terminal = rows.pop()
    body = terminal["result"]
    value = json.loads(body)
    rows.append(
        {
            "type": "assistant",
            "session_id": "synthetic-session",
            "message": {
                "model": "claude-opus-5-5",
                "content": [{"type": "text", "text": "Public synthetic progress"}],
            },
        }
    )
    rows.extend(
        [
            {
                "type": "stream_event",
                "event": {
                    "type": "message_start",
                    "message": {"id": "report-message", "model": "claude-opus-5-5"},
                },
            },
            {
                "type": "stream_event",
                "event": {
                    "type": "content_block_start",
                    "index": 0,
                    "content_block": {
                        "type": "tool_use",
                        "id": "report-tool",
                        "name": "StructuredOutput",
                        "input": {},
                    },
                },
            },
            {
                "type": "stream_event",
                "event": {
                    "type": "content_block_delta",
                    "index": 0,
                    "delta": {"type": "input_json_delta", "partial_json": body},
                },
            },
            {
                "type": "assistant",
                "message": {
                    "id": "report-message",
                    "model": "claude-opus-5-5",
                    "content": [
                        {"type": "tool_use", "id": "report-tool", "name": "StructuredOutput", "input": value}
                    ],
                },
            },
            {"type": "stream_event", "event": {"type": "content_block_stop", "index": 0}},
            {"type": "stream_event", "event": {"type": "message_stop"}},
            {
                "type": "user",
                "message": {
                    "content": [
                        {
                            "type": "tool_result",
                            "tool_use_id": "report-tool",
                            "content": "Structured output provided successfully",
                            "is_error": False,
                        }
                    ]
                },
            },
            {**terminal, "result": "Public synthetic auxiliary", "structured_output": value},
        ]
    )
    count = 0
    for i, row in enumerate(rows):
        row.update(uuid=f"synthetic-event-{i}", session_id="synthetic-session")
        if row["type"] == "assistant":
            count += 1
            message = row["message"]
            message.setdefault("id", f"synthetic-message-{i}")
            message["usage"] = {
                "input_tokens": 100 + count,
                "cache_read_input_tokens": 20,
                "cache_creation_input_tokens": 30,
                "output_tokens": 5,
                "service_tier": "standard",
            }
            for block in message["content"]:
                if block.get("name") == "Grep":
                    block["input"] = copy.deepcopy(diagnostic_tool_contract.GREP)
    return rows


def raw(rows):
    return b"\n".join(capacity.encoded(row) for row in rows)


def bindings(packet, rows):
    body, diagnostics, proof = claude_telemetry_v8.capture(
        raw(rows),
        packet,
        packet,
        policy(),
        "synthetic-session",
        diagnostic_purpose="native-tools-and-source",
        diagnostic_tool_contract=diagnostic_tool_contract.contract(),
    )
    result = {k: hashlib.sha256(k.encode()).hexdigest() for k in observation.BINDINGS}
    for key, value in {
        "policy": capacity.encoded(policy()),
        "report": body.encode(),
        "proof": capacity.encoded(proof),
        "diagnostic": capacity.encoded(diagnostics),
        "descriptor": observation._bytes(observation.DESCRIPTOR),
    }.items():
        result[key + "_sha256"] = capacity.sha(value)
    return result, diagnostics, proof


class CapacityNativeTests(unittest.TestCase):
    def test_frozen_budget_refusal_and_closed_capacity_profile(self):
        current = policy()
        with self.assertRaises(WorkflowError):
            reporting_activation_v4.validate_policy(current)
        self.assertEqual(capacity.profile(current)["schema_version"], 1)
        for mutate in (
            lambda p: p["budget"].update(estimated_usd=9),
            lambda p: p.update(max_turns=399),
            lambda p: p.update(model="other"),
            lambda p: p["budget"].update(extra_spend_authorized_usd=1),
        ):
            changed = copy.deepcopy(current)
            mutate(changed)
            with self.assertRaises(WorkflowError):
                capacity.profile(changed)

    def test_closed_transport_identity_confers_no_authority(self):
        for number in (22, 23):
            claim = {
                "schema_version": 6,
                "number": number,
                "purpose": capacity.PURPOSES[number],
                "fixture_sha256": "a" * 64,
                "descriptor_sha256": capacity.sha(capacity.encoded(capacity.DESCRIPTOR)),
                "native_seconds": 900,
                "reference_usd": 10,
            }
            self.assertEqual(capacity.transport(number, claim, "a" * 64), "native-tools-and-source")
            for key in claim:
                changed = {**claim, key: None}
                with self.assertRaises(WorkflowError):
                    capacity.transport(number, changed, "a" * 64)
        for number in (True, 20, 21, 24):
            with self.assertRaises(WorkflowError):
                capacity.transport(number, claim, "a" * 64)

    def test_all_report_layouts_are_exact_deterministic_and_lossless(self):
        first = capacity.reports()
        self.assertEqual(first, capacity.reports())
        self.assertEqual(len(first), 48)
        for layout in capacity.LAYOUTS:
            selected = [r for r in first if r[0] == layout]
            self.assertEqual(len(selected), 6)
            for _, document, projection in selected:
                self.assertEqual(len(document), 10000)
                self.assertEqual(material.reconstruct(projection), document)
                self.assertEqual(len(json.loads(document)["reviewed"]), 1 if layout == "multiline" else 128)
                self.assertLessEqual(projection.count(b"\n"), 79)
        self.assertGreater(first[0][1].count(b"\n"), 9000)

    def test_largest_fixture_ranges_supplement_and_stress(self):
        items, files = additional()
        fixture = capacity.largest_fixture(catalog(), items, files, {"source": "c" * 64})
        self.assertEqual(fixture, capacity.largest_fixture(catalog(), items, files, {"source": "c" * 64}))
        self.assertEqual(fixture["files"]["source.txt"], b"Public source\n")
        self.assertEqual(len(fixture["files"]["capacity/supplement.txt"]) + len(b"Public source\n"), 500000)
        self.assertEqual(fixture["files"]["capacity/supplement.txt"].count(b"\n") + 1, 9000)
        for label in ("optional", "navigation"):
            self.assertEqual(len(fixture["files"][f"capacity/{label}.txt"]), 100000)
        capacity.verify_fixture(
            fixture, capacity.largest_fixture(catalog(), items, files, {"source": "c" * 64})
        )

    def test_fixture_bounds_and_independent_mutations(self):
        items, files = additional()
        expected = capacity.integration_fixture(items, files, {"source": "c" * 64})
        self.assertEqual(sum(p.startswith("capacity/reports/") for p in expected["files"]), 48)
        self.assertEqual(sum(p.startswith("capacity/projections/") for p in expected["files"]), 48)
        for mutate in (
            lambda f: f["manifest"]["dependencies"].update(source="d" * 64),
            lambda f: f["manifest"]["items"].pop(),
            lambda f: f["files"].update({"guidance.txt": b"changed"}),
            lambda f: f.update(sha256="d" * 64),
        ):
            changed = copy.deepcopy(expected)
            mutate(changed)
            with self.assertRaises(WorkflowError):
                capacity.verify_fixture(changed, expected)
        with self.assertRaises(ValueError):
            capacity.integration_fixture(
                items, files, {"source": "c" * 64}, existing_projection_bytes=2000000
            )
        for changed in ([], catalog() * 49, [{**catalog()[0], "files": {"source.txt": b"x" * 500001}}]):
            with self.assertRaises(WorkflowError):
                capacity.largest_fixture(changed, items, files, {"source": "c" * 64})

    def test_exact_arithmetic_thresholds(self):
        self.assertEqual(capacity.estimate({22: 448000, 23: 1})["planned_total"], 1000000)
        self.assertEqual(capacity.estimate({22: 1, 23: 0})["planning_input"], 200002)
        for values in (
            {22: 448001, 23: 1},
            {22: True, 23: 1},
            {22: 1.0, 23: 1},
            {22: -1, 23: 1},
            {22: 1},
            {22: 1, 23: 1, 24: 1},
        ):
            with self.assertRaises(WorkflowError):
                capacity.estimate(values)

    def test_frozen_stream_numeric_bridge_and_completion_integrity(self):
        with tempfile.TemporaryDirectory() as tmp:
            packet = Path(tmp)
            for path, content in reporting_diagnostic_v2.packet_contents().items():
                target = packet / path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(content)
            rows = stream(packet)
            bound, diagnostics, proof = bindings(packet, rows)
            self.assertTrue(
                review_coverage.assess(packet, proof["report"], diagnostics, policy=policy())["qualified"],
                diagnostics,
            )
            sidecar, correlation = capacity.bridge(
                raw(rows), packet, packet, policy(), "synthetic-session", bound
            )
            summary = observation.replay(sidecar, bound, diagnostics["usage"]["counters"], correlation)
            self.assertGreater(sum(summary["sums"].values()), summary["max_observed_input"])
            self.assertGreater(json.loads(sidecar)["responses"][-1]["ordinal"], correlation["output_ordinal"])
            for forbidden in (
                b"synthetic-message",
                b"service_tier",
                b"Public synthetic",
                b"report-message",
                b"session_id",
            ):
                self.assertNotIn(forbidden, sidecar)
            capture_hash = capacity.sha(b"synthetic capture")
            completion = observation.completion(sidecar, capture_hash)
            observation.replay_completed(
                sidecar, completion, capture_hash, bound, diagnostics["usage"]["counters"], correlation
            )
            for key in bound:
                with self.assertRaises(WorkflowError):
                    observation.replay_completed(
                        sidecar,
                        completion,
                        capture_hash,
                        {**bound, key: "f" * 64},
                        diagnostics["usage"]["counters"],
                        correlation,
                    )
            with self.assertRaises(WorkflowError):
                observation.replay_completed(
                    sidecar, completion, "f" * 64, bound, diagnostics["usage"]["counters"], correlation
                )

    def test_native_missing_conflicting_order_terminal_and_unknown_refusals(self):
        with tempfile.TemporaryDirectory() as tmp:
            packet = Path(tmp)
            for path, content in reporting_diagnostic_v2.packet_contents().items():
                target = packet / path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(content)
            original = stream(packet)
            assistant_index = next(i for i, r in enumerate(original) if r["type"] == "assistant")
            mutations = []
            for value in (None, True, -1, 9007199254740992):
                rows = copy.deepcopy(original)
                rows[assistant_index]["message"]["usage"]["input_tokens"] = value
                mutations.append(rows)
            for mutate in (
                lambda r: r.pop(),
                lambda r: r[-1].update(is_error=True),
                lambda r: r[0].update(model="wrong"),
                lambda r: r[0].update(session_id="wrong"),
                lambda r: r.insert(
                    1, {"type": "system", "subtype": "compact_boundary", "session_id": "synthetic-session"}
                ),
            ):
                rows = copy.deepcopy(original)
                mutate(rows)
                mutations.append(rows)
            conflict = copy.deepcopy(original)
            duplicate = copy.deepcopy(conflict[assistant_index])
            duplicate["message"]["usage"]["service_tier"] = "changed"
            duplicate["uuid"] = "different-uuid"
            conflict.insert(assistant_index + 1, duplicate)
            mutations.append(conflict)
            for rows in mutations:
                with self.subTest(case=mutations.index(rows)), self.assertRaises((WorkflowError, ValueError)):
                    changed_binding, _, _ = bindings(packet, rows)
                    capacity.bridge(raw(rows), packet, packet, policy(), "synthetic-session", changed_binding)

    def test_frozen_qualified_dedup_and_independent_correlation_refusals(self):
        with tempfile.TemporaryDirectory() as tmp:
            packet = Path(tmp)
            for path, content in reporting_diagnostic_v2.packet_contents().items():
                target = packet / path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(content)
            original = stream(packet)
            first = next(i for i, row in enumerate(original) if row["type"] == "assistant")
            duplicate = copy.deepcopy(original[first])
            duplicate["uuid"] = "new-envelope-same-message"
            rows = copy.deepcopy(original)
            rows.insert(first + 1, duplicate)
            bound, diagnostics, _ = bindings(packet, rows)
            sidecar, _ = capacity.bridge(raw(rows), packet, packet, policy(), "synthetic-session", bound)
            self.assertEqual(
                json.loads(sidecar)["summary"]["response_count"],
                diagnostics["usage"]["counters"]["model_steps_observed"],
            )
            mutations = []
            # These pass frozen qualification: the new observer must independently refuse.
            rows = copy.deepcopy(original)
            rows = [
                row
                for row in rows
                if not (row["type"] == "assistant" and row["message"]["content"][0]["type"] == "text")
            ]
            mutations.append(rows)
            rows = copy.deepcopy(original)
            rows[first]["message"]["usage"]["input_tokens"] = 400
            mutations.append(rows)
            rows = copy.deepcopy(original)
            progress = next(
                i
                for i, row in enumerate(rows)
                if row["type"] == "assistant" and row["message"]["content"][0]["type"] == "text"
            )
            changed = copy.deepcopy(rows[progress])
            changed["uuid"] = "conflicting-progress-envelope"
            changed["message"]["usage"]["service_tier"] = "conflicting auxiliary"
            rows.insert(progress + 1, changed)
            mutations.append(rows)
            for rows in mutations:
                bound, diagnostics, proof = bindings(packet, rows)
                self.assertTrue(
                    review_coverage.assess(packet, proof["report"], diagnostics, policy=policy())["qualified"]
                )
                with self.assertRaises(WorkflowError):
                    capacity.bridge(raw(rows), packet, packet, policy(), "synthetic-session", bound)

    def test_bound_empirical_receipt_is_storage_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            packet = Path(tmp)
            for path, content in reporting_diagnostic_v2.packet_contents().items():
                target = packet / path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(content)
            rows = stream(packet)
            cases, expected = {}, {}
            for number in (22, 23):
                bound, diagnostics, _ = bindings(packet, rows)
                bound["fixture_sha256"] = capacity.sha(str(number).encode())
                expected[number] = bound
                sidecar, correlation = capacity.bridge(
                    raw(rows), packet, packet, policy(), "synthetic-session", bound
                )
                capture = capacity.encoded({"synthetic_case": number})
                cases[number] = {
                    "sidecar": sidecar,
                    "completion": observation.completion(sidecar, capacity.sha(capture)),
                    "capture": capture,
                    "counters": diagnostics["usage"]["counters"],
                    "correlation": correlation,
                }
            receipt = capacity.empirical_receipt(cases, expected)
            self.assertNotIn("qualified", receipt)
            self.assertNotIn("ready", receipt)
            for name in expected[23]:
                changed = copy.deepcopy(expected)
                changed[23][name] = "f" * 64
                with self.assertRaises(WorkflowError):
                    capacity.empirical_receipt(cases, changed)
            for key in cases[23]:
                changed = copy.deepcopy(cases)
                del changed[23][key]
                with self.assertRaises(WorkflowError):
                    capacity.empirical_receipt(changed, expected)
            changed = copy.deepcopy(cases)
            changed[23]["capture"] += b"changed"
            with self.assertRaises(WorkflowError):
                capacity.empirical_receipt(changed, expected)

    def test_largest_selection_ties_impossible_padding_and_duplicate_ids(self):
        items, files = additional()
        first = catalog()[0]
        second = {
            "id": "component-0",
            "items": [{"id": "c" * 24, "artifact": "other.txt", "start_line": 1, "end_line": 1}],
            "files": {"other.txt": b"Public source\n"},
        }
        result = capacity.largest_fixture([first, second], items, files, {"source": "c" * 64})
        self.assertIn("other.txt", result["files"])
        self.assertNotIn("source.txt", result["files"])
        second["files"]["other.txt"] += b"more"
        # Outside-range file bytes cannot inflate the primary selection.
        result = capacity.largest_fixture([first, second], items, files, {"source": "c" * 64})
        self.assertIn("other.txt", result["files"])
        for component in (
            {
                "id": "full-lines",
                "items": [{"id": "d" * 24, "artifact": "x", "start_line": 1, "end_line": 9000}],
                "files": {"x": b"x\n" * 9000},
            },
            {
                "id": "too-many-lines",
                "items": [{"id": "d" * 24, "artifact": "x", "start_line": 1, "end_line": 9001}],
                "files": {"x": b"x\n" * 9001},
            },
        ):
            with self.assertRaises(WorkflowError):
                capacity.largest_fixture([component], items, files, {"source": "c" * 64})
        second["items"][0]["id"] = first["items"][0]["id"]
        with self.assertRaises(WorkflowError):
            capacity.largest_fixture([first, second], items, files, {"source": "c" * 64})
        for mutate in (
            lambda c: c["items"][0].update(start_line=True),
            lambda c: c["items"].append(copy.deepcopy(c["items"][0])),
            lambda c: c["files"].update(extra=b"undeclared"),
        ):
            changed = catalog()[0]
            mutate(changed)
            with self.assertRaises(WorkflowError):
                capacity.largest_fixture([changed], items, files, {"source": "c" * 64})


class BatchObservationTests(unittest.TestCase):
    """Ordinary frozen capture and numeric checks; no claim or admission authority."""

    def packet_stream(self, packet):
        for name, content in reporting_diagnostic_v2.packet_contents().items():
            target = packet / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
        inventory_path = packet / "required-material.json"
        inventory = json.loads(inventory_path.read_bytes())
        selected = [item["id"] for item in inventory["required"]]
        inventory["required"].append(
            {
                **inventory["required"][-1],
                "id": "c" * 24,
                "artifact": "surrounding.txt",
                "path": "surrounding.txt",
                "start_line": 1,
                "end_line": 1,
            }
        )
        (packet / "surrounding.txt").write_bytes(b"Unassigned surrounding source remains available.\n")
        inventory_path.write_bytes(capacity.encoded(inventory))
        (packet / "inventory-sha256.txt").write_text(capacity.sha(inventory_path.read_bytes()) + "\n")
        rows = stream(packet)
        omitted = {
            block["id"]
            for row in rows
            if row["type"] == "assistant"
            for block in row["message"]["content"]
            if block.get("name") == "Read" and block["input"]["file_path"] == "surrounding.txt"
        }
        rows = [
            row
            for row in rows
            if not (
                row["type"] in {"assistant", "user"}
                and any(
                    block.get("id", block.get("tool_use_id")) in omitted
                    for block in row["message"]["content"]
                )
            )
        ]
        report = rows[-1]["structured_output"]
        report["reviewed"].remove("c" * 24)
        for row in rows:
            if row["type"] == "stream_event" and row["event"]["type"] == "content_block_delta":
                row["event"]["delta"]["partial_json"] = json.dumps(report)
        return rows, selected

    def captured(self, packet, rows):
        body, diagnostics, proof = claude_telemetry_v8.capture(
            raw(rows), packet, packet, policy(), "synthetic-session"
        )
        bound = {key: capacity.sha(key.encode()) for key in observation.BINDINGS}
        for key, value in {
            "policy": capacity.encoded(policy()),
            "report": body.encode(),
            "proof": capacity.encoded(proof),
            "diagnostic": capacity.encoded(diagnostics),
            "descriptor": observation._bytes(observation.DESCRIPTOR),
        }.items():
            bound[key + "_sha256"] = capacity.sha(value)
        return bound, diagnostics, proof

    def test_child_observation_keeps_unassigned_parent_material_unread(self):
        with tempfile.TemporaryDirectory() as tmp:
            packet = Path(tmp)
            rows, selected = self.packet_stream(packet)
            bound, diagnostics, proof = self.captured(packet, rows)
            assessment = review_coverage.assess(packet, proof["report"], diagnostics, policy=policy())
            self.assertFalse(assessment["qualified"])
            self.assertEqual(assessment["material"][-1]["state"], "unread")
            self.assertTrue(proof["accepted"])
            # The historical diagnostic bridge still requires its entire inventory.
            with self.assertRaises(WorkflowError):
                capacity.bridge(raw(rows), packet, packet, policy(), "synthetic-session", bound)
            sidecar, correlation = capacity.batch_bridge(
                raw(rows), packet, packet, policy(), "synthetic-session", bound, selected
            )
            observation.replay(sidecar, bound, diagnostics["usage"]["counters"], correlation)
            self.assertNotIn(b"synthetic-session", sidecar)
            self.assertEqual(len(json.loads((packet / "required-material.json").read_bytes())["required"]), 3)

    def test_unknown_duplicate_unread_assignment_and_binding_refuse(self):
        with tempfile.TemporaryDirectory() as tmp:
            packet = Path(tmp)
            rows, selected = self.packet_stream(packet)
            bound, _, _ = self.captured(packet, rows)
            for selection in ([], True, [True], selected + selected, ["f" * 24], selected + ["c" * 24]):
                with self.subTest(selection=selection), self.assertRaises(WorkflowError):
                    capacity.batch_bridge(
                        raw(rows), packet, packet, policy(), "synthetic-session", bound, selection
                    )
            for key in ("policy", "report", "proof", "diagnostic", "descriptor"):
                with self.subTest(key=key), self.assertRaises(WorkflowError):
                    capacity.batch_bridge(
                        raw(rows),
                        packet,
                        packet,
                        policy(),
                        "synthetic-session",
                        {**bound, key + "_sha256": "f" * 64},
                        selected,
                    )

    def test_observed_input_ceiling_and_missing_final_response(self):
        with tempfile.TemporaryDirectory() as tmp:
            packet = Path(tmp)
            rows, selected = self.packet_stream(packet)
            for total in (872000, 872001):
                changed = copy.deepcopy(rows)
                for row in changed:
                    if row["type"] == "assistant":
                        row["message"]["usage"]["input_tokens"] = total - 50
                bound, _, _ = self.captured(packet, changed)
                if total == 872000:
                    capacity.batch_bridge(
                        raw(changed), packet, packet, policy(), "synthetic-session", bound, selected
                    )
                else:
                    with self.assertRaises(WorkflowError):
                        capacity.batch_bridge(
                            raw(changed), packet, packet, policy(), "synthetic-session", bound, selected
                        )
            changed = [
                row
                for row in rows
                if not (row["type"] == "assistant" and row["message"]["content"][0]["type"] == "text")
            ]
            bound, _, _ = self.captured(packet, changed)
            with self.assertRaises(WorkflowError):
                capacity.batch_bridge(
                    raw(changed), packet, packet, policy(), "synthetic-session", bound, selected
                )
