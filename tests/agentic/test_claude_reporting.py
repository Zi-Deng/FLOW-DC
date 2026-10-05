"""Synthetic exact-report transport checks; no provider or coverage qualification."""

import copy
import hashlib
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts/agentic"))

import claude_reporting as reporting  # noqa: E402

DIGEST = "a" * 64
ITEM = "b" * 24
# Deliberately preserve lexical choices that object serialization loses.
REPORT = (
    ' \r\n{"schema_version":2, "inventory_sha256":"' + DIGEST + '",'
    '"findings":[],"reviewed":["' + ITEM + '"],"incomplete":[], '
    '"limitations":["caf\\u00e9", "line\\r\\nend", "tab\\tcontrol\\u0001"]}\r\n'
)
AUXILIARY = "Explanatory prose remains separate.\r\n\t\x01"
LIMITS = {
    "report_bytes": 10000,
    "fragment_bytes": 4000,
    "terminal_bytes": 10000,
    "proof_bytes": 100000,
    "events": 100,
}


def events(report=REPORT):
    value = json.loads(report)
    pieces = [report[:9], report[9:117], report[117:]]
    rows = [
        {"kind": "message_start", "message_id": "msg_report", "model": "claude-opus-5-5"},
        {"kind": "block_start", "index": 0, "tool_id": "toolu_report", "input": {}},
        *[{"kind": "fragment", "index": 0, "text": p} for p in pieces],
        # The pinned native loop yields the normalized assistant block BEFORE
        # forwarding content_block_stop. Requiring the inverse rejects real output.
        {"kind": "assistant", "message_id": "msg_report", "tool_id": "toolu_report", "input": value},
        {"kind": "block_stop", "index": 0},
        {"kind": "message_stop"},
        {
            "kind": "tool_result",
            "tool_id": "toolu_report",
            "success": True,
            "content": "Structured output provided successfully",
        },
        {"kind": "terminal", "success": True, "structured_output": value, "text": AUXILIARY},
    ]
    return [{"event_id": f"event_{i}", **row} for i, row in enumerate(rows)]


def capture(rows=None, limits=None):
    return reporting.capture(
        events() if rows is None else rows,
        model="claude-opus-5-5",
        inventory_sha256=DIGEST,
        limits=LIMITS if limits is None else limits,
    )


class ReportingTests(unittest.TestCase):
    def test_exact_model_fragments_and_auxiliary_are_separate_and_replayable(self):
        result = capture()
        self.assertTrue(result["accepted"], result["reasons"])
        self.assertEqual(result["report"].encode(), REPORT.encode())
        self.assertEqual(result["terminal_text"].encode(), AUXILIARY.encode())
        self.assertNotEqual(result["report"], json.dumps(json.loads(REPORT)))
        self.assertEqual(reporting.replay(json.loads(json.dumps(result, sort_keys=True))), result)
        self.assertNotIn("qualified", result)
        self.assertNotIn("capability", result)

    def test_each_required_event_missing_or_duplicated_is_rejected(self):
        original = events()
        for i in range(len(original)):
            for operation in ("missing", "duplicate"):
                rows = copy.deepcopy(original)
                if operation == "missing":
                    del rows[i]
                else:
                    rows.insert(i, copy.deepcopy(rows[i]))
                with self.subTest(i=i, operation=operation):
                    self.assertFalse(capture(rows)["accepted"])

    def test_ordering_and_wrong_correlations_cannot_supply_report_provenance(self):
        for a, b in ((0, 1), (1, 2), (2, 3), (5, 8), (6, 7), (8, 9)):
            rows = events()
            rows[a], rows[b] = rows[b], rows[a]
            with self.subTest(a=a, b=b):
                self.assertFalse(capture(rows)["accepted"])
        for index, key, value in (
            (0, "model", "other-model"),
            (1, "input", {"already": "parsed"}),
            (2, "index", 1),
            (2, "index", True),
            (5, "message_id", "other"),
            (5, "tool_id", "other"),
            (5, "input", {"normalization": "changed"}),
            (6, "index", 1),
            (8, "tool_id", "other"),
            (8, "success", False),
            (8, "content", "unrecognized success"),
            (9, "success", False),
            (9, "structured_output", None),
            (9, "structured_output", {"wrong": 2}),
        ):
            rows = events()
            rows[index][key] = value
            with self.subTest(index=index, key=key):
                self.assertFalse(capture(rows)["accepted"])

    def test_no_prose_salvage_or_object_only_fallback(self):
        for text in ("Prose\n" + REPORT, "```json\n" + REPORT + "\n```", REPORT[:-4], "[REDACTED]"):
            rows = events()
            rows[2]["text"] = text
            rows[3]["text"] = rows[4]["text"] = ""
            result = capture(rows)
            self.assertFalse(result["accepted"])
            self.assertEqual(result["report"], text)
            self.assertEqual(result["terminal_text"], AUXILIARY)
        rows = [events()[-1]]
        self.assertFalse(capture(rows)["accepted"])
        self.assertEqual(capture(rows)["report"], "")

    def test_report_contract_and_json_are_strict(self):
        for text in (
            REPORT.replace('"schema_version":2', '"schema_version":true'),
            REPORT.replace('"schema_version":2', '"schema_version":2,"schema_version":2'),
            REPORT.replace('"findings":[]', '"findings":[NaN]'),
            REPORT.replace(DIGEST, "c" * 64),
            REPORT.replace('"reviewed":["' + ITEM + '"]', '"reviewed":["invalid"]'),
            REPORT.replace('"findings":[]', '"findings":[{}]'),
            REPORT.replace('"incomplete":[]', '"incomplete":[{"ids":[],"state":"unread","reason":""}]'),
        ):
            rows = events(text)
            with self.subTest(text=text):
                self.assertFalse(capture(rows)["accepted"])

    def test_retracted_or_multiple_calls_cannot_be_selected_by_matching_object(self):
        rows = events()
        rows[-1]["structured_output"] = None  # Native deferred/retracted terminal.
        self.assertFalse(capture(rows)["accepted"])
        rows = events()[:-1] + events()  # Identical reports do not identify the survivor.
        for i, row in enumerate(rows):
            row["event_id"] = f"unique_{i}"
        self.assertFalse(capture(rows)["accepted"])
        rows = events()
        rows.insert(-1, {"event_id": "tombstone", "kind": "retracted"})
        self.assertFalse(capture(rows)["accepted"])

    def test_bounds_are_strict_and_oversized_bytes_are_not_replaced(self):
        for name in LIMITS:
            for value in (True, 0, -1, 1.5, float("nan"), float("inf")):
                with self.subTest(name=name, value=value):
                    with self.assertRaises(ValueError):
                        capture(limits={**LIMITS, name: value})
        with self.assertRaises(ValueError):
            capture(limits={**LIMITS, "proof_bytes": 1})
        for name in ("report_bytes", "fragment_bytes", "terminal_bytes", "proof_bytes", "events"):
            result = capture(limits={**LIMITS, name: 2})
            self.assertFalse(result["accepted"])
            self.assertTrue(any("limit" in reason for reason in result["reasons"]))
        rows = events()
        rows[2]["text"] = "\ud800"
        self.assertFalse(capture(rows)["accepted"])

    def test_incomplete_records_replay_without_fabricating_missing_bytes(self):
        for name in LIMITS:
            result = capture(limits={**LIMITS, name: 2})
            self.assertFalse(result["accepted"])
            self.assertEqual(reporting.replay(json.loads(json.dumps(result))), result)
        rows = events()
        rows[2]["text"] = "prose\r\n" + rows[2]["text"]
        result = capture(rows)
        self.assertFalse(result["accepted"])
        self.assertEqual(reporting.replay(result), result)

    def test_escaped_proof_size_is_refused_before_serialization(self):
        value = {"value": "\x00" * 30}
        with patch.object(reporting.json, "dumps", side_effect=AssertionError("allocated oversized proof")):
            with self.assertRaises(ValueError):
                reporting._json_bytes(value, 100)
        expected = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
        self.assertEqual(reporting._json_bytes(value, len(expected)), expected)
        with self.assertRaises(ValueError):
            reporting._json_bytes(value, len(expected) - 1)

    def test_pinned_source_preserves_envelopes_fragments_and_pure_tool_ack(self):
        path = Path(__file__).parent / "fixtures/claude-reporting-2.1.282-source.json"
        raw = path.read_bytes()
        self.assertEqual(
            hashlib.sha256(raw).hexdigest(),
            "41a3182979d20b3ffe0c19d405dd0549c075cd7c37bfecbb4247c5b91887cbf7",
        )
        fixture = json.loads(raw)
        self.assertEqual(
            fixture["binary_sha256"], "3afe8535c0cc33f0e24f7b25dab7a1727b8b592196f8496a8bc302ba2161eed3"
        )
        for excerpt in fixture["excerpts"].values():
            source = excerpt["source"].encode()
            self.assertEqual(len(source), excerpt["end_byte"] - excerpt["start_byte"])
            self.assertEqual(hashlib.sha256(source).hexdigest(), excerpt["sha256"])
        node = shutil.which("node")
        if node is None:
            self.skipTest("Node unavailable; pinned source exercise needs a separate local check")
        source = {key: value["source"] for key, value in fixture["excerpts"].items()}
        script = "import assert from 'node:assert/strict';\n"
        script += "const Dt=v=>v,ui='StructuredOutput';\n"
        script += (
            "const envelope=(()=>{const Dt='session',uc=()=>'generated_uuid';return "
            + source["envelope"]
            + ";})();\n"
        )
        script += source["stream-envelope"] + "\n" + source["report-tool"] + "\n"
        script += "const report=" + json.dumps(REPORT) + ";\n"
        script += (
            "const bi={input:''}; for(const Ua of [{partial_json:report.slice(0,9)},{partial_json:report.slice(9)}]){"
            + source["fragment-append"]
            + ";}\n"
        )
        script += """
assert.equal(bi.input,report);
const original={type:'stream_event',event:{type:'content_block_delta',index:0,
 delta:{type:'input_json_delta',partial_json:bi.input}},uuid:'native_uuid'};
const wrapped=envelope(original), emitted=Jf(wrapped,wrapped);
assert.equal(emitted.event,original.event);
assert.equal(emitted.uuid,'native_uuid');
assert.equal(emitted.session_id,'session');
assert.equal(emitted.parent_tool_use_id,null);
assert.equal(emitted.event.delta.partial_json,report);
assert.equal(envelope({type:'stream_event'}).uuid,'generated_uuid');
let validations=0;
const tool=fe({},input=>{validations++;assert.equal(input.schema_version,2)});
assert.equal(tool.name,'StructuredOutput');
assert.equal(tool.isReadOnly(),true);
assert.equal(tool.isOpenWorld(),false);
assert.equal(tool.isMcp,false);
assert.equal(tool.backgrounding,'never');
const input=JSON.parse(report), output=await tool.create().call(input);
assert.equal(validations,1);
assert.equal(output.structured_output,input);
assert.equal(output.endsTurn,true);
assert.deepEqual(tool.mapToolResultToToolResultBlockParam(output.data,'toolu_report'),
 {type:'tool_result',tool_use_id:'toolu_report',content:'Structured output provided successfully'});
await assert.rejects(fe({},()=>{throw Error('invalid schema')}).create().call(input),/invalid schema/);
console.log('pinned-source-synthetic-only');
"""
        with tempfile.TemporaryDirectory() as directory:
            file = Path(directory) / "reporting-source.mjs"
            file.write_text(script, encoding="utf-8")
            result = subprocess.run([node, str(file)], capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "pinned-source-synthetic-only")

    def native_rows(self):
        # Synthetic native envelopes use the pinned API/CLI field names and
        # block-stop ordering. Inspection and progress payloads must not persist.
        value = json.loads(REPORT)
        rows = [
            {"type": "system", "subtype": "init", "private": "DO_NOT_RETAIN_INIT"},
            {
                "type": "assistant",
                "message": {
                    "id": "msg_before",
                    "content": [
                        {"type": "text", "text": "DO_NOT_RETAIN_PROGRESS"},
                        {
                            "type": "tool_use",
                            "name": "Read",
                            "id": "toolu_read",
                            "input": {"file_path": "DO_NOT_RETAIN_PATH"},
                        },
                    ],
                },
            },
            {
                "type": "user",
                "message": {
                    "content": [
                        {
                            "type": "tool_result",
                            "tool_use_id": "toolu_read",
                            "content": "DO_NOT_RETAIN_SOURCE",
                        }
                    ]
                },
            },
            {
                "type": "stream_event",
                "event": {
                    "type": "message_start",
                    "message": {"id": "msg_report", "model": "claude-opus-5-5"},
                },
            },
            {
                "type": "stream_event",
                "event": {
                    "type": "content_block_start",
                    "index": 0,
                    "content_block": {
                        "type": "tool_use",
                        "id": "toolu_report",
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
                    "delta": {"type": "input_json_delta", "partial_json": REPORT},
                },
            },
            {
                "type": "assistant",
                "message": {
                    "id": "msg_report",
                    "model": "claude-opus-5-5",
                    "content": [
                        {"type": "tool_use", "name": "StructuredOutput", "id": "toolu_report", "input": value}
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
                            "tool_use_id": "toolu_report",
                            "content": "Structured output provided successfully",
                        }
                    ]
                },
            },
            {
                "type": "result",
                "subtype": "success",
                "is_error": False,
                "result": AUXILIARY,
                "structured_output": value,
            },
        ]
        return [
            {"uuid": f"native_{i}", "session_id": "session", "parent_tool_use_id": None, **r}
            for i, r in enumerate(rows)
        ]

    def test_native_projection_retains_original_identity_and_only_report_material(self):
        rows = self.native_rows()
        projected = list(reporting.native_projection(rows, session_id="session"))
        result = capture(projected)
        self.assertTrue(result["accepted"], result["reasons"])
        self.assertEqual(result["report"].encode(), REPORT.encode())
        self.assertEqual([r["event_id"] for r in projected], [r["uuid"] for r in rows[3:]])
        self.assertNotIn("DO_NOT_RETAIN", json.dumps(result))
        self.assertEqual(reporting.replay(json.loads(json.dumps(result, sort_keys=True))), result)
        for index in range(3, len(rows)):
            missing = copy.deepcopy(rows)
            del missing[index]
            duplicate = copy.deepcopy(rows)
            duplicate.insert(index, copy.deepcopy(rows[index]))
            for variant in (missing, duplicate):
                with self.subTest(index=index):
                    self.assertFalse(
                        capture(reporting.native_projection(variant, session_id="session"))["accepted"]
                    )

    def test_native_projection_rejects_foreign_identity_normalization_and_deferred_result(self):
        for index, key, value in (
            (5, "uuid", None),
            (5, "session_id", "other"),
            (5, "parent_tool_use_id", "delegated"),
            (6, "error", "refused"),
            (10, "structured_output", None),
        ):
            rows = self.native_rows()
            rows[index][key] = value
            self.assertFalse(capture(reporting.native_projection(rows, session_id="session"))["accepted"])
        rows = self.native_rows()
        rows[6]["message"]["content"][0]["input"]["limitations"] = ["normalized"]
        self.assertFalse(capture(reporting.native_projection(rows, session_id="session"))["accepted"])

    def test_native_projection_does_not_normalize_invalid_block_indexes(self):
        for index in (5, 7):
            for value in (False, 0.0):
                rows = self.native_rows()
                rows[index]["event"]["index"] = value
                with self.subTest(index=index, value=value):
                    self.assertFalse(
                        capture(reporting.native_projection(rows, session_id="session"))["accepted"]
                    )

    def test_saved_proof_is_recomputed_and_arbitrary_fields_are_not_retained(self):
        result = capture()
        for field, value in (
            ("report", "{}"),
            ("terminal_text", "changed"),
            ("accepted", False),
            ("reasons", ["x"]),
        ):
            changed = copy.deepcopy(result)
            changed[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                reporting.replay(changed)
        changed = copy.deepcopy(result)
        changed["proof"][2]["text"] = "changed"
        with self.assertRaises(ValueError):
            reporting.replay(changed)
        rows = events()
        rows[2]["private_payload"] = "DO_NOT_RETAIN"
        result = capture(rows)
        self.assertFalse(result["accepted"])
        self.assertNotIn("DO_NOT_RETAIN", json.dumps(result))


if __name__ == "__main__":
    unittest.main()
