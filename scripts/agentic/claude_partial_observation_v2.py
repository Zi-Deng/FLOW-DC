"""Fixed noncontent delta-field observations; never a provider acceptance rule."""

import copy
import json
import math

import claude_partial_observation as legacy
from workflow import WorkflowError

SCHEMA = 2
MAX_BYTES = 32768
MAX_OTHER_FIELDS = 8
DESCRIPTOR = {
    "schema_version": SCHEMA,
    "max_bytes": MAX_BYTES,
    "max_samples": legacy.MAX_SAMPLES,
    "max_rejections": legacy.MAX_REJECTIONS,
    "max_other_fields": MAX_OTHER_FIELDS,
}
PAIRS = {
    "tool_use": {"input_json_delta": "partial_json"},
    "text": {"text_delta": "text"},
    "thinking": {"thinking_delta": "thinking", "signature_delta": "signature"},
    "redacted_thinking": {},
}
CLASSES = frozenset(
    {
        "absent",
        "null",
        "boolean",
        "string",
        "array",
        "object",
        "nonfinite",
        "negative",
        "fractional",
        "integer_safe",
        "above_safe",
        "unavailable",
    }
)
FIELDS = frozenset(
    {
        "block_kind",
        "delta_kind",
        "type_present",
        "payload_present",
        "estimated_tokens_present",
        "other_fields",
        "other_fields_overflow",
        "estimated_tokens_class",
    }
)


def numeric_class(value):
    kind = type(value)
    if value is None:
        return "null"
    if kind in (bool, str, list, dict):
        return {bool: "boolean", str: "string", list: "array", dict: "object"}[kind]
    if kind not in (int, float):
        return "unavailable"
    if kind is float and not math.isfinite(value):
        return "nonfinite"
    if value < 0:
        return "negative"
    if value > 9007199254740991:
        return "above_safe"
    if kind is float and not value.is_integer():
        return "fractional"
    return "integer_safe"


def shape(blocks, event):
    # Only called at delta_fields: preceding frozen guards validated this pair.
    # Fixed lookups and cardinality never enumerate/stringify unknown keys/values.
    index = event.get("index") if type(event) is dict else None
    block = blocks.get(index) if type(index) is int else None
    delta = event.get("delta") if type(event) is dict else None
    kind = delta.get("type") if type(delta) is dict else None
    field = PAIRS.get(block, {}).get(kind) if type(block) is str and type(kind) is str else None
    if field is None:
        raise WorkflowError("Unavailable delta-field observation boundary")
    present = "estimated_tokens" in delta
    count = len(delta) - sum(key in delta for key in ("type", field, "estimated_tokens"))
    return {
        "block_kind": block,
        "delta_kind": kind,
        "type_present": "type" in delta,
        "payload_present": field in delta,
        "estimated_tokens_present": present,
        "other_fields": min(count, MAX_OTHER_FIELDS),
        "other_fields_overflow": count > MAX_OTHER_FIELDS,
        "estimated_tokens_class": numeric_class(delta["estimated_tokens"]) if present else "absent",
    }


class Observation(legacy.Observation):
    def reject(self, predicate, evaluation_error, active, blocks, event):
        before = len(self.samples)
        super().reject(predicate, evaluation_error, active, blocks, event)
        if len(self.samples) > before:
            self.samples[-1]["delta_shape"] = shape(blocks, event) if predicate == "delta_fields" else None

    def record(self):
        value = super().record()
        value["schema_version"] = SCHEMA
        return copy.deepcopy(value)


def validate_shape(row):
    value = row["delta_shape"]
    if row["predicate"] != "delta_fields":
        if value is not None:
            raise ValueError
        return
    if type(value) is not dict or len(value) != len(FIELDS) or set(value) != FIELDS:
        raise ValueError
    block, kind, classification = value["block_kind"], value["delta_kind"], value["estimated_tokens_class"]
    if (
        type(block) is not str
        or type(kind) is not str
        or type(classification) is not str
        or kind not in PAIRS.get(block, {})
        or classification not in CLASSES
        or any(
            type(value[k]) is not bool
            for k in ("type_present", "payload_present", "estimated_tokens_present", "other_fields_overflow")
        )
        or not value["type_present"]
        or not row["index_open"]
        or type(value["other_fields"]) is not int
        or not 0 <= value["other_fields"] <= MAX_OTHER_FIELDS
        or (value["other_fields_overflow"] and value["other_fields"] != MAX_OTHER_FIELDS)
        or value["estimated_tokens_present"] != (classification != "absent")
        or (value["payload_present"] and not value["estimated_tokens_present"] and value["other_fields"] == 0)
    ):
        raise ValueError


def validate(value, stream_count):
    if type(value) is dict and type(value.get("schema_version")) is int and value["schema_version"] == 1:
        return legacy.validate(value, stream_count)
    try:
        if (
            type(value) is not dict
            or type(value.get("schema_version")) is not int
            or value["schema_version"] != SCHEMA
        ):
            raise ValueError
        samples = value.get("samples")
        if type(samples) is not list or len(samples) > legacy.MAX_SAMPLES:
            raise ValueError
        projected = []
        for row in samples:
            if (
                type(row) is not dict
                or len(row) != 7
                or set(row) != legacy.FLAGS | {"predicate", "delta_shape"}
            ):
                raise ValueError
            projected.append({key: row[key] for key in legacy.FLAGS | {"predicate"}})
        legacy.validate({**value, "schema_version": 1, "samples": projected}, stream_count)
        for row in samples:
            validate_shape(row)
        # Every value is now from fixed enums, booleans or capped integers.
        if (
            len(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode())
            > MAX_BYTES
        ):
            raise ValueError
    except (KeyError, TypeError, ValueError, RecursionError, UnicodeError):
        raise WorkflowError("Invalid v7 structural partial-stream observation") from None


validate_reasons = legacy.validate_reasons
