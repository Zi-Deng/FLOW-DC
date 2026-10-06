"""V7-only bounded guard observations; no provider payload or inspection credit.

Names describe the predicate being evaluated, including a typed evaluation error.
They do not describe unseen payloads or establish provider compatibility. Counts
saturate independently of the first 32 correlation samples. No input string, key,
identifier, length or hash is needed for this classification and none is retained.
"""

from collections import Counter

from workflow import WorkflowError

SCHEMA = 1
MAX_SAMPLES = 32
MAX_REJECTIONS = 20000
PREDICATES = frozenset(
    {
        "event_object",
        "message_inactive",
        "message_object",
        "message_id",
        "message_unique",
        "message_model",
        "message_active",
        "event_kind",
        "block_index",
        "block_unique",
        "block_object",
        "tool_name",
        "tool_id",
        "tool_input",
        "block_kind",
        "block_open",
        "delta_object",
        "delta_kind",
        "delta_fields",
        "delta_text",
        "message_delta_object",
        "message_usage_object",
        "message_blocks_closed",
    }
)
FLAGS = frozenset({"evaluation_error", "message_active", "index_valid", "index_known", "index_open"})


class Observation:
    def __init__(self):
        self.counts = {}
        self.samples = []
        self.total = 0
        self.count_saturated = False

    def reject(self, predicate, evaluation_error, active, blocks, event):
        # Called only with fixed internal predicate names. Never enumerate or
        # serialize the rejected event, even if it contains a huge/hostile object.
        if self.total == MAX_REJECTIONS:
            self.count_saturated = True
            return
        self.total += 1
        self.counts[predicate] = self.counts.get(predicate, 0) + 1
        if len(self.samples) == MAX_SAMPLES:
            return
        index = event.get("index") if type(event) is dict else None
        valid = type(index) is int and 0 <= index < MAX_REJECTIONS
        known = valid and index in blocks
        self.samples.append(
            {
                "predicate": predicate,
                "evaluation_error": evaluation_error,
                "message_active": active is not None,
                "index_valid": valid,
                "index_known": known,
                "index_open": known and blocks[index] is not None,
            }
        )

    def record(self):
        return {
            "schema_version": SCHEMA,
            "total": self.total,
            "counts": dict(self.counts),
            "samples": [dict(row) for row in self.samples],
            "samples_overflow": self.total > MAX_SAMPLES,
            "count_saturated": self.count_saturated,
        }


def validate(value, stream_count):
    """Exact optional nested schema; fixed size, typed counts and correlations."""
    try:
        if (
            type(value) is not dict
            or len(value) != 6
            or set(value)
            != {
                "schema_version",
                "total",
                "counts",
                "samples",
                "samples_overflow",
                "count_saturated",
            }
        ):
            raise ValueError
        total, counts, samples = value["total"], value["counts"], value["samples"]
        if (
            type(stream_count) is not int
            or not 0 <= stream_count <= MAX_REJECTIONS
            or type(value["schema_version"]) is not int
            or value["schema_version"] != SCHEMA
            or type(total) is not int
            or not 0 <= total <= min(stream_count, MAX_REJECTIONS)
            or type(counts) is not dict
            or len(counts) > len(PREDICATES)
            or not counts.keys() <= PREDICATES
            or any(type(n) is not int or not 1 <= n <= MAX_REJECTIONS for n in counts.values())
            or sum(counts.values()) != total
            or type(samples) is not list
            or len(samples) != min(total, MAX_SAMPLES)
            or type(value["samples_overflow"]) is not bool
            or value["samples_overflow"] != (total > MAX_SAMPLES)
            or type(value["count_saturated"]) is not bool
            or (value["count_saturated"] and total != MAX_REJECTIONS)
        ):
            raise ValueError
        sampled = Counter()
        for row in samples:
            if (
                type(row) is not dict
                or len(row) != 6
                or set(row) != FLAGS | {"predicate"}
                or row["predicate"] not in PREDICATES
                or any(type(row[k]) is not bool for k in FLAGS)
                or (row["index_open"] and not row["message_active"])
                or (row["index_open"] and not row["index_known"])
                or (row["index_known"] and not row["index_valid"])
            ):
                raise ValueError
            sampled[row["predicate"]] += 1
        if any(n > counts.get(k, 0) for k, n in sampled.items()):
            raise ValueError
    except (KeyError, TypeError, ValueError):
        raise WorkflowError("Invalid v7 partial-stream observation") from None


def validate_reasons(summary, reasons):
    if "partial_stream" in summary and (
        (summary["partial_stream"]["total"] > 0) != ("unsupported_partial_stream" in reasons)
    ):
        raise WorkflowError("Partial-stream observation differs from rejection reasons")
