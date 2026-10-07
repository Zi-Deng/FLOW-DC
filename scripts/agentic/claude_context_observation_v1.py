"""Opt-in numeric context evidence; no provider parser or readiness authority.

An owned-capture consumer must first validate the native model/session/protocol,
then deliver EVERY correlated protocol step in order. Steps may split one native
envelope only where the validated protocol establishes that order; the consumer
must never reorder or omit native steps to obtain a qualifying correlation.
It independently derives mandatory Read completion and output ordinals.
Replay requires those same correlations and all artifact bindings; hashes are
owner-writable integrity records, not provider attestations or inspection credit.
"""

from __future__ import annotations

import hashlib
import json

from review_coverage import strict_json
from workflow import WorkflowError

MAX_RESPONSES = 400
MAX_BYTES = 65_536
MAX_EVENTS = 20_000
SAFE_INTEGER = 9_007_199_254_740_991
COUNTERS = ("input_tokens", "cache_read_input_tokens", "cache_creation_input_tokens")
BINDINGS = frozenset(
    f"{name}_sha256"
    for name in (
        "input",
        "policy",
        "execution",
        "fixture",
        "source",
        "descriptor",
        "report",
        "proof",
        "diagnostic",
    )
)
DESCRIPTOR = {"schema_version": 1, "max_responses": MAX_RESPONSES, "max_bytes": MAX_BYTES}


def _fail():
    raise WorkflowError("Incomplete or inconsistent context observation") from None


def _integer(value, maximum=SAFE_INTEGER):
    return type(value) is int and 0 <= value <= maximum


def _bytes(value):
    try:
        return json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
        ).encode()
    except (ValueError, TypeError, UnicodeError, RecursionError):
        _fail()


def _hash(value):
    return type(value) is str and len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def _bindings(value):
    if type(value) is not dict or set(value) != BINDINGS or not all(_hash(v) for v in value.values()):
        _fail()
    if value["descriptor_sha256"] != hashlib.sha256(_bytes(DESCRIPTOR)).hexdigest():
        _fail()


def _summary(rows):
    if type(rows) is not list or not 1 <= len(rows) <= MAX_RESPONSES:
        _fail()
    previous_ordinal, previous_total = 0, 0
    totals = []
    for row in rows:
        if type(row) is not dict or set(row) != {"ordinal", *COUNTERS}:
            _fail()
        if not _integer(row["ordinal"], MAX_EVENTS) or row["ordinal"] <= previous_ordinal:
            _fail()
        if not all(_integer(row[k]) for k in COUNTERS):
            _fail()
        total = sum(row[k] for k in COUNTERS)
        if total > SAFE_INTEGER or total < previous_total:
            _fail()
        previous_ordinal, previous_total = row["ordinal"], total
        totals.append(total)
    return {
        "response_count": len(rows),
        "sums": {k: sum(row[k] for row in rows) for k in COUNTERS},
        "max_observed_input": max(totals),
        "final_observed_input": totals[-1],
    }


def _correlation(value, rows):
    if type(value) is not dict or set(value) != {"event_count", "mandatory_read_ordinals", "output_ordinal"}:
        _fail()
    count, reads, output = (value[k] for k in ("event_count", "mandatory_read_ordinals", "output_ordinal"))
    if not _integer(count, MAX_EVENTS) or not _integer(output, MAX_EVENTS) or not 0 < output < count:
        _fail()
    if type(reads) is not list or not reads or len(reads) > MAX_EVENTS:
        _fail()
    previous = 0
    for ordinal in reads:
        if not _integer(ordinal, MAX_EVENTS) or not previous < ordinal < output:
            _fail()
        previous = ordinal
    if any(row["ordinal"] >= count or row["ordinal"] == output or row["ordinal"] in reads for row in rows):
        _fail()
    if not any(reads[-1] < row["ordinal"] < output for row in rows):
        _fail()


def _diagnostic(summary, counters):
    # Consume selected frozen diagnostic counters; never rewrite their sums.
    if type(counters) is not dict:
        _fail()
    expected = {"model_steps_observed": summary["response_count"]}
    expected.update({f"assistant_{k}_observed": v for k, v in summary["sums"].items()})
    for key, value in expected.items():
        if not _integer(counters.get(key)) or counters[key] != value:
            _fail()


class Observer:
    """Transient reducer of closed, independently correlated protocol steps.

    `assistant` carries a complete native message, including id and usage; its
    full canonical fingerprint is kept only in memory to reject conflicting IDs.
    `native_assistant` is reserved for a frozen-qualified native bridge: usage
    may include native auxiliary fields and the complete report message can arrive
    after the output block starts. Full-message dedup still includes those fields;
    replay still requires a distinct response after the last Read BEFORE output.
    `mandatory_read` means a successful exact mandatory span, established by the
    consumer, not a tool name or a model claim. Any feed rejection poisons this object.
    """

    def __init__(self):
        self._ordinal = 0
        self._seen = {}
        self._rows = []
        self._reads = []
        self._output = None
        self._terminal = False
        self._failed = False

    def feed(self, event):
        try:
            self._feed(event)
        except (WorkflowError, ValueError, TypeError, KeyError, RecursionError):
            self._failed = True
            _fail()

    def _feed(self, event):
        if self._failed or self._terminal or type(event) is not dict:
            _fail()
        kind = event.get("kind")
        keys = (
            {"ordinal", "kind", "message"}
            if kind in {"assistant", "native_assistant"}
            else {"ordinal", "kind"}
        )
        if set(event) != keys or not _integer(event["ordinal"], MAX_EVENTS):
            _fail()
        if event["ordinal"] != self._ordinal + 1:
            _fail()
        self._ordinal = event["ordinal"]
        if kind in {"assistant", "native_assistant"}:
            if kind == "assistant" and self._output is not None:
                _fail()
            message = event["message"]
            if type(message) is not dict:
                _fail()
            identity, usage = message.get("id"), message.get("usage")
            if type(identity) is not str or not 0 < len(identity) <= 256:
                _fail()
            if type(usage) is not dict or (kind == "assistant" and set(usage) - {*COUNTERS, "output_tokens"}):
                _fail()
            if not all(_integer(usage.get(k)) for k in COUNTERS):
                _fail()
            if "output_tokens" in usage and not _integer(usage["output_tokens"]):
                _fail()
            raw = _bytes(message)
            if len(raw) > 16_000_000:
                _fail()
            fingerprint = hashlib.sha256(raw).digest()
            if identity in self._seen:
                if self._seen[identity] != fingerprint:
                    _fail()
                return
            if len(self._rows) == MAX_RESPONSES:
                _fail()
            self._seen[identity] = fingerprint
            self._rows.append({"ordinal": self._ordinal, **{k: usage[k] for k in COUNTERS}})
            _summary(self._rows)
        elif kind == "mandatory_read":
            if self._output is not None:
                _fail()
            self._reads.append(self._ordinal)
        elif kind == "structured_output":
            if self._output is not None:
                _fail()
            self._output = self._ordinal
        elif kind == "terminal_success":
            if self._output is None:
                _fail()
            self._terminal = True
        elif kind != "other":
            # Includes compaction, gaps, unsupported material and terminal failure.
            _fail()

    def seal(self, bindings, diagnostic_counters, correlation):
        """Produce canonical bytes before capture persistence; no capture credit."""
        if self._failed or not self._terminal:
            _fail()
        actual = {
            "event_count": self._ordinal,
            "mandatory_read_ordinals": self._reads,
            "output_ordinal": self._output,
        }
        if _bytes(actual) != _bytes(correlation):
            _fail()
        value = {
            "schema_version": 1,
            "bindings": bindings,
            "responses": self._rows,
            "correlation": actual,
            "summary": _summary(self._rows),
        }
        raw = _bytes(value)
        replay(raw, bindings, diagnostic_counters, correlation)
        return raw


def replay(raw, bindings, diagnostic_counters, correlation):
    """Check exact caller-supplied artifact/correlation bindings, not live authority."""
    if type(raw) is not bytes or not 0 < len(raw) <= MAX_BYTES:
        _fail()
    try:
        value = strict_json(raw.decode("ascii"))
    except (ValueError, UnicodeError, TypeError, RecursionError):
        _fail()
    if type(value) is not dict or set(value) != {
        "schema_version",
        "bindings",
        "responses",
        "correlation",
        "summary",
    }:
        _fail()
    if type(value["schema_version"]) is not int or value["schema_version"] != 1 or _bytes(value) != raw:
        _fail()
    _bindings(bindings)
    if _bytes(value["bindings"]) != _bytes(bindings):
        _fail()
    summary = _summary(value["responses"])
    if _bytes(value["summary"]) != _bytes(summary) or _bytes(value["correlation"]) != _bytes(correlation):
        _fail()
    _correlation(correlation, value["responses"])
    _diagnostic(summary, diagnostic_counters)
    return summary


def completion(raw, capture_sha256):
    """Bind sealed bytes to a later exact capture; caller must replay separately."""
    if type(raw) is not bytes or not 0 < len(raw) <= MAX_BYTES or not _hash(capture_sha256):
        _fail()
    return {
        "schema_version": 1,
        "sidecar_sha256": hashlib.sha256(raw).hexdigest(),
        "capture_sha256": capture_sha256,
    }


def replay_completed(raw, receipt, capture_sha256, bindings, diagnostic_counters, correlation):
    if _bytes(receipt) != _bytes(completion(raw, capture_sha256)):
        _fail()
    return replay(raw, bindings, diagnostic_counters, correlation)
