"""Optional v7 structural observations around the byte-frozen v7 evaluator.

Acceptance, inspection, reporting projection and reasons come from the original
capture. A second bounded scan classifies exactly its partial-event dispatches;
no payload survives this pass. No mutation of frozen globals or closure state.
"""

import claude_partial_observation
import claude_reporting
import claude_reporting_policy as reporting_policy
import claude_telemetry_v7 as frozen
import review_coverage as coverage


def validate_summary(value):
    if type(value) is dict and "partial_stream" in value:
        original = {k: v for k, v in value.items() if k != "partial_stream"}
        frozen.validate_summary(original)
        claude_partial_observation.validate(value["partial_stream"], original["types"].get("stream_event", 0))
    else:
        frozen.validate_summary(value)


class PartialStream:
    """Validate partial-message framing without treating partial reads as evidence."""

    def __init__(self, model, reasons):
        self.model, self.reasons = model, reasons
        self.active = None
        self._observation = claude_partial_observation.Observation()
        self.messages, self.blocks = set(), {}

    def observation(self):
        return self._observation.record()

    def observe(self, event):
        # Set the fixed predicate BEFORE evaluating it: TypeError/KeyError must
        # identify the evaluation boundary without rendering the offending value.
        guard = "event_object"
        try:
            if type(event) is not dict:
                raise ValueError
            kind = event.get("type")
            if kind == "message_start":
                message = event.get("message")
                guard = "message_inactive"
                if self.active is not None:
                    raise ValueError
                guard = "message_object"
                if type(message) is not dict:
                    raise ValueError
                guard = "message_id"
                if not claude_reporting._identifier(message.get("id")):
                    raise ValueError
                guard = "message_unique"
                if message["id"] in self.messages:
                    raise ValueError
                guard = "message_model"
                if message.get("model") != self.model:
                    raise ValueError
                self.active = message["id"]
                self.messages.add(self.active)
                self.blocks = {}
                return
            guard = "message_active"
            if self.active is None:
                raise ValueError
            guard = "event_kind"
            if kind in {"content_block_start", "content_block_delta", "content_block_stop"}:
                index = event.get("index")
                guard = "block_index"
                if type(index) is not int or not 0 <= index < coverage.MAX_EVENTS:
                    raise ValueError
                if kind == "content_block_start":
                    block = event.get("content_block")
                    guard = "block_unique"
                    if index in self.blocks:
                        raise ValueError
                    guard = "block_object"
                    if type(block) is not dict:
                        raise ValueError
                    block_type = block.get("type")
                    if block_type == "tool_use":
                        guard = "tool_name"
                        if block.get("name") not in reporting_policy.TOOLS:
                            raise ValueError
                        guard = "tool_id"
                        if not claude_reporting._identifier(block.get("id")):
                            raise ValueError
                        guard = "tool_input"
                        if block.get("input") != {}:
                            raise ValueError
                    else:
                        guard = "block_kind"
                        if block_type not in {"text", "thinking", "redacted_thinking"}:
                            raise ValueError
                    self.blocks[index] = block_type
                else:
                    guard = "block_open"
                    if index not in self.blocks or self.blocks[index] is None:
                        raise ValueError
                    if kind == "content_block_stop":
                        self.blocks[index] = None
                    else:
                        delta = event.get("delta")
                        guard = "delta_object"
                        if type(delta) is not dict:
                            raise ValueError
                        allowed = {
                            "tool_use": {"input_json_delta": "partial_json"},
                            "text": {"text_delta": "text"},
                            "thinking": {"thinking_delta": "thinking", "signature_delta": "signature"},
                            "redacted_thinking": {},
                        }[self.blocks[index]]
                        guard = "delta_kind"
                        field = allowed.get(delta.get("type"))
                        if field is None:
                            raise ValueError
                        guard = "delta_fields"
                        if set(delta) != {"type", field}:
                            raise ValueError
                        guard = "delta_text"
                        if not isinstance(delta[field], str):
                            raise ValueError
            elif kind == "message_delta":
                guard = "message_delta_object"
                if type(event.get("delta")) is not dict:
                    raise ValueError
                guard = "message_usage_object"
                if type(event.get("usage")) is not dict:
                    raise ValueError
            elif kind == "message_stop":
                guard = "message_blocks_closed"
                if any(v is not None for v in self.blocks.values()):
                    raise ValueError
                self.active = None
            else:
                raise ValueError
        except (ValueError, TypeError, KeyError) as exc:
            self.reasons.add("unsupported_partial_stream")
            self._observation.reject(
                guard, type(exc) in {TypeError, KeyError}, self.active, self.blocks, event
            )


def observe(raw, model):
    """Mirror only the frozen raw/line/type bounds before partial dispatch.

    The frozen evaluator dispatches every parsed stream_event to PartialStream,
    even when separate session/delegation/initialization checks also reject it.
    This scan must not filter those events or claim their surrounding controls pass.
    """
    stream = PartialStream(model, set())
    try:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        if not isinstance(raw, str) or len(raw.encode("utf-8")) > coverage.MAX_STREAM_BYTES:
            raise ValueError
    except (UnicodeError, ValueError):
        return stream.observation()
    count = 0
    for line in raw.split("\n"):
        if not line.strip():
            continue
        if count >= coverage.MAX_EVENTS:
            break
        count += 1
        try:
            event = coverage.strict_json(line)
            if (
                isinstance(event, dict)
                and isinstance(event.get("type"), str)
                and event["type"] == "stream_event"
            ):
                stream.observe(event.get("event"))
        except (ValueError, TypeError, KeyError, UnicodeError, RecursionError):
            continue
    return stream.observation()


def capture(raw, packet, workspace, policy, session_id, **kwargs):
    body, diagnostics, proof = frozen.capture(raw, packet, workspace, policy, session_id, **kwargs)
    diagnostics["telemetry"]["partial_stream"] = observe(raw, policy["model"])
    return body, diagnostics, proof
