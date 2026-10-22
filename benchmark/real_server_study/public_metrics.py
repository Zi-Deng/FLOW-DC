"""Streaming, cross-tool request-boundary accounting (not controller signals)."""

import json
import math
from collections import Counter
from pathlib import Path

from .common import require


def transport(path, ceiling):
    attempted, ended, active, http, first_bytes = set(), set(), set(), {}, set()
    statuses = Counter()
    first, last, previous, peak, common_peak = None, None, -1.0, 0, 0
    area, latency, ends, closure = 0.0, [], 0, None
    with Path(path).open() as stream:
        for number, line in enumerate(stream):
            event = json.loads(line)
            now, kind = event["monotonic_s"], event["event"]
            require(now >= previous, "request clock reversed")
            previous = now
            if number == 0:
                require(kind == "start", "request journal lacks start")
            token = event.get("token")
            if kind == "attempt":
                require(event["url"] not in attempted, "repeat URL")
                attempted.add(event["url"])
            elif kind == "http_start":
                require(token in attempted and token not in http and token not in ended, "invalid HTTP start")
                require(len(http) < ceiling, "common HTTP ceiling exceeded")
                http[token] = now
                first = now if first is None else first
                common_peak = max(common_peak, len(http))
            elif kind == "http_first_byte":
                require(token in http and token not in first_bytes, "invalid first byte")
                first_bytes.add(token)
                latency.append(now - http[token])
            elif kind == "dispatch":
                require(token not in active and len(active) < ceiling, "duplicate/excess native dispatch")
                active.add(token)
                peak = max(peak, len(active))
                require(event["inflight"] == len(active), "native inflight mismatch")
            elif kind == "headers":
                statuses[str(event["status"])] += 1
            elif kind == "network_end":
                require(
                    token not in ended and event["dispatched"] == (token in active), "network closure differs"
                )
                ended.add(token)
                active.discard(token)
                if token in http:
                    area += now - http.pop(token)
                    last = now
                    ends += 1
            elif kind == "closed":
                require(closure is None, "duplicate transport closure")
                closure = event
    require(
        closure and kind == "closed" and not closure.get("stop_reason") and not active and not http,
        "stopped/unclosed transport",
    )
    require(
        len(ended) == len(attempted) == closure["logical_attempts"] and closure["active"] == 0,
        "attempt denominator incomplete",
    )
    span = last - first if first is not None and last is not None else 0
    require(span > 0 and ends > 0, "no measurable HTTP period")
    latency.sort()
    return dict(
        attempts=len(attempted),
        body_bytes=closure["body_bytes"],
        native_peak=peak,
        common_peak=common_peak,
        http_span_s=span,
        request_seconds=area,
        mean_http_concurrency=area / span,
        common_ttfb_p50_s=latency[len(latency) // 2] if latency else None,
        common_ttfb_p95_s=latency[math.ceil(0.95 * len(latency)) - 1] if latency else None,
        status_counts=dict(statuses),
        boundary="before connection setup through body/failure completion",
    )
