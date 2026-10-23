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


def partial_transport(directory, body_limit):
    """Observed refusal evidence; incomplete clocks never become full-run rates."""
    directory = Path(directory)
    files = [directory / "request-audit.jsonl"]
    if list(directory.glob("tool-request-*.jsonl")):
        files = sorted(directory.glob("tool-request-*.jsonl"))
    statuses, reasons, attempts = Counter(), set(), set()
    invalid = 0
    server_until = 0
    for path in files:
        if not path.exists():
            continue
        with path.open() as stream:
            for line in stream:
                try:
                    event = json.loads(line)
                except (ValueError, UnicodeError):
                    invalid += 1
                    continue
                if event.get("event") == "headers":
                    statuses[str(event["status"])] += 1
                    server_until = max(server_until, event.get("server_retry_after_until_utc", 0))
                if event.get("event") == "attempt":
                    attempts.add(event["url"])
                if event.get("reason"):
                    reasons.add(event["reason"])
                if event.get("stop_reason"):
                    reasons.add(event["stop_reason"])
    category = ("confirmed_rate_limit" if statuses["429"] else
                "access_refusal_cause_unknown" if statuses["401"] or statuses["403"] else
                "service_unavailable" if statuses["503"] else
                "performance_degradation" if "sustained_latency" in reasons else "interrupted_or_failed")
    return dict(attempts=len(attempts), status_counts=dict(statuses), stop_reasons=sorted(reasons),
                server_retry_after_until_utc=server_until,
                classification=category, malformed_lines=invalid, incomplete=True,
                body_bytes=body_limit, body_bytes_basis="conservative reservation; actual count incomplete",
                http_span_s=None, mean_http_concurrency=None, request_seconds=None)


def progress(directory, payloads):
    """Compact post-verified acquisition curve; not bytes usable at that time.

    Raw journals remain on the manager. Time is relative to the first observed
    HTTP start. Credit is assigned retrospectively only to verified payload URLs.
    """
    import heapq
    from .common import file_hash
    directory = Path(directory)
    paths = sorted(directory.glob('tool-request-*.jsonl')) or [directory / 'request-audit.jsonl']
    paths = [p for p in paths if p.exists()]
    invalid = [0]

    def events(path):
        previous = -math.inf
        with path.open(errors='replace') as stream:
            for line in stream:
                try:
                    value = json.loads(line)
                    now = float(value['monotonic_s'])
                    require(math.isfinite(now) and now >= previous, 'invalid progress clock')
                    previous = now
                    yield value
                except (ValueError, KeyError, TypeError):
                    invalid[0] += 1
    eligible = {v['url']: v['bytes'] for v in payloads.values()}
    active, credited, pending = set(), set(), {}
    bins, refusals = {}, []
    start, rows, body = None, 0, 0
    statuses = Counter()
    for event in heapq.merge(*(events(p) for p in paths), key=lambda e: e['monotonic_s']):
        kind, token, now = event['event'], event.get('token'), event['monotonic_s']
        if kind == 'http_start':
            if start is None:
                start = now
            active.add(token)
            pending[token] = now
        if start is None:
            continue
        tick = int((now-start)//5)
        bucket = bins.setdefault(tick, dict(second=5*(tick+1), credited_rows=rows,
            credited_bytes=body, peak_http_inflight=len(active), completed_http=0, first_byte_delays_s=[]))
        bucket['peak_http_inflight'] = max(bucket['peak_http_inflight'],len(active))
        if kind == 'http_first_byte' and token in pending:
            bucket['first_byte_delays_s'].append(now-pending[token])
        if kind == 'headers':
            status = str(event['status']); statuses[status] += 1
            if status in ('401','403','429','503') and len(refusals) < 256:
                refusals.append(dict(second=now-start,status=status,retry_after=event.get('retry_after')))
        if kind == 'network_end':
            active.discard(token); pending.pop(token,None)
            bucket['completed_http'] += 1
            if token in eligible and token not in credited:
                credited.add(token); body += eligible[token]; rows += 1
        bucket.update(credited_rows=rows,credited_bytes=body)
    for bucket in bins.values():
        latency = sorted(bucket.pop('first_byte_delays_s'))
        bucket['ttfb_p95_s'] = latency[math.ceil(.95*len(latency))-1] if latency else None
    return dict(schema='flowdc-acquisition-progress-v1', interval_seconds=5,
        interpretation='Retrospectively verified bytes at HTTP completion; excludes packaging/return/audit time',
        points=list(bins.values()), status_counts=dict(statuses), refusal_events=refusals,
        malformed_events=invalid[0], complete_credit=rows==len(payloads),
        raw_journals={p.name:file_hash(p) for p in paths})
