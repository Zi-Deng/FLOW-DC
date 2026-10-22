"""Public-study transport observation; no proxy, payload replay or controller change."""

import json
import ssl
import threading
import time
import urllib.error
import urllib.request
from urllib.parse import urlsplit

from . import telemetry
from .common import require, tls_context

SOURCES = {
    "taibif-moth": ("moth.tbn.org.tw", "/images/"),
    "openimages-cvdf": ("open-images-dataset.s3.amazonaws.com", "/train/"),
}
UA = "FLOW-DC-research/real-world-focused (bounded dataset acquisition)"
MAX_OBJECT = 64 * 2**20


def checked_url(url, source):
    host, prefix = SOURCES[source]
    p = urlsplit(url)
    require(
        p.scheme == "https"
        and p.hostname == host
        and p.port in (None, 443)
        and p.path.startswith(prefix)
        and not p.username
        and not p.password
        and not p.query
        and not p.fragment
        and not any(c.isspace() for c in url),
        "URL outside frozen public source",
    )
    return url


class PublicPolicy(telemetry.StopPolicy):
    def __init__(self, baseline):
        super().__init__(baseline)
        self.total_headers = self.dead = self.total_ends = self.transport_errors = 0

    def header(self, now, status, retry_after=None):
        super().header(now, status, retry_after)
        self.total_headers += 1
        self.dead += status in (404, 410)
        if self.total_headers >= 100 and self.dead / self.total_headers > 0.05:
            self.reason, self.fatal = "dead_link_fraction", True
        if status in (401, 403, 429, 503) or 300 <= status < 400:
            self.reason, self.fatal = "public_access_stop", True
            self.cooldown_until = max(self.cooldown_until, time.time() + 1800)

    def inspect(self, headers):
        directives = str(headers.get("X-Robots-Tag", "")).lower()
        if any(x in directives for x in ("noai", "noimageai", "noindex", "noimageindex")):
            self.reason, self.fatal = "robots_header", True

    def network_end(self, now, status, measurement):
        self.total_ends += 1
        self.transport_errors += measurement.get("failure_kind") == "transport"
        if self.total_ends >= 100 and self.transport_errors / self.total_ends > 0.02:
            self.reason, self.fatal = "transport_fraction", True

    def tick(self, now):
        if self.origin is None:
            self.origin = now
        bucket = int((now - self.origin) // 10)
        if self.bucket is not None and bucket != self.bucket:
            import statistics

            delays = [
                d
                for t, d in self.completed
                if self.origin + 10 * self.bucket <= t < self.origin + 10 * (self.bucket + 1)
            ]
            self.bad_latency = (
                self.bad_latency + 1
                if len(delays) >= 20 and statistics.median(delays) > 5 * self.baseline
                else 0
            )
            if self.bad_latency >= 3:
                self.reason, self.fatal = "sustained_latency", True
        self.bucket = bucket
        while self.completed and self.completed[0][0] < now - 40:
            self.completed.popleft()


class FlowGuard(telemetry.Guard):
    def __init__(self, source, allowed, stream, baseline, ignore_time=False, body_limit=2**30):
        require(not ignore_time or source == "taibif-moth", "TLS exception is moth-host only")
        super().__init__(
            allowed, stream, baseline, ignore_time, policy=PublicPolicy(baseline), body_limit=body_limit
        )
        self.source = source

    def install(self):
        original = telemetry.checked_url
        telemetry.checked_url = lambda url: checked_url(url, self.source)
        restore = super().install()
        import aiohttp

        init, request = aiohttp.ClientSession.__init__, aiohttp.ClientSession._request

        def identified(obj, *args, **kwargs):
            kwargs["headers"] = {**dict(kwargs.get("headers") or {}), "User-Agent": UA}
            init(obj, *args, **kwargs)

        async def inspected(obj, method, url, **kwargs):
            response = await request(obj, method, url, **kwargs)
            self.policy.inspect(response.headers)
            if self.policy.reason:
                response.close()
                self.check()
            return response

        aiohttp.ClientSession.__init__ = identified
        aiohttp.ClientSession._request = inspected

        def undo():
            restore()
            telemetry.checked_url = original

        return undo


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class ToolGuard:
    """Observe urllib in the pinned tool process, keeping its decision/output code."""

    def __init__(
        self, source, allowed, stream, baseline, *, ignore_time=False, body_limit=2**30, ceiling=192
    ):
        require(not ignore_time or source == "taibif-moth", "TLS exception is moth-host only")
        self.source, self.allowed, self.stream = source, set(allowed), stream
        self.policy, self.lock = PublicPolicy(baseline), threading.RLock()
        self.started = {}
        self.counts, self.active, self.body_bytes, self.reserved = set(), set(), 0, 0
        self.body_limit, self.ceiling = body_limit, ceiling
        self.context = tls_context(ignore_time)
        self.opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({}), NoRedirect(), urllib.request.HTTPSHandler(context=self.context)
        )

    def emit(self, event, **fields):
        with self.lock:
            self.stream.write(
                json.dumps(dict(event=event, monotonic_s=time.monotonic(), **fields), allow_nan=False) + "\n"
            )
            if event in ("closed", "stop"):
                self.stream.flush()

    def check(self):
        if self.policy.reason:
            raise RuntimeError("public dispatch stopped: " + self.policy.reason)

    def begin(self, url):
        with self.lock:
            self.check()
            require(checked_url(url, self.source) in self.allowed, "unplanned tool URL")
            require(
                url not in self.counts and len(self.active) < self.ceiling, "duplicate/excess tool request"
            )
            self.counts.add(url)
            self.active.add(url)
            self.started[url] = time.monotonic()
            self.emit("attempt", token=url, url=url)
            self.emit("dispatch", token=url, url=url, inflight=len(self.active), cap=self.ceiling)

    def end(self, url, status, error=None):
        with self.lock:
            if url not in self.active:
                return
            self.active.remove(url)
            self.policy.network_end(
                time.monotonic(), status, {"failure_kind": "transport" if error and status is None else None}
            )
            self.emit(
                "network_end",
                token=url,
                dispatched=True,
                inflight=len(self.active),
                status=status,
                ttfb_s=None,
                error=error,
            )

    def header(self, url, response):
        with self.lock:
            self.policy.inspect(response.headers)
            self.policy.header(time.monotonic(), response.code, response.headers.get("Retry-After"))
            self.emit(
                "headers",
                token=url,
                url=url,
                status=response.code,
                retry_after=response.headers.get("Retry-After"),
            )
            if self.policy.reason:
                self.emit("stop", reason=self.policy.reason, inflight=len(self.active))
            self.check()

    def install(self):
        original = urllib.request.urlopen
        guard = self

        class Response:
            def __init__(self, response, url):
                self.response, self.url, self.received = response, url, 0
                self.headers, self.code = response.headers, response.code

            def __enter__(self):
                return self

            def __exit__(self, typ, value, tb):
                self.response.close()
                guard.end(self.url, self.code, str(value) if value else None)

            def read(self, n=-1):
                chunks, left = [], n
                while left != 0:
                    with guard.lock:
                        guard.check()
                        available = guard.body_limit - guard.body_bytes - guard.reserved
                        if available <= 0:
                            guard.policy.reason, guard.policy.fatal = "body_budget", True
                            guard.check()
                        wanted = min(
                            1 if self.received == 0 else 65536,
                            available,
                            MAX_OBJECT - self.received + 1,
                            left if left > 0 else 65536,
                        )
                        require(wanted > 0, "public object budget exhausted")
                        guard.reserved += wanted
                    try:
                        raw = self.response.read(wanted)
                    finally:
                        with guard.lock:
                            guard.reserved -= wanted
                    with guard.lock:
                        guard.body_bytes += len(raw)
                        if self.received == 0 and raw:
                            guard.policy.complete(
                                time.monotonic(), time.monotonic() - guard.started[self.url]
                            )
                            guard.policy.tick(time.monotonic())
                        self.received += len(raw)
                        if self.received > MAX_OBJECT:
                            guard.policy.reason, guard.policy.fatal = "object_budget", True
                            guard.check()
                    if not raw:
                        break
                    chunks.append(raw)
                    if left > 0:
                        left -= len(raw)
                return b"".join(chunks)

        def observed(request, data=None, timeout=30, **kwargs):
            require(data is None and not kwargs, "tool HTTP overrides prohibited")
            url = request.full_url if isinstance(request, urllib.request.Request) else str(request)
            require(
                not isinstance(request, urllib.request.Request) or request.get_method() == "GET",
                "tool method prohibited",
            )
            guard.begin(url)
            try:
                headers = dict(request.header_items()) if isinstance(request, urllib.request.Request) else {}
                require(
                    not any(k.lower() in ("authorization", "proxy-authorization", "cookie") for k in headers),
                    "tool credential override prohibited",
                )
                request = urllib.request.Request(url, headers={**headers, "User-Agent": UA})
                response = guard.opener.open(request, timeout=timeout)
                try:
                    guard.header(url, response)
                except BaseException:
                    response.close()
                    raise
                return Response(response, url)
            except BaseException as error:
                if isinstance(error, urllib.error.HTTPError):
                    try:
                        guard.header(url, error)
                    except Exception:
                        pass
                    error.close()
                elif isinstance(error, ssl.SSLError) or "CERTIFICATE_VERIFY_FAILED" in str(error):
                    guard.policy.reason, guard.policy.fatal = "tls_failure", True
                guard.end(url, getattr(error, "code", None), type(error).__name__)
                raise

        urllib.request.urlopen = observed
        return lambda: setattr(urllib.request, "urlopen", original)

    def close(self):
        self.emit(
            "closed",
            active=len(self.active),
            logical_attempts=len(self.counts),
            body_bytes=self.body_bytes,
            stop_reason=self.policy.reason,
        )
