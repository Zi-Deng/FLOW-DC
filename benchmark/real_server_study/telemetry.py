"""Study-only observation/dispatch guards; controller feedback remains native."""
import asyncio
import contextvars
import json
import math
import os
import resource
import statistics
import time
from collections import deque
from email.utils import parsedate_to_datetime

from .common import checked_url, finite, require, tls_context


def retry_until(value, now):
    if value is None:
        return now
    try:
        seconds = float(value)
        return now + max(0, seconds) if math.isfinite(seconds) else now
    except ValueError:
        try:
            return max(now, parsedate_to_datetime(value).timestamp())
        except (TypeError, ValueError, OverflowError):
            return now


class StopPolicy:
    def __init__(self, baseline):
        require(finite(baseline) and baseline > 0, "missing positive probe delay")
        self.baseline, self.reason, self.fatal = baseline, None, False
        self.headers, self.completed = deque(), deque()
        self.refusals, self.bad_latency = 0, 0
        self.cooldown_until = 0
        self.bucket = None
        self.origin = None

    def header(self, now, status, retry_after=None):
        self.headers.append((now, status))
        while self.headers and self.headers[0][0] < now - 30:
            self.headers.popleft()
        self.refusals += status in (429, 503)
        self.cooldown_until = max(self.cooldown_until, retry_until(retry_after, time.time()))
        if status in (401, 403):
            self.reason, self.fatal = "access_refusal", True
        elif self.refusals >= 5 or (len(self.headers) >= 200 and
                sum(s in (429, 503) for _, s in self.headers) / len(self.headers) >= .01):
            self.reason = self.reason or "refusal_threshold"
        if self.reason:
            self.cooldown_until = max(self.cooldown_until, time.time() + 600)

    def complete(self, now, delay):
        if delay is not None:
            self.completed.append((now, delay))

    def tick(self, now):
        if self.origin is None:
            self.origin = now
        bucket = int((now-self.origin) // 10)
        if self.bucket is None:
            self.bucket = bucket
        elif bucket != self.bucket:
            delays = [d for t, d in self.completed if self.origin+10*self.bucket <= t < self.origin+10*(self.bucket+1)]
            self.bad_latency = self.bad_latency + 1 if len(delays) >= 100 and statistics.median(delays) >= 3*self.baseline else 0
            self.bucket = bucket
            if self.bad_latency >= 3:
                self.reason = self.reason or "sustained_latency"
                self.cooldown_until = max(self.cooldown_until, time.time() + 600)
        while self.completed and self.completed[0][0] < now - 40:
            self.completed.popleft()


class Guard:
    def __init__(self, allowed, stream, baseline, ignore_time=False):
        self.allowed, self.stream = set(allowed), stream
        self.policy = StopPolicy(baseline)
        self.context = tls_context(ignore_time)
        self.counts, self.active, self.finished = {}, set(), set()
        self.token = contextvars.ContextVar("flowdc-study-request", default=None)
        self.semaphore = None
        self.last_flush, self.last_stop = 0, None
        self.sampler = None

    def emit(self, event, **fields):
        now = time.monotonic()
        self.stream.write(json.dumps({"event": event, "monotonic_s": now, **fields}, allow_nan=False) + "\n")
        if now - self.last_flush >= .25 or event in ("stop", "closed"):
            self.stream.flush()
            self.last_flush = now

    def check(self):
        if self.policy.reason:
            if self.last_stop != self.policy.reason:
                self.emit("stop", reason=self.policy.reason, fatal=self.policy.fatal,
                          cooldown_until_utc=self.policy.cooldown_until)
                self.last_stop = self.policy.reason
            raise RuntimeError("study dispatch stopped: " + self.policy.reason)

    async def sample(self):
        next_at = time.monotonic()
        try:
            while True:
                now = time.monotonic()
                self.policy.tick(now)
                self.emit("sample", inflight=len(self.active),
                          cap=self.semaphore.limit if self.semaphore else None,
                          backlog=max(0, len(self.allowed)-len(self.finished)-256),
                          loop_lag_s=max(0, now-next_at),
                          max_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                          cpu_s=sum(os.times()[:2]))
                if self.policy.reason and self.last_stop != self.policy.reason:
                    self.emit("stop", reason=self.policy.reason, fatal=self.policy.fatal,
                              cooldown_until_utc=self.policy.cooldown_until)
                    self.last_stop = self.policy.reason
                next_at = now + 1
                await asyncio.sleep(1)
        finally:
            self.emit("sampler_end", active=len(self.active))

    def install(self):
        import aiohttp
        import flowdc_methods
        import single_download
        require(aiohttp.__version__ == "3.13.3", "pinned aiohttp required")
        init, request = aiohttp.ClientSession.__init__, aiohttp.ClientSession._request
        fetch, candidate = single_download.download_via_http_get, flowdc_methods.CandidateController.__init__

        def observe_candidate(obj, *args, **kwargs):
            candidate(obj, *args, **kwargs)
            self.semaphore = obj.semaphore

        async def dispatch(session, ctx, params):
            self.check()
            url = checked_url(str(params.url))
            require(url in self.allowed, "unplanned dispatch")
            token = self.token.get()
            require(token is not None and token not in self.active, "duplicate or unbound dispatch")
            require(len(self.active) < 256, "external concurrency ceiling exceeded")
            self.active.add(token)
            self.emit("dispatch", token=token, url=url, inflight=len(self.active),
                      cap=self.semaphore.limit if self.semaphore else None)

        def guarded_init(obj, *args, **kwargs):
            require(not kwargs.get("trust_env"), "proxy environment prohibited")
            trace = aiohttp.TraceConfig()
            trace.on_request_headers_sent.append(dispatch)
            kwargs["trace_configs"] = [*kwargs.get("trace_configs", []), trace]
            init(obj, *args, **kwargs)
            obj._retry_connection = False
            if self.sampler is None:
                self.sampler = asyncio.get_running_loop().create_task(self.sample())

        async def guarded_request(obj, method, url, **kwargs):
            self.check()
            url = checked_url(str(url))
            require(method == "GET" and url in self.allowed, "unplanned request")
            require(not kwargs.get("proxy") and not kwargs.get("auth") and not kwargs.get("params"), "request override prohibited")
            kwargs.update(allow_redirects=False, ssl=self.context, proxy=None)
            self.emit("request", token=self.token.get(), url=url)
            try:
                response = await request(obj, method, url, **kwargs)
                connection = response.connection
                transport = connection.transport if connection else None
                peer = transport.get_extra_info("peername") if transport else None
                self.policy.header(time.monotonic(), response.status, response.headers.get("Retry-After"))
                self.emit("headers", token=self.token.get(), url=url, status=response.status,
                          retry_after=response.headers.get("Retry-After"), peer=peer,
                          peer_unavailable_reason=None if peer else "response_connection_not_exposed")
                # Release the response even if subsequent requests are now stopped.
                return response
            except Exception as error:
                if "CERTIFICATE_VERIFY_FAILED" in str(error) or "SSLCertVerificationError" in type(error).__name__:
                    self.policy.reason, self.policy.fatal = "tls_failure", True
                self.emit("error", token=self.token.get(), error=f"{type(error).__name__}: {error}")
                raise

        async def observed_fetch(session, url, timeout):
            require(url in self.allowed, "unplanned logical attempt")
            self.counts[url] = self.counts.get(url, 0) + 1
            require(self.counts[url] == 1, "study permits one logical request per URL")
            self.emit("attempt", token=url, url=url)
            context = self.token.set(url)
            measurement = single_download.HTTP_TRACE_CTX.get()
            status = None
            try:
                result = await fetch(session, url, timeout)
                status = result[1]
                return result
            finally:
                was_active = url in self.active
                self.active.discard(url)
                self.finished.add(url)
                delay = measurement.get("ttfb") if measurement and measurement.get("latency_eligible") else None
                self.policy.complete(time.monotonic(), delay)
                self.emit("network_end", token=url, dispatched=was_active, inflight=len(self.active),
                          status=status, ttfb_s=delay,
                          failure_kind=measurement.get("failure_kind") if measurement else None)
                self.token.reset(context)

        flowdc_methods.CandidateController.__init__ = observe_candidate
        aiohttp.ClientSession.__init__, aiohttp.ClientSession._request = guarded_init, guarded_request
        single_download.download_via_http_get = observed_fetch

        def restore():
            flowdc_methods.CandidateController.__init__ = candidate
            aiohttp.ClientSession.__init__, aiohttp.ClientSession._request = init, request
            single_download.download_via_http_get = fetch
        return restore
