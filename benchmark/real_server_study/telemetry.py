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
    def __init__(self, allowed, stream, baseline, ignore_time=False, policy=None, body_limit=None):
        self.allowed, self.stream = set(allowed), stream
        self.policy = policy or StopPolicy(baseline)
        self.context = tls_context(ignore_time)
        self.counts, self.active, self.finished = {}, set(), set()
        self.token = contextvars.ContextVar("flowdc-study-request", default=None)
        self.semaphore = None
        self.last_flush, self.last_stop = 0, None
        self.sampler = None
        self.active_since, self.body_bytes, self.body_reserved = {}, 0, 0
        self.body_limit = body_limit
        self.readers, self.reader_ids = {}, set()

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
                          fatal_reason=getattr(self.policy, 'fatal_reason', None),
                          inflight=len(self.active), cap=self.semaphore.limit if self.semaphore else None,
                          cooldown_until_utc=self.policy.cooldown_until)
                self.last_stop = self.policy.reason
            raise RuntimeError("study dispatch stopped: " + self.policy.reason)

    async def sample(self):
        next_at = time.monotonic()
        try:
            while True:
                now = time.monotonic()
                if hasattr(self.policy, "sample"):
                    self.policy.sample(now, len(self.active), bool(len(self.allowed)-len(self.finished)-len(self.active)),
                                       max((now-t for t in self.active_since.values()), default=0),
                                       max(0, now-next_at), sum(os.times()[:2]))
                self.policy.tick(now)
                self.emit("sample", inflight=len(self.active),
                          cap=self.semaphore.limit if self.semaphore else None,
                          backlog=max(0, len(self.allowed)-len(self.finished)-len(self.active)),
                          oldest_pending_s=max((now-t for t in self.active_since.values()), default=0),
                          observed_body_bytes=self.body_bytes,
                          file_descriptors=len(os.listdir('/proc/self/fd')),
                          loop_lag_s=max(0, now-next_at),
                          max_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                          cpu_s=sum(os.times()[:2]))
                if self.policy.reason and self.last_stop != self.policy.reason:
                    self.emit("stop", reason=self.policy.reason, fatal=self.policy.fatal,
                              fatal_reason=getattr(self.policy, 'fatal_reason', None),
                              inflight=len(self.active), cap=self.semaphore.limit if self.semaphore else None,
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
        read = aiohttp.StreamReader.read

        async def observed_read(obj, n=-1):
            if id(obj) not in self.reader_ids or self.body_limit is None:
                return await read(obj, n)
            # Reserve before awaiting: simultaneous reads cannot overdraw the budget.
            require(n >= 0, 'unbounded study body read refused')
            wanted = n
            available = self.body_limit-self.body_bytes-self.body_reserved
            if wanted > available:
                self.policy.reason = self.policy.reason or 'body_budget'
                self.policy.fatal, self.policy.fatal_reason = True, 'body_budget'
                self.check()
            self.body_reserved += wanted
            try:
                value = await read(obj, n)
                self.body_bytes += len(value)
                return value
            finally:
                self.body_reserved -= wanted

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
            self.active_since[token] = time.monotonic()
            self.emit("dispatch", token=token, url=url, inflight=len(self.active),
                      user_agent=params.headers.get('User-Agent'),
                      cap=self.semaphore.limit if self.semaphore else None)

        def guarded_init(obj, *args, **kwargs):
            require(not kwargs.get("trust_env"), "proxy environment prohibited")
            trace = aiohttp.TraceConfig()
            trace.on_request_headers_sent.append(dispatch)
            kwargs["trace_configs"] = [*kwargs.get("trace_configs", []), trace]
            async def queued(session, ctx, params):
                self.emit('connection_queue', token=self.token.get(), phase='start')
            async def dequeued(session, ctx, params):
                self.emit('connection_queue', token=self.token.get(), phase='end')
            trace.on_connection_queued_start.append(queued)
            trace.on_connection_queued_end.append(dequeued)
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
                reader_id = id(response.content)
                self.readers[self.token.get()] = reader_id
                self.reader_ids.add(reader_id)
                connection = response.connection
                transport = connection.transport if connection else None
                peer = transport.get_extra_info("peername") if transport else None
                self.policy.header(time.monotonic(), response.status, response.headers.get("Retry-After"))
                self.emit("headers", token=self.token.get(), url=url, status=response.status,
                          retry_after=response.headers.get("Retry-After"), peer=peer,
                          rate_headers={k: v for k, v in response.headers.items() if k.lower() in
                            ('ratelimit', 'ratelimit-policy', 'ratelimit-limit', 'ratelimit-remaining',
                             'ratelimit-reset', 'x-ratelimit-limit', 'x-ratelimit-remaining', 'x-ratelimit-reset')},
                          peer_unavailable_reason=None if peer else "response_connection_not_exposed")
                # Release the response even if subsequent requests are now stopped.
                return response
            except Exception as error:
                if "CERTIFICATE_VERIFY_FAILED" in str(error) or "SSLCertVerificationError" in type(error).__name__:
                    self.policy.reason = self.policy.reason or 'tls_failure'
                    self.policy.fatal, self.policy.fatal_reason = True, 'tls_failure'
                self.emit("error", token=self.token.get(), error=f"{type(error).__name__}: {error}",
                          fatal=self.policy.fatal, fatal_reason=getattr(self.policy, 'fatal_reason', None))
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
                self.active_since.pop(url, None)
                self.reader_ids.discard(self.readers.pop(url, None))
                self.finished.add(url)
                delay = measurement.get("ttfb") if measurement and measurement.get("latency_eligible") else None
                self.policy.complete(time.monotonic(), delay)
                if hasattr(self.policy, 'network_end'):
                    self.policy.network_end(time.monotonic(), status, measurement or {})
                self.emit("network_end", token=url, dispatched=was_active, inflight=len(self.active),
                          status=status, ttfb_s=delay,
                          body_delay_s=measurement.get('body_delay') if measurement else None,
                          observed_bytes=measurement.get('observed_response_body_bytes', 0) if measurement else 0,
                          failure_kind=measurement.get("failure_kind") if measurement else None)
                self.token.reset(context)

        flowdc_methods.CandidateController.__init__ = observe_candidate
        aiohttp.ClientSession.__init__, aiohttp.ClientSession._request = guarded_init, guarded_request
        single_download.download_via_http_get = observed_fetch
        aiohttp.StreamReader.read = observed_read

        def restore():
            flowdc_methods.CandidateController.__init__ = candidate
            aiohttp.ClientSession.__init__, aiohttp.ClientSession._request = init, request
            single_download.download_via_http_get = fetch
            aiohttp.StreamReader.read = read
        return restore
