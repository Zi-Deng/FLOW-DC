# Asynchronous HTTP measurement and admission

Both `download_batch.py` and `download_batch_gradient.py` use the same helper,
tracing and outcome classification. Overview reports label these semantics as
`http_measurement.version = "2-body-first-byte"`. Earlier TTFB observations included
whole-body reads and are not directly comparable. The method's equations and
configuration defaults have not changed; corrected inputs can change its decisions.

## Timing boundary

All event timestamps and intervals use `time.monotonic()` within one process.
They are application observations, not packet capture, RTT or direct server queue
measurements. The helper retains timing in the optional context dictionary shared
with the batch controller; it does not add fields to either public helper tuple.

| Context field | Observation |
| --- | --- |
| `attempt_started_at` | Entry to the HTTP helper, before shared admission. Initial adaptive permit acquisition and smoothing precede this point; redirected hops acquire their permits within the attempt. |
| `hops[].dispatch_at` | aiohttp's request-headers-send callback, after connector/DNS and admission waits, immediately before handing headers to its writer. Socket buffering and scheduling can still intervene. |
| `hops[].headers_at` | Parsed response headers observed by the redirect or request-end callback. Each hop includes URL, status and parsed Retry-After. |
| `t0` | Dispatch timestamp of the last dispatched hop; retained legacy field name. |
| `final_headers_at` | Headers of the final response, after aiohttp's automatic redirects. |
| `first_body_byte_at` | Completion of the first nonempty `response.content.read(1)` on HTTP 200. This observes decoded application-body availability. |
| `body_completed_at` | Completion of the remaining successful body read. |
| `ttfb` | `first_body_byte_at - t0`, eligible only after a complete HTTP-200 body. A local save failure clears eligibility too. |

Headers and first-body-byte delays remain visible separately; delaying only the
tail changes completion time without moving the first-byte observation. Empty bodies
have no first-byte event or sample. Partial bodies may retain an observed first-byte
timestamp, but failed/timed-out/cancelled acquisitions provide no successful sample.
Error response bodies are not read for latency samples. A zero-byte HTTP-200 output
that saves successfully counts as an acquisition, with zero useful bytes and no
latency sample.

Redirects retain aiohttp's existing limits, URL resolution and credential/cookie
behavior. The latency signal is for the **final hop**, not elapsed time across the
chain. Both controllers receive final-response success/overload/latency feedback
under that authority. On a cross-authority redirect the batch path releases the
original adaptive permit before acquiring the destination's permit and applying
its smoother. Same-controller redirects retain the permit and apply smoothing
again. This happens before the next connector acquisition, so the destination's
existing adaptive limit governs the request that supplies its feedback. Reciprocal
redirects never hold two authorities' permits at once. Redirect hops themselves
are not saved acquisitions and do not add useful-success counts or latency samples.
No controller equation or tuning parameter changes.

Feedback credits the controller owning the current permit, not a fresh lookup from
the trace URL: aiohttp normalization can remove default ports or encode IDNA names.
If redirect admission fails between releasing and acquiring permits, the target
controller receives that failed-admission outcome. Existing manager host keys are
preserved; differently spelled input aliases can still select distinct adaptive
controllers. The normalized Retry-After authority gate is independent of those keys.

## Outcomes and denominators

`HostMetrics.record` receives explicit saved-output success from the batch path.
Only completed HTTP-200 acquisitions with saved output increase `n_success` or
useful saved `bytes`. Webdataset bytes are credited after both payload and metadata
are saved. A failed save can leave partial output; collision/transactional output
repair remains a separate gate.

Every completed acquisition attempt belongs to exactly one interval outcome:

- `n_success`: saved HTTP-200 acquisition, including empty output.
- `n_http_failures`: unsuccessful HTTP status, including 404 and overload statuses.
- `n_transport_failures`: transport/read failure or network timeout.
- `n_local_failures`: local output/validation failure or timeout awaiting admission.
- `n_unknown_failures`: unexpected acquisition exception with unclassified cause.

`n_failed` is the sum of the four failure categories; `total = n_success + n_failed`.
The legacy `n_errors` field is the **overload subset** (429, 408, 5xx and transport
failure), not another disjoint outcome. Local errors and admission timeouts do not
signal server overload. Unexpected acquisition exceptions do not establish either
local output failure or server congestion, so they occupy the additive unknown
category and produce no overload or latency sample. Known aiohttp `ClientError`
transport failures retain their transport/overload classification. `has_overload`
follows that subset. A latency sample also
requires a positive finite first-byte interval and nonempty saved payload; neither
404 nor other failed statuses nor local failures contribute one. Metrics count
completed attempts, so retried URLs can contribute several outcomes; cancellation
does not create a completed-attempt observation.

Overview `successful_downloads`/`failed_downloads` retain final-per-URL semantics.
The additive `unattempted_or_cancelled_urls` counts input URLs without a final outcome,
making the input denominator interpretable on shutdown. If a prior failed attempt
exists when its retry is cancelled, that URL retains its last failed outcome.

## Shared Retry-After policy

One `ClientSession` owns one gate; the maintained asynchronous run keeps that session
through all retry rounds. The key is `(normalized hostname, effective port)`, using
aiohttp's YARL IDNA/IP representation. Paths and userinfo do not affect the key.
HTTP defaults to 80 and HTTPS to 443; schemes using the same explicit port share
the authority deadline. This is deliberately authority-based rather than RFC-origin
isolation (an origin also includes scheme): traffic on one scheme can therefore
delay the other on that same explicit port. This scope applies with PAARC on or off.
It is not shared across processes, sessions, workers or VMs.

The response's actual authority owns its embargo, including redirect responses.
Headers are observed before body reading. Nonnegative finite numeric values and
HTTP dates establish deadlines. Finite fractional/exponent/leading-plus numeric
forms retain the previous float parser's compatibility extension beyond RFC integer
delay-seconds, including Python float's underscore separators and surrounding
whitespace. Past dates mean zero additional delay; malformed, negative and
nonfinite values are ignored. A date is converted once from wall-clock time into a
monotonic deadline; subsequent wall-clock changes cannot move it. Unrepresentable
deadline sums are rejected.

A valid Retry-After on a followed 3xx also delays that chain before issuing the
redirected request, even across authorities, as specified by
[RFC 9110 §10.2.3](https://www.rfc-editor.org/rfc/rfc9110.html#name-retry-after).
The chain waits for the responding authority's outstanding deadline; concurrent
extensions therefore apply. This does not install an embargo on the destination,
so unrelated direct requests there can proceed. The redirect response is released
before waiting, including an unfinished body, so timeout/cancellation cannot strand
its connection. A 3xx without a redirect target is a terminal failed acquisition;
its header is observed once even though aiohttp calls both redirect and end hooks.

Updates take the maximum outstanding deadline without awaiting; one event loop
cannot lose a concurrent extension. Waiters sleep without a global lock and recheck
after waking. Admission is checked before the helper starts the request and again
at the headers-send callback after connector waits, including every redirect hop.
Requests already handed to the writer are not recalled. Other authorities have
independent deadlines and can progress subject to ordinary worker/connection limits.
A callback recheck can occupy a connector slot while waiting; the positive attempt
timeout and cancellation still bound it.

Expired entries are removed lazily when that authority next waits. An authority
observed only on its last attempt can retain an inactive entry until its session
is collected. Storage is proportional to distinct embargoed authorities in the run;
there is no constant-size cache or long-lived-session eviction guarantee.

`build_trace_config()` installs these hooks in maintained sessions. For a caller
using the standalone helper with a plain session, the helper appends one frozen
configuration to the session's public `trace_configs` list before starting its first
request. Existing trace configurations remain intact. The implementation uses
aiohttp 3.13.3's inspected send/redirect lifecycle; its localhost tests must be rerun
when that library is upgraded.

## Timeout, cancellation and compatibility

A positive configured request timeout bounds admission, connection, all redirects
and body reading as one attempt. Initial adaptive permit/smoothing waits precede
this HTTP timeout, as before; subsequent redirect permit/smoothing waits and 3xx
follow-up delays are inside that same attempt budget. A timeout while waiting for
redirect admission is a local admission failure, not overload feedback. Zero or
negative values retain the previous unbounded timeout setting; cancellation still
works. An embargo longer than a positive timeout can
therefore produce a retryable timeout without dispatching a request. It counts
toward the existing total `max_retry_attempts`; header waits never bypass the bound.
For example, Retry-After 60 with timeout 5 and three total attempts can finish as a
failed URL after the initial 429 and two admission timeouts, with no second network
request. Eventual acquisition after an arbitrarily long embargo is not guaranteed.
The initial 429/503 remains overload feedback and its Retry-After participates in
the controller's cooldown. A later local wait is not a fresh server observation and
does not synthesize another overload event or erase that cooldown. Admission remains
closed until the gate deadline regardless of the controller's current concurrency.

Cancellation propagates through gate waits, connector waits and reads. The batch
scheduler cancels and awaits all its workers on cancellation or shutdown, closing
its progress bar and releasing adaptive permits. A 100-ms shutdown monitor also
cancels workers waiting on a very long embargo. This does not make synchronous
filesystem writes interruptible or redesign global retry-backoff sleep.
For the maintained CLI callers, a set shutdown flag takes precedence and returns
partial outcomes, including when external cancellation races with that flag.
External batch cancellation while the flag is clear propagates `CancelledError`.

The four- and six-element helper tuples, CLI/config keys, retry-attempt counting,
output paths, overwrite consent and archive behavior are preserved. The implementation
stays in the already-staged `single_download.py`/`download_batch.py`; TaskVine and
experiment source packaging need no new file. YARL is already part of aiohttp's
installed dependency set. The multithread comparison implementation, cloud-upload
path and gradient distributed integration are outside this change.

See the [validation record](HTTP-MEASUREMENT-VALIDATION.md) for observed checks and
limitations and the [research contract](MANUSCRIPT-READINESS.md) for still-open
scientific evidence gates. These semantics establish no throughput or speedup claim.
