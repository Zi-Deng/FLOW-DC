# Versioned engineering methods

These are executable engineering candidates for issue #26, not tuned scientific
results. The maintainer reports advisor approval but has not supplied numerical
decisions. Defaults below are provisional and versioned. No efficacy or novelty
claim follows from the deterministic traces.

## Selection and compatibility

Use `bin/download_batch.py --control_method METHOD`, or the JSON `control_method`
key, with `enable_paarc=true`. All methods use the same acquisition, redirect,
Retry-After, row journal and archive publication code. Concurrency always obeys
`C_min <= C_init <= C_max <= 10000`; the downloader's worker count is a further
bound. The example `files/config/gradient-candidate-v1.json` uses a maximum of 16.
`--method_options` accepts a JSON object. Unknown/conflicting values fail before
output handling or HTTP.

New explicit-method inputs also enforce the research metadata precondition before
overwrite consent: nullable String/Boolean/Int64/finite Float64/Null; Int64 within
±(2^53−1); rows at most 64 KiB when encoded; ordinary column names use letters,
digits and underscores. Product provenance columns are validated separately.
Narrow/nested/temporal/decimal data are rejected, not silently converted. This
does not repair the legacy arbitrary-metadata path in issue #25.

| ID | Behavior |
| --- | --- |
| `paarc-base-v2` | Existing corrected PAARC state machine, scheduler and named PAARC parameters |
| `gradient-candidate-v1` | Elapsed-time queue-gradient candidate specified below |
| `fixed-v1` | Exactly `C_init`; feedback never changes the limit. Mandatory embargo/admission still applies |
| `ratio-v1` | FLOW-DC buffered delay-ratio comparator specified below |

Omitting the ID preserves the existing downloader default, including its historical
host key. `download_batch_gradient.py` retains the old implementation, now labelled
`gradient-legacy-v0`, and refuses explicit new-method configuration. It is not an
alias for the candidate. `enable_paarc=false` remains `legacy-uncontrolled-v0`, with
mandatory Retry-After handling; it does not represent img2dataset.

Explicit methods use `(scheme, lowercase IDNA host, effective port)` keys. Thus
`http://example:80` and `http://EXAMPLE` share a controller; different schemes or
effective ports do not. The existing session Retry-After gate remains shared by
hostname/effective port and can be more conservative across schemes. Aggregate
cross-worker authority is milestone D; these local methods alone do not provide it.

The maintained TaskVine route forwards both keys and stages `flowdc_methods.py`.
Committed-source bundles include it when required and retain old source snapshot
interpretation. The UI prototype preserves these keys when importing/exporting;
its simulated job execution is unchanged. The legacy cloud submitter remains
unsupported as recorded by issue #25.

## Measurements, equations and units

Only complete, nonempty HTTP 200 bodies with finite positive final-hop application
first-byte delay supply samples. Local output failure does not erase a completed
HTTP observation. Empty/truncated/failed bodies do not supply latency confidence.
Overload includes transport failures, 408, 429 and 5xx, excluding known local,
admission and unclassified errors. Eligible observations are consumed once.

By default `sample_window_s=null` keeps the original candidate's per-tick batches.
At the defaults, a service producing only four completions per 0.2-second tick
stays in sparse hold indefinitely; a slow baseline probe can do the same. That
regime must be reported as effectively fixed concurrency, not exercised adaptation.
The study configurations and example explicitly select `sample_window_s=1.5`.
This opt-in retains a subminimum batch across ticks until enough samples arrive;
both receipt and final-dispatch ages must remain strictly less than that window.
Expired or timestamp-less samples are discarded, never reused. The window must
be at least one interval and shorter than the stale-sample gap. Overload clears
pending samples immediately and a probe clears all pre-probe observations.
Trace fields distinguish new observations from pending samples. Cold start and
probe refresh at four completions/second are deterministic fixtures; still lower
rates or long requests can remain in sparse hold. No scientific parameter choice
is inferred from this engineering calibration.

The delay is measured from final-hop dispatch after connector/admission waits to
the first nonempty application body read. It includes application/network effects;
it is neither packet RTT nor measured server queueing. Clocks are local monotonic
seconds; no remote epochs are subtracted.

Let `p10`, `p50` be linearly interpolated percentiles of the current eligible
samples, and `b` the positive baseline. Initialize `b=max(p10, epsilon)`, and reset
the derivative when a lower floored `p10` replaces it. The candidate queue proxy is
`q_raw=max(0,p50-b)` in seconds. With elapsed time `dt` since the last *eligible*
sample (including intervening sparse/empty intervals):

```text
a_q(dt) = 1 - exp(-dt / queue_tau_s)
q       = q_previous + a_q(dt) * (q_raw - q_previous)
d       = d_previous + a_q(dt) * (p50 - d_previous)
g_raw   = (q - q_previous) / (dt * max(b, epsilon))     [s^-1]
a_g(dt) = 1 - exp(-dt / gradient_tau_s)
g       = g_raw, on the first derivative; otherwise g_previous + a_g(dt)*(g_raw-g_previous)
```

The first eligible sample after initialization/reset sets `q=q_raw`, `d=p50` and
the sample time, with no derivative claim and no increase. Baseline changes,
probe transitions, stale gaps and decreases reset derivative/persistence state.
Both time constants must be positive finite seconds. An optional `legacy_alpha`
in `(0,1)` requires `alpha_reference_s>0` and maps both taus to
`-alpha_reference_s/log(1-legacy_alpha)`. Mixing alpha and either tau is rejected.

## Parameters and priority

The executable defaults are in `bin/flowdc_methods.py:MethodConfig`. In addition
to the concurrency bounds (2/4/10000), defaults are:

| Parameter | Default / units |
| --- | --- |
| interval, sample minimum | 0.2 seconds, 5 observations |
| optional fresh sample window | null by default; study/example explicitly select 1.5 seconds |
| queue/gradient time constants | 1 second each |
| baseline maximum age, stale sample gap | 10 seconds, 2 seconds |
| probe wait | 0.4 seconds |
| queue floor, numerical floor | 0.001 seconds, 0.000001 seconds |
| gradient hold/decrease thresholds | 0.1 / 0.25 s^-1 |
| hard queue ratio | 2 times baseline |
| soft/hard decrease factors | 0.8 / 0.5 |
| persistence, increase step | 2 consecutive eligible overuse intervals, 1 permit |
| recovery grace | 1 second |
| ratio buffer fraction, headroom | 0.1, 1 permit |

Every result clamps its integer limit to `[C_min,C_max]`. Decreases use floor;
additive increases use an integer step. The following priority applies to gradient
and ratio candidates; fixed uses its constant limit after validating observations.

| Priority | Condition and transition |
| --- | --- |
| 1 | Any overload: `floor(C*hard_factor)`, reset, begin grace, even with no samples or an old baseline |
| 2 | Baseline age `>= maximum`: enter probe at `C_min`, reset; no increase |
| 3 | Fewer than minimum samples: hold, reset persistence; sample gap `>= stale_after_s` also resets derivative |
| 4 | Probe: hold until elapsed probe time `>= probe_wait_s` and outstanding work `<= C_min`; only samples dispatched since probe start count; then replace baseline and reset |
| 5 | Initialize/lower baseline, or eligible sample gap `>= stale_after_s`: initialize state and hold |
| 6 | Updated `q >= max(queue_floor_s, hard_queue_ratio*b)`: hard decrease/reset/grace |
| 7 | `now < recovery_until`: hold and clear persistence (equality ends grace) |
| 8 | Ratio: apply the comparator equation below |
| 9 | Gradient with `q >= queue_floor_s` and `g >= decrease threshold`: increment persistence; decrease/reset/grace when count reaches threshold; otherwise hold |
| 10 | Gradient with `q >= queue_floor_s` and `g >= hold threshold`: hold, clear persistence |
| 11 | Eligible gradient evidence outside those branches: bounded additive increase, clear persistence |

Probe samples are selected by dispatch time, not completion time, so pre-probe
requests cannot refresh the baseline. Sparse evidence during a probe keeps the
probe open. A decrease prevents new admission until outstanding work drains below
the new target; it does not claim already-issued work vanished. Trace fields include
both decision values and post-transition state. A failed controller/trajectory
operation stops subsequent dispatch and prevents a successful final boundary.

The ratio comparator uses the same eligible samples, freshness, probes, overload
and grace rules. Its update is
`C_next=clamp(floor(C * min(1, b*(1+buffer_fraction)/max(d,epsilon)) + headroom))`.
Headroom is a positive integer, bounded by the final concurrency clamp. This is
FLOW-DC's specified comparator, not an exact implementation of another project.
Because the ratio is capped at one, growth is at most `headroom` permits per
eligible interval; the gradient-only `eligible_increase` branch is not used by
`ratio-v1`. The engineering defaults give both methods one permit of additive
growth, while their delay-dependent decrease rules differ. This specified comparison
does not establish scientific fairness or authorize parameter tuning on evaluation
results; the advisor protocol remains provisional.

## Single-mechanism ablations

Set `method_options.ablation` to one of the following for the gradient candidate.
The name and all parameters appear in output provenance and trace descriptors.

| Name | Difference |
| --- | --- |
| `no-gradient-term` | Bypass gradient hold/soft-decrease branches; hard queue/overload branches remain |
| `no-elapsed-smoothing` | Both alphas are 1; derivative still divides by actual elapsed time and baseline |
| `no-sample-gate` | Minimum becomes one real finite positive observation; empty intervals still hold |
| `no-baseline-refresh` | Disable active probes; expired baselines hold until an independently lower baseline resets freshness |
| `no-recovery-grace` | Set decrease grace duration to zero; reset/initialization still holds |

No ablation removes ownership, Retry-After, admission, finite-data validation or
absolute concurrency bounds. Overload stays ahead of sample gating and probes.

## Retained evidence and advisor packet

The overview reports the selected ID/version and effective parameters. Explicit
methods write per-invocation `.flowdc/control/*.jsonl` records. Closed descriptors
bind byte counts and SHA-256, and the final completion record binds the descriptor
list. Interrupted invocations remain distinct with `complete=false`; reconciliation
detects modified closed trajectories. Recovery never reconstructs a missing whole-run
timer. Required distributed return of these files is tracked in milestone D.

| Claim / acceptance | Evidence | Limit |
| --- | --- | --- |
| Formula, elapsed interval, baseline/reset rules agree with code (3) | `tests/test_flowdc_methods.py` deterministic transitions and equality tests | V1, not efficacy |
| Selection affects actual admission (3) | Real `AdaptiveSemaphore` decrease/drain test and selected native manager classes | In-process gate; retained localhost trajectory run still required |
| All five ablations differ as specified (3) | Paired deterministic trajectories, safety bounds/overload assertions | Mechanism only |
| Complete-body feedback survives local save failures (3) | Observation buffer fixture plus existing HTTP measurement suite | Native HTTP suite remains required |
| Legacy defaults/staging preserved (3,7) | Config/legacy rejection, isolated worker CLI import, source snapshot and TaskVine declaration tests | Stub TaskVine declarations do not establish distributed V2 |
| Reports retain traces and reject tampering (2,3) | Closed/interrupted invocation and digest mismatch fixture | No cross-worker authority yet |

Advisor decisions remain pending for allowable latency/coverage tradeoffs, tuning
constraints, scenario weights, estimands and the frozen protocol. Eight predetermined
tuning candidates per method, six paired blocks in three scenario families and a
95% CI half-width target of 5% of the reference mean are **proposals**. Evaluation
results must not tune these defaults. Milestone C supplies separate plan namespaces,
run-level statistics and a freeze gate; request observations are not replicates.

Related-work scope differs: TIMELY adjusts transport transmission rates using
packet RTT gradients measured at hosts ([paper abstract](https://research.google/pubs/timely-rtt-based-congestion-control-for-the-datacenter/)).
Netflix Gradient2 compares exponential averages on different time scales to detect
latency divergence ([project description](https://github.com/Netflix/concurrency-limits#gradient2)).
Envoy's documented controller uses a buffered baseline/sample-delay ratio and
square-root headroom ([design](https://www.envoyproxy.io/docs/envoy/latest/configuration/http/http_filters/adaptive_concurrency_filter.html)).
FLOW-DC's candidate controls application acquisition concurrency with retained row
artifacts. These distinctions do not establish novelty or superiority.
