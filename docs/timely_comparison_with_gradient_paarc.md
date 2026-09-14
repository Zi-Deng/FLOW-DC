# TIMELY vs. FLOW-DC Gradient PAARC

This document compares the current FLOW-DC gradient controller in [bin/download_batch_gradient.py](/Users/zideng/Work/github/FLOW-DC/bin/download_batch_gradient.py:1) against the original **TIMELY** congestion-control design from:

- Radhika Mittal et al., **TIMELY: RTT-based Congestion Control for the Datacenter**, SIGCOMM 2015
- Paper PDF: [TIMELY paper](https://conferences.sigcomm.org/sigcomm/2015/pdf/papers/p537.pdf)

This comparison is intentionally **code-aware and unbiased**:

- it describes what TIMELY actually proposes
- it describes what FLOW-DC actually implements today
- it distinguishes between:
  - ideas that are truly shared
  - ideas that are only loosely analogous
  - ideas that are completely different

## Executive Summary

The current FLOW-DC gradient controller is:

- **clearly inspired by TIMELY**
- **not a reimplementation of TIMELY**
- **best described as PAARC with a TIMELY-like early-warning signal**

That means two things can be true at once:

- the design is legitimately TIMELY-inspired
- the resulting controller is still fundamentally a different kind of control system

The biggest reason is simple:

- **TIMELY is a transport-layer rate controller over RTT**
- **FLOW-DC gradient PAARC is an application-layer per-host concurrency controller over interval TTFB summaries**

That is a major architectural difference, not a small detail.

---

## Comparison Scope

This document compares:

- the **TIMELY control law and its parameters**
- the current FLOW-DC implementation in:
  - [bin/download_batch_gradient.py](/Users/zideng/Work/github/FLOW-DC/bin/download_batch_gradient.py:1)
  - the inherited base controller in [bin/download_batch.py](/Users/zideng/Work/github/FLOW-DC/bin/download_batch.py:1)

It does **not** claim that FLOW-DC is “TIMELY for HTTP.” That would be too strong.

---

## What TIMELY Is Trying To Do

TIMELY is a datacenter congestion controller designed around **RTT measurements**. Its key claim is that RTT can serve as an early congestion signal before significant packet loss occurs.

At a high level, TIMELY:

1. measures RTT on every completion event
2. estimates a smoothed RTT gradient
3. uses that gradient and RTT thresholds to decide whether to:
   - increase rate
   - hold
   - decrease rate

Important design assumptions in TIMELY:

- very accurate RTT measurement
- low-noise datacenter environment
- transport-layer sender-side rate control
- per-flow control loop

Those assumptions matter because FLOW-DC does not share all of them.

---

## What FLOW-DC Gradient PAARC Is Trying To Do

The gradient controller in [bin/download_batch_gradient.py](/Users/zideng/Work/github/FLOW-DC/bin/download_batch_gradient.py:1) is trying to solve a different practical problem:

- large-scale HTTP downloading across internet hosts
- application-layer observability only
- per-host concurrency control rather than packet pacing

Its specific goal is:

- detect when a host looks increasingly stressed
- ease off **before** explicit rate limiting or hard overload if possible

The current implementation does this by building a queue-like signal from:

```text
queue_delay_raw = max(0, p50_raw - RTprop)
```

and then computing a **time-normalized**, `RTprop`-normalized smoothed trend from that value.

---

## What Is Actually The Same

These are the parts that are genuinely aligned with TIMELY.

## 1. Delay Gradient Is The Central Early-Warning Signal

TIMELY’s headline idea is:

- react to the **trend** of delay, not only to large absolute delay

FLOW-DC gradient PAARC does the same at a high level:

- it reacts to the trend of a delay-derived quantity
- not just a hard threshold on `p50` or `p95`

In your code, the trend logic begins in [bin/download_batch_gradient.py](/Users/zideng/Work/github/FLOW-DC/bin/download_batch_gradient.py:296).

So this part is a real similarity, not a metaphor.

## 2. Exponential Smoothing Is Used

TIMELY smooths the RTT gradient with EWMA-like logic.

FLOW-DC gradient PAARC smooths:

- `queue_delay_raw` into `queue_delay_ema`
- `gradient_raw` into `gradient_ema`

using `gradient_alpha` in [bin/download_batch_gradient.py](/Users/zideng/Work/github/FLOW-DC/bin/download_batch_gradient.py:284).

This is the same general design instinct:

- do not react directly to raw jitter
- react to a smoothed trend signal

After the hardening pass, FLOW-DC now also divides the change in queue-delay EMA by interval duration before smoothing the final gradient. That makes the controller more faithful to the idea of a slope rather than a simple per-interval delta.

## 3. A Baseline Delay Reference Exists

TIMELY uses `minRTT` as a baseline reference.

FLOW-DC uses `RTprop` inherited from the base PAARC metrics.

Both serve the same broad purpose:

- define a low-latency reference point
- measure current stress relative to that reference

This is a meaningful conceptual similarity, even though the estimators are not identical.

## 4. The Goal Is Pre-Loss / Pre-Hard-Overload Reaction

TIMELY’s philosophy is to respond before packet loss becomes the dominant signal.

FLOW-DC gradient PAARC is trying to respond before:

- `429`
- connection resets
- timeouts
- harder overload/backoff states

That is a strong goal-level similarity.

---

## What Is Different

These differences are large enough that the two methods should not be described as equivalent controllers.

## 1. RTT vs. TTFB

TIMELY uses:

- transport-level RTT

FLOW-DC gradient PAARC uses:

- application-level interval `p50_raw` TTFB

That changes the nature of the signal substantially.

RTT mainly reflects:

- network path propagation
- queueing delay
- transport timing

TTFB can additionally include:

- server-side processing
- server request scheduling
- HTTP stack effects
- cache behavior
- connection reuse effects

So even if the math shape looks similar, the underlying observable is not the same.

This is one of the biggest differences in the whole comparison.

## 2. Per-Event Control vs. Interval Control

TIMELY updates on **every completion event**.

FLOW-DC gradient PAARC updates once per control interval after summarizing many requests into interval statistics.

In your implementation:

- the inherited base metrics gather samples over an interval
- then [bin/download_batch_gradient.py](/Users/zideng/Work/github/FLOW-DC/bin/download_batch_gradient.py:289) adds gradient-specific calculations on top of that interval snapshot

So the timing granularity is very different:

- TIMELY: event-level controller
- FLOW-DC gradient PAARC: interval-level controller

## 3. Rate Control vs. Concurrency Control

TIMELY directly controls **sending rate**.

FLOW-DC gradient PAARC controls **per-host in-flight request concurrency**, with inherited semaphore/smoother mechanics from PAARC.

That means the controlled variable is different:

- TIMELY: packets or bytes per time
- FLOW-DC: concurrent HTTP requests per host

These are related, but not equivalent.

## 4. TIMELY Uses RTT Thresholds `Tlow` and `Thigh`

TIMELY’s control law explicitly reasons about:

- `Tlow`
- `Thigh`

The current FLOW-DC gradient controller does **not** implement those exact concepts.

The closest analogue is:

- `gradient_queue_floor_mult`

but that is not the same as TIMELY’s low/high RTT threshold scheme.

This is an important structural difference.

## 5. TIMELY Uses A More Explicit Additive/Multiplicative Rate Law

TIMELY’s controller is an explicit rate law:

- if conditions are favorable, increase
- if gradient is bad, reduce rate
- the amount of reduction depends on the signal

FLOW-DC gradient PAARC instead plugs the signal into an existing **PAARC state machine**:

- `STARTUP`
- `PROBE_BW`
- `PROBE_RTT`
- `BACKOFF`

The gradient signal decides whether to:

- exit startup
- hold probing
- soft-backoff

After the hardening patch, the `PROBE_BW` reaction is more structured than before. It now has four steady-state regions:

- below queue floor: normal PAARC probe behavior
- above queue floor with `gradient_ema <= 0`: cautious probe-up
- above queue floor with small positive gradient: hold
- above queue floor with larger positive gradient: proportional or maximum soft backoff

That means the gradient signal is not the whole controller. It is one decision layer inside a larger controller framework.

## 6. FLOW-DC Adds A Persistence Requirement

Your controller requires:

- `gradient_required_intervals = 2`
- `startup_gradient_required_intervals = 2`

before acting.

This is not a faithful TIMELY feature. It is an adaptation for a noisier, interval-based environment.

It makes sense in your system, but it is still a real difference.

## 7. FLOW-DC Uses A Double-Smoothing-Like Structure

In practical terms, your controller does:

1. smooth queue-like delay
2. derive a time-normalized gradient from the smoothed delay
3. smooth the gradient again

TIMELY is conceptually much closer to:

1. observe RTT change
2. smooth the RTT-difference signal
3. feed it into the rate update law

So FLOW-DC is more layered and more defensive against noisy application-layer measurements.

---

## Hyperparameter Comparison

## Side-By-Side Table

| Topic | TIMELY | FLOW-DC Gradient PAARC | Same / Different |
|---|---|---|---|
| Base delay reference | `minRTT` | `RTprop` | Similar purpose, different estimator |
| Main raw signal | RTT | interval `p50_raw` TTFB | Different |
| Gradient signal | RTT gradient | time-normalized queue-delay gradient from `p50_raw - RTprop` | Similar idea, different construction |
| Smoothing | EWMA on delay-diff signal | EMA on queue delay and EMA on gradient | Same family, different pipeline |
| Controller cadence | per completion event | per interval | Different |
| Controlled variable | rate | per-host concurrency | Different |
| Low/high absolute delay thresholds | `Tlow`, `Thigh` | no direct equivalent | Different |
| Additive increase | direct rate increase | inherited PAARC concurrency increase | Similar purpose, different quantity |
| Multiplicative decrease | direct rate decrease | proportional-to-maximum soft backoff `gradient_backoff_beta`, hard overload `beta` | Similar purpose, different split |
| Persistence rule | not in the same form | consecutive-interval checks | Different |

## TIMELY Parameters Versus Current FLOW-DC Parameters

The TIMELY paper discusses parameters such as:

- `Tlow`
- `Thigh`
- additive increase amount
- multiplicative decrease factor
- EWMA weight

The current FLOW-DC gradient controller uses:

- `gradient_alpha`
- `gradient_threshold`
- `gradient_severe_threshold`
- `startup_gradient_threshold`
- `gradient_required_intervals`
- `startup_gradient_required_intervals`
- `gradient_queue_floor_mult`
- `gradient_backoff_beta`
- `post_backoff_grace_intervals`

These are **not** drop-in TIMELY parameters. They are best understood as a new parameterization for a new environment.

---

## Why The FLOW-DC Changes Make Sense

These changes are not arbitrary. They exist because the environment is different.

## 1. `p50_raw` Instead Of RTT

Reason:

- FLOW-DC is an application-layer downloader
- it does not have transport-layer RTT as its primary controller signal
- TTFB is what is consistently observable

Why `p50_raw` specifically:

- it captures typical interval behavior
- it is less tail-sensitive than `p95`
- it is less fragile than a minimum-like signal

This is a practical adaptation, not a theoretical replication of TIMELY.

## 2. Interval Summaries Instead Of Per-Completion Updates

Reason:

- downloader traffic is async and bursty
- application-level timing is noisy
- one-interval summaries are more stable than reacting to every request completion

This change trades responsiveness for stability.

That is often the right trade in an internet-scale downloader, but it makes the controller less TIMELY-like.

The hardened FLOW-DC version compensates a bit by explicitly dividing its queue-delay change by interval duration, so interval variability no longer changes the meaning of the slope as badly as it did before.

## 3. Persistence Checks

Reason:

- a single interval can be noisy
- acting on one positive gradient would likely overreact

So:

- `gradient_required_intervals = 2`
- `startup_gradient_required_intervals = 2`

serve as anti-flap guards.

This is a sensible adaptation for application-layer control.

## 4. Queue Floor Guard

Reason:

- a positive trend on a tiny queue-like delay should not trigger control action

So your controller requires:

```text
queue_delay_ema >= gradient_queue_floor_mult * rtprop
```

This is a good “do not overreact to tiny drift” mechanism.

It plays a role somewhat similar to TIMELY’s attempt to avoid noise sensitivity, but it is not the same mechanism as `Tlow`/`Thigh`.

## 5. Soft Backoff Separate From Hard Overload Backoff

Reason:

- a gradient signal is only early warning
- it should not be treated as equivalent to confirmed overload

So your controller uses:

- `gradient_backoff_beta = 0.85` for proactive soft trims
- inherited hard `beta = 0.5` after explicit overload

This is one of the strongest signs that your design is deliberately more conservative than a direct TIMELY-style multiplicative response.

## 6. Post-Backoff Grace Window

Reason:

- once explicit overload has already occurred, stale pre-overload gradient memory should not immediately resume shaping the controller

So the hardened FLOW-DC controller adds:

- `post_backoff_grace_intervals = 2`

During this grace period after `BACKOFF -> PROBE_BW`:

- hard overload still dominates immediately
- gradient shaping is suppressed
- upward probe steps are suppressed

This is not a TIMELY concept. It is a FLOW-DC state-machine hygiene addition that makes sense in a noisier application-layer environment.

---

## Exact Same vs. Exact Different

## Exact Same In Spirit

- use a delay-gradient-like signal as early warning
- normalize that signal using a baseline delay reference
- smooth the signal with EWMA/EMA
- try to react before harder congestion signals

## Exact Same In Implementation

Very little.

There is no part of [bin/download_batch_gradient.py](/Users/zideng/Work/github/FLOW-DC/bin/download_batch_gradient.py:1) that should be described as a line-by-line or parameter-by-parameter implementation of TIMELY.

## Exact Different

- measurement layer
- control cadence
- controlled variable
- state-machine structure
- threshold structure
- multiplicative decrease structure
- persistence logic
- post-overload recovery hygiene

That is enough difference that “TIMELY-inspired” is accurate, while “TIMELY-implemented” is not.

---

## Are They Comparable As Rate Controllers?

## Short Answer

**Only loosely.**

## Precise Answer

They are comparable at the level of:

- “controllers that use a delay-trend signal to regulate offered load”

They are **not** strongly comparable as the same class of controller in an engineering sense, because:

- TIMELY is a transport-layer rate controller
- FLOW-DC gradient PAARC is an application-layer concurrency controller

That means they are comparable in **control philosophy**, but not in **mechanism** or **measurement semantics**.

## Unbiased Verdict

If someone said:

- “FLOW-DC gradient PAARC borrows TIMELY’s delay-gradient idea”

that would be fair.

If someone said:

- “FLOW-DC gradient PAARC is basically TIMELY”

that would be too strong.

If someone said:

- “FLOW-DC gradient PAARC and TIMELY are directly comparable as rate controllers”

that would only be partly true, and only at a high abstraction level.

The more accurate statement is:

> FLOW-DC gradient PAARC is a PAARC-based per-host concurrency controller that uses a TIMELY-like, time-normalized delay-gradient signal as an early-warning heuristic.

That is the most honest description.

---

## Practical Takeaway For This Repo

The current gradient method should be evaluated as:

- a pragmatic application-layer adaptation
- not a transport-paper reproduction

That is a good thing from an engineering standpoint, because FLOW-DC’s environment is genuinely different from TIMELY’s datacenter assumptions.

But it also means:

- success or failure should be judged on downloader behavior
- not on how faithfully it mirrors the paper

So the right questions for future evaluation are:

- does it reduce explicit overload events?
- does it preserve throughput?
- does it improve stability across hosts?
- does it avoid overreacting to TTFB noise?

Those are better evaluation criteria for FLOW-DC than “does it exactly match TIMELY?”

---

## References

- TIMELY paper: [RTT-based Congestion Control for the Datacenter](https://conferences.sigcomm.org/sigcomm/2015/pdf/papers/p537.pdf)
- Gradient controller implementation: [bin/download_batch_gradient.py](/Users/zideng/Work/github/FLOW-DC/bin/download_batch_gradient.py:1)
- Base PAARC implementation: [bin/download_batch.py](/Users/zideng/Work/github/FLOW-DC/bin/download_batch.py:1)
