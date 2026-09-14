# `download_batch_gradient.py` Guide

This document explains the current implementation of [bin/download_batch_gradient.py](../bin/download_batch_gradient.py) with pipeline integration updated on September 14, 2026. The controller design originates from the April 2026 experiments.

It is intentionally code-first. Where this guide describes behavior, it is based on the current implementation in:

- [bin/download_batch_gradient.py](../bin/download_batch_gradient.py)
- [bin/download_batch.py](../bin/download_batch.py)
- [files/config/spider_test_gradient.json](../files/config/spider_test_gradient.json)

## Consolidated Pipeline Behavior

The script registers signal handlers from `main()`, shares worker sizing with the main downloader, passes the required `effective_workers` argument, and shares the overview schema and archive reporting. Existing output folders require confirmation unless JSON `force_overwrite` or CLI `--force` is enabled. `--force` also works with `--config`.

The historical gradient defaults remain 256 workers, `mu=0.75`, and a 15-second RTprop window. Explicit `concurrent_downloads=0` or JSON null selects automatic worker sizing. `rtprop_window` can be supplied through JSON or the CLI. Original April source and documentation are preserved under `archives/2026-09-14/pre-consolidation/local-tree/`.

## What This Script Is

`download_batch_gradient.py` is a standalone variant of the main FLOW-DC downloader.

It keeps the normal FLOW-DC pipeline:

- config parsing
- manifest loading and cleanup
- async `aiohttp` downloads
- per-host PAARC controllers
- retries
- optional tar creation
- overview JSON generation

The main change is the controller signal:

- [bin/download_batch.py](../bin/download_batch.py) uses threshold-based latency degradation logic built around `p50` and `p95`
- [bin/download_batch_gradient.py](../bin/download_batch_gradient.py) replaces that with a TIMELY-style queue-delay gradient detector built from `p50_raw - RTprop`

In plain English:

- the base downloader asks: "Is latency already inflated enough to call this degraded?"
- the gradient downloader asks: "Is queue-like delay rising in a sustained way, even before hard overload happens?"

That makes the gradient variant an early-warning controller rather than a pure threshold-triggered controller.

## High-Level Architecture

The gradient script is not a full rewrite. It imports the main downloader as `base`:

```python
import download_batch as base
```

That means most of the heavy lifting is still done by the original script. The gradient script overrides only the parts that need different controller behavior.

The main reused pieces are:

- `base.validate_and_load(...)`
- `base.build_trace_config()`
- `base.download_batch_bounded(...)`
- `base.controller_loop(...)`
- `base.create_tar(...)`
- `base.DownloadOutcome`
- `base.HostMetrics` and `base.PAARCController` as the inheritance base

So the gradient script should be read as:

- "same downloader pipeline"
- "same host-controller framework"
- "different control signal and different controller decisions"

## Step-By-Step Walkthrough

## 1. File Header And Imports

The file starts with a short docstring explaining the design intent:

- standalone variant
- same pipeline
- gradient detector instead of `p50/p95` degradation

Then it imports:

- standard library modules for config parsing, timing, counting, and paths
- `aiohttp` and `polars`
- the base downloader module as `base`

This import is the most important design choice in the file. It keeps the variant small enough to maintain and reduces the risk of the core downloader paths drifting apart.

## 2. `PAARCConfig`: Gradient-Specific Controller Parameters

Defined at [bin/download_batch_gradient.py](../bin/download_batch_gradient.py).

This dataclass extends `base.PAARCConfig` and adds only the new hyperparameters needed for the gradient logic:

- `gradient_alpha`
- `gradient_threshold`
- `gradient_severe_threshold`
- `startup_gradient_threshold`
- `gradient_required_intervals`
- `startup_gradient_required_intervals`
- `gradient_queue_floor_mult`
- `gradient_backoff_beta`
- `post_backoff_grace_intervals`

These fields live at the controller layer, not the general app layer. They are used once a host controller is already running.

## 3. `Config`: Main App Config

Defined at [bin/download_batch_gradient.py](../bin/download_batch_gradient.py).

This dataclass extends `base.Config`, so it still includes:

- input/output fields
- downloader concurrency
- timeouts
- retry settings
- tar and overview settings
- ordinary PAARC controls

Then it adds the same gradient-specific fields as `PAARCConfig`.

The key method is `to_paarc_config()` at [bin/download_batch_gradient.py](../bin/download_batch_gradient.py). It converts the app-level config into the controller config that each per-host PAARC controller receives.

Important detail:

- legacy threshold knobs like `theta_50`, `theta_95`, `startup_theta_50`, and `startup_theta_95` are still passed through for compatibility
- but this gradient variant does not use them for its degradation or startup plateau decisions

They are preserved so the script stays config-compatible with existing JSON files and downstream expectations.

## 4. `parse_args()`: CLI And JSON Config Loading

Defined at [bin/download_batch_gradient.py](../bin/download_batch_gradient.py).

This function supports two modes:

- load everything from `--config`
- build the config directly from CLI flags

### What It Parses

It parses four broad categories:

- input/output options
- download behavior
- inherited PAARC options
- gradient-specific options

### Why The Compatibility Flags Still Exist

These are still accepted:

- `startup_theta_50`
- `startup_theta_95`
- `theta_50`
- `theta_95`

That is deliberate. Existing config files and tooling in this repo still know about these fields. The gradient script accepts them so users do not need a totally new schema just to try the new controller.

### Defaults Versus Example Config

There are two layers of values to keep distinct:

1. script defaults in `parse_args()` and the dataclasses
2. explicit overrides in [files/config/spider_test_gradient.json](../files/config/spider_test_gradient.json)

This matters because the example config does not simply use every default.

## 5. `HostMetrics`: Turning Interval Statistics Into A Gradient Signal

Defined at [bin/download_batch_gradient.py](../bin/download_batch_gradient.py).

This is the core measurement change in the file.

It subclasses `base.HostMetrics`, which already knows how to:

- store successful-request TTFB samples
- compute `p10_raw`, `p50_raw`, `p95_raw`
- compute EMA-smoothed percentiles
- maintain `RTprop`
- summarize a control interval

The gradient subclass adds:

- `_queue_delay_ema`
- `_gradient_ema`
- bookkeeping counters for reporting
- `reset_gradient_state()` so hard-overload recovery can clear soft-signal memory

### The Measurement Flow

The method `finish_interval()` at [bin/download_batch_gradient.py](../bin/download_batch_gradient.py) works like this:

1. Call `await super().finish_interval(...)`
2. Read:
   - `p50_raw`
   - `rtprop`
3. If both exist and gradient tracking is enabled for the current controller state:
   - compute `queue_delay_raw = max(0, p50_raw - rtprop)`
   - update `queue_delay_ema`
   - compute `gradient_raw`
   - update `gradient_ema`
4. Store those values in the interval snapshot

Gradient tracking is intentionally disabled while the controller is in `PROBE_RTT` or `BACKOFF`.

Why:

- `PROBE_RTT` deliberately disturbs concurrency to refresh the baseline
- `BACKOFF` is a hard-overload recovery phase
- neither phase should teach the proactive early-warning signal

### The Exact Gradient Formulas

If both `p50_raw` and `rtprop` exist, and the current interval is allowed to train the gradient signal:

```text
queue_delay_raw = max(0, p50_raw - rtprop)
queue_delay_ema = EMA(queue_delay_raw, alpha=gradient_alpha)
gradient_raw = ((queue_delay_ema - previous_queue_delay_ema) / max(interval_duration, 1e-6)) / max(rtprop, 1e-6)
gradient_ema = EMA(gradient_raw, alpha=gradient_alpha)
```

Interpretation:

- `queue_delay_raw` is the extra median delay above the low-latency baseline
- `queue_delay_ema` smooths that queue-like delay
- `gradient_raw` asks whether that queue-like delay is rising or falling per unit time
- dividing by interval duration makes the slope comparable across short and long control intervals
- normalizing by `rtprop` makes the slope more scale-aware across fast and slow hosts
- `gradient_ema` smooths the trend so the controller reacts to sustained changes rather than one noisy interval

### Why `p50_raw` And Not `p95`

This implementation uses `p50_raw` as the latency sample for the gradient path.

That choice means:

- the signal reflects typical delay, not tail spikes
- the controller is less sensitive to isolated outliers
- the gradient is more about persistent queue buildup than one bad burst

`p95` is still computed by the inherited base metrics for observability, but it is not used for the gradient decisions.

### What The Snapshot Gains

The gradient `finish_interval()` adds these keys to the normal interval snapshot:

- `queue_delay_raw`
- `queue_delay_ema`
- `gradient_raw`
- `gradient_ema`
- `has_gradient_sample`
- `gradient_interval_sec`
- `gradient_sample_count`
- `gradient_confidence`

`gradient_confidence` is logged for analysis only. It is currently:

```text
min(1.0, successful_ttfb_samples_in_interval / N_min)
```

The hardened controller does not weight decisions by confidence yet. It simply exposes the value so future tuning can tell whether a host is often being judged from weak interval sample sets.

Those fields are the inputs to the gradient controller.

## 6. `PAARCController`: Where The Behavioral Change Happens

Defined at [bin/download_batch_gradient.py](../bin/download_batch_gradient.py).

This class subclasses `base.PAARCController`.

That means it inherits:

- per-host state machine structure
- concurrency semaphore/smoother wiring
- cooldown logic
- `PROBE_RTT` behavior
- ceiling revision logic
- explicit overload handling machinery
- the sample-aware control interval scheduler now present in the base file

It then overrides the parts where the decision signal changes.

### 6.1 Controller State That Is Added

The gradient controller adds:

- `_gradient_overuse_intervals`
- `_startup_gradient_overuse_intervals`
- `gradient_hold_events`
- `gradient_soft_backoffs`
- `gradient_plateau_events`
- `post_backoff_grace_events`
- `_post_backoff_grace_intervals_remaining`

These track consecutive signal triggers and make the final overview easier to interpret.

### 6.2 `_gradient_queue_floor_met()`

Defined at [bin/download_batch_gradient.py](../bin/download_batch_gradient.py).

This is a gatekeeper. It checks whether the smoothed queue-like delay is meaningfully above baseline:

```text
queue_delay_ema >= gradient_queue_floor_mult * rtprop
```

This prevents the controller from overreacting when queue-like delay is technically rising but still tiny in absolute terms.

### 6.3 `_is_gradient_plateau()`

Defined at [bin/download_batch_gradient.py](../bin/download_batch_gradient.py).

This replaces the base script's startup plateau logic.

A startup plateau is declared only if all of these are true:

- there is a valid gradient sample
- `rtprop` exists
- `gradient_ema > startup_gradient_threshold`
- `queue_delay_ema >= gradient_queue_floor_mult * rtprop`
- this has happened for at least `startup_gradient_required_intervals` consecutive intervals

Why this matters:

- `STARTUP` is intentionally aggressive
- this function gives it an early-warning exit before hard overload or very large latency inflation

When it fires, the controller logs a message like:

```text
[PAARC-GRAD] host: STARTUP gradient plateau | rtprop=... | queue=... | gradient=...
```

### 6.4 `_update_gradient_overuse_counter()`

Defined at [bin/download_batch_gradient.py](../bin/download_batch_gradient.py).

This is the `PROBE_BW` persistence tracker.

It does not decide the final reaction by itself. Instead it answers:

- "Have we seen enough consecutive above-floor positive-gradient evidence to trust the soft signal?"

The counter increases only when:

- there is a valid gradient sample
- `rtprop` exists
- `queue_delay_ema >= gradient_queue_floor_mult * rtprop`
- `gradient_ema > 0`

It resets if:

- the queue floor is not met
- `gradient_ema <= 0`
- or there is no usable gradient sample

Only once the counter reaches `gradient_required_intervals` can the steady-state reaction law hold or trim concurrency.

### 6.5 `_step_startup()`

Defined at [bin/download_batch_gradient.py](../bin/download_batch_gradient.py).

This function controls the aggressive ramp-up phase.

It behaves in this order:

1. If explicit overload happened:
   - reduce concurrency with hard `beta`
   - reset gradient soft state
   - enter `BACKOFF`
2. Else if gradient plateau happened:
   - set `C_ceiling = current concurrency`
   - set `C_operating = int(C_ceiling * mu)`
   - move to `PROBE_BW`
3. Else if `C_max` is reached:
   - cap and move to `PROBE_BW`
4. Else:
   - keep increasing by `startup_additive_increase`

This is one of the most important differences from the base script. In the gradient variant, `STARTUP` does not wait for `p50/p95` threshold inflation. It exits when the queue-like trend is rising enough, consistently enough.

### 6.6 `_step_probe_bw()`

Defined at [bin/download_batch_gradient.py](../bin/download_batch_gradient.py).

This is the steady-state probing phase.

It behaves in this order:

1. If explicit overload happened:
   - do the normal hard reduction
   - reset gradient soft state
   - enter `BACKOFF`
2. Else if the controller is in post-backoff grace:
   - do not evaluate gradient shaping
   - do not probe upward
   - just observe clean intervals and count grace down
3. Else if it is time for `PROBE_RTT`:
   - temporarily reduce concurrency to refresh `RTprop`
4. Else:
   - use the inherited steady-state probing logic

The new part is the reaction law used inside steady-state probing.

#### Region 1: Below Queue Floor

If:

```text
queue_delay_ema < gradient_queue_floor_mult * rtprop
```

then the queue-like delay is considered too small to react to. The controller keeps the ordinary PAARC `PROBE_BW` logic, including full additive probe-up when stable.

#### Region 2: Above Queue Floor, Relief Zone

If:

```text
queue_delay_ema >= gradient_queue_floor_mult * rtprop
gradient_ema <= 0
```

then the host still looks somewhat loaded, but the queue-like delay is no longer rising. In that case the hardened controller allows only a **cautious** increase:

```text
max(1, probe_bw_additive_increase // 2)
```

This is intentionally gentler than a full probe step.

#### Region 3: Above Queue Floor, Small Positive Gradient

If the queue floor is met, `gradient_ema > 0`, and the persistence counter is ready, but:

```text
0 < gradient_ema < gradient_threshold
```

the controller enters a hold zone:

- it does not increase concurrency
- it resets `stable_intervals`
- it records a `gradient_hold_event`
- it logs a `PROBE_BW hold`

This means:

- "pause probing upward"
- "but do not cut concurrency yet"

#### Region 4: Proportional Or Maximum Soft Backoff

If the persistence counter is ready and:

```text
gradient_threshold <= gradient_ema < gradient_severe_threshold
```

the controller applies a **proportional** soft multiplicative reduction. The beta value is interpolated between `1.0` and `gradient_backoff_beta`, so stronger positive slopes produce stronger trims.

If:

```text
gradient_ema >= gradient_severe_threshold
```

the controller applies the maximum soft multiplicative reduction using `gradient_backoff_beta`.

The resulting soft-backoff shape is:

```text
new_ceiling = max(C_min, int(current_concurrency * gradient_backoff_beta))
C_ceiling = new_ceiling
C_operating = int(C_ceiling * mu)
```

Then it sets concurrency using `"gradient_soft_backoff"`.

Important distinction:

- this is not the same as a hard overload backoff
- it does not enter `BACKOFF`
- it is a proactive trim triggered by trend, not by explicit rejection or failure

### 6.7 `BACKOFF` Recovery And Grace

The hardened controller keeps explicit overload as the authoritative emergency brake, but it also cleans up the transition back into soft shaping.

When the controller enters `BACKOFF` after hard overload:

- gradient soft state is reset
- consecutive gradient counters are reset

When the controller recovers from `BACKOFF` back to `PROBE_BW`:

- gradient state is reset again
- `post_backoff_grace_intervals_remaining` is set from `post_backoff_grace_intervals`
- `stable_intervals` is reset

During this grace window:

- hard overload still takes immediate precedence
- gradient shaping is suppressed
- upward additive probing is suppressed
- ceiling revision upward is suppressed

This prevents stale pre-overload gradient memory from immediately influencing the controller on the first clean recovery intervals.

That is closely aligned with the design goal we discussed: react before hard rate limiting if possible.

## 7. `HostControllerManager`

Defined at [bin/download_batch_gradient.py](../bin/download_batch_gradient.py).

This is a thin wrapper that creates `PAARCController` instances from this file instead of the base controller class.

Each host still gets its own independent controller.

So if the input manifest touches many domains:

- each host tracks its own `RTprop`
- its own gradient
- its own concurrency
- its own soft backoffs and holds

## 8. `collect_gradient_summary()`

Defined at [bin/download_batch_gradient.py](../bin/download_batch_gradient.py).

This aggregates controller metrics across all hosts for the final report:

- `gradient_hold_events`
- `gradient_soft_backoffs`
- `gradient_plateau_events`
- `intervals_with_gradient_samples`
- `max_queue_delay_ms`
- `max_gradient_ema`
- `avg_gradient_sample_count`
- `avg_gradient_confidence`
- `post_backoff_grace_events`
- `controller_variant = "gradient"`

These are important because the ordinary success/failure counts alone do not reveal whether the gradient logic actually activated.

## 9. `write_overview()`

Defined at [bin/download_batch_gradient.py](../bin/download_batch_gradient.py).

The gradient report extends `base.generate_overview_report()`. An internal `overview.json` is written before tar creation, so the archive contains the report and gradient counters. `write_overview()` delegates external-file writing to the base downloader and records the archive path there.

It is almost the same kind of report as the base downloader, but with:

- `paarc_version = "2.0.0-gradient"`
- `controller_variant = "gradient"`
- the new gradient config values under `script_inputs.paarc_config`
- a top-level `gradient_summary`

This makes runs auditable. You can tell after the fact:

- which controller variant was used
- which hyperparameters were active
- whether the gradient path actually influenced control behavior

## 10. `main()`

Defined at [bin/download_batch_gradient.py](../bin/download_batch_gradient.py).

This is the orchestration function. The flow is:

1. Parse config
2. Print banner
3. Load and validate manifest via `base.validate_and_load(cfg)`
4. If PAARC is enabled:
   - convert config with `to_paarc_config()`
   - create gradient-aware host manager
   - start `base.controller_loop(manager)`
5. Create `aiohttp` connector and trace config
6. Create shared naming and path-tracking structures
7. Open one `aiohttp.ClientSession`
8. Run the same bounded download pipeline as the base downloader
9. Retry retryable failures only
10. Cancel the controller loop
11. Collect gradient summary
12. Print final summary
13. Optionally write the internal overview JSON, including gradient counters
14. Optionally create tar, then write the external overview with its archive path
15. Print timing breakdown

The key point is that the actual downloading is still done by:

- [bin/download_batch.py](../bin/download_batch.py)

through `base.download_batch_bounded(...)`.

So the new script changes control decisions, not the fundamental download worker implementation.

## What The Config Is Doing

The config has three roles.

## 1. Describe The Download Job

These fields tell the downloader what to do:

- `input`
- `input_format`
- `output`
- `output_format`
- `url`
- `label`
- `concurrent_downloads`
- `timeout`
- `naming_mode`
- `file_name_pattern`
- `create_tar`
- `create_overview`

These are ordinary downloader/job settings.

## 2. Configure The Base PAARC Mechanics

These inherited parameters still shape the controller:

- `C_init`, `C_min`, `C_max`
- `mu`
- `beta`
- `probe_rtt_period`
- `rtprop_window`
- `cooldown_floor`
- `alpha_ema`
- `startup_additive_increase`
- `probe_bw_additive_increase`

These still matter because the gradient variant reuses the base controller framework.

## 3. Configure The New Gradient Decision Logic

These are the true new hyperparameters:

- `gradient_alpha`
- `gradient_threshold`
- `gradient_severe_threshold`
- `startup_gradient_threshold`
- `gradient_required_intervals`
- `startup_gradient_required_intervals`
- `gradient_queue_floor_mult`
- `gradient_backoff_beta`
- `post_backoff_grace_intervals`

Those are what change the controller from threshold-based to trend-based.

## Explicit Hyperparameters: Defaults Versus Example Config

This section separates:

- script defaults
- values explicitly set in [files/config/spider_test_gradient.json](../files/config/spider_test_gradient.json)

That distinction is important for correctness.

## A. Gradient-Specific Hyperparameters

### `gradient_alpha`

- Script default: `0.3`
- Example config: `0.3`
- Used in:
  - [bin/download_batch_gradient.py](../bin/download_batch_gradient.py)

What it does:

- smooths `queue_delay_raw` into `queue_delay_ema`
- smooths `gradient_raw` into `gradient_ema`

Why `0.3` is a reasonable choice:

- it gives noticeable smoothing without becoming sluggish
- it is consistent with the inherited PAARC EMA default `alpha_ema = 0.3`
- it helps the controller respond to trend while damping interval-to-interval jitter

Why this specific value was chosen here:

- in this repo, `0.3` is the established smoothing constant already used in PAARC-style metrics
- using the same value keeps the gradient path behaviorally aligned with the rest of the controller

### `gradient_threshold`

- Script default: `0.10`
- Example config: `0.10`

What it does:

- lower bound of the steady-state soft-reaction region once persistence is satisfied

Interpretation:

- the time-normalized, `RTprop`-normalized slope must be strong enough that a simple hold is no longer too permissive

Why `0.10` is a reasonable choice:

- it is tuned for the new time-normalized slope rather than the older per-interval delta ratio
- it is still early enough to react before explicit overload
- it is high enough that tiny positive drift, once normalized by time, does not immediately push the controller into multiplicative trim territory

### `gradient_severe_threshold`

- Script default: `0.30`
- Example config: `0.30`

What it does:

- upper bound of the proportional zone; above this, the maximum soft-backoff beta is used

Why `0.30` is a reasonable choice:

- it creates room for a meaningful proportional region between light and severe trend growth
- it reserves maximum soft backoff for clearly strong rising queue pressure
- it fits the new four-region law better than the older binary hold/severe split

### `startup_gradient_threshold`

- Script default: `0.15`
- Example config: `0.15`

What it does:

- controls when `STARTUP` exits due to a gradient plateau

Why `0.15` is a reasonable choice:

- startup should be more cautious than steady-state probing because concurrency is rising aggressively
- the threshold now lives on the new time-normalized slope scale
- it is intentionally above the ordinary "small positive trend" region
- that keeps `STARTUP` from bailing out on very weak upward drift while still exiting before explicit overload is required

This is a practical early-warning threshold, not a mathematically derived universal constant.

### `gradient_required_intervals`

- Script default: `2`
- Example config: `2`

What it does:

- requires overuse to persist for two consecutive control intervals before acting in `PROBE_BW`

Why `2` is a reasonable choice:

- one interval can be noisy
- two intervals gives a basic persistence check
- larger values would reduce noise further but make the controller slower to react

This is a standard compromise between sensitivity and stability.

### `startup_gradient_required_intervals`

- Script default: `2`
- Example config: `2`

What it does:

- requires startup plateau evidence to persist for two consecutive intervals

Why `2` is a reasonable choice:

- it avoids bailing out of startup on one noisy interval
- but still lets startup stop early enough to be useful

### `gradient_queue_floor_mult`

- Script default: `0.25`
- Example config: `0.25`

What it does:

- requires `queue_delay_ema >= 0.25 * rtprop`

Why this exists:

- a positive gradient is not enough by itself
- if the queue-like delay is still tiny, reacting would be too sensitive

Why `0.25` is a reasonable choice:

- it means the extra queue-like delay must be at least one quarter of the baseline latency
- that is large enough to be meaningful
- but still early relative to very large latency inflation

This is one of the most important "do not overreact to noise" guards in the design.

### `gradient_backoff_beta`

- Script default: `0.85`
- Example config: `0.85`

What it does:

- multiplicative reduction factor for soft backoff in `PROBE_BW`

Why `0.85` is a reasonable choice:

- it reduces concurrency by about 15%
- that is intentionally gentler than hard overload backoff with `beta = 0.5`
- it matches the semantics of a proactive trim rather than a punitive collapse

This is exactly what we wanted from a pre-overload signal.

### `post_backoff_grace_intervals`

- Script default: `2`
- Example config: `2`

What it does:

- after a hard-overload `BACKOFF` recovery, it forces the controller to spend two clean `PROBE_BW` intervals only observing fresh signals

Why `2` is a reasonable choice:

- one interval can still be contaminated by immediate post-recovery timing noise
- two intervals give the controller a small reset window without making recovery sluggish
- it cleanly separates emergency braking from renewed proactive shaping

## B. Inherited PAARC Hyperparameters That Still Matter

These are not unique to the gradient variant, but they are still active and still important.

### `C_init`

- Script default: `4`
- Example config: `8`

What it does:

- initial concurrency per host during `INIT`

Why the example uses `8`:

- it starts discovery a little faster than the base default
- still small enough to avoid an obviously reckless initial burst

### `C_min`

- Script default: `2`
- Example config: `2`

What it does:

- minimum per-host concurrency floor

Why `2`:

- avoids complete collapse to serial behavior
- still keeps the floor conservative

### `C_max`

- Script default: `10000`
- Example config: `1500`

What it does:

- absolute safety ceiling on per-host concurrency

Why the example uses `1500`:

- it aligns the per-host ceiling with the global run budget used in that example workload
- it is a practical safety bound for the spider test instead of a huge theoretical maximum

### `mu`

- Script default in the gradient script: `0.75`
- Example config: `0.85`

What it does:

- once a ceiling is identified, steady operation targets `int(C_ceiling * mu)`

Why `0.85` in the example:

- it keeps operation closer to the discovered ceiling
- this is more throughput-seeking than `0.75`
- it is still below 1.0, so the controller retains operating headroom

Important note:

- the example config explicitly overrides the script default here
- the script default and the example are not the same

### `beta`

- Script default: `0.5`
- Example config: `0.5`

What it does:

- hard multiplicative backoff factor after explicit overload

Why `0.5`:

- it is intentionally strong
- explicit overload is treated as a real congestion/failure event, not just early stress

This is separate from `gradient_backoff_beta = 0.85`, which is deliberately gentler.

### `startup_theta_50`, `startup_theta_95`, `theta_50`, `theta_95`

- Script defaults:
  - `startup_theta_50 = 3.0`
  - `startup_theta_95 = 4.0`
  - `theta_50 = 1.5`
  - `theta_95 = 2.0`
- Example config:
  - `startup_theta_50 = 12.0`
  - `startup_theta_95 = 24.0`
  - `theta_50 = 12.0`
  - `theta_95 = 24.0`

What they do in this script:

- they are parsed
- stored
- passed into `PAARCConfig`
- written to the overview

What they do not do here:

- they do not drive the gradient startup plateau logic
- they do not drive the gradient overuse logic

Why the example sets them so high:

- because this variant is intentionally not using them as its primary degradation trigger
- setting them very high effectively avoids accidental interference if inherited logic is ever consulted elsewhere

So in this script they are best viewed as compatibility fields, not active tuning knobs for the new controller behavior.

### `probe_rtt_period`

- Script default: `10.0`
- Example config: `30.0`

What it does:

- how often a controller is allowed to enter `PROBE_RTT`

Why the example uses `30.0`:

- less frequent baseline refresh
- lower controller disturbance on a large throughput-oriented run

This is a tradeoff:

- shorter period refreshes `RTprop` more aggressively
- longer period is less intrusive

### `rtprop_window`

- Script default: `15.0`
- Example config: `35.0`

What it does:

- inherited `RTprop` retention window in the base metrics logic

Why the example uses `35.0`:

- it keeps the baseline memory longer
- that can make the queue-delay signal more stable during a long single-host run

This is especially relevant for the gradient method because `queue_delay_raw` depends directly on `RTprop`.

### `cooldown_floor`

- Script default: `2.0`
- Example config: `3.0`

What it does:

- minimum cooldown after explicit overload

Why the example uses `3.0`:

- slightly more conservative recovery timing
- useful for public-host workloads where aggressive re-probing after overload is undesirable

### `alpha_ema`

- Script default: `0.3`
- Example config: `0.3`

What it does:

- inherited smoothing constant for the base PAARC metrics

Why it still matters:

- the base metrics path still computes smoothed latency summaries and other controller signals

### `startup_additive_increase`

- Script default: `30`
- Example config: not overridden, so `30`

What it does:

- amount added to concurrency on each `STARTUP` control step

Why `30`:

- it preserves the aggressive but additive startup strategy used by the current PAARC design

### `probe_bw_additive_increase`

- Script default: `10`
- Example config: not overridden, so `10`

What it does:

- amount added during stable `PROBE_BW` increase steps

Why `10`:

- slower than startup
- lets the controller probe carefully once it believes it has found a usable ceiling

## Hyperparameters In The Example Job Config

The tracked example config at [files/config/spider_test_gradient.json](../files/config/spider_test_gradient.json) defines this concrete job:

- input: `files/input/spider100_urls_200.parquet`
- output: `files/output/spider100_urls_200_gradient`
- url column: `photo_url`
- label column: `taxon_name`
- output format: `imagefolder`
- global downloader concurrency: `1500`
- timeout: `30`
- tar creation: disabled
- overview: enabled

This is a practical single-machine stress-style config, not merely a tiny toy example.

## The Core Design Difference From `download_batch.py`

The base downloader's question is roughly:

- "Is current latency already high enough relative to baseline?"

The gradient downloader's question is roughly:

- "Is queue-like delay rising persistently enough that we should stop pushing?"

So the variant changes the control philosophy from:

- threshold crossing

to:

- time-normalized trend detection with persistence, a queue floor, and a soft-versus-hard reaction split

That is the real conceptual change in this file.

## What Is Still Inherited And Unchanged

The following behaviors still come from the base downloader:

- manifest loading and output directory setup
- retry classification
- actual `aiohttp` download worker behavior
- TTFB trace collection
- semaphore/smoother mechanics
- `PROBE_RTT` implementation
- hard overload handling and cooldown logic
- control-interval scheduling helper
- tar creation

This is important because when you debug this file, some behavior you observe will still live in [bin/download_batch.py](../bin/download_batch.py), not in the variant itself.

## What The Current Example Run Suggests

In the recent repo-local A/B run already performed with this script:

- the gradient controller exited `STARTUP` earlier than the threshold-based controller
- it recorded hold events and soft backoffs
- it matched the baseline success count
- it did not regress throughput in that test

That means the current implementation is behaving consistently with its intended purpose:

- detect growing pressure early
- avoid waiting for explicit overload
- trim or pause before a harder backoff is needed

That does not prove the hyperparameters are globally optimal. It does show the implementation is operational and that the gradient path is actually live rather than dead code.

## Practical Reading Order For Future Work

If you want to modify this script, the best reading order is:

1. [bin/download_batch_gradient.py](../bin/download_batch_gradient.py) for config fields
2. [bin/download_batch_gradient.py](../bin/download_batch_gradient.py) for gradient signal construction
3. [bin/download_batch_gradient.py](../bin/download_batch_gradient.py) for startup plateau logic
4. [bin/download_batch_gradient.py](../bin/download_batch_gradient.py) for steady-state overuse logic
5. [bin/download_batch_gradient.py](../bin/download_batch_gradient.py) for the `PROBE_BW` action path
6. [bin/download_batch.py](../bin/download_batch.py) for the inherited control-interval scheduler
7. [bin/download_batch.py](../bin/download_batch.py) for the inherited download worker scheduler

## Short Summary

`download_batch_gradient.py` is a mostly-inherited FLOW-DC downloader whose real innovation is this:

- compute `queue_delay = p50_raw - RTprop`
- smooth it
- look at whether that queue-like delay is rising
- require the signal to persist
- hold or softly reduce concurrency before hard overload happens

The config therefore serves two jobs at once:

- ordinary downloader and PAARC configuration
- explicit tuning of that new early-warning gradient detector

The current hyperparameter values are practical heuristics chosen to make the signal:

- early enough to matter
- stable enough not to flap
- gentler than hard overload backoff
- cleaner around hard-overload recovery

They are reasonable and internally consistent for the current implementation, but they should still be treated as tuning defaults rather than mathematically final constants.
