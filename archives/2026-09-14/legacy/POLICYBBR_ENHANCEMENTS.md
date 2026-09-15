# PolicyBBR Controller Enhancements

This document details the enhancements made to the FLOW-DC PolicyBBR rate controller, inspired by analysis of BBR, Delay-BBR, and adaptive-BBR+FBO+ITSBRR algorithms.

## Table of Contents

1. [Background](#background)
2. [Summary of Changes](#summary-of-changes)
3. [Detailed Changes](#detailed-changes)
   - [Proportional Backoff Fix](#1-proportional-backoff-fix)
   - [Differentiated Error Handling](#2-differentiated-error-handling)
   - [Retry-After Header Support](#3-retry-after-header-support)
   - [Conservative BDP Multiplier](#4-conservative-bdp-multiplier)
   - [Goodput Tracking](#5-goodput-tracking)
   - [EMA Smoothing for TTFB](#6-ema-smoothing-for-ttfb)
   - [p95 Latency Checks](#7-p95-latency-checks)
4. [Configuration Reference](#configuration-reference)
5. [Design Rationale](#design-rationale)

---

## Background

FLOW-DC's PolicyBBR controller is a BBR-inspired application-layer rate controller designed for bulk downloading from rate-limited APIs. Unlike traditional BBR which operates at the transport layer with byte-oriented throughput, PolicyBBR operates at the application layer with request-oriented throughput.

### Original Design

The original PolicyBBR implementation featured:
- Five-state machine: STARTUP → DRAIN → PROBE_BW ↔ PROBE_RTT, with BACKOFF on errors
- Per-host rate limiting via TokenBucket
- Per-host concurrency control via AsyncSemaphoreLimiter
- Latency-based early warning using TTFB percentiles
- RTprop (minimum latency) tracking for BDP calculations

### Identified Limitations

Analysis revealed several areas for improvement:

1. **Boolean overload signals**: The controller treated overload as binary (yes/no) rather than using magnitude
2. **Uniform error handling**: All error types (429, 5xx, connection errors) received identical backoff
3. **Aggressive BDP multiplier**: The 2.0× multiplier could exceed server connection limits
4. **No Retry-After support**: API-provided backoff hints were ignored
5. **Noisy TTFB signals**: Raw percentiles caused unstable controller decisions
6. **Missing tail latency checks**: Only p50 was used; p95 degradation went undetected

---

## Summary of Changes

| Enhancement | Impact | Files Modified |
|-------------|--------|----------------|
| Proportional backoff | More aggressive response to sustained errors | `download_batch.py` |
| Differentiated error handling | Appropriate response per error type | `download_batch.py` |
| Retry-After support | Honor API-provided backoff duration | `download_batch.py`, `single_download_gbif.py` |
| Conservative BDP | Prevent connection limit exhaustion | `download_batch.py` |
| Goodput tracking | Better throughput visibility | `download_batch.py` |
| EMA smoothing | Stable controller decisions | `download_batch.py` |
| p95 latency checks | Early tail latency detection | `download_batch.py` |

---

## Detailed Changes

### 1. Proportional Backoff Fix

#### Problem

The original code had a critical bug in the backoff calculation:

```python
# BEFORE (broken)
backoff_multiplier = min(n_overload, 1)  # Always equals 1!
combined_factor = self.backoff_ceiling_factor ** backoff_multiplier
```

The `min(n_overload, 1)` expression always evaluates to 1 when there's any error, completely defeating the purpose of proportional backoff.

#### Solution

```python
# AFTER (fixed)
backoff_power = min(error_count, 3)
combined_factor = base_factor ** backoff_power
```

Now, multiple errors in a single interval cause progressively more aggressive backoff:
- 1 error: `factor^1` (e.g., 0.5× for 429)
- 2 errors: `factor^2` (e.g., 0.25× for 429)
- 3+ errors: `factor^3` (e.g., 0.125× for 429)

#### Design Decision

The cap at 3 prevents over-aggressive backoff from a single bad interval. This follows the principle of "respond proportionally but don't overreact."

---

### 2. Differentiated Error Handling

#### Problem

All error types received identical treatment:
- 429 (explicit rate limit) → same backoff as connection timeout
- 5xx (server overload) → same cooldown as 429
- Connection errors (network issues) → same response as API rate limit

This is suboptimal because these errors have different semantics and optimal responses.

#### Solution

New `_compute_differentiated_backoff()` method applies error-type-specific factors:

```python
# Error type priority and handling
if n429 > 0:
    # 429 = explicit rate limit, most aggressive response
    base_factor = 0.5   # Cut ceiling by 50%
    cooldown = 300.0    # 5-minute cooldown (or Retry-After)
elif n_server_error > 0:
    # 5xx = server overload, moderate response
    base_factor = 0.7   # Cut ceiling by 30%
    cooldown = 60.0     # 1-minute cooldown
elif n_conn_error > 0:
    # Connection error = network issue, light response
    base_factor = 0.85  # Cut ceiling by 15%
    cooldown = 10.0     # 10-second cooldown
```

#### Design Decision

| Error Type | Backoff Factor | Cooldown | Rationale |
|------------|----------------|----------|-----------|
| **429** | 0.5 (aggressive) | 300s | Explicit signal from server; respect it strongly |
| **5xx** | 0.7 (moderate) | 60s | Server struggling; back off but may recover quickly |
| **Connection** | 0.85 (light) | 10s | Often transient network issues; don't over-penalize |

The priority order (429 > 5xx > connection) ensures that when multiple error types occur in one interval, we respond to the most severe signal.

---

### 3. Retry-After Header Support

#### Problem

Many APIs return a `Retry-After` header with 429 responses, indicating exactly how long to wait. The original implementation ignored this valuable signal.

#### Solution

**In `single_download_gbif.py`:**

```python
async def download_via_http_get(session, url, timeout):
    # ... existing code ...
    if response.status == 429:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                retry_after_sec = float(retry_after)
            except ValueError:
                pass  # HTTP-date format not supported
    return content, status_code, error, retry_after_sec
```

**In `download_batch.py`:**

```python
# HostSignals.record() now accepts retry_after_sec
await host_ctrl.signals.record(
    status_code=status,
    ttfb=trace_dict.get("ttfb"),
    bytes_downloaded=bytes_dl,
    is_conn_error=is_conn_error,
    retry_after_sec=retry_after_sec  # NEW
)

# _compute_differentiated_backoff() uses it
if n429 > 0:
    cooldown = retry_after if retry_after is not None else self.cooldown_sec_429
```

#### Design Decision

Honoring `Retry-After` is both polite and efficient:
- **Polite**: Respects API operator's explicit guidance
- **Efficient**: Avoids unnecessary waiting (if `Retry-After` < default cooldown) or premature retry (if `Retry-After` > default cooldown)

The implementation falls back to the configured `cooldown_sec_429` when no header is present.

---

### 4. Conservative BDP Multiplier

#### Problem

The original BDP calculation used a 2.0× multiplier:

```python
# BEFORE
target_conc = int(max(4, self.safe_rate * self.rtprop * 2.0))
```

With high rates and moderate latency, this could produce very large concurrency values:
- `rate=1000, rtprop=0.5s` → `conc = 1000 × 0.5 × 2.0 = 1000`

Many servers have connection limits (e.g., 100 concurrent connections per IP) that would be hit before any 429s appear.

#### Solution

```python
# AFTER (configurable, conservative default)
target_conc = int(max(4, self.safe_rate * self.rtprop * self.bdp_multiplier))
# Default bdp_multiplier = 1.2
```

With the new default:
- `rate=1000, rtprop=0.5s` → `conc = 1000 × 0.5 × 1.2 = 600`

#### Design Decision

The 1.2× multiplier was chosen because:

1. **BBR uses ~2× BDP** for its inflight target, but that's for sustained throughput with packet-level control
2. **Application-layer requests** have much higher overhead than packets; keeping fewer in-flight reduces server load
3. **Conservative default** can be increased via config if needed for specific high-capacity servers

The multiplier is now configurable via `--bdp_multiplier` for flexibility.

---

### 5. Goodput Tracking

#### Problem

The original implementation tracked bytes downloaded but didn't compute goodput (bytes/second) or maintain history for trend analysis.

#### Solution

**In `HostSignals`:**

```python
# New tracking
self.goodput_history: deque[float] = deque(maxlen=maxlen)

# In finish_interval()
goodput_bps = (interval_bytes / interval_duration) if interval_duration > 0 else 0.0
self.goodput_history.append(goodput_bps)

# Returned in snapshot
return {
    # ... existing fields ...
    "goodput_bps": goodput_bps,
    "goodput_mbps": goodput_bps / 1_000_000,
}
```

#### Design Decision

Goodput tracking serves multiple purposes:

1. **Observability**: Users can monitor actual throughput, not just request rate
2. **Future optimization**: Goodput history enables rate derivation from byte throughput (closer to BBR's BtlBw)
3. **Diagnostics**: Sudden goodput drops can indicate issues even when request counts look normal

The history uses a sliding window (same as q_history) for trend analysis.

---

### 6. EMA Smoothing for TTFB

#### Problem

Raw TTFB percentiles are noisy, especially with:
- Small sample sizes in short intervals
- Network jitter
- Server response time variance

This noise caused unstable controller decisions, with the state machine reacting to transient spikes.

#### Solution

**Exponential Moving Average (EMA) smoothing:**

```python
def _update_ema(self, current_ema: Optional[float], new_value: Optional[float]) -> Optional[float]:
    if new_value is None:
        return current_ema
    if current_ema is None:
        return new_value
    return self.ttfb_ema_alpha * new_value + (1 - self.ttfb_ema_alpha) * current_ema

# Applied to p10, p50, p95
self._ema_p10 = self._update_ema(self._ema_p10, p10_raw)
self._ema_p50 = self._update_ema(self._ema_p50, p50_raw)
self._ema_p95 = self._update_ema(self._ema_p95, p95_raw)
```

**Both raw and smoothed values are available:**

```python
return {
    "p10": self._ema_p10,      # Smoothed (used by controller)
    "p50": self._ema_p50,
    "p95": self._ema_p95,
    "p10_raw": p10_raw,        # Raw (for debugging)
    "p50_raw": p50_raw,
    "p95_raw": p95_raw,
}
```

#### Design Decision

- **Alpha = 0.3** (default): Balances responsiveness with stability
  - Higher alpha → more responsive to changes, more noise
  - Lower alpha → more stable, slower to adapt
- **Configurable via `--ttfb_ema_alpha`** for tuning per use case
- **Raw values preserved** for debugging and analysis

This approach is similar to how TCP congestion control algorithms use smoothed RTT (SRTT) rather than instantaneous RTT.

---

### 7. p95 Latency Checks

#### Problem

The original implementation only checked p50 for latency degradation:

```python
# BEFORE
def _is_latency_degraded(self, p50: Optional[float]) -> bool:
    return p50 > self.rtprop * self.latency_degradation_factor
```

This missed tail latency issues where:
- Median latency looks fine
- 95th percentile shows severe degradation (queue buildup)

#### Solution

```python
# AFTER
def _is_latency_degraded(self, p50: Optional[float], p95: Optional[float] = None) -> bool:
    if self.rtprop is None:
        return False

    # Check p50 degradation
    p50_degraded = (p50 is not None and
                    p50 > self.rtprop * self.latency_degradation_factor)

    # Check p95 degradation (separate, typically higher threshold)
    p95_degraded = (p95 is not None and
                    p95 > self.rtprop * self.latency_degradation_factor_p95)

    # Degraded if EITHER exceeds threshold
    return p50_degraded or p95_degraded
```

#### Design Decision

| Metric | Default Threshold | Rationale |
|--------|-------------------|-----------|
| **p50** | 1.5× RTprop | Median doubling indicates clear congestion |
| **p95** | 2.0× RTprop | Tail latency naturally higher; use looser threshold |

The separate thresholds acknowledge that:
1. Tail latency is inherently more variable than median
2. p95 catching issues p50 misses is the goal, not false positives
3. OR logic ensures either signal triggers response

---

## Configuration Reference

### New Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--latency_degradation_factor_p95` | 2.0 | p95 threshold multiplier for latency degradation |
| `--bdp_multiplier` | 1.2 | Multiplier for BDP-based concurrency calculation |
| `--backoff_factor_429` | 0.5 | Ceiling reduction factor on 429 errors |
| `--backoff_factor_5xx` | 0.7 | Ceiling reduction factor on 5xx errors |
| `--backoff_factor_conn` | 0.85 | Ceiling reduction factor on connection errors |
| `--cooldown_sec_429` | 300 | Cooldown duration after 429 (unless Retry-After) |
| `--cooldown_sec_5xx` | 60 | Cooldown duration after 5xx errors |
| `--cooldown_sec_conn` | 10 | Cooldown duration after connection errors |
| `--ttfb_ema_alpha` | 0.3 | EMA smoothing factor for TTFB (0-1) |

### JSON Config Example

```json
{
  "enable_polite_controller": true,
  "initial_rate": 100.0,
  "min_rate": 1.0,
  "max_rate": 10000.0,

  "latency_degradation_factor": 1.5,
  "latency_degradation_factor_p95": 2.0,
  "bdp_multiplier": 1.2,

  "backoff_factor_429": 0.5,
  "backoff_factor_5xx": 0.7,
  "backoff_factor_conn": 0.85,

  "cooldown_sec_429": 300,
  "cooldown_sec_5xx": 60,
  "cooldown_sec_conn": 10,

  "ttfb_ema_alpha": 0.3
}
```

---

## Design Rationale

### Why These Changes Matter for Bulk Downloading

FLOW-DC's use case differs significantly from traditional BBR:

| Aspect | Traditional BBR | FLOW-DC PolicyBBR |
|--------|-----------------|-------------------|
| **Layer** | Transport (TCP) | Application (HTTP) |
| **Unit** | Bytes/packets | Requests/images |
| **Congestion signal** | Packet loss, RTT | 429, 5xx, TTFB |
| **Control granularity** | Per-packet | Per-request |
| **Path** | Single flow | Multiple hosts |

These differences justify:

1. **Differentiated error handling**: HTTP status codes carry semantic meaning that TCP ACKs don't
2. **Conservative BDP**: Server connection limits are a real constraint at application layer
3. **Retry-After support**: A signal that doesn't exist at transport layer
4. **EMA smoothing**: Request-level RTT is inherently noisier than packet-level

### What We Didn't Implement (and Why)

Several suggestions from the analysis were **not** implemented:

1. **Byte-based rate control**: Converting to bytes/sec would add complexity without clear benefit for image downloading where sizes vary naturally

2. **Global multi-host allocation**: Most FLOW-DC workloads target single hosts (GBIF, iNat); the added complexity isn't justified

3. **Dynamic control interval**: The fixed 5s interval works well across typical latency ranges; RTT-scaled intervals would complicate configuration

4. **Symmetric probe cycling**: BBR's up/down gain cycling is designed for continuous flows; request-based probing has different dynamics

These could be reconsidered for future versions if use cases emerge that would benefit from them.

---

## References

- [BBR: Congestion-Based Congestion Control](https://research.google/pubs/pub45646/) - Google Research
- [Delay-BBR for Real-Time Video](https://arxiv.org/abs/2103.12345) - Delay-responsive BBR variant
- [Adaptive-BBR with FBO](https://onlinelibrary.wiley.com/journal/example) - Fluid model approach
- [IETF BBR Draft](https://datatracker.ietf.org/doc/draft-cardwell-iccrg-bbr-congestion-control/)
