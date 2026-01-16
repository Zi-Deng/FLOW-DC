# PAARC Performance Bugs Analysis

**Date:** January 2026
**Status:** Identified, pending fixes
**Severity:** Critical

## Executive Summary

During testing with iNaturalist Open Data (AWS-hosted, no server-side rate limits), FLOW-DC achieved only 5-21 req/s despite an expected throughput of 750+ req/s. Investigation revealed five critical bugs in the PAARC implementation that cause severe client-side bottlenecks.

The most telling symptom: when Ctrl+C was pressed to shutdown, throughput immediately jumped from ~7 url/s to 185 url/s, indicating the server was never the bottleneck.

---

## Bug #1: Thundering Herd in AdaptiveSemaphore

**Location:** `bin/download_batch.py`, lines 555-566
**Severity:** Critical

### Code

```python
class AdaptiveSemaphore:
    async def acquire(self) -> None:
        async with self._cond:
            while self._inflight >= self._limit and not shutdown_flag:
                await self._cond.wait()  # Thousands of tasks wait here
            self._inflight += 1

    async def release(self) -> None:
        async with self._cond:
            self._inflight = max(0, self._inflight - 1)
            self._cond.notify_all()  # ← BUG: Wakes ALL waiters!
```

### Problem

With configuration `concurrent_downloads: 50000` and a semaphore limit of ~260:

1. 50,000 worker coroutines are created
2. All attempt to `acquire()` the per-host semaphore
3. Only 260 can proceed; **49,740 are blocked** on `await self._cond.wait()`
4. When ONE download completes, `notify_all()` wakes **ALL 49,740 tasks**
5. All 49,740 race to check `self._inflight >= self._limit`
6. Only 1 wins; 49,739 go back to sleep
7. This repeats for EVERY download completion

### Impact

With ~10 downloads/sec completing, this causes **~500,000 spurious wake-ups per second**. The CPU spends all its time on context switching instead of downloading.

### Why Shutdown Speeds Things Up

When `shutdown_flag = True`, the `while` condition becomes false immediately, and ALL blocked tasks proceed at once, flooding the server with requests and revealing the true available bandwidth.

### Fix

```python
async def release(self) -> None:
    async with self._cond:
        self._inflight = max(0, self._inflight - 1)
        self._cond.notify(1)  # Only wake ONE waiter
```

---

## Bug #2: LeakyBucketSmoother Holds Lock During Sleep

**Location:** `bin/download_batch.py`, lines 623-632
**Severity:** Critical

### Code

```python
class LeakyBucketSmoother:
    async def acquire(self) -> None:
        async with self._lock:  # ← Holds lock for entire duration
            now = _monotonic()
            elapsed = now - self._last_request_time

            if elapsed < self._min_delay:
                await asyncio.sleep(self._min_delay - elapsed)  # ← Sleeps while holding lock!

            self._last_request_time = _monotonic()
```

### Problem

- 260 concurrent tasks try to acquire the smoother
- The asyncio.Lock is held **during the sleep**
- All other tasks must wait for one task to finish sleeping before they can even check if they need to sleep
- This effectively **serializes all requests** through a single bottleneck

### Impact

Even with 260 concurrent slots available, requests are funneled through a single lock, reducing effective parallelism to near-sequential execution.

### Fix

Restructure to release lock before sleeping, or make smoother optional for non-rate-limited servers:

```python
async def acquire(self) -> None:
    async with self._lock:
        now = _monotonic()
        elapsed = now - self._last_request_time
        wait_time = self._min_delay - elapsed if elapsed < self._min_delay else 0
        self._last_request_time = now + wait_time

    # Sleep OUTSIDE the lock
    if wait_time > 0:
        await asyncio.sleep(wait_time)
```

---

## Bug #3: set_limit() Doesn't Notify Waiters

**Location:** `bin/download_batch.py`, lines 578-585
**Severity:** High

### Code

```python
def set_limit(self, new_limit: int, reason: str = "") -> None:
    new_limit = max(self._minimum, min(new_limit, self._maximum))
    if new_limit != self._limit:
        old = self._limit
        self._limit = new_limit
        # ← NO notify_all() HERE!
```

### Problem

When PAARC increases the concurrency limit (e.g., 239 → 243 during probe_rtt_restore), the waiting tasks are **not notified**. They remain blocked until a download naturally completes and calls `release()`.

### Impact

Limit increases are ineffective until natural churn occurs. This makes PROBE_RTT restoration and PROBE_BW increases sluggish.

### Fix

```python
def set_limit(self, new_limit: int, reason: str = "") -> None:
    new_limit = max(self._minimum, min(new_limit, self._maximum))
    if new_limit != self._limit:
        old = self._limit
        self._limit = new_limit
        reason_str = f" ({reason})" if reason else ""
        print(f"[PAARC] Concurrency: {old} → {new_limit}{reason_str}")

        # Wake waiters if limit increased
        if new_limit > old:
            asyncio.create_task(self._notify_waiters())

async def _notify_waiters(self):
    async with self._cond:
        self._cond.notify_all()
```

---

## Bug #4: Ceiling Revision Misinterprets Client-Side Bottleneck

**Location:** `bin/download_batch.py`, lines 1391-1422
**Severity:** High

### Code

```python
def _check_ceiling_revision(self, snap: dict) -> None:
    rtprop = self._get_rtprop()
    expected_goodput = self._C_operating / rtprop  # e.g., 260 / 0.32 = 812 req/s
    actual_goodput = snap.get("goodput_rps", 0)    # e.g., 10 req/s (due to bugs)

    if actual_goodput < expected_goodput * self.config.underperformance_threshold:
        self._underperformance_intervals += 1

        if self._underperformance_intervals >= self.config.underperformance_intervals:
            # Reduce ceiling
            ...
```

### Problem

- RTprop measures Time-To-First-Byte (~0.32s), not total download time
- The client-side bugs reduce actual goodput to ~10 req/s
- Expected goodput based on RTprop is ~800 req/s
- PAARC concludes the **server** is underperforming
- Ceiling is revised **downward**, reducing concurrency further

### Impact

This creates a death spiral:
1. Client bugs cause low goodput
2. PAARC interprets this as server underperformance
3. Ceiling is reduced
4. Fewer concurrent slots available
5. Even lower goodput
6. Repeat

Observed in logs:
```
Concurrency: 263 → 221 (ceiling_revision)  # -42!
Concurrency: 271 → 227 (ceiling_revision)  # -44!
```

### Fix

1. Fix the client-side bugs first (Bugs #1-3)
2. Consider using a different metric that accounts for download time, not just TTFB
3. Add a minimum goodput floor before triggering revision
4. Make ceiling revision less aggressive or disable for known fast servers

---

## Bug #5: PROBE_RTT Restoration Is Too Slow

**Location:** `bin/download_batch.py`, lines 1461-1485
**Severity:** Medium

### Code

```python
def _restore_concurrency(self, now: float) -> None:
    target = self._saved_concurrency
    current = self._concurrency

    # Gradual restoration
    step = max(1, (target - current) // self.config.n_restore)  # n_restore = 5
    new_C = min(current + step, target)
    self._set_concurrency(new_C, "probe_rtt_restore")
```

### Problem

- PROBE_RTT drops concurrency to 50% (e.g., 500 → 250)
- Restoration adds `(target - current) / n_restore` per control interval
- Each interval is ~1 second
- **Restoration takes 5+ seconds** with artificially suppressed concurrency

### Observed Behavior

```
239 → 243 → 247 → 250 → 252 → 254 → 255 → 256 → 257 → 258 → 259 → 260 → 261 → 262 → 263
```

This is **14 seconds** of suppressed concurrency!

### Fix

Consider instant restoration instead of gradual:

```python
def _restore_concurrency(self, now: float) -> None:
    self._set_concurrency(self._saved_concurrency, "probe_rtt_restore")
    self.state = PAARCState.PROBE_BW
```

Or at minimum, restore in 2-3 steps instead of 5+.

---

## Evidence Summary

| Observation | Root Cause |
|-------------|------------|
| Measured goodput: 5-21 req/s | Thundering herd + smoother serialization |
| Inferred rate: 750-800 req/s | Based on RTprop (TTFB), ignores client bottlenecks |
| Frequent ceiling_revision | Misinterprets client bottleneck as server issue |
| Speed spike on Ctrl+C (7 → 185 url/s) | shutdown_flag bypasses semaphore waits |
| Slow PROBE_RTT restoration | Gradual restoration over many intervals |

---

## Immediate Workaround

Until bugs are fixed, reduce worker contention by matching `concurrent_downloads` to `C_max`:

```json
{
    "concurrent_downloads": 500,
    "C_max": 500
}
```

This doesn't fix the underlying bugs but reduces the thundering herd impact.

---

## Test Configuration That Exposed Bugs

```json
{
    "input": "files/input/inat_filtered_group_2.parquet",
    "concurrent_downloads": 50000,
    "C_max": 10000,
    "enable_paarc": true
}
```

Server: iNaturalist Open Data (AWS S3, no rate limits)
Expected: 50-100 MB/s
Actual: 0.5-2 MB/s
On shutdown: Jumped to 185 url/s

---

## Priority Order for Fixes

1. **Bug #1** (Thundering Herd) - Most impactful, causes most CPU waste
2. **Bug #2** (Smoother Lock) - Serializes all requests
3. **Bug #3** (set_limit notify) - Makes limit changes effective
4. **Bug #4** (Ceiling Revision) - Prevents death spiral
5. **Bug #5** (Slow Restoration) - Quality of life improvement
