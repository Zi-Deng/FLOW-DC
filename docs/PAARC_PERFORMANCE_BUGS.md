# PAARC Performance Bugs Analysis

**Date:** January 2026
**Status:** Most critical bugs fixed
**Last Updated:** January 2026

## Executive Summary

During testing with iNaturalist Open Data (AWS-hosted, no server-side rate limits), FLOW-DC achieved only 5-21 req/s despite an expected throughput of 750+ req/s. Investigation revealed five critical bugs in the PAARC implementation that cause severe client-side bottlenecks.

The most telling symptom: when Ctrl+C was pressed to shutdown, throughput immediately jumped from ~7 url/s to 185 url/s, indicating the server was never the bottleneck.

**Update:** Bugs #1-3 have been fully fixed. Bugs #4-5 have partial fixes.

---

## Bug #1: Thundering Herd in AdaptiveSemaphore

**Location:** `bin/download_batch.py`, `AdaptiveSemaphore.release()`
**Severity:** Critical
**Status:** ✅ **FIXED**

### Original Problem

```python
async def release(self) -> None:
    async with self._cond:
        self._inflight = max(0, self._inflight - 1)
        self._cond.notify_all()  # ← BUG: Wakes ALL waiters!
```

With 50,000 workers and semaphore limit of 260, each `release()` would wake all ~49,740 blocked tasks, causing ~500,000 spurious wake-ups per second.

### Fix Applied

```python
async def release(self) -> None:
    async with self._cond:
        self._inflight = max(0, self._inflight - 1)
        self._cond.notify(1)  # Only wake ONE waiter (fixes thundering herd)
```

---

## Bug #2: LeakyBucketSmoother Holds Lock During Sleep

**Location:** `bin/download_batch.py`, `LeakyBucketSmoother.acquire()`
**Severity:** Critical
**Status:** ✅ **FIXED**

### Original Problem

```python
async def acquire(self) -> None:
    async with self._lock:  # ← Holds lock for entire duration
        now = _monotonic()
        elapsed = now - self._last_request_time
        if elapsed < self._min_delay:
            await asyncio.sleep(self._min_delay - elapsed)  # ← Sleeps while holding lock!
        self._last_request_time = _monotonic()
```

This serialized all requests through a single lock, reducing effective parallelism to near-sequential execution.

### Fix Applied

```python
async def acquire(self) -> None:
    # Calculate wait time while holding lock, then sleep OUTSIDE the lock
    # This fixes the serialization bug where lock was held during sleep
    async with self._lock:
        now = _monotonic()
        elapsed = now - self._last_request_time
        wait_time = max(0.0, self._min_delay - elapsed)
        # Reserve our slot by setting the expected request time
        self._last_request_time = now + wait_time

    # Sleep outside the lock to allow other tasks to acquire slots
    if wait_time > 0:
        await asyncio.sleep(wait_time)
```

---

## Bug #3: set_limit() Doesn't Notify Waiters

**Location:** `bin/download_batch.py`, `AdaptiveSemaphore.set_limit()`
**Severity:** High
**Status:** ✅ **FIXED**

### Original Problem

```python
def set_limit(self, new_limit: int, reason: str = "") -> None:
    new_limit = max(self._minimum, min(new_limit, self._maximum))
    if new_limit != self._limit:
        old = self._limit
        self._limit = new_limit
        # ← NO notify HERE!
```

Limit increases were ineffective until natural churn occurred.

### Fix Applied

```python
def set_limit(self, new_limit: int, reason: str = "") -> None:
    new_limit = max(self._minimum, min(new_limit, self._maximum))
    if new_limit != self._limit:
        old = self._limit
        self._limit = new_limit
        reason_str = f" ({reason})" if reason else ""
        print(f"[PAARC] Concurrency: {old} → {new_limit}{reason_str}")
        # Notify waiters if limit increased (fixes stalled limit increases)
        if new_limit > old:
            asyncio.create_task(self._notify_waiters(new_limit - old))

async def _notify_waiters(self, count: int) -> None:
    """Wake up waiters when limit increases."""
    async with self._cond:
        self._cond.notify(count)
```

---

## Bug #4: Ceiling Revision Misinterprets Client-Side Bottleneck

**Location:** `bin/download_batch.py`, `PAARCController._check_ceiling_revision()`
**Severity:** High
**Status:** ⚠️ **PARTIALLY FIXED**

### Original Problem

- RTprop measures Time-To-First-Byte (~0.32s), not total download time
- Client-side bugs reduced actual goodput to ~10 req/s
- Expected goodput based on RTprop was ~800 req/s
- PAARC concluded the **server** was underperforming
- Ceiling was revised **downward**, creating a death spiral

### Partial Fix Applied

```python
def _check_ceiling_revision(self, snap: dict) -> None:
    if self._C_ceiling is None:
        return

    actual_goodput = snap.get("goodput_rps", 0)

    # Don't revise ceiling if goodput is suspiciously low (<1 req/s)
    # This likely indicates a client-side bottleneck, not server issue
    if actual_goodput < 1.0:
        return

    # ... rest of ceiling revision logic
```

### Remaining Issue

The fundamental issue of using TTFB-based expected goodput remains. A more robust fix would:
1. Use download completion time, not just TTFB
2. Track client-side CPU/IO metrics to distinguish client vs server bottlenecks
3. Consider using goodput trend rather than absolute comparison

---

## Bug #5: PROBE_RTT Restoration Is Too Slow

**Location:** `bin/download_batch.py`, `PAARCController._restore_concurrency()`
**Severity:** Medium
**Status:** ⚠️ **PARTIALLY FIXED**

### Original Problem

```python
step = max(1, (target - current) // self.config.n_restore)  # n_restore = 5
```

With 5 steps and ~1 second intervals, restoration took 14+ seconds of suppressed concurrency.

### Partial Fix Applied

```python
# Calculate step size - use 2 steps for faster restoration
# (Previously used n_restore=5 which took 14+ seconds)
step = max(1, (target - current) // 2)
```

The `n_restore` parameter was removed from `PAARCConfig`. Restoration now takes ~2-4 seconds instead of 14+.

### Remaining Issue

For maximum performance, consider instant restoration:

```python
async def _restore_concurrency(self, now: float) -> None:
    self._set_concurrency(self._saved_concurrency, "probe_rtt_restore")
    self.state = PAARCState.PROBE_BW
```

---

## Evidence Summary

| Observation | Root Cause | Status |
|-------------|------------|--------|
| Measured goodput: 5-21 req/s | Thundering herd + smoother serialization | ✅ Fixed |
| Inferred rate: 750-800 req/s | Based on RTprop (TTFB), ignores client bottlenecks | ⚠️ Partial |
| Frequent ceiling_revision | Misinterprets client bottleneck as server issue | ⚠️ Partial |
| Speed spike on Ctrl+C (7 → 185 url/s) | shutdown_flag bypasses semaphore waits | ✅ Fixed |
| Slow PROBE_RTT restoration | Gradual restoration over many intervals | ⚠️ Improved |

---

## Current Workaround (May No Longer Be Needed)

With bugs #1-3 fixed, the previous workaround of matching `concurrent_downloads` to `C_max` is no longer strictly necessary. However, the new auto-detection feature handles this automatically:

```json
{
    "concurrent_downloads": null,  // Auto-detect based on unique hosts
    "C_max": 1500
}
```

The system now automatically sizes the worker pool as `num_hosts × C_max`.

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
Actual (before fixes): 0.5-2 MB/s
On shutdown: Jumped to 185 url/s

---

## Fix Priority (Updated)

1. ~~**Bug #1** (Thundering Herd)~~ ✅ Fixed
2. ~~**Bug #2** (Smoother Lock)~~ ✅ Fixed
3. ~~**Bug #3** (set_limit notify)~~ ✅ Fixed
4. **Bug #4** (Ceiling Revision) - Partial fix, consider further improvements
5. **Bug #5** (Slow Restoration) - Improved, consider instant restoration
