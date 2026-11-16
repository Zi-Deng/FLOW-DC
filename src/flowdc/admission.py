"""Adaptive request permits; shrinking limits never cancels healthy requests."""

import asyncio
import sys


class AdaptiveSemaphore:
    """
    Adjustable semaphore for concurrency control.

    The primary control mechanism in PAARC v2.0. Limits the number of
    concurrent in-flight requests.
    """

    def __init__(self, initial: int, minimum: int = 2, maximum: int = 10000):
        self._limit = max(minimum, min(initial, maximum))
        self._minimum = minimum
        self._maximum = maximum
        self._inflight = 0
        self._cond = asyncio.Condition()

    async def acquire(self) -> None:
        """Acquire a slot, blocking if at limit."""
        async with self._cond:
            while self._inflight >= self._limit:
                await self._cond.wait()
            self._inflight += 1

    async def release(self) -> None:
        """Release a slot."""
        async with self._cond:
            self._inflight = max(0, self._inflight - 1)
            self._cond.notify(1)  # Only wake ONE waiter (fixes thundering herd)

    @property
    def limit(self) -> int:
        """Current concurrency limit."""
        return self._limit

    @property
    def inflight(self) -> int:
        """Current in-flight count."""
        return self._inflight

    def set_limit(self, new_limit: int, reason: str = "") -> None:
        """Adjust the concurrency limit."""
        new_limit = max(self._minimum, min(new_limit, self._maximum))
        if new_limit != self._limit:
            old = self._limit
            self._limit = new_limit
            reason_str = f" ({reason})" if reason else ""
            print(f"[PAARC] Concurrency: {old} → {new_limit}{reason_str}", file=sys.stderr)
            # Notify waiters if limit increased (fixes stalled limit increases)
            if new_limit > old:
                asyncio.create_task(self._notify_waiters(new_limit - old))

    async def _notify_waiters(self, count: int) -> None:
        """Wake up waiters when limit increases."""
        async with self._cond:
            self._cond.notify(count)
