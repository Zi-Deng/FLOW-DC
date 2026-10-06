"""Prospective batch lifetime checks; the frozen per-process reader stays unchanged."""

import math
import time

import claude_native_auth as auth
from workflow import WorkflowError


def current_binding(unit_seconds, window_seconds, *, root=None, clock=time.time):
    """Validate fresh native provenance and a separate full remaining window.

    Only the integer process timeout enters the native reader. Fractional wall
    time remaining is rounded up, never down. No token or account data escapes
    this check, and neither a receipt nor a credential is renewed here.
    """
    if (
        type(unit_seconds) is not int
        or not 1 <= unit_seconds <= 900
        or not auth.finite(window_seconds)
        or not 0 < window_seconds <= auth.RECEIPT_SECONDS
    ):
        raise WorkflowError("Invalid batch lifetime window or per-process timeout")
    window = max(unit_seconds, math.ceil(window_seconds))
    start = clock()
    if not auth.finite(start) or start <= 0:
        raise WorkflowError("Invalid batch lifetime clock")
    with auth.store(root) as storage:
        receipt = storage.read("receipt.json")
        registration, snapshot, _ = auth._load(storage, unit_seconds, now=start)
        if receipt != storage.read("receipt.json"):
            raise WorkflowError("Paid-usage receipt changed during batch lifetime check")
        ready = clock()
        if not auth.finite(ready) or ready < start:
            raise WorkflowError("Batch lifetime clock rollback")
        required_until = ready + window + auth.REFRESH_MARGIN + auth.CLOCK_ALLOWANCE
        if (
            snapshot["claudeAiOauth"]["expiresAt"] / 1000 <= required_until
            or receipt["expires_at"] <= required_until
        ):
            raise WorkflowError("Native credentials or paid-usage receipt cannot cover full batch window")
        return dict(registration["authentication"])
