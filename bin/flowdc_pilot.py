"""Conservative accounting primitives for the bounded Jetstream2 pilot.

This module performs no cloud operations. The supervisor must persist each returned
state before acting on it, especially activation intent. An obligation lasts until
a fresh, identity-validated provider observation confirms SHELVED_OFFLOADED.
"""

import math
from uuid import UUID
from dataclasses import dataclass, replace

ALLOWANCE_SECONDS = 7200
SHUTDOWN_RESERVE_SECONDS = 600
INSPECTION_SECONDS = 1800
CLOCK_TOLERANCE_SECONDS = 5


class AccountingError(ValueError):
    """Fixed, public failure code; never carries provider diagnostics."""


def finite_seconds(value):
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
        raise AccountingError("invalid_seconds")
    return value


@dataclass(frozen=True)
class ClockSample:
    """CLOCK_BOOTTIME includes suspend; wall time supplies recovery evidence."""

    boot_id: str
    boottime: float
    utc: float

    def __post_init__(self):
        if not isinstance(self.boot_id, str) or not self.boot_id or len(self.boot_id) > 128:
            raise AccountingError("invalid_boot_identity")
        finite_seconds(self.boottime)
        finite_seconds(self.utc)


@dataclass(frozen=True)
class Allowance:
    """One VM's cumulative account, independent of operation/run/spec names.

    Consumption is deliberately not capped: overdue cleanup remains visible.
    Clock ambiguity permanently prevents activation, even after verified offload.
    Durable storage and resource/context binding belong to the journal layer.
    """

    limit: float = ALLOWANCE_SECONDS
    consumed: float = 0
    obligation: bool = False
    sample: ClockSample | None = None
    shutdown_at_consumed: float | None = None
    uncertain: bool = False
    campaign_id: str | None = None
    campaign_limit: float | None = None
    campaign_expires_utc: float | None = None
    campaign_window_seconds: int | None = None

    def __post_init__(self):
        finite_seconds(self.limit)
        finite_seconds(self.consumed)
        if not 0 < self.limit <= ALLOWANCE_SECONDS:
            raise AccountingError("invalid_allowance")
        campaign = (self.campaign_id, self.campaign_limit, self.campaign_expires_utc, self.campaign_window_seconds)
        if any(value is not None for value in campaign):
            try:
                canonical_id = str(UUID(self.campaign_id)) == self.campaign_id
            except (ValueError, TypeError, AttributeError):
                canonical_id = False
            if (not canonical_id
                    or self.campaign_limit is None or self.campaign_expires_utc is None
                    or type(self.campaign_window_seconds) is not int
                    or not 601 <= self.campaign_window_seconds <= 1800):
                raise AccountingError("invalid_campaign_account")
            finite_seconds(self.campaign_limit)
            finite_seconds(self.campaign_expires_utc)
            if not self.limit <= self.campaign_limit <= 604800:
                raise AccountingError("invalid_campaign_allowance")
        if type(self.obligation) is not bool or type(self.uncertain) is not bool:
            raise AccountingError("invalid_account")
        if self.obligation:
            if not isinstance(self.sample, ClockSample) or self.shutdown_at_consumed is None:
                raise AccountingError("missing_obligation_history")
            finite_seconds(self.shutdown_at_consumed)
            if self.shutdown_at_consumed > self.effective_limit - SHUTDOWN_RESERVE_SECONDS:
                raise AccountingError("invalid_shutdown_threshold")
        elif self.sample is not None or self.shutdown_at_consumed is not None:
            raise AccountingError("unexpected_obligation_history")

    @property
    def effective_limit(self):
        return self.campaign_limit if self.campaign_id is not None else self.limit

    @property
    def remaining(self):
        return max(0, self.effective_limit - self.consumed)

    def account(self, now):
        """Charge unknown time as active; fail closed on discontinuity/reboot.

        A reboot cannot establish a trustworthy elapsed upper bound using local
        clocks alone. Exhaust the remaining allowance and require cleanup, while
        retaining the obligation. Repeated observations never restore time.
        """
        if not isinstance(now, ClockSample):
            raise AccountingError("invalid_clock_sample")
        if not self.obligation:
            return self
        wall = now.utc - self.sample.utc
        elapsed = now.boottime - self.sample.boottime
        ambiguous = (
            now.boot_id != self.sample.boot_id
            or wall < 0
            or elapsed < 0
            or abs(wall - elapsed) > CLOCK_TOLERANCE_SECONDS
        )
        consumed = self.consumed + max(0, elapsed, wall)
        if ambiguous:
            consumed = max(self.effective_limit, consumed)
        return replace(self, consumed=consumed, sample=now, uncertain=self.uncertain or ambiguous)

    def activation_intent(self, now, *, window_seconds, inspection=False):
        """Create a chargeable obligation BEFORE attempting any unshelve.

        Window includes cleanup reserve and cannot exceed the remaining account.
        Duplicate intent cannot reset the current deadline or consume a new window.
        """
        finite_seconds(window_seconds)
        if not isinstance(now, ClockSample) or type(inspection) is not bool:
            raise AccountingError("invalid_activation_request")
        if self.obligation:
            raise AccountingError("outstanding_obligation")
        if self.uncertain:
            raise AccountingError("ambiguous_history")
        maximum = min(self.remaining, INSPECTION_SECONDS if inspection else self.effective_limit)
        if self.campaign_id is not None:
            if window_seconds > self.campaign_window_seconds or now.utc + window_seconds > self.campaign_expires_utc:
                raise AccountingError("campaign_expired_or_window_exceeded")
        if not SHUTDOWN_RESERVE_SECONDS < window_seconds <= maximum:
            raise AccountingError("insufficient_allowance_or_invalid_window")
        return replace(
            self,
            obligation=True,
            sample=now,
            shutdown_at_consumed=self.consumed + window_seconds - SHUTDOWN_RESERVE_SECONDS,
        )

    def shutdown_due(self, now):
        current = self.account(now)
        return current.obligation and (current.uncertain or current.consumed >= current.shutdown_at_consumed
            or (current.campaign_id is not None and now.utc >= current.campaign_expires_utc - SHUTDOWN_RESERVE_SECONDS))

    def observe(self, now, *, state):
        """Caller must validate observation identity/freshness before this call.

        SHUTOFF, SHELVED, request acknowledgement, timeout and unknown state do
        not settle an obligation. No provider response can replenish the account.
        """
        current = self.account(now)
        if state != "SHELVED_OFFLOADED":
            return current
        return replace(current, obligation=False, sample=None, shutdown_at_consumed=None)
