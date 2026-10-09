"""Gradient2 decision engine; acquisition measurement integration is separate.

Adapted from Netflix concurrency-limits, commit 78a74b9878d38c4c048b0304ce12a162ab7b7222.
Copyright 2018 Netflix, Inc. Licensed under Apache-2.0; see
third_party/netflix-gradient2/LICENSE and README.md for provenance and boundaries.
"""

import math
from dataclasses import dataclass

UPSTREAM_REVISION = "78a74b9878d38c4c048b0304ce12a162ab7b7222"
MAX_DELAY_NS = 2**53 - 1


def require(condition, message):
    if not condition:
        raise ValueError(message)


def nanoseconds(seconds):
    """Truncate positive finite application seconds; never fabricate a sample."""
    require(
        type(seconds) in (int, float) and 0 < seconds <= MAX_DELAY_NS / 1e9 and math.isfinite(seconds),
        "delay seconds outside research profile",
    )
    value = int(seconds * 1e9)
    require(1 <= value <= MAX_DELAY_NS, "delay is below one nanosecond or outside profile")
    return value


@dataclass(frozen=True)
class Gradient2Config:
    initial_limit: int = 20
    min_limit: int = 20
    max_limit: int = 200
    queue_size: int = 4
    smoothing: float = 0.2
    long_window: int = 600
    rtt_tolerance: float = 1.5

    def __post_init__(self):
        for name in ("initial_limit", "min_limit", "max_limit", "long_window"):
            require(
                type(getattr(self, name)) is int and 1 <= getattr(self, name) <= 10000,
                f"{name} must be an integer in [1,10000]",
            )
        require(self.min_limit <= self.initial_limit <= self.max_limit, "invalid concurrency bounds")
        require(type(self.queue_size) is int and 0 <= self.queue_size <= 10000, "invalid constant queue size")
        require(
            type(self.smoothing) in (int, float)
            and 0 <= self.smoothing <= 1
            and math.isfinite(self.smoothing),
            "smoothing must be finite in [0,1]",
        )
        require(
            type(self.rtt_tolerance) in (int, float)
            and 1 <= self.rtt_tolerance <= 10
            and math.isfinite(self.rtt_tolerance),
            "tolerance must be finite in [1,10]",
        )


class Gradient2:
    """Source-order translation of the pinned decision class's finite profile.

    The upstream request aggregation and limiter are not reproduced. Each call
    consumes one supplied observation; did_drop is ignored exactly as upstream.
    No PAARC probe, backoff, sample gate or recovery grace is added.
    """

    def __init__(self, config=None):
        self.config = Gradient2Config() if config is None else config
        require(isinstance(self.config, Gradient2Config), "Gradient2Config required")
        self.estimated_limit = float(self.config.initial_limit)
        self.long_delay = self.warmup_sum = 0.0
        self.warmup_count = self.observations = self.last_delay = 0
        self.reason = "no_observation"

    @property
    def limit(self):
        return int(self.estimated_limit)

    def sample(self, delay_ns, inflight, did_drop=False):
        require(type(delay_ns) is int and 1 <= delay_ns <= MAX_DELAY_NS, "invalid nanosecond sample")
        require(type(inflight) is int and 0 <= inflight <= 10000, "invalid aggregate inflight")
        require(type(did_drop) is bool, "drop flag must be boolean")
        cfg = self.config
        estimated = self.estimated_limit
        short = float(delay_ns)
        self.last_delay = delay_ns
        self.observations += 1
        if self.warmup_count < 10:
            self.warmup_count += 1
            self.warmup_sum += short
            self.long_delay = self.warmup_sum / self.warmup_count
        else:
            factor = 2.0 / (cfg.long_window + 1)
            self.long_delay = self.long_delay * (1 - factor) + short * factor
        long = self.long_delay
        if long / short > 2:
            self.long_delay *= 0.95
        if inflight < estimated / 2:
            self.reason = "application_limited"
            return self.limit
        gradient = max(0.5, min(1.0, cfg.rtt_tolerance * long / short))
        new_limit = estimated * gradient + cfg.queue_size
        new_limit = estimated * (1 - cfg.smoothing) + new_limit * cfg.smoothing
        self.estimated_limit = max(cfg.min_limit, min(cfg.max_limit, new_limit))
        self.reason = "updated"
        return self.limit

    def state(self):
        return {
            "limit": self.limit,
            "estimated_limit": self.estimated_limit,
            "long_delay_ns": self.long_delay,
            "last_delay_ns": self.last_delay,
            "observations": self.observations,
            "reason": self.reason,
        }
