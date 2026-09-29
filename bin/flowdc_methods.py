"""Versioned application-delay control candidates; not transport RTT algorithms.

The pure transition system also supplies the future shared-origin authority.
Defaults are engineering candidates, not advisor-selected scientific parameters.
"""

import hashlib
import math
import os
import re
import time
from dataclasses import asdict, dataclass, fields
from uuid import uuid4

from yarl import URL

METHODS = ("paarc-base-v2", "gradient-candidate-v1", "fixed-v1", "ratio-v1")
ABLATIONS = (
    "no-gradient-term",
    "no-elapsed-smoothing",
    "no-sample-gate",
    "no-baseline-refresh",
    "no-recovery-grace",
)


def require(value, message):
    if not value:
        raise ValueError(message)


def origin_key(url):
    parsed = URL(url)
    require(parsed.scheme in ("http", "https") and parsed.raw_host is not None, "HTTP origin required")
    return parsed.scheme, parsed.raw_host.lower(), parsed.port


def validate_research_metadata(frame, url_column):
    """New-method precondition, not a repair of legacy arbitrary typed metadata."""
    import polars as pl
    from flowdc_integrity import PROVENANCE, encode

    require(frame.schema[url_column] == pl.String, "research URL metadata must be String")
    for name, dtype in frame.schema.items():
        require(
            name in PROVENANCE or re.fullmatch(r"[a-zA-Z][a-zA-Z0-9_]*", name) is not None,
            "unsupported research metadata column",
        )
        require(
            dtype in (pl.String, pl.Boolean, pl.Int64, pl.Float64, pl.Null),
            f"unsupported research metadata dtype: {name}: {dtype}",
        )
    for row in frame.iter_rows(named=True):
        for value in row.values():
            require(type(value) is not int or abs(value) <= 2**53 - 1, "unsafe research metadata integer")
            require(type(value) is not float or math.isfinite(value), "nonfinite research metadata")
        require(len(encode(row)) <= 64 * 1024, "research row metadata exceeds 64 KiB")


def percentile(values, fraction):
    values = sorted(values)
    position = (len(values) - 1) * fraction
    low, high = math.floor(position), math.ceil(position)
    return values[low] + (values[high] - values[low]) * (position - low)


@dataclass(frozen=True)
class MethodConfig:
    method: str = "gradient-candidate-v1"
    c_min: int = 2
    c_max: int = 10000
    c_init: int = 4
    sample_min: int = 5
    sample_window_s: float | None = None  # None consumes each tick, preserving v1 defaults.
    interval_s: float = 0.2
    queue_tau_s: float = 1.0
    gradient_tau_s: float = 1.0
    baseline_max_age_s: float = 10.0
    stale_after_s: float = 2.0
    probe_wait_s: float = 0.4
    queue_floor_s: float = 0.001
    numerical_floor_s: float = 1e-6
    gradient_hold_per_s: float = 0.1
    gradient_decrease_per_s: float = 0.25
    hard_queue_ratio: float = 2.0
    soft_factor: float = 0.8
    hard_factor: float = 0.5
    persistence: int = 2
    increase_step: int = 1
    recovery_grace_s: float = 1.0
    ratio_buffer_fraction: float = 0.1
    ratio_headroom: int = 1
    ablation: str | None = None

    def __post_init__(self):
        require(self.method in METHODS, "unknown versioned control method")
        require(self.ablation is None or self.ablation in ABLATIONS, "unknown mechanism ablation")
        require(
            self.ablation is None or self.method == "gradient-candidate-v1",
            "ablations require gradient candidate",
        )
        for name in (
            "c_min",
            "c_max",
            "c_init",
            "sample_min",
            "persistence",
            "increase_step",
            "ratio_headroom",
        ):
            require(
                type(getattr(self, name)) is int and getattr(self, name) > 0,
                f"{name} must be a positive integer",
            )
        require(self.c_min <= self.c_init <= self.c_max <= 10000, "invalid concurrency bounds")
        for field in fields(self):
            if field.name not in ("method", "ablation"):
                value = getattr(self, field.name)
                if field.name == "sample_window_s" and value is None:
                    continue
                require(
                    type(value) in (int, float) and math.isfinite(value) and value > 0,
                    f"{field.name} must be finite and positive",
                )
        require(0 < self.hard_factor <= self.soft_factor < 1, "invalid decrease factors")
        require(self.gradient_hold_per_s <= self.gradient_decrease_per_s, "invalid gradient thresholds")
        require(
            self.interval_s < self.stale_after_s <= self.baseline_max_age_s, "invalid freshness intervals"
        )
        require(self.probe_wait_s >= self.interval_s, "probe must span at least one interval")
        require(self.ratio_buffer_fraction <= 1, "ratio buffer fraction exceeds one")
        require(
            self.sample_window_s is None or self.interval_s <= self.sample_window_s < self.stale_after_s,
            "sample window must span an interval and be shorter than the stale gap",
        )

    @classmethod
    def from_config(cls, config):
        options = dict(config.method_options or {})
        require(
            not set(options).intersection({"method", "c_min", "c_max", "c_init"}),
            "method options cannot override method or concurrency bounds",
        )
        if "legacy_alpha" in options:
            require(
                not set(options).intersection({"queue_tau_s", "gradient_tau_s"}),
                "conflicting alpha/tau inputs",
            )
            alpha, reference = options.pop("legacy_alpha"), options.pop("alpha_reference_s", None)
            require(
                type(alpha) in (int, float) and math.isfinite(alpha) and 0 < alpha < 1,
                "legacy_alpha must be in (0,1)",
            )
            require(
                type(reference) in (int, float) and math.isfinite(reference) and reference > 0,
                "legacy alpha requires positive alpha_reference_s",
            )
            options.update(
                queue_tau_s=-reference / math.log1p(-alpha), gradient_tau_s=-reference / math.log1p(-alpha)
            )
        known = {field.name for field in fields(cls)}
        require(not set(options) - known, "unknown versioned method option")
        require(
            config.control_method != "paarc-base-v2" or not options,
            "base PAARC uses its existing named parameters, not method_options",
        )
        return cls(
            method=config.control_method,
            c_min=config.C_min,
            c_max=config.C_max,
            c_init=config.C_init,
            **options,
        )


class DelayPolicy:
    """One origin's deterministic transition state on a monotonic seconds clock."""

    def __init__(self, config):
        require(config.method != "paarc-base-v2", "base PAARC retains its established state machine")
        self.config = config
        self.limit = config.c_init
        self.baseline = self.baseline_at = self.last_step = self.last_sample = None
        self.queue = self.gradient = self.current_delay = None
        self.probe_started = None
        self.recovery_until = 0.0
        self.overuse = 0

    def reset_derivative(self):
        self.queue = self.gradient = self.current_delay = self.last_sample = None
        self.overuse = 0

    def step(self, now, delays, *, overload=False, inflight=0):
        cfg = self.config
        require(type(now) in (int, float) and math.isfinite(now) and now >= 0, "invalid monotonic timestamp")
        require(self.last_step is None or now > self.last_step, "control clock must advance")
        require(
            type(overload) is bool and type(inflight) is int and inflight >= 0, "invalid observation counters"
        )
        require(
            all(type(x) in (int, float) and math.isfinite(x) and x > 0 for x in delays),
            "delay samples must be finite and positive",
        )
        before = self.limit
        self.last_step = now
        p10 = percentile(delays, 0.1) if delays else None
        p50 = percentile(delays, 0.5) if delays else None
        elapsed = None
        derivative = None
        alpha_q = alpha_g = None
        raw_queue = decision_queue = decision_gradient = ratio = None
        baseline_changed = False

        def result(reason):
            self.limit = min(cfg.c_max, max(cfg.c_min, int(self.limit)))
            return {
                "method": cfg.method,
                "ablation": cfg.ablation,
                "monotonic_s": now,
                "reason": reason,
                "limit_before": before,
                "limit": self.limit,
                "sample_count": len(delays),
                "p10_s": p10,
                "p50_s": p50,
                "baseline_s": self.baseline,
                "baseline_age_s": None if self.baseline_at is None else now - self.baseline_at,
                "baseline_changed": baseline_changed,
                "queue_ewma_s": self.queue,
                "current_delay_ewma_s": self.current_delay,
                "derivative_per_s": derivative,
                "gradient_ewma_per_s": self.gradient,
                "sample_elapsed_s": elapsed,
                "queue_alpha": alpha_q,
                "gradient_alpha": alpha_g,
                "raw_queue_s": raw_queue,
                "decision_queue_s": decision_queue,
                "decision_gradient_per_s": decision_gradient,
                "ratio": ratio,
                "overuse_count": self.overuse,
                "probe_started_s": self.probe_started,
                "recovery_until_s": self.recovery_until,
                "overload": overload,
                "inflight": inflight,
            }

        if cfg.method == "fixed-v1":
            return result("fixed")  # Mandatory embargo remains in shared HTTP admission.
        if overload:
            self.limit = math.floor(self.limit * cfg.hard_factor)
            self.recovery_until = now + (0 if cfg.ablation == "no-recovery-grace" else cfg.recovery_grace_s)
            self.reset_derivative()
            return result("overload_decrease")
        minimum = 1 if cfg.ablation == "no-sample-gate" else cfg.sample_min
        stale_baseline = self.baseline_at is not None and now - self.baseline_at >= cfg.baseline_max_age_s
        if stale_baseline and cfg.ablation != "no-baseline-refresh" and self.probe_started is None:
            self.probe_started = now
            self.limit = cfg.c_min
            self.reset_derivative()
            return result("baseline_probe")
        if len(delays) < minimum:
            self.overuse = 0
            if self.last_sample is not None and now - self.last_sample >= cfg.stale_after_s:
                self.reset_derivative()
            return result("empty_hold" if not delays else "sparse_hold")
        if self.probe_started is not None:
            if now - self.probe_started < cfg.probe_wait_s or inflight > cfg.c_min:
                return result("probe_drain_hold")
            # The integration supplies only observations dispatched in this probe.
            self.baseline, self.baseline_at = max(p10, cfg.numerical_floor_s), now
            self.probe_started = None
            self.reset_derivative()
            baseline_changed = True
        elif self.baseline is None or max(p10, cfg.numerical_floor_s) < self.baseline:
            self.baseline, self.baseline_at = max(p10, cfg.numerical_floor_s), now
            self.reset_derivative()
            baseline_changed = True
        if stale_baseline and not baseline_changed and cfg.ablation == "no-baseline-refresh":
            # Ablation removes active refresh, not evidence freshness safety.
            self.reset_derivative()
            return result("stale_baseline_hold")
        raw_queue = max(0.0, p50 - self.baseline)
        if self.last_sample is None or now - self.last_sample >= cfg.stale_after_s:
            self.reset_derivative()
            self.queue, self.current_delay, self.last_sample = raw_queue, p50, now
            return result("baseline_reset" if baseline_changed else "sample_initialize")
        elapsed = now - self.last_sample
        alpha_q = 1 if cfg.ablation == "no-elapsed-smoothing" else -math.expm1(-elapsed / cfg.queue_tau_s)
        alpha_g = 1 if cfg.ablation == "no-elapsed-smoothing" else -math.expm1(-elapsed / cfg.gradient_tau_s)
        queue = self.queue + alpha_q * (raw_queue - self.queue)
        derivative = (queue - self.queue) / (elapsed * max(self.baseline, cfg.numerical_floor_s))
        self.gradient = (
            derivative if self.gradient is None else self.gradient + alpha_g * (derivative - self.gradient)
        )
        self.queue = queue
        self.current_delay += alpha_q * (p50 - self.current_delay)
        self.last_sample = now
        decision_queue, decision_gradient = self.queue, self.gradient
        if self.queue >= max(cfg.queue_floor_s, cfg.hard_queue_ratio * self.baseline):
            self.limit = math.floor(self.limit * cfg.hard_factor)
            self.reset_derivative()
            self.recovery_until = now + (0 if cfg.ablation == "no-recovery-grace" else cfg.recovery_grace_s)
            return result("hard_queue_decrease")
        if now < self.recovery_until:
            self.overuse = 0
            return result("recovery_hold")
        if cfg.method == "ratio-v1":
            ratio = min(
                1.0,
                self.baseline
                * (1 + cfg.ratio_buffer_fraction)
                / max(self.current_delay, cfg.numerical_floor_s),
            )
            self.limit = math.floor(self.limit * ratio + cfg.ratio_headroom)
            return result("ratio_update")
        gradient_active = cfg.ablation != "no-gradient-term" and self.queue >= cfg.queue_floor_s
        if gradient_active and self.gradient >= cfg.gradient_decrease_per_s:
            self.overuse += 1
            if self.overuse >= cfg.persistence:
                self.limit = math.floor(self.limit * cfg.soft_factor)
                self.reset_derivative()
                self.recovery_until = now + (
                    0 if cfg.ablation == "no-recovery-grace" else cfg.recovery_grace_s
                )
                return result("gradient_decrease")
            return result("gradient_persistence_hold")
        self.overuse = 0
        if gradient_active and self.gradient >= cfg.gradient_hold_per_s:
            return result("gradient_hold")
        self.limit += cfg.increase_step
        return result("eligible_increase")


class ObservationBuffer:
    """Consume complete-body delay observations independently of local save success."""

    def __init__(self):
        self.samples = []
        self.overload = False
        self.total = 0

    async def record(
        self,
        status_code,
        ttfb,
        bytes_downloaded=0,
        is_conn_error=False,
        retry_after_sec=None,
        *,
        acquisition_success=None,
        is_local_error=False,
        is_unknown_error=False,
        latency_eligible=None,
        dispatch_at=None,
    ):
        self.total += 1
        self.overload |= (
            not is_local_error
            and not is_unknown_error
            and (
                is_conn_error or status_code in (408, 429) or (status_code is not None and status_code >= 500)
            )
        )
        eligible = (
            (acquisition_success is not False and bytes_downloaded > 0)
            if latency_eligible is None
            else latency_eligible
        )
        if (
            eligible
            and status_code == 200
            and not is_conn_error
            and not is_unknown_error
            and type(ttfb) in (float, int)
            and math.isfinite(ttfb)
            and ttfb > 0
        ):
            require(len(self.samples) < 100000, "observation buffer full")
            self.samples.append((dispatch_at, time.monotonic(), ttfb))

    def consume(self, since=None, *, now=None, window=None, minimum=1):
        self.samples = [
            (dispatched, received, delay)
            for dispatched, received, delay in self.samples
            if (since is None or dispatched is not None and dispatched >= since)
            and (
                window is None
                or (
                    type(dispatched) in (int, float)
                    and math.isfinite(dispatched)
                    and 0 <= now - dispatched < window
                    and 0 <= now - received < window
                )
            )
        ]
        delays = [delay for _, _, delay in self.samples]
        snap = {"delays": delays, "overload": self.overload, "total": self.total}
        if window is None or len(delays) >= minimum or self.overload:
            self.samples = []  # Eligible observations are used once; overload clears pending evidence.
        self.overload, self.total = False, 0
        return snap


class CandidateController:
    def __init__(self, host, config, semaphore_factory, emit):
        self.host, self.config, self.emit = host, config, emit
        self.policy = DelayPolicy(config)
        self.semaphore = semaphore_factory(config.c_init, config.c_min, config.c_max)
        self.metrics = ObservationBuffer()
        self.smoother = None

    async def step_interval(self):
        now = time.monotonic()
        snap = self.metrics.consume(
            self.policy.probe_started,
            now=now,
            window=self.config.sample_window_s,
            minimum=1 if self.config.ablation == "no-sample-gate" else self.config.sample_min,
        )
        record = self.policy.step(
            now, snap["delays"], overload=snap["overload"], inflight=self.semaphore.inflight
        )
        if record["reason"] == "baseline_probe":
            self.metrics.samples.clear()
        self.semaphore.set_limit(record["limit"], record["reason"])
        self.emit(
            {
                "origin": self.host,
                "new_observations": snap["total"],
                "pending_samples": len(self.metrics.samples),
                **record,
            }
        )
        return snap

    def _calculate_control_interval(self, snap):
        return self.config.interval_s


class ControllerManager:
    def __init__(self, config, semaphore_factory, base_factory, emit):
        self.config = config
        self.method = MethodConfig.from_config(config)
        self.semaphore_factory, self.base_factory, self.emit = semaphore_factory, base_factory, emit
        self.controllers = {}
        self.failure = None

    def check_health(self):
        if self.failure is not None:
            raise RuntimeError("versioned controller unavailable; admission stopped") from self.failure

    async def get_controller(self, url):
        self.check_health()
        origin = origin_key(url)
        if origin not in self.controllers:
            if self.method.method == "paarc-base-v2":
                controller = self.base_factory(str(origin), self.config.to_paarc_config())
                native_step = controller.step_interval

                async def step():
                    snap = await native_step()
                    self.emit(
                        {
                            "origin": origin,
                            "method": "paarc-base-v2",
                            "monotonic_s": time.monotonic(),
                            "state": controller.state.name,
                            "limit": controller.semaphore.limit,
                            "snapshot": snap,
                        }
                    )
                    return snap

                controller.step_interval = step
            else:
                controller = CandidateController(str(origin), self.method, self.semaphore_factory, self.emit)
            self.controllers[origin] = controller
        return self.controllers[origin]

    async def all_controllers(self):
        return list(self.controllers.values())


def method_record(config):
    if config.control_method is None:
        method = "gradient-legacy-v0" if hasattr(config, "gradient_threshold") else "paarc-base-v2"
        return {"id": method if config.enable_paarc else "legacy-uncontrolled-v0", "explicit": False}
    parameters = (
        asdict(config.to_paarc_config())
        if config.control_method == "paarc-base-v2"
        else asdict(MethodConfig.from_config(config))
    )
    return {
        "id": config.control_method,
        "explicit": True,
        "candidate": config.control_method != "paarc-base-v2",
        "parameters": parameters,
        "measurement": "complete final-hop body-first-byte application delay; not packet RTT",
    }


class ControlTrace:
    """Retain each invocation, including interrupted ones, under run ownership."""

    def __init__(self, store, config):
        import flowdc_integrity as integrity

        self.store, self.encode = store, integrity.encode
        self.identifier = uuid4().hex
        self.path = f".flowdc/control/{self.identifier}.jsonl"
        self.record_path = f".flowdc/control/{self.identifier}.json"
        self.record = {
            "schema": "flowdc-control-trace-v1",
            "path": self.path,
            "invocation_id": self.identifier,
            "run_id": store.owner["run_id"],
            "method": method_record(config),
            "closed": False,
            "records": 0,
        }
        self.stream = store.fs.open(self.path, write=True)
        try:
            store.fs.atomic(self.record_path, self.record)
        except BaseException:
            self.stream.close()
            raise
        self.emit({"event": "start", "monotonic_s": time.monotonic()})

    def emit(self, record):
        raw = self.encode({"run_id": self.record["run_id"], "invocation_id": self.identifier, **record})
        self.stream.write(raw)
        self.stream.flush()
        self.record["records"] += 1

    def close(self, complete=False):
        if self.stream.closed:
            return
        self.emit({"event": "end", "complete": complete, "monotonic_s": time.monotonic()})
        self.stream.flush()
        os.fsync(self.stream.fileno())
        self.stream.close()
        self.record.update(closed=True, complete=complete, **trace_hash(self.store, self.path))
        self.store.fs.atomic(self.record_path, self.record, replace=True)


def trace_hash(store, path):
    hashed, length = hashlib.sha256(), 0
    with store.fs.open(path) as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            hashed.update(chunk)
            length += len(chunk)
    return {"bytes": length, "sha256": hashed.hexdigest()}


def control_records(store):
    directory = ".flowdc/control"
    if not store.fs.exists(directory):
        return []
    records = []
    for name in store.fs.list(directory):
        if not name.endswith(".json"):
            continue  # A torn atomic record is retained but never promoted.
        record = store.fs.json(directory + "/" + name)
        require(
            record["schema"] == "flowdc-control-trace-v1" and record["run_id"] == store.owner["run_id"],
            "control trajectory ownership mismatch",
        )
        require(record["path"] == directory + "/" + name[:-5] + ".jsonl", "control trajectory path mismatch")
        observed = trace_hash(store, record["path"])
        if record["closed"]:
            require(all(record[key] == observed[key] for key in observed), "control trajectory modified")
        else:
            record = {**record, **observed, "complete": False, "records": None}
        records.append(record)
    return records
