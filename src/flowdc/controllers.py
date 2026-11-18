"""PAARC and its PAARC-Ratio alternative: pure policy transitions and HTTP observation integration."""

import hashlib
import math
import os
import re
import time
from dataclasses import asdict, dataclass, fields
from uuid import uuid4

from yarl import URL

from .admission import AdaptiveSemaphore

METHODS = ("paarc", "paarc-ratio")
HISTORICAL_IDS = {"paarc": "gradient-candidate-v2", "paarc-ratio": "ratio-v2"}


def require(value, message):
    if not value:
        raise ValueError(message)


def origin_key(url):
    parsed = URL(url)
    require(parsed.scheme in ("http", "https") and parsed.raw_host is not None, "HTTP origin required")
    return (parsed.scheme, parsed.raw_host.lower(), parsed.port)


def validate_research_metadata(frame, url_column):
    """Require portable, finite row metadata before publication or partitioning."""
    import polars as pl

    from .integrity import EXTERNAL_KEY, PARTITION_HOST, PROVENANCE, encode

    require(frame.schema[url_column] in (pl.String, pl.Null), "URL metadata must be String or null")
    for name, dtype in frame.schema.items():
        require(
            name in (*PROVENANCE, "__key__", EXTERNAL_KEY, PARTITION_HOST)
            or re.fullmatch("[a-zA-Z][a-zA-Z0-9_]*", name) is not None,
            "unsupported research metadata column",
        )
        require(
            dtype in (pl.String, pl.Boolean, pl.Float32, pl.Float64, pl.Null) or dtype.is_integer(),
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
    low, high = (math.floor(position), math.ceil(position))
    return values[low] + (values[high] - values[low]) * (position - low)


@dataclass(frozen=True)
class MethodConfig:
    method: str = "paarc"
    c_min: int = 2
    c_max: int = 192
    c_init: int = 4
    sample_min: int = 5
    sample_window_s: float = 2.0
    interval_s: float = 1.0
    queue_tau_s: float = 1.0
    delay_rate_tau_s: float = 1.0
    baseline_max_age_s: float = 120.0
    stale_after_s: float = 5.0
    probe_wait_s: float = 1.0
    queue_floor_s: float = 0.001
    numerical_floor_s: float = 1e-6
    delay_rate_hold_per_s: float = 0.1
    delay_rate_decrease_per_s: float = 0.25
    hard_queue_ratio: float = 4.0
    soft_factor: float = 0.85
    hard_factor: float = 0.5
    persistence: int = 2
    recovery_grace_s: float = 1.0
    startup_growth_factor: float = 1.5
    startup_max_step: int = 64
    target_delay_ratio: float = 2.0
    utilization_threshold: float = 0.8

    def __post_init__(self):
        require(
            self.method != "paarc-grad",
            "method 'paarc-grad' was renamed to 'paarc' in FLOW-DC 4; see docs/migration.md",
        )
        require(
            self.method in METHODS,
            "method must be paarc or paarc-ratio; retired spellings require migration (docs/migration.md)",
        )
        for field in fields(self):
            if field.name == "method":
                continue
            value = getattr(self, field.name)
            require(
                type(value) in (int, float) and math.isfinite(value) and value > 0,
                f"{field.name} must be finite and positive",
            )
        for name in ("c_min", "c_max", "c_init", "sample_min", "persistence", "startup_max_step"):
            require(type(getattr(self, name)) is int, f"{name} must be an integer")
        require(self.c_min <= self.c_init <= self.c_max <= 10000, "invalid concurrency bounds")
        require(0 < self.hard_factor <= self.soft_factor < 1, "invalid decrease factors")
        require(self.delay_rate_hold_per_s <= self.delay_rate_decrease_per_s, "invalid delay-rate thresholds")
        require(
            self.interval_s <= self.sample_window_s < self.stale_after_s <= self.baseline_max_age_s,
            "invalid freshness intervals",
        )
        require(self.probe_wait_s >= self.interval_s, "probe must span an interval")
        require(
            1 < self.startup_growth_factor <= 2 and 1 < self.target_delay_ratio < 5,
            "invalid growth or delay budget",
        )
        require(self.utilization_threshold <= 1, "invalid utilization threshold")

    @classmethod
    def from_config(cls, config):
        options = dict(config.method_options)
        retired = {
            "gradient_tau_s": "delay_rate_tau_s",
            "gradient_hold_per_s": "delay_rate_hold_per_s",
            "gradient_decrease_per_s": "delay_rate_decrease_per_s",
        }
        replacements = [f"{old} -> {new}" for old, new in retired.items() if old in options]
        require(
            not replacements, f"renamed controller options: {', '.join(replacements)}; see docs/migration.md"
        )
        require(
            not set(options).intersection({"method", "c_min", "c_max", "c_init"}),
            "method_options cannot override method or concurrency bounds",
        )
        require(
            not set(options) - {f.name for f in fields(cls)},
            f"unknown controller options: {sorted(set(options) - {f.name for f in fields(cls)})}; see docs/migration.md",
        )
        return cls(
            method=config.method, c_min=config.c_min, c_max=config.c_max, c_init=config.c_init, **options
        )


class DelayPolicy:
    """One origin's deterministic transition state on a monotonic seconds clock."""

    def __init__(self, config):
        self.config = config
        self.limit = config.c_init
        self.baseline = self.baseline_at = self.last_step = self.last_sample = None
        self.queue = self.delay_rate = self.current_delay = None
        self.probe_started = None
        self.recovery_until = 0.0
        self.overuse = 0
        self.exploring = True
        self.falling = 0

    def reset_delay_state(self):
        self.queue = self.delay_rate = self.current_delay = self.last_sample = None
        self.overuse = 0

    def step(self, now, delays, *, overload=False, inflight=0):
        cfg = self.config
        require(
            type(now) in (int, float) and math.isfinite(now) and (now >= 0), "invalid monotonic timestamp"
        )
        require(self.last_step is None or now > self.last_step, "control clock must advance")
        require(
            type(overload) is bool and type(inflight) is int and (inflight >= 0),
            "invalid observation counters",
        )
        require(
            all(type(x) in (int, float) and math.isfinite(x) and (x > 0) for x in delays),
            "delay samples must be finite and positive",
        )
        before = self.limit
        self.last_step = now
        p10 = percentile(delays, 0.1) if delays else None
        p50 = percentile(delays, 0.5) if delays else None
        elapsed = None
        raw_delay_rate = None
        alpha_q = alpha_s = None
        raw_queue = decision_queue = decision_delay_rate = ratio = None
        baseline_changed = False

        def result(reason):
            self.limit = min(cfg.c_max, max(cfg.c_min, int(self.limit)))
            return {
                "method": cfg.method,
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
                "raw_delay_rate_per_s": raw_delay_rate,
                "delay_rate_ewma_per_s": self.delay_rate,
                "sample_elapsed_s": elapsed,
                "queue_alpha": alpha_q,
                "delay_rate_alpha": alpha_s,
                "raw_queue_s": raw_queue,
                "decision_queue_s": decision_queue,
                "decision_delay_rate_per_s": decision_delay_rate,
                "ratio": ratio,
                "overuse_count": self.overuse,
                "probe_started_s": self.probe_started,
                "recovery_until_s": self.recovery_until,
                "overload": overload,
                "inflight": inflight,
                **{"exploring": self.exploring, "falling_ticks": self.falling},
            }

        if overload:
            self.limit = math.floor(self.limit * cfg.hard_factor)
            self.recovery_until = now + cfg.recovery_grace_s
            self.reset_delay_state()
            return result("overload_decrease")
        minimum = cfg.sample_min
        stale_baseline = self.baseline_at is not None and now - self.baseline_at >= cfg.baseline_max_age_s
        if stale_baseline and self.probe_started is None:
            self.probe_started = now
            self.exploring = True
            self.limit = cfg.c_min
            self.reset_delay_state()
            return result("baseline_probe")
        if len(delays) < minimum:
            self.overuse = 0
            if self.last_sample is not None and now - self.last_sample >= cfg.stale_after_s:
                self.reset_delay_state()
            return result("empty_hold" if not delays else "sparse_hold")
        if self.probe_started is not None:
            if now - self.probe_started < cfg.probe_wait_s or inflight > cfg.c_min:
                return result("probe_drain_hold")
            self.baseline, self.baseline_at = (max(p10, cfg.numerical_floor_s), now)
            self.probe_started = None
            self.reset_delay_state()
            baseline_changed = True
        elif self.baseline is None or max(p10, cfg.numerical_floor_s) < self.baseline:
            self.baseline, self.baseline_at = (max(p10, cfg.numerical_floor_s), now)
            self.reset_delay_state()
            baseline_changed = True
        raw_queue = max(0.0, p50 - self.baseline)
        if self.last_sample is None or now - self.last_sample >= cfg.stale_after_s:
            self.reset_delay_state()
            self.queue, self.current_delay, self.last_sample = (raw_queue, p50, now)
            return result("baseline_reset" if baseline_changed else "sample_initialize")
        elapsed = now - self.last_sample
        alpha_q = -math.expm1(-elapsed / cfg.queue_tau_s)
        alpha_s = -math.expm1(-elapsed / cfg.delay_rate_tau_s)
        queue = self.queue + alpha_q * (raw_queue - self.queue)
        raw_delay_rate = (queue - self.queue) / (elapsed * max(self.baseline, cfg.numerical_floor_s))
        self.delay_rate = (
            raw_delay_rate
            if self.delay_rate is None
            else self.delay_rate + alpha_s * (raw_delay_rate - self.delay_rate)
        )
        self.queue = queue
        self.current_delay += alpha_q * (p50 - self.current_delay)
        self.last_sample = now
        decision_queue, decision_delay_rate = (self.queue, self.delay_rate)
        if self.queue >= max(cfg.queue_floor_s, cfg.hard_queue_ratio * self.baseline):
            self.limit = math.floor(self.limit * cfg.hard_factor)
            self.reset_delay_state()
            self.recovery_until = now + cfg.recovery_grace_s
            return result("hard_queue_decrease")
        if now < self.recovery_until:
            self.overuse = 0
            return result("recovery_hold")
        utilized = inflight >= cfg.utilization_threshold * before
        budget = cfg.target_delay_ratio * self.baseline
        self.falling = self.falling + 1 if self.delay_rate <= -0.1 else 0
        if self.falling >= 3 and now >= self.recovery_until:
            self.exploring = True
        if self.current_delay >= budget:
            self.exploring = False
        if self.exploring and utilized:
            self.limit = min(before + cfg.startup_max_step, math.ceil(before * cfg.startup_growth_factor))
            return result("startup_increase")
        if cfg.method == "paarc-ratio":
            ratio = min(1.1, max(0.85, math.sqrt(budget / self.current_delay)))
            proposed = math.floor(before * ratio + 1)
            self.limit = proposed if proposed <= before or utilized else before
            return result("ratio_update" if self.limit != before else "utilization_hold")
        if self.current_delay >= budget:
            self.limit = math.floor(before * cfg.soft_factor)
            self.recovery_until = now + cfg.recovery_grace_s
            self.reset_delay_state()
            return result("delay_budget_decrease")
        if self.queue >= cfg.queue_floor_s and (self.delay_rate >= cfg.delay_rate_decrease_per_s):
            self.overuse += 1
            if self.overuse >= cfg.persistence:
                self.limit = math.floor(before * cfg.soft_factor)
                self.reset_delay_state()
                self.recovery_until = now + cfg.recovery_grace_s
                return result("delay_rate_decrease")
            return result("delay_rate_persistence_hold")
        self.overuse = 0
        if self.queue >= cfg.queue_floor_s and (self.delay_rate >= cfg.delay_rate_hold_per_s):
            return result("delay_rate_hold")
        if not utilized:
            return result("utilization_hold")
        self.limit += min(8, max(1, math.ceil(0.02 * before)))
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
            and (not is_unknown_error)
            and (
                is_conn_error or status_code in (408, 429) or (status_code is not None and status_code >= 500)
            )
        )
        eligible = (
            acquisition_success is not False and bytes_downloaded > 0
            if latency_eligible is None
            else latency_eligible
        )
        if (
            eligible
            and status_code == 200
            and (not is_conn_error)
            and (not is_unknown_error)
            and (type(ttfb) in (float, int))
            and math.isfinite(ttfb)
            and (ttfb > 0)
        ):
            require(len(self.samples) < 100000, "observation buffer full")
            self.samples.append((dispatch_at, time.monotonic(), ttfb))

    def consume(self, since=None, *, now=None, window=None, minimum=1):
        self.samples = [
            (dispatched, received, delay)
            for dispatched, received, delay in self.samples
            if (since is None or (dispatched is not None and dispatched >= since))
            and (
                window is None
                or (
                    type(dispatched) in (int, float)
                    and math.isfinite(dispatched)
                    and (0 <= now - dispatched < window)
                    and (0 <= now - received < window)
                )
            )
        ]
        delays = [delay for _, _, delay in self.samples]
        snap = {"delays": delays, "overload": self.overload, "total": self.total}
        if window is None or len(delays) >= minimum or self.overload:
            self.samples = []
        self.overload, self.total = (False, 0)
        return snap


class CandidateController:
    def __init__(self, host, config, semaphore_factory, emit):
        self.host, self.config, self.emit = (host, config, emit)
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
            minimum=self.config.sample_min,
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


class ControlTrace:
    """Retain each invocation, including interrupted ones, under run ownership."""

    def __init__(self, store, config):
        from . import integrity

        self.store, self.encode = (store, integrity.encode)
        self.identifier = uuid4().hex
        self.path = f".flowdc/control/{self.identifier}.jsonl"
        self.record_path = f".flowdc/control/{self.identifier}.json"
        self.record = {
            "schema": "flowdc-control-trace-v2",
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
    hashed, length = (hashlib.sha256(), 0)
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
            continue
        record = store.fs.json(directory + "/" + name)
        require(
            record["schema"] == "flowdc-control-trace-v2" and record["run_id"] == store.owner["run_id"],
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


class ControllerManager:
    def __init__(self, config, emit):
        self.method = MethodConfig.from_config(config)
        self.controllers = {}
        self.emit = emit
        self.failure = None

    def check_health(self):
        if self.failure is not None:
            raise RuntimeError("controller unavailable; admission stopped") from self.failure

    async def get_controller(self, url):
        self.check_health()
        origin = origin_key(url)
        if origin not in self.controllers:
            self.controllers[origin] = CandidateController(
                str(origin), self.method, AdaptiveSemaphore, self.emit
            )
        return self.controllers[origin]

    async def all_controllers(self):
        return list(self.controllers.values())


def method_record(config):
    return {
        "id": config.method,
        "historical_id": HISTORICAL_IDS[config.method],
        "explicit": True,
        "parameters": asdict(MethodConfig.from_config(config)),
        "measurement": "complete final-hop body-first-byte application delay; not packet RTT",
    }
