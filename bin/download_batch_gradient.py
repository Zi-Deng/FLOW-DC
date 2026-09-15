#!/usr/bin/env python3
"""
FLOW-DC Batch Downloader with Gradient PAARC

This standalone variant keeps the current FLOW-DC downloader pipeline but
replaces the p50/p95 threshold degradation logic with a TIMELY-style
queue-delay gradient detector built from interval p50_raw and RTprop.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlsplit

import aiohttp
import polars as pl

import download_batch as base


@dataclass(frozen=True)
class PAARCConfig(base.PAARCConfig):
    """Gradient-augmented PAARC configuration."""

    rtprop_window: float = 15.0
    gradient_alpha: float = 0.3
    gradient_threshold: float = 0.10
    gradient_severe_threshold: float = 0.30
    startup_gradient_threshold: float = 0.15
    gradient_required_intervals: int = 2
    startup_gradient_required_intervals: int = 2
    gradient_queue_floor_mult: float = 0.25
    gradient_backoff_beta: float = 0.85
    post_backoff_grace_intervals: int = 2


@dataclass(frozen=True)
class Config(base.Config):
    """Main application configuration for the gradient variant."""

    # Preserve experimental defaults while supporting the shared pipeline.
    concurrent_downloads: Optional[int] = 256
    mu: float = 0.75
    rtprop_window: float = 15.0
    gradient_alpha: float = 0.3
    gradient_threshold: float = 0.10
    gradient_severe_threshold: float = 0.30
    startup_gradient_threshold: float = 0.15
    gradient_required_intervals: int = 2
    startup_gradient_required_intervals: int = 2
    gradient_queue_floor_mult: float = 0.25
    gradient_backoff_beta: float = 0.85
    post_backoff_grace_intervals: int = 2

    def to_paarc_config(self) -> PAARCConfig:
        return PAARCConfig(
            C_init=self.C_init,
            C_min=self.C_min,
            C_max=self.C_max,
            mu=self.mu,
            startup_theta_50=self.startup_theta_50,
            startup_theta_95=self.startup_theta_95,
            efficiency_threshold=self.efficiency_threshold,
            efficiency_window=self.efficiency_window,
            startup_additive_increase=self.startup_additive_increase,
            probe_bw_additive_increase=self.probe_bw_additive_increase,
            beta=self.beta,
            theta_50=self.theta_50,
            theta_95=self.theta_95,
            probe_rtt_period=self.probe_rtt_period,
            rtprop_window=self.rtprop_window,
            cooldown_floor=self.cooldown_floor,
            alpha_ema=self.alpha_ema,
            gradient_alpha=self.gradient_alpha,
            gradient_threshold=self.gradient_threshold,
            gradient_severe_threshold=self.gradient_severe_threshold,
            startup_gradient_threshold=self.startup_gradient_threshold,
            gradient_required_intervals=self.gradient_required_intervals,
            startup_gradient_required_intervals=self.startup_gradient_required_intervals,
            gradient_queue_floor_mult=self.gradient_queue_floor_mult,
            gradient_backoff_beta=self.gradient_backoff_beta,
            post_backoff_grace_intervals=self.post_backoff_grace_intervals,
        )


def parse_args() -> Config:
    """Parse command line arguments or JSON config file."""
    p = argparse.ArgumentParser(
        description="FLOW-DC Batch Downloader with Gradient PAARC",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python download_batch_gradient.py --config files/config/spider_test_gradient.json
  python download_batch_gradient.py --input urls.parquet --output images/ --enable_paarc
""",
    )

    p.add_argument("--config", type=str, help="Path to JSON config file")

    # Input/Output
    p.add_argument("--input", dest="input_path", type=str, help="Input file path")
    p.add_argument("--input_format", type=str, default="parquet")
    p.add_argument("--url", dest="url_col", type=str, default="url")
    p.add_argument("--label", dest="label_col", type=str, default=None)
    p.add_argument("--output", dest="output_folder", type=str, help="Output folder")
    p.add_argument("--output_format", type=str, default="imagefolder")

    # Download settings
    p.add_argument("--concurrent_downloads", type=int, default=256,
                   help="Worker count; 0 enables host-based automatic sizing")
    p.add_argument("--timeout", dest="timeout_sec", type=int, default=30)

    # PAARC toggle
    p.add_argument("--enable_paarc", action="store_true", default=True)
    p.add_argument("--disable_paarc", action="store_true")

    # Compatibility parameters (accepted but not used for gradient decisions)
    p.add_argument("--C_init", type=int, default=4)
    p.add_argument("--C_min", type=int, default=2)
    p.add_argument("--C_max", type=int, default=10000)
    p.add_argument("--mu", type=float, default=0.75, help="Utilization factor")
    p.add_argument("--startup_theta_50", type=float, default=3.0)
    p.add_argument("--startup_theta_95", type=float, default=4.0)
    p.add_argument("--efficiency_threshold", type=float, default=0.5)
    p.add_argument("--efficiency_window", type=int, default=5)
    p.add_argument("--beta", type=float, default=0.5, help="Hard backoff factor")
    p.add_argument("--theta_50", type=float, default=1.5)
    p.add_argument("--theta_95", type=float, default=2.0)
    p.add_argument("--probe_rtt_period", type=float, default=10.0)
    p.add_argument("--rtprop_window", type=float, default=15.0)
    p.add_argument("--cooldown_floor", type=float, default=2.0)
    p.add_argument("--alpha_ema", type=float, default=0.3)
    p.add_argument("--startup_additive_increase", type=int, default=30)
    p.add_argument("--probe_bw_additive_increase", type=int, default=10)

    # Gradient-specific parameters
    p.add_argument("--gradient_alpha", type=float, default=0.3)
    p.add_argument("--gradient_threshold", type=float, default=0.10)
    p.add_argument("--gradient_severe_threshold", type=float, default=0.30)
    p.add_argument("--startup_gradient_threshold", type=float, default=0.15)
    p.add_argument("--gradient_required_intervals", type=int, default=2)
    p.add_argument("--startup_gradient_required_intervals", type=int, default=2)
    p.add_argument("--gradient_queue_floor_mult", type=float, default=0.25)
    p.add_argument("--gradient_backoff_beta", type=float, default=0.85)
    p.add_argument("--post_backoff_grace_intervals", type=int, default=2)

    # Retry
    p.add_argument("--max_retry_attempts", type=int, default=3)
    p.add_argument("--retry_backoff_sec", type=float, default=2.0)

    # Naming
    p.add_argument("--naming_mode", type=str, default="sequential", choices=["sequential", "url_based"])
    p.add_argument("--file_name_pattern", type=str, default="{segment[-2]}")

    # Output options
    p.add_argument("--no_tar", action="store_true")
    p.add_argument(
        "--no_compress_tar",
        action="store_true",
        help="Create uncompressed .tar instead of .tar.gz (faster)",
    )
    p.add_argument("--no_overview", action="store_true")
    p.add_argument("--force", "-f", action="store_true",
                   help="Overwrite an existing output directory without confirmation")

    args = p.parse_args()

    if args.config:
        cfg_path = Path(args.config)
        with cfg_path.open("r") as f:
            data = json.load(f)

        return Config(
            input_path=data.get("input", ""),
            output_folder=data.get("output", ""),
            input_format=data.get("input_format", "parquet"),
            url_col=data.get("url", "url"),
            label_col=data.get("label"),
            output_format=data.get("output_format", "imagefolder"),
            concurrent_downloads=(int(data["concurrent_downloads"])
                                  if data.get("concurrent_downloads") is not None
                                  else (None if "concurrent_downloads" in data else 256)),
            timeout_sec=int(data.get("timeout", 30)),
            enable_paarc=bool(data.get("enable_paarc", True)),
            C_init=int(data.get("C_init", 4)),
            C_min=int(data.get("C_min", 2)),
            C_max=int(data.get("C_max", 10000)),
            mu=float(data.get("mu", 0.75)),
            startup_theta_50=float(data.get("startup_theta_50", 3.0)),
            startup_theta_95=float(data.get("startup_theta_95", 4.0)),
            efficiency_threshold=float(data.get("efficiency_threshold", 0.5)),
            efficiency_window=int(data.get("efficiency_window", 5)),
            beta=float(data.get("beta", 0.5)),
            theta_50=float(data.get("theta_50", 1.5)),
            theta_95=float(data.get("theta_95", 2.0)),
            probe_rtt_period=float(data.get("probe_rtt_period", 10.0)),
            rtprop_window=float(data.get("rtprop_window", 15.0)),
            cooldown_floor=float(data.get("cooldown_floor", 2.0)),
            alpha_ema=float(data.get("alpha_ema", 0.3)),
            startup_additive_increase=int(data.get("startup_additive_increase", 30)),
            probe_bw_additive_increase=int(data.get("probe_bw_additive_increase", 10)),
            gradient_alpha=float(data.get("gradient_alpha", 0.3)),
            gradient_threshold=float(data.get("gradient_threshold", 0.10)),
            gradient_severe_threshold=float(data.get("gradient_severe_threshold", 0.30)),
            startup_gradient_threshold=float(data.get("startup_gradient_threshold", 0.15)),
            gradient_required_intervals=int(data.get("gradient_required_intervals", 2)),
            startup_gradient_required_intervals=int(
                data.get("startup_gradient_required_intervals", 2)
            ),
            gradient_queue_floor_mult=float(data.get("gradient_queue_floor_mult", 0.25)),
            gradient_backoff_beta=float(data.get("gradient_backoff_beta", 0.85)),
            post_backoff_grace_intervals=int(data.get("post_backoff_grace_intervals", 2)),
            max_retry_attempts=int(data.get("max_retry_attempts", 3)),
            retry_backoff_sec=float(data.get("retry_backoff_sec", 2.0)),
            naming_mode=data.get("naming_mode", "sequential"),
            file_name_pattern=data.get("file_name_pattern", "{segment[-2]}"),
            create_tar=bool(data.get("create_tar", True)),
            compress_tar=bool(data.get("compress_tar", True)),
            create_overview=bool(data.get("create_overview", True)),
            force_overwrite=args.force or bool(data.get("force_overwrite", False)),
        )

    if not args.input_path or not args.output_folder:
        p.error("--input and --output are required unless --config is provided")

    return Config(
        input_path=args.input_path,
        output_folder=args.output_folder,
        input_format=args.input_format,
        url_col=args.url_col,
        label_col=args.label_col,
        output_format=args.output_format,
        concurrent_downloads=args.concurrent_downloads,
        timeout_sec=args.timeout_sec,
        enable_paarc=args.enable_paarc and not args.disable_paarc,
        C_init=args.C_init,
        C_min=args.C_min,
        C_max=args.C_max,
        mu=args.mu,
        startup_theta_50=args.startup_theta_50,
        startup_theta_95=args.startup_theta_95,
        efficiency_threshold=args.efficiency_threshold,
        efficiency_window=args.efficiency_window,
        beta=args.beta,
        theta_50=args.theta_50,
        theta_95=args.theta_95,
        probe_rtt_period=args.probe_rtt_period,
        rtprop_window=args.rtprop_window,
        cooldown_floor=args.cooldown_floor,
        alpha_ema=args.alpha_ema,
        startup_additive_increase=args.startup_additive_increase,
        probe_bw_additive_increase=args.probe_bw_additive_increase,
        gradient_alpha=args.gradient_alpha,
        gradient_threshold=args.gradient_threshold,
        gradient_severe_threshold=args.gradient_severe_threshold,
        startup_gradient_threshold=args.startup_gradient_threshold,
        gradient_required_intervals=args.gradient_required_intervals,
        startup_gradient_required_intervals=args.startup_gradient_required_intervals,
        gradient_queue_floor_mult=args.gradient_queue_floor_mult,
        gradient_backoff_beta=args.gradient_backoff_beta,
        post_backoff_grace_intervals=args.post_backoff_grace_intervals,
        max_retry_attempts=args.max_retry_attempts,
        retry_backoff_sec=args.retry_backoff_sec,
        naming_mode=args.naming_mode,
        file_name_pattern=args.file_name_pattern,
        create_tar=not args.no_tar,
        compress_tar=not args.no_compress_tar,
        create_overview=not args.no_overview,
        force_overwrite=args.force,
    )


class HostMetrics(base.HostMetrics):
    """Per-host metrics with gradient-aware queue-delay tracking."""

    def __init__(self, config: PAARCConfig, host: str = "unknown"):
        super().__init__(config, host)
        self._queue_delay_ema: Optional[float] = None
        self._gradient_ema: Optional[float] = None
        self._intervals_with_gradient_samples = 0
        self._gradient_sample_count_sum = 0
        self._gradient_confidence_sum = 0.0
        self._max_queue_delay_ms = 0.0
        self._max_gradient_ema = 0.0

    @property
    def intervals_with_gradient_samples(self) -> int:
        return self._intervals_with_gradient_samples

    @property
    def max_queue_delay_ms(self) -> float:
        return self._max_queue_delay_ms

    @property
    def max_gradient_ema(self) -> float:
        return self._max_gradient_ema

    @property
    def avg_gradient_sample_count(self) -> float:
        if self._intervals_with_gradient_samples == 0:
            return 0.0
        return self._gradient_sample_count_sum / self._intervals_with_gradient_samples

    @property
    def avg_gradient_confidence(self) -> float:
        if self._intervals_with_gradient_samples == 0:
            return 0.0
        return self._gradient_confidence_sum / self._intervals_with_gradient_samples

    def _update_gradient_ema(self, current: Optional[float], new_value: float) -> float:
        alpha = self.config.gradient_alpha
        if current is None:
            return new_value
        return alpha * new_value + (1 - alpha) * current

    def reset_gradient_state(self) -> None:
        self._queue_delay_ema = None
        self._gradient_ema = None

    async def finish_interval(
        self,
        allow_rtprop_update: bool = True,
        gradient_tracking_enabled: bool = True,
    ) -> dict[str, Any]:
        snap = await super().finish_interval(allow_rtprop_update=allow_rtprop_update)

        p50_raw = snap.get("p50_raw")
        rtprop = snap.get("rtprop")
        duration = max(float(snap.get("duration", 0.0) or 0.0), 1e-6)
        n_samples = int(snap.get("n_samples", 0) or 0)

        queue_delay_raw: Optional[float] = None
        gradient_raw: Optional[float] = None
        has_gradient_sample = False
        gradient_sample_count = 0
        gradient_confidence = 0.0

        if gradient_tracking_enabled and p50_raw is not None and rtprop is not None:
            has_gradient_sample = True
            queue_delay_raw = max(0.0, p50_raw - rtprop)
            prev_queue_delay_ema = self._queue_delay_ema
            self._queue_delay_ema = self._update_gradient_ema(self._queue_delay_ema, queue_delay_raw)

            if prev_queue_delay_ema is None:
                gradient_raw = 0.0
            else:
                gradient_raw = (
                    (self._queue_delay_ema - prev_queue_delay_ema) / duration
                ) / max(rtprop, 1e-6)

            self._gradient_ema = self._update_gradient_ema(self._gradient_ema, gradient_raw)
            gradient_sample_count = n_samples
            gradient_confidence = min(1.0, n_samples / max(self.config.N_min, 1))
            self._intervals_with_gradient_samples += 1
            self._gradient_sample_count_sum += gradient_sample_count
            self._gradient_confidence_sum += gradient_confidence
            self._max_queue_delay_ms = max(self._max_queue_delay_ms, self._queue_delay_ema * 1000)
            self._max_gradient_ema = max(self._max_gradient_ema, self._gradient_ema)

        snap.update(
            {
                "queue_delay_raw": queue_delay_raw,
                "queue_delay_ema": self._queue_delay_ema,
                "gradient_raw": gradient_raw,
                "gradient_ema": self._gradient_ema,
                "has_gradient_sample": has_gradient_sample,
                "gradient_interval_sec": duration,
                "gradient_sample_count": gradient_sample_count,
                "gradient_confidence": gradient_confidence,
            }
        )
        return snap


class PAARCController(base.PAARCController):
    """Gradient-aware PAARC controller."""

    def __init__(self, host: str, config: PAARCConfig):
        super().__init__(host, config)
        self.metrics = HostMetrics(config, host)
        self._gradient_overuse_intervals = 0
        self._startup_gradient_overuse_intervals = 0
        self._post_backoff_grace_intervals_remaining = 0
        self.gradient_hold_events = 0
        self.gradient_soft_backoffs = 0
        self.gradient_plateau_events = 0
        self.post_backoff_grace_events = 0

    def _reset_gradient_counters(self) -> None:
        self._gradient_overuse_intervals = 0
        self._startup_gradient_overuse_intervals = 0

    def _reset_gradient_soft_state(self) -> None:
        self.metrics.reset_gradient_state()
        self._reset_gradient_counters()

    def _enter_post_backoff_grace(self) -> None:
        self.metrics.reset_gradient_state()
        self._reset_gradient_counters()
        self._stable_intervals = 0
        self._post_backoff_grace_intervals_remaining = self.config.post_backoff_grace_intervals
        if self._post_backoff_grace_intervals_remaining > 0:
            self.post_backoff_grace_events += 1

    def _soft_backoff_ceiling(self, beta: float) -> int:
        if self._concurrency <= self.config.C_min:
            return self.config.C_min
        scaled = int(self._concurrency * beta)
        return max(self.config.C_min, min(scaled, self._concurrency - 1))

    def _proportional_soft_beta(self, gradient_ema: float) -> float:
        span = max(self.config.gradient_severe_threshold - self.config.gradient_threshold, 1e-6)
        severity = (gradient_ema - self.config.gradient_threshold) / span
        severity = min(1.0, max(0.0, severity))
        return 1.0 - severity * (1.0 - self.config.gradient_backoff_beta)

    async def step_interval(self) -> dict[str, Any]:
        """Execute one control interval step."""
        now = base._monotonic()

        if self.state == base.PAARCState.INIT:
            allow_rtprop_update = True
        elif self.state == base.PAARCState.PROBE_RTT:
            allow_rtprop_update = self._restoring
        else:
            allow_rtprop_update = False

        gradient_tracking_enabled = self.state not in (
            base.PAARCState.PROBE_RTT,
            base.PAARCState.BACKOFF,
        )
        snap = await self.metrics.finish_interval(
            allow_rtprop_update=allow_rtprop_update,
            gradient_tracking_enabled=gradient_tracking_enabled,
        )

        self._samples_since_probe_rtt += snap.get("n_samples", 0)

        if snap["total"] == 0:
            self._last_interval_time = now
            return snap

        rtprop = self.metrics.rtprop
        rtprop_ms = rtprop * 1000 if rtprop else 0
        goodput = snap.get("goodput_rps", 0)
        n_errors = snap.get("n_errors", 0)
        if n_errors > 0:
            print(
                f"[PAARC] {self.host}: {self.state.name} | C={self._concurrency} | RTprop={rtprop_ms:.0f}ms | "
                f"Goodput={goodput:.1f}/s | Errors={n_errors}"
            )

        if self.state == base.PAARCState.INIT:
            await self._step_init(snap, now)
        elif self.state == base.PAARCState.STARTUP:
            await self._step_startup(snap, now)
        elif self.state == base.PAARCState.PROBE_BW:
            await self._step_probe_bw(snap, now)
        elif self.state == base.PAARCState.PROBE_RTT:
            await self._step_probe_rtt(snap, now)
        elif self.state == base.PAARCState.BACKOFF:
            await self._step_backoff(snap, now)

        self._last_interval_time = now
        return snap

    def _gradient_queue_floor_met(self, snap: dict, rtprop: float) -> bool:
        queue_delay_ema = snap.get("queue_delay_ema")
        if queue_delay_ema is None:
            return False
        return queue_delay_ema >= self.config.gradient_queue_floor_mult * rtprop

    def _is_gradient_plateau(self, snap: dict) -> bool:
        rtprop = self.metrics.rtprop
        if rtprop is None or not snap.get("has_gradient_sample"):
            self._startup_gradient_overuse_intervals = 0
            return False

        gradient_ema = snap.get("gradient_ema")
        if gradient_ema is None:
            self._startup_gradient_overuse_intervals = 0
            return False

        triggered = (
            self._gradient_queue_floor_met(snap, rtprop)
            and gradient_ema > self.config.startup_gradient_threshold
        )
        if triggered:
            self._startup_gradient_overuse_intervals += 1
        else:
            self._startup_gradient_overuse_intervals = 0

        if self._startup_gradient_overuse_intervals < self.config.startup_gradient_required_intervals:
            return False

        queue_delay_ema = snap.get("queue_delay_ema")
        print(
            f"[PAARC-GRAD] {self.host}: STARTUP gradient plateau | "
            f"rtprop={rtprop:.3f}s | queue={queue_delay_ema:.3f}s | gradient={gradient_ema:.4f}"
        )
        return True

    def _update_gradient_overuse_counter(self, snap: dict) -> bool:
        rtprop = self.metrics.rtprop
        if rtprop is None or not snap.get("has_gradient_sample"):
            self._gradient_overuse_intervals = 0
            return False

        gradient_ema = snap.get("gradient_ema")
        if gradient_ema is None:
            self._gradient_overuse_intervals = 0
            return False

        triggered = self._gradient_queue_floor_met(snap, rtprop) and gradient_ema > 0
        if triggered:
            self._gradient_overuse_intervals += 1
        else:
            self._gradient_overuse_intervals = 0

        return self._gradient_overuse_intervals >= self.config.gradient_required_intervals

    async def _step_startup(self, snap: dict, now: float) -> None:
        if snap.get("has_overload"):
            self._reset_gradient_soft_state()
            self._C_ceiling = max(self.config.C_min, int(self._concurrency * self.config.beta))
            self._C_operating = int(self._C_ceiling * self.config.mu)
            self._set_concurrency(self._C_operating, "startup_overload")

            cooldown = self._calculate_cooldown(snap.get("retry_after"))
            self._cooldown_until = now + cooldown
            self._last_overload_reduction_time = now
            self.state = base.PAARCState.BACKOFF

            print(f"[PAARC-GRAD] {self.host}: STARTUP→BACKOFF | C_ceiling={self._C_ceiling}")
            return

        if self._is_gradient_plateau(snap):
            self._startup_gradient_overuse_intervals = 0
            self.gradient_plateau_events += 1
            self._C_ceiling = self._concurrency
            self._C_operating = int(self._C_ceiling * self.config.mu)
            self._set_concurrency(self._C_operating, "startup_gradient_plateau")

            self._last_probe_rtt_time = now
            self._samples_since_probe_rtt = 0
            self.state = base.PAARCState.PROBE_BW

            print(
                f"[PAARC-GRAD] {self.host}: STARTUP→PROBE_BW (gradient plateau) | "
                f"C_ceiling={self._C_ceiling}"
            )
            return

        if self._concurrency >= self.config.C_max:
            self._startup_gradient_overuse_intervals = 0
            self._C_ceiling = self.config.C_max
            self._C_operating = int(self._C_ceiling * self.config.mu)
            self._set_concurrency(self._C_operating, "startup_max")

            self._last_probe_rtt_time = now
            self._samples_since_probe_rtt = 0
            self.state = base.PAARCState.PROBE_BW

            print(f"[PAARC-GRAD] {self.host}: STARTUP→PROBE_BW (max) | C_ceiling={self._C_ceiling}")
            return

        new_C = min(self._concurrency + self.config.startup_additive_increase, self.config.C_max)
        self._set_concurrency(new_C, "startup_grow")

    async def _step_probe_bw(self, snap: dict, now: float) -> None:
        if snap.get("has_overload"):
            self._reset_gradient_soft_state()
            if self._C_ceiling is not None:
                self._C_ceiling = max(self.config.C_min, int(self._C_ceiling * self.config.beta))
                self._C_operating = int(self._C_ceiling * self.config.mu)
            else:
                self._C_operating = max(self.config.C_min, int(self._concurrency * self.config.beta))

            self._set_concurrency(self._C_operating, "probe_bw_overload")

            cooldown = self._calculate_cooldown(snap.get("retry_after"))
            self._cooldown_until = now + cooldown
            self._last_overload_reduction_time = now
            self.state = base.PAARCState.BACKOFF

            print(f"[PAARC-GRAD] {self.host}: PROBE_BW→BACKOFF | C_ceiling={self._C_ceiling}")
            return

        if self._post_backoff_grace_intervals_remaining > 0:
            self._stable_intervals = 0
            self._post_backoff_grace_intervals_remaining -= 1
            print(
                f"[PAARC-GRAD] {self.host}: PROBE_BW grace | "
                f"remaining={self._post_backoff_grace_intervals_remaining}"
            )
            return

        time_since_probe = now - self._last_probe_rtt_time
        if (
            time_since_probe > self.config.probe_rtt_period
            and self._samples_since_probe_rtt > self.config.probe_rtt_min_samples
        ):
            self._saved_concurrency = self._concurrency
            probe_C = max(
                self.config.C_min,
                int(self._concurrency * self.config.probe_rtt_concurrency_factor),
            )
            self._set_concurrency(probe_C, "probe_rtt_enter")

            self.state = base.PAARCState.PROBE_RTT
            print(f"[PAARC-GRAD] {self.host}: PROBE_BW→PROBE_RTT | C={probe_C}")
            return

        rtprop = self.metrics.rtprop
        gradient_ema = snap.get("gradient_ema")
        queue_delay_ema = snap.get("queue_delay_ema")
        has_gradient_sample = bool(snap.get("has_gradient_sample"))
        floor_met = (
            rtprop is not None
            and has_gradient_sample
            and self._gradient_queue_floor_met(snap, rtprop)
        )
        overuse_ready = self._update_gradient_overuse_counter(snap)

        if floor_met and gradient_ema is not None:
            if gradient_ema <= 0:
                if self._C_ceiling is not None and self._concurrency > self._C_ceiling:
                    self._C_ceiling = self._concurrency
                    self._C_operating = int(self._C_ceiling * self.config.mu)

                self._check_ceiling_revision(snap)
                self._stable_intervals += 1
                if self._stable_intervals >= self.config.stable_intervals_required:
                    cautious_step = max(1, self.config.probe_bw_additive_increase // 2)
                    if self._concurrency < self.config.C_max:
                        new_C = min(self._concurrency + cautious_step, self.config.C_max)
                        self._set_concurrency(new_C, "probe_bw_cautious_increase")
                    self._stable_intervals = 0
                return

            if not overuse_ready:
                self._stable_intervals = 0
                return

            self._stable_intervals = 0
            if gradient_ema < self.config.gradient_threshold:
                self.gradient_hold_events += 1
                print(
                    f"[PAARC-GRAD] {self.host}: PROBE_BW hold | "
                    f"queue={queue_delay_ema:.3f}s | gradient={gradient_ema:.4f}"
                )
                return

            if gradient_ema < self.config.gradient_severe_threshold:
                beta = self._proportional_soft_beta(gradient_ema)
                new_ceiling = self._soft_backoff_ceiling(beta)
                self._C_ceiling = new_ceiling
                self._C_operating = int(self._C_ceiling * self.config.mu)
                self._set_concurrency(self._C_operating, "gradient_proportional_soft_backoff")
                self.gradient_soft_backoffs += 1
                self._gradient_overuse_intervals = 0
                print(
                    f"[PAARC-GRAD] {self.host}: PROBE_BW proportional soft backoff | "
                    f"queue={queue_delay_ema:.3f}s | gradient={gradient_ema:.4f} | beta={beta:.3f} | "
                    f"C_ceiling={self._C_ceiling}"
                )
                return

            new_ceiling = self._soft_backoff_ceiling(self.config.gradient_backoff_beta)
            self._C_ceiling = new_ceiling
            self._C_operating = int(self._C_ceiling * self.config.mu)
            self._set_concurrency(self._C_operating, "gradient_soft_backoff")
            self.gradient_soft_backoffs += 1
            self._gradient_overuse_intervals = 0
            print(
                f"[PAARC-GRAD] {self.host}: PROBE_BW soft backoff | "
                f"queue={queue_delay_ema:.3f}s | gradient={gradient_ema:.4f} | C_ceiling={self._C_ceiling}"
            )
            return

        if self._C_ceiling is not None and self._concurrency > self._C_ceiling:
            self._C_ceiling = self._concurrency
            self._C_operating = int(self._C_ceiling * self.config.mu)

        self._check_ceiling_revision(snap)

        self._stable_intervals += 1
        if self._stable_intervals >= self.config.stable_intervals_required:
            if self._concurrency < self.config.C_max:
                new_C = min(
                    self._concurrency + self.config.probe_bw_additive_increase,
                    self.config.C_max,
                )
                self._set_concurrency(new_C, "probe_bw_increase")
                self._stable_intervals = 0

    async def _step_probe_rtt(self, snap: dict, now: float) -> None:
        if snap.get("has_overload"):
            self._reset_gradient_soft_state()
            if self._C_ceiling is not None:
                self._C_ceiling = max(
                    self.config.C_min,
                    int(self._C_ceiling * self.config.beta)
                )
                self._C_operating = int(self._C_ceiling * self.config.mu)

            cooldown = self._calculate_cooldown(snap.get("retry_after"))
            self._cooldown_until = now + cooldown
            self._last_overload_reduction_time = now
            self.state = base.PAARCState.BACKOFF

            print(f"[PAARC-GRAD] {self.host}: PROBE_RTT→BACKOFF")
            return

        await super()._step_probe_rtt(snap, now)

    async def _step_backoff(self, snap: dict, now: float) -> None:
        rtprop = self._get_rtprop()
        overload_check_cooldown = max(
            self.config.overload_check_floor,
            rtprop * self.config.overload_check_rtprop_mult
        )
        time_since_last_reduction = now - self._last_overload_reduction_time

        if snap.get("has_overload") and time_since_last_reduction >= overload_check_cooldown:
            new_C = max(
                self.config.C_min,
                int(self._concurrency * self.config.beta)
            )
            self._set_concurrency(new_C, "backoff_continued")

            if self._C_ceiling is not None:
                self._C_ceiling = max(
                    self.config.C_min,
                    int(self._C_ceiling * self.config.beta)
                )

            cooldown = self._calculate_cooldown(snap.get("retry_after"))
            self._cooldown_until = now + cooldown
            self._last_overload_reduction_time = now

            print(
                f"[PAARC] {self.host}: BACKOFF continued | C={new_C} | "
                f"overload_cooldown={overload_check_cooldown:.1f}s"
            )
            return

        if now >= self._cooldown_until:
            if self._C_ceiling is not None:
                self._C_operating = int(self._C_ceiling * self.config.mu)
            self._set_concurrency(self._C_operating, "backoff_recover")

            self._last_probe_rtt_time = now
            self._samples_since_probe_rtt = 0
            self._enter_post_backoff_grace()
            self.state = base.PAARCState.PROBE_BW

            print(
                f"[PAARC] {self.host}: BACKOFF→PROBE_BW | C={self._C_operating} | "
                f"grace={self._post_backoff_grace_intervals_remaining}"
            )
            return


class HostControllerManager(base.HostControllerManager):
    """Manager that creates gradient-aware PAARC controllers."""

    def __init__(self, config: PAARCConfig):
        self.config = config
        self._lock = asyncio.Lock()
        self._controllers: dict[str, PAARCController] = {}

    async def get_controller(self, url: str) -> PAARCController:
        host = urlsplit(url).netloc.lower() or "unknown"

        async with self._lock:
            if host not in self._controllers:
                self._controllers[host] = PAARCController(host, self.config)
                print(f"[PAARC-GRAD] Created controller for {host}")
            return self._controllers[host]

    async def all_controllers(self) -> list[PAARCController]:
        async with self._lock:
            return list(self._controllers.values())


def collect_gradient_summary(controllers: list[PAARCController]) -> dict[str, Any]:
    if not controllers:
        return {
            "gradient_hold_events": 0,
            "gradient_soft_backoffs": 0,
            "gradient_plateau_events": 0,
            "intervals_with_gradient_samples": 0,
            "max_queue_delay_ms": 0.0,
            "max_gradient_ema": 0.0,
            "avg_gradient_sample_count": 0.0,
            "avg_gradient_confidence": 0.0,
            "post_backoff_grace_events": 0,
            "controller_variant": "gradient",
        }

    total_gradient_intervals = sum(
        ctrl.metrics.intervals_with_gradient_samples for ctrl in controllers
    )
    total_gradient_sample_count = sum(
        ctrl.metrics.avg_gradient_sample_count * ctrl.metrics.intervals_with_gradient_samples
        for ctrl in controllers
    )
    total_gradient_confidence = sum(
        ctrl.metrics.avg_gradient_confidence * ctrl.metrics.intervals_with_gradient_samples
        for ctrl in controllers
    )

    return {
        "gradient_hold_events": sum(ctrl.gradient_hold_events for ctrl in controllers),
        "gradient_soft_backoffs": sum(ctrl.gradient_soft_backoffs for ctrl in controllers),
        "gradient_plateau_events": sum(ctrl.gradient_plateau_events for ctrl in controllers),
        "intervals_with_gradient_samples": total_gradient_intervals,
        "max_queue_delay_ms": round(
            max(ctrl.metrics.max_queue_delay_ms for ctrl in controllers),
            3,
        ),
        "max_gradient_ema": round(
            max(ctrl.metrics.max_gradient_ema for ctrl in controllers),
            6,
        ),
        "avg_gradient_sample_count": round(
            (total_gradient_sample_count / total_gradient_intervals) if total_gradient_intervals else 0.0,
            3,
        ),
        "avg_gradient_confidence": round(
            (total_gradient_confidence / total_gradient_intervals) if total_gradient_intervals else 0.0,
            6,
        ),
        "post_backoff_grace_events": sum(ctrl.post_backoff_grace_events for ctrl in controllers),
        "controller_variant": "gradient",
    }


def generate_overview_report(
    *,
    cfg: Config,
    df_total: int,
    outcomes: dict[str, base.DownloadOutcome],
    elapsed_sec: float,
    gradient_summary: dict[str, Any],
    tar_path: Optional[str] = None,
) -> dict[str, Any]:
    """Extend the shared overview schema with gradient settings and counters."""
    report = base.generate_overview_report(
        cfg=cfg, df_total=df_total, outcomes=outcomes,
        elapsed_sec=elapsed_sec, tar_path=tar_path,
    )
    report["paarc_version"] = "2.0.0-gradient"
    report["controller_variant"] = "gradient"
    report["gradient_summary"] = gradient_summary
    for key in (
        "gradient_alpha", "gradient_threshold", "gradient_severe_threshold",
        "startup_gradient_threshold", "gradient_required_intervals",
        "startup_gradient_required_intervals", "gradient_queue_floor_mult",
        "gradient_backoff_beta", "post_backoff_grace_intervals",
    ):
        report["script_inputs"]["paarc_config"][key] = getattr(cfg, key)
    return report


def write_overview(
    *,
    cfg: Config,
    df_total: int,
    outcomes: dict[str, base.DownloadOutcome],
    elapsed_sec: float,
    tar_path: Optional[str],
    gradient_summary: dict[str, Any],
    report: Optional[dict[str, Any]] = None,
) -> str:
    """Write an external overview, retaining the experimental report fields."""
    if report is None:
        report = generate_overview_report(
            cfg=cfg, df_total=df_total, outcomes=outcomes,
            elapsed_sec=elapsed_sec, tar_path=tar_path,
            gradient_summary=gradient_summary,
        )
    return base.write_overview(
        cfg=cfg, df_total=df_total, outcomes=outcomes,
        elapsed_sec=elapsed_sec, tar_path=tar_path, report=report,
    )


async def main() -> None:
    base._setup_signal_handlers()
    cfg = parse_args()

    print("=" * 72)
    print("FLOW-DC Batch Downloader with Gradient PAARC")
    print("=" * 72)

    df = base.validate_and_load(cfg)
    print(f"[Load] URLs after filtering: {df.height}")
    effective_workers = base.resolve_worker_count(cfg, df)

    manager: Optional[HostControllerManager] = None
    ctrl_task: Optional[asyncio.Task] = None

    if cfg.enable_paarc:
        paarc_config = cfg.to_paarc_config()
        manager = HostControllerManager(paarc_config)
        ctrl_task = asyncio.create_task(base.controller_loop(manager))
        print(
            f"[PAARC-GRAD] Enabled | C_init={paarc_config.C_init} | "
            f"μ={paarc_config.mu} | gradient_threshold={paarc_config.gradient_threshold}"
        )
    else:
        print("[PAARC-GRAD] Disabled - using fixed concurrency")

    connector = aiohttp.TCPConnector(
        limit=max(50, int(effective_workers * 1.1)),
        ttl_dns_cache=300,
        use_dns_cache=True,
    )
    trace_config = base.build_trace_config()

    sequential_namer = base.SequentialNamer()
    global_written_paths: dict[str, str] = {}
    final_outcomes: dict[str, base.DownloadOutcome] = {}

    start = base._monotonic()

    try:
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=max(1, cfg.timeout_sec * 2)),
            headers={"User-Agent": "FLOW-DC/2.0 PAARC-GRADIENT/2.0"},
            trace_configs=[trace_config],
        ) as session:
            current_df = df.clone()
            attempt = 1

            while attempt <= cfg.max_retry_attempts and current_df.height > 0 and not base.shutdown_flag:
                print(f"\n[Attempt {attempt}] Processing {current_df.height} URLs...")

                outcomes = await base.download_batch_bounded(
                    cfg=cfg,
                    session=session,
                    df=current_df,
                    manager=manager,
                    sequential_namer=sequential_namer,
                    global_written_paths=global_written_paths,
                    effective_workers=effective_workers,
                )

                final_outcomes.update(outcomes)

                retry_keys = []
                for row in current_df.iter_rows(named=True):
                    key = str(row["__key__"])
                    out = outcomes.get(key)
                    if out is None:
                        continue
                    if out.success:
                        continue
                    if base._is_retryable(out.status_code, out.error):
                        retry_keys.append(key)

                succ = sum(1 for o in outcomes.values() if o.success)
                fail = sum(1 for o in outcomes.values() if not o.success)
                retryable = len(retry_keys)
                print(f"[Attempt {attempt}] Success={succ} Failed={fail} Retryable={retryable}")

                if retry_keys and attempt < cfg.max_retry_attempts and not base.shutdown_flag:
                    await asyncio.sleep(cfg.retry_backoff_sec)
                    current_df = current_df.filter(pl.col("__key__").is_in(retry_keys))
                    attempt += 1
                else:
                    break

    finally:
        if ctrl_task is not None:
            ctrl_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await ctrl_task

    elapsed = base._monotonic() - start
    controllers = await manager.all_controllers() if manager is not None else []
    gradient_summary = collect_gradient_summary(controllers)

    successes = [o for o in final_outcomes.values() if o.success]
    failures = [o for o in final_outcomes.values() if not o.success]

    download_elapsed = elapsed

    print("\n" + "=" * 72)
    print("FINAL SUMMARY")
    print("=" * 72)
    print(f"Total URLs:            {df.height}")
    print(f"Successful downloads:  {len(successes)}")
    print(f"Failed downloads:      {len(failures)}")
    print(f"Download time:         {download_elapsed:.2f}s")
    if df.height > 0:
        print(f"Success rate:          {(len(successes) / df.height) * 100:.2f}%")

    total_mb = sum(o.bytes_downloaded for o in successes) / 1e6
    print(f"Total downloaded:      {total_mb:.2f} MB")
    if download_elapsed > 0:
        print(f"Average speed:         {total_mb / download_elapsed:.2f} MB/s")
    print(f"Gradient holds:        {gradient_summary['gradient_hold_events']}")
    print(f"Soft backoffs:         {gradient_summary['gradient_soft_backoffs']}")
    print(f"Gradient plateaus:     {gradient_summary['gradient_plateau_events']}")
    print(f"Grace windows:         {gradient_summary['post_backoff_grace_events']}")
    print(f"Avg grad samples:      {gradient_summary['avg_gradient_sample_count']:.2f}")
    print(f"Avg grad confidence:   {gradient_summary['avg_gradient_confidence']:.3f}")

    overview_report = None
    if cfg.create_overview:
        overview_report = generate_overview_report(
            cfg=cfg, df_total=df.height, outcomes=final_outcomes,
            elapsed_sec=elapsed, gradient_summary=gradient_summary,
        )
        internal_overview = Path(cfg.output_folder) / "overview.json"
        internal_overview.write_text(json.dumps(overview_report, indent=2))
        print(f"[Report] Internal overview written: {internal_overview}")

    tar_path = None
    tar_elapsed = 0.0
    if cfg.create_tar and not base.shutdown_flag and successes:
        try:
            tar_start = base._monotonic()
            tar_path = base.create_tar(cfg.output_folder, compress=cfg.compress_tar)
            tar_elapsed = base._monotonic() - tar_start
            tar_size_mb = Path(tar_path).stat().st_size / 1e6
            compress_str = "compressed" if cfg.compress_tar else "uncompressed"
            print(f"[Tar] Created ({compress_str}): {tar_path}")
            print(f"[Tar] Size: {tar_size_mb:.2f} MB, Time: {tar_elapsed:.2f}s")
        except Exception as e:
            print(f"[Tar] Failed: {e}")

    if cfg.create_overview:
        try:
            overview = write_overview(
                cfg=cfg,
                df_total=df.height,
                outcomes=final_outcomes,
                elapsed_sec=elapsed,
                tar_path=tar_path,
                gradient_summary=gradient_summary,
                report=overview_report,
            )
            print(f"[Report] Overview: {overview}")
        except Exception as e:
            print(f"[Report] Failed: {e}")

    total_elapsed = download_elapsed + tar_elapsed
    print("\n" + "-" * 72)
    print("TIMING BREAKDOWN")
    print("-" * 72)
    if total_elapsed > 0:
        print(f"Download time:         {download_elapsed:.2f}s ({download_elapsed / total_elapsed * 100:.1f}%)")
        print(f"Tar creation time:     {tar_elapsed:.2f}s ({tar_elapsed / total_elapsed * 100:.1f}%)")
    else:
        print(f"Download time:         {download_elapsed:.2f}s")
        print(f"Tar creation time:     {tar_elapsed:.2f}s")
    print(f"Total time:            {total_elapsed:.2f}s")
    print("=" * 72)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
