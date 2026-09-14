#!/usr/bin/env python3
"""FLOW-DC Gradient research campaign runner.

This script builds fixed manifests, runs FLOW-DC Gradient / PAARC / img2dataset
download experiments, cleans payloads, and preserves paper-grade artifacts.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import json
import math
import os
import platform
import random
import re
import shutil
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib.parse import urlsplit

import aiohttp
import polars as pl
import psutil
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MANIFEST_DIR = PROJECT_ROOT / "benchmark" / "manifests" / "flowdc_gradient_20260429"
RESULTS_DIR = PROJECT_ROOT / "benchmark" / "results"
SEED = 20260429

SERVER_FAULT_CODES = {429, 500, 502, 503, 504}
LINK_ROT_CODES = {404, 410}
ACCESS_POLICY_CODES = {401, 403, 451}
CONNECTION_ERROR_TOKENS = (
    "timeout",
    "timed out",
    "connection",
    "clientconnector",
    "server disconnected",
    "reset",
    "too many redirects",
)


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    full_rows: int
    pilot_rows: int
    url_column: str = "url"
    label_column: Optional[str] = "label"
    low_concurrency: int = 256
    high_concurrency: int = 1024
    source: str = ""


@dataclass(frozen=True)
class ToolSpec:
    name: str
    variant: str


@dataclass
class RunSpec:
    dataset: DatasetSpec
    manifest_path: Path
    tool: ToolSpec
    concurrency: int
    run_number: int
    phase: str
    wall_clock_limit_sec: int
    timeout_sec: int = 30
    retries: int = 1

    @property
    def run_id(self) -> str:
        return (
            f"{self.phase}_{self.dataset.name}_{self.tool.name}_{self.tool.variant}"
            f"_c{self.concurrency}_r{self.run_number}"
        )


@dataclass
class CampaignConfig:
    output_dir: Path
    run_name: str
    full_campaign_limit_sec: int = 8 * 60 * 60
    measured_runs: int = 2
    pilot_wall_clock_limit_sec: int = 8 * 60
    full_wall_clock_limit_sec: int = 12 * 60
    min_free_gib: float = 100.0
    max_payload_gib: float = 25.0
    resource_sample_interval: float = 0.5
    show_progress: bool = False


DATASETS = [
    DatasetSpec(
        name="inat_s3",
        full_rows=10_000,
        pilot_rows=500,
        label_column="scientificName",
        low_concurrency=256,
        high_concurrency=1024,
        source="iNaturalist Open Data S3 sampled from local FLOW-DC manifest",
    ),
    DatasetSpec(
        name="gbif_multimedia",
        full_rows=500,
        pilot_rows=500,
        label_column="species",
        low_concurrency=256,
        high_concurrency=1024,
        source="GBIF occurrence search API, direct multimedia identifier URLs",
    ),
    DatasetSpec(
        name="openverse_mixed",
        full_rows=3_000,
        pilot_rows=500,
        label_column="title",
        low_concurrency=128,
        high_concurrency=512,
        source="Openverse image search API, original provider URLs",
    ),
]

GRADIENT = ToolSpec("flowdc", "gradient")
PAARC = ToolSpec("flowdc", "paarc_p50_p95")
IMG2DATASET = ToolSpec("img2dataset", "default")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def text_sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def get_host(url: str) -> str:
    return urlsplit(str(url)).netloc.lower()


def free_gib(path: Path) -> float:
    usage = shutil.disk_usage(path)
    return usage.free / (1024**3)


def dir_size_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for item in path.rglob("*"):
        try:
            if item.is_file() and not item.is_symlink():
                total += item.stat().st_size
        except OSError:
            continue
    return total


def safe_rmtree(path: Path) -> None:
    if not path.exists():
        return
    resolved = path.resolve()
    allowed_roots = [
        (PROJECT_ROOT / "benchmark" / "results").resolve(),
        (PROJECT_ROOT / "files" / "output").resolve(),
    ]
    if not any(str(resolved).startswith(str(root)) for root in allowed_roots):
        raise RuntimeError(f"Refusing to remove unsafe path: {path}")
    shutil.rmtree(resolved)


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        json.dump(data, f, indent=2, sort_keys=True)


def read_json(path: Path) -> dict[str, Any]:
    with path.open() as f:
        return json.load(f)


def normalize_url_frame(
    df: pl.DataFrame,
    *,
    url_column: str,
    label_column: Optional[str],
    source: str,
    target_rows: int,
) -> pl.DataFrame:
    if url_column not in df.columns:
        raise ValueError(f"Missing URL column {url_column!r}; available columns: {df.columns}")

    keep_cols = [url_column]
    if label_column and label_column in df.columns:
        keep_cols.append(label_column)

    extra_cols = [
        c
        for c in (
            "license",
            "license_url",
            "provider",
            "source",
            "foreign_landing_url",
            "gbifID",
            "datasetKey",
            "rightsHolder",
            "creator",
            "title",
        )
        if c in df.columns and c not in keep_cols
    ]

    out = df.select(keep_cols + extra_cols)
    out = out.rename({url_column: "url"})
    if label_column and label_column in out.columns:
        out = out.rename({label_column: "label"})
    elif "label" not in out.columns:
        out = out.with_columns(pl.lit(None, dtype=pl.Utf8).alias("label"))

    out = (
        out.with_columns(pl.col("url").cast(pl.Utf8).str.strip_chars())
        .filter(pl.col("url").is_not_null() & (pl.col("url").str.len_chars() > 0))
        .unique(subset=["url"], keep="first")
        .with_columns(
            pl.col("url").map_elements(get_host, return_dtype=pl.Utf8).alias("host"),
            pl.lit(source).alias("manifest_source"),
        )
    )

    if len(out) < target_rows:
        raise RuntimeError(f"Only {len(out)} usable URLs for {source}; need {target_rows}")

    return out.sample(n=target_rows, shuffle=True, seed=SEED)


def manifest_metadata(path: Path, df: pl.DataFrame, spec: DatasetSpec, source_notes: dict[str, Any]) -> dict[str, Any]:
    host_counts = (
        df.group_by("host")
        .len()
        .sort("len", descending=True)
        .head(50)
        .rename({"len": "count"})
    )
    host_rows = host_counts.to_dicts()
    payload = {
        "created_utc": utc_now(),
        "dataset": spec.name,
        "manifest_path": rel(path),
        "rows": len(df),
        "url_column": "url",
        "label_column": "label",
        "unique_hosts": df.select(pl.col("host").n_unique()).item(),
        "top_hosts": host_rows,
        "sha256": file_sha256(path),
        "source": spec.source,
        "source_notes": source_notes,
    }
    return payload


def write_manifest_pair(df: pl.DataFrame, spec: DatasetSpec, source_notes: dict[str, Any]) -> dict[str, Any]:
    MANIFEST_DIR.mkdir(parents=True, exist_ok=True)
    full_path = MANIFEST_DIR / f"{spec.name}_{spec.full_rows}.parquet"
    pilot_path = MANIFEST_DIR / f"{spec.name}_{spec.pilot_rows}_pilot.parquet"
    meta_path = MANIFEST_DIR / f"{spec.name}_{spec.full_rows}.metadata.json"
    pilot_meta_path = MANIFEST_DIR / f"{spec.name}_{spec.pilot_rows}_pilot.metadata.json"

    df.write_parquet(full_path)
    pilot_df = df.head(spec.pilot_rows)
    pilot_df.write_parquet(pilot_path)

    full_meta = manifest_metadata(full_path, df, spec, source_notes)
    pilot_meta = manifest_metadata(pilot_path, pilot_df, spec, {**source_notes, "sample": "first rows from fixed full manifest"})
    write_json(meta_path, full_meta)
    write_json(pilot_meta_path, pilot_meta)
    return {"full": full_meta, "pilot": pilot_meta}


def build_inat_manifest(spec: DatasetSpec) -> dict[str, Any]:
    source_path = PROJECT_ROOT / "files" / "input" / "inat_filtered_group_2.parquet"
    if not source_path.exists():
        raise FileNotFoundError(f"Missing local iNaturalist source manifest: {source_path}")
    df = pl.read_parquet(source_path)
    df = df.with_columns(
        pl.col("url")
        .cast(pl.Utf8)
        .str.replace("http://inaturalist-open-data.s3.amazonaws.com", "https://inaturalist-open-data.s3.amazonaws.com")
        .alias("url")
    )
    out = normalize_url_frame(
        df,
        url_column="url",
        label_column="scientificName",
        source="inat_local_manifest_s3",
        target_rows=spec.full_rows,
    )
    return write_manifest_pair(
        out,
        spec,
        {
            "local_source_path": rel(source_path),
            "local_source_sha256": file_sha256(source_path),
            "official_docs": "https://github.com/inaturalist/inaturalist-open-data",
            "official_note": "Current full iNaturalist metadata object is ~29GB; local FLOW-DC manifest is used as the safe fixed sample.",
        },
    )


async def fetch_json(session: aiohttp.ClientSession, url: str, *, retries: int = 4, sleep_sec: float = 0.5) -> dict[str, Any]:
    last_error: Optional[str] = None
    for attempt in range(retries):
        try:
            async with session.get(url) as resp:
                if resp.status == 429:
                    await asyncio.sleep(sleep_sec * (attempt + 2))
                    continue
                resp.raise_for_status()
                return await resp.json()
        except Exception as exc:  # pragma: no cover - network branch
            last_error = repr(exc)
            await asyncio.sleep(sleep_sec * (attempt + 1))
    raise RuntimeError(f"Failed to fetch {url}: {last_error}")


def extract_gbif_media(result: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    extensions = result.get("extensions") or {}
    multimedia = extensions.get("http://rs.gbif.org/terms/1.0/Multimedia") or []
    for media in multimedia:
        url = media.get("http://purl.org/dc/terms/identifier")
        media_type = media.get("http://purl.org/dc/terms/type")
        fmt = str(media.get("http://purl.org/dc/terms/format") or "").lower()
        if not url or not str(url).startswith(("http://", "https://")):
            continue
        if media_type and "still" not in str(media_type).lower():
            continue
        if fmt and not any(token in fmt for token in ("image", "jpg", "jpeg", "png", "webp")):
            continue
        rows.append(
            {
                "url": str(url),
                "species": result.get("species") or result.get("scientificName"),
                "gbifID": str(result.get("key") or ""),
                "datasetKey": result.get("datasetKey"),
                "license": media.get("http://purl.org/dc/terms/license") or result.get("license"),
                "rightsHolder": media.get("http://purl.org/dc/terms/rightsHolder"),
                "title": media.get("http://purl.org/dc/terms/title"),
                "source": media.get("http://purl.org/dc/terms/source"),
            }
        )
    return rows


async def build_gbif_manifest_async(spec: DatasetSpec) -> dict[str, Any]:
    larger_manifest = MANIFEST_DIR / "gbif_multimedia_10000.parquet"
    if larger_manifest.exists():
        larger_df = pl.read_parquet(larger_manifest)
        if len(larger_df) >= spec.full_rows:
            out = larger_df.sample(n=spec.full_rows, shuffle=True, seed=SEED)
            return write_manifest_pair(
                out,
                spec,
                {
                    "sampled_from": rel(larger_manifest),
                    "sampled_from_sha256": file_sha256(larger_manifest),
                    "official_docs": "https://techdocs.gbif.org/en/data-use/download-formats",
                    "calibration_note": "Downsampled after pilot projection to keep each measured run under the 12 minute cap.",
                },
            )

    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    dataset_keys: list[str] = []
    headers = {"User-Agent": "FLOW-DC-gradient-benchmark/2026 (+local research benchmark)"}
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
        facet_url = (
            "https://api.gbif.org/v1/occurrence/search"
            "?mediaType=StillImage&limit=0&facet=datasetKey&facetLimit=40"
        )
        facet = await fetch_json(session, facet_url)
        for facet_block in facet.get("facets", []):
            if facet_block.get("field") == "DATASET_KEY":
                dataset_keys = [c["name"] for c in facet_block.get("counts", []) if c.get("name")]
                break

        if not dataset_keys:
            dataset_keys = [""]

        target_per_dataset = max(80, math.ceil(spec.full_rows / max(1, min(len(dataset_keys), 30))) + 20)
        for dataset_key in dataset_keys[:30]:
            dataset_rows = 0
            offset = 0
            while dataset_rows < target_per_dataset and len(rows) < spec.full_rows * 2 and offset <= 900:
                url = (
                    "https://api.gbif.org/v1/occurrence/search"
                    f"?mediaType=StillImage&limit=100&offset={offset}"
                )
                if dataset_key:
                    url += f"&datasetKey={dataset_key}"
                payload = await fetch_json(session, url)
                for result in payload.get("results", []):
                    for row in extract_gbif_media(result):
                        if row["url"] in seen:
                            continue
                        seen.add(row["url"])
                        rows.append(row)
                        dataset_rows += 1
                        if dataset_rows >= target_per_dataset:
                            break
                    if dataset_rows >= target_per_dataset:
                        break
                if payload.get("endOfRecords"):
                    break
                offset += 100
                await asyncio.sleep(0.05)

        if len(rows) < spec.full_rows:
            offset = 0
            while len(rows) < spec.full_rows and offset <= 5000:
                payload = await fetch_json(
                    session,
                    "https://api.gbif.org/v1/occurrence/search"
                    f"?mediaType=StillImage&limit=100&offset={offset}",
                )
                for result in payload.get("results", []):
                    for row in extract_gbif_media(result):
                        if row["url"] in seen:
                            continue
                        seen.add(row["url"])
                        rows.append(row)
                        if len(rows) >= spec.full_rows:
                            break
                    if len(rows) >= spec.full_rows:
                        break
                if payload.get("endOfRecords"):
                    break
                offset += 100
                await asyncio.sleep(0.05)

    df = pl.DataFrame(rows, infer_schema_length=None)
    out = normalize_url_frame(
        df,
        url_column="url",
        label_column="species",
        source="gbif_api_multimedia",
        target_rows=spec.full_rows,
    )
    return write_manifest_pair(
        out,
        spec,
        {
            "api": "https://api.gbif.org/v1/occurrence/search?mediaType=StillImage",
            "official_docs": "https://techdocs.gbif.org/en/data-use/download-formats",
            "facet_dataset_keys": dataset_keys[:30],
        },
    )


def openverse_queries() -> list[str]:
    return [
        "nature",
        "bird",
        "plant",
        "insect",
        "fungi",
        "flower",
        "tree",
        "mammal",
        "butterfly",
        "museum specimen",
        "landscape",
        "ocean",
        "mountain",
        "geology",
        "space",
        "microscope",
        "architecture",
        "archive",
        "painting",
        "botany",
        "wildlife",
        "river",
        "forest",
        "coral",
    ]


async def build_openverse_manifest_async(spec: DatasetSpec) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    headers = {"User-Agent": "FLOW-DC-gradient-benchmark/2026 (+local research benchmark)"}
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
        for query in openverse_queries():
            page = 1
            while len(rows) < spec.full_rows and page <= 10:
                url = (
                    "https://api.openverse.org/v1/images/"
                    f"?q={query.replace(' ', '+')}&page_size=20&page={page}&mature=false"
                )
                payload = await fetch_json(session, url, sleep_sec=1.0)
                for item in payload.get("results", []):
                    media_url = item.get("url")
                    if not media_url or not str(media_url).startswith(("http://", "https://")):
                        continue
                    if media_url in seen:
                        continue
                    seen.add(media_url)
                    rows.append(
                        {
                            "url": media_url,
                            "title": item.get("title"),
                            "license": item.get("license"),
                            "license_url": item.get("license_url"),
                            "provider": item.get("provider"),
                            "source": item.get("source"),
                            "creator": item.get("creator"),
                            "foreign_landing_url": item.get("foreign_landing_url"),
                        }
                    )
                    if len(rows) >= spec.full_rows:
                        break
                if page >= int(payload.get("page_count") or 0):
                    break
                page += 1
                await asyncio.sleep(0.25)
            if len(rows) >= spec.full_rows:
                break

    df = pl.DataFrame(rows, infer_schema_length=None)
    out = normalize_url_frame(
        df,
        url_column="url",
        label_column="title",
        source="openverse_api_images",
        target_rows=spec.full_rows,
    )
    return write_manifest_pair(
        out,
        spec,
        {
            "api": "https://api.openverse.org/v1/images/",
            "official_docs": "https://docs.openverse.org/api/reference/made_with_ov.html",
            "queries": openverse_queries(),
            "license_caveat": "Openverse metadata is retained for analysis; licenses should be independently verified for reuse.",
        },
    )


async def build_manifests() -> dict[str, Any]:
    MANIFEST_DIR.mkdir(parents=True, exist_ok=True)
    all_meta: dict[str, Any] = {"created_utc": utc_now(), "manifests": {}}
    for spec in DATASETS:
        print(f"[Manifest] Building {spec.name}...")
        full_path = MANIFEST_DIR / f"{spec.name}_{spec.full_rows}.parquet"
        pilot_path = MANIFEST_DIR / f"{spec.name}_{spec.pilot_rows}_pilot.parquet"
        meta_path = MANIFEST_DIR / f"{spec.name}_{spec.full_rows}.metadata.json"
        pilot_meta_path = MANIFEST_DIR / f"{spec.name}_{spec.pilot_rows}_pilot.metadata.json"
        if full_path.exists() and pilot_path.exists() and meta_path.exists() and pilot_meta_path.exists():
            meta = {"full": read_json(meta_path), "pilot": read_json(pilot_meta_path)}
            print(
                f"[Manifest] Reusing {spec.name}: {meta['full']['rows']} rows, "
                f"{meta['full']['unique_hosts']} hosts, sha={meta['full']['sha256'][:12]}"
            )
        elif spec.name == "inat_s3":
            meta = build_inat_manifest(spec)
        elif spec.name == "gbif_multimedia":
            meta = await build_gbif_manifest_async(spec)
        elif spec.name == "openverse_mixed":
            meta = await build_openverse_manifest_async(spec)
        else:
            raise ValueError(f"Unknown dataset {spec.name}")
        all_meta["manifests"][spec.name] = meta
        print(
            f"[Manifest] {spec.name}: {meta['full']['rows']} rows, "
            f"{meta['full']['unique_hosts']} hosts, sha={meta['full']['sha256'][:12]}"
        )
    write_json(MANIFEST_DIR / "manifest_index.json", all_meta)
    return all_meta


def record_preflight(output_dir: Path) -> dict[str, Any]:
    deps = {}
    for module in ("aiohttp", "polars", "pyarrow", "psutil", "yaml", "jinja2", "img2dataset", "matplotlib"):
        try:
            mod = __import__(module)
            deps[module] = getattr(mod, "__version__", "installed")
        except Exception as exc:
            deps[module] = f"missing: {exc}"

    def run_cmd(args: list[str]) -> str:
        try:
            return subprocess.check_output(args, cwd=PROJECT_ROOT, text=True, stderr=subprocess.STDOUT).strip()
        except Exception as exc:
            return f"ERROR: {exc}"

    data = {
        "created_utc": utc_now(),
        "project_root": str(PROJECT_ROOT),
        "python": sys.version,
        "python_executable": sys.executable,
        "platform": platform.platform(),
        "cpu_count": psutil.cpu_count(),
        "memory_gib": round(psutil.virtual_memory().total / (1024**3), 3),
        "free_disk_gib": round(free_gib(PROJECT_ROOT), 3),
        "git_head": run_cmd(["git", "rev-parse", "HEAD"]),
        "git_origin_main": run_cmd(["git", "rev-parse", "origin/main"]),
        "git_status_short": run_cmd(["git", "status", "--short"]),
        "dependency_versions": deps,
        "img2dataset_help_hash": text_sha256(run_cmd([str(PROJECT_ROOT / ".venv" / "bin" / "img2dataset"), "--help"])[:10000]),
    }
    write_json(output_dir / "preflight.json", data)
    return data


class ResourceSampler:
    def __init__(self, pid: int, interval: float):
        self.pid = pid
        self.interval = interval
        self.samples: list[dict[str, Any]] = []
        self._running = True
        self._task: Optional[asyncio.Task] = None
        net = psutil.net_io_counters()
        self.net_start = (net.bytes_sent, net.bytes_recv)

    async def start(self) -> None:
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> dict[str, Any]:
        self._running = False
        if self._task:
            await self._task
        return self.summary()

    async def _loop(self) -> None:
        try:
            proc = psutil.Process(self.pid)
        except psutil.Error:
            return
        while self._running:
            cpu = 0.0
            mem = 0
            try:
                procs = [proc] + proc.children(recursive=True)
                for item in procs:
                    try:
                        cpu += item.cpu_percent(interval=None)
                        mem += item.memory_info().rss
                    except psutil.Error:
                        continue
            except psutil.Error:
                pass
            net = psutil.net_io_counters()
            self.samples.append(
                {
                    "timestamp": time.time(),
                    "cpu_percent": cpu,
                    "memory_mb": mem / (1024 * 1024),
                    "net_bytes_sent": net.bytes_sent,
                    "net_bytes_recv": net.bytes_recv,
                }
            )
            await asyncio.sleep(self.interval)

    def summary(self) -> dict[str, Any]:
        if not self.samples:
            return {
                "cpu_avg_percent": 0.0,
                "cpu_max_percent": 0.0,
                "memory_avg_mb": 0.0,
                "memory_max_mb": 0.0,
                "net_bytes_sent": 0,
                "net_bytes_recv": 0,
                "sample_count": 0,
            }
        cpu = [s["cpu_percent"] for s in self.samples]
        mem = [s["memory_mb"] for s in self.samples]
        last = self.samples[-1]
        return {
            "cpu_avg_percent": round(sum(cpu) / len(cpu), 3),
            "cpu_max_percent": round(max(cpu), 3),
            "memory_avg_mb": round(sum(mem) / len(mem), 3),
            "memory_max_mb": round(max(mem), 3),
            "net_bytes_sent": int(last["net_bytes_sent"] - self.net_start[0]),
            "net_bytes_recv": int(last["net_bytes_recv"] - self.net_start[1]),
            "sample_count": len(self.samples),
        }


def flowdc_config_for_run(spec: RunSpec, output_folder: Path) -> dict[str, Any]:
    config = {
        "input": str(spec.manifest_path),
        "input_format": "parquet",
        "output": str(output_folder),
        "output_format": "imagefolder",
        "url": "url",
        "label": "label",
        "concurrent_downloads": spec.concurrency,
        "timeout": spec.timeout_sec,
        "enable_paarc": True,
        "C_init": 8,
        "C_min": 2,
        "C_max": spec.concurrency,
        "max_retry_attempts": spec.retries,
        "create_tar": False,
        "create_overview": True,
    }
    if spec.tool.variant == "gradient":
        config.update(
            {
                "gradient_alpha": 0.3,
                "gradient_threshold": 0.10,
                "gradient_severe_threshold": 0.30,
                "startup_gradient_threshold": 0.15,
                "gradient_required_intervals": 2,
                "startup_gradient_required_intervals": 2,
                "gradient_queue_floor_mult": 0.25,
                "gradient_backoff_beta": 0.85,
                "post_backoff_grace_intervals": 2,
            }
        )
    return config


def command_for_run(spec: RunSpec, run_dir: Path) -> tuple[list[str], dict[str, str], Optional[Path]]:
    output_folder = run_dir / "payload"
    env = os.environ.copy()
    env["NO_ALBUMENTATIONS_UPDATE"] = "1"
    env["PYTHONUNBUFFERED"] = "1"

    if spec.tool.name == "flowdc":
        script = PROJECT_ROOT / "bin" / (
            "download_batch_gradient.py" if spec.tool.variant == "gradient" else "download_batch.py"
        )
        config = flowdc_config_for_run(spec, output_folder)
        config_path = run_dir / "flowdc_config.json"
        write_json(config_path, config)
        cmd = [sys.executable, str(script), "--config", str(config_path)]
        return cmd, env, output_folder

    if spec.tool.name == "img2dataset":
        cmd = [
            str(PROJECT_ROOT / ".venv" / "bin" / "img2dataset"),
            "--url_list",
            str(spec.manifest_path),
            "--output_folder",
            str(output_folder),
            "--input_format",
            "parquet",
            "--url_col",
            "url",
            "--output_format",
            "files",
            "--thread_count",
            str(spec.concurrency),
            "--processes_count",
            "1",
            "--timeout",
            str(spec.timeout_sec),
            "--retries",
            str(spec.retries),
            "--resize_mode",
            "no",
            "--enable_wandb",
            "False",
            "--extract_exif",
            "False",
        ]
        return cmd, env, output_folder

    raise ValueError(f"Unknown tool: {spec.tool}")


def classify_failure(status_code: Any, error: Any) -> str:
    try:
        code = int(status_code)
    except Exception:
        code = None
    err = str(error or "").lower()
    if code in LINK_ROT_CODES:
        return "link_rot_or_client"
    if code in ACCESS_POLICY_CODES:
        return "access_policy"
    if code in SERVER_FAULT_CODES:
        return "server_fault"
    if any(token in err for token in CONNECTION_ERROR_TOKENS):
        return "server_fault"
    if code and 400 <= code < 500:
        return "link_rot_or_client"
    return "other"


def parse_flowdc_result(overview_path: Path) -> dict[str, Any]:
    if not overview_path.exists():
        return {"error": "missing_flowdc_overview"}
    overview = read_json(overview_path)
    summary = overview.get("summary", {})
    total = int(summary.get("total_urls") or 0)
    success = int(summary.get("successful_downloads") or 0)
    failed = int(summary.get("failed_downloads") or 0)
    elapsed = float(summary.get("elapsed_sec") or 0.0)
    mb = float(summary.get("downloaded_mb") or 0.0)
    failure_classes: dict[str, int] = {}
    for item in overview.get("error_breakdown", []):
        klass = classify_failure(item.get("status_code"), item.get("error"))
        failure_classes[klass] = failure_classes.get(klass, 0) + int(item.get("count") or 0)
    return {
        "input_urls": total,
        "successful_downloads": success,
        "failed_downloads": failed,
        "success_rate_percent": (success / total * 100.0) if total else 0.0,
        "elapsed_seconds": elapsed,
        "downloaded_mb_decimal": mb,
        "throughput_mbps_decimal": (mb / elapsed) if elapsed else 0.0,
        "throughput_imgs_per_sec": (success / elapsed) if elapsed else 0.0,
        "failure_classes": failure_classes,
        "raw_error_breakdown": overview.get("error_breakdown", []),
        "gradient_summary": overview.get("gradient_summary", {}),
        "controller_variant": overview.get("controller_variant") or overview.get("paarc_version"),
        "overview_path": rel(overview_path),
    }


def parse_img2dataset_stdout(stdout_path: Path) -> dict[str, int]:
    """Parse img2dataset's terminal-only total line in files mode."""
    if not stdout_path.exists():
        return {}
    text = stdout_path.read_text(errors="replace")
    matches = re.findall(
        r"total\s+- success:\s*([0-9.]+)\s+- failed to download:\s*([0-9.]+)"
        r"\s+- failed to resize:\s*([0-9.]+)\s+- images per sec:\s*[0-9.]+\s+- count:\s*(\d+)",
        text,
    )
    if not matches:
        return {}
    success_ratio, failed_ratio, resize_ratio, count_text = matches[-1]
    count = int(count_text)
    successes = int(round(float(success_ratio) * count))
    failed_resize = int(round(float(resize_ratio) * count))
    failed_download = int(round(float(failed_ratio) * count))
    if successes + failed_download + failed_resize != count:
        failed_download = max(0, count - successes - failed_resize)
    return {
        "count": count,
        "successes": successes,
        "failed_to_download": failed_download,
        "failed_to_resize": failed_resize,
    }


def parse_img2dataset_stats(
    output_folder: Path,
    elapsed: float,
    *,
    stdout_path: Optional[Path] = None,
    manifest_count: Optional[int] = None,
) -> dict[str, Any]:
    success = 0
    failed = 0
    total_bytes = 0
    raw_stats: list[dict[str, Any]] = []
    failure_classes: dict[str, int] = {}

    for item in output_folder.rglob("*") if output_folder.exists() else []:
        if item.is_file() and item.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp", ".gif"}:
            success += 1
            total_bytes += item.stat().st_size

    for stats_path in output_folder.rglob("_stats.json") if output_folder.exists() else []:
        try:
            stats = read_json(stats_path)
        except Exception:
            continue
        raw_stats.append(stats)
        success = max(success, int(stats.get("successes") or 0))
        failed = max(failed, int(stats.get("failed_to_download") or stats.get("failed") or 0))

    # Per-sample json files often carry status/error strings; use them for taxonomy.
    for meta_path in output_folder.rglob("*.json") if output_folder.exists() else []:
        if meta_path.name == "_stats.json":
            continue
        try:
            meta = read_json(meta_path)
        except Exception:
            continue
        status = meta.get("status")
        if status == "success":
            continue
        klass = classify_failure(meta.get("status_code"), meta.get("error_message") or meta.get("error") or status)
        failure_classes[klass] = failure_classes.get(klass, 0) + 1

    stdout_stats = parse_img2dataset_stdout(stdout_path) if stdout_path else {}
    if stdout_stats:
        success = stdout_stats["successes"]
        failed = stdout_stats["failed_to_download"] + stdout_stats["failed_to_resize"]
    if manifest_count is not None:
        total = manifest_count
        failed = max(failed, total - success)
    else:
        total = success + failed
    classified_failures = sum(failure_classes.values())
    if failed > classified_failures:
        failure_classes["other"] = failure_classes.get("other", 0) + (failed - classified_failures)
    mb = total_bytes / 1e6
    return {
        "input_urls": total,
        "successful_downloads": success,
        "failed_downloads": failed,
        "success_rate_percent": (success / total * 100.0) if total else 0.0,
        "elapsed_seconds": elapsed,
        "downloaded_mb_decimal": mb,
        "throughput_mbps_decimal": (mb / elapsed) if elapsed else 0.0,
        "throughput_imgs_per_sec": (success / elapsed) if elapsed else 0.0,
        "failure_classes": failure_classes,
        "raw_shard_stats": raw_stats[:20],
        "stdout_stats": stdout_stats,
    }


async def terminate_process(proc: asyncio.subprocess.Process) -> None:
    if proc.returncode is not None:
        return
    try:
        parent = psutil.Process(proc.pid)
        children = parent.children(recursive=True)
        for child in children:
            child.terminate()
        parent.terminate()
        _, alive = psutil.wait_procs(children + [parent], timeout=10)
        for process in alive:
            process.kill()
    except psutil.Error:
        try:
            proc.terminate()
        except ProcessLookupError:
            return
    try:
        await asyncio.wait_for(proc.wait(), timeout=10)
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except ProcessLookupError:
            pass


async def run_one(spec: RunSpec, campaign: CampaignConfig) -> dict[str, Any]:
    if free_gib(PROJECT_ROOT) < campaign.min_free_gib:
        raise RuntimeError(f"Free disk below safety floor before run: {free_gib(PROJECT_ROOT):.1f} GiB")

    run_dir = campaign.output_dir / "runs" / spec.run_id
    if run_dir.exists():
        safe_rmtree(run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    artifacts_dir = run_dir / "artifacts"
    artifacts_dir.mkdir(exist_ok=True)

    cmd, env, payload_dir = command_for_run(spec, run_dir)
    command_meta = {
        "run_id": spec.run_id,
        "created_utc": utc_now(),
        "dataset": spec.dataset.name,
        "manifest_path": rel(spec.manifest_path),
        "manifest_sha256": file_sha256(spec.manifest_path),
        "tool": spec.tool.name,
        "variant": spec.tool.variant,
        "concurrency": spec.concurrency,
        "run_number": spec.run_number,
        "phase": spec.phase,
        "wall_clock_limit_sec": spec.wall_clock_limit_sec,
        "timeout_sec": spec.timeout_sec,
        "retries": spec.retries,
        "command": cmd,
    }
    write_json(artifacts_dir / "command.json", command_meta)

    stdout_path = artifacts_dir / "stdout.log"
    stderr_path = artifacts_dir / "stderr.log"
    print(f"[Run] {spec.run_id}")
    start = time.time()
    with stdout_path.open("wb") as stdout_f, stderr_path.open("wb") as stderr_f:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=str(PROJECT_ROOT),
            stdout=stdout_f if campaign.show_progress else stdout_f,
            stderr=stderr_f,
            env=env,
        )
        sampler = ResourceSampler(proc.pid, campaign.resource_sample_interval)
        await sampler.start()
        timed_out = False
        try:
            await asyncio.wait_for(proc.wait(), timeout=spec.wall_clock_limit_sec)
        except asyncio.TimeoutError:
            timed_out = True
            await terminate_process(proc)
        resources = await sampler.stop()

    elapsed = time.time() - start
    payload_bytes = dir_size_bytes(payload_dir) if payload_dir else 0
    payload_gib = payload_bytes / (1024**3)
    if payload_gib > campaign.max_payload_gib:
        print(f"[Safety] {spec.run_id} payload reached {payload_gib:.2f} GiB; it will be cleaned.")

    if spec.tool.name == "flowdc":
        overview_path = payload_dir.with_name(payload_dir.name + "_overview.json")
        if overview_path.exists():
            shutil.copy2(overview_path, artifacts_dir / overview_path.name)
        metrics = parse_flowdc_result(overview_path)
    else:
        stats_dir = artifacts_dir / "img2dataset_stats"
        stats_dir.mkdir(exist_ok=True)
        if payload_dir and payload_dir.exists():
            for stats_path in payload_dir.rglob("_stats.json"):
                dest = stats_dir / f"{stats_path.parent.name}_{stats_path.name}"
                shutil.copy2(stats_path, dest)
        manifest_count = pl.scan_parquet(spec.manifest_path).select(pl.len()).collect().item()
        metrics = parse_img2dataset_stats(
            payload_dir,
            elapsed,
            stdout_path=stdout_path,
            manifest_count=int(manifest_count),
        )

    result = {
        **command_meta,
        "completed_utc": utc_now(),
        "returncode": proc.returncode,
        "timed_out": timed_out,
        "censored": timed_out,
        "elapsed_wall_seconds": elapsed,
        "payload_bytes_before_cleanup": payload_bytes,
        "payload_gib_before_cleanup": payload_gib,
        "resources": resources,
        "metrics": metrics,
        "free_disk_gib_after_run_before_cleanup": round(free_gib(PROJECT_ROOT), 3),
    }

    # Payload cleanup after metrics/artifacts are captured.
    if payload_dir and payload_dir.exists():
        safe_rmtree(payload_dir)
    result["payload_exists_after_cleanup"] = bool(payload_dir and payload_dir.exists())
    result["free_disk_gib_after_cleanup"] = round(free_gib(PROJECT_ROOT), 3)
    write_json(artifacts_dir / "result.json", result)
    return result


def get_manifest_path(dataset: DatasetSpec, phase: str) -> Path:
    suffix = f"{dataset.pilot_rows}_pilot" if phase == "pilot" else str(dataset.full_rows)
    path = MANIFEST_DIR / f"{dataset.name}_{suffix}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Manifest missing: {path}. Run build-manifests first.")
    return path


def pilot_specs(config: CampaignConfig) -> list[RunSpec]:
    specs: list[RunSpec] = []
    for dataset in DATASETS:
        manifest = get_manifest_path(dataset, "pilot")
        tools = [GRADIENT, IMG2DATASET]
        if dataset.name in {"inat_s3", "gbif_multimedia"}:
            tools.append(PAARC)
        for tool in tools:
            specs.append(
                RunSpec(
                    dataset=dataset,
                    manifest_path=manifest,
                    tool=tool,
                    concurrency=min(dataset.low_concurrency, 256),
                    run_number=0,
                    phase="pilot",
                    wall_clock_limit_sec=config.pilot_wall_clock_limit_sec,
                )
            )
    return specs


def full_specs(config: CampaignConfig) -> list[RunSpec]:
    specs: list[RunSpec] = []
    for dataset in DATASETS:
        manifest = get_manifest_path(dataset, "full")
        primary_levels = [dataset.low_concurrency, dataset.high_concurrency]
        for concurrency in primary_levels:
            for run_number in range(config.measured_runs):
                specs.append(
                    RunSpec(
                        dataset=dataset,
                        manifest_path=manifest,
                        tool=GRADIENT,
                        concurrency=concurrency,
                        run_number=run_number,
                        phase="full",
                        wall_clock_limit_sec=config.full_wall_clock_limit_sec,
                    )
                )
                specs.append(
                    RunSpec(
                        dataset=dataset,
                        manifest_path=manifest,
                        tool=IMG2DATASET,
                        concurrency=concurrency,
                        run_number=run_number,
                        phase="full",
                        wall_clock_limit_sec=config.full_wall_clock_limit_sec,
                    )
                )
    for dataset in [d for d in DATASETS if d.name in {"inat_s3", "gbif_multimedia"}]:
        manifest = get_manifest_path(dataset, "full")
        for run_number in range(config.measured_runs):
            specs.append(
                RunSpec(
                    dataset=dataset,
                    manifest_path=manifest,
                    tool=PAARC,
                    concurrency=dataset.high_concurrency,
                    run_number=run_number,
                    phase="full",
                    wall_clock_limit_sec=config.full_wall_clock_limit_sec,
                )
            )
    rng = random.Random(SEED)
    rng.shuffle(specs)
    return specs


async def run_specs(specs: list[RunSpec], config: CampaignConfig, *, campaign_limit_sec: Optional[int] = None) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    start = time.time()
    for index, spec in enumerate(specs, start=1):
        if campaign_limit_sec and (time.time() - start) > campaign_limit_sec:
            print("[Safety] Campaign time budget exhausted; stopping remaining runs.")
            break
        print(f"[Campaign] {index}/{len(specs)} free_disk={free_gib(PROJECT_ROOT):.1f}GiB")
        try:
            result = await run_one(spec, config)
        except Exception as exc:
            result = {
                "run_id": spec.run_id,
                "dataset": spec.dataset.name,
                "tool": spec.tool.name,
                "variant": spec.tool.variant,
                "concurrency": spec.concurrency,
                "run_number": spec.run_number,
                "phase": spec.phase,
                "failed_before_completion": True,
                "error": repr(exc),
                "completed_utc": utc_now(),
            }
            err_dir = config.output_dir / "runs" / spec.run_id / "artifacts"
            write_json(err_dir / "result.json", result)
            print(f"[RunError] {spec.run_id}: {exc}")
        results.append(result)
        write_json(config.output_dir / f"{spec.phase}_partial_results.json", results)
    return results


def collect_results(output_dir: Path) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for result_path in sorted((output_dir / "runs").glob("*/artifacts/result.json")):
        try:
            results.append(read_json(result_path))
        except Exception:
            continue
    return results


def aggregate_results(results: list[dict[str, Any]]) -> dict[str, Any]:
    groups: dict[tuple[str, str, str, int, str], list[dict[str, Any]]] = {}
    for result in results:
        if result.get("failed_before_completion"):
            continue
        key = (
            str(result.get("phase")),
            str(result.get("dataset")),
            str(result.get("tool")),
            int(result.get("concurrency") or 0),
            str(result.get("variant")),
        )
        groups.setdefault(key, []).append(result)

    aggregates = []
    for (phase, dataset, tool, concurrency, variant), runs in sorted(groups.items()):
        metric_rows = [r.get("metrics", {}) for r in runs]
        input_urls = [m.get("input_urls", 0) for m in metric_rows]
        total_input_urls = max(1, sum(int(v or 0) for v in input_urls))
        failure_totals = {
            klass: sum(int(m.get("failure_classes", {}).get(klass, 0) or 0) for m in metric_rows)
            for klass in ("server_fault", "link_rot_or_client", "access_policy", "other")
        }
        def avg(values: Iterable[float]) -> float:
            vals = [float(v) for v in values]
            return sum(vals) / len(vals) if vals else 0.0
        aggregates.append(
            {
                "phase": phase,
                "dataset": dataset,
                "tool": tool,
                "variant": variant,
                "concurrency": concurrency,
                "runs": len(runs),
                "censored_runs": sum(1 for r in runs if r.get("censored")),
                "avg_success_rate_percent": avg(m.get("success_rate_percent", 0.0) for m in metric_rows),
                "avg_throughput_mbps_decimal": avg(m.get("throughput_mbps_decimal", 0.0) for m in metric_rows),
                "avg_throughput_imgs_per_sec": avg(m.get("throughput_imgs_per_sec", 0.0) for m in metric_rows),
                "avg_elapsed_seconds": avg(m.get("elapsed_seconds", 0.0) for m in metric_rows),
                "server_faults_per_10k_urls": failure_totals["server_fault"] / total_input_urls * 10_000,
                "link_rot_per_10k_urls": failure_totals["link_rot_or_client"] / total_input_urls * 10_000,
                "access_policy_per_10k_urls": failure_totals["access_policy"] / total_input_urls * 10_000,
                "unknown_other_failures_per_10k_urls": failure_totals["other"] / total_input_urls * 10_000,
                "avg_cpu_percent": avg(r.get("resources", {}).get("cpu_avg_percent", 0.0) for r in runs),
                "avg_memory_mb": avg(r.get("resources", {}).get("memory_avg_mb", 0.0) for r in runs),
                "avg_payload_gib": avg(r.get("payload_gib_before_cleanup", 0.0) for r in runs),
            }
        )
    return {"created_utc": utc_now(), "aggregates": aggregates}


def write_csv_tables(output_dir: Path, aggregate: dict[str, Any]) -> None:
    tables_dir = output_dir / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)
    rows = aggregate.get("aggregates", [])
    if not rows:
        return
    with (tables_dir / "aggregate_results.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_markdown_summary(output_dir: Path, results: list[dict[str, Any]], aggregate: dict[str, Any]) -> None:
    lines = [
        "# FLOW-DC Gradient Campaign Results",
        "",
        f"Generated UTC: {utc_now()}",
        f"Runs parsed: {len(results)}",
        "",
        "## Aggregate Results",
        "",
        "| Phase | Dataset | Tool | Variant | C | Runs | Success % | MB/s | Img/s | Server faults / 10k | Unknown / 10k |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in aggregate.get("aggregates", []):
        lines.append(
            "| {phase} | {dataset} | {tool} | {variant} | {concurrency} | {runs} | "
            "{avg_success_rate_percent:.2f} | {avg_throughput_mbps_decimal:.2f} | "
            "{avg_throughput_imgs_per_sec:.2f} | {server_faults_per_10k_urls:.2f} | "
            "{unknown_other_failures_per_10k_urls:.2f} |".format(**row)
        )
    lines.extend(
        [
            "",
            "## Interpretation Notes",
            "",
            "- 404/410 failures are treated as link rot or dataset quality, not congestion-control failure.",
            "- 429, 5xx, timeout, and connection-reset style errors are treated as server-fault evidence.",
            "- img2dataset failures are often untyped in files mode, so unknown failures are shown separately instead of inferred as server faults.",
            "- Payload directories are deleted after each run; JSON, logs, stats, and summaries are preserved.",
        ]
    )
    (output_dir / "results_summary.md").write_text("\n".join(lines) + "\n")


def write_plots(output_dir: Path, aggregate: dict[str, Any]) -> None:
    try:
        import matplotlib.pyplot as plt
    except Exception as exc:
        print(f"[Analyze] Skipping plots: {exc}")
        return
    plots_dir = output_dir / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)
    rows = [r for r in aggregate.get("aggregates", []) if r.get("phase") == "full"]
    if not rows:
        rows = aggregate.get("aggregates", [])
    if not rows:
        return

    for dataset in sorted({r["dataset"] for r in rows}):
        subset = [r for r in rows if r["dataset"] == dataset]
        labels = [f"{r['tool']}\n{r['variant']}\nC={r['concurrency']}" for r in subset]
        throughput = [r["avg_throughput_mbps_decimal"] for r in subset]
        faults = [r["server_faults_per_10k_urls"] for r in subset]
        x = range(len(subset))

        plt.figure(figsize=(max(8, len(subset) * 1.2), 5))
        plt.bar(x, throughput)
        plt.xticks(x, labels, rotation=35, ha="right")
        plt.ylabel("Throughput (MB/s)")
        plt.title(f"{dataset}: throughput")
        plt.tight_layout()
        plt.savefig(plots_dir / f"{dataset}_throughput.png", dpi=160)
        plt.close()

        plt.figure(figsize=(max(8, len(subset) * 1.2), 5))
        plt.bar(x, faults)
        plt.xticks(x, labels, rotation=35, ha="right")
        plt.ylabel("Server faults per 10k URLs")
        plt.title(f"{dataset}: server-fault rate")
        plt.tight_layout()
        plt.savefig(plots_dir / f"{dataset}_server_faults.png", dpi=160)
        plt.close()


def analyze(output_dir: Path) -> dict[str, Any]:
    results = collect_results(output_dir)
    aggregate = aggregate_results(results)
    write_json(output_dir / "aggregate_results.json", aggregate)
    write_csv_tables(output_dir, aggregate)
    write_markdown_summary(output_dir, results, aggregate)
    write_plots(output_dir, aggregate)
    return aggregate


def make_campaign_config(args: argparse.Namespace) -> CampaignConfig:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_name = args.run_name or f"flowdc_gradient_campaign_{timestamp}"
    output_dir = Path(args.output_dir or RESULTS_DIR) / run_name
    output_dir.mkdir(parents=True, exist_ok=True)
    return CampaignConfig(
        output_dir=output_dir,
        run_name=run_name,
        full_campaign_limit_sec=args.campaign_limit_sec,
        measured_runs=args.measured_runs,
        pilot_wall_clock_limit_sec=args.pilot_wall_clock_limit_sec,
        full_wall_clock_limit_sec=args.full_wall_clock_limit_sec,
        min_free_gib=args.min_free_gib,
        max_payload_gib=args.max_payload_gib,
        show_progress=args.show_progress,
    )


async def async_main(args: argparse.Namespace) -> int:
    config = make_campaign_config(args)
    print(f"[Campaign] output={rel(config.output_dir)}")

    if args.command in {"preflight", "all", "pilot", "full"}:
        record_preflight(config.output_dir)

    if args.command in {"build-manifests", "all"}:
        await build_manifests()

    if args.command in {"pilot", "all"}:
        if not (MANIFEST_DIR / "manifest_index.json").exists():
            await build_manifests()
        specs = pilot_specs(config)
        if args.datasets:
            selected = set(args.datasets)
            specs = [spec for spec in specs if spec.dataset.name in selected]
        pilot_results = await run_specs(specs, config)
        write_json(config.output_dir / "pilot_results.json", pilot_results)
        analyze(config.output_dir)

    if args.command in {"full", "all"}:
        if not (MANIFEST_DIR / "manifest_index.json").exists():
            await build_manifests()
        specs = full_specs(config)
        if args.datasets:
            selected = set(args.datasets)
            specs = [spec for spec in specs if spec.dataset.name in selected]
        full_results = await run_specs(specs, config, campaign_limit_sec=config.full_campaign_limit_sec)
        write_json(config.output_dir / "full_results.json", full_results)
        analyze(config.output_dir)

    if args.command == "analyze":
        analyze(Path(args.existing_output_dir or config.output_dir))

    if args.command == "preflight":
        analyze(config.output_dir)

    print(f"[Campaign] complete output={rel(config.output_dir)}")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the FLOW-DC Gradient research campaign.")
    parser.add_argument(
        "command",
        choices=["preflight", "build-manifests", "pilot", "full", "analyze", "all"],
    )
    parser.add_argument("--output-dir", type=Path, default=RESULTS_DIR)
    parser.add_argument("--existing-output-dir", type=Path)
    parser.add_argument("--run-name", default="")
    parser.add_argument("--campaign-limit-sec", type=int, default=8 * 60 * 60)
    parser.add_argument("--measured-runs", type=int, default=2)
    parser.add_argument("--pilot-wall-clock-limit-sec", type=int, default=8 * 60)
    parser.add_argument("--full-wall-clock-limit-sec", type=int, default=12 * 60)
    parser.add_argument("--min-free-gib", type=float, default=100.0)
    parser.add_argument("--max-payload-gib", type=float, default=25.0)
    parser.add_argument("--show-progress", action="store_true")
    parser.add_argument("--datasets", nargs="+", choices=[dataset.name for dataset in DATASETS])
    return parser.parse_args()


def main() -> int:
    return asyncio.run(async_main(parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
