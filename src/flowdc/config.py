"""A single validated configuration shared by CLI, local and TaskVine execution."""

from __future__ import annotations

import math
from dataclasses import dataclass, field, fields, replace
from pathlib import Path

from .controllers import MethodConfig
from .integrity import parse


@dataclass(frozen=True)
class DownloadConfig:
    input_path: str
    output_dir: str
    input_format: str | None = None
    url_column: str = "url"
    label_column: str | None = None
    output_format: str = "imagefolder"
    workers: int | None = None
    timeout_s: float = 30.0
    method: str = MethodConfig.method
    method_options: dict = field(default_factory=dict)
    c_min: int = MethodConfig.c_min
    c_init: int = MethodConfig.c_init
    c_max: int = MethodConfig.c_max
    max_attempts: int = 3
    retry_backoff_s: float = 2.0
    naming_mode: str = "row_id"
    file_name_pattern: str = "{segment[-2]}"
    create_tar: bool = True
    compress_archive: bool = False
    create_overview: bool = True
    force: bool = False
    resume: bool = False
    reconcile: bool = False
    research_profile: bool = False
    http_connection_limit: int | None = None

    @classmethod
    def from_dict(cls, value: dict) -> DownloadConfig:
        if not isinstance(value, dict):
            raise ValueError("download configuration must be a JSON object")
        unknown = set(value) - {f.name for f in fields(cls)}
        if unknown:
            raise ValueError(
                f"Unknown configuration fields: {', '.join(sorted(unknown))}. See docs/migration.md"
            )
        try:
            return normalize_config(cls(**value))
        except TypeError as exc:
            raise ValueError(f"Invalid download configuration: {exc}") from exc

    @classmethod
    def read(cls, path: str | Path, **overrides) -> DownloadConfig:
        value = parse(Path(path).read_bytes())
        if not isinstance(value, dict):
            raise ValueError("download configuration must be a JSON object")
        return cls.from_dict({**value, **overrides})


def normalize_config(cfg: DownloadConfig) -> DownloadConfig:
    for name in ("input_path", "output_dir", "url_column", "file_name_pattern"):
        if not isinstance(getattr(cfg, name), str) or not getattr(cfg, name):
            raise ValueError(f"{name} must be a nonempty string")
    if cfg.input_format not in (None, "parquet", "csv", "excel", "xml"):
        raise ValueError("unsupported input_format")
    if cfg.label_column is not None and not isinstance(cfg.label_column, str):
        raise ValueError("label_column must be a string or null")
    for name in ("timeout_s", "retry_backoff_s"):
        value = getattr(cfg, name)
        if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
            raise ValueError(f"{name} must be finite and positive")
    for name in ("workers", "http_connection_limit"):
        value = getattr(cfg, name)
        if value is not None and (type(value) is not int or not 1 <= value <= 10000):
            raise ValueError(f"{name} must be null or an integer in 1..10000")
    for name in (
        "create_tar",
        "compress_archive",
        "create_overview",
        "force",
        "resume",
        "reconcile",
        "research_profile",
    ):
        if type(getattr(cfg, name)) is not bool:
            raise ValueError(f"{name} must be boolean")
    if not isinstance(cfg.method_options, dict):
        raise ValueError("method_options must be an object")
    MethodConfig.from_config(cfg)
    if cfg.resume and cfg.reconcile or (cfg.resume or cfg.reconcile) and cfg.force:
        raise ValueError("resume, reconcile and force are mutually exclusive")
    if cfg.naming_mode not in ("sequential", "url_based", "row_id"):
        raise ValueError("unsupported naming_mode")
    if cfg.output_format not in ("imagefolder", "webdataset"):
        raise ValueError("unsupported output_format")
    if type(cfg.max_attempts) is not int or cfg.max_attempts < 1:
        raise ValueError("max_attempts must be a positive integer including the first attempt")
    if cfg.research_profile:
        cfg = replace(
            cfg,
            naming_mode="row_id",
            output_format="webdataset",
            create_tar=True,
            compress_archive=False,
            create_overview=True,
        )
    return cfg
