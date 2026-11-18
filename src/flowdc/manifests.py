"""Manifest identity, preflight validation and explicit overwrite consent."""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

import polars as pl

from . import integrity
from .config import DownloadConfig as Config
from .config import normalize_config
from .http import load_input_file


def load_manifest(cfg):
    path = Path(cfg.input_path)
    raw = path.read_bytes()
    df = load_input_file(str(path), cfg.input_format)
    if cfg.url_column not in df.columns:
        raise ValueError(f"URL column '{cfg.url_column}' not found")
    if cfg.label_column is not None and cfg.label_column not in df.columns:
        raise ValueError(f"Label column '{cfg.label_column}' not found")
    if cfg.url_column == "__key__" or (cfg.label_column and cfg.label_column == "__key__"):
        raise ValueError("__key__ is reserved for internal row identity; rename the mapped input column")
    if cfg.method is not None:
        from .controllers import validate_research_metadata

        validate_research_metadata(df, cfg.url_column)
    # Verify input has not changed between hashing and loading.
    if path.read_bytes() != raw:
        raise ValueError("Input manifest changed during loading")
    df = integrity.stamp_frame(df, integrity.digest(raw))
    return df, {
        "sha256": integrity.digest(raw),
        "original_rows": df.height,
        "source_counts": {
            row[integrity.PROVENANCE[0]]: row[integrity.PROVENANCE[2]] for row in df.iter_rows(named=True)
        },
    }


def validate_and_load(cfg: Config, prepared=None) -> pl.DataFrame:
    """Validate the entire manifest and output ownership before overwrite consent."""
    cfg = normalize_config(cfg)
    df, _manifest = prepared if prepared is not None else load_manifest(cfg)
    out = Path(os.path.abspath(cfg.output_dir))
    cwd = Path.cwd().resolve()
    if (
        out == Path(out.anchor)
        or out == cwd
        or out in cwd.parents
        or out in Path(cfg.input_path).resolve().parents
    ):
        raise ValueError("Output cannot contain the input or working directory, or be the root directory")
    integrity.component(out.name)
    # Opening every existing ancestor rejects symlinks, including the root itself.
    with integrity.Files(out.parent, create=True):
        pass
    if out.exists() or out.is_symlink():
        with integrity.Files(out):
            pass
        if (out / integrity.INTERNAL).exists() or (out / integrity.INTERNAL).is_symlink():
            # Never erase a malformed/newer journal or an active managed run.
            with integrity.RunStore(out, allow_historical=True) as previous:
                integrity.require(
                    not previous.read_only,
                    "Earlier-version output is preserved, including with --force. "
                    "Choose a new output directory or move the intact run elsewhere; "
                    "use the original recovery runtime for historical recovery.",
                )
        if cfg.resume or cfg.reconcile:
            return df
        if not cfg.force:
            print(
                f"[I/O] Output folder exists: {out}\n      Delete and continue? [y/N]: ",
                end="",
                file=sys.stderr,
            )
            response = input().strip().lower()
            if response not in ("y", "yes"):
                raise SystemExit("Aborted: output folder exists. Use --force to overwrite.")
        print(f"[I/O] Deleting existing output with consent: {out}", file=sys.stderr)
        if (out / integrity.INTERNAL).exists():
            with integrity.RunStore(out) as previous:
                previous.remove_owned_exports()
                shutil.rmtree(out)
        else:
            shutil.rmtree(out)
    elif cfg.resume or cfg.reconcile:
        raise ValueError("Recovery requires existing owned output")
    out.mkdir(parents=True)
    return df


def count_unique_hosts(df: pl.DataFrame, url_column: str) -> int:
    """
    Count unique hosts in the URL column.

    Used for auto-sizing the worker pool and connection limits.

    Args:
        df: DataFrame with URLs
        url_column: Name of the URL column

    Returns:
        Number of unique hosts found
    """
    # Extract hosts from URLs using Polars string operations
    # URL format: scheme://host/path -> extract host part
    hosts = (
        df.select(pl.col(url_column))
        .with_columns(
            pl.col(url_column)
            .str.replace(r"^https?://", "")  # Remove scheme
            .str.replace(r"/.*$", "")  # Remove path
            .str.to_lowercase()
            .alias("__host__")
        )
        .select("__host__")
        .unique()
    )
    return hosts.height


def resolve_worker_count(cfg: Config, df: pl.DataFrame) -> int:
    """Resolve explicit or host-based concurrency for either downloader."""
    num_hosts = count_unique_hosts(df, cfg.url_column)
    print(f"[Load] Unique hosts detected: {num_hosts}", file=sys.stderr)
    if cfg.workers is not None and cfg.workers > 0:
        workers = cfg.workers
        print(f"[Config] Using configured workers: {workers}", file=sys.stderr)
    else:
        workers = max(1, min(num_hosts * cfg.c_max, 10000))
        print(
            f"[Config] Auto-sized worker pool: {workers} ({num_hosts} hosts × {cfg.c_max} c_max)",
            file=sys.stderr,
        )
    return workers
