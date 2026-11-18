"""FLOW-DC command-line interface. Optional runtimes load only when selected."""

from __future__ import annotations

import argparse
import asyncio
import json
import subprocess
import sys
from dataclasses import fields
from pathlib import Path

from .config import DownloadConfig
from .integrity import parse


def parser():
    root = argparse.ArgumentParser(
        prog="flowdc",
        description="Verifiable image acquisition with adaptive per-host concurrency.",
        allow_abbrev=False,
    )
    root.add_argument("--version", action="version", version="FLOW-DC 4.0.0")
    sub = root.add_subparsers(dest="command", required=True)
    download = sub.add_parser("download", help="Download a URL manifest", allow_abbrev=False)
    download.add_argument("--config")
    download.add_argument("--backend", choices=["local", "taskvine"], default="local")
    download.add_argument("--taskvine-config")
    for flag in (
        "input-path",
        "output-dir",
        "url-column",
        "label-column",
        "input-format",
        "output-format",
        "naming-mode",
        "file-name-pattern",
    ):
        download.add_argument("--" + flag, default=argparse.SUPPRESS)
    download.add_argument(
        "--method",
        default=argparse.SUPPRESS,
        help=f"Request controller: {DownloadConfig.method} (default) or paarc-ratio",
    )
    for flag in ("workers", "c-min", "c-init", "c-max", "max-attempts", "http-connection-limit"):
        download.add_argument("--" + flag, type=int, default=argparse.SUPPRESS)
    for flag in ("timeout-s", "retry-backoff-s"):
        download.add_argument("--" + flag, type=float, default=argparse.SUPPRESS)
    download.add_argument("--method-options", type=json.loads, default=argparse.SUPPRESS)
    for flag in ("force", "create-tar", "compress-archive", "create-overview", "research-profile"):
        download.add_argument("--" + flag, action=argparse.BooleanOptionalAction, default=argparse.SUPPRESS)
    for command in ("resume", "reconcile", "verify"):
        item = sub.add_parser(command, allow_abbrev=False)
        item.add_argument("--output-dir", required=True)
        if command == "resume":
            item.add_argument(
                "--input-path", help="Override the recorded source path; its content identity must match"
            )
    part = sub.add_parser("partition", allow_abbrev=False)
    part.add_argument("--input-path", required=True)
    part.add_argument("--input-format", choices=["parquet", "csv", "excel", "xml"])
    part.add_argument("--output-dir", required=True)
    part.add_argument("--groups", type=int, default=1)
    part.add_argument("--url-column", default="url")
    part.add_argument("--method", choices=["host", "rows"], default="host")
    exp = sub.add_parser("experiment", help="Prepare, run, verify or analyze a frozen protocol")
    exp.add_argument("arguments", nargs=argparse.REMAINDER)
    ops = sub.add_parser("ops", help="Optional Jetstream2 operations")
    ops.add_argument("arguments", nargs=argparse.REMAINDER)
    return root


def recovery_config(output_dir, command, input_path=None):
    from .integrity import RunStore

    with RunStore(output_dir) as store:
        owner = store.owner
    record = owner["config"]
    if record.get("implementation") != "flowdc-package-v2":
        raise ValueError(
            "Historical runs require their original runtime for recovery; no automatic migration is performed"
        )
    values = record["values"]
    source = input_path or owner.get("input_path")
    if command == "resume" and not source:
        raise ValueError(
            "No source path was recorded; supply --input-path to resume with the original manifest"
        )
    # Offline reconciliation needs only the journal, even before report publication.
    source = str(Path(source).resolve()) if source else "unavailable-for-offline-reconciliation"
    return DownloadConfig.from_dict({**values, "input_path": source, "output_dir": output_dir, command: True})


def main(argv=None):
    root = parser()
    args = root.parse_args(argv)
    try:
        if args.command == "ops":
            runtime = Path(__file__).parent / "ops/runtime/flowdc_ops.py"
            return subprocess.call([sys.executable, "-B", str(runtime), *args.arguments])
        if args.command == "experiment":
            from .experiments import main as experiment_main

            return experiment_main(args.arguments)
        if args.command == "partition":
            from .partition import partition_manifest

            result = [
                str(p)
                for p in partition_manifest(
                    args.input_path,
                    args.output_dir,
                    groups=args.groups,
                    url_column=args.url_column,
                    method=args.method,
                    input_format=args.input_format,
                )
            ]
        elif args.command == "verify":
            from .verification import verify_output

            result = verify_output(args.output_dir)
        else:
            from . import downloader

            if args.command == "download":
                accepted = {f.name for f in fields(DownloadConfig)}
                overrides = {k: v for k, v in vars(args).items() if k in accepted}
                config = (
                    DownloadConfig.read(args.config, **overrides)
                    if args.config
                    else DownloadConfig.from_dict(overrides)
                )
            else:
                config = recovery_config(args.output_dir, args.command, getattr(args, "input_path", None))
            downloader._setup_signal_handlers()
            if getattr(args, "backend", "local") == "taskvine":
                from .distributed import run_distributed

                if not args.taskvine_config:
                    raise ValueError("--backend taskvine requires --taskvine-config")
                result = run_distributed(config, parse(Path(args.taskvine_config).read_bytes()))
            else:
                result = asyncio.run(downloader.run_acquisition(config))
        print(json.dumps(result, indent=2, allow_nan=False))
        if isinstance(result, dict) and "output_integrity" in result:
            return 0 if result["output_integrity"]["run_complete"] else 1
        if isinstance(result, dict) and "run_complete" in result:
            return 0 if result["run_complete"] else 1
        return 0
    except (ValueError, OSError, KeyError) as exc:
        root.exit(2, f"flowdc: {exc}\n")
