#!/usr/bin/env python3
"""Bounded real-native localhost engineering smoke; never an efficacy campaign."""

import argparse
import importlib.metadata
import io
import json
import sys
import threading
import time
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from benchmark.core.flowdc_adapter import FlowDCAdapter, FlowDCConfig  # noqa: E402
from benchmark.core.img2dataset_adapter import Img2DatasetAdapter, Img2DatasetConfig  # noqa: E402
from benchmark.core.lifecycle import run_verified  # noqa: E402
from benchmark.core.provenance import environment_record  # noqa: E402
from benchmark.core.truth import PROVENANCE, Truth, digest, encode, require  # noqa: E402
from benchmark.core.verifier import verify_native  # noqa: E402


def image_payloads():
    from PIL import Image

    result = {}
    for format_name, color in (("JPEG", (23, 83, 149)), ("PNG", (197, 79, 43))):
        stream = io.BytesIO()
        Image.new("RGB", (17, 13), color).save(stream, format=format_name)
        raw = stream.getvalue()
        # Validity is checked before the clients see any payload.
        with Image.open(io.BytesIO(raw)) as image:
            image.verify()
        result[format_name] = raw
    return result


class FixtureOrigin:
    """Concurrent primary success fixture with arrival/response event records.

    The capacity/scenario service of milestone C is deliberately separate future
    work. This origin provides independent byte and request truth for milestone A.
    """

    def __init__(self, directory, payloads):
        self.lock = threading.Lock()
        self.events = []
        self.payloads = {
            "/left/same.jpg": payloads["JPEG"],
            "/right/same.jpg": payloads["PNG"],
            "/alias.png": payloads["JPEG"],
            "/plain.png": payloads["PNG"],
        }
        self.log = (directory / "origin.jsonl").open("xb")
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def setup(self):
                super().setup()
                self.connection.settimeout(5)

            def do_GET(self):
                with owner.lock:
                    request_id = 1 + sum(event["phase"] == "arrival" for event in owner.events)
                    owner.event(request_id, "arrival", self.path)
                payload = owner.payloads.get(self.path)
                status, sent, disconnected = (200 if payload is not None else 404), 0, False
                try:
                    self.send_response(status)
                    self.send_header("Content-Length", str(len(payload or b"")))
                    self.end_headers()
                    if payload:
                        self.wfile.write(payload)
                        self.wfile.flush()
                        sent = len(payload)
                except (BrokenPipeError, ConnectionResetError, TimeoutError):
                    disconnected = True
                    sent = None  # A partial write's exact byte count is unknown.
                finally:
                    with owner.lock:
                        owner.event(
                            request_id,
                            "response",
                            self.path,
                            status=status,
                            body_bytes_written=sent,
                            disconnected=disconnected,
                        )

            def log_message(self, *args):
                pass

        try:
            self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        except BaseException:
            self.log.close()
            raise
        self.server.daemon_threads = False
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": 0.05})

    def event(self, request_id, phase, path, **fields):
        event = {
            "request_id": request_id,
            "phase": phase,
            "path": path,
            "origin_monotonic_ns": time.monotonic_ns(),
            **fields,
        }
        self.events.append(event)
        self.log.write(encode(event))
        self.log.flush()

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.log.close()

    @property
    def base_url(self):
        return f"http://127.0.0.1:{self.server.server_port}"


def smoke(output):
    output = output.absolute()
    require(not output.exists() and not output.is_symlink(), "output collision; evidence must be retained")
    # Do not silently pick a global executable from an unrelated environment.
    require(importlib.metadata.version("img2dataset") == "1.47.0", "img2dataset==1.47.0 required")
    executable = Path(sys.executable).parent / "img2dataset"
    require(executable.is_file(), "img2dataset CLI must be installed beside this Python executable")
    payloads = image_payloads()
    environment = environment_record(ROOT)
    environment["img2dataset_cli_sha256"] = digest(executable.read_bytes())
    output.mkdir(parents=True, exist_ok=False)
    (output / "environment.json").write_bytes(encode(environment))
    originals = output / "origin-payloads"
    originals.mkdir()
    for format_name, raw in payloads.items():
        (originals / (format_name.lower() + ".bin")).write_bytes(raw)
    try:
        origin = FixtureOrigin(output, payloads)
    except OSError as exc:
        (output / "failure.json").write_bytes(encode({"stage": "origin_bind", "error": str(exc)}))
        raise
    results = {}
    with origin:
        urls = [origin.base_url + path for path in origin.payloads]
        urls += [urls[0], urls[1], None, "", "not-an-http-url"]
        pl.DataFrame(
            {
                "url": urls,
                "label": [f"row-{i}" for i in range(len(urls))],
                "nullable": [None, "", "alpha", "beta", None, "gamma", None, None, None],
            }
        ).write_parquet(output / "input.parquet")
        catalog = {
            origin.base_url + path: {"bytes": len(raw), "sha256": digest(raw)}
            for path, raw in origin.payloads.items()
        }
        truth = Truth.load(output / "input.parquet", catalog)
        input_path = truth.write(output / "fixture")
        for tool in ("flowdc", "img2dataset"):
            run_dir = output / tool
            native_dir = run_dir / "native"
            if tool == "flowdc":
                config_path = output / "flowdc-config.json"
                config = FlowDCConfig(
                    str(input_path),
                    str(native_dir),
                    "url",
                    None,
                    2,
                    5,
                    True,
                    paarc_c_init=2,
                    paarc_c_min=1,
                    paarc_c_max=2,
                    research_profile=True,
                )
                adapter = FlowDCAdapter(ROOT)
                adapter.generate_config(config, config_path)
                command = [sys.executable, str(adapter.bin_path), "--config", str(config_path)]
                config_digest = digest(config_path.read_bytes())
            else:
                config = Img2DatasetConfig(
                    str(input_path),
                    str(native_dir),
                    "url",
                    2,
                    timeout_sec=5,
                    retries=0,
                    max_shard_retry=0,
                    output_format="webdataset",
                    additional_columns=[c for c in truth.frame.columns if c != "url"] + list(PROVENANCE),
                    executable=str(executable),
                )
                command = Img2DatasetAdapter().build_command(config)
                command.extend(
                    [
                        "--number_sample_per_shard",
                        "3",
                        "--encode_format",
                        "jpg",
                        "--compute_hash",
                        "sha256",
                        "--distributor",
                        "multiprocessing",
                    ]
                )
                config_digest = digest(encode(command))
            start_event = len(origin.events)
            provenance = {
                "environment_sha256": digest(encode(environment)),
                "config_sha256": config_digest,
                "truth_sha256": digest(encode(truth.record)),
                "prepared_manifest_sha256": digest(input_path.read_bytes()),
                "attempt_budget_per_row": 1,
                "shard_retries": 0,
                "timeout_semantics": "aiohttp native" if tool == "flowdc" else "urllib native",
                "timeout_comparability": "outer deadline only; native per-request semantics differ",
            }
            result = run_verified(
                command,
                run_dir,
                truth.record,
                lambda tool=tool, native_dir=native_dir: verify_native(tool, native_dir, truth.record),
                cwd=ROOT,
                provenance=provenance,
            )
            with origin.lock:
                events = list(origin.events[start_event:])
            attempts = Counter(e["path"] for e in events if e["phase"] == "arrival")
            expected_attempts = Counter(url.removeprefix(origin.base_url) for url in urls if url in catalog)
            request_record = {
                "attempts_by_path": dict(attempts),
                "total_attempts": sum(attempts.values()),
                "expected_primary_attempts_by_path": dict(expected_attempts),
                "primary_attempts_match": attempts == expected_attempts,
                "row_attribution": "unavailable for duplicate URLs; no row-to-request mapping inferred",
                "events": events,
            }
            (run_dir / "origin-requests.json").write_bytes(encode(request_record))
            results[tool] = {"run": result, "origin_requests": request_record}
            if result["status"] in ("interrupted", "cleanup_failed"):
                break
    (output / "smoke.json").write_bytes(encode(results))
    return len(results) == 2 and all(
        r["run"]["run_complete"] and r["origin_requests"]["primary_attempts_match"] for r in results.values()
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output", type=Path, required=True, help="New retained evidence directory; collisions refused"
    )
    args = parser.parse_args()
    try:
        success = smoke(args.output)
    except (OSError, ValueError, importlib.metadata.PackageNotFoundError) as exc:
        print(f"Known-truth integration unavailable/failed: {exc}", file=sys.stderr)
        return 2
    print(json.dumps({"success": success, "evidence": str(args.output)}))
    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())
