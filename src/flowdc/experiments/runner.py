"""Execute frozen first attempts; failures remain visible and never replay automatically."""

from __future__ import annotations

import fcntl
import os
import shutil
import signal
import subprocess
import sys
import time
from dataclasses import asdict
from pathlib import Path

from .. import integrity
from ..config import DownloadConfig
from ..distributed import run_distributed, verify_partition
from .common import file_hash, read_json, require, write_new
from .metrics import transport
from .protocol import validate


def freeze(study):
    root = Path(study).resolve()
    protocol = validate(read_json(root / "protocol.json"))
    frozen = read_json(root / "freeze.json")
    require(frozen["protocol_sha256"] == file_hash(root / "protocol.json"), "frozen protocol changed")
    cells = frozen["cells"]
    require(frozen["schema"] == "flowdc-study-v1", "unknown study schema")
    expected = {
        f"b{b}-c{c}-m{m}"
        for b in range(protocol["blocks"])
        for c in range(len(protocol["conditions"]))
        for m in range(len(protocol["methods"]))
    }
    require(
        len(cells) == len(expected) and {c["run_id"] for c in cells} == expected,
        "frozen matrix differs from the protocol",
    )
    for cell in cells:
        block, condition, method = cell["block"], cell["condition"], cell["method_index"]
        require(cell["run_id"] == f"b{block}-c{condition}-m{method}", "cell identity differs")
        require(
            cell["configuration"] == protocol["methods"][method]
            and {k: cell[k] for k in ("rows", "workers")} == protocol["conditions"][condition]
            and cell["phase"] == protocol.get("phase", "evaluation"),
            "frozen configuration differs",
        )
        require(cell["input"] == f"inputs/b{block}-c{condition}.parquet", "invalid frozen input path")
        require(file_hash(root / cell["input"]) == cell["input_sha256"], "frozen block changed")
    return root, protocol, frozen


def run(study, *, taskvine=None, comparator_python=None, limit=None):
    require(limit is None or type(limit) is int and limit > 0, "limit must be a positive integer")
    root = Path(study).resolve()
    with (root / ".run.lock").open("a+b") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ValueError("another runner owns this study") from exc
        return _run(root, taskvine=taskvine, comparator_python=comparator_python, limit=limit)


def _run(study, *, taskvine, comparator_python, limit):
    root, protocol, frozen = freeze(study)
    require(
        protocol["access_qualified"] and protocol["media_rights_qualified"],
        "source access and media rights must be explicitly qualified",
    )
    results = []
    for cell in frozen["cells"]:
        directory = root / "runs" / cell["run_id"]
        if directory.exists():
            continue  # An interrupted attempt still consumes its slot.
        if limit is not None and len(results) >= limit:
            break
        directory.mkdir()
        write_new(directory / "attempt.json", cell)
        source = root / cell["input"]
        shutil.copy2(source, directory / "input.parquet")
        entry = cell["configuration"]
        method = entry["method"]
        record = {
            **cell,
            "status": "stopped",
            "credited_bytes": 0,
            "goodput_Bps": None,
            "coverage": None,
            "protocol_sha256": frozen["protocol_sha256"],
        }
        started = time.monotonic()
        try:
            options = {k: v for k, v in entry.items() if k != "method"} if method != "img2dataset" else {}
            config = DownloadConfig.from_dict(
                {
                    "input_path": str(directory / "input.parquet"),
                    "input_format": "parquet",
                    "output_dir": str(directory / "native"),
                    "method": method if method != "img2dataset" else "paarc",
                    "c_max": entry["processes"] * entry["threads"]
                    if method == "img2dataset"
                    else entry.get("c_max", 192),
                    "research_profile": True,
                    "max_attempts": 1,
                    **(
                        {"c_min": 1, "c_init": min(4, entry["processes"] * entry["threads"])}
                        if method == "img2dataset"
                        else {}
                    ),
                    **options,
                }
            )
            context = {
                "source": protocol["source"],
                "body_limit_bytes": protocol["body_limit_bytes"],
                "baseline_s": protocol.get("baseline_s", 0.1),
                "ignore_time": protocol.get("ignore_time", False),
                "user_cooldown_seconds": protocol.get("user_cooldown_seconds", 0),
            }
            tool = (
                {**context, "processes": entry["processes"], "threads": entry["threads"]}
                if method == "img2dataset"
                else None
            )
            if tool:
                require(
                    comparator_python is not None and Path(comparator_python).is_file(),
                    "isolated img2dataset 1.47.0 interpreter is required",
                )
            if taskvine:
                settings = {**taskvine, "workers": cell["workers"], "timeout_s": protocol["timeout_s"]}
                if tool:
                    settings["python_executable"] = comparator_python
                result = run_distributed(
                    DownloadConfig.from_dict(
                        {**asdict(config), "output_dir": str(directory / "distributed")}
                    ),
                    settings,
                    tool=tool,
                    experiment=context,
                )
                require(result["execution_complete"], "distributed execution incomplete")
                record["distributed_transport"] = [p["transport"] for p in result["partitions"]]
                verification = {
                    "original_rows": result["original_rows"],
                    "credited_bytes": result["credited_bytes"],
                    "coverage": sum(r["counts"]["verified"] for r in result["partitions"])
                    / result["original_rows"],
                    "payloads": {
                        key: value for part in result["partitions"] for key, value in part["payloads"].items()
                    },
                }
            else:
                require(cell["workers"] == 1, "multiworker condition requires TaskVine configuration")
                launch = {"download": asdict(config), "experiment": context, "tool": tool}
                write_new(directory / "launch.json", launch)
                executable = comparator_python if tool else sys.executable
                from ..distributed import bundle_package

                bundle_package(directory / "application.zip")
                with (directory / "stdout.log").open("x") as log:
                    process = subprocess.Popen(
                        [executable, "-B", "-m", "flowdc.experiments.launch", "launch.json"],
                        cwd=directory,
                        env={
                            **os.environ,
                            "PYTHONPATH": str(directory / "application.zip"),
                            "POLARS_MAX_THREADS": "8",
                            "OPENBLAS_NUM_THREADS": "1",
                            "NO_ALBUMENTATIONS_UPDATE": "1",
                            "WANDB_MODE": "disabled",
                        },
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        start_new_session=True,
                    )
                    try:
                        code = process.wait(timeout=protocol["timeout_s"])
                    finally:
                        if process.poll() is None:
                            os.killpg(process.pid, signal.SIGTERM)
                            try:
                                process.wait(timeout=30)
                            except subprocess.TimeoutExpired:
                                os.killpg(process.pid, signal.SIGKILL)
                                process.wait()
                record["native_seconds"] = time.monotonic() - started
                require(code == 0, "native client failed; inspect retained stdout.log")
                destination = directory / "return" if tool else directory / "native"
                if not tool:
                    from ..verification import verify_output

                    verify_output(destination)
                verification = verify_partition(
                    destination,
                    directory / "input.parquet",
                    archive_path=directory / "native.tar" if not tool else None,
                    decode_images=True,
                )
                metrics = transport(
                    directory / "request-audit.jsonl",
                    entry["threads"] * entry["processes"] if tool else config.c_max,
                )
                record["transport"] = metrics
            elapsed = time.monotonic() - started
            require(verification["original_rows"] == cell["rows"], "verified denominator differs")
            record.update(
                status="complete",
                credited_bytes=verification["credited_bytes"],
                coverage=verification["coverage"],
                elapsed_s=elapsed,
                goodput_Bps=verification["credited_bytes"] / elapsed,
                verification=verification,
            )
        except Exception as error:
            record.update(error=f"{type(error).__name__}: {error}", elapsed_s=time.monotonic() - started)
            from .metrics import partial_transport

            record["partial_transport"] = partial_transport(directory, protocol["body_limit_bytes"])
        record["evidence"] = {
            str(p.relative_to(directory)): file_hash(p)
            for p in directory.rglob("*")
            if p.is_file()
            and p.suffix in (".json", ".jsonl", ".parquet", ".tar", ".zip")
            and p.name != "record.json"
        }
        write_new(directory / "record.json", record)
        results.append(record)
    return results


def analyze(study):
    import math
    import statistics

    from scipy.stats import t

    root, protocol, frozen = freeze(study)
    inventory = []
    for cell in frozen["cells"]:
        directory = root / "runs" / cell["run_id"]
        path = directory / "record.json"
        if path.exists():
            record = read_json(path)
            require(all(record.get(k) == v for k, v in cell.items()), "record differs from its frozen cell")
            require(read_json(directory / "attempt.json") == cell, "first-attempt identity differs")
            require(record["protocol_sha256"] == frozen["protocol_sha256"], "record protocol differs")
            for name, digest in record["evidence"].items():
                require(
                    not Path(name).is_absolute() and ".." not in Path(name).parts, "unsafe evidence reference"
                )
                require(file_hash(directory / name) == digest, "retained experiment evidence changed")
            if record["status"] == "complete":
                require(
                    math.isclose(
                        record["goodput_Bps"], record["credited_bytes"] / record["elapsed_s"], rel_tol=1e-12
                    ),
                    "goodput arithmetic differs",
                )
            inventory.append(record)
        else:
            inventory.append(
                {**cell, "status": "interrupted" if directory.exists() else "unstarted", "goodput_Bps": None}
            )
    contrasts = []
    for method in ["paarc-ratio", "paarc"]:
        for condition in range(len(protocol["conditions"])):
            pairs = []
            changed_blocks = []
            for block in range(protocol["blocks"]):
                rows = [
                    r
                    for r in inventory
                    if r["condition"] == condition and r["block"] == block and r["status"] == "complete"
                ]
                a = [r for r in rows if r["configuration"]["method"] == method]
                b = [r for r in rows if r["configuration"]["method"] == "img2dataset"]
                # Calibration grids are selected explicitly, never pooled as replication.
                if len(a) == len(b) == 1 and a[0]["goodput_Bps"] > 0 and b[0]["goodput_Bps"] > 0:
                    require(a[0]["input_sha256"] == b[0]["input_sha256"], "matched inputs differ")
                    first = a[0]["verification"]["payloads"]
                    second = b[0]["verification"]["payloads"]
                    changed = [
                        key
                        for key in first.keys() & second.keys()
                        if first[key]["sha256"] != second[key]["sha256"]
                    ]
                    if changed:
                        changed_blocks.append({"block": block, "changed_common_payloads": len(changed)})
                    else:
                        pairs.append(math.log(a[0]["goodput_Bps"] / b[0]["goodput_Bps"]))
            result = {
                "method": method,
                "condition": condition,
                "pairs": len(pairs),
                "changed_blocks": changed_blocks,
                "geometric_ratio": math.exp(statistics.mean(pairs)) if pairs else None,
                "interval95": None,
            }
            if len(pairs) > 1:
                half = float(t.ppf(0.975, len(pairs) - 1)) * statistics.stdev(pairs) / math.sqrt(len(pairs))
                mean = statistics.mean(pairs)
                result["interval95"] = [math.exp(mean - half), math.exp(mean + half)]
            contrasts.append(result)
    result = {
        "inventory": inventory,
        "contrasts": contrasts,
        "interpretation": "Run-level exploratory paired intervals; incomplete attempts retained; no superiority assumption",
    }
    output = root / "analysis"
    output.mkdir(exist_ok=True)
    (output / "results.json").write_bytes(integrity.encode(result))
    return result


def verify(study):
    """Repeat archive, image, provenance and journal verification without new requests."""
    root, _, frozen = freeze(study)
    result = analyze(study)
    checked = 0
    for cell, record in zip(frozen["cells"], result["inventory"], strict=True):
        if record["status"] != "complete":
            continue
        directory = root / "runs" / cell["run_id"]
        distributed = directory / "distributed"
        if distributed.exists():
            actual = []
            for i in range(cell["workers"]):
                actual.append(
                    verify_partition(
                        distributed / f"part-{i}",
                        distributed / "inputs" / f"part-{i:04d}.parquet",
                        decode_images=True,
                    )
                )
            credited = sum(p["credited_bytes"] for p in actual)
            coverage = sum(p["counts"]["verified"] for p in actual) / cell["rows"]
            payloads = {key: value for part in actual for key, value in part["payloads"].items()}
        else:
            tool = cell["configuration"]["method"] == "img2dataset"
            if not tool:
                from ..verification import verify_output

                verify_output(directory / "native")
            value = verify_partition(
                directory / ("return" if tool else "native"),
                directory / "input.parquet",
                archive_path=None if tool else directory / "native.tar",
                decode_images=True,
            )
            credited, coverage, payloads = value["credited_bytes"], value["coverage"], value["payloads"]
        require(
            credited == record["credited_bytes"]
            and coverage == record["coverage"]
            and payloads == record["verification"]["payloads"],
            "recomputed scientific credit differs",
        )
        checked += 1
    return {
        "verified_completed_attempts": checked,
        "unavailable_attempts": len(result["inventory"]) - checked,
        "freeze_sha256": file_hash(root / "freeze.json"),
    }


def select(study):
    """Apply a frozen calibration rule; never silently treat tuning as evaluation."""
    import math
    import statistics

    root, protocol, _ = freeze(study)
    require(
        protocol.get("phase") == "calibration" and "selection" in protocol,
        "selection requires a frozen calibration protocol",
    )
    verification = verify(study)
    inventory = analyze(study)["inventory"]
    rule = protocol["selection"]
    candidates, selected = [], []
    for condition in range(len(protocol["conditions"])):
        group = []
        for index, configuration in enumerate(protocol["methods"]):
            rows = [r for r in inventory if r["condition"] == condition and r["method_index"] == index]
            metrics = []
            for row in rows:
                transport_records = (
                    [row["transport"]]
                    if "transport" in row
                    else [p.get("transport") for p in row.get("verification", {}).get("partitions", [])]
                )
                # Distributed records keep each worker's observation boundary explicit.
                if row.get("distributed_transport"):
                    transport_records = row["distributed_transport"]
                if transport_records and all(t is not None for t in transport_records):
                    metrics.append(
                        (
                            min(t["http_span_s"] for t in transport_records),
                            sum(t["mean_http_concurrency"] for t in transport_records),
                        )
                    )
            eligible = (
                len(rows) == protocol["blocks"]
                and all(r["status"] == "complete" for r in rows)
                and len(metrics) == len(rows)
                and all(m[0] >= rule["minimum_http_s"] for m in metrics)
                and all(r["goodput_Bps"] > 0 for r in rows)
            )
            candidate = {
                "condition": condition,
                "method_index": index,
                "configuration": configuration,
                "eligible": eligible,
                "minimum_coverage": min((r.get("coverage") or 0 for r in rows), default=0),
                "geometric_goodput_Bps": math.exp(statistics.mean(math.log(r["goodput_Bps"]) for r in rows))
                if eligible
                else None,
                "mean_http_concurrency": statistics.mean(m[1] for m in metrics) if eligible else None,
            }
            group.append(candidate)
        for method in sorted({m["method"] for m in protocol["methods"]}):
            entries = [c for c in group if c["eligible"] and c["configuration"]["method"] == method]
            if not entries:
                selected.append(
                    {
                        "condition": condition,
                        "method": method,
                        "configuration": None,
                        "reason": "no complete duration-qualified candidate",
                    }
                )
                continue
            coverage = max(c["minimum_coverage"] for c in entries)
            entries = [c for c in entries if c["minimum_coverage"] >= coverage - rule["coverage_tolerance"]]
            best = max(c["geometric_goodput_Bps"] for c in entries)
            tied = [
                c for c in entries if c["geometric_goodput_Bps"] >= best * (1 - rule["goodput_tie_fraction"])
            ]
            winner = min(tied, key=lambda c: (c["mean_http_concurrency"], c["method_index"]))
            selected.append({"condition": condition, "method": method, **winner})
        candidates.extend(group)
    result = {
        "freeze_sha256": verification["freeze_sha256"],
        "rule": rule,
        "candidates": candidates,
        "selected": selected,
        "interpretation": "Calibration only; freeze a separate evaluation protocol and exclude these URLs",
    }
    (root / "analysis" / "selection.json").write_bytes(integrity.encode(result))
    return result
