"""Freeze disjoint blocks, explicit configurations and balanced method orders."""

from __future__ import annotations

import math
import random
from pathlib import Path

from .. import integrity
from ..config import DownloadConfig
from ..http import load_input_file
from .common import file_hash, read_json, require, write_new

METHODS = ("paarc", "paarc-ratio", "img2dataset")


def validate(protocol):
    required = {
        "name",
        "source",
        "blocks",
        "conditions",
        "methods",
        "seed",
        "timeout_s",
        "body_limit_bytes",
        "access_qualified",
        "media_rights_qualified",
    }
    allowed = required | {"phase", "baseline_s", "ignore_time", "user_cooldown_seconds", "selection"}
    require(isinstance(protocol, dict) and required <= set(protocol) <= allowed, "invalid protocol fields")
    require(
        protocol["source"] in ("openimages-cvdf", "inaturalist-open", "taibif-moth", "loopback"),
        "unknown source",
    )
    for name in ("blocks", "timeout_s", "body_limit_bytes"):
        require(type(protocol[name]) is int and protocol[name] > 0, f"{name} must be positive")
    require(type(protocol["seed"]) is int, "seed must be an integer")
    require(
        protocol.get("phase", "evaluation") in ("calibration", "evaluation", "engineering"), "unknown phase"
    )
    for name in ("access_qualified", "media_rights_qualified"):
        require(type(protocol[name]) is bool, f"{name} must be boolean")
    require(isinstance(protocol["name"], str) and protocol["name"].strip(), "name must be nonempty")
    require(isinstance(protocol["conditions"], list) and protocol["conditions"], "empty conditions")
    require(isinstance(protocol["methods"], list) and protocol["methods"], "empty methods")
    for condition in protocol["conditions"]:
        require(
            isinstance(condition, dict)
            and set(condition) == {"rows", "workers"}
            and type(condition["rows"]) is int
            and condition["rows"] > 0
            and type(condition["workers"]) is int
            and 1 <= condition["workers"] <= 64,
            "conditions require positive rows and 1..64 workers",
        )
        require(condition["rows"] >= condition["workers"], "each worker requires an input row")
        require(protocol["body_limit_bytes"] >= condition["workers"], "each worker requires a byte budget")
    require(
        len({integrity.encode(c) for c in protocol["conditions"]}) == len(protocol["conditions"]),
        "duplicate conditions",
    )
    require(
        len({integrity.encode(m) for m in protocol["methods"]}) == len(protocol["methods"]),
        "duplicate method configurations",
    )
    for name, default, zero_ok in [("baseline_s", 0.1, False), ("user_cooldown_seconds", 0, True)]:
        value = protocol.get(name, default)
        require(
            type(value) in (int, float) and math.isfinite(value) and (value >= 0 if zero_ok else value > 0),
            f"{name} must be finite and {'nonnegative' if zero_ok else 'positive'}",
        )
    require(type(protocol.get("ignore_time", False)) is bool, "ignore_time must be boolean")
    require(
        not protocol.get("ignore_time") or protocol["source"] == "taibif-moth",
        "the certificate-date exception is restricted to taibif-moth",
    )
    if "selection" in protocol:
        rule = protocol["selection"]
        require(
            protocol.get("phase") == "calibration"
            and isinstance(rule, dict)
            and set(rule) == {"minimum_http_s", "coverage_tolerance", "goodput_tie_fraction"},
            "selection requires an explicit calibration rule",
        )
        require(
            all(type(v) in (int, float) and math.isfinite(v) and v >= 0 for v in rule.values()),
            "selection limits must be finite and nonnegative",
        )
        require(
            rule["minimum_http_s"] > 0
            and rule["coverage_tolerance"] <= 1
            and rule["goodput_tie_fraction"] < 1,
            "invalid selection limits",
        )
    for entry in protocol["methods"]:
        require(
            isinstance(entry, dict) and entry.get("method") in METHODS, "explicit supported method required"
        )
        require(
            set(entry) <= {"method", "c_max", "method_options", "processes", "threads"},
            "unknown method configuration",
        )
        if entry["method"] != "img2dataset":
            DownloadConfig.from_dict({"input_path": "manifest", "output_dir": "output", **entry})
        else:
            require(
                set(entry) == {"method", "processes", "threads"}
                and all(type(entry[k]) is int and entry[k] > 0 for k in ["processes", "threads"]),
                "invalid comparator configuration",
            )
    return protocol


def prepare(protocol_path, input_path, output_dir, exclude_study=None):
    protocol = validate(read_json(protocol_path))
    source, root = Path(input_path), Path(output_dir)
    require(not root.exists(), "study already exists; preserve first attempts")
    raw = source.read_bytes()
    frame = load_input_file(str(source))
    require(source.read_bytes() == raw, "source manifest changed")
    require("url" in frame.columns, "experiments require the url column")
    import polars as pl

    require(frame.schema["url"] == pl.String, "experiment URLs must be strings")
    require(
        frame["url"].null_count() == 0 and frame["url"].n_unique() == frame.height,
        "experiment URLs must be nonnull and distinct",
    )
    from .guard import checked_url

    for url in frame["url"]:
        checked_url(url, protocol["source"])
    frame = integrity.stamp_frame(frame, integrity.digest(raw))
    exclusions = None
    if exclude_study is not None:
        from .runner import freeze

        old_root, _, old_freeze = freeze(exclude_study)
        excluded = set()
        for cell in old_freeze["cells"]:
            excluded.update(load_input_file(str(old_root / cell["input"]))["url"])
        frame = frame.filter(~frame["url"].is_in(list(excluded)))
        exclusions = {"freeze_sha256": file_hash(old_root / "freeze.json"), "distinct_urls": len(excluded)}
    size = max(c["rows"] for c in protocol["conditions"])
    require(frame.height >= size * protocol["blocks"], "insufficient population for disjoint blocks")
    indices = list(range(frame.height))
    rng = random.Random(protocol["seed"])
    rng.shuffle(indices)
    root.mkdir(parents=True)
    (root / "inputs").mkdir()
    (root / "runs").mkdir()
    write_new(root / "protocol.json", protocol)
    cells = []
    orders = {}
    for block in range(protocol["blocks"]):
        selected = indices[block * size : (block + 1) * size]
        for condition_index, condition in enumerate(protocol["conditions"]):
            path = root / "inputs" / f"b{block}-c{condition_index}.parquet"
            frame[selected[: condition["rows"]]].write_parquet(path)
            count = len(protocol["methods"])
            if block % count == 0:
                orders[condition_index] = list(range(count))
                rng.shuffle(orders[condition_index])
            base = orders[condition_index]
            shift = block % count
            order = base[shift:] + base[:shift]
            for method_index in order:
                cells.append(
                    {
                        "run_id": f"b{block}-c{condition_index}-m{method_index}",
                        "block": block,
                        "condition": condition_index,
                        "rows": condition["rows"],
                        "workers": condition["workers"],
                        "configuration": protocol["methods"][method_index],
                        "method_index": method_index,
                        "phase": protocol.get("phase", "evaluation"),
                        "input": str(path.relative_to(root)),
                        "input_sha256": file_hash(path),
                    }
                )
    freeze = {
        "schema": "flowdc-study-v1",
        "protocol_sha256": file_hash(root / "protocol.json"),
        "population_sha256": integrity.digest(raw),
        "exclusions": exclusions,
        "cells": cells,
    }
    write_new(root / "freeze.json", freeze)
    return freeze
