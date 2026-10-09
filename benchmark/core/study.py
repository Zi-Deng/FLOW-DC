"""Frozen ordering, isolated namespaces and conservative run-level inference."""

import math
import random
import statistics
import sys
from pathlib import Path
from types import SimpleNamespace

from .truth import digest, encode, parse, require, workload

METHODS = ("paarc-base-v2", "gradient-candidate-v1", "fixed-v1", "ratio-v1", "gradient2-application-delay-v1")
LEGACY_METHODS = METHODS[:-1]
FAMILIES = ("steady", "drop-recovery", "overload")


def method_configs(configurations=None, *, legacy=False):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "bin"))
    from flowdc_methods import MethodConfig

    methods = (
        configurations
        if configurations is not None
        else {
            method: {
                "control_method": method,
                "C_min": 2,
                "C_init": 4,
                "C_max": 16,
                "method_options": {"sample_window_s": 1.5}
                if method in ("gradient-candidate-v1", "ratio-v1")
                else {},
            }
            for method in (LEGACY_METHODS if legacy else METHODS)
        }
    )
    required = LEGACY_METHODS if legacy else METHODS
    require(isinstance(methods, dict) and set(methods) == set(required), "all named comparators required")
    for name, config in methods.items():
        require(
            isinstance(config, dict)
            and set(config) == {"control_method", "C_min", "C_init", "C_max", "method_options"}
            and config["control_method"] == name
            and isinstance(config["method_options"], dict),
            "invalid method configuration",
        )
        MethodConfig.from_config(SimpleNamespace(**config))
    require(
        len({(config["C_min"], config["C_max"]) for config in methods.values()}) == 1,
        "comparators require common absolute bounds",
    )
    return methods


def tuning_catalog(*, research_workload=None):
    proposals = {}
    limits = workload(research_workload)
    for method, base in method_configs(legacy=research_workload is None).items():
        variants = []
        for i in range(8):
            config = {**base, "method_options": dict(base["method_options"])}
            if method in ("paarc-base-v2", "fixed-v1"):
                config["C_init"] = (2, 3, 4, 5, 6, 8, 12, 16)[i]
            elif method == "gradient-candidate-v1":
                config["method_options"] = {
                    **base["method_options"],
                    "queue_tau_s": (0.25, 0.5, 1, 2)[i // 2],
                    "gradient_tau_s": (0.25, 0.5, 1, 2)[i // 2],
                    "sample_min": (3, 5)[i % 2],
                }
            elif method == "ratio-v1":
                config["method_options"] = {
                    **base["method_options"],
                    "ratio_buffer_fraction": (0.05, 0.1, 0.2, 0.3)[i // 2],
                    "ratio_headroom": (1, 2)[i % 2],
                }
            else:
                config["method_options"] = {"queue_size": (0, 1, 2, 4)[i // 2], "smoothing": (.1, .2)[i % 2]}
            variants.append(
                {"id": f"tuning-{method}-{i}", "config": config, "sha256": digest(encode(config))}
            )
        proposals[method] = variants
    result = {
        "schema": "flowdc-tuning-catalog-v1",
        "namespace": "tuning",
        "advisor_decisions": "pending_specific_decisions",
        "status": "proposal_only",
        "candidates": proposals,
        "evaluation_rule": "freeze selected configs before opening evaluation outcomes",
    }
    if research_workload is not None:
        result.update(schema="flowdc-tuning-catalog-v2", workload=limits.record())
    return result


def make_plan(
    *,
    seed,
    blocks=6,
    families=None,
    namespace="evaluation",
    purpose="provisional-pilot",
    rows=128,
    configurations=None,
    research_workload=None,
):
    require(type(seed) is int and 0 <= seed < 2**32, "seed must be a uint32")
    require(type(blocks) is int and 1 <= blocks <= 100, "block count must be 1..100")
    require(namespace in ("tuning", "evaluation", "engineering"), "unknown study namespace")
    require(purpose in ("engineering", "provisional-pilot", "confirmatory"), "unknown study purpose")
    require((purpose == "engineering") == (namespace == "engineering"), "engineering needs its own namespace")
    limits = workload(research_workload)
    if research_workload is not None and purpose == "confirmatory":
        require(blocks <= 60, "research confirmation is limited to 60 frozen blocks")
    from .controlled_origin import SCENARIOS, RESEARCH_SCENARIOS

    if families is None:
        families = FAMILIES if research_workload is None else ("drop-recovery", "mixed-sizes", "sustained-overload")

    require(
        families and len(set(families)) == len(families)
        and set(families) <= set(SCENARIOS if research_workload is None else RESEARCH_SCENARIOS),
        "unknown/duplicate scenario family",
    )
    row_map = rows if isinstance(rows, dict) else {family: rows for family in families}
    require((not isinstance(rows, dict) or research_workload is not None)
            and set(row_map) == set(families)
            and all(type(value) is int and 1 <= value <= limits.max_rows for value in row_map.values()),
            "rows must fit the finite workload and name every scenario")
    methods = method_configs(configurations, legacy=research_workload is None)
    rng, cells = random.Random(seed), []
    for block in range(blocks):
        ordered_families = list(families)
        rng.shuffle(ordered_families)
        for family in ordered_families:
            ordered_methods = list(methods)
            rng.shuffle(ordered_methods)
            for method in ordered_methods:
                cells.append(
                    {
                        "cell_id": f"{namespace}-b{block:03d}-{family}-{method}",
                        "block": block,
                        "scenario": family,
                        "method": method,
                        "fixture_seed": int(digest(encode([namespace, seed, block]))[:8], 16),
                        "rows": row_map[family],
                        "config_sha256": digest(encode(methods[method])),
                    }
                )
    result = {
        "schema": "flowdc-study-plan-v1",
        "seed": seed,
        "blocks": blocks,
        "families": list(families),
        "namespace": namespace,
        "purpose": purpose,
        "methods": methods,
        "cells": cells,
        "replicate": "independent process/run; paired within scenario and block",
        "advisor_decisions": "pending_specific_decisions",
        "proposal": {
            "tuning_candidates_per_method": 8,
            "blocks": 6,
            "families": 3,
            "confidence": 0.95,
            "relative_half_width": 0.05,
            "approved": False,
        },
        "limits": {
            "worker_processes": 1,
            "rows": 256,
            "expected_payload_bytes": 67108864,
            "process_deadline_s": 180,
            "cleanup_reserve_s": 60,
        },
    }
    if research_workload is not None:
        result.update(schema="flowdc-study-plan-v2", workload=limits.record())
        result["limits"].update(rows=limits.max_rows, expected_payload_bytes=limits.max_payload_bytes,
                                process_deadline_s=limits.acquisition_seconds,
                                metadata_bytes=limits.max_metadata_bytes, artifact_bytes=limits.max_artifact_bytes)
        if isinstance(rows, dict):
            result['rows_by_scenario'] = dict(rows)
    return result


def validate_plan(plan):
    require(plan.get("schema") in ("flowdc-study-plan-v1", "flowdc-study-plan-v2"), "unsupported study plan")
    rebuilt = make_plan(
        seed=plan["seed"],
        blocks=plan["blocks"],
        families=plan["families"],
        namespace=plan["namespace"],
        purpose=plan["purpose"],
        rows=plan.get('rows_by_scenario', plan["cells"][0]["rows"]),
        configurations=plan["methods"],
        research_workload=plan.get("workload", {}).get("name"),
    )
    require(plan == rebuilt, "study plan/order/configuration changed; generate a distinct plan")


def freeze_protocol(plan, decisions, *, source_sha256, environment_sha256):
    """Caller supplies explicit human decision provenance, never shipped approval."""
    validate_plan(plan)
    require(plan["purpose"] != "engineering", "engineering checks do not freeze a scientific protocol")
    expected = {"approved_plan_sha256", "provenance", "constraints", "estimand", "repetition_rule"}
    require(isinstance(decisions, dict) and expected <= set(decisions) <= expected | {"approval_authority"},
            "explicit scientific decisions required")
    authority = decisions.get("approval_authority", "advisor")
    require(authority in ("advisor", "maintainer-provisional"), "unknown protocol authority")
    require(decisions["approved_plan_sha256"] == digest(encode(plan)), "decisions bind a different plan")
    for name in expected - {"approved_plan_sha256"}:
        require(
            isinstance(decisions[name], str) and len(decisions[name].strip()) >= 12,
            "specific advisor decision/provenance text required",
        )
    for value in (source_sha256, environment_sha256):
        require(
            isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value),
            "invalid protocol hash",
        )
    return {
        "schema": "flowdc-frozen-protocol-v2" if plan["schema"] == "flowdc-study-plan-v2" else "flowdc-frozen-protocol-v1",
        "plan_sha256": digest(encode(plan)),
        "decisions": decisions,
        "decisions_sha256": digest(encode(decisions)),
        "source_sha256": source_sha256,
        "environment_sha256": environment_sha256,
        "status": "frozen",
        "authority": ("user-supplied advisor decision provenance, not automatic approval"
                      if authority == "advisor" else "maintainer-approved provisional defaults; advisor decisions pending"),
    }


def authorize_plan(plan, protocol, *, source_sha256, environment_sha256):
    validate_plan(plan)
    if plan["purpose"] == "engineering":
        require(
            plan["blocks"] == 1, "engineering smoke is one block; broader campaigns require protocol freeze"
        )
        return
    require(isinstance(protocol, dict), "non-engineering campaign requires explicit frozen protocol")
    expected = freeze_protocol(
        plan, protocol.get("decisions"), source_sha256=source_sha256, environment_sha256=environment_sha256
    )
    require(protocol == expected, "frozen protocol/source/environment mismatch")


def _beta_fraction(a, b, x):
    # Modified Lentz evaluation of the incomplete-beta continued fraction.
    tiny, epsilon = 1e-300, 3e-14
    c, d = 1.0, 1 - (a + b) * x / (a + 1)
    d = 1 / (d if abs(d) > tiny else tiny)
    value = d
    for m in range(1, 301):
        for coefficient in (
            m * (b - m) * x / ((a + 2 * m - 1) * (a + 2 * m)),
            -(a + m) * (a + b + m) * x / ((a + 2 * m) * (a + 2 * m + 1)),
        ):
            d = 1 + coefficient * d
            d = 1 / (d if abs(d) > tiny else tiny)
            c = 1 + coefficient / c
            c = c if abs(c) > tiny else tiny
            change = d * c
            value *= change
        if abs(change - 1) < epsilon:
            return value
    raise ValueError("precision calculation failed to converge")


def _regularized_beta(x, a, b):
    if x <= 0:
        return 0.0
    if x >= 1:
        return 1.0
    front = math.exp(
        math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b) + a * math.log(x) + b * math.log1p(-x)
    )
    if x < (a + 1) / (a + b + 2):
        return front * _beta_fraction(a, b, x) / a
    return 1 - front * _beta_fraction(b, a, 1 - x) / b


def t_critical(n, confidence=0.95):
    require(type(n) is int and 2 <= n <= 10000 and confidence == 0.95, "95% Student-t requires 2..10000 runs")
    df, target = n - 1, (1 + confidence) / 2

    def cdf(value):
        return 1 - 0.5 * _regularized_beta(df / (df + value * value), df / 2, 0.5)

    lo, hi = 0.0, 128.0
    for _ in range(70):
        mid = (lo + hi) / 2
        if cdf(mid) < target:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2


def paired_summary(pairs, *, minimum=6):
    """Every planned pair remains present; no favorable-result deletion."""
    require(type(minimum) is int and minimum >= 2, "minimum pairs must be at least two")
    records, values, references = list(pairs), [], []
    undefined, censored = False, False
    for pair in records:
        censored |= bool(pair.get("censored"))
        for key in ("reference", "candidate"):
            value = pair.get(key)
            if value is None:
                undefined = True
            else:
                require(
                    type(value) in (int, float) and math.isfinite(value) and value >= 0, "invalid run metric"
                )
        if pair.get("reference") is not None and pair.get("candidate") is not None:
            references.append(pair["reference"])
            values.append(pair["candidate"] - pair["reference"])
    result = {
        "schema": "flowdc-paired-summary-v1",
        "planned_pairs": len(records),
        "pairs": records,
        "replicate": "independent paired runs",
        "confidence": 0.95,
        "minimum_pairs": minimum,
        "ci": None,
        "mean_difference": None,
        "reference_mean": None,
        "sd_difference": None,
    }
    if undefined:
        return {**result, "status": "undefined_cells_present"}
    if values:
        result.update(mean_difference=statistics.mean(values), reference_mean=statistics.mean(references))
    if censored:
        return {**result, "status": "censored_cells_present"}
    if len(values) < minimum:
        return {**result, "status": "insufficient_pairs"}
    sd = statistics.stdev(values)
    half = t_critical(len(values)) * sd / math.sqrt(len(values))
    result.update(
        sd_difference=sd,
        half_width=half,
        ci=[result["mean_difference"] - half, result["mean_difference"] + half],
        status="estimated",
    )
    return result


def precision_plan(summary, *, relative_half_width=0.05, max_pairs=10000):
    require(
        type(relative_half_width) in (int, float)
        and math.isfinite(relative_half_width)
        and 0 < relative_half_width < 1,
        "invalid relative half-width",
    )
    require(type(max_pairs) is int and 6 <= max_pairs <= 10000, "max_pairs must be 6..10000")
    base = {
        "schema": "flowdc-precision-plan-v1",
        "confidence": 0.95,
        "target_relative_half_width": relative_half_width,
        "max_pairs": max_pairs,
        "required_total_pairs": None,
        "approval": "proposal_only",
        "rule": "plug-in paired-run SD, Student-t 95%; plan once, no favorable-result stopping",
    }
    if summary["status"] != "estimated":
        return {**base, "status": summary["status"]}
    if summary["reference_mean"] <= 0:
        return {**base, "status": "zero_reference_mean"}
    target, sd = relative_half_width * summary["reference_mean"], summary["sd_difference"]
    minimum = max(6, summary["planned_pairs"])
    if minimum > max_pairs or t_critical(max_pairs) * sd / math.sqrt(max_pairs) > target:
        return {**base, "status": "infeasible_within_bound"}
    lo, hi = minimum, max_pairs
    while lo < hi:
        mid = (lo + hi) // 2
        if t_critical(mid) * sd / math.sqrt(mid) <= target:
            hi = mid
        else:
            lo = mid + 1
    return {
        **base,
        "status": "proposal",
        "required_total_pairs": lo,
        "absolute_half_width_target": target,
        "assumptions": "independent representative paired runs; future variance similar; approximate normal paired means; tiny pilot SD uncertain",
    }


def write_new(path, record):
    path = Path(path)
    with path.open("xb") as stream:
        stream.write(encode(record))


def read_plan(path):
    plan = parse(Path(path).read_bytes())
    validate_plan(plan)
    return plan
