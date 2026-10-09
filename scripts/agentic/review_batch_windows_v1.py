"""Finite batch9 windows and lossless catalog accounting.

Plans are not readiness. Actual qualification/publication adapters must independently
replay children before any stopped boundary can be sealed or resumed.
"""

from __future__ import annotations

import copy
import hashlib
import math
import re
from pathlib import Path

from claude_reporting_execution import exclusive
from reporting_activation_v2 import read
from tasks import digest, plain_path, private_directory
from workflow import WorkflowError

LIMITS = {
    "components": 48,
    "component_ids": 128,
    "component_bytes": 500000,
    "component_lines": 9000,
    "items": 1700,
    "bytes": 6800000,
    "lines": 140000,
    "native_seconds": 900,
    "local_seconds": 840,
    "reference_usd": 10,
    "setup_seconds": 900,
    "final_seconds_per_child": 180,
    "pause_seconds": 1800,
    "expiry_seconds": 129600,
    "outer_expiry_seconds": 172800,
    "margin_seconds": 360,
    "paid_extra_usd": 0,
    "api_usd": 0,
}


def refuse(message):
    raise WorkflowError("Batch9 windows: " + message)


def integer(value, minimum, maximum):
    if type(value) is not int or not minimum <= value <= maximum:
        refuse("invalid finite integer")
    return value


def clock(value):
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
        refuse("invalid clock")
    return value


def checksum(value):
    if type(value) is not str or re.fullmatch("[0-9a-f]{64}", value) is None:
        refuse("invalid digest")
    return value


def schedule(components):
    """Name and fund the whole suffix before any first-window reservation."""
    if (
        type(components) is not list
        or not 1 <= len(components) <= 48
        or any(
            type(x) is not str
            or re.fullmatch("[a-z][a-z0-9-]{0,79}", x) is None
            or x in {"integration", "final-validation"}
            for x in components
        )
        or components != sorted(set(components))
    ):
        refuse("components must be unique, lexical and bounded")
    windows = [components[i : i + 6] for i in range(0, len(components), 6)]
    windows += [["integration"], ["final-validation"]]
    children = len(components) + 1
    active = 900 + children * (900 + 840 + 180)
    pauses = 1800 * (len(windows) - 1)
    result = {
        "schema_version": 1,
        "windows": windows,
        "processes": children,
        "native_seconds": children * 900,
        "reference_usd": children * 10,
        "paid_extra_usd": 0,
        "api_usd": 0,
        "active_seconds": active,
        "pause_seconds": pauses,
        "wall_seconds": active + pauses,
        "expiry_seconds": 129600,
        "outer_expiry_seconds": 172800,
        "window_seconds": [len(w) * 1740 + 360 + (900 if i == 0 else 0) for i, w in enumerate(windows[:-1])]
        + [children * 180 + 360],
    }
    if active > 94980 or active + pauses > 111180 or len(windows) > 10:
        refuse("whole allocation exceeds approved ceiling")
    return result


def validate_plan(plan):
    if type(plan) is not dict or set(plan) != {
        "schema_version",
        "catalog",
        "catalog_digest",
        "schedule",
        "limits",
    }:
        refuse("unknown plan fields")
    if type(plan["schema_version"]) is not int or plan["schema_version"] != 9:
        refuse("unsupported plan version")
    catalog = plan["catalog"]
    validate_catalog(catalog)
    if (
        plan["catalog_digest"] != digest(catalog)
        or digest(plan["limits"]) != digest(LIMITS)
        or digest(plan["schedule"]) != digest(schedule([u["id"] for u in catalog["components"]]))
    ):
        refuse("catalog, whole funding or limits changed")
    from claude_reporting import _json_bytes

    try:
        _json_bytes(plan, 2000000)
    except ValueError:
        refuse("whole plan exceeds unchanged record bound before claim")
    return plan


def context_reference(all_ids, primary_ids):
    """Lossless shared context: all catalog IDs except this unit's primary IDs.

    Binding the common set once by digest avoids repeating up to 1,700 IDs for
    each unit inside the unchanged 2 MB record limit. No material is removed.
    """
    return {"all_ids_sha256": digest(sorted(all_ids)), "excluded_ids": sorted(primary_ids)}


def surrounding_ids(catalog, unit):
    validate_catalog(catalog)
    if not any(
        digest(unit) == digest(candidate) for candidate in catalog["components"] + [catalog["integration"]]
    ):
        refuse("unknown or changed context owner")
    return sorted({item["id"] for item in catalog["items"]} - set(unit["required_ids"]))


def validate_catalog(catalog):
    if type(catalog) is not dict or set(catalog) != {
        "binding",
        "items",
        "components",
        "integration",
        "files",
    }:
        refuse("catalog fields differ")
    if type(catalog["binding"]) is not dict or not catalog["binding"]:
        refuse("missing source/provenance binding")
    for value in catalog["binding"].values():
        checksum(value)
    items = catalog["items"]
    if type(items) is not list or not 1 <= len(items) <= 1700:
        refuse("inventory bounds")
    lookup = {}
    for item in items:
        if (
            type(item) is not dict
            or set(item) != {"id", "family", "artifact", "start_line", "end_line", "bytes", "sha256", "kind"}
            or type(item["id"]) is not str
            or re.fullmatch("[0-9a-f]{24}", item["id"]) is None
            or item["id"] in lookup
            or type(item["family"]) is not str
            or not item["family"]
        ):
            refuse("invalid or duplicate primary item")
        if type(item["kind"]) is not str or item["kind"] not in {
            "source",
            "changed-source",
            "contract",
            "acceptance",
            "policy",
            "validation",
            "finding",
            "findings",
            "diff",
            "inventory",
            "test-mapping",
            "test",
            "prior-material",
            "repair",
            "cross-boundary",
        }:
            refuse("unknown primary material kind")
        integer(item["start_line"], 1, 140000)
        integer(item["end_line"], item["start_line"], 140000)
        integer(item["bytes"], 1, 6800000)
        checksum(item["sha256"])
        if (
            type(item["artifact"]) is not str
            or str(Path(item["artifact"])) != item["artifact"]
            or Path(item["artifact"]).is_absolute()
            or ".." in Path(item["artifact"]).parts
        ):
            refuse("unsafe material path")
        lookup[item["id"]] = item
    if (
        sum(i["bytes"] for i in items) > 6800000
        or sum(i["end_line"] - i["start_line"] + 1 for i in items) > 140000
    ):
        refuse("whole catalog volume")
    if type(catalog["files"]) is not dict or not catalog["files"]:
        refuse("missing source file bindings")
    for name, value in catalog["files"].items():
        if (
            type(name) is not str
            or not name
            or Path(name).is_absolute()
            or ".." in Path(name).parts
            or str(Path(name)) != name
            or "\\" in name
        ):
            refuse("invalid source file")
        checksum(value)
    if any(catalog["files"].get(item["artifact"]) != item["sha256"] for item in items):
        refuse("primary material file hash differs")
    seen, families = set(), {}
    components = catalog["components"]
    if type(components) is not list or any(type(u) is not dict or "id" not in u for u in components):
        refuse("invalid component catalog")
    schedule([u["id"] for u in components])
    for unit in components + [catalog["integration"]]:
        if type(unit) is not dict or set(unit) != {"id", "required_ids", "context_ids"}:
            refuse("unit fields differ")
        ids, context = unit["required_ids"], unit["context_ids"]
        if (
            type(ids) is not list
            or not ids
            or len(ids) != len(set(ids))
            or set(ids) - lookup.keys()
            or seen.intersection(ids)
            or digest(context) != digest(context_reference(lookup.keys(), ids))
        ):
            refuse("missing, duplicate or inaccessible surrounding material")
        rows = [lookup[key] for key in ids]
        if (
            len(ids) + (len(components) if unit["id"] == "integration" else 0) > 128
            or sum(i["bytes"] for i in rows) > 500000
        ):
            refuse("unit IDs or bytes exceed bounds")
        if unit["id"] != "integration" and sum(i["end_line"] - i["start_line"] + 1 for i in rows) > 9000:
            refuse("component lines exceed bounds")
        for item in rows:
            if (item["kind"] == "cross-boundary") != (unit["id"] == "integration"):
                refuse("cross-boundary ownership changed")
            if item["family"] in families and families[item["family"]] != unit["id"]:
                refuse("coherent family was split")
            families[item["family"]] = unit["id"]
        seen.update(ids)
    if catalog["integration"]["id"] != "integration" or seen != lookup.keys():
        refuse("incomplete primary partition")


def plan_catalog(catalog):
    validate_catalog(catalog)
    result = {
        "schema_version": 9,
        "catalog": copy.deepcopy(catalog),
        "catalog_digest": digest(catalog),
        "schedule": schedule([u["id"] for u in catalog["components"]]),
        "limits": copy.deepcopy(LIMITS),
    }

    return validate_plan(result)


def application(plan, qualification_applied, now, *, qualification_finished):
    """Pure immutable accounting; this does not apply a grant or invoke a model."""
    validate_current_plan(plan)
    start, now = clock(qualification_applied), clock(now)
    finished = clock(qualification_finished)
    if not start <= finished <= start + 7380 or not finished <= now <= finished + 1800:
        refuse("qualification/interphase window expired")
    return {
        "schema_version": 1,
        "plan_digest": digest(plan),
        "applied_at": now,
        "deadline": min(now + 129600, start + 172800),
        "wall_deadline": min(
            now + plan["schedule"]["wall_seconds"], start + 120360, now + 129600, start + 172800
        ),
        "qualification_applied": start,
        "qualification_finished": finished,
        "active_seconds": plan["schedule"]["active_seconds"],
        "pause_seconds": plan["schedule"]["pause_seconds"],
    }


def validate_application(plan, record):
    expected = application(
        plan,
        record.get("qualification_applied"),
        record.get("applied_at"),
        qualification_finished=record.get("qualification_finished"),
    )
    if digest(record) != digest(expected):
        refuse("application changed or torn")


def seal(plan, applied, window, children, now):
    """Seal independently recomputed child receipts, never readiness booleans.

    The execution adapter must supply exact complete receipts from independent
    qualification/publication/claim replay. This pure encoder grants no authority.
    """
    validate_application(plan, applied)
    integer(window, 0, len(plan["schedule"]["windows"]) - 2)
    now = clock(now)
    if not applied["applied_at"] <= now < applied["wall_deadline"]:
        refuse("seal clock/expiry")
    expected = [u for w in plan["schedule"]["windows"][: window + 1] for u in w]
    if type(children) is not list or [c.get("unit") for c in children] != expected:
        refuse("incomplete or reordered published prefix")
    for child in children:
        if type(child) is not dict or set(child) != {
            "unit",
            "binding",
            "claim",
            "report",
            "publication",
            "execution",
            "capture",
            "observer",
            "usage",
        }:
            refuse("partial child receipt")
        for key in set(child) - {"unit", "usage"}:
            checksum(child[key])
        if child["binding"] != plan["catalog_digest"]:
            refuse("copied child belongs to another catalog")
        from reporting_recovery_history_v6 import known_usage

        if not known_usage(child["usage"], 900, 10):
            refuse("unknown or excessive child usage")
    remaining = [u for w in plan["schedule"]["windows"][window + 1 :] for u in w]
    return {
        "schema_version": 1,
        "application_digest": digest(applied),
        "window": window,
        "sealed_at": now,
        "children": copy.deepcopy(children),
        "remaining": remaining,
        "remaining_reference_usd": 10 * len([u for u in remaining if u != "final-validation"]),
        "resume_before": min(now + 1800, applied["wall_deadline"]),
    }


def resume(plan, applied, paused, children, now):
    """Recompute the entire sealed prefix before considering the next window."""
    if digest(paused) != digest(seal(plan, applied, paused.get("window"), children, paused.get("sealed_at"))):
        refuse("pause seal/imports changed")
    now = clock(now)
    next_window = paused["window"] + 1
    required = plan["schedule"]["window_seconds"][next_window]
    if not paused["sealed_at"] <= now <= min(paused["resume_before"], applied["wall_deadline"] - required):
        refuse("next whole window does not fit or pause expired")
    return {
        "schema_version": 1,
        "pause_digest": digest(paused),
        "resumed_at": now,
        "window": next_window,
        "required_seconds": required,
    }


def claim(directory, plan, applied):
    """Exclusive local application claim; uncertain/torn files are never replaced.

    This storage primitive is not exposed by the CLI until the catalog, funding,
    global claims, current admission and owned-window consumers are complete.
    """
    validate_application(plan, applied)
    directory = plain_path(directory)
    exclusive(directory / "windows-application.json", applied)
    exclusive(directory / "windows-plan.json", plan, limit=2000000)


def load(directory):
    directory = plain_path(directory)
    plan, applied = read(directory / "windows-plan.json"), read(directory / "windows-application.json")
    validate_application(plan, applied)
    return plan, applied


# Explicit interface/version families; never pack unrelated items to reach a quota.
FAMILIES = {
    ".agents/skills/agentic-review/SKILL.md": "operating-guides",
    ".github/workflows/agentic-quality.yml": "runner",
    "acceptance": "contract",
    "agentic:archives": "archive-install",
    "agentic:batch_fixtures": "batch-legacy",
    "agentic:capacity_native_v1": "capacity-native",
    "agentic:check": "runner",
    "agentic:check_runner": "runner",
    "agentic:claude_context_observation_v1": "context-observation",
    "agentic:claude_credentials": "native-auth",
    "agentic:claude_diagnostics": "diagnostic-legacy",
    "agentic:claude_execution_v7": "owned-execution",
    "agentic:claude_fixtures": "report-codec",
    "agentic:claude_native_auth": "native-auth",
    "agentic:claude_normalization": "native-tool-rendering",
    "agentic:claude_owned_auth": "native-auth",
    "agentic:claude_partial_observation": "observation-v1",
    "agentic:claude_partial_observation_v2": "report-stream-v8",
    "agentic:claude_reporting": "report-codec",
    "agentic:claude_reporting_execution": "owned-execution",
    "agentic:claude_reporting_policy": "report-policy",
    "agentic:claude_reporting_policy_v8": "report-policy",
    "agentic:claude_reporting_versions": "report-policy",
    "agentic:claude_telemetry": "native-telemetry-legacy",
    "agentic:claude_telemetry_v6": "native-telemetry-v6",
    "agentic:claude_telemetry_v7": "report-stream-v7",
    "agentic:claude_telemetry_v7_observed": "report-stream-v8",
    "agentic:claude_telemetry_v7_observed_v1": "observation-v1",
    "agentic:claude_telemetry_v8": "report-stream-v8",
    "agentic:claude_v4": "native-telemetry-legacy",
    "agentic:claude_v5": "native-telemetry-legacy",
    "agentic:claude_v6": "native-telemetry-v6",
    "agentic:claude_v6_sources": "native-tool-rendering",
    "agentic:claude_v7": "report-stream-v7",
    "agentic:claude_v8": "report-stream-v8",
    "agentic:configuration": "governance",
    "agentic:context_observation_v1": "context-observation",
    "agentic:coverage_regressions": "coverage-current",
    "agentic:diagnostic_recovery": "diagnostic-legacy",
    "agentic:diagnostic_recovery_v5": "diagnostic-legacy",
    "agentic:diagnostic_recovery_v6": "diagnostic-v6-v8",
    "agentic:diagnostic_recovery_v7": "diagnostic-v6-v8",
    "agentic:diagnostic_recovery_v8": "diagnostic-v6-v8",
    "agentic:finish": "finish",
    "agentic:github_transport": "pipeline",
    "agentic:packet_tree": "packet",
    "agentic:partial_observation": "observation-v1",
    "agentic:partial_observation_v2": "report-stream-v8",
    "agentic:pipeline": "pipeline",
    "agentic:process_groups": "owned-execution",
    "agentic:recovery_cli": "diagnostic-v6-v8",
    "agentic:reporting_activation": "reporting-v1",
    "agentic:reporting_activation_v2": "reporting-v2",
    "agentic:reporting_activation_v3": "reporting-v3",
    "agentic:reporting_activation_v4": "reporting-v4",
    "agentic:reporting_activation_v5": "reporting-v5",
    "agentic:reporting_activation_v6": "reporting-v6",
    "agentic:reporting_admission": "current-admission",
    "agentic:reporting_admission_v1": "reporting-v1",
    "agentic:reporting_admission_v2": "reporting-v2",
    "agentic:reporting_admission_v3": "reporting-v3",
    "agentic:reporting_admission_v4": "reporting-v4",
    "agentic:reporting_admission_v6": "reporting-v6",
    "agentic:reporting_authority_v5": "reporting-v5",
    "agentic:reporting_cli": "current-admission",
    "agentic:reporting_cli_v2": "reporting-v2",
    "agentic:reporting_cli_v3": "reporting-v3",
    "agentic:reporting_cli_v4": "reporting-v4",
    "agentic:reporting_cli_v6": "reporting-v6",
    "agentic:reporting_consumers": "current-admission",
    "agentic:reporting_diagnostic": "reporting-v1",
    "agentic:reporting_diagnostic_v2": "reporting-v2",
    "agentic:reporting_diagnostic_v3": "reporting-v3",
    "agentic:reporting_diagnostic_v4": "reporting-v4",
    "agentic:reporting_diagnostic_v5": "reporting-v5",
    "agentic:reporting_diagnostic_v6": "reporting-v6",
    "agentic:reporting_history_v3": "reporting-v3",
    "agentic:reporting_history_v5": "reporting-v5",
    "agentic:reporting_owned_auth": "owned-execution",
    "agentic:reporting_preflight": "owned-execution",
    "agentic:reporting_preflight_v5": "owned-execution",
    "agentic:reporting_qualification_v6": "reporting-v6",
    "agentic:reporting_recovery_history": "reporting-v1",
    "agentic:reporting_recovery_history_v3": "reporting-v3",
    "agentic:reporting_recovery_history_v4": "reporting-v4",
    "agentic:reporting_recovery_history_v5": "reporting-v5",
    "agentic:reporting_recovery_history_v6": "reporting-v6",
    "agentic:reporting_recovery_v2": "reporting-v2",
    "agentic:reporting_recovery_v3": "reporting-v3",
    "agentic:reporting_recovery_v3_boundaries": "reporting-v3",
    "agentic:reporting_recovery_v4": "reporting-v4",
    "agentic:reporting_recovery_v4_boundaries": "reporting-v4",
    "agentic:reporting_recovery_v5": "reporting-v5",
    "agentic:reporting_recovery_v5_boundaries": "reporting-v5",
    "agentic:reporting_versions": "current-admission",
    "agentic:review": "review-storage",
    "agentic:review_batch": "batch-legacy",
    "agentic:review_batch_v4": "batch-legacy",
    "agentic:review_batch_v7": "batch-reporting",
    "agentic:review_batch_windows_v1": "windows",
    "agentic:review_capacity": "prompt-capacity",
    "agentic:review_capacity_native_v1": "capacity-native",
    "agentic:review_claims": "continuation",
    "agentic:review_claude": "owned-execution",
    "agentic:review_continuation": "continuation",
    "agentic:review_copilot": "copilot-legacy",
    "agentic:review_coverage": "coverage-current",
    "agentic:review_coverage_issue31_v3": "coverage-issue31",
    "agentic:review_coverage_v5": "coverage-v5-v6",
    "agentic:review_coverage_v6": "coverage-v5-v6",
    "agentic:review_diagnostics": "coverage-current",
    "agentic:review_issue31_v3": "coverage-issue31",
    "agentic:review_lifetime": "continuation",
    "agentic:review_navigation": "projection-navigation",
    "agentic:review_packet": "packet",
    "agentic:review_policy": "prompt-capacity",
    "agentic:review_projection": "projection-navigation",
    "agentic:review_prompt": "prompt-capacity",
    "agentic:review_report_material_v1": "report-material",
    "agentic:review_telemetry": "copilot-legacy",
    "agentic:review_telemetry_issue31_v3": "copilot-legacy",
    "agentic:review_telemetry_v5": "copilot-legacy",
    "agentic:review_telemetry_v6": "copilot-legacy",
    "agentic:review_windows_v1": "windows",
    "agentic:reviewer_installation": "archive-install",
    "agentic:sessions": "governance",
    "agentic:tasks": "governance",
    "agentic:workflow": "governance",
    "changed-files.json": "inventory-validation",
    "docs/agent-workflow/COVERAGE.md": "coverage-guide",
    "docs/agent-workflow/FINISH.md": "finish",
    "docs/agent-workflow/OPERATING-GUIDE.md": "operating-guides",
    "docs/agent-workflow/PROVIDERS.md": "provider-guide",
    "docs/agent-workflow/REVIEW.md": "operating-guides",
    "docs/agent-workflow/SETUP.md": "runner",
    "docs/agent-workflow/SKILLS.md": "operating-guides",
    "issue.txt": "contract",
    "plan.txt": "contract",
    "policy": "contract",
    "tests/agentic/fixtures/claude-catalog-2.1.282.json": "report-policy",
    "tests/agentic/fixtures/claude-controls-2.1.282-v4.json": "native-telemetry-v6",
    "tests/agentic/fixtures/claude-grep-2.1.282-v6.json": "native-tool-rendering",
    "tests/agentic/fixtures/claude-grep-2.1.282-v6.mjs": "native-tool-rendering",
    "tests/agentic/fixtures/claude-grep-normalization-2.1.282-v6.json": "native-tool-rendering",
    "tests/agentic/fixtures/claude-grep-normalization-2.1.282-v6.mjs": "native-tool-rendering",
    "tests/agentic/fixtures/claude-read-2.1.282.json": "native-tool-rendering",
    "tests/agentic/fixtures/claude-read-input-2.1.282.json": "native-tool-rendering",
    "tests/agentic/fixtures/claude-refusal-2.1.282-v5.json": "native-telemetry-legacy",
    "tests/agentic/fixtures/claude-refusal-2.1.282-v5.mjs": "native-telemetry-legacy",
    "tests/agentic/fixtures/claude-reporting-2.1.282-source.json": "report-codec",
    "tests/agentic/fixtures/claude-reporting-controls-2.1.282-source.json": "report-codec",
    "tests/agentic/fixtures/claude-reporting-schema-2.1.282-source.json": "report-codec",
    "tests/agentic/fixtures/claude-status-2.1.282.json": "report-policy",
    "tests/agentic/fixtures/claude-transport-2.1.282-v6-expected.json": "native-telemetry-v6",
    "tests/agentic/fixtures/claude-transport-2.1.282-v6.json": "native-telemetry-v6",
    "tests/agentic/fixtures/claude-transport-2.1.282-v6.mjs": "native-telemetry-v6",
    "tests/agentic/fixtures/copilot-1.0.83-view-canary.json": "copilot-legacy",
    "tests/test_integrity_review.py": "product-integrity-tests",
    "tests/test_integrity_second_review.py": "product-integrity-tests",
    "tests/test_second_review.py": "product-integrity-tests",
    "tests/test_shared_review.py": "product-integrity-tests",
    "validation.json": "inventory-validation",
}


def partition(packet, inventory, binding):
    """Measure actual ranges and keep every primary ID and surrounding file."""
    import hashlib

    packet = plain_path(packet)
    if type(inventory) is not list or not inventory:
        refuse("missing complete inventory")
    files = {}
    for path in packet.rglob("*"):
        if path.is_symlink():
            refuse("packet symlink")
        if path.is_file():
            files[str(path.relative_to(packet))] = hashlib.sha256(path.read_bytes()).hexdigest()
    stems = {Path(i["path"]).stem for i in inventory if i["path"].startswith("scripts/agentic/")}

    def family(item):
        path = item["path"]
        if item["kind"] == "cross-boundary":
            return "integration"
        lineage = {
            **dict.fromkeys(
                ("pr_comments:6006702232", "pr_comments:6007203047", "pr_comments:6007480835"),
                "dispositions-admission",
            ),
            **dict.fromkeys(
                ("pr_comments:6008052335", "pr_comments:6008468809", "pr_comments:6008944198"),
                "dispositions-owned-recovery",
            ),
            **dict.fromkeys(
                ("pr_comments:6009404762", "pr_comments:6010267718", "pr_comments:6010751855"),
                "dispositions-v3-v4",
            ),
        }
        if path in lineage:
            return lineage[path]
        stem = Path(path).stem.removeprefix("test_")
        if path.startswith("tests/agentic/") and path.endswith(".py"):
            stem = max((s for s in stems if stem == s or stem.startswith(s + "_")), key=len, default=stem)
        key = (
            "agentic:" + stem
            if path.startswith(("scripts/agentic/", "tests/agentic/")) and path.endswith(".py")
            else path
        )
        if item["kind"] == "finding":
            # A whole response is indivisible; never distribute its fragments.
            body = (packet / item["artifact"]).read_text(encoding="utf-8")
            counts = {}
            for candidate in inventory:
                if candidate["kind"] not in {"finding", "cross-boundary"} and candidate["path"] in body:
                    owner = family(candidate)
                    counts[owner] = counts.get(owner, 0) + 1
            return min(counts, key=lambda key: (-counts[key], key)) if counts else "cross-cutting-findings"
        return FAMILIES.get(key, "family-" + digest(key)[:16])

    items, owners = [], {}
    for item in inventory:
        if item.get("omitted") or not item.get("artifact"):
            refuse("omitted/binary/unknown material remains incomplete")
        artifact = item["artifact"]
        if artifact not in files:
            refuse("missing required artifact")
        raw = (packet / artifact).read_bytes()
        try:
            lines = raw.decode("utf-8").splitlines(keepends=True)
        except UnicodeError:
            refuse("non-UTF8 mandatory material")
        lo, hi = item["start_line"], item["end_line"]
        integer(lo, 1, len(lines))
        integer(hi, lo, len(lines))
        size = len("".join(lines[lo - 1 : hi]).encode("utf-8"))
        if item.get("bytes") != size or type(item.get("bytes")) is not int:
            refuse("range size differs from actual source")
        if item.get("projection"):
            # Current frozen source projections remain independently validated.
            import review_projection

            if not review_projection.validate(packet, item):
                refuse("source projection differs")
        owner = family(item)
        owners.setdefault(owner, []).append(item["id"])
        items.append(
            {
                "id": item["id"],
                "family": owner,
                "artifact": artifact,
                "start_line": lo,
                "end_line": hi,
                "bytes": size,
                "sha256": hashlib.sha256(raw).hexdigest(),
                "kind": item["kind"],
            }
        )
    all_ids = {i["id"] for i in items}
    units = [
        {"id": key, "required_ids": ids, "context_ids": context_reference(all_ids, ids)}
        for key, ids in sorted(owners.items())
        if key != "integration"
    ]
    cross = owners.get("integration", [])
    value = {
        "binding": binding,
        "items": items,
        "components": units,
        "integration": {
            "id": "integration",
            "required_ids": cross,
            "context_ids": context_reference(all_ids, cross),
        },
        "files": files,
    }
    validate_catalog(value)
    return value


def runner_request(root, jobs, *, suite_profile=None):
    """Fresh interpreter avoids importing installed tests from the source checkout."""
    import subprocess
    import sys
    import tempfile

    if suite_profile not in (None, "issue31-suite1800-v1"):
        refuse("unknown runner descriptor profile")
    code = """import json, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
import check_runner as runner
root, jobs = Path(sys.argv[2]), int(sys.argv[3])
source = runner.source(root)
suite, rows, objects, errors = runner.discover(root)
policy, assignments = runner.assignment_policy(root, suite, rows, objects, source, jobs, suite_profile=sys.argv[5] if len(sys.argv) == 6 else None)
value = dict(version=runner.VERSION, jobs=jobs, source=source, rows=rows,
             assignments=assignments, assignment_policy=policy, errors=errors,
             evidence_limit=runner.EVIDENCE_BYTES)
if len(sys.argv) == 6:
    value["version"] = runner.SUITE_VERSION
    value["execution_limits"] = runner.execution_limits(sys.argv[5], 1800, runner.TEXT_BYTES, runner.EVIDENCE_BYTES)
Path(sys.argv[4]).write_text(json.dumps(value, allow_nan=False))
"""
    with tempfile.TemporaryDirectory(prefix="agentic-descriptor-") as temporary:
        target = Path(temporary) / "request.json"
        result = subprocess.run(
            [sys.executable, "-B", "-c", code, str(Path(__file__).parent), str(root), str(jobs), str(target)]
            + ([suite_profile] if suite_profile is not None else []),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=180,
        )
        if result.returncode:
            refuse("fresh complete descriptor reconstruction failed")
        value = read(target)
        if value["errors"]:
            refuse("fresh complete discovery failed")
        return value


def suite_summary(directory, request, jobs):
    import check_runner

    value = read(directory / "summary.json")
    limits = check_runner.execution_limits(
        "issue31-suite1800-v1", 1800, check_runner.TEXT_BYTES, check_runner.EVIDENCE_BYTES
    )
    if (
        type(value) is not dict
        or type(value.get("version")) is not int
        or value["version"] != 3
        or request.get("version") != 3
        or digest(request.get("execution_limits")) != digest(limits)
        or digest(value.get("execution_limits")) != digest(limits)
        or value.get("request_digest") != check_runner.digest(request)
        or type(value.get("jobs")) is not int
        or value["jobs"] != jobs
        or value.get("successful") is not True
        or value.get("error") is not None
        or type(value.get("process_exits")) is not list
        or len(value["process_exits"]) != jobs
        or any(type(code) is not int or code != 0 for code in value["process_exits"])
        or type(value.get("elapsed_seconds")) not in (int, float)
        or not 0 <= value["elapsed_seconds"] <= 1800
        or type(value.get("occurrences")) is not int
        or value["occurrences"] != len(request["rows"])
    ):
        refuse("incomplete or wrong-deadline suite summary")


def full_checks(repo, directory, meta):
    return _full_checks(repo, directory, meta, suite_profile=None)


def full_checks_g14(repo, directory, meta):
    import reporting_activation_v6 as activation

    current = activation.authorization(repo)
    if current["contract_digest"] != activation.G14_CONTRACT_DIGEST or meta["plan_comment"] != 6061320190:
        refuse("generation14 full gates require current literal authority")
    result = _full_checks(repo, directory, meta, suite_profile="issue31-suite1800-v1")
    if activation.authorization(repo) != current:
        refuse("full gate authority changed")
    return result


def full_checks_g15(repo, directory, meta):
    import reporting_activation_v6 as activation

    current = activation.authorization(repo)
    if current["contract_digest"] != activation.G15_CONTRACT_DIGEST or meta["plan_comment"] != 6062530466:
        refuse("generation15 full gates require current literal authority")
    result = _full_checks(repo, directory, meta, suite_profile="issue31-suite1800-v1")
    if activation.authorization(repo) != current:
        refuse("full gate authority changed")
    return result


def _full_checks(repo, directory, meta, *, suite_profile):
    """Validate retained local execution records and fresh hosted associations.

    Local artifacts are owner-writable bookkeeping, not execution attestations.
    Hosted receipts are independently fetched through the existing collector.
    No test runner, installer or provider is invoked by this read-only adapter.
    """
    import hashlib

    import check_runner
    import ci_evidence

    evidence = read(plain_path(directory) / "full-checks.json")
    commands = {
        "serial": "python3 -B scripts/agentic/check.py --jobs 1",
        "parallel": "make check-agentic",
        "full": "make check",
        "clean": "make check-clean",
        "installed": "installed-full-suite",
        "lint": "ruff check",
        "format": "ruff format --check",
        "repository": "python3 -B scripts/check_repository.py",
    }
    if suite_profile is not None:
        commands["serial"] += " --suite-profile issue31-suite1800-v1"
        for name in ("parallel", "full"):
            commands[name] += " AGENTIC_SUITE_PROFILE=issue31-suite1800-v1"
    current = check_runner.source(repo.root)
    if (
        type(evidence) is not dict
        or set(evidence) != {"head", "source", "commands", "installed_files"}
        or evidence["head"] != meta["head_sha"]
        or evidence["source"] != current
        or type(evidence["commands"]) is not dict
        or set(evidence["commands"]) != set(commands)
    ):
        refuse("full check evidence is missing, stale or partial")
    for name, command in commands.items():
        record = evidence["commands"][name]
        if (
            type(record) is not dict
            or set(record) != {"command", "exit_status", "artifacts"}
            or record["command"] != command
            or type(record["exit_status"]) is not int
            or record["exit_status"] != 0
            or type(record["artifacts"]) is not dict
            or not record["artifacts"]
        ):
            refuse("required command provenance differs")
        for relative, expected in record["artifacts"].items():
            if type(relative) is not str or Path(relative).is_absolute() or ".." in Path(relative).parts:
                refuse("unsafe check evidence path")
            checksum(expected)
            if hashlib.sha256(plain_path(directory / relative).read_bytes()).hexdigest() != expected:
                refuse("check artifact changed")
    # Both standard runner records must reconcile every occurrence and worker.
    rows = None
    for name, jobs in [("serial", 1), ("parallel", 2)]:
        request = read(directory / name / "request.json")
        expected = runner_request(repo.root, jobs, suite_profile=suite_profile)
        if expected["source"] != current:
            refuse("source changed during descriptor reconstruction")
        rows = expected["rows"]
        if digest(request) != digest(expected):
            refuse("complete runner request differs from current discovery")
        if suite_profile is not None:
            suite_summary(directory / name, request, jobs)
        for index in range(jobs):
            result = check_runner.reconcile(
                request, index, plain_path(directory / name / f"worker-{index}.jsonl"), 0
            )
            if result["successful"] is not True:
                refuse("incomplete runner occurrence or fixture execution")
    installed = evidence["installed_files"]
    # Require the whole workflow payload, not an import-only or selected-file smoke.
    import install

    expected_payload = {
        str(p): hashlib.sha256((repo.root / p).read_bytes()).hexdigest() for p in install.payload(repo.root)
    }
    if type(installed) is not dict or installed != expected_payload:
        refuse("installed payload/source closure differs")
    installed_root = plain_path(directory / "installed-root")
    manifest = read(installed_root / ".agentic/template-origin.json")
    if digest(manifest) != digest(
        {"schema_version": 1, "source": "agentic-github-template", "files": expected_payload}
    ):
        refuse("installed origin differs")
    retained_payload = {}
    for path in installed_root.rglob("*"):
        if path.is_symlink():
            refuse("installed symlink")
        if path.is_file():
            relative = str(path.relative_to(installed_root))
            if relative != ".agentic/template-origin.json":
                retained_payload[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
    if retained_payload != expected_payload:
        refuse("retained installed payload changed or contains extra/private files")
    installed_source = check_runner.source(installed_root)
    expected_installed = runner_request(installed_root, 1, suite_profile=suite_profile)
    if expected_installed["source"] != installed_source or digest(expected_installed["rows"]) != digest(rows):
        refuse("installed discovery differs from the complete source suite")
    installed_request = read(directory / "installed" / "request.json")
    if digest(installed_request) != digest(expected_installed):
        refuse("installed complete runner request differs")
    if suite_profile is not None:
        suite_summary(directory / "installed", installed_request, 1)
        installed_argv = read(directory / "installed" / "argv.json")
        if installed_argv != [
            "python3",
            "-B",
            "scripts/agentic/check.py",
            "--jobs",
            "1",
            "--suite-profile",
            suite_profile,
        ]:
            refuse("installed actual suite command differs")
    if (
        check_runner.reconcile(installed_request, 0, plain_path(directory / "installed/worker-0.jsonl"), 0)[
            "successful"
        ]
        is not True
    ):
        refuse("installed execution incomplete")
    checks = repo.api(
        f"commits/{meta['head_sha']}/check-runs?per_page=100", paginate=True, page_key="check_runs"
    )
    receipts = ci_evidence.collect(repo, meta["head_sha"], checks, meta["base_sha"])
    for name in ("flowdc-tests", "agentic-quality"):
        found = [r for r in receipts if r.get("check") == name]
        if len(found) != 1:
            refuse("missing or ambiguous hosted execution")
        receipt = found[0]
        if (
            receipt.get("state") != "observed"
            or receipt.get("run_attempt") != 1
            or type(receipt.get("run_attempt")) is not int
            or receipt.get("test_status") != "success"
            or receipt.get("clean_status") != "success"
            or receipt.get("pr_head_sha") != meta["head_sha"]
            or receipt.get("pr_base_sha") != meta["base_sha"]
        ):
            refuse("hosted first-attempt test/clean evidence incomplete")
        # Preserve actual merge checkout separately from the head association.
        if type(receipt.get("tested_checkout_sha")) is not str or not re.fullmatch(
            "[0-9a-f]{40}", receipt["tested_checkout_sha"]
        ):
            refuse("unknown hosted checkout")
    return {"local": digest(evidence), "hosted": digest(receipts), "source": digest(current)}


def full_checks_g19(repo, directory, meta):
    import check_runner
    import reporting_activation_v6 as activation

    current = activation.authorization(repo)
    before = check_runner.source(repo.root)
    if (
        current["contract_digest"] != activation.G19_CONTRACT_DIGEST
        or type(meta["plan_comment"]) is not int
        or meta["plan_comment"] != 6072111969
    ):
        refuse("generation19 full gates require current literal authority")
    result = _full_checks_g19(repo, directory, meta, suite_profile="issue31-suite1800-v1")
    if activation.authorization(repo) != current or check_runner.source(repo.root) != before:
        refuse("full gate authority or source changed")
    return result


def _full_checks_g19(repo, directory, meta, *, suite_profile):
    """Validate retained local execution records and fresh hosted associations.

    Local artifacts are owner-writable bookkeeping, not execution attestations.
    Hosted receipts are independently fetched through the existing collector.
    No test runner, installer or provider is invoked by this read-only adapter.
    """
    import hashlib

    import check_runner
    import ci_evidence

    evidence = read(plain_path(directory) / "full-checks.json")
    commands = {
        "serial": "python3 -B scripts/agentic/check.py --jobs 1",
        "parallel": "make check-agentic",
        "full": "make check",
        "clean": "make check-clean",
        "installed": "installed-full-suite",
        "lint": "ruff check",
        "format": "ruff format --check",
        "repository": "python3 -B scripts/check_repository.py",
    }
    if suite_profile is not None:
        commands["serial"] += " --suite-profile issue31-suite1800-v1"
        for name in ("parallel", "full"):
            commands[name] += " AGENTIC_SUITE_PROFILE=issue31-suite1800-v1"
    current = check_runner.source(repo.root)
    if (
        type(evidence) is not dict
        or set(evidence) != {"head", "source", "commands", "installed_files"}
        or evidence["head"] != meta["head_sha"]
        or evidence["source"] != current
        or type(evidence["commands"]) is not dict
        or set(evidence["commands"]) != set(commands)
    ):
        refuse("full check evidence is missing, stale or partial")
    for name, command in commands.items():
        record = evidence["commands"][name]
        if (
            type(record) is not dict
            or set(record) != {"command", "exit_status", "artifacts"}
            or record["command"] != command
            or type(record["exit_status"]) is not int
            or record["exit_status"] != 0
            or type(record["artifacts"]) is not dict
            or not record["artifacts"]
        ):
            refuse("required command provenance differs")
        for relative, expected in record["artifacts"].items():
            if type(relative) is not str or Path(relative).is_absolute() or ".." in Path(relative).parts:
                refuse("unsafe check evidence path")
            checksum(expected)
            if hashlib.sha256(plain_path(directory / relative).read_bytes()).hexdigest() != expected:
                refuse("check artifact changed")
    # Both standard runner records must reconcile every occurrence and worker.
    rows = None
    for name, jobs in [("serial", 1), ("parallel", 2)]:
        request = read(directory / name / "request.json")
        expected = runner_request(repo.root, jobs, suite_profile=suite_profile)
        if expected["source"] != current:
            refuse("source changed during descriptor reconstruction")
        rows = expected["rows"]
        if digest(request) != digest(expected):
            refuse("complete runner request differs from current discovery")
        if suite_profile is not None:
            suite_summary(directory / name, request, jobs)
        for index in range(jobs):
            result = check_runner.reconcile(
                request, index, plain_path(directory / name / f"worker-{index}.jsonl"), 0
            )
            if result["successful"] is not True:
                refuse("incomplete runner occurrence or fixture execution")
    import install

    expected_payload = {
        str(p): hashlib.sha256((repo.root / p).read_bytes()).hexdigest() for p in install.payload(repo.root)
    }
    if digest(evidence["installed_files"]) != digest(expected_payload):
        refuse("current installed payload closure differs")
    validate_installed_adoption(repo.root, directory, rows)
    checks = repo.api(
        f"commits/{meta['head_sha']}/check-runs?per_page=100", paginate=True, page_key="check_runs"
    )
    receipts = ci_evidence.collect(repo, meta["head_sha"], checks, meta["base_sha"])
    for name in ("flowdc-tests", "agentic-quality"):
        found = [r for r in receipts if r.get("check") == name]
        if len(found) != 1:
            refuse("missing or ambiguous hosted execution")
        receipt = found[0]
        if (
            receipt.get("state") != "observed"
            or receipt.get("run_attempt") != 1
            or type(receipt.get("run_attempt")) is not int
            or receipt.get("test_status") != "success"
            or receipt.get("clean_status") != "success"
            or receipt.get("pr_head_sha") != meta["head_sha"]
            or receipt.get("pr_base_sha") != meta["base_sha"]
        ):
            refuse("hosted first-attempt test/clean evidence incomplete")
        # Preserve actual merge checkout separately from the head association.
        if type(receipt.get("tested_checkout_sha")) is not str or not re.fullmatch(
            "[0-9a-f]{40}", receipt["tested_checkout_sha"]
        ):
            refuse("unknown hosted checkout")
    return {"local": digest(evidence), "hosted": digest(receipts), "source": digest(current)}


def full_checks_g18(repo, directory, meta):
    import check_runner
    import reporting_activation_v6 as activation

    current = activation.authorization(repo)
    before = check_runner.source(repo.root)
    if (
        current["contract_digest"] != activation.G18_CONTRACT_DIGEST
        or type(meta["plan_comment"]) is not int
        or meta["plan_comment"] != 6068705967
    ):
        refuse("generation18 full gates require current literal authority")
    result = _full_checks_g18(repo, directory, meta, suite_profile="issue31-suite1800-v1")
    if activation.authorization(repo) != current or check_runner.source(repo.root) != before:
        refuse("full gate authority or source changed")
    return result


def _full_checks_g18(repo, directory, meta, *, suite_profile):
    """Validate retained local execution records and fresh hosted associations.

    Local artifacts are owner-writable bookkeeping, not execution attestations.
    Hosted receipts are independently fetched through the existing collector.
    No test runner, installer or provider is invoked by this read-only adapter.
    """
    import hashlib

    import check_runner
    import ci_evidence

    evidence = read(plain_path(directory) / "full-checks.json")
    commands = {
        "serial": "python3 -B scripts/agentic/check.py --jobs 1",
        "parallel": "make check-agentic",
        "full": "make check",
        "clean": "make check-clean",
        "installed": "installed-full-suite",
        "lint": "ruff check",
        "format": "ruff format --check",
        "repository": "python3 -B scripts/check_repository.py",
    }
    if suite_profile is not None:
        commands["serial"] += " --suite-profile issue31-suite1800-v1"
        for name in ("parallel", "full"):
            commands[name] += " AGENTIC_SUITE_PROFILE=issue31-suite1800-v1"
    current = check_runner.source(repo.root)
    if (
        type(evidence) is not dict
        or set(evidence) != {"head", "source", "commands", "installed_files"}
        or evidence["head"] != meta["head_sha"]
        or evidence["source"] != current
        or type(evidence["commands"]) is not dict
        or set(evidence["commands"]) != set(commands)
    ):
        refuse("full check evidence is missing, stale or partial")
    for name, command in commands.items():
        record = evidence["commands"][name]
        if (
            type(record) is not dict
            or set(record) != {"command", "exit_status", "artifacts"}
            or record["command"] != command
            or type(record["exit_status"]) is not int
            or record["exit_status"] != 0
            or type(record["artifacts"]) is not dict
            or not record["artifacts"]
        ):
            refuse("required command provenance differs")
        for relative, expected in record["artifacts"].items():
            if type(relative) is not str or Path(relative).is_absolute() or ".." in Path(relative).parts:
                refuse("unsafe check evidence path")
            checksum(expected)
            if hashlib.sha256(plain_path(directory / relative).read_bytes()).hexdigest() != expected:
                refuse("check artifact changed")
    # Both standard runner records must reconcile every occurrence and worker.
    rows = None
    for name, jobs in [("serial", 1), ("parallel", 2)]:
        request = read(directory / name / "request.json")
        expected = runner_request(repo.root, jobs, suite_profile=suite_profile)
        if expected["source"] != current:
            refuse("source changed during descriptor reconstruction")
        rows = expected["rows"]
        if digest(request) != digest(expected):
            refuse("complete runner request differs from current discovery")
        if suite_profile is not None:
            suite_summary(directory / name, request, jobs)
        for index in range(jobs):
            result = check_runner.reconcile(
                request, index, plain_path(directory / name / f"worker-{index}.jsonl"), 0
            )
            if result["successful"] is not True:
                refuse("incomplete runner occurrence or fixture execution")
    import install

    expected_payload = {
        str(p): hashlib.sha256((repo.root / p).read_bytes()).hexdigest() for p in install.payload(repo.root)
    }
    if digest(evidence["installed_files"]) != digest(expected_payload):
        refuse("current installed payload closure differs")
    validate_installed_adoption(repo.root, directory, rows)
    checks = repo.api(
        f"commits/{meta['head_sha']}/check-runs?per_page=100", paginate=True, page_key="check_runs"
    )
    receipts = ci_evidence.collect(repo, meta["head_sha"], checks, meta["base_sha"])
    for name in ("flowdc-tests", "agentic-quality"):
        found = [r for r in receipts if r.get("check") == name]
        if len(found) != 1:
            refuse("missing or ambiguous hosted execution")
        receipt = found[0]
        if (
            receipt.get("state") != "observed"
            or receipt.get("run_attempt") != 1
            or type(receipt.get("run_attempt")) is not int
            or receipt.get("test_status") != "success"
            or receipt.get("clean_status") != "success"
            or receipt.get("pr_head_sha") != meta["head_sha"]
            or receipt.get("pr_base_sha") != meta["base_sha"]
        ):
            refuse("hosted first-attempt test/clean evidence incomplete")
        # Preserve actual merge checkout separately from the head association.
        if type(receipt.get("tested_checkout_sha")) is not str or not re.fullmatch(
            "[0-9a-f]{40}", receipt["tested_checkout_sha"]
        ):
            refuse("unknown hosted checkout")
    return {"local": digest(evidence), "hosted": digest(receipts), "source": digest(current)}


def full_checks_g17(repo, directory, meta):
    import check_runner
    import reporting_activation_v6 as activation

    current = activation.authorization(repo)
    before = check_runner.source(repo.root)
    if (
        current["contract_digest"] != activation.G17_CONTRACT_DIGEST
        or type(meta["plan_comment"]) is not int
        or meta["plan_comment"] != 6068144159
    ):
        refuse("generation17 full gates require current literal authority")
    result = _full_checks_g17(repo, directory, meta, suite_profile="issue31-suite1800-v1")
    if activation.authorization(repo) != current or check_runner.source(repo.root) != before:
        refuse("full gate authority or source changed")
    return result


def _full_checks_g17(repo, directory, meta, *, suite_profile):
    """Validate retained local execution records and fresh hosted associations.

    Local artifacts are owner-writable bookkeeping, not execution attestations.
    Hosted receipts are independently fetched through the existing collector.
    No test runner, installer or provider is invoked by this read-only adapter.
    """
    import hashlib

    import check_runner
    import ci_evidence

    evidence = read(plain_path(directory) / "full-checks.json")
    commands = {
        "serial": "python3 -B scripts/agentic/check.py --jobs 1",
        "parallel": "make check-agentic",
        "full": "make check",
        "clean": "make check-clean",
        "installed": "installed-full-suite",
        "lint": "ruff check",
        "format": "ruff format --check",
        "repository": "python3 -B scripts/check_repository.py",
    }
    if suite_profile is not None:
        commands["serial"] += " --suite-profile issue31-suite1800-v1"
        for name in ("parallel", "full"):
            commands[name] += " AGENTIC_SUITE_PROFILE=issue31-suite1800-v1"
    current = check_runner.source(repo.root)
    if (
        type(evidence) is not dict
        or set(evidence) != {"head", "source", "commands", "installed_files"}
        or evidence["head"] != meta["head_sha"]
        or evidence["source"] != current
        or type(evidence["commands"]) is not dict
        or set(evidence["commands"]) != set(commands)
    ):
        refuse("full check evidence is missing, stale or partial")
    for name, command in commands.items():
        record = evidence["commands"][name]
        if (
            type(record) is not dict
            or set(record) != {"command", "exit_status", "artifacts"}
            or record["command"] != command
            or type(record["exit_status"]) is not int
            or record["exit_status"] != 0
            or type(record["artifacts"]) is not dict
            or not record["artifacts"]
        ):
            refuse("required command provenance differs")
        for relative, expected in record["artifacts"].items():
            if type(relative) is not str or Path(relative).is_absolute() or ".." in Path(relative).parts:
                refuse("unsafe check evidence path")
            checksum(expected)
            if hashlib.sha256(plain_path(directory / relative).read_bytes()).hexdigest() != expected:
                refuse("check artifact changed")
    # Both standard runner records must reconcile every occurrence and worker.
    rows = None
    for name, jobs in [("serial", 1), ("parallel", 2)]:
        request = read(directory / name / "request.json")
        expected = runner_request(repo.root, jobs, suite_profile=suite_profile)
        if expected["source"] != current:
            refuse("source changed during descriptor reconstruction")
        rows = expected["rows"]
        if digest(request) != digest(expected):
            refuse("complete runner request differs from current discovery")
        if suite_profile is not None:
            suite_summary(directory / name, request, jobs)
        for index in range(jobs):
            result = check_runner.reconcile(
                request, index, plain_path(directory / name / f"worker-{index}.jsonl"), 0
            )
            if result["successful"] is not True:
                refuse("incomplete runner occurrence or fixture execution")
    import install

    expected_payload = {
        str(p): hashlib.sha256((repo.root / p).read_bytes()).hexdigest() for p in install.payload(repo.root)
    }
    if digest(evidence["installed_files"]) != digest(expected_payload):
        refuse("current installed payload closure differs")
    validate_installed_adoption(repo.root, directory, rows)
    checks = repo.api(
        f"commits/{meta['head_sha']}/check-runs?per_page=100", paginate=True, page_key="check_runs"
    )
    receipts = ci_evidence.collect(repo, meta["head_sha"], checks, meta["base_sha"])
    for name in ("flowdc-tests", "agentic-quality"):
        found = [r for r in receipts if r.get("check") == name]
        if len(found) != 1:
            refuse("missing or ambiguous hosted execution")
        receipt = found[0]
        if (
            receipt.get("state") != "observed"
            or receipt.get("run_attempt") != 1
            or type(receipt.get("run_attempt")) is not int
            or receipt.get("test_status") != "success"
            or receipt.get("clean_status") != "success"
            or receipt.get("pr_head_sha") != meta["head_sha"]
            or receipt.get("pr_base_sha") != meta["base_sha"]
        ):
            refuse("hosted first-attempt test/clean evidence incomplete")
        # Preserve actual merge checkout separately from the head association.
        if type(receipt.get("tested_checkout_sha")) is not str or not re.fullmatch(
            "[0-9a-f]{40}", receipt["tested_checkout_sha"]
        ):
            refuse("unknown hosted checkout")
    return {"local": digest(evidence), "hosted": digest(receipts), "source": digest(current)}


def full_checks_g16(repo, directory, meta):
    import check_runner
    import reporting_activation_v6 as activation

    current = activation.authorization(repo)
    before = check_runner.source(repo.root)
    if (
        current["contract_digest"] != activation.G16_CONTRACT_DIGEST
        or type(meta["plan_comment"]) is not int
        or meta["plan_comment"] != 6064513854
    ):
        refuse("generation16 full gates require current literal authority")
    result = _full_checks_g16(repo, directory, meta, suite_profile="issue31-suite1800-v1")
    if activation.authorization(repo) != current or check_runner.source(repo.root) != before:
        refuse("full gate authority or source changed")
    return result


def _full_checks_g16(repo, directory, meta, *, suite_profile):
    """Validate retained local execution records and fresh hosted associations.

    Local artifacts are owner-writable bookkeeping, not execution attestations.
    Hosted receipts are independently fetched through the existing collector.
    No test runner, installer or provider is invoked by this read-only adapter.
    """
    import hashlib

    import check_runner
    import ci_evidence

    evidence = read(plain_path(directory) / "full-checks.json")
    commands = {
        "serial": "python3 -B scripts/agentic/check.py --jobs 1",
        "parallel": "make check-agentic",
        "full": "make check",
        "clean": "make check-clean",
        "installed": "installed-full-suite",
        "lint": "ruff check",
        "format": "ruff format --check",
        "repository": "python3 -B scripts/check_repository.py",
    }
    if suite_profile is not None:
        commands["serial"] += " --suite-profile issue31-suite1800-v1"
        for name in ("parallel", "full"):
            commands[name] += " AGENTIC_SUITE_PROFILE=issue31-suite1800-v1"
    current = check_runner.source(repo.root)
    if (
        type(evidence) is not dict
        or set(evidence) != {"head", "source", "commands", "installed_files"}
        or evidence["head"] != meta["head_sha"]
        or evidence["source"] != current
        or type(evidence["commands"]) is not dict
        or set(evidence["commands"]) != set(commands)
    ):
        refuse("full check evidence is missing, stale or partial")
    for name, command in commands.items():
        record = evidence["commands"][name]
        if (
            type(record) is not dict
            or set(record) != {"command", "exit_status", "artifacts"}
            or record["command"] != command
            or type(record["exit_status"]) is not int
            or record["exit_status"] != 0
            or type(record["artifacts"]) is not dict
            or not record["artifacts"]
        ):
            refuse("required command provenance differs")
        for relative, expected in record["artifacts"].items():
            if type(relative) is not str or Path(relative).is_absolute() or ".." in Path(relative).parts:
                refuse("unsafe check evidence path")
            checksum(expected)
            if hashlib.sha256(plain_path(directory / relative).read_bytes()).hexdigest() != expected:
                refuse("check artifact changed")
    # Both standard runner records must reconcile every occurrence and worker.
    rows = None
    for name, jobs in [("serial", 1), ("parallel", 2)]:
        request = read(directory / name / "request.json")
        expected = runner_request(repo.root, jobs, suite_profile=suite_profile)
        if expected["source"] != current:
            refuse("source changed during descriptor reconstruction")
        rows = expected["rows"]
        if digest(request) != digest(expected):
            refuse("complete runner request differs from current discovery")
        if suite_profile is not None:
            suite_summary(directory / name, request, jobs)
        for index in range(jobs):
            result = check_runner.reconcile(
                request, index, plain_path(directory / name / f"worker-{index}.jsonl"), 0
            )
            if result["successful"] is not True:
                refuse("incomplete runner occurrence or fixture execution")
    import install

    expected_payload = {
        str(p): hashlib.sha256((repo.root / p).read_bytes()).hexdigest() for p in install.payload(repo.root)
    }
    if digest(evidence["installed_files"]) != digest(expected_payload):
        refuse("current installed payload closure differs")
    validate_installed_adoption(repo.root, directory, rows)
    checks = repo.api(
        f"commits/{meta['head_sha']}/check-runs?per_page=100", paginate=True, page_key="check_runs"
    )
    receipts = ci_evidence.collect(repo, meta["head_sha"], checks, meta["base_sha"])
    for name in ("flowdc-tests", "agentic-quality"):
        found = [r for r in receipts if r.get("check") == name]
        if len(found) != 1:
            refuse("missing or ambiguous hosted execution")
        receipt = found[0]
        if (
            receipt.get("state") != "observed"
            or receipt.get("run_attempt") != 1
            or type(receipt.get("run_attempt")) is not int
            or receipt.get("test_status") != "success"
            or receipt.get("clean_status") != "success"
            or receipt.get("pr_head_sha") != meta["head_sha"]
            or receipt.get("pr_base_sha") != meta["base_sha"]
        ):
            refuse("hosted first-attempt test/clean evidence incomplete")
        # Preserve actual merge checkout separately from the head association.
        if type(receipt.get("tested_checkout_sha")) is not str or not re.fullmatch(
            "[0-9a-f]{40}", receipt["tested_checkout_sha"]
        ):
            refuse("unknown hosted checkout")
    return {"local": digest(evidence), "hosted": digest(receipts), "source": digest(current)}


# Installed adoption v1 is deliberately separate from historical installed readers.
ADOPTION_PROFILE = "installed-adoption-v1"
ADOPTION_INTEGRATION = ("Makefile", ".github/workflows/flowdc-tests.yml")
ADOPTION_CONFIG = (
    b"[core]\n\trepositoryformatversion = 0\n\tfilemode = true\n\tbare = false\n\tlogallrefupdates = false\n"
)
ADOPTION_INDEX_OPTIONS = (
    "-c",
    "index.threads=1",
    "-c",
    "index.recordEndOfIndexEntries=false",
    "-c",
    "index.recordOffsetTable=false",
    "-c",
    "core.splitIndex=false",
    "-c",
    "core.untrackedCache=false",
    "-c",
    "core.fsmonitor=false",
)


def adoption_environment():
    import os

    if any(key.startswith("GIT_") for key in os.environ):
        refuse("inherited Git environment is not permitted")
    return {
        **os.environ,
        "LC_ALL": "C",
        "TZ": "UTC",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": "/dev/null",
    }


def adoption_tree(root):
    import stat

    root = plain_path(root)
    files, directories = {}, []
    if not root.is_dir():
        refuse("missing adoption root")
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root).as_posix()
        mode = path.lstat().st_mode
        if stat.S_ISDIR(mode):
            directories.append(relative)
        elif stat.S_ISREG(mode):
            files[relative] = {
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "mode": stat.S_IMODE(mode),
            }
        else:
            refuse("adoption symlink or special file")
    return {"files": files, "directories": directories}


def adoption_path(name):
    if type(name) is not str or not name or "\0" in name or Path(name).is_absolute():
        refuse("invalid adoption path")
    if any(part in ("", ".", "..", ".git") for part in name.split("/")):
        refuse("invalid adoption path component")
    return name.encode("utf-8")


def adoption_objects(contents):
    """Independently serialize every expected object and the extension-free index."""
    import struct

    objects, blobs, trees = {}, {}, []

    def obj(kind, data):
        framed = kind.encode() + b" " + str(len(data)).encode() + b"\0" + data
        oid = hashlib.sha1(framed).hexdigest()
        if oid in objects and objects[oid] != framed:
            refuse("object collision")
        objects[oid] = framed
        return oid

    for name, (data, mode) in contents.items():
        adoption_path(name)
        if type(data) is not bytes or type(mode) is not int or mode not in (0o600, 0o644, 0o755):
            refuse("unsupported adoption file mode or bytes")
        blobs[name] = (0o100755 if mode & 0o111 else 0o100644, obj("blob", data))

    def tree(prefix):
        entries = {}
        for name, value in blobs.items():
            if not name.startswith(prefix):
                continue
            base, slash, _ = name[len(prefix) :].partition("/")
            if base in entries:
                if not slash or entries[base][0] != 0o40000:
                    refuse("file directory collision")
                continue
            entries[base] = (0o40000, tree(prefix + base + "/")) if slash else value
        ordered = sorted(
            entries.items(), key=lambda row: row[0].encode() + (b"/" if row[1][0] == 0o40000 else b"\0")
        )
        raw = b"".join(
            f"{mode:o} ".encode() + name.encode() + b"\0" + bytes.fromhex(oid)
            for name, (mode, oid) in ordered
        )
        value = obj("tree", raw)
        stdin = b"".join(
            f"{mode:06o} {'tree' if mode == 0o40000 else 'blob'} {oid}\t{name}".encode() + b"\0"
            for name, (mode, oid) in ordered
        )
        trees.append((value, stdin))
        return value

    top = tree("")
    person = b"Installed Qualification <installed-qualification@example.invalid> 946684800 +0000\n"
    commit = obj(
        "commit",
        b"tree "
        + top.encode()
        + b"\nauthor "
        + person
        + b"committer "
        + person
        + b"\ninstalled-adoption-v1\n",
    )
    index = b"DIRC" + struct.pack("!II", 2, len(blobs))
    for name, (mode, oid) in sorted(blobs.items(), key=lambda row: row[0].encode()):
        path = adoption_path(name)
        entry = (
            struct.pack("!10I", 0, 0, 0, 0, 0, 0, mode, 0, 0, 0)
            + bytes.fromhex(oid)
            + struct.pack("!H", min(len(path), 4095))
            + path
            + b"\0"
        )
        index += entry + b"\0" * (-len(entry) % 8)
    index += hashlib.sha1(index).digest()
    return {"objects": objects, "blobs": blobs, "trees": trees, "tree": top, "commit": commit, "index": index}


def adoption_git(root, args, *, env, deadline, journal, data=None, identity=False):
    import subprocess
    import time

    remaining = deadline - time.monotonic()
    if remaining <= 0:
        refuse("adoption operation deadline")
    child_env = env.copy()
    if identity:
        child_env.update(
            {
                f"GIT_{role}_{key}": value
                for role in ("AUTHOR", "COMMITTER")
                for key, value in (
                    ("NAME", "Installed Qualification"),
                    ("EMAIL", "installed-qualification@example.invalid"),
                    ("DATE", "2000-01-01T00:00:00+0000"),
                )
            }
        )
    argv = ["git", "-C", str(root), *args]
    started = time.monotonic()
    result = subprocess.run(argv, input=data, capture_output=True, env=child_env, timeout=remaining)
    journal.append(
        {
            "argv": argv,
            "exit_status": result.returncode,
            "elapsed_seconds": time.monotonic() - started,
            "input_digest": hashlib.sha256(data).hexdigest() if data is not None else None,
        }
    )
    if result.returncode or time.monotonic() > deadline:
        refuse("adoption Git operation failed or exceeded deadline")
    return result.stdout


def adoption_expected(source, *, env, deadline, journal):
    import stat

    import install

    source = plain_path(source)
    head = (
        adoption_git(source, ["rev-parse", "HEAD"], env=env, deadline=deadline, journal=journal)
        .decode()
        .strip()
    )
    rows = adoption_git(
        source, ["ls-tree", "-rz", "--full-tree", head], env=env, deadline=deadline, journal=journal
    )
    tracked = {}
    for row in rows.split(b"\0"):
        if not row:
            continue
        info, rawname = row.split(b"\t", 1)
        mode, kind, oid = info.decode().split()
        tracked[rawname.decode()] = (mode, kind, oid)
    contents = {}
    names = [str(path) for path in install.payload(source)] + list(ADOPTION_INTEGRATION)
    if len(set(names)) != len(names):
        refuse("duplicate integration payload")
    for name in names:
        adoption_path(name)
        path = plain_path(source / name)
        data = path.read_bytes()
        mode = stat.S_IMODE(path.stat().st_mode)
        entry = tracked.get(name)
        framed = b"blob " + str(len(data)).encode() + b"\0" + data
        if entry != ("100755" if mode & 0o111 else "100644", "blob", hashlib.sha1(framed).hexdigest()):
            refuse("adoption payload not exact committed source")
        contents[name] = (data, mode)
    return head, contents


def adoption_verify_git(root, expected):
    import zlib

    root = plain_path(root)
    git = root / ".git"
    snapshot = adoption_tree(git)
    fixed = {
        "HEAD": b"ref: refs/heads/installed-fixture\n",
        "config": ADOPTION_CONFIG,
        "index": expected["index"],
        "refs/heads/installed-fixture": expected["commit"].encode() + b"\n",
    }
    object_paths = {f"objects/{oid[:2]}/{oid[2:]}": data for oid, data in expected["objects"].items()}
    if set(snapshot["files"]) != set(fixed) | set(object_paths):
        refuse("unexpected Git metadata or object closure")
    for name, data in fixed.items():
        if (git / name).read_bytes() != data:
            refuse("Git metadata or extension-free index differs")
    for name, framed in object_paths.items():
        decoder = zlib.decompressobj()
        raw = (git / name).read_bytes()
        decoded = decoder.decompress(raw, len(framed) + 1)
        if decoded != framed or not decoder.eof or decoder.unused_data or decoder.unconsumed_tail:
            refuse("Git object framing or bytes differ")
    dirs = {"objects", "objects/info", "objects/pack", "refs", "refs/heads", "refs/tags"}
    dirs.update(f"objects/{oid[:2]}" for oid in expected["objects"])
    if set(snapshot["directories"]) != dirs:
        refuse("unexpected Git directory")
    return snapshot


def adoption_manifest(contents):
    return {
        name: {"sha256": hashlib.sha256(data).hexdigest(), "mode": mode}
        for name, (data, mode) in contents.items()
    }


def adoption_verify_worktree(root, contents, *, git=False):
    value = adoption_tree(root)
    files = {name: row for name, row in value["files"].items() if not (git and name.startswith(".git/"))}
    dirs = {str(parent) for name in contents for parent in Path(name).parents if str(parent) != "."}
    actual_dirs = {
        name for name in value["directories"] if not (git and (name == ".git" or name.startswith(".git/")))
    }
    if files != adoption_manifest(contents) or actual_dirs != dirs:
        refuse("installed adoption worktree missing changed or extra files")
    return {"files": files, "directories": sorted(actual_dirs)}


def adoption_origin(payload):
    """Exact unchanged install.install/tasks.atomic_json output, independent of disk."""
    import json

    value = {
        "schema_version": 1,
        "source": "agentic-github-template",
        "files": {name: hashlib.sha256(data).hexdigest() for name, (data, _) in payload.items()},
    }
    return (json.dumps(value, indent=2, ensure_ascii=False) + "\n").encode("utf-8"), 0o600


def adoption_require_origin(path, expected):
    import stat

    path = plain_path(path)
    if path.read_bytes() != expected[0] or stat.S_IMODE(path.stat().st_mode) != expected[1]:
        refuse("installed canonical origin bytes or mode differ")


def prepare_installed_adoption(source, directory):
    """Explicit bounded software fixture preparation; no suite/provider invocation."""
    import datetime
    import os
    import shutil
    import time

    import install
    from tasks import atomic_json

    env = adoption_environment()
    start = time.monotonic()
    deadline = start + 180
    utc = datetime.datetime.now(datetime.UTC).isoformat()
    directory = plain_path(directory)
    journal = []
    head, contents = adoption_expected(source, env=env, deadline=deadline, journal=journal)
    pristine, execution, template = (
        directory / n for n in ("installed-root", "installed-execution-root", "empty-template")
    )
    if any(path.exists() for path in (pristine, execution, template)):
        refuse("adoption fixture already exists")
    template.mkdir()
    install.install(source, pristine, apply=True)
    payload = {name: value for name, value in contents.items() if name not in ADOPTION_INTEGRATION}
    origin = pristine / ".agentic/template-origin.json"
    origin_value = adoption_origin(payload)
    adoption_require_origin(origin, origin_value)
    payload[".agentic/template-origin.json"] = origin_value
    pristine_map = adoption_verify_worktree(pristine, payload)
    shutil.copytree(pristine, execution)
    for name in ADOPTION_INTEGRATION:
        target = execution / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(contents[name][0])
        os.chmod(target, contents[name][1])
    contents[".agentic/template-origin.json"] = payload[".agentic/template-origin.json"]
    expected = adoption_objects(contents)
    git_version = (
        adoption_git(execution, ["--version"], env=env, deadline=deadline, journal=journal).decode().strip()
    )
    adoption_git(
        execution,
        [
            "init",
            f"--template={template}",
            "--object-format=sha1",
            "--initial-branch=installed-fixture",
            str(execution),
        ],
        env=env,
        deadline=deadline,
        journal=journal,
    )
    (execution / ".git/config").write_bytes(ADOPTION_CONFIG)
    for name, (_, oid) in expected["blobs"].items():
        if (
            adoption_git(
                execution,
                ["hash-object", "-w", "--stdin"],
                env=env,
                deadline=deadline,
                journal=journal,
                data=contents[name][0],
            )
            .decode()
            .strip()
            != oid
        ):
            refuse("real Git blob differs")
    for oid, data in expected["trees"]:
        if (
            adoption_git(execution, ["mktree", "-z"], env=env, deadline=deadline, journal=journal, data=data)
            .decode()
            .strip()
            != oid
        ):
            refuse("real Git tree differs")
    stdin = b"".join(
        f"{mode:o} {oid}\t{name}".encode() + b"\0"
        for name, (mode, oid) in sorted(expected["blobs"].items(), key=lambda row: row[0].encode())
    )
    adoption_git(
        execution,
        [*ADOPTION_INDEX_OPTIONS, "update-index", "--index-version=2", "-z", "--index-info"],
        env=env,
        deadline=deadline,
        journal=journal,
        data=stdin,
    )
    commit = (
        adoption_git(
            execution,
            ["-c", "commit.gpgSign=false", "commit-tree", expected["tree"]],
            env=env,
            deadline=deadline,
            journal=journal,
            data=b"installed-adoption-v1\n",
            identity=True,
        )
        .decode()
        .strip()
    )
    if commit != expected["commit"]:
        refuse("real Git commit differs")
    adoption_git(
        execution,
        ["update-ref", "refs/heads/installed-fixture", commit, "0" * 40],
        env=env,
        deadline=deadline,
        journal=journal,
    )
    git_map = adoption_verify_git(execution, expected)
    worktree_map = adoption_verify_worktree(execution, contents, git=True)
    if adoption_verify_worktree(pristine, payload) != pristine_map or time.monotonic() > deadline:
        refuse("preparation changed pristine or exceeded deadline")
    result = {
        "schema_version": 1,
        "profile": ADOPTION_PROFILE,
        "source_head": head,
        "payload_files": adoption_manifest(
            {n: v for n, v in payload.items() if n != ".agentic/template-origin.json"}
        ),
        "integration_files": adoption_manifest({n: contents[n] for n in ADOPTION_INTEGRATION}),
        "pristine_manifest": pristine_map,
        "execution_worktree_manifest": worktree_map,
        "git_manifest_before": git_map,
        "fixture_commit": commit,
        "fixture_tree": expected["tree"],
        "git_version": git_version,
        "commands": journal,
        "argv": ["install.install", str(source), str(pristine), "apply=True"],
        "cwd": str(source),
        "safe_environment": {
            "git_keys_before": [],
            "configuration": {
                "LC_ALL": "C",
                "TZ": "UTC",
                "GIT_CONFIG_NOSYSTEM": "1",
                "GIT_CONFIG_GLOBAL": "/dev/null",
            },
        },
        "utc_start": utc,
        "utc_end": datetime.datetime.now(datetime.UTC).isoformat(),
        "elapsed_seconds": time.monotonic() - start,
        "cap_seconds": 180,
        "exit_status": 0,
    }
    atomic_json(directory / "adoption-preparation.json", result)
    return result


def adoption_descriptor(root):
    """Fresh installed interpreter; no imported source-test objects are reused."""
    import json
    import subprocess

    code = """import hashlib,json,sys
from pathlib import Path
root=Path.cwd()
sys.path.insert(0,str(root/'scripts/agentic'))
import check_runner as r
source=r.source(root)
suite,rows,objects,errors=r.discover(root)
policy,assignments=r.assignment_policy(root,suite,rows,objects,source,1,suite_profile="issue31-suite1800-v1")
request=dict(version=r.SUITE_VERSION,jobs=1,source=source,rows=rows,assignments=assignments,assignment_policy=policy,errors=errors,evidence_limit=r.EVIDENCE_BYTES,execution_limits=r.execution_limits('issue31-suite1800-v1',1800,r.TEXT_BYTES,r.EVIDENCE_BYTES))
owned={p.stem for d in ('scripts/agentic','tests/agentic') for p in (root/d).glob('*.py')}
modules={};system={}
for name,module in sorted(sys.modules.items()):
    path=getattr(module,'__file__',None)
    if not path or path.startswith('<'):continue
    path=Path(path).resolve()
    if name.split('.')[0] in owned or path.is_relative_to(root):
        if not path.is_relative_to(root):raise SystemExit('outside installed module origin')
        modules[name]=dict(path=str(path.relative_to(root)),sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    else:system[name]=dict(path=str(path),version=str(getattr(module,'__version__','unknown')))
print(json.dumps(dict(request=request,origins=dict(python=sys.executable,modules=modules,system_modules=system))))
"""
    import os

    if os.environ.get("PYTHONPATH") is not None:
        refuse("descriptor caller PYTHONPATH must be absent")
    result = subprocess.run(
        ["/usr/bin/python3", "-B", "-c", code],
        cwd=root,
        capture_output=True,
        timeout=180,
    )
    if result.returncode:
        refuse("installed descriptor interpreter failed")
    value = json.loads(result.stdout)
    if value["request"]["errors"]:
        refuse("installed discovery errors")
    return value


def adoption_execution(record, root):
    import datetime

    keys = {
        "argv",
        "cwd",
        "safe_environment",
        "utc_start",
        "utc_end",
        "elapsed_seconds",
        "cap_seconds",
        "exit_status",
    }
    argv = [
        "/usr/bin/python3",
        "-B",
        str(root / "scripts/agentic/check.py"),
        "--jobs",
        "1",
        "--suite-profile",
        "issue31-suite1800-v1",
    ]
    if (
        type(record) is not dict
        or set(record) != keys
        or record["argv"] != argv
        or record["cwd"] != str(root)
    ):
        refuse("installed execution command differs")
    if digest(record["safe_environment"]) != digest({"PYTHONPATH": None, "git_keys": []}):
        refuse("installed execution environment differs")
    if (
        type(record["cap_seconds"]) is not int
        or record["cap_seconds"] != 1860
        or type(record["exit_status"]) is not int
        or record["exit_status"] != 0
    ):
        refuse("installed execution failed or wrong cap")
    elapsed = clock(record["elapsed_seconds"])
    if elapsed > 1860:
        refuse("installed outer duration exceeded")
    try:
        start, end = (datetime.datetime.fromisoformat(record[k]) for k in ("utc_start", "utc_end"))
        if (
            start.utcoffset() != datetime.timedelta(0)
            or end.utcoffset() != datetime.timedelta(0)
            or abs((end - start).total_seconds() - elapsed) > 2
        ):
            refuse("installed execution clock mismatch")
    except (TypeError, ValueError):
        refuse("invalid installed execution clock")


def validate_installed_adoption(source, directory, source_rows):
    """No suite launch; all receipt assertions are replayed against complete roots."""
    import time

    import check_runner

    env = adoption_environment()
    env["GIT_OPTIONAL_LOCKS"] = "0"
    deadline = time.monotonic() + 180
    directory = plain_path(directory)
    pristine, execution = (directory / n for n in ("installed-root", "installed-execution-root"))
    receipt = read(directory / "installed-adoption.json")
    keys = {
        "schema_version",
        "profile",
        "source_head",
        "source_files",
        "payload_files",
        "integration_files",
        "pristine_manifest",
        "execution_worktree_manifest",
        "git_manifest_before",
        "git_manifest_after",
        "fixture_commit",
        "fixture_tree",
        "preparation",
        "execution",
        "request_digest",
        "summary_digest",
        "journal_digest",
        "origins",
    }
    if (
        type(receipt) is not dict
        or set(receipt) != keys
        or type(receipt["schema_version"]) is not int
        or receipt["schema_version"] != 1
        or receipt["profile"] != ADOPTION_PROFILE
    ):
        refuse("unsupported installed adoption receipt")
    journal = []
    head, contents = adoption_expected(source, env=env, deadline=deadline, journal=journal)
    payload = {n: v for n, v in contents.items() if n not in ADOPTION_INTEGRATION}
    origin_value = adoption_origin(payload)
    for root in (pristine, execution):
        adoption_require_origin(root / ".agentic/template-origin.json", origin_value)
    contents[".agentic/template-origin.json"] = origin_value
    payload_with_origin = {**payload, ".agentic/template-origin.json": origin_value}
    expected = adoption_objects(contents)
    git_map = adoption_verify_git(execution, expected)
    current_source = check_runner.source(source)
    actual = {
        "source_head": head,
        "source_files": current_source,
        "payload_files": adoption_manifest(payload),
        "integration_files": adoption_manifest({n: contents[n] for n in ADOPTION_INTEGRATION}),
        "pristine_manifest": adoption_verify_worktree(pristine, payload_with_origin),
        "execution_worktree_manifest": adoption_verify_worktree(execution, contents, git=True),
        "git_manifest_before": git_map,
        "git_manifest_after": git_map,
        "fixture_commit": expected["commit"],
        "fixture_tree": expected["tree"],
    }
    for key, value in actual.items():
        if digest(receipt[key]) != digest(value):
            refuse("installed adoption binding differs: " + key)
    template = plain_path(directory / "empty-template")
    if not template.is_dir() or list(template.iterdir()):
        refuse("nonempty or missing Git template")
    prep = read(directory / "adoption-preparation.json")
    prep_keys = {
        "schema_version",
        "profile",
        "source_head",
        "payload_files",
        "integration_files",
        "pristine_manifest",
        "execution_worktree_manifest",
        "git_manifest_before",
        "fixture_commit",
        "fixture_tree",
        "git_version",
        "commands",
        "argv",
        "cwd",
        "safe_environment",
        "utc_start",
        "utc_end",
        "elapsed_seconds",
        "cap_seconds",
        "exit_status",
    }
    if type(prep) is not dict or set(prep) != prep_keys or digest(prep) != digest(receipt["preparation"]):
        refuse("preparation shape or receipt differs")
    if (
        type(prep["schema_version"]) is not int
        or prep["schema_version"] != 1
        or prep["profile"] != ADOPTION_PROFILE
        or type(prep["exit_status"]) is not int
        or prep["exit_status"] != 0
        or type(prep["cap_seconds"]) is not int
        or prep["cap_seconds"] != 180
        or clock(prep["elapsed_seconds"]) > 180
    ):
        refuse("invalid preparation bounds")
    if (
        prep["argv"] != ["install.install", str(source), str(pristine), "apply=True"]
        or prep["cwd"] != str(source)
        or digest(prep["safe_environment"])
        != digest(
            {
                "git_keys_before": [],
                "configuration": {
                    "LC_ALL": "C",
                    "TZ": "UTC",
                    "GIT_CONFIG_NOSYSTEM": "1",
                    "GIT_CONFIG_GLOBAL": "/dev/null",
                },
            }
        )
    ):
        refuse("preparation environment or installer invocation differs")
    import datetime

    try:
        started, ended = (datetime.datetime.fromisoformat(prep[k]) for k in ("utc_start", "utc_end"))
        if (
            started.utcoffset() != datetime.timedelta(0)
            or ended.utcoffset() != datetime.timedelta(0)
            or abs((ended - started).total_seconds() - prep["elapsed_seconds"]) > 2
        ):
            refuse("preparation UTC and monotonic clocks differ")
    except (TypeError, ValueError):
        refuse("invalid preparation UTC")
    for key in actual.keys() & prep.keys():
        if digest(prep[key]) != digest(actual[key]):
            refuse("preparation binding changed")
    # Closed sequence reconstructed from the expected source and every object.
    expected_commands = [
        (["git", "-C", str(source), "rev-parse", "HEAD"], None),
        (["git", "-C", str(source), "ls-tree", "-rz", "--full-tree", head], None),
    ]

    def command(args, data=None):
        expected_commands.append(
            (
                ["git", "-C", str(execution), *args],
                hashlib.sha256(data).hexdigest() if data is not None else None,
            )
        )

    command(["--version"])
    command(
        [
            "init",
            f"--template={template}",
            "--object-format=sha1",
            "--initial-branch=installed-fixture",
            str(execution),
        ]
    )
    for name in expected["blobs"]:
        command(["hash-object", "-w", "--stdin"], contents[name][0])
    for _, data in expected["trees"]:
        command(["mktree", "-z"], data)
    index_stdin = b"".join(
        f"{mode:o} {oid}\t{name}".encode() + b"\0"
        for name, (mode, oid) in sorted(expected["blobs"].items(), key=lambda row: row[0].encode())
    )
    command([*ADOPTION_INDEX_OPTIONS, "update-index", "--index-version=2", "-z", "--index-info"], index_stdin)
    command(["-c", "commit.gpgSign=false", "commit-tree", expected["tree"]], b"installed-adoption-v1\n")
    command(["update-ref", "refs/heads/installed-fixture", expected["commit"], "0" * 40])
    if type(prep["commands"]) is not list or len(prep["commands"]) != len(expected_commands):
        refuse("preparation Git command sequence differs")
    for row, (argv, data_digest) in zip(prep["commands"], expected_commands, strict=True):
        if (
            type(row) is not dict
            or set(row) != {"argv", "input_digest", "exit_status", "elapsed_seconds"}
            or row["argv"] != argv
            or row["input_digest"] != data_digest
            or type(row["exit_status"]) is not int
            or row["exit_status"] != 0
            or clock(row["elapsed_seconds"]) > 180
        ):
            refuse("preparation command record differs")
    if (
        adoption_git(execution, ["--version"], env=env, deadline=deadline, journal=journal).decode().strip()
        != prep["git_version"]
    ):
        refuse("Git runtime version differs")
    if (
        adoption_git(execution, ["rev-parse", "HEAD"], env=env, deadline=deadline, journal=journal)
        .decode()
        .strip()
        != expected["commit"]
    ):
        refuse("actual fixture checkout differs")
    stage = adoption_git(
        execution, ["ls-files", "--stage", "-z"], env=env, deadline=deadline, journal=journal
    )
    expected_stage = b"".join(
        f"{mode:o} {oid} 0\t{name}".encode() + b"\0"
        for name, (mode, oid) in sorted(expected["blobs"].items(), key=lambda row: row[0].encode())
    )
    if stage != expected_stage:
        refuse("actual Git index stage listing differs")
    adoption_execution(receipt["execution"], execution)
    observed = adoption_descriptor(execution)
    request = read(directory / "installed/request.json")
    if (
        digest(observed["origins"]) != digest(receipt["origins"])
        or digest(observed["request"]) != digest(request)
        or digest(request["rows"]) != digest(source_rows)
    ):
        refuse("installed origins or complete request differs")
    for field, name in (
        ("request_digest", "request.json"),
        ("summary_digest", "summary.json"),
        ("journal_digest", "worker-0.jsonl"),
    ):
        if (
            receipt[field]
            != hashlib.sha256(plain_path(directory / "installed" / name).read_bytes()).hexdigest()
        ):
            refuse("installed execution artifact changed")
    suite_summary(directory / "installed", request, 1)
    if (
        check_runner.reconcile(request, 0, plain_path(directory / "installed/worker-0.jsonl"), 0)[
            "successful"
        ]
        is not True
    ):
        refuse("installed full suite failed")
    if (
        digest(adoption_verify_git(execution, expected)) != digest(git_map)
        or digest(adoption_verify_worktree(pristine, payload_with_origin))
        != digest(actual["pristine_manifest"])
        or digest(adoption_verify_worktree(execution, contents, git=True))
        != digest(actual["execution_worktree_manifest"])
        or check_runner.source(source) != current_source
        or time.monotonic() > deadline
    ):
        refuse("installed replay changed source or exceeded deadline")
    return digest(receipt)


def catalog(repo, *, plan_only=False, packet_target=None, batch_directory=None):
    """Actual source/check adapter; no readiness flag or provisional catalog input.

    The coordinator supplies a prepared full packet plus retained check artifacts
    at the task's v6_catalog directory after final source gates. All inventory is
    regenerated from Git and exact public context, not imported assignments.
    """
    import reporting_activation_v6 as current_authority

    if current_authority.authorization(repo).get("contract_digest") == current_authority.G20_CONTRACT_DIGEST:
        from review_public_catalog_v1 import catalog as public_catalog

        return public_catalog(
            repo, plan_only=plan_only, packet_target=packet_target, batch_directory=batch_directory
        )
    import hashlib
    import tempfile

    import reporting_activation_v6 as activation
    import review
    import review_packet
    from tasks import issue_contract
    from workflow import configuration, run, write_json

    state = read(repo.main / ".agentic-local/tasks/issue-31.json")
    authority = activation.authorization(repo)
    if digest(read(repo.main / ".agentic-local/tasks/issue-31.json")) != digest(state):
        refuse("catalog authority changed during selection")
    # Authorization rejects unknown generations before this literal selection.
    # Keep the historical catalog API independent of its authorization receipt shape.
    if type(state.get("contract_generation")) is int and state["contract_generation"] == 19:
        current_contract = activation.G19_CONTRACT
        current_digest = activation.G19_CONTRACT_DIGEST
        if digest(current_contract) != current_digest or authority["contract_digest"] != current_digest:
            refuse("catalog generation19 contract differs")
    elif type(state.get("contract_generation")) is int and state["contract_generation"] == 18:
        current_contract = activation.G18_CONTRACT
        current_digest = activation.G18_CONTRACT_DIGEST
        if digest(current_contract) != current_digest or authority["contract_digest"] != current_digest:
            refuse("catalog generation18 contract differs")
    elif type(state.get("contract_generation")) is int and state["contract_generation"] == 17:
        current_contract = activation.G17_CONTRACT
        current_digest = activation.G17_CONTRACT_DIGEST
        if digest(current_contract) != current_digest or authority["contract_digest"] != current_digest:
            refuse("catalog generation17 contract differs")
    elif type(state.get("contract_generation")) is int and state["contract_generation"] == 16:
        current_contract = activation.G16_CONTRACT
        current_digest = activation.G16_CONTRACT_DIGEST
        if digest(current_contract) != current_digest or authority["contract_digest"] != current_digest:
            refuse("catalog generation16 contract differs")
    elif type(state.get("contract_generation")) is int and state["contract_generation"] == 15:
        current_contract = activation.G15_CONTRACT
        current_digest = activation.G15_CONTRACT_DIGEST
        if digest(current_contract) != current_digest or authority["contract_digest"] != current_digest:
            refuse("catalog generation15 contract differs")
    elif type(state.get("contract_generation")) is int and state["contract_generation"] == 14:
        current_contract = activation.G14_CONTRACT
        current_digest = activation.G14_CONTRACT_DIGEST
        if digest(current_contract) != current_digest or authority["contract_digest"] != current_digest:
            refuse("catalog generation14 contract differs")
    elif type(state.get("contract_generation")) is int and state["contract_generation"] == 13:
        current_contract = activation.NEXT_CONTRACT
        current_digest = activation.NEXT_CONTRACT_DIGEST
        if digest(current_contract) != current_digest or authority["contract_digest"] != current_digest:
            refuse("catalog current contract differs")
    else:
        current_contract = activation.CONTRACT
        current_digest = activation.CONTRACT_DIGEST
    pointer = state.get("v6_catalog")
    if type(pointer) is not str or not pointer:
        refuse("actual final catalog/full-check designation is absent")
    directory = plain_path(Path(pointer))
    meta = review.verify_packet(directory)
    if digest(meta.get("config")) != digest(configuration(repo.root)):
        refuse("packet limits/configuration differ from trusted current source")
    if (
        meta.get("schema_version") != 7
        or meta.get("kind") not in {"single", "batch-parent"}
        or meta.get("issue") != 31
        or meta.get("plan_comment") != current_contract["plan_comment"]
        or meta.get("repository") != repo.name
        or meta.get("pr") != 32
        or meta.get("batch_unit")
        or meta.get("prior_review")
        or meta.get("reporting_activation")
    ):
        refuse("final catalog requires full current metadata7 parent")
    if repo.git("status", "--porcelain").strip() or repo.git("rev-parse", "HEAD") != meta["head_sha"]:
        refuse("catalog source is dirty or stale")
    ancestor = repo.git("merge-base", meta["base_sha"], meta["head_sha"])
    if ancestor != meta["merge_base_sha"]:
        refuse("catalog merge base differs")
    if digest(issue_contract(repo, 31, current_contract["plan_comment"])) != current_digest:
        refuse("current issue/plan changed")
    review.current_pr(repo, 32, meta["head_sha"], meta["base_sha"])
    gates = (
        full_checks_g19(repo, directory, meta)
        if current_digest == activation.G19_CONTRACT_DIGEST
        else full_checks_g18(repo, directory, meta)
        if current_digest == activation.G18_CONTRACT_DIGEST
        else full_checks_g17(repo, directory, meta)
        if current_digest == activation.G17_CONTRACT_DIGEST
        else full_checks_g16(repo, directory, meta)
        if current_digest == activation.G16_CONTRACT_DIGEST
        else full_checks_g15(repo, directory, meta)
        if current_digest == activation.G15_CONTRACT_DIGEST
        else full_checks_g14(repo, directory, meta)
        if current_digest == activation.G14_CONTRACT_DIGEST
        else full_checks(repo, directory, meta)
    )
    original = directory / "packet"
    context = read(original / "context.json")
    saved_contract = {
        "issue": 31,
        "plan_comment": current_contract["plan_comment"],
        "issue_digest": digest(
            {"title": context["issue"]["title"], "body": context["issue"].get("body") or ""}
        ),
        "plan_digest": digest(
            {
                "id": context["designated_plan_comment"]["id"],
                "body": context["designated_plan_comment"].get("body") or "",
            }
        ),
    }
    if digest(saved_contract) != current_digest:
        refuse("saved issue/plan contract differs")
    if (
        context["pull_request"]["head"]["sha"] != meta["head_sha"]
        or context["pull_request"]["base"]["sha"] != meta["base_sha"]
    ):
        refuse("saved PR identity differs")
    if batch_directory is not None:
        if packet_target is not None or plan_only:
            refuse("batch reconciliation cannot prepare or replace the initial catalog")
        reconcile_public_context(repo, batch_directory, context)
    else:
        for key, endpoint in [
            ("reviews", "pulls/32/reviews"),
            ("inline_comments", "pulls/32/comments"),
            ("pr_comments", "issues/32/comments"),
            ("issue_comments", "issues/31/comments"),
        ]:
            if digest(context[key]) != digest(repo.api(endpoint, paginate=True)):
                refuse("public findings/dispositions/context changed")
    # Rebuild current Git snapshots and the complete original obligation inventory.
    with tempfile.TemporaryDirectory(prefix="agentic-catalog-") as temporary:
        packet = Path(temporary)
        head_index = review.snapshot(repo, meta["head_sha"], packet / "source", meta["config"])
        base_index = review.snapshot(repo, ancestor, packet / "base-source", meta["config"])
        write_json(packet / "source-index.json", head_index)
        write_json(packet / "base-source-index.json", base_index)
        for name in ("source-index.json", "base-source-index.json"):
            if digest(review.coverage.read_json(packet / name)) != digest(
                review.coverage.read_json(original / name)
            ):
                refuse("snapshot inventory differs from Git")
        diff = run(
            [
                "git",
                "-C",
                repo.root,
                "diff",
                "--no-ext-diff",
                "--no-textconv",
                "--no-renames",
                ancestor,
                meta["head_sha"],
            ]
        ).stdout
        (packet / "diff.txt").write_text(diff, encoding="utf-8")
        write_json(packet / "context.json", context)
        for source, target in [
            ("AGENTS.md", "repository-policy.txt"),
            ("docs/agent-workflow/REVIEW.md", "review-policy.txt"),
            (meta["config"]["domain_rubric"], "domain-policy.txt"),
            (".agentic/schemas/review-report.json", "report-schema.json"),
        ]:
            raw = plain_path(repo.root / source).read_bytes()
            if raw != (original / target).read_bytes():
                refuse("trusted policy/schema differs")
            (packet / target).write_bytes(raw)
        review_packet.build(
            repo,
            packet,
            meta["head_sha"],
            ancestor,
            head_index,
            base_index,
            context,
            meta["config"],
            provider="claude-code",
        )
        inventory = read(packet / "required-material.json")["required"]
        if digest(inventory) != digest(read(original / "required-material.json")["required"]):
            refuse("prepared packet omits or changes regenerated obligations")
        # Every whole original response remains primary, including coverage/accounting
        # omitted by the legacy finding navigation rendering. No inherited credit.
        for surface in ("reviews", "inline_comments", "pr_comments"):
            for record in context[surface]:
                body = record.get("body") or ""
                if not body:
                    continue
                name = "whole-responses/" + digest([surface, record["id"]]) + ".txt"
                raw = body.encode("utf-8")
                (packet / name).parent.mkdir(exist_ok=True)
                (packet / name).write_bytes(raw)
                inventory.append(
                    {
                        "id": digest([surface, record["id"], hashlib.sha256(raw).hexdigest()])[:24],
                        "path": f"{surface}:{record['id']}",
                        "kind": "finding",
                        "artifact": name,
                        "start_line": 1,
                        "end_line": len(body.splitlines()),
                        "bytes": len(raw),
                    }
                )
        # Current final guidance is mandatory in capacity/integration as well as
        # retaining its original primary obligations. Distinct cross-boundary IDs
        # make this additional inspection explicit, not an optional substitution.
        for original_name in (
            "repository-policy.txt",
            "review-policy.txt",
            "domain-policy.txt",
            "report-schema.json",
        ):
            raw = (packet / original_name).read_bytes()
            name = "final-guidance/" + original_name
            (packet / name).parent.mkdir(exist_ok=True)
            (packet / name).write_bytes(raw)
            inventory.append(
                {
                    "id": digest(["integration-guidance", name, hashlib.sha256(raw).hexdigest()])[:24],
                    "path": name,
                    "kind": "cross-boundary",
                    "artifact": name,
                    "start_line": 1,
                    "end_line": len(raw.decode("utf-8").splitlines()),
                    "bytes": len(raw),
                }
            )
        binding = {
            **gates,
            "authorization": digest(authority),
            "context": digest(context),
            "contract": current_digest,
            "identity": digest(
                {
                    k: meta[k]
                    for k in (
                        "repository",
                        "pr",
                        "issue",
                        "plan_comment",
                        "head_sha",
                        "base_sha",
                        "merge_base_sha",
                    )
                }
            ),
            "policy": digest(meta["review_policy"]),
            "inventory": digest(inventory),
        }
        # Export the complete regenerated inventory, including whole responses and
        # additional cross-boundary guidance. The same bytes bind every consumer.
        write_json(packet / "required-material.json", {"schema_version": 3, "required": inventory})
        (packet / "inventory-sha256.txt").write_text(review.digest(packet / "required-material.json") + "\n")
        planned = partition(packet, inventory, binding)
        planned_record = plan_catalog(planned)
        if packet_target is not None:
            import shutil

            target = plain_path(Path(packet_target))
            if target.exists():
                refuse("catalog export must be exclusive")
            shutil.copytree(packet, target)
            return {"plan": planned_record, "metadata": copy.deepcopy(meta)}
        if plan_only:
            return planned_record
        components = []
        lookup = {i["id"]: i for i in planned["items"]}
        for unit in planned["components"]:
            rows = [
                {k: lookup[key][k] for k in ("id", "artifact", "start_line", "end_line")}
                for key in unit["required_ids"]
            ]
            components.append(
                {
                    "id": unit["id"],
                    "items": rows,
                    "files": {r["artifact"]: (packet / r["artifact"]).read_bytes() for r in rows},
                }
            )
        additional = [
            {k: lookup[key][k] for k in ("id", "artifact", "start_line", "end_line")}
            for key in planned["integration"]["required_ids"]
        ]
        return {
            "components": components,
            "items": additional,
            "files": {r["artifact"]: (packet / r["artifact"]).read_bytes() for r in additional},
            "dependencies": {**binding, "assignments": digest(planned)},
        }


def integration_reports(plan, reports, dependencies, *, existing_projection_bytes=0):
    """Materialize every whole report; dependency qualification is a separate gate."""
    import review_report_material_v1 as material

    validate_current_plan(plan)
    names = [u["id"] for u in plan["catalog"]["components"]]
    if (
        type(reports) is not dict
        or type(dependencies) is not dict
        or list(reports) != names
        or list(dependencies) != names
    ):
        refuse("integration requires every exact ordered component dependency")
    files, items, projections = {}, [], []
    for name in names:
        raw = reports[name]
        artifact = f"component-reports/{name}.txt"
        item, projection = material.material(raw, artifact, dependencies[name])
        material.verify(raw, projection, item, dependencies[name])
        files[artifact], files[item["artifact"]] = raw, projection
        items.append(item)
        projections.append(projection)
    material.packet_budget(projections, existing_projection_bytes + sum(map(len, reports.values())))
    if len(items) + len(plan["catalog"]["integration"]["required_ids"]) > 128:
        refuse("integration report and cross-boundary IDs exceed bound")
    return {"items": items, "files": files, "dependencies": copy.deepcopy(dependencies)}


def integration_material(repo, directory, batch):
    """Rebuild exact report inputs from the immutable complete component seal.

    Every component is independently qualified locally. Remote publication and
    current source are separately re-fetched by the encompassing prefix adapter.
    No current clock, admission or source context is reconstructed here.
    """
    import review

    directory = Path(directory)
    plan = batch["plan"]
    index = len(plan["schedule"]["windows"]) - 2
    transitions = journal(directory, plan, batch["application"])
    if len(transitions) < index * 2:
        refuse("integration requires its independently sealed and resumed complete prefix")
    sealed = transitions[index * 2 - 2]["value"]["children"]
    names = [u["id"] for u in plan["catalog"]["components"]]
    if [r["unit"] for r in sealed] != names:
        refuse("integration seal omits or reorders a component")
    reports, dependencies = {}, {}
    for name, row in zip(names, sealed, strict=True):
        child = directory / "units" / name
        qualified = qualify_child(repo, child)
        meta, _, reservation = runtime_reservation(repo, child)
        ack = read(child / PUBLICATION_ACK)
        intent = read(child / PUBLICATION_INTENT)
        if (
            row["binding"] != plan["catalog_digest"]
            or row["claim"] != reservation["material_claim"]
            or row["report"] != review.digest(child / "review.md")
            or row["publication"] != digest(ack)
            or row["execution"] != review.digest(child / "reporting-execution.json")
            or row["capture"] != review.digest(child / "review-capture.json")
            or row["observer"] != review.digest(child / OBSERVATION)
            or row["usage"] != qualified["usage"]
            or ack["intent_sha256"] != digest(intent)
        ):
            refuse("integration component dependency differs from complete stopped seal")
        dependencies[name] = {
            **{
                key: meta[key]
                for key in (
                    "review_sha256",
                    "diagnostics_sha256",
                    "coverage_sha256",
                    "reporting_sha256",
                    "terminal_sha256",
                )
            },
            "execution_sha256": row["execution"],
            "publication_sha256": review.coverage.checksum(intent["body"]),
        }
        reports[name] = review.exact_reporting_bytes(child / "review.md", 10000)
    existing = sum(
        (directory / "packet" / name).stat().st_size
        for name in plan["catalog"]["files"]
        if name.startswith(("projections/", "whole-report-projections/", "component-reports/"))
    )
    extra = integration_reports(plan, reports, dependencies, existing_projection_bytes=existing)
    extra["dependencies"] = {
        "schema_version": 1,
        "components": dependencies,
        "prefix": copy.deepcopy(sealed),
        "transition": digest(transitions[index * 2 - 1]),
    }
    return extra


def replay_prefix(repo, directory, plan, window):
    """Recompute the complete declared component prefix, without repairs or imports."""
    if not (Path(directory) / "batch.json").exists():
        refuse("prefix adapter requires actual complete batch9 preparation")
    batch = load_preparation(directory)
    if (
        type(window) is not int
        or not 0 <= window < len(plan["schedule"]["windows"]) - 1
        or digest(plan) != digest(batch["plan"])
    ):
        refuse("only the exact original component/integration windows can replay")
    value = component_prefix(repo, directory)
    if value["pending"] is not None or [r["unit"] for r in value["rows"]] != [
        u for w in plan["schedule"]["windows"][: window + 1] for u in w
    ]:
        refuse("incomplete published window or active/uncertain reservation")
    return value["rows"]


def current_plan(repo, plan, *, directory=None):
    source = (
        catalog(repo, batch_directory=directory)
        if directory is not None and _has_publications(directory)
        else catalog(repo)
    )
    if source["dependencies"]["assignments"] != plan["catalog_digest"]:
        refuse("current source/catalog/check provenance changed")


def window_clock(plan, applied, rows, window, now):
    integer(window, 0, len(plan["schedule"]["windows"]) - 1)
    started = rows[-1]["value"]["resumed_at"] if rows else applied["applied_at"]
    allocation = plan["schedule"]["window_seconds"][window] - 360
    if not started <= clock(now) <= min(started + allocation, applied["wall_deadline"]):
        refuse("active window clock rollback or allocation overrun")


def journal(directory, plan, applied):
    """Read an append-only stopped-boundary journal; a torn transition stops it."""
    root = plain_path(directory / "window-transitions")
    if (Path(directory) / "window-transition-failures").exists():
        refuse("stopped-boundary failure remains consumed; no repair or repeat")
    if not root.exists():
        if (Path(directory) / "window-transition-acks").exists():
            refuse("orphan stopped-boundary acknowledgments")
        return []
    paths = sorted(root.iterdir())
    if (Path(directory) / "batch.json").exists():
        acknowledgments = plain_path(Path(directory) / "window-transition-acks")
        if not acknowledgments.exists() or sorted(p.name for p in acknowledgments.iterdir()) != [
            p.name for p in paths
        ]:
            refuse("torn or hidden stopped-boundary acknowledgments")
    if len(paths) > 18 or [p.name for p in paths] != [f"{i:02d}.json" for i in range(len(paths))]:
        refuse("torn, renamed or excessive window transition journal")
    rows = []
    for i, path in enumerate(paths):
        row = read(plain_path(path))
        if type(row) is not dict or set(row) not in (
            {"operation", "previous", "value", "authentication"},
            {"operation", "previous", "value", "authentication", "batch_proof"},
        ):
            refuse("incomplete window transition")
        if row["previous"] != (digest(rows[-1]) if rows else digest(applied)):
            refuse("window transition predecessor changed")
        value = row["value"]
        if i % 2 == 0:
            if row["operation"] != "pause" or value.get("window") != i // 2:
                refuse("pause reordered or copied")
            if rows and row["authentication"] != rows[-1]["authentication"]:
                refuse("journal generation changed during an active window")
            window_clock(plan, applied, rows, i // 2, value.get("sealed_at"))
            expected = seal(plan, applied, i // 2, value.get("children"), value.get("sealed_at"))
        else:
            if row["operation"] != "resume":
                refuse("resume reordered")
            prior = rows[-1]["value"]
            expected = resume(plan, applied, prior, prior["children"], value.get("resumed_at"))
        if digest(value) != digest(expected):
            refuse("window transition value changed")
        import claude_native_auth

        claude_native_auth.validate_binding(row["authentication"])
        if (Path(directory) / "batch.json").exists():
            validate_transition(directory, row, rows)
            transition_ack(directory, row, rows, plan, applied)
        elif "batch_proof" in row:
            refuse("production transition without batch")
        rows.append(row)
    return rows


def pause(repo, directory, *, owned, now, final_validation=False):
    """Stop only after current source and independently published prefix replay."""
    import claude_owned_auth

    if (Path(directory) / "batch.json").exists():
        return component_transition(
            repo, Path(directory), owned=owned, now=now, resuming=False, final_validation=final_validation
        )
    plan, applied = load(directory)
    current_plan(repo, plan)
    rows = journal(directory, plan, applied)
    if len(rows) % 2:
        refuse("already paused; no replacement or extension")
    window = len(rows) // 2
    window_clock(plan, applied, rows, window, now)
    children = replay_prefix(repo, directory, plan, window)
    binding = claude_owned_auth.require(owned).current_binding(900)
    if rows and binding != rows[-1]["authentication"]:
        refuse("generation changed inside an active window")
    value = seal(plan, applied, window, children, now)
    record = {
        "operation": "pause",
        "previous": digest(rows[-1]) if rows else digest(applied),
        "value": value,
        "authentication": binding,
    }
    private_directory(plain_path(directory / "window-transitions"))
    exclusive(plain_path(directory / "window-transitions" / f"{len(rows):02d}.json"), record, limit=2000000)
    return record


def resume_window(repo, directory, *, owned, now, final_validation=False):
    """Manual stopped renewal only; owned verifier covers the whole next window."""
    import claude_owned_auth

    if (Path(directory) / "batch.json").exists():
        return component_transition(
            repo, Path(directory), owned=owned, now=now, resuming=True, final_validation=final_validation
        )
    plan, applied = load(directory)
    current_plan(repo, plan)
    rows = journal(directory, plan, applied)
    if not len(rows) % 2:
        refuse("no complete stopped boundary to resume")
    paused = rows[-1]["value"]
    children = replay_prefix(repo, directory, plan, paused["window"])
    value = resume(plan, applied, paused, children, now)
    owned = claude_owned_auth.require(owned)
    # current_binding adds the same fixed 300+60 credential/receipt margins.
    current = owned.current_binding(900, value["required_seconds"] - 360)
    if not owned.capability_lineage(rows[-1]["authentication"], current, 900):
        refuse("renewal is not verified same-account lineage")
    record = {"operation": "resume", "previous": digest(rows[-1]), "value": value, "authentication": current}
    private_directory(plain_path(directory / "window-transitions"))
    exclusive(plain_path(directory / "window-transitions" / f"{len(rows):02d}.json"), record, limit=2000000)
    return record


IDENTITY = ("repository", "pr", "issue", "plan_comment", "head_sha", "base_sha", "merge_base_sha")


def packet_hashes(packet):
    import packet_tree
    import review

    return {name: review.digest(path) for name, path in packet_tree.files(packet)}


def finite_authorization(value, plan, policy, now):
    """Named whole funding, with no caller-selected slots or lowered allocation."""
    if (
        type(value) is not dict
        or set(value) != {"name", "plan_digest", "policy_digest", "funding", "expires_at"}
        or type(value["name"]) is not str
        or re.fullmatch("[a-z0-9][a-z0-9-]{0,79}", value["name"]) is None
        or value["plan_digest"] != digest(validate_current_plan(plan))
        or value["policy_digest"] != digest(policy)
        or digest(value["funding"]) != digest(plan["schedule"])
        or not clock(now) + plan["schedule"]["wall_seconds"] <= clock(value["expires_at"])
    ):
        refuse("named complete finite funding differs or is insufficient")


def select_preparation(repo, directory, authorization, *, owned_auth):
    """Apply a complete batch9 preparation; this does not authorize dispatch.

    Source/catalog/full gates precede owned admission. Every original primary
    obligation receives its existing global material claim before child creation.
    A torn directory or any partial claim remains consumed, never overwritten.
    """
    import shutil
    import tempfile
    import time

    import claude_owned_auth
    import reporting_activation_v6 as activation
    import reporting_admission_v6 as admission
    import review_capacity_native_v1 as capacity
    import review_claims

    started, monotonic = time.time(), time.monotonic()
    target = plain_path(Path(directory))
    if target.exists():
        refuse("preparation already exists or is torn; no replacement")
    with tempfile.TemporaryDirectory(prefix="batch9-preparation-") as temporary:
        exported = Path(temporary) / "packet"
        actual = catalog(repo, packet_target=exported)
        plan, meta = actual["plan"], actual["metadata"]
        validate_current_plan(plan)
        if set(plan["catalog"]["binding"]) != ({"profile"} if plan["schema_version"] == 10 else set()) | {
            "local",
            "hosted",
            "source",
            "authorization",
            "context",
            "contract",
            "identity",
            "policy",
            "inventory",
        }:
            refuse("incomplete actual source/check/catalog provenance")
        policy = meta["review_policy"]
        capacity.profile(policy)
        finite_authorization(authorization, plan, policy, started)
        if packet_hashes(exported) != plan["catalog"]["files"]:
            refuse("catalog export changed before admission")
        identity = {key: meta[key] for key in IDENTITY}
        if plan["catalog"]["binding"]["identity"] != digest(identity):
            refuse("catalog identity differs from parent")
        owned = claude_owned_auth.require(owned_auth)
        evidence = admission.check(repo, owned_auth=owned, capacity_required=True)
        grant, qualification = activation.load(repo)
        last = activation.outcome(repo, 23)
        if (
            evidence.get("schema_version") != 6
            or evidence.get("grant_digest") != digest(grant)
            or set(evidence.get("outcomes", {})) != {"20", "21", "22", "23"}
            or evidence["outcomes"]["23"] != digest(last)
            or not evidence.get("empirical_receipt")
            or grant["binding"]["harness"]["head"] != identity["head_sha"]
        ):
            refuse("actual V6 capability and empirical evidence incomplete")
        applied = application(
            plan, qualification["applied_at"], started, qualification_finished=last["finished"]
        )
        if authorization["expires_at"] > applied["deadline"]:
            refuse("named expiry exceeds fixed application/outer expiry")
        # The owned verifier adds the unchanged 300+60 margins to both lifetimes.
        authentication = owned.current_binding(900, plan["schedule"]["window_seconds"][0] - 360)
        if authentication != policy["authentication"] or authentication != evidence["authentication"]:
            refuse("preparation authentication generation changed")
        current_plan(repo, plan)
        if not started <= time.time() <= started + 900 or not 0 <= time.monotonic() - monotonic <= 900:
            refuse("setup clock rollback or allocation exhausted")
        owned.recheck()
        record = {
            "schema_version": 9,
            "binding": identity,
            "contract_digest": plan["catalog"]["binding"]["contract"],
            "plan": plan,
            "authorization": copy.deepcopy(authorization),
            "unit_policy": copy.deepcopy(policy),
            "admission": evidence,
            "application": applied,
        }
        # Prove complete storage fits before any application or material claim.
        from claude_reporting import _json_bytes

        planned_claims = {
            unit["id"]: {
                "binding": material_binding(record, unit),
                "claim": review_claims.record(record, unit, material_binding(record, unit)),
            }
            for unit in plan["catalog"]["components"] + [plan["catalog"]["integration"]]
        }
        try:
            _json_bytes(record, 2000000)
            _json_bytes(planned_claims, 2000000)
        except ValueError:
            refuse("whole preparation records exceed fixed storage before claim")
        # Exclusive directory is the first durable local application boundary.
        target.mkdir(mode=0o700)
        claim(target, plan, applied)
        exclusive(target / "batch.json", record, limit=2000000)
        shutil.copytree(exported, target / "packet")
        claims = {}
        for unit in plan["catalog"]["components"] + [plan["catalog"]["integration"]]:
            if not started <= time.time() <= started + 900 or not 0 <= time.monotonic() - monotonic <= 900:
                refuse("setup exhausted during global claims; partial claims remain consumed")
            binding = material_binding(record, unit)
            claims[unit["id"]] = {
                "binding": binding,
                "claim": review_claims.reserve(repo, record, unit, binding),
            }
        if digest(claims) != digest(planned_claims):
            refuse("actual global claims differ from complete prepared bindings")
        exclusive(target / "batch-claims.json", claims, limit=2000000)
        parent = {key: value for key, value in meta.items() if key not in _result_fields()}
        parent.update(
            schema_version=7,
            kind="batch-parent",
            batch_version=9,
            batch_sha256=digest(record),
            files=packet_hashes(target / "packet"),
        )
        exclusive(target / "metadata.json", parent, limit=2000000)
        if not started <= time.time() <= started + 900 or not 0 <= time.monotonic() - monotonic <= 900:
            refuse("setup exhausted after application; claims remain consumed")
        owned.recheck()
        current_plan(repo, plan)
        if not started <= time.time() <= started + 900 or not 0 <= time.monotonic() - monotonic <= 900:
            refuse("final source checks exhausted setup; claims remain consumed")
        result = load_preparation(target)
        if not started <= time.time() <= started + 900 or not 0 <= time.monotonic() - monotonic <= 900:
            refuse("final preparation verification exhausted setup")
        return result


def _result_fields():
    import review

    return review.RESULT_FIELDS | {"batch_sha256", "batch_unit", "reservation_digest", "claim_digest"}


def material_binding(batch, unit):
    return {
        "schema_version": 1,
        "purpose": "batch9-preparation-only",
        "plan_digest": digest(batch["plan"]),
        "application_digest": digest(batch["application"]),
        "unit": unit["id"],
        "required_ids": unit["required_ids"],
        "native_seconds": 900,
        "local_seconds": 840,
        "reference_usd": 10,
    }


def load_preparation(directory):
    """Verify immutable storage. Current source/admission must be rechecked separately."""
    import review
    import review_capacity_native_v1 as capacity
    import review_claims

    directory = plain_path(Path(directory))
    meta = review.verify_packet(directory)
    batch = read(directory / "batch.json")
    if type(batch) is not dict or set(batch) != {
        "schema_version",
        "binding",
        "contract_digest",
        "plan",
        "authorization",
        "unit_policy",
        "admission",
        "application",
    }:
        refuse("unknown preparation fields")
    plan, applied = load(directory)
    if (
        type(batch["schema_version"]) is not int
        or batch["schema_version"] != 9
        or meta.get("schema_version") != 7
        or meta.get("kind") != "batch-parent"
        or type(meta.get("batch_version")) is not int
        or meta["batch_version"] != 9
        or meta.get("batch_sha256") != digest(batch)
        or digest(plan) != digest(batch["plan"])
        or digest(applied) != digest(batch["application"])
        or batch["binding"] != {key: meta[key] for key in IDENTITY}
        or digest(batch["binding"]) != plan["catalog"]["binding"].get("identity")
        or batch["contract_digest"] != plan["catalog"]["binding"].get("contract")
        or batch["unit_policy"] != meta["review_policy"]
        or digest(batch["unit_policy"]) != plan["catalog"]["binding"].get("policy")
        or meta["files"] != plan["catalog"]["files"]
    ):
        refuse("preparation identity/source/packet changed")
    capacity.profile(batch["unit_policy"])
    finite_authorization(batch["authorization"], plan, batch["unit_policy"], applied["applied_at"])
    if batch["authorization"]["expires_at"] > applied["deadline"]:
        refuse("preparation expiry exceeds fixed ceiling")
    evidence = batch["admission"]
    if (
        type(evidence) is not dict
        or type(evidence.get("schema_version")) is not int
        or evidence["schema_version"] != 6
        or not evidence.get("empirical_receipt")
        or set(evidence.get("outcomes", {})) != {"20", "21", "22", "23"}
        or evidence.get("authentication") != batch["unit_policy"]["authentication"]
    ):
        refuse("missing complete retained V6 admission; storage is not readiness")
    claims = read(directory / "batch-claims.json")
    units = plan["catalog"]["components"] + [plan["catalog"]["integration"]]
    if type(claims) is not dict or set(claims) != {u["id"] for u in units}:
        refuse("missing whole material claims")
    for unit in units:
        row = claims[unit["id"]]
        expected = material_binding(batch, unit)
        if (
            type(row) is not dict
            or set(row) != {"binding", "claim"}
            or digest(row["binding"]) != digest(expected)
        ):
            refuse("changed preparation material binding")
        if digest(row["claim"]) != digest(review_claims.record(batch, unit, expected)):
            refuse("changed material claim identity")
    return batch


def prepare_child(repo, directory, unit_id, *, owned_auth):
    """Prepare exact assigned material; no executable reservation is created."""
    import shutil
    import time

    import claude_owned_auth
    import review
    import review_claims
    from tasks import atomic_json, atomic_text

    started, monotonic = time.time(), time.monotonic()
    directory = plain_path(Path(directory))
    batch = load_preparation(directory)
    plan = batch["plan"]
    # Rebuild the original catalog, accounting only for independently replayed
    # publications from this batch. Unrelated public changes still refuse.
    current_plan(repo, plan, directory=directory)
    units = plan["catalog"]["components"] + [plan["catalog"]["integration"]]
    matches = [unit for unit in units if unit["id"] == unit_id]
    if len(matches) != 1:
        refuse("unknown child or renamed assignment")
    unit = matches[0]
    if unit_id == "integration" and len(journal(directory, plan, batch["application"])) != 2 * (
        len(plan["schedule"]["windows"]) - 2
    ):
        refuse("integration preparation requires its current complete window and verified resume")
    if unit != units[0]:
        component_prefix(repo, directory, before=unit_id)
    target = plain_path(directory / "units" / unit_id)
    if target.exists():
        refuse("child exists or is torn; no repeat preparation")
    rows = journal(directory, plan, batch["application"])
    if len(rows) % 2:
        refuse("cannot prepare inside a paused window")
    window = len(rows) // 2
    if unit_id not in plan["schedule"]["windows"][window]:
        refuse("child is outside the current complete window")
    window_clock(plan, batch["application"], rows, window, started)
    window_start = rows[-1]["value"]["resumed_at"] if rows else batch["application"]["applied_at"]
    if (
        started + (1740 if window == 0 else 900)
        > window_start + plan["schedule"]["window_seconds"][window] - 360
    ):
        refuse("complete child allocation does not fit the current window")
    if started + 1740 > min(batch["application"]["wall_deadline"], batch["authorization"]["expires_at"]):
        refuse("complete child allocation does not fit")
    claims = read(directory / "batch-claims.json")
    for candidate in units:
        review_claims.verify(repo, batch, candidate, claims[candidate["id"]])
    extra = {"items": [], "files": {}, "dependencies": {}}
    if unit_id == "integration":
        extra = integration_material(repo, directory, batch)
    policy, transition = child_window_policy(directory, batch, unit_id)
    owned = claude_owned_auth.require(owned_auth)
    actual_admission = _component_admission(repo, directory, owned_auth=owned)
    if digest(actual_admission) != digest(batch["admission"]):
        refuse("current V6 admission differs; renewal runtime is not available")
    if (
        owned.current_binding(900, plan["schedule"]["window_seconds"][window] - 360)
        != policy["authentication"]
    ):
        refuse("current whole-window authentication changed")
    current_plan(repo, plan, directory=directory)
    checked = time.time()
    window_clock(plan, batch["application"], rows, window, checked)
    if not started <= checked <= started + 840 or not 0 <= time.monotonic() - monotonic <= 840:
        refuse("preparation local allocation exhausted or clock rolled back")
    owned.recheck()
    target.parent.mkdir(mode=0o700, exist_ok=True)
    target.mkdir(mode=0o700)
    shutil.copytree(directory / "packet", target / "packet")
    packet = target / "packet"
    for name, raw in extra["files"].items():
        path = plain_path(packet / name)
        if path.exists():
            refuse("integration would replace existing source")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
    if extra["items"]:
        inventory = read(packet / "required-material.json")
        inventory["required"].extend(extra["items"])
        atomic_json(packet / "required-material.json", inventory)
        atomic_text(packet / "inventory-sha256.txt", review.digest(packet / "required-material.json") + "\n")
    assignment = {
        "schema_version": 9,
        "batch_sha256": digest(batch),
        "unit": unit,
        "required_ids": unit["required_ids"] + [item["id"] for item in extra["items"]],
        "context_ids": surrounding_current_ids(plan, unit),
        "dependencies": extra["dependencies"],
        "material_claim": digest(claims[unit_id]),
        "policy_digest": digest(policy),
        "authorization_digest": digest(batch["authorization"]),
    }
    if transition is not None:
        assignment["window_transition"] = transition
    atomic_json(packet / "assignment.json", assignment)
    meta = review.verify_packet(directory)
    meta = {key: value for key, value in meta.items() if key not in _result_fields()}
    meta.update(
        kind="batch-unit",
        batch_version=9,
        batch_unit=assignment,
        files=packet_hashes(packet),
        review_policy=policy,
    )
    exclusive(target / "metadata.json", meta, limit=2000000)
    if not started <= time.time() <= started + 840 or not 0 <= time.monotonic() - monotonic <= 840:
        refuse("preparation overrun after storage; material remains consumed")
    owned.recheck()
    exclusive(
        target / "batch-preparation.json",
        {
            "schema_version": 1,
            "batch_sha256": digest(batch),
            "assignment_sha256": digest(assignment),
            "started": started,
            "finished": time.time(),
            "local_deadline": started + 840,
            "action_deadline": min(
                started + 1740,
                batch["application"]["wall_deadline"],
                batch["authorization"]["expires_at"],
                child_window_deadline(directory, batch, unit_id),
            ),
            "monotonic_seconds": time.monotonic() - monotonic,
        },
    )
    if not started <= time.time() <= started + 840 or not 0 <= time.monotonic() - monotonic <= 840:
        refuse("final owned preparation checks exhausted local allocation")
    exclusive(
        target / "batch-preparation-clock.json",
        {
            "schema_version": 1,
            "preparation_sha256": digest(read(target / "batch-preparation.json")),
            "wall_started": started,
            "monotonic_started": monotonic,
            "wall_finished": time.time(),
            "monotonic_finished": time.monotonic(),
        },
    )
    return target


def verify_child(repo, directory):
    """Recompute prepared component identity and actual global claims, not readiness."""
    import review
    import review_claims

    directory = plain_path(Path(directory))
    parent = directory.parent.parent
    batch = load_preparation(parent)
    meta = review.verify_packet(directory)
    assignment = read(directory / "packet/assignment.json")
    if (
        meta.get("batch_version") != 9
        or meta.get("kind") != "batch-unit"
        or meta.get("batch_unit") != assignment
    ):
        refuse("child metadata/assignment changed")
    unit = assignment.get("unit")
    units = batch["plan"]["catalog"]["components"] + [batch["plan"]["catalog"]["integration"]]
    if not any(digest(unit) == digest(candidate) for candidate in units):
        refuse("child is outside the exact component/integration catalog")
    if directory != parent / "units" / unit["id"]:
        refuse("copied or renamed child")
    claims = read(parent / "batch-claims.json")
    review_claims.verify(repo, batch, unit, claims[unit["id"]])
    policy, transition = child_window_policy(parent, batch, unit["id"])
    extra = (
        integration_material(repo, parent, batch)
        if unit["id"] == "integration"
        else {"items": [], "files": {}, "dependencies": {}}
    )
    expected = {
        "schema_version": 9,
        "batch_sha256": digest(batch),
        "unit": unit,
        "required_ids": unit["required_ids"] + [item["id"] for item in extra["items"]],
        "context_ids": surrounding_current_ids(batch["plan"], unit),
        "dependencies": extra["dependencies"],
        "material_claim": digest(claims[unit["id"]]),
        "policy_digest": digest(policy),
        "authorization_digest": digest(batch["authorization"]),
    }
    if transition is not None:
        expected["window_transition"] = transition
    if digest(assignment) != digest(expected) or any(meta[k] != batch["binding"][k] for k in IDENTITY):
        refuse("child source/contract/owner/assignment changed")
    expected_files = {
        **batch["plan"]["catalog"]["files"],
        "assignment.json": review.digest(directory / "packet/assignment.json"),
    }
    if extra["items"]:
        import hashlib
        import json

        inventory = read(parent / "packet/required-material.json")
        inventory["required"].extend(extra["items"])
        raw = (json.dumps(inventory, indent=2, ensure_ascii=False) + "\n").encode("utf-8")
        extra_files = {
            **extra["files"],
            "required-material.json": raw,
            "inventory-sha256.txt": (hashlib.sha256(raw).hexdigest() + "\n").encode(),
        }
        expected_files.update({name: hashlib.sha256(raw).hexdigest() for name, raw in extra_files.items()})
    if meta["files"] != expected_files or meta["review_policy"] != policy:
        refuse("child omits or changes surrounding material or policy")
    timing = read(directory / "batch-preparation.json")
    if type(timing) is not dict or set(timing) != {
        "schema_version",
        "batch_sha256",
        "assignment_sha256",
        "started",
        "finished",
        "local_deadline",
        "action_deadline",
        "monotonic_seconds",
    }:
        refuse("missing exact preparation timing")
    start, finish = clock(timing["started"]), clock(timing["finished"])
    if (
        type(timing["schema_version"]) is not int
        or timing["schema_version"] != 1
        or timing["batch_sha256"] != digest(batch)
        or timing["assignment_sha256"] != digest(assignment)
        or not batch["application"]["applied_at"] <= start <= finish <= start + 840
        or not 0 <= clock(timing["monotonic_seconds"]) <= 840
        or timing["local_deadline"] != start + 840
        or timing["action_deadline"]
        != min(
            start + 1740,
            batch["application"]["wall_deadline"],
            batch["authorization"]["expires_at"],
            child_window_deadline(parent, batch, unit["id"]),
        )
    ):
        refuse("preparation clock, allocation or timing changed")
    return meta


RUNTIME = "batch-runtime-reservation.json"
OUTPUT = "batch-owned-output.json"
OBSERVATION = "batch-context-observation.json"
COMPLETION = "batch-runtime-completion.json"


def runtime_identity(repo, directory):
    """Offline exact component, material claim and executable reservation binding."""
    import review
    import review_capacity_native_v1 as capacity

    directory = plain_path(Path(directory))
    meta = verify_child(repo, directory)
    capacity.profile(meta["review_policy"])
    batch = load_preparation(directory.parent.parent)
    preparation = read(directory / "batch-preparation.json")
    origin = read(directory / "batch-preparation-clock.json")
    if (
        type(origin) is not dict
        or set(origin)
        != {
            "schema_version",
            "preparation_sha256",
            "wall_started",
            "monotonic_started",
            "wall_finished",
            "monotonic_finished",
        }
        or type(origin["schema_version"]) is not int
        or origin["schema_version"] != 1
    ):
        refuse("missing exact preparation clock origin")
    if (
        origin["preparation_sha256"] != digest(preparation)
        or origin["wall_started"] != preparation["started"]
        or not origin["wall_started"] <= clock(origin["wall_finished"]) <= preparation["local_deadline"]
        or not 0 <= clock(origin["monotonic_finished"]) - clock(origin["monotonic_started"]) <= 840
        or abs(
            (origin["wall_finished"] - origin["wall_started"])
            - (origin["monotonic_finished"] - origin["monotonic_started"])
        )
        > 1
    ):
        refuse("preparation wall/monotonic origin differs")
    return (
        meta,
        batch,
        {
            "schema_version": 1,
            "directory": str(directory.resolve()),
            "batch_sha256": digest(batch),
            "input_digest": digest({k: v for k, v in meta.items() if k not in review.RESULT_FIELDS}),
            "assignment_sha256": digest(meta["batch_unit"]),
            "preparation_sha256": digest(preparation),
            "preparation_clock_sha256": digest(origin),
            "material_claim": meta["batch_unit"]["material_claim"],
            "policy_digest": digest(meta["review_policy"]),
            "admission_digest": digest(batch["admission"]),
            "started": preparation["started"],
            "local_deadline": preparation["local_deadline"],
            "action_deadline": preparation["action_deadline"],
            "native_seconds": 900,
            "local_seconds": 840,
            "reference_usd": 10,
            "wrapper_invocations": 1,
        },
    )


def runtime_reservation(repo, directory):
    meta, batch, expected = runtime_identity(repo, directory)
    if digest(read(Path(directory) / RUNTIME)) != digest(expected):
        refuse("executable reservation differs from original preparation and global claim")
    if digest(read(Path(directory) / "reporting-admission.json")) != digest(batch["admission"]):
        refuse("executable admission differs")
    import review_claims

    global_path = review_claims.root(repo) / "batch9-executions" / (expected["material_claim"] + ".json")
    if digest(read(global_path)) != digest(expected):
        refuse("global executable claim missing, copied or changed")
    return meta, batch, expected


class ChildDispatch:
    """One owned component invocation; original preparation deadlines never move.

    The in-memory object is only a routing guard. Every launch rederives packet,
    catalog, admission and durable global/executable claims independently.
    """

    def __init__(self, repo, directory):
        import time

        self.repo, self.directory = repo, plain_path(Path(directory))
        self.wall, self.monotonic = time.time(), time.monotonic()
        self.meta, self.batch, self.reservation = runtime_identity(repo, self.directory)
        self.origin = read(self.directory / "batch-preparation-clock.json")
        self.last, self.last_mono = self.wall, self.monotonic
        self.clocks = []
        self.native = 0.0
        self.native_start = None
        self.claimed = False
        self.check_clock()

    def check_clock(self):
        import time

        now, mono = time.time(), time.monotonic()
        elapsed = now - self.reservation["started"]
        if (
            now < self.last
            or mono < self.last_mono
            or now < self.origin["wall_finished"]
            or mono < self.origin["monotonic_finished"]
            or abs((now - self.origin["wall_started"]) - (mono - self.origin["monotonic_started"])) > 1
            or abs((now - self.wall) - (mono - self.monotonic)) > 1
            or elapsed < 0
            or elapsed - self.native > 840
            or elapsed > 1740
            or now > self.reservation["action_deadline"]
            or not 0 <= self.native <= 900
        ):
            refuse("component clock rollback or native/local/action allocation exhausted")
        self.last, self.last_mono = now, mono
        self.clocks.append({"wall": now, "monotonic": mono, "native_seconds": self.native})
        if len(self.clocks) > 400:
            refuse("component operation clock record exceeds bound")
        return now

    def recheck(self, meta, *, owned_auth):
        import claude_owned_auth

        self.check_clock()
        owned = claude_owned_auth.require(owned_auth)
        import review

        actual, batch, reservation = runtime_identity(self.repo, self.directory)
        if digest({k: v for k, v in meta.items() if k not in review.RESULT_FIELDS}) != digest(
            {k: v for k, v in actual.items() if k not in review.RESULT_FIELDS}
        ) or digest(reservation) != digest(self.reservation):
            refuse("dispatch packet, assignment, source or preparation changed")
        self.check_clock()
        current_plan(self.repo, batch["plan"], directory=self.directory.parent.parent)
        self.check_clock()
        if digest(_component_admission(self.repo, self.directory.parent.parent, owned_auth=owned)) != digest(
            batch["admission"]
        ):
            refuse("actual V6 admission changed; successor lineage awaits published prefix")
        self.check_clock()
        parent = self.directory.parent.parent
        rows = journal(parent, batch["plan"], batch["application"])
        if len(rows) % 2:
            refuse("stopped-window runtime requires a complete resume")
        window = len(rows) // 2
        if meta["batch_unit"]["unit"]["id"] not in batch["plan"]["schedule"]["windows"][window]:
            refuse("component is not in the current declared window")
        unit = meta["batch_unit"]["unit"]
        components = batch["plan"]["catalog"]["components"]
        if unit != components[0]:
            component_prefix(self.repo, parent, before=unit["id"])
            self.check_clock()
        window_clock(batch["plan"], batch["application"], rows, window, self.check_clock())
        if (
            owned.current_binding(
                900, batch["plan"]["schedule"]["window_seconds"][active_component_window(parent, batch)] - 360
            )
            != meta["review_policy"]["authentication"]
        ):
            refuse("initial whole-window authentication changed")
        self.check_clock()
        if self.claimed:
            runtime_reservation(self.repo, self.directory)
            self.check_clock()

    def claim(self, repo, directory, meta, *, owned_auth):
        if self.claimed or repo is not self.repo or Path(directory) != self.directory:
            refuse("copied, competing or repeated executable dispatch")
        self.recheck(meta, owned_auth=owned_auth)
        # A torn pair stays consumed. No replacement, refund or retry path exists.
        import review_claims

        with review_claims.locked(repo):
            namespace = plain_path(review_claims.root(repo) / "batch9-executions")
            namespace.mkdir(mode=0o700, exist_ok=True)
            exclusive(namespace / (self.reservation["material_claim"] + ".json"), self.reservation)
            exclusive(self.directory / RUNTIME, self.reservation)
        exclusive(self.directory / "reporting-admission.json", self.batch["admission"], limit=2000000)
        self.claimed = True
        self.check_clock()

    def begin_native(self, meta, *, owned_auth):
        if not self.claimed or self.native_start is not None:
            refuse("missing or repeated executable claim")
        self.recheck(meta, owned_auth=owned_auth)
        # The full native allocation must still fit; local time cannot borrow it.
        if self.check_clock() + 900 > self.reservation["action_deadline"]:
            refuse("complete native allocation no longer fits")
        return 900

    def start_native(self, owned_auth):
        import time

        import claude_owned_auth

        owned = claude_owned_auth.require(owned_auth)
        if not self.claimed or self.native_start is not None:
            refuse("native process is unclaimed or already started")
        if (
            owned.current_binding(
                900,
                self.batch["plan"]["schedule"]["window_seconds"][
                    active_component_window(self.directory.parent.parent, self.batch)
                ]
                - 360,
            )
            != self.meta["review_policy"]["authentication"]
        ):
            refuse("final owned whole-window identity differs")
        if self.check_clock() + 900 > self.reservation["action_deadline"]:
            refuse("final complete native allocation does not fit")
        self.native_start = (time.time(), time.monotonic())

    def native_returned(self):
        import time

        self.native_end = (time.time(), time.monotonic())

    def end_native(self):
        if self.native_start is None:
            refuse("native completion without start")
        wall, mono = self.native_end
        self.native = wall - self.native_start[0]
        if abs(self.native - (mono - self.native_start[1])) > 1:
            refuse("native wall/monotonic clock differs")
        self.check_clock()


def require_dispatch(repo, directory, meta, dispatch, owned_auth):
    if (
        type(dispatch) is not ChildDispatch
        or dispatch.repo is not repo
        or dispatch.directory != Path(directory)
    ):
        refuse("batch9 requires its actual owned executable dispatch")
    dispatch.recheck(meta, owned_auth=owned_auth)
    return dispatch


def observation_binding(meta, batch, capture):
    import claude_context_observation_v1 as observer
    import review_capacity_native_v1 as capacity

    values = {
        "input": capture["input_digest"],
        "policy": capacity.sha(capacity.encoded(meta["review_policy"])),
        "execution": capacity.sha(capacity.encoded(capture["execution"])),
        "fixture": capacity.sha(capacity.encoded(meta["batch_unit"])),
        "source": capacity.sha(capacity.encoded(batch["plan"]["catalog"])),
        "descriptor": capacity.sha(observer._bytes(observer.DESCRIPTOR)),
        "report": capacity.sha(capture["body"].encode("utf-8")),
        "proof": capacity.sha(capacity.encoded(capture["reporting"])),
        "diagnostic": capacity.sha(capacity.encoded(capture["diagnostics"])),
    }
    return {key + "_sha256": value for key, value in values.items()}


def persist_child_capture(dispatch, meta, owned_auth, raw, workspace, session, body, diagnostics, proof):
    import os
    import time

    import claude_context_observation_v1 as observer
    import claude_owned_auth
    import review
    import review_capacity_native_v1 as capacity

    owned = claude_owned_auth.require(owned_auth)
    directory = dispatch.directory
    # Save sanitized partial output even when proof, clocks or observer refuse.
    exclusive(
        directory / OUTPUT, {"body": body, "diagnostics": diagnostics, "reporting": proof}, limit=8000000
    )
    review.save_result(
        directory, meta, body, diagnostics, meta["review_policy"]["cli"]["version"], reporting=proof
    )
    dispatch.end_native()
    dispatch.recheck(meta, owned_auth=owned)
    capture = review.read_result_artifact(directory, "review-capture.json", meta)
    bindings = observation_binding(meta, dispatch.batch, capture)
    input_bytes = input_accounting(raw, directory, meta, diagnostics)
    dispatch.check_clock()
    sidecar, correlation = capacity.batch_bridge(
        raw,
        directory / "packet",
        workspace,
        meta["review_policy"],
        session,
        bindings,
        meta["batch_unit"]["required_ids"],
    )
    dispatch.check_clock()
    fd = os.open(plain_path(directory / OBSERVATION), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(sidecar)
        stream.flush()
        os.fsync(stream.fileno())
    owned.recheck()
    dispatch.check_clock()
    capture_hash = review.digest(directory / "review-capture.json")
    exclusive(
        directory / COMPLETION,
        {
            "schema_version": 1,
            "reservation_sha256": digest(dispatch.reservation),
            "capture_sha256": capture_hash,
            "output_sha256": review.digest(directory / OUTPUT),
            "bindings": bindings,
            "input_accounting": input_bytes,
            "observation": observer.completion(sidecar, capture_hash),
            "correlation": correlation,
            "finished": dispatch.check_clock(),
            "native_seconds": dispatch.native,
            "runtime_started": dispatch.wall,
            "runtime_monotonic_seconds": time.monotonic() - dispatch.monotonic,
        },
        limit=100000,
    )
    dispatch.check_clock()


def run_child(repo, directory):
    """Declared component windows only. No batch-wide CLI or aggregate readiness credit."""
    import claude_owned_auth
    import review_claude

    dispatch = ChildDispatch(repo, directory)
    current_plan(repo, dispatch.batch["plan"], directory=Path(directory).parent.parent)
    dispatch.check_clock()
    if any(
        plain_path(Path(directory) / name).exists()
        for name in (
            RUNTIME,
            "attempt.json",
            "reporting-execution.json",
            OUTPUT,
            COMPLETION,
        )
    ):
        refuse("prior or torn child execution cannot be repeated; offline recovery only")
    try:
        with claude_owned_auth.snapshot(dispatch.meta["review_policy"]) as owned:
            dispatch.check_clock()
            review_claude.execute(repo, directory, dispatch.meta, dispatch_context=dispatch, owned_auth=owned)
            dispatch.check_clock()
            result = _replay_child(repo, directory)
            dispatch.check_clock()
            _materialize_child(repo, directory)
            dispatch.check_clock()
            dispatch.recheck(dispatch.meta, owned_auth=owned)
            dispatch.check_clock()
            exclusive(
                Path(directory) / "batch-runtime-acknowledged.json",
                {
                    "schema_version": 1,
                    "reservation_sha256": digest(dispatch.reservation),
                    "completion_sha256": result["completion_sha256"],
                    "clocks": dispatch.clocks,
                },
                limit=100000,
            )
            dispatch.check_clock()
            return result
    except BaseException:
        if plain_path(Path(directory) / RUNTIME).exists():
            # Stop marker says only that the operation did not acknowledge success.
            # It does not fabricate provider failure, zero use or a successor slot.
            marker = Path(directory) / "batch-runtime-interrupted.json"
            if not plain_path(marker).exists():
                exclusive(
                    marker,
                    {
                        "schema_version": 1,
                        "reservation_sha256": digest(dispatch.reservation),
                        "status": "incomplete-no-repeat",
                    },
                )
        raise


def _replay_child(repo, directory):
    """Independent offline child replay; never parent or current admission readiness."""
    import claude_context_observation_v1 as observer
    import review
    import review_capacity_native_v1 as capacity
    from reporting_recovery_history_v6 import known_usage

    directory = plain_path(Path(directory))
    meta, batch, reservation = runtime_reservation(repo, directory)
    if plain_path(directory / "batch-runtime-interrupted.json").exists():
        refuse("interrupted child stays incomplete; no reconstructed success")
    result, assessment = review.stored_result(directory, meta)
    for key in review.RESULT_FIELDS:
        if key in meta and meta[key] != result.get(key):
            refuse("completed child metadata differs from exact capture")
    for name, key, exact in (
        ("review.md", "review_sha256", True),
        ("terminal.txt", "terminal_sha256", True),
        ("diagnostics.json", "diagnostics_sha256", False),
        ("coverage.json", "coverage_sha256", False),
        ("reporting-proof.json", "reporting_sha256", False),
    ):
        path = plain_path(directory / name)
        if path.exists() or key in meta:
            observed = (
                review.digest(path) if exact else digest(review.read_result_artifact(directory, name, meta))
            )
            if observed != result[key]:
                refuse("materialized child report or evidence changed")
    completion = read(directory / COMPLETION)
    expected_keys = {
        "schema_version",
        "reservation_sha256",
        "capture_sha256",
        "output_sha256",
        "bindings",
        "observation",
        "correlation",
        "finished",
        "native_seconds",
        "runtime_started",
        "runtime_monotonic_seconds",
        "input_accounting",
    }
    if (
        type(completion) is not dict
        or set(completion) != expected_keys
        or type(completion["schema_version"]) is not int
        or completion["schema_version"] != 1
    ):
        refuse("missing exact child completion")
    totals = completion["input_accounting"]
    if (
        type(totals) is not dict
        or set(totals) != {"input_bytes", "optional_bytes", "navigation_bytes", "protocol_bytes"}
        or any(type(v) is not int or not 0 <= v <= capacity.DESCRIPTOR[k] for k, v in totals.items())
    ):
        refuse("missing or over-budget retained input accounting")
    bindings = observation_binding(meta, batch, result)
    if (
        completion["reservation_sha256"] != digest(reservation)
        or completion["capture_sha256"] != review.digest(directory / "review-capture.json")
        or completion["output_sha256"] != review.digest(directory / OUTPUT)
        or completion["bindings"] != bindings
        or digest(read(directory / OUTPUT))
        != digest(
            {"body": result["body"], "diagnostics": result["diagnostics"], "reporting": result["reporting"]}
        )
    ):
        refuse("child capture, output, claim or observation identity changed")
    finished = clock(completion["finished"])
    native = clock(completion["native_seconds"])
    runtime_start = clock(completion["runtime_started"])
    mono = clock(completion["runtime_monotonic_seconds"])
    if (
        not reservation["started"] <= runtime_start <= finished <= reservation["action_deadline"]
        or not 0 <= native <= 900
        or not 0 <= finished - reservation["started"] - native <= 840
        or abs(finished - runtime_start - mono) > 1
    ):
        refuse("child immutable timing or allocations differ")
    if not known_usage(result["diagnostics"].get("usage"), 900, 10):
        refuse("unknown or over-budget child usage")
    required = set(meta["batch_unit"]["required_ids"])
    if (
        result["diagnostics"]["reasons"]
        or assessment["reasons"]
        or not result["reporting"]["accepted"]
        or required - {row["id"] for row in assessment["material"] if row["state"] == "reviewed"}
    ):
        refuse("assigned component evidence is incomplete")
    summary = observer.replay_completed(
        review.exact_reporting_bytes(directory / OBSERVATION, observer.MAX_BYTES),
        completion["observation"],
        completion["capture_sha256"],
        bindings,
        result["diagnostics"]["usage"]["counters"],
        completion["correlation"],
    )
    if summary["max_observed_input"] > capacity.DESCRIPTOR["batch_observed_input"]:
        refuse("component observed input exceeds fixed bound")
    return {
        "schema_version": 1,
        "scope": "integration-only"
        if meta["batch_unit"]["unit"]["id"] == "integration"
        else "component-only",
        "required_ids": sorted(required),
        "qualified": True,
        "usage": result["diagnostics"]["usage"],
        "completion_sha256": digest(completion),
    }


def _materialize_child(repo, directory):
    """Derive exact report artifacts from replayed capture without readiness credit."""
    import review
    from tasks import atomic_json

    directory = plain_path(Path(directory))
    meta, _, _ = runtime_reservation(repo, directory)
    if not plain_path(directory / "review-result.json").exists():
        capture = review.read_result_artifact(directory, "review-capture.json", meta)
        inputs = {k: v for k, v in meta.items() if k not in review.RESULT_FIELDS}
        if capture.get("input_digest") != digest(inputs):
            refuse("offline capture belongs to another input")
        review.save_result(
            directory,
            inputs,
            capture["body"],
            capture["diagnostics"],
            capture["provider_version"],
            reporting=capture["reporting"],
        )
    _replay_child(repo, directory)
    result, assessment = review.stored_result(directory, meta)
    from claude_reporting import _json_bytes

    artifacts = {
        "review.md": result["body"].encode("utf-8"),
        "terminal.txt": result["reporting"]["terminal_text"].encode("utf-8"),
        "diagnostics.json": _json_bytes(result["diagnostics"], 2000000),
        "coverage.json": _json_bytes(assessment, 8000000),
        "reporting-proof.json": _json_bytes(result["reporting"], 2000000),
    }
    import os

    for name, raw in artifacts.items():
        path = plain_path(directory / name)
        if path.exists():
            if review.exact_reporting_bytes(path, len(raw)) != raw:
                refuse("offline recovery would replace existing evidence")
            continue
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
    fields = {key: result[key] for key in review.RESULT_FIELDS if key in result}
    if any(key in meta and meta[key] != value for key, value in fields.items()):
        refuse("offline completed metadata differs")
    atomic_json(directory / "metadata.json", {**meta, **fields})
    return directory / "review.md"


def input_accounting(raw, directory, meta, diagnostics):
    """Conservative transient byte accounting, not tokenizer/context proof.

    Mixed or repeated source responses charge their whole returned content to
    optional source. All other responses charge navigation. JSON framing, model
    output and escaping stay in protocol overhead rather than disappearing.
    Only bounded numeric totals survive; exact diagnostic hashes bind the reads.
    """
    import review_capacity_native_v1 as capacity
    import review_coverage as coverage

    inventory = read(Path(directory) / "packet/required-material.json")["required"]
    required_ids = set(meta["batch_unit"]["required_ids"])
    required = {
        (i["artifact"], n)
        for i in inventory
        if i["id"] in required_ids
        for n in range(i["start_line"], i["end_line"] + 1)
    }
    source = {(i["artifact"], n) for i in inventory for n in range(i["start_line"], i["end_line"] + 1)}
    seen, index, content_bytes, optional, navigation = set(), 0, 0, 0, 0
    for line in raw.decode("utf-8").splitlines():
        event = coverage.strict_json(line)
        if event.get("type") != "user":
            continue
        for block in event["message"]["content"]:
            # StructuredOutput acknowledgment is protocol, not source evidence.
            if index >= len(diagnostics["events"]):
                continue
            record = diagnostics["events"][index]
            if coverage.checksum(block["content"]) != record["result_sha256"]:
                refuse("byte accounting does not match exact tool evidence")
            index += 1
            amount = len(block["content"].encode("utf-8"))
            content_bytes += amount
            spans = {
                (s["artifact"], n) for s in record["spans"] for n in range(s["start_line"], s["end_line"] + 1)
            }
            if spans & source:
                if record["tool"] != "view" or spans - required or spans & seen:
                    optional += amount
                seen.update(spans)
            else:
                navigation += amount
    from review_prompt import native

    fixed_protocol = len(native(directory, meta).encode("utf-8")) + len(
        (Path(directory) / "packet/report-schema.json").read_bytes()
    )
    totals = {
        "input_bytes": len(raw) + fixed_protocol,
        "optional_bytes": optional,
        "navigation_bytes": navigation,
        "protocol_bytes": len(raw) - content_bytes + fixed_protocol,
    }
    if index != len(diagnostics["events"]) or any(
        type(v) is not int or not 0 <= v <= capacity.DESCRIPTOR[k] for k, v in totals.items()
    ):
        refuse("native input, optional, navigation or protocol byte allocation exceeded")
    return totals


def qualify_child(repo, directory):
    """Require complete owned-operation acknowledgment as well as exact output replay."""
    result = _replay_child(repo, directory)
    directory = Path(directory)
    record = read(directory / "batch-runtime-acknowledged.json")
    reservation = read(directory / RUNTIME)
    origin = read(directory / "batch-preparation-clock.json")
    if (
        type(record) is not dict
        or set(record) != {"schema_version", "reservation_sha256", "completion_sha256", "clocks"}
        or type(record["schema_version"]) is not int
        or record["schema_version"] != 1
        or record["reservation_sha256"] != digest(reservation)
        or record["completion_sha256"] != result["completion_sha256"]
    ):
        refuse("missing or changed final owned-operation acknowledgment")
    clocks = record["clocks"]
    if type(clocks) is not list or not 1 <= len(clocks) <= 400:
        refuse("missing bounded operation clocks")
    completion = read(directory / COMPLETION)
    previous_wall, previous_mono = origin["wall_finished"], origin["monotonic_finished"]
    previous_native = 0
    for row in clocks:
        if type(row) is not dict or set(row) != {"wall", "monotonic", "native_seconds"}:
            refuse("invalid operation clock record")
        wall, mono, native = (clock(row[key]) for key in ("wall", "monotonic", "native_seconds"))
        if (
            not previous_wall <= wall <= reservation["action_deadline"]
            or mono < previous_mono
            or native not in (0, completion["native_seconds"])
            or native < previous_native
            or not 0 <= native <= 900
            or not 0 <= wall - reservation["started"] - native <= 840
            or abs((wall - origin["wall_started"]) - (mono - origin["monotonic_started"])) > 1
        ):
            refuse("operation clock rollback, drift or allocation overrun")
        previous_wall, previous_mono, previous_native = wall, mono, native
    if previous_wall < completion["finished"] or previous_native != completion["native_seconds"]:
        refuse("operation acknowledgment predates capture completion")
    return result


def recover_child(repo, directory):
    """Pure offline storage recovery; never launch, authorize or extend deadlines."""
    report = _materialize_child(repo, directory)
    qualify_child(repo, directory)
    return report


PUBLICATION_INTENT = "batch-publication-intent.json"
PUBLICATION_ACK = "batch-publication-acknowledged.json"
PUBLICATION_FAILURE = "batch-publication-uncertain.json"
PUBLIC_CONTEXT = (
    "pulls/32/reviews",
    "pulls/32/comments",
    "issues/32/comments",
    "issues/31/comments",
    "issues/31",
)


def publication_actor(repo):
    """Resolve the actual GitHub credential's actor; never accept a caller assertion."""
    import review_coverage
    import workflow

    result = workflow.run(["gh", "api", "--hostname", "github.com", "user"], cwd=repo.root, check=False)
    if result.returncode or len(result.stdout.encode("utf-8")) > 65536:
        refuse("authenticated GitHub actor unavailable")
    value = review_coverage.strict_json(result.stdout)
    return _publication_actor(value)


def _publication_actor(value):
    if (
        type(value) is not dict
        or type(value.get("id")) is not int
        or value["id"] <= 0
        or type(value.get("login")) is not str
        or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]{0,38}(?:\[bot\])?", value["login"]) is None
    ):
        refuse("invalid authenticated publication actor")
    return {"id": value["id"], "login": value["login"]}


class PublicationClock:
    """Publication shares the original child clock; recovery grants no new time."""

    def __init__(self, directory):
        self.exhausted = False
        self.directory = Path(directory)
        self.origin = read(self.directory / "batch-preparation-clock.json")
        self.reservation = read(self.directory / RUNTIME)
        self.native = clock(read(self.directory / COMPLETION)["native_seconds"])
        self.rows = copy.deepcopy(read(self.directory / "batch-runtime-acknowledged.json")["clocks"][-1:])
        self.check()

    def check(self):
        import time

        now, mono = time.time(), time.monotonic()
        last = self.rows[-1]
        if (
            clock(now) < last["wall"]
            or clock(mono) < last["monotonic"]
            or abs(now - self.origin["wall_started"] - (mono - self.origin["monotonic_started"])) > 1
            or not 0 <= now - self.reservation["started"] - self.native <= 840
            or now - self.reservation["started"] > 1740
            or now > self.reservation["action_deadline"]
            or len(self.rows) >= 400
        ):
            self.exhausted = True
            refuse("publication clock rollback or original child allocation exhausted")
        self.rows.append({"wall": now, "monotonic": mono, "native_seconds": self.native})
        return now


def _publication_identity(repo, directory):
    """Requalify all owned artifacts and global claims, without another invocation."""
    identity, report, meta, batch, _ = _publication_evidence(repo, directory)
    return identity, report, meta, batch


def _publication_evidence(repo, directory):
    """Return internally replayed qualification with its same-sweep identity."""
    import review

    directory = plain_path(Path(directory))
    qualified = qualify_child(repo, directory)
    meta, batch, reservation = runtime_reservation(repo, directory)
    if meta["repository"] != repo.name:
        refuse("publication repository differs")
    unit = meta["batch_unit"]["unit"]
    components = batch["plan"]["catalog"]["components"] + [batch["plan"]["catalog"]["integration"]]
    sequence = next(i for i, item in enumerate(components) if item == unit)
    # Historical publications remain replayable across declared stopped boundaries.
    child_window_policy(directory.parent.parent, batch, unit["id"])
    report = review.exact_reporting_bytes(directory / "review.md", 10000)
    if review.coverage.checksum(report.decode("utf-8")) != meta["review_sha256"]:
        refuse("publication report differs")
    artifacts = (
        RUNTIME,
        OUTPUT,
        OBSERVATION,
        COMPLETION,
        "batch-runtime-acknowledged.json",
        "reporting-execution.json",
        "review-capture.json",
        "reporting-proof.json",
        "diagnostics.json",
        "coverage.json",
        "terminal.txt",
        "review.md",
        "metadata.json",
    )
    identity = {
        "directory": str(directory),
        "parent": str(directory.parent.parent),
        "batch_sha256": digest(batch),
        "catalog_sha256": batch["plan"]["catalog_digest"],
        "unit": unit["id"],
        "sequence": sequence,
        "claim": reservation["material_claim"],
        "binding": copy.deepcopy(batch["binding"]),
        "qualification": digest(qualified),
        "artifacts": {name: review.digest(directory / name) for name in artifacts},
    }
    return identity, report, meta, batch, qualified


def _publication_body(identity, report, operation):
    import uuid

    if type(operation) is not str or str(uuid.UUID(operation)) != operation:
        refuse("invalid publication operation identity")
    scope = "integration" if identity["unit"] == "integration" else "component"
    marker = f"<!-- agentic-batch9-{scope}:{operation}:{digest(identity)} -->"
    body = (
        f"Scoped model review — {scope} only; the full PR review remains incomplete.\n"
        f"Unit: {identity['unit']}; sequence: {identity['sequence']}; "
        f"head: {identity['binding']['head_sha']}.\n"
        "Observed reads do not prove understanding. This COMMENT is not human approval.\n\n"
        + report.decode("utf-8")
        + "\n\n"
        + marker
    )
    if len(body.encode("utf-8")) > 60000:
        refuse("scoped publication exceeds exact body limit")
    return body, marker


def _publication_context(repo, timer):
    value = {}
    for endpoint in PUBLIC_CONTEXT:
        timer.check()
        rows = [repo.api(endpoint)] if endpoint == "issues/31" else repo.api(endpoint, paginate=True)
        timer.check()
        if type(rows) is not list or any(type(r) is not dict or type(r.get("id")) is not int for r in rows):
            refuse("unsupported remote context shape")
        if len({r["id"] for r in rows}) != len(rows):
            refuse("ambiguous remote context IDs")
        value[endpoint] = rows
    return value


def _publication_source(repo, meta, timer):
    import check_runner
    import review

    timer.check()
    review.current_pr(repo, meta["pr"], meta["head_sha"], meta["base_sha"])
    timer.check()
    source = check_runner.source(repo.root)
    timer.check()
    if source["checkout"] != meta["head_sha"]:
        refuse("publication source checkout changed")
    return digest(source)


def _publication_intent(repo, directory, timer):
    import hashlib

    intent = read(Path(directory) / PUBLICATION_INTENT)
    keys = {
        "schema_version",
        "identity",
        "operation",
        "body",
        "body_sha256",
        "marker",
        "actor",
        "source",
        "context",
        "clocks",
    }
    if (
        type(intent) is not dict
        or type(intent.get("schema_version")) is not int
        or intent["schema_version"] not in (1, 2)
        or set(intent) != keys | ({"prefix"} if intent["schema_version"] == 2 else set())
    ):
        refuse("torn or unsupported publication intent")
    identity, report, meta, _ = _publication_identity(repo, directory)
    timer.check()
    if (intent["schema_version"] == 1 and identity["sequence"] != 0) or (
        intent["schema_version"] == 2
        and (type(intent["prefix"]) is not list or len(intent["prefix"]) != identity["sequence"])
    ):
        refuse("publication version/sequence/prefix differs")
    body, marker = _publication_body(identity, report, intent["operation"])
    if (
        intent["identity"] != identity
        or intent["body"] != body
        or intent["marker"] != marker
        or intent["body_sha256"] != hashlib.sha256(body.encode("utf-8")).hexdigest()
    ):
        refuse("publication intent source, claim or exact body changed")
    if _publication_actor(intent["actor"]) != intent["actor"]:
        refuse("publication actor record changed")
    if intent["source"] != _publication_source(repo, meta, timer):
        refuse("publication current source differs from prewrite checks")
    _publication_clocks(directory, intent["clocks"])
    if (
        intent["clocks"][-1]["wall"] > timer.rows[-1]["wall"]
        or intent["clocks"][-1]["monotonic"] > timer.rows[-1]["monotonic"]
    ):
        refuse("publication intent is from a later clock")
    if type(intent["context"]) is not dict or set(intent["context"]) != set(PUBLIC_CONTEXT):
        refuse("publication initial context missing")
    for binding in intent["context"].values():
        if (
            type(binding) is not dict
            or set(binding) != {"sha256", "ids"}
            or type(binding["ids"]) is not list
            or any(type(i) is not int or i <= 0 for i in binding["ids"])
            or len(set(binding["ids"])) != len(binding["ids"])
        ):
            refuse("invalid initial public context binding")
        checksum(binding["sha256"])
    failure_path = Path(directory) / PUBLICATION_FAILURE
    if failure_path.exists():
        failure = read(failure_path)
        if (
            type(failure) is not dict
            or set(failure) != {"schema_version", "intent_sha256", "status", "clocks", "clock_exhausted"}
            or type(failure["schema_version"]) is not int
            or failure["schema_version"] != 1
            or failure["intent_sha256"] != digest(intent)
            or failure["status"] != "uncertain-no-repeat"
            or type(failure["clock_exhausted"]) is not bool
            or failure["clock_exhausted"]
        ):
            refuse("publication failure is torn or its clock allocation is exhausted")
        _publication_clocks(directory, failure["clocks"])
    if any(intent["clocks"][-1][key] > timer.rows[1][key] for key in ("wall", "monotonic")):
        refuse("publication intent clock rollback")
    timer.rows = copy.deepcopy(intent["clocks"]) + timer.rows[1:]
    timer.check()
    return intent, meta


def _publication_clocks(directory, rows):
    origin = read(Path(directory) / "batch-preparation-clock.json")
    reservation = read(Path(directory) / RUNTIME)
    native = read(Path(directory) / COMPLETION)["native_seconds"]
    prior = read(Path(directory) / "batch-runtime-acknowledged.json")["clocks"][-1]
    if type(rows) is not list or not 2 <= len(rows) <= 400 or rows[0] != prior:
        refuse("missing original publication clock origin")
    for row in rows:
        if type(row) is not dict or set(row) != {"wall", "monotonic", "native_seconds"}:
            refuse("invalid publication clock row")
        wall, mono = clock(row["wall"]), clock(row["monotonic"])
        if (
            row["native_seconds"] != native
            or wall < prior["wall"]
            or mono < prior["monotonic"]
            or abs(wall - origin["wall_started"] - (mono - origin["monotonic_started"])) > 1
            or wall > reservation["action_deadline"]
            or wall - reservation["started"] > 1740
            or not 0 <= wall - reservation["started"] - native <= 840
        ):
            refuse("retained publication clocks exceed original allocation")
        prior = row


def _publication_remote(repo, intent, timer, *, returned_id=None):
    """Only this operation's exact artifact may extend its prewrite context."""
    context = _publication_context(repo, timer)
    baseline = intent["context"]
    reviews = context[PUBLIC_CONTEXT[0]]
    matches = [r for r in reviews if intent["marker"] in (r.get("body") or "")]
    if len(matches) != 1:
        refuse("publication is absent or ambiguous; no repeat write")
    remote = matches[0]
    identifier = remote["id"]
    if identifier <= 0 or (
        returned_id is not None and (type(returned_id) is not int or identifier != returned_id)
    ):
        refuse("returned publication ID differs")
    if identifier in baseline[PUBLIC_CONTEXT[0]]["ids"]:
        refuse("preexisting publication cannot represent this operation")
    timer.check()
    fetched = repo.api(f"pulls/32/reviews/{identifier}")
    timer.check()
    if fetched != remote or type(fetched.get("id")) is not int:
        refuse("independently fetched publication differs")
    if (
        remote.get("body") != intent["body"]
        or remote.get("state") != "COMMENTED"
        or remote.get("commit_id") != intent["identity"]["binding"]["head_sha"]
        or _publication_actor(remote.get("user")) != intent["actor"]
    ):
        refuse("remote publication body, actor, head or COMMENT state differs")
    context[PUBLIC_CONTEXT[0]] = [r for r in reviews if r["id"] != identifier]
    if any(digest(rows) != baseline[endpoint]["sha256"] for endpoint, rows in context.items()):
        refuse("unrelated public context changed during scoped publication")
    return remote


def _publication_result(intent, remote):
    import hashlib

    return {
        "exact_match": True,
        "review_id": remote["id"],
        "head_sha": remote["commit_id"],
        "body_sha256": hashlib.sha256(intent["body"].encode("utf-8")).hexdigest(),
        "body_bytes": len(intent["body"].encode("utf-8")),
        "coverage_qualified": False,
        "component_qualified": intent["identity"]["unit"] != "integration",
        **({"integration_qualified": True} if intent["identity"]["unit"] == "integration" else {}),
        "scope": "integration-only" if intent["identity"]["unit"] == "integration" else "component-only",
    }


def verify_component_publication(repo, directory):
    parent = Path(directory).parent.parent
    batch = load_preparation(parent)
    if (
        sum(
            (parent / "units" / u["id"] / PUBLICATION_ACK).exists()
            for u in batch["plan"]["catalog"]["components"] + [batch["plan"]["catalog"]["integration"]]
        )
        > 1
    ):
        value = component_prefix(repo, parent)
        for intent, ack in value["records"]:
            if intent["identity"]["directory"] == str(Path(directory)):
                return _publication_result(intent, ack["remote"])
        refuse("requested publication is not in the actual prefix")
    timer = PublicationClock(directory)
    intent, _ = _publication_intent(repo, directory, timer)
    ack = read(Path(directory) / PUBLICATION_ACK)
    if (
        type(ack) is not dict
        or set(ack) != {"schema_version", "intent_sha256", "remote", "clocks"}
        or type(ack["schema_version"]) is not int
        or ack["schema_version"] != 1
        or ack["intent_sha256"] != digest(intent)
    ):
        refuse("missing or changed publication acknowledgment")
    _publication_clocks(directory, ack["clocks"])
    if ack["clocks"][: len(intent["clocks"])] != intent["clocks"] or any(
        ack["clocks"][-1][key] > timer.rows[-1][key] for key in ("wall", "monotonic")
    ):
        refuse("publication acknowledgment clock origin or current time differs")
    if (
        ack["clocks"][-1]["wall"] < intent["clocks"][-1]["wall"]
        or ack["clocks"][-1]["monotonic"] < intent["clocks"][-1]["monotonic"]
    ):
        refuse("publication acknowledgment precedes intent")
    remote = _publication_remote(repo, intent, timer)
    if remote != ack["remote"]:
        refuse("acknowledged remote publication changed")
    _, _, meta, _ = _publication_identity(repo, directory)
    timer.check()
    if _publication_source(repo, meta, timer) != intent["source"]:
        refuse("source changed during publication verification")
    timer.check()
    return _publication_result(intent, remote)


def recover_component_publication(repo, directory):
    """Read-only remote reconciliation; never POST, launch or renew credentials."""
    directory = plain_path(Path(directory))
    if (directory / PUBLICATION_ACK).exists():
        return verify_component_publication(repo, directory)
    timer = PublicationClock(directory)
    intent, meta = _publication_intent(repo, directory, timer)
    remote = _publication_remote(repo, intent, timer)
    if intent["schema_version"] == 2:
        component_prefix(
            repo, directory.parent.parent, before=intent["identity"]["unit"], publication=directory
        )
        timer.check()
    _publication_identity(repo, directory)
    timer.check()
    if _publication_source(repo, meta, timer) != intent["source"]:
        refuse("source changed during publication recovery")
    exclusive(
        directory / PUBLICATION_ACK,
        {"schema_version": 1, "intent_sha256": digest(intent), "remote": remote, "clocks": timer.rows},
        limit=2000000,
    )
    timer.check()
    return _publication_result(intent, remote)


def publish_component(repo, directory):
    """One scoped COMMENT attempt, guarded by exclusive durable intent."""
    import hashlib
    import uuid

    import claude_owned_auth

    directory = plain_path(Path(directory))
    if any(
        (directory / name).exists() for name in (PUBLICATION_INTENT, PUBLICATION_ACK, PUBLICATION_FAILURE)
    ):
        refuse("publication was attempted; use read-only recovery, never repeat POST")
    timer = PublicationClock(directory)
    identity, report, meta, batch = _publication_identity(repo, directory)
    timer.check()
    window = active_component_window(directory.parent.parent, batch)
    if identity["unit"] not in batch["plan"]["schedule"]["windows"][window]:
        refuse("publication is not in the active declared component window")
    preceding = []
    if identity["sequence"]:
        preceding = component_prefix(repo, directory.parent.parent, before=identity["unit"])["rows"]
        timer.check()
    source = _publication_source(repo, meta, timer)
    current_plan(repo, batch["plan"], directory=directory.parent.parent)
    timer.check()
    with claude_owned_auth.snapshot(meta["review_policy"]) as owned:
        if digest(_component_admission(repo, directory.parent.parent, owned_auth=owned)) != digest(
            batch["admission"]
        ):
            refuse("publication current V6 admission changed")
        timer.check()
        if (
            owned.current_binding(
                900,
                batch["plan"]["schedule"]["window_seconds"][
                    active_component_window(directory.parent.parent, batch)
                ]
                - 360,
            )
            != meta["review_policy"]["authentication"]
        ):
            refuse("publication whole-window authentication changed")
        timer.check()
        actor = publication_actor(repo)
        timer.check()
        context = _publication_context(repo, timer)
        operation = str(uuid.uuid4())
        body, marker = _publication_body(identity, report, operation)
        if any(f":{digest(identity)} -->" in (row.get("body") or "") for row in context[PUBLIC_CONTEXT[0]]):
            refuse("preexisting component publication requires its original intent; no new POST")
        current_plan(repo, batch["plan"], directory=directory.parent.parent)
        timer.check()
        if (
            source != _publication_source(repo, meta, timer)
            or identity != _publication_identity(repo, directory)[0]
        ):
            refuse("publication source or child changed before intent")
        if digest(_component_admission(repo, directory.parent.parent, owned_auth=owned)) != digest(
            batch["admission"]
        ):
            refuse("publication final V6 admission changed")
        timer.check()
        owned.recheck()
        timer.check()
        intent = {
            "schema_version": 1,
            "identity": identity,
            "operation": operation,
            "body": body,
            "body_sha256": hashlib.sha256(body.encode("utf-8")).hexdigest(),
            "marker": marker,
            "actor": actor,
            "source": source,
            "context": {
                endpoint: {"sha256": digest(rows), "ids": [r["id"] for r in rows]}
                for endpoint, rows in context.items()
            },
            "clocks": copy.deepcopy(timer.rows),
        }
        if identity["sequence"]:
            intent.update(schema_version=2, prefix=preceding)
        exclusive(directory / PUBLICATION_INTENT, intent, limit=2000000)
        try:
            timer.check()
            owned.recheck()
            timer.check()
            if identity != _publication_identity(repo, directory)[0] or source != _publication_source(
                repo, meta, timer
            ):
                refuse("publication final source or executable claim changed")
            owned.recheck()
            timer.check()
            posted = repo.api(
                "pulls/32/reviews", data={"commit_id": meta["head_sha"], "event": "COMMENT", "body": body}
            )
            timer.check()
            if type(posted) is not dict or type(posted.get("id")) is not int:
                refuse("unsupported publication write response")
            remote = _publication_remote(repo, intent, timer, returned_id=posted["id"])
            if identity["sequence"]:
                component_prefix(
                    repo, directory.parent.parent, before=identity["unit"], publication=directory
                )
                timer.check()
            if posted != remote:
                refuse("write response and independently fetched publication differ")
            if identity != _publication_identity(repo, directory)[0] or source != _publication_source(
                repo, meta, timer
            ):
                refuse("publication source or child changed after write")
            owned.recheck()
            timer.check()
            exclusive(
                directory / PUBLICATION_ACK,
                {
                    "schema_version": 1,
                    "intent_sha256": digest(intent),
                    "remote": remote,
                    "clocks": copy.deepcopy(timer.rows),
                },
                limit=2000000,
            )
            timer.check()
            return _publication_result(intent, remote)
        except BaseException:
            if not (directory / PUBLICATION_FAILURE).exists():
                exclusive(
                    directory / PUBLICATION_FAILURE,
                    {
                        "schema_version": 1,
                        "intent_sha256": digest(intent),
                        "status": "uncertain-no-repeat",
                        "clocks": timer.rows,
                        "clock_exhausted": timer.exhausted,
                    },
                    limit=2000000,
                )
            raise


class PrefixClock:
    """Charge replay to the declared active/pause budget, never renew an old child allowance."""

    def __init__(self, directory):
        import time

        self.plan, self.applied = load(directory)
        self.directory = Path(directory)
        self.rows = journal(directory, self.plan, self.applied)
        self.wall, self.mono = time.time(), time.monotonic()
        self.last, self.last_mono = self.wall, self.mono
        if self.rows:
            ack = read(Path(directory) / "window-transition-acks" / f"{len(self.rows) - 1:02d}.json")
            if (
                self.wall < ack["wall"]
                or self.mono < ack["monotonic"]
                or abs(self.wall - ack["wall"] - (self.mono - ack["monotonic"])) > 1
            ):
                refuse("window clock predates its immutable acknowledgment")
        self.check()

    def check(self):
        import time

        now, mono = clock(time.time()), clock(time.monotonic())
        if now < self.last or mono < self.last_mono or abs(now - self.wall - (mono - self.mono)) > 1:
            refuse("prefix operation clock rollback")
        if journal(self.directory, self.plan, self.applied) != self.rows:
            refuse("window journal changed during prefix operation")
        if len(self.rows) % 2:
            paused = self.rows[-1]["value"]
            if not paused["sealed_at"] <= now <= paused["resume_before"]:
                refuse("stopped prefix pause expired or clock rolled back")
        else:
            window_clock(self.plan, self.applied, self.rows, len(self.rows) // 2, now)
        self.last, self.last_mono = now, mono
        return now


def _stored_publication(repo, child, timer):
    """Historical evidence replay: original clocks remain bound, not restarted."""
    import hashlib
    import time

    identity, report, meta, batch = _publication_identity(repo, child)
    timer.check()
    intent = read(child / PUBLICATION_INTENT)
    if type(intent) is not dict:
        refuse("unsupported or torn prefix publication intent")
    version = intent.get("schema_version")
    keys = {
        "schema_version",
        "identity",
        "operation",
        "body",
        "body_sha256",
        "marker",
        "actor",
        "source",
        "context",
        "clocks",
    }
    if (
        type(version) is not int
        or version not in (1, 2)
        or set(intent) != keys | ({"prefix"} if version == 2 else set())
    ):
        refuse("unsupported or torn prefix publication intent")
    if version == 1 and identity["sequence"] != 0:
        refuse("legacy publication cannot represent a later child")
    body, marker = _publication_body(identity, report, intent["operation"])
    if (
        intent["identity"] != identity
        or intent["body"] != body
        or intent["marker"] != marker
        or intent["body_sha256"] != hashlib.sha256(body.encode("utf-8")).hexdigest()
        or intent["actor"] != _publication_actor(intent["actor"])
    ):
        refuse("prefix publication identity, actor or exact body changed")
    if intent["source"] != _publication_source(repo, meta, timer):
        refuse("prefix current source changed")
    _publication_clocks(child, intent["clocks"])
    ack = read(child / PUBLICATION_ACK)
    if (
        type(ack) is not dict
        or set(ack) != {"schema_version", "intent_sha256", "remote", "clocks"}
        or type(ack["schema_version"]) is not int
        or ack["schema_version"] != 1
        or ack["intent_sha256"] != digest(intent)
    ):
        refuse("prefix publication acknowledgment missing or changed")
    _publication_clocks(child, ack["clocks"])
    if ack["clocks"][: len(intent["clocks"])] != intent["clocks"]:
        refuse("prefix acknowledgment lost original operation clocks")
    last = ack["clocks"][-1]
    origin = read(child / "batch-preparation-clock.json")
    now, mono = timer.check(), time.monotonic()
    if (
        now < last["wall"]
        or mono < last["monotonic"]
        or abs(now - origin["wall_started"] - (mono - origin["monotonic_started"])) > 1
    ):
        refuse("prefix clock predates completed child or rolled back")
    failure_path = child / PUBLICATION_FAILURE
    if failure_path.exists():
        failure = read(failure_path)
        if (
            type(failure) is not dict
            or set(failure) != {"schema_version", "intent_sha256", "status", "clocks", "clock_exhausted"}
            or type(failure["schema_version"]) is not int
            or failure["schema_version"] != 1
            or failure["intent_sha256"] != digest(intent)
            or failure["status"] != "uncertain-no-repeat"
            or failure["clock_exhausted"] is not False
        ):
            refuse("prefix publication has an unresolved failure")
        _publication_clocks(child, failure["clocks"])
    qualified = qualify_child(repo, child)
    timer.check()
    row = {
        "unit": identity["unit"],
        "binding": batch["plan"]["catalog_digest"],
        "claim": identity["claim"],
        "report": identity["artifacts"]["review.md"],
        "publication": digest(ack),
        "execution": identity["artifacts"]["reporting-execution.json"],
        "capture": identity["artifacts"]["review-capture.json"],
        "observer": identity["artifacts"][OBSERVATION],
        "usage": qualified["usage"],
    }
    return intent, ack, row


def component_prefix(repo, directory, *, before=None, publication=None):
    """Actual declared component publications only; no imported rows or readiness callbacks.

    A single prepared/active tail may exist for its current operation. It is never
    returned as qualified prefix. Explicit replay_prefix requires the whole window.
    The publication argument is only the current tail's own uncertain write: its
    exact scoped remote artifact is independently verified, never imported.
    """
    directory = plain_path(Path(directory))
    batch = load_preparation(directory)
    plan = batch["plan"]
    timer = PrefixClock(directory)
    components = plan["catalog"]["components"]
    names = [u["id"] for u in components] + ["integration"]
    transitions = journal(directory, plan, batch["application"])
    window = len(transitions) // 2
    if window >= len(plan["schedule"]["windows"]):
        refuse("window outside the finite original plan")
    first = [u for w in plan["schedule"]["windows"][: window + 1] for u in w if u != "final-validation"]
    units_root = plain_path(directory / "units")
    paths = sorted(units_root.iterdir()) if units_root.exists() else []
    if any(p.is_symlink() or not p.is_dir() or p.name not in first for p in paths):
        refuse("hidden, copied or out-of-window child/import")
    import review_claims

    claims = read(directory / "batch-claims.json")
    for unit in components + [plan["catalog"]["integration"]]:
        review_claims.verify(repo, batch, unit, claims[unit["id"]])
        execution = review_claims.root(repo) / "batch9-executions" / (digest(claims[unit["id"]]) + ".json")
        if execution.exists():
            child = units_root / unit["id"]
            if child not in paths:
                refuse("hidden or torn executable reservation outside the child prefix")
            runtime_reservation(repo, child)
        timer.check()
    paths.sort(key=lambda p: names.index(p.name))
    present = [p.name for p in paths]
    if present != names[: len(present)]:
        refuse("child preparation is not an exact lexical prefix")
    records, rows, pending = [], [], None
    for path in paths:
        timer.check()
        verify_child(repo, path)
        timer.check()
        if not (path / PUBLICATION_ACK).exists():
            if pending is not None or path != paths[-1]:
                refuse("unpublished or active child inside required prefix")
            pending = path
            continue
        if pending is not None:
            refuse("published child after an unresolved reservation")
        intent, ack, row = _stored_publication(repo, path, timer)
        if intent["identity"]["sequence"] != len(rows):
            refuse("prefix sequence changed")
        if intent["schema_version"] == 2 and intent["prefix"] != rows:
            refuse("declared publication prefix changed or omitted")
        records.append((intent, ack))
        rows.append(row)
    for transition in transitions[::2]:
        sealed = transition["value"]["children"]
        if rows[: len(sealed)] != sealed:
            refuse("stopped prefix differs from immutable complete seal")
    if len(transitions) % 2 and (pending is not None or rows != transitions[-1]["value"]["children"]):
        refuse("active or hidden child at a stopped boundary")
    if before is not None:
        if len(transitions) % 2 or before not in plan["schedule"]["windows"][window]:
            refuse("child is outside current active window")
        if before not in first or len(rows) != names.index(before):
            refuse("required preceding component is missing, active or unpublished")
        if pending is not None and pending.name != before:
            refuse("competing active component")
    context = aggregate_context(repo, directory, _publication_context(repo, timer), timer)
    if publication is not None:
        own = plain_path(Path(publication))
        if pending != own or before != own.name:
            refuse("scoped publication is not the current unresolved tail")
        # The caller's own intent is not prefix credit. Verify its remote identity
        # through the existing exact own-artifact adapter before removing it.
        own_timer = PublicationClock(own)
        own_intent, _ = _publication_intent(repo, own, own_timer)
        own_remote = _publication_remote(repo, own_intent, own_timer)
        timer.check()
        if [r for r in context[PUBLIC_CONTEXT[0]] if r["id"] == own_remote["id"]] != [own_remote]:
            refuse("current publication changed between independent context reads")
        context[PUBLIC_CONTEXT[0]] = [r for r in context[PUBLIC_CONTEXT[0]] if r["id"] != own_remote["id"]]
        if own_intent.get("prefix") != rows:
            refuse("current publication prefix differs from independent replay")
    remote_ids = []
    for intent, ack in records:
        remote = ack["remote"]
        if type(remote) is not dict:
            refuse("unsupported prefix remote acknowledgment")
        identifier = remote.get("id")
        if type(identifier) is not int or identifier <= 0 or identifier in remote_ids:
            refuse("copied or ambiguous prefix publication ID")
        matches = [r for r in context[PUBLIC_CONTEXT[0]] if intent["marker"] in (r.get("body") or "")]
        if matches != [remote]:
            refuse("prefix listed publication missing, altered or ambiguous")
        timer.check()
        fetched = repo.api(f"pulls/32/reviews/{identifier}")
        timer.check()
        if (
            matches != [remote]
            or fetched != remote
            or remote.get("body") != intent["body"]
            or remote.get("state") != "COMMENTED"
            or remote.get("commit_id") != batch["binding"]["head_sha"]
            or _publication_actor(remote.get("user")) != intent["actor"]
        ):
            refuse("prefix remote ID/body/actor/head/state differs")
        remote_ids.append(identifier)
    for index, (intent, _ack) in enumerate(records):
        baseline = intent["context"]
        if type(baseline) is not dict or set(baseline) != set(PUBLIC_CONTEXT):
            refuse("prefix initial context binding missing")
        for endpoint, current in context.items():
            binding = baseline[endpoint]
            if type(binding) is not dict or set(binding) != {"sha256", "ids"}:
                refuse("prefix context binding malformed")
            prior = (
                [r for r in current if r["id"] not in remote_ids[index:]]
                if endpoint == PUBLIC_CONTEXT[0]
                else current
            )
            if binding != {"sha256": digest(prior), "ids": [r["id"] for r in prior]}:
                refuse("unrelated or altered original public context")
        if [r["id"] for r in context[PUBLIC_CONTEXT[0]]] != baseline[PUBLIC_CONTEXT[0]]["ids"] + remote_ids[
            index:
        ]:
            refuse("remote publication sequence reordered or hidden")
    timer.check()
    return {"rows": rows, "records": records, "context": context, "pending": pending}


def reconcile_public_context(repo, directory, original):
    """Compare every initial object unchanged; remove only independently replayed new reviews."""
    evidence = component_prefix(repo, directory)
    ids = {ack["remote"]["id"] for _, ack in evidence["records"]}
    for key, endpoint in (
        ("reviews", "pulls/32/reviews"),
        ("inline_comments", "pulls/32/comments"),
        ("pr_comments", "issues/32/comments"),
        ("issue_comments", "issues/31/comments"),
    ):
        current = evidence["context"][endpoint]
        if key == "reviews":
            current = [row for row in current if row["id"] not in ids]
        if digest(current) != digest(original[key]):
            refuse("immutable original public context changed")
    # Contract title/body is checked independently, without volatile issue counts.
    issue = evidence["context"]["issues/31"][0]
    if any(issue.get(k) != original["issue"].get(k) for k in ("id", "title", "body")):
        refuse("immutable original issue changed")
    return evidence


def _has_publications(directory):
    root = Path(directory) / "units"
    return root.exists() and any((p / PUBLICATION_ACK).exists() for p in root.iterdir())


def _component_admission(repo, directory, *, owned_auth):
    import reporting_admission_v6 as admission

    if _has_publications(directory):
        return admission.check_batch(repo, directory, owned_auth=owned_auth)
    return admission.check(repo, owned_auth=owned_auth, capacity_required=True)


def active_component_window(directory, batch):
    rows = journal(Path(directory), batch["plan"], batch["application"])
    window = len(rows) // 2
    if len(rows) % 2 or window >= len(batch["plan"]["schedule"]["windows"]) - 1:
        refuse("no active declared component window")
    return window


def child_window_policy(directory, batch, unit_id):
    """Derive policy from the immutable declared resume, never rewrite ancestors."""
    windows = batch["plan"]["schedule"]["windows"][:-1]
    indices = [i for i, units in enumerate(windows) if unit_id in units]
    if len(indices) != 1:
        refuse("unknown component window")
    index = indices[0]
    rows = journal(Path(directory), batch["plan"], batch["application"])
    policy = copy.deepcopy(batch["unit_policy"])
    if index == 0:
        return policy, None
    if len(rows) < index * 2:
        refuse("component window has no immutable verified resume")
    record = rows[index * 2 - 1]
    policy["authentication"] = copy.deepcopy(record["authentication"])
    return policy, digest(record)


def validate_transition(directory, row, prior):
    """Closed production record and clock replay; owned lineage is rechecked live."""
    batch = read(Path(directory) / "batch.json")
    proof = row.get("batch_proof")
    keys = {
        "schema_version",
        "directory",
        "batch_sha256",
        "admission_digest",
        "observed",
        "current",
        "clocks",
    }
    if (
        type(proof) is not dict
        or set(proof) != keys
        or type(proof["schema_version"]) is not int
        or proof["schema_version"] != 1
    ):
        refuse("missing or torn production stopped-boundary proof")
    observed = prior[-1]["authentication"] if prior else batch["unit_policy"]["authentication"]
    if (
        proof["directory"] != str(Path(directory).resolve())
        or proof["batch_sha256"] != digest(batch)
        or proof["admission_digest"] != digest(batch["admission"])
        or proof["observed"] != observed
        or proof["current"] != row["authentication"]
        or (row["operation"] == "pause" and observed != row["authentication"])
    ):
        refuse("stopped-boundary source/admission/account binding changed")
    clocks = proof["clocks"]
    if type(clocks) is not list or len(clocks) != 2:
        refuse("missing stopped-boundary operation clocks")
    for item in clocks:
        if type(item) is not dict or set(item) != {"wall", "monotonic"}:
            refuse("invalid stopped-boundary clocks")
        clock(item["wall"])
        clock(item["monotonic"])
    start, end = clocks
    if (
        end["wall"] < start["wall"]
        or end["monotonic"] < start["monotonic"]
        or abs(end["wall"] - start["wall"] - (end["monotonic"] - start["monotonic"])) > 1
        or end["wall"] != row["value"]["sealed_at" if row["operation"] == "pause" else "resumed_at"]
    ):
        refuse("stopped-boundary clock rollback or changed acknowledgment")
    if prior:
        previous = prior[-1]["batch_proof"]["clocks"][-1]
        if start["wall"] < previous["wall"] or start["monotonic"] < previous["monotonic"]:
            refuse("stopped-boundary clocks precede previous transition")


def component_transition(repo, directory, *, owned, now, resuming, final_validation=False):
    """Actual exclusive stopped boundary, with owned verification and no external mutation."""
    import time

    import claude_owned_auth
    import reporting_admission_v6 as admission

    start = {"wall": time.time(), "monotonic": time.monotonic()}
    batch = load_preparation(directory)
    plan, applied = batch["plan"], batch["application"]
    rows = journal(directory, plan, applied)
    if bool(len(rows) % 2) != resuming:
        refuse("already paused or missing complete stopped boundary")
    window = len(rows) // 2
    next_window = window + 1
    if next_window >= len(plan["schedule"]["windows"]) or (
        next_window == len(plan["schedule"]["windows"]) - 1 and not final_validation
    ):
        refuse("production stopped-window transitions to final validation require explicit selection")
    if abs(clock(now) - start["wall"]) > 1:
        refuse("stopped-window transitions require the actual current clock")
    owned = claude_owned_auth.require(owned)
    timer = PrefixClock(directory)
    current_plan(repo, plan, directory=directory)
    timer.check()
    children = replay_prefix(repo, directory, plan, window)
    timer.check()
    checker = admission.check_batch if resuming else admission.check_pause
    if checker(repo, directory, owned_auth=owned) != batch["admission"]:
        refuse("stopped-boundary original admission changed")
    timer.check()
    observed = rows[-1]["authentication"] if rows else batch["unit_policy"]["authentication"]
    required_window = next_window if resuming else window
    current = (
        owned.current_binding(900, plan["schedule"]["window_seconds"][required_window] - 360)
        if resuming
        else owned.current_binding(900)
    )
    if (not resuming and current != observed) or not owned.capability_lineage(observed, current, 900):
        refuse("renewal is not verified same-account stopped lineage")
    timer.check()
    # Re-fetch all remote publications and claims after admission/history work.
    if replay_prefix(repo, directory, plan, window) != children:
        refuse("stopped prefix changed during transition")
    current_plan(repo, plan, directory=directory)
    timer.check()
    owned.recheck()
    final_binding = (
        owned.current_binding(900, plan["schedule"]["window_seconds"][required_window] - 360)
        if resuming
        else owned.current_binding(900)
    )
    if final_binding != current:
        refuse("stopped-boundary current generation changed")
    end = {"wall": time.time(), "monotonic": time.monotonic()}
    timer.check()
    value = (
        resume(plan, applied, rows[-1]["value"], children, end["wall"])
        if resuming
        else seal(plan, applied, window, children, end["wall"])
    )
    record = {
        "operation": "resume" if resuming else "pause",
        "previous": digest(rows[-1]) if rows else digest(applied),
        "value": value,
        "authentication": current,
        "batch_proof": {
            "schema_version": 1,
            "directory": str(directory.resolve()),
            "batch_sha256": digest(batch),
            "admission_digest": digest(batch["admission"]),
            "observed": observed,
            "current": current,
            "clocks": [start, end],
        },
    }
    validate_transition(directory, record, rows)
    if journal(directory, plan, applied) != rows:
        refuse("competing stopped-boundary transition")
    target = plain_path(directory / "window-transitions")
    private_directory(target)
    exclusive(target / f"{len(rows):02d}.json", record, limit=2000000)
    # A post-write failure leaves the transition consumed; no rewrite/refund.
    try:
        owned.recheck()
        if abs((time.time() - start["wall"]) - (time.monotonic() - start["monotonic"])) > 1:
            refuse("clock changed after stopped-boundary write")
        if resuming:
            window_clock(plan, applied, rows + [record], next_window, time.time())
        else:
            window_clock(plan, applied, rows, window, time.time())
        acknowledgments = plain_path(directory / "window-transition-acks")
        private_directory(acknowledgments)
        ack = {"record_sha256": digest(record), "wall": time.time(), "monotonic": time.monotonic()}
        exclusive(acknowledgments / f"{len(rows):02d}.json", ack)
        transition_ack(directory, record, rows, plan, applied)
        owned.recheck()
        finished, monotonic = time.time(), time.monotonic()
        if (
            finished < ack["wall"]
            or monotonic < ack["monotonic"]
            or abs(finished - start["wall"] - (monotonic - start["monotonic"])) > 1
        ):
            refuse("clock changed after stopped-boundary acknowledgment")
        if resuming:
            if finished > rows[-1]["value"]["resume_before"]:
                refuse("resume acknowledgment exceeded original pause")
            window_clock(plan, applied, rows + [record], next_window, finished)
        else:
            window_clock(plan, applied, rows, window, finished)
    except BaseException:
        failures = plain_path(directory / "window-transition-failures")
        private_directory(failures)
        exclusive(
            failures / f"{len(rows):02d}.json",
            {"schema_version": 1, "record_sha256": digest(record), "status": "consumed-uncertain-no-repeat"},
        )
        raise
    return record


def child_window_deadline(directory, batch, unit_id):
    _, transition = child_window_policy(directory, batch, unit_id)
    if transition is None:
        # Preserve exact first-window preparation semantics.
        return batch["application"]["wall_deadline"]
    rows = journal(Path(directory), batch["plan"], batch["application"])
    index = next(i for i, w in enumerate(batch["plan"]["schedule"]["windows"]) if unit_id in w)
    return (
        rows[2 * index - 1]["value"]["resumed_at"] + batch["plan"]["schedule"]["window_seconds"][index] - 360
    )


def transition_ack(directory, record, prior, plan, applied):
    ack = read(Path(directory) / "window-transition-acks" / f"{len(prior):02d}.json")
    if (
        type(ack) is not dict
        or set(ack) != {"record_sha256", "wall", "monotonic"}
        or ack["record_sha256"] != digest(record)
    ):
        refuse("missing or changed stopped-boundary acknowledgment")
    end = record["batch_proof"]["clocks"][-1]
    if (
        clock(ack["wall"]) < end["wall"]
        or clock(ack["monotonic"]) < end["monotonic"]
        or abs(ack["wall"] - end["wall"] - (ack["monotonic"] - end["monotonic"])) > 1
    ):
        refuse("stopped-boundary acknowledgment clock changed")
    if record["operation"] == "pause":
        window_clock(plan, applied, prior, len(prior) // 2, ack["wall"])
    else:
        if ack["wall"] > prior[-1]["value"]["resume_before"]:
            refuse("resume acknowledgment exceeded original pause")
        window_clock(plan, applied, prior + [record], (len(prior) + 1) // 2, ack["wall"])
    return ack


AGGREGATE = "batch-final-aggregate.json"
FINAL_START = "batch-final-start.json"
FINAL_ACK = "batch-final-acknowledged.json"
FINAL_FAILURE = "batch-final-failure.json"
AGGREGATE_INTENT = "batch-aggregate-publication-intent.json"
AGGREGATE_ACK = "batch-aggregate-publication-acknowledged.json"
AGGREGATE_FAILURE = "batch-aggregate-publication-uncertain.json"


def final_window(directory):
    directory = plain_path(Path(directory))
    batch = load_preparation(directory)
    rows = journal(Path(directory), batch["plan"], batch["application"])
    if len(rows) != 2 * (len(batch["plan"]["schedule"]["windows"]) - 1):
        refuse("final validation requires its exact complete stopped resume")
    PrefixClock(directory).check()
    return batch, rows


def final_policy(directory):
    batch, rows = final_window(directory)
    policy = copy.deepcopy(batch["unit_policy"])
    policy["authentication"] = copy.deepcopy(rows[-1]["authentication"])
    return policy


def aggregate_current(repo, directory, *, owned_auth):
    """Actual complete evidence replay, never a saved qualification flag."""
    import claude_owned_auth
    import reporting_admission_v6 as admission

    directory = plain_path(Path(directory))
    batch, transitions = final_window(directory)
    owned = claude_owned_auth.require(owned_auth)
    timer = PrefixClock(directory)
    if admission.check_batch(repo, directory, owned_auth=owned) != batch["admission"]:
        refuse("final original admission changed")
    timer.check()
    source = catalog(repo, batch_directory=directory)
    if source["dependencies"]["assignments"] != batch["plan"]["catalog_digest"]:
        refuse("final current source assignments changed")
    timer.check()
    prefix = component_prefix(repo, directory)
    names = [u["id"] for u in batch["plan"]["catalog"]["components"]] + ["integration"]
    if prefix["pending"] is not None or [r["unit"] for r in prefix["rows"]] != names:
        refuse("aggregate requires all actual component and integration publications")
    if prefix["rows"] != transitions[-2]["value"]["children"]:
        refuse("final stopped seal differs from complete current prefix")
    members, covered = [], set()
    for name, row, (_, ack) in zip(names, prefix["rows"], prefix["records"], strict=True):
        import time

        started, mono = time.time(), time.monotonic()
        child = directory / "units" / name
        identity, report, meta, _, qualified = _publication_evidence(repo, child)
        covered.update(qualified["required_ids"])
        members.append(
            {
                "unit": name,
                "identity": identity,
                "assignment": digest(meta["batch_unit"]),
                "evidence": row,
                "report_bytes": len(report),
                "remote": ack["remote"],
            }
        )
        if not 0 <= time.time() - started <= 180 or not 0 <= time.monotonic() - mono <= 180:
            refuse("final child replay allocation exhausted")
        timer.check()
    required = {i["id"] for i in batch["plan"]["catalog"]["items"]}
    if not required <= covered:
        refuse("aggregate leaves original primary material unread")
    # Rebuilding the integration input rechecks every raw report and projected row.
    material = integration_material(repo, directory, batch)
    timer.check()
    owned.recheck()
    if owned.current_binding(900) != transitions[-1]["authentication"]:
        refuse("final generation changed in flight")
    timer.check()
    return {
        "schema_version": 1,
        "kind": "batch9-complete-aggregate",
        "directory": str(directory),
        "batch_sha256": digest(batch),
        "binding": copy.deepcopy(batch["binding"]),
        "transition": digest(transitions[-1]),
        "source": digest(source["dependencies"]),
        "admission": digest(batch["admission"]),
        "members": members,
        "required_ids": sorted(required),
        "integration_material": digest(
            {
                "items": material["items"],
                "dependencies": material["dependencies"],
                "files": {name: hashlib.sha256(raw).hexdigest() for name, raw in material["files"].items()},
            }
        ),
    }


def final_clocks(directory, value):
    batch, transitions = final_window(directory)
    if type(value) is not dict or set(value) != {"wall", "monotonic"}:
        refuse("invalid final completion clocks")
    origin = transitions[-1]["batch_proof"]["clocks"][-1]
    wall, mono = clock(value["wall"]), clock(value["monotonic"])
    if (
        wall < origin["wall"]
        or mono < origin["monotonic"]
        or abs(wall - origin["wall"] - (mono - origin["monotonic"])) > 1
    ):
        refuse("final completion clock rollback")
    window_clock(batch["plan"], batch["application"], transitions, len(transitions) // 2, wall)


def final_now(directory):
    import time

    value = {"wall": time.time(), "monotonic": time.monotonic()}
    final_clocks(directory, value)
    return value


def finalize(repo, directory, *, owned_auth):
    """One exclusive completion; no provider process or parent capture is invented."""
    directory = plain_path(Path(directory))
    batch, rows = final_window(directory)
    start = {
        "schema_version": 1,
        "batch_sha256": digest(batch),
        "transition": digest(rows[-1]),
        "directory": str(directory),
        "clocks": final_now(directory),
    }
    exclusive(directory / FINAL_START, start)
    try:
        value = aggregate_current(repo, directory, owned_auth=owned_auth)
        exclusive(directory / AGGREGATE, value, limit=2000000)
        ack = {
            "schema_version": 1,
            "start_sha256": digest(start),
            "aggregate_sha256": digest(value),
            "clocks": final_now(directory),
        }
        owned_auth.recheck()
        exclusive(directory / FINAL_ACK, ack)
        final_now(directory)
        owned_auth.recheck()
        return value
    except BaseException:
        exclusive(
            directory / FINAL_FAILURE,
            {"schema_version": 1, "start_sha256": digest(start), "status": "consumed-incomplete-no-repeat"},
        )
        raise


def aggregate_qualification(repo, directory, *, owned_auth=None):
    import claude_owned_auth

    directory = plain_path(Path(directory))
    if owned_auth is None:
        with claude_owned_auth.snapshot(final_policy(directory)) as owned:
            return aggregate_qualification(repo, directory, owned_auth=owned)
    batch, transitions = final_window(directory)
    if (directory / FINAL_FAILURE).exists():
        refuse("failed final validation remains incomplete")
    start, ack = read(directory / FINAL_START), read(directory / FINAL_ACK)
    value = read(directory / AGGREGATE)
    if (
        type(start) is not dict
        or set(start) != {"schema_version", "batch_sha256", "transition", "directory", "clocks"}
        or type(ack) is not dict
        or set(ack) != {"schema_version", "start_sha256", "aggregate_sha256", "clocks"}
    ):
        refuse("torn final validation")
    if (
        type(start["schema_version"]) is not int
        or start["schema_version"] != 1
        or type(ack["schema_version"]) is not int
        or ack["schema_version"] != 1
        or start["batch_sha256"] != digest(batch)
        or start["transition"] != digest(transitions[-1])
        or start["directory"] != str(directory)
        or ack["start_sha256"] != digest(start)
        or ack["aggregate_sha256"] != digest(value)
    ):
        refuse("final completion identity changed")
    for row in (start["clocks"], ack["clocks"]):
        final_clocks(directory, row)
    now = final_now(directory)
    if any(not start["clocks"][k] <= ack["clocks"][k] <= now[k] for k in now):
        refuse("final completion clock order changed")
    actual = aggregate_current(repo, directory, owned_auth=owned_auth)
    if digest(actual) != digest(value):
        refuse("aggregate source, members or qualification changed")
    return {
        "schema_version": 1,
        "qualified": True,
        "scope": "complete-batch9",
        "aggregate_sha256": digest(actual),
        "required_ids": actual["required_ids"],
        "members": actual["members"],
    }


def aggregate_envelope(value, operation):
    import hashlib
    import uuid

    if type(operation) is not str or str(uuid.UUID(operation)) != operation:
        refuse("invalid aggregate publication operation")
    marker = f"<!-- agentic-batch9-aggregate:{operation}:{digest(value)} -->"
    body = "Complete batch9 static-review evidence index. This is a COMMENT, not human approval.\n"
    body += "All exact model reports remain independently published and retained; this index is not a model report or a findings summary. Observed reads do not prove understanding. Tests and CI are separate evidence.\n"
    body += f"Head: {value['binding']['head_sha']}; base: {value['binding']['base_sha']}; aggregate SHA256: {digest(value)}.\n\n"
    for member in value["members"]:
        body += (
            f"{member['unit']}: https://github.com/{value['binding']['repository']}/pull/{value['binding']['pr']}#pullrequestreview-{member['remote']['id']} "
            f"exact report SHA256 {member['evidence']['report']}, {member['report_bytes']} bytes; publication SHA256 {hashlib.sha256(member['remote']['body'].encode()).hexdigest()}.\n"
        )
    body += "\n" + marker
    if len(body.encode("utf-8")) > 60000:
        refuse("complete aggregate envelope exceeds publication bound")
    return body, marker


def aggregate_intent(directory):
    """Closed transport binding only; callers must independently qualify the aggregate."""
    import hashlib

    directory = Path(directory)
    value = read(directory / AGGREGATE)
    intent = read(directory / AGGREGATE_INTENT)
    keys = {
        "schema_version",
        "operation",
        "identity",
        "body",
        "marker",
        "body_sha256",
        "actor",
        "context",
        "clocks",
    }
    if (
        type(intent) is not dict
        or set(intent) != keys
        or type(intent["schema_version"]) is not int
        or intent["schema_version"] != 1
    ):
        refuse("torn aggregate publication intent")
    body, marker = aggregate_envelope(value, intent["operation"])
    if (
        intent["identity"]
        != {
            "directory": str(directory),
            "aggregate_sha256": digest(value),
            "binding": value["binding"],
            "completion_sha256": digest(read(directory / FINAL_ACK)),
        }
        or intent["body"] != body
        or intent["marker"] != marker
        or intent["body_sha256"] != hashlib.sha256(body.encode()).hexdigest()
        or _publication_actor(intent["actor"]) != intent["actor"]
    ):
        refuse("aggregate publication intent differs from exact completion")
    final_clocks(directory, intent["clocks"])
    if type(intent["context"]) is not dict or set(intent["context"]) != set(PUBLIC_CONTEXT):
        refuse("missing aggregate initial context")
    for binding in intent["context"].values():
        if (
            type(binding) is not dict
            or set(binding) != {"sha256", "ids"}
            or type(binding["ids"]) is not list
            or any(type(i) is not int or i <= 0 for i in binding["ids"])
            or len(set(binding["ids"])) != len(binding["ids"])
        ):
            refuse("invalid aggregate original context binding")
        checksum(binding["sha256"])
    if (directory / AGGREGATE_FAILURE).exists():
        failure = read(directory / AGGREGATE_FAILURE)
        if failure != {"schema_version": 1, "intent_sha256": digest(intent), "status": "uncertain-no-repeat"}:
            refuse("changed aggregate uncertain-write record")
    return intent


def aggregate_context(repo, directory, context, timer):
    """Remove only this exact declared aggregate artifact during source replay."""
    directory = Path(directory)
    if not (directory / AGGREGATE_INTENT).exists():
        if (directory / AGGREGATE_ACK).exists() or (directory / AGGREGATE_FAILURE).exists():
            refuse("orphan aggregate publication")
        return context
    intent = aggregate_intent(directory)
    matches = [r for r in context[PUBLIC_CONTEXT[0]] if intent["marker"] in (r.get("body") or "")]
    if not matches and not (directory / AGGREGATE_ACK).exists():
        return context
    remote = _publication_remote(repo, intent, timer)
    if matches != [remote]:
        refuse("aggregate context changed between reads")
    if (directory / AGGREGATE_ACK).exists():
        aggregate_ack(directory, intent, remote)
    context[PUBLIC_CONTEXT[0]] = [r for r in context[PUBLIC_CONTEXT[0]] if r["id"] != remote["id"]]
    return context


def aggregate_ack(directory, intent, remote):
    ack = read(Path(directory) / AGGREGATE_ACK)
    if (
        type(ack) is not dict
        or set(ack) != {"schema_version", "intent_sha256", "remote", "clocks"}
        or type(ack["schema_version"]) is not int
        or ack["schema_version"] != 1
        or ack["intent_sha256"] != digest(intent)
        or ack["remote"] != remote
    ):
        refuse("aggregate publication acknowledgment changed")
    final_clocks(directory, ack["clocks"])
    now = final_now(directory)
    if any(not intent["clocks"][k] <= ack["clocks"][k] <= now[k] for k in now):
        refuse("aggregate publication clock order differs")
    return ack


def publish_aggregate(repo, directory):
    import hashlib
    import uuid

    import claude_owned_auth

    directory = plain_path(Path(directory))
    if any((directory / name).exists() for name in (AGGREGATE_INTENT, AGGREGATE_ACK, AGGREGATE_FAILURE)):
        refuse("aggregate publication already attempted; read-only recovery only")
    with claude_owned_auth.snapshot(final_policy(directory)) as owned:
        aggregate_qualification(repo, directory, owned_auth=owned)
        timer = PrefixClock(directory)
        value = read(directory / AGGREGATE)
        operation = str(uuid.uuid4())
        body, marker = aggregate_envelope(value, operation)
        context = _publication_context(repo, timer)
        actor = publication_actor(repo)
        timer.check()
        intent = {
            "schema_version": 1,
            "operation": operation,
            "identity": {
                "directory": str(directory),
                "aggregate_sha256": digest(value),
                "binding": value["binding"],
                "completion_sha256": digest(read(directory / FINAL_ACK)),
            },
            "body": body,
            "marker": marker,
            "body_sha256": hashlib.sha256(body.encode()).hexdigest(),
            "actor": actor,
            "context": {k: {"sha256": digest(v), "ids": [r["id"] for r in v]} for k, v in context.items()},
            "clocks": final_now(directory),
        }
        exclusive(directory / AGGREGATE_INTENT, intent, limit=2000000)
        try:
            aggregate_qualification(repo, directory, owned_auth=owned)
            owned.recheck()
            timer.check()
            posted = repo.api(
                "pulls/32/reviews",
                data={"commit_id": value["binding"]["head_sha"], "event": "COMMENT", "body": body},
            )
            timer.check()
            if type(posted) is not dict or type(posted.get("id")) is not int:
                refuse("unsupported aggregate write response")
            remote = _publication_remote(repo, intent, timer, returned_id=posted["id"])
            if posted != remote:
                refuse("aggregate returned artifact differs from independent GET")
            aggregate_qualification(repo, directory, owned_auth=owned)
            owned.recheck()
            exclusive(
                directory / AGGREGATE_ACK,
                {
                    "schema_version": 1,
                    "intent_sha256": digest(intent),
                    "remote": remote,
                    "clocks": final_now(directory),
                },
                limit=2000000,
            )
            timer.check()
            owned.recheck()
            return remote
        except BaseException:
            exclusive(
                directory / AGGREGATE_FAILURE,
                {"schema_version": 1, "intent_sha256": digest(intent), "status": "uncertain-no-repeat"},
            )
            raise


def verify_aggregate_publication(repo, directory, *, owned_auth=None):
    import claude_owned_auth

    directory = plain_path(Path(directory))

    if owned_auth is None:
        with claude_owned_auth.snapshot(final_policy(directory)) as owned:
            return verify_aggregate_publication(repo, directory, owned_auth=owned)
    owned = claude_owned_auth.require(owned_auth)
    aggregate_qualification(repo, directory, owned_auth=owned)
    intent = aggregate_intent(directory)
    remote = _publication_remote(repo, intent, PrefixClock(directory))
    aggregate_ack(directory, intent, remote)
    owned.recheck()
    if owned.current_binding(900) != final_policy(directory)["authentication"]:
        refuse("aggregate verification generation changed")
    return remote


def recover_aggregate_publication(repo, directory):
    """GET-only recovery from complete retained final and member evidence."""
    import claude_owned_auth

    directory = Path(directory)
    with claude_owned_auth.snapshot(final_policy(directory)) as owned:
        aggregate_qualification(repo, directory, owned_auth=owned)
        intent = aggregate_intent(directory)
        remote = _publication_remote(repo, intent, PrefixClock(directory))
        owned.recheck()
        if (directory / AGGREGATE_ACK).exists():
            aggregate_ack(directory, intent, remote)
        else:
            exclusive(
                directory / AGGREGATE_ACK,
                {
                    "schema_version": 1,
                    "intent_sha256": digest(intent),
                    "remote": remote,
                    "clocks": final_now(directory),
                },
                limit=2000000,
            )
        return verify_aggregate_publication(repo, directory, owned_auth=owned)


def task_repository(repo, directory=None):
    """Resolve the registered reviewed worktree; never trust a caller's checkout."""
    import review
    from tasks import verify_contract, workspace
    from workflow import Repo

    state = read(repo.main / ".agentic-local/tasks/issue-31.json")
    verify_contract(repo, state)
    target = workspace(repo, state)
    current = Repo(target)
    if current.main != repo.main or current.name != repo.name:
        refuse("registered batch repository differs")
    if directory is not None:
        directory = plain_path(Path(directory))
        if not directory.is_relative_to(plain_path(repo.main / ".agentic-local/reviews")):
            refuse("batch record outside canonical review storage")
        meta = review.verify_packet(directory)
        if (
            meta.get("repository") != repo.name
            or meta.get("issue") != 31
            or meta.get("pr") != state.get("pr")
        ):
            refuse("registered batch identity differs")
    return current


def add_commands(subparsers):
    """Explicit opt-in routes; legacy commands keep their original defaults."""
    subparsers.add_parser("batch9-catalog")
    for name in ("prepare", "run", "status", "pause", "resume", "recover", "finalize", "designate"):
        parser = subparsers.add_parser("batch9-" + name)
        parser.add_argument("directory")
        if name == "prepare":
            parser.add_argument("--authorization", required=True)
        if name == "run":
            parser.add_argument("--unit", required=True)
        if name in {"pause", "resume"}:
            parser.add_argument("--final-validation", action="store_true")
        if name == "recover":
            parser.add_argument("--publication", action="store_true")


def command(repo, args):
    """One requested operation only; manual stopped renewal is always external."""
    import time

    import claude_native_auth
    import claude_owned_auth
    import pipeline
    import review

    action = args.command.removeprefix("batch9-")
    current = task_repository(repo)
    if action == "catalog":
        return catalog(current, plan_only=True)
    directory = plain_path(Path(args.directory))
    if not directory.is_relative_to(plain_path(repo.main / ".agentic-local/reviews")):
        refuse("batch9 commands require canonical review storage")
    if action == "prepare":
        # This is the original source-bound policy, never an ordinary V6 selector.
        import reporting_activation_v6 as activation

        grant, _ = activation.load(current)
        policy = grant["binding"]["policy"]
        with claude_owned_auth.snapshot(policy) as owned:
            return select_preparation(current, directory, read(Path(args.authorization)), owned_auth=owned)
    task_repository(repo, directory)
    if action == "recover":
        meta = review.verify_packet(directory)
        if meta.get("kind") == "batch-parent":
            if not args.publication:
                refuse("torn final completion cannot be reconstructed")
            return recover_aggregate_publication(current, directory)
        return (recover_component_publication if args.publication else recover_child)(current, directory)
    batch = load_preparation(directory)
    rows = journal(directory, batch["plan"], batch["application"])
    if action == "status":
        return {
            "schema_version": 1,
            "batch_sha256": digest(batch),
            "window": len(rows) // 2,
            "stopped": bool(len(rows) % 2),
            "declared_windows": batch["plan"]["schedule"]["windows"],
            "qualified": False,
            "meaning": "local journal status only; no readiness credit",
        }
    if action == "designate":
        return pipeline.designate_batch9(repo, 31, directory)
    policy = copy.deepcopy(batch["unit_policy"])
    policy["authentication"] = copy.deepcopy(rows[-1]["authentication"] if rows else policy["authentication"])
    if action == "resume":
        if not rows or len(rows) % 2 != 1:
            refuse("manual resume requires a complete stopped boundary")
        # Reads the existing dedicated registration. No refresh/login/account mutation.
        # The owned consumer independently verifies all prior generations and lineage.
        policy["authentication"] = claude_native_auth.current_binding(900)
    with claude_owned_auth.snapshot(policy) as owned:
        if action == "pause":
            return pause(
                current, directory, owned=owned, now=time.time(), final_validation=args.final_validation
            )
        if action == "resume":
            return resume_window(
                current, directory, owned=owned, now=time.time(), final_validation=args.final_validation
            )
        if action == "finalize":
            return finalize(current, directory, owned_auth=owned)
        if action == "run":
            # Prepare exactly the requested next unit; existing attempts cannot be repeated.
            child = prepare_child(current, directory, args.unit, owned_auth=owned)
        else:
            refuse("unsupported explicit batch9 operation")
    # run_child obtains and verifies its own owned lock through capture, preserving
    # the preparation origin and refusing any intervening generation/source change.
    return run_child(current, child)


def full_checks_g20(repo, directory, meta):
    import check_runner
    import reporting_activation_v6 as activation

    current = activation.authorization(repo)
    before = check_runner.source(repo.root)
    if (
        current["contract_digest"] != activation.G20_CONTRACT_DIGEST
        or type(meta["plan_comment"]) is not int
        or meta["plan_comment"] != 6074133818
    ):
        refuse("generation20 full gates require current literal authority")
    result = _full_checks_g20(repo, directory, meta, suite_profile="issue31-suite1800-v1")
    if activation.authorization(repo) != current or check_runner.source(repo.root) != before:
        refuse("full gate authority or source changed")
    return result


def _full_checks_g20(repo, directory, meta, *, suite_profile):
    """Validate retained local execution records and fresh hosted associations.

    Local artifacts are owner-writable bookkeeping, not execution attestations.
    Hosted receipts are independently fetched through the existing collector.
    No test runner, installer or provider is invoked by this read-only adapter.
    """
    import hashlib

    import check_runner
    import ci_evidence

    evidence = read(plain_path(directory) / "full-checks.json")
    commands = {
        "serial": "python3 -B scripts/agentic/check.py --jobs 1",
        "parallel": "make check-agentic",
        "full": "make check",
        "clean": "make check-clean",
        "installed": "installed-full-suite",
        "lint": "ruff check",
        "format": "ruff format --check",
        "repository": "python3 -B scripts/check_repository.py",
    }
    if suite_profile is not None:
        commands["serial"] += " --suite-profile issue31-suite1800-v1"
        for name in ("parallel", "full"):
            commands[name] += " AGENTIC_SUITE_PROFILE=issue31-suite1800-v1"
    current = check_runner.source(repo.root)
    if (
        type(evidence) is not dict
        or set(evidence) != {"head", "source", "commands", "installed_files"}
        or evidence["head"] != meta["head_sha"]
        or evidence["source"] != current
        or type(evidence["commands"]) is not dict
        or set(evidence["commands"]) != set(commands)
    ):
        refuse("full check evidence is missing, stale or partial")
    for name, command in commands.items():
        record = evidence["commands"][name]
        if (
            type(record) is not dict
            or set(record) != {"command", "exit_status", "artifacts"}
            or record["command"] != command
            or type(record["exit_status"]) is not int
            or record["exit_status"] != 0
            or type(record["artifacts"]) is not dict
            or not record["artifacts"]
        ):
            refuse("required command provenance differs")
        for relative, expected in record["artifacts"].items():
            if type(relative) is not str or Path(relative).is_absolute() or ".." in Path(relative).parts:
                refuse("unsafe check evidence path")
            checksum(expected)
            if hashlib.sha256(plain_path(directory / relative).read_bytes()).hexdigest() != expected:
                refuse("check artifact changed")
    # Both standard runner records must reconcile every occurrence and worker.
    rows = None
    for name, jobs in [("serial", 1), ("parallel", 2)]:
        request = read(directory / name / "request.json")
        expected = runner_request(repo.root, jobs, suite_profile=suite_profile)
        if expected["source"] != current:
            refuse("source changed during descriptor reconstruction")
        rows = expected["rows"]
        if digest(request) != digest(expected):
            refuse("complete runner request differs from current discovery")
        if suite_profile is not None:
            suite_summary(directory / name, request, jobs)
        for index in range(jobs):
            result = check_runner.reconcile(
                request, index, plain_path(directory / name / f"worker-{index}.jsonl"), 0
            )
            if result["successful"] is not True:
                refuse("incomplete runner occurrence or fixture execution")
    import install

    expected_payload = {
        str(p): hashlib.sha256((repo.root / p).read_bytes()).hexdigest() for p in install.payload(repo.root)
    }
    if digest(evidence["installed_files"]) != digest(expected_payload):
        refuse("current installed payload closure differs")
    validate_installed_adoption(repo.root, directory, rows)
    checks = repo.api(
        f"commits/{meta['head_sha']}/check-runs?per_page=100", paginate=True, page_key="check_runs"
    )
    receipts = ci_evidence.collect(repo, meta["head_sha"], checks, meta["base_sha"])
    for name in ("flowdc-tests", "agentic-quality"):
        found = [r for r in receipts if r.get("check") == name]
        if len(found) != 1:
            refuse("missing or ambiguous hosted execution")
        receipt = found[0]
        if (
            receipt.get("state") != "observed"
            or receipt.get("run_attempt") != 1
            or type(receipt.get("run_attempt")) is not int
            or receipt.get("test_status") != "success"
            or receipt.get("clean_status") != "success"
            or receipt.get("pr_head_sha") != meta["head_sha"]
            or receipt.get("pr_base_sha") != meta["base_sha"]
        ):
            refuse("hosted first-attempt test/clean evidence incomplete")
        # Preserve actual merge checkout separately from the head association.
        if type(receipt.get("tested_checkout_sha")) is not str or not re.fullmatch(
            "[0-9a-f]{40}", receipt["tested_checkout_sha"]
        ):
            refuse("unknown hosted checkout")
    return {"local": digest(evidence), "hosted": digest(receipts), "source": digest(current)}


def validate_current_plan(plan):
    if type(plan) is dict and type(plan.get("schema_version")) is int and plan["schema_version"] == 10:
        from review_public_catalog_v1 import validate_plan as validate_public_plan

        return validate_public_plan(plan)
    return validate_plan(plan)


def surrounding_current_ids(plan, unit):
    validate_current_plan(plan)
    if plan["schema_version"] == 9:
        return surrounding_ids(plan["catalog"], unit)
    catalog = plan["catalog"]
    if unit not in catalog["components"] + [catalog["integration"]]:
        refuse("unknown current context owner")
    return sorted({i["id"] for i in catalog["items"]} - set(unit["required_ids"]))
