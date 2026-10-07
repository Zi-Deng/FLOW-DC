"""Finite batch9 windows and lossless catalog accounting.

Plans are not readiness. Actual qualification/publication adapters must independently
replay children before any stopped boundary can be sealed or resumed.
"""

from __future__ import annotations

import copy
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
    validate_plan(plan)
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


def runner_request(root, jobs):
    """Fresh interpreter avoids importing installed tests from the source checkout."""
    import subprocess
    import sys
    import tempfile

    code = """import json, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
import check_runner as runner
root, jobs = Path(sys.argv[2]), int(sys.argv[3])
source = runner.source(root)
suite, rows, objects, errors = runner.discover(root)
policy, assignments = runner.assignment_policy(root, suite, rows, objects, source, jobs)
value = dict(version=runner.VERSION, jobs=jobs, source=source, rows=rows,
             assignments=assignments, assignment_policy=policy, errors=errors,
             evidence_limit=runner.EVIDENCE_BYTES)
Path(sys.argv[4]).write_text(json.dumps(value, allow_nan=False))
"""
    with tempfile.TemporaryDirectory(prefix="agentic-descriptor-") as temporary:
        target = Path(temporary) / "request.json"
        result = subprocess.run(
            [sys.executable, "-B", "-c", code, str(Path(__file__).parent), str(root), str(jobs), str(target)],
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


def full_checks(repo, directory, meta):
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
        expected = runner_request(repo.root, jobs)
        if expected["source"] != current:
            refuse("source changed during descriptor reconstruction")
        rows = expected["rows"]
        if digest(request) != digest(expected):
            refuse("complete runner request differs from current discovery")
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
    expected_installed = runner_request(installed_root, 1)
    if expected_installed["source"] != installed_source or digest(expected_installed["rows"]) != digest(rows):
        refuse("installed discovery differs from the complete source suite")
    installed_request = read(directory / "installed" / "request.json")
    if digest(installed_request) != digest(expected_installed):
        refuse("installed complete runner request differs")
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


def catalog(repo, *, plan_only=False, packet_target=None):
    """Actual source/check adapter; no readiness flag or provisional catalog input.

    The coordinator supplies a prepared full packet plus retained check artifacts
    at the task's v6_catalog directory after final source gates. All inventory is
    regenerated from Git and exact public context, not imported assignments.
    """
    import hashlib
    import tempfile

    import reporting_activation_v6 as activation
    import review
    import review_packet
    from tasks import issue_contract
    from workflow import configuration, run, write_json

    state = read(repo.main / ".agentic-local/tasks/issue-31.json")
    authority = activation.authorization(repo)
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
        or meta.get("plan_comment") != activation.CONTRACT["plan_comment"]
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
    if digest(issue_contract(repo, 31, activation.CONTRACT["plan_comment"])) != activation.CONTRACT_DIGEST:
        refuse("current issue/plan changed")
    review.current_pr(repo, 32, meta["head_sha"], meta["base_sha"])
    gates = full_checks(repo, directory, meta)
    original = directory / "packet"
    context = read(original / "context.json")
    saved_contract = {
        "issue": 31,
        "plan_comment": activation.CONTRACT["plan_comment"],
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
    if digest(saved_contract) != activation.CONTRACT_DIGEST:
        refuse("saved issue/plan contract differs")
    if (
        context["pull_request"]["head"]["sha"] != meta["head_sha"]
        or context["pull_request"]["base"]["sha"] != meta["base_sha"]
    ):
        refuse("saved PR identity differs")
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
            "contract": activation.CONTRACT_DIGEST,
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

    validate_plan(plan)
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
    material.packet_budget(projections, existing_projection_bytes)
    if len(items) + len(plan["catalog"]["integration"]["required_ids"]) > 128:
        refuse("integration report and cross-boundary IDs exceed bound")
    return {"items": items, "files": files, "dependencies": copy.deepcopy(dependencies)}


def replay_prefix(repo, directory, plan, window):
    """Closed production seam for the next batch9 execution/publication slice.

    Must independently requalify every exact child, verify global claims, known
    usage and publications and refuse any active/uncertain reservation. There is
    deliberately no receipt, boolean or callback parameter to bypass this gate.
    """
    refuse("batch9 execution/publication prefix adapter is not implemented")


def current_plan(repo, plan):
    source = catalog(repo)
    if source["dependencies"]["assignments"] != plan["catalog_digest"]:
        refuse("current source/catalog/check provenance changed")


def window_clock(plan, applied, rows, window, now):
    integer(window, 0, len(plan["schedule"]["windows"]) - 2)
    started = rows[-1]["value"]["resumed_at"] if rows else applied["applied_at"]
    allocation = plan["schedule"]["window_seconds"][window] - 360
    if not started <= clock(now) <= min(started + allocation, applied["wall_deadline"]):
        refuse("active window clock rollback or allocation overrun")


def journal(directory, plan, applied):
    """Read an append-only stopped-boundary journal; a torn transition stops it."""
    root = plain_path(directory / "window-transitions")
    if not root.exists():
        return []
    paths = sorted(root.iterdir())
    if len(paths) > 18 or [p.name for p in paths] != [f"{i:02d}.json" for i in range(len(paths))]:
        refuse("torn, renamed or excessive window transition journal")
    rows = []
    for i, path in enumerate(paths):
        row = read(plain_path(path))
        if type(row) is not dict or set(row) != {"operation", "previous", "value", "authentication"}:
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
        rows.append(row)
    return rows


def pause(repo, directory, *, owned, now):
    """Stop only after current source and independently published prefix replay."""
    import claude_owned_auth

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


def resume_window(repo, directory, *, owned, now):
    """Manual stopped renewal only; owned verifier covers the whole next window."""
    import claude_owned_auth

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
        or value["plan_digest"] != digest(validate_plan(plan))
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
        validate_plan(plan)
        if set(plan["catalog"]["binding"]) != {
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
    import reporting_admission_v6 as admission
    import review
    import review_claims
    from tasks import atomic_json, atomic_text

    started, monotonic = time.time(), time.monotonic()
    directory = plain_path(Path(directory))
    batch = load_preparation(directory)
    plan = batch["plan"]
    # Until prefix replay is implemented, any changed public context (including
    # newly published children) remains closed. No arbitrary exclusion is applied.
    current_plan(repo, plan)
    units = plan["catalog"]["components"] + [plan["catalog"]["integration"]]
    matches = [unit for unit in units if unit["id"] == unit_id]
    if len(matches) != 1:
        refuse("unknown child or renamed assignment")
    unit = matches[0]
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
    if started + 1740 > window_start + plan["schedule"]["window_seconds"][window] - 360:
        refuse("complete child allocation does not fit the current window")
    if started + 1740 > min(batch["application"]["wall_deadline"], batch["authorization"]["expires_at"]):
        refuse("complete child allocation does not fit")
    claims = read(directory / "batch-claims.json")
    for candidate in units:
        review_claims.verify(repo, batch, candidate, claims[candidate["id"]])
    extra = {"items": [], "files": {}, "dependencies": {}}
    if unit_id == "integration":
        # This production gate is deliberately still closed, not a receipt flag.
        replay_prefix(repo, directory, plan, len(plan["schedule"]["windows"]) - 3)
        reports, dependencies = {}, {}
        for component in plan["catalog"]["components"]:
            child = directory / "units" / component["id"]
            child_meta = review.verify_packet(child)
            publication = review.verify_publication(repo, child)
            dependencies[component["id"]] = {
                **{
                    key: child_meta[key]
                    for key in (
                        "review_sha256",
                        "diagnostics_sha256",
                        "coverage_sha256",
                        "reporting_sha256",
                        "terminal_sha256",
                    )
                },
                "execution_sha256": review.digest(child / "reporting-execution.json"),
                "publication_sha256": publication["body_sha256"],
            }
            reports[component["id"]] = (child / "review.md").read_bytes()
        existing = sum(
            (directory / "packet" / name).stat().st_size
            for name in plan["catalog"]["files"]
            if name.startswith("projections/")
        )
        extra = integration_reports(plan, reports, dependencies, existing_projection_bytes=existing)
    owned = claude_owned_auth.require(owned_auth)
    actual_admission = admission.check(repo, owned_auth=owned, capacity_required=True)
    if digest(actual_admission) != digest(batch["admission"]):
        refuse("current V6 admission differs; renewal runtime is not available")
    if (
        owned.current_binding(900, plan["schedule"]["window_seconds"][window] - 360)
        != batch["unit_policy"]["authentication"]
    ):
        refuse("current whole-window authentication changed")
    current_plan(repo, plan)
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
        "context_ids": surrounding_ids(plan["catalog"], unit),
        "dependencies": extra["dependencies"],
        "material_claim": digest(claims[unit_id]),
        "policy_digest": digest(batch["unit_policy"]),
        "authorization_digest": digest(batch["authorization"]),
    }
    atomic_json(packet / "assignment.json", assignment)
    meta = review.verify_packet(directory)
    meta = {key: value for key, value in meta.items() if key not in _result_fields()}
    meta.update(kind="batch-unit", batch_version=9, batch_unit=assignment, files=packet_hashes(packet))
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
                started + 1740, batch["application"]["wall_deadline"], batch["authorization"]["expires_at"]
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
    units = batch["plan"]["catalog"]["components"]
    if not any(digest(unit) == digest(candidate) for candidate in units):
        refuse("integration child verification awaits qualified prefix replay")
    if directory != parent / "units" / unit["id"]:
        refuse("copied or renamed child")
    claims = read(parent / "batch-claims.json")
    review_claims.verify(repo, batch, unit, claims[unit["id"]])
    expected = {
        "schema_version": 9,
        "batch_sha256": digest(batch),
        "unit": unit,
        "required_ids": unit["required_ids"],
        "context_ids": surrounding_ids(batch["plan"]["catalog"], unit),
        "dependencies": {},
        "material_claim": digest(claims[unit["id"]]),
        "policy_digest": digest(batch["unit_policy"]),
        "authorization_digest": digest(batch["authorization"]),
    }
    if digest(assignment) != digest(expected) or any(meta[k] != batch["binding"][k] for k in IDENTITY):
        refuse("child source/contract/owner/assignment changed")
    expected_files = {
        **batch["plan"]["catalog"]["files"],
        "assignment.json": review.digest(directory / "packet/assignment.json"),
    }
    if meta["files"] != expected_files or meta["review_policy"] != batch["unit_policy"]:
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
        != min(start + 1740, batch["application"]["wall_deadline"], batch["authorization"]["expires_at"])
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
        import reporting_admission_v6 as admission

        self.check_clock()
        owned = claude_owned_auth.require(owned_auth)
        import review

        actual, batch, reservation = runtime_identity(self.repo, self.directory)
        if digest({k: v for k, v in meta.items() if k not in review.RESULT_FIELDS}) != digest(
            {k: v for k, v in actual.items() if k not in review.RESULT_FIELDS}
        ) or digest(reservation) != digest(self.reservation):
            refuse("dispatch packet, assignment, source or preparation changed")
        self.check_clock()
        current_plan(self.repo, batch["plan"])
        self.check_clock()
        if digest(admission.check(self.repo, owned_auth=owned, capacity_required=True)) != digest(
            batch["admission"]
        ):
            refuse("actual V6 admission changed; successor lineage awaits published prefix")
        self.check_clock()
        parent = self.directory.parent.parent
        rows = journal(parent, batch["plan"], batch["application"])
        if rows:
            refuse("stopped-window runtime awaits independent published prefix replay")
        unit = meta["batch_unit"]["unit"]
        components = batch["plan"]["catalog"]["components"]
        if unit != components[0]:
            refuse("later component runtime awaits independent published prefix replay")
        window_clock(batch["plan"], batch["application"], rows, 0, self.check_clock())
        if (
            owned.current_binding(900, batch["plan"]["schedule"]["window_seconds"][0] - 360)
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
            owned.current_binding(900, self.batch["plan"]["schedule"]["window_seconds"][0] - 360)
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
    """Initial component only. No batch-wide CLI or publication/readiness credit."""
    import claude_owned_auth
    import review_claude

    dispatch = ChildDispatch(repo, directory)
    current_plan(repo, dispatch.batch["plan"])
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
        "scope": "component-only",
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
