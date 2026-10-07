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


def catalog(repo, *, plan_only=False):
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
        planned = partition(packet, inventory, binding)
        planned_record = plan_catalog(planned)
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
