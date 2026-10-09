"""Closed G20 public material profile; no execution or readiness authority."""

import copy
import hashlib
import os
import re
import stat
from pathlib import Path

from review_coverage import strict_json
from tasks import digest
from workflow import WorkflowError

PROFILE = "issue31-complete-public-catalog-v1"
CONTEXT_BYTES = 4_000_000
SOURCE_BYTES = 500_000
SNAPSHOT_BYTES = 12_000_000
ITEMS = 2400
RANGE_BYTES = 12_000_000
RANGE_LINES = 220_000


def refuse(message):
    raise WorkflowError("Public catalog v1: " + message)


def selected(repo, issue, plan):
    if type(plan) is int and plan == 6076545397:
        return selected_g21(repo, issue, plan)
    if type(issue) is not int or type(plan) is not int:
        refuse("invalid contract identity")
    if plan != 6074133818:
        return False
    import reporting_activation_v6 as authority

    if issue != 31 or authority.authorization(repo) != {
        "contract_digest": authority.G20_CONTRACT_DIGEST,
        "approval_digest": authority.G20_APPROVAL_DIGEST,
    }:
        refuse("current G20 authority required")
    return True


def read_context(path):
    """Public JSON only: a bounded, quiescent, no-follow exact read."""
    path = Path(path)
    if path.is_symlink():
        refuse("unsafe public context")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_size > CONTEXT_BYTES:
            refuse("public context bounds or identity")
        parts, total = [], 0
        while True:
            chunk = os.read(fd, min(65536, CONTEXT_BYTES + 1 - total))
            if not chunk:
                break
            parts.append(chunk)
            total += len(chunk)
            if total > CONTEXT_BYTES:
                refuse("public context exceeds bound")
        after = os.fstat(fd)
        current = path.lstat()
        fields = ("st_dev", "st_ino", "st_mode", "st_nlink", "st_size", "st_mtime_ns", "st_ctime_ns")
        if any(
            getattr(before, k) != getattr(after, k) or getattr(after, k) != getattr(current, k)
            for k in fields
        ):
            refuse("public context changed")
        value = strict_json(b"".join(parts).decode("utf-8"))
    finally:
        os.close(fd)
    expected = {
        "pull_request",
        "issue",
        "designated_plan_comment",
        "issue_comments",
        "pr_comments",
        "inline_comments",
        "reviews",
        "check_runs",
        "note",
        "commit_statuses",
        "hosted_receipts",
    }
    if type(value) is not dict or set(value) != expected:
        refuse("public context shape")
    for key in (
        "issue_comments",
        "pr_comments",
        "inline_comments",
        "reviews",
        "check_runs",
        "commit_statuses",
    ):
        if type(value[key]) is not list:
            refuse("public context collection")
    for key in ("issue", "designated_plan_comment", "pull_request"):
        if type(value[key]) is not dict:
            refuse("public context identity")
    for key in ("issue", "designated_plan_comment", "pull_request"):
        if type(value[key].get("id")) is not int or value[key]["id"] <= 0:
            refuse("public context record ID")
    for key in ("issue_comments", "pr_comments", "inline_comments", "reviews"):
        ids = []
        for row in value[key]:
            if type(row) is not dict or type(row.get("id")) is not int or row["id"] <= 0:
                refuse("public context comment ID")
            ids.append(row["id"])
        if len(ids) != len(set(ids)):
            refuse("duplicate public context record")
    if type(value["note"]) is not str or type(value["hosted_receipts"]) is not list:
        refuse("public context auxiliary fields")
    issue, plan, pr = (value[k] for k in ("issue", "designated_plan_comment", "pull_request"))
    if (
        type(issue.get("number")) is not int
        or issue["number"] != 31
        or type(pr.get("number")) is not int
        or pr["number"] != 32
        or plan["id"] != 6074133818
        or issue.get("html_url") != "https://github.com/Zi-Deng/FLOW-DC/issues/31"
        or pr.get("html_url") != "https://github.com/Zi-Deng/FLOW-DC/pull/32"
        or plan.get("issue_url") != "https://api.github.com/repos/Zi-Deng/FLOW-DC/issues/31"
        or type(plan.get("user")) is not dict
        or plan["user"].get("login") != "Zi-Deng"
        or type(plan["user"].get("id")) is not int
        or plan["user"]["id"] != 29555112
        or type(plan.get("body")) is not str
        or type(issue.get("title")) is not str
        or type(issue.get("body")) is not str
    ):
        refuse("public context designated identity")
    for side in ("head", "base"):
        ref = pr.get(side)
        if (
            type(ref) is not dict
            or type(ref.get("sha")) is not str
            or re.fullmatch(r"[0-9a-f]{40}", ref["sha"]) is None
        ):
            refuse("public context Git identity")
    return value


def snapshot(repo, commit, output, config):
    import review

    limits = copy.deepcopy(config)
    if limits["max_source_file_bytes"] != 250000 or limits["max_snapshot_bytes"] != SNAPSHOT_BYTES:
        refuse("unexpected historical snapshot policy")
    limits["max_source_file_bytes"] = SOURCE_BYTES
    return review.snapshot(repo, commit, output, limits)


def verify_metadata(meta):
    if type(meta.get("plan_comment")) is int and meta["plan_comment"] == 6076545397:
        return verify_metadata_g21(meta)
    declared = meta.get("public_catalog_profile")
    current = type(meta.get("plan_comment")) is int and meta["plan_comment"] == 6074133818
    if declared is None and not current:
        return
    if declared != PROFILE or not current or type(meta.get("issue")) is not int or meta["issue"] != 31:
        refuse("profile/contract mismatch")
    if (
        meta.get("repository") != "Zi-Deng/FLOW-DC"
        or type(meta.get("schema_version")) is not int
        or meta["schema_version"] != 7
    ):
        refuse("profile metadata mismatch")


def validate_plan(plan):
    if type(plan) is not dict or set(plan) != {
        "schema_version",
        "profile",
        "catalog",
        "catalog_digest",
        "schedule",
        "limits",
    }:
        refuse("plan fields")
    if type(plan["schema_version"]) is not int or plan["schema_version"] != 10 or plan["profile"] != PROFILE:
        refuse("plan version/profile")
    validate_catalog(plan["catalog"])
    import review_batch_windows_v1 as windows

    limits = {**windows.LIMITS, "items": ITEMS, "bytes": RANGE_BYTES, "lines": RANGE_LINES}
    if (
        plan["limits"] != limits
        or plan["catalog_digest"] != digest(plan["catalog"])
        or plan["schedule"] != windows.schedule([u["id"] for u in plan["catalog"]["components"]])
    ):
        refuse("plan bindings")
    from claude_reporting import _json_bytes

    _json_bytes(plan, 2000000)
    return plan


def plan_catalog(catalog):
    import review_batch_windows_v1 as windows

    return validate_plan(
        {
            "schema_version": 10,
            "profile": PROFILE,
            "catalog": catalog,
            "catalog_digest": digest(catalog),
            "schedule": windows.schedule([u["id"] for u in catalog["components"]]),
            "limits": {**windows.LIMITS, "items": ITEMS, "bytes": RANGE_BYTES, "lines": RANGE_LINES},
        }
    )


def validate_binding(binding, *, assignments=False):
    import reporting_activation_v6 as current

    if type(binding) is dict and binding.get("contract") == current.G21_CONTRACT_DIGEST:
        return validate_binding_g21(binding, assignments=assignments)
    import reporting_activation_v6 as authority

    keys = {
        "local",
        "hosted",
        "source",
        "authorization",
        "context",
        "contract",
        "identity",
        "policy",
        "inventory",
        "profile",
    }
    if assignments:
        keys.add("assignments")
    if type(binding) is not dict or set(binding) != keys:
        refuse("closed catalog provenance required")
    if any(
        type(value) is not str or re.fullmatch(r"[0-9a-f]{64}", value) is None for value in binding.values()
    ):
        refuse("invalid catalog provenance digest")
    if (
        binding["profile"] != digest(PROFILE)
        or binding["contract"] != authority.G20_CONTRACT_DIGEST
        or binding["authorization"]
        != digest(
            {
                "contract_digest": authority.G20_CONTRACT_DIGEST,
                "approval_digest": authority.G20_APPROVAL_DIGEST,
            }
        )
    ):
        refuse("catalog profile/authority mismatch")


def validate_catalog(catalog):
    from review_batch_windows_v1 import checksum, context_reference, integer, schedule

    if type(catalog) is not dict or set(catalog) != {
        "binding",
        "items",
        "components",
        "integration",
        "files",
    }:
        refuse("catalog fields differ")
    validate_binding(catalog["binding"])
    items = catalog["items"]
    if type(items) is not list or not 1 <= len(items) <= 2400:
        refuse("inventory bounds")
    lookup = {}
    for item in items:
        if (
            type(item) is not dict
            or set(item)
            != {
                "id",
                "family",
                "artifact",
                "start_line",
                "end_line",
                "bytes",
                "sha256",
                "kind",
                "source_family",
            }
            or type(item["id"]) is not str
            or re.fullmatch("[0-9a-f]{24}", item["id"]) is None
            or item["id"] in lookup
            or type(item["family"]) is not str
            or not item["family"]
            or type(item["source_family"]) is not str
            or not item["source_family"]
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
        integer(item["start_line"], 1, 220000)
        integer(item["end_line"], item["start_line"], 220000)
        integer(item["bytes"], 1, 12000000)
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
        sum(i["bytes"] for i in items) > 12000000
        or sum(i["end_line"] - i["start_line"] + 1 for i in items) > 220000
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
            if not unit["id"].startswith(item["family"] + "-part-") and unit["id"] != "integration":
                refuse("semantic domain ownership differs")
            families[item["family"]] = unit["id"]
        seen.update(ids)
    if catalog["integration"]["id"] != "integration" or seen != lookup.keys():
        refuse("incomplete primary partition")


DOMAIN_GROUPS = {
    "contract-guidance": ["contract", "governance", "operating-guides"],
    "adoption-finish": [
        "archive-install",
        "finish",
        "product-integrity-tests",
        "unmapped:agentic:finish_gates",
        "unmapped:agentic:installed_qualification_v1",
        "unmapped:agentic:workflow_fixture",
    ],
    "coverage-current": ["coverage-current", "coverage-guide"],
    "coverage-evolution": ["coverage-issue31", "coverage-v5-v6"],
    "context-capacity": [
        "capacity-native",
        "context-observation",
        "observation-v1",
        "prompt-capacity",
        "provider-guide",
    ],
    "native-isolation": ["native-auth", "owned-execution"],
    "native-tools": ["native-telemetry-legacy", "native-telemetry-v6", "native-tool-rendering"],
    "report-codec-policy": ["report-codec", "report-policy", "report-material"],
    "report-stream": ["report-stream-v7", "report-stream-v8"],
    "diagnostic-evolution": ["diagnostic-legacy", "diagnostic-v6-v8"],
    "admission-history": ["current-admission", "reporting-v1", "reporting-v2"],
    "recovery-history": ["reporting-v3", "reporting-v4"],
    "packet-continuation": ["packet", "projection-navigation", "continuation"],
    "ci-transport": ["pipeline", "unmapped:agentic:ci_diagnostics", "unmapped:Makefile"],
    "runner": [
        "runner",
        "unmapped:agentic:scoped_scheduling_seed",
        "unmapped:agentic:suite_deadline_issue31",
    ],
}


def domain(family):
    if family.startswith(("unmapped:contract-predecessor-", "unmapped:criterion-")) or family in {
        "unmapped:domain-policy.txt",
        "unmapped:repository-policy.txt",
        "unmapped:review-policy.txt",
    }:
        return "contract-guidance"
    for name, families in DOMAIN_GROUPS.items():
        if family in families:
            return name
    if family == "unmapped:test-map.json":
        return "test-mapping"
    return "family-" + digest(family)[:16] if family.startswith("unmapped:") else family


def partition(packet, inventory, binding):
    from review_batch_windows_v1 import FAMILIES, context_reference, integer

    packet = Path(packet)
    files = {}
    for path in packet.rglob("*"):
        if path.is_symlink():
            refuse("packet symlink")
        if path.is_file():
            files[str(path.relative_to(packet))] = hashlib.sha256(path.read_bytes()).hexdigest()
    if (
        type(inventory) is not list
        or not inventory
        or any(
            type(i) is not dict or i.get("omitted") or not i.get("artifact") or type(i.get("path")) is not str
            for i in inventory
        )
    ):
        refuse("invalid or omitted inventory")
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
        return domain(FAMILIES.get(key, "unmapped:" + key))

    def source_family(item):
        path = item["path"]
        if item["kind"] in {"finding", "cross-boundary"}:
            return family(item)
        stem = Path(path).stem.removeprefix("test_")
        if path.startswith("tests/agentic/") and path.endswith(".py"):
            stem = max((s for s in stems if stem == s or stem.startswith(s + "_")), key=len, default=stem)
        key = (
            "agentic:" + stem
            if path.startswith(("scripts/agentic/", "tests/agentic/")) and path.endswith(".py")
            else path
        )
        return FAMILIES.get(key, "unmapped:" + key)

    items, owners = [], {}
    for item in inventory:
        if item.get("omitted") or not item.get("artifact"):
            refuse("required source remains omitted")
        raw = (packet / item["artifact"]).read_bytes()
        lines = raw.decode("utf-8").splitlines(keepends=True)
        lo, hi = item["start_line"], item["end_line"]
        integer(lo, 1, len(lines))
        integer(hi, lo, len(lines))
        size = len("".join(lines[lo - 1 : hi]).encode("utf-8"))
        if type(item.get("bytes")) is not int or size != item["bytes"]:
            refuse("range size mismatch")
        if item.get("projection"):
            import review_projection

            if not review_projection.validate(packet, item):
                refuse("projection mismatch")
        owner = family(item)
        row = dict(
            id=item["id"],
            family=owner,
            source_family=source_family(item),
            artifact=item["artifact"],
            start_line=lo,
            end_line=hi,
            bytes=size,
            sha256=hashlib.sha256(raw).hexdigest(),
            kind=item["kind"],
        )
        items.append(row)
        owners.setdefault(owner, []).append(row)
    all_ids = {i["id"] for i in items}
    if len(all_ids) != len(items):
        refuse("duplicate primary IDs")
    components = []
    for owner, rows in sorted(owners.items()):
        if owner == "integration":
            continue
        part, size, lines, number = [], 0, 0, 1
        for row in sorted(rows, key=lambda r: (r["artifact"], r["start_line"], r["end_line"], r["id"])):
            length = row["end_line"] - row["start_line"] + 1
            if part and (len(part) == 128 or size + row["bytes"] > 500000 or lines + length > 9000):
                components.append(
                    dict(
                        id=owner + "-part-" + str(number).zfill(2),
                        required_ids=part,
                        context_ids=context_reference(all_ids, part),
                    )
                )
                part, size, lines, number = [], 0, 0, number + 1
            part.append(row["id"])
            size += row["bytes"]
            lines += length
        components.append(
            dict(
                id=owner + "-part-" + str(number).zfill(2),
                required_ids=part,
                context_ids=context_reference(all_ids, part),
            )
        )
    cross = [r["id"] for r in owners.get("integration", [])]
    value = dict(
        binding=binding,
        items=items,
        components=components,
        integration=dict(id="integration", required_ids=cross, context_ids=context_reference(all_ids, cross)),
        files=files,
    )
    validate_catalog(value)
    return value


def catalog_observation(repo):
    import check_runner
    import reporting_activation_v6 as activation
    from reporting_activation_v2 import read

    state = read(repo.main / ".agentic-local/tasks/issue-31.json")
    authority = activation.authorization(repo)
    if authority != {
        "contract_digest": activation.G20_CONTRACT_DIGEST,
        "approval_digest": activation.G20_APPROVAL_DIGEST,
    }:
        refuse("final catalog requires actual G20 authority")
    return {"source": check_runner.source(repo.root), "authority": authority, "task": digest(state)}


def final_catalog_result(repo, initial, value):
    if catalog_observation(repo) != initial:
        refuse("catalog source or authority changed during assembly")
    return value


def catalog(repo, *, plan_only=False, packet_target=None, batch_directory=None):
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
    from reporting_activation_v2 import read
    from review_batch_windows_v1 import (
        full_checks,
        full_checks_g14,
        full_checks_g15,
        full_checks_g16,
        full_checks_g17,
        full_checks_g18,
        full_checks_g19,
        full_checks_g20,
        reconcile_public_context,
    )
    from tasks import issue_contract, plain_path
    from workflow import configuration, run, write_json

    initial = catalog_observation(repo)
    state = read(repo.main / ".agentic-local/tasks/issue-31.json")
    authority = activation.authorization(repo)
    if digest(state) != initial["task"] or authority != initial["authority"]:
        refuse("catalog initial authority changed")
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
    if authority["contract_digest"] != activation.G20_CONTRACT_DIGEST:
        refuse("G20 required")
    current_contract = activation.G20_CONTRACT
    current_digest = activation.G20_CONTRACT_DIGEST
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
        full_checks_g20(repo, directory, meta)
        if current_digest == activation.G20_CONTRACT_DIGEST
        else full_checks_g19(repo, directory, meta)
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
    context = read_context(original / "context.json")
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
        head_index = snapshot(repo, meta["head_sha"], packet / "source", meta["config"])
        base_index = snapshot(repo, ancestor, packet / "base-source", meta["config"])
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
        inventory = normalize(packet, inventory)
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
        binding["profile"] = digest(PROFILE)
        preliminary = partition(packet, inventory, binding)
        add_relations(packet, inventory, preliminary, context=context)
        binding["inventory"] = digest(inventory)
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
            return final_catalog_result(
                repo, initial, {"plan": planned_record, "metadata": copy.deepcopy(meta)}
            )
        if plan_only:
            return final_catalog_result(repo, initial, planned_record)
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
        return final_catalog_result(
            repo,
            initial,
            {
                "components": components,
                "items": additional,
                "files": {r["artifact"]: (packet / r["artifact"]).read_bytes() for r in additional},
                "dependencies": {**binding, "assignments": digest(planned)},
            },
        )


def whole_source_mapping(packet, inventory):
    """Map formerly omitted whole Git sources to every newly required range."""
    import json

    import review_packet

    mappings = []
    for index_name, revision in (("source-index.json", "head"), ("base-source-index.json", "base")):
        for row in strict_json((packet / index_name).read_text()):
            if not row.get("snapshot"):
                continue
            raw = (packet / row["snapshot"]).read_bytes()
            blob = hashlib.sha1(b"blob " + str(len(raw)).encode("ascii") + b"\0" + raw).hexdigest()
            if row.get("blob") != blob:
                refuse("immutable source blob differs")
            if len(raw) <= 250000:
                continue
            ranges = [
                i
                for i in inventory
                if i.get("artifact") == row["snapshot"]
                or (
                    type(i.get("projection")) is dict
                    and i["projection"].get("source_artifact") == row["snapshot"]
                )
            ]
            if not ranges:
                # Unrelated source remains available context, not an invented requirement.
                continue
            required = set(range(1, len(raw.decode("utf-8").splitlines()) + 1))
            covered = set()
            for item in ranges:
                if item.get("projection"):
                    import review_projection

                    if not review_projection.validate(packet, item):
                        refuse("restored projection differs")
                    projected_lines = len(
                        (packet / item["artifact"]).read_bytes().decode("utf-8").splitlines()
                    )
                    if (item["start_line"], item["end_line"]) != (1, projected_lines):
                        refuse("restored projection is incomplete")
                    covered.add(item["projection"]["source_line"])
                else:
                    covered.update(range(item["start_line"], item["end_line"] + 1))
            if covered != required:
                refuse("restored source lacks whole line coverage")
            for kind in sorted({i["kind"] for i in ranges}):
                mappings.append(
                    {
                        "old_omitted_id": review_packet.stable_id(
                            kind, revision, row["path"], "file exceeds configured size limit"
                        ),
                        "path": row["path"],
                        "revision": revision,
                        "blob": row["blob"],
                        "sha256": hashlib.sha256(raw).hexdigest(),
                        "range_ids": sorted(i["id"] for i in ranges),
                        "lines": len(required),
                    }
                )
    raw = (json.dumps({"profile": PROFILE, "mappings": mappings}, sort_keys=True, indent=2) + "\n").encode()
    name = "public-source-mapping.json"
    (packet / name).write_bytes(raw)
    inventory.append(
        dict(
            id=digest([name, hashlib.sha256(raw).hexdigest()])[:24],
            kind="cross-boundary",
            path=name,
            artifact=name,
            revision="packet",
            start_line=1,
            end_line=len(raw.decode().splitlines()),
            bytes=len(raw),
            links=[],
        )
    )


def public_reference(packet, inventory, context, url):
    """Resolve retained public bodies; outside-scope citations confer no ownership."""
    from urllib.parse import urlsplit

    parsed = urlsplit(url)
    if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password:
        refuse("invalid public citation")
    if parsed.netloc != "github.com" or not parsed.path.startswith("/Zi-Deng/FLOW-DC/"):
        return "external-citation", []
    if not re.fullmatch(r"/Zi-Deng/FLOW-DC/(?:pull/32|issues/31)", parsed.path):
        return "outside-retained-scope", []
    if parsed.query or type(context) is not dict:
        refuse("missing exact retained public context")
    surface = None
    if parsed.path.endswith("/pull/32"):
        for prefix, name in (
            ("issuecomment-", "pr_comments"),
            ("pullrequestreview-", "reviews"),
            ("discussion_r", "inline_comments"),
        ):
            if re.fullmatch(re.escape(prefix) + r"[1-9][0-9]*", parsed.fragment):
                surface = name
                break
    elif re.fullmatch(r"issuecomment-[1-9][0-9]*", parsed.fragment):
        surface = "issue_comments"
    if surface is None:
        refuse("unresolved in-scope public citation")
    rows = context.get(surface)
    if type(rows) is not list:
        refuse("missing public citation collection")
    matches = [row for row in rows if type(row) is dict and row.get("html_url") == url]
    if len(matches) != 1:
        refuse("unresolved or ambiguous in-scope public citation")
    record = matches[0]
    if (
        type(record.get("id")) is not int
        or record["id"] <= 0
        or int(re.search(r"[0-9]+$", parsed.fragment).group()) != record["id"]
        or type(record.get("body")) is not str
        or not record["body"]
    ):
        refuse("invalid retained public citation identity")
    if surface == "issue_comments":
        name = (
            "plan.txt"
            if record["id"] == context.get("designated_plan_comment", {}).get("id")
            else f"contract-predecessor-{record['id']}.txt"
        )
        targets = [row for row in inventory if row.get("artifact") == name]
        expected = record["body"].encode() + (b"\n" if name == "plan.txt" else b"")
    else:
        targets = [
            row
            for row in inventory
            if row.get("path") == f"{surface}:{record['id']}"
            and str(row.get("artifact", "")).startswith("whole-responses/")
        ]
        expected = record["body"].encode()
    artifacts = {row.get("artifact") for row in targets}
    if not targets or len(artifacts) != 1 or any(row.get("omitted") for row in targets):
        refuse("public citation lacks unambiguous whole primary owner")
    artifact = next(iter(artifacts))
    actual = (Path(packet) / artifact).read_bytes()
    if actual != expected:
        refuse("public citation whole body differs")
    covered = [line for row in targets for line in range(row["start_line"], row["end_line"] + 1)]
    if sorted(covered) != list(range(1, len(expected.decode().splitlines()) + 1)):
        refuse("public citation whole primary lines differ")
    return "retained-primary", sorted(row["id"] for row in targets)


def add_relations(packet, inventory, catalog=None, *, context=None):
    """Compact, lossless adjacency is primary integration material, never credit."""
    import json

    ids = sorted(i["id"] for i in inventory)
    if len(ids) != len(set(ids)):
        refuse("duplicate relationship owner")
    positions = {key: n for n, key in enumerate(ids)}
    edges, references = [], []
    for item in inventory:
        for link in item.get("links", []):
            if link in positions:
                edges.append([positions[item["id"]], positions[link]])
            elif type(link) is str and link.startswith("https://"):
                classification, targets = public_reference(packet, inventory, context, link)
                references.append(
                    {"source": item["id"], "url": link, "classification": classification, "targets": targets}
                )
                edges.extend([positions[item["id"]], positions[target]] for target in targets)
            else:
                refuse("unresolved obligation link")
    edges = sorted({tuple(edge) for edge in edges})
    value = {
        "profile": PROFILE,
        "ids": ids,
        "edges": edges,
        "public_references": sorted(references, key=lambda row: (row["source"], row["url"])),
    }
    if catalog is not None:
        validate_catalog(catalog)
        owners = {
            key: unit["id"]
            for unit in catalog["components"] + [catalog["integration"]]
            for key in unit["required_ids"]
        }
        if set(owners) != set(ids):
            refuse("relationship ownership incomplete")
        value["owners"] = [owners[key] for key in ids]
        value["families"] = {row["id"]: row["source_family"] for row in catalog["items"]}
        value["cross_part_edges"] = [
            edge for edge in sorted(edges) if owners[ids[edge[0]]] != owners[ids[edge[1]]]
        ]
    raw = (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()
    name = "public-catalog-relations.json"
    (packet / name).write_bytes(raw)
    inventory.append(
        dict(
            id=digest([name, hashlib.sha256(raw).hexdigest()])[:24],
            kind="cross-boundary",
            path=name,
            artifact=name,
            start_line=1,
            end_line=1,
            bytes=len(raw),
        )
    )


def normalize(packet, inventory):
    """Canonical nonoverlapping ranges, with an exact original-obligation map."""
    import json

    originals = {row["id"]: row for row in inventory}
    if len(originals) != len(inventory):
        refuse("duplicate original obligations")
    groups = {}
    for row in inventory:
        if row.get("omitted") or not row.get("artifact"):
            refuse("normalization cannot repair omitted source")
        groups.setdefault(row["artifact"], []).append(row)
    result, mapping = [], {key: [] for key in originals}
    for artifact, rows in sorted(groups.items()):
        raw = (packet / artifact).read_bytes()
        lines = raw.decode("utf-8").splitlines(keepends=True)
        events = {}
        for row in rows:
            lo, hi = row["start_line"], row["end_line"]
            if type(lo) is not int or type(hi) is not int or not 1 <= lo <= hi <= len(lines):
                refuse("normalization range bounds")
            events.setdefault(lo, []).append((row["id"], True))
            events.setdefault(hi + 1, []).append((row["id"], False))
        active = set()
        points = sorted(events)
        for n, lo in enumerate(points[:-1]):
            for key, entering in events[lo]:
                if entering:
                    active.add(key)
                else:
                    active.remove(key)
            if not active:
                continue
            hi = points[n + 1] - 1
            owners = [originals[key] for key in sorted(active)]
            if len({(row["path"], row.get("revision")) for row in owners}) != 1:
                refuse("overlap crosses semantic owners")
            if any(
                row["kind"] == "finding" and (lo != row["start_line"] or hi != row["end_line"])
                for row in owners
            ):
                refuse("whole finding cannot be fragmented")
            representative = next((owner for owner in owners if owner["kind"] == "finding"), owners[0])
            row = copy.deepcopy(representative)
            if len(owners) != 1 or lo != row["start_line"] or hi != row["end_line"]:
                row["id"] = digest(
                    [PROFILE, artifact, hashlib.sha256(raw).hexdigest(), lo, hi, sorted(active)]
                )[:24]
            row.update(start_line=lo, end_line=hi, bytes=len("".join(lines[lo - 1 : hi]).encode()))
            row["links"] = sorted({link for owner in owners for link in owner.get("links", [])})
            result.append(row)
            for key in active:
                mapping[key].append(row["id"])
    lookup = {row["id"]: row for row in result}
    for key, targets in mapping.items():
        old = originals[key]
        expected = set(range(old["start_line"], old["end_line"] + 1))
        covered = {
            line
            for target in targets
            for line in range(lookup[target]["start_line"], lookup[target]["end_line"] + 1)
        }
        if covered != expected:
            refuse("original whole line set changed")
    for row in result:
        row["links"] = sorted(
            {target for link in row.get("links", []) for target in mapping.get(link, [link])}
        )
    record = {
        "profile": PROFILE,
        "original_inventory_sha256": digest(inventory),
        "originals": [
            [
                row["id"],
                row["artifact"],
                row["start_line"],
                row["end_line"],
                row["kind"],
                row["path"],
                row.get("revision"),
            ]
            for row in inventory
        ],
        "mapping": mapping,
    }
    raw = (json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n").encode()
    name = "public-obligation-mapping.json"
    (packet / name).write_bytes(raw)
    if len(raw) > 500000:
        refuse("complete obligation mapping exceeds integration bound")
    result.append(
        dict(
            id=digest([name, hashlib.sha256(raw).hexdigest()])[:24],
            kind="cross-boundary",
            path=name,
            artifact=name,
            start_line=1,
            end_line=1,
            bytes=len(raw),
            links=[],
        )
    )
    return result


def selected_g21(repo, issue, plan):
    if type(issue) is not int or type(plan) is not int:
        refuse("invalid contract identity")
    if plan != 6076545397:
        return False
    import reporting_activation_v6 as authority

    if issue != 31 or authority.authorization(repo) != {
        "contract_digest": authority.G21_CONTRACT_DIGEST,
        "approval_digest": authority.G21_APPROVAL_DIGEST,
    }:
        refuse("current G21 authority required")
    return True


def read_context_g21(path):
    """Public JSON only: a bounded, quiescent, no-follow exact read."""
    path = Path(path)
    if path.is_symlink():
        refuse("unsafe public context")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_size > CONTEXT_BYTES:
            refuse("public context bounds or identity")
        parts, total = [], 0
        while True:
            chunk = os.read(fd, min(65536, CONTEXT_BYTES + 1 - total))
            if not chunk:
                break
            parts.append(chunk)
            total += len(chunk)
            if total > CONTEXT_BYTES:
                refuse("public context exceeds bound")
        after = os.fstat(fd)
        current = path.lstat()
        fields = ("st_dev", "st_ino", "st_mode", "st_nlink", "st_size", "st_mtime_ns", "st_ctime_ns")
        if any(
            getattr(before, k) != getattr(after, k) or getattr(after, k) != getattr(current, k)
            for k in fields
        ):
            refuse("public context changed")
        value = strict_json(b"".join(parts).decode("utf-8"))
    finally:
        os.close(fd)
    expected = {
        "pull_request",
        "issue",
        "designated_plan_comment",
        "issue_comments",
        "pr_comments",
        "inline_comments",
        "reviews",
        "check_runs",
        "note",
        "commit_statuses",
        "hosted_receipts",
    }
    if type(value) is not dict or set(value) != expected:
        refuse("public context shape")
    for key in (
        "issue_comments",
        "pr_comments",
        "inline_comments",
        "reviews",
        "check_runs",
        "commit_statuses",
    ):
        if type(value[key]) is not list:
            refuse("public context collection")
    for key in ("issue", "designated_plan_comment", "pull_request"):
        if type(value[key]) is not dict:
            refuse("public context identity")
    for key in ("issue", "designated_plan_comment", "pull_request"):
        if type(value[key].get("id")) is not int or value[key]["id"] <= 0:
            refuse("public context record ID")
    for key in ("issue_comments", "pr_comments", "inline_comments", "reviews"):
        ids = []
        for row in value[key]:
            if type(row) is not dict or type(row.get("id")) is not int or row["id"] <= 0:
                refuse("public context comment ID")
            ids.append(row["id"])
        if len(ids) != len(set(ids)):
            refuse("duplicate public context record")
    if type(value["note"]) is not str or type(value["hosted_receipts"]) is not list:
        refuse("public context auxiliary fields")
    issue, plan, pr = (value[k] for k in ("issue", "designated_plan_comment", "pull_request"))
    if (
        type(issue.get("number")) is not int
        or issue["number"] != 31
        or type(pr.get("number")) is not int
        or pr["number"] != 32
        or plan["id"] != 6076545397
        or issue.get("html_url") != "https://github.com/Zi-Deng/FLOW-DC/issues/31"
        or pr.get("html_url") != "https://github.com/Zi-Deng/FLOW-DC/pull/32"
        or plan.get("issue_url") != "https://api.github.com/repos/Zi-Deng/FLOW-DC/issues/31"
        or type(plan.get("user")) is not dict
        or plan["user"].get("login") != "Zi-Deng"
        or type(plan["user"].get("id")) is not int
        or plan["user"]["id"] != 29555112
        or type(plan.get("body")) is not str
        or type(issue.get("title")) is not str
        or type(issue.get("body")) is not str
    ):
        refuse("public context designated identity")
    for side in ("head", "base"):
        ref = pr.get(side)
        if (
            type(ref) is not dict
            or type(ref.get("sha")) is not str
            or re.fullmatch(r"[0-9a-f]{40}", ref["sha"]) is None
        ):
            refuse("public context Git identity")
    return value


def verify_metadata_g21(meta):
    declared = meta.get("public_catalog_profile")
    current = type(meta.get("plan_comment")) is int and meta["plan_comment"] == 6076545397
    if declared is None and not current:
        return
    if declared != PROFILE or not current or type(meta.get("issue")) is not int or meta["issue"] != 31:
        refuse("profile/contract mismatch")
    if (
        meta.get("repository") != "Zi-Deng/FLOW-DC"
        or type(meta.get("schema_version")) is not int
        or meta["schema_version"] != 7
    ):
        refuse("profile metadata mismatch")


def validate_binding_g21(binding, *, assignments=False):
    import reporting_activation_v6 as authority

    keys = {
        "local",
        "hosted",
        "source",
        "authorization",
        "context",
        "contract",
        "identity",
        "policy",
        "inventory",
        "profile",
    }
    if assignments:
        keys.add("assignments")
    if type(binding) is not dict or set(binding) != keys:
        refuse("closed catalog provenance required")
    if any(
        type(value) is not str or re.fullmatch(r"[0-9a-f]{64}", value) is None for value in binding.values()
    ):
        refuse("invalid catalog provenance digest")
    if (
        binding["profile"] != digest(PROFILE)
        or binding["contract"] != authority.G21_CONTRACT_DIGEST
        or binding["authorization"]
        != digest(
            {
                "contract_digest": authority.G21_CONTRACT_DIGEST,
                "approval_digest": authority.G21_APPROVAL_DIGEST,
            }
        )
    ):
        refuse("catalog profile/authority mismatch")


def catalog_observation_g21(repo):
    import check_runner
    import reporting_activation_v6 as activation
    from reporting_activation_v2 import read

    state = read(repo.main / ".agentic-local/tasks/issue-31.json")
    authority = activation.authorization(repo)
    if authority != {
        "contract_digest": activation.G21_CONTRACT_DIGEST,
        "approval_digest": activation.G21_APPROVAL_DIGEST,
    }:
        refuse("final catalog requires actual G21 authority")
    return {"source": check_runner.source(repo.root), "authority": authority, "task": digest(state)}


def catalog_g21(repo, *, plan_only=False, packet_target=None, batch_directory=None):
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
    from reporting_activation_v2 import read
    from review_batch_windows_v1 import (
        full_checks,
        full_checks_g14,
        full_checks_g15,
        full_checks_g16,
        full_checks_g17,
        full_checks_g18,
        full_checks_g19,
        full_checks_g21,
        reconcile_public_context,
    )
    from tasks import issue_contract, plain_path
    from workflow import configuration, run, write_json

    initial = catalog_observation_g21(repo)
    state = read(repo.main / ".agentic-local/tasks/issue-31.json")
    authority = activation.authorization(repo)
    if digest(state) != initial["task"] or authority != initial["authority"]:
        refuse("catalog initial authority changed")
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
    if authority["contract_digest"] != activation.G21_CONTRACT_DIGEST:
        refuse("G21 required")
    current_contract = activation.G21_CONTRACT
    current_digest = activation.G21_CONTRACT_DIGEST
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
        full_checks_g21(repo, directory, meta)
        if current_digest == activation.G21_CONTRACT_DIGEST
        else full_checks_g19(repo, directory, meta)
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
    context = read_context_g21(original / "context.json")
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
        head_index = snapshot(repo, meta["head_sha"], packet / "source", meta["config"])
        base_index = snapshot(repo, ancestor, packet / "base-source", meta["config"])
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
        inventory = normalize(packet, inventory)
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
        binding["profile"] = digest(PROFILE)
        preliminary = partition(packet, inventory, binding)
        add_relations(packet, inventory, preliminary, context=context)
        binding["inventory"] = digest(inventory)
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
            return final_catalog_result_g21(
                repo, initial, {"plan": planned_record, "metadata": copy.deepcopy(meta)}
            )
        if plan_only:
            return final_catalog_result_g21(repo, initial, planned_record)
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
        return final_catalog_result_g21(
            repo,
            initial,
            {
                "components": components,
                "items": additional,
                "files": {r["artifact"]: (packet / r["artifact"]).read_bytes() for r in additional},
                "dependencies": {**binding, "assignments": digest(planned)},
            },
        )


def final_catalog_result_g21(repo, initial, value):
    if catalog_observation_g21(repo) != initial:
        refuse("catalog source or authority changed during assembly")
    return value
