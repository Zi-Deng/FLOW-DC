"""Explicit bounded review batches. Assignments never replace the parent inventory."""

import contextlib
import fcntl
import math
import os
import shutil
import stat
import time
from pathlib import Path

import review_coverage as coverage
from tasks import atomic_json, atomic_text, digest, plain_path
from workflow import WorkflowError

SCHEMA = 4
BINDING = (
    "repository",
    "pr",
    "issue",
    "plan_comment",
    "head_sha",
    "base_sha",
    "merge_base_sha",
    "requested_model",
)


def api():
    # Avoid a circular import: review owns the existing single-request machinery.
    import review

    return review


def bounded(value, name, integer=False):
    if type(value) not in ({int} if integer else {int, float}) or not math.isfinite(value) or value <= 0:
        raise WorkflowError(f"Batch {name} must be explicit, finite and positive")
    return value


def budget(requests, credits, seconds, unit_credits, unit_seconds):
    result = {
        "requests": bounded(requests, "requests", True),
        "credits": bounded(credits, "credits"),
        "seconds": bounded(seconds, "seconds"),
        "unit_credits": bounded(unit_credits, "unit credits"),
        "unit_seconds": bounded(unit_seconds, "unit seconds"),
    }
    if unit_credits > credits or unit_seconds > seconds:
        raise WorkflowError("Per-unit bounds exceed aggregate allocation")
    return result


def plan(directory):
    """Deterministic scope partition with explicit linked navigation context."""
    directory = Path(directory)
    meta = api().verify_packet(directory)
    if meta["schema_version"] not in {3, SCHEMA} or meta.get("batch_unit"):
        raise WorkflowError("Batch planning requires a current parent packet")
    packet = directory / "packet"
    inventory = coverage.read_json(packet / "required-material.json")["required"]
    lookup = {item["id"]: item for item in inventory}
    if not inventory or len(lookup) != len(inventory):
        raise WorkflowError("Invalid parent inventory")
    scopes = coverage.read_json(packet / "scopes.json")["scopes"]
    seen, components, integration = set(), [], []
    for scope in scopes:
        ids = scope["required_ids"]
        if not ids or len(set(ids)) != len(ids) or set(ids) - lookup.keys() or seen.intersection(ids):
            raise WorkflowError("Scopes do not partition the parent inventory")
        seen.update(ids)
        cross = [key for key in ids if lookup[key]["kind"] == "cross-boundary"]
        integration.extend(cross)
        primary = [key for key in ids if key not in cross]
        if primary:
            paths = {lookup[key]["path"] for key in primary}
            for mapping in coverage.read_json(packet / "test-map.json"):
                if mapping["changed_path"] in paths or paths.intersection(mapping["candidates"]):
                    paths.update([mapping["changed_path"], *mapping["candidates"]])
            links = {link for key in primary for link in lookup[key].get("links", [])}
            context = sorted(
                key
                for key, item in lookup.items()
                if key not in primary and (key in links or item["path"] in paths)
            )
            components.append(
                {
                    "id": f"unit-{len(components):04d}",
                    "kind": "component",
                    "scope": scope["id"],
                    "required_ids": primary,
                    "context_ids": context,
                    "links": sorted(links),
                }
            )
    if seen != lookup.keys() or not components or not integration:
        raise WorkflowError("Parent scopes omit material or the integration obligation")
    units = components + [
        {
            "id": "integration",
            "kind": "integration",
            "required_ids": integration,
            "context_ids": sorted(lookup.keys() - set(integration)),
            "depends_on": [unit["id"] for unit in components],
        }
    ]
    return {
        "schema_version": 1,
        "binding": {key: meta[key] for key in BINDING},
        "files": meta["files"],
        "policy": meta["config"],
        "inventory_sha256": api().digest(packet / "required-material.json"),
        "units": units,
        "note": "Assignments are navigation, not inspection. Full original inventory and surrounding context remain required.",
    }


def select(directory, limits):
    """Opt in before any attempt; no inference and no implicit paid defaults."""
    directory = plain_path(directory)
    meta = api().verify_packet(directory)
    expected = {**plan(directory), "budget": budget(**limits)}
    if meta["schema_version"] == SCHEMA:
        if load(directory) != expected:
            raise WorkflowError("Batch selection or budget changed; continuation refused")
        return expected
    if any(
        (directory / name).exists()
        for name in ("attempt.json", "review-capture.json", "review-result.json", "review.md")
    ):
        raise WorkflowError("Cannot convert an attempted single review into a batch")
    atomic_json(directory / "batch.json", expected)
    meta.update(schema_version=SCHEMA, batch_sha256=digest(expected))
    atomic_json(directory / "metadata.json", meta)
    return expected


def load(directory):
    meta = api().verify_packet(directory)
    saved = coverage.read_json(plain_path(Path(directory) / "batch.json"))
    if meta["schema_version"] != SCHEMA or digest(saved) != meta.get("batch_sha256"):
        raise WorkflowError("Batch plan changed")
    limits = saved.get("budget", {})
    if saved != {**plan(directory), "budget": budget(**limits)}:
        raise WorkflowError("Batch binding, inventory, policy or membership changed")
    return saved


def unit_path(directory, unit):
    return plain_path(Path(directory) / "units" / unit["id"])


def report_binding(directory):
    meta = api().verify_packet(directory)
    return {key: meta[key] for key in ("review_sha256", "diagnostics_sha256", "coverage_sha256")}


def material_for_report(packet, artifact, body):
    """Integration reads exact report bytes, including every line of findings."""
    lines = body.splitlines(keepends=True)
    result = []
    for start in range(0, len(lines), 120):
        end = min(start + 120, len(lines))
        result.append(
            {
                "id": digest([artifact, api().digest(packet / artifact), start + 1, end]),
                "kind": "component-report",
                "path": artifact,
                "revision": "packet",
                "artifact": artifact,
                "start_line": start + 1,
                "end_line": end,
                "bytes": len("".join(lines[start:end]).encode("utf-8")),
                "links": [],
            }
        )
    return result


def prepare_unit(directory, batch, unit, reservation):
    parent = Path(directory)
    target = unit_path(parent, unit)
    if target.exists():
        raise WorkflowError("Unreserved unit directory exists; inspect ambiguous state")
    target.mkdir(parents=True)
    shutil.copytree(parent / "packet", target / "packet")
    packet = target / "packet"
    dependencies, extra = {}, []
    if unit["kind"] == "integration":
        (packet / "component-reports").mkdir()
        for component in batch["units"][:-1]:
            child = unit_path(parent, component)
            assessment = unit_assessment(parent, batch, component)
            if not assessment["complete"]:
                raise WorkflowError("Integration requires complete component dependencies")
            dependencies[component["id"]] = report_binding(child)
            body = (child / "review.md").read_bytes()
            artifact = f"component-reports/{component['id']}.txt"
            (packet / artifact).write_bytes(body)
            extra.extend(material_for_report(packet, artifact, body.decode("utf-8")))
        inventory = coverage.read_json(packet / "required-material.json")
        inventory["required"].extend(extra)
        atomic_json(packet / "required-material.json", inventory)
        atomic_text(packet / "inventory-sha256.txt", api().digest(packet / "required-material.json") + "\n")
    assignment = {
        "batch_sha256": digest(batch),
        "unit": unit,
        "required_ids": unit["required_ids"] + [item["id"] for item in extra],
        "dependencies": dependencies,
    }
    atomic_json(packet / "assignment.json", assignment)
    meta = api().verify_packet(parent)
    child_meta = {
        key: value for key, value in meta.items() if key not in api().RESULT_FIELDS | {"batch_sha256"}
    }
    child_meta.update(
        schema_version=3,
        batch_unit=assignment,
        files={p.relative_to(packet).as_posix(): api().digest(p) for p in packet.rglob("*") if p.is_file()},
        config={
            **meta["config"],
            "review_max_ai_credits": reservation["credits"],
            "review_timeout_seconds": reservation["seconds"],
        },
    )
    atomic_json(target / "metadata.json", child_meta)
    return target


def unit_assessment(directory, batch, unit):
    target = unit_path(directory, unit)
    meta = api().verify_packet(target)
    assignment = coverage.read_json(target / "packet/assignment.json")
    if (
        assignment != meta.get("batch_unit")
        or assignment.get("batch_sha256") != digest(batch)
        or assignment.get("unit") != unit
    ):
        raise WorkflowError("Unit assignment changed")
    if any(meta.get(key) != batch["binding"][key] for key in BINDING):
        raise WorkflowError("Unit snapshot or contract changed")
    # Every original artifact remains byte-identical, except the integration's
    # inventory which must contain every parent item plus exact report obligations.
    exceptions = (
        {"required-material.json", "inventory-sha256.txt"} if unit["kind"] == "integration" else set()
    )
    for name, checksum in batch["files"].items():
        if name not in exceptions and meta["files"].get(name) != checksum:
            raise WorkflowError("Unit lost original packet context")
    inventory = coverage.read_json(target / "packet/required-material.json")["required"]
    parent = coverage.read_json(Path(directory) / "packet/required-material.json")["required"]
    extras, dependencies = [], {}
    if unit["kind"] == "integration":
        for component in batch["units"][:-1]:
            child = unit_path(directory, component)
            if not unit_assessment(directory, batch, component)["complete"]:
                raise WorkflowError("Integration dependency incomplete")
            dependencies[component["id"]] = report_binding(child)
            artifact = f"component-reports/{component['id']}.txt"
            if (target / "packet" / artifact).read_bytes() != (child / "review.md").read_bytes():
                raise WorkflowError("Integration report input changed")
            extras.extend(
                material_for_report(
                    target / "packet", artifact, (child / "review.md").read_bytes().decode("utf-8")
                )
            )
    ids = unit["required_ids"] + [item["id"] for item in extras]
    if (
        inventory != parent + extras
        or assignment["dependencies"] != dependencies
        or assignment["required_ids"] != ids
    ):
        raise WorkflowError("Unit inventory or dependency binding changed")
    state = state_for(directory, batch)
    reservation = (
        next((row for row in state["reservations"] if row["unit"] == unit["id"]), None) if state else None
    )
    if not reservation or meta["config"] != {
        **batch["policy"],
        "review_max_ai_credits": reservation["credits"],
        "review_timeout_seconds": reservation["seconds"],
    }:
        raise WorkflowError("Unit policy differs from reserved limits")
    assessment = api().qualification(target)
    selected = [row for row in assessment["material"] if row["id"] in ids]
    complete = (
        not assessment["reasons"]
        and len(selected) == len(ids)
        and all(row["state"] == "reviewed" for row in selected)
    )
    return {"complete": complete, "material": selected, "reasons": assessment["reasons"]}


def state_for(directory, batch):
    path = plain_path(Path(directory) / "batch-state.json")
    if not path.exists():
        return None
    state = coverage.read_json(path)
    if (
        not isinstance(state, dict)
        or set(state) != {"schema_version", "batch_sha256", "started", "deadline", "reservations"}
        or state.get("batch_sha256") != digest(batch)
        or type(state.get("schema_version")) is not int
        or state["schema_version"] != 1
    ):
        raise WorkflowError("Batch execution state changed")
    reservations = state.get("reservations")
    if not isinstance(reservations, list) or len(reservations) > batch["budget"]["requests"]:
        raise WorkflowError("Invalid request accounting")
    if any(not isinstance(row, dict) or set(row) != {"unit", "credits", "seconds"} for row in reservations):
        raise WorkflowError("Malformed reservation record")
    ids = [row.get("unit") for row in reservations]
    if ids != [unit["id"] for unit in batch["units"][: len(ids)]]:
        raise WorkflowError("Ambiguous, duplicate or reordered reservations")
    start = bounded(state.get("started"), "start")
    if state.get("deadline") != start + batch["budget"]["seconds"]:
        raise WorkflowError("Batch deadline changed")
    for row in reservations:
        bounded(row.get("credits"), "reserved credits")
        bounded(row.get("seconds"), "reserved seconds")
        if (
            row.get("credits") != batch["budget"]["unit_credits"]
            or not 0 < row.get("seconds", 0) <= batch["budget"]["unit_seconds"]
        ):
            raise WorkflowError("Unit reservation changed")
    if sum(row["credits"] for row in reservations) > batch["budget"]["credits"]:
        raise WorkflowError("Reserved allocation exceeds budget")
    return state


def observed_credits(target):
    """Retain counters; nano-AIU is an AI-credit unit, never a currency quote."""
    diagnostics = coverage.read_json(target / "diagnostics.json")
    usage = diagnostics.get("usage", {})
    value = usage.get("counters", {}).get("totalNanoAiu")
    if type(value) not in {int, float} or not math.isfinite(value) or value < 0:
        return None
    return value / 1_000_000_000


@contextlib.contextmanager
def locked(directory):
    path = plain_path(Path(directory) / "batch.lock")
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "a") as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise WorkflowError("Batch lock is not a regular file")
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise WorkflowError("Another batch operation is active") from exc
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


def current_contract(repo, directory, meta):
    context = coverage.read_json(directory / "packet/context.json")
    issue = repo.api(f"issues/{meta['issue']}")
    approved = repo.api(f"issues/comments/{meta['plan_comment']}")
    if (
        issue.get("body") != context["issue"].get("body")
        or issue.get("title") != context["issue"].get("title")
        or issue.get("state") != "open"
    ) or approved.get("body") != context["designated_plan_comment"].get("body"):
        raise WorkflowError("Batch contract changed; continuation refused")


def execute(repo, directory, *, resume=False, recover_only=False, clock=time.time):
    """Reserve before dispatch; never retry any started or uncertain unit."""
    directory = plain_path(directory)
    with locked(directory):
        batch = load(directory)
        meta = api().verify_packet(directory)
        if repo.name != meta["repository"]:
            raise WorkflowError("Batch belongs to another repository")
        api().current_pr(repo, meta["pr"], meta["head_sha"], meta["base_sha"])
        current_contract(repo, directory, meta)
        state = state_for(directory, batch)
        if state is not None and not (resume or recover_only):
            raise WorkflowError("Batch already started; select explicit resume or recovery")
        if state is None:
            if recover_only:
                return finalize(directory)
            now = clock()
            state = {
                "schema_version": 1,
                "batch_sha256": digest(batch),
                "started": now,
                "deadline": now + batch["budget"]["seconds"],
                "reservations": [],
            }
            atomic_json(directory / "batch-state.json", state)
        for unit in batch["units"]:
            target = unit_path(directory, unit)
            reserved = next((row for row in state["reservations"] if row["unit"] == unit["id"]), None)
            if reserved:
                if not target.exists() or api().recover_review(repo, target) is None:
                    finalize(directory)
                    raise WorkflowError("Attempted unit has no recoverable report; no automatic retry")
                continue
            if recover_only:
                break
            # All prior calls count, even failed/uncertain ones. Unknown usage or
            # an incomplete dependency stops further spending, not just integration.
            spent = 0
            for previous in batch["units"][: len(state["reservations"])]:
                child = unit_path(directory, previous)
                if not unit_assessment(directory, batch, previous)["complete"]:
                    finalize(directory)
                    raise WorkflowError("Incomplete prior unit; further requests stopped")
                actual = observed_credits(child)
                if actual is None:
                    finalize(directory)
                    raise WorkflowError("Unknown AI-credit usage; further requests stopped")
                if actual > batch["budget"]["unit_credits"]:
                    finalize(directory)
                    raise WorkflowError("Per-unit soft credit allocation exceeded; further requests stopped")
                spent += actual
            limits = batch["budget"]
            now = clock()
            if now < state["started"]:
                raise WorkflowError("Clock moved before batch start; continuation is ambiguous")
            remaining = state["deadline"] - now
            allocated = sum(row["credits"] for row in state["reservations"])
            if (
                len(state["reservations"]) >= limits["requests"]
                or remaining <= 0
                or max(spent, allocated) + limits["unit_credits"] > limits["credits"]
            ):
                finalize(directory)
                raise WorkflowError("Aggregate request, credit or time allocation exhausted")
            api().current_pr(repo, meta["pr"], meta["head_sha"], meta["base_sha"])
            current_contract(repo, directory, meta)
            reservation = {
                "unit": unit["id"],
                "credits": limits["unit_credits"],
                "seconds": min(remaining, limits["unit_seconds"]),
            }
            state["reservations"].append(reservation)
            atomic_json(directory / "batch-state.json", state)
            try:
                target = prepare_unit(directory, batch, unit, reservation)
                api().review(repo, target, _batch_authorized=True, _batch_deadline=state["deadline"])
            finally:
                # Persist bookkeeping even on interruption. Exact unit capture is
                # owned by the existing single-request recovery journal.
                finalize(directory)
        return finalize(directory)


def assessment(directory):
    batch = load(directory)
    state = state_for(directory, batch)
    reserved = {row["unit"] for row in state["reservations"]} if state else set()
    rows, units, findings = {}, [], []
    reasons, usage = [], []
    for unit in batch["units"]:
        target = unit_path(directory, unit)
        if unit["id"] not in reserved:
            units.append({"id": unit["id"], "state": "never-started"})
            continue
        actual = None
        usage_path = target / "diagnostics.json"
        if usage_path.exists():
            diagnostics = coverage.read_json(usage_path)
            coverage.validate_diagnostics(diagnostics, target / "packet")
            projection = diagnostics["usage"]
            actual = observed_credits(target)
            usage.append({"unit": unit["id"], "usage": projection, "ai_credits": actual})
        else:
            usage.append({"unit": unit["id"], "usage": {"status": "unknown"}, "ai_credits": None})
        if actual is None:
            reasons.append("unknown_credit_usage")
        elif actual > batch["budget"]["unit_credits"]:
            reasons.append("unit_credit_allocation_exceeded")
        if not (target / "review.md").exists():
            units.append({"id": unit["id"], "state": "attempted-incomplete"})
            continue
        result = unit_assessment(directory, batch, unit)
        binding = report_binding(target)
        units.append(
            {"id": unit["id"], "state": "complete" if result["complete"] else "incomplete", **binding}
        )
        if result["reasons"]:
            reasons.append("unit_protocol_incomplete")
        for row in result["material"]:
            if row["id"] in unit["required_ids"] and not result["reasons"]:
                rows[row["id"]] = {**row, "unit": unit["id"]}
        if not result["reasons"]:
            document = coverage.report_document((target / "review.md").read_bytes().decode("utf-8"))
            findings.extend({"unit": unit["id"], "finding": finding} for finding in document["findings"])
    inventory = coverage.read_json(Path(directory) / "packet/required-material.json")["required"]
    material = [
        rows.get(
            item["id"],
            {
                "id": item["id"],
                "state": "unsupported" if item.get("omitted") else "unread",
                "evidence": [],
                "reason": item.get("omitted") or "assigned_unit_not_inspected",
                "location": {
                    key: item.get(key)
                    for key in ("artifact", "start_line", "end_line", "path", "revision", "kind")
                },
            },
        )
        for item in inventory
    ]
    if sum(row["ai_credits"] or 0 for row in usage) > batch["budget"]["credits"]:
        reasons.append("observed_credit_allocation_exceeded")
    complete = all(unit["state"] == "complete" for unit in units)
    return {
        "schema_version": SCHEMA,
        "qualified": complete and not reasons,
        "reasons": sorted(set(reasons)),
        "material": material,
        "units": units,
        "findings": findings,
        "required_count": len(material),
        "inspected_count": sum(row["state"] == "reviewed" for row in material),
        "budget": batch["budget"],
        "usage": usage,
        "reservations": state["reservations"] if state else [],
        "started": state["started"] if state else None,
        "deadline": state["deadline"] if state else None,
        "limit": "Attributed aggregate bookkeeping, not a model response. Observed reads do not prove understanding. AI-credit limits are soft; in-flight overshoot is possible.",
    }


def finalize(directory):
    result = assessment(directory)
    atomic_json(Path(directory) / "coverage.json", result)
    atomic_json(Path(directory) / "review.md", result)
    meta = api().verify_packet(directory)
    meta.update(review_sha256=api().digest(Path(directory) / "review.md"), copilot_version=api().CLI_VERSION)
    atomic_json(Path(directory) / "metadata.json", meta)
    return Path(directory) / "review.md"


def qualification(directory, require=False):
    result = assessment(directory)
    meta = api().verify_packet(directory)
    if (
        coverage.read_json(Path(directory) / "coverage.json") != result
        or coverage.read_json(Path(directory) / "review.md") != result
        or api().digest(Path(directory) / "review.md") != meta.get("review_sha256")
    ):
        raise WorkflowError("Aggregate bookkeeping changed or requires saved-result recovery")
    if require and not result["qualified"]:
        raise WorkflowError("Batch coverage incomplete; every component and integration is required")
    return result


def publication_body(directory):
    result = qualification(directory)
    meta = api().verify_packet(directory)
    label = "coverage-qualified" if result["qualified"] else "INCOMPLETE — not ready"
    # Exact component outputs are separate COMMENTs, never concatenated and
    # described as one independent model response.
    lines = [
        f"## Attributed batch bookkeeping: {label}",
        f"PR #{meta['pr']} · head `{meta['head_sha']}` · base `{meta['base_sha']}`",
        "Exact unit reports are published separately with their report hashes. This aggregate is coordinator bookkeeping, not model output or human approval. Static reviewers ran no tests. CI head association and tested checkout remain separate in validation.json.",
    ]
    for unit in result["units"]:
        lines.append(
            f"- {unit['id']}: {unit['state']}; exact report `{unit.get('review_sha256', 'unavailable')}`"
        )
    lines.extend(
        [
            f"Inspected required entries: {result['inspected_count']} of {result['required_count']}. Integration is separately required.",
            result["limit"],
            f"<!-- agentic-review:{meta['head_sha']}:{meta['review_sha256']} -->",
            f"<!-- agentic-batch:v1:{digest(result)} -->",
        ]
    )
    return "\n\n".join(lines)


def publish_units(repo, directory):
    batch = load(directory)
    result = qualification(directory)
    for unit, record in zip(batch["units"], result["units"], strict=True):
        if "review_sha256" in record:
            api().publish(repo, unit_path(directory, unit))


def verify_unit_publications(repo, directory, complete_only=True):
    batch = load(directory)
    records = qualification(directory)["units"]
    for unit, record in zip(batch["units"], records, strict=True):
        if complete_only or "review_sha256" in record:
            api().verify_publication(repo, unit_path(directory, unit))


def add_budget_arguments(parser):
    for name, kind in (
        ("requests", int),
        ("credits", float),
        ("seconds", float),
        ("unit-credits", float),
        ("unit-seconds", float),
    ):
        parser.add_argument("--batch-" + name, type=kind, required=True)


def arguments_budget(args):
    return {
        name: getattr(args, "batch_" + name)
        for name in ("requests", "credits", "seconds", "unit_credits", "unit_seconds")
    }
