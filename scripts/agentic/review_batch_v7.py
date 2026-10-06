"""Version-seven native reporting batches; version-six records never enter this evaluator."""

import contextlib
import fcntl
import os
import shutil
import stat
import time
import uuid
from pathlib import Path

import review_coverage as coverage
import review_navigation
import review_policy
from tasks import atomic_json, atomic_text, digest, plain_path
from workflow import WorkflowError

SCHEMA = 7
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
    import review

    return review


def plan(directory, *, version=7, continuation=True):
    """Deterministic scope partition with explicit linked navigation context."""
    if type(version) is not int or version != 7:
        raise WorkflowError("Unsupported batch plan version")
    directory = Path(directory)
    meta = api().verify_packet(directory)
    if meta["schema_version"] != 7 or meta.get("kind") not in {"single", "batch-parent"}:
        raise WorkflowError("Batch planning requires a current parent packet")
    if meta["review_policy"].get("schema_version") != 2:
        raise WorkflowError("V7 batches require the exact native reporting policy")
    packet = directory / "packet"
    inventory = coverage.read_json(packet / "required-material.json")["required"]
    lookup = {item["id"]: item for item in inventory}
    if not inventory or len(lookup) != len(inventory):
        raise WorkflowError("Invalid parent inventory")
    scopes = coverage.read_json(packet / "scopes.json")["scopes"]
    mappings = coverage.read_json(packet / "test-map.json")
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
            while True:
                previous = paths.copy()
                for mapping in mappings:
                    if mapping["changed_path"] in paths or paths.intersection(mapping["candidates"]):
                        paths.update([mapping["changed_path"], *mapping["candidates"]])
                if version == 1 or paths == previous:
                    break
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
    if seen != lookup.keys():
        raise WorkflowError("Parent scopes omit material")
    if not components:
        raise WorkflowError("Parent scopes contain no component material")
    if not integration:
        raise WorkflowError("Parent scopes omit the integration obligation")
    units = components + [
        {
            "id": "integration",
            "kind": "integration",
            "required_ids": integration,
            "context_ids": sorted(lookup.keys() - set(integration)),
            "depends_on": [unit["id"] for unit in components],
        }
    ]
    implementation_stems = {
        Path(item["path"]).stem
        for item in inventory
        if item["path"].startswith("scripts/agentic/") and item["path"].endswith(".py")
    }

    def family_stem(path):
        stem = Path(path).stem.removeprefix("test_")
        if path.startswith("tests/agentic/"):
            return max(
                (s for s in implementation_stems if stem == s or stem.startswith(s + "_")),
                key=len,
                default=stem,
            )
        return stem

    for unit in units:
        if version == 7:
            primary_paths = {lookup[key]["path"] for key in unit["required_ids"]}
            stems = {family_stem(path) for path in primary_paths}
            related_paths = {item["path"] for item in inventory if family_stem(item["path"]) in stems}
            for mapping in mappings:
                if family_stem(mapping["changed_path"]) in stems:
                    related_paths.update(p for p in mapping["candidates"] if family_stem(p) in stems)
            relevant_findings = set()
            for key, item in lookup.items():
                if item["kind"] == "finding" and item.get("artifact") and not item.get("omitted"):
                    text = (packet / item["artifact"]).read_bytes().decode("utf-8")
                    if any(path in text for path in related_paths):
                        relevant_findings.add(key)
            linked = {link for key in unit["required_ids"] for link in lookup[key].get("links", [])}
            related = {
                key for key, item in lookup.items() if item["path"] in related_paths or key in linked
            } | relevant_findings
            unit["context_ids"] = sorted(set(unit["context_ids"]) | (related - set(unit["required_ids"])))
            unit["navigation_ids"] = sorted(
                related - set(unit["required_ids"]),
                key=lambda key: (
                    lookup[key]["kind"],
                    lookup[key]["path"],
                    lookup[key].get("start_line", 0),
                    key,
                ),
            )
        for field in ("required_ids", "context_ids"):
            entries = [lookup[key] for key in unit[field]]
            unit[field.removesuffix("_ids") + "_volume"] = {
                "items": len(entries),
                "bytes": sum(item.get("bytes", 0) for item in entries),
                "lines": sum(
                    item["end_line"] - item["start_line"] + 1 for item in entries if not item.get("omitted")
                ),
            }
    result = {
        "schema_version": version,
        "binding": {key: meta[key] for key in BINDING},
        "files": meta["files"],
        "policy": meta["review_policy"],
        "contract_digest": contract_digest(meta, coverage.read_json(packet / "context.json")),
        "config": meta["config"],
        "inventory_sha256": api().digest(packet / "required-material.json"),
        "units": units,
        "resources": {
            **(
                {
                    "navigation": {
                        "version": 1,
                        "page_bytes": review_navigation.PAGE_BYTES,
                        "page_lines": review_navigation.PAGE_LINES,
                        "max_bytes_per_unit": review_navigation.MAX_NAVIGATION_BYTES,
                    }
                }
                if version == 7
                else {}
            ),
            "context_bytes": sum(item.get("bytes", 0) for item in inventory),
            "omissions": [item["id"] for item in inventory if item.get("omitted")],
            "report_materialization": "exact-utf8-ranges-v4",
            "integration_dependencies": [u["id"] for u in components],
        },
        "note": "Assignments are navigation, not inspection. Full original inventory and surrounding context remain required.",
    }

    if continuation:
        import review_continuation

        value = review_continuation.validate(directory, result)
        if value is not None:
            result["continuation"] = value
    return result


def work_units(batch):
    imports = batch.get("continuation", {}).get("snapshot", {}).get("imports", [])
    return [unit for unit in batch["units"] if unit["id"] not in imports]


def imported(directory, unit):
    import review_continuation

    value = review_continuation.manifest(directory)
    if value and unit["id"] in value["snapshot"]["imports"]:
        return plain_path(value["snapshot"]["ancestor"])
    return None


def unit_path(directory, unit):
    ancestor = imported(directory, unit)
    return plain_path((ancestor or Path(directory)) / "units" / unit["id"])


def report_binding(directory):
    meta = api().verify_packet(directory)
    return {
        key: meta[key]
        for key in (
            "review_sha256",
            "diagnostics_sha256",
            "coverage_sha256",
            "reporting_sha256",
            "terminal_sha256",
        )
    }


def material_for_report(packet, artifact, body, *, version=4):
    """Integration reads exact report bytes, including every line of findings."""
    lines = body.splitlines(keepends=True)
    result = []
    for start in range(0, len(lines), 120):
        end = min(start + 120, len(lines))
        result.append(
            {
                "id": digest([artifact, api().digest(packet / artifact), start + 1, end])[
                    : 24 if version >= 4 else 64
                ],
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


def inspection_suggestions(packet, items, *, version=3):
    """Navigation only; original IDs/ranges and evidence requirements never change."""
    result = []
    for item in items:
        row = {"id": item["id"]}
        if item.get("omitted") or not item.get("artifact"):
            result.append({**row, "state": "unavailable"})
            continue
        artifact = item["artifact"]
        lines = (packet / artifact).read_bytes().decode("utf-8").splitlines()
        start, end = item["start_line"], item["end_line"]
        if not coverage.valid_range(start, end, len(lines)):
            raise WorkflowError("Inspection suggestion requires a valid source range")
        expanded = end
        while expanded < len(lines) and not lines[expanded - 1].strip():
            expanded += 1
        row.update(path=artifact, view_range=[start, expanded])
        if not lines[expanded - 1].strip():
            # EOF has no following context anchor. Actual numbered grep results
            # can establish blank lines; an unnumbered empty result cannot.
            first = expanded
            while first > start and not lines[first - 2].strip():
                first -= 1
            if version >= 3:
                if first > start:
                    row["view_range"] = [start, first - 1]
                else:
                    row.pop("view_range")
            row["grep_blank_lines"] = [first, expanded]
            row["grep_pattern"] = r"^\s*$"
        result.append(row)
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
            extra.extend(
                material_for_report(packet, artifact, body.decode("utf-8"), version=batch["schema_version"])
            )
        inventory = coverage.read_json(packet / "required-material.json")
        inventory["required"].extend(extra)
        atomic_json(packet / "required-material.json", inventory)
        atomic_text(packet / "inventory-sha256.txt", api().digest(packet / "required-material.json") + "\n")
    if any(
        len((packet / f"component-reports/{key}.txt").read_bytes()) > batch["budget"]["max_report_bytes"]
        for key in dependencies
    ):
        raise WorkflowError("Component report exceeds authorized output bound")
    if sum(item["bytes"] for item in extra) > batch["budget"]["max_integration_bytes"]:
        raise WorkflowError("Integration report material exceeds authorized bound")
    assignment = {
        "batch_sha256": digest(batch),
        "unit": unit,
        "required_ids": unit["required_ids"] + [item["id"] for item in extra],
        "dependencies": dependencies,
        "policy_digest": digest(batch["unit_policy"]),
        "authorization_digest": digest(batch["authorization"]),
    }
    if batch["schema_version"] >= 4:
        assignment["publication_version"] = 2
    if batch["schema_version"] >= 2:
        required = set(assignment["required_ids"])
        assignment["inspection_suggestions"] = inspection_suggestions(
            packet,
            [
                item
                for item in coverage.read_json(packet / "required-material.json")["required"]
                if item["id"] in required
            ],
            version=batch["schema_version"],
        )
    if batch["schema_version"] == 7:
        assignment["max_report_bytes"] = batch["budget"]["max_report_bytes"]
        assignment["navigation"] = review_navigation.materialize(
            packet, {**unit, "required_ids": assignment["required_ids"]}, assignment["max_report_bytes"]
        )
    if unit["kind"] == "integration":
        assignment["integration_capacity"] = batch["authorization"]["integration_capacity"]
    atomic_json(packet / "assignment.json", assignment)
    meta = api().verify_packet(parent)
    child_meta = {
        key: value for key, value in meta.items() if key not in api().RESULT_FIELDS | {"batch_sha256"}
    }
    child_meta.update(
        schema_version=7,
        kind="batch-unit",
        review_policy=batch["unit_policy"],
        reservation_digest=digest(reservation["binding"]),
        claim_digest=digest(reservation["claim"]),
        authorization_digest=digest(batch["authorization"]),
        batch_unit=assignment,
        files={p.relative_to(packet).as_posix(): api().digest(p) for p in packet.rglob("*") if p.is_file()},
        config=meta["config"],
    )
    atomic_json(target / "metadata.json", child_meta)
    return target


def unit_assessment(directory, batch, unit):
    ancestor = imported(directory, unit)
    if ancestor:
        import review_continuation

        review_continuation.validate(directory)
        return unit_assessment(ancestor, load(ancestor), unit)
    target = unit_path(directory, unit)
    meta = api().verify_packet(target)
    assignment = coverage.read_json(target / "packet/assignment.json")
    if (
        assignment != meta.get("batch_unit")
        or assignment.get("batch_sha256") != digest(batch)
        or assignment.get("unit") != unit
    ):
        raise WorkflowError("Unit assignment changed")
    if meta.get("config") != batch["config"] or any(
        meta.get(key) != batch["binding"][key] for key in BINDING
    ):
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
                    target / "packet",
                    artifact,
                    (child / "review.md").read_bytes().decode("utf-8"),
                    version=batch["schema_version"],
                )
            )
    ids = unit["required_ids"] + [item["id"] for item in extras]
    if (
        assignment.get("publication_version") != (2 if batch["schema_version"] >= 4 else None)
        or inventory != parent + extras
        or assignment["dependencies"] != dependencies
        or assignment["required_ids"] != ids
    ):
        raise WorkflowError("Unit inventory or dependency binding changed")
    if batch["schema_version"] >= 2 and assignment.get("inspection_suggestions") != inspection_suggestions(
        target / "packet",
        [item for item in inventory if item["id"] in set(ids)],
        version=batch["schema_version"],
    ):
        raise WorkflowError("Unit inspection suggestions changed")
    validate_navigation(target, batch, unit, assignment)
    state = state_for(directory, batch)
    reservation = (
        next((row for row in state["reservations"] if row["binding"]["unit"] == unit["id"]), None)
        if state
        else None
    )
    if (
        not reservation
        or meta["review_policy"] != batch["unit_policy"]
        or meta.get("reservation_digest") != digest(reservation["binding"])
        or meta.get("claim_digest") != digest(reservation.get("claim"))
        or meta.get("authorization_digest") != digest(batch["authorization"])
        or assignment.get("policy_digest") != digest(batch["unit_policy"])
        or assignment.get("authorization_digest") != digest(batch["authorization"])
        or reservation.get("assignment_digest") != digest(assignment)
    ):
        raise WorkflowError("Unit policy or reservation binding changed")
    import review_claims

    claim = reservation.get("claim")
    if type(claim) is not dict or digest(claim) != digest(
        review_claims.record(batch, unit, reservation["binding"], claim.get("previous_digest"))
    ):
        raise WorkflowError("Unit lost its exact global material claim")
    assessment = api().qualification(target)
    from claude_reporting_execution import retained
    from reporting_activation import read
    from reporting_admission import FILENAME

    admission = read(target / FILENAME)
    execution = retained(target, meta)
    if execution is None or execution.get("admission_digest") != digest(admission):
        raise WorkflowError("Batch unit lacks its admitted native execution")
    if (target / "review.md").stat().st_size > batch["budget"]["max_report_bytes"]:
        raise WorkflowError("Unit report exceeds authorized output bound")
    if unit["kind"] == "integration":
        import review_capacity

        try:
            review_capacity.actual(target, batch)
            review_capacity.observed(target, batch)
        except review_capacity.IntegrationCapacityExceeded:
            assessment = {
                **assessment,
                "reasons": [*assessment["reasons"], "integration_input_capacity_exceeded"],
            }
    selected = [row for row in assessment["material"] if row["id"] in ids]
    complete = (
        not assessment["reasons"]
        and len(selected) == len(ids)
        and all(row["state"] == "reviewed" for row in selected)
    )
    return {"complete": complete, "material": selected, "reasons": assessment["reasons"]}


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
    continuation = load(directory).get("continuation")
    for unit in result["units"]:
        receipt = (continuation or {}).get("publications", {}).get(unit["id"])
        origin = (
            f"imported exact COMMENT https://github.com/{meta['repository']}/pull/{meta['pr']}#pullrequestreview-{receipt['review_id']}"
            if receipt
            else "new allocation"
        )
        lines.append(
            f"- {unit['id']}: {unit['state']}; {origin}; exact report `{unit.get('review_sha256', 'unavailable')}`"
        )
    if continuation:
        lines.append(
            f"Prior consumed reservations: {continuation['snapshot']['consumed']['reservations']}; "
            f"new reservations: {len(result['reservations'])}. Prior stops and usage remain in the bound ancestor; "
            "imported observations are counted only in prior consumption, never refunded."
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
        if imported(directory, unit):
            api().verify_publication(repo, unit_path(directory, unit))
            continue
        if "review_sha256" in record:
            api().publish(repo, unit_path(directory, unit))


def verify_unit_publications(repo, directory, complete_only=True):
    batch = load(directory)
    records = qualification(directory)["units"]
    for unit, record in zip(batch["units"], records, strict=True):
        if complete_only or "review_sha256" in record:
            api().verify_publication(repo, unit_path(directory, unit))


def select(directory, limits, authorization=None):
    directory = plain_path(directory)
    meta = api().verify_packet(directory)
    preview = plan(
        directory, version=load(directory)["schema_version"] if meta.get("kind") == "batch-parent" else 7
    )
    limits, policy = review_policy.batch_budget(limits, meta["review_policy"], len(work_units(preview)))
    validate_capacity(preview, limits, policy)
    import review_capacity

    executable = {
        **preview,
        "budget": limits,
        "unit_policy": policy,
        "integration_capacity_estimate": review_capacity.estimate(directory, preview, limits),
    }
    if meta.get("kind") == "batch-parent":
        existing = load(directory)
        if {k: v for k, v in existing.items() if k != "authorization"} != executable:
            raise WorkflowError("Batch selection or budget changed")
        if authorization is not None and existing["authorization"] != authorization:
            raise WorkflowError("Batch authorization changed")
        return existing
    if any(
        (directory / n).exists()
        for n in ("attempt.json", "review-capture.json", "review-result.json", "review.md")
    ):
        raise WorkflowError("Attempted single review cannot become a batch")
    validate_authorization(authorization, executable)
    if executable.get("continuation"):
        old = load(executable["continuation"]["snapshot"]["ancestor"])
        if any(authorization[k] != old["authorization"][k] for k in ("harness_commit", "harness_files")):
            raise WorkflowError("Continuation harness changed")
        if authorization["name"] == old["authorization"]["name"]:
            raise WorkflowError("Successor requires a separately named finite grant")
    record = {**executable, "authorization": authorization}
    from claude_reporting_execution import exclusive

    exclusive(directory / "batch.json", record, limit=8_000_000)
    meta.update(kind="batch-parent", batch_sha256=digest(record))
    atomic_json(directory / "metadata.json", meta)
    return record


def validate_authorization(auth, executable):
    keys = {"name", "preview_digest", "harness_commit", "harness_files", "expires_at", "integration_capacity"}
    if (
        not isinstance(auth, dict)
        or set(auth) != keys
        or not isinstance(auth["name"], str)
        or not auth["name"].strip()
        or auth["preview_digest"] != digest(executable)
    ):
        raise WorkflowError("Named finite authorization must bind the executable preview")
    import review_capacity

    review_capacity.validate(auth, executable)
    from workflow import sha

    sha(auth["harness_commit"])
    if type(auth["expires_at"]) not in {int, float}:
        raise WorkflowError("Authorization expiry must be a finite timestamp")
    review_policy.exact_amount(auth["expires_at"], "authorization expiry")
    if not isinstance(auth["harness_files"], dict) or not auth["harness_files"]:
        raise WorkflowError("Exact tested harness hashes are required")
    for name, value in auth["harness_files"].items():
        if not isinstance(name, str) or not isinstance(value, str) or len(value) != 64:
            raise WorkflowError("Invalid harness binding")


def load(directory):
    directory = plain_path(directory)
    meta = api().verify_packet(directory)
    saved = coverage.read_json(directory / "batch.json")
    if meta.get("kind") != "batch-parent" or digest(saved) != meta.get("batch_sha256"):
        raise WorkflowError("Batch binding changed")
    limits, policy = review_policy.batch_budget(
        saved["budget"], meta["review_policy"], len(work_units(saved))
    )
    planned = plan(directory, version=saved["schema_version"])
    validate_capacity(planned, limits, policy)
    import review_capacity

    expected = {
        **planned,
        "budget": limits,
        "unit_policy": policy,
        "integration_capacity_estimate": review_capacity.estimate(directory, planned, limits),
    }
    validate_authorization(saved["authorization"], expected)
    if saved != {**expected, "authorization": saved["authorization"]}:
        raise WorkflowError("Batch plan, provider or allocation changed")
    return saved


def state_for(directory, batch):
    path = Path(directory) / "batch-state.json"
    if not path.exists():
        return None
    state = coverage.read_json(path)
    if (
        not isinstance(state, dict)
        or set(state)
        != {
            "schema_version",
            "batch_sha256",
            "authorization_digest",
            "started",
            "deadline",
            "last_clock",
            "reservations",
            "stop_reason",
        }
        or type(state.get("schema_version")) is not int
        or state.get("schema_version") != 3
        or state.get("batch_sha256") != digest(batch)
        or state.get("authorization_digest") != digest(batch["authorization"])
    ):
        raise WorkflowError("Batch ledger binding changed")
    for key in ("started", "deadline", "last_clock"):
        review_policy.exact_amount(state.get(key), key, positive=False)
    if (
        state["deadline"]
        != min(state["started"] + batch["budget"]["seconds"], batch["authorization"]["expires_at"])
        or not state["started"] <= state["last_clock"] <= state["deadline"]
    ):
        raise WorkflowError("Batch clock or deadline changed")
    if state["stop_reason"] not in {None, "execution_incomplete_or_interrupted"}:
        raise WorkflowError("Invalid durable stop state")
    rows = state.get("reservations")
    if not isinstance(rows, list) or len(rows) > min(batch["budget"]["requests"], len(work_units(batch))):
        raise WorkflowError("Invalid batch reservations")
    for index, row in enumerate(rows):
        if not isinstance(row, dict) or row.get("status") not in {
            "reserved/uncertain",
            "captured",
            "assessed-complete",
            "assessed-incomplete",
        }:
            raise WorkflowError("Invalid reservation state")
        binding = row.get("binding", {})
        if (
            not isinstance(binding, dict)
            or set(binding)
            != {
                "unit",
                "slot",
                "dispatch_id",
                "policy_digest",
                "authorization_digest",
                "batch_sha256",
                "allocation",
            }
            or type(binding.get("slot")) is not int
            or binding.get("unit") != work_units(batch)[index]["id"]
            or binding.get("slot") != index + 1
            or binding.get("policy_digest") != digest(batch["unit_policy"])
            or binding.get("authorization_digest") != digest(batch["authorization"])
            or binding.get("batch_sha256") != digest(batch)
            or binding.get("allocation")
            != {"cost": batch["budget"]["unit_cost"], "seconds": batch["budget"]["unit_seconds"]}
        ):
            raise WorkflowError("Reserved identity or allocation changed")
        try:
            uuid.UUID(binding["dispatch_id"])
        except (ValueError, KeyError, TypeError, AttributeError):
            raise WorkflowError("Invalid dispatch identity") from None
    return state


def observed_usage(target, policy):
    path = Path(target) / "diagnostics.json"
    if not path.exists():
        return None
    diagnostics = coverage.read_json(path)
    coverage.validate_diagnostics(diagnostics, Path(target) / "packet", policy)
    usage = diagnostics["usage"]
    if usage.get("status") != "observed":
        return None
    counters = usage.get("counters", {})
    field = "estimated_usd" if policy["provider"] == "claude-code" else "totalNanoAiu"
    if field not in counters:
        return None
    for key, value in counters.items():
        if (key == "totalNanoAiu" or key.endswith("_tokens") or key in {"num_turns", "requests"}) and (
            type(value) is not int or value < 0
        ):
            raise WorkflowError("Invalid integer provider counter")
    amount = review_policy.exact_amount(counters[field], "observed usage", positive=False)
    if field == "totalNanoAiu":
        # Decimal division otherwise obeys the process's finite precision context.
        from decimal import Decimal

        parts = amount.as_tuple()
        amount = Decimal((parts.sign, parts.digits, parts.exponent - 9))
    return amount


def captured(directory, meta):
    """Record durable capture before assessment; recovery never reclaims its slot."""
    parent = Path(directory).parent.parent
    batch = load(parent)
    state = state_for(parent, batch)
    if not state or not state["reservations"]:
        raise WorkflowError("Captured child has no reservation")
    row = next(
        (r for r in state["reservations"] if digest(r["binding"]) == meta.get("reservation_digest")),
        None,
    )
    if (
        row is None
        or unit_path(parent, work_units(batch)[row["binding"]["slot"] - 1]) != Path(directory)
        or row.get("assignment_digest") != digest(meta.get("batch_unit"))
    ):
        raise WorkflowError("Capture reservation changed")
    if row["status"] == "reserved/uncertain":
        row["status"] = "captured"
        atomic_json(parent / "batch-state.json", state)


def dispatch_timeout(repo, directory, meta, context, *, clock=time.time, owned_auth=None):
    """Called again inside the adapter, after preflight and immediately before spawn."""
    if meta.get("kind") != "batch-unit":
        if context is not None:
            raise WorkflowError("Single review received batch dispatch context")
        if owned_auth is not None:
            from claude_owned_auth import require

            require(owned_auth).recheck()
        return meta["review_policy"]["budget"]["timeout_seconds"]
    if not isinstance(context, dict) or set(context) != {"parent", "dispatch_id"}:
        raise WorkflowError("Batch child requires a validated reservation context")
    parent = plain_path(context["parent"])
    batch = load(parent)
    state = state_for(parent, batch)
    if not state or state["stop_reason"] is not None:
        raise WorkflowError("Batch is stopped or unreserved")
    row = state["reservations"][-1]
    unit = work_units(batch)[row["binding"]["slot"] - 1]
    if (
        unit_path(parent, unit) != Path(directory)
        or row["binding"]["dispatch_id"] != context["dispatch_id"]
        or row["status"] != "reserved/uncertain"
        or meta.get("reservation_digest") != digest(row["binding"])
        or meta.get("batch_unit") != coverage.read_json(Path(directory) / "packet/assignment.json")
        or digest(meta["batch_unit"]) != row.get("assignment_digest")
        or meta["review_policy"] != batch["unit_policy"]
    ):
        raise WorkflowError("Dispatch context or child binding changed")
    import review_claims

    review_claims.verify(repo, batch, unit, row)
    import review_continuation

    review_continuation.verify_live(
        repo, parent, batch, **({"owned_auth": owned_auth} if owned_auth is not None else {})
    )
    review_continuation.owner(repo, parent, batch)
    api().verify_packet(directory)
    validate_navigation(directory, batch, unit, meta["batch_unit"])
    if unit["kind"] == "integration":
        import review_capacity

        review_capacity.actual(directory, batch)
    api().current_pr(repo, meta["pr"], meta["head_sha"], meta["base_sha"])
    current_contract(repo, parent, meta)
    for prior in work_units(batch)[: row["binding"]["slot"] - 1]:
        if not unit_assessment(parent, batch, prior)["complete"]:
            raise WorkflowError("Prior component is incomplete")
        use = observed_usage(unit_path(parent, prior), batch["unit_policy"])
        if use is None or use > review_policy.exact_amount(batch["budget"]["unit_cost"], "unit cost"):
            raise WorkflowError("Prior usage is unknown or exceeds allocation")
    now = clock()
    if now < state["last_clock"] or now >= state["deadline"]:
        raise WorkflowError("Batch clock rollback or deadline expiry")
    verify_harness(batch["authorization"])
    ready = clock()
    if ready < now or ready >= state["deadline"]:
        raise WorkflowError("Batch clock rollback or deadline expiry")
    if owned_auth is not None:
        from claude_owned_auth import require

        if (
            require(owned_auth).current_binding(batch["budget"]["unit_seconds"], state["deadline"] - ready)
            != batch["policy"]["authentication"]
        ):
            raise WorkflowError("Batch authentication changed before launch")
        checked = clock()
        if checked < ready or checked >= state["deadline"]:
            raise WorkflowError("Batch clock rollback or deadline expiry")
        ready = checked
    now = ready
    effective = min(row["binding"]["allocation"]["seconds"], state["deadline"] - now)
    state["last_clock"] = now
    row["effective_timeout"] = effective
    atomic_json(parent / "batch-state.json", state)
    return effective


def execute(repo, directory, *, resume=False, recover_only=False, clock=time.time):
    directory = plain_path(directory)
    if api().verify_packet(directory)["schema_version"] == 4:
        from review_batch_v4 import execute as historical_execute

        return historical_execute(repo, directory, resume=resume, recover_only=recover_only)
    with locked(directory):
        batch = load(directory)
        state = state_for(directory, batch)
        if not recover_only:
            meta = api().verify_packet(directory)
            api().current_pr(repo, meta["pr"], meta["head_sha"], meta["base_sha"])
            current_contract(repo, directory, meta)
            from review_lifetime import current_binding

            required_window = batch["budget"]["seconds"] if state is None else state["deadline"] - clock()
            if (
                current_binding(batch["budget"]["unit_seconds"], required_window)
                != batch["policy"]["authentication"]
            ):
                raise WorkflowError("Batch authentication generation or remaining lifetime differs")
            import review_continuation

            review_continuation.verify_live(repo, directory, batch)
            if state:
                review_continuation.owner(repo, directory, batch)
        if recover_only:
            saved_meta = api().verify_packet(directory)
            if saved_meta.get("review_sha256"):
                if api().digest(directory / "review.md") != saved_meta["review_sha256"] or coverage.read_json(
                    directory / "review.md"
                ) != coverage.read_json(directory / "coverage.json"):
                    raise WorkflowError("Completed aggregate bytes changed")
            if state:
                for unit in work_units(batch)[: len(state["reservations"])]:
                    target = unit_path(directory, unit)
                    if target.exists():
                        api().recover_review(repo, target)
            meta = api().verify_packet(directory)
            if meta.get("review_sha256") and all(
                row.get("status") == "assessed-complete" for row in (state or {}).get("reservations", [])
            ):
                qualification(directory)
                return directory / "review.md"
            return finalize(directory)
        if state and (not resume or state["stop_reason"] is not None):
            raise WorkflowError("Batch already started or durably stopped; no replay")
        if not state:
            if (directory / "units").exists() or (directory / "review.md").exists():
                raise WorkflowError("Missing ledger cannot reset existing execution")
            now = clock()
            if (
                now + len(work_units(batch)) * batch["budget"]["unit_seconds"]
                > batch["authorization"]["expires_at"]
            ):
                raise WorkflowError("Authorization cannot fund the full remaining time allocation")
            state = {
                "schema_version": 3,
                "batch_sha256": digest(batch),
                "authorization_digest": digest(batch["authorization"]),
                "started": now,
                "deadline": min(now + batch["budget"]["seconds"], batch["authorization"]["expires_at"]),
                "last_clock": now,
                "reservations": [],
                "stop_reason": None,
            }
        if not (directory / "batch-state.json").exists():
            # The exclusive owner is consumed before the first ledger write.
            # A crash in between cannot obtain another successor allocation.
            import review_continuation

            review_continuation.owner(repo, directory, batch, create=True)
        try:
            for index, unit in enumerate(work_units(batch)):
                target = unit_path(directory, unit)
                if index < len(state["reservations"]):
                    if not target.exists() or api().recover_review(repo, target) is None:
                        raise WorkflowError("Uncertain reservation cannot be replayed")
                else:
                    now = clock()
                    if now < state["last_clock"] or now >= state["deadline"]:
                        raise WorkflowError("Batch clock rollback or expired authorization")
                    state["last_clock"] = now
                    meta = api().verify_packet(directory)
                    api().current_pr(repo, meta["pr"], meta["head_sha"], meta["base_sha"])
                    current_contract(repo, directory, meta)
                    row = {
                        "binding": {
                            "unit": unit["id"],
                            "slot": index + 1,
                            "dispatch_id": str(uuid.uuid4()),
                            "batch_sha256": digest(batch),
                            "policy_digest": digest(batch["unit_policy"]),
                            "authorization_digest": digest(batch["authorization"]),
                            "allocation": {
                                "cost": batch["budget"]["unit_cost"],
                                "seconds": batch["budget"]["unit_seconds"],
                            },
                        },
                        "status": "reserved/uncertain",
                        "assignment_digest": None,
                    }
                    state["reservations"].append(row)
                    atomic_json(directory / "batch-state.json", state)
                    import review_claims

                    row["claim"] = review_claims.reserve(
                        repo,
                        batch,
                        unit,
                        row["binding"],
                        successor=directory if batch.get("continuation") else None,
                    )
                    atomic_json(directory / "batch-state.json", state)
                    target = prepare_unit(directory, batch, unit, row)
                    row["assignment_digest"] = digest(api().verify_packet(target)["batch_unit"])
                    atomic_json(directory / "batch-state.json", state)
                    context = {"parent": str(directory), "dispatch_id": row["binding"]["dispatch_id"]}
                    api().review(repo, target, dispatch_context=context)
                    state = state_for(directory, batch)
                now = clock()
                if now < state["last_clock"] or now > state["deadline"]:
                    raise WorkflowError("Batch clock rollback or deadline exceeded during completion")
                state["last_clock"] = now
                result = unit_assessment(directory, batch, unit)
                row = state["reservations"][index]
                row["status"] = "assessed-complete" if result["complete"] else "assessed-incomplete"
                atomic_json(directory / "batch-state.json", state)
                use = observed_usage(target, batch["unit_policy"])
                if (
                    not result["complete"]
                    or use is None
                    or use > review_policy.exact_amount(batch["budget"]["unit_cost"], "unit cost")
                ):
                    raise WorkflowError("Incomplete unit or unknown/over-allocation usage; dispatch stopped")
            return finalize(directory)
        except BaseException as exc:
            # The capture writer may have advanced the durable ledger before an
            # assessment/storage failure. Never overwrite that state with a stale copy.
            state = state_for(directory, batch) or state
            state["stop_reason"] = state.get("stop_reason") or "execution_incomplete_or_interrupted"
            atomic_json(directory / "batch-state.json", state)
            finalize(directory)
            raise exc


def assessment(directory):
    batch = load(directory)
    state = state_for(directory, batch)
    selected, units, findings, usage, reasons = {}, [], [], [], []
    reservations = state["reservations"] if state else []
    for unit in batch["units"]:
        target = unit_path(directory, unit)
        if not imported(directory, unit) and unit["id"] not in {r["binding"]["unit"] for r in reservations}:
            units.append({"id": unit["id"], "state": "never-started"})
            continue
        actual = observed_usage(
            target,
            api().verify_packet(target)["review_policy"]
            if imported(directory, unit)
            else batch["unit_policy"],
        )
        usage.append(
            {
                "unit": unit["id"],
                "origin": "imported" if imported(directory, unit) else "new",
                "kind": batch["budget"]["kind"],
                "amount": str(actual) if actual is not None else None,
            }
        )
        if actual is None or actual > review_policy.exact_amount(batch["budget"]["unit_cost"], "unit cost"):
            reasons.append("unknown_or_exceeded_unit_usage")
        if not (target / "review.md").exists():
            units.append({"id": unit["id"], "state": "reserved/uncertain"})
            continue
        result = unit_assessment(directory, batch, unit)
        units.append(
            {
                "id": unit["id"],
                "state": "complete" if result["complete"] else "incomplete",
                **report_binding(target),
            }
        )
        if result["reasons"]:
            reasons.append("unit_protocol_incomplete")
        else:
            selected.update(
                {
                    row["id"]: {**row, "unit": unit["id"]}
                    for row in result["material"]
                    if row["id"] in unit["required_ids"]
                }
            )
        # A valid finding is still publishable when evidence/capability is incomplete.
        # Never turn a protocol failure into deletion of the model's finding obligations.
        try:
            report = coverage.report_document((target / "review.md").read_bytes().decode("utf-8"))
        except (WorkflowError, ValueError):
            report = None
        if isinstance(report, dict) and "malformed_report_contract" not in result["reasons"]:
            findings.extend({"unit": unit["id"], "finding": f} for f in report["findings"])
    inventory = coverage.read_json(Path(directory) / "packet/required-material.json")["required"]
    material = [
        selected.get(
            item["id"],
            {
                "id": item["id"],
                "state": "unsupported" if item.get("omitted") else "unread",
                "reason": item.get("omitted") or "assigned_unit_not_inspected",
                "evidence": [],
                "location": {
                    key: item.get(key)
                    for key in ("artifact", "start_line", "end_line", "path", "revision", "kind")
                },
            },
        )
        for item in inventory
    ]
    if state and state["stop_reason"]:
        reasons.append(state["stop_reason"])
    return {
        "schema_version": 6,
        "qualified": not reasons and all(u["state"] == "complete" for u in units),
        "reasons": sorted(set(reasons)),
        "material": material,
        "units": units,
        "findings": findings,
        "required_count": len(material),
        "inspected_count": sum(m["state"] == "reviewed" for m in material),
        "policy_digest": digest(batch["policy"]),
        "batch_sha256": digest(batch),
        "authorization_digest": digest(batch["authorization"]),
        "budget": batch["budget"],
        "usage": usage,
        "prior_consumption": batch.get("continuation", {}).get("snapshot", {}).get("consumed"),
        "combined_consumption": {
            "wrapper_reservations": len(reservations)
            + batch.get("continuation", {}).get("snapshot", {}).get("consumed", {}).get("reservations", 0),
            "prior": batch.get("continuation", {}).get("snapshot", {}).get("consumed", {}).get("usage", []),
            "new": [row for row in usage if row["origin"] == "new"],
            "note": "Imported observations occur only in prior consumption; unknown new usage is not zero or refundable.",
        },
        "reservations": reservations,
        "started": state["started"] if state else None,
        "deadline": state["deadline"] if state else None,
        "limit": "Attributed bookkeeping, not model output. Provider ceilings are soft; reference cost is not billing. Observed reads do not prove understanding.",
    }


def finalize(directory):
    result = assessment(directory)
    atomic_json(Path(directory) / "coverage.json", result)
    atomic_json(Path(directory) / "review.md", result)
    meta = api().verify_packet(directory)
    meta.update(review_sha256=api().digest(Path(directory) / "review.md"))
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
        raise WorkflowError("Aggregate changed or requires saved-result recovery")
    if require:
        if not result["qualified"]:
            raise WorkflowError("Batch coverage incomplete; every component and integration required")
        review_policy.require_current_adapter(meta["review_policy"])
        if meta["review_policy"]["provider"] == "claude-code":
            from claude_native_auth import validate_binding

            validate_binding(meta["review_policy"].get("authentication"))
        from reporting_admission import require_packet

        for unit in load(directory)["units"]:
            target = unit_path(directory, unit)
            if imported(directory, unit):
                from review_continuation import require_import
                from workflow import Repo

                require_import(Repo(directory), directory, load(directory), unit)
            else:
                require_packet(target, api().verify_packet(target))
            import review_claims
            from workflow import Repo

            batch = load(directory)
            state = state_for(directory, batch)
            ancestor = imported(directory, unit)
            origin = load(ancestor) if ancestor else batch
            ledger = state_for(ancestor, origin) if ancestor else state
            row = next(row for row in ledger["reservations"] if row["binding"]["unit"] == unit["id"])
            review_claims.verify(Repo(directory), origin, unit, row)
        import review_continuation

        review_continuation.verify_live(Repo(directory), directory, batch)
        review_continuation.owner(Repo(directory), directory, batch)
    return result


def add_budget_arguments(parser, *, required=True, authorization=True):
    for name, kind in (
        ("requests", int),
        ("kind", str),
        ("cost", str),
        ("seconds", int),
        ("unit-cost", str),
        ("unit-seconds", int),
        ("max-report-bytes", int),
        ("max-integration-bytes", int),
    ):
        parser.add_argument("--batch-" + name, type=kind, required=required)
    if authorization:
        parser.add_argument("--batch-authorization", required=required)


def arguments_budget(args):
    return {
        name: getattr(args, "batch_" + name)
        for name in (
            "requests",
            "kind",
            "cost",
            "seconds",
            "unit_cost",
            "unit_seconds",
            "max_report_bytes",
            "max_integration_bytes",
        )
    }


def contract_digest(meta, context):
    issue, plan = context["issue"], context["designated_plan_comment"]
    return digest(
        {
            "issue": meta["issue"],
            "plan_comment": meta["plan_comment"],
            "issue_digest": digest({"title": issue["title"], "body": issue.get("body") or ""}),
            "plan_digest": digest({"id": meta["plan_comment"], "body": plan.get("body") or ""}),
        }
    )


def preview(directory, limits=None):
    planned = plan(directory)
    if limits is None:
        return planned
    limits, policy = review_policy.batch_budget(limits, planned["policy"], len(work_units(planned)))
    validate_capacity(planned, limits, policy)
    import review_capacity

    return {
        **planned,
        "budget": limits,
        "unit_policy": policy,
        "integration_capacity_estimate": review_capacity.estimate(directory, planned, limits),
    }


def verify_harness(authorization):
    """Bind the actual imported harness, not an unrelated control checkout."""
    from workflow import run

    root = Path(__file__).resolve().parents[2]
    actual_commit = run(["git", "-C", root, "rev-parse", "HEAD"]).stdout.strip()
    files = {
        str(p.relative_to(root)): api().digest(plain_path(p)) for p in (root / "scripts/agentic").glob("*.py")
    }
    if actual_commit != authorization["harness_commit"] or files != authorization["harness_files"]:
        raise WorkflowError("Authorized harness commit or complete module hashes changed")


def validate_navigation(directory, batch, unit, assignment):
    expected = batch["authorization"]["integration_capacity"] if unit["kind"] == "integration" else None
    if assignment.get("integration_capacity") != expected:
        raise WorkflowError("Integration input allocation changed")
    if batch["schema_version"] == 7:
        limit = batch["budget"]["max_report_bytes"]
        if assignment.get("max_report_bytes") != limit:
            raise WorkflowError("Unit report bound changed")
        review_navigation.validate(
            Path(directory) / "packet",
            {**unit, "required_ids": assignment["required_ids"]},
            limit,
            assignment.get("navigation"),
        )


def validate_capacity(planned, limits, policy):
    if planned.get("continuation"):
        old = load(planned["continuation"]["snapshot"]["ancestor"])
        if any(
            limits[k] != old["budget"][k]
            for k in ("kind", "unit_cost", "unit_seconds", "max_report_bytes", "max_integration_bytes")
        ):
            raise WorkflowError("Continuation changes original per-unit/report constraints")
    if limits["max_report_bytes"] != policy["reporting"]["limits"]["report_bytes"]:
        raise WorkflowError("Batch report limit differs from qualified reporting policy")
    if (len(planned["units"]) - 1) * limits["max_report_bytes"] > limits["max_integration_bytes"]:
        raise WorkflowError("Integration allocation cannot retain every worst-case exact report")
