"""Deterministic public capacity fixtures and numeric evidence, never admission.

These pure interfaces do not apply a grant, launch a process, or certify a live
provider. Owned capture and V6 replay must supply independently verified execution,
source and claim bindings. Synthetic fixtures are explicitly not child reviews.
"""

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path, PurePosixPath

import claude_context_observation_v1 as observation
import claude_reporting
import claude_telemetry_v8 as telemetry
import diagnostic_tool_contract
import review_coverage as coverage
import review_policy
import review_report_material_v1 as material
from reporting_activation_v4 import validate_policy as legacy_policy
from workflow import WorkflowError

SEED = "issue31-public-capacity-fixtures-v1"
DESCRIPTOR = {
    "schema_version": 1,
    "generator": SEED,
    "component_bytes": 500000,
    "component_lines": 9000,
    "component_ids": 128,
    "qualification_ids": 200,
    "reports": 48,
    "report_bytes": 10000,
    "optional_bytes": 100000,
    "navigation_bytes": 100000,
    "protocol_bytes": 500000,
    "input_bytes": 16000000,
    "batch_observed_input": 872000,
    "capacity_observed_input": 448000,
    "output_reserve": 128000,
    "unknown_allocation": 200000,
}
PURPOSES = {22: "capacity-largest-component", 23: "capacity-integration48-projected"}
LAYOUTS = ("multiline", "entropy", "backslash", "quote", "nel", "ls-ps", "unicode", "del")


def fail():
    raise WorkflowError("Incomplete or inconsistent native capacity evidence") from None


def encoded(value):
    try:
        return json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode("utf-8")
    except (ValueError, TypeError, UnicodeError, RecursionError):
        fail()


def sha(raw):
    if type(raw) is not bytes:
        fail()
    return hashlib.sha256(raw).hexdigest()


def profile(policy):
    """Validate only the closed capacity budget difference; grants stay separate."""
    candidate = copy.deepcopy(policy)
    if type(candidate) is not dict or encoded(candidate.get("budget")) != encoded(
        review_policy.budget("claude-code", {})
    ):
        fail()
    candidate["budget"] = review_policy.budget("claude-code", {}, diagnostic=True)
    legacy_policy(candidate)
    return {
        "schema_version": 1,
        "policy_sha256": sha(encoded(policy)),
        "descriptor_sha256": sha(encoded(DESCRIPTOR)),
    }


def transport(number, claim, fixture_sha256):
    """Identity check only. The caller must independently replay the actual claim."""
    if type(number) is not int or number not in PURPOSES or not material._hex(fixture_sha256):
        fail()
    expected = {
        "schema_version": 6,
        "number": number,
        "purpose": PURPOSES[number],
        "fixture_sha256": fixture_sha256,
        "descriptor_sha256": sha(encoded(DESCRIPTOR)),
        "native_seconds": 900,
        "reference_usd": 10,
    }
    if encoded(claim) != encoded(expected):
        fail()
    return "native-tools-and-source"


def public_bytes(label, size):
    if (
        type(size) is not int
        or not 0 <= size <= 500000
        or label not in {"optional", "navigation", "supplement"}
    ):
        fail()
    # Public deterministic entropy, not a private sample or token estimate.
    chunks = [
        hashlib.sha256(f"{SEED}:{label}:{i}".encode()).hexdigest().encode() for i in range((size + 63) // 64)
    ]
    return b"".join(chunks)[:size]


def reports():
    """48 strict schema2 documents, six per layout, each exactly 10000 UTF8 bytes."""
    result = []
    for layout in LAYOUTS:
        for index in range(6):
            key = f"{SEED}:{layout}:{index}"
            document = {
                "schema_version": 2,
                "inventory_sha256": sha(key.encode()),
                "findings": [],
                "incomplete": [],
                "reviewed": [
                    sha(f"{key}:{i}".encode())[:24] for i in range(1 if layout == "multiline" else 128)
                ],
                "limitations": ["Public synthetic capacity fixture; not a review."],
            }
            pattern = {
                "backslash": "\\",
                "quote": '"',
                "nel": "\u0085",
                "ls-ps": "\u2028\u2029",
                "unicode": "\U0001f642",
                "del": "\x7f",
            }.get(layout)
            if layout == "entropy":
                pattern = "".join(sha(f"{key}:entropy:{i}".encode()) for i in range(160))
            if layout != "multiline":
                original = document["limitations"][0]
                # Binary search preserves the exact encoded size of each character.
                lower, upper = 0, 10000
                while lower < upper:
                    middle = (lower + upper + 1) // 2
                    document["limitations"] = [original + (pattern * (middle // len(pattern) + 1))[:middle]]
                    if len(encoded(document)) <= 10000:
                        lower = middle
                    else:
                        upper = middle - 1
                document["limitations"] = [original + (pattern * (lower // len(pattern) + 1))[:lower]]
            raw = encoded(document)
            raw += (b"\n" if layout == "multiline" else b" ") * (10000 - len(raw))
            projection = material.render(raw)
            if len(raw) != 10000 or material.reconstruct(projection) != raw:
                fail()
            result.append((layout, raw, projection))
    material.packet_budget([row[2] for row in result], 0)
    return result


def _ranges(items, files, maximum):
    """Closed adapter shape: exact inventory ranges and their public file bytes."""
    if type(items) is not list or not 1 <= len(items) <= maximum or type(files) is not dict:
        fail()
    identities, used, size, lines = set(), set(), 0, 0
    for item in items:
        if type(item) is not dict or set(item) != {"id", "artifact", "start_line", "end_line"}:
            fail()
        identity, path = item["id"], item["artifact"]
        if (
            not material._hex(identity + "0" * 40 if type(identity) is str else None)
            or len(identity) != 24
            or identity in identities
        ):
            fail()
        if (
            type(path) is not str
            or str(PurePosixPath(path)) != path
            or PurePosixPath(path).is_absolute()
            or ".." in PurePosixPath(path).parts
            or "\\" in path
        ):
            fail()
        raw = files.get(path)
        if type(raw) is not bytes or len(raw) > 16000000:
            fail()
        try:
            rows = raw.decode("utf-8").splitlines(keepends=True)
        except UnicodeError:
            fail()
        start, end = item["start_line"], item["end_line"]
        if type(start) is not int or type(end) is not int or not 1 <= start <= end <= len(rows):
            fail()
        span = {(path, n) for n in range(start, end + 1)}
        if used.intersection(span):
            fail()
        used.update(span)
        identities.add(identity)
        size += len("".join(rows[start - 1 : end]).encode())
        lines += end - start + 1
    if set(files) != {item["artifact"] for item in items}:
        fail()
    return size, lines


def _bundle(number, items, files, dependencies):
    if (
        type(dependencies) is not dict
        or not dependencies
        or not all(material._hex(v) for v in dependencies.values())
    ):
        fail()
    files = dict(files)
    items = copy.deepcopy(items)
    for label in ("optional", "navigation"):
        path = f"capacity/{label}.txt"
        if path in files:
            fail()
        files[path] = public_bytes(label, 100000)
        items.append(
            {"id": sha(f"{SEED}:{label}".encode())[:24], "artifact": path, "start_line": 1, "end_line": 1}
        )
    if (
        len(items) > 200
        or len({item["id"] for item in items}) != len(items)
        or sum(map(len, files.values())) > 16000000
    ):
        fail()
    manifest = {
        "schema_version": 1,
        "number": number,
        "descriptor": copy.deepcopy(DESCRIPTOR),
        "dependencies": copy.deepcopy(dependencies),
        "items": items,
        "files": {p: sha(raw) for p, raw in sorted(files.items())},
    }
    return {"manifest": manifest, "sha256": sha(encoded(manifest)), "files": files}


def largest_fixture(catalog, additional_items, additional_files, dependencies):
    """Select by bytes, lines, then ascending lexical ID; retain every source range.

    Catalog adapter entries are {id, items, files}; no supplied size is trusted.
    Additional ranges carry actual guidance/schema/diagnostic/cross-boundary input.
    """
    if type(catalog) is not list or not 1 <= len(catalog) <= 48:
        fail()
    choices, identifiers, total_items, total_bytes, total_lines = [], set(), 0, 0, 0
    primary_ids = set()
    for component in catalog:
        if (
            type(component) is not dict
            or set(component) != {"id", "items", "files"}
            or type(component["id"]) is not str
            or not component["id"]
            or component["id"] in identifiers
        ):
            fail()
        identifiers.add(component["id"])
        size, lines = _ranges(component["items"], component["files"], 128)
        ids = {item["id"] for item in component["items"]}
        if primary_ids.intersection(ids):
            fail()
        primary_ids.update(ids)
        if size > 500000 or lines > 9000:
            fail()
        total_items += len(component["items"])
        total_bytes += size
        total_lines += lines
        choices.append((-size, -lines, component["id"], component))
    if total_items > 1700 or total_bytes > 6800000 or total_lines > 140000:
        fail()
    neg_size, neg_lines, _, selected = min(choices, key=lambda row: row[:3])
    size, lines = -neg_size, -neg_lines
    items, files = copy.deepcopy(selected["items"]), dict(selected["files"])
    missing_bytes, missing_lines = 500000 - size, 9000 - lines
    if missing_bytes or missing_lines:
        if len(items) == 128 or missing_lines == 0 or missing_bytes < missing_lines:
            fail()
        path = "capacity/supplement.txt"
        if path in files:
            fail()
        files[path] = public_bytes("supplement", missing_bytes - missing_lines) + b"\n" * missing_lines
        items.append(
            {
                "id": sha(f"{SEED}:supplement".encode())[:24],
                "artifact": path,
                "start_line": 1,
                "end_line": missing_lines,
            }
        )
    if _ranges(items, files, 128) != (500000, 9000):
        fail()
    _ranges(additional_items, additional_files, 200)
    if set(files).intersection(additional_files):
        fail()
    files.update(additional_files)
    items.extend(copy.deepcopy(additional_items))
    if type(dependencies) is not dict or "catalog_sha256" in dependencies:
        fail()
    return _bundle(
        22,
        items,
        files,
        {
            **dependencies,
            "catalog_sha256": sha(
                encoded(
                    [
                        {k: v for k, v in c.items() if k != "files"}
                        | {"files": {p: sha(b) for p, b in c["files"].items()}}
                        for c in catalog
                    ]
                )
            ),
        },
    )


def integration_fixture(additional_items, additional_files, dependencies, *, existing_projection_bytes=0):
    _ranges(additional_items, additional_files, 150)
    files, items, projections = dict(additional_files), copy.deepcopy(additional_items), []
    for index, (layout, raw, projection) in enumerate(reports()):
        raw_path, path = f"capacity/reports/{index:02}-{layout}.json", f"capacity/projections/{index:02}.txt"
        if raw_path in files or path in files:
            fail()
        files[raw_path], files[path] = raw, projection
        projections.append(projection)
        items.append(
            {"id": sha(raw)[:24], "artifact": path, "start_line": 1, "end_line": projection.count(b"\n")}
        )
    material.packet_budget(projections, existing_projection_bytes)
    return _bundle(23, items, files, dependencies)


def verify_fixture(actual, expected):
    """Expected must be regenerated from final source, not read from the artifact."""
    if type(actual) is not dict or set(actual) != {"manifest", "sha256", "files"}:
        fail()
    if (
        encoded(actual["manifest"]) != encoded(expected["manifest"])
        or actual["sha256"] != sha(encoded(expected["manifest"]))
        or actual["files"] != expected["files"]
    ):
        fail()
    if {p: sha(b) for p, b in actual["files"].items()} != actual["manifest"]["files"]:
        fail()


def estimate(maxima):
    """Exact empirical arithmetic only; caller must replay BOTH actual cases."""
    if (
        type(maxima) is not dict
        or set(maxima) != {22, 23}
        or any(type(v) is not int or not 0 <= v <= 448000 for v in maxima.values())
    ):
        fail()
    peak = max(maxima.values())
    projected = (3 * peak + 1) // 2 + 200000
    return {
        "schema_version": 1,
        "max_observed_input": peak,
        "planning_input": projected,
        "output_reserve": 128000,
        "planned_total": projected + 128000,
        "meaning": "Empirical planning allocation; not universal fit or provider attestation",
    }


def bridge(raw, packet, workspace, policy, session_id, bindings):
    """Requalify transient native bytes before deriving numeric/order evidence.

    No dispatch authority is accepted here. Execution/input/source/fixture bindings
    must be checked by the future owned V6 consumer. Returned hashes never replace
    that check. All native IDs and full-message fingerprints die with this call.
    """
    profile(policy)
    body, diagnostics, proof = telemetry.capture(
        raw,
        packet,
        workspace,
        policy,
        session_id,
        diagnostic_purpose="native-tools-and-source",
        diagnostic_tool_contract=diagnostic_tool_contract.contract(),
    )
    if (
        not proof["accepted"]
        or claude_reporting.replay(proof) != proof
        or diagnostics["reasons"]
        or not coverage.assess(packet, body, diagnostics, policy=policy)["qualified"]
    ):
        fail()
    required = coverage.strict_json((Path(packet) / "required-material.json").read_text())["required"]
    return _observe(raw, body, diagnostics, proof, policy, bindings, required)


def batch_bridge(raw, packet, workspace, policy, session_id, bindings, required_ids):
    """Derive ordinary child observations, never dispatch or readiness authority.

    The runtime must independently derive required_ids from the immutable plan,
    validate the child packet/claim and retain the result inside its owned capture.
    This pure function cannot certify that association. Unassigned material stays
    present and unread; it is never removed to manufacture global qualification.
    """
    profile(policy)
    inventory = coverage.strict_json((Path(packet) / "required-material.json").read_text())
    required = inventory["required"]
    if (
        type(required_ids) is not list
        or not required_ids
        or any(type(value) is not str for value in required_ids)
        or len(required_ids) != len(set(required_ids))
        or len(required_ids) > 128
    ):
        fail()
    selected = set(required_ids)
    if not selected <= {item["id"] for item in required}:
        fail()
    body, diagnostics, proof = telemetry.capture(raw, packet, workspace, policy, session_id)
    assessment = coverage.assess(packet, body, diagnostics, policy=policy)
    if (
        not proof["accepted"]
        or claude_reporting.replay(proof) != proof
        or diagnostics["reasons"]
        or assessment["reasons"]
        or {row["id"] for row in assessment["material"] if row["state"] == "reviewed"} & selected != selected
    ):
        fail()
    sidecar, correlation = _observe(
        raw,
        body,
        diagnostics,
        proof,
        policy,
        bindings,
        [item for item in required if item["id"] in selected],
    )
    summary = observation.replay(sidecar, bindings, diagnostics["usage"]["counters"], correlation)
    if summary["max_observed_input"] > DESCRIPTOR["batch_observed_input"]:
        fail()
    return sidecar, correlation


def _observe(raw, body, diagnostics, proof, policy, bindings, required):
    """Shared numeric correlation after the caller's distinct frozen qualification."""
    expected = dict(bindings)
    for key, value in {
        "policy": encoded(policy),
        "report": body.encode(),
        "proof": encoded(proof),
        "diagnostic": encoded(diagnostics),
        "descriptor": observation._bytes(observation.DESCRIPTOR),
    }.items():
        if expected.get(key + "_sha256") != sha(value):
            fail()
    observer = observation.Observer()
    pending, seen, reads, output, ordinal, record_index = {}, {}, [], None, 0, 0
    spans_needed = {(i["artifact"], n) for i in required for n in range(i["start_line"], i["end_line"] + 1)}
    spans_seen = set()

    def feed(kind, message=None):
        nonlocal ordinal
        ordinal += 1
        event = {"ordinal": ordinal, "kind": kind}
        if message is not None:
            event["message"] = message
        observer.feed(event)

    try:
        for line in raw.decode().splitlines():
            event = coverage.strict_json(line)
            kind = event["type"]
            if kind == "assistant":
                message = event["message"]
                fingerprint = sha(encoded(message))
                identity = message["id"]
                if identity in seen and seen[identity] != fingerprint:
                    fail()
                duplicate = identity in seen
                seen[identity] = fingerprint
                feed("native_assistant", message)
                if not duplicate:
                    for block in message["content"]:
                        if block["type"] == "tool_use":
                            pending[block["id"]] = block["name"]
            elif kind == "user":
                for block in event["message"]["content"]:
                    name = pending.pop(block["tool_use_id"])
                    mandatory = False
                    if name != "StructuredOutput":
                        record = diagnostics["events"][record_index]
                        record_index += 1
                        if (
                            record["result_sha256"] != coverage.checksum(block["content"])
                            or record["tool"] != {"Read": "view", "Grep": "grep", "Glob": "glob"}[name]
                        ):
                            fail()
                        if name == "Read":
                            spans = {
                                (s["artifact"], n)
                                for s in record["spans"]
                                for n in range(s["start_line"], s["end_line"] + 1)
                            }
                            mandatory = bool(spans & spans_needed)
                            spans_seen.update(spans & spans_needed)
                    feed("mandatory_read" if mandatory else "other")
                    if mandatory:
                        reads.append(ordinal)
            elif (
                kind == "stream_event"
                and event["event"]["type"] == "content_block_start"
                and event["event"]["content_block"].get("name") == "StructuredOutput"
            ):
                if output is not None or spans_seen != spans_needed:
                    fail()
                feed("structured_output")
                output = ordinal
            elif kind == "result":
                feed("terminal_success")
            else:
                feed("other")
        if pending or record_index != len(diagnostics["events"]) or spans_seen != spans_needed:
            fail()
        correlation = {"event_count": ordinal, "mandatory_read_ordinals": reads, "output_ordinal": output}
        sidecar = observer.seal(expected, diagnostics["usage"]["counters"], correlation)
    except (ValueError, TypeError, KeyError, IndexError, UnicodeError):
        fail()
    return sidecar, correlation


def empirical_receipt(cases, expected_bindings):
    """Replay two retained observations and bind the planning arithmetic.

    This storage receipt has NO qualified/ready field. Only future V6 admission
    may establish that these are the actual independently qualified 22/23 calls.
    Expected bindings must come from that independent replay, never this receipt.
    """
    if (
        type(cases) is not dict
        or set(cases) != {22, 23}
        or type(expected_bindings) is not dict
        or set(expected_bindings) != {22, 23}
    ):
        fail()
    maxima, hashes = {}, {}
    for number in (22, 23):
        case = cases[number]
        if type(case) is not dict or set(case) != {
            "sidecar",
            "completion",
            "capture",
            "counters",
            "correlation",
        }:
            fail()
        if type(case["capture"]) is not bytes or not 0 < len(case["capture"]) <= 8000000:
            fail()
        binding = expected_bindings[number]
        summary = observation.replay_completed(
            case["sidecar"],
            case["completion"],
            sha(case["capture"]),
            binding,
            case["counters"],
            case["correlation"],
        )
        maxima[number] = summary["max_observed_input"]
        hashes[str(number)] = {
            "bindings": copy.deepcopy(binding),
            "sidecar_sha256": sha(case["sidecar"]),
            "capture_sha256": sha(case["capture"]),
        }
    for name in ("source_sha256", "policy_sha256", "descriptor_sha256"):
        if expected_bindings[22][name] != expected_bindings[23][name]:
            fail()
    if (
        expected_bindings[22]["fixture_sha256"] == expected_bindings[23]["fixture_sha256"]
        or hashes["22"]["capture_sha256"] == hashes["23"]["capture_sha256"]
    ):
        fail()
    return {
        "schema_version": 1,
        "descriptor_sha256": sha(encoded(DESCRIPTOR)),
        "cases": hashes,
        "estimate": estimate(maxima),
    }


def largest_fixture_public_catalog_v1(catalog, additional_items, additional_files, dependencies):
    """Select by bytes, lines, then ascending lexical ID; retain every source range.

    Catalog adapter entries are {id, items, files}; no supplied size is trusted.
    Additional ranges carry actual guidance/schema/diagnostic/cross-boundary input.
    """
    import reporting_activation_v6 as authority
    from review_public_catalog_v1 import PROFILE, validate_binding
    from tasks import digest

    validate_binding(dependencies, assignments=True)
    if (
        type(dependencies) is not dict
        or dependencies.get("profile") != digest(PROFILE)
        or dependencies.get("contract") != authority.G20_CONTRACT_DIGEST
        or dependencies.get("authorization")
        != digest(
            {
                "contract_digest": authority.G20_CONTRACT_DIGEST,
                "approval_digest": authority.G20_APPROVAL_DIGEST,
            }
        )
    ):
        fail()
    if type(catalog) is not list or not 1 <= len(catalog) <= 48:
        fail()
    choices, identifiers, total_items, total_bytes, total_lines = [], set(), 0, 0, 0
    primary_ids = set()
    for component in catalog:
        if (
            type(component) is not dict
            or set(component) != {"id", "items", "files"}
            or type(component["id"]) is not str
            or not component["id"]
            or component["id"] in identifiers
        ):
            fail()
        identifiers.add(component["id"])
        size, lines = _ranges(component["items"], component["files"], 128)
        ids = {item["id"] for item in component["items"]}
        if primary_ids.intersection(ids):
            fail()
        primary_ids.update(ids)
        if size > 500000 or lines > 9000:
            fail()
        total_items += len(component["items"])
        total_bytes += size
        total_lines += lines
        choices.append((-size, -lines, component["id"], component))
    extra_bytes, extra_lines = _ranges(additional_items, additional_files, 128 - len(catalog))
    if extra_bytes > 500000 or primary_ids.intersection(item["id"] for item in additional_items):
        fail()
    total_items += len(additional_items)
    total_bytes += extra_bytes
    total_lines += extra_lines
    if total_items > 2400 or total_bytes > 12000000 or total_lines > 220000:
        fail()
    neg_size, neg_lines, _, selected = min(choices, key=lambda row: row[:3])
    size, lines = -neg_size, -neg_lines
    items, files = copy.deepcopy(selected["items"]), dict(selected["files"])
    missing_bytes, missing_lines = 500000 - size, 9000 - lines
    if missing_bytes or missing_lines:
        if len(items) == 128 or missing_lines == 0 or missing_bytes < missing_lines:
            fail()
        path = "capacity/supplement.txt"
        if path in files:
            fail()
        files[path] = public_bytes("supplement", missing_bytes - missing_lines) + b"\n" * missing_lines
        items.append(
            {
                "id": sha(f"{SEED}:supplement".encode())[:24],
                "artifact": path,
                "start_line": 1,
                "end_line": missing_lines,
            }
        )
    if _ranges(items, files, 128) != (500000, 9000):
        fail()
    _ranges(additional_items, additional_files, 200)
    if set(files).intersection(additional_files):
        fail()
    files.update(additional_files)
    items.extend(copy.deepcopy(additional_items))
    if type(dependencies) is not dict or "catalog_sha256" in dependencies:
        fail()
    return _bundle(
        22,
        items,
        files,
        {
            **dependencies,
            "catalog_sha256": sha(
                encoded(
                    [
                        {k: v for k, v in c.items() if k != "files"}
                        | {"files": {p: sha(b) for p, b in c["files"].items()}}
                        for c in catalog
                    ]
                )
            ),
        },
    )
