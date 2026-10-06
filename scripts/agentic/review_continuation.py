"""Explicit same-revision successor preparation; never a retry or grant renewal.

The initial format admits one stopped first-attempt v7 ancestor. Its records stay
in place and are re-evaluated through that version's evaluator. A successor does
not itself authorize a further successor. Preparation performs no inference.
"""

import copy
import shutil
from pathlib import Path

import review_claims
from claude_reporting_execution import exclusive
from reporting_activation import read
from tasks import atomic_json, digest, plain_path, private_directory
from workflow import WorkflowError

FILENAME = "continuation.json"
MAX_BYTES = 2_000_000


def engine():
    import review_batch_v7

    return review_batch_v7


def manifest(directory):
    path = plain_path(Path(directory) / FILENAME)
    if not path.exists():
        return None
    value = read(path)
    meta = engine().api().verify_packet(directory)
    if value.get("schema_version") != 1 or digest(value) != meta.get("continuation_sha256"):
        raise WorkflowError("Continuation manifest binding changed")
    return value


def evidence(directory):
    """Fresh hashes of exact saved artifacts; no raw provider session is retained."""
    directory = plain_path(directory)
    result = {}
    for path in sorted(directory.iterdir()):
        if path.suffix in {".json", ".md", ".txt"}:
            path = plain_path(path)
            if not path.is_file() or path.stat().st_size > 16_000_000:
                raise WorkflowError("Unsupported ancestor evidence artifact")
            result[path.name] = engine().api().digest(path)
    return result


def snapshot(ancestor):
    b = engine()
    ancestor = plain_path(ancestor)
    meta = b.api().verify_packet(ancestor)
    if meta.get("schema_version") != 7 or meta.get("continuation_sha256") or (ancestor / FILENAME).exists():
        raise WorkflowError("Continuation requires an original v7 batch; no mixed or automatic chain")
    batch = b.load(ancestor)
    state = b.state_for(ancestor, batch)
    if (
        not state
        or state["stop_reason"] != "execution_incomplete_or_interrupted"
        or not state["reservations"]
    ):
        raise WorkflowError("Continuation requires an intact stopped ledger")
    aggregate = b.qualification(ancestor)
    rows, imports = {}, []
    for row in state["reservations"]:
        unit = next(u for u in batch["units"] if u["id"] == row["binding"]["unit"])
        target = b.unit_path(ancestor, unit)
        if row["status"] == "reserved/uncertain" or not (target / "review-capture.json").is_file():
            raise WorkflowError("Uncertain ancestor invocation cannot be repeated")
        result = b.unit_assessment(ancestor, batch, unit)
        use = b.observed_usage(target, batch["unit_policy"])
        if use is None:
            raise WorkflowError("Unknown ancestor usage cannot fund a continuation")
        # Known overshoot remains consumed. It is not credited back to either grant.
        complete = result["complete"] and use <= b.review_policy.exact_amount(
            batch["budget"]["unit_cost"], "unit cost"
        )
        if complete and unit["kind"] == "component":
            imports.append(unit["id"])
        rows[unit["id"]] = {
            "files": evidence(target),
            "assessment": digest(result),
            "usage": str(use),
            "claim": row["claim"],
            "complete": complete,
        }
    return {
        "ancestor": str(ancestor),
        "batch_sha256": digest(batch),
        "ledger_sha256": digest(state),
        "aggregate_sha256": digest(aggregate),
        "files": evidence(ancestor),
        "records": rows,
        "imports": imports,
        "remaining": [u["id"] for u in batch["units"] if u["id"] not in imports],
        "reattempts": [u["id"] for u in batch["units"] if u["id"] in rows and u["id"] not in imports],
        "consumed": {
            "reservations": len(rows),
            "usage": [{"unit": k, "reference_usd": v["usage"]} for k, v in rows.items()],
        },
    }


def validate(directory, planned=None):
    value = manifest(directory)
    if value is None:
        return None
    b = engine()
    observed = snapshot(value["snapshot"]["ancestor"])
    if digest(observed) != digest(value["snapshot"]):
        raise WorkflowError("Stopped ancestor reports, assignments, usage or ledger changed")
    old = b.load(observed["ancestor"])
    current = planned or b.plan(directory, continuation=False)
    for key in ("binding", "files", "contract_digest", "config", "inventory_sha256", "units", "resources"):
        if digest(current[key]) != digest(old[key]):
            raise WorkflowError("Continuation reviewed identity, full inventory or assignment differs")
    before, after = copy.deepcopy(old["policy"]), copy.deepcopy(current["policy"])
    before.pop("authentication")
    after.pop("authentication")
    if before != after:
        raise WorkflowError("Continuation reporting/inspection policy changed")
    if set(value["publications"]) != set(observed["imports"]):
        raise WorkflowError("Continuation omitted exact imported publications")
    for unit in old["units"]:
        if unit["id"] in observed["imports"]:
            body = b.api().publication_body(b.unit_path(observed["ancestor"], unit))
            receipt = value["publications"][unit["id"]]
            if (
                receipt["head_sha"] != old["binding"]["head_sha"]
                or receipt["body_sha256"] != b.coverage.checksum(body)
                or receipt["exact_match"] is not True
                or type(receipt.get("review_id")) is not int
                or receipt["review_id"] <= 0
            ):
                raise WorkflowError("Imported COMMENT publication differs")
    return value


def verify_live(repo, directory, batch):
    value = validate(directory)
    if value is None:
        return
    b = engine()
    old = b.load(value["snapshot"]["ancestor"])
    for key in ("harness_commit", "harness_files"):
        if batch["authorization"][key] != old["authorization"][key]:
            raise WorkflowError("Continuation harness changed")
    from claude_native_auth import capability_lineage

    if not capability_lineage(
        old["policy"]["authentication"], batch["policy"]["authentication"], batch["budget"]["unit_seconds"]
    ):
        raise WorkflowError("Continuation lacks verified same-account generation lineage")
    for unit in old["units"]:
        if unit["id"] in value["snapshot"]["imports"]:
            receipt = b.api().verify_publication(repo, b.unit_path(value["snapshot"]["ancestor"], unit))
            if receipt != value["publications"][unit["id"]]:
                raise WorkflowError("Imported publication changed or is ambiguous")


def prepare(repo, ancestor, destination, *, authentication=None):
    """Create only a new immutable proposal. Named finite selection is separate."""
    b = engine()
    ancestor, destination = plain_path(ancestor), plain_path(destination)
    with b.locked(ancestor):
        saved = snapshot(ancestor)
        old = b.load(ancestor)
        meta = b.api().verify_packet(ancestor)
        b.api().current_pr(repo, meta["pr"], meta["head_sha"], meta["base_sha"])
        b.current_contract(repo, ancestor, meta)
        b.verify_harness(old["authorization"])
        receipts = {}
        for unit in old["units"]:
            row = next(
                (r for r in b.state_for(ancestor, old)["reservations"] if r["binding"]["unit"] == unit["id"]),
                None,
            )
            if row:
                review_claims.verify(repo, old, unit, row)
            if unit["id"] in saved["imports"]:
                receipts[unit["id"]] = b.api().verify_publication(repo, b.unit_path(ancestor, unit))
        new_meta = {k: v for k, v in meta.items() if k not in b.api().RESULT_FIELDS | {"batch_sha256"}}
        new_meta["kind"] = "single"
        if authentication is not None:
            from claude_native_auth import capability_lineage

            if not capability_lineage(
                meta["review_policy"]["authentication"],
                authentication,
                meta["review_policy"]["budget"]["timeout_seconds"],
            ):
                raise WorkflowError("Continuation auth renewal is not verified same-account lineage")
            new_meta["review_policy"] = copy.deepcopy(meta["review_policy"])
            new_meta["review_policy"]["authentication"] = authentication
        value = {"schema_version": 1, "snapshot": saved, "publications": receipts}
        destination.mkdir(mode=0o700)  # Never reuse or clean a partly prepared proposal.
        shutil.copytree(ancestor / "packet", destination / "packet")
        exclusive(destination / FILENAME, value, limit=MAX_BYTES)
        new_meta["continuation_sha256"] = digest(value)
        atomic_json(destination / "metadata.json", new_meta)
        validate(destination)
    return destination


def owner(repo, directory, batch, *, create=False):
    """One successor owns the whole stopped allocation, including never-started work."""
    value = validate(directory)
    if value is None:
        return None
    key = value["snapshot"]["batch_sha256"]
    path = review_claims.root(repo) / "successors" / (key + ".json")
    expected = {
        "schema_version": 1,
        "manifest_digest": digest(value),
        "batch_sha256": digest(batch),
        "directory": str(plain_path(directory)),
    }
    with review_claims.locked(repo):
        if create:
            private_directory(path.parent)
            exclusive(path, expected, limit=10000)
        elif read(plain_path(path)) != expected:
            raise WorkflowError("Missing or competing exclusive successor claim")
    return expected


def previous(repo, directory, batch, unit):
    value = validate(directory)
    if value is None:
        return None
    owner(repo, directory, batch)
    if unit["id"] not in value["snapshot"]["remaining"]:
        raise WorkflowError("Imported success cannot be dispatched again")
    row = value["snapshot"]["records"].get(unit["id"])
    return digest(row["claim"]) if row else None


def require_import(repo, directory, batch, unit):
    """Retained old admission plus separately verified current same-account lineage."""
    from claude_native_auth import capability_lineage
    from claude_reporting_execution import retained
    from reporting_admission import FILENAME as ADMISSION
    from reporting_admission import check

    b = engine()
    target = b.unit_path(directory, unit)
    meta = b.api().verify_packet(target)
    old = read(target / ADMISSION)
    new = check(repo, batch["unit_policy"])
    for key in ("schema_version", "grant_digest", "harness", "outcomes", "observed_authentication"):
        if old.get(key) != new.get(key):
            raise WorkflowError("Imported reporting activation changed")
    if (
        old.get("policy_digest") != digest(meta["review_policy"])
        or old.get("current_authentication") != meta["review_policy"]["authentication"]
    ):
        raise WorkflowError("Imported admission lost its original policy")
    if not capability_lineage(
        old["current_authentication"], new["current_authentication"], batch["budget"]["unit_seconds"]
    ):
        raise WorkflowError("Imported admission lacks verified same-account lineage")
    execution = retained(target, meta)
    if execution is None or execution.get("admission_digest") != digest(old):
        raise WorkflowError("Imported report lacks admitted native execution")
