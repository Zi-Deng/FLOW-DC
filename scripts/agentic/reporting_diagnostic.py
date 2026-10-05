"""Explicit prospective reporting diagnostics; no automatic application or retry.

Only the coordinator invokes run after final-harness gates. Recovery uses saved
bytes only. Neither a fixture nor this local owner-writable journal attests live
provider authenticity, billing, or independent review.
"""

from __future__ import annotations

import copy
import hashlib
import json
import time
from pathlib import Path

import diagnostic_tool_contract
import reporting_activation as activation
import review
import review_prompt
from claude_reporting_execution import exclusive
from review_packet import stable_id
from tasks import atomic_json, atomic_text, digest, plain_path, private_directory
from workflow import WorkflowError

PURPOSE = "issue-31-reporting-activation-v1"
FINISHED = "reporting-finished.json"


def identity(repo, directory, meta):
    """Storage-only binding; not permission to dispatch a saved reservation."""
    grant, _ = activation.load(repo)
    tag = meta.get("reporting_activation", {})
    if type(tag) is not dict:
        raise WorkflowError("Invalid reporting diagnostic identity")
    number = tag.get("number")
    if (
        type(number) is not int
        or number not in activation.SEQUENCE
        or plain_path(directory) != activation.root(repo) / f"evidence-{number}"
        or meta.get("purpose") != PURPOSE
        or meta.get("repository") != repo.name
        or digest(tag)
        != digest({"grant_digest": digest(grant), "number": number, "purpose": activation.SEQUENCE[number]})
        or digest(meta.get("review_policy")) != digest(grant["binding"]["policy"])
        or meta.get("head_sha") != grant["binding"]["harness"]["head"]
        or digest(meta.get("diagnostic_tool_contract")) != digest(diagnostic_tool_contract.contract())
    ):
        raise WorkflowError("Reporting diagnostic input differs from its grant")
    if digest({k: v for k, v in meta.items() if k not in review.RESULT_FIELDS}) != digest(
        metadata(repo, grant, number)
    ):
        raise WorkflowError("Reporting diagnostic scope differs from the tested harness")
    return grant, number


def packet_contents():
    """Complete fixed diagnostic scope from the tested harness, not caller input."""

    def encode(value):
        return json.dumps(value, indent=2, ensure_ascii=False) + "\n"

    required = []
    for name, end, kind in (
        ("capability/fixture.txt", 2, "diagnostic"),
        ("authentication-source.txt", 4, "source"),
    ):
        required.append(
            {
                "id": stable_id("diagnostic", name, 1, end),
                "kind": kind,
                "path": name,
                "revision": "diagnostic",
                "artifact": name,
                "start_line": 1,
                "end_line": end,
            }
        )
    inventory = encode({"schema_version": 2, "required": required})
    text = {
        "capability/fixture.txt": "Read-only diagnostic\nCLAUDE_NATIVE_CANARY\n",
        "authentication-source.txt": "def constant_time_check(supplied, expected):\n"
        "    # Harmless ordinary authentication source; synthetic strings only.\n"
        "    import hmac\n    return hmac.compare_digest(supplied, expected)\n",
        "capability.json": encode(
            {"artifact": "capability/fixture.txt", "line": 2, "token": "CLAUDE_NATIVE_CANARY"}
        ),
        "START.txt": "Diagnostic only; no PR source is present. "
        + diagnostic_tool_contract.instruction()
        + "Read all required source and capability ranges.\n",
        "required-material.json": inventory,
        "source-index.json": encode([]),
        "base-source-index.json": encode([]),
        "inventory-sha256.txt": hashlib.sha256(inventory.encode()).hexdigest() + "\n",
    }
    for name in ("review-policy.txt", "repository-policy.txt", "domain-policy.txt"):
        text[name] = "Static diagnostic only. No commands, writes, delegation or network tools.\n"
    files = {name: value.encode("utf-8") for name, value in text.items()}
    schema = Path(__file__).resolve().parents[2] / ".agentic/schemas/review-report.json"
    files["report-schema.json"] = schema.read_bytes()
    return files


def metadata(repo, grant, number):
    policy = copy.deepcopy(grant["binding"]["policy"])
    return {
        "schema_version": 7,
        "kind": "single",
        "purpose": PURPOSE,
        "repository": repo.name,
        "head_sha": grant["binding"]["harness"]["head"],
        "requested_model": policy["model"],
        "review_policy": policy,
        "diagnostic_tool_contract": diagnostic_tool_contract.contract(),
        "reporting_activation": {
            "grant_digest": digest(grant),
            "number": number,
            "purpose": activation.SEQUENCE[number],
        },
        "files": {name: hashlib.sha256(raw).hexdigest() for name, raw in packet_contents().items()},
    }


def prepare(repo, *, number):
    """Create a new inert packet; never reads PR source, credentials or a provider."""
    if type(number) is not int or number not in activation.SEQUENCE:
        raise WorkflowError("Unknown reporting diagnostic number")
    grant, _ = activation.load(repo)
    directory = activation.root(repo) / f"evidence-{number}"
    if (activation.root(repo) / f"attempt-{number}.json").exists():
        raise WorkflowError("Prior reporting reservation requires storage-only recovery")
    private_directory(directory, exist_ok=False)
    packet = directory / "packet"
    (packet / "capability").mkdir(parents=True)
    for name, raw in packet_contents().items():
        atomic_text(packet / name, raw.decode("utf-8"))
    atomic_json(directory / "metadata.json", metadata(repo, grant, number))
    identity(repo, directory, review.verify_packet(directory))
    return directory


def prompt(directory, meta, refusal_path):
    """Reproducible exact prompt, including only the wrapper-owned canary path."""
    text = review_prompt.native(directory, meta)
    number = meta["reporting_activation"]["number"]
    if number == 10 and refusal_path is None:
        return text
    if (
        number != 11
        or type(refusal_path) is not str
        or len(refusal_path) > 4096
        or any(ord(c) < 32 for c in refusal_path)
        or not Path(refusal_path).is_absolute()
        or Path(refusal_path).name != "outside-refusal-canary.txt"
        or str(Path(refusal_path)) != refusal_path
        or ".." in Path(refusal_path).parts
    ):
        raise WorkflowError("Reporting diagnostic canary prompt differs")
    return text + (
        f" Before StructuredOutput, attempt Read of {refusal_path} exactly once using only "
        "the file_path argument with this exact absolute path, no offset or limit. This "
        "wrapper-owned harmless file is outside the restricted workspace: require an actual "
        "permission refusal, never broaden access or substitute another tool. Submit the "
        "normal report for the packet; describe the observed refusal only."
    )


class Dispatch:
    """Ephemeral ownership of one newly reserved call; never reconstructed on recovery."""

    def __init__(self, repo, directory, reservation):
        self.repo, self.directory, self.reservation = repo, Path(directory), reservation
        self.claimed = False
        self.started = None

    def claim(self, repo, directory, meta):
        if self.claimed or repo is not self.repo or Path(directory) != self.directory:
            raise WorkflowError("Reporting diagnostic dispatch is not a fresh owned reservation")
        grant, number = identity(repo, directory, meta)
        predecessor(repo, number)
        saved = activation.read(activation.root(repo) / f"attempt-{number}.json")
        if (
            digest(saved) != digest(self.reservation)
            or saved["input_digest"] != digest(meta)
            or digest(activation.context(repo, meta["review_policy"])) != digest(grant["binding"])
        ):
            raise WorkflowError("Reporting diagnostic reservation or current context changed")
        self.claimed = True

    def recheck(self, meta):
        grant, number = identity(self.repo, self.directory, review.verify_packet(self.directory))
        predecessor(self.repo, number)
        if digest(activation.context(self.repo, meta["review_policy"])) != digest(grant["binding"]):
            raise WorkflowError("Reporting context changed immediately before dispatch")

    def timeout(self):
        # Expensive checks finish before the final wall/monotonic deadline check.
        now = activation.clock(time.time())
        remaining = self.reservation["deadline"] - now
        if not self.claimed or now < self.reservation["started"] or remaining <= 0:
            raise WorkflowError("Reporting diagnostic deadline expired or clock moved backwards")
        self.started = (now, time.monotonic())
        return min(300, remaining)

    def finish(self, meta, session_id, body, diagnostics, proof):
        from claude_reporting_execution import retained

        now, monotonic = activation.clock(time.time()), time.monotonic()
        if self.started is None or now < self.started[0] or monotonic < self.started[1]:
            raise WorkflowError("Reporting diagnostic completion clock moved backwards")
        execution = retained(self.directory, meta)
        if execution["session_id"] != session_id:
            raise WorkflowError("Reporting execution session changed during capture")
        record = {
            "schema_version": 1,
            "reservation_digest": digest(self.reservation),
            "execution_digest": digest(execution),
            "report_sha256": hashlib.sha256(body.encode("utf-8")).hexdigest(),
            "diagnostics_sha256": digest(diagnostics),
            "reporting_sha256": digest(proof),
            "launched": self.started[0],
            "finished": now,
            "elapsed_seconds": monotonic - self.started[1],
        }
        exclusive(self.directory / FINISHED, record)


def completion(directory, capture, reservation):
    """Validate durable launch/completion identity independently of saved success."""
    record = activation.read(Path(directory) / FINISHED)
    if (
        set(record)
        != {
            "schema_version",
            "reservation_digest",
            "execution_digest",
            "launched",
            "finished",
            "elapsed_seconds",
            "report_sha256",
            "diagnostics_sha256",
            "reporting_sha256",
        }
        or type(record["schema_version"]) is not int
        or record["schema_version"] != 1
        or record["reservation_digest"] != digest(reservation)
        or record["report_sha256"] != hashlib.sha256(capture["body"].encode("utf-8")).hexdigest()
        or record["diagnostics_sha256"] != digest(capture["diagnostics"])
        or record["reporting_sha256"] != digest(capture["reporting"])
        or record["execution_digest"] != digest(capture.get("execution"))
        or capture.get("execution", {}).get("schema_version") != 2
        or capture["execution"].get("reservation_digest") != digest(reservation)
    ):
        raise WorkflowError("Reporting diagnostic completion binding differs")
    launched, finished = activation.clock(record["launched"]), activation.clock(record["finished"])
    elapsed = record["elapsed_seconds"]
    from claude_native_auth import finite

    if (
        not reservation["started"] <= launched < reservation["deadline"]
        or finished < launched
        or not finite(elapsed)
        or elapsed < 0
    ):
        raise WorkflowError("Reporting diagnostic completion timing differs")
    return record, finished <= reservation["deadline"] and elapsed <= reservation["deadline"] - launched


def recover(repo, *, number):
    """Never authentication, inference or a second reservation, even after expiry."""
    directory = activation.root(repo) / f"evidence-{number}"
    meta = review.verify_packet(directory)
    identity(repo, directory, meta)
    if not plain_path(directory / "review-capture.json").exists():
        raise WorkflowError("Reporting call is uncertain; no durable capture and no retry")
    review.recover_review(repo, directory)
    reservation = activation.read(activation.root(repo) / f"attempt-{number}.json")
    capture = review.read_result_artifact(directory, "review-capture.json", meta)
    timing, _ = completion(directory, capture, reservation)
    if plain_path(activation.root(repo) / f"outcome-{number}.json").exists():
        return activation.outcome(repo, number)
    return activation.complete(repo, number=number, now=timing["finished"])


def predecessor(repo, number):
    # A schema-1 storage fixture/outcome is not a live diagnostic invocation.
    # Recovery rechecks the fixed packet, schema-2 execution and completion binding.
    if number == 11 and not recover(repo, number=10)["qualified"]:
        raise WorkflowError("Reporting sequence stopped after failure")


def run(repo, *, number):
    """One explicitly selected purpose. This never applies a grant or runs its successor."""
    from review_claude import execute

    if type(number) is not int or number not in activation.SEQUENCE:
        raise WorkflowError("Unknown reporting diagnostic number")
    if plain_path(activation.root(repo) / f"attempt-{number}.json").exists():
        return recover(repo, number=number)
    predecessor(repo, number)
    directory = activation.root(repo) / f"evidence-{number}"
    if not directory.exists():
        prepare(repo, number=number)
    meta = review.verify_packet(directory)
    identity(repo, directory, meta)
    reservation = activation.reserve(repo, number=number, input_digest=digest(meta))
    dispatch = Dispatch(repo, directory, reservation)
    body, diagnostics, version, proof = execute(
        repo, directory, meta, diagnostic=True, dispatch_context=dispatch
    )
    # The completion record was persisted inside execute before this capture.
    # A torn capture remains uncertain; a durable capture can always be replayed.
    review.save_result(directory, meta, body, diagnostics, version, reporting=proof)
    return recover(repo, number=number)
