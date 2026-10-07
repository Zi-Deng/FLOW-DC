"""V6 diagnostic identities, owned capture and durable replay.

The final catalog adapter remains an explicit closed dependency.
Neither a local journal nor a synthetic capture attests a live provider. Storage
replay never obtains credentials, launches a process, or invents a missing call.
"""

from __future__ import annotations

import copy
import hashlib
import time
from pathlib import Path
from types import SimpleNamespace

import claude_context_observation_v1 as observer
import claude_partial_observation_v2 as partial
import claude_reporting
import diagnostic_tool_contract
import reporting_activation_v6 as activation
import review
import review_capacity_native_v1 as capacity
import review_coverage
import review_policy
import review_prompt
from claude_reporting_execution import exclusive, retained
from reporting_diagnostic_v2 import packet_contents as base_packet
from reporting_recovery_history_v6 import known_usage
from tasks import atomic_json, digest, plain_path, private_directory
from workflow import WorkflowError

PURPOSE = activation.PURPOSE
FINISHED = "reporting-finished.json"
DESCRIPTOR = {
    "schema_version": 6,
    "metadata_version": 7,
    "capacity_descriptor_sha256": capacity.sha(capacity.encoded(capacity.DESCRIPTOR)),
    "observer_descriptor_sha256": capacity.sha(observer._bytes(observer.DESCRIPTOR)),
    "final_navigation": {"tool": "Glob", "input": {"pattern": "capability/fixture.txt"}},
}


def catalog(repo):
    """Recompute the designated final catalog and all current check provenance."""
    import review_batch_windows_v1

    task = plain_path(repo.main / ".agentic-local/tasks/issue-31.json")
    if not task.exists() or not activation.read(task).get("v6_catalog"):
        # Preserve the historical unconfigured dependency diagnostic.
        raise WorkflowError(
            "V6 fixture/profile and owned-window integration is not implemented: final catalog designation missing"
        )
    return review_batch_windows_v1.catalog(repo)


def policies(base, number):
    activation.slot(number)
    activation.validate_policy(base)
    result = copy.deepcopy(base)
    if number >= 22:
        result["budget"] = review_policy.budget("claude-code", {})
        capacity.profile(result)
    return result


def fixture_binding(files):
    if type(files) is not dict or not files:
        raise WorkflowError("V6 requires an exact complete packet")
    hashes = {}
    for name, raw in files.items():
        if (
            type(name) is not str
            or not name
            or str(Path(name)) != name
            or Path(name).is_absolute()
            or ".." in Path(name).parts
            or "\\" in name
            or type(raw) is not bytes
        ):
            raise WorkflowError("Invalid V6 packet path or bytes")
        hashes[name] = hashlib.sha256(raw).hexdigest()
    if sum(map(len, files.values())) > 16000000:
        raise WorkflowError("V6 packet exceeds fixed input envelope")
    return {"fixture_sha256": digest(hashes), "descriptor_sha256": digest(DESCRIPTOR)}


def packets(source):
    """Pure catalog adapter, not authority: all public material remains mandatory.

    Final catalog supplier independently validates completeness and check receipts.
    It supplies components plus integration/additional ranges using capacity-v1's
    closed shapes; no private investigation sample is accepted as a substitute.
    """
    if type(source) is not dict or set(source) != {"components", "items", "files", "dependencies"}:
        raise WorkflowError("Invalid V6 catalog adapter result")
    base = base_packet()
    original = review_coverage.strict_json(base["required-material.json"].decode())["required"]
    items = [{key: row[key] for key in ("id", "artifact", "start_line", "end_line")} for row in original]
    files = {row["artifact"]: base[row["artifact"]] for row in original}
    if set(files).intersection(source["files"]):
        raise WorkflowError("V6 catalog overwrites diagnostic ranges")
    # Packet guidance/schema are additional mandatory capacity ranges. The
    # inventory/hash controls themselves cannot recursively inspect their hashes.
    for name in (
        "START.txt",
        "report-schema.json",
        "review-policy.txt",
        "repository-policy.txt",
        "domain-policy.txt",
    ):
        files[name] = base[name]
        items.append(
            {
                "id": digest(["v6-capacity-guidance", name])[:24],
                "artifact": name,
                "start_line": 1,
                "end_line": len(base[name].decode().splitlines()),
            }
        )
    if set(files).intersection(source["files"]):
        raise WorkflowError("V6 catalog overwrites fixed guidance")
    files.update(source["files"])
    items.extend(copy.deepcopy(source["items"]))
    generated = {
        22: capacity.largest_fixture(source["components"], items, files, source["dependencies"]),
        23: capacity.integration_fixture(items, files, source["dependencies"]),
    }
    result = {}
    for number in activation.SEQUENCE:
        packet = dict(base)
        packet["v6-descriptor.json"] = capacity.encoded(DESCRIPTOR)
        if number >= 22:
            bundle = generated[number]
            for name, raw in bundle["files"].items():
                if name in packet and packet[name] != raw:
                    raise WorkflowError("V6 fixture overwrites packet controls")
                packet[name] = raw
            required = [
                {**row, "kind": "diagnostic", "path": row["artifact"], "revision": "diagnostic"}
                for row in bundle["manifest"]["items"]
            ]
            packet["required-material.json"] = capacity.encoded({"schema_version": 2, "required": required})
            packet["inventory-sha256.txt"] = (
                hashlib.sha256(packet["required-material.json"]).hexdigest() + "\n"
            ).encode()
            packet["capacity-manifest.json"] = capacity.encoded(bundle["manifest"])
        fixture_binding(packet)
        result[str(number)] = packet
    return result


def metadata(repo, grant, number, files):
    activation.validate_grant(grant)
    selected = policies(grant["binding"]["policy"], number)
    if fixture_binding(files) != grant["binding"]["fixtures"][str(number)]:
        raise WorkflowError("V6 prepared fixture differs from immutable grant")
    return {
        "schema_version": 7,
        "kind": "single",
        "purpose": PURPOSE,
        "repository": repo.name,
        "head_sha": grant["binding"]["harness"]["head"],
        "requested_model": selected["model"],
        "review_policy": selected,
        "diagnostic_tool_contract": diagnostic_tool_contract.contract(),
        "reporting_activation": {
            "grant_digest": digest(grant),
            "number": number,
            "purpose": activation.SEQUENCE[number],
        },
        "v6_descriptor": copy.deepcopy(DESCRIPTOR),
        "v6_fixture": copy.deepcopy(grant["binding"]["fixtures"][str(number)]),
        "files": {name: hashlib.sha256(raw).hexdigest() for name, raw in files.items()},
    }


def identity(repo, directory, meta):
    grant, _ = activation.load(repo)
    tag = meta.get("reporting_activation")
    if type(tag) is not dict or type(tag.get("number")) is not int:
        raise WorkflowError("Invalid V6 diagnostic identity")
    number = tag["number"]
    activation.slot(number)
    if plain_path(directory) != activation.root(repo) / f"evidence-{number}":
        raise WorkflowError("V6 diagnostic directory differs")
    # verify_packet binds every declared file; metadata() additionally binds its
    # entire hash map to the immutable four-fixture grant, not a caller assertion.
    checked = review.verify_packet(directory)
    inputs = {k: v for k, v in checked.items() if k not in review.RESULT_FIELDS}
    if digest(inputs) != digest({k: v for k, v in meta.items() if k not in review.RESULT_FIELDS}):
        raise WorkflowError("V6 diagnostic metadata changed")
    files = {name: (Path(directory) / "packet" / name).read_bytes() for name in inputs["files"]}
    if digest(inputs) != digest(metadata(repo, grant, number, files)):
        raise WorkflowError("V6 diagnostic input differs from grant or source")
    return grant, number


def prepare(repo, *, number):
    activation.slot(number)
    grant, _ = activation.load(repo)
    if (activation.root(repo) / f"attempt-{number}.json").exists():
        raise WorkflowError("Prior V6 reservation requires storage-only recovery")
    files = packets(catalog(repo))[str(number)]
    meta = metadata(repo, grant, number, files)
    directory = activation.root(repo) / f"evidence-{number}"
    private_directory(directory, exist_ok=False)
    for name, raw in files.items():
        target = directory / "packet" / name
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("xb") as stream:
            stream.write(raw)
    atomic_json(directory / "metadata.json", meta)
    identity(repo, directory, meta)
    return directory


def prompt(directory, meta, refusal_path):
    number = meta.get("reporting_activation", {}).get("number")
    activation.slot(number)
    if meta.get("purpose") != PURPOSE or digest(meta.get("v6_descriptor")) != digest(DESCRIPTOR):
        raise WorkflowError("V6 diagnostic prompt identity differs")
    text = review_prompt.native(directory, meta)
    if number == 20:
        if (
            type(refusal_path) is not str
            or len(refusal_path) > 4096
            or not Path(refusal_path).is_absolute()
            or str(Path(refusal_path)) != refusal_path
            or ".." in Path(refusal_path).parts
            or Path(refusal_path).name != "outside-refusal-canary.txt"
            or any(ord(c) < 32 for c in refusal_path)
        ):
            raise WorkflowError("V6 controlled refusal path differs")
        text += f" Before final navigation attempt Read of {refusal_path} exactly once with only file_path, no offset or limit. Require the actual permission refusal; do not broaden access or substitute another tool."
    elif refusal_path is not None:
        raise WorkflowError("V6 non-isolation purpose cannot acquire a refusal canary")
    return text + (
        " After ALL mandatory Read results and any controlled refusal, call Glob with exactly "
        '{"pattern":"capability/fixture.txt"}. Wait for its result before StructuredOutput. '
        "This final permitted navigation call earns no source credit. Do not perform another "
        "Read after it; if more Read is necessary, repeat final Glob after the last Read. "
        "Submit the exact full report once, preserving all required IDs and findings."
    )


def predecessor(repo, number):
    activation.slot(number)
    for n in activation.SEQUENCE:
        if n < number:
            previous = activation.outcome(repo, n)
            allocation = activation.slot(n)
            if previous.get("qualified") is not True or not known_usage(
                previous.get("usage"), allocation["native_seconds"], allocation["reference_usd"]
            ):
                raise WorkflowError("V6 sequence stopped: predecessor incomplete or usage unknown")


class Dispatch:
    """Single-use in-memory ownership; recovery can never reconstruct dispatch."""

    def __init__(self, repo, directory, reservation):
        self.repo, self.directory = repo, Path(directory)
        self.reservation = copy.deepcopy(reservation)
        self.claimed, self.started = False, None

    def _check(self, meta, owned_auth=None):
        grant, number = identity(self.repo, self.directory, meta)
        predecessor(self.repo, number)
        saved = activation.read(activation.root(self.repo) / f"attempt-{number}.json")
        _, application = activation.load(self.repo)
        now = activation.clock(time.time())
        if (
            digest(saved) != digest(self.reservation)
            or saved["input_digest"]
            != digest({k: v for k, v in meta.items() if k not in review.RESULT_FIELDS})
            or digest(saved)
            != digest(activation._reservation(grant, number, saved["input_digest"], saved["started"]))
            or not saved["started"]
            <= now
            <= min(
                saved["deadline"] - activation.slot(number)["native_seconds"],
                application["deadline"] - activation.remaining(number),
            )
        ):
            raise WorkflowError("V6 reservation or full remaining window changed")
        current = activation.context(
            self.repo,
            grant["binding"]["policy"],
            remaining_seconds=application["deadline"] - now,
            owned_auth=owned_auth,
        )
        if digest(current) != digest(grant["binding"]):
            raise WorkflowError("V6 source, fixtures, authority or authentication changed")
        checked = activation.clock(time.time())
        if (
            not now
            <= checked
            <= min(
                saved["deadline"] - activation.slot(number)["native_seconds"],
                application["deadline"] - activation.remaining(number),
            )
        ):
            raise WorkflowError("V6 prerequisite checks exhausted immutable window")
        return number

    def claim(self, repo, directory, meta, *, owned_auth=None):
        if self.claimed or repo is not self.repo or Path(directory) != self.directory:
            raise WorkflowError("V6 dispatch is not a fresh owned reservation")
        self._check(meta, owned_auth)
        self.claimed = True

    def recheck(self, meta, *, owned_auth=None):
        if not self.claimed:
            raise WorkflowError("V6 dispatch has no claim")
        return self._check(meta, owned_auth)

    def timeout(self):
        now = activation.clock(time.time())
        seconds = activation.slot(self.reservation.get("number"))["native_seconds"]
        if (
            not self.claimed
            or self.started is not None
            or not self.reservation["started"] <= now <= self.reservation["deadline"] - seconds
        ):
            raise WorkflowError("V6 full native allocation cannot fit or launch was repeated")
        self.started = (now, time.monotonic())
        return seconds

    def finish(self, meta, session_id, body, diagnostics, proof):
        now, monotonic = activation.clock(time.time()), time.monotonic()
        if self.started is None or now < self.started[0] or monotonic < self.started[1]:
            raise WorkflowError("V6 completion clock rolled back")
        execution = retained(self.directory, meta)
        if execution is None or execution["session_id"] != session_id:
            raise WorkflowError("V6 completion lacks its exact execution")
        seconds = activation.slot(self.reservation["number"])["native_seconds"]
        if (
            now > self.reservation["deadline"]
            or now - self.started[0] > seconds
            or monotonic - self.started[1] > seconds
        ):
            raise WorkflowError("V6 completion exceeded immutable allocation")
        exclusive(
            self.directory / FINISHED,
            {
                "schema_version": 6,
                "reservation_digest": digest(self.reservation),
                "execution_digest": digest(execution),
                "report_sha256": hashlib.sha256(body.encode()).hexdigest(),
                "diagnostics_sha256": digest(diagnostics),
                "reporting_sha256": digest(proof),
                "fixture": copy.deepcopy(meta["v6_fixture"]),
                "launched": self.started[0],
                "finished": now,
                "elapsed_seconds": monotonic - self.started[1],
            },
        )


def completion(directory, capture, reservation):
    record = activation.read(Path(directory) / FINISHED)
    fields = {
        "schema_version",
        "reservation_digest",
        "execution_digest",
        "report_sha256",
        "diagnostics_sha256",
        "reporting_sha256",
        "fixture",
        "launched",
        "finished",
        "elapsed_seconds",
    }
    if type(record) is not dict or set(record) != fields:
        raise WorkflowError("V6 completion is torn or unsupported")
    expected = {
        **record,
        "schema_version": 6,
        "reservation_digest": digest(reservation),
        "execution_digest": digest(capture.get("execution")),
        "report_sha256": hashlib.sha256(capture["body"].encode()).hexdigest(),
        "diagnostics_sha256": digest(capture["diagnostics"]),
        "reporting_sha256": digest(capture["reporting"]),
        "fixture": reservation["fixture"],
    }
    execution = capture.get("execution")
    if (
        digest(record) != digest(expected)
        or type(execution) is not dict
        or type(execution.get("schema_version")) is not int
        or execution["schema_version"] != 2
        or execution.get("reservation_digest") != digest(reservation)
    ):
        raise WorkflowError("V6 completion identity differs")
    launched, finished = activation.clock(record["launched"]), activation.clock(record["finished"])
    from claude_native_auth import finite

    elapsed = record["elapsed_seconds"]
    seconds = activation.slot(reservation["number"])["native_seconds"]
    if (
        not finite(elapsed)
        or elapsed < 0
        or not reservation["started"] <= launched <= finished
        or abs((finished - launched) - elapsed) > 1
    ):
        raise WorkflowError("V6 completion timing differs")
    return record, finished <= reservation[
        "deadline"
    ] and finished - launched <= seconds and elapsed <= seconds


OWNED = "v6-owned-capture.json"
SIDECAR = "v6-context-observation.json"


def observation_bindings(grant, meta, capture):
    """Recompute expected identities from independently loaded durable inputs."""
    return {
        "input_sha256": capture["input_digest"],
        "policy_sha256": capacity.sha(capacity.encoded(meta["review_policy"])),
        "execution_sha256": capacity.sha(capacity.encoded(capture["execution"])),
        "fixture_sha256": meta["v6_fixture"]["fixture_sha256"],
        "source_sha256": capacity.sha(capacity.encoded(grant["binding"]["harness"])),
        "descriptor_sha256": capacity.sha(observer._bytes(observer.DESCRIPTOR)),
        "report_sha256": capacity.sha(capture["body"].encode("utf-8")),
        "proof_sha256": capacity.sha(capacity.encoded(capture["reporting"])),
        "diagnostic_sha256": capacity.sha(capacity.encoded(capture["diagnostics"])),
    }


def persist_owned_capture(dispatch, meta, owned_auth, raw, workspace, session_id, body, diagnostics, proof):
    """Sanitized output is durable before any observer or completion refusal.

    Raw native events are transient. This function runs inside the existing
    snapshot, and never obtains another registration lock or retains raw events.
    """
    import os

    import claude_owned_auth

    owned = claude_owned_auth.require(owned_auth)
    directory = dispatch.directory
    # save_result writes the exact capture before assessing it. Its assessment
    # may refuse, but cannot erase a consumed invocation's sanitized output.
    review.save_result(
        directory, meta, body, diagnostics, meta["review_policy"]["cli"]["version"], reporting=proof
    )
    dispatch.finish(meta, session_id, body, diagnostics, proof)
    owned.recheck()
    grant, number = identity(dispatch.repo, directory, meta)
    capture = review.read_result_artifact(directory, "review-capture.json", meta)
    bindings = observation_bindings(grant, meta, capture)
    capture_bytes = review.exact_reporting_bytes(directory / "review-capture.json", 8000000)
    receipt = {
        "schema_version": 6,
        "bindings": bindings,
        "capture_sha256": capacity.sha(capture_bytes),
        "completion_sha256": digest(activation.read(directory / FINISHED)),
        "observation": None,
    }
    if number >= 22:
        sidecar, correlation = capacity.bridge(
            raw, directory / "packet", workspace, meta["review_policy"], session_id, bindings
        )
        # Write exact canonical observer bytes, never a reserialized projection.
        fd = os.open(plain_path(directory / SIDECAR), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(sidecar)
            stream.flush()
            os.fsync(stream.fileno())
        receipt["observation"] = {
            "completion": observer.completion(sidecar, capacity.sha(capture_bytes)),
            "correlation": correlation,
        }
    owned.recheck()
    exclusive(directory / OWNED, receipt, limit=100000)


def owned_capture(directory, meta, capture):
    """Offline integrity replay is not an attestation of discarded native events."""
    directory = Path(directory)
    if not plain_path(directory / OWNED).exists():
        raise WorkflowError("V6 diagnostic/capacity replay is not implemented: owned capture persistence")
    # No current credentials or network: the immutable application is storage.
    # The directory must be the exact namespace child already checked by identity.
    repo = SimpleNamespace(main=directory.parent.parent.parent)
    grant, _ = activation.load(repo)
    number = meta["reporting_activation"]["number"]
    activation.slot(number)
    if directory != activation.root(repo) / f"evidence-{number}":
        raise WorkflowError("V6 owned capture location differs")
    bindings = observation_bindings(grant, meta, capture)
    raw = review.exact_reporting_bytes(directory / "review-capture.json", 8000000)
    if digest(review_coverage.strict_json(raw.decode("utf-8"))) != digest(capture):
        raise WorkflowError("V6 durable capture differs")
    receipt = activation.read(directory / OWNED)
    observation = receipt.get("observation")
    expected = {
        "schema_version": 6,
        "bindings": bindings,
        "capture_sha256": capacity.sha(raw),
        "completion_sha256": digest(activation.read(directory / FINISHED)),
        "observation": observation,
    }
    if digest(receipt) != digest(expected):
        raise WorkflowError("V6 owned capture binding differs")
    if number < 22:
        if observation is not None or plain_path(directory / SIDECAR).exists():
            raise WorkflowError("Unexpected V6 capability observation")
        return None
    if type(observation) is not dict or set(observation) != {"completion", "correlation"}:
        raise WorkflowError("Missing V6 capacity observation")
    sidecar = review.exact_reporting_bytes(directory / SIDECAR, observer.MAX_BYTES)
    observer.replay_completed(
        sidecar,
        observation["completion"],
        capacity.sha(raw),
        bindings,
        capture["diagnostics"]["usage"]["counters"],
        observation["correlation"],
    )
    return {
        "sidecar": sidecar,
        "completion": observation["completion"],
        "capture": raw,
        "counters": capture["diagnostics"]["usage"]["counters"],
        "correlation": observation["correlation"],
    }


def assess_capture(directory, meta, capture, reservation):
    """Pure offline checks; full replay additionally requires owned_capture()."""
    number = meta["reporting_activation"]["number"]
    activation.slot(number)
    if (
        reservation.get("number") != number
        or type(reservation.get("number")) is not int
        or reservation.get("input_digest")
        != digest({k: v for k, v in meta.items() if k not in review.RESULT_FIELDS})
    ):
        raise WorkflowError("V6 capture reservation differs from exact metadata")
    if (
        capture.get("provider_version") != meta["review_policy"]["cli"]["version"]
        or capture.get("input_digest") != reservation["input_digest"]
    ):
        raise WorkflowError("V6 capture provider or input differs")
    execution = retained(directory, meta)
    if execution is None or digest(execution) != digest(capture.get("execution")):
        raise WorkflowError("V6 capture execution differs")
    timing, timing_ok = completion(directory, capture, reservation)
    proof = claude_reporting.replay(capture["reporting"])
    if proof["report"] != capture["body"]:
        raise WorkflowError("V6 exact report differs from frozen proof")
    diagnostics = copy.deepcopy(capture["diagnostics"])
    transport = "isolation-refusal" if number == 20 else "native-tools-and-source"
    if digest(diagnostics["telemetry"].get("diagnostic")) != digest(
        {"purpose": transport, "tool_contract_digest": digest(diagnostic_tool_contract.contract())}
    ):
        raise WorkflowError("V6 diagnostic transport differs")
    if number == 20:
        if (
            diagnostics["telemetry"].get("controlled_refusals") != 1
            or diagnostics["reasons"].count("controlled_refusal_diagnostic_only") != 1
        ):
            raise WorkflowError("V6 requires exactly one actual controlled refusal")
        diagnostics["reasons"].remove("controlled_refusal_diagnostic_only")
    elif diagnostics["telemetry"].get("controlled_refusals", 0) != 0:
        raise WorkflowError("V6 unexpected controlled refusal")
    partial.validate(
        diagnostics["telemetry"].get("partial_stream"),
        diagnostics["telemetry"]["types"].get("stream_event", 0),
    )
    if diagnostics["telemetry"]["partial_stream"]["schema_version"] != 2:
        raise WorkflowError("V6 requires unchanged structural observation schema2")
    assessment = review_coverage.assess(
        Path(directory) / "packet", capture["body"], diagnostics, policy=meta["review_policy"]
    )
    allocation = activation.slot(number)
    return (
        timing,
        assessment,
        (
            timing_ok
            and proof["accepted"]
            and assessment["qualified"]
            and known_usage(diagnostics["usage"], allocation["native_seconds"], allocation["reference_usd"])
        ),
    )


def replay(repo, grant, reservation, finished):
    replay_wall, replay_monotonic = activation.clock(time.time()), time.monotonic()
    number = reservation["number"]
    directory = activation.root(repo) / f"evidence-{number}"
    if not (directory / "review-capture.json").exists():
        raise WorkflowError(
            "V6 diagnostic/capacity replay is not implemented: no durable capture; uncertain, no retry"
        )
    meta = review.verify_packet(directory)
    identity(repo, directory, meta)
    predecessor(repo, number)
    capture = review.read_result_artifact(directory, "review-capture.json", meta)
    from claude_reporting_execution import validate_capture

    validate_capture(directory, meta, capture)
    review.qualification(directory)  # Exact retained report/proof/diagnostic artifacts, not PR readiness.
    timing, assessment, qualified = assess_capture(directory, meta, capture, reservation)
    if finished < timing["finished"] or finished - timing["finished"] > 180:
        raise WorkflowError("V6 offline replay allocation exceeded")
    owned_capture(directory, meta, capture)
    ended_wall, ended_monotonic = activation.clock(time.time()), time.monotonic()
    elapsed = ended_monotonic - replay_monotonic
    if (
        not 0 <= elapsed <= 180
        or not replay_wall <= ended_wall <= replay_wall + 180
        or abs((ended_wall - replay_wall) - elapsed) > 1
    ):
        raise WorkflowError("V6 independent replay exceeded its allocation or clock changed")
    return {
        "schema_version": 6,
        "number": number,
        "grant_digest": digest(grant),
        "reservation_digest": digest(reservation),
        "capture_digest": digest(capture),
        "assessment_digest": digest(assessment),
        "finished": finished,
        "qualified": qualified,
        "usage": copy.deepcopy(capture["diagnostics"]["usage"]),
    }


def recover(repo, *, number):
    activation.slot(number)
    # Materialize retained exact bytes only; never reconstruct missing native evidence.
    directory = activation.root(repo) / f"evidence-{number}"
    if (directory / "review-capture.json").exists():
        review.recover_review(repo, directory)
    # No auth/provider, reservation, or automatic successor.
    if (activation.root(repo) / f"outcome-{number}.json").exists():
        return activation.outcome(repo, number)
    return activation.complete(repo, number=number)


def run(repo, *, number):
    import claude_owned_auth
    from review_claude import execute

    activation.slot(number)
    # The final production catalog must exist before touching authentication.
    try:
        catalog(repo)
    except WorkflowError as error:
        raise WorkflowError("V6 actual owned-capture dispatch wiring requires final catalog") from error
    if (activation.root(repo) / f"attempt-{number}.json").exists():
        return recover(repo, number=number)
    predecessor(repo, number)
    directory = activation.root(repo) / f"evidence-{number}"
    if not directory.exists():
        prepare(repo, number=number)
    meta = review.verify_packet(directory)
    identity(repo, directory, meta)
    with claude_owned_auth.snapshot(meta["review_policy"]) as owned:
        reservation = activation.reserve(repo, number=number, input_digest=digest(meta), owned_auth=owned)
        dispatch = Dispatch(repo, directory, reservation)
        execute(repo, directory, meta, diagnostic=True, dispatch_context=dispatch, owned_auth=owned)
    return recover(repo, number=number)


def validate_profile(repo, directory, meta, dispatch, *, owned_auth):
    if (
        type(dispatch) is not Dispatch
        or not dispatch.claimed
        or dispatch.repo is not repo
        or dispatch.directory != Path(directory)
    ):
        raise WorkflowError("V6 profile requires its actual claimed Dispatch")
    grant, number = identity(repo, directory, meta)
    dispatch.recheck(meta, owned_auth=owned_auth)
    if digest(meta["review_policy"]) != digest(policies(grant["binding"]["policy"], number)):
        raise WorkflowError("V6 policy differs from claimed slot")
    if number >= 22:
        capacity.profile(meta["review_policy"])
    else:
        activation.validate_policy(meta["review_policy"])
    return "isolation-refusal" if number == 20 else "native-tools-and-source"
