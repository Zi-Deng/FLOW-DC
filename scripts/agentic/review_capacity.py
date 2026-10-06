"""Integration input planning, distinct from available archive and proof storage.

Byte envelopes and independently reviewed token estimates are different quantities.
Neither proves token fit or eventual completion; no tokenizer or provider is called.
"""

from tasks import plain_path
from workflow import WorkflowError

# An administrative input-envelope bound, NOT a model's context size in bytes.
MAX_ENVELOPE_BYTES = 16_000_000
CONTEXT_TOKENS = 1_000_000
MAX_OUTPUT_TOKENS = 128_000
GUIDANCE = (
    "START.txt",
    "review-policy.txt",
    "repository-policy.txt",
    "domain-policy.txt",
    "report-schema.json",
    "capability/fixture.txt",
)


class IntegrationCapacityExceeded(WorkflowError):
    """Intact evidence exceeds its planning allocation; retain an incomplete report."""


def guidance_bytes(directory):
    return sum(plain_path(directory / "packet" / name).stat().st_size for name in GUIDANCE)


def estimate(directory, planned, limits):
    import review_batch_v7 as batch
    import review_navigation
    from review_prompt import native

    meta = batch.api().verify_packet(directory)
    imports = planned.get("continuation", {}).get("snapshot", {}).get("imports", [])
    actual = sum(
        (batch.unit_path(directory, unit) / "review.md").stat().st_size
        for unit in planned["units"][:-1]
        if unit["id"] in imports
    )
    remaining = len(planned["units"]) - 1 - len(imports)
    schema_bytes = len(planned["policy"]["reporting"]["schema_text"].encode("utf-8"))
    prompt_bytes = len(native(directory, meta).encode("utf-8"))
    mandatory = planned["units"][-1]["required_volume"]["bytes"]
    guidance = guidance_bytes(plain_path(directory))
    report_bytes = actual + remaining * limits["max_report_bytes"]
    return {
        "schema_version": 2,
        "available_source_test_context_bytes": planned["resources"]["context_bytes"],
        "mandatory_cross_boundary_bytes": mandatory,
        "mandatory_guidance_bytes": guidance,
        "retained_exact_report_bytes": actual,
        "remaining_component_count": remaining,
        "worst_remaining_report_bytes": remaining * limits["max_report_bytes"],
        "integration_report_bytes": report_bytes,
        "navigation_storage_ceiling_bytes": review_navigation.MAX_NAVIGATION_BYTES,
        "schema_bytes": schema_bytes,
        "prompt_bytes": prompt_bytes,
        "input_envelope_bytes": mandatory + guidance + report_bytes + schema_bytes + prompt_bytes,
        "report_output_bytes": limits["max_report_bytes"],
        "proof_storage_bytes": planned["policy"]["reporting"]["limits"]["proof_bytes"],
        "integration_seconds": limits["unit_seconds"],
        "integration_reference_usd": limits["unit_cost"],
        "wrapper_invocations": len(batch.work_units(planned)),
        "token_fit_verified": False,
    }


def validate(authorization, executable):
    value = authorization.get("integration_capacity")
    if type(value) is not dict or set(value) != {
        "model",
        "input_utf8_bytes",
        "protocol_overhead_bytes",
        "output_utf8_bytes",
        "optional_source_bytes",
        "navigation_input_bytes",
        "context_tokens",
        "estimated_input_tokens",
        "reserved_output_tokens",
        "evidence",
    }:
        raise WorkflowError("Explicit reviewed integration model-input capacity is required")
    if (
        value["model"] != executable["policy"]["model"]
        or not isinstance(value["evidence"], str)
        or not value["evidence"].strip()
    ):
        raise WorkflowError("Integration capacity lacks exact model/evidence binding")
    for key in set(value) - {"model", "evidence"}:
        if type(value[key]) is not int or value[key] <= 0:
            raise WorkflowError("Integration capacity requires positive finite byte/token limits")
    if (
        value["input_utf8_bytes"] > MAX_ENVELOPE_BYTES
        or value["context_tokens"] != CONTEXT_TOKENS
        or value["reserved_output_tokens"] > MAX_OUTPUT_TOKENS
        or value["estimated_input_tokens"] + value["reserved_output_tokens"] > CONTEXT_TOKENS
    ):
        raise WorkflowError("Integration capacity exceeds bounded byte/token planning limits")
    estimate = executable["integration_capacity_estimate"]
    if (
        envelope(estimate["input_envelope_bytes"], value) > value["input_utf8_bytes"]
        or estimate["report_output_bytes"] > value["output_utf8_bytes"]
    ):
        raise WorkflowError("Integration cannot fit the reviewed model-input/output envelope")


def envelope(mandatory, bound):
    return mandatory + sum(
        bound[k] for k in ("optional_source_bytes", "navigation_input_bytes", "protocol_overhead_bytes")
    )


def actual(directory, batch):
    """Check mandatory assigned ranges and exact reports; keep all context accessible."""
    import review
    import review_coverage
    from review_prompt import native

    target = plain_path(directory)
    meta = review.verify_packet(target)
    inventory = review_coverage.read_json(target / "packet/required-material.json")["required"]
    assigned = set(meta["batch_unit"]["required_ids"])
    material = sum(item["bytes"] for item in inventory if item["id"] in assigned)
    total = (
        material
        + guidance_bytes(target)
        + len(native(target, meta).encode("utf-8"))
        + len(meta["review_policy"]["reporting"]["schema_text"].encode("utf-8"))
    )
    bound = batch["authorization"]["integration_capacity"]
    if envelope(total, bound) > bound["input_utf8_bytes"]:
        raise IntegrationCapacityExceeded("Actual integration material exceeds reviewed model-input capacity")


def observed(directory, batch):
    """Bound evidenced optional reads, including repeated mandatory ranges.

    This counts exact source bytes in retained spans, not provider framing or
    tokens. The separately reserved protocol allowance and token estimate remain
    assumptions; masked/unsupported results cannot become source evidence here.
    """
    import review
    import review_coverage

    target = plain_path(directory)
    meta = review.verify_packet(target)
    assigned = set(meta["batch_unit"]["required_ids"])
    rows = review_coverage.read_json(target / "packet/required-material.json")["required"]
    mandatory = set()
    for row in rows:
        if row["id"] in assigned and not row.get("omitted"):
            mandatory.update((row["artifact"], n) for n in range(row["start_line"], row["end_line"] + 1))
    for name in GUIDANCE:
        lines = plain_path(target / "packet" / name).read_bytes().splitlines(keepends=True)
        mandatory.update((name, n) for n in range(1, len(lines) + 1))
    seen, cache = set(), {}
    optional, navigation = 0, 0
    diagnostics = review_coverage.read_json(target / "diagnostics.json")
    review_coverage.validate_diagnostics(diagnostics, target / "packet", meta["review_policy"])
    for event in diagnostics["events"]:
        for span in event["spans"]:
            artifact = span["artifact"]
            if artifact not in cache:
                cache[artifact] = (
                    plain_path(target / "packet" / artifact).read_bytes().splitlines(keepends=True)
                )
            for n in range(span["start_line"], span["end_line"] + 1):
                key = (artifact, n)
                size = len(cache[artifact][n - 1])
                if artifact.startswith("navigation/"):
                    navigation += size
                elif key not in mandatory or key in seen:
                    optional += size
                seen.add(key)
    bound = batch["authorization"]["integration_capacity"]
    if optional > bound["optional_source_bytes"] or navigation > bound["navigation_input_bytes"]:
        raise IntegrationCapacityExceeded(
            "Integration observed reads exceed optional source/navigation allocation"
        )
    return {"optional_source_bytes": optional, "navigation_input_bytes": navigation}
