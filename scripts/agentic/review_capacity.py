"""Finite integration capacity accounting for prospective native batches.

The coordinator supplies the independently reviewed model-input envelope. Storage
bounds or this arithmetic alone cannot certify token capacity or eventual model
completion. Nothing here reduces a report bound or adds an integration process.
"""

from tasks import plain_path
from workflow import WorkflowError


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
    # Conservative initial envelope: full original source/test context, all exact
    # report bytes, the finite navigation ceiling, schema and trusted prompt.
    prompt_bytes = len(native(directory, meta).encode("utf-8"))
    source_bytes = planned["resources"]["context_bytes"]
    report_bytes = actual + remaining * limits["max_report_bytes"]
    return {
        "schema_version": 1,
        "source_test_context_bytes": source_bytes,
        "retained_exact_report_bytes": actual,
        "remaining_component_count": remaining,
        "worst_remaining_report_bytes": remaining * limits["max_report_bytes"],
        "integration_report_bytes": report_bytes,
        "navigation_ceiling_bytes": review_navigation.MAX_NAVIGATION_BYTES,
        "schema_bytes": schema_bytes,
        "prompt_bytes": prompt_bytes,
        "input_envelope_bytes": source_bytes
        + report_bytes
        + review_navigation.MAX_NAVIGATION_BYTES
        + schema_bytes
        + prompt_bytes,
        "report_output_bytes": limits["max_report_bytes"],
        "proof_storage_bytes": planned["policy"]["reporting"]["limits"]["proof_bytes"],
        "integration_seconds": limits["unit_seconds"],
        "integration_reference_usd": limits["unit_cost"],
        "wrapper_invocations": len(batch.work_units(planned)),
    }


def validate(authorization, executable):
    value = authorization.get("integration_capacity")
    if type(value) is not dict or set(value) != {
        "model",
        "input_utf8_bytes",
        "protocol_overhead_bytes",
        "output_utf8_bytes",
        "evidence",
    }:
        raise WorkflowError("Explicit reviewed integration model-input capacity is required")
    if (
        value["model"] != executable["policy"]["model"]
        or not isinstance(value["evidence"], str)
        or not value["evidence"].strip()
    ):
        raise WorkflowError("Integration capacity lacks exact model/evidence binding")
    for key in ("input_utf8_bytes", "protocol_overhead_bytes", "output_utf8_bytes"):
        if type(value[key]) is not int or value[key] <= 0:
            raise WorkflowError("Integration capacity requires positive finite byte limits")
    estimate = executable["integration_capacity_estimate"]
    if (
        estimate["input_envelope_bytes"] + value["protocol_overhead_bytes"] > value["input_utf8_bytes"]
        or estimate["report_output_bytes"] > value["output_utf8_bytes"]
    ):
        raise WorkflowError("Integration cannot fit the reviewed model-input/output envelope")


def actual(directory, batch):
    """Recheck the materialized integration without truncating any dependency."""
    import review
    import review_coverage
    from review_prompt import native

    target = plain_path(directory)
    meta = review.verify_packet(target)
    inventory = review_coverage.read_json(target / "packet/required-material.json")["required"]
    material = sum(item.get("bytes", 0) for item in inventory)
    navigation = sum(
        plain_path(path).stat().st_size
        for path in (target / "packet/navigation").rglob("*")
        if path.is_file()
    )
    total = (
        material
        + navigation
        + len(native(target, meta).encode("utf-8"))
        + len(meta["review_policy"]["reporting"]["schema_text"].encode("utf-8"))
    )
    bound = batch["authorization"]["integration_capacity"]
    if total + bound["protocol_overhead_bytes"] > bound["input_utf8_bytes"]:
        raise WorkflowError("Actual integration material exceeds reviewed model-input capacity")
