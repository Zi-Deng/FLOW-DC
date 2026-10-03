#!/usr/bin/env python3
"""Frozen issue-31 schema-3 storage and publication; no inference entrypoint.

Source: 8f274ac129ac380c69962d3f38be33eb66441340.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import review_batch_v4 as review_batch
import review_coverage_issue31_v3 as coverage
import review_coverage_v1 as legacy_coverage
from copilot_policy import CLI_VERSION as CLI_VERSION
from tasks import atomic_json, atomic_text, plain_path
from tasks import digest as value_digest
from workflow import WorkflowError

TEXT_SUFFIXES = {
    ".py",
    ".md",
    ".txt",
    ".json",
    ".yml",
    ".yaml",
    ".toml",
    ".ini",
    ".cfg",
    ".sh",
    ".js",
    ".ts",
    ".tsx",
    ".jsx",
    ".html",
    ".css",
    ".sql",
    ".rs",
    ".go",
    ".c",
    ".h",
    ".cpp",
    ".java",
    ".xml",
    ".tex",
    ".r",
    ".R",
}
PRIVATE_PARTS = {"memory", ".agentic-local", ".env", ".ssh", "data", "checkpoints", "weights", "wandb"}
# FLOW-DC data and historical trees, matched at repository-relative boundaries.
# Maintained example configs and benchmark source remain available for review.
PRIVATE_PATHS = (
    "files/input",
    "files/output",
    "files/biotrove_train_stats.json",
    "benchmark/manifests",
    "benchmark/results",
    "playground",
    "archives",
)


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def current_pr(repo, number, head, base):
    pr = repo.pr(number)
    if pr["state"] != "open" or pr.get("merged"):
        raise WorkflowError("Review requires an open PR")
    if pr["head"]["sha"] != head or pr["base"]["sha"] != base:
        raise WorkflowError("PR head or base changed; prepare and review a fresh snapshot")
    return pr


def verify_packet(directory):
    directory = plain_path(directory)
    metadata = coverage.read_json(plain_path(directory / "metadata.json"))
    packet = directory / "packet"
    actual = {str(p.relative_to(packet)): digest(p) for p in packet.rglob("*") if p.is_file()}
    if any(p.is_symlink() for p in packet.rglob("*")) or actual != metadata["files"]:
        raise WorkflowError("Review packet changed after preparation")
    return metadata


RESULT_FIELDS = {"review_sha256", "copilot_version", "diagnostics_sha256", "coverage_sha256"}


def stored_result(directory, meta):
    result = coverage.read_json(plain_path(Path(directory) / "review-result.json"))
    inputs = {key: value for key, value in meta.items() if key not in RESULT_FIELDS}
    if (
        not isinstance(result, dict)
        or type(result.get("schema_version")) is not int
        or result["schema_version"] != meta.get("schema_version")
        or result["schema_version"] not in {1, 2, 3}
        or result.get("input_digest") != value_digest(inputs)
        or not isinstance(result.get("body"), str)
        or not result["body"].strip()
        or not isinstance(result.get("copilot_version"), str)
        or not result["copilot_version"].strip()
        or coverage.checksum(result["body"]) != result.get("review_sha256")
    ):
        raise WorkflowError("Saved review result changed or belongs to another packet")
    if result["schema_version"] in {2, 3}:
        diagnostics = result.get("diagnostics")
        if value_digest(diagnostics) != result.get("diagnostics_sha256"):
            raise WorkflowError("Saved diagnostics changed")
        policy = legacy_coverage if result["schema_version"] == 2 else coverage
        assessment = policy.assess(Path(directory) / "packet", result["body"], diagnostics)
        if value_digest(assessment) != result.get("coverage_sha256"):
            raise WorkflowError("Saved coverage changed")
    else:
        assessment = {"qualified": False, "reasons": ["legacy_report_without_coverage"]}
    if result["schema_version"] == 3:
        capture = coverage.read_json(plain_path(Path(directory) / "review-capture.json"))
        if capture != {key: value for key, value in result.items() if key != "coverage_sha256"}:
            raise WorkflowError("Exact review capture changed or is missing")
    return result, assessment


def qualification(directory, *, require=False):
    """Shared gate used by recovery, publication, managed designation and preflight."""
    directory = plain_path(directory)
    meta = verify_packet(directory)
    if require:
        raise WorkflowError("Historical issue-31 evidence cannot establish current readiness")
    if meta.get("schema_version") == review_batch.SCHEMA:
        return review_batch.qualification(directory, require)
    if require and meta.get("batch_unit"):
        raise WorkflowError("A batch unit cannot independently qualify the parent review")
    result, assessment = stored_result(directory, meta)
    for name, key in (("review.md", "review_sha256"),):
        if not (directory / name).is_file() or digest(plain_path(directory / name)) != meta.get(key):
            raise WorkflowError("Review report changed or is incomplete")
    if result["schema_version"] in {2, 3}:
        for name, key in (("diagnostics.json", "diagnostics_sha256"), ("coverage.json", "coverage_sha256")):
            value = coverage.read_json(plain_path(directory / name))
            if value_digest(value) != result[key] or meta.get(key) != result[key]:
                raise WorkflowError("Coverage or diagnostics changed or are missing")
    if require and result["schema_version"] != 3:
        raise WorkflowError("Legacy review policy cannot establish current coverage readiness")
    if require and not assessment["qualified"]:
        raise WorkflowError(
            "Review coverage is incomplete; observed capability and every required material are necessary"
        )
    return assessment


def coverage_ready(directory):
    """Frozen assessments reproduce history, never current readiness."""
    qualification(directory)
    return False


def recover_review(repo, directory):
    """Finalize a durably saved exact result without another model request."""
    directory = plain_path(directory)
    if verify_packet(directory).get("schema_version") == review_batch.SCHEMA:
        return review_batch.execute(repo, directory, recover_only=True)
    result_path = plain_path(directory / "review-result.json")
    capture_path = plain_path(directory / "review-capture.json")
    if not result_path.exists() and not capture_path.exists():
        return None
    meta = verify_packet(directory)
    if repo.name != meta["repository"]:
        raise WorkflowError("Review packet belongs to another repository")
    current_pr(repo, meta["pr"], meta["head_sha"], meta["base_sha"])
    if not result_path.exists():
        capture = coverage.read_json(capture_path)
        if (
            meta.get("schema_version") != 3
            or not isinstance(capture, dict)
            or capture.get("input_digest") != value_digest(meta)
        ):
            raise WorkflowError("Pending review capture belongs to another packet")
        save_result(
            directory, meta, capture.get("body"), capture.get("diagnostics"), capture.get("copilot_version")
        )
    result, assessment = stored_result(directory, meta)
    report = plain_path(directory / "review.md")
    if meta.get("review_sha256"):
        if (
            meta["review_sha256"] != result["review_sha256"]
            or meta.get("copilot_version") != result["copilot_version"]
        ):
            raise WorkflowError("Completed review report or metadata changed")
        qualification(directory)
        return report
    atomic_text(report, result["body"])
    if result["schema_version"] in {2, 3}:
        atomic_json(directory / "diagnostics.json", result["diagnostics"])
        atomic_json(directory / "coverage.json", assessment)
        meta.update(
            diagnostics_sha256=result["diagnostics_sha256"], coverage_sha256=result["coverage_sha256"]
        )
    meta.update(review_sha256=result["review_sha256"], copilot_version=result["copilot_version"])
    atomic_json(directory / "metadata.json", meta)
    return report


def save_result(directory, meta, body, diagnostics, version):
    directory = Path(directory)
    if meta.get("schema_version") != 3 or not isinstance(body, str) or not body.strip():
        raise WorkflowError("New results require a current packet and exact nonempty report")
    capture = {
        "schema_version": 3,
        "input_digest": value_digest(meta),
        "body": body,
        "review_sha256": coverage.checksum(body),
        "copilot_version": version,
        "diagnostics": diagnostics,
        "diagnostics_sha256": value_digest(diagnostics),
    }
    capture_path = plain_path(directory / "review-capture.json")
    if capture_path.exists():
        if coverage.read_json(capture_path) != capture:
            raise WorkflowError("Pending exact review capture changed")
    else:
        # Durable before assessment reads any packet file. A transient storage
        # failure can be recovered without another paid provider invocation.
        atomic_json(capture_path, capture)
    assessment = coverage.assess(directory / "packet", body, diagnostics)
    atomic_json(directory / "review-result.json", {**capture, "coverage_sha256": value_digest(assessment)})


def report_marker(meta):
    unit = meta.get("batch_unit")
    suffix = f":{unit['batch_sha256']}:{unit['unit']['id']}" if unit else ""
    return f"<!-- agentic-review:{meta['head_sha']}:{meta['review_sha256']}{suffix} -->"


def publication_body(directory):
    meta = verify_packet(directory)
    assessment = qualification(directory)
    body = (Path(directory) / "review.md").read_bytes().decode("utf-8")
    if meta.get("schema_version") == review_batch.SCHEMA:
        return review_batch.publication_body(directory)
    label = (
        "coverage-qualified static inspection"
        if assessment["qualified"]
        else "INCOMPLETE static inspection — not ready"
    )
    if meta.get("batch_unit"):
        status = f"{label}; " if meta["batch_unit"].get("publication_version") == 2 else ""
        label = f"batch unit {meta['batch_unit']['unit']['id']} — {status}parent readiness requires aggregate qualification"
    header = (
        f"## Independent Copilot CLI review\n\nPR #{meta['pr']} · reviewed head `{meta['head_sha']}` "
        f"· base `{meta['base_sha']}`\n\nRequested model: `{meta['requested_model']}`. "
        f"Status: **{label}**. Model output below is preserved exactly. "
        "This is not human approval. The reviewer executed no tests. CI association and tested checkout "
        "are separately recorded in validation.json; unknown execution details remain unknown.\n\n"
    )
    marker = report_marker(meta)
    binding = f"<!-- agentic-coverage:v{1 if meta['schema_version'] < 3 else 2}:{value_digest(assessment)}:{meta.get('diagnostics_sha256', 'legacy')} -->"
    return header + body + "\n\n" + marker + "\n" + binding


def verified_published(repo, directory, number, head, base):
    meta = verify_packet(directory)
    if any(
        meta.get(key) != value
        for key, value in {
            "repository": repo.name,
            "pr": number,
            "head_sha": head,
            "base_sha": base,
        }.items()
    ):
        raise WorkflowError("Review coverage record is stale or belongs to another PR")
    qualification(directory, require=True)
    if meta.get("schema_version") == review_batch.SCHEMA:
        review_batch.verify_unit_publications(repo, directory)
    expected = publication_body(directory)
    matching = [
        item
        for item in repo.api(f"pulls/{number}/reviews", paginate=True)
        if item.get("commit_id") == head and item.get("state") == "COMMENTED" and item.get("body") == expected
    ]
    if len(matching) != 1:
        raise WorkflowError("No unique published coverage-qualified review matches the exact saved report")
    return matching[0]


def verify_publication(repo, directory):
    """Read-only byte comparison, including historical reports; never inference."""
    meta = verify_packet(directory)
    if meta["repository"] != repo.name:
        raise WorkflowError("Review packet belongs to another repository")
    report = plain_path(Path(directory) / "review.md")
    if digest(report) != meta.get("review_sha256"):
        raise WorkflowError("Saved review bytes changed")
    if meta.get("schema_version") == 1:
        expected = (
            report.read_bytes().decode("utf-8")
            + f"\n<!-- agentic-review:{meta['head_sha']}:{meta['review_sha256']} -->"
        )
    else:
        expected = publication_body(directory)
    if meta.get("schema_version") == review_batch.SCHEMA:
        review_batch.verify_unit_publications(repo, directory, complete_only=False)
    marker = report_marker(meta)
    matches = [
        item
        for item in repo.api(f"pulls/{meta['pr']}/reviews", paginate=True)
        if marker in (item.get("body") or "") and item.get("commit_id") == meta["head_sha"]
    ]
    if len(matches) != 1 or matches[0].get("body") != expected:
        raise WorkflowError(
            "Exact saved/published review comparison failed; no model rerun or record rewrite"
        )
    return {
        "exact_match": True,
        "review_id": matches[0].get("id"),
        "head_sha": meta["head_sha"],
        "body_sha256": coverage.checksum(expected),
        "body_bytes": len(expected.encode("utf-8")),
        "coverage_qualified": False if meta.get("schema_version") == 1 else coverage_ready(directory),
    }


def publish(repo, directory):
    directory = plain_path(directory)
    meta = verify_packet(directory)
    body = publication_body(directory)
    if repo.name != meta["repository"]:
        raise WorkflowError("Wrong repository for review publication")
    if len(body.encode("utf-8")) > 60000:
        raise WorkflowError("Review exceeds the publication budget; summarize separately with attribution")
    current_pr(repo, meta["pr"], meta["head_sha"], meta["base_sha"])
    marker = report_marker(meta)
    if meta.get("schema_version") == review_batch.SCHEMA:
        review_batch.publish_units(repo, directory)
    reviews = repo.api(f"pulls/{meta['pr']}/reviews", paginate=True)
    for existing in reviews:
        if marker in (existing.get("body") or ""):
            if (
                existing.get("body") != body
                or existing.get("commit_id") != meta["head_sha"]
                or existing.get("state") != "COMMENTED"
            ):
                raise WorkflowError("Existing publication marker has different content, head or state")
            return {"existing_review": existing["html_url"]}
    posted = repo.api(
        f"pulls/{meta['pr']}/reviews",
        data={
            "commit_id": meta["head_sha"],
            "event": "COMMENT",
            "body": body,
        },
    )
    return {"review": posted["html_url"], "commit_id": meta["head_sha"]}
