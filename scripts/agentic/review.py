#!/usr/bin/env python3
"""Prepare a bound PR snapshot, run a fresh selected provider, publish on request."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import shutil
import subprocess
import sys
import uuid
from pathlib import Path, PurePosixPath

import ci_evidence
import review_batch
import review_coverage as coverage
import review_coverage_v1 as legacy_coverage
import review_coverage_v2 as schema3_coverage
import review_coverage_v5 as schema5_coverage
import review_coverage_v6 as schema6_coverage
import review_issue31_v3 as issue31_history
import review_packet
import review_policy
from tasks import atomic_json, atomic_text, plain_path, private_directory
from tasks import digest as value_digest
from workflow import Repo, WorkflowError, configuration, positive, run, sha, write_json

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
    ".mjs",
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


def private_path(name):
    path = PurePosixPath(name)
    return (
        bool(PRIVATE_PARTS.intersection(path.parts))
        or any(path.is_relative_to(prefix) for prefix in PRIVATE_PATHS)
        or path.name.startswith(".env")
        or path.suffix in {".pem", ".key"}
    )


def tree(repo, commit):
    result = run(["git", "-C", repo.root, "ls-tree", "-r", "-l", "-z", commit]).stdout
    entries = []
    for record in result.rstrip("\0").split("\0"):
        if not record:
            continue
        meta, name = record.split("\t", 1)
        mode, kind, oid, size = meta.split()
        entries.append(
            {"path": name, "mode": mode, "kind": kind, "oid": oid, "size": int(size) if size != "-" else 0}
        )
    return entries


def snapshot(repo, commit, output, limits):
    """Read Git blobs directly. Never checkout, follow symlinks, or load PR settings."""
    output.mkdir()
    manifest = []
    total = 0
    for index, item in enumerate(tree(repo, commit)):
        name = item["path"]
        reason = None
        if private_path(name):
            reason = "private/data path excluded"
        elif item["kind"] != "blob" or item["mode"] not in {"100644", "100755"}:
            reason = "symlink/submodule/nonregular entry excluded"
        elif Path(name).suffix not in TEXT_SUFFIXES and Path(name).name not in {
            "Makefile",
            "Dockerfile",
            ".gitignore",
        }:
            reason = "unsupported text type/binary excluded"
        elif item["size"] > limits["max_source_file_bytes"]:
            reason = "file exceeds configured size limit"
        if reason:
            manifest.append({"path": name, "omitted": reason})
            continue
        total += item["size"]
        if total > limits["max_snapshot_bytes"]:
            raise WorkflowError(
                "Source snapshot exceeds budget; reduce scope or explicitly revise the size limit"
            )
        blob = subprocess.run(
            ["git", "-C", str(repo.root), "cat-file", "blob", item["oid"]], capture_output=True, check=True
        ).stdout
        try:
            text = blob.decode("utf-8")
            if "\0" in text:
                raise UnicodeError("binary")
        except UnicodeError:
            manifest.append({"path": name, "omitted": "not UTF-8 text"})
            continue
        # Numeric filenames neutralize discovery of AGENTS, hooks, skills and MCP config.
        target = output / f"{index:06d}.txt"
        target.write_text(text, encoding="utf-8")
        manifest.append({"path": name, "snapshot": f"{output.name}/{target.name}", "blob": item["oid"]})
    return manifest


def current_pr(repo, number, head, base):
    pr = repo.pr(number)
    if pr["state"] != "open" or pr.get("merged"):
        raise WorkflowError("Review requires an open PR")
    if pr["head"]["sha"] != head or pr["base"]["sha"] != base:
        raise WorkflowError("PR head or base changed; prepare and review a fresh snapshot")
    return pr


def prepare(
    repo,
    number,
    issue_number,
    plan_comment,
    expected_head=None,
    output=None,
    prior_review=None,
    *,
    review_provider=None,
    review_model=None,
    review_effort=None,
    reporting=None,
):
    number, issue_number, plan_comment = map(positive, (number, issue_number, plan_comment))
    cfg = configuration(repo.root)
    selection = review_policy.resolve(
        repo, cfg, review_provider=review_provider, review_model=review_model, review_effort=review_effort
    )
    from claude_native_auth import bind

    selection["policy"] = bind(selection["policy"])
    if reporting is not None:
        from claude_reporting_policy import build

        if type(reporting) is not dict or set(reporting) != {"max_turns", "limits"}:
            raise WorkflowError("Explicit reporting turn and retention limits are required")
        selection["policy"] = build(selection["policy"], **reporting)
    pr = repo.pr(number)
    head, base = sha(pr["head"]["sha"]), sha(pr["base"]["sha"])
    if expected_head and head != sha(expected_head):
        raise WorkflowError("Supplied reviewed SHA is not the current PR head")
    current_pr(repo, number, head, base)
    if not pr["head"].get("repo") or pr["head"]["repo"]["full_name"] != repo.name:
        raise WorkflowError(
            "Automated review supports same-repository PRs only; use a separately isolated review for forks"
        )
    if pr["base"]["ref"] != repo.base:
        raise WorkflowError("PR must target the default branch")
    issue = repo.api(f"issues/{issue_number}")
    if "pull_request" in issue:
        raise WorkflowError("Issue number refers to a PR")
    plan = repo.api(f"issues/comments/{plan_comment}")
    if plan["issue_url"].rstrip("/") != issue["url"].rstrip("/"):
        raise WorkflowError("The plan comment does not belong to the specified issue")
    comments = repo.api(f"issues/{issue_number}/comments", paginate=True)
    # Fetch full ancestry: GitHub's PR diff is the merge-base-to-head comparison.
    repo.fetch(f"refs/heads/{repo.base}", f"refs/pull/{number}/head")
    repo.git("cat-file", "-e", f"{head}^{{commit}}")
    repo.git("cat-file", "-e", f"{base}^{{commit}}")
    ancestor = repo.git("merge-base", base, head)
    names = (
        run(["git", "-C", repo.root, "diff", "--no-renames", "--name-only", "-z", ancestor, head])
        .stdout.rstrip("\0")
        .split("\0")
    )
    if any(private_path(name) for name in names):
        raise WorkflowError("Diff touches excluded private/data paths; do not transmit it to the reviewer")
    diff = run(
        ["git", "-C", repo.root, "diff", "--no-ext-diff", "--no-textconv", "--no-renames", ancestor, head]
    ).stdout
    if not diff.strip():
        raise WorkflowError("Diff is empty; there is no committed change to review")
    diff_limit = cfg["max_diff_bytes"]
    if diff_limit is not None and len(diff.encode("utf-8")) > diff_limit:
        raise WorkflowError("Diff exceeds max_diff_bytes; split the PR or explicitly adjust the cap")
    prior = None
    if prior_review:
        previous = plain_path(prior_review)
        old = verify_packet(previous)
        if any(
            old.get(key) != value
            for key, value in {
                "repository": repo.name,
                "pr": number,
                "issue": issue_number,
                "plan_comment": plan_comment,
            }.items()
        ):
            raise WorkflowError("Prior review repository/PR/contract binding differs")
        old_context = coverage.read_json(previous / "packet/context.json")
        if old_context["issue"].get("body") != issue.get("body") or old_context[
            "designated_plan_comment"
        ].get("body") != plan.get("body"):
            raise WorkflowError("Prior review contract changed; prepare a full fresh review")
        if run(
            ["git", "-C", repo.root, "merge-base", "--is-ancestor", old["head_sha"], head], check=False
        ).returncode:
            raise WorkflowError("Prior review head is not an ancestor of current head")
        if old.get("schema_version") not in {2, 3, 4, 5, 6}:
            raise WorkflowError(
                "Legacy prior review has no required-material inventory; use a full fresh packet"
            )
        prior = (previous, old, qualification(previous))
    directory = (
        plain_path(output)
        if output
        else repo.main / ".agentic-local/reviews" / f"pr-{number}-{head[:12]}-{uuid.uuid4().hex[:8]}"
    )
    private_directory(repo.main / ".agentic-local")
    private_directory(directory, exist_ok=False)
    packet = directory / "packet"
    packet.mkdir()
    manifest = snapshot(repo, head, packet / "source", cfg)
    base_manifest = snapshot(repo, ancestor, packet / "base-source", cfg)
    write_json(packet / "source-index.json", manifest)
    write_json(packet / "base-source-index.json", base_manifest)
    (packet / "diff.txt").write_text(diff, encoding="utf-8")
    context = {
        "pull_request": pr,
        "issue": issue,
        "designated_plan_comment": plan,
        "issue_comments": comments,
        "pr_comments": repo.api(f"issues/{number}/comments", paginate=True),
        "inline_comments": repo.api(f"pulls/{number}/comments", paginate=True),
        "reviews": repo.api(f"pulls/{number}/reviews", paginate=True),
        "check_runs": repo.api(
            f"commits/{head}/check-runs?per_page=100", paginate=True, page_key="check_runs"
        ),
        "note": "Plan designation is supplied by the operator. Independently check its approval and scope. Check data is a point-in-time snapshot.",
    }
    context["commit_statuses"] = repo.api(f"commits/{head}/statuses?per_page=100", paginate=True)
    context["hosted_receipts"] = ci_evidence.collect(repo, head, context["check_runs"], base)
    write_json(packet / "context.json", context)
    # Trusted policy comes from the caller's clean default-branch checkout, never PR code.
    for source, target in [
        ("AGENTS.md", "repository-policy.txt"),
        ("docs/agent-workflow/REVIEW.md", "review-policy.txt"),
        (cfg["domain_rubric"], "domain-policy.txt"),
    ]:
        (packet / target).write_text((repo.root / source).read_text(encoding="utf-8"), encoding="utf-8")
    if selection["policy"]["provider"] == "copilot":
        agents = packet / ".github/agents"
        agents.mkdir(parents=True)
        shutil.copyfile(
            repo.root / ".github/agents/independent-reviewer.agent.md",
            agents / "independent-reviewer.agent.md",
        )
    shutil.copyfile(repo.root / ".agentic/schemas/review-report.json", packet / "report-schema.json")
    review_packet.build(
        repo,
        packet,
        head,
        ancestor,
        manifest,
        base_manifest,
        context,
        cfg,
        prior,
        provider=selection["policy"]["provider"],
    )
    files = {str(p.relative_to(packet)): digest(p) for p in packet.rglob("*") if p.is_file()}
    metadata = {
        "schema_version": 7 if reporting is not None else 6,
        "kind": "single",
        "review_policy": selection["policy"],
        "selection_sources": selection["sources"],
        "repository": repo.name,
        "pr": number,
        "issue": issue_number,
        "plan_comment": plan_comment,
        "head_sha": head,
        "base_sha": base,
        "merge_base_sha": ancestor,
        "created_at": dt.datetime.now(dt.UTC).isoformat(),
        "files": files,
        "requested_model": selection["policy"]["model"],
        "config": cfg,
    }
    write_json(directory / "metadata.json", metadata)
    current_pr(repo, number, head, base)
    return directory


def verify_packet(directory):
    directory = plain_path(directory)
    metadata = coverage.read_json(plain_path(directory / "metadata.json"))
    if type(metadata.get("schema_version")) is not int or metadata["schema_version"] not in {
        1,
        2,
        3,
        4,
        5,
        6,
        7,
    }:
        raise WorkflowError("Unsupported review packet schema")
    if metadata["schema_version"] == 6 and metadata.get("kind") not in {
        "single",
        "batch-parent",
        "batch-unit",
    }:
        raise WorkflowError("Unsupported schema-6 packet kind")
    if metadata["schema_version"] == 4 and (metadata.get("kind") or metadata.get("review_policy")):
        raise WorkflowError("Historical schema-4 packet has prospective fields")
    if metadata["schema_version"] in {5, 6, 7}:
        review_policy.validate_policy(metadata.get("review_policy"))
        if metadata.get("requested_model") != metadata["review_policy"]["model"]:
            raise WorkflowError("Packet model differs from immutable review policy")
    if metadata["schema_version"] == 7:
        if metadata.get("kind") != "single" or metadata["review_policy"].get("schema_version") != 2:
            raise WorkflowError("Prospective reporting currently requires a schema-7 single packet")
        expected = metadata["review_policy"]["reporting"]["report_schema_sha256"]
        if metadata["files"].get("report-schema.json") != expected:
            raise WorkflowError("Packet reporting schema differs from its policy")
    elif metadata.get("review_policy", {}).get("schema_version") == 2 or any(
        key in metadata for key in ("reporting_sha256", "terminal_sha256")
    ):
        raise WorkflowError("Structured reporting cannot reinterpret historical metadata")
    packet = directory / "packet"
    actual, has_symlink = {}, False
    for parent, directories, names in packet.walk(follow_symlinks=False):
        prefix = "" if parent == packet else parent.relative_to(packet).as_posix() + "/"
        has_symlink |= any((parent / name).is_symlink() for name in directories + names)
        for name in names:
            path = parent / name
            if path.is_file():
                actual[prefix + name] = digest(path)
    if has_symlink or actual != metadata["files"]:
        raise WorkflowError("Review packet changed after preparation")
    return metadata


RESULT_FIELDS = {
    "review_sha256",
    "copilot_version",
    "provider_version",
    "diagnostics_sha256",
    "coverage_sha256",
    "reporting_sha256",
    "terminal_sha256",
}


def historical_child(directory, meta):
    parent = Path(directory).parent.parent
    original = issue31_history.verify_packet(parent)
    if original["schema_version"] != 4 or not meta.get("batch_unit"):
        raise WorkflowError("Historical child requires retained schema-4 parent lineage")
    batch = issue31_history.review_batch.load(parent)
    unit = meta["batch_unit"]["unit"]
    if unit not in batch["units"] or Path(directory) != issue31_history.review_batch.unit_path(parent, unit):
        raise WorkflowError("Historical child lineage changed")
    if meta["batch_unit"]["batch_sha256"] != value_digest(batch):
        raise WorkflowError("Historical child parent binding changed")


def version_field(meta):
    return "provider_version" if meta["schema_version"] in {5, 6, 7} else "copilot_version"


def assess_result(directory, meta, body, diagnostics, *, reporting=None):
    if meta["schema_version"] == 2:
        return legacy_coverage.assess(Path(directory) / "packet", body, diagnostics)
    if meta["schema_version"] == 3 and meta.get("batch_unit"):
        historical_child(directory, meta)
        return issue31_history.coverage.assess(Path(directory) / "packet", body, diagnostics)
    if meta["schema_version"] == 3:
        return schema3_coverage.assess(Path(directory) / "packet", body, diagnostics)
    evaluator = schema5_coverage if meta["schema_version"] == 5 else coverage
    if meta["schema_version"] == 6 and meta["review_policy"]["provider"] == "claude-code":
        evaluator = schema6_coverage
    if meta["schema_version"] == 7:
        validate_reporting(directory, meta, body, diagnostics, reporting)
    result = evaluator.assess(Path(directory) / "packet", body, diagnostics, policy=meta["review_policy"])
    if meta["schema_version"] in {6, 7}:
        result = {
            **result,
            "schema_version": 4 if meta["schema_version"] == 7 else 3,
            "policy_digest": value_digest(meta["review_policy"]),
            "input_digest": value_digest({k: v for k, v in meta.items() if k not in RESULT_FIELDS}),
        }
    if meta["schema_version"] == 7:
        result["reporting_sha256"] = value_digest(reporting)
        result["qualified"] = result["qualified"] and reporting["accepted"]
    return result


def validate_reporting(directory, meta, body, diagnostics, reporting):
    import claude_reporting
    from claude_telemetry_v7 import reporting_summary

    try:
        replay = claude_reporting.replay(reporting)
        expected = meta["review_policy"]["reporting"]
        if (
            replay["model"] != meta["review_policy"]["model"]
            or value_digest(replay["limits"]) != value_digest(expected["limits"])
            or replay["inventory_sha256"] != digest(Path(directory) / "packet/required-material.json")
            or replay["report"] != body
            or diagnostics["telemetry"]["reporting"] != reporting_summary(replay)
            or not set("reporting_" + reason for reason in replay["reasons"]).issubset(diagnostics["reasons"])
        ):
            raise ValueError
    except (ValueError, TypeError, KeyError, UnicodeError, RecursionError):
        raise WorkflowError("Reporting proof changed or differs from its capture bindings") from None


def exact_reporting_bytes(path, limit):
    try:
        with plain_path(path).open("rb") as stream:
            raw = stream.read(limit + 1)
        if len(raw) > limit:
            raise ValueError
        return raw
    except (OSError, ValueError):
        raise WorkflowError("Missing or oversized exact reporting artifact") from None


def read_result_artifact(directory, name, meta):
    path = plain_path(Path(directory) / name)
    if meta["schema_version"] != 7:
        return coverage.read_json(path)
    try:
        with path.open("rb") as stream:
            raw = stream.read(meta["review_policy"]["reporting"]["capture_bytes"] + 1)
        if len(raw) > meta["review_policy"]["reporting"]["capture_bytes"]:
            raise ValueError
        return coverage.strict_json(raw.decode("utf-8"))
    except (OSError, ValueError, RecursionError):
        raise WorkflowError("Missing, oversized or malformed reporting artifact") from None


def write_result_artifact(directory, name, meta, value):
    path = plain_path(Path(directory) / name)
    if meta["schema_version"] != 7:
        atomic_json(path, value)
        return
    import claude_reporting

    try:
        raw = claude_reporting._json_bytes(value, meta["review_policy"]["reporting"]["capture_bytes"])
    except (ValueError, UnicodeError, RecursionError):
        raise WorkflowError("Reporting artifact exceeds its immutable capture bound") from None
    atomic_text(path, raw.decode("utf-8"))


def stored_result(directory, meta):
    result = read_result_artifact(directory, "review-result.json", meta)
    inputs = {key: value for key, value in meta.items() if key not in RESULT_FIELDS}
    if (
        not isinstance(result, dict)
        or type(result.get("schema_version")) is not int
        or result["schema_version"] != meta.get("schema_version")
        or result["schema_version"] not in {1, 2, 3, 5, 6, 7}
        or result.get("input_digest") != value_digest(inputs)
        or not isinstance(result.get("body"), str)
        or (meta["schema_version"] != 7 and not result["body"].strip())
        or not isinstance(result.get(version_field(meta)), str)
        or not result[version_field(meta)].strip()
        or coverage.checksum(result["body"]) != result.get("review_sha256")
    ):
        raise WorkflowError("Saved review result changed or belongs to another packet")
    if result["schema_version"] in {5, 6, 7} and result.get("policy_digest") != value_digest(
        meta["review_policy"]
    ):
        raise WorkflowError("Saved review policy binding changed")
    if result["schema_version"] in {2, 3, 5, 6, 7}:
        diagnostics = result.get("diagnostics")
        if value_digest(diagnostics) != result.get("diagnostics_sha256"):
            raise WorkflowError("Saved diagnostics changed")
        assessment = assess_result(
            directory, meta, result["body"], diagnostics, reporting=result.get("reporting")
        )
        if meta["schema_version"] == 7 and (
            result.get("reporting_sha256") != value_digest(result.get("reporting"))
            or result.get("terminal_sha256") != coverage.checksum(result["reporting"]["terminal_text"])
        ):
            raise WorkflowError("Reporting capture hashes changed")
        if meta["schema_version"] == 7:
            from claude_reporting_execution import validate_capture

            validate_capture(directory, meta, result)
        if value_digest(assessment) != result.get("coverage_sha256"):
            raise WorkflowError("Saved coverage changed")
    else:
        assessment = {"qualified": False, "reasons": ["legacy_report_without_coverage"]}
    if result["schema_version"] in {3, 5, 6, 7}:
        capture = read_result_artifact(directory, "review-capture.json", meta)
        expected_capture = {key: value for key, value in result.items() if key != "coverage_sha256"}
        if (
            value_digest(capture) != value_digest(expected_capture)
            if meta["schema_version"] == 7
            else capture != expected_capture
        ):
            raise WorkflowError("Exact review capture changed or is missing")
    return result, assessment


def qualification(directory, *, require=False):
    """Shared gate used by recovery, publication, managed designation and preflight."""
    directory = plain_path(directory)
    meta = verify_packet(directory)
    if meta["schema_version"] == 4:
        if require:
            raise WorkflowError("Historical batches cannot establish current readiness")
        return issue31_history.qualification(directory)
    if meta.get("kind") == "batch-parent":
        return review_batch.qualification(directory, require)
    if require and (meta.get("batch_unit") or meta.get("kind") == "batch-unit"):
        raise WorkflowError("A batch unit cannot independently qualify its parent")
    result, assessment = stored_result(directory, meta)
    for name, key in (("review.md", "review_sha256"),):
        observed = (
            hashlib.sha256(
                exact_reporting_bytes(
                    directory / name, meta["review_policy"]["reporting"]["limits"]["report_bytes"]
                )
            ).hexdigest()
            if meta["schema_version"] == 7
            else digest(plain_path(directory / name))
            if (directory / name).is_file()
            else None
        )
        if observed != meta.get(key):
            raise WorkflowError("Review report changed or is incomplete")
    if result["schema_version"] in {2, 3, 5, 6, 7}:
        for name, key in (("diagnostics.json", "diagnostics_sha256"), ("coverage.json", "coverage_sha256")):
            value = read_result_artifact(directory, name, meta)
            if value_digest(value) != result[key] or meta.get(key) != result[key]:
                raise WorkflowError("Coverage or diagnostics changed or are missing")
    if meta["schema_version"] == 7:
        if (
            hashlib.sha256(
                exact_reporting_bytes(
                    directory / "terminal.txt", meta["review_policy"]["reporting"]["limits"]["terminal_bytes"]
                )
            ).hexdigest()
            != result["terminal_sha256"]
            or value_digest(read_result_artifact(directory, "reporting-proof.json", meta))
            != result["reporting_sha256"]
            or any(meta.get(key) != result[key] for key in ("reporting_sha256", "terminal_sha256"))
        ):
            raise WorkflowError("Reporting proof or auxiliary terminal artifact changed")
    if require and result["schema_version"] not in {5, 6, 7}:
        raise WorkflowError("Legacy review policy cannot establish current coverage readiness")
    if require and meta.get("review_policy", {}).get("provider") == "claude-code":
        from claude_native_auth import validate_binding

        review_policy.require_current_adapter(meta["review_policy"])
        validate_binding(meta["review_policy"].get("authentication"))
    if require and not assessment["qualified"]:
        raise WorkflowError(
            "Review coverage is incomplete; observed capability and every required material are necessary"
        )
    return assessment


def coverage_ready(directory):
    """Current-policy readiness, distinct from an immutable historical assessment."""
    assessment = qualification(directory)
    meta = verify_packet(directory)
    if meta.get("review_policy", {}).get("provider") == "claude-code":
        from claude_native_auth import validate_binding

        try:
            review_policy.require_current_adapter(meta["review_policy"])
            validate_binding(meta["review_policy"].get("authentication"))
        except WorkflowError:
            return False
    return meta.get("schema_version") in {5, 6, 7} and not meta.get("batch_unit") and assessment["qualified"]


def recover_review(repo, directory):
    """Finalize a durably saved exact result without another model request."""
    directory = plain_path(directory)
    meta = verify_packet(directory)
    if meta["schema_version"] == 4:
        return issue31_history.recover_review(repo, directory)
    if meta.get("kind") == "batch-parent":
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
        inputs = {key: value for key, value in meta.items() if key not in RESULT_FIELDS}
        capture = read_result_artifact(directory, "review-capture.json", meta)
        if (
            meta.get("schema_version") not in {3, 5, 6, 7}
            or not isinstance(capture, dict)
            or capture.get("input_digest") != value_digest(inputs)
        ):
            raise WorkflowError("Pending review capture belongs to another packet")
        save_result(
            directory,
            inputs,
            capture.get("body"),
            capture.get("diagnostics"),
            capture.get(version_field(meta)),
            reporting=capture.get("reporting"),
        )
    result, assessment = stored_result(directory, meta)
    report = plain_path(directory / "review.md")
    if meta.get("review_sha256"):
        if (
            meta["review_sha256"] != result["review_sha256"]
            or meta.get(version_field(meta)) != result[version_field(meta)]
        ):
            raise WorkflowError("Completed review report or metadata changed")
        qualification(directory)
        return report
    atomic_text(report, result["body"])
    if result["schema_version"] in {2, 3, 5, 6, 7}:
        atomic_json(directory / "diagnostics.json", result["diagnostics"])
        atomic_json(directory / "coverage.json", assessment)
        meta.update(
            diagnostics_sha256=result["diagnostics_sha256"], coverage_sha256=result["coverage_sha256"]
        )
    if meta["schema_version"] == 7:
        atomic_text(directory / "terminal.txt", result["reporting"]["terminal_text"])
        write_result_artifact(directory, "reporting-proof.json", meta, result["reporting"])
        meta.update(reporting_sha256=result["reporting_sha256"], terminal_sha256=result["terminal_sha256"])
    meta.update(review_sha256=result["review_sha256"])
    meta[version_field(meta)] = result[version_field(meta)]
    atomic_json(directory / "metadata.json", meta)
    return report


def save_result(directory, meta, body, diagnostics, version, *, reporting=None):
    directory = Path(directory)
    if (
        meta.get("schema_version") not in {3, 5, 6, 7}
        or not isinstance(body, str)
        or (meta.get("schema_version") != 7 and not body.strip())
    ):
        raise WorkflowError("New results require a current packet and exact nonempty report")
    if meta["schema_version"] == 7:
        validate_reporting(directory, meta, body, diagnostics, reporting)
    elif reporting is not None:
        raise WorkflowError("Historical capture cannot contain prospective reporting evidence")
    capture = {
        "schema_version": meta["schema_version"],
        "input_digest": value_digest(meta),
        "body": body,
        "review_sha256": coverage.checksum(body),
        version_field(meta): version,
        "diagnostics": diagnostics,
        "diagnostics_sha256": value_digest(diagnostics),
    }
    if meta["schema_version"] in {5, 6, 7}:
        capture["policy_digest"] = value_digest(meta["review_policy"])
    if meta["schema_version"] == 7:
        capture.update(
            reporting=reporting,
            reporting_sha256=value_digest(reporting),
            terminal_sha256=coverage.checksum(reporting["terminal_text"]),
        )
        from claude_reporting_execution import retained

        execution = retained(directory, meta)
        if execution is not None:
            capture["execution"] = execution
    capture_path = plain_path(directory / "review-capture.json")
    if capture_path.exists():
        previous_capture = read_result_artifact(directory, "review-capture.json", meta)
        if (
            value_digest(previous_capture) != value_digest(capture)
            if meta["schema_version"] == 7
            else previous_capture != capture
        ):
            raise WorkflowError("Pending exact review capture changed")
    else:
        # Durable before assessment reads any packet file. A transient storage
        # failure can be recovered without another paid provider invocation.
        write_result_artifact(directory, "review-capture.json", meta, capture)
    if meta.get("kind") == "batch-unit":
        review_batch.captured(directory, meta)
    assessment = assess_result(directory, meta, body, diagnostics, reporting=reporting)
    write_result_artifact(
        directory, "review-result.json", meta, {**capture, "coverage_sha256": value_digest(assessment)}
    )


def review(repo, directory, *, dispatch_context=None):
    try:
        return run_review(repo, directory, dispatch_context=dispatch_context)
    except BaseException:
        # Failures before inference still leave bounded diagnostic reasons. Never
        # replace a journal or already-captured attempt with a generic failure.
        directory = plain_path(directory)
        if not (directory / "diagnostics.json").exists() and (directory / "packet/capability.json").is_file():
            meta = coverage.read_json(directory / "metadata.json")
            if (
                meta.get("schema_version") in {5, 6, 7}
                and meta.get("review_policy", {}).get("provider") == "claude-code"
            ):
                import claude_telemetry

                _, diagnostics, *_ = claude_telemetry.capture(
                    b"",
                    directory / "packet",
                    directory / "packet",
                    meta["review_policy"],
                    "",
                    exit_code=None,
                    failure="preflight_or_storage_failure",
                )
            else:
                _, diagnostics = coverage.parse_events(
                    "",
                    directory / "packet",
                    directory / "packet",
                    exit_code=None,
                    failure="preflight_or_storage_failure",
                )
            atomic_json(directory / "diagnostics.json", diagnostics)
        raise


def run_review(repo, directory, *, dispatch_context=None):
    directory = plain_path(directory)
    meta = verify_packet(directory)
    if meta.get("kind") == "batch-parent":
        raise WorkflowError("Batch execution requires explicit batch-run or batch-resume")
    if meta.get("batch_unit") and dispatch_context is None:
        raise WorkflowError("Batch units require an aggregate reservation")
    if repo.name != meta["repository"]:
        raise WorkflowError("Review packet belongs to another repository")
    current_pr(repo, meta["pr"], meta["head_sha"], meta["base_sha"])
    recovered = recover_review(repo, directory)
    if recovered is not None:
        return recovered
    if meta.get("schema_version") == 7:
        from claude_reporting_execution import require_activation

        require_activation(repo, meta)
    if meta.get("schema_version") not in {5, 6, 7}:
        raise WorkflowError("Legacy packets cannot run a coverage review; prepare a fresh packet")
    if (directory / "attempt.json").exists():
        raise WorkflowError(
            "Prior review attempt has no recoverable result; do not automatically spend another request"
        )
    from review_prompt import native

    native(directory, meta)  # Validate generated fixture before any provider preflight.
    if meta["review_policy"]["provider"] == "copilot":
        from review_copilot import execute
    else:
        from review_claude import execute
    captured = execute(
        repo,
        directory,
        meta,
        **({"dispatch_context": dispatch_context} if dispatch_context is not None else {}),
    )
    body, diagnostics, version = captured[:3]
    # Save sanitized diagnostics on failure too. Provider homes and raw stdout /
    # stderr are discarded; only exact final model output survives separately.
    if meta["schema_version"] == 7:
        save_result(directory, meta, body, diagnostics, version, reporting=captured[3])
    elif body.strip():
        save_result(directory, meta, body, diagnostics, version)
    atomic_json(directory / "diagnostics.json", diagnostics)
    atomic_json(directory / "usage.json", diagnostics["usage"])
    execution_binding = {}
    if meta["schema_version"] == 7:
        from claude_reporting_execution import retained

        execution_binding = {"execution_sha256": value_digest(retained(directory, meta))}
    atomic_json(
        directory / "attempt.json",
        {
            "schema_version": meta["schema_version"],
            "input_digest": value_digest(meta),
            "policy_digest": value_digest(meta["review_policy"]),
            "cli_version": version,
            "status": "finished",
            "requests": 1,
            "reasons": diagnostics["reasons"],
            **execution_binding,
        },
    )
    if not body.strip() and meta["schema_version"] != 7:
        raise WorkflowError(
            "Reviewer returned no recoverable final report; see sanitized diagnostics.json (no automatic retry)"
        )
    verify_packet(directory)
    current_pr(repo, meta["pr"], meta["head_sha"], meta["base_sha"])
    return recover_review(repo, directory)


def publication_body(directory):
    meta = verify_packet(directory)
    if meta["schema_version"] == 4 or (meta["schema_version"] == 3 and meta.get("batch_unit")):
        if meta.get("batch_unit"):
            historical_child(directory, meta)
        return issue31_history.publication_body(directory)
    if meta.get("kind") == "batch-parent":
        return review_batch.publication_body(directory)
    body = (
        exact_reporting_bytes(
            Path(directory) / "review.md", meta["review_policy"]["reporting"]["limits"]["report_bytes"]
        )
        if meta["schema_version"] == 7
        else (Path(directory) / "review.md").read_bytes()
    ).decode("utf-8")
    if meta["schema_version"] == 1:
        if coverage.checksum(body) != meta.get("review_sha256"):
            raise WorkflowError("Legacy report bytes changed")
        return body + f"\n<!-- agentic-review:{meta['head_sha']}:{meta['review_sha256']} -->"
    assessment = qualification(directory)
    label = (
        "coverage-qualified static inspection"
        if assessment["qualified"]
        else "INCOMPLETE static inspection — not ready"
    )
    if meta.get("batch_unit"):
        parent = Path(directory).parent.parent
        planned = review_batch.load(parent)
        complete = review_batch.unit_assessment(parent, planned, meta["batch_unit"]["unit"])["complete"]
        label = "assigned material complete" if complete else "INCOMPLETE static inspection — not ready"
        label = f"batch unit {meta['batch_unit']['unit']['id']} — {label}; parent readiness requires aggregate qualification"
    if meta["schema_version"] == 7:
        label = "INCOMPLETE prospective reporting evidence — activation unavailable; not ready"
    provider_label = "Copilot CLI"
    if meta["schema_version"] in {5, 6, 7} and meta["review_policy"]["provider"] == "claude-code":
        provider_label = "Claude Code"
    header = (
        f"## Independent {provider_label} review\n\nPR #{meta['pr']} · reviewed head `{meta['head_sha']}` "
        f"· base `{meta['base_sha']}`\n\nRequested model: `{meta['requested_model']}`. "
        f"Status: **{label}**. Model output below is preserved exactly. "
        "This is not human approval. The reviewer executed no tests. CI association and tested checkout "
        "are separately recorded in validation.json; unknown execution details remain unknown.\n\n"
    )
    if meta["schema_version"] == 7:
        header += (
            "Report representation: exact StructuredOutput JSON argument fragments. "
            "Auxiliary terminal text is retained separately in the immutable capture; "
            "it is not substituted for this report.\n\n"
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
    current_pr(repo, number, head, base)
    review_batch.current_contract(repo, Path(directory), meta)
    qualification(directory, require=True)
    if meta.get("kind") == "batch-parent":
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
    if meta["schema_version"] == 4:
        return issue31_history.verify_publication(repo, directory)
    if meta.get("kind") == "batch-parent":
        review_batch.verify_unit_publications(repo, directory, complete_only=False)
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
    marker = report_marker(meta)
    matches = [
        item
        for item in repo.api(f"pulls/{meta['pr']}/reviews", paginate=True)
        if marker in (item.get("body") or "") and item.get("commit_id") == meta["head_sha"]
    ]
    if len(matches) != 1 or matches[0].get("body") != expected or matches[0].get("state") != "COMMENTED":
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
    if meta["schema_version"] == 4:
        return issue31_history.publish(repo, directory)
    body = publication_body(directory)
    if repo.name != meta["repository"]:
        raise WorkflowError("Wrong repository for review publication")
    if len(body.encode("utf-8")) > 60000:
        raise WorkflowError("Review exceeds the publication budget; summarize separately with attribution")
    current_pr(repo, meta["pr"], meta["head_sha"], meta["base_sha"])
    marker = report_marker(meta)
    if meta.get("kind") == "batch-parent":
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    prep = sub.add_parser("prepare")
    prep.add_argument("pr")
    prep.add_argument("--issue", required=True)
    prep.add_argument("--plan-comment", required=True)
    prep.add_argument("--expected-head")
    prep.add_argument("--output")
    prep.add_argument("--prior-review")
    review_policy.add_arguments(prep)
    for name in [
        "run",
        "publish",
        "qualify",
        "verify-publication",
        "batch-preview",
        "batch-run",
        "batch-resume",
        "batch-recover",
    ]:
        p = sub.add_parser(name)
        p.add_argument("directory")
        if name in {"batch-run", "batch-preview"}:
            review_batch.add_budget_arguments(
                p, required=name == "batch-run", authorization=name == "batch-run"
            )
    args = parser.parse_args()
    try:
        repo = Repo()
        repo.assert_main()
        if args.command == "prepare":
            result = prepare(
                repo,
                args.pr,
                args.issue,
                args.plan_comment,
                args.expected_head,
                args.output,
                args.prior_review,
                review_provider=args.review_provider,
                review_model=args.review_model,
                review_effort=args.review_effort,
            )
        elif args.command == "batch-preview":
            result = review_batch.preview(
                args.directory,
                review_batch.arguments_budget(args)
                if any(review_batch.arguments_budget(args).values())
                else None,
            )
        elif args.command in {"batch-run", "batch-resume", "batch-recover"}:
            if args.command == "batch-run":
                review_batch.select(
                    args.directory,
                    review_batch.arguments_budget(args),
                    coverage.read_json(Path(args.batch_authorization)),
                )
            result = review_batch.execute(
                repo,
                args.directory,
                resume=args.command == "batch-resume",
                recover_only=args.command == "batch-recover",
            )
        elif args.command == "run":
            result = review(repo, args.directory)
        elif args.command == "publish":
            result = publish(repo, args.directory)
        elif args.command == "verify-publication":
            result = verify_publication(repo, args.directory)
        else:
            meta = verify_packet(args.directory)
            current_pr(repo, meta["pr"], meta["head_sha"], meta["base_sha"])
            review_batch.current_contract(repo, Path(args.directory), meta)
            result = qualification(args.directory, require=True)
        print(json.dumps(result, indent=2) if isinstance(result, dict) else result)
        if args.command == "run" and not coverage_ready(args.directory):
            return 2
        return 0
    except (WorkflowError, OSError, ValueError, subprocess.SubprocessError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


def report_marker(meta):
    unit = meta.get("batch_unit")
    suffix = f":{unit['batch_sha256']}:{unit['unit']['id']}" if unit else ""
    return f"<!-- agentic-review:{meta['head_sha']}:{meta['review_sha256']}{suffix} -->"


if __name__ == "__main__":
    sys.exit(main())
