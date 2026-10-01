"""Discoverable immutable review material, conservative test mapping and scopes."""

from __future__ import annotations

import ast
import hashlib
import json
import re
from pathlib import Path, PurePosixPath

from review_coverage import read_json, report_document
from workflow import WorkflowError, run, write_json

MATERIAL_LINES = 120
MATERIAL_BYTES = 16000
SCOPE_LINES = 800
SCOPE_BYTES = 64000
SCOPE_ITEMS = 12


def stable_id(*parts):
    return hashlib.sha256(json.dumps(parts, ensure_ascii=False).encode("utf-8")).hexdigest()[:24]


def finding_document(body):
    """Only recognized complete report contracts can omit repetitive accounting."""
    document = report_document(body)
    if not isinstance(document, dict) or type(document.get("schema_version")) is not int:
        raise ValueError("not a structured review")
    fields = {
        1: {"schema_version", "findings", "coverage", "limitations"},
        2: {"schema_version", "inventory_sha256", "findings", "reviewed", "incomplete", "limitations"},
    }
    version = document["schema_version"]
    if version not in fields or set(document) != fields[version]:
        raise ValueError("unsupported report fields")
    lists = (
        ("findings", "limitations", "coverage")
        if version == 1
        else ("findings", "limitations", "reviewed", "incomplete")
    )
    if not all(isinstance(document[key], list) for key in lists):
        raise ValueError("invalid report fields")
    return document


def public_finding_text(record):
    """Navigate prior findings without re-requiring an old coverage inventory.

    Complete public records remain in context.json. No public coverage assertion
    is inherited. Unstructured bodies retain every line as required material.
    """
    body = record.get("body") or ""
    metadata = {key: record.get(key) for key in ("id", "html_url", "commit_id", "state", "path", "line")}
    try:
        text = body
        if text.startswith("## Independent Copilot CLI review\n"):
            text = text.split("unknown execution details remain unknown.\n\n", 1)[1].split(
                "\n\n<!-- agentic-review:", 1
            )[0]
        document = finding_document(text)
        body = json.dumps(
            {"findings": document["findings"], "limitations": document["limitations"]},
            indent=2,
            ensure_ascii=False,
        )
        metadata["prior_coverage"] = (
            "Not inherited. Full original public record is in context.json; only validated --prior-review carries uncovered obligations."
        )
    except (ValueError, TypeError, KeyError, IndexError):
        pass
    return json.dumps(metadata, indent=2, ensure_ascii=False) + "\n\n" + body + "\n"


def validation_evidence(context, required):
    """Point-in-time API observations, separate from static coverage qualification.

    A check's head_sha associates it with a revision. It does NOT attest to the
    actually checked-out merge commit. Execution details remain unknown unless a
    separately bound hosted execution receipt is available.
    """
    head = context["pull_request"]["head"]["sha"]
    records = []
    for name in required:
        candidates = [item for item in context["check_runs"] if item.get("name") == name]
        observations = []
        for item in candidates:
            association = item.get("head_sha")
            status = (
                "unknown"
                if not association
                else "stale"
                if association != head
                else item.get("conclusion") or item.get("status", "unknown")
            )
            receipts = [
                r
                for r in context.get("hosted_receipts", [])
                if r.get("check_id") == item.get("id") and r.get("state") == "observed"
            ]
            receipt = receipts[0] if len(receipts) == 1 else None
            observations.append(
                {
                    "id": item.get("id"),
                    "head_association": association,
                    "state": status,
                    "details_url": item.get("details_url"),
                    "tested_checkout_sha": receipt["tested_checkout_sha"] if receipt else None,
                    "execution_details": receipt or "unknown",
                }
            )
        records.append(
            {
                "name": name,
                "state": "missing" if not observations else "observed",
                "observations": observations,
            }
        )
    return {
        "schema_version": 1,
        "pr_head_sha": head,
        "required_checks": records,
        "commit_statuses": context["commit_statuses"],
        "hosted_receipts": context.get("hosted_receipts", []),
        "reviewer_execution": "none: static inspection only",
        "note": "Check association and tested checkout are distinct. Unknown/skipped/stale/failed checks do not pass readiness. Hosted execution receipts are separate CI artifacts; tests are not scientific validation.",
    }


def test_file(path):
    parts = PurePosixPath(path).parts
    return "tests" in parts or "test" in parts or PurePosixPath(path).name.startswith("test_")


def component(path):
    parts = PurePosixPath(path).parts
    if (
        "agentic" in parts
        or "agent-workflow" in parts
        or parts[0] in {".agentic", ".github"}
        or parts[:2] in {(".github", "agents"), (".agents", "skills")}
    ):
        return "agentic-workflow"
    return parts[0] if len(parts) > 1 else "repository-root"


def source_ranges(path, text, hunks, revision):
    """Changed lines plus enclosing Python definitions; full non-Python code.

    Markdown requires changed sections. All complete source bytes stay available.
    This is a deterministic navigation policy, not proof of semantic sufficiency.
    """
    lines = text.splitlines()
    if not lines:
        return []
    suffix = PurePosixPath(path).suffix
    if suffix not in {".py", ".md", ".txt"} or not hunks:
        return [(1, len(lines))]
    selected = set()
    definitions = []
    if suffix == ".py":
        try:
            tree = ast.parse(text)
            definitions = [
                (
                    min([node.lineno] + [d.lineno for d in getattr(node, "decorator_list", [])]),
                    node.end_lineno,
                )
                for node in ast.walk(tree)
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
            ]
            # Top-level imports and assignments are necessary module context.
            for node in tree.body:
                if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                    selected.update(range(node.lineno, node.end_lineno + 1))
        except SyntaxError:
            return [(1, len(lines))]
    for hunk in hunks:
        start = max(1, hunk[f"{revision}_start"])
        end = min(len(lines), start + max(1, hunk[f"{revision}_count"]) - 1)
        selected.update(range(max(1, start - 8), min(len(lines), end + 8) + 1))
        if definitions:
            for number in range(start, end + 1):
                enclosing = [(lo, hi) for lo, hi in definitions if lo <= number <= hi]
                if enclosing:
                    lo, hi = min(enclosing, key=lambda pair: pair[1] - pair[0])
                    selected.update(range(lo, hi + 1))
        elif suffix == ".md":
            headings = [i for i, line in enumerate(lines, 1) if re.match(r"^#{1,6} ", line)]
            lo = max([n for n in headings if n <= start], default=1)
            hi = min([n - 1 for n in headings if n > end], default=len(lines))
            selected.update(range(lo, hi + 1))
    from review_coverage import ranges

    return ranges(selected)


def build(repo, packet, head, ancestor, head_index, base_index, context, cfg, prior=None, provider="copilot"):
    packet = Path(packet)
    required = []

    def add(artifact, kind, original=None, revision="packet", links=None, omitted=None, intervals=None):
        original = original or artifact
        if omitted:
            required.append(
                {
                    "id": stable_id(kind, revision, original, omitted),
                    "kind": kind,
                    "path": original,
                    "revision": revision,
                    "artifact": None,
                    "omitted": omitted,
                    "links": links or [],
                    "bytes": 0,
                }
            )
            return
        lines = (packet / artifact).read_bytes().decode("utf-8").splitlines(keepends=True)
        if not lines:
            # An empty blob has no readable line range. Its explicit inventory
            # record, plus the diff, accounts for its empty content.
            marker = f"empty/{stable_id(revision, original)}.txt"
            (packet / "empty").mkdir(exist_ok=True)
            (packet / marker).write_text(
                f"Empty text blob: {json.dumps(original)} at {revision}\n", encoding="utf-8"
            )
            artifact, lines = marker, [(packet / marker).read_text(encoding="utf-8")]
        start, size = 1, 0
        selected = {number for lo, hi in (intervals or [(1, len(lines))]) for number in range(lo, hi + 1)}
        blob = hashlib.sha256((packet / artifact).read_bytes()).hexdigest()

        def entry(end):
            required.append(
                {
                    "id": stable_id(kind, revision, original, lines[start - 1 : end], start, end),
                    "kind": kind,
                    "path": original,
                    "revision": revision,
                    "artifact": artifact,
                    "start_line": start,
                    "end_line": end,
                    "bytes": size,
                    "links": links or [],
                }
            )

        for number, line in enumerate(lines, 1):
            if number not in selected:
                if size:
                    entry(number - 1)
                start, size = number + 1, 0
                continue
            length = len(line.encode("utf-8"))
            if length > MATERIAL_BYTES:
                if size:
                    entry(number - 1)
                required.append(
                    {
                        "id": stable_id(kind, revision, original, blob, number),
                        "kind": kind,
                        "path": original,
                        "revision": revision,
                        "artifact": artifact,
                        "omitted": f"Line {number} exceeds bounded material byte limit",
                        "links": links or [],
                        "bytes": length,
                    }
                )
                start, size = number + 1, 0
                continue
            if size and (number - start >= MATERIAL_LINES or size + length > MATERIAL_BYTES):
                entry(number - 1)
                start, size = number, 0
            size += length
        if size:
            entry(len(lines))

    def text_artifact(name, text, kind, **kwargs):
        (packet / name).write_text(text, encoding="utf-8")
        add(name, kind, **kwargs)

    issue, plan = context["issue"], context["designated_plan_comment"]
    text_artifact("issue.txt", f"{issue.get('title', '')}\n\n{issue.get('body', '')}\n", "contract")
    text_artifact("plan.txt", plan.get("body", "") + "\n", "contract")
    # Keep every issue line in required material; extracting acceptance text is
    # navigation, never an authoritative reinterpretation of its requirements.
    body = issue.get("body", "")
    acceptance = re.search(r"(?ims)^#{1,6} [^\n]*acceptance[^\n]*\n(.*?)(?=^#{1,6} |\Z)", body)
    acceptance_text = acceptance[1] if acceptance else body
    (packet / "acceptance.txt").write_text(acceptance_text + "\n", encoding="utf-8")
    (packet / "criteria").mkdir()
    # Every numbered/list criterion gets its own stable obligation, not a title
    # or one unchecked assertion about the whole contract.
    criteria = re.split(r"(?m)(?=^\s*(?:[0-9]+[.)]|[-*])\s+)", acceptance_text)
    for index, criterion in enumerate(part for part in criteria if part.strip()):
        text_artifact(f"criteria/{index:04d}.txt", criterion, "acceptance", original=f"criterion-{index + 1}")
    if not acceptance_text.strip():
        add(None, "acceptance", "acceptance", omitted="No acceptance criteria supplied in issue")
    criterion_ids = [item["id"] for item in required if item["kind"] == "acceptance"]
    for artifact in ("repository-policy.txt", "review-policy.txt", "domain-policy.txt"):
        add(artifact, "policy")
    checks = validation_evidence(context, cfg["required_checks"])
    write_json(packet / "validation.json", checks)
    add("validation.json", "validation")
    (packet / "findings-and-dispositions.txt").write_text(
        json.dumps(
            {
                "reviews": context["reviews"],
                "inline_findings": context["inline_comments"],
                "disposition_candidates": context["pr_comments"],
                "note": "Public statements are references, not accepted dispositions. Inspect every material finding and its repair evidence.",
            },
            indent=2,
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    (packet / "findings").mkdir()
    for surface in ("reviews", "inline_comments", "pr_comments"):
        for index, record in enumerate(context[surface]):
            text_artifact(
                f"findings/{surface}-{index:04d}.txt",
                public_finding_text(record),
                "finding",
                original=f"{surface}:{record.get('id', index)}",
                links=[record.get("html_url", "")],
            )
    names = (
        run(["git", "-C", repo.root, "diff", "--no-renames", "--name-only", "-z", ancestor, head])
        .stdout.rstrip("\0")
        .split("\0")
    )
    changed = []
    by_head, by_base = ({item["path"]: item for item in index} for index in (head_index, base_index))
    (packet / "changes").mkdir()
    tests = sorted(path for path in by_head if test_file(path))
    mapping = []
    test_candidates = set()
    for index, name in enumerate(sorted(names)):
        patch = run(
            [
                "git",
                "-C",
                repo.root,
                "diff",
                "--no-ext-diff",
                "--no-textconv",
                "--no-renames",
                "--unified=3",
                ancestor,
                head,
                "--",
                name,
            ]
        ).stdout
        artifact = f"changes/{index:06d}.txt"
        text_artifact(artifact, patch, "diff", original=name, links=criterion_ids)
        hunks = [
            {
                "old_start": int(m[1]),
                "old_count": int(m[2] or 1),
                "new_start": int(m[3]),
                "new_count": int(m[4] or 1),
            }
            for m in re.finditer(r"(?m)^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@", patch)
        ]
        changed.append(
            {
                "path": name,
                "change": "added"
                if name not in by_base
                else "deleted"
                if name not in by_head
                else "modified",
                "diff": artifact,
                "hunks": hunks,
                "note": "Renames are accounted as old-path deletion and new-path addition; neither side disappears.",
            }
        )
        for revision, source in (("base", by_base), ("head", by_head)):
            if name in source:
                item = source[name]
                artifact_path = item.get("snapshot")
                intervals = (
                    source_ranges(
                        name,
                        (packet / artifact_path).read_bytes().decode("utf-8"),
                        hunks,
                        "old" if revision == "base" else "new",
                    )
                    if artifact_path
                    else None
                )
                add(
                    artifact_path,
                    "changed-source",
                    name,
                    revision,
                    criterion_ids,
                    item.get("omitted"),
                    intervals,
                )
        candidates = [
            path
            for path in tests
            if component(path) == component(name)
            or PurePosixPath(name).stem.removeprefix("test_") in PurePosixPath(path).stem
        ]
        is_doc = PurePosixPath(name).suffix in {".md", ".txt"}
        if is_doc and component(name) != "agentic-workflow":
            candidates = []
        fallback = not candidates and not is_doc
        mapping.append(
            {
                "changed_path": name,
                "candidates": candidates,
                "rule": "unresolved code/test mapping"
                if fallback
                else "documentation: static contract review"
                if not candidates
                else "same component or filename stem",
                "limit": "Conservative candidates, not proof of sufficient testing. Cross-boundary pass must identify additional gaps.",
            }
        )
        test_candidates.update(candidates)
        if fallback:
            text_artifact(
                f"changes/{index:06d}-test-gap.txt",
                f"No test candidate discovered for {json.dumps(name)}. Assess missing coverage and identify relevant tests or explicitly report the gap.\n",
                "test-mapping",
                original=name,
            )
    write_json(packet / "changed-files.json", changed)
    write_json(packet / "test-map.json", mapping)
    add("changed-files.json", "inventory")
    add("test-map.json", "test-mapping")
    for name in sorted(test_candidates - set(names)):
        item = by_head[name]
        add(item.get("snapshot"), "test", name, "head", criterion_ids, item.get("omitted"))
    if not tests:
        text_artifact(
            "test-limitations.txt",
            "No test files were found in the committed source inventory. Assess this gap against the approved contract.\n",
            "test-mapping",
        )
    if prior:
        previous, metadata, assessment = prior
        previous = Path(previous)
        write_json(
            packet / "prior-review.json",
            {
                "head_sha": metadata["head_sha"],
                "base_sha": metadata["base_sha"],
                "report_sha256": metadata["review_sha256"],
                "coverage": assessment,
                "note": "Prior qualification is not inherited. Current original diff scope is required. Prior uncovered code/tests and finding obligations are carried without recursively copying old policy/context.",
            },
        )
        add("prior-review.json", "findings")
        (packet / "prior-report.txt").write_bytes((previous / "review.md").read_bytes())
        try:
            findings = finding_document((previous / "review.md").read_bytes().decode("utf-8"))["findings"]
        except (ValueError, KeyError, TypeError):
            findings = [
                {"unstructured_prior_report": "Read prior-report.txt; previous findings could not be parsed."}
            ]
            add("prior-report.txt", "finding", "unstructured-prior-findings")
        for index, finding in enumerate(findings):
            text_artifact(
                f"findings/prior-{index:04d}.txt",
                json.dumps(
                    {
                        "finding": finding,
                        "disposition_candidates": context["pr_comments"],
                        "repair_delta": "repair-delta.txt",
                    },
                    indent=2,
                    ensure_ascii=False,
                )
                + "\n",
                "finding",
                original=f"prior-finding:{finding.get('id', index)}",
                links=["repair-delta.txt"],
            )
        delta = run(
            [
                "git",
                "-C",
                repo.root,
                "diff",
                "--no-ext-diff",
                "--no-textconv",
                "--no-renames",
                metadata["head_sha"],
                head,
            ]
        ).stdout
        text_artifact("repair-delta.txt", delta or "No changes since the prior head.\n", "repair")
        old_required = read_json(previous / "packet/required-material.json")["required"]
        old_states = {row["id"]: row["state"] for row in assessment["material"]}
        current_ids = {item["id"] for item in required}
        (packet / "prior-source").mkdir()
        copied = {}
        for old in old_required:
            if (
                old["id"] in current_ids
                or old["kind"] not in {"changed-source", "test", "finding", "prior-material"}
                or old_states.get(old["id"]) == "reviewed"
            ):
                continue
            old_artifact = old.get("artifact")
            if old.get("omitted"):
                required.append(dict(old))
            else:
                if old_artifact not in copied:
                    target = f"prior-source/{len(copied):06d}.txt"
                    (packet / target).write_bytes((previous / "packet" / old_artifact).read_bytes())
                    copied[old_artifact] = target
                required.append({**old, "artifact": copied[old_artifact]})
    text_artifact(
        "cross-boundary.txt",
        "Cross-boundary static pass: assess config/CLI/schema consumers, callers, error and recovery paths, permissions, hosted/local/managed readiness, installer payload, test adequacy and contract criteria together.\n"
        "Record unsupported material explicitly. Observed reads are not proof of understanding.\n\n"
        + "\n".join(json.dumps({"path": item["path"], "diff": item["diff"]}) for item in changed)
        + "\n",
        "cross-boundary",
        links=criterion_ids,
    )
    scopes, current = [], []
    totals = [0, 0]
    # Cross-boundary material always follows component material. Repair material
    # leads navigation; the inventory still requires the original complete scope.
    ordered = sorted(
        required,
        key=lambda item: (
            0 if item["kind"] == "repair" else 2 if item["kind"] == "cross-boundary" else 1,
            component(item["path"]),
            item["id"],
        ),
    )

    def flush():
        if current:
            scopes.append(
                {
                    "id": f"scope-{len(scopes):04d}",
                    "component": component(current[0]["path"]),
                    "required_ids": [item["id"] for item in current],
                    "lines": totals[0],
                    "bytes": totals[1],
                }
            )

    for item in ordered:
        lines = item.get("end_line", 0) - item.get("start_line", 1) + 1
        size = item["bytes"] if not item.get("omitted") else 0
        if current and (
            len(current) >= SCOPE_ITEMS
            or totals[0] + lines > SCOPE_LINES
            or totals[1] + size > SCOPE_BYTES
            or component(current[0]["path"]) != component(item["path"])
            or current[0]["kind"] == "cross-boundary"
            or item["kind"] == "cross-boundary"
        ):
            flush()
            current, totals = [], [0, 0]
        current.append(item)
        totals[0] += lines
        totals[1] += size
    flush()
    (packet / "scopes").mkdir()
    lookup = {item["id"]: item for item in required}
    for scope in scopes:
        artifact = f"scopes/{scope['id']}.json"
        write_json(packet / artifact, {**scope, "material": [lookup[key] for key in scope["required_ids"]]})
        scope["artifact"] = artifact
    write_json(packet / "required-material.json", {"schema_version": 1, "required": required})
    (packet / "inventory-sha256.txt").write_text(
        hashlib.sha256((packet / "required-material.json").read_bytes()).hexdigest() + "\n", encoding="utf-8"
    )
    write_json(
        packet / "scopes.json",
        {
            "schema_version": 1,
            "scopes": scopes,
            "required_items": len(required),
            "required_lines": sum(s["lines"] for s in scopes),
            "required_bytes": sum(s["bytes"] for s in scopes),
            "available_source_bytes": sum(
                p.stat().st_size
                for d in ("source", "base-source", "prior-source")
                for p in (packet / d).glob("*.txt")
            ),
            "limits": {"lines": SCOPE_LINES, "bytes": SCOPE_BYTES, "items": SCOPE_ITEMS},
            "requests": 1,
            "note": "Organization only: no automatic paid fan-out, increased budgets or reduced scope.",
        },
    )
    (packet / "capability").mkdir()
    token = "REVIEW_CANARY_" + stable_id(repo.name, head, ancestor)
    (packet / "capability/fixture.txt").write_text(
        f"Read-only tool capability fixture\n{token}\n", encoding="utf-8"
    )
    write_json(packet / "capability.json", {"artifact": "capability/fixture.txt", "line": 2, "token": token})
    (packet / "START.txt").write_text(
        "Static review, one invocation. First view capability/fixture.txt, grep its token in that file with content and line numbers, and glob capability/*.txt. Actual successful model-facing results are required.\n"
        "Read issue.txt, plan.txt, acceptance.txt, changed-files.json, test-map.json, findings-and-dispositions.txt and validation.json.\n"
        + (
            "Repair run: begin with repair-delta.txt, prior-review.json and prior-report.txt; all prior material remains accountable.\n"
            if prior
            else ""
        )
        + "Use scopes.json and scopes/*.json to navigate every entry in required-material.json. Full diff.txt, source-index.json and base-source-index.json remain available.\n"
        "Read each required range through view(path, view_range=[start,end]); grep only credits actual returned matching lines, glob only proves discovery.\n"
        "Return compact JSON matching report-schema.json: copy inventory-sha256.txt into inventory_sha256, list only positively inspected IDs in reviewed, and group specific incomplete reasons in incomplete. Omitted IDs remain unread and block readiness; do not repeat an unread row for each ID. Explain general limits once in limitations. Do not assert budget exhaustion without a provider signal. Never infer execution from static inspection.\n",
        encoding="utf-8",
    )
    if provider == "claude-code":
        start = packet / "START.txt"
        text = (
            start.read_text(encoding="utf-8")
            .replace("First view ", "First Read ")
            .replace("grep its token", "Grep its token")
            .replace("glob capability", "Glob capability")
            .replace(
                "through view(path, view_range=[start,end])",
                "through Read(file_path, offset=start, limit=end-start+1)",
            )
            .replace("grep only credits", "Grep (output_mode=content, -n=true) only credits")
            .replace("glob only proves", "Glob only proves")
        )
        start.write_text(text, encoding="utf-8")
    # Count all source material, including immutable carried-forward material.
    source_bytes = sum(
        p.stat().st_size
        for directory in ("source", "base-source", "prior-source")
        for p in (packet / directory).glob("*.txt")
    )
    if source_bytes > cfg["max_snapshot_bytes"]:
        raise WorkflowError(
            "Full head/base/prior source exceeds snapshot budget; required material was not reduced"
        )
