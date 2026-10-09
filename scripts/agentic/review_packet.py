"""Discoverable immutable review material, conservative test mapping and scopes."""

from __future__ import annotations

import ast
import hashlib
import json
import re
from pathlib import Path, PurePosixPath

import review_projection
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


def predecessor_contract(repo, context):
    """Fixed public g13 predecessor; content verification grants no authority."""
    plan = context.get("designated_plan_comment", {})
    if plan.get("id") != 6061320190:
        return None
    rows = context.get("issue_comments")
    if repo.name != "Zi-Deng/FLOW-DC" or type(rows) is not list:
        raise WorkflowError("Missing fixed predecessor context")
    matches = [row for row in rows if type(row) is dict and row.get("id") == 6045434332]
    if len(matches) != 1:
        raise WorkflowError("Missing or ambiguous fixed predecessor")
    row = matches[0]
    if (
        type(row.get("id")) is not int
        or row.get("issue_url") != "https://api.github.com/repos/Zi-Deng/FLOW-DC/issues/31"
        or row.get("user", {}).get("login") != "Zi-Deng"
        or type(row.get("body")) is not str
    ):
        raise WorkflowError("Fixed predecessor identity differs")
    raw = row["body"].encode("utf-8")
    if (
        len(raw) != 59994
        or hashlib.sha256(raw).hexdigest()
        != "9afa53484f6a1c88347e9b7f9d6069cd423ddcdbfdb8fc59979203260c690094"
    ):
        raise WorkflowError("Fixed predecessor bytes differ")
    return raw


def g15_predecessors(repo, context):
    """Two fixed complete public contracts; verification is not authority."""
    plan = context.get("designated_plan_comment")
    if type(plan) is not dict or plan.get("id") != 6062530466:
        return None
    if type(plan.get("id")) is not int:
        raise WorkflowError("Invalid generation15 plan identity")
    rows = context.get("issue_comments")
    if repo.name != "Zi-Deng/FLOW-DC" or type(rows) is not list:
        raise WorkflowError("Missing generation15 predecessor context")
    result = []
    for number, size, expected in (
        (6061320190, 11837, "cc222e50c5ef6511333f2e60226d0903c8d5f36e3053f11c59e90196dbb0d131"),
        (6045434332, 59994, "9afa53484f6a1c88347e9b7f9d6069cd423ddcdbfdb8fc59979203260c690094"),
    ):
        matches = [row for row in rows if type(row) is dict and row.get("id") == number]
        if len(matches) != 1:
            raise WorkflowError("Missing or ambiguous generation15 predecessor")
        row = matches[0]
        if (
            type(row.get("id")) is not int
            or row.get("issue_url") != "https://api.github.com/repos/Zi-Deng/FLOW-DC/issues/31"
            or type(row.get("user")) is not dict
            or row["user"].get("login") != "Zi-Deng"
            or type(row.get("body")) is not str
        ):
            raise WorkflowError("Invalid generation15 predecessor identity")
        raw = row["body"].encode("utf-8")
        if len(raw) != size or hashlib.sha256(raw).hexdigest() != expected:
            raise WorkflowError("Generation15 predecessor bytes differ")
        result.append((number, raw))
    return result


def g16_predecessors(repo, context):
    """Two fixed complete public contracts; verification is not authority."""
    plan = context.get("designated_plan_comment")
    if type(plan) is not dict or plan.get("id") != 6064513854:
        return None
    if type(plan.get("id")) is not int:
        raise WorkflowError("Invalid generation16 plan identity")
    rows = context.get("issue_comments")
    if repo.name != "Zi-Deng/FLOW-DC" or type(rows) is not list:
        raise WorkflowError("Missing generation16 predecessor context")
    result = []
    for number, size, expected in (
        (6062530466, 10290, "c47fbc3aef3daaaa5431c7ffa1b32ed20150a6e63c21c7566ff2c4be258b31ff"),
        (6061320190, 11837, "cc222e50c5ef6511333f2e60226d0903c8d5f36e3053f11c59e90196dbb0d131"),
        (6045434332, 59994, "9afa53484f6a1c88347e9b7f9d6069cd423ddcdbfdb8fc59979203260c690094"),
    ):
        matches = [row for row in rows if type(row) is dict and row.get("id") == number]
        if len(matches) != 1:
            raise WorkflowError("Missing or ambiguous generation16 predecessor")
        row = matches[0]
        if (
            type(row.get("id")) is not int
            or row.get("issue_url") != "https://api.github.com/repos/Zi-Deng/FLOW-DC/issues/31"
            or type(row.get("user")) is not dict
            or row["user"].get("login") != "Zi-Deng"
            or type(row.get("body")) is not str
        ):
            raise WorkflowError("Invalid generation16 predecessor identity")
        raw = row["body"].encode("utf-8")
        if len(raw) != size or hashlib.sha256(raw).hexdigest() != expected:
            raise WorkflowError("Generation15 predecessor bytes differ")
        result.append((number, raw))
    return result


def g17_predecessors(repo, context):
    """Four fixed complete public contracts; verification is not authority."""
    plan = context.get("designated_plan_comment")
    if type(plan) is not dict or plan.get("id") != 6068144159:
        return None
    if type(plan.get("id")) is not int:
        raise WorkflowError("Invalid generation17 plan identity")
    rows = context.get("issue_comments")
    if repo.name != "Zi-Deng/FLOW-DC" or type(rows) is not list:
        raise WorkflowError("Missing generation17 predecessor context")
    result = []
    for number, size, expected in (
        (6064513854, 17466, "8949c427934a19c483af202a77c8df570f76d74a6bbbde0301ef7b2814fb07c3"),
        (6062530466, 10290, "c47fbc3aef3daaaa5431c7ffa1b32ed20150a6e63c21c7566ff2c4be258b31ff"),
        (6061320190, 11837, "cc222e50c5ef6511333f2e60226d0903c8d5f36e3053f11c59e90196dbb0d131"),
        (6045434332, 59994, "9afa53484f6a1c88347e9b7f9d6069cd423ddcdbfdb8fc59979203260c690094"),
    ):
        matches = [row for row in rows if type(row) is dict and row.get("id") == number]
        if len(matches) != 1:
            raise WorkflowError("Missing or ambiguous generation17 predecessor")
        row = matches[0]
        if (
            type(row.get("id")) is not int
            or row.get("issue_url") != "https://api.github.com/repos/Zi-Deng/FLOW-DC/issues/31"
            or type(row.get("user")) is not dict
            or row["user"].get("login") != "Zi-Deng"
            or type(row.get("body")) is not str
        ):
            raise WorkflowError("Invalid generation17 predecessor identity")
        raw = row["body"].encode("utf-8")
        if len(raw) != size or hashlib.sha256(raw).hexdigest() != expected:
            raise WorkflowError("Generation17 predecessor bytes differ")
        result.append((number, raw))
    return result


def g18_predecessors(repo, context):
    """Five fixed complete public contracts; verification is not authority."""
    plan = context.get("designated_plan_comment")
    if type(plan) is not dict or plan.get("id") != 6068705967:
        return None
    if type(plan.get("id")) is not int:
        raise WorkflowError("Invalid generation18 plan identity")
    rows = context.get("issue_comments")
    if repo.name != "Zi-Deng/FLOW-DC" or type(rows) is not list:
        raise WorkflowError("Missing generation18 predecessor context")
    result = []
    for number, size, expected in (
        (6068144159, 28497, "6a7a7186c5edbc2199763d9204e788d743d9f2078c8aa7170265a8dabe4d732f"),
        (6064513854, 17466, "8949c427934a19c483af202a77c8df570f76d74a6bbbde0301ef7b2814fb07c3"),
        (6062530466, 10290, "c47fbc3aef3daaaa5431c7ffa1b32ed20150a6e63c21c7566ff2c4be258b31ff"),
        (6061320190, 11837, "cc222e50c5ef6511333f2e60226d0903c8d5f36e3053f11c59e90196dbb0d131"),
        (6045434332, 59994, "9afa53484f6a1c88347e9b7f9d6069cd423ddcdbfdb8fc59979203260c690094"),
    ):
        matches = [row for row in rows if type(row) is dict and row.get("id") == number]
        if len(matches) != 1:
            raise WorkflowError("Missing or ambiguous generation18 predecessor")
        row = matches[0]
        if (
            type(row.get("id")) is not int
            or row.get("issue_url") != "https://api.github.com/repos/Zi-Deng/FLOW-DC/issues/31"
            or type(row.get("user")) is not dict
            or row["user"].get("login") != "Zi-Deng"
            or type(row.get("body")) is not str
        ):
            raise WorkflowError("Invalid generation18 predecessor identity")
        raw = row["body"].encode("utf-8")
        if len(raw) != size or hashlib.sha256(raw).hexdigest() != expected:
            raise WorkflowError("Generation17 predecessor bytes differ")
        result.append((number, raw))
    return result


def g19_predecessors(repo, context):
    """Seven fixed complete public contracts; verification is not authority."""
    plan = context.get("designated_plan_comment")
    if type(plan) is not dict or plan.get("id") != 6072111969:
        return None
    if type(plan.get("id")) is not int:
        raise WorkflowError("Invalid generation19 plan identity")
    rows = context.get("issue_comments")
    if repo.name != "Zi-Deng/FLOW-DC" or type(rows) is not list:
        raise WorkflowError("Missing generation19 predecessor context")
    result = []
    for number, size, expected in (
        (6068705967, 52551, "9c9285d24d6b8ff9609a737474ff7c27f548c18fe6e1ef867341ffcfe96b310d"),
        (6072002965, 9957, "0ded08099e7fb0dc354f4eef6992cacafca05a4ac9388a2eb9e4ca5a493bdbc0"),
        (6068144159, 28497, "6a7a7186c5edbc2199763d9204e788d743d9f2078c8aa7170265a8dabe4d732f"),
        (6064513854, 17466, "8949c427934a19c483af202a77c8df570f76d74a6bbbde0301ef7b2814fb07c3"),
        (6062530466, 10290, "c47fbc3aef3daaaa5431c7ffa1b32ed20150a6e63c21c7566ff2c4be258b31ff"),
        (6061320190, 11837, "cc222e50c5ef6511333f2e60226d0903c8d5f36e3053f11c59e90196dbb0d131"),
        (6045434332, 59994, "9afa53484f6a1c88347e9b7f9d6069cd423ddcdbfdb8fc59979203260c690094"),
    ):
        matches = [row for row in rows if type(row) is dict and row.get("id") == number]
        if len(matches) != 1:
            raise WorkflowError("Missing or ambiguous generation19 predecessor")
        row = matches[0]
        if (
            type(row.get("id")) is not int
            or row.get("issue_url") != "https://api.github.com/repos/Zi-Deng/FLOW-DC/issues/31"
            or type(row.get("user")) is not dict
            or row["user"].get("login") != "Zi-Deng"
            or type(row.get("body")) is not str
        ):
            raise WorkflowError("Invalid generation19 predecessor identity")
        raw = row["body"].encode("utf-8")
        if len(raw) != size or hashlib.sha256(raw).hexdigest() != expected:
            raise WorkflowError("Generation19 predecessor bytes differ")
        result.append((number, raw))
    return result


def g20_predecessors(repo, context):
    """Nine fixed complete public contracts; verification is not authority."""
    plan = context.get("designated_plan_comment")
    if type(plan) is not dict or plan.get("id") != 6074133818:
        return None
    if type(plan.get("id")) is not int:
        raise WorkflowError("Invalid generation20 plan identity")
    if (
        type(plan.get("user")) is not dict
        or plan["user"].get("login") != "Zi-Deng"
        or plan.get("issue_url") != "https://api.github.com/repos/Zi-Deng/FLOW-DC/issues/31"
        or type(plan.get("body")) is not str
        or len(plan["body"].encode("utf-8")) != 11857
        or hashlib.sha256(plan["body"].encode("utf-8")).hexdigest()
        != "321402ce9af5ef89a186e03b355391cf6725e78506fd5418464a69526fa83c53"
    ):
        raise WorkflowError("Generation20 whole designated contract differs")
    rows = context.get("issue_comments")
    if repo.name != "Zi-Deng/FLOW-DC" or type(rows) is not list:
        raise WorkflowError("Missing generation20 predecessor context")
    result = []
    for number, size, expected in (
        (6072111969, 59052, "b95262d11a49abb5e82baffea069ce716d4a9a2635ded69201d2837a7bf37654"),
        (6074219127, 3948, "12ff82a218ce3b6d064b49669ecc61a5260d8472acafe264ab762c85c1f74a6a"),
        (6068705967, 52551, "9c9285d24d6b8ff9609a737474ff7c27f548c18fe6e1ef867341ffcfe96b310d"),
        (6072002965, 9957, "0ded08099e7fb0dc354f4eef6992cacafca05a4ac9388a2eb9e4ca5a493bdbc0"),
        (6068144159, 28497, "6a7a7186c5edbc2199763d9204e788d743d9f2078c8aa7170265a8dabe4d732f"),
        (6064513854, 17466, "8949c427934a19c483af202a77c8df570f76d74a6bbbde0301ef7b2814fb07c3"),
        (6062530466, 10290, "c47fbc3aef3daaaa5431c7ffa1b32ed20150a6e63c21c7566ff2c4be258b31ff"),
        (6061320190, 11837, "cc222e50c5ef6511333f2e60226d0903c8d5f36e3053f11c59e90196dbb0d131"),
        (6045434332, 59994, "9afa53484f6a1c88347e9b7f9d6069cd423ddcdbfdb8fc59979203260c690094"),
    ):
        matches = [row for row in rows if type(row) is dict and row.get("id") == number]
        if len(matches) != 1:
            raise WorkflowError("Missing or ambiguous generation20 predecessor")
        row = matches[0]
        if (
            type(row.get("id")) is not int
            or row.get("issue_url") != "https://api.github.com/repos/Zi-Deng/FLOW-DC/issues/31"
            or type(row.get("user")) is not dict
            or row["user"].get("login") != "Zi-Deng"
            or type(row.get("body")) is not str
        ):
            raise WorkflowError("Invalid generation20 predecessor identity")
        raw = row["body"].encode("utf-8")
        if len(raw) != size or hashlib.sha256(raw).hexdigest() != expected:
            raise WorkflowError("Generation20 predecessor bytes differ")
        result.append((number, raw))
    return result


def build(repo, packet, head, ancestor, head_index, base_index, context, cfg, prior=None, provider="copilot"):
    packet = Path(packet)
    required = []
    projection_bytes = 0

    def add(
        artifact,
        kind,
        original=None,
        revision="packet",
        links=None,
        omitted=None,
        intervals=None,
        projection=None,
    ):
        nonlocal projection_bytes
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
                    "id": stable_id(kind, revision, original, lines[start - 1 : end], start, end)
                    if projection is None
                    else stable_id(kind, revision, original, projection, start, end),
                    "kind": kind,
                    "path": original,
                    "revision": revision,
                    "artifact": artifact,
                    "start_line": start,
                    "end_line": end,
                    **({"projection": projection} if projection is not None else {}),
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
                try:
                    start_byte = sum(len(part.encode("utf-8")) for part in lines[: number - 1])
                    rendered = review_projection.render(line, start_byte)
                    target = f"projections/{stable_id(artifact, blob, number)}.txt"
                    if not (packet / target).exists():
                        if projection_bytes + len(rendered) > review_projection.MAX_PACKET_BYTES:
                            raise ValueError("Packet exceeds bounded projection byte limit")
                        (packet / "projections").mkdir(exist_ok=True)
                        (packet / target).write_bytes(rendered)
                        projection_bytes += len(rendered)
                    add(
                        target,
                        kind,
                        original=original,
                        revision=revision,
                        links=links,
                        projection={
                            "schema_version": review_projection.VERSION,
                            "source_artifact": artifact,
                            "source_sha256": blob,
                            "source_line": number,
                            "start_byte": start_byte,
                            "end_byte": start_byte + length,
                            "sha256": hashlib.sha256(rendered).hexdigest(),
                        },
                    )
                except ValueError as exc:
                    required.append(
                        {
                            "id": stable_id(kind, revision, original, blob, number),
                            "kind": kind,
                            "path": original,
                            "revision": revision,
                            "artifact": artifact,
                            "omitted": f"Line {number}: {exc}",
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
    predecessor = predecessor_contract(repo, context)
    if predecessor is not None:
        name = "contract-predecessor-6045434332.txt"
        (packet / name).write_bytes(predecessor)
        add(name, "contract")
    predecessors = g20_predecessors(repo, context)
    if predecessors is None:
        predecessors = g19_predecessors(repo, context)
    if predecessors is None:
        predecessors = g18_predecessors(repo, context)
    if predecessors is None:
        predecessors = g17_predecessors(repo, context)
    if predecessors is None:
        predecessors = g16_predecessors(repo, context)
    if predecessors is None:
        predecessors = g15_predecessors(repo, context)
    if predecessors is not None:
        for number, raw in predecessors:
            name = f"contract-predecessor-{number}.txt"
            (packet / name).write_bytes(raw)
            add(name, "contract")
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
                if (
                    plan.get("id") == 6074133818
                    and artifact_path
                    and (packet / artifact_path).stat().st_size > 250000
                ):
                    intervals = [(1, len((packet / artifact_path).read_text().splitlines()))]
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
        if metadata.get("schema_version") == 4 or (
            metadata.get("schema_version") == 6 and metadata.get("kind") == "batch-parent"
        ):
            (packet / "prior-unit-reports").mkdir()
            for unit in assessment["units"]:
                if "review_sha256" in unit:
                    artifact = f"prior-unit-reports/{unit['id']}.txt"
                    (packet / artifact).write_bytes(
                        (previous / "units" / unit["id"] / "review.md").read_bytes()
                    )
                    add(artifact, "finding", f"prior-unit-report:{unit['id']}")
            findings = assessment["findings"]
        else:
            findings = None
        try:
            findings = (
                findings
                if findings is not None
                else finding_document((previous / "review.md").read_bytes().decode("utf-8"))["findings"]
            )
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
            carried = dict(old)
            provenance = old.get("provenance")
            if provenance is None:
                # Source/base-source inherit the validated prior packet commits.
                # Legacy prior-source has only a trustworthy snapshot digest.
                source_commit = (
                    metadata["head_sha"]
                    if old_artifact
                    and old.get("projection", {}).get("source_artifact", old_artifact).startswith("source/")
                    else metadata["merge_base_sha"]
                    if old_artifact
                    and old.get("projection", {})
                    .get("source_artifact", old_artifact)
                    .startswith("base-source/")
                    else None
                )
                provenance = {
                    "schema_version": 1,
                    "source_commit": source_commit,
                    "snapshot_sha256": hashlib.sha256(
                        (previous / "packet" / old_artifact).read_bytes()
                    ).hexdigest()
                    if old_artifact and not old.get("omitted")
                    else None,
                    "original_revision": old["revision"],
                    "original_id": old["id"],
                    "from_packet_head": metadata["head_sha"],
                }
            carried["provenance"] = provenance
            carried["revision"] = (
                "prior:" + provenance["source_commit"]
                if provenance["source_commit"]
                else "prior-snapshot:" + provenance["snapshot_sha256"]
                if provenance["snapshot_sha256"]
                else "prior-unavailable:" + provenance["from_packet_head"]
            )
            if old.get("omitted"):
                carried["artifact"] = None
                required.append(carried)
            else:
                if old_artifact not in copied:
                    target = f"prior-source/{len(copied):06d}.txt"
                    (packet / target).write_bytes((previous / "packet" / old_artifact).read_bytes())
                    copied[old_artifact] = target
                if old.get("projection"):
                    source_artifact = old["projection"]["source_artifact"]
                    if source_artifact not in copied:
                        target = f"prior-source/{len(copied):06d}.txt"
                        (packet / target).write_bytes((previous / "packet" / source_artifact).read_bytes())
                        copied[source_artifact] = target
                    carried["projection"] = {**old["projection"], "source_artifact": copied[source_artifact]}
                required.append({**carried, "artifact": copied[old_artifact]})
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
    implementation_stems = {
        PurePosixPath(path).stem
        for path in set(by_head) | set(by_base)
        if path.startswith("scripts/agentic/") and path.endswith(".py")
    }

    def family(item):
        path = PurePosixPath(item["path"])
        if item["kind"] in {"acceptance", "policy", "finding"}:
            return item["kind"]
        if str(path).startswith(("scripts/agentic/", "tests/agentic/")) and path.suffix == ".py":
            stem = path.stem.removeprefix("test_")
            if str(path).startswith("tests/agentic/"):
                stem = max(
                    (s for s in implementation_stems if stem == s or stem.startswith(s + "_")),
                    key=len,
                    default=stem,
                )
            return "agentic:" + stem
        return item["path"]

    if type(plan.get("id")) is int and plan["id"] == 6074133818:
        from review_public_catalog_v1 import whole_source_mapping

        whole_source_mapping(packet, required)
    ordered = sorted(
        required,
        key=lambda item: (
            0 if item["kind"] == "repair" else 2 if item["kind"] == "cross-boundary" else 1,
            component(item["path"]),
            family(item),
            item["path"],
            item.get("revision", ""),
            item.get("artifact") or "",
            item.get("start_line", 0),
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
            or family(current[0]) != family(item)
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
    write_json(
        packet / "required-material.json",
        {"schema_version": 3 if any("projection" in item for item in required) else 2, "required": required},
    )
    (packet / "inventory-sha256.txt").write_text(
        hashlib.sha256((packet / "required-material.json").read_bytes()).hexdigest() + "\n", encoding="utf-8"
    )
    write_json(
        packet / "scopes.json",
        {
            "schema_version": 1,
            "partition": "source-test-family-v2",
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
        "Projection version 1 rows are JSON [absolute UTF-8 start byte, exclusive end byte, exact text] chunks of one oversized raw line. Their inventory binding names the original source snapshot, line and SHA-256. Read every assigned projection range; decoded text concatenates without separators. Raw snapshots remain unchanged. Projection evidence credits only returned projection ranges, never inferred raw-line reads.\n"
        "Read each required range through view(path, view_range=[start,end]) using 1-based inclusive positions. If the end is blank, extend through a following nonblank line when available. At EOF, view only the nonblank prefix and use grep pattern ^\\s*$ with actual numbered matches for the blank tail; for all-blank ranges use grep alone. Never infer or reconstruct omitted output. Grep only credits actual returned matching lines, glob only proves discovery.\n"
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
    if source_bytes + projection_bytes > cfg["max_snapshot_bytes"]:
        raise WorkflowError(
            "Full head/base/prior source exceeds snapshot budget; required material was not reduced"
        )
