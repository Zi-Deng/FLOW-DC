"""Actual maintained fixtures must remain losslessly inspectable as inert text."""

import json
from pathlib import Path

from test_workflow import GitFixture, git, review

# isort: split
import review_coverage as coverage
from review_fixtures import stream

FIXTURES = Path(__file__).parent / "fixtures"
NAMES = [
    "claude-grep-2.1.282-v6.mjs",
    "claude-grep-normalization-2.1.282-v6.mjs",
    "claude-refusal-2.1.282-v5.mjs",
    "claude-transport-2.1.282-v6.mjs",
    "claude-refusal-2.1.282-v5.json",
]


class ProjectionTests(GitFixture):
    def prepare_fixtures(self):
        self.commit_task()
        target = self.task_path / "tests/agentic/fixtures"
        target.mkdir(parents=True)
        for name in NAMES:
            (target / name).write_bytes((FIXTURES / name).read_bytes())
        git(self.task_path, "add", "tests")
        git(self.task_path, "commit", "-m", "exact maintained fixtures")
        self.head = git(self.task_path, "rev-parse", "HEAD")
        self.pr_data["head"]["sha"] = self.head
        git(self.task_path, "push", "origin", "HEAD:refs/pull/31/head")
        self.directory = review.prepare(self.repo, 31, 12, 1234)
        self.packet = self.directory / "packet"
        return coverage.read_json(self.packet / "required-material.json")["required"]

    def test_actual_fixtures_are_required_and_losslessly_inspectable(self):
        required = self.prepare_fixtures()
        for name in NAMES:
            with self.subTest(name=name):
                path = "tests/agentic/fixtures/" + name
                rows = [r for r in required if r["path"] == path and r["revision"] == "head"]
                self.assertTrue(rows)
                self.assertFalse([r for r in rows if r.get("omitted")])
                index = next(
                    r for r in coverage.read_json(self.packet / "source-index.json") if r["path"] == path
                )
                self.assertEqual(
                    (self.packet / index["snapshot"]).read_bytes(), (FIXTURES / name).read_bytes()
                )
        projected = [r for r in required if r.get("projection")]
        self.assertTrue(projected)
        for row in projected:
            binding = row["projection"]
            source = (self.packet / binding["source_artifact"]).read_bytes()
            chunks = [json.loads(line) for line in (self.packet / row["artifact"]).read_text().splitlines()]
            restored = b"".join(chunk[2].encode("utf-8") for chunk in chunks)
            self.assertEqual(restored, source[binding["start_byte"] : binding["end_byte"]])
            self.assertEqual(
                restored,
                source.decode("utf-8").splitlines(keepends=True)[binding["source_line"] - 1].encode("utf-8"),
            )
            self.assertEqual(chunks[0][0], binding["start_byte"])
            self.assertEqual(chunks[-1][1], binding["end_byte"])
            self.assertTrue(all(a[1] == b[0] for a, b in zip(chunks, chunks[1:], strict=False)))
        body, diagnostics = coverage.parse_events(
            stream(self.packet), self.packet, self.packet, version="1.0.83"
        )
        self.assertTrue(coverage.assess(self.packet, body, diagnostics)["qualified"])

    def assess(self, packet, omit=()):
        body, diagnostics = coverage.parse_events(stream(packet, omit=omit), packet, packet, version="1.0.83")
        return coverage.assess(packet, body, diagnostics)

    def test_missing_changed_or_truncated_projection_and_source_fail_closed(self):
        from review_fixtures import events

        required = self.prepare_fixtures()
        item = next(r for r in required if r.get("projection") and r["revision"] == "head")
        self.assertFalse(self.assess(self.packet, omit=[item["id"]])["qualified"])
        # Actual reads of the raw line do not substitute for required projection spans.
        records = events(self.packet, omit=[item["id"]])
        binding = item["projection"]
        raw_path = self.packet / binding["source_artifact"]
        saved = raw_path.read_bytes()
        records[1:1] = [
            {
                "type": "tool.execution_start",
                "data": {
                    "toolCallId": "raw-line",
                    "toolName": "view",
                    "arguments": {
                        "path": binding["source_artifact"],
                        "view_range": [binding["source_line"], binding["source_line"]],
                    },
                },
            },
            {
                "type": "tool.execution_complete",
                "data": {
                    "toolCallId": "raw-line",
                    "success": True,
                    "result": {
                        "content": str(binding["source_line"])
                        + ". "
                        + saved.decode("utf-8").splitlines()[binding["source_line"] - 1]
                    },
                },
            },
        ]
        body, diag = coverage.parse_events(
            "\n".join(json.dumps(r) for r in records), self.packet, self.packet, version="1.0.83"
        )
        self.assertFalse(coverage.assess(self.packet, body, diag)["qualified"])
        for changed in (saved[:-1], saved + b"changed", None):
            if changed is None:
                raw_path.unlink()
            else:
                raw_path.write_bytes(changed)
            body, diag = coverage.parse_events(
                "\n".join(json.dumps(r) for r in records), self.packet, self.packet, version="1.0.83"
            )
            result = coverage.assess(self.packet, body, diag)
            self.assertFalse(result["qualified"])
            self.assertIn("projection_source_binding_mismatch", result["reasons"])
            raw_path.write_bytes(saved)
        path = self.packet / item["artifact"]
        projected = path.read_bytes()
        body, diag = coverage.parse_events(stream(self.packet), self.packet, self.packet, version="1.0.83")
        for changed in (projected[:-1], projected.replace(b"[", b" ", 1), None):
            if changed is None:
                path.unlink()
            else:
                path.write_bytes(changed)
            # Packet validation must reject a missing artifact, or assessment must
            # reject the projection binding even when saved spans were positive.
            try:
                result = coverage.assess(self.packet, body, diag)
            except review.WorkflowError:
                pass
            else:
                self.assertFalse(result["qualified"])
            path.write_bytes(projected)
        rows = events(self.packet)
        calls = {
            r["data"]["toolCallId"]
            for r in rows
            if r["type"] == "tool.execution_start" and r["data"]["arguments"].get("path") == item["artifact"]
        }
        self.assertTrue(calls)
        for row in rows:
            data = row.get("data", {})
            if row["type"] == "tool.execution_complete" and data["toolCallId"] in calls:
                data["result"]["content"] = data["result"]["content"][:50] + "[output truncated]"
        body, diag = coverage.parse_events(
            "\n".join(json.dumps(r) for r in rows), self.packet, self.packet, version="1.0.83"
        )
        self.assertFalse(coverage.assess(self.packet, body, diag)["qualified"])

    def test_projection_schema_offsets_and_source_index_are_bound(self):
        import copy

        required = self.prepare_fixtures()
        path = self.packet / "required-material.json"
        saved = path.read_bytes()
        index = next(i for i, r in enumerate(required) if r.get("projection") and r["revision"] == "head")
        for field, value in (
            ("schema_version", True),
            ("source_line", 1),
            ("start_byte", 0),
            ("end_byte", 1),
            ("sha256", "0" * 64),
            ("source_sha256", "0" * 64),
            ("source_artifact", "../metadata.json"),
        ):
            document = json.loads(saved)
            document["required"][index]["projection"][field] = value
            path.write_text(json.dumps(document))
            with self.subTest(field=field):
                self.assertFalse(self.assess(self.packet)["qualified"])
        document = json.loads(saved)
        document["required"][index]["projection"] = copy.deepcopy(required[index]["projection"])
        document["required"][index]["path"] = "different-source.json"
        path.write_text(json.dumps(document))
        self.assertIn("source_revision_binding_mismatch", self.assess(self.packet)["reasons"])
        path.write_bytes(saved)

    def test_native_returned_projection_ranges_require_exact_content(self):
        import claude_telemetry
        import review_policy
        from claude_fixtures import native_events
        from test_review_providers import AUTHENTICATION

        self.prepare_fixtures()
        policy = {
            **review_policy.policy(review_policy.choices("claude-code"), {}),
            "authentication": AUTHENTICATION,
        }
        rows = native_events(self.packet, self.packet, "fixture")
        raw = "\n".join(json.dumps(r) for r in rows)
        body, diagnostics = claude_telemetry.capture(raw, self.packet, self.packet, policy, "fixture")
        self.assertTrue(coverage.assess(self.packet, body, diagnostics, policy)["qualified"])
        # Damage actual returned content while retaining its successful envelope.
        pending = set()
        for row in rows:
            for block in row.get("message", {}).get("content", []):
                if block.get("type") == "tool_use" and block.get("input", {}).get("file_path", "").startswith(
                    "projections/"
                ):
                    pending.add(block["id"])
                if block.get("type") == "tool_result" and block["tool_use_id"] in pending:
                    block["content"] = block["content"][:-10]
        body, diagnostics = claude_telemetry.capture(
            "\n".join(json.dumps(r) for r in rows), self.packet, self.packet, policy, "fixture"
        )
        self.assertFalse(coverage.assess(self.packet, body, diagnostics, policy)["qualified"])

    def test_finite_projection_bounds_preserve_unicode_controls_and_exact_limit(self):
        import review_projection as projection

        line = ('é😀\t\r\u2028\\"' * 1000) + "\r\n"
        rendered = projection.render(line, 25)
        chunks = [json.loads(x) for x in rendered.splitlines()]
        self.assertEqual("".join(r[2] for r in chunks), line)
        self.assertLess(max(len(x) for x in rendered.splitlines()), 2000)
        self.assertTrue(projection.render("x" * projection.MAX_LINE_BYTES, 0))
        with self.assertRaisesRegex(ValueError, "bounded projection"):
            projection.render("x" * (projection.MAX_LINE_BYTES + 1), 0)

    def test_repair_carries_unread_projection_and_original_raw_bytes(self):
        import review_projection
        from review_fixtures import store

        required = self.prepare_fixtures()
        unread = [r for r in required if r.get("projection") and r["revision"] == "head"]
        previous, original_head = self.directory, self.head
        store(self.repo, previous, omit=[r["id"] for r in unread])
        for _count in range(2):
            target = self.task_path / "tests/agentic/fixtures/claude-refusal-2.1.282-v5.json"
            target.write_bytes(b"\n" + target.read_bytes())
            git(self.task_path, "add", ".")
            git(self.task_path, "commit", "-m", "move fixture line")
            self.pr_data["head"]["sha"] = git(self.task_path, "rev-parse", "HEAD")
            git(self.task_path, "push", "origin", "HEAD:refs/pull/31/head")
            current = review.prepare(self.repo, 31, 12, 1234, prior_review=previous)
            packet = current / "packet"
            now = {r["id"]: r for r in coverage.read_json(packet / "required-material.json")["required"]}
            for old in unread:
                row = now[old["id"]]
                self.assertEqual(row["revision"], "prior:" + original_head)
                self.assertTrue(review_projection.validate(packet, row))
                self.assertEqual(
                    (packet / row["artifact"]).read_bytes(), (self.packet / old["artifact"]).read_bytes()
                )
                self.assertEqual(
                    (packet / row["projection"]["source_artifact"]).read_bytes(),
                    (self.packet / old["projection"]["source_artifact"]).read_bytes(),
                )
            self.assertTrue(self.assess(packet)["qualified"])
            store(self.repo, current, omit=[r["id"] for r in unread])
            previous = current

    def test_packet_projection_budget_refuses_without_truncating_source(self):
        from unittest.mock import patch

        import review_projection

        with patch.object(review_projection, "MAX_PACKET_BYTES", 1):
            required = self.prepare_fixtures()
        omitted = [r for r in required if r.get("omitted")]
        self.assertTrue(omitted)
        self.assertTrue(all("Packet exceeds bounded projection byte limit" in r["omitted"] for r in omitted))
        self.assertFalse(self.assess(self.packet)["qualified"])
        row = next(r for r in omitted if r["revision"] == "head")
        self.assertEqual(
            (self.packet / row["artifact"]).read_bytes(),
            (FIXTURES / "claude-refusal-2.1.282-v5.json").read_bytes(),
        )

    def test_mjs_support_still_excludes_symlinks_non_utf8_and_nul(self):
        from test_workflow import workflow

        self.commit_task()
        (self.task_path / "bad.mjs").write_bytes(b"\xff")
        (self.task_path / "nul.mjs").write_bytes(b"\0")
        (self.task_path / "link.mjs").symlink_to("code.py")
        git(self.task_path, "add", ".")
        git(self.task_path, "commit", "-m", "unsupported modules")
        head = git(self.task_path, "rev-parse", "HEAD")
        index = review.snapshot(self.repo, head, self.root / "inert", workflow.configuration(self.root))
        entries = {r["path"]: r for r in index}
        for name in ("bad.mjs", "nul.mjs", "link.mjs"):
            self.assertIn("omitted", entries[name])
            self.assertNotIn("snapshot", entries[name])
