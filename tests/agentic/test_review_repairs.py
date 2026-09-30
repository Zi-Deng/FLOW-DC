"""PR30 repair regressions; live provenance is limited to the generated canary."""

import json
from pathlib import Path
from unittest.mock import patch

from test_workflow import SOURCE, GitFixture, review

# isort: split
import review_coverage as coverage
from review_fixtures import stream


class RepairTests(GitFixture):
    def setUp(self):
        super().setUp()
        self.commit_task()
        self.directory = review.prepare(self.repo, 31, 12, 1234)
        self.packet = self.directory / "packet"

    def test_live_generated_unnumbered_view_matches_exact_fixture(self):
        fixture = json.loads((SOURCE / "tests/agentic/fixtures/copilot-1.0.83-view-canary.json").read_bytes())
        artifact = fixture["packet_artifact"]
        files = {artifact: fixture["fixture"]}
        spans, _, reason = coverage.tool_observation(
            "view", {"path": artifact}, fixture["model_facing_content"], self.packet, files
        )
        self.assertIsNone(reason)
        self.assertEqual([(s["start_line"], s["end_line"]) for s in spans], [(1, 2)])
        self.assertEqual(coverage.checksum(fixture["model_facing_content"]), fixture["result_sha256"])

    def test_packet_read_failure_retains_stream_diagnostics_and_report(self):
        raw = stream(self.packet)
        original = Path.read_bytes

        def failed(path):
            if path == self.packet / "issue.txt":
                raise OSError("simulated storage failure with private details")
            return original(path)

        with patch.object(Path, "read_bytes", failed):
            body, diagnostics = coverage.parse_events(raw, self.packet, self.packet, version="1.0.83")
        self.assertTrue(body)
        self.assertIn("unreadable_packet_artifact", diagnostics["reasons"])
        self.assertNotIn("private details", json.dumps(diagnostics))

    def test_undecodable_packet_is_never_replacement_decoded_as_evidence(self):
        raw = stream(self.packet)
        (self.packet / "broken.txt").write_bytes(b"\xff")
        body, diagnostics = coverage.parse_events(raw, self.packet, self.packet, version="1.0.83")
        self.assertTrue(body)
        self.assertIn("undecodable_packet_artifact", diagnostics["reasons"])
        self.assertFalse(coverage.assess(self.packet, body, diagnostics)["qualified"])

    def evaluate(self, body=None, rows=None):
        raw = stream(self.packet, body) if rows is None else "\n".join(json.dumps(row) for row in rows)
        report, diagnostics = coverage.parse_events(raw, self.packet, self.packet, version="1.0.83")
        return coverage.assess(self.packet, report, diagnostics), diagnostics

    def test_unnumbered_view_range_requires_exact_unambiguous_returned_text(self):
        text = "first\r\n\r\n  \r\ncafé\u2028inside\fsection\r\nlast"
        chunks = text.splitlines(keepends=True)
        for selected in (None, [2, 5], [1, -1]):
            returned = text if selected is None or selected == [1, -1] else "".join(chunks[1:5])
            spans, _, reason = coverage.tool_observation(
                "view",
                {"path": "sample.txt", "view_range": selected},
                returned,
                self.packet,
                {"sample.txt": text},
            )
            self.assertIsNone(reason)
            self.assertTrue(spans)
        for returned in (text[:-1], text.replace("\r\n", "\n"), "first", "café"):
            spans, _, _ = coverage.tool_observation(
                "view", {"path": "sample.txt"}, returned, self.packet, {"sample.txt": text}
            )
            self.assertEqual(spans, [])
        for selected in ([True, 2], [1, 999], [0, 2], "all"):
            self.assertEqual(
                coverage.tool_observation(
                    "view",
                    {"path": "sample.txt", "view_range": selected},
                    text,
                    self.packet,
                    {"sample.txt": text},
                )[0],
                [],
            )
        self.assertEqual(
            coverage.tool_observation(
                "view",
                {"path": "sample.txt", "view_range": [2, 2]},
                "repeat\n",
                self.packet,
                {"sample.txt": "repeat\nrepeat\nend\n"},
            )[0],
            [],
        )
        self.assertEqual(
            coverage.tool_observation(
                "view", {"path": "../sample.txt"}, text, self.packet, {"sample.txt": text}
            )[0],
            [],
        )

    def test_unnumbered_results_qualify_blank_lines_without_crediting_truncated_or_ui_content(self):
        from review_fixtures import events

        rows = events(self.packet)
        for start, complete in zip(rows[1::2], rows[2::2], strict=True):
            if start["type"] != "tool.execution_start" or start["data"]["toolName"] != "view":
                continue
            args = start["data"]["arguments"]
            # Whole returned artifacts are actual content, regardless of request.
            complete["data"]["result"]["content"] = (self.packet / args["path"]).read_bytes().decode("utf-8")
        result, diag = self.evaluate(rows=rows)
        self.assertTrue(result["qualified"])
        self.assertTrue(diag["capability"]["view"])
        rows[2]["data"]["truncated"] = True
        self.assertFalse(self.evaluate(rows=rows)[0]["qualified"])
        del rows[2]["data"]["truncated"]
        rows[2]["data"]["result"] = {
            "content": "Summary only",
            "detailedContent": (self.packet / "capability/fixture.txt").read_text(),
        }
        self.assertFalse(self.evaluate(rows=rows)[0]["qualified"])

    def test_compact_claims_materialize_every_unread_item_and_require_evidence(self):
        from review_fixtures import events

        body = json.loads(events(self.packet)[-2]["data"]["content"])
        full_count = len(body["reviewed"])
        body["reviewed"] = body["reviewed"][:1]
        result, _ = self.evaluate(json.dumps(body))
        self.assertFalse(result["qualified"])
        self.assertEqual(result["required_count"], full_count)
        self.assertEqual(result["inspected_count"], 1)
        self.assertEqual(
            sum(row["reason"] == "not_claimed_by_reviewer" for row in result["material"]), full_count - 1
        )
        self.assertTrue(all("location" in row for row in result["material"]))
        body["incomplete"] = [{"ids": body["reviewed"], "state": "unread", "reason": "contradiction"}]
        self.assertIn("malformed_report_contract", self.evaluate(json.dumps(body))[0]["reasons"])
        body["reviewed"] = []
        result, _ = self.evaluate(json.dumps(body))
        self.assertEqual(result["inspected_count"], 0)  # Actual read alone is not a model inspection claim.
        self.assertTrue(any(row.get("reported_reason") == "contradiction" for row in result["material"]))

    def test_strict_outer_fence_preserves_exact_report_bytes(self):
        from review_fixtures import events, store

        bare = events(self.packet)[-2]["data"]["content"]
        body = " \r\n```json\r\n" + bare + "\r\n```\r\n "
        self.assertEqual(self.evaluate(bare)[0], self.evaluate(body)[0])
        store(self.repo, self.directory, body)
        self.assertEqual((self.directory / "review.md").read_bytes(), body.encode())
        self.assertIn(body, review.publication_body(self.directory))
        for invalid in (
            "prose\n" + body,
            body + "\ntrailing",
            body + "\n```json\n{}\n```",
            "```JSON\n" + bare + "\n```",
            "```json\n" + bare,
        ):
            self.assertFalse(self.evaluate(invalid)[0]["qualified"])

    def test_prose_prefixed_report_remains_exact_and_incomplete_after_recovery(self):
        from review_fixtures import events, store

        bare = events(self.packet)[-2]["data"]["content"]
        body = "Capability and scope summary.\r\n\r\n```json\r\n" + bare + "\r\n```\r\n"
        store(self.repo, self.directory, body)
        result = review.qualification(self.directory)
        self.assertIn("malformed_report_contract", result["reasons"])
        self.assertEqual(result["inspected_count"], 0)
        diagnostics = coverage.read_json(self.directory / "diagnostics.json")
        self.assertTrue(all(diagnostics["capability"].values()))
        retained = {
            name: (self.directory / name).read_bytes()
            for name in (
                "review.md",
                "review-capture.json",
                "review-result.json",
                "diagnostics.json",
                "coverage.json",
            )
        }
        with patch.object(review, "run", side_effect=AssertionError("no paid retry")):
            review.recover_review(self.repo, self.directory)
        for name, exact in retained.items():
            self.assertEqual((self.directory / name).read_bytes(), exact)
        self.assertEqual((self.directory / "review.md").read_bytes(), body.encode())
        self.assertIn(body, review.publication_body(self.directory))
        with self.assertRaises(coverage.WorkflowError):
            review.qualification(self.directory, require=True)

    def test_assessment_read_failure_retains_exact_capture_and_recovers_without_provider(self):
        body, diagnostics = coverage.parse_events(
            stream(self.packet), self.packet, self.packet, version="1.0.83"
        )
        meta = review.verify_packet(self.directory)
        with patch.object(coverage, "assess", side_effect=OSError("transient read failure")):
            with self.assertRaises(OSError):
                review.save_result(self.directory, meta, body, diagnostics, "1.0.83")
        capture = coverage.read_json(self.directory / "review-capture.json")
        self.assertEqual(capture["body"].encode(), body.encode())
        self.assertEqual(capture["diagnostics"], diagnostics)
        self.assertFalse((self.directory / "review-result.json").exists())
        with patch.object(review, "run", side_effect=AssertionError("no provider retry")):
            report = review.recover_review(self.repo, self.directory)
        self.assertEqual(report.read_bytes(), body.encode())
        self.assertTrue(review.qualification(self.directory, require=True)["qualified"])
        (self.directory / "review-capture.json").unlink()
        with self.assertRaises(review.WorkflowError):
            review.qualification(self.directory, require=True)

    def test_legacy_assessment_recovery_and_publication_are_frozen_but_not_current_readiness(self):
        import review_coverage_v1 as legacy
        from review_fixtures import events

        items = coverage.read_json(self.packet / "required-material.json")["required"]
        bare = json.dumps(
            {
                "schema_version": 1,
                "findings": [],
                "coverage": [
                    {
                        "id": item["id"],
                        "state": "reviewed",
                        "locations": [{key: item[key] for key in ("artifact", "start_line", "end_line")}],
                        "reason": "",
                    }
                    for item in items
                ],
                "limitations": ["Historical synthetic fixture"],
            }
        )
        # A legacy fenced report must remain malformed; a legacy qualified report
        # keeps its historical label but cannot satisfy today's readiness policy.
        for body in (bare, "```json\n" + bare + "\n```"):
            directory = review.prepare(self.repo, 31, 12, 1234)
            meta = review.verify_packet(directory)
            meta["schema_version"] = 2
            review.atomic_json(directory / "metadata.json", meta)
            rows = events(directory / "packet", body)
            _, diag = legacy.parse_events(
                "\n".join(json.dumps(row) for row in rows),
                directory / "packet",
                directory / "packet",
                version="1.0.83",
            )
            assessment = legacy.assess(directory / "packet", body, diag)
            result = {
                "schema_version": 2,
                "input_digest": review.value_digest(meta),
                "body": body,
                "review_sha256": coverage.checksum(body),
                "copilot_version": "1.0.83",
                "diagnostics": diag,
                "diagnostics_sha256": review.value_digest(diag),
                "coverage_sha256": review.value_digest(assessment),
            }
            review.atomic_json(directory / "review-result.json", result)
            with patch.object(review, "run", side_effect=AssertionError("no provider retry")):
                review.recover_review(self.repo, directory)
            saved = {
                name: (directory / name).read_bytes()
                for name in (
                    "metadata.json",
                    "review-result.json",
                    "review.md",
                    "diagnostics.json",
                    "coverage.json",
                )
            }
            published = review.publication_body(directory)
            self.assertIn("agentic-coverage:v1:", published)
            self.assertEqual(review.qualification(directory), assessment)
            self.assertFalse(review.coverage_ready(directory))
            with self.assertRaisesRegex(review.WorkflowError, "Legacy"):
                review.qualification(directory, require=True)
            self.assertEqual(review.publication_body(directory), published)
            self.assertEqual(saved, {name: (directory / name).read_bytes() for name in saved})
            repair = review.prepare(self.repo, 31, 12, 1234, prior_review=directory)
            self.assertEqual((repair / "packet/prior-report.txt").read_bytes(), body.encode())
            self.assertEqual(coverage.read_json(repair / "packet/prior-review.json")["coverage"], assessment)

    def test_unknown_versions_cannot_gain_readiness_from_valid_evidence(self):
        from copilot_policy import CLI_VERSION

        body, diagnostics = coverage.parse_events(
            stream(self.packet), self.packet, self.packet, version=CLI_VERSION
        )
        self.assertTrue(coverage.assess(self.packet, body, diagnostics)["qualified"])
        for version in ("unknown", "1.0.84"):
            diagnostics["cli_version"] = version
            self.assertIn(
                "unsupported_cli_version", coverage.assess(self.packet, body, diagnostics)["reasons"]
            )

    def test_snapshot_excludes_non_utf8_git_source_without_replacement(self):
        from test_workflow import git, workflow

        (self.task_path / "latin1.py").write_bytes(b"# caf\xe9\n")
        git(self.task_path, "add", "latin1.py")
        git(self.task_path, "commit", "-m", "non-UTF-8 fixture")
        head = git(self.task_path, "rev-parse", "HEAD")
        destination = self.parent / "non-utf8-snapshot"
        manifest = review.snapshot(self.repo, head, destination, workflow.configuration(self.root))
        item = next(item for item in manifest if item["path"] == "latin1.py")
        self.assertEqual(item["omitted"], "not UTF-8 text")
        self.assertNotIn("artifact", item)

    def test_unrecognized_prior_report_fields_remain_required_text(self):
        import review_packet

        body = json.dumps(
            {
                "schema_version": 1,
                "findings": [],
                "coverage": [],
                "limitations": [],
                "outstanding_finding": "must remain accountable",
            }
        )
        text = review_packet.public_finding_text({"body": body})
        self.assertIn("outstanding_finding", text)
        self.assertIn("must remain accountable", text)
        with self.assertRaises(ValueError):
            review_packet.finding_document(body)
