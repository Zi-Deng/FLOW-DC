"""Hand-authored view-boundary fixtures, informed by retained live result hashes.

The original PR30 event-000083 hash matches source/000248.txt lines 361-695
without its final LF. Original request arguments were not retained. These reduced
fixtures do not reproduce ordinary source or claim another live invocation.
"""

import copy
import json
import unittest
from pathlib import Path

from test_workflow import GitFixture, review

# isort: split
import review_coverage as coverage
from review_fixtures import events


class ViewBoundaryTests(unittest.TestCase):
    def observed(self, text, content, selected):
        spans, _, reason = coverage.tool_observation(
            "view",
            {"path": "source.txt", "view_range": selected},
            content,
            Path("/packet"),
            {"source.txt": text},
        )
        return [(span["start_line"], span["end_line"]) for span in spans], reason

    def test_full_requested_lines_allow_only_one_omitted_final_separator(self):
        for separator in ("\n", "\r\n"):
            text = separator.join(["before", "café α", "  ", "last", "after"]) + separator
            selected = separator.join(["café α", "  ", "last"]) + separator
            for content in (selected, selected[: -len(separator)]):
                with self.subTest(separator=repr(separator), content=repr(content)):
                    spans, reason = self.observed(text, content, [2, 4])
                    self.assertEqual(spans, [(2, 4)])
                    self.assertIsNone(reason)

    def test_numeric_raw_text_cannot_credit_a_different_original_line(self):
        # These bytes could be raw line 1 or a rendered prefix for line 2.
        # The request cannot select an otherwise ambiguous interpretation.
        for prefix in ("2. ", "2: ", "2\t"):
            text = prefix + "target\ntarget\nother\n"
            for returned in (prefix + "target\n", prefix + "target"):
                for requested in ([1, 2], [2, 2]):
                    with self.subTest(prefix=prefix, requested=requested):
                        self.assertEqual(self.observed(text, returned, requested)[0], [])
                self.assertEqual(self.observed(text, returned, [1, 1])[0], [(1, 1)])
            partial = prefix + "target"
            longer = prefix + "target-extra\ntarget\nother\n"
            self.assertEqual(self.observed(longer, partial, [2, 2])[0], [])

    def test_raw_repeated_slices_stay_ambiguous_with_separator_elision(self):
        text = "first\nrepeat\nrepeat\nlast\n"
        for content in ("repeat\n", "repeat"):
            self.assertEqual(self.observed(text, content, [2, 2])[0], [])
        mixed = "same\r\nsame\nend\n"
        # Eliding either LF or CRLF produces identical content at two locations.
        self.assertEqual(self.observed(mixed, "same", [1, 1])[0], [])

    def test_no_partial_prefix_normalization_or_blank_line_is_inferred(self):
        text = "before\r\ncafé\u2028α\r\n  \r\nlast\r\nafter\r\n"
        expected = "café\u2028α\r\n  \r\nlast\r\n"
        for content in (
            expected[:-3],
            expected[:-1],
            expected.replace("\r\n", "\n"),
            expected.replace("\u2028", "\n"),
            expected.rstrip() + "\n",
            "",
            "café",
        ):
            self.assertEqual(self.observed(text, content, [2, 5])[0], [], repr(content))
        for content in ("first\n", "first"):
            self.assertEqual(self.observed("first\n\nafter\n", content, [1, 2])[0], [])
        self.assertEqual(self.observed("\n", "", [1, 1])[0], [])
        self.assertEqual(self.observed("\r\n", "", [1, 1])[0], [])
        self.assertEqual(self.observed("first\n\nafter\n", "first\n\n", [1, 2])[0], [(1, 2)])

    def test_whole_file_and_unambiguous_numbered_results_still_match(self):
        text = "first\nsecond\n"
        for content in (text, text[:-1]):
            self.assertEqual(self.observed(text, content, None)[0], [(1, 2)])
        self.assertEqual(self.observed(text, "2. second", [2, 2])[0], [(2, 2)])
        numeric = "2. second\nsecond\n"
        self.assertEqual(self.observed(numeric, numeric, None)[0], [(1, 2)])


class ViewBoundaryEventTests(GitFixture):
    def setUp(self):
        super().setUp()
        self.commit_task()
        self.directory = review.prepare(self.repo, 31, 12, 1234)
        self.packet = self.directory / "packet"

    def test_separator_elision_does_not_bypass_completion_or_path_checks(self):
        rows = events(self.packet)
        returned = (self.packet / "capability/fixture.txt").read_bytes().decode("utf-8")[:-1]
        rows[2]["data"]["result"]["content"] = returned

        def evaluate(records):
            raw = "\n".join(json.dumps(row) for row in records)
            body, diagnostics = coverage.parse_events(raw, self.packet, self.packet, version="1.0.83")
            return coverage.assess(self.packet, body, diagnostics)

        self.assertTrue(evaluate(rows)["qualified"])
        for variant in ("truncated", "ui-only", "partial-line", "outside-packet"):
            changed = copy.deepcopy(rows)
            if variant == "truncated":
                changed[2]["data"]["result"]["truncated"] = True
            elif variant == "ui-only":
                changed[2]["data"]["result"] = {"content": "summary only", "detailedContent": returned}
            elif variant == "partial-line":
                changed[2]["data"]["result"]["content"] = returned[:-1]
            else:
                changed[1]["data"]["arguments"]["path"] = "../capability/fixture.txt"
            with self.subTest(variant=variant):
                self.assertFalse(evaluate(changed)["qualified"])

    def test_historical_fenced_public_findings_keep_original_text_without_unread_row_expansion(self):
        # Reduced hand-authored schema-1 envelope with the first public report's
        # framing. This is not copied model output or prior coverage attestation.
        document = {
            "schema_version": 1,
            "findings": [{"id": "F1", "claim": "Inspect the retained finding"}],
            "coverage": [
                {"id": "old-unread-row", "state": "unread", "locations": [], "reason": "not inspected"}
            ],
            "limitations": ["Original inspection was incomplete"],
        }
        body = (
            "## Independent Copilot CLI review\n\nPR #31 · reviewed head fixture\n\n"
            "CI association and tested checkout are separately recorded in validation.json; "
            "unknown execution details remain unknown.\n\n```json\n"
            + json.dumps(document)
            + "\n```\n\n<!-- agentic-review:fixture -->\n"
            "<!-- agentic-coverage:v1:fixture -->"
        )
        self.reviews.append({"id": 9, "body": body, "commit_id": self.head, "state": "COMMENTED"})
        packet = review.prepare(self.repo, 31, 12, 1234) / "packet"
        finding = (packet / "findings/reviews-0000.txt").read_text()
        self.assertIn("Inspect the retained finding", finding)
        self.assertIn("Original inspection was incomplete", finding)
        self.assertIn("Not inherited", finding)
        self.assertNotIn("old-unread-row", finding)
        self.assertEqual(coverage.read_json(packet / "context.json")["reviews"][0]["body"], body)
        self.assertTrue(
            any(
                row["artifact"] == "findings/reviews-0000.txt"
                for row in coverage.read_json(packet / "required-material.json")["required"]
            )
        )
