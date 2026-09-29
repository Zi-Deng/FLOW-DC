"""Synthetic capability, range, sanitization and packet regression evidence."""

import copy
import json

from test_workflow import GitFixture, git, review, workflow

# isort: split
import review_coverage as coverage
import review_packet
from review_fixtures import events, store, stream


class CoverageTests(GitFixture):
    def setUp(self):
        super().setUp()
        self.commit_task()
        self.directory = review.prepare(self.repo, 31, 12, 1234)
        self.packet = self.directory / "packet"

    def evaluate(self, records, **kwargs):
        raw = "\n".join(json.dumps(event, ensure_ascii=False) for event in records) + "\n"
        body, diagnostics = coverage.parse_events(raw, self.packet, self.packet, version="1.0.83", **kwargs)
        return coverage.assess(self.packet, body, diagnostics), diagnostics

    def test_complete_synthetic_events_qualify_without_claiming_live_capability(self):
        result, diagnostics = self.evaluate(events(self.packet))
        self.assertTrue(result["qualified"])
        self.assertEqual(diagnostics["capability"], {"view": True, "grep": True, "glob": True})
        self.assertEqual(result["inspected_count"], result["required_count"])
        self.assertTrue(all(row["evidence"] for row in result["material"]))

    def test_bad_missing_truncated_unknown_and_uncorrelated_telemetry_fail_closed(self):
        original = events(self.packet)
        variants = []
        variants.append(original[:-1])
        variants.append([event for event in original if not event["type"].startswith("tool.")])
        variants.append(original + [{"type": "provider.mystery", "data": {}}])
        variants.append(original + [original[2]])
        variants.append(original[:2] + original[3:])
        for field, value in [("success", False), ("success", "true"), ("result", {}), ("truncated", True)]:
            modified = copy.deepcopy(original)
            modified[4]["data"][field] = value  # grep canary completion
            variants.append(modified)
        for variant in variants:
            with self.subTest(index=variants.index(variant)):
                result, _ = self.evaluate(variant)
                self.assertFalse(result["qualified"])
        body, diagnostics = coverage.parse_events(stream(self.packet) + '{"type":', self.packet, self.packet)
        self.assertFalse(coverage.assess(self.packet, body, diagnostics)["qualified"])

    def test_optional_bookkeeping_data_and_cli_terminal_result(self):
        original = events(self.packet)
        original[0].pop("data")
        original[-1] = {
            "type": "result",
            "usage": {"premiumRequests": 1},
            "result": original[-2]["data"]["content"],
            "is_error": False,
        }
        result, diagnostics = self.evaluate(original)
        self.assertTrue(result["qualified"])
        self.assertEqual(diagnostics["usage"]["counters"], {"premiumRequests": 1})
        original[-1]["is_error"] = True
        self.assertFalse(self.evaluate(original)[0]["qualified"])

    def test_empty_file_list_and_truncated_exploration_can_be_followed_by_complete_reads(self):
        for tool, args, content in [
            ("grep", {"pattern": "absent"}, "No matches found"),
            ("grep", {"pattern": "value", "output_mode": "files_with_matches"}, "source/000000.txt"),
            ("view", {"path": "."}, "directory listing"),
            ("view", {"path": "issue.txt"}, "[output truncated]"),
        ]:
            original = events(self.packet)
            original[1:1] = [
                {
                    "type": "tool.execution_start",
                    "data": {"toolCallId": "explore", "toolName": tool, "arguments": args},
                },
                {
                    "type": "tool.execution_complete",
                    "data": {"toolCallId": "explore", "success": True, "result": {"content": content}},
                },
            ]
            with self.subTest(tool=tool, content=content):
                result, diagnostics = self.evaluate(original)
                self.assertTrue(result["qualified"])
                self.assertEqual(diagnostics["events"][0]["spans"], [])

    def test_jsonl_unicode_line_separators_are_part_of_exact_model_content(self):
        original = events(self.packet)
        document = json.loads(original[-2]["data"]["content"])
        document["limitations"].append("Unicode separator \u2028 stays in this JSONL record.")
        body = json.dumps(document, ensure_ascii=False)
        original[-2]["data"]["content"] = body
        result, diagnostics = self.evaluate(original)
        self.assertTrue(result["qualified"])
        raw = "\n".join(json.dumps(row, ensure_ascii=False) for row in original)
        exact, _ = coverage.parse_events(raw, self.packet, self.packet, version="1.0.83")
        self.assertEqual(exact.encode(), body.encode())

    def test_missing_source_read_cannot_be_replaced_by_a_model_checkmark(self):
        required = coverage.read_json(self.packet / "required-material.json")["required"]
        item = next(row for row in required if row["kind"] == "changed-source")
        result, _ = self.evaluate(events(self.packet, omit=[item["id"]]))
        self.assertFalse(result["qualified"])
        self.assertEqual(
            next(row for row in result["material"] if row["id"] == item["id"])["state"], "unsupported"
        )

    def test_claim_schema_ids_and_ranges_cannot_expand_actual_evidence(self):
        original = events(self.packet)
        report = json.loads(original[-2]["data"]["content"])
        variants = []
        for key, value in [("start_line", True), ("end_line", 1000000), ("artifact", "../memory/secret")]:
            doc = copy.deepcopy(report)
            doc["coverage"][0]["locations"][0][key] = value
            variants.append(doc)
        variants.extend(
            [
                {**report, "coverage": report["coverage"][1:]},
                {**report, "coverage": report["coverage"] + report["coverage"][:1]},
                {**report, "coverage_percent": 100},
                {**report, "schema_version": True},
            ]
        )
        for document in variants:
            original[-2]["data"]["content"] = json.dumps(document)
            self.assertFalse(self.evaluate(original)[0]["qualified"])
        original[-2]["data"]["content"] = '{"schema_version":1,"schema_version":1}'
        self.assertFalse(self.evaluate(original)[0]["qualified"])

    def test_diagnostics_exclude_arbitrary_provider_fields(self):
        original = events(self.packet)
        for event in original:
            event["secret"] = "private-token-example"
            event["data"]["reasoningText"] = "private-token-example"
        original[1]["data"]["arguments"]["extraneous"] = "private-token-example"
        original[2]["data"]["result"]["detailedContent"] = "private-token-example"
        _, diagnostics = self.evaluate(
            original,
            usage={
                "token": "private-token-example",
                "totalNanoAiu": 123,
                "totalPremiumRequestCost": 2,
                "modelUsage": {
                    "claude-opus-5": {"inputTokens": 12, "requests": 1, "secret": "private-token-example"}
                },
            },
        )
        encoded = json.dumps(diagnostics)
        self.assertNotIn("private-token-example", encoded)
        self.assertNotIn("synthetic-call", encoded)
        self.assertEqual(diagnostics["usage"]["counters"]["totalNanoAiu"], 123)
        self.assertEqual(diagnostics["usage"]["models"]["claude-opus-5"]["inputTokens"], 12)

    def test_forbidden_tools_external_paths_delegation_and_process_failure_disqualify(self):
        original = events(self.packet)
        for key, value in [
            ("toolName", "bash"),
            ("parentToolCallId", "other-agent"),
            ("mcpServerName", "private-server"),
            ("arguments", {"path": "/proc/self/environ"}),
        ]:
            modified = copy.deepcopy(original)
            modified[1]["data"][key] = value
            with self.subTest(key=key):
                self.assertFalse(self.evaluate(modified)[0]["qualified"])
        self.assertFalse(self.evaluate(original, exit_code=1)[0]["qualified"])

    def test_diagnostic_tampering_and_missing_files_block_all_qualification(self):
        store(self.repo, self.directory)
        for name in ("diagnostics.json", "coverage.json", "review-result.json"):
            path = self.directory / name
            saved = path.read_bytes()
            path.unlink()
            with self.subTest(name=name), self.assertRaises(workflow.WorkflowError):
                review.qualification(self.directory, require=True)
            path.write_bytes(saved)
        diagnostics = coverage.read_json(self.directory / "diagnostics.json")
        diagnostics["events"] = []
        with self.assertRaises(workflow.WorkflowError):
            coverage.assess(self.packet, (self.directory / "review.md").read_text(), diagnostics)

    def test_partial_reports_publish_with_incomplete_label_and_exact_controls(self):
        body = "  café\r\n\x1b[31m\x07literal \\u001b \\n\r\n"
        store(self.repo, self.directory, body)
        review.publish(self.repo, self.directory)
        self.assertIn("INCOMPLETE", self.posts[-1][1]["body"])
        self.assertIn(body, self.posts[-1][1]["body"])
        self.assertEqual((self.directory / "review.md").read_bytes(), body.encode())
        with self.assertRaises(workflow.WorkflowError):
            review.qualification(self.directory, require=True)

    def test_stale_base_cannot_use_a_previously_qualified_record(self):
        store(self.repo, self.directory)
        self.pr_data["base"]["sha"] = "a" * 40
        with self.assertRaisesRegex(workflow.WorkflowError, "changed"):
            review.publish(self.repo, self.directory)
        with self.assertRaisesRegex(workflow.WorkflowError, "stale"):
            review.verified_published(self.repo, self.directory, 31, self.head, "a" * 40)


class PacketTests(GitFixture):
    def updated_head(self):
        git(self.task_path, "add", ".")
        git(self.task_path, "commit", "-m", "packet fixture")
        self.head = git(self.task_path, "rev-parse", "HEAD")
        self.pr_data["head"]["sha"] = self.head
        git(self.task_path, "push", "origin", "HEAD:refs/pull/31/head")

    def test_criteria_deleted_renamed_and_empty_files_are_accounted(self):
        self.issue["body"] = (
            "## Acceptance criteria\n1. Preserve deletion evidence.\n2. Inspect renamed and empty paths.\n"
        )
        self.commit_task()
        (self.task_path / "code.py").rename(self.task_path / "renamed.py")
        (self.task_path / "empty.py").touch()
        self.updated_head()
        directory = review.prepare(self.repo, 31, 12, 1234)
        packet = directory / "packet"
        required = coverage.read_json(packet / "required-material.json")["required"]
        self.assertEqual(len([item for item in required if item["kind"] == "acceptance"]), 2)
        source = [item for item in required if item["kind"] == "changed-source"]
        self.assertEqual({item["path"] for item in source}, {"code.py", "renamed.py", "empty.py"})
        self.assertTrue(any(item["path"] == "code.py" and item["revision"] == "base" for item in source))
        store(self.repo, directory)
        self.assertTrue(review.qualification(directory)["qualified"])

    def test_relevant_unchanged_test_and_function_context_are_required(self):
        tests = self.root / "tests"
        tests.mkdir()
        (tests / "test_code.py").write_text("def test_value():\n    assert True\n")
        (self.root / "code.py").write_text(
            "def changed():\n    value = 1\n    return value\n\n" + "def unrelated():\n    return 5\n" * 100
        )
        git(self.root, "add", ".")
        git(self.root, "commit", "-m", "context fixtures")
        git(self.root, "push", "origin", "trunk")
        self.base = git(self.root, "rev-parse", "HEAD")
        self.commit_task()
        # commit_task replaces code.py; restore a one-line change for this case.
        text = (self.root / "code.py").read_text().replace("value = 1", "value = 2")
        (self.task_path / "code.py").write_text(text)
        self.updated_head()
        packet = review.prepare(self.repo, 31, 12, 1234) / "packet"
        required = coverage.read_json(packet / "required-material.json")["required"]
        self.assertTrue(
            any(item["path"] == "tests/test_code.py" and item["kind"] == "test" for item in required)
        )
        code = [item for item in required if item["path"] == "code.py" and item["kind"] == "changed-source"]
        self.assertLess(sum(item["end_line"] - item["start_line"] + 1 for item in code), 50)

    def test_large_scopes_are_deterministic_complete_and_not_extra_requests(self):
        self.commit_task()
        (self.task_path / "large.py").write_text("\n".join(f"value_{i} = {i}" for i in range(4000)) + "\n")
        self.updated_head()
        packets = [review.prepare(self.repo, 31, 12, 1234) / "packet" for _ in range(2)]
        scopes = [coverage.read_json(packet / "scopes.json") for packet in packets]
        self.assertEqual(scopes[0], scopes[1])
        self.assertEqual(scopes[0]["requests"], 1)
        required = coverage.read_json(packets[0] / "required-material.json")["required"]
        ids = [identifier for scope in scopes[0]["scopes"] for identifier in scope["required_ids"]]
        self.assertEqual(sorted(ids), sorted(item["id"] for item in required))
        self.assertEqual(len(ids), len(set(ids)))
        for scope in scopes[0]["scopes"]:
            self.assertLessEqual(scope["lines"], review_packet.SCOPE_LINES)
            self.assertLessEqual(scope["bytes"], review_packet.SCOPE_BYTES)

    def test_doc_only_change_does_not_require_all_product_tests(self):
        (self.root / "tests").mkdir()
        (self.root / "tests/test_downloader.py").write_text("assert True\n")
        git(self.root, "add", ".")
        git(self.root, "commit", "-m", "unrelated test")
        git(self.root, "push", "origin", "trunk")
        self.base = git(self.root, "rev-parse", "HEAD")
        self.commit_task()
        (self.task_path / "code.py").write_text("value = 1\n")
        (self.task_path / "README.md").write_text("# Documentation\nSmall correction.\n")
        self.updated_head()
        packet = review.prepare(self.repo, 31, 12, 1234) / "packet"
        required = coverage.read_json(packet / "required-material.json")["required"]
        self.assertFalse(any(item["path"] == "tests/test_downloader.py" for item in required))

    def test_repair_preserves_unread_material_without_recursive_policy_growth(self):
        self.commit_task()
        previous = review.prepare(self.repo, 31, 12, 1234)
        required = coverage.read_json(previous / "packet/required-material.json")["required"]
        unread = next(
            item for item in required if item["kind"] == "changed-source" and item["revision"] == "head"
        )
        store(self.repo, previous, omit=[unread["id"]])
        (self.task_path / "code.py").write_text("value = 3\n")
        self.updated_head()
        current = review.prepare(self.repo, 31, 12, 1234, prior_review=previous)
        now = coverage.read_json(current / "packet/required-material.json")["required"]
        self.assertTrue(any(item["id"] == unread["id"] for item in now))
        self.assertTrue((current / "packet/repair-delta.txt").is_file())
        self.assertEqual(
            len([i for i in now if i["kind"] == "policy"]),
            len([i for i in required if i["kind"] == "policy"]),
        )
        store(self.repo, current)
        third = review.prepare(self.repo, 31, 12, 1234, prior_review=current)
        later = coverage.read_json(third / "packet/required-material.json")["required"]
        self.assertLessEqual(len(later), len(now) + 2)

    def test_invalid_prior_repository_ancestry_and_missing_coverage_are_refused(self):
        self.commit_task()
        previous = review.prepare(self.repo, 31, 12, 1234)
        store(self.repo, previous)
        metadata = coverage.read_json(previous / "metadata.json")
        for key, value in (("repository", "other/project"), ("head_sha", "a" * 40), ("schema_version", 1)):
            workflow.write_json(previous / "metadata.json", {**metadata, key: value})
            with self.subTest(key=key), self.assertRaises(workflow.WorkflowError):
                review.prepare(self.repo, 31, 12, 1234, prior_review=previous)
        workflow.write_json(previous / "metadata.json", metadata)
        (previous / "coverage.json").unlink()
        with self.assertRaises(workflow.WorkflowError):
            review.prepare(self.repo, 31, 12, 1234, prior_review=previous)

    def test_prior_public_coverage_does_not_recursively_expand_required_findings(self):
        self.commit_task()
        previous = review.prepare(self.repo, 31, 12, 1234)
        store(self.repo, previous)
        report = review.publication_body(previous)
        self.reviews.append({"id": 9, "body": report, "commit_id": self.head, "state": "COMMENTED"})
        current = review.prepare(self.repo, 31, 12, 1234, prior_review=previous)
        public = (current / "packet/findings/reviews-0000.txt").read_text()
        self.assertIn("Not inherited", public)
        self.assertNotIn('"locations":', public)
        self.assertLess(len(public), len(report))
        self.assertEqual(coverage.read_json(current / "packet/context.json")["reviews"][0]["body"], report)
        required = coverage.read_json(current / "packet/required-material.json")["required"]
        self.assertFalse(any(row.get("omitted") for row in required))

    def test_ci_missing_skipped_stale_and_unknown_checkout_remain_visible(self):
        context = {
            "pull_request": {"head": {"sha": "a" * 40}},
            "commit_statuses": [],
            "check_runs": [
                {"name": "quality", "head_sha": "b" * 40, "conclusion": "success"},
                {"name": "quality", "head_sha": "a" * 40, "conclusion": "skipped"},
            ],
        }
        data = review_packet.validation_evidence(context, ["quality", "missing"])
        observations = data["required_checks"][0]["observations"]
        self.assertEqual([item["state"] for item in observations], ["stale", "skipped"])
        self.assertTrue(all(item["tested_checkout_sha"] is None for item in observations))
        self.assertEqual(data["required_checks"][1]["state"], "missing")

    def test_binary_changed_material_is_retained_as_unsupported(self):
        self.commit_task()
        (self.task_path / "unsupported.bin").write_bytes(b"\0\xff")
        self.updated_head()
        directory = review.prepare(self.repo, 31, 12, 1234)
        required = coverage.read_json(directory / "packet/required-material.json")["required"]
        omitted = [row for row in required if row["path"] == "unsupported.bin" and row.get("omitted")]
        self.assertEqual(len(omitted), 1)
        store(self.repo, directory)
        self.assertFalse(review.qualification(directory)["qualified"])
        with self.assertRaises(workflow.WorkflowError):
            review.qualification(directory, require=True)
