"""Synthetic batch accounting against real immutable local Git packets; no paid calls."""

import copy
import json
import re
from unittest.mock import patch

from review_fixtures import events
from test_workflow import GitFixture, review, workflow

# isort: split
import batch_fixtures
import review_batch as batch
import review_cli
import review_coverage as coverage
import review_policy
import review_process
from tasks import atomic_json


class BatchTests(GitFixture):
    def setUp(self):
        super().setUp()
        self.commit_task()
        self.directory = review.prepare(self.repo, 31, 12, 1234)
        self.limits = batch_fixtures.limits()
        self.calls = []
        self.usage = {"totalNanoAiu": 100_000_000}
        self.omit = []

    def provider(self, repo, directory, **kwargs):
        self.assertEqual(kwargs["dispatch_context"]["parent"], str(self.directory))
        meta = review.verify_packet(directory)
        state = batch.state_for(self.directory, batch.load(self.directory))
        self.assertEqual(state["reservations"][-1]["binding"]["unit"], meta["batch_unit"]["unit"]["id"])
        self.assertLessEqual(meta["review_policy"]["budget"]["timeout_seconds"], 60)
        self.calls.append(directory)
        packet = directory / "packet"
        original = events(packet, omit=self.omit)
        document = json.loads(original[-2]["data"]["content"])
        document["reviewed"] = [
            key for key in document["reviewed"] if key in meta["batch_unit"]["required_ids"]
        ]
        original[-2]["data"]["content"] = json.dumps(document, ensure_ascii=False) + "\r\n"
        body, diagnostics = coverage.parse_events(
            "\n".join(json.dumps(row) for row in original),
            packet,
            packet,
            version="1.0.83",
            usage=self.usage,
        )
        review.save_result(directory, meta, body, diagnostics, "1.0.83")
        return review.recover_review(repo, directory)

    def execute(self, **kwargs):
        batch_fixtures.select(self.repo, self.directory, self.limits)
        with patch.object(review, "review", side_effect=self.provider):
            return batch.execute(self.repo, self.directory, **kwargs)

    def test_partition_binding_context_and_integration_preserve_parent_inventory(self):
        planned = batch.plan(self.directory)
        self.assertEqual(planned, batch.plan(self.directory))
        inventory = coverage.read_json(self.directory / "packet/required-material.json")["required"]
        ids = [key for unit in planned["units"] for key in unit["required_ids"]]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertEqual(set(ids), {item["id"] for item in inventory})
        self.assertEqual(planned["units"][-1]["kind"], "integration")
        self.assertTrue(any(unit.get("context_ids") for unit in planned["units"][:-1]))
        self.execute()
        assessment = review.qualification(self.directory, require=True)
        self.assertTrue(review.coverage_ready(self.directory))
        self.assertEqual(assessment["inspected_count"], len(inventory))
        self.assertEqual(len(self.calls), len(planned["units"]))
        for target in self.calls:
            self.assertFalse(review.coverage_ready(target))
            self.assertIn("assigned material complete", review.publication_body(target))
            with self.assertRaises(workflow.WorkflowError):
                review.qualification(target, require=True)
        integration = self.calls[-1]
        required = coverage.read_json(integration / "packet/required-material.json")["required"]
        schema = coverage.read_json(integration / "packet/report-schema.json")
        for item in required:
            self.assertRegex(item["id"], schema["properties"]["reviewed"]["items"]["pattern"])
        self.assertEqual(required[: len(inventory)], inventory)
        self.assertTrue(all(item["kind"] == "component-report" for item in required[len(inventory) :]))
        for target in self.calls[:-1]:
            retained = integration / "packet/component-reports" / (target.name + ".txt")
            self.assertEqual(retained.read_bytes(), (target / "review.md").read_bytes())
        calls = len(self.calls)
        self.execute(resume=True)
        self.assertEqual(len(self.calls), calls)

    def test_legacy_report_ids_and_incomplete_unit_labels(self):
        artifact = "sample.txt"
        packet = self.directory / "packet"
        (packet / artifact).write_text("report\n")
        self.assertEqual(len(batch.material_for_report(packet, artifact, "report\n", version=3)[0]["id"]), 64)
        self.assertEqual(len(batch.material_for_report(packet, artifact, "report\n", version=4)[0]["id"]), 24)
        (packet / artifact).unlink()
        self.omit = batch.plan(self.directory)["units"][0]["required_ids"]
        with self.assertRaises(workflow.WorkflowError):
            self.execute()
        self.assertIn("INCOMPLETE static inspection", review.publication_body(self.calls[0]))
        saved = (self.directory / "batch.json").read_bytes()
        with self.assertRaisesRegex(workflow.WorkflowError, "budget changed"):
            batch.select(self.directory, {**self.limits, "cost": "200"})
        self.assertEqual((self.directory / "batch.json").read_bytes(), saved)

    def test_valid_findings_survive_missing_capability_without_source_credit(self):
        from review_fixtures import events as original_events

        finding = {
            "id": "retained-finding",
            "severity": "P2",
            "path": "task.py",
            "line": 1,
            "claim": "Synthetic finding",
            "trigger": "fixture",
            "impact": "fixture",
            "evidence": "fixture",
            "fix": "fixture",
        }

        def missing_probe(packet, **kwargs):
            rows = original_events(packet, **kwargs)
            document = json.loads(rows[-2]["data"]["content"])
            document["findings"] = [finding]
            rows[-2]["data"]["content"] = json.dumps(document)
            # Remove both actual grep events, preserving all source views.
            return rows[:3] + rows[5:]

        with patch(__name__ + ".events", side_effect=missing_probe):
            with self.assertRaises(workflow.WorkflowError):
                self.execute()
        result = review.qualification(self.directory)
        self.assertEqual(result["findings"], [{"unit": "unit-0000", "finding": finding}])
        self.assertFalse(result["qualified"])
        self.assertEqual(result["inspected_count"], 0)

    def test_single_file_packet_always_requires_cross_boundary_integration(self):
        packet = self.directory / "packet"
        self.assertEqual(len(coverage.read_json(packet / "changed-files.json")), 1)
        inventory = coverage.read_json(packet / "required-material.json")
        cross = {item["id"] for item in inventory["required"] if item["kind"] == "cross-boundary"}
        self.assertTrue(cross)
        self.assertEqual(set(batch.plan(self.directory)["units"][-1]["required_ids"]), cross)
        # An externally reduced packet must not turn the mandatory integration
        # obligation into a vacuous empty assignment, even if scopes partition it.
        inventory["required"] = [item for item in inventory["required"] if item["id"] not in cross]
        atomic_json(packet / "required-material.json", inventory)
        scopes = coverage.read_json(packet / "scopes.json")
        for scope in scopes["scopes"]:
            scope["required_ids"] = [key for key in scope["required_ids"] if key not in cross]
        scopes["scopes"] = [scope for scope in scopes["scopes"] if scope["required_ids"]]
        atomic_json(packet / "scopes.json", scopes)
        (packet / "inventory-sha256.txt").write_text(review.digest(packet / "required-material.json") + "\n")
        meta = coverage.read_json(self.directory / "metadata.json")
        meta["files"] = {
            p.relative_to(packet).as_posix(): review.digest(p) for p in packet.rglob("*") if p.is_file()
        }
        atomic_json(self.directory / "metadata.json", meta)
        with self.assertRaisesRegex(workflow.WorkflowError, "integration obligation"):
            batch.plan(self.directory)

    def test_test_map_context_reaches_order_independent_closure(self):
        packet = self.directory / "packet"
        inventory = coverage.read_json(packet / "required-material.json")["required"]
        scope = coverage.read_json(packet / "scopes.json")["scopes"][0]
        primary = set(scope["required_ids"])
        path = next(item["path"] for item in inventory if item["id"] in primary)
        primary_paths = {item["path"] for item in inventory if item["id"] in primary}
        target = next(item for item in inventory if item["path"] not in primary_paths)
        mappings = [
            {"changed_path": "bridge", "candidates": [target["path"]]},
            {"changed_path": path, "candidates": ["bridge"]},
        ]
        results = []
        for rows in (mappings, list(reversed(mappings))):
            atomic_json(packet / "test-map.json", rows)
            meta = coverage.read_json(self.directory / "metadata.json")
            meta["files"]["test-map.json"] = review.digest(packet / "test-map.json")
            atomic_json(self.directory / "metadata.json", meta)
            results.append(batch.plan(self.directory)["units"][0]["context_ids"])
        self.assertEqual(results[0], results[1])
        self.assertIn(target["id"], results[0])

    def test_legacy_batch_plan_is_validated_without_new_navigation_semantics(self):
        import review_batch_v4 as historical

        meta = coverage.read_json(self.directory / "metadata.json")
        for key in ("kind", "review_policy", "selection_sources"):
            meta.pop(key, None)
        meta["schema_version"] = 3
        atomic_json(self.directory / "metadata.json", meta)
        for version in (1, 2, 3, 4):
            saved = {
                **historical.plan(self.directory, version=version),
                "budget": historical.budget(100, 100, 6000, 1, 60),
            }
            meta.update(schema_version=4, batch_sha256=batch.digest(saved))
            atomic_json(self.directory / "batch.json", saved)
            atomic_json(self.directory / "metadata.json", meta)
            self.assertEqual(historical.load(self.directory), saved)
            self.assertEqual(saved["schema_version"], version)
            with self.assertRaisesRegex(workflow.WorkflowError, "recovery-only"):
                historical.execute(self.repo, self.directory)
        with self.assertRaises(workflow.WorkflowError):
            batch.plan(self.directory)

    def test_suggested_ranges_preserve_blank_boundary_evidence(self):
        packet = self.directory / "packet"
        text = "".join(f"line {n}\n" for n in range(1, 124)) + "\ncontext\n\n"
        (packet / "boundary.txt").write_text(text)
        items = [
            {"id": "middle", "artifact": "boundary.txt", "start_line": 94, "end_line": 124},
            {"id": "eof", "artifact": "boundary.txt", "start_line": 125, "end_line": 126},
            {"id": "omitted", "artifact": None, "omitted": "unavailable"},
        ]
        before = copy.deepcopy(items)
        hints = batch.inspection_suggestions(packet, items)
        self.assertEqual(items, before)
        self.assertEqual(hints[0]["view_range"], [94, 125])
        self.assertEqual(hints[1]["grep_blank_lines"], [126, 126])
        self.assertEqual(hints[1]["view_range"], [125, 125])
        self.assertEqual(hints[2]["state"], "unavailable")
        (packet / "separators.txt").write_text("one\fnext\u2028last\n")
        separated = batch.inspection_suggestions(
            packet, [{"id": "separators", "artifact": "separators.txt", "start_line": 2, "end_line": 3}]
        )[0]
        self.assertEqual(separated["view_range"], [2, 3])
        files = {"boundary.txt": text}
        chunks = text.splitlines(keepends=True)
        spans, _, reason = coverage.tool_observation(
            "view",
            {"path": "boundary.txt", "view_range": [94, 124]},
            "".join(chunks[93:124])[:-1],
            packet,
            files,
        )
        self.assertFalse(spans)
        self.assertEqual(reason, "ambiguous_or_out_of_range_view")
        spans, _, _ = coverage.tool_observation(
            "view",
            {"path": "boundary.txt", "view_range": hints[0]["view_range"]},
            "".join(chunks[93:125])[:-1],
            packet,
            files,
        )
        self.assertEqual((spans[0]["start_line"], spans[0]["end_line"]), (94, 125))
        spans, _, _ = coverage.tool_observation(
            "grep",
            {"path": "boundary.txt", "pattern": "^$"},
            "boundary.txt:126:",
            packet,
            files,
        )
        self.assertEqual((spans[0]["start_line"], spans[0]["end_line"]), (126, 126))
        self.assertFalse(coverage.tool_observation("grep", {}, "", packet, files)[0])

    def test_eof_suggestions_credit_nonblank_prefix_and_whitespace_tail(self):
        packet = self.directory / "packet"
        for tail in ("\n", "   \n", "\t\n\n"):
            text = "prefix\n" + tail
            (packet / "eof.txt").write_text(text)
            lines = text.splitlines(keepends=True)
            for start in (1, 2):
                item = {"id": "eof", "artifact": "eof.txt", "start_line": start, "end_line": len(lines)}
                hint = batch.inspection_suggestions(packet, [item])[0]
                self.assertEqual(hint.get("view_range"), [1, 1] if start == 1 else None)
                covered = set()
                if "view_range" in hint:
                    lo, hi = hint["view_range"]
                    spans, _, _ = coverage.tool_observation(
                        "view",
                        {"path": "eof.txt", "view_range": [lo, hi]},
                        "".join(lines[lo - 1 : hi]).removesuffix("\n"),
                        packet,
                        {"eof.txt": text},
                    )
                    for span in spans:
                        covered.update(range(span["start_line"], span["end_line"] + 1))
                matches = "\n".join(
                    f"eof.txt:{n}:{line}"
                    for n, line in enumerate(text.splitlines(), 1)
                    if re.fullmatch(hint["grep_pattern"], line)
                )
                spans, _, _ = coverage.tool_observation(
                    "grep",
                    {"path": "eof.txt", "pattern": hint["grep_pattern"]},
                    matches,
                    packet,
                    {"eof.txt": text},
                )
                for span in spans:
                    covered.update(range(span["start_line"], span["end_line"] + 1))
                self.assertTrue(set(range(start, len(lines) + 1)) <= covered)

    def test_missing_or_invalid_budgets_fail_before_inference(self):
        for key in self.limits:
            for value in (None, 0, -1, True, float("inf"), float("nan")):
                with self.subTest(key=key, value=value), self.assertRaises(workflow.WorkflowError):
                    review_policy.batch_budget(
                        {**self.limits, key: value},
                        review.verify_packet(self.directory)["review_policy"],
                        len(batch.plan(self.directory)["units"]),
                    )
        with self.assertRaises(workflow.WorkflowError):
            review_policy.batch_budget(
                {**self.limits, "unit_cost": "101"}, review.verify_packet(self.directory)["review_policy"], 1
            )
        self.assertEqual(self.calls, [])

    def test_unknown_usage_exhaustion_and_incomplete_inspection_stop_dispatch(self):
        self.usage = None
        with self.assertRaisesRegex(workflow.WorkflowError, "usage; dispatch stopped"):
            self.execute()
        self.assertEqual(len(self.calls), 1)
        self.assertFalse(review.coverage_ready(self.directory))
        with self.assertRaisesRegex(workflow.WorkflowError, "durably stopped"):
            self.execute(resume=True)
        self.assertEqual(len(self.calls), 1)

    def test_request_bound_and_no_implicit_resume(self):
        self.limits["requests"] = 1
        with self.assertRaisesRegex(workflow.WorkflowError, "cannot fund every"):
            self.execute()
        self.assertEqual(self.calls, [])
        self.assertFalse((self.directory / "batch-state.json").exists())
        self.limits["requests"] = 100
        self.execute()
        with self.assertRaisesRegex(workflow.WorkflowError, "already started"):
            self.execute()
        count = len(self.calls)
        self.execute(resume=True)
        self.assertEqual(len(self.calls), count)

    def test_uncertain_reservation_never_retries(self):
        batch_fixtures.select(self.repo, self.directory, self.limits)
        with (
            patch.object(review, "review", side_effect=KeyboardInterrupt),
            self.assertRaises(KeyboardInterrupt),
        ):
            batch.execute(self.repo, self.directory)
        with patch.object(review, "review") as provider:
            with self.assertRaisesRegex(workflow.WorkflowError, "durably stopped"):
                batch.execute(self.repo, self.directory, resume=True)
            provider.assert_not_called()
        self.assertFalse(review.coverage_ready(self.directory))

    def test_exact_bytes_and_parent_binding_are_revalidated(self):
        self.execute()
        report = self.calls[0] / "review.md"
        original = report.read_bytes()
        report.write_bytes(original + b" ")
        with self.assertRaises(workflow.WorkflowError):
            review.qualification(self.directory, require=True)
        report.write_bytes(original)
        self.assertTrue(review.coverage_ready(self.directory))
        meta_path = self.directory / "metadata.json"
        meta = coverage.read_json(meta_path)
        for key in ("head_sha", "base_sha", "issue", "plan_comment", "config"):
            changed = copy.deepcopy(meta)
            changed[key] = "different"
            atomic_json(meta_path, changed)
            with self.subTest(key=key), self.assertRaises(workflow.WorkflowError):
                review.qualification(self.directory, require=True)
        atomic_json(meta_path, meta)

    def test_unit_cannot_execute_outside_budget_dispatch(self):
        self.execute()
        with self.assertRaisesRegex(workflow.WorkflowError, "aggregate reservation"):
            review.run_review(self.repo, self.calls[0])

    def test_aggregate_cannot_invent_source_inspection(self):
        planned = batch.plan(self.directory)
        self.omit = planned["units"][0]["required_ids"]
        with self.assertRaisesRegex(workflow.WorkflowError, "Incomplete unit"):
            self.execute()
        self.assertEqual(len(self.calls), 1)
        self.assertFalse(review.coverage_ready(self.directory))

    def test_credit_overshoot_stops_following_calls(self):
        self.usage = {"totalNanoAiu": 200_000_000_000}
        with self.assertRaisesRegex(workflow.WorkflowError, "over-allocation"):
            self.execute()
        self.assertEqual(len(self.calls), 1)

    def test_deadline_persists_and_expired_resume_cannot_call(self):
        clock_values = iter([1000, 1000, 1000, 7001])
        with self.assertRaisesRegex(workflow.WorkflowError, "expired authorization"):
            self.execute(clock=lambda: next(clock_values))
        state = batch.state_for(self.directory, batch.load(self.directory))
        self.assertEqual(state["deadline"], 7000)
        with self.assertRaises(workflow.WorkflowError):
            self.execute(resume=True, clock=lambda: 7001)
        self.assertEqual(len(self.calls), 1)

    def test_saved_capture_recovers_without_repeating_attempt(self):
        batch_fixtures.select(self.repo, self.directory, self.limits)
        original = review.recover_review

        def interrupted(repo, directory):
            if directory.name.startswith("unit-"):
                raise OSError("synthetic storage interruption")
            return original(repo, directory)

        with (
            patch.object(review, "review", side_effect=self.provider),
            patch.object(review, "recover_review", side_effect=interrupted),
        ):
            with self.assertRaises(OSError):
                batch.execute(self.repo, self.directory)
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(
            batch.state_for(self.directory, batch.load(self.directory))["reservations"][0]["status"],
            "captured",
        )
        with patch.object(review, "review") as provider:
            batch.execute(self.repo, self.directory, recover_only=True)
            provider.assert_not_called()
        self.assertFalse(review.coverage_ready(self.directory))
        with self.assertRaisesRegex(workflow.WorkflowError, "durably stopped"):
            self.execute(resume=True)
        self.assertEqual(len(self.calls), 1)
        self.assertFalse(review.coverage_ready(self.directory))

    def test_missing_integration_report_reads_block_completion(self):
        original = self.provider

        def provider(repo, directory, **kwargs):
            if directory.name == "integration":
                inventory = coverage.read_json(directory / "packet/required-material.json")["required"]
                self.omit = [item["id"] for item in inventory if item["kind"] == "component-report"]
            return original(repo, directory, **kwargs)

        batch_fixtures.select(self.repo, self.directory, self.limits)
        with patch.object(review, "review", side_effect=provider), self.assertRaises(workflow.WorkflowError):
            batch.execute(self.repo, self.directory)
        result = review.qualification(self.directory)
        self.assertFalse(result["qualified"])
        # Parent material alone is insufficient: integration must read reports.
        self.assertEqual(result["required_count"], result["inspected_count"])
        with self.assertRaises(workflow.WorkflowError):
            review.qualification(self.directory, require=True)

    def test_expired_and_stale_head_refuse_new_calls(self):
        batch_fixtures.select(self.repo, self.directory, self.limits)
        self.pr_data["head"]["sha"] = "0" * 40
        with patch.object(review, "review") as provider, self.assertRaises(workflow.WorkflowError):
            batch.execute(self.repo, self.directory)
        provider.assert_not_called()

    def test_malformed_missing_probe_and_masked_reads_remain_incomplete(self):
        original_events = events
        for mode in ("malformed", "probe", "masked", "truncated"):
            self.directory = review.prepare(self.repo, 31, 12, 1234)
            self.calls = []

            def altered(packet, mode=mode, **kwargs):
                rows = original_events(packet, **kwargs)
                if mode == "malformed":
                    # Valid outer JSON syntax with an invalid report contract.
                    document = json.loads(rows[-2]["data"]["content"])
                    document["inventory_sha256"] = "0" * 64
                    rows[-2]["data"]["content"] = json.dumps(document)
                elif mode == "probe":
                    rows[6]["data"]["result"]["content"] = ""
                else:
                    for row in rows[7:]:
                        if row["type"] == "tool.execution_complete":
                            row["data"]["result"]["content"] = (
                                "[MASKED]" if mode == "masked" else "[output truncated]"
                            )
                return rows

            with self.subTest(mode=mode), patch(__name__ + ".events", side_effect=altered):
                with self.assertRaises(workflow.WorkflowError):
                    self.execute()
                self.assertEqual(len(self.calls), 1)
                self.assertFalse(review.coverage_ready(self.directory))

    def test_contract_change_and_omitted_test_obligations_are_not_waived(self):
        planned = batch.plan(self.directory)
        inventory_path = self.directory / "packet/required-material.json"
        inventory = coverage.read_json(inventory_path)
        item = inventory["required"][0]
        item.update(kind="test", omitted="Required test source unavailable")
        atomic_json(inventory_path, inventory)
        (self.directory / "packet/inventory-sha256.txt").write_text(review.digest(inventory_path) + "\n")
        meta = coverage.read_json(self.directory / "metadata.json")
        for name in ("required-material.json", "inventory-sha256.txt"):
            meta["files"][name] = review.digest(self.directory / "packet" / name)
        atomic_json(self.directory / "metadata.json", meta)
        self.assertEqual(
            {key for unit in batch.plan(self.directory)["units"] for key in unit["required_ids"]},
            {key for unit in planned["units"] for key in unit["required_ids"]},
        )
        with self.assertRaises(workflow.WorkflowError):
            self.execute()
        result = review.qualification(self.directory)
        self.assertFalse(result["qualified"])
        self.assertEqual(
            next(row for row in result["material"] if row["id"] == item["id"])["state"], "unsupported"
        )
        self.issue["body"] += "Changed contract"
        with (
            patch.object(review, "review") as provider,
            self.assertRaisesRegex(workflow.WorkflowError, "contract changed"),
        ):
            batch.execute(self.repo, self.directory, resume=True)
        provider.assert_not_called()

    def test_batch_prior_preserves_exact_unit_reports_without_inheriting_readiness(self):
        self.execute()
        prior = self.directory
        next_packet = review.prepare(self.repo, 31, 12, 1234, prior_review=prior)
        required = coverage.read_json(next_packet / "packet/required-material.json")["required"]
        obligated_reports = {item["artifact"] for item in required if item["kind"] == "finding"}
        for target in self.calls:
            self.assertIn(f"prior-unit-reports/{target.name}.txt", obligated_reports)
        for target in self.calls:
            self.assertEqual(
                (next_packet / "packet/prior-unit-reports" / (target.name + ".txt")).read_bytes(),
                (target / "review.md").read_bytes(),
            )
        self.assertFalse((next_packet / "review.md").exists())

    def test_actual_invocations_keep_isolation_and_reservation_limits(self):
        import os
        import subprocess
        import time
        from pathlib import Path

        from review_fixtures import HELP, provider_response

        batch_fixtures.select(self.repo, self.directory, self.limits)
        original = review.run
        homes = []
        snapshots = []

        def cli(args, **kwargs):
            if args[0] != "/fixture/copilot":
                return original(args, **kwargs)
            if args[1] == "--help":
                return subprocess.CompletedProcess(args, 0, HELP, "")
            if args[1] == "--version":
                return subprocess.CompletedProcess(args, 0, "1.0.83", "")
            prompt = args[args.index("--prompt") + 1]
            probe = coverage.read_json(Path(kwargs["cwd"]) / "capability.json")
            self.assertIn('view({"path": "capability/fixture.txt", "view_range": [1, 2]})', prompt)
            self.assertIn(
                "grep("
                + json.dumps(
                    {
                        "path": "capability/fixture.txt",
                        "pattern": probe["token"],
                        "output_mode": "content",
                        "-n": True,
                    }
                )
                + ")",
                prompt,
            )
            self.assertIn('glob({"pattern": "capability/*.txt"})', prompt)
            self.assertIn("grep is required even if no source range needs it", prompt)
            self.assertIn("invalidates the entire unit", prompt)
            self.assertIn("The final assistant message itself must be JSON-only", prompt)
            self.assertIn("Do not announce report emission", prompt)
            self.assertIn("Return exactly one JSON object", prompt)
            self.assertIn(
                "Do not add introductory prose, markdown fences, or text outside that object", prompt
            )
            self.assertIn("inspect every required_ids entry", prompt)
            self.assertIn("1-based inclusive", prompt)
            self.assertIn("inspection_suggestions", prompt)
            self.assertNotIn("inspect EVERY required-material.json entry", prompt)
            self.assertNotIn("then cover the full inventory", prompt)
            self.assertIn("--available-tools=view,grep,glob", args)
            self.assertIn("--allow-tool=view,grep,glob", args)
            self.assertIn("--no-custom-instructions", args)
            self.assertIn("--disable-builtin-mcps", args)
            self.assertEqual(args[args.index("--max-ai-credits") + 1], "1")
            self.assertLessEqual(kwargs["timeout"], 60)
            state = batch.state_for(self.directory, batch.load(self.directory))
            self.assertLess(time.time(), state["deadline"])
            self.assertEqual(len(state["reservations"]), len(homes) + 1)
            homes.append(kwargs["env"]["COPILOT_HOME"])
            self.assertNotIn("COPILOT_PROVIDER_BASE_URL", kwargs["env"])
            snapshots.append(Path(kwargs["cwd"]))
            atomic_json(Path(args[args.index("--usage-output-file") + 1]), self.usage)
            return subprocess.CompletedProcess(args, 0, provider_response(args, kwargs, kwargs["cwd"]), "")

        with (
            patch.dict(
                os.environ, {"COPILOT_GITHUB_TOKEN": "test-token", "COPILOT_PROVIDER_BASE_URL": "invalid"}
            ),
            patch.object(review, "run", side_effect=cli),
            patch.object(review_cli, "executable", return_value="/fixture/copilot"),
            patch.object(review_process, "capture", side_effect=cli),
            patch.object(batch, "verify_harness"),
        ):
            batch.execute(self.repo, self.directory)
        self.assertEqual(len(homes), len(set(homes)))
        self.assertTrue(all(not path.exists() for path in snapshots))
        self.assertTrue(review.coverage_ready(self.directory))


# Exercise the real managed/publication gates with local Git and mocked GitHub.
from test_pipeline import PipelineFixture  # noqa: E402

# isort: split
import pipeline  # noqa: E402
import tasks  # noqa: E402


class BatchPipelineTests(PipelineFixture):
    def authorization(self, limits):
        record = pipeline.review_task(self.repo, 12, batch=True)
        return batch_fixtures.authorize(self.repo, record["directory"], limits)

    def model(self, repo, directory, **kwargs):
        packet = directory / "packet"
        raw = "\n".join(json.dumps(row) for row in events(packet))
        body, diagnostics = coverage.parse_events(
            raw, packet, packet, version="1.0.83", usage={"totalNanoAiu": 100}
        )
        review.save_result(directory, review.verify_packet(directory), body, diagnostics, "1.0.83")
        return review.recover_review(repo, directory)

    def test_managed_batch_preview_publication_designation_and_exact_unit_gate(self):
        with patch.object(review, "review", side_effect=self.model) as provider:
            preview = pipeline.review_task(self.repo, 12, batch=True)
            provider.assert_not_called()
            self.assertTrue(preview["batch_preview"]["units"])
            limits = batch_fixtures.limits()
            result = pipeline.review_task(
                self.repo,
                12,
                batch=True,
                batch_limits=limits,
                batch_authorization=self.authorization(limits),
                execute=True,
                publish=True,
            )
        self.assertEqual(result["status"], "published")
        with patch.object(review, "review") as provider:
            recovered = pipeline.review_task(self.repo, 12, batch=True, execute=True)
            self.assertEqual(recovered["status"], "published")
            provider.assert_not_called()
        state = tasks.TaskStore(self.repo).read("issue-12")
        pipeline.validate_designated(self.repo, state)
        for key in ("batch_sha256", "authorization_digest"):
            changed = copy.deepcopy(state)
            changed["review_rounds"][-1][key] = "0" * 64
            with self.subTest(binding=key), self.assertRaises(workflow.WorkflowError):
                pipeline.validate_designated(self.repo, changed)
        self.assertTrue(review.verify_publication(self.repo, result["directory"])["exact_match"])
        review.verified_published(self.repo, result["directory"], 31, self.head, self.base)
        self.assertGreater(len(self.reviews), 1)
        original = self.reviews[0]["body"]
        self.reviews[0]["body"] += "altered"
        with self.assertRaises(workflow.WorkflowError):
            pipeline.validate_designated(self.repo, state)
        with self.assertRaises(workflow.WorkflowError):
            review.verified_published(self.repo, result["directory"], 31, self.head, self.base)
        with self.assertRaises(workflow.WorkflowError):
            review.verify_publication(self.repo, result["directory"])
        self.reviews[0]["body"] = original
        self.pr_data["base"]["sha"] = "0" * 40
        with self.assertRaises(workflow.WorkflowError):
            pipeline.validate_designated(self.repo, state)

    def test_managed_missing_budget_does_not_attempt_or_publish(self):
        with patch.object(review, "review") as provider, self.assertRaises(workflow.WorkflowError):
            pipeline.review_task(self.repo, 12, batch=True, execute=True)
        provider.assert_not_called()
        self.assertFalse(self.reviews)

    def test_partial_batch_publication_never_designates_readiness(self):
        from pathlib import Path

        limits = batch_fixtures.limits()

        def incomplete(repo, directory, **kwargs):
            self.model(repo, directory, **kwargs)
            raise workflow.WorkflowError("synthetic interruption after capture")

        with patch.object(review, "review", side_effect=incomplete):
            with self.assertRaises(workflow.WorkflowError):
                pipeline.review_task(
                    self.repo,
                    12,
                    batch=True,
                    batch_limits=limits,
                    batch_authorization=self.authorization(limits),
                    execute=True,
                )
        result = pipeline.review_task(self.repo, 12, batch=True, publish=True)
        self.assertEqual(result["status"], "published-incomplete")
        self.assertIsNone(result["designated_review"])
        unit_review = next(r for r in self.reviews if "batch unit" in r["body"])
        unit_review["body"] += "tampered"
        with self.assertRaises(workflow.WorkflowError):
            pipeline.review_task(self.repo, 12, batch=True, publish=True)
        self.assertIn("INCOMPLETE", review.publication_body(Path(result["directory"])))
        with self.assertRaises(workflow.WorkflowError):
            review.qualification(result["directory"], require=True)

    def test_hosted_qualify_and_low_level_preflight_use_batch_gate(self):
        import contextlib
        import io
        import subprocess
        import sys

        limits = batch_fixtures.limits()
        with patch.object(review, "review", side_effect=self.model):
            result = pipeline.review_task(
                self.repo,
                12,
                batch=True,
                batch_limits=limits,
                batch_authorization=self.authorization(limits),
                execute=True,
                publish=True,
            )
        directory = result["directory"]
        with (
            patch.object(review, "Repo", return_value=self.repo),
            patch.object(sys, "argv", ["review.py", "qualify", directory]),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            self.assertEqual(review.main(), 0)
        original = workflow.run

        def cli(args, **kwargs):
            if args[0] == "gh":
                return subprocess.CompletedProcess(
                    args, 0, json.dumps([{"name": "quality", "bucket": "pass"}]), ""
                )
            return original(args, **kwargs)

        with patch.object(workflow, "run", side_effect=cli):
            self.assertIn("command", workflow.merge_preflight(self.repo, 31, self.head, directory))
            self.reviews[0]["body"] += " altered"
            with self.assertRaises(workflow.WorkflowError):
                workflow.merge_preflight(self.repo, 31, self.head, directory)
        self.pr_data["head"]["sha"] = "0" * 40
        with (
            patch.object(review, "Repo", return_value=self.repo),
            patch.object(sys, "argv", ["review.py", "qualify", directory]),
            contextlib.redirect_stderr(io.StringIO()),
        ):
            self.assertEqual(review.main(), 1)
