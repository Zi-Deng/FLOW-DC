"""Synthetic batch accounting against real immutable local Git packets; no paid calls."""

import copy
import json
from unittest.mock import patch

from review_fixtures import events
from test_workflow import GitFixture, review, workflow

# isort: split
import review_batch as batch
import review_coverage as coverage
from tasks import atomic_json


class BatchTests(GitFixture):
    def setUp(self):
        super().setUp()
        self.commit_task()
        self.directory = review.prepare(self.repo, 31, 12, 1234)
        self.limits = dict(requests=100, credits=100, seconds=600, unit_credits=1, unit_seconds=60)
        self.calls = []
        self.usage = {"totalNanoAiu": 100_000_000}
        self.omit = []

    def provider(self, repo, directory, **kwargs):
        self.assertTrue(kwargs["_batch_authorized"])
        meta = review.verify_packet(directory)
        state = batch.state_for(self.directory, batch.load(self.directory))
        self.assertEqual(state["reservations"][-1]["unit"], meta["batch_unit"]["unit"]["id"])
        self.assertLessEqual(meta["config"]["review_timeout_seconds"], 60)
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
        batch.select(self.directory, self.limits)
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
            with self.assertRaises(workflow.WorkflowError):
                review.qualification(target, require=True)
        integration = self.calls[-1]
        required = coverage.read_json(integration / "packet/required-material.json")["required"]
        self.assertEqual(required[: len(inventory)], inventory)
        self.assertTrue(all(item["kind"] == "component-report" for item in required[len(inventory) :]))
        for target in self.calls[:-1]:
            retained = integration / "packet/component-reports" / (target.name + ".txt")
            self.assertEqual(retained.read_bytes(), (target / "review.md").read_bytes())
        calls = len(self.calls)
        self.execute(resume=True)
        self.assertEqual(len(self.calls), calls)

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

    def test_missing_or_invalid_budgets_fail_before_inference(self):
        for key in self.limits:
            for value in (None, 0, -1, True, float("inf"), float("nan")):
                with self.subTest(key=key, value=value), self.assertRaises(workflow.WorkflowError):
                    batch.budget(**{**self.limits, key: value})
        with self.assertRaises(workflow.WorkflowError):
            batch.budget(**{**self.limits, "unit_credits": 101})
        self.assertEqual(self.calls, [])

    def test_unknown_usage_exhaustion_and_incomplete_inspection_stop_dispatch(self):
        self.usage = None
        with self.assertRaisesRegex(workflow.WorkflowError, "Unknown AI-credit"):
            self.execute()
        self.assertEqual(len(self.calls), 1)
        self.assertFalse(review.coverage_ready(self.directory))
        with self.assertRaisesRegex(workflow.WorkflowError, "Unknown AI-credit"):
            self.execute(resume=True)
        self.assertEqual(len(self.calls), 1)

    def test_request_bound_and_no_implicit_resume(self):
        self.limits["requests"] = 1
        with self.assertRaisesRegex(workflow.WorkflowError, "exhausted"):
            self.execute()
        self.assertEqual(len(self.calls), 1)
        with self.assertRaisesRegex(workflow.WorkflowError, "explicit resume"):
            self.execute()
        with self.assertRaisesRegex(workflow.WorkflowError, "exhausted"):
            self.execute(resume=True)
        self.assertEqual(len(self.calls), 1)
        with self.assertRaises(workflow.WorkflowError):
            batch.select(self.directory, {**self.limits, "requests": 100})

    def test_uncertain_reservation_never_retries(self):
        batch.select(self.directory, self.limits)
        with (
            patch.object(review, "review", side_effect=KeyboardInterrupt),
            self.assertRaises(KeyboardInterrupt),
        ):
            batch.execute(self.repo, self.directory)
        with patch.object(review, "review") as provider:
            with self.assertRaisesRegex(workflow.WorkflowError, "no automatic retry"):
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
        with self.assertRaisesRegex(workflow.WorkflowError, "Incomplete prior unit"):
            self.execute()
        self.assertEqual(len(self.calls), 1)
        self.assertFalse(review.coverage_ready(self.directory))

    def test_credit_overshoot_stops_following_calls(self):
        self.usage = {"totalNanoAiu": 200_000_000_000}
        with self.assertRaisesRegex(workflow.WorkflowError, "allocation exceeded"):
            self.execute()
        self.assertEqual(len(self.calls), 1)

    def test_deadline_persists_and_expired_resume_cannot_call(self):
        clock_values = iter([1000, 1000, 1601])
        with self.assertRaisesRegex(workflow.WorkflowError, "exhausted"):
            self.execute(clock=lambda: next(clock_values))
        state = batch.state_for(self.directory, batch.load(self.directory))
        self.assertEqual(state["deadline"], 1600)
        with self.assertRaises(workflow.WorkflowError):
            self.execute(resume=True, clock=lambda: 1601)
        self.assertEqual(len(self.calls), 1)

    def test_saved_capture_recovers_without_repeating_attempt(self):
        batch.select(self.directory, self.limits)
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
        with patch.object(review, "review") as provider:
            batch.execute(self.repo, self.directory, recover_only=True)
            provider.assert_not_called()
        self.assertFalse(review.coverage_ready(self.directory))
        self.execute(resume=True)
        self.assertTrue(review.coverage_ready(self.directory))
        self.assertEqual(len(self.calls), len(batch.plan(self.directory)["units"]))

    def test_missing_integration_report_reads_block_completion(self):
        original = self.provider

        def provider(repo, directory, **kwargs):
            if directory.name == "integration":
                inventory = coverage.read_json(directory / "packet/required-material.json")["required"]
                self.omit = [item["id"] for item in inventory if item["kind"] == "component-report"]
            return original(repo, directory, **kwargs)

        batch.select(self.directory, self.limits)
        with patch.object(review, "review", side_effect=provider):
            batch.execute(self.repo, self.directory)
        result = review.qualification(self.directory)
        self.assertFalse(result["qualified"])
        # Parent material alone is insufficient: integration must read reports.
        self.assertEqual(result["required_count"], result["inspected_count"])
        with self.assertRaises(workflow.WorkflowError):
            review.qualification(self.directory, require=True)

    def test_expired_and_stale_head_refuse_new_calls(self):
        batch.select(self.directory, self.limits)
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

        batch.select(self.directory, self.limits)
        original = review.run
        homes = []
        snapshots = []

        def cli(args, **kwargs):
            if args[0] != "copilot":
                return original(args, **kwargs)
            if args[1] == "--help":
                return subprocess.CompletedProcess(args, 0, HELP, "")
            if args[1] == "--version":
                return subprocess.CompletedProcess(args, 0, "1.0.83", "")
            prompt = args[args.index("--prompt") + 1]
            self.assertIn("Return exactly one JSON object", prompt)
            self.assertIn(
                "Do not add introductory prose, markdown fences, or text outside that object", prompt
            )
            self.assertIn("inspect every required_ids entry", prompt)
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
            limits = dict(requests=100, credits=100, seconds=600, unit_credits=1, unit_seconds=60)
            result = pipeline.review_task(
                self.repo, 12, batch=True, batch_limits=limits, execute=True, publish=True
            )
        self.assertEqual(result["status"], "published")
        with patch.object(review, "review") as provider:
            recovered = pipeline.review_task(self.repo, 12, batch=True, execute=True)
            self.assertEqual(recovered["status"], "published")
            provider.assert_not_called()
        state = tasks.TaskStore(self.repo).read("issue-12")
        pipeline.validate_designated(self.repo, state)
        self.assertTrue(review.verify_publication(self.repo, result["directory"])["exact_match"])
        review.verified_published(self.repo, result["directory"], 31, self.head, self.base)
        self.assertGreater(len(self.reviews), 1)
        original = self.reviews[0]["body"]
        self.reviews[0]["body"] += "altered"
        with self.assertRaises(workflow.WorkflowError):
            pipeline.validate_designated(self.repo, state)
        with self.assertRaises(workflow.WorkflowError):
            review.verified_published(self.repo, result["directory"], 31, self.head, self.base)
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

        limits = dict(requests=1, credits=100, seconds=600, unit_credits=1, unit_seconds=60)
        with patch.object(review, "review", side_effect=self.model):
            with self.assertRaises(workflow.WorkflowError):
                pipeline.review_task(self.repo, 12, batch=True, batch_limits=limits, execute=True)
        result = pipeline.review_task(self.repo, 12, batch=True, publish=True)
        self.assertEqual(result["status"], "published-incomplete")
        self.assertIsNone(result["designated_review"])
        self.assertIn("INCOMPLETE", review.publication_body(Path(result["directory"])))
        with self.assertRaises(workflow.WorkflowError):
            review.qualification(result["directory"], require=True)

    def test_hosted_qualify_and_low_level_preflight_use_batch_gate(self):
        import contextlib
        import io
        import subprocess
        import sys

        limits = dict(requests=100, credits=100, seconds=600, unit_credits=1, unit_seconds=60)
        with patch.object(review, "review", side_effect=self.model):
            result = pipeline.review_task(
                self.repo, 12, batch=True, batch_limits=limits, execute=True, publish=True
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
