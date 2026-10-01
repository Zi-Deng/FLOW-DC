"""PR and independent-review integration using Git fixtures and model doubles."""

from pathlib import Path
from unittest.mock import patch

from review_fixtures import store as store_review
from test_workflow import GitFixture, git, workflow

# The shared fixture establishes the scripts import path.
# isort: split
import pipeline
import review
import tasks


class PipelineFixture(GitFixture):
    def setUp(self):
        super().setUp()
        self.inline = []
        self.conversation = []
        self.commit_task()
        self.pr_data.update(
            {
                "number": 31,
                "id": 3100,
                "html_url": "https://github.com/example/project/pull/31",
                "title": "Earlier title",
                "body": "Earlier evidence\n\nFixes #12\n",
            }
        )
        tasks.approve_plan(self.repo, 12, 1234, "Explicit task authorization")
        tasks.prepare(self.repo, 12, "correct-value")
        pipeline.bind_pr(self.repo, 12, 31)
        self.body_file = self.parent / "pr-body.md"
        self.body_file.write_text("Current validation evidence\n\nFixes #12\n")
        self.model_runs = 0

    def api(self, suffix, *, data=None, **kwargs):
        if data is not None:
            self.posts.append((suffix, data))
            if suffix == "pulls/31":
                self.pr_data.update(data)
                return self.pr_data
            if suffix == "pulls/31/reviews":
                item = {
                    "id": 9000 + len(self.reviews),
                    "body": data["body"],
                    "commit_id": data["commit_id"],
                    "state": "COMMENTED",
                    "html_url": f"https://github.com/example/project/pull/31#review-{len(self.reviews)}",
                }
                self.reviews.append(item)
                return item
            if suffix == "issues/31/comments":
                item = {
                    "id": 8000 + len(self.conversation),
                    "body": data["body"],
                    "html_url": "https://github.com/example/project/pull/31#response",
                }
                self.conversation.append(item)
                return item
        if suffix.startswith("pulls?"):
            return [self.pr_data]
        if suffix.startswith("pulls/31/reviews"):
            return self.reviews
        if suffix.startswith("pulls/31/comments"):
            return self.inline
        if suffix.startswith("issues/31/comments"):
            return self.conversation
        return super().api(suffix, data=data, **kwargs)

    def model_double(self, repo, directory):
        self.model_runs += 1
        return store_review(repo, directory)


class PipelineTests(PipelineFixture):
    def test_prepared_model_compatibility_change_requires_fresh_packet(self):
        entry = {
            "provider": "copilot",
            "model": "gpt-fixture-99",
            "efforts": ["default"],
            "cli_version": "1.0.83",
            "adapter": "copilot-session-events-v2",
            "evidence": [
                "https://docs.github.com/en/copilot/reference/copilot-cli-reference/cli-command-reference"
            ],
        }
        cfg = workflow.configuration(self.root)
        cfg["review_model_extensions"] = [entry]
        workflow.write_json(self.root / ".agentic/config.json", cfg)
        git(self.root, "commit", "-am", "declare fixture model compatibility")
        first = pipeline.review_task(self.repo, 12, review_model=entry["model"])
        packet = Path(first["directory"]) / "metadata.json"
        original = packet.read_bytes()
        entry["efforts"].append("high")
        workflow.write_json(self.root / ".agentic/config.json", cfg)
        git(self.root, "commit", "-am", "extend fixture model compatibility")
        with self.assertRaisesRegex(workflow.WorkflowError, "--fresh"):
            pipeline.review_task(self.repo, 12, review_model=entry["model"])
        self.assertEqual(packet.read_bytes(), original)
        fresh = pipeline.review_task(self.repo, 12, review_model=entry["model"], fresh=True)
        self.assertNotEqual(fresh["directory"], first["directory"])
        self.assertEqual(self.model_runs, 0)

    def test_prepared_selection_is_immutable_and_fresh_provider_packet_is_explicit(self):
        result = pipeline.review_task(self.repo, 12, review_provider="claude-code")
        directory = Path(result["directory"])
        original = (directory / "metadata.json").read_bytes()
        for overrides in (
            {"review_provider": "copilot"},
            {"review_provider": "claude-code", "review_effort": "high"},
        ):
            with self.subTest(overrides=overrides), self.assertRaisesRegex(workflow.WorkflowError, "--fresh"):
                pipeline.review_task(self.repo, 12, **overrides)
            self.assertEqual((directory / "metadata.json").read_bytes(), original)
        fresh = pipeline.review_task(self.repo, 12, fresh=True, review_provider="copilot")
        self.assertNotEqual(fresh["directory"], result["directory"])
        self.assertEqual(fresh["review_policy"]["provider"], "copilot")

    def test_attempted_recovery_uses_bound_policy_and_provider_change_needs_continuation(self):
        import review_policy

        with patch.object(review, "review", side_effect=self.model_double):
            first = pipeline.review_task(self.repo, 12, execute=True)
        review_policy.save_selection(
            self.repo, workflow.configuration(self.root), review_provider="claude-code"
        )
        with patch.object(review, "review", side_effect=AssertionError("Recovery cannot infer again")):
            recovered = pipeline.review_task(self.repo, 12, execute=True, publish=True)
            self.assertEqual(recovered["directory"], first["directory"])
            self.assertEqual(recovered["review_policy"]["provider"], "copilot")
            with self.assertRaisesRegex(workflow.WorkflowError, "--fresh"):
                pipeline.review_task(self.repo, 12, execute=True, review_provider="claude-code")
            with self.assertRaisesRegex(workflow.WorkflowError, "continuation"):
                pipeline.review_task(self.repo, 12, execute=True, fresh=True, review_provider="claude-code")
        self.assertEqual(self.model_runs, 1)

    def test_attempted_recovery_does_not_resolve_obsolete_saved_selection(self):
        import review_policy

        with patch.object(review, "review", side_effect=self.model_double):
            first = pipeline.review_task(self.repo, 12, execute=True)
        with (
            patch.object(review_policy, "resolve", side_effect=workflow.WorkflowError("Obsolete selection")),
            patch.object(review, "review", side_effect=AssertionError("Recovery cannot infer")),
        ):
            recovered = pipeline.review_task(self.repo, 12, execute=True, publish=True)
            self.assertEqual(recovered["review_policy"], first["review_policy"])
            with self.assertRaisesRegex(workflow.WorkflowError, "Obsolete selection"):
                pipeline.review_task(self.repo, 12, fresh=True)
        self.assertEqual(self.model_runs, 1)

    def test_partial_report_is_published_without_readiness_designation(self):
        with patch.object(
            review,
            "review",
            side_effect=lambda repo, directory: store_review(
                repo, directory, "Incomplete source inspection.\n"
            ),
        ):
            result = pipeline.review_task(self.repo, 12, execute=True, publish=True)
        self.assertEqual(result["status"], "published-incomplete")
        self.assertIsNone(result["designated_review"])
        self.assertEqual(result["attempted_rounds"], 1)
        self.assertIn("INCOMPLETE", self.reviews[0]["body"])
        with self.assertRaises(workflow.WorkflowError):
            pipeline.validate_designated(self.repo, tasks.TaskStore(self.repo).read("issue-12"))

    def test_historical_qualified_report_can_publish_but_cannot_be_designated(self):
        import json

        import review_coverage_v1 as legacy
        from review_fixtures import events

        def historical(repo, directory):
            directory = Path(directory)
            meta = review.verify_packet(directory)
            meta["schema_version"] = 2
            review.atomic_json(directory / "metadata.json", meta)
            items = legacy.read_json(directory / "packet/required-material.json")["required"]
            body = json.dumps(
                {
                    "schema_version": 1,
                    "findings": [],
                    "coverage": [
                        {
                            "id": item["id"],
                            "state": "reviewed",
                            "reason": "",
                            "locations": [{key: item[key] for key in ("artifact", "start_line", "end_line")}],
                        }
                        for item in items
                    ],
                    "limitations": [],
                }
            )
            _, diagnostics = legacy.parse_events(
                "\n".join(json.dumps(row) for row in events(directory / "packet", body)),
                directory / "packet",
                directory / "packet",
                version="1.0.83",
            )
            assessment = legacy.assess(directory / "packet", body, diagnostics)
            self.assertTrue(assessment["qualified"])
            review.atomic_json(
                directory / "review-result.json",
                {
                    "schema_version": 2,
                    "input_digest": review.value_digest(meta),
                    "body": body,
                    "review_sha256": legacy.checksum(body),
                    "copilot_version": "1.0.83",
                    "diagnostics": diagnostics,
                    "diagnostics_sha256": review.value_digest(diagnostics),
                    "coverage_sha256": review.value_digest(assessment),
                },
            )
            return review.recover_review(repo, directory)

        with patch.object(review, "review", side_effect=historical):
            result = pipeline.review_task(self.repo, 12, execute=True, publish=True)
        self.assertEqual(result["status"], "published-incomplete")
        self.assertFalse(result["coverage_qualified"])
        self.assertIsNone(result["designated_review"])
        self.assertIn("coverage-qualified static inspection", self.reviews[0]["body"])
        with self.assertRaises(workflow.WorkflowError):
            review.verified_published(self.repo, result["directory"], 31, self.head, self.base)

    def test_missing_diagnostics_refuse_even_a_manually_changed_designation(self):
        with patch.object(review, "review", side_effect=self.model_double):
            result = pipeline.review_task(self.repo, 12, execute=True, publish=True)
        (Path(result["directory"]) / "diagnostics.json").unlink()
        with self.assertRaises(workflow.WorkflowError):
            pipeline.validate_designated(self.repo, tasks.TaskStore(self.repo).read("issue-12"))

    def test_existing_pr_receives_push_and_updated_evidence(self):
        result = pipeline.publish_pr(self.repo, 12, "Current title", self.body_file)
        self.assertEqual(result["pr"], 31)
        self.assertEqual(self.pr_data["title"], "Current title")
        self.assertEqual(self.pr_data["body"], self.body_file.read_text())
        self.assertEqual(
            git(self.root, "ls-remote", "--heads", "origin", "issue-12-correct-value").split()[0],
            self.head,
        )
        self.assertTrue(any(endpoint == "pulls/31" for endpoint, _ in self.posts))

    def test_accepted_pr_patch_is_reconciled_after_transport_error(self):
        original = self.repo.api

        def interrupted(suffix, **kwargs):
            result = original(suffix, **kwargs)
            if suffix == "pulls/31" and kwargs.get("data") is not None:
                raise workflow.WorkflowError("connection lost after PATCH")
            return result

        with patch.object(self.repo, "api", side_effect=interrupted):
            result = pipeline.publish_pr(self.repo, 12, "Updated title", self.body_file)
        self.assertTrue(result["updated"])
        self.assertEqual(self.pr_data["title"], "Updated title")

    def test_feedback_covers_reviews_inline_and_conversation(self):
        self.reviews.append(
            {"id": 1, "commit_id": self.head, "state": "COMMENTED", "body": "No action needed"}
        )
        self.inline.append({"id": 2, "commit_id": self.head, "body": "Inspect this edge"})
        self.conversation.append({"id": 3, "body": "Public context"})
        result = pipeline.feedback(self.repo, 12)
        self.assertEqual(set(result["records"]), {"review:1", "inline:2"})
        self.assertEqual(result["feedback"]["conversation"], self.conversation)
        self.assertTrue(result["records"]["review:1"]["digest"])

    def test_response_publication_is_idempotent(self):
        first = pipeline.respond(self.repo, 12, "round-one", self.body_file)
        second = pipeline.respond(self.repo, 12, "round-one", self.body_file)
        self.assertEqual(first, second)
        self.assertEqual(len(self.conversation), 1)

    def test_pipeline_designates_only_its_published_current_report(self):
        with patch.object(review, "review", side_effect=self.model_double):
            result = pipeline.review_task(self.repo, 12, execute=True, publish=True)
        self.assertEqual(result["status"], "published")
        store = tasks.TaskStore(self.repo)
        verified = pipeline.validate_designated(self.repo, store.read("issue-12"))
        self.assertEqual(verified["review"]["commit_id"], self.head)
        self.assertEqual(verified["review"]["state"], "COMMENTED")
        self.pr_data["base"]["sha"] = "a" * 40
        with self.assertRaisesRegex(workflow.WorkflowError, "stale"):
            pipeline.validate_designated(self.repo, store.read("issue-12"))

    def test_arbitrary_current_comment_does_not_establish_pipeline_readiness(self):
        self.reviews.append({"id": 1, "commit_id": self.head, "state": "COMMENTED", "body": "Looks fine"})
        state = tasks.TaskStore(self.repo).read("issue-12")
        with self.assertRaisesRegex(workflow.WorkflowError, "No designated"):
            pipeline.validate_designated(self.repo, state)

    def test_second_model_round_requires_explicit_reason_and_continuation(self):
        store = tasks.TaskStore(self.repo)
        reason = "Fixture operator explicitly approved another review of the recovery change"
        with patch.object(review, "review", side_effect=self.model_double):
            pipeline.review_task(self.repo, 12, execute=True)
            original_rounds = store.read("issue-12")["review_rounds"]
            for approved, supplied_reason in (
                (False, None),
                (True, None),
                (True, ""),
                (True, " \n\t "),
                (False, reason),
            ):
                with self.subTest(approved=approved, reason=supplied_reason):
                    with self.assertRaisesRegex(workflow.WorkflowError, "explicit continuation"):
                        pipeline.review_task(
                            self.repo,
                            12,
                            execute=True,
                            fresh=True,
                            continue_reason=supplied_reason,
                            approved_continuation=approved,
                        )
                    self.assertEqual(self.model_runs, 1)
                    self.assertEqual(store.read("issue-12")["review_rounds"], original_rounds)
            result = pipeline.review_task(
                self.repo,
                12,
                execute=True,
                fresh=True,
                continue_reason=reason,
                approved_continuation=True,
            )
            self.assertEqual(result["attempted_rounds"], 2)
            saved_rounds = store.read("issue-12")["review_rounds"]
            self.assertEqual(saved_rounds[:-1], original_rounds)
            self.assertEqual(
                saved_rounds[-1]["continuation"],
                {"approved": True, "reason": reason},
            )
            with self.assertRaisesRegex(workflow.WorkflowError, "explicit continuation"):
                pipeline.review_task(self.repo, 12, execute=True, fresh=True)
            self.assertEqual(store.read("issue-12")["review_rounds"], saved_rounds)
        self.assertEqual(self.model_runs, 2)

    def test_failed_model_run_is_not_silently_restarted(self):
        with patch.object(review, "review", side_effect=workflow.WorkflowError("model unavailable")):
            with self.assertRaisesRegex(workflow.WorkflowError, "model unavailable"):
                pipeline.review_task(self.repo, 12, execute=True)
        with patch.object(review, "review") as model:
            with self.assertRaises((workflow.WorkflowError, OSError)):
                pipeline.review_task(self.repo, 12, execute=True)
        model.assert_not_called()
        state = tasks.TaskStore(self.repo).read("issue-12")
        self.assertEqual(sum(r["run_attempted"] for r in state["review_rounds"]), 1)

    def test_saved_result_recovers_and_publishes_without_another_attempt(self):
        def interrupted_result(repo, directory):
            self.model_runs += 1
            with patch.object(
                review, "atomic_text", side_effect=OSError("Interrupted after saving model result")
            ):
                store_review(repo, directory)

        with patch.object(review, "review", side_effect=interrupted_result):
            with self.assertRaisesRegex(OSError, "Interrupted"):
                pipeline.review_task(self.repo, 12, execute=True)
        with patch.object(review, "review", side_effect=AssertionError("Must not invoke another model")):
            result = pipeline.review_task(self.repo, 12, execute=True, publish=True)
        self.assertEqual(result["status"], "published")
        self.assertEqual(result["attempted_rounds"], 1)
        self.assertEqual(self.model_runs, 1)
        self.assertEqual(len(self.reviews), 1)

    def test_report_changes_invalidate_designated_review(self):
        with patch.object(review, "review", side_effect=self.model_double):
            result = pipeline.review_task(self.repo, 12, execute=True, publish=True)
        (Path(result["directory"]) / "review.md").write_text("Edited report")
        with self.assertRaisesRegex(workflow.WorkflowError, "changed"):
            pipeline.validate_designated(self.repo, tasks.TaskStore(self.repo).read("issue-12"))
