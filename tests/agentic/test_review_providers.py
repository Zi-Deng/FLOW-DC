"""Provider selection, immutable packets and historical recovery; no live inference."""

import copy
import json
from unittest.mock import patch

from test_workflow import GitFixture, review, workflow

# isort: split
import review_coverage as coverage
import review_coverage_v2 as frozen
import review_policy as policy
from review_fixtures import stream


class ProviderPolicyTests(GitFixture):
    def config(self, **fields):
        return {**workflow.configuration(self.root), **fields}

    def test_schema_two_defaults_to_claude_but_schema_one_keeps_copilot(self):
        cfg = self.config(schema_version=2)
        for key in ("review_provider", "review_model", "review_effort"):
            cfg.pop(key)
        selected = policy.resolve(self.repo, cfg)
        self.assertEqual(selected["policy"]["provider"], "claude-code")
        self.assertEqual(selected["policy"]["model"], "claude-opus-5-5")
        self.assertEqual(selected["policy"]["effort"], "medium")
        self.assertEqual(selected["policy"]["budget"]["extra_spend_authorized_usd"], 0)
        cfg["schema_version"] = 1
        self.assertEqual(policy.resolve(self.repo, cfg)["policy"]["provider"], "copilot")
        self.assertEqual(policy.resolve(self.repo, cfg)["policy"]["budget"]["ai_credits"], 400)

    def test_override_precedence_and_provider_local_defaults(self):
        cfg = self.config()
        policy.save_selection(self.repo, cfg, review_provider="claude-code", review_effort="high")
        saved = policy.resolve(self.repo, cfg)
        self.assertEqual(saved["policy"]["effort"], "high")
        self.assertEqual(set(saved["sources"].values()), {"saved-selection"})
        override = policy.resolve(self.repo, cfg, review_provider="copilot")
        self.assertEqual(override["policy"]["model"], "claude-opus-5")
        self.assertEqual(override["policy"]["effort"], "default")
        self.assertEqual(policy.resolve(self.repo, cfg)["policy"], saved["policy"])

    def test_deliberate_supported_models_and_efforts_keep_exact_provider_spelling(self):
        for provider, model, effort in [
            ("claude-code", "claude-sonnet-5", "high"),
            ("copilot", "claude-opus-5.5", "xhigh"),
            ("copilot", "gpt-5.4", "none"),
        ]:
            selected = policy.resolve(
                self.repo, self.config(), review_provider=provider, review_model=model, review_effort=effort
            )["policy"]
            self.assertEqual(
                (selected["provider"], selected["model"], selected["effort"]), (provider, model, effort)
            )
            policy.validate_policy(selected)
        with self.assertRaises(workflow.WorkflowError):
            policy.choices("claude-code", "claude-sonnet-4-6", "xhigh")

    def test_invalid_combinations_never_prepare_packet(self):
        self.commit_task()
        for overrides in [
            {"review_provider": "auto"},
            {"review_model": "opus"},
            {"review_provider": "copilot", "review_model": "claude-opus-5-5"},
            {"review_provider": "copilot", "review_model": "claude-haiku-4.5", "review_effort": "medium"},
            {"review_provider": "claude-code", "review_effort": "ultracode"},
        ]:
            with self.subTest(overrides=overrides), self.assertRaises(workflow.WorkflowError):
                review.prepare(self.repo, 31, 12, 1234, **overrides)
        self.assertFalse((self.root / ".agentic-local/reviews").exists())

    def test_private_selection_rejects_tampering_and_symlinks(self):
        cfg = self.config()
        policy.save_selection(self.repo, cfg, review_provider="claude-code")
        path = policy.selection_path(self.repo)
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        path.chmod(0o644)
        with self.assertRaises(workflow.WorkflowError):
            policy.read_selection(self.repo)
        path.chmod(0o600)
        data = path.read_text()
        path.unlink()
        target = self.parent / "outside-selection"
        target.write_text(data)
        path.symlink_to(target)
        with self.assertRaises(workflow.WorkflowError):
            policy.read_selection(self.repo)
        self.assertEqual(target.read_text(), data)

    def test_packet_binds_policy_and_rejects_reserved_schema(self):
        self.commit_task()
        directory = review.prepare(self.repo, 31, 12, 1234, review_provider="claude-code")
        meta = review.verify_packet(directory)
        self.assertEqual(meta["schema_version"], 5)
        for field, value in [("model", "auto"), ("adapter", "future-adapter"), ("billing_mode", "api-key")]:
            changed = copy.deepcopy(meta)
            changed["review_policy"][field] = value
            review.atomic_json(directory / "metadata.json", changed)
            with self.subTest(field=field), self.assertRaises(workflow.WorkflowError):
                review.verify_packet(directory)
        review.atomic_json(directory / "metadata.json", {**meta, "schema_version": 4})
        with self.assertRaisesRegex(workflow.WorkflowError, "reserved"):
            review.verify_packet(directory)

    def test_typed_budgets_reject_unbounded_and_paid_claude_policy(self):
        selected = policy.choices("claude-code")
        for amount in [True, float("nan"), float("inf"), -1, 0, 11, "2"]:
            with self.subTest(amount=amount), self.assertRaises(workflow.WorkflowError):
                policy.policy(selected, {"review_max_estimated_usd": amount})
        value = policy.policy(selected, {})
        value["budget"]["extra_spend_authorized_usd"] = 1
        with self.assertRaises(workflow.WorkflowError):
            policy.validate_policy(value)
        value = policy.policy(selected, {})
        value["budget"]["ai_credits"] = 400
        with self.assertRaises(workflow.WorkflowError):
            policy.validate_policy(value)

    def test_schema_three_capture_recovers_without_current_assessor_and_never_qualifies(self):
        self.commit_task()
        directory = review.prepare(self.repo, 31, 12, 1234)
        meta = review.verify_packet(directory)
        meta["schema_version"] = 3
        meta.pop("review_policy")
        meta.pop("selection_sources")
        review.atomic_json(directory / "metadata.json", meta)
        body, diagnostics = frozen.parse_events(
            stream(directory / "packet"), directory / "packet", directory / "packet", version="1.0.83"
        )
        expected = frozen.assess(directory / "packet", body, diagnostics)
        with patch.object(
            coverage, "assess", side_effect=AssertionError("Current assessor cannot change history")
        ):
            review.save_result(directory, meta, body, diagnostics, "1.0.83")
            result = directory / "review-result.json"
            original = result.read_bytes()
            result.unlink()  # Recover the original pending exact capture, without inference.
            review.recover_review(self.repo, directory)
            self.assertEqual(result.read_bytes(), original)
            self.assertEqual(review.qualification(directory), expected)
            envelope = review.publication_body(directory)
            self.assertIn("Independent Copilot CLI", envelope)
            self.assertIn("agentic-coverage:v2:", envelope)
            self.assertFalse(review.coverage_ready(directory))
            with self.assertRaisesRegex(workflow.WorkflowError, "Legacy"):
                review.qualification(directory, require=True)
        self.assertEqual((directory / "review.md").read_bytes(), body.encode())
        self.assertEqual(json.loads(result.read_bytes())["schema_version"], 3)

    def test_schema_one_exact_recovery_and_publication_never_gain_coverage(self):
        self.commit_task()
        directory = review.prepare(self.repo, 31, 12, 1234)
        meta = review.verify_packet(directory)
        meta["schema_version"] = 1
        meta.pop("review_policy")
        meta.pop("selection_sources")
        review.atomic_json(directory / "metadata.json", meta)
        body = "Legacy exact café\r\n\x07\x1b\\n\r\n"
        journal = {
            "schema_version": 1,
            "input_digest": review.value_digest(meta),
            "body": body,
            "review_sha256": coverage.checksum(body),
            "copilot_version": "legacy-identity",
        }
        review.atomic_json(directory / "review-result.json", journal)
        with patch.object(coverage, "assess", side_effect=AssertionError("No new historical coverage")):
            review.recover_review(self.repo, directory)
            expected = body + f"\n<!-- agentic-review:{self.head}:{coverage.checksum(body)} -->"
            self.assertEqual(review.publication_body(directory), expected)
            self.assertFalse(review.coverage_ready(directory))
        self.assertEqual((directory / "review.md").read_bytes(), body.encode())
        self.assertEqual(json.loads((directory / "review-result.json").read_bytes()), journal)
