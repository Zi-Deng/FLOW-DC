"""Provider selection, immutable packets and historical recovery; no live inference."""

import copy
import json
import subprocess
from unittest.mock import patch

from test_workflow import GitFixture, review, workflow

# isort: split
import claude_native_auth
import review_coverage as coverage
import review_coverage_v2 as frozen
import review_policy as policy
from claude_fixtures import AUTHENTICATION, native_events
from review_fixtures import stream


class ProviderPolicyTests(GitFixture):
    def setUp(self):
        super().setUp()
        binding = patch.object(claude_native_auth, "current_binding", return_value=AUTHENTICATION)
        binding.start()
        self.addCleanup(binding.stop)

    def config(self, **fields):
        return {**workflow.configuration(self.root), **fields}

    def extension(self, provider="claude-code"):
        # Deliberately fictional IDs: these fixtures test the trusted declaration
        # boundary, not actual provider support or entitlement for a future model.
        return {
            "provider": provider,
            "model": "claude-fixture-99" if provider == "claude-code" else "gpt-fixture-99",
            "efforts": ["medium"] if provider == "claude-code" else ["default", "high"],
            "cli_version": "2.1.282" if provider == "claude-code" else "1.0.83",
            "adapter": "claude-stream-json-2.1.282-v4"
            if provider == "claude-code"
            else "copilot-session-events-v2",
            "evidence": [
                "https://code.claude.com/docs/en/model-config"
                if provider == "claude-code"
                else "https://docs.github.com/en/copilot/reference/copilot-cli-reference/cli-command-reference"
            ],
        }

    def test_configured_exact_models_bind_compatibility_and_survive_config_removal(self):
        for provider in ("claude-code", "copilot"):
            entry = self.extension(provider)
            cfg = self.config(review_model_extensions=[entry])
            selected = policy.save_selection(
                self.repo, cfg, review_provider=provider, review_model=entry["model"]
            )["policy"]
            self.assertEqual(selected["model_compatibility"], entry)
            self.assertEqual(policy.resolve(self.repo, cfg)["policy"], selected)
            self.assertEqual(policy.validate_policy(copy.deepcopy(selected)), selected)
            # Saved choices are not authority to invent model compatibility.
            with self.assertRaises(workflow.WorkflowError):
                policy.resolve(self.repo, self.config())
            # Detached recovery validates the frozen declaration, not today's config.
            changed = copy.deepcopy(selected)
            changed["model_compatibility"]["cli_version"] = "future-version"
            with self.assertRaises(workflow.WorkflowError):
                policy.validate_policy(changed)
            policy.selection_path(self.repo).unlink()

    def test_model_extension_rejects_aliases_conflicts_and_unverified_contracts(self):
        entry = self.extension()
        bad = [
            {**entry, "model": model}
            for model in ("opus", "claude-auto-99", "claude-latest-99", "claude-fixture-99[1m]", "--model-99")
        ]
        bad += [
            {**entry, "cli_version": "2.1.283"},
            {**entry, "adapter": "unknown-telemetry"},
            {**entry, "efforts": ["default"]},
            {**entry, "efforts": ["medium", "medium"]},
            {**entry, "evidence": []},
            {**entry, "evidence": ["https://example.invalid/model"]},
            {**entry, "evidence": ["https://code.claude.com/docs/en/model-config?token=private"]},
            {**entry, "model": "claude-opus-5-5"},
        ]
        for item in bad:
            with self.subTest(item=item), self.assertRaises(workflow.WorkflowError):
                policy.resolve(self.repo, self.config(review_model_extensions=[item]), saved=False)
        with self.assertRaises(workflow.WorkflowError):
            policy.resolve(self.repo, self.config(review_model_extensions=[entry, entry]), saved=False)
        with self.assertRaises(workflow.WorkflowError):
            policy.resolve(self.repo, self.config(schema_version=1, review_model_extensions=[entry]))
        with self.assertRaises(workflow.WorkflowError):
            policy.resolve(
                self.repo,
                self.config(review_model_extensions=[entry]),
                review_provider="claude-code",
                review_model=entry["model"],
                review_effort="max",
            )

    def test_configured_native_model_requires_bound_settings_and_exact_observed_identity(self):
        import claude_telemetry
        import review_claude

        self.commit_task()
        entry = self.extension()
        cfg = self.config(review_model_extensions=[entry])
        path = self.root / ".agentic/config.json"
        workflow.write_json(path, cfg)
        directory = review.prepare(
            self.repo, 31, 12, 1234, review_provider="claude-code", review_model=entry["model"]
        )
        meta = review.verify_packet(directory)
        selected = meta["review_policy"]
        settings = review_claude.trusted_settings(selected)
        with self.assertRaisesRegex(workflow.WorkflowError, "bound exact-model"):
            review_claude.check_controls("/not-opened", settings)
        # All preflight controls are still required for a declared model. This
        # deliberately invalid binary cannot pass by supplying a declaration.
        binary = self.parent / "invalid-native"
        binary.write_bytes(b"missing-native-controls")
        with self.assertRaisesRegex(workflow.WorkflowError, "required isolation control"):
            review_claude.check_controls(binary, settings, selected)
        packet = directory / "packet"
        rows = native_events(packet, packet, "fixture", model=entry["model"])
        raw = "\n".join(json.dumps(row) for row in rows)
        report, diagnostics = claude_telemetry.capture(raw, packet, packet, selected, "fixture")
        self.assertTrue(coverage.assess(packet, report, diagnostics, policy=selected)["qualified"])
        review.save_result(directory, meta, report, diagnostics, "2.1.282")
        original = (directory / "review-result.json").read_bytes()
        (directory / "review-result.json").unlink()
        workflow.write_json(path, self.config(review_model_extensions=[]))
        with patch.object(review_claude, "execute", side_effect=AssertionError("No inference on recovery")):
            review.recover_review(self.repo, directory)
        self.assertEqual((directory / "review-result.json").read_bytes(), original)
        rows[0]["model"] = "claude-opus-5-5"
        raw = "\n".join(json.dumps(row) for row in rows)
        report, diagnostics = claude_telemetry.capture(raw, packet, packet, selected, "fixture")
        self.assertFalse(coverage.assess(packet, report, diagnostics, policy=selected)["qualified"])

    def test_configured_copilot_model_reaches_native_command_without_substitution(self):
        import review_cli
        import review_process
        from review_fixtures import HELP, provider_response

        self.commit_task()
        entry = self.extension("copilot")
        cfg = self.config(review_model_extensions=[entry])
        workflow.write_json(self.root / ".agentic/config.json", cfg)
        with patch.object(review_cli, "executable", return_value="/fixture/copilot"):
            status = policy.status(self.repo, cfg, review_provider="copilot", review_model=entry["model"])
        self.assertEqual(status["model_compatibility_sources"][entry["model"]], "trusted-config-declaration")
        self.assertEqual(status["model_compatibility_sources"]["claude-opus-5"], "built-in")
        directory = review.prepare(
            self.repo,
            31,
            12,
            1234,
            review_provider="copilot",
            review_model=entry["model"],
            review_effort="high",
        )
        ordinary = review.run

        def native_info(args, **kwargs):
            if args[1:] in (["--help"], ["--version"]):
                return subprocess.CompletedProcess(args, 0, HELP if args[1] == "--help" else "1.0.83", "")
            return ordinary(args, **kwargs)

        def capture(args, **kwargs):
            self.assertEqual(args[args.index("--model") + 1], entry["model"])
            self.assertEqual(args[args.index("--effort") + 1], "high")
            stdout = provider_response(args, kwargs, directory / "packet")
            state = kwargs["env"]["COPILOT_HOME"]
            session = args[args.index("--session-id") + 1]
            from pathlib import Path

            event_file = Path(state) / "session-state" / session / "events.jsonl"
            rows = [json.loads(line) for line in event_file.read_text().splitlines()]
            rows[0]["data"]["selectedModel"] = entry["model"]
            event_file.write_text("\n".join(json.dumps(row) for row in rows) + "\n")
            return subprocess.CompletedProcess(args, 0, stdout, b"")

        with (
            patch.dict("os.environ", {"COPILOT_GITHUB_TOKEN": "test-only-token"}),
            patch.object(review_cli, "executable", return_value="/fixture/copilot"),
            patch.object(review, "run", side_effect=native_info),
            patch.object(review_process, "capture", side_effect=capture) as bounded,
        ):
            review.review(self.repo, directory)
        bounded.assert_called_once()
        self.assertEqual(review.verify_packet(directory)["requested_model"], entry["model"])
        self.assertTrue(review.coverage_ready(directory))

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

    def test_frozen_schema_five_token_record_recovers_without_authentication_or_relabelling(self):
        self.frozen_authentication_recovery(None)

    def test_frozen_native_revision_two_recovers_exactly_without_current_authentication(self):
        self.frozen_authentication_recovery(
            {
                "schema_version": 1,
                "mode": "native-max-access-only-v1",
                "registration_id": "11111111-1111-4111-8111-111111111111",
                "generation_id": "22222222-2222-4222-8222-222222222222",
            }
        )

    def frozen_authentication_recovery(self, authentication):
        import claude_telemetry

        self.commit_task()
        directory = review.prepare(self.repo, 31, 12, 1234, review_provider="claude-code")
        meta = review.verify_packet(directory)
        meta["review_policy"].pop("authentication")
        if authentication is not None:
            meta["review_policy"]["authentication"] = authentication
        review.atomic_json(directory / "metadata.json", meta)
        packet = directory / "packet"
        rows = native_events(packet, packet, "legacy-fixture")
        body, diagnostics = claude_telemetry.capture(
            "\n".join(json.dumps(row) for row in rows),
            packet,
            packet,
            meta["review_policy"],
            "legacy-fixture",
        )
        review.save_result(directory, meta, body, diagnostics, "2.1.282")
        review.recover_review(self.repo, directory)
        original = {
            name: (directory / name).read_bytes()
            for name in ("review-result.json", "review-capture.json", "coverage.json", "review.md")
        }
        envelope = review.publication_body(directory)
        (directory / "review-result.json").unlink()
        with patch.object(
            claude_native_auth, "current_binding", side_effect=AssertionError("No authentication on recovery")
        ):
            review.recover_review(self.repo, directory)
            self.assertEqual(review.publication_body(directory), envelope)
        self.assertEqual(original, {name: (directory / name).read_bytes() for name in original})
        self.assertEqual(
            review.verify_packet(directory)["review_policy"].get("authentication"), authentication
        )
        self.assertFalse(review.coverage_ready(directory))
        with self.assertRaisesRegex(workflow.WorkflowError, "authentication binding|setup provenance"):
            review.qualification(directory, require=True)

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

    def test_legacy_adapter_model_declaration_is_frozen_not_a_new_selection(self):
        for adapter in ("claude-stream-json-2.1.282-v1", "claude-stream-json-2.1.282-v2"):
            with self.subTest(adapter=adapter):
                extension = self.extension()
                extension["adapter"] = adapter
                selected = policy.policy(policy.choices("claude-code"), {})
                selected.update(model=extension["model"], adapter=adapter, model_compatibility=extension)
                original = copy.deepcopy(selected)
                self.assertEqual(policy.validate_policy(selected), original)
                self.assertEqual(selected, original)
                with self.assertRaises(workflow.WorkflowError):
                    policy.model_extensions(self.config(review_model_extensions=[extension]))
                with self.assertRaisesRegex(workflow.WorkflowError, "recovery-only"):
                    policy.require_current_adapter(selected)
                selected["model_compatibility"]["adapter"] = policy.PROVIDERS["claude-code"]["adapter"]
                with self.assertRaises(workflow.WorkflowError):
                    policy.validate_policy(selected)
                selected["model_compatibility"] = "malformed"
                with self.assertRaises(workflow.WorkflowError):
                    policy.validate_policy(selected)
