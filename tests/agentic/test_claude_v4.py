"""Source-grounded v4 controls and strict progress events; never live capability."""

import copy
import hashlib
import json
from pathlib import Path
from unittest.mock import patch

import test_claude_telemetry as fixtures
from test_workflow import GitFixture, review, workflow

# isort: split
import claude_native_auth
import claude_telemetry as telemetry
import claude_telemetry_v3 as frozen
import review_claude
import review_coverage as coverage
import review_policy

FIXTURE = json.loads((Path(__file__).parent / "fixtures/claude-controls-2.1.282-v4.json").read_text())


class ClaudeV4Tests(GitFixture):
    def setUp(self):
        super().setUp()
        binding = patch.object(claude_native_auth, "current_binding", return_value=fixtures.AUTHENTICATION)
        binding.start()
        self.addCleanup(binding.stop)
        self.commit_task()
        self.directory = review.prepare(self.repo, 31, 12, 1234, review_provider="claude-code")
        self.packet = self.directory / "packet"
        self.policy = review.verify_packet(self.directory)["review_policy"]
        self.rows = fixtures.native_events(self.packet, self.packet, "fixture-session")

    evaluate = fixtures.ClaudeTelemetryTests.evaluate

    def thinking(self):
        return {**FIXTURE["events"][1], "session_id": "fixture-session"}

    def test_native_zero_fraction_signature_and_reset_do_not_change_accounting(self):
        expected = self.evaluate(self.rows)
        for event in FIXTURE["events"]:
            self.rows.insert(-1, {**event, "session_id": "fixture-session"})
        assessment, diag, body = self.evaluate(self.rows)
        self.assertTrue(assessment["qualified"])
        self.assertEqual(diag["schema_version"], 6)
        self.assertEqual(diag["usage"], expected[1]["usage"])
        self.assertEqual(diag["events"], expected[1]["events"])
        self.assertEqual(body.encode(), expected[2].encode())
        for key in telemetry.NUMERIC_FIELDS:
            self.assertTrue(
                all(
                    x["numeric_validity"] == "valid"
                    for x in diag["telemetry"]["control_fields"][key]["observations"]
                )
            )

    def test_progress_never_rescues_missing_usage_or_tools(self):
        for missing in ("usage", "tools"):
            rows = copy.deepcopy(self.rows)
            rows.insert(-1, self.thinking())
            if missing == "usage":
                rows[-1].pop("total_cost_usd")
            else:
                rows = [row for row in rows if row["type"] not in {"assistant", "user"}]
            self.assertFalse(self.evaluate(rows)[0]["qualified"])

    def test_exact_numeric_envelope_and_identity_negative_matrix(self):
        changes = []
        for key in self.thinking():
            changes.append(lambda e, key=key: e.pop(key))
        for key in ("estimated_tokens", "estimated_tokens_delta"):
            for value in (
                True,
                False,
                None,
                "2",
                -1,
                9007199254740992,
                10**400,
                float("inf"),
                float("nan"),
                [],
                {},
            ):
                changes.append(lambda e, key=key, value=value: e.update({key: value}))
        for change in (
            lambda e: e.update(estimated_tokens=1, estimated_tokens_delta=2),
            lambda e: e.update(uuid="not-uuid"),
            lambda e: e.update(session_id="other"),
            lambda e: e.update(subtype="unknown"),
            lambda e: e.update(agent_id="synthetic-delegation"),
            lambda e: e.update(parent_tool_use_id=None),
            lambda e: e.update(thinking="synthetic secret must never persist"),
        ):
            changes.append(change)
        for change in changes:
            with self.subTest(change=change):
                event = self.thinking()
                change(event)
                assessment, diag, _ = self.evaluate(self.rows[:1] + [event] + self.rows[1:])
                self.assertFalse(assessment["qualified"])
                self.assertNotIn("synthetic secret", json.dumps(diag))
        for rows in (
            [self.thinking()] + self.rows,
            self.rows + [self.thinking()],
            self.rows[:1] * 2 + [self.thinking()] + self.rows[1:],
        ):
            self.assertFalse(self.evaluate(rows)[0]["qualified"])

    def test_numeric_observations_are_bounded_and_validator_rejects_injected_shapes(self):
        events = [{**self.thinking(), "estimated_tokens": n, "estimated_tokens_delta": n} for n in range(100)]
        _, diag, _ = self.evaluate(self.rows[:1] + events + self.rows[1:])
        for name in telemetry.NUMERIC_FIELDS:
            field = diag["telemetry"]["control_fields"][name]
            self.assertEqual(len(field["observations"]), 8)
            self.assertEqual(field["overflow"], 92)
        for value in ("raw-provider-value", {}, True):
            changed = copy.deepcopy(diag["telemetry"])
            changed["control_fields"]["system.estimated_tokens"]["observations"][0]["numeric_validity"] = (
                value
            )
            with self.assertRaises(workflow.WorkflowError):
                telemetry.validate_summary(changed)

    def test_all_builtin_plugins_disabled_without_mutating_shared_settings(self):
        settings = review_claude.trusted_settings(self.policy)
        self.assertEqual(
            settings["enabledPlugins"], {name + "@builtin": False for name in FIXTURE["builtin_names"]}
        )
        self.assertEqual(len(settings["enabledPlugins"]), 9)
        settings["enabledPlugins"]["agents-md@builtin"] = True
        self.assertIs(
            review_claude.trusted_settings(self.policy)["enabledPlugins"]["agents-md@builtin"], False
        )
        self.assertIn("agents-md", FIXTURE["pluginCases"]["before"]["modules"])
        for key in ("enabled", "modules", "plugins"):
            self.assertEqual(FIXTURE["pluginCases"]["after"][key], [])
        self.assertIn("sec-default", FIXTURE["pluginCases"]["managedConflict"]["modules"])

    def test_missing_extra_nonboolean_or_enabled_builtin_controls_refused_before_binary_read(self):
        changes = [lambda m: m.pop("agents-md@builtin"), lambda m: m.update({"unknown@builtin": False})]
        for value in (True, 0, "false", None, {"enabled": False}):
            changes.append(lambda m, value=value: m.update({"agents-md@builtin": value}))
        for change in changes:
            settings = review_claude.trusted_settings(self.policy)
            change(settings["enabledPlugins"])
            with self.assertRaises(workflow.WorkflowError):
                review_claude.check_controls("/not-opened", settings, self.policy)

    def test_pinned_registrar_and_schema_guard(self):
        # Controlled native declaration fixture, not an executable binary.
        data = " ".join(review_claude.FLAGS + list(review_claude.FIXED_ENV)).encode()
        declarations = {
            **{k: ":O().optional()" for k, v in review_claude.SETTINGS.items() if type(v) is bool},
            "fallbackModel": ":C(o()).optional()",
            "modelOverrides": ":me(o(),o()).optional()",
            "enabledPlugins": ":me(o(),Fe([C(o()),O(),Jee()])).optional()",
            "model": ":o().optional()",
            "availableModels": ":C(o()).optional()",
            "enforceAvailableModels": ":O().optional()",
        }
        data += " ".join(k + v for k, v in declarations.items()).encode()
        registrar = FIXTURE["registrar_source"].encode()
        row = next(x for x in FIXTURE["ranges"] if x["label"] == "registrar")
        self.assertEqual(hashlib.sha256(registrar).hexdigest(), row["sha256"])
        binary = self.parent / "synthetic-nonexecutable-native"
        binary.write_bytes(data + registrar + b"\nexport{_ye}")
        review_claude.check_controls(binary, review_claude.trusted_settings(self.policy), self.policy)
        for bad in (
            registrar.replace(b'"agents-md"', b'"other-plugin"'),
            registrar.replace(b'"sec-default"', b'"sec-default-renamed"'),
        ):
            binary.write_bytes(data + bad + b"\nexport{_ye}")
            with self.assertRaisesRegex(workflow.WorkflowError, "registrar"):
                review_claude.check_controls(binary, review_claude.trusted_settings(self.policy), self.policy)

    def test_v3_exact_recovery_publication_cannot_gain_v4_semantics(self):
        legacy_policy = {**self.policy, "adapter": frozen.ADAPTER}
        rows = self.rows[:1] + [self.thinking()] + self.rows[1:]
        raw = "\n".join(json.dumps(row) for row in rows)
        expected = frozen.capture(raw, self.packet, self.packet, legacy_policy, "fixture-session")
        self.assertEqual(
            expected, telemetry.capture(raw, self.packet, self.packet, legacy_policy, "fixture-session")
        )
        body, diag = expected
        self.assertEqual(diag["schema_version"], 5)
        self.assertIn("unsupported_system_event", diag["reasons"])
        self.assertFalse(coverage.assess(self.packet, body, diag, policy=legacy_policy)["qualified"])
        meta = review.verify_packet(self.directory)
        meta["review_policy"] = legacy_policy
        review.atomic_json(self.directory / "metadata.json", meta)
        review.save_result(self.directory, meta, body, diag, "2.1.282")
        review.recover_review(self.repo, self.directory)
        envelope = review.publication_body(self.directory)
        names = ("review-result.json", "review-capture.json", "coverage.json", "review.md")
        original = {name: (self.directory / name).read_bytes() for name in names}
        (self.directory / "review-result.json").unlink()
        with (
            patch.object(claude_native_auth, "current_binding", side_effect=AssertionError("No credentials")),
            patch.object(review_claude.review_process, "capture", side_effect=AssertionError("No inference")),
        ):
            review.recover_review(self.repo, self.directory)
            review.publish(self.repo, self.directory)
        self.assertEqual(original, {name: (self.directory / name).read_bytes() for name in names})
        self.assertEqual(envelope, review.publication_body(self.directory))
        self.assertFalse(review.coverage_ready(self.directory))
        with self.assertRaises(workflow.WorkflowError):
            review_policy.require_current_adapter(legacy_policy)

    def test_complete_v3_assessment_stays_historical_and_cannot_establish_readiness(self):
        policy = {**self.policy, "adapter": frozen.ADAPTER}
        raw = "\n".join(json.dumps(row) for row in self.rows)
        body, diag = telemetry.capture(raw, self.packet, self.packet, policy, "fixture-session")
        self.assertTrue(coverage.assess(self.packet, body, diag, policy=policy)["qualified"])
        meta = review.verify_packet(self.directory)
        meta["review_policy"] = policy
        review.atomic_json(self.directory / "metadata.json", meta)
        review.save_result(self.directory, meta, body, diag, "2.1.282")
        review.recover_review(self.repo, self.directory)
        self.assertFalse(review.coverage_ready(self.directory))
        with self.assertRaisesRegex(workflow.WorkflowError, "recovery-only"):
            review.qualification(self.directory, require=True)
