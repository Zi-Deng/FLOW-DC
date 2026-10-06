"""Real preflight policy routing; CLI processes and accounts are synthetic only."""

import contextlib
import copy
import subprocess
import time
import unittest
from unittest.mock import patch

import claude_owned_auth
import claude_reporting_policy as v7
import claude_reporting_policy_v8 as v8
import reporting_activation_v4 as activation
import reporting_diagnostic_v4 as diagnostic
import review_claude
import review_policy
from claude_fixtures import AUTHENTICATION
from test_claude_reporting import LIMITS
from test_reporting_diagnostic import EXECUTE
from test_reporting_owned_auth import BINDING, SNAPSHOT
from test_reporting_recovery_v4 import RecoveryFixture
from test_workflow import workflow


class DiscoveryReached(Exception):
    pass


def policy(module=v8, turns=400):
    return module.build(
        {
            **review_policy.policy(review_policy.choices("claude-code"), {}, diagnostic=True),
            "authentication": AUTHENTICATION,
        },
        max_turns=turns,
        limits={**LIMITS, "report_bytes": 10000},
    )


def controls(args, **kwargs):
    if args[-1] not in ("--version", "--help"):
        raise AssertionError("Only synthetic control queries are allowed")
    text = "2.1.282 (Claude Code)" if args[-1] == "--version" else " ".join(review_claude.FLAGS)
    return subprocess.CompletedProcess(args, 0, text.encode(), b"")


class PreflightTests(unittest.TestCase):
    def test_current_policy_reaches_discovery_without_running_a_process(self):
        with (
            patch.object(review_claude.review_cli, "executable", side_effect=DiscoveryReached) as discovery,
            patch.object(
                review_claude.review_process, "capture", side_effect=AssertionError("No process")
            ) as process,
            self.assertRaises(DiscoveryReached),
        ):
            try:
                review_claude.preflight(None, policy(), reporting_diagnostic=True)
            finally:
                process.assert_not_called()
        discovery.assert_called_once_with(None, "claude-code")

    def test_historical_v7_profile_keeps_original_preflight_meaning(self):
        # V1 allowed 80 turns; the current v8 grant requires exactly 400.
        for turns in (80, 400):
            with (
                self.subTest(turns=turns),
                patch.object(review_claude.review_cli, "executable", return_value="/synthetic/claude"),
                patch.object(review_claude, "check_controls") as checked,
                patch.object(review_claude.review_process, "capture", side_effect=controls) as process,
            ):
                selected = policy(v7, turns)
                self.assertEqual(
                    review_claude.preflight(None, selected, reporting_diagnostic=True), "/synthetic/claude"
                )
                checked.assert_called_once_with(
                    "/synthetic/claude", review_claude.trusted_settings(selected), selected
                )
                self.assertEqual(process.call_count, 2)

    def test_wrong_versions_profiles_and_controls_stop_before_discovery(self):
        good = policy()
        mutations = [
            (("schema_version",), True),
            (("schema_version",), 3),
            (("adapter",), "claude-stream-json-2.1.282-v9"),
            (("model",), "unknown"),
            (("reporting", "max_turns"), 80),
            (("reporting", "limits", "report_bytes"), 9999),
            (("budget", "timeout_seconds"), 301),
            (("budget", "cost"), 3),
            (("authentication", "generation_id"), "invalid"),
            (("reporting", "tools"), ["Read", "Grep", "Glob", "StructuredOutput", "Bash"]),
        ]
        for path, value in mutations:
            changed = copy.deepcopy(good)
            target = changed
            for key in path[:-1]:
                target = target[key]
            target[path[-1]] = value
            with (
                self.subTest(path=path),
                patch.object(
                    review_claude.review_cli, "executable", side_effect=AssertionError("No discovery")
                ) as discovery,
                patch.object(
                    review_claude.review_process, "capture", side_effect=AssertionError("No process")
                ) as process,
                self.assertRaises(workflow.WorkflowError),
            ):
                try:
                    review_claude.preflight(None, changed, reporting_diagnostic=True)
                finally:
                    discovery.assert_not_called()
                    process.assert_not_called()

    def test_current_controls_and_historical_budget_still_fail_closed(self):
        with (
            patch.object(review_claude.review_cli, "executable", return_value="/synthetic/claude"),
            patch.object(
                review_claude,
                "check_controls",
                side_effect=workflow.WorkflowError("Unverified binary controls"),
            ),
            patch.object(
                review_claude.review_process, "capture", side_effect=AssertionError("No process")
            ) as process,
            self.assertRaisesRegex(workflow.WorkflowError, "Unverified binary controls"),
        ):
            try:
                review_claude.preflight(None, policy(), reporting_diagnostic=True)
            finally:
                process.assert_not_called()
        legacy = policy(v7, 80)
        legacy["budget"]["timeout_seconds"] = 301
        with (
            patch.object(review_claude.review_cli, "executable", side_effect=AssertionError("No discovery")),
            self.assertRaises(workflow.WorkflowError),
        ):
            review_claude.preflight(None, legacy, reporting_diagnostic=True)


class ExecutePreflightTests(RecoveryFixture):
    @contextlib.contextmanager
    def real_preflight(self, *, process=None):
        # Deliberately do not use inherited isolated(): it mocks preflight.
        # Real native store/flock and owned snapshot operate on synthetic accounts.
        def capture(args, **kwargs):
            if args[-1] in ("--version", "--help"):
                return controls(args, **kwargs)
            with self.assertRaisesRegex(workflow.WorkflowError, "registration is busy"):
                BINDING(300)
            return (process or self.response)(args, **kwargs)

        with (
            patch.object(review_claude, "execute", EXECUTE),
            patch.object(claude_owned_auth, "snapshot", SNAPSHOT),
            patch.object(review_claude.review_cli, "executable", return_value="/synthetic/claude"),
            patch.object(review_claude, "check_controls") as checked,
            patch.object(review_claude.review_process, "capture", side_effect=capture) as captured,
            patch.object(time, "time", side_effect=lambda: self.now),
            patch.object(time, "monotonic", side_effect=lambda: self.monotonic),
        ):
            yield checked, captured

    def test_actual_execute_boundary_traverses_preflight_for_both_purposes(self):
        with self.real_preflight() as (checked, captured):
            for number in (16, 17):
                self.assertTrue(diagnostic.run(self.repo, number=number)["qualified"])
        self.assertEqual(self.calls, 2)
        self.assertEqual(checked.call_count, 2)
        self.assertEqual(captured.call_count, 6)  # version/help/provider per wrapper
        for call in checked.call_args_list:
            self.assertEqual(call.args[2], self.policy)
        with patch.object(review_claude.review_process, "capture", side_effect=AssertionError("No replay")):
            for number in (16, 17):
                self.assertTrue(diagnostic.recover(self.repo, number=number)["qualified"])

    def test_bad_metadata_purpose_adapter_and_profile_refuse_before_discovery(self):
        directory = diagnostic.prepare(self.repo, number=16)
        original = activation.read(directory / "metadata.json")
        mutations = [
            (("purpose",), "unknown"),
            (("purpose",), "issue-31-reporting-recovery-v3"),
            (("review_policy", "adapter"), v7.ADAPTER),
            (("review_policy", "reporting", "max_turns"), 80),
            (("reporting_activation", "purpose"), "native-tools-and-source"),
        ]
        for path, value in mutations:
            changed = copy.deepcopy(original)
            target = changed
            for key in path[:-1]:
                target = target[key]
            target[path[-1]] = value
            context = diagnostic.Dispatch(self.repo, directory, {})
            with (
                self.subTest(path=path, value=value),
                patch.object(
                    review_claude.review_cli, "executable", side_effect=AssertionError("No discovery")
                ) as discovery,
                patch.object(
                    review_claude.review_process, "capture", side_effect=AssertionError("No process")
                ) as process,
                self.assertRaises(workflow.WorkflowError),
            ):
                try:
                    EXECUTE(self.repo, directory, changed, diagnostic=True, dispatch_context=context)
                finally:
                    discovery.assert_not_called()
                    process.assert_not_called()
        self.assertEqual(self.calls, 0)
        self.assertFalse((activation.root(self.repo) / "attempt-16.json").exists())
