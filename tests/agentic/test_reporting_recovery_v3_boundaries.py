"""Independent v3 negative boundaries after actual synthetic positive baselines."""

import copy
import json
from unittest.mock import patch

import claude_native_auth as auth
import claude_reporting_execution as execution
import reporting_activation_v3 as activation
import reporting_admission as admission
import reporting_diagnostic_v3 as diagnostic
import reporting_recovery_history_v3 as history
from tasks import atomic_json, digest
from test_reporting_recovery_v3 import RecoveryFixture
from test_workflow import workflow


class BoundaryTests(RecoveryFixture):
    def test_application_torn_claim_and_changed_bindings_refuse(self):
        fresh = self.parent / "fresh-v3-application"
        with patch.object(activation, "root", return_value=fresh):
            for key in ("authorization", "history", "harness", "policy", "tool_contract", "observation"):
                proposal = copy.deepcopy(self.proposal)
                proposal["grant"]["binding"][key] = {}
                proposal["preview_digest"] = digest(proposal["grant"])
                with self.subTest(key=key), self.assertRaises((workflow.WorkflowError, KeyError)):
                    activation.apply(
                        self.repo, proposal, preview_digest=proposal["preview_digest"], now=self.now
                    )
                self.assertFalse(fresh.exists())
            exclusive = activation.exclusive

            def torn(path, value, **kwargs):
                if path.name == "grant.json":
                    raise OSError("synthetic torn write")
                return exclusive(path, value, **kwargs)

            with patch.object(activation, "exclusive", side_effect=torn), self.assertRaises(OSError):
                activation.apply(
                    self.repo, self.proposal, preview_digest=self.proposal["preview_digest"], now=self.now
                )
            self.assertTrue((fresh / "application.json").is_file())
            with self.assertRaisesRegex(workflow.WorkflowError, "uncertain writes"):
                activation.apply(
                    self.repo, self.proposal, preview_digest=self.proposal["preview_digest"], now=self.now
                )
        self.assertEqual(self.calls, 0)

    def test_exact_policy_version_observation_and_cost_caps_cannot_change(self):
        activation.context(self.repo, self.policy)
        for path, value in (
            (("model",), "other"),
            (("effort",), "high"),
            (("cli", "version"), "other"),
            (("budget", "timeout_seconds"), 301),
            (("budget", "timeout_seconds"), True),
            (("reporting", "max_turns"), 399),
            (("reporting", "retry_limit"), 2),
            (("reporting", "limits", "report_bytes"), 9999),
            (("authentication", "generation_id"), "33333333-3333-4333-8333-333333333333"),
        ):
            policy = copy.deepcopy(self.policy)
            target = policy
            for part in path[:-1]:
                target = target[part]
            target[path[-1]] = value
            with self.subTest(path=path), self.assertRaises(workflow.WorkflowError):
                activation.context(self.repo, policy)
        for field, value in (
            ("schema_version", 2),
            ("slots", []),
            ("stop_on_failure", False),
            ("limits", {}),
        ):
            grant = copy.deepcopy(self.proposal["grant"])
            grant[field] = value
            with self.assertRaises(workflow.WorkflowError):
                activation.validate_grant(grant)
        for value in (True, 1, 3):
            grant = copy.deepcopy(self.proposal["grant"])
            grant["binding"]["observation"]["schema_version"] = value
            with self.assertRaises(workflow.WorkflowError):
                activation.validate_grant(grant)
        with patch.object(history, "V2_GRANT", "f" * 64), self.assertRaises(workflow.WorkflowError):
            activation.context(self.repo, self.policy)
        self.assertEqual(self.calls, 0)

    def test_full_application_and_reservation_window_include_final_check_time(self):
        context = activation.context

        def delayed(*args, **kwargs):
            result = context(*args, **kwargs)
            self.now = 2100
            return result

        fresh = self.parent / "expired-v3-application"
        with (
            patch.object(activation, "root", return_value=fresh),
            patch.object(activation, "context", side_effect=delayed),
        ):
            with self.assertRaisesRegex(workflow.WorkflowError, "window expired"):
                activation.apply(self.repo, self.proposal, preview_digest=self.proposal["preview_digest"])
        self.assertFalse(fresh.exists())
        self.now = 1110
        with (
            patch.object(activation, "context", side_effect=delayed),
            self.assertRaisesRegex(workflow.WorkflowError, "window expired"),
        ):
            activation.reserve(self.repo, number=14, input_digest="a" * 64)
        for instant in (1, 2500, float("nan"), float("inf"), True):
            with self.assertRaises(workflow.WorkflowError):
                activation.reserve(self.repo, number=14, input_digest="a" * 64, now=instant)
        self.assertFalse((activation.root(self.repo) / "attempt-14.json").exists())
        self.assertEqual(self.calls, 0)

    def test_unknown_usage_and_missing_refusal_fail_without_releasing_slots(self):
        def process(*args, **kwargs):
            result = self.response(*args, **kwargs)
            rows = [json.loads(row) for row in result.stdout.splitlines()]
            del rows[-1]["total_cost_usd"]
            rows[-1]["permission_denials"] = []
            result.stdout = "\n".join(json.dumps(row) for row in rows).encode()
            return result

        with self.isolated(process=process):
            self.assertFalse(diagnostic.run(self.repo, number=14)["qualified"])
            with self.assertRaisesRegex(workflow.WorkflowError, "stopped"):
                diagnostic.run(self.repo, number=15)
        with self.assertRaises(workflow.WorkflowError):
            activation.reserve(self.repo, number=14, input_digest="b" * 64)
        self.assertEqual(self.calls, 1)

    def test_unknown_capture_never_replays_or_fabricates_completion(self):
        def interrupted(*args, **kwargs):
            raise OSError("synthetic interruption")

        with (
            self.isolated(process=interrupted),
            self.assertRaisesRegex(workflow.WorkflowError, "Dedicated native Max registration"),
        ):
            diagnostic.run(self.repo, number=14)
        for number in (14, 15):
            with self.assertRaises((workflow.WorkflowError, OSError)):
                diagnostic.run(self.repo, number=number)
        self.assertEqual(self.calls, 0)
        self.assertTrue((activation.root(self.repo) / "attempt-14.json").is_file())
        self.assertFalse((activation.root(self.repo) / "evidence-14" / diagnostic.FINISHED).exists())

    def test_completed_pair_each_artifact_tamper_refuses_offline_and_current_admission(self):
        self.qualify()
        self.assertEqual(admission.check(self.repo, self.policy)["schema_version"], 3)
        for number in (14, 15):
            directory = activation.root(self.repo) / f"evidence-{number}"
            for name in (
                "review.md",
                "diagnostics.json",
                "reporting-proof.json",
                "review-capture.json",
                "review-result.json",
                diagnostic.FINISHED,
                "reporting-execution.json",
            ):
                path = directory / name
                raw = path.read_bytes()
                path.write_bytes(raw + b"tampered")
                try:
                    with (
                        self.subTest(number=number, name=name),
                        self.assertRaises((workflow.WorkflowError, ValueError)),
                    ):
                        admission.check(self.repo, self.policy)
                finally:
                    path.write_bytes(raw)
        self.assertEqual(admission.check(self.repo, self.policy)["schema_version"], 3)
        self.assertEqual(self.calls, 2)

    def test_schema2_observation_is_required_and_tamper_bound_to_completion(self):
        self.qualify()
        directory = activation.root(self.repo) / "evidence-14"
        path = directory / "review-capture.json"
        original = path.read_bytes()
        capture = json.loads(original)
        for value in (None, {"schema_version": 1}, {"schema_version": 2}):
            changed = copy.deepcopy(capture)
            if value is None:
                del changed["diagnostics"]["telemetry"]["partial_stream"]
            else:
                changed["diagnostics"]["telemetry"]["partial_stream"] = value
            atomic_json(path, changed)
            try:
                with (
                    patch.object(auth, "store", side_effect=AssertionError("offline")),
                    self.assertRaises(workflow.WorkflowError),
                ):
                    diagnostic.recover(self.repo, number=14)
            finally:
                path.write_bytes(original)
        self.assertTrue(diagnostic.recover(self.repo, number=14)["qualified"])

    def test_last_moment_receipt_expiry_never_dispatch(self):
        reserve = execution.reserve

        def mutate(*args, **kwargs):
            result = reserve(*args, **kwargs)
            path = self.native.root / "receipt.json"
            receipt = json.loads(path.read_bytes())
            receipt["expires_at"] = 1
            path.write_text(json.dumps(receipt))
            return result

        with (
            self.isolated(),
            patch.object(execution, "reserve", side_effect=mutate),
            self.assertRaises(workflow.WorkflowError),
        ):
            diagnostic.run(self.repo, number=14)
        self.assertEqual(self.calls, 0)
        with self.assertRaisesRegex(workflow.WorkflowError, "uncertain"):
            diagnostic.recover(self.repo, number=14)

    def test_closed_history_tree_and_approval_refuse_extra_symlink_missing_or_rebound(self):
        directory = self.parent / "history-shape"
        directory.mkdir()
        (directory / "one").write_bytes(b"original")
        expected = {"one"}
        baseline = history.tree(directory, expected=expected)
        (directory / "extra").write_bytes(b"extra")
        with self.assertRaises(workflow.WorkflowError):
            history.tree(directory, expected=expected)
        (directory / "extra").unlink()
        (directory / "one").unlink()
        (directory / "one").symlink_to(self.task)
        with self.assertRaises(workflow.WorkflowError):
            history.tree(directory, expected=expected)
        (directory / "one").unlink()
        (directory / "one").write_bytes(b"changed")
        self.assertNotEqual(history.tree(directory, expected=expected), baseline)
        with self.assertRaises(workflow.WorkflowError):
            history.tree(directory, expected=expected, max_bytes=1)
        state = old_state = json.loads(self.task.read_bytes())
        state = copy.deepcopy(state)
        state["approval_history"][1]["source"] = "different provenance"
        atomic_json(self.task, state)
        try:
            with self.assertRaises(workflow.WorkflowError):
                activation.context(self.repo, self.policy)
        finally:
            atomic_json(self.task, old_state)

    def test_last_moment_clock_rollback_preserves_uncertain_claim(self):
        reserve = execution.reserve

        def mutate(*args, **kwargs):
            result = reserve(*args, **kwargs)
            self.now -= 50
            return result

        with (
            self.isolated(),
            patch.object(execution, "reserve", side_effect=mutate),
            self.assertRaises(workflow.WorkflowError),
        ):
            diagnostic.run(self.repo, number=14)
        self.assertEqual(self.calls, 0)
        self.assertTrue((activation.root(self.repo) / "attempt-14.json").is_file())

    def test_missing_actual_refusal_alone_cannot_qualify_isolation_first(self):
        def process(*args, **kwargs):
            result = self.response(*args, **kwargs)
            rows = [json.loads(row) for row in result.stdout.splitlines()]
            rows[-1]["permission_denials"] = []
            result.stdout = "\n".join(json.dumps(row) for row in rows).encode()
            return result

        with self.isolated(process=process):
            self.assertFalse(diagnostic.run(self.repo, number=14)["qualified"])
            with self.assertRaisesRegex(workflow.WorkflowError, "stopped"):
                diagnostic.run(self.repo, number=15)
        self.assertEqual(self.calls, 1)
