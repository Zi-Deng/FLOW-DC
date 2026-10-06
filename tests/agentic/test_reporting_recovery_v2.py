"""Finite 12/13 recovery against real stopped records; provider output is synthetic."""

import contextlib
import copy
import json
import time
from unittest.mock import patch

import claude_native_auth as auth
import claude_owned_auth as owned
import claude_reporting_execution as execution
import reporting_activation as old
import reporting_activation_v2 as activation
import reporting_admission_v2 as admission
import reporting_diagnostic as old_diagnostic
import reporting_diagnostic_v2 as diagnostic
import reporting_recovery_history as history
import review_batch_v7 as batch
import review_lifetime
import test_claude_native_auth as native_tests
import test_reporting_admission as admission_tests
import test_reporting_owned_auth as guarded_tests
import test_review_batch_v7 as fixtures
from tasks import atomic_json, digest
from test_reporting_diagnostic import ReportingDiagnosticFixture
from test_reporting_owned_auth import BINDING, SNAPSHOT, STORE
from test_workflow import review, workflow


class RecoveryFixture(ReportingDiagnosticFixture):
    legacy_admission = False
    apply_new = True

    def preview(self):
        self.policy["reporting"]["max_turns"] = 400
        return super().preview()

    def setUp(self):
        super().setUp()
        patch("reporting_admission.check", side_effect=admission.check).start()
        # Produce the predecessor's real durable reservation/dispatch claim, but
        # interrupt the mocked process before any capture. No success or usage.
        with self.isolated(process=lambda *a, **kw: (_ for _ in ()).throw(OSError("synthetic stop"))):
            with self.assertRaisesRegex(OSError, "synthetic stop"):
                old_diagnostic.run(self.repo, number=10)
        self.task = self.repo.main / ".agentic-local/tasks/issue-31.json"
        state = old.read(self.task)
        state["approval_history"] = [state["approval"]]
        state["approval"] = {
            **state["approval"],
            "plan_comment": 6008093895,
            "contract": activation.CONTRACT,
        }
        atomic_json(self.task, state)
        patch.object(activation, "harness", side_effect=lambda: copy.deepcopy(self.harness)).start()
        self.before = self.old_bytes()
        self.stopped = history.stopped(self.repo)
        self.proposal = activation.preview(
            self.repo,
            self.policy,
            name="synthetic-recovery-v2",
            tested_head=self.harness["head"],
            expires_at=2200,
            now=1100,
        )
        self.assertFalse(activation.root(self.repo).exists())
        if self.apply_new:
            activation.apply(
                self.repo, self.proposal, preview_digest=self.proposal["preview_digest"], now=1101
            )
        self.now = 1102
        self.response_activation = activation
        self.first_number = 12

    def old_bytes(self):
        return {
            str(p.relative_to(old.root(self.repo))): p.read_bytes()
            for p in old.root(self.repo).rglob("*")
            if p.is_file()
        }

    def qualify(self):
        with self.isolated():
            for number in (12, 13):
                self.assertTrue(diagnostic.run(self.repo, number=number)["qualified"])


class RecoveryTests(RecoveryFixture):
    def test_new_exact_records_recover_offline_preserving_unknown_predecessor(self):
        self.qualify()
        self.assertEqual(admission.check(self.repo, self.policy)["outcomes"].keys(), {"12", "13"})
        self.assertEqual(self.calls, 2)
        self.assertEqual(self.stopped["usage"], "unknown")
        self.assertEqual(self.stopped["reserved_wrapper_processes"], 1)
        self.assertEqual(self.old_bytes(), self.before)
        with patch.object(activation, "context", side_effect=AssertionError("No auth or dispatch")):
            for number in (12, 13):
                self.assertTrue(diagnostic.recover(self.repo, number=number)["qualified"])
                directory = activation.root(self.repo) / f"evidence-{number}"
                capture = old.read(directory / "review-capture.json")
                self.assertEqual((directory / "review.md").read_bytes(), capture["body"].encode())
                self.assertEqual(old.read(directory / diagnostic.FINISHED)["schema_version"], 2)
                self.assertEqual(capture["execution"]["schema_version"], 2)
        for number in (10, 11, 14, True):
            with self.subTest(number=number), self.assertRaises(workflow.WorkflowError):
                diagnostic.run(self.repo, number=number)
        with self.assertRaisesRegex(workflow.WorkflowError, "uncertain"):
            old_diagnostic.recover(self.repo, number=10)
        self.assertEqual(self.old_bytes(), self.before)

    def test_old_approval_and_stopped_history_are_independently_required(self):
        state = old.read(self.task)
        for mutate in (
            lambda s: s.pop("approval_history"),
            lambda s: s["approval_history"][0].update(source="substituted"),
            lambda s: s["approval"].update(contract=old.CONTRACT),
        ):
            value = copy.deepcopy(state)
            mutate(value)
            atomic_json(self.task, value)
            with self.assertRaises(workflow.WorkflowError):
                activation.context(self.repo, self.policy)
            atomic_json(self.task, state)
        path = old.root(self.repo) / "evidence-10/reporting-execution.json"
        before = path.read_bytes()
        path.write_bytes(before + b"x")
        with self.assertRaises(workflow.WorkflowError):
            activation.context(self.repo, self.policy)
        path.write_bytes(before)
        with self.isolated():
            self.assertTrue(diagnostic.run(self.repo, number=12)["qualified"])
        self.assertEqual(self.old_bytes(), self.before)

    def test_storage_success_missing_completion_and_uncertain_first_cannot_start_second(self):
        with self.isolated(process=lambda *a, **kw: (_ for _ in ()).throw(OSError("interrupted"))):
            with self.assertRaises(OSError):
                diagnostic.run(self.repo, number=12)
        for number in (12, 13):
            with self.assertRaises((workflow.WorkflowError, OSError)):
                diagnostic.run(self.repo, number=number)
        self.assertEqual(self.calls, 0)
        self.assertTrue((activation.root(self.repo) / "attempt-12.json").exists())
        self.assertFalse((activation.root(self.repo) / "attempt-13.json").exists())

    def test_unknown_usage_stops_without_refund(self):
        def process(*args, **kwargs):
            result = self.response(*args, **kwargs)
            rows = [json.loads(row) for row in result.stdout.splitlines()]
            del rows[-1]["total_cost_usd"]
            result.stdout = "\n".join(json.dumps(row) for row in rows).encode()
            return result

        with self.isolated(process=process):
            self.assertFalse(diagnostic.run(self.repo, number=12)["qualified"])
            with self.assertRaisesRegex(workflow.WorkflowError, "stopped"):
                diagnostic.run(self.repo, number=13)
        self.assertEqual(self.calls, 1)
        self.assertEqual(self.old_bytes(), self.before)

    def test_rehashed_smaller_scope_and_wrong_version_cannot_launch(self):
        directory = diagnostic.prepare(self.repo, number=12)
        meta = review.verify_packet(directory)
        source = directory / "packet/authentication-source.txt"
        source.write_text("smaller\n")
        meta["files"]["authentication-source.txt"] = review.digest(source)
        atomic_json(directory / "metadata.json", meta)
        with self.isolated(), self.assertRaisesRegex(workflow.WorkflowError, "scope differs"):
            diagnostic.run(self.repo, number=12)
        self.assertEqual(self.calls, 0)
        self.assertFalse((activation.root(self.repo) / "attempt-12.json").exists())

    def test_current_admission_rejects_old_pair_even_when_legacy_evaluator_qualifies(self):
        # A separate original fixture can still replay v1, but current admission
        # cannot select that evaluator or infer new qualification from it.
        fixture = ReportingDiagnosticFixture()
        fixture.legacy_admission = False
        fixture.setUp()
        try:
            with fixture.isolated():
                for number in (10, 11):
                    self.assertTrue(old_diagnostic.run(fixture.repo, number=number)["qualified"])
            with self.assertRaisesRegex(workflow.WorkflowError, "Missing separate"):
                admission.check(fixture.repo, fixture.policy)
        finally:
            fixture.tearDown()
            fixture.doCleanups()

    def test_each_current_capture_execution_completion_and_publication_input_is_required(self):
        self.qualify()
        for number in (12, 13):
            directory = activation.root(self.repo) / f"evidence-{number}"
            for name in (
                "review.md",
                "terminal.txt",
                "reporting-proof.json",
                "diagnostics.json",
                "review-capture.json",
                "reporting-execution.json",
                diagnostic.FINISHED,
                "attempt.json",
                "metadata.json",
                "packet/authentication-source.txt",
            ):
                path = directory / name
                raw = path.read_bytes()
                path.write_bytes(raw + b"x")
                try:
                    with (
                        self.subTest(number=number, name=name),
                        self.assertRaises((workflow.WorkflowError, OSError, ValueError)),
                    ):
                        admission.check(self.repo, self.policy)
                finally:
                    path.write_bytes(raw)
        self.assertEqual(admission.check(self.repo, self.policy)["outcomes"].keys(), {"12", "13"})

    def test_expiry_rollback_context_drift_and_replay_refuse_before_process(self):
        for now in (1100, 2200, float("nan")):
            with self.assertRaises(workflow.WorkflowError):
                activation.reserve(self.repo, number=12, input_digest="a" * 64, now=now)
        for key in ("authorization", "harness", "history", "policy", "tool_contract"):
            # A newly reviewed digest cannot excuse a context substituted after
            # preview; test reservation rechecks the actual bound records.
            path = activation.root(self.repo) / "grant.json"
            raw = path.read_bytes()
            grant = old.read(path)
            grant["binding"][key] = {}
            atomic_json(path, grant)
            try:
                with self.assertRaises((workflow.WorkflowError, KeyError, ValueError)):
                    activation.reserve(self.repo, number=12, input_digest="a" * 64, now=1102)
            finally:
                path.write_bytes(raw)
        with self.assertRaises(workflow.WorkflowError):
            activation.apply(
                self.repo, self.proposal, preview_digest=self.proposal["preview_digest"], now=1102
            )
        self.assertEqual(self.calls, 0)


class GuardedRecoveryFixture(RecoveryFixture):
    ordinary_response = admission_tests.ReportingAdmissionTests.ordinary_response
    renew = guarded_tests.GuardedDiagnosticTests.renew

    def setUp(self):
        super().setUp()
        self.native = native_tests.NativeAuthTests()
        self.native.setUp()
        self.addCleanup(self.native.doCleanups)
        self.native.now = 1102
        self.native.credentials["claudeAiOauth"]["expiresAt"] = (1102 + 7200) * 1000
        with (
            patch.object(auth, "store", STORE),
            patch.object(auth, "new_binding", return_value=self.policy["authentication"]),
        ):
            self.native.register_fixture()
        patch.object(auth, "store", STORE).start()
        patch.object(auth, "current_binding", BINDING).start()
        patch.object(auth, "default_root", return_value=self.native.root).start()
        patch.object(time, "time", side_effect=lambda: self.now).start()

    @contextlib.contextmanager
    def isolated(self, **kwargs):
        # During superclass setup the old stopped record uses its simulated
        # snapshot. Every v2 call uses the actual guarded store/snapshot.
        with super().isolated(**kwargs) as preflight:
            if hasattr(self, "native"):
                with patch.object(owned, "snapshot", SNAPSHOT):
                    yield preflight
            else:
                yield preflight


class GuardedRecoveryTests(GuardedRecoveryFixture):
    def test_guarded_partial_failure_completion_retains_observation_and_stops(self):
        def process(*args, **kwargs):
            with self.assertRaisesRegex(workflow.WorkflowError, "registration is busy"):
                with STORE(self.native.root):
                    self.fail("Capture must retain the real registration lock")
            result = self.response(*args, **kwargs)
            if self.calls == 2:
                rows = [json.loads(row) for row in result.stdout.splitlines()]
                rows.insert(
                    1,
                    {
                        "type": "stream_event",
                        "uuid": "bad-partial",
                        "session_id": rows[0]["session_id"],
                        "event": {"type": "private-unknown", "text": "PRIVATE-NOT-RETAINED"},
                    },
                )
                result.stdout = "\n".join(json.dumps(row) for row in rows).encode()
            return result

        with self.isolated(process=process):
            self.assertTrue(diagnostic.run(self.repo, number=12)["qualified"])
            result = diagnostic.run(self.repo, number=13)
            self.assertFalse(result["qualified"])
        directory = activation.root(self.repo) / "evidence-13"
        capture = old.read(directory / "review-capture.json")
        observed = capture["diagnostics"]["telemetry"]["partial_stream"]
        self.assertEqual(observed["counts"], {"message_active": 1})
        self.assertTrue(capture["reporting"]["accepted"])
        self.assertEqual(capture["diagnostics"]["telemetry"]["controlled_refusals"], 1)
        self.assertTrue((directory / diagnostic.FINISHED).is_file())
        before = {str(p): p.read_bytes() for p in activation.root(self.repo).rglob("*") if p.is_file()}
        with (
            patch.object(auth, "store", side_effect=AssertionError("Offline recovery")),
            patch.object(activation, "context", side_effect=AssertionError("No live context")),
        ):
            self.assertEqual(diagnostic.recover(self.repo, number=13), result)
            self.assertEqual(diagnostic.run(self.repo, number=13), result)
            with self.assertRaises(workflow.WorkflowError):
                diagnostic.run(self.repo, number=14)
        self.assertEqual(
            before, {str(p): p.read_bytes() for p in activation.root(self.repo).rglob("*") if p.is_file()}
        )
        self.assertNotIn(b"PRIVATE-NOT-RETAINED", b"".join(before.values()))
        self.assertEqual(self.calls, 2)
        self.assertEqual(self.old_bytes(), self.before)

    def test_guarded_pair_ordinary_and_renewal_keep_lock_and_full_window(self):
        def process(*args, **kwargs):
            with self.assertRaisesRegex(workflow.WorkflowError, "registration is busy"):
                BINDING(300)
            return self.response(*args, **kwargs)

        with self.isolated(process=process):
            for number in (12, 13):
                self.assertTrue(diagnostic.run(self.repo, number=number)["qualified"])
        for renewal in (False, True):
            if renewal:
                old_auth, new_auth = self.renew()
                self.policy["authentication"] = new_auth
            with patch.object(time, "time", return_value=self.now), SNAPSHOT(self.policy) as handle:
                self.assertEqual(handle.current_binding(300, 3600.5), self.policy["authentication"])
                self.assertEqual(
                    admission.check(self.repo, self.policy, owned_auth=handle)["outcomes"].keys(),
                    {"12", "13"},
                )
                if renewal:
                    self.assertTrue(handle.capability_lineage(old_auth, new_auth, 300))
                with self.assertRaisesRegex(workflow.WorkflowError, "full batch window"):
                    handle.current_binding(300, 7000)
            self.ordinary = review.prepare(
                self.repo,
                31,
                12,
                1234,
                review_provider="claude-code",
                reporting={"max_turns": 400, "limits": self.policy["reporting"]["limits"]},
            )
            self.policy = review.verify_packet(self.ordinary)["review_policy"]
            with self.isolated(process=self.ordinary_response), patch.object(review, "current_pr"):
                review.review(self.repo, self.ordinary)
            with (
                patch.object(workflow, "Repo", return_value=self.repo),
                patch.object(time, "time", return_value=self.now),
            ):
                self.assertTrue(review.coverage_ready(self.ordinary))
                self.assertIn(
                    (self.ordinary / "review.md").read_bytes().decode("utf-8"),
                    review.publication_body(self.ordinary),
                )
        self.assertEqual(self.calls, 4)
        self.assertEqual(self.old_bytes(), self.before)

    def test_external_lock_and_last_moment_receipt_change_refuse_zero_calls(self):
        with (
            self.isolated(),
            STORE(self.native.root),
            self.assertRaisesRegex(workflow.WorkflowError, "registration is busy"),
        ):
            diagnostic.run(self.repo, number=12)
        self.assertFalse((activation.root(self.repo) / "attempt-12.json").exists())
        reserve = execution.reserve

        def mutate(*args, **kwargs):
            value = reserve(*args, **kwargs)
            path = self.native.root / "receipt.json"
            receipt = json.loads(path.read_bytes())
            receipt["expires_at"] = 1
            path.write_text(json.dumps(receipt))
            return value

        with (
            self.isolated(),
            patch.object(execution, "reserve", side_effect=mutate),
            self.assertRaises(workflow.WorkflowError),
        ):
            diagnostic.run(self.repo, number=12)
        self.assertEqual(self.calls, 0)
        with self.assertRaisesRegex(workflow.WorkflowError, "uncertain"):
            diagnostic.recover(self.repo, number=12)


class ApplicationTests(RecoveryFixture):
    apply_new = False

    def test_final_context_time_counts_before_application_or_reservation(self):
        context = activation.context

        def delayed(*args, **kwargs):
            result = context(*args, **kwargs)
            self.now = 2000
            return result

        with (
            patch.object(time, "time", side_effect=lambda: self.now),
            patch.object(activation, "context", side_effect=delayed),
        ):
            with self.assertRaisesRegex(workflow.WorkflowError, "window expired"):
                activation.apply(self.repo, self.proposal, preview_digest=self.proposal["preview_digest"])
        self.assertFalse(activation.root(self.repo).exists())
        self.now = 1102
        activation.apply(self.repo, self.proposal, preview_digest=self.proposal["preview_digest"], now=1101)
        with (
            patch.object(time, "time", side_effect=lambda: self.now),
            patch.object(activation, "context", side_effect=delayed),
        ):
            with self.assertRaisesRegex(workflow.WorkflowError, "window expired"):
                activation.reserve(self.repo, number=12, input_digest="a" * 64)
        self.assertFalse((activation.root(self.repo) / "attempt-12.json").exists())
        self.assertEqual(self.calls, 0)

    def test_torn_application_is_consumed_and_cannot_rename_or_retry(self):
        exclusive = activation.exclusive

        def torn(path, value, **kwargs):
            if path.name == "grant.json":
                raise OSError("synthetic torn grant")
            return exclusive(path, value, **kwargs)

        with (
            patch.object(activation, "exclusive", side_effect=torn),
            self.assertRaisesRegex(OSError, "torn grant"),
        ):
            activation.apply(
                self.repo, self.proposal, preview_digest=self.proposal["preview_digest"], now=1101
            )
        self.assertTrue((activation.root(self.repo) / "application.json").exists())
        self.assertFalse((activation.root(self.repo) / "grant.json").exists())
        with self.assertRaisesRegex(workflow.WorkflowError, "uncertain writes"):
            activation.apply(
                self.repo, self.proposal, preview_digest=self.proposal["preview_digest"], now=1101
            )
        self.assertEqual(self.calls, 0)
        self.assertEqual(self.old_bytes(), self.before)

    def test_fresh_approval_policy_source_auth_and_history_required_at_application(self):
        for key in ("authorization", "harness", "history", "policy", "tool_contract"):
            proposal = copy.deepcopy(self.proposal)
            bound = proposal["grant"]["binding"]
            if key == "authorization":
                bound[key]["approval_digest"] = "f" * 64
            elif key == "harness":
                bound[key]["head"] = "f" * 40
            elif key == "history":
                bound[key]["stopped_v1"]["files"]["attempt-10.json"] = "f" * 64
            elif key == "policy":
                bound[key]["authentication"]["generation_id"] = "33333333-3333-4333-8333-333333333333"
            else:
                bound[key] = {}
            proposal["preview_digest"] = digest(proposal["grant"])
            with self.subTest(key=key), self.assertRaises(workflow.WorkflowError):
                activation.apply(self.repo, proposal, preview_digest=proposal["preview_digest"], now=1101)
            self.assertFalse(activation.root(self.repo).exists())
        for key, value in (("max_turns", 399), ("retry_limit", 2)):
            policy = copy.deepcopy(self.policy)
            policy["reporting"][key] = value
            with self.assertRaises(workflow.WorkflowError):
                activation.context(self.repo, policy)
        activation.apply(self.repo, self.proposal, preview_digest=self.proposal["preview_digest"], now=1101)
        with self.assertRaises(workflow.WorkflowError):
            activation.apply(
                self.repo, self.proposal, preview_digest=self.proposal["preview_digest"], now=1101
            )
        self.assertEqual(self.calls, 0)


class FailureBoundaryTests(RecoveryFixture):
    def test_malformed_first_report_preserves_exact_bytes_and_blocks_second(self):
        def process(*args, **kwargs):
            result = self.response(*args, **kwargs)
            rows = [json.loads(row) for row in result.stdout.splitlines()]
            for row in rows:
                delta = row.get("event", {}).get("delta", {})
                if delta.get("type") == "input_json_delta":
                    delta["partial_json"] = "prose before invalid report"
            result.stdout = "\n".join(json.dumps(row) for row in rows).encode()
            return result

        with self.isolated(process=process):
            self.assertFalse(diagnostic.run(self.repo, number=12)["qualified"])
            with self.assertRaisesRegex(workflow.WorkflowError, "stopped"):
                diagnostic.run(self.repo, number=13)
        directory = activation.root(self.repo) / "evidence-12"
        capture = old.read(directory / "review-capture.json")
        self.assertEqual((directory / "review.md").read_bytes(), capture["body"].encode())
        self.assertEqual(self.calls, 1)

    def test_second_without_actual_refusal_never_admits_or_releases_slot(self):
        with self.isolated():
            self.assertTrue(diagnostic.run(self.repo, number=12)["qualified"])

        def process(*args, **kwargs):
            result = self.response(*args, **kwargs)
            rows = [json.loads(row) for row in result.stdout.splitlines()]
            rows[-1]["permission_denials"] = []
            result.stdout = "\n".join(json.dumps(row) for row in rows).encode()
            return result

        with self.isolated(process=process):
            self.assertFalse(diagnostic.run(self.repo, number=13)["qualified"])
        with self.assertRaises(workflow.WorkflowError):
            admission.check(self.repo, self.policy)
        with self.assertRaises(workflow.WorkflowError):
            activation.reserve(self.repo, number=13, input_digest="f" * 64, now=self.now)
        self.assertEqual(self.calls, 2)

    def test_late_prelaunch_source_recheck_never_calls_provider(self):
        def mutate():
            self.harness["files"]["scripts/agentic/reporting_activation.py"] = "f" * 64

        with self.isolated(recheck=mutate), self.assertRaisesRegex(workflow.WorkflowError, "context changed"):
            diagnostic.run(self.repo, number=12)
        self.assertEqual(self.calls, 0)
        with self.assertRaisesRegex(workflow.WorkflowError, "uncertain"):
            diagnostic.recover(self.repo, number=12)


class CurrentBatchTests(GuardedRecoveryFixture):
    def test_current_v2_batch_integration_requires_every_exact_component_report(self):
        import batch_fixtures
        import review_batch

        self.qualify()
        self.directory = review.prepare(
            self.repo,
            31,
            12,
            1234,
            review_provider="claude-code",
            reporting={"max_turns": 400, "limits": self.policy["reporting"]["limits"]},
        )
        self.bounds = {
            **batch_fixtures.limits(),
            "kind": "reference-usd",
            "cost": "100",
            "unit_cost": "2",
            "max_report_bytes": self.policy["reporting"]["limits"]["report_bytes"],
        }
        self.children = []
        self.ordinary_response = admission_tests.ReportingAdmissionTests.ordinary_response.__get__(self)
        self.child = fixtures.ReportingBatchFixture.child.__get__(self)
        self.execute_batch = lambda **kwargs: fixtures.ReportingBatchFixture.execute_batch(
            self, clock=lambda: self.now, **kwargs
        )
        # Same complete integration/material assertions as the retained v1 suite,
        # now through real current admission (no compatibility evaluator patch).
        lifetime = review_lifetime.current_binding
        timeout = batch.dispatch_timeout
        # Inject the same synthetic clock into default-bound clock parameters;
        # execute the real store/lifetime/dispatch verification without bypass.
        with (
            patch.object(
                review_lifetime,
                "current_binding",
                side_effect=lambda unit, window: lifetime(unit, window, clock=lambda: self.now),
            ),
            patch.object(
                batch,
                "dispatch_timeout",
                side_effect=lambda *a, **kw: timeout(*a, **{**kw, "clock": lambda: self.now}),
            ),
        ):
            fixtures.ReportingBatchTests.test_full_scope_exact_reports_and_current_admission(self)
        ledger = review_batch.state_for(self.directory, batch.load(self.directory))
        self.assertFalse(ledger.get("stop_reason"))
        self.assertEqual(self.old_bytes(), self.before)
