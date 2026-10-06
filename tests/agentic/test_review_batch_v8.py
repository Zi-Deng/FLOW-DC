"""Current adapter batch/continuation paths with separately qualified v4 fixtures."""

from unittest.mock import patch

import test_reporting_admission as admission_tests
import test_review_batch_v7 as batch_tests
import test_review_continuation as continuation_tests
from test_reporting_recovery_v4 import RecoveryFixture
from test_workflow import review


class CurrentBatchTests(RecoveryFixture):
    ordinary_response = admission_tests.ReportingAdmissionTests.ordinary_response
    child = batch_tests.ReportingBatchFixture.child

    def execute_batch(self, **kwargs):
        return batch_tests.ReportingBatchFixture.execute_batch(self, clock=lambda: self.now, **kwargs)

    def setUp(self):
        super().setUp()
        import review_batch_v7 as batch
        import review_lifetime

        lifetime, timeout = review_lifetime.current_binding, batch.dispatch_timeout
        patch.object(
            review_lifetime,
            "current_binding",
            side_effect=lambda unit, window: lifetime(unit, window, clock=lambda: self.now),
        ).start()
        patch.object(
            batch,
            "dispatch_timeout",
            side_effect=lambda *a, **kw: timeout(*a, **{**kw, "clock": lambda: self.now}),
        ).start()
        import copy

        import reporting_recovery_history_v3

        self.historical_c303 = copy.deepcopy(self.ancestor)
        patch.object(
            reporting_recovery_history_v3,
            "c303",
            side_effect=lambda repo: copy.deepcopy(self.historical_c303),
        ).start()
        self.historical_stopped = self.stopped
        del self.stopped
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
            **batch_tests.batch_fixtures.limits(),
            "kind": "reference-usd",
            "cost": "100",
            "unit_cost": "2",
            "max_report_bytes": self.policy["reporting"]["limits"]["report_bytes"],
        }
        self.children = []

    test_full_scope_exact_reports_and_current_admission = (
        batch_tests.ReportingBatchTests.test_full_scope_exact_reports_and_current_admission
    )

    def test_old_adapter_cannot_substitute_for_current_pair(self):
        import copy

        import claude_reporting_policy
        import reporting_admission
        from workflow import WorkflowError

        policy = copy.deepcopy(self.policy)
        policy["adapter"] = claude_reporting_policy.ADAPTER
        with self.assertRaises(WorkflowError):
            reporting_admission.check(self.repo, policy)
        self.assertEqual(self.calls, 2)


class CurrentContinuationTests(CurrentBatchTests):
    stopped = continuation_tests.ContinuationTests.stopped
    successor_proposal = continuation_tests.ContinuationTests.successor_proposal
    original_execute_batch = CurrentBatchTests.execute_batch

    def execute_batch(self, **kwargs):
        # The existing helper's super() belongs to its legacy fixture. Select its
        # successor branch explicitly; no admission/owned-auth checks are patched.
        if continuation_tests.continuation.manifest(self.directory):
            return continuation_tests.ContinuationTests.execute_batch(self, clock=lambda: self.now, **kwargs)
        return self.original_execute_batch(**kwargs)

    test_complete_successor = continuation_tests.ContinuationTests.test_successor_imports_exact_published_success_and_integrates_every_report
