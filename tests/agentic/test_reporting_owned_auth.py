"""The native registration flock is real; credentials and provider are synthetic."""

import contextlib
import copy
import json
import time
import uuid
from pathlib import Path
from unittest.mock import patch

import claude_native_auth as auth
import claude_owned_auth as owned
import claude_reporting_execution as execution
import reporting_activation as activation
import reporting_diagnostic as diagnostic
import test_claude_native_auth as native_tests
import test_reporting_admission as admission_tests
from test_reporting_diagnostic import ReportingDiagnosticFixture
from test_workflow import review, workflow

STORE = auth.store
BINDING = auth.current_binding
SNAPSHOT = owned.snapshot
TIME = time.time
MONOTONIC = time.monotonic


class GuardedDiagnosticTests(ReportingDiagnosticFixture):
    qualify = admission_tests.ReportingAdmissionTests.qualify
    ordinary_packet = admission_tests.ReportingAdmissionTests.ordinary_packet
    ordinary_response = admission_tests.ReportingAdmissionTests.ordinary_response

    def preview(self):
        self.start = TIME() - 10
        return activation.preview(
            self.repo,
            self.policy,
            name="synthetic-owned-auth-only",
            tested_head=self.harness["head"],
            expires_at=self.start + 1000,
            now=self.start,
        )

    def apply(self):
        activation.apply(
            self.repo, self.proposal, preview_digest=self.proposal["preview_digest"], now=self.start + 1
        )

    @contextlib.contextmanager
    def isolated(self, **kwargs):
        with (
            super().isolated(**kwargs) as preflight,
            patch.object(owned, "snapshot", SNAPSHOT),
            patch.object(time, "time", TIME),
            patch.object(time, "monotonic", MONOTONIC),
        ):
            yield preflight

    def setUp(self):
        super().setUp()
        self.native = native_tests.NativeAuthTests()
        self.native.setUp()
        self.addCleanup(self.native.doCleanups)
        # Register the synthetic grant binding against the real clock in a
        # disposable guarded store outside Git. No real profile is consulted.
        self.now = self.start + 2
        self.native.now = self.now
        self.native.credentials["claudeAiOauth"]["expiresAt"] = (self.now + 7200) * 1000
        with (
            patch.object(auth, "store", STORE),
            patch.object(auth, "new_binding", return_value=self.policy["authentication"]),
        ):
            self.native.register_fixture()
        patch.object(auth, "store", STORE).start()
        patch.object(auth, "current_binding", BINDING).start()
        patch.object(auth, "default_root", return_value=self.native.root).start()

    def test_guarded_diagnostic_dispatches_once_with_registration_lock_held(self):
        before = {str(p): p.read_bytes() for p in self.native.root.rglob("*") if p.is_file()}

        def process(*args, **kwargs):
            with self.assertRaisesRegex(workflow.WorkflowError, "registration is busy"):
                with STORE(self.native.root):
                    self.fail("The provider must run under the registration lock")
            saved = json.loads((self.native.root / "registration.json").read_bytes())
            self.assertEqual(saved["authentication"], self.policy["authentication"])
            return self.response(*args, **kwargs)

        with self.isolated(process=process):
            result = diagnostic.run(self.repo, number=10)
            self.assertTrue(result["qualified"])
            self.assertEqual(diagnostic.run(self.repo, number=10), result)
        self.assertEqual(self.calls, 1)
        self.assertEqual(before, {str(p): p.read_bytes() for p in self.native.root.rglob("*") if p.is_file()})

    def test_external_registration_lock_refuses_before_any_provider(self):
        with self.isolated(), STORE(self.native.root):
            with self.assertRaisesRegex(workflow.WorkflowError, "registration is busy"):
                diagnostic.run(self.repo, number=10)
        self.assertEqual(self.calls, 0)
        self.assertFalse((activation.root(self.repo) / "attempt-10.json").exists())

    def test_prelaunch_source_receipt_generation_expiry_and_clock_mutations_stop(self):
        # Each independent fixture has its own immutable, single-use grant.
        for mutation in (
            "source",
            "receipt",
            "generation",
            "expiry",
            "clock",
            "deadline",
            "billing",
            "max",
            "credential-expiry",
            "valid-receipt-change",
        ):
            fixture = GuardedDiagnosticTests()
            fixture.setUp()
            try:
                reserve = execution.reserve

                def changed(*args, reserve=reserve, mutation=mutation, fixture=fixture, **kwargs):
                    result = reserve(*args, **kwargs)
                    if mutation == "source":
                        fixture.harness["files"]["scripts/agentic/reporting_activation.py"] = "e" * 64
                    elif mutation in ("clock", "deadline"):
                        jump = 10 if mutation == "clock" else 1001
                        patch.object(time, "time", return_value=TIME() + jump).start()
                        if mutation == "deadline":
                            patch.object(time, "monotonic", return_value=MONOTONIC() + jump).start()
                    elif mutation in ("billing", "max", "credential-expiry"):
                        registration_path = fixture.native.root / "registration.json"
                        registration = json.loads(registration_path.read_bytes())
                        suffix = ".claude.json" if mutation == "billing" else ".credentials.json"
                        name = next(n for n in registration["files"] if n.endswith(suffix))
                        path = fixture.native.root / name
                        data = json.loads(path.read_bytes())
                        if mutation == "billing":
                            data["oauthAccount"]["hasExtraUsageEnabled"] = True
                        elif mutation == "max":
                            data["claudeAiOauth"]["subscriptionType"] = "pro"
                        else:
                            data["claudeAiOauth"]["expiresAt"] = (TIME() + 100) * 1000
                        path.write_text(json.dumps(data))
                        registration["files"][name] = auth._digest(path.read_bytes())
                        registration_path.write_text(json.dumps(registration))
                    else:
                        path = fixture.native.root / (
                            "registration.json" if mutation == "generation" else "receipt.json"
                        )
                        data = json.loads(path.read_bytes())
                        if mutation == "generation":
                            data["authentication"]["generation_id"] = str(uuid.uuid4())
                        elif mutation == "receipt":
                            data["paid_usage_disabled"] = False
                        elif mutation == "valid-receipt-change":
                            data["expires_at"] -= 1
                        else:
                            data["expires_at"] = TIME() - 1
                        path.write_text(json.dumps(data))
                    return result

                with (
                    self.subTest(mutation=mutation),
                    fixture.isolated(),
                    patch.object(execution, "reserve", side_effect=changed),
                ):
                    expected = {
                        "source": "Reporting context changed",
                        "receipt": auth.ERROR,
                        "generation": "Native login setup is incomplete",
                        "expiry": auth.ERROR,
                        "clock": "Clock changed during native",
                        "deadline": "deadline expired",
                        "billing": auth.ERROR,
                        "max": auth.ERROR,
                        "credential-expiry": auth.ERROR,
                        "valid-receipt-change": "Paid-usage receipt changed",
                    }[mutation]
                    with self.assertRaisesRegex(workflow.WorkflowError, expected):
                        diagnostic.run(fixture.repo, number=10)
                    self.assertEqual(fixture.calls, 0)
                    directory = activation.root(fixture.repo) / "evidence-10"
                    before = (directory / execution.FILENAME).read_bytes()
                    self.assertFalse((directory / "review-capture.json").exists())
                    with self.assertRaises(workflow.WorkflowError):
                        diagnostic.run(fixture.repo, number=10)
                    self.assertEqual((directory / execution.FILENAME).read_bytes(), before)
            finally:
                fixture.tearDown()
                fixture.doCleanups()

    def renew(self):
        old = copy.deepcopy(self.policy["authentication"])
        new = {**old, "generation_id": str(uuid.uuid4())}
        with STORE(self.native.root) as storage:
            registration = storage.read("registration.json")
            prefix = "generations/" + new["generation_id"] + "/config/"
            (self.native.root / prefix).mkdir(mode=0o700, parents=True)
            (self.native.root / "generations" / new["generation_id"]).chmod(0o700)
            for name, value in (
                (".credentials.json", self.native.credentials),
                (".claude.json", self.native.config),
            ):
                storage.write(prefix + name, value)
            registration.update(
                authentication=new,
                lineage=[old],
                retained_capability_generations=[old["generation_id"]],
                files={
                    prefix + name: auth._digest(storage.raw(prefix + name))
                    for name in (".credentials.json", ".claude.json")
                },
            )
            receipt = storage.read("receipt.json")
            receipt["authentication"] = new
            storage.write("registration.json", registration, replace=True)
            storage.write("receipt.json", receipt, replace=True)
            storage.write(
                "setup-attempt.json",
                {"schema_version": 2, "authentication": new, "status": "completed"},
                replace=True,
            )
        return old, new

    def test_ordinary_same_generation_and_renewal_recheck_real_owned_admission(self):
        self.qualify()
        grant = (activation.root(self.repo) / "grant.json").read_bytes()
        for renewed in (False, True):
            if renewed:
                old, new = self.renew()
            self.ordinary_packet()

            def process(*args, **kwargs):
                with self.assertRaisesRegex(workflow.WorkflowError, "registration is busy"):
                    BINDING(300)
                return self.ordinary_response(*args, **kwargs)

            with self.isolated(process=process), patch.object(review, "current_pr"):
                review.review(self.repo, self.ordinary)
            with patch.object(workflow, "Repo", return_value=self.repo):
                self.assertTrue(review.coverage_ready(self.ordinary))
        self.assertEqual(self.calls, 4)
        self.assertEqual((activation.root(self.repo) / "grant.json").read_bytes(), grant)

    def test_owned_window_lineage_lifetime_and_snapshot_mutation(self):
        old, new = self.renew()
        policy = {**self.policy, "authentication": new}
        with SNAPSHOT(policy) as handle:
            self.assertEqual(handle.current_binding(300, 3600.5), new)
            self.assertTrue(handle.capability_lineage(old, new, 300))
            self.assertTrue(handle.capability_lineage(old, old, 300))
            unknown = {**old, "generation_id": str(uuid.uuid4())}
            self.assertFalse(handle.capability_lineage(unknown, new, 300))
            with self.assertRaisesRegex(workflow.WorkflowError, "full batch window"):
                handle.current_binding(300, 7000)
            with self.assertRaisesRegex(workflow.WorkflowError, "Invalid batch lifetime"):
                handle.current_binding(300, float("nan"))
            snapshot = Path(handle.env["CLAUDE_CONFIG_DIR"]) / ".credentials.json"
            self.assertNotIn("refreshToken", snapshot.read_text())
            snapshot.write_text("{}")
            with self.assertRaisesRegex(workflow.WorkflowError, "snapshot changed"):
                handle.recheck()
        self.assertFalse(snapshot.exists())
        with self.assertRaisesRegex(workflow.WorkflowError, "ownership has ended"):
            handle.recheck()
        with self.assertRaisesRegex(workflow.WorkflowError, "active owned"):
            owned.require(handle)
        with self.assertRaisesRegex(workflow.WorkflowError, "active owned"):
            owned.require({"authentication": new})
        self.assertEqual(self.calls, 0)

    def test_guarded_successor_rechecks_imported_publication_and_full_window(self):
        import batch_fixtures
        import review_batch_v7 as batch
        import review_continuation as continuation
        import test_review_batch_v7 as batch_tests
        import test_review_continuation as continuation_tests

        self.qualify()
        self.ordinary_packet()
        self.directory = self.ordinary
        self.bounds = {
            **batch_fixtures.limits(),
            "kind": "reference-usd",
            "cost": "100",
            "unit_cost": "2",
            "max_report_bytes": self.policy["reporting"]["limits"]["report_bytes"],
        }
        self.children = []
        self.child = batch_tests.ReportingBatchFixture.child.__get__(self)

        def execute_batch():
            authorization = batch_fixtures.authorize(self.repo, self.directory, self.bounds)
            if continuation.manifest(self.directory):
                authorization["name"] = "explicit guarded successor including failed unit reattempt"
            batch.select(self.directory, self.bounds, authorization)
            with (
                self.isolated(process=self.ordinary_response),
                patch.object(review, "current_pr"),
                patch.object(review, "review", side_effect=self.child),
                patch.object(batch, "verify_harness"),
                patch.object(workflow, "Repo", return_value=self.repo),
            ):
                return batch.execute(self.repo, self.directory)

        self.execute_batch = execute_batch
        continuation_tests.ContinuationTests.stopped(self)
        before = {str(p): p.read_bytes() for p in self.ancestor.rglob("*") if p.is_file()}
        old, new = self.renew()
        target = self.ancestor.parent / "guarded-renewed-successor"
        with patch.object(batch, "verify_harness"), patch.object(review, "current_pr"):
            continuation.prepare(self.repo, self.ancestor, target, authentication=new)
        self.directory = target
        self.execute_batch()
        with patch.object(workflow, "Repo", return_value=self.repo):
            self.assertTrue(review.qualification(target, require=True)["qualified"])
        self.assertEqual(before, {str(p): p.read_bytes() for p in self.ancestor.rglob("*") if p.is_file()})
        self.assertEqual(review.verify_packet(self.ancestor)["review_policy"]["authentication"], old)
        count = self.calls
        with self.assertRaises(workflow.WorkflowError):
            self.execute_batch()
        self.assertEqual(self.calls, count)
