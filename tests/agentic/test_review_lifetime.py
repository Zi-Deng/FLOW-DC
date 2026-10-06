"""Full batch windows use synthetic native stores, never workstation credentials."""

import copy
import unittest
import uuid
from unittest.mock import patch

from test_workflow import workflow

# isort: split
import batch_fixtures
import claude_native_auth as auth
import review_batch_v7 as batch
import review_continuation as continuation
import review_lifetime
import test_claude_native_auth as native_tests
from claude_fixtures import AUTHENTICATION
from test_review_batch_v7 import ReportingBatchFixture

LIFETIME = review_lifetime.current_binding

STORE = auth.store
CURRENT = auth.current_binding


class BatchLifetimeBoundaryTests(ReportingBatchFixture):
    def test_large_window_reaches_dispatch_admission_with_real_native_reader(self):
        fixture = native_tests.NativeAuthTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        with (
            patch.object(auth, "store", STORE),
            patch.object(auth, "new_binding", return_value=AUTHENTICATION),
        ):
            fixture.register_fixture()
        batch.select(
            self.directory, self.bounds, batch_fixtures.authorize(self.repo, self.directory, self.bounds)
        )
        with (
            patch.object(auth, "store", STORE),
            patch.object(auth, "default_root", return_value=fixture.root),
            patch.object(auth, "current_binding", CURRENT),
            patch.object(review_lifetime, "current_binding", LIFETIME),
            patch.object(batch.api(), "current_pr"),
            patch.object(continuation, "verify_live", side_effect=RuntimeError("passed lifetime boundary")),
        ):
            with self.assertRaisesRegex(RuntimeError, "passed lifetime boundary"):
                batch.execute(self.repo, self.directory)
        self.assertEqual(self.calls, 2)  # Only the synthetic setup probes; no batch dispatch.

    def test_insufficient_full_window_refuses_without_reservation_or_process(self):
        fixture = native_tests.NativeAuthTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        fixture.credentials["claudeAiOauth"]["expiresAt"] = (fixture.now + 4000) * 1000
        with (
            patch.object(auth, "store", STORE),
            patch.object(auth, "new_binding", return_value=AUTHENTICATION),
        ):
            fixture.register_fixture()
        batch.select(
            self.directory, self.bounds, batch_fixtures.authorize(self.repo, self.directory, self.bounds)
        )
        with (
            patch.object(auth, "store", STORE),
            patch.object(auth, "default_root", return_value=fixture.root),
            patch.object(review_lifetime, "current_binding", LIFETIME),
            patch.object(batch.api(), "current_pr"),
        ):
            with self.assertRaisesRegex(workflow.WorkflowError, "full batch window"):
                batch.execute(self.repo, self.directory)
        self.assertFalse((self.directory / "batch-state.json").exists())
        self.assertEqual(self.calls, 2)


class NativeWindowTests(unittest.TestCase):
    setUp = native_tests.NativeAuthTests.setUp
    register_fixture = native_tests.NativeAuthTests.register_fixture

    def test_full_and_fractional_windows_preserve_native_store(self):
        binding = self.register_fixture()
        before = {str(p): p.read_bytes() for p in self.root.rglob("*") if p.is_file()}
        for window in (3600, 600.5):
            self.assertEqual(LIFETIME(300, window, root=self.root, clock=lambda: self.now + 1), binding)
        self.assertEqual(before, {str(p): p.read_bytes() for p in self.root.rglob("*") if p.is_file()})
        for invalid in (0, -1, True, float("nan"), float("inf"), "3600", auth.RECEIPT_SECONDS + 1):
            with self.subTest(invalid=invalid), self.assertRaises(workflow.WorkflowError):
                LIFETIME(300, invalid, root=self.root)
        for invalid in (600.5, 3600, True):
            with self.subTest(unit=invalid), self.assertRaises(workflow.WorkflowError):
                LIFETIME(invalid, 3600, root=self.root)

    def test_insufficient_window_receipt_expiry_and_rollback_refuse(self):
        self.register_fixture()
        with self.assertRaisesRegex(workflow.WorkflowError, "full batch window"):
            LIFETIME(300, 7000, root=self.root, clock=lambda: self.now + 1)
        with self.assertRaisesRegex(workflow.WorkflowError, "rollback"):
            LIFETIME(300, 3600, root=self.root, clock=iter([self.now + 2, self.now + 1]).__next__)
        with auth.store(self.root) as store:
            receipt = store.read("receipt.json")
            receipt["expires_at"] = self.now + 3000
            store.write("receipt.json", receipt, replace=True)
        with self.assertRaisesRegex(workflow.WorkflowError, "full batch window"):
            LIFETIME(300, 3600, root=self.root, clock=lambda: self.now + 1)
        with self.assertRaises(workflow.WorkflowError):
            LIFETIME(300, 100, root=self.root, clock=lambda: self.now + 3000)

    def test_fresh_source_and_billing_provenance_cannot_be_bypassed(self):
        self.register_fixture()
        with auth.store(self.root) as store:
            registration = store.read("registration.json")
            name = next(n for n in registration["files"] if n.endswith(".claude.json"))
            config = store.read(name)
            config["oauthAccount"]["hasExtraUsageEnabled"] = True
            store.write(name, config, replace=True)
        with self.assertRaises(workflow.WorkflowError):
            LIFETIME(300, 3600, root=self.root)
        with auth.store(self.root) as store:
            registration["files"][name] = auth._digest(store.raw(name))
            store.write("registration.json", registration, replace=True)
        with self.assertRaises(workflow.WorkflowError):
            LIFETIME(300, 3600, root=self.root)

    def test_renewed_same_account_uses_process_timeout_and_separate_window(self):
        binding = self.register_fixture()
        old = {**binding, "generation_id": str(uuid.uuid4())}
        with auth.store(self.root) as store:
            registration = store.read("registration.json")
            registration["lineage"] = [old]
            registration["retained_capability_generations"] = [old["generation_id"]]
            store.write("registration.json", registration, replace=True)
        previous = {
            "policy": {"authentication": old},
            "authorization": {"harness_commit": "head", "harness_files": {}},
            "units": [],
        }
        current = {
            **copy.deepcopy(previous),
            "policy": {"authentication": binding},
            "budget": {"unit_seconds": 300, "seconds": 3600},
        }
        value = {"snapshot": {"ancestor": "fixture", "imports": []}}
        with (
            patch.object(auth, "default_root", return_value=self.root),
            patch.object(continuation, "validate", return_value=value),
            patch.object(batch, "load", return_value=previous),
        ):
            continuation.verify_live(None, None, current)
            self.assertEqual(LIFETIME(300, 3600, root=self.root), binding)
            with self.assertRaisesRegex(workflow.WorkflowError, "full batch window"):
                LIFETIME(300, 7000, root=self.root)
        with auth.store(self.root) as store:
            registration["retained_capability_generations"] = []
            store.write("registration.json", registration, replace=True)
        self.assertFalse(auth.capability_lineage(old, binding, 300, root=self.root))
