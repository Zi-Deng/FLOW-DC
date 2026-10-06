"""V5 dispatch guards reach real preflight; no inherited preflight stub."""

import copy
from unittest.mock import patch

import claude_reporting_policy as v7
import reporting_activation_v5 as activation
import reporting_diagnostic_v5 as diagnostic
import review_claude
from test_reporting_diagnostic import EXECUTE
from test_reporting_recovery_v5 import RecoveryFixture
from test_workflow import workflow


class PreflightTests(RecoveryFixture):
    def test_bad_metadata_purpose_adapter_and_profile_refuse_before_discovery(self):
        directory = diagnostic.prepare(self.repo, number=18)
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
        self.assertFalse((activation.root(self.repo) / "attempt-18.json").exists())

    def test_last_moment_authority_history_grant_and_generation_changes_refuse(self):
        import json
        from pathlib import Path

        import claude_reporting_execution as execution
        from tasks import atomic_json

        for change in ("approval", "history", "grant", "registration", "receipt"):
            fixture = RecoveryFixture()
            fixture.setUp()
            try:
                reserve = execution.reserve

                def mutate(*args, fixture=fixture, reserve=reserve, change=change, **kwargs):
                    record = reserve(*args, **kwargs)
                    if change == "approval":
                        path = fixture.task
                        value = activation.read(path)
                        value["approval"]["source"] += " changed"
                    elif change == "history":
                        path = Path(next(iter(fixture.old_v4)))
                        path.write_bytes(path.read_bytes() + b" changed")
                        return record
                    elif change == "grant":
                        path = activation.root(fixture.repo) / "grant.json"
                        value = activation.read(path)
                        value["limits"]["processes"] = 3
                    else:
                        path = fixture.native.root / (
                            "registration.json" if change == "registration" else "receipt.json"
                        )
                        value = json.loads(path.read_bytes())
                        if change == "registration":
                            value["authentication"]["generation_id"] = "33333333-3333-4333-8333-333333333333"
                        else:
                            value["expires_at"] = 1
                    atomic_json(path, value)
                    return record

                with fixture.real_preflight(), patch.object(execution, "reserve", side_effect=mutate):
                    with self.subTest(change=change), self.assertRaises(workflow.WorkflowError):
                        diagnostic.run(fixture.repo, number=18)
                self.assertEqual(fixture.calls, 0)
                self.assertTrue((activation.root(fixture.repo) / "attempt-18.json").is_file())
                self.assertFalse((activation.root(fixture.repo) / "attempt-19.json").exists())
            finally:
                fixture.doCleanups()

    def test_dead_owned_handle_refuses_current_admission(self):
        import reporting_admission
        from test_reporting_owned_auth import SNAPSHOT

        self.qualify()
        with SNAPSHOT(self.policy) as handle:
            self.assertEqual(
                reporting_admission.check(self.repo, self.policy, owned_auth=handle)["schema_version"], 5
            )
        with self.assertRaisesRegex(workflow.WorkflowError, "active owned"):
            reporting_admission.check(self.repo, self.policy, owned_auth=handle)

    def test_last_moment_reservation_mutation_refuses_before_capture(self):
        import claude_reporting_execution as execution
        from tasks import atomic_json

        reserve = execution.reserve

        def mutate(*args, **kwargs):
            record = reserve(*args, **kwargs)
            path = activation.root(self.repo) / "attempt-18.json"
            reservation = activation.read(path)
            reservation["input_digest"] = "f" * 64
            atomic_json(path, reservation)
            return record

        with self.real_preflight(), patch.object(execution, "reserve", side_effect=mutate):
            with self.assertRaises(workflow.WorkflowError):
                diagnostic.run(self.repo, number=18)
        self.assertEqual(self.calls, 0)
