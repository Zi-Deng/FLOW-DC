"""Cheap disposable journals and pure history records; no native qualification."""

import copy
import json
import socket
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import claude_native_auth
import claude_reporting_policy_v8
import reporting_activation_v6 as activation
import reporting_recovery_history_v6 as history
import review_policy
from claude_fixtures import AUTHENTICATION
from tasks import digest
from workflow import WorkflowError

# Independent published identity; synthetic provenance never means live approval.
CONTRACT = {
    "issue": 31,
    "plan_comment": 6035844223,
    "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
    "plan_digest": "494abcda1fa95d346ae5f8a84b638202701e4756c3ceb24fc4b4f0a9f9d114e5",
}
CONTRACT_DIGEST = "d124f792051c909f1cf77c8717393888141ff64c2c7cf5a38411f48cce7c5467"


def policy():
    return claude_reporting_policy_v8.build(
        {
            **review_policy.policy(review_policy.choices("claude-code"), {}, diagnostic=True),
            "authentication": AUTHENTICATION,
        },
        max_turns=400,
        limits={
            "events": 20000,
            "fragment_bytes": 10000,
            "proof_bytes": 2000000,
            "report_bytes": 10000,
            "terminal_bytes": 60000,
        },
    )


class QualificationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.repo = SimpleNamespace(main=Path(temporary.name), name="Zi-Deng/FLOW-DC")
        self.task = self.repo.main / ".agentic-local/tasks/issue-31.json"
        self.task.parent.mkdir(parents=True)
        self.state = {
            "key": "issue-31",
            "repository": self.repo.name,
            "contract_generation": 12,
            "approval": {
                "issue": 31,
                "plan_comment": 6035844223,
                "contract": copy.deepcopy(CONTRACT),
                "source": "synthetic-only",
                "recorded_at": "synthetic-time",
            },
            "approval_history": [],
        }
        self.write_state(self.state)
        for owner, name in (
            (claude_native_auth, "current_binding"),
            (claude_native_auth, "store"),
            (subprocess, "Popen"),
            (socket.socket, "connect"),
        ):
            self.enterContext(
                patch.object(owner, name, side_effect=AssertionError("Forbidden external operation"))
            )
        self.binding = {
            "authorization": activation.authorization(self.repo),
            "history": {"synthetic": "not qualification"},
            "harness": {"head": "a" * 40, "files": {"synthetic.py": "b" * 64}},
            "policy": policy(),
            "fixtures": {
                str(n): {
                    "fixture_sha256": digest([n, "fixture"]),
                    "descriptor_sha256": digest([n, "descriptor"]),
                }
                for n in (20, 21, 22, 23)
            },
        }

    def write_state(self, state):
        self.task.write_text(json.dumps(state))

    def proposal(self):
        # ONLY the missing context seam is synthetic. Policy and journal validation
        # remain real. No evaluator, outcome or live admission is mocked successful.
        with patch.object(activation, "context", return_value=copy.deepcopy(self.binding)):
            return activation.preview(self.repo, policy(), name="synthetic", tested_head="a" * 40, now=1000)

    def applied(self):
        proposal = self.proposal()
        with patch.object(activation, "context", return_value=copy.deepcopy(self.binding)):
            activation.apply(self.repo, proposal, preview_digest=proposal["preview_digest"], now=1001)
        return proposal

    def test_exact_authority_and_provenance_digest(self):
        self.assertEqual(digest(CONTRACT), CONTRACT_DIGEST)
        first = activation.authorization(self.repo)
        self.assertEqual(
            first, {"contract_digest": CONTRACT_DIGEST, "approval_digest": digest(self.state["approval"])}
        )
        changed = copy.deepcopy(self.state)
        changed["approval"]["source"] = "different synthetic provenance"
        self.write_state(changed)
        self.assertNotEqual(activation.authorization(self.repo)["approval_digest"], first["approval_digest"])
        with patch.object(activation, "CONTRACT_DIGEST", "f" * 64), self.assertRaises(WorkflowError):
            activation.authorization(self.repo)

    def test_authority_independent_mutations(self):
        cases = []
        for field in ("issue", "plan_comment"):
            for value in (None, True, 31.0, "31", 999):
                row = copy.deepcopy(self.state)
                row["approval"][field] = value
                cases.append(row)
        for field in ("source", "recorded_at"):
            for value in (None, "", " ", 1):
                row = copy.deepcopy(self.state)
                row["approval"][field] = value
                cases.append(row)
        for field in CONTRACT:
            row = copy.deepcopy(self.state)
            row["approval"]["contract"][field] = "changed"
            cases.append(row)
        for field, value in [
            ("approval", None),
            ("approval", {}),
            ("repository", "elsewhere"),
            ("key", "issue-32"),
            ("contract_generation", True),
            ("contract_generation", 11),
        ]:
            row = copy.deepcopy(self.state)
            row[field] = value
            cases.append(row)
        row = copy.deepcopy(self.state)
        row["approval"]["contract"]["extra"] = 1
        cases.append(row)
        row = copy.deepcopy(self.state)
        row["approval_history"] = [row.pop("approval")]
        cases.append(row)
        for row in cases:
            self.write_state(row)
            with self.subTest(state=row), self.assertRaises(WorkflowError):
                activation.authorization(self.repo)
        self.assertFalse(activation.root(self.repo).exists())

    def test_inactive_dependencies_refuse_before_any_real_namespace(self):
        with self.assertRaisesRegex(WorkflowError, "integration is not implemented"):
            activation.preview(self.repo, policy(), name="not-live", tested_head="a" * 40, now=1000)
        proposal = self.proposal()
        with self.assertRaisesRegex(WorkflowError, "integration is not implemented"):
            activation.apply(self.repo, proposal, preview_digest=proposal["preview_digest"], now=1001)
        self.assertFalse(activation.root(self.repo).exists())

    def test_positive_local_application_reservation_and_incomplete_completion(self):
        proposal = self.applied()
        grant, app = activation.load(self.repo)
        self.assertEqual(grant, proposal["grant"])
        self.assertEqual(app["deadline"], 8381)
        self.assertEqual(activation.remaining(20), 6480)
        self.assertEqual(sum(s["native_seconds"] for s in grant["slots"]), 2400)
        self.assertEqual(sum(s["reference_usd"] for s in grant["slots"]), 24)
        with patch.object(activation, "context", return_value=copy.deepcopy(self.binding)) as checked:
            record = activation.reserve(self.repo, number=20, input_digest="c" * 64, now=1002)
        self.assertEqual(checked.call_args.kwargs["remaining_seconds"], 7379)
        self.assertEqual(record["deadline"], 2142)
        self.assertEqual(record["replay_deadline"], 2322)
        with self.assertRaisesRegex(WorkflowError, "replay is not implemented"):
            activation.complete(self.repo, number=20, now=1100)
        self.assertFalse((activation.root(self.repo) / "outcome-20.json").exists())
        with self.assertRaises(WorkflowError):
            activation.reserve(self.repo, number=20, input_digest="c" * 64, now=1003)
        with self.assertRaises((WorkflowError, OSError)):
            activation.reserve(self.repo, number=21, input_digest="c" * 64, now=1200)
        for number in (True, 19, 24, 22.0):
            with self.assertRaises(WorkflowError):
                activation.slot(number)

    def test_grant_fields_bounds_and_profile_mutations(self):
        grant = self.proposal()["grant"]
        for mutate in (
            lambda g: g.update(schema_version=True),
            lambda g: g.update(extra=1),
            lambda g: g.update(expires_at=11801),
            lambda g: g.update(not_before=True),
            lambda g: g.update(name="../other"),
            lambda g: g["limits"].update(seconds=2401),
            lambda g: g["limits"].update(api_usd=1),
            lambda g: g["limits"].update(processes=5),
            lambda g: g["slots"][2].update(native_seconds=300),
            lambda g: g["slots"].reverse(),
            lambda g: g["binding"]["authorization"].update(contract_digest="0" * 64),
            lambda g: g["binding"]["fixtures"].pop("23"),
            lambda g: g["binding"]["fixtures"]["22"].update(extra=1),
            lambda g: g["binding"]["harness"].update(head="bad"),
            lambda g: g["binding"]["policy"]["budget"].update(wall_seconds=900),
        ):
            changed = copy.deepcopy(grant)
            mutate(changed)
            with self.subTest(grant=changed), self.assertRaises(WorkflowError):
                activation.validate_grant(changed)

    def test_context_drift_clock_expiry_and_rename(self):
        proposal = self.proposal()
        for key in ("history", "harness", "authorization", "fixtures"):
            changed = copy.deepcopy(self.binding)
            changed[key] = {"changed": True}
            with patch.object(activation, "context", return_value=changed), self.assertRaises(WorkflowError):
                activation.apply(self.repo, proposal, preview_digest=proposal["preview_digest"], now=1001)
        for now in (True, 999, 5000, float("inf")):
            with self.assertRaises(WorkflowError):
                activation.apply(self.repo, proposal, preview_digest=proposal["preview_digest"], now=now)
        self.applied()
        changed = copy.deepcopy(proposal)
        changed["grant"]["name"] = "renamed"
        changed["preview_digest"] = digest(changed["grant"])
        with self.assertRaises(WorkflowError):
            activation.apply(self.repo, changed, preview_digest=changed["preview_digest"], now=1002)
        for now in (999, 1902):
            with self.assertRaises(WorkflowError):
                activation.reserve(self.repo, number=20, input_digest="c" * 64, now=now)
        with patch.object(activation, "context", return_value={}), self.assertRaises(WorkflowError):
            activation.reserve(self.repo, number=20, input_digest="c" * 64, now=1002)

    def test_torn_claim_truncated_record_and_copied_success(self):
        proposal = self.proposal()
        real = activation.exclusive

        def torn(path, value, **kwargs):
            if path.name == "grant.json":
                raise OSError("synthetic interrupted write")
            return real(path, value, **kwargs)

        with (
            patch.object(activation, "context", return_value=self.binding),
            patch.object(activation, "exclusive", side_effect=torn),
            self.assertRaises(OSError),
        ):
            activation.apply(self.repo, proposal, preview_digest=proposal["preview_digest"], now=1001)
        self.assertTrue((activation.root(self.repo) / "application.json").exists())
        with self.assertRaises(WorkflowError):
            activation.apply(self.repo, proposal, preview_digest=proposal["preview_digest"], now=1002)
        with self.assertRaises((WorkflowError, OSError)):
            activation.load(self.repo)
        # Separate disposable journal, never reset the consumed claim above.
        with tempfile.TemporaryDirectory() as directory:
            other = SimpleNamespace(main=Path(directory), name=self.repo.name)
            target = activation.root(other)
            target.mkdir(parents=True)
            (target / "grant.json").write_text("{")
            with self.assertRaises(WorkflowError):
                activation.load(other)

    def test_saved_success_never_advances_unimplemented_replay(self):
        self.applied()
        with patch.object(activation, "context", return_value=self.binding):
            activation.reserve(self.repo, number=20, input_digest="c" * 64, now=1002)
        activation.exclusive(
            activation.root(self.repo) / "outcome-20.json",
            {
                "schema_version": 6,
                "number": 20,
                "qualified": True,
                "finished": 1100,
                "usage": {"status": "observed", "counters": {"duration_ms": 100, "estimated_usd": 0.01}},
            },
        )
        with self.assertRaisesRegex(WorkflowError, "replay is not implemented"):
            activation.reserve(self.repo, number=21, input_digest="d" * 64, now=1200)
        self.assertFalse((activation.root(self.repo) / "attempt-21.json").exists())

    def test_history_cross_record_positive_and_independent_mutations(self):
        original = {
            "stopped_v1": {"usage": "unknown"},
            "stopped_v4": {"usage": "unknown", "unavailable": [17]},
        }
        grant = {
            "schema_version": 5,
            "kind": "reporting-recovery-v5",
            "binding": {
                "history": original,
                "authorization": {
                    "contract_digest": history.old.CONTRACT_DIGEST,
                    "approval_digest": history.V5_APPROVAL,
                },
            },
        }
        outcomes = {
            str(n): {
                "schema_version": 5,
                "number": n,
                "qualified": True,
                "grant_digest": digest(grant),
                "usage": {"status": "observed", "counters": {"duration_ms": 100, "estimated_usd": 0.01}},
            }
            for n in (18, 19)
        }
        history.validate_records(history.V5_TREE, grant, original, outcomes, history.V5_APPROVAL)
        for mutation in (
            lambda c, g, o, h: c.update(file_count=51),
            lambda c, g, o, h: c.update(files_digest="f" * 64),
            lambda c, g, o, h: g.update(schema_version=6),
            lambda c, g, o, h: g["binding"]["history"].update(lost=True),
            lambda c, g, o, h: o.pop("19"),
            lambda c, g, o, h: o["18"].update(number=True),
            lambda c, g, o, h: o["19"].update(qualified=False),
            lambda c, g, o, h: o["18"]["usage"].update(status="unknown"),
        ):
            closure, g, o, h = copy.deepcopy((history.V5_TREE, grant, outcomes, original))
            mutation(closure, g, o, h)
            with self.assertRaises(WorkflowError):
                history.validate_records(closure, g, h, o, history.V5_APPROVAL)
        with self.assertRaises(WorkflowError):
            history.validate_records(history.V5_TREE, grant, original, outcomes, "f" * 64)

    def test_history_refuses_closure_before_older_readers(self):
        with (
            patch.object(history, "tree", return_value={}),
            patch.object(
                history.old, "historical", side_effect=AssertionError("older history must not be read")
            ) as old,
        ):
            with self.assertRaises(WorkflowError):
                history.stopped(self.repo)
            old.assert_not_called()

    def test_unknown_usage_and_budget_overshoot_never_mean_zero(self):
        positive = {"status": "observed", "counters": {"duration_ms": 300000, "estimated_usd": 2}}
        self.assertTrue(history.known_usage(positive, 300, 2))
        for key in ("duration_ms", "estimated_usd"):
            for value in (None, True, -1, float("inf"), float("nan"), 300001):
                changed = copy.deepcopy(positive)
                changed["counters"][key] = value
                self.assertFalse(history.known_usage(changed, 300, 2))
        self.assertFalse(history.known_usage({"status": "unknown"}, 300, 2))
        self.assertFalse(history.known_usage({"status": "observed"}, 300, 2))

    def test_application_record_mutations_and_unexpected_namespace(self):
        self.applied()
        target = activation.root(self.repo)
        path = target / "application.json"
        original = json.loads(path.read_text())
        for key, value in (
            ("schema_version", True),
            ("grant_digest", "f" * 64),
            ("deadline", 999999),
            ("applied_at", 999),
            ("extra", 0),
        ):
            changed = {**original, key: value}
            path.write_text(json.dumps(changed))
            with self.assertRaises(WorkflowError):
                activation.load(self.repo)
        path.write_text(json.dumps(original))
        (target / "attempt-24.json").write_text("{}")
        with self.assertRaises(WorkflowError):
            activation.load(self.repo)

    def test_clock_advance_during_application_checks_leaves_no_claim(self):
        proposal = self.proposal()
        with (
            patch.object(activation, "context", return_value=self.binding),
            patch.object(activation.time, "time", side_effect=[1001, 5000]),
            self.assertRaises(WorkflowError),
        ):
            activation.apply(self.repo, proposal, preview_digest=proposal["preview_digest"])
        self.assertFalse(activation.root(self.repo).exists())

    def test_prerequisite_elapsed_time_never_resets_application_or_slot(self):
        proposal = self.proposal()
        with (
            patch.object(activation, "context", return_value=self.binding),
            patch.object(activation.time, "time", side_effect=[1001, 1004]),
        ):
            activation.apply(self.repo, proposal, preview_digest=proposal["preview_digest"])
        self.assertEqual(activation.load(self.repo)[1]["applied_at"], 1001)
        with (
            patch.object(activation, "context", return_value=self.binding),
            patch.object(activation.time, "time", side_effect=[1005, 1009]),
        ):
            reserved = activation.reserve(self.repo, number=20, input_digest="c" * 64)
        self.assertEqual(reserved["started"], 1005)
        self.assertEqual(reserved["deadline"], 2145)


class DiagnosticDependencyTests(unittest.TestCase):
    """Standalone synthetic storage, not a successful dispatch or live allowance."""

    def setUp(self):
        import reporting_diagnostic_v6 as diagnostic
        from test_capacity_native_v1 import additional, catalog

        self.diagnostic = diagnostic
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.repo = SimpleNamespace(main=Path(temporary.name), name="Zi-Deng/FLOW-DC")
        items, files = additional()
        self.source = {
            "components": catalog(),
            "items": items,
            "files": files,
            "dependencies": {"source": "c" * 64},
        }
        self.packets = diagnostic.packets(self.source)
        self.binding = {
            "authorization": {"contract_digest": CONTRACT_DIGEST, "approval_digest": "d" * 64},
            "history": {"synthetic": "no live qualification"},
            "harness": {"head": "a" * 40, "files": {"synthetic.py": "b" * 64}},
            "policy": policy(),
            "fixtures": {n: diagnostic.fixture_binding(files) for n, files in self.packets.items()},
        }
        with patch.object(activation, "context", return_value=self.binding):
            proposal = activation.preview(
                self.repo, policy(), name="synthetic-storage", tested_head="a" * 40, now=1000
            )
            activation.apply(self.repo, proposal, preview_digest=proposal["preview_digest"], now=1001)
        self.grant = proposal["grant"]
        for owner, name in (
            (claude_native_auth, "current_binding"),
            (claude_native_auth, "store"),
            (subprocess, "Popen"),
            (socket.socket, "connect"),
        ):
            self.enterContext(
                patch.object(owner, name, side_effect=AssertionError("Forbidden external operation"))
            )

    def prepared(self, number):
        import review

        with patch.object(self.diagnostic, "catalog", return_value=self.source):
            directory = self.diagnostic.prepare(self.repo, number=number)
        meta = review.verify_packet(directory)
        return directory, meta

    def stored(self, number=21):
        import claude_reporting_execution as execution
        import claude_telemetry_v8
        import diagnostic_tool_contract
        from test_capacity_native_v1 import raw, stream

        directory, meta = self.prepared(number)
        # A local schema fixture, deliberately NOT a successful predecessor chain.
        reservation = activation._reservation(self.grant, number, digest(meta), 1002)
        activation.exclusive(activation.root(self.repo) / f"attempt-{number}.json", reservation)
        packet = directory / "packet"
        rows = stream(packet)
        session = "11111111-1111-4111-8111-111111111111"
        for row in rows:
            row["session_id"] = session
        progress = next(
            i
            for i, row in enumerate(rows)
            if row["type"] == "assistant" and row["message"]["content"][0]["type"] == "text"
        )
        # Actual permitted Glob response after all mandatory Reads, not an invented
        # reordering or a model statement claiming that a Read happened.
        rows[progress]["message"]["content"] = [
            {
                "type": "tool_use",
                "id": "final-navigation",
                "name": "Glob",
                "input": {"pattern": "capability/fixture.txt"},
            }
        ]
        rows.insert(
            progress + 1,
            {
                "type": "user",
                "uuid": "final-navigation-result",
                "session_id": session,
                "message": {
                    "content": [
                        {
                            "type": "tool_result",
                            "tool_use_id": "final-navigation",
                            "content": "capability/fixture.txt",
                            "is_error": False,
                        }
                    ]
                },
            },
        )
        refusal = None
        if number == 20:
            refusal = str(directory / "outside-refusal-canary.txt")
            fixture = json.loads(
                (Path(__file__).parent / "fixtures/claude-refusal-2.1.282-v5.json").read_text()
            )["positive"]
            fixture["tool"]["message"]["content"][0]["input"]["file_path"] = refusal
            fixture["denials"][0]["tool_input"]["file_path"] = refusal
            message = f"{refusal} is outside {packet}; --restricted confines the file tools to the working directory."
            fixture["advisory"]["message"] = message
            fixture["result"]["message"]["content"][0]["content"] = message
            for key in ("tool", "advisory", "result"):
                fixture[key].update(session_id=session)
                fixture[key].setdefault(
                    "uuid",
                    "11111111-1111-4111-8111-"
                    + {"tool": "000000000021", "advisory": "000000000022", "result": "000000000023"}[key],
                )
            rows[progress:progress] = [fixture["tool"], fixture["advisory"], fixture["result"]]
            rows[-1]["permission_denials"] = fixture["denials"]
        body, diagnostics, proof = claude_telemetry_v8.capture(
            raw(rows),
            packet,
            packet,
            meta["review_policy"],
            session,
            diagnostic_purpose="isolation-refusal" if number == 20 else "native-tools-and-source",
            diagnostic_tool_contract=diagnostic_tool_contract.contract(),
            refusal_path=refusal,
        )
        prompt = self.diagnostic.prompt(directory, meta, refusal)
        record = execution.binding(
            directory,
            meta,
            session,
            prompt,
            diagnostic={"reservation_digest": digest(reservation), "refusal_path": refusal},
        )
        execution.exclusive(directory / execution.FILENAME, record)
        dispatch = self.diagnostic.Dispatch(self.repo, directory, reservation)
        # Exercise only timeout/finish storage here. Real claim/owned-lock launch
        # positives are explicitly deferred to review_claude wiring.
        dispatch.claimed = True
        with (
            patch.object(self.diagnostic.time, "time", return_value=1003),
            patch.object(self.diagnostic.time, "monotonic", return_value=50),
        ):
            self.assertEqual(dispatch.timeout(), activation.slot(number)["native_seconds"])
        with (
            patch.object(self.diagnostic.time, "time", return_value=1004),
            patch.object(self.diagnostic.time, "monotonic", return_value=51),
        ):
            dispatch.finish(meta, session, body, diagnostics, proof)
        capture = {
            "input_digest": digest(meta),
            "provider_version": "2.1.282",
            "body": body,
            "diagnostics": diagnostics,
            "reporting": proof,
            "execution": record,
        }
        return directory, meta, reservation, capture, rows

    def test_four_exact_metadata_profiles_and_prompt_routes(self):
        import claude_reporting_versions
        import reporting_versions

        for number in (20, 21, 22, 23):
            directory, meta = self.prepared(number)
            self.assertIs(reporting_versions.diagnostic(meta), self.diagnostic)
            self.assertEqual(self.diagnostic.identity(self.repo, directory, meta)[1], number)
            self.assertEqual(meta["review_policy"]["budget"]["timeout_seconds"], 300 if number < 22 else 900)
            canary = str(directory / "outside-refusal-canary.txt") if number == 20 else None
            text = self.diagnostic.prompt(directory, meta, canary)
            self.assertIn('Glob with exactly {"pattern":"capability/fixture.txt"}', text)
            if number >= 22:
                with self.assertRaisesRegex(WorkflowError, "300 seconds"):
                    claude_reporting_versions.validate_diagnostic(meta["review_policy"])
            else:
                claude_reporting_versions.validate_diagnostic(meta["review_policy"])
            with self.assertRaises(WorkflowError):
                claude_reporting_versions.validate_v6_diagnostic(
                    self.repo, directory, meta, object(), owned_auth=None
                )
            with self.assertRaises(WorkflowError):
                self.diagnostic.prompt(
                    directory, meta, None if number == 20 else "/tmp/outside-refusal-canary.txt"
                )

    def test_metadata_source_fixture_slot_and_packet_mutations(self):
        import review

        directory, meta = self.prepared(22)
        for mutate in (
            lambda m: m.update(schema_version=True),
            lambda m: m.update(head_sha="f" * 40),
            lambda m: m.update(purpose="issue-31-reporting-recovery-v5"),
            lambda m: m["reporting_activation"].update(number=23),
            lambda m: m["v6_fixture"].update(fixture_sha256="f" * 64),
            lambda m: m["review_policy"]["budget"].update(estimated_usd=2),
            lambda m: m.update(extra="unknown"),
        ):
            changed = copy.deepcopy(meta)
            mutate(changed)
            (directory / "metadata.json").write_text(json.dumps(changed))
            with self.assertRaises(WorkflowError):
                self.diagnostic.identity(self.repo, directory, changed)
        (directory / "metadata.json").write_text(json.dumps(meta))
        (directory / "packet/guidance.txt").write_text("changed")
        with self.assertRaises(WorkflowError):
            review.verify_packet(directory)

    def test_claim_timeout_replay_and_window_refusals(self):
        directory, meta = self.prepared(20)
        with patch.object(activation, "context", return_value=self.binding):
            reservation = activation.reserve(self.repo, number=20, input_digest=digest(meta), now=1002)
        dispatch = self.diagnostic.Dispatch(self.repo, directory, reservation)
        with self.assertRaises(WorkflowError):
            dispatch.timeout()
        with (
            patch.object(activation, "context", return_value=self.binding),
            patch.object(self.diagnostic.time, "time", return_value=1003),
        ):
            dispatch.claim(self.repo, directory, meta)
            with self.assertRaises(WorkflowError):
                dispatch.claim(self.repo, directory, meta)
        with (
            patch.object(activation, "context", return_value={}),
            patch.object(self.diagnostic.time, "time", return_value=1003),
            self.assertRaises(WorkflowError),
        ):
            dispatch.recheck(meta)
        for now in (1001, 1900, float("nan")):
            with (
                patch.object(self.diagnostic.time, "time", return_value=now),
                self.assertRaises(WorkflowError),
            ):
                dispatch.timeout()
        with patch.object(self.diagnostic.time, "time", return_value=1003):
            self.assertEqual(dispatch.timeout(), 300)
            with self.assertRaises(WorkflowError):
                dispatch.timeout()

    def test_frozen_capture_completion_and_independent_mutations(self):
        directory, meta, reservation, capture, _ = self.stored()
        timing, assessment, qualified = self.diagnostic.assess_capture(directory, meta, capture, reservation)
        self.assertTrue(qualified)
        self.assertTrue(assessment["qualified"])
        self.assertEqual(timing["finished"], 1004)
        with self.assertRaisesRegex(WorkflowError, "owned capture persistence"):
            self.diagnostic.owned_capture(directory, meta, capture)
        for mutate in (
            lambda c: c.update(body=c["body"] + " "),
            lambda c: c.update(provider_version="wrong"),
            lambda c: c["execution"].update(session_id="changed"),
            lambda c: c["diagnostics"]["usage"].update(status="unknown"),
            lambda c: c["reporting"].update(accepted=False),
        ):
            changed = copy.deepcopy(capture)
            mutate(changed)
            with self.assertRaises((WorkflowError, ValueError)):
                self.diagnostic.assess_capture(directory, meta, changed, reservation)
        record = json.loads((directory / self.diagnostic.FINISHED).read_text())
        for key, value in (
            ("schema_version", True),
            ("elapsed_seconds", -1),
            ("finished", 999),
            ("fixture", {}),
            ("extra", 1),
        ):
            (directory / self.diagnostic.FINISHED).write_text(json.dumps({**record, key: value}))
            with self.assertRaises(WorkflowError):
                self.diagnostic.completion(directory, capture, reservation)
        (directory / self.diagnostic.FINISHED).write_text("{")
        with self.assertRaises(WorkflowError):
            self.diagnostic.completion(directory, capture, reservation)

    def test_actual_synthetic_isolation_chain_is_independent(self):
        directory, meta, reservation, capture, _ = self.stored(20)
        _, _, qualified = self.diagnostic.assess_capture(directory, meta, capture, reservation)
        self.assertTrue(qualified, capture["diagnostics"]["reasons"])
        # Rebind a structurally valid storage completion to mutated telemetry: the
        # diagnostic requirement itself must refuse, not just the earlier hash.
        changed = copy.deepcopy(capture)
        changed["diagnostics"]["telemetry"]["controlled_refusals"] = 0
        path = directory / self.diagnostic.FINISHED
        record = json.loads(path.read_text())
        record["diagnostics_sha256"] = digest(changed["diagnostics"])
        path.write_text(json.dumps(record))
        with self.assertRaisesRegex(WorkflowError, "exactly one"):
            self.diagnostic.assess_capture(directory, meta, changed, reservation)

    def test_default_catalog_capture_and_dispatch_stay_closed(self):
        with self.assertRaisesRegex(WorkflowError, "final catalog"):
            self.diagnostic.catalog(self.repo)
        for number in (20, 21, 22, 23):
            with self.assertRaisesRegex(WorkflowError, "dispatch wiring"):
                self.diagnostic.run(self.repo, number=number)
        directory, meta, reservation, capture, _ = self.stored()
        with self.assertRaises(WorkflowError):
            self.diagnostic.predecessor(self.repo, 22)
        with self.assertRaises((WorkflowError, OSError)):
            self.diagnostic.recover(self.repo, number=21)
        self.assertFalse((activation.root(self.repo) / "outcome-21.json").exists())

    def test_full_capacity_packets_frozen_capture_and_observer(self):
        import claude_context_observation_v1 as observer
        import review_capacity_native_v1 as capacity
        from test_capacity_native_v1 import bindings, raw

        for number in (22, 23):
            directory, meta, reservation, capture, rows = self.stored(number)
            _, assessment, qualified = self.diagnostic.assess_capture(directory, meta, capture, reservation)
            self.assertTrue(qualified, capture["diagnostics"]["reasons"])
            self.assertEqual(assessment["required_count"], assessment["inspected_count"])
            # Bridge helpers bind the actual synthetic session used by stored().
            for row in rows:
                row["session_id"] = "synthetic-session"
            packet = directory / "packet"
            bound, diagnostics, _ = bindings(packet, rows)
            sidecar, correlation = capacity.bridge(
                raw(rows), packet, packet, meta["review_policy"], "synthetic-session", bound
            )
            summary = observer.replay(sidecar, bound, diagnostics["usage"]["counters"], correlation)
            self.assertLessEqual(summary["response_count"], 400)
            self.assertLessEqual(len(sidecar), 65536)
            with self.assertRaisesRegex(WorkflowError, "owned capture persistence"):
                self.diagnostic.owned_capture(directory, meta, capture)

    def test_legacy_routes_unknown_versions_and_current_profile_guard(self):
        import claude_reporting_versions
        import reporting_diagnostic
        import reporting_diagnostic_v2
        import reporting_diagnostic_v3
        import reporting_diagnostic_v4
        import reporting_diagnostic_v5
        import reporting_versions

        for module in (
            reporting_diagnostic,
            reporting_diagnostic_v2,
            reporting_diagnostic_v3,
            reporting_diagnostic_v4,
            reporting_diagnostic_v5,
        ):
            self.assertIs(reporting_versions.diagnostic({"purpose": module.PURPOSE}), module)
        for purpose in (None, True, "issue-31-reporting-recovery-v7", "issue-31-reporting-recovery-v6 "):
            with self.assertRaises(WorkflowError):
                reporting_versions.diagnostic({"purpose": purpose})
        directory, meta = self.prepared(22)
        dispatch = self.diagnostic.Dispatch(
            self.repo, directory, activation._reservation(self.grant, 22, digest(meta), 1002)
        )
        dispatch.claimed = True
        with self.assertRaises((WorkflowError, OSError)):
            claude_reporting_versions.validate_v6_diagnostic(
                self.repo, directory, meta, dispatch, owned_auth=None
            )

    def test_unknown_usage_and_overrun_remain_failed_after_rebinding(self):
        directory, meta, reservation, capture, _ = self.stored()
        path = directory / self.diagnostic.FINISHED
        original = json.loads(path.read_text())
        for usage in (
            {"status": "unknown", "counters": {}},
            {"status": "observed", "counters": {"duration_ms": 300001, "estimated_usd": 0.1}},
            {"status": "observed", "counters": {"duration_ms": 10, "estimated_usd": 3}},
        ):
            changed = copy.deepcopy(capture)
            changed["diagnostics"]["usage"] = {**usage, "models": {}}
            path.write_text(json.dumps({**original, "diagnostics_sha256": digest(changed["diagnostics"])}))
            self.assertFalse(self.diagnostic.assess_capture(directory, meta, changed, reservation)[2])
        path.write_text(json.dumps({**original, "finished": 1305, "elapsed_seconds": 302}))
        self.assertFalse(self.diagnostic.completion(directory, capture, reservation)[1])
        with self.assertRaises(WorkflowError):
            self.diagnostic.assess_capture(directory, meta, capture, {**reservation, "number": 22})

    def test_final_recheck_elapsed_and_torn_dispatch_do_not_relaunch(self):
        directory, meta = self.prepared(20)
        with patch.object(activation, "context", return_value=self.binding):
            reservation = activation.reserve(self.repo, number=20, input_digest=digest(meta), now=1002)
        dispatch = self.diagnostic.Dispatch(self.repo, directory, reservation)
        with (
            patch.object(activation, "context", return_value=self.binding),
            patch.object(self.diagnostic.time, "time", side_effect=[1003, 1903]),
            self.assertRaisesRegex(WorkflowError, "exhausted"),
        ):
            dispatch.claim(self.repo, directory, meta)
        self.assertFalse(dispatch.claimed)
        self.assertTrue((activation.root(self.repo) / "attempt-20.json").exists())
        saved = activation.root(self.repo) / "attempt-20.json"
        saved.write_text("{")
        with self.assertRaises((WorkflowError, OSError)):
            dispatch.claim(self.repo, directory, meta)
        with self.assertRaises(WorkflowError):
            dispatch.timeout()


class OwnedCaptureTests(unittest.TestCase):
    """Fresh finite stores, real flock/preflight, and synthetic inference only."""

    def setUp(self):
        import time

        import reporting_activation_v2
        import reporting_diagnostic_v6 as diagnostic
        import review_claude
        from reporting_recovery_history import semantics
        from test_capacity_native_v1 import additional, catalog

        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.repo = SimpleNamespace(main=Path(temporary.name) / "repo", name="Zi-Deng/FLOW-DC")
        self.native = Path(temporary.name) / "native"
        self.repo.main.mkdir()
        self.policy = policy()
        self.diagnostic = diagnostic
        self.calls = 0
        self.mutate = None
        items, files = additional()
        self.source = {
            "components": catalog(),
            "items": items,
            "files": files,
            "dependencies": {"source": "c" * 64},
        }
        self.enterContext(patch.object(diagnostic, "catalog", return_value=self.source))
        self.enterContext(
            patch.object(
                activation,
                "authorization",
                return_value={
                    "contract_digest": CONTRACT_DIGEST,
                    "approval_digest": "ae8e4b2ff106d44908e471d1f36be74b5d9f632f1d0b92b32e5261193a245cd8",
                },
            )
        )
        self.enterContext(
            patch.object(
                activation,
                "historical",
                return_value={
                    "stopped_v4": {"policy_semantics": semantics(self.policy)},
                    "synthetic": "no native credit",
                },
            )
        )
        self.enterContext(
            patch.object(
                reporting_activation_v2,
                "harness",
                return_value={"head": "a" * 40, "files": {"synthetic.py": "b" * 64}},
            )
        )
        self.enterContext(patch.object(claude_native_auth, "default_root", return_value=self.native))
        credentials = {
            "claudeAiOauth": {
                "accessToken": "synthetic-never-real",
                "refreshToken": "synthetic-never-real",
                "expiresAt": (time.time() + 20000) * 1000,
                "scopes": ["user:profile", "user:inference"],
                "subscriptionType": "max",
            }
        }
        config = {
            "oauthAccount": {
                "accountUuid": "11111111-1111-4111-8111-111111111111",
                "organizationUuid": "22222222-2222-4222-8222-222222222222",
                "hasExtraUsageEnabled": False,
            },
            "hasCompletedOnboarding": True,
        }
        prefix = "generations/" + self.policy["authentication"]["generation_id"] + "/config/"
        with claude_native_auth.store(self.native, create=True) as storage:
            (self.native / prefix).mkdir(mode=0o700, parents=True)
            (self.native / "generations").chmod(0o700)
            (self.native / "generations" / self.policy["authentication"]["generation_id"]).chmod(0o700)
            storage.write(prefix + ".credentials.json", credentials)
            storage.write(prefix + ".claude.json", config)
            _, account = claude_native_auth.native_records(credentials, config, 900)
            storage.write(
                "registration.json",
                {
                    "authentication": self.policy["authentication"],
                    "cli": self.policy["cli"],
                    "native_exit": 0,
                    "interactive": True,
                    "account": account,
                    "lineage": [],
                    "retained_capability_generations": [],
                    "files": {
                        prefix + name: claude_native_auth._digest(storage.raw(prefix + name))
                        for name in (".credentials.json", ".claude.json")
                    },
                },
            )
            storage.write(
                "setup-attempt.json",
                {"schema_version": 2, "authentication": self.policy["authentication"], "status": "completed"},
            )
            recorded = time.time()
            storage.write(
                "receipt.json",
                {
                    "schema_version": 1,
                    "authentication": self.policy["authentication"],
                    "account": account,
                    "paid_usage_disabled": True,
                    "recorded_at": recorded,
                    "expires_at": recorded + claude_native_auth.RECEIPT_SECONDS,
                },
            )
        import claude_owned_auth

        with claude_owned_auth.snapshot(self.policy) as owned:
            proposal = activation.preview(
                self.repo, self.policy, name="synthetic-owned", tested_head="a" * 40, owned_auth=owned
            )
            activation.apply(self.repo, proposal, preview_digest=proposal["preview_digest"], owned_auth=owned)
        self.enterContext(
            patch.object(review_claude.review_cli, "executable", return_value="/synthetic/claude")
        )
        # Only binary inspection and process output are doubles, not policy,
        # preflight, snapshot, final checks, telemetry, journal or replay.
        self.enterContext(patch.object(review_claude, "check_controls"))
        self.enterContext(patch.object(review_claude.review_process, "capture", side_effect=self.response))
        self.enterContext(patch.object(subprocess, "Popen", side_effect=AssertionError("No subprocess")))
        self.enterContext(patch.object(socket.socket, "connect", side_effect=AssertionError("No network")))

    def response(self, args, **kwargs):
        from test_capacity_native_v1 import raw, stream
        from test_reporting_preflight import controls

        with self.assertRaisesRegex(WorkflowError, "registration is busy"):
            with claude_native_auth.store(self.native):
                self.fail("Original lock must remain held")
        if args[-1] in ("--version", "--help"):
            return controls(args, **kwargs)
        self.calls += 1
        workspace = Path(kwargs["cwd"])
        session = args[args.index("--session-id") + 1]
        rows = stream(workspace)
        for row in rows:
            row["session_id"] = session
        progress = next(
            i
            for i, row in enumerate(rows)
            if row["type"] == "assistant" and row["message"]["content"][0]["type"] == "text"
        )
        rows[progress]["message"]["content"] = [
            {
                "type": "tool_use",
                "id": "final-navigation",
                "name": "Glob",
                "input": {"pattern": "capability/fixture.txt"},
            }
        ]
        rows.insert(
            progress + 1,
            {
                "type": "user",
                "uuid": "final-navigation-result",
                "session_id": session,
                "message": {
                    "content": [
                        {
                            "type": "tool_result",
                            "tool_use_id": "final-navigation",
                            "content": "capability/fixture.txt",
                            "is_error": False,
                        }
                    ]
                },
            },
        )
        outside = workspace.parent / "outside-refusal-canary.txt"
        if outside.exists():
            fixture = json.loads(
                (Path(__file__).parent / "fixtures/claude-refusal-2.1.282-v5.json").read_text()
            )["positive"]
            fixture["tool"]["message"]["content"][0]["input"]["file_path"] = str(outside)
            fixture["denials"][0]["tool_input"]["file_path"] = str(outside)
            message = f"{outside} is outside {workspace}; --restricted confines the file tools to the working directory."
            fixture["advisory"]["message"] = message
            fixture["result"]["message"]["content"][0]["content"] = message
            for key in ("tool", "advisory", "result"):
                fixture[key]["session_id"] = session
                fixture[key].setdefault(
                    "uuid",
                    "11111111-1111-4111-8111-"
                    + {"tool": "000000000021", "advisory": "000000000022", "result": "000000000023"}[key],
                )
            rows[progress:progress] = [fixture["tool"], fixture["advisory"], fixture["result"]]
            rows[-1]["permission_denials"] = fixture["denials"]
        if self.mutate:
            self.mutate(rows)
        return subprocess.CompletedProcess(args, 0, raw(rows), b"")

    def test_real_owned_four_slots_and_independent_admission(self):
        import claude_owned_auth
        import reporting_admission_v6 as admission

        for number in (20, 21, 22, 23):
            outcome = self.diagnostic.run(self.repo, number=number)
            self.assertTrue(outcome["qualified"], outcome)
            self.assertEqual(activation.outcome(self.repo, number), outcome)
        self.assertEqual(self.calls, 4)
        with claude_owned_auth.snapshot(self.policy) as owned:
            record = admission.check(self.repo, owned_auth=owned, capacity_required=True)
        self.assertEqual(record["schema_version"], 6)
        self.assertEqual(set(record["outcomes"]), {"20", "21", "22", "23"})
        self.assertLessEqual(record["empirical_receipt"]["estimate"]["planned_total"], 1000000)
        with self.assertRaises(WorkflowError):
            admission.require_packet(None, {})

    def test_final_receipt_mutation_stops_before_native_capture(self):
        import claude_reporting_execution as execution

        original = execution.reserve

        def mutate(*args, **kwargs):
            result = original(*args, **kwargs)
            path = self.native / "receipt.json"
            receipt = json.loads(path.read_text())
            receipt["expires_at"] = receipt["recorded_at"] + 1
            path.write_text(json.dumps(receipt))
            return result

        with patch.object(execution, "reserve", side_effect=mutate), self.assertRaises(WorkflowError):
            self.diagnostic.run(self.repo, number=20)
        self.assertEqual(self.calls, 0)
        self.assertTrue((activation.root(self.repo) / "attempt-20.json").exists())
        with self.assertRaises(WorkflowError):
            self.diagnostic.run(self.repo, number=21)
        self.assertFalse((activation.root(self.repo) / "attempt-21.json").exists())

    def test_observer_refusal_preserves_exact_capture_and_stops(self):
        import review

        for number in (20, 21):
            self.assertTrue(self.diagnostic.run(self.repo, number=number)["qualified"])

        def missing_counter(rows):
            # Frozen parser accepts incomplete optional counters; the observer
            # must refuse, leaving the original sanitized evidence untouched.
            for row in rows:
                if row["type"] == "assistant":
                    row["message"].get("usage", {}).pop("cache_read_input_tokens", None)

        self.mutate = missing_counter
        with self.assertRaises(WorkflowError):
            self.diagnostic.run(self.repo, number=22)
        directory = activation.root(self.repo) / "evidence-22"
        raw = (directory / "review-capture.json").read_bytes()
        capture = json.loads(raw)
        self.assertEqual(capture["body"], capture["reporting"]["report"])
        self.assertFalse((directory / self.diagnostic.OWNED).exists())
        for number in (22, 23):
            with self.assertRaises(WorkflowError):
                self.diagnostic.run(self.repo, number=number)
        self.assertEqual((directory / "review-capture.json").read_bytes(), raw)
        self.assertEqual(self.calls, 3)
        with self.assertRaises(WorkflowError):
            review.qualification(directory, require=True)

    def test_independent_durable_capture_sidecar_and_source_mutations(self):
        import claude_owned_auth
        import reporting_admission_v6 as admission

        for number in (20, 21, 22):
            self.assertTrue(self.diagnostic.run(self.repo, number=number)["qualified"])
        directory = activation.root(self.repo) / "evidence-22"
        for name, key, value in (
            (self.diagnostic.OWNED, "capture_sha256", "f" * 64),
            (self.diagnostic.OWNED, "completion_sha256", "f" * 64),
            (self.diagnostic.SIDECAR, "schema_version", True),
            ("review-capture.json", "body", "{}"),
            ("reporting-finished.json", "elapsed_seconds", 1000),
            ("reporting-execution.json", "input_digest", "f" * 64),
        ):
            path = directory / name
            raw = path.read_bytes()
            record = json.loads(raw)
            record[key] = value
            path.write_text(json.dumps(record))
            try:
                with self.subTest(name=name, key=key), self.assertRaises((WorkflowError, ValueError)):
                    activation.outcome(self.repo, 22)
            finally:
                path.write_bytes(raw)
        with (
            claude_owned_auth.snapshot(self.policy) as owned,
            patch(
                "reporting_activation_v2.harness",
                return_value={"head": "f" * 40, "files": {"synthetic.py": "b" * 64}},
            ),
            self.assertRaises(WorkflowError),
        ):
            admission.check(self.repo, owned_auth=owned)
        self.assertTrue(activation.outcome(self.repo, 22)["qualified"])

    def test_cli_closed_catalog_and_legacy_sidecar_refusal(self):
        import reporting_cli_v6 as cli
        import review
        from workflow import WorkflowError

        for operation in ("prepare", "run", "recover"):
            for number in (20, 21, 22, 23):
                self.assertEqual(cli.parser().parse_args([operation, "--number", str(number)]).number, number)
        repo = SimpleNamespace(assert_main=lambda: None)
        with (
            patch.object(self.diagnostic, "catalog", side_effect=WorkflowError("closed catalog")),
            patch("claude_owned_auth.snapshot", side_effect=AssertionError("No auth")),
            self.assertRaisesRegex(WorkflowError, "closed catalog"),
        ):
            cli.dispatch(
                repo,
                cli.parser().parse_args(
                    ["preview", "--policy", "absent", "--name", "x", "--tested-head", "a" * 40]
                ),
            )
        directory = self.diagnostic.prepare(self.repo, number=20)
        meta = json.loads((directory / "metadata.json").read_bytes())
        meta["purpose"] = "issue-31-reporting-recovery-v5"
        (directory / "metadata.json").write_text(json.dumps(meta))
        with self.assertRaisesRegex(WorkflowError, "Legacy packet"):
            review.verify_packet(directory)

    def test_final_source_mutation_stops_before_capture(self):
        import claude_reporting_execution as execution
        import reporting_activation_v2

        original = execution.reserve

        def mutate(*args, **kwargs):
            result = original(*args, **kwargs)
            reporting_activation_v2.harness.return_value["files"]["synthetic.py"] = "f" * 64
            return result

        with patch.object(execution, "reserve", side_effect=mutate), self.assertRaises(WorkflowError):
            self.diagnostic.run(self.repo, number=20)
        self.assertEqual(self.calls, 0)
        self.assertTrue((activation.root(self.repo) / "attempt-20.json").exists())

    def test_whole_window_refuses_short_credentials_without_reservation(self):
        prefix = "generations/" + self.policy["authentication"]["generation_id"] + "/config/"
        import time

        with claude_native_auth.store(self.native) as storage:
            credentials = storage.read(prefix + ".credentials.json")
            credentials["claudeAiOauth"]["expiresAt"] = (time.time() + 5000) * 1000
            (self.native / prefix / ".credentials.json").write_text(json.dumps(credentials))
            registration = storage.read("registration.json")
            registration["files"][prefix + ".credentials.json"] = claude_native_auth._digest(
                storage.raw(prefix + ".credentials.json")
            )
            (self.native / "registration.json").write_text(json.dumps(registration))
        with self.assertRaises(WorkflowError):
            self.diagnostic.run(self.repo, number=20)
        self.assertEqual(self.calls, 0)
        self.assertFalse((activation.root(self.repo) / "attempt-20.json").exists())
