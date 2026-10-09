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


class StrictMillisecondTests(unittest.TestCase):
    def test_fixed_machine_thresholds(self):
        import math

        now = 1791396000.000005
        config = {
            "oauthAccount": {
                "accountUuid": "11111111-1111-4111-8111-111111111111",
                "organizationUuid": "22222222-2222-4222-8222-222222222222",
                "hasExtraUsageEnabled": False,
            },
            "hasCompletedOnboarding": True,
        }
        for timeout in (300, 900):
            threshold = (now + timeout + 360) * 1000
            for expiry, accepted in (
                (math.nextafter(threshold, -math.inf), False),
                (threshold, False),
                (math.nextafter(threshold, math.inf), True),
                (threshold + 1000, True),
                (True, False),
                (float("nan"), False),
                ((now + 367 * 86400) * 1000, False),
            ):
                credentials = {
                    "claudeAiOauth": {
                        "accessToken": "fixture-access-never-real",
                        "refreshToken": "fixture-refresh-never-real",
                        "expiresAt": expiry,
                        "scopes": ["user:profile", "user:inference"],
                        "subscriptionType": "max",
                    }
                }
                with self.subTest(timeout=timeout, expiry=expiry, accepted=accepted):
                    if accepted:
                        claude_native_auth.native_records(credentials, config, timeout, now=now)
                    else:
                        with self.assertRaises(WorkflowError):
                            claude_native_auth.native_records(credentials, config, timeout, now=now)


# Exact noncredential governance records, replayed only in disposable fixtures.
G13_STATE = {
    "contract_generation": 13,
    "approval": {
        "issue": 31,
        "plan_comment": 6045434332,
        "contract": {
            "issue": 31,
            "plan_comment": 6045434332,
            "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
            "plan_digest": "05e7a6d54163067d7b0a02f1e3d61e995482e617233b0d973a41940add09d85f",
        },
        "source": "Direct coordinating conversation: maintainer explicitly approved all "
        "things/decisions/plans/actions necessary through a finished private "
        "coauthor-review manuscript and directed this override to persist in "
        "repository memory (2026-10-03). This concrete append-only S amendment "
        "repairs a deterministically reproduced strict-expiry bug and reconciles "
        "only exact prospective authority/history; inherited tests, limits and "
        "zero reviewer paid-extra/API usage remain. Standing receipt "
        "memory/FLOW-DC-standing-authorization.md; exact full verified "
        "plan6045434332. Operator assertion of existing authorization, not new "
        "advisor/coauthor approval or GitHub human review.",
        "recorded_at": "2026-10-07T19:42:04.886852+00:00",
        "note": "Operator assertion of prior human authorization; not public approval proof.",
    },
    "approval_history": [
        {
            "issue": 31,
            "plan_comment": 5900844013,
            "contract": {
                "issue": 31,
                "plan_comment": 5900844013,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "85a4947faefeb29410ed250b29fbf82c3e5dd1c95d2106dbaa928e0c9bc883d7",
            },
            "source": "Maintainer explicitly requested implementation of the supplied "
            "twelve-step roadmap on September 29, 2026 and repeated that "
            "request after pausing; this comment maps its step 2 to "
            "existing source and tests without expanding scope. Paid live "
            "batch remains separately unapproved.",
            "recorded_at": "2026-09-29T23:12:35.136186+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 5966428269,
            "contract": {
                "issue": 31,
                "plan_comment": 5966428269,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "b170fe0c80b101de1568b96b5b07ac6f4418778ca585b53b5f6c755105511b7a",
            },
            "source": "User explicitly authorized all necessary "
            "decisions/plans/actions to complete the coauthor-review "
            "manuscript, requested this override be retained in repository "
            "memory, and confirmed PR34 merged; applied to the concrete "
            "issue31 provider-aware amendment under "
            "memory/FLOW-DC-standing-authorization.md. This is an operator "
            "receipt of standing authorization, not an advisor/coauthor "
            "decision or GitHub approval.",
            "recorded_at": "2026-10-03T06:38:52.487404+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6001819615,
            "contract": {
                "issue": 31,
                "plan_comment": 6001819615,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "2163be1844b7ef658d23276ce425ea8797ffa90bf776c6318109423288865e57",
            },
            "source": "Direct maintainer standing override authorizes all necessary "
            "decisions/plans/actions through private coauthor review, "
            "retained in FLOW-DC memory. Coordinator bound prospective "
            "reporting/test/continuation amendment6001819615 and two new "
            "separately capped activation purposes10/11; old grants, calls, "
            "reports and human merge duties preserved. No approval inferred "
            "from public issue text.",
            "recorded_at": "2026-10-05T19:49:38.315425+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6008093895,
            "contract": {
                "issue": 31,
                "plan_comment": 6008093895,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "e793c7623749aba94ae69fd623d7b406b4ec40227a3f71f271487a5019724734",
            },
            "source": "Maintainer explicitly authorized all necessary "
            "decisions/plans/actions and paid processes through the "
            "finished private coauthor-review manuscript, requested the "
            "override retained in repository memory, and now requested "
            "continuation after upgrading usage. Applied prospectively to "
            "the concrete finite recovery amendment6008093895: only new "
            "purposes12then13,300s/$2referenceeach600/$4total,Maxincludedonly/extraAPI0,no14,alloldreservations/stops/historypreserved. "
            "This supersedes task-level approval prompts; it does not "
            "attest diagnostics, provider usage, advisor/coauthor approval, "
            "merge or scientific readiness. Source: "
            "memory/FLOW-DC-standing-authorization.md and direct "
            "coordinating conversation.",
            "recorded_at": "2026-10-06T02:28:48.397470+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6009076812,
            "contract": {
                "issue": 31,
                "plan_comment": 6009076812,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "88af4787bfab768b81ff05ea7b09ab20bdefbb6e87572666b38c8b890ce1d2d0",
            },
            "source": "Direct maintainer standing override "
            "recorded2026-10-03T01:36:38.550153UTC: all necessary "
            "decisions/plans/actions and paid processes through private "
            "coauthor review. Applied prospectively to this exact "
            "original-Astra-authored v3 plan, published6009076812: "
            "observations only;14isolation-first "
            "then15tools/source;2wrappers,300s/$2reference "
            "each,600s/$4total,Maxincluded/extraAPI0,failure-stop/no16. "
            "Preserve all historical records, existing evidence/review "
            "gates and human login/merge/submission. This is an operator "
            "provenance receipt, not a fabricated human GitHub approval.",
            "recorded_at": "2026-10-06T04:03:24.534151+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6009865197,
            "contract": {
                "issue": 31,
                "plan_comment": 6009865197,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "6493f379fdb2ed9ea1580a38d2fcdc1d618608f42b30613b145faea69323f484",
            },
            "source": 'Direct user standing override recorded 2026-10-03: "I approve '
            "of all things/decisions/plans/actions needed for us to get the "
            "finished manuscript that is ready for co-author review ... "
            'this is an explicit overrite"; reinforced by subsequent '
            "requests to continue and authorize necessary paid processes. "
            "Applied prospectively to the complete source-verified "
            "v8/recovery-v4 amendment6009865197 after actual15 identified "
            "estimated_tokens shapes. Concrete finite limits and all "
            "evidence/human boundaries retained. Local operator receipt, "
            "not inferred GitHub approval or new credential/billing "
            "authority.",
            "recorded_at": "2026-10-06T05:19:19.280275+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6010775261,
            "contract": {
                "issue": 31,
                "plan_comment": 6010775261,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "e87de935601b8bbb50f7fa4188ddeb2bac4e1782479aeed00e2185f9dfc1cf05",
            },
            "source": "Direct coordinating user explicit standing override: approve "
            "all necessary things/decisions/plans/actions and paid "
            "continuations through private coauthor review, recorded "
            "memory/FLOW-DC-standing-authorization.md (2026-10-03). Applies "
            "prospectively to this exact published finite recovery-v5 "
            "contract only, preserving extra/API0, all evidence gates and "
            "human login/merge/submission boundaries. Local operator "
            "receipt, not fabricated human GitHub approval. Current hosted "
            "workflow must pass before source mutation or paid trial.",
            "recorded_at": "2026-10-06T06:33:27.488866+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6011162252,
            "contract": {
                "issue": 31,
                "plan_comment": 6011162252,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "f3d7d5d6656a3397d4c11a759ef543fcbb94c8fa49e83795117f8e922799cde4",
            },
            "source": "Direct coordinating user standing explicit override recorded "
            "memory/FLOW-DC-standing-authorization.md: approves necessary "
            "actions and finite paid continuations through private coauthor "
            "review. Applied prospectively to this exact narrow CI-runtime "
            "amendment only: permits runner repair before hosted success to "
            "resolve two cancellations, preserving all tests, CI15min, "
            "historical evidence and human boundaries. Native-v5 "
            "implementation/diagnostics remain gated on successful "
            "repaired-head software receipts and separate prospective "
            "contract reconciliation. Local operator receipt, not human "
            "GitHub approval.",
            "recorded_at": "2026-10-06T07:02:43.864501+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6012492318,
            "contract": {
                "issue": 31,
                "plan_comment": 6012492318,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "c0dbb09a3b7a03176988263aee50059c905af6750ef42de8042ff386e65b8461",
            },
            "source": "Direct coordinating user standing explicit override in "
            "memory/FLOW-DC-standing-authorization.md approves necessary "
            "concrete actions and finite paid continuations through private "
            "coauthor review. Applied prospectively to this exact "
            "original-Astra-authored recovery-v5 reconciliation at4fbe2af "
            "after verified full local/installed/hosted gates: two separate "
            "single-use diagnostics18isolation-first and "
            "conditional19tools/source,300seconds/$2reference "
            "each,600seconds/$4total,Maxincluded/extraAPI0,failure-stop/no20. "
            "No repeated task approval is pending; exact final-head "
            "software gates and fresh actual prerequisites still precede "
            "separate preview/application/invokes. Full "
            "component+integration review requires a distinct populated "
            "finite grant. Historical approvals/evidence and human "
            "login/merge/submission boundaries remain. Local operator "
            "receipt, not human GitHub approval.",
            "recorded_at": "2026-10-06T08:33:16.223093+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6013795098,
            "contract": {
                "issue": 31,
                "plan_comment": 6013795098,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "8351ce7ce762488c0f2447332487565c1c216e1f24ac440b237dc4d275277130",
            },
            "source": "Direct coordinating user standing explicit override in "
            "memory/FLOW-DC-standing-authorization.md authorizes necessary "
            "concrete actions through private coauthor review. Applied "
            "prospectively to this exact original-Astra-authored 58960-byte "
            "fixture-group scheduling plan at44eeac9. Only check_runner.py, "
            "additive test_check_runner.py and SETUP.md change; preserve803 "
            "occurrences and old methods, protocol2, two workers,840second "
            "phase,900second CI and all historical findings/evidence. "
            "Require exact final serial/parallel, installed affected "
            "suites, coordinator full local and both first-attempt hosted "
            "gates. No native grant/trial funded or authority constant "
            "change; separate prospective native binding reconciliation "
            "remains required. Retain generation9 and prior approvals once "
            "and original executor UUID. Human login, merge and submission "
            "boundaries remain. Local operator receipt, not human GitHub "
            "approval.",
            "recorded_at": "2026-10-06T09:55:50.305381+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6014789492,
            "contract": {
                "issue": 31,
                "plan_comment": 6014789492,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "e2fe530795df6701f43de54bb869032cd5375e92c9ecf543fa4e5a7d7830fb84",
            },
            "source": "Direct coordinating user standing explicit override in "
            "memory/FLOW-DC-standing-authorization.md authorizes necessary "
            "concrete actions through private coauthor review. Applied "
            "prospectively to this exact original-Astra-authored whole "
            "native-v5 authority reconciliation at8bb1abb. Only "
            "reporting_activation_v5.py exact next contract literals and "
            "duplicated plan check, new standalone "
            "test_reporting_authority_v5.py, and PROVIDERS "
            "current-authority guidance change. Preserve all810 occurrences "
            "and existing methods,56frozen paths,83009history, fixed runner "
            "seed/two workers/840second phase/900second CI and every "
            "historical finding/obligation. Require behavioral base "
            "regression, full affected/serial/parallel/disposable "
            "installed, coordinator full local and both first-attempt "
            "final-head hosted gates. Separately prepare/review/apply the "
            "existing finite18/conditional19 pair only after final "
            "gates/fresh prerequisites:two300second/$2reference "
            "wrappers,600seconds/$4total,Maxincluded,paid-extra/API0,failure-stop/no20. "
            "This receipt itself applies no grant and funds no full "
            "component/integration review. Retain generations6-10 exactly "
            "once, same executor UUID. Human login, merge and submission "
            "boundaries remain. Local operator receipt, not human GitHub "
            "approval.",
            "recorded_at": "2026-10-06T10:58:57.843959+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6035844223,
            "contract": {
                "issue": 31,
                "plan_comment": 6035844223,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "494abcda1fa95d346ae5f8a84b638202701e4756c3ceb24fc4b4f0a9f9d114e5",
            },
            "source": "Maintainer explicit standing override recorded October3 in "
            "memory/FLOW-DC-standing-authorization.md authorizes all "
            "necessary decisions/actions through the private "
            "coauthor-review manuscript. Applying it to this exact "
            "reconciled Q1-Q7 no-Console engineering and finite "
            "qualification contract, verified whole-body "
            "publication6035844223 and proposal "
            "SHAe9dcbd25045504eb0f9bddcec2614e93507bc4b53c4c74ffe73b292d301986f3. "
            "Included Max only, extra/API0; human login/merge/submission "
            "boundaries preserved. No live grant or "
            "scientific/advisor/coauthor approval is implied.",
            "recorded_at": "2026-10-07T10:19:09.955094+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
    ],
    "key": "issue-31",
    "repository": "Zi-Deng/FLOW-DC",
}


class NextAuthorityTests(unittest.TestCase):
    write_state = QualificationTests.write_state

    def setUp(self):
        QualificationTests.setUp(self)
        self.state = copy.deepcopy(G13_STATE)
        self.write_state(self.state)

    def test_literal_current_and_historical_storage(self):
        current = activation.authorization(self.repo)
        self.assertEqual(
            current["contract_digest"], "02e78ae136d06616db297cf656a3e17f6d6ee466a29970d7f6869adfe928d691"
        )
        self.assertEqual(
            current["approval_digest"], "0cf9de4afdbd048248941023eaf66b4d4f7be70108d11026c69b30a085bd7605"
        )
        self.assertEqual(activation.selected_contract(self.repo), self.state["approval"]["contract"])
        activation._binding(self.binding)  # Legacy storage still interpretable.
        self.assertNotEqual(self.binding["authorization"], current)
        self.binding["authorization"] = current
        activation._binding(self.binding)
        self.binding["authorization"]["approval_digest"] = "a" * 64
        with self.assertRaises(WorkflowError):
            activation._binding(self.binding)

    def test_exact_history_and_authority_mutations(self):
        changes = [
            lambda s: s.update(contract_generation=True),
            lambda s: s.update(contract_generation=14),
            lambda s: s.update(repository="other/repo"),
            lambda s: s.update(key="issue-32"),
            lambda s: s.update(approval=copy.deepcopy(s["approval_history"][-1])),
            lambda s: s["approval"].update(plan_comment=True),
            lambda s: s["approval"].update(source="copied"),
            lambda s: s["approval"]["contract"].update(plan_digest="a" * 64),
            lambda s: s["approval"].update(extra="torn"),
            lambda s: s.pop("approval"),
            lambda s: s["approval_history"].pop(),
            lambda s: s["approval_history"].append(copy.deepcopy(s["approval_history"][-1])),
            lambda s: s["approval_history"].reverse(),
            lambda s: s["approval_history"].__setitem__(10, copy.deepcopy(s["approval_history"][11])),
            lambda s: s["approval_history"][0].update(source="changed"),
        ]
        for index, change in enumerate(changes):
            with self.subTest(index=index):
                state = copy.deepcopy(self.state)
                change(state)
                self.write_state(state)
                with self.assertRaises(WorkflowError):
                    activation.authorization(self.repo)


class NextOwnedAuthorityTests(unittest.TestCase):
    response = OwnedCaptureTests.response

    def setUp(self):
        self.actual_authorization = activation.authorization
        OwnedCaptureTests.setUp(self)
        self.enterContext(patch.object(activation, "authorization", self.actual_authorization))
        self.task = self.repo.main / ".agentic-local/tasks/issue-31.json"
        self.task.parent.mkdir(parents=True, exist_ok=True)
        self.task.write_text(json.dumps(G13_STATE))

    def test_old_grant_refused_then_new_owned_dispatch_and_admission(self):
        import claude_owned_auth
        import reporting_admission_v6 as admission

        legacy, _ = activation.load(self.repo)
        self.assertEqual(legacy["binding"]["authorization"]["contract_digest"], CONTRACT_DIGEST)
        with claude_owned_auth.snapshot(self.policy) as owned:
            current = activation.context(self.repo, self.policy, owned_auth=owned)
            self.assertNotEqual(current, legacy["binding"])
            with self.assertRaisesRegex(WorkflowError, "current source, authority"):
                admission.check(self.repo, owned_auth=owned)
        with self.assertRaises(WorkflowError):
            self.diagnostic.run(self.repo, number=20)
        self.assertEqual(self.calls, 0)
        # Separate disposable repository; preserve the old grant rather than reset it.
        self.repo = SimpleNamespace(main=self.repo.main.parent / "next-repo", name=self.repo.name)
        self.task = self.repo.main / ".agentic-local/tasks/issue-31.json"
        self.task.parent.mkdir(parents=True)
        self.task.write_text(json.dumps(G13_STATE))
        with claude_owned_auth.snapshot(self.policy) as owned:
            proposal = activation.preview(
                self.repo, self.policy, name="next-synthetic", tested_head="a" * 40, owned_auth=owned
            )
            activation.apply(self.repo, proposal, preview_digest=proposal["preview_digest"], owned_auth=owned)
        for number in (20, 21):
            self.assertTrue(self.diagnostic.run(self.repo, number=number)["qualified"])
        with claude_owned_auth.snapshot(self.policy) as owned:
            self.assertEqual(admission.check(self.repo, owned_auth=owned)["schema_version"], 6)
            state = copy.deepcopy(G13_STATE)
            state["approval_history"].reverse()
            self.task.write_text(json.dumps(state))
            with self.assertRaises(WorkflowError):
                admission.check(self.repo, owned_auth=owned)
        self.assertEqual(self.calls, 2)


class CatalogAuthorityTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        self.repo = SimpleNamespace(main=root, root=root, name="Zi-Deng/FLOW-DC")
        self.task = root / ".agentic-local/tasks/issue-31.json"
        self.task.parent.mkdir(parents=True)
        self.state = copy.deepcopy(G13_STATE)
        self.state["v6_catalog"] = str(root)
        self.task.write_text(json.dumps(self.state))

    def test_current_literal_public_contract_and_old_grant_refusal(self):
        import review
        import review_batch_windows_v1 as windows
        import tasks
        import workflow

        meta = {
            "config": {},
            "schema_version": 7,
            "kind": "single",
            "issue": 31,
            "plan_comment": 6045434332,
            "repository": self.repo.name,
            "pr": 32,
            "head_sha": "a" * 40,
            "base_sha": "b" * 40,
            "merge_base_sha": "c" * 40,
        }
        self.repo.git = lambda *args: {
            "status": "",
            "rev-parse": "a" * 40,
            "merge-base": "c" * 40,
        }[args[0]]
        with (
            patch.object(review, "verify_packet", return_value=meta),
            patch.object(workflow, "configuration", return_value={}),
            patch.object(tasks, "issue_contract", return_value=activation.NEXT_CONTRACT) as public,
            patch.object(review, "current_pr", side_effect=WorkflowError("public transport boundary")),
        ):
            with self.assertRaisesRegex(WorkflowError, "public transport boundary"):
                windows.catalog(self.repo)
            public.assert_called_once_with(self.repo, 31, 6045434332)
            public.return_value = activation.CONTRACT
            with self.assertRaisesRegex(WorkflowError, "current issue/plan changed"):
                windows.catalog(self.repo)
            meta["reporting_activation"] = {"contract_digest": CONTRACT_DIGEST}
            with self.assertRaisesRegex(WorkflowError, "full current metadata7 parent"):
                windows.catalog(self.repo)

    def test_real_authorization_rejects_unknown_missing_bool_and_copied_approval(self):
        import review
        import review_batch_windows_v1 as windows

        changes = [
            lambda s: s.update(contract_generation=14),
            lambda s: s.pop("contract_generation"),
            lambda s: s.update(contract_generation=True),
            lambda s: s.update(approval=copy.deepcopy(s["approval_history"][-1])),
        ]
        with patch.object(review, "verify_packet", side_effect=AssertionError("unauthorized packet read")):
            for change in changes:
                state = copy.deepcopy(self.state)
                change(state)
                self.task.write_text(json.dumps(state))
                with self.assertRaises(WorkflowError):
                    windows.catalog(self.repo)

    def test_fresh_state_change_after_real_authorization_refuses(self):
        import review_batch_windows_v1 as windows

        actual_read = windows.read
        reads = 0

        def changed_read(path):
            nonlocal reads
            value = actual_read(path)
            reads += 1
            if reads == 2:
                value["v6_catalog"] += "-changed"
            return value

        with patch.object(windows, "read", side_effect=changed_read):
            with self.assertRaisesRegex(WorkflowError, "authority changed during selection"):
                windows.catalog(self.repo)
        self.assertEqual(reads, 2)


G14_STATE = {
    "key": "issue-31",
    "repository": "Zi-Deng/FLOW-DC",
    "contract_generation": 14,
    "approval": {
        "issue": 31,
        "plan_comment": 6061320190,
        "contract": {
            "issue": 31,
            "plan_comment": 6061320190,
            "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
            "plan_digest": "22712ec0c4661047adc4b00960ca9f2dd8d162cbc243b658f7ce23a8d63fac21",
        },
        "source": 'Direct coordinating conversation October8,2026: maintainer said "Please increase the operational limit to30 minutes for this particular case then continue the task/plan", accepting the proposed1800-second complete-suite/45-minute enclosing CI recovery while preserving live840/native/credential/billing limits. Existing standing override authorizes all necessary concrete actions through private coauthor-review preparation. This exact T amendment6061320190 scopes only explicit suite opt-in, accountable new execution records, current authority/history and complete inherited public-contract material. Old tests/fixtures/history/limits remain. Authorization record memory/manuscript-2026-10/decisions/suite-timeout-1800-authorization-2026-10-08.json. Operator receipt of actual authorization; not advisor/coauthor/GitHub approval or a new native grant.',
        "recorded_at": "2026-10-08T13:49:39.161057+00:00",
        "note": "Operator assertion of prior human authorization; not public approval proof.",
    },
    "approval_history": [
        {
            "issue": 31,
            "plan_comment": 5900844013,
            "contract": {
                "issue": 31,
                "plan_comment": 5900844013,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "85a4947faefeb29410ed250b29fbf82c3e5dd1c95d2106dbaa928e0c9bc883d7",
            },
            "source": "Maintainer explicitly requested implementation of the supplied twelve-step roadmap on September 29, 2026 and repeated that request after pausing; this comment maps its step 2 to existing source and tests without expanding scope. Paid live batch remains separately unapproved.",
            "recorded_at": "2026-09-29T23:12:35.136186+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 5966428269,
            "contract": {
                "issue": 31,
                "plan_comment": 5966428269,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "b170fe0c80b101de1568b96b5b07ac6f4418778ca585b53b5f6c755105511b7a",
            },
            "source": "User explicitly authorized all necessary decisions/plans/actions to complete the coauthor-review manuscript, requested this override be retained in repository memory, and confirmed PR34 merged; applied to the concrete issue31 provider-aware amendment under memory/FLOW-DC-standing-authorization.md. This is an operator receipt of standing authorization, not an advisor/coauthor decision or GitHub approval.",
            "recorded_at": "2026-10-03T06:38:52.487404+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6001819615,
            "contract": {
                "issue": 31,
                "plan_comment": 6001819615,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "2163be1844b7ef658d23276ce425ea8797ffa90bf776c6318109423288865e57",
            },
            "source": "Direct maintainer standing override authorizes all necessary decisions/plans/actions through private coauthor review, retained in FLOW-DC memory. Coordinator bound prospective reporting/test/continuation amendment6001819615 and two new separately capped activation purposes10/11; old grants, calls, reports and human merge duties preserved. No approval inferred from public issue text.",
            "recorded_at": "2026-10-05T19:49:38.315425+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6008093895,
            "contract": {
                "issue": 31,
                "plan_comment": 6008093895,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "e793c7623749aba94ae69fd623d7b406b4ec40227a3f71f271487a5019724734",
            },
            "source": "Maintainer explicitly authorized all necessary decisions/plans/actions and paid processes through the finished private coauthor-review manuscript, requested the override retained in repository memory, and now requested continuation after upgrading usage. Applied prospectively to the concrete finite recovery amendment6008093895: only new purposes12then13,300s/$2referenceeach600/$4total,Maxincludedonly/extraAPI0,no14,alloldreservations/stops/historypreserved. This supersedes task-level approval prompts; it does not attest diagnostics, provider usage, advisor/coauthor approval, merge or scientific readiness. Source: memory/FLOW-DC-standing-authorization.md and direct coordinating conversation.",
            "recorded_at": "2026-10-06T02:28:48.397470+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6009076812,
            "contract": {
                "issue": 31,
                "plan_comment": 6009076812,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "88af4787bfab768b81ff05ea7b09ab20bdefbb6e87572666b38c8b890ce1d2d0",
            },
            "source": "Direct maintainer standing override recorded2026-10-03T01:36:38.550153UTC: all necessary decisions/plans/actions and paid processes through private coauthor review. Applied prospectively to this exact original-Astra-authored v3 plan, published6009076812: observations only;14isolation-first then15tools/source;2wrappers,300s/$2reference each,600s/$4total,Maxincluded/extraAPI0,failure-stop/no16. Preserve all historical records, existing evidence/review gates and human login/merge/submission. This is an operator provenance receipt, not a fabricated human GitHub approval.",
            "recorded_at": "2026-10-06T04:03:24.534151+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6009865197,
            "contract": {
                "issue": 31,
                "plan_comment": 6009865197,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "6493f379fdb2ed9ea1580a38d2fcdc1d618608f42b30613b145faea69323f484",
            },
            "source": 'Direct user standing override recorded 2026-10-03: "I approve of all things/decisions/plans/actions needed for us to get the finished manuscript that is ready for co-author review ... this is an explicit overrite"; reinforced by subsequent requests to continue and authorize necessary paid processes. Applied prospectively to the complete source-verified v8/recovery-v4 amendment6009865197 after actual15 identified estimated_tokens shapes. Concrete finite limits and all evidence/human boundaries retained. Local operator receipt, not inferred GitHub approval or new credential/billing authority.',
            "recorded_at": "2026-10-06T05:19:19.280275+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6010775261,
            "contract": {
                "issue": 31,
                "plan_comment": 6010775261,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "e87de935601b8bbb50f7fa4188ddeb2bac4e1782479aeed00e2185f9dfc1cf05",
            },
            "source": "Direct coordinating user explicit standing override: approve all necessary things/decisions/plans/actions and paid continuations through private coauthor review, recorded memory/FLOW-DC-standing-authorization.md (2026-10-03). Applies prospectively to this exact published finite recovery-v5 contract only, preserving extra/API0, all evidence gates and human login/merge/submission boundaries. Local operator receipt, not fabricated human GitHub approval. Current hosted workflow must pass before source mutation or paid trial.",
            "recorded_at": "2026-10-06T06:33:27.488866+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6011162252,
            "contract": {
                "issue": 31,
                "plan_comment": 6011162252,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "f3d7d5d6656a3397d4c11a759ef543fcbb94c8fa49e83795117f8e922799cde4",
            },
            "source": "Direct coordinating user standing explicit override recorded memory/FLOW-DC-standing-authorization.md: approves necessary actions and finite paid continuations through private coauthor review. Applied prospectively to this exact narrow CI-runtime amendment only: permits runner repair before hosted success to resolve two cancellations, preserving all tests, CI15min, historical evidence and human boundaries. Native-v5 implementation/diagnostics remain gated on successful repaired-head software receipts and separate prospective contract reconciliation. Local operator receipt, not human GitHub approval.",
            "recorded_at": "2026-10-06T07:02:43.864501+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6012492318,
            "contract": {
                "issue": 31,
                "plan_comment": 6012492318,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "c0dbb09a3b7a03176988263aee50059c905af6750ef42de8042ff386e65b8461",
            },
            "source": "Direct coordinating user standing explicit override in memory/FLOW-DC-standing-authorization.md approves necessary concrete actions and finite paid continuations through private coauthor review. Applied prospectively to this exact original-Astra-authored recovery-v5 reconciliation at4fbe2af after verified full local/installed/hosted gates: two separate single-use diagnostics18isolation-first and conditional19tools/source,300seconds/$2reference each,600seconds/$4total,Maxincluded/extraAPI0,failure-stop/no20. No repeated task approval is pending; exact final-head software gates and fresh actual prerequisites still precede separate preview/application/invokes. Full component+integration review requires a distinct populated finite grant. Historical approvals/evidence and human login/merge/submission boundaries remain. Local operator receipt, not human GitHub approval.",
            "recorded_at": "2026-10-06T08:33:16.223093+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6013795098,
            "contract": {
                "issue": 31,
                "plan_comment": 6013795098,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "8351ce7ce762488c0f2447332487565c1c216e1f24ac440b237dc4d275277130",
            },
            "source": "Direct coordinating user standing explicit override in memory/FLOW-DC-standing-authorization.md authorizes necessary concrete actions through private coauthor review. Applied prospectively to this exact original-Astra-authored 58960-byte fixture-group scheduling plan at44eeac9. Only check_runner.py, additive test_check_runner.py and SETUP.md change; preserve803 occurrences and old methods, protocol2, two workers,840second phase,900second CI and all historical findings/evidence. Require exact final serial/parallel, installed affected suites, coordinator full local and both first-attempt hosted gates. No native grant/trial funded or authority constant change; separate prospective native binding reconciliation remains required. Retain generation9 and prior approvals once and original executor UUID. Human login, merge and submission boundaries remain. Local operator receipt, not human GitHub approval.",
            "recorded_at": "2026-10-06T09:55:50.305381+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6014789492,
            "contract": {
                "issue": 31,
                "plan_comment": 6014789492,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "e2fe530795df6701f43de54bb869032cd5375e92c9ecf543fa4e5a7d7830fb84",
            },
            "source": "Direct coordinating user standing explicit override in memory/FLOW-DC-standing-authorization.md authorizes necessary concrete actions through private coauthor review. Applied prospectively to this exact original-Astra-authored whole native-v5 authority reconciliation at8bb1abb. Only reporting_activation_v5.py exact next contract literals and duplicated plan check, new standalone test_reporting_authority_v5.py, and PROVIDERS current-authority guidance change. Preserve all810 occurrences and existing methods,56frozen paths,83009history, fixed runner seed/two workers/840second phase/900second CI and every historical finding/obligation. Require behavioral base regression, full affected/serial/parallel/disposable installed, coordinator full local and both first-attempt final-head hosted gates. Separately prepare/review/apply the existing finite18/conditional19 pair only after final gates/fresh prerequisites:two300second/$2reference wrappers,600seconds/$4total,Maxincluded,paid-extra/API0,failure-stop/no20. This receipt itself applies no grant and funds no full component/integration review. Retain generations6-10 exactly once, same executor UUID. Human login, merge and submission boundaries remain. Local operator receipt, not human GitHub approval.",
            "recorded_at": "2026-10-06T10:58:57.843959+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6035844223,
            "contract": {
                "issue": 31,
                "plan_comment": 6035844223,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "494abcda1fa95d346ae5f8a84b638202701e4756c3ceb24fc4b4f0a9f9d114e5",
            },
            "source": "Maintainer explicit standing override recorded October3 in memory/FLOW-DC-standing-authorization.md authorizes all necessary decisions/actions through the private coauthor-review manuscript. Applying it to this exact reconciled Q1-Q7 no-Console engineering and finite qualification contract, verified whole-body publication6035844223 and proposal SHAe9dcbd25045504eb0f9bddcec2614e93507bc4b53c4c74ffe73b292d301986f3. Included Max only, extra/API0; human login/merge/submission boundaries preserved. No live grant or scientific/advisor/coauthor approval is implied.",
            "recorded_at": "2026-10-07T10:19:09.955094+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6045434332,
            "contract": {
                "issue": 31,
                "plan_comment": 6045434332,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "05e7a6d54163067d7b0a02f1e3d61e995482e617233b0d973a41940add09d85f",
            },
            "source": "Direct coordinating conversation: maintainer explicitly approved all things/decisions/plans/actions necessary through a finished private coauthor-review manuscript and directed this override to persist in repository memory (2026-10-03). This concrete append-only S amendment repairs a deterministically reproduced strict-expiry bug and reconciles only exact prospective authority/history; inherited tests, limits and zero reviewer paid-extra/API usage remain. Standing receipt memory/FLOW-DC-standing-authorization.md; exact full verified plan6045434332. Operator assertion of existing authorization, not new advisor/coauthor approval or GitHub human review.",
            "recorded_at": "2026-10-07T19:42:04.886852+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
    ],
}


class Generation14Tests(unittest.TestCase):
    def setUp(self):
        QualificationTests.setUp(self)
        self.state = copy.deepcopy(G14_STATE)
        self.write_state(self.state)

    write_state = QualificationTests.write_state

    def test_literal_history_storage_and_current_gate_refusals(self):
        import review_batch_windows_v1 as windows

        current = activation.authorization(self.repo)
        self.assertEqual(
            current["contract_digest"], "dbe1725734df4a5392ebcdbe7ef6f898c722240c939ebef88578078ce25e40a8"
        )
        self.assertEqual(
            current["approval_digest"], "4e6725fa686b93c38afcc3eec46c5ff5583cc28f319105b9eefb77051dea40bc"
        )
        self.assertEqual(activation.selected_contract(self.repo), self.state["approval"]["contract"])
        activation._g14_history(self.state)
        activation._binding(self.binding)
        self.binding["authorization"] = current
        activation._binding(self.binding)
        with self.assertRaises(WorkflowError):
            windows.full_checks_g14(self.repo, self.repo.main, {"plan_comment": 6045434332})
        with patch.object(windows, "_full_checks", side_effect=WorkflowError("receipt boundary")):
            with self.assertRaisesRegex(WorkflowError, "receipt boundary"):
                windows.full_checks_g14(self.repo, self.repo.main, {"plan_comment": 6061320190})
        self.write_state(copy.deepcopy(G13_STATE))
        self.assertEqual(activation.selected_contract(self.repo), activation.NEXT_CONTRACT)
        with self.assertRaises(WorkflowError):
            windows.full_checks_g14(self.repo, self.repo.main, {"plan_comment": 6061320190})

    def test_all_current_authority_mutations_refuse(self):
        mutations = [
            lambda s: s.pop("contract_generation"),
            lambda s: s.update(contract_generation=True),
            lambda s: s.update(contract_generation=15),
            lambda s: s.pop("approval"),
            lambda s: s["approval"].update(plan_comment=True),
            lambda s: s["approval"]["contract"].update(plan_digest="a" * 64),
            lambda s: s.update(approval=copy.deepcopy(s["approval_history"][-1])),
            lambda s: s["approval_history"].reverse(),
            lambda s: s["approval_history"].pop(),
            lambda s: s["approval_history"].append(copy.deepcopy(s["approval_history"][-1])),
            lambda s: s["approval_history"][0].update(source="copied"),
        ]
        for mutate in mutations:
            state = copy.deepcopy(self.state)
            mutate(state)
            self.write_state(state)
            with self.assertRaises(WorkflowError):
                activation.authorization(self.repo)


class Generation14OwnedRefusalTests(unittest.TestCase):
    response = OwnedCaptureTests.response
    setUp = NextOwnedAuthorityTests.setUp

    def test_old_grant_cannot_satisfy_current_owned_admission(self):
        import claude_owned_auth
        import reporting_admission_v6 as admission

        self.task.write_text(json.dumps(G14_STATE))
        legacy, _ = activation.load(self.repo)
        with claude_owned_auth.snapshot(self.policy) as owned:
            current = activation.context(self.repo, self.policy, owned_auth=owned)
            self.assertEqual(current["authorization"]["contract_digest"], activation.G14_CONTRACT_DIGEST)
            self.assertNotEqual(legacy["binding"], current)
            with self.assertRaisesRegex(WorkflowError, "current source, authority"):
                admission.check(self.repo, owned_auth=owned)
        with self.assertRaises(WorkflowError):
            self.diagnostic.run(self.repo, number=20)
        self.assertEqual(self.calls, 0)


G15_STATE = {
    "key": "issue-31",
    "repository": "Zi-Deng/FLOW-DC",
    "contract_generation": 15,
    "approval": {
        "issue": 31,
        "plan_comment": 6062530466,
        "contract": {
            "issue": 31,
            "plan_comment": 6062530466,
            "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
            "plan_digest": "915e46d413f2d19b0ee2b69ce6ef903fa787d777ebad442d4236781bb2120436",
        },
        "source": "Operator reconciliation October8,2026 under the "
        "maintainer's explicit standing override: \"I approve "
        "of all things/decisions/plans/actions needed for us "
        "to get the finished manuscript that is ready for "
        "co-author review, please remember this for future "
        "decisions/approval, this is an explicit overrite "
        "(note it in this repositories' memory)\" and "
        'subsequent instruction "Please increase the '
        "operational limit to 30 minutes for this particular "
        'case then continue the task/plan". Standing receipt '
        "memory/FLOW-DC-standing-authorization.md; exact "
        "U6062530466 is the necessary bounded portable "
        "worker-import repair after retained Phase69 "
        "completed972cases in1323.075749seconds "
        "with971success/oneerror. Public U designates its "
        "closed source/test/gate/authority/material scope; "
        "preserve all inherited tests/history, source-bound "
        "readiness and "
        "software1800/CI45/live840/native/MaxextraAPI0 limits. "
        "This is an operator receipt of existing user "
        "authorization, not advisor/coauthor approval or human "
        "GitHub approval, and applies no native grant.",
        "recorded_at": "2026-10-08T14:51:57.337830+00:00",
        "note": "Operator assertion of prior human authorization; not public approval proof.",
    },
    "approval_history": [
        {
            "issue": 31,
            "plan_comment": 5900844013,
            "contract": {
                "issue": 31,
                "plan_comment": 5900844013,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "85a4947faefeb29410ed250b29fbf82c3e5dd1c95d2106dbaa928e0c9bc883d7",
            },
            "source": "Maintainer explicitly requested "
            "implementation of the supplied twelve-step "
            "roadmap on September 29, 2026 and repeated "
            "that request after pausing; this comment "
            "maps its step 2 to existing source and tests "
            "without expanding scope. Paid live batch "
            "remains separately unapproved.",
            "recorded_at": "2026-09-29T23:12:35.136186+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 5966428269,
            "contract": {
                "issue": 31,
                "plan_comment": 5966428269,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "b170fe0c80b101de1568b96b5b07ac6f4418778ca585b53b5f6c755105511b7a",
            },
            "source": "User explicitly authorized all necessary "
            "decisions/plans/actions to complete the "
            "coauthor-review manuscript, requested this "
            "override be retained in repository memory, "
            "and confirmed PR34 merged; applied to the "
            "concrete issue31 provider-aware amendment "
            "under "
            "memory/FLOW-DC-standing-authorization.md. "
            "This is an operator receipt of standing "
            "authorization, not an advisor/coauthor "
            "decision or GitHub approval.",
            "recorded_at": "2026-10-03T06:38:52.487404+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6001819615,
            "contract": {
                "issue": 31,
                "plan_comment": 6001819615,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "2163be1844b7ef658d23276ce425ea8797ffa90bf776c6318109423288865e57",
            },
            "source": "Direct maintainer standing override "
            "authorizes all necessary "
            "decisions/plans/actions through private "
            "coauthor review, retained in FLOW-DC memory. "
            "Coordinator bound prospective "
            "reporting/test/continuation "
            "amendment6001819615 and two new separately "
            "capped activation purposes10/11; old grants, "
            "calls, reports and human merge duties "
            "preserved. No approval inferred from public "
            "issue text.",
            "recorded_at": "2026-10-05T19:49:38.315425+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6008093895,
            "contract": {
                "issue": 31,
                "plan_comment": 6008093895,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "e793c7623749aba94ae69fd623d7b406b4ec40227a3f71f271487a5019724734",
            },
            "source": "Maintainer explicitly authorized all "
            "necessary decisions/plans/actions and paid "
            "processes through the finished private "
            "coauthor-review manuscript, requested the "
            "override retained in repository memory, and "
            "now requested continuation after upgrading "
            "usage. Applied prospectively to the concrete "
            "finite recovery amendment6008093895: only "
            "new "
            "purposes12then13,300s/$2referenceeach600/$4total,Maxincludedonly/extraAPI0,no14,alloldreservations/stops/historypreserved. "
            "This supersedes task-level approval prompts; "
            "it does not attest diagnostics, provider "
            "usage, advisor/coauthor approval, merge or "
            "scientific readiness. Source: "
            "memory/FLOW-DC-standing-authorization.md and "
            "direct coordinating conversation.",
            "recorded_at": "2026-10-06T02:28:48.397470+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6009076812,
            "contract": {
                "issue": 31,
                "plan_comment": 6009076812,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "88af4787bfab768b81ff05ea7b09ab20bdefbb6e87572666b38c8b890ce1d2d0",
            },
            "source": "Direct maintainer standing override "
            "recorded2026-10-03T01:36:38.550153UTC: all "
            "necessary decisions/plans/actions and paid "
            "processes through private coauthor review. "
            "Applied prospectively to this exact "
            "original-Astra-authored v3 plan, "
            "published6009076812: observations "
            "only;14isolation-first "
            "then15tools/source;2wrappers,300s/$2reference "
            "each,600s/$4total,Maxincluded/extraAPI0,failure-stop/no16. "
            "Preserve all historical records, existing "
            "evidence/review gates and human "
            "login/merge/submission. This is an operator "
            "provenance receipt, not a fabricated human "
            "GitHub approval.",
            "recorded_at": "2026-10-06T04:03:24.534151+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6009865197,
            "contract": {
                "issue": 31,
                "plan_comment": 6009865197,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "6493f379fdb2ed9ea1580a38d2fcdc1d618608f42b30613b145faea69323f484",
            },
            "source": "Direct user standing override recorded "
            '2026-10-03: "I approve of all '
            "things/decisions/plans/actions needed for us "
            "to get the finished manuscript that is ready "
            "for co-author review ... this is an explicit "
            'overrite"; reinforced by subsequent requests '
            "to continue and authorize necessary paid "
            "processes. Applied prospectively to the "
            "complete source-verified v8/recovery-v4 "
            "amendment6009865197 after actual15 "
            "identified estimated_tokens shapes. Concrete "
            "finite limits and all evidence/human "
            "boundaries retained. Local operator receipt, "
            "not inferred GitHub approval or new "
            "credential/billing authority.",
            "recorded_at": "2026-10-06T05:19:19.280275+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6010775261,
            "contract": {
                "issue": 31,
                "plan_comment": 6010775261,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "e87de935601b8bbb50f7fa4188ddeb2bac4e1782479aeed00e2185f9dfc1cf05",
            },
            "source": "Direct coordinating user explicit standing "
            "override: approve all necessary "
            "things/decisions/plans/actions and paid "
            "continuations through private coauthor "
            "review, recorded "
            "memory/FLOW-DC-standing-authorization.md "
            "(2026-10-03). Applies prospectively to this "
            "exact published finite recovery-v5 contract "
            "only, preserving extra/API0, all evidence "
            "gates and human login/merge/submission "
            "boundaries. Local operator receipt, not "
            "fabricated human GitHub approval. Current "
            "hosted workflow must pass before source "
            "mutation or paid trial.",
            "recorded_at": "2026-10-06T06:33:27.488866+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6011162252,
            "contract": {
                "issue": 31,
                "plan_comment": 6011162252,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "f3d7d5d6656a3397d4c11a759ef543fcbb94c8fa49e83795117f8e922799cde4",
            },
            "source": "Direct coordinating user standing explicit "
            "override recorded "
            "memory/FLOW-DC-standing-authorization.md: "
            "approves necessary actions and finite paid "
            "continuations through private coauthor "
            "review. Applied prospectively to this exact "
            "narrow CI-runtime amendment only: permits "
            "runner repair before hosted success to "
            "resolve two cancellations, preserving all "
            "tests, CI15min, historical evidence and "
            "human boundaries. Native-v5 "
            "implementation/diagnostics remain gated on "
            "successful repaired-head software receipts "
            "and separate prospective contract "
            "reconciliation. Local operator receipt, not "
            "human GitHub approval.",
            "recorded_at": "2026-10-06T07:02:43.864501+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6012492318,
            "contract": {
                "issue": 31,
                "plan_comment": 6012492318,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "c0dbb09a3b7a03176988263aee50059c905af6750ef42de8042ff386e65b8461",
            },
            "source": "Direct coordinating user standing explicit "
            "override in "
            "memory/FLOW-DC-standing-authorization.md "
            "approves necessary concrete actions and "
            "finite paid continuations through private "
            "coauthor review. Applied prospectively to "
            "this exact original-Astra-authored "
            "recovery-v5 reconciliation at4fbe2af after "
            "verified full local/installed/hosted gates: "
            "two separate single-use "
            "diagnostics18isolation-first and "
            "conditional19tools/source,300seconds/$2reference "
            "each,600seconds/$4total,Maxincluded/extraAPI0,failure-stop/no20. "
            "No repeated task approval is pending; exact "
            "final-head software gates and fresh actual "
            "prerequisites still precede separate "
            "preview/application/invokes. Full "
            "component+integration review requires a "
            "distinct populated finite grant. Historical "
            "approvals/evidence and human "
            "login/merge/submission boundaries remain. "
            "Local operator receipt, not human GitHub "
            "approval.",
            "recorded_at": "2026-10-06T08:33:16.223093+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6013795098,
            "contract": {
                "issue": 31,
                "plan_comment": 6013795098,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "8351ce7ce762488c0f2447332487565c1c216e1f24ac440b237dc4d275277130",
            },
            "source": "Direct coordinating user standing explicit "
            "override in "
            "memory/FLOW-DC-standing-authorization.md "
            "authorizes necessary concrete actions "
            "through private coauthor review. Applied "
            "prospectively to this exact "
            "original-Astra-authored 58960-byte "
            "fixture-group scheduling plan at44eeac9. "
            "Only check_runner.py, additive "
            "test_check_runner.py and SETUP.md change; "
            "preserve803 occurrences and old methods, "
            "protocol2, two workers,840second "
            "phase,900second CI and all historical "
            "findings/evidence. Require exact final "
            "serial/parallel, installed affected suites, "
            "coordinator full local and both "
            "first-attempt hosted gates. No native "
            "grant/trial funded or authority constant "
            "change; separate prospective native binding "
            "reconciliation remains required. Retain "
            "generation9 and prior approvals once and "
            "original executor UUID. Human login, merge "
            "and submission boundaries remain. Local "
            "operator receipt, not human GitHub approval.",
            "recorded_at": "2026-10-06T09:55:50.305381+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6014789492,
            "contract": {
                "issue": 31,
                "plan_comment": 6014789492,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "e2fe530795df6701f43de54bb869032cd5375e92c9ecf543fa4e5a7d7830fb84",
            },
            "source": "Direct coordinating user standing explicit "
            "override in "
            "memory/FLOW-DC-standing-authorization.md "
            "authorizes necessary concrete actions "
            "through private coauthor review. Applied "
            "prospectively to this exact "
            "original-Astra-authored whole native-v5 "
            "authority reconciliation at8bb1abb. Only "
            "reporting_activation_v5.py exact next "
            "contract literals and duplicated plan check, "
            "new standalone "
            "test_reporting_authority_v5.py, and "
            "PROVIDERS current-authority guidance change. "
            "Preserve all810 occurrences and existing "
            "methods,56frozen paths,83009history, fixed "
            "runner seed/two workers/840second "
            "phase/900second CI and every historical "
            "finding/obligation. Require behavioral base "
            "regression, full "
            "affected/serial/parallel/disposable "
            "installed, coordinator full local and both "
            "first-attempt final-head hosted gates. "
            "Separately prepare/review/apply the existing "
            "finite18/conditional19 pair only after final "
            "gates/fresh "
            "prerequisites:two300second/$2reference "
            "wrappers,600seconds/$4total,Maxincluded,paid-extra/API0,failure-stop/no20. "
            "This receipt itself applies no grant and "
            "funds no full component/integration review. "
            "Retain generations6-10 exactly once, same "
            "executor UUID. Human login, merge and "
            "submission boundaries remain. Local operator "
            "receipt, not human GitHub approval.",
            "recorded_at": "2026-10-06T10:58:57.843959+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6035844223,
            "contract": {
                "issue": 31,
                "plan_comment": 6035844223,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "494abcda1fa95d346ae5f8a84b638202701e4756c3ceb24fc4b4f0a9f9d114e5",
            },
            "source": "Maintainer explicit standing override "
            "recorded October3 in "
            "memory/FLOW-DC-standing-authorization.md "
            "authorizes all necessary decisions/actions "
            "through the private coauthor-review "
            "manuscript. Applying it to this exact "
            "reconciled Q1-Q7 no-Console engineering and "
            "finite qualification contract, verified "
            "whole-body publication6035844223 and "
            "proposal "
            "SHAe9dcbd25045504eb0f9bddcec2614e93507bc4b53c4c74ffe73b292d301986f3. "
            "Included Max only, extra/API0; human "
            "login/merge/submission boundaries preserved. "
            "No live grant or scientific/advisor/coauthor "
            "approval is implied.",
            "recorded_at": "2026-10-07T10:19:09.955094+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6045434332,
            "contract": {
                "issue": 31,
                "plan_comment": 6045434332,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "05e7a6d54163067d7b0a02f1e3d61e995482e617233b0d973a41940add09d85f",
            },
            "source": "Direct coordinating conversation: maintainer "
            "explicitly approved all "
            "things/decisions/plans/actions necessary "
            "through a finished private coauthor-review "
            "manuscript and directed this override to "
            "persist in repository memory (2026-10-03). "
            "This concrete append-only S amendment "
            "repairs a deterministically reproduced "
            "strict-expiry bug and reconciles only exact "
            "prospective authority/history; inherited "
            "tests, limits and zero reviewer "
            "paid-extra/API usage remain. Standing "
            "receipt "
            "memory/FLOW-DC-standing-authorization.md; "
            "exact full verified plan6045434332. Operator "
            "assertion of existing authorization, not new "
            "advisor/coauthor approval or GitHub human "
            "review.",
            "recorded_at": "2026-10-07T19:42:04.886852+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6061320190,
            "contract": {
                "issue": 31,
                "plan_comment": 6061320190,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "22712ec0c4661047adc4b00960ca9f2dd8d162cbc243b658f7ce23a8d63fac21",
            },
            "source": "Direct coordinating conversation "
            'October8,2026: maintainer said "Please '
            "increase the operational limit to30 minutes "
            "for this particular case then continue the "
            'task/plan", accepting the '
            "proposed1800-second complete-suite/45-minute "
            "enclosing CI recovery while preserving "
            "live840/native/credential/billing limits. "
            "Existing standing override authorizes all "
            "necessary concrete actions through private "
            "coauthor-review preparation. This exact T "
            "amendment6061320190 scopes only explicit "
            "suite opt-in, accountable new execution "
            "records, current authority/history and "
            "complete inherited public-contract material. "
            "Old tests/fixtures/history/limits remain. "
            "Authorization record "
            "memory/manuscript-2026-10/decisions/suite-timeout-1800-authorization-2026-10-08.json. "
            "Operator receipt of actual authorization; "
            "not advisor/coauthor/GitHub approval or a "
            "new native grant.",
            "recorded_at": "2026-10-08T13:49:39.161057+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
    ],
}


class Generation15Tests(unittest.TestCase):
    def setUp(self):
        QualificationTests.setUp(self)
        self.state = copy.deepcopy(G15_STATE)
        self.write_state(self.state)

    write_state = QualificationTests.write_state

    def test_literal_history_storage_and_current_gate_refusals(self):
        import review_batch_windows_v1 as windows

        current = activation.authorization(self.repo)
        self.assertEqual(
            current["contract_digest"], "729bca73b3fcc64ad044e7dd876657436c8cb614df871876fc855fbff2e93105"
        )
        self.assertEqual(
            current["approval_digest"], "248965ba97be950afe274bb9f76780f2811837030420b1cd9968370d9b1d36f5"
        )
        self.assertEqual(activation.selected_contract(self.repo), self.state["approval"]["contract"])
        activation._g15_history(self.state)
        activation._binding(self.binding)
        self.binding["authorization"] = current
        activation._binding(self.binding)
        with self.assertRaises(WorkflowError):
            windows.full_checks_g15(self.repo, self.repo.main, {"plan_comment": 6061320190})
        with patch.object(windows, "_full_checks", side_effect=WorkflowError("receipt boundary")):
            with self.assertRaisesRegex(WorkflowError, "receipt boundary"):
                windows.full_checks_g15(self.repo, self.repo.main, {"plan_comment": 6062530466})
        self.write_state(copy.deepcopy(G14_STATE))
        self.assertEqual(activation.selected_contract(self.repo), activation.G14_CONTRACT)
        with self.assertRaises(WorkflowError):
            windows.full_checks_g15(self.repo, self.repo.main, {"plan_comment": 6062530466})

    def test_all_current_authority_mutations_refuse(self):
        mutations = [
            lambda s: s.pop("contract_generation"),
            lambda s: s.update(contract_generation=True),
            lambda s: s.update(contract_generation=16),
            lambda s: s.pop("approval"),
            lambda s: s["approval"].update(plan_comment=True),
            lambda s: s["approval"]["contract"].update(plan_digest="a" * 64),
            lambda s: s.update(approval=copy.deepcopy(s["approval_history"][-1])),
            lambda s: s["approval_history"].reverse(),
            lambda s: s["approval_history"].pop(),
            lambda s: s["approval_history"].append(copy.deepcopy(s["approval_history"][-1])),
            lambda s: s["approval_history"][0].update(source="copied"),
        ]
        for mutate in mutations:
            state = copy.deepcopy(self.state)
            mutate(state)
            self.write_state(state)
            with self.assertRaises(WorkflowError):
                activation.authorization(self.repo)


class Generation15OwnedRefusalTests(unittest.TestCase):
    response = OwnedCaptureTests.response
    setUp = NextOwnedAuthorityTests.setUp

    def test_old_grant_cannot_satisfy_current_owned_admission(self):
        import claude_owned_auth
        import reporting_admission_v6 as admission

        self.task.write_text(json.dumps(G15_STATE))
        legacy, _ = activation.load(self.repo)
        with claude_owned_auth.snapshot(self.policy) as owned:
            current = activation.context(self.repo, self.policy, owned_auth=owned)
            self.assertEqual(current["authorization"]["contract_digest"], activation.G15_CONTRACT_DIGEST)
            self.assertNotEqual(legacy["binding"], current)
            with self.assertRaisesRegex(WorkflowError, "current source, authority"):
                admission.check(self.repo, owned_auth=owned)
        with self.assertRaises(WorkflowError):
            self.diagnostic.run(self.repo, number=20)
        self.assertEqual(self.calls, 0)


G16_STATE = {
    "repository": "Zi-Deng/FLOW-DC",
    "key": "issue-31",
    "contract_generation": 16,
    "approval": {
        "issue": 31,
        "plan_comment": 6064513854,
        "contract": {
            "issue": 31,
            "plan_comment": 6064513854,
            "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
            "plan_digest": "63e7370374cab273b55c0188f92ec84d41b37ea6c0e43bf00da710a544e28731",
        },
        "source": "Operator reconciliation October8,2026 under the maintainer's "
        "explicit standing override approving all necessary "
        "decisions/plans/actions through the private coauthor-review "
        "manuscript and subsequent instruction to continue under the30-minute "
        "software limit. Receipt memory/FLOW-DC-standing-authorization.md. "
        "Exact V6064513854 prospectively reconciles the documented "
        "installer/adopter qualification mismatch after retained Phase75 "
        "finished979cases with977success/2failures. New installed-adoption-v1 "
        "preserves immutable fullpristinepayload+origin and "
        "everyoldtest/installer; separatelybinds byte-identical "
        "installedexecutionroot plus exactly2pinnedprojectintegrationfiles "
        "and real isolated deterministic Git provenance. "
        "Sourcehead/fixturecommit/hostedcheckout staydistinct, "
        "newclosedreader/schema/currentauthority/predecessors and finite "
        "focused/separatefullgates explicit. All "
        "software1800/CI45/live840/native/MaxextraAPI0 limits stayunchanged. "
        "Operator receipt of existing user authorization, not "
        "namedadvisor/coauthor approval or humanGitHub approval; applies no "
        "native grant.",
        "recorded_at": "2026-10-08T16:36:43.256974+00:00",
        "note": "Operator assertion of prior human authorization; not public approval proof.",
    },
    "approval_history": [
        {
            "issue": 31,
            "plan_comment": 5900844013,
            "contract": {
                "issue": 31,
                "plan_comment": 5900844013,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "85a4947faefeb29410ed250b29fbf82c3e5dd1c95d2106dbaa928e0c9bc883d7",
            },
            "source": "Maintainer explicitly requested implementation of the "
            "supplied twelve-step roadmap on September 29, 2026 and "
            "repeated that request after pausing; this comment maps its "
            "step 2 to existing source and tests without expanding "
            "scope. Paid live batch remains separately unapproved.",
            "recorded_at": "2026-09-29T23:12:35.136186+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 5966428269,
            "contract": {
                "issue": 31,
                "plan_comment": 5966428269,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "b170fe0c80b101de1568b96b5b07ac6f4418778ca585b53b5f6c755105511b7a",
            },
            "source": "User explicitly authorized all necessary "
            "decisions/plans/actions to complete the coauthor-review "
            "manuscript, requested this override be retained in "
            "repository memory, and confirmed PR34 merged; applied to "
            "the concrete issue31 provider-aware amendment under "
            "memory/FLOW-DC-standing-authorization.md. This is an "
            "operator receipt of standing authorization, not an "
            "advisor/coauthor decision or GitHub approval.",
            "recorded_at": "2026-10-03T06:38:52.487404+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6001819615,
            "contract": {
                "issue": 31,
                "plan_comment": 6001819615,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "2163be1844b7ef658d23276ce425ea8797ffa90bf776c6318109423288865e57",
            },
            "source": "Direct maintainer standing override authorizes all "
            "necessary decisions/plans/actions through private coauthor "
            "review, retained in FLOW-DC memory. Coordinator bound "
            "prospective reporting/test/continuation amendment6001819615 "
            "and two new separately capped activation purposes10/11; old "
            "grants, calls, reports and human merge duties preserved. No "
            "approval inferred from public issue text.",
            "recorded_at": "2026-10-05T19:49:38.315425+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6008093895,
            "contract": {
                "issue": 31,
                "plan_comment": 6008093895,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "e793c7623749aba94ae69fd623d7b406b4ec40227a3f71f271487a5019724734",
            },
            "source": "Maintainer explicitly authorized all necessary "
            "decisions/plans/actions and paid processes through the "
            "finished private coauthor-review manuscript, requested the "
            "override retained in repository memory, and now requested "
            "continuation after upgrading usage. Applied prospectively "
            "to the concrete finite recovery amendment6008093895: only "
            "new "
            "purposes12then13,300s/$2referenceeach600/$4total,Maxincludedonly/extraAPI0,no14,alloldreservations/stops/historypreserved. "
            "This supersedes task-level approval prompts; it does not "
            "attest diagnostics, provider usage, advisor/coauthor "
            "approval, merge or scientific readiness. Source: "
            "memory/FLOW-DC-standing-authorization.md and direct "
            "coordinating conversation.",
            "recorded_at": "2026-10-06T02:28:48.397470+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6009076812,
            "contract": {
                "issue": 31,
                "plan_comment": 6009076812,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "88af4787bfab768b81ff05ea7b09ab20bdefbb6e87572666b38c8b890ce1d2d0",
            },
            "source": "Direct maintainer standing override "
            "recorded2026-10-03T01:36:38.550153UTC: all necessary "
            "decisions/plans/actions and paid processes through private "
            "coauthor review. Applied prospectively to this exact "
            "original-Astra-authored v3 plan, published6009076812: "
            "observations only;14isolation-first "
            "then15tools/source;2wrappers,300s/$2reference "
            "each,600s/$4total,Maxincluded/extraAPI0,failure-stop/no16. "
            "Preserve all historical records, existing evidence/review "
            "gates and human login/merge/submission. This is an operator "
            "provenance receipt, not a fabricated human GitHub approval.",
            "recorded_at": "2026-10-06T04:03:24.534151+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6009865197,
            "contract": {
                "issue": 31,
                "plan_comment": 6009865197,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "6493f379fdb2ed9ea1580a38d2fcdc1d618608f42b30613b145faea69323f484",
            },
            "source": 'Direct user standing override recorded 2026-10-03: "I '
            "approve of all things/decisions/plans/actions needed for us "
            "to get the finished manuscript that is ready for co-author "
            'review ... this is an explicit overrite"; reinforced by '
            "subsequent requests to continue and authorize necessary "
            "paid processes. Applied prospectively to the complete "
            "source-verified v8/recovery-v4 amendment6009865197 after "
            "actual15 identified estimated_tokens shapes. Concrete "
            "finite limits and all evidence/human boundaries retained. "
            "Local operator receipt, not inferred GitHub approval or new "
            "credential/billing authority.",
            "recorded_at": "2026-10-06T05:19:19.280275+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6010775261,
            "contract": {
                "issue": 31,
                "plan_comment": 6010775261,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "e87de935601b8bbb50f7fa4188ddeb2bac4e1782479aeed00e2185f9dfc1cf05",
            },
            "source": "Direct coordinating user explicit standing override: "
            "approve all necessary things/decisions/plans/actions and "
            "paid continuations through private coauthor review, "
            "recorded memory/FLOW-DC-standing-authorization.md "
            "(2026-10-03). Applies prospectively to this exact published "
            "finite recovery-v5 contract only, preserving extra/API0, "
            "all evidence gates and human login/merge/submission "
            "boundaries. Local operator receipt, not fabricated human "
            "GitHub approval. Current hosted workflow must pass before "
            "source mutation or paid trial.",
            "recorded_at": "2026-10-06T06:33:27.488866+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6011162252,
            "contract": {
                "issue": 31,
                "plan_comment": 6011162252,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "f3d7d5d6656a3397d4c11a759ef543fcbb94c8fa49e83795117f8e922799cde4",
            },
            "source": "Direct coordinating user standing explicit override "
            "recorded memory/FLOW-DC-standing-authorization.md: approves "
            "necessary actions and finite paid continuations through "
            "private coauthor review. Applied prospectively to this "
            "exact narrow CI-runtime amendment only: permits runner "
            "repair before hosted success to resolve two cancellations, "
            "preserving all tests, CI15min, historical evidence and "
            "human boundaries. Native-v5 implementation/diagnostics "
            "remain gated on successful repaired-head software receipts "
            "and separate prospective contract reconciliation. Local "
            "operator receipt, not human GitHub approval.",
            "recorded_at": "2026-10-06T07:02:43.864501+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6012492318,
            "contract": {
                "issue": 31,
                "plan_comment": 6012492318,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "c0dbb09a3b7a03176988263aee50059c905af6750ef42de8042ff386e65b8461",
            },
            "source": "Direct coordinating user standing explicit override in "
            "memory/FLOW-DC-standing-authorization.md approves necessary "
            "concrete actions and finite paid continuations through "
            "private coauthor review. Applied prospectively to this "
            "exact original-Astra-authored recovery-v5 reconciliation "
            "at4fbe2af after verified full local/installed/hosted gates: "
            "two separate single-use diagnostics18isolation-first and "
            "conditional19tools/source,300seconds/$2reference "
            "each,600seconds/$4total,Maxincluded/extraAPI0,failure-stop/no20. "
            "No repeated task approval is pending; exact final-head "
            "software gates and fresh actual prerequisites still precede "
            "separate preview/application/invokes. Full "
            "component+integration review requires a distinct populated "
            "finite grant. Historical approvals/evidence and human "
            "login/merge/submission boundaries remain. Local operator "
            "receipt, not human GitHub approval.",
            "recorded_at": "2026-10-06T08:33:16.223093+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6013795098,
            "contract": {
                "issue": 31,
                "plan_comment": 6013795098,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "8351ce7ce762488c0f2447332487565c1c216e1f24ac440b237dc4d275277130",
            },
            "source": "Direct coordinating user standing explicit override in "
            "memory/FLOW-DC-standing-authorization.md authorizes "
            "necessary concrete actions through private coauthor review. "
            "Applied prospectively to this exact original-Astra-authored "
            "58960-byte fixture-group scheduling plan at44eeac9. Only "
            "check_runner.py, additive test_check_runner.py and SETUP.md "
            "change; preserve803 occurrences and old methods, protocol2, "
            "two workers,840second phase,900second CI and all historical "
            "findings/evidence. Require exact final serial/parallel, "
            "installed affected suites, coordinator full local and both "
            "first-attempt hosted gates. No native grant/trial funded or "
            "authority constant change; separate prospective native "
            "binding reconciliation remains required. Retain generation9 "
            "and prior approvals once and original executor UUID. Human "
            "login, merge and submission boundaries remain. Local "
            "operator receipt, not human GitHub approval.",
            "recorded_at": "2026-10-06T09:55:50.305381+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6014789492,
            "contract": {
                "issue": 31,
                "plan_comment": 6014789492,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "e2fe530795df6701f43de54bb869032cd5375e92c9ecf543fa4e5a7d7830fb84",
            },
            "source": "Direct coordinating user standing explicit override in "
            "memory/FLOW-DC-standing-authorization.md authorizes "
            "necessary concrete actions through private coauthor review. "
            "Applied prospectively to this exact original-Astra-authored "
            "whole native-v5 authority reconciliation at8bb1abb. Only "
            "reporting_activation_v5.py exact next contract literals and "
            "duplicated plan check, new standalone "
            "test_reporting_authority_v5.py, and PROVIDERS "
            "current-authority guidance change. Preserve all810 "
            "occurrences and existing methods,56frozen "
            "paths,83009history, fixed runner seed/two workers/840second "
            "phase/900second CI and every historical finding/obligation. "
            "Require behavioral base regression, full "
            "affected/serial/parallel/disposable installed, coordinator "
            "full local and both first-attempt final-head hosted gates. "
            "Separately prepare/review/apply the existing "
            "finite18/conditional19 pair only after final gates/fresh "
            "prerequisites:two300second/$2reference "
            "wrappers,600seconds/$4total,Maxincluded,paid-extra/API0,failure-stop/no20. "
            "This receipt itself applies no grant and funds no full "
            "component/integration review. Retain generations6-10 "
            "exactly once, same executor UUID. Human login, merge and "
            "submission boundaries remain. Local operator receipt, not "
            "human GitHub approval.",
            "recorded_at": "2026-10-06T10:58:57.843959+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6035844223,
            "contract": {
                "issue": 31,
                "plan_comment": 6035844223,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "494abcda1fa95d346ae5f8a84b638202701e4756c3ceb24fc4b4f0a9f9d114e5",
            },
            "source": "Maintainer explicit standing override recorded October3 in "
            "memory/FLOW-DC-standing-authorization.md authorizes all "
            "necessary decisions/actions through the private "
            "coauthor-review manuscript. Applying it to this exact "
            "reconciled Q1-Q7 no-Console engineering and finite "
            "qualification contract, verified whole-body "
            "publication6035844223 and proposal "
            "SHAe9dcbd25045504eb0f9bddcec2614e93507bc4b53c4c74ffe73b292d301986f3. "
            "Included Max only, extra/API0; human login/merge/submission "
            "boundaries preserved. No live grant or "
            "scientific/advisor/coauthor approval is implied.",
            "recorded_at": "2026-10-07T10:19:09.955094+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6045434332,
            "contract": {
                "issue": 31,
                "plan_comment": 6045434332,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "05e7a6d54163067d7b0a02f1e3d61e995482e617233b0d973a41940add09d85f",
            },
            "source": "Direct coordinating conversation: maintainer explicitly "
            "approved all things/decisions/plans/actions necessary "
            "through a finished private coauthor-review manuscript and "
            "directed this override to persist in repository memory "
            "(2026-10-03). This concrete append-only S amendment repairs "
            "a deterministically reproduced strict-expiry bug and "
            "reconciles only exact prospective authority/history; "
            "inherited tests, limits and zero reviewer paid-extra/API "
            "usage remain. Standing receipt "
            "memory/FLOW-DC-standing-authorization.md; exact full "
            "verified plan6045434332. Operator assertion of existing "
            "authorization, not new advisor/coauthor approval or GitHub "
            "human review.",
            "recorded_at": "2026-10-07T19:42:04.886852+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6061320190,
            "contract": {
                "issue": 31,
                "plan_comment": 6061320190,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "22712ec0c4661047adc4b00960ca9f2dd8d162cbc243b658f7ce23a8d63fac21",
            },
            "source": "Direct coordinating conversation October8,2026: maintainer "
            'said "Please increase the operational limit to30 minutes '
            'for this particular case then continue the task/plan", '
            "accepting the proposed1800-second complete-suite/45-minute "
            "enclosing CI recovery while preserving "
            "live840/native/credential/billing limits. Existing standing "
            "override authorizes all necessary concrete actions through "
            "private coauthor-review preparation. This exact T "
            "amendment6061320190 scopes only explicit suite opt-in, "
            "accountable new execution records, current "
            "authority/history and complete inherited public-contract "
            "material. Old tests/fixtures/history/limits remain. "
            "Authorization record "
            "memory/manuscript-2026-10/decisions/suite-timeout-1800-authorization-2026-10-08.json. "
            "Operator receipt of actual authorization; not "
            "advisor/coauthor/GitHub approval or a new native grant.",
            "recorded_at": "2026-10-08T13:49:39.161057+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6062530466,
            "contract": {
                "issue": 31,
                "plan_comment": 6062530466,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "915e46d413f2d19b0ee2b69ce6ef903fa787d777ebad442d4236781bb2120436",
            },
            "source": "Operator reconciliation October8,2026 under the "
            "maintainer's explicit standing override: \"I approve of all "
            "things/decisions/plans/actions needed for us to get the "
            "finished manuscript that is ready for co-author review, "
            "please remember this for future decisions/approval, this is "
            "an explicit overrite (note it in this repositories' "
            'memory)" and subsequent instruction "Please increase the '
            "operational limit to 30 minutes for this particular case "
            'then continue the task/plan". Standing receipt '
            "memory/FLOW-DC-standing-authorization.md; exact U6062530466 "
            "is the necessary bounded portable worker-import repair "
            "after retained Phase69 completed972cases "
            "in1323.075749seconds with971success/oneerror. Public U "
            "designates its closed source/test/gate/authority/material "
            "scope; preserve all inherited tests/history, source-bound "
            "readiness and software1800/CI45/live840/native/MaxextraAPI0 "
            "limits. This is an operator receipt of existing user "
            "authorization, not advisor/coauthor approval or human "
            "GitHub approval, and applies no native grant.",
            "recorded_at": "2026-10-08T14:51:57.337830+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
    ],
}


class Generation16Tests(unittest.TestCase):
    def setUp(self):
        QualificationTests.setUp(self)
        self.repo.root = self.repo.main
        self.state = copy.deepcopy(G16_STATE)
        self.write_state(self.state)

    write_state = QualificationTests.write_state

    def test_literal_history_storage_and_current_gate_refusals(self):
        import review_batch_windows_v1 as windows

        current = activation.authorization(self.repo)
        self.assertEqual(
            current["contract_digest"], "bfbdf3dc6142487812e945c1fc91338e8c634a1eda9d5e7b4f50108499391af9"
        )
        self.assertEqual(
            current["approval_digest"], "8aad3237f9f908ce48cef1f9d84ac82fdd669d0a33e26d7bf723a28deebefe79"
        )
        self.assertEqual(activation.selected_contract(self.repo), self.state["approval"]["contract"])
        activation._g16_history(self.state)
        activation._binding(self.binding)
        self.binding["authorization"] = current
        activation._binding(self.binding)
        with self.assertRaises(WorkflowError):
            windows.full_checks_g16(self.repo, self.repo.main, {"plan_comment": 6062530466})
        with patch.object(windows, "_full_checks_g16", side_effect=WorkflowError("receipt boundary")):
            with self.assertRaisesRegex(WorkflowError, "receipt boundary"):
                windows.full_checks_g16(self.repo, self.repo.main, {"plan_comment": 6064513854})
        self.write_state(copy.deepcopy(G15_STATE))
        self.assertEqual(activation.selected_contract(self.repo), activation.G15_CONTRACT)
        with self.assertRaises(WorkflowError):
            windows.full_checks_g16(self.repo, self.repo.main, {"plan_comment": 6062530466})

    def test_all_current_authority_mutations_refuse(self):
        mutations = [
            lambda s: s.pop("contract_generation"),
            lambda s: s.update(contract_generation=True),
            lambda s: s.update(contract_generation=17),
            lambda s: s.pop("approval"),
            lambda s: s["approval"].update(plan_comment=True),
            lambda s: s["approval"]["contract"].update(plan_digest="a" * 64),
            lambda s: s.update(approval=copy.deepcopy(s["approval_history"][-1])),
            lambda s: s["approval_history"].reverse(),
            lambda s: s["approval_history"].pop(),
            lambda s: s["approval_history"].append(copy.deepcopy(s["approval_history"][-1])),
            lambda s: s["approval_history"][0].update(source="copied"),
        ]
        for mutate in mutations:
            state = copy.deepcopy(self.state)
            mutate(state)
            self.write_state(state)
            with self.assertRaises(WorkflowError):
                activation.authorization(self.repo)


class Generation16OwnedRefusalTests(unittest.TestCase):
    response = OwnedCaptureTests.response
    setUp = NextOwnedAuthorityTests.setUp

    def test_old_grant_cannot_satisfy_current_owned_admission(self):
        import claude_owned_auth
        import reporting_admission_v6 as admission

        self.task.write_text(json.dumps(G16_STATE))
        legacy, _ = activation.load(self.repo)
        with claude_owned_auth.snapshot(self.policy) as owned:
            current = activation.context(self.repo, self.policy, owned_auth=owned)
            self.assertEqual(current["authorization"]["contract_digest"], activation.G16_CONTRACT_DIGEST)
            self.assertNotEqual(legacy["binding"], current)
            with self.assertRaisesRegex(WorkflowError, "current source, authority"):
                admission.check(self.repo, owned_auth=owned)
        with self.assertRaises(WorkflowError):
            self.diagnostic.run(self.repo, number=20)
        self.assertEqual(self.calls, 0)


G17_STATE = {
    "repository": "Zi-Deng/FLOW-DC",
    "key": "issue-31",
    "contract_generation": 17,
    "approval": {
        "issue": 31,
        "plan_comment": 6068144159,
        "contract": {
            "issue": 31,
            "plan_comment": 6068144159,
            "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
            "plan_digest": "d53015047613e856093221f416a0811abac71feb40554b80deb38e5f4576aca4",
        },
        "source": "Operator reconciliation October8,2026 under the maintainer's explicit standing override approving all necessary decisions/plans/actions through private coauthor review, stored at memory/FLOW-DC-standing-authorization.md, and latest instruction to continue with30-minute operational software limit. Exact W6068144159 authorizes the necessary narrow bounded CI diagnostic-retention amendment after actual first hosted c2f1680 attempt reached1800seconds, failed, and retained only receipt/no worker journals. Explicit source spans and G17 authority additions preserve all prior constants/readers/history, entire runner/seed/scheduling/discovery/fixtures/receipt/native/auth code and tests. Optional fixed controlled evidence path and exact six-file bounded separate artifact do not count as qualification. Old failed head/run/checkout/failure bytes remain historical. Finite regression/static/serial/parallel/installed/coordinator gates and one new-head first hosted attempt precede read-only diagnosis; no blind retry or promised pass. Suite1800/scopedCI45/live840/native300/900 and extraAPIpaid0 remain unchanged. Actual16history appends G16 once preserving old15prefix and same executor. This records existing explicit user authority, not a namedadvisor/coauthor decision, humanGitHub approval, native grant or manuscript scientific evidence.",
        "recorded_at": "2026-10-08T20:10:24.307689+00:00",
        "note": "Operator assertion of prior human authorization; not public approval proof.",
    },
    "approval_history": [
        {
            "issue": 31,
            "plan_comment": 5900844013,
            "contract": {
                "issue": 31,
                "plan_comment": 5900844013,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "85a4947faefeb29410ed250b29fbf82c3e5dd1c95d2106dbaa928e0c9bc883d7",
            },
            "source": "Maintainer explicitly requested implementation of the supplied twelve-step roadmap on September 29, 2026 and repeated that request after pausing; this comment maps its step 2 to existing source and tests without expanding scope. Paid live batch remains separately unapproved.",
            "recorded_at": "2026-09-29T23:12:35.136186+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 5966428269,
            "contract": {
                "issue": 31,
                "plan_comment": 5966428269,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "b170fe0c80b101de1568b96b5b07ac6f4418778ca585b53b5f6c755105511b7a",
            },
            "source": "User explicitly authorized all necessary decisions/plans/actions to complete the coauthor-review manuscript, requested this override be retained in repository memory, and confirmed PR34 merged; applied to the concrete issue31 provider-aware amendment under memory/FLOW-DC-standing-authorization.md. This is an operator receipt of standing authorization, not an advisor/coauthor decision or GitHub approval.",
            "recorded_at": "2026-10-03T06:38:52.487404+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6001819615,
            "contract": {
                "issue": 31,
                "plan_comment": 6001819615,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "2163be1844b7ef658d23276ce425ea8797ffa90bf776c6318109423288865e57",
            },
            "source": "Direct maintainer standing override authorizes all necessary decisions/plans/actions through private coauthor review, retained in FLOW-DC memory. Coordinator bound prospective reporting/test/continuation amendment6001819615 and two new separately capped activation purposes10/11; old grants, calls, reports and human merge duties preserved. No approval inferred from public issue text.",
            "recorded_at": "2026-10-05T19:49:38.315425+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6008093895,
            "contract": {
                "issue": 31,
                "plan_comment": 6008093895,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "e793c7623749aba94ae69fd623d7b406b4ec40227a3f71f271487a5019724734",
            },
            "source": "Maintainer explicitly authorized all necessary decisions/plans/actions and paid processes through the finished private coauthor-review manuscript, requested the override retained in repository memory, and now requested continuation after upgrading usage. Applied prospectively to the concrete finite recovery amendment6008093895: only new purposes12then13,300s/$2referenceeach600/$4total,Maxincludedonly/extraAPI0,no14,alloldreservations/stops/historypreserved. This supersedes task-level approval prompts; it does not attest diagnostics, provider usage, advisor/coauthor approval, merge or scientific readiness. Source: memory/FLOW-DC-standing-authorization.md and direct coordinating conversation.",
            "recorded_at": "2026-10-06T02:28:48.397470+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6009076812,
            "contract": {
                "issue": 31,
                "plan_comment": 6009076812,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "88af4787bfab768b81ff05ea7b09ab20bdefbb6e87572666b38c8b890ce1d2d0",
            },
            "source": "Direct maintainer standing override recorded2026-10-03T01:36:38.550153UTC: all necessary decisions/plans/actions and paid processes through private coauthor review. Applied prospectively to this exact original-Astra-authored v3 plan, published6009076812: observations only;14isolation-first then15tools/source;2wrappers,300s/$2reference each,600s/$4total,Maxincluded/extraAPI0,failure-stop/no16. Preserve all historical records, existing evidence/review gates and human login/merge/submission. This is an operator provenance receipt, not a fabricated human GitHub approval.",
            "recorded_at": "2026-10-06T04:03:24.534151+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6009865197,
            "contract": {
                "issue": 31,
                "plan_comment": 6009865197,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "6493f379fdb2ed9ea1580a38d2fcdc1d618608f42b30613b145faea69323f484",
            },
            "source": 'Direct user standing override recorded 2026-10-03: "I approve of all things/decisions/plans/actions needed for us to get the finished manuscript that is ready for co-author review ... this is an explicit overrite"; reinforced by subsequent requests to continue and authorize necessary paid processes. Applied prospectively to the complete source-verified v8/recovery-v4 amendment6009865197 after actual15 identified estimated_tokens shapes. Concrete finite limits and all evidence/human boundaries retained. Local operator receipt, not inferred GitHub approval or new credential/billing authority.',
            "recorded_at": "2026-10-06T05:19:19.280275+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6010775261,
            "contract": {
                "issue": 31,
                "plan_comment": 6010775261,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "e87de935601b8bbb50f7fa4188ddeb2bac4e1782479aeed00e2185f9dfc1cf05",
            },
            "source": "Direct coordinating user explicit standing override: approve all necessary things/decisions/plans/actions and paid continuations through private coauthor review, recorded memory/FLOW-DC-standing-authorization.md (2026-10-03). Applies prospectively to this exact published finite recovery-v5 contract only, preserving extra/API0, all evidence gates and human login/merge/submission boundaries. Local operator receipt, not fabricated human GitHub approval. Current hosted workflow must pass before source mutation or paid trial.",
            "recorded_at": "2026-10-06T06:33:27.488866+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6011162252,
            "contract": {
                "issue": 31,
                "plan_comment": 6011162252,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "f3d7d5d6656a3397d4c11a759ef543fcbb94c8fa49e83795117f8e922799cde4",
            },
            "source": "Direct coordinating user standing explicit override recorded memory/FLOW-DC-standing-authorization.md: approves necessary actions and finite paid continuations through private coauthor review. Applied prospectively to this exact narrow CI-runtime amendment only: permits runner repair before hosted success to resolve two cancellations, preserving all tests, CI15min, historical evidence and human boundaries. Native-v5 implementation/diagnostics remain gated on successful repaired-head software receipts and separate prospective contract reconciliation. Local operator receipt, not human GitHub approval.",
            "recorded_at": "2026-10-06T07:02:43.864501+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6012492318,
            "contract": {
                "issue": 31,
                "plan_comment": 6012492318,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "c0dbb09a3b7a03176988263aee50059c905af6750ef42de8042ff386e65b8461",
            },
            "source": "Direct coordinating user standing explicit override in memory/FLOW-DC-standing-authorization.md approves necessary concrete actions and finite paid continuations through private coauthor review. Applied prospectively to this exact original-Astra-authored recovery-v5 reconciliation at4fbe2af after verified full local/installed/hosted gates: two separate single-use diagnostics18isolation-first and conditional19tools/source,300seconds/$2reference each,600seconds/$4total,Maxincluded/extraAPI0,failure-stop/no20. No repeated task approval is pending; exact final-head software gates and fresh actual prerequisites still precede separate preview/application/invokes. Full component+integration review requires a distinct populated finite grant. Historical approvals/evidence and human login/merge/submission boundaries remain. Local operator receipt, not human GitHub approval.",
            "recorded_at": "2026-10-06T08:33:16.223093+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6013795098,
            "contract": {
                "issue": 31,
                "plan_comment": 6013795098,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "8351ce7ce762488c0f2447332487565c1c216e1f24ac440b237dc4d275277130",
            },
            "source": "Direct coordinating user standing explicit override in memory/FLOW-DC-standing-authorization.md authorizes necessary concrete actions through private coauthor review. Applied prospectively to this exact original-Astra-authored 58960-byte fixture-group scheduling plan at44eeac9. Only check_runner.py, additive test_check_runner.py and SETUP.md change; preserve803 occurrences and old methods, protocol2, two workers,840second phase,900second CI and all historical findings/evidence. Require exact final serial/parallel, installed affected suites, coordinator full local and both first-attempt hosted gates. No native grant/trial funded or authority constant change; separate prospective native binding reconciliation remains required. Retain generation9 and prior approvals once and original executor UUID. Human login, merge and submission boundaries remain. Local operator receipt, not human GitHub approval.",
            "recorded_at": "2026-10-06T09:55:50.305381+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6014789492,
            "contract": {
                "issue": 31,
                "plan_comment": 6014789492,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "e2fe530795df6701f43de54bb869032cd5375e92c9ecf543fa4e5a7d7830fb84",
            },
            "source": "Direct coordinating user standing explicit override in memory/FLOW-DC-standing-authorization.md authorizes necessary concrete actions through private coauthor review. Applied prospectively to this exact original-Astra-authored whole native-v5 authority reconciliation at8bb1abb. Only reporting_activation_v5.py exact next contract literals and duplicated plan check, new standalone test_reporting_authority_v5.py, and PROVIDERS current-authority guidance change. Preserve all810 occurrences and existing methods,56frozen paths,83009history, fixed runner seed/two workers/840second phase/900second CI and every historical finding/obligation. Require behavioral base regression, full affected/serial/parallel/disposable installed, coordinator full local and both first-attempt final-head hosted gates. Separately prepare/review/apply the existing finite18/conditional19 pair only after final gates/fresh prerequisites:two300second/$2reference wrappers,600seconds/$4total,Maxincluded,paid-extra/API0,failure-stop/no20. This receipt itself applies no grant and funds no full component/integration review. Retain generations6-10 exactly once, same executor UUID. Human login, merge and submission boundaries remain. Local operator receipt, not human GitHub approval.",
            "recorded_at": "2026-10-06T10:58:57.843959+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6035844223,
            "contract": {
                "issue": 31,
                "plan_comment": 6035844223,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "494abcda1fa95d346ae5f8a84b638202701e4756c3ceb24fc4b4f0a9f9d114e5",
            },
            "source": "Maintainer explicit standing override recorded October3 in memory/FLOW-DC-standing-authorization.md authorizes all necessary decisions/actions through the private coauthor-review manuscript. Applying it to this exact reconciled Q1-Q7 no-Console engineering and finite qualification contract, verified whole-body publication6035844223 and proposal SHAe9dcbd25045504eb0f9bddcec2614e93507bc4b53c4c74ffe73b292d301986f3. Included Max only, extra/API0; human login/merge/submission boundaries preserved. No live grant or scientific/advisor/coauthor approval is implied.",
            "recorded_at": "2026-10-07T10:19:09.955094+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6045434332,
            "contract": {
                "issue": 31,
                "plan_comment": 6045434332,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "05e7a6d54163067d7b0a02f1e3d61e995482e617233b0d973a41940add09d85f",
            },
            "source": "Direct coordinating conversation: maintainer explicitly approved all things/decisions/plans/actions necessary through a finished private coauthor-review manuscript and directed this override to persist in repository memory (2026-10-03). This concrete append-only S amendment repairs a deterministically reproduced strict-expiry bug and reconciles only exact prospective authority/history; inherited tests, limits and zero reviewer paid-extra/API usage remain. Standing receipt memory/FLOW-DC-standing-authorization.md; exact full verified plan6045434332. Operator assertion of existing authorization, not new advisor/coauthor approval or GitHub human review.",
            "recorded_at": "2026-10-07T19:42:04.886852+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6061320190,
            "contract": {
                "issue": 31,
                "plan_comment": 6061320190,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "22712ec0c4661047adc4b00960ca9f2dd8d162cbc243b658f7ce23a8d63fac21",
            },
            "source": 'Direct coordinating conversation October8,2026: maintainer said "Please increase the operational limit to30 minutes for this particular case then continue the task/plan", accepting the proposed1800-second complete-suite/45-minute enclosing CI recovery while preserving live840/native/credential/billing limits. Existing standing override authorizes all necessary concrete actions through private coauthor-review preparation. This exact T amendment6061320190 scopes only explicit suite opt-in, accountable new execution records, current authority/history and complete inherited public-contract material. Old tests/fixtures/history/limits remain. Authorization record memory/manuscript-2026-10/decisions/suite-timeout-1800-authorization-2026-10-08.json. Operator receipt of actual authorization; not advisor/coauthor/GitHub approval or a new native grant.',
            "recorded_at": "2026-10-08T13:49:39.161057+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6062530466,
            "contract": {
                "issue": 31,
                "plan_comment": 6062530466,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "915e46d413f2d19b0ee2b69ce6ef903fa787d777ebad442d4236781bb2120436",
            },
            "source": 'Operator reconciliation October8,2026 under the maintainer\'s explicit standing override: "I approve of all things/decisions/plans/actions needed for us to get the finished manuscript that is ready for co-author review, please remember this for future decisions/approval, this is an explicit overrite (note it in this repositories\' memory)" and subsequent instruction "Please increase the operational limit to 30 minutes for this particular case then continue the task/plan". Standing receipt memory/FLOW-DC-standing-authorization.md; exact U6062530466 is the necessary bounded portable worker-import repair after retained Phase69 completed972cases in1323.075749seconds with971success/oneerror. Public U designates its closed source/test/gate/authority/material scope; preserve all inherited tests/history, source-bound readiness and software1800/CI45/live840/native/MaxextraAPI0 limits. This is an operator receipt of existing user authorization, not advisor/coauthor approval or human GitHub approval, and applies no native grant.',
            "recorded_at": "2026-10-08T14:51:57.337830+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
        {
            "issue": 31,
            "plan_comment": 6064513854,
            "contract": {
                "issue": 31,
                "plan_comment": 6064513854,
                "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
                "plan_digest": "63e7370374cab273b55c0188f92ec84d41b37ea6c0e43bf00da710a544e28731",
            },
            "source": "Operator reconciliation October8,2026 under the maintainer's explicit standing override approving all necessary decisions/plans/actions through the private coauthor-review manuscript and subsequent instruction to continue under the30-minute software limit. Receipt memory/FLOW-DC-standing-authorization.md. Exact V6064513854 prospectively reconciles the documented installer/adopter qualification mismatch after retained Phase75 finished979cases with977success/2failures. New installed-adoption-v1 preserves immutable fullpristinepayload+origin and everyoldtest/installer; separatelybinds byte-identical installedexecutionroot plus exactly2pinnedprojectintegrationfiles and real isolated deterministic Git provenance. Sourcehead/fixturecommit/hostedcheckout staydistinct, newclosedreader/schema/currentauthority/predecessors and finite focused/separatefullgates explicit. All software1800/CI45/live840/native/MaxextraAPI0 limits stayunchanged. Operator receipt of existing user authorization, not namedadvisor/coauthor approval or humanGitHub approval; applies no native grant.",
            "recorded_at": "2026-10-08T16:36:43.256974+00:00",
            "note": "Operator assertion of prior human authorization; not public approval proof.",
        },
    ],
}


class Generation17Tests(unittest.TestCase):
    def setUp(self):
        QualificationTests.setUp(self)
        self.repo.root = self.repo.main
        self.state = copy.deepcopy(G17_STATE)
        self.write_state(self.state)

    write_state = QualificationTests.write_state

    def test_literal_history_storage_and_current_gate_refusals(self):
        import review_batch_windows_v1 as windows

        current = activation.authorization(self.repo)
        self.assertEqual(
            current["contract_digest"], "721f492ae03b8ac6356766996c5b87167f914c76a7acd902e8fabf35ee630d33"
        )
        self.assertEqual(
            current["approval_digest"], "0e2155b4bb9fc33653859cf350d6d9b9e250b7b705a92c9d00d232340fb8a35c"
        )
        self.assertEqual(activation.selected_contract(self.repo), self.state["approval"]["contract"])
        activation._g17_history(self.state)
        activation._binding(self.binding)
        self.binding["authorization"] = current
        activation._binding(self.binding)
        with self.assertRaises(WorkflowError):
            windows.full_checks_g17(self.repo, self.repo.main, {"plan_comment": 6062530466})
        with patch.object(windows, "_full_checks_g17", side_effect=WorkflowError("receipt boundary")):
            with self.assertRaisesRegex(WorkflowError, "receipt boundary"):
                windows.full_checks_g17(self.repo, self.repo.main, {"plan_comment": 6068144159})
        self.write_state(copy.deepcopy(G15_STATE))
        self.assertEqual(activation.selected_contract(self.repo), activation.G15_CONTRACT)
        with self.assertRaises(WorkflowError):
            windows.full_checks_g17(self.repo, self.repo.main, {"plan_comment": 6062530466})

    def test_all_current_authority_mutations_refuse(self):
        mutations = [
            lambda s: s.pop("contract_generation"),
            lambda s: s.update(contract_generation=True),
            lambda s: s.update(contract_generation=18),
            lambda s: s.pop("approval"),
            lambda s: s["approval"].update(plan_comment=True),
            lambda s: s["approval"]["contract"].update(plan_digest="a" * 64),
            lambda s: s.update(approval=copy.deepcopy(s["approval_history"][-1])),
            lambda s: s["approval_history"].reverse(),
            lambda s: s["approval_history"].pop(),
            lambda s: s["approval_history"].append(copy.deepcopy(s["approval_history"][-1])),
            lambda s: s["approval_history"][0].update(source="copied"),
        ]
        for mutate in mutations:
            state = copy.deepcopy(self.state)
            mutate(state)
            self.write_state(state)
            with self.assertRaises(WorkflowError):
                activation.authorization(self.repo)


class Generation17OwnedRefusalTests(unittest.TestCase):
    response = OwnedCaptureTests.response
    setUp = NextOwnedAuthorityTests.setUp

    def test_old_grant_cannot_satisfy_current_owned_admission(self):
        import claude_owned_auth
        import reporting_admission_v6 as admission

        self.task.write_text(json.dumps(G17_STATE))
        legacy, _ = activation.load(self.repo)
        with claude_owned_auth.snapshot(self.policy) as owned:
            current = activation.context(self.repo, self.policy, owned_auth=owned)
            self.assertEqual(current["authorization"]["contract_digest"], activation.G17_CONTRACT_DIGEST)
            self.assertNotEqual(legacy["binding"], current)
            with self.assertRaisesRegex(WorkflowError, "current source, authority"):
                admission.check(self.repo, owned_auth=owned)
        with self.assertRaises(WorkflowError):
            self.diagnostic.run(self.repo, number=20)
        self.assertEqual(self.calls, 0)


PUBLIC_V = "c$|$~+j84Tl6}`#)JAW_c5f3b-bGsDiIpYVR*WrbB)VsMUT6>~kZ6Md2L(u0Kg~bve&Kw{p3JNQK(%{z9qpD#f<RSXPM*A!zf(8X)@D3ZTHW1`N9v#d_#gGtD-~shHL*%_TZLI>;vkHRW0e-U`g<K_X_7`Ey*fL4n|?J`eK#JSo!#B9?p6=WYqh%l@N-|S%5<0JVWuu^Rfg)*r<?1+FdUfVd>Bm62PQb5m_e{T-;RRO*vw|(VmzCUXFXM#U1}>+((~Qoo{I8IRT`tWWtd0%9@b~Vd^`{5^Jp;`jK|YtF&YjgBQqKeCT2UFnfYQm+s4EBJX!SBUA@gxyI0jdwd!eKWJZMtlgId5;mxwBcl4oAHSQkfI}^_a_!DfOE2DOGSjH;MWA$jtG<jBGR6z(THa6YmEC&rS<#C$tu=vQN$4YHYQj|uehht{wDSTO9-4?i=u_W7_$xX>`4`-^Xah)nkm76D(rH}ai=stxIR9IL0qD+6q*S62Hi(^?F3mazrv$OBMQ$G#+ic~==+hdhjPGoq^rK!R+#|KH6W_4-K&TjIkD31m1Zq(K4<K2hldZ~^ip-6q24Q8Y1crcmG)c5<UI@(uS@6u{tZ~IYk(0@yVYm@Kv+YhV11lL!Z6=d~z_#OX-OK7t}dzT-2!@+1V__TgICt+Is@cwc%ovCPYo@}FWXp(Up;?Hz49!BR$5)QZHk%>ox;dBtr%xoNu@cwi*OOjDE*_zRGz8w#f1SU}Au&by#A1$^~cs|~alPH=^!gw&5nDaQEpU-B~^T~J?&7<vXIEj<_`4Dd=(R?~hwz!{JjE95ig<4~|!!aJY81#n2`S|>FrO{|KGSfjcHOXu`9FE6he04S$jb`y+F&Iblc$$o7W;~dVli@I$F3bYAn#J4gARdl~7p#WeaIB75n9l|iY;`gok9yO^Vlg>+)FMp6>3A}qB(rcB&FACExmj$_lLgFiwus`%XdK5;yiMZmJi&t&=Zn!`Jem!n8N6UI>8r~uQ-9X?`YtT1H~K-Z^(R$8vpY5=%R)=@+9>liGRI0x$$ULR_i5E}_Y*HyCVx!JA}7^q_<+TGJ!Ol!jQoxG=Cn`e8@c&fd8@6S^#Qh4rg&fc*I2k@X{;&9&^=B#6WkVYYHUx1N0|6ym{~#um}`Y!Oj#DCdk}UC(ZSP5g7AX7eQ3@36br%{;2yvb*nPVvW$P3cUwGJnEXX-46GMzD!j~;Bh8+z1=Y#opJhQ#U`MIs5i2T*+k#iv1Yrd&|8Y|dfnwe6YuW2>Zd7%#Q1VRFL5lM#LuF``k>WWn~noiU)e9npxZi#Qho3q+FNvK1ZrwQ$>GzW2A-V%wdcuZqcz6q;nzs41|PZHQfby-3+n|)a3SnFn+G8S#3Fwcu>lLP8DVYyAQ)be>_>#a2vDcwZaJj0&fP;e;XIx_)82_<0xNo`4Mb>$UnzM81680m_lQhV$Gn|qg5tR$OOMfu!U4<zbq6UG_V-&yG){9<Us8)$GtBHu(QmWCe^Zx}rv@e5t3NTj1zjRs5nR$-rzC|*93<z+QMSbcPN?W>#0D&F7UYeIGyP+Uh<aCL(_W2H}_#cFA(A&fAFXW~)<$|6xoRy@T~Q1N1)4;jO*Gx7jSaMEg>smHep$%RUKpRKupS;#>A!?NM)V_4E2VC5lMlhdugKi!?&jRS;RH94Mzu*rcCYZ5%40z}uI*)(aPX`a9}OzeWe2v0ZkM3^Y4i9CnBB1v=d0S@MnXOe!cI3(O6>ijm=2am(kaPNYNRn=3WZkK;iHibA~3vmj~LG>y7kV>HjL#9fy^`AYz>O6vthZcS~)K$1e?E8(wmW_+BbM(U<=|0kxfvsX^fw)Wm7mI}v<+$kF-<c`);GYCtI+0`~&XUcnxy*|K3NS!81wTNf*QKegQ65=Xl%#{wWZ@RNtoC8W3a4<6J45`RY==a2_ChG?RRA}lK$%u#P+|cv-8?o&P!ens3oI#=9&6}|mh;?-BCPhByi;R^MkQr&@DJ(xYX}{WX`VMXzrgK&A@iv@Vtz4kPd1qHZ-Q4n71tC7unk~493aiVkiD0rnMblD58{3GlklKMWl2q741VVA%psYDmmNYa5XYe1`Wpg733qfni9bxpCXc14i9M{<k5C!mR=6SypV?R3-U&r%aWF+J*qV%;m7Q7HL-B<)vuy6gCP9&qZJFW+OEVyYg@v(tj%3PM=H-VE?LM7^f0XUH%|cu2zpUU>aVpS61h&pXfvcS(aV&#Z4OH=d`a-NUv0H*4t6zdpPgVFr-dUqFBGNknw*|I4lXCG`EQ^I>qY~n>8d`|HY&!USe1^82wM86kBLYPHJi<KSX!H)?gv7xC_+SqBUe9w@MrsbpB<{}60bOip5gBJ{MUHV_t@n)PmIDWCrO3$*%jcl1b3yR`u>48j{G7C%9z{?Nexa+P+AE+rOB3)dmkL{{o*UTC2<;oE>8tC4v0ALjc!1@{@eb(mils#8#hj?Y17cKOK}y1-)$&bjC(9g`K>QgYOV=Y!+$`bj><Vil=%wI8t1L2>V^)y8oyJdDgy~p6TWrm=;`5G)8vYjq9tvMyzIj-E__SVbmLKodKX2Cf@!=AzJ_w2}DI|cC!F^xbeK^FM^mQmU5H|L@g-y3>wfyVy>eKpWb-TG=t=69}WI2Z;B9VV43A%%*O_~9YW8p}mDn?AUMFjZV5R*F;6|+0?S0qpLyU*?vVGdryXOc>2UlvB3J`okcJNu&G3$kL%UYaNAuJ&nC4m#kdDe^Lh1-Yr{_PmA|Sej8@CA!8+DL*4K?kmD=PVZo^q>fR4GVTv3^c=QyD+?eaTeBC8&1HN_h$w`4YRu1iLfAHeU?N$g`_(_VM_z-4A}_3&XMtDl*_rC9fTm#o#1BAsIFXC%QYa-fRSGxI9tGdstT(GWQr6{%&4<<1pB`QhNI3}yDm<B8O|E9Q199OP2R8t~aSUr$tAsSF`WUm4Vdjhtu;912ArYrmP9EsU9eZM#!PN`nQ53D58LHq3)@LiQ)6cw_`apz%*aE{EQ~4e=*$ss(GR6(qAQGh^!SWUs0AC9a_~1u^a$MV2Z|XFQWsME%AVY*mgZikR0Jp!l%7a*7@ayLughVXlv2dh^45qnV&$%Ipi-}XBA<>>5Dn?<|=Z~xFrFyti>!0uV=j!HKtuNoGH<u5~+sltjb^GZ997<FW1U_3R=*5<ZJ$Sw;0pZH&jYS-A(4Yulhl9avGST>PKAkc=Hj5ifsA|!Kdl$tmCGheK^Gp~m->4Ju73B!T63NANTvYUzeo%<w>;N*!nMWSBCXS(dY@<i<yvP_*!Am5>(?WGC{iw*Yl)R#w@`<(^Gor810!05_HaS67mj23R5y}++Vp&W?3B2fpadKo4afP%9u;Pl+MA*s6Gwiommh&j4FR}o-NfA{}Ol&m-6&ZYqH1(?~3wSOfC2W3+b<xs$B#x0Q7y&SlXp!bAOD+lK+4KnI=?jvPkyMfTw8)~qM<@hx;qq1*y^bYUWEa3p$fM$D64O#`1_%40uddwvgfMcHaYhZX*BN$LNN9WEUcxgegJGk&xxN5UHkN_l+pr?J0`e&h`v^(7`JzV^=5-DGZv!8BFFpBdH)^bIua*)rnHg40i8No<`<Lz-L~^r!wI__TuZQhh;)=^WUL~aNuS>9An91t|RK~o#Y?UO)hS!AO?FIP2^erS%@eN*l(6Hz$0>&2~CZL-+MRE7O;AkK_59DXy0>$eQ@TPeaIb89XQs?6llJTa%78N)vYU&P@fK<df7#4nmJsxxJF4ylFZ+cvaq#6%yji9qLKndjSt!)_*l0@jH>f*z%ZjnMfUD5(hM@g2<Hru?LI4EJk0TJg|9P3P&4+j!<w7$B>rFgiyzPZ2hfpeq<7-%hIm=N>@*LmFHyfbZK-r<vE9kiUD+fuJBYLMq@h(WV+!c8dD1lU&^;~^DUf2{47^r)q#fb>7ySuQJ=J$AnUn6!fd0zI<$HT>>-_HB8$HyJFZKTsj4Bmt6)wMlpTN&rAMa3o?3z#vI*m55$&0OOE`6~SpqB&7Kuavn{TKpqM}h^T6$5Bz_y7c92WHMX$BUkhmbK^<x;ff+`nR;nV$<?5|Vs+<6S^bdZ;c?}DKv02CyuXcUET<YcOW~pzMtF@-Mp+DWO@Acb<kGE10g8JU1l#hi^&ZPx6C709kNa5{Wq=DC4<6+`pN&*02FV>&gz~S?!yip-@l9Ko&;XSbCXsP7Vt{U$lzd5?-33to3aLZG~Aw<N(sZ1h?(f@@iE`8b^cMtGC=fv`+@FSAX2A1{N=ekU1)aP%OPd<Z%Wk?*vXbCfO^F%c<-brxO!GjjS+ylkn1-^<erBm(81SJBOPbvO<Qjz(?@6>Gn3yUML=!3|R)&CKJgxL0o+8To){#y?QulT<oi8QG1k$yz`#bh>{Pv&^d@BVP{LWiyc0+I-nqnJQ8z@3@jtk~@cF)|DDFU0L7nFG<V@YWvFHvu)$3poZSiuvzp$3ZDfkh1|z53u^T$;;L1<45Ae0Kvik^Ur5`a%v@N`un(rrBX8t^QKZAxKx{XNg?}IVMRHiba{j-cgea(MC%ARRh2@Q9e1niVx0+_z=x|1ob>gTUjOa&r}b5jiT>5<_U+BP&F$*p=LckYuZOzx?z<1GH<uq?Gg9OPjwJ4;xSxF6u$cL2szzp3vx#u#&@p*}74mxDTA~CA5B*Q|7?vpo&ZdfkND7*b*pBNcv4D#bwIjm`BW@FLhfJal<uH=LX<-l&_wkfp<Wr7NZ5bk9x}j{uno#do;L=iOllp?IZNi5O-;dZ92$5luNJ*lj4x~O()px9ss!ye9lUSHJxOhsLystW0srM2p0e^^iIUu6q<w+*mSAR0*2p^%s9~XBRfm<av^+Cda??_SzVE!X_Yb#W|Mq(G88i6GAE^rQd1fLZ1USo@<x5Z`zx_Vi$!#W7I*?Sxda^xy69PpC`w7~$<Em(zC6aiFvzr4IwsckA=pK-@m|I!UVOWD@p9Ww-6j^$V?YVEirO~5CKJ=LA?!|p?ZOAdnUUxMEXzA24`ONXZK*r2(r2(i%WdQJ(FK7u}SVoQ)KRStR(E1F8h4I6Ed&6k9^H^3F?tSDgglxbQ`eqc4-O&sIG(`AW#M0BurDr7Pv5tWf2b!&<)OI>mug{-v+`i@y^Vgfz?f@IQT5tF=5G9DIfDZ$=<J2>Ln^sAPA`W%Qef*j<B%XaKiqT9~xntCsOgBgBd3-GC;3(P-J5Fw?+T$wJVhwX&m5^?ReC<SUK$doE`4v9jaXK@*uIT|POOrtDvOwJj4eD&5dj02n!PY%C9GT5pTD&D<ITh__xBKcl$l!jUC(5GGSAv^-5@1>CA(40-Ylbc9dB1N9lkiga;HL-8R3Z^&o+1)+IKw8hfu*^zLxid`-sIRW2bSSkxM|(PtcwwQuASo2N^1iDs>TaRgrLdfdf91qiR_MhjFvj=t^>pe;mCn&UgLE4&&12wrn<r_S$u>X1Nt2p)B809QFPy@g>K=WA^wcQSH#%Q1N2as~DTQ$5(1ba{3ewkNOMs|C|LJA^40#ihQ@-7)Lvs<r1sBIq%|3jjgvHk<ITiMOHxV6kOjkF<mm`>Nx6{{D<`|<gsyXb+;;GuZuG^TJM^CET$|T`p*RDa!<v3HGovjERXwpL(Bts2D<xK9|JvfSZL@s28<z#N8tu&voKNo^1L)r-E?{!~ncEicbH>N$c>AWLZ3vv@ktSxV8*?}}o0@s$ljJqu3<fbzTt&LkXk~p1<p{5U2hKL?W`NDp)d8+B|nhk;ylS)ez)NNSQ&eNor&$6dbOE+fRZ{^kWCm#jbeBn0Y!Ggkt97b;6Xr<!Pi7wNe--emMbFTach3k)tCt2@G??@VMj>oyGu%?c{*}J7MC2zLjqaCKGLi<HK0kOwCg(Y_wUd(JYCMenDD`#z+W`e_6ogd}c5Lz`a*w=d=V*JYv(ll4N?%2k=bEj>$@y_4*5DcezwGk1#c#}8P^T@RkL~LJ6CH>od!XAsAZW5<EejKvh1C2fS*S}zUfQaU`kb0sxO;;7sK>}yv%H0}l$SJ42aQNDkV%k~?Q)*voFH|#n(Ke7>eDaM-hQ+M#zT4TF3!J_W#HDSHjDoFUxqH;n6KO3>OqXQ#6z2?w0?<VaQ(>7$Oa0y`Ja#8_wso38eE~Gf0~Z%<QP=g$Vt!0Zj$g*eHW}=YKsah312^0%r_BCKUOeT_7<Ki0L@3!7MMl#xxg+oJ#oEuEaNfZIn)MMEZghi-Qr*zlE#tEMxH!?&%8;MKBN4R#T%~sAp`96MXCBH;ymV&)n}#KC#vuygWD+(EV(B`Ho)+ucmKyre$7gtJK32gS)%NC2(pl}hvwp6DAJ9KOt04FVlv7cojwKj0(Ltsm<=r8YbOZ<J^C^DODeO7>Qg=VE->+`D3MlneQiGpO=!}`T9K}-UF?Ago(v>rj4x-dK)uLKk@2j^=NC+27*lk6@L7HkbAIV!Rs0bh(Qy!ueyd`qV=F_g-4=|ADTyW=r*o6cH`hbc;t`o~0Sl5nKz8&unS5cWAL6Ug*ixw(pJ#D|qKaiB)>r6}f`GqD%@Iu@D`211op3k=L`oc=)Mfhqz*KsQqtFqIgcQs2WF#^>o4(YGPb>QQ`HQ)&(I@+Uchp@~%rW;aUgdSU&Ls#%8Eq%tXoaY?4$bjpL6UN%ccNY(k-Ut-(SL4(Q`cXB*sn}aVM-Zp3C)CXl72#(sZ`U{XOK(O$jXgNK^wW6_Su)}B*20?G*%>c<sQN3=F+~iZOvRX3gRrJM8Ao*6!P;X^SY0m#c1Z;gez%!}fek#_?ixFWO`m-4Vmr+Pa)u?QC%p2}a1-jn_xrg#jrdEl`Jvyb&v_*y_i)|Y%pkm_2RC|@CJ8wSXNcQp7e&6o<efSicHO~jpR1l`AD!|que*q3KT(TMg``&`nYzNa0Kx7al;ZLrW3O#&r`4d>g$d7!P0NV#n5(c+MN0Is_QDUrS?$`J#NeogqaByzf=UDPZk(u@81P&E#&i(Z{7<KXcwwH4aWkmXY2>ZCvfX?*X$+#3jn(Ii1yYiI5z8#@`RHImdVK81JtPPV4ABgfOt470jVvcD0h;Cc6mPPU-E5!j_>fvXfwTuyJmS(qBLbJ9I$lMYGD6~)FCgzcl0^>Ojdab<wfo{p;nBK8WCAiO%*7D(H8L1Qo5;nLu!k<AJUjcy<8ES;FXIwYz|=f#xHr9d!fN0uo5vwVDoT~e&ncC0C~{(zooR)ZWuIh3ozUud^MxBNq5)P|B8{FAk$jxd0|lb}$7GMBm5nP8xr=C3&W1ZwCFf!@k#J#Mv?%vIdL-|Wz}Hm%9?mnV07=G<ciIU!Pxc>{>rZ$6gACoR?w0qL>zmtm!Mjg4*Gqbv3PT`_hQSsI=jk|Q%ml-+7D}U5w@bxG2|^l|k)WyD!AH>A2C_CMw{i+$M{$pnfLl2#BYm})dg6SaR*Em~!y+U@aG|R+w527m>!x1d!GqI5SB&j?Uo-yLQ(Y@+Rto0?Hii+=FCJZXxzI(2?S(1Z*rDkdQP<M$>eNc%<Ozg#r+#-{-4|nJ1g_5EhIo8KwtCG(p1|VOO*@T;VDlx-s4CuxtJ>kA&V}{!q#MH!Rn?|bd|U~qm-3{`k*P3Hx4o?`g>s^uTylWhUP{kQtX2-F$T+-&g3eKs9u<Gsj=dlA*%1w^P6B@>f)iywK(8d#>D1sH3J#}%bSj(jYNNTU)$RKJ@@lPbmw#RBcf$$(PQ{}`o&(tslz7L}R8Z)?nEDqkLKG!&8sI98WwC{%MF}`=X8fC}eZo`;zD^jpS##NFGGEN5+i-EdHH+y0DbMJ9vfVC{^V#`)kc{ShdSfsiOh$w4Fp3w88NH4d+hIJOB~y-n7r5mxpxbgbMQ=3|f_yfI;mjRIxVhFMJu6KcIh%w{x(u$(%4Ot`+sK?Imj%x(vc`OGk}K<ofQaxAqHgFVbe7wM>Y4A`szVnVU4!2ZjW?W+V+jrCyP+)$3$g71B{GsGG{VT}HJ50=R&8~nr3QvLKdjK7@p0*f?qrAo<iTH%KDV2MBd}avUcGmS**Qbh>mIG4BQWjpmp(jC(3D1eSQSmmwk=}eD$Vds$TLT-nZQ#oO-0F>e9#4IpQc%v-{ey-ySnHW%}WL=eRFMfbJ~HWdn{8&Vwm>A0|Canb|Tw4hR1lUojgxUu-yYUWdALvMecnv2PM>@Q_@osf|_zAq~sWsyG1*Rp~J)0ZK}I<t}fs%#3d*?^4zL0E$N>-lBT7;$ZYaZ2%6V47U7)GafvB)q&YW-=~LC8cGYXezYBf2nV{_K?2Rxs6Ej<<c=m_a<AL>!9G5r=JGKnq<PgFkU47JzuW_->TLd7VPP>u4$1rV~>W}Em9XBNq<|wP<S&o?J`e75$3C~zn+@kB84d}GuJP-@j^22y8&I#rszFqhNWqTGNYyhfL>S#sOAWCTvbeerKedUt^N)TTH9QDGDfg5l=N64tFA~EBsb0^8+bU+1aq>YlmlPq6J*BT!B+>wp6Vq3^>Q=O|1Hy6USTf6kTO5Bu+EWwHol2w%Q5LYOWE_L1E)}0>01@F{X1%pPXe3YnjM#Ftqdn(vwm71SLH8C#hx)w=J5EV3$lg^EZixK<?JO62<kz{g(NlrnuIhY82O+xY+Pm?0^PJXwN4i<DM7=#^?_L`Ox4d<~@#_j%PG^)v&C_r?`dL(6obwT5WUXg?f$e-xOX=$r2QE-WrYfTVC3FAlFkkt6{!dVbRR&kZ2kj9n+x!mlSroc#iCMK>?g@+?2LO$_y%IM~wO^@|LNR}IW0OTDh+>w21*G*xY)2@rC{ecmxb6kL20K0S7oz>0UH}xq`8vj}#!i^@-db!JU(VkjCHrefH5z_fuR$;4vo5mN-dsNNVG;XQeAl*<RW<a`$#pDo0rfXNDbmvxFmd2OCuCSt-3IK(i+-Z?G#+5#?tfagle&RBC-*R%Y=5aMn0(~)rDXg#Mg6zq;IX+V8({eXY)c~T;-}`Fmt4q@Q@JS62AUsHCnuyEk>5I6iw_Ln$Cc)br->1?{WvH;h0|E|4n&*EjE$aBBtk#c<K!AZjigpZ-O1cg0^?u;13WRs;=O%X4&&|bwkagQWmk;Z1>xU{U+5xqr%I@HZcsPAdQvz6(A5u(Re~vmzkYp#t-|<wmF>&|gm}(18&1og-DY}Kf@)KUcN&AXQBjhNi9L{Hh3+206o%a`Wyx)R%rvZSAPG-nxZm)%^=3ojRUw!$YsDM?vfnkyWlSyfxDZXrUJ{U-|z2LE<l8)b0bi$+ZEo&;vbr_2rb{o&b_E40YF7^qX=QQnUSr)#-4)43%81JONwkbRLNhvL7BaHPW`=&mreR+tnxea0rfxYJRmmK`+@qtT<5e71n#y1G*h}$qvVM^wYoH%p=;=Wt|<<zN}l4z3B*ti+o&Ut3-8(n<g&h+yo?>(uW0B)=w3-~ng)y-t8CHI|A25|W#gb=T8hEtzl;`aefq7r;-`WFG{x-o9Bz+XAdf&1xpJ_?`xI2&ljGd&$Yl~f2Q^}kP%ynPJ6GVWjA-3-LF8oKKf)PmC<m(|KmxD_hKXQh16{-X(G^=?~IYG9t|=a|}YAdQULQRqhb!WoYQudz8jM8uk=<NyEs$A6E;7cJg&t6<G}gde-CTPh50g!m+RX6~5EN$h<M>vDq_{tT{LfIBmnk~kCUm?cC4qUT;`%CC(dH1EtY^#n9&M9O#SsqS}QxXh1+4G_as^Y3VMTHa9>rU&bRTeqIWXBj)vp$)R3!)pDkASWp-(F>siV15)^p<@bMZH7u8Q6la}6#wvVLC_qaYyPo`S4f+qC>};ZGMH?G$!rh?i*Pgw=pWcHW<Hxn@mvMLAJ6_5@&K(-"


class Generation17Predecessors(unittest.TestCase):
    def test_complete_primary_predecessors_and_identity_mutations(self):
        import base64
        import zlib
        from types import SimpleNamespace

        import review_packet
        from test_installed_qualification_v1 import PUBLIC_U
        from test_suite_deadline_issue31 import PUBLIC_G13, PUBLIC_T

        numbers = [6064513854, 6062530466, 6061320190, 6045434332]
        bodies = [zlib.decompress(base64.b85decode(v)) for v in (PUBLIC_V, PUBLIC_U, PUBLIC_T, PUBLIC_G13)]
        rows = [
            dict(
                id=n,
                issue_url="https://api.github.com/repos/Zi-Deng/FLOW-DC/issues/31",
                user={"login": "Zi-Deng"},
                body=b.decode(),
            )
            for n, b in zip(numbers, bodies, strict=True)
        ]
        context = dict(designated_plan_comment={"id": 6068144159}, issue_comments=rows)
        repo = SimpleNamespace(name="Zi-Deng/FLOW-DC")
        self.assertEqual(
            review_packet.g17_predecessors(repo, context), list(zip(numbers, bodies, strict=True))
        )
        for index in range(4):
            for field, value in (
                ("id", True),
                ("id", float(numbers[index])),
                ("body", rows[index]["body"] + "\n"),
                ("user", {"login": "other"}),
                ("issue_url", "other"),
            ):
                bad = copy.deepcopy(context)
                bad["issue_comments"][index][field] = value
                with self.subTest(index=index, field=field), self.assertRaises(WorkflowError):
                    review_packet.g17_predecessors(repo, bad)
            for duplicate in (False, True):
                bad = copy.deepcopy(context)
                if duplicate:
                    bad["issue_comments"].append(copy.deepcopy(rows[index]))
                else:
                    bad["issue_comments"].pop(index)
                with self.assertRaises(WorkflowError):
                    review_packet.g17_predecessors(repo, bad)
        self.assertEqual(G17_STATE["approval_history"][:-1], G16_STATE["approval_history"])
        self.assertEqual(G17_STATE["approval_history"][-1], G16_STATE["approval"])


G18_APPROVAL = {
    "issue": 31,
    "plan_comment": 6068705967,
    "contract": {
        "issue": 31,
        "plan_comment": 6068705967,
        "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
        "plan_digest": "a9d3c8e4275db2f356d81889b5ba6079519e8670faeca5b5da6a032bbafa3c9f",
    },
    "source": "Operator reconciliation October8,2026 under the maintainer's explicit standing override authorizing all necessary concrete decisions/plans/actions through private coauthor review, stored at memory/FLOW-DC-standing-authorization.md, and latest continue instruction with30-minute software limit. Exact X6068705967 supersedes only specified uncommitted W6068144159 diagnostic path/threat/test meanings: CI-owned exclusive temporary root and SOFTWAREchildTMPDIR use unchanged default runner/CLI/Make; seven baseline source exceptions plus two new files, with check.py/Make restoredexact. Preflight/quiescent checks and bounded six-file raw diagnostic artifact are owner-writable bookkeeping, not active hostile-owner proof or execution/readiness attestation. Existingmissing/failed/stale/unknown evidence, exact sourcefreshness, oldhistorical tests/readers/constants/receipt, independentcoverage and scientific gates remainstrict. Fix original60-secondinvocationdeadline, migrate exactly five named new-uncommitted test expectations with originalbytesretained, preserveallother tests andG17draftreaders/history. Complete raw X embeds historicalWexact, and W/V/U/T/g13 primary material staysrequired. Actual17history addsactualG17 once preservingold16prefix/sameexecutor; source/local/installed/coordinator andone newheadfirsthostedattempt remainfinite/separate beforeanalysis. No promisedCIpass/scheduling/performancefix. Suite1800/scopedCI45/live840/native300/900/extraAPIpaid0 unchanged. This records existing explicit userauthority, not namedadvisor/coauthor agreement, humanGitHub approval, nativegrant or scientific evidence.",
    "recorded_at": "2026-10-08T20:45:07.862801+00:00",
    "note": "Operator assertion of prior human authorization; not public approval proof.",
}
G18_HISTORY = [
    {
        "issue": 31,
        "plan_comment": 5900844013,
        "contract": {
            "issue": 31,
            "plan_comment": 5900844013,
            "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
            "plan_digest": "85a4947faefeb29410ed250b29fbf82c3e5dd1c95d2106dbaa928e0c9bc883d7",
        },
        "source": "Maintainer explicitly requested implementation of the supplied twelve-step roadmap on September 29, 2026 and repeated that request after pausing; this comment maps its step 2 to existing source and tests without expanding scope. Paid live batch remains separately unapproved.",
        "recorded_at": "2026-09-29T23:12:35.136186+00:00",
        "note": "Operator assertion of prior human authorization; not public approval proof.",
    },
    {
        "issue": 31,
        "plan_comment": 5966428269,
        "contract": {
            "issue": 31,
            "plan_comment": 5966428269,
            "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
            "plan_digest": "b170fe0c80b101de1568b96b5b07ac6f4418778ca585b53b5f6c755105511b7a",
        },
        "source": "User explicitly authorized all necessary decisions/plans/actions to complete the coauthor-review manuscript, requested this override be retained in repository memory, and confirmed PR34 merged; applied to the concrete issue31 provider-aware amendment under memory/FLOW-DC-standing-authorization.md. This is an operator receipt of standing authorization, not an advisor/coauthor decision or GitHub approval.",
        "recorded_at": "2026-10-03T06:38:52.487404+00:00",
        "note": "Operator assertion of prior human authorization; not public approval proof.",
    },
    {
        "issue": 31,
        "plan_comment": 6001819615,
        "contract": {
            "issue": 31,
            "plan_comment": 6001819615,
            "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
            "plan_digest": "2163be1844b7ef658d23276ce425ea8797ffa90bf776c6318109423288865e57",
        },
        "source": "Direct maintainer standing override authorizes all necessary decisions/plans/actions through private coauthor review, retained in FLOW-DC memory. Coordinator bound prospective reporting/test/continuation amendment6001819615 and two new separately capped activation purposes10/11; old grants, calls, reports and human merge duties preserved. No approval inferred from public issue text.",
        "recorded_at": "2026-10-05T19:49:38.315425+00:00",
        "note": "Operator assertion of prior human authorization; not public approval proof.",
    },
    {
        "issue": 31,
        "plan_comment": 6008093895,
        "contract": {
            "issue": 31,
            "plan_comment": 6008093895,
            "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
            "plan_digest": "e793c7623749aba94ae69fd623d7b406b4ec40227a3f71f271487a5019724734",
        },
        "source": "Maintainer explicitly authorized all necessary decisions/plans/actions and paid processes through the finished private coauthor-review manuscript, requested the override retained in repository memory, and now requested continuation after upgrading usage. Applied prospectively to the concrete finite recovery amendment6008093895: only new purposes12then13,300s/$2referenceeach600/$4total,Maxincludedonly/extraAPI0,no14,alloldreservations/stops/historypreserved. This supersedes task-level approval prompts; it does not attest diagnostics, provider usage, advisor/coauthor approval, merge or scientific readiness. Source: memory/FLOW-DC-standing-authorization.md and direct coordinating conversation.",
        "recorded_at": "2026-10-06T02:28:48.397470+00:00",
        "note": "Operator assertion of prior human authorization; not public approval proof.",
    },
    {
        "issue": 31,
        "plan_comment": 6009076812,
        "contract": {
            "issue": 31,
            "plan_comment": 6009076812,
            "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
            "plan_digest": "88af4787bfab768b81ff05ea7b09ab20bdefbb6e87572666b38c8b890ce1d2d0",
        },
        "source": "Direct maintainer standing override recorded2026-10-03T01:36:38.550153UTC: all necessary decisions/plans/actions and paid processes through private coauthor review. Applied prospectively to this exact original-Astra-authored v3 plan, published6009076812: observations only;14isolation-first then15tools/source;2wrappers,300s/$2reference each,600s/$4total,Maxincluded/extraAPI0,failure-stop/no16. Preserve all historical records, existing evidence/review gates and human login/merge/submission. This is an operator provenance receipt, not a fabricated human GitHub approval.",
        "recorded_at": "2026-10-06T04:03:24.534151+00:00",
        "note": "Operator assertion of prior human authorization; not public approval proof.",
    },
    {
        "issue": 31,
        "plan_comment": 6009865197,
        "contract": {
            "issue": 31,
            "plan_comment": 6009865197,
            "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
            "plan_digest": "6493f379fdb2ed9ea1580a38d2fcdc1d618608f42b30613b145faea69323f484",
        },
        "source": 'Direct user standing override recorded 2026-10-03: "I approve of all things/decisions/plans/actions needed for us to get the finished manuscript that is ready for co-author review ... this is an explicit overrite"; reinforced by subsequent requests to continue and authorize necessary paid processes. Applied prospectively to the complete source-verified v8/recovery-v4 amendment6009865197 after actual15 identified estimated_tokens shapes. Concrete finite limits and all evidence/human boundaries retained. Local operator receipt, not inferred GitHub approval or new credential/billing authority.',
        "recorded_at": "2026-10-06T05:19:19.280275+00:00",
        "note": "Operator assertion of prior human authorization; not public approval proof.",
    },
    {
        "issue": 31,
        "plan_comment": 6010775261,
        "contract": {
            "issue": 31,
            "plan_comment": 6010775261,
            "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
            "plan_digest": "e87de935601b8bbb50f7fa4188ddeb2bac4e1782479aeed00e2185f9dfc1cf05",
        },
        "source": "Direct coordinating user explicit standing override: approve all necessary things/decisions/plans/actions and paid continuations through private coauthor review, recorded memory/FLOW-DC-standing-authorization.md (2026-10-03). Applies prospectively to this exact published finite recovery-v5 contract only, preserving extra/API0, all evidence gates and human login/merge/submission boundaries. Local operator receipt, not fabricated human GitHub approval. Current hosted workflow must pass before source mutation or paid trial.",
        "recorded_at": "2026-10-06T06:33:27.488866+00:00",
        "note": "Operator assertion of prior human authorization; not public approval proof.",
    },
    {
        "issue": 31,
        "plan_comment": 6011162252,
        "contract": {
            "issue": 31,
            "plan_comment": 6011162252,
            "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
            "plan_digest": "f3d7d5d6656a3397d4c11a759ef543fcbb94c8fa49e83795117f8e922799cde4",
        },
        "source": "Direct coordinating user standing explicit override recorded memory/FLOW-DC-standing-authorization.md: approves necessary actions and finite paid continuations through private coauthor review. Applied prospectively to this exact narrow CI-runtime amendment only: permits runner repair before hosted success to resolve two cancellations, preserving all tests, CI15min, historical evidence and human boundaries. Native-v5 implementation/diagnostics remain gated on successful repaired-head software receipts and separate prospective contract reconciliation. Local operator receipt, not human GitHub approval.",
        "recorded_at": "2026-10-06T07:02:43.864501+00:00",
        "note": "Operator assertion of prior human authorization; not public approval proof.",
    },
    {
        "issue": 31,
        "plan_comment": 6012492318,
        "contract": {
            "issue": 31,
            "plan_comment": 6012492318,
            "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
            "plan_digest": "c0dbb09a3b7a03176988263aee50059c905af6750ef42de8042ff386e65b8461",
        },
        "source": "Direct coordinating user standing explicit override in memory/FLOW-DC-standing-authorization.md approves necessary concrete actions and finite paid continuations through private coauthor review. Applied prospectively to this exact original-Astra-authored recovery-v5 reconciliation at4fbe2af after verified full local/installed/hosted gates: two separate single-use diagnostics18isolation-first and conditional19tools/source,300seconds/$2reference each,600seconds/$4total,Maxincluded/extraAPI0,failure-stop/no20. No repeated task approval is pending; exact final-head software gates and fresh actual prerequisites still precede separate preview/application/invokes. Full component+integration review requires a distinct populated finite grant. Historical approvals/evidence and human login/merge/submission boundaries remain. Local operator receipt, not human GitHub approval.",
        "recorded_at": "2026-10-06T08:33:16.223093+00:00",
        "note": "Operator assertion of prior human authorization; not public approval proof.",
    },
    {
        "issue": 31,
        "plan_comment": 6013795098,
        "contract": {
            "issue": 31,
            "plan_comment": 6013795098,
            "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
            "plan_digest": "8351ce7ce762488c0f2447332487565c1c216e1f24ac440b237dc4d275277130",
        },
        "source": "Direct coordinating user standing explicit override in memory/FLOW-DC-standing-authorization.md authorizes necessary concrete actions through private coauthor review. Applied prospectively to this exact original-Astra-authored 58960-byte fixture-group scheduling plan at44eeac9. Only check_runner.py, additive test_check_runner.py and SETUP.md change; preserve803 occurrences and old methods, protocol2, two workers,840second phase,900second CI and all historical findings/evidence. Require exact final serial/parallel, installed affected suites, coordinator full local and both first-attempt hosted gates. No native grant/trial funded or authority constant change; separate prospective native binding reconciliation remains required. Retain generation9 and prior approvals once and original executor UUID. Human login, merge and submission boundaries remain. Local operator receipt, not human GitHub approval.",
        "recorded_at": "2026-10-06T09:55:50.305381+00:00",
        "note": "Operator assertion of prior human authorization; not public approval proof.",
    },
    {
        "issue": 31,
        "plan_comment": 6014789492,
        "contract": {
            "issue": 31,
            "plan_comment": 6014789492,
            "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
            "plan_digest": "e2fe530795df6701f43de54bb869032cd5375e92c9ecf543fa4e5a7d7830fb84",
        },
        "source": "Direct coordinating user standing explicit override in memory/FLOW-DC-standing-authorization.md authorizes necessary concrete actions through private coauthor review. Applied prospectively to this exact original-Astra-authored whole native-v5 authority reconciliation at8bb1abb. Only reporting_activation_v5.py exact next contract literals and duplicated plan check, new standalone test_reporting_authority_v5.py, and PROVIDERS current-authority guidance change. Preserve all810 occurrences and existing methods,56frozen paths,83009history, fixed runner seed/two workers/840second phase/900second CI and every historical finding/obligation. Require behavioral base regression, full affected/serial/parallel/disposable installed, coordinator full local and both first-attempt final-head hosted gates. Separately prepare/review/apply the existing finite18/conditional19 pair only after final gates/fresh prerequisites:two300second/$2reference wrappers,600seconds/$4total,Maxincluded,paid-extra/API0,failure-stop/no20. This receipt itself applies no grant and funds no full component/integration review. Retain generations6-10 exactly once, same executor UUID. Human login, merge and submission boundaries remain. Local operator receipt, not human GitHub approval.",
        "recorded_at": "2026-10-06T10:58:57.843959+00:00",
        "note": "Operator assertion of prior human authorization; not public approval proof.",
    },
    {
        "issue": 31,
        "plan_comment": 6035844223,
        "contract": {
            "issue": 31,
            "plan_comment": 6035844223,
            "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
            "plan_digest": "494abcda1fa95d346ae5f8a84b638202701e4756c3ceb24fc4b4f0a9f9d114e5",
        },
        "source": "Maintainer explicit standing override recorded October3 in memory/FLOW-DC-standing-authorization.md authorizes all necessary decisions/actions through the private coauthor-review manuscript. Applying it to this exact reconciled Q1-Q7 no-Console engineering and finite qualification contract, verified whole-body publication6035844223 and proposal SHAe9dcbd25045504eb0f9bddcec2614e93507bc4b53c4c74ffe73b292d301986f3. Included Max only, extra/API0; human login/merge/submission boundaries preserved. No live grant or scientific/advisor/coauthor approval is implied.",
        "recorded_at": "2026-10-07T10:19:09.955094+00:00",
        "note": "Operator assertion of prior human authorization; not public approval proof.",
    },
    {
        "issue": 31,
        "plan_comment": 6045434332,
        "contract": {
            "issue": 31,
            "plan_comment": 6045434332,
            "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
            "plan_digest": "05e7a6d54163067d7b0a02f1e3d61e995482e617233b0d973a41940add09d85f",
        },
        "source": "Direct coordinating conversation: maintainer explicitly approved all things/decisions/plans/actions necessary through a finished private coauthor-review manuscript and directed this override to persist in repository memory (2026-10-03). This concrete append-only S amendment repairs a deterministically reproduced strict-expiry bug and reconciles only exact prospective authority/history; inherited tests, limits and zero reviewer paid-extra/API usage remain. Standing receipt memory/FLOW-DC-standing-authorization.md; exact full verified plan6045434332. Operator assertion of existing authorization, not new advisor/coauthor approval or GitHub human review.",
        "recorded_at": "2026-10-07T19:42:04.886852+00:00",
        "note": "Operator assertion of prior human authorization; not public approval proof.",
    },
    {
        "issue": 31,
        "plan_comment": 6061320190,
        "contract": {
            "issue": 31,
            "plan_comment": 6061320190,
            "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
            "plan_digest": "22712ec0c4661047adc4b00960ca9f2dd8d162cbc243b658f7ce23a8d63fac21",
        },
        "source": 'Direct coordinating conversation October8,2026: maintainer said "Please increase the operational limit to30 minutes for this particular case then continue the task/plan", accepting the proposed1800-second complete-suite/45-minute enclosing CI recovery while preserving live840/native/credential/billing limits. Existing standing override authorizes all necessary concrete actions through private coauthor-review preparation. This exact T amendment6061320190 scopes only explicit suite opt-in, accountable new execution records, current authority/history and complete inherited public-contract material. Old tests/fixtures/history/limits remain. Authorization record memory/manuscript-2026-10/decisions/suite-timeout-1800-authorization-2026-10-08.json. Operator receipt of actual authorization; not advisor/coauthor/GitHub approval or a new native grant.',
        "recorded_at": "2026-10-08T13:49:39.161057+00:00",
        "note": "Operator assertion of prior human authorization; not public approval proof.",
    },
    {
        "issue": 31,
        "plan_comment": 6062530466,
        "contract": {
            "issue": 31,
            "plan_comment": 6062530466,
            "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
            "plan_digest": "915e46d413f2d19b0ee2b69ce6ef903fa787d777ebad442d4236781bb2120436",
        },
        "source": 'Operator reconciliation October8,2026 under the maintainer\'s explicit standing override: "I approve of all things/decisions/plans/actions needed for us to get the finished manuscript that is ready for co-author review, please remember this for future decisions/approval, this is an explicit overrite (note it in this repositories\' memory)" and subsequent instruction "Please increase the operational limit to 30 minutes for this particular case then continue the task/plan". Standing receipt memory/FLOW-DC-standing-authorization.md; exact U6062530466 is the necessary bounded portable worker-import repair after retained Phase69 completed972cases in1323.075749seconds with971success/oneerror. Public U designates its closed source/test/gate/authority/material scope; preserve all inherited tests/history, source-bound readiness and software1800/CI45/live840/native/MaxextraAPI0 limits. This is an operator receipt of existing user authorization, not advisor/coauthor approval or human GitHub approval, and applies no native grant.',
        "recorded_at": "2026-10-08T14:51:57.337830+00:00",
        "note": "Operator assertion of prior human authorization; not public approval proof.",
    },
    {
        "issue": 31,
        "plan_comment": 6064513854,
        "contract": {
            "issue": 31,
            "plan_comment": 6064513854,
            "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
            "plan_digest": "63e7370374cab273b55c0188f92ec84d41b37ea6c0e43bf00da710a544e28731",
        },
        "source": "Operator reconciliation October8,2026 under the maintainer's explicit standing override approving all necessary decisions/plans/actions through the private coauthor-review manuscript and subsequent instruction to continue under the30-minute software limit. Receipt memory/FLOW-DC-standing-authorization.md. Exact V6064513854 prospectively reconciles the documented installer/adopter qualification mismatch after retained Phase75 finished979cases with977success/2failures. New installed-adoption-v1 preserves immutable fullpristinepayload+origin and everyoldtest/installer; separatelybinds byte-identical installedexecutionroot plus exactly2pinnedprojectintegrationfiles and real isolated deterministic Git provenance. Sourcehead/fixturecommit/hostedcheckout staydistinct, newclosedreader/schema/currentauthority/predecessors and finite focused/separatefullgates explicit. All software1800/CI45/live840/native/MaxextraAPI0 limits stayunchanged. Operator receipt of existing user authorization, not namedadvisor/coauthor approval or humanGitHub approval; applies no native grant.",
        "recorded_at": "2026-10-08T16:36:43.256974+00:00",
        "note": "Operator assertion of prior human authorization; not public approval proof.",
    },
    {
        "issue": 31,
        "plan_comment": 6068144159,
        "contract": {
            "issue": 31,
            "plan_comment": 6068144159,
            "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
            "plan_digest": "d53015047613e856093221f416a0811abac71feb40554b80deb38e5f4576aca4",
        },
        "source": "Operator reconciliation October8,2026 under the maintainer's explicit standing override approving all necessary decisions/plans/actions through private coauthor review, stored at memory/FLOW-DC-standing-authorization.md, and latest instruction to continue with30-minute operational software limit. Exact W6068144159 authorizes the necessary narrow bounded CI diagnostic-retention amendment after actual first hosted c2f1680 attempt reached1800seconds, failed, and retained only receipt/no worker journals. Explicit source spans and G17 authority additions preserve all prior constants/readers/history, entire runner/seed/scheduling/discovery/fixtures/receipt/native/auth code and tests. Optional fixed controlled evidence path and exact six-file bounded separate artifact do not count as qualification. Old failed head/run/checkout/failure bytes remain historical. Finite regression/static/serial/parallel/installed/coordinator gates and one new-head first hosted attempt precede read-only diagnosis; no blind retry or promised pass. Suite1800/scopedCI45/live840/native300/900 and extraAPIpaid0 remain unchanged. Actual16history appends G16 once preserving old15prefix and same executor. This records existing explicit user authority, not a namedadvisor/coauthor decision, humanGitHub approval, native grant or manuscript scientific evidence.",
        "recorded_at": "2026-10-08T20:10:24.307689+00:00",
        "note": "Operator assertion of prior human authorization; not public approval proof.",
    },
]
PUBLIC_W = "c$}?$-E!Q>vF1IW0vp~7&v7?Vg@1^)BV>uv@K_^hDQd>v8%Ck3K*DM^+CVoobj*d{;q2w^%lS$6%d9E@O-c5Qd?scjcC#DEs?7ZI%b&vhD!LfQgH2@=M{h6FB>KnS|4;P8lc?zrTVqXh+mCzvWjb}U#gCni2D`W0eb;XzwKb9bakuGow~zE;81R0i4*T1F==KjMCoj4m?HK(kl_w`}FW<a<^X~k4^yc-eKc7Zdx7`?hyzMtOQg?Q1?&zC?{p+C{@D1JfgXy+v-w)C5(BS%t@3+4l`oqmF{<hI!t8dlz#+pLlpLO%Fx6#c(4Q3VfAGdah?(NXE;hTLMDZM|a4X%B++h7I$!V!IO<dy6OYpk~8*bn0>eB9WvyV<&n(1r|HaEEQ!-pnf>_xL)!<Cp$7{`iV7s-n^E)Sz&~j&4i~byrg(U722=Mi=|&R*liNkGkzW-ob`@@gOem&BgN-e%|f}$OUEG*l~{=>~=$cPg3$55w`2bV(okbSw`4oY~h3d^=b5W=<Z1p-S0Xq+Vr^Nq_DqzA|WpKSL{L8K0r11o&AW_-{Jd1r#7e2r5z7<79Z+vpsG#ubcArx`}Y^mg;c_}rCjHwu<J6jP*2$;>m;>Bq3X2A(*g^{MQ<RW#kNPCx51D7@L@k#8#jZ3f>%+aM!V^@kU42(QHdmPZI!iY)k>r5QdFraD}!rVTen446{peb9#`INl;)*CO;#VslJM_<ABVBKx6wwC0iB+l{OVWH4-y*K-eO%)q_(U#7`ei2P1JS+cD7aB<}lcklZ&nHhh5JKefH+Nx3A8x&V#PLFGP{$GOhA#CCjWRqW4!XDraHwyKkQ+c~R9_t+S-8(@aVHRHdp-M54-4SEj{&wPj(-OchPj7%NgCv5`$u$*gTesndlKG;L$r)aXKHNmiAWOl+OyTDH2DX<FGz7jzSARBduwHd&hsDXOfl(^{&wK8xN%-Xu>2Bwb07)WS)sv$Ab;sw}Rq@K2tlQkQM3WRoV=Btqsw6}Cuqg7@>HXxl_*jZN~ZNoCufMOV16O!2{5tfZ{c(qC97Nn&%MbK4fVgp_HU*g_;pVMHxbY(d^alc~tlR!W`Mw#GFI(=@`!RGvjQGL4|wZ6Pv9m8EI2%Imt$-1TeKsyxkL9)*&+s?w~qbyK!AFruin$&%C<ZJO3JRg0_FWu1sLDTFR+A?oZjdb-)bFxe8I`u`PQ#&6ZI|3myPzKY++-$y<4e8bKGgy`<>4tv!A!mJuL-NHFMoOE{z&NmQBjWDP?7!;X(+Yc}@`=hqIJww@64MYDi8v6r&vEHfuZS28xJ%K4c57Zs$&V)B49>lb4?Ykb<_ppj?a4Vof+x@tQt*)X`-&%8k_uSCW{f?X!fTj#P6x{BhX(^`!)MVry>eUUwyI!;J<Icljo0e5lXGv9MMWpt7d$-$5_6d9(K=JnS7FYEi8k5UbNhu3i)UC*Ll9^hirI4i%rD+o&fXUhxT9yE7D2{G=XNMa&`K{F-`olg`fR8Fk>s(uFm1wnAqD)jPo5I4bN|O|gvdL-m;sBLx`kTo4m&)rLc4XUH!8l=%rc`Av%d`?znYS>CJTs!KGil1Ckd@73k(5nY8(3H+q*v28hZh*du<u%UoRoQ)%Cg4SVm0Vnf)R(TWmPq8j%W#AQ?i27RT@A5E6kg$%vA<A5w?`Am4z``-d43K;bst~Ql?Qe+VtDa1B}&$GoY32cF-u|0+c{O?JuwqfbDo1y|WvuIZzCJ3q=G7U5^JCFV-T~_cDI_0Q=ddr!qN}`56TaMRrBMo-6@z<mLug%rR;k;$z(6Hu&9l>Vrkd+hebP065!0!u||&L?GJ_`!N?O=@9N$W~TtaF@E#MGX`=y2$(HlmA@#AExv=Cu*i7d-BHLqY(H!fuVKf|4!rS3xWNwh=tdW!5JdreCffJA^~T=Y4SfJtg#$d>ShbB0yG^f5G#)fXaD+DNzS)BY44tuTXV|uO(|<I2&99c04bYw~dxzLentu~w+wAH-5JbTM$kX2<cH1&1k0)k8y9gKEkaL8Od+$GN_x*vq5<d6L^QBF%)uyiMr>|aNl>{Gv&ib@S1u%xF11_{dfIH1aPN);E4fxDYGbtT;Sx_cK5!eJd_iu1nQUonJYT+qWot`G5##bN{-#8-69#_0q-v5aQb&L$yuUGsb+OBKI0j@uGTho6WukR&s7C8B7Sqp&D0Yn7`tQ~X#;h<p~HBjPV3t*&GSIG)o?QUEkQ-mvbgqvr8LTDTRs_6D%*8><wyl|knIwC5b4EB+8(deV?Zf>DYVvB(5jzP_7rO4O<HZXj;*{$8|;6={%M!GkY+Kmu~e5?icyxqcl)}v}+f;;-0yBmz3*b&$k><ar}c!e_)CckApOooTg!qfro0Pe7}-a0+&NXT~kk>2M;dW@#4H1<~Acl~g3@`f%<dtqndN$x%(*gS*{=(awNkOsz`+Kx|fu>sgI#vaXMhSt;F123y5qxLSg+Z^s1T=tAzouTCMN7Qh0ATD(lo!)f&+d~rvtD3GKqQ&XM-G-d_IAt)TWyhjp-ERoF=*Ni~JMTTi?rDF;b+|+P=I#0A)2oZuFV`=@EzeKy48a(32Hy0uPZsK62@AT0WfR}Uov!Z-EPWNtzXP_CVLV)Wjy!*Yjl3a)U6}gvcRQs&*gbD0Dd&eg`ZQWAW7rHwtkJLb%t6p3r}wEKkYn0DUvbiS@La5xxC7k~XboE(or^v-#(fgMzxv^gGmp<0KzBWBWTeZJrF}-GPd>;bW3oQ_OblOu+%e(gz^V+m?b4~)HczAT<rfKDTFweRBC_CwYI<|v9EuE?gc%%UUr%`Vc!wlQ_T1$#H=Dk}rzlau76DIu?$Z%^u}3&^N3b$XNkvwt5s~Q+9C*m7Se!GUDUcdS)Pm2=x`Y`cfg*;)NZ~<=w}37HhbR45tvJ#Id=CpCaC+b%#aB=AoWO3%Z%DlWhCf=nb2RAMg?v)f`Rl8T%kx)%j{flHtMm2wpPoLuT5j3*=a=s;-n@?Ay}!6RzxJ<Ik$Z{6`^CkpbN|bC=g;1}e*SJ1UB7$w?fLWfui#gB^Za?te_lVoczOQrYDpchM{7aT=4|yk&`tLi34&ch7K$k1fGjrM$lkT`G$1B#+iL7^j77o9hC4z7hjpIP;9UUa28c_W2OA71%dY2O=Nih|j%0cK=Crf%UX34c`wd8pemL@B{vaXGy`7TSnEW($%ZSNJVE!?aXQ;*yasJS4Hpo}{ZQI?%#*+U*6y(gYkF5Mb7Jo@782pxoO(}Q3cRbA*m`7|EFC6`!>Ig0+U`taC_V0{>(qTK;jq*g#T`JFGzoOt{uiZ;rsT&A3b{-vE34-i?W2S0BSTZ=b+PXN%vYwm}lC0NZ<JSgGsJVc^vDU?p71{VlHBfD6-_Jbx*l6+BuU+u?-RbG+Hx!{7_}+{nfwj6k-$LbYm>EK55*`Lc1q$1$!)8B)L(uM<dk<R#K#FS{LC6Vd8Lze$85L3CaI1LAe1M?~0@7_E<!SUiz{QYFj)xueLGf!2T9AM|n=&s(>nBjq*l!M0OHtc?%P>QUTl5izN@_FxKE{9a*X#GM;t`h7Z9gy(?ziwFSS$8tBxdTPKb1XJzQ#mmG1aS(@Aph5=S2^F50q+|n-YRh<PRwVyrN^<BzxK%sM-Nr(}QIr;@a4n%@2#Yp$(XJ5NPUur0US!VWgd(eogQFnh@_FfB(OTKZlJPoK64Z@Bi-`_za0)+m=)SQ*<>r3RmGaq&E2WazWT(4cAgz$3T(5_J<vD3s~z%%E2kQ^xt3k<<9zs3b=Uo?p9F=X?y_3x{IiSLk3QaRNn%mqIiF|gE=tA;}wx?;8@J7pg%&8Gya)tp0vLJW_ryqFOarQi*V`FDyVrI^a@}C-i<!>$JhD|0>zs2#0v%@xJuupZ4V<G5-f7T=$1>vPGdj2#`9yXc9zHoDS0v^;^>1j_Z1}`RM(5WC6OVztl+REYwm|h^W$yj!^=o17i5MKINcj8$)%zrr>8{dG`i}x4<Ru_W{i)~=CHzfho;>b4jVgLa7XnvP9BzKi#UAWcYs5fK>v}xMeBjD9|n4H@7$0m1;{W+lZry>MI@Ra!|8w=&+AiZ!cB_Hlsqi#E9jE}K@Cl}XE9ujbaUk3iljj*KqyRf!PP}3Mhp%x&b0XHONz+PuFt;*A$|Sq9AWeF{Mpr;%RdKjq@}QveY~|>G89TIft`nuU4f}KN$Qvdag^RUJ=^peyKvzAzw-$JIgS|~*6{I&1Lc%ZPGySoA4$>pBy9t>P`N4;G>H5C_0V$yf&^%TJw}#EI|>bd4(`uFWtby7;v`oFAb}GL@Am#m-r(TnmVQT{qm{+(w{sjCooaj%4?sq=y<1R2-kmtWJ=MHPc1EC3H#5voDPipwHTbY66A^{EzF!!<bA`b<9WFCRu`L^a5R^*9A#&`n=^Q8aw}{YpL@nH^n7~pk1eWnbNUI&N!Z@~e2HekJhWCrh_pe`{UtV9GfA`jbh|856Ev8&-48TvDe}iwlytw-I{U5IJ#p{db@#59fE4s?nSpa^0+fkJFsCYGDn=5CGF28muh5yFF0T_R|gcSRh!+POJw2c%UrmSF5pILM`+`R*Ig+2BG$=|adrYaxr_KY{sG&eKgjg%vH?1Qt7_-fcxp@VJ~n;9>F!+-MhFC#9+n>^Wz^0mCkfhi=pQ?b%hH_KT(mE)W_aB&Hm1Sw}K<y7OUN4^3zS9bS~MNw{_o<`qw+wSgghfQ{Tw{w~BXZmO-C$G<ch(68Fy3w_kfG6us*D!WXg$B2`&T>h)Boak}p4=Ju+xHu;h`{iN^~7nsYf*YKrQtA>T_K{o9xhK*#=Ggb$S@Y0tNYzUe5=OW7#HeV&gteB9m9}gv(B(XMzDa|nc^%g>~Vngcf@Z<fmCk%B-A4EBL;ABbZYAmi9&>d+hQA|cNc$(2KAAZjf|N3i_`}ryI66-ieiI%<r0;(;Fp`}wS4qioNoG?#c%Oy%0X#oB5;lMe@wx0HPx@QB9^-%Or)YIe%Ri3L(i;$KIU<9MR>wcr@Z=Hebg~6tZWgfh|o|BW|#y#+fB>SFbeE!!B6PxXB@h>{n~}z7{6{dz5YPk?h9U)BxtF>-~o64;OkLDe`A*qkKNyFd<PfdAmhjcg%Xdli{9IcW4D8s7Z)$yzzps?O+1AvDDmX_u&2SWoa(OWZVudaP@Jj*gTcN@Ot>|2XRvMeK2iZY^C?fRkyrZ^pS~x3pSg1puw%9fbA@Se&{+5)>CWf#<jEoB+s~natHYzmg_D!F6r6G6238HXbb0HO$U@E3j!2X5x<8z`mp;Lj1;6Eu(7*CVD095RVqmNLeorjYS+Gn>{9?tyWpcOhJHEhFx|d0XH=NIsE5JM84C;<J7<}2m;Ffz&z5)R!awW^D58%_yaX)mNveH+tT}6lec0U~0>^63LLjmCr#II(-CYzuOP+sZg16*ZlM0mr761hE_0t4X-F~%@#FlE3oCsTwE<R64qRIds=&!y5N7;JfP%@SUqYXUiwec(p-fvT1vqlBm%Di2u`V<}tB`0}6c-n=GHn_C~Aj(fdD%mu(-2uT82;=KY^;j(1#svAzD7eq`*;STv+P6j9)jBB`46@>%j%Dal8+fto`JnAg+oe{q}s?Z1LU*45^(GAtLNOtB;0UMk)kSA|KhZ6Lk{kr*!<syGb)!iL>E`(dc);mW>IU+cEwI}rWn;m+$u|YwhpE2qDBXuFkr6}mzi8(^gcdjq7QT)@?2%@6h)1$Ql(ZGI5gfW_n!^>8b7X`R`OP}XOJ*tNLARHQLA-S=4#D}A!(qZh&@C+y&)%$S`yP@w;>2Sl#7hD|~eY_>A3;X38`{9WooC{D~-@q2ZwtZWa$s+F^eRDFcMTqArDWx=<=vLi7-haXAhu>vKRHV185B7oUyY${Qn-2jjm6FGf;B@6c88PObt!+14Q-Ah)yjA>yDhBR_Yrb9wLgwCtXIyv2{q0*~t58I7U%4h6Jdi;VJ(^yD4_$MG#Qg{ag$Nt9gQZ`4PC<ciH@}$B>zV=7Va>{^bhR*sYdZ``>49n3OJAvVSmL&B-WPVe6?e#2?rK;QFkuB{k5q(rE*N_cjD!2R*htM}*V2t0>~Z>bK$KP^%-%Q$ya9ld_i{Sqz=^okfk?Jzh|Qtfz&h6;g$pDik0>(4UOU{{AN`%qTt6QR0$EhP^hfF>dHu>Je8gt}H2fYb?Qyey^-tTuoI7zEz^$gE77YQeGot8!ckAas`1r-EH$SYOKRb&qo{wQg5gQp%t&S?>To7u&HNROA_+|p@T1Czv=H2KUhF=p+%}CFkiP88nmp4HQ)*R>;L738LawONVyy++hxL-5+M&F)4eeSg4c?TDE+&}{N*RV%UPlW2&iunE2vq$xS1r_uBjI$I@p4KGHLjVY_&d*^k<UL&a_n8%f1e?y;k*oV`6ZkW>4>NZ8zF<ggctR;)lAngVa{=&*Qw~B5Pw|)Jd6GztQv;n|uR<r%PkKD!Fi)Az`OJX`(E@!PNmrYX>S6r#H_i`;vimH~!Pbrv8;I3$%Y?sslp@894+~|#J8t+ry36V~JzH$x`jMHhf9q~TTK@g|w-BnMzdc(0e&uShu4Bd(s--R_+DWt<2)Q=uVNS+;C3DwpsYvMMRKBkKHV}zPgL7sC+8^{7mccA0=C!zL++A-FhH%|a`WfLX*Wntb8yy)AndlT9!i<A2fI-{aWzZQTU@BG4=6DjKwoekK3_5&`^II2N!yE<c*F{s}aSyJu4#kGZAG<inu0*ETZ4Y*hU0|f1WTY`W>Y8nDC?R$+f2RHJW^*FR^$C|gLUYq-$hXK22q(CZ6q-kzi&C+7<LC};2NAt{_G0`w8!uboVHbxkGgX=oiuyg)`l&!%8H{l59y9|!3DcYai%orWk#=kM^nNrohPa&c5%=j2hGsY@$>C{)kc}(X_n(!Bd>ckP2JR5dDa+Sb9JLkx(l?=gLN*6q0w1E7V8>{JbYE=9!2KS$(ZSZKrfP<3cuiwzCnqmp8Gj?fwV~GM)O!p)P&QZ3p*?6K+3<5+XLG-edY7S@$u(0CfT76ZAYf|XMc>mX?gLMt9H)|ZAO|cd9K&VD<owjEV*1qhRdL67HI159GQ-b?2S?|5lQAyqnxOEuH-6@_Orj9E=?;gDpY)||Mb1Xs;xB(1ox2&nG7keOlbg()WMY*cLvxsGKOSPRdcSA~Hw(JwI-w7)Ajca_#goXDv|VeTVwNQ%^KA#W3zK#zXgj23%0G3`rYb+Iap^bnuo6s+hj5rNow@zTmQ#FwpaA0{L&tMXv<9XIR3hs3J{b<Jg?RuN`^Aw7EWlC>q2^*3lOio{!dzH9!man9R!f>O#sQ0vB#&{J!3HcsU7<mFJRIO>{Jbr@;R|2$@cE<PdN)q~Lzu>;GWneHOmjrIMu>QJWA7DLX2YjkbPEN<c!KA6ZZ&fyc!C8k(hjr7QKs<y(Iojibxnh-)NiGo#cJ6MoC9nqzo&%H@fcnxw<Sj^aF8)^r!g3nX9iPoZSUZCODm3-5-rA69fMkQYjNW<PCj`Zu<+eD6~(zIK^mDl9>(7FZqCCI(cqLGfpTB&q{@X;#?c)5G`s5OnV!D5I=}S#S`-iC#jyQ~B5=6aOIcn&d-MA0k_R7_ucm<odh_Yqx0i3efBNd^&2KN>QDerxH<u(P1X+IQ^34zLT=)2J2h7r7zD`f@3lHYLap>Lv8Fr)atdJwtf3yLM+A+7`V$aTF-h=ft)6E_l+<<DSG<I5qF@8(zhwj_;gUqAZUFNZnqYlDLSxg_Qlrm|OmTleEqS7|Qf30k5Q)Nn5No!0|)JCW@wMk*hrY)0HC6%!jfNjefNT=OE!srH;Vj4?m2%%^~!W?$fDGjcrwBP}b*<zoa6R^-O=c&_FBt<O}ZL~<*y0JE?GEvrr&?2v@vMic9vr4BTlTBG?xyY4J3c^WQG}(mBo~F_M-bne6zyH?_*33nio+Hmh*d~k$#{t-fN5F}VcY~v)A^7ts2*|72siq^k831977!g|PY!hDcR7IHino^lC?9#dIV5p+Bg4(MfQuBN{&6i-;_F=lf8k&7MIr;Jg+Y>*GgayHD5qtO4$c(3R9#(JsLR@6HzJYQ3-=6t7^NpV+H-2D(@_%oCtH&o;JgAWP{K@rn;$#}OaQA6CcLGMcfyQ|dj!I*`aHm)!Pr2XbV_0zG`1Sdpt|q6BU&;*s<uMsBQ!~ZWSac&Arwl8KvAk^gB455J!ix}1W-s|1kq?6?bZ)4@^6$<#R=Br4Wcd9)K3c>>Kpgc$>Z8UFE{7#{;HJ0vq|3lFOGw(rUFh8VB(vN8#SIBf8@B>C-)tI1WgJdH+l{&**HRnE;D)D`x}vrx)8&}5@df$><CvnWFFqa(#5vY9PQy@syeQ1wEZWRV>-_u-<&(%ym-IU-F@)L3S)xa_6QbEVsLNHC$<pW1DA=)!VE%Ha$Fsnn+PXc0&K0|AQ1P_P(7~INxMf4#w{-^VM^P3dR1y4$3#eh06W68v`O*X_doN?WtGJCMvOBcq!pa=(79KQ5EKh8RNAP*DFgCnJEWhQ}g<1B=*(SN!;Y&r~l73FaO^!}QjO)jO`E=sZP-Zyh$G+*VySbsu-rhyOcJu3Tm|BnNlCB64BT(#PH=7yHrOJs|nb_4rzF8qev@NHM&XDND479@9;Z(Y7IfI+*X-)1#{zyD>o{6TS3vn>z$hYeQxlLK<yKkds?=LUUUtc|{vMcsY>T4}0Nqyy*Y=NVMC#RgKI2KD+T<m0gysO!x*Li&X<qsF}Bb^kFh%u8Xz>uN?FhMZ^q>jDphKCf8j_k-T*$`o7r}J@wwK9(FQJv5?#$C{iUv{K2Zd>81$kw@xvJ_>i`}@sz5!Eh+cf-{~pZ;^db8{qqwtg8iF6R@czDzPsbXhS>&t)?{#zHuGbO?}gsBpY#E>H!1o-Mlx102Q|lBO~S4>s+nQp}^hFMJNetQmWGPh#?X*W~`wkhpVt!lq?b;Gzbfx|)xX+PU??R5J8k7qlH-N0HGbf1V(29=JcaB%i+RL}wiqhZ5t(fxmchMlZgR>EcA4pWX9S;06AKLz?d|U#;eBgq_Y|wo7WPuVJ4?KQM4I9h;LEDv;7w>?FIkQ<ERjePE5chU5fJfuXDyo4CGse!Bm0KOMB2Y-17L7ayNkPE15$sf?J&!jX2oE7Q{9>O4ZjY3CFWx%cQzf9ft0UbuRFI1m^_TP9-69A^@Uy?1jko<ZBGToZyD0ENyyhQrT($k`obaU|u3h3$l~&AS7cIg>r#5%v|nNj5(k#a<lTn!ho{kf~-Dd;5zgDPvch><?q$_@V#rZ8pHtQR|Hg2JSS2AEEJM(9XX2ltUX18~w9`5#-K`U`Zr1@EloiuqPk!GgXVK*ou1<v9Bxw)O->c_pZ9KSRYSlF8SDm`EH?Vvswgck9$gk+>MO?KIn!K#D&AOh`!M$U@)cP{_3aEw~SXYk#+7vk5=8e7QkV78ptjNZd~QR2Tz#0>`?51+=70!Xa&rOxhz!9HM`foQ1a<<5K5s28h#ib9f0EWkdqh+o0EXPW#BZvr-`re=0LL()4U&VUZ^D$)Zn5{%`!~W4iVZi5|1Y*7vZEj$vmCna#MK616RNOT`I;fgLirU{3zLShUBK!;9Wkioq~;<&!>utE7FDWVD5VPBQUOF!AAnMX3zQNY|57w=l?=q`ieR!lpqfL5CV?I2!*>vtfoZ(CH`jVtgz-mf#3d4CW1>2WHV2qJ&okjbghgNH;m+}!fwLI*~2_pcfUB#8g`wAj&7;dwe7cDVP2jG4bzlC{#7XdFXkpfr)$|d35{|dxC-MAp`AOmT7g?@p2(ltzf%ifIzkdgn*ETxbH_0KJk1NmEIdy;#bMNFc2B=sq5i*^XXA-ho|lAKtmbKkF!U7fA9gl&Lp_|pO#`iNG<dbx&qqBxKNK>zFNsjToFB#z&oq320r1ru&bg*mpVPr~c*Ai(SAcO?e}6P;<-X<z`&q9+oikNhct4wRG_@2wlc0ls_pwg~g9&<naSu#bdD3#&eq!QGhR$5J%YY2Kplq)1cc;<2zTJN$IoyzHXi|qd>Uz;e+xF|5ZofWsL_~Qe6ic9|hD2@{W)7V%FRreizWd|br|;g;T<rDRr!UVho$a|O>&9-D{oOEn<9b>y`8~eif1HJ}*|&eb`u5H1w@<IW4TgN!ZpfZy-A}H^<CWv$;BP3s^FZmA!s?j^q4?W3-<{9bVDs|L7B^HI%CJZ_H#~*!vT-(?scRd6!86n++;a!qT^`ytq*+hjT_L|(-(e4-2#P;^0&7mbDAS6Mx<(_>d?I87n*f~2Lsv}We*3#Zj5M(Dun8nz&F5cfI$-Kk@S(ej4EPfVFWi7d+b$q4bbXd-mc1cm#fuYz@pSqroN}PjxtomBJq;mk_r7)E=Z>iJ%=g{(-PX+-yNa_v=<de5A3gG91awjoe@>j0kM8oP0}my(!l&N&5TI)x5glAUux42tdNO5YPPF~vu&+DnT?s)LX9rmo;%qway?DP~w%!2WAU*C}$zuQUaSB!2h!2s6_TYSBUpom5#N~A(Cw6o!%N<kM^wcRE&yH3zXYdhUEh&Y7J0I@c%Lt%HCE&5jD-q^~e~p{}hHm>C65jnp-}}bb!VqkD{$^|9MeBvo!qE=DL)|vyisM9DkI10!XoW2f1%0l~a2yRaU(bwU{sdnadru|BP<vv*D5kE*yZ(daJlsvrU^g4^i2dR5006fTUH?1|z42*$VNvf76W3Hu9_gtEcBFGZl%-rO=1e_Zj+gTv0QZN{lRX}u^9!Y^2Y$*%td56gT;;+Y@p5x3M0aV9?bLT(oYA`T<XjS1efqE^CQ^K!9J;u5L$@Z(@QhR5_Tu7_`YAqB25+BF2!HIUe+^LbV;foSy4t=A6)=%(bQPkK6#g@-d2j)S>kbe2>(Eu%k%`k%$1k+u1`E8U2}nLr@tF*sy5@eQ!S~LZhiN|-be(pMvZ`q`Vlg2VE*W|cKJamQLe0%8I^haA<7o(F9&K8VR6CXWyDU;y=7$tZ{UI|C#GgYT64pihFsiEX#{)e!!Q(CkIaeNWBdRpn0C%NMe@Bi&6N^Xv8h|qOa%j-};phPfJQ@}ruCM|d80c&Y@h}SOtHmsYf96OuLVW7O3`^PFBMLVCmcJdIH{r+t9nl+oJ!d`>8%`%RenerTwr6~xYJP52coa@JaUE(^UQubVEZ?pkJr`k!bc1luF0wp!iFK8Ua1iO)h0Mc&U;HdU2=6WZ-UVdnC9d(Gb5vX{o@e**yWbQd&eM+r_4tSQf95iazf(VQf#d1h3*jq3oapQh1GR_7MEv9L|COYVs(i2eFC1X@Nn8A^-;Qwe*v+44bqDR<Z2gI5KJ40&R8%1a_VvDR;Z6bgV?pyn*y!_ttT96Ps2u>xi*f|SJWw#z!io88mP+^f@O?VxCV1Olf|8<VAu=sT@29h}zghOQ-5#utO5(nI+>LHHF?7Ff55NikWTC0f;iPhpG-zo7uvpzYTUs2EJ~ZJt@{Guf$Aw^196O%AN);kHR6Rf9CA#cg2ge=cLGn5kI$b)*6M#M|;5q4OI)aB?j<=9C&m%pQ7PzBhT+C)Aoz1gCdvg4v&ZL`hqIk>Qw8LG<Wkbqc0y@LANvPmadN2)=_$P}vT1HLRk-vC;Jg73y3jNEcw4I#%6uqSxy`RF&{!b@At=DV*55N23(@TDe>ckX9)9OsiMpkgRs%+a_sH`YUAyuB{wMukZWioGswX!HHS(mx0tJ)+gZRk$(qfUN`iZU_yO;Kg0$*M*unNr8Q%9IgG<ia*tqFZ`+Rj#olYsxZdD{1niu4SeaU+?EV7v-nOHib#kqRg_qF0{~P+u%}dWAe5#Dv_#g3MH&CMJcUr4LvbS>m<)olcbH+e6yeVl$4*MOv<KgWm?+GRx*{9!BwkLi!{w!XeV#*vQRdoG_=yX&9$+%5JKgqky1vVQ^l9N)3~Eajm`z`sZE`kRA{KBu%gYg#%N_5Y+YNFg|=E`!j!<!rfRYb_94qM&-K5!<L5h$bl*~GQwW$uo#!w&krh>I%3L=}rM0LUQ)EIGS}Rl6t!OkoCrp^C6fldV`O<yA)XC$OwrQhfYE&cAT1jlGvRcA=OsflBris;suw<=SUF2z+mLjXm%I1)(tlEEe$N32}KSfOqmq}VBgv^z$QY+iEC`2ZUwr#Xc5a|n%)by-AnU#e?oWUnyL`{*x;w|6j7e9ICrznRTTcvQpR0^B5N!C=VNpvMkdMKYN>NGD^k?JC?OIejRuZpH_O1$6V{pz#pd;DONpCZv}1K(_uv=ve$q%@=w8LpDGtyE=F+Qy1XRM^c%=&EWOI6|c}S;JMT^z*kldU^=mB`K-JQIxt7a3o#TjWUfX3!A_gk|u=+rA%^|Vu{z%W>sRcq)IAiSroL1gm)hEJsO5QjzJMw&-<$v>uMS_<FdPZqRpT<M+$9?Zzk}2;u@MiMX$I{^EoX10v`SoCwwwZRp5dN&BP=H{Z!l3HdR$ARTVYXiXG1^usLnAI!hCJFkcB%qpeQxudN&1B;-78o+r8brC2bF5k{{dLrKz<<UMtvY7GczfW5jl8IUP!g@&GTQA4$f$m^712R$<{)2R^_+EcPllcXw>UoOqjgbD8lWJxnZ$p&WEYQTJ}Y6&COX;wqJTxwMa_%lRRNmc7s%R0*p0$6TM!=b_8pMO)9d3(~FM9T4#ER!@xFc2+}qD^(G5K0MBQ(Hm|1$R_cowWtHNmk`)Q6aEqct1&Pg1^2}q(=jEBvL^lk^R6Dv?}SDnYKxb8oMC0wRvWBUL*iJL=wC~?jw?-Oml>L#Is6BU9|-w&sU1{XnYS6kqxD6Lxz-4RY{X4CNZ+eZB@gi^D@mE!j2R%r%X))>=dm{5lQO?;sIN9p6M?k@XwWqW(7%}3i9NVl-5=)LT8$QHOSl~pgUj-g=|HRI9D__Hzu)FV+~?eQ@0BLRb>jSkrwFw6(06dW=oUK6B32C2609wHUqfA<CCIBno`zz)o7zyK)OvcVGZ`FOl6M9kBFTJT^IuzRU<5a9=w0HD6_J1-W*&ms%W0DY!Ds+Jvj&mc8Z=$i|E@Xh$d-`tx6jBmNZbIOqv3mOPK_Ch#mS$i9R*xoZLRI@l)9tC3K#*HLYEhE!bgYH2!R`Y2<sbDPt1^0xL?8Ht-_=rp{<&(TMu%B>Kdj3(ENlK}v&`z-1HSmsW$NXm}c+xByit3q*Y*HG)2(FZJnb_?J*xi`G<Ns>#=2;!+@Q&IJXGGG|y+k~pYpV9Ow)dC?l9Oo`h8hY>*`tW7}XVZS;{Tcv>lO(h!qqZ9EJ4*yc-OLHy>dn)p*60tvp0F{wI+}wavng&)wey<vEF3>qC@)B&nK^$*^Kt)kDjRo}t$NV=%nRQi>gH;Jd4-nQ&h%Bpfcs62HfzSY0ZozSLXe<FXC!kLPDFV>B&>$VKp&aThD&<1_FPG?3bFNa7D5amkb<?z}YDM8ff>qO#b0u)NGPZ?3<!P0{sVZBWs<z0<O30WS_*(`b_$mj>KQ-qnBY`-qX2f&>a0y~2NJX20E9N4h_*&}(HwEJZyHgg31iNa>q%Z{n2Sfu?{wj0MADeSUAyo=;FAX}3TV|?6{%9qrpave78B#LjL`YK*GANGZvd$B7_67+hFufKzh~U?Ri<h!k+Orgrg()~QGf+)Jk4Z-kh$JBe%mJwZ(csZJ45>~Pyqpr~QkIA$fW-`-M>3n&$yeD=@fnMjqGXvUXyj=N^C_}afaimaB{d~Ti1~OM)KNkfF!L(0NL57xPL=?A01kOAKugr;srbK?=~J7QqGFjSY@`ix9xzvM9;B2)SXs#y={Y=IBV9m-3HFYRIzhs!43fyKY{B6ZaL9x*m9LZO6RVb@W|25<fIuY3ARE9<p4O?XVNE%-Q<ilF6fy-E3Bp$j+d?jkT?1u<Ng-83jM4w5NZzicq_80!v$ud@ShlGOayeBYE0qEf7C-0|88GEasYwu2Tc7|0&hthWMFT@ZYFGYpfsUrInXa;%RkS??$YEO9($sWvpiO0=D9Z-lsAN)Qv}w3GxMG^OIz{9~aLY|AWo{dM>MJaJIR#F_)P&LryP3dlz-TfM#kwt!m{$!@z_!SuHIhssNsuNd^J?Lq8ECC8VP{fkaQyn~gvl4eBy2cILI^DzDM0w(%+LcI8b3A&Wbkq%60)wSU`9!{Pz7u^X}MwmKBQz*S<(FBY77fgEQLuZq9&POW+p16#7aXkC0MC8W!^$8<k(0C5!Jy?k?R)?1yoVOsE`pgU_glUNF2YkGhZy2JXsFKk{4y<<YmStlhFSW0Kj5Y+9o2cka&WOCWuw=XI&%r$EH=H6bgx1mQf~D=MWw41_=76rIddu3t11z!{oUvBs-KSOx;L$3-~EG4MLf$QbW(J7maB%4aM0sPw}}N5zdy#LUJUss!3&I?Y}Ee&g6#zDU8JdVv1Y~NsL5Nn3gpFsJ03TpUnV32w31}I)!w3s>r*tJS#Jyf!uA=HeYh^e<IKIBV{oas}m9|&7dcN?P_37K(tbU31&qil!B2UW~P-YkssP5t2BVREy0XHfDl!{<#X}xZcf1^U@{(L!{p)$aSDr-Ix$!<lJ-)AjKXA!tRXN|RbCVzL76~02FjLV`*K8K(SmifU*bJKb8~_~S;6sv^FYMI3XFydSXt59qRl9(BLEayKu1NL<q6gBD3ao>A|bR%N^EKZI$!5MuQK@Lv$L&?e-3g;S*poO1j+(;sI!(>sm|&ARMudNk_>xQ8)O+E$_a7`YY{0ZcxF^_%XRXzMR`0?%V0w~N;uW3vP6RBwO}}PU6w62CaDX7aEWB5K=gvA3j;5M1#575Qwwl2owxXUfhhX(#V9@zqu^*zlaIFs;E&h{{wqb(5))03E2zdIEVU2<7>vz=SJ|x4wH6H(KW&Xbr7Km)uPdb<*W-w<C)IyfJ?`HSW>J-6d#V4ss&Zc;6Xl`ruAW|<GnWvaOr)-mQDMCWcsLklZPG%5&uHKY5GK_|H&P2q+ZE-^1^rKBKy)hPVO6T?I{SKw@>!y~WE+BW5Eixy?qO3{1(mmyO+aZ>18^xNc(aAs63-J=0;&!}f>c)4R;I|3k&~8RFHtc`lw>tKSM5kn0j38plGOG{Rg3%_$xB-!1i-LMg>5O7&C8k+n=*l2gRe@CWrax7&xg;)mHH>)DdS9Pm|$_m$t3i@c`50C>Qo~lE2;;kwh|5Oq$2Rib1Dn~@#s`10(@4gL>ZVpc;Dx_@Ta$a{IK%iNGS_}#2aA|*_5Jd!eAwGCJ+V$f{Ik5V6Bv&BV;yhTOd_|?;}(Jej56JFa2yG<_Dxx9OTkE{$v6}DWocNktkfWDBB{<j6zUG@`xNsAq=F5+*O(*9Mm-!6!;wsDc7LR<<FJi=z;EJitM>1yRL%U=2hA>z|Ra6Mbuay6qABIiL|g9B55j2QZEqxlp2uCL|SAiaNjS4!7qv6pCV5pq+|>DlRHKU(o^JyxzH7r6zZx~$V#9SQ!C_grhwgLV49%5RfS-Ha0hrx^M>No=PBivM3}9yOu1y0k|IFEz|>%a4U)v1jttZPw5NtoB;fwgOq$68j(|)=rA?WtMuNGB1ZgDrTSEYL^4tHhUQdrz>UaK-*%K&Ai#$u#MOu|>U}U|n+pJx~=+g$tMUmEZv|j)I<o^S_d)HO"


class Generation18Tests(unittest.TestCase):
    write_state = QualificationTests.write_state

    def setUp(self):
        QualificationTests.setUp(self)
        self.repo.root = self.repo.main
        self.state = copy.deepcopy(G17_STATE)
        self.state.update(
            contract_generation=18,
            approval=copy.deepcopy(G18_APPROVAL),
            approval_history=copy.deepcopy(G18_HISTORY),
        )
        self.write_state(self.state)

    def test_actual_history_and_fresh_gate_authority_change(self):
        import review_batch_windows_v1 as windows

        authority = activation.authorization(self.repo)
        self.assertEqual(authority["contract_digest"], activation.G18_CONTRACT_DIGEST)
        self.assertEqual(self.state["approval_history"][:-1], G17_STATE["approval_history"])
        self.assertEqual(self.state["approval_history"][-1], G17_STATE["approval"])

        def changed(*args, **kwargs):
            self.write_state(copy.deepcopy(G17_STATE))
            return None

        with patch.object(windows, "_full_checks_g18", side_effect=changed):
            with self.assertRaisesRegex(WorkflowError, "authority or source changed"):
                windows.full_checks_g18(self.repo, self.repo.main, {"plan_comment": 6068705967})
        for mutate in (
            lambda v: v.update(contract_generation=True),
            lambda v: v["approval_history"].reverse(),
            lambda v: v["approval_history"].pop(),
            lambda v: v.update(approval=copy.deepcopy(G17_STATE["approval"])),
        ):
            state = copy.deepcopy(self.state)
            mutate(state)
            self.write_state(state)
            with self.assertRaises(WorkflowError):
                activation.authorization(self.repo)

    def test_five_primary_predecessors_and_closed_capacity(self):
        import base64
        import zlib
        from types import SimpleNamespace

        import review_packet
        from test_installed_qualification_v1 import PUBLIC_U
        from test_suite_deadline_issue31 import PUBLIC_G13, PUBLIC_T

        numbers = [6068144159, 6064513854, 6062530466, 6061320190, 6045434332]
        bodies = [
            zlib.decompress(base64.b85decode(v)) for v in (PUBLIC_W, PUBLIC_V, PUBLIC_U, PUBLIC_T, PUBLIC_G13)
        ]
        rows = [
            dict(
                id=n,
                body=b.decode(),
                user={"login": "Zi-Deng"},
                issue_url="https://api.github.com/repos/Zi-Deng/FLOW-DC/issues/31",
            )
            for n, b in zip(numbers, bodies, strict=True)
        ]
        context = dict(
            designated_plan_comment={"id": 6068705967, "body": "X"},
            issue={"title": "31", "body": "acceptance"},
            issue_comments=rows,
            reviews=[],
            inline_comments=[],
            pr_comments=[],
            pull_request={"head": {"sha": "a" * 40}},
            commit_statuses=[],
            check_runs=[],
        )
        repo = SimpleNamespace(name="Zi-Deng/FLOW-DC", root=Path.cwd())
        self.assertEqual(
            review_packet.g18_predecessors(repo, context), list(zip(numbers, bodies, strict=True))
        )
        for index in range(5):
            bad = copy.deepcopy(context)
            bad["issue_comments"].append(copy.deepcopy(rows[index]))
            with self.assertRaises(WorkflowError):
                review_packet.g18_predecessors(repo, bad)
            bad = copy.deepcopy(context)
            bad["issue_comments"][index]["body"] += "\n"
            with self.assertRaises(WorkflowError):
                review_packet.g18_predecessors(repo, bad)
        for cap in (1000000, 0):
            with tempfile.TemporaryDirectory() as tmp:
                packet = Path(tmp)
                for name in ("repository-policy.txt", "review-policy.txt", "domain-policy.txt"):
                    (packet / name).write_text("policy\n")
                (packet / "source").mkdir()
                (packet / "source/pinned.txt").write_bytes(b"x")
                with patch.object(review_packet, "run", return_value=SimpleNamespace(stdout="")):
                    if cap == 0:
                        with self.assertRaisesRegex(WorkflowError, "snapshot budget"):
                            review_packet.build(
                                repo,
                                packet,
                                "a" * 40,
                                "b" * 40,
                                [],
                                [],
                                context,
                                {"required_checks": [], "max_snapshot_bytes": cap},
                            )
                    else:
                        review_packet.build(
                            repo,
                            packet,
                            "a" * 40,
                            "b" * 40,
                            [],
                            [],
                            context,
                            {"required_checks": [], "max_snapshot_bytes": cap},
                        )
                        inventory = json.loads((packet / "required-material.json").read_text())["required"]
                        for n, b in zip(numbers, bodies, strict=True):
                            name = f"contract-predecessor-{n}.txt"
                            self.assertEqual((packet / name).read_bytes(), b)
                            entries = [v for v in inventory if v["path"] == name]
                            self.assertTrue(entries)
                            self.assertTrue(
                                all(v["kind"] == "contract" and not v.get("omitted") for v in entries)
                            )
                            covered = [i for v in entries for i in range(v["start_line"], v["end_line"] + 1)]
                            self.assertEqual(covered, list(range(1, len(b.decode().splitlines()) + 1)))


G19_APPROVAL = {
    "issue": 31,
    "plan_comment": 6072111969,
    "contract": {
        "issue": 31,
        "plan_comment": 6072111969,
        "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
        "plan_digest": "7b5e7dbdca5891700820040539638618b4e48f38b0fcf82f297e1bd4525176f8",
    },
    "source": "Direct maintainer standing override recorded "
    "memory/FLOW-DC-standing-authorization.md: all necessary decisions/plans/actions "
    "and paid processes through the private coauthor-review manuscript; latest continue "
    "request selects 30-minute software limit. Applied prospectively to exact "
    "amendment6072111969 and whole required seed-source6072002965 after first dbf4e1e "
    "hosted timeout. Only enumerated profile-specific measured seed/explicit routing "
    "and G19 authority/primary-material plumbing exceptions; "
    "legacy71seed/default840/V2, assign/intervals/fixtureorder, every prior1021test "
    "identity/assertion, freshness and allG18older literals/helpers/history preserved. "
    "Whole X52551 bytes embeddedexactonce; full seed source9957 bytes hash-bound "
    "mandatoryprimary, canonical8858/80entries/dd2f9ec derived from complete "
    "Phase1071021journals only; earlier68318byte unpublishabledraftretained, "
    "no60000guardchange/truncation. New "
    "finite450focus/static+8580fullmatrix+3300firsthosted+300conditionaldiagnosis=12630seconds, "
    "allpriorallocationsremaincharged. Actualhistory18 appendsactualG18once "
    "preservingold17prefix; original Astra/workspace retained. "
    "Software1800/scopedCI45/live840/native300/900/paidextraAPI0 unchanged. No "
    "grants/login/native/cloud/scientific/author approval/merge or guaranteedCIpass. "
    "Operator assertion of direct existing user authorization; not inferred approval "
    "from publictext or fabricated humanGitHub/advisor/coauthor agreement.",
    "recorded_at": "2026-10-09T01:03:03.112713+00:00",
    "note": "Operator assertion of prior human authorization; not public approval proof.",
}
G19_HISTORY = [
    {
        "issue": 31,
        "plan_comment": 5900844013,
        "contract": {
            "issue": 31,
            "plan_comment": 5900844013,
            "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
            "plan_digest": "85a4947faefeb29410ed250b29fbf82c3e5dd1c95d2106dbaa928e0c9bc883d7",
        },
        "source": "Maintainer explicitly requested implementation of the supplied twelve-step "
        "roadmap on September 29, 2026 and repeated that request after pausing; this "
        "comment maps its step 2 to existing source and tests without expanding scope. "
        "Paid live batch remains separately unapproved.",
        "recorded_at": "2026-09-29T23:12:35.136186+00:00",
        "note": "Operator assertion of prior human authorization; not public approval proof.",
    },
    {
        "issue": 31,
        "plan_comment": 5966428269,
        "contract": {
            "issue": 31,
            "plan_comment": 5966428269,
            "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
            "plan_digest": "b170fe0c80b101de1568b96b5b07ac6f4418778ca585b53b5f6c755105511b7a",
        },
        "source": "User explicitly authorized all necessary decisions/plans/actions to complete the "
        "coauthor-review manuscript, requested this override be retained in repository "
        "memory, and confirmed PR34 merged; applied to the concrete issue31 provider-aware "
        "amendment under memory/FLOW-DC-standing-authorization.md. This is an operator "
        "receipt of standing authorization, not an advisor/coauthor decision or GitHub "
        "approval.",
        "recorded_at": "2026-10-03T06:38:52.487404+00:00",
        "note": "Operator assertion of prior human authorization; not public approval proof.",
    },
    {
        "issue": 31,
        "plan_comment": 6001819615,
        "contract": {
            "issue": 31,
            "plan_comment": 6001819615,
            "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
            "plan_digest": "2163be1844b7ef658d23276ce425ea8797ffa90bf776c6318109423288865e57",
        },
        "source": "Direct maintainer standing override authorizes all necessary "
        "decisions/plans/actions through private coauthor review, retained in FLOW-DC "
        "memory. Coordinator bound prospective reporting/test/continuation "
        "amendment6001819615 and two new separately capped activation purposes10/11; old "
        "grants, calls, reports and human merge duties preserved. No approval inferred "
        "from public issue text.",
        "recorded_at": "2026-10-05T19:49:38.315425+00:00",
        "note": "Operator assertion of prior human authorization; not public approval proof.",
    },
    {
        "issue": 31,
        "plan_comment": 6008093895,
        "contract": {
            "issue": 31,
            "plan_comment": 6008093895,
            "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
            "plan_digest": "e793c7623749aba94ae69fd623d7b406b4ec40227a3f71f271487a5019724734",
        },
        "source": "Maintainer explicitly authorized all necessary decisions/plans/actions and paid "
        "processes through the finished private coauthor-review manuscript, requested the "
        "override retained in repository memory, and now requested continuation after "
        "upgrading usage. Applied prospectively to the concrete finite recovery "
        "amendment6008093895: only new "
        "purposes12then13,300s/$2referenceeach600/$4total,Maxincludedonly/extraAPI0,no14,alloldreservations/stops/historypreserved. "
        "This supersedes task-level approval prompts; it does not attest diagnostics, "
        "provider usage, advisor/coauthor approval, merge or scientific readiness. Source: "
        "memory/FLOW-DC-standing-authorization.md and direct coordinating conversation.",
        "recorded_at": "2026-10-06T02:28:48.397470+00:00",
        "note": "Operator assertion of prior human authorization; not public approval proof.",
    },
    {
        "issue": 31,
        "plan_comment": 6009076812,
        "contract": {
            "issue": 31,
            "plan_comment": 6009076812,
            "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
            "plan_digest": "88af4787bfab768b81ff05ea7b09ab20bdefbb6e87572666b38c8b890ce1d2d0",
        },
        "source": "Direct maintainer standing override recorded2026-10-03T01:36:38.550153UTC: all "
        "necessary decisions/plans/actions and paid processes through private coauthor "
        "review. Applied prospectively to this exact original-Astra-authored v3 plan, "
        "published6009076812: observations only;14isolation-first "
        "then15tools/source;2wrappers,300s/$2reference "
        "each,600s/$4total,Maxincluded/extraAPI0,failure-stop/no16. Preserve all "
        "historical records, existing evidence/review gates and human "
        "login/merge/submission. This is an operator provenance receipt, not a fabricated "
        "human GitHub approval.",
        "recorded_at": "2026-10-06T04:03:24.534151+00:00",
        "note": "Operator assertion of prior human authorization; not public approval proof.",
    },
    {
        "issue": 31,
        "plan_comment": 6009865197,
        "contract": {
            "issue": 31,
            "plan_comment": 6009865197,
            "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
            "plan_digest": "6493f379fdb2ed9ea1580a38d2fcdc1d618608f42b30613b145faea69323f484",
        },
        "source": 'Direct user standing override recorded 2026-10-03: "I approve of all '
        "things/decisions/plans/actions needed for us to get the finished manuscript that "
        'is ready for co-author review ... this is an explicit overrite"; reinforced by '
        "subsequent requests to continue and authorize necessary paid processes. Applied "
        "prospectively to the complete source-verified v8/recovery-v4 amendment6009865197 "
        "after actual15 identified estimated_tokens shapes. Concrete finite limits and all "
        "evidence/human boundaries retained. Local operator receipt, not inferred GitHub "
        "approval or new credential/billing authority.",
        "recorded_at": "2026-10-06T05:19:19.280275+00:00",
        "note": "Operator assertion of prior human authorization; not public approval proof.",
    },
    {
        "issue": 31,
        "plan_comment": 6010775261,
        "contract": {
            "issue": 31,
            "plan_comment": 6010775261,
            "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
            "plan_digest": "e87de935601b8bbb50f7fa4188ddeb2bac4e1782479aeed00e2185f9dfc1cf05",
        },
        "source": "Direct coordinating user explicit standing override: approve all necessary "
        "things/decisions/plans/actions and paid continuations through private coauthor "
        "review, recorded memory/FLOW-DC-standing-authorization.md (2026-10-03). Applies "
        "prospectively to this exact published finite recovery-v5 contract only, "
        "preserving extra/API0, all evidence gates and human login/merge/submission "
        "boundaries. Local operator receipt, not fabricated human GitHub approval. Current "
        "hosted workflow must pass before source mutation or paid trial.",
        "recorded_at": "2026-10-06T06:33:27.488866+00:00",
        "note": "Operator assertion of prior human authorization; not public approval proof.",
    },
    {
        "issue": 31,
        "plan_comment": 6011162252,
        "contract": {
            "issue": 31,
            "plan_comment": 6011162252,
            "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
            "plan_digest": "f3d7d5d6656a3397d4c11a759ef543fcbb94c8fa49e83795117f8e922799cde4",
        },
        "source": "Direct coordinating user standing explicit override recorded "
        "memory/FLOW-DC-standing-authorization.md: approves necessary actions and finite "
        "paid continuations through private coauthor review. Applied prospectively to this "
        "exact narrow CI-runtime amendment only: permits runner repair before hosted "
        "success to resolve two cancellations, preserving all tests, CI15min, historical "
        "evidence and human boundaries. Native-v5 implementation/diagnostics remain gated "
        "on successful repaired-head software receipts and separate prospective contract "
        "reconciliation. Local operator receipt, not human GitHub approval.",
        "recorded_at": "2026-10-06T07:02:43.864501+00:00",
        "note": "Operator assertion of prior human authorization; not public approval proof.",
    },
    {
        "issue": 31,
        "plan_comment": 6012492318,
        "contract": {
            "issue": 31,
            "plan_comment": 6012492318,
            "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
            "plan_digest": "c0dbb09a3b7a03176988263aee50059c905af6750ef42de8042ff386e65b8461",
        },
        "source": "Direct coordinating user standing explicit override in "
        "memory/FLOW-DC-standing-authorization.md approves necessary concrete actions and "
        "finite paid continuations through private coauthor review. Applied prospectively "
        "to this exact original-Astra-authored recovery-v5 reconciliation at4fbe2af after "
        "verified full local/installed/hosted gates: two separate single-use "
        "diagnostics18isolation-first and conditional19tools/source,300seconds/$2reference "
        "each,600seconds/$4total,Maxincluded/extraAPI0,failure-stop/no20. No repeated task "
        "approval is pending; exact final-head software gates and fresh actual "
        "prerequisites still precede separate preview/application/invokes. Full "
        "component+integration review requires a distinct populated finite grant. "
        "Historical approvals/evidence and human login/merge/submission boundaries remain. "
        "Local operator receipt, not human GitHub approval.",
        "recorded_at": "2026-10-06T08:33:16.223093+00:00",
        "note": "Operator assertion of prior human authorization; not public approval proof.",
    },
    {
        "issue": 31,
        "plan_comment": 6013795098,
        "contract": {
            "issue": 31,
            "plan_comment": 6013795098,
            "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
            "plan_digest": "8351ce7ce762488c0f2447332487565c1c216e1f24ac440b237dc4d275277130",
        },
        "source": "Direct coordinating user standing explicit override in "
        "memory/FLOW-DC-standing-authorization.md authorizes necessary concrete actions "
        "through private coauthor review. Applied prospectively to this exact "
        "original-Astra-authored 58960-byte fixture-group scheduling plan at44eeac9. Only "
        "check_runner.py, additive test_check_runner.py and SETUP.md change; preserve803 "
        "occurrences and old methods, protocol2, two workers,840second phase,900second CI "
        "and all historical findings/evidence. Require exact final serial/parallel, "
        "installed affected suites, coordinator full local and both first-attempt hosted "
        "gates. No native grant/trial funded or authority constant change; separate "
        "prospective native binding reconciliation remains required. Retain generation9 "
        "and prior approvals once and original executor UUID. Human login, merge and "
        "submission boundaries remain. Local operator receipt, not human GitHub approval.",
        "recorded_at": "2026-10-06T09:55:50.305381+00:00",
        "note": "Operator assertion of prior human authorization; not public approval proof.",
    },
    {
        "issue": 31,
        "plan_comment": 6014789492,
        "contract": {
            "issue": 31,
            "plan_comment": 6014789492,
            "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
            "plan_digest": "e2fe530795df6701f43de54bb869032cd5375e92c9ecf543fa4e5a7d7830fb84",
        },
        "source": "Direct coordinating user standing explicit override in "
        "memory/FLOW-DC-standing-authorization.md authorizes necessary concrete actions "
        "through private coauthor review. Applied prospectively to this exact "
        "original-Astra-authored whole native-v5 authority reconciliation at8bb1abb. Only "
        "reporting_activation_v5.py exact next contract literals and duplicated plan "
        "check, new standalone test_reporting_authority_v5.py, and PROVIDERS "
        "current-authority guidance change. Preserve all810 occurrences and existing "
        "methods,56frozen paths,83009history, fixed runner seed/two workers/840second "
        "phase/900second CI and every historical finding/obligation. Require behavioral "
        "base regression, full affected/serial/parallel/disposable installed, coordinator "
        "full local and both first-attempt final-head hosted gates. Separately "
        "prepare/review/apply the existing finite18/conditional19 pair only after final "
        "gates/fresh prerequisites:two300second/$2reference "
        "wrappers,600seconds/$4total,Maxincluded,paid-extra/API0,failure-stop/no20. This "
        "receipt itself applies no grant and funds no full component/integration review. "
        "Retain generations6-10 exactly once, same executor UUID. Human login, merge and "
        "submission boundaries remain. Local operator receipt, not human GitHub approval.",
        "recorded_at": "2026-10-06T10:58:57.843959+00:00",
        "note": "Operator assertion of prior human authorization; not public approval proof.",
    },
    {
        "issue": 31,
        "plan_comment": 6035844223,
        "contract": {
            "issue": 31,
            "plan_comment": 6035844223,
            "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
            "plan_digest": "494abcda1fa95d346ae5f8a84b638202701e4756c3ceb24fc4b4f0a9f9d114e5",
        },
        "source": "Maintainer explicit standing override recorded October3 in "
        "memory/FLOW-DC-standing-authorization.md authorizes all necessary "
        "decisions/actions through the private coauthor-review manuscript. Applying it to "
        "this exact reconciled Q1-Q7 no-Console engineering and finite qualification "
        "contract, verified whole-body publication6035844223 and proposal "
        "SHAe9dcbd25045504eb0f9bddcec2614e93507bc4b53c4c74ffe73b292d301986f3. Included Max "
        "only, extra/API0; human login/merge/submission boundaries preserved. No live "
        "grant or scientific/advisor/coauthor approval is implied.",
        "recorded_at": "2026-10-07T10:19:09.955094+00:00",
        "note": "Operator assertion of prior human authorization; not public approval proof.",
    },
    {
        "issue": 31,
        "plan_comment": 6045434332,
        "contract": {
            "issue": 31,
            "plan_comment": 6045434332,
            "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
            "plan_digest": "05e7a6d54163067d7b0a02f1e3d61e995482e617233b0d973a41940add09d85f",
        },
        "source": "Direct coordinating conversation: maintainer explicitly approved all "
        "things/decisions/plans/actions necessary through a finished private "
        "coauthor-review manuscript and directed this override to persist in repository "
        "memory (2026-10-03). This concrete append-only S amendment repairs a "
        "deterministically reproduced strict-expiry bug and reconciles only exact "
        "prospective authority/history; inherited tests, limits and zero reviewer "
        "paid-extra/API usage remain. Standing receipt "
        "memory/FLOW-DC-standing-authorization.md; exact full verified plan6045434332. "
        "Operator assertion of existing authorization, not new advisor/coauthor approval "
        "or GitHub human review.",
        "recorded_at": "2026-10-07T19:42:04.886852+00:00",
        "note": "Operator assertion of prior human authorization; not public approval proof.",
    },
    {
        "issue": 31,
        "plan_comment": 6061320190,
        "contract": {
            "issue": 31,
            "plan_comment": 6061320190,
            "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
            "plan_digest": "22712ec0c4661047adc4b00960ca9f2dd8d162cbc243b658f7ce23a8d63fac21",
        },
        "source": 'Direct coordinating conversation October8,2026: maintainer said "Please increase '
        "the operational limit to30 minutes for this particular case then continue the "
        'task/plan", accepting the proposed1800-second complete-suite/45-minute enclosing '
        "CI recovery while preserving live840/native/credential/billing limits. Existing "
        "standing override authorizes all necessary concrete actions through private "
        "coauthor-review preparation. This exact T amendment6061320190 scopes only "
        "explicit suite opt-in, accountable new execution records, current "
        "authority/history and complete inherited public-contract material. Old "
        "tests/fixtures/history/limits remain. Authorization record "
        "memory/manuscript-2026-10/decisions/suite-timeout-1800-authorization-2026-10-08.json. "
        "Operator receipt of actual authorization; not advisor/coauthor/GitHub approval or "
        "a new native grant.",
        "recorded_at": "2026-10-08T13:49:39.161057+00:00",
        "note": "Operator assertion of prior human authorization; not public approval proof.",
    },
    {
        "issue": 31,
        "plan_comment": 6062530466,
        "contract": {
            "issue": 31,
            "plan_comment": 6062530466,
            "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
            "plan_digest": "915e46d413f2d19b0ee2b69ce6ef903fa787d777ebad442d4236781bb2120436",
        },
        "source": "Operator reconciliation October8,2026 under the maintainer's explicit standing "
        'override: "I approve of all things/decisions/plans/actions needed for us to get '
        "the finished manuscript that is ready for co-author review, please remember this "
        "for future decisions/approval, this is an explicit overrite (note it in this "
        'repositories\' memory)" and subsequent instruction "Please increase the '
        "operational limit to 30 minutes for this particular case then continue the "
        'task/plan". Standing receipt memory/FLOW-DC-standing-authorization.md; exact '
        "U6062530466 is the necessary bounded portable worker-import repair after retained "
        "Phase69 completed972cases in1323.075749seconds with971success/oneerror. Public U "
        "designates its closed source/test/gate/authority/material scope; preserve all "
        "inherited tests/history, source-bound readiness and "
        "software1800/CI45/live840/native/MaxextraAPI0 limits. This is an operator receipt "
        "of existing user authorization, not advisor/coauthor approval or human GitHub "
        "approval, and applies no native grant.",
        "recorded_at": "2026-10-08T14:51:57.337830+00:00",
        "note": "Operator assertion of prior human authorization; not public approval proof.",
    },
    {
        "issue": 31,
        "plan_comment": 6064513854,
        "contract": {
            "issue": 31,
            "plan_comment": 6064513854,
            "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
            "plan_digest": "63e7370374cab273b55c0188f92ec84d41b37ea6c0e43bf00da710a544e28731",
        },
        "source": "Operator reconciliation October8,2026 under the maintainer's explicit standing "
        "override approving all necessary decisions/plans/actions through the private "
        "coauthor-review manuscript and subsequent instruction to continue under "
        "the30-minute software limit. Receipt memory/FLOW-DC-standing-authorization.md. "
        "Exact V6064513854 prospectively reconciles the documented installer/adopter "
        "qualification mismatch after retained Phase75 finished979cases "
        "with977success/2failures. New installed-adoption-v1 preserves immutable "
        "fullpristinepayload+origin and everyoldtest/installer; separatelybinds "
        "byte-identical installedexecutionroot plus exactly2pinnedprojectintegrationfiles "
        "and real isolated deterministic Git provenance. "
        "Sourcehead/fixturecommit/hostedcheckout staydistinct, "
        "newclosedreader/schema/currentauthority/predecessors and finite "
        "focused/separatefullgates explicit. All "
        "software1800/CI45/live840/native/MaxextraAPI0 limits stayunchanged. Operator "
        "receipt of existing user authorization, not namedadvisor/coauthor approval or "
        "humanGitHub approval; applies no native grant.",
        "recorded_at": "2026-10-08T16:36:43.256974+00:00",
        "note": "Operator assertion of prior human authorization; not public approval proof.",
    },
    {
        "issue": 31,
        "plan_comment": 6068144159,
        "contract": {
            "issue": 31,
            "plan_comment": 6068144159,
            "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
            "plan_digest": "d53015047613e856093221f416a0811abac71feb40554b80deb38e5f4576aca4",
        },
        "source": "Operator reconciliation October8,2026 under the maintainer's explicit standing "
        "override approving all necessary decisions/plans/actions through private coauthor "
        "review, stored at memory/FLOW-DC-standing-authorization.md, and latest "
        "instruction to continue with30-minute operational software limit. Exact "
        "W6068144159 authorizes the necessary narrow bounded CI diagnostic-retention "
        "amendment after actual first hosted c2f1680 attempt reached1800seconds, failed, "
        "and retained only receipt/no worker journals. Explicit source spans and G17 "
        "authority additions preserve all prior constants/readers/history, entire "
        "runner/seed/scheduling/discovery/fixtures/receipt/native/auth code and tests. "
        "Optional fixed controlled evidence path and exact six-file bounded separate "
        "artifact do not count as qualification. Old failed head/run/checkout/failure "
        "bytes remain historical. Finite "
        "regression/static/serial/parallel/installed/coordinator gates and one new-head "
        "first hosted attempt precede read-only diagnosis; no blind retry or promised "
        "pass. Suite1800/scopedCI45/live840/native300/900 and extraAPIpaid0 remain "
        "unchanged. Actual16history appends G16 once preserving old15prefix and same "
        "executor. This records existing explicit user authority, not a "
        "namedadvisor/coauthor decision, humanGitHub approval, native grant or manuscript "
        "scientific evidence.",
        "recorded_at": "2026-10-08T20:10:24.307689+00:00",
        "note": "Operator assertion of prior human authorization; not public approval proof.",
    },
    {
        "issue": 31,
        "plan_comment": 6068705967,
        "contract": {
            "issue": 31,
            "plan_comment": 6068705967,
            "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
            "plan_digest": "a9d3c8e4275db2f356d81889b5ba6079519e8670faeca5b5da6a032bbafa3c9f",
        },
        "source": "Operator reconciliation October8,2026 under the maintainer's explicit standing "
        "override authorizing all necessary concrete decisions/plans/actions through "
        "private coauthor review, stored at memory/FLOW-DC-standing-authorization.md, and "
        "latest continue instruction with30-minute software limit. Exact X6068705967 "
        "supersedes only specified uncommitted W6068144159 diagnostic path/threat/test "
        "meanings: CI-owned exclusive temporary root and SOFTWAREchildTMPDIR use unchanged "
        "default runner/CLI/Make; seven baseline source exceptions plus two new files, "
        "with check.py/Make restoredexact. Preflight/quiescent checks and bounded six-file "
        "raw diagnostic artifact are owner-writable bookkeeping, not active hostile-owner "
        "proof or execution/readiness attestation. Existingmissing/failed/stale/unknown "
        "evidence, exact sourcefreshness, oldhistorical tests/readers/constants/receipt, "
        "independentcoverage and scientific gates remainstrict. Fix "
        "original60-secondinvocationdeadline, migrate exactly five named new-uncommitted "
        "test expectations with originalbytesretained, preserveallother tests "
        "andG17draftreaders/history. Complete raw X embeds historicalWexact, and "
        "W/V/U/T/g13 primary material staysrequired. Actual17history addsactualG17 once "
        "preservingold16prefix/sameexecutor; source/local/installed/coordinator andone "
        "newheadfirsthostedattempt remainfinite/separate beforeanalysis. No "
        "promisedCIpass/scheduling/performancefix. "
        "Suite1800/scopedCI45/live840/native300/900/extraAPIpaid0 unchanged. This records "
        "existing explicit userauthority, not namedadvisor/coauthor agreement, humanGitHub "
        "approval, nativegrant or scientific evidence.",
        "recorded_at": "2026-10-08T20:45:07.862801+00:00",
        "note": "Operator assertion of prior human authorization; not public approval proof.",
    },
]
PUBLIC_X = "c$}@h*>W65nl5;cr-(T<W-WDPBKD0awHZZ%6s9SXhXlo{xq-#v0c0_eiO$F*g)&{#JDj<kdAWX)`ToT{JQAQvr8;FL3Asghxc|%d?|%p`#_?d%I0%Ec@6sgr@4x>)!JnT5``uyOn?87U5$w(F-FDaP?w7mmb|37_-fZ@(?dB{nKi{oa-D)3<hdZ+y%{b!iU^@gKgS%#b6O87z!B@NS<mAQbXEO$WNaK@}x9{G(ee?eOdGO}-tG}EEueZVK_HJ!%an**dy9n+MEv~Eo(f4{YuC6!wVq?Ap&0&AD-L3Zb!FCs1H+zFsbRE_Ztj55M`vyNAZ}151eXzg5dw09lXZl*d?GALa)8N^5yX#lDD6Vje=aoB-H}d_DKfHN$9(2v7U-k66{mpKBxTX(HaM*M=&F0$lMTGw?+k1R|eQ0+5BKT}}tKmLqy8WS92iq?jvkPzyY!==Mx-C5`zKv}>>^8K#ZGXSO%Wk{rR%>H_hzG!pH`n$#|HhBbSGwDo-q4e7cjJPVv%yZ^H9IVFjT?`&QAc|k?6=sa)ioY=9XuWPyC(SX;o^A|H<202c$t@xS(cf>qm*s3Oj1)6O`R5bTAT*Yx4{MnzuJ7p<$}f@&&Bga@G`FONE=+Q+lS5F-ERB2S%=s?Z0UW_udZ<rbUyZ@V7L8p8l3-(FYvR;vrb1z+g?OP6=zwT*Wt^!T)69(NjzkA(wHc1tEMZ`yeNyJu8S^jt1>Rip^mezESj?E`Z`KXH8kyz=Ef9h)Tikp7!K>TT}fn;IM3UxZR?>+=}D`+?(p@fFZ#NzO_E30i!#caI_c^@iuxo?(kvQq^EB@kfj(ti=yRr7XY#hmJJUy<N$bp{QCHVZmp54!RY^1CWfi9p?pmj1Q`L>>>Z*y_I!>!JO;3aGo6)RS8$(obyV?`~{c!&Dd6x`vQAJ5Um?|66YKZ%;E~6^#%c{pA8B-5MRuyNUqH8S5?1-#(4QMp{vfX{!?~K9Dg3Xs{yll6JO>g?;&U{{(FU!MbwI5G|OCf4^-;c)xbkmGkofFwMD<WHBa8TcB)2$Cgr&*l_tzAEV$h5luvSn@^K-%M309k<}UTqv3(02}-{{j`PhL!10Pfq^uhv3igX>hT*0V9D4I<p{($F3Uu?Xi;VKLo@&_;_;ibiEG#XZS9B+wAt=hwsD7@I&}h_z?^KufuA`4>ID#Emn`;cjj)-jBG_17{_cN7Kn-Z;Lm}u_ZYNhO%(fQE8-xAf{)3+LBf1q@q(5;uHG@ZKYMi%{@8rN<F605Ez>s^i>0nULpU|JW+~DD#4OSR!qJ#BUdP8Y7`KPr?0VQt{);cuubS)47Hrckw;QY$l65r@cU{vH(IvX|7VE=Kn{_`1<DKbvOXU&Hf{%1~oxxhhfUiKmQ3l+YpZ60NhPT*+-3ssHZL?va-VAulm&m7JRBVG!Gt&~GGq5u3E!NN99<kng-Bh`Idq3NMe13OFTc#Tht+2)CyWoq#j&A4^5RR3FAe2dns5ttlrwj7By*-flHy(L{^2SL5LIm7xNBr@H6@VPn_Kr@U9`?4`eKI>@3f$AG3%wS0k1k~iIH|I_6!}kroTPt?i`?Ql_Fs&VlacF<_9XDuh$r9Bfzi$F$IZ^LpoJ*kvlbP$6l(t%y#L{8k{8$pl5k~QLGGEp?UJ~z%OdUTB+5)#RehP^#gIg0)m06?SVl>aH&x%XChN;I8Ng)UpTE3#9i0F9>9fn=hl}@@Z{A%zd-@7D`TouGznuKxF;MXFNuZ)5xY>?J=j}j&@~0kZci8P%XIRquH6Xyd{pDs$IuIM)^QJ5N(uPmE+9Si?s+EQFSU1~WVuzLdoitKXq92Zrs9OEH2SozfkR2XaX>meCkE-v5NCc;@kA<x0ZUHA3dy=^z2vxi!b7gtNakMw2fUR;=9kiG<?WBCK%|<CZE`zylJ`LU~D(bd(D?`-IB<L9Jc!ju}2<!Vm6bPQ)ii^|Wof*L^zZYrXL46IjQW~@>EEJWtnAFT~Rg5f7cIIvk!OJ^mem32KC<*_EP(18L64C312uM`W$GHmPb91_HSnaU0L$g{RcIM>d0y1z%5E<L}?9GpFU!7l`+q3!!XeP^{;PY$|mswE+A1+^1LSdnBbX8|{mnCJLW=)L0s<Z+3CQVs(RS(P%4P^nQZi=>Tdw`!P#zwXYFw)RQWtVzH&<<@sq<vS!S&~&i8;Pm2yo-kp7$dDr1=NI_n7*m|+>~uL<WU?!gw<&sH$#0Ee875>JdLo@MI0q{BrEN*a%j7>F}Qkz|K(X4cjYiNahoQlPog-FngT!z;;1Y0q8Nsx%UXb_s!iizI14Uu;W)(?>u3?jRa)wWyCg|W9zm83MIK|7>5!NrN|K_F>L|q)<O5C;uqhqlxXWu(;~GWZwoxCa@mX*kr%<@frie1EDofL3k=J#d$@S}IX!0}zc@$0DRaKgmrfvZhAtQ>q>$4>7`>t<?zO4pay)Gd~)1-*HqK=|EI}Hd}tb`J9^ba6Lu*ErFi;`PRswXhbZoTrDL&eDo!XB(6;6mXD{y%J?`h8roX?B3h784Mj3RRdcC$#L;^GNcBmWO2;ruMrns1J6<HGvEu-__4BH=q#54kpn7+;dHTL*)?*^rq>Vp}=;Bj<q`pY9ewK_02Vj_io9&k2~)Uo3yNwI!mf5D}rWEM&dqZp1{{3DBixi!Bv$*Lt@z~0Z5OFdWf<v$@;oU%LvLTD*GXU1n9G2z$wQN)`T8#6`(Po1uVGxv_0&z2I8Yi(i#BUm?j#!u8B&Z>bNZo=&J0KqHRoa8oU6^57ygj)tF6Q=b$4q)D4Ie1ldD<=5d);QB~#xh$7GWsI0TNFOwp!OcoajkUubgA6HTAaD`JZAd207HGt#dI4{$<tnst3*>yK0Mntkg6SqSSZ3$j$;tEVxbr1xgLKqz-Oaib;WXgCjanbi#K2&vIg3Yj)s_ENik~RdFK&+z97{cA*M4=aO2&B~h3L1g11&DvofRPl%ZcBjMiqPeF0P*5kqUGl}e0vZ2*`%j&avJAnq%azyEBe?`0`$oBHDodCD2LX=0k^?t1ahJB_Sox>Tf#0e=&#2ak&xZ)_G2EUbcSHRGCPF;9K$#NdB%d=?4r3P3}Op!u})BA2;4*}^RW4}fxZSE3mqu&2Dri9fei>&QP83gd@wP#cgwZ;Y}WJzTonxPY;BrNc(_|{o8Gwv`mK>)ZUF;!K<`Xv*tP+vtnZfm%BO4y?TNDQpquIB-&ox?vAQaPz!Cs)`djF3Q|83+gbZjG!J=zo4*PZG{=;U!JrGxd=bkBET5n<1)K&fT)hj$Di4Ta*`V@97WDG$ETxbmicbZ2z8)15Fh|l~qi(>&V9%O<R0ZkBd{}Gp^gP@0IgSe{G(<G_|e)5e2qU>?Od!_tOIB3O^0sHlezeL-0B^co9%WBhazl>L(W5O(8^3$@8Ae0UODj?uaL>D9+P8dKLhj`dPFw#?3$pTpIcJw3@d2d!EH_yZoL%=_{xxXWWV8ja%ii;Ugab6C!F}|49^$kOQhSMuCS@=|nj45EP2FHzA1Na9*dvnhbcx(XnyxD+!mSZ!31n=m3f3p@4p(CIzGns=P5@s`pO*cz;_%2A@h`FUZtJJBeBO%&lGfo~-Wi(x-H8;)Y)pmDs@<y#UG9oPSWc%7TveYQtG;{*vU9%aV;9@(-jxkhe9<pdXy}Ref+7eMKi&+Eqj9HyU$?;d3-8Gv<XTj<9YJYQRLrYce`c_+<-ruf?iRUImPp2#>I_$P<l3etsgT}(WXJmir6_@r7;hVST@19;>yneZS32b?OdfSs2GaC_>XAd4!#1bCpimh?J>DA`~kG=?|&&US{!nnUuj68jViM%C(^+?_S?5^oPnLTeM9S&^mhuXU7dnUs%6vLifs3IoqI+dsFg48Y2dFxf-={+rB2Qls;u-O9(7Jb^Qmjs_*etaY3@dyE|uBH<i>GJHedAOzrUu2NssE@uAuE<Ti4@pk$_$aO17ISK%P0w~`KLLcxz*eDi&tT!c?aqz@EM_Mn`^w3?;~k25W$LfIUT<4`iwqTP5#)*QF=CDoa3_8SYgCXQi@@|JRy@R1yqq&g(*wDX#1Z1o5@ZYmiVzY@3KdGc1#|&0Je`l}8rC#czS{>N;dIYRim#sJISD&w-_Y?wF#M-6cLIa1MCH>#oxi@kcz6ElFTwYJxjbL8CGT(9Pv`I6U%Yu8zW;D>d48p@7J<Bk;r-&`)wzE7{`}dS*U#TCf~)t>emH;r;T8A_Z=OF7`QKO1FJ7L%zx1Kw)o2W0+GMR>S-2^0(L%6GutK3lM39C3YGm$OC=3Xl$2-JBfy#EbBn?E?DWt)>5R_|3T-rR)pp~-BdLnkNa9Ep>D39NCI~(qs@e^*p28ikIXFSYbB+2vHI3qSBJ`LqDLZT9of5_k&N7EB<{<K=JVXtgA!|FQh4e=kA0-HHh&B|Zo<@<nwg`ZE@WO8r!g42Y+e79ILcT1{Cz?9as*uOJUN{7wPtQ!S-a;ZEI^@%Kxy_T1_(rQOy<4&c6xFCq`*S+%=*hdEDZZ@I^d95cWBuSRbN4%Y^Yej!7h{nG(JMs<fw-ZjD6D|JpjVO=rPESw2B@NYr_a-S~QLEVb21oveks;Phiw8lGgTf5WVZC?i5U~6Dvx+T^Qw7H^R94on#SzU=*xo91mJej82S8REtnxJY3Bsk9$kH9ogY?&=w6Fr=Y_hyqT0g-7jobBsd@0RlyJ0cIVX!YCR64ePyASaX{pa<ESK$cCSZzKr5Z-RcKfNI3IubHf?N4rxh5DF0cP-odTLzQULsM|>3=~B=nj{EmerqCtD^^UKL{IAjc{@OB-Oex(v2RSg%6OP-+5oqMkf!=4d51Vi)*AOQz4r}Cy#N0D{|o(dST{SN>Hq%w|M%^Jecpi?1|u$T@xhV0vbUjQ6K|?}Anb4l)@n9_Kw-dc4|jwuK&@ZM1}EcEf4|Vjoz5Faz|OPxHx0Rv#(Q9_+kiYcMBs!-yBi3pAl%>HfgBj)@e|S7dh3+#Sb~USo_(IQzYxsyn#Ftsv(-I>m`@9P%o}^I5KNGFqniHs*>?Sz;w}_Kr3Z#Y5KrGbGaN=HBv7P>==?{+PGdh;t>WXZxibVl=#U*D5k}t$xi4(AGgL~NA~J^!QS)}^PJX;usd^d7<ieV92yE`{46UmPW=v0p&}neF+T2^i43_a|cysu`d57HY><())QSgfVZEQUFWD7m~dAou*1PN@v(68uu!0U${z4$C_NRR?x7@(;Mtm#D{nnZ@20WqGQXVa0~B)v?=gGXQXJXs(#yLPqbWr&Y-J!9~ORs&OjBN+`lS|AV|A_fK+Cs_RSC28bmSLZ(gkiLF)4z>C2{MqH3cYm?MksgJa><dL~K`3NcLUtZF2+%Oprj-h^AdIpVre~U7Viyi<|F6^_Ajat_Dt`dfCq?NDO6R6H|CtV&qHfp*Y+)l_D4Zbf_g{xC8z3-%*4SfMnY5!g;m5@Nne_~_W`~|+&j3~+%fh=`y^<0fxZKcZ^gVjAP(GdX&?rZvMmz*EwC$}24S9D$EAGkXO>1WfWc_A6BUCb2x1JB)?ui4UAlDBb(F-f=gz0dZNsDcm`0YZ;MQk<49X6fy#QvtCSQVg!Je3BzR-`IFTC-XND}~w2Byb;*8Oj&$KD>T?{_g7X{KvN<M8sASSWLFqXazrQ{xyE_^5XJ`58q$mhgTQRL+{noOS;PCnHBuq&5E?VO2tbj+r*tQihV66h5o{00G7Y(LJBqHu=E&-wvn`hvkE-_j2GdMdt1?E_gEE@-gCQiUOwLKN#5Y3IhX-%BpY$XJUG#adc&ND4rk+SX6OkH{pRUkM_h_G+0jd*zPvz$DOPgZgiYt7SwivTjuUcV=Mqj5pj_j-2X%*x)&n(pcK3p!$hJ>UgCAF$)$QRHn=E*DC6@3bakP_@*XJLDhxXZOl+Y67$#T7FS#~)`gY2zPE*&l{i8R5M*ctM7yIr$K1cbj^I!vRiMdpb!!$Br@MHJj_!SV!Ulubv^!tge?+uq%WH_dnxx~TLlQhaXFtPr-f2s}|cK^%|5juoul5ueZjlDqMNs)go<4q)e~3+oVwf`$RxVjF|^7k>_R%@;mwSi}@xq&OJS#eyAHq#NXw7%Iz%U#{Kj_~><Xy53%UpW>CXL1||KV2$O!J7u|W{;RGbl)E6ANKRAyvH85(Z5b8N*Hlg}NS?5$YxMZUKPpHIPv&VV0yLz9SxiDaoAtojFtF&%!zc9fGgjT3?NU^4h>zR#w);fet`09lD;OxgpaOS)ul^{4zoFQ}<LYlFyakJ}k`XXLszjx%7kgW<?iTU#;^M^{kiq9wM>vH%D4}D0(9^ExbvC!{>iWP@hlWjc$Y7vvS|->UwlmPSybn{rOn6GcHSB6N@#%N5#+g^50auJRL9QSTRvI2J(%Gp!Pn>K`zWt;c*gHI`TsS#-OUfBHZb8*xOR-y@1O~@U;fOT(arOO~yi@}>F8D1aLVcw~80UC}hXJkbw|hd7LcwuT;)4Y%7sqb)GroXx-N#9VH-u-26~G-}hUS(q7<gI4;D%#Q>Ve=i1aeurIDnd)<9@edla+pYB_18-Tbn4jHk)fw2;URFnil40f-XRIrOV$TYXj;H8%p5z%qa#=vcQFHvVpUJLpG*JJ`jJ9v?70%#d%^%)51W@I|-KX18Uj?lk7bQx)0>Fv=$|nx+eFK=|eoqrepc?f4zV6nmEl%R1!F@=N2#)0Dd7!V!;yc6=c;&DLznttKDhvf`ADfxJW*+$skI1y#(CJi^2+Wp{&x=ZOKnU9Ca3GWJI5iJoJ|N{ku{ux+T9Bt(|d`l?`$lu#?v|LJ9cKeBJ)lu#?}K>T-uIJHidg)?0z2Yt2C-j6>4nZ)UfZjkO14;~DPEzfcsCSc;UsamW$pd@FH@b;EzTKoB|Yp3a^X5)J5=mM{jBbJ!0>=~5tfZ|M8{P)FWyRl+uqW{n$jOL#b#xeh~h!?QqHk-uN^s0fbiZaEWbwU5|4qS;gAK_U9(8*B1}U7RQ=_HST|K-(G?Ww6M5N56EW<*9h~l9EZY4sM#y$NL|#`JuZkKqXen`eg3ezf133G5L^yC0FveB5}G9p$r}KnW^n=cSZ5ptMR7c7vwRJ7ZQBEvVct91ZP~Wdik%0#1__xB0sqzId~v~B6#Fp0S{fVheZB_1jQ28%^fKHN-+f~!n^5(lU`RWKt<Lp<VY7DF<hD54kkTh8un5>wIWMo>!y8Swp(z7d?8oEGeIUSaM&X`;e`c5<-pLgpXf#kCQC>+6tTzV+W}g-8A0~FFyJ)=IB_qVLn53ATO9~wD?)4!t2L-|2~g-sBI1Z5gYGrE8}qZ?=}hAJoRY|961o29IBC85WKKTfI}kMd9v|9quzvBY;b69%*bLxM(=0?o0#}GA_`g@n=aBH>i&t+xE}uU;3of3I_K5-}GJ;ww@{qGbs0G&ic0s~7i||~FKnP;mjcv=~w+~z}Qn5238g+9i36ipAMeiwLGNXx+Bw%^HA{*fIlBF-_%*v@K?qJ7`BqVTr4SOW#L{c4F5q`LQcH|H6pqSz_wo=%5TGC?ft$<+f{G{fr-NUYbwXC2dm~_r&tghB3<j*+VPqItng4VR*3?(a*{A5-D88$gcVkpG-!LwUYGr9mey<XTzq-J^?VVI{3=hSi_K;%hZ0qM>9OLISd^R4g!L3Xv`L~PxWVY8<s$us-gnTZs#eDDtY{XF1zbeF~4JoD3FkH~!aC%Fxs@_#J<ghj3BzbdW%V<Em+iI}m6%0CMSI|+6}LN5E}elo_?lX<t=kdsi?*{EOntpyV91SiV~u-~4eeGEo1AwP?F<8HUT<mo<$XV{-y*=zKxQNXaZM4fi9IS%RoqtXIj>5KtnDtXQ(ePXq?8VSyVwm)P0R&=Y)qu}!uZAv)qULxx_SgZL%(SyuNM2f4;!7Q;0EUDcZDb0?eW}9m=h(+g5uwQOA8ItUua5>s8HRK0a2P7xhkz|8MY>Se!cP(&-wu68k*DcV$C*ox)Jlusg%1oYSn~|+qfw(dd;S@b+clah{BwK;Sq&|*Xk+P@v0~Z*&mnxNRy*>1l!$C$4XCqi^ypg#7bcnFGLA0aA4wjs<e2Isnu)<%rt@WP}&4HJIhe#)wF>oSXoedeV-#rI9m^!E85eay?w6v3x7od#45#U<SQuu8Ql*v^wv<gia8=BX3HpSa0c4>o|>@!sXIIr|@0leTRDU+cLl({K+3vj?I1!IVHOw3QgDuz$3J{3od7cSMbiZlG(bK>ZfZ!(5uU6Uxh+4h=qStfzi+;oS-S~GoVTVb;`oaWA#KMl?$hp)_SLW*OPDUwX6@{0`)v+qYm44z&eTEvY9_aqW}FAh20U~--W;?kDTzRlaWVF%d-cRHlBMba|lcM-H%-G#?DQ?e3BjFWJ*AcOZGTTc4<o)nB|h85>E(K9eKkQ{$ChFu|*O8~}xv1S4aFsD+A;ZlI<6xTK{EF8hsd&=MUrEh3u5sc(9v^m%ii#RSDpqwWMXpW!KvLs)q&qM7;-FitU|7f#u$xS|)JT8w2*RUGTtj*_!J+t;(qTQ?mF?8ZNoI=g)33g(EowU1&<H%C@X_iSoWnEL^Di=3kXYsU52EqVqvhQt0fIcXPB}Z~_5HWG2aVjTcWZ;}@dlAPQdg9Puk(aJ22+E7r;>KreeDZUE!tX}s6lbRdorsHg^yYRec^(2pEmNul%GKRTo(nmQSsuE}UDbT1r!Owg-|2aI&ckq4_XiDZFZMDnub#bmeff?PAN*G?VS(O!`u6R+H$OdnHG5Mk9{!^5c_l?wg7{y0_vYh!i5^pV5ZArQIdMrL3v<85Pzp=vjQBuisDHKrp6{5$aG|2}koREea=MvAEgO(8mC{ZNo5pVl{cvsLb{FTt#4b}>$Si{JGA`Vgs;24FBrS(}sH3Vg8U7jKq3)}!>8fPt`=Y4(s7X_k6n)taWs){Y)f)qWZOR&wu9Jj>Q4*FyN=s--LQ#f9f4Fm18th9MfCB{CVxNTxc+q0>)M*+eMI9ww-$lt#w}y&qqOvZcF3PK_EQ_|zOw*-N7Pn=c<x$>5O@qb7anWW@HY-e{{k@Lk|Ni^`y2dlJ6K2br#{o7cqii_<^Y939V#2$fz*4LHITZxpwYh8B6~Rp_5XOKd!a$L2l9!xSVROEmDPxmeR$5?mkf7q0!m9vMQ+_$+mtfcSHoL&|l>2gW^7Rb1Cz_0e2Ljnb_wFf>In`jAZmka@n&Iji#I2v6X`cC7bIE&6m>~OK>2Gmd!J<M%tGi6UbQLEq*+TBquA~A+>(Zt~IC71t<F4TodCKuN)nS&6!`J73zI03-zKk>clZQmW49yzO#-bZhI;DN05RaGnFLM7yVP9BnGI7acK;9;zxT>x*))n%$iVWTFd1(<R0kPJ%rcX0|5*wD#fn;xUrOS?UmSD6|y`FBc7@4chUnNP%ZQKIbe7$ZPa^tWGI;@*(uC85U4U#<NpB05Y87_xpjeF9^5=S_d<+AHzmJlbXX>`d@nqFk{ZoDwFPo0nFP(BGXyJUMuE(V($Ij!i3cC0pA+T#-MGEw@J8U;EQ4W^eneOM_yr*$(EoeO4Fd&KTBZ3J&J;{1TRhIMvEQ&GGWDk^Pe2daHahjnRxzLr7C+{-dvJZ>W`S&G&~txW39<Df}n*;0An2(AaSX~P@n@*56a^nRS2X_A8->MC+IpYlV=9Cc2NtK)^KIdPPfY0LbWH|4tPYr5>sZSalcuZK2kJ)}#D6Ci}5*oTsv8BVTB2dw%~d?DX1pdyBWO-3Oks+hs4@aeEAy&BkpbM&;-^4CtPGHp&YIb8^YH3GgR4kVl6o%ePKo_%=t?)>%Tk(a$;-lVvepGm5oW1<Dt5(-XjO(;1eaYb(@!}+deN3Zkn>g!)F!lN@Oj({<PDTtwl3SjKPSRpm%vTIIKfH`tUbjgGWGCQ5h36@PSXpj7a8W<O)8NOW6k#X2+YV$B#iu3#H=8Hx(Uh=NJx{cF6_L=07Xl}i)Gu}-Vr|Kq|3TPV^)05kb%UDQG&WZrZhO*^NlY`2h=b0Z(*nwfxk>uPMoY+J);VVw<eW5lCqh{>oXIdubcRBWVN#ervB%6GzK(q!|T}@@AW(s{Ul(cbI54MBrNHdD@rvPz%&+$PYd}`Q<Y8||y#Lz4F3%we>@H$StiaO2RQ!lV5e{7NFhj*_QlQqIjCo)@%8lyh!)8HcuPKINX@j?z#`iXIC?~DubL%Z+zL?s~Ugp)-mOK%gG7tc@kKkr?kouds;y?bAGC?_PK;2%avU}1JT%F0|RzV11NhRsf=5IOd!r0=4OBrn8YZwmtL(tJaVk>dmcvG<Y(qX^nKca2qU5GYjlXp5gU$yrKS1f+cQXvd~)-X4g|8SH69SUr4hZJHXzTx{8zf8lh9^Vx+;f1z7tD9*|4VYC?E#{ZROtyr3c-pFAf)eM?KqiN7WUtHxd*kYq!D~up^_LL=o%+R&yU}aA{qB&KbS8Tzticmd^5Nc`!hFkHjc+bZfnm!(LGG89FSuZ>#t#YsF0PaSX|EhFt3Zlm_1GI1OKo~evT(5o_{J`=mB(TnL=+QK{5&}5*)j-BexDn6)7C2#wvg2S6#1{0C7Ydjprthene0HzZQSz`HgiNR%B|nTu1yF1rvJpdS(=AX#26Ezi%J>?u50pFM^8KX$hWZi;3UE=R##hs{LnLik5|1Y*7q(KJ*6gadBnxkzaP_C}(rC0fc<;`i&x|c0B*|I>cd1?Ll#S%)lSf6IbT&Phqh4A9BOVJb3G6!NoNv#Zz2w#Z*|_v2MN-H>+;I~E*2YkUcb=}Mhk%s$o1t3ajuQp`^j(}p>~bKQc@pd?C6}^m<1mqAB=HJMhLO<2lv(%Lt7o;lPDw{M6zbY+H|$~d>p^WcrCtBRy8per2^;D1Lnk&+&IwmG-NA-)U8oguYsne;F8u340Ir0@rZj7kys)Fqes=jnAupb@okE*xG_j}dmi7O8`D~og%K4HYi^Y`9V3VH0{r#N@C8>uEIG4~Wslf|xKaXN~niMjHF9}e-oSHF&6Abrc0C@G9Z7vt;lQVF|8-fAF0VA^h^DNa$ex`~2e6IF5y<Q^|_cJL6E~KDHf(rdUhiVL35>$SXh6xJ=Er-nm5@#@UCe|(sWY7g!bKC9RY4CnK?7z@DBuUi<sjVM%>4niY+vW9YzdWo6h;mLS9)Y@s1d<Fhsm_-dmsd~U|L5DM@845i?A6<+FVEiz?TMC^v>QLZYg2C|rX|Mj{DS{}X47Wh{^jzAH?QA5z5Kxv@?o<kdYaDOu^y*aj$Yxfb-i;!>4wzmnF^ushc`c-PuF1b(qM}u)mk?!jLkJ?;fpoSgyW*NRxl_+eZnz!h`V=(VX$V_)AyIKua>viLmUL@AFjZfj4!gZ!dcX4B$!WtjKn4cj^oe;!?-_vS41NvEZnax$Zw|lSIQ1>aSATFb6`L#4qixt#V~jx&qjTGGs|4lVTE4BVCbr!Y?TAK&LuOh+fowJX0M?O%{!vVGvD`ayV^*uv3Q)d&|T8Ik6NCL5S?VipA%-~(p~8`cieJ~ivT5jL~zh=SmQf~o;a&)>YQY)b}99)qKIUi*~L{6ow<5%@BP{jy+M2f^hjNaxBvLM^oHXiavL6;F08&2i$KJ#8`-d<vMec6S#K#)HlEF%X0qS|zFJa75!m@~D=z~GJ#qn$RbE9lZ}=PB{Eu|oKhommFZ$iL>I;Li;ryFTA9|q|k`@9xbca?OYb%Zpw9dewMzrh}TZcaTW>}9}pRXdLkiWs##okMgF!4R{!bqns$J^~E!*;l2&R{oN;E4U<I01k|h!Q^!ZEW17FL?fbn{iF<<dM1_n33v!$V$2J@=O&jhkpJCg!}!dV2_jM^g$`>fxB#k%{+NVJQq^pC3!0ZcPWqUR3k4!v{FybE`i0vW=jmDxSkwmaU)5$z0Khno!$20;vL0P)KUg+pDKjEY$<*Xp`>XWSuRm+jY3(ONHn^zS}Bh7JBulC0fZ~X19~00Dl;-+TIuNZNij3zEoDG*LB%5$JVnhlr9oq7?Y-Mi53bX$kyYhVBfJbLdr2F6P{qgYiDn8`QH3kk8M-8pDYeN@sg@(vyLhH6<3rM=T4bhz_%R7YixrJ;Q&k)M>z>*ssN5wbC+-nRQKifVuq#FSSHviku{ev@KqylzhZ4>2XAKZIHOw|wSO5*|s5XUg7^(GzmkXgiM}iT{r#Q^cU%Rv@SZ_D{Yuj%^zyOu#jq1;tYGQ45QmZKn>t=Jt1yx>yIBkVvE3U1tN)IZvl%?V7QLzY%NJ)fyc9G?w7}ix5*+Qge7jbS2e(`q%K=|3v=c15tE@6#-gi&#|aLV1sXMZfBFi*eiD8|1F|Ia+m!XKNT*}?Jj?M0*>AU1UF4m%1DjY;(1fB#=edgSGMz5Pl7vl?yTv+ZUCn}?Er(lxg@-Rq53G;^`*idID)Qpmoo#x0}@K+A%rCT#S53)WadsMfXuN|!Pd#GFvzeBp$A*8ZXEd1#zY*aUA+jUa<(Rx|mj_pVm<kA6&B_Fyq{iEH$DHA-@#jegx6ASbl5kn?lMs-&gCKo0<l?Y2ToUWxRfwdKf@ME2T+U{eGgyPuMWh>EJG7B9iOtweC7kO#)Ab99P1$QgiY6>y%k%Z}h=m-!a*$#Y5%nFUfh#?EX$q_ZhkXitnki%d$66X{!yrX6mrEo)8g(rZU*J$PguxI_}|EF!Rsg03U~@O+-AGUW>W!!B(nC%*)5DM#-Yo3sDR$uG<0lK;VHU+rG<OHe0$QM5ysb#WV4V7RIrhCFJrqAa7h$<w@UlCG?>IBz3k;-akLy3Cups{5o#Te{QK>g1Q8D3cza6jj!@S=B~OoKnQQ%9=iEqC7HfmUII(ugW_-lIhDb8LGI?le&(xrs3=Tw!SF81g0(eG%d<3%j=?xx^if7siE!jq3W9?ZtAvZA`|sR8JlkCsbf~xC3%+iN!pr@Z}uCzr2G<Oaom<eoR+3CRh-6EkE>Q?7o}-F;B@j9FN?-xWQJB<H{@M!Oc6y*-nVfa2ag@a*Q3+8W1se29^sx{UuS(9bvTy7L_?mneb<;4TQ?MC(U~sFqP~O-ZL2oRKp$~g=6UxI?)Z4bk?u=<fQtwuQRg|xEy{{&=*zron<lNJs_ly`ii@sm`nn#Xwxd2_QD2o2$RcUKcHgf>^7u)IwC&=wZ`vrWn;4tgm@WqO^g~y4Wty0-hzwC{Ru_4ire&1XWo2@#tE`4!yW`Yh=9i$Y!7|CvL{XeKU6q=6NQ)xM;$j%u&Lq(EMU>Rkt1r&VqJf^lH$g;gk%Hn4-{+t1Jo8JCgN;qo;DTu!nQTb1wrbj>tKyQH@-;=B=4Df)U6IyhT$LuTineY`yg%Um>e1&rZfx>P5Di@q-W-y2h~g-rLxU-i;VRiM#7&u$rZrI&RoKlo>Z+=3!3b5C#Wh%^N*}+?ta}LEB`GPyQIuUvt)087ZkxXC%fciehNMlggfdQYkYb6~vB|2W&yp&saLQ3Zn@I9bWxh&7YsXznzu|8_T)tRVF42tL?$Xg_*RYLb!y4Z@;rB!Wn!g0E*iZAAEc}W*{1qmA;<74m!Gv;Rl7jxKO={Yzs+y)MYCJ1;JTs8ZX`9tqnowiD5~N03o#6kbZo4)i<{9!l$(w&x7EH^qsaIG-NvkP|d+MU8JBWZ5vbU@I43a4uq7LVjM>UQ%iSjxn-9bI`vM%i-gVSr`I!%(QO#b=R%rZ=PKd_cGBPrQ}?1m0verW0#MBJrWjn(CG*Ax->8B1-Fs_up^uCuI%0;6WUtQva!?_XTYv^{A~OUm(ATqbD_We^RJ6hqpj4OA(K)YOz%Mgw+isyZ7AV3VxM)1rc6&G3Gbngsv(ol81Opra)fv?QV*P(s&~)HBnxX;EVrqRyB+GhJRJ5O&Zcc!Ss{N{TYgq3)s2swnQNp@8Q3ol81O-@}TChEk@Xo(`d^k~UBJq>qc-R5e&SFVn0g*^xr$lxg2Wc1D9qp-Jl&%Y$s`@~r!s1pcinqFg~*Plfg5F&)}a4N#qF0@M)aeFC@xv{1xDlta%It;zd7F;#1N=&H6J8vIk0DP&D-Al-ilhrNt5pQQ7ImO@(tJ<}y7gK!1MCq)f2rL6O+?fPbbNH=K~nI8L8rg09<4~?BfUD5Y|Q8m=^<HGwlFJ(HcoHqxT3o6PdEL*5Yh@KpP13N{1X`y|G1llC6u~kV6-imu1VVtxDFjvzjkcZfz-?^fP1f3Jx=QaK++rEjqJRfR$_NE+w4y(Syzguh?@g8WZHwhGhiAsPr;3EjkE~AviKB|A;iXPB&K{j6z(V^j#fMpZHm!<<s>A-0a#RZ^BSwQReaR;Rj?Mrd`8vGSCT^9{~1*Dq%9+~JbNXfY%g;C}#7MqwbXw!n00Yvj+==-KGaXZLiXizNHBmncE-!4msrh^n{tEk2Qx+MA?4F59DeR3{I_Ef}KC3Js^1yp?uiJSKTm3<4UA--=~U@pM9ILb?){T6zBfCMUvvTY5ZCotx}xRmLvDq^rIA?*Rcnnh8T)j2pDx~hO`fLI=YadVtl0@<7ZK1DDQAf1a2paV3N<9Lgz5f%T>ujnB;S1GM1rN4l6)3mDUhSWt2R85_8W60sEHv{-7Ppb?}Rhhc4Y6F|B#2WJ+{FXrw{4NH|AChyG(Slg5W`uMh;1cLefQlglR?MS>^lRNExG4}H&|PC7kw8~NnG}5i#et;(DgQ2V&X38tB9$6P#9kfXFm9POCG1BN0}6JK$7KeS3^ozW6sQc+BY9ls2{C&MgAy{mj&cCO-=i*G#)VJMaTN1n3bxF898E%P(qRL_NJt^(AgLjufunN}Qk^#7ax$RHxP&HwSj_N!7_)hu{4V+_9--(sDtS$$H1c!+`4m|i0p|maB{dmH(D`^9&@skZfXu7Jz*LP|V6p_F2f`t*Bfyg8aVY)|*YuF4<EY{_k=jUG*gQb4z&tQ1qsYWnJit5$r*|+HU||Bi!=g@Lur@u6$gCWI;S*rUge;Zcx26YF9Y-}UiS-5mL;?%4g}BMnI*n^kQ;yRq%esOT>I)zesIL^X1zQ-q2FM1If~f`_)BTG}Qo4>~QX8=#djl~H%I>RzSgxsHmBtY?EdJ<HSioc}rF{aWIzS3w!Fk?xMbUzgVA_@c{DNj#*bG;h%_`cS0^l&MOxf2|IWVMg(U6r5yivtTmC>f*=D><+K6EKGFO*x}4{@BE7T@|El<jAM(_(6p(h0L!gxvtqWB`iwP{1&+T1Wviz>4l*WD-ciYH}j40qmIp)^;W6ERH&0{QCDTCifPTFySN#N$9wZBLF@yGtL1FjXzr`GH^KziMX!FVMa!F)D)oIWMGd0@K6)Cm5JJa+8e`*Dg4DGq*0SBVq_LoFo~NE2U7x-c72%-SQcz-7=zI2K&P<vi<T5>RD!5r5w$=-(DX1IzqT`9b(lQylVXXBGGg*FW0FZ|A^-$S-=sqlr4<ZMfYAiH3jEyFu>G-VRa8a|46`gFOQ_DVbg&yl(672u{^44PdT2cip35R;hKgt`R19tbJ_V+MDvPVMr@r-3+YecXgEMKK;(IwXoGD?2<S=BLHjP_j{?+y541P$Ff>=Z#rm(eO#KbTP)3SyDs!aof&twolP*}icU5eG^X+zwV<yo0U9VGYA4(-<%{8!d<b#zz^#p;9>mS#Ao2;0?yoFLK421qa~lBj7wB+!{@)s(OgO_Eg|1ocn?836!6s{qUA(Z9Mm1-pQWcwh|^i#O0yc-Xj0dOR?U_Ob&Q1<4dyOTw_J@}d9;$|9I!fNUwYFNYS62B41NYq;k(ZjN10R<M3xI}rM?0-_-YR#ptA7&6l8Pyj_2;f#tp%M<e9ktW4kMMBahDY2;u;C!9`HqYRbM{`?Q{)xz;!)n^BgrY2PhdLVwm3BGRPh~B(D9Nx_bq^~8KskX;VGJ|{DbI{NZh4pd=A|5G)UvRlGfLRhs<I>o%o~7k>bfikY)n!Y5!5A&nF87ioF4VyGEi^_EZ^4=uvwQ6_;~>>`rDULJXl7-+Mp&LA9@IX=tkh*IBEw%q6usTnw3=IC;;^;sLTRfWwN5HyQn4Sr>UW+x~eJS-{(r5`{M|&C)NKhf84*Y7|$!m^iu!t^2+^=HIW_q{_^SNIb(@P!9*&Kj0(?N0EYu%)_q#Uz%w1>2_#I~^<5ix5t;1`*~|qEjRDZ9V24#{Q`gz=Ur|1-s4kg?z#JqChX(9nQcwlCw;GcG(ljlEOBn+<8;~vGyr@b5)j>#Dm5FN;r?8S?la{}KMTJ{YlGV&ywSb%gNDo{TQ`jSI2H58?UWOVf0EAsO*p{*}d0CTTQzoEm;MJIQSrMh_<LdL+Q~$(v8D~(#0E<0NeL`biN*d}^LnAlj4@^xJwV;!VgioH6V*nD5syd<Iv$9E=9%K*P_c#`Qc<bY4<(83BUIYwps6|*)4P6ri8^dM-V1Od1=x7>HE7|8zne8wXFjc_&P*o5=EzMr~&5M{Cq>~<G*E;^q0ESG+rs#^K!9|O5DAKHNpp;=e!bWPK22yD5D$St|>KX_N_zr}WcYx02Z(YHxp*xWxb8gA3+kkEJDs5ZH&kPVHs_}d{m=x$KN(<9rNgX*RDHaHRN_&9JBsQ>8z`kEmgI`;Mc9Ewgq(lq&H%E*To=F$wQCE>mp{|AoRte6euN&BLeF3`5fHVPps|v~h>JH*9&0Eq_k3-6@En%X@GG&)lN(TWL2BZcWY+)qkR5DCMdus4R0_=~INwc^BBft`A(zZ;SHU@Hu5}1*|Z!HP1lRy0*%cX0p<mLoE!9jIVo+Zm7t;!{2WWB71Y*>Qm(-y`>k=AvvT>i(&$@%N&U!Atc6E71=8RG6v+{n{b<#yZMPEu<3H`G*C+9{lz@F0^q`L{7HUD~J3(UrXTMjAMc<I!9NTbgmk-Fv00oqpRjLm#wqS6ioOkI>a#2dLEPze!fTEm2)^!uy(2>35V9%IkGa7;J9$)XFco!6Mf>7?&rga6i0zE*I9uG2BYN(T*I_tJ3BdQ?6VbMyC*MG&l}4Ugi<bugo>wa4PPTaxmNRqllK;Z<uEQ9qdRLC<WOAuhxZo@-%*%(*q~S-92#$NLN!jJr_C4Xh^Lqpho6Ix7*#>uXM!M=i~HaivMYc{BKWhKjGrv$hF4rE_VmB;AFb1?I(Wqt)?^f(rl6{cQ|m!g{jhlhv$q^*3@>j556>`?F`#{Ig72N$MJc))}-3cZV^z2y=8mougzYH<81om=sH_Sva8Ke@hW7wE%l98ltR>TYj#FQmbl3}rza<ueo2MjdW+k1+7^?N%;WlgbG%iul6cetWdcePjNj+}v9`;zX7BVXYKY65GOTHA5LVCK5pBBZLVB|w7a}ZN|5@hQ?Z%sqpv8Fq({&j2`)JW&+xBg)_o_!Qw(F{vF$nXxUhN@QIdIj47#xd-{a9UEq0O>>!)fjGoZ7{ohdWW9JgH=sZbZa#M@@LAe(7K9sZZ5ME#>;Jx=;6yOkv>ep4xWO>*l|t8%rh;mGkqDr;D->d(cQ<67{9O6gQDv(d~O}%iT=v;I+zwsevBo{nH=MWd}Knijtcbwpo@f=2pQ-+ulk(Bo)I+Rhb*-YWMR!4-z2v3hlBq*whqRC07~t8+tuvA%<5Y#F%S+XNL^iG*o>!v*N?jtg8(V=yRlDd+Sn%FUjh4`#2TCay#~a*s|7&Ntam3#VwLj_wM}dtEbP--^&aH?R+)wgv}#r6WMx~Lc80xJV;$K^ZAA}pm^LrG6$I(vdLykK{*#QO3A0+e4Q%vx6}<lODm=(9E;%1d(eP(H<h6WIR9(uWNQ1;Q733_A8e$V%+VxaQNStA^j3;Io?S==t$usT%=IOc<%^S(cbZ@K)%-NNfJ-i86!7XVJU+$c|7(hzwy4sBhsOR`E~MPWRlak-Jv$k2KCpY5I;~X*u2g>|8KaMmagpr9#jnjTu(JUcw!^!B0Iq!<x7^pEVD6i@+s!9)FBNUv0<N{J2}yQ)`e3S&bfKCmY5kE#nt=<&NHWGY+zu{(e2b+@otdMXP+OpeG^xlAq6()8FW&SaTM6c>*yp3Iyb=^vGPW?$Ya^{nQ^`fkKDBw)@l^GGhtsj$1a>wVltvM|o-rpUFE~M$yK(aP<>Tp_(nI9S@wkDns=ev7<u7iecQjLGYOd~e&|lbIqjsK*?I7B%mVa_xKczYBe;mDI8!r;DC2cx2@VAMn{*;l^;2A#!BdGaPNO>4nUMy*B3pXrvT4s`LLUQvSoAX%&lv%xEh&P`~fWu^Pt?koBI~rr^mr0|Y&J-5doGl*W1Ks7O{cvl?rAtxv8kcM8JxOXAdviaXo+ng7<r)$cZg_`ddwZJeDO-D|@NohcvRvT_S01hdBF<m`q*!Y<xNVePk5U0=>*L<Pd2#vi>AQ2?{aLvjQ#k#aEAZ^SwswKxbW~IUp_9@qv}o2+g)+^j=Mui<%hi7Q;X*o}Xa{L3cEi55v>9)wI=N6v^mNX>*PP`QsV~i4ccaPNeTB@Wt2Id-+doum`o6o95`}jkUcC5jcNhi-BBUR>n@S|;Lp3*<C<pH6$>n(Be{@;+-e?7{xe#d*p7kQLT*7CsE*8>Wo4SWJ*Hd-#@vN(<<*G`Dsj;t{@uB#8#M|5@7phabOlc4N+29@Oo8nbe6N;B|lL!6p{>}DD(Mk?iM+<fu`KnB`DxANO0=@74hm~Bi<NxE^GyOd-r*w1FrNC7JaD|?o6XGapp`~;J-?5;>n(5dMrL<iH+#EhmY-d?Kn!PgLOpjaT=5-(s4;zQ6c$c`_kyqTRQk%ESFLk!9@_?nOY~}NcWDie;#eHey)O0*-Q0b=89}vvbz(Ot^AeA|F*VXYdW~M{^=dgUG^}lX^83N1p&<d6j_jt!2v0!XlrmAvOYV)~Fb*gpMCdbTKlWwM2xN3cF=7W0afq3T_5bs3zsT1O8zSrIx-wQ$7OKsoLgh(3sdTY2&PIit5Nm1diKBPZi%Ci#fdSo;j=uW0=-Fp{sGb)wEtfZN`bZSvQ_tsTa>v56x(dQaxJKmM*-na?>BnSnPd7VFHR5Nv;VNZx}4{qE(Sp78e$Xr{cd`mQc$D^9;&du5E-@Toj&CK8?<*e^s`vX$Oj=S3LoD0T|DtQQUmRh4rhiw`xk5Qo1icNizbbhxJTR3)cv*aM-51V=a&2PS?ff@Zcwfp9Ng(4>vTCtE5SZwcL4}f*{*~3|sp)2{c*RcKu5j0|R@etyv2-cSCNpTW2=e4R%32kRs`x@6%d(^=@{J_Fy>{^?K{yIxz(wjnl4sb4{Om#sWCR_UNG`VU%m+&<BQN}Y4hqY~SwB{PsI83Z{Bn-pP_*r`FN@=Io63RGxTRA+{rs(u*JGba)KB;R8#rBMf8tt_@9$P+(c`QSu^37D)?@bjJyf1n2lS^h&K5ikW!B3C$rE=Y<xxdJlt?m8j7l-NS46n!M*Be;*wZ`FWG^-u-I&19f`1#BgBkOOc9aa~t_QH}ljuc=LrUYma|K`C>PV;^%4Z1#w>=lxESn=r(4Pr6WuA2=tCUPXJz3K*)fYP;r96`j}w>$)j<|Z+saEvAO<P!^zuEk5wh4R#FL}*6)Rdb&Y?l+=Sv!qeV<V&@^w!|(_vpbsxUe9`yS#db;CSqsdK4?Gl=u9k`PR}6QcE}+*8E?gJIdv+)#dr4tIQCm-2`SAS&&!l4kTS^X0pL})-&^nED?4RiVIL}u+^~zy)?q~Tj_X7N=fHE(2KlydmUbKWfF{APGm<bnWgDJv??ZjAllCTjS=O0-iL}GoOm6b?*Uy)iZ<Z9kfYI+6<958jLf1>|WNY+w>5J(|7l9iQCat}-xr$(Eku>UNmd-;OF0fM<gEutn$PFVC7PeNEUCNns>32~KYyC)BKAjb-r_FNw!t=5iBFK1L5{u35)(*>^#z<llKFcU)Li!ue-|k`>+}mDUxFW%<N#<JN+wKN(2W@-Ij>X>1dgMTiYhB69lh&wvUDQHC40JKx=207EvLn!+%R?G;IP6&Kt3nUGlGrmncqhe4I$%w%nLqoWPJ@d<`X9JqcA!57bJpLBPqlftJ<IQ@+l(0ovRWSu1)3&bx)u!n#l`CI%C|N0rG7@+qWg+BgWJ>I-CwQ7tErh1J0M596`>xbc;+V!?@!F)Efca6R59Lf@Q9?cM3c)*FO3OIjVDz4`$KFe2{8FuCO&Ykd-7*2{Nb-|;I9QgHn!lbd}ez$vkeTsqJetci<(^;w=i&4GNafo!8S2cKaonEIH|nF86B0wvetTcq+?%_=Sl-Va#Jk*ChfG}QeMP9H7F6YXPVeW?f<{|Hl(J|_|Jqh!FHcO(ct52#Uy%a&MErl^|{WkBV5LtJ43eG%GwQ2b!uXS!M^%=F`HHEZJP`DOv8vN6e7kEg;XPMVe(7{@>pER^?BSmNF@!0Wo+wH2eHp=lxX^q8U@7S4y^l-oPoTIVj>HiS%bS5+}%M)uvdXYGa7xgc$9?6#@)o(2DX)xdTzK#oQzH>H{W_qzd|o8B>oE*iqKKJov$BUtYzHyjLWe$etg?+ie>>c+<@o>ch@{@R#>Qv#PTMpJPdV7C*&dUr!SJC<Y=TnQ%#`++4dCCIF4$`?43sjxXGj@HB;a7>^d3hW+Qu3Yekw6#XL5+4s!>~7~Ac7*^JATOlcNcWz21nqq>>X1vfWIEb3ABjBRGYW0bTFw)0oYcq@%j{n~`4&>N2>{VArub%u<E%iQhX?Va90fNO8tU%8lrZKCZznjDAr0(J%&H<_QslfE`~DfmWhzj^x^nZ5Mw!ZmmH?xLf+{%e|)F(31^v%?OYB}d~bWN5OaRD316?oGZ97^YKuXT@`P_9qX_mYLPEIGz77hT|t#jN?Mf`WKH>+%JyCLBBt}?L%wpg!Xp2NY`?U@GHwTr=)0K*W*%lMh#r`JzE!*N;VL^-|o&EuS#Tpgo%&(T1m5!pSkynLO5>rrhOajyk}`?Xic3^a|ou^jSenb?xf*7Gyq(-e-@TjQ^Af3FCvh(4iK<;2lo=Zw6RP#<%;ZC+8~m4?1=Koktkxn^_$r_--2}O5G(nih2^aHaq70ZlEFbUFDmr{^*gr8bRFpxDZ?OGMvXFFQy<eJ(KBf*r>zdX(Ol}s<MXr|K#$o7bvq<`FRpHH8l~@a+*)YPef!wMPupV+FG2VcMR@3Hlg4NgQ|7T<4z%#|Yp-K%`sH=e@y8heEbd*e5%oUKx{7M3Z6O!&g4y#aOK$$|tQq%?cW>Uznc=Bjj85-UEv!viw>u{-RwL_7>gc7=mlYKCq$E`%T&Cf=W<OJ6-Zss~P<?p){hJT4iKh18b0F@xM>MhXzE9WVoEe?jNeN0;t2<SX2lSdhkB;$OkKIsGD%?XgzjYv|!^CbeIxN&S_SFVdc0}7`4sm0A*P@5sXt=wtj76k-2nGQS@^Ds;56vbl&ju_X%~w7euRNQs><?EKjrsB4tdG!{VxsTjB8>Bp{#}G=66(}loU{zi4gTo%g{I{O-yM(3J>^S3J`&kZQP%FI6Pnc265E4x$v&faF*Oh60m*Fe*qj}a6#}|s@~&-QBv}B`_hG?w>f8<1yMpD>I92}nar<?OX8MC{L)!_AWzOp1(AvHB0^`ImwE`7P3Uep{G@ZZ%8zc<XN4sYf?5NG-6YH`D@rzWHIcqX1Q^vmdzGxBwGAvuC{LV&VhcbsT)VbJRXRRr5jiiN};Yf>T{4UM3DcY>&q_uVB@%;Y7u7R7KOeey}4p(xcMpwQ#w!>&a_s^%^aBf3vb2wjPVG;8tv1g8*>|ocG?S;L0Jcr<@N!M)KKd)>yqQXvl(QhbDp&kAD5bIfQ?+F1&1953}Cxg3Pvd^~<S{!~%JypVSSV;%$duPcB2D@@|4pj=+p)jYjFHoK)X$E!=u-JrPqo%bwjAkKSS-;Q$aXyl@`q*3L>{r($)lmRc!*+d;p2|0;$BU31X3wbA5IT(6=KWdSu{dm+&!7y+g3;s+tC8&N;DE-lhzD8t^}(##PzSiu$kx$~Dsy7~N@e_FddEq7?>!cC-fPtZ_TlW>?Q8c8!Xj4|EBj-<ZOFw9JouRjlY1ryvS?u<0Cb+OeLLmyYD~ehhP2w$I0EM&+1ZP;DKD;xv>kgb(t)U88m+66&<cw0S5pg3>ZqumWC7Hnnoxc(Th3k%#rTlsm_7?0St`<5cIp%B)E`v;$5h4CLBe|inDyKjYTit3K}m3u?ay82CQFDC`MlPVwlSz<G&&UT6g&{5GJ@MmLY7B5V~|Jxcw?yDJZ(<iRho7<X@Yi>!8BzjYCfCw2`PkizB)im&POE$$B1y44Yk(I;>ec_$@ZSqjm%T8{H6DcS?5gV$w5sEx%{cR{N1j|;ZVlP!u8kI(IjrN>S8{lRRs`>Ha~prCeQxbP}ymmtT#+{Iy||NN@_pzNZm6J#@X_CS*YUs(I!{fd1c*%=O4^Y^=GC|Q(q_N=d_1LMz`vOR7!64rlZaVqE1ci4Uh35qfhgqrc6?0WH)#9V%Dcs{T#T<)XtST$aYcv>g1GhJ{^YH%f`<chI6CkdB{j(X&O25<_oIwXC34k9P-*uPHYoH8g4o%(b<4_ZECLZm-pvCoxeUBsrlr$HS;^u;hma3`kPwvk!An2a_z-G>T<XEdQs})K`kU1K68%;OAwd+Yo~H~W2Q7d0Tt&2w>Ss<=lo|2*VJuidj`+qPnRFxu&>_-Jd&07Fy?2IeI8DuAUP>HxsT`I0^dvxDrbhB7l7crTN=(d1Rv!={YAO05jbFDt{m!lknP1OK%Z?{g>Z#S1(GaC^+!`sj|Hk8vnK9mRWYqt_JbWFv%;Rnb`(F$>KxCwC!ZZpbjD6}jmBUWR)Yd3;KVpR-vL~m|E5Ae&LXedI#fy(XJ47IYlp>^$)m${{js|29FXnlUEKVU5xeAZV(g03WURRGTKFxFhh;COX{<DBZw$Q#`JrZnhyyQlZHZ}`sro6xXBYYxYg8G*sX<8A6(37adQh`dXKcB%rhyJVi5yL03}rfxxZUPs)sAO+m2TM3ICXjT5|P;ekZ$@5o)mk{F(f+|LI*<e$V3J9ay^!^4@Z~bhpXnO9r$d1?w`Llk@d-ZBHzona-+uFWF>ku*F)Dfo;aG|cpX<)j^bs)X}JWDv}|C<oXV==f<N;HXn1N}psbhi$&>T!jJ_|N0U>TEMwQ@@$Ru%|w^`fPL&pVWRbF>Pnnyjge5g&5M{U`bQQp)^SN9RMi%ili8rrH!^G?T#a^{Fx4{ma6_~5DKmh#LxxbNb*b8XIZOPtrNm*J6YWoz;ik7Tyqo8LCj)YWV0oI^%w6h1oR4U)dlY%KG)-8gvhtsOQx<JVX2Hc#-d@wzs<QK^!>%Rw0#k7`@$d-UhG{XczV^yqx@s16=oIP=X80J?MBaqn3eRCc0;lqw*JJng*4`D{#Y>XQtWnZiSpHgq#G*%Hw4n`G0;9M5nz#xt~8qSRpAAE`L!^y--PtEpyb8rwLHiQ@POQ?whf$n;J+({i4+7kAZa(-Vj<eu;fs3YXhBn#;Dl*1^O!oyksFJj(mz8H09;g*E|yI8AV*9Y15L`^rc`=ehERBiPbD3l>E+Nk3ID+50I^(5vP{+hh>UBV*2FblyZ9a#b0X_4v5F*}ywHn^|kb7&o0iefGn2G9QbmfO^d0yk2<Y^%mZEy@fYkZ!sIMXGiUPOscFd$@5n<WpjFZDns+8DdORW@RBC;i3B?uu%|=hG?nfn$A!hV08)1I<Bn(V@!Y-Hygi&81#6|qfmD#4SobRf_ngC928}Imz5CI6DHU3#=7|;r4Xu0qdko#2-FcIHNmaCic<d^69JvlJo{yn7>wF9!j9vFN81aWn+&|m9Of_rs>FUoKh(J!gnvGwZ(g0?dmSx(5ndp+n#F-pR8tpzG86pNu!6hD|Iepx~nCo_>&m!S5jzc*YCPa!ulzNO>5FXZPSZDOOQW7h!c_}Hsbv5F)w9rlB(Y#dhbqcoFv%`fBV7@NpR7U@X$Auns{hy_g`ZB`~#8|KK8(pnkjwD&IZqYa`sfIg<9Z4`|=<-JEV4%NWT>NzYz)bKQ4z?1WGL<wUMVSs;-YLoBmoVOwlNdOBvK=_qc=~IJMo%=cVOAQ=Q(+}(chqz(>k_uX7C$2^oaarQBwt&D%b~bZU)*>lM|S112%fkHeD>;MDQQbf4taaeDLUnmY(=l&S$usY*rs|WJ7~=gUYieG^8(LOh)Rhxk`E_|s8XT}`^l^M<eGFEBCz37x72_1XX};s@D#PZ>ejIBqzdK=J9%ZH=9P>}y0Wv8p4ccDk&?q(lr=AGU~MOCQXD3ft08-0D{MH;gzHE_Nw%VpCKo;|n6x#HXLcN$uR$J(uzFJ-{b@v@rfW}$>$6mH7nruW(YzcBDs0W%g}RVAZ-$==;QWrVSX7I;u{oNbLbD(!8HNRm>>-DPCKsK0MdvvfIx5N)h1;qLbt$sDQGD;~rZxPP#xB7n?XW-c^d$3-{9+Q?-qBN$))j@v_T!Zmtyi3vbJdJj6vlGrtrCPz+CPrz#JSR{r+nta<|tc+3$J|X?J{R<Deak`ajn_(mW^zEjZHDOc|)uqT=}4jdL2y*gm6_6?gY~`VNufWPV%qICOqPm2C=UA+sqv*qpoPovMmyJJ0*pcv^OqcYRTEvl2x%9?T8esw`|J}$(Qkp63(2=kpe6oB_=Oo0i;KvmhWti9Q(!X{b#XZfpnC>M^LoWgv+J%UR#pmlxIpnR`2#~fYL<9R8JR4+(C-wXfb3%9melmjBi%`K6P8sj0t-#YHceXJr=9mgAJc@fY=MIo}N5|a>Cb+50Pq^4tF8@AVcL)i-lNcX$dih&p8%n<0wvahsPB^l0?BHN3jpG*EURcw1wVeYvmrV-b6ftE(*Qy4-3yGDc@6_9EBZ1o7v*VZMxB&p-XiUM6j6H!KK>j#!_M^4g0lLj27>+DkI#$p?0j|_4!AU>6Rc|1_`x|pci;HmRdG5f{~4TLg#m*VXrpsDGnw%91BXPh%!X>p)>w2QG!xO&|}N_6JD~5qb(w4-T0+c8QJRS+2Ae)?Xx{zauSrA{YjZg4;V%!*?(p{!Kwc2a1)M@IbB|Axxn^*rRvK`8Oa&_%)jLX5gA=R^9dg%`iL#-<x}xEJc%Uw19m$~m<Y8ikU&DdlV3Ry3Ka75-fgBk(TC*dlOo~Fe1%am3*1(FN>QB~QETaamdQR*t2d@rb}`g!6q_J1PM4T0MNNMWIjMaAWoz?{yo6r9#hSyx*EUxckG>U;hv-u|oN;>Jo}>#u-ZpnMBW`qoOqw@4WiPR#d8t)+J-`&yW^eph-mgxZb&0-H#Lt@)>+s*Kr;rEmj)&|zW68Dqk<<{%7PMJLUS<p)Y0CxGcSL(;^x6-wWhR|bglVjgqoTLNJ4%Q<o~f;gWESSB@b!Qe8wma=T}kTN*m7>$m`WzNcWUr0-7sgn9g>IV4KStbJ_pUL_@CHcCM-^6Ig=`2i?4KJtQ~$|SnJa{vv5aE;nSK)PT6r|{b<-7i8LC!8rhyIk`OGQ8z7u5Vp>IOripOs5a)q#enE%^9<lkp<9GH2+A*(-!ciKj1F#DXkV`~|bcnVKlg40PA*ytTT5-(?o1mN#D%l*|&deK1x8S6MzlnR>HyA^nOsryBELtfPvJKHRy;A06>l{)=Dl-9QO-~DNmn^>AIZJC1G006Hm)5$JS_GD}WQohn<QKm>3fWglHfY14&2jOyA<5j9Y&MWL<WajzDPS|bI=77-uS-<I10r2s8+&}T%#6r}><0EzT1WUW@H1w0>R>ZX(hbR=8)eG<Y}%W(RMg4fd=*|&$U<h%J~E0oJRZZVgMtU%X6i$Aw{IAPOSCt5%N@qR%4>RDa-i9^Uo<||`Wm$KEu2D2ByKqO2@B1V$kT{qUyN~?MY`)e29L_()m1G;4|d|A%*A$*!O_lMe){%;(Z3xHOfDWCm8=7TT}hJT9Iq>d`d_~~BlvfY0w$vH2mc;l;PElOGQjteB8D`xSH}2Sj%mip2}dIiNjf`0`yip3OxuOWI$mgg`VtijY8mb{y4Yeyf1o*$91Y*~QfsuPg&%MlP3w=reS9>nxQ4yav}`l`)sbVC+aPuvsZ19z<rT47c4nC@7M7AaG5Fb|6}n%a$;y1{=CVqTFbR0gG26_vJGU#O94s5oW$Y-aNHmmnbL|Gj+sT%egy-3w)9la1EGLnbGFQ@a5z{VsFauNPemc+TqoX;)uaD-Gynx?6p;Kmv+WDOS!l>5xct9&}+tLni{q5sg-K^H|9}Z}pw%O`lr;9l%J>5#ivZ`w4CwE<`xKSddr!4Q9JC4|B`>C%F67?E?O%ti9{gs1J)<O2qP85YK79aXY>nNbga5oW@3`99$FK=iH*a)!jJn<BI@D^ob1`-f$jUNJ~$;_VUQkIz`kuwR5+5*ZRC_qM&%C3*vqC+BdpUte})>j(5W@XpKNn9?gK2Mif+-1u;s`_PGnzT#Wc<B3#8@&GiybC&2"
PUBLIC_SCOPED_SEED = "c${^b+m2kvb%yWx6g%Ua1mIA0J_tboBY<<ba16*n0tA6nhgF<TW~Q0$9@2`Sch9#*wiIo0+mbzO_Eha!>tFw&x_`6%?sEQLdEUnP>BFPkWP6f!eYwcouEPJU^JTj|$oAoK{&2qX!s&W_k?l9$ZrAZ3^X2jM{C;as@;v$X_U6qWA5K@^v|V5N`7%$>jU}(=m&;&fe!iUA<J;}|eB0VK<$8LrZ(vp3To-7!)A@P(@YtR=c5WZo_hWl}xBXTZyvqN)@SaWn)W+?XO}D3)>uu|&=gFh8^{0pPqv(NN|FZG&MQ-i%yq);dm7|=V+U5I=_sa*C;7#`z`FN5~KR@BS?VoPjJj?a<NykHbe$Rq0my0g=@5!W+O{^(@^$)ge?dA4xzMO8~zvZCo_NiUutH;wv*&a_%r`z@I_WshI_5W{jy<K0AHOBen`9|g5X~9^!J)fUXqdji_{LR1oe(Umh{-i~0kB?h18@pakJj~?bK5A1g=civ9_J@ad6>BnOk@50cKmM701+nt=>l*g0adtb8pN=p@i*TMVL*LBV$N71j9$8pxbvr%r(`$(zzh|W(@3z1H`T73!kH5PwTmPQ3XdAa@`2+&w?e@(-{?^5`eK)6DRYpwg=u4^MhixwRWNI3rrBJ#x-p#<z#^yXE8=u6r8KlJ_-)(<g-j?|1J^tq1_HS}}|3KYdu0qRh@?I|6kkjLKUSQ%$AC~IB2WgMn_0}%8udcWAhwYh1zUs*)hviqMtkW|UzyAr0Uja@5<@XmV{r+-(`S5mo;tfwPPn+GIuJ5*QegdtQWbxxKn`pf*bEE1H6q0W9SmW_!%6zxcOdj*`tfd;X<+YkWy$MW@SBUfW4==atPg8ImCtf<O(_r1}%cH*j^8`}d`^(F-K0kB#)ab=4e75&5O)({+MR+)0b%g$m2Riz0`vYYF#(()M9)goI7@YWZS86$d-gn=9_g~lZ^P3;;+T(kL@Wa#H*LUmmryJYfzgFe`@x$%@>3aXs-o3s1GY;>*{*NE-PC7S${@d$Aqw)M+kIR~B>@dCXQz}Jz8_wq0N0~F4RZCte$JwU%;GB&vxtfyrTwP9V@aeVJ-#)R0jsC~CU)m<DV9m(E*KB=UAvSBm^fQ~}>y+qx$io)D^d+-w9k~o=yq|+#Y3A44#ALs`%`(M^Na#hp&#v3ts-#+c4#N#UhDlNE^ir&sC~UoQw|0)`svE2QdYhEI`SLzKjXYVa^K>QDVruKTI6I4Pb-7&3oJ@BdE7euJ_1$*y{JEqwt%P6i6I=~n-X~fkvy~Foa57e(C1<-bvuR}-W7CLbj8fbbsq}8zlq%2S>{?P%{`EdCe|eY5WE(SN=F+AX-uqahscueNYwy$BHWyot6h`hPx>bb2xzt-rVt3662hGXLXWx3XsZY}>eP}kau4{I#)ojStOgm$n#ilvgVnIH|rC431+`Ge=RKL7W5{S!*x1pvgGpyV~7qAe0iPf~VN*vaXY6Kuk;OEY7lq73Z*WDh{M^#(hmqv%7#SZ2y>}Yw~Of^RheL9}Q=Ul57p0!%g4*+}&u8h|G?l=cUb9pRCws)67*D`e&5XT6mX7Y+5dlO2aSlzABO$YC-Rqt)-EvB_Y+R=EHNjp*oxvbV%f!!d1<XxJjQ6*39YFc6#NMozjy`v7(ioNBy+r<R;<$X+Uqvd6@w^D836f;t=7?(mVm0=`<EN}%Z%uHSyJ5oRNhO+G-V3RvI&#Zuobt*gL8oP(gdg{icS-mOj;qy$HaSAqw;%+P>VaBjbbAE?tSIxnEt_3zF+MXzj$6grb-0|qnjgs4{3nE~edh{(#Y0R<hzOoUu3wG>yptys5n7>@5r!WIun%&GUwK3o!&$=4@s^Ww<NFQl5Tux+~*>D=LR_1P>gTaXf!h;(Zb%8shVK8HAnaKfxb`&?paCuR;nY#4}mT1fsJmjKn^6OnZ)9~eehOk}*Q?5v}wuamwt3mx&3-Dz?X_RV$btBfoIfuzUmDVM~ePNG+qY0(XwTOYYV0WFu=$QUGbz(+Wd^;BmB;)ODAZ@?^9fAO%iS718kPfs!YPU1-4}vwU2)&pHOoqf#3ZXh%i$f|c+)!MTfp$>gA=UI*cU80QV07w&qoMg?dq?HUlmO@=^WTbtR~>AL6d`+U892Keo`?-A>PP4LPTImjZ=g(1FU@McQS-@@uQgI*Jc}KRt~JBRa@Oe!;<vKzv9w_PO0n&0=0ZGB&}r(N!K3u(Jg>zXnA1@tHuS#cm?A6s1@t`oWvOEXpivR3Zj1d)K@avpqr(t^WALdHBJ0yG%Qp1hhtA@n|3MOPv&P^a7+OE^!78?MhqY3UFujl!9=+gms0Y@0U;v=T+$R+U&bB%woKz~rjNeYZBNjDIlbPJ|8MEbsAss!970rl109o;}6cP*Cb|eQtl7T(skm1cxa}Nw`suQ?5KgH}Q2a!w%CsI9jbRt8a*p`*y{LbnbP@rkGl(AeNdDXg*>8^@Fw`{boAXT5qbukAL2Ol@T3}jGOP=}=OmYNG#6FZXW%LSw%q*b~?umyZ*jA@{Cey`UDT##%pr8U|OVQCW_lvZ7EixWl2ds(D{BTUN)G8B#!RCF*!NHFX+sm>hWjf4~gRl{K*EsU82FT5L%0EZE%pdNHKIGvZ37IK&;uEeMhN^8&5<^ToFwU9t>YS<%jEZ}5%M7d2!v|-*k7^V#?oS^FLWwf@YV~BCwz3xfp0BiKVswSG$YSW$@wbRS71heW14jG8p#hNA|d1D1qfw!jhf=k#zARftewHEI0BQB>7+7m+~h8LwVg;Wx2US$$Th^Pf?KNeD8F~x=~!4vq;igs|GVg{yp^lI^CGEr@ur*_|w7_55IfmJ}C*TkkJ{K&*i3qBY*h;c^-iAOjh9n?4mV?qcW$As=LHbh)-Qz6c35Kb$AUeu=IvO?}A40HmR_FW8#2X;E!lKL1)S1Az#!rIa@O~U>nb1~Z3Q&sY>*jo}k0mwQOqJ*%YPjBnNK5E37w*+G+sMr^#up7L_qT3BOYSi##GqH7nWCq7AMhm}E1?kxzCd4BeSZdEXJ-8LD#>^lHh!3=Qf@;4=8yJvU5>gTML;wbh;D+=VWe>J5@6tmjIY83*h=p*W#b|&Tlndu%11QIfA~MKL#iZ$#kuQ^^!Yh66r0S8Cg52>cXt4%Fs2|Bj;#|`h?feLhoFO`hiOE$z9Z?SD+RWd1zmo%>@{udRQBMLprl2(VOG?NgRrQIukUl3i4#mTU;DZZ7UQz>w1FdyyrnQ@#gA9WIHiOBwtzs((B2c8qxJ?U`IDxkV1VRbJ6Hk#Yb_Vp;W2>+`dksBG2ea3fj86e(9F@!-VMp5vCOahWSM-BnCk4|r@;#oB{D1|99FT^wM*tD_D8A>2J~qW7j)IoE1bYdCIq^#P)=Ye(=7=|tbA(o)L68&AT){uP?!<T?p`DT=Th=MBB&KJ2jA+c+pmb1kTP<Qkpgw3QH|yXttQDmbC8k|`R~vqSYFnFz%rHHNA-=2os0v;;JfrG~g>c2xkUcYMJ{uzpMGTJ&#<DvQ<7^LbjkVyRU4_I#C6S>&lRr3{bEXRs&$P^FN%>o2iG_O*ci3<^?9u+ZC=u+BG6L)l&A6f|$_J-NP7U5>UJ{Y8VQ*B<XyF*5#%ghLNHv6M$1aiLjE`7Huc30qJljN`382#wn`@1N0fHjBb9&FIHki|liF!BOB8C{L-TMNIA{}6E4N^|)<eSAj8ecPU(C7h1v@~tSjOi&$?a7Q3tPw{N+0C>(%Gyn;)oDCv5m{imMfgWw^6(f<3P##sf+iugO&CV-TyVA-Iz~mAJ769=`FJOC&R2h=a<rceQUMqU#~C6J)7ygVG%V?e@EFz;OC`iC^2Cr4TMGm)7%+`Jy3JAg)r)}AW=+IGoIJfDB4F4t(#(#pcwvafni_O$1KFX%@EjQnRG6#%Em+M*rK3MtO%>YTfiWAv`niSHDg_;SPt>h}a|w0}1J*mCA1&dWv-$lkr87t9CcYzE$=eqDqP+=&1+3^9hC}e$Y8II&><;fD@dv9ZzZ<vu!tQ;Rv=8>-VFe*IR40=a5lyU`K+%Nj-XU2$3f{|k@===3N)Cea!T(xUhyN^_7IWa>dIJR+3*h3K#1qUW8ndd!(Ewf(UO_Q4I^7}DTwAmxZuJZ;eRdJT7+HTH64v6$gRSI%i69Jy5$z@h!!sT>U9G6uUF;|+AoCX2zlat|FG%}7XjR&MboZ(km5k3qeQ*Y~k^U$T7QSLY3rs=HcoOWQM|<htNUlR$<Odl$Q(75}gW6g8uUDWQ=5i3Uk=$q)3L81b0(vHsg!^g)iAp{tka-w7gCZT@{e9&Dbz`lqm`*X45PI?OS<vi^+4iJFyCjKTFJj<0EdO2XB&id?XE%MX;*8OO7zN^CtzGXx9lBh!ex<K2rRwdpC#yO`Y7hj2&J2@^AuDA{=VluFD@!Q(=sr1SOiURBE{tIgK!cbNFkM;i+e<UuLUwj?hGe4NTi}#i5vC6;>>70RnL;p3A>>8THJs3o22;B_$)(nrHS`7sjfZw{#Od`iwlxxF@OfaG_xHSqpD9#9+y>%G-tGa!HaG>E?j%Mc6)15T$y;cvIG2SPFoevw0RH-{Z0SgcEVJ)0=#0M!Mz?`dhf@<Dq`=nXVEwQM@7^7QH5-7z*zrq^?6*J8QFg>JqV8meLc^hDw&WxsBM7)iycQ<A0XDrpre{_pU<?sO4Mv3Uy;HCU?J1}*W<(rZ7a{FQ(r0cAu{$}!wM|MygdTDnu2d&z;9Zj5z((Ug69;?5aG6X3yAb1Sg4F~wta71RCdJ@C(Wq3t@y_G)^1=*z*YTiw?-2LbA;BN)BN+8geMXnA+K=Esnu)~fBI!gh@!{2yLU=PbADL<fxYXhu7HscLkFH=N@?YFCBts^o6}s<}=w)GHPFT4FUzgIWIuQ$rPNyFQBfx-U=i}S~g<?cvRATIIU@wN6+C}OyU2v<V4oxL%3IasCnu=(E`C92<0F>;`Vva^9pj)|XW1WjE1zwSEy_PLxWLVmGuLvQGxvrlizy`rJyro7lvy;;UEhV-&4>U7+Nx<b<<7Ad17FvBrfK3I<!#ngYwm?l@FYVckoD}2dq2Zu2NN9nH3(5%JgCL#8LNb^ZTjw<nK|>8!>ecIp#fd;zqkn9){|M7LYmNjC`-U9*S4|^w=}j&c#NruB6aBnFX~l!$ka&HHUW>L02s75Z@2~rhuJ<&>>x>dfa?`99m`@Fj5e-Mwbdq2m{YU^1;##Ng>Olfv)6a|d*(4vhL~MquC~uyl6rwh$MYl#RB@%k*+6q%Y`g1XQtIS-01_mf~fF9PqKjvt3Ffr2UkR$9s^(c!#!bH<tXoNr!`bicoA+Gg;{}{!hPv8=g12cQ8V~*}!Y@!3j>0NQ1sr4H5qq#Doqi6WFh~ud~4K!m;F+?E!co+{JJ?y@#o6kodml}$baXP`{XL=!64DyCE0_{69<LgI2SX_J?GHsUz{gm`BDe*LMe}xeIkN^4hPQL*p&-&emyRUz^yXaSduD5sky#);pqgwJ-?ineVlL8VKaKbPEa$tg9RQ9GHr{cG1l_i&VOub%>>9r3#>UT)m<@>*^Nb(!>QoW~OUEnW#yHQGs^qs#H(+Kts-i9inx<V$YnFE4Cz^G?9U;bMz_vUrUM<p*TGA399rRftkYQSe;(U5t!oc5zKtf+j@Pckq`dfl!UABEVO>@REDpVu5XoQznJ^f@MzfGnYW7PKN#_=pYzE92Lp!qQzg@%2^&mj>+C-H#l7Jjhe~_M=>`Cq2C7rPpu0eEUEsehn9|Um_aB7}3iN)223z$YQN#fJ%@`NaWqmzxdQX{67Lh7r`v{wiR_C)9PL6qy&s>=S`E8hawb4Igd<IBBNBNcD1Wm-2JFuQF-&`Z~yB4erxX)ZsY#^K`yV~68joUjR1^%UwZWSShsRtTTS<PbxHd9Y!7|AzyG^8{|B`5qqz"


class Generation19Tests(unittest.TestCase):
    write_state = QualificationTests.write_state

    def setUp(self):
        QualificationTests.setUp(self)
        self.repo.root = self.repo.main
        self.state = copy.deepcopy(G17_STATE)
        self.state.update(
            contract_generation=19,
            approval=copy.deepcopy(G19_APPROVAL),
            approval_history=copy.deepcopy(G19_HISTORY),
        )
        self.write_state(self.state)

    def test_actual_history_and_fresh_gate_authority_change(self):
        import review_batch_windows_v1 as windows

        authority = activation.authorization(self.repo)
        self.assertEqual(authority["contract_digest"], activation.G19_CONTRACT_DIGEST)
        self.assertEqual(self.state["approval_history"][:-1], G18_HISTORY)
        self.assertEqual(self.state["approval_history"][-1], G18_APPROVAL)

        def changed(*args, **kwargs):
            self.write_state(copy.deepcopy(G17_STATE))
            return None

        with patch.object(windows, "_full_checks_g19", side_effect=changed):
            with self.assertRaisesRegex(WorkflowError, "authority or source changed"):
                windows.full_checks_g19(self.repo, self.repo.main, {"plan_comment": 6072111969})
        for mutate in (
            lambda v: v.update(contract_generation=True),
            lambda v: v.update(contract_generation=20),
            lambda v: v["approval_history"].append(copy.deepcopy(v["approval_history"][-1])),
            lambda v: v["approval"].update(plan_comment=True),
            lambda v: v["approval_history"].reverse(),
            lambda v: v["approval_history"].pop(),
            lambda v: v.update(approval=copy.deepcopy(G18_APPROVAL)),
        ):
            state = copy.deepcopy(self.state)
            mutate(state)
            self.write_state(state)
            with self.assertRaises(WorkflowError):
                activation.authorization(self.repo)

    def test_seven_primary_predecessors_and_closed_capacity(self):
        import base64
        import zlib
        from types import SimpleNamespace

        import review_packet
        from test_installed_qualification_v1 import PUBLIC_U
        from test_suite_deadline_issue31 import PUBLIC_G13, PUBLIC_T

        numbers = [6068705967, 6072002965, 6068144159, 6064513854, 6062530466, 6061320190, 6045434332]
        bodies = [
            zlib.decompress(base64.b85decode(v))
            for v in (PUBLIC_X, PUBLIC_SCOPED_SEED, PUBLIC_W, PUBLIC_V, PUBLIC_U, PUBLIC_T, PUBLIC_G13)
        ]
        rows = [
            dict(
                id=n,
                body=b.decode(),
                user={"login": "Zi-Deng"},
                issue_url="https://api.github.com/repos/Zi-Deng/FLOW-DC/issues/31",
            )
            for n, b in zip(numbers, bodies, strict=True)
        ]
        context = dict(
            designated_plan_comment={"id": 6072111969, "body": "X"},
            issue={"title": "31", "body": "acceptance"},
            issue_comments=rows,
            reviews=[],
            inline_comments=[],
            pr_comments=[],
            pull_request={"head": {"sha": "a" * 40}},
            commit_statuses=[],
            check_runs=[],
        )
        repo = SimpleNamespace(name="Zi-Deng/FLOW-DC", root=Path.cwd())
        self.assertEqual(
            review_packet.g19_predecessors(repo, context), list(zip(numbers, bodies, strict=True))
        )
        for index in range(len(rows)):
            bad = copy.deepcopy(context)
            bad["issue_comments"].pop(index)
            with self.assertRaises(WorkflowError):
                review_packet.g19_predecessors(repo, bad)
            bad = copy.deepcopy(context)
            bad["issue_comments"].append(copy.deepcopy(rows[index]))
            with self.assertRaises(WorkflowError):
                review_packet.g19_predecessors(repo, bad)
            bad = copy.deepcopy(context)
            bad["issue_comments"][index]["body"] += "\n"
            with self.assertRaises(WorkflowError):
                review_packet.g19_predecessors(repo, bad)
        for index in range(len(rows)):
            for key, value in (
                ("id", True),
                ("user", {"login": "other"}),
                ("issue_url", "wrong"),
                ("body", None),
            ):
                bad = copy.deepcopy(context)
                bad["issue_comments"][index][key] = value
                with self.assertRaises(WorkflowError):
                    review_packet.g19_predecessors(repo, bad)
        # Intact canonical JSON cannot rescue a mutated whole source wrapper.
        bad = copy.deepcopy(context)
        bad["issue_comments"][1]["body"] = bad["issue_comments"][1]["body"].replace("SOURCE", "source", 1)
        if bad == context:
            bad["issue_comments"][1]["body"] += " "
        with self.assertRaises(WorkflowError):
            review_packet.g19_predecessors(repo, bad)
        for cap in (1000000, 0):
            with tempfile.TemporaryDirectory() as tmp:
                packet = Path(tmp)
                for name in ("repository-policy.txt", "review-policy.txt", "domain-policy.txt"):
                    (packet / name).write_text("policy\n")
                (packet / "source").mkdir()
                (packet / "source/pinned.txt").write_bytes(b"x")
                with patch.object(review_packet, "run", return_value=SimpleNamespace(stdout="")):
                    if cap == 0:
                        with self.assertRaisesRegex(WorkflowError, "snapshot budget"):
                            review_packet.build(
                                repo,
                                packet,
                                "a" * 40,
                                "b" * 40,
                                [],
                                [],
                                context,
                                {"required_checks": [], "max_snapshot_bytes": cap},
                            )
                    else:
                        review_packet.build(
                            repo,
                            packet,
                            "a" * 40,
                            "b" * 40,
                            [],
                            [],
                            context,
                            {"required_checks": [], "max_snapshot_bytes": cap},
                        )
                        inventory = json.loads((packet / "required-material.json").read_text())["required"]
                        for n, b in zip(numbers, bodies, strict=True):
                            name = f"contract-predecessor-{n}.txt"
                            self.assertEqual((packet / name).read_bytes(), b)
                            entries = [v for v in inventory if v["path"] == name]
                            self.assertTrue(entries)
                            self.assertTrue(
                                all(v["kind"] == "contract" and not v.get("omitted") for v in entries)
                            )
                            covered = [i for v in entries for i in range(v["start_line"], v["end_line"] + 1)]
                            self.assertEqual(covered, list(range(1, len(b.decode().splitlines()) + 1)))
