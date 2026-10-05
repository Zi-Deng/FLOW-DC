"""Synthetic reporting journal and native diagnostics; never actual activation."""

import copy
import json
import shutil
import uuid
from unittest.mock import patch

from test_workflow import GitFixture, review, workflow

# isort: split
import claude_native_auth
import claude_reporting_execution
import claude_reporting_policy
import claude_telemetry_v7
import diagnostic_tool_contract
import reporting_activation as activation
import review_policy
import review_prompt
import test_claude_reporting as reporting_fixtures
from claude_fixtures import AUTHENTICATION, native_events
from tasks import atomic_json, digest
from test_claude_reporting import AUXILIARY, LIMITS
from test_claude_v6 import FIXTURE


class ReportingActivationTests(GitFixture):
    def setUp(self):
        super().setUp()
        self.repo._info["nameWithOwner"] = "Zi-Deng/FLOW-DC"
        self.commit_task()
        self.addCleanup(patch.stopall)
        patch.object(claude_native_auth, "current_binding", return_value=AUTHENTICATION).start()
        patch.object(claude_native_auth, "store", side_effect=AssertionError("No real credentials")).start()
        patch("review_claude.execute", side_effect=AssertionError("No provider process")).start()
        self.history = {
            "ledger_digest": "a" * 64,
            "files": {"ledger.json": "b" * 64},
            "attempts": list(range(9)),
        }
        patch.object(activation, "historical", side_effect=lambda repo: copy.deepcopy(self.history)).start()
        self.harness = {"head": "a" * 40, "files": {"scripts/agentic/reporting_activation.py": "c" * 64}}
        patch.object(activation, "harness", side_effect=lambda: copy.deepcopy(self.harness)).start()
        task = self.repo.main / ".agentic-local/tasks/issue-31.json"
        task.parent.mkdir(parents=True, exist_ok=True)
        atomic_json(
            task,
            {
                "repository": self.repo.name,
                "key": "issue-31",
                "approval": {
                    "issue": 31,
                    "plan_comment": 6001819615,
                    "contract": activation.CONTRACT,
                    "source": "Synthetic explicit test authority",
                    "recorded_at": "fixture",
                },
            },
        )
        self.policy = claude_reporting_policy.build(
            {
                **review_policy.policy(review_policy.choices("claude-code"), {}, diagnostic=True),
                "authentication": AUTHENTICATION,
            },
            max_turns=80,
            limits=LIMITS,
        )
        self.proposal = self.preview()

    def preview(self):
        return activation.preview(
            self.repo,
            self.policy,
            name="synthetic-reporting-only",
            tested_head=self.harness["head"],
            expires_at=2000,
            now=1000,
        )

    def apply(self):
        activation.apply(self.repo, self.proposal, preview_digest=self.proposal["preview_digest"], now=1001)

    def evidence(self, number, *, failure=False, mutate=None):
        directory = activation.root(self.repo) / f"evidence-{number}"
        source = review.prepare(
            self.repo,
            31,
            12,
            1234,
            review_provider="claude-code",
            reporting={"max_turns": 80, "limits": LIMITS},
        )
        shutil.copytree(source, directory)
        meta = review.verify_packet(directory)
        meta["review_policy"] = copy.deepcopy(self.policy)
        meta["reporting_activation"] = {
            "grant_digest": self.proposal["preview_digest"],
            "number": number,
            "purpose": activation.SEQUENCE[number],
        }
        packet = directory / "packet"
        (packet / "capability/fixture.txt").write_text("Diagnostic\nCLAUDE_NATIVE_CANARY\n")
        atomic_json(
            packet / "capability.json",
            {"artifact": "capability/fixture.txt", "line": 2, "token": "CLAUDE_NATIVE_CANARY"},
        )
        for name in ("capability/fixture.txt", "capability.json"):
            meta["files"][name] = review.digest(packet / name)
        atomic_json(directory / "metadata.json", meta)
        self.assertEqual(review.verify_packet(directory), meta)
        session = str(uuid.uuid4())
        rows = native_events(packet, packet, session)
        for row in rows:
            for block in row.get("message", {}).get("content", []):
                if block.get("name") == "Grep":
                    block["input"] = copy.deepcopy(diagnostic_tool_contract.GREP)
        outside = None
        if number == 11:
            outside = str(self.parent / "outside-synthetic.txt")
            f = copy.deepcopy(FIXTURE["positive"])
            for key in ("tool", "advisory", "result"):
                f[key]["session_id"] = session
            f["tool"]["message"]["content"][0]["input"]["file_path"] = outside
            f["denials"][0]["tool_input"]["file_path"] = outside
            message = f"{outside} is outside {packet}; --restricted confines the file tools to the working directory."
            f["advisory"]["message"] = message
            f["result"]["message"]["content"][0]["content"] = message
            rows[-1]["permission_denials"] = f["denials"]
            rows[-1:-1] = [f["tool"], f["advisory"], f["result"]]
        body = " \r\n" + rows[-1]["result"] + "\r\n"
        output = reporting_fixtures.ReportingTests().native_rows()[3:]
        output[2]["event"]["delta"]["partial_json"] = body
        output[3]["message"]["content"][0]["input"] = json.loads(body)
        output[-1].update(rows[-1], result=AUXILIARY, structured_output=json.loads(body))
        rows[0]["tools"].append("StructuredOutput")
        rows = rows[:-1] + output
        for row in rows:
            row["session_id"] = session
            # Retain valid original UUID in the frozen refusal advisory.
            row.setdefault("uuid", str(uuid.uuid4()))
        if mutate:
            mutate(rows)
        raw = "\n".join(json.dumps(row) for row in rows)
        body, diagnostics, proof = claude_telemetry_v7.capture(
            raw,
            packet,
            packet,
            self.policy,
            session,
            diagnostic_purpose=activation.SEQUENCE[number],
            refusal_path=outside,
            diagnostic_tool_contract=diagnostic_tool_contract.contract(),
            exit_code=1 if failure else 0,
        )
        start = 1002 if number == 10 else 1100
        activation.reserve(self.repo, number=number, input_digest=digest(meta), now=start)
        attempt = {
            "schema_version": 7,
            "input_digest": digest(meta),
            "policy_digest": digest(self.policy),
            "status": "started",
            "requests": 1,
        }
        claude_reporting_execution.reserve(
            directory, meta, session, review_prompt.native(directory, meta), attempt
        )
        review.save_result(directory, meta, body, diagnostics, "2.1.282", reporting=proof)
        review.recover_review(self.repo, directory)
        self.assertEqual((directory / "review.md").read_bytes(), body.encode())
        self.assertEqual((directory / "terminal.txt").read_bytes(), AUXILIARY.encode())
        return directory

    def test_preview_is_read_only_and_binds_exact_finite_policy(self):
        self.assertFalse(activation.root(self.repo).exists())
        grant = self.proposal["grant"]
        self.assertEqual(grant["limits"], activation.LIMITS)
        self.assertEqual([s["number"] for s in grant["slots"]], [10, 11])
        self.assertEqual(grant["binding"]["policy"], self.policy)
        self.assertEqual(grant["binding"]["history"], self.history)

    def test_current_approval_and_actual_harness_auth_history_must_match_application(self):
        for field in ("history", "harness", "auth", "policy", "approval"):
            with self.subTest(field=field):
                proposal = copy.deepcopy(self.proposal)
                if field == "history":
                    proposal["grant"]["binding"]["history"]["files"]["ledger.json"] = "d" * 64
                if field == "harness":
                    proposal["grant"]["binding"]["harness"]["head"] = "d" * 40
                if field == "auth":
                    proposal["grant"]["binding"]["policy"]["authentication"]["generation_id"] = str(
                        uuid.uuid4()
                    )
                if field == "policy":
                    proposal["grant"]["binding"]["policy"]["reporting"]["max_turns"] = 81
                if field == "approval":
                    proposal["grant"]["binding"]["authorization"]["approval_digest"] = "d" * 64
                # The coordinator supplies the digest of the reviewed preview unchanged.
                with self.assertRaises(workflow.WorkflowError):
                    activation.apply(self.repo, proposal, preview_digest=proposal["preview_digest"], now=1001)
        self.assertFalse(activation.root(self.repo).exists())

    def test_application_replay_and_uncertain_application_cannot_reclaim_grant(self):
        self.apply()
        with self.assertRaises(workflow.WorkflowError):
            self.apply()
        (activation.root(self.repo) / "grant.json").unlink()
        with self.assertRaises(workflow.WorkflowError):
            self.apply()
        self.assertFalse((activation.root(self.repo) / "grant.json").exists())

    def test_old_grant_and_renamed_grant_cannot_dispatch_or_reset(self):
        with self.assertRaises(workflow.WorkflowError):
            activation.reserve(self.repo, number=10, input_digest="a" * 64, now=1002)
        self.apply()
        changed = copy.deepcopy(self.proposal)
        changed["grant"]["name"] = "another-name"
        changed["preview_digest"] = digest(changed["grant"])
        with self.assertRaises(workflow.WorkflowError):
            activation.apply(self.repo, changed, preview_digest=changed["preview_digest"], now=1002)

    def test_uncertain_reservation_and_missing_tenth_success_stop_sequence(self):
        self.apply()
        activation.reserve(self.repo, number=10, input_digest="a" * 64, now=1002)
        for number in (10, 11, 12, True):
            with self.subTest(number=number), self.assertRaises(workflow.WorkflowError):
                activation.reserve(self.repo, number=number, input_digest="a" * 64, now=1100)

    def test_two_exact_captures_allow_eleven_only_after_ten_and_never_twelve(self):
        self.apply()
        self.evidence(10)
        first = activation.complete(self.repo, number=10, now=1050)
        self.assertTrue(first["qualified"], first)
        directory = self.evidence(11)
        second = activation.complete(self.repo, number=11, now=1150)
        self.assertTrue(second["qualified"], second)
        self.assertFalse(review.coverage_ready(directory))
        with self.assertRaises(workflow.WorkflowError):
            activation.reserve(self.repo, number=12, input_digest="a" * 64, now=1160)
        with patch.object(activation, "context", side_effect=AssertionError("offline outcomes")):
            self.assertEqual(activation.outcome(self.repo, 11), second)

    def test_failure_or_late_completion_stops_without_refund(self):
        self.apply()
        self.evidence(10, failure=True)
        self.assertFalse(activation.complete(self.repo, number=10, now=1400)["qualified"])
        with self.assertRaises(workflow.WorkflowError):
            activation.reserve(self.repo, number=11, input_digest="a" * 64, now=1401)
        self.assertEqual(
            activation.read(activation.root(self.repo) / "attempt-10.json")["reserved_reference_usd"], 2
        )

    def test_mutated_exact_report_invalidates_prior_success_and_next_reservation(self):
        self.apply()
        directory = self.evidence(10)
        self.assertTrue(activation.complete(self.repo, number=10, now=1050)["qualified"])
        report = directory / "review.md"
        report.write_bytes(report.read_bytes() + b" ")
        with self.assertRaises(workflow.WorkflowError):
            activation.outcome(self.repo, 10)
        with self.assertRaises(workflow.WorkflowError):
            activation.reserve(self.repo, number=11, input_digest="a" * 64, now=1100)

    def test_expiry_rollback_and_nonfinite_window_refused(self):
        for now in (999, 1500, True, float("nan")):
            with self.subTest(now=now), self.assertRaises(workflow.WorkflowError):
                activation.apply(
                    self.repo, self.proposal, preview_digest=self.proposal["preview_digest"], now=now
                )
        self.apply()
        with self.assertRaises(workflow.WorkflowError):
            activation.reserve(self.repo, number=10, input_digest="a" * 64, now=1000)

    def test_changed_current_context_refuses_before_any_reservation(self):
        self.apply()
        for target, key in ((self.history, "ledger_digest"), (self.harness, "head")):
            original = target[key]
            target[key] = "e" * len(original)
            with self.assertRaises(workflow.WorkflowError):
                activation.reserve(self.repo, number=10, input_digest="a" * 64, now=1002)
            target[key] = original
        self.assertFalse((activation.root(self.repo) / "attempt-10.json").exists())

    def test_concurrent_claim_has_exactly_one_winner(self):
        from concurrent.futures import ThreadPoolExecutor

        self.apply()

        def claim(_):
            try:
                activation.reserve(self.repo, number=10, input_digest="a" * 64, now=1002)
                return True
            except workflow.WorkflowError:
                return False

        with ThreadPoolExecutor(max_workers=2) as pool:
            self.assertEqual(sorted(pool.map(claim, range(2))), [False, True])

    def test_valid_report_past_deadline_still_stops(self):
        self.apply()
        self.evidence(10)
        result = activation.complete(self.repo, number=10, now=1303)
        self.assertFalse(result["qualified"])
        with self.assertRaises(workflow.WorkflowError):
            activation.reserve(self.repo, number=11, input_digest="a" * 64, now=1304)

    def test_integration_rechecks_tenth_evidence_even_after_eleventh_completes(self):
        self.apply()
        first = self.evidence(10)
        self.assertTrue(activation.complete(self.repo, number=10, now=1050)["qualified"])
        self.evidence(11)
        self.assertTrue(activation.complete(self.repo, number=11, now=1150)["qualified"])
        (first / "terminal.txt").write_bytes(b"changed")
        with self.assertRaises(workflow.WorkflowError):
            activation.outcome(self.repo, 11)

    def test_outcome_mutation_duplicate_completion_and_bound_types_refuse(self):
        self.apply()
        self.evidence(10)
        activation.complete(self.repo, number=10, now=1050)
        with self.assertRaises(workflow.WorkflowError):
            activation.complete(self.repo, number=10, now=1050)
        path = activation.root(self.repo) / "outcome-10.json"
        value = activation.read(path)
        value["qualified"] = 1
        atomic_json(path, value)
        with self.assertRaises(workflow.WorkflowError):
            activation.outcome(self.repo, 10)
        for change in (
            lambda g: g["limits"].update(processes=3),
            lambda g: g["limits"].update(paid_extra_usd=False),
            lambda g: g.update(stop_on_failure=1),
            lambda g: g["slots"].append({"number": 12, "purpose": "native-tools-and-source"}),
        ):
            grant = copy.deepcopy(self.proposal["grant"])
            change(grant)
            with self.assertRaises(workflow.WorkflowError):
                activation.validate_grant(grant)

    def test_diagnostic_never_becomes_pr_ready_when_adapter_is_admitted(self):
        self.apply()
        directory = self.evidence(10)
        self.assertTrue(activation.complete(self.repo, number=10, now=1050)["qualified"])
        with patch.object(review_policy, "require_current_adapter"):
            self.assertFalse(review.coverage_ready(directory))
            with self.assertRaisesRegex(workflow.WorkflowError, "diagnostic"):
                review.qualification(directory, require=True)
            with self.assertRaisesRegex(workflow.WorkflowError, "diagnostic"):
                review.publication_body(directory)

    def test_reporting_without_source_probe_cannot_unlock_eleven(self):
        self.apply()

        def remove_probe(rows):
            identifiers = {
                b["id"]
                for row in rows
                for b in row.get("message", {}).get("content", [])
                if b.get("name") == "Glob"
            }
            rows[:] = [
                row
                for row in rows
                if not any(
                    b.get("id") in identifiers or b.get("tool_use_id") in identifiers
                    for b in row.get("message", {}).get("content", [])
                )
            ]

        self.evidence(10, mutate=remove_probe)
        self.assertFalse(activation.complete(self.repo, number=10, now=1050)["qualified"])
        with self.assertRaises(workflow.WorkflowError):
            activation.reserve(self.repo, number=11, input_digest="a" * 64, now=1100)

    def test_missing_advisory_preserves_incomplete_isolation_capture(self):
        self.apply()
        self.evidence(10)
        self.assertTrue(activation.complete(self.repo, number=10, now=1050)["qualified"])

        def remove_advisory(rows):
            rows[:] = [
                row for row in rows if row.get("subtype") != FIXTURE["positive"]["advisory"]["subtype"]
            ]

        directory = self.evidence(11, mutate=remove_advisory)
        self.assertTrue((directory / "review-capture.json").is_file())
        self.assertFalse(activation.complete(self.repo, number=11, now=1150)["qualified"])
        with self.assertRaises(workflow.WorkflowError):
            activation.reserve(self.repo, number=11, input_digest="a" * 64, now=1151)
