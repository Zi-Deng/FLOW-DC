"""Pinned synthetic denial-chain regressions. None establishes live capability."""

import contextlib
import copy
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import patch

import test_claude_telemetry as fixtures
from test_workflow import GitFixture, review, workflow

# isort: split
import claude_native_auth
import claude_telemetry as telemetry
import claude_telemetry_v5 as frozen
import diagnostic_tool_contract as tool_contract
import review_claude
import review_coverage as coverage
import review_diagnostics
import review_policy

FIXTURE_PATH = Path(__file__).parent / "fixtures/claude-refusal-2.1.282-v5.json"
FIXTURE = json.loads(FIXTURE_PATH.read_text())


class ClaudeV6Tests(GitFixture):
    def setUp(self):
        super().setUp()
        version = patch.dict(review_policy.PROVIDERS["claude-code"], adapter="claude-stream-json-2.1.282-v6")
        version.start()
        self.addCleanup(version.stop)
        binding = patch.object(claude_native_auth, "current_binding", return_value=fixtures.AUTHENTICATION)
        binding.start()
        self.addCleanup(binding.stop)
        self.commit_task()
        self.directory = review.prepare(self.repo, 31, 12, 1234, review_provider="claude-code")
        self.packet = self.directory / "packet"
        self.policy = review.verify_packet(self.directory)["review_policy"]
        # The diagnostic fixture has a fixed literal, independent of ordinary packet canaries.
        (self.packet / "capability/fixture.txt").write_text("Read-only diagnostic\nCLAUDE_NATIVE_CANARY\n")
        (self.packet / "capability.json").write_text(
            json.dumps({"artifact": "capability/fixture.txt", "line": 2, "token": "CLAUDE_NATIVE_CANARY"})
        )
        meta = json.loads((self.directory / "metadata.json").read_text())
        for name in ("capability/fixture.txt", "capability.json"):
            meta["files"][name] = coverage.checksum((self.packet / name).read_text())
        review.atomic_json(self.directory / "metadata.json", meta)
        self.outside = str(self.parent / "outside-canary.txt")
        self.rows = fixtures.native_events(self.packet, self.packet, "fixture-session")
        for row in self.rows:
            msg = row.get("message")
            if isinstance(msg, dict):
                for block in msg.get("content", []):
                    if block.get("name") == "Grep":
                        block["input"] = copy.deepcopy(tool_contract.GREP)
        f = copy.deepcopy(FIXTURE["positive"])
        f["tool"]["message"]["content"][0]["input"]["file_path"] = self.outside
        f["denials"][0]["tool_input"]["file_path"] = self.outside
        message = f"{self.outside} is outside {self.packet}; --restricted confines the file tools to the working directory."
        f["advisory"]["message"] = message
        f["result"]["message"]["content"][0]["content"] = message
        self.rows[-1]["permission_denials"] = f["denials"]
        self.rows[-1:-1] = [f["tool"], f["advisory"], f["result"]]

    def evaluate(self, rows=None, purpose="isolation-refusal"):
        body, diag = telemetry.capture(
            "\n".join(json.dumps(x) for x in (self.rows if rows is None else rows)),
            self.packet,
            self.packet,
            self.policy,
            "fixture-session",
            refusal_path=self.outside,
            diagnostic_purpose=purpose,
            diagnostic_tool_contract=tool_contract.contract() if purpose else None,
        )
        meta = {"review_policy": self.policy, "diagnostic_purpose": purpose or "native-tools-and-source"}
        meta["diagnostic_tool_contract"] = tool_contract.contract()
        return review_diagnostics.assess_diagnostic(self.packet, body, diag, meta), diag, body

    def test_complete_chain_only_qualifies_bound_isolation_and_preserves_exact_report(self):
        result, diag, body = self.evaluate()
        self.assertTrue(result["qualified"], diag["reasons"])
        self.assertEqual(diag["telemetry"]["controlled_refusals"], 1)
        self.assertEqual(diag["schema_version"], 8)
        self.assertEqual(body.encode(), self.rows[-1]["result"].encode())
        self.assertFalse(coverage.assess(self.packet, body, diag, policy=self.policy)["qualified"])
        for purpose in (None, "native-tools-and-source"):
            result, diag, _ = self.evaluate(purpose=purpose)
            self.assertFalse(result["qualified"])
            self.assertEqual(diag["telemetry"]["controlled_refusals"], 0)
        telemetry.validate_summary(self.evaluate()[1]["telemetry"])

    def test_all_advisory_fields_required_exact_and_no_undefined_null_substitution(self):
        for key in FIXTURE["positive"]["advisory"]:
            for missing in (True, False):
                rows = copy.deepcopy(self.rows)
                if missing:
                    del rows[-3][key]
                else:
                    rows[-3][key] = None
                with self.subTest(key=key, missing=missing):
                    self.assertFalse(self.evaluate(rows)[0]["qualified"])
        for key in ("agent_id", "decision_reason_code", "parent_tool_use_id", "unknown"):
            rows = copy.deepcopy(self.rows)
            rows[-3][key] = None
            self.assertFalse(self.evaluate(rows)[0]["qualified"])

    def test_categories_unknown_identity_and_message_fail_closed(self):
        changes = [
            ("decision_reason_type", x)
            for x in ("mode", "rule", "hook", "classifier", "asyncAgent", "safetyCheck")
        ] + [
            ("decision_reason", "other"),
            ("tool_name", "Grep"),
            ("tool_use_id", "wrong"),
            ("session_id", "wrong"),
            ("uuid", "wrong"),
            ("message", "Permission to use Read has been denied"),
            ("subtype", "unknown"),
            ("type", "unknown"),
        ]
        for key, value in changes:
            rows = copy.deepcopy(self.rows)
            rows[-3][key] = value
            with self.subTest(key=key, value=value):
                self.assertFalse(self.evaluate(rows)[0]["qualified"])
        for name in ("mode", "outsideBlock", "delegated"):
            rows = copy.deepcopy(self.rows)
            rows[-3] = FIXTURE[name]
            self.assertFalse(self.evaluate(rows)[0]["qualified"])

    def test_terminal_binding_must_be_present_unique_and_exact(self):
        variants = [
            None,
            [],
            [{**self.rows[-1]["permission_denials"][0], "tool_use_id": "wrong"}],
            self.rows[-1]["permission_denials"] * 2,
            [{**self.rows[-1]["permission_denials"][0], "extra": None}],
            [
                {
                    "tool_name": "Read",
                    "tool_use_id": "canary-call",
                    "tool_input": {"file_path": self.outside, "limit": 1},
                }
            ],
        ]
        for value in variants:
            rows = copy.deepcopy(self.rows)
            rows[-1]["permission_denials"] = value
            self.assertFalse(self.evaluate(rows)[0]["qualified"])
        rows = copy.deepcopy(self.rows)
        del rows[-1]["permission_denials"]
        self.assertFalse(self.evaluate(rows)[0]["qualified"])

    def test_missing_duplicate_and_ordering_cannot_count_a_refusal(self):
        for index in (-4, -3, -2, -1):
            rows = copy.deepcopy(self.rows)
            del rows[index]
            self.assertFalse(self.evaluate(rows)[0]["qualified"])
            rows = copy.deepcopy(self.rows)
            rows.insert(index, copy.deepcopy(rows[index]))
            self.assertFalse(self.evaluate(rows)[0]["qualified"])
        for a, b in ((-4, -3), (-3, -2), (-2, -1), (0, -3)):
            rows = copy.deepcopy(self.rows)
            rows[a], rows[b] = rows[b], rows[a]
            self.assertFalse(self.evaluate(rows)[0]["qualified"])

    def test_wrong_inputs_wrappers_success_and_unrelated_errors_stay_incomplete(self):
        for change in (
            lambda r: r[-4]["message"]["content"][0]["input"].update(limit=1),
            lambda r: r[-4]["message"]["content"][0]["input"].update(file_path=self.outside + "/../alias"),
            lambda r: r[-4]["message"]["content"][0].update(id="x" * 257),
            lambda r: r[-4].update(agent_id=None),
            lambda r: r[-2]["message"]["content"][0].update(
                content="<tool_use_error>denied</tool_use_error>"
            ),
            lambda r: r[-2]["message"]["content"][0].update(
                is_error=False, content="HARMLESS_OUTSIDE_CANARY_fixture"
            ),
            lambda r: r[-2]["message"]["content"][0].update(extra="untrusted"),
            lambda r: r[0].update(plugins=["agents-md"]),
        ):
            rows = copy.deepcopy(self.rows)
            change(rows)
            self.assertFalse(self.evaluate(rows)[0]["qualified"])

    def test_bounded_privacy_diagnostics_never_retain_raw_provider_text(self):
        rows = copy.deepcopy(self.rows)
        rows[-3]["message"] = "PRIVATE_PROVIDER_TEXT"
        rows[-3]["decision_reason"] = "PRIVATE_PROVIDER_REASON"
        rows[-3]["PRIVATE_FIELD"] = "PRIVATE_VALUE"
        rows[-3:-3] = [copy.deepcopy(rows[-3]) for _ in range(80)]
        _, diag, _ = self.evaluate(rows)
        saved = json.dumps(diag)
        for value in (
            "PRIVATE_PROVIDER_TEXT",
            "PRIVATE_PROVIDER_REASON",
            "PRIVATE_FIELD",
            "PRIVATE_VALUE",
            self.outside,
            "canary-call",
            "fixture-session",
        ):
            self.assertNotIn(value, saved)
        telemetry.validate_summary(diag["telemetry"])

    def test_frozen_v5_preserves_report_diagnostics_and_incomplete_publication(self):
        policy = {**self.policy, "adapter": frozen.ADAPTER}
        raw = "\n".join(json.dumps(x) for x in self.rows)
        expected = frozen.capture(
            raw, self.packet, self.packet, policy, "fixture-session", refusal_path=self.outside
        )
        self.assertEqual(
            expected,
            telemetry.capture(
                raw, self.packet, self.packet, policy, "fixture-session", refusal_path=self.outside
            ),
        )
        body, diag = expected
        self.assertEqual(diag["schema_version"], 7)
        self.assertFalse(
            review_diagnostics.assess_diagnostic(
                self.packet, body, diag, {"review_policy": policy, "diagnostic_purpose": "isolation-refusal"}
            )["qualified"]
        )
        coverage.validate_diagnostics(diag, self.packet, policy)
        # Pinned from the original 6f865849 blob; works in shallow CI/export.
        self.assertEqual(
            hashlib.sha256(Path(frozen.__file__).read_bytes()).hexdigest(),
            "df51519d411be9514344bd4230cb4899aa17fd7c0309bf9767c72e02d3f16a59",
        )

    def test_exported_source_fixture_is_correlated_after_negative_exercises(self):
        node = shutil.which("node")
        if node is None:
            self.skipTest("Node unavailable; source fixture execution remains an explicit local check")
        output = json.loads(subprocess.check_output([node, str(FIXTURE_PATH.with_suffix(".mjs"))]))
        for key in (
            "positive",
            "mode",
            "outsideBlock",
            "delegated",
            "unresolved",
            "terminal_report_exact",
            "queue_drained",
        ):
            self.assertEqual(output[key], FIXTURE[key], key)
        self.assertEqual(output["positive"], FIXTURE["positive"])
        f = output["positive"]
        self.assertEqual(
            f["denials"],
            [
                {
                    "tool_name": "Read",
                    "tool_use_id": "canary-call",
                    "tool_input": f["tool"]["message"]["content"][0]["input"],
                }
            ],
        )
        self.assertEqual(f["advisory"]["message"], f["result"]["message"]["content"][0]["content"])

    def test_portable_fixture_preserves_native_source_and_selected_branch_boundaries(self):
        original = FIXTURE["permission_modes_source"]
        digest = "d18af403c90f39a41d6db3e2f5a823d1d5d0988b35e59ea05c27580d6bced6ce"
        self.assertEqual(hashlib.sha256(original["source"].encode()).hexdigest(), digest)
        self.assertEqual(original["sha256"], digest)
        self.assertTrue(original["source"].startswith(original["direct_prefix"]))
        self.assertTrue(original["source"].endswith(original["direct_return"]))
        native_remainder = original["source"][len(original["direct_prefix"]) :]
        self.assertTrue(native_remainder.startswith('if(_e.behavior==="allow")'))
        harness = FIXTURE_PATH.with_suffix(".mjs").read_text()
        selected = harness.split("// BEGIN selected direct-denial branch\n", 1)[1].split(
            "\n// END selected direct-denial branch", 1
        )[0]
        self.assertEqual(
            selected, original["direct_prefix"] + original["fixture_guard"] + original["direct_return"]
        )
        dont_ask_literal = harness.split("const dontAskPrefix=", 1)[1].split(";\n", 1)[0]
        self.assertTrue(original["source"].startswith(json.loads(dont_ask_literal)))

    def test_canary_location_type_modes_links_and_native_exempt_roots(self):
        with tempfile.TemporaryDirectory() as temporary, tempfile.TemporaryDirectory() as auth:
            root = Path(temporary)
            workspace = root / "workspace"
            workspace.mkdir()
            canary = root / "outside-refusal-canary.txt"
            canary.write_text("harmless")
            canary.chmod(0o600)
            env = {"HOME": auth}
            review_claude.validate_canary(canary, workspace, root, env)
            for bad in ({"HOME": temporary}, {"XDG_STATE_HOME": temporary}):
                with self.assertRaises(workflow.WorkflowError):
                    review_claude.validate_canary(canary, workspace, root, bad)
            canary.chmod(0o644)
            with self.assertRaises(workflow.WorkflowError):
                review_claude.validate_canary(canary, workspace, root, env)
            canary.chmod(0o600)
            os.link(canary, root / "hardlink")
            with self.assertRaises(workflow.WorkflowError):
                review_claude.validate_canary(canary, workspace, root, env)
            (root / "hardlink").unlink()
            canary.unlink()
            canary.symlink_to(workspace)
            with self.assertRaises(workflow.WorkflowError):
                review_claude.validate_canary(canary, workspace, root, env)
            canary.unlink()
            with self.assertRaises(workflow.WorkflowError):
                review_claude.validate_canary(canary, workspace, root, env)

    def check_v5_recovery(self, rows):
        policy = {**self.policy, "adapter": frozen.ADAPTER}
        raw = "\n".join(json.dumps(row) for row in rows)
        body, diag = frozen.capture(
            raw, self.packet, self.packet, policy, "fixture-session", refusal_path=self.outside
        )
        meta = review.verify_packet(self.directory)
        meta["review_policy"] = policy
        review.atomic_json(self.directory / "metadata.json", meta)
        review.save_result(self.directory, meta, body, diag, "2.1.282")
        review.recover_review(self.repo, self.directory)
        envelope = review.publication_body(self.directory)
        names = ("review-result.json", "review-capture.json", "coverage.json", "review.md")
        original = {name: (self.directory / name).read_bytes() for name in names}
        (self.directory / "review-result.json").unlink()
        with (
            patch.object(claude_native_auth, "current_binding", side_effect=AssertionError("No credentials")),
            patch.object(review_claude.review_process, "capture", side_effect=AssertionError("No inference")),
        ):
            review.recover_review(self.repo, self.directory)
            review.publish(self.repo, self.directory)
        self.assertEqual(original, {name: (self.directory / name).read_bytes() for name in names})
        self.assertEqual(envelope, review.publication_body(self.directory))
        self.assertFalse(review.coverage_ready(self.directory))
        with self.assertRaises(workflow.WorkflowError):
            review_policy.require_current_adapter(policy)

    def test_partial_v5_recovery_publication_stays_exact_and_incomplete(self):
        self.check_v5_recovery(self.rows)

    def test_complete_v5_recovery_publication_stays_historical_without_readiness(self):
        self.check_v5_recovery(fixtures.native_events(self.packet, self.packet, "fixture-session"))

    def test_decoded_canary_exposure_is_refused_even_in_escaped_json(self):
        rows = copy.deepcopy(self.rows)
        rows.insert(
            -1,
            {
                "type": "assistant",
                "session_id": "fixture-session",
                "message": {
                    "model": self.policy["model"],
                    "content": [{"type": "text", "text": "HARMLESS_OUTSIDE_CANARY_fixture-session"}],
                },
            },
        )
        raw = "\n".join(json.dumps(r) for r in rows).replace("HARMLESS_OUTSIDE", "\\u0048ARMLESS_OUTSIDE")
        _, diag = telemetry.capture(
            raw,
            self.packet,
            self.packet,
            self.policy,
            "fixture-session",
            refusal_path=self.outside,
            diagnostic_purpose="isolation-refusal",
            diagnostic_tool_contract=tool_contract.contract(),
        )
        self.assertIn("restricted_workspace_canary_exposed", diag["reasons"])
        self.assertEqual(diag["telemetry"]["controlled_refusals"], 0)

    def test_executor_supplies_verified_canary_and_purpose_and_cleans_after_interruption(self):
        paths = []

        @contextlib.contextmanager
        def snapshot(policy):
            with tempfile.TemporaryDirectory(prefix="synthetic-auth-") as home:
                yield review_claude.environment(Path(home)), lambda: None

        def capture(args, **kwargs):
            workspace = kwargs["cwd"]
            canary = workspace.parent / "outside-refusal-canary.txt"
            self.assertTrue(canary.is_file())
            self.assertEqual(canary.stat().st_mode & 0o777, 0o600)
            self.assertIn("only the file_path argument", args[-1])
            paths.append(workspace.parent)
            raise KeyboardInterrupt

        meta = review.verify_packet(self.directory)
        meta["diagnostic_purpose"] = "isolation-refusal"
        meta["diagnostic_tool_contract"] = tool_contract.contract()
        with (
            patch.object(review_claude, "preflight", return_value="/synthetic/claude"),
            patch.object(review_claude.review_cli, "executable", return_value="/synthetic/claude"),
            patch.object(review_claude, "managed_controls"),
            patch.object(claude_native_auth, "snapshot", side_effect=snapshot),
            patch.object(review_claude.review_process, "capture", side_effect=capture),
            self.assertRaises(KeyboardInterrupt),
        ):
            review_claude.execute(self.repo, self.directory, meta, diagnostic=True)
        self.assertEqual(len(paths), 1)
        self.assertFalse(paths[0].exists())

    def test_invalid_initialization_or_terminal_never_counts_and_other_errors_survive(self):
        for index, change in (
            (0, {"model": "wrong"}),
            (0, {"plugins": ["agents-md"]}),
            (-1, {"subtype": "error"}),
            (-1, {"is_error": True}),
        ):
            rows = copy.deepcopy(self.rows)
            rows[index].update(change)
            assessment, diag, _ = self.evaluate(rows)
            self.assertFalse(assessment["qualified"])
            self.assertEqual(diag["telemetry"]["controlled_refusals"], 0)
        rows = copy.deepcopy(self.rows)
        ordinary = next(r for r in rows if r["type"] == "user")
        ordinary["message"]["content"][0]["is_error"] = True
        assessment, diag, _ = self.evaluate(rows)
        self.assertFalse(assessment["qualified"])
        self.assertIn("tool_execution_failed", diag["reasons"])
        self.assertEqual(diag["telemetry"]["controlled_refusals"], 1)

    def test_refused_canary_never_changes_source_or_usage_evidence(self):
        ordinary = fixtures.native_events(self.packet, self.packet, "fixture-session")
        _, baseline = telemetry.capture(
            "\n".join(json.dumps(r) for r in ordinary),
            self.packet,
            self.packet,
            self.policy,
            "fixture-session",
        )
        _, diag, _ = self.evaluate()
        for field in ("events", "capability", "usage"):
            self.assertEqual(diag[field], baseline[field])

    def test_direct_caller_only_and_specific_predicates(self):
        for caller in ({"type": "direct"},):
            rows = copy.deepcopy(self.rows)
            rows[-4]["message"]["content"][0]["caller"] = caller
            result, diag, _ = self.evaluate(rows)
            self.assertTrue(result["qualified"], diag["reasons"])
        for caller in (None, "direct", True, [], {}, {"type": "server"}, {"type": "direct", "extra": None}):
            rows = copy.deepcopy(self.rows)
            rows[-4]["message"]["content"][0]["caller"] = caller
            result, diag, _ = self.evaluate(rows)
            self.assertFalse(result["qualified"])
            self.assertIn("controlled_refusal_call_caller_category", diag["reasons"])
        rows = copy.deepcopy(self.rows)
        rows[-4]["message"]["content"][0]["private-unknown-field"] = "private-unknown-value"
        result, diag, _ = self.evaluate(rows)
        self.assertFalse(result["qualified"])
        self.assertIn("controlled_refusal_call_envelope", diag["reasons"])
        self.assertIn("call.block_keys", diag["telemetry"]["control_fields"])
        self.assertNotIn("private-unknown", json.dumps(diag))

    def test_call_identity_input_and_id_predicates(self):
        for key, value in (
            ("agent_id", None),
            ("agentId", None),
            ("parent_tool_use_id", False),
            ("parent_tool_use_id", ""),
            ("session_id", "other"),
        ):
            rows = copy.deepcopy(self.rows)
            rows[-4][key] = value
            result, diag, _ = self.evaluate(rows)
            self.assertFalse(result["qualified"])
            self.assertIn("controlled_refusal_call_identity", diag["reasons"])
        for identifier in ("", "x" * 257, "é" * 129, None):
            rows = copy.deepcopy(self.rows)
            rows[-4]["message"]["content"][0]["id"] = identifier
            result, diag, _ = self.evaluate(rows)
            self.assertFalse(result["qualified"])
            self.assertIn("controlled_refusal_call_use_id", diag["reasons"])
        for value in (None, [], {}, {"file_path": self.outside, "offset": 1}, {"file_path": "other"}):
            rows = copy.deepcopy(self.rows)
            rows[-4]["message"]["content"][0]["input"] = value
            result, diag, _ = self.evaluate(rows)
            self.assertFalse(result["qualified"])
            self.assertIn("call.input", diag["telemetry"]["control_fields"])

    def test_ordinary_source_calls_reject_caller_and_delegation(self):
        for caller in (None, {}, {"type": "server"}, {"type": "direct", "extra": False}):
            rows = copy.deepcopy(self.rows)
            rows[1]["message"]["content"][0]["caller"] = caller
            self.assertFalse(self.evaluate(rows)[0]["qualified"])
        rows = copy.deepcopy(self.rows)
        rows[1]["message"]["content"][0]["caller"] = {"type": "direct"}
        self.assertTrue(self.evaluate(rows)[0]["qualified"])

    def test_exact_grep_contract_and_no_line_only_reconstruction(self):
        index = next(
            i
            for i, row in enumerate(self.rows)
            if isinstance(row.get("message"), dict)
            and any(b.get("name") == "Grep" for b in row["message"].get("content", []))
        )
        for key, value in (
            ("path", "capability/fixture.txt"),
            ("-n", 1),
            ("head_limit", True),
            ("head_limit", 10.0),
            ("glob", "*"),
            ("output_mode", "files_with_matches"),
            ("pattern", ".*"),
            ("offset", 0),
        ):
            rows = copy.deepcopy(self.rows)
            rows[index]["message"]["content"][0]["input"][key] = value
            result, diag, _ = self.evaluate(rows)
            self.assertFalse(result["qualified"])
            self.assertIn("diagnostic_grep_input_mismatch", diag["reasons"])
        for content in (
            "2:CLAUDE_NATIVE_CANARY",
            "capability/fixture.txt:2:wrong",
            "capability/fixture.txt:1:CLAUDE_NATIVE_CANARY",
            "capability/fixture.txt",
            "",
        ):
            rows = copy.deepcopy(self.rows)
            rows[index + 1]["message"]["content"][0]["content"] = content
            result, diag, _ = self.evaluate(rows)
            self.assertFalse(result["qualified"])
            self.assertFalse(diag["capability"]["grep"])
            self.assertIn("grep_no_qualifying_lines", diag["reasons"])
        rows = copy.deepcopy(self.rows)
        rows[index:index] = copy.deepcopy(rows[index : index + 2])
        self.assertFalse(self.evaluate(rows)[0]["qualified"])

    def test_contract_missing_or_tampered_refuses_before_authentication(self):
        meta = review.verify_packet(self.directory)
        meta["diagnostic_purpose"] = "native-tools-and-source"
        for value in (
            None,
            {},
            {"schema_version": True, "grep_canary": tool_contract.GREP},
            {**tool_contract.contract(), "extra": None},
        ):
            meta["diagnostic_tool_contract"] = value
            with patch.object(
                review_claude, "preflight", side_effect=AssertionError("No authenticated call")
            ):
                with self.assertRaises(workflow.WorkflowError):
                    review_claude.execute(self.repo, self.directory, meta, diagnostic=True)

    def test_observations_are_closed_bounded_and_frozen_helper_unchanged(self):
        root = Path(__file__).resolve().parents[2] / "scripts/agentic"
        for name, expected in {
            "claude_telemetry_v5.py": "df51519d411be9514344bd4230cb4899aa17fd7c0309bf9767c72e02d3f16a59",
            "claude_refusal.py": "08421d149dfeba92b90d0c0613a1603ac6bcfa352a67d881e13d9a6c307dfd27",
        }.items():
            self.assertEqual(hashlib.sha256((root / name).read_bytes()).hexdigest(), expected)
        summary = self.evaluate()[1]["telemetry"]
        for n in range(100):
            telemetry.observe_control(
                summary, "call.input", {"input": {"private-" + str(n): "sensitive-" + str(n)}}, "input"
            )
        telemetry.validate_summary(summary)
        self.assertLessEqual(len(summary["control_fields"]["call.input"]["observations"]), 8)
        self.assertNotIn("sensitive-", json.dumps(summary))
        invalid = copy.deepcopy(summary)
        invalid["control_fields"]["unrecognized"] = {}
        with self.assertRaises(workflow.WorkflowError):
            telemetry.validate_summary(invalid)

    def test_malformed_tool_envelopes_observed_before_dispatch(self):
        for field in ("type", "id", "name", "input"):
            rows = copy.deepcopy(self.rows)
            del rows[-4]["message"]["content"][0][field]
            result, diag, _ = self.evaluate(rows)
            self.assertFalse(result["qualified"])
            self.assertIn("tool_call_envelope", diag["reasons"])
            self.assertIn("call.block_keys", diag["telemetry"]["control_fields"])

    def test_nonobject_block_shape_is_observed_without_creating_a_call(self):
        rows = copy.deepcopy(self.rows)
        rows[-4]["message"]["content"][0] = None
        result, diag, _ = self.evaluate(rows)
        self.assertFalse(result["qualified"])
        self.assertEqual(diag["telemetry"]["controlled_refusals"], 0)
        fields = diag["telemetry"]["control_fields"]
        self.assertEqual(fields["call.block"]["observations"][0]["type"], "null")

    def test_ordinary_no_match_search_does_not_invalidate_other_successful_evidence(self):
        rows = fixtures.native_events(self.packet, self.packet, "fixture-session")
        call = next(
            row
            for row in rows
            if isinstance(row.get("message"), dict)
            and any(b.get("name") == "Grep" for b in row["message"].get("content", []))
        )
        call = copy.deepcopy(call)
        call["message"]["content"][0].update(
            id="no-match", input={"pattern": "absent", "path": ".", "output_mode": "content", "-n": True}
        )
        result = {
            "type": "user",
            "session_id": "fixture-session",
            "message": {
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": "no-match",
                        "is_error": False,
                        "content": "No matches found",
                    }
                ]
            },
        }
        rows[-1:-1] = [call, result]
        body, diag = telemetry.capture(
            "\n".join(json.dumps(row) for row in rows),
            self.packet,
            self.packet,
            self.policy,
            "fixture-session",
        )
        self.assertTrue(
            coverage.assess(self.packet, body, diag, policy=self.policy)["qualified"], diag["reasons"]
        )
        self.assertEqual(diag["events"][-1]["spans"], [])
