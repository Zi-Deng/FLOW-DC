"""Prospective full native validation and storage; no activation or provider calls."""

import copy
import json
from unittest.mock import patch

from test_workflow import GitFixture, review, workflow

# isort: split
import claude_native_auth
import claude_telemetry_v7 as telemetry
import review_coverage as coverage
import test_claude_reporting as reporting_fixtures
from claude_fixtures import AUTHENTICATION, native_events
from test_claude_reporting import AUXILIARY, LIMITS


class ClaudeV7Tests(GitFixture):
    def setUp(self):
        super().setUp()
        binding = patch.object(claude_native_auth, "current_binding", return_value=AUTHENTICATION)
        binding.start()
        self.addCleanup(binding.stop)
        self.commit_task()
        self.directory = review.prepare(
            self.repo,
            31,
            12,
            1234,
            review_provider="claude-code",
            reporting={"max_turns": 80, "limits": LIMITS},
        )
        self.packet = self.directory / "packet"
        self.meta = review.verify_packet(self.directory)
        self.policy = self.meta["review_policy"]
        rows = native_events(self.packet, self.packet, "session")
        self.body = " \r\n" + rows[-1]["result"] + "\r\n"
        value = json.loads(self.body)
        reporting_rows = reporting_fixtures.ReportingTests().native_rows()[3:]
        reporting_rows[2]["event"]["delta"]["partial_json"] = self.body
        reporting_rows[3]["message"]["content"][0]["input"] = value
        reporting_rows[-1].update(rows[-1], result=AUXILIARY, structured_output=value)
        rows[0]["tools"].append("StructuredOutput")
        self.rows = rows[:-1] + reporting_rows
        for i, row in enumerate(self.rows):
            row["uuid"] = f"original_{i}"

    def evaluate(self, rows=None):
        return telemetry.capture(
            "\n".join(json.dumps(r) for r in (self.rows if rows is None else rows)),
            self.packet,
            self.packet,
            self.policy,
            "session",
        )

    def test_full_stream_keeps_report_bytes_and_independent_inspection(self):
        body, diagnostic, proof = self.evaluate()
        self.assertEqual(body.encode(), self.body.encode())
        self.assertEqual(proof["terminal_text"], AUXILIARY)
        self.assertTrue(proof["accepted"], proof["reasons"])
        self.assertEqual(diagnostic["reasons"], [])
        self.assertTrue(all(diagnostic["capability"].values()))
        self.assertEqual(diagnostic["schema_version"], 9)
        self.assertNotIn("StructuredOutput", [e["tool"] for e in diagnostic["events"]])
        self.assertTrue(coverage.assess(self.packet, body, diagnostic, policy=self.policy)["qualified"])

    def test_partial_observation_failure_survives_capture_recovery_and_tampering_refuses(self):
        rows = copy.deepcopy(self.rows)
        rows.insert(
            1,
            {
                "type": "stream_event",
                "uuid": "bad_partial",
                "session_id": "session",
                "event": {"type": "unknown-private-kind", "private": "not-retained"},
            },
        )
        body, diagnostic, proof = self.evaluate(rows)
        observed = diagnostic["telemetry"]["partial_stream"]
        self.assertEqual(observed["counts"], {"message_active": 1})
        self.assertTrue(proof["accepted"])
        self.assertEqual(diagnostic["reasons"], ["unsupported_partial_stream"])
        telemetry.validate_summary(diagnostic["telemetry"])
        self.assertFalse(coverage.assess(self.packet, body, diagnostic, policy=self.policy)["qualified"])
        review.save_result(self.directory, self.meta, body, diagnostic, "2.1.282", reporting=proof)
        capture_path = self.directory / "review-capture.json"
        original = capture_path.read_bytes()
        (self.directory / "review-result.json").unlink()
        with patch("review_claude.execute", side_effect=AssertionError("no inference")):
            review.recover_review(self.repo, self.directory)
        self.assertEqual(capture_path.read_bytes(), original)
        self.assertEqual(json.loads((self.directory / "diagnostics.json").read_bytes()), diagnostic)
        self.assertEqual((self.directory / "review.md").read_bytes(), body.encode())
        self.assertFalse(review.qualification(self.directory)["qualified"])
        # Even a structurally valid changed observation must fail the existing
        # capture/result diagnostic hash, not silently become new evidence.
        for name in ("review-capture.json", "review-result.json", "diagnostics.json"):
            path = self.directory / name
            raw = path.read_bytes()
            changed = json.loads(raw)
            target = changed if name == "diagnostics.json" else changed["diagnostics"]
            target["telemetry"]["partial_stream"]["samples"][0]["evaluation_error"] = True
            path.write_text(json.dumps(changed))
            with self.subTest(name=name), self.assertRaises(workflow.WorkflowError):
                review.qualification(self.directory)
            path.write_bytes(raw)
        changed = copy.deepcopy(diagnostic)
        changed["reasons"] = []
        with self.assertRaises(workflow.WorkflowError):
            coverage.assess(self.packet, body, changed, policy=self.policy)
        changed = copy.deepcopy(diagnostic)
        changed["telemetry"]["partial_stream"]["samples"][0]["private"] = "private"
        with self.assertRaises(workflow.WorkflowError):
            coverage.assess(self.packet, body, changed, policy=self.policy)

    def test_old_optional_absence_retains_assessment_and_new_empty_record_earns_no_credit(self):
        for rows in (self.rows, [self.rows[0], self.rows[-1]]):
            body, diagnostic, _ = self.evaluate(rows)
            original = coverage.assess(self.packet, body, diagnostic, policy=self.policy)
            old = copy.deepcopy(diagnostic)
            del old["telemetry"]["partial_stream"]
            self.assertEqual(coverage.assess(self.packet, body, old, policy=self.policy), original)
        # Unknown-stream acceptance remains unchanged, also without the new field.
        rows = copy.deepcopy(self.rows)
        rows.insert(
            1,
            {
                "type": "stream_event",
                "uuid": "unknown",
                "session_id": "session",
                "event": {"type": "unknown"},
            },
        )
        body, diagnostic, proof = self.evaluate(rows)
        old = copy.deepcopy(diagnostic)
        del old["telemetry"]["partial_stream"]
        self.assertEqual(
            coverage.assess(self.packet, body, old, policy=self.policy),
            coverage.assess(self.packet, body, diagnostic, policy=self.policy),
        )
        self.assertFalse(coverage.assess(self.packet, body, old, policy=self.policy)["qualified"])
        review.save_result(self.directory, self.meta, body, old, "2.1.282", reporting=proof)
        original_capture = (self.directory / "review-capture.json").read_bytes()
        (self.directory / "review-result.json").unlink()
        with patch("review_claude.execute", side_effect=AssertionError("no inference")):
            review.recover_review(self.repo, self.directory)
        self.assertEqual((self.directory / "review-capture.json").read_bytes(), original_capture)
        self.assertNotIn(
            "partial_stream", json.loads((self.directory / "diagnostics.json").read_bytes())["telemetry"]
        )

    def test_successful_reporting_alone_never_earns_inspection(self):
        rows = [self.rows[0]] + self.rows[-8:]
        body, diagnostic, proof = self.evaluate(rows)
        self.assertTrue(proof["accepted"])
        self.assertFalse(any(diagnostic["capability"].values()))
        self.assertFalse(coverage.assess(self.packet, body, diagnostic, policy=self.policy)["qualified"])

    def test_full_controls_still_reject_with_valid_reporting(self):
        for field, value in (
            ("tools", ["Read", "Grep", "Glob", "StructuredOutput", "Bash"]),
            ("plugins", ["untrusted"]),
            ("permissionMode", "bypassPermissions"),
            ("session_id", "foreign"),
            ("agent_id", "delegated"),
        ):
            rows = copy.deepcopy(self.rows)
            rows[0][field] = value
            with self.subTest(field=field):
                body, diagnostic, _ = self.evaluate(rows)
                self.assertFalse(
                    coverage.assess(self.packet, body, diagnostic, policy=self.policy)["qualified"]
                )
        for field, value in (("total_cost_usd", None), ("num_turns", True), ("num_turns", 81)):
            rows = copy.deepcopy(self.rows)
            rows[-1][field] = value
            with self.subTest(field=field, value=value):
                self.assertTrue(self.evaluate(rows)[1]["reasons"])

    def test_capture_recovery_replays_proof_without_inference_and_binds_auxiliary(self):
        body, diagnostic, proof = self.evaluate()
        review.save_result(self.directory, self.meta, body, diagnostic, "2.1.282", reporting=proof)
        (self.directory / "review-result.json").unlink()
        with patch("review_claude.execute", side_effect=AssertionError("no inference")):
            review.recover_review(self.repo, self.directory)
        self.assertEqual((self.directory / "review.md").read_bytes(), self.body.encode())
        self.assertEqual((self.directory / "terminal.txt").read_bytes(), AUXILIARY.encode())
        self.assertTrue(review.qualification(self.directory)["qualified"])
        # Prospective storage is not activation; the old grant cannot enable v7.
        self.assertFalse(review.coverage_ready(self.directory))
        with self.assertRaises(workflow.WorkflowError):
            review.qualification(self.directory, require=True)
        (self.directory / "terminal.txt").write_bytes(b"changed")
        with self.assertRaises(workflow.WorkflowError):
            review.qualification(self.directory)

    def test_policy_and_proof_mutations_fail_closed(self):
        for key, value in (
            ("max_turns", True),
            ("max_turns", 0),
            ("retry_limit", 2),
            ("schema_sha256", "0" * 64),
        ):
            policy = copy.deepcopy(self.policy)
            policy["reporting"][key] = value
            with self.subTest(key=key):
                with self.assertRaises(workflow.WorkflowError):
                    review.review_policy.validate_policy(policy)
        body, diagnostic, proof = self.evaluate()
        for key, value in (("report", body + " "), ("terminal_text", "changed"), ("accepted", False)):
            changed = copy.deepcopy(proof)
            changed[key] = value
            with self.subTest(key=key):
                with self.assertRaises(workflow.WorkflowError):
                    review.save_result(
                        self.directory, self.meta, body, diagnostic, "2.1.282", reporting=changed
                    )
        self.assertFalse((self.directory / "review-capture.json").exists())

    def test_missing_report_is_captured_as_incomplete_without_auxiliary_substitution(self):
        body, diagnostic, proof = self.evaluate([self.rows[0], self.rows[-1]])
        self.assertEqual(body, "")
        self.assertFalse(proof["accepted"])
        review.save_result(self.directory, self.meta, body, diagnostic, "2.1.282", reporting=proof)
        review.recover_review(self.repo, self.directory)
        self.assertEqual((self.directory / "review.md").read_bytes(), b"")
        self.assertFalse(review.qualification(self.directory)["qualified"])

    def test_foreign_partial_stream_and_forbidden_calls_cannot_hide_behind_projection(self):
        variants = []
        for field, value in (("name", "Bash"), ("caller", {"type": "agent"})):
            rows = copy.deepcopy(self.rows)
            rows[1]["message"]["content"][0][field] = value
            variants.append(rows)
        rows = copy.deepcopy(self.rows)
        rows[2]["message"]["content"][0]["content"] = "[REDACTED]"
        variants.append(rows)
        for change in ({"type": "unexpected"}, {"type": "content_block_stop", "index": True}):
            rows = copy.deepcopy(self.rows)
            rows.insert(
                -8,
                {"type": "stream_event", "uuid": "foreign_partial", "session_id": "session", "event": change},
            )
            variants.append(rows)
        for i, rows in enumerate(variants):
            with self.subTest(i=i):
                body, diagnostic, _ = self.evaluate(rows)
                self.assertFalse(
                    coverage.assess(self.packet, body, diagnostic, policy=self.policy)["qualified"]
                )

    def test_each_reporting_artifact_and_bound_capture_is_revalidated(self):
        body, diagnostic, proof = self.evaluate()
        review.save_result(self.directory, self.meta, body, diagnostic, "2.1.282", reporting=proof)
        review.recover_review(self.repo, self.directory)
        self.assertTrue(review.qualification(self.directory)["qualified"])
        for name in (
            "review.md",
            "terminal.txt",
            "reporting-proof.json",
            "diagnostics.json",
            "coverage.json",
            "review-capture.json",
            "review-result.json",
        ):
            path = self.directory / name
            original = path.read_bytes()
            with self.subTest(name=name):
                path.write_bytes(b"{}" if name.endswith(".json") else b"changed")
                with self.assertRaises(workflow.WorkflowError):
                    review.qualification(self.directory)
                path.write_bytes(original)
        self.assertTrue(review.qualification(self.directory)["qualified"])
        published = review.publication_body(self.directory)
        self.assertIn(self.body, published)
        self.assertNotIn(AUXILIARY, published)
        self.assertIn("INCOMPLETE prospective", published)
        self.assertIn("Claude Code", published)

    def test_command_and_environment_bind_finite_reporting_without_enabling_dispatch(self):
        import tempfile
        from pathlib import Path

        import review_claude

        cmd = review_claude.command("never-run", self.policy, "session", "settings", "mcp", "prompt")
        for flag, value in (
            ("--tools", "Read,Grep,Glob,StructuredOutput"),
            ("--allowedTools", "Read,Grep,Glob,StructuredOutput"),
            ("--max-turns", "80"),
            ("--json-schema", self.policy["reporting"]["schema_text"]),
            ("--permission-mode", "dontAsk"),
            ("--output-format", "stream-json"),
        ):
            self.assertEqual(cmd[cmd.index(flag) + 1], value)
        self.assertIn("--include-partial-messages", cmd)
        with tempfile.TemporaryDirectory() as temporary:
            with patch.dict(
                "os.environ",
                {"MAX_STRUCTURED_OUTPUT_RETRIES": "999", "ANTHROPIC_API_KEY": "synthetic-never-use"},
            ):
                env = review_claude.environment(Path(temporary), policy=self.policy)
            self.assertEqual(env["MAX_STRUCTURED_OUTPUT_RETRIES"], "1")
            self.assertNotIn("ANTHROPIC_API_KEY", env)
        original_validate = claude_native_auth.validate_binding

        def stored_binding_only(binding, *, current=True):
            self.assertFalse(current, "Prospective preflight must not read current credentials")
            return original_validate(binding, current=False)

        with (
            patch.object(claude_native_auth, "validate_binding", side_effect=stored_binding_only),
            patch.object(review_claude.review_process, "capture", side_effect=AssertionError("no process")),
        ):
            with self.assertRaises(workflow.WorkflowError):
                review_claude.preflight(self.repo, self.policy)

    def test_pinned_retry_and_turn_source_expressions_are_finite_not_api_call_counts(self):
        import hashlib
        import shutil
        import subprocess
        from pathlib import Path

        fixture = json.loads(
            (Path(__file__).parent / "fixtures/claude-reporting-controls-2.1.282-source.json").read_text()
        )
        snippets = fixture["excerpts"]
        for row in snippets.values():
            if "source" in row:
                self.assertEqual(hashlib.sha256(row["source"].encode()).hexdigest(), row["sha256"])
        script = (
            "const assert=require('node:assert/strict');\n" + snippets["report-attempt-counter"]["source"]
        )
        script += (
            "\nconst exhausted=(s,sn,el,xt,Ge=1)=>{const ui='StructuredOutput';return "
            + snippets["retry-exhaustion"]["source"]
            + ";};"
        )
        script += "\nconst next=(En,A)=>{let " + snippets["turn-bound"]["source"] + ";return ir;};"
        script += """
const call={type:'assistant',message:{content:[{type:'tool_use',name:'StructuredOutput'}]}};
assert.equal(exhausted([],0,0,[]),false);
assert.equal(exhausted([call],0,0,[]),true);
assert.equal(exhausted([call],0,0,[{data:{}}]),false);
assert.equal(exhausted([],1,0,[]),true);
assert.equal(next(79,80),undefined);
assert.equal(next(80,80),80);
console.log('synthetic pinned source only');
"""
        node = shutil.which("node")
        self.assertIsNotNone(node, "Pinned source expression test requires Node")
        result = subprocess.run([node, "-e", script], capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_v6_assessment_is_frozen_and_cannot_gain_reporting_semantics(self):
        import claude_telemetry
        import claude_telemetry_v6

        directory = review.prepare(self.repo, 31, 12, 1234, review_provider="claude-code")
        packet = directory / "packet"
        meta = review.verify_packet(directory)
        rows = native_events(packet, packet, "historical")
        raw = "\n".join(json.dumps(row) for row in rows)
        expected = claude_telemetry_v6.capture(raw, packet, packet, meta["review_policy"], "historical")
        self.assertEqual(
            claude_telemetry.capture(raw, packet, packet, meta["review_policy"], "historical"), expected
        )
        body, diagnostic = expected
        with patch.object(coverage, "assess", side_effect=AssertionError("No current evaluator for v6")):
            review.save_result(directory, meta, body, diagnostic, "2.1.282")
            review.recover_review(self.repo, directory)
            self.assertTrue(review.qualification(directory)["qualified"])
        with self.assertRaises(workflow.WorkflowError):
            review.save_result(directory, meta, body, diagnostic, "2.1.282", reporting=self.evaluate()[2])
        policy = copy.deepcopy(meta["review_policy"])
        policy.update(schema_version=2, adapter=self.policy["adapter"], reporting=self.policy["reporting"])
        meta["review_policy"] = policy
        (directory / "metadata.json").write_text(json.dumps(meta))
        with self.assertRaises(workflow.WorkflowError):
            review.verify_packet(directory)

    def test_assessment_failure_is_recoverable_but_reporting_capture_cannot_be_overwritten(self):
        body, diagnostic, proof = self.evaluate()
        with patch.object(coverage, "assess", side_effect=OSError("temporary source failure")):
            with self.assertRaises(OSError):
                review.save_result(self.directory, self.meta, body, diagnostic, "2.1.282", reporting=proof)
        exact_capture = (self.directory / "review-capture.json").read_bytes()
        self.assertFalse((self.directory / "review-result.json").exists())
        with patch("review_claude.execute", side_effect=AssertionError("no inference")):
            review.recover_review(self.repo, self.directory)
        self.assertEqual((self.directory / "review-capture.json").read_bytes(), exact_capture)
        altered = copy.deepcopy(diagnostic)
        altered["reasons"].append("provider_exit_failure")
        with self.assertRaises(workflow.WorkflowError):
            review.save_result(self.directory, self.meta, body, altered, "2.1.282", reporting=proof)
        self.assertEqual((self.directory / "review-capture.json").read_bytes(), exact_capture)

    def test_oversized_saved_artifact_is_refused_before_json_parsing(self):
        path = self.directory / "review-capture.json"
        with path.open("wb") as stream:
            stream.truncate(self.policy["reporting"]["capture_bytes"] + 1)
        with patch.object(coverage, "strict_json", side_effect=AssertionError("No oversized parse")):
            with self.assertRaises(workflow.WorkflowError):
                review.read_result_artifact(self.directory, path.name, self.meta)

    def test_capture_comparison_does_not_equate_booleans_with_integers(self):
        body, diagnostic, proof = self.evaluate()
        review.save_result(self.directory, self.meta, body, diagnostic, "2.1.282", reporting=proof)
        review.recover_review(self.repo, self.directory)
        capture_path = self.directory / "review-capture.json"
        value = json.loads(capture_path.read_bytes())
        self.assertIs(value["reporting"]["accepted"], True)
        value["reporting"]["accepted"] = 1
        capture_path.write_text(json.dumps(value))
        with self.assertRaises(workflow.WorkflowError):
            review.qualification(self.directory)

    def test_cli_qualification_and_unstarted_dispatch_remain_blocked(self):
        import contextlib
        import io

        import review_claude

        with patch.object(review_claude, "execute", side_effect=AssertionError("No prospective process")):
            with self.assertRaisesRegex(workflow.WorkflowError, "separate reporting activation"):
                review.review(self.repo, self.directory)
        self.assertFalse((self.directory / "attempt.json").exists())
        self.assertEqual(json.loads((self.directory / "diagnostics.json").read_bytes())["schema_version"], 9)
        body, diagnostic, proof = self.evaluate()
        review.save_result(self.directory, self.meta, body, diagnostic, "2.1.282", reporting=proof)
        review.recover_review(self.repo, self.directory)
        with (
            patch.object(review, "Repo", return_value=self.repo),
            patch("sys.argv", ["review.py", "qualify", str(self.directory)]),
            contextlib.redirect_stderr(io.StringIO()) as errors,
            patch.object(review, "qualification", wraps=review.qualification) as qualify,
        ):
            self.assertEqual(review.main(), 1)
            qualify.assert_called_once_with(str(self.directory), require=True)
            self.assertIn(
                "Missing reporting dispatch admission; storage-only evidence is not ready", errors.getvalue()
            )

    def test_tool_schema_resolves_in_pinned_ajv_dialect_without_changing_report_contract(self):
        import hashlib
        import shutil
        import subprocess
        from pathlib import Path

        fixture = json.loads(
            (Path(__file__).parent / "fixtures/claude-reporting-schema-2.1.282-source.json").read_text()
        )
        excerpts = fixture["excerpts"]
        for row in excerpts.values():
            self.assertEqual(hashlib.sha256(row["source"].encode()).hexdigest(), row["sha256"])
        # Exact pinned registration/dispatch methods; the validator behind the
        # registered dialect is a stub. Full bundled compilation is separately
        # exercised in the retained source audit, never by invoking Claude.
        script = "const assert=require('node:assert/strict'); let " + excerpts["dialect-uri"]["source"] + ";"
        script += """
const Dh={default:class {
 constructor(){this.refs={};this.schemas={};this.opts={meta:true};this.logger=console;}
 _addDefaultMetaSchema(){} defaultMeta(){} addMetaSchema(schema,id){this.schemas[id]=()=>true;}
 getSchema(id){return this.schemas[id.replace(/#$/,'')];}
}}, Vh={default:[]},zh={},ka={},Uh=[];
"""
        script += excerpts["dialect-class"]["source"]
        script += (
            "\nObject.assign(_t.prototype,{"
            + excerpts["validate"]["source"]
            + ","
            + excerpts["validate-schema"]["source"]
            + "});"
        )
        script += "\nconst validator=new _t();validator._addDefaultMetaSchema();\n"
        original = json.loads((self.packet / "report-schema.json").read_bytes())
        tool = json.loads(self.policy["reporting"]["schema_text"])
        self.assertEqual(
            {k: v for k, v in original.items() if k != "$schema"},
            {k: v for k, v in tool.items() if k != "$schema"},
        )
        script += "const original=" + json.dumps(original) + ";const tool=" + json.dumps(tool) + ";"
        script += "assert.throws(()=>validator.validateSchema(original),/no schema with key or ref/);assert.equal(validator.validateSchema(tool),true);"
        node = shutil.which("node")
        self.assertIsNotNone(node)
        result = subprocess.run([node, "-e", script], capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
