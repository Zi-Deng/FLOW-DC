"""Exact JSON strings, authenticated pagination and hosted execution provenance."""

import io
import json
import os
import unittest
import urllib.error
import zipfile
from unittest.mock import Mock, patch

from test_workflow import workflow

# isort: split
import ci_evidence
import github_transport
import review_packet


class Response(io.BytesIO):
    def __init__(self, value, link=""):
        super().__init__(json.dumps(value, ensure_ascii=False).encode("utf-8"))
        self.headers = {"Link": link}


class TransportTests(unittest.TestCase):
    def test_exact_controls_crlf_visible_escapes_and_unicode_roundtrip(self):
        body = "\x1b[31mcafé\x07\r\nline two\r\n literal \\n \\u001b [ESC] [BEL]  "
        first = [{"id": 1, "body": body}]
        second = [{"id": 2, "body": "\n" + body}]
        opener = Mock()
        opener.open.side_effect = [
            Response(
                first, '<https://api.github.com/repos/example/project/pulls/31/reviews?page=2>; rel="next"'
            ),
            Response(second),
        ]
        with (
            patch.object(github_transport.urllib.request, "build_opener", return_value=opener),
            patch.dict(os.environ, {"GH_TOKEN": "fixture-token"}),
        ):
            result = github_transport.api(
                "example/project", "pulls/31/reviews", paginate=True, token_source=lambda: "unused"
            )
        self.assertEqual(result, first + second)
        self.assertEqual(result[0]["body"].encode(), body.encode())
        opener.open.side_effect = [Response({"body": body})]
        with (
            patch.object(github_transport.urllib.request, "build_opener", return_value=opener),
            patch.dict(os.environ, {"GH_TOKEN": "fixture-token"}),
        ):
            result = github_transport.api(
                "example/project", "pulls/31/reviews", data={"body": body}, token_source=lambda: "unused"
            )
        request = opener.open.call_args.args[0]
        self.assertEqual(json.loads(request.data)["body"].encode(), body.encode())
        self.assertEqual(result["body"], body)

    def test_repo_api_uses_exact_transport_instead_of_terminal_output(self):
        # This is also executed against the base module in the recorded regression
        # command. The old gh output path is deliberately modeled as lossy.
        body = "\x1b[1mquoted\x07\r\n\\u001b café"
        repo = object.__new__(workflow.Repo)
        repo.root = None
        repo._info = {"nameWithOwner": "example/project"}
        opener = Mock()
        opener.open.return_value = Response({"body": body})
        lossy = Mock(stdout=json.dumps({"body": body.replace("\x1b", "").replace("\x07", "")}))
        with (
            patch.object(github_transport.urllib.request, "build_opener", return_value=opener),
            patch.object(workflow, "run", return_value=lossy),
            patch.dict(os.environ, {"GH_TOKEN": "fixture-token"}),
        ):
            self.assertEqual(repo.api("pulls/31/reviews/1")["body"], body)

    def test_pagination_refuses_other_origins_repository_escape_and_cycles(self):
        prefix = "https://api.github.com/repos/example/project/"
        for url in [
            "https://evil.invalid/",
            "http://api.github.com/repos/example/project/issues",
            prefix + "../other/issues",
            prefix + "%2e%2e/other",
            prefix + "issues",
            prefix + "pulls/31/reviews?page=2",
        ]:
            opener = Mock()
            opener.open.return_value = Response([], f'<{url}>; rel="next"')
            with (
                patch.object(github_transport.urllib.request, "build_opener", return_value=opener),
                patch.dict(os.environ, {"GH_TOKEN": "fixture-token"}),
            ):
                with self.subTest(url=url), self.assertRaises(workflow.WorkflowError) as raised:
                    github_transport.api(
                        "example/project", "issues", paginate=True, token_source=lambda: "unused"
                    )
                self.assertNotIn("fixture-token", str(raised.exception))
                self.assertEqual(opener.open.call_count, 1)

    def test_paginated_check_runs_preserve_object_page_key(self):
        opener = Mock()
        opener.open.return_value = Response({"check_runs": [{"id": 1}]})
        with (
            patch.object(github_transport.urllib.request, "build_opener", return_value=opener),
            patch.dict(os.environ, {"GH_TOKEN": "fixture-token"}),
        ):
            self.assertEqual(
                github_transport.api(
                    "example/project",
                    "commits/abc/check-runs",
                    paginate=True,
                    page_key="check_runs",
                    token_source=lambda: "unused",
                ),
                [{"id": 1}],
            )

    def test_hosted_receipt_keeps_tested_merge_sha_distinct_from_pr_head(self):
        head, checkout = "a" * 40, "b" * 40
        repo = Mock(name="repo")
        repo.name = "example/project"
        execution = {"head_sha": head, "repository": {"full_name": repo.name}, "run_attempt": 2}
        artifacts = [{"id": 50, "name": f"validation-quality-{head}-2", "expired": False}]
        repo.api.side_effect = [execution, artifacts]
        receipt = {
            "schema_version": 1,
            "repository": repo.name,
            "pr_head_sha": head,
            "tested_checkout_sha": checkout,
            "run_id": 5,
            "run_attempt": 2,
            "check": "quality",
            "runner_environment": "github-hosted",
            "test_status": "success",
            "clean_status": "success",
        }
        checks = [
            {
                "id": 20,
                "name": "quality",
                "head_sha": head,
                "conclusion": "success",
                "details_url": "https://github.com/example/project/actions/runs/5/job/10",
            }
        ]
        with patch.object(ci_evidence, "artifact_receipt", return_value=(receipt, "c" * 64)):
            collected = ci_evidence.collect(repo, head, checks)
        context = {
            "pull_request": {"head": {"sha": head}},
            "check_runs": checks,
            "hosted_receipts": collected,
            "commit_statuses": [],
        }
        evidence = review_packet.validation_evidence(context, ["quality"])
        observed = evidence["required_checks"][0]["observations"][0]
        self.assertEqual(observed["head_association"], head)
        self.assertEqual(observed["tested_checkout_sha"], checkout)
        self.assertNotEqual(head, checkout)
        repo.api.side_effect = [{**execution, "head_sha": checkout}]
        with patch.object(ci_evidence, "artifact_receipt") as fetch:
            self.assertEqual(ci_evidence.collect(repo, head, checks)[0]["state"], "stale_run_association")
        fetch.assert_not_called()

    def test_hosted_receipt_refuses_local_execution_claim(self):
        with patch.dict(os.environ, {"GITHUB_ACTIONS": "true", "RUNNER_ENVIRONMENT": "self-hosted"}):
            with self.assertRaises(workflow.WorkflowError):
                ci_evidence.record("unused", "quality", "success", "success")

    def test_artifact_redirect_does_not_forward_authentication_or_extract_files(self):
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            archive.writestr("receipt.json", '{"schema_version":1}')
        opener = Mock()
        response = io.BytesIO(buffer.getvalue())
        opener.open.side_effect = [
            urllib.error.HTTPError(
                "withheld", 302, "", {"Location": "https://example.blob.core.windows.net/signed"}, None
            ),
            response,
        ]
        with (
            patch.object(github_transport.urllib.request, "build_opener", return_value=opener),
            patch.dict(os.environ, {"GH_TOKEN": "fixture-token"}),
        ):
            value, digest = github_transport.artifact_receipt("example/project", 1, lambda: "unused")
        self.assertEqual(value, {"schema_version": 1})
        self.assertEqual(len(digest), 64)
        self.assertEqual(
            opener.open.call_args_list[0].args[0].get_header("Authorization"), "Bearer fixture-token"
        )
        self.assertIsNone(opener.open.call_args_list[1].args[0].get_header("Authorization"))
        for host in ("https://evil.invalid/signed", "http://example.blob.core.windows.net/signed"):
            opener.open.side_effect = [urllib.error.HTTPError("withheld", 302, "", {"Location": host}, None)]
            with (
                patch.object(github_transport.urllib.request, "build_opener", return_value=opener),
                patch.dict(os.environ, {"GH_TOKEN": "fixture-token"}),
            ):
                with self.assertRaises(workflow.WorkflowError):
                    github_transport.artifact_receipt("example/project", 1, lambda: "unused")
