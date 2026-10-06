"""Same-revision continuation over exact simulated native captures and local Git."""

import json
from unittest.mock import patch

from test_workflow import review, workflow

# isort: split
import batch_fixtures
import review_batch
import review_batch_v7 as batch
import review_continuation as continuation
import test_review_batch_v7 as fixtures
from tasks import atomic_json, digest


class ContinuationTests(fixtures.ReportingBatchFixture):
    def execute_batch(self, **kwargs):
        if continuation.manifest(self.directory):
            auth = batch_fixtures.authorize(self.repo, self.directory, self.bounds)
            auth["name"] = "explicit synthetic successor, including failed unit reattempt"
            review_batch.select(self.directory, self.bounds, auth)
            with (
                self.isolated(process=self.ordinary_response),
                patch.object(review, "current_pr"),
                patch.object(review, "review", side_effect=self.child),
                patch.object(batch, "verify_harness"),
                patch.object(workflow, "Repo", return_value=self.repo),
            ):
                return review_batch.execute(self.repo, self.directory, **kwargs)
        return super().execute_batch(**kwargs)

    def stopped(self):
        self.fail_unit = batch.plan(self.directory)["units"][1]["id"]
        normal = self.ordinary_response

        def incomplete(args, **kw):
            result = normal(args, **kw)
            if self.ordinary.name == self.fail_unit:
                rows = [json.loads(line) for line in result.stdout.splitlines()]
                document = rows[-1]["structured_output"]
                document["reviewed"] = []
                body = json.dumps(document)
                for row in rows:
                    event = row.get("event", {})
                    if event.get("delta", {}).get("type") == "input_json_delta":
                        event["delta"]["partial_json"] = body
                    for block in row.get("message", {}).get("content", []):
                        if block.get("name") == "StructuredOutput":
                            block["input"] = document
                result.stdout = "\n".join(json.dumps(row) for row in rows).encode()
            return result

        with patch.object(self, "ordinary_response", side_effect=incomplete):
            with self.assertRaisesRegex(workflow.WorkflowError, "Incomplete unit"):
                self.execute_batch()
        self.ancestor = self.directory
        self.original = continuation.snapshot(self.ancestor)
        self.assertEqual(len(self.original["imports"]), 1)
        for number, unit in enumerate(batch.load(self.ancestor)["units"]):
            if unit["id"] in self.original["imports"]:
                self.reviews.append(
                    {
                        "id": 400 + number,
                        "body": review.publication_body(batch.unit_path(self.ancestor, unit)),
                        "commit_id": self.head,
                        "state": "COMMENTED",
                    }
                )
        return normal

    def successor_proposal(self, name="successor"):
        target = self.ancestor.parent / name
        with patch.object(batch, "verify_harness"), patch.object(review, "current_pr"):
            continuation.prepare(self.repo, self.ancestor, target)
        return target

    def test_successor_imports_exact_published_success_and_integrates_every_report(self):
        self.stopped()
        self.directory = self.successor_proposal()
        plan = batch.plan(self.directory)
        self.assertEqual(plan["continuation"]["snapshot"], self.original)
        self.bounds["requests"] = len(batch.work_units(plan))
        self.children = []
        self.execute_batch()
        self.assertEqual(continuation.snapshot(self.ancestor), self.original)
        self.assertNotIn(self.original["imports"][0], [p.name for p in self.children])
        self.assertIn(self.fail_unit, [p.name for p in self.children])
        self.assertEqual(len(self.children), len(batch.work_units(plan)))
        with patch.object(workflow, "Repo", return_value=self.repo):
            result = review.qualification(self.directory, require=True)
        self.assertTrue(result["qualified"])
        self.assertEqual(result["prior_consumption"], self.original["consumed"])
        old = self.ancestor / "units" / self.original["imports"][0] / "review.md"
        self.assertEqual(
            old.read_bytes(),
            (self.children[-1] / "packet/component-reports" / (old.parent.name + ".txt")).read_bytes(),
        )
        count = self.calls
        with patch.object(review, "review", side_effect=AssertionError("Recovery cannot infer")):
            review_batch.execute(self.repo, self.directory, recover_only=True)
        self.assertEqual(self.calls, count)
        original_api = self.repo.api
        imported_review_id = self.reviews[0]["id"]

        def public_double(suffix, *, data=None, **kwargs):
            if data is not None and suffix.endswith("/reviews"):
                item = {
                    "id": 1000 + len(self.reviews),
                    "body": data["body"],
                    "commit_id": data["commit_id"],
                    "state": "COMMENTED",
                    "html_url": "https://example.test/review",
                }
                self.reviews.append(item)
                return item
            return original_api(suffix, data=data, **kwargs)

        with (
            patch.object(self.repo, "api", side_effect=public_double),
            patch.object(workflow, "Repo", return_value=self.repo),
        ):
            review.publish(self.repo, self.directory)
            self.assertTrue(review.verify_publication(self.repo, self.directory)["exact_match"])
        self.assertEqual(sum(row["id"] == imported_review_id for row in self.reviews), 1)
        self.assertIn("imported exact COMMENT", self.reviews[-1]["body"])
        self.assertIn("Prior consumed reservations: 2", self.reviews[-1]["body"])
        self.reviews[0]["body"] += "changed"
        with (
            patch.object(workflow, "Repo", return_value=self.repo),
            self.assertRaises(workflow.WorkflowError),
        ):
            review.qualification(self.directory, require=True)

    def test_ancestor_mutations_unknown_calls_and_competing_successors_refuse(self):
        self.stopped()
        first = self.successor_proposal()
        second = self.successor_proposal("competing")
        selected = batch.select(
            first,
            self.bounds,
            {**batch_fixtures.authorize(self.repo, first, self.bounds), "name": "successor one"},
        )
        other = batch.select(
            second,
            self.bounds,
            {**batch_fixtures.authorize(self.repo, second, self.bounds), "name": "successor two"},
        )
        continuation.owner(self.repo, first, selected, create=True)
        with self.assertRaises(workflow.WorkflowError):
            continuation.owner(self.repo, second, other, create=True)
        continuation.owner(self.repo, first, selected)
        for relative in (
            "batch-state.json",
            "units/" + self.original["imports"][0] + "/review.md",
            "units/" + self.fail_unit + "/review-capture.json",
        ):
            path = self.ancestor / relative
            before = path.read_bytes()
            path.write_bytes(before + b"X")
            try:
                with self.assertRaises((workflow.WorkflowError, ValueError)):
                    batch.load(first)
            finally:
                path.write_bytes(before)
        state_path = self.ancestor / "batch-state.json"
        raw = state_path.read_bytes()
        state = json.loads(raw)
        state["reservations"][-1]["status"] = "reserved/uncertain"
        atomic_json(state_path, state)
        with self.assertRaises(workflow.WorkflowError):
            continuation.snapshot(self.ancestor)
        state_path.write_bytes(raw)
        self.assertEqual(digest(continuation.snapshot(self.ancestor)), digest(self.original))

    def test_second_stop_is_durable_and_changed_identity_or_harness_refuses(self):
        self.stopped()
        self.directory = self.successor_proposal()
        meta_path = self.directory / "metadata.json"
        raw = meta_path.read_bytes()
        changed = json.loads(raw)
        changed["head_sha"] = changed["base_sha"]
        atomic_json(meta_path, changed)
        with self.assertRaises(workflow.WorkflowError):
            batch.preview(self.directory, self.bounds)
        meta_path.write_bytes(raw)
        auth = batch_fixtures.authorize(self.repo, self.directory, self.bounds)
        auth["name"] = "new named successor"
        auth["harness_commit"] = "f" * 40
        with self.assertRaisesRegex(workflow.WorkflowError, "harness"):
            batch.select(self.directory, self.bounds, auth)
        self.children = []
        with patch.object(self, "ordinary_response", side_effect=RuntimeError("interrupted native process")):
            with self.assertRaises(RuntimeError):
                self.execute_batch()
        state = batch.state_for(self.directory, batch.load(self.directory))
        self.assertIsNotNone(state["stop_reason"])
        self.assertEqual(len(state["reservations"]), 1)
        with self.assertRaises(workflow.WorkflowError):
            batch.execute(self.repo, self.directory, resume=True)
        with self.assertRaises(workflow.WorkflowError):
            continuation.prepare(self.repo, self.directory, self.directory.parent / "implicit-chain")
        self.assertEqual(continuation.snapshot(self.ancestor), self.original)

    def test_managed_successor_adopts_only_designated_same_revision(self):
        import pipeline
        import tasks

        self.stopped()
        proposal = self.successor_proposal()
        tasks.approve_plan(self.repo, 12, 1234, "Synthetic task authority")
        tasks.prepare(self.repo, 12, "correct-value")
        pipeline.bind_pr(self.repo, 12, 31)
        store = tasks.TaskStore(self.repo)
        state = store.read("issue-12")
        policy = review.verify_packet(self.ancestor)["review_policy"]
        state["review_rounds"] = [
            {
                "head_sha": self.head,
                "base_sha": self.base,
                "contract_digest": digest(state["approval"]["contract"]),
                "review_policy": policy,
                "review_policy_digest": digest(policy),
                "directory": str(self.ancestor),
                "run_attempted": True,
                "status": "incomplete",
            }
        ]
        store.save(state)
        value = pipeline.review_task(
            self.repo,
            12,
            batch=True,
            fresh=True,
            approved_continuation=True,
            continue_reason="Explicit same-revision successor including failed unit",
            batch_successor=str(proposal),
            review_provider="claude-code",
            reporting={"max_turns": 80, "limits": policy["reporting"]["limits"]},
        )
        self.assertEqual(value["directory"], str(proposal))
        self.assertEqual(len(store.read("issue-12")["review_rounds"]), 2)
        self.assertEqual(continuation.snapshot(self.ancestor), self.original)
        with self.assertRaises(workflow.WorkflowError):
            pipeline.review_task(self.repo, 12, batch=True, batch_successor=str(proposal))

    def test_generation_renewal_requires_verified_lineage_and_retains_original_auth(self):
        import copy
        import uuid

        import claude_native_auth

        self.stopped()
        old = review.verify_packet(self.ancestor)["review_policy"]["authentication"]
        renewed = {**old, "generation_id": str(uuid.uuid4())}
        destination = self.ancestor.parent / "renewed-successor"
        with (
            patch.object(batch, "verify_harness"),
            patch.object(review, "current_pr"),
            patch.object(claude_native_auth, "capability_lineage", return_value=False),
        ):
            with self.assertRaisesRegex(workflow.WorkflowError, "lineage"):
                continuation.prepare(self.repo, self.ancestor, destination, authentication=renewed)
        self.assertFalse(destination.exists())
        observed = []

        def lineage(before, after, timeout):
            observed.append((copy.deepcopy(before), copy.deepcopy(after)))
            return before == after or before == old and after == renewed

        with (
            patch.object(claude_native_auth, "capability_lineage", side_effect=lineage),
            patch.object(claude_native_auth, "current_binding", return_value=renewed),
            patch.object(batch, "verify_harness"),
            patch.object(review, "current_pr"),
        ):
            continuation.prepare(self.repo, self.ancestor, destination, authentication=renewed)
            self.directory = destination
            self.execute_batch()
            with patch.object(workflow, "Repo", return_value=self.repo):
                self.assertTrue(review.qualification(destination, require=True)["qualified"])
        self.assertIn((old, renewed), observed)
        self.assertEqual(review.verify_packet(self.ancestor)["review_policy"]["authentication"], old)
        self.assertEqual(continuation.snapshot(self.ancestor), self.original)
