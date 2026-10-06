"""Global claim exclusion and partial-write failure with disposable real Git roots."""

import copy
from unittest.mock import patch

from test_workflow import GitFixture, workflow

# isort: split
import review_claims as claims
from tasks import digest


class ReviewClaimTests(GitFixture):
    def setUp(self):
        super().setUp()
        self.batch = {
            "binding": dict.fromkeys(
                ("repository", "pr", "issue", "plan_comment", "head_sha", "base_sha", "merge_base_sha"),
                "exact-identity",
            ),
            "contract_digest": "a" * 64,
            "authorization": {"name": "first finite grant"},
        }
        self.unit = {"id": "unit-0000", "required_ids": ["source-a", "test-a"]}
        self.binding = {"dispatch_id": "one"}

    def test_independently_prepared_grant_cannot_claim_same_material(self):
        first = claims.reserve(self.repo, self.batch, self.unit, self.binding)
        second = copy.deepcopy(self.batch)
        second["authorization"]["name"] = "another independently named grant"
        with self.assertRaisesRegex(workflow.WorkflowError, "already claimed"):
            claims.reserve(self.repo, second, self.unit, {"dispatch_id": "two"})
        self.assertEqual(
            claims.verify(self.repo, self.batch, self.unit, {"binding": self.binding, "claim": first}), first
        )

    def test_partial_claim_write_consumes_obligations_without_reset(self):
        original = claims.exclusive
        count = 0

        def interrupted(*args, **kwargs):
            nonlocal count
            count += 1
            if count == 2:
                raise OSError("interrupted claim persistence")
            return original(*args, **kwargs)

        with patch.object(claims, "exclusive", side_effect=interrupted):
            with self.assertRaises(OSError):
                claims.reserve(self.repo, self.batch, self.unit, self.binding)
        with self.assertRaises(workflow.WorkflowError):
            claims.reserve(self.repo, self.batch, self.unit, self.binding)

    def test_concurrent_global_lock_refuses_even_different_batch(self):
        with claims.locked(self.repo):
            with self.assertRaisesRegex(workflow.WorkflowError, "Another batch claim"):
                claims.reserve(self.repo, self.batch, self.unit, self.binding)

    def test_claim_deletion_or_mutation_cannot_qualify(self):
        value = claims.reserve(self.repo, self.batch, self.unit, self.binding)
        path = claims.root(self.repo) / value["keys"][0] / "0001.json"
        raw = path.read_bytes()
        path.write_bytes(raw + b"X")
        with self.assertRaises(workflow.WorkflowError):
            claims.verify(self.repo, self.batch, self.unit, {"binding": self.binding, "claim": value})
        path.write_bytes(raw)
        self.assertEqual(
            digest(
                claims.verify(self.repo, self.batch, self.unit, {"binding": self.binding, "claim": value})
            ),
            digest(value),
        )
        path.unlink()
        with self.assertRaises(workflow.WorkflowError):
            claims.verify(self.repo, self.batch, self.unit, {"binding": self.binding, "claim": value})
