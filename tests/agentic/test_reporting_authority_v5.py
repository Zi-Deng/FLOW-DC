"""Exact prospective V5 authority; disposable records, never native activation."""

import copy
import json
import socket
import subprocess
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import claude_native_auth
import claude_owned_auth
import claude_reporting_policy_v8
import reporting_activation_v5 as activation
import review_claude
import review_policy
from claude_fixtures import AUTHENTICATION
from tasks import digest
from workflow import WorkflowError

# Independently transcribed from the verified publication/receipt for 6014789492.
# Do not derive the expected identity from the production constant under test.
CONTRACT = {
    "issue": 31,
    "plan_comment": 6014789492,
    "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
    "plan_digest": "e2fe530795df6701f43de54bb869032cd5375e92c9ecf543fa4e5a7d7830fb84",
}
CONTRACT_DIGEST = "980ea11c7ec6a2b3b1f3488c475ef9013957f525ef0f3cd115b87490f6fafec0"
HISTORICAL_CONTRACTS = (
    {
        **CONTRACT,
        "plan_comment": 6012492318,
        "plan_digest": "c0dbb09a3b7a03176988263aee50059c905af6750ef42de8042ff386e65b8461",
    },
    {
        **CONTRACT,
        "plan_comment": 6013795098,
        "plan_digest": "8351ce7ce762488c0f2447332487565c1c216e1f24ac440b237dc4d275277130",
    },
)


class AuthorityTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.repo = SimpleNamespace(main=Path(temporary.name), name="Zi-Deng/FLOW-DC")
        self.task = self.repo.main / ".agentic-local/tasks/issue-31.json"
        self.task.parent.mkdir(parents=True)
        self.state = {
            "repository": self.repo.name,
            "key": "issue-31",
            "approval": {
                "issue": 31,
                "plan_comment": 6014789492,
                "contract": copy.deepcopy(CONTRACT),
                "source": "Synthetic test provenance; not actual human approval",
                "recorded_at": "synthetic-time",
            },
            "approval_history": [],
        }
        self.write(self.state)
        self.guards = self.enterContext(ExitStack())
        self.forbidden = []
        for owner, name in (
            (claude_native_auth, "store"),
            (claude_native_auth, "current_binding"),
            (claude_owned_auth, "require"),
            (review_claude, "preflight"),
            (review_claude.review_process, "capture"),
            (subprocess, "Popen"),
            (socket.socket, "connect"),
            (activation, "exclusive"),
        ):
            self.forbidden.append(
                self.guards.enter_context(
                    patch.object(owner, name, side_effect=AssertionError("No native operation"))
                )
            )

    def write(self, state):
        self.task.write_text(json.dumps(state), encoding="utf-8")

    def positive(self):
        self.write(self.state)
        expected = {
            "contract_digest": CONTRACT_DIGEST,
            "approval_digest": digest(self.state["approval"]),
        }
        self.assertEqual(activation.authorization(self.repo), expected)
        return expected

    def refused(self, state):
        self.positive()
        self.write(state)
        with self.assertRaises(WorkflowError):
            activation.authorization(self.repo)
        self.assertFalse(activation.root(self.repo).exists())
        for guard in self.forbidden:
            guard.assert_not_called()

    def test_next_exact_contract_authorizes(self):
        self.assertEqual(digest(CONTRACT), CONTRACT_DIGEST)
        self.positive()
        self.assertFalse(activation.root(self.repo).exists())
        for guard in self.forbidden:
            guard.assert_not_called()

    def test_stale_and_malformed_current_authority_refuses(self):
        self.positive()
        for key in ("repository", "key"):
            state = copy.deepcopy(self.state)
            state[key] = "wrong"
            with self.subTest(state_key=key):
                self.refused(state)
        self.positive()
        with patch.object(self.repo, "name", "other/repository"):
            with self.assertRaises(WorkflowError):
                activation.authorization(self.repo)
        for value in (None, [], "approval", True):
            state = {**self.state, "approval": value}
            with self.subTest(approval=value):
                self.refused(state)
        state = copy.deepcopy(self.state)
        del state["approval"]
        self.refused(state)
        for key in ("issue", "plan_comment"):
            for value in (None, -1, True, float(self.state["approval"][key]), "31"):
                state = copy.deepcopy(self.state)
                state["approval"][key] = value
                with self.subTest(outer=key, value=value):
                    self.refused(state)
            state = copy.deepcopy(self.state)
            del state["approval"][key]
            self.refused(state)
        for key in CONTRACT:
            for remove in (False, True):
                state = copy.deepcopy(self.state)
                if remove:
                    del state["approval"]["contract"][key]
                else:
                    state["approval"]["contract"][key] = "changed"
                with self.subTest(contract=key, remove=remove):
                    self.refused(state)
        state = copy.deepcopy(self.state)
        state["approval"]["contract"]["extra"] = "unknown"
        self.refused(state)
        for contract in (*HISTORICAL_CONTRACTS, {**CONTRACT, "plan_comment": 1}):
            state = copy.deepcopy(self.state)
            state["approval"].update(plan_comment=contract["plan_comment"], contract=contract)
            state["approval_history"] = [self.state["approval"]]
            with self.subTest(old_plan=contract["plan_comment"]):
                self.refused(state)
        state = copy.deepcopy(self.state)
        state["approval_history"] = [state.pop("approval")]
        self.refused(state)
        for key in ("source", "recorded_at"):
            for value in (None, "", " \t\n", 1, True, []):
                state = copy.deepcopy(self.state)
                state["approval"][key] = value
                with self.subTest(provenance=key, value=value):
                    self.refused(state)
            state = copy.deepcopy(self.state)
            del state["approval"][key]
            self.refused(state)
        self.positive()
        with patch.object(activation, "CONTRACT_DIGEST", "f" * 64):
            with self.assertRaises(WorkflowError):
                activation.authorization(self.repo)
        self.positive()

    def test_provenance_is_bound_without_new_attestation_semantics(self):
        baseline = self.positive()
        for key in ("source", "recorded_at"):
            state = copy.deepcopy(self.state)
            state["approval"][key] = "different nonempty synthetic provenance"
            self.write(state)
            observed = activation.authorization(self.repo)
            self.assertEqual(observed["contract_digest"], CONTRACT_DIGEST)
            self.assertEqual(observed["approval_digest"], digest(state["approval"]))
            self.assertNotEqual(observed["approval_digest"], baseline["approval_digest"])
        self.positive()

    def test_authority_refuses_before_history_harness_auth_or_write(self):
        self.positive()
        policy = claude_reporting_policy_v8.build(
            {
                **review_policy.policy(review_policy.choices("claude-code"), {}, diagnostic=True),
                "authentication": copy.deepcopy(AUTHENTICATION),
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
        activation.validate_policy(policy)
        for contract in HISTORICAL_CONTRACTS:
            for owned in (None, object()):
                self.positive()
                stale = copy.deepcopy(self.state)
                stale["approval"].update(plan_comment=contract["plan_comment"], contract=contract)
                self.write(stale)
                with (
                    patch.object(
                        activation, "historical", side_effect=AssertionError("No history")
                    ) as history,
                    patch.object(activation, "harness", side_effect=AssertionError("No harness")) as harness,
                    self.assertRaisesRegex(WorkflowError, "current exact issue-31 approval"),
                ):
                    activation.context(self.repo, policy, owned_auth=owned)
                history.assert_not_called()
                harness.assert_not_called()
                self.assertFalse(activation.root(self.repo).exists())
                for guard in self.forbidden:
                    guard.assert_not_called()
