"""Closed reporting-policy dispatch; old policy bytes and meaning stay frozen."""

import claude_reporting_policy as v7
import claude_reporting_policy_v8 as v8
from workflow import WorkflowError

selection = v7.selection
read_selection = v7.read_selection
build = v8.build


def version(policy):
    if (
        type(policy) is not dict
        or type(policy.get("schema_version")) is not int
        or policy["schema_version"] != 2
    ):
        raise WorkflowError("Unsupported reporting policy version")
    for module in (v7, v8):
        if policy.get("adapter") == module.ADAPTER:
            return module
    raise WorkflowError("Unsupported reporting adapter")


def validate(policy):
    return version(policy).validate(policy)


def validate_controls(data, policy):
    return version(policy).validate_controls(data, policy)
