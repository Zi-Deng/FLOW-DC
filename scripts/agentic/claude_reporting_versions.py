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


def validate_diagnostic(policy):
    """Route preflight controls without changing historical grant semantics."""
    selected = version(policy)
    if selected is v7:
        from reporting_activation import validate_policy
    else:  # version() accepts only the exact v7/v8 identities above.
        from reporting_activation_v4 import validate_policy

    validate_policy(policy)


def validate_v6_diagnostic(repo, directory, meta, dispatch, *, owned_auth):
    """Separate exact claimed-V6 route; legacy diagnostic selection is unchanged."""
    from reporting_diagnostic_v6 import PURPOSE, validate_profile

    if type(meta) is not dict or meta.get("purpose") != PURPOSE:
        raise WorkflowError("V6 profile requires exact diagnostic metadata")
    version(meta.get("review_policy"))
    return validate_profile(repo, directory, meta, dispatch, owned_auth=owned_auth)
