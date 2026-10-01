"""Explicit reviewer selection and immutable, provider-specific execution policy."""

from __future__ import annotations

import copy
import os
import stat

from copilot_policy import CLI_ARCHIVE_SHA256, CLI_VERSION
from tasks import atomic_json, plain_path, private_directory
from workflow import WorkflowError

# Exact provider spellings, not aliases. This compatibility table is deliberately
# bounded to documented native models/efforts; account entitlement is a separate
# preflight/runtime concern and never authorizes fallback to another entry.
CLAUDE_EFFORTS = ["low", "medium", "high", "xhigh", "max"]
MODELS = {
    "claude-code": {
        "claude-opus-5-5": CLAUDE_EFFORTS,
        "claude-opus-5": CLAUDE_EFFORTS,
        "claude-sonnet-5": CLAUDE_EFFORTS,
        "claude-opus-4-7": CLAUDE_EFFORTS,
        "claude-sonnet-4-6": ["low", "medium", "high", "max"],
    },
    "copilot": {
        "claude-opus-5": ["default", *CLAUDE_EFFORTS],
        "claude-opus-5.5": ["default", *CLAUDE_EFFORTS],
        "claude-sonnet-5": ["default", *CLAUDE_EFFORTS],
        "claude-sonnet-4.6": ["default", "low", "medium", "high", "max"],
        "claude-haiku-4.5": ["default"],
        "gpt-5.4": ["default", "none", "low", "medium", "high", "xhigh"],
        "gpt-6-astra": ["default"],
        "gpt-6-sol": ["default"],
        "gpt-6-luna": ["default"],
    },
}

PROVIDERS = {
    "copilot": {
        "model": "claude-opus-5",
        "effort": "default",
        "efforts": ["default"],
        "cli": {"version": CLI_VERSION, "platform": "linux-x64", "archive_sha256": CLI_ARCHIVE_SHA256},
        "adapter": "copilot-session-events-v2",
        "billing_mode": "copilot-ai-credits",
    },
    "claude-code": {
        "model": "claude-opus-5-5",
        "effort": "medium",
        "efforts": ["low", "medium", "high", "xhigh", "max"],
        "cli": {
            "version": "2.1.282",
            "platform": "linux-x64",
            "binary_sha256": "3afe8535c0cc33f0e24f7b25dab7a1727b8b592196f8496a8bc302ba2161eed3",
            "manifest_sha256": "041abb14aba47e7dd31f8ba83d8102e54b6350d099382cb1def7ab10add415ed",
            "signing_fingerprint": "31DDDE24DDFAB679F42D7BD2BAA929FF1A7ECACE",
        },
        "adapter": "claude-stream-json-2.1.282-v1",
        "billing_mode": "included-max-subscription-only",
    },
}


def choices(provider, model=None, effort=None):
    if not isinstance(provider, str) or provider not in PROVIDERS:
        raise WorkflowError("Unsupported review provider")
    spec = PROVIDERS[provider]
    model = spec["model"] if model is None else model
    effort = spec["effort"] if effort is None else effort
    if not isinstance(model, str) or model not in MODELS[provider] or effort not in MODELS[provider][model]:
        raise WorkflowError("Unsupported exact review model/provider/effort combination")
    return {"provider": provider, "model": model, "effort": effort}


def budget(provider, cfg, *, diagnostic=False):
    if diagnostic:
        if provider != "claude-code":
            raise WorkflowError("Migration diagnostics are Claude-only")
        return {
            "schema_version": 1,
            "kind": "reference-usd",
            "timeout_seconds": 300,
            "estimated_usd": 2,
            "extra_spend_authorized_usd": 0,
        }
    timeout = cfg.get("review_timeout_seconds", 900)
    if type(timeout) is not int or not 0 < timeout <= 900:
        raise WorkflowError("Reviewer timeout must be at most 900 seconds")
    if provider == "copilot":
        amount = cfg.get("review_max_ai_credits", 400)
        if type(amount) is not int or amount <= 0:
            raise WorkflowError("Invalid Copilot credit budget")
        return {"schema_version": 1, "kind": "ai-credits", "timeout_seconds": timeout, "ai_credits": amount}
    amount = cfg.get("review_max_estimated_usd", 10)
    if type(amount) not in {int, float} or not 0 < amount <= 10:
        raise WorkflowError("Claude reference-cost ceiling must be positive and at most $10")
    return {
        "schema_version": 1,
        "kind": "reference-usd",
        "timeout_seconds": timeout,
        "estimated_usd": amount,
        "extra_spend_authorized_usd": 0,
    }


def policy(selection, cfg, *, diagnostic=False):
    selected = choices(**selection)
    spec = PROVIDERS[selected["provider"]]
    return {
        "schema_version": 1,
        **selected,
        "cli": copy.deepcopy(spec["cli"]),
        "adapter": spec["adapter"],
        "billing_mode": spec["billing_mode"],
        "budget": budget(selected["provider"], cfg, diagnostic=diagnostic),
    }


def validate_policy(value):
    if not isinstance(value, dict):
        raise WorkflowError("Missing immutable review policy")
    if type(value.get("schema_version")) is not int or value["schema_version"] != 1:
        raise WorkflowError("Unsupported review policy version")
    selected = {key: value.get(key) for key in ("provider", "model", "effort")}
    selected = choices(**selected)
    limits = value.get("budget")
    if not isinstance(limits, dict):
        raise WorkflowError("Missing provider-specific budget")
    if type(limits.get("schema_version")) is not int or limits["schema_version"] != 1:
        raise WorkflowError("Unsupported review budget version")
    if selected["provider"] == "claude-code" and (
        type(limits.get("extra_spend_authorized_usd")) is not int or limits["extra_spend_authorized_usd"] != 0
    ):
        raise WorkflowError("Claude extra spending is not authorized")
    cfg = {
        "review_timeout_seconds": limits.get("timeout_seconds"),
        "review_max_ai_credits": limits.get("ai_credits"),
        "review_max_estimated_usd": limits.get("estimated_usd"),
    }
    if value != policy(selected, cfg):
        raise WorkflowError("Immutable review policy differs from supported provider bindings")
    return value


def defaults(cfg):
    if cfg["schema_version"] == 1:
        return choices("copilot", cfg["copilot_model"], "default")
    return choices(
        cfg.get("review_provider", "claude-code"), cfg.get("review_model"), cfg.get("review_effort")
    )


def selection_path(repo):
    return plain_path(repo.main / ".agentic-local/review-selection.json")


def read_selection(repo):
    path = selection_path(repo)
    if not path.exists():
        return None
    info = path.stat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o600:
        raise WorkflowError("Saved review selection must be an owner-only regular file")
    from review_coverage_v2 import read_json

    value = read_json(path)
    if (
        not isinstance(value, dict)
        or set(value) != {"schema_version", "selection"}
        or type(value["schema_version"]) is not int
        or value["schema_version"] != 1
    ):
        raise WorkflowError("Unsupported saved review selection")
    selected = value["selection"]
    if (
        not isinstance(selected, dict)
        or set(selected) != {"provider", "model", "effort"}
        or selected != choices(**selected)
    ):
        raise WorkflowError("Invalid saved review selection")
    return selected


def resolve(repo, cfg, *, review_provider=None, review_model=None, review_effort=None, saved=True):
    selected = defaults(cfg)
    sources = dict.fromkeys(selected, "trusted-default")
    stored = read_selection(repo) if saved else None
    if stored:
        selected, sources = stored, dict.fromkeys(selected, "saved-selection")
    if review_provider is not None:
        selected = choices(review_provider)
        sources = dict.fromkeys(selected, "per-call-provider-default")
        sources["provider"] = "per-call"
    for key, value in (("model", review_model), ("effort", review_effort)):
        if value is not None:
            selected[key], sources[key] = value, "per-call"
    return {"policy": policy(selected, cfg), "sources": sources}


def save_selection(repo, cfg, **overrides):
    repo.assert_main()
    result = resolve(repo, cfg, **overrides)
    selected = {key: result["policy"][key] for key in ("provider", "model", "effort")}
    private_directory(repo.main / ".agentic-local")
    atomic_json(selection_path(repo), {"schema_version": 1, "selection": selected})
    return resolve(repo, cfg)


def add_arguments(parser):
    for field in ("provider", "model", "effort"):
        parser.add_argument("--review-" + field)


def status(repo, cfg, **overrides):
    result = resolve(repo, cfg, **overrides)
    blockers = []
    import review_cli

    try:
        review_cli.executable(repo, result["policy"]["provider"])
    except (WorkflowError, OSError, ValueError):
        blockers.append("verified_pinned_cli_unavailable")
    if result["policy"]["provider"] == "claude-code":
        import claude_credentials
        import review_diagnostics
        from review_claude import managed_controls

        blockers.extend(claude_credentials.status())
        try:
            managed_controls()
        except (WorkflowError, OSError):
            blockers.append("managed_controls_require_verification")
        try:
            review_diagnostics.require_activation(repo, result["policy"])
        except (WorkflowError, OSError, ValueError, KeyError):
            blockers.append("matching_native_capability_diagnostic_unavailable")
    result["activation_blockers"] = blockers
    result["supported_models"] = copy.deepcopy(MODELS[result["policy"]["provider"]])
    result["note"] = (
        "Selection is not activation or evidence of included billing, isolation, capability or review readiness."
    )
    return result
