"""Explicit reviewer selection and immutable, provider-specific execution policy."""

from __future__ import annotations

import copy
import os
import re
import stat
from urllib.parse import urlsplit

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
        "adapter": "claude-stream-json-2.1.282-v6",
        "billing_mode": "included-max-subscription-only",
    },
}


def model_extensions(cfg):
    """Validate trusted compatibility declarations, never discover/fetch models.

    These declarations require operator verification against primary evidence.
    URL syntax checks cannot establish that evidence's truth or account entitlement.
    Records travel with packets so later configuration cannot reinterpret recovery.
    """
    entries = cfg.get("review_model_extensions", [])
    if not isinstance(entries, list) or len(entries) > 64:
        raise WorkflowError("Invalid review model compatibility declarations")
    if entries and cfg.get("schema_version", 2) != 2:
        raise WorkflowError("Model compatibility extensions require configuration schema 2")
    result = {}
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) != {
            "provider",
            "model",
            "efforts",
            "cli_version",
            "adapter",
            "evidence",
        }:
            raise WorkflowError("Incomplete model compatibility declaration")
        provider, model = entry["provider"], entry["model"]
        if not isinstance(provider, str) or provider not in PROVIDERS:
            raise WorkflowError("Unsupported model compatibility provider")
        spec = PROVIDERS[provider]
        if (
            not isinstance(model, str)
            or len(model) > 128
            or not re.fullmatch(r"[a-z][a-z0-9]*(?:[-.][a-z0-9]+)+", model)
            or not re.search(r"\d", model)
            or {"auto", "default", "latest"}.intersection(re.split(r"[-.]", model))
            or (provider == "claude-code" and not model.startswith("claude-"))
            or model in MODELS[provider]
            or (provider, model) in result
        ):
            raise WorkflowError("Model extension must name a unique exact model, not an alias or override")
        efforts = entry["efforts"]
        allowed = (
            CLAUDE_EFFORTS
            if provider == "claude-code"
            else ["default", "none", "minimal", "low", "medium", "high", "xhigh", "max"]
        )
        if (
            not isinstance(efforts, list)
            or not efforts
            or any(not isinstance(item, str) or item not in allowed for item in efforts)
            or len(set(efforts)) != len(efforts)
            or entry["cli_version"] != spec["cli"]["version"]
            or entry["adapter"] != spec["adapter"]
        ):
            raise WorkflowError("Model extension is incompatible with pinned CLI/adapter/effort controls")
        evidence = entry["evidence"]
        if not isinstance(evidence, list) or not 1 <= len(evidence) <= 8:
            raise WorkflowError("Model compatibility requires primary-source evidence references")
        for reference in evidence:
            if not isinstance(reference, str) or len(reference) > 512 or not reference.isascii():
                raise WorkflowError("Invalid model compatibility evidence reference")
            try:
                url = urlsplit(reference)
            except ValueError:
                raise WorkflowError("Invalid model compatibility evidence reference") from None
            domains = (
                {"code.claude.com", "platform.claude.com"}
                if provider == "claude-code"
                else {"docs.github.com"}
            )
            if (
                url.scheme != "https"
                or url.netloc not in domains
                or not url.path.startswith("/")
                or url.query
                or any(char.isspace() or ord(char) < 32 for char in reference)
            ):
                raise WorkflowError(
                    "Model compatibility evidence must reference public primary documentation"
                )
        result[provider, model] = copy.deepcopy(entry)
    return result


def model_catalog(cfg):
    catalog = copy.deepcopy(MODELS)
    for (provider, model), entry in model_extensions(cfg).items():
        catalog[provider][model] = entry["efforts"]
    return catalog


def choices(provider, model=None, effort=None, *, cfg=None):
    if not isinstance(provider, str) or provider not in PROVIDERS:
        raise WorkflowError("Unsupported review provider")
    spec = PROVIDERS[provider]
    model = spec["model"] if model is None else model
    effort = spec["effort"] if effort is None else effort
    catalog = model_catalog(cfg or {})[provider]
    if not isinstance(model, str) or model not in catalog or effort not in catalog[model]:
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
    selected = choices(**selection, cfg=cfg)
    spec = PROVIDERS[selected["provider"]]
    value = {
        "schema_version": 1,
        **selected,
        "cli": copy.deepcopy(spec["cli"]),
        "adapter": spec["adapter"],
        "billing_mode": spec["billing_mode"],
        "budget": budget(selected["provider"], cfg, diagnostic=diagnostic),
    }
    extension = model_extensions(cfg).get((selected["provider"], selected["model"]))
    if extension is not None:
        value["model_compatibility"] = extension
    return value


def validate_policy(value):
    if not isinstance(value, dict):
        raise WorkflowError("Missing immutable review policy")
    if type(value.get("schema_version")) is int and value["schema_version"] == 2:
        from claude_reporting_policy import validate

        return validate(value)
    if type(value.get("schema_version")) is not int or value["schema_version"] != 1:
        raise WorkflowError("Unsupported review policy version")
    selected = {key: value.get(key) for key in ("provider", "model", "effort")}
    legacy_claude = value.get("provider") == "claude-code" and value.get("adapter") in {
        "claude-stream-json-2.1.282-v1",
        "claude-stream-json-2.1.282-v2",
        "claude-stream-json-2.1.282-v3",
        "claude-stream-json-2.1.282-v4",
        "claude-stream-json-2.1.282-v5",
    }
    cfg = {}
    if "model_compatibility" in value:
        cfg["review_model_extensions"] = [copy.deepcopy(value["model_compatibility"])]
        if (
            legacy_claude
            and isinstance(cfg["review_model_extensions"][0], dict)
            and cfg["review_model_extensions"][0].get("adapter") == value["adapter"]
        ):
            cfg["review_model_extensions"][0]["adapter"] = PROVIDERS["claude-code"]["adapter"]
    selected = choices(**selected, cfg=cfg)
    limits = value.get("budget")
    if not isinstance(limits, dict):
        raise WorkflowError("Missing provider-specific budget")
    if type(limits.get("schema_version")) is not int or limits["schema_version"] != 1:
        raise WorkflowError("Unsupported review budget version")
    if selected["provider"] == "claude-code" and (
        type(limits.get("extra_spend_authorized_usd")) is not int or limits["extra_spend_authorized_usd"] != 0
    ):
        raise WorkflowError("Claude extra spending is not authorized")
    cfg.update(
        {
            "review_timeout_seconds": limits.get("timeout_seconds"),
            "review_max_ai_credits": limits.get("ai_credits"),
            "review_max_estimated_usd": limits.get("estimated_usd"),
        }
    )
    expected = policy(selected, cfg)
    if legacy_claude:
        expected["adapter"] = value["adapter"]
        if "model_compatibility" in expected:
            expected["model_compatibility"]["adapter"] = value["adapter"]
    if "authentication" in value:
        import claude_native_auth

        if selected["provider"] != "claude-code":
            raise WorkflowError("Native authentication cannot bind a different provider")
        expected["authentication"] = claude_native_auth.validate_binding(
            value["authentication"], current=False
        )
    if value != expected:
        raise WorkflowError("Immutable review policy differs from supported provider bindings")
    return value


def defaults(cfg):
    model_extensions(cfg)
    if cfg["schema_version"] == 1:
        return choices("copilot", cfg["copilot_model"], "default")
    return choices(
        cfg.get("review_provider", "claude-code"), cfg.get("review_model"), cfg.get("review_effort"), cfg=cfg
    )


def selection_path(repo):
    return plain_path(repo.main / ".agentic-local/review-selection.json")


def read_selection(repo, cfg=None):
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
        or selected != choices(**selected, cfg=cfg)
    ):
        raise WorkflowError("Invalid saved review selection")
    return selected


def resolve(repo, cfg, *, review_provider=None, review_model=None, review_effort=None, saved=True):
    selected = defaults(cfg)
    sources = dict.fromkeys(selected, "trusted-default")
    stored = read_selection(repo, cfg) if saved else None
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
        import claude_native_auth
        import review_diagnostics
        from review_claude import managed_controls

        native = claude_native_auth.status(result["policy"]["budget"]["timeout_seconds"])
        result["native_authentication"] = native
        blockers.extend(native["blockers"])
        if cfg.get("review_reporting") is not None and "authentication" in native:
            from claude_reporting_policy import build, selection

            result["policy"] = build(
                {**result["policy"], "authentication": native["authentication"]},
                **selection(result["policy"], cfg),
            )
        try:
            managed_controls()
        except (WorkflowError, OSError):
            blockers.append("managed_controls_require_verification")
        try:
            if result["policy"].get("schema_version") == 2:
                from reporting_admission import check as activation_check
            else:
                activation_check = review_diagnostics.require_activation
            result["native_capability"] = activation_check(
                repo,
                {
                    **result["policy"],
                    **({"authentication": native["authentication"]} if "authentication" in native else {}),
                },
            )
        except (WorkflowError, OSError, ValueError, KeyError):
            blockers.append("matching_native_capability_diagnostic_unavailable")
    result["activation_blockers"] = blockers
    result["supported_models"] = model_catalog(cfg)[result["policy"]["provider"]]
    result["model_compatibility_sources"] = {
        model: "built-in" if model in MODELS[result["policy"]["provider"]] else "trusted-config-declaration"
        for model in result["supported_models"]
    }
    result["note"] = (
        "Selection is not activation or evidence of included billing, isolation, capability or review readiness."
    )
    return result


def require_current_adapter(value):
    validate_policy(value)
    if value.get("schema_version") == 2:
        # Structural recognition only. Current readiness/dispatch separately
        # require both native reporting qualifications and the bound harness.
        return
    if value["adapter"] != PROVIDERS[value["provider"]]["adapter"]:
        raise WorkflowError("Historical review adapter is recovery-only; prepare a fresh packet")


def exact_amount(value, name, *, positive=True):
    """JSON decimal input, with no binary-float allowance or bool coercion."""
    from decimal import Decimal, InvalidOperation

    if type(value) not in {str, int, float}:
        raise WorkflowError(f"Invalid {name}")
    try:
        result = Decimal(str(value))
    except InvalidOperation:
        raise WorkflowError(f"Invalid {name}") from None
    if not result.is_finite() or result < 0 or (positive and result == 0):
        raise WorkflowError(f"Invalid {name}")
    return result


def batch_budget(value, parent_policy, units):
    """Reserve a full sequential review in the provider's own accounting domain."""
    from fractions import Fraction

    validate_policy(parent_policy)
    keys = {
        "requests",
        "kind",
        "cost",
        "seconds",
        "unit_cost",
        "unit_seconds",
        "max_report_bytes",
        "max_integration_bytes",
    }
    if not isinstance(value, dict) or set(value) != keys:
        raise WorkflowError("Explicit typed batch budget and report bounds are required")
    for key in ("requests", "seconds", "unit_seconds", "max_report_bytes", "max_integration_bytes"):
        if type(value[key]) is not int or value[key] <= 0:
            raise WorkflowError(f"Batch {key} must be a positive integer")
    if value["kind"] != parent_policy["budget"]["kind"]:
        raise WorkflowError("Batch cost kind differs from provider")
    cost, unit = (exact_amount(value[k], k) for k in ("cost", "unit_cost"))
    if (
        value["requests"] < units
        or Fraction(cost) < Fraction(unit) * units
        or value["seconds"] < value["unit_seconds"] * units
        or value["max_report_bytes"] > 50000
    ):
        raise WorkflowError("Full-review allocation cannot fund every component and integration")
    unit_policy = copy.deepcopy(parent_policy)
    field = "estimated_usd" if value["kind"] == "reference-usd" else "ai_credits"
    # Policy's established numeric syntax remains unchanged. Reject lossy conversion.
    numeric = int(unit) if unit == unit.to_integral_value() else float(unit)
    if exact_amount(numeric, field) != unit:
        raise WorkflowError("Per-unit allocation is not exactly representable by provider policy")
    if (
        unit > exact_amount(parent_policy["budget"][field], field)
        or value["unit_seconds"] > parent_policy["budget"]["timeout_seconds"]
    ):
        raise WorkflowError("Unit allocation exceeds immutable parent policy")
    unit_policy["budget"][field] = numeric
    unit_policy["budget"]["timeout_seconds"] = value["unit_seconds"]
    validate_policy(unit_policy)
    return {**value, "cost": str(cost), "unit_cost": str(unit)}, unit_policy
