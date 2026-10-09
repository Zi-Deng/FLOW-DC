"""Explicit provider selection and one bounded, read-only native invocation."""

import contextlib
import json
import os
import re
import shutil
import signal
import subprocess
import tempfile
import time
from pathlib import Path

import claude_auth
from workflow import WorkflowError, run, write_json

DEFAULTS = {"claude-code": ("claude-opus-5-5", "medium"), "copilot": ("claude-opus-5", "default")}


def selection(repo, config, *, review_provider=None, review_model=None, review_effort=None):
    saved_path = repo.state / "review-selection.json"
    saved = json.loads(saved_path.read_text()) if saved_path.exists() else {}
    # Old selection records have the same three values; no generation/replay compatibility.
    provider = review_provider or saved.get("provider") or config["review_provider"]
    if provider not in DEFAULTS:
        raise WorkflowError("Select claude-code or copilot")
    changed = review_provider is not None and review_provider != saved.get(
        "provider", config["review_provider"]
    )
    model = review_model or (
        DEFAULTS[provider][0] if changed else saved.get("model") or config["review_model"]
    )
    effort = review_effort or (
        DEFAULTS[provider][1] if changed else saved.get("effort") or config["review_effort"]
    )
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,100}", model) or model == "auto":
        raise WorkflowError("Select an explicit model, not auto")
    if effort not in {"default", "low", "medium", "high", "xhigh", "max"}:
        raise WorkflowError("Unsupported effort")
    if provider == "claude-code" and effort == "default":
        raise WorkflowError("Select an explicit Claude effort")
    return {"provider": provider, "model": model, "effort": effort}


def command(binary, policy, config, settings, mcp, prompt, schema):
    if policy["provider"] == "claude-code":
        args = [
            binary,
            "-p",
            "--safe-mode",
            "--restricted",
            "--tools",
            "Read,Grep,Glob",
            "--allowedTools",
            "Read,Grep,Glob",
            "--disallowedTools",
            "mcp__*",
            "--permission-mode",
            "dontAsk",
            "--permission-prompts",
            "none",
            "--setting-sources",
            "",
            "--settings",
            str(settings),
            "--strict-mcp-config",
            "--mcp-config",
            str(mcp),
            "--no-session-persistence",
            "--disable-slash-commands",
            "--model",
            policy["model"],
            "--effort",
            policy["effort"],
            "--max-turns",
            str(config["review_max_turns"]),
            "--max-budget-usd",
            str(config["review_max_estimated_usd"]),
            "--output-format",
            "json",
            "--json-schema",
            json.dumps(schema),
            prompt,
        ]
    else:
        args = [
            binary,
            "-p",
            prompt,
            "--model",
            policy["model"],
            "--available-tools=view,grep,glob",
            "--allow-tool=read",
            "--deny-tool=shell,write,url",
            "--disable-builtin-mcps",
            "--disable-mcp-server",
            "*",
            "--disallow-temp-dir",
            "--no-custom-instructions",
            "--no-ask-user",
            "--no-auto-update",
            "--no-bash-env",
            "--no-remote",
            "--no-remote-export",
            "--max-ai-credits",
            str(config["review_max_ai_credits"]),
            "--log-level",
            "none",
            "--silent",
        ]
        if policy["effort"] != "default":
            args += ["--effort", policy["effort"]]
    return args


def capture(args, *, cwd, env, seconds, output_limit=4_000_000, include_stderr=False):
    """Stop the owned process group on time/output exhaustion; never replay."""
    start = time.monotonic()
    reason = None
    with tempfile.TemporaryFile() as out, tempfile.TemporaryFile() as err:
        process = subprocess.Popen(args, cwd=cwd, env=env, stdout=out, stderr=err, start_new_session=True)
        try:
            while process.poll() is None:
                if time.monotonic() - start >= seconds:
                    reason = "timeout"
                elif os.fstat(out.fileno()).st_size + os.fstat(err.fileno()).st_size > output_limit:
                    reason = "output_limit"
                if reason:
                    break
                time.sleep(0.05)
        finally:
            # A successful parent must not leave owned background children behind.
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=2)
            finally:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        if os.fstat(out.fileno()).st_size + os.fstat(err.fileno()).st_size > output_limit:
            reason = reason or "output_limit"
        out.seek(0)
        raw = out.read(output_limit) if not reason else b""
        if include_stderr and not reason:
            err.seek(0)
            raw += err.read(output_limit - len(raw))
        return {
            "exit_status": process.returncode,
            "reason": reason,
            "elapsed_seconds": time.monotonic() - start,
        }, raw


@contextlib.contextmanager
def provider_environment(policy, seconds):
    if policy["provider"] == "claude-code":
        for name in ("managed-settings.json", "managed-settings.d", "managed-mcp.json"):
            if (Path("/etc/claude-code") / name).exists():
                raise WorkflowError("Endpoint-managed customization needs operator inspection")
        with claude_auth.snapshot(seconds) as env:
            yield env
    else:
        token = (
            os.environ.get("COPILOT_GITHUB_TOKEN")
            or os.environ.get("GH_TOKEN")
            or os.environ.get("GITHUB_TOKEN")
        )
        if not token:
            result = run(["gh", "auth", "token"], check=False)
            token = result.stdout.strip() if result.returncode == 0 else None
        if not token:
            raise WorkflowError("Copilot authentication unavailable")
        with tempfile.TemporaryDirectory(prefix="flowdc-copilot-home-") as home:
            yield {
                "HOME": home,
                "COPILOT_HOME": home,
                "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
                "LANG": "C.UTF-8",
                "COPILOT_GITHUB_TOKEN": token,
                "COPILOT_AUTO_UPDATE": "false",
                **{
                    k: os.environ[k]
                    for k in ("HTTPS_PROXY", "HTTP_PROXY", "NO_PROXY", "SSL_CERT_FILE")
                    if k in os.environ
                },
            }


def invoke(packet, policy, config, prompt, schema):
    binary = shutil.which("claude" if policy["provider"] == "claude-code" else "copilot")
    if not binary:
        raise WorkflowError("Selected reviewer CLI is not installed")
    version = run([binary, "--version"]).stdout.strip().splitlines()[0]
    if policy["provider"] == "claude-code":
        match = re.match(r"(\d+)\.(\d+)\.(\d+)", version)
        if not match or tuple(map(int, match.groups())) < (2, 1, 259):
            raise WorkflowError("Claude Code >=2.1.259 is required for the restricted review controls")
    with tempfile.TemporaryDirectory(prefix="flowdc-review-controls-") as controls:
        settings = Path(controls) / "settings.json"
        mcp = Path(controls) / "mcp.json"
        write_json(
            settings,
            {
                "disableAllHooks": True,
                "autoMemoryEnabled": False,
                "autoContinueAtUsageLimit": False,
                "fallbackModel": [],
                "availableModels": [policy["model"]],
                "enforceAvailableModels": True,
            },
        )
        write_json(mcp, {"mcpServers": {}})
        args = command(binary, policy, config, settings, mcp, prompt, schema)
        with provider_environment(policy, config["review_timeout_seconds"]) as env:
            result, raw = capture(args, cwd=packet, env=env, seconds=config["review_timeout_seconds"])
    result.update(
        cli_version=version,
        provider=policy["provider"],
        requested_model=policy["model"],
        wrapper_invocations=1,
        billing="included-Max-only" if policy["provider"] == "claude-code" else "Copilot credits",
    )
    if result["reason"] or result["exit_status"]:
        raise WorkflowError(
            f"Reviewer stopped: {result['reason'] or 'native CLI failure'}; no automatic retry"
        )
    try:
        text = raw.decode("utf-8")
        if policy["provider"] == "claude-code":
            native = json.loads(text)
            if native.get("is_error") or native.get("subtype") != "success":
                raise ValueError("unsuccessful result")
            body = native.get("structured_output")
            if body is None:
                body = json.loads(native["result"])
            result["estimated_usd"] = native.get("total_cost_usd")
            result["usage"] = native.get("usage")
            result["reported_models"] = list(native.get("modelUsage", {}))
        else:
            if text.strip().startswith("```json"):
                text = text.strip()[7:].removesuffix("```").strip()
            body = json.loads(text)
        return result, body
    except (ValueError, KeyError, TypeError, UnicodeError):
        raise WorkflowError("Reviewer output incomplete or malformed; no automatic retry") from None
