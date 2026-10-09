"""One dedicated Max login; access-only snapshots, no generation compatibility."""

import contextlib
import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from workflow import WorkflowError, write_json

STORE = Path.home() / ".config/flowdc-agentic/claude-review-login"
REFRESH_MARGIN = 300  # Reserve the native CLI's early-refresh window without sending a refresh token.


def private_json(path):
    path = Path(path)
    mode = path.lstat()
    if (
        not stat.S_ISREG(mode.st_mode)
        or mode.st_uid != os.getuid()
        or mode.st_mode & 0o077
        or mode.st_size > 131072
    ):
        raise WorkflowError("Reviewer credential file must be owner-only, regular and bounded")
    return json.loads(path.read_text())


def profile():
    pointer = STORE / "current-profile.json"
    if pointer.exists():
        value = private_json(pointer)
        path = Path(value["path"])
    else:
        # One-time reuse of the user's completed dedicated login; no old runtime imports.
        registration = private_json(STORE / "registration.json")
        generation = registration["authentication"]["generation_id"]
        if not isinstance(generation, str) or "/" in generation or ".." in generation:
            raise WorkflowError("Invalid dedicated profile")
        path = STORE / "generations" / generation / "config"
    resolved = path.resolve()
    if not resolved.is_relative_to(STORE.resolve()) or path.is_symlink():
        raise WorkflowError("Dedicated login escaped its private store")
    return resolved


def validate(credentials, config, receipt, seconds, now=None):
    now = time.time() if now is None else now
    token = credentials.get("claudeAiOauth", {})
    account = config.get("oauthAccount", {})
    if token.get("subscriptionType") != "max" or account.get("hasExtraUsageEnabled") is not False:
        raise WorkflowError("Max subscription with paid extra usage disabled is required")
    if (
        receipt.get("paid_usage_disabled") is not True
        or not 0 <= now - receipt.get("recorded_at", 0) <= 7 * 86400
    ):
        raise WorkflowError("Current paid-extra-disabled observation is required")
    if (
        type(token.get("expiresAt")) not in (int, float)
        or token["expiresAt"] / 1000 <= now + seconds + REFRESH_MARGIN + 60
    ):
        raise WorkflowError("Dedicated reviewer login needs renewal")
    if not isinstance(token.get("accessToken"), str) or len(token["accessToken"]) < 16:
        raise WorkflowError("Dedicated reviewer access token unavailable")
    if not {"user:profile", "user:inference"}.issubset(token.get("scopes", [])):
        raise WorkflowError("Dedicated login lacks inference scope")
    if any(k in config for k in ("primaryApiKey", "apiKeyHelper", "env", "profiles")):
        raise WorkflowError("Dedicated subscription profile must not contain API alternatives")
    projected = {k: token[k] for k in ("accessToken", "expiresAt", "scopes", "subscriptionType")}
    return {"claudeAiOauth": projected}, {
        "oauthAccount": {k: account[k] for k in ("accountUuid", "organizationUuid")}
    }


def environment(home, config):
    return {
        "HOME": str(home),
        "CLAUDE_CONFIG_DIR": str(config),
        "PATH": "/usr/bin:/bin",
        "LANG": "C.UTF-8",
        "DISABLE_AUTOUPDATER": "1",
        "DISABLE_TELEMETRY": "1",
        "DISABLE_ERROR_REPORTING": "1",
        "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
        "CLAUDE_CODE_NO_MODEL_FALLBACK": "1",
        "CLAUDE_CODE_DISABLE_REFUSAL_FALLBACK": "1",
    }


@contextlib.contextmanager
def snapshot(seconds):
    source = profile()
    credentials, config = validate(
        private_json(source / ".credentials.json"),
        private_json(source / ".claude.json"),
        private_json(STORE / "receipt.json"),
        seconds,
    )
    with tempfile.TemporaryDirectory(prefix="flowdc-review-auth-") as temporary:
        home = Path(temporary)
        directory = home / "config"
        directory.mkdir(mode=0o700)
        write_json(directory / ".credentials.json", credentials)
        write_json(directory / ".claude.json", config)
        yield environment(home, directory)


def login(*, paid_usage_disabled):
    if not paid_usage_disabled or not sys.stdin.isatty():
        raise WorkflowError("Use a private terminal after confirming paid extra usage is disabled")
    binary = shutil.which("claude")
    if not binary:
        raise WorkflowError("Install Claude Code before login")
    directory = STORE / "profile"
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    home = STORE / "home"
    home.mkdir(exist_ok=True, mode=0o700)
    subprocess.run(
        [binary, "--safe-mode", "--restricted", "auth", "login", "--claudeai"],
        cwd=home,
        env=environment(home, directory),
        check=True,
    )
    receipt = {"paid_usage_disabled": True, "recorded_at": time.time()}
    validate(
        private_json(directory / ".credentials.json"), private_json(directory / ".claude.json"), receipt, 60
    )
    write_json(STORE / "receipt.json", receipt)
    write_json(STORE / "current-profile.json", {"path": str(directory)})
    return {"status": "ready", "dedicated_max_login": True, "paid_extra_disabled": True}
