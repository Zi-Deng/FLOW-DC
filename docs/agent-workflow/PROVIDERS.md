# Current reviewer providers

The single current runtime supports `claude-code` and `copilot`. Select per call with `--review-provider`, `--review-model`, `--review-effort`, or save defaults with `workflow.py review-selection --save`. Switching provider selects that provider's default unless explicitly overridden. There is no automatic provider/model fallback. Availability is established by the selected native CLI response, not inferred from a subscription price.

Claude defaults to `claude-opus-5-5`/medium. Require Claude Code>=2.1.259 for the used restricted/nonprompting flags; record the installed version rather than cloning adapter versions. A dedicated Max login is reused. The existing completed dedicated registration is discovered once as a path; the obsolete authorization/runtime generation stack is not imported.

In a private terminal, after checking paid extra usage is disabled:

```bash
python3 scripts/agentic/workflow.py claude-login-setup --paid-usage-disabled
```

Login uses normal native callbacks in the dedicated profile and validates Max/disabled-extra fields. Review copies only the access token and necessary account identity to an ephemeral profile, with no refresh token or API key. It checks remaining lifetime for that one bounded call. Ordinary login and credentials remain untouched. Renewal aliases remain accepted for convenience, not as compatibility with old review schemas.

Review uses safe mode, restricted mode, explicit Read/Grep/Glob, no permission prompts, no inherited project/user settings, hooks, MCP, persistence or delegation. Endpoint-managed customization is refused pending inspection. Native restrictions are vendor controls, not an independently proved OS sandbox. Keep paid extra usage disabled in the Anthropic account; the local observation and estimated cost cap cannot attest a bill or prevent an account owner changing the setting concurrently.

Copilot defaults to `claude-opus-5`/default, with explicit alternatives supported by the installed CLI. It receives a fresh configuration home, read-only available tools, no built-in MCP/custom instructions, no automatic updates, no shell/writes/URLs, a400-credit limit and900-second wrapper. Masked/unsupported material limits review confidence; selecting Copilot does not erase its earlier masking history.

Research checked October9,2026 against [Anthropic CLI reference](https://code.claude.com/docs/en/cli-reference), [permissions](https://code.claude.com/docs/en/permissions), and [GitHub CLI reference](https://docs.github.com/en/copilot/reference/copilot-cli-reference/cli-command-reference). `--tools` restricts available Claude built-ins; `--allowedTools` alone is not that restriction. Anthropic's cost cap is an estimate and may be exceeded before stopping. Hosted subscription/OAuth support does not require moving this dedicated local login into GitHub secrets.

The local review path is the supported default. CI runs software checks without model/cloud secrets. No model Actions workflow is enabled by this simplification.
