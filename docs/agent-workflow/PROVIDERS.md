# Reviewer providers and activation

Configuration schema 2 selects `claude-code`, exact model `claude-opus-5-5`, effort
`medium`. Schema 1 retains Copilot selection. Copilot remains an explicit supported
choice: `copilot`, `claude-opus-5`, effort `default` (no effort flag is sent to that CLI).
Exact supported alternatives and their effort lists appear in `review-selection` status.
The compatibility table includes Claude Opus 5.5, Opus 5, Sonnet 5, Opus 4.7 and
Sonnet 4.6; Copilot also supports explicitly listed GPT models. Provider spellings
differ (`claude-opus-5-5` versus `claude-opus-5.5`); they are never translated silently.
Claude Sonnet 4.6 has no xhigh entry. Copilot `default` leaves its effort flag unset;
other validated efforts are passed explicitly. Model availability and included billing
remain account-specific activation requirements, not promises made by this table.
See the [native effort table](https://code.claude.com/docs/en/model-config#adjust-effort-level)
and [Copilot model reference](https://docs.github.com/en/copilot/reference/copilot-cli-reference/cli-command-reference#supported-models). Aliases,
unregistered models/providers, and incompatible effort combinations fail before inference.

**Migration activation is incomplete.** Claude execution currently fails closed on
unverified token-only remote managed policy. No successful native diagnostic is
claimed. Neither a workstation login, binary inspection nor synthetic tests establish
isolated tool capability or included-usage billing. The diagnostic entrypoint also
refuses inference until this isolation blocker is resolved; it has no force option.

## Inspect or deliberately select

From the trusted control checkout:

```bash
python3 scripts/agentic/workflow.py review-selection
python3 scripts/agentic/workflow.py review-selection --review-provider copilot
python3 scripts/agentic/workflow.py review-selection --review-provider copilot --save
python3 scripts/agentic/review.py prepare 456 --issue 123 --plan-comment 987654321 \
  --review-provider claude-code --review-model claude-opus-5-5 --review-effort medium
python3 scripts/agentic/workflow.py task-review 123 --review-provider copilot
```

Per-call fields override the private saved selection, which overrides trusted defaults.
Selecting a provider explicitly resets model and effort to that provider's defaults
before applying explicit model/effort fields. The saved selection is an owner-only
`.agentic-local/review-selection.json` in the control checkout, not a credential.
Status prints effective provider/model/effort, per-field provenance, CLI pin, adapter,
billing mode, typed budgets and activation blockers. Preparation is not inference.

### Adding an exact model without changing adapter code

Schema-2 trusted `.agentic/config.json` can add `review_model_extensions` declarations.
This is a deliberate configuration change, not automatic provider discovery or a
fallback. Before adding one, the maintainer must verify primary provider documentation
for the exact model/efforts and compatibility with the **pinned** CLI and current
tool/telemetry adapter. An identifier that merely looks versioned is not evidence of
support. Unsupported combinations stay refused; changing a CLI or telemetry protocol
still requires an implementation change and its validation.

Each declaration has exactly these fields:

| Field | Required value |
| --- | --- |
| `provider` | `claude-code` or `copilot` |
| `model` | Exact lowercase versioned provider identifier, at most 128 characters; no aliases, auto/default/latest segments, paths or context modifiers |
| `efforts` | Nonempty unique list of the model's verified efforts, within the pinned CLI's supported controls; Copilot `default` omits its effort flag |
| `cli_version` | `2.1.282` for Claude or `1.0.83` for Copilot |
| `adapter` | `claude-stream-json-2.1.282-v1` or `copilot-session-events-v2`, matching the provider |
| `evidence` | One to eight public HTTPS primary documentation URLs, without query strings or credentials; Claude documentation hosts for Claude, `docs.github.com` for Copilot |

The list defaults to empty. Declarations cannot override built-in model entries or
duplicate each other. They do not change provider-local defaults. Select a declared
model through the same `--review-model` interface; explicitly supply an effort if
the provider's default is not supported by that model. Saved selections do not grant
compatibility: new preparations require the declaration to remain in trusted config.
Status includes the merged catalog, `model_compatibility_sources` distinguishing
`built-in` from `trusted-config-declaration`, and the selected extension's
`model_compatibility` record. Every configured entry is validated before new selection,
even when another model is selected.

The complete selected declaration, including evidence references, is frozen into
the execution policy and its packet/round digest. Changing it requires fresh
preparation; recovery of an attempted packet uses the bound declaration even after
today's configuration or saved selection changes. Existing built-in policies retain
their original fields and hashes. The wrapper validates declaration structure and
pin/adapter/effort consistency; **it does not fetch URLs or verify the maintainer's
compatibility claim**. This has the same trust boundary as other trusted repository
policy, not a provider-signed capability attestation. A declaration never establishes
account entitlement, included billing, live tool capability, or permission for an
additional inference. Native flag/settings checks, exact observed identity, actual
tool canaries, usage/isolation gates and continuation authorization still apply.

Schema-5 packets freeze the resolved policy and its provenance with contract artifacts,
head/base and packet hashes. Results/captures bind that policy and preserve the exact
terminal report text. A changed provider, model, effort or budget requires explicit
fresh preparation; an attempted round still requires the existing continuation
permission. Recovery never runs inference, uses the packet's bound policy and does
not adopt current defaults. No reviewer resume, retry or within-packet fallback exists.

## Verify and register binaries

Only Linux x64 glibc is currently supported. Claude's native binary is pinned to
2.1.282, Copilot's archive to 1.0.83. Installation is an explicit operation; review
never upgrades a CLI. The installer stages and verifies artifacts before writing the
requested destination. Existing destination files are refused.

```bash
python3 scripts/agentic/install_tool.py claude-code --directory /absolute/staging/claude-2.1.282
python3 scripts/agentic/workflow.py register-reviewer claude-code \
  --binary /absolute/staging/claude-2.1.282/claude \
  --proof-directory /absolute/staging/claude-2.1.282
python3 scripts/agentic/install_tool.py copilot --directory /absolute/staging/copilot-1.0.83
python3 scripts/agentic/workflow.py register-reviewer copilot \
  --binary /absolute/staging/copilot-1.0.83/copilot \
  --proof-directory /absolute/staging/copilot-1.0.83
```

Registration copies verified files into a versioned private bundle under
`.agentic-local/provider-clis`. Execution uses that absolute binary and revalidates
its proof. Claude verification pins both manifest and binary SHA-256, checks version
and platform, and verifies the detached GPG signature in a temporary keyring against
fingerprint `31DDDE24DDFAB679F42D7BD2BAA929FF1A7ECACE`. Copilot verification retains
its existing archive digest and compares the executable with the archive member.
Unknown identity, unsupported platform, unsafe paths or verification failure refuse
execution. No global keyring, ordinary CLI login or template-origin record is changed.
The [official signing procedure](https://code.claude.com/docs/en/setup#verify-the-manifest-signature)
documents the release trust chain.

## Dedicated subscription token and billing receipt

The operator manually runs `claude setup-token` in their ordinary terminal and keeps
the resulting dedicated subscription token out of chat, shell arguments and Git.
After disabling paid usage credits/extra usage for the subscription, use the helper:

```bash
python3 scripts/agentic/workflow.py claude-subscription-setup --paid-usage-disabled
```

It accepts hidden terminal input and defaults to
`~/.config/flowdc-agentic/claude-review-token`. The dedicated directory must be 0700;
its token and adjacent receipt must be single-link, owner-owned regular files with
mode 0600, outside Git checkouts and without symlink components. `--replace` is an
explicit replacement; unsafe existing objects remain refused. Ordinary Claude login
and configuration are untouched. API keys and alternative provider credentials are
never sourced by this adapter.

The receipt is a seven-day operator assertion that paid usage is disabled, bound to
the dedicated token. It is not a cryptographic billing guarantee. Renew the assertion
with hidden setup/explicit replacement after expiry or account/billing changes; a
changed token invalidates the old receipt. Missing, expired, unsafe or inconsistent
records block preflight. Credential material and its receipt remain outside artifacts.

## Isolation and the unresolved native boundary

Each call is designed to use fresh HOME, Claude config, XDG state and an inert packet
workspace. Only fixed wrapper environment values and the dedicated OAuth token are
passed. The command requests safe/restricted mode, native Read/Grep/Glob only,
`dontAsk`, `--permission-prompts none`, empty settings discovery, explicit trusted
settings, empty strict MCP, disabled skills/slash commands and no session persistence.
The closed settings subset disables hooks, model switching, fallback models,
auto-memory and automatic continuation at usage limits. Native environment controls
also disable retries and fallback. No shell, edits, delegation or network-fetch tool
is authorized. Workspace hashes are checked afterwards. This is CLI containment,
not an OS sandbox against a compromised signed executable.

Static inspection on 2026-10-01 of the pinned binary matched its required option and
closed-settings declarations. Its native Read renderer uses a line number followed
by a tab or colon; Grep content is matched against exact immutable source lines.
These observations support conservative fixtures, not live coverage.

The same inspection identified the unresolved pre-inference boundary: native
`auth status` reports environment tokens as `oauth_token` and does not include their
subscription identity; the remote managed-settings eligibility branch includes an
unknown subscription identity. Empty local HOME/config and a billing receipt do not
prove remote policy is absent or harmless. Local managed policy paths are refused,
and token-only remote policy is currently refused too. A post-call check cannot
prevent hooks executing before the first response. Do not replace this blocker with
an empty remote-policy override, a copied ordinary login, a guessed account class,
a fabricated diagnostic receipt or broader permissions. Resolving it requires
verified native control/effective-policy evidence within the approved contract.
A proposed `claude doctor` probe does not resolve this boundary: the pinned native
shared preAction initializes managed controls before dispatch, and the doctor handler
samples a potentially asynchronous fetch outcome. No authenticated doctor call was
made. A different authentication method requires a superseding approved contract;
this implementation does not copy the ordinary login or fabricate subscription metadata.
The [server-managed settings guide](https://code.claude.com/docs/en/server-managed-settings#verify-settings-delivery)
documents fetch status reporting, which is not a before-effect isolation barrier.
The [CLI reference](https://code.claude.com/docs/en/cli-reference) documents safe and
restricted mode; it does not itself prove effective runtime isolation.

## Budgets, diagnostics and failures

Claude's unit policy is 900 seconds and a $10 provider-estimated reference-cost
ceiling, with **zero extra actual spending authorized**. The estimate is not a
subscription bill, paid allowance or Copilot-credit conversion. The CLI receives its
native estimate ceiling; wrapper timeout/output caps remain enforced. In-flight work
can overshoot a reported estimate. Unsupported usage, quota rejection, unknown native
shapes, isolation uncertainty or failed tools produce incomplete evidence. Copilot's
policy remains 400 AI credits and 900 seconds. Unknown usage grants no retry authority.

After all prerequisites, at most two issue-33 diagnostics are authorized, each 300
seconds and $2 estimated reference cost. They contain only a tiny canary packet and
are not PR reviews. The private ledger records attempts before inference; interruption
still counts. No automatic retry or reset exists.

```bash
python3 scripts/agentic/workflow.py diagnose-claude --review-provider claude-code
```

This command is presently blocked by the isolation finding above. When that finding
is resolved, normal activation additionally requires a matching successful diagnostic
with exact native model/session identity, tools, terminal text, numerical accounting
and unchanged packet/report evidence. Attempt 1 inspects actual Read/Grep/Glob results
and harmless ordinary authentication source. Attempt 2 additionally requires a
correlated native permission refusal for an existing wrapper-owned file outside the
restricted workspace. A missing-file error or assistant assertion is insufficient.
The two purposes are distinct and neither is retried. Both must pass for CLI/adapter
activation; every subsequent review still verifies its own exact model and actual
tool canaries. A diagnostic refusal can never qualify ordinary PR coverage. Synthetic
fixtures cannot create live evidence.

Native stream parsing requires correlated successful model-facing Read/Grep/Glob
results and one successful terminal result. UI metadata, assistant fragments,
structured-output channels, listings and empty/failed results do not establish source
inspection. Unknown/malformed renderings retain bounded fixed reasons/digests only.
Accounting keeps terminal cost/token totals and per-model numerical usage. Assistant
message IDs deduplicate observed steps and input/cache counters; raw IDs are discarded.
Missing step counters remain unknown, and native `num_turns` is not exact API-call
accounting. Output tokens come from terminal totals, not intermediate placeholders.
Pinned built-in agent declarations alone do not imply delegation; the Agent tool,
actual child messages and unknown agent/customization declarations remain refused.
The terminal text is saved exactly even when partial; no substring extraction,
concatenation, report synthesis or inference repair is performed. Partial findings
remain publishable as incomplete, while all ordinary readiness gates remain strict.

## Historical records, hosted operation and this migration

Schema 1 publication retains its original bytes/envelope. Schema 2 uses the frozen
`review_coverage_v1.py`; schema 3 uses `review_coverage_v2.py` and
`review_telemetry_v2.py`. Historical assessment hashes/envelopes do not change and
cannot qualify a current review. Schema 4 is reserved for PR #32 and explicitly
unsupported here. Current packets/results/captures use schema 5; model reports remain
schema 2. Dated research, adoption evidence and template provenance remain historical.

The hosted opt-in workflow explicitly selects and registers Copilot. It remains
disabled unless separately enabled, and receives no Claude subscription token.
The installer/export payload includes both providers and frozen compatibility code;
private state, credentials and sessions are excluded.

The user waived independent model PR review for issue #33 / PR #34 only. No generic
skip flag, forged review designation or weakened finish gate is introduced. This
incomplete migration is not merge-ready. When implementation, activation evidence and
required CI are complete, the maintainer must manually verify the exact final head,
`flowdc-tests` and `agentic-quality`, their actual checkout receipts separately from
head association, acceptance evidence and unresolved threads before a human-only
merge with `--match-head-commit`. Normal finish tooling still requires a genuine
current review and may remain inapplicable to this one exception.

PR #32 is untouched. After human merge, resume its original recorded Astra UUID,
reconcile batch budgets/provider bindings/record versions and run its normal review.
Rollback is deliberate Copilot selection under its own authority or a normal revert,
never automatic provider fallback. Observed reads do not prove understanding, and
owner-writable private records are not tamper-proof attestations.
