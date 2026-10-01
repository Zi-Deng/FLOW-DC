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
unverified native-login callback policy isolation. No successful native diagnostic is
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
not adopt current defaults. No reviewer resume, new wrapper inference retry or within-packet fallback exists.

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

## Dedicated native Max login and billing receipt

The approved [native-login revision](https://github.com/Zi-Deng/FLOW-DC/issues/33#issuecomment-5938298025)
replaces injected setup tokens. The ordinary Claude login/configuration must never
be read, copied or modified. The old `claude-subscription-setup` command refuses;
existing token/receipt files remain protected and unused, without migration or logout.

The guarded terminal interface is:

```bash
python3 -B scripts/agentic/workflow.py claude-login-setup --paid-usage-disabled
python3 -B scripts/agentic/workflow.py claude-login-setup --renew --paid-usage-disabled
```

**These commands currently stop before authentication.** Native setup callback
isolation remains unverified. There is no force option or manual credential-import
route. Do not run the native login separately to bypass the helper's blocker.

The implementation stages each explicit browser login under
`~/.config/flowdc-agentic/claude-review-login/generations/<opaque-generation>/`, with
separate private `home`, `config` and inert `workspace` directories. Each generation
is retained; renewal does not overwrite or revoke older credentials. Parent paths
are opened without following symlinks; Git ancestry, ownership, modes, file types,
link counts, bounded strict JSON and an exclusive nonblocking registration lock are
checked. Directories are 0700 and files 0600. Partial/failed setup cannot activate.
OAuth URLs/codes stay in the operator terminal, never saved process output or chat.
The actual native browser flow, its successful exit and genuine native credential
and account records are necessary; a manually written Max field is not setup.

The separate receipt asserts that paid usage credits/extra usage are disabled for
that registration, generation and account. It expires within seven days and must
be revalidated after account, credential or billing changes. It is owner-writable
accounting, not a cryptographic billing guarantee. Account identifiers and private
integrity digests remain in the guarded store. Only random registration/generation
IDs enter packet policies/status. Missing, inconsistent or unsafe records block use.

Before a call, the wrapper projects only native accessToken, expiresAt, scopes and
genuine subscriptionType, plus minimal native account/org identity, into fresh
HOME/config/XDG state. The refreshToken field is absent, not null or an empty-string
sentinel. Persistent settings, history and caches are not copied. No ephemeral native
changes are committed back. The snapshot is removed on return, exceptions and
interruptions, while its registration lock remains held through invocation.

Remaining access-token life must strictly exceed the full timeout +300 seconds of
native refresh margin +60 seconds clock allowance: >1260 seconds for a default
review, >660 seconds for a diagnostic. The check runs again immediately before
launch, with a wall/monotonic clock comparison. Insufficient life requires explicit
manual renewal; the wrapper does not refresh or relaunch. A changed generation
requires fresh preparation. Attempted recovery/publication does not authenticate.
Capability evidence is not silently carried across renewal. An explicit renewal
with `--retain-capability` may retain prior observations only after verifying the
same private account/registration lineage, unchanged auth mode/CLI/adapter and a
current receipt. Status retains the originally observed generations and distinguishes
the current preflight-validated generation from one live-diagnostic-tested. Without
that flag, a new generation cannot reuse prior capability evidence. No renewal resets
diagnostic allowance or relabels the generation actually observed by a diagnostic.

## Isolation and the unresolved native boundary

Each invocation uses a fresh inert workspace and a fixed minimal environment.
No injected token/FD, API/profile/cloud/endpoint override or ordinary login is
inherited. Native `dF`/`gF` select the supplied actual claudeAiOauth access record;
`ult` reads its actual subscriptionType. For genuine Max without competing auth,
`MR` returns unsupported_subscription and the remote fetch returns without delivery.
This is conditional native eligibility, not administrator-policy suppression.
Endpoint-managed paths are checked separately with lstat; unreadable or present
policy is refused, not ignored. `auth status` display fallback cannot prove this chain.

The command requests safe/restricted mode, Read/Grep/Glob only, dontAsk, no permission
prompts, empty settings discovery, explicit trusted settings, empty strict MCP,
no slash commands and no persistence. Settings disable hooks, model switching,
fallback models, auto-memory and usage-limit continuation. No shell, edits,
delegation or fetch tool is authorized. Workspace hashes are checked afterwards.
This is CLI containment, not an OS sandbox against a compromised signed executable.

On 2026-10-01, static inspection of signed native 2.1.282 confirmed these reader
branches and the no-refresh return. The native OAuth flow resolves actual
Max profile metadata before persistence; cache reset/helper arming alone does not
prove a managed-policy effect. The unresolved boundary is an unintended non-Max
selection: native login performs subsequent authenticated operations before the
wrapper can reject it. No verified pre-return Max-only filter is available. Setup
therefore remains blocked pending the coordinator's precise setup-boundary decision
and contract reconciliation; ordinary login is not offered as a workaround.
[The pinned-source audit](NATIVE-AUTH-AUDIT.md) records offsets, conditions and omissions.
No authenticated doctor/status/login command or inference was used for this audit.
A doctor probe is not a before-effect barrier: initialization precedes its handler.

The [authentication guide](https://code.claude.com/docs/en/authentication) documents
separate configuration directories. The minimal access-only snapshot is pinned
implementation compatibility, not a portable credential-export API. The
[server-managed settings guide](https://code.claude.com/docs/en/server-managed-settings)
and [CLI reference](https://code.claude.com/docs/en/cli-reference) do not certify this
wrapper's runtime isolation. Synthetic tests and source inspection are not live proof.

## Budgets, diagnostics and failures

Claude's unit policy is 900 seconds and a $10 provider-estimated reference-cost
ceiling, with **zero extra actual spending authorized**. The estimate is not a
subscription bill, paid allowance or Copilot-credit conversion. The CLI receives its
native estimate ceiling; wrapper timeout/output caps remain enforced. In-flight work
can overshoot a reported estimate. Unsupported usage, quota rejection, unknown native
shapes, isolation uncertainty or failed tools produce incomplete evidence. Copilot's
policy remains 400 AI credits and 900 seconds. Unknown usage grants no retry authority.

Pinned native 2.1.282 may repeat an authentication-rejected request once with the
same access credential after the first 401 inside one captured invocation. Absence
of refresh material prevents refresh-token exchange. This disclosed boundary is
not permission for a new wrapper call, successful-output replay, provider/auth/model
substitution or another diagnostic slot. Do not promise first-401 immediate termination
or exact API-call counts. No undocumented auth-retry-disable variable is used;
`CLAUDE_CODE_MAX_RETRIES` was removed. Complete native retry/fallback qualification
and controlled native error-path evidence remain activation requirements.

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
schema 2. Schema-5 token-mode records without an authentication payload retain
exact historical hashes, assessment and publication envelopes, but cannot execute
or establish native readiness. New schema-5 Claude policies bind the versioned
native auth mode and random registration/generation identifiers. Dated research, adoption evidence and template provenance remain historical.

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
