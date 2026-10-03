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

**Migration activation is incomplete.** Human-terminal setup is available under the
approved interactive-login boundary. Reviewer calls still require actual dedicated
Max credentials, a current billing receipt, verified reviewer controls and both
successful native diagnostics. Neither a workstation login, binary inspection nor
synthetic tests establish isolated tool capability or included-usage billing.

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
| `adapter` | `claude-stream-json-2.1.282-v6` or `copilot-session-events-v2`, matching the provider |
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

The approved [interactive-setup revision](https://github.com/Zi-Deng/FLOW-DC/issues/33#issuecomment-5959064895)
replaces injected setup tokens. The ordinary Claude login/configuration must never
be read, copied or modified. The old `claude-subscription-setup` command refuses;
existing token/receipt files remain protected and unused, without migration or logout.

The guarded terminal interface is:

```bash
python3 -B scripts/agentic/workflow.py claude-login-setup --paid-usage-disabled
python3 -B scripts/agentic/workflow.py claude-login-setup --renew --paid-usage-disabled
```

Run setup yourself in a private real terminal; agents must not run browser login
in a tool terminal or capture its output. Setup uses the verified absolute native
binary with `--safe-mode --restricted --setting-sources '' auth login --claudeai`.
The flags exclude ordinary customization/settings discovery; they do not suppress
normal vendor login-time callbacks or enforce a pre-return Max-only filter. Those
callbacks are explicitly accepted for human setup and renewal only. Select your
personal Max account. After native return, the helper checks genuine Max metadata,
account, full lifetime, billing assertion and endpoint controls before registration.
Non-Max, failed or interrupted setup remains inactive and private, without retry,
revocation or automatic cleanup. Reviewer tool restrictions and remote-policy
ineligibility are separate requirements, unchanged by this setup boundary.

Until this PR merges, the control checkout does not contain the new helper. After
checking the exact committed candidate source and its validation, the coordinator
provides its absolute `scripts/agentic/workflow.py` path for the human to run from
the clean control checkout. This uses the control's verified CLI registration and
the candidate's implementation without changing main or importing ordinary auth.
The same candidate command with `--help` is safe to inspect without authentication.

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

## Reviewer isolation and the separate setup boundary

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
no slash commands and no persistence. Settings explicitly require
`disableBundledSkills: true` as well as disabling hooks, model switching, fallback
models, auto-memory and usage-limit continuation. The effective trusted settings also
set exactly nine `enabledPlugins` entries to boolean `false`: `sec-default@builtin`,
`agents-md@builtin`, `telemetry@builtin`, `plugin-authoring@builtin`, `tips@builtin`,
`mermaid@builtin`, `responsive-mode@builtin`, `diff@builtin`, `claude-test@builtin`.
Missing, extra, extended or nonboolean entries are refused. The pinned registrar's
exact names and bytes are checked. Safe mode and hook-disable settings alone retain
built-in hook modules; the enabled selector must exclude them before skills, settings,
metadata and hook/module loading. Flag settings remain effective with empty settings
discovery, but administrator policy takes precedence. `sec-default` is policy-only:
its flag entry cannot override policy. Existing endpoint-policy refusal and genuine
Max remote ineligibility remain mandatory; Team/managed paths cannot qualify.
No plugin, including `agents-md`, is allowed in initialization metadata. These are
conditional source controls, not live absence evidence. The bundled-skills setting alone
does not prove an empty catalog: native exceptions exist. Required empty initial
skills/plugins/MCP/slash-command metadata and empty command updates remain separate
validation requirements; unexpected metadata blocks qualification. No shell, edits,
delegation or fetch tool is authorized. Workspace hashes are checked afterwards.
This is CLI containment, not an OS sandbox against a compromised signed executable.

On 2026-10-01, static inspection of signed native 2.1.282 confirmed these reader
branches and the no-refresh return. The native OAuth flow resolves actual
Max profile metadata before persistence; cache reset/helper arming alone does not
prove a managed-policy effect. On 2026-10-02, the maintainer accepted standard
human-controlled dedicated login callbacks, including callbacks before rejection
of a non-Max selection. Post-return rejection cannot undo those native effects.
No pre-return Max-only filter is claimed. This acceptance does not allow ordinary
login import, reviewer policy bypass, API fallback or agent-run authentication.
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

Issue 33's five historical trials retain their original statuses: incomplete,
incomplete, incomplete, qualified under v4, incomplete. All four ledgers and old
grants are frozen history; the revision-6 grant stopped after slot 5. Its executable
allowance is zero. The [revision-8 sequence below](#issue-33-revision-8-prospective-recovery)
requires current final checks/CI and explicit coordinator preview/application.
Publication, software changes and renewal never run or authorize an automatic call.

Both distinct purposes must succeed under the current exact CLI/adapter/model/effort
and compatible authentication provenance. The tools-and-source purpose inspects
actual Read/Grep/Glob results and harmless ordinary authentication source. The
isolation purpose additionally requires a correlated native permission refusal for
an existing wrapper-owned file outside the restricted workspace. A missing-file
error or assistant assertion is insufficient. Every subsequent review still verifies
its own model and tool canaries. A diagnostic refusal cannot qualify ordinary PR
coverage; synthetic fixtures cannot create live capability evidence.

### Issue 33 revision-4 history and stopped execution

The recorded approval binds [plan comment 5963470903](https://github.com/Zi-Deng/FLOW-DC/issues/33#issuecomment-5963470903)
and contract `dbbe4f2dccf83acd2a5fab4b6b031c96673516f4e63820df1036a198fd1c81c1`.
The historical “PROPOSED” wording in that published draft does not replace the actual
operator approval record. Publication alone never establishes that approval.
The coordinator explicitly previewed/applied the grant and invoked slot 2 once on
`495dd3de7f6e7404441b033409c831f3190d17f2`, after deterministic checks and required CI.

That grant authorized exactly slot 2 for replacement native-tools-and-source and
slot 3 for isolation-refusal only after slot 2 qualified. Total ceilings were
900 seconds/$6 reference estimate including incomplete slot 1. Slot 2 returned a
valid complete JSON report, matching model/session, actual Read/Grep/Glob and both
inventory entries, but remained incomplete for customization and an unsupported
system event. Its $0.0621016 reference estimate is not a subscription bill. Under
the approved failure-stop rule, slot 3 cannot run. No fourth attempt, successful-output
replay, report repair, automatic retry or fallback is authorized.

The schema-2 `recovery-ledger.json` and its version-1 grant remain bound to the
original v2 policy, approval, original ledger text and attempt-1 hashes. Both trials'
exact packet/report/capture/assessment bytes remain unchanged and incomplete. The
current code reads the stopped grant using frozen v2 policy semantics, without
credentials or inference. `claude-diagnostic-recovery` cannot reuse this old grant for v3. Frozen reads select
the exact matching revision-4 approval snapshot from current approval or its history,
refusing missing, edited or ambiguous history. Frozen loads compare stored registration
and generation bindings without reading current credentials; that comparison cannot
authorize renewal lineage. Invocation and activation separately verify current lineage.
New execution requires current authority.

### Issue 33 revision-5 prospective recovery

**Historical, stopped after incomplete trial 3 on 2026-10-03 UTC.** The sequence
below records that grant's original meaning; its unused isolation slot 4 cannot run.
Use the revision-8 procedure below for the current binding.

The maintainer's recorded standing override and approval bind
[plan comment 5964179523](https://github.com/Zi-Deng/FLOW-DC/issues/33#issuecomment-5964179523)
and contract `5c8c8c8bc87c2cd02229ea1c0f74b98a9748542fa770fc93178234d73501c45f`.
Its explicit authority update supersedes the preserved proposal-state wording. No
repeat approval of this same continuation is pending. Standing authorization does
not turn an incomplete trial into success or automatically extend this finite grant.

After final-source checks and required CI, the coordinator previews/applies from the
clean control checkout. Before merge, invoke `workflow.py` through the absolute
issue-33 worktree path while retaining that control working directory:

```bash
python3 -B /absolute/issue-33-worktree/scripts/agentic/workflow.py claude-diagnostic-recovery
python3 -B /absolute/issue-33-worktree/scripts/agentic/workflow.py claude-diagnostic-recovery \
  --apply --preview-digest EXACT_DIGEST_FROM_PREVIEW
```

This locally validates current approval and the native authentication binding; it makes
no inference. Full invocation prerequisites are checked again before each call.
Application requires an unchanged preview digest. A separate schema-3
`recovery-v5-ledger.json` contains a version-2 grant bound to current exact approval,
contract, v3 CLI/model/effort/auth/budget policy, both historical ledger texts and all
trial-1/2 hashes. Both original ledgers/grants and failed trials stay byte-identical;
no old purpose, count, status or report is rewritten. Partial/conflicting state and
missing/tampered evidence are refused. Historical approval can validate a frozen
read but never a fresh application, invocation or activation.

The new grant explicitly assigns counted slot 3 to native-tools-and-source and
counted slot 4 to isolation-refusal **only if slot 3 qualifies**. The old unused
slot-3 purpose remains in its stopped historical grant. Each new packet binds the
new grant digest, counted slot and purpose. Total ceilings are four counted attempts,
1200 seconds/$8 reference estimate including both old failures; prospective ceilings
are 600 seconds/$4. Zero extra actual spending remains authorized. No fifth attempt,
automatic call, report repair/replay, reset, provider/model/auth fallback or widened
permission follows from this grant. Any future failure/interruption stops it;
idempotent application does not reset an attempted slot.

The coordinator invokes the diagnostic command once for slot 3, inspects its exact
qualified evidence, then explicitly invokes it once for slot 4 only after success:

```bash
python3 -B /absolute/issue-33-worktree/scripts/agentic/workflow.py diagnose-claude --review-provider claude-code
```

Before each call recheck the durable ledger, guarded Max registration, current
account-bound disabled-paid-usage receipt, token lifetime and endpoint/remote
isolation. Renewal remains human-only and never creates allowance. Both successful
purposes must match current policy; observed generations remain unchanged with only
explicit verified same-account lineage permitted. Owner-writable receipts are
accounting, not cryptographic attestations or billing guarantees. The implementation
and synthetic ledger tests do not establish either successful live purpose.

### Issue 33 revision-6 recovery — historical, stopped after slot 5

The following records the original finite authorization. Its tools trial qualified
and isolation trial failed; it grants no further executable allowance. Use revision 8
for current preparation and execution after its required gates.

The standing authorization is exactly bound to
[plan comment 5964751600](https://github.com/Zi-Deng/FLOW-DC/issues/33#issuecomment-5964751600)
and contract `993afb1688760fb95a966b8526a61bfcc50d5d518de974b9698aebda9f20062e`.
No repeat user approval is pending. After reviewed implementation, final local checks
and required CI, the coordinator uses the same `claude-diagnostic-recovery` preview
and `--apply --preview-digest` commands above from the clean control checkout with
the exact issue-worktree script. This never invokes inference. A separate schema-5
`recovery-v6-ledger.json` holds a version-3 grant; schema 4 remains reserved.

The grant binds exact current authority, v4 policy, all three historical ledger texts,
all three failed-trial evidence hashes and contiguous identities. Historical reads
use original approval receipts and frozen v1/v2/v3 semantics without credential reads.
History has bounded file counts and byte sizes. Partial/conflicting state, edited
limits/purposes/policies and stale authority fail before new authentication/inference.
Original ledgers, grants, observed generations, reports and assessments remain exact;
old unused purposes are never rewritten. Owner-writable records are not attestations.

Prospective counted slot 4 is fresh v4 native-tools-and-source. Slot 5 is
isolation-refusal only after slot 4 qualifies. Each new packet binds the grant digest,
counted number and purpose. Five total attempts include failures 1/2/3; total ceilings
are 1500 seconds/$10 reference estimate, prospective ceilings 600 seconds/$4, each
call 300 seconds/$2 and zero extra actual spending. Any failure/interruption stops
this grant. There is no sixth call, automatic retry, reset, substitution, permission
widening or report repair/replay. Idempotent application creates no new allowance.

The coordinator invokes `diagnose-claude --review-provider claude-code` separately
for each purpose only after current native setup/receipt/lifetime/endpoint/remote
prerequisites pass. Both purposes must qualify under the current exact policy;
verified same-account renewal retains original observed generations and never grants
calls. All three existing trials remain incomplete. Offline fixtures and this ledger
implementation do not establish either required live success.

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

The current v6 Claude stream adapter uses diagnostic schema 8 (schema 4 remains reserved). Its Read parser recognizes
the pinned renderer's numbered final empty segment after a trailing newline, without
crediting that segment as source. LF/CRLF and tab-aware separators are supported;
ambiguous native/inventory line numbering and extra reminder/truncation text remain
incomplete. A precisely shaped `system/status/requesting` event is accepted; compact,
null/unknown status, hook, retry and fallback events remain unsupported. Subtypes,
unknown agent names and rejected system payloads are retained only as bounded hash
counts. V5 retains v3/v4 acceptance of only an exact `commands_changed` envelope with an
empty commands array, matching session, valid UUID and correct event order. Nonempty
built-in/custom catalogs, extra fields and missing/null catalogs remain incomplete.
Required initialization fields must be present; optional terminal commands and
plugin/MCP error fields must be empty if present. Fixed per-field reasons and bounded
presence/type/length observations with hashed names/values make failures inspectable
without retaining raw provider strings. `claude-code-guide` is a known built-in declaration, never permission to use
Agent or delegated tools. See [the pinned source audit](NATIVE-TELEMETRY-AUDIT.md).

V4 also accepts only an exact six-field `thinking_tokens` system envelope:
`type`, `subtype`, `estimated_tokens`, `estimated_tokens_delta`, `session_id`, `uuid`.
It must occur after exactly one initialization and before the terminal result, with
matching session and valid UUID. Both estimates must be finite JSON numbers, never
booleans, in [0, 9007199254740991], with delta no greater than total. Zero, fractional
values and block/request resets are supported; global monotonicity is not required.
The upper bound is conservative wrapper validation, not a native guarantee. Missing,
extra, delegated and malformed fields remain incomplete. Fixed numeric-validity and
presence/type observations use the existing eight-shape/64-hash caps and bounded
counters; no raw thinking or payload values survive. These estimates cannot replace
terminal usage, source evidence or model identity, and are not exact tokens/API calls
or spending. Exact terminal report bytes and accounting semantics are unchanged.

## Historical records, hosted operation and this migration

The original v1/schema 2, v2/schema 3, v3/schema 5 and v4/schema 6 Claude adapters
retain byte-identical frozen parsers and their original validation for exact
recovery and publication. They cannot execute or
establish current readiness. Adapter changes require fresh preparation and matching
live capability evidence; they never upgrade a failed diagnostic or reset its ledger.

Interactive setup uses authentication payload schema 2 with
`setup_provenance: human-interactive-native-v1` and a version-2 setup completion
record. The access-only auth mode is unchanged. Frozen authentication schema 1 and
old token-mode packets retain their original hashes, reports and publication bytes
for recovery, but cannot execute, retain activation through lineage, or establish
current readiness. A provenance/generation change requires explicit fresh packet
preparation; no old blocked store is silently upgraded or replaced. No setup or
renewal creates a new diagnostic allowance.


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


### Issue 33 revision-7 prospective recovery

Historical procedure, superseded by revision 8 below. Its grant stopped after trial 7; it authorizes no slot 8.

[Plan comment 5965161662](https://github.com/Zi-Deng/FLOW-DC/issues/33#issuecomment-5965161662)
is bound to contract `8c342fdbb9093d5b310bfe10854910dbf1e66e66527ac1a2514b7a90d615589b`
under existing standing authorization. No repeated user permission is pending.
A distinct schema-6 `recovery-v7-ledger.json` holds a version-5 grant, preserving
all four old ledger texts and all five trial evidence sets under their original
actual approval receipts and policies. Historical validation never reads credentials.
Current authority is mandatory before new authentication; edited history, counters,
limits, purposes or policy and partial migrations fail closed. Idempotent application
and verified same-account renewal cannot reset the count or relabel observations.

After reviewed implementation, full final-head checks and required CI, the coordinator
runs the preview and then applies its unchanged digest explicitly, from the clean
control checkout using the exact issue-worktree script:

```bash
python3 -B /absolute/issue-33-worktree/scripts/agentic/workflow.py claude-diagnostic-recovery
python3 -B /absolute/issue-33-worktree/scripts/agentic/workflow.py claude-diagnostic-recovery --apply --preview-digest <exact-preview-digest>
```

These commands do not infer. Counted slot 6 is fresh v5 native-tools-and-source;
slot 7 is isolation-refusal only after 6 qualifies. Invoke `diagnose-claude
--review-provider claude-code` separately for each permitted purpose. Every call
rechecks current guarded Max setup, account-bound paid-usage-disabled receipt,
expiry and endpoint/remote isolation. Seven total counted attempts include history;
each future call is at most 300 seconds/$2 reference estimate/zero extra spending,
total ceilings 2100 seconds/$14, prospective ceilings 600 seconds/$4. Any future
failure/interruption consumes its slot and stops the grant. No eighth call, automatic
retry/reset, substitution, permission widening, successful-output replay or synthesis.

Current adapter v5 requires diagnostic schema 7. V1/v2/v3/v4 recovery remains frozen
at schemas 2/3/5/6; packet/result/capture5 and report2 stay unchanged, schema4 reserved.
Historical v4 tools success cannot activate v5. Both new purposes must qualify under
the current policy. The canary is one owner-private existing regular single-link
file outside the workspace and all native exempt trees. A unique exact Read input,
native nine-field restricted-path advisory, exact matching error result and singleton
terminal denial must correlate in order with matching session/use ID and UUID.
Missing, duplicate, extra, delegated, aliased or mismatched evidence fails closed.
A dontAsk denial or missing-file error is insufficient. No denied source lines count;
ordinary reviews and tools diagnostics stay incomplete on every permission denial.
Unknown values retain bounded shapes/hashes only; exact terminal report bytes and
independent usage accounting are preserved. See [the native audit](NATIVE-TELEMETRY-AUDIT.md).

The full927 gate/CI at `6f865849` is historical after this repair; its CI merge
checkout was `3a0433202f98761021f3ddc4b5eca6bc4f73c004`, not the associated PR head.
New final-head checks/CI and actual current live evidence remain required. No model
PR review is requested under the migration-only exception; no generic gate bypass
or model-reviewed SHA is invented. Human merge and the separate PR32 original-UUID
reconciliation/review remain unchanged.

### Issue 33 revision-8 prospective recovery

[Plan comment 5965755308](https://github.com/Zi-Deng/FLOW-DC/issues/33#issuecomment-5965755308)
binds contract `dd3a54615f7db9cd099b083df98c33ca3a525552a5a591d20e7dc68b6977191e`.
The stopped revision-7 grant has zero executable allowance. Trials 1–7 remain
incomplete/incomplete/incomplete/qualified/incomplete/qualified/incomplete, with
all five original ledgers, approvals, policies, observed generations and report bytes
preserved. Trial 7 lacks qualifying Grep spans and has an unidentified refusal-call
predicate mismatch. Its raw input/envelope/output were not retained; source inspection
and report assertions cannot reconstruct them or change its status.

Current v6 uses diagnostic schema 8. V1–v5 recovery remains frozen at schemas
2/3/5/6/7, including the original v5 refusal helper. Packet/result/capture5 and
report2 are unchanged; schema4 remains reserved. Historical v4/v5 success cannot
activate v6. The sole optional tool-use annotation is exact `caller:{"type":"direct"}`;
unknown/null/extended/server callers and delegation remain incomplete. Main-session
agent identifiers must be absent, and a parent identifier may only be absent or null.
A direct annotation does not prove a tool result, source read or isolation. All native
restricted advisory/error/terminal correlations and no-exposure checks still apply.

Both new purposes require exactly one capability Grep with this complete input:

```json
{"pattern":"CLAUDE_NATIVE_CANARY","path":".","glob":"capability/fixture.txt","output_mode":"content","-n":true,"head_limit":10}
```

The schema-1 `diagnostic_tool_contract` containing this `grep_canary` is bound into
the new grant, packet, capture input digest and assessment. A directory search plus
the fixture glob yields the supported path:line:text rendering conditionally; a
single-file line:text result is still unsupported. Prompts do not guarantee obedience.
Wrong/missing/extra input, missing exact returned lines or duplicate calls stay
incomplete, without retry. Fixed call/Grep predicates, bounded shapes and hashes
make future failures distinguishable without retaining raw provider inputs.

After reviewed implementation, full final-head checks and required CI, the coordinator
uses the same explicit preview/apply commands shown above. The distinct schema-7
`recovery-v8-ledger.json` contains a version-6 grant, with exact current approval,
policy/tool contract and bounded snapshots of all seven trials/five ledgers.
Slot 8 is fresh v6 tools/source; slot 9 is isolation only after 8 qualifies. Each
is at most 300 seconds/$2 provider reference estimate/zero extra actual spending;
nine total attempts have ceilings 2700 seconds/$18, with prospective ceilings
600 seconds/$4. Failure/interruption counts and stops. No tenth call, reset, replay,
substitution, permission widening or automatic invocation is authorized. Preview,
apply and each invocation are separate coordinator actions. Human-only renewal,
receipt/lifetime/endpoint/remote checks and verified same-account lineage remain
unchanged; renewal creates no allowance or relabeling.

The 961-test gate at `0ed402a` and CI associated with that head (actual merge checkout
`dfb54803d4c363393b317e12cc9cef241b9539b0`) are historical after this change. Repeat
final-head checks/CI and obtain both current live purposes before activation. Pure
Node fixtures establish conditional source behavior only. The migration-only model
PR-review exception, strict ordinary gates, human merge and separate PR32 original-UUID
reconciliation remain unchanged.
