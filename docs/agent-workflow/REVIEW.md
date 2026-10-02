# Independent provider review

The reviewer receives committed artifacts in a fresh isolated provider process, without
the implementation conversation or private memory. It performs static inspection through
Claude native `Read`, `Grep`, `Glob` or explicit Copilot `view`, `grep`, `glob`.
Read [provider selection and activation](PROVIDERS.md) before inference. Claude requires
verified isolation controls, guarded native Max authentication with a current receipt
and sufficient credential lifetime, and both successful live capability/isolation
diagnostics. The wrapper performs Git/GitHub operations outside the model process.
Read [the coverage contract and migration runbook](COVERAGE.md) for the packet, adapter, evidence schema, recovery and all readiness gates.

## Managed skill procedure

Use `$agentic-review PR #456` from the clean trusted control checkout. The coordinator
prepares the approved issue/plan and fixed head/base snapshot, invokes the configured
provider/model/effort once and publishes a COMMENT review. Use `$agentic-repair` to resume
the original Astra executor UUID for repairs. See [SKILLS.md](SKILLS.md).

One attempted round is the default, including failed or incomplete attempts. A supported
critical P0/P1 finding permits a further round to verify its repair; record the public
finding and concrete reason. Other extra rounds require explicit user continuation.
P2/P3 findings, uncertain questions and incomplete coverage do not automatically permit
another request. Never silently switch models, broaden permissions or launch another
wrapper inference attempt. The pinned native 401 boundary is disclosed in
PROVIDERS.md; it grants no wrapper retry.

## Local procedure

```bash
python3 scripts/agentic/review.py prepare 456 --issue 123 --plan-comment 987654321
python3 scripts/agentic/review.py run /absolute/path/printed/by/prepare
python3 scripts/agentic/review.py publish /absolute/path/printed/by/prepare
python3 scripts/agentic/review.py qualify /absolute/path/printed/by/prepare
```

Preparation paginates the public contract, discussions, reviews, check runs and status
contexts. It reads Git blobs without checkout, filters or project execution. Small
contract artifacts and deterministic component scopes lead to required code, tests,
findings/repairs and cross-boundary material; complete source and diff remain available.
A repair packet may add `--prior-review DIRECTORY`. Its validated ancestor report
provides delta-first navigation while preserving current original scope and prior
uncovered obligations. It cannot inherit qualification for the new head.

`run` performs its actual-tool capability probe inside the same request and records
sanitized events. It returns exit 2 when a report is saved but coverage is incomplete.
Publication retains useful findings with an explicit incomplete label. Only verified
complete material can be designated by the managed pipeline or pass low-level/finish
readiness. Findings still require human disposition. Model reports never impersonate
human approval or resolve threads automatically.

## Isolation and budgets

Snapshots map original paths to numbered inert `.txt` blobs. PR agent profiles, hooks,
skills and configuration remain data. Active policy/profile come from the trusted
clean default-branch checkout. The process has a temporary home, fresh provider/XDG
state, disabled hooks/MCP, no inherited provider override, no prompt memory/resume,
no permission to execute, edit or delegate and no broad `*` permission. Authentication
is supplied separately: Copilot uses its isolated token environment; Claude requires
a guarded access-only native snapshot (see PROVIDERS.md). Workspace hashes detect changes.
These controls restrict model tools/config discovery; they are not an OS sandbox
against a compromised CLI executable.

Private/data exclusions include `memory`, `.agentic-local`, credentials, `files/input`,
`files/output`, `files/biotrove_train_stats.json`, `benchmark/manifests`,
`benchmark/results`, `playground` and `archives`. A diff touching them is refused,
including deletions/renames. Symlinks/submodules are never followed. Maintained example
configs and benchmark code remain inspectable. Path exclusions are not a content-based
secret detector: inspect public source and comments for sensitive material.

`max_diff_bytes` is unlimited when null/omitted; an explicit positive integer opts into
a hard pre-packet cap. There is no silent diff truncation. Per-source-file limit is
250000 bytes and the combined head/base/carried-source budget is 12000000 bytes.
Scopes are navigation within one request, not additional paid rounds. Limits are
900 seconds and, for Claude, $10 estimated reference cost with zero extra spending
authorized; explicit Copilot retains 400 AI credits. These are different units. In-flight
requests may overshoot estimates; unknown usage cannot authorize continuation. Large
reviews may exhaust time/context/usage and remain incomplete.
The separate managed executor prompt limit remains 300000 bytes.

Pinned versions are native Claude Code 2.1.282 and optional Copilot 1.0.83. Requested configuration, help and synthetic fixtures do not
prove live tool availability. Successful actual canary calls and supported events are
necessary. An unknown layout yields a durable incomplete result and actionable reasons;
it never triggers a broader permission workaround or automatic paid retry.

## Exact storage, publication and recovery

`review.md` contains exact model-response bytes. `review-capture.json` saves them with
sanitized diagnostics before assessment reads packet files; `review-result.json` journals
the resulting assessment before final storage. Retry storage with the same directory only
when that journal is valid; recovery and completed-result reuse invoke no model. A
started attempt without a valid journal needs investigation, not a blind rerun.
Reports exceeding the 60000-byte publication limit remain intact; the prompt requests
less than 50000 bytes but that guidance is not a guarantee.

GitHub publication uses exact UTF-8 JSON transport, an attributed coverage label,
explicit head SHA and an idempotence marker. Reconciliation compares the whole body,
head and state. Missing/changed diagnostics, altered reports or stale head/base fail.
Legacy records stay readable but cannot acquire new qualification. Use
`review.py verify-publication DIRECTORY` for a read-only exact-byte comparison with
GitHub, including historical control characters; it performs no model request.

Raw provider homes, reasoning, unrestricted logs, tokens and environment dumps are not
archived. Bounded sanitized diagnostics and known usage counters survive failures.
Local state directories use owner-only access and atomic records use mode 0600. The
owner can rewrite private records and hashes; they are not independent attestations.

## Report contract

Return compact schema-2 JSON matching `report-schema.json`: copy `inventory-sha256.txt`,
list positively inspected IDs in `reviewed`, group specific reasons in `incomplete`,
and state general `limitations` once. Every unclaimed inventory ID remains unread and
blocks readiness. The wrapper supplies original paths/ranges from the hash-bound
inventory and correlates them with actual successful tool results. One complete outer
`json` fence is accepted without changing saved bytes. A checkmark, percentage, listing
or diff header cannot replace source/test inspection. Observed reads do not prove
understanding. Do not infer a provider timeout or exhausted budget from partial coverage.
Historical assessments/publication bytes retain their original policy; current readiness
requires the new schema. See COVERAGE.md for recovery and migration.

| Severity | Meaning |
| --- | --- |
| P0 | Concrete catastrophic merge blocker |
| P1 | Likely major correctness or security failure in supported use |
| P2 | Reachable edge-case defect or substantive evidence gap |
| P3 | Minor optional improvement; suppress tooling-covered style remarks |

Every finding needs an ID, severity, original path/line, claim, trigger, impact,
evidence and minimal fix direction. Empty findings are allowed when no material defect
is supported. State uncertainty and unsupported acceptance evidence honestly. Never
invent executed commands. Apply [the domain rubric](domain-review.md) to scientific
changes: green software checks are not scientific validation.

## Static versus executable validation

`validation.json` contains separately attributed, point-in-time CI observations and
available hosted execution receipts. Each receipt distinguishes PR head association
from the actual tested checkout/merge SHA. Unknown, missing, skipped, failed, stale or
expired observations stay visible. Required CI still must pass independently. The
reviewer executes no tests and receives no model shell access.

CI runs PR code only on disposable hosted runners without model/cloud secrets.
Never execute unfamiliar PR code on a credential-bearing workstation as a substitute.
For this implementation the coordinator separately runs the local suite in its existing
allowed environment because the executor sandbox denies local test-server sockets.

## Manual Actions procedure

Complete [the protected environment setup](SETUP.md#6-hosted-review-is-opt-in), then
run the default-branch workflow with PR, issue, designated plan and exact head:

```bash
gh workflow run copilot-review.yml --ref main \
  -f pr=456 -f issue=123 -f plan_comment=987654321 \
  -f head_sha=FULL_CURRENT_PR_HEAD_SHA -F publish=false
```

The model job is read-only; publication runs separately with PR write permission and
no model. Neither executes PR code. Sanitized artifacts upload on failed runs too.
When publication is requested, an intact partial report can be published after an
incomplete run, but the qualification step keeps the workflow nonpassing. Same-repo
PRs targeting the default branch are supported. No comment-triggered repair loop or
self-hosted runner is enabled.

## Model selection and repair

Select provider/model/effort explicitly when overriding the local default. Precedence is
per-call options, private saved selection, then trusted configuration; switching provider
without a model selects that provider's default. Use `workflow.py review-selection` to
inspect the effective selection and provenance, and `--save` from the clean control
checkout to save it. Both local `prepare` and managed `task-review` accept
`--review-provider`, `--review-model`, `--review-effort`. Unsupported aliases/combinations
fail before inference. See [the complete provider procedure](PROVIDERS.md).

Historical Sonnet/Fable and Copilot adoption records retain their original meaning.
New schema-5 packets freeze provider, exact model, effort, CLI identity, adapter, billing
and budget. Changes require an explicit fresh packet and any existing continuation
authority. Recovery uses the packet's original policy, never the current default.

Issue #33 / PR #34 has an explicit, migration-only exemption from independent model PR
review. No normal gate is bypassed and no model-reviewed SHA is asserted. At most two
narrow capability diagnostics ($2 estimated reference cost and 300 seconds each) are
authorized after all credential, billing and isolation prerequisites exist. They are not
a PR review; no diagnostic has established native capability. See PROVIDERS.md for
activation blockers and the separate human handoff. Other tasks retain normal review.
Repair stays on the original branch and Astra UUID. New commits invalidate readiness.

## Migration status — 2026-10-02

For issue #33, the coordinator verified guarded native Max login and preflight. The
first live diagnostic nevertheless remained incomplete; successful authentication
and preflight do not establish tool capability or coverage readiness. Its attempt
still counts against the two-attempt allowance, leaving one slot while both distinct
successful purposes remain required. The original report and sanitized evidence
are preserved. The repaired adapter requires fresh matching live evidence; any
additional recovery allowance requires an explicitly approved, bound amendment.
No such allowance is implemented, and this status does not waive any normal gate.
