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

Explicit provider-bound batches use the [current coverage procedure](COVERAGE.md#provider-aware-bounded-batches-current).
They require a named finite authorization bound to the final executable preview and
tested harness, then sequential component requests and one integration request. They
never reuse historical reservations. Every invocation must perform all three provider
probes, even when no source range needs searching; the final message itself is JSON-only.
Prompts cannot guarantee compliance. Missing evidence remains incomplete.

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
By default, scopes are navigation within one request, not additional paid rounds.
Explicit schema-6 batches require separately supplied finite aggregate and per-unit
bounds and named authorization; see [bounded batches](COVERAGE.md#provider-aware-bounded-batches-current).
Single-request limits are
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
inventory and correlates them with actual successful tool results. The invocation asks
for one bare JSON object with no introductory prose or Markdown fences; capability and scope notes belong inside `limitations`. Batch prompts require
the assigned IDs, while single-request prompts require the full inventory. These are
prompt constraints, not a guarantee of model compliance. One complete outer
`json` fence is still accepted without changing saved bytes; surrounding prose stays
malformed and cannot be stripped to recover qualification. A checkmark, percentage, listing
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
New schema-6 packets freeze provider, exact model, effort, CLI identity, adapter, billing
and budget. Changes require an explicit fresh packet and any existing continuation
authority. Recovery uses the packet's original policy, never the current default.

Issue #33 / PR #34 has an explicit, migration-only exemption from independent model PR
review. No normal gate is bypassed and no model-reviewed SHA is asserted. Narrow
capability diagnostics remain limited to $2 estimated reference cost and 300 seconds
each, after credential, billing and isolation prerequisites. The original two-slot
allowance has only the explicitly approved issue-33 recovery exception described in
PROVIDERS.md. Retained successful v6 purposes 8 and 9 establish the native capability
activation prerequisite under their exact bindings, not PR inspection or readiness.
All nine attempts remain historical and the grant is exhausted; no attempt 10.
See PROVIDERS.md for current authentication, isolation and activation checks and
the separate human handoff. Other tasks retain normal review.
Repair stays on the original branch and Astra UUID. New commits invalidate readiness.

The dated migration sections below retain their then-current blockers and prospective
authorizations as history. Purposes 8 and 9 subsequently qualified under v6; the
nine-attempt diagnostic grant is exhausted. These historical sequences cannot be
executed again and supply no PR #32 inspection.

## Historical migration status — 2026-10-02, before revision-4 recovery

For issue #33, the coordinator verified guarded native Max login and preflight. The
first live diagnostic nevertheless remained incomplete; successful authentication
and preflight do not establish tool capability or coverage readiness. Its attempt
still counts against the two-attempt allowance, leaving one slot while both distinct
successful purposes remain required. The original report and sanitized evidence
are preserved. The repaired adapter requires fresh matching live evidence; any
additional recovery allowance requires an explicitly approved, bound amendment.
No such allowance is implemented, and this status does not waive any normal gate.


## Revision-4 recovery implementation — 2026-10-02

The subsequently recorded revision-4 approval authorizes one prospective replacement
slot, for three total counted attempts including failed slot 1. The explicit preview
and application interface in [PROVIDERS.md](PROVIDERS.md) preserves the original
ledger and all failed-trial bytes. Slot 2 must prove native-tools-and-source capability
before slot 3 can attempt isolation-refusal; a future failure stops the sequence.
No fourth attempt or report repair is authorized. The coordinator applies the grant
and invokes these diagnostics separately after final-head deterministic checks and CI.
Implementation and synthetic regressions do not establish live capability. Both
successful purposes and final-head CI evidence remain outstanding at this checkpoint.


## Stopped recovery and v3 offline repair — 2026-10-03 UTC

The coordinator applied revision 4 and invoked slot 2 once on `495dd3d`. Required CI
passed for that PR head with actual tested merge checkout
`47ecd1c24ab2da33b275e902e8d3e518a515a07f`; that evidence does not cover later commits.
Slot 2 returned valid report/tool/source evidence but failed initialization/customization
and system-event checks. The sequence therefore stopped; slot 3 was not invoked and
no executable allowance remains. Both trials retain their exact incomplete records.

The offline v3 repair enforces the supported bundled-skills setting and strict empty
initial/update catalogs, records bounded field shapes and hashes, and freezes v2
recovery semantics. Source fixtures do not reconstruct the missing trial payloads or
establish live capability. The stopped v2 grant cannot authorize v3. Any proposed
further recovery requires actual separate approval and exact binding before new
allowance code or calls. See [the source audit](NATIVE-TELEMETRY-AUDIT.md) for the
conditional native behavior and evidence limits. Final-head checks and both successful
current live purposes remain required; the migration-only PR-review exception persists.


## Approved revision-5 recovery — 2026-10-03 UTC

Historical: this grant stopped after incomplete trial 3. Its unused isolation slot 4
cannot run. The following records its original authorization and limits.

The maintainer's explicit standing override approves the complete revision-5 plan
and supersedes its preserved draft-state wording. The exact current contract is
`5c8c8c8bc87c2cd02229ea1c0f74b98a9748542fa770fc93178234d73501c45f`.
A separate prospective ledger preserves both failed trials and the stopped v4 grant.
Counted slot 3 is v3 tools/source; slot 4 is isolation-refusal only if slot 3 qualifies.
Each remains 300 seconds/$2 reference/zero extra actual spending, four total counted
attempts and no fifth. Failure/interruption stops this sequence. No repeat permission
question for this approved continuation is needed; explicit grant application and
separate coordinator invocation still follow final-source checks and required CI.
See [the exact operating sequence](PROVIDERS.md#issue-33-revision-5-prospective-recovery).

Frozen grant reads use the exact matching historical approval receipt after approval
supersession. Missing, edited or ambiguous history fails; it cannot authorize a new
call or current readiness. All required successful live evidence and final-head CI
remain independent obligations. The coordinator's full gate passed at `8516570`;
CI associated with that head tested merge checkout
`1034f122420a21ba85b01089ee556f74f52b7490`. Those receipts
do not validate later ledger changes or qualify a capability diagnostic.

## Revision-6 implementation and handoff — 2026-10-03 UTC

[Revision 6](https://github.com/Zi-Deng/FLOW-DC/issues/33#issuecomment-5964751600)
is bound under the standing authorization. The v4 adapter uses diagnostic schema 6,
closed nine-builtin false settings and a strict numeric `thinking_tokens` envelope.
V3 is frozen byte-identically for diagnostic schema 5 recovery; all three failed
trials and their old grants remain incomplete and unchanged. Progress estimates do
not establish usage or source inspection. Static fixtures are conditional evidence.

The [historical revision-6 sequence](PROVIDERS.md#issue-33-revision-6-recovery--historical-stopped-after-slot-5)
requires final-head checks/CI and explicit coordinator grant application before
counted slot 4 tools/source, then slot 5 isolation only after 4 qualifies. Five total
attempts include history; no sixth under this grant, and any failure stops it.
No repeat user approval is pending. No provider call follows automatically from
implementation. Both current successful purposes remain required for activation.

The coordinator recorded 901 tests and required CI associated with `6ebd582`, whose
actual tested merge checkout was `74bdee61b67d20b81e3c0375e65fdeba9c8ca68b` on
base `72e23c47ce40911f64cce5a7b9159a0cd51cdb72`. Those checks are historical after
v4 changes. Record final-head CI association separately from its tested checkout.
The migration-only model PR-review exemption remains; ordinary gates are unchanged,
no model-reviewed SHA is claimed, and only the human may merge. PR #32 is untouched.


## Revision-7 refusal correlation — 2026-10-03 UTC

[Revision 7](https://github.com/Zi-Deng/FLOW-DC/issues/33#issuecomment-5965161662)
is exactly bound under standing authorization. V4 trial4 qualified; trial5 remained
incomplete. All five historical records and four ledgers retain their exact original
meaning and bytes. V4 is now frozen for schema6 recovery; current v5 uses schema7.
The diagnostic canary requires the exact native restricted Read call, nine-field
advisory, error result and singleton terminal denial in order. A message substring,
dontAsk denial or absence of exposure cannot replace positive correlation. Ordinary
reviews remain incomplete on all denials. No report synthesis or replay occurs.

Historical revision-7 sequence (now stopped): [revision 7](PROVIDERS.md#issue-33-revision-7-prospective-recovery):
final-head checks/CI, explicit coordinator grant application, slot6 current tools/source,
then slot7 isolation only if6 qualifies. Seven total including history; each300s/$2
reference/zero extra spending; total2100s/$14, prospective600s/$4, failure-stop/no8.
The old grant is stopped. No repeated approval question or automatic call follows.
Prior927 tests and CI associated with6f865849 (actual checkout3a043320) are historical
for changed source. Both current live purposes remain necessary. Migration-only model
PR-review exemption, strict normal gates, human merge and untouched PR32 still apply.

## Revision-8 call provenance and deterministic Grep — 2026-10-03 UTC

Historical preparation used the [revision-8 sequence](PROVIDERS.md#issue-33-revision-8-prospective-recovery).
V5 telemetry and its helper remain frozen for schema7 recovery; current v6 uses
schema8. Exact optional direct-caller metadata is accepted only with all existing
identity, tool, input, result and terminal evidence. Unknown/delegated metadata is
refused. Fixed bounded call predicates identify which checks fail without raw logs.
Both new diagnostic packets bind the exact directory/glob/content/line-number/
head-limit Grep command. Unsupported rendering still earns no source spans.
Seven historical trials and five ledgers retain their original bytes and status;
trial7 remains incomplete on both refusal and Grep evidence. After final gates/CI,
coordinator-only explicit grant application permits slot8 tools/source and then9
isolation if8 qualifies, failure-stop/no10. No software repair or renewal runs inference.


## Full batch feasibility before authorization

Inspect the fresh preview, including omissions, component count, required/context
volumes and integration report envelope. Ordinary required test fixtures must not
have known unsupported material before a completion trial. Long-line inventory
projections are described in [COVERAGE.md](COVERAGE.md); they preserve raw bytes and
need actual returned evidence just like other required material.

For N sequential wrapper invocations at timeout T, the full-review elapsed allocation
must cover at least N × T plus preparation/dispatch overhead. A native receipt's
seven-day validity does not establish credential lifetime. Each native call requires
its credential to outlive its effective timeout plus the 300-second refresh margin
and 60-second clock allowance. A no-renewal full sequence therefore needs observed
remaining credential lifetime greater than the whole elapsed envelope plus 360
seconds, as well as a valid receipt throughout. The coordinator must establish this
without exposing credentials. Existing verified same-account human renewal preserves
history; it does not reset the original deadline, stop state or invocation count.
Do not assume a one-call preflight proves a long sequence feasible.

Integration must read all exact component reports. At C components and report bound
R, reserve at least C × R bytes if authorizing the worst-case report envelope; a
smaller explicit integration envelope may stop before its call when actual reports
exceed it. Parent context remains available, and navigation context is not proof of
semantic integration. Assess report size, required lines, provider context limits,
tool/event/capture bounds and the 900-second / $10 reference per-call ceiling together.
A storage bound or green synthetic test does not prove those volumes fit one model
context or one invocation. No automatic report truncation, extra integration call,
provider switch, renewed diagnostic grant or paid-extra spending follows a mismatch.
Keep the completion allocation blocked while feasibility evidence is missing.

### Bounded unit navigation

New plan-6 units start at `navigation/START.txt` and follow paged required-material,
related-context and complete-artifact indexes. Read explicit offset/limit windows;
use actual numbered Grep matches for discovery or blank tails. Do not request whole
large findings/disposition or source-index files. All originals remain available;
lossless context chunks do not grant source inspection credit. The trusted prompt
states the unit's frozen report ceiling, which may be below 50,000 bytes.

Source/test-family grouping (longest matching implementation stem, including specialized
test suffixes) replaces workflow-wide hash ordering for new packets.
Criteria, findings and cross-boundary obligations remain fully assigned. Linked
context is conservative, not a guarantee of sufficient reasoning. Plan 5 assignments,
exact reports and stopped ledgers retain their original semantics. The db897e9
one-invocation trial is incomplete and cannot be replayed; its seven positive primary
reads and successful probes did not overcome the failed Read. A fresh changed-head
preview, current CI and separately bounded coordinator authorization remain necessary.


Current reporting qualification follows [finite recovery v2](PROVIDERS.md#finite-reporting-recovery-v2):
only actual independently replayed purposes 12 and 13 admit ordinary v7 review.
Preserve the stopped 10 reservation/execution with unknown usage, unavailable 11,
all nine original diagnostics and historical approval/report bytes. No skill action
implicitly applies the new grant or invokes either purpose. Recovery stays offline;
a changed head needs fresh qualification, full independent review and hosted receipts
with PR head association distinguished from the actual tested checkout.
