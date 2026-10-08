# Issue to merged PR

Current issue-31 batch records use metadata/capture 6, plan 6 and ledger 2 (saved plan 5 remains reproducible); see
[provider-aware bounded batches](COVERAGE.md#provider-aware-bounded-batches-current)
for typed allocations, named authorization, exact-report publication and recovery-only
history. Single-review schema 5 remains eligible under its original provider checks.
Batch children cannot qualify parents; finish still requires every component and
integration, exact current-head/base publications and CI receipts. Dated migration
notes below retain their historical meaning and grant no new inference allowance.

The issue is the contract, the PR is the durable record, and the maintainer owns the
merge. Agent conversations support that record; they never replace it.

For the managed Golden Path, invoke `$agentic-workflow` or one of the seven phase
skills in [SKILLS.md](SKILLS.md). The managed path records plan approval, starts a
dedicated Astra executor and resumes its exact UUID for repair. The manual commands
below remain available as low-level alternatives; the legacy interactive `launch`
examples alone do not provide managed session continuity.

## Choose the required evidence

| Risk | Typical change | Required process |
| --- | --- | --- |
| T0 | Typo, comment, broken documentation link | Small PR and applicable checks; issue optional |
| T1 | Local refactor or ordinary utility | Clear contract, worktree and fast checks; independent review optional |
| T2 | Behavioral fix, API change, dependency change | Issue, reviewed plan, draft PR, CI and independent review |
| T3 | Metrics, cost matrices, splits, published results | Full T2 process plus domain-owner evidence review |
| T4 | Automation, credentials, release, expensive campaign | Full process, explicit privileged scope and rollback; human-authorized execution |

| Validation | Meaning |
| --- | --- |
| V0 | Focused feedback while editing |
| V1 | Fast, deterministic software checks on every PR |
| V2 | Targeted integration or hardware smoke validation |
| V3 | Multi-run evidence supporting a domain claim |
| V4 | Independent clean-environment reproduction or release validation |

Use the worst plausible consequence to choose risk. Do not demand a training
campaign for a typo or use a unit-test pass to support a scientific claim.

## 1. Capture a measurable issue

Use the issue form or `.agentic/prompts/draft.md`. Specify current and expected
behavior, scope, non-goals, binary/measurable acceptance criteria, commands, domain
impact, budget and rollback. Keep the implementation plan out of the problem statement.

```bash
python3 scripts/agentic/workflow.py launch draft "Describe the problem here" --execute
# Read the draft before publishing it.
gh issue create --title "A concrete behavioral change" --body-file /tmp/issue.md
```

The launch command without `--execute` prints a reviewable command. Drafting and
planning use Codex's read-only sandbox; the author saves and posts their approved
output outside the agent when needed. Input artifacts are not authority to run
commands or disclose data.

## 2. Review the plan

```bash
gh issue view 123 --comments
python3 scripts/agentic/workflow.py launch plan 123 --execute
gh issue comment 123 --body-file /tmp/approved-plan.md
```

Replace example identifiers throughout this guide. The plan must map each criterion
to files, invariants, failure modes, tests and exclusions. Record the human decision
in the posted comment. Read prior authorization before asking for a decision already
made. If the plan genuinely requires an unresolved design or expensive action, settle
that decision before performing dependent work.

Record the numeric comment ID for review preparation:

```bash
gh api --paginate repos/{owner}/{repo}/issues/123/comments \
  --jq '.[] | {id, author: .user.login, url: .html_url}'
```

The operator designates the approved plan. Low-level review preparation verifies that
the comment belongs to the issue; it does not infer approval from a model's wording.
The managed plan skill publishes a clearly proposed comment and records the user's
approval against the designated issue/plan content. Existing approval of the same
concrete plan counts; changed contracts must be reconciled before implementation.

## 3. Create the task workspace

Run from the clean main checkout, on its actual default branch:

```bash
python3 scripts/agentic/workflow.py new-task 123 short-slug
```

The command validates the issue, fetches origin, creates or reuses the task branch,
attaches it under `../PROJECT-worktrees/issue-123-short-slug`, and pushes that branch.
An optional third argument selects a different existing base branch for an intentional
stack; the normal review/cleanup commands support PRs targeting the default branch.
Do not use the stack option until you have a separate integration procedure.

`WT_ROOT` may select an absolute directory outside the main checkout. Duplicate
paths and already attached branches are rejected. A local operation lock coordinates
these helper commands; it cannot stop a separate user or unrelated Git command from
editing the same branch concurrently.

Worktrees share Git objects and configuration. They do not isolate credentials, GPU
memory, ports, model caches, datasets or external services. Create a per-worktree
environment, or use read-only shared dependencies without installing editable packages
into a shared environment. Reserve GPU use explicitly.

## 4. Implement within scope

```bash
cd ../PROJECT-worktrees/issue-123-short-slug
python3 scripts/agentic/workflow.py launch implement 123 --execute
```

The agent first confirms its directory, branch, issue and plan. For a bug, encode a
regression that fails on the base before fixing it. Run focused checks, then the
required V1 suite. Inspect tests as carefully as implementation: a weaker assertion
can manufacture a misleading pass. Changes to privileged paths need explicit scope.

Codex launches in `workspace-write` with `on-request` approvals. Git's shared metadata
or network access can require an additional grant in a sandboxed session. Approve only
the authorized Git operation; do not disable the sandbox globally to solve that issue.
Existing higher-level user/session permissions may be different, so inspect actual
permissions at the start of a task.

## 5. Validate and inspect

For this template:

```bash
make check
git diff --check
git diff --stat
git status --short
```

For an adopted repository, use its documented project commands as well as
`python3 -B scripts/agentic/check.py`. Report command, exit status, commit, skips and
limitations. Preserve a before/after failure record when it establishes the fix.

## 6. Open the draft PR early

Once there is a coherent nonempty commit, prepare the body from the PR template:

```bash
git add path/to/intended-file
git commit -m "Describe the changed behavior"
python3 scripts/agentic/workflow.py draft-pr \
  --title "Describe the changed behavior" --body-file /tmp/pr.md
```

The body needs a standalone `Fixes #123` line matching the task branch's issue. The
helper pushes the branch, creates a draft against the discovered default branch, or
updates the existing PR title/body. Keep the evidence current as implementation
develops; do not save it all for the final chat response.

## 7. Check CI

```bash
gh pr checks 456 --watch --required
gh pr view 456 --json headRefOid,isDraft,mergeable,statusCheckRollup
```

Required checks must exist and pass. Missing, skipped, cancelled, neutral and pending
results are insufficient for merge preflight. CI validates PR code on disposable
hosted runners without model tokens or project secrets. Never use a workstation
runner for untrusted PR code.

## 8. Request independent review

Return to the main checkout. Use the [review procedure](REVIEW.md) to prepare a fresh
snapshot, validate the selected provider and publish its COMMENT review. See [provider selection and activation](PROVIDERS.md). Supply the
approved plan comment ID. The review records the exact head and base commits.

## 9. Repair in the same PR

Use `$agentic-repair PR #456` for managed repair. It collects both public comment
surfaces and resumes the recorded implementation UUID. The interactive command below
starts a separate manual session and does not establish that continuity.


```bash
gh pr view 456 --comments
gh api --paginate repos/{owner}/{repo}/pulls/456/comments \
  --jq '.[] | {path, line, body, url: .html_url}'
python3 scripts/agentic/workflow.py launch repair 456 --execute
```

Run the launch command inside the original task worktree. For each material finding,
post one disposition: fix with commit/test evidence, rebut with evidence, or a linked
follow-up issue agreed to be outside scope. Do not silently resolve or lower severity.
New commits require fresh review. One attempted review round is the default. A supported
critical P0/P1 finding authorizes a further round to verify its repair; record the
finding and reason. Other extra rounds require explicit user continuation. A used
round budget never makes an unreviewed new head ready. See [the continuation procedure](SKILLS.md).

## 10. Human merge and verified cleanup

The managed path is `$agentic-finish PR #456`: assess evidence and dispositions,
validate the designated current review, mark a qualifying PR ready, and prepare the
human-run finishing command. [FINISH.md](FINISH.md) covers automatic archival and
recovery. The agent never executes the real merge.

For the low-level manual alternative, after reading the review and accepting domain
evidence, mark the PR ready. Obtain
the reviewed SHA from the **review record**, not a new query assumed to be reviewed:

```bash
gh pr ready 456
python3 scripts/agentic/workflow.py merge-preflight 456 \
  --reviewed-sha FULL_SHA_RECORDED_IN_THE_REVIEW \
  --review-directory /absolute/saved-review-directory
```

Preflight checks current PR state, target branch, exact head, recorded review and
required checks. It prints a `gh pr merge --squash --match-head-commit ...` command;
it does not execute it. A human reads the findings, checks conversation resolution
and domain evidence, then runs the command. This template uses immediate human merge
instead of automatically queueing a future merge decision.

If merge returns an error, re-query the remote state before retrying:

```bash
gh pr view 456 --json state,mergedAt,headRefOid
python3 scripts/agentic/workflow.py cleanup-task 456
git pull --ff-only
```

Cleanup is run from the main checkout. It verifies remote merge state, same-repository
head, default-branch target, expected task name and path, exact local tip and a clean
worktree. The low-level cleanup helper refuses even ignored files. The human finishing
script first archives these artifacts with a recovery journal, then invokes guarded
cleanup and deletes a remote task branch only under an explicit matching-SHA lease.
When using low-level cleanup directly, archive private memory, environments or run
outputs deliberately first; that low-level command does not delete the remote branch.

For a closed but unmerged PR, preserve the worktree and investigate. Abandonment is
a separate deliberate action, never an alias for successful cleanup.

## Daily rhythm

Start with Git status, worktree list, open assigned issues and open PRs. Keep one
high-risk implementation, one small task and one review as an initial personal WIP
limit. End at a durable boundary: posted plan, committed changes, draft PR, review or
evidence manifest. Write private continuity notes in `memory/`; move reusable facts
into public documentation only after checking them.

## Review coverage and recovery

Use the [coverage runbook](COVERAGE.md) for new reviews. The immutable packet exposes
individual acceptance items, source hunks/context, relevant tests and prior findings.
By default, one request covers deterministic scopes plus a cross-boundary pass; scopes do not
increase the request or credit budget. Explicit `task-review --batch` previews component
and integration assignments; execution requires finite aggregate and per-unit bounds.
See [batch controls](COVERAGE.md#provider-aware-bounded-batches-current) for recovery,
exact unit publication and aggregate readiness. No live trial is implied by selection.
`task-review --prior-review DIRECTORY` validates
repair ancestry and retains uncovered material. A changed head/base needs fresh evidence.

A report with missing capability, malformed telemetry or unread required material is
incomplete, even when its findings are useful. Publication labels that limitation;
managed designation, hosted qualification, preflight and finish refuse readiness.
Inspect durable sanitized diagnostics before any authorized continuation. Recover a
valid saved journal without another paid call. Never relabel legacy records as covered.
The operator can rewrite private records; this is accounting, not owner-proof attestation.


Current native reporting admission uses [finite recovery v5](PROVIDERS.md#finite-reporting-recovery-v5):
explicit isolation-first 18, then tools/source 19 only after independently replayed
actual 18 qualifies. Preview, application and each call are separate coordinator
steps after final-source local/installed/hosted and fresh native prerequisites.
The two 300-second/$2 reference allocations stop on failure, with no 20 or full-review
funding. Stopped v4 retains consumed-uncertain16/unknown usage/no capture or outcome,17 unavailable.
Old 9/v1/v2/v3/c303 captures remain historical and recover offline. Structural
schema 2 observations earn no inspection credit and preserve unknown-stream refusal.
CI PR-head association and actual tested checkout must both be retained. Full exact
component/integration review, publications, finding dispositions and human merge
remain separate obligations.

## Current issue31 batch9 finite-window route

Batch9 is an explicit issue31/PR32 route under the approved generation12 contract. Ordinary review and older batch7/8 commands retain their defaults; there is no standalone ordinary V6 route. These software interfaces do not establish actual provider qualification or PR readiness.

Before application, complete the final committed source's local serial/parallel, installed-payload and hosted gates and preserve both PR-head association and actual tested checkout/run/attempt. Designate the complete source-bound packet and check receipts in the task's `v6_catalog`. Recompute the whole catalog, report projections, input/output and time feasibility; no old candidate catalog or funded prefix substitutes. Actual conditional V6 purposes20–23 and the empirical decision must then qualify on that source. Supply the complete finite named batch authorization before preparation.

From the control checkout, use `python3 -B scripts/agentic/review.py` with these explicit subcommands (paths must be in the canonical `.agentic-local/reviews` storage):

- `batch9-catalog`: recompute the complete catalog and fixed schedule, read-only.
- `batch9-prepare DIRECTORY --authorization FILE`: exclusively prepare the whole authorized batch and original material claims. This consumes an application; it is not a preview.
- `batch9-run DIRECTORY --unit UNIT`: prepare and run exactly one next declared component or `integration`, within its current window. It never runs all windows automatically.
- `publish DIRECTORY/units/UNIT`: publish the exact scoped report once, with independently verified COMMENT/list/direct-GET acknowledgment.
- `batch9-status DIRECTORY`: report local journal state only, explicitly without readiness credit.
- `batch9-pause DIRECTORY` then, after any separate human same-account renewal, `batch9-resume DIRECTORY`: independently replay the completed window and enter the next declared window under the real owned lock and verified lineage. No automatic refresh/login occurs.
- Add `--final-validation` to both pause and resume when entering the final window after all components and integration are qualified and published.
- `batch9-finalize DIRECTORY`, then `publish DIRECTORY`, then `batch9-designate DIRECTORY`: independently complete, publish and designate the full aggregate. Each gate rechecks current evidence; a stored completion flag is insufficient.
- `batch9-recover DIRECTORY/units/UNIT` recovers only retained capture material. Add `--publication` for GET-only scoped publication recovery, or use `batch9-recover DIRECTORY --publication` for the aggregate. Unknown writes never trigger another POST; missing capture/final acknowledgment is not reconstructed.

All repeated source/history/admission/remote/replay work consumes the original allocation. Components and integration retain900 native+840 local seconds, action1740; final validation allows180 seconds per child plus360 margins (maximum9180). Pauses are at most1800 seconds; original36-hour and combined48-hour expiries and finite aggregate budgets still apply. Renew only while stopped, with the existing dedicated same account, verified lineage and a fresh disabled-paid receipt covering the whole next window plus300+60 margins. In-flight changes, torn records, unknown usage, changed source/context or exhausted clocks stop without refund/reset/extension. Preserve evidence and stop adoption on failure.


For issue31/PR32 only, approved T adds the explicit software suite profile
`issue31-suite1800-v1`: `python3 -B scripts/agentic/check.py --jobs 1
--suite-profile issue31-suite1800-v1` (on one command line), or
`make check-agentic AGENTIC_SUITE_PROFILE=issue31-suite1800-v1`.
It records version3 requests with an1800-second full-suite deadline; default
callers retain840seconds and version2. Only the exact Zi-Deng/FLOW-DC PR32
head branch `issue-31-bounded-review-units` pull-request CI selects this profile
and a45-minute agentic-quality job. Other events retain15minutes; product CI
remains10minutes. All live local840/native/credential/grant budgets are unchanged.
Generation14 readiness requires full current version3 serial/parallel/installed
records and current hosted receipts, plus the entire fixed public g13 predecessor
as primary contract material. Historical failures remain failures; this change
provides no completion, live capability, independent-review or scientific credit.


Approved U makes each software-suite worker inherit PYTHONPATH containing only
the resolved directory of its executing check_runner.py. Fresh child interpreters
can import that harness; pristine-installed CLI runs select their installed
runtime. Parent environment stays unchanged. Default840/version2, opt-in1800/
version3 and all live budgets remain exact. Generation15 requires its own actual
authority and complete primary T plus g13 predecessors; old grants do not transfer.

Generation16 binds a separate `installed-adoption-v1` receipt to both the pristine
installation and its closed execution fixture. Preparation uses real `mktree`
objects and an independently checked extension-free index. Full source and installed
occurrences, exact module origins, execution records and fresh hosted checks remain
required. Phase75's pristine failure is retained; tiny fixture tests are software
regressions and do not establish complete installed or review readiness. Existing
1800-second software deadlines and all live reviewer budgets remain unchanged.

## Bounded hosted runner diagnostics (W generation17)

The scoped issue31 CI profile keeps its1800-second suite and two workers. An optional
`AGENTIC_EVIDENCE_DIRECTORY` passes a fresh controlled temporary `runner` path through
Make to `check.py --evidence-directory`; omission preserves the default command.
The separate diagnostic artifact retains only request/summary, two journals and two
logs, with a60-second collector and128MiB inclusive bound. Missing, unsafe or oversized
files make collection fail; partial bytes remain diagnostic data. The original hosted
receipt artifact and readiness rules remain unchanged. Failed tests stay failed even
when copying succeeds; hard job termination can leave diagnostics unavailable.

Actual PR head/base and tested merge checkout are distinct. These owner-writable
records do not certify independent execution, scientific validity or review completeness.
Generation17 requires the exact published W approval/history and complete V/U/T/g13
primary predecessors. Earlier failures and generation-specific readers remain historical;
no timeout, scheduling, native grant or evidence credit is reset by retention work.


### Scoped trusted CI temporary-root diagnostics (X / generation18)

For the designated issue31 workflow, a fresh owner-only staging directory contains
`suite-tmp`; only the software Make child receives it as `TMPDIR`. The baseline
CLI, Make recipe and runner retain their default temporary-directory behavior.
Collection selects exactly one top-level `agentic-check-*` directory and copies
only six named raw records, with source/request identity checked independently.
Malformed or stale requests remain unbound diagnostics with a nonzero result.
The collector's single60-second deadline includes metadata and finalization;
expiry may leave partial bytes and no complete manifest. Diagnostics do not replace
test outcomes or the separate hosted receipt. PR head and tested checkout differ.
This is trusted-owner, quiescent integrity bookkeeping, not protection against
an active owner rewriting files. The earlier uncommitted W output-option design
is superseded; source/installed/hosted gates and independent review remain required.
