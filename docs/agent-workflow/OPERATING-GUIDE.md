# Issue to merged PR

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
snapshot, run Claude through Copilot CLI and publish its COMMENT review. Supply the
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
  --reviewed-sha FULL_SHA_RECORDED_IN_THE_REVIEW
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
