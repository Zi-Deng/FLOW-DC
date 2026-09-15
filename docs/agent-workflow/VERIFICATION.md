# Verification record

Baseline recorded 2026-09-10, with the Golden Path extension recorded separately below
on 2026-09-14 (America/Los_Angeles; linked Actions timestamps use UTC).
This file distinguishes implemented behavior, tests actually
executed, and account-dependent verification. A green local suite does not establish
that a model account or GitHub environment has been configured.

## Local template validation

- **30 regression tests passed** using real temporary Git repositories and bare
  remotes. GitHub and Copilot calls in that suite are controlled test doubles.
- Ruff 0.16.7 lint and formatting checks passed.
- actionlint 1.7.12 accepted the three GitHub Actions workflows.
- The suite exercises sibling isolation, branch reuse, non-`main` default branches,
  dirty-checkout rejection, malformed input rejection, SHA-bound review publication,
  snapshot mutation detection, private-path exclusion and symlink handling.
- Cleanup tests prove that squash-merged work can be removed while dirty/ignored
  work, fork branches, wrong bases and post-merge local commits are preserved.
- Merge preflight tests reject stale heads, missing review records and missing or
  skipped checks; the successful path prints a pinned command without merging.
- Installer previews for NICME and SpiderML found no conflicting workflow-owned
  paths at inspection time and wrote nothing to either project.

Reproduce the template gate:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-dev.txt
make check
```

CI additionally checks that tests did not dirty a committed checkout. Local private
memory and virtual environments are ignored; absence from the index is checked
separately before publication.

## Limited NICME baseline

Copied the current `nicme/`, `utils/`, `model/` directories and three test modules to
a temporary directory. Ran them with the existing `ml` environment, GPU visibility
disabled, Hugging Face offline variables, W&B offline and pytest's cache provider
disabled:

```text
test_costs.py + test_calibration.py + test_loss_functions.py
56 passed in 2.42 seconds
```

This establishes a passing CPU subset for the copied working files and the installed
environment. It is not a clean-clone or locked-environment result, not a pass for all
NICME tests, and not reproduction of the research findings. The working tree differed
from HEAD, so the test result must not be attributed to HEAD `e0df0a4` alone. NICME
itself was not edited and no GPU campaign was launched.

## Live GitHub bootstrap verification

On 2026-09-10, published [Zi-Deng/agentic-github-template](https://github.com/Zi-Deng/agentic-github-template)
as a public repository and enabled the template flag. The initial commit is
`fc2556e19445b694b8c76f3462843587ee317fb0`.

Both initial hosted workflows succeeded:

- [ci / quality run](https://github.com/Zi-Deng/agentic-github-template/actions/runs/34541182069).
- [agentic workflow tests run](https://github.com/Zi-Deng/agentic-github-template/actions/runs/34541182063).

Created [pilot issue #1](https://github.com/Zi-Deng/agentic-github-template/issues/1)
and its [designated plan comment](https://github.com/Zi-Deng/agentic-github-template/issues/1#issuecomment-5626696118).
The new-task command created and pushed `issue-1-live-verification` in a sibling
worktree while main stayed clean. The initial publication included 41 tracked files,
with no memory, runtime state or virtual environment files.

The [pilot PR #2](https://github.com/Zi-Deng/agentic-github-template/pull/2) was created
in draft using the helper. Its hosted `quality` and `agentic-quality` checks passed.
The effective `main` rules include PR review-thread resolution, strict required
`quality` from GitHub Actions integration 15368, linear history, and blocks on force
pushes/deletion. Ruleset ID: `22841485`; no bypass actors are configured.

### Local review after the plan upgrade

The first review attempt failed because the credential could not select the configured
Claude model. It returned nonzero, produced no `review.md`, and posted no model review.
After the maintainer enabled an eligible Copilot plan, the existing `gh` OAuth
credential successfully ran the requested model, `claude-sonnet-5`. The CLI usage
artifact also reported that model. No automatic model substitution was used.

The [local COMMENT review](https://github.com/Zi-Deng/agentic-github-template/pull/2#pullrequestreview-5173711278)
was published against `bd322735cc4c3140aae7ffe7264548e4a962e12b`, with base
`fc2556e19445b694b8c76f3462843587ee317fb0`. It reported no P0/P1/P2 findings but
inspected only part of the supplied evidence. Its optional observations and
limitations remain in the report; this is a static review, not human approval.

### Hosted generation and artifact verification

Protected environments `copilot-review` and `copilot-review-publish` were configured
with maintainer approval and protected-branch deployment restrictions. The maintainer
stored a fine-grained Copilot Requests token, the opt-in variable was enabled, and
the first run paused until the maintainer approved `copilot-review`.

[Actions run 34548086695](https://github.com/Zi-Deng/agentic-github-template/actions/runs/34548086695)
then succeeded: pinned CLI installation, packet preparation, Claude generation and
artifact upload all passed. It used trusted control code from the initial main
commit and reviewed head `bd322735cc4c3140aae7ffe7264548e4a962e12b`. The downloaded
artifact's packet and report hashes were verified locally; its requested and
CLI-reported model were both `claude-sonnet-5`.

That run used `publish=false`, so the separate Actions publication job was correctly
skipped. After inspection, the downloaded report was published using the local
wrapper as a [SHA-bound COMMENT review](https://github.com/Zi-Deng/agentic-github-template/pull/2#pullrequestreview-5173913692).
This verifies hosted generation and local publication of its artifact. It does not
claim that the separate Actions publication job has been exercised.

The hosted report identified the obsolete statement that Claude access remained
blocked. This follow-up record fixes that P2 documentation finding. It also clarifies
the two credential identities in the setup guide. The report's claim that check-run
evidence was absent is inaccurate: the downloaded `context.json` contains successful
`quality` and two `agentic-quality` runs plus the preceding local review. The reviewer
disclosed incomplete inspection of those arrays. Branch-rule read-back remains an
operator verification, since the packet does not include the ruleset object.

### Remaining acceptance boundary

Account access and hosted inference are now verified. The pilot remains subject to
review of its latest head, maintainer assessment and a human merge decision. Each
review above is evidence for its recorded commit, not for later documentation edits.
The effective branch rules and remote tree inventory were read back through GitHub;
no private memory/runtime path appeared in the remote tree.

Future adopters must configure their own token, protected environments and opt-in
variable. This repository's settings are not copied by GitHub's template mechanism.
Local regression tests cover cleanup without an unattended production merge.

## Golden Path skills extension — 2026-09-14

The public contract is [issue #5](https://github.com/Zi-Deng/agentic-github-template/issues/5)
and its [amended approved plan](https://github.com/Zi-Deng/agentic-github-template/issues/5#issuecomment-5671809804).
Implementation is published in [PR #6](https://github.com/Zi-Deng/agentic-github-template/pull/6).
The original checkout's unrelated untracked material was preserved; work used a clean
control clone and a registered sibling issue worktree. Neither adoption target was edited.

### Evidence obtained during implementation

- **111 regression tests passed** in the full local gate. The suite uses real temporary
  Git repositories and local processes, with mocked GitHub/model services. It covers
  publication reconciliation, approval invalidation, exact-UUID repair with all feedback
  surfaces, stale review rejection, bounded process termination, queued/failed merges,
  archive recovery, advanced remote refs, and installer preservation. Lint, formatting,
  workflow configuration and skill/documentation link checks passed.

- All eight skill packages passed the Skill Creator metadata validator. A live Astra
  routing probe selected the expected skill for eight phase/lifecycle requests and
  selected none for two unrelated requests. This tested selection from the metadata
  catalog; it did not execute ten complete workflows or prove host-wide discovery.
- The approval helper recorded the existing human approval of the designated plan.
  The preparation helper recovered the actual task worktree, preserved its pending
  implementation edits, and verified its upstream. The PR binding helper associated
  the task with the existing draft PR.
- A disposable local Git test confirmed that an explicit stale SHA lease rejects
  remote branch deletion and preserves the advanced tip, while a matching lease
  permits deletion. No production branch was deleted for this test.
- A live local archival probe moved a directory between two different filesystem
  devices using the copy/verify path. File content and the symlink were preserved;
  the link's external target remained untouched. Both probe directories were temporary.
- The dedicated Astra session started in the issue worktree and subsequent authoring
  turns resumed the same saved UUID. It produced inspectable implementation patches;
  the coordinator applied them in that worktree and ran validation.
- The executor recovery command imported that actual saved UUID into the task record
  after the authoring process stopped; it retained the registered worktree identity.
- The live GitHub queries for required checks, review-thread pagination and merge-queue
  state succeeded against the draft PR. No merge was requested by these queries.

Head-specific hosted checks and independent reviews are retained on
[PR #6](https://github.com/Zi-Deng/agentic-github-template/pull/6). A review is evidence
only for its recorded head/base; later commits require renewed review. Local test
results above do not substitute for that independent model review or a human merge.

The [first Fable review](https://github.com/Zi-Deng/agentic-github-template/pull/6#pullrequestreview-5202984172)
identified a process-group ownership lookup race and disclosed incomplete inspection
of several helpers and tests. The original Astra session authored the repair. A
regression reproduced the failure using the reviewed head's `stop_process` function;
the repaired function passes it. Another regression confirms truncated JSON records
an incomplete failure and preserves the original UUID for resumption. The reviewer
executed no tests; those results come from coordinator-run local validation.

The [second Fable review](https://github.com/Zi-Deng/agentic-github-template/pull/6#pullrequestreview-5203073695)
also exhausted its session budget and did not confirm complete acceptance coverage.
Its missing-executor question led to a confirmed completion-gate fix: managed finish
requires a valid original UUID and an explicitly completed latest run for the current
contract. The rejection regression fails against the reviewed function and passes
after repair. A further regression demonstrates recovery through normal publication,
completion and renewed review after a merge queue rejects a task. The latest repair
requires fresh independent review; no third round was run without explicit continuation.

The user subsequently selected one attempted review round by default, with further
review for supported critical P0/P1 findings or an explicit request, and requested a
fresh Fable review now. The continuation regression confirms that the second attempt
needs both recorded authorization and a nonblank reason, preserves the historical
rounds, and does not grant later rounds automatically. The 98-test gate passes with
this policy. Per-review credit/time limits and executor permissions remain unchanged.

Local validation uses a separate environment built from system CPython 3.12.3 with
the repository's pinned Ruff 0.16.7 and PyYAML 6.0.3. The previously reused ML environment
failed to import `_ctypes` because of an ABI error; it was preserved rather than modified.

### Workstation execution limitation

The installed Codex CLI was **0.154.0**. Its `workspace-write` sandbox failed before
even `pwd` could execute, reporting `bwrap: loopback: Failed RTM_NEWADDR: Operation not
permitted`. Native patch execution also failed. The legacy sandbox backend rejected
the active permission profile. Permissions were not silently widened.

Consequently, live session continuity and model-authored patch handoff are distinct
from successful native worker command execution. The latter remains unverified on
this workstation until its sandbox is repaired or the maintainer explicitly chooses
an appropriate execution policy. The template keeps `workspace-write`; a failed or
blocked managed run remains incomplete. The manual development handoff used here is
not an automatic runtime fallback.

## Limits

No test suite proves the absence of all defects. Model findings need assessment,
credentials and account entitlements can expire, and new projects need real project
checks beyond the portable workflow tests. Keep this record current as live stages
complete and after substantive harness changes.

### Opus/400-credit amendment

After three budget-limited Fable reports, the maintainer explicitly selected
`claude-opus-5` and a 400-AI-credit allowance and requested renewed review. The model
pin, managed review guards, skill metadata and operating/adoption guidance were
updated together. The existing isolation regression also checks propagation of the
packet's frozen credit allowance into the CLI arguments. Current-head live evidence
is published on PR #6; the historical Fable reports above are not Opus verification.
The 900-second timeout, one-attempt default, continuation records and pending native
Astra execution limitation remain unchanged.

### Opus findings and repair evidence

The [Opus review](https://github.com/Zi-Deng/agentic-github-template/pull/6#pullrequestreview-5203667943)
ran with the requested model and 400-credit allowance. Its CLI usage record reports
`claude-opus-5`. It found no P0/P1 defect, but supported a same-head finish-recovery bug
and identified gaps in direct merge-gate coverage. The report remains a partial static
inspection, not human approval.

Repairs allow fresh assessment of a still-unmerged queued/declined task while preserving
ambiguous merge and existing archive/cleanup records. Direct tests exercise the actual
required-check and GraphQL parsers using only subprocess-output doubles. They cover
zero/nonzero CLI results, malformed responses, missing checks, extra required checks,
thread/queue state, changed head/base, duplicate threads and cursor cycles. The installer
rejects parent-path components before traversal. New queue/declined recovery and installer
regressions fail against the reviewed functions and pass after repair. Live read-only
calls through the revised gates also returned the expected GitHub state.

Two review claims require correction. The packet did contain the issue, designated plan,
three prior reviews and three check runs in `context.json`; they were not missing from
the supplied artifacts. GitHub CLI 2.100.0's JSON export path can return zero for failing
or pending checks, so the report's claim that the bucket-validation path was generally
unreachable is unsupported for the installed version. Both zero and nonzero outcomes
are tested. Existing empty/skipped-check tests also provided some earlier coverage.

No P0/P1 finding authorizes an automatic extra round under the selected policy. These
repairs therefore need explicit continuation for renewed independent review; local/CI
validation does not make a changed head reviewed. The enlarged bootstrap diff also
exceeds the unchanged 300 KB packet cap; any future review must explicitly resolve that
size limit, for example through a scoped larger packet allowance. The 400-credit setting
alone does not change the diff cap. Native managed executor completion remains blocked.
