# Independent Copilot review

The reviewer runs in a new Copilot CLI process and receives committed artifacts,
not the implementation conversation. Its model-facing tools are `view`, `grep` and
`glob`. The wrapper performs Git/GitHub operations outside that model process.

## Managed skill procedure

Use `$agentic-review PR #456` to coordinate the existing snapshot/run/publication
steps with a recorded task and approved plan. Use `$agentic-repair PR #456` afterwards
to retrieve both submitted reviews and inline comments and resume the exact Astra
implementation UUID. The standalone review skill completes review; the complete
workflow skill coordinates subsequent repair and re-review. See [SKILLS.md](SKILLS.md).

One attempted round per task is the default. A supported critical P0/P1 finding permits
a further round to verify its repair; the coordinator records the public finding and
concrete reason through the continuation flags. Other extra rounds need an explicit
user request. P2/P3 findings, uncertain questions and incomplete coverage do not by
themselves permit another round. Every additional invocation needs its own recorded
basis; see [the continuation procedure](SKILLS.md). A failed or incomplete run does not
establish readiness. Keep the original report intact and publish dispositions separately.

## Local procedure

From the clean main checkout on the default branch:

```bash
python3 scripts/agentic/review.py prepare 456 --issue 123 --plan-comment 987654321
```

The command prints a private directory under `.agentic-local/reviews/`. Use that exact
path in the next commands:

```bash
python3 scripts/agentic/review.py run /absolute/path/printed/by/prepare
# Read review.md and confirm its evidence before publication.
python3 scripts/agentic/review.py publish /absolute/path/printed/by/prepare
```

`prepare` checks the plan's issue association and the current PR head/base. It fetches
the PR and base refs, computes the merge base, collects the complete textual diff,
issue and PR comments, inline findings, reviews, status contexts and check runs.
Pagination covers all timeline/review/check pages. Source files are read as Git blobs.
No PR checkout, Git filter, project hook or project command is executed.

The snapshot contains an index mapping original paths to numbered `.txt` files, so
repository agent settings, hooks and skills cannot become active configuration.
Git symlinks and submodules are listed as omissions and never followed. Known private
and data directories are excluded; a diff touching such paths is refused before it is
transmitted. This path policy is a baseline, not a content-based secret detector.
Inspect your own source and augment exclusions for a project's restricted paths.

The review diff has **no byte cap by default**: `max_diff_bytes` is `null`, and
omitting that key also means unlimited. To opt into a cap, set a positive integer
number of UTF-8 bytes, for example `"max_diff_bytes": 500000`. Boolean, string, zero
and negative values are invalid. An empty diff is still rejected. A diff exceeding
an enabled cap fails before packet creation; it is never silently truncated.

Other limits remain: 250000 bytes per source file, 12000000 bytes of source snapshot,
900 seconds and 400 Copilot AI credits. Source-file omissions are indexed; exceeding
the total source budget fails. These are operational choices, not claims about model
capacity or complete coverage. Credit controls can overshoot by an in-flight request.
A large diff can still exhaust context, time or credits, and an incomplete review
consumes the task's attempted round. Report coverage honestly.

The managed executor has a separate `managed_max_prompt_bytes` limit, defaulting to
300000. It must be a positive integer and does not inherit the review diff setting.
Changing one limit never silently changes the other.

`run` copies the packet to a fresh workspace outside the repository, then uses fresh Copilot state, disabled hooks, no built-in MCP server, no inherited
provider override and no permission to execute, edit or delegate. Prompt-mode memory
and resume are not enabled. Repository policy and domain rubric come from the trusted
main checkout and are explicitly supplied as text. The requested model is recorded;
the template does not claim it independently attests to the provider's internal model
identity. Inspect `usage.json` and provider session metadata when that matters.
The tool controls follow the [Copilot CLI reference](https://docs.github.com/en/copilot/reference/copilot-cli-reference/cli-command-reference)
and [custom agent configuration](https://docs.github.com/en/copilot/reference/custom-agents-configuration).

`publish` rechecks head and base, verifies snapshot and report hashes, and creates a
COMMENT review with an explicit `commit_id`. Repeated publication of the same report
returns its existing review URL. Any new head/base requires a new snapshot. Findings
do not automatically become approval, a green required check, or resolved threads.

The review directory is private working state, not a cryptographic attestation against
its own owner. Its hashes catch accidental edits. A user able to rewrite the manifest
can rewrite the record, so GitHub permissions and human assessment remain necessary.

## Recover a saved review without another model request

After a successful model response and packet/head/base checks, the helper atomically
saves `review-result.json` outside the packet. This private journal binds the exact
UTF-8 report, its digest and the observed CLI version to the input metadata. It then
atomically writes `review.md` and the final metadata. The CLI version is checked
before the paid request, so a later version-probe failure cannot strand its output.

If either final-file write fails after the journal is saved, retry `run` using the
same directory. The helper verifies the journal, packet and current head/base and
finishes storage without invoking Copilot. The managed `task-review --run --publish`
path performs the same recovery and retains one attempted round. A completed,
unaltered journaled report can also be reused without another request. Changed
completed reports, changed journals and stale snapshots are rejected.

This recovery applies only when a valid journal was durably saved. A failure before
that point remains incomplete; do not delete records or assume a repeated command
is free to rerun the model. An older incomplete packet without a journal has no new
recovery guarantee. Storage failure, budget exhaustion and snapshot staleness can
still prevent completion and must be reported honestly.

All review text and subprocess text I/O use explicit UTF-8, including when Python's
UTF-8 mode and locale coercion are disabled. The prompt asks for a report below
50000 UTF-8 bytes; publication retains the existing 60000-byte hard limit. Oversized
reports are preserved for inspection and refused without silent truncation or an
automatic model retry. A provider can exceed a requested output length, so the prompt
limit is guidance rather than a guarantee.

## Why the snapshot differs from the PDF's detached checkout

A detached worktree reduces branch mistakes but is not a read-only security boundary:
an agent with shell access can still edit files, push a SHA or inspect shared credentials.
This implementation strengthens the review boundary by exporting committed text and
limiting tools. It also prevents a PR from installing its own reviewer hooks.

For independent execution tests, a human can create a detached worktree in a suitable
environment and run inspected commands without giving the model execution permission:

```bash
git fetch origin refs/pull/456/head
git worktree add --detach ../PROJECT-worktrees/review-pr-456 FULL_REVIEWED_HEAD_SHA
```

Confirm the fetched SHA matches the record. Test environments write caches and may
execute arbitrary project code; use disposable infrastructure for unfamiliar code.
Attach command and commit evidence to the PR. The static reviewer must never describe
those tests as having executed them itself.

## Report contract

| Severity | Meaning |
| --- | --- |
| P0 | Catastrophic, concrete merge blocker |
| P1 | Likely major correctness or security failure in supported use |
| P2 | Reachable edge-case defect or substantive evidence gap |
| P3 | Minor optional improvement; suppress tooling-covered style remarks |

Every finding needs **location, claim, trigger, impact, evidence and minimal fix
direction**. Use original repository paths and line numbers from the mapped source,
not the numbered snapshot filenames. Demonstrate the reachable code path or provide
a reproducible test proposal. Do not invent executed commands.

End with:

1. Acceptance criteria verified or still unsupported.
2. Validation independently executed, if any; the static CLI reviewer executes none.
3. Limitations, omitted source, inaccessible artifacts and residual risks.

Explicitly state when no material finding is supported. Treat uncertain concerns as
questions, not proven defects. Check negative inputs, missing data, ties, NaNs,
interruption, compatibility, concurrency and numerical boundaries where relevant.
For scientific changes apply [the domain rubric](domain-review.md).

## Manual Actions procedure

Complete the protected-environment setup in [SETUP.md](SETUP.md#6-hosted-review-is-opt-in).
Then use **Actions → manual Copilot review → Run workflow**, on the default branch.
Select the PR, issue, plan comment ID and exact head. Choose publication only when
you intend to post the generated report; otherwise download and inspect the artifact.

```bash
gh workflow run copilot-review.yml --ref main \
  -f pr=456 -f issue=123 -f plan_comment=987654321 \
  -f head_sha=FULL_CURRENT_PR_HEAD_SHA -F publish=false
```

Generation and publication are separate jobs. The model job has only read permissions;
the publication job has PR write permission and does not run a model. Neither checks
out or executes PR code. Same-repository PRs only are supported. Forks and non-default
base branches need a separate reviewed isolation procedure, not a bypass flag.

The workflow is deliberately opt-in and manual. It has no issue-comment trigger,
automatic repair loop, model token on ordinary PR checks, or self-hosted runner.
Changing the workflow or its credentials is T4 work.

## Model selection and repair

The configuration requests exactly `claude-opus-5`. Other Opus versions and Claude
families are separate choices and are not automatic substitutes. Choose another explicit
Claude ID only after checking account availability and deciding the cost is
justified. Do not use `auto`, a built-in agent that silently
selects another family, or the implementation conversation as a review session.
Native GitHub Copilot code review is a separate service and does not let you pin this
Claude choice; see [GitHub's code review description](https://docs.github.com/en/copilot/concepts/agents/code-review).

Local preparation reads configuration from the caller's clean main checkout;
Actions reads it from the trusted default branch. A model-change PR takes effect
for the standard review procedure after the maintainer merges it and the local
main checkout is updated. The normal procedure reviews the introducing PR under the existing trusted configuration.
An explicit maintainer instruction to use the new model/budget for that PR permits
a narrowly scoped override in the trusted invocation. Record the authorization and
exact values in fresh packet metadata; do not activate unreviewed PR instructions or
rewrite existing packets. This is how the Opus/400-credit transition was authorized. Changing an interactive Copilot model preference
does not override this wrapper's explicit `--model` argument.

Prepare a fresh packet after a model change. Each packet records its requested
model and budgets at preparation; changing `.agentic/config.json` afterwards does
not retarget that packet. Check `metadata.json`, `review.md`, and CLI-reported
`usage.json` for the actual run. Configuration and mocked tests do not prove live
account access. Historical Sonnet pilot results in the [upstream verification record](https://github.com/Zi-Deng/agentic-github-template/blob/b4a1df739a15b20b26635602e6b6a21fb08e0ac5/docs/agent-workflow/VERIFICATION.md) remain
evidence for those earlier runs, not evidence of Opus inference. Historical Fable
reports are likewise retained under their original model and budgets.

Managed repair remains an Astra task on the original branch **and original session
UUID**. A missing UUID must be recovered rather than replaced or selected with
`--last`. The legacy interactive launcher remains a manual alternative that starts
a separate session. Post a finding-by-finding
response with commits and evidence. One attempted round remains the default; additional
review follows the critical-finding or explicit-continuation policy above. A larger
per-review credit allowance does not authorize more rounds or establish completeness.

### Claude Opus 5 access

Checked on 2026-09-14. GitHub lists Claude Opus 5 as generally available and supports
it in Copilot CLI for eligible plans. Business and Enterprise administrators may need
to enable its model policy. Check the actual authenticated account rather than infer
availability from a template setting. See [GitHub's Opus 5 announcement](https://github.blog/changelog/2026-07-24-claude-opus-5-is-now-available-in-github-copilot/)
and [current supported models](https://docs.github.com/en/copilot/reference/ai-models/supported-models).

The 400-credit setting is a provider-enforced soft allowance for this single-prompt
review invocation, not a price quote or guarantee of complete inspection. Actual usage
may overshoot at a request boundary. Preserve `usage.json` and report any incomplete
coverage. See [GitHub's credit-limit reference](https://docs.github.com/en/copilot/reference/copilot-cli-reference/cli-command-reference#command-line-options).

Private-path exclusions and static tool restrictions remain in force. The snapshot's
ordinary source and public comments are sent to Copilot for the authorized review.
