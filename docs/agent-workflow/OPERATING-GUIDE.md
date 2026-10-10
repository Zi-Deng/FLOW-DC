# Streamlined FLOW-DC workflow

Effective October9,2026 under the maintainer's explicit direction; supersedes the old additive-only generation and qualification procedures.

Work in an issue worktree. Use a concise current issue/plan; prior authorization persists. Implement directly with the working agent. Delegate or start a fresh executor only when useful; there is no mandatory coordinator/executor session or approval-generation chain.

Maintain one current implementation. Replace obsolete modules and tests. Git history retains previous source; private reports remain reference records. Old reports cannot establish current review, and old contracts are not mandatory inputs for every new review.

For routine changes, run affected tests. For substantial changes, run `make check` once, then current CI. Repeat only affected checks when a defect/change justifies it. Do not run serial/parallel/installed full-suite matrices or hash every source file before commits. Software checks and scientific experiments have separate purposes.

Publish a draft PR with a standalone `Fixes #N`, concrete resulting behavior and actual validation. Use one fresh bounded independent static review for substantive behavior, credentials, dependencies or workflow changes. Address supported material findings; a changed commit requires explicit delta assessment before human merge within the bounded review policy below. Never start an automatic paid retry loop. A failed provider call stops review; continue useful independent work.

Commands:

```bash
python3 scripts/agentic/workflow.py new-task 31 short-slug
python3 scripts/agentic/workflow.py draft-pr --title 'Concrete change' --body-file /tmp/pr.md
python3 scripts/agentic/workflow.py review 32 --issue 31 --plan-comment CURRENT_COMMENT --publish
python3 scripts/agentic/finish.py 32 --review-directory /absolute/private/review
```

The final command prints a merge command for the maintainer; agents never execute it. Preserve ignored artifacts before human worktree cleanup. Existing server branch protection remains authoritative.

Workflow budgets: one working day for this cleanup, <=2,500 runtime lines and <=1,200 workflow-test lines. Workflow suite deadline120seconds and hosted job5minutes. One reviewer process<=900seconds, Claude estimated reference cap$10 or Copilot400credits. These are finite project limits, not cost guarantees. Do not grow a framework or increase limits merely to obtain a pass. There is no mandatory paid capability/coverage campaign.

Read [review procedure](REVIEW.md) and [provider setup](PROVIDERS.md) only when needed. Scientific accounting, operational grants and manuscript originals remain protected.

## Bounded review and repair policy

Use at most two automated review invocations and two review-driven repair rounds per task across providers; failed or interrupted invocations count. Plan the first review after implementation qualification and the final review after the remaining material changes. Ordinary development/test fixes and coauthor revisions are not extra review-driven repairs. After the last repair, report the last reviewed SHA, final SHA, changed delta and current-head CI for human assessment; never label an earlier review as final-head review. No automatic third review. Shelve remaining nonblocking findings in a concise existing or consolidated GitHub issue. Material acceptance failures remain blockers. Extra cycles require a documented credential compromise, data loss, uncontrolled spending, or defect invalidating required central evidence, and a bounded corrective scope. Existing finite provider limits and human-only merge/submission still apply.
