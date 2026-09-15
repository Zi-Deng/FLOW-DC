# Human merge, archival and cleanup

The finish skill prepares the decision and command; the maintainer runs the command.
Merge preparation is not a merge approval. The script checks mechanical preconditions,
but the human still decides whether the reviewed change and domain evidence justify
merging.

## Before running the command

Invoke `$agentic-finish PR #456`. The coordinator reads the designated published
review, all material dispositions, current acceptance evidence and required checks.
It records an explicit assessment tied to the repository, PR, reviewed head/base and
review record. Local records cannot prove human approval or the semantic correctness
of a model's assessment.

Preparation must reject a stale or altered review, wrong repository, changed contract,
missing evidence assessment, unresolved material blockers, failed/missing required
checks or conflicting/unknown mergeability. It inventories artifacts and reports
tracked changes or unexpected non-ignored files. A qualifying PR can be marked ready,
and the coordinator supplies the exact `scripts/finish-task.sh` command with its
recorded inputs. Copy that command rather than obtaining a new head SHA and assuming
it was reviewed.

Marking ready uses [GitHub CLI's ready operation](https://cli.github.com/manual/gh_pr_ready).
It changes draft status; it does not submit an approval or merge the PR.

## Prepare the assessment

Collect the current evidence identifiers from the clean control checkout:

```bash
python3 scripts/agentic/workflow.py feedback 123 > /tmp/issue-123-feedback.json
```

Read the complete feedback and approved contract. Write an assessment JSON file using
this structure, replacing the example values with actual evidence:

```json
{
  "head_sha": "FULL_REVIEWED_HEAD_SHA",
  "base_sha": "FULL_REVIEWED_BASE_SHA",
  "review_id": 123456789,
  "feedback_digest": "DIGEST_FROM_FEEDBACK",
  "pr_digest": "DIGEST_FROM_FEEDBACK",
  "summary": "Assessment of all criteria and material findings.",
  "limitations": "Checks or claims outside the available evidence.",
  "domain_evidence": "Domain validation, or why no scientific claim is involved.",
  "acceptance": [
    {
      "criterion": "One measurable criterion from the approved contract",
      "supported": true,
      "evidence": ["https://github.com/OWNER/REPO/actions/runs/RUN"]
    }
  ],
  "records": [
    {
      "id": "review:123456789",
      "digest": "RECORD_DIGEST_FROM_FEEDBACK",
      "disposition": "no-action",
      "rationale": "Explain why this record supports no remaining material defect.",
      "evidence": ["https://github.com/OWNER/REPO/pull/PR#pullrequestreview-123456789"]
    }
  ]
}
```

Include every acceptance requirement and every published review/inline record returned
by `feedback`, including older heads. Dispositions are `fixed`, `rebutted`, `deferred`,
or justified `no-action`. If a review contains several findings, its rationale and linked
public response must address each material finding. PR conversation comments are also
read and included in the feedback digest; they have no separate review-record row.

A `deferred` record also needs `follow_up`, the URL of an open GitHub issue, and
`acceptance_source`, identifying the user's actual acceptance of that deferral. The
helper checks that the follow-up exists and is open. The coordinator remains responsible
for the appropriateness and authorization of deferral. Evidence arrays require public
HTTPS links. The helper validates structure and freshness; it cannot prove the truth
of linked evidence or the semantic completeness of an assessment.

Prepare and, when qualified, mark ready:

```bash
python3 scripts/agentic/workflow.py finish-prepare 123 \
  --assessment-file /tmp/issue-123-assessment.json --ready
```

The result includes PR/review links, exact head/base, inventory, reserved archive
destination, working directory, and the human command. Preparation does not move task
artifacts. Changed PR text or feedback requires reassessment; changed head/base also
requires a new independent review. Readiness requires the saved original executor UUID
and a latest recorded run that completed the current contract. A recovered UUID without
a completed run, an absent run history, or an incomplete phase cannot qualify. A
`checkpoint` result permits early draft publication, then requires continued
implementation in the same UUID.

## What the human-run script does

1. Recheck the PR and the designated review against current GitHub state.
2. Request a squash merge pinned with `--match-head-commit`, without administrative
   bypass or the combined `--delete-branch` option.
3. Re-query GitHub after success or error. A queued or otherwise unmerged PR retains
   its worktree and branches. A nonzero CLI exit does not prove the merge failed.
4. Once merge is confirmed, preserve ignored worktree artifacts in a private archive.
5. Re-run the existing identity, registration, exact-tip and cleanliness guards, remove
   the verified worktree and delete its matching local task branch.
6. Remove the remote task branch only if its current tip still matches the merged PR
   head. An already-deleted branch is harmless; an advanced branch is preserved.

`--match-head-commit` protects the head. It does not atomically freeze the base branch;
GitHub's current rules and required checks remain authoritative for merge eligibility.
A merge queue may defer the merge. See [GitHub CLI merge semantics](https://cli.github.com/manual/gh_pr_merge).

The remote deletion uses an explicit expected-SHA lease, so a background fetch cannot
silently change which tip is considered safe to delete. See [Git's lease semantics](https://git-scm.com/docs/git-push).

## How ignored artifacts are preserved

Automatic archival currently requires Linux and a filesystem supporting
`renameat2(RENAME_NOREPLACE)`. The implementation stops if that atomic operation is
unavailable; it does not substitute an overwrite-prone rename. Use a functioning
CPython 3.12+ interpreter with its standard `ctypes` module and a process C library
that exports `renameat2` to `ctypes.CDLL(None)`. Kernel syscall support alone is
insufficient for this implementation; an unavailable symbol stops archival. Other platforms can use
the skills and ordinary GitHub phases, but need a reviewed archival adapter before
relying on automatic finishing.

Ordinary `git status` can hide virtual environments, caches, private notes, checkpoints
and experiment outputs. The finishing script archives ignored entries under a unique
directory below the control checkout's `.agentic-local/archives`. It keeps their
relative paths and records a persistent operation journal. Archive records and contents
are private and have no automatic expiry.

Symlinks are preserved as links; their targets are not followed or copied. Same-filesystem
renames avoid duplicating large artifacts. Cross-filesystem transfers copy and verify
content before source removal. Unsupported file types, changed inputs, destination
conflicts or failed verification stop cleanup and retain recovery information.

Archival is for ignored artifacts. Tracked modifications and unexpected non-ignored
files still block cleanup. The script does not turn arbitrary user edits into disposable
outputs. It never uses blanket `git clean`, a forced worktree removal, or an unbounded
branch deletion.

An archived virtual environment preserves files but may contain absolute paths to its
original location; recreating the environment can be more appropriate than running it
from the archive. Keep experiment configuration, package locks and evidence manifests
with research artifacts so that archival supports reproducibility.

## Interrupted or partial completion

Read the reported state before retrying:

| State | Next action |
| --- | --- |
| PR unmerged or queued | Wait for or resolve GitHub requirements; rerun the supplied command when appropriate. No cleanup has occurred. |
| PR still open/unmerged, saved assessment stale after new feedback | Collect feedback and prepare a current assessment, then rerun `finish-prepare` and use its returned human command. Re-preparation may replace only a prepared, queued or unmerged record after current GitHub and workspace checks; it cannot reset an ambiguous merge request or an archive/cleanup already in progress. |
| PR merged, archival incomplete | Inspect the reported journal and retained source/destination entries. Rerun the same command to reconcile the interrupted archive; do not overwrite either side manually. |
| PR merged, worktree changed | Preserve and inspect the changes. Reconcile them explicitly before cleanup; the script does not discard new work. |
| Local cleanup complete, remote branch advanced | Preserve the advanced remote branch and investigate its new commits. Do not replace the expected SHA to force deletion. |
| Cleanup complete | Retain the final report and archive location. Any later archive deletion is a separate human action. |

Close the executor and stop task-specific writers before finishing. Local operation
locks coordinate these helpers; they cannot stop unrelated programs from modifying
files. Concurrent writes observed during inventory or verification stop the operation.

The low-level `cleanup-task` helper retains its original refusal to delete ignored
files. The human finishing script archives them first and calls that guarded cleanup
only after the task worktree meets its preconditions. The human owns any decision to
abandon an unmerged PR; abandonment is not successful finishing.
