---
name: agentic-finish
description: "Assess a reviewed GitHub PR and prepare the human-run merge-and-cleanup command with automatic archival of ignored artifacts. Use for finish or merge readiness; the skill never executes a real merge or deletes task branches."
---

# Prepare Human Merge

Prepare a PR for the maintainer's merge decision. Read [the skill operating contract](../../../docs/agent-workflow/SKILLS.md), [review guidance](../../../docs/agent-workflow/REVIEW.md), and [the finishing procedure](../../../docs/agent-workflow/FINISH.md).

Read the designated published review and every material disposition. Verify current issue/plan acceptance and domain evidence separately from CI. Record an explicit assessment of supported acceptance, findings, remaining limitations and the exact head/base/review. Local assessment and model output are not human approval or cryptographic attestation. If evidence is missing or a material blocker remains, report it and return to the appropriate phase.

Require the saved original executor UUID and a latest recorded run that completed the current contract. A recovered UUID alone or an empty run history does not establish completion. Do not manufacture a completion record to qualify a bootstrap or legacy task.

Use finish preparation to validate the designated pipeline review, current head/base, expected required checks and mergeability. Confirm that thread resolution requirements are satisfied. Mark the PR ready only after it qualifies; do not equate draft removal with a merge decision. Inventory cleanup blockers and ignored artifacts without moving them during preparation.

Return the PR URL, reviewed head/base, review and evidence links, archive destination/inventory, and the exact `scripts/finish-task.sh` invocation for the human. Explain any queued/merged-but-not-cleaned state and the precise retry action. Never execute that script or `gh pr merge` yourself, including in an attempt to validate it against a real PR.

The human-run script must verify remote merge state before archival/cleanup, preserve ignored files in its journaled archive, then reapply worktree/tip guards and delete only matching task branches. Tracked edits, unexpected non-ignored files, unsupported objects, changed tips and failed archival remain blockers. Archives have no automatic expiry. Validate destructive behavior only in disposable local Git fixtures with mocked GitHub.
