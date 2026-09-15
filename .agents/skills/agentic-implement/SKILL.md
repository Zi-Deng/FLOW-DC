---
name: agentic-implement
description: "Implement an approved GitHub issue in its registered sibling worktree, run required checks, and publish or update its draft PR through the coordinator. Use for the implementation phase, not for independent review or merging."
---

# Implement Approved Issue

Implement the approved contract with the dedicated Astra executor. Read [the skill operating contract](../../../docs/agent-workflow/SKILLS.md), [the operating guide](../../../docs/agent-workflow/OPERATING-GUIDE.md), and [the implementer prompt](../../../.agentic/prompts/implement.md).

## Coordinator

Verify approval, issue/worktree/branch association and current task state. Use the managed launcher with the configured Astra model. Persist and retain the returned executor UUID. If an implementation session already exists, continue that exact session according to the helper; never select `--last` or silently replace it. Do not broaden permissions after a denied operation.

Inspect the executor's status and actual Git state. Blocked, failed or interrupted output is not completion. At the first coherent committed checkpoint, publish a draft PR using the coordinator's helper. Include a standalone `Fixes #N`, the approved plan URL, scope, criterion-to-evidence mapping, commands and exit statuses, omissions, domain evidence and rollback. Update the same PR and push subsequent commits. Watch the required checks and keep the body current before independent review.

## Already-running executor

If the launch prompt identifies this session as the managed executor, perform the implementation directly; do not invoke another executor or the complete-workflow coordinator. Confirm the task directory, branch and contract. Reproduce a bug before fixing it when applicable, implement the smallest adequate change, run focused and required checks, inspect the diff, and prepare coherent commits and publication text.

Follow the managed-role contract for results and handoff. GitHub publication belongs to the coordinator. Preserve unrelated edits, tests and acceptance criteria; expose unresolved questions and unavailable evidence. Do not merge.

Return the PR URL, executor/task identity, current head and validation evidence. The next phase is fresh independent review, not the executor reviewing its own implementation.
