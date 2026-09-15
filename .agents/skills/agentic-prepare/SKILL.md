---
name: agentic-prepare
description: "Prepare the approved GitHub issue for implementation by creating or recovering its branch and sibling Git worktree. Use for issue workspace preparation or handoff, not for cloning an unrelated project or changing the main branch."
---

# Prepare Issue Worktree

Establish the implementation workspace from the approved issue. Read [the skill operating contract](../../../docs/agent-workflow/SKILLS.md) and the worktree section of [the operating guide](../../../docs/agent-workflow/OPERATING-GUIDE.md).

Resolve the task record, issue and approved plan; verify that their repository and current content match. Run control operations from the registered clean control checkout on its discovered default branch. Preserve dirty or unrelated files. Do not stash, reset, move or ignore user work simply to satisfy a clean-checkout requirement. Use a separate clean control checkout when necessary and report its path.

Use the task preparation helper to create or recover `issue-N-slug` beneath the configured sibling worktree root and establish remote tracking. Reuse only a worktree/branch that passes the helper's identity, location and association checks. Do not replace the helper with a forced checkout or blanket cleanup.

Confirm the repository, branch, base, registered path and upstream. Worktrees share Git objects/configuration; they are not credential, GPU or service isolation. Preserve project-specific environment and resource requirements.

Return the worktree path and exact managed implementation launch command. The coordinator remains in the control checkout and the dedicated executor starts with the task worktree as its working directory. A shell `cd` in one tool call does not relocate the current application conversation. Start implementation only when that phase is requested or the complete workflow is active.
