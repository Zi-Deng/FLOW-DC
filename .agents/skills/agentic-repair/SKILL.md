---
name: agentic-repair
description: "Assess review findings and repair supported defects."
---

# agentic-repair

Read relevant review/inline comments. Fix concrete supported material defects in the task worktree; explain rebuttals or accepted follow-ups. Run affected checks once and update the PR. Re-review the relevant changed behavior before human merge. Same-session executor continuity is optional, and prior authorization persists.

Follow [the streamlined guide](../../../docs/agent-workflow/OPERATING-GUIDE.md). User instructions and existing authorization take precedence. Keep credentials/private material separate and scientific validity distinct from software checks.

Follow the task ceiling of two automated review invocations and two review-driven repair rounds, including failed calls. After the final repair, disclose the reviewed/final SHAs, delta and current CI for human assessment. Shelve only nonblocking findings; use a documented bounded exception only for credential compromise, data loss, uncontrolled spending or invalid central evidence. Do not require a third ordinary review.
