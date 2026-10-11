---
name: agentic-finish
description: "Assess current review/checks and prepare a human merge command."
---

# agentic-finish

Inspect findings/dispositions and scientific evidence where relevant. Use finish.py with the completed published current-head review. Return the exact command for the maintainer. Never execute a merge or destructive cleanup. Preserve ignored artifacts before human worktree removal.

Follow [the streamlined guide](../../../docs/agent-workflow/OPERATING-GUIDE.md). User instructions and existing authorization take precedence. Keep credentials/private material separate and scientific validity distinct from software checks.

Follow the task ceiling of two automated review invocations and two review-driven repair rounds, including failed calls. After the final repair, disclose the reviewed/final SHAs, delta and current CI for human assessment. Shelve only nonblocking findings; use a documented bounded exception only for credential compromise, data loss, uncontrolled spending or invalid central evidence. Do not require a third ordinary review.
