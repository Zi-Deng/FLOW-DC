---
name: agentic-implement
description: "Implement authorized changes in the assigned issue worktree."
---

# agentic-implement

Implement directly with the working agent. Use focused regression checks; run make check once for substantial changes. Commit and publish/update the draft PR with actual behavior, checks and omissions. A separate executor is optional; do not launch recursively. Replace obsolete code/tests rather than append compatibility versions.

Follow [the streamlined guide](../../../docs/agent-workflow/OPERATING-GUIDE.md). User instructions and existing authorization take precedence. Keep credentials/private material separate and scientific validity distinct from software checks.

Follow the task ceiling of two automated review invocations and two review-driven repair rounds, including failed calls. After the final repair, disclose the reviewed/final SHAs, delta and current CI for human assessment. Shelve only nonblocking findings; use a documented bounded exception only for credential compromise, data loss, uncontrolled spending or invalid central evidence. Do not require a third ordinary review.
