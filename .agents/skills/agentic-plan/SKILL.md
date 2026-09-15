---
name: agentic-plan
description: "Inspect an existing GitHub issue and repository, then publish an implementation plan as an issue comment. Use for planning before coding or revising a task plan; implementation requires the user's approval of the designated plan."
---

# Plan GitHub Issue

Produce the public implementation contract for an existing issue. Read [the skill operating contract](../../../docs/agent-workflow/SKILLS.md), [the operating guide](../../../docs/agent-workflow/OPERATING-GUIDE.md), and [the plan role prompt](../../../.agentic/prompts/plan.md).

Read the issue, existing plan comments and relevant code, tests and configuration. Map every acceptance criterion to the implementation boundary, affected interfaces, failure cases and exact evidence. Explain compatibility, domain validation, costs, migrations, exclusions and rollback where relevant. A plan that merely restates the issue is incomplete.

Settle decisions that materially affect correctness, scope or cost before dependent implementation. Publish the proposed plan using a body file and the task helper. Return its numeric comment ID and URL. Identify it as proposed until the user has approved it.

Request approval of this concrete plan before coding. Reuse explicit approval of the same concrete plan already given by the user; publication on GitHub is not a reason to ask again. Record that approval against the issue and plan content digests using the documented approval operation. That local record is an operator receipt, not a human GitHub review or permission inferred from public text. A changed issue or designated plan must be reconciled before further implementation.

Return the approved/proposed status, comment URL, criterion-to-evidence mapping and unresolved decisions. Stop after planning unless coordinating the complete workflow.
