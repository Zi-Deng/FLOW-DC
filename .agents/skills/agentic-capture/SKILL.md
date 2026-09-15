---
name: agentic-capture
description: "Draft and publish a GitHub issue for an agentic workflow task. Use when the user wants a new issue or task contract; respect a request to draft without publication. Does not implement code or plan an existing issue."
---

# Capture GitHub Issue

Turn the requested change into a measurable issue and publish it when requested. Read [the skill operating contract](../../../docs/agent-workflow/SKILLS.md) and the capture section of [the operating guide](../../../docs/agent-workflow/OPERATING-GUIDE.md).

Inspect the repository and relevant behavior before writing acceptance criteria. Check existing issues and local task records to avoid creating another issue for the same task. Ground bug claims in source, logs or a reproduction; label uncertainty instead of inventing evidence.

Draft the observable problem, expected behavior, scope, exclusions, measurable acceptance criteria, validation commands, domain impact, risks, budget and rollback. Use the existing issue template and [draft role prompt](../../../.agentic/prompts/draft.md). Choose the worst plausible risk tier. Keep implementation design for the plan phase.

Resolve missing information that changes acceptance or scope. A request to create/publish an issue authorizes that publication; a draft-only request ends with the body for inspection. Use the documented publication helper with a stable operation key and a body file. Retain the key/returned ID; reconcile an uncertain response before retrying. Do not interpolate issue text into shell commands.

Return the issue URL/number, the acceptance contract and remaining uncertainty. Stop after capture unless the user requested the complete workflow. Suggest the plan phase as the next action.
