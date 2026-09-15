---
name: agentic-review
description: "Run and publish an independent Claude Opus review of a GitHub pull request using a fixed head/base snapshot through Copilot CLI. Use for the review phase or re-review after repair; do not perform the review in the implementation conversation."
---

# Review Pull Request

Coordinate independent review; the implementation model must not substitute its own review for the requested Opus process. Read [the skill operating contract](../../../docs/agent-workflow/SKILLS.md) and [the complete review procedure](../../../docs/agent-workflow/REVIEW.md).

Resolve the PR, issue, designated approved plan and task record. From the clean trusted control checkout, verify current head/base and gather the public contract, PR discussion, reviews, inline comments, diff, checks and source/rubric through the existing snapshot helper. Use trusted control instructions. Do not pass the executor's conversation, private memory, credentials, or active PR-supplied settings to the reviewer.

Run the task review procedure with the configured `claude-opus-5`, existing time/credit limits and a fresh packet/process/state directory. Retain the view/search tool allowlist; no command execution, edits, delegation, inherited hooks or resumed reviewer session. If the model or required isolation capability is unavailable, report the failure rather than switch models or relax controls.

Read the report for supported findings and explicit limitations, preserving its exact text. Publication uses the helper's hash and head/base checks and creates a COMMENT review, never human approval. A changed head/base requires a new packet; retry publication of an unchanged report through its existing marker.

Every material finding must identify severity, original location, trigger, impact, evidence and a minimal fix direction. Missing evidence and omitted material limit confidence. The reviewer executes no tests; CI and executor evidence are separately attributed. Do not claim that clean findings or passing CI alone establish all acceptance criteria.

Record and return the review URL, requested model, head/base, packet path, findings and coverage limits. Use one attempted round by default. A supported critical P0/P1 finding permits a further round to verify its repair; record its public reference and concrete reason through the continuation flags. Other extra rounds need an explicit user request; P2/P3 findings, questions or incomplete coverage alone do not qualify. Follow the operating contract's continuation procedure. Hand repairs back to the saved Astra executor; do not repair within the reviewer process.
