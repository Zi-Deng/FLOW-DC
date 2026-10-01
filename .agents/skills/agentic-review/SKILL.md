---
name: agentic-review
description: "Run and publish an independent static review of a GitHub pull request using a fixed head/base snapshot through the explicitly selected provider. Use for the review phase or re-review after repair; do not perform the review in the implementation conversation."
---

# Review Pull Request

Coordinate independent review; the implementation model must not substitute its own review for the selected provider/model process. Read [the skill operating contract](../../../docs/agent-workflow/SKILLS.md) and [the complete review procedure](../../../docs/agent-workflow/REVIEW.md).

Resolve the PR, issue, designated approved plan and task record. From the clean trusted control checkout, verify current head/base and gather the public contract, PR discussion, reviews, inline comments, diff, checks and source/rubric through the existing snapshot helper. Use trusted control instructions. Do not pass the executor's conversation, private memory, credentials, or active PR-supplied settings to the reviewer.

Inspect `workflow.py review-selection` and its provenance/activation blockers before execution. Defaults are Claude Code `claude-opus-5-5` / `medium`; explicit Copilot `claude-opus-5` / `default` remains supported. Local preparation and managed `task-review` accept `--review-provider`, `--review-model`, `--review-effort`; precedence is per-call, saved selection, trusted default. Selecting a provider without a model selects its own default. Read [provider setup and limits](../../../docs/agent-workflow/PROVIDERS.md).

Use a fresh packet/process/state directory bound to the exact provider policy. Claude uses native `Read`, `Grep`, `Glob`, 900 seconds/$10 estimated reference cost and zero extra spending; Copilot uses `view`, `grep`, `glob`, 900 seconds/400 AI credits. No command execution, edits, delegation, inherited hooks, fallback, new wrapper inference retry or resumed session. Claude requires a guarded dedicated native Max login, an access-only snapshot without refresh material, sufficient remaining lifetime, a current account-bound disabled-paid-usage receipt, immutable registration/generation bindings, verifiable isolation and actual native capability evidence. Native login callback isolation currently blocks setup. The specifically disclosed same-credential native 401 handling within one captured invocation does not authorize a new call or extra diagnostic slot; see PROVIDERS.md. An unavailable model, quota, credential or isolation control stops execution. Never switch provider or relax controls automatically.

Read the report for supported findings and explicit limitations, preserving its exact text. Publication uses the helper's hash and head/base checks and creates a COMMENT review, never human approval. A changed head/base requires a new packet; retry publication of an unchanged report through its existing marker.

Every material finding must identify severity, original location, trigger, impact, evidence and a minimal fix direction. Missing evidence and omitted material limit confidence. The reviewer executes no tests; CI and executor evidence are separately attributed. Do not claim that clean findings or passing CI alone establish all acceptance criteria.

Record and return the review URL, provider/exact model/effort, billing mode, typed budget/usage, head/base, packet path, findings and coverage limits. Use one attempted round by default. A supported critical P0/P1 finding permits a further round to verify its repair; record its public reference and concrete reason through the continuation flags. Other extra rounds need an explicit user request; P2/P3 findings, questions or incomplete coverage alone do not qualify. Follow the operating contract's continuation procedure. Hand repairs back to the saved Astra executor; do not repair within the reviewer process.

Use the required-material checklist and same-request actual provider read/search/list probe. Require successful returned-line evidence, not model assertions. Use --prior-review only with a validated same-PR ancestor packet; retain uncovered material and findings. Partial reports may be published as incomplete but never designated ready. Preserve exact output and sanitized failure diagnostics; never publish raw sessions or silently rerun paid calls.

Read [the coverage contract and migration runbook](../../../docs/agent-workflow/COVERAGE.md).
