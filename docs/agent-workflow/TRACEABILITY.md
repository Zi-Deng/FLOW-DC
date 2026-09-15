# Guide-to-FLOW-DC traceability

This mapping connects the supplied guides, the pinned template and the approved FLOW-DC adoption. Exact sources and hashes are in [RESEARCH.md](RESEARCH.md). Commands and safeguards are verified against code; historical PDF examples are not copied without checking their assumptions.

| Requirement | Implementation | Validation / boundary |
| --- | --- | --- |
| Issue as the task contract | Issue form, capture skill, `tasks.py` durable operation keys | Idempotent issue/plan recovery and issue association tests |
| Reviewed plan before implementation | Plan skill, `approve-plan`, contract digests | Existing interactive approval counts; an agent comment cannot authorize itself |
| One issue branch and isolated worktree | `new-task`, prepare skill, sibling paths and repository lock | Real temporary Git fixtures; rejects dirty controls, invalid paths and ambiguous branches |
| Dedicated OpenAI implementation and repair | `sessions.py`, implement/repair skills, `gpt-6-astra` | Exact UUID persisted from native events; no latest-session shortcut; incomplete runs remain incomplete |
| Independent reviewer | `review.py`, static numbered source snapshot, fresh Copilot `claude-opus-5` | Read-only tool allowlist, disabled hooks/MCP/custom instructions; reviewer never executes tests |
| Complete review context | Committed diff, issue/plan, paginated comments/reviews/checks | Source omissions indexed; private paths refused; no project hooks or textconv executed |
| Exact reviewed code | Head/base association and packet/report hashes | Stale head/base and altered packet/report are rejected before run or publication |
| Honest model feedback | COMMENT review tied to commit; feedback/disposition pipeline | Model output is not a human approval or a CI check |
| FLOW-DC validation | `flowdc-tests`, `requirements-test.txt`, existing tests | Eight local HTTP/config regressions; TaskVine stub is not live cluster proof |
| Workflow validation | `agentic-quality`, `make check-agentic` | Runtime tests, Ruff, all eight skills, result schema, CI contexts and maintained links |
| Domain evidence | FLOW-DC [rubric](domain-review.md), `evidence.py` | Distinguishes software behavior, measurement fairness and scientific inference |
| Cost/context controls | `.agentic/config.json`, managed review attempts | One attempt, 400 credits, 900 seconds; explicit continuation rules |
| Unlimited review diff | `max_diff_bytes: null`, separate `managed_max_prompt_bytes` | Tests unlimited and finite UTF-8 boundaries, invalid values and independent prompt cap |
| Human merge | Finish skill, `finish-prepare`, `finish-task.sh` | Agents prepare only; current completed executor and acceptance assessment required |
| Preserve ignored artifacts | `archives.py`, journaled archival before guarded cleanup | Linux no-replace rename, cross-filesystem verification, interrupted-operation tests |
| Persistent memory | Ignored `/memory/` and `/.agentic-local/` | Full original prompt stored privately; no tracked memory or review inclusion |
| GitHub enforcement | Main ruleset, two actual Actions check contexts | Read back settings; no invented required reviewer or CODEOWNERS identity |
| Reusable skills | Eight `.agents/skills/*/SKILL.md` packages and UI metadata | Package names, descriptions, invocation prompts and local links validated |

## Deliberate differences from historical examples

1. Review uses exported committed text in a separate restricted workspace. A detached Git worktree alone is not a read-only model boundary.
2. Managed execution resumes the explicit original session UUID. Interactive `launch` commands are low-level alternatives and do not supply managed continuity by themselves.
3. The reviewer is Opus 5 with 400 AI credits. Earlier Sonnet/Fable names and budgets in upstream history are not FLOW-DC's current policy.
4. Review diffs have no default byte cap; managed prompts and source snapshots retain separate limits. Unlimited input does not justify a complete-coverage claim.
5. The original FLOW-DC application notes, benchmark support code, generated-artifact policy and immutable local archives are preserved.
6. There are two required CI jobs. The template's synthetic `quality` name remains only in generic test fixtures; production configuration uses the actual FLOW-DC contexts.
7. Hosted review is installed but disabled. Account/environment setup and a successful hosted request are separate future work.
8. Bootstrap has a separate baseline PR and trusted local control clone. It cannot use a manufactured executor completion to pass the managed finish gate.

## Updating from the template

Stage a specific future template revision and compare its payload against `.agentic/template-origin.json`. Review local deltas in configuration, runtime budget handling, tests, CI, `AGENTS.md`, setup and domain documentation before replacing any file. Preserve all eight skill packages and their shared operating contract. Run the focused checks and a representative live task after changes to model invocation, permissions, session persistence, review integrity or finish behavior.
