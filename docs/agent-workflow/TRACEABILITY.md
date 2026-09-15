# Guide-to-implementation traceability

Both supplied files were read completely: all 33 physical PDF pages (cover plus
printed pages 1–32) and all 1,887 TeX lines, including examples, appendices and
references. The originals are preserved. This table maps the complete guide into
the template and identifies deliberate adaptations.

| Guide section | Implemented artifact / procedure | Verification or qualification |
| --- | --- | --- |
| 1. Overview and minimum checklist | README diagram; operating guide; helper commands | Local tests cover mechanical transitions; live GitHub stages are recorded separately |
| 2. Issue contract, PR state and small changes | Required issue form; evidence-based PR template; early draft helper | Human reviews acceptance and scope; no claim of automatic semantic enforcement |
| 2.3. Worktree isolation | `new_task`, POSIX operation lock, sibling paths | Real Git tests with spaces, reused branches, duplicate paths and non-main default branch |
| 3. Provider-neutral interface | Python/Git/gh core plus separate Codex and Copilot adapters | Basic GitHub flow remains usable without models |
| 3.3. Shared instructions | Root AGENTS.md, short Copilot instructions and reusable role prompts | Project-specific policy is adapted during installation |
| 3.4. 2026 tooling | Research record and verified CLI/action versions | Preview features and stale guide dates are not baseline dependencies |
| 3.5. Manual-first adoption | Local review baseline; optional manual Actions path | No autonomous issue triage or repair trigger |
| 4.1–4.2. CLI and layout | Setup guide; doctor; safe new-task wrapper | Authentication is an external prerequisite, not a mocked success claim |
| 4.3. Default-branch protection | Concrete ruleset generator; merge preflight | Solo approval count zero; actual check names must be observed; GitHub settings are external state |
| 4.4. Labels | Small setup vocabulary; required risk dropdown | Form fields do not automatically create risk labels |
| 4.5. Issue/PR templates | `.github/ISSUE_TEMPLATE/change.yml`; PR template | Required criteria, commands, risk and rollback fields |
| 4.6. CODEOWNERS | Setup instructions for active eligible reviewers | No fictional team or unsatisfiable solo owner approval |
| 5. Risk and plan design | T0–T4 table; role prompts; plan comment ID | Plan association checked mechanically; approval interpretation remains human |
| 6.1–6.5. Golden path to checks | Capture/plan/prepare/implement skills, approved-contract state, managed Astra launch, draft PR helper and CI | Exact executor UUID and task binding; approval before coding; early draft PR and documented evidence |
| 6.6. Independent review | Snapshot preparer, Copilot custom agent, read/search tool allowlist | Fresh process/config; no implementation history or PR hook execution |
| 6.7. Repair protocol | Repair skill, exact-session resume and public disposition procedure | Reviews and inline comments collected; user-selected one attempted round by default, with documented critical P0/P1 or explicit continuation; changed head/base requires fresh review |
| 6.8. Merge and cleanup | Finish skill, human-run script, journaled archival and guarded cleanup | Verified merge, matching tips and preserved ignored artifacts; remote deletion uses explicit SHA lease |
| 7. Review quality and independence | Review rubric with P0–P3 and six required finding components | Static reviewer explicitly reports that it executed no tests |
| 8. Domain gate | General rubric; NICME/SpiderML adaptations; evidence runner | Software pass does not establish a scientific conclusion |
| 8.3–8.4. Provenance and large artifacts | Hash/timing/exit-status manifest; artifact policy | Artifact freshness and scientific design still require inspection |
| 9. Validation tiers | V0–V4 ladder; CI; project CPU rollout plans | No GPU or costly campaign automatically triggered by PRs |
| 10. Security/governance | Read-only CI tokens; pinned actions; protected manual environments | No PR code execution in model job; tool controls are not an OS sandbox |
| 11. Cost/context/WIP | Explicit model policy and bounded review; daily checklist | All-Astra start follows user choice; cheaper helpers only after deliberate policy change |
| 12. Failure modes | Negative regression tests; recovery guidance | Preserves dirty worktrees, ignored outputs and post-merge commits |
| 13. Daily checklist | Operating guide daily rhythm | Durable artifacts rather than dependence on a private chat |
| Appendix A | Required issue form | Retains all contract dimensions and adds budget/stop conditions |
| Appendix B | PR template | Software/domain evidence, risk, rollback and model disclosure |
| Appendix C | AGENTS.md and NICME draft | Concise contract with linked detailed guidance |
| Appendix D | Compatible shell wrappers over tested Python Git helpers | Stronger preconditions than the sample shell scripts |
| Appendix E | `.agentic/prompts/` and independent-reviewer agent | Provider-specific launch is separate from role contract |
| Appendix F | REVIEW.md and domain-review.md | Findings require trigger, impact and evidence |
| Appendix G | Definitions in operating/review guides | Terms retain their original operational meaning |
| Research method/references | RESEARCH.md | Current primary documentation checked; no blanket guarantee of model correctness |

## Corrections and deliberate choices

1. **Preserve commits after a squash merge.** The guide's cleanup example checks remote
   merge state but not whether the local task branch acquired additional commits. This
   implementation also requires the local tip to equal the PR's merged head before
   bounded force deletion. It rejects mismatched worktree paths, fork PRs and wrong bases.
2. **Protect ignored local data.** A clean ordinary Git status can still hide important
   ignored files. Low-level cleanup inspects ignored paths and refuses their deletion.
   The human finishing script archives them first, preserving links and recording a
   recoverable journal, then reuses the original cleanup guards.
3. **Carry the reviewed SHA forward.** Fetching a fresh head immediately before merge
   does not establish that it was reviewed. The helper requires the recorded review SHA,
   checks that it is current, and prints a human-run command with `--match-head-commit`.
4. **Use an artifact snapshot for model review.** Detached worktrees are useful for
   isolated test execution, but detachment is not read-only enforcement. The review
   process instead receives inert text with an explicit read/search tool allowlist.
5. **Keep execution evidence honest.** The default model reviewer cannot run tests.
   Independent CI or human test execution supplies that evidence separately.
6. **Correct the check-state query.** The PDF's governance example reads
   `statusCheckRollup` in jq without requesting it in `--json`. The operating guide
   requests that field explicitly, and merge preflight reads required checks directly.
7. **Use concrete ruleset parameters.** Rather than relying on undocumented permissive
   defaults, the generator sets PR, review-resolution and required-check options explicitly.
   It binds the required GitHub.com Actions integration and requires an up-to-date base.
8. **Keep human merge immediate and deliberate.** The default printed command omits
   `--auto`. Merge queues and deferred auto-merge can be added later with a separate
   reviewed policy; they are not necessary for a solo maintainer's initial workflow.
9. **Separate template files from hosted state.** Creating a repository from a template
   does not establish account permissions, rulesets, secrets or environment approvals.
10. **Respect the earlier chat.** The recovered decisions specify public publication,
    guides only for sibling projects, both review paths and an initial all-Astra policy.
    The private memory record preserves those decisions and the full current prompt.

11. **Expose the whole Golden Path as skills.** Eight checked-in skill entrypoints cover
    orchestration and each requested phase. Managed repair resumes the original
    executor UUID; each independent reviewer still starts fresh. A local approval
    receipt binds the designated issue/plan but cannot authenticate a human decision.
