# Sources and implementation decisions

Research and repository inspection were performed September 14, 2026 (America/Los_Angeles; some GitHub records fall on September 15 UTC). The evidence hierarchy is: observed implementation and tests for behavior, current primary documentation for provider/platform interfaces, and the supplied PDF guides for intent and design rationale. Historical examples are not treated as current executable commands.

## Supplied resources

Both PDFs were read in full. The template runtime, eight skills, tests, CI, operating documentation and merged PR #6 were inspected together with FLOW-DC's application, repository instructions, Git state and GitHub settings.

| Resource | Exact provenance | Role in adoption |
| --- | --- | --- |
| `Issue-to-PR-Development-with-Worktree-Isolation.pdf` | 33 physical pages; SHA-256 `e00456e51e9bb304288ede320e30e1245f7acdae5e6444f37e54bd761c3434ce` | General issue, worktree, review, evidence and human-merge design |
| `Current-Agentic-GitHub-Workflow-Guide.pdf` | 32 physical pages; SHA-256 `5c92d782cebd6d1170ad139a798460e417445043657b3f28778f9dea9d3d8c96` | September 11 operating account at historical template baseline `609594c`; private source supplied locally |
| [Merged template PR #6](https://github.com/Zi-Deng/agentic-github-template/pull/6) | Commit `b4a1df739a15b20b26635602e6b6a21fb08e0ac5`; tree `0fa56f75ed7a652a2b0c7de2794cec828c0e0a8a` | Authoritative imported runtime and all eight skills, including Opus 5/400-credit policy |
| FLOW-DC consolidation | Commit `d8c4ec35c9722067968626b8d9dbda8e28bcfa9e`; [baseline PR #2](https://github.com/Zi-Deng/FLOW-DC/pull/2) | Existing maintained code and eight application regressions |
| [Adoption contract](https://github.com/Zi-Deng/FLOW-DC/issues/3#issuecomment-5673478789) | Maintainer-approved plan, including unlimited review diff and independent prompt limit | FLOW-DC-specific changes and acceptance boundaries |

The first PDF is in the template repository root. The current-workflow PDF was supplied in the template's private `memory/` directory; it is not copied into FLOW-DC or uploaded as a review artifact. Hashes identify the supplied files without publishing their contents. Original imported-file hashes are retained in [the provenance manifest](../../.agentic/template-origin.json); the manifest describes the source payload, not the final adapted files.

## Current primary references

| Reference | Decision it informs |
| --- | --- |
| [OpenAI model catalog](https://developers.openai.com/api/docs/models) | Explicit `gpt-6-astra` default; availability and account entitlement still require a live request |
| [Codex noninteractive execution](https://learn.chatgpt.com/docs/non-interactive-mode) | JSON events, explicit session-ID continuation, structured final results and separate final-message output |
| [Codex skills](https://learn.chatgpt.com/docs/build-skills) | Repository skill packages and invocation metadata; scoped phase entrypoints |
| [Codex permissions](https://learn.chatgpt.com/docs/permissions#how-enforcement-works) | Workspace sandbox enforcement must be tested on the actual host |
| [Copilot supported models](https://docs.github.com/en/copilot/reference/ai-models/supported-models) | Opus 5 is listed; an installed CLI alone does not prove the account can select it |
| [Copilot programmatic reference](https://docs.github.com/en/copilot/reference/copilot-cli-reference/cli-programmatic-reference) | Explicit model, static tool allowlist, fresh settings, usage output and noninteractive controls |
| [Copilot models and pricing](https://docs.github.com/en/copilot/reference/copilot-billing/models-and-pricing) | Credit budget is a provider-specific spending control, not an API-dollar conversion |
| [GitHub Actions security](https://docs.github.com/en/actions/reference/security/secure-use) | Pinned action commits, minimum permissions, trusted policy checkout and separation of model inference from publication |
| [Repository rules API](https://docs.github.com/en/rest/repos/rules#create-a-repository-ruleset) | Concrete check contexts, Actions integration binding and explicit branch rules |
| [GitHub CLI merge](https://cli.github.com/manual/gh_pr_merge) | Pin the reviewed head and leave merge invocation to the maintainer |
| [Ubuntu 24.04 release notes](https://discourse.ubuntu.com/t/ubuntu-24-04-lts-noble-numbat-release-notes/39890) | An executable-specific AppArmor `userns` profile is the supported narrow host remedy |

Current official documentation, installed CLI help and GitHub release/API responses were cross-checked. Website model listings do not independently attest to the model a provider executed. Review metadata records the requested model and reported usage, with that limitation made explicit.

## Observed tool and action versions

| Component | Version / immutable reference |
| --- | --- |
| Codex | `0.154.0` |
| Copilot CLI | `1.0.83` |
| GitHub CLI | `2.100.0` |
| Local dedicated Python environment | `3.12.12`; created from the healthy `ml` interpreter |
| CI runtime | Ubuntu 24.04 / Python 3.12 |
| `actions/checkout` | v7.0.1 — `3d3c42e5aac5ba805825da76410c181273ba90b1` |
| `actions/setup-python` | v7.0.0 — `5fda3b95a4ea91299a34e894583c3862153e4b97` |
| `actions/upload-artifact` | v7.0.1 — `043fb46d1a93c77aae656e7c1c64a875d1fc6a0a` |
| `actions/download-artifact` | v8.0.1 — `3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c` |
| Hosted Copilot Linux x64 archive | SHA-256 `ffbe1c429664b8a05efed67ecdb467123e40fcaa3c6c14ef9a98ba74da4687b7`; pinned by `scripts/agentic/install_tool.py` |

These observations are date-specific. Upgrade pins through an explicit maintenance task, validate the new CLI flags and sandbox, then exercise an actual request. Do not substitute a mutable action tag or silently choose a different model when an invocation fails.

## Local adaptations and tradeoffs

### Review diff and executor prompt are separate controls

The template's `max_diff_bytes` previously capped both PR diffs and managed executor prompts at 300000 bytes. FLOW-DC defaults the review diff to unlimited: `null` or an omitted key. A positive integer opts into a cap; booleans, strings, zero, negative values and other types fail configuration validation. Empty diffs still fail. The independent `managed_max_prompt_bytes` defaults to 300000 and must remain a positive integer.

Unlimited means there is no configured byte cap on `diff.txt`; it does not mean unlimited model context, cost or complete review coverage. The existing 250000-byte source-file limit, 12000000-byte source snapshot budget, 400-credit budget, 900-second timeout and output limits remain. Per-file omissions are indexed, private-path diffs are rejected, and failed or incomplete reviews consume the task's one attempted round. Regression tests cover a diff above the old cap, inclusive UTF-8 byte boundaries, empty/private diffs, invalid settings and prompt independence.

### Preserve the project and make CI useful

The application suite exercises maintained behavior through local HTTP and temporary output directories. It remains separate from the imported runtime suite, which uses real temporary Git repositories and controlled doubles for external services. Lint and Markdown-link checks are scoped to workflow-owned code and documentation; historical research files are not rewritten to satisfy a newly introduced style rule. FLOW-DC's existing `AGENTS.md` guidance is retained, with the workflow contract appended.

### Bootstrap differs from normal managed work

The source template is not modified. Its installer payload is staged, then adapted in a separate control clone and issue worktree. That control clone permits independent review with trusted code before remote main contains the workflow. It is not evidence that a managed executor completed the bootstrap. The post-installation pilot must supply actual native session and continuity evidence. Old upstream verification logs are linked as provenance rather than copied as FLOW-DC results.

### Models and cost

Astra and Opus 5 are the maintainer's explicit initial choices. No evidence collected here establishes that they are optimal for every FLOW-DC task, or that a cheaper model would perform equally well. A later cost study should use representative tasks with identical contracts, seeded defects where appropriate, matched evidence, recorded latency/usage, and rates of missed material findings and unnecessary changes. Adopt a cheaper model only through a deliberate policy/runtime/test update; the managed guards currently require Astra and Opus 5. Do not infer quality from model price or provider identity.

One attempted review round per task is an orchestration policy, not a lifetime account quota. The low-level review CLI remains a manual primitive; operators must preserve the same policy when using it for bootstrap. Extra rounds need supported critical P0/P1 repair verification or explicit user continuation. Minor findings and incomplete coverage do not grant another invocation automatically.

## Limits of this research

Static inspection and mocked tests establish specific properties, not an error-free system. The local application suite does not validate live TaskVine, remote storage or research-scale throughput. Native execution, current-head Copilot review, Actions runs, branch protection and human finish require their own recorded evidence. The [verification record](VERIFICATION.md) separates those outcomes and remaining handoffs.
