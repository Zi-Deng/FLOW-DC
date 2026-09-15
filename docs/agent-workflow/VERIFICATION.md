# FLOW-DC workflow verification

This record describes FLOW-DC evidence collected during the September 14, 2026 adoption. It does not import upstream results as local proof. The public contract is [issue #3 and its approved plan](https://github.com/Zi-Deng/FLOW-DC/issues/3#issuecomment-5673478789); implementation is published in [PR #4](https://github.com/Zi-Deng/FLOW-DC/pull/4). Update this record with actual run and review links as rollout advances.

## Baseline and provenance

- FLOW-DC consolidation: `d8c4ec35c9722067968626b8d9dbda8e28bcfa9e`, published separately in [PR #2](https://github.com/Zi-Deng/FLOW-DC/pull/2).
- Imported template: `b4a1df739a15b20b26635602e6b6a21fb08e0ac5`, merged template PR #6. Its 111 runtime tests and all eight skill packages passed local pre-adoption inspection. That establishes the inspected source baseline only.
- The source PDFs, exact hashes, release pins and deliberate adaptations are recorded in [RESEARCH.md](RESEARCH.md).
- The original FLOW-DC checkout, historical `.venv`, recovery stash, data and immutable archive snapshots are preserved. Original repository instructions are retained in `AGENTS.md`.

## Local checks obtained

| Check | Result and scope |
| --- | --- |
| Existing FLOW-DC suite | 8 tests passed at consolidation SHA with Python 3.12.12; local HTTP and temporary output directories |
| Adapted runtime suite | 120 tests passed, including all imported cases and 9 new budget/configuration regressions |
| New budget behavior | Unlimited/default/null diff; finite inclusive UTF-8 boundary; invalid values; empty diff; unchanged private-path guard; separate bounded executor prompt |
| Full local gate | `make check` passed with the dedicated Python 3.12.12 environment: 8 application tests, 120 runtime tests, scoped Ruff, schema/configuration/CI context checks, all eight skills, workflow YAML and maintained Markdown links |
| Skill payload | All eight packages and `agents/openai.yaml` metadata imported and structurally validated; shared contract links adapted for FLOW-DC |
| GitHub configuration | Four approved labels created and read back; `AGENTIC_COPILOT_ACTIONS_ENABLED=false` set and read back |
| Sandboxed workflow suite | All 120 workflow tests passed inside the default Codex workspace sandbox after the host fix; external GitHub/model services remain controlled doubles |
| AppArmor profile and startup | Administrator-installed profile matches the prepared source, is owned by root with mode 0644, and `codex sandbox --config 'sandbox_mode="workspace-write"' -- /usr/bin/pwd` now succeeds |
| Private memory | Created after local exclusions; full original task prompt and decisions retained outside Git |

The local gate passed without changing application code or historical artifacts. Hosted check links and independent review evidence belong in the adoption PR as they complete.

The application suite covers base/gradient downloader consolidation behavior, retries, payload and overview integrity, imagefolder/webdataset and compressed/uncompressed tar outputs, overwrite permission, benchmark-owned output directories, configuration compatibility, sample-aware scheduling/snapshots and import safety. The TaskVine runtime is stubbed; these tests do not validate cluster execution.

The runtime suite uses real temporary Git repositories and controlled doubles for GitHub and model processes. It exercises worktree identity, durable contract/task state, exact UUID continuation, review integrity, review-round accounting, evidence/finish gates, process cleanup and journaled archival. It never merges a real PR to test destructive behavior.

## GitHub Actions evidence

The first complete adopted code/documentation revision,
`6492bfcdb002cd1c3f5186f681eb4595be4820e8`, passed both required contexts:

- [FLOW-DC regression run](https://github.com/Zi-Deng/FLOW-DC/actions/runs/34920044821): `flowdc-tests`, success.
- [Workflow quality run](https://github.com/Zi-Deng/FLOW-DC/actions/runs/34920044832): `agentic-quality`, success.
- [Push-triggered workflow quality run](https://github.com/Zi-Deng/FLOW-DC/actions/runs/34920041199): success.

GitHub's check-run API reported integration ID `15368` for both required names on
that head. These immutable links record that revision; use the current PR checks
for later documentation or code commits. The four approved labels and the disabled
hosted-review variable were read back successfully. The [active main ruleset](https://github.com/Zi-Deng/FLOW-DC/rules/23400835) was created after the baseline merge and read back against the complete proposed configuration. It requires both check names from Actions integration 15368, strict up-to-date status, resolved threads, a PR and linear history, and blocks force pushes/deletion with no bypass actors.

## Live rollout acceptance

| Acceptance | Required evidence |
| --- | --- |
| Baseline publication | PR #2 human-merged at `0e88dbc38449a4e7cdfef5c04aeca8c5bbe94de1` on September 15, 2026 UTC; the original checkout fast-forwarded successfully |
| Adoption CI | Actual `flowdc-tests` and `agentic-quality` success on the committed adoption head |
| Local independent review | Fresh Copilot Opus 5 invocation, exact head/base, recorded usage and published COMMENT review with findings dispositions |
| Main protection | Read-back of active `agentic-default-branch` ruleset matching observed check names and Actions integration |
| Native executor | Successful Codex workspace sandbox on this host and an actual managed task result |
| Session continuity | Pilot checkpoint and continuation report the same native executor UUID |
| Finish path | Explicit assessment and human command; after the human runs it, verify merge plus artifact archive/cleanup |
| Hosted review | Deliberately disabled; no hosted entitlement, secret or generation claim |

At initial implementation, the host sandbox test failed with bubblewrap/AppArmor permission errors. After the administrator installed the targeted profile, startup succeeded. A separate socket probe remains denied by the default network restriction; the local HTTP application suite therefore runs in the coordinator environment. This is distinct from the repaired namespace-startup failure. The adoption merge and native managed model pilot remain separate acceptance steps; bootstrap changes must not be assigned a fabricated managed completion record.

## Reproduce focused checks

```bash
python3 -m venv .venv-agentic
.venv-agentic/bin/python -m pip install -r requirements-dev.txt
make check
# From a committed checkout, also require no generated tracked/untracked changes:
make check-clean
```

Use a healthy existing Python if the OS venv package is unavailable. See [SETUP.md](SETUP.md) for the known workstation environment and explicit interpreter overrides. Record the commit, exact command, interpreter and dependency versions with each live evidence run. Passing these checks does not guarantee no defects or support an unmeasured performance claim.
