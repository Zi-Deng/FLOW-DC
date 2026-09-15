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
| Initial adapted runtime suite | 120 tests passed, including all imported cases and 9 new budget/configuration regressions |
| New budget behavior | Unlimited/default/null diff; finite inclusive UTF-8 boundary; invalid values; empty diff; unchanged private-path guard; separate bounded executor prompt |
| Initial full local gate | `make check` passed with the dedicated Python 3.12.12 environment: 8 application tests, 120 runtime tests, scoped Ruff, schema/configuration/CI context checks, all eight skills, workflow YAML and maintained Markdown links |
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

## First independent review and repairs

The [first local Opus 5 COMMENT review](https://github.com/Zi-Deng/FLOW-DC/pull/4#pullrequestreview-5205046280)
completed against head `97e03a7960ac481b0ff2ce9039e62999e2d05b53` and base
`0e88dbc38449a4e7cdfef5c04aeca8c5bbe94de1`. The packet contained the full 428210-byte
diff, 184 indexed source entries, three current check-run records, and the public
issue/plan/PR context. Its 32 omissions were binary or unsupported source types.
Packet/workspace integrity and publication checks passed. The provider usage file
identified `claude-opus-5`; it remains a provider report rather than independent model
attestation. No tests were executed by the reviewer.

The report raised two P2 findings (result-finalization recovery and text encoding),
two P3 concerns (configuration validation and oversized publication), and questions.
It also disclosed that it sampled the diff and did not inspect several critical
runtime/skill files or fully read the issue/plan. Those limits remain attached to the
unaltered report; successful inference is not complete acceptance coverage.

Repairs add a durable result journal and atomic finalization, pre-request CLI-version
validation, recovery without another model invocation, explicit UTF-8 text/process
I/O, complete required configuration validation and a report-size prompt. The existing
60000-byte publication guard remains. Tests inject report/metadata failures, reject
altered/stale recovery records, prove one-attempt pipeline recovery, exercise an
explicitly ASCII process locale and preserve an oversized report without publishing
or rerunning it. The ordinary C locale enables Python 3.12 UTF-8 mode on this host;
the encoding reproduction required explicitly disabling that mode and locale coercion.

The repaired implementation passed `make check`: 8 application tests and 133 workflow tests, plus all scoped quality/structural gates. All 133 workflow tests also passed inside the default Codex workspace sandbox. These are coordinator and controlled-fixture results, not model-executed tests.

Those repairs changed the reviewed head and required explicit user continuation for
another review. That continuation produced the second report below. The native
managed Astra pilot remains a separate post-installation acceptance step.

## Second independent review and dispositions

The [second Opus 5 COMMENT review](https://github.com/Zi-Deng/FLOW-DC/pull/4#pullrequestreview-5206383294)
completed against head `aef76b5a36f049572525480dc3d05a4305176799` and the same
`0e88dbc38449a4e7cdfef5c04aeca8c5bbe94de1` base under explicit user continuation.
It raised one P2 finding, four P3 findings and four questions. Its report is preserved
unchanged, including its unread-file and public-contract coverage limitations.

The subsequent repair addresses every finding:

| Finding | Disposition |
| --- | --- |
| F1: project data paths absent from review exclusions | Add repository-relative exclusions for input/output data, dataset statistics, benchmark manifests/results, playground and archives. Snapshot tests retain maintained example configs and benchmark source. Git fixtures also reject restricted additions, deletions and renames out of a data directory. Public configs are intentionally reviewable; their JSON format alone is not a privacy defect. |
| F2: finish wrapper lacks executable mode | Commit executable mode for `scripts/finish-task.sh`; the structural gate now checks all three shell wrappers. |
| F3: private parents use ambient umask | Create all missing state ancestors with mode `0700`, restrict existing state roots, reject symlinked destinations and atomically write executor prompts as `0600`. Tests exercise a permissive umask. Existing `0700` run directories already protected their contents, so readable child files did not establish the report's claimed cross-user content exposure. |
| F4: duplicate task-branch CI | Restrict the quality workflow's push trigger to `main`, matching application CI, and validate both trigger mappings. PR pushes continue to run both required checks. |
| F5: installer omits project validation integration | Document the omitted Makefile, dependency, lint and project-check assets in the installer result and setup guide. FLOW-DC contains the manually integrated files. Copying its application-specific gates into arbitrary projects would be inappropriate. |

Question dispositions:

1. **Personal configuration:** give the review process a temporary home and explicit
   temporary XDG directories in addition to fresh `COPILOT_HOME`. Regression checks
   verify the child environment and removal of inherited provider/skill overrides.
   This is configuration-discovery isolation, not a kernel sandbox or proof against
   a compromised CLI. Actual model execution under the revised environment is a
   fresh-review step, separate from mocked regression evidence.
2. **Old-head feedback:** retain assessment of all published findings; remove the
   unused `head` argument from `relevant_records` and both callers. Advancing the PR
   must not silently erase an unresolved older finding.
3. **Provenance:** rechecked all 60 original payload hashes against pinned template
   commit `b4a1df739a15b20b26635602e6b6a21fb08e0ac5`; all match. FLOW-DC additions and
   local adaptations are intentionally separate from the original import manifest.
4. **Post-merge pilot:** still pending by the approved rollout sequence. The adoption
   PR should leave issue #3 open until its post-installation acceptance is fulfilled.

The repair passed `make check`: **8 application tests and 141 workflow tests**, plus
scoped lint/formatting and all skill, configuration, schema, workflow and link gates.
All **141 workflow tests** also passed inside the default Codex workspace sandbox.
The regression suite uses controlled model/service doubles; no application, benchmark
source or data artifact was changed. The repair also corrects the documented managed
recovery flag to `--execute`.

The second report predates these repairs. Current-head review and required Actions
results must be assessed before marking the PR ready. Two attempts are consumed;
no supported P0/P1 finding authorizes an automatic extra round. Keep current evidence
and the explicit continuation decision in the PR rather than treating this historical
verification record as a live readiness signal.

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
