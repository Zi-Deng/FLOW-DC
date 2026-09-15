# Set up the FLOW-DC agentic workflow

The adopted repository is [Zi-Deng/FLOW-DC](https://github.com/Zi-Deng/FLOW-DC). Use local execution and local Copilot review first. The hosted review workflow is present but deliberately disabled. See [verification](VERIFICATION.md) for the current rollout state; file installation alone does not prove that accounts, CI or branch protection work.

## 1. Establish a clean control checkout

Use a normal clone on the repository's actual default branch, `main`. Keep implementation in sibling issue worktrees. Preserve user edits before updating the control checkout, and use a fast-forward pull once the adoption is merged:

```bash
git status --short
git fetch origin
git pull --ff-only origin main
```

If fast-forwarding fails, inspect the divergence. Do not reset or force-push an existing checkout to make the helper accept it. The [bootstrap procedure](#bootstrap-and-existing-projects) explains this installation's separate local control clone.

Use credential-free Git remotes. For HTTPS, authenticate the intended account with `gh auth login`, then configure Git with `gh auth setup-git`. Never put a token in the remote URL, task prompt, issue or configuration file. Repository API identity and Copilot inference entitlement are separate concerns.

Uploading `.github/workflows/` over HTTPS also needs a credential authorized for
workflow updates. During adoption the existing OAuth token lacked that scope, while
the existing SSH key was verified to authenticate as `Zi-Deng`. This workstation
therefore retains credential-free HTTPS for fetch and uses
`git@github.com:Zi-Deng/FLOW-DC.git` as origin's push URL. On another machine, use an
already verified SSH identity or deliberately authorize the needed HTTPS scope;
do not silently upload a new key or replace the account.

## 2. Install and verify local tools

The workflow runtime requires Python 3.12+, Git, GitHub CLI, GNU Make, Codex and Copilot CLI. The human finish/archive helper currently targets Linux and requires `renameat2` through the process C library and support on the destination filesystem.

Observed during adoption on September 14, 2026: Ubuntu 24.04.4, Codex 0.154.0, Copilot CLI 1.0.83 and gh 2.100.0. These are tested observations, not a promise that they will remain the latest versions. Install from official releases and inspect new CLI help before upgrading the pinned workflow assumptions.

```bash
git --version
gh --version
codex --version
copilot --version
gh auth status
codex login status
python3 -c 'import ctypes; print(ctypes.CDLL(None).renameat2)'
```

Codex is authenticated through ChatGPT on this workstation. Managed execution explicitly requests `gpt-6-astra`; the helper does not change the account's global model. The review helper uses `COPILOT_GITHUB_TOKEN` if supplied, otherwise retrieves the active `gh` token internally. Confirm that the inference account can select `claude-opus-5`; CLI installation and successful GitHub API calls do not establish model entitlement. Consult the [Copilot CLI authentication reference](https://docs.github.com/en/copilot/reference/copilot-cli-reference/cli-command-reference) and [supported models](https://docs.github.com/en/copilot/reference/ai-models/supported-models) before changing account plans.

### Create the dedicated Python environment

Preserve the existing research environment. This checkout's old `.venv` is a broken historical environment; the workflow uses `.venv-agentic` instead.

```bash
python3 -m venv .venv-agentic
.venv-agentic/bin/python -m pip install -r requirements-dev.txt
make check
```

If the OS Python lacks `ensurepip`, use an existing healthy Python to create the venv. On this workstation:

```bash
/home/zi/micromamba/envs/ml/bin/python -m venv .venv-agentic
```

This creates a separate environment without installing into the shared `ml` environment. `requirements-test.txt` supplies only the focused downloader-test dependencies. `requirements-dev.txt` adds pinned Ruff and PyYAML. These direct pins are not a complete transitive lock; record `pip freeze` with evidence when exact environment reproduction matters. The full application and TaskVine environment remains described by the root project documentation.

Configuration validation checks every required model, rubric, check-name and positive-integer budget field. Missing or malformed settings produce a named error; `max_diff_bytes` remains the sole nullable unlimited budget. Optional managed timeout/output limits must be positive integers when supplied.

`make check` runs both suites, scoped lint/format checks and skills/configuration/schema/workflow/link validation. It does not run a dataset campaign or HPC job. Individual targets are `test-flowdc`, `test-agentic`, `check-agentic`, `lint` and `check-clean`. Override `PYTHON` and `RUFF` explicitly when using a prepared environment outside the worktree; never install editable project code into a shared environment.

### Validate the native Codex sandbox

For the installed Codex 0.154.0, the direct smoke command is:

```bash
codex sandbox --config 'sandbox_mode="workspace-write"' -- /usr/bin/pwd
```

On this host the initial command failed because Ubuntu's AppArmor user-namespace restriction denied operations needed by bubblewrap. A profile attached to the exact Codex binary was prepared under the original checkout's ignored `.agentic-local/host-setup/`, with installer and rollback scripts. The administrator installed that profile, and the workspace sandbox startup command now succeeds. Native managed model execution still requires the post-installation pilot. The default sandbox continues to block sockets, including local HTTP test servers; run those application tests in the coordinator environment and report their provenance separately. This host fix does not expand the executor network policy.

The proposed profile follows Ubuntu's documented application-specific `userns` exception. It does not disable the global restriction. Its executable attachment must be reviewed after Codex upgrades. The host-specific source path and installation status are recorded privately; they are not portable repository configuration. See [Ubuntu's release notes](https://discourse.ubuntu.com/t/ubuntu-24-04-lts-noble-numbat-release-notes/39890) and the [Codex permissions model](https://learn.chatgpt.com/docs/permissions#how-enforcement-works).

## 3. Establish private continuity

Both `/memory/` and `/.agentic-local/` must be ignored before the first private write:

```bash
python3 -B scripts/agentic/workflow.py memory-init
python3 -B scripts/agentic/workflow.py doctor
git check-ignore memory/README.md .agentic-local/probe
git ls-files memory .agentic-local
```

The last command must print nothing. `doctor` checks local prerequisites and authentication; it does not prove successful model execution, GitHub protection or CI.

Keep human-readable persistent memory in the registered control checkout's `memory/README.md` and task notes. The adoption task note includes the original input prompt verbatim. Git worktrees do not share ignored directories. Copy only necessary notes deliberately when changing control checkouts and identify the authoritative index. Machine-readable task associations, original executor UUIDs, review packets and archives live under the control checkout's `.agentic-local/`; preserve them when relocating a clone.

## 4. Verify GitHub integration

The approved labels are `agent-assisted`, `risk:domain`, `risk:high` and `needs-design`. The issue form's risk field does not automatically create or apply a risk label. Apply labels deliberately to the actual task.

The two required contexts must first appear and pass on a real PR:

```bash
gh pr checks PR_NUMBER --repo Zi-Deng/FLOW-DC --json name,state,workflow
```

Regular CI uses Ubuntu 24.04, Python 3.12, read-only permissions, pinned action commits, no persisted checkout credentials and bounded job timeouts. `flowdc-tests` runs the eight current application regressions. `agentic-quality` runs the workflow tests, scoped Ruff and structural checks. Both jobs also verify a clean checkout after validation.

After the baseline PR is merged and both contexts exist, generate and inspect the main ruleset from the control checkout:

```bash
python3 -B scripts/agentic/workflow.py ruleset \
  --check flowdc-tests --check agentic-quality > .agentic-local/main-ruleset.json
cat .agentic-local/main-ruleset.json
gh api repos/Zi-Deng/FLOW-DC/rulesets
```

Create the reviewed ruleset only if it does not already exist; use a deliberate update to its ID otherwise. Read it back afterward. The intended `agentic-default-branch` policy requires a PR, strict up-to-date passing checks from GitHub Actions integration 15368, resolved review conversations and linear history. It blocks deletion and force pushes, and has no bypass actors. Required human approvals are zero for this solo-maintainer repository; a manual maintainer merge remains workflow policy. The owner can still change repository settings. A recorded ruleset response and actual PR checks are stronger evidence than a configuration file alone. See the [GitHub ruleset API](https://docs.github.com/en/rest/repos/rules#create-a-repository-ruleset).

## 5. Run a managed task

Use the [skill contract](SKILLS.md), beginning with `$agentic-workflow`. The coordinator owns GitHub writes and the clean control checkout. The dedicated Astra executor owns implementation in the assigned worktree and retains one exact session UUID through continuation and repair. Each Opus review uses a fresh static snapshot and independent session.

The first post-installation pilot is a small, useful FLOW-DC maintainer quick reference. Its acceptance includes a native checkpoint, continuation of that same UUID, a draft PR, both CI checks, local Opus review, explicit feedback assessment and human finish preparation. If the review has no material findings, a no-edit feedback assessment can demonstrate continuation without inventing repairs. A successful model turn alone does not satisfy the whole pilot.

## 6. Hosted review is opt-in

Keep repository variable `AGENTIC_COPILOT_ACTIONS_ENABLED=false` during this rollout. No hosted reviewer token is being onboarded. Normal CI never invokes a model.

A later authorized onboarding should first confirm local review, then create `copilot-review` and `copilot-review-publish` environments restricted to the trusted default branch. Configure environment reviewers where the account plan supports them. Store an eligible Copilot credential in the first environment as `COPILOT_REVIEW_TOKEN` using GitHub's hidden secret prompt; never paste it into a chat or command argument. Set the enabling variable to `true` only after reviewing those settings.

The manual workflow accepts PR number, issue number, designated plan-comment ID and exact head SHA. It checks out trusted main, exports PR blobs as data, generates review with the inference token, and optionally publishes through a separate job with narrowly scoped `pull-requests: write`. Publication defaults to false. The publisher has no Copilot secret. Review artifacts are retained for seven days; they contain source/context and should be treated accordingly. Follow [the manual review procedure](REVIEW.md#manual-actions-procedure) and verify generation, hashes, usage and the resulting COMMENT review before claiming hosted review works.

## Bootstrap and existing projects

This adoption starts from existing consolidation commit `d8c4ec35c9722067968626b8d9dbda8e28bcfa9e`. [Baseline PR #2](https://github.com/Zi-Deng/FLOW-DC/pull/2) publishes that commit separately. Its human merge uses a merge commit to preserve the existing local main ancestry. The workflow adoption and later tasks use squash merges once linear-history protection is active.

The installer was applied to an ignored staging directory from pinned template commit `b4a1df739a15b20b26635602e6b6a21fb08e0ac5`. Its sole existing-file conflict, `AGENTS.md`, was merged by retaining all FLOW-DC instructions and appending workflow guidance. The source template was not modified. `.agentic/template-origin.json` records original payload hashes; [research notes](RESEARCH.md) record the commit and PDF hashes. Local adaptations intentionally differ from those original file hashes.

A separate bootstrap control clone supplies trusted review code before workflow tooling exists on remote main. Its control commits are local implementation scaffolding; never push that clone's `main`. The adoption branch lives in its registered sibling issue worktree and is the only implementation branch published. After the adoption's human merge, update the original clean checkout with a fast-forward pull and use it as the permanent control checkout. Retain the bootstrap clone and its review artifacts until continuity has been reconciled; do not reset it or fabricate a managed executor record to qualify it for automatic finish.

For later template upgrades, stage the pinned installer payload again, inspect conflicts and compare changes against the provenance manifest. Keep FLOW-DC's runtime policy, required check names, application guidance and domain rubric. Do not reapply an upstream file blindly over a local adaptation. See [traceability](TRACEABILITY.md).
