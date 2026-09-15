# Setup and adoption

Use the local workflow first. Enable hosted review after a representative local PR
has completed the review loop. The same files support both paths.

## 1. Install and authenticate the tools

Use Python 3.12 or newer, current Git and GNU Make. Install GitHub CLI, Codex and Copilot CLI
from their official distribution channels. This workstation was provisioned with
GitHub CLI 2.100.0 and Copilot CLI 1.0.83 from release archives whose SHA-256 digests
were checked against GitHub release metadata. The Golden Path extension was validated
against installed Codex 0.154.0 on 2026-09-14. These are observed versions, not a
claim that they remain the latest releases.

The human finishing script currently targets Linux with atomic no-replace rename
support. Its [archival requirements and recovery procedure](FINISH.md) are part of
adoption. Use a healthy Python installation; `python3 -c 'import ctypes'` should
succeed before relying on the finishing helper. Model sandbox startup must also be
tested in the environment that will run the managed executor.

The process C library must also expose `renameat2` through `ctypes.CDLL(None)`;
kernel support alone is insufficient. The helper refuses archival if that symbol
or the filesystem's no-replace operation is unavailable. Installer target paths
must not contain parent (`..`) components or symlinks; supply a direct target path.

```bash
git --version
gh --version
codex --version
copilot --version
gh auth login --hostname github.com --git-protocol ssh
gh auth status
codex login status
```

For HTTPS Git remotes, run `gh auth setup-git` after login. SSH remotes use your
existing SSH authentication; do not upload or replace a key without checking which
account it identifies. The review fetch uses a temporary Git credential helper for
private repositories in Actions.

The reviewer uses `COPILOT_GITHUB_TOKEN` when supplied, otherwise it retrieves the
active `gh` OAuth token internally without printing it. A classic PAT is not a
Copilot credential. An explicitly supplied fine-grained token needs **Copilot Requests**.
A token cannot grant model access that its account lacks. This workflow requires
an account entitled to an explicit Claude model. GitHub's
[current plan comparison](https://docs.github.com/en/copilot/get-started/plans)
lists Free and Student as Auto-only. Verify model access before buying or changing
a plan. You can keep `gh` authenticated as the repository owner and supply your
eligible Copilot account's token through `COPILOT_GITHUB_TOKEN`. The model account
supplies inference access; the publishing credential determines the GitHub review's
author. Record that distinction when different accounts are deliberately used.
Keep tokens in the environment or credential store, never `.agentic/config.json`.
See [GitHub's Copilot authentication reference](https://docs.github.com/en/copilot/reference/copilot-cli-reference/cli-command-reference).

The configured reviewer is `claude-opus-5`, with a 400-AI-credit review limit. Verify
that the inference account can select that exact model; earlier Sonnet or Fable access
does not establish Opus access. Read the [Opus access guidance](REVIEW.md#claude-opus-5-access).
The Copilot Requests token permission and existing environment secret names remain the
same; selecting a model does not grant account entitlement.

Open Codex and use `/model`; also inspect Copilot's `/model` picker for account availability before
spending on a project task. The initial names in `.agentic/config.json` are explicit
requests, not guarantees of entitlement. Codex receives `--model gpt-6-astra` on
every role launch; no user-wide model setting is modified. See the
[Codex CLI reference](https://developers.openai.com/codex/cli/reference).

## 2. Validate this checkout

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-dev.txt
make check
python3 scripts/agentic/workflow.py memory-init
python3 scripts/agentic/workflow.py doctor
git check-ignore memory/README.md
git ls-files memory
```

The final command must print nothing. On systems without `ensurepip`, install the
OS's Python venv package or create the environment using an existing managed Python.
For this workstation, `micromamba run -n ml python -m venv .venv` provides that route
without modifying the shared `ml` environment.

`doctor` checks tools, Git state, local configuration and `gh` authentication. It does
not prove Copilot entitlement, model access, ruleset enforcement or successful CI.
Those need the first live PR described below.

## 3. Publish the template

The original owner's selected destination is public `Zi-Deng/agentic-github-template`.
If adapting these instructions elsewhere, choose your own owner and visibility.
Before the initial push, inspect `git status`, `git diff --cached`, the file list and
the local verification report. Do not force-add ignored memory.

```bash
# Only for a fresh directory: git init -b main
git add .github .agentic scripts tests docs AGENTS.md README.md Makefile \
  pyproject.toml requirements-dev.txt .gitignore \
  Issue-to-PR-Development-with-Worktree-Isolation.pdf \
  Issue-to-PR-Development-with-Worktree-Isolation.tex
git diff --cached --check
git commit -m "Initialize issue-to-PR workflow template"
gh repo create Zi-Deng/agentic-github-template --public --source . --remote origin --push
gh repo edit Zi-Deng/agentic-github-template --template \
  --enable-issues --enable-squash-merge --delete-branch-on-merge
```

If the repository or remote already exists, inspect it and use a normal push to its
intended branch. Do not repeat `repo create`, overwrite a remote or force-push to
recover from an ambiguous error. Branch protection is configured after a real check
run exists, so its required names can be verified rather than guessed.

## 4. Create labels and a first PR

Create only the labels you intend to use. The issue form stores risk as a required
field; it does not magically translate that field into a label.

```bash
gh label create agent-assisted --color 2E75B6 --description "Agent-assisted change"
gh label create risk:domain --color C55A11 --description "Domain evidence required"
gh label create risk:high --color 9E2B25 --description "Privileged or high consequence change"
gh label create needs-design --color D4C5F9 --description "Design decision needed"
```

Existing labels need no replacement. Use the issue form to create a small, real
documentation improvement with a measurable criterion. Post its reviewed plan,
create its worktree, open a draft PR and watch CI. Follow the operating guide for
the exact commands. Avoid a meaningless empty PR solely to obtain a green check.

## 5. Protect the default branch

```bash
gh pr checks 1 --json name,state,workflow
python3 scripts/agentic/workflow.py ruleset --check quality > /tmp/agentic-ruleset.json
cat /tmp/agentic-ruleset.json
gh api repos/{owner}/{repo}/rulesets --method POST --input /tmp/agentic-ruleset.json
gh ruleset list
```

Replace `1` with the real PR number and `quality` with the **observed** check name.
Set `.agentic/config.json` → `required_checks` to the same names. Existing projects
should normally require their own project check and `agentic-quality`. The template's
`quality` job runs lint, formatting and its complete workflow tests.

The generated ruleset blocks deletion and force pushes, requires linear history,
requires a PR, resolves review conversations, dismisses stale approvals and requires
up-to-date passing checks. Status checks are bound to the GitHub Actions integration
(15368), so a different app cannot satisfy that context by using the same name.
This GitHub.com integration ID should be revalidated for a different host. There are
no configured bypass actors. These are concrete parameters for the
[ruleset API](https://docs.github.com/en/rest/repos/rules#create-a-repository-ruleset).

For a solo maintainer, the required human approval count is zero because an author
cannot approve their own PR. The maintainer's manual merge is still mandatory policy.
The scripts do not pretend this creates a distinct human identity or prevents the
owner from changing their own repository rules. With a second active collaborator,
add required approvals and real CODEOWNERS entries for `.github/`, `.agentic/` and
domain-critical paths. Do not install fictional team handles.

When updating an existing ruleset, inspect its ID and settings first, then deliberately
`PUT` the reviewed JSON to that ID. Do not repeatedly create duplicate rulesets.
After activation, confirm a failing PR cannot merge and a direct default-branch push
is blocked using an expendable test branch/repository or the rule evaluation UI.

## 6. Enable manual Actions review

Create an environment named `copilot-review`, restrict deployments to the default
branch, and add required reviewers when your plan supports them. Store a fine-grained
Copilot token in that environment as `COPILOT_REVIEW_TOKEN`. Create
`copilot-review-publish` for the separate publication job and configure its reviewers.
Only then set repository variable `AGENTIC_COPILOT_ACTIONS_ENABLED=true`.

### Create and store the reviewer credential

1. Open the [fine-grained token form](https://github.com/settings/personal-access-tokens/new)
   while signed into the personal account with eligible Copilot access.
2. Set **Resource owner** to that personal account and choose an expiration date.
3. For this inference-only token, select **Public repositories**. Repository API
   access uses separate credentials in the workflow, including for private projects.
4. In **Permissions**, select the **Account** tab beside **Repositories**, then click
   **Add permissions**. Search for **Copilot Requests** and choose **Read-only**.
   The token form used during this setup offered only that access level, matching
   GitHub's [PAT setup guidance](https://github.github.com/gh-aw/reference/auth/#copilot_github_token).
5. Leave other optional permissions unselected. **Copilot agent settings** is a
   different repository permission and is not needed here. If the dropdown says
   **Select repository permissions**, close it and switch to **Account** first.
6. Generate the token and store it using the hidden terminal prompt:

```bash
gh secret set COPILOT_REVIEW_TOKEN --repo YOUR-OWNER/YOUR-REPO --env copilot-review
```

Do not paste the token into a chat, commit it, or pass its value on the command line.
The secret authenticates model requests only. The workflow's built-in `GITHUB_TOKEN`
handles repository reads and the separate COMMENT publication job. Token visibility
in `gh secret list --env copilot-review` confirms storage, not successful inference;
the first hosted run must establish that.

After successful local review and secret storage, enable the manual workflow:

```bash
gh variable set AGENTIC_COPILOT_ACTIONS_ENABLED --repo YOUR-OWNER/YOUR-REPO --body true
```

Follow the [manual review procedure](REVIEW.md#manual-actions-procedure). If the run
waits for environment approval, open its Actions page, choose **Review deployments**,
select **copilot-review**, and choose **Approve and deploy**. This is the configured
maintainer gate, not a token failure. An enabled publication job has a separate gate.

The manual workflow requires PR number, issue number, approved-plan comment ID and
the exact head SHA. It uses a trusted default-branch checkout, builds a text snapshot,
and runs Copilot with no GitHub write credential. An optional publication job gets
PR write permission only after the generation job succeeds. `publish` defaults to
false. Review artifacts expire after seven days; retain durable findings on the PR.

Organization repositories may qualify for a built-in token path under separate
Copilot policies. This template deliberately implements the fine-grained-token path
for the selected personal repository. Do not assume an ordinary Actions token has
Copilot entitlement. See [Copilot in Actions](https://docs.github.com/en/copilot/how-tos/copilot-cli/automate-copilot-cli/automate-with-actions).

## New projects

Once this repository is marked as a GitHub template, use **Use this template**, or:

```bash
gh repo create YOUR-OWNER/YOUR-PROJECT --template Zi-Deng/agentic-github-template --private --clone
```

Adapt `AGENTS.md`, the README, validation commands, model configuration and domain
rubric. Keep the generic runtime and regression tests. Set up labels, rulesets,
secrets, environments and repository settings in the new repository; those settings
are not supplied by copying files. Run `memory-init` in every fresh clone.
See [creating a repository from a template](https://docs.github.com/en/repositories/creating-and-managing-repositories/creating-a-repository-from-a-template).

## Existing projects

Do not replace the project with this repository or merge unrelated Git histories.
Inventory the project, establish a clean baseline and create an adoption worktree.
Preview the copy from the template checkout:

```bash
python3 scripts/agentic/install.py /absolute/path/to/project-adoption-worktree
```

The preview reports new, identical and conflicting paths and writes nothing. If
there are no conflicts, `--apply` installs only workflow-owned files. It does not
copy the source PDF, this root README, project dependencies, a root Makefile, or private
memory. It records file hashes in `.agentic/template-origin.json` for future updates.

If files conflict, install into a new temporary staging directory and merge the
relevant files into the adoption worktree manually. Preserve existing instructions,
CI jobs, issue forms and project validation. There is intentionally no `--force`
option. Add `/memory/` and `/.agentic-local/` to the project's `.gitignore`; investigate
already tracked memory before assuming that ignore rules make it private.

The installer includes `.agents/skills` with all eight entrypoints, their metadata,
and the supporting helpers and guides. After adoption, launch Codex in the project
and verify that `$agentic-workflow` and the seven phase skills appear; restart the
session if discovery has not refreshed. Preserve any existing project skills when
reconciling conflicts. Read [SKILLS.md](SKILLS.md) for invocation and managed session
recovery and [FINISH.md](FINISH.md) before using the human finishing script.

The portable `agentic-quality` job tests the workflow infrastructure. It cannot test
your application automatically. Add or retain a separate deterministic project CI
job, adapt the domain rubric and require the appropriate observed check names.

## Updates and recovery

Pin the template commit used for adoption in your rollout issue. Review updates as
ordinary PRs using `.agentic/template-origin.json` to compare installed versions.
Upgrade CLI versions and action SHAs together with their verification records.

If a push or API write fails, read current remote state before retrying. A task
worktree left after a failed push is useful recovery material; inspect its branch
and rerun the push deliberately. Never delete it merely because a command exited
nonzero. If a review fails or the head changes, keep its private diagnostic directory
and prepare a fresh snapshot.
