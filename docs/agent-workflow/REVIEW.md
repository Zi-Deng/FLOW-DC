# Independent Copilot review

The reviewer receives committed artifacts in a new Copilot CLI process, without the
implementation conversation or private memory. It performs static inspection through
literal `view`, `grep`, `glob`. The wrapper performs Git/GitHub operations outside the
model process. Read [the coverage contract and migration runbook](COVERAGE.md) for the
packet, adapter, evidence schema, recovery and all readiness gates.

## Managed skill procedure

Use `$agentic-review PR #456` from the clean trusted control checkout. The coordinator
prepares the approved issue/plan and fixed head/base snapshot, invokes the configured
`claude-opus-5` once and publishes a COMMENT review. Use `$agentic-repair` to resume
the original Astra executor UUID for repairs. See [SKILLS.md](SKILLS.md).

One attempted round is the default, including failed or incomplete attempts. A supported
critical P0/P1 finding permits a further round to verify its repair; record the public
finding and concrete reason. Other extra rounds require explicit user continuation.
P2/P3 findings, uncertain questions and incomplete coverage do not automatically permit
another request. Never silently switch models, broaden permissions or retry inference.

## Local procedure

```bash
python3 scripts/agentic/review.py prepare 456 --issue 123 --plan-comment 987654321
python3 scripts/agentic/review.py run /absolute/path/printed/by/prepare
python3 scripts/agentic/review.py publish /absolute/path/printed/by/prepare
python3 scripts/agentic/review.py qualify /absolute/path/printed/by/prepare
```

Preparation paginates the public contract, discussions, reviews, check runs and status
contexts. It reads Git blobs without checkout, filters or project execution. Small
contract artifacts and deterministic component scopes lead to required code, tests,
findings/repairs and cross-boundary material; complete source and diff remain available.
A repair packet may add `--prior-review DIRECTORY`. Its validated ancestor report
provides delta-first navigation while preserving current original scope and prior
uncovered obligations. It cannot inherit qualification for the new head.

`run` performs its actual-tool capability probe inside the same request and records
sanitized events. It returns exit 2 when a report is saved but coverage is incomplete.
Publication retains useful findings with an explicit incomplete label. Only verified
complete material can be designated by the managed pipeline or pass low-level/finish
readiness. Findings still require human disposition. Model reports never impersonate
human approval or resolve threads automatically.

## Isolation and budgets

Snapshots map original paths to numbered inert `.txt` blobs. PR agent profiles, hooks,
skills and configuration remain data. Active policy/profile come from the trusted
clean default-branch checkout. The process has a temporary home, fresh Copilot/XDG
state, disabled hooks/MCP, no inherited provider override, no prompt memory/resume,
no permission to execute, edit or delegate and no broad `*` permission. Authentication
is supplied separately in the token environment. Workspace hashes detect changes.
These controls restrict model tools/config discovery; they are not an OS sandbox
against a compromised CLI executable.

Private/data exclusions include `memory`, `.agentic-local`, credentials, `files/input`,
`files/output`, `files/biotrove_train_stats.json`, `benchmark/manifests`,
`benchmark/results`, `playground` and `archives`. A diff touching them is refused,
including deletions/renames. Symlinks/submodules are never followed. Maintained example
configs and benchmark code remain inspectable. Path exclusions are not a content-based
secret detector: inspect public source and comments for sensitive material.

`max_diff_bytes` is unlimited when null/omitted; an explicit positive integer opts into
a hard pre-packet cap. There is no silent diff truncation. Per-source-file limit is
250000 bytes and the combined head/base/carried-source budget is 12000000 bytes.
Scopes are navigation within one request, not additional paid rounds. Existing limits
remain 900 seconds and 400 Copilot AI credits; in-flight provider requests can overshoot
credits. Large reviews may still exhaust time/context/credits and remain incomplete.
The separate managed executor prompt limit remains 300000 bytes.

The pinned CLI is 1.0.83. Requested configuration, help and synthetic fixtures do not
prove live tool availability. Successful actual canary calls and supported events are
necessary. An unknown layout yields a durable incomplete result and actionable reasons;
it never triggers a broader permission workaround or automatic paid retry.

## Exact storage, publication and recovery

`review.md` contains exact model-response bytes. `review-result.json` journals them with
sanitized diagnostics before final storage. Retry storage with the same directory only
when that journal is valid; recovery and completed-result reuse invoke no model. A
started attempt without a valid journal needs investigation, not a blind rerun.
Reports exceeding the 60000-byte publication limit remain intact; the prompt requests
less than 50000 bytes but that guidance is not a guarantee.

GitHub publication uses exact UTF-8 JSON transport, an attributed coverage label,
explicit head SHA and an idempotence marker. Reconciliation compares the whole body,
head and state. Missing/changed diagnostics, altered reports or stale head/base fail.
Legacy records stay readable but cannot acquire new qualification. Use
`review.py verify-publication DIRECTORY` for a read-only exact-byte comparison with
GitHub, including historical control characters; it performs no model request.

Raw provider homes, reasoning, unrestricted logs, tokens and environment dumps are not
archived. Bounded sanitized diagnostics and known usage counters survive failures.
Local state directories use owner-only access and atomic records use mode 0600. The
owner can rewrite private records and hashes; they are not independent attestations.

## Report contract

Return JSON matching `report-schema.json`: version 1, findings, coverage and limitations.
Every required ID must have a reviewed/unread/unsupported row, inspected packet line
locations and an explicit reason when incomplete. The wrapper correlates locations
with actual successful tool results. A checkmark, percentage, file listing or diff
header cannot replace source/test inspection. Observed reads do not prove understanding.

| Severity | Meaning |
| --- | --- |
| P0 | Concrete catastrophic merge blocker |
| P1 | Likely major correctness or security failure in supported use |
| P2 | Reachable edge-case defect or substantive evidence gap |
| P3 | Minor optional improvement; suppress tooling-covered style remarks |

Every finding needs an ID, severity, original path/line, claim, trigger, impact,
evidence and minimal fix direction. Empty findings are allowed when no material defect
is supported. State uncertainty and unsupported acceptance evidence honestly. Never
invent executed commands. Apply [the domain rubric](domain-review.md) to scientific
changes: green software checks are not scientific validation.

## Static versus executable validation

`validation.json` contains separately attributed, point-in-time CI observations and
available hosted execution receipts. Each receipt distinguishes PR head association
from the actual tested checkout/merge SHA. Unknown, missing, skipped, failed, stale or
expired observations stay visible. Required CI still must pass independently. The
reviewer executes no tests and receives no model shell access.

CI runs PR code only on disposable hosted runners without model/cloud secrets.
Never execute unfamiliar PR code on a credential-bearing workstation as a substitute.
For this implementation the coordinator separately runs the local suite in its existing
allowed environment because the executor sandbox denies local test-server sockets.

## Manual Actions procedure

Complete [the protected environment setup](SETUP.md#6-hosted-review-is-opt-in), then
run the default-branch workflow with PR, issue, designated plan and exact head:

```bash
gh workflow run copilot-review.yml --ref main \
  -f pr=456 -f issue=123 -f plan_comment=987654321 \
  -f head_sha=FULL_CURRENT_PR_HEAD_SHA -F publish=false
```

The model job is read-only; publication runs separately with PR write permission and
no model. Neither executes PR code. Sanitized artifacts upload on failed runs too.
When publication is requested, an intact partial report can be published after an
incomplete run, but the qualification step keeps the workflow nonpassing. Same-repo
PRs targeting the default branch are supported. No comment-triggered repair loop or
self-hosted runner is enabled.

## Model selection and repair

Request exactly `claude-opus-5`, not `auto` or another Claude version/family. A model
change needs deliberate policy/availability/budget review. Historical Sonnet/Fable
adoption records retain their original meaning; neither those runs nor mocked tests
prove Opus access or current capability. New packets freeze the trusted model/budget.

This PR changes review machinery. Unmerged PR code/instructions do not become trusted
review policy automatically. The maintainer's existing request permits a documented
narrow literal-tool/capability diagnostic invocation if necessary, not arbitrary PR
hooks/configuration or extra requests. Record live versus synthetic evidence honestly.
Repair stays on the original branch and Astra UUID. New commits invalidate readiness;
continuation authorization and the existing review allowance still apply.
