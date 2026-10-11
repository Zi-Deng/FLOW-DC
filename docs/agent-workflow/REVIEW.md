# Independent static review

Use `workflow.py review PR --issue N --plan-comment CURRENT --publish` from the trusted workflow installation. The coordinator fetches immutable Git blobs and the current task contract; no private memory, credentials or executor conversation enters the snapshot. Historical process comments are not automatically added. Omitted binary, historical, oversized or unsupported material is listed.

Claude Code is the default; Copilot is explicit. The provider receives a fresh snapshot with only read/search/list tools, no test execution, edits, shell or delegation. Tests run separately. Record requested provider/model/effort, actual CLI version, head/base, invocation count, elapsed time, available usage, findings and limitations. No fallback or automatic model replay.

A completed report contains evidence-backed findings and its reported inspected paths/limitations. That scope is a reviewer assertion, not mechanically proven exhaustive coverage. No percentage, clean report or passing test establishes scientific validity. There is no required whole-repository census, tool-canary campaign, integration batch or report-version replay.

Publication creates a COMMENT review on the recorded head. If head/base changes, publication refuses. Repeating publication observes the existing report marker and does not invoke a model. A native failure, quota, malformed output or timeout is retained as failed; diagnose before a separately bounded attempt. Existing authorization suffices for authorized continuation.

Review severity: P0 catastrophic; P1 major correctness/security; P2 meaningful defect/evidence gap; P3 minor optional improvement. Give original location, concrete trigger/impact, supporting code and a minimal fix. Suppress style findings already covered by tooling. The maintainer assesses findings before merging.

## Bounded review and repair policy

Use at most two automated review invocations and two review-driven repair rounds per task across providers; failed or interrupted invocations count. Plan the first review after implementation qualification and the final review after the remaining material changes. Ordinary development/test fixes and coauthor revisions are not extra review-driven repairs. After the last repair, report the last reviewed SHA, final SHA, changed delta and current-head CI for human assessment; never label an earlier review as final-head review. No automatic third review. Shelve remaining nonblocking findings in a concise existing or consolidated GitHub issue. Material acceptance failures remain blockers. Extra cycles require a documented credential compromise, data loss, uncontrolled spending, or defect invalidating required central evidence, and a bounded corrective scope. Existing finite provider limits and human-only merge/submission still apply.
