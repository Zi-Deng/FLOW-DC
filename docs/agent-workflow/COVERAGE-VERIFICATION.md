# Coverage implementation research — 2026-09-29

This is a new implementation/verification record for issue #29. Earlier adoption,
provenance and PR #27 review records remain unchanged and retain their original
limitations. This record does not retroactively certify those reviews.

## Primary references and supported boundary

- The [Copilot SDK event schema](https://raw.githubusercontent.com/github/copilot-sdk/main/nodejs/src/generated/session-events.ts)
  describes correlated tool start/completion events and model-facing result content.
  [SDK issue #2211](https://github.com/github/copilot-sdk/issues/2211) explains why
  a completion must be correlated with its start to obtain the tool name/arguments.
- [CLI issue #4107](https://github.com/github/copilot-cli/issues/4107) documents a
  terminal stdout result with numerical usage. [CLI issue #4572](https://github.com/github/copilot-cli/issues/4572)
  includes actual session events; [CLI issue #4911](https://github.com/github/copilot-cli/issues/4911)
  documents failure behavior. These reports do not prove local 1.0.83 compatibility.
- [gh-aw issue #50937](https://github.com/github/gh-aw/issues/50937) contains stdout
  tool events without correlation IDs. Therefore stdout alone is insufficient.
  The wrapper supplies a new `--session-id` in a fresh temporary home and reads only
  that session's bounded `session-state/<UUID>/events.jsonl`. The session-start ID,
  single-session directory and actual call IDs must match. No temporal pairing is
  invented. CLI stdout separately supplies terminal completion framing; explicit
  nonzero terminal exit codes fail even if the process exited zero.
- Local installed 1.0.83 `--help` documents `--output-format json` as JSONL and
  `--session-id` for a new UUID. This is configuration evidence, not tool capability.

The adapter currently supports complete single `assistant.message` responses and
matching CLI terminal output. A `chunkCount` other than one is explicitly unsupported
and disqualifies coverage; fragments are not guessed or silently concatenated. Unknown
provider events/renderings fail closed. Safe event-type/shape counts help diagnose
format failures without retaining source text, reasoning or raw session records.

## Synthetic regressions and live evidence

`tests/agentic/review_fixtures.py` generates representative synthetic events and
numbered tool renderings based on those public shapes. The tests exercise call
correlation, fixture success/failure, missing/truncated evidence, forged ranges,
coverage accounting, repair ancestry, transport and recovery. They are **not** a live
capability demonstration, independent review or scientific validation.

Before the fixes, regressions reproduced nonempty/manual-report readiness and report
whitespace loss. A separate base regression models the observed `gh api` ESC/BEL
transformation; exact transport now avoids terminal rendering. The coordinator's
read-only comparison of saved PR #21 review 5343819047 confirmed that authenticated
JSON preserves 20 ESC and 20 BEL characters and matches the original publication.
That comparison used zero model requests and confers no coverage qualification.

No paid canary or independent Opus review was run by the implementation executor.
The one authorized final request remains the coordinator's responsibility under
trusted clean-main policy. Do not activate arbitrary unmerged PR hooks/configuration
or spend extra rounds to work around an unsupported telemetry format. A failed live
probe is a visible blocker, not permission to infer capability from these fixtures.

Full local/hosted validation results must identify their exact committed head. The
executor's focused workflow checks do not replace application validation or both
required hosted jobs. Keep unknown/skipped/stale/failed checks visible. Neither
observed reads nor green tests establish understanding or scientific validity, and
the local owner can rewrite private journals and hashes.

## Repair evidence after the first review — 2026-09-29

The preceding implementation-stage request status is historical. The coordinator
subsequently published [the first Opus review](https://github.com/Zi-Deng/FLOW-DC/pull/30#pullrequestreview-5357413972)
at `db525b6b1883147af308a38cbc89f6f2dee5cb74` and
[assessed its findings](https://github.com/Zi-Deng/FLOW-DC/pull/30#issuecomment-5897261833).
It remains INCOMPLETE. Its exact fenced response, 318 rows for 319 obligations,
9 claimed inspected rows, unknown-event counts and old qualification are preserved.
The provider exited zero and recorded about 329 seconds of API duration. The report's
claim of budget exhaustion is attributed model text, not an observed failure cause.

F1's unnumbered view failure was reproduced using only the generated fixture's actual
model-facing content from pinned 1.0.83. The minimal, non-sensitive record is
`tests/agentic/fixtures/copilot-1.0.83-view-canary.json`; its content digest is
`2fc36a2cf49359deb48d1a30b6e39ec5c48652c28d8b330629eb4b6f8ce318da`.
No ordinary source result, provider identifier, credentials or raw session is retained.
Grep and glob probes had passed in the original run; F1's broader claim was unsupported.
The repaired adapter matches exact whole-file text or an unambiguous exact requested
slice. Range, duplicate-content, blank-line, CRLF, Unicode, truncation and outside-packet
cases are synthetic regressions, not new live capability evidence.

F2's suggested ordinary non-UTF-8 Git-blob trigger is already excluded in `snapshot`.
Injected post-construction UTF-8 and IO failures did reproduce lost telemetry. Strict
failure reasons now survive without replacement decoding, and an exact capture precedes
assessment reads so transient assessment failure can recover without another invocation.
F3 retains the intentional verified-version boundary; installer, wrapper and new gate
share the explicit pin. Historical schema-1 assessment remains frozen separately.

The current [SDK schema](https://raw.githubusercontent.com/github/copilot-sdk/main/nodejs/src/generated/session-events.ts)
documents root `subagent.selected` and `system.message`, and distinguishes a delegated
instance by top-level `agentId`. Synthetic tests restrict selection to the exact named
reviewer and three tools, discard system content, and reject delegation/permission
expansion. These types are research-supported, not an identification of the two unknown
types from the first run: those names were not retained. New unknown names produce
bounded digests/counts and still block qualification.

Schema-2 compact positive inspection claims bind the unchanged complete inventory.
Every omitted claim is materialized unread. Bare JSON and one strict outer JSON fence
parse without changing any response bytes. Metadata/result schema 3 separates the new
policy from old schema-2 records, whose original assessment/publication is reproduced
by frozen policy. Migration, recovery and readiness regressions cover both historical
qualified and malformed reports. A changed head still requires fresh exact-head checks
and independent review. The supported P1 authorizes one repair-verification round under
the existing policy; only the coordinator may perform that phase, within the unchanged
400-credit / 900-second cap. No new model invocation was made by this repair executor.
