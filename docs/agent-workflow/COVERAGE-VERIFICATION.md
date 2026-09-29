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
