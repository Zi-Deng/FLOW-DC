# Pinned native authentication audit — 2026-10-01

This is static implementation evidence for issue #33, not authenticated execution,
live capability, or an independent model review of PR #34. No ordinary login,
credential file, authenticated CLI subcommand or inference was used.

Native Linux x64 2.1.282 binary SHA-256:
`3afe8535c0cc33f0e24f7b25dab7a1727b8b592196f8496a8bc302ba2161eed3`.

Offsets below are zero-based, end-exclusive byte ranges in the verified native
artifact. Range hashes help reproduce this inspection; they do not prove the
reasoning complete or establish runtime isolation. The signed release verification
still runs independently before any eligible invocation.

| Inspected boundary | Bytes | SHA-256 |
| --- | --- | --- |
| native synchronous reader dF | 197174800–197176200 | `ebcd0fc38240d1a870eac9f92398ca9c0aa7d203d3dfe48ec57a79a5baf98d2d` |
| native asynchronous reader gF | 197183394–197183705 | `a55faca81b948a7a2b7fccc5ad3b43f822fecacbe1b57cb7092e2f4be5d9011f` |
| refresh helper da early exits | 197187797–197188570 | `1b418c5ba7274ff42d12e32626969b44bfd89f9587e122297f525afe7b11edfc` |
| subscription reader ult | 197192450–197193250 | `dab7db41b67a2394eaf4285446d79c71f5bc5c05f073f1fb856fd3947361bf14` |
| remote eligibility MR | 198142660–198144051 | `9e1812424870fa8e4ce9f193ccec8a866ae6e7fdcec197b6509fd0df24aae3d4` |
| remote fetch early return | 209976418–209976830 | `7961bd447c0aacedad1cb9ccb6dffba33664b7fe7572241c74a4fc5c34d43a94` |
| login preAction and late helper registration | 210755580–210759150 | `321461a135ec33e813d4d0244f4993a8651c6e479d91b13943fdf510a09ac2d2` |
| native login post-auth ERe | 217019884–217022579 | `941f7ef76971a5a9481b64966b16fe99208f7779ba7d3d43d75bca5116707b36` |
| native expiry margin gD | 197218038–197218139 | `7486efab2c9004c843efc00fd8d9608447e8acf883be96cf36a5a231f5703e69` |
| fallback guard nee | 196907415–196907570 | `97b8f925f1b9d600a80fecc117948d71df749556ef2093bbdd875973b33aba1e` |
| refusal fallback WM | 200117455–200117630 | `ce4b5ee4bc19898f27fda2c80f410425e7d81e321b8917055959a90b7914bc81` |
| refusal retry Ya | 209691660–209691850 | `83c273b04c3880094e7996cf112ed5b7d7ebc877d09569f2ff028c2f08b71847` |
| nonstreaming fallback gate | 203385940–203386310 | `3be8fe18a1ce701abf31a21adcfa62e4cc9d3d83e353dc120b8be1213c024dd4` |

The actual native reader requires accessToken and returns the native record. Its
subscription reader uses that record, not a status-display fallback. With actual
Max metadata, no API/Console/profile/injected auth/endpoint/secure-storage overrides
and first-party defaults, the remote-policy decision is unsupported_subscription.
The remote fetch then returns ineligible before delivering policy. This conditional
reasoning does not suppress endpoint-managed controls. Unreadable endpoint paths
are refused with lstat; this is not an administrator override.

The expiry helper uses milliseconds and a 300-second margin. The refresh helper
returns no_refresh_token before lock/network when the refresh field is absent.
Snapshots omit the field entirely; no refresh or commit-back is implemented.
The bounded native 401 behavior is distinct from a new wrapper inference attempt.
No immediate-first-401 or exact request-count claim is made. Controlled native
error-path execution and complete cache/reset transition qualification are missing.

The retained fallback controls are pinned internal compatibility, not promises of
a stable public API: nee reads the boolean model-fallback flag; WM excludes refusal
fallback when either refusal fallback is disabled or nee is active; Ya excludes
refusal retry with its disable flag; the stream error branch includes the explicit
nonstreaming-fallback disable value. Finding these branches is necessary but does
not establish that every fallback pivot or settings-precedence path is covered.
CLAUDE_CODE_MAX_RETRIES was removed: it cannot promise first-401 termination.
No identity, remote-policy suppression or auth-retry-disable control was added.

## Setup activation blocker

The auth command has some preAction exemptions, but the same preAction still arms
DRo and invokes managed-policy handling before its browser handler. Native login
stores the selected identity through ERe; vYn invokes i2t, which resets remote
eligibility/cache state. The wrapper cannot inspect and reject a non-Max selection
until native login returns. It has no verified before-effect barrier across those
callbacks or late helper handling. A user intending to choose Max does not establish
that barrier, nor does a post-login record check. The setup helper therefore refuses
before creating state or authenticating. No private recovery prose or operator-owned
metadata is promoted into proof of this native transition.

No safe callback control has been established for this pinned version. Completing
that verification, or obtaining a supported native barrier under the approved
contract, is required before removing the gate. Do not manually import credentials,
fabricate Max metadata, call authenticated doctor as a probe, disable administrator
policy, or change the CLI pin as a workaround.

## Deterministic evidence and its limits

test_claude_native_auth uses synthetic native records to exercise strict identity
and lifetime checks, owner/mode/link/type/lock guards, generation/receipt integrity,
minimal snapshots, hostile environment exclusion, and cleanup on interruption.
A fake native child tests the guarded setup lifecycle. It is not a real login and
does not qualify the callback path. Parser and transport tests preserve exact
terminal text and frozen token-mode history. All actual capability diagnostics
remain blocked and the existing two-attempt allowance is unchanged.
