# Higher-concurrency real-host study

Issue #43 implements one current runner, separate from the completed October17
study. Its private frozen scripts and evidence remain unchanged.

Ratio/Gradient retain the selected decision rules/options, use minimum2, initial4,
maximum256 and256 downloader tasks. Actual in-flight requests are measured from
header dispatch through HTTP completion/termination, including body reading.
The safety ceiling is not literally unlimited concurrency. Report if it binds.

The design is a300-URL sequential health probe, four8,192-row adaptive calibration
runs, six balanced Ratio/Gradient pairs, then six balanced Fixed-low/high pairs:
28 planned runs, one logical attempt per row, disjoint URLs excluding old requests.
Calibration prospectively selects equal main row counts targeting120s, forecasting
at least60s and fitting the population/byte limits. No post-outcome retuning.
Measured calibration verification/finalization cost also constrains the main row
count before main outcomes. Execution reserves 300s acquisition, 180--300s
verification, 120--180s finalization and a 30s margin; actual earlier completion
can permit a second run in the same window. Infeasible calibration or unavailable
Fixed selection ends the protocol with retained results and unrun cells.

Fixed-low uses80% of the largest clean observed concurrency, also below an
observed429 dispatch cap if present. Fixed-high uses that cap or, without429,
150% of time-weighted p95 actual concurrency, bounded by256. A stress candidate
is not proof of a server quota. Clean ten-second windows require100 completions,
backlog and median application first-body delay<=1.5x probe baseline, no429/503,
and transport failures<1%. The source contains the exact deterministic rules.

All Fixed runs follow all adaptive first attempts. Primary inference is the six
paired Gradient–Ratio differences: two-sided paired t, ordinary95% intervals,
exact paired randomization sensitivity, two one-sided alpha.025 safeguards
(1percentage point coverage,10% run-level p95 latency). Comparisons against later
Fixed are descriptive and confounded with time/phase. Requests are not replicates.

Stops: five429/503s,1% rolling refusals after200 responses, or three qualifying
10s latency windows>=3x probe baseline; honor Retry-After and10min cooldown across
processes/windows. Two consecutive refusal stops terminate the host; TLS or401/403
terminates immediately. Known content anomalies/dead links also retain the
previous5%/20% campaign-stop thresholds. Interrupted/unknown work is incomplete,
not silently zero-imputed. No replacement, proxy, IP change or automatic replay.

The `bounded-external-v1` profile permits32,768rows,4GiB expected payload,8GiB
observed bodies/archive,64MiB/object and300s acquisition. Previous profiles retain
their meanings. Native journals, hashes and original denominators are checked by
an independent verifier. Image credit additionally requires decoding. Closed
archives remain authoritative; verified loose payload duplicates can be removed
with a compaction receipt, preserving journals/index/overview. Never resume or
reconcile those archived invocations. Third-party payloads remain private.

## Offline preparation

Use the qualified Python3.12 environment and a clean committed source worktree:

```bash
python -m benchmark.real_server_study prepare \
  --source /absolute/source --study /absolute/private/study \
  --previous /absolute/private/october17-study
python -m benchmark.real_server_study cloud-prepare \
  --source /absolute/source --packet /absolute/private/packet \
  --study /absolute/private/study --state /absolute/existing/controller-state \
  --allocation /absolute/private/allocation.json \
  --ssh-config /absolute/private/trusted-ssh.conf \
  --guest-python /absolute/existing/guest-python3.12
```

Allocation JSON: `allocation: CIS240296`, `expiry_date: 2027-04-17`, numeric
`cpu_su` and timezone-aware `observed_at`. If the maintainer reports the current
balance without the portal observation time, preserve `observed_at: null` and
record the receipt time as `reported_at`, with that distinction in
`observation_basis`. Preparation can preserve an unknown balance; execution
requires>=1,000SUs and an observation or current-balance report within24hours. No secrets.
Preparation downloads pinned binary wheels and binds source/private-input hashes;
it never activates guests or applies a grant.
Execution rereads the allocation file and saves a hash-bound observation receipt;
`cloud-bootstrap` and `cloud-run` also accept `--allocation` to select a refreshed
file. The frozen preparation snapshot is not rewritten.

## Explicit cloud deployment/execution

```bash
python -m benchmark.real_server_study cloud-bootstrap --packet /absolute/private/packet
python -m benchmark.real_server_study cloud-run --packet /absolute/private/packet
```

These activate real billed resources. Require human-merged source, current
allocation facts, existing supervisor/registration, trusted SSH, guest Python3.12
and noninteractive sudo/systemd. Only the first worker downloads from the moth
host; manager and origin-role guests support the existing lifecycle. No TaskVine
cluster or local acquisition fallback is used. The supervisor is not upgraded.

Bootstrap applies the finite grant, activates one window, creates a campaign-owned
256GiB volume, attaches it only to the selected worker, stages an isolated pinned
environment/source, probes and cleans up. Ownership, exact full/truncated volume
serial from `lsblk`, size and filesystem identity are checked. The helper adopts
only an empty filesystem on the newly campaign-created volume (featured guests
can automount it); it never formats a filesystem or replaces an unrelated mount.
Later windows require the recorded filesystem UUID. Mutation intents are
retained before dispatch; uncertain creation/attachment is not automatically retried.

One eighteen-hour envelope,<=36 thirty-minute windows,600s/window cleanup reserve,
640SUs operational ceiling and1,000SUs controller authorization. The
latter includes unused cumulative allowance from the expired campaign and is not
an expected bill. Preserve all prior grants/consumption/history/obligations.
Stored rates total32SU/h; fresh identity/rate checks precede deployment. Provider
delay is not a hard billing guarantee. Every exit requests cleanup and verifies
fresh `SHELVED_OFFLOADED` and restored networking.
The capacity check covers one worst-case run per window, separate bootstrap,
analysis and collection windows, plus five contingency windows. More runs can
share a window when the complete next-run reserve fits. This ceiling replaces
the initial zero-slack 16-window proposal; it grants no additional acquisitions.
At the stored rate, eighteen hours is 576SUs before provider overrun, below the
640SU ceiling ([provider rates](https://docs.jetstream-cloud.org/general/instance-flavors/)).
The last windows are reserved for terminal analysis/collection if the
planned runs have not finished. The unchanged offline auditor can use the time
remaining in the original guest lease after cold setup, without extending that
lease or starting acquisitions. Metadata transfer uses at most900s, clipped to
the remaining work time with its cleanup margin intact.
Lease-limited probes, interrupted finalization
and analysis-audit timeouts retain explicit noncreditable outcomes; they never
authorize a replacement acquisition or an extra window.

Keep<=160GiB total campaign-volume use and40GiB free reserve; admission reserves
20GiB of peak space for the next run, counting loose duplicates and journals.
Raw evidence remains
there after offload; no automatic evidence-volume deletion. Only bounded metadata
and analyzed results are collected locally. Source changes, interrupted attempts,
time/storage stops and missing evidence remain explicit; do not create extra runs.
Metadata transfer is bounded to8GiB/900s with16GiB local peak space for transfer
plus extraction, preserving40GiB free. Interrupted transfers can be replaced;
closed transfers/extractions are reused, and PDF retries use collected files
without VM activation. An incomplete derived analysis may be superseded by a
full re-audit of the same retained first attempts; completed analyses remain fixed.

## Qualification and reports

```bash
python -B -m unittest discover -s tests -p test_real_server_study.py -v
ruff check benchmark/real_server_study tests/test_real_server_study.py --select E9,F
make check
```

Fixtures bind only to loopback; they do not establish external efficacy or cloud
cleanup. The guest analyzes retained archives/receipts and observation hashes;
the coordinator builds a private LaTeX/PDF. Explicit offline `analyze` and `report`
commands never acquire images. This study does not replace the October17 findings
or establish distributed scaling, independent-human reproduction or submission readiness.
