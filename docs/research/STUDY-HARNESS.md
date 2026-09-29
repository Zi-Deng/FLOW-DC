# Controlled-origin and retained study tools

This is engineering software for milestones A–C of issue #26. Local V1 fixtures
test state/accounting; V2 requires the real localhost executions described below.
There is no efficacy result, frozen scientific protocol or live cloud authorization
in these files. Advisor approval has been reported, but the specific decisions are
pending. Distributed admission, native TaskVine and the offline topology ladder are described
in [the milestone D contract](DISTRIBUTED-WORKFLOW.md).

## Origin contract

`benchmark/core/controlled_origin.py` implements a concurrent HTTP origin with
explicit nonpreemptive service slots and a FIFO queue. Arrivals either obtain a
slot, join the bounded queue, or receive 503 plus Retry-After. Each object has a
predeclared service duration, original bytes/hash and response sequence. Service
includes that duration and the write to the response socket. A capacity reduction
does not erase work already in service: new service waits until outstanding work
falls below the reduced limit. Recovery can start queued requests immediately.

The origin records arrival, admission, service start/end, capacity transitions,
response status, bytes written and observed disconnects. A failed socket write has
unknown partial bytes rather than invented zero bytes. Events use the origin's
monotonic clock; scheduled offsets start at that origin's first request arrival.
There is no subtraction between client and origin epochs. A separate independent
event replay checks FIFO admission, capacity at service start, unique closure and
the observed request count. This checks the service mechanism, not the controller's
efficacy. Duplicate URL requests retain aggregate path counts without invented row
attribution.

Each cell creates fresh origin state, generated valid JPEG/PNG payloads, source/
environment/scenario/catalog hashes and an immutable original-row fixture. The
client is the real maintained downloader with the explicitly selected method,
research metadata precondition and independent artifact verifier. Native output,
attempt journals, control trajectories, stdout/stderr, archives and common results
remain together in the attempt directory.

The gradient and ratio study configurations explicitly select a 1.5-second fresh
observation window; standalone defaults retain per-tick batches. This allows
several sparse ticks to supply one fresh eligible batch without reusing samples.
See [the method contract](CONTROL-METHODS.md) for expiry, probe filtering and the
remaining effectively fixed regime at too-low completion rates.

| Scenario | Defined mechanism |
| --- | --- |
| `steady` | Four service slots, 16 queued requests, 30 ms per object |
| `drop-recovery` | Four slots, drop to one at 0.6 seconds, restore four at 1.2 seconds; old service drains |
| `mixed-sizes` | Original small JPEG/PNG and seeded 257×257 noise PNG; service `0.02 + min(0.2, bytes/1e6)` seconds |
| `balanced` | Two independent origins, alternating rows |
| `skewed` | Two independent origins, one tenth of rows sent to the second |
| `overload` | Queue bound two; first request per path is 429 or 503 with 0.1-second Retry-After, then 200; two total native attempts allowed |
| `sparse-interrupted` | At most eight rows, 0.5-second service, one path returns a truncated body |

Defaults are engineering stimuli, not advisor-selected network models. Queue
bounds are at most 32, service slots at most 16 and object service at most one
second. Shutdown cancels queued work and wakes service waits, then joins handlers
and server threads. The localhost/guest entrypoint binds IPv4 loopback only; a
guest can be reached through an explicitly configured authenticated SSH forward.
It makes no production firewall, credential or service changes:

```bash
.agentic-local/research-env/bin/python -B benchmark/study.py origin --scenario steady --seed 20260929 --seconds 60 --output benchmark/results/origin-001
```

Read the new directory's `ready.json` for its endpoint. Without a client this is
only an origin startup check. Source/payload policies are recorded before serving.

## Frozen ordering and retained cells

The tracked machine-readable examples are:

- `benchmark/plans/provisional-evaluation-v1.json`: **unexecuted proposal**, six
  paired blocks × three families × four methods = 72 planned cells.
- `benchmark/plans/provisional-tuning-v1.json`: eight predetermined proposed
  configurations per method, with individual hashes and tuning identities.
- `benchmark/plans/engineering-smoke-v1.json`: one engineering block with a
  pre-generated order, for bounded semantic checks only.

`plan` shuffles family and method order within each block using an explicit seed.
Every block/family contains all four methods. The original plan is validated and
hashed before execution; changing cells/order/configuration is refused. Source and
environment hashes bind the study namespace. Tuning, evaluation and engineering
use different directories, logical IDs and derived fixture seeds. An explicit
`--method-configs` JSON may select parameters *before* generating a new plan; it
must retain all four comparators and common absolute bounds. Evaluation outcomes
never automatically select a configuration.

`run-cell` executes one cell per invocation and enforces the frozen preceding-cell
order. `--resume` reads an existing namespace; recorded success, failure, zero output
and interruption all remain recorded and are never automatically rerun. A deliberate
`--rerun` creates a distinct UUID and retains all earlier native/request work. The
summary uses the first planned attempt, never whichever rerun looks best. After a
crash, uncertain ownership or a live previous process group prevents continuation;
the harness does not issue blind process kills to recover it.

Every run has one downloader process, at most 256 rows and 64 MiB expected row
payload. The lifecycle has a 180-second deadline and 60-second cleanup reserve,
retains the group leader's PID until all group signalling is finished, and handles
INT/TERM by cleaning the owned group. Repeated signals cannot abort that cleanup.
SIGKILL/power loss cannot promise cleanup; recovery records that uncertainty.

Example first engineering cell (choose a new study root; resume later indices):

```bash
NO_ALBUMENTATIONS_UPDATE=1 WANDB_MODE=disabled .agentic-local/research-env/bin/python -B benchmark/study.py run-cell --plan benchmark/plans/engineering-smoke-v1.json --study-root benchmark/results/engineering-001 --cell-index 0
NO_ALBUMENTATIONS_UPDATE=1 WANDB_MODE=disabled .agentic-local/research-env/bin/python -B benchmark/study.py run-cell --plan benchmark/plans/engineering-smoke-v1.json --study-root benchmark/results/engineering-001 --cell-index 1 --resume
.agentic-local/research-env/bin/python -B benchmark/study.py summarize --plan benchmark/plans/engineering-smoke-v1.json --study-root benchmark/results/engineering-001 --output benchmark/results/engineering-001-summary.json
```

A `run-cell` exit 0 means the cell was recorded (or already recorded), not that the
native client succeeded. Inspect `native.status`, `native.run_complete`, the original
denominator, `origin-audit.json` and retained logs. Exit 1 is an interrupted/failed
harness invocation; exit 2 is a refused/unavailable prerequisite. The summary keeps
every planned cell, including pending ones. Ordinary unit discovery does not claim
these real executions ran.

## Calibration and precision

The `calibrate` command runs one fixed-client pair in seeded on/off order, with
origin event instrumentation on and off. Both use fresh origins/processes and
retain raw durations. Each native process gets 60 seconds plus 15 seconds cleanup;
the two invocations fit within the engineering process/cleanup envelope. Off mode
retains aggregate request counters and explicitly lacks event-level evidence:

```bash
NO_ALBUMENTATIONS_UPDATE=1 WANDB_MODE=disabled .agentic-local/research-env/bin/python -B benchmark/study.py calibrate --seed 20260929 --scenario steady --output benchmark/results/calibration-001
```

This tiny pair checks instrumentation/accounting and does not estimate publishable
overhead uncertainty. Controller logging remains on in both cases. No performance
superiority or power claim is inferred.

Paired summaries use independent runs within a scenario/block as replicates.
The engineering endpoint is verified original-row bytes divided by the common
launch-through-verification duration. Observed nonzero-exit/incomplete runs, including
zero useful bytes, remain in the planned cell. Missing/undefined cells prevent a CI;
timeouts/cancellations/uncertain cleanup are censored and also prevent a CI. Their
records and any defined observed mean are retained.

For at least six uncensored, defined pairs, the implemented provisional calculation
uses differences `d_i = candidate_i - reference_i` and a two-sided 95% Student-t
interval `mean(d) ± t_(.975,n-1) * sd(d)/sqrt(n)`. This is the standard mean interval
applied to independent run-level differences ([NIST definition](https://www.itl.nist.gov/div898/handbook/eda/section3/eda352.htm)).
Quantiles are evaluated by a bounded incomplete-beta inversion and checked against
reference values ([NIST table](https://www.itl.nist.gov/div898/handbook/eda/section3/eda3672.htm)).
Its assumptions are independent representative pairs and approximately normal
paired means; a tiny pilot does not validate those assumptions or estimate its
variance precisely. Requests are never treated as independent replicates.

The precision planner uses the observed paired-run SD once, then finds the smallest
total `n` satisfying `t_(.975,n-1)*sd/sqrt(n) <= relative_target*reference_mean`.
It cannot reduce below the observed count or six pairs, and caps proposals at
10,000 pairs. It reports insufficient/undefined/censored, zero reference mean and
infeasible-within-bound cases explicitly. A 5% target is only a proposal. It is not
a favorable-result stopping rule; future variance may differ. Repetition decisions,
estimand and acceptable failure/censor treatment require the advisor checkpoint.

## Human freeze checkpoint

Engineering checks do not freeze a protocol. Every provisional pilot or confirmatory
`run-cell` requires a `flowdc-frozen-protocol-v1` record bound to the exact plan,
source and environment. `approved=true` is insufficient. `freeze` requires an
explicit decisions file with the approved plan hash, decision provenance, constraints,
estimand and repetition rule. That is user-supplied attestation, not software proof
of advisor agreement. No such real record or decision file is shipped or generated
by this PR. Later source/environment changes invalidate the binding.

The later manual command is `benchmark/study.py freeze --plan PLAN --decisions
USER_SUPPLIED_DECISIONS --output NEW_PROTOCOL`; do not invoke it until those
decisions are actually supplied. No broad pilot, cloud activation, operational
migration or live allowance change was executed by these tools during implementation.

| Acceptance | V1 evidence | Remaining gate |
| --- | --- | --- |
| 4: concurrent service/capacity accounting | Pure FIFO, capacity drop/recovery, independent origin state and event-replay fixtures | Historical committed A–C scenario/calibration evidence retained; final-head replay required |
| 4: scheduling/recovery/cleanup | Seeded complete block tests, enforced order, collision/source refusal, distinct rerun IDs; real child-process TERM/descendant cleanup regression | Historical committed interruption check retained; final-head replay required |
| 5: plans/freeze/inference | Machine plans/catalog, namespace and hash tests; freeze refusal, t-quantile/paired CI and zero/censor/infeasible tests | Actual advisor decisions; later frozen scientific protocol/campaign |
| 6–8: distributed authority/topology | Separate shared-ledger, native staging/reconciliation and UUID migration/selection fixtures | Separate native 1/2/4-worker and operational gates; future production checkpoint |
