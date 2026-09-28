# Manuscript research contract

This contract frames a proposed evaluation for **IEEE Transactions on Big Data**.
Gradient PAARC is the proposed primary method; it is not yet a validated scientific
contribution. There is no approved submission deadline or advisor endorsement in
this record. Advisor decisions about hypotheses, baselines, scope and venue fit
remain pending. This document does not edit or replace the user's manuscript.

The bounded source increment is [issue #20](https://github.com/Zi-Deng/FLOW-DC/issues/20)
and its [approved implementation plan](https://github.com/Zi-Deng/FLOW-DC/issues/20#issuecomment-5876154148).
It addresses shared asynchronous HTTP measurement and admission correctness plus
local validation. The [validation record](HTTP-MEASUREMENT-VALIDATION.md) separates
observed results, blocked execution and work still required.

## Candidate claims and evidence gates

Every row is a hypothesis to test, not a result. A passing software check cannot
close a publication gate.

| Proposed claim | Comparisons and interventions | Required measurements and evidence | Current gate |
| --- | --- | --- | --- |
| Gradient feedback improves the goodput/latency tradeoff under changing origin conditions. | Gradient PAARC versus base PAARC, matched fixed concurrency and a precisely specified ratio-based controller. Tune comparators with equal budgets. | Useful saved bytes and acquisitions per second; application body-first-byte distributions; makespan; failures, timeouts, overload and server pressure; independent repeated runs with uncertainty. | Timing/classification calibration, ratio baseline specification, gradient equations and tuning protocol remain open. |
| The gradient mechanism causes any observed improvement. | Matched ablations of gradient shaping, smoothing, confidence/sample gating, RTprop refresh and overload recovery. Keep shared HTTP correctness and mandatory Retry-After admission enabled in every comparison. | Effect of each intervention on useful goodput, latency, concurrency trajectories, response to load changes and recovery; repeat across independently controlled conditions. | Mechanism definitions, parameter matching and ablation implementations remain open. No new equations or tuning are authorized by issue #20. |
| Benefits persist under distributed scaling and host imbalance. | Balanced and skewed host/partition allocations at **1, 2 and 4 workers**, comparing gradient, base, fixed and ratio methods at matched per-worker and aggregate resource budgets. | Strong-scaling speedup and efficiency for fixed work; per-host/per-worker useful goodput, skew, contention, origin load, coordination overhead and outcome coverage. | The current operational route does not establish this worker matrix. Distributed gradient integration and 1/2/4-worker support remain open and outside this increment. |
| Recovery preserves useful output and accountable costs. | Controlled overload, interruption, retry and recovery cases across methods, with equivalent fault schedules and output validation. | Payload identity and completeness, duplicate/lost output counts, retry attempts, failed/censored runs, wall time, resource/cost ledger reconciliation and verified cleanup. | Output-collision/integrity repair, end-to-end accounting validation and distributed recovery evidence remain open. |

## Experimental units and reporting

- Treat an independently started run as the experimental unit. Requests within a
  run share origin and controller state and are not independent replicates.
- Block comparisons by manifest/host mix, origin schedule, worker allocation and
  comparable network/time conditions. Randomize method order within each block;
  retain the randomization seed, source SHA, configuration and environment manifest.
- Separate tuning and evaluation workloads, conditions and seeds. Freeze the
  method specification, budgets, primary outcomes and stopping rule before
  evaluation. Select replicate counts using a precision/power rationale agreed
  with the advisor, not by stopping when a favorable result appears.
- Report run-level estimates, variability and uncertainty with paired/block-aware
  comparisons where warranted. Distinguish exploratory plots from prespecified
  claims. Record negative results and all tested conditions.
- Retain failed, cancelled, timed-out and censored outcomes with reasons, elapsed
  exposure and attempted/completed work. Define every denominator; do not remove
  unsuccessful acquisitions from throughput, recovery or cost accounting silently.
- Keep delivered/saved useful bytes separate from response traffic and retries.
  Verify output contents before crediting useful work. A successful process exit
  or a generated tar archive alone does not establish dataset integrity.

## Measurement prerequisite

Base and gradient must consume the same calibrated application-observed body-first-
byte signal. Document monotonic request dispatch, final-response headers, first
nonempty body read and completion separately, including redirect attribution and
admission/connector waits. This signal is not packet RTT or a direct measurement of
network queueing. Empty/failed acquisitions must not fabricate successful latency
samples. Saved-output success, overload feedback and latency eligibility are
distinct outcomes, with accountable totals.

Retry-After must establish an authority-specific shared monotonic deadline within
one asynchronous run, including fixed mode, retries and redirects. Numeric and
HTTP-date headers, invalid inputs, concurrent extensions, independent authorities,
timeouts, cancellation and permit recovery require controlled fixtures. This is
not a cross-process or cross-VM politeness guarantee. Updated timing/report semantics
must be labeled; old measurements cannot be pooled with corrected measurements
without establishing comparability.

## Functional evidence is not publication evidence

The previously reported **64-image, one-worker PAARC-toggle run** is functional
smoke evidence only. It does not establish gradient efficacy, a fair baseline,
distributed scaling, statistical significance or journal readiness. Prior ad hoc
timing and Retry-After probes are diagnostic leads, not new benchmark results.
Ephemeral localhost regression tests establish only the exercised software
invariants under their recorded environment and timing tolerances.

Still-open scientific gates include output integrity/collisions; benchmark adapter
fairness and equivalent retry/output semantics; a frozen gradient/ratio specification;
mechanism ablations; tuning/evaluation separation; distributed integration; balanced
and skewed 1/2/4-worker studies; and independent reproducibility plus domain-owner
review. No agent may mark advisor decisions approved.

Issue #20 does not authorize cloud activation or runtime installation, new permissions
or dependencies, workflow changes, grants, dataset/performance campaigns, benchmark
adapter redesign, multithread parity, cloud-upload mode, worker-matrix implementation,
manuscript rewriting or merge. Those require subsequent task scope and evidence.
