# Minimum measured public-source study

This is the current successor to the completed short public pilot. Its purpose
is to replace illustrative manuscript values with measured practical acquisition
results; it does not guarantee that FLOW-DC will outperform img2dataset.

Compare `ratio-v2` (PAARC-Ratio), `gradient-candidate-v2` (PAARC-Grad) and pinned
img2dataset1.47.0 on iNaturalist open-data S3 original JPEG renditions and CVDF
Open Images hosted train JPEGs. Use actual per-photo attribution and explicit
CC0/CC-BY iNaturalist metadata. The bounded, documented metadata prefix is a
cohort, not a representative sample of the entire publisher population. CVDF
and iNaturalist renditions are publisher-resized; both services use AWS.

The minimum evaluation is27 jobs: three methods × three independent matched
blocks × (iNaturalist one-worker, CVDF one-worker, iNaturalist two-worker). The
two-worker comparison uses the same manifests as the one-worker arm. Optionally
add three primary iNaturalist blocks for36 jobs, chosen from calibration cost
before evaluation outcomes. Preserve missing/stopped/changed-content cases.

Calibrate13 long jobs: two PAARC settings and three img2dataset configurations
per source, then all three selected methods on two workers. At most three size
correction jobs. Short512/2048-row engineering cases establish setup/per-row cost
and actual worker participation; they are never the long scientific evidence.
Target720s HTTP acquisition for the fastest configuration, intended minimum600s,
with matched fixed work. Do not pad runs or select successful URLs. If the finite
long profile,2400s whole-job deadline or total budget cannot accommodate the
design, stop before evaluation and report the feasibility blocker.

Size one-worker calibration only from one-worker engineering observations.
Fit setup plus per-row time from the512/2048-row pair for each candidate and
worker count; do not scale fixed startup cost as per-row work. Long calibration
updates the slopes while confirmation outcomes never alter the planning model.
Require25% headroom against each candidate's native, complete-job and body
budgets; a body-limited size still needs a predicted600s HTTP period. Persist the
single three-job correction size before dispatch (either source may use this
finite correction budget), and check every final cell's
individual feasibility before freezing the matrix. Calibration failures are
retained and never replaced by more favorable attempts. Observe at least one
completed cleanup before long calibration. At that point forecast all remaining
calibration,27 evaluation jobs, the three-job correction reserve and final
collection; refuse early if they cannot fit. Repeat the feasibility decision
after calibration before choosing27/36 evaluation jobs.

PAARC initial4/minimum2, growth1.5, target ratio2, baseline120s, source ceiling192.
No algorithm changes/retuning. Tool candidates(processes,threads/process) are
(1,32),(1,192),(8,24), selected by verified goodput subject to coverage. Two workers
halve the per-worker ceiling/thread count. This is static partitioning between
independent local controllers, not new shared global adaptive admission.

Both tools use matched TaskVine sharding, eight cores/24GiB per downloader, one
HTTP attempt/row, zero tool shard retries, uncompressed original-byte archives,
the same source guards and manager-side independent decoding/digest verification.
Tool timeout semantics differ despite the common nominal30s setting. The tool's
spawned processes share a finite response-body reservation counter and stop flag.
An mmap counter and per-response leases of at most8MiB amortize shared accounting;
unused reservations are returned and unknown failed reads remain conservatively
charged. Local engineering measurements separately quantify guard effects; they
are not public-source performance evidence or a universal overhead bound.
Each immutable shard has a separate audit; merging recomputes global concurrency.
Comparison HTTP timing starts before connection setup and ends at body/failure
completion, without changing the PAARC application-delay feedback signal.

Pinned TaskVine uses native `retries=1` (zero means unlimited in7.17.2),
`max_forsaken=0`, one owned worker per unique feature and no replacement worker.
An atomic persistent per-partition directory prevents any native redispatch from
repeating acquisition, including after lost acknowledgments. Such native failures
remain incomplete; the coordinator never retries a scientific job.

Primary goodput is verified original-row bytes divided by manager submission
through verified returned artifacts. Per-job staging, execution, archiving,
transfer and verification are included. Calibration, installation, VM lifecycle,
final metadata collection and report generation are separate overhead. Historical
worker-verified short-pilot rates cannot be pooled with this timing definition.

`bounded-public-long-v1` explicitly bounds524288 rows,64GiB response payload,
2GiB metadata,80GiB native archive and96GiB returned artifacts per partition.
The job-level64GiB body ceiling is divided between workers. Old workload profiles
retain their exact semantics. Oversized whole-run metadata may use its declared
metadata limit; image members retain their64MiB object limit. Archive I/O streams.

The authorized campaign is24h, no new downloads after21h, at most1000 new compute
SUs,16million image attempts,3TiB response bodies and3.5TiB retained evidence.
One-hour VM windows reserve600s for cleanup. Up to64 early-closed windows share
the same24h cumulative allowance and expiry; this is not64 hours of authority.
New volumes total at most5TiB(4TiB manager;512GiB each acquisition scratch).
Short engineering jobs have600s deadlines and share windows. Longer jobs use
matching-condition duration forecasts, capped at2400s. A recurring jobless window
stops acquisition rather than spending repeated setup windows. Manager return
processing and independent verification run concurrently with TaskVine progress,
with a1200s verification ceiling inside the common job deadline.
The last window is reserved for metadata if needed. Expired/exhausted authority
produces an explicit blocked outcome; a shorter offline-only collection window
is allowed only within the existing remaining time and SU limits.
Local collection is capped at4GiB so compressed-transfer/extraction duplicates
fit the12GiB peak allowance while retaining50GiB free. Archives stay on the
manager volume. Worker duplicates are removed only after verified return.

Reuse the already trusted manager, worker and registered origin as the second
downloader without changing enrollment or accounting. No owned image server is
started. Existing supervisor, grants and history remain intact. Reuse exact
qualified patched CCTools7.17.2 bytes; install tool dependencies separately.
Fresh provider/SSH/network checks precede acquisition. Runtime scopes use owned
systemd groups; failures stop all owned groups and trigger supervised offload.
Only freshly observed SHELVED_OFFLOADED and restored networking qualify cleanup.

Implementation entrypoints: `public_population` (metadata only), `minimum_study`
(protocol/configuration), `minimum_queue` (calibration/freeze), `minimum_native`
(matched TaskVine jobs), `minimum_cloud` (finite supervised execution), and
`minimum_analysis` (measured-only summaries). All input populations, deployment
packets, credentials, raw evidence and manuscript files remain private.

First-attachment setup waits at most120s for the guest's automatic filesystem
initialization; it never formats a disk. Native returns contain regular files:
integrity hardlink aliases of the retained archive, archived row bodies and row
metadata are omitted, while journals remain. This avoids both unsafe hardlink
members and redundant body transfer. The independent archive verifier is unchanged.

A stopped engineering job may receive one explicit corrective attempt after an
execution defect is repaired. `gates/engineering-recovery.json` binds its unchanged
failed record hash, repair source head, reason and a replacement ID ending in
`-repair-1`. The failed record stays in place and remains charged to campaign
accounting. This exception cannot replace calibration/evaluation results, cannot
approve a second failed correction, and does not extend any deadline or allowance.

Use affected offline tests, one full local check/current CI and at most two
independent review invocations/two review-driven repairs. Under the maintainer's
explicit exception, one final PR follows experiments, reporting and cleanup;
there is no intermediate human-merge prerequisite. No email is authorized.
After the second review, supported findings can receive the second repair with
an exact reviewed/final delta, current CI and explicit validated dispositions.
The original report remains unchanged; this is not a final-head review or a
waiver of unresolved material findings, and no third call is authorized.

Primary interpretation: show all paired runs. Three blocks are descriptive;
six permit modest conditional paired-log-ratio inference, Holm-adjusted across
the two primary method contrasts. Report coverage, time, concurrency, latency,
errors, resources and costs, including negative findings. No causal ablation,
four-worker scaling, optimum-concurrency or guaranteed rate-limit-avoidance claim.

Primary sources: [iNaturalist](https://github.com/inaturalist/inaturalist-open-data),
[CVDF](https://github.com/cvdfoundation/open-images-dataset),
[img2dataset1.47.0](https://github.com/rom1504/img2dataset/blob/1.47.0/README.md),
[Jetstream2 flavors](https://docs.jetstream-cloud.org/general/instance-flavors/).
