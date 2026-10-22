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

Use affected offline tests, one full local check/current CI and at most two
independent review invocations/two review-driven repairs. Under the maintainer's
explicit exception, one final PR follows experiments, reporting and cleanup;
there is no intermediate human-merge prerequisite. No email is authorized.

Primary interpretation: show all paired runs. Three blocks are descriptive;
six permit modest conditional paired-log-ratio inference, Holm-adjusted across
the two primary method contrasts. Report coverage, time, concurrency, latency,
errors, resources and costs, including negative findings. No causal ablation,
four-worker scaling, optimum-concurrency or guaranteed rate-limit-avoidance claim.

Primary sources: [iNaturalist](https://github.com/inaturalist/inaturalist-open-data),
[CVDF](https://github.com/cvdfoundation/open-images-dataset),
[img2dataset1.47.0](https://github.com/rom1504/img2dataset/blob/1.47.0/README.md),
[Jetstream2 flavors](https://docs.jetstream-cloud.org/general/instance-flavors/).
