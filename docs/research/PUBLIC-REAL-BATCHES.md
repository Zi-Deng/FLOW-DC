# Bounded public download pilot

Issue #53 implements the first partition of the private focused manuscript plan.
This is an exploratory delivery comparison, not a submission qualification or a
claim that adaptive concurrency universally beats Fixed.

The source catalog contains TaiBIF moth images and CVDF-hosted Open Images JPEGs.
Each source needs a recorded media-rights qualification, current robots/TLS/access
checks and a disjoint32-object low-load probe. A missing source remains missing;
the runner does not silently substitute another service. Dataset licensing alone
must not be relabeled as separately established image rights. CVDF hosted bytes
may already be resized; Flickr OriginalMD5 does not validate the hosted rendition.

## Design and limits

Four methods: `ratio-v2`, `gradient-candidate-v2`, `fixed-v1` and img2dataset1.47.0.
Per source, four128-row qualification jobs (maximum concurrency4), two disjoint
Fixed32 size calibrations (512/2048 rows),16 tuning jobs and16 fresh evaluation
jobs. The nominal batch contains64 scientific jobs plus12 engineering jobs and
64 preflight GETs. All arms in an evaluation block receive the same original rows;
blocks and phases are disjoint. Tuning deliberately reuses its frozen source
manifest across settings. Balanced seeded method order limits order imbalance;
it does not remove server-cache or time effects.

Adaptive tuning uses startup growth1.25/1.5 × baseline age10/120 seconds,
target-delay ratio2, minimum2, initial4 and ceiling192. Fixed and single-process
img2dataset use8/32/96/192. All tool attempts/shard retries are disabled after the
first attempt. Tool original bytes are preserved (`resize_mode=no` and
`disable_all_reencoding=True`) in WebDataset archives with original metadata.
Tool HTTP observation is installed inside the native spawned shard process.
The guard blocks redirects, unplanned URLs, proxy use, repeat URLs and budget
exhaustion. Refusals stop the source; no public fault injection or threshold search
is performed. An interrupted acquisition is never replayed.

The selected size is2048 or512, frozen before tuning using conservative measured
qualification/calibration cost. Selection maximizes verified delivery goodput
subject to coverage within one percentage point of the Fixed32 tuning reference.
This first exploratory batch reports latency but does not claim the later joint
coverage/latency acceptance test. Later confirmation requires a separate freeze.

Batch limits:100 new journal SUs,150000 image attempts,32GiB received bodies,
40GiB authoritative archives,50GiB free reserve,240 seconds/job, at most eight
3600-second VM windows including600 seconds cleanup. No acquisition starts after
T+5h30; total target is eight hours including reporting/cleanup. The supervisor
retains responsibility if provider cleanup exceeds that target. One trusted
worker is selected; manager and origin remain offloaded. Preserved historical
cumulative grants can exceed the new admission budget; neither history nor
consumption is reset. Allocation receipt age is at most seven days, with its
actual observation basis disclosed rather than invented.

## Execution

Use one pinned environment for every arm:
`benchmark/requirements-tools-linux-py312.lock`. The ordinary real-server lock
has different scientific dependency pins and must not be merged into it.

1. Prepare scalar metadata populations (`url`, `record_id`, `author`, `license`,
   `landing_url`) offline, recording publisher/license provenance and exclusions.
2. Commit source, then call `public_batch.prepare(study, source, populations,
   provenance)`. Input pools and protocol are immutable first-attempt records.
3. Run `python -m benchmark.real_server_study.public_cloud prepare --study STUDY
   --source SOURCE --previous PREVIOUS_PRIVATE_PACKET --packet NEW_PRIVATE_PACKET`.
   This only prepares artifacts/wheels and reuses existing verified SSH/volume
   references. It does not activate a VM.
4. Put current-head CI run IDs and fixed-head independent review receipts in the
   private packet's `qualification.json`. The user's explicit exception permits
   branch CI/review before the single final PR, without an intermediate merge.
5. Run `python -m benchmark.real_server_study.public_cloud run --packet PACKET`.
   This applies the finite supported campaign grant and supervises bounded worker
   windows. Keep the controller process alive. Each window stops/offloads through
   the existing supervisor and verifies original networking restoration.

No credentials, private manifests, results or manuscript enter Git/reviewer
snapshots. No supervisor upgrade/restart, account reset, email or human merge is
performed by these commands. If a remote invocation fails, its owned unit is
stopped and cleanup is attempted; diagnose its first attempt before any recovery.

## Results and interpretation

Each run retains input/configuration/attempt identities, request events, native
outputs, archive digests and independent verification. FLOW-DC archives are
verified against original-row identities, payloads, observations and closure.
img2dataset's Parquet ledger retains every original row including terminal
failures; successful archive entries must agree with that ledger. Independently
decode credited images. This proves retained-byte consistency, not independently
known publisher byte truth. Delete only validated loose duplicates after keeping
the authoritative archive and a compaction receipt.

Primary goodput is decoded, archive-verified original-row bytes divided by job
launch through independent worker-side verification. Environment installation,
batch staging and final metadata collection are separate overhead. Report these
as well as total campaign time/SUs. Tool activity begins before connection setup;
FLOW-DC dispatch begins when headers are sent, so their mean activity measures
have different boundaries and must not be presented as identical latency metrics.

`public_analysis` regenerates JSON and a readable Markdown report. It retains
missing/stopped cells, changed content, conditional paired log-ratio intervals and
explicitly exploratory limitations. Four blocks are insufficient to establish
the later precision target. Metadata collection validates its transfer manifest;
original images remain private on the retained worker volume. No image download
is repeated for final reporting.

Offline regression fixtures (including a native pinned-tool shard with mocked
HTTP) are software checks only and never enter scientific results.

Sources: [CVDF](https://github.com/cvdfoundation/open-images-dataset),
[Open Images metadata](https://storage.googleapis.com/openimages/web/download_v7.html),
[img2dataset1.47.0](https://github.com/rom1504/img2dataset/tree/1.47.0),
[TaiBIF resource](https://ipt.taibif.tw/resource?r=twmoth-fb).
