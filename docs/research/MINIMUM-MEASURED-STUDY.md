# Adaptive transfer study on public image servers

This is the current, separately identified amendment to the closed minimum
measured study. Its purpose is to test practical advantages of PAARC-Ratio
(`ratio-v2`) and PAARC-Grad (`gradient-candidate-v2`) against img2dataset1.47.0.
Faster verified delivery, less target-specific configuration effort and naturally
observed refusal avoidance are hypotheses, not promised outcomes. Retained
historical comparisons, including cases won by img2dataset, remain evidence.
No controlled image origin or generated scientific dataset is used.

## Sources and independent selection

Develop settings on CVDF Open Images hosted JPEGs. Transfer the selected PAARC
profiles and tool configuration to iNaturalist open-data S3 and, independently
if qualified, the TaiBIF/TBN moth image host. Per-image attribution, license,
original row identity and official metadata provenance travel with each input.
The iNaturalist cohort uses a bounded publisher metadata prefix with CC0/CC-BY
JPEGs; it is not representative of all iNaturalist. Hosted renditions can be
publisher-resized. Both CVDF and iNaturalist use AWS; they do not establish
cross-provider generality. A dataset license alone does not establish every
linked image's rights or permission to stress its host.

A source requires explicit `media_rights_qualified` and `access_qualified`
provenance before any image probe. TaiBIF remains pending until this is supported;
its absence is a recorded missing case, not a substituted success. No email is
authorized. Robots, TLS and bounded sequential32-object health checks precede
acquisition; redirects, publisher opt-outs and unsupported pacing stop it.

Metadata is shuffled with seed20261023 before downloads. The first32 records
serve the health probe. Three disjoint equal-capacity partitions supply sizing,
calibration and confirmation (each at most524288 rows). Confirmation repeats a
fixed manifest across six independently initialized temporal blocks. URL order
is the same across methods within each block and changes by block. Repeated
URLs/cache effects are disclosed. No post-outcome successful-URL filtering.

## Minimum selection and comparison matrix

| Stage | Work | Maximum long jobs |
|---|---|---:|
| CVDF development | Four profiles for each of PAARC-Ratio, PAARC-Grad and img2dataset |12|
| Target tool calibration | Four tool configurations per target |8|
| Target confirmation | Four arms × six blocks × two targets |48|

The maximum scientific matrix is68 jobs, plus one sizing job and at most two
sizing corrections per source, and the32-object health checks. Each missing,
stopped or interrupted first attempt is retained. Identical transferred and
target-calibrated tool candidates share their six confirmation runs; they are
reported as aliases, not independent replications.

PAARC profile grid `(target_delay_ratio, startup_growth_factor)`:
`(2,1.5), (3,1.5), (3,2), (4.5,2)`. All use minimum2, initial4, baseline age120s
and existing v2 state transitions, sample gates, overload handling and rounding.
The acquisition ceiling is256 for AWS sources and192 for TaiBIF. This study
changes parameter selection, not the PAARC equations. Telemetry distinguishes
rounding holds from utilization holds and reports rejected stale samples.

Tool grid `(processes, threads/process)`:
AWS `(1,32), (1,128), (1,256), (8,32)`; TaiBIF `(1,32), (1,128), (1,192), (8,24)`.
Transfer the selected candidate index, applying this predefined ceiling mapping.
Select a configuration only after all four candidate dispositions exist. Eligible
runs complete at least600s of observed HTTP and have original-row coverage within
one percentage point of the best eligible coverage. Maximize independently
verified end-to-end goodput; within2% of the maximum prefer lower measured mean
HTTP concurrency, then candidate order. Freeze global PAARC selection before
examining target calibration. Target-specific tool calibration is an additional,
stronger comparator; its extra configuration cost is disclosed. There is no
claim that one chosen profile is universally optimal.

Target900s observed HTTP, intended minimum600s, using fixed real work without
padding. Size using the predefined eight-process tool candidate. At most two
corrections may enlarge the disjoint sizing workload. All methods receive the
same frozen row count. Preserve shorter-than-intended results rather than rerun
only fast methods. Linear forecasts maintain physical clock ordering and use
measured full native/verification cost; confirmation cannot tune future work.
Before freezing, check measured runtime and retained-storage forecasts against
the finite envelope. A forecast is not a guarantee of completion.

Within each target, rotate a seeded method order over six blocks so position
counts differ by at most one. Blocks3–5 begin no earlier than the UTC date after
the first confirmation run. VMs are offloaded during that wait. The operational
context, idle gaps and uncontrolled remote-cache/background-traffic effects are
retained; six blocks provide modest case-study evidence, not universal proof.

## Matched execution and evidence

One eight-core/24GiB TaskVine downloader per arm, with a common manager and the
same return/verification path. The third registered VM remains part of the finite
operational topology/accounting; no image-origin service runs on it. This study
does not claim distributed scale-out. Reuse the qualified pinned CCTools7.17.2
runtime. TaskVine's finite retries=1 and persistent exclusive run directories
prevent redispatch from repeating image acquisition; failed native tasks remain
failed and no replacement worker is launched.

Both methods preserve hosted bytes in uncompressed archives and carry original
metadata; img2dataset has resizing/reencoding disabled, zero request/shard
retries and a common nominal30s timeout. Native timeout semantics still differ.
Each original URL has at most one attempt. Tool processes share the finite body
budget. Common HTTP timing starts before connection setup and ends after body
completion/failure, independently of PAARC's application-delay feedback signal.

Primary goodput is independently verified original-row bytes divided by manager
submission through returned-artifact verification. This includes per-job staging,
processing, archiving, transfer and verification. Also report HTTP throughput,
actual mean/peak concurrency, original-row coverage, TTFB, elapsed/native time,
CPU/memory, HTTP status counts, configuration effort and operational cost.
Concurrency alone is not a performance endpoint. Retrospectively verified
acquisition-progress curves are labeled separately from usable-output time.

On interruption, PAARC credit requires matching original metadata, source
identity, commit/intent/ready records, digest and decode. Tool credit requires
sealed matching Parquet/tar shards, original metadata, digest and decode.
Unstarted/unsealed/unverified rows remain in the original denominator. Partial
records have no complete-run goodput. Unknown transport bytes are conservatively
charged at the job reservation, never entered as zero. Raw artifacts remain on
the manager; compact progress/status data and verification receipts are collected.

## Natural refusals and source suspension

No rate-limit onset search, IP rotation or repeated failing configuration is
part of this study. A429 is confirmed HTTP rate limiting;403/401 is access refusal
with unknown cause,503 is service unavailability, and sustained latency alone
is performance degradation. The existing TaiBIF onset at requested256 was a
latency stop, not evidence of blocking or150+ sustained actual inflight work.

A public-source guard stops acquisition promptly on refusal, opt-out or excessive
failures. The current protocol explicitly records `latency_stop_enabled: false`
for both PAARC and every img2dataset process. Latency remains a diagnostic from
the common pre-connection-to-first-body-byte events; it no longer terminates a
run by itself. PAARC's native feedback/adaptation is unchanged. Research timeout,
body, concurrency and campaign limits still apply. Missing flags in historical
protocols retain their original latency-stop behavior. The October23 sizing
attempt stopped under that earlier rule and is preserved separately; it is not
evidence of a server block or a completed calibration. The corrective campaign
keeps the same frozen population/partitions and documents repeated sizing URLs.

Conservatively suspend a stopped source for the rest of this finite campaign;
qualified unrelated sources can continue. This preserves the stopped first
attempt without automatically replaying a failing configuration. If development
on CVDF stops, dependent calibration and confirmation cannot proceed.

There is **no harness-imposed cooldown by default**. Preparation records
`user_cooldown_seconds: 0`; set a positive duration only when the user requests
one, via the same keyword on `minimum_study.prepare(...)`. For example,
`user_cooldown_seconds=600` requests ten minutes. Values must be finite,
nonnegative numbers. The legacy `prepare` and `onset-prepare` commands also
expose `--user-cooldown-seconds`. Do not edit an already attempted protocol.
The October23 user instruction supersedes the former automatic 10-minute,
30-minute and 24-hour harness waits for future execution.

Server `Retry-After` deadlines are recorded separately and still apply; when
both are present, use the later deadline. No header means no server-requested
wait. A later acquisition needs a separately identified batch, a bounded health
check, and clarification of access refusal when necessary. Historical stop and
cooldown records remain evidence, not a reason to silently add a new wait.
Removing the cooldown does not clear a source suspension, replay an attempt or
remove the separately planned temporal blocks across two UTC dates.
Never bypass server restrictions.
A figure showing img2dataset refused while PAARC succeeded requires actual
matched observations and disclosed ordering/carryover; otherwise omit that claim.

## Finite operations

The separately identified envelope is72h, no dispatch in the last4h, at most
2389 new compute SUs,40million image attempts,4TiB response bodies and3.5TiB
retained artifacts. These are ceilings, not expected consumption.
The corrective compute ceiling leaves room for the prior10.902SU attempt within
the original2400SU envelope. Its clock starts at the authorized campaign start;
the closed campaign's clock, grants, consumed allowance and history are unchanged.
Up to72 windows share this single wall deadline and cumulative allowance. A new explicit grant
may permit a four-hour window; historical one-hour grants and1800s inspection
limits retain their semantics. Cleanup reserves600s. Each whole job is at most
12600s, with at most10800s native processing and1800s reserved for return/audit;
independent verification is separately capped at1200s. Confirmation deadlines
are common across all arms on a target. No jobless activation loop is allowed.

`bounded-public-transfer-v1` explicitly limits524288 rows,128GiB response payload,
2GiB metadata,160GiB native artifacts and192GiB returned artifacts per job.
Older workload records are interpreted by their original finite profiles.
Physical free-space admission reserves384GiB for temporary duplication. Reuse
existing4TiB manager and512GiB scratch volumes in a new study subdirectory;
no new volume allocation, formatting or deletion of historical evidence.
Worker duplicates are removed only after independent manager verification.

Collect at most4GiB metadata while retaining50GiB local free space. Fresh provider,
SSH, runtime and network checks precede activation. Preserve registration,
grants, consumption and history. A necessary guarded supervisor upgrade happens
only while idle and freshly offloaded; once long-window grants exist, repair
forward rather than restore a journal or downgrade to an incompatible reader.
All failures stop owned guest process groups and invoke supervised cleanup.
Only fresh SHELVED_OFFLOADED and restored networking qualify final cleanup.

## Analysis, figures and workflow

Report all frozen cells and first attempts, including missing/censored cases.
Use paired log-goodput ratios and ordinary95% t intervals; Holm-adjust one-sided
primary superiority p values across four contrasts (two PAARC methods × two
targets versus transferred img2dataset), retaining unavailable hypotheses in the
family. Report target-calibrated comparisons separately. A practical speed claim
requires all six matched complete pairs, at least10% point improvement, adjusted
p<.05 and a coverage-difference interval no worse than one percentage point.
Remote-content changes invalidate that block's matched-byte comparison and are
reported explicitly. These small-sample intervals assume independent normal log
ratios; they do not prove universal superiority or causality.

Generate all figures from retained observations: paired goodput/elapsed time,
configuration curves, transferred versus locally calibrated performance and cost,
progress/concurrency, actual refusal evidence if any, and coverage/overhead.
Use common axes and show all methods and negative findings. Never generate
favorable values or relabel incomplete acquisition as completed throughput.

Use affected tests, one full local pass and current-head CI. Both automated
review slots and repair rounds for this task are already consumed. Retain the
actual reviewed SHA, disclose the final unreviewed delta and resolved findings;
no third review or claim that the amended head was independently reviewed.
One final PR follows experiment disposition, report and cleanup, with human
merge. Credentials, populations, private evidence and manuscripts stay untracked.

Entrypoints: `minimum_study`, `minimum_queue`, `minimum_native`, `minimum_cloud`,
`minimum_analysis`; `public_population` retrieves official metadata.

Sources checked October23,2026: [iNaturalist open data](https://github.com/inaturalist/inaturalist-open-data),
[CVDF](https://github.com/cvdfoundation/open-images-dataset),
[TaiBIF dataset](https://ipt.taibif.tw/resource?r=twmoth-fb),
[img2dataset1.47.0](https://github.com/rom1504/img2dataset/blob/1.47.0/README.md),
[HTTP429](https://www.rfc-editor.org/rfc/rfc6585#section-4),
[Jetstream2 flavors](https://docs.jetstream-cloud.org/general/instance-flavors/).
