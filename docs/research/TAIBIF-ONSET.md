# Bounded TaiBIF concurrency-onset study

This separate exploratory protocol uses the maintained downloader and the existing
trusted three-VM Jetstream2 topology. It preserves completed study records. It tests
automatic concurrency selection, not a theorem that every adaptive controller
outperforms every Fixed setting.

The target is `https://moth.tbn.org.tw/images/`. TaiBIF's metadata API pagination
limit does not establish an image-server rate or concurrency limit.

## Protocol

Search Fixed caps 66,80,96,128,160,192,256, in that order. Each first attempt has
8192 unique previously unrequested URLs, a180-second acquisition ceiling and one
HTTP acquisition attempt per URL. Require20 seconds of backlogged actual HTTP
concurrency >=80% of the requested setting after10-second warmup. Request/body,
process and verification time limits remain separate. Short/client-limited runs
are inconclusive rather than evidence that the requested load was sustained.

Stop at the first401/403/429/503; sustained latency, sparse completion collapse,
transport failures or client limits also end escalation. A collapse means <5
complete HTTP bodies/second and <20% of the fresh66 baseline for two10-second
windows, while backlogged with >=8 active requests and pending age>=5seconds.
Body completion is provisional until independent image verification. Record actual
inflight, cap, queueing, pending ages, bytes, response statuses and allowed headers.

If every search stage qualifies cleanly, select the lowest cap within5% of the
highest observed verified steady goodput. Freeze that setting, then run three
position-balanced blocks of Ratio, Gradient and selected Fixed. Adaptive methods
retain minimum2, initialization4, ceiling256,256 tasks, native rules and common
Retry-After safeguards. These are exploratory paired comparisons with independent
run units, not request-level replication or confirmatory efficacy evidence.

After a429/503 or qualifying server slowdown, wait >=600seconds and any longer
Retry-After; permit only one128-URL recovery check at concurrency2. Compare against
the initial300-URL concurrency2 probe. Do not resume escalation/validation, even
after recovery. Skip recovery if it cannot fit the finite windows/expiry. No recovery
request follows401/403 or a TLS failure. Do not rotate clients/egress to evade limits.

Bounds: two3600-second VM windows,14400-second campaign expiry,80SU new-exposure
admission ceiling,135168 image requests,24GiB read response bodies,56GiB new
artifacts,40GiB free reserve,2GiB collected metadata,1200-second aggregate audit,
900-second collection. The supervisor's old monotonically increasing cumulative
allowances are preserved: their conservative preview is not new spending authority.
The onset driver independently refuses additional windows or exposure above80SU.
Cleanup continues until fresh offload and original networking are verified.

## Commands

All paths below are private operator paths, outside commits/reviewer snapshots.
Preparation and reporting do not activate guests. Acquisition is refused outside
a matching OpenStack VM and current bounded guest lease.

```bash
python -m benchmark.real_server_study onset-prepare \
  --study PRIVATE_STUDY --source COMMITTED_SOURCE \
  --previous ORIGINAL_STUDY --completed PREVIOUS_COLLECTED_STUDY
python -m benchmark.real_server_study onset-cloud-prepare \
  --study PRIVATE_STUDY --source COMMITTED_SOURCE \
  --previous-packet PREVIOUS_DEPLOYMENT --packet PRIVATE_PACKET
```

Before execution, record actual current-head CI and accepted independent review in
`PRIVATE_PACKET/qualification.json`: `head`, `review_head`, `ci_passed`,
`review_accepted`, with links/receipt paths for inspection. The supplied study
authorization permits qualified branch execution before the single final human
merge; it does not waive review or scientific evidence requirements.

```bash
python -m benchmark.real_server_study onset-cloud-run --packet PRIVATE_PACKET
python -m benchmark.real_server_study onset-audit \
  --study RETAINED_STUDY --source FROZEN_SOURCE --seconds 1200
python -m benchmark.real_server_study onset-report --study COLLECTED_STUDY
```

An existing acquisition intent consumes its attempt; interrupted acquisition is
never replayed. Audit units carry forward successful unchanged archive checks,
while small metadata and observation digests are rechecked. Metadata transfer may
resume without image downloads. The PDF/LaTeX are generated beside collected data
in `final-report/`, so collected manifest contents remain unchanged.

## Qualification and interpretation

Tests use loopback only: first-warning429/503/403, Retry-After, sparse/no completions,
client-cap/CPU distinctions, actual native known-image archives, body accounting,
deterministic allocation/selection, interrupted-attempt refusal, exhausted window/
SU admission and changed audit evidence. Tests are separate from static review and
live science. Use the task-wide maximum of two review calls/two repair rounds.

Report explicit429 separately from unattributed degradation, transport or local
limits. Publish all planned cell statuses. A last clean setting and first adverse
setting describe this exposure history; they are not an exact invariant quota.
Absence of a signal by256 does not exclude time-window, cumulative or other limits.
No changing-threshold or universal adaptive-superiority claim follows from this
single host. A fixed setting cannot adjust itself, but an adaptive controller can
still misestimate, explore slowly or encounter a refusal.

Primary sources: [HTTP429](https://www.rfc-editor.org/rfc/rfc6585.html#section-4),
[Retry-After/503](https://www.rfc-editor.org/rfc/rfc9110.html#section-10.2.3),
[Netflix concurrency-limits](https://github.com/Netflix/concurrency-limits),
[TaiBIF metadata API](https://portal.taibif.tw/zh-hant/taibif-api).
