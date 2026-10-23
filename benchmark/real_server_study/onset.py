"""Finite onset-study design and sparse-aware stop policy (no cloud effects)."""
import math
import random
import statistics
from collections import deque
from pathlib import Path

from .common import OPTIONS, PROFILE, checked_url, file_hash, read_json, require, source_identity, utc, write_new
from .prepare import read_lines
from .telemetry import StopPolicy

SCHEMA = 'flowdc-external-onset-v1'
CAPS = (66, 80, 96, 128, 160, 192, 256)
ROWS = 8192
LIMITS = dict(max_requests=135168, body_bytes=24*2**30, artifact_bytes=56*2**30,
              free_bytes=40*2**30, metadata_bytes=2*2**30, max_windows=2,
              window_seconds=3600, wall_seconds=14400, budget_su=80,
              acquisition_seconds=180, audit_seconds=1200, collection_seconds=900)


def schedule(seed=20261020):
    result = [dict(run_id=f'search-{i:02}-{cap}', phase='search', arm='fixed-v1',
                   method='fixed-v1', cap=cap, block=0, position=i, slice=f'{i-1:02}')
              for i, cap in enumerate(CAPS, 1)]
    rng = random.Random(seed)
    arms = ['ratio-v1', 'gradient-candidate-v1', 'fixed-v1']
    rng.shuffle(arms)
    rotations = list(range(3)); rng.shuffle(rotations)
    for block, offset in enumerate(rotations, 1):
        for position, arm in enumerate(arms[offset:]+arms[:offset], 1):
            result.append(dict(run_id=f'validation-{block}-{position}-{arm}', phase='validation',
                               arm=arm, method=arm, cap=None, block=block, position=position,
                               slice=f'{len(result):02}'))
    return result


def check(study, source):
    study = Path(study)
    protocol = read_json(study/'protocol.json')
    require(protocol['schema'] == SCHEMA and protocol['limits'] == LIMITS, 'onset contract changed')
    require(protocol['source'] == source_identity(source), 'onset source changed')
    require(protocol['caps'] == list(CAPS) and protocol['rows'] == ROWS and protocol['options'] == OPTIONS,
            'onset design changed')
    for name, digest in protocol['files'].items():
        relative = Path(name)
        require(not relative.is_absolute() and '..' not in relative.parts and
                not (study/relative).is_symlink() and file_hash(study/relative) == digest, 'onset input changed')
    require(read_json(study/'schedule.json')['runs'] == schedule(), 'onset schedule changed')
    return protocol


def prepare(study, source, previous, completed, *, user_cooldown_seconds=0):
    """Exclude actual dispatches in both completed studies, never success-select."""
    import polars as pl
    from .cooldown import user_seconds
    cooldown = user_seconds({"user_cooldown_seconds": user_cooldown_seconds})
    study, source, previous, completed = map(lambda p: Path(p).resolve(), (study, source, previous, completed))
    require(not study.exists(), 'onset study already exists')
    population = read_json(previous/'population-record.json')
    require(file_hash(previous/'population.parquet') == population['population_sha256'], 'population changed')
    seen, evidence = set(), {}
    for root in (previous, completed):
        probe = root/'probe-results.jsonl'
        require(probe.is_file(), 'previous probe missing')
        evidence[str(probe)] = file_hash(probe)
        seen.update(e['url'] for e in read_lines(probe))
        for path in sorted(root.glob('runs/*/request-audit.jsonl')):
            record = read_json(path.parent/'record.json')
            hashes = record.get('evidence', record.get('evidence_sha256', {}))
            digest = file_hash(path)
            require(digest == hashes['request-audit.jsonl'], 'previous dispatch evidence changed')
            evidence[str(path)] = digest
            seen.update(e['url'] for e in read_lines(path) if e['event'] == 'dispatch')
    frame = pl.read_parquet(previous/'population.parquet').filter(~pl.col('url').is_in(list(seen))).sort('url')
    require(frame['url'].n_unique() == frame.height, 'duplicate population URLs')
    runs = schedule()
    needed = len(runs)*ROWS+300+128
    require(frame.height >= needed, 'insufficient unused population')
    rng = random.Random(20261020)
    selected = frame[rng.sample(range(frame.height), needed)]
    chosen = pl.concat([selected[:428], selected[428:].sort('url')])
    # Round-robin sorted paths with randomized destinations balances path groups.
    allocations = [[] for _ in runs]
    cursor = 428
    for at in range(cursor, needed, len(runs)):
        destinations = list(range(len(runs))); rng.shuffle(destinations)
        for offset, destination in enumerate(destinations):
            allocations[destination].append(at+offset)
    study.mkdir(mode=0o700, parents=True); (study/'slices').mkdir()
    chosen[:300].write_parquet(study/'probe.parquet')
    chosen[300:428].write_parquet(study/'recovery.parquet')
    for run, indices in zip(runs, allocations):
        rng.shuffle(indices)
        chosen[indices].write_parquet(study/'slices'/f"{run['slice']}.parquet")
    write_new(study/'schedule.json', {'runs': runs, 'seed': 20261020})
    files = ['schedule.json', 'probe.parquet', 'recovery.parquet']+[f"slices/{r['slice']}.parquet" for r in runs]
    write_new(study/'protocol.json', dict(schema=SCHEMA, utc=utc(), source=source_identity(source),
              user_cooldown_seconds=cooldown,
              limits=LIMITS, rows=ROWS, caps=list(CAPS), options=OPTIONS,
              files={name: file_hash(study/name) for name in files}, previous_population=population,
              excluded_dispatch_urls=len(seen), prior_dispatch_evidence=evidence,
              interpretation='Exploratory conditional onset and automatic selection; no universal superiority claim.'))
    return read_json(study/'protocol.json')


def config(study, run):
    study = Path(study); fixed = run['cap']
    if run['phase'] == 'validation' and run['method'] == 'fixed-v1':
        fixed = read_json(study/'fixed-selection.json')['cap']
    return dict(input=str(study/'slices'/f"{run['slice']}.parquet"), input_format='parquet',
                output=str(study/'runs'/run['run_id']/'native'), output_format='webdataset',
                url='url', label=None, concurrent_downloads=256, timeout=30, enable_paarc=True,
                control_method=run['method'], method_options=OPTIONS[run['method']],
                C_min=fixed or 2, C_init=fixed or 4, C_max=fixed or 256,
                max_retry_attempts=1, research_profile=True, research_workload=PROFILE,
                create_tar=True, compress_tar=False, create_overview=True, force_overwrite=False)


def summarize(events, cap, credited=None):
    """Actual header-to-body exposure; decoded credit may be supplied by verifier."""
    from .design import activity_intervals, weighted_quantile
    starts = [e['monotonic_s'] for e in events if e['event'] == 'dispatch']
    if not starts:
        return dict(qualified=False, achieved_peak=0, qualified_seconds=0, steady_goodput_Bps=None)
    started = min(starts)
    intervals = activity_intervals(events, started)
    exposure = sum(end-begin for begin, end, active, backlog in intervals if backlog and active >= .8*cap)
    weighted = [(active, end-begin) for begin, end, active, backlog in intervals if backlog]
    ends = [e for e in events if e['event'] == 'network_end' and e.get('dispatched') and
            e['monotonic_s'] >= started+10]
    last = max((e['monotonic_s'] for e in ends), default=started+10)
    useful = sum((credited or {}).get(e['token'], 0) for e in ends)
    return dict(qualified=exposure >= 20, achieved_peak=max(e.get('inflight', 0) for e in events),
                backlogged_seconds=sum(w for active, w in weighted),
                actual_median=weighted_quantile(weighted, .5) if weighted else None,
                actual_p95=weighted_quantile(weighted, .95) if weighted else None,
                qualified_seconds=exposure, steady_goodput_Bps=useful/(last-started-10)
                if credited is not None and last > started+10 else None)


def select_fixed(rows):
    search = [r for r in rows if r['phase'] == 'search']
    require(len(search) == len(CAPS) and all(r['complete'] and not r.get('termination_reason') for r in search),
            'all search stages must finish cleanly before validation')
    clean = [r for r in rows if r['phase'] == 'search' and r['complete'] and
             r['exposure']['qualified'] and r['exposure']['steady_goodput_Bps'] is not None and not r.get('termination_reason')]
    require(clean, 'no adequately exercised Fixed setting')
    best = max(r['exposure']['steady_goodput_Bps'] for r in clean)
    require(best > 0, 'no verified steady goodput')
    return dict(cap=min(r['cap'] for r in clean if r['exposure']['steady_goodput_Bps'] >= .95*best),
                best_observed_Bps=best, criterion='lowest cap within 5% of best clean verified steady goodput',
                search_run_hashes={r['run_id']: r['record_sha256'] for r in clean})


class Policy(StopPolicy):
    """First-warning stop; body-rate collapse never requires 100 completions."""
    def __init__(self, baseline, rate=None, *, cooldown_seconds=0):
        super().__init__(baseline, cooldown_seconds=cooldown_seconds)
        self.rate = rate
        self.ends, self.samples = deque(), deque()
        self.current = (0, False, 0)
        self.fatal_reason = None

    def header(self, now, status, retry_after=None):
        first = self.reason
        super().header(now, status, retry_after)
        if status in (401, 403, 429, 503):
            self.reason = first or {401:'access_refusal', 403:'access_refusal', 429:'explicit_429', 503:'service_unavailable'}[status]
            self.fatal = self.fatal or status in (401, 403)
            if status in (401, 403): self.fatal_reason = self.fatal_reason or 'access_refusal'
            self.apply_cooldown()

    def network_end(self, now, status, measurement):
        self.ends.append((now, status == 200 and measurement.get('body_completed_at') is not None,
                          measurement.get('failure_kind') == 'transport'))

    def sample(self, now, active, backlog, pending, lag, cpu):
        self.current = (active, backlog, pending)
        self.samples.append((now, active, backlog, pending, lag, cpu))

    def tick(self, now):
        if self.origin is None:
            self.origin = now
        for queue in (self.ends, self.samples, self.completed):
            while queue and queue[0][0] < now-40:
                queue.popleft()
        if self.reason or now-self.origin < 30:
            return
        if self.rate is None:
            self.rate = sum(ok for at, ok, failed in self.ends if self.origin+10 < at <= self.origin+20)/10
            if self.rate < 5:
                self.reason = 'initial_baseline_slow'
                return
        recent = [s for s in self.samples if s[0] >= now-10]
        if len(recent) >= 9 and (recent[-1][5]-recent[0][5])/(recent[-1][0]-recent[0][0]) >= 3.8:
            self.reason = 'client_cpu_limit'
        if len(recent) >= 9 and all(s[4] >= .25 for s in recent):
            self.reason = 'client_event_loop_limit'
        if sum(t >= now-30 and failed for t, complete, failed in self.ends) >= 5:
            self.reason = self.reason or 'transport_failures'
        latency_bad = []
        collapse = []
        for index in range(3):
            lo, hi = now-10*(index+1), now-10*index
            delays = [delay for at, delay in self.completed if lo < at <= hi]
            latency_bad.append(len(delays) >= 20 and statistics.median(delays) >= 3*self.baseline)
            samples = [s for s in self.samples if lo < s[0] <= hi]
            rate = sum(complete for at, complete, failed in self.ends if lo < at <= hi)/10
            collapse.append(bool(self.rate and rate < 5 and rate < .2*self.rate and len(samples) >= 9 and
                                 all(s[1] >= 8 and s[2] and s[3] >= 5 for s in samples)))
        if all(latency_bad):
            self.reason = self.reason or 'sustained_latency'
        if all(collapse[:2]):
            self.reason = self.reason or 'body_completion_collapse'
        if self.reason:
            self.apply_cooldown()
