"""Receipt-based offline evidence audit and private LaTeX onset report."""
import json
import statistics
import subprocess
import time
from pathlib import Path

from . import onset
from .analyze import summary
from .common import file_hash, lock, read_json, replace_derived, require, utc
from .prepare import read_lines


def audit(study, source, seconds=1200):
    study = Path(study)
    with lock(study/'study.lock'):
        return _audit(study, source, seconds)


def _audit(study, source, seconds):
    """Recheck credited archives once, resume unchanged successful audit units."""
    began = time.monotonic()
    require(0 < seconds <= onset.LIMITS['audit_seconds'], 'audit budget invalid')
    onset.check(study, source)
    deadline = began+seconds
    out = study/'analysis'; out.mkdir(exist_ok=True)
    units = out/'audit'; units.mkdir(exist_ok=True)
    result, pending, signals = [], 0, []
    for run in read_json(study/'schedule.json')['runs']:
        directory = study/'runs'/run['run_id']; path = directory/'record.json'
        if not path.exists():
            partial = {**run, 'complete':False, 'status':'interrupted' if directory.exists() else 'unstarted'}
            if directory.exists():
                partial['retained_metadata'] = {}
                try:
                    for evidence in sorted(directory.glob('*')):
                        if evidence.is_file() and not evidence.is_symlink():
                            partial['retained_metadata'][evidence.name] = file_hash(evidence, deadline)
                except TimeoutError:
                    pending += 1; partial['status'] = 'audit_pending'
            result.append(partial)
            continue
        row = read_json(path)
        require(all(row[k] == value for k, value in run.items()), 'audit schedule changed')
        try:
            # Small structural material is always rechecked; only immutable large
            # archive/observation scans may reuse an unchanged successful unit.
            require(time.monotonic() < deadline, 'offline audit deadline')
            binding = read_json(directory/'attempt.json')
            require(binding['protocol_sha256'] == file_hash(study/'protocol.json', deadline), 'attempt protocol changed')
            require(binding['slice_sha256'] == file_hash(study/'slices'/f"{run['slice']}.parquet", deadline), 'slice changed')
            if run['phase'] == 'validation':
                require(binding['fixed_selection_sha256'] == file_hash(study/'fixed-selection.json', deadline), 'selection changed')
            digest = file_hash(path, deadline)
            cache = units/(run['run_id']+'.json')
            stamp = lambda p: [getattr(p.stat(), k) for k in ('st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns')]
            large_stamp = stamp(directory/'native.tar') if row['complete'] else None
            previous = read_json(cache) if cache.exists() else None
            reused = previous is not None and previous['record_sha256'] == digest and previous['archive_stat'] == large_stamp
            for name, expected in row['evidence'].items():
                relative = Path(name)
                require(not relative.is_absolute() and '..' not in relative.parts and not (directory/relative).is_symlink(), 'unsafe evidence path')
                if name != 'native.tar' or not reused:
                    require(file_hash(directory/relative, deadline) == expected, 'retained evidence changed')
            if row['complete']:
                verification = read_json(directory/'verification.json')
                require(verification.pop('observations') == row['observations'], 'measurement receipt changed')
                credits = verification.pop('credited_url_bytes')
                require(verification == row['verification'] and sum(credits.values()) == verification['credited_bytes'], 'credit receipt changed')
                overview = read_json(directory/'native/overview.json')
                for spec in overview['http_observations']['files']+overview['control_trajectories']:
                    name = Path(spec['path'])
                    require(not name.is_absolute() and '..' not in name.parts, 'unsafe observation path')
                    observed = directory/'native'/name
                    require(not observed.is_symlink() and observed.stat().st_size == spec['bytes'], 'observation changed')
                    # Even on resume, small observations are read afresh.
                    require(file_hash(observed, deadline) == spec['sha256'], 'observation digest mismatch')
                require(row['goodput_Bps'] == row['verification']['credited_bytes']/row['elapsed_s'], 'goodput arithmetic changed')
                require(row['exposure'] == onset.summarize(read_lines(directory/'request-audit.jsonl'),
                        read_json(directory/'flowdc-config.json')['C_max'], credits), 'exposure arithmetic changed')
                require(stamp(directory/'native.tar') == large_stamp, 'archive changed during audit')
            from .onset_run import audit_events
            events, invalid = audit_events(directory/'request-audit.jsonl')
            require(not invalid or not row['complete'], 'malformed credited audit')
            for event in events:
                if event['event'] == 'stop' or (event['event'] == 'headers' and event.get('status') in (429,503)):
                    signals.append(dict(run_id=row['run_id'], requested_cap=row['configured_cap'], event=event))
            facts = dict(record_sha256=digest, archive_stat=large_stamp, verified_utc=utc())
            if not reused: replace_derived(cache, facts)
            result.append({**row, 'status': 'complete' if row['complete'] else 'stopped'})
        except TimeoutError:
            pending += 1
            result.append({**run, 'complete': False, 'status': 'audit_pending'})
        except ValueError as error:
            if str(error) != 'offline audit deadline': raise
            pending += 1; result.append({**run, 'complete': False, 'status': 'audit_pending'})
    by = {(r['block'], r['arm']): r for r in result if r['phase'] == 'validation' and r['complete']}
    contrasts = {}
    for method in ('ratio-v1', 'gradient-candidate-v1'):
        pairs = [(by[(b, method)], by[(b, 'fixed-v1')]) for b in range(1, 4)
                 if (b, method) in by and (b, 'fixed-v1') in by]
        contrasts[method] = summary([a['goodput_Bps']-b['goodput_Bps'] for a, b in pairs])
    stop = read_json(study/'STOP.json') if (study/'STOP.json').exists() else None
    reason = stop['reason'] if stop else None
    interpretation = ('Explicit rate limiting observed' if any(s['event'].get('status') == 429 for s in signals) else
                      'Performance degradation observed; rate-limit attribution unconfirmed' if reason in
                      ('sustained_latency', 'body_completion_collapse', 'service_unavailable') else
                      'Inconclusive: '+reason if reason else
                      'No onset observed within qualified exposure' if all(r['complete'] and r['exposure']['qualified'] for r in result[:7]) else
                      'Inconclusive: unfinished or unqualified search')
    analyzed = dict(schema='flowdc-onset-results-v1', protocol_sha256=file_hash(study/'protocol.json'),
                    interpretation=interpretation, stop=stop, signals=signals, runs=result, paired_differences_Bps=contrasts,
                    audit_pending=pending, last_qualifying_search_cap=max((r['cap'] for r in result if r['phase'] == 'search'
                        and r['complete'] and r['exposure']['qualified']), default=None),
                    limitation='Same-host receipt/retained-file verification; no second payload decode, independent-human review or clean-environment reproduction inferred.',
                    audit_elapsed_s=time.monotonic()-began)
    replace_derived(out/'results.json', analyzed)
    return analyzed


def report(study, output=None):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    study = Path(study); out = Path(output) if output else study.parent/'final-report'
    out.mkdir(exist_ok=True, parents=True)
    if (study/'analysis/results.json').exists():
        result = read_json(study/'analysis/results.json')
        stale = False
        for index, row in enumerate(result['runs']):
            path = study/'runs'/row['run_id']/'record.json'
            if path.exists() and any(row.get(k) != v for k,v in read_json(path).items()):
                result['runs'][index] = {**next(r for r in onset.schedule() if r['run_id'] == row['run_id']),
                                         'complete':False, 'status':'audit_pending'}
                stale = True
        if stale:
            result.update(interpretation='Inconclusive: collected attempts lack current aggregate audit',
                          audit_pending=sum(r['status']=='audit_pending' for r in result['runs']), paired_differences_Bps={})
    else:
        inventory = [{**r, 'complete':False, 'status':'audit_pending' if (study/'runs'/r['run_id']).exists() else 'unstarted'}
                     for r in read_json(study/'schedule.json')['runs']]
        result = dict(protocol_sha256=file_hash(study/'protocol.json'),
                      interpretation='Inconclusive: aggregate audit unavailable within finite budget',
                      last_qualifying_search_cap=None, audit_pending=sum(r['status']=='audit_pending' for r in inventory),
                      runs=inventory, paired_differences_Bps={}, stop=None, signals=[],
                      limitation='Status-only inventory from collected metadata; no unaudited output credit.')
        replace_derived(out/'inventory-only.json', result)
    require(result['protocol_sha256'] == file_hash(study/'protocol.json'), 'report protocol changed')
    protocol = read_json(study/'protocol.json')
    escape = lambda value: ''.join({'_':r'\_', '%':r'\%', '&':r'\&', '#':r'\#', '{':r'\{', '}':r'\}',
                                  '\\':r'\textbackslash{}'}.get(c, c) for c in str(value))
    number = lambda n, scale=1: '--' if n is None else f'{n/scale:.3f}'
    fig, axes = plt.subplots(2, 1, figsize=(6.6, 5.2), layout='constrained')
    search = [r for r in result['runs'] if r['phase'] == 'search' and r.get('exposure')]
    for key, label in (('actual_median','Time-weighted median'), ('actual_p95','Time-weighted p95'), ('achieved_peak','Peak')):
        points = [(r['cap'], r['exposure'].get(key)) for r in search if r['exposure'].get(key) is not None]
        if points: axes[0].plot(*zip(*points), 'o-', label=label, linewidth=1)
    if axes[0].get_legend_handles_labels()[0]: axes[0].legend(frameon=False, fontsize=8)
    for row in result['runs']:
        if row['phase'] == 'search' and row.get('exposure') and row['exposure']['steady_goodput_Bps'] is not None:
            axes[1].plot(row['cap'], row['exposure']['steady_goodput_Bps']/1e6, 'o', color='#246477')
    axes[0].set(xlabel='Requested search concurrency', ylabel='Actual HTTP concurrency')
    axes[1].set(xlabel='Requested search concurrency', ylabel='Verified steady MB/s')
    for ax in axes: ax.grid(alpha=.2)
    fig.savefig(out/'onset.pdf'); plt.close(fig)
    trajectories = [r for r in result['runs'] if r['phase'] == 'validation' and r['block'] == 1 and
                    (study/'runs'/r['run_id']/'request-audit.jsonl').exists()]
    if trajectories:
        from bisect import bisect_right
        from .onset_run import audit_events
        fig, axs = plt.subplots(len(trajectories), 1, figsize=(6.6, 2.3*len(trajectories)), layout='constrained', squeeze=False)
        for ax, row in zip(axs[:,0], trajectories):
            events, invalid = audit_events(study/'runs'/row['run_id']/'request-audit.jsonl')
            samples = [e for e in events if e['event'] == 'sample']
            ends = [e['monotonic_s'] for e in events if e['event'] == 'network_end' and e.get('status') == 200 and e.get('body_delay_s')]
            if samples:
                origin = samples[0]['monotonic_s']; times = [e['monotonic_s']-origin for e in samples]
                ax.plot(times, [e['inflight'] for e in samples], label='Actual HTTP', color='#246477')
                ax.plot(times, [e.get('cap') for e in samples], '--', label='Controller cap', color='#666666')
                rate_ax = ax.twinx()
                rate_ax.plot(times, [(bisect_right(ends,e['monotonic_s'])-bisect_right(ends,e['monotonic_s']-10))/10 for e in samples],
                             color='#8B6A2F', alpha=.7)
                rate_ax.set_ylabel('Body completions/s', color='#8B6A2F')
            ax.set(title=row['arm'], xlabel='Seconds within run', ylabel='Concurrency'); ax.grid(alpha=.15)
            if ax.get_legend_handles_labels()[0]: ax.legend(frameon=False, fontsize=7, loc='upper left')
        fig.savefig(out/'trajectories.pdf'); plt.close(fig)
    lines = [r'\documentclass[11pt]{article}', r'\usepackage[margin=.8in]{geometry}',
             r'\usepackage[T1]{fontenc}\usepackage{newtxtext,newtxmath,booktabs,longtable,graphicx,xcolor,hyperref}',
             r'\definecolor{navy}{HTML}{18394A}\hypersetup{colorlinks=true,urlcolor=navy}',
             r'\setlength{\parindent}{0pt}\setlength{\parskip}{6pt}\begin{document}',
             r'\title{\textcolor{navy}{FLOW-DC}\\TaiBIF concurrency-onset study}\author{Private exploratory evidence report}\date{\today}\maketitle',
             r'\section{Outcome}', escape(result['interpretation'])+'.',
             f"Last qualifying clean tested setting: {result['last_qualifying_search_cap']}. Audit-pending runs: {result['audit_pending']}.",
             r'\section{Design and rationale}',
             'Search caps were 66, 80, 96, 128, 160, 192 and 256. Each run had 8,192 distinct previously unrequested moth-image URLs, one attempt per URL and a 180-second acquisition deadline. '
             'Nine conditional validation runs compared Ratio, Gradient and an independently data-selected Fixed setting in three position-balanced blocks. '
             'Adaptive methods started at four, minimum two, ceiling 256; algorithms and common acquisition safeguards were unchanged.',
             'The source is the retained Taiwan Moth Occurrence Data Collected From Social Network dataset, served from moth.tbn.org.tw/images/. '
             'Disjoint samples exclude recorded prior dispatches, including failed requests; URLs are not replaced after observing success. '
             f"The protocol excludes {protocol.get('excluded_dispatch_urls','unavailable')} prior requested URLs. Dataset/archive identity, source revision and every frozen input digest are retained in protocol.json.",
             'A fixed setting cannot adjust its concurrency automatically. An adaptive controller can use feedback to select a setting, but being adaptive alone does not prove higher performance or avoidance of every refusal. '
             'This small study describes discovery costs and behavior; it cannot establish universal superiority or a permanent server quota.',
             r'\section{Every planned first attempt}\small\begin{longtable}{llrrrr}\toprule Run & Status & Cap & Peak & Qualified s & MB/s\\\midrule\endhead']
    for row in result['runs']:
        exposure = row.get('exposure', {})
        label = row['run_id'].replace('gradient-candidate-v1', 'Gradient').replace('ratio-v1', 'Ratio').replace('fixed-v1', 'Fixed')
        lines.append(f"{escape(label)} & {escape(row['status'])} & {row.get('configured_cap') or row['cap'] or '--'} & {exposure.get('achieved_peak','--')} & "
                     f"{number(exposure.get('qualified_seconds'))} & {number(row.get('goodput_Bps'),1e6)} "+r'\\')
    lines += [r'\bottomrule\end{longtable}\normalsize',
              r'\includegraphics[width=\linewidth]{onset.pdf}',
              'The upper plot distinguishes actual HTTP concurrency from the requested cap. The lower plot uses verified steady-window bytes for clean complete search runs. '
              'Primary run goodput in the table includes launch through native finalization and independent verification; these are different timing boundaries.',
              r'\section{Validation and uncertainty}']
    for method, value in result['paired_differences_Bps'].items():
        interval = '['+', '.join(number(n, 1e6) for n in value['ci'])+'] MB/s' if value['ci'] else 'unavailable'
        lines.append(escape(method)+f": {value['n']} complete pairs; mean difference versus selected Fixed {number(value['mean'],1e6)} MB/s; ordinary 95\\% paired t interval "+interval+'.')
    if (study/'fixed-selection.json').exists():
        selection = read_json(study/'fixed-selection.json')
        lines.append(f"Fixed selected at {selection['cap']}: lowest tested cap within 5\\% of the highest clean verified steady goodput. Its calibration cost belongs to the search, not the validation timing boundary.")
    if trajectories:
        lines += [r'\includegraphics[width=\linewidth]{trajectories.pdf}',
                  'First planned validation block: controller cap, actual HTTP activity and trailing ten-second body completions. The first ten seconds have a partial rate window; body counts are provisional until verification.']
    if result['stop']:
        lines += [r'\section{Stop and recovery}', 'Stop reason: '+escape(result['stop']['reason'])+
                  '; run: '+escape(result['stop'].get('run_id') or 'none')+'.']
    if result.get('signals'):
        first = result['signals'][0]
        lines.append(f"First retained adverse event was in {escape(first['run_id'])}, at requested cap {first['requested_cap']}. Actual in-flight activity and response status remain in the event trace; requested cap is not a configured server quota.")
    if (study/'recovery-comparison.json').exists(): lines.append(escape(read_json(study/'recovery-comparison.json')))
    if (study/'probe-summary.json').exists():
        probe = read_json(study/'probe-summary.json')
        lines += [r'\section{Health probe}', f"At concurrency two: {probe['requests']} attempts, {probe['valid']} decoded images, "
                  f"{number(probe['observed_bytes'],2**20)} MiB read response bodies, median application first-byte delay {number(probe['ttfb_p50_s'])} s."]
    if (study/'host-gate.json').exists() and read_json(study/'host-gate.json')['ignore_time']:
        lines.append('For this host only, the previously authorized TLS validity-period exception was used; trust-chain and hostname verification remained enabled. Certificate and robots observations are retained.')
    closures = sorted(study.parent.glob('window-[0-9][0-9].json'))
    if closures:
        closure = read_json(closures[-1])
        lines += [r'\section{Operational exposure and cleanup}',
                  f"Closed VM windows: {len(closures)} of two allowed. New conservative journal exposure: {number(closure.get('new_exposure_su'))} SUs, "
                  'separate from exact provider billing. New-work admission ceiling: 80 SUs. '
                  f"Fresh offload and original-network verification: {escape(closure.get('fresh_offload_and_network_verified',False))}; observation time {escape(closure['utc'])}."]
    lines += [r'\section{Interpretation, accounting and limits}',
              'The reported historical slowdown means fewer than five completed images per second, not fewer than five concurrent requests. '
              'Live body-completion rates are provisional; only decoded and digest-verified retained outputs receive image credit. '
              'HTTP 429 identifies rate limiting; 503, transport failures, latency growth and throughput collapse are separate observations. '
              'The last clean setting and first adverse setting describe an ordered exposure history, not an exact invariant threshold. '
              'Short runs, client limits, time-window quotas, caches, server background load and hysteresis limit inference. '
              'Stopping early leaves later cells unstarted; failed requests and unknown partial credit are not replaced by favorable retries.',
              escape(result['limitation']),
              r'\href{https://www.rfc-editor.org/rfc/rfc6585.html\#section-4}{RFC 6585: HTTP 429}; '
              r'\href{https://www.rfc-editor.org/rfc/rfc9110.html\#section-10.2.3}{RFC 9110: Retry-After}; '
              r'\href{https://github.com/Netflix/concurrency-limits}{Netflix concurrency-limits}.',
              r'\end{document}']
    (out/'report.tex').write_text('\n'.join(lines)+'\n')
    with (out/'latex.log').open('wb') as log:
        for _ in range(2):
            subprocess.run(['pdflatex', '-interaction=nonstopmode', '-halt-on-error', 'report.tex'], cwd=out,
                           stdout=log, stderr=log, check=True, timeout=60)
    replace_derived(out/'report-manifest.json', dict(protocol_sha256=result['protocol_sha256'],
                    audit_pending=result['audit_pending'],
                    results_sha256=file_hash(study/'analysis/results.json') if (study/'analysis/results.json').exists() else None,
                    files={p.name:file_hash(p) for p in out.iterdir() if p.suffix in ('.pdf','.tex')}))
    return out/'report.pdf'
