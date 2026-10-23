"""Guarded native onset acquisition, first-attempt resume, and offline auditing."""
import argparse
import asyncio
import contextlib
import json
import os
from pathlib import Path
import runpy
import shutil
import socket
import statistics
import subprocess
import sys
import time

from . import onset
from .cooldown import deadline, user_seconds
from .common import (checked_url, decode_image, file_hash, lock, read_json, remaining,
                     replace_derived, require, tls_context, utc, write_new)
from .prepare import UA, guest_check, host_gate, read_lines, robots_permit
from .runner import compact_outputs, terminate


def audit_events(path):
    events, invalid = [], False
    with path.open() as stream:
        for line in stream:
            try:
                require(line.endswith('\n'), 'truncated audit line')
                events.append(json.loads(line))
            except (ValueError, TypeError):
                invalid = True; break
    return events, invalid


def fatal_signal(event):
    if event['event'] == 'headers' and event.get('status') in (401, 403):
        return 'access_refusal'
    return (event.get('fatal_reason') or event.get('reason') or 'fatal_stop') if event.get('fatal') else None


def recovery_allowed(decision):
    return not decision.get('fatal') and decision['reason'] in (
        'explicit_429', 'service_unavailable', 'sustained_latency', 'body_completion_collapse')


def rows(study):
    result, gap = [], False
    for run in read_json(study/'schedule.json')['runs']:
        directory = study/'runs'/run['run_id']
        if directory.exists():
            require(not gap and (directory/'record.json').is_file(), 'unfinished/out-of-order attempt; no acquisition replay')
            row = read_json(directory/'record.json')
            require(all(row[k] == value for k, value in run.items()), 'run identity changed')
            result.append({**row, 'record_sha256': file_hash(directory/'record.json')})
        else:
            gap = True
    return result


def used_budget(study):
    """Unsettled attempts retain their full body/artifact reservations."""
    body, artifacts = 0, 0
    if (study/'storage-baseline.json').exists():
        artifacts = read_json(study/'storage-baseline.json')['fixed_artifact_bytes']+2*2**30
    for path in study.glob('runs/*/attempt.json'):
        binding = read_json(path)
        record = path.with_name('record.json')
        row = read_json(record) if record.exists() else {}
        body += row.get('body_bytes', binding['body_reservation'])
        artifacts += row.get('artifact_bytes', binding['artifact_reservation'])
    for name in ('probe', 'recovery'):
        intent = study/f'{name}-intent.json'
        if intent.exists():
            summary = study/f'{name}-summary.json'
            body += read_json(summary)['observed_bytes'] if summary.exists() else read_json(intent)['body_reservation']
    return body, artifacts


async def health(study, recovery=False):
    """Two native HTTP clients, no redirects/replays, finite first-attempt probe."""
    import aiohttp
    import polars as pl
    guest_check(study)
    name = 'recovery' if recovery else 'probe'
    require(not (study/f'{name}-intent.json').exists(), 'health acquisition already attempted')
    body_before, _ = used_budget(study)
    limit = min(2**30, onset.LIMITS['body_bytes']-body_before)
    require(limit >= 64*2**20, 'health body allowance insufficient')
    write_new(study/f'{name}-intent.json', {'utc': utc(), 'body_reservation': limit})
    urls = pl.read_parquet(study/f'{name}.parquet')['url'].to_list()
    gate = read_json(study/'host-gate.json')
    if gate.get('robots_txt'):
        robots_permit(gate['robots_txt'], urls)
    results, total, stop, pending_reads, completed = [], 0, None, 0, []
    semaphore = asyncio.Semaphore(2)
    begin = time.monotonic()
    require(aiohttp.__version__ == '3.13.3', 'pinned aiohttp required')
    trace = aiohttp.TraceConfig()
    async def sent(session, ctx, params):
        ctx.trace_request_ctx['sent'] = time.monotonic()
    trace.on_request_headers_sent.append(sent)
    with (study/f'{name}-results.jsonl').open('x') as log:
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=2, ssl=tls_context(gate['ignore_time'])),
                                        headers={'User-Agent': UA}, trace_configs=[trace], trust_env=False) as session:
            session._retry_connection = False
            async def fetch(url):
                nonlocal total, stop, pending_reads
                async with semaphore:
                    if stop or remaining(study) < 45:
                        stop = stop or 'health_deadline'; return
                    checked_url(url)
                    row = dict(url=url, status=None, valid=False, bytes=0, ttfb_s=None)
                    body, timing = bytearray(), {}
                    try:
                        async with session.get(url, allow_redirects=False, timeout=aiohttp.ClientTimeout(total=30), trace_request_ctx=timing) as response:
                            row.update(status=response.status, retry_after=response.headers.get('Retry-After'))
                            if response.status in (401, 403, 429) or response.status >= 500:
                                stop = 'health_refusal'
                            else:
                                while True:
                                    amount = min(65536, limit-total-pending_reads)
                                    require(amount > 0, 'health body budget')
                                    pending_reads += amount
                                    try:
                                        chunk = await response.content.read(amount)
                                    finally:
                                        pending_reads -= amount
                                    total += len(chunk)
                                    if not chunk: break
                                    if not body: row['ttfb_s'] = time.monotonic()-timing['sent']
                                    body.extend(chunk)
                                    require(len(body) <= 64*2**20, 'health object too large')
                                if response.status == 200:
                                    try:
                                        decode_image(body); row['valid'] = True
                                    except Exception:
                                        pass
                    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as error:
                        row['error'] = f'{type(error).__name__}: {error}'
                        stop = 'health_transport_or_budget'
                    row.update(bytes=len(body), ended_monotonic=time.monotonic())
                    results.append(row)
                    if row['valid']: completed.append(row['ended_monotonic'])
                    log.write(json.dumps(row)+'\n'); log.flush()
            await asyncio.gather(*(fetch(url) for url in urls))
    elapsed = time.monotonic()-begin
    good = [r for r in results if r['valid']]
    summary = dict(passed=not stop and len(results) == len(urls) and len(good) >= .9*len(urls),
                   reason=stop, requests=len(results), valid=len(good), observed_bytes=total,
                   ttfb_p50_s=statistics.median(r['ttfb_s'] for r in good) if good else None,
                   mean_bytes=statistics.mean(r['bytes'] for r in good) if good else None,
                   elapsed_s=elapsed, images_per_s=len(good)/elapsed, concurrency=2, utc=utc())
    write_new(study/f'{name}-summary.json', summary)
    return summary


def launch(study, source, run_id):
    import polars as pl
    from .telemetry import Guard
    onset.check(study, source); guest_check(study)
    require(not (study/'STOP.json').exists(), 'onset campaign stopped')
    run = next(r for r in read_json(study/'schedule.json')['runs'] if r['run_id'] == run_id)
    directory = study/'runs'/run_id
    binding = read_json(directory/'attempt.json')
    cfg = onset.config(study, run)
    require(read_json(directory/'flowdc-config.json') == cfg and
            binding['protocol_sha256'] == file_hash(study/'protocol.json') and
            binding['slice_sha256'] == file_hash(cfg['input']) and
            binding['execution_sha256'] == file_hash(directory/'execution.json'), 'attempt binding changed')
    require(binding.get('fixed_selection_sha256') ==
            (file_hash(study/'fixed-selection.json') if run['phase'] == 'validation' else None), 'selection changed')
    baseline = read_json(study/'baseline-66.json') if (study/'baseline-66.json').exists() else read_json(study/'probe-summary.json')
    policy = onset.Policy(baseline['ttfb_p50_s'], baseline.get('body_rate'),
                          cooldown_seconds=user_seconds(read_json(study/'protocol.json')))
    gate = read_json(study/'host-gate.json')
    sys.path.insert(0, str(source/'bin'))
    with (directory/'request-audit.jsonl').open('x', buffering=65536) as stream:
        guard = Guard(pl.read_parquet(cfg['input'])['url'].to_list(), stream, policy.baseline,
                      gate['ignore_time'], policy=policy, body_limit=binding['body_reservation'])
        guard.emit('start', run_id=run_id, rows=onset.ROWS, definition_sha256=file_hash(study/'protocol.json'))
        restore = guard.install()
        try:
            sys.argv = [str(source/'bin/download_batch.py'), '--config', str(directory/'flowdc-config.json')]
            runpy.run_path(sys.argv[0], run_name='__main__')
        finally:
            restore()
            guard.emit('closed', active=len(guard.active), logical_attempts=len(guard.counts),
                       body_bytes=guard.body_bytes, stop_reason=policy.reason, fatal=policy.fatal,
                       fatal_reason=policy.fatal_reason,
                       cooldown_until_utc=policy.cooldown_until)


def execute(study, source, run):
    require(remaining(study) >= 180+300+180+60, 'run cannot fit acquisition/verification/finalization reserves')
    body, artifacts = used_budget(study)
    allocation = min(4*2**30, onset.LIMITS['body_bytes']-body)
    reservation = 2*allocation+2**30
    require(allocation >= 64*2**20 and artifacts+reservation <= onset.LIMITS['artifact_bytes'], 'study byte/artifact budget exhausted')
    require(shutil.disk_usage(study).free >= onset.LIMITS['free_bytes']+reservation, 'storage reserve insufficient')
    directory = study/'runs'/run['run_id']; directory.mkdir(parents=True)
    cfg = onset.config(study, run)
    binding = dict(run, utc=utc(), protocol_sha256=file_hash(study/'protocol.json'),
                   slice_sha256=file_hash(cfg['input']), body_reservation=allocation, artifact_reservation=reservation,
                   execution_sha256=file_hash(study/'execution.json'),
                   fixed_selection_sha256=file_hash(study/'fixed-selection.json') if run['phase'] == 'validation' else None)
    write_new(directory/'attempt.json', binding)
    write_new(directory/'execution.json', read_json(study/'execution.json'))
    write_new(directory/'flowdc-config.json', cfg)
    gate = read_json(study/'host-gate.json')
    if gate.get('robots_txt'):
        import polars as pl
        robots_permit(gate['robots_txt'], pl.read_parquet(cfg['input'])['url'].to_list())
    command = [sys.executable, '-B', '-m', 'benchmark.real_server_study.onset_run', 'launch',
               '--study', str(study), '--source', str(source), '--run-id', run['run_id']]
    reason, fatal_reason, cooldown, offset = None, None, 0, 0
    started = time.monotonic()
    with (directory/'stdout.log').open('x') as out, (directory/'stderr.log').open('x') as err:
        process = subprocess.Popen(command, cwd=source, stdout=out, stderr=err, start_new_session=True)
        try:
            while process.poll() is None:
                path = directory/'request-audit.jsonl'
                if path.exists():
                    with path.open() as stream:
                        stream.seek(offset)
                        while True:
                            before = stream.tell(); line = stream.readline()
                            if not line.endswith('\n'): offset = before; break
                            try:
                                event = json.loads(line)
                            except ValueError:
                                fatal_reason = 'malformed_request_audit'; reason = reason or fatal_reason
                                break
                            fatal_reason = fatal_reason or fatal_signal(event)
                            if event['event'] in ('stop', 'closed'):
                                reason = reason or event.get('reason') or event.get('stop_reason')
                                cooldown = max(cooldown, event.get('cooldown_until_utc', 0))
                if time.monotonic()-started >= onset.LIMITS['acquisition_seconds']:
                    reason = reason or 'acquisition_deadline'
                if shutil.disk_usage(study).free < onset.LIMITS['free_bytes']:
                    reason = 'storage_reserve'
                    fatal_reason = 'storage_reserve'
                if reason:
                    terminate(process); break
                time.sleep(.2)
        finally:
            terminate(process)
    acquired = time.monotonic()
    write_new(directory/'incomplete-record.json', {**run, 'complete': False, 'termination_reason': 'verification_interrupted'})
    verification, observations = {}, {}
    events, invalid_audit = audit_events(directory/'request-audit.jsonl') if (directory/'request-audit.jsonl').exists() else ([], True)
    if invalid_audit:
        reason = reason or 'malformed_request_audit'
        fatal_reason = fatal_reason or 'malformed_request_audit'
    # A killed audit may end in an incomplete JSON line; do not convert it to a clean result.
    for event in events:
        fatal_reason = fatal_reason or fatal_signal(event)
        if event['event'] in ('closed', 'stop'):
            reason = reason or event.get('stop_reason') or event.get('reason')
            cooldown = max(cooldown, event.get('cooldown_until_utc', 0))
    normal = process.returncode == 0 and not reason
    verify = [sys.executable, '-B', '-m', 'benchmark.real_server_study.verify', '--run-dir', str(directory),
              '--slice', cfg['input'], '--source', str(source), '--started', str(started), '--ended', str(acquired)]
    verify += ['--credit-urls'] if normal else ['--partial']
    with (directory/'verification.json').open('x') as out, (directory/'verification.log').open('x') as err:
        try:
            result = subprocess.run(verify, cwd=source, stdout=out, stderr=err, timeout=300)
            if result.returncode == 0:
                value = read_json(directory/'verification.json')
                if normal:
                    verification = value; observations = verification.pop('observations')
                else:
                    observations = value
            else:
                reason = reason or 'verification_failed'
                fatal_reason = fatal_reason or 'verification_failed'
        except subprocess.TimeoutExpired:
            reason = reason or 'verification_timeout'
            fatal_reason = fatal_reason or 'verification_timeout'
    complete = normal and bool(verification) and not reason
    if complete:
        dead = verification.get('row_final_status', {})
        if verification.get('decode_failures', 0) >= .05*onset.ROWS or \
                dead.get('404', 0)+dead.get('410', 0) >= .2*onset.ROWS:
            reason = 'content_or_dead_link_threshold'
        if run['run_id'] == 'search-01-66' and not observations.get('eligible_ttfb_s'):
            reason = reason or 'baseline_unavailable'
        compact_outputs(directory)
    credits = verification.pop('credited_url_bytes', None)
    exposure = onset.summarize(events, cfg['C_max'], credits)
    ended = time.monotonic()
    body_bytes = next((e['body_bytes'] for e in reversed(events) if e['event'] == 'closed' and
                       e.get('active') == 0 and 'body_bytes' in e), allocation)
    artifact_bytes = sum(p.stat().st_size for p in directory.rglob('*') if p.is_file() and not p.is_symlink())
    row = dict(run, configured_cap=cfg['C_max'], audit_invalid=invalid_audit,
               fatal=bool(fatal_reason), fatal_reason=fatal_reason,
               utc=utc(), complete=complete, returncode=process.returncode, termination_reason=reason,
               cooldown_until_utc=cooldown, rows=onset.ROWS, started_monotonic=started,
               acquired_monotonic=acquired, ended_monotonic=ended, acquisition_s=acquired-started,
               elapsed_s=ended-started, verification=verification, observations=observations, exposure=exposure,
               body_bytes=body_bytes, artifact_bytes=artifact_bytes,
               goodput_Bps=verification.get('credited_bytes', 0)/(ended-started) if complete else None)
    row['evidence'] = {str(p.relative_to(directory)): file_hash(p) for p in
                       [directory/'attempt.json', directory/'flowdc-config.json', directory/'request-audit.jsonl',
                        directory/'verification.json', directory/'execution.json'] if p.is_file()}
    if complete:
        row['evidence']['native.tar'] = verification['archive_sha256']
        for name in ('native/.flowdc/final.json', 'native/outcome-index.json', 'native/overview.json'):
            row['evidence'][name] = file_hash(directory/name)
    write_new(directory/'record.json', row)
    if complete and not reason and run['run_id'] == 'search-01-66':
        ends = [e for e in events if e['event'] == 'network_end' and e.get('status') == 200 and
                e['monotonic_s'] >= min(e['monotonic_s'] for e in events if e['event'] == 'dispatch')+10]
        duration = max(e['monotonic_s'] for e in ends)-min(e['monotonic_s'] for e in ends) if len(ends) > 1 else 0
        write_new(study/'baseline-66.json', dict(ttfb_p50_s=statistics.median(observations['eligible_ttfb_s']),
                  body_rate=len(ends)/duration if duration > 0 else 0, source_run_sha256=file_hash(directory/'record.json')))
    return row


def stop(study, reason, row=None, fatal=False):
    if not (study/'STOP.json').exists():
        write_new(study/'STOP.json', dict(reason=reason, utc=utc(), run_id=row['run_id'] if row else None,
                  fatal=fatal or bool(row and row.get('fatal')), fatal_reason=row.get('fatal_reason') if row else None,
                  cooldown_until_utc=max(deadline(user_seconds(read_json(study/'protocol.json'))),
                                         row.get('cooldown_until_utc', 0)) if row else 0))


def window(study, source, final=False):
    onset.check(study, source); guest_check(study)
    with lock(study/'study.lock'):
        try:
            recorded = rows(study)
        except (ValueError, KeyError, FileNotFoundError):
            stop(study, 'interrupted_attempt', fatal=True)
            return dict(status='stopped', reason='interrupted_attempt', runs=len(list(study.glob('runs/*/attempt.json'))))
        if not (study/'storage-baseline.json').exists():
            require(not any(study.glob('runs/*/attempt.json')), 'storage baseline missing after acquisition')
            fixed = sum(p.stat().st_size for p in study.parent.rglob('*') if p.is_file() and not p.is_symlink())
            write_new(study/'storage-baseline.json', {'fixed_artifact_bytes':fixed,
                      'future_analysis_reserve_bytes':2*2**30, 'utc':utc()})
        if not (study/'host-gate.json').exists(): host_gate(study)
        if not (study/'probe-summary.json').exists():
            if (study/'probe-intent.json').exists():
                stop(study, 'interrupted_health_probe')
            elif not asyncio.run(health(study))['passed']:
                stop(study, 'health_failed')
        for run in read_json(study/'schedule.json')['runs'][len(recorded):]:
            if (study/'STOP.json').exists(): break
            if remaining(study) < 720:
                if final: stop(study, 'finite_window_budget_exhausted')
                break
            if run['phase'] == 'validation' and not (study/'fixed-selection.json').exists():
                try:
                    selected = onset.select_fixed(recorded)
                except ValueError:
                    stop(study, 'no_adequately_exercised_fixed_setting')
                    break
                write_new(study/'fixed-selection.json', selected)
            row = execute(study, source, run)
            recorded.append({**row, 'record_sha256': file_hash(study/'runs'/run['run_id']/'record.json')})
            if row['termination_reason'] or not row['complete']:
                stop(study, row['termination_reason'] or 'insufficient_achieved_exposure', row)
                break
        if (study/'STOP.json').exists():
            decision = read_json(study/'STOP.json')
            recoverable = recovery_allowed(decision)
            if recoverable and not (study/'recovery-summary.json').exists() and not (study/'recovery-intent.json').exists():
                wait = decision['cooldown_until_utc']-time.time()
                if wait > 0:
                    return dict(status='cooldown', until_utc=decision['cooldown_until_utc'], runs=len(recorded))
                if remaining(study) >= 150:
                    summary = asyncio.run(health(study, recovery=True))
                    baseline = read_json(study/'probe-summary.json')
                    replace_derived(study/'recovery-comparison.json', dict(passed=summary['passed'],
                          latency_ratio=summary['ttfb_p50_s']/baseline['ttfb_p50_s'] if summary['ttfb_p50_s'] else None,
                          rate_ratio=summary['images_per_s']/baseline['images_per_s']))
            return dict(status='stopped', reason=decision['reason'], runs=len(recorded))
        return dict(status='complete' if len(recorded) == 16 else 'pending', runs=len(recorded))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['launch', 'window'])
    parser.add_argument('--study', type=Path, required=True)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--run-id')
    parser.add_argument('--campaign-id'); parser.add_argument('--vm-id')
    parser.add_argument('--seconds', type=int); parser.add_argument('--final', action='store_true')
    args = parser.parse_args(); os.umask(0o077)
    if args.action == 'launch':
        launch(args.study, args.source, args.run_id)
    else:
        from .cloud import execution_receipt
        require(type(args.seconds) is int and 120 <= args.seconds <= 2820, 'invalid onset guest lease')
        receipt = execution_receipt({'campaign_id': args.campaign_id}, {'worker_id': args.vm_id}, args.seconds, socket.gethostname())
        replace_derived(args.study/'execution.json', receipt)
        result = window(args.study, args.source, args.final)
        print(json.dumps(result))


if __name__ == '__main__':
    main()
