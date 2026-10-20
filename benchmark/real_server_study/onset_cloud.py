"""Two-window onset campaign bridge; preserves the existing account history."""
import datetime as dt
import json
import math
import os
from pathlib import Path
import subprocess
import time
from uuid import uuid4

from . import cloud, onset, storage
from .common import file_hash, lock, read_json, require, source_identity, utc, write_new


def prepare(study, source, previous_packet, packet):
    """Offline packet only. Reuse trusted guests and the retained volume."""
    study, source, previous_packet, packet = [Path(p).resolve() for p in (study, source, previous_packet, packet)]
    onset.check(study, source)
    require(not packet.exists(), 'onset deployment exists')
    require(not subprocess.check_output(['git', '-C', str(source), 'status', '--porcelain'], text=True).strip(), 'commit source before staging')
    previous = read_json(previous_packet/'operator.json')
    Journal, _, _, _ = cloud.imports(source)
    journal = Journal(previous['state']); current = journal.read()
    require(current['desired'] == 'idle' and not any(v['account']['obligation'] or v['account']['uncertain']
            for v in current['vms'].values()), 'existing cleanup unresolved')
    selected = {v['role']: v['id'] for v in current['spec']['vms'] if v['role'] in ('manager', 'worker', 'origin')}
    packet.mkdir(mode=0o700, parents=True)
    identity = source_identity(source); campaign = str(uuid4())
    with (packet/'source.tar').open('xb') as stream:
        subprocess.run(['git', '-C', str(source), 'archive', 'HEAD'], stdout=stream, check=True, timeout=60)
    write_new(packet/'deployment.json', dict(campaign_id=campaign, source=identity, selected=selected,
              source_archive_sha256=file_hash(packet/'source.tar'), registered_rate_total_per_hour=32,
              limits=onset.LIMITS, protocol_sha256=file_hash(study/'protocol.json'),
              execution_before_merge_authorized=True, authority='Maintainer: Implement the bounded onset plan.'))
    op = {**previous, 'source': str(source), 'study': str(study),
          'branch': subprocess.check_output(['git', '-C', str(source), 'branch', '--show-current'], text=True).strip(),
          'guest_root': previous['guest_root']+'/onset-'+campaign, 'previous_packet': str(previous_packet)}
    write_new(packet/'operator.json', op)
    write_new(packet/'ssh-identity.json', read_json(previous_packet/'ssh-identity.json'))
    write_new(packet/'retained-volume.json', dict(volume=read_json(previous_packet/'volume.json'),
              mount=read_json(previous_packet/'volume-mount.json'), root=previous['guest_root']))
    storage.prepare_payload(packet, study)
    return read_json(packet/'deployment.json')


def local_status(journal):
    from flowdc_pilot_cli import status
    return status(journal)[0]['data']


def exposure(record, journal):
    from flowdc_pilot_journal import allowance
    from flowdc_pilot_supervisor import sample_clock
    current, clock = journal.read(), sample_clock()
    return sum((allowance(current['vms'][k]['account']).account(clock).consumed-value)*record['rates'][k]/3600
               for k, value in record['baseline'].items())


def wall_left(record):
    from flowdc_pilot_supervisor import sample_clock
    now = sample_clock(); clock = record['clock']
    require(now.boot_id == clock['boot_id'], 'controller reboot invalidated campaign clock')
    return min(clock['utc']+14400-now.utc, clock['boottime']+14400-now.boottime)


def authorize(packet):
    deployment, op = read_json(packet/'deployment.json'), read_json(packet/'operator.json')
    Journal, allowance, binding, account_digest = cloud.imports(op['source'])
    from flowdc_pilot_provider import Provider
    from flowdc_pilot_supervisor import sample_clock
    import flowdc_ops as ops
    journal = Journal(op['state']); current = journal.read()
    qualification = read_json(packet/'qualification.json')
    require(qualification['head'] == deployment['source']['head'] and qualification['ci_passed'] and
            qualification['review_accepted'] and qualification['review_head'] == qualification['head'], 'qualified current-head review/CI required')
    require(source_identity(op['source']) == deployment['source'], 'prepared source changed')
    require(not subprocess.check_output(['git', '-C', op['source'], 'status', '--porcelain'], text=True).strip(), 'uncommitted deployment source')
    facts = read_json(op['allocation_file'])
    require(facts['allocation'] == 'CIS240296' and facts['cpu_su'] >= 80 and
            time.time()+18000 < dt.datetime.fromisoformat(facts['expiry_date']).replace(tzinfo=dt.UTC).timestamp(), 'allocation facts insufficient')
    target = packet/'campaign.json'
    if target.exists():
        record = read_json(target)
        require(record['limits'] == onset.LIMITS and record['protocol_sha256'] == deployment['protocol_sha256'], 'campaign limits changed')
        return journal, record
    Provider(ops.load_profile(current['profile_path'])).verify_idle(current)
    rates = cloud.verify_rates(Provider(ops.load_profile(current['profile_path'])), current, deployment)
    clock = sample_clock()
    chosen = sorted(deployment['selected'].values())
    record = dict(campaign_id=deployment['campaign_id'], limits=onset.LIMITS, clock=vars(clock),
                  protocol_sha256=deployment['protocol_sha256'], allocation_basis=facts,
                  baseline={k:current['vms'][k]['account']['consumed'] for k in chosen},
                  rates={r['id']:r['su_per_hour'] for r in rates['servers']})
    # The legacy account API requires monotonically increasing cumulative grants.
    # Preserve its already-authorized unused ceiling; this driver separately
    # admits <=2 windows and <=80SU NEW exposure, not another1000SU campaign.
    limits = {k:max(math.ceil(allowance(current['vms'][k]['account']).effective_limit)+1,
                    math.ceil(current['vms'][k]['account']['consumed'])+9000) for k in chosen}
    preview = sum((limits[k]-record['baseline'][k])*record['rates'][k]/3600 for k in chosen)
    require(preview <= 1000, 'preserved cumulative authority exceeds prior1000SU bound')
    record.update(preserved_cumulative_preview_su=preview, new_admission_limit_su=80)
    write_new(target, record); write_new(packet/'rates.json', rates)
    request = dict(schema='flowdc-campaign-authorization-v1', campaign_id=record['campaign_id'],
                   registration_id=current['registration_id'], expected_binding_sha256=binding(current),
                   expected_accounts_sha256=account_digest(current), selected_ids=chosen,
                   cumulative_limits_seconds=limits,
                   expires_at=dt.datetime.fromtimestamp(clock.utc+14400, dt.UTC).isoformat(),
                   max_window_seconds=3600, protocol_sha256=file_hash(target), budget_sha256=file_hash(target), budget_su=1000)
    write_new(packet/'grant.json', request)
    path = cloud.operational_grant(packet, op)
    service = current['service']
    call = subprocess.run([service['interpreter'], '-E', '-s', '-B', str(Path(service['release'])/'flowdc_ops.py'),
             'pilot', 'campaign-apply', '--state-root', op['state'], '--grant', str(path)], capture_output=True, timeout=180)
    write_new(packet/'grant-application.json', dict(returncode=call.returncode,
              stdout=call.stdout.decode('utf8', 'replace'), stderr=call.stderr.decode('utf8', 'replace')))
    require(call.returncode == 0, 'onset allowance refused; no activation')
    return journal, record


def ready(journal):
    from flowdc_experiment_transport import ready as is_ready
    deadline = time.monotonic()+360
    while True:
        value = local_status(journal)
        if is_ready(value): return
        require(value['desired'] == 'run' and time.monotonic() < deadline, 'activation failed; no replacement start')
        time.sleep(5)


def remote(packet, number, action, seconds, final=False):
    op = read_json(packet/'operator.json'); deployment = read_json(packet/'deployment.json')
    installation = read_json(packet/'deployment-receipt.json')
    root = op['guest_root']; unit = f"flowdc-onset-{deployment['campaign_id']}-{number}-{action}"
    command = [installation['python'], '-B', '-m', 'benchmark.real_server_study.onset_run', 'window',
               '--study', root+'/study', '--source', root+'/source', '--campaign-id', deployment['campaign_id'],
               '--vm-id', op['worker_id'], '--seconds', str(seconds)]
    if final: command.append('--final')
    if action == 'audit':
        script = "from pathlib import Path;import json,sys;from benchmark.real_server_study.onset_analysis import audit;print(json.dumps(audit(Path(sys.argv[1]),Path(sys.argv[2]),int(sys.argv[3]))))"
        command = [installation['python'], '-B', '-c', script, root+'/study', root+'/source', str(seconds)]
    wrapper = ['sudo', '-n', 'systemd-run', '--quiet', '--wait', '--pipe', '--collect', '--unit', unit,
               '--property=RuntimeMaxSec='+str(seconds+15), '--property=TimeoutStopSec=30',
               '--property=KillMode=control-group', '--property=MemoryMax=24G', '--property=CPUQuota=400%',
               '--property=LimitNOFILE=4096', '--property=UMask=0077', '--uid=exouser', '--gid=exouser',
               '--setenv=POLARS_MAX_THREADS=4', '--setenv=OMP_NUM_THREADS=4', '--setenv=OPENBLAS_NUM_THREADS=4',
               '--working-directory='+root+'/source', *command]
    raw = cloud.ssh(packet, wrapper, timeout=seconds+60)
    return json.loads(raw)


def run(packet):
    packet = Path(packet).resolve(); os.umask(0o077)
    deployment, op = read_json(packet/'deployment.json'), read_json(packet/'operator.json')
    Journal, _, _, _ = cloud.imports(op['source'])
    from flowdc_experiment_transport import idle, remaining
    from flowdc_pilot_provider import Provider
    import flowdc_ops as ops
    with lock(packet/'controller.lock'):
        if (packet/'collected').exists() or (packet/'return.tar').exists():
            cloud.collect(packet, metadata_limit=onset.LIMITS['metadata_bytes'])
            from .onset_analysis import report
            report(packet/'collected')
            return {'status':'report_rebuilt', 'cloud_activation':False}
        journal, record = authorize(packet)
        for number in range(2):
            closed = packet/f'window-{number:02}.json'
            if closed.exists():
                require(read_json(closed)['cleaned'], 'cleanup unresolved'); continue
            require(not (packet/f'window-{number:02}-intent.json').exists(), 'unfinished window; reconcile, never start replacement')
            require(wall_left(record) >= 3600 and exposure(record, journal)+32 <= 80, 'finite time/SU admission exhausted')
            current = journal.read()
            require(all(current['vms'][key]['account']['campaign_id'] == record['campaign_id'] for key in record['baseline']), 'campaign binding changed')
            Provider(ops.load_profile(current['profile_path'])).verify_idle(current)
            controller = cloud.controller_for(packet); controller.preflight(3600)
            write_new(packet/f'window-{number:02}-intent.json', {'utc':utc(), 'campaign_id':record['campaign_id']})
            outcome = None
            try:
                controller.call('start', seconds=30, window=3600); ready(journal)
                cloud.refresh_ssh(packet, controller); cloud.wait_ssh(packet, controller, number)
                volume = read_json(packet/'retained-volume.json')
                cloud.ssh(packet, ['sudo', '-n', '/usr/bin/python3', '-c', storage.MOUNT,
                    volume['volume']['volume_id'], volume['root'], volume['mount']['filesystem_uuid']], timeout=180)
                if not (packet/'deployment-receipt.json').exists():
                    require(not (packet/'staging-intent.json').exists(), 'staging interrupted; no overwrite')
                    write_new(packet/'staging-intent.json', {'utc':utc()})
                    cloud.ssh(packet, ['mkdir', '--', op['guest_root']])
                    storage.stage(packet)
                seconds = int(min(2820, remaining(local_status(journal))-60, wall_left(record)-780))
                if seconds >= 120:
                    outcome = remote(packet, number, 'acquire', seconds, final=number == 1)
                    write_new(packet/f'window-{number:02}-guest.json', outcome)
                if outcome and outcome['status'] == 'cooldown' and (number == 1 or
                        outcome['until_utc']-time.time()+3600 >= wall_left(record)):
                    write_new(packet/'recovery-skipped.json', {'utc':utc(), 'reason':'recovery cannot fit finite windows/expiry',
                              'requested_cooldown_until':outcome['until_utc']})
                    outcome = {**outcome, 'status':'stopped', 'recovery':'not attempted: budget/expiry'}
                if outcome and outcome['status'] != 'cooldown':
                    seconds = int(min(1200, remaining(local_status(journal))-120))
                    if seconds >= 60:
                        audited = remote(packet, number, 'audit', seconds)
                        write_new(packet/f'window-{number:02}-audit.json', audited)
                        if outcome['status'] in ('complete', 'stopped') and not audited['audit_pending']:
                            seconds = int(min(900, remaining(local_status(journal))-60))
                            if seconds >= 60:
                                cloud.collect(packet, seconds=seconds, metadata_limit=onset.LIMITS['metadata_bytes'])
            finally:
                controller.call('stop', seconds=30)
                deadline = time.monotonic()+600
                while not idle(local_status(journal)):
                    require(time.monotonic() < deadline, 'cleanup unresolved; supervisor remains running')
                    time.sleep(5)
                Provider(ops.load_profile(journal.read()['profile_path'])).verify_idle(journal.read())
                write_new(closed, {'utc':utc(), 'cleaned':True, 'outcome':outcome,
                          'new_exposure_su':exposure(record, journal), 'fresh_offload_and_network_verified':True})
            if (packet/'collected').exists():
                from .onset_analysis import report
                report(packet/'collected')
                write_new(packet/'complete.json', {'utc':utc(), 'windows_used':number+1,
                          'new_exposure_su':exposure(record, journal), 'cleanup_complete':True})
                return read_json(packet/'complete.json')
            if outcome and outcome['status'] == 'cooldown':
                wait = outcome['until_utc']-time.time()
                require(wait+3600 < wall_left(record), 'cooldown cannot fit; no further image requests')
                while wait > 0:
                    time.sleep(min(30, wait)); wait = outcome['until_utc']-time.time()
        raise ValueError('two-window envelope exhausted; retain all artifacts, no automatic third activation')
