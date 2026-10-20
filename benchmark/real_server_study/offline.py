"""Two-window offline continuation of closed runs; never launches acquisition."""
import datetime as dt
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import time
from uuid import uuid4

from . import cloud
from .common import file_hash, lock, read_json, require, utc, write_new

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'bin'))
from flowdc_campaign import account_digest
from flowdc_experiment_transport import idle, ready, remaining
from flowdc_pilot_cli import status
from flowdc_pilot_journal import Journal, allowance, allowance_binding_sha256
from flowdc_pilot_provider import Provider
from flowdc_pilot_supervisor import sample_clock
import flowdc_ops as ops


WINDOW = 3600
AUDIT_SECONDS = 1200
WALL_SECONDS = 4 * 3600
MAX_ATTEMPTS = 2
BUDGET_SU = 500


def local_status(journal):
    """Read validated local state; no CLI process, provider query or release hash."""
    return status(journal)[0]['data']


def wall_remaining(record):
    clock = record['clock']
    require(clock['boot_id'] == sample_clock().boot_id, 'continuation control host reboot')
    return min(clock['utc'] + WALL_SECONDS - time.time(),
               clock['boottime'] + WALL_SECONDS - time.clock_gettime(time.CLOCK_BOOTTIME))


def exposure(record, journal):
    current = journal.read()
    now = sample_clock()
    return sum((allowance(current['vms'][key]['account']).account(now).consumed - before)
               * record['rates'][key] / 3600 for key, before in record['baseline'].items())


def prepare(packet, directory):
    """Create once immediately before application; never reset an earlier clock."""
    packet, directory = Path(packet).resolve(), Path(directory).resolve()
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    target = directory / 'continuation.json'
    pointer = packet / 'offline-continuation.json'
    if pointer.exists():
        require(read_json(pointer)['directory'] == str(directory), 'study already has a different offline continuation')
    if target.exists():
        record = read_json(target)
        require(record['schema'] == 'flowdc-offline-continuation-v1', 'unsupported continuation')
        require(record['original_packet'] == str(packet), 'continuation packet changed')
        require((record['window_seconds'], record['audit_seconds'], record['max_attempts'],
                 record['wall_seconds'], record['budget_su'], record['collection_seconds']) ==
                (WINDOW, AUDIT_SECONDS, MAX_ATTEMPTS, WALL_SECONDS, BUDGET_SU, 900),
                'continuation limits changed')
        if pointer.exists():
            require(read_json(pointer)['sha256'] == file_hash(target), 'continuation record changed')
        return record
    deployment, operator = read_json(packet / 'deployment.json'), read_json(packet / 'operator.json')
    journal = Journal(operator['state'])
    current = journal.read()
    require(idle(local_status(journal)), 'original cleanup unresolved')
    for intent in packet.glob('window-*-intent.json'):
        closed = intent.with_name(intent.name.replace('-intent', ''))
        require(closed.exists() and read_json(closed)['cleaned'], 'original window closure missing')
    # Reuse reported renewal facts; distinguish them from a new portal observation.
    facts = read_json(operator['allocation_file'])
    require(facts['allocation'] == 'CIS240296' and facts['expiry_date'] == '2027-04-17', 'allocation identity changed')
    require(facts['cpu_su'] >= BUDGET_SU, 'reported allocation balance insufficient')
    cutoff = dt.datetime.fromisoformat(facts['expiry_date']).replace(tzinfo=dt.UTC).timestamp()
    require(time.time() + WALL_SECONDS + 3600 < cutoff, 'allocation expiry insufficient')
    rates = cloud.verify_rates(Provider(ops.load_profile(current['profile_path'])), current, deployment)
    clock = sample_clock()
    record = {'schema': 'flowdc-offline-continuation-v1', 'continuation_id': str(uuid4()),
              'original_packet': str(packet), 'original_campaign_id': deployment['campaign_id'],
              'protocol_sha256': file_hash(packet.parent / 'study/protocol.json'),
              'scientific_source': deployment['source'], 'registration_id': current['registration_id'],
              'selected': deployment['selected'], 'window_seconds': WINDOW, 'audit_seconds': AUDIT_SECONDS,
              'collection_seconds': 900, 'max_attempts': MAX_ATTEMPTS, 'wall_seconds': WALL_SECONDS,
              'budget_su': BUDGET_SU, 'clock': vars(clock),
              'baseline': {row['id']: current['vms'][row['id']]['account']['consumed'] for row in rates['servers']},
              'rates': {row['id']: row['su_per_hour'] for row in rates['servers']},
              'operational_head': subprocess.check_output(['git', '-C', str(Path(__file__).resolve().parents[2]), 'rev-parse', 'HEAD'], text=True).strip(),
              'allocation_basis': facts, 'scope': 'Offline verification and metadata collection only; no HTTP acquisition.'}
    write_new(target, record)
    # The prior 1000-SU cumulative-account authority is not a new spending target.
    # Existing unused balances stay intact. The driver separately enforces500SU.
    grant = {'schema': 'flowdc-campaign-authorization-v1', 'campaign_id': record['continuation_id'],
             'registration_id': current['registration_id'], 'expected_binding_sha256': allowance_binding_sha256(current),
             'expected_accounts_sha256': account_digest(current), 'selected_ids': sorted(record['baseline']),
             'cumulative_limits_seconds': {key: max(math.ceil(allowance(current['vms'][key]['account']).effective_limit) + 1,
                 math.ceil(current['vms'][key]['account']['consumed']) + MAX_ATTEMPTS * WINDOW) for key in record['baseline']},
             'expires_at': dt.datetime.fromtimestamp(clock.utc + WALL_SECONDS, dt.UTC).isoformat(),
             'max_window_seconds': WINDOW, 'protocol_sha256': file_hash(target),
             'budget_sha256': file_hash(target), 'budget_su': 1000}
    write_new(directory / 'grant.json', grant)
    write_new(directory / 'deployment.json', {'campaign_id': record['continuation_id']})
    write_new(directory / 'rates.json', rates)
    write_new(pointer, {'directory': str(directory), 'continuation_id': record['continuation_id'], 'sha256': file_hash(target)})
    return record


def authorize(packet, directory, record):
    require(wall_remaining(record) >= WINDOW, 'continuation expired or full window cannot fit')
    operator = read_json(packet / 'operator.json')
    journal = Journal(operator['state'])
    current = journal.read()
    require(current['registration_id'] == record['registration_id'], 'registration changed')
    request = read_json(directory / 'grant.json')
    require(request['protocol_sha256'] == request['budget_sha256'] == file_hash(directory / 'continuation.json')
            and request['campaign_id'] == record['continuation_id'], 'continuation authorization binding changed')
    source = str(Path(__file__).resolve().parents[2])
    require(subprocess.check_output(['git', '-C', source, 'rev-parse', 'HEAD'], text=True).strip() == record['operational_head']
            and not subprocess.check_output(['git', '-C', source, 'status', '--porcelain'], text=True).strip(),
            'offline operational source changed or uncommitted')
    grant = cloud.operational_grant(directory, operator)
    service = current['service']
    result = subprocess.run([service['interpreter'], '-E', '-s', '-B', str(Path(service['release']) / 'flowdc_ops.py'),
        'pilot', 'campaign-apply', '--state-root', operator['state'], '--grant', str(grant)],
        capture_output=True, timeout=180, check=False)
    value = json.loads(result.stdout)
    require(result.returncode == 0 and not value.get('errors'), 'offline campaign application refused')
    if not (directory / 'grant-receipt.json').exists():
        write_new(directory / 'grant-receipt.json', value)
    return journal


def wait_ready(journal):
    deadline = time.monotonic() + 360
    while True:
        value = local_status(journal)
        if ready(value):
            return value
        require(value['desired'] == 'run', 'activation stopped; inspect provider diagnostic')
        require(time.monotonic() < deadline, 'activation timeout')
        time.sleep(5)


AUDIT = r'''
import importlib,json,os,socket,sys,time,urllib.request
from pathlib import Path
from benchmark.real_server_study.common import lock,read_json,write_new,protocol_check,NoRedirect
study=Path(sys.argv[1]);source=Path(sys.argv[2]);lease=json.loads(sys.argv[3])
started=time.monotonic();deadline=started+lease['seconds']
metadata=urllib.request.build_opener(urllib.request.ProxyHandler({}),NoRedirect())
with metadata.open('http://169.254.169.254/openstack/latest/meta_data.json',timeout=3) as response:
 identity=json.loads(response.read(65536))
if identity.get('uuid')!=lease['vm_id']:raise ValueError('offline guest identity mismatch')
if time.time()+lease['seconds']>lease['expires_at']:raise ValueError('offline lease exceeds continuation')
protocol=protocol_check(study,source)
if protocol['source']!=lease['scientific_source']:raise ValueError('offline frozen source changed')
from benchmark.real_server_study.common import file_hash
if file_hash(study/'protocol.json')!=lease['protocol_sha256']:raise ValueError('offline protocol changed')
lease.update(hostname=socket.gethostname(),started_utc=time.time(),started_monotonic=started,
 boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip())
lease_path=study/'analysis'/('offline-lease-'+lease['continuation_id']+'-'+str(lease['attempt'])+'.json')
write_new(lease_path,lease)
for name in ('POLARS_MAX_THREADS','OMP_NUM_THREADS','OPENBLAS_NUM_THREADS'):os.environ[name]='4'
os.sched_setaffinity(0,sorted(os.sched_getaffinity(0))[:4])
module=importlib.import_module('benchmark.real_server_study.analyze')
''' + cloud.EVIDENCE_CACHE + r'''
with lock(study/'study.lock'):
 schedule=read_json(study/'schedule.json')['runs']
 if len(schedule)!=28:raise ValueError('offline schedule changed')
 if {p.name for p in (study/'runs').iterdir() if p.is_dir()}!={r['run_id'] for r in schedule}:raise ValueError('unexpected offline run directory')
 for planned in schedule:
  row=read_json(study/'runs'/planned['run_id']/'record.json')
  if not row['complete'] or any(row[k]!=v for k,v in planned.items()):raise ValueError('closed scientific record required')
 install_evidence_cache(module,study)
 left=deadline-time.monotonic()-5
 if left<=0:raise TimeoutError('offline cache setup exhausted audit budget')
 result=module.analyze(study,seconds=left)
print(json.dumps({'status':'complete' if result['complete_runs']==28 else 'analysis_pending',
 'complete_runs':result['complete_runs'],'pending_runs':result['analysis_incomplete_runs'],
 'elapsed_s':time.monotonic()-started,'acquisition_calls':0}))
'''


def audit(packet, controller, record, attempt):
    operator, installation = read_json(packet / 'operator.json'), read_json(packet / 'deployment-receipt.json')
    seconds = int(min(AUDIT_SECONDS, remaining(local_status(controller.journal)) - 60, wall_remaining(record) - 780))
    require(seconds >= 60, 'offline audit admission insufficient')
    lease = {'continuation_id': record['continuation_id'], 'attempt': attempt, 'vm_id': operator['worker_id'],
             'protocol_sha256': record['protocol_sha256'], 'scientific_source': record['scientific_source'],
             'seconds': seconds, 'expires_at': record['clock']['utc'] + WALL_SECONDS}
    unit = 'flowdc-offline-' + record['continuation_id'] + '-' + str(attempt)
    command = ['sudo', '-n', 'systemd-run', '--quiet', '--wait', '--pipe', '--collect', '--unit', unit,
        '--property=RuntimeMaxSec=' + str(seconds + 30), '--property=TimeoutStopSec=30', '--property=KillMode=control-group',
        '--property=MemoryMax=24G', '--property=CPUQuota=400%', '--property=LimitNOFILE=4096', '--property=UMask=0077',
        '--uid=exouser', '--gid=exouser', '--working-directory=' + operator['guest_root'] + '/source',
        installation['python'], '-B', '-c', AUDIT, operator['guest_root'] + '/study', operator['guest_root'] + '/source', json.dumps(lease)]
    return json.loads(cloud.ssh(packet, command, timeout=seconds + 60))


def close_window(packet, directory, controller, outcome, attempt):
    controller.call('stop', seconds=30)
    deadline = time.monotonic() + 600
    while not idle(local_status(controller.journal)):
        require(time.monotonic() < deadline, 'offline cleanup unresolved; keep supervisor running')
        time.sleep(5)
    current = controller.journal.read()
    Provider(ops.load_profile(current['profile_path'])).verify_idle(current)
    write_new(directory / f'attempt-{attempt:02}.json', {'utc': utc(), 'cleaned': True, 'outcome': outcome,
              'cleanup': local_status(controller.journal),
              'fresh_provider_verification': 'offload and original networking verified after durable idle'})


def run(packet, directory):
    """Explicit operator invocation; failed attempts stop rather than replaying."""
    previous_mask = os.umask(0o077)
    try:
        return run_private(packet, directory)
    finally:
        os.umask(previous_mask)


def run_private(packet, directory):
    packet, directory = Path(packet).resolve(), Path(directory).resolve()
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    with lock(packet / 'controller.lock'), lock(directory / 'controller.lock'):
        # Local report/extraction retries never authorize or activate a guest.
        if (packet / 'collected').exists() or (packet / 'return.tar').exists():
            cloud.collect(packet)
            from .analyze import report
            report(packet / 'collected')
            operator = read_json(packet / 'operator.json')
            return {'status': 'report_rebuilt', 'cloud_activation': False,
                    'cleanup_complete': idle(local_status(Journal(operator['state'])))}
        record = prepare(packet, directory)
        journal = authorize(packet, directory, record)
        for attempt in range(1, MAX_ATTEMPTS + 1):
            receipt = directory / f'attempt-{attempt:02}.json'
            if receipt.exists():
                require(read_json(receipt)['cleaned'], 'prior continuation cleanup unresolved')
                continue
            intent = directory / f'attempt-{attempt:02}-intent.json'
            require(not intent.exists(), 'unfinished continuation attempt requires diagnosis')
            require(wall_remaining(record) >= WINDOW, 'whole offline window cannot fit before expiry')
            require(exposure(record, journal) + sum(record['rates'].values()) <= BUDGET_SU, 'continuation compute budget insufficient')
            controller = cloud.controller_for(packet)
            current = journal.read()
            require(all(current['vms'][key]['account']['campaign_id'] == record['continuation_id'] for key in record['baseline']),
                    'offline campaign binding changed')
            controller.preflight(WINDOW)
            write_new(intent, {'utc': utc(), 'continuation_id': record['continuation_id'], 'attempt': attempt})
            outcome = None
            try:
                controller.call('start', seconds=30, window=WINDOW)
                wait_ready(journal)
                cloud.refresh_ssh(packet, controller)
                cloud.wait_ssh(packet, controller, 100 + attempt)
                from .storage import mount
                mount(packet)
                if not (directory / 'audit-complete.json').exists():
                    outcome = audit(packet, controller, record, attempt)
                    write_new(directory / f'audit-{attempt:02}.json', outcome)
                    if outcome['status'] == 'complete':
                        write_new(directory / 'audit-complete.json', outcome)
                else:
                    outcome = read_json(directory / 'audit-complete.json')
                if outcome['status'] == 'complete':
                    seconds = int(min(900, remaining(local_status(journal)) - 120))
                    if seconds >= 60:
                        cloud.collect(packet, seconds=seconds)
            finally:
                close_window(packet, directory, controller, outcome, attempt)
            if (packet / 'collected').exists():
                require((packet / 'collected/metadata-manifest.json').is_file(), 'offline collection manifest missing')
                from .analyze import report
                report(packet / 'collected')
                write_new(directory / 'complete.json', {'utc': utc(), 'aggregate_audit_complete_runs': 28,
                    'new_exposure_su': exposure(record, journal), 'attempts': attempt, 'cleanup_complete': True})
                return read_json(directory / 'complete.json')
        raise ValueError('two offline attempts exhausted; retain partial evidence, no automatic additional activation')
