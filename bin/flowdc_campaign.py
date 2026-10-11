"""Explicit finite campaign grants; pilot limits and all consumption stay intact."""
import copy
import hashlib
import math
from dataclasses import asdict

import flowdc_ops as ops
from flowdc_pilot import CLOCK_TOLERANCE_SECONDS, ClockSample
from flowdc_pilot_journal import allowance, allowance_binding_sha256, encode, failure, require_grant_idle


def sha(value):
    return hashlib.sha256(encode(value).encode()).hexdigest()


def account_digest(record):
    return sha({key:value['account'] for key,value in record['vms'].items()})


def validate_recovery_request(request):
    """Explicit operator recovery of confirmed activity outside supervision."""
    try:
        ops.fields(request, ('schema', 'recovery_id', 'registration_id', 'vm_id',
            'expected_binding_sha256', 'expected_accounts_sha256', 'evidence_sha256',
            'reason', 'max_elapsed_seconds'))
        if (request['schema'] != 'flowdc-account-recovery-v1'
                or request['reason'] != 'confirmed_external_activity'):
            raise ValueError
        for name in ('recovery_id', 'registration_id', 'vm_id'):
            if ops.uuid_value(request[name]) != request[name]: raise ValueError
        import re
        for name in ('expected_binding_sha256', 'expected_accounts_sha256', 'evidence_sha256'):
            if not isinstance(request[name], str) or not re.fullmatch(r'[0-9a-f]{64}', request[name]):
                raise ValueError
        ops.integer(request['max_elapsed_seconds'], 1, 604800)
    except (KeyError, TypeError, ValueError, ops.OpsError):
        raise failure('invalid_account_recovery', invalid=True) from None
    return request


def find_recovery_receipt(record, request):
    validate_recovery_request(request)
    for event in record['events']:
        if event['kind'] == 'account_recovered' and event['data']['request']['recovery_id'] == request['recovery_id']:
            receipt = event['data']
            if receipt['request'] != request or receipt['request_sha256'] != sha(request):
                raise failure('account_recovery_replay_conflict')
            return copy.deepcopy(receipt)
    return None


def require_recovery_idle(record):
    # Uncertainty is the explicit target; all actual cleanup must already be done.
    if (record['desired'] != 'idle' or record['checkpoint'] is not None
            or not record['network']['rolled_back'] or record['network']['ready']
            or any(vm['phase'] != 'offloaded' or allowance(vm['account']).obligation
                or vm['observed'] is None or vm['observed']['state'] != 'SHELVED_OFFLOADED'
                for vm in record['vms'].values())):
        raise failure('account_recovery_idle_required')


def recovery_bound(record, vm_id, campaign_id, now):
    """Charge all time since a fresh idle campaign proof, including its lookback.

    No provider history is assumed complete, and no old consumption is removed.
    Reboot or inconsistent clocks cannot establish this bound and remain blocked.
    """
    baseline = next((r for r in receipts(record) if r['request']['campaign_id'] == campaign_id), None)
    if baseline is None or vm_id not in baseline['changes']:
        raise failure('account_recovery_baseline_required')
    start = ClockSample(**baseline['clock'])
    before = allowance(baseline['changes'][vm_id]['before'])
    elapsed, wall = now.boottime - start.boottime, now.utc - start.utc
    if (before.uncertain or before.obligation or start.boot_id != now.boot_id
            or not 0 <= elapsed <= 604680 or not 0 <= wall <= 604680
            or abs(wall - elapsed) > CLOCK_TOLERANCE_SECONDS):
        raise failure('account_recovery_clock_bound_required')
    # Campaign application requires its whole provider proof to finish in120s.
    # Include that interval: the last server read may precede the receipt clock.
    return baseline, math.ceil(max(elapsed, wall) + 120)


def recovery_preview(record, request, now):
    validate_recovery_request(request)
    existing = find_recovery_receipt(record, request)
    if existing is not None: return copy.deepcopy(record), existing, False
    require_recovery_idle(record)
    key = request['vm_id']
    if (request['registration_id'] != record['registration_id']
            or request['expected_binding_sha256'] != allowance_binding_sha256(record)
            or request['expected_accounts_sha256'] != account_digest(record)
            or key not in record['vms']):
        raise failure('account_recovery_expectation_changed')
    old = allowance(record['vms'][key]['account'])
    if not old.uncertain: raise failure('account_recovery_uncertainty_required')
    baseline, elapsed = recovery_bound(record, key, old.campaign_id, now)
    if elapsed > request['max_elapsed_seconds']:
        raise failure('account_recovery_elapsed_limit')
    updated = asdict(old)
    updated.update(uncertain=False, consumed=max(old.consumed,
        baseline['changes'][key]['before']['consumed'] + elapsed))
    allowance(updated)
    candidate = copy.deepcopy(record)
    candidate['vms'][key]['account'] = updated
    receipt = {'request':copy.deepcopy(request), 'request_sha256':sha(request),
        'clock':asdict(now), 'baseline_campaign_id':old.campaign_id,
        'charged_elapsed_bound_seconds':elapsed, 'before':asdict(old), 'after':updated,
        'limitation':'Conservative operational exposure bound, not measured provider billing; all prior uncertainty and consumption retained here. No allowance or activation granted.'}
    candidate['events'].append({'kind':'account_recovered', 'data':receipt})
    return candidate, receipt, True


def validate_recovery_receipts(record):
    seen = set()
    for event in record['events']:
        if event['kind'] != 'account_recovered': continue
        receipt = ops.fields(event['data'], ('request', 'request_sha256', 'clock',
            'baseline_campaign_id', 'charged_elapsed_bound_seconds', 'before', 'after', 'limitation'))
        request = validate_recovery_request(receipt['request'])
        if (request['recovery_id'] in seen or receipt['request_sha256'] != sha(request)
                or request['registration_id'] != record['registration_id']
                or request['vm_id'] not in record['vms']): raise ValueError('invalid recovery identity')
        seen.add(request['recovery_id'])
        before, after = allowance(receipt['before']), allowance(receipt['after'])
        baseline, elapsed = recovery_bound(record, request['vm_id'], receipt['baseline_campaign_id'], ClockSample(**receipt['clock']))
        expected = asdict(before)
        expected.update(uncertain=False, consumed=max(before.consumed,
            baseline['changes'][request['vm_id']]['before']['consumed'] + elapsed))
        if (not before.uncertain or before.obligation or after.obligation
                or before.campaign_id != receipt['baseline_campaign_id']
                or elapsed != receipt['charged_elapsed_bound_seconds']
                or elapsed > request['max_elapsed_seconds'] or asdict(after) != expected
                or allowance(record['vms'][request['vm_id']]['account']).consumed < after.consumed):
            raise ValueError('invalid recovery transition')


def validate_request(request):
    try:
        ops.fields(request, ('schema','campaign_id','registration_id','expected_binding_sha256',
            'expected_accounts_sha256','selected_ids','cumulative_limits_seconds','expires_at',
            'max_window_seconds','protocol_sha256','budget_sha256','budget_su'))
        if request['schema'] != 'flowdc-campaign-authorization-v1': raise ValueError
        for name in ('campaign_id','registration_id'):
            if ops.uuid_value(request[name]) != request[name]: raise ValueError
        import re
        for name in ('expected_binding_sha256','expected_accounts_sha256','protocol_sha256','budget_sha256'):
            if not isinstance(request[name],str) or not re.fullmatch(r'[0-9a-f]{64}',request[name]): raise ValueError
        ids=request['selected_ids']
        if not isinstance(ids,list) or not 3 <= len(ids) <= 6 or ids != sorted(set(ids)): raise ValueError
        for key in ids:
            if ops.uuid_value(key) != key: raise ValueError
        if set(request['cumulative_limits_seconds']) != set(ids): raise ValueError
        for value in request['cumulative_limits_seconds'].values(): ops.integer(value,1,604800)
        ops.integer(request['max_window_seconds'],601,1800)
        ops.timestamp(request['expires_at'])
        budget=ops.number(request['budget_su'])
        if not 0 < budget <= 1e9: raise ValueError
    except (KeyError,TypeError,ValueError,ops.OpsError):
        raise failure('invalid_campaign_authorization',invalid=True) from None
    return request


def receipts(record):
    return [event['data'] for event in record['events'] if event['kind']=='campaign_authorized']


def find_receipt(record, request):
    for receipt in receipts(record):
        if receipt['request']['campaign_id']==request['campaign_id']:
            if receipt['request_sha256'] != sha(request) or receipt['request'] != request:
                raise failure('campaign_replay_conflict')
            return copy.deepcopy(receipt)
    return None


def preview(record, request, now):
    validate_request(request)
    receipt=find_receipt(record,request)
    if receipt is not None:
        return copy.deepcopy(record),receipt,False
    require_grant_idle(record)
    if (request['registration_id'] != record['registration_id']
            or request['expected_binding_sha256'] != allowance_binding_sha256(record)
            or request['expected_accounts_sha256'] != account_digest(record)
            or not set(request['selected_ids']) <= set(record['vms'])):
        raise failure('campaign_expectation_changed')
    expiry=ops.timestamp(request['expires_at']).timestamp()
    if not now.utc + request['max_window_seconds'] < expiry <= now.utc + 7*86400:
        raise failure('campaign_expiry_outside_bound')
    candidate=copy.deepcopy(record)
    changes={}
    estimated=0
    selected={vm['id']:vm for vm in record['spec']['vms']}
    for key in request['selected_ids']:
        old=allowance(record['vms'][key]['account'])
        limit=request['cumulative_limits_seconds'][key]
        if limit <= old.effective_limit or limit-old.consumed < request['max_window_seconds']:
            raise failure('campaign_limit_not_increased_or_insufficient')
        rate=selected[key]['rate']
        if rate is None or rate['verified'] is not True or not rate['su_per_hour'] > 0:
            raise failure('campaign_verified_rate_required')
        estimated += (limit-old.consumed)*rate['su_per_hour']/3600
        updated=asdict(old)
        updated.update(campaign_id=request['campaign_id'],campaign_limit=limit,
            campaign_expires_utc=expiry,campaign_window_seconds=request['max_window_seconds'])
        allowance(updated)
        candidate['vms'][key]['account']=updated
        changes[key]={'before':copy.deepcopy(record['vms'][key]['account']),'after':copy.deepcopy(updated)}
    if estimated > request['budget_su']:
        raise failure('campaign_allowance_exceeds_compute_budget')
    receipt={'request':copy.deepcopy(request),'request_sha256':sha(request),'clock':asdict(now),
        'changes':changes,'conservative_allowance_su':estimated,
        'limitation':'Provider cleanup delay can exceed account/SU estimates; obligations continue until observed offload.'}
    candidate['events'].append({'kind':'campaign_authorized','data':receipt})
    return candidate,receipt,True


def validate_receipts(record):
    ids,latest=set(),{}
    for receipt in receipts(record):
        ops.fields(receipt, ('request','request_sha256','clock','changes','conservative_allowance_su','limitation'))
        request=validate_request(receipt['request'])
        if request['campaign_id'] in ids or receipt['request_sha256'] != sha(request): raise ValueError('invalid campaign receipt')
        ids.add(request['campaign_id']); now = ClockSample(**receipt['clock'])
        expiry = ops.timestamp(request['expires_at']).timestamp()
        if not now.utc + request['max_window_seconds'] < expiry <= now.utc + 7*86400:
            raise ValueError('campaign receipt expiry outside bound')
        if request['registration_id'] != record['registration_id'] or set(receipt['changes']) != set(request['selected_ids']): raise ValueError('campaign registry mismatch')
        estimated = 0
        rates = {vm['id']:vm['rate'] for vm in record['spec']['vms']}
        for key,entry in receipt['changes'].items():
            ops.fields(entry, ('before','after'))
            before,after=allowance(entry['before']),allowance(entry['after'])
            previous = latest.get(key)
            if previous is not None and (before.consumed < previous.consumed or any(
                    getattr(before,name) != getattr(previous,name) for name in
                    ('campaign_id','campaign_limit','campaign_expires_utc','campaign_window_seconds'))):
                raise ValueError('campaign history changed')
            baseline=asdict(after)
            for name in ('campaign_id','campaign_limit','campaign_expires_utc','campaign_window_seconds'):
                baseline[name]=getattr(before,name)
            if (after.campaign_id != request['campaign_id']
                    or after.campaign_limit != request['cumulative_limits_seconds'][key]
                    or after.campaign_expires_utc != ops.timestamp(request['expires_at']).timestamp()
                    or after.campaign_window_seconds != request['max_window_seconds']
                    or baseline != asdict(before)
                    or after.effective_limit <= before.effective_limit
                    or after.remaining < request['max_window_seconds']): raise ValueError('invalid campaign transition')
            rate = rates[key]
            if rate is None or rate['verified'] is not True or not rate['su_per_hour'] > 0:
                raise ValueError('campaign receipt needs verified positive rate')
            estimated += after.remaining*rate['su_per_hour']/3600
            latest[key]=after
        if receipt['conservative_allowance_su'] != estimated or estimated > request['budget_su']:
            raise ValueError('campaign receipt budget mismatch')
    for key,vm in record['vms'].items():
        account=allowance(vm['account'])
        previous=latest.get(key)
        if account.campaign_id is None and previous is None: continue
        if previous is None or any(getattr(account,name) != getattr(previous,name)
                for name in ('campaign_id','campaign_limit','campaign_expires_utc','campaign_window_seconds')):
            raise ValueError('campaign account has no matching authorization')
        if account.consumed < previous.consumed: raise ValueError('campaign consumption regressed')
