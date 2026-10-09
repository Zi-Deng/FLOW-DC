"""Explicit finite campaign grants; pilot limits and all consumption stay intact."""
import copy
import hashlib
from dataclasses import asdict

import flowdc_ops as ops
from flowdc_pilot import ClockSample
from flowdc_pilot_journal import allowance, allowance_binding_sha256, encode, failure, require_grant_idle


def sha(value):
    return hashlib.sha256(encode(value).encode()).hexdigest()


def account_digest(record):
    return sha({key:value['account'] for key,value in record['vms'].items()})


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
