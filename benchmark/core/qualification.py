"""Retained request-observation checks and realized controlled stimuli."""
import math
import re
from collections import Counter
from pathlib import Path

from .truth import digest, encode, parse, require, truth_workload
from .verifier import archive_members, read_file


def observations(native, truth):
    """Use the archived overview and verify the retained observation inventory."""
    native = Path(native)
    limits = truth_workload(truth)
    members = archive_members(native.parent / (native.name+'.tar'), limits)
    overview = parse(members[native.name+'/overview.json'])
    descriptor = overview['http_observations']
    require(descriptor['schema'] == 'flowdc-http-observations-v1'
            and digest(encode(descriptor)) == overview['output_integrity']['http_observations_sha256'],
            'HTTP descriptor binding mismatch')
    expected_rows = {row['row_id'] for row in truth['rows'] if row['eligible']}
    found, records = set(), []
    for item in descriptor['files']:
        name = item['path']
        require(re.fullmatch(r'\.flowdc/attempts/[0-9a-f]{64}/[1-9][0-9]*/measurement\.json', name)
                and name not in found, 'unsafe/duplicate HTTP observation path')
        found.add(name)
        raw = read_file(native/name, limits.max_row_metadata_bytes)
        require(len(raw) == item['bytes'] and digest(raw) == item['sha256'], 'HTTP observation digest mismatch')
        record = parse(raw)
        intent = parse(read_file((native/name).with_name('intent.json'), limits.max_row_metadata_bytes))
        require(record['schema'] == 'flowdc-http-observation-v1'
                and record['measurement_version'] == '4-output-independent-delay-signals'
                and record['row_id'] in expected_rows
                and record['run_id'] == overview['output_integrity']['run_id']
                and all(record.get(k) == v for k,v in intent.items()), 'HTTP observation identity mismatch')
        require(type(record['latency_eligible']) is bool, 'invalid eligibility')
        if record['latency_eligible']:
            require(record['status'] == 200 and record['observed_response_body_bytes'] > 0
                    and all(type(record[k]) in (int,float) and math.isfinite(record[k]) and record[k] > 0
                            for k in ('ttfb','body_delay','t0','first_body_byte_at','body_completed_at'))
                    and record['t0'] <= record['first_body_byte_at'] <= record['body_completed_at']
                    and record['ttfb'] <= record['body_delay'], 'invalid eligible request timing')
        records.append(record)
    actual = {p.relative_to(native).as_posix() for p in (native/'.flowdc/attempts').glob('*/*/measurement.json')}
    require(actual == found, 'HTTP observation inventory mismatch')
    intents = {p.parent.relative_to(native).as_posix() for p in (native/'.flowdc/attempts').glob('*/*/intent.json')}
    missing = intents - {str(Path(name).parent) for name in found}
    require(set(descriptor['missing_attempts']) == missing and descriptor['complete'] == (not missing),
            'HTTP completeness mismatch')
    samples = sorted(r['ttfb'] for r in records if r['latency_eligible'])
    position = .95*(len(samples)-1)
    low, high = math.floor(position), math.ceil(position)
    p95 = samples[low] + (samples[high]-samples[low])*(position-low) if len(samples) >= 100 and not missing else None
    return records, {'eligible_latency_samples':len(samples), 'p95_s':p95, 'complete':not missing,
                     'attempts':len(records), 'missing_attempts':sorted(missing),
                     'failures':dict(Counter(r['failure_kind'] or ('http' if r['status'] != 200 else 'none') for r in records))}


def realized_stimulus(plan, events, *, minimum_phase_requests=100, averaging_observations=1800):
    """Origin-local phases; never subtract worker and origin clock epochs."""
    require(plan.get('schema') == 'flowdc-origin-scenario-v2', 'explicit research stimulus required')
    responses = [e for e in events if e['phase'] == 'response']
    starts = [e for e in events if e['phase'] == 'service_start']
    end = max((e['origin_elapsed_s'] for e in events), default=0)
    checks = {}
    for index,(at,slots) in enumerate(plan['schedule']):
        stop = plan['schedule'][index+1][0] if index+1 < len(plan['schedule']) else end
        count = sum(at <= e['origin_elapsed_s'] < stop for e in starts)
        checks[f'capacity_phase_{index}'] = {'passed': count >= minimum_phase_requests,
            'start_s':at,'end_s':stop,'slots':slots,'service_starts':count,'minimum':minimum_phase_requests}
    for index,(at,stop) in enumerate(plan.get('overload_windows', [])):
        hits = [e for e in responses if e.get('overload_stimulus') and e['status'] in (429,503)
                and at <= e['origin_elapsed_s'] <= stop+1]
        duration = max((e['origin_elapsed_s'] for e in hits),default=at)-min((e['origin_elapsed_s'] for e in hits),default=at)
        expected_duration = min(stop-at, 10)
        checks[f'overload_{index}'] = {'passed':len(hits) >= 10 and duration >= .8*expected_duration,
            'responses':len(hits),'observed_span_s':duration,'required_span_s':.8*expected_duration}
    good = [e for e in responses if e['status'] == 200 and not e['disconnected'] and e['body_bytes_written']]
    if plan['name'] in ('drop-recovery','mixed-sizes','steady','baseline-drift','oscillation'):
        after = plan['schedule'][-1][0]
        count = sum(e['origin_elapsed_s'] >= after for e in good)
        checks['averaging_observations'] = {'passed':count >= averaging_observations,
            'after_s':after,'completed_origin_200':count,'minimum':averaging_observations,
            'interpretation':'Three long-window lengths; exact Gradient2 adaptation utilization is checked in controller traces.'}
    if plan['name'] == 'baseline-drift':
        durations = [e['service_duration_s'] for e in responses if e.get('service_duration_s') is not None]
        checks['baseline_drift'] = {'passed':bool(durations) and max(durations)-min(durations) >= .04,
                                   'observed_service_range_s':max(durations)-min(durations) if durations else 0}
    checks['duration'] = {'passed':end >= max(30, plan['schedule'][-1][0]+10), 'observed_s':end}
    return {'schema':'flowdc-realized-stimulus-v1','scenario':plan['name'],'checks':checks,
            'qualified':all(item['passed'] for item in checks.values()),
            'limit':'Origin realization only; controller decisions, accounting and resource fit are separate gates.'}
