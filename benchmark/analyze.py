#!/usr/bin/env python3
"""Re-verify retained first attempts and render the declared primary contrasts."""
import argparse
import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from benchmark.core.inference import PRIMARY, GRADIENT, REFERENCES, primary_analysis
from benchmark.core.qualification import observations, control_observations
from benchmark.core.study import authorize_plan, read_plan, validate_plan, write_new
from benchmark.core.truth import digest, encode, parse, require
from benchmark.core.verifier import verify_native
from benchmark.study import attempts_for, identities


def first_attempt(directory, cell, binding, config):
    result = dict(scenario=cell['scenario'], block=cell['block'], method=cell['method'],
                  cell_id=cell['cell_id'], attempt=1, status='missing', later_attempts=[])
    attempts = attempts_for(directory / cell['cell_id'], cell)
    if not attempts:
        return result
    first = attempts[0]
    result['attempt_id'] = first.name
    result['later_attempts'] = [p.name for p in attempts[1:]]
    record_path = first / 'record.json'
    if not record_path.exists():
        return dict(result, status='censored', reason='first attempt has no closed record')
    raw = record_path.read_bytes()
    record = parse(raw)
    result['record_sha256'] = digest(raw)
    require(record['cell'] == cell and record['attempt_id'] == first.name
            and record['source'] == {k:binding[k] for k in ('source_sha256','environment_sha256')},
            'first attempt identity/source mismatch')
    if record['status'] != 'recorded' or record.get('native') is None:
        return dict(result, status='failed' if record['status'] == 'failed' else 'censored',
                    reason=record.get('reason',record['status']))
    try:
        native = parse((first/'run/result.json').read_bytes())
        require(native == record['native'], 'run record changed after closure')
        truth = parse((first/'fixture/truth.json').read_bytes())
        provenance = native['provenance']
        require(provenance['truth_sha256'] == digest(encode(truth))
                and provenance['cell'] == cell
                and all(provenance[k] == binding[k] for k in ('source_sha256','environment_sha256')),
                'truth/provenance mismatch')
        outcomes_raw = (first/'run/outcomes.json').read_bytes()
        require(digest(outcomes_raw) == native['outcome_index_sha256'], 'common index changed')
        outcomes = parse(outcomes_raw)
        independent = verify_native('flowdc', first/'run/native', truth)
        require(independent['artifacts_valid'] and all(independent[k] == outcomes[k] for k in independent),
                'independent output re-verification differs')
        rows = independent['rows']
        require(len(rows) == truth['original_rows'] == native['original_rows'], 'original denominator changed')
        useful = sum(row['useful_bytes'] for row in rows)
        verified = sum(row['disposition'] == 'verified' for row in rows)
        elapsed = native['elapsed_ns']
        require(type(elapsed) is int and elapsed > 0 and useful == native['useful_payload_bytes']
                and verified == native['verified_rows']
                and verified/len(rows) == native['verified_coverage'], 'credited metrics changed')
        request_records, timing = observations(first/'run/native', truth)
        mechanisms=control_observations(first/'run/native',truth,config,request_records)
        terminal = (native['process_exit_code'] == 0 and native['status'] in ('complete','incomplete')
            and not native['cleanup_errors'] and not native['interruption_requested'] and timing['complete'] and mechanisms['complete']
            and all(row['disposition'] in ('verified','failed','skipped')
                    and not row.get('native_attempt_information_uncertain',False) for row in rows))
        result.update(status='known_terminal' if terminal else 'censored', goodput=useful/(elapsed/1e9),
            coverage=verified/len(rows), useful_bytes=useful, original_rows=len(rows), elapsed_s=elapsed/1e9,
            verification_s=native['verification_ns']/1e9, process_s=native['process_ns']/1e9,
            resources=native['resources'], mechanisms=mechanisms, **timing)
        if not terminal:
            result['reason']='incomplete request/accounting or process/cleanup failure; excluded from complete-run inference'
    except (ValueError, OSError, KeyError, TypeError, ZeroDivisionError) as exc:
        result.update(status='failed', reason=f'{type(exc).__name__}: {exc}')
    return result


def analyze(plan, study_root, *, protocol=None):
    validate_plan(plan)
    require(plan['schema'] == 'flowdc-study-plan-v2' and set(plan['families']) == set(PRIMARY)
            and set(plan['methods']) == {GRADIENT,*REFERENCES}, 'primary analysis requires the declared five-method/three-scenario plan')
    directory = Path(study_root).absolute()/plan['namespace']
    binding = parse((directory/'binding.json').read_bytes())
    environment = parse((directory/'environment.json').read_bytes())
    require(binding == dict(plan_sha256=digest(encode(plan)), **identities(environment)), 'analysis binding mismatch')
    require(parse((directory/'plan.json').read_bytes()) == plan, 'retained plan differs')
    authorize_plan(plan,protocol,**identities(environment))
    inventory = [first_attempt(directory,cell,binding,plan['methods'][cell['method']]) for cell in plan['cells']]
    result = primary_analysis(inventory,blocks=plan['blocks'], minimum=10 if plan['purpose']=='confirmatory' else 6)
    result.update(plan_sha256=binding['plan_sha256'], binding=binding, purpose=plan['purpose'],
                  protocol_sha256=digest(encode(protocol)) if protocol else None,
                  scientific_status='confirmation' if plan['purpose']=='confirmatory' else 'pilot/engineering; no confirmatory efficacy claim')
    for contrast in result['contrasts']:
        contrast['statistical_criteria_met'] = contrast['claim_supported']
        contrast['claim_supported'] = contrast['claim_supported'] and plan['purpose']=='confirmatory'
    return result


def render(result, output):
    """All numbers/absent intervals come directly from the retained analysis JSON."""
    output = Path(output)
    output.mkdir(parents=True,exist_ok=False)
    write_new(output/'analysis.json',result)
    with (output/'contrasts.csv').open('x',newline='') as stream:
        writer = csv.writer(stream)
        writer.writerow(['scenario','reference','status','paired_goodput_difference_Bps','ordinary_95_low','ordinary_95_high','holm_p','claim_supported'])
        for item in result['contrasts']:
            efficacy=item['efficacy']
            writer.writerow([item['scenario'],item['reference'],efficacy['status'],efficacy['mean'],
                             *(efficacy['interval'] or [None,None]),efficacy['holm_p'],item['claim_supported']])
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    figure, axes=plt.subplots(1,3,figsize=(12,3.6),sharey=True)
    for axis,scenario in zip(axes,PRIMARY,strict=True):
        for position,reference in enumerate(REFERENCES):
            item=next(x for x in result['contrasts'] if x['scenario']==scenario and x['reference']==reference)
            estimate=item['efficacy']
            if estimate['interval'] is not None:
                mean=estimate['mean']/1e6
                lo,hi=(v/1e6 for v in estimate['interval'])
                axis.errorbar(mean,position,xerr=[[max(0,mean-lo)],[max(0,hi-mean)]],fmt='o',color='black')
            else:
                axis.text(.04,position,estimate['status'].replace('_',' '),transform=axis.get_yaxis_transform(),fontsize=8)
        axis.axvline(0,color='gray',linewidth=.8)
        axis.set_title(scenario)
        axis.set_yticks(range(4),[r.replace('-v1','').replace('-v2','') for r in REFERENCES])
        axis.set_xlabel('Gradient − reference goodput (MB/s)')
    figure.suptitle('Ordinary paired 95% intervals; Holm and safeguards in analysis.json',fontsize=10)
    figure.tight_layout()
    figure.savefig(output/'primary-contrasts.pdf',metadata={'CreationDate':None,'ModDate':None})
    figure.savefig(output/'primary-contrasts.png',dpi=200)
    plt.close(figure)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan',type=Path,required=True)
    parser.add_argument('--study-root',type=Path,required=True)
    parser.add_argument('--protocol',type=Path)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    try:
        result=analyze(read_plan(args.plan),args.study_root,
                       protocol=parse(args.protocol.read_bytes()) if args.protocol else None)
        render(result,args.output)
    except (ValueError,OSError,KeyError,TypeError) as exc:
        print(f'Analysis refused: {exc}',file=sys.stderr)
        return 1
    return 0


if __name__=='__main__':
    raise SystemExit(main())
