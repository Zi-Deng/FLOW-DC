#!/usr/bin/env python3
"""Finite fixed-concurrency workload calibration, before adaptive outcomes."""
import argparse
import time
import sys
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

from benchmark.core.controlled_origin import RESEARCH_SCENARIOS, scenario
from benchmark.core.provenance import environment_record
from benchmark.core.qualification import observations, realized_stimulus
from benchmark.core.study import method_configs, write_new
from benchmark.core.truth import parse, require, workload
from benchmark.study import execute_cell, originals

CANDIDATES=(512,1024,2048,4096,8192,16384,32768)


def calibrate(output, *, seed, scenarios, wall_seconds):
    require(type(seed) is int and 0 <= seed < 2**32,'invalid seed')
    require(600 <= wall_seconds <= 14400,'calibration requires a finite 600..14400 second budget')
    require(scenarios and len(set(scenarios))==len(scenarios) and set(scenarios)<=set(RESEARCH_SCENARIOS),'invalid scenarios')
    output=Path(output)
    output.mkdir(parents=True,exist_ok=False)
    limits=workload('bounded-research-v2')
    config=method_configs()['fixed-v1']
    protocol=dict(schema='flowdc-fixed-calibration-v1',seed=seed,candidates=list(CANDIDATES),scenarios=scenarios,
        wall_seconds=wall_seconds,workload=limits.record(),config=config,
        rule='First ascending feasible workload with complete fixed acquisition and qualified realized stimulus; retain every candidate. No adaptive outcomes inspected.',
        limitation='Fixed-client origin/observation qualification is necessary; controller mechanisms and cloud resources require separate checks.')
    write_new(output/'protocol.json',protocol)
    environment=environment_record(ROOT)
    write_new(output/'environment.json',environment)
    started=time.monotonic()
    records,selected=[],{}
    payloads=originals(seed)
    stop=False
    for family in scenarios:
        for rows in CANDIDATES:
            entry=dict(scenario=family,rows=rows,status='not_started')
            try:
                scenario(family,payloads,rows=rows,research_workload=limits.name)
            except ValueError as exc:
                entry.update(status='infeasible',reason=str(exc)); records.append(entry)
                continue
            if time.monotonic()-started + limits.acquisition_seconds+60 > wall_seconds:
                entry.update(status='budget_exhausted');records.append(entry);stop=True;break
            directory=output/f'{family}-{rows:05d}'
            directory.mkdir()
            cell=dict(cell_id=directory.name,scenario=family,rows=rows,fixture_seed=seed,method='fixed-v1')
            try:
                evidence=execute_cell(directory,cell,config,environment,deadline=limits.acquisition_seconds,
                                      cleanup=60,research_workload=limits.name)
                write_new(directory/'record.json',evidence)
                native=evidence['native']
                truth=parse((directory/'fixture/truth.json').read_bytes())
                _,timing=observations(directory/'run/native',truth)
                events=[parse(line) for line in (directory/'origin-0/origin.jsonl').read_bytes().splitlines()]
                stimulus=realized_stimulus(parse((directory/'scenario.json').read_bytes()),events)
                files=[p for p in directory.rglob('*') if p.is_file()]
                entry.update(status=native['status'],elapsed_s=native['elapsed_ns']/1e9,resources=native['resources'],
                    verification_s=native['verification_ns']/1e9,artifacts_bytes=sum(p.stat().st_size for p in files),
                    artifacts_allocated_bytes=sum(p.stat().st_blocks*512 for p in files),files=len(files),
                    measurements=timing,stimulus=stimulus,
                    qualified=native['process_exit_code']==0 and timing['complete'] and stimulus['qualified']
                        and native['status'] in ('complete','incomplete') and all(a['status']=='verified' for a in evidence['origin_audit']))
                # Overload failures may be fully accounted; sparse holds do not exercise adaptation.
                if family=='sparse':
                    entry['qualification_interpretation']='Sparse traffic/hold boundary; no exercised adaptation claim.'
            except (ValueError,OSError,KeyError,TypeError) as exc:
                entry.update(status='failed',qualified=False,reason=f'{type(exc).__name__}: {exc}')
            records.append(entry)
            write_new(directory/'qualification.json',entry)
            if entry.get('qualified'):
                selected[family]=rows;break
        if stop:break
    result=dict(schema='flowdc-fixed-calibration-result-v1',selected_rows=selected,records=records,
                elapsed_s=time.monotonic()-started,complete=set(selected)==set(scenarios),
                missing_scenarios=sorted(set(scenarios)-set(selected)))
    write_new(output/'calibration.json',result)
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--seed',type=int,required=True)
    parser.add_argument('--scenarios',nargs='+',choices=RESEARCH_SCENARIOS,default=['drop-recovery','mixed-sizes','sustained-overload'])
    parser.add_argument('--wall-seconds',type=int,default=4500)
    args=parser.parse_args()
    result=calibrate(args.output,seed=args.seed,scenarios=args.scenarios,wall_seconds=args.wall_seconds)
    print('Qualified workloads:',result['selected_rows'],'; complete:',result['complete'])
    return 0 if result['complete'] else 1


if __name__=='__main__':
    raise SystemExit(main())
