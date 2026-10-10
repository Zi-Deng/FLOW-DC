#!/usr/bin/env python3
"""Finite secondary studies; acquisition and verification reuse the primary path."""
import argparse
import copy
import fcntl
import importlib.metadata
import json
import os
from pathlib import Path
import random
import shutil
import sys
import time
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
PRIMARY_ROWS = {'drop-recovery': 4096, 'mixed-sizes': 2048, 'sustained-overload': 4608}
SUPPORTING_ROWS = {'steady': 2048, 'sparse': 512, 'baseline-drift': 2048,
                   'oscillation': 4096, 'recovery': 2048, 'transient-overload': 2048}


def make_plan(kind, configs, *, seed, blocks=6, rows=None):
    from benchmark.core.study import method_configs
    from benchmark.core.truth import digest, encode, require
    from flowdc_methods import ABLATIONS

    require(kind in ('mechanisms', 'supporting') and type(blocks) is int and 1 <= blocks <= 60,
            'invalid supplementary scope')
    method_configs(configs)
    sizes = dict(PRIMARY_ROWS if kind == 'mechanisms' else SUPPORTING_ROWS) if rows is None else dict(rows)
    require(set(sizes) == set(PRIMARY_ROWS if kind == 'mechanisms' else SUPPORTING_ROWS)
            and all(type(n) is int and 1 <= n <= 32768 for n in sizes.values()), 'invalid workload rows')
    arms = copy.deepcopy(configs) if kind == 'supporting' else {'full': copy.deepcopy(configs['gradient-candidate-v1'])}
    if kind == 'mechanisms':
        for ablation in sorted(ABLATIONS):
            arms[ablation] = copy.deepcopy(arms['full'])
            arms[ablation]['method_options']['ablation'] = ablation
    rng, cells = random.Random(seed), []
    for block in range(blocks):
        families = sorted(sizes)
        rng.shuffle(families)
        for family in families:
            ordered = sorted(arms)
            rng.shuffle(ordered)
            fixture_seed = int(digest(encode([seed, kind, block, family]))[:16], 16)
            for arm in ordered:
                cells.append({'cell_id': f'{kind}-{family}-b{block:03d}-{arm}', 'scenario': family,
                              'block': block, 'method': arm, 'fixture_seed': fixture_seed, 'rows': sizes[family]})
    return {'schema': 'flowdc-supplementary-plan-v1', 'kind': kind, 'seed': seed, 'blocks': blocks,
            'rows': sizes, 'configs': copy.deepcopy(configs), 'arms': arms, 'cells': cells,
            'workload': 'bounded-research-v2',
            'interpretation': 'Secondary evidence; ordinary paired intervals, not confirmatory primary claims'}


def analyze(plan, directory, binding):
    from benchmark.analyze import first_attempt
    from benchmark.core.inference import paired_interval, holm
    from benchmark.core.controlled_origin import audit_events
    from benchmark.core.qualification import realized_stimulus
    from benchmark.core.truth import parse, require

    inventory = [first_attempt(directory / 'evaluation', cell, binding, plan['arms'][cell['method']])
                 for cell in plan['cells']]
    for record in inventory:
        if record['status'] != 'known_terminal':
            continue
        attempt = directory / 'evaluation' / record['cell_id'] / record['attempt_id']
        try:
            work = parse((attempt / 'origin-work.json').read_bytes())
            audits, events = [], []
            for index, snapshot in enumerate(work):
                with (attempt / f'origin-{index}/origin.jsonl').open('rb') as stream:
                    observed = [parse(raw) for raw in stream]
                audits.append(audit_events(observed, snapshot))
                events.extend(observed)
            require(audits == parse((attempt / 'origin-audit.json').read_bytes()), 'origin evidence changed')
            record['realized_stimulus'] = realized_stimulus(parse((attempt / 'scenario.json').read_bytes()), events)
            record['origin_audit_verified'] = True
        except (ValueError, OSError, KeyError, TypeError) as exc:
            record.update(status='integrity_failed', reason=f'Origin evidence: {type(exc).__name__}: {exc}')
    observed = {(r['scenario'], r['block'], r['method']): r for r in inventory}
    full = 'full' if plan['kind'] == 'mechanisms' else 'gradient-candidate-v1'
    contrasts = []
    for family in sorted(plan['rows']):
        for arm in sorted(set(plan['arms']) - {full}):
            metrics = {k: [] for k in ('goodput', 'coverage', 'p95_s')}
            for block in range(plan['blocks']):
                candidate, reference = observed[(family, block, full)], observed[(family, block, arm)]
                complete = candidate['status'] == reference['status'] == 'known_terminal'
                for key in metrics:
                    metrics[key].append(candidate[key] - reference[key] if complete
                                        and candidate.get(key) is not None and reference.get(key) is not None else None)
            contrasts.append({'scenario': family, 'reference': arm,
                              **{k: paired_interval(v, minimum=6) for k, v in metrics.items()}})
    for record, p in zip(contrasts, holm([r['goodput']['p_two_sided'] for r in contrasts]), strict=True):
        record['goodput']['secondary_family_holm_p'] = p
    return {'schema': 'flowdc-supplementary-analysis-v1', 'kind': plan['kind'], 'inventory': inventory,
            'contrasts': contrasts, 'claim_supported': False,
            'interpretation': 'Secondary paired run differences; separate Holm goodput family per study. '
            'Ordinary 95% intervals are not simultaneous; six blocks do not prove adequate precision. '
            'Missing/censored evidence prevents intervals. Sparse holds and unexercised mechanisms remain visible.'}


def run(plan, environment, decisions, output, *, wall_seconds, reserve_bytes):
    from benchmark.core.truth import digest, encode, parse, require
    from benchmark.core.study import write_new
    from benchmark.study import ROOT as acquisition_root, identities, execute_cell, require_quiescent, atomic_record
    from benchmark.analyze import first_attempt

    require(plan == make_plan(plan['kind'], plan['configs'], seed=plan['seed'], blocks=plan['blocks'], rows=plan['rows']),
            'changed supplementary plan')
    require(decisions['approved_plan_sha256'] == digest(encode(plan))
            and decisions['authority'] in ('maintainer-provisional', 'advisor')
            and all(isinstance(decisions.get(k), str) and len(decisions[k].strip()) >= 12
                    for k in ('provenance', 'replication', 'estimand')), 'explicit bound scientific decisions required')
    require(360 <= wall_seconds <= 172800 and reserve_bytes >= 50 * 1024**3, 'finite resource budget required')
    require(environment['source_files_sha256'] and all(digest((acquisition_root / name).read_bytes()) == sha
                for name, sha in environment['source_files_sha256'].items()), 'acquisition source differs')
    require(environment['python_version'] == sys.version
            and environment['python_binary_sha256'] == digest(Path(sys.executable).read_bytes())
            and environment['distributions']
            and all(importlib.metadata.version(item['name']) == item['version']
                    for item in environment['distributions']), 'acquisition runtime differs')
    output.mkdir(parents=True, exist_ok=True)
    with (output / '.lock').open('a+b') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        identity = identities(environment)
        binding = {'plan_sha256': digest(encode(plan)), **identity}
        protocol = {'plan': plan, 'binding': binding, 'decisions': decisions,
                    'orchestrator_sha256': digest(Path(__file__).read_bytes()), 'environment': environment,
                    'wall_seconds': wall_seconds, 'reserve_bytes': reserve_bytes}
        path = output / 'protocol.json'
        if path.exists():
            require(parse(path.read_bytes()) == protocol, 'frozen supplementary protocol changed')
        else:
            write_new(path, protocol)
            write_new(output / 'budget.json', {'started_at': time.time(), 'expires_at': time.time() + wall_seconds,
                                             'maximum_first_attempts': len(plan['cells'])})
        end = parse((output / 'budget.json').read_bytes())['expires_at']
        directory = output / 'evaluation'
        directory.mkdir(exist_ok=True)
        inventory = []
        for cell in plan['cells']:
            cell_dir = directory / cell['cell_id']
            attempts = sorted(cell_dir.glob('0001-*')) if cell_dir.exists() else []
            if attempts:
                require(len(attempts) == len(list(cell_dir.iterdir())) == 1, 'conflicting/repeated first attempt')
                require_quiescent(attempts[0])
                require(parse((attempts[0] / 'record.json').read_bytes())['status'] == 'recorded',
                        'uncertain previous attempt; no automatic replay')
            else:
                require(time.time() + 360 <= end and shutil.disk_usage(output).free >= reserve_bytes + 2 * 1024**3,
                        'finite supplementary time/storage exhausted')
                cell_dir.mkdir(exist_ok=False)
                attempt = cell_dir / ('0001-' + uuid4().hex)
                attempt.mkdir()
                record = {'cell': cell, 'attempt_id': attempt.name, 'status': 'started', 'native': None,
                          'source': identity, 'deliberate_rerun': False}
                write_new(attempt / 'record.json', record)
                write_new(attempt / 'host-before.json', {'load_average': os.getloadavg(),
                          'affinity': sorted(os.sched_getaffinity(0)), 'observed_at': time.time()})
                started = time.monotonic()
                try:
                    record.update(execute_cell(attempt, cell, plan['arms'][cell['method']], environment,
                                  deadline=300, cleanup=60, research_workload=plan['workload']), status='recorded',
                                  preparation_execution_wall_s=time.monotonic() - started)
                except (KeyboardInterrupt, Exception) as exc:
                    record.update(status='interrupted' if isinstance(exc, KeyboardInterrupt) else 'failed',
                                  reason=f'{type(exc).__name__}: {exc}')
                atomic_record(attempt / 'record.json', record)
            metric = first_attempt(directory, cell, binding, plan['arms'][cell['method']])
            inventory.append(metric)
            atomic_record(output / 'progress.json', {'planned': len(plan['cells']), 'finished': len(inventory),
                                                    'inventory': inventory})
            print(json.dumps({'finished': len(inventory), 'cell': cell['cell_id'], 'status': metric['status']}), flush=True)
            require(metric['status'] == 'known_terminal', 'incomplete first attempt retained; no replacement')
        result = analyze(plan, output, binding)
        atomic_record(output / 'analysis.json', {**result, 'authority': decisions['authority']})
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-root', type=Path, default=ROOT)
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--environment', type=Path, required=True)
    parser.add_argument('--decisions', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--wall-seconds', type=int, required=True)
    args = parser.parse_args()
    sys.path[:0] = [str(args.source_root), str(args.source_root / 'bin')]
    from benchmark.core.truth import parse
    environment = parse(args.environment.read_bytes())
    run(parse(args.plan.read_bytes()), environment, parse(args.decisions.read_bytes()), args.output,
        wall_seconds=args.wall_seconds, reserve_bytes=50 * 1024**3)


if __name__ == '__main__':
    main()
