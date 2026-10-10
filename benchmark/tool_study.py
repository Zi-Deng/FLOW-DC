"""Pinned img2dataset cells using the controlled study's bytes and timing boundary."""
from contextlib import ExitStack
from pathlib import Path
import subprocess
import sys

import polars as pl

HERE = Path(__file__).resolve().parents[1]
sys.path.extend([str(HERE), str(HERE / 'bin')])

from benchmark.core.controlled_origin import ControlledOrigin, audit_events, public_scenario, scenario
from benchmark.core.img2dataset_adapter import Img2DatasetAdapter, Img2DatasetConfig
from benchmark.core.lifecycle import run_verified
from benchmark.core.truth import Truth, PROVENANCE, digest, encode, parse, require
from benchmark.core.verifier import verify_native
from benchmark.study import ROOT, originals, identities, attempts_for


def execute_cell(directory, cell, threads, environment, executable, *, deadline=300, cleanup=60):
    """One fresh origin and process; metadata and original byte outputs are matched."""
    require(type(threads) is int and 1 <= threads <= 16, 'thread count exceeds common bound')
    require(environment['source_files_sha256'] and all(digest((ROOT / name).read_bytes()) == sha
            for name, sha in environment['source_files_sha256'].items()), 'tool acquisition source differs')
    require(Path(executable).is_file(), 'explicit pinned img2dataset CLI required')
    version = subprocess.check_output([str(Path(executable).absolute().parent / 'python'), '-I', '-c',
                                      'import importlib.metadata; print(importlib.metadata.version("img2dataset"))'],
                                      timeout=10, text=True).strip()
    require(version == '1.47.0', 'img2dataset1.47.0 required; no executable fallback')
    require(cell['scenario'] in ('drop-recovery', 'mixed-sizes', 'sustained-overload'), 'unsupported tool condition')
    raw_payloads = originals(cell['fixture_seed'])
    plan = scenario(cell['scenario'], raw_payloads, rows=cell['rows'],
                    research_workload='bounded-research-v2')
    directory.mkdir(parents=True, exist_ok=True)
    (directory / 'scenario.json').write_bytes(encode(public_scenario(plan)))
    payloads = directory / 'originals'
    payloads.mkdir()
    for name, raw in raw_payloads.items():
        (payloads / (name + '.bin')).write_bytes(raw)
    with ExitStack() as resources:
        origin_dir = directory / 'origin-0'
        origin_dir.mkdir()
        origin = resources.enter_context(ControlledOrigin(origin_dir, plan))
        urls = [origin.base_url + item['path'] for item in plan['assignments']]
        frame = pl.DataFrame({'url': urls, 'label': [f'row-{i}' for i in range(len(urls))],
                              'origin_index': [0] * len(urls)})
        frame.write_parquet(directory / 'input.parquet')
        catalog = {origin.base_url + path: {'bytes': len(spec['payload']), 'sha256': digest(spec['payload'])}
                   for path, spec in plan['objects'].items()}
        truth = Truth.load(directory / 'input.parquet', catalog, research_workload='bounded-research-v2')
        eligible = truth.write(directory / 'fixture')
        attempts = 2 if cell['scenario'] == 'sustained-overload' else 1
        config = Img2DatasetConfig(str(eligible), str(directory / 'run/native'), 'url', threads,
                                  timeout_sec=30, retries=attempts - 1, max_shard_retry=0,
                                  output_format='webdataset', executable=str(executable),
                                  additional_columns=[c for c in truth.frame.columns if c != 'url'] + list(PROVENANCE))
        command = Img2DatasetAdapter().build_command(config)
        command.extend(['--number_sample_per_shard', '256', '--encode_format', 'jpg',
                        '--compute_hash', 'sha256', '--distributor', 'multiprocessing'])
        result = run_verified(command, directory / 'run', truth.record,
                              lambda: verify_native('img2dataset', directory / 'run/native', truth.record),
                              cwd=ROOT, deadline=deadline, cleanup=cleanup,
                              provenance={**identities(environment), 'cell': cell,
                              'config_sha256': digest(encode(command)), 'truth_sha256': digest(encode(truth.record)),
                              'scenario_sha256': digest(encode(public_scenario(plan))), 'threads': threads,
                              'processes': 1, 'attempt_budget_per_row': attempts,
                              'img2dataset_cli_sha256': digest(Path(executable).read_bytes()),
                              'timeout_semantics': 'Native urllib timeout; FLOW-DC uses native aiohttp. '
                              'Same30second numeric setting is not semantic equivalence.',
                              'attempt_attribution': 'Observed aggregate origin requests; native per-row attempts unavailable.'})
    work = origin.snapshot()
    audit = audit_events(origin.events, work)
    (directory / 'origin-work.json').write_bytes(encode([work]))
    (directory / 'origin-audit.json').write_bytes(encode([audit]))
    return {'native': result, 'origin_work': [work], 'origin_audit': [audit], 'instrumentation': True}


def first_attempt(directory, cell, binding, threads):
    """Independent output and origin re-verification; first attempts cannot be replaced."""
    result = {'scenario': cell['scenario'], 'block': cell['block'], 'method': cell['method'],
              'cell_id': cell['cell_id'], 'attempt': 1, 'status': 'missing', 'later_attempts': []}
    attempts = attempts_for(directory / cell['cell_id'], cell)
    if not attempts:
        return result
    first = attempts[0]
    result.update(attempt_id=first.name, later_attempts=[p.name for p in attempts[1:]])
    if not (first / 'record.json').exists():
        return {**result, 'status': 'censored', 'reason': 'First attempt has no closed record'}
    record = parse((first / 'record.json').read_bytes())
    require(record['cell'] == cell and record['attempt_id'] == first.name
            and record['source'] == {k: binding[k] for k in ('source_sha256', 'environment_sha256')},
            'tool first-attempt identity/source mismatch')
    if record['status'] != 'recorded' or record.get('native') is None:
        return {**result, 'status': 'failed' if record['status'] == 'failed' else 'censored',
                'reason': record.get('reason', record['status'])}
    try:
        native = parse((first / 'run/result.json').read_bytes())
        require(native == record['native'], 'closed native record changed')
        truth = parse((first / 'fixture/truth.json').read_bytes())
        provenance = native['provenance']
        require(provenance['truth_sha256'] == digest(encode(truth)) and provenance['cell'] == cell
                and provenance['threads'] == threads and provenance['processes'] == 1
                and all(provenance[k] == binding[k] for k in ('source_sha256', 'environment_sha256')),
                'tool provenance differs')
        raw = (first / 'run/outcomes.json').read_bytes()
        require(digest(raw) == native['outcome_index_sha256'], 'tool common index changed')
        independent = verify_native('img2dataset', first / 'run/native', truth)
        common = parse(raw)
        require(independent['artifacts_valid'] and all(independent[k] == common[k] for k in independent),
                'tool output differs on re-verification')
        rows = independent['rows']
        useful = sum(r['useful_bytes'] for r in rows)
        verified = sum(r['disposition'] == 'verified' for r in rows)
        require(len(rows) == truth['original_rows'] == native['original_rows']
                and useful == native['useful_payload_bytes'] and verified == native['verified_rows']
                and verified / len(rows) == native['verified_coverage'], 'credited tool metrics differ')
        work = parse((first / 'origin-work.json').read_bytes())
        require(len(work) == 1, 'tool origin inventory changed')
        with (first / 'origin-0/origin.jsonl').open('rb') as stream:
            events = [parse(raw) for raw in stream]
        audit = audit_events(events, work[0])
        require([audit] == parse((first / 'origin-audit.json').read_bytes())
                and work[0]['requests'] <= len(rows) * provenance['attempt_budget_per_row'],
                'tool origin/attempt budget differs')
        terminal = native['process_exit_code'] == 0 and native['status'] in ('complete', 'incomplete') \
            and not native['cleanup_errors'] and not native['interruption_requested'] \
            and all(r['disposition'] in ('verified', 'failed', 'skipped') for r in rows)
        result.update(status='known_terminal' if terminal else 'censored', original_rows=len(rows),
                      useful_bytes=useful, coverage=verified / len(rows), elapsed_s=native['elapsed_ns'] / 1e9,
                      goodput=useful / (native['elapsed_ns'] / 1e9), verification_s=native['verification_ns'] / 1e9,
                      process_s=native['process_ns'] / 1e9, resources=native['resources'],
                      observed_origin_attempts=work[0]['requests'], per_row_attempts='Unavailable in native img2dataset',
                      p95_s=None, latency_limitation='Comparable native request-latency journal is unavailable; '
                      'origin service timing is not substituted for client latency.')
    except (ValueError, OSError, KeyError, TypeError, ZeroDivisionError) as exc:
        result.update(status='integrity_failed', reason=f'{type(exc).__name__}: {exc}')
    return result
