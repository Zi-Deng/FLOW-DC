"""Offline continuation checks; no public scientific requests."""
import io
import json
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import polars as pl
from PIL import Image

from benchmark.real_server_study import minimum_queue as queue, minimum_study as design
from benchmark.real_server_study import minimum_recovery as recovery
from benchmark.real_server_study.common import file_hash, read_json, write_new
from benchmark.real_server_study.public_guard import ToolGuard, FlowGuard, SharedBodyBudget

SOURCE = design.SOURCES[0]
URL = 'https://open-images-dataset.s3.amazonaws.com/train/a.jpg'


def stopped_shard(args):
    path, trigger = args
    guard = ToolGuard(SOURCE, [URL], io.StringIO(), 1, shared=SharedBodyBudget(path, 100))
    if trigger:
        guard.policy.header(time.monotonic(), 503)
    try:
        guard.begin(URL)
    except RuntimeError:
        return guard.policy.reason
    return 'unexpected dispatch'


class ContinuationTests(unittest.TestCase):
    def prepare(self, path):
        for name in ('gates', 'runs', 'pools'):
            (path / name).mkdir()
        write_new(path / 'protocol.json', dict(stop_policy='stop-run-continue-campaign',
            initial_rows={s:1000 for s in design.SOURCES},
            partitions={s:dict(capacity=100000) for s in design.SOURCES},
            provenance={s:dict(media_rights_qualified=True, access_qualified=True) for s in design.SOURCES}))

    def save(self, study, cell, status='complete', scope=None):
        p = study / 'runs' / cell['run_id']; p.mkdir()
        record = dict(cell, status=status, rows=cell['rows'], original_rows=cell['rows'], http_span_s=900,
            elapsed_s=1000, native_seconds=950, body_bytes=1000, coverage=1, goodput_Bps=100,
            mean_http_concurrency=100, started_utc='2020-01-01T00:00:00Z', closed_utc='2020-01-01T00:01:00Z')
        if status != 'complete':
            record.update(stop_scope=scope or 'run', goodput_Bps=None,
                          stop_evidence=[dict(status_counts={'503':1}, server_retry_after_until_utc=0)])
        write_new(p/'record.json', record)
        return record

    def finish_recovery(self, study, cell, passed=True):
        write_new(study/'gates'/(cell['run_id']+'.json'), dict(source=cell['source'], after_run=cell['after_run'],
                  status='passed' if passed else 'failed', requests=4, probe_body_bytes=100))

    def test_run_stop_recovery_and_sizing_fallback_no_replay(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d);self.prepare(p)
            first=queue.next_cell(p, 259200)
            self.assertEqual((first['source'], first['candidate']), (SOURCE,2))
            self.save(p,first,'stopped')
            probe=queue.next_cell(p,259200)
            self.assertEqual(probe['phase'],'recovery_probe')
            self.finish_recovery(p,probe)
            next_run=queue.next_cell(p,259200)
            self.assertEqual(next_run['candidate'],1)
            self.assertNotEqual(next_run['run_id'],first['run_id'])
            self.assertEqual(len(queue.records(p)),1)

    def test_failed_probe_only_suspends_affected_source(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d);self.prepare(p)
            self.save(p,queue.next_cell(p,259200),'stopped')
            self.finish_recovery(p,queue.next_cell(p,259200),False)
            cell=queue.next_cell(p,259200)
            self.assertEqual(cell['source'], design.TARGETS[0])
            self.assertTrue((p/'gates/development-selection.json').exists())
            self.assertEqual(read_json(p/'gates'/f'{SOURCE}-disposition.json')['status'],'suspended')

    def test_all_sizing_stops_are_bounded_then_other_sources_continue(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d);self.prepare(p)
            attempts=[]
            while True:
                cell=queue.next_cell(p,259200)
                if cell['source']!=SOURCE:break
                if cell['phase']=='recovery_probe':self.finish_recovery(p,cell)
                else:
                    attempts.append(cell);self.save(p,cell,'stopped')
                self.assertLess(len(attempts),4)
            self.assertEqual([r['candidate'] for r in attempts],[2,1,0])
            self.assertEqual(read_json(p/'gates'/f'{SOURCE}-size.json')['status'],'dependency_blocked')

    def test_calibration_failure_does_not_discard_other_candidates_or_confirmation(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d);self.prepare(p); seen=set(); failures=0
            for _ in range(60):
                cell=queue.next_cell(p,259200)
                if cell['phase']=='evaluation':break
                if cell['phase']=='recovery_probe':
                    self.finish_recovery(p,cell);continue
                self.assertNotIn(cell['run_id'],seen);seen.add(cell['run_id'])
                fail=cell['phase']=='calibration' and cell['source']==SOURCE and cell['method']==design.METHODS[0]
                failures+=fail;self.save(p,cell,'stopped' if fail else 'complete')
            else:self.fail('no confirmation after closed development')
            frozen=read_json(p/'freeze.json')
            self.assertEqual(failures,4)
            self.assertTrue(all(c['arm']!='ratio' for c in frozen['cells']))
            self.assertTrue(any(c['arm']=='gradient' for c in frozen['cells']))
            self.assertEqual(sum(r['arm']=='ratio' for r in frozen['unavailable_arms']),2)
            self.save(p,cell,'stopped')
            self.finish_recovery(p,queue.next_cell(p,259200))
            after=queue.next_cell(p,259200)
            self.assertEqual(after['phase'],'evaluation')
            self.assertNotEqual(after['run_id'],cell['run_id'])

    def test_retry_after_defers_source_but_other_sizing_can_run(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d);self.prepare(p)
            first=queue.next_cell(p,259200);row=self.save(p,first,'stopped')
            row['stop_evidence'][0]['server_retry_after_until_utc']=time.time()+3600
            (p/'runs'/first['run_id']/'record.json').write_text(json.dumps(row))
            following=queue.next_cell(p,259200)
            self.assertEqual(following['source'],design.TARGETS[0])
            self.assertFalse((p/'gates/development-selection.json').exists())

    def test_no_available_adaptive_arm_closes_with_explicit_dependency_blocks(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d);self.prepare(p)
            for _ in range(60):
                cell=queue.next_cell(p,259200)
                if cell is None:break
                self.assertNotEqual(cell['phase'],'evaluation')
                if cell['phase']=='recovery_probe':
                    self.finish_recovery(p,cell);continue
                fail=cell['phase']=='calibration' and cell['source']==SOURCE
                self.save(p,cell,'stopped' if fail else 'complete')
            else:self.fail('unavailable development never closed')
            frozen=read_json(p/'freeze.json')
            self.assertEqual(frozen['cells'],[])
            self.assertEqual(len(frozen['unavailable_arms']),8)
            targets=[r for r in queue.records(p).values() if r['source'] in design.TARGETS and r['phase']=='calibration']
            self.assertEqual(len(targets),8)

    def test_temporal_wait_does_not_block_other_target(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d);self.prepare(p)
            selected=dict(development={m:dict(candidate=0) for m in design.METHODS},
                          **{s:dict(candidate=1) for s in design.TARGETS})
            cells=design.schedule(selected,{s:1000 for s in design.TARGETS},design.TARGETS)
            for c in cells:
                if c['source']==design.TARGETS[0] and c['block']<3:
                    r=self.save(p,c);r['started_utc']=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())
                    (p/'runs'/c['run_id']/'record.json').write_text(json.dumps(r))
            write_new(p/'freeze.json',dict(cells=cells,protocol_sha256=file_hash(p/'protocol.json')))
            self.assertEqual(queue.next_cell(p,259200)['source'],design.TARGETS[1])

    def test_campaign_failure_and_unfinished_attempt_refuse_continuation(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d);self.prepare(p);cell=queue.next_cell(p,259200)
            self.save(p,cell,'stopped','campaign')
            with self.assertRaisesRegex(ValueError,'campaign-scoped'):queue.next_cell(p,259200)
        self.assertEqual(recovery.stop_scope(dict(status='stopped', error='verification mismatch')), 'campaign')
        self.assertEqual(recovery.stop_scope(dict(status='stopped',worker_closed=True,transport={})), 'run')
        for status in ('401','403','429','302'):
            self.assertEqual(recovery.stop_scope(dict(status='stopped',transport=dict(status_counts={status:1}))), 'source')

    def test_spawned_guard_stop_reaches_other_shards_and_flow_guard(self):
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)/'shared';budget=SharedBodyBudget.create(path,100)
            # Separate interpreters exercise shared stop propagation without
            # leaving multiprocessing's persistent resource tracker in the
            # parent test process (which also runs ownership-cleanup tests).
            code = ('import sys;sys.path.insert(0,sys.argv[1]);'
                    'from test_minimum_continuation import stopped_shard;'
                    'print(stopped_shard((sys.argv[2],sys.argv[3]=="True")))')
            for trigger in (True, False):
                child = subprocess.run([sys.executable, '-c', code, str(Path(__file__).parent),
                    str(path), str(trigger)], capture_output=True, text=True, timeout=30, check=True)
                self.assertEqual(child.stdout.strip(), 'public_access_stop')
            budget.close()
        guard=FlowGuard(SOURCE,[URL],io.StringIO(),1,latency_stop=False)
        guard.policy.header(0,503)
        with self.assertRaisesRegex(RuntimeError,'public_access_stop'):guard.check()

    def test_recovery_exactly_four_rows_once_and_failures_retained(self):
        buf=io.BytesIO();Image.new('RGB',(2,2),'red').save(buf,format='JPEG');raw=buf.getvalue()
        class Response(io.BytesIO):
            code=200;headers={}
        for succeeds in (True,False):
            with tempfile.TemporaryDirectory() as d:
                p=Path(d);self.prepare(p)
                pl.DataFrame({'url':[URL.replace('a.jpg',f'{i}.jpg') for i in range(6)]}).write_parquet(p/'pools'/f'{SOURCE}.parquet')
                write_new(p/'gates'/f'{SOURCE}.json',dict(ignore_time=False,robots_txt=''))
                with patch.object(recovery,'guest_check'), patch('urllib.request.OpenerDirector.open',
                    side_effect=(lambda *a,**kw:Response(raw)) if succeeds else OSError('offline')) as opened:
                    result=recovery.recovery_probe(p,SOURCE,'failed-run')
                    self.assertEqual(result['status'],'passed' if succeeds else 'failed')
                    self.assertEqual(opened.call_count,4 if succeeds else 1)
                    self.assertEqual(recovery.recovery_probe(p,SOURCE,'failed-run'),result)
                    self.assertEqual(opened.call_count,4 if succeeds else 1)
                if succeeds:self.assertEqual(result['probe_body_bytes'],4*len(raw))

if __name__=='__main__':unittest.main()
