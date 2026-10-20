"""Bounded onset qualification: deterministic design and loopback HTTP only."""
import asyncio
import io
import json
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import Mock, patch

import aiohttp
import polars as pl
from PIL import Image

from benchmark.real_server_study import common, onset, onset_run, telemetry
from benchmark.real_server_study import onset_analysis, onset_cloud

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'bin'))


class DesignTests(unittest.TestCase):
    def test_finite_schedule_and_position_balance(self):
        runs = onset.schedule()
        self.assertEqual(runs, onset.schedule())
        self.assertEqual([r['cap'] for r in runs[:7]], list(onset.CAPS))
        self.assertEqual(len({r['run_id'] for r in runs}), 16)
        self.assertLessEqual(16*onset.ROWS+428, onset.LIMITS['max_requests'])
        for arm in ('ratio-v1', 'gradient-candidate-v1', 'fixed-v1'):
            self.assertEqual(sorted(r['position'] for r in runs[7:] if r['arm'] == arm), [1, 2, 3])

    def test_prepare_excludes_dispatches_not_only_successes(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); old = root/'old'; new = root/'new'; old.mkdir(); new.mkdir()
            urls = [common.PREFIX+f'twmoth{i//20}/image{i}.jpg' for i in range(600)]
            pl.DataFrame({'url':urls}).write_parquet(old/'population.parquet')
            common.write_new(old/'population-record.json', {'population_sha256':common.file_hash(old/'population.parquet')})
            excluded = {urls[0], urls[1], urls[2], urls[3]}
            for index, directory in enumerate((old, new)):
                (directory/'probe-results.jsonl').write_text(json.dumps({'url':urls[2*index]})+'\n')
                run = directory/'runs'/'run'; run.mkdir(parents=True)
                path = run/'request-audit.jsonl'
                path.write_text(json.dumps({'event':'dispatch', 'url':urls[2*index+1], 'status':404})+'\n')
                common.write_new(run/'record.json', {'evidence':{'request-audit.jsonl':common.file_hash(path)}})
            study = root/'study'
            with patch.object(onset, 'ROWS', 2), patch.object(onset, 'source_identity', return_value={'head':'fixture'}):
                protocol = onset.prepare(study, root, old, new)
                onset.check(study, root)
                all_urls = []
                for p in [study/'probe.parquet', study/'recovery.parquet', *sorted((study/'slices').glob('*.parquet'))]:
                    all_urls.extend(pl.read_parquet(p)['url'].to_list())
                self.assertFalse(excluded.intersection(all_urls))
                self.assertEqual(len(all_urls), len(set(all_urls)))
                self.assertEqual(protocol['excluded_dispatch_urls'], 4)
                (study/'schedule.json').write_text('{}')
                with self.assertRaisesRegex(ValueError, 'input changed'): onset.check(study, root)

    def test_fixed_selection_and_native_configuration(self):
        rows = [dict(phase='search', complete=True, cap=cap, record_sha256=str(cap), run_id=str(cap),
                     exposure=dict(qualified=True, steady_goodput_Bps=100 if cap >= 128 else 90)) for cap in onset.CAPS]
        self.assertEqual(onset.select_fixed(rows)['cap'], 128)
        rows[0]['exposure']['qualified'] = False
        self.assertEqual(onset.select_fixed(rows)['cap'], 128)
        rows[0]['complete'] = False
        with self.assertRaises(ValueError): onset.select_fixed(rows)
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); common.write_new(root/'fixed-selection.json', {'cap':128})
            for run in onset.schedule():
                cfg = onset.config(root, run)
                if run['method'] == 'fixed-v1':
                    self.assertEqual(cfg['C_min'], cfg['C_max'])
                else:
                    self.assertEqual((cfg['C_min'], cfg['C_init'], cfg['C_max']), (2, 4, 256))
                self.assertEqual(cfg['max_retry_attempts'], 1)
                self.assertFalse(cfg['force_overwrite'])

    def test_unclosed_attempt_consumes_budget_and_cannot_replay(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); run = onset.schedule()[0]
            common.write_new(root/'schedule.json', {'runs':onset.schedule()})
            directory = root/'runs'/run['run_id']; directory.mkdir(parents=True)
            common.write_new(directory/'attempt.json', {'body_reservation':123, 'artifact_reservation':456})
            self.assertEqual(onset_run.used_budget(root), (123, 456))
            with self.assertRaisesRegex(ValueError, 'no acquisition replay'): onset_run.rows(root)

    def test_truncated_audit_remains_invalid_and_first_signal_is_not_overwritten(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)/'audit.jsonl'
            p.write_text('{"event":"start"}\n{"event":')
            records, invalid = onset_run.audit_events(p)
            self.assertTrue(invalid); self.assertEqual(records, [{'event':'start'}])
        policy = onset.Policy(.1, 100)
        policy.header(0, 429); policy.header(1, 403); policy.header(2, 503)
        self.assertEqual(policy.reason, 'explicit_429')
        self.assertTrue(policy.fatal)
        decision = dict(reason=policy.reason, fatal=policy.fatal)
        self.assertFalse(onset_run.recovery_allowed(decision))
        self.assertEqual(onset_run.fatal_signal({'event':'headers','status':403}), 'access_refusal')

    def test_interrupted_attempt_closes_without_replaying_health_or_acquisition(self):
        with tempfile.TemporaryDirectory() as d:
            study = Path(d); run = onset.schedule()[0]
            common.write_new(study/'schedule.json', {'runs':onset.schedule()})
            (study/'runs'/run['run_id']).mkdir(parents=True)
            with patch.object(onset, 'check'), patch.object(onset_run, 'guest_check'), \
                 patch.object(onset_run, 'health') as health, patch.object(onset_run, 'host_gate') as gate:
                outcome = onset_run.window(study, study)
            self.assertEqual(outcome['status'], 'stopped')
            self.assertTrue(common.read_json(study/'STOP.json')['fatal'])
            health.assert_not_called(); gate.assert_not_called()

    def test_aggregate_audit_budget_charges_uncertain_attempts_and_refunds_known_unused_time(self):
        with tempfile.TemporaryDirectory() as d:
            packet = Path(d)
            common.write_new(packet/'window-00-audit-intent.json', {'seconds':900})
            common.write_new(packet/'window-00-audit.json', {'audit_elapsed_s':400})
            common.write_new(packet/'window-01-audit-intent.json', {'seconds':800})
            self.assertEqual(onset_cloud.audit_spent(packet), 1200)
            common.write_new(packet/'window-01-audit.json', {'audit_elapsed_s':50})
            self.assertEqual(onset_cloud.audit_spent(packet), 450)

    def test_qualification_requires_review_receipt_and_fresh_github_head(self):
        for mode in ('pass','stale','failed','material','tampered'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as d:
                root = Path(d); review = root/'review'; review.mkdir()
                common.write_new(review/'report.json', {'findings':[{'severity':'P2'}] if mode == 'material' else []})
                common.write_new(review/'review.json', {'status':'completed','head':'head','repository':'Zi-Deng/FLOW-DC',
                                 'pr':49,'report_sha256':common.file_hash(review/'report.json')})
                common.write_new(root/'qualification.json', {'head':'head','review_head':'head','ci_passed':True,
                                 'review_accepted':True,'review_directory':str(review),'pr':49,
                                 'review_metadata_sha256':common.file_hash(review/'review.json')})
                if mode == 'tampered': (review/'review.json').write_text('{}')
                response = {'headRefOid':'other' if mode == 'stale' else 'head',
                            'statusCheckRollup':[{'name':name,'conclusion':'FAILURE' if mode == 'failed' else 'SUCCESS'}
                                                 for name in ('flowdc-tests','agentic-quality')]}
                with patch.object(subprocess := onset_cloud.subprocess, 'check_output', return_value=json.dumps(response)):
                    if mode == 'pass': onset_cloud.qualification_gate(root, {'source':{'head':'head'}})
                    else:
                        with self.assertRaises(ValueError): onset_cloud.qualification_gate(root, {'source':{'head':'head'}})

    def test_unreviewed_head_and_exhausted_cloud_windows_cannot_activate(self):
        for mode in ('unreviewed', 'two_windows', 'su_exhausted'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as d:
                packet = Path(d)
                common.write_new(packet/'deployment.json', {'source':{'head':'head'}})
                common.write_new(packet/'operator.json', {'source':'source', 'state':'state'})
                common.write_new(packet/'qualification.json', {'head':'other', 'ci_passed':True,
                                 'review_accepted':True, 'review_head':'head'})
                journal = Mock(); journal.read.return_value = {}
                with patch.object(onset_cloud.cloud, 'imports', return_value=(lambda _:journal, None, None, None)), \
                     patch.object(onset_cloud.cloud, 'controller_for') as controller:
                    if mode == 'unreviewed':
                        with self.assertRaisesRegex(ValueError, 'qualified current-head'):
                            onset_cloud.authorize(packet)
                    else:
                        if mode == 'two_windows':
                            for number in range(2): common.write_new(packet/f'window-{number:02}.json', {'cleaned':True})
                        with patch.object(onset_cloud, 'authorize', return_value=(journal, {})), \
                             patch.object(onset_cloud, 'wall_left', return_value=14400), \
                             patch.object(onset_cloud, 'exposure', return_value=49):
                            with self.assertRaisesRegex(ValueError, 'two-window' if mode == 'two_windows' else 'SU admission'):
                                onset_cloud.run(packet)
                    controller.assert_not_called()

    def test_offline_audit_refuses_changed_partial_evidence(self):
        with tempfile.TemporaryDirectory() as d:
            study = Path(d); run = onset.schedule()[0]; directory = study/'runs'/run['run_id']
            directory.mkdir(parents=True); (study/'slices').mkdir()
            common.write_new(study/'protocol.json', {'contract':'fixture'})
            common.write_new(study/'schedule.json', {'runs':[run]})
            slice_path = study/'slices'/f"{run['slice']}.parquet"; slice_path.write_bytes(b'fixture')
            common.write_new(directory/'attempt.json', {'protocol_sha256':common.file_hash(study/'protocol.json'),
                             'slice_sha256':common.file_hash(slice_path)})
            path = directory/'request-audit.jsonl'; path.write_text('{"event":"stop","reason":"explicit_429"}\n')
            common.write_new(directory/'record.json', {**run, 'configured_cap':66, 'complete':False,
                             'evidence':{'request-audit.jsonl':common.file_hash(path)}})
            with patch.object(onset, 'check'):
                result = onset_analysis.audit(study, study)
                self.assertEqual(result['runs'][0]['status'], 'stopped')
                path.write_text('{"event":"closed"}\n')
                with self.assertRaisesRegex(ValueError, 'evidence changed'): onset_analysis.audit(study, study)

    def test_acquisition_failure_collects_offline_without_replay_even_after_uncertain_stop(self):
        for stopped in (True, False):
            with self.subTest(quiescent=stopped), tempfile.TemporaryDirectory() as d:
                packet = Path(d); report = packet/'final-report'; report.mkdir()
                common.write_new(report/'report-manifest.json', {'audit_pending':0})
                common.write_new(packet/'deployment.json', {'campaign_id':'campaign'})
                common.write_new(packet/'operator.json', {'source':'source','state':'state','guest_root':'root','guest_python':'python'})
                common.write_new(packet/'deployment-receipt.json', {'python':'python'})
                common.write_new(packet/'retained-volume.json', {'volume':{'volume_id':'volume'},'root':'root','mount':{'filesystem_uuid':'fs'}})
                current = {'profile_path':'profile','vms':{'vm':{'account':{'campaign_id':'campaign'}}}}
                journal = Mock(); journal.read.return_value = current
                controller = Mock()
                actions = []
                def remote(packet, number, action, seconds, final=False):
                    actions.append(action)
                    if action == 'acquire': raise ValueError('fixture interrupted transport')
                    return {'audit_elapsed_s':.1,'audit_pending':0}
                def collected(*args, **kwargs):
                    (packet/'collected').mkdir(exist_ok=True)
                with patch.object(onset_cloud.cloud, 'imports', return_value=(lambda _:journal,None,None,None)), \
                     patch.object(onset_cloud, 'authorize', return_value=(journal, {'campaign_id':'campaign','baseline':{'vm':0}})), \
                     patch.object(onset_cloud, 'wall_left', return_value=14400), patch.object(onset_cloud,'exposure',return_value=0), \
                     patch.object(onset_cloud,'local_status',return_value={}), patch.object(onset_cloud,'ready'), \
                     patch.object(onset_cloud,'remote',side_effect=remote), \
                     patch.object(onset_cloud,'quiesce',side_effect=(lambda *_:{'after':'inactive'}) if stopped else ValueError('uncertain')), \
                     patch.object(onset_cloud.cloud,'controller_for',return_value=controller), \
                     patch.object(onset_cloud.cloud,'refresh_ssh'), patch.object(onset_cloud.cloud,'wait_ssh'), \
                     patch.object(onset_cloud.cloud,'ssh',return_value=b'{}'), patch.object(onset_cloud.cloud,'collect',side_effect=collected), \
                     patch('flowdc_pilot_provider.Provider'), patch('flowdc_ops.load_profile',return_value={}), \
                     patch('flowdc_experiment_transport.remaining',return_value=2600), patch('flowdc_experiment_transport.idle',return_value=True), \
                     patch.object(onset_analysis,'report',return_value=report/'report.pdf'):
                    result = onset_cloud.run(packet)
                self.assertEqual(actions, ['acquire','audit'])
                self.assertTrue(result['cleanup_complete'])
                self.assertEqual(result['windows_used'], 1 if stopped else 2)
                self.assertTrue((packet/'acquisition-failed.json').exists())
                self.assertEqual(sum(c.args[0]=='start' for c in controller.call.call_args_list), 1 if stopped else 2)


class StopTests(unittest.TestCase):
    def test_first_warning_and_retry_after(self):
        for status, reason in ((429, 'explicit_429'), (503, 'service_unavailable'), (403, 'access_refusal')):
            p = onset.Policy(.1, 100); p.header(0, status, '1800')
            self.assertEqual(p.reason, reason)
            self.assertGreater(p.cooldown_until, time.time()+1790)
        self.assertIsNone(onset.Policy(.1, 100).reason)

    def test_sparse_body_collapse_and_no_completion_stall(self):
        for completions in (0, 3):
            p = onset.Policy(.1, 100); p.tick(0)
            for second in range(1, 31):
                p.sample(second, 32, True, 12, 0, 0)
                for i in range(completions):
                    p.network_end(second-.1-i*.01, 200, {'body_completed_at':second})
                    p.complete(second-.1-i*.01, .1)
                p.tick(second)
            self.assertEqual(p.reason, 'body_completion_collapse')

    def test_low_client_cap_and_no_backlog_are_not_server_collapse(self):
        for active, backlog in ((4, True), (32, False)):
            p = onset.Policy(.1, 100); p.tick(0)
            for second in range(1, 35):
                p.sample(second, active, backlog, 12, 0, 0); p.tick(second)
            self.assertIsNone(p.reason)

    def test_latency_transport_and_local_cpu_are_separate(self):
        for mode, expected in (('latency', 'sustained_latency'), ('transport', 'transport_failures'), ('cpu', 'client_cpu_limit')):
            p = onset.Policy(.1, 100); p.tick(0)
            for second in range(1, 31):
                p.sample(second, 32, True, 0, 0, second*3.9 if mode == 'cpu' else 0)
                for i in range(3):
                    p.complete(second-i*.01, .4 if mode == 'latency' else .1)
                    p.network_end(second-i*.01, 200, {'failure_kind':'transport'} if mode == 'transport' else {'body_completed_at':second})
                p.tick(second)
            self.assertEqual(p.reason, expected)


class NativeTests(unittest.TestCase):
    def setUp(self):
        buf = io.BytesIO(); Image.new('RGB', (8, 8), '#339977').save(buf, format='JPEG'); body = buf.getvalue()
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.server.handled += 1
                self.send_response(200); self.send_header('Content-Length', str(len(body))); self.end_headers()
                self.wfile.write(body)
            def log_message(self, *args): pass
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.server.handled = 0
        self.server.daemon_threads = True
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.server_close); self.addCleanup(self.server.shutdown)

    def test_health_is_bounded_and_never_replayed(self):
        with tempfile.TemporaryDirectory() as d:
            study = Path(d)
            urls = [f'http://127.0.0.1:{self.server.server_port}/{i}' for i in range(12)]
            pl.DataFrame({'url':urls}).write_parquet(study/'probe.parquet')
            common.write_new(study/'host-gate.json', {'ignore_time':False})
            with patch.object(onset_run, 'guest_check'), patch.object(onset_run, 'checked_url', side_effect=lambda u:u), \
                 patch.object(onset_run, 'remaining', return_value=600):
                result = asyncio.run(onset_run.health(study))
                self.assertTrue(result['passed']); self.assertEqual(result['valid'], 12)
                self.assertGreater(result['ttfb_p50_s'], 0)
                self.assertEqual(self.server.handled, 12)
                with self.assertRaises(ValueError): asyncio.run(onset_run.health(study))
                self.assertEqual(self.server.handled, 12)

    def test_native_known_images_budget_and_credited_url_receipt(self):
        import download_batch
        from benchmark.real_server_study.verify import verify
        async def run():
            with tempfile.TemporaryDirectory() as d:
                directory = Path(d); run = onset.schedule()[0]
                cfg = onset.config(directory, run)
                urls = [f'http://127.0.0.1:{self.server.server_port}/{i}' for i in range(12)]
                pl.DataFrame({'url':urls}).write_parquet(directory/'input.parquet')
                cfg.update(input=str(directory/'input.parquet'), output=str(directory/'native'), C_min=4, C_init=4, C_max=4)
                common.write_new(directory/'flowdc-config.json', cfg)
                with patch.object(sys, 'argv', ['download_batch.py', '--config', str(directory/'flowdc-config.json')]):
                    config = download_batch.parse_args()
                with (directory/'request-audit.jsonl').open('x') as stream:
                    guard = telemetry.Guard(urls, stream, 1, policy=onset.Policy(1, 100), body_limit=2**20)
                    guard.emit('start', run_id='fixture', rows=12)
                    with patch.object(telemetry, 'checked_url', side_effect=lambda u:u):
                        restore = guard.install(); started = time.monotonic()
                        try:
                            download_batch.shutdown_flag = False
                            await download_batch.run_acquisition(config)
                        finally:
                            if guard.sampler:
                                guard.sampler.cancel(); await asyncio.gather(guard.sampler, return_exceptions=True)
                            restore()
                        ended = time.monotonic()
                    guard.emit('closed', active=len(guard.active), stop_reason=None)
                verified = verify(directory, directory/'input.parquet', Path(__file__).resolve().parents[1], started, ended, credit_urls=True)
                self.assertEqual(verified['credited_rows'], 12)
                self.assertEqual(set(verified['credited_url_bytes']), set(urls))
                self.assertEqual(sum(verified['credited_url_bytes'].values()), guard.body_bytes)
                self.assertFalse(guard.active)
        asyncio.run(run())


if __name__ == '__main__':
    unittest.main()
