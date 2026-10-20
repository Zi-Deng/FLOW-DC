"""Provider fakes and local files only: no Jetstream credentials or acquisition."""
import copy
from dataclasses import asdict
import io
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import tarfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'bin'))
import flowdc_ops as ops
from flowdc_campaign import activation_binding, resolve_activation
from flowdc_pilot_journal import allowance, register
from flowdc_pilot_supervisor import Supervisor
import flowdc_pilot_cli as cli
from benchmark.real_server_study import cloud, common, offline
from test_flowdc_pilot_lifecycle import FakeClock, access, spec, synthetic_service


class DispositionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.old_mask = os.umask(0o077)
        self.addCleanup(os.umask, self.old_mask)
        profile = self.root / 'profile.json'; profile.write_text('{}')
        self.journal = register(profile, self.root / 'state', spec(), access())
        self.clock = FakeClock()
        self.vm_id = spec()['vms'][0]['id']
        def pending(record):
            record.update(desired='stop', checkpoint='activation_completion_unresolved',
                          service=synthetic_service(), heartbeat=asdict(self.clock()))
            for vm in record['vms'].values():
                vm.update(phase='offloaded', observed={'state': 'SHELVED_OFFLOADED', 'clock': asdict(self.clock())})
            vm = record['vms'][self.vm_id]
            vm.update(phase='unshelve_intent', activation_seen=False,
                      account=asdict(allowance(vm['account']).activation_intent(self.clock(), window_seconds=1800)))
            record['events'].append({'kind': 'unshelve_intent', 'data': {'id': self.vm_id}})
        self.journal.change(pending)
        self.before = self.journal.read()
        response = self.root / 'response.json'; response.write_text('{"provider":"terminal; cannot activate"}')
        self.evidence = {'schema': 'flowdc-activation-disposition-v1', 'resolution_id': str(uuid4()),
            'registration_id': self.before['registration_id'], 'vm_id': self.vm_id,
            'intent_sha256': activation_binding(self.before, self.vm_id), 'provider_reference': 'ticket-example',
            'provider_evidence_file': str(response), 'provider_evidence_sha256': common.file_hash(response),
            'disposition': 'rejected', 'future_activation_possible': False}
        self.observation = {'state': 'SHELVED_OFFLOADED', 'clock': asdict(self.clock())}

    def test_settlement_keeps_charges_history_and_normal_cleanup(self):
        self.clock.advance(20)
        result, receipt, applied = resolve_activation(self.before, self.evidence, self.observation, self.clock())
        self.assertTrue(applied)
        self.assertEqual(result['events'][:-1], self.before['events'])
        vm = result['vms'][self.vm_id]
        self.assertFalse(vm['account']['obligation'])
        self.assertEqual(vm['account']['consumed'], 20)
        self.assertFalse(vm['activation_seen'])
        self.assertEqual(result['desired'], 'stop')
        self.journal.change(lambda current: (current.clear(), current.update(result)))
        Supervisor(self.journal, SimpleNamespace(), clock=self.clock).tick()
        self.assertEqual(self.journal.read()['desired'], 'idle')
        replay, same, applied = resolve_activation(self.journal.read(), self.evidence, self.observation, self.clock())
        self.assertFalse(applied)
        self.assertEqual(same, receipt)
        self.assertEqual(replay, self.journal.read())

    def test_unknown_pending_mismatched_stale_or_active_cannot_settle(self):
        for field, value in [('disposition', 'pending'), ('future_activation_possible', True),
                             ('intent_sha256', '0'*64), ('registration_id', str(uuid4()))]:
            evidence = {**self.evidence, field: value}
            with self.subTest(field=field), self.assertRaises(ops.OpsError):
                resolve_activation(self.before, evidence, self.observation, self.clock())
        for state in ('ACTIVE', 'SHUTOFF', 'SHELVED'):
            with self.subTest(state=state), self.assertRaises(ops.OpsError):
                resolve_activation(self.before, self.evidence, {**self.observation, 'state': state}, self.clock())
        self.clock.advance(121)
        with self.assertRaises(ops.OpsError):
            resolve_activation(self.before, self.evidence, self.observation, self.clock())
        self.assertEqual(self.journal.read(), self.before)

    def test_moving_account_samples_do_not_require_new_authority(self):
        self.clock.advance(10)
        current = copy.deepcopy(self.before)
        vm = current['vms'][self.vm_id]
        vm['account'] = asdict(allowance(vm['account']).account(self.clock()))
        self.clock.advance(10)
        result, _, _ = resolve_activation(current, self.evidence, self.observation, self.clock())
        self.assertEqual(result['vms'][self.vm_id]['account']['consumed'], 20)
        changed = copy.deepcopy(current)
        changed['events'].append({'kind': 'unshelve_intent', 'data': {'id': self.vm_id}})
        with self.assertRaises(ops.OpsError):
            resolve_activation(changed, self.evidence, self.observation, self.clock())

    def test_cli_preview_apply_and_replay_never_mutate_provider(self):
        path = self.root / 'disposition.json'; path.write_text(json.dumps(self.evidence))
        provider = Mock(); provider.observe.return_value = 'SHELVED_OFFLOADED'
        with patch.object(cli, 'verify_release'), patch.object(cli, 'require_persistent_session'), \
             patch.object(cli, 'verify_unit_origin'), patch.object(cli.ops, 'load_profile', return_value={}), \
             patch.object(cli, 'Provider', return_value=provider), \
             patch.object(self.journal, 'supervisor_locked', return_value=True):
            cli.resolve_activation(self.journal, path, clock=self.clock)
            self.assertEqual(self.journal.read(), self.before)
            cli.resolve_activation(self.journal, path, apply=True, clock=self.clock)
            cli.resolve_activation(self.journal, path, apply=True, clock=self.clock)
        self.assertEqual(provider.observe.call_count, 2)  # Preview and apply; replay does not query again.
        provider.lifecycle.assert_not_called()
        self.assertEqual(sum(e['kind']=='activation_resolved' for e in self.journal.read()['events']), 1)

    def test_prepare_continuation_preserves_original_clock_and_accounts(self):
        result, _, _ = resolve_activation(self.before, self.evidence, self.observation, self.clock())
        self.journal.change(lambda current: (current.clear(), current.update(result)))
        Supervisor(self.journal, SimpleNamespace(), clock=self.clock).tick()
        packet=self.root/'packet';packet.mkdir();(self.root/'study').mkdir()
        common.write_new(packet/'clock.json',{'original':True})
        common.write_new(self.root/'study/protocol.json',{'source':'original'})
        allocation=self.root/'allocation.json'
        common.write_new(allocation,{'allocation':'CIS240296','expiry_date':'2027-04-17','cpu_su':34977})
        common.write_new(packet/'operator.json',{'state':str(self.journal.root),'allocation_file':str(allocation)})
        selected={v['role']:k for k,v in self.journal.read()['vms'].items()}
        common.write_new(packet/'deployment.json',{'selected':selected,'campaign_id':str(uuid4()),'source':{'head':'frozen'}})
        rates={'servers':[{'id':k,'su_per_hour':16 if v['role']=='manager' else 8} for k,v in self.journal.read()['vms'].items()]}
        before=self.journal.read();clock_bytes=(packet/'clock.json').read_bytes()
        directory=self.root/'continuation'
        state=cli.status(self.journal)[0]['data'];state['supervisor_ready']=True
        with patch.object(offline.ops,'load_profile',return_value={}),patch.object(cloud,'verify_rates',return_value=rates), \
             patch.object(offline,'local_status',return_value=state), \
             patch.object(offline.subprocess,'check_output',return_value='head\n'):
            record=offline.prepare(packet,directory)
            again=offline.prepare(packet,directory)
        self.assertEqual(record,again)
        self.assertEqual((packet/'clock.json').read_bytes(),clock_bytes)
        self.assertEqual(self.journal.read(),before)
        self.assertEqual((record['max_attempts'],record['window_seconds'],record['audit_seconds'],record['budget_su']),(2,3600,1200,500))
        grant=common.read_json(directory/'grant.json')
        self.assertEqual(grant['protocol_sha256'],common.file_hash(directory/'continuation.json'))
        self.assertEqual(grant['max_window_seconds'],3600)


class OfflineTests(unittest.TestCase):
    def test_embedded_audit_preserves_old_lease_and_has_no_acquisition(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); (root/'analysis').mkdir()
            common.write_new(root/'protocol.json', {'source': {'head': 'frozen'}})
            common.write_new(root/'execution.json', {'old': 'expired scientific lease'})
            original = (root/'execution.json').read_bytes()
            runs = [{'run_id': str(i)} for i in range(28)]
            common.write_new(root/'schedule.json', {'runs': runs})
            for run in runs:
                target=root/'runs'/run['run_id'];target.mkdir(parents=True)
                common.write_new(target/'record.json', {**run, 'complete': True})
            lease={'vm_id':'guest','seconds':1200,'expires_at':common.time.time()+14400,
                   'scientific_source':{'head':'frozen'},'protocol_sha256':common.file_hash(root/'protocol.json'),
                   'continuation_id':'fixture','attempt':1}
            metadata=Mock();metadata.__enter__=Mock(return_value=io.BytesIO(b'{"uuid":"guest"}'));metadata.__exit__=Mock(return_value=False)
            analyzer=Mock(return_value={'complete_runs':28,'analysis_incomplete_runs':0})
            with patch('sys.argv',['audit',str(root),str(root),json.dumps(lease)]), \
                 patch('urllib.request.build_opener') as opener, patch.object(common,'protocol_check',return_value={'source':{'head':'frozen'}}), \
                 patch('benchmark.real_server_study.analyze.analyze',analyzer), patch('os.sched_setaffinity'), \
                 patch('benchmark.real_server_study.prepare.probe',side_effect=AssertionError('acquisition forbidden')), \
                 patch('sys.stdout',new_callable=io.StringIO) as stdout:
                opener.return_value.open.return_value=metadata
                exec(compile(offline.AUDIT,'offline-fixture','exec'),{})
            self.assertEqual((root/'execution.json').read_bytes(),original)
            self.assertEqual(json.loads(stdout.getvalue())['acquisition_calls'],0)
            self.assertGreater(analyzer.call_args.kwargs['seconds'],1100)
            self.assertLessEqual(analyzer.call_args.kwargs['seconds'],1200)

    def test_two_attempts_and_time_or_compute_stop_new_activation(self):
        for mode in ('two', 'expired', 'budget'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as d:
                packet=Path(d)/'packet';packet.mkdir();directory=Path(d)/'continuation';directory.mkdir()
                common.write_new(packet/'operator.json',{'state':'unused'})
                record={'continuation_id':'cont','baseline':{'vm':0},'rates':{'vm':32}}
                journal=SimpleNamespace(read=lambda:{'vms':{'vm':{'account':{'campaign_id':'cont'}}}})
                controller=Mock();controller.journal=journal
                def close(packet,directory,controller,outcome,attempt):
                    common.write_new(directory/f'attempt-{attempt:02}.json',{'cleaned':True})
                with patch.object(offline,'prepare',return_value=record), patch.object(offline,'authorize',return_value=journal), \
                     patch.object(offline,'wall_remaining',return_value=3500 if mode=='expired' else 14000), \
                     patch.object(offline,'exposure',return_value=480 if mode=='budget' else 0), \
                     patch.object(cloud,'controller_for',return_value=controller), patch.object(offline,'wait_ready'), \
                     patch.object(cloud,'refresh_ssh'),patch.object(cloud,'wait_ssh'), \
                     patch('benchmark.real_server_study.storage.mount'), \
                     patch.object(offline,'audit',return_value={'status':'analysis_pending','complete_runs':24}), \
                     patch.object(offline,'close_window',side_effect=close), self.assertRaises(ValueError):
                    offline.run(packet,directory)
                starts=[c for c in controller.call.call_args_list if c.args[0]=='start']
                self.assertEqual(len(starts),2 if mode=='two' else 0)
                self.assertEqual(len(list(directory.glob('attempt-*-intent.json'))),len(starts))

    def test_failed_activation_consumes_attempt_and_stops_before_retry(self):
        with tempfile.TemporaryDirectory() as d:
            packet=Path(d)/'packet';packet.mkdir();directory=Path(d)/'continuation';directory.mkdir()
            record={'continuation_id':'cont','baseline':{'vm':0},'rates':{'vm':32}}
            journal=SimpleNamespace(read=lambda:{'vms':{'vm':{'account':{'campaign_id':'cont'}}}})
            controller=Mock();controller.call.side_effect=RuntimeError('activation rejected')
            with patch.object(offline,'prepare',return_value=record),patch.object(offline,'authorize',return_value=journal), \
                 patch.object(offline,'wall_remaining',return_value=14000),patch.object(offline,'exposure',return_value=0), \
                 patch.object(cloud,'controller_for',return_value=controller),patch.object(offline,'close_window') as close, \
                 self.assertRaisesRegex(RuntimeError,'activation rejected'):
                offline.run(packet,directory)
            controller.call.assert_called_once_with('start',seconds=30,window=3600)
            close.assert_called_once()
            self.assertTrue((directory/'attempt-01-intent.json').exists())
            self.assertFalse((directory/'attempt-02-intent.json').exists())

    def test_complete_audit_is_not_repeated_for_a_collection_window(self):
        with tempfile.TemporaryDirectory() as d:
            packet=Path(d)/'packet';packet.mkdir();directory=Path(d)/'continuation';directory.mkdir()
            record={'continuation_id':'cont','baseline':{'vm':0},'rates':{'vm':32}}
            journal=SimpleNamespace(read=lambda:{'vms':{'vm':{'account':{'campaign_id':'cont'}}}})
            controller=Mock();controller.journal=journal
            def close(packet,directory,controller,outcome,attempt):
                common.write_new(directory/f'attempt-{attempt:02}.json',{'cleaned':True})
            def collect(packet,seconds):
                target=packet/'collected';target.mkdir()
                common.write_new(target/'metadata-manifest.json',{'schema':'flowdc-metadata-collection-v1'})
                return target
            with patch.object(offline,'prepare',return_value=record),patch.object(offline,'authorize',return_value=journal), \
                 patch.object(offline,'wall_remaining',return_value=14000),patch.object(offline,'exposure',return_value=0), \
                 patch.object(cloud,'controller_for',return_value=controller),patch.object(offline,'wait_ready'), \
                 patch.object(cloud,'refresh_ssh'),patch.object(cloud,'wait_ssh'),patch('benchmark.real_server_study.storage.mount'), \
                 patch.object(offline,'local_status',return_value={}),patch.object(offline,'remaining',side_effect=[100,1200]), \
                 patch.object(offline,'audit',return_value={'status':'complete','complete_runs':28}) as audit, \
                 patch.object(offline,'close_window',side_effect=close),patch.object(cloud,'collect',side_effect=collect) as collection, \
                 patch('benchmark.real_server_study.analyze.report'):
                result=offline.run(packet,directory)
            self.assertEqual(result['attempts'],2)
            audit.assert_called_once()
            collection.assert_called_once_with(packet,seconds=900)
            self.assertEqual(len([c for c in controller.call.call_args_list if c.args[0]=='start']),2)

    def test_collection_transfers_nested_metadata_and_verifies_manifest(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);packet=root/'packet';packet.mkdir();study=root/'guest/study';study.mkdir(parents=True)
            directory=study/'runs/example';(directory/'native/.flowdc').mkdir(parents=True)
            native=directory/'native/overview.json';common.write_new(native,{'manifest':True})
            common.write_new(directory/'record.json',{'evidence':{'native/overview.json':common.file_hash(native)}})
            (directory/'native.tar').write_bytes(b'private payload archive')
            common.write_new(packet/'operator.json',{'guest_root':str(study.parent),'guest_python':sys.executable})
            popen=subprocess.Popen
            def local_ssh(argv,**kwargs):
                return popen(shlex.split(argv[-1]),**kwargs)
            with patch.object(cloud,'ssh_configuration',return_value='unused'), \
                 patch('shutil.disk_usage',return_value=SimpleNamespace(free=100*2**30)), \
                 patch.object(cloud.subprocess,'Popen',side_effect=local_ssh):
                result=cloud.collect(packet)
            manifest=common.read_json(result/'metadata-manifest.json')
            self.assertIn('runs/example/native/overview.json',manifest['files'])
            self.assertEqual((result/'runs/example/native/overview.json').read_bytes(),native.read_bytes())
            self.assertFalse((result/'runs/example/native.tar').exists())

    def test_collection_rejects_digest_mismatch_and_duplicate_members(self):
        for mode in ('digest', 'duplicate'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as d:
                packet=Path(d)
                raw=b'{"kept":true}'
                manifest={'schema':'flowdc-metadata-collection-v1','files':{'record.json':{'bytes':len(raw),'sha256':'0'*64}}}
                with tarfile.open(packet/'return.tar','w') as archive:
                    for _ in range(2 if mode=='duplicate' else 1):
                        info=tarfile.TarInfo('record.json');info.size=len(raw);archive.addfile(info,io.BytesIO(raw))
                    data=json.dumps(manifest).encode();info=tarfile.TarInfo('metadata-manifest.json');info.size=len(data)
                    archive.addfile(info,io.BytesIO(data))
                with self.assertRaisesRegex(ValueError,'unsafe collection' if mode=='duplicate' else 'digest mismatch'):
                    cloud.collect(packet)
                self.assertFalse((packet/'collected').exists())


if __name__ == '__main__':
    unittest.main()
