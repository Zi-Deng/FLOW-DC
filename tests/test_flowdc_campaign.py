"""Finite campaign accounting with real journals and a deterministic clock."""
import copy
import os
import sys
import tempfile
import unittest
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'bin'))
import flowdc_ops as ops
from flowdc_campaign import account_digest, preview, validate_request
from flowdc_pilot import AccountingError, Allowance
from flowdc_pilot_journal import Journal, allowance, allowance_binding_sha256, register, validate_record
from test_flowdc_pilot_lifecycle import FakeClock, access, spec, synthetic_service


class CampaignTests(unittest.TestCase):
    def setUp(self):
        mask = os.umask(0o077)
        self.addCleanup(os.umask, mask)
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        profile = self.root / 'profile.json'
        profile.write_text('{}')
        selected = spec()
        for vm in selected['vms']:
            vm['rate'] = dict(su_per_hour=8, source='https://cloud.example.test/rates',
                observed_at='2026-10-09T00:00:00Z', verified=True, flavor_id='m3.medium')
        self.journal = register(profile, self.root / 'state', selected, access())
        self.clock = FakeClock()

        def settled(record):
            record['service'] = synthetic_service()
            record['events'].append(dict(kind='historical_evidence', data={}))
            for vm in record['vms'].values():
                vm['phase'] = 'offloaded'
                vm['observed'] = dict(state='SHELVED_OFFLOADED', clock=asdict(self.clock()))
                vm['account']['consumed'] = 6000
        self.journal.change(settled)
        self.before = self.journal.read()
        self.request = self.make_request(self.before)

    def make_request(self, record):
        return dict(schema='flowdc-campaign-authorization-v1', campaign_id=str(uuid4()),
            registration_id=record['registration_id'], expected_binding_sha256=allowance_binding_sha256(record),
            expected_accounts_sha256=account_digest(record), selected_ids=sorted(record['vms']),
            cumulative_limits_seconds={key:36000 for key in record['vms']},
            expires_at=datetime.fromtimestamp(self.clock().utc+43200,UTC).isoformat(),
            max_window_seconds=1800, protocol_sha256='1'*64, budget_sha256='2'*64, budget_su=200)

    def apply(self, request=None, snapshot=None, recheck=lambda _:None):
        return self.journal.authorize_campaign(request or self.request, snapshot or self.before,
            self.clock(), clock=self.clock, recheck=recheck)

    def test_preserves_pilot_history_consumption_and_restart_replay(self):
        receipt, applied, after = self.apply()
        self.assertTrue(applied)
        self.assertEqual(after['spec'],self.before['spec'])
        self.assertEqual(after['events'][:-1],self.before['events'])
        for vm in after['vms'].values():
            account = allowance(vm['account'])
            self.assertEqual((account.limit,account.consumed,account.remaining),(7200,6000,30000))
            active = account.activation_intent(self.clock(), window_seconds=1800)
            self.assertFalse(active.shutdown_due(self.clock()))
        reopened = Journal(self.journal.root)
        self.assertEqual(reopened.read(),after)
        replay, applied, unchanged = reopened.authorize_campaign(self.request, self.before,
            self.clock(), clock=self.clock, recheck=lambda _:self.fail('replay probes'))
        self.assertFalse(applied)
        self.assertEqual(replay,receipt)
        self.assertEqual(unchanged,after)
        conflict = copy.deepcopy(self.request); conflict['budget_su'] += 1
        with self.assertRaises(ops.OpsError): self.apply(conflict)

    def test_expiry_window_and_remaining_refuse_but_cleanup_keeps_charging(self):
        _,_,after = self.apply()
        account = allowance(next(iter(after['vms'].values()))['account'])
        with self.assertRaises(AccountingError): account.activation_intent(self.clock(),window_seconds=1801)
        active = account.activation_intent(self.clock(),window_seconds=1800)
        self.clock.advance(43200)
        self.assertTrue(active.shutdown_due(self.clock()))
        overdue = active.account(self.clock())
        self.assertGreater(overdue.consumed,overdue.effective_limit)
        self.assertTrue(overdue.observe(self.clock(),state='SHUTOFF').obligation)
        self.assertFalse(overdue.observe(self.clock(),state='SHELVED_OFFLOADED').obligation)
        with self.assertRaises(AccountingError): account.activation_intent(self.clock(),window_seconds=1800)

    def test_clock_ambiguity_exhausts_campaign_without_erasing_obligation(self):
        _,_,after = self.apply()
        account = allowance(next(iter(after['vms'].values()))['account']).activation_intent(self.clock(),window_seconds=1800)
        self.clock.boot = 'new-boot'
        uncertain = account.account(self.clock())
        self.assertTrue(uncertain.uncertain and uncertain.obligation)
        self.assertEqual(uncertain.remaining,0)

    def test_finite_budget_expiry_and_stale_accounts_refuse_without_mutation(self):
        for name,value in [('budget_su',1),('max_window_seconds',14401),
                ('expires_at',datetime.fromtimestamp(self.clock().utc+8*86400,UTC).isoformat()),
                ('expected_accounts_sha256','0'*64)]:
            request = copy.deepcopy(self.request); request[name]=value
            with self.subTest(name=name),self.assertRaises(ops.OpsError): self.apply(request)
            self.assertEqual(self.journal.read(),self.before)
        request = copy.deepcopy(self.request)
        request['cumulative_limits_seconds'][request['selected_ids'][0]]=604801
        with self.assertRaises(ops.OpsError): validate_request(request)
        with self.assertRaises(AccountingError): Allowance(campaign_id='bad')

    def test_explicit_hour_campaign_preserves_inspection_default_and_old_grants(self):
        request = copy.deepcopy(self.request)
        request['max_window_seconds'] = 3600
        _, _, after = self.apply(request)
        account = allowance(next(iter(after['vms'].values()))['account'])
        with self.assertRaises(AccountingError):
            account.activation_intent(self.clock(), window_seconds=3600, inspection=True)
        active = account.activation_intent(self.clock(), window_seconds=3600, inspection=False)
        self.assertEqual(active.shutdown_at_consumed - active.consumed, 3000)
        with self.assertRaises(AccountingError):
            account.activation_intent(self.clock(), window_seconds=3601, inspection=False)

    def test_long_campaign_is_explicit_and_preserves_cleanup_and_consumption(self):
        from unittest.mock import patch
        from flowdc_pilot_supervisor import request as supervisor_request
        request=copy.deepcopy(self.request); request['max_window_seconds']=14400
        _,_,after=self.apply(request)
        account=allowance(next(iter(after['vms'].values()))['account'])
        self.assertEqual(account.consumed,6000)
        with self.assertRaises(AccountingError): account.activation_intent(self.clock(),window_seconds=14400,inspection=True)
        active=account.activation_intent(self.clock(),window_seconds=14400,inspection=False)
        self.assertEqual(active.shutdown_at_consumed-active.consumed,13800)
        with self.assertRaises(AccountingError): account.activation_intent(self.clock(),window_seconds=14401,inspection=False)
        self.journal.change(lambda r:r.update(heartbeat=asdict(self.clock())))
        with patch.object(self.journal, 'supervisor_locked', return_value=True):
            supervisor_request(self.journal, 'start', window=14400, inspection=False, clock=self.clock)
        running=self.journal.read()
        self.assertEqual(running['desired'],'run')
        self.assertEqual(running['window'],dict(seconds=14400,inspection=False))
        self.assertEqual(running['events'][-1]['kind'],'start_requested')
        self.assertEqual([v['account']['consumed'] for v in running['vms'].values()],[6000]*3)

    def test_transport_hour_start_uses_explicit_full_window_flag(self):
        import json
        from unittest.mock import patch
        import flowdc_experiment_transport as transport
        request = copy.deepcopy(self.request); request['max_window_seconds'] = 3600
        _, _, after = self.apply(request)
        controller = transport.Controller.__new__(transport.Controller)
        controller.root = self.journal.root
        controller.journal = self.journal
        controller.record = controller.expected = after
        controller.command = ['fake', 'pilot']
        response = json.dumps({'data':{'registration_id':after['registration_id'],'desired':'idle'}}).encode()
        with patch.object(transport,'verify_release'),patch.object(transport,'execute',return_value=(0,response)) as execute:
            controller.call('start',window=3600)
        self.assertIn('--full-window',execute.call_args.args[0])

    def test_commit_rechecks_snapshot_age_and_local_failure_rolls_back(self):
        def change_then_probe(record):
            self.clock.advance(1000)
        with self.assertRaises(ops.OpsError): self.apply(recheck=change_then_probe)
        self.assertEqual(self.journal.read(),self.before)
        self.journal.change(lambda r:r['events'].append(dict(kind='changed',data={})))
        current = self.journal.read()
        with self.assertRaises(ops.OpsError): self.apply()
        self.assertEqual(self.journal.read(),current)

    def test_second_campaign_history_and_tampered_receipt_refused(self):
        _,_,after = self.apply()
        request = self.make_request(after)
        request['budget_su'] = 250
        request['cumulative_limits_seconds'] = {key:40000 for key in after['vms']}
        _,_,final = self.apply(request,after)
        self.assertEqual(len(final['events']),len(self.before['events'])+2)
        corrupted = copy.deepcopy(final)
        corrupted['events'][-1]['data']['changes'][request['selected_ids'][0]]['before']['consumed']-=1
        with self.assertRaises(ops.OpsError): validate_record(corrupted)
        corrupted = copy.deepcopy(final)
        corrupted['events'][-1]['data']['conservative_allowance_su'] = 0
        with self.assertRaises(ops.OpsError): validate_record(corrupted)

    def test_prepared_case_binding_changes_on_grant_but_not_consumption(self):
        from flowdc_experiment_transport import binding, registration_binding
        old = binding(self.before)
        _,_,after = self.apply()
        granted = binding(after)
        self.assertNotEqual(old,granted)
        self.assertEqual(registration_binding(granted),granted)
        changed=copy.deepcopy(after)
        next(iter(changed['vms'].values()))['account']['consumed']+=1
        self.assertEqual(binding(changed),granted)
        next(iter(changed['vms'].values()))['account']['campaign_expires_utc']+=1
        self.assertNotEqual(binding(changed),granted)

    def test_preview_is_read_only_and_needs_idle_positive_verified_rates(self):
        candidate,_,applied = preview(self.before,self.request,self.clock())
        self.assertTrue(applied)
        self.assertNotEqual(candidate,self.before)
        self.assertEqual(self.journal.read(),self.before)
        for field,value in [('verified',False),('su_per_hour',0)]:
            record = copy.deepcopy(self.before)
            record['spec']['vms'][0]['rate'][field] = value
            request = self.make_request(record)
            with self.assertRaises(ops.OpsError): preview(record,request,self.clock())
        record = copy.deepcopy(self.before); record['desired']='cleanup'
        with self.assertRaises(ops.OpsError): preview(record,self.make_request(record),self.clock())
