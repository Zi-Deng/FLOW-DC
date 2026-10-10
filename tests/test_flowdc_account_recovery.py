"""Explicit conservative account recovery; real journals, no live cloud calls."""
import copy
import os
import sys
import tempfile
import unittest
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'bin'))
import flowdc_ops as ops
import flowdc_pilot_cli as cli
from flowdc_campaign import account_digest, recovery_preview, validate_recovery_request
from flowdc_pilot_journal import allowance_binding_sha256, register, validate_record
from test_flowdc_pilot_lifecycle import FakeClock, access, spec, synthetic_service


class AccountRecoveryTests(unittest.TestCase):
    def setUp(self):
        mask = os.umask(0o077)
        self.addCleanup(os.umask, mask)
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        profile = root / 'profile.json'; profile.write_text('{}')
        selected = spec()
        for vm in selected['vms']:
            vm['rate'] = dict(su_per_hour=8, source='https://cloud.example.test/rates',
                observed_at='2026-10-09T00:00:00Z', verified=True, flavor_id='m3.medium')
        self.journal = register(profile, root / 'state', selected, access())
        self.clock = FakeClock()
        self.key = next(vm['id'] for vm in selected['vms'] if vm['role'] == 'origin')
        def settled(record):
            record['service'] = synthetic_service()
            for vm in record['vms'].values():
                vm.update(phase='offloaded', observed=dict(state='SHELVED_OFFLOADED', clock=asdict(self.clock())))
                vm['account']['consumed'] = 6000
        self.journal.change(settled)
        before = self.journal.read()
        grant = dict(schema='flowdc-campaign-authorization-v1', campaign_id=str(uuid4()),
            registration_id=before['registration_id'], expected_binding_sha256=allowance_binding_sha256(before),
            expected_accounts_sha256=account_digest(before), selected_ids=sorted(before['vms']),
            cumulative_limits_seconds={key:36000 for key in before['vms']},
            expires_at=datetime.fromtimestamp(self.clock().utc+86400, UTC).isoformat(),
            max_window_seconds=1800, protocol_sha256='1'*64, budget_sha256='2'*64, budget_su=200)
        self.journal.authorize_campaign(grant, before, self.clock(), clock=self.clock, recheck=lambda _:None)
        self.clock.advance(40000)
        def uncertainty(record):
            record['vms'][self.key]['account'].update(uncertain=True, consumed=36000)
        self.journal.change(uncertainty)
        self.before = self.journal.read()
        self.request = dict(schema='flowdc-account-recovery-v1', recovery_id=str(uuid4()),
            registration_id=self.before['registration_id'], vm_id=self.key,
            expected_binding_sha256=allowance_binding_sha256(self.before),
            expected_accounts_sha256=account_digest(self.before), evidence_sha256='3'*64,
            reason='confirmed_external_activity', max_elapsed_seconds=50000)

    def apply(self, request=None, snapshot=None, recheck=lambda _:None, start=None):
        return self.journal.recover_account(request or self.request, snapshot or self.before,
            start or self.clock(), clock=self.clock, recheck=recheck)

    def test_charges_entire_interval_and_preserves_all_other_state(self):
        candidate, receipt, applicable = recovery_preview(self.before, self.request, self.clock())
        self.assertTrue(applicable)
        self.assertEqual(self.journal.read(), self.before)
        self.assertEqual(receipt['after']['consumed'], 6000+40000+120)
        self.assertTrue(receipt['before']['uncertain'])
        self.assertFalse(receipt['after']['uncertain'])
        preserved = copy.deepcopy(candidate)
        preserved['vms'][self.key]['account'] = self.before['vms'][self.key]['account']
        preserved['events'].pop()
        self.assertEqual(preserved, self.before)
        _, applied, actual = self.apply()
        self.assertTrue(applied)
        self.assertEqual(actual, candidate)
        validate_record(actual)

    def test_never_reduces_existing_conservative_consumption(self):
        def increase(r): r['vms'][self.key]['account']['consumed'] = 90000
        self.journal.change(increase); before=self.journal.read()
        request=dict(self.request, expected_accounts_sha256=account_digest(before))
        receipt, _, _ = self.apply(request, before)
        self.assertEqual(receipt['after']['consumed'], 90000)

    def test_replay_is_idempotent_and_changed_request_refuses(self):
        receipt, _, after = self.apply()
        self.clock.advance(1)
        repeated, applied, current = self.apply(recheck=lambda _:self.fail('replay rechecks'))
        self.assertFalse(applied)
        self.assertEqual((repeated,current),(receipt,after))
        with self.assertRaises(ops.OpsError): self.apply(dict(self.request, evidence_sha256='4'*64))

    def test_reboot_drift_and_elapsed_limit_refuse_without_writes(self):
        original = copy.deepcopy(self.clock.__dict__)
        for mutation in ('reboot','drift','limit'):
            self.clock.__dict__.update(original)
            request=self.request
            if mutation=='reboot': self.clock.boot='new-boot'
            elif mutation=='drift': self.clock.wall_offset+=60
            else: request=dict(request,max_elapsed_seconds=40000)
            with self.subTest(mutation=mutation), self.assertRaises(ops.OpsError): self.apply(request)
            self.assertEqual(self.journal.read(),self.before)

    def test_changed_snapshot_and_expired_provider_proof_refuse(self):
        self.clock.advance(121)
        old_start = type(self.clock())(**self.before['vms'][self.key]['observed']['clock'])
        with self.assertRaises(ops.OpsError): self.apply(start=old_start)
        self.assertEqual(self.journal.read(),self.before)
        self.journal.change(lambda r:r['events'].append(dict(kind='intervening_action',data={})))
        current=self.journal.read(); request=dict(self.request, expected_accounts_sha256=account_digest(current))
        with self.assertRaises(ops.OpsError): self.apply(request)
        self.assertEqual(self.journal.read(),current)

    def test_cleanup_obligations_and_non_idle_state_refuse(self):
        for field,value in [('desired','stop'),('checkpoint','cleanup_pending')]:
            r=copy.deepcopy(self.before);r[field]=value
            with self.subTest(field=field),self.assertRaises(ops.OpsError): recovery_preview(r,self.request,self.clock())
        r=copy.deepcopy(self.before);r['vms'][self.key]['phase']='verify_offload'
        with self.assertRaises(ops.OpsError): recovery_preview(r,self.request,self.clock())
        r=copy.deepcopy(self.before);r['vms'][self.key]['observed']['state']='ACTIVE'
        with self.assertRaises(ops.OpsError): recovery_preview(r,self.request,self.clock())

    def test_normal_writes_cannot_clear_uncertainty_or_edit_recovery_receipt(self):
        with self.assertRaises(ops.OpsError):
            self.journal.change(lambda r:r['vms'][self.key]['account'].update(uncertain=False))
        self.apply()
        with self.assertRaises(ops.OpsError):
            self.journal.change(lambda r:r['events'][-1]['data'].update(limitation='changed'))

    def test_malformed_requests_and_receipt_corruption_refuse(self):
        for name,value in [('reason','automatic'),('max_elapsed_seconds',True),('evidence_sha256','bad'),('vm_id','bad')]:
            with self.subTest(name=name),self.assertRaises(ops.OpsError):
                validate_recovery_request(dict(self.request,**{name:value}))
        _,_,r=self.apply()
        r['events'][-1]['data']['after']['consumed']-=1
        with self.assertRaises((ops.OpsError,ValueError)):validate_record(r)

    def test_cli_apply_uses_fresh_provider_verification_and_owner_lock(self):
        path=self.journal.root/'recovery.json';path.write_text(__import__('json').dumps(self.request))
        with patch.object(cli,'verify_grant_controller'),patch.object(cli,'verify_grant_local'),patch.object(ops,'load_profile',return_value={}),patch.object(cli,'Provider') as provider:
            outcome,code=cli.extend_allowance(self.journal,str(path),clock=self.clock,recovery=True)
        self.assertEqual(code,0)
        self.assertEqual(outcome['operation'],'pilot account-recovery-apply')
        provider.return_value.verify_idle.assert_called_once()


if __name__ == '__main__': unittest.main()
