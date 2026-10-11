"""Actual accountable request timings and refusal to infer unobserved stimulus."""
import copy
import tempfile
import unittest
from pathlib import Path
from benchmark.core.qualification import observations, realized_stimulus
from benchmark.core.study import method_configs
from benchmark.core.truth import parse
from benchmark.core.controlled_origin import scenario
from benchmark.study import execute_cell


class QualificationTests(unittest.TestCase):
    def test_completed_request_inventory_and_tamper_refusal(self):
        with tempfile.TemporaryDirectory() as temporary:
            path=Path(temporary)
            config=method_configs()['fixed-v1']
            evidence=execute_cell(path,dict(cell_id='test',scenario='steady',rows=100,fixture_seed=5,method='fixed-v1'),
                config,{'source_files_sha256':{}},deadline=15,research_workload='bounded-research-v2')
            self.assertEqual(evidence['native']['verified_rows'],100)
            truth=parse((path/'fixture/truth.json').read_bytes())
            native=path/'run/native'
            records,metrics=observations(native,truth)
            self.assertEqual(metrics['eligible_latency_samples'],100)
            self.assertGreater(metrics['p95_s'],0)
            self.assertTrue(metrics['complete'])
            member=next((native/'.flowdc/attempts').glob('*/*/measurement.json'))
            member.write_bytes(member.read_bytes()+b' ')
            with self.assertRaises(ValueError): observations(native,truth)

    def test_named_short_run_does_not_realize_late_capacity_or_averaging(self):
        plan=scenario('drop-recovery',{'JPEG':b'x','PNG':b'y'},research_workload='bounded-research-v2')
        events=[dict(phase='service_start',origin_elapsed_s=.5),
                dict(phase='response',origin_elapsed_s=1,status=200,disconnected=False,body_bytes_written=1)]
        result=realized_stimulus(plan,events)
        self.assertFalse(result['qualified'])
        self.assertFalse(result['checks']['capacity_phase_2']['passed'])
        self.assertFalse(result['checks']['averaging_observations']['passed'])
        self.assertFalse(result['checks']['duration']['passed'])
