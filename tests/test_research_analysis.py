"""Real retained acquisition re-verification, first-attempt selection and figures."""
import tempfile
import unittest
from pathlib import Path
from uuid import uuid4

from benchmark.analyze import analyze, render
from benchmark.core.study import make_plan, write_new
from benchmark.core.truth import digest, encode
from benchmark.study import execute_cell, identities


class AnalysisTests(unittest.TestCase):
    def test_real_first_attempt_reverified_missing_cells_and_no_favorable_retry(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            plan=make_plan(seed=812,blocks=1,namespace='engineering',purpose='engineering',rows=100,
                           research_workload='bounded-research-v2')
            directory=root/'engineering';directory.mkdir()
            environment={'source_files_sha256':{}}
            binding=dict(plan_sha256=digest(encode(plan)),**identities(environment))
            for name,value in [('plan',plan),('environment',environment),('binding',binding)]:
                write_new(directory/(name+'.json'),value)
            cell=next(c for c in plan['cells'] if c['scenario']=='drop-recovery' and c['method']=='fixed-v1')
            cell_dir=directory/cell['cell_id'];cell_dir.mkdir()
            attempt=cell_dir/('0001-'+uuid4().hex);attempt.mkdir()
            evidence=execute_cell(attempt,cell,plan['methods'][cell['method']],environment,deadline=15,
                                  research_workload='bounded-research-v2')
            record=dict(cell=cell,attempt_id=attempt.name,status='recorded',source=identities(environment),**evidence)
            write_new(attempt/'record.json',record)
            later=cell_dir/('0002-'+uuid4().hex);later.mkdir()
            write_new(later/'record.json',dict(cell=cell,status='failed'))
            result=analyze(plan,root)
            acquired=next(x for x in result['inventory'] if x['cell_id']==cell['cell_id'])
            self.assertEqual(acquired['status'],'known_terminal')
            self.assertEqual(acquired['eligible_latency_samples'],100)
            self.assertEqual(acquired['attempt'],1)
            self.assertEqual(acquired['later_attempts'],[later.name])
            self.assertEqual(sum(x['status']=='missing' for x in result['inventory']),14)
            self.assertFalse(any(x['claim_supported'] for x in result['contrasts']))
            render(result,root/'figures-a');render(result,root/'figures-b')
            for name in ('analysis.json','contrasts.csv','primary-contrasts.pdf','primary-contrasts.png'):
                self.assertEqual((root/'figures-a'/name).read_bytes(),(root/'figures-b'/name).read_bytes())
            measurement=next((attempt/'run/native/.flowdc/attempts').glob('*/*/measurement.json'))
            measurement.write_bytes(measurement.read_bytes()+b' ')
            changed=analyze(plan,root)
            self.assertEqual(next(x for x in changed['inventory'] if x['cell_id']==cell['cell_id'])['status'],'failed')
            self.assertEqual(next(x for x in changed['inventory'] if x['cell_id']==cell['cell_id'])['attempt_id'],attempt.name)

    def test_confirmation_limit_is_60_before_execution(self):
        with self.assertRaises(ValueError):
            make_plan(seed=1,blocks=61,purpose='confirmatory',research_workload='bounded-research-v2')
