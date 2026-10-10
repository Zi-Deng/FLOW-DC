"""Real retained acquisition re-verification, first-attempt selection and figures."""
import tempfile
import unittest
from pathlib import Path
from uuid import uuid4

from benchmark.analyze import analyze, render
from benchmark.core.study import make_plan, validate_plan, write_new, freeze_protocol, read_plan
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
            plan=read_plan(directory/'plan.json')
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
            self.assertEqual(next(x for x in changed['inventory'] if x['cell_id']==cell['cell_id'])['status'],'integrity_failed')
            self.assertEqual(changed['evidence_status_counts']['integrity_failed'],1)
            self.assertEqual(changed['evidence_status_counts']['failed'],0)
            self.assertEqual(next(x for x in changed['inventory'] if x['cell_id']==cell['cell_id'])['attempt_id'],attempt.name)

    def test_confirmation_limit_is_60_before_execution(self):
        with self.assertRaises(ValueError):
            make_plan(seed=1,blocks=61,purpose='confirmatory',research_workload='bounded-research-v2')

    def test_provisional_confirmation_authority_is_retained_in_json_and_csv(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary); directory=root/'evaluation'; directory.mkdir()
            plan=make_plan(seed=17,blocks=10,purpose='confirmatory',research_workload='bounded-research-v2')
            environment={'source_files_sha256':{}}
            identity=identities(environment)
            for name,value in [('plan',plan),('environment',environment),('binding',dict(plan_sha256=digest(encode(plan)),**identity))]:
                write_new(directory/(name+'.json'),value)
            decisions={'approved_plan_sha256':digest(encode(plan)),
                'provenance':'Test fixture only; no scientific approval.',
                'constraints':'Test fixture only, explicit margins.',
                'estimand':'Test fixture only, paired run goodput.',
                'repetition_rule':'Test fixture only, ten fixed blocks.',
                'approval_authority':'maintainer-provisional'}
            result=analyze(plan,root,protocol=freeze_protocol(plan,decisions,**identity))
            self.assertEqual(result['approval_authority'],'maintainer-provisional')
            self.assertIn('advisor decisions pending',result['scientific_status'])
            self.assertIn('advisor decisions pending',result['protocol_authority'])
            render(result,root/'figures')
            csv=(root/'figures/contrasts.csv').read_text()
            self.assertIn('approval_authority',csv)
            self.assertIn('maintainer-provisional',csv)

    def test_calibrated_sizes_are_bound_to_the_plan_for_each_scenario(self):
        sizes={'drop-recovery':4096,'mixed-sizes':2048,'sustained-overload':1024}
        plan=make_plan(seed=21,rows=sizes,research_workload='bounded-research-v2')
        validate_plan(plan)
        self.assertTrue(all(c['rows']==sizes[c['scenario']] for c in plan['cells']))
        plan['cells'][0]['rows']+=1
        with self.assertRaises(ValueError):validate_plan(plan)
