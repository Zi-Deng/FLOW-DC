import copy
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from benchmark.supplementary import make_plan, analyze, run
from benchmark.core.study import method_configs
from benchmark.core.truth import digest, encode


class SupplementarySafetyTests(unittest.TestCase):
    def test_missing_pair_cannot_gain_interval_or_adjusted_p(self):
        plan = make_plan('mechanisms', method_configs(), seed=409420)
        def missing(directory, cell, binding, config):
            return {**cell, 'attempt': 1, 'status': 'missing'}
        with patch('benchmark.analyze.first_attempt', side_effect=missing):
            result = analyze(plan, Path('/unused'), {})
        self.assertEqual(len(result['inventory']), 108)
        self.assertEqual(len(result['contrasts']), 15)
        self.assertFalse(result['claim_supported'])
        for item in result['contrasts']:
            self.assertEqual(item['goodput']['status'], 'missing_or_censored')
            self.assertIsNone(item['goodput']['interval'])
            self.assertIsNone(item['goodput']['secondary_family_holm_p'])

    def test_changed_arm_and_unbound_decisions_refuse_before_acquisition(self):
        plan = make_plan('mechanisms', method_configs(), seed=409420)
        decisions = {'approved_plan_sha256': digest(encode(plan)), 'authority': 'maintainer-provisional',
                     'provenance': 'Supplied maintainer decision', 'replication': 'Six paired independent blocks',
                     'estimand': 'Verified bytes per common elapsed second'}
        altered = copy.deepcopy(plan)
        altered['arms']['no-gradient-term']['C_max'] = 4
        with tempfile.TemporaryDirectory() as temporary, patch('benchmark.study.execute_cell') as execute:
            for proposed, receipt in [(altered, decisions), (plan, {**decisions, 'approved_plan_sha256': '0' * 64})]:
                with self.assertRaises(ValueError):
                    run(proposed, {}, receipt, Path(temporary) / 'study', wall_seconds=3600,
                        reserve_bytes=50 * 1024**3)
            execute.assert_not_called()
            self.assertFalse((Path(temporary) / 'study').exists())

    def test_changed_runtime_refuses_before_output_or_requests(self):
        plan = make_plan('supporting', method_configs(), seed=409421)
        decisions = {'approved_plan_sha256': digest(encode(plan)), 'authority': 'maintainer-provisional',
                     'provenance': 'Supplied maintainer decision', 'replication': 'Six paired independent blocks',
                     'estimand': 'Verified bytes per common elapsed second'}
        from benchmark.study import ROOT
        environment = {'source_files_sha256': {'benchmark/study.py': digest((ROOT / 'benchmark/study.py').read_bytes())},
                       'python_version': sys.version, 'python_binary_sha256': digest(Path(sys.executable).read_bytes()),
                       'distributions': [{'name': 'polars', 'version': '0.0.0'}]}
        with tempfile.TemporaryDirectory() as temporary, patch('benchmark.study.execute_cell') as execute:
            with self.assertRaisesRegex(ValueError, 'runtime differs'):
                run(plan, environment, decisions, Path(temporary) / 'study', wall_seconds=3600,
                    reserve_bytes=50 * 1024**3)
            execute.assert_not_called()
            self.assertFalse((Path(temporary) / 'study').exists())


if __name__ == '__main__':
    unittest.main()
