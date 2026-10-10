"""Known statistical results and missing/first-attempt refusal."""
import copy
import unittest
from scipy import stats
from benchmark.core.inference import PRIMARY, GRADIENT, REFERENCES, holm, paired_interval, primary_analysis


class InferenceTests(unittest.TestCase):
    def inventory(self):
        return [dict(scenario=s, block=b, method=m, attempt=1, status='known_terminal',
                     goodput=120+(b%3) if m == GRADIENT else 100, coverage=1,
                     p95_s=.09 if m == GRADIENT else .1, eligible_latency_samples=100)
                for s in PRIMARY for b in range(10) for m in (GRADIENT, *REFERENCES)]

    def test_known_paired_interval_and_holm(self):
        values = [2, 4, 5, 1, 3, 8, 9, 4, 6, 2]
        result = paired_interval(values)
        expected = stats.ttest_1samp(values, 0)
        self.assertAlmostEqual(result['p_two_sided'], expected.pvalue)
        self.assertAlmostEqual(result['interval'][0], expected.confidence_interval().low)
        self.assertEqual(holm([.01, .04, .03, .002]), [.03, .06, .06, .008])
        self.assertEqual(holm([.01, None, .02]), [.03, None, .04])
        self.assertEqual(paired_interval([None]*10)['status'], 'missing_or_censored')

    def test_primary_family_and_separate_safeguards(self):
        result = primary_analysis(self.inventory(), blocks=10)
        self.assertEqual(len(result['contrasts']), 12)
        self.assertTrue(all(x['claim_supported'] for x in result['contrasts']))
        inventory = self.inventory()
        for record in inventory:
            if record['method'] == GRADIENT: record['coverage'] = .98
        self.assertFalse(any(x['claim_supported'] for x in primary_analysis(inventory, blocks=10)['contrasts']))

    def test_missing_censor_and_retry_cannot_be_selected(self):
        records = self.inventory(); records[0]['status'] = 'censored'
        result = primary_analysis(records, blocks=10)
        self.assertEqual(result['contrasts'][0]['efficacy']['status'], 'missing_or_censored')
        with self.assertRaises(ValueError): primary_analysis(records[:-1], blocks=10)
        with self.assertRaises(ValueError): primary_analysis(records + [copy.deepcopy(records[1])], blocks=10)
        records[0]['attempt'] = 2
        with self.assertRaises(ValueError): primary_analysis(records, blocks=10)

    def test_latency_minimum_and_boundaries(self):
        records = self.inventory(); records[0]['eligible_latency_samples'] = 99
        with self.assertRaises(ValueError): primary_analysis(records, blocks=10)
        records[0]['p95_s'] = None
        self.assertFalse(primary_analysis(records, blocks=10)['contrasts'][0]['claim_supported'])
        self.assertEqual(paired_interval([0]*10)['p_two_sided'], 1)
        self.assertEqual(paired_interval([1]*10)['p_two_sided'], 0)
