"""Run-level paired inference with declared multiplicity and visible missing cells."""
import math
import statistics

from scipy import stats

from .truth import require

PRIMARY = ('drop-recovery', 'mixed-sizes', 'sustained-overload')
GRADIENT = 'gradient-candidate-v1'
REFERENCES = ('paarc-base-v2', 'fixed-v1', 'ratio-v1', 'gradient2-application-delay-v1')


def paired_interval(values, *, confidence=.95, one_sided=False, minimum=10):
    require(type(minimum) is int and minimum >= 2, 'invalid minimum blocks')
    require(.5 < confidence < 1, 'invalid confidence')
    if any(value is None for value in values):
        return {'status': 'missing_or_censored', 'planned_blocks': len(values), 'mean': None, 'interval': None, 'p_two_sided': None}
    require(all(type(value) in (int, float) and math.isfinite(value) for value in values), 'invalid paired metric')
    if len(values) < minimum:
        return {'status': 'insufficient_blocks', 'planned_blocks': len(values), 'mean': None, 'interval': None, 'p_two_sided': None}
    mean, sd = statistics.mean(values), statistics.stdev(values)
    se = sd / math.sqrt(len(values))
    critical = float(stats.t.ppf(confidence if one_sided else (1 + confidence) / 2, len(values)-1))
    # Constant paired differences are explicitly degenerate; no hidden NaN.
    p = 1.0 if mean == 0 else 0.0 if se == 0 else float(2 * stats.t.sf(abs(mean / se), len(values)-1))
    return {'status': 'estimated', 'planned_blocks': len(values), 'mean': mean, 'sd': sd,
            'interval': [mean-critical*se, None if one_sided else mean+critical*se],
            'lower_bound': mean-critical*se if one_sided else None,
            'confidence': confidence, 'p_two_sided': p,
            'assumptions': 'Independent paired runs; approximate normal paired means; constant sample differences may understate future variability.'}


def holm(pvalues):
    """Keep the full declared family; unavailable p-values remain unavailable."""
    require(all(p is None or (type(p) in (int, float) and math.isfinite(p) and 0 <= p <= 1) for p in pvalues), 'invalid p-value')
    ordered = sorted((1 if p is None else p, i) for i, p in enumerate(pvalues))
    adjusted, previous = [None] * len(pvalues), 0
    for rank, (p, index) in enumerate(ordered):
        previous = max(previous, min(1.0, (len(pvalues) - rank) * p))
        if pvalues[index] is not None:
            adjusted[index] = previous
    return adjusted


def primary_analysis(inventory, *, blocks, minimum=10):
    """Inventory must contain every planned first attempt; duplicates cannot win."""
    require(type(blocks) is int and 1 <= blocks <= 60, 'confirmation requires 1..60 fixed blocks')
    required = {(s, b, m) for s in PRIMARY for b in range(blocks) for m in (GRADIENT, *REFERENCES)}
    observed = {}
    for record in inventory:
        key = (record['scenario'], record['block'], record['method'])
        require(key in required and key not in observed and record.get('attempt') == 1,
                'unexpected/duplicate cell or replacement attempt')
        require(record['status'] in ('known_terminal', 'missing', 'failed', 'censored'), 'unknown evidence status')
        if record['status'] == 'known_terminal':
            require(all(type(record.get(k)) in (int, float) and math.isfinite(record[k]) and record[k] >= 0
                        for k in ('goodput', 'coverage')), 'invalid terminal run metric')
            require(record['coverage'] <= 1, 'invalid coverage')
            p95 = record.get('p95_s')
            require(p95 is None or (type(p95) in (int, float) and math.isfinite(p95) and p95 > 0
                    and type(record.get('eligible_latency_samples')) is int and record['eligible_latency_samples'] >= 100), 'unqualified run p95')
        observed[key] = record
    require(set(observed) == required, 'inventory omits planned cells; include explicit missing records')
    contrasts = []
    for scenario in PRIMARY:
        for reference in REFERENCES:
            values = {name: [] for name in ('goodput', 'coverage_margin', 'latency_margin')}
            for block in range(blocks):
                candidate, baseline = observed[(scenario, block, GRADIENT)], observed[(scenario, block, reference)]
                valid = candidate['status'] == baseline['status'] == 'known_terminal'
                values['goodput'].append(candidate['goodput'] - baseline['goodput'] if valid else None)
                values['coverage_margin'].append(candidate['coverage'] - baseline['coverage'] + .01 if valid else None)
                latency = valid and candidate.get('p95_s') is not None and baseline.get('p95_s') is not None
                values['latency_margin'].append(1.10*baseline['p95_s'] - candidate['p95_s'] if latency else None)
            efficacy = paired_interval(values['goodput'], minimum=minimum)
            # 24 one-sided safeguard intervals are a separate Bonferroni family.
            safeguards = {name: paired_interval(values[name], confidence=1-.05/24, one_sided=True, minimum=minimum)
                          for name in ('coverage_margin', 'latency_margin')}
            contrasts.append({'scenario': scenario, 'reference': reference, 'efficacy': efficacy,
                              'safeguards': safeguards, 'paired_values': values})
    adjusted = holm([item['efficacy']['p_two_sided'] for item in contrasts])
    for item, p in zip(contrasts, adjusted, strict=True):
        item['efficacy']['holm_p'] = p
        item['claim_supported'] = bool(p is not None and p <= .05 and item['efficacy']['mean'] > 0
            and all(value['status'] == 'estimated' and value['lower_bound'] >= 0 for value in item['safeguards'].values()))
    return {'schema': 'flowdc-primary-inference-v1', 'blocks': blocks, 'contrasts': contrasts,
            'efficacy_family': '12 two-sided paired t-tests with Holm; ordinary 95% intervals are not simultaneous.',
            'safeguard_family': '24 separate one-sided Bonferroni lower bounds at family alpha .05.',
            'unit': 'Independent paired run; requests are not replicates.',
            'inventory': inventory}
