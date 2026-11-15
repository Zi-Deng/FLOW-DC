# Experiments

The package provides one frozen-protocol runner for local and TaskVine acquisition.
Numerical inputs supporting the current manuscript are kept once, under
[`paper/data`](../paper/data/); they are compact projections of retained evidence.
The original records remain private and unchanged.

## Prepare and run

Protocol JSON files explicitly define source, methods/configurations, block count,
row count, worker count, random seed, deadlines and body budgets. Source access and
media rights default to unqualified. Establish those prerequisites and supply a
publisher-derived URL manifest before executing public acquisitions.

```sh
flowdc experiment prepare --protocol experiments/protocols/cvdf-comparison.json \
  --input-path /absolute/frozen-population.parquet --output-dir /absolute/new-study
flowdc experiment run --study /absolute/new-study \
  --comparator-python /absolute/comparator-env/bin/python
flowdc experiment analyze --study /absolute/new-study
flowdc experiment verify --study /absolute/new-study
```

Preparation is offline. It stamps original identities, selects disjoint blocks,
records input/protocol hashes and freezes method ordering before outcomes exist.
An existing run directory consumes its first-attempt slot even after interruption;
normal continuation executes only cells that have never started. Failures,
unstarted cells and missing comparisons remain visible. `--limit` bounds the number
of newly started cells in one invocation.

Add `--taskvine-config` to run matched native worker jobs. Worker counts come from the
frozen condition. img2dataset process/thread budgets are split across workers while
preserving their aggregate count. Resource provisioning is separate; this command
does not activate VMs, change allowances or install a supervisor.

## Comparator environment

Keep the pinned comparator separate from the NumPy-2 analysis environment:

```sh
python3.12 -m venv /absolute/comparator-env
/absolute/comparator-env/bin/python -m pip install -r experiments/comparator-requirements.txt
```

The adapter requires img2dataset 1.47.0, disables resizing/reencoding and request/shard
retries, preserves row metadata and observes each native shard. Its original archives
and ledger are independently checked before sealing a common verification archive.
This extra processing is included in the practical endpoint, not treated as free.

## Measurement and interpretation

Verified goodput divides eligible image bytes by complete job time through independent
verification. Coverage uses original requested rows. Decode failures receive no useful
byte credit. Request activity and native/HTTP spans remain separate diagnostics.
The clients retain their native timeout/connection semantics; this compares complete
acquisition configurations rather than isolated controller substitution.

Analysis uses runs as units, reports paired log-ratio intervals when complete,
content-consistent pairs exist, and preserves interrupted outcomes. Calibration
candidates are not pooled as independent evaluation replications. The calibration
template deliberately has an unfrozen row count and cannot be prepared as-is.
Choose a feasible row count and freeze the calibration rule before acquisition.
`flowdc experiment select --study STUDY` applies the recorded minimum HTTP duration,
coverage tolerance and goodput tie rule. Within the goodput tolerance it chooses
lower observed mean HTTP activity. Ineligible or missing candidates remain
unselected. Freeze the resulting configurations in a separate evaluation protocol;
`prepare --exclude-study CALIBRATION_STUDY` excludes every calibration URL when
sampling confirmation inputs. The prospective template leaves sizes/configurations
unfrozen and cannot run until those decisions are explicit. The short historical observations do not
supply missing long-run evidence.

The historical TaiBIF protocol includes an explicit host-scoped certificate-date
exception. Chain and hostname checks remain enabled. That exception never applies
to another host; current source access must still be qualified. Runtime guards honor
explicit refusals, publisher opt-outs and body budgets. Latency is diagnostic in the
current guarded comparison; user pauses are explicit and server Retry-After remains
effective. No public acquisition is part of ordinary software qualification.
