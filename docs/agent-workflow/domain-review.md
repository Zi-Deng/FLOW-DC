# Domain validation rubric

Software correctness and domain validity answer different questions. Passing CI
establishes only the invariants that the checks actually exercise. The domain owner
decides whether the evidence supports the proposed interpretation.

## For this workflow template

- Git operations must preserve unrelated work, ignored artifacts and unmerged commits.
- Reviews must identify the actual head, have a separate context and disclose omissions.
- Missing checks, absent authentication or a CLI failure must not become a success claim.
- A generated review is commentary, not human approval or permission to merge.
- Project adoption must preserve existing code, instructions and validation.
- Private memory must not enter Git, review snapshots or public artifacts.

## Scientific and ML projects

Use the following as a starting point, then encode the project's actual definitions.

| Area | Evidence to inspect |
| --- | --- |
| Splits and leakage | Immutable sample IDs, disjoint splits, grouping unit, duplicates, train-only fitting, validation-only selection |
| Labels and costs | Stable class order; explicit true/predicted orientation; expected-cost decision checked by hand |
| Metrics | Units, normalization, denominators, missing classes, ties, empty cases, macro versus prevalence weighting |
| Training changes | Loss formulas, reductions, gradient direction, dtype, zero/limit cases, seeds, checkpoint compatibility |
| Comparison fairness | Comparable backbones, data, tuning budgets, stopping/selection rules and evaluation protocol |
| Statistical claims | Independent run unit, paired comparisons when appropriate, uncertainty method and stated limitations |
| Provenance | Clean commit, command, config hash, input manifest hash, environment lock, seed list, artifact hashes |
| Reproducibility | Exact, numerically equivalent, statistically supported or procedural; claim only the level measured |

Dataset or model access from the workstation does not imply permission to upload
those assets to a model service or public GitHub. Link authorized immutable artifacts
instead of placing raw restricted data into a review packet.

## Record a validation run

For an explicitly authorized command, from a clean committed worktree:

```bash
python3 scripts/agentic/evidence.py --issue 123 --pr 456 --tier V2 \
  --config config/smoke.json --inputs manifests/smoke-inputs.json \
  --environment-lock environment.lock --seed 42 \
  --artifact .agentic-local/smoke/metrics.json \
  --output .agentic-local/smoke/evidence.json \
  -- python scripts/validate_smoke.py --config config/smoke.json
```

The paths and project command above are examples; this generic template does not
contain that application smoke script. The runner executes the supplied argv without
a shell, records timing and exit status, hashes declared artifacts, and rejects a
dirty starting tree. A failed command still produces a failure manifest when the
process returns normally. Hashes bind files; they do not establish statistical validity
or prove an artifact was freshly produced. Use a new output directory for each run,
and inspect the command's data-generation behavior.

Promote small manifests and checksums deliberately. Store large outputs in approved
artifact storage with retrieval instructions. Avoid mutable `latest` paths and keep
the model review's access consistent with project privacy requirements.
