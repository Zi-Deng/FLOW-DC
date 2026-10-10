# Artifact reproduction

Installation, software tests, scientific reanalysis, native execution and manuscript
builds are separate checks. Retain their source revision, commands, exits, environment
and limitations. A passing smoke cannot establish the full artifact or efficacy.
Private operational inventory, SSH credentials, manuscripts and third-party payloads
are not part of the public repository or reviewer snapshot.

## Source and environments

Check out the exact revision named by the artifact. Scientific results name their
own immutable source export; a later operations fix does not change that source.
Record `git rev-parse HEAD` and `git status --short`. Use Linux/POSIX and Python 3.12
for the qualified harness, with JDK 17+ for the actual Gradient2 Java comparison.

Create environments in new directories. Install with pip's `--no-cache-dir` and
`--report`, then retain `pip check`, `pip freeze` and the harness environment record.
The declared direct dependency pins are not a complete transitive, hash-locked
environment. A release must also include its resolved installation/lock records;
inspect download URLs before redistribution. Keep the older img2dataset NumPy1
environment separate from the SciPy/NumPy2 analysis environment.

```bash
python3.12 -m venv .agentic-local/reproduce-tools
.agentic-local/reproduce-tools/bin/python -m pip install --no-cache-dir \
  --report .agentic-local/reproduce-tools-install.json \
  -r benchmark/requirements-research.txt
.agentic-local/reproduce-tools/bin/python -m pip check
NO_ALBUMENTATIONS_UPDATE=1 WANDB_MODE=disabled \
  .agentic-local/reproduce-tools/bin/python -B benchmark/known_truth.py \
  --output benchmark/results/reproduce-known-truth
```

This nine-original-row localhost fixture must independently verify six eligible
outputs and 2,157 original bytes for both actual tools, with observed attempt counts
and process cleanup. Inspect `assessment.json`, both native results and origin
records; a recorded process exit alone is insufficient. Never reuse an output
directory or replace the first attempt. No external URLs or cloud VMs are involved.

`python3 -B scripts/verify_gradient2.py` compares deterministic traces with the
actual pinned Java class. Its temporary SLF4J dependency is checksum-verified.
The pin, source and license are in `third_party/netflix-gradient2/`. Record the
trace/scenario count from the result rather than copying a historical count.

## Retained scientific results

The artifact must provide the exact plan, protocol, source/environment bindings,
every planned first attempt and all later attempts. Restore compressed synthetic
records losslessly before analysis and verify the artifact manifest. Third-party
payload exclusions and missing records remain explicit.

Run the analysis command in an isolated environment containing its recorded
Polars, psutil, Pillow, NumPy, SciPy and Matplotlib versions:

```bash
python -B benchmark/analyze.py --plan /absolute/artifact/evaluation/plan.json \
  --study-root /absolute/artifact --protocol /absolute/artifact/protocol.json \
  --output /absolute/new-analysis-directory
```

The command independently checks original-row outputs, attempts and control
records before generating JSON, CSV and PDF/PNG figures. Compare numerical analysis
content and source-bound inputs; a different plotting/runtime version need not
produce byte-identical PDFs. All missing, censored and invalid cells remain visible.
Pilot figures carry their pilot status and cannot become confirmatory evidence.

Reexecution is a separate reproduction dataset. Run a controlled comparison and
representative single-mechanism ablation with the released selected configurations
and predeclared inputs. A new environment has a new binding: do not pretend it is
the original campaign or pool its outcomes into confirmation. Freeze a reproduction
plan/protocol with the actual authority, resource policy and finite limits first.
See [STUDY-HARNESS.md](STUDY-HARNESS.md) for plan/freeze/run-cell commands. A changed
ephemeral localhost port changes manifest bytes; matched seeds preserve semantic
row/object assignments and payloads, while each actual manifest hash is recorded.

## Native runtime and private paper

Use matching patched CCTools 7.17.2 manager, worker and Python binding, following
`third_party/cctools/README.md` and `scripts/build_research_runtime.py`. Preserve the
unpatched crash evidence separately. `benchmark/package_environment.py` packages
the dedicated environment under `.agentic-local/`, including explicit installed
native overlays and required launcher. Check the archive's installed binary hashes,
then test a freshly relocated prefix; successful packing alone is insufficient.

Run `benchmark/taskvine_local.py --help` and the bounded `primary` native fixture
with the verified archive, matching worker and a new output directory. Record
actual worker participation, original-row verification, admission/dispatch counts
and owned-process cleanup. Required failure cases are specified in
[DISTRIBUTED-WORKFLOW.md](DISTRIBUTED-WORKFLOW.md). These commands never authorize
or activate provider resources.

The private coauthor package separately includes source and a Makefile for the main
paper and supplement. Build in fresh source directories with its declared TeX/
IEEEtran environment, check resolved references and trace every quantitative claim
to retained analysis. Same-host clean builds are not independent-host reproduction.
Final release readiness also requires the complete scientific evidence and exact
author approval; those facts cannot be generated by a software test.
