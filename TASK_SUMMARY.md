# FLOW-DC refactor summary

## Result

FLOW-DC is an installable package with one acquisition pipeline and two manuscript
controllers: PAARC-Grad (default) and PAARC-Ratio. The CLI exposes download,
partition, resume, reconcile, verify, experiment and optional operations commands.
Configuration is typed, strict and shared; explicit CLI settings override JSON.
Legacy interfaces are replaced by a migration guide.

HTTP transfer, admission, controller transitions, manifests, naming, reporting,
integrity and distributed coordination have separate modules. Schema-2 historical
outputs remain verifiable without becoming resumable under changed identities.
TaskVine stages one complete package, divides an aggregate request budget across
independent workers, checks patched native build identities and independently
verifies returned artifacts. Local worker cleanup retains process ownership.
Optional operations preserve existing accounting and immutable-release formats.

One experiment runner freezes protocols and input blocks, retains first attempts
and unavailable outcomes, verifies image credit, analyzes matched runs and applies
explicit calibration rules. Confirmation preparation can exclude calibration URLs.
img2dataset 1.47.0 has its own pinned environment. Prospective longer protocols
remain explicitly incomplete until sizes and selected configurations are frozen.

The canonical `paper/` includes the article, supplement, bibliography, 12 CVDF
observations, 12 TaiBIF dispositions, two timing decompositions, 31 engineering
attempts and separate author-reported observations. Numerical inputs preserve
measurements and failure dispositions. The different-machine disclosure is
restored and Table 2 remains pending. Figures, tables, summaries, PDFs, standalone
LaTeX, source ZIP and validation receipts are built under ignored directories.
Historical observations remain historical; no public-host experiment was run.

README, architecture, configuration, integrity, migration, experiment, operations
and contributor documentation reflect the package. Existing AGENTS.md and
CLAUDE.md are updated; no singular AGENT.md is introduced. The existing two
required CI contexts remain unchanged, with wheel installation and a separate
manuscript reproduction job added. Branch protection is unchanged.

## Validation

- Complete local `make check`: 350 product/operations/native tests in 94.583 s;
  27 workflow tests in 0.841 s; Ruff lint/format and repository checks passed.
- All 560 frozen historical controller transitions matched exactly, with focused
  freshness, probing, utilization, overload and recovery checks.
- Loopback HTTP, redirects, Retry-After, invalid/duplicate rows, interrupted
  publication, resume, reconciliation, corruption and archive checks passed.
- A built wheel installed in a fresh environment outside the checkout acquired
  and verified the core-only local example.
- After the complete pass, four affected native tests also passed (23.777 s),
  including the aggregate request ceiling across multiple origins.
- Actual one/two-worker TaskVine acquisitions, worker loss/descendant cleanup and
  a two-worker img2dataset acquisition passed against the disclosed patched
  CCTools 7.17.2 build, with its binding and worker hashes verified.
- Fake-provider operations tests preserve grant/accounting, owner locks, cleanup
  obligations and immutable releases. No live cloud operation was performed.
- Two isolated manuscript builds and a build from the source ZIP reproduced the
  article; arithmetic, references and source-only prerequisites passed. Article
  and standalone remain 12 pages; supplement remains six pages.
- Repository links, private-path/credential patterns, stale entrypoints,
  generated-output exclusions and workflow budgets passed. These checks are
  targeted checks, not a proof that all possible defects or secrets are absent.

The retained workflow has 1,131 runtime lines and 527 test lines, within its
existing budgets. Native execution establishes local software behavior; it does
not establish new public-service performance or general cluster scaling.

## Evidence and workspace cleanup

Preserved 16,464 original evidence files (330,113,800 bytes) unchanged outside the repository. Retained copies were hashed against originals before deletion and checked again afterward. The immutable pre-consolidation backup was relocated atomically and its complete inode/size/mode identity was verified. Sanitized manuscript projections are versioned separately.

Removed 164,686 obsolete workspace files (22,699,841,492 bytes; 21.14 GiB). Installed supervisor state, process identity, credentials, accounting, external datasets/runtimes and sibling worktrees were preserved.

The external retention manifest, policy, cleanup inventory, recovery backup and qualification logs are under `/mnt/storage/flowdc-artifacts/refactor-54/`. Detailed private filenames stay in that external inventory.

## Review and CI

The first independent static review covered revision `a33679b1c74a0e1e91066ccd8fc8413da17a962d` using Claude Code / `claude-opus-5-5` at medium effort. Its [published report](https://github.com/Zi-Deng/FLOW-DC/pull/55#pullrequestreview-5912145819) disclosed partial scope and found one P1 recovery defect and three P3 issues. All four supported findings were addressed in the first repair round:

- Recovery now reads durable journal state, records an absolute input path, supports explicit source relocation with unchanged content identity, and works before overview publication.
- Distributed partitioning honors explicit formats and validates a single input snapshot before creating the destination.
- Overwrite refuses ancestors of the working directory.
- Diagnostics and consent prompts use stderr; acquisition stdout contains result JSON.

The affected package/native suite passed 56 tests in 36.663 s after these repairs, and a newly built wheel passed installation/acquisition/verification outside the checkout. The initial three hosted jobs passed on the reviewed revision. The second and final [static review](https://github.com/Zi-Deng/FLOW-DC/pull/55#pullrequestreview-5912190649) covered revision `5d4c841111805cda51c3afad65851be548868d06` with the same provider/model/effort and CLI version 2.1.312. It reported one P2 privacy issue and one P3 manuscript provenance issue. Both supported findings were repaired in the second round:

- Review inventories use unquoted NUL-separated Git paths, with all excluded trees also removed by Git pathspecs. Regression cases cover Unicode, quotes, backslashes, tabs and newlines in private filenames.
- Paper ZIPs and input hashes use the exact staged originals. Concurrent live-input edits abort publication before a receipt or ZIP is written.
- Standalone qualification now compares compiled citation/reference numbering, page counts and exact embedded figure bytes, and rejects unresolved references or typesetting errors.

Affected validation after the final repair: 28 workflow tests in 0.894 s, the concurrent-input publication regression in 0.007 s, and two isolated paper builds plus source-ZIP reproduction all passed. These are focused checks after the earlier complete local pass. Review scope is professional static judgment, not exhaustive inspection. All three hosted jobs passed on the last reviewed revision. Exact final-head CI, reviewed/final revisions and delta assessment are recorded on [PR #55](https://github.com/Zi-Deng/FLOW-DC/pull/55). The final repair delta covers review-packet privacy, manuscript staging/standalone checks, their regression tests and documentation; it has not received a third automated review. Both allowed invocations and repair rounds are used. The PR remains draft for human delta assessment. No merge, deployment or paper submission is performed.

## Exact original prompt

This section preserves the original request verbatim under the agreed content exception.

```text
<Identity>
You are a coding agent/research agent meant to assist a machine learning/data science PhD candidate.
</Identity>

<Task>
I want to do a complete refactor of the FLOW-DC repository. It is abundantly clear that there are a large amount of redundant, outdated and unnecessary files in this repository; it is particularly noticeable in the untracked memory folder (where we can see that it contains exceedingly large quantities and memory of unneeded files). Take a thorough read through the recently created REPORT.md which states which files and scripts are needed to create the most recent FLOW-DC paper manuscript; this new manuscript is our updated FLOW-DC tool's desired functionality; you can assume that previous iterations/experiments/feature sets are outdated and not required anymore. With this information in mind please do a complete and thorough refactor of my FLOW-DC repository with emphasis/focus on interpretability and readability on the repository itself. You are fully allowed to deleted unneeded scripts and completely change existing scripts and files as needed. The goal is for a competent machine learning PhD candidate or industry machine learning engineers would be able to look through the repository and understand it in a reasonable amount of time. Ensure that the repository is as good if not better as a repository from a top tier tech company or the repository of a top academic paper. Ensure that the updated paper manuscript remains able to created, along with all the appropriate experiments (though it is valid to change the names if deemed required). Keep the overall function of the FLOW-DC repository and tool in tact. Ensure that an appropriate and upated README file exists for the repository after the refactor. Once the repo is refactored, please also appropriately update the CLAUDE.md and AGENT.md files. Additionally, also create a task summary markdown file at the end of the task that contains the exact input prompt, a summary of the task and specifically a summary list of exactly what files and folders were changed or removed.
</Task>

<Instruction>
Ensure that any research done is thorough and rigorous.
Ensure that any research done is up to date for the current time (2026).
Ensure that your response does not contain any errors or hallucinations.
Verify relevant claims by cross referencing the original source; when appropriate, cite the source.
In general, be particularly generous to FLOW-DC when it comes to interpretations of its performance and results, however do not lie.
Ensure any sources referenced are reputable.
Provide mathematical formulas when appropriate.
Ensure that there is no reference meta information regarding this task/prompt.
Ensure that there is absolutely no reference to AI or how the files were generated with the help of AI. 
</Instruction>

<Context>
The user is an Electrical Engineering/Machine Learning PhD candidate at University of Arizona.
The purpose of this task is to do a complete refactor the the FLOW-DC repository for the purposes and readability/interpretability and to remove clutter.
The user is familiar with ML and Deep Learning topics.
The user has a solid background in statistics and mathematics.
</Context>

<Output>
The output to this task is a refactored repository accompanied with an appropriate task summary file.
Ensure your output is comprehensive and thorough.
Ensure your output is well-structured and well-formatted.
Ensure your any text sounds natural and flows well.
Ensure your any code is well-written, befitting of a top tier tech company.
Ensure your response is easy to read and visually appealing.
</Output>
```

## Exhaustive versioned path inventory

Statuses describe the complete refactor relative to `f9362f4`, including moves. Substantial rewrites may appear as a deletion and addition.

### Added

- `.gitattributes`
- `.github/workflows/manuscript.yml`
- `CONTRIBUTING.md`
- `TASK_SUMMARY.md`
- `docs/architecture.md`
- `docs/configuration.md`
- `docs/distributed.md`
- `docs/integrity.md`
- `docs/migration.md`
- `examples/download.json`
- `examples/local_demo.py`
- `examples/manifest.csv`
- `examples/taskvine.json`
- `experiments/README.md`
- `experiments/protocols/calibration.template.json`
- `experiments/protocols/confirmation.template.json`
- `experiments/protocols/cvdf-comparison.json`
- `experiments/protocols/cvdf-engineering.json`
- `experiments/protocols/inaturalist-engineering.json`
- `experiments/protocols/taibif-validation.json`
- `paper/FLOW-DC.tex`
- `paper/Makefile`
- `paper/README.md`
- `paper/claim-evidence.csv`
- `paper/data/author-concurrency.json`
- `paper/data/engineering-campaign.json`
- `paper/data/experiment-status.csv`
- `paper/data/measured.json`
- `paper/data/run-inventory.csv`
- `paper/provenance/evidence.json`
- `paper/provenance/original-reference-keys.json`
- `paper/references.bib`
- `paper/requirements-lock.txt`
- `paper/requirements.txt`
- `paper/scripts/build.py`
- `paper/scripts/check.py`
- `paper/scripts/engineering_campaign.py`
- `paper/scripts/export_single_file.py`
- `paper/scripts/generate.py`
- `paper/sections/applications.tex`
- `paper/sections/architecture.tex`
- `paper/sections/background.tex`
- `paper/sections/engineering-results.tex`
- `paper/sections/evaluation.tex`
- `paper/sections/introduction.tex`
- `paper/supplement.tex`
- `scripts/check_package.py`
- `src/flowdc/__init__.py`
- `src/flowdc/__main__.py`
- `src/flowdc/admission.py`
- `src/flowdc/cli.py`
- `src/flowdc/config.py`
- `src/flowdc/controllers.py`
- `src/flowdc/distributed/__init__.py`
- `src/flowdc/distributed/native.py`
- `src/flowdc/distributed/protocol.py`
- `src/flowdc/distributed/runtime.py`
- `src/flowdc/distributed/worker.py`
- `src/flowdc/downloader.py`
- `src/flowdc/experiments/__init__.py`
- `src/flowdc/experiments/archive.py`
- `src/flowdc/experiments/common.py`
- `src/flowdc/experiments/comparator.py`
- `src/flowdc/experiments/cooldown.py`
- `src/flowdc/experiments/guard.py`
- `src/flowdc/experiments/launch.py`
- `src/flowdc/experiments/metrics.py`
- `src/flowdc/experiments/protocol.py`
- `src/flowdc/experiments/runner.py`
- `src/flowdc/manifests.py`
- `src/flowdc/naming.py`
- `src/flowdc/ops/__init__.py`
- `src/flowdc/ops/runtime/flowdc_campaign.py`
- `src/flowdc/partition.py`
- `src/flowdc/reports.py`
- `src/flowdc/verification.py`
- `tests/__init__.py`
- `tests/fixtures/controllers-v2.json`
- `tests/ops/__init__.py`
- `tests/ops/test_flowdc_account_recovery.py`
- `tests/ops/test_flowdc_campaign.py`
- `tests/test_acquisition.py`
- `tests/test_controllers.py`
- `tests/test_distributed.py`
- `tests/test_experiments.py`
- `tests/test_native.py`
- `tests/test_paper.py`

### Modified

- `.github/workflows/agentic-quality.yml`
- `.github/workflows/flowdc-tests.yml`
- `.gitignore`
- `AGENTS.md`
- `CLAUDE.md`
- `Makefile`
- `NOTICE`
- `README.md`
- `docs/agent-workflow/REVIEW.md`
- `docs/agent-workflow/SETUP.md`
- `docs/agent-workflow/domain-review.md`
- `docs/jetstream2/EXPERIMENTS.md`
- `docs/jetstream2/LIFECYCLE.md`
- `docs/jetstream2/README.md`
- `pyproject.toml`
- `scripts/agentic/review.py`
- `scripts/build_research_runtime.py`
- `scripts/check_repository.py`
- `tests/agentic/test_workflow.py`
- `third_party/cctools/README.md`

### Moved

- `benchmark/requirements-research.txt` → `experiments/comparator-requirements.txt`
- `third_party/cctools/provenance.json` → `src/flowdc/distributed/cctools.json`
- `bin/flowdc_vine_cohort.py` → `src/flowdc/distributed/cohort.py`
- `bin/flowdc_vine_ownership.py` → `src/flowdc/distributed/ownership.py`
- `benchmark/real_server_study/telemetry.py` → `src/flowdc/experiments/telemetry.py`
- `bin/single_download.py` → `src/flowdc/http.py`
- `bin/flowdc_integrity.py` → `src/flowdc/integrity.py`
- `bin/flowdc_ops.py` → `src/flowdc/ops/runtime/flowdc_ops.py`
- `bin/flowdc_pilot.py` → `src/flowdc/ops/runtime/flowdc_pilot.py`
- `bin/flowdc_pilot_cli.py` → `src/flowdc/ops/runtime/flowdc_pilot_cli.py`
- `bin/flowdc_pilot_journal.py` → `src/flowdc/ops/runtime/flowdc_pilot_journal.py`
- `bin/flowdc_pilot_provider.py` → `src/flowdc/ops/runtime/flowdc_pilot_provider.py`
- `bin/flowdc_pilot_supervisor.py` → `src/flowdc/ops/runtime/flowdc_pilot_supervisor.py`
- `bin/flowdc_pilot_topology.py` → `src/flowdc/ops/runtime/flowdc_pilot_topology.py`
- `bin/flowdc_topology.py` → `src/flowdc/ops/runtime/flowdc_topology.py`
- `tests/pilot_allowance_smoke.py` → `tests/ops/pilot_allowance_smoke.py`
- `tests/pilot_offload_compat.py` → `tests/ops/pilot_offload_compat.py`
- `tests/pilot_sdk_transport.py` → `tests/ops/pilot_sdk_transport.py`
- `tests/pilot_systemd_smoke.py` → `tests/ops/pilot_systemd_smoke.py`
- `tests/pilot_upgrade_smoke.py` → `tests/ops/pilot_upgrade_smoke.py`
- `tests/test_flowdc_ops.py` → `tests/ops/test_flowdc_ops.py`
- `tests/test_flowdc_pilot.py` → `tests/ops/test_flowdc_pilot.py`
- `tests/test_flowdc_pilot_allowance.py` → `tests/ops/test_flowdc_pilot_allowance.py`
- `tests/test_flowdc_pilot_cli.py` → `tests/ops/test_flowdc_pilot_cli.py`
- `tests/test_flowdc_pilot_diagnostics.py` → `tests/ops/test_flowdc_pilot_diagnostics.py`
- `tests/test_flowdc_pilot_lifecycle.py` → `tests/ops/test_flowdc_pilot_lifecycle.py`
- `tests/test_flowdc_pilot_network.py` → `tests/ops/test_flowdc_pilot_network.py`
- `tests/test_flowdc_pilot_offload.py` → `tests/ops/test_flowdc_pilot_offload.py`
- `tests/test_flowdc_pilot_upgrade.py` → `tests/ops/test_flowdc_pilot_upgrade.py`
- `tests/test_flowdc_pilot_upgrade_smoke.py` → `tests/ops/test_flowdc_pilot_upgrade_smoke.py`
- `tests/test_pilot_topology.py` → `tests/ops/test_pilot_topology.py`
- `tests/test_integrity_protocol.py` → `tests/test_integrity.py`

### Deleted

- `archives/2026-09-14/legacy/POLICYBBR_ENHANCEMENTS.md`
- `archives/2026-09-14/legacy/old_configs/gbif-old.json`
- `archives/2026-09-14/legacy/old_configs/gbif.json`
- `archives/2026-09-14/legacy/old_configs/gbif_policybbr.json`
- `archives/2026-09-14/legacy/old_configs/imagenet_flow-dc.json`
- `archives/2026-09-14/legacy/old_configs/partition_config.json`
- `archives/2026-09-14/legacy/old_configs/spiderAnomConfig.json`
- `archives/2026-09-14/legacy/old_configs/taskvine_flowdc.json`
- `archives/2026-09-14/legacy/old_configs/taskvine_flowdc_cloud.json`
- `archives/2026-09-14/validation.json`
- `archives/README.md`
- `benchmark/README.md`
- `benchmark/__init__.py`
- `benchmark/analyze.py`
- `benchmark/calibrate.py`
- `benchmark/compare_results.py`
- `benchmark/configs/benchmark_config.yaml`
- `benchmark/core/__init__.py`
- `benchmark/core/controlled_origin.py`
- `benchmark/core/flowdc_adapter.py`
- `benchmark/core/http_cases.py`
- `benchmark/core/img2dataset_adapter.py`
- `benchmark/core/inference.py`
- `benchmark/core/lifecycle.py`
- `benchmark/core/metrics.py`
- `benchmark/core/provenance.py`
- `benchmark/core/qualification.py`
- `benchmark/core/resource_monitor.py`
- `benchmark/core/runner.py`
- `benchmark/core/study.py`
- `benchmark/core/truth.py`
- `benchmark/core/verifier.py`
- `benchmark/gradient_campaign.py`
- `benchmark/known_truth.py`
- `benchmark/manifests/flowdc_gradient_20260429/gbif_multimedia_10000.metadata.json`
- `benchmark/manifests/flowdc_gradient_20260429/gbif_multimedia_10000.parquet`
- `benchmark/manifests/flowdc_gradient_20260429/gbif_multimedia_1500.metadata.json`
- `benchmark/manifests/flowdc_gradient_20260429/gbif_multimedia_1500.parquet`
- `benchmark/manifests/flowdc_gradient_20260429/gbif_multimedia_4000.metadata.json`
- `benchmark/manifests/flowdc_gradient_20260429/gbif_multimedia_4000.parquet`
- `benchmark/manifests/flowdc_gradient_20260429/gbif_multimedia_500.metadata.json`
- `benchmark/manifests/flowdc_gradient_20260429/gbif_multimedia_500.parquet`
- `benchmark/manifests/flowdc_gradient_20260429/gbif_multimedia_500_pilot.metadata.json`
- `benchmark/manifests/flowdc_gradient_20260429/gbif_multimedia_500_pilot.parquet`
- `benchmark/manifests/flowdc_gradient_20260429/inat_s3_10000.metadata.json`
- `benchmark/manifests/flowdc_gradient_20260429/inat_s3_10000.parquet`
- `benchmark/manifests/flowdc_gradient_20260429/inat_s3_20000.metadata.json`
- `benchmark/manifests/flowdc_gradient_20260429/inat_s3_20000.parquet`
- `benchmark/manifests/flowdc_gradient_20260429/inat_s3_500_pilot.metadata.json`
- `benchmark/manifests/flowdc_gradient_20260429/inat_s3_500_pilot.parquet`
- `benchmark/manifests/flowdc_gradient_20260429/manifest_index.json`
- `benchmark/manifests/flowdc_gradient_20260429/openverse_mixed_3000.metadata.json`
- `benchmark/manifests/flowdc_gradient_20260429/openverse_mixed_3000.parquet`
- `benchmark/manifests/flowdc_gradient_20260429/openverse_mixed_500_pilot.metadata.json`
- `benchmark/manifests/flowdc_gradient_20260429/openverse_mixed_500_pilot.parquet`
- `benchmark/package_environment.py`
- `benchmark/plans/engineering-smoke-v1.json`
- `benchmark/plans/provisional-evaluation-v1.json`
- `benchmark/plans/provisional-tuning-v1.json`
- `benchmark/real_server_study/README.md`
- `benchmark/real_server_study/__init__.py`
- `benchmark/real_server_study/__main__.py`
- `benchmark/real_server_study/ablation.py`
- `benchmark/real_server_study/ablation_analysis.py`
- `benchmark/real_server_study/ablation_cloud.py`
- `benchmark/real_server_study/ablation_collect.py`
- `benchmark/real_server_study/ablation_controlled.py`
- `benchmark/real_server_study/ablation_run.py`
- `benchmark/real_server_study/analyze.py`
- `benchmark/real_server_study/cloud.py`
- `benchmark/real_server_study/common.py`
- `benchmark/real_server_study/design.py`
- `benchmark/real_server_study/launch.py`
- `benchmark/real_server_study/offline.py`
- `benchmark/real_server_study/onset.py`
- `benchmark/real_server_study/onset_analysis.py`
- `benchmark/real_server_study/onset_cloud.py`
- `benchmark/real_server_study/onset_run.py`
- `benchmark/real_server_study/prepare.py`
- `benchmark/real_server_study/requirements.txt`
- `benchmark/real_server_study/runner.py`
- `benchmark/real_server_study/storage.py`
- `benchmark/real_server_study/verify.py`
- `benchmark/reports/__init__.py`
- `benchmark/reports/generator.py`
- `benchmark/reports/templates/report.html.j2`
- `benchmark/requirements-analysis-linux-py312.lock`
- `benchmark/requirements-taskvine.txt`
- `benchmark/requirements-tools-linux-py312.lock`
- `benchmark/requirements.txt`
- `benchmark/run_benchmark.py`
- `benchmark/shared_faults.py`
- `benchmark/shared_interrupt.py`
- `benchmark/shared_origin.py`
- `benchmark/shared_restart.py`
- `benchmark/study.py`
- `benchmark/supplementary.py`
- `benchmark/taskvine_local.py`
- `benchmark/tool_study.py`
- `benchmark/topology_plan.py`
- `bin/CalcDatasetSize.py`
- `bin/FLOWDC.ipynb`
- `bin/SplitParquet.py`
- `bin/TaskvineFLOWDC.py`
- `bin/TaskvineFLOWDCCloud.py`
- `bin/download_batch.py`
- `bin/download_batch_gradient.py`
- `bin/download_batch_multithread.py`
- `bin/flowdc_campaign.py`
- `bin/flowdc_experiment.py`
- `bin/flowdc_experiment_artifacts.py`
- `bin/flowdc_experiment_data.py`
- `bin/flowdc_experiment_fixture.py`
- `bin/flowdc_experiment_guest.py`
- `bin/flowdc_experiment_process.py`
- `bin/flowdc_experiment_research.py`
- `bin/flowdc_experiment_source.py`
- `bin/flowdc_experiment_transport.py`
- `bin/flowdc_gradient2.py`
- `bin/flowdc_methods.py`
- `bin/flowdc_research_profile.py`
- `bin/flowdc_shared.py`
- `bin/flowdc_shared_state.py`
- `bin/flowdc_staging.py`
- `bin/flowdc_vine.py`
- `bin/flowdc_vine_native.py`
- `bin/flowdc_vine_protocol.py`
- `bin/flowdc_vine_worker.py`
- `bin/gather_run_stats.py`
- `bin/probe_extensions.py`
- `bin/single_download_gbif.py`
- `bin/ui_app.py`
- `docs/BOUNDED-TOPOLOGY.md`
- `docs/CONSOLIDATION_2026-09-14.md`
- `docs/FLOW-DC-Structure.docx`
- `docs/FLOW-DC-Structure.md`
- `docs/PAARC_PERFORMANCE_BUGS.md`
- `docs/PRODUCTION-CHECKPOINT.md`
- `docs/Research-Proposal-Final.tex`
- `docs/UI_BUILD_GUIDE.md`
- `docs/UI_TUTORIAL.md`
- `docs/download_batch_gradient_guide.md`
- `docs/jetstream2/CLEANUP-RELIABILITY-EVIDENCE.md`
- `docs/jetstream2/experiment.example.json`
- `docs/research/ADAPTIVE-150.md`
- `docs/research/ARTIFACT-REPRODUCTION.md`
- `docs/research/CONTROL-METHODS.md`
- `docs/research/DISTRIBUTED-WORKFLOW.md`
- `docs/research/HTTP-MEASUREMENT-VALIDATION.md`
- `docs/research/HTTP-MEASUREMENT.md`
- `docs/research/MANUSCRIPT-READINESS.md`
- `docs/research/OUTPUT-INTEGRITY-REVIEW-REPAIR.md`
- `docs/research/OUTPUT-INTEGRITY-SECOND-REVIEW-REPAIR.md`
- `docs/research/OUTPUT-INTEGRITY-VALIDATION.md`
- `docs/research/OUTPUT-INTEGRITY.md`
- `docs/research/SHARED-ADMISSION.md`
- `docs/research/STUDY-HARNESS.md`
- `docs/research/TAIBIF-ONSET.md`
- `docs/research/WORKLOAD.md`
- `docs/research/benchmark-contract.md`
- `docs/timely_comparison_with_gradient_paarc.md`
- `environment.yml`
- `files/biotrove_train_stats.json`
- `files/config/biotrove_test_100k.json`
- `files/config/biotrove_test_100k_large.json`
- `files/config/biotrove_train_probe.json`
- `files/config/experiment-paarc-off.json`
- `files/config/experiment-paarc-on.json`
- `files/config/gradient-candidate-v1.json`
- `files/config/gradient2-application-delay-v1.json`
- `files/config/inat_filtered_group_2.json`
- `files/config/inat_multithread.json`
- `files/config/probe_extensions.json`
- `files/config/research-output-integrity.json`
- `files/config/spider_test.json`
- `files/config/spider_test_10g.json`
- `files/config/spider_test_gradient.json`
- `files/config/spiders_inat.json`
- `files/config/taibif_test_config.json`
- `files/config/taibif_test_config_100k.json`
- `files/config/taibif_test_config_10k.json`
- `files/config/taskvine_biotrove.json`
- `files/config/taskvine_biotrove_test.json`
- `files/config/taskvine_shared_origin.json`
- `files/config/taskvine_spider.json`
- `files/config/taskvine_test.json`
- `files/config/test_inat.json`
- `files/input/TaiBIF-Test-GBIF-API.parquet`
- `files/input/TaiBIF-Test.parquet`
- `files/input/biotrove_test_partitions/partition_001.parquet`
- `files/input/biotrove_test_partitions/partition_002.parquet`
- `files/input/biotrove_test_partitions/partition_003.parquet`
- `files/input/biotrove_test_partitions/partition_004.parquet`
- `files/input/biotrove_test_partitions/partition_005.parquet`
- `files/input/biotrove_test_partitions/partition_006.parquet`
- `files/input/biotrove_test_partitions/partition_007.parquet`
- `files/input/biotrove_test_partitions/partition_008.parquet`
- `files/input/biotrove_test_partitions/partition_009.parquet`
- `files/input/biotrove_test_partitions/partition_010.parquet`
- `files/input/gbif_url_10000.parquet`
- `files/input/gbif_url_10000_transformed.parquet`
- `files/input/group_1.parquet`
- `files/input/inat_filtered_group_2.parquet`
- `files/input/partitions/spider100_urls_10gb.parquet`
- `files/input/readme.md`
- `files/input/spider100_urls_10gb.parquet`
- `files/input/spider100_urls_200.parquet`
- `jupyter/readme.md`
- `requirements-dev.txt`
- `requirements-research.txt`
- `requirements-test.txt`
- `requirements.txt`
- `scripts/cleanup-task.sh`
- `scripts/finish-task.sh`
- `scripts/new-task.sh`
- `scripts/verify_gradient2.py`
- `tests/fixtures/gradient2/cases.json`
- `tests/fixtures/gradient2/reference.csv`
- `tests/test_ablation_finalization.py`
- `tests/test_adaptive_ablation.py`
- `tests/test_benchmark_contract.py`
- `tests/test_consolidation.py`
- `tests/test_flowdc_account_recovery.py`
- `tests/test_flowdc_campaign.py`
- `tests/test_flowdc_experiment.py`
- `tests/test_flowdc_experiment_guest.py`
- `tests/test_flowdc_experiment_source.py`
- `tests/test_flowdc_methods.py`
- `tests/test_flowdc_offline_recovery.py`
- `tests/test_gradient2.py`
- `tests/test_gradient2_integration.py`
- `tests/test_http_measurement.py`
- `tests/test_integrity_review.py`
- `tests/test_integrity_second_review.py`
- `tests/test_onset_study.py`
- `tests/test_origin_admission_wait.py`
- `tests/test_output_integrity.py`
- `tests/test_real_server_study.py`
- `tests/test_research_analysis.py`
- `tests/test_research_guest.py`
- `tests/test_research_inference.py`
- `tests/test_research_packaging.py`
- `tests/test_research_qualification.py`
- `tests/test_research_workload.py`
- `tests/test_second_review.py`
- `tests/test_shared_admission.py`
- `tests/test_shared_review.py`
- `tests/test_study.py`
- `tests/test_supplementary.py`
- `tests/test_tool_study.py`
- `tests/test_topology_plan.py`
- `tests/test_vine_cohort.py`
- `tests/test_vine_completion.py`
- `tests/test_vine_contract.py`
- `tests/test_vine_lifecycle.py`
- `third_party/netflix-gradient2/LICENSE`
- `third_party/netflix-gradient2/README.md`
- `third_party/netflix-gradient2/ReferenceTrace.java`
- `third_party/netflix-gradient2/provenance.json`
- `third_party/netflix-gradient2/src/com/netflix/concurrency/limits/Limit.java`
- `third_party/netflix-gradient2/src/com/netflix/concurrency/limits/MetricIds.java`
- `third_party/netflix-gradient2/src/com/netflix/concurrency/limits/MetricRegistry.java`
- `third_party/netflix-gradient2/src/com/netflix/concurrency/limits/internal/EmptyMetricRegistry.java`
- `third_party/netflix-gradient2/src/com/netflix/concurrency/limits/internal/Preconditions.java`
- `third_party/netflix-gradient2/src/com/netflix/concurrency/limits/limit/AbstractLimit.java`
- `third_party/netflix-gradient2/src/com/netflix/concurrency/limits/limit/Gradient2Limit.java`
- `third_party/netflix-gradient2/src/com/netflix/concurrency/limits/limit/measurement/ExpAvgMeasurement.java`
- `third_party/netflix-gradient2/src/com/netflix/concurrency/limits/limit/measurement/Measurement.java`

### Folders changed

- `.github/`
- `.github/workflows/`
- `docs/`
- `docs/agent-workflow/`
- `docs/jetstream2/`
- `examples/`
- `experiments/`
- `experiments/protocols/`
- `paper/`
- `paper/data/`
- `paper/provenance/`
- `paper/scripts/`
- `paper/sections/`
- `scripts/`
- `scripts/agentic/`
- `src/`
- `src/flowdc/`
- `src/flowdc/distributed/`
- `src/flowdc/experiments/`
- `src/flowdc/ops/`
- `src/flowdc/ops/runtime/`
- `tests/`
- `tests/agentic/`
- `tests/fixtures/`
- `tests/ops/`
- `third_party/`
- `third_party/cctools/`

### Entire versioned trees removed

- `archives/` — recursive removal.
- `benchmark/` — recursive removal.
- `bin/` — recursive removal.
- `docs/research/` — recursive removal.
- `files/` — recursive removal.
- `jupyter/` — recursive removal.
- `tests/fixtures/gradient2/` — recursive removal.
- `third_party/netflix-gradient2/` — recursive removal.

### Additional local workspace removals

These paths belonged to the control checkout and were ignored or untracked; its tracked source files were preserved.

- `memory/` — recursive removal; 110,440 files, 9,971,898,412 bytes.
- `playground/` — recursive removal; 3,441 files, 8,219,846,514 bytes.
- `files/output/` — recursive removal; 40,024 files, 3,958,386,233 bytes.
- `benchmark/results/` — recursive removal; 10,241 files, 477,284,504 bytes.
- `paper/` — recursive removal; 516 files, 72,130,356 bytes.
- `real-server-study-scripts/` — recursive removal; 13 files, 214,834 bytes.
- `EXPERIMENT-RECOVERY-PLAN-2026-11-03.md` — file removal; 1 files, 47,532 bytes.
- `real-server-study-plan.md` — file removal; 1 files, 16,585 bytes.
- `.ruff_cache/` — recursive removal; 8 files, 2,182 bytes.
- `.DS_Store` — file removal; 1 files, 14,340 bytes.

- `archives/2026-09-14/pre-consolidation/` — complete immutable tree moved outside the working tree; no contents edited.
