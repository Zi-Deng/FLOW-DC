# FLOW-DC consolidation — September 14, 2026

The local `main` branch now incorporates all six published GitHub updates after its January 21 checkout, while preserving the April gradient-controller and benchmark work. This is a local consolidation; no changes were pushed to GitHub.

## Inputs and Git operations

- Local starting HEAD: `6f892fbe8a53d672e0583bb9e6c3a0afcc74c6ef`.
- Verified GitHub main: `efdadb2f5e67b0765a3fe8749af4dc25ad018f3b`.
- Created source snapshots, a Git bundle, a binary patch, and a backup of benchmark results under [archives](../archives/README.md).
- Ran `git fetch` using the public GitHub URL, `git stash push` for tracked edits, `git merge --ff-only origin/main`, and `git stash apply`.
- Resolved one conflict in the base controller loop in favor of the local sample-aware scheduling correction, retaining the published changes elsewhere.
- Retained the recovery stash. The final consolidation is recorded in a local commit on `main`.

## Consolidated implementation

1. The main downloader retains GitHub's host-based worker sizing, explicit overwrite permission, filename accessors, embedded overview reports, updated controller defaults, and import-safe signal registration. It also retains the local controller snapshot return and sample-aware live scheduling.
2. The gradient downloader now passes the required worker-count argument and shares worker sizing, connection-pool sizing, signal initialization, and overview/archive reporting with the base downloader. Gradient counters remain in both reports. JSON and CLI overwrite controls work, including `--force` with `--config`.
3. The gradient experiment retains its prior defaults: 256 workers, `mu=0.75`, and 15-second RTprop retention. The main downloader uses automatic workers, `mu=0.85`, and 35 seconds. Explicit config overrides remain supported; gradient CLI now also exposes `rtprop_window`.
4. The benchmark adapter uses the current Python interpreter and explicitly permits replacing the output directory it already owns/recreates. This avoids the new downloader's overwrite prompt blocking automated benchmark runs.
5. The cloud orchestrator now uses the maintained TaskVine PAARC configuration builder. It translates legacy enable/concurrency aliases and keeps compressed tar output as required by its existing upload commands. Old PolicyBBR rate-specific settings have no direct equivalent in PAARC and are not forwarded. Cloud execution still uses the threshold-based downloader; gradient TaskVine/UI integration was not added.
6. The published Polars partitioner, TaskVine improvements, utilities, reference partitions, configurations, and environment changes are incorporated. The pip requirements also include the newly needed `requests` dependency and reflect the TaskVine pin in their installation note.
7. Maintained benchmark source, configurations, templates, and manifests are now versioned. Generated results and payloads remain ignored. Finder metadata is retained locally but removed from version control.
8. Historical PolicyBBR notes and `playground/old_configs/` were moved into `archives/2026-09-14/legacy/`. Original versions of superseded files remain in the local recovery snapshot. Alternate downloader implementations and experimental datasets were retained because their age alone does not establish that they are obsolete.

## Research evidence and preservation

The April benchmark results remain historical measurements of the pre-consolidation implementation. They were not regenerated or relabeled as results for this version. Original source/configuration snapshots are preserved alongside the backup. The existing separate manuscript folder and the essentially empty `github/old/FLOW-DC` folder were not altered.

The source snapshot was verified against SHA-256 hashes. Existing benchmark result contents were compared byte-for-byte with the archive; retained dataset/output/reference files were checked for unchanged sizes and modification times, excluding Finder metadata. Moved old configs retained their contents and timestamps. See the recorded counts in [validation.json](../archives/2026-09-14/validation.json).

## Validation

`python -m unittest discover -s tests -v` passed all eight focused tests, including subcases for both downloaders and both compressed/uncompressed archives. Tests exercise local HTTP downloads, transient retries, non-retryable failures, payload contents, imagefolder/webdataset outputs, embedded/external reports, existing-output refusal and forced replacement, benchmark execution, config/default compatibility, controller snapshot/scheduling, and import-time signal behavior.

Changed/imported Python entry points passed compilation checks, and the Polars partitioner CLI loaded successfully. No external dataset downloads or full research campaigns were run. The environment lacks `ndcctools`, so the cloud config translation test uses a mocked TaskVine import; it does not establish live distributed or cloud execution correctness. The UI remains a simulation prototype and is documented accordingly.

Run the tests with the existing environment:

```bash
.venv/bin/python -m unittest discover -s tests -v
```
