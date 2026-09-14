# FLOW-DC archives

The September 14, 2026 consolidation integrated GitHub `main` at `efdadb2f5e67b0765a3fe8749af4dc25ad018f3b` into the local repository, previously at `6f892fbe8a53d672e0583bb9e6c3a0afcc74c6ef` with additional uncommitted research.

## Historical files moved out of active paths

- [Legacy PolicyBBR controller notes](2026-09-14/legacy/POLICYBBR_ENHANCEMENTS.md): previously `docs/POLICYBBR_ENHANCEMENTS.md`; describes the older controller rather than current PAARC.
- [Old example configurations](2026-09-14/legacy/old_configs/): previously `playground/old_configs/`; retain their original contents as historical references, not current runnable examples.

## Local recovery snapshot

[2026-09-14/pre-consolidation/](2026-09-14/pre-consolidation/) contains the original working versions, including superseded downloader, partitioner, TaskVine, dependency, config, and documentation files. It also preserves the April gradient and benchmark implementation before adaptation. Source-file contents were checked against SHA-256 values in `source-manifest.json`.

- `local-tree/`: 95 original tracked/untracked project and maintained support files, including benchmark manifests and old configurations. Original paths are preserved beneath this directory.
- `github-tree/`: pristine published versions of selected files adapted during integration. The complete GitHub source is also available from its commit in the Git bundle.
- `repository.bundle`: Git history and refs from before the fast-forward. It excludes remote configuration/credentials.
- `local-changes.patch`: original tracked uncommitted changes, including binary changes.
- `refs.json`, `git-status.txt`: input commit IDs and original status.
- `benchmark-results.tar.gz`: complete backup of the existing benchmark results. The live results remain in `benchmark/results/`.
- `preserved-artifacts.json`: original sizes and timestamps of generated/reference files retained in place, or relocated as old configs.

This recovery directory is deliberately **local-only and Git-ignored**, including its large result archive. The archive index, legacy notes/configs, and validation record are versioned. The snapshot is not a full workstation backup: it excludes virtual environments, Git configuration, and the large ignored download/playground datasets, which remain at their original paths.

The original tracked edits are additionally retained in Git stash:

`69ba4f05e5369e8b3c1df5b2b6270eea3d43cde3`

To inspect the recovery history without changing this repository:

```bash
git bundle verify archives/2026-09-14/pre-consolidation/repository.bundle
git clone archives/2026-09-14/pre-consolidation/repository.bundle /path/to/a/new/recovery-checkout
```

The bundle restores committed history. Use `local-tree/` and the patch to inspect/recover the former uncommitted state in that separate checkout; do not apply the old patch directly to the consolidated working tree.

See [the consolidation record](../docs/CONSOLIDATION_2026-09-14.md) and [validation results](2026-09-14/validation.json).
