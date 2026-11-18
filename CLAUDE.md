# Repository guidance

Read [AGENTS.md](AGENTS.md) for shared rules, current layout and validation.
Use [docs/architecture.md](docs/architecture.md) to locate subsystem boundaries and
[CONTRIBUTING.md](CONTRIBUTING.md) for the development commands.

The default downloader is PAARC; PAARC-Ratio is its explicit alternative.
Both use the same acquisition and integrity pipeline. The canonical manuscript is
under `paper/`. Optional operational service releases retain their own deployment
boundary and must never be upgraded implicitly during product work.

Current method identifiers are `paarc` and `paarc-ratio`. PAARC uses a normalized
excess-delay rate, a sampled finite difference over time. Preserve original identifiers in
historical evidence and read-only verification; consult the migration guide before
changing configuration or telemetry names.
