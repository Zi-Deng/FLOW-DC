# Verification

Run affected tests during implementation. For substantial changes, run `make check` once and observe current CI. `make check-clean` verifies committed cleanliness. One current workflow suite runs within120seconds; do not run duplicate serial/parallel/installed matrices.

Regression tests exercise observable behavior: immutable snapshots/path refusal, provider selection/read-only command controls, credential/billing refusal, deadline/process cleanup and stale/idempotent publication. They do not assert old module counts, byte preservation or generation histories. Actual native review and scientific evidence remain separately attributed.
