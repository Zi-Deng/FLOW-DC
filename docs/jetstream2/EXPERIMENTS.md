# Experiments on provisioned resources

Use the [frozen experiment runner](../../experiments/README.md) for local and
TaskVine acquisitions. Configure an existing qualified manager/runtime and worker
resources using [distributed execution](../distributed.md). The runner neither
activates cloud instances nor changes operational allowances.

The [pilot lifecycle](LIFECYCLE.md) and [finite campaign accounting](CAMPAIGN.md)
remain separate. Obtain authorization for each activation, network or allowance
change. Preserve the installed supervisor, immutable releases, registration,
accounting journal, locks, ownership records and cleanup obligations.

Historical guest experiments require their recorded source revision for recovery.
Their state is not automatically migrated to the package runner. An existing
experiment owner blocks grants until the original recovery procedure verifies
that all associated instances are `SHELVED_OFFLOADED` and retires that owner. Never
delete ownership or lock files to bypass these checks. Source history and retained
private recovery records supply the original procedure.
