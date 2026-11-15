# TaskVine execution

Use the qualified CCTools 7.17.2 runtime and retained one-line patch under
[`third_party/cctools`](../third_party/cctools/README.md). The upstream correction
addresses the zero-completion debug division; it is not a downloader algorithm
change. The manager and worker native binaries must belong to the qualified build.

Install the core package in the Python environment used for tasks and install
`psutil` in the manager environment. Edit `examples/taskvine.json` to name the
native manager interpreter, worker binary and task interpreter. Supply the builder’s `flowdc-runtime-build.json` as `runtime_receipt`;
the manager verifies the pinned patch and actual native binary hashes. Local workers
are explicit (`start_local_workers: true`).

The manager starts TLS/password-protected native execution and one owned single-shot
worker per partition. Native manager execution is isolated in a child process.
Worker identities and descendants are tracked with Linux process handles and cleanup
must be proven before the run is reported complete. Lost workers never create
fabricated useful-byte credit. Existing destinations are refused to preserve first
attempts; distributed execution does not overwrite or resume them automatically.

`workers`, `cores`, `memory_mb`, `disk_mb`, `timeout_s` and `port` configure execution.
The aggregate request ceiling must support each worker's minimum concurrency.
Each worker's coroutine pool is capped at its allocation, so multiple origins
within a partition cannot exceed the total budget across workers.
Application files are staged as one deterministic package ZIP; source and partition
hashes are checked by each task. The manager independently verifies returned archive
contents, row identity, metadata and byte credit.

Remote workers require a portable `environment_archive` and its exact
`environment_sha256`, containing the task dependencies. The native manager records
its port and unique scheduling features in `endpoint.json`. Launch prepared workers
with TLS, the owner-private password file and the matching feature. Use only exclusive,
trusted workers and the approved network boundary. Features select scheduling; they
are not credentials. Remote worker lifecycle and provider cleanup remain under the
operator's separately authorized operations workflow.

The measured engineering evidence covers one/two-worker configurations. Support for
additional partitions does not establish general scale-out performance.
