# Bounded three-VM experiments

`bin/flowdc_experiment.py` adds offline preparation and explicit execution to the
[installed pilot](LIFECYCLE.md). It uses exactly the registered manager, one worker
and origin. It neither creates VMs nor installs packages, enrolls credentials,
upgrades the supervisor, renews allowance or changes PAARC. The six pilot modules
and their journal/accounting remain unchanged.

**These instructions do not authorize activation.** Running the new CLI against
cloud guests is a later, separately authorized operational checkpoint. Local
fixtures and fake infrastructure checks establish software behavior, not a new
live TaskVine result, performance comparison or scientific conclusion.

## Prerequisites and manual checkpoints

Use Linux and Python 3.12+, Git, OpenSSH (`ssh`, `ssh-keygen`) and the existing
Polars environment locally. Use the existing registered state root, reviewed
installed pilot release, healthy supervisor, and persistent local user manager
(`Linger=yes`). The requested window must fit every VM's remaining cumulative
allowance. A fresh run identity never replenishes it.

Before authorizing activation, privately verify that each guest already has:

- The same non-root SSH account, existing absolute Python environment and an
  owner-private (`0700`) experiment parent directory. No path may traverse a
  symlink. Each run creates a new child; existing checkouts remain untouched.
- Python 3.12+. Manager: `ndcctools.taskvine`, Polars, aiohttp and tqdm. Worker:
  Polars, aiohttp, tqdm and `vine_worker` supporting `--workspace`, `--wall-time`
  and `--single-shot` (the established environment uses ndcctools 7.15.8).
- Space for the configured disk budget and finite systemd transient services.
  The default `guest.service_mode: "system"` uses existing noninteractive
  `sudo -n /usr/bin/systemd-run` and `sudo -n /usr/bin/systemctl` authorization.
  Services explicitly run as the SSH account's UID/GID, with time, memory, CPU,
  process and per-file size limits. No sudoers or service installation changes
  are made. The runner probes the existing sudo policy before deploying work.
- Alternatively, explicit `service_mode: "user"` requires an available guest user
  manager **and verified `Linger=yes`** so services survive SSH logout. That guest
  prerequisite has not been established for the current VMs. The runner does not
  enable lingering or assume user services are available.

Offloaded guests cannot answer SSH probes. Thus these are manual pre-activation
prerequisites; the runner rechecks the actual guest environment **after startup
and before deployment/workload admission**, and stops on failure. It does not
claim an online guest probe occurred before activation.

Keep the existing owner-private SSH identity and known-hosts files outside Git.
Their paths are local references; private-key contents never enter the bundle.
The enrolled host aliases are `flowdc-manager`, `flowdc-worker`, `flowdc-origin`.
Verify those entries through the existing independent host-key enrollment process;
do not accept newly scanned keys on first connection. The manifest binds the
known-hosts digest and these roles to the registration's VM identities. Replacing
an enrolled VM/key requires explicit operator trust reconciliation, not an
experiment option. No reenrollment is performed by preparation.

SSH uses a generated configuration with `BatchMode=yes`,
`StrictHostKeyChecking=yes`, `IdentitiesOnly=yes`, no agent forwarding and no user
configuration hooks. Worker/origin use `ProxyJump manager`. Destinations come
from enrolled private addresses and a context-verified, read-only lookup of the
manager's registered floating route when needed. No arbitrary ProxyCommand,
shell-command, environment-variable or destination field is accepted.

## Prepare offline

Copy [experiment.example.json](experiment.example.json) to a private file outside
Git and replace all placeholders with existing paths, the actual registration
UUID and an explicit locally available hexadecimal commit ID. `HEAD`, branch
names and tags are deliberately rejected. Read the registration through the
installed pilot's `status`, using its pinned interpreter/release paths. A changed
registration or installed service binding requires a newly prepared experiment.

The example selects `fixture: true` and these named configurations:

- [experiment-paarc-on.json](../../files/config/experiment-paarc-on.json)
- [experiment-paarc-off.json](../../files/config/experiment-paarc-off.json)

Set the example's case paths to their absolute locations. Configuration files may
live in the checkout; the specification and generated artifacts must be private
and outside Git. Do not put credential contents in any JSON configuration.

```bash
# Run from the reviewed checkout using the existing Polars environment.
python bin/flowdc_experiment.py prepare --spec /absolute/private/experiment.json
```

This command is also the reproducible controlled-fixture preparation command. It
generates 64 deterministic 128×128 PNGs, two 32-row Parquet partitions for each
PAARC mode, and expected SHA-256 values. URLs use the enrolled origin's private IP
and port 8000. The origin injects one 503 followed by success at `0.png` per case.
All generated bytes remain outside Git. The TaskVine manager port is 9123.
At 2048 accepted fixture requests, the next valid request receives HTTP 429 and one refusal record; the production origin
then exits without calling synchronous shutdown from its serving thread.

Preparation reads only the three maintained source entrypoints from committed
Git objects, never working-tree source edits. The fixed guest helper is separately
hashed from the running reviewed runner. No Git metadata is copied. Retain the
returned `exp-<32 hex digits>` run ID; each prepare creates a different directory.
Interrupted preparation preserves a partial directory which cannot execute.

For existing datasets replace `fixture` with a `partitions` list:

```json
"partitions": [
  {"path": "/absolute/private/input-a.parquet", "rows": 32},
  {"path": "/absolute/private/input-b.parquet", "rows": 32}
]
```

Each partition must contain unique, non-null HTTP(S) URLs in `url` or the configured
`url_col`. Polars validates the actual row count. The runner canonicalizes filenames
to `part-000.parquet`, etc., preserving original input bytes and their hashes.
Optionally add `expected_sha256`, a list containing one image-content SHA-256 per
row, compared as a multiset against downloaded images. Without it, validation
records observed hashes and verifies task/overview/count consistency; it does not
prove expected image content. Fixture hashes are mandatory and generated locally.

Case names are unique lowercase identifiers (up to 40 characters; `origin` and the `-worker` suffix are
reserved). Configurations require Boolean `enable_paarc`; they accept bounded
current concurrency, retry, PAARC and URL-column settings. Paths, shell fragments,
TaskVine resources, output format/naming, ports and legacy PolicyBBR options are
rejected. This intentionally bounded subset requires float settings greater than
zero and at most 120 (ratios at most 1); zero backoff/cooldown and longer request
timeouts are not admitted. Missing concurrency values use the TaskVine wrapper
defaults C_min=2, C_init=8, C_max=2000; explicit values must satisfy their ordering.
Outputs are compressed image-folder archives with overviews. TaskVine
resubmission is disabled; downloader retries remain configurable.

## Execute, inspect, collect and cancel

Set `STATE` to the existing pilot state root and `RUN` to the prepared ID.
Only `run` requests activation, after checking immutable supervisor identity,
unit provenance, persistent local service, registration, host trust, ownership,
idle state and allowance. Expired idle observations trigger bounded read-only
provider verification followed by the installed pilot's supported `reconcile`
request to refresh cached observations. An unrelated active/incomplete operation
is refused; no history or account fields are reset.

```bash
STATE=/absolute/existing/flowdc-state
RUN=exp-0123456789abcdef0123456789abcdef  # Replace with the complete returned run ID.

# Requires separate operational activation authorization.
python bin/flowdc_experiment.py run --state-root "$STATE" --run-id "$RUN"

# Observational; usable while run is active.
python bin/flowdc_experiment.py status --state-root "$STATE" --run-id "$RUN"

# Cancel the active run, or retry cleanup after its process exits.
python bin/flowdc_experiment.py stop --state-root "$STATE" --run-id "$RUN"

# Revalidate collected outputs or retry missing snapshots; never starts tasks/VMs.
python bin/flowdc_experiment.py collect --state-root "$STATE" --run-id "$RUN"
```

`run` cannot replay a started, failed, cancelled or completed workload. Prepare a
new experiment for a new attempt. A registration lock and durable ownership record
exclude other runs until both cloud and guest cleanup are verified. A separate
`stop` process writes a cancellation request bound to this run and manifest;
the active process checks it during subprocess waits and enters bounded cleanup.
It never signals a recorded PID that could have been reused. The stop client waits
at most 30 seconds; an `incomplete` response means inspect status and repeat stop
once the executor exits. Cancelling an unstarted run makes it non-executable.

Do not manually start the pilot or change enrollment/services while an experiment
owns the registration. This lock coordinates experiment commands; it cannot lock
out separately invoked administrative tools or a human operator.

## Budgets and honest outcomes

Defaults are a 1,800-second controller window, 900-second explicit-stop target,
60 seconds reserved for collection and up to 300 seconds per deployment/case
phase. After measured startup, admission reserves at least 30 seconds for deployment
and 30 seconds per case. After deployment, each case receives the smaller of the
phase maximum and its share of remaining work time. The second case is re-admitted
against actual remaining time, controller consumption, 600-second cleanup reserve
and 180-second lead. There is no fixed 480-second cutoff and no requirement to
reserve a fresh 300 seconds for every case. These bounds can still refuse a slow
startup/workload; they never extend allowance. Configure `min_case_seconds` and
`deployment_min_seconds` when your workload needs more admission time.

Every SSH/child process has a deadline and output cap, with process-group cleanup.
Artifact decompression and hashing run in a bounded child. Collection cannot consume
the separate `stop_seconds` budget (default 300). On normal completion, named guest
services are stopped/verified before final collection. On failure or lost start
acknowledgement, the runner attempts cloud stop before cleanup bookkeeping or guest
operations, within the original stop deadline. Scheduling stalls or process loss can
still prevent dispatch; an expired deadline is never extended. Half the remaining configured stop
window is shared among outstanding guest services; the rest is reserved for fresh cloud-cleanup
evidence. Already stopped services do not consume that budget.
The controller's fixed 600-second reserve and 180-second action lead protect its
independent shutdown/accounting boundary. `stop_seconds` is a separate client
cleanup/evidence-wait limit (1–1800 seconds); changing it never changes that reserve.
A short client limit may return incomplete while supervision continues.
SIGINT/SIGTERM retain evidence and enter cleanup; SIGKILL, power loss, disconnection
or provider outage can prevent local follow-through. The unchanged independent
pilot supervisor remains authoritative for cloud obligations.

CLI JSON uses schema version 1. Exit 0 from `run` requires workload pass, verified
cloud cleanup, verified guest cleanup, and no retained errors. Exit 3 is incomplete;
exit 2 is a refused operation. `status` exit 0 means the local record was read,
not that the experiment passed. `stop` exit 0 means cleanup verified, not workload
success. `workload` describes the latest artifact-validation outcome and can become
`passed` after collection recovery. Earlier execution/collection errors remain in
`errors`, so recovery does not turn a failed run into a successful run/collect exit.
Cloud success requires fresh selected-VM `SHELVED_OFFLOADED` observations, settled
accounts without uncertainty, idle healthy supervision and verified network rollback.
A stop acknowledgement, cached observation or workload pass alone is insufficient.

## Artifact layout and recovery

Everything below `STATE/runs` is owner-private (directories 0700, files 0600):

```text
runs/experiment.lock                 # serializes experiment operations
runs/active-experiment.json          # durable unfinished owner
runs/exp-.../original-spec.json
runs/exp-.../original-case-CASE.json
runs/exp-.../manifest.json            # v1 source/input/config/helper hashes and binding
runs/exp-.../staged-NNNN.bin          # immutable bytes indexed by manifest
runs/exp-.../guest.py
runs/exp-.../bundle.tar
runs/exp-.../state.json               # v1 phases, intent, outcomes, errors, snapshot pointers
runs/exp-.../cancel.json              # when explicitly cancelled
runs/exp-.../environment-ROLE.json
runs/exp-.../artifacts-CASE-UUID.tar   # manager archives, logs and resolved task configs
runs/exp-.../artifacts-CASE-worker-UUID.tar
runs/exp-.../artifacts-origin-UUID.tar
runs/exp-.../validation-CASE.json     # observed hashes, expected verification, overviews
```

Supplied partition reads are limited by the remaining aggregate staging budget,
including one staged copy per case and the source/configuration/helper payloads.
Cases sharing a URL column reuse its validation. These are staged-byte bounds,
not a bound on Parquet decompression or total process RSS.

Preparation bounds source to 4 MiB, staging to 64 MiB, cases to 8 and partitions to
32 (up to 4093 rows each: the 4096-member nested archive limit reserves the
root directory, image subdirectory and overview). Unknown image sizes cannot be
predicted from row counts; even an admitted partition can exceed the configured byte cap and fail
collection. Repartition and choose the finite byte budget before activation.
The private specification is capped at 256 KiB.
Collection bounds each snapshot and decompressed archive to `output_bytes`
(default 256 MiB, maximum 1 GiB), 4096 archive members (guest enumeration also stops
at 4096 entries and 32 directory levels), and a 64 MiB validation response. Guest service file-size limits are per file; configured worker disk is
TaskVine's advertised task budget, not a filesystem quota. Oversized evidence fails
validation. No unsafe archive links, special files or traversal paths are extracted.
Trusted selected code and Parquet inputs are not treated as sandboxed executables.

Partial collections use new snapshot names. A process loss between writing a
snapshot and recording it can leave an unreferenced private snapshot; it is retained,
never overwritten. Successful snapshots are hash-checked and reused on repeated
collection. Fully collected evidence can be revalidated locally even after guests
are offloaded. Missing remote files can only be recovered while guests remain
reachable; collection does not reactivate them or rerun downloads.

For `cleanup_incomplete`, retain all records and run the same `stop` command.
Guest service locks, launch acknowledgements and cancellation markers prevent a
late launch from bypassing a preceding stop. An interrupted, unacknowledged guest
launch remains uncertain rather than treating a momentarily absent unit as proof.
If guest cleanup remains uncertain after cloud offload, ownership deliberately
remains pending. Do not edit `state.json`, erase the ownership record, create a
replacement pilot root, or activate solely to make validation green. Preserve the
private service names in `state.json` and seek separately authorized operator
recovery to verify those exact guest services. Lost floating routes, host trust
changes, corrupt manifests or unavailable guests may require that manual checkpoint.
Use the installed pilot's existing stop/reconcile and emergency procedure to settle
cloud obligations regardless of experiment-record damage. No experiment command
claims it can prove unreachable guest state.

Retain guest run directories and private logs for inspection; the runner stops its
named services but does not delete guest evidence. Rollback is ceasing use of this
additive CLI while preserving records and the pilot's existing cleanup/accounting.

## Software evidence

Focused tests exercise actual CLI preparation, Git objects, private serialization,
Parquet, archive transfers and validation, the installed controller status subprocess,
provider external-I/O boundaries, cancellation with two CLI processes, safe recovery
and process limits. Infrastructure fakes do not establish live SSH/systemd/TaskVine
execution. `tests/test_flowdc_experiment_guest.py` additionally runs the maintained
downloader against the maintained synthetic HTTP origin, both PAARC modes, expected
128 image hashes and two injected retries. A host permitting local sockets is needed.
