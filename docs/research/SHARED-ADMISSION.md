# Shared origin admission: engineering contract

Issue #26 milestone D adds an explicit shared acquisition path. The manager owns
the only policy instance and aggregate issued-permit budget for each canonical
`(scheme, IDNA host, effective port)`. Local and legacy defaults remain unchanged.
The new `shared_control_file` downloader option names an owner-private descriptor
created by the manager. It contains a run-scoped credential; its contents must
never be placed in public configs, command arguments or logs.

The authority and HTTP hooks connect to the [maintained native TaskVine profile](DISTRIBUTED-WORKFLOW.md),
returned-artifact reconciliation and [offline UUID topology/accounting code](../BOUNDED-TOPOLOGY.md).
Real-client, native-worker and operational fixture gates remain separate evidence domains.

## Admission and observation

`flowdc_shared_state.py` stores the run binding, credentials' hashes, task scopes,
client attempts, permits, embargoes and ordered events in an owner-private SQLite
ledger. State and its event commit together using full synchronous transactions.
Permits occupy individually keyed rows with indexed outstanding-work queries;
completed history is retained without whole-history request-path serialization.
The storage version refuses earlier incompatible development ledgers, which must
remain retained as evidence. This does not migrate any operational account journal.
An exclusive owner lock prevents concurrent managers. Source/config/method hashes
and the run UUID bind every RPC. Credentials authorize a disjoint set of logical
row IDs and a positive bounded number of task attempts. Worker names are reported
labels bound at connection, not cryptographic attestations of a physical machine.

The downloader uses a separate authenticated control session. Immediately before
each origin header write, it obtains a uniquely identified permit and asks the
manager to confirm dispatch against the current epoch and aggregate Retry-After
embargo. Dispatch confirmation is the admission linearization point: work already
confirmed before a later embargo may be in flight. A repeated dispatch is refused;
an uncertain RPC acknowledgement never authorizes another origin write. Hidden
HTTP-library connection replay cannot reuse a dispatched permit.

Redirect responses close/release their transport and prior permit before any
destination or Retry-After wait. Same-origin redirects also require a new permit.
Invalid targets release the old permit; missing targets are accounted as final
responses. Reciprocal redirects cannot hold both origins at once. Completion
feedback belongs to the final hop, with the same complete-body latency eligibility
as the local methods. Local save failures do not erase valid HTTP observations.

Headers install Retry-After at the manager before the client continues. Duplicate
headers cannot extend the embargo again. A reordered completion carries those
same headers and installs their embargo atomically with capacity release. Conflicting
replays are refused. Request/completion records retain session, worker label,
task-attempt, logical-row, request and permit identities. Useful-byte credit still
requires the independent artifact verifier; controller observations do not grant it.

Base PAARC and the candidate/fixed/ratio methods use their existing policy classes
on the manager. The manager supplies aggregate outstanding work to the policy and
retains control trajectories. A decreased limit stops new permits until issued work
drains; previously issued work may transiently exceed the new target. Fixed limits
remain fixed while still obeying admission and Retry-After.

Client latency is a duration, not a timestamp subtracted from the manager's clock.
Fresh-window dispatch ages use the manager's recorded dispatch time. Control RPC
and instrumentation overhead are part of the acquisition path and must be included
in later calibration; this is not a transport RTT measurement or an efficacy claim.

Shared acquisition requires a finite positive `timeout` (default 30 seconds).
The existing total acquisition timer covers local Retry-After waiting, both remote
acquire/dispatch loops, connector waits, redirect hops and the body. An admission
timeout is recorded as a failed attempt without a latency sample or remote overload
claim; retries consume the existing finite `max_retry_attempts` budget. A timeout
does not settle previously dispatched uncertain work. Local legacy configurations
retain their existing zero/negative timeout meaning; shared configurations reject it
before loading input or handling output.

## Loss, restart and fencing

There are no expiring permits. An expired heartbeat marks a client uncertain and
prevents its new admissions, while its permits continue consuming aggregate
capacity. A manager/channel failure cancels the client's active acquisition tasks
and leaves uncertain work recorded. The worker never creates an independent full
capacity controller as fallback.

Heartbeats run every 0.5 seconds; a control RPC has one 3-second deadline including
its four-message client queue, transport and any backpressure retries. Five seconds
without a processed heartbeat fences that client. This is a conservative failure
detector, not a promise that every live client survives arbitrary disk, event-loop
or network stalls. It deliberately does not reopen an uncertain client in place.
Authenticated late completions and closure still drain acknowledged work; new work
requires the documented recovery/new-attempt path. A false suspicion may stop useful
work but cannot manufacture free origin capacity.

Reopening a ledger fences all new admission. Old authenticated completions and
closure acknowledgements may drain it. Owner recovery requires every permit and
client to have acknowledged closure, then increments the epoch and requires fresh
credential enrollment. Old credentials cannot join the new epoch. There is no
force-reset or timeout-based reclamation command. Unreachable clients can therefore
leave recovery blocked. Owned process-tree quiescence closes the client but releases
only never-dispatched permits. Dispatched incomplete responses remain uncertain;
only independent origin-drain evidence can settle them. The controlled worker-loss
fixture records both proofs. Heartbeat expiry and client disconnect prove neither.

Monotonic epochs are not reused across manager restarts. Restart conservatively
reapplies each origin's largest recorded Retry-After duration on its new clock and
preserves it through epoch recovery. This may over-wait. It cannot silently shorten
the known embargo. Policy confidence is recreated after recovery, not restored
from stale client samples.

## Authentication, bounds and staging

Private descriptors are regular current-owner files without group/other access;
symlinks are refused. The manager's directory is mode 0700. Exported manager
evidence omits credential hashes as well as raw credentials. The control client
does not follow endpoint redirects. Plain HTTP is allowed only for literal loopback
addresses, including the local end of an explicitly configured authenticated SSH
forward. Remote endpoints require verified HTTPS; certificate checking is never
disabled. No SSH, credential-store or firewall change is performed by this code.

RPC bodies/responses are bounded to 32 KiB; completion observations to 4 KiB. There
are at most 64 simultaneous server handlers, four concurrent RPCs per client,
64 enrolled scopes/clients, 256 original logical rows, 256 outstanding permits per
client, 32 active origins, 16,384 retained permits and 262,144 ledger events. A bound
or storage failure refuses further admission. Polling for capacity does not create
an unbounded manager-side queue. Heartbeat loss after five seconds does not free
capacity. The 256-row ceiling is currently a limit of this engineering API, not just
its fixture. This increment is not manuscript-scale acquisition. These bounds are
not advisor-selected parameters. Synchronous RPC/transaction overhead still needs
native calibration even after removing growing whole-permit serialization.

The manager's handler-limit 429 (`{"error":"backpressure"}`) is returned before
operation execution. The client retries only that complete refusal, at most three
times after 0.05/0.1/0.2-second waits, within the same 3-second RPC deadline. Retries
reuse the exact envelope and are retained in the control trace. Other responses,
including authentication refusal (403), storage/session failure (503), malformed
replies and lost acknowledgements, remain fatal and are not automatically replayed.
Exhausted backpressure stops the client conservatively. Control-channel 429 handling
is separate from the origin's mandatory Retry-After embargo.

`flowdc_staging.py` is the declarative acquisition-module closure used by maintained
TaskVine staging, source hashing and committed guest source packaging. Historical
commits may omit modules they do not reference. A missing dependency referenced by
a selected source commit fails closed. An isolated staged downloader CLI and shared
module import are tested; that check is not real TaskVine execution. The legacy
`TaskvineFLOWDCCloud.py` remains unsupported by this research path.

## Required local execution

The real-client entrypoint starts one authority and 1, 2 or 4 independent maintained
downloader processes against independently cataloged local JPEG/PNG objects:

```bash
NO_ALBUMENTATIONS_UPDATE=1 WANDB_MODE=disabled .agentic-local/research-env-7.17.2/bin/python -B benchmark/shared_origin.py --workers 4 --case primary --method fixed-v1 --output benchmark/results/shared-four-001
```

Use distinct output directories. Cases also include `reciprocal-redirect`,
`retry-after`, `worker-loss` and `manager-stop`; all four explicit methods are
selectable. Thirty-two logical rows are partitioned without changing their parent
identities. Per-task verification views retain the immutable full parent, hash,
original count and validated membership; partition scope/count are explicit.
the aggregate result uses the original 32-row denominator. Every native process
has a 120-second deadline and 20-second cleanup reserve, within the issue envelope.
Retained outputs include native archives/journals/trajectories, process ownership,
manager evidence, origin events/audits and exact verification results. Loss cases
must retain uncertain permits and failed/missing rows rather than fabricate success.
An independent single-epoch event replay checks issued limits, embargoes and
duplicate dispatches; it explicitly refuses to reinterpret clocks across restarts.
The origin also audits arrival-to-response outstanding requests, including queued
work, against the shared cap; its own service-slot limit cannot hide excess arrivals.
Worker-loss injection requires Linux pidfds. Where CPython omits its wrappers, an
explicit libc binding invokes the same kernel handle operations. Missing support
fails the fixture; it never falls back to signaling an unreserved numeric PID.

The managed executor cannot bind localhost sockets. Its ledger/HTTP-hook tests are
V1 evidence only. Real executions through this entrypoint remain a required
coordinator gate, and do not substitute for the separate real TaskVine 1/2/4-worker
gate. No live deployment, allowance change or production journal migration is
authorized by green local checks.


The additional explicit transport-fault entrypoints use the same research dependencies
(see [setup](../../benchmark/README.md)) and fresh owner-private output directories:

```bash
.agentic-local/research-env-7.17.2/bin/python -B benchmark/shared_faults.py --output benchmark/results/shared-faults-001
.agentic-local/research-env-7.17.2/bin/python -B benchmark/shared_restart.py --output benchmark/results/shared-restart-001
```

`shared_faults.py` aborts actual TCP replies after durable acquire/incomplete
completion, tests simultaneous permits, forged credentials/identity/row scope,
idempotent reconnect, duplicate/reordered completion and Retry-After, then reopens
a fenced ledger. It also occupies all 64 actual HTTP handlers to exercise 429
backpressure recovery and verifies an acquisition timeout behind a deliberately
uncertain permit without an origin request. `shared_restart.py` kills an actual child manager while an origin
request is held, opens the same ledger in a new manager process, verifies admission
is fenced, then acknowledges the actual completed response. It preserves the epoch
and never automatically recycles outstanding work. Child processes are owned and
reaped; the 180-second timeout has bounded cleanup. Failure records and private
journals remain retained. These tests do not claim host-reboot recovery or complete
TaskVine acquisition; ordinary dependency-light unit discovery does not execute them.
