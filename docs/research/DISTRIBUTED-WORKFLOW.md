# Distributed engineering profile and offline topology

Issue #26 adds the explicit `shared-origin-v1` profile to `bin/TaskvineFLOWDC.py`.
Historical configurations continue through the legacy route. The separate
`TaskvineFLOWDCCloud.py` is unsupported by this profile. These are engineering tools;
local correctness does not establish scaling, efficacy, cloud readiness or a frozen
scientific protocol. Advisor decisions and confirmatory/live checkpoints remain open.

## Native execution

Pass a JSON config containing `distributed_profile`, `original_manifest`, `catalog`,
`output_directory`, `environment_archive`, `environment_sha256`, `workers` (1/2/4),
and `download` with an explicit `control_method`. The maintained local CLI additionally
requires `local_worker_binary`, an absolute path to the official matching worker;
it owns every worker process and records the binary hash and cleanup in a new
`<output_directory>-workers` sibling. Use the prepared guest bridge for remote workers.
Arbitrary external workers/factories are not a supported bounded research cohort.
Methods are `paarc-base-v2`,
`gradient-candidate-v1`, `fixed-v1`, `ratio-v1` and `gradient2-application-delay-v1`; their equations and provisional
parameters are in [CONTROL-METHODS.md](CONTROL-METHODS.md). Original-byte truth and
safe metadata restrictions follow [benchmark-contract.md](benchmark-contract.md).
Output collisions fail before HTTP. Original invalid rows remain in the denominator.

Use matching official **TaskVine 7.17.2** manager, workers and portable environment.
The profile checks the archive SHA256 and workers check source/module closure and
that runtime dependencies resolve inside the staged prefix. TaskVine owns staging
and dispatch; no substitute executor is used. The native C manager runs in a private
child because its blocking SWIG wait can starve Python admission even in a thread.
The shared authority stays in the parent's event loop.

Returned uncompressed envelopes contain identity, receipt, native archives, outcomes,
attempt/control journals and stdout/stderr. A bounded independent extractor rejects
links, path traversal, duplicates, conflicting names, truncation, unknown content
and hash/identity mismatches. Native hardlinked publication files are packaged as
regular bytes. Logical row identity survives partitioning; each actual worker entry
has a fresh attempt UUID. Repeated returns cannot earn repeated useful credit.
Nonzero native exit, incomplete rows and rejected artifacts remain separate failures.
Manager receipt and final verification are included in monotonic end-to-end timing.

`max_attempts` defaults to 1, accepts 1..4 and bounds admitted acquisition clients per
partition. Native dispatch has a separate owned-cohort contract: each partition has
a unique feature, and only a finite number K of single-shot worker launches can use
that feature. Local launch intent spends its budget before process creation; a failed
spawn cannot refund it. Replacement requires fresh process-tree quiescence, not just
root exit. Prepared guest services use one exclusive launch intent and `Restart=no`.
Features are eligibility labels, not credentials or a security boundary.

In pinned 7.17.2, `try_count` increments on dispatch and survives worker-loss cleanup.
The profile explicitly disables default/category fast-abort, uses fixed allocation,
`max_forsaken=0`, and native `retries=1`. Resource/sandbox exhaustion can therefore
add at most one dispatch; loss can spend at most K worker connections. The conservative
per-partition ceiling is **K+1**, conditional on the owned single-shot cohort and
exclusive native endpoint containment. Normal local/guest K=1; the local worker-loss
fixture allows K=2 for slot 0 only. Actual RUNNING transactions are retained and
independently checked against this ceiling. Post-run counting supplements the launch
enforcement; it is not itself enforcement. Native dispatch, admitted client and HTTP
attempt counts remain distinct. No absent native metric is replaced by a zero.

`acquisition_complete` describes verified rows and receipts. Overall `run_complete`
also requires closed clients, no uncertain/outstanding permits, successful native
manager shutdown and the native dispatch audit. Verified partial bytes remain creditable
when one of these closure conditions fails. Guest collection rechecks those conditions.

Run a small real fixture in a task-local pinned environment:

```bash
NO_ALBUMENTATIONS_UPDATE=1 WANDB_MODE=disabled .agentic-local/research-env-7.17.2/bin/python -B benchmark/taskvine_local.py --workers 4 --method fixed-v1 --case primary --environment /absolute/path/research-env.tar.gz --output benchmark/results/vine-four-001
```

Use a fresh output directory for every invocation. Cases are `primary`, `worker-loss`,
`preconnect-loss`, `manager-stop`, `timeout`, `partial-artifact`, `sandbox-exhaustion`
and `forsaken`. They use 35 original rows (32 valid),
not an external dataset. The manager deadline is 170 seconds; cleanup has a separate
bounded reserve. Failure cases require their specific receipts, not just any failed
run. Linux pidfds pin signal targets; only tracked worker descendants are reaped.
Unattributed adopted children prevent a worker-quiescence claim. Normal native
shutdown gets a bounded grace period before escalation. Owner-only process
quiescence proof closes a client and releases only permits
that were never dispatched. A cancelled, timed-out or truncated dispatched response
retains its origin capacity even after client exit: disconnect does not prove the
origin finished its work. Redirect and HTTP error bodies are drained to EOF within
the existing request timeout before their permits are released. The controlled
worker-loss fixture additionally records independent origin drain evidence after
process-tree exit; only this owner-side evidence can settle its uncertain requests.
Production clients and heartbeat expiry cannot submit an origin drain proof.

## Prepared guest bridge

Experiment specification schema 2 adds `distributed` with `manifest`, `catalog`,
`origin_plan`, `environment_archive`, `environment_sha256`, and `control_tls`.
It requires the controlled fixture mode and explicit methods in case configs.
Top-level `worker_ids` optionally selects 1/2/4 enrolled worker UUIDs; omission
selects all enrolled workers. Preparation retains the selection separately from
all immutable accounts, including when the chosen worker is `worker-3` alone.
The first three paths refer to local preparation inputs; environment/certificate
paths refer to existing guest files. Preparation reads committed source and rejects
older source without this profile. Every independent case gets a new concurrent
origin/service log, manager session and returned evidence. The collected verifier
binds original truth, worker source hashes, method and environment to preparation.
Per-case unique owned-cohort contracts also bind worker launch receipts and native
transaction counts. Each worker service can launch once, remains single-shot and
does not restart; a replacement requires a separately prepared run. Collection keeps
real native transaction directories while excluding the runtime cache, authority
private state and TaskVine's `most-recent` convenience symlink. Those runtime files
remain on the guest; other public symlinks still fail collection.
It independently requires every eligible row to verify and every returned task to
have a successful zero exit and returned receipt; an empty forged “complete” claim
cannot pass. Controlled guest origin plans specify schema/name/schedule/queue_bound/
assignments/objects. Queue rejection defaults to status 503 and Retry-After 0.1s;
explicit rejection status must be 429/503 and the delay must be finite, 0..5s.

`control_tls` requires `host`, `port`, `endpoint`, `certfile`, `keyfile`, `ca_file`,
`ca_sha256`. Host/endpoint map to the registered manager's private IPv4; the server
certificate must cover that IP. Only the pinned public CA certificate enters worker
descriptors. TLS verifies both chain and hostname; no trust-store modification or
verification bypass is implemented. Run-private bearer credentials are owner-private,
excluded from public configs/logs and collected artifacts. Server and CA private keys
are never staged to workers. Local fixtures keep authority/origin on literal loopback.

Native TaskVine listeners bind all interfaces. Native TLS/password is **not** an
independent worker identity boundary. Before the future guest launch, a read-only
provider check verifies every selected VM has one registered NIC, only its owned
security group, exact selected-peer IPv4 ingress and manager SSH ingress. Additional
interfaces/rules or changed bindings fail closed. Provider checks cannot prove guest
isolation from other users: exclusive trusted guests are a separate human checkpoint.
No firewall, SSH trust, production environment or VM is changed by local validation.

## Evidence and remaining gates

`test_vine_contract.py`, `test_vine_lifecycle.py`, `test_research_guest.py` and
`test_pilot_topology.py` provide deterministic/offline checks. They do not run a real
TaskVine cluster or TLS endpoint. Retained coordinator development runs established
original-byte acquisition with 1/2/4 real workers and controller selection, plus
worker-loss/timeout/manager-stop/partial-artifact behavior. They are development
sources, not final-head evidence. Final native cleanup/TLS/adversarial admission,
full product/service gates, both CI jobs and independent review remain required.

[Example configuration](../../files/config/taskvine_shared_origin.json) is a template:
replace its paths and hash with verified local inputs before use. It intentionally
fails validation with the placeholder hash and authorizes no acquisition by itself.

The unpatched pinned 7.17.2 native manager has a retained upstream limitation: a worker's
first `FORSAKEN` task reaches `exit_debug_message` with zero completed tasks,
causing integer division by zero and SIGFPE. The stage-in-conflict fixture records
one dispatch and the `RETRIEVED FORSAKEN` transaction, then requires failed/incomplete
outcomes, EOFError, native exit -8, no fabricated task receipt, and owned-worker
cleanup. Its engineering assertion can pass while acquisition remains failed.
The raw failed run and transactions remain available. Issue40's research runtime
builder applies the disclosed one-line zero-completion guard from upstream commit
`73ead49d394416eb9ff80a2371e7263474135eef` to pinned source
`ce1360061996e547ea14e22a00bc6042a42a13ce`. The private local qualification repeats
the crash fixture and ordinary 1/2/4-worker executions against that patched runtime;
it does not establish cloud execution or every recovery path. Manager and worker
runtimes must both use the qualified build. No automatic retry cohort or invented
successful healthy-partition return is assumed after a process crash.

`benchmark/package_environment.py` explicitly overlays the installed worker and
Python native binding and verifies their archive hashes. Managed conda records
can otherwise select the original package-cache binaries and silently discard a
local patch. The inventory describes installed files, rather than that cache.
Make the native ELF paths relocatable before packaging, then extract the archive
into a fresh prefix and repeat ordinary execution and the forsaken-task API
check there. A passing manager check outside the archive does not qualify the
packaged manager. The retained initial archive failed this relocated crash check;
it remains historical and is not a qualified patched deployment artifact.
