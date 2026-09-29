# Distributed engineering profile and offline topology

Issue #26 adds the explicit `shared-origin-v1` profile to `bin/TaskvineFLOWDC.py`.
Historical configurations continue through the legacy route. The separate
`TaskvineFLOWDCCloud.py` is unsupported by this profile. These are engineering tools;
local correctness does not establish scaling, efficacy, cloud readiness or a frozen
scientific protocol. Advisor decisions and confirmatory/live checkpoints remain open.

## Native execution

Pass a JSON config containing `distributed_profile`, `original_manifest`, `catalog`,
`output_directory`, `environment_archive`, `environment_sha256`, `workers` (1/2/4),
and `download` with an explicit `control_method`. Methods are `paarc-base-v2`,
`gradient-candidate-v1`, `fixed-v1`, and `ratio-v1`; their equations and provisional
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
partition. **An open limitation is native redispatch before acquisition enrollment:**
7.17.2's retry counter does not bound every worker-loss dispatch. Transaction logs
retain observed native dispatches, and manager/task deadlines remain bounded. This
limitation prevents claiming the complete retry acceptance criterion. No absent
native attempt metric is replaced by a fabricated zero.

Run a small real fixture in a task-local pinned environment:

```bash
NO_ALBUMENTATIONS_UPDATE=1 WANDB_MODE=disabled .agentic-local/research-env-7.17.2/bin/python -B benchmark/taskvine_local.py --workers 4 --method fixed-v1 --case primary --environment /absolute/path/research-env.tar.gz --output benchmark/results/vine-four-001
```

Use a fresh output directory for every invocation. Cases are `primary`, `worker-loss`,
`manager-stop`, `timeout`, and `partial-artifact`. They use 35 original rows (32 valid),
not an external dataset. The manager deadline is 170 seconds; cleanup has a separate
bounded reserve. Failure cases require their specific receipts, not just any failed
run. Linux pidfds pin signal targets; only tracked worker descendants are reaped.
Unattributed adopted children prevent a worker-quiescence claim. Normal native
shutdown gets a bounded grace period before escalation. Owner-only quiescence proof
can release uncertain permits after actual process exit; heartbeat expiry cannot.

## Prepared guest bridge

Experiment specification schema 2 adds `distributed` with `manifest`, `catalog`,
`origin_plan`, `environment_archive`, `environment_sha256`, and `control_tls`.
It requires the controlled fixture mode and explicit methods in case configs.
The first three paths refer to local preparation inputs; environment/certificate
paths refer to existing guest files. Preparation reads committed source and rejects
older source without this profile. Every independent case gets a new concurrent
origin/service log, manager session and returned evidence. The collected verifier
binds original truth, worker source hashes, method and environment to preparation.

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
