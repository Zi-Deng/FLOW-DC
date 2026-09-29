# Offline 3/4/6-VM topology and accounting

The versioned topology supports one manager, one origin, and 1/2/4 workers. Schema-1
three-role specifications retain their old interpretation. Schema-2 specifications
include `topology` binding manager/origin and an ordered worker UUID list; roles are
`manager`, `worker`, `worker-2`, `worker-3`, `worker-4`, `origin` as applicable.
Accounts remain keyed by immutable VM UUID, never a mutable worker index.

These shape examples deliberately contain placeholders and are not runnable live
specifications. Fill them from verified existing allocation records, not guessed IDs:

| VMs | `topology.workers` | Required VM roles |
| --- | --- | --- |
| 3 | `["<existing-worker-uuid>"]` | manager, worker, origin |
| 4 | `["<existing-worker-uuid>", "<new-worker-2-uuid>"]` | manager, worker, worker-2, origin |
| 6 | `["<existing-worker-uuid>", "<new-worker-2-uuid>", "<new-worker-3-uuid>", "<new-worker-4-uuid>"]` | manager, worker, worker-2, worker-3, worker-4, origin |

Keep the existing context and every existing VM/access entry exactly unchanged.
Access schema 2 has one explicit registered interface per selected role. The first
worker retains its legacy role name. Full specification validation is shared with
existing offline operations planning; the experiment runner stages, probes, starts,
stops and collects every selected worker with a distinct service identity.

## Migration request and preview

A request contains schema_version=1, migration_id, registration_id,
expected_binding_sha256, expected_state_sha256, expected_source_sha256, complete
new `spec`/`access`, and an exact `new_worker_ids` list. Expected source hashes the
entire installed supervisor module closure. Preview is read-only:

```bash
python -B bin/flowdc_ops.py pilot topology-preview --state-root /absolute/fixture/state --request /absolute/fixture/request.json
```

Use only fixture journals during this PR. `topology-apply` exists for the separately
approved future manual step; no real journal application or registration is authorized
here. Preview explicitly reports `activation_ready: false`. It refuses active or
uncertain obligations, incomplete old history, stale expectations, account/role
reassignment, removals and conflicting replays. Existing limits, consumption,
history, grants and obligations are preserved. New worker accounts require the exact
explicit request; no grant is added to an existing account. The lifetime ceiling
remains **7,200 seconds per VM**. The existing grant format retains its three-role
interpretation; this increment does not authorize multiworker grants.

Apply holds the supervisor lock and one SQLite transaction. It writes, fsyncs and
re-reads an exclusive owner-private backup of the old record before publishing the
new record and SQLite user_version=3 together. Schema 2 remains reserved for the old
upgrade fence; topology-aware maintenance uses fence 4. Header/body disagreement
fails closed. Crash-cut fixtures cover backup, precommit and postcommit boundaries;
replay returns the durable receipt without resetting later consumption.

## Backup and rollback rule

Before commit, the verified old record remains authoritative. After commit, preserve
both the new journal and backup; old code refuses user_version=3. Never copy the
backup over a journal that could contain newer consumption/obligations. There is no
automatic downgrade. A later rollback requires an independently verified offline
reconciliation preserving all accounts, receipts and postmigration history. Source
reversion alone must leave versioned outputs and journals intact.

## Required human checkpoint before production work

Record fresh allocation/project/region, exact selected UUIDs/NICs, SSH fingerprints,
source commit/module hashes, Python/native versions and portable archive hash; current
per-UUID consumption and remaining allowance; migration preview/request/backup hash;
verified idle/offloaded state and absent uncertain obligations; precise installation
and trust enrollment changes; TLS endpoint/CA/server certificate validation and native
network containment. Do not treat the historical 8,385-SU balance or October 17, 2026
expiry as fresh readiness.

Calculate conservative startup, service, collection, stop and cleanup time for every
selected VM before requesting a bounded run. Preserve the current stop/window caps,
cleanup reserves and allowlists. Six VMs require more network inspection/setup steps
than three; an insufficient existing window must fail, not expand automatically.
Cleanup must confirm **SHELVED_OFFLOADED for each UUID**, retain failed collection/stop
records and restore only owned networking. SHUTOFF or SHELVED is insufficient.

Actual installation, migration, new VM registration, trust changes, grants and live
activation each remain outside this PR's execution. Mock-provider tests exercise all
selected VMs, partial unshelve/lost replies, network restoration, migration crash replay
and old-code refusal. Green fixtures are not a production rollout approval.

Current limitation: enrollment and per-run selection are still coupled. Migration
can append workers while preserving all old accounts, but a single six-VM journal
does not yet select a smaller 1/2-worker subset for a subsequent run. Separate
selection must retain every enrolled account/history and avoid unshelving unselected
VMs; the 4→1→2-worker sequence on one journal remains an acceptance gap. Do not
create replacement journals to bypass this limitation or reset accounting.
