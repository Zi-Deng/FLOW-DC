# Offline 3/4/6-VM topology and accounting

The versioned topology supports one manager, one origin, and 1/2/4 workers. Schema-1
three-role specifications retain their old interpretation. Schema-2 specifications
include `topology` binding manager/origin and an ordered worker UUID list; roles are
`manager`, `worker`, `worker-2`, `worker-3`, `worker-4`, `origin` as applicable.
Accounts remain keyed by immutable VM UUID, never a mutable worker index.

Generate runnable **offline fixture inputs** for all three shapes and non-prefix
selection within a six-VM registry:

```bash
python -B benchmark/topology_plan.py examples --output benchmark/results/topology-examples-001
python -B benchmark/topology_plan.py plan --spec benchmark/results/topology-examples-001/6-vm-spec.json --window-seconds 1800 --output benchmark/results/topology-plan-001.json
```

The first command validates and writes 3/4/6-VM specifications, fixture access
records, sizing plans, and 1/2/4-worker selections. UUIDs are deterministic synthetic
values, the identity endpoint uses `.invalid`, and addresses use a documentation
network. The fixture route attestation is **not a verified live route**. Do not
register these inputs against a real journal. Neither command reads credentials,
opens an accounting journal, installs a service or contacts a provider.

For existing verified records, `plan --spec PATH --worker-id UUID` accepts repeated
worker IDs and retains their canonical selection. It refuses invalid counts,
duplicates and insufficient windows. Plans always report `activation_ready: false`,
missing remaining allowance as null, and missing rates as null rather than zero.
Configured limits cannot substitute for current per-UUID consumption. Use the
[production checkpoint packet](PRODUCTION-CHECKPOINT.md) to fill the missing facts
and describe the exact later installation, migration, trust and live-run steps.

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

## Per-run worker selection

Enrollment retains every UUID account. A schema-3 journal stores an optional
`selection` with schema_version=1 and `worker_ids` in registered-role order. Start
accepts exactly 1/2/4 distinct enrolled worker UUIDs, including non-prefix subsets.
Omitting a selection means all enrolled workers. For the later authorized run:

```bash
python -B bin/flowdc_ops.py pilot start --state-root /absolute/fixture/state --window-seconds 1200 --worker-id <enrolled-worker-3-uuid>
```

Repeat `--worker-id` for two or four workers. Schema-2 experiment specifications
accept the same UUID list as top-level `worker_ids`. Offline preparation binds this
selection into the retained manifest and guest addresses; start/status refuse a
changed selection during the run. The existing schema-1 interpretation is unchanged.

Only selected accounts receive activation intent and setup charges. Network setup,
SSH, native containment verification, staging and collection use selected roles.
An exhausted but offloaded unselected account neither funds nor blocks another
selection. Any enrolled account's uncertain/active obligation still blocks a new
start, and recovery/cleanup inspect the complete registry. Status retains all VM
accounts and reports `selected_ids`; it never hides off-selection obligations.
No account is removed or reset when switching 4→1→2 workers.

The stop scheduling lead is `max(180, 40*N+60)` seconds for N selected VMs, before
the unchanged 600-second cleanup reserve. The experiment stop bound uses the same
lead and existing window cap; it refuses insufficient bounds. This is conservative
scheduling slack, not a guarantee against provider outages.

`tests/test_pilot_topology.py` covers repeated 4→1→2 selection, exact unshelve sets,
unchanged unselected consumption, active/uncertain rejection and subset network
restoration. `tests/pilot_systemd_smoke.py --topology-subset` (also with `--sigterm`)
is an explicit fake-only real user-systemd gate: six fixed synthetic enrolled IDs,
one selected worker-3, exhausted unselected account, and all-account cleanup proof.
It never loads a live profile or operates the production service.
