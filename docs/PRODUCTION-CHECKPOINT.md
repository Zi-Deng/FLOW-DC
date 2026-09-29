# Human checkpoint packet for later production work

Issue #26 delivers local engineering tools. This is a fill-in packet for later
production work; its live checks and privileged changes were not executed in this PR.
Reuse existing authorization and verified setup. Keep owner-private records private; publish only sanitized hashes,
commands, outcomes and relevant failures. Never include credentials or private keys.

## 1. Fresh facts and exact proposed scope

Record each field with an evidence path/hash, observation time and responsible human:

| Field | Required evidence / value to supply |
| --- | --- |
| Allocation | Project UUID, region, current balance, current expiry, official source and UTC observation time; the historical 8,385 SU / October 17, 2026 input is not current evidence |
| Registry | Existing registration ID, state schema, state/binding hashes, source module digest and installed service identity |
| Every enrolled VM | Immutable UUID, role, flavor, per-UUID limit/consumed/remaining seconds, grant/history receipts, observed SHELVED_OFFLOADED, no active/uncertain obligations |
| This selection | Exactly manager + origin + 1/2/4 enrolled worker UUIDs; separate selected IDs from the full registry; exact newly requested worker registrations, if any |
| Network | One verified NIC per selected VM, port/network/subnet IDs, fixed IP, operator /32, route evidence and selected-peer rules; no inferred UUIDs |
| SSH trust | Out-of-band fingerprint evidence for each UUID/host, exact known_hosts additions if needed, current known_hosts hash; never learn-and-accept a key automatically |
| Control trust | Verified manager endpoint/IP, public CA hash, server certificate SAN/chain/expiry, owner-private key location; no verification bypass or global trust-store edits |
| Native containment | Exact selected-peer ingress check, no extra NIC/rules, exclusive trusted guests/users; TaskVine SSL/password and eligibility features alone do not attest worker identity |
| Release | Reviewed commit, full supervisor/source closure hashes, Python and TaskVine 7.17.2 binary versions/hashes, portable environment hash and relocation check, effective config/manifest/catalog hashes |
| Permissions | Exact installation/trust/network changes, paths and owners; approval must cover each concrete change separately from running a workload |

Use existing authenticated allocation records and operator inventory exports to
prepare this packet. Fresh read-only validation uses the authorization already in
place; missing privileged installation, enrollment, trust, migration or activation
requires its concrete scoped approval. Do not guess missing IDs, attest a synthetic
route or treat an expired snapshot as current. The offline planner accepts existing
files and performs no discovery.

### Ordered manual and agent steps

Guest console/SSH checks below require an already-running guest. A
SHELVED_OFFLOADED VM cannot execute them. Reuse adequate retained trusted facts;
if facts/setup require booting a VM, first prepare a bounded maintenance activation
packet with exact UUIDs, existing allowance, window, proposed guest changes and
offload cleanup, and obtain its applicable authorization. Activation cost is not
waived because the eventual guest commands are read-only. Alternatively, an already
authorized bounded live packet may explicitly include prerequisite checks with
abort/cleanup on failure. Neither alternative is executed by this PR.

1. **Human, ACCESS/allocation browser:** open the existing allocation account page,
   complete any required MFA, and record the allocation identifier, current SU,
   expiry and UTC observation time. Supply those values and a sanitized evidence
   reference, never the session cookie or login secret. Existing authenticated
   access need not be reenrolled.
2. **Agent, local workstation:** inspect the existing registered profile and retained
   inventory under current authorization. Compare project/region, all enrolled UUIDs,
   observed offload states, flavors, NICs and current accounting with the proposed
   selection. Identify exactly which worker records are missing. Supply a concrete
   proposed spec/access diff, hashes and offline sizing; do not replace the user's
   profile, credential file or journal with generated examples.
3. **Human, guest console only where trust is missing:** use the provider's trusted
   console for the exact UUID and read the public host-key fingerprint, for example
   `ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub`. Give the public fingerprint and
   its UUID mapping to the agent. **Agent, workstation:** compare with the existing
   known_hosts record. Reuse matching verified trust; if an addition or key change
   is needed, present the exact file diff for the applicable trust approval before
   enrollment. An unverified network key scan is not out-of-band verification.
4. **Agent/operator, already trusted guest session:** collect read-only version and
   binary hashes from the intended environment (`PYTHON --version`,
   `VINE_WORKER --version`, `sha256sum PYTHON VINE_WORKER ENV_ARCHIVE`). Paths stand
   for the exact reviewed absolute files. Compare the runtime, archive and relocation
   evidence with the local release. Missing packages require a concrete guest
   installation plan; local research setup is not authorization to edit a guest.
5. **Agent/operator, manager guest and workstation:** inspect the existing public CA
   and server certificate. Record `sha256sum PUBLIC_CA` and
   `openssl x509 -in SERVER_CERT -noout -dates -ext subjectAltName`; verify the chain
   and selected manager IP with
   `openssl verify -CAfile PUBLIC_CA -verify_ip MANAGER_IP SERVER_CERT`. Keep the
   private key on its owner-private manager path. Present missing certificate or
   endpoint setup as concrete changes; reuse valid existing material. Independently
   verify selected-peer network containment and exclusive trusted guest ownership.
6. **Agent, workstation:** finish the exact installation/migration packet below,
   including command arguments, expected state/source hashes, backup and rollback
   rules. The human approves its effects. After those operations and fresh validation,
   prepare a separate bounded live-run packet; installation approval alone never
   starts a workload. If all manual facts already exist, steps 1–5 are verification,
   not a demand to repeat setup or approval.

## 2. Sizing and installation/migration preview

Run `benchmark/topology_plan.py plan` against the proposed specification to retain
the selection, per-VM required window, conditional SU, stop lead and cleanup reserve.
Its `activation_ready` is always false: configured lifetime limits are not current
remaining allowance. Independently compare each selected account's **existing**
remaining seconds with the complete window. Add no grants. Keep the 7,200-second
lifetime ceiling, the experiment's 1,800-second window cap, and its stop target caps.

For N=3/4/6 selected VMs, stop lead is 180/220/300 seconds and cleanup reserve is
600 seconds. A 1,800-second window therefore permits stop-after at most 1,020/980/900
seconds. Budget startup, staging, each bounded case and collection inside that
stop-after interval; reserve actual stop/offload work separately. Report each phase
bound and the sum. Conditional cost is `sum(rate_i * window_i / 3600)` with exact
rate/flavor evidence. Missing rates produce null, not zero. Provider outages can
exceed this scheduling bound, so retain any uncertain billing exposure.

The production packet must contain the exact installed and candidate digests,
absolute reviewed candidate source path, absolute state root, proposed request
JSON/hash and a read-only migration preview. The request binds registration,
source/state/binding hashes, complete unchanged old accounts/spec/access and the
exact additional worker IDs. Populate it from current durable records; never build
it from the synthetic examples. Retain backup location, expected old record hash,
receipt ID and the post-application reconciliation procedure.

After the human has approved these concrete operations, the corresponding commands
are filled with those reviewed absolute paths and hashes:

```text
python -B bin/flowdc_ops.py pilot upgrade-supervisor --state-root STATE --expected-current-digest CURRENT --expected-candidate-digest CANDIDATE --candidate-source REVIEWED_SOURCE
python -B bin/flowdc_ops.py pilot topology-preview --state-root STATE --request REQUEST
python -B bin/flowdc_ops.py pilot topology-apply --state-root STATE --request REQUEST
python -B bin/flowdc_ops.py pilot status --state-root STATE
```

For an existing registration, use its preserved state root; never call fresh
`prepare --install-supervisor` to replace it. Review the candidate upgrade preflight,
maintenance fence, service restart and heartbeat evidence before applying topology.
If an already-approved upgrade changes only the expected state hash, regenerate
the preview/request from the current record and verify the reviewed effects are
identical. This is routine validation; seek a new decision only for a material
changed effect, scope or failed precondition.
Migration adds only explicitly requested UUID accounts; it cannot reset old
consumption or create an allowance grant. Failed/uncertain state blocks the sequence.

Before application, the old record remains authoritative. After application,
preserve the verified backup **and** new journal; old code must refuse the new schema.
Never overwrite newer consumption/history with the backup. There is no automatic
topology downgrade. Upgrade rollback and topology rollback are distinct: any later
topology rollback needs a separately reviewed reconciliation preserving every
post-migration account, receipt and obligation.

## 3. Separate bounded live-run approval

After installation/migration/trust checks are complete, present a fresh run packet:
selected UUIDs, source/env/config/input hashes, method/version, row/byte/attempt
bounds, exact prepared run ID, window and per-phase deadlines, current remaining
allowance, conditional SU, stop command and evidence destination. The reviewed
`flowdc_experiment.py prepare --spec SPEC` manifest must bind the same selection.
The human approves this particular run; approval of software or a migration does
not activate it. Keep each engineering invocation within four workers, 256 rows,
64 MiB expected payload, 180 seconds process time and 60 seconds process cleanup.

Record start/stop intent and lost acknowledgements. Retain stdout/stderr, raw
archives, origin/control/native transactions, failed collection/stop records and
per-UUID accounting. Require actual **SHELVED_OFFLOADED** for every cleanup
obligation and restoration of owned networking. SHUTOFF/SHELVED is insufficient.
An unresolved worker, permit, provider observation or collection failure remains
explicitly incomplete; do not authorize a replacement by assuming remote work ended.

## 4. Scientific and merge checkpoints

The maintainer reports advisor approval; particular numerical decisions and their
provenance are still pending. Software delivery of steps 2–5 does not freeze the
scientific protocol. Later step 6 is the approved frozen manuscript campaign with
run-level replicates. Step 7 is final paper/artifact reproduction, coauthor approval
and submission. Neither has been performed here. The maintainer also owns merge;
green local checks and an independent model review are evidence for that decision.
