# Bounded pilot lifecycle

The additive `pilot` commands manage exactly the manager, worker and origin in one
registered specification. Preparation is local and non-activating. `start` asks an
independent user-systemd service to prepare selected networking and unshelve the
three existing VMs. No command creates, deletes, resizes or rebuilds a VM, deploys
guests or downloads data. The original `js2` wrapper remains read-only.

**Implementation/review authorizes zero live cloud mutations.** The commands below
are the post-merge operational procedure, conditional on separate activation
authorization, verified allocation/access facts, a reviewed installed release and
an awake, powered, online operator workstation. Fake tests are software evidence;
they establish neither successful Jetstream2 mutation nor a hard billing cap.

## Prepare and install

Use the existing private profile, validated three-role pilot specification with
verified rates, and a complete inventory no more than **300 seconds old**. All
three VMs must be observed `SHELVED_OFFLOADED`. Limits must be greater than 600 and
no more than 7200 seconds. The service revalidates cloud context and initial states
before creating activation intentions; a snapshot alone never authorizes mutation.

Create mode-0600 `access.json` outside Git from [the synthetic example](access.example.json).
Supply the exact selected port, network, subnet and fixed IPv4 address for each
role, plus the operator's exact `/32` source address. This initial bounded adapter
requires one interface per VM, one shared private IPv4 subnet, port security
without allowed-address-pair overrides, and no extra subnet routes. Unsupported
or ambiguous topology remains a manual checkpoint; it is never guessed.

`route.mode: private` attests an existing operator route into that private subnet.
It must have no selected floating IPs. For `route.mode: floating`, set verified
`external_network_id` and `router_id` UUIDs. The router must already connect the
selected subnet to that external network. The tool never creates routers, subnets,
networks or new VM interfaces. `operator_route_verified: true` is the operator's
explicit access attestation, not a connectivity test performed by the tool.

```bash
python3 bin/flowdc_ops.py pilot prepare \
  --profile "$HOME/.config/flowdc/profile.json" \
  --spec "$HOME/.config/flowdc/pilot.json" \
  --inventory "$HOME/.local/share/flowdc-ops/inventory/pilot-observation.json" \
  --access "$HOME/.config/flowdc/access.json" \
  --install-supervisor
python3 bin/flowdc_ops.py pilot status
```

Omit `--install-supervisor` to register without arming. This leaves readiness
pending. The explicit flag copies the six tool modules into a content-addressed
private release, pins the resolved interpreter and its digest, writes
`~/.config/systemd/user/flowdc-pilot.service`, and runs user-manager daemon reload
and `enable --now`. It does not activate VMs or modify cloud networking. Existing
unit/release conflicts are preserved and refused. Existing directory permissions
are never relaxed. The unit restarts on failure and discards stdout/stderr; durable
state and fixed checkpoints are in the private journal. No packages, lingering,
system-wide units or privileged settings are changed.

`pilot supervise --state-root ABSOLUTE_ROOT` is an internal entrypoint. It requires
the registered immutable release/interpreter and matching systemd MainPID and
InvocationID, then takes the exclusive supervisor lock. Running it manually from
the checkout is refused. The foreground CLI never performs lifecycle mutation.
Do not launch a second service, copy the profile to obtain another budget, edit the
unit during obligations, or remove registration files. For survival across logout,
start requires verified `Linger=yes`, checked read-only through `loginctl`. If it
needs enabling, the operator must arrange that separately; cleanup remains available
without this start prerequisite. Enabled user units restart when that user's manager starts.

## Start, inspect, stop and reconcile

```bash
# Default inspection: total window <=1800 seconds, cleanup threshold <=1200.
python3 bin/flowdc_ops.py pilot start --window-seconds 1800
python3 bin/flowdc_ops.py pilot status
python3 bin/flowdc_ops.py pilot stop
python3 bin/flowdc_ops.py pilot reconcile
```

Every command also accepts the same absolute `--state-root` used for preparation.
`start --full-window --window-seconds SECONDS` permits a longer total window within
the **existing remaining balance**. It cannot grant additional allowance. The
window includes setup and cleanup, not just useful inspection time. Extremely
short windows can expire during setup without activating anything.

All commands use version-1 JSON with the existing outcome envelope. Exit 0 means
the request/status check succeeded; **request acceptance is not ACTIVE or offload
completion**. Exit 3 means a pending prerequisite, stale/missing supervisor,
activation/cleanup in progress, stale observations or an explicit recovery
checkpoint. Exit 2 rejects unsafe paths,
arguments or schemas; exit 1 reports local operational failure. Invalid arguments
and raw provider diagnostics are never echoed. Status projects current conservative
consumption, shows observation freshness (120-second bound), and labels stale
provider state UNKNOWN. Preparation remains pending until observations exist;
start performs a fresh preflight, or reconcile can refresh them without activation.
A fresh heartbeat and held supervisor lock are required
before accepting start. They are local liveness evidence, not cloud availability.

Identical duplicate starts preserve the active window. Changed windows are refused
until cleanup completes. Stop and reconcile both cancel pending activation and
request verified offload plus scoped rollback. Reconcile refreshes settled
observations and never resumes unshelving. Unexpected
activity discovered after a settled interval exhausts remaining allowance and
creates a new cleanup obligation because its start time is unknown.
Concurrent clients use short SQLite transactions; provider calls execute outside
transactions. A stop arriving during an external call is preserved when its result
is saved. An already-issued external request cannot be recalled.

## Cumulative accounting and recovery

`pilot-binding.json` next to the existing profile binds a single `pilot.sqlite3`
to the immutable context, three UUIDs, limits and access facts. Changing run/spec
filenames or state roots cannot replenish it. Repeat preparation checks the same
record. Missing, corrupt or unsupported history is refused; it is not recreated.
Private containing paths, database, lock files and SQLite auxiliary files reject
unsafe permissions, links and nonregular types. SQLite uses full synchronous
commits and rollback journaling. Preserve disk space and the entire state tree.

Activation intent is committed **before network setup or unshelve**, starting all
three accounts. Unknown provider periods, partial activation, setup, retries and
cleanup remain chargeable until a fresh identity-validated observation confirms
`SHELVED_OFFLOADED`. Consumption can exceed the limit when cleanup is late; reporting
never caps it to conceal an overrun. SHUTOFF, SHELVED, API acknowledgement and guest
shutdown do not settle an obligation or establish a billing stop.

The nominal shutdown threshold is total window minus 600 seconds, bounded by the
remaining cumulative allowance. The scheduler begins cleanup another **180 seconds
early**, allowing three bounded observation/mutation steps and polling overhead.
An 1800-second inspection therefore enters cleanup by approximately 1020 seconds
in normal operation, before the required 1200-second threshold. Provider failures
can defeat completion despite that reserve. Each adapter step shares one 20-second
budget across verification and subprocesses, with 256 KiB combined output limits
per subprocess and bounded child cleanup. Each VM's cleanup attempt is at least ten
boot-time seconds apart (a reboot/backwards boot clock permits immediate retry);
attempts rotate so one failed VM cannot starve the other two.
Private journal and registration lock acquisition waits at most two seconds before
returning `pilot_state_busy` (exit 3). Retry shortly; persistent contention requires
inspection of the stalled local process. Preserve lock files and journal; do not
unlink them to bypass serialization. Duplicate supervisors are rejected immediately.
SQLite's separate two-second busy timeout still applies after lock acquisition.
There is no unlimited foreground blocking request or automatic budget extension.

Accounting uses CLOCK_BOOTTIME (including suspend), a boot identity and UTC.
Same-boot intervals charge the larger elapsed measurement. Clock disagreement
over five seconds, backwards time or a changed boot identity exhausts any remaining
allowance and permanently prohibits new activation for that registration. Process
restart with an outstanding run forces cleanup, rather than replaying unshelve.
The service retains obligations during unsuccessful cleanup and continues bounded
attempts. It must stay running while recovery is pending.

An accepted or lost unshelve reply followed by `SHELVED_OFFLOADED` may still
show the state before activation. Until a settled activation state is observed,
`activation_completion_unresolved` retains the charged obligation and keeps
polling, across stop requests and restarts. A later ACTIVE observation triggers
shelving. If activation never becomes observable, this remains a manual checkpoint;
contact the cloud operator to establish the request's disposition and preserve the
journal. Neither elapsed time nor another offloaded observation cancels that intent.
Cleanup verifies cloud context and VM identity even if selected networking drifts;
network restoration retains its own stricter attachment checks and can remain pending.

This is local durable state, not a tamper-proof quota service. Deleting both the
binding and journal, copying all credentials into another independent installation,
restoring an old backup over current history, or manually activating VMs outside
this service defeats local accounting and is prohibited by the operating contract.
There is no outage-proof wall-clock/billing guarantee: power loss, suspension,
logout without a persistent user manager, lost connectivity, credential revocation,
disk failure or a provider outage can prevent timely offload. Use the emergency
procedure from another reachable control path when needed.

## Selected-interface network setup and rollback

Routing is inspected before mutation. A verified existing manager floating entry
is reused. Otherwise at most one pilot-owned floating IP is created for the manager
on the verified external network; existing worker/origin public entries are refused.
The tool creates a dedicated security group per selected interface. Manager ingress
allows only operator `/32` SSH plus private peer traffic. Each VM allows TCP, UDP
and ICMP only from the other selected private `/32` peers; no public TaskVine/origin
access is added. Provider-default egress is retained. Original security groups are
recorded and replaced on those exact ports; their rules are never edited.

Creation intentions carry unique recorded ownership markers. After a lost response,
reconciliation searches for that identity; it does not blindly duplicate creation.
Unobserved creation stays pending, including when a quota/permission error cannot
establish whether the side effect occurred. Fixed `network_quota_pending` and
`provider_permission_pending` checkpoints distinguish recognized failures without
printing diagnostics. Unexpected output remains `provider_request_failed` or
`provider_schema`; inspect the provider privately for details.

Rollback starts after registered obligations are confirmed offloaded. It removes
only the recorded pilot-owned floating entry, restores exact original selected-port
attachments, and deletes only owned groups with no remaining attached ports.
External attachment/ownership changes cause a checkpoint instead of overwriting
someone else's work. Accepted deletions/restorations require subsequent observation.
Existing manager floating IPs and shared groups are retained. Prior network intent
history and consumption survive a later start.

The fixed adapter follows the upstream [compute commands](https://docs.openstack.org/python-openstackclient/latest/cli/command-objects/compute/v2/index.html)
and [network commands](https://docs.openstack.org/python-openstackclient/latest/cli/command-objects/network/v2/index.html).
Explicit offload is admin-only by default in OpenStack; local tests cannot establish
this allocation's permission or automatic-offload policy. The older read-only
compatibility record does not validate these new mutations or network response shapes.

## Exact manual checkpoint and emergency procedure

1. Keep the supervisor enabled. Save `pilot status` privately and identify the
   registered project, region and exactly three UUIDs from the preserved spec/journal.
   Never publish credentials, raw provider logs or the private journal.
2. From an available authorized workstation, open Horizon and select that exact
   allocation/project and region. Inspect each registered VM by UUID. For every VM
   not confirmed SHELVED_OFFLOADED, request Shelve. Inspect again; acknowledgement,
   guest shutdown and SHELVED alone are insufficient.
3. If it remains SHELVED and separate operational authorization/access permits it,
   the equivalent trusted-administration CLI action is
   `openstack server shelve --offload REGISTERED_UUID`, followed by
   `openstack server show REGISTERED_UUID -f json -c id -c project_id -c status`.
   Recheck allocation/region before each command. If denied or stuck, immediately
   contact the allocation/cloud operator for verified offload. Do not substitute
   delete/rebuild/resize, activate another VM, or claim a billing guarantee.
4. Restore workstation access, keep the same journal, then run `pilot reconcile`
   and inspect status. Resolve named quota/permission or routing facts privately.
   For ambiguous network creation, have the provider operator reconcile the exact
   recorded marker and UUID. If absence cannot be established by the tool, preserve
   the pending checkpoint; do not clear intent history or rerun preparation in a
   new root to obtain a clean status. A conflict requires operator investigation,
   not automatic recreation or edits to shared rules.
5. Wait for all per-VM obligations to be false, fresh offloaded observations, idle
   desired state and `network_rolled_back: true`. Only then may the operator disable
   `flowdc-pilot.service` with `systemctl --user disable --now flowdc-pilot.service`
   for tool rollback. Preserve unit contents, releases, journal, binding, credentials
   and prior artifacts. Select a reviewed release through an explicit installation
   change after cleanup; the installer deliberately refuses silently replacing a unit.

## Validation without cloud mutation

```bash
python3 -m unittest discover -s tests -p 'test_flowdc_pilot*.py' -v
python3 tests/pilot_systemd_smoke.py
python -m unittest discover -s tests -v
make check
git diff --check
ruff check bin/flowdc_ops.py bin/flowdc_pilot*.py tests/test_flowdc_pilot*.py tests/pilot_systemd_smoke.py
ruff format --check bin/flowdc_ops.py bin/flowdc_pilot*.py tests/test_flowdc_pilot*.py tests/pilot_systemd_smoke.py
```

The explicit systemd smoke uses a uniquely named transient **fake-only** unit,
synthetic identifiers, an invalid-for-live fake profile and accelerated test clocks.
A separate requester exits, then the actual supervisor state machine reaches its
deadline and confirms fake offload. The fixture never selects the live provider or
touches `flowdc-pilot.service`; it stops only its own disposable unit. A private
`/tmp/flowdc-pilot-systemd-fake-*` evidence directory is retained. Exit 3 means the
real user manager was unavailable; it does **not** count as a successful smoke.
The coordinator must execute this unchanged procedure and the unchanged localhost
application tests if the executor sandbox lacks those capabilities. Real cloud
mutation, guest readiness, scientific validity and hard billing guarantees remain
outside this software validation.

### Service termination and integrity recovery

The installed unit sets `TimeoutStopSec=infinity`. SIGTERM (including a unit stop)
requests cleanup and the process exits only after verified offload and scoped
network rollback. A stop can therefore wait indefinitely on an unresolved action,
network conflict or provider outage. Prefer `pilot stop` and inspect `pilot status`
while the service continues running. Do not force-kill or power off to claim billing
has stopped; workstation/user-manager shutdown can still interrupt recovery. Use
the emergency procedure above if completion cannot be verified.

On service startup, permanent release/interpreter/service identity verification
failures exit with status 78, which `RestartPreventExitStatus=78` exposes as a failed
unit without an endless restart loop. `systemctl --user status flowdc-pilot.service`
and `journalctl --user -u flowdc-pilot.service` show the failed exit and sanitized
verification code; raw provider diagnostics are never logged. The private checkpoint
is best effort: contention may prevent storing it, but does not replace the original
verification error. Transient user-bus/provider/lock failures remain restartable.
No cleanup code runs after a failed integrity check. Perform emergency cloud cleanup
manually, preserve the journal and release files, restore the verified runtime through
trusted operator recovery, then restart the same unit and reconcile. Do not edit
recorded hashes, discard obligations or create a new allowance to bypass verification.
Interpreter updates are checked on a subsequent start/restart, not on every tick of
an already-running process.

`last_cleanup_request` in status is the last durably recorded shelve/offload intent
and its clock, possibly from an earlier run. It is neither proof of completion nor
proof that a request is currently in flight; use the separately timestamped provider
observation. Intents are retained and validated on reload.

A rejected installation can leave a content-addressed release directory. Preservation
is intentional; do not remove the release referenced by the journal/unit or any
outstanding recovery. Review unreferenced copies only after all obligations are
resolved; no automatic release deletion or accounting reset is provided.

The standalone `flowdc_ops.py` read-only commands remain usable without pilot sibling
modules. An unavailable `pilot` command returns versioned `pilot_unavailable` (exit 3);
install the complete reviewed release before using it. Internal import defects are
not hidden as optional-module absence.

For coordinator integration checks, run both `python3 tests/pilot_systemd_smoke.py`
and `python3 tests/pilot_systemd_smoke.py --sigterm`. Both use a unique transient
fake unit and the copied production `supervise`/`verify_service` implementation,
with real systemd identity checks and synthetic provider/clock injection confined to
the test harness. The second signals the service while three fake obligations exist.
These checks do not install the production unit, load real credentials, or establish
live cloud behavior. Installer unit generation is separately covered by unit tests.
