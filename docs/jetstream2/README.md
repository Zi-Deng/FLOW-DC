# Local Jetstream2 control and read-only preflight

`bin/flowdc_ops.py` runs on the **local control machine**, using Python 3.12+ on
Linux. It provides `init`, `doctor`, `inventory`, and `plan`. It does not deploy to
guests, unlock SSH keys, change systemd settings, install packages, download data,
or activate resources. `run`, `fleet`, and lifecycle commands are not implemented.
Run as the ordinary operator account with matching real/effective user IDs;
privileged or setuid execution is not a supported mode. Inventory uses `/bin/bash`,
and doctor checks that exact executable path. The empty `OS_*` environment case
was exercised with Bash 5.2.21; older-shell compatibility is not established.

| Location | Purpose |
| --- | --- |
| Local repository checkout | Versioned tool, tests, and synthetic examples. |
| Local `$HOME/.config/flowdc` | Private operator profile, trusted `js2` wrapper, and optionally the OpenRC and pilot spec. |
| Local `$HOME/.local/share/flowdc-ops` | Private `inventory/`, `releases/`, and `runs/` directories. |
| Local separate administration environment | Coordinator-installed OpenStack client; no automatic installation or sudo. |
| Remote manager, worker, origin | Three existing VMs described by UUID; no guest action is performed. |

## Initialize and inspect locally

From the repository checkout:

```bash
python3 bin/flowdc_ops.py init
python3 bin/flowdc_ops.py doctor
```

Initialization creates directories with mode 0700, the profile with mode 0600, and
the wrapper with mode 0700. It returns exit 0 for successful **local setup**, with
`cloud_readiness: not_assessed`. Running it again preserves existing contents.
Existing profile contents are not read during init. Existing wrapper contents must
match the fixed program for this tool version; a conflict is preserved and reported.

Optional absolute roots allow disposable validation without touching real state:

```bash
python3 bin/flowdc_ops.py init \
  --state-root /absolute/private/flowdc-state \
  --config-root /absolute/private/flowdc-config
python3 bin/flowdc_ops.py doctor \
  --state-root /absolute/private/flowdc-state \
  --profile /absolute/private/flowdc-config/profile.json
```

Roots must be separate and nonoverlapping, outside Git working trees, without
symlinks or `..`. Managed directories and input-file parents must be owner-private
(0700). Inputs must be regular, singly linked, owner-owned files with mode 0600;
the wrapper requires 0700. Ancestors must be controlled by the operator or OS root
and not writable by others, except sticky temporary directories such as `/tmp`.
The explicit client path must name an executable regular file, controlled by the
operator or root, without group/other write access or symlink components. Use the
administration environment's `bin/openstack`, not its often-symlinked Python binary.
The CLI never changes existing permissions automatically.

`doctor` checks tools and configured paths, credential **metadata only**, Python,
the local SSH agent (`ssh-add -l`), the user systemd manager, and lingering. It
discards key identities and probe diagnostics. An absent/empty agent does not rule
out direct key-file or other SSH authentication. Failed probes, missing paths,
unconfigured context, disabled lingering, or missing tools yield exit 3 and manual
next actions. A pass means these local prerequisites were observed, not that cloud
authentication, guest reachability, or persistent services work.

## Manual enrollment and the missing-credential checkpoint

Until the operator supplies an application credential, leave `credential_file`
unset. Doctor reports a pending checkpoint; inventory makes no provider call.
Do not create substitute credentials or infer readiness from installed SSH.

Follow Jetstream2's [application-credential instructions](https://docs.jetstream-cloud.org/ui/cli/auth/):

1. Open [Horizon](https://js2.jetstream-cloud.org), select ACCESS CILogin, and complete
   your browser authentication and MFA.
2. Select the intended allocation/project at the top left. Choose **Identity →
   Application Credentials → Create Application Credential**. Choose a descriptive
   name and an intentional expiration; follow the provider's guidance for roles
   and access rules.
3. Download that credential's OpenRC from its confirmation screen. Do not use the
   unrelated username-menu OpenRC generator. Store the downloaded file privately
   outside Git, under a 0700 directory, and set its file mode to 0600 manually.
4. Record its absolute path in `profile.json`. Never paste its contents, secret,
   credential ID, or tokens into the profile, terminal transcripts, issues, or PRs.

The operator/coordinator separately prepares and freezes the administration
environment, following the [client setup guide](https://docs.jetstream-cloud.org/ui/cli/clients/).
Record the absolute `bin/openstack` path and retain its version/dependency evidence
privately. No top-level project dependency is added by this CLI.

OpenRC is **trusted operator-provided Bash code**, not a JSON data file. Only
inventory sources it. The wrapper clears inherited `OS_*`, disables tracing,
suppresses OpenRC prints, and uses a minimal child environment. It requires
`v3applicationcredential`, credential ID/secret, auth URL, and region in that file.
Only those exports plus identity API version and interface are accepted; cloud,
token, password/project-scope, and endpoint overrides are rejected. Start with the
downloaded application-credential OpenRC rather than combining other profiles.
Do not invoke `js2` directly or replace it with a general command dispatcher.

## Profile and initial discovery

Init writes a version 1 template. Fill it privately using the
[synthetic profile example](profile.example.json) as a field reference. Actual
project IDs, regions, endpoints and VM UUIDs must come from your allocation; the
examples are not live identifiers. An unset project/region/site remains `null`.
`wrapper_path` is the generated config directory's `js2` path. `known_seed_id` is
optional, and when set must belong to `intended_server_ids`. The allowlist accepts
at most 64 distinct UUIDs for bounded read-only fleet observation, including the
existing 22-VM fleet. The pilot limit is separately fixed at three VMs. UUIDs may
be hyphenated or 32 hex digits and are normalized for comparisons.

If upgrading an early checkpoint profile, init preserves it: manually add
`"auth_url": null` and set `wrapper_path` if needed. Unknown fields and unsupported
schema versions are rejected rather than silently migrated.

After configuring only the credential, wrapper, and client paths, obtain a bounded
first observation without guessing project or region:

```bash
python3 bin/flowdc_ops.py inventory \
  --profile "$HOME/.config/flowdc/profile.json" \
  --output "$HOME/.local/share/flowdc-ops/inventory/first-observation.json"
```

This returns exit 3 with `complete: false` while expected context is unset. It can
observe the authenticated project ID, selected region and identity auth URL, and
check that the region is advertised. It does not enumerate arbitrary VMs. Verify
these values against the intended Horizon allocation/site before recording them
as `expected_project_id`, `region`, and `auth_url`. Copy the intended existing fleet
UUIDs from Horizon into the allowlist. Do not copy unrelated instances. A pilot
selects exactly three of those observed UUIDs; inventory activates none of them.
Then rerun inventory with a **fresh** output name:

```bash
python3 bin/flowdc_ops.py inventory \
  --profile "$HOME/.config/flowdc/profile.json" \
  --output "$HOME/.local/share/flowdc-ops/inventory/pilot-observation.json"
```

The fixed command set uses JSON formatting and selected columns:

| Command | Retained observation |
| --- | --- |
| `configuration show --mask` | Region and `auth_url` / `auth.auth_url` only; reject conflicting URL values. |
| `token issue -c project_id` | Authenticated project ID; the token ID is never requested as an output column. |
| `region list` | Check selected region membership; no guessed region name. |
| `server show UUID` | Intended UUID/project/state, flavor reference, IP addresses, image UUID, keypair presence. |
| `flavor show ID` | ID, vCPUs, RAM MB, root disk GB. |
| `quota show --compute` | Instance, core and RAM quota limits; these are not an activation guarantee. |
| `network list --project UUID --long` | Project-filtered network IDs, states and subnet IDs; `--long` supplies the Status column. |

Calls select the public interface and compute microversion 2.1. The maintained
[OpenStack command reference](https://docs.openstack.org/python-openstackclient/latest/cli/command-objects/common/index.html)
and upstream source define the expected JSON shapes. OSC 10.3 environment-based
configuration reports top-level `auth_url`; nested auth mappings can instead yield
`auth.auth_url`, so both safe columns are requested. Quota rows have `Resource`
and `Limit`; server addresses are IP lists by network; region rows use `Region`.
Incompatible client/provider output is an incomplete operational failure, without
a fallback to broader commands. No live provider compatibility is claimed by the
offline tests. Record the administration client version for any later live check.

The verified administration client version is **python-openstackclient 10.3.0**.
The coordinator's [read-only compatibility record](https://github.com/Zi-Deng/FLOW-DC/pull/6#issuecomment-5724481691)
at commit `eac509238b01d72f9150992b823cdd5583a01db2` covers context/region,
22 intended servers, two flavor IDs, compute quota and project-filtered networks.
It establishes compatibility for that installed client/site, not an untested
version range. Revalidate before changing the administration client. Flavor lookups
must return the exact requested identifier; a bare name resolving to a different
ID remains incomplete rather than relaxing the identity check.

Each cloud process is capped at 20 seconds and 256 KiB combined stdout/stderr;
inventory has a 90-second total subprocess budget. Each unique flavor is inspected
once per inventory, including caching failed inspections. Calls are bounded by
`5 + VM count + unique flavor count`: 28 for 22 VMs sharing one flavor, at most 133
for the 64-VM inventory bound. Exceeding the time budget leaves the snapshot
incomplete rather than skipping observations. Local doctor probes have five
seconds each. Child cleanup gets at most one additional second. A cleanup timeout
is reported as `probe_cleanup_timeout`, never success; inspect local child
processes manually rather than assume the probe has stopped.
Region/network responses have bounded collection sizes. Excess output, timeout,
malformed JSON, missing resources, wrong UUID/project/site, or any required partial
failure cannot produce a complete snapshot. Display names, keypair names, metadata,
token IDs, raw errors, and unrelated provider fields are discarded. The retained
data is still private.

## Validate a three-VM pilot offline

Create an owner-private `pilot.json` using the [synthetic pilot example](pilot.example.json).
Set exactly one manager, one origin, and one worker, using three distinct observed,
allowlisted UUIDs. Match the snapshot's project, region, and auth URL. Each VM's
`active_seconds` must be an integer from 1 through 7200. The plan does not track
past usage or enforce runtime budgets; those belong to the later execution stage.

An inventory must be complete and at most 24 hours old, with no future observation
time. Supported planning states are ACTIVE, SHUTOFF, SHELVED and SHELVED_OFFLOADED;
other states require manual resolution and a fresh observation. Neither state nor
the existence of a keypair establishes guest readiness.

The example leaves every `rate` null, so it is intentionally pending. For each VM,
observe the applicable SU/hour rate for its actual flavor and supply this structure:

```json
{
  "su_per_hour": 1.25,
  "source": "https://billing.example.test/verified-rate-record",
  "observed_at": "2026-09-18T00:00:00+00:00",
  "verified": true,
  "flavor_id": "55555555-5555-4555-8555-555555555555"
}
```

These are synthetic values demonstrating the schema, not Jetstream2 rates. Replace
all of them with your observation. Use a query-free HTTPS source URL, an observation
timestamp with timezone, and the flavor ID from the snapshot. `verified: true` is
an explicit operator attestation; the offline tool cannot independently verify a
price source. Consult the current [Jetstream2 flavor information](https://docs.jetstream-cloud.org/general/instance-flavors/)
and allocation billing context. Missing/unverified rates remain pending; unknown
provenance fields, booleans as numbers, nonfinite values, zero/negative rates and
future rate timestamps are rejected. A flavor mismatch cannot produce a total.

```bash
python3 bin/flowdc_ops.py plan \
  --inventory "$HOME/.local/share/flowdc-ops/inventory/pilot-observation.json" \
  --spec "$HOME/.config/flowdc/pilot.json" \
  --output "$HOME/.local/share/flowdc-ops/runs/pilot-plan.json"
```

Plan performs no subprocess or network operation. With complete facts it reports
per-VM `su_per_hour * active_seconds / 3600` and the sum. Synthetic rates 1.25, 2 and
3 for two hours each yield 2.5 + 4 + 6 = 12.5 SUs. Missing facts produce named
checks and a null total, not an invented rate. Invalid inventory context/freshness
suppresses all per-VM estimates. For usable inventory with one missing rate, any
reported other per-VM estimates are partial and the total remains null.

Exit 0 validates the supplied bounded specification only. It does not reserve
resources, guarantee activation, verify routes or guest services, or establish
that stopping guest services stops billing. No resources are unshelved or changed.
Future execution must honor the approved cumulative two-hour allowance per VM.

## JSON outcomes and private artifacts

Every response has `schema_version`, `operation`, `status`, `checks`, `errors`,
`next_actions`, and `data`. Help is JSON too. Unknown options/fields, duplicate JSON
keys and nonfinite JSON numbers are rejected. Diagnostics never echo raw provider
stderr or invalid argument values.

| Exit | Meaning |
| --- | --- |
| 0 | Operation succeeded: setup, local doctor checks, complete inventory, or validated offline spec. |
| 1 | Operational failure, including failed/bounded provider probes or local I/O. |
| 2 | Invalid input, schema, unsafe input paths, wrapper conflict, or existing output. |
| 3 | Pending readiness/manual action: missing prerequisites, unverified context, incomplete/stale inventory, unsuitable VM state, or missing rates. |

Inventory and plan write nothing unless `--output` is provided. The output parent
must already exist with mode 0700 outside Git. Fresh outputs are mode 0600; existing
files, directories and links are never replaced. Output availability is checked
before discovery and exclusive creation protects against concurrent collisions.
Saved partial inventory retains `complete: false` and its failure/pending status.
Do not substitute an edited snapshot for a fresh observation: snapshots are trusted
local records, not signed cloud attestations.

Avoid redirecting stdout to a shared file: shell redirection is outside these
permission guarantees. Use `--output` for private snapshots. An I/O interruption
can leave a partial file; inspect it and choose a fresh output name. Do not overwrite
it or infer success from its existence. Init similarly retains partial setup files
after failure rather than deleting prior data.

## Validation, remaining evidence, and rollback

```bash
python3 -B -m unittest discover -s tests -p test_flowdc_ops.py -v
python -m unittest discover -s tests -v
make check
git diff --check
ruff check bin/flowdc_ops.py tests/test_flowdc_ops.py
ruff format --check bin/flowdc_ops.py tests/test_flowdc_ops.py
```

Use Ruff from the development environment, or its explicit executable path.
The existing `make check` lint target covers agentic workflow files; application
tests are discovered by its unittest target. Run the two focused Ruff commands
above for the operations CLI and its tests. This unit does not change that gate's
workflow policy.

Ops tests use temporary directories, synthetic OpenRC files, fake executables and
mocked local-session probes. They need neither credentials nor network access.
The existing downloader suite uses a localhost HTTP server; a restricted executor
must report socket failures and leave the coordinator to run the full gate in its
authorized environment. No acceptance test is skipped or weakened for that reason.

Coordinator V2 records disposable init/re-init, the missing-credential doctor
checkpoint, an offline plan, actual tool availability, and installation of the
verified candidate in the real private workspace. Optional live read-only inventory
waits for enrollment; its absence must be recorded. Software checks are not cloud
readiness or scientific evidence. This unit activates zero VMs and spends zero
experiment SUs.

Rollback by selecting the previous local tool version. Preserve existing profiles,
wrappers, credentials, SSH keys, snapshots and run data. Remove only newly created,
identified empty setup files/directories after inspection; never use blanket cleanup.
