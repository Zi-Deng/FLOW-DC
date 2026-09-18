# Local Jetstream2 operations setup

`bin/flowdc_ops.py` is a Python 3.12+ Linux CLI for the operator's **local control
machine**. This initial implementation checkpoint provides `init`. The approved
issue also calls for `doctor`, read-only `inventory`, a trusted OpenStack wrapper,
and offline `plan`; those are pending implementation. `run`, `fleet`, deployment,
VM lifecycle changes, and experiments are future work outside this issue.

Nothing in this checkpoint runs on a manager, worker, or origin VM. Initialization
does not contact OpenStack, test SSH, read credentials, install dependencies, or
establish cloud readiness. It creates zero VMs and activates zero resources.

## Initialize on the local control machine

From the checkout containing the tool:

```bash
python3 bin/flowdc_ops.py init
```

The defaults are `$HOME/.local/share/flowdc-ops` for operations state and
`$HOME/.config/flowdc` for configuration. The state directory contains `inventory/`,
`releases/`, and `runs/`. The config directory contains `profile.json`. Every newly
created directory is mode 0700 and the profile is mode 0600. Missing parent
directories are also created privately; existing ancestor permissions are preserved.

Operators and tests can select two distinct, nonoverlapping absolute roots:

```bash
python3 bin/flowdc_ops.py init \
  --state-root /absolute/private/location/flowdc-ops \
  --config-root /absolute/private/location/flowdc-config
```

Use locations outside Git working trees. Symlinks anywhere in the path, parent
traversal, nonregular or hardlinked profile files, and unsafe managed-path
permissions are rejected. Managed directories must be owned by the current user
with mode 0700; existing profiles must be owned by that user with mode 0600.
Ancestors must be controlled by the current user or the OS root owner and must
not be writable by others, except for sticky temporary directories such as `/tmp`.
Inspect conflicts manually; the tool does not change existing permissions.

Repeating the same command preserves the contents, inode, and modification time of
the profile, as well as all prior state. Initialization checks existing profile
metadata but does not read or validate its contents. Existing paths are reported
as `preserved`. Known conflicts are checked before creation; an interruption or
concurrent change may still leave some newly created private setup files. Inspect
those files before retrying. There is no automatic deletion or replacement.

## Profile and manual checkpoint

The generated operator-editable profile is intentionally unconfigured:

```json
{
  "schema_version": 1,
  "expected_project_id": null,
  "region": null,
  "credential_file": null,
  "wrapper_path": null,
  "openstack_client": null,
  "intended_server_ids": [],
  "known_seed_id": null
}
```

Keep the profile, credential files, VM identities, and generated state private and
outside Git. The profile is for paths and verified context, never credential
values. Do not paste passwords, application credential secrets, or token IDs into
it, logs, issues, or PRs. Initial enrollment and a separately prepared administration
environment remain manual operator steps; initialization neither reads nor creates
credentials or SSH keys.

A missing credential is an unresolved manual checkpoint. Merely installing SSH
or creating these directories does not establish access. Leave unknown project,
region, and UUID values unset. The completed preflight commands and enrollment
guide will explain how to record verified context; this checkpoint cannot claim
that enrollment, guest reachability, or a pilot is ready.

## Responses and exits

Each invocation emits one JSON object, including help and argument errors. The
common keys are `schema_version`, `operation`, `status`, `checks`, `errors`,
`next_actions`, and `data`. Named checks distinguish `created`, `preserved`, and
`not_assessed`. Error messages do not echo raw exception text or unknown argument
values. Every failure includes a manual next action.

| Exit | Meaning |
| --- | --- |
| 0 | Local initialization succeeded, or help was displayed. This is not cloud readiness. |
| 1 | Filesystem operation failed; inspect space, access, and any partial setup. |
| 2 | Invalid arguments, unsafe paths, or conflicting types/permissions. |
| 3 | Reserved for incomplete readiness in the forthcoming preflight commands. |

Successful initialization includes a check with `name: "cloud_readiness"` and
`state: "not_assessed"`, plus manual next actions. Run
`python3 bin/flowdc_ops.py init --help` to get the supported options in a JSON help
response. Unknown options and unavailable commands fail with exit 2.

## Validation and rollback

The initialization tests use temporary directories and do not need credentials,
OpenStack, a network connection, or a systemd session:

```bash
python3 -B -m unittest discover -s tests -p test_flowdc_ops.py -v
```

They are also discovered by the existing `python -m unittest discover -s tests -v`
and `make check` project gates. Coordinator validation of a disposable workspace,
installation in the real private workspace, and any optional live inventory are
separate steps. No live cloud or scientific evidence is established by these tests.

To roll back, select the previous tool version. Remove only newly created,
identified empty setup files or directories after manual inspection. Preserve
edited profiles, credentials, SSH keys, snapshots, and prior run data.
