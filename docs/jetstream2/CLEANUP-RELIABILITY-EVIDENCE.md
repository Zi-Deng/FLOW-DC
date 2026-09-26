# Cleanup reliability software evidence

This records software implementation evidence for [issue #17](https://github.com/Zi-Deng/FLOW-DC/issues/17),
under its [approved plan](https://github.com/Zi-Deng/FLOW-DC/issues/17#issuecomment-5839864053).
It covers rollback scheduling, explicit SDK offload, bounded diagnostics and
cleanup-only recovery. Final committed-head host validation and independent review
remain separate requirements; these local results do not establish merge or
deployment readiness.

## Synthetic rollback counterexample

`tests/test_flowdc_pilot_network.py` drives the production `network_step`,
`rollback_step`, and `read_batch` methods with three selected role ports, their
three temporary groups, a shared original group, and three unrelated floating IPs.
All identities and resources are synthetic. The fixture uses these durations:

| Operation | Logical seconds |
| --- | ---: |
| Context configuration read | 0.4 |
| Authenticated project read | 1.1 |
| Security group list | 1.75 |
| Each security group detail | 1.7 |
| Floating IP list | 2.2 |
| Each floating IP detail | 1.7 |
| Each selected port read | 2.4 |
| Group attachment list | 2.0 |
| Port restore or group delete | 1.9 |

`LogicalExecutor` uses real worker threads. Production submission and join order
determine which reads overlap; the fixture advances time to their completion when
the actor joins them. Each operation consumes the same step deadline and is capped
at its expiration. It does not sleep for the modeled latency. The primary
counterexample chooses no applied effect for a timed-out mutation. Separate tests
cover possible applied effects and fresh reconciliation after restarting the adapter
and reopening the journal.

Before changing production code, the new regression failed at base
`6865546e34431835cdcfb65ab1bf2558a7918e74` with this trace. The same failure was
subsequently reproduced using an exported copy of that base:

| Step | Base logical seconds / result | Repaired logical seconds / result |
| --- | --- | --- |
| 1 | 16.55 / restore acknowledged | 13.15 / restore acknowledged |
| 2 | 18.55 / delete acknowledged | 15.15 / delete acknowledged |
| 3 | 18.95 / restore acknowledged | 13.15 / restore acknowledged |
| 4 | 20.00 / `probe_timeout` | 15.15 / delete acknowledged |
| 5 | 20.00 / `probe_timeout` | 13.15 / restore acknowledged |
| 6 | 20.00 / `probe_timeout` | 15.15 / delete acknowledged |
| 7 | 20.00 / `probe_timeout` | 9.55 / fresh final rollback proof |
| 8–10 | 20.00 / `probe_timeout` each; still pending | Already verified |

The repaired provider batches floating IP details and selected port observations
with at most four workers. It validates the returned identities on the actor, then
performs at most one mutation. Restored ports are freshly checked on subsequent
steps. No deadline, ownership rule, journal schema, or dependency changes are needed.
Unrelated floating IPs and the shared original group remain unchanged.

Serializing the two new read batches, while leaving the existing group batching
intact, restores the timeout failure: ten 20-second steps, without completing even
the first restore. This control uses the repaired read selection (all selected ports
each step), so its early-step durations differ from the base's serial role traversal.

## Reproduce the base failure without changing the task checkout

Run from the task root with the repository development environment. The following
exports tracked base code into a new temporary directory and overlays only the new
regression file. It creates no Git worktree and loads no cloud profile or credentials.

```bash
issue17_repro_dir=$(mktemp -d /tmp/flowdc-issue17-base-XXXXXX)
git archive 6865546e34431835cdcfb65ab1bf2558a7918e74 bin tests | tar -x -C "$issue17_repro_dir"
cp tests/test_flowdc_pilot_network.py "$issue17_repro_dir/tests/"
.venv-agentic/bin/python -B -m unittest discover -s "$issue17_repro_dir/tests" -p 'test_flowdc_pilot_network.py' -k test_slow_rollback_completes_with_three_unrelated_floating_ips -v
```

Observed exit: **1**, one failed regression. The assertion reports `rolled_back`
remaining false and the exact base trace above. The temporary export contains only
tracked source and the synthetic test; it is not a controller release or accounting
snapshot. The equivalent command against the repaired checkout exits **0**:

```bash
.venv-agentic/bin/python -B -m unittest discover -s tests -p 'test_flowdc_pilot_network.py' -k test_slow_rollback_completes_with_three_unrelated_floating_ips -v
```

## Offload and diagnostic regressions

`tests/pilot_offload_compat.py` exercises the actual installed OpenStackClient
10.3.0 `ShelveServer.take_action(offload=True, wait=False)` with a fake compute
service and socket creation forbidden. The observed reproduction command was:

```bash
"$HOME/.local/share/flowdc-ops/venv/bin/python" -B tests/pilot_offload_compat.py
```

Exit **0**, with source SHA-256
`63795d7b1720123545d6dfd20396dab06a124ac4ea2b9fda785e1fe3dd0e9070`:

```json
[{"initial_state":"SHELVED","actions":[]},
 {"initial_state":"ACTIVE","actions":["shelve","offload"]}]
```

This command was run before the runtime trust guard identified the installed
administration environment's group-writable base interpreter/ancestor. It has
**not** been rerun through that refused runtime. Future reproduction requires a
separately established trusted existing 10.3.0 environment; this task neither
installs packages nor repairs runtime permissions. The ACTIVE case is a control,
not a recovery workaround. No OpenRC, profile, authentication or cloud was used.
Normal CI does not depend on that optional installed-client script.

Before their corresponding production repairs, two maintained regressions failed.
They were also rerun against an exported base with only test files overlaid:

```bash
issue17_repro_dir=$(mktemp -d /tmp/flowdc-issue17-offload-base-XXXXXX)
git archive 6865546e34431835cdcfb65ab1bf2558a7918e74 bin tests | tar -x -C "$issue17_repro_dir"
cp tests/test_flowdc_pilot_offload.py tests/test_flowdc_pilot_cli.py "$issue17_repro_dir/tests/"
.venv-agentic/bin/python -B -m unittest discover -s "$issue17_repro_dir/tests" -p 'test_flowdc_pilot_offload.py' -k test_verified_shelved_vm_dispatches_one_sdk_offload -v
.venv-agentic/bin/python -B -m unittest discover -s "$issue17_repro_dir/tests" -p 'test_flowdc_pilot_cli.py' -k test_provider_failure_retains_action_and_budget_without_secret_text -v
```

Each command exits **1** for the intended assertion: the offload request list is
`[]` instead of one request for synthetic UUID
`22222222-2222-4222-8222-000000000001`; the diagnostic action is `None` instead of
`port`. The equivalent tests against the repaired checkout pass.

The SDK tests use `/usr/bin/python3.12 -I -B -m venv --without-pip` to create a
private disposable venv after checking that explicit test interpreter's ownership
and permissions. Tiny fake SDK modules are written only inside that fixture; no
package is installed. This is not a production fallback. The production adapter
still selects only the configured administration venv and refuses unsupported
paths. The tests execute the actual fixed child program and OpenRC allowlist through
`Provider.lifecycle`, checking exactly one SDK offload, bound project/region/VM,
changed/already-offloaded state refusal, isolated runtime semantics and hostile
configuration rejection. Permission/conflict/transient/unknown responses, lost
acknowledgement, timeout/reaping, malformed replies and secret canaries are covered.
An actual supervisor test verifies durable intent and continued obligation after
both acknowledgement and lost response, with no task metadata, until a later
fresh `SHELVED_OFFLOADED` observation.

`tests/test_flowdc_pilot_diagnostics.py` verifies the fixed action survives worker
failure while every future joins; equivalent failures coalesce and the ring stays
at 32 entries (under 12 KiB in the fixture). Hostile text, URLs, nonfinite numbers
and environment values cannot enter journal/status diagnostics. Binding, consumed
seconds, lifecycle/network history and actual allowance receipts are unchanged.
The retained legacy journal validator accepts the new ordinary event envelope.
A failed diagnostic transaction cannot undo the stop or previously charged usage.
Only explicit HTTP markers are classified; a bare resource number or the word
“conflict” remains unknown. The historical nine generic failures remain unexplained.

The experiment regression advances logical time through a 300-second client
cleanup wait. It verifies exit 3 with passed workload and verified guest stops,
retained ownership and unchanged collected artifacts, followed by cleanup-only
stop exit 0 with no new start. The original incomplete output and historical
errors are retained. No runner defaults, reserve or allowance limits change.

## Validation and remaining host checks

On the first checkpoint `1557d840b860708883bd59a461f23827b06a5e16`, the coordinator
reported these exact host commands with exit **0**:

```bash
make check
make check-clean
.venv-agentic/bin/python -B tests/pilot_systemd_smoke.py
.venv-agentic/bin/python -B tests/pilot_systemd_smoke.py --sigterm
.venv-agentic/bin/python -B tests/pilot_upgrade_smoke.py
.venv-agentic/bin/python -B tests/pilot_upgrade_smoke.py --rollback
.venv-agentic/bin/python -B tests/pilot_allowance_smoke.py
```

That full gate passed 293 maintained tests plus 141 workflow tests (434 total).
The coordinator corrected an initial host-wrapper umask issue and reran the gate;
no source or test assertion was changed. Earlier worker HTTP-fixture and user-bus
permission errors were environmental; no checks were skipped or weakened.
These checkpoint results do **not** validate subsequent implementation changes.

The implementation's worker checks are:

| Command | Exit / result |
| --- | --- |
| `.venv-agentic/bin/python -B -m unittest discover -s tests -p 'test_flowdc_pilot*.py' -v` | 0; 176 tests |
| `.venv-agentic/bin/python -B -m unittest discover -s tests -p 'test_flowdc_experiment.py' -v` | 0; 35 tests |
| `.venv-agentic/bin/python -B -m unittest discover -s tests -p 'test_flowdc_ops.py' -v` | 0; 80 tests |
| `.venv-agentic/bin/python -B -m unittest discover -s tests -p 'test_flowdc_pilot_diagnostics.py' -v` | 0; 8 tests, including oversized/nonfinite number rejection |
| `make check-agentic` | 0; 141 workflow tests, lint, format and repository configuration |
| Targeted `ruff check` and `ruff format --check` on the changed Python files (listed in PR handoff) | 0 |
| `git diff --check` | 0 |

Final clean-head `make check`, the complete `test_flowdc_experiment*.py` suite,
and all five user-systemd smokes still require the coordinator's host environment.
The restricted worker cannot create the existing HTTP fixture sockets or reach
the user bus; it does not re-probe those known restrictions or widen permissions.
The upgraded fake-service fixture now includes the diagnostic event in its full
history-preservation assertion through both upgrade and rollback, including the
allowance-smoke path. Exact final SHA, command exit statuses, `make check-clean`,
CI and independent review belong in the PR handoff once available.

The installed SDK was inspected but its new offload path was not executed through
the refused administration runtime. The actual fixed child/fake-SDK tests do not
establish compatibility of every deployed SDK transport or cloud permission.
Deployment/runtime repair remains a later operational checkpoint. Independent
review of this boundary is still required.

These are V0/V1 software and V2 local fake-service results. They do not reconstruct
the September 25 provider failures, promise a provider-latency bound, establish an
HTTP status for historical generic errors, or constitute a new live/scientific
result. No deployment, cloud mutation, allowance grant, or real task cleanup was
performed. The prior trial's exit-3/recovered-exit-0 history remains unchanged.
