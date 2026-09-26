# Cleanup reliability software evidence

This is the first implementation checkpoint for [issue #17](https://github.com/Zi-Deng/FLOW-DC/issues/17),
under its [approved plan](https://github.com/Zi-Deng/FLOW-DC/issues/17#issuecomment-5839864053).
It covers rollback scheduling (AC1) and related preservation regressions. Explicit
SDK offload, bounded diagnostics, runner recovery evidence, and operator recovery
documentation remain incomplete. This checkpoint is not deployment or merge readiness.

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

## Checkpoint validation

| Command | Exit / result |
| --- | --- |
| `.venv-agentic/bin/python -B -m unittest discover -s tests -p 'test_flowdc_pilot_network.py' -v` | 0; 40 tests pass (base suite had 30 passing tests) |
| `.venv-agentic/bin/python -B -m unittest discover -s tests -p 'test_flowdc_pilot*.py' -v` | 0; 154 tests pass |
| `.venv-agentic/bin/python -B -m unittest discover -s tests -p 'test_flowdc_experiment*.py' -v` | 1; 51 tests run, one environment error creating the local HTTP fixture socket: `PermissionError: [Errno 1] Operation not permitted` |
| `.venv-agentic/bin/ruff check bin/flowdc_pilot_provider.py tests/test_flowdc_pilot_network.py` | 0 |
| `.venv-agentic/bin/ruff format --check bin/flowdc_pilot_provider.py tests/test_flowdc_pilot_network.py` | 0 |
| `git diff --check` | 0 |
| `systemctl --user show --property=Version` | 1; `Failed to connect to bus: Operation not permitted` |

The network tests include zero/one/multiple/maximum floating collections, malformed
and duplicate identities, actor-side validation, four-worker concurrency, no mutation
after a read timeout, possible-applied mutation timeouts, port drift, foreign group
attachments, late creation, restart, and joined/reaped subprocesses. An existing
router test now enters the provider's step context when calling its internal helper,
so the newly batched reads receive the real bounded deadline; its assertions are
unchanged. No test was skipped or weakened to bypass the sandbox restrictions.

Full `make check` and `make check-clean` results belong in the coordinator's PR
handoff for the committed checkpoint. The ordinary, SIGTERM, upgrade, rollback and
allowance user-systemd smokes require a reachable user manager, which this executor
cannot access. The experiment socket error and smoke restrictions remain validation
blockers; permissions were not expanded. SDK compatibility and diagnostic canary
checks await the remaining implementation.

These are V0/V1 software and synthetic scheduling results. They do not reconstruct
the September 25 provider failures, prove a provider-latency bound, establish an HTTP
status for the historical generic errors, or constitute a new live/scientific result.
No deployment, cloud mutation, allowance grant, or real task cleanup was performed.
