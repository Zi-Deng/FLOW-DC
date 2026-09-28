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

## Real SDK constructor and transport integration

A coordinator fake-HTTP run with the real SDK exposed an integration defect in
`cd646682ea628f65fba249348003d3267cb98a59`: after synthetic authentication,
`Connection(config=...)` raised `KeyError('secgroup_source')` before offload.
The original lightweight fake did not enforce the real constructor's requirements.
Source inspection also found the required `image_api_use_tasks` key. The repair
supplies just these two fixed keys (`None` and `False`) for the unused network/image
facilities; it does not load packaged defaults, clouds.yaml, vendor profiles or
inherited environment configuration. The fake constructor now enforces both keys.
With that stronger fixture, the existing one-offload regression exited **1** against
the unrepaired provider before the production change, then passed after it.

`tests/pilot_sdk_transport.py` is a maintained optional reproduction using the actual
fixed child and the existing installed SDK packages. Tested versions were
openstacksdk **4.20.0**, keystoneauth1 **5.17.0**, and requests **2.34.2**. It creates a
private `--without-pip` test venv with trusted `/usr/bin/python3.12`, adds only the
explicitly supplied existing SDK directory through a test-only `.pth`, and installs
no package. It never executes the refused administration interpreter or reads real
OpenRC/profile files. Its unique hook replaces `requests.Session.send` with synthetic
auth/catalog/server/action responses and forbids socket connects. Local interface
enumeration is an explicit empty synthetic set. Initial worker attempts without
that local-interface fixture exited **1** on the sandbox's `PermissionError` from
the SDK's IPv6 availability probe; that was a fixture/environment limitation, and
production interface probing was not changed.

From the task root, with the already available SDK directory (no installation):

```bash
issue17_sdk_base=$(mktemp -d /tmp/flowdc-sdk-constructor-base-XXXXXX)
git archive cd646682ea628f65fba249348003d3267cb98a59 bin | tar -x -C "$issue17_sdk_base"
.venv-agentic/bin/python -B tests/pilot_sdk_transport.py \
  --sdk-site-packages "$HOME/.local/share/flowdc-ops/venv/lib/python3.12/site-packages" \
  --provider-source "$issue17_sdk_base/bin" \
  --output "$issue17_sdk_base/base-result.json"
.venv-agentic/bin/python -B tests/pilot_sdk_transport.py \
  --sdk-site-packages "$HOME/.local/share/flowdc-ops/venv/lib/python3.12/site-packages" \
  --output "$issue17_sdk_base/repaired-result.json"
```

The base command exits **1**: all four cases stop after the token request, with
`provider_request_failed`, category `unknown`, dispatch false and the missing
`secgroup_source` exception. The repaired command exits **0**. Each case records
one token request, compute discovery, fresh selected-server GET, and exactly one
POST to `/v2.1/servers/22222222-2222-4222-8222-000000000001/action` with
`{"shelveOffload": null}` and `OpenStack-API-Version: compute 2.1`.

| Synthetic action response | Fixed result | Category | Offload requests |
| --- | --- | --- | ---: |
| 202 | `offload_acknowledged` | `ok` | 1 |
| 403 | `provider_permission_pending` | `permission` | 1 |
| 409 | `provider_request_failed` | `conflict` | 1 |
| 503 | `provider_request_failed` | `transient` | 1 |

No automatic mutation retry occurs for the refusals. Secret canaries in synthetic
error responses do not reach the child output. Provider-source SHA-256 in the
fixture results binds the exact tested contents: base
`7ea97be3bf9d1ee90e814d25879043a6ccfe7c2b89599178c6bdf866dd5d825b`, repaired
`90d0faaacd12418ed9c4789c7712be490e37b5ecbfd92d9581fa67ccc4a5a8d1`.
The coordinator's separate host harness also passed without the synthetic
interface override, against the repaired provider hash above. Host gate results
and their source commits are recorded below. Normal CI remains independent of
these optional SDK packages; the stricter lightweight constructor check runs in
the ordinary pilot suite.

## Offline runtime refusal guidance

The negative `pilot runtime-check` path at
`e6c34b1e8a1382800bec105e550390b53c043e5c` correctly refused an unsupported
administration runtime, but returned generic `pilot` output with emergency cloud
cleanup instructions. A regression now exercises the public CLI against a
synthetic group-writable venv configuration, forbidding journal construction,
OpenRC access, provider reads and child creation. It failed before the correction
(exit **1**, `pilot` instead of `pilot runtime-check`) and passes afterwards.
The same failure was reproduced against an exported pre-correction commit:

```bash
issue17_guidance_base=$(mktemp -d /tmp/flowdc-guidance-base-XXXXXX)
git archive e6c34b1e8a1382800bec105e550390b53c043e5c bin tests | tar -x -C "$issue17_guidance_base"
cp tests/test_flowdc_pilot_offload.py "$issue17_guidance_base/tests/"
.venv-agentic/bin/python -B -m unittest discover -s "$issue17_guidance_base/tests" -p 'test_flowdc_pilot_offload.py' -k test_runtime_check_cli_refusal_is_an_offline_local_prerequisite -v
```

The repaired CLI retains exit **3** and `offload_runtime_unsupported`, reports
`cloud_readiness: not_assessed`, and directs the operator to local administration
runtime setup followed by another offline check. The separate lifecycle-failure
regression retains the existing emergency recovery guidance. The focused command
`.venv-agentic/bin/python -B -m unittest discover -s tests -p 'test_flowdc_pilot_offload.py' -v`
exits **0**, with all **15** tests passing. No installed runtime, permissions,
profile or credential file was changed.

## Validation and remaining host checks

Results below belong to their named source commits. Later source changes require
new validation; the PR body carries the latest final-head evidence after the
coordinator supplies it. A pre-handoff pending note is not a failed test result.

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

On `e6c34b1e8a1382800bec105e550390b53c043e5c`, the coordinator reran all seven
commands above with exit **0** and confirmed the source stayed unchanged.
`make check` passed **316 maintained + 141 workflow tests (457 total)**.
All five fake-service smokes passed, including ordinary/SIGTERM cleanup, upgrade,
rollback and allowance preservation with the new diagnostic event. The public
real-SDK transport fixture also passed all four cases on that head. A separate
coordinator transport harness passed without the synthetic local-interface
override, against identical provider contents verified by SHA-256
`90d0faaacd12418ed9c4789c7712be490e37b5ecbfd92d9581fa67ccc4a5a8d1`.

On the reviewed commit `5b67004a0eb4557de1020e9f7e968aae974e04aa`, those seven host
commands again exited **0**, with source unchanged. `make check` passed **318
maintained + 141 workflow tests (459 total)**. The separate command
`.venv-agentic/bin/python -B -m unittest discover -s tests -p 'test_flowdc_experiment*.py' -v`
exited **0** with **52 tests**. The public SDK fixture passed all four cases on that
head, and the offline runtime check returned the expected **exit 3** with local
setup guidance and cloud readiness unassessed. This resolved the earlier pending
host-validation note without changing the tested commit.

The implementation's pre-review worker checks are:

| Command | Exit / result |
| --- | --- |
| `.venv-agentic/bin/python -B -m unittest discover -s tests -p 'test_flowdc_pilot*.py' -v` | 0; 178 tests after the offline guidance correction |
| `.venv-agentic/bin/python -B -m unittest discover -s tests -p 'test_flowdc_experiment.py' -v` | 0; 35 tests |
| `.venv-agentic/bin/python -B -m unittest discover -s tests -p 'test_flowdc_ops.py' -v` | 0; 80 tests |
| `.venv-agentic/bin/python -B -m unittest discover -s tests -p 'test_flowdc_pilot_diagnostics.py' -v` | 0; 8 tests, including oversized/nonfinite number rejection |
| `make check-agentic` | 0; 141 workflow tests, lint, format and repository configuration |
| Targeted `ruff check` and `ruff format --check` on the changed Python files (listed in PR handoff) | 0 |
| `git diff --check` | 0 |

The review repair below changes the source after `5b67004`. Final clean-head
`make check`, the complete `test_flowdc_experiment*.py` suite and all five
user-systemd smokes require a fresh run in the coordinator's host environment.
The earlier successful host run does not validate a later head.
The restricted worker cannot create the existing HTTP fixture sockets or reach
the user bus; it does not re-probe those known restrictions or widen permissions.
The upgraded fake-service fixture now includes the diagnostic event in its full
history-preservation assertion through both upgrade and rollback, including the
allowance-smoke path. Exact final SHA, command exit statuses, `make check-clean`,
CI and independent review belong in the PR handoff once available.

The real installed SDK passed the offline transport cases above in the isolated
test runtime. The refused administration interpreter remains unexecuted. These
fixtures do not establish real cloud permission, endpoint availability or deployed
runtime readiness. Deployment/runtime repair remains a later operational checkpoint.
The [first static review](https://github.com/Zi-Deng/FLOW-DC/pull/18#pullrequestreview-5324441601)
applies to `5b67004`; a changed head requires fresh independent review under the
repository's continuation policy.

## Review repair and assessment regressions

The F2 regression overlays the maintained test onto the reviewed source and fails
with exit **1** because `runtime-check` accepts the unused `--state-root` option:

```bash
issue17_review_base=$(mktemp -d /tmp/flowdc-review-base-XXXXXX)
git archive 5b67004a0eb4557de1020e9f7e968aae974e04aa bin tests | tar -x -C "$issue17_review_base"
cp tests/test_flowdc_pilot_offload.py "$issue17_review_base/tests/"
.venv-agentic/bin/python -B -m unittest discover -s "$issue17_review_base/tests" -p 'test_flowdc_pilot_offload.py' -k test_runtime_check_rejects_state_root_and_lifecycle_still_accepts_it -v
```

The repaired parser rejects that option with the existing structured
`invalid_arguments` error / exit **2**, while lifecycle commands retain it.
`.venv-agentic/bin/python -B -m unittest discover -s tests -p 'test_flowdc_pilot_offload.py' -v`
exits **0** with **17 tests** after the repair.
The full focused command
`.venv-agentic/bin/python -B -m unittest discover -s tests -p 'test_flowdc_pilot*.py' -v`
exits **0** with **181 tests** on the repair contents.

For F1, the diagnostic suite already asserted that repeated failures add no
unbounded checkpoint events and leave every pre-existing non-diagnostic event
unchanged. The added alternating-failure test retains older checkpoint events
and counts, exercises the real diagnostic category mapping for 100 alternating
timeout/request failures, verifies the latest top-level code on every write,
and leaves two summaries counted 50 times each. This is bounded evidence, not a
complete chronology of every historical code. The diagnostic-write-failure test
also checks that the latest code and stop survive. Before any production repair,
`.venv-agentic/bin/python -B -m unittest discover -s tests -p 'test_flowdc_pilot_diagnostics.py' -v`
exited **0** with **9 tests**; the supervisor/journal behavior is unchanged.

For F3, an actual SDK child fixture writes secret-canary bytes directly to file
descriptors 1 and 2 after recording its single offload request. Corrupted stdout
produces a fixed `provider_schema` refusal with dispatch uncertainty retained;
stderr noise is discarded. Neither raw output reaches public errors, diagnostics
or status, and neither path changes the journal. Before any production repair,
`.venv-agentic/bin/python -B -m unittest discover -s tests -p 'test_flowdc_pilot_offload.py' -k test_raw_child_fd_output_is_discarded_and_corrupt_stdout_fails_closed -v`
exited **0**. Python stream redirection does not promise descriptor-level isolation;
the existing parent protocol rejects malformed output and retains the output cap.
The production SDK transport is unchanged.

These are V0/V1 software and V2 local fake-service results. They do not reconstruct
the September 25 provider failures, promise a provider-latency bound, establish an
HTTP status for historical generic errors, or constitute a new live/scientific
result. No deployment, cloud mutation, allowance grant, or real task cleanup was
performed. The prior trial's exit-3/recovered-exit-0 history remains unchanged.
