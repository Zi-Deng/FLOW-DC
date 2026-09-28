"""Exercise the real network state machine against a synthetic Neutron backend."""

import copy
import os
import sys
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bin"))

import flowdc_ops as ops
from flowdc_pilot_journal import Journal, register
from flowdc_pilot_provider import Provider, validate_access
from test_flowdc_pilot_lifecycle import NETWORK, PORT_IDS, PROJECT, ROLES, SUBNET, VM_IDS, access, spec


class NetworkBackend(Provider):
    def __init__(self):
        super().__init__({}, before_activation=lambda: None)
        self.groups = {}
        self.fips = {}
        self.ports = {}
        self.calls = []
        self.counter = 100
        self.fail_action = None
        self.lose_response = None
        self.forbid_route = False
        self.external = "88888888-8888-4888-8888-888888888888"
        self.router = "99999999-9999-4999-8999-999999999999"
        self.router_port = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        self.original = "66666666-6666-4666-8666-666666666666"
        self.groups[self.original] = {
            "id": self.original,
            "name": "shared-original",
            "description": "shared",
            "project_id": PROJECT,
            "rules": [],
        }
        for index, (role, vm_id) in enumerate(zip(ROLES, VM_IDS, strict=True)):
            interface = access()["interfaces"][role]
            self.ports[PORT_IDS[index]] = {
                "id": PORT_IDS[index],
                "device_id": vm_id,
                "project_id": PROJECT,
                "network_id": NETWORK,
                "port_security_enabled": True,
                "allowed_address_pairs": [],
                "fixed_ips": [{"ip_address": interface["fixed_ip"], "subnet_id": SUBNET}],
                "security_group_ids": [self.original],
            }

    def new_id(self):
        self.counter += 1
        return f"77777777-7777-4777-8777-{self.counter:012d}"

    def context(self, record):
        pass

    def call(self, action, *args, mutation=False, on_dispatch=None):
        self.validate_call(action, args)
        if on_dispatch is not None:
            on_dispatch()
        self.calls.append((action, args, mutation))
        if action == self.fail_action:
            raise ops.OpsError("network_quota_pending", "fixed", "fixed", 3)
        if action == "groups":
            return [{"ID": key, "Name": value["name"]} for key, value in self.groups.items()]
        if action == "group":
            return copy.deepcopy(self.groups[args[0]])
        if action == "group_create":
            key = self.new_id()
            self.groups[key] = {
                "id": key,
                "name": args[0],
                "description": args[0],
                "project_id": PROJECT,
                "rules": [],
            }
        elif action == "rule":
            self.groups[args[0]]["rules"].append(
                {
                    "direction": "ingress",
                    "protocol": args[1],
                    "remote_ip_prefix": args[2],
                    "ethertype": "IPv4",
                    "port_range_min": 22 if args[3] == "22" else None,
                    "port_range_max": 22 if args[3] == "22" else None,
                }
            )
        elif action == "port":
            return copy.deepcopy(self.ports[args[0]])
        elif action == "ports":
            return [{"ID": key} for key, port in self.ports.items() if port["device_id"] == args[0]]
        elif action == "network":
            if args[0] == self.external:
                return {"id": self.external, "router:external": True}
            return {"id": NETWORK, "project_id": PROJECT}
        elif action == "subnet":
            return {"id": SUBNET, "network_id": NETWORK, "project_id": PROJECT, "host_routes": []}
        elif action == "attach":
            self.ports[args[0]]["security_group_ids"] = list(args[1:])
        elif action == "group_ports":
            return [
                {"ID": key} for key, value in self.ports.items() if args[0] in value["security_group_ids"]
            ]
        elif action == "group_delete":
            del self.groups[args[0]]
        elif action == "floating":
            if self.forbid_route:
                raise ops.OpsError("route_unverified", "fixed", "fixed", 3)
            return [{"ID": key} for key in self.fips]
        elif action == "floating_show":
            return copy.deepcopy(self.fips[args[0]])
        elif action == "router":
            return {
                "id": self.router,
                "project_id": PROJECT,
                "external_gateway_info": {"network_id": self.external},
            }
        elif action == "router_ports":
            self.ports[self.router_port] = {
                "id": self.router_port,
                "network_id": NETWORK,
                "device_id": self.router,
                "fixed_ips": [{"subnet_id": SUBNET}],
                "security_group_ids": [],
            }
            return [{"ID": self.router_port}]
        elif action == "floating_create":
            resource = self.new_id()
            self.fips[resource] = {
                "id": resource,
                "project_id": PROJECT,
                "description": args[0],
                "port_id": args[1],
                "fixed_ip_address": args[2],
                "floating_network_id": args[3],
            }
        elif action == "floating_delete":
            del self.fips[args[0]]
        else:
            raise AssertionError(action)
        if action == self.lose_response:
            raise ops.OpsError("lost_response", "fixed", "fixed", 3)
        return None


class LogicalExecutor(ThreadPoolExecutor):
    """Real workers; advance synthetic time when the actor joins submitted work.

    No batching policy lives here: submission order comes from Provider.read_batch.
    Submitting and joining one request at a time therefore charges serial latency.
    """

    def __init__(self, backend, **kwargs):
        super().__init__(**kwargs)
        self.backend = backend
        self.submitted = backend.arrivals

    def submit(self, *args, **kwargs):
        future = super().submit(*args, **kwargs)
        self.submitted += 1
        executor = self

        class JoinedFuture:
            def result(self):
                backend = executor.backend
                with backend.condition:
                    if not backend.condition.wait_for(lambda: backend.arrivals == executor.submitted, 2):
                        raise AssertionError("logical workers did not arrive")
                    if backend.pending:
                        backend.now = max(end for end, _ in backend.pending)
                        for _, ready in backend.pending:
                            ready.set()
                        backend.pending.clear()
                return future.result(timeout=2)

            def cancel(self):
                return future.cancel()

        return JoinedFuture()


class LatencyBackend(NetworkBackend):
    """Synthetic durations through the inherited production batching loop.

    This is not a replay or cloud measurement, and excludes transport overhead.
    Real worker threads rendezvous at joins instead of sleeping for API latency.
    """

    def __init__(self):
        super().__init__()
        self.now = 100.0
        self.reads = []
        self.actor = threading.get_ident()
        self.condition = threading.Condition()
        self.arrivals = 0
        self.pending = []

    def charge(self, action):
        with self.condition:
            self.reads.append(action)
            end = min(self.now + self.duration(action), self.deadline)
            if threading.get_ident() == self.actor:
                self.now = end
                ready = None
            else:
                ready = threading.Event()
                self.pending.append((end, ready))
                self.arrivals += 1
                self.condition.notify_all()
        if ready is not None and not ready.wait(2):
            raise AssertionError("logical probe was not joined")
        if self.now >= self.deadline:
            raise ops.OpsError("probe_timeout", "synthetic deadline", "synthetic", 1)

    def duration(self, action):
        return 8.22 if action == "ports" else 1.3

    def context(self, record):
        self.charge("context")
        self.charge("project")

    def call(self, action, *args, mutation=False, on_dispatch=None):
        if not mutation:
            self.charge(action)
        return super().call(action, *args, mutation=mutation, on_dispatch=on_dispatch)


class RollbackLatencyBackend(LatencyBackend):
    """Issue #17's synthetic timings, including the shared mutation budget.

    Submission/join order is the production read_batch loop via LogicalExecutor.
    A timed-out mutation has no effect by default; apply_on_timeout separately
    models an applied request whose response does not arrive before the deadline.
    """

    DURATIONS = {
        "context": 0.4,
        "project": 1.1,
        "groups": 1.75,
        "group": 1.7,
        "floating": 2.2,
        "floating_show": 1.7,
        "port": 2.4,
        "group_ports": 2.0,
        "attach": 1.9,
        "group_delete": 1.9,
    }

    def __init__(self):
        super().__init__()
        self.dispatches = []
        self.apply_on_timeout = False
        self.durations = dict(self.DURATIONS)

    def duration(self, action):
        return self.durations[action]

    def call(self, action, *args, mutation=False, on_dispatch=None):
        started = self.now
        if mutation:
            if threading.get_ident() != self.actor or self.pending:
                raise AssertionError("mutation outside actor or before read batch joined")
            if started < self.deadline:
                self.dispatches.append((action, args))
        try:
            self.charge(action)
        except ops.OpsError:
            if mutation and self.apply_on_timeout and started < self.deadline:
                NetworkBackend.call(self, action, *args, mutation=True, on_dispatch=on_dispatch)
            raise
        return NetworkBackend.call(self, action, *args, mutation=mutation, on_dispatch=on_dispatch)


class ReadBatchTests(unittest.TestCase):
    def test_four_read_barrier_and_actor_join(self):
        provider = Provider({})
        barrier = threading.Barrier(4, timeout=2)
        lock = threading.Lock()
        active = maximum = 0
        actor = threading.get_ident()
        workers = set()

        def read(action, resource):
            nonlocal active, maximum
            with lock:
                active += 1
                maximum = max(maximum, active)
                workers.add(threading.get_ident())
            barrier.wait()
            with lock:
                active -= 1
            return resource

        requests = [("group", PROJECT)] * 8
        with provider.step(), patch.object(provider, "call", side_effect=read):
            self.assertEqual(provider.read_batch(requests), [PROJECT] * 8)
        self.assertEqual(maximum, 4)
        self.assertEqual(active, 0)
        self.assertNotIn(actor, workers)

    def test_mutation_rejected_before_any_submission(self):
        provider = Provider({})
        with provider.step(), patch.object(provider, "call") as call:
            with self.assertRaises(ops.OpsError):
                provider.read_batch([("group", PROJECT), ("group_delete", PROJECT)])
        call.assert_not_called()

    def test_every_batched_request_revalidates_credentials(self):
        from contextlib import nullcontext

        provider = Provider({"openstack_client": "/synthetic", "credential_file": "/synthetic"})
        with (
            provider.step(),
            patch.object(ops, "validate_client") as client,
            patch.object(ops, "private_file", side_effect=lambda path: nullcontext(0)) as credential,
            patch.object(ops, "run_bounded", return_value=(0, b"{}")) as runner,
        ):
            self.assertEqual(provider.read_batch([("group", PROJECT)] * 4), [{}, {}, {}, {}])
        self.assertEqual(client.call_count, 4)
        self.assertEqual(credential.call_count, 4)
        self.assertEqual(runner.call_count, 4)
        with (
            provider.step(),
            patch.object(ops, "validate_client"),
            patch.object(ops, "private_file", side_effect=PermissionError()),
            patch.object(ops, "run_bounded") as runner,
            self.assertRaises(PermissionError),
        ):
            provider.read_batch([("group", PROJECT)] * 4)
        runner.assert_not_called()

    def test_expired_batch_does_not_submit(self):
        provider = Provider({})
        with patch.object(provider, "call") as call, self.assertRaises(ops.OpsError):
            provider.read_batch([("group", PROJECT)])
        call.assert_not_called()

    def test_timed_out_batch_joins_and_reaps_all_children(self):
        provider = Provider({})
        children = []
        lock = threading.Lock()
        barrier = threading.Barrier(4, timeout=2)
        real_popen = ops.subprocess.Popen

        def spawn(*args, **kwargs):
            child = real_popen(*args, **kwargs)
            with lock:
                children.append(child)
            return child

        def read(action, resource):
            barrier.wait()
            return ops.run_bounded(
                [sys.executable, "-c", "import time; time.sleep(10)"],
                timeout=provider.deadline - time.monotonic(),
            )

        with (
            provider.step(),
            patch.object(provider, "call", side_effect=read),
            patch.object(ops.subprocess, "Popen", side_effect=spawn),
        ):
            provider.deadline = time.monotonic() + 0.2
            with self.assertRaises(ops.OpsError) as caught:
                provider.read_batch([("group", PROJECT)] * 8)
        self.assertEqual(caught.exception.code, "probe_timeout")
        self.assertEqual(len(children), 4)
        for child in children:
            self.assertIsNotNone(child.returncode)
            self.assertTrue(child.stdout.closed)
            self.assertTrue(child.stderr.closed)


class FloatingReadTests(unittest.TestCase):
    def setUp(self):
        self.provider = NetworkBackend()
        self.record = {"spec": {"context": {"project_id": PROJECT}}}

    def populate(self, count):
        self.provider.fips.clear()
        for index in range(count):
            resource = f"bbbbbbbb-bbbb-4bbb-8bbb-{index:012d}"
            self.provider.fips[resource] = {"id": resource, "project_id": PROJECT}

    def test_zero_one_multiple_and_maximum_floating_collections(self):
        for count in (0, 1, 3, 4, 5, 128):
            with self.subTest(count=count), self.provider.step():
                self.populate(count)
                expected = copy.deepcopy(list(self.provider.fips.values()))
                self.assertEqual(self.provider.floating(self.record), expected)
                self.assertEqual(list(self.provider.fips.values()), expected)
        self.assertFalse(any(mutation for _, _, mutation in self.provider.calls))

    def test_floating_details_use_four_workers_and_validate_on_actor(self):
        self.populate(8)
        barrier = threading.Barrier(4, timeout=2)
        lock = threading.Lock()
        active = maximum = 0
        actor = threading.get_ident()
        readers, validators = set(), []
        original_call = self.provider.call
        original_uuid = ops.uuid_value

        def call(action, *args, **kwargs):
            nonlocal active, maximum
            if action == "floating_show":
                with lock:
                    readers.add(threading.get_ident())
                    active += 1
                    maximum = max(maximum, active)
                barrier.wait()
                with lock:
                    active -= 1
            return original_call(action, *args, **kwargs)

        def validate(value):
            if value == PROJECT:
                validators.append(threading.get_ident())
            return original_uuid(value)

        with (
            self.provider.step(),
            patch.object(self.provider, "call", side_effect=call),
            patch.object(ops, "uuid_value", side_effect=validate),
        ):
            self.assertEqual(self.provider.floating(self.record), list(self.provider.fips.values()))
        self.assertEqual((maximum, active), (4, 0))
        self.assertNotIn(actor, readers)
        self.assertEqual(validators, [actor] * 8)

    def test_invalid_collection_refused_before_detail_submission(self):
        self.populate(129)
        rows = [{"ID": key} for key in self.provider.fips]
        for invalid in (rows, rows[:1] * 2, {}, [None], [{"ID": "invalid"}], [{}]):
            with self.subTest(rows=invalid), self.provider.step():
                with patch.object(self.provider, "call", return_value=invalid) as call:
                    with self.assertRaises(ops.OpsError):
                        self.provider.floating(self.record)
                self.assertEqual(call.call_count, 1)

    def test_wrong_and_missing_detail_identities_fail_closed(self):
        self.populate(3)
        resource = next(iter(self.provider.fips))
        for invalid in (
            {"id": PORT_IDS[0], "project_id": PROJECT},
            {"id": resource, "project_id": PORT_IDS[0]},
            {"id": resource},
            {"project_id": PROJECT},
            {},
        ):
            with self.subTest(detail=invalid), self.provider.step():
                self.provider.fips[resource] = invalid
                with self.assertRaises(ops.OpsError):
                    self.provider.floating(self.record)
        self.assertFalse(any(mutation for _, _, mutation in self.provider.calls))


class NetworkTests(unittest.TestCase):
    def setUp(self):
        mask = os.umask(0o077)
        self.addCleanup(os.umask, mask)
        temp = tempfile.TemporaryDirectory(prefix="flowdc-network-test-")
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        config = self.root / "config"
        config.mkdir(mode=0o700)
        profile = config / "profile.json"
        profile.write_text("{}")
        self.journal = register(profile, self.root / "state", spec(), access())
        self.journal.change(
            lambda record: record.update(desired="run", window={"seconds": 1800, "inspection": True})
        )
        self.provider = NetworkBackend()

    def setup_network(self):
        for _ in range(60):
            self.provider.network_step(self.journal, rollback=False)
            if self.journal.read()["network"]["ready"]:
                return
        self.fail("network setup never finished")

    def rollback(self):
        self.journal.change(lambda record: record.update(desired="stop"))
        for _ in range(20):
            self.provider.network_step(self.journal, rollback=True)
            if self.journal.read()["network"]["rolled_back"]:
                return
        self.fail("rollback never finished")

    def slow_rollback_fixture(self):
        self.setup_network()
        backend = RollbackLatencyBackend()
        backend.groups = copy.deepcopy(self.provider.groups)
        backend.ports = copy.deepcopy(self.provider.ports)
        for index in range(3):
            resource = f"bbbbbbbb-bbbb-4bbb-8bbb-{index:012d}"
            backend.fips[resource] = {
                "id": resource,
                "project_id": PROJECT,
                "description": "unrelated-floating-entry",
                "port_id": PROJECT,
                "fixed_ip_address": f"10.1.0.{index + 1}",
                "floating_network_id": backend.external,
            }
        self.journal.change(lambda record: record.update(desired="stop"))
        return backend

    def timed_rollback_step(self, backend):
        start = backend.now
        before = len(backend.dispatches)
        error = None
        with (
            patch("flowdc_pilot_provider.time.monotonic", side_effect=lambda: backend.now),
            patch(
                "flowdc_pilot_provider.ThreadPoolExecutor",
                side_effect=lambda **kw: LogicalExecutor(backend, **kw),
            ),
        ):
            try:
                backend.network_step(self.journal, rollback=True)
            except ops.OpsError as exc:
                error = exc.code
        self.assertAlmostEqual(backend.deadline - start, 20)
        self.assertLessEqual(backend.now, backend.deadline)
        self.assertLessEqual(len(backend.dispatches) - before, 1)
        self.assertEqual(backend.pending, [])
        return round(backend.now - start, 2), error

    def test_slow_rollback_completes_with_three_unrelated_floating_ips(self):
        backend = self.slow_rollback_fixture()
        original_fips = copy.deepcopy(backend.fips)
        original_group = copy.deepcopy(backend.groups[backend.original])
        trace = []
        for _ in range(10):
            trace.append(self.timed_rollback_step(backend))
            if self.journal.read()["network"]["rolled_back"]:
                break
        self.assertTrue(self.journal.read()["network"]["rolled_back"], trace)
        self.assertEqual(trace, [(13.15, None), (15.15, None)] * 3 + [(9.55, None)])
        self.assertEqual(backend.fips, original_fips)
        self.assertEqual(backend.groups, {backend.original: original_group})
        self.assertTrue(
            all(port["security_group_ids"] == [backend.original] for port in backend.ports.values())
        )
        self.assertEqual([action for action, _ in backend.dispatches], ["attach", "group_delete"] * 3)

    def test_slow_rollback_fails_when_new_read_batches_are_serialized(self):
        backend = self.slow_rollback_fixture()
        original_batch = backend.read_batch

        def serial_new_reads(requests):
            if requests and requests[0][0] in ("port", "floating_show"):
                return [backend.call(*request) for request in requests]
            return original_batch(requests)

        with patch.object(backend, "read_batch", side_effect=serial_new_reads):
            trace = [self.timed_rollback_step(backend) for _ in range(10)]
        self.assertEqual(trace, [(20.0, "probe_timeout")] * 10)
        self.assertFalse(self.journal.read()["network"]["rolled_back"])
        self.assertEqual(len(backend.groups), 4)
        self.assertFalse(any(mutation for _, _, mutation in backend.calls))

    def test_rollback_read_timeout_never_dispatches_mutation(self):
        backend = self.slow_rollback_fixture()
        for action in ("floating_show", "port"):
            with self.subTest(action=action):
                backend.durations = dict(backend.DURATIONS, **{action: 25})
                self.assertEqual(self.timed_rollback_step(backend), (20.0, "probe_timeout"))
                self.assertEqual(backend.dispatches, [])
                self.assertFalse(self.journal.read()["network"]["rolled_back"])

    def test_rollback_mutation_timeout_requires_fresh_proof_after_restart(self):
        backend = self.slow_rollback_fixture()
        original_fips = copy.deepcopy(backend.fips)
        for action in ("attach", "group_delete"):
            # The no-effect case must remain pending. A later request may apply
            # but lose its response; neither return path is rollback proof.
            for applied in (False, True):
                with self.subTest(action=action, applied=applied):
                    backend.durations = dict(backend.DURATIONS, **{action: 25})
                    backend.apply_on_timeout = applied
                    before = copy.deepcopy((backend.groups, backend.ports))
                    count = len(backend.dispatches)
                    self.assertEqual(self.timed_rollback_step(backend), (20.0, "probe_timeout"))
                    self.assertEqual(len(backend.dispatches), count + 1)
                    self.assertEqual(backend.dispatches[-1][0], action)
                    self.assertEqual(before == (backend.groups, backend.ports), not applied)
                    self.assertFalse(self.journal.read()["network"]["rolled_back"])
        restarted = RollbackLatencyBackend()
        restarted.groups = copy.deepcopy(backend.groups)
        restarted.ports = copy.deepcopy(backend.ports)
        restarted.fips = copy.deepcopy(backend.fips)
        self.journal = Journal(self.journal.root)
        for _ in range(10):
            self.assertIsNone(self.timed_rollback_step(restarted)[1])
            if self.journal.read()["network"]["rolled_back"]:
                break
        self.assertTrue(self.journal.read()["network"]["rolled_back"])
        self.assertEqual(restarted.fips, original_fips)
        self.assertEqual([action for action, _ in restarted.dispatches], ["attach", "group_delete"] * 2)

    def test_rollback_validates_all_batched_port_identities_before_mutation(self):
        self.setup_network()
        # The first role would be restorable, but another selected VM has drifted.
        self.provider.ports[PORT_IDS[-1]]["device_id"] = PROJECT
        self.provider.calls.clear()
        with self.assertRaises(ops.OpsError) as caught:
            self.rollback()
        self.assertEqual(caught.exception.code, "selected_port_identity_mismatch")
        self.assertEqual(sum(action == "port" for action, _, _ in self.provider.calls), 3)
        self.assertFalse(any(mutation for _, _, mutation in self.provider.calls))

    def test_slow_port_read_prepares_rule_within_shared_deadline(self):
        self.setup_network()
        backend = LatencyBackend()
        backend.groups = copy.deepcopy(self.provider.groups)
        backend.ports = copy.deepcopy(self.provider.ports)
        record = self.journal.read()
        manager = record["network"]["seen_groups"]["manager"]
        backend.groups[manager]["rules"] = []

        def pending(current):
            current["network"]["configured"].remove("manager")
            current["network"]["ready"] = False
            current["network"]["intents"] = {
                key: value
                for key, value in current["network"]["intents"].items()
                if not key.startswith("rule-manager-")
            }

        self.journal.change(pending)
        with (
            patch("flowdc_pilot_provider.time.monotonic", side_effect=lambda: backend.now),
            patch(
                "flowdc_pilot_provider.ThreadPoolExecutor",
                side_effect=lambda **kw: LogicalExecutor(backend, **kw),
            ),
        ):
            backend.network_step(self.journal, rollback=False)
        self.assertLess(backend.now, backend.deadline)
        self.assertAlmostEqual(backend.now - 100.0, 14.72)
        self.assertCountEqual(
            backend.reads,
            [
                "context",
                "project",
                "groups",
                "group",
                "group",
                "group",
                "ports",
                "port",
                "network",
                "subnet",
                "group",
            ],
        )
        mutations = [call for call in backend.calls if call[2]]
        self.assertEqual(len(mutations), 1)
        self.assertEqual(mutations[0][0], "rule")

    def test_maintenance_reads_verify_actual_rollback_without_mutation(self):
        self.setup_network()
        self.rollback()
        self.provider.calls.clear()
        with patch.object(self.provider, "server", return_value="SHELVED_OFFLOADED"):
            self.provider.verify_idle(self.journal.read())
            self.assertFalse(any(call[2] for call in self.provider.calls))
            self.provider.ports[PORT_IDS[0]]["security_group_ids"] = []
            with self.assertRaises(ops.OpsError) as caught:
                self.provider.verify_idle(self.journal.read())
        self.assertEqual(caught.exception.code, "maintenance_network_rollback_required")
        with patch.object(self.provider, "server", return_value="ACTIVE"):
            with self.assertRaises(ops.OpsError) as caught:
                self.provider.verify_idle(self.journal.read())
        self.assertEqual(caught.exception.code, "initial_offload_required")
        self.assertFalse(any(call[2] for call in self.provider.calls))

    def test_failed_group_batch_and_stop_during_topology_never_mutate(self):
        self.setup_network()
        manager = self.journal.read()["network"]["seen_groups"]["manager"]
        self.provider.groups[manager]["rules"] = []

        def pending(current):
            current["network"]["configured"].remove("manager")
            current["network"]["ready"] = False
            current["network"]["intents"] = {
                key: value
                for key, value in current["network"]["intents"].items()
                if not key.startswith("rule-manager-")
            }

        self.journal.change(pending)
        self.provider.calls.clear()
        self.provider.fail_action = "group"
        with self.assertRaises(ops.OpsError):
            self.provider.network_step(self.journal, rollback=False)
        self.assertFalse(any(call[2] for call in self.provider.calls))
        self.provider.fail_action = None
        original = self.provider.call

        def stop(action, *args, **kwargs):
            value = original(action, *args, **kwargs)
            if action == "ports":
                self.journal.change(lambda r: r.update(desired="stop"))
            return value

        with patch.object(self.provider, "call", side_effect=stop):
            with self.assertRaises(ops.OpsError) as caught:
                self.provider.network_step(self.journal, rollback=False)
        self.assertEqual(caught.exception.code, "stop_requested")
        self.assertFalse(any(call[2] for call in self.provider.calls))
        self.assertFalse(
            any(key.startswith("rule-manager-") for key in self.journal.read()["network"]["intents"])
        )

    def test_real_runner_dispatch_provenance(self):
        import signal
        import time
        from contextlib import nullcontext

        real_popen = ops.subprocess.Popen
        for mode in ("expired", "sigchld", "spawn", "timeout"):
            with self.subTest(mode=mode):
                self.journal.change(lambda r: r["network"]["intents"].clear())
                self.journal.change(lambda r: r.update(desired="run"))
                provider = Provider({"openstack_client": "/synthetic", "credential_file": "/synthetic"})
                provider.deadline = time.monotonic() + (0.05 if mode == "timeout" else 20)
                if mode == "expired":
                    provider.deadline = 0
                marker = "flowdc-" + self.journal.read()["network"]["generation"] + "-manager"

                def spawn(*args, mode=mode, **kwargs):
                    if mode == "spawn":
                        raise OSError("synthetic-private")
                    return real_popen([sys.executable, "-c", "import time; time.sleep(10)"], **kwargs)

                with (
                    patch.object(ops, "validate_client"),
                    patch.object(ops, "private_file", return_value=nullcontext(0)),
                    patch.object(
                        ops.signal,
                        "getsignal",
                        return_value=signal.SIG_IGN if mode == "sigchld" else signal.SIG_DFL,
                    ),
                    patch.object(ops.subprocess, "Popen", side_effect=spawn) as child,
                ):
                    with self.assertRaises(ops.OpsError):
                        provider.network_intent(self.journal, "group-manager", "group_create", marker)
                self.assertEqual(child.call_count, int(mode in ("spawn", "timeout")))
                intent = self.journal.read()["network"]["intents"]["group-manager"]
                self.assertEqual(intent["not_sent"], mode != "timeout")
                if mode == "timeout":
                    with self.assertRaises(ops.OpsError) as caught:
                        self.rollback()
                    self.assertEqual(caught.exception.code, "unresolved_network_creation")
                else:
                    self.rollback()
                    self.assertTrue(self.journal.read()["network"]["rolled_back"])

    def test_runner_callback_failure_reaps_child(self):
        child = None
        real_popen = ops.subprocess.Popen

        def spawn(*args, **kwargs):
            nonlocal child
            child = real_popen(*args, **kwargs)
            return child

        def callback():
            raise RuntimeError("callback failed")

        with patch.object(ops.subprocess, "Popen", side_effect=spawn):
            with self.assertRaises(RuntimeError):
                ops.run_bounded(
                    [sys.executable, "-c", "import time; time.sleep(10)"], timeout=1, on_dispatch=callback
                )
        self.assertIsNotNone(child.returncode)
        self.assertTrue(child.stdout.closed)
        self.assertTrue(child.stderr.closed)

    def test_router_connection_is_order_independent(self):
        self.configure_floating()
        original = self.provider.call
        extra = PROJECT

        for order in ("match_first", "match_last", "none"):

            def call(action, *args, order=order, **kwargs):
                if action == "router_ports":
                    result = original(action, *args, **kwargs)
                    self.provider.ports[extra] = dict(
                        self.provider.ports[self.provider.router_port],
                        id=extra,
                        fixed_ips=[{"subnet_id": PROJECT, "ip_address": "10.1.0.1"}],
                    )
                    if order == "none":
                        return [{"ID": extra}]
                    return result + [{"ID": extra}] if order == "match_first" else [{"ID": extra}] + result
                return original(action, *args, **kwargs)

            with (
                self.subTest(order=order),
                self.provider.step(),
                patch.object(self.provider, "call", side_effect=call),
            ):
                if order == "none":
                    with self.assertRaises(ops.OpsError) as caught:
                        self.provider.route_step(self.journal, self.journal.read(), inspect_only=True)
                    self.assertEqual(caught.exception.code, "router_not_connected")
                else:
                    self.provider.route_step(self.journal, self.journal.read(), inspect_only=True)

    def test_creation_dispatch_boundary_and_recovered_rollback(self):
        from contextlib import nullcontext

        for mode in ("client", "credential", "guard", "dispatched"):
            with self.subTest(mode=mode):
                self.journal.change(lambda r: r["network"]["intents"].clear())
                self.journal.change(lambda r: r.update(desired="run"))
                provider = Provider({"openstack_client": "/absent", "credential_file": "/absent"})
                provider.activating = True
                provider.before_activation = lambda: None
                marker = "flowdc-" + self.journal.read()["network"]["generation"] + "-manager"
                error = ops.OpsError("provider_timeout", "fixed", "fixed", 3)
                if mode == "guard":
                    provider.before_activation = None

                def dispatched_runner(*args, error=error, **kwargs):
                    kwargs["on_dispatch"]()
                    raise error

                with (
                    patch.object(
                        ops, "validate_client", side_effect=FileNotFoundError() if mode == "client" else None
                    ),
                    patch.object(
                        ops,
                        "private_file",
                        side_effect=PermissionError() if mode == "credential" else None,
                        return_value=nullcontext(123),
                    ),
                    patch.object(ops, "run_bounded", side_effect=dispatched_runner) as runner,
                ):
                    with self.assertRaises((ops.OpsError, OSError)):
                        provider.network_intent(self.journal, "group-manager", "group_create", marker)
                intent = self.journal.read()["network"]["intents"]["group-manager"]
                self.assertEqual(intent["not_sent"], mode != "dispatched")
                self.assertEqual(runner.call_count, int(mode == "dispatched"))
                if mode != "dispatched":
                    self.rollback()
                    self.assertTrue(self.journal.read()["network"]["rolled_back"])
                else:
                    with self.assertRaises(ops.OpsError) as caught:
                        self.rollback()
                    self.assertEqual(caught.exception.code, "unresolved_network_creation")
                    self.assertFalse(self.journal.read()["network"]["rolled_back"])

    def test_unsent_checkpoint_failure_preserves_original_and_ambiguous_intent(self):
        provider = Provider({"openstack_client": "/absent", "credential_file": "/absent"})
        marker = "flowdc-" + self.journal.read()["network"]["generation"] + "-manager"
        change = self.journal.change
        calls = 0

        def busy_result(update):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise ops.OpsError("pilot_state_busy", "fixed", "fixed", 3)
            return change(update)

        original = FileNotFoundError("synthetic")
        with (
            patch.object(self.journal, "change", side_effect=busy_result),
            patch.object(ops, "validate_client", side_effect=original),
        ):
            with self.assertRaises(FileNotFoundError) as caught:
                provider.network_intent(self.journal, "group-manager", "group_create", marker)
        self.assertIs(caught.exception, original)
        self.assertNotIn("not_sent", self.journal.read()["network"]["intents"]["group-manager"])

    def test_project_filters_use_compact_uuid_with_positive_results(self):
        self.configure_floating()
        self.setup_network()
        for value in (*self.provider.groups.values(), *self.provider.fips.values()):
            value["project_id"] = PROJECT.replace("-", "")
        record = self.journal.read()
        self.assertEqual(len(self.provider.owned_groups(record)), 3)
        self.assertEqual(len(self.provider.floating(record)), 1)
        for action, args, _ in self.provider.calls:
            if action in ("groups", "floating"):
                self.assertEqual(args, (PROJECT.replace("-", ""),))
        self.rollback()

    def test_cleanup_does_not_depend_on_missing_or_drifted_port(self):
        record = self.journal.read()
        for action, state in (("shelve", "ACTIVE"), ("offload", "SHELVED")):
            with (
                self.subTest(action=action),
                patch.object(
                    ops, "cloud_query", return_value={"id": VM_IDS[0], "project_id": PROJECT, "status": state}
                ),
                patch.object(self.provider, "port", side_effect=ops.invalid_path()) as port,
                patch.object(self.provider, "call") as call,
            ):
                self.provider.lifecycle(record, VM_IDS[0], action)
                port.assert_not_called()
                call.assert_called_once_with(action, VM_IDS[0], mutation=True)

    def test_setup_restricts_selected_ports_and_rollback_preserves_shared_group(self):
        shared = copy.deepcopy(self.provider.groups[self.provider.original])
        self.setup_network()
        for role, interface in access()["interfaces"].items():
            group_id = self.provider.ports[interface["port_id"]]["security_group_ids"][0]
            rules = self.provider.groups[group_id]["rules"]
            self.assertEqual(len(rules), 7 if role == "manager" else 6)
            for rule in rules:
                self.assertTrue(rule["remote_ip_prefix"].endswith("/32"))
                if rule["remote_ip_prefix"] == access()["operator_cidr"]:
                    self.assertEqual((role, rule["protocol"], rule["port_range_min"]), ("manager", "tcp", 22))
        self.rollback()
        self.assertEqual(self.provider.groups, {self.provider.original: shared})
        self.assertTrue(
            all(
                port["security_group_ids"] == [self.provider.original]
                for port in self.provider.ports.values()
            )
        )

    def test_rules_changed_after_setup_are_rejected_before_activation(self):
        self.setup_network()
        record = self.journal.read()
        resource = record["network"]["seen_groups"]["manager"]
        self.provider.verify_ingress(record, "manager", self.provider.groups[resource])
        self.provider.groups[resource]["rules"].append(
            {
                "direction": "ingress",
                "protocol": "tcp",
                "remote_ip_prefix": "0.0.0.0/0",
                "port_range_min": 80,
                "port_range_max": 80,
                "ethertype": "IPv4",
            }
        )
        with self.assertRaises(ops.OpsError):
            self.provider.verify_ingress(record, "manager", self.provider.groups[resource])

    def test_compact_project_uuid_is_verified_by_identity(self):
        for port in self.provider.ports.values():
            port["project_id"] = PROJECT.replace("-", "")
        self.setup_network()
        self.rollback()

    def test_routes_are_checked_before_any_mutation(self):
        self.provider.forbid_route = True
        with self.assertRaises(ops.OpsError):
            self.provider.network_step(self.journal, rollback=False)
        self.assertFalse(any(mutation for _, _, mutation in self.provider.calls))

    def test_wrong_selected_port_or_additional_interface_never_mutates(self):
        self.provider.ports[PORT_IDS[0]]["device_id"] = VM_IDS[1]
        with self.assertRaises(ops.OpsError):
            for _ in range(3):
                self.provider.network_step(self.journal, rollback=False)
        self.assertFalse(any(mutation for _, _, mutation in self.provider.calls))

    def test_lost_creation_response_is_discovered_and_never_duplicated(self):
        self.provider.lose_response = "group_create"
        with self.assertRaises(ops.OpsError):
            for _ in range(4):
                self.provider.network_step(self.journal, rollback=False)
        self.provider.lose_response = None
        self.setup_network()
        self.assertEqual(sum(action == "group_create" for action, _, _ in self.provider.calls), 3)
        self.rollback()

    def test_unknown_creation_result_without_resource_is_not_replayed(self):
        self.provider.fail_action = "group_create"
        with self.assertRaises(ops.OpsError):
            for _ in range(4):
                self.provider.network_step(self.journal, rollback=False)
        self.provider.fail_action = None
        with self.assertRaises(ops.OpsError):
            self.provider.network_step(self.journal, rollback=False)
        self.assertEqual(sum(action == "group_create" for action, _, _ in self.provider.calls), 1)

    def test_rollback_waits_for_late_creation_then_reconciles_after_restart(self):
        self.provider.fail_action = "group_create"
        with self.assertRaises(ops.OpsError):
            self.setup_network()
        self.provider.fail_action = None
        with self.assertRaises(ops.OpsError) as caught:
            self.rollback()
        self.assertEqual(caught.exception.code, "unresolved_network_creation")
        self.assertFalse(self.journal.read()["network"]["rolled_back"])
        # Simulate the previously dispatched create becoming visible later.
        marker = "flowdc-" + self.journal.read()["network"]["generation"] + "-manager"
        resource = self.provider.new_id()
        self.provider.groups[resource] = {
            "id": resource,
            "name": marker,
            "description": marker,
            "project_id": PROJECT,
            "rules": [],
        }
        self.journal = Journal(self.journal.root)
        self.rollback()
        self.assertEqual(sum(action == "group_create" for action, _, _ in self.provider.calls), 1)
        self.assertNotIn(resource, self.provider.groups)
        self.assertTrue(self.journal.read()["network"]["rolled_back"])

    def test_external_attachment_change_blocks_rollback(self):
        self.setup_network()
        self.provider.ports[PORT_IDS[0]]["security_group_ids"].append(self.provider.original)
        with self.assertRaises(ops.OpsError):
            self.rollback()
        self.assertFalse(any(action == "group_delete" for action, _, _ in self.provider.calls))

    def test_unrelated_vm_using_owned_group_prevents_group_deletion(self):
        self.setup_network()
        group = self.provider.ports[PORT_IDS[0]]["security_group_ids"][0]
        unrelated = copy.deepcopy(self.provider.ports[PORT_IDS[0]])
        unrelated["device_id"] = PROJECT
        self.provider.ports[PROJECT] = unrelated
        with self.assertRaises(ops.OpsError):
            self.rollback()
        self.assertIn(group, self.provider.groups)
        self.assertEqual(self.provider.ports[PROJECT], unrelated)

    def configure_floating(self):
        facts = access()
        facts["route"] = {
            "mode": "floating",
            "external_network_id": self.provider.external,
            "router_id": self.provider.router,
            "operator_route_verified": True,
        }
        config = self.root / "floating-config"
        config.mkdir(mode=0o700)
        profile = config / "profile.json"
        profile.write_text("{}")
        self.journal = register(profile, self.root / "floating-state", spec(), facts)
        self.journal.change(
            lambda record: record.update(desired="run", window={"seconds": 1800, "inspection": True})
        )

    def test_only_one_manager_floating_entry_is_created_and_removed(self):
        self.configure_floating()
        self.setup_network()
        self.assertEqual(len(self.provider.fips), 1)
        value = next(iter(self.provider.fips.values()))
        self.assertEqual(value["port_id"], PORT_IDS[0])
        self.rollback()
        self.assertEqual(self.provider.fips, {})
        self.assertEqual(sum(action == "floating_create" for action, _, _ in self.provider.calls), 1)

    def test_existing_manager_floating_entry_is_reused_and_never_deleted(self):
        self.configure_floating()
        resource = self.provider.new_id()
        original = {
            "id": resource,
            "project_id": PROJECT,
            "description": "operator-owned",
            "port_id": PORT_IDS[0],
            "fixed_ip_address": "10.0.0.10",
            "floating_network_id": self.provider.external,
        }
        self.provider.fips[resource] = copy.deepcopy(original)
        self.setup_network()
        self.rollback()
        self.assertEqual(self.provider.fips, {resource: original})
        self.assertFalse(
            any(action.startswith("floating_") and mutation for action, _, mutation in self.provider.calls)
        )

    def test_lost_floating_response_reconciles_by_recorded_marker(self):
        self.configure_floating()
        self.provider.lose_response = "floating_create"
        with self.assertRaises(ops.OpsError):
            self.setup_network()
        self.provider.lose_response = None
        self.setup_network()
        self.assertEqual(sum(action == "floating_create" for action, _, _ in self.provider.calls), 1)
        self.rollback()

    def test_existing_worker_public_entry_refuses_setup_before_mutation(self):
        self.configure_floating()
        resource = self.provider.new_id()
        self.provider.fips[resource] = {
            "id": resource,
            "project_id": PROJECT,
            "description": "existing",
            "port_id": PORT_IDS[1],
        }
        with self.assertRaises(ops.OpsError):
            self.provider.network_step(self.journal, rollback=False)
        self.assertFalse(any(mutation for _, _, mutation in self.provider.calls))

    def test_access_facts_reject_public_peer_broad_operator_and_unknown_fields(self):
        path = self.root / "access.json"
        import json

        for change in (
            lambda value: value.update(operator_cidr="0.0.0.0/0"),
            lambda value: value["interfaces"]["worker"].update(fixed_ip="8.8.8.8"),
            lambda value: value.update(unknown=True),
        ):
            value = access()
            change(value)
            path.write_text(json.dumps(value))
            with self.assertRaises(ops.OpsError):
                validate_access(str(path))

    def test_adapter_validates_all_argv_before_starting_subprocess(self):
        provider = Provider({})
        for action, args in (
            ("delete", [VM_IDS[0]]),
            ("shelve", ["--all"]),
            ("attach", [PORT_IDS[0], "bad"]),
            ("rule", [PROJECT, "tcp", "0.0.0.0/0", "22", "bad"]),
            ("group_create", ["unrelated"]),
        ):
            with self.subTest(action=action), self.assertRaises(ops.OpsError):
                provider.call(action, *args, mutation=True)


if __name__ == "__main__":
    unittest.main()
