"""Offline guest shared-profile preparation; never VM or trust activation."""

import base64
import copy
import json
import ssl
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bin"))
from flowdc_experiment_data import ExperimentError
from flowdc_experiment_research import origin_plan, prepare, run_case, validate
from flowdc_shared import control_tls_context
from flowdc_vine_protocol import digest


class ResearchGuestTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.url = "http://10.0.0.30:18080/image"
        self.plan = {
            "schema": "flowdc-guest-origin-v1",
            "name": "steady",
            "schedule": [[0, 2]],
            "queue_bound": 4,
            "assignments": ["/image"],
            "objects": {
                "/image": {
                    "payload_base64": base64.b64encode(b"known bytes").decode(),
                    "service_s": 0.05,
                    "responses": [{"status": 200}],
                }
            },
        }
        pl.DataFrame(
            {"url": [self.url, self.url, None], "label": ["first", "duplicate", None]}
        ).write_parquet(self.root / "original.parquet")
        (self.root / "catalog.json").write_text(
            json.dumps({self.url: {"bytes": 11, "sha256": digest(b"known bytes")}})
        )
        (self.root / "origin.json").write_text(json.dumps(self.plan))
        self.value = {
            "manifest": str(self.root / "original.parquet"),
            "catalog": str(self.root / "catalog.json"),
            "origin_plan": str(self.root / "origin.json"),
            "environment_archive": "/home/test/env.tar.gz",
            "environment_sha256": "a" * 64,
            "control_tls": {
                "host": "10.0.0.10",
                "port": 18443,
                "endpoint": "https://10.0.0.10:18443",
                "certfile": "/home/test/server.pem",
                "keyfile": "/home/test/server.key",
                "ca_file": "/home/test/ca.pem",
                "ca_sha256": "b" * 64,
            },
        }
        self.record = {
            "access": {
                "interfaces": {"manager": {"fixed_ip": "10.0.0.10"}, "origin": {"fixed_ip": "10.0.0.30"}}
            }
        }

    def test_independent_catalog_and_original_duplicates_survive_prepare(self):
        before = (self.root / "original.parquet").read_bytes()
        files, truth = prepare(self.value, self.record)
        self.assertEqual(files["research/original.parquet"], before)
        self.assertEqual(truth["original_rows"], 3)
        self.assertEqual(len(truth["rows"]), 3)
        self.assertEqual((self.root / "original.parquet").read_bytes(), before)
        (self.root / "catalog.json").write_text("{}")
        with self.assertRaises(ValueError):
            prepare(self.value, self.record)

    def test_origin_plan_rejects_nonfinite_schedule_service_and_bad_responses(self):
        for mutation in (
            lambda p: p.update(schedule=[[0, 2], [float("nan"), 1]]),
            lambda p: p["objects"]["/image"].update(service_s=0),
            lambda p: p["objects"]["/image"].update(responses=[{"status": 200, "truncate": "false"}]),
            lambda p: p.update(assignments=["/missing"]),
        ):
            value = copy.deepcopy(self.plan)
            mutation(value)
            with self.assertRaises((ValueError, ExperimentError)):
                origin_plan(json.dumps(value))

    def test_guest_queue_overload_policy_is_executable_before_binding(self):
        plan = origin_plan(json.dumps(self.plan))
        self.assertEqual(plan["queue_rejection_status"], 503)
        self.assertEqual(plan["queue_retry_after"], "0.1")
        for update in ({"queue_rejection_status": 200}, {"queue_retry_after": "NaN"}, {"unknown": True}):
            with self.subTest(update=update), self.assertRaises(ExperimentError):
                origin_plan(json.dumps({**self.plan, **update}))

    def test_guest_endpoint_mapping_and_explicit_trust_fail_closed(self):
        bad = copy.deepcopy(self.value)
        bad["control_tls"]["endpoint"] = "http://10.0.0.10:18443"
        with self.assertRaises(ExperimentError):
            validate(bad)
        bad = copy.deepcopy(self.value)
        bad["control_tls"]["host"] = "10.0.0.11"
        bad["control_tls"]["endpoint"] = "https://10.0.0.11:18443"
        with self.assertRaises(ExperimentError):
            prepare(bad, self.record)
        self.assertIs(control_tls_context({}), True)
        with self.assertRaises(ssl.SSLError):
            control_tls_context({"ca_pem": "not a certificate"})

    def test_prepared_nonprefix_selection_reaches_guest_boundaries_and_cleanup(self):
        from dataclasses import asdict
        from itertools import product

        import flowdc_experiment as cli
        import flowdc_experiment_data as data
        from flowdc_experiment_artifacts import bundle, members
        from flowdc_pilot import Allowance
        from flowdc_pilot_topology import apply
        from flowdc_topology import selected_ids, selected_roles
        from test_flowdc_experiment import ExperimentTests
        from test_pilot_topology import TopologyTests

        harness, topology = ExperimentTests(), TopologyTests()
        harness.setUp()
        self.addCleanup(harness.doCleanups)
        topology.setUp()
        self.addCleanup(topology.doCleanups)
        apply(topology.journal, topology.request(4))
        registered = topology.journal.read()
        harness.journal, harness.state = topology.journal, topology.journal.root
        (harness.state / "runs").mkdir(mode=0o700)
        harness.store = data.Store(harness.state)
        enrolled = registered["spec"]["topology"]["workers"]
        harness.value.update(
            schema_version=2,
            state_root=str(harness.state),
            registration_id=registered["registration_id"],
            worker_ids=[enrolled[2]],
        )
        harness.value["cases"] = harness.value["cases"][:1]
        Path(harness.value["cases"][0]["config"]).write_text(
            json.dumps(
                {"enable_paarc": True, "control_method": "fixed-v1", "C_min": 1, "C_init": 2, "C_max": 2}
            )
        )
        public = harness.key.with_suffix(".pub").read_text().split()[:2]
        harness.hosts.write_text(
            "".join(f"flowdc-{vm['role']} {' '.join(public)}\n" for vm in registered["spec"]["vms"])
        )
        url = "http://" + registered["access"]["interfaces"]["origin"]["fixed_ip"] + ":18080/image"
        pl.DataFrame({"url": [url, url, None], "label": ["first", "duplicate", None]}).write_parquet(
            self.root / "original.parquet"
        )
        (self.root / "catalog.json").write_text(
            json.dumps({url: {"bytes": 11, "sha256": digest(b"known bytes")}})
        )
        harness.value["distributed"] = self.value
        for count, failure in product(
            (1, 2, 4), (None, "worker-launch", "no-response", "collection", "origin-stop")
        ):
            with self.subTest(workers=count, failure=failure):
                harness.value["worker_ids"] = {1: [enrolled[2]], 2: [enrolled[1], enrolled[3]], 4: enrolled}[
                    count
                ]
                before = harness.journal.read()
                harness.path.write_bytes(data.encode(harness.value))
                run_id = cli.prepare(harness.path)["run_id"]
                manifest, state = cli.load(harness.store, run_id)
                self.assertEqual(harness.journal.read(), before)
                workers = {
                    1: {"worker-3"},
                    2: {"worker-2", "worker-4"},
                    4: {"worker", "worker-2", "worker-3", "worker-4"},
                }[count]
                expected_roles = {"manager", "origin"} | workers
                self.assertEqual(set(selected_roles(manifest["binding"])), expected_roles)
                staged = members(harness.store.read(run_id, "bundle.tar"), data.LIMIT)
                self.assertEqual(set(json.loads(staged["guest.json"])["addresses"]), expected_roles)
                calls = []
                owner = self

                class Controller:
                    def __init__(self, root, expected, owner=owner, manifest=manifest):
                        owner.assertEqual(expected, manifest["binding"])
                        self.active = False

                    def preflight(self, window):
                        pass

                    def call(self, action, calls=calls, manifest=manifest, **kwargs):
                        calls.append(("controller", action))
                        if action in ("start", "stop"):
                            self.active = action == "start"
                        ids = selected_ids(manifest["binding"])
                        vms = []
                        for vm_id, vm in registered["vms"].items():
                            account = Allowance()
                            active = self.active and vm_id in ids
                            if active:
                                account = account.activation_intent(
                                    topology.clock(), window_seconds=1800, inspection=True
                                )
                            vms.append(
                                dict(
                                    id=vm_id,
                                    role=vm["role"],
                                    account=asdict(account),
                                    phase="requested" if active else "offloaded",
                                    observation_fresh=True,
                                    provider_state="ACTIVE" if active else "SHELVED_OFFLOADED",
                                )
                            )
                        return dict(
                            supervisor_ready=True,
                            desired="run" if self.active else "idle",
                            checkpoint=None,
                            network_ready=self.active,
                            network_rolled_back=not self.active,
                            selected_ids=list(ids),
                            vms=vms,
                        )

                    def addresses(self, seconds, expected_roles=expected_roles):
                        return {
                            role: registered["access"]["interfaces"][role]["fixed_ip"]
                            for role in expected_roles
                        }

                    def containment(self, seconds, calls=calls, expected_roles=expected_roles):
                        calls.append(("controller", "containment"))
                        return {"native_network_contained": True, "roles": sorted(expected_roles)}

                class Transport:
                    def __init__(self, *args):
                        pass

                    def call(
                        self,
                        role,
                        action,
                        case="all",
                        owner=owner,
                        expected_roles=expected_roles,
                        calls=calls,
                        failure=failure,
                        workers=workers,
                        **kwargs,
                    ):
                        owner.assertIn(role, expected_roles)
                        calls.append((role, action, case))
                        if failure == "worker-launch" and role == sorted(workers)[0] and action == "launch":
                            raise data.ExperimentError("lost_worker_launch_ack")
                        if failure == "no-response" and role == "manager" and action == "status":
                            raise data.ExperimentError("manager_status_deadline")
                        if failure == "origin-stop" and role == "origin" and action == "stop":
                            raise data.ExperimentError("origin_stop_uncertain")
                        if failure == "collection" and role == "manager" and action == "collect":
                            raise data.ExperimentError("missing_manager_output")
                        if action == "probe":
                            return {
                                "packages": {
                                    "ndcctools.taskvine": "7.17.2",
                                    "vine_worker": "vine_worker 7.17.2",
                                }
                            }
                        if action == "status":
                            return {"ActiveState": "inactive", "Result": "success", "ExecMainStatus": "0"}
                        if action == "collect":
                            return bundle({role + ".log": b"mock guest receipt"})
                        return {}

                def validate(argv, owner=owner, **kwargs):
                    # This exercises orchestration boundaries, not native artifact validity.
                    owner.assertTrue(argv[-1].endswith("flowdc_experiment_artifacts.py"))
                    return 0, data.encode({"result": {"mock_boundary": True}})

                diagnostics = []
                original_error_code = cli.error_code

                def diagnose(exc, diagnostics=diagnostics, original_error_code=original_error_code):
                    import traceback

                    diagnostics.append("".join(traceback.format_exception(exc)))
                    return original_error_code(exc)

                with (
                    patch.object(cli, "Controller", Controller),
                    patch.object(cli, "Transport", Transport),
                    patch.object(cli, "execute", validate),
                    patch.object(cli, "error_code", diagnose),
                    harness.store.lock(),
                ):
                    cli.run(harness.store, run_id, manifest, state)
                self.assertIn(("controller", "stop"), calls)
                self.assertEqual(state["cleanup"], "verified")
                self.assertEqual(harness.journal.read(), before)
                if failure is None:
                    self.assertEqual(state["workload"], "passed", diagnostics)
                    self.assertEqual(
                        set(state["collected"]),
                        {"paarc-on", "origin-paarc-on"} | {"paarc-on-" + role for role in workers},
                    )
                    self.assertTrue(all(value["valid"] for value in state["collected"].values()))
                else:
                    self.assertTrue(state["errors"])
                    if failure == "collection":
                        self.assertEqual(state["workload"], "failed")
                    # Complete artifacts can survive a lost service acknowledgement;
                    # the retained execution/cleanup errors still prevent success.
                self.assertTrue(
                    all(service.get("stopped") for service in state["services"])
                    if failure != "origin-stop"
                    else state["guest_cleanup"] == "uncertain",
                    state,
                )
                if failure == "origin-stop":
                    self.assertEqual(harness.store.owner(), run_id)
                    # Keep the failed run; release only the fixture's simulated ownership for teardown.
                    with harness.store.lock():
                        harness.store.release(run_id)
                else:
                    self.assertIsNone(harness.store.owner())

    def test_collected_real_native_publication_is_reverified_and_tampering_refused(self):
        import shutil
        from uuid import uuid4

        from flowdc_experiment_research import verify_return
        from flowdc_vine import Reconciler
        from flowdc_vine_protocol import encode, pack_return
        from test_benchmark_contract import KnownTruthFixtures

        fixture = KnownTruthFixtures()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        fixture.flow_fixture()
        rows = [row["row_id"] for row in fixture.truth.record["rows"] if row["eligible"]]
        scope, attempt = uuid4().hex, uuid4().hex
        spec = dict(
            scope_id=scope,
            binding={"method": "fixed-v1"},
            partition_sha256="a" * 64,
            row_ids=rows,
            files={"worker.py": "b" * 64},
            environment_sha256="c" * 64,
        )
        output = self.root / "return"
        output.mkdir()
        shutil.copytree(fixture.native, output / "native")
        shutil.copyfile(fixture.native.parent / "native.tar", output / "native.tar")
        identity = {"schema": "flowdc-vine-attempt-v1", "attempt_id": attempt, **spec}
        identity["source_files"] = identity.pop("files")
        (output / "identity.json").write_bytes(encode(identity))
        (output / "receipt.json").write_bytes(
            encode(
                dict(schema="flowdc-vine-receipt-v1", attempt_id=attempt, scope_id=scope, status="returned")
            )
        )
        archive = self.root / "return.tar"
        pack_return(output, archive)
        clients = {attempt: {"scope": scope}}
        reconciler = Reconciler(fixture.truth.record)
        reconciler.accept(
            self.root / "accepted", archive, spec, {"successful": True, "exit_code": 0}, clients
        )
        self.assertFalse(reconciler.errors, reconciler.returns)
        claimed = dict(
            reconciler.summary(), schema="flowdc-distributed-result-v1", method="fixed-v1", run_complete=True
        )
        files = {
            "distributed/fixture/truth.json": encode(fixture.truth.record),
            "distributed/outcomes.json": encode(claimed),
            "distributed/control.json": encode({"state": {"binding": spec["binding"], "clients": clients}}),
            "distributed/partition-0/task.json": encode(spec),
            "distributed/partition-0/return.tar": archive.read_bytes(),
            "distributed/timing.json": encode({"end_to_end_ns": 123456789}),
        }
        case = {"config": {"control_method": "fixed-v1"}}
        report = verify_return(files, case, fixture.truth.record, spec["files"], spec["environment_sha256"])
        self.assertEqual(report["useful_bytes"], 3 * len(fixture.payload))
        self.assertEqual(report["original_rows"], 6)
        self.assertEqual(report["end_to_end_ns"], 123456789)
        for kind in (
            "source",
            "environment",
            "rows",
            "archive",
            "missing",
            "scope",
            "exit",
            "method",
            "empty",
        ):
            bad = dict(files)
            if kind in ("source", "environment"):
                value = copy.deepcopy(spec)
                value["files" if kind == "source" else "environment_sha256"] = (
                    {} if kind == "source" else "e" * 64
                )
                bad["distributed/partition-0/task.json"] = encode(value)
            if kind in ("rows", "scope", "exit", "method"):
                value = copy.deepcopy(claimed)
                if kind == "rows":
                    value["rows"][0]["useful_bytes"] += 1
                if kind == "scope":
                    value["returns"][0]["scope_id"] = "../../escape"
                if kind == "exit":
                    value["returns"][0]["native"]["successful"] = False
                if kind == "method":
                    value["method"] = "ratio-v1"
                bad["distributed/outcomes.json"] = encode(value)
            if kind == "archive":
                bad["distributed/partition-0/return.tar"] = archive.read_bytes()[:1024]
            if kind == "missing":
                del bad["distributed/partition-0/return.tar"]
            if kind == "empty":
                from benchmark.core.truth import initial_outcomes

                bad["distributed/outcomes.json"] = encode(
                    dict(claimed, returns=[], rows=list(initial_outcomes(fixture.truth.record).values()))
                )
            with (
                self.subTest(kind=kind),
                self.assertRaises((ValueError, KeyError, ExperimentError, tarfile.TarError)),
            ):
                verify_return(bad, case, fixture.truth.record, spec["files"], spec["environment_sha256"])

    def test_all_worker_counts_select_real_shared_manager_profile(self):
        (self.root / "configs").mkdir()
        (self.root / "configs/case.json").write_text(
            json.dumps({"enable_paarc": True, "control_method": "gradient-candidate-v1"})
        )
        for count in (1, 2, 4):
            settings = {
                "distributed": self.value,
                "addresses": {str(i): "10.0.0.1" for i in range(count + 2)},
                "bounds": {"phase_seconds": 150},
            }
            (self.root / "guest.json").write_text(json.dumps(settings))
            with patch("flowdc_vine.cli", return_value=0) as cli, self.assertRaises(SystemExit) as stopped:
                run_case(self.root, "case")
            self.assertEqual(stopped.exception.code, 0)
            cfg = cli.call_args.args[0]
            self.assertEqual(cfg["workers"], count)
            self.assertEqual(cfg["distributed_profile"], "shared-origin-v1")
            self.assertEqual(cfg["control_tls"], self.value["control_tls"])
            self.assertEqual(cfg["download"]["control_method"], "gradient-candidate-v1")
