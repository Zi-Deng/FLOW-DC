"""Offline guest shared-profile preparation; never VM or trust activation."""

import base64
import copy
import json
import ssl
import sys
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
