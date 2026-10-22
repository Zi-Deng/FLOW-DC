"""First-attempt freshness and bounded preparation regressions; no cloud calls."""

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from benchmark.real_server_study import (
    ablation,
    ablation_analysis as analysis,
    ablation_controlled,
    ablation_run,
)
from benchmark.real_server_study.common import file_hash, read_json, write_new


class FinalizationTests(unittest.TestCase):
    def test_stale_audit_never_survives_new_or_changed_first_attempt(self):
        with tempfile.TemporaryDirectory() as t:
            s = Path(t)
            write_new(s / "protocol.json", {"fixture": True})
            with (
                patch.object(ablation, "schedule", return_value=[{"run_id": "one"}]),
                patch.object(ablation_controlled, "cases", return_value=[{"run_id": "lab"}]),
            ):
                value = {
                    "protocol_sha256": file_hash(s / "protocol.json"),
                    "runs": [{"run_id": "one", "status": "unstarted", "audited": False}],
                    "controlled": [{"run_id": "lab", "status": "unstarted", "audited": False}],
                }
                self.assertTrue(analysis.results_current(s, value))
                d = s / "runs/one"
                d.mkdir(parents=True)
                write_new(
                    d / "record.json", {"run_id": "one", "protocol_window_complete": True, "goodput_Bps": 4}
                )
                self.assertFalse(analysis.results_current(s, value))
                value["runs"] = [
                    {
                        "run_id": "one",
                        "protocol_window_complete": True,
                        "goodput_Bps": 4,
                        "status": "complete",
                        "audited": True,
                    }
                ]
                self.assertTrue(analysis.results_current(s, value))
                value["record_inventory"] = analysis.record_inventory(s)
                self.assertTrue(analysis.results_current(s, value))
                (d / "record.json").write_text(
                    json.dumps({"run_id": "one", "protocol_window_complete": True, "goodput_Bps": 5})
                )
                self.assertFalse(analysis.results_current(s, value))
                value.pop("record_inventory")
                self.assertFalse(analysis.results_current(s, value))
                value["runs"][0]["goodput_Bps"] = 5
                self.assertTrue(analysis.results_current(s, value))
                (s / "controlled/lab").mkdir(parents=True)
                self.assertFalse(analysis.results_current(s, value))

    def test_lifecycle_amendment_binds_source_scope_native_qualification_and_audit(self):
        import copy

        with tempfile.TemporaryDirectory() as t:
            study = Path(t)
            parent = {
                "head": "old",
                "files": {"bin/download_batch.py": "old-native", "bin/flowdc_methods.py": "controller"},
            }
            protocol = {"source": parent}
            write_new(study / "protocol.json", protocol)
            actual = {"head": "new", "files": {**parent["files"], "bin/download_batch.py": "new-native"}}
            with self.assertRaises(ValueError):
                ablation.check_lifecycle_source(study, protocol, actual)
            ids = [r["run_id"] for r in ablation.schedule() if r["phase"] == "validation"][:9]
            value = dict(
                schema="flowdc-lifecycle-source-amendment-v1",
                parent_protocol_sha256=file_hash(study / "protocol.json"),
                parent_source=parent,
                source=actual,
                permitted_run_ids=ids,
                qualified_native_sha256="new-native",
                native_qualification=dict(
                    status="qualified", public_requests=0, credited_rows=27166, native_seconds=247
                ),
            )
            path = study / "lifecycle-source-amendment.json"
            write_new(path, value)
            ablation.check_lifecycle_source(study, protocol, actual)
            self.assertEqual(ablation.execution_source(study), actual)
            self.assertFalse(analysis.results_current(study, {"runs": [], "controlled": []}))
            for bad in ("controller", "qualification", "scope", "protocol"):
                changed = copy.deepcopy(value)
                if bad == "controller":
                    changed["source"]["files"]["bin/flowdc_methods.py"] = "changed"
                elif bad == "qualification":
                    changed["native_qualification"]["public_requests"] = 1
                elif bad == "scope":
                    changed["permitted_run_ids"] = ids[:8]
                else:
                    changed["parent_protocol_sha256"] = "wrong"
                path.write_text(json.dumps(changed))
                with self.assertRaises(ValueError):
                    ablation.check_lifecycle_source(study, protocol, changed["source"])
            path.write_text(json.dumps(value))
            with patch.object(ablation_run.time, "time", return_value=100):
                value["expires_at_epoch"] = 99
                path.write_text(json.dumps(value))
                with self.assertRaisesRegex(ValueError, "scope/expiry"):
                    ablation_run.execute(study, study, {"run_id": ids[0]})

    def test_artifact_upper_bound_includes_hardlinks_sparse_and_symlinks(self):
        with tempfile.TemporaryDirectory() as t:
            r = Path(t)
            d = r / "study/runs/a"
            d.mkdir(parents=True)
            f = d / "x"
            f.write_bytes(b"x" * 1234)
            os.link(f, d / "hard")
            (r / "link").symlink_to(f)
            (r / "dirlink").symlink_to(d, target_is_directory=True)
            with (d / "sparse").open("wb") as stream:
                stream.truncate(2**20)
            exact = sum(p.stat().st_size for p in r.rglob("*") if p.is_file() and not p.is_symlink())
            self.assertGreaterEqual(ablation_run.artifact_usage(r, privileged=False), exact)

    def test_declining_clock_does_not_consume_attempt(self):
        with tempfile.TemporaryDirectory() as t:
            s = Path(t)
            run = {"run_id": "not-started", "phase": "screen", "budget_seconds": 420}
            with (
                patch.object(ablation_run, "remaining", side_effect=[660, 659]),
                patch.object(ablation_run, "budget", return_value=0),
                patch.object(ablation_run, "artifact_usage", return_value=0),
                patch.object(
                    ablation_run.shutil, "disk_usage", return_value=type("D", (), {"free": 100 * 2**30})()
                ),
            ):
                with self.assertRaises(ablation_run.ArtifactAdmissionPending):
                    ablation_run.execute(s, s, run)
            self.assertFalse((s / "runs/not-started").exists())

    def test_pack_from_guest_home_uses_explicit_source(self):
        import subprocess
        import sys
        from benchmark.real_server_study import ablation_collect, cloud

        with tempfile.TemporaryDirectory() as t:
            r = Path(t)
            guest = r / "guest"
            s = guest / "study"
            s.mkdir(parents=True)
            (guest / "source").symlink_to(Path(__file__).resolve().parents[1], target_is_directory=True)
            write_new(s / "protocol.json", {"fixture": True})
            run = s / "runs/fixture"
            run.mkdir(parents=True)
            write_new(run / "record.json", {"run_id": "fixture"})
            (run / "trace.jsonl").write_text('{"fixture":true}\n')
            p = r / "packet"
            p.mkdir()
            write_new(p / "operator.json", {"guest_root": str(guest)})
            write_new(p / "deployment-receipt.json", {"python": sys.executable})

            def ssh(packet, args, **kwargs):
                result = subprocess.check_output(args, cwd=r, text=True)
                (p / "collection.tar.gz").write_bytes((s / "collection.tar.gz").read_bytes())
                return result

            with (
                patch.object(cloud, "ssh", side_effect=ssh),
                patch.object(
                    ablation_collect.shutil, "disk_usage", return_value=type("D", (), {"free": 100 * 2**30})()
                ),
            ):
                out = ablation_collect.collect(p, 30)
            self.assertTrue(read_json(out / "protocol.json")["fixture"])
            self.assertTrue((out / "runs/fixture/metadata-inspection.json").exists())
            self.assertFalse((out / "runs/fixture/metadata").exists())

    def test_archive_cache_requires_verified_unchanged_closure(self):
        with tempfile.TemporaryDirectory() as t:
            s = Path(t)
            d = s / "runs/one"
            (d / "native/.flowdc").mkdir(parents=True)
            a = d / "native.tar"
            a.write_bytes(b"archive")
            v = {"archive_sha256": file_hash(a)}
            write_new(d / "native/.flowdc/final.json", {"archive": {"bytes": a.stat().st_size}})
            write_new(d / "compaction.json", v)
            write_new(d / "record.json", {"run_id": "one"})
            ablation_run.seed_archive_digest(s, d, {"run_id": "one", "protocol_window_complete": False}, {})
            self.assertFalse((s / "analysis/audit-units.json").exists())
            ablation_run.seed_archive_digest(s, d, {"run_id": "one", "protocol_window_complete": True}, v)
            self.assertEqual(
                read_json(s / "analysis/audit-units.json")["one"]["record_sha256"],
                file_hash(d / "record.json"),
            )
            stamp = a.stat()
            a.write_bytes(b"changed")
            os.utime(a, ns=(stamp.st_atime_ns, stamp.st_mtime_ns))
            with self.assertRaises(ValueError):
                ablation_run.seed_archive_digest(s, d, {"run_id": "one", "protocol_window_complete": True}, v)


class BundleInspectionTests(unittest.TestCase):
    def test_stream_inspection_retains_lossless_bundle_and_rejects_corruption(self):
        import io
        import tarfile
        from benchmark.real_server_study.ablation_analysis import seal
        from benchmark.real_server_study.ablation_collect import inspect_bundle, extract

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run = root / "run"
            run.mkdir()
            (run / "trace.jsonl").write_bytes(b'{"event":"fixture"}\n')
            spec = seal(run)
            value = inspect_bundle(run / "metadata.tar.gz", 2**20)
            self.assertEqual(value["archive_sha256"], spec["sha256"])
            self.assertEqual(value["files"], 1)
            destination = root / "on-demand"
            destination.mkdir()
            extract(run / "metadata.tar.gz", destination, 2**20)
            self.assertEqual((destination / "trace.jsonl").read_bytes(), (run / "trace.jsonl").read_bytes())
            with self.assertRaises(ValueError):
                inspect_bundle(run / "metadata.tar.gz", 1)
            with self.assertRaises(ValueError):
                inspect_bundle(run / "metadata.tar.gz", 2**20, deadline=0)
            manifest = {"trace.jsonl": {"bytes": 19, "sha256": "0" * 64}}
            for name, members in [
                ("bad-digest", [("trace.jsonl", b"bad")]),
                ("traversal", [("../bad", b"bad")]),
                ("duplicate", [("trace.jsonl", b"a"), ("trace.jsonl", b"b")]),
            ]:
                archive = root / (name + ".tar.gz")
                with tarfile.open(archive, "w:gz") as stream:
                    for path, raw in [*members, ("bundle-manifest.json", json.dumps(manifest).encode())]:
                        member = tarfile.TarInfo(path)
                        member.size = len(raw)
                        stream.addfile(member, io.BytesIO(raw))
                with self.assertRaises(ValueError):
                    inspect_bundle(archive, 2**20)


if __name__ == "__main__":
    unittest.main()
