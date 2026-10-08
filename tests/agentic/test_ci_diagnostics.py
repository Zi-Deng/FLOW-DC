"""Local diagnostic transport fixtures; no hosted execution claims."""

import copy
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import ci_diagnostics as diagnostics
import test_check_runner as legacy


class DiagnosticsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.parent = Path(self.temp.name)
        self.source = Path(__file__).resolve().parents[2]
        self.identity = dict(
            repository="Zi-Deng/FLOW-DC",
            check="agentic-quality",
            event="pull_request",
            pr_head_sha="a" * 40,
            pr_base_sha="b" * 40,
            tested_checkout_sha="c" * 40,
            run_id=1,
            run_attempt=1,
            profile="issue31-suite1800-v1",
            test_status="failure",
            clean_status="success",
        )
        self.enterContext(patch.dict(os.environ, {"RUNNER_TEMP": str(self.parent.parent)}))

    def files(self):
        runner = self.parent / "suite-tmp" / "agentic-check-fixture"
        runner.parent.mkdir(mode=0o700)
        runner.mkdir(mode=0o700)
        for name in diagnostics.LIMITS:
            (runner / name).write_bytes(b'{"partial":\xff\n')
        return runner

    def collect(self):
        result = diagnostics.collect(self.parent, self.source, self.identity)
        manifest = json.loads((self.parent / "diagnostics/manifest.json").read_bytes())
        self.assertNotIn("successful", manifest)
        self.assertEqual(manifest["test_status"], "failure")
        return result, manifest

    def test_exact_partial_bytes_and_failure_are_retained(self):
        runner = self.files()
        (runner / "private").mkdir()
        (runner / "private/secret").write_text("excluded")
        result, manifest = self.collect()
        self.assertEqual(result, 1)
        self.assertEqual(manifest["request_binding"], "invalid")
        self.assertEqual(len(manifest["files"]), 6)
        for row in manifest["files"]:
            self.assertEqual(row["state"], "copied")
            self.assertEqual(
                (runner / row["name"]).read_bytes(), (self.parent / "diagnostics" / row["name"]).read_bytes()
            )
        self.assertFalse((self.parent / "diagnostics/private").exists())

    def test_missing_unsafe_and_oversize_files_are_not_success(self):
        runner = self.files()
        (runner / "request.json").unlink()
        (runner / "summary.json").unlink()
        (runner / "summary.json").symlink_to(runner / "worker-0.log")
        (runner / "worker-0.jsonl").unlink()
        os.mkfifo(runner / "worker-0.jsonl")
        (runner / "worker-1.jsonl").unlink()
        os.link(runner / "worker-1.log", runner / "worker-1.jsonl")
        with (runner / "worker-0.log").open("wb") as stream:
            stream.truncate(diagnostics.LIMITS["worker-0.log"] + 1)
        result, manifest = self.collect()
        self.assertEqual(result, 1)
        self.assertEqual(
            [r["state"] for r in manifest["files"]],
            ["absent", "refused", "refused", "refused", "oversize", "refused"],
        )

    def test_source_changed_during_copy_refuses(self):
        runner = self.files()
        original = diagnostics.os.read
        changed = False
        request_info = (runner / "request.json").stat()
        request_identity = (request_info.st_dev, request_info.st_ino)

        def read(fd, count):
            nonlocal changed
            data = original(fd, count)
            info = os.fstat(fd)
            if data and not changed and (info.st_dev, info.st_ino) == request_identity:
                changed = True
                (runner / "request.json").write_bytes(b"changed")
            return data

        with patch.object(diagnostics.os, "read", side_effect=read):
            result, manifest = self.collect()
        self.assertEqual(result, 1)
        self.assertIn("changed", manifest["collection_errors"])

    def test_directory_refusals_before_execution(self):
        fd = diagnostics.staging(self.parent, self.source)
        os.close(fd)
        for bad in (str(self.parent) + "/", str(self.parent) + "/../x", "relative"):
            with self.assertRaises((OSError, ValueError)):
                diagnostics.staging(bad, self.source)
        with self.assertRaises(ValueError):
            diagnostics.staging(self.parent, self.parent)
        self.parent.chmod(0o755)
        with self.assertRaises(ValueError):
            diagnostics.staging(self.parent, self.source)
        self.parent.chmod(0o700)
        link = self.parent / "link"
        link.symlink_to(self.source)
        with self.assertRaises(OSError):
            diagnostics.directory(link)

    def test_metadata_and_aggregate_manifest_deadline_bounds(self):
        for key, value in (
            ("run_id", True),
            ("run_attempt", 0),
            ("profile", "legacy"),
            ("pr_head_sha", "wrong"),
            ("test_status", "ready"),
            ("repository", "other"),
        ):
            bad = copy.deepcopy(self.identity)
            bad[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                diagnostics.metadata(bad)
        bad = {**self.identity, "ready": True}
        with self.assertRaises(ValueError):
            diagnostics.metadata(bad)
        self.files()
        with patch.object(diagnostics, "AGGREGATE_LIMIT", diagnostics.MANIFEST_LIMIT + 1):
            result, manifest = self.collect()
        self.assertEqual(result, 1)
        self.assertIn("size", manifest["collection_errors"])

    def test_manifest_bound(self):
        self.files()
        with patch.object(diagnostics, "MANIFEST_LIMIT", 1), self.assertRaisesRegex(ValueError, "manifest"):
            diagnostics.collect(self.parent, self.source, self.identity)

    def test_expired_copy_is_failure(self):
        self.files()
        with patch.object(diagnostics, "SECONDS", 0):
            self.assertEqual(diagnostics.collect(self.parent, self.source, self.identity), 1)
        self.assertFalse((self.parent / "diagnostics").exists())


class DiagnosticCliTests(unittest.TestCase):
    setUp = legacy.RunnerTests.setUp
    good = legacy.RunnerTests.good
    module = legacy.RunnerTests.module
    evidence = legacy.RunnerTests.evidence

    def test_real_cli_default_and_explicit_serial_parallel(self):
        self.good()
        for jobs, isolated in ((1, False), (1, True), (2, True)):
            with tempfile.TemporaryDirectory() as tmp:
                env = os.environ.copy()
                if isolated:
                    env["TMPDIR"] = tmp
                result = subprocess.run(
                    [
                        sys.executable,
                        "-B",
                        str(self.root / "scripts/agentic/check.py"),
                        "--jobs",
                        str(jobs),
                        "--suite-profile",
                        "issue31-suite1800-v1",
                    ],
                    cwd=self.root,
                    env=env,
                    capture_output=True,
                    timeout=15,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                directory, summary = self.evidence(result)
                self.assertTrue(summary["successful"])
                request = json.loads((directory / "request.json").read_bytes())
                self.assertEqual(summary["occurrences"], 1)
                self.assertEqual(sum(len(v) for v in request["assignments"]), len(request["rows"]))
                self.assertEqual(len(summary["process_exits"]), jobs)
                if isolated:
                    self.assertEqual(directory.parent, Path(tmp))
                    self.assertEqual(len(list(Path(tmp).glob("agentic-check-*"))), 1)
                for index in range(jobs):
                    self.assertTrue((directory / f"tmp-{index}").is_dir())

    def test_real_assertion_and_short_deadline_keep_failure(self):
        env = {k: v for k, v in os.environ.items() if k != "RUNNER_TEMP"}
        for body, seconds in (("self.fail('expected')", 3), ("__import__('time').sleep(5)", 0.2)):
            self.module(
                "test_a.py",
                "import unittest\nclass A(unittest.TestCase):\n def test_case(self): " + body + "\n",
            )
            code = (
                "import sys;from pathlib import Path;sys.path.insert(0,str(Path.cwd()/'scripts/agentic'));import check_runner as r;sys.exit(r.run(Path.cwd(),2,seconds="
                + str(seconds)
                + "))"
            )
            result = subprocess.run(
                [sys.executable, "-B", "-c", code], cwd=self.root, env=env, capture_output=True, timeout=10
            )
            self.assertEqual(result.returncode, 1)
            _, summary = self.evidence(result)
            self.assertFalse(summary["successful"])
            self.assertEqual(len(summary["process_exits"]), 2)

    def test_late_child_is_reaped_at_original_deadline(self):
        self.module(
            "test_a.py",
            "import unittest,subprocess,sys\nclass A(unittest.TestCase):\n def test_child(self): subprocess.Popen([sys.executable,'-c','import time;time.sleep(20)'])\n",
        )
        code = "import sys;from pathlib import Path;sys.path.insert(0,str(Path.cwd()/'scripts/agentic'));import check_runner as r;sys.exit(r.run(Path.cwd(),2,seconds=0.5))"
        result = subprocess.run(
            [sys.executable, "-B", "-c", code], cwd=self.root, capture_output=True, timeout=10
        )
        self.assertEqual(result.returncode, 1)
        _, summary = self.evidence(result)
        self.assertFalse(summary["successful"])
        self.assertLess(summary["elapsed_seconds"], 5)


class DiagnosticIntegrationTests(unittest.TestCase):
    def test_make_treats_optional_value_as_data(self):
        result = subprocess.run(
            ["make", "-n", "test-agentic", "PYTHON=python3"],
            cwd=legacy.SOURCE,
            capture_output=True,
            timeout=10,
        )
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, b"python3 -B scripts/agentic/check.py \n")
        self.assertNotIn("AGENTIC_EVIDENCE_DIRECTORY", (legacy.SOURCE / "Makefile").read_text())
        workflow = (legacy.SOURCE / ".github/workflows/agentic-quality.yml").read_text()
        self.assertIn('TMPDIR="$AGENTIC_DIAGNOSTIC_STAGING/suite-tmp" make', workflow)
        self.assertNotIn("output.write(f'TMPDIR=", workflow)

    def test_diagnostic_artifact_does_not_replace_receipt(self):
        from types import SimpleNamespace

        import ci_evidence

        head = "a" * 40
        receipt = dict(
            schema_version=1,
            repository="Zi-Deng/FLOW-DC",
            pr_head_sha=head,
            pr_base_sha="b" * 40,
            tested_checkout_sha="c" * 40,
            run_id=1,
            run_attempt=1,
            check="agentic-quality",
            runner_environment="github-hosted",
            test_status="failure",
            clean_status="success",
        )
        artifacts = [
            {"name": f"diagnostic-agentic-quality-{head}-1-1", "id": 2},
            {"name": f"validation-agentic-quality-{head}-1", "id": 3},
        ]

        def api(endpoint, **kwargs):
            if "artifacts" in endpoint:
                return artifacts
            return dict(head_sha=head, repository={"full_name": "Zi-Deng/FLOW-DC"}, run_attempt=1)

        repo = SimpleNamespace(name="Zi-Deng/FLOW-DC", api=api, github_token=lambda: None)
        checks = [
            dict(
                id=1, name="agentic-quality", details_url="https://github.com/Zi-Deng/FLOW-DC/actions/runs/1"
            )
        ]
        with patch.object(ci_evidence, "artifact_receipt", return_value=(receipt, "d" * 64)) as reader:
            rows = ci_evidence.collect(repo, head, checks, "b" * 40)
            self.assertEqual(reader.call_args.args[1], 3)
            self.assertEqual(rows[0]["test_status"], "failure")
            artifacts.pop()
            self.assertEqual(
                ci_evidence.collect(repo, head, checks)[0]["state"], "missing_or_ambiguous_receipt"
            )

    def test_per_file_limit_and_nonregular_boundaries(self):
        for name, limit in diagnostics.LIMITS.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmp:
                parent = Path(tmp)
                (parent / "suite-tmp").mkdir(mode=0o700)
                (parent / "suite-tmp/agentic-check-fixture").mkdir(mode=0o700)
                for other in diagnostics.LIMITS:
                    (parent / "suite-tmp/agentic-check-fixture" / other).write_bytes(b"x")
                with (parent / "suite-tmp/agentic-check-fixture" / name).open("wb") as stream:
                    stream.truncate(limit + 1)
                identity = dict(
                    repository="Zi-Deng/FLOW-DC",
                    check="agentic-quality",
                    event="pull_request",
                    pr_head_sha="a" * 40,
                    pr_base_sha="b" * 40,
                    tested_checkout_sha="c" * 40,
                    run_id=1,
                    run_attempt=1,
                    profile="issue31-suite1800-v1",
                    test_status="failure",
                    clean_status="success",
                )
                with patch.dict(os.environ, {"RUNNER_TEMP": str(parent.parent)}):
                    self.assertEqual(diagnostics.collect(parent, legacy.SOURCE, identity), 1)
                rows = json.loads((parent / "diagnostics/manifest.json").read_text())["files"]
                self.assertEqual(next(r for r in rows if r["name"] == name)["state"], "oversize")


class TemporaryRootTests(unittest.TestCase):
    setUp = DiagnosticsTests.setUp
    files = DiagnosticsTests.files
    collect = DiagnosticsTests.collect

    def test_valid_request_and_independent_source(self):
        import check_runner

        runner = self.files()
        identity = diagnostics.source_identity(self.source, diagnostics.Deadline(60))
        self.assertEqual(identity, check_runner.source(self.source))
        self.identity["tested_checkout_sha"] = identity["checkout"]
        request = dict(
            version=3,
            jobs=2,
            source=identity,
            evidence_limit=check_runner.EVIDENCE_BYTES,
            execution_limits=check_runner.execution_limits(
                "issue31-suite1800-v1", 1800, check_runner.TEXT_BYTES, check_runner.EVIDENCE_BYTES
            ),
        )
        (runner / "request.json").write_text(json.dumps(request))
        (runner / "tmp-0/agentic-check-nested").mkdir(parents=True)
        result, manifest = self.collect()
        self.assertEqual(result, 0)
        self.assertEqual(manifest["request_binding"], "matched")
        self.assertFalse((self.parent / "diagnostics/tmp-0").exists())
        for field, value in (("jobs", True), ("jobs", 1), ("source", {}), ("version", True)):
            bad = {**request, field: value}
            self.assertEqual(diagnostics.request_binding(json.dumps(bad).encode(), identity), "mismatch")
        self.assertEqual(diagnostics.request_binding(b'{"jobs":2,"jobs":2}', identity), "invalid")

    def test_selection_is_bounded_unique_and_nonrecursive(self):
        runner = self.files()
        fd = diagnostics.directory(runner.parent)
        try:
            name, selected = diagnostics.select(fd, diagnostics.Deadline(60))
            os.close(selected)
            self.assertEqual(name, runner.name)
            (runner.parent / "agentic-check-other").mkdir(mode=0o700)
            with self.assertRaisesRegex(ValueError, "selection_ambiguous"):
                diagnostics.select(fd, diagnostics.Deadline(60))
        finally:
            os.close(fd)
        with tempfile.TemporaryDirectory() as tmp:
            for index in range(129):
                (Path(tmp) / str(index)).touch()
            fd = diagnostics.directory(tmp)
            try:
                with self.assertRaisesRegex(ValueError, "selection_limit"):
                    diagnostics.select(fd, diagnostics.Deadline(60))
            finally:
                os.close(fd)

    def test_end_to_end_copy_and_manifest_deadlines(self):
        for phase in ("copy", "manifest"):
            with self.subTest(phase=phase), tempfile.TemporaryDirectory() as tmp:
                parent = Path(tmp)
                runner = parent / "suite-tmp/agentic-check-fixture"
                runner.mkdir(parents=True, mode=0o700)
                runner.parent.chmod(0o700)
                for name in diagnostics.LIMITS:
                    (runner / name).write_bytes(b"raw")
                now = [0.0]
                original_read = diagnostics.os.read
                original_dumps = json.dumps

                def read(fd, n, original_read=original_read, phase=phase, now=now):
                    data = original_read(fd, n)
                    if phase == "copy":
                        now[0] = 60.0
                    return data

                def dumps(*args, original_dumps=original_dumps, phase=phase, now=now, **kwargs):
                    value = original_dumps(*args, **kwargs)
                    if phase == "manifest":
                        now[0] = 60.0
                    return value

                with (
                    patch.object(diagnostics.time, "monotonic", side_effect=lambda now=now: now[0]),
                    patch.object(diagnostics.os, "read", side_effect=read),
                    patch.object(diagnostics.json, "dumps", side_effect=dumps),
                    patch.object(
                        diagnostics, "source_identity", return_value={"checkout": "c" * 40, "files": {}}
                    ),
                ):
                    self.assertEqual(diagnostics.collect(parent, self.source, self.identity), 1)
                self.assertFalse((parent / "diagnostics/manifest.json").exists())
                if phase == "copy":
                    self.assertFalse((parent / "diagnostics/summary.json").exists())

    def test_metadata_and_actual_alarm_do_not_restart_deadline(self):
        import signal

        self.files()
        for phase in ("metadata", "alarm"):
            with tempfile.TemporaryDirectory() as tmp:
                parent = Path(tmp)
                runner = parent / "suite-tmp/agentic-check-fixture"
                runner.mkdir(parents=True, mode=0o700)
                runner.parent.chmod(0o700)
                for name in diagnostics.LIMITS:
                    (runner / name).write_bytes(b"raw")
                now = [0.0]
                handlers = []

                def handler(sig, callback, handlers=handlers):
                    handlers.append(callback)
                    return signal.SIG_DFL

                def output(*args, phase=phase, now=now, **kwargs):
                    if phase == "metadata":
                        now[0] = 60.0
                    return b"c" * 40 + b"\n"

                def read(fd, n, handlers=handlers):
                    handlers[0](signal.SIGALRM, None)

                env = dict(
                    GITHUB_REPOSITORY="Zi-Deng/FLOW-DC",
                    GITHUB_EVENT_NAME="pull_request",
                    REVIEW_HEAD_SHA="a" * 40,
                    REVIEW_BASE_SHA="b" * 40,
                    GITHUB_RUN_ID="1",
                    GITHUB_RUN_ATTEMPT="1",
                    AGENTIC_SUITE_PROFILE="issue31-suite1800-v1",
                )
                with (
                    patch.dict(os.environ, env),
                    patch.object(
                        sys,
                        "argv",
                        [
                            "diagnostics",
                            "--staging",
                            tmp,
                            "--test-status",
                            "failure",
                            "--clean-status",
                            "success",
                        ],
                    ),
                    patch.object(diagnostics.time, "monotonic", side_effect=lambda now=now: now[0]),
                    patch.object(diagnostics.signal, "signal", side_effect=handler),
                    patch.object(diagnostics.signal, "setitimer"),
                    patch.object(diagnostics.subprocess, "check_output", side_effect=output),
                    patch.object(diagnostics.os, "read", side_effect=read),
                    patch.object(
                        diagnostics, "source_identity", return_value={"checkout": "c" * 40, "files": {}}
                    ),
                ):
                    self.assertEqual(diagnostics.main(), 1)
                self.assertFalse((parent / "diagnostics/summary.json").exists())
                self.assertFalse((parent / "diagnostics/manifest.json").exists())


class RefusalRepairTests(unittest.TestCase):
    setUp = DiagnosticsTests.setUp

    def test_invalid_directory_handles_are_closed_and_never_copied(self):
        for unsafe in ("suite", "candidate", "missing", "file", "symlink"):
            with self.subTest(unsafe=unsafe), tempfile.TemporaryDirectory() as tmp:
                parent = Path(tmp)
                suite = parent / "suite-tmp"
                suite.mkdir(mode=0o700)
                candidate = suite / "agentic-check-fixture"
                if unsafe == "file":
                    candidate.touch()
                elif unsafe == "symlink":
                    candidate.symlink_to(parent, target_is_directory=True)
                elif unsafe != "missing":
                    candidate.mkdir(mode=0o700)
                    for name in diagnostics.LIMITS:
                        (candidate / name).write_bytes(b"raw")
                if unsafe == "suite":
                    suite.chmod(0o755)
                if unsafe == "candidate":
                    candidate.chmod(0o755)
                opened = []
                closed = []
                active = {}
                close_errors = []
                real_open, real_close = os.open, os.close

                def opening(*args, real_open=real_open, active=active, opened=opened, **kwargs):
                    fd = real_open(*args, **kwargs)
                    self.assertNotIn(fd, active)
                    info = os.fstat(fd)
                    lifetime = (len(opened), fd, info.st_dev, info.st_ino)
                    active[fd] = lifetime
                    opened.append(lifetime)
                    return fd

                def closing(
                    fd, active=active, real_close=real_close, close_errors=close_errors, closed=closed
                ):
                    lifetime = active.get(fd)
                    if lifetime is not None:
                        info = os.fstat(fd)
                        self.assertEqual((info.st_dev, info.st_ino), lifetime[2:])
                    try:
                        result = real_close(fd)
                    except OSError as exc:
                        close_errors.append(type(exc).__name__)
                        raise
                    if lifetime is not None:
                        closed.append(lifetime)
                        del active[fd]
                    return result

                with (
                    patch.object(diagnostics.os, "open", side_effect=opening),
                    patch.object(diagnostics.os, "close", side_effect=closing),
                ):
                    self.assertEqual(diagnostics.collect(parent, self.source, self.identity), 1)
                self.assertEqual(sorted(opened), sorted(closed))
                self.assertEqual(active, {})
                self.assertEqual(close_errors, [])
                manifest = json.loads((parent / "diagnostics/manifest.json").read_bytes())
                reason = (
                    "ownership"
                    if unsafe in {"suite", "candidate"}
                    else "selection_missing"
                    if unsafe == "missing"
                    else "selection_unsafe"
                )
                self.assertIn(reason, manifest["collection_errors"])
                self.assertIsNone(manifest["runner_name"])
                self.assertTrue(all(row["state"] == "refused" for row in manifest["files"]))
                self.assertEqual({p.name for p in (parent / "diagnostics").iterdir()}, {"manifest.json"})

    def test_real_source_and_installed_fixture_maps_and_missing_source(self):
        import check_runner
        import install

        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "source"
            source.mkdir()
            for name in install.PATHS:
                path = source / name
                if Path(name).suffix:
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text("fixture\n")
                else:
                    path.mkdir(parents=True, exist_ok=True)
                    (path / "fixture.txt").write_text("fixture\n")
            (source / "Makefile").write_text("fixture\n")
            (source / "tracked-extra.txt").write_text("tracked\n")
            for argv in (
                ["git", "init", "-q"],
                ["git", "add", "."],
                [
                    "git",
                    "-c",
                    "user.name=Fixture",
                    "-c",
                    "user.email=fixture@example.invalid",
                    "commit",
                    "-qm",
                    "fixture",
                ],
            ):
                subprocess.run(argv, cwd=source, check=True, capture_output=True, timeout=5)
            expected = check_runner.source(source)
            self.assertEqual(diagnostics.source_identity(source, diagnostics.Deadline(60)), expected)
            self.assertIn("tracked-extra.txt", expected["files"])
            target = Path(tmp) / "installed"
            install.install(source, target, apply=True)
            installed = check_runner.source(target)
            self.assertIsNone(installed["checkout"])
            self.assertEqual(diagnostics.source_identity(target, diagnostics.Deadline(60)), installed)
            self.assertIn(".agentic/template-origin.json", installed["files"])
            self.assertNotIn("Makefile", installed["files"])
            (source / "tracked-extra.txt").unlink()
            with self.assertRaises(OSError):
                diagnostics.source_identity(source, diagnostics.Deadline(60))
            shutil.rmtree(target / "tests/agentic")
            (target / "tests/agentic").symlink_to(source / "tests/agentic", target_is_directory=True)
            with self.assertRaisesRegex(ValueError, "source_changed"):
                diagnostics.source_identity(target, diagnostics.Deadline(60))

    def test_strict_request_identity_and_checkout_binding(self):
        import check_runner

        identity = {"checkout": "d" * 40, "files": {"file": "e" * 64}}
        request = dict(
            version=3,
            jobs=2,
            source=identity,
            evidence_limit=check_runner.EVIDENCE_BYTES,
            execution_limits=check_runner.execution_limits(
                "issue31-suite1800-v1", 1800, check_runner.TEXT_BYTES, check_runner.EVIDENCE_BYTES
            ),
        )
        self.assertEqual(diagnostics.request_binding(json.dumps(request).encode(), identity), "matched")
        for key, value in (("profile", "legacy"), ("seconds", True), ("text_limit", 1), ("extra", 1)):
            bad = copy.deepcopy(request)
            bad["execution_limits"][key] = value
            self.assertEqual(diagnostics.request_binding(json.dumps(bad).encode(), identity), "mismatch")
        for raw in (b"{", b'{"version":NaN}', b"[]", b"null"):
            self.assertEqual(diagnostics.request_binding(raw, identity), "invalid")
        self.assertEqual(diagnostics.request_binding(None, identity), "absent")
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            runner = parent / "suite-tmp/agentic-check-fixture"
            runner.mkdir(parents=True, mode=0o700)
            runner.parent.chmod(0o700)
            for name in diagnostics.LIMITS:
                (runner / name).write_bytes(b"raw")
            (runner / "request.json").write_text(json.dumps(request))
            with patch.object(diagnostics, "source_identity", return_value=identity):
                self.assertEqual(diagnostics.collect(parent, self.source, self.identity), 1)
            manifest = json.loads((parent / "diagnostics/manifest.json").read_bytes())
            self.assertEqual(manifest["request_binding"], "mismatch")
            self.assertTrue(all(row["state"] == "copied" for row in manifest["files"]))


class TinyTemporaryRootTests(unittest.TestCase):
    setUp = legacy.RunnerTests.setUp
    module = legacy.RunnerTests.module
    evidence = legacy.RunnerTests.evidence

    def test_tiny_nested_outputs_keep_order_and_do_not_fall_back(self):
        self.module(
            "test_nested.py",
            "import os,tempfile,unittest\nfrom pathlib import Path\nclass Tiny(unittest.TestCase):\n def test_a(self):\n  p=Path(tempfile.mkdtemp(prefix='agentic-check-nested-'))\n  self.assertEqual(p.parent,Path(os.environ['TMPDIR']))\n def test_b(self): self.assertTrue(True)\n",
        )
        for jobs in (1, 2):
            with tempfile.TemporaryDirectory() as tmp:
                env = {**os.environ, "TMPDIR": tmp}
                result = subprocess.run(
                    [sys.executable, "-B", str(self.root / "scripts/agentic/check.py"), "--jobs", str(jobs)],
                    cwd=self.root,
                    env=env,
                    capture_output=True,
                    timeout=15,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                directory, summary = self.evidence(result)
                self.assertEqual(directory.parent, Path(tmp))
                request = json.loads((directory / "request.json").read_bytes())
                self.assertEqual(summary["occurrences"], 2)
                self.assertEqual(
                    [row["id"].rsplit(".", 1)[-1] for row in request["rows"]], ["test_a", "test_b"]
                )
                self.assertEqual(
                    sorted(v for group in request["assignments"] for v in group),
                    sorted(set(row["position"][0] for row in request["rows"])),
                )
                fd = diagnostics.directory(tmp)
                try:
                    name, selected = diagnostics.select(fd, diagnostics.Deadline(60))
                    os.close(selected)
                    self.assertEqual(name, directory.name)
                finally:
                    os.close(fd)
                self.assertTrue(list(directory.glob("tmp-*/agentic-check-nested-*")))
        # A missing expected execution root refuses; no search of fallback /tmp.
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            identity = dict(
                repository="Zi-Deng/FLOW-DC",
                check="agentic-quality",
                event="pull_request",
                pr_head_sha="a" * 40,
                pr_base_sha="b" * 40,
                tested_checkout_sha="c" * 40,
                run_id=1,
                run_attempt=1,
                profile="issue31-suite1800-v1",
                test_status="failure",
                clean_status="success",
            )
            with patch.dict(os.environ, {"RUNNER_TEMP": str(parent.parent)}):
                self.assertEqual(diagnostics.collect(parent, legacy.SOURCE, identity), 1)
            manifest = json.loads((parent / "diagnostics/manifest.json").read_bytes())
            self.assertIsNone(manifest["runner_name"])
            self.assertIn("selection_unsafe", manifest["collection_errors"])
            self.assertTrue(all(row["state"] == "refused" for row in manifest["files"]))


class QuiescentDirectoryTests(unittest.TestCase):
    setUp = DiagnosticsTests.setUp
    files = DiagnosticsTests.files
    collect = DiagnosticsTests.collect

    def test_observed_directory_mode_change_refuses(self):
        runner = self.files()
        original = diagnostics.source_identity
        calls = 0

        def identity(*args):
            nonlocal calls
            calls += 1
            value = original(*args)
            if calls == 2:
                runner.chmod(0o755)
            return value

        with patch.object(diagnostics, "source_identity", side_effect=identity):
            result, manifest = self.collect()
        self.assertEqual(result, 1)
        self.assertIn("boundary", manifest["collection_errors"])


class SourceAncestorTests(unittest.TestCase):
    def test_intermediate_source_and_tracked_parents_refuse(self):
        for prefix in ("scripts", ".agents", "docs", "tracked"):
            with self.subTest(prefix=prefix), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp) / "source"
                root.mkdir()
                outside = Path(tmp) / "outside"
                suffix = {
                    "scripts": "agentic/a.py",
                    ".agents": "skills/a.py",
                    "docs": "agent-workflow/a.py",
                    "tracked": "nested/a.py",
                }[prefix]
                target = outside / suffix
                target.parent.mkdir(parents=True)
                target.write_bytes(b"fixture\n")
                if prefix == "tracked":
                    original = root / prefix / suffix
                    original.parent.mkdir(parents=True)
                    original.write_bytes(b"fixture\n")
                    for argv in (
                        ["git", "init", "-q"],
                        ["git", "add", "."],
                        [
                            "git",
                            "-c",
                            "user.name=Fixture",
                            "-c",
                            "user.email=fixture@example.invalid",
                            "commit",
                            "-qm",
                            "fixture",
                        ],
                    ):
                        subprocess.run(argv, cwd=root, check=True, capture_output=True, timeout=5)
                    shutil.rmtree(root / prefix)
                (root / prefix).symlink_to(outside, target_is_directory=True)
                with self.assertRaisesRegex(ValueError, "source_changed"):
                    diagnostics.source_identity(root, diagnostics.Deadline(60))
                self.assertEqual(target.read_bytes(), b"fixture\n")

    def test_source_parent_replacement_and_nonregular_refuse(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            base = root / "scripts/agentic"
            base.mkdir(parents=True)
            file = base / "a.py"
            file.write_bytes(b"fixture\n")
            real_read = os.read
            changed = False

            def read(fd, size):
                nonlocal changed
                value = real_read(fd, size)
                if value and not changed:
                    changed = True
                    (root / "scripts").rename(root / "old-scripts")
                    shutil.copytree(root / "old-scripts", root / "scripts")
                return value

            with patch.object(diagnostics.os, "read", side_effect=read):
                with self.assertRaisesRegex(ValueError, "source_changed"):
                    diagnostics.source_identity(root, diagnostics.Deadline(60))
            shutil.rmtree(root / "scripts")
            (root / "scripts").write_bytes(b"non-directory")
            with self.assertRaisesRegex(ValueError, "source_changed"):
                diagnostics.source_identity(root, diagnostics.Deadline(60))

    def test_deep_malformed_request_is_unbound(self):
        self.assertEqual(diagnostics.request_binding(b"[" * 2000 + b"0" + b"]" * 2000, {}), "invalid")

    def test_postcopy_source_change_is_unbound_and_newline_path_refuses(self):
        import check_runner

        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            runner = parent / "suite-tmp/agentic-check-fixture"
            runner.mkdir(parents=True, mode=0o700)
            runner.parent.chmod(0o700)
            before = {"checkout": "c" * 40, "files": {"a": "d" * 64}}
            after = {"checkout": "c" * 40, "files": {"a": "e" * 64}}
            request = dict(
                version=3,
                jobs=2,
                source=before,
                evidence_limit=check_runner.EVIDENCE_BYTES,
                execution_limits=check_runner.execution_limits(
                    "issue31-suite1800-v1", 1800, check_runner.TEXT_BYTES, check_runner.EVIDENCE_BYTES
                ),
            )
            for name in diagnostics.LIMITS:
                (runner / name).write_bytes(b"raw")
            (runner / "request.json").write_text(json.dumps(request))
            identity = dict(
                repository="Zi-Deng/FLOW-DC",
                check="agentic-quality",
                event="pull_request",
                pr_head_sha="a" * 40,
                pr_base_sha="b" * 40,
                tested_checkout_sha="c" * 40,
                run_id=1,
                run_attempt=1,
                profile="issue31-suite1800-v1",
                test_status="failure",
                clean_status="success",
            )
            with (
                patch.dict(os.environ, {"RUNNER_TEMP": str(parent.parent)}),
                patch.object(diagnostics, "source_identity", side_effect=[before, after]),
            ):
                self.assertEqual(diagnostics.collect(parent, legacy.SOURCE, identity), 1)
            manifest = json.loads((parent / "diagnostics/manifest.json").read_bytes())
            self.assertEqual(manifest["request_binding"], "mismatch")
            self.assertIn("source_changed", manifest["collection_errors"])
            with self.assertRaisesRegex(ValueError, "path"):
                diagnostics.directory(str(parent) + "/newline\ncomponent")
