"""Exercise complete test execution with disposable suites, never the real suite recursively."""

import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

SOURCE = Path(__file__).resolve().parents[2]


class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="runner-test-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "scripts/agentic").mkdir(parents=True)
        (self.root / "tests/agentic").mkdir(parents=True)
        (self.root / ".agentic").mkdir()
        (self.root / ".agentic/config.json").write_text("{}")
        for name in ("check.py", "check_runner.py"):
            source = SOURCE / "scripts/agentic" / name
            if source.exists():
                shutil.copyfile(source, self.root / "scripts/agentic" / name)

    def module(self, name, text):
        (self.root / "tests/agentic" / name).write_text(text)

    def command(self, jobs=2):
        return subprocess.run(
            [sys.executable, "-B", str(self.root / "scripts/agentic/check.py"), "--jobs", str(jobs)],
            cwd=self.root,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=30,
            env={**os.environ, "TMPDIR": str(self.root)},
        )

    def test_two_workers_account_for_the_complete_tiny_suite(self):
        for name in ("a", "b"):
            self.module(
                f"test_{name}.py",
                "import unittest\nclass Tiny(unittest.TestCase):\n"
                " def test_ok(self): self.assertEqual(2 + 2, 4)\n",
            )
        result = self.command()
        self.assertEqual(result.returncode, 0, result.stdout.decode())
        self.assertIn(b"Workflow aggregate: jobs=2 occurrences=2", result.stdout)

    def helper(self, jobs=2, **limits):
        expression = (
            "import sys; from pathlib import Path; import check_runner; "
            f"sys.exit(check_runner.run(Path({str(self.root)!r}), {jobs!r}, **{limits!r}))"
        )
        return subprocess.run(
            [sys.executable, "-B", "-c", expression],
            cwd=self.root,
            env={**os.environ, "PYTHONPATH": str(self.root / "scripts/agentic"), "TMPDIR": str(self.root)},
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=30,
        )

    def evidence(self, result):
        line = next(x for x in result.stdout.decode().splitlines() if x.startswith("Workflow evidence: "))
        directory = Path(line.split(": ", 1)[1])
        return directory, json.loads((directory / "summary.json").read_bytes())

    def good(self):
        self.module("test_a.py", "import unittest\nclass A(unittest.TestCase):\n def test_ok(self): pass\n")

    def test_serial_parallel_duplicate_occurrences_and_fixture_traces_match(self):
        self.module(
            "test_a.py",
            """import unittest
from pathlib import Path
def note(s):
 with Path("trace").open("a") as f: f.write(s+"\\n")
def setUpModule(): note("module-start")
def tearDownModule(): note("module-stop")
class A(unittest.TestCase):
 @classmethod
 def setUpClass(cls): note("class-start")
 @classmethod
 def tearDownClass(cls): note("class-stop")
 def test_ok(self): note("test")
def load_tests(loader, tests, pattern):
 return unittest.TestSuite([tests, loader.loadTestsFromTestCase(A)])
""",
        )
        self.module("test_b.py", "from test_a import A\n")
        serial = self.command(1)
        self.assertEqual(serial.returncode, 0, serial.stdout.decode())
        _, a = self.evidence(serial)
        trace = (self.root / "trace").read_bytes()
        (self.root / "trace").unlink()
        parallel = self.command(2)
        self.assertEqual(parallel.returncode, 0, parallel.stdout.decode())
        _, b = self.evidence(parallel)
        self.assertEqual(trace, (self.root / "trace").read_bytes())
        rows_a = [r for w in a["workers"] for r in w["occurrences"]]
        rows_b = [r for w in b["workers"] for r in w["occurrences"]]
        self.assertEqual(
            [{k: v for k, v in r.items() if k != "timing_ns"} for r in rows_a],
            [{k: v for k, v in r.items() if k != "timing_ns"} for r in rows_b],
        )
        self.assertEqual(len(rows_a), 3)
        self.assertEqual(len({r["id"] for r in rows_a}), 1)

    def test_failures_subtests_skips_and_fixture_errors_are_not_lost(self):
        self.module(
            "test_a.py",
            """import unittest
class A(unittest.TestCase):
 def test_failure(self): self.fail("EXACT FAILURE")
 def test_subtest(self):
  with self.subTest(i=3): self.fail("EXACT SUBTEST")
 def test_error(self): raise ValueError("EXACT ERROR")
 def test_skip(self): self.skipTest("EXACT SKIP")
 @unittest.expectedFailure
 def test_expected(self): self.fail("EXPECTED")
 @unittest.expectedFailure
 def test_unexpected(self): pass
 def test_last(self): self.assertTrue(True)
class B(unittest.TestCase):
 @classmethod
 def setUpClass(cls): raise ValueError("EXACT FIXTURE")
 def test_missing(self): pass
class C(unittest.TestCase):
 def test_ok(self): pass
 @classmethod
 def tearDownClass(cls): raise ValueError("EXACT TEARDOWN")
""",
        )
        result = self.command()
        self.assertEqual(result.returncode, 1)
        _, summary = self.evidence(result)
        self.assertIsNone(summary["error"])
        rows = [r for w in summary["workers"] for r in w["occurrences"]]
        self.assertEqual(len(rows), 9)
        serial = self.command(1)
        self.assertEqual(serial.returncode, 1)
        _, serial_summary = self.evidence(serial)
        serial_rows = [r for w in serial_summary["workers"] for r in w["occurrences"]]

        def normalize(items):
            return [{k: v for k, v in r.items() if k != "timing_ns"} for r in items]

        self.assertEqual(normalize(rows), normalize(serial_rows))
        self.assertEqual(
            [f for w in summary["workers"] for f in w["fixtures"]],
            [f for w in serial_summary["workers"] for f in w["fixtures"]],
        )
        self.assertEqual(sum(r["state"] == "incomplete" for r in rows), 1)
        for message in (
            b"EXACT FAILURE",
            b"EXACT SUBTEST",
            b"EXACT ERROR",
            b"EXACT SKIP",
            b"EXACT FIXTURE",
            b"EXACT TEARDOWN",
        ):
            self.assertIn(message, result.stdout)
        self.assertTrue(any(r["id"].endswith("test_last") and r["state"] == "completed" for r in rows))

    def test_module_and_class_skip_preserve_standard_success(self):
        self.module(
            "test_a.py",
            """import unittest
def setUpModule(): raise unittest.SkipTest("module skipped")
class A(unittest.TestCase):
 def test_ok(self): pass
""",
        )
        self.module(
            "test_b.py",
            """import unittest
class B(unittest.TestCase):
 @classmethod
 def setUpClass(cls): raise unittest.SkipTest("class skipped")
 def test_ok(self): pass
""",
        )
        result = self.command()
        self.assertEqual(result.returncode, 0, result.stdout.decode())
        _, summary = self.evidence(result)
        self.assertEqual(
            [r["state"] for w in summary["workers"] for r in w["occurrences"]], ["fixture_skip"] * 2
        )

    def test_import_error_and_empty_discovery_fail(self):
        result = self.command()
        self.assertEqual(result.returncode, 1)
        self.assertIn(b"Empty test discovery", result.stdout)
        self.module("test_bad.py", "raise ImportError('EXACT IMPORT')\n")
        result = self.command()
        self.assertEqual(result.returncode, 1)
        self.assertIn(b"EXACT IMPORT", result.stdout)
        _, summary = self.evidence(result)
        self.assertEqual(summary["occurrences"], 1)

    def test_custom_suite_is_refused_without_running(self):
        self.module(
            "test_a.py",
            """import unittest
class Special(unittest.TestSuite):
 def run(self, result, debug=False): raise AssertionError("must not run")
def load_tests(loader, tests, pattern): return Special()
""",
        )
        result = self.command()
        self.assertEqual(result.returncode, 1)
        self.assertIn(b"Unsupported custom suite", result.stdout)

    def test_process_isolation_and_bounded_two_worker_assignment(self):
        for name in ("a", "b"):
            self.module(
                f"test_{name}.py",
                f"""import unittest, os, builtins
from pathlib import Path
class A(unittest.TestCase):
 def test_isolated(self):
  self.assertFalse(hasattr(builtins,"worker_mutation"))
  builtins.worker_mutation=True
  Path("pid-{name}").write_text(str(os.getpid())+"\\n"+os.environ["TMPDIR"])
""",
            )
        result = self.command()
        self.assertEqual(result.returncode, 0, result.stdout.decode())
        a, b = [(self.root / f"pid-{name}").read_text().splitlines() for name in ("a", "b")]
        self.assertNotEqual(a[0], b[0])
        self.assertNotEqual(a[1], b[1])
        directory, summary = self.evidence(result)
        self.assertEqual(len(summary["process_exits"]), 2)
        self.assertEqual(json.loads((directory / "request.json").read_bytes())["assignments"], [[0], [1]])

    def test_timeout_crash_signal_and_source_drift_fail(self):
        for body, message in (
            ("import time; time.sleep(5)", b"deadline"),
            ("import os; os._exit(7)", b"trailer"),
            ("import os, signal; os.kill(os.getpid(), signal.SIGTERM)", b"trailer"),
            ("from pathlib import Path; Path(__file__).write_text('changed')", b"trailer"),
        ):
            with self.subTest(body=body):
                self.module(
                    "test_a.py",
                    f"import unittest\nclass A(unittest.TestCase):\n def test_run(self): {body}\n",
                )
                result = self.helper(seconds=0.5 if "sleep" in body else 10)
                self.assertEqual(result.returncode, 1, result.stdout.decode())
                self.assertIn(message, result.stdout)
                _, summary = self.evidence(result)
                self.assertFalse(summary["successful"])
                self.assertTrue(all(x is not None for x in summary["process_exits"]))

    def test_output_overflow_and_pipe_backpressure(self):
        self.module(
            "test_a.py",
            """import unittest, os
class A(unittest.TestCase):
 def test_output(self):
  for stream in (1,2):
   for _ in range(32): os.write(stream,b"X"*8192)
""",
        )
        result = self.helper(text_limit=1024)
        self.assertEqual(result.returncode, 1)
        self.assertIn(b"Text output overflow", result.stdout)
        directory, _ = self.evidence(result)
        self.assertLessEqual((directory / "worker-0.log").stat().st_size, 1024)
        result = self.helper()
        self.assertEqual(result.returncode, 0, result.stdout[-2000:].decode())
        self.good()
        result = self.helper(evidence_limit=100)
        self.assertEqual(result.returncode, 1)
        self.assertIn(b"Structured evidence overflow", result.stdout)

    def test_cli_rejects_unknown_counts_and_options(self):
        for number in (0, 3, -1):
            result = self.command(number)
            self.assertEqual(result.returncode, 2)
        result = subprocess.run(
            [sys.executable, "-B", str(self.root / "scripts/agentic/check.py"), "--filter", "x"],
            capture_output=True,
        )
        self.assertEqual(result.returncode, 2)

    def test_protocol_mutations_cannot_manufacture_success(self):
        sys.path.insert(0, str(SOURCE / "scripts/agentic"))
        import check_runner

        self.good()
        result = self.command()
        self.assertEqual(result.returncode, 0)
        directory, _ = self.evidence(result)
        request = json.loads((directory / "request.json").read_bytes())
        path = directory / "worker-0.jsonl"
        records = [json.loads(line) for line in path.read_bytes().splitlines()]
        self.assertTrue(check_runner.reconcile(request, 0, path, 0)["successful"])
        import copy

        mutations = []
        for key, value in (("worker", 1), ("request", "wrong"), ("version", True)):
            changed = copy.deepcopy(records)
            changed[0][key] = value
            mutations.append(changed)
        mutations += [
            records[:-1],
            records[:1] + records[2:],
            records[:2] + records[1:],
            records + records[-1:],
        ]
        changed = copy.deepcopy(records)
        changed[1]["position"] = [100, 0]
        mutations.append(changed)
        changed = copy.deepcopy(records)
        changed[-1]["tests_run"] = True
        mutations.append(changed)
        for changed in mutations:
            path.write_bytes(b"\n".join(check_runner.canonical(r) for r in changed))
            with self.assertRaises((check_runner.RunnerError, ValueError)):
                check_runner.reconcile(request, 0, path, 0)
        path.write_bytes(b'{"event":"header","event":"end"}\n')
        with self.assertRaises(check_runner.RunnerError):
            check_runner.reconcile(request, 0, path, 0)
        path.write_bytes(b"\n".join(check_runner.canonical(r) for r in records))
        with self.assertRaises(check_runner.RunnerError):
            check_runner.reconcile(request, 0, path, 7)

    def test_interval_closure_empty_groups_and_deterministic_ties(self):
        sys.path.insert(0, str(SOURCE / "scripts/agentic"))
        import check_runner

        suite = unittest.TestSuite([unittest.TestSuite() for _ in range(7)])
        rows = [
            {"position": [i, 0], "module": module}
            for i, module in ((0, "a"), (1, "b"), (2, "a"), (3, "b"), (5, "c"), (6, "d"))
        ]
        self.assertEqual(check_runner.assign(suite, rows, 2), [[0, 1, 2, 3], [4, 5, 6]])
        self.assertEqual(check_runner.assign(suite, rows, 1), [list(range(7))])
        rows = [{"position": [i, 0], "module": str(i)} for i in range(7)]
        self.assertEqual(check_runner.assign(suite, rows, 2), [[0, 2, 4, 6], [1, 3, 5]])

    def test_module_errors_leave_incomplete_descendants_and_preserve_other_worker(self):
        for phase in ("setUpModule", "tearDownModule"):
            with self.subTest(phase=phase):
                self.module(
                    "test_a.py",
                    f"import unittest\ndef {phase}(): raise ValueError('MODULE ERROR')\n"
                    "class A(unittest.TestCase):\n def test_ok(self): pass\n",
                )
                self.module(
                    "test_b.py", "import unittest\nclass B(unittest.TestCase):\n def test_ok(self): pass\n"
                )
                result = self.command()
                self.assertEqual(result.returncode, 1)
                _, summary = self.evidence(result)
                self.assertIsNone(summary["error"])
                self.assertEqual(len(summary["workers"]), 2)
                self.assertTrue(summary["workers"][1]["successful"])
                self.assertEqual(
                    summary["workers"][0]["occurrences"][0]["state"],
                    "incomplete" if phase == "setUpModule" else "completed",
                )
                self.assertIn(b"MODULE ERROR", result.stdout)

    def test_controller_interruption_terminates_owned_group_without_retry(self):
        self.module(
            "test_a.py",
            """import unittest, os, time
from pathlib import Path
class A(unittest.TestCase):
 def test_wait(self):
  Path('worker-pid').write_text(str(os.getpid()))
  time.sleep(20)
""",
        )
        process = subprocess.Popen(
            [sys.executable, "-B", str(self.root / "scripts/agentic/check.py")],
            cwd=self.root,
            env={**os.environ, "TMPDIR": str(self.root)},
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        self.addCleanup(lambda: process.poll() is None and process.kill())
        limit = time.monotonic() + 10
        while not (self.root / "worker-pid").exists() and time.monotonic() < limit:
            time.sleep(0.01)
        self.assertTrue((self.root / "worker-pid").exists())
        pid = int((self.root / "worker-pid").read_text())
        process.send_signal(signal.SIGTERM)
        output, _ = process.communicate(timeout=10)
        self.assertEqual(process.returncode, 1, output.decode())
        self.assertIn(b"interruption", output)
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)
        _, summary = self.evidence(subprocess.CompletedProcess([], process.returncode, output))
        self.assertEqual(len(summary["process_exits"]), 2)
        self.assertTrue(all(x is not None for x in summary["process_exits"]))

    def test_worker_rejects_source_inventory_and_assignment_mutations(self):
        sys.path.insert(0, str(SOURCE / "scripts/agentic"))
        import copy

        import check_runner

        self.good()
        result = self.command()
        self.assertEqual(result.returncode, 0)
        directory, _ = self.evidence(result)
        original = json.loads((directory / "request.json").read_bytes())
        mutations = []
        for key, value in (
            ("version", 99),
            ("jobs", True),
            ("assignments", [[], [0]]),
            ("rows", []),
            ("source", {"checkout": "wrong", "files": {}}),
        ):
            changed = copy.deepcopy(original)
            changed[key] = value
            mutations.append(changed)
        for index, changed in enumerate(mutations):
            request = directory / f"changed-{index}.json"
            request.write_bytes(check_runner.canonical(changed))
            evidence = directory / f"changed-{index}.jsonl"
            result = subprocess.run(
                [
                    sys.executable,
                    "-B",
                    str(self.root / "scripts/agentic/check_runner.py"),
                    "--worker",
                    "0",
                    str(self.root),
                    str(request),
                    str(evidence),
                ],
                capture_output=True,
                timeout=10,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(evidence.exists())

    def test_lifecycle_clock_terminal_and_execution_bounds_refuse(self):
        sys.path.insert(0, str(SOURCE / "scripts/agentic"))
        import copy

        import check_runner

        self.good()
        result = self.command()
        self.assertEqual(result.returncode, 0)
        directory, _ = self.evidence(result)
        request = json.loads((directory / "request.json").read_bytes())
        path = directory / "worker-0.jsonl"
        records = [json.loads(line) for line in path.read_bytes().splitlines()]
        for field, value, index in (
            ("monotonic_ns", -1, 3),
            ("monotonic_ns", True, 1),
            ("kind", "subtest_success", 2),
        ):
            changed = copy.deepcopy(records)
            changed[index][field] = value
            path.write_bytes(b"\n".join(check_runner.canonical(r) for r in changed))
            with self.assertRaises(check_runner.RunnerError):
                check_runner.reconcile(request, 0, path, 0)
        for limits in (
            {"seconds": 841},
            {"seconds": True},
            {"seconds": float("nan")},
            {"text_limit": 33554433},
            {"evidence_limit": 16777217},
        ):
            with self.assertRaises(check_runner.RunnerError):
                check_runner.run(self.root, **limits)

    def test_interrupt_during_spawn_is_registered_before_cleanup(self):
        self.good()
        script = self.root / "interrupt-spawn.py"
        script.write_text("""import os, signal, sys, subprocess
from pathlib import Path
from unittest.mock import patch
import check_runner
original = subprocess.Popen
def launch(*args, **kwargs):
 child = original(*args, **kwargs)
 Path('spawned-pid').write_text(str(child.pid))
 os.kill(os.getpid(), signal.SIGTERM)
 return child
with patch('check_runner.subprocess.Popen', side_effect=launch):
 sys.exit(check_runner.run(Path.cwd()))
""")
        result = subprocess.run(
            [sys.executable, "-B", str(script)],
            cwd=self.root,
            env={**os.environ, "PYTHONPATH": str(self.root / "scripts/agentic"), "TMPDIR": str(self.root)},
            capture_output=True,
            timeout=10,
        )
        pid = int((self.root / "spawned-pid").read_text())
        try:
            self.assertEqual(result.returncode, 1)
            _, summary = self.evidence(result)
            self.assertEqual(len(summary["process_exits"]), 1)
            self.assertIsNotNone(summary["process_exits"][0])
            with self.assertRaises(ProcessLookupError):
                os.kill(pid, 0)
        finally:
            try:
                os.killpg(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass

    def test_later_class_lifecycle_cannot_borrow_earlier_skip(self):
        sys.path.insert(0, str(SOURCE / "scripts/agentic"))
        import check_runner

        self.module(
            "test_fixture.py",
            'import unittest\nclass A(unittest.TestCase):\n count = 0\n @classmethod\n def setUpClass(cls):\n  cls.count += 1\n  if cls.count == 1: raise unittest.SkipTest("first occurrence group only")\n def test_first(self): pass\n def test_second(self): pass\nclass B(unittest.TestCase):\n def test_middle(self): pass\ndef load_tests(loader, tests, pattern):\n return unittest.TestSuite([A("test_first"), B("test_middle"), A("test_second")])\n',
        )
        result = self.command()
        self.assertEqual(result.returncode, 0, result.stdout.decode())
        directory, summary = self.evidence(result)
        rows = [r for w in summary["workers"] for r in w["occurrences"]]
        self.assertEqual([r["state"] for r in rows], ["fixture_skip", "completed", "completed"])
        request = json.loads((directory / "request.json").read_bytes())
        path = directory / "worker-0.jsonl"
        records = [json.loads(line) for line in path.read_bytes().splitlines()]
        self.assertTrue(check_runner.reconcile(request, 0, path, 0)["successful"])
        removed = rows[-1]["position"]
        records = [r for r in records if r.get("position") != removed]
        records[-1]["tests_run"] -= 1
        path.write_bytes(b"\n".join(check_runner.canonical(r) for r in records))
        try:
            accepted = check_runner.reconcile(request, 0, path, 0)["successful"]
        except check_runner.RunnerError:
            accepted = False
        self.assertFalse(accepted, "An earlier fixture skip must not credit the later A lifecycle")

    def repeated_fixture(self, level, *, fault="skip", cleanup=False, teardown=False):
        self.module(
            "test_fixture.py",
            f"""import sys, types, unittest
from pathlib import Path
level={level!r}; fault={fault!r}; cleanup={cleanup!r}; teardown={teardown!r}
def note(text):
 with Path("trace").open("a") as stream: stream.write(text+"\\n")
def fail(): raise ValueError("cleanup error")
class A(unittest.TestCase):
 count=0
 @classmethod
 def setUpClass(cls):
  note("A class setup")
  if level == "class": setup()
 @classmethod
 def tearDownClass(cls):
  note("A class teardown")
  if level == "class" and teardown: raise unittest.SkipTest("teardown skip")
 def test_repeat(self): note("A test")
class B(unittest.TestCase):
 def test_middle(self): note("B test")
def setup():
 A.count += 1
 note("setup attempt "+str(A.count))
 if A.count == 1 and fault != "none":
  if cleanup:
   if level == "class": A.addClassCleanup(fail)
   else: unittest.addModuleCleanup(fail)
  if fault == "skip": raise unittest.SkipTest("first interval only")
  raise ValueError("first setup error")
if level == "module":
 a=types.ModuleType("fixture_a"); b=types.ModuleType("fixture_b")
 sys.modules[a.__name__]=a; sys.modules[b.__name__]=b
 A.__module__=a.__name__; B.__module__=b.__name__
 a.setUpModule=setup
 def finish():
  note("A module teardown")
  if teardown: raise unittest.SkipTest("teardown skip")
 a.tearDownModule=finish
def load_tests(loader, tests, pattern):
 a=A("test_repeat")
 return unittest.TestSuite([unittest.TestSuite([a]), unittest.TestSuite([B("test_middle")]), unittest.TestSuite([a])])
""",
        )

    def records(self, result):
        directory, summary = self.evidence(result)
        request = json.loads((directory / "request.json").read_bytes())
        path = directory / "worker-0.jsonl"
        return request, path, [json.loads(x) for x in path.read_bytes().splitlines()], summary

    def refused_records(self, request, path, records, exit_status=0):
        import check_runner

        path.write_bytes(b"\n".join(check_runner.canonical(r) for r in records))
        try:
            self.assertFalse(check_runner.reconcile(request, 0, path, exit_status)["successful"])
        except check_runner.RunnerError:
            pass

    def test_module_and_duplicate_runs_match_standard_and_refuse_missing_later_lifecycle(self):
        sys.path.insert(0, str(SOURCE / "scripts/agentic"))
        import check_runner

        for level in ("class", "module"):
            with self.subTest(level=level):
                self.repeated_fixture(level)
                trace = self.root / "trace"
                if trace.exists():
                    trace.unlink()
                reference = subprocess.run(
                    [sys.executable, "-B", "-m", "unittest", "discover", "-s", "tests/agentic", "-v"],
                    cwd=self.root,
                    capture_output=True,
                    timeout=10,
                )
                self.assertEqual(reference.returncode, 0, reference.stderr.decode())
                expected_trace = trace.read_bytes()
                for jobs in (1, 2):
                    trace.unlink()
                    result = self.command(jobs)
                    self.assertEqual(result.returncode, 0, result.stdout.decode())
                    self.assertEqual(trace.read_bytes(), expected_trace)
                    request, path, records, summary = self.records(result)
                    rows = [r for w in summary["workers"] for r in w["occurrences"]]
                    self.assertEqual([r["state"] for r in rows], ["fixture_skip", "completed", "completed"])
                    self.assertEqual(rows[0]["id"], rows[2]["id"])
                    self.assertNotEqual(rows[0]["position"], rows[2]["position"])
                    fixtures = [r for r in records if r["event"] == "fixture"]
                    self.assertEqual(fixtures[0]["interval"], [0, 1])
                    self.assertTrue(check_runner.reconcile(request, 0, path, 0)["successful"])
                    changed = [r for r in records if r.get("position") != rows[-1]["position"]]
                    changed[-1] = {**changed[-1], "tests_run": 1}
                    self.refused_records(request, path, changed)

    def test_fixture_intervals_refuse_forgery_misplacement_overlap_and_legacy_versions(self):
        import copy

        sys.path.insert(0, str(SOURCE / "scripts/agentic"))
        import check_runner

        for level in ("class", "module"):
            self.repeated_fixture(level)
            result = self.command()
            self.assertEqual(result.returncode, 0, result.stdout.decode())
            request, path, records, _ = self.records(result)
            self.assertTrue(check_runner.reconcile(request, 0, path, 0)["successful"])
            mutations = []
            for field, value in (
                ("interval", [0, 3]),
                ("interval", [2, 3]),
                ("interval", [-1, 1]),
                ("interval", [True, 1]),
                ("interval", None),
                ("sequence", True),
                ("sequence", None),
                ("sequence", 1),
                ("id", "setUpClass (foreign.A)"),
                ("id", records[1]["id"].replace("setUp", "tearDown")),
            ):
                changed = copy.deepcopy(records)
                changed[1][field] = value
                mutations.append(changed)
            changed = copy.deepcopy(records)
            del changed[1]["interval"]
            mutations.append(changed)
            mutations.extend(
                (
                    records[:2] + records[1:],
                    records[:1] + records[2:4] + records[1:2] + records[4:],
                    records[:1] + records[2:-1] + records[1:2] + records[-1:],
                )
            )
            for i, changed in enumerate(mutations):
                with self.subTest(level=level, mutation=i):
                    self.refused_records(request, path, changed)
            old = copy.deepcopy(request)
            old["version"] = 1
            changed = copy.deepcopy(records)
            changed[0]["version"] = 1
            changed[0]["request"] = check_runner.digest(old)
            self.refused_records(old, path, changed)

    def test_setup_cleanup_and_teardown_callbacks_preserve_standard_outcomes(self):
        for level in ("class", "module"):
            for fault, cleanup, teardown, expected_exit in (
                ("error", False, False, 1),
                ("skip", True, False, 1),
                ("none", False, True, 0),
            ):
                with self.subTest(level=level, fault=fault, cleanup=cleanup, teardown=teardown):
                    self.repeated_fixture(level, fault=fault, cleanup=cleanup, teardown=teardown)
                    trace = self.root / "trace"
                    if trace.exists():
                        trace.unlink()
                    reference = subprocess.run(
                        [sys.executable, "-B", "-m", "unittest", "discover", "-s", "tests/agentic", "-v"],
                        cwd=self.root,
                        capture_output=True,
                        timeout=10,
                    )
                    self.assertEqual(reference.returncode, expected_exit)
                    expected_trace = trace.read_bytes()
                    for jobs in (1, 2):
                        trace.unlink()
                        result = self.command(jobs)
                        self.assertEqual(result.returncode, expected_exit, result.stdout.decode())
                        self.assertEqual(trace.read_bytes(), expected_trace)
                        request, path, records, summary = self.records(result)
                        self.assertIsNone(summary["error"])
                        rows = [r for w in summary["workers"] for r in w["occurrences"]]
                        self.assertEqual(
                            [r["state"] for r in rows],
                            ["completed"] * 3 if teardown else ["incomplete", "completed", "completed"],
                        )
                        fixtures = [r for r in records if r["event"] == "fixture"]
                        self.assertEqual([r["sequence"] for r in fixtures], list(range(len(fixtures))))
                        if cleanup:
                            self.assertEqual([r["interval"] for r in fixtures], [[0, 1], [0, 1]])
                        if teardown:
                            self.assertEqual([r["interval"] for r in fixtures], [[0, 1], [2, 3]])
                            changed = [r for r in records if r.get("position") != rows[2]["position"]]
                            changed[-1] = {**changed[-1], "tests_run": 2}
                            self.refused_records(request, path, changed)

    def test_weighted_parent_request_balances_retained_complete_inventory(self):
        import base64
        import types
        import zlib
        from unittest.mock import patch

        import check_runner

        # Complete 803-row/476-source scheduling input from the 44eeac9 request
        # 938cd3a9009bfdc1d99232dec4559e42c684904d7394354316f25d925d6fd045.
        # Compressed inert metadata keeps the fixture bounded; no test is invoked.
        fixture = json.loads(
            zlib.decompress(
                base64.b64decode(
                    "eNrMvWuT48iRJfpf9Dm7Gu/HfNP09MyOXY1GJunO2LW1MVoACGRCRQIUQGZVam3/+z3u8QDIBEESRYBts6uuymISfhAR7scf"
                    "4f5/fie/72V+kMXv/ul//2/vJXxxnRfXfXH9Fzd4cdMXz33xvBfPf/HCFy968eIX33nx3Rc/ePGTFz99CZyXwH0JvJcgfAmi"
                    "lyB+CdKXEN8UvoTxS5i8hOlL5L1E4UsUv0TpS+z8z8v/xjNe/JfgJXqJX5KX9MX1Xlw8Onpx4xc3efGcFy948fCH9MX3Xnz/"
                    "xQ9f/OjFx5fj14KXAF/rvITuS+i/hMFLGL1EzkvkvkT+S4RvxV+T//mfl9+1zbcOuP7P7/Kt6PCn3x1kd9iINn+r3mX35ffq"
                    "D3/FD7vfvfyuKiY/8YX/KW/q/Ni2sj5suubY5nKTv4n6VW72rXzHTzvz41bumnexxffumuK4leffjX/YN111qJoaIjov+L//"
                    "+b8vDxW1bbpuU1Zb2X10B7mD7PuPTdVt3mVblZUsNpksm3aOqO6DRS3wP1Ut6AmQcrutOvoTXmknW/zGJmsOb5uuKliWW4X0"
                    "HixkKfAqC/32ciVsKw+iqu2iixovVRTqTePd0r+3H3fI7D9a5qpsaMlbWR67fsW1uIdW1F0p2zsEDB4sYFUfZNse99BAELIW"
                    "O6nEpXPGr/tbdXhrjodNg/f+ra0O8g5hwwWF1a/wWG+r+isE7o47bNSybXabvayLqn7d/A0fqcV2g8O3v0Pq6MFS69fanyaS"
                    "uLMvtpWCpT28yardHET7Kg/3HLP4weJ2Hzt+pUOdIOocf8N5wt6oG2xr6IjmG4zW7WImn8XM32SOpTvWtWy//Jn/81nSSx/S"
                    "OnZb4QX+DRa0w1b4WjffSIEdyQ6QLmj2JMHntzn80lNR3XFD8AhRm/rQ4r3Jtt/H9G7xxx29Z2wMSI+d/do2x/1ge4xpsGkA"
                    "7jIAjtgCu013hBIY6jQrKX4BG/lOWb1FZCVbccSBg7QZ/QB/+Frt1aYoq+8H/NsGS9C0+BH+SHt623SHO2X3l5Fdy8e7BLzA"
                    "vGkyaK+y/djsqm6/FbncEQsivbwVe0a2la8i/yAL2c3Y9sEiaKrdvmkP6mWzkHK3P3xsiqrLSfQPXqo7JQ2XkdScyk1xbEkj"
                    "d3sBdcI7/bWCEWl7A55vpaiP+zvljpaTG/sEQjUdb2x+w6xG1I4vpFIyAFHlm0Ml790a8SKCb6H02g1/K2xiKfMPvNZNLmo6"
                    "jlmDHfNtI0W7rfApOr53Cr2MxRkIum3wOa2+t2pvf5f5kbV6BiNUmJN7p+DpIoIrCVhM9crplVpWApKEfxEtiNUxh7G/d4e4"
                    "ztJCF8f9lmi/pA93m5045G+91PQJrSahHjs6vmp72fW6F5C7JCBte6BF3knR581uv8UhJdKVg70KQ2Ds8sABA5hvTft1xF24"
                    "gmQZEwuTv9d+QQkyqMSt9hIOWP6V5CZddK+oy1jUfdvQloYqb7aG0BbqiEKlH741+sVu8NjqtSa7eq/gwVKCHxo45Jvd8cCC"
                    "d0Y97mB+SpEzUZh5YpexotiuFbThXrQCXHc7OLZNriI3kPWEh8ELph/xgb4XQ7QQhgMYuLbxLOsBdqhg5wKoaIN3I5oTJwIH"
                    "+W7j6i5jXQ/VThItz1vRUfDm1Rgp7TkXbVUe5rAvdxnD2h9DbI+cnTgO4UDxbax+hEf6oTyQe4VexqhqvWFcUP1qq5pCkQ3I"
                    "Lb3vXqn0x/heJ2nMum7FscCrASvFV+PMdV9+sX8ewXH14wqR2Febr1JJ/u0NLxosmCJrHKf8mwpVfxL+03efQvAuONXLIOBX"
                    "STufN7uKZkDNF1UL6XlRGAs7r7OguKtBeasK/FDrIxiD4kiact9W76RQ2YBRrE5WcFpsoI7t8ixg3mrADEeT3/cVuVXlkU2B"
                    "iuVbTNbzNdg6iW/WzmQ3C6K/GsSmLeAXYLvpX9g2rxUFrHccsIZ2a47YqfN2YLAaCrX1Wvn3Y0XhFLPZ8GDZWibVSpgWvVer"
                    "eu7uC1cDBf9BlJJTM0TIKUyk1ES3eYMhV39SFKVsflhdRBdxFZV4rRtyyrsv/2L/fBHXxMcVLpwmcK2KnNRaildp0yNNxlSl"
                    "2LxK2BdFgc2ZqiW4++FAQYOLR2rw5FN8/qRmfzQ+nQEiDmYPkqYKUrmAmh2DGLOKnIXHXQ2P0YKH5itODqer1PHKQRE4n1E3"
                    "Zm1mQfHWg9IUKqcpv+PlAwhH/QAJ7OGrpCDVu9hWBYe59eqQmHNA+auB6mXXdK4q6CyBHREp7Y57HDe2Vgy2gzrfiVmQgtUg"
                    "EcvWOwpaYlcplsS+F+3EfQMF8vGjK3RZk/cBsvf4yy/8s1/Nj/4rvgjrhl/T7FVD6/1l0hkbUVIoSElJHpt452XrjtuL52r4"
                    "yFOEwaTOWxphLvba6W/fKdPGprjr2L3QWQ+r2gURpqZWGnMeUPd5QLei2vVQ9MJWRJ1ycA8o/6HGb6WkMPg8kN7TQCqnZBAk"
                    "MTi7Q7PvTLZBHKFvQC/yySM5jdF/GkaVjlBKknSMISXi+L3aVsSQO0nhooPcKreTsc9DGTwNpeYmJtT4VUqs34kxzJhsvUMR"
                    "/f0IW1iOpmFuAfk89ToswCiqbs8xeBxJzVf6XLDRUhdLcG4CGj0NKNU9vGsPm3ZoVlE2R9bvVduoUA4Wms6pyvqoVZ5vTuLn"
                    "AW0Gxn6TUUrLqp2uFvvurVE0rm3eq2Kugk2ehq9XO3rB+jIc8iKgh6j8kr6oybbV66Xo3C0g0+eB5LAh0xqjc4C2KQcq9iw7"
                    "aQoPVQ5+JjW4TIL08SG79eWP/Off448XgU58XNM6TnBsmhoWAsj+ZpYM/JVjRFjTN5WAotKH7kAHVJWrVP+YtJmDJ58CDCdJ"
                    "3sPxbaFXa7b/+Acb/mZEoio2x468+WEM9jwkNh31m4TprgbTVO6SNqUjOYhGHMRuT1v4ILayjzJR9Fy/C/jKKuV7KaV+A1Jv"
                    "NaSyLvZNRWaCygM6LqdVJTBaxaq42oDfzQLkrwaoPEJbmpSSOXE70VJMUxVCCCoJpEJBIgTdoa3yedsxWA3T61G0lACmXCqV"
                    "ifKSqHhmLb/hJ0TiBptUxQQVhdNlTRzUnQUzXA1mtQMzq0i5wPbhd2ocIqqKoNXU5SC8fsogUPLkcqBzElK0JiQ4+Nn23DWC"
                    "tvzYNqKwCTldEadiHFsctt1MbPFq2PQHqCyYM4s68o5teKxqCRjfuVwRmvJwpPCGykfPApWsBwqsGYJrJ4mW46SWm08eEH3g"
                    "x0xH5bX61xvQpauha7ZFn6N7l7XRgoZfMXGZZ5vX4yB4Mr/tY1v35uv8co2qhGzncyp3PbZh0qRUuG4yOKOZHpXlIUXYXy5q"
                    "9kbpz4PprQjTbjmTTyCaQbdgtpUJj+GDUJcVbLb+vvmGy12PdHBe8bU6z+TvBS/RIC9ny3LxG/O3ZrAisHd1xYrC8Wp/6psV"
                    "x/1rK4iUgFdxkfc8KOuxC6X3bDDIHDO+8sL+GjajTQ5lnBGfBylaGdIg541deFLm22e/9b7ji1EzNcV6zMLGcxiR2nZ1Y9l8"
                    "3uzgTxOZoDWr6jdJqArtkjJPvFD0fgvMCa4BVia22lH/8sfh3y5jvfI75pixtfoYbk8BhtsZWU++5iKusw8NkEWTUQIbe/ry"
                    "Z/Oni4AufVYB0dXkpPGUZ6VuN/FFj3/QufrgvKq+yYLvIs1xkejaZ52CiVcCQ0tgTlexYWIxtFw2F1IoCjVdczaBxV0DS5eL"
                    "vQr7N+WGFmPstieXxF7ZZRNAvDWAcNSQimG2m7IVrztpStAHMUTGolI12k/GRvsgP2wGKn8FVIPKeuLiLd/IIJmte1GKrGXn"
                    "EZvPbEI+TTMQBSsg0oprEAQtGqmog9FT0hRbqFgT/lbI77MAhU8BZHx3OjvVa72puLTq8HGqiPXFphJO5NVc/gTE6EkQdV1W"
                    "W70yq7AYWa8TbdJJwx1dYqnEdga0eA1oDcHqqKRi+04xaijwJiOYCsW18MSE+MkK4kMjSL7qx9XObYM/QUtQlOzksgVX+tgl"
                    "6R2uGajSFVDtq7rub8j3CS9Zv8stHNvuTMPv+RZG04Dczlopdw3WoF8+36IWmgD9rSM9YEnRHNHdVURnkbEk5AtBT1V7vue4"
                    "3dodlpFl3arMTvbR12OpszQH2BqkQdVRKe6jw127/dHUYoo2qwCcrvhWclsM+Slpv3mc7jJrOOD97ShYqFOlfzV/vwhu+jdO"
                    "mTcVA59WeIh2eHPvEhb7jFMsySTXXgjKscLWA2GQ+CWT0SgaXhJy2mCMwB2gxMHHSRuQeTrNonYzULqro6wLWqbua8XnS3Vd"
                    "oO1pq6MHZWM4d5REnnAwJqB5K0PTHwW7a3S6A6hE1jXbo7kNQlcpbDKcbnzvLlOHCWRrnzKzSvi5KMRBqNJUdeyMkqneubeB"
                    "qiKecKQmYAUrw8Jy9M4sdaFRtyM6faVQR5T0RVqd6B7toHEVWbgyMlXTRxEjdZgOYtu8bo57KvDm/C8YE3S4rlPQ3uWkXzUB"
                    "LlobHIckmBvqc6T8wvq4k+QsbnU9hrqC8GNGIF4Z2+uxourt3gT0/iMn8PFPlDM9yEG50AxYycqwoOno9o3Zl3o/DniHiTfN"
                    "wJKujEXnrNWlEF2Qb9I4vCTsZWWEaSsyud3OQuWuTT9s/n1Q7WyvAXJpzN+PDVR/dixepTLSpqOTOm7jV5WvA12bgRhQcLx4"
                    "13WUGzApVOzUUvIl+DlQ1mYcOnLBxT9UkMCFeHQvU4Lx7wTffbYlvxSO5nWi7MFhDjz/OfDotpwp9a2wVHSbr9IBDFPbS2lx"
                    "bF366CwWHDwH2l60rBe196wLEozbzxlhWzKqkghmOX9on65NR3TEg44b+6UH0doKGYL6yerNAbU2DSGNUTXHDrvQXBgoNu/u"
                    "Bgx5z42TzlP7pnZBdRQc7QhyHeXahITVii3uVJVBNmhPfgEh0l6qFG1t277yIZ2DMFkdoS7ZNSTFeDuanqhwyIavJHdvYj9P"
                    "wazNUjp5IF/FRrBkzfV4b03zlQMH1yK/U3712tREbyn53cTpegLJLWo6bfe0SWCOqXydOejcZ6ODkoBrAw1i4M1B4T0bhVbv"
                    "6sT03XhIdwiTZSnBHOdwRm9tJqJbOIFo9fWflNHqtOr/GPrRpsM1lebNWru1yYgNdZzWKXQf9QH0gysYoDHs0s6BtDbjMK6J"
                    "bu3N8W6OnBakzbcKpAnJvQmiWXNgRavDYjssbS9RfYZ01TunvjpLNPpL4Jb9zwG5NuUAhzL3E22FrmknoU7YSYSYenoxTfmY"
                    "A25ttvHuWZ9FQzMVa/qSuOZSr8Q53n1r6jq8gFpf578b5NrkQ6VqbdIcHmemus+ovAXHDiqTR1d6dHjzds5C+pdJyXugRf2v"
                    "4CKq88+Ye19bm5bZb4+vtAOLqiMm1d/wUr0MqB/tm2hVtTwzr0sg3oNT6dPJXNN84S1Lxy4a7C8q4+82bxXVF3KwdHCYel/l"
                    "qo8yhsJdAIU6LDq0a1PkyhGxhScUESWjDOXYVt/vEdlbQOS+eRYMJl2tyLDrpeAOMoqGF3ZXaV/jU02aqsTlZbgHjr8AHE14"
                    "/iHbhgoTFHfjfoWsolWhGTa90Vz6TsL1fMkYgGAJAHrzqC5L5qrYwP4TBJ24avpmgVWt/cJpp28MRbgAChtDUTc8VOpD9bRR"
                    "N+XuETBaQsC2eaW2slqV44/5kfqG6tOggsTUMJJswD3CxgsIC514ZoY53KbztlodKhscXLe9Y1JfJhbvoZEovCz12Wd6eySK"
                    "96pr+qqNvlLXUqO62dDGLqmIA7sfv9QdM9DAw2S66D08a2zpTBum2SjMsTM540GvIrt3OOTSim+2sQFs//fDXaK7C4iOjUFq"
                    "eWsS/IePvWlR13el0wpTfudOMW3TXG7cNiq4t4jgB/nacPjcuEYnNY07HFyTxOFG9ZfdvFGZ/SVktmHGN9oNTBdtaxbdXfO0"
                    "ZfSglm7Q2eUuIMGSQKBLRvXNJ052OnxmioaNYQgXwFBA7oJDb3wC5Pe9GmYwKKenBBrdizJF91T+d5fc0QJyq5QszI5uBzcY"
                    "76Wh6BLLPZXJ2ms0ne5JNhwFcxeWeBEsumhDR5jsJJLOlsUSm2HBLSfGC2jzqpP3baElrFfZNv+QbFD7c6oTX0MTcJZjGJyT"
                    "uxCkCyAwhfs6U2BK3YnUmBCaMmDDGUc8GUAPFNBd4e6zY0vYYMPKBp3NSFZTb60JkG5zci1SOy72EvbX5EunFWnPhK7X/ozL"
                    "voQJpo3OORhzbPtToBnDYEpeX23cCmoI0ae/7wOyhF02+l7rT73j2euzLdZxJBThl8TeJtLU42IvYYX7KLdub7M7dlxhyutQ"
                    "U3C1+vtR9i3D7hN5CaOr42iqKPFbK/bUP8kkI3QJjlH7RsOoLg3ztv2EAY6MfNFlDGefWcNnic5guNM+y2wUC/gsY6K7C4jO"
                    "7Zgt2efNpMNntPML1v7dXVJ6i0j5YM9qTHB/EcEf51mNyRwsIfPSntUYkHBJIO/hwz2rMQzRIhj0lanBJWvVbk0Wtl/DXZ1m"
                    "R0WPFxD90U7hmNzJEnKrtr6kGaGlefsz6drLnGYXz9WL6QKiquP2ivN2ercO9pLrInUSjTq8Htpjfve2cJ1FhF7e6R4F4y4C"
                    "ZhGve1T+JSyrcbvDh7vdoxCWsLGL+92jSJawvH2VPV/o7a/82hEgWtOblij3ibyEjX1sqGBU7CXMat3U+qa7arPAWTw6tPZN"
                    "28p47kpFt8wnG/GPi76EWf2UqFS08SRfqU/1m9xSX1ZqGsmjme4TfgnbaucqwULpIaCSxo73F5n6WSP6kA6q666GC0ZxpEvi"
                    "MAXNfB9cJYcNc1A9LvhC1r160lvC7trwWPiw8Nio7EuY2aXDY6NAlrC3jw2PjYq9hI19XHhsVOQljOmC4bFRDFPWVa9sp1u7"
                    "/Vf0F/X3cTSXPj10Txqu/6nNLVeywVs9FK2oyiu3f/pHnAHxrgTIHomD7qV9cCSnUB7MaYcgNSvmmt2awOGuhEOJTYNw6063"
                    "mDmNhjTsquAjJ47lLEzeSphOitQH4wco+nqySllTkAP3JtirIbTT5dKT6CYUlxmhMDFw4fwzOsD8ecBS39uXLUp2HA6aMLXF"
                    "gxYT+CC1iDzIyzb8bMCCOz1Zbz4aKx3dk2irbngDXP79qOaKNspb5jsH5Cy/ysvjNkcldxeUfFCzTa3cOt2A5tPFRTXkmwe3"
                    "2B52d4HwlgCxrUwkNO+jn8earywOp+ro5gLsW0yorzG5lzgEptUDH+TBGBx6v7AZ8JsHfXvsWnA1Kvt217zNMRzBAjh080kj"
                    "KPHZ0rBVFZc224zPuHEk7luAcAHBTTc8Q8G7A7xKdfGkL38/6Sn1RndeM/lWcXjd9J67C0i0BBDKvdmy5O5Q8d1y9npY36hI"
                    "zLAH1O3yxkvJq9+2mupl5nmpy0zs5hRyT0yK7/ebiUJ3SZ4sILkJrGh5KZ6o9jbdBxnG4uyQvX4i2y250TEc6QI4BuEKw8KH"
                    "DIltgQ5XmMPLeeD+UN9nu5YwuzQiwLI4AUBkoOwkvJ5hqAkj3+zQPGpayveeeRbs9KXncTRLmOK+87DqzmTV6Ofmt9yjD0qr"
                    "u/csu0uYX6M9h1vKkDo7UfMT0eB2TZwxU8eJU2b3gVnCJtuK/YOR8djfj5ff6Yh0NsSnbTQ3+tlXKmmvotn3AVnCKJups7Vp"
                    "Jng2vvVKVntUziVs8CCOOKAQ26aWOgqjDupME+AuYW05C6BvcWA/NFva4HTbTO0d8bd3StJsjfXlcDV5zAO7YdKC94FZwhTD"
                    "9RuoyqozWn9ww4yvVPSLc/1mxajwE9Y4+fKLUg+XZT/9yKBKyWhJHQG7LFRyJtT0yOG5Mp3rubvEcR8vjuaEd4nhPVwMFYOQ"
                    "3+Gs84gOxVf1q3oTbIeHYyNNpPWKbR4T3n+48MZLGHTPukui4OESnZLQu4QJHy4MT7uwuuGWRjKjgkUPF+zM7N0lTfxwaY61"
                    "KEupO6GojU8FlfDMbFyF5t9hYSn6pRKf1/KyY5JPqtlf4YBQ9/Mp0U8+c1qNEFtnjR1fdV1WHek7ZHSn1e5MGU25gdT/dFJQ"
                    "YKTsfeOpeZejMrsLyFxD1zGX72+7qmus3M1DV6hyXxlu5iGK6ba6o3J7C8hts/HmZW/4+4ctY9RilK3YTfkoYwJPKvB/o0uz"
                    "U9L2H7B9R0Hk+a4tGxpBYTNsXWr5Qs2h+DmngcPeDaZ2OPfsEn96Z98t/NCj0k2zTS0zZ5fM7KG82X/cJab7UDGPNWUXhNVb"
                    "pv5BvVlixODMb3S7/EAzQSkAeByAEu19wnsPFV5tVCr6hoxV/VXZ1UL1Q2IHkNi+njCrA8i2E+9dco9t7KYuq9ej6p/35Zfh"
                    "30YwXPmwKXo/vA1aBWs3hWMLxm29nH0YfucZgkvDmx+PwPYU5xsFNCedxXhXKVSynifdkO9G4a6CwtgjI3N3Mmr6bqG9VYS2"
                    "ZWODF32suXG4Pg164rtp7aOrm+6Gs85ZUKOX2iONI1IOLU3i5Tl6ajkmUhFXAASrAFD0RXXvtXVMZmdRjl5fsWhqe0ZIucr7"
                    "j3a4Ch69e2hn2VGAVnKtlu4WPVpFdHv9iTqIym99r0aOpnGWRbVZYTuoKlfuhhKvAuXUJvQrQTa6txxzl2PMATFtjFv5agKo"
                    "kE798M/2Z2N4bv/FzzPX9BAAPQWU5tns94pJg+KV1ffRY//5gWcYL80nXA/jfiu4C9HOZjfwIUrUkHbrByxhgbryg7Fuq9e3"
                    "w1yw7lPBHmtqW8QVLrVJ4KgQqoJZSNXWSE7dDroN6IiNHXTjMxG9L3/Wf/iM7uqndVBOlU/ZuxQ6VE8N6HUZn8bWT5Dptg3x"
                    "UyzzJ3AjDz3DFo+73Mtgo/QH3aJsj/XAp2IKMcj36LZTqgcVgetm4nJXwmVJ6V7rfy7c4BFm1ue1k+fovhePIe59TZXlkHNh"
                    "rrU1y6Oq0+jz2FqJdodm3w0u4u1F1aoA0MTNwtuw+Wtj6y8qqftAtGMVu1UoO2p5PlaneRueYCU8gwuSun0bjULTDF6PtNXL"
                    "yL62cWEvJCZuAxeuBE6PJR9gNFXXI/lwpflfoV9aEOWZyKKVkPXBezutVwPD8qnqaGMY9JL9sPKPV4JWN2Aioj58nsXJM9kJ"
                    "xuXk4W1QkpWgUG2JMb+9GTOWG4q/bd7p4JkZMgdZqxgtfID3sajhbfDSleBZUmgrNWzBKt1KPe6kmlde6kJ27vRg74PSJdax"
                    "vsQ3muy1uEh3EHDF1I7k2IUt8yO6yH6bViCXxzLdCMldFZLhIHxHThXLaZum9qa9O/SJmszF562Gr2Ef7d2zmkQFmkjF27pw"
                    "lRqx7LjbvPtzga1FPuzZaWUtv/H0GK0iTS7lFf+gx4f1NIt9dFiEHzxxt1ESamjzZ+ws8o/+tXqXN4Ed/SVdn8FakhuwqP3K"
                    "mX78V9u3bqQds15/Xvxb0H5uinNhDOgKeE0Ld2sdqmHXHO6pUNI1v4qrhRS1+ZgP0n0KSDUKs7cfemcqL4/0kTYodgyqsSdc"
                    "Rz0frfcctCM+w+C++qlXZHfxvq3omjvNGP8BxP4zEZ8NuTq/hMrA50N7jj4aOBM4hdp8UMcfGtKoTEyn7EtzYD2lSv97Vncj"
                    "QRjHHD4Fswq8GIfCRGeGxaF6oXfVq7Y+fZRmqlD0ZtzRU3Dbfu6FTV2JXVa9HpujaS2kB3FIU97ygzYnfirO3kfs/UntPpbN"
                    "UZH4R+zi5CkorR+pSCF3V9L3GgczEYzZ7a9eTDZeuhlz+hTMp1T/EsNXnfz3UhxUNPIH6MRzSBNFBk44ofGg1TWsqjy86W54"
                    "NuzzAxifw5ks+e/Ezg5ZsJ5AX2/Uu6ojXsEPmFv3OeRJBbMGi6tiKOcs2bRZ0Nr4Ll/8At5bqVNkRf9L9f1GuJ9/Z01v57zH"
                    "QXqHt/NItLf5OtEcX2cMovsEiGt4OmNYvWdgXcnPGcPrPw/v47ycMWDBE4AN+JCuGB2SfFJHW1m8cmExa9vbelTeDDl8LmS6"
                    "zLSgWzeGOHoC4r7N66fovWVPBI9ewKarvmsKNR9k/ASQ63quY6iTJ6Be1m8dQ5k+ESV7reyffnJaqSJwjtM6yh6ewZDW81lH"
                    "IT+DMS3pso6C9J4CUpF2yhDz0vbN5KkPyukC0uVBy5V54thcqM9gSdO+eW9Y7vbNRxE+gy6t6ZmPgn4GYVrFLx9FeytZinvJ"
                    "yZe8Ee/Yb63pm59de/ecO3zzRyO+zT+P5/jnYzDdJ8Fcw0cfw+s9C+9KfvoYZv+5mB/nq4+BC54EbkV/fQx2+HzYUEJL+uxj"
                    "qJ9lhu7y2yUptHsc9zGk8ZOQruu8jyFPnoR8WQd+DGn6ZKSqrrf6/smH75hnzHHiRynGs6jUeo78KOxnUaslnflRoN7TgM52"
                    "6MMfgPssOjXt1A+szt1e/SjMZxGrNT37UeDPolarePejiG+lVYmV/VfabTciHvutNb37s5YennuHd/9oxLd598kc734Mpvsk"
                    "mGt492N4vWfhXcm7H8PsPxfz47z7MXDBk8Ct6N2PwQ6fDxtKaEnvfgz1s8zQXd79Aet7j3M/BjR+EtB1nfsx5MmTkC/r3I8h"
                    "TZ+MlJ17dgo+ufc1jTKd492PUoxnUSnmiSL/KofdidgkMbS+NTJP5VAWiVvCXWzhfDvmZ/Gq9SIao7CfRa+WjGiMAvWfBnR2"
                    "RCP6AbjPYlknx3FgatQFgrYbqC56M49mWe6zaNZ0JKdnGHcHckZRPotWrRnIGQX+LJq1SiBnFPEIvaJRF93bl3/l/3yG9vmf"
                    "dWgmzyVrWDIgTbnp8mYPatQwBz7uqX+GHt31Wc+q7zyT78IoxLniFe8CaoEU/q6hKW1qMmnVNzIubhfLfZhYps0IeBb1Tqn2"
                    "fVUkWW2eu37c3y6Z9zDJ1M/NuBbLEm2DtcFw8tvF8x8vnmqjbcTjVia6PbiZcn88VnesbfB4EXXTRSuQ4h00m4j247G+4wWG"
                    "j5aOF1e8vrY0JVknBlhfKJZ4rNn/uzxs/qKk0cMkxVcc+peH92Vn35rWZcUJs7lI1y/KGj9QVnKQSymLDJ5Gv/agYwP/QrGS"
                    "nWxH+nxcFPJxqpofrBqXmnAyLK/2cvUwc5yn4yCsoYa0362O0ofJTAT3TQqelsjTX+wmMP36th/ah7tLkz/OwuypVZ4Ogtj3"
                    "pl41dWtrqO8tNGY7GPh1x8F3H2dzhiPHaUXbDoZZHmVhdAK7wyqAZ87UcP/eIfTjzJGWUL1P3RWozd944hhpBUkvuNt8a9qv"
                    "h7H2aZdlfJxN0tSCRs4fVLtdGi1m94JWBlZGkhscqVZn7Q6JgwdKPNy1w7nJ3M7y1bzjzd+aYzvWVOqykOFCQhpC0kfj8BrN"
                    "/FetoDb7NzHWgfOytNHC0uqtobZvIbs9TZNT0//0zmaR75A4fqDEOp2ouwpPxW0vy/M488TeH+v6QuaUCLSKiXyITFpldJd8"
                    "6QLynarNT9JtRElsgD9FR+oOzek9zi4daz25ruCuIZyQYWs+2Kd3CHbRDm2IQHb6+f82Ompm6oNWzUNl9qPc5XfyKnmeRQbG"
                    "vFPas+SqyfHG68NnnMnvT3mUDxH//Cgpp5jH1+2FDrOThm3PP3kvDHdVGHocAQ0f0CiMv0VUYSe24LO7QX/Te9F4i6LRb5xD"
                    "wXtB4XAzM8p05hs5IvTPfHDvxeKvhsX4viaGwCqJWotfNiQXpQ5Wl7o4qkC8PHnl4JsduXIfsOR3H4pwdRBKB52dgkIcxL2i"
                    "j/APnaoipvjlT/znv+KPnyWf+JwSHHQjl3VBMVPqtdwnCPDOK4C40Et58L1nAl8Y7PYwgfUH2qbREledHaejap6+Udt1Pbq6"
                    "2vJgyTeswAH6qrsTiLsgEGYKHXf1pz8dqh1+KnZ7G8nl6W9UzSQgeXGn5N6Y5J8GNEMy/tl/9j8aQ3Lz79lpo0Wl53FDn5KL"
                    "YGdq9cM/VW2PXTquj+iobGvY7nsE9SdhztBfmFC1GvoFRs3dBtp9Iuga/yNN4NI6Nqc5uGOtx0SpEfLdTJjP3Nk/NInvNnj+"
                    "E+Gd6NHDhvjagZJund7OfUNiokZcdCg+to0oZmINnoh1gWmFt4EOnwj6sVMOb8Mb3YR38+59+cuhPeJtt/ejnv7ts2Jp+Ftq"
                    "O6sG8GrC+edRvNDY22PB8a5Lw23HJTl7B9HN9mj5d2AGtRYDOzyYZ00/bQUfBGuzqD6Th0leGJZ78ztwfyPv4I3yklszIY/B"
                    "HmvxLqotK4Nex9HEXZ02xFuhFuy38ZFx+N5vBL6eCKNMs5l8aPYBJVL0WDjj/u6kqMdm994M3P+NAFdzEkes9nDKEVaZ4kfz"
                    "0Y6ZtGovKVL55U/6DyOQRj9yVjuxbyHGQdUlENGqc2xjE0ccRMXI7fwMQH/xmcjxizeqnH5Q5DarIBBocJ92VVPqbL6DfJ1s"
                    "S16v+aKJwW0TwruPF/5woAFzms0ymR9kaTgSRNug4Q2jovSd3Mp8NFQ6Ibm3huRMw3nPnxRmtw0F41ozMKqWsug4NV7VR3Ev"
                    "EP/hQGyGaU+hiFxyIm9/1NnH475QJuxSQHFC1scfT0qIUO5Rz5it7ZxJ/LimvHMrdQLqHjnDx8tpyg54Z3R6fildDNuaAm5s"
                    "ALZy926A6OHCDuqPVVmYGpliBj6qLDW2A5ETPXspk/0MyLtedfxw6fvxybYKypbl8VX4zU7gnG2xOUx41oh+55tPHi772QUs"
                    "KoFT71oO3T+tqOeKnT5ebGNE7B7oNk2NF0wxzB5BX7EwOqhwys483kqqFNvpyB+tkG1pkLr6+PmuH8f+7pLfXU5+pfuIREG6"
                    "rNrSHa9xIDPk9paT25ptviC7o7dLHgDXi+srp9pO6lhr1dmCorswPN5CGl2oQ8W6rFEchkpQq/i7JA0WkBS0tKbxtoPKInrf"
                    "BbhKcxgrGZoS8PGGUVE4iHncHgxx0hc0td7oL9uKWo1i10zrLskfbyU7cgIs9WCSN6gn1YNqqeLAjC2cSfDGyklwNGiS7eYV"
                    "z913X/6k/vpv9LcREFc+/CkVVXX28opyb7YwOfjPN4qnb8n+EK8S+zFLf/KsMzQXhoItAAZYhL4G+rWiW61c3sesoId5go1q"
                    "QMhobfCz3f2w3FVgDWQF4zp8k3y1EZ9R8W81VHuzbZqv+BHFWbbmTYzl3q5h8lbBZD6FA95BK1HCRhacfe4RTVyduYpiRPtb"
                    "/yzfVnZM4L/TTbRS5CNa4IbPfzn94Jvc7jdNWbIu4/nUtEBF9cpXSvr5zSQr3VZt2msXoYYynKG8MH1CGSlO/9iroZBe//D3"
                    "9mdjeO/4Tdv9w5oX/PpWfOjIZi5bimSdfED7DIC0FdXuwuXcMSFOgfvOeLJ7VeDGhRhWuFOA3yThzDW3ml3RQaB+Flr32Wib"
                    "2gLltaMUK19HwBZvaorf1mDTc+F5z4Z3fq1LX6ymFNSbaNnponNqb5lzoTsXNww3+Fz4/pPhD+6mqUIUVdfXqUAQWX0u7hCF"
                    "2JPlhFYWxa46jHn8NyIOnowYPK2iZgHNdsvRGTVluaaKoANv5qL5dtH03IgxfDJG0wcDNoaIwHAa/WkvG8JZz4YZPRkm3SR/"
                    "taXdujpSXb+1F49V/IdpH5zPd3U3V2cfurnA4ycDt/WexXvVkVLq61kGuZW+L4rOts6FmzwbLkVhpGkCYQJl1gPvdCem7pgz"
                    "L+QTrQzyD1vf9MnQbScBXSFbi53pnmY4VVF1KkvFF286OZtXuc8mVtBIXFSuom50cctWpQ40GL0IlWBRCcYLafJbQT+bX9m2"
                    "GKo/kwqiMsaK8qZqz2vrpDJKs6E+m2v1v2LsT9cc25xagzR0f1Tt52PN9yGUtp6N9dnEinpEqLXTyrcz7aUZl15mlUnWzXoV"
                    "AYOLv32fv52fTa9692+gfVVJtjZaykAbZc2NQTq68zqWWLwV9bMJlyrz0pZpLzi2JgrOj6iYzHgfvVvhTROtgt/siYzmR5Pg"
                    "rv2egsalSLqouG94Qk6A8oI4O3JVL5kvPkPnXo1jLAvOeH99q8vzDgv9whi/HnpZ9PGbmaDdJ4LmXkSq2mhw+7m3uroacSj+"
                    "HIjeMyGSk0dZXsONtF83qCvhRlNYRNmKbti7AOpaqJTaTNz+E3Fzc4Zdw5U/JnpRtFV50H6f3sJNW6hNvBXQ13P3cPBEoLWs"
                    "OC9EAaiauopYs6NzNLZ1D2mqbibC8IkI7Rpp3WsqZvtagGF+j2PNtNAzkUa/BaQ66auKowbpYfM7qh+K6bPEBHmsUPI2xPET"
                    "EZsmWjZ52Km7v7JvumVaLg8yB1vxwT1/Rnss3wY6eSpo8wvYx9yOgXIfWEeVTqW1PVVZ1x27CaTpE5HqGkXNcM0KmnPce7La"
                    "4WEVNZdHPJM9ae9NZ7mpCGWQDWdKQQ0W9FVYFb+g3pcVBTPwag7tXBPrPpM+URpAcKWNTs+paxsUeivAKmq6w0Mt136UP027"
                    "67rjetv1wv6ifzSF8dqvaVq8rXQOU+litZomUqFzmmxyLJ861pM9yEYkOIPsXcvsLYp4kAch8HYfE7+wlR31cZfJduCr52+0"
                    "8DPBuk8DuxO1eFVjG2xrFBNt0lD3bcNXecWwxEXVaFFc7grTmILtPR22dtPt1Ul1efw9NgXbP4LOfxo6a17qsnq162cPqC4P"
                    "tAucN/tq28yFGTwNpgHG/1PpUnt9l07dtJkJaZLr9+qhF+5f7M+mUN3wm4M4iw2w9EF9SmKZFoXvXAlEujj7mE5f9Y89g+qP"
                    "3z9ZFSp4D9N91fYbON/O+tR31rjOheg+HaLy1gz5G7A+vl+xP/QlMRSJmIvTezbOHpeNHw0zczqlMxee/3R4qrRHOSQqXN/H"
                    "Ck9iKroqbS7S4MlIbbxaJ9JtISgXv8AZl/iqgwQl1Hivho4m4T5b2w74Xr+e/dgQDm/buARP3ZoFM3o6zJZaRQ3Q9uGT3lfb"
                    "v0kYRbhmxurMhRs/G+6x5YhRHyszxeVqK/dNX/NWdLNhJk+GqW/AmTpYc3FYe2mn94kuD3G6EWz6ZLBbPb7nDLTSvTrGgn++"
                    "uaJpmjY8mxpt5SvdkjYxBhNDOi/12BLVt+Bno302SzJJtmNHYKvOtkXop679OMhnU6Rdg6Vrauhe1kVvTWO53xtVv2fyjSbc"
                    "fKM2GTml3mYjfTZbMq2nqp2ko8gVLcWR2vxnHyfBBrkVe+rISZ+cjfbZjMneauvvqoE/aZXLLIKKaE2Hotkwn82UWklte3qY"
                    "1m2hleWGPWX1nVOtlmPwsInZgJ/NmYzu5QodwyR6Z20rAFvv9CGZomDSbMzPJk59+tTY12HFjnkJplXhjzis7iR5MkX27/6X"
                    "/9Szsn7ZNh0ePgXzym+dzd7K1T/R1jX9d4b9U83tjj23jtb9Ja8konoBzhAH1+pb7G+GX/6X+uNNQE8/bWoeLg0S68soTwbZ"
                    "2PGQt+MLz/CFK+I7WUAyHtS03fYfLwo5zA33xJCpxMeOOlfOReiuhHBYMadBqsZ93DPJBFVgTeUHlTjwC1At4yaLVSaxeSth"
                    "M8zuvCqQWE/fC0rTXx01UribstTXZGcBnCRFkIgCHTgJX/6NOpTJ4jbFevX3zJGE21mzM/YKGTX74fLWkcti5vL7BNL+uWdI"
                    "o2sHcWGkr+rfhizAOCucRctVKHTkRbxhg89F7P4GEPeaxl5BqXaqf9+nCp3yuN3qUrS5kL0nQrYBMe6fO2iioe8m6JoWfgsq"
                    "16+/+IZM+CToZ55h9TFdP2iqcrZVKcl5OW1CaiLgc2EGT4QJrqMiuqaqw6QpBsusr87puURYYwNYhWPmwp70cGx09suvbNzl"
                    "n8wPpkBf+S3T5001GlWfsF1GTe9BvldkQsM00eokQzcB1v7WGdb4mo5eEGsmaFrPQVA39EFtpLrVqTuUcUr1dCA6dPjVPNwU"
                    "XPc2uHfiHAN4Mm6s2SoHZjgG/li8yoOuxp+aqXILMPfWdXwgMF2cwJNkVYJYlx/ZyOex5g7B4oZU1BS09dZsOK4+tjvwq5T7"
                    "rmf7/Rm81BjzNljearDU/FBSIawaNa7OdoTh7TkciPtjp+xK0Un//sDL70P3+TfWUyafyXxys/p8HNKCb+Sx8XoDsG3vcFov"
                    "+gZucwWUuzKo4SWBPghgnKj+tumnLmk33LW8AtV7ItSh52lrFc7csOsXpa8AnDyK/fRf78vv+94RUwinfsVODhVbW6jApJQn"
                    "Nfeu5aANi74ifP1S9OC5Zyivtp5ZBiWXHtqIljaGmqpyiUJv7z/6iUnicGOTjmm87up4AeO0gU7Vmeo1PRjUBKTpRrha1+l6"
                    "6mmE3q0If1F675/Js78V4qffOWU27x4sCrdZPunnYKoTBvNvqdqoqSf6Sd4G1719A/+r6qTxz9o3uBXx2K8N0tljzhY7zbqs"
                    "mG543BIOmgLpPRFkP32prNq+eeJwoqUtYlR33dWVQ3V3azZg92mA9aUz27VQeZZsXGgkjxqQyjfR1FHdSpqIuemmK3Cn0d58"
                    "ZLXbb7qY3Yp27NfOwpocBKD1O7Wzur7RNCRlDvgP2TZXry5NAQ5u388PB2xifXtRtX0UbBj1Iqelfx23xfmm0bq/AbSqksh0"
                    "EzqpYVUlR8Pu/LdUGk1DvnlH34t1BOQn/m4vAlDLFV7ofraZTkvokTRNa3tkz8Ua3b6ZH4CVL3zbAImZvGJzZWe9VU7axtZ7"
                    "U8ihGNVsvO6aeM9afJ1V8fKx5X6Fp/7p9bDJNMQ1ty+0jm2HRJUHtmLepMuMseXyKp17w48KebWaahqkvyJIOoknfflMi1rD"
                    "8yk9OLh1t/348X0arIjPltZ0IExbrB0XzjDSkxiSIftXS66noYUrQjsvYLSlJKe6pq87UURRA+X5DT/GBqPp2usHwzUnTBUy"
                    "3tljbxpGfBsM/04Y/hgMzVvZ+hlmc8ZnFNNRIbErxYqDh53iCpzbvZOH4OobmFafu6bRBCbwcGbmfKFzNiR3RUh9cz91cnjN"
                    "eKKU1v/2943XdQc5GwPnrQhu6E6YGI+6AseexVn0TlUW2OgId6mdDdNfESZtPWvK6oGt6/Oqpwzl3tjdGMBgRYADwqF7d5w4"
                    "jq3sI3U9B7s96DEGL1wRXmcHoFkfyThG5/0K745vjGG70aL5JpUN/+TLfTGOy7/6uec3hyhVS+hh3aTuynJDrmD8sWe43dvD"
                    "zQvi1tV45rBy5d3JiTXThricVJcbWtLDIR9+wCPehvv8t2GbZSk/mdxL20ZMzzk0iRVjjk4qT2/Imd38Orynvw5970qlJAxl"
                    "P4+M5I26FWzv8qjz8og34D/9DTA/HOoGZa37N6C5o57wanJWHPK+cgfk5rcQPP0tDFmLqo+yEYg+8t17PHfwlGnk4W8KuSFp"
                    "OgijYt83XCy9Ge7zreCgN+QwzC+21C1GH28VIfw45+mPeAHx01+AmljrnSi5QZBQDbZWdkB3am4GjOgR7yB5+jswLv+nyzFb"
                    "8dGpC6tZy7cuugdjT38z2FW4Y3hVyJwFrrDrIyCU76J/Zkf7ISToRk4Y3MnzgzXDIsEZMO/2sMhDcC0QFhmD5D4Jks7aYCvZ"
                    "X6pOCakqNdQ3y6+3KpmG6a0Ic9Hozxg4f0VwK0V/xmAGK8JcPPozBjBcEeDC0Z8xeNGK8JaM/oxhu5H2BfPNfvCc6E9w2dT7"
                    "t0d/FsS9avTnyttwn/82Vo3+XHkd3tNfx+LRnytvwH/6G1gl+nPlLQRPfwuLRX+uIA9/U8h/PPpzBW70dLhLR3+uvIDn0wB7"
                    "NQuelWmmwTUrb83WTB3HVxRwPqq6p3bjs9FnvIHk6W9ghfjXlXeQPv0dLBj/ukaBnN8M+AUDYNdewo1EMDS3L37ZVrdh//Qb"
                    "nyMrIs/lnmenc4v4b8wEb0J1dmcqCG4n+OGdTlu4ZijvMy5/VVx26KdGdnJxdiwwNBuVuyKqRcNcY+C8FcGtFOYag+mvCHPx"
                    "MNcYwGBFgAuHucbghSvCWzLMNYbtRoIfzjfu4XPCXOFlgx7eYQWXw71qmOvK23Cf/zZWDXNdeR3e01/H4mGuK2/Af/obWCXM"
                    "deUtBE9/C4uFua4gD39TyH88zHUF7vOt4NJhrisvIH76C+jDXMEyYa4rbyB5+htYIcx15R2kT38HC4a5rlEg5zcDfsEw17WX"
                    "MEoEuSsz9wz5wk1F/lTtJRGwMdSTn9V9yEBbsJ2NNuOGBtTsEUs9DKOwj8rFUa9Y7hF0/aPOkFzszvpgIGbCnZLStK8eXugu"
                    "ZFe91j1/UbTuWFeHWaDcFUGZrac7+fXDeg4HSXPEcBYZ6ehcl0kU3gooTDMFvTSDJdHkQS8MN9nSE57vxeHfguMW+T8HTKv6"
                    "vcl1uIbjo73JP6fB2+rCxO0J2d3bjse9sr++tpK2teErQDGItlV1t5f5BXU9Kay7gLDmzFZN+6lRDp9PpT37e8VV/SbbSqvT"
                    "WTvGvW3n3wmEWxxytybVAIbjtdgSpGIVscLef9V7icJjtDLfRPU+emt/UvwlNnwOZkPDv+ysFnWbu4SX03ybmr89KWmwgKR2"
                    "ZNue3HH8WGv0PZMzbKHjzu58EvpemcMFZJZNuemOrzRAmNdfv+26qbOtqNmHLKvvmmdX+JU9jduAK3m39NES0p+1raL5L9An"
                    "OhqoY722T/+9EseLSLy3VL07CDguPIxbR/IodTdrOycLiKrLtbUexG4gX6MfemDHqOMLavGuNcimg4dSXxg5PQkhXQ6CbndW"
                    "FWqXVLXNzrEi34pM3v/K3SUsZN+ozbCrfdtkmuqL7ivrEcHdZWjG+QDK3eIvYTON0KeNA/V0ZxKbUxLTftm01N6CUsPQ61Gp"
                    "mtOqqwgmlFDVpYR6ye9/1UuYReavdlK4HqbMXZXUFu8XwKZitXZUpKsZTTlP41jCaFLwQo1bpoCFGtVFAxvUVGxlNO8WdAlL"
                    "2Yl3yvOeT7LsfW2awsRWR/k/dwu9hIGkfW2Gc+t5YWL7jSIltp9n3jZd1zfbH+ycuxEsYTA1OSHFp1oe2+2sCIoVXFIKd87p"
                    "XMJ2amW+t4fStIlv2kK2wx5ZJpR5t9hL2Ms+TzB0IE2I7dCON9ib9micReSsbG8qMy4Ch7Dj8Y06HHE5+D8tr7uIvMO4nfz+"
                    "Jo6d9dUHdKT3gFU/+tkgljCTyjKWJtOOTftOM/cGQ+lEVm2pdbnRidqvVz7F3RiuWE1mEdig6plffhn+bRrZ1d/U0YrtdjgR"
                    "gRmwHSKg+x9ra3DeplFbh0nIp1KcvYD4eoBycfyFPJDOoiKLalv08b1ObmmGdSneGzXd2bbh/DG87pPxquxKaBeVKwl2sqVY"
                    "Z9k2/6DphLbhKL0I9tZoncWr/DHoV46rLVDqvvxJ/+mGAzz5W7bCZJgw1gMxTMUE/yMFrekvN6mi/pFnIJPrEcXlQGo4b6Kl"
                    "kFzPl5X94H7tos2qQ8ujtI5ZJw/zUbpPQmlG+Grzp4seOl31oAzmbW7XJLxn7VSlbwuZV7t+eJD2FnT9ExGq+cD8pwHjioXi"
                    "2J5OszDtVc02zWBHt5LnG2cfU1XDN+INnoR34Gyb4j098+QfQ7Z5teX+jTDDJ8HUA4m5Rsk63CduucrefufJtuo7dMfrHzqf"
                    "0ZPwsjNc/UOehh5UqN7ETw79gl6Pf0+ijJ+Ekuv49dKa+Uqm+NYMflUROx37L0ta0H4G+3zIV7zUwTWRW6COfXpQVaf6Hpt+"
                    "1irNdWst5clTzoCkN9DaRwHhZtV6jUwbY9h2LBF5izQ/0E5yG78scw2IezOQXxrsjPp4cQzNbb90drHEzjm9W3T3jjV4pOgL"
                    "by33SSuy8Ea7zrne436a/E1nZvTzenvBi89Ji9GavEfMjlXO79PsuBvcgff4FE7o3LL5HgmHbwqc35Pow1xUF2WoFvdH5xt0"
                    "tk5hBjp3RXSf5r9aT244zZBGWWubdd3qjmJacwM+VEmMgfFXBDPkQmp0h42i6Pj7YNa6aPM3WiSKLgzyqWpjzgAarAy0bY97"
                    "itOrgYUqSjQYlkAM8VtbUZRzR1fNsq3UszJmYAtXxNbs6clcc0UrSDjaYz9yR+c37bSdQUbocM1BG8UWrYiNCe0wG6GSRF0/"
                    "zk5p0O6NPU8d0B0UDM3AF6+Kjz5I6fSmkNveup1XTV+6J3UNy1VWntxFZpNVWHlyBsS9hZU/CMhDydIYEPdmIPdwwOTRrPyz"
                    "6O4da/BI0RfeWu6TVmThjTZFilhldlBK9Ldf6C8XwVz46JC8Qm9JMyvW3kE/ve9zSXr17Weye5M77WGy92mE122TmdvX5oIm"
                    "j3IrqpIrag42B3gfCndxFKezrPY4GWwGFcMxlZzKQFIWYQeXoa3E9m4ky+8lU+6upFU8TDcv6E4KgHv2MpEGuQjkMrnOB8f0"
                    "y00Hffo3dI4Wnhwx5EHzgv6CEEWQVUprt5dcmmO188RF8U/PPoPpT1rLZWAOJn+bJim2fgd0lCYfFhsqPTbXguw8SHNnjmLs"
                    "M8G6K4M190vsYkGPN7a7jb2XUagzR188ZZiugPNWBqcH07J/DhJaHFUKfdjLwAaSgdzkLLXenAly7VPZr1y1U1xC31ZX14H6"
                    "tT2pVbSzpS/Okr4FazCBVaXqIbX6wwTGsU+aayF9be4BfmDOW9EoHT1Dr2lpoLS6YLGVO3IEVSmpaulwGZotJhjCCq5onEfA"
                    "0laMSyBsobSuuLOFVnvuPqEuIF0rt5vA4i6NxZLxj/rwBs2fM9/ANjQ3Co2RY9S0klsVHTMVTDNAeQuDKirxWjcdgVFXfLnU"
                    "dHADlGo8TYE1lS7pkbj5ZCnnBCB/NUCkHlSLhr4MwyYKYdq2RTdD/qUVAdX4fqjKWig1FQDqFQIOy7ZpLVmn9L26saTy93aL"
                    "cln8DHThwujgIGVVgRO+OTTNtuubKO3F4a1jp+S1z2PrSb020F5U3TXX5DK0aGFo5I+QhPrGs4107beU5rj5LuIEgngNBHUz"
                    "AEFUolcQfVkst4BQizIDR7IwDnPbQtNbqrOrvnPt3dn0ixmypwvL/rcOdJTqqvKmkMy9we3gZVF1oLpGSS7Xpik199EBSCoH"
                    "rw9zjObSDMDYkVecBH3rSK8PU7MGlrT9VnXyx3WX664ERV3eNfV+GZ0aM517UHQNGN2sY+56K+HQOone96AcTGsstifC7C/q"
                    "brQT7dc5cJa2+DaRkjXNV7q0Ttiwu4TOYFZgy+2O3VW697OddUyWNvsmkGKipNqnUe0gB7uKLxUOWjrwLexmO+u8LG3s1UXU"
                    "THRW8VKmX6iGFc2xg9ut2STfP5zK2U3BWNqw8wfMfbIhvSRtzBkene0pGjaROow3eTthCs8NZv5PnE67jmbwOeNmYgW6Wuy7"
                    "NyYnKpyjFBrXpKtPfHuTteagUlB/Aoqp3o3Fu8W9nA8FJ5p4lg5vmCCpysERLloVitt1xz2dqRn+sXeLTzkbQC7alna+aZqg"
                    "1fGboEz+II3A1+v2DefCZ0DwloRQ9cbka7XfS3P7XAUsdMSUjIeK/fLlYgqsZVs5A4q/JBSKXWMDqcQIKyQqUlInu3fK1KkX"
                    "eY5zP29LBQuCKJpchTN1T47PZSFb6nQEiXN1mbCbASBcEICh7kojkTHEs1VLYYrI66okezdMf4/uyECxzDkrEi0IaCtIqXIe"
                    "VG2dQio2UrFLZe26unNw0JcOtLWZszpLWo8dyFNFq0KdUge9rJW111bFNA6y1TgUc8FZmhPU827xFWfD0U14VIc/u5cGZwY6"
                    "uKveJc6TDlLa4g5zXXEGonRBRNSZ7Z3Yx7E2dlHns6j1fa0Lp8yF/kHL6jmm0VkUCO+xYatRdlOsle/ziXqRTM/a17b5dnib"
                    "g+eyrQdFlVRpr+6u/kH/baoH362/ZrRedyzLKq/I4A9HFJi89jB7aq5Pc/8ojo9dAmuefwY2nIz7L41VKUSLT11ft+NIbUGI"
                    "cnyGFyNo/S8ToAmsN6zrH/kR/81CXYf46dM6tonVeRuqxazabnWvFa0AB2529rGHQJdP3kVA7i2L9xhA3AmZVAf1+VI+tlq4"
                    "QbsEvTqULZczsKy1OCdHzO6+k+6+rOBNpfN0Hn8CkLcSIE7Ym2y15qPqbrGJmuu7QaqEVMUQzcGbgesy9+6riiGr+eNFXJc/"
                    "3beVI+tkNhpZqW3TdVtp2oloTm5iiD0bNE37qW/XJYT9488wTjcnfCxGfWawUNTQkhfNIBuUaKuTxxfEdTEwtisplFnY3NWw"
                    "WQC2bb69xEfx7M5eihhWJ1hmRcMG5q2etxJC1bGH+jaENoKignF4cvVac+fwsw4cpOq7bjcRo5+Ettbh08ZLaX+xw26Tfe/G"
                    "N9G9qYp8DgoVs5Bc9n7Bxt9F/qELzf6k/nYRyeVP6wquQ7MDqzdx+RzsgaYm0ecOpmWVrkbg27HT+04/7QxPPKkyHosHuoLF"
                    "tLE7ndvuVKQ7O/bRPAqy6PiXWs/7UbkroWrJp69yCreIoqhUpd2wORqvG3Mo9hDuR+Ktj2RYWauCSNQwXbuZlOdWIcrJaMVl"
                    "QP5qgPhj3YHOS9XZo8MltkzO9xTH6Nj3OlITwfuxrKUM9AwbirUeD3TZhheAztOg4eQ33WT38NY2x1cs0ozdFk4AavQlfLoF"
                    "rv84AejSp0+6Neu0dnc6fUr1NVeMafthSg7ERMi1f9wZpuSKhnskprKqK1bR5t8UJxq4GiYrbnJegzQYN6OeBc9dCd45S4Vy"
                    "6Mt6BqCZrSszfENp3yQ0by1of6O2XZxkgb6A09sbJ33yKKBWb46HMlGBzuN2FiB/JUA24oDTVZ8uj65dNPH0Pgk7Qe0mMQUr"
                    "YTJ5pMHxUt1uzmNMelf2BQKzYK2lCQd4dJVpU5ad1NzOVl0V8nufrp0FKFoJkI58quSgjXueKQhbf9+Kb9P+7iSmeArTWc+S"
                    "P3F4dQrW5V+wnn1ZvR7JROXNvqIr8qq6xEQBzW3yZrfjxuCm3Z+5XzVRNXehxUmYXjNgS6IcVGip9mCnDdvUDtXtDtWvUYqU"
                    "RvDNgek+C6ZeNrOW+v6IaryLk3jgto69qVZThgYXEuaA9VYFC2BVxqWEG1tMYNZV9+Phoh1OfOk7CabQGORrOxW0mULpr4pS"
                    "x5qs8VMXYDaHb03flpjBbQeV7uYi5/HwRqs5XRY+hTV4BlZtMkpupdR8lbUOGHzuw3wKULl2XIs1d23DVfGavDoUENXNKEdb"
                    "tUTT1w+vetiXoUSrQtHN980lXKV2Cmoo0NrRZWYrUxcW7qdp20bMwbeumVSAqDC+7lQUlQw4FM22Eh1PH6tL/IpmOMfaXtQz"
                    "A1m6ORiTVTFyswvqqrynHAxf+DEF/0prbhvqjVvIUhy38wClqwLS7JqMfGcy0X1wsdOJW4rIscaZZd/X5TEm3tM3XDFYTu8J"
                    "GQ9vFqR1OYtxEOq+JYG+53DeaFlpxleebHqlE+8kPu8Z+A5vNNH54kQBa7I5JaGHmSvAphp33mL6TwELdmLUBM+l3Iojd2s/"
                    "WW4iZp3xOmaBW5eeHD72sp8WojMUx3qY/oSXWBi0SuHMwnWZhpjDoaOqUyPmr35eRy2zrtkeVYxPqXl1m4irxE6yoGbzFnAl"
                    "iG9egjboVztAFk23g3s8MBPT+1tz5MtsVJrTG4RBU+jMnsfLIb0JTO6amOxcdHVpQsXMj6oX9GGQt1UzkPqA+gxc3oq4BvfZ"
                    "aK5DLlp7vVBpic8t+E8GVVxtHjyB018RJ90U5Vk3p7GW3l1VYRd75PrJj33x4AyIwYoQd/IgOPvZN2LUts7a8+7NDlfUqR9j"
                    "Bvl1zAC4psIcDOv6pC9PeiBfmQ00ASdaEU5fB9Jrjb1lxmZhhjysaafb3U0Ai1cERoMZuTrurdmdOTLG0+nLKPQ8z1mnK1kR"
                    "lCXHhvf3cwVnSJ6uKjnvLtWN6JNuKNtmZxukaIs9xxY7qyKy5Xn5R05sqVbqQOt40eVVZQv/2HmWczCtSTBMeaIpW+lEDWFI"
                    "O5zcsTu3v3Ngrckv9Cgfu+/exH4v627YNZepu77iMQfOFI2g5BE166L/TqD49DHN0G2ZmroVfN7h+DM3+uReXqdH/OwzXO4V"
                    "xv5jsDjtkuu+LJ29RIDNplsD6Swb9MVuM7jncbUbzWUw7mJgTPxzuFQXwhhkcnQolDg7t5XT3v/VUdKXoXnLQaM4fN+14VjX"
                    "x11GvpK6g7ujKwKWz+nil/vlX+74aKdv9Oh0B/x490m3TTelugwiWA5E23RSD2juqZq6u2kLXc+qW0s1sWJ6eMxlMOFiYLT0"
                    "0twqITec2rTzudEqOaNjQn03iOF8dP2w3PuBRIsB+Vz3aStqXitbuDuYGko9HKbqjS+DiJcDwSWS5IlRByaO+vc3vIZdYqdr"
                    "Gi6LniwmOhGuvCnUhUZ1zKvOMhP7tjf8KXXzfL7xSBeEoe5k24GDOjD1qoZzapugOLLKGP6ACXQWhGEtg4pO9T3Y1PhWddHA"
                    "hry5F8Npaz24lsfqWuXWBDh3DXDqg3yV2xZWqHNyrMUuq16PzbHr69To4swMKN6CUMgovNZM6/s73e1B918zLQFs7epMBBMm"
                    "/VjzLao/q/9ehjDyuROjrq8bkRGhwgjuG5pbDkwtDjq+Xa+aBKiiR76rbjrIXoSlnngGy5u8pPnjqFrJHqShVNz1g5UuC/3W"
                    "VPjk5ttbwwMkDlVJe44ugNyPwl0exR6fgOQ9czxQBoyvYB6rrSohLmVLG2wn6Wh1b9We1+9+OJcPi+3J+eUX5SBdBDT+SR0k"
                    "+6YrTtmVN65WdXr3rbeak4bSPucMx3Sv4V66v5o/3YDk9LM6Bv1urkINou6A1N3YCuSy/N4q8vMF2EHXVfK4SAHYPOmmKvhe"
                    "YncomkFsuRRZW+ka2qro5sBzV4Cne8uquwRqhvSgzlRlQ5QpGrr3qizAxN7nYPNWw6bXpRTbjsIuH9tGFCoSe5XkTwHwVwBg"
                    "21WbCsqh56jrE7jRlK42me43NYUmWAFNTSdp2J9Nr4yxP7b7xvlI47vRhGuhoQamdF5eW9VJe2fcfLoF1m4au/sOb6K2qkNN"
                    "/p6FLFoBWdtQydyrPJm0pMp8OgqO7QDDNNypRds237Q9Yq0wB1W8Airz8m0LdMWd6U+6fOmkrZDu6nzD9ZspYMkKwGxjQ/md"
                    "M73FQLFt8rejuvPWWrdPB6HMZu1DOHMApisANIIrJsFTA0/a7hXVK334Uw3TrAPmOqsgMppd7t/wry1WTyuKMxevboxu797E"
                    "ftYSuWvQiG+irbnl5sBAFdTeqsYR69425G+/Nlw7MZhhhg2o7vUqozwLnbciugEzogJjjo1SMnuw9b5J8ZVSPVXXbCfr4CdB"
                    "+SuC4o2lr0oddzvqp9hXd2qO/oNqcKJy7gD+0lE04MtfzZ8u47nwWZ23Mi4qkSAqGeu7eEEZAm0xUsDPrdY4kbCdcKHM086g"
                    "TQ9PeByy0y6vYHnb0nhNG9nldvnM3diWFCM4yH4OIHcFQG/QEKrdK/feUV4G/Qs759ws701o/ZEfVCRy306WHUxB8taHZC5U"
                    "qhr2vmE6Zx/nYPBXwAA/D6ee+hO86vshvTlSfTAHzRiv3dyaArOGOtAgmANJauZ0rAfXyJuMy2n3xGO/ylkgwvVAnDTSoWbp"
                    "+vJnN1wQpQlUQx2qD5mlzqIVQJHMG7GvVLmvUm72N6hVwYFyp03ZM1tVtjcHz2XPQs3gU73pQEu+/Bf+blrV/UpM8yK6235T"
                    "qwV29SpSApzrMrl52zxS3Re0d3l12yvFkHRFAjlY3HtyahbVmVBnr2K6Y9w6r8KOMNjIrbpVaO2z6oNnyo1NY0PqkqKC6vNB"
                    "u7NAz8J7HjPUxT2245WgES2qMy9dRzAdn0ted/ty5kL1Zq7vj0IlB0W3rVflAvhJu6O6Gsux+mwcseaqVlH4+UCfs6bGONJt"
                    "ez6lZkYjx9w2YjBy0p5mwjwfp/cUnIQPGlqqYDdUFTdrEyZeQrlG7vzz6TjPB+o/BahKbHGDe0Wee4CfEsuqo8lual7MdZgX"
                    "WQ/sOVk9KAjVlu3P+qf/PvjhJbQ3/66pglMXAXTjfJxH1rCUUqL0P1GOquau+XpivMqVXQB99vAzzNGUe7QiZthObpzY1KoK"
                    "K2uocZW5EDVssagDs7pQrr/+eLgc17z+CtzfwCsw3glV3QzKuM5uh6gkXUf1LTz5Rd+pm43c+w0g15/tpwN/V1EpGvZ5MNza"
                    "EK5aHr417Vd15eBa9dR1/P5vAD81oyTWqU61DX6rcpGuv46nFMJsqL8F3aaiVrR1lXLbiboqTedzG+PncszJW2rX0Ya/AbQm"
                    "Ko5teqBg//WGgtdxRb8BXN/ahoKSPIhXXcjjabW9cbb/1D1mMUdcRJ0r6r78Rf3hM7TRT+iZNDQmk+pPcMBuybKYrzoT7UJj"
                    "0R8STYqW54LA4vMsGS5fpOtIJBqpOj1U0vph9wjrPlhYkyLRzbCIivFAcUOuVVANW4PpKJddSVlw4+7u2JZQ3t094nuPFt8M"
                    "8n1T1wrseLUDALxiW4qTmlFs0ZES8Al5/QfLu9c81jrlLJEyha+wmSyx/v175AweLKe96DCY0aHGa4q6t+4Xo5MTkoZLSWqD"
                    "qOxAcM6F20cfj5Uq5SLSQeVq90gbPVxaOl8qJmfPGEvYd3vlPjq6yn9kTtuEtPHDpdXlrsMzxNJOmMEJ+ZKHy6dGjYh9v0+5"
                    "P2mvtlTn93uETB8spCmJ+NTfV90svNrbd8oYPNp0dQeI1F6ugTy8ycGpukvURxuuXuPT6Fx9yVTfM6kpcsSFDHeJ+Gjj1JfA"
                    "q/Aj33wxfdLNraXTF6z1wxzF6j7aVh1rPktc+1GW/QnTzuv5GdNNVO4S+dFm6x+ybVSFDnfPb497HuX9ru7h/QhJHGvfcsBx"
                    "7b78Ff/7Wcizf9PJ8z0FRGh+KnevwrE/6g2wFfXFGxL8VWciXWgXfb9Eqnn/6QXufjua+aGXbNEl0dwHiGbvvua53B+U7ima"
                    "/EhGCItKHosOBH7cLJf3QLnoldS5GlTIIlJnBX6P6pJen2y7NL/7kpT+A6S0h9NOR+9HrFEDkq9UAIolpeDMSD7kkmjBA0Tj"
                    "zW56vHCjG25Eq8YF6R6QfFDG+j1eEuwRx9P0Z+ybm/BCUldD+vygUQ/35YEzxH6vVTQ3CxstISztMu4uU6k80bF7oz2Yf1X+"
                    "kZo3e7OI8UNEHER2bWxoOBFA22s9EOBm4ZJHCKcyxJxksqaNSpdAy/8h9esdry27JFb6MLGEvhfQtwSxI5ea4nZ15z7CRFgH"
                    "qzcLRdUePtifMvRF1fGavpiTcd2Lwj7CaHRH6hAjuROYtbZ9TwWtE/ni1YXy8IviPcJ2sBqhyFV/uZiMiO4UwNaj7W4X6RGG"
                    "4ljbNtHKeg1qY3XmghsD0X3b8QzkRfGCh4hnrgQpjmS7u6nVvUPpjjE42sLltvn2RYdJ5cjNywufOU18sFKjdC4zY24cv6Ob"
                    "i/0lnt3YZjPffSbuhV7qj5JWno/7tBdHVUjCxvTvkdddVF6jBukqvqo77UcVU/FDb0dG4pETUnsLSK1GXjFbaGr+k6FgumLW"
                    "bpqa2mg2++ry/KgJ0f0lRNcRFfUiySS+me6zg2bIGfcN0GOjmv1d2zqYkFrlLCZEHnzgtB1k3yfgYqLiokze5FGbIZPO88L5"
                    "5wnzelgpcyB1h5nq7qhAie81ccHStrrnsHmTh+1+iZs97Unl5VMsjeuHYJ44KKXW/kjmnXtgjF+DnZDVe6istvGLiqKoukaW"
                    "nO3DYBTJDhZijFROiOo/VlQ7Au/wpkQcob4aj5q+NmJcJ6R97FHSTRy5WEV7Y2qwBHQU1YlzI5pCQqeNBikm5AwfKqdtUXC1"
                    "Vd6ESNFDRbKjl/iuHl9QKSTfg+di1HsEix8q2CCCp4pMbDCFtyMUORVpb7kL8D1SJktJSVKpKjCzvtRs80Pr+XtETCdE/G+d"
                    "CJoQ8uQjWq9vpaiP+1E3aDStdFE6f9LiPEq66rVuqE5j9O74hGzuCrLhmQd9B0Xd3SCVUx26e+T0FpPT7L3SOLiqWmI0fjIh"
                    "oL+4gLuG3bYZGzBYUDaThqXlvUemcAGZ6C7dMM7e6qpTXdQynsGckDF6uIxKifD9v2GT3nuEih8v1HHPUShpN9dUxndCtOTh"
                    "otniA6U4shZO4FvfMv0e4dLlhKPpYlTv9wPSuY83ErzLmr1sTy87qeA21Ss1x8NdIj7eViijQJccttXrG/cTpDCKKqw08+rI"
                    "e7pLTm9xOfsAhb5NQzU93ddqvzd34+4ybq6/msQ9g+47tuOjd7lO/miw7+ECa8uinD8uSyEqeZeYjzcytbp+ZBUlN/aYpy3d"
                    "aAHpTPKK57KbK/N8Ra57Iz9UqSi+/UvRNdYRl2rSp2R/vBVix/nQCkpvCTVQASLTFRF5EHAJKNXAabp5ttx9vHFq8eQO7jyr"
                    "1aLq9AAS85J1HA1LAdswQyk83mB1fz8KahxhyRtRyk6Fp+y4s4H0J3Huu9j6hDUzDYa//Kv67y86jXQZyuRvKGTcoo6aoJmc"
                    "FMXioJ2rOufqGr449t6XFEyHXwY9kAfIYmf88s2iwFhqbldS6Y4E4l1UW048AIw6Dn3zYNsVUR+RORBh5v/n5Xc6RP1PAGu4"
                    "wj/9LgikFHmapmUeZlHpl8KRMotcz8mDMkyLLHFF7GZ0MFX3B/z6F257VOU/q9GmX6guCl+VBV7piEAkrifcNMuLIM6zxA+9"
                    "LJaxTN2gyMvQK8MgyfLIzeNC+gm+O3VTx/fyPMEj7DerkqTu56IV5eHLjt5zEUC6whNx7AR5FrpumImgcL0oC/zSSwoh3SJJ"
                    "EhmFvhfj/wVh6ssiTUL8JU+cbOzrbUm3eoTr4XU4hePHskxFHLplkPkyE2mSlonvSz9wHF+mpZukSZkGjh/lnvB8Ly1LJysz"
                    "OfYICmuqb3eKzBMy94uwzL0oFmkWRSISZZhEQZGKsixKLxRu7pZlmIciFU7spz6Ww3fzMg3DsW9XZd/q+2UcSBH4UenEcZin"
                    "Xu4UUVDKFEuBd5SkhRuUsQhFErhBEIlcFI4Ig5KWIfPTxB1+v5oI1v1sUq4/qTt+ZqlLgVdchEGE3SNFmWa+X8RxmeROGHhJ"
                    "lrpF4SZenBeFjJw89rC5QgggsRIi8jIx9ihFBn5SXU/Ng5wcu8fN87RwnCD1XRGVsojK0vP8AIuTlNipoZsXqeeHqSfzsnDD"
                    "UAjaiF6YZ+XwQTT4hUznTypLZh4h/DRyIy+IitR1XPxvmrqh78R4KdhqMs29JI5iJ3eL3PdzmeVBEmWZI4oMh0bk/aJ3P4Mv"
                    "brfdz/qBP+lKo5//8v/8+x/+oPdA4EE2L5NRGgNaJN0M+zWQqXT9EuuMvZo6sRfQqYv8JInTDP8/zhxZ+I5XRtcfpv8VXkIt"
                    "qi8fYrfFY/0cxxlnAUtelNJ1C2yt0IlSKb0iD7HtXRcy4XwGscy8LPVLFweoCIqo8NMwzS4/tqzqqnsbQvQ9iC3SFC8wC0oR"
                    "eKmPE48vBFaQPscvJPaC73tOBLQyk2lcZEUQSRyuAKCvPmsUYRJGnhNGSZhjfXy8rDyOUj9Poyz2sLsDB7vICyO/8LDaWZx5"
                    "ssBTy9LHHo78YOKpVkcMQSYS5yZzQuF7QRqUnghxhrH9vCSABKnIXSEBPvSwI73CK+PYLV2HdpLwsNucWx43ihMazSsEtjlO"
                    "Rhg4Ig3LwEmhMLC2+EniBMJ3wkImCZRMmAbYQD60lwsR8BGnvPxgUlQn65jgLZWpjPwsSeMwhd6IXDwlxsPjJM+gemOsbIyl"
                    "E3EuczePRBbFOD6x6/qee+VJo+ji3A08kfhBEYYB9Iob504cxlFRpEFBILMoSWFioHNlIvLIK7B0ZR7hHDm5TIOJZ6pyqxOA"
                    "uXCgkXPXTWFKROG7AQ586MgkxZnAs6BcoihyysR1szSM8XEfphAWyy+wedzrDxvFiFPvJH5EhjIqQ5nEssAGzbwwjvmswVjl"
                    "qR9BtxXC96M0S3M/ddLEwyIIKMHLj1XGYAgx9KIgCXJZlAnsgciwJ2Gn3ASaOgsCkaVOKKNceHhqEGShg72SeDg7CVY8DmV2"
                    "9VmjCAtst7CMpJe52O6eF8A0O54jkkLC9LhhgOUMywR6B4rCjzyvSKIcL93zC9eVmT/1VLIRQ4Q5DG+IY5bm2PhFWcTQnClA"
                    "RVArWZnnRZgF+GecUk9EDl5yUpTYuw5sQ+Dg7Fx91rg+DcooSrzEC8q8yALYwozIQgiNgPcdx4HvRDKHosWpAY2AJYYkATSE"
                    "L8IkkxM2w7C5IUbsvDzIChCDPE6SEMcgxXEAaSsDHyYefMFLaKMKN3QA1Ut8ESVOJBJoIuyf4oanjaIEQ/FciS3vCxioyMlS"
                    "0JQM9pKOJNScl+VpkLllCP3nYinzNEmwmaKkwP85CSu51+rwdsx+/ve//OX//XXz11//409/+P1ff/1Z5YO+fPBjHA+KuQiw"
                    "/kUoIpg6HInQBbvIXbxDKGcnyeIEhgJK1oE+cwQomgu7mICIQQ0PHqNhVHUBJ6cmN+Enc8FRvQHNlHCuQy+F3SszP/dDFweP"
                    "DCFIaBTEIayHDCNXuEVWwlp5aRI5sBpOKXH4QQO8wRN1tcBPVBPSHrnrXaceEpUuEelAgMtlcUzPLGWE14djF7txGiShUwjX"
                    "AQ/AMzKQyKIA35DYQDD0Sn/qh+wHnVI2hsboE17KHPoyc2E8M3AeqMYoBOF2gSCDuI4nAtAHmIBUJmBQQQ5KRuwizlKJpw+e"
                    "YjZDvz24Q+ThQ6+ThyMceokbwHgSAw8FLJ4Pjhc6Dgh9nARZjt1egnI7YRi6MdiEB54DPwCcQBn2T48y708tk35SkpcZFho6"
                    "FhzSxQkHK8uz3MV+i+GgQKkkjhfEAakZ38Xui6ECvByWKU2cIhx/Ev1vkf90YE9MIwokDhQ8HlFgL8OvCKQL8iWi0EniOA8D"
                    "F1wZWquEa4QTHuOF+VDJ8AlAq2UQ6+eoTB0tRgZCkDk4EQFUjVvCikERwWRHAZhNCIfEjcq8zIMw8MHHYqiiKImxYjhngcPf"
                    "9/t/+/WPf/2LWtvSIb4O/R8moQ99g1/AAYOqlFlGzlYYYfd7ReaUME9OCtuF3RbCBQqdGGaaWPZ/iK+S/DbSlEJAKWBDlwlI"
                    "UBzAymSeTzTNyeEH5A40M9bLC8II1t7Be8XjJDw4zyevkPbjn3/9/b/8x69aKWFPOfDJXGw97GVoX+kFMM/YgqKAkgLLyzOo"
                    "QXBneDJZCMIuoCJTbEJ80CMmq7tndD97jhf95KQ/ucHPambTz3/6zz/8+y//3z//8583v/7xf/3+j7/8+h/9a8HmcwuwZOHC"
                    "MQjBKV3HL1MnguKJS+hGWJQ8cCLHwWIGMV5FCl4G/SiTwgfVdqefzOl09mu7n1+zqvwJPzCuQkgKHQ4m3Fg3yYn7JKXn41zF"
                    "oCJ+CD4d0tuAsXZcAb6C/4CR4gMw5DK598nWry79OIsj7L/SFY4D5w17BtYsywq/TEAsixjuGHaag0cJH9wbHkSMPYe9Da1a"
                    "3PdU3Uk1y1r7fGhcsCA/8kv4lSX4XliQavE9aCyYWLKxIk5JD6W07WIHbznxpSxh8cP0DtTVjjSPPGzorP5U5EYCr4jhnZCj"
                    "GcFCAHFe+EXiEgsTxJs8CaUnSDkHokjhRefQsvDJwWmSAgtxuwTcpUlf7x7GNmBKfehM6AU4MVFK2zgi3wg7PQEvlVjvvAxx"
                    "muDcJb7rOVkBS4zFL+Fox0l6uwTdnhqu/L5udr+cSCChBEsAl4EPPRcmktziohRF5OI4gGREJRYmorMIzxXOTg7lIWDQwBqD"
                    "WGS3S0Dh5nfqQ6U0phEADlpZwAFOXPi+IgTJSrDUpMZgs/xUQL0VIOSlG0ZhCmUaB9gAXgo9mQWZHwWzBaA08NGeQUD1Ivjd"
                    "Tgam7MFBEC72BvSeDzLgSi+Gd+TSWoCJiQiaVrowe7BBUqawWZdOQz+0xDwogG8Kou8VbiYSsNnUh+GEkxnDBATkovseqBg+"
                    "k7t+jH+K+UmhA83gJrAawwcNFScoS+CBm4KPk07wSh8vtsycDMc6IFuKtYPngTWEGobagiNbSLziFISl9BMybpms87edaL+e"
                    "fDEMC3ZmmcLX9jPp4HQ6Dg6oL0NoaSh4KN08daDPQQnwzTDnYZEnjp9C4DjywpMv3vBFlM3my/6DNh+sWh7AlYdXie0WpAJ+"
                    "REA8NILtgTqP4e66Er403BVwDIEXkabwlZLQy5Lw9Ku5cT2H/LltlHoCDAy0HBQ3eBI8zQJ8N05d+CZp6sgC3gv0riy9LHDh"
                    "DjH5hOsHdAV8GDf14rMnqI1kf2JOs3GiUzDLzIXu8PHCXLgdSQqugTMr4BjgXyhmATNPugbMnRbehUMKW+a7MuJwyPBh8PQW"
                    "fV34ft3WdmtrvNWDYBN8DpxIN5Qx+ESKtwhSlFJ8ESoSHl+GH4gCli93IIkPZi6zQAhZ0vn5/CB95HDC9weQZn4K/McQfn0O"
                    "9xfuHPFlQdEMrIET+Y4IQESwi+IC/wznIPOheSWcnRy8Cmw9GXnK2+Gw3+Sik3r1sxIsOI3xmqLSicCwwbJAJjKcbbA7CUfH"
                    "zXHA6bTkDo53FPrQ+okIpXRKmMfPT6h2rx5VTlE25wQM+DiWGY4mXhysiAANh+Pi52DDMOwUavMp8iuCHKQNuw//7guQirwQ"
                    "cDw85/Oj7KxZ9QAwX3wabgv0Hr4Sx80FjcW+wbFPY7A3uEulL8hCuAAJX0limwELsc3UST4/gBpGV7l+VfAmBViczKENEj/x"
                    "QN6wecsy8lNi39AVoLDQTdjJWCIQRZciwjm5GQ6cqhH5+R5GTbX46gkFUcAUvmQU5DgLLl4KsUSZQdF6jvRAYzOQehwOeIQ5"
                    "FG4Kek90EMrXgYfz+Qk46bqmt6mpDat6jue7OBOw6qXE2y7BjBPwHFD8AnbFdeAre1gpOOpO6cGPTf04TSS2MWgsBcHckecc"
                    "69qsc5K4ORwVr0hhJ+E6wlBnQZiAysIkyRxULvbzsBQgoxJKAJuNYgShBDcuoyyNPn97dzgWH/oVxVgx+FVQ1RG0h/AhTu7h"
                    "bAXwQuAo4z2DEERQMX5YwqfEGw3dBFqcUh5QZp+/HM7j4U1rj0g6QgoPex4+bgGdA75XeJELV8CBkYDuxuNBr/GOwLMSWSYw"
                    "g65IYQ0dvxg5DDoTaE4AWHNBmQKBIwZ+CG83E6kQsQwKHGDs9BL80iFlAa2VuTAp8FZSClfB7MrT9/7aiqJSlySg1eEJmbWF"
                    "9E4Q4oTC3LglRBSuhCcAME6RQ9MlGRlr8BkHqtyFpizCCMqvyPFaOW7fP0O1cBq8IbBPMmygvNA/UJ8uHuPQaw/SAJYPuz2D"
                    "N0MRe3DxMsKzEhnBx8NeylJYjZNvN5dljIe4sZCIHjg4tIoX72Csqp0sKrGBs+04X8xgH8tMiO/gMIcZ/FCncCQkzEk4GYgs"
                    "pGPuhCVMeA5zJqAfwpjCxFCVMC/wHLzHSAXT+vejpNQetHEQRGBrihUXGYUyg4KMLDRp4mHXhzFcdij0GAwkpEwdlj6AqsBy"
                    "BO4PyxOOvCRJhAwuihthweCICj+Ks8CHcw+qXEjsQfgRhRODEEEHURw0iwUEKvFBGNH0IUL17wjGpACTAwOTZGzwR7yagOJb"
                    "8J0K6ftCur7EbvNBpeDSgF+7YGoODJZTePKHxQnGNhLp9BJGNw9CaCMfh8PDCvpFGoMbwTLGeF8wk/DzY48MJlhcHsdlCJYN"
                    "7144DxGqf0cRdCX2L/SlKLGDBRg4RCH7I+BxwBfKiUOFcAPhA7uyhEsMDY5/cKFs8+iH99HYNsrArOCOQkNhx8bwzTxIF8JB"
                    "dDwpyRWO3QBrhBflgkoHbgB/CX6a6ydOJvDvj5Cpf0OBg8ONDUyLBpWcFgHoOulovLgEXFtEeA1OBpckLAtsMwdfm5cUDsmg"
                    "UAvvAdJsOIz2eSv5Kc41ZYWd1MtgCvFfyAlXFuaVQoI4cAFeUFo6IgVFCWELsJg4DPAwncB/nGTPe1vUImHT+Re0NsXwUzBx"
                    "mKoYthlPk34K5g9TWFJ2BFQrSkMKaqQhCJvLieIc3DH2Yduy9Mek6d9K4rmkj2kne5TiTkuJRSuyGFYtgLcG3404dSCgjaAS"
                    "whD6iVgCLGBRynC2HN7oW4ldV5Kj4oT4MZwKWCn4qpSLhs0FU8BGiSn1CC+CsmdBDHvrOkmUgGUmMDw/Jk3/VgQUHJg/nWM4"
                    "MRR5ADOElwP95yd5msdZiM1KMfMI5DwFP4lg3ODX4Q268KDnynH5RIUp1Z5kcNxAkbI8TzJRyALeXeh5MTzUOBIB7Aa8hdTF"
                    "W6S0QiKiJC7yGMJlzo9LNLBenu9R5BiuSxFL0M80p7SagPcpvUikITYMDkwOfhXIkBL72LGlGycRFR/kwb2y2F6hlGD5bvdK"
                    "Tq5zkYU4H7COcE1ALfIE9hveTRY6VJKRMlWDcXBFLFzwjSiF7wieFCZ3vxFKUlFJIdwJni3kj21gePE5nFaPQinCB6cMAj/P"
                    "4AI44B04ZDKOKAiAPSzgNyVenPgwa9BDoL3gQe5DhBpwMRBtqsAA0QLDgTtJ6U4ceelDH0MJuuA7rp+TjxBHZeFGiSAPCyJG"
                    "gZN4P/yOLu/niEMFUDGZECDePvwiz0vKEJ4EFgqeIKyCB88v8X0fKjhxHNdL8KZEJmF/z2IhPyTZYF+DtsP5B1cPI+HmlEpI"
                    "wEtdWPospgKHIIoTbLsQ+inGBssjkcA+5AI+HBzFU+tONzRpgAkeW7VNzdVeys+KS/hqcZyClIMZh2DhcBuA1ge1gM9ZBjKj"
                    "4FlUxnkIzexnWeH4Ae1buGhngTiqaeh+lvVrVUseCvVTt2u+yp/eXRtPDKlSAaorJ6rpBx5sXBjAJYLOo7oRWOUgg6ctPZHh"
                    "1adx5rlUPRPBaGMFipHHcZMd6ukgtj/Jd7E9cgBz8EyfysAi34uwVnBvkxyaIYrSApoi9nAQQAJ8P4WjFLrwCrCkiUgjysIK"
                    "J3ZKP73yzMORZmMNnufmqYTNDh0KzJJR9WTmpfB/fK9Mc/wPlCMVHPmUMY4LsDdQSS+hDLb08vT0lapKsG7J2Jp5hJ6HbCIT"
                    "aQI2C78jg6dNebCSoqhBTNWRPo4A7HReiBzeWwnvIAUYaJCIwh8wghLPHH2Gydt2+idf3g677Ze/eXQGc8qs+vSG4OpDb4NU"
                    "Cz9L3KCgykiYN/iMsCYBLC/UBtzbFJQEPgl0LEzeOSYus6d93lHZnqQo9JfD9wO7qLmQlD+LPQqnhbAUMcgFBZzjDGteOEEG"
                    "LeWGGUw9wBZw0H0QAcowZG6eXH6QidnrB0VkiKAd4O/7gAG1GgbY0o4IoN7AIhIPfgvFpSPplhGYZuxTrWWQFxG5OOXFB+nv"
                    "D+BqxLC9eeaLPAth9OMwLDIKQcYBLBFMXOTR3i8jB3STagRzEUPvxn5OBZen33+sN/Zvage4XpoVIMc4EtB9WNqyjIIYfyw9"
                    "vB+JnQuPlQpPQihweIxCUG4oAiUQDt7eqWXt3gQVkXNJvo7iQbdIkjCgEsKw9J1YwDyASXiU2QsomRlEWKwYR5eYcJ7ixbhg"
                    "v9KRHs7u2PfbTlY6BAP3FfoKTykLMGoYExy8JKFEs8RrTnDiQypXc3CAKOGeiIxqn8IETwudtBx7xDD2DKUJ0hVAoQR+itNS"
                    "pvCwQN+pyBdbNYJehj/vY22gsuEpFgVcf0pyUOAzKcXY92O7Uj9dHeWBOi4juLuln4A3uCHZIbDyTEQxtBleXJZFVJNAdSlY"
                    "XuzdJHPjiBRmmcenymQQvEtlDAvmJ34We0Uq3AivFuwEJMGF05jJOIB6CQIcChmR4sfLgUESLhYqyPH2Tr7X5qr4Opt6QOTh"
                    "HFGlDJVYFFEc+l5ANchJga2TQHNJ2Dc4qwF2fYgVwRaGioY3Bs+1TItT9XFo9s22ef3g/gj6zYNbpgFt7jDPY5wjv3ShbQsf"
                    "ngK84NzPcagTT8BRwCkuvAIrAHqDM+f5gVRkoqp//kVs839RIfK/VP/Qwd+Ekv0UqMz8rAypVjOmqGNOrk+IkykSJ8T+87Mw"
                    "BzGAsijhYDqFB8ObgY27+rv/9Q//+d//8suXav9RZ5RBwD6GYYa2j2EU0jKkqk8X5hTvJpa0E+HqZX6UULrUK8K4AAEBz8aX"
                    "luDPnv7Wv+y31eFPiiaYcxR7WUgvM0+gosAzM4mjKPIipvQnVjPwZUFlw4UUSUp7JMVR9kCcqVoy0F/8V72KWmy1/SilDxvo"
                    "+wkYQQ5BPDC0MCZLBeuShQWIB9gBHDUID5MjoBqonigD0rh0R7/6F05l6kRE5ACadMgzcD23hAMcOmUocRahbygMLiFrAM5c"
                    "uDGF8MFeiQ44vo99mevvL5pvNc0F3WTUNllvQGxlKu2Fj5RlDnxrSpOHODpQaxKmvpQCHqwbAg4OL7w8GcFWUg1BQZUG/uhX"
                    "Wy6nN0oBHwwaxQkCUcILiUpBCceYSiFc2DO8/qIA405l7ieB4+MAFYkDq5nCYSy8cfFV6EC1htXpjqSM4S5IvFsqmXZJt6Sl"
                    "44G6RVEWZW4BcwKdBZ8MGhqaDduKKkfBmgu6MaAfo+mo/E4TO3sKGBMJT3FeErwOGLwiyGQEq+LCNvnY1CG/aVAxMi/gazi+"
                    "WRlT/jZwhPSSS99ux2xqde9T5sqB2nXdCN8nPfinIX0pdCKFhh049zF8OQevH+wMNjeD2YpKYA+gvcOLz2Emz49wCvDzCCav"
                    "oKyNJ/OE2JFwqAQk8MCUvbT0sdp4cRncUWhIcOsiS7HoYBCKXo4+wtxoUYcCpws7JnBxssCBQ2zFBG4mVpVIrRfmUUI6Rgqf"
                    "oijSJ7WAhffBbKm03Ln4lFeqbTOGi1LS8GepGlgUXggnqXCokMOlpDZdaoC3loBa5KmLzVBQolxQYVicRU5+ccmpz+3/39u5"
                    "Ncl9HNn9u/gd2rpfHikSkhCWRQYIyt4nRF1JaEGCgYtsOWK/u3/nP92DnsHcSDu8YVMkKHV2V2VlnlOVeXKsD6ctYRdD3Dof"
                    "eL1qbiOupqYInKHFygoVA1mUvxHu8OVkeugbau8AHu7+9bqGXFfv2jH1yF+GJ36S0TvHYSkkg644Dbkm9qaVbfrgvxKges0D"
                    "ZAIocE6379/5qxe1kxHAVG8TEJcgtHBcwCTLRgaK1RTOeVAlziqF/LUITpUNZ23JPypHbeteI59HFF7FFWUb1sISlKquy8g1"
                    "0CfVoAFXKpQfnAiG3YqDJNS9Nv/PGTUDxOLdTTvCKz++V33hVUCBDwB+SDk1FW+7dSBVQdNoiBzEcKfXPLCddd5OdeKATVX4"
                    "100rkKabHw7N/endPO21fmhb8P5QZpgwUeg2scGT2Dh2bXHo1MjiWblgPcDPWZxhqb5qA8H2zY9+9+v51ZrTCghpJirWqsZT"
                    "ZYGd1BjJ+pCr3Kp4qqrCANrg7DrM5AurPwpue/NjTyT4+L6RnI73Z99VtALv8DkHFW3zt6up9wkQO1PDh/iGa6rizGyQ1TB2"
                    "1X3HB78eb9+cIyqRDiQMmx0bJIZzhMr35usHAoeCkbciWIQRnNaPQEzFV8uGlgDn7vrwf+CMv5zhT9RnAh3McGMBeYDlrLNr"
                    "s4aUOMkkNcDEAPirrj37lTi8qi0ilLjTjcJtA+dBVCcLpqipw3TLz7VAt0jQS0u/LHdV9FYw+SKXJUVTcCr0cHZDDK4kjXzn"
                    "TziUJ2G2Zw7YCIuER2KY3Xxz8IJVfYhu/TiXJcBcIGJjgLA5w3puVAneJKyswm6vu2ycsdyprAICDrhUISOZuai6i6gMnh4e"
                    "UM6ewMrZIJgUmbXo+oGjkNghS0T8IilcIegTQByxLgiSASRaK+aQcPp+MKQyq2BzxXeWceABfpF4x1aPSAdHuTnv+uQrBbRT"
                    "7PTZqhwVRkHCLWpFGR6woSd6VapuMF3cxlgj1FfL0d1Q9VYNCsu34w0frG71k+sfSYN4rDfdkGwWiHGEdZVgqNvOEhhxSKJ1"
                    "BNgtVbDCxfjqtoI+7M2PvrngJJGN09epno+trsShBpA4G4w0OoA/28h+2+7YCtHh3UEza1jWst8+rwfbvVoO/jfwsxEUhvgm"
                    "gAfxE7uM3yJBafldUiQYEsV2IHGpmL/ao9yLpP3l574e7366jrohTN/IVI7k56Anu/DPyUAwplm9QGiP/rS2jvdC3J7fAfap"
                    "aXMIXGp3fLwmev1zncEikJiUq8Ph2UARX/VIEctLUgmAA3c2ILl6BE2a9jgCNhtlq2nuWJXXYLr1/sNPb349rQ/oux/XZmPD"
                    "DsmIMH4SQosxAle2GiltC1rzOiawBOv4b9eFsem3MdxhgXjw8d14d4o4hkQOZVvQtBVKaWOkWDvRlk/bqrAGI0KOlh41xLMg"
                    "ZbilcdCGpYh8hwHVjZ/DjfopGznWqZusqvySE0pc7EQ3QLNom1cnAduh2xVAKOc4iYn1TCysp4//sR1DiXXboLN0xiG+2Elu"
                    "U+SAs8xZyKtAhFLERI3KXEGiwapqgzxKzCPbtez5KXodPudUFqSrbfjj+uVKXP/qw5tRYSTprW8LmDHqmxJhH4mj2W0SigMx"
                    "RygN8Y2cB+qEGwRVYvRwBuka0XN0J19h9ROmJYk5NQ+qgXFan4i6qlAn6BSQICxox+bU9hvmBC6utQma4Dk4JHm83v3Zr4/K"
                    "56soBh3SMwJIkrOid8wJpS2ZnW6wrzX34MPUh0ZGZHv4q1yI78VJdOOcRz69kdbsaSeJnwC5UFgX/LrypYMeKvh2ufk92AMA"
                    "X15jqHyFQNRUQNpWIH2E1rTYatH+tz9++8Pfvnn+zbNX33737V+//fO/nxrKyJtTrUV6BprQFgCXU/trMEOVKQZw3AQ74Y+9"
                    "dOEP4gOsWK3RxPfzp3/97d++//avL7756tWLb//2+nOR6qlJuXXdu7OkA2cTwQ0wfqhzxgnV+gffMpaTupMHFxMCSI1Ld59A"
                    "CmLf2YoI6bNvvn72/dG3IoTPH/8vZT4SAeDXEw1ZpDFGk2NU/JDP0+ksqiiLau0qsarwLy3QTCZVAfpGvN/AVeOAGIRaOJxu"
                    "iziPLAgwH9qsFMdiwR50/70B8w3O6ozvoJDeyuC/PP3547/76quXX7/+7vnLP3378r+pReD1H3/48/fnrhhVZeuO0BABSaC1"
                    "HHWlpO5FigOJV/0Y3KSp8Wdw2vAMQJ8FouR8vUbfvfz2mx++1jY8+/ovz7/+r999++Jvr65MdBJzBzByGJuucKxZsbpaiDNG"
                    "N+qJdYuRQ1oKWdtXsCQM1cTcQoc+5rOJlye68Oy79ySrD+3tsz9p2MgfPi7tRVfDbt2VHYAEQxMMxNCEo/wj6I+Ut/nK4MEw"
                    "syP2xe5XqY2Dk1eqZys/vGB1Xvz1m9d//uHFN6d6Ybd0FVh1T2K9fH1AgKb4AKcqA+ihDBAhwphRqSSnupNy8O2qqqLQLz77"
                    "1Q+vvn354qtzcyxpLjQFB4I3NM7EskCvKbHPkUjBMu0StakwIxaRGG16IYqCxYl311/6aEP63KP29bd/f/7yqz8/f8Z/vPjT"
                    "i6+P43FlsYE0dRduQi1kWhAriXwAXhvoA4fLowHRQCyNaKSSw64WHBiSLypZbI9YPKsD5KELbdVBquMWH/aBdRKgJW2C+tkh"
                    "wguwwU3yA1u/JxhvcyzYr3GPkT+9+NuL7/9yOhsJjhArPP54AVQrDUB9siMVpsW2E7AIdVJS2PwcQqzRBWzKTWlt3Ldyf2Op"
                    "/v782Vc/vPoLf/nmxcmF8URwm17QidpZ9zkW3A7EgAgkIh5QQ86dulF/PyTHg5fKmCMRekJ1D1t79fyvz//b81cv//3S5Oyk"
                    "UmKpvj3ovRjHweMrwHI4mQ3/JoLmQpgp8h1LdqkDIlUhOin5dI/JbwkDWP3bn59d+HddKgpg+RwABLMgYSgkfyceCOYmEIgX"
                    "yyNUTzunSitG9N4Y0ct4jy1iwt+x8fIUauAsIUK9UjTgD2C5y6o+3CT1nOF8M9g+smo5SiJjFUPegtVytAhP8LZ7rFwW9ksL"
                    "gIy3wWAwehcFYrOqW3cFDBQCDbSG1RsYmjBwuwVd8bywomrB7zXx/XNC6Mn19KrQ1J1bdQXCtx8w7a112mKCpN/hnXr8qp6q"
                    "TebnkFZICoV/bWq618jfXzz/7+cGBcAOuFsN3fB2I6QGul+p6NYOKq1LfQPuiWQcsu1aXVWntUyWtZr7Tun3z1/98N3JQk6w"
                    "mgwQVhE0xwQ34kM9zIMQUDeeFoKdI2wSYgRmTqmJNHKlBS/HdZ8F9eKe9ntu9bOAxH2KpVcoZp9+wHBN76T7aLv60fVuVwcg"
                    "ltMz1V44FZ06AOEeE69efvX186/++OKvL16d0EQEDRarNYLj+DQ4eHpdgXeBnrx60Q1EICRdF2VdfW08Qi9u5Oqd4n2x4Mvg"
                    "OYAgYAr4bg1BFw3jYIcql6icU7VX4eR6OsKbCGW4Iywj7tH4lXCuewzNd5KiOjd2XjU0BpC/Lh829D+q8Rs+B+FwfDq8MLVs"
                    "g94jo0qsChlOEgrTWw9G1+88W7rnYvr1j5/ezBPKIN4ej1BEFAcYzQ2ku/lZJAdiqYJbA4Th8Uv3CFOvXnj/LoI+xHd7NvWP"
                    "9fHDx/er/ez+7eu/Pv/qbz989+zl87++OO3Us+cKBWCPU2cieI6sYEcWE9lRfDhluCGEAOSdd2TxhDr1GsoxxTmbKtxNy5FE"
                    "lO8w+vx/ENpeXHQ/kuqhZD1niR/omSYW00JQlVzIRHJSYAehVd2/1qP1NhSv53JdZ/c27rDx1xd/ev71v/PzTsiGuDUA8XGl"
                    "wDcGQkI/9iR6ZaINFKEdd1qlgOj7IKOCLgHKWTlk7M8efmHhRv+T00Me/GVKeqM7IXICNeshqmMLnDiElB1HZ2wXI4haKIpf"
                    "MYC8+a6dkSryhw9/WP+rSTzjui4JjtfbhhiEUoE5MBAwgGH5C6nSZSIw/yfZB87rSsDAauGceH2Qn6Y7DF28H9wyBp/3RiIZ"
                    "Y9idYYXgDjv4M30iwTQcmTWBStRdMVZKoVZC6zwuMkq5w9jVheAtO0kP82aY6NgPXSu65tXO4/gngE50DXKntl0SKm4ioQUD"
                    "zg4DSsABvmv7oY5qEb5tSTXz1vqeSMRjN1GAoJ4+GC2JO8ymGkQVI8CCGye51KyqlYEf1nl1PXVYOt+Hi9a8evntX58BCv7y"
                    "7Tcnd+56BKt+q3fPREgKaCDYCosEN4tAHuWFsDHdtpXiodnBrC59kFJ77V9Y+ebF969evvjjD6+gZ//925f/VSzklEbVDjVC"
                    "XhxN/Hvhs72rUzEAeuHeLNBUH4tArqkd0G3zUpdCU/DI6QtTf3n16jt+zVff//Dy6E9+9vevzqztDIR10lklMACnfVqYXuWA"
                    "cpYANgBQoDeBVOagQaAOFekdvqhrtc/x7l6Lp5AAzOX47SGiaXNatpAtiHCb/L/A1MF4iOzMQQBkEh10/6tXblh1+Hxgr81A"
                    "p374/uuXL7579Uwn98Xfnn///XUE7wmiTVxju+EFenIgcWRFa7Wl6lYdED862B/stoL1ymOrGlB+XV+Y+vaHV9/98OoZtOr5"
                    "n18qol4BBv7ju69evDxtHCR5R/IfjBaiXFkZvd9ixumOUHe8bkomKwNT+Zc4zBJlYfEXbNU9bvT75zjnN3fZJlMVqK1Z8AQD"
                    "zvIlWkIX6Ig1yJmoRRJkbRNUYELHjVfrc2E7vUhGedz2bafRSSMJLXF4dftsTpUUADIMMvJXbXGDIwCfU4VqOqK8YgvsU/07"
                    "9VGLJ3kKo1dl/oezLsAFnu+BNKbp5dDktnY1Ui4BV9TQ4LkL/4G92tYl3RS/3Mnv//LVS84cgf7F999f/xpwUeu66uCQwTV6"
                    "gkYO8ClnXJf/qgTxaantaoPEF/lzG068HcbY3POX5/v7Vz988+/PsPXZKVUZNauunpzoBBgmeGnKNa/KEOCEVL2IwJAzDr/l"
                    "Gyz1CakmjSMxvjxm19UXz87qg2dJD8AYAQu3dq5Vwb2qos1qlHA5acRKwdsy4GiJUGKInIQYvTOxgtdrpup4jRg/GmHfaKro"
                    "MTj4Gsf82vgap/gIHVlEvux1AiwnOghArGHxLitFH3WgSxtJteHNAKT1ul9LOq78dFtyWfh4pXsBrM9H1zJcC6Yi4Z3IKSIS"
                    "lWS18VUP4BLPs1OCE8DxXgkyfcB07JGsDjHAf+tv3n1U96AeCt+crzXP7VDQxGr9TAmmYNgE3bgmyB24ecBlDiWRDUHK1ksi"
                    "Tv3Uup2PKt3tLVwbuerdvbClCmRrzH9cK7gBcUAf21kcgBjn3N6dTwgwFpusXreL9L6kvOb0crO77hSH+A1kcDxu6fXb9v7H"
                    "68wIxR8Jfl8aaMeqr2qxGxliBGGFu7INTtl9Bk9iNjuAYxosQC/uzhxB8B57xyoeV7nXFZXBq86WYKoKGvwcrMCXT40M0ci+"
                    "yhtWd6A1LV1jHgIAEbIb4KC44W1bnxHMs8PLnr3b10ITe1S1h4ScmwSMJg4EfN1EPQif5dfk427aqLRod0KGTWQu3/iGulgO"
                    "j9u6bq7Xvpukih+n/gtlE5JS57TYYW1sC+6PI7OLrURy4TpSc1YnK5FjfmHqfHSejfbLVCP/ZeWtqkOSG3N16EzS+4nTY39R"
                    "dbPHa8C5LagbdRc2VQp1NhRFeZ2NfDTC3TB2lOTzJx8Pbdgf37/79Otrd90f4MAlfmFvi+CbkFg09j7g9Fmt/3p4GviIrSsK"
                    "oZvODzQxmi29KnOnscuymDO6TSDnFiVGAkAn79gAWEnR9yFQG73lF5BJrKrWzN52R5CWl/4iCGfP23a+eEA4N+yR4NhsdSlC"
                    "lqBvKS8imsrEZjM7g2697dJecaWA0AbnNxIj9fRNMPnC2c9B9tm7Tx9//SThpHMVwBlJzzj18F8d2yW1z+4rQKyl1czIh7Rf"
                    "kswIuGLpLmpMBxXqYVTVcxyvRTcMXglqHGf5WlUykOhA3Wvq6ovME0mhKUlZy0sbAN+eXnJiiQDZSX9N7zjEXUgWWOAhE4SL"
                    "a8mOxunAYYeeoDmcwzqX1TFPkIaNbNUWsbJel11GCK7qnVOdjsC0lY5n/HvNXBdjnSu9c/dFT3WDQ7Px6KIaekVFNZaH6cJq"
                    "PXp++By6epr4SoIVZuCNakPutqVBUu3ahrXCCQsWoaeNBANwcyjoLgLPls7AqjjBwD9gkKsH2BxcNwfVlRbrbtv42N6oXew0"
                    "detS7IRMBiv0DqqDTe8JR0AFwg3sQyVI2VVCKugd4Hd0ZqahxmL8kQUvtTxu6WYCqSQ4QkxXjYZpA4cAQIzW1LCcwFv5eLve"
                    "gK9ppalS9CyqhppMtDzkTB83d20tglFTM1ECi+xT8rqtrywWFG5DqknuwYEoegnS0PFjqB3bLSIvccn7L62dSlDPeeQ6RLg8"
                    "1SLqOEuAYg9s3Gpv4KPl/0YttoTayBlT0WetriqjEJfXCgCERw3dOFSqz8IxEoy7c0CTJeOZJk2wYDYQDwS4pE431OOVSJzi"
                    "/dOxaYCl4e63drP2+OwhUE+SQ7Qcy7x8hD0RBMS9Zzqq9aoelzvICRirApzcZvagv24O6YL7rR2O/1n7aElKgSzhWBGyvhUZ"
                    "wQOB3eqpWoSH2TYRSgXgun/V4fUDXu5bGeteM5crl4+dhfDhxbkVMHbWs2ADbfJJMI4GgxxxGjjU3Gpu6ET2UGEdnIPwpftd"
                    "tWh9PrgBN/NF9wAkV6lZ2OZaIsl3QTTrVT+d64LVp3U0Qh7nOBNFepe20LWBN78Qsv/tVXvzxxd/eiZN62d/1t999d2Li+4d"
                    "3G0BtEl1R/2kVzHxJo4S97whmlbd/ZF+w1KtmLuS3Bu6s1TBbfL3W7swokvcoHLJq4CdVNKYeiYl2aWCiC0RJKu3A7GpkVXM"
                    "FVTuqvNwJRF3aeQm5LtWgboUhIKQXDZ05a1Kg0VkOroAZu1pJwgFsIz9ZPmKbjOaDhg4WpK0WfnpQKr44++x7y6bsqVwgMeB"
                    "BNuC+wmJZukNqBApbfATUYygTPBYC/RLVIF3QetUQhNN+T32/YV9NW1slWjzF2KiXr1qNqaLUuo7zU0YGOxyV4aVDrj6Q5vg"
                    "PyH8AgP/Bvvhwr43DoA4yQjFebJEwsvqUusymQa2TqiZpOqQ9TWk0zVJgAB1nMWAUezvsR8vf/8qyjUCJXAXnd2smiUyKVwZ"
                    "hrHGEKStCZYGnnBNym34oh6CpY/9e+ynC/vwTnXlqavAyoSHVJHS9yZPqjuoktJIHlnMyui9QDJkkpGubkvD9vfYz5f+Vwu0"
                    "foYqpeE+OQKkmq6cFU1YgcOMZcCZU/8ikYwgKlRHBFoV7Bt+j/1yef6iHtt0TSBhcRFUqytiDwvsamEPIwHwdtzLqSBCJZOW"
                    "Y7f8kNRPMr/Hfr3x+30D1YAa6hYYaJC/qOaJ7JaErXE8YLaamojqkkiFWzmSVAODQUfz77BvLxtKOcalE+0GXFNFxKWOqZc0"
                    "dd6xE/xpEh6UUhlLr/ewHDmq1VnnSQW31//o2//0/u0XbekwvalK7qk7k6Wie4lpr8VCFyM9ZM67cHQt6hcZyiBAFr0eiQcF"
                    "86Cdq/ri/e79z6pcvLbJkrXmekge4EUyInKbCJxtsU/hS93gEPcdiXwsofjcbcz6YQ1QNm779hVBuwzeZPNlgZSkiuB1DWR1"
                    "i046ClPBCyZKsK5KtOohBF8ku5peYQWbdtu3DNzNBy9a+8cEBI9pQGIAlDbGUBFEBpSrMG6rAHNLhBQX0/NQkdaj3sXVEVhr"
                    "u2XuwjuuwAprqTX9IO7RL30088FlHrfpRtICwEwVJ6vmU4Wm0DldS0siCi6apANPMOX7lWbVY3jLrtjnz6fntf8HXZiXH/3/"
                    "9XfcMuZu+PsshrQi5Tf2vCRJLNeQnO6LiF+m2pqsrrgBzHnjPQ36MyvYyUvKy8nWPz79+i9c4f/1ev36Lyi6Rj794eO74xZP"
                    "7Rv4+3KSd19u+1CxQlpSw2ILfP/FsZE+KRCAQKyS61zw5EDOIkTzmTdaOOf656m7UhpyGVq6JG6p9+ASg9PzUpbAVSOsk2GH"
                    "y12PS46f4HBtTbFw/C+mjbc/+QC7Vx+triZwofMJVr39ARGSKdG7AU6TLpEenMExUTUTxDcJPFlHrAc3OltuffS53VSJvZN6"
                    "CgFID9deNciabhEEmAt0Rg/LUaulYj5y9nHTSJb0cR29iB/G+zeaqHGeFnHWbjyV+lvr9OIJiC7S9wSxQjqdanRKEIefMfis"
                    "9/AgQUVASSvH6ZWekTueAW4bOIawnMoSpWVKRlkjq/LSEleFZ9Wo4fCH4kByiz2Q6MQhMUk87lKX4A+b8+a+T399KYg2bG/q"
                    "NOPzJD/F71EPOlkLzGbUDTQOQWfBiKz3Sa8RJY51BD9wjOxdRt68XirLv5aOIxdZD4HMECD10XNOcVLIIO6Ji7DYrWYVh4aQ"
                    "jcTFVUIcRAL8lmT6XTbetk9zafyV5LzftLenHeFskPcwCPxUn3xiDbNYhm6tOa1V11W6F/cacKPazp6UJdQ5YdpRSXSPqavi"
                    "6NftWm5MFXDqm9n4TvRQDpOgT0uX17hwgQLMEAI5r8dIngiSj+tEBImkZKjb/aZUKD0vLJFlgwrxB/QOAlk5KgaPiK11NQsA"
                    "5kMPV6VgkgSYJGEvjEfi0vNNv9/SkTTa29fvuuZTXgmdXnXD6vnIqr+qDtXHQGxaUnGb7AarmpnGF+Jgq9WlDtgTRNtC8VVe"
                    "uspvMvn6n+662WdNgqjqvL0KvoN1cM88pbbaBJSDyqcD0H6osl9NbRzm7SU+kw9dl3usHlOSzq0oMFqWiXA7pe8lBSQzzTAq"
                    "DLSSAVElG3Alsp3S1ib8QOQanFUFVNM9bub1P9Op0hq87RYQO+ysL01g8I5QK41i8nu1HKIJJhNUWb4Fk0pxhGEJmsdwBON7"
                    "LakB67pB4ugZB+Sbrc4Zr3lCZZk6APi7WN0ghGNawJrGqSNOl7Ahlp7cVKveeIKh11dTf65dZBwV533rCT6ovX40FX7UmHVH"
                    "B8GVEGguFSRm8hEXrd5+iRprcEryU0xeqU6fnGMR5/gc23R9uVg50tEIRaMGwLccvbidKiRVhili04cynRZX98Dh6fZe/7Oc"
                    "itmr7nCAXn2oICSyhKYsIEY2aehBDadRRmEFjfhdIqvgKkqTLi5TnmJSqiefS/NNJ++EPDQ7xSf1rgygKAGKuN/aLjLd1SQO"
                    "lfek9kkoSBqGpGua1h74kR+XBsh8fH9aTaO7+qqKPamybnVM8XN8nOpOKeCC2MuW+CO/3gqNkl1aLsDirVumpxh6/U97asdW"
                    "jUYqQxoFuOlRHmcl76nwf/RlOT2tFDvUcNkkiQhRqaSaGNj1/CRbpxAixaFAJgEyed2uZEIXCMtIVTntGfQo7LeEM1lcpwe0"
                    "PoYmLfQyyKT+Sbb8ueOeXMXG6/k4wnNAhHhAk3NWWO9cEWLn1LYH7QMaAbP0hkD6qfC0tJ9kK5ya25IuTIak/KJqqCVEQvo1"
                    "6pXfsNvi8tbjbpG0qXRdiNIcu+ikRu+PaR+P24qnNdwRXkwGsKppjlatvZ2E7Im6KpoiFMP/8jD+KIjJSfcJC8Trt03tqDF8"
                    "3NYpQqZK9sok6ZWDGgmUylJMVh1LTsedg7dUZhAlUK1CqeFIRY6/JUS2p/2ufMrY5LIqYd8+NPaIjA1o6lnFRNmrJV2tOwZk"
                    "A32PhZO2OcWpqMJ+rnpUjDzB1imzXXfdbfUFqywzpE3Ogp0Qd8HLKkVP0osNSfdzOIxmcUhg2Curhuak72F/o9HrQydd2YhD"
                    "zkOwqepFsFdWcOdJ5GZxV/dtctKalLjA59AQ+EfOrSUVOj/J7ilO8plkuqjORE1YiKBrNmnHDUXicIMUMpjUpaPm0mBUBbil"
                    "+5QA5c3eubDvTi2eFxlg9GpBV+ygaY5QK4UmYHdVQ6olEFfiB7l6eD0Q5s26xnzA1em66eWusz3ftB9/efeBvz3mzxKM/3X2"
                    "S41f0hX7VjMhJ0CTG1QTZgw4eDc+L7BTGk3VYWiHCLmLVVXxBNX6NFvXR66nWDTuLQGG9aypCl6R26WEJhWJyaq2tdUNPs2y"
                    "esHRQEIVbEj1+qnmTqduLdYOahklEwe1Slu0p3fNRlDzZ2uaGBGNJ3t6vbEHFcpI4F83W8fV/5PMnQ5eWmlpD6zgmuoR4tBN"
                    "2Eq6w/EcLoL99EXj0bRTHGvwLom1cuihge6p5sr5yHWSmWYFhRU6K7ol2qRaT5VlFD0DHT2QWmKjftXs2Fi1QcU+wA8Pm/v4"
                    "7t3b6/mjp+VUb5wmHBKNdb+39H5KJCnEQ7ghyM56TvpOGSitQT49QePIHJD5We4kODcZVPINaJ9M1ZA8EpqTuLTToKfIj8nN"
                    "bJhM1YvZTFfxJrGpajRx0j28C4hcjc07dwYnmJaLBjS3Q1VTYZBDgLOsM+KhAHtpGqn8tzoJ5bVGCOnKprPflciuBu3clgqY"
                    "fNV6IDYrVc2kRz7VdOvWxg540liSGlFNm594tlUvkJtEYQ+nK3cxGY1Yam/PTeYVbtKEehPIbVd8CyC4WHtYiuqQZwFjb43L"
                    "0PMPp7vwuRrqo8KZ4e///GPbT9HO86Epm2OA0chbbYas19r6NZD/MPWOC6AwxkczNFisgB2diWG2Pu6KrFKjWxJWWGdFDXN0"
                    "Q3RdH+es/nuA7IBlSrswdhbLC+UTC6dXqU0ThGtSChITnHf9kF/f/LreXncnw7PEQbaaKkZzA281WgjjXK5rSvl0LL63XqrM"
                    "hC+za2plxcHhGuuuvPsZ03Iw3lyyyeZ1PeRZEY6EV4tH9IOjAdorRc8lpHMT4ZtSv4ZGLrv1AK0LsJDb9k80dg0CUydwqAO/"
                    "zgLSqseLnxBy19t51vA52HqxZNjN98ia42pBBIfzs7ZPtufP2uyWo8Y3TmKl0SRVp4N4twoSSIEQH2d6LnN0H1hBa6W4qUsU"
                    "kli70yPutncCgyFJf1XjDtTuDf5TLR3ZURWFh8TaGDVKWdBOsBqMtQFvqibISu27xyfbO2cn26U+Te5Rlabe5XFOIK+BJSgm"
                    "aYxmlpq5XywwORraKxENlt8YMx9Zz/nzmw8frn1lN4nS9Hh1I9qKNODwjXrohSn9gNt8wyWlvjv0iAi0KQQ64Bowxj3N1jVM"
                    "ylBmsOsuJHbcg3N2TOyTBqMAGl8fcIThZDVIbUxIfNYj+NTcgSu1jieZO9OTYPDHOIfZUz0ganyUenC0Htal8Q+QvJZqmHoP"
                    "d7oa0IN5gCOZ1Ld9qjl/RjEuqvQb5tDhrlInB65b6fjXJPlnl3Xeh1BiyYXT52MkAksCDWj85F938ks4SnLCj5XznjdbKYGt"
                    "tiC1cUm4q8LETGN9m8ZdmOisxNy3Gzhz3PVBc9dSJRBGFSqtKT3qTawnS3SCe/Iauho3nGXqPWBVIo7jx6v4CMgv9acBbXnM"
                    "yufNApGRmUi1lv1XDW3lA6QpHqz6d7Pm2C690WjDdl1el41BahygxDIfN+TPyi58YXYkqSVZjXRzrWlzcxB9xTA9Foohb8tP"
                    "5bMnP5nf7W2U5kUxjxs6bRCZSGMIpP8zu/FehbG4YSZpAKFzONLlFCAryx+jbDXATylMz2qxP2joM0Y6XTH4mFgS1XLUqjIs"
                    "oPIeQ+MOpekmefpdA+fNWOiQl+gu26dLKetVavpEY9fbBX3U8xnbA80uwbugso5qyblDNb8Ghkm2TJrS2jTzV1M4ptQsvdeg"
                    "gSfbO9P/VYbaqrdRi8/eVXRZk8ccwFwXT5qMxm6WBizcQ5VJGvygwXx6milPthfOs0vws6I5vvh5DRoyqobqpfY+q5Qn1c+s"
                    "l1DJjmapUujKZUcyELB3PNneOerXY1DM8LuAwZNuSY5bhS65jNXgsFAk8Cc/lt88OcHidiGCvTLbnR+0dw3af3rz4eO7M+ta"
                    "mYCkGMxXh6GEtPQkI7nNohKkBXHlrOM+MQ7nqnQ+AXB+N1Vyp15/k8nrjWT7SlK3D4fYZjUzdtWKSxBbraBA6dWMHlpyzXCm"
                    "IgHOIR3HqYTTfqvV03bqnhDvJKs1kT1SzzCa1FZ0e7/BfOFK7KaalZTLi2C4yivKapzf8hutnu92NBdtGNOtSuBwmOmII1Ot"
                    "22om0LTxFJeKyeaBofaoyrGarKcimYed6OZdprdOaJ+EqfH1kvgMKRgN1SKcjcn5082pm13X/RqBLhEjMiKMG1Qx704+R9vo"
                    "1XHYupUxqqmOs3aNAqkJEk6YGVXCuUXjNjf+a1W+bTWeIqtgfneV6Nz/8ZeaiUQwPg5/8F4iK14cGADcpZNstz/angZHDA8E"
                    "v0KYyTqSSs7WRta5P2Lk8/WeRMf5EWBIG0SuvMSxrG4K9VmkyGWTTt7kDwepgOgGEGswmyj130ftnJg4yL3MpakKgJ2okYRe"
                    "Ja4GGOKHaxy3tutu/GPo2jqpQhJJAHkra8D1/XZGg8Rcy7aFqarmzt7Ywm6wWFHFkERCQvIm2wHrjYX3SdFQry8xpMHqAZlZ"
                    "7bYesPO2vfn5PPVql9BANCQyKdoNGDaRXyN21FKpcu2hm6/JL8ySHGRn1Engg4oF+g4PWvk0z6O7FulirqF3ypaTpLeXlDSc"
                    "RLSgm1DvAMUnAKcS9K8lEW6XH4rVyfaHrLw5a8MulSaAMNNYlkCaJ1iAtRqVXyWYv7MEjflIcqkG8uylnjcNI5bq3gMm3vFP"
                    "v3y6IGMe2m4aSZj8PF0c0Fe4nYY2O/hsVYOHjxqeSH4h6MOoRWY0lJhkClh8yNaFWh3Ivy3V/nv46NZbskYylKNCXDO2jHdq"
                    "Fm9FjeYBrNulEkMia5q7UfNDZogz/NkpC+v3HwBWt2VxrT75jK7qHAJ4KCNoSqnNUmxf5E5NhMFPbJ1Nc7/m43Zeg3I/AWSu"
                    "80WWmo4VKIzumCcRTW/+yFpmNwM4zaOp8k6PhluzKQhGZBdV9rtU7RNMnulJkPDY0hMMMFTvgqF11UewSploUKPqW9kh0mE1"
                    "Q5gdKGUMiesYweefYsqdNdYU6ZIUfYbEVIckMWCTaUrid2tBNfRQMgOHVGRxXZC+A4TJFcE9xVQ8m2ILOLYiP1LaNIGN2I3F"
                    "yYFFzarqsL0f6sENPi0tNomMwJzh8848xdTpppOvH8WO1eZsnWoDuzqKfDC6oya9Hi1yYl/g6VSmlP4boWTq/SP1fb+pz4jp"
                    "nO+KfB7CKp9jl5z0niKQW6MLwbkdsjdVU51UG4kZgkqzhvN96Ew8sFe3HXCANQ9hQ6P9GZKFzSTVkpO7Ku3EaRJHrcaxieOQ"
                    "6V4lTAjQN2PcgzwPS5paqD7EEzDCwfVkteG/08QmeQ413izWp1X1SQKdlwkb7g3VkqawJj8PEr3ULR5wiV/aP9/8eBGUCHr5"
                    "aJRr1ksR/KiNFOQhxANiJzRL151RiqFjKHMEVrt3+foy+YG1u7paOyP2tQpRdgg6rL1VXmCaRl3gBZrFsbrU5lXCDAST2szQ"
                    "sD0N4AAV9gfyxY2H6qyBSl3Nf7pz9F6X0YPUKvxUyN1tEWNz0guPcT3Iiu7c1WWRTH4g992QqgXi1zxV4GXDbA7oT96ckK2p"
                    "iiEjeaGq6gxpi0tdXMXfeLi06pIZrT5oRhVln1/6AVAseBO+WuTOHSaeMYSWA6CFP2jwBD1QyusJfx02MsnkqpDLNT5o6eez"
                    "Mv3Uxb9ANsk6setAcAzsGpsGRFh4oxRPxzIQbU1EORSpwP6rR7W4PfB7bj19V9WErrgIW2q5b3pXlWqByWk4deFFTXd1nbPD"
                    "4TnYKuFVk08OVa39BENfnNhp1PCcVZN0SOAT5KKkyZtqZYP3UAtOWlcvlToOCSORbUxEv83uhf4Um+dATgyQiH90nFcA0DJG"
                    "U2yC1HK2qdJE1miEXusUPhZ3JBCpeS8Fp1kMT7J1phOEAQvBnKpdtKoCrroPAWNG0tLQTEnJ6Gv2eNFVgPe+TWFCNYvFPp5k"
                    "6/xoVbOEBSuIMWMKMi/lW8d5NkQ3dWDxDXZkY8GYy+u3Wg0gN1ZFTHde7n9YHy4Iiwa/2KWG4XKVnoikJmuWtvEcVEKBRjg5"
                    "q7mnzeE/qiVMrgNlyV/prnCnzp4P569vk0rrGryhQZHTqLWCDoB9eso0TZKdBFDObzmEZG0cTToo8MTE7t3x6WdVnZMBsIjL"
                    "SVpDVncBBbpoCr6Ua1J/J/akPEcMGq7wBzHXY8KAhGQdQfbCwKlOEGL34c1ndr5j6YR7fYxwaZrSwlcNk4+c9F6lriAxetiS"
                    "8WrIBXiDfDTjc2Yw86WBt6v98unXY0DIHz78pKjJEe5T0XxZ2J/zgRMhPYehygmZjmGACpJumE1ywJSuURKrj6PK7uLDr562"
                    "Lj6bwG6bhvn0GqQbVjSkQ7NAumYULpb/+P5gYa93VrM6LEv9vq4IwV8u/S/rf158MGiSaGegZ8EIPwdIpsgHXLdblSD30kIU"
                    "QWg5KucveSZZuEWNCtFR+3gMRjrv6BVbO4mrn2saU9etKnDB58B5lRBwDSLrpGaj+RNSBXWSeQJmAswUPEkJElXqa6cvjJze"
                    "8W9akVyFl1jgkja4ng+kHKt6W42SWFAEstOWNEAwS7ujBkIyNPB2sH5fWjl//Mncs9E+trfvfnzm/mD/4Mp1Z3QdTuHcq+Fa"
                    "Q0/wqULO1UjyLakfjTBm1yzH/OhRGmoYg7pyuGETB5h+xO7VkOwPZ8PP4Nxn2xpOkjU3POBdS7e41lawUhJFUc6UFhP/6XQ5"
                    "uuySHB6kYsR0SNE/avtHjtBnu+l6/qL6UjV6I0eJtXmpA6sqT2OAxet6UdMW/gdxAagQSpuqR5bOKS66f7Pdn//xQQGi45xm"
                    "5a5es3R0c0Z4JPFaA/xgSS3oR+d41J5pnIKcnyXRzJwrwfMnmP3l3fuf29s3//tq6tSXPz6qcoKFdBxagNwR/uBJzfm29D4u"
                    "UKY65p5g0uqd68n4RaxZV+1j/5ff4mop4kia3OSGooBV1mPXsSHNSQjpkBq2nYUjYGZVsVPTAvjVNKYoH82VD38JlfnfdnUy"
                    "S+Q3hmP4lupsMEHGLKovjKy4s4XvpXmKIB0Hf82lODfxPulwtPQ0o0c7w23ThAW22VUSSawADzB92kRWqMg+mney9O5i07hu"
                    "XZ+T/XQlDuRRb8VB7h4zfVS+fl7peP2rU9RFOMdrZGiW5G6rB2xBF/TijxMQMMOOnsSUvJXaAnmjG364mVfQ5zebvtpk9YRs"
                    "PTfVDVwkIUGRWsi6igrOaSqZppeT/4aubgwHTePc1KnWpMdqn2D5dPN5bfs0f+HcYWs12doB7qaDUuwNARCvNJIvA+qC2aNe"
                    "pLzKaEcnd+pI9qp2h3FVIvvUL/BFlLv5TfQaPiRepUJnqzcIFaEXjVwMUarAGoRFxpXCc3AdYAB8b3V6Cds013/DN/kAePi5"
                    "3fM9vHo/JbwKzyKmdgs8HceYVduA2GCgpHOmUok84ByA5TTs1AVyqeUoY3z4e0hs5tOH2+6/ZtFY85ynhtzZqCsjyclYkvEa"
                    "q+LlRYVqQ9dvCbajFzcIX9UuhUP082Gz13UuF6HmmZROxsd1Lc5RQDE+qOExSu4XLqhHeiUBwqsEQ/tR0L6kbigSH7udCoGm"
                    "Z6nXtt/1La7VDBokA75I/nTqboMcwH17CCOD/wHWahtvJGOpvReNcopuqxiHXWr7CcH/TuNXBzFEfixMDhfX4CkPi2zCnVnD"
                    "iNQw2baEJasU5jWJzWo8mwLgzFOZ6QHbV1eOz+wfzB+KfybmILGX9v5fn/FFZIW96l2y8F5Q9X3G48OWSD6EUgX6gzUfRe2H"
                    "E94OOzESAQT9fBkETvzkJnriDNfoNTlFUBvM64ekt0FOyWepWMPISSMAz6k71+YycGccj8uHS35h5ej4vNltRJiQVlbQLBRg"
                    "vJTJjV6m6sKwL3AHSQ83EGsglEKLJS20AC6SR7dHTdAdNr5sCVpAbEBGUPfMiBpRU3WBlYCSsPo5pgGuLY1Iheep+WPpJcmp"
                    "6nFIPOseO/d07PC9p+orpRfrfQL/S28Y58cRIW5ZNwgSjYip5yLLYYI6jWge7LmYB619cUenkjujGdaAG9Ix2KJYpxqLEqTP"
                    "Opo1W4MbnTGrSmnYmJDVQroDYDE+/NuuGySun1uMFIwmmb4BKDNIakkeD/QuMAfUNIlQewxmClIOV6Peku67OpVJCuNBc190"
                    "JKUlQXMwFL/NGxiABzUOtlGy42Ao09mjpikidqgZMKpYnnzX1aw07sh3N6xdAqpzmdU8puZKE78NFT0AqPbewJXY1Mo6OEpR"
                    "4yDUlRKFcXW5q+SjdujwoL1bDS6hHJIlx+NXrLrq1GUFPHnO1uWae8MUwzCS+nNdr4iz9qCFLwGi+6CtWxdEi1hkxyGop5r6"
                    "XDQihbS8B2kDWoezlCHVDDLpqJAmPTeSQnryknB+2CXPL36Z72p2Nbqhy8cccXKwg0zD2r0jRAH5w1JlNfxXWl+k6mRddFMd"
                    "42E+bOR0PTOcl2hgdJLo93ienXvpkqQfilNquNCM7qQzQTq2qlXVbWgk9fdt08NGTvcym/Xi6KjuzOuxnoOK/x3HV1poUZP3"
                    "SiOQE4MjQfgYpw7b1g2QM9s+YuQ00+p865mBa5pweJBUGEtbmvjo8GNVYfoxpOucrWQ4OOxE36I2l1lMdauG/bCx07HVaOpe"
                    "bIaVsPp8/ymxyubaAhmYKi1YkgPOsDXvvIMnF6ZV0MquphwfNnKqUu5bE45YsAXkgRQlieE30v/RriSVRylerSppGtWSkkua"
                    "BNKmaoiTXfcYOcRePr2/OKauqhwxRk5dIM3iAZxK/En3Wa53MLGBcku4OqmlhJ1XV6vVm4ytq9y3P+eXlvcLwnV5jdagsI0I"
                    "K7GHCGpLeZgd1JG4qiY97aZWVsfhib2p4nSnQ7MiSBa0untW795KfTyvHyVhsx+hlixetxRUkx43iTpWtY+LGFA0B3MbdTIK"
                    "2y1+e9v5yeY+nyqvfiyObdk4c71qVT6GA9ZjopTXeAHCrsTkbfcYmzg8cUTi9X78FounIyYNowFoJKJlLwgnwSl1LGkA1THz"
                    "Q3cx0GmvMRzJN3Wolan+RkmZp99g8XQEND5ACgpuauS4ugZx+KA7XM4tyTDrznWmLLl+lQqr/Dxq3BbAUnrAv8Hi6TxMjZBz"
                    "EsJx7KcETKBhjkgLQIczJElLFGvV7glxUu4XKV+tDK+40u62eFnmXvRoPyogMzrWb2dBjTGHJDLBL2Uet6G4yZxgVSlTkV+O"
                    "B+Sq8Z7+IQuvf9QQ51OMUh0WqRhsraJfyXppvG20uCXREDastkWWk/AyV+LLu70IwIQSDve4J398USOuYWZkeoEiXVdqTIGU"
                    "0rOtboNepXHe9UK2FmetB5VVD3WPRU69Lg3vM3NPS7CR7gVUsNYQ4lp7briCseS8JZndnVazUEpbNSVisaCHhOMC2zixuP5k"
                    "c9cPGeTuztZLqRXv67qVc+VKfWGyjS4RxnS3rwPXbDTTB2kORIhi4Nvet4436uD1GEOESFOlF3q20tjY7MDSUk0XcCGmOzXM"
                    "gntJkS2FsIE6yWn2Z74nOJ4e6q5UP85e0WtrC9TlyCOkvTanNRrPAGZWl5363QFLmuaQVkpJL3a1Snt+qIbwnu26PkbXtSRS"
                    "NPNkOnIIfpxc8lUTGDXvEYZLXpRwrJRqp4Soaqu+xA64ssNBSFy4z859xf0KMZr1vA4RKz3jF7OGVASHJj7uQ4cJiC3lKfxO"
                    "SKl6xebVOQ8lP2rvZn24PW4qfbXeypPtMau+5EPXKAVJ0WvIUlpFTz+5Eui3IKFXYWI1tj1mjgT64dPP6/2ZMGyjotSmaTdw"
                    "OpWDq5hZ0gU9L9B00J0hbpJrJ+0FMDV8UyPcnKqd52PmbhfNisqNoSYbVSDi/ppyoJt36YASLnD91HWJpWnDVv3eIx8t5WDS"
                    "RlB8zN7t2kc94TfdaOJolXQRtoQaih5woi5E3DGTQw11bJyEkjNfiy3QeG31zDzZXjy/grVhXejq2yd9mkEgkQAurl91cB3Q"
                    "a0sDWoKLGhbchkYt2x3DJtDXx+zd1k6IGvMOwzrmKmv+ux55NF05GHVMqA2/gBSkQla2nmelCFI0uGBUDW58zN6v79d+++bH"
                    "n87TyVfaTu81VrfcB0GXyDnooADpjtk1UUPyCNlRjdeEL6iKTZrcABhKTzZ3vaCRCNYkcyK+r3HoeGpVEQu4zndvjz5BPdLs"
                    "oi4uycOOq9AZ9WRnH7P4OU+forJVnaZ6mQN4y4N+ozQ2Ytd7wQjQzXYMTHTraPeXrJke28D+QRIsrjzd4LnahaU5GkqA+Zo5"
                    "Zls3w/ejW2jgv6MsDvhQt5BKwKZimtVEIokGw8p+g8HX/d2nX2Z7/+acyjnm0hKcMGmFK3gXadpjt0vURhcT0hfUaMqkria9"
                    "t+agnkNojrsXy95l+1zkPYj/DrDhVHOzNGOJKI2jlK07ZIkoBFidP9pH0iGZqLSklFJcuI/f3Gnwix+rqs/qnCePg9zVbEOE"
                    "gRGAvzyHNDtNyDEC2xBIkI1nW5eGlhyPV20+3fbJd7OrITarckM9OIFhjca6kCJtNS5vSZzGJUnNNMox30yXJaNsqFAlH/8G"
                    "g1/8WEgd8TwFMoOVbP9sm7jH15mqSRuipurRyvyLMaRNB1RTY2qtWe/l9wba2xXEuRvJN8GkpOwUOK5qA08s9VJBWFOihtdb"
                    "jZee1R9N4Lppaip0390+budKMf7jm/7m7XX9rar0F9lXI1JgWEtK4/YQn7RzLFGuydfoas2Rug+ws6lllJ/n9eDbn2D1PHz3"
                    "zPqGrktVWzc0hzdrbu2sThccxxwhooYLze6evJrcVR4FDh8SFOdLmKf8zrPbqI9IQwJ24/9vFRosI2lPyUJDzQ07F40qT2CF"
                    "mvE7pNqr+SgaDI+3PsXUuXN5dj0Sb0LpAZty3ur6KWpI0/iFqBlyxzzkXC1El3PqOJWq0iar7Sf9qnIOc0H5fkQ+yRYLq4Qo"
                    "Q6/0EC/FJPUKBk1GbaRmPhzSoub3EerU3MqHf9Vl1TTYHOdqfpA4joF4I+3eymLlusTzqrQliHUaZD0HLGt5EeWlscoE4vqw"
                    "oS/qjeOKQwUt/J5+KG/WfJzezqk2QQiuJlKhWyrvywALN7OmkS+BZOjLI+Yua4FzqR4cHTYYA2pMUIlz76PPAGI5oEekhJab"
                    "LWp7GGEmqaJ2PF+itC0/aOpmaaS6cHeU1sfSzHGnSJaGFo7IzW/DCQkinRwR6gCiRklYWK243v9CedDU7epI8LPqBiH2fQGK"
                    "NFk88JF76d5N9UfR157MJPHJ05OUV/ch9hhV+dQeNPbre+D8uXZR/uZhWr6Mqv5RB1seWaPNwElZ2nEaI8rfhVE18RrPG5wx"
                    "DnApsPfwiKWbdYWHVsM8xMEq+yVUYsNWcVhWwx0/OeqyBmKyNWa11RUllAT5mlKMHo8ZuwxPh4hUddMkyBXglZDeN6lTsgOk"
                    "1yWlGKen4QV44V+Tzsm3bOjqhMi0HrR180JKqkrdZc1My0a3xrNImlLluDsozwDKDN+lGVWxqj0TDNxKmGQ4XdE9YurX9uY6"
                    "5lYNzCAfa0CbCcf4nng82GuwhriyuuYH5ynoEScc19cWOKgqdTLEw5Y+/fLZ3cFXBNC4Sc5eY4kOcRJ/VBG2rMGJqx2qcQs+"
                    "pLZ9OBffqOoheUls6kFLt+szgwAX0MMRBzlCXdrXuxf9n4QFq4YpWt3AKTZLTcJoLqz43VZt2MO2bgoSJA2PrWYnDXpkg7xx"
                    "4tu2S1FMM5UrtHZx+hR0s2Z2EuaHJaDAZ2t8+BhfBfkvQNY2U4oDcWiws1dPVDEq6T8eEzhHOLpd0iVgFcVTVpN0sCqiNM7r"
                    "IYvr/euTXMFF8MADYYhSG/MuVrO6akDChp5qNKHYXEtbgy4GeX8dJeoclVEXC2DcfbeVNysncWEVAQL3nV4poP4SouYnSbcy"
                    "OvhWk7JVUlvUiJqxszVLTWUhai66ByZfFE9qsm0mDAiFNsd6WF2fWdCh7dlIV9nr/nEt3GYRhT28UlWJOWoUUr3H02/WTwK3"
                    "+d2QTek4cBo3kYc0Mbu1/Kuex+T0sJI4XY4a4oBbBgjzmkVzIPLDNs4PxucO/gWYzok9dQ62EG1Q6/UguuFrds3e8WpdolgJ"
                    "1dcOwA9SaJwqk7t4L75+CT/J5bxfGgLxy8fX/3j36f0vZ8W7krqqjLaon3TusupNS6sSJND1STR+JzelGzr3kiyCnU11hYuE"
                    "6cYTzEl+89BiP64KZ9FkU6CeRsDAkIi12RIR2T21YEncpaoeVlpdYLZ8pP8YgKaks4sHtFvm/mmFL365zCG2s8ODbZNSGKlW"
                    "1Syqu9dA+SxFILaxVF1beCuBp5XAcNGqZL67sT+nxisLHJ13/7P9MtbrDz+/+49zGwQoi5yrafGHwF1hG0jvVUWfQIhj+iGQ"
                    "PeqdMYetOVmsW9UTgBrt/S0b7/Y+xlRewfbTz1C/iaS4w8ya59nIucWYhJ8togUeptpbFS/EQ0PZaaaVYfsKnLeZcsvEh/kf"
                    "tyMdecAnKI+axr3m+EDHq2r6o0Sy+77Sp8mkSDIjOAOwvlWRtg8g68xtC//68HH9PC+XCeQWOHZSvoZrRE0N4nzmWaOJ7AsQ"
                    "gv2dQmgSyA0l9Cl5bs2SatOU2xY+/appJjc2QuK6IHFAHRGTryplpL6X+v7jSoretvKJZCD1X/gx3AKg8d/yunO1n8PMcTKv"
                    "53ndUvM5ZheSXKCZwQJvgjNL4rHg/ETmVrMG8J598hKJrEr3ElMHQluo/5o3reg+8d1bDR+6kLjYimexHbnHwCabL7MQXnII"
                    "ydjtxpLYHpGfbMtvhhDYmapq1gCu66YBBZc5Xl8MoTwBVA38GYbgpxnpkn1MaiLteohNbEuB13QpGHhAf/aDc+/0fmWnAWvN"
                    "R4y8/vHzcdd7NUEEaj43mJM8AldKugpe4Peusr+WpI1nBwFIvVGaBFOs/nFA8x8zdSohu3IywBPrcVQpF93bNbPJZ1bVnOBF"
                    "9gI6pPEy0uJRaU2RgFzWKKKwp2l32gJ8/PRuniUYS+U7Vq/OvmZHSRIaqThyTFCKqCbMyu7tFNXS2lhA+AQkqhOnM1t5p4V3"
                    "51eAqXfqY1S2rh+tP7SBSJqgCf7qwfYxanbEdEHcbqlLkx+5lI8BefXOT79ou7QpbBA8H28OVa9UVckIoCYd2qC2MNVgaeJU"
                    "UsWhUwbgXw5dFZKCyv2f/zk2nlW2wMvg6Qnqaxr4HPUYmovXoWyaoZ3TVFPQUkuVNkhcYntps5l4Uc36paXrt4wZetyuLE2C"
                    "1r2jVYULGLPrnRy3YvGCxsks3dIEL12WFHXVSpjzxa8HbHxRGqQXDE3I2qxD7wAvEEQvzaszukrFZ5NCiWMabIZnWRx34fBs"
                    "jA2aSf2ALVHH8a/x9hzGqs9q5oUjcMQrgd8aDujsuB4croSldxgNeZmCU8QCIhGnE7ihMk33gKVf1kcBjpMdkCp2ADSxSehC"
                    "y7hIL0M3jDWAaiD8hlycNMKLyK0IAyWyzWVcezxg55TDztd4rFFjJ5Zoe9gaekCer+DxIQkRMmezanSMfA/bEuFFMxhIkAJu"
                    "FwjzSzunJHA6mUNNGRpfW2o3WRGzwwaJXiOAl6LVU6FeysEE6iEeSRhgrDL5e47q43Yuk42aoqxrw2hkRVt6cSlqxR2T5dIU"
                    "ZOjU3IfSOmj0uNYfC4SthxlNFLxp7aePH38lzrQPn640ys8SKf5Qn16arQ2xsZoZ4UPukOpxtIoNsJ9kRVaVzKjq1Se416rp"
                    "agZ708b10DSR34/vxlmwTC+3wkM26i2O8xck5+01p5f1lFoeATkJvttaNTYe28MBY6Jmv4d6n5VLNQZI3pT6TtQEq612wJCa"
                    "RmxkHDtMjVVn9Yya9kBQwRCZoXdm1WY92OreX/IBav3LvGEKJFyLEaLV18OPOsfV+1RX3JFNm6loRLEdI9oUoVRLotXZaUi9"
                    "r/HWz7kaOPfZ4il79mLVZiOVGo2Ti31K1ccdlSphOCmGrK14AeBMmrIBluSAqd02fBF4rnzs47tf37199+P5hhayZ5vmXzkO"
                    "oo4+YVlCEmbGpusNqxe/NWP0V1p2LWheX5gSsF0XFdkn/nc1P+8yLWv+buXTwA4cc1Z+xF0kATUgNV5uVLskhPX8BuLBQsEd"
                    "WVt4h96h77GgOoT243VhXnYak2f5D84AgVFaqgRQkDIHHrgq9WATnFRXNejaNtUX9+pWgtOyhTet3LHdo0iRm0SV9XRto+Ti"
                    "DLx1VikR1tKm7tFzkJgxngta7pOAQL4szQx3C/SdxobdelRW1JUEAvFW/Wv9kFtRlhmAmd2JO/DvJIGsPZvNi18DFS8cKwcm"
                    "KOtOG5c/IklTl6/TPV+dKNs1pt1Fad5zHK1Tr0mpQYCItVSlDedHUwy3ZND3rb348PHTPF+W5AWInGZolqTatGbpatwnO3Gk"
                    "XWt6nlO7H+RlJjLlVlErkLOuMcPtTT676Otf37bz9YFT63wRi3fywyI1Gpxfc+QNhJR/owfhAA8P6lzVaDCdARKWF52+aeCY"
                    "cjbe/XRNS5aUtIVw1MltIfGS52xWxZVT8lcA66oRVd0fo5vFE6FDamPaBOC47vx4DSy/gNuSgLXqnGNnSdbwptaLJA01ThQu"
                    "1Z3kBgNApu7tAqy8HVN0t6TM67jTxCVhgM96vV3p4DY4ICwQklv1lBQOLKwJo1m3LSZ7afYvjrID20laqQZ3h4FbiCHhfBrZ"
                    "U5ZT0WTi29ajAdLjt05KPZperFHpUb67IAlpGOgCZ0O3Ff/lP//zP/8Pkk0izw=="
                )
            )
        )
        rows = fixture["rows"]
        self.assertEqual(len(rows), 803)
        objects, modules, classes = [], {}, {}
        for row in rows:
            name = row["module"]
            if name not in modules:
                module = types.ModuleType(name)
                module.__file__ = str(self.root / "tests/agentic" / (name + ".py"))
                modules[name] = module
            if row["class"] not in classes:
                classes[row["class"]] = type(
                    row["class"].split(".")[-1], (unittest.TestCase,), {"__module__": name}
                )
            objects.append(classes[row["class"]]())
        suite = unittest.TestSuite([unittest.TestSuite() for _ in range(71)])
        output = self.root / "retained-request"
        with (
            patch.dict(sys.modules, modules),
            patch.object(check_runner, "source", return_value=fixture["source"]),
            patch.object(check_runner, "discover", return_value=(suite, rows, objects, [])),
            patch.object(
                check_runner.subprocess, "Popen", side_effect=OSError("inert launch boundary")
            ) as launch,
        ):
            self.assertEqual(check_runner.run(self.root, output=output), 1)
        launch.assert_called_once()
        request = json.loads((output / "request.json").read_bytes())
        self.assertEqual(request["assignments"], fixture["expected"])
        self.assertEqual(request["assignment_policy"]["estimated_load_ms"], [270141, 270141])
        self.assertEqual([g["reason"] for g in request["assignment_policy"]["groups"]], ["matched"] * 71)
        self.assertEqual(request["rows"], rows)
        self.assertEqual(request["source"], fixture["source"])
        self.assertEqual(request["jobs"], 2)
        self.assertEqual(request["version"], 2)
        self.assertEqual(request["evidence_limit"], 16 * 1024 * 1024)
        self.assertEqual(sorted(i for groups in request["assignments"] for i in groups), list(range(71)))

    def test_weighted_intervals_types_and_duplicate_work(self):
        import check_runner

        suite = unittest.TestSuite([unittest.TestSuite() for _ in range(7)])
        rows = [
            {"position": [i, 0], "module": m}
            for i, m in ((0, "a"), (1, "b"), (2, "a"), (3, "b"), (5, "c"), (6, "d"))
        ]
        self.assertEqual(check_runner.intervals(suite, rows), [[0, 3], [4, 4], [5, 5], [6, 6]])
        self.assertEqual(
            check_runner.assign(suite, rows, 2, [1, 1, 1, 1, 0, 50, 40]), [[5], [0, 1, 2, 3, 4, 6]]
        )
        self.assertEqual(check_runner.assign(suite, rows, 1, [1, 1, 1, 1, 0, 50, 40]), [list(range(7))])
        duplicate = rows + [rows[0].copy()]
        self.assertEqual(
            check_runner.assign(suite, duplicate, 2, [2, 1, 1, 1, 0, 50, 40]), [[5], [0, 1, 2, 3, 4, 6]]
        )
        for value in (True, 1.0, 0, -1, None):
            with self.subTest(value=value), self.assertRaises(check_runner.RunnerError):
                check_runner.assign(suite, rows, 2, [value, 1, 1, 1, 0, 50, 40])
        for weights in ([], [1] * 6, [1] * 8, [1] * 7, {0: 1}):
            with self.assertRaises(check_runner.RunnerError):
                check_runner.assign(suite, rows, 2, weights)

    def test_scheduling_seed_closed_types_and_provenance(self):
        import copy
        from unittest.mock import patch

        import check_runner

        original = copy.deepcopy(check_runner._SCHEDULING_SEED)
        self.assertEqual(len(check_runner.scheduling_seed()), 71)
        self.assertEqual(
            check_runner.digest(original), "1bc1a7df04b831d4104c82432124785875936b76762dcdf27d350380f2da4338"
        )
        mutations = []
        for key, values in {
            "schema_version": [True, 1.0, 2],
            "algorithm": ["other"],
            "source_head": ["0" * 40, True],
            "source_map_sha256": ["0" * 64],
            "provenance": [{}, {**original["provenance"], "extra": "0" * 64}],
            "entries": [original["entries"][:-1], original["entries"] + [original["entries"][0]]],
            "extra": [1],
        }.items():
            for value in values:
                changed = copy.deepcopy(original)
                changed[key] = value
                mutations.append(changed)
        for key in original:
            changed = copy.deepcopy(original)
            del changed[key]
            mutations.append(changed)
        for key, values in {
            "weight_ms": [True, 1.0, 0, -1, 840001, 1],
            "identity_sha256": [True, "x" * 64, original["entries"][1]["identity_sha256"]],
            "extra": [1],
        }.items():
            for value in values:
                changed = copy.deepcopy(original)
                changed["entries"][0][key] = value
                mutations.append(changed)
        for changed in mutations:
            with patch.object(check_runner, "_SCHEDULING_SEED", changed):
                with self.assertRaises(check_runner.RunnerError):
                    check_runner.scheduling_seed()
        with self.assertRaises(check_runner.RunnerError):
            check_runner.strict(b'{"schema_version":1,"schema_version":1}')

    def test_fingerprints_bind_complete_rows_and_actual_defining_origins(self):
        import copy
        import types
        from unittest.mock import patch

        import check_runner

        module = types.ModuleType("inert_origin")
        module.__file__ = str(self.root / "tests/agentic/inert.py")
        case = type("Imported", (unittest.TestCase,), {"__module__": module.__name__})
        obj = case()
        row = {
            "position": [0, 2, 3],
            "id": "alias.method",
            "module": module.__name__,
            "class": module.__name__ + ".Imported",
        }
        identity = {"files": {"tests/agentic/inert.py": "a" * 64}}
        suite = unittest.TestSuite([unittest.TestSuite()])

        def policy(rows, objects=None, source=None, selected_suite=None):
            return check_runner.assignment_policy(
                self.root, selected_suite or suite, rows, objects or [obj] * len(rows), source or identity, 2
            )[0]

        with patch.dict(sys.modules, {module.__name__: module}):
            initial = policy([row])
            self.assertEqual(initial["groups"][0]["reason"], "unmatched")
            self.assertEqual(initial["groups"][0]["weight_ms"], 1000)
            fingerprint = initial["groups"][0]["fingerprint"]
            for key, value in (("position", [0, 2, 4]), ("id", "different"), ("class", "different")):
                changed = {**row, key: value}
                self.assertNotEqual(policy([changed])["groups"][0]["fingerprint"], fingerprint)
            changed_source = copy.deepcopy(identity)
            changed_source["files"]["tests/agentic/inert.py"] = "b" * 64
            self.assertNotEqual(policy([row], source=changed_source)["groups"][0]["fingerprint"], fingerprint)
            duplicate = policy([row, row.copy()])
            self.assertEqual(duplicate["groups"][0]["weight_ms"], 2000)
            self.assertNotEqual(duplicate["groups"][0]["fingerprint"], fingerprint)
            another = {**row, "id": "second", "position": [0, 2, 4]}
            self.assertNotEqual(policy([row, another]), policy([another, row]))
            expanded = unittest.TestSuite([unittest.TestSuite(), unittest.TestSuite()])
            shifted = policy([{**row, "position": [1, 2, 3]}], selected_suite=expanded)
            self.assertEqual(shifted["groups"][1]["fingerprint"], fingerprint)
            self.assertEqual(shifted["groups"][0]["weight_ms"], 0)
            self.assertEqual(shifted["groups"][0]["reason"], "empty")
            for origin in (None, "relative.py", "/outside.py", str(self.root / "missing.py")):
                module.__file__ = origin
                miss = policy([row])["groups"][0]
                self.assertEqual(
                    miss, {"fingerprint": None, "reason": "unsupported-origin", "weight_ms": 1000}
                )
            with self.assertRaises(check_runner.RunnerError):
                policy([{**row, "module": "forged"}])

    def test_workers_recompute_entire_weighted_descriptor_before_execution(self):
        import copy

        import check_runner

        self.module(
            "test_a.py",
            "import unittest\nfrom pathlib import Path\nclass A(unittest.TestCase):\n def test_ok(self): Path('sentinel').write_text('ran')\n",
        )
        self.module("test_b.py", "import unittest\nclass B(unittest.TestCase):\n def test_ok(self): pass\n")
        result = self.command()
        self.assertEqual(result.returncode, 0, result.stdout.decode())
        directory, summary = self.evidence(result)
        original = json.loads((directory / "request.json").read_bytes())
        policy = original["assignment_policy"]
        self.assertEqual(policy, summary["assignment_policy"])
        self.assertEqual(policy["estimated_load_ms"], [1000, 1000])
        self.assertEqual([g["reason"] for g in policy["groups"]], ["unmatched", "unmatched"])
        self.assertEqual(len(summary["workers"]), 2)
        self.assertTrue(all(w["successful"] for w in summary["workers"]))
        mutations = []
        for key, value in (
            ("schema_version", True),
            ("schema_version", 1.0),
            ("schema_version", 2),
            ("algorithm", "other"),
            ("seed_digest", "0" * 64),
            ("provenance", {}),
            ("groups", []),
            ("intervals", []),
            ("estimated_load_ms", [1, 1]),
            ("extra", 1),
        ):
            changed = copy.deepcopy(original)
            changed["assignment_policy"][key] = value
            mutations.append(changed)
        for key in policy:
            changed = copy.deepcopy(original)
            del changed["assignment_policy"][key]
            mutations.append(changed)
        for key, value in (("fingerprint", "0" * 64), ("reason", "matched"), ("weight_ms", True)):
            changed = copy.deepcopy(original)
            changed["assignment_policy"]["groups"][0][key] = value
            mutations.append(changed)
        for value in ([[], [0, 1]], [[0, 1], [0]], [[0], []], [[1], [0]]):
            changed = copy.deepcopy(original)
            changed["assignments"] = value
            mutations.append(changed)
        (self.root / "sentinel").unlink()
        for index, changed in enumerate(mutations):
            request = directory / f"policy-{index}.json"
            request.write_bytes(check_runner.canonical(changed))
            evidence = directory / f"policy-{index}.jsonl"
            result = subprocess.run(
                [
                    sys.executable,
                    "-B",
                    str(self.root / "scripts/agentic/check_runner.py"),
                    "--worker",
                    "0",
                    str(self.root),
                    str(request),
                    str(evidence),
                ],
                cwd=self.root,
                capture_output=True,
                timeout=10,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(evidence.exists())
            self.assertFalse((self.root / "sentinel").exists())
        # A real worker with a different literal seed refuses before opening its journal.
        runner = self.root / "scripts/agentic/check_runner.py"
        runner.write_text(runner.read_text().replace("fixture-group-lpt-ms-v1", "fixture-group-lpt-ms-v9"))
        result = subprocess.run(
            [
                sys.executable,
                "-B",
                str(runner),
                "--worker",
                "0",
                str(self.root),
                str(directory / "request.json"),
                str(directory / "changed-seed.jsonl"),
            ],
            cwd=self.root,
            capture_output=True,
            timeout=10,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((directory / "changed-seed.jsonl").exists())
        self.assertFalse((self.root / "sentinel").exists())

    def test_weighted_route_preserves_fixture_traces_and_refusals(self):
        import copy

        import check_runner

        self.repeated_fixture("module")
        trace = self.root / "trace"
        reference = self.command(1)
        self.assertEqual(reference.returncode, 0, reference.stdout.decode())
        expected = trace.read_bytes()
        trace.unlink()
        result = self.command(2)
        self.assertEqual(result.returncode, 0, result.stdout.decode())
        self.assertEqual(trace.read_bytes(), expected)
        request, path, records, summary = self.records(result)
        self.assertEqual(request["assignment_policy"]["algorithm"], "fixture-group-lpt-ms-v1")
        self.assertTrue(check_runner.reconcile(request, 0, path, 0)["successful"])
        rows = summary["workers"][0]["occurrences"]
        removed = [r for r in records if r.get("position") != rows[-1]["position"]]
        removed[-1] = {**removed[-1], "tests_run": 1}
        self.refused_records(request, path, removed)
        for interval in ([0, 3], [2, 3]):
            changed = copy.deepcopy(records)
            changed[1]["interval"] = interval
            self.refused_records(request, path, changed)
        self.refused_records(request, path, records[:1] + records[2:4] + records[1:2] + records[4:])

    def test_weighted_deadline_retains_incomplete_work(self):
        self.module(
            "test_a.py",
            "import unittest,time\nclass A(unittest.TestCase):\n def test_wait(self): time.sleep(5)\n",
        )
        self.module("test_b.py", "import unittest\nclass B(unittest.TestCase):\n def test_ok(self): pass\n")
        result = self.helper(seconds=0.3)
        self.assertEqual(result.returncode, 1)
        _, summary = self.evidence(result)
        self.assertIn("deadline", summary["error"].lower())
        self.assertFalse(summary["successful"])
        self.assertEqual(summary["assignment_policy"]["estimated_load_ms"], [1000, 1000])
        self.assertEqual(len(summary["process_exits"]), 2)
        self.assertTrue(all(code is not None for code in summary["process_exits"]))
