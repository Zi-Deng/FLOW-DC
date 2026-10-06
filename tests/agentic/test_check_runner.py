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
