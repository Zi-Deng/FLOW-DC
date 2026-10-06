"""Bounded process-isolated unittest execution with complete occurrence accounting."""

import collections
import hashlib
import json
import math
import os
import selectors
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

SECONDS = 840
TEXT_BYTES = 32 * 1024 * 1024
EVIDENCE_BYTES = 16 * 1024 * 1024
VERSION = 2


class RunnerError(Exception):
    """An incomplete or inconsistent gate, never a successful fallback."""


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def strict(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise RunnerError("Duplicate protocol key")
            result[key] = value
        return result

    return json.loads(raw, object_pairs_hook=pairs, parse_constant=lambda _: _invalid())


def _invalid():
    raise RunnerError("Invalid protocol constant")


def source(root):
    files = {}
    for name in (
        "scripts/agentic",
        "tests/agentic",
        ".agentic",
        ".agents/skills",
        ".github",
        "docs/agent-workflow",
    ):
        directory = root / name
        for path in sorted(directory.rglob("*")):
            if any(p in {"__pycache__", ".ruff_cache"} for p in path.parts):
                continue
            if path.is_symlink():
                raise RunnerError("Source symlink")
            if path.is_file():
                files[str(path.relative_to(root))] = hashlib.sha256(path.read_bytes()).hexdigest()
    for name in ("AGENTS.md", "Makefile", "scripts/check_repository.py"):
        path = root / name
        if path.exists():
            files[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    # An installed disposable tree need not be a Git repository.
    checkout = None
    if (root / ".git").exists():
        checkout = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=root, timeout=10, text=True
        ).strip()
        tracked = subprocess.check_output(["git", "ls-files", "-z"], cwd=root, timeout=10)
        for raw in tracked.split(b"\0"):
            if not raw:
                continue
            name = os.fsdecode(raw)
            path = root / name
            if path.is_symlink() or not path.is_file():
                raise RunnerError("Missing or symlinked tracked source")
            files[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    return {"checkout": checkout, "files": files}


def discover(root):
    sys.path.insert(0, str(root / "scripts/agentic"))
    loader = unittest.TestLoader()
    suite = loader.discover(str(root / "tests/agentic"))
    rows, objects = [], []

    def visit(node, position):
        if type(node) is unittest.TestSuite:
            for index, child in enumerate(node):
                visit(child, position + [index])
        elif (
            isinstance(node, unittest.TestCase)
            and type(node).run is unittest.TestCase.run
            and type(node).__call__ is unittest.TestCase.__call__
        ):
            rows.append(
                {
                    "position": position,
                    "id": node.id(),
                    "module": type(node).__module__,
                    "class": type(node).__module__ + "." + type(node).__qualname__,
                }
            )
            objects.append(node)
        else:
            raise RunnerError("Unsupported custom suite or test runner")

    visit(suite, [])
    if not rows:
        raise RunnerError("Empty test discovery")
    return suite, rows, objects, loader.errors


def assign(suite, rows, jobs):
    if type(jobs) is not int or jobs not in (1, 2):
        raise RunnerError("Worker count must be 1 or 2")
    spans = {}
    for row in rows:
        group = row["position"][0]
        a, b = spans.get(row["module"], (group, group))
        spans[row["module"]] = min(a, group), max(b, group)
    intervals = []
    for a, b in sorted([*spans.values(), *((i, i) for i, _ in enumerate(suite))]):
        if intervals and a <= intervals[-1][1]:
            intervals[-1][1] = max(b, intervals[-1][1])
        else:
            intervals.append([a, b])
    counts = collections.Counter(r["position"][0] for r in rows)
    loads, selected = [0] * jobs, [[] for _ in range(jobs)]
    for a, b in sorted(intervals, key=lambda p: (-sum(counts[i] for i in range(p[0], p[1] + 1)), p[0])):
        worker = min(range(jobs), key=lambda i: (loads[i], i))
        selected[worker].extend(range(a, b + 1))
        loads[worker] += sum(counts[i] for i in range(a, b + 1))
    return [sorted(groups) for groups in selected]


class Journal:
    def __init__(self, path, limit):
        self.stream = path.open("xb")
        self.limit = limit
        self.size = 0

    def emit(self, value):
        raw = canonical(value) + b"\n"
        if self.size + len(raw) > self.limit:
            raise RunnerError("Structured evidence overflow")
        self.stream.write(raw)
        self.stream.flush()
        self.size += len(raw)


class FixtureCursor:
    """Correlate callbacks with contiguous runs in this worker's occurrence order."""

    def __init__(self, rows):
        self.rows = rows
        self.cursor = 0
        self.active = None
        self.sequence = 0
        self.last_fixture = None
        self.blocked = {}
        self.failed_setups = set()
        self.runs = {}
        for field, suffix in (("module", "Module"), ("class", "Class")):
            start = 0
            while start < len(rows):
                end = start + 1
                while end < len(rows) and rows[end][field] == rows[start][field]:
                    end += 1
                for prefix in ("setUp", "tearDown"):
                    name = f"{prefix}{suffix} ({rows[start][field]})"
                    self.runs.setdefault(name, []).append((start, end))
                start = end

    def start(self, position):
        if (
            self.active is not None
            or self.cursor >= len(self.rows)
            or position != self.rows[self.cursor]["position"]
        ):
            raise RunnerError("Occurrence skips an unaccounted lifecycle")
        self.active = position
        self.last_fixture = None

    def stop(self, position):
        if position != self.active:
            raise RunnerError("Occurrence stop differs")
        self.active = None
        self.cursor += 1
        self.last_fixture = None

    def fixture(self, name, kind, interval=None, sequence=None):
        if (
            self.active is not None
            or type(kind) is not str
            or kind not in {"skip", "error"}
            or name not in self.runs
        ):
            raise RunnerError("Invalid fixture boundary")
        setup = name.startswith("setUp")
        # Multiple cleanup exceptions belong to the same actual fixture attempt.
        if self.last_fixture and self.last_fixture[0] == name:
            span = self.last_fixture[1]
        else:
            matches = [p for p in self.runs[name] if p[0 if setup else 1] == self.cursor]
            if len(matches) != 1:
                raise RunnerError("Fixture is outside its occurrence interval")
            span = matches[0]
        start, end = span
        if interval is not None and (
            type(interval) is not list
            or len(interval) != 2
            or any(type(i) is not int for i in interval)
            or tuple(interval) != span
        ):
            raise RunnerError("Fixture interval differs")
        if sequence is not None and (type(sequence) is not int or sequence != self.sequence):
            raise RunnerError("Fixture callback sequence differs")
        if setup:
            for i in range(start, end):
                prior = self.blocked.get(i)
                self.blocked[i] = "error" if kind == "error" or prior == "error" else "skip"
            self.failed_setups.add((name, span))
            self.cursor = end
        else:
            setup_name = name.replace("tearDown", "setUp", 1)
            if (setup_name, span) in self.failed_setups or any(
                n.startswith("setUpModule") and a <= start and end <= b for n, (a, b) in self.failed_setups
            ):
                raise RunnerError("Teardown follows failed setup")
        value = {"interval": [start, end], "sequence": self.sequence}
        self.sequence += 1
        self.last_fixture = (name, span)
        return value


class Result(unittest.TextTestResult):
    def __init__(self, *args, journal, rows, objects, **kwargs):
        super().__init__(*args, **kwargs)
        self.journal = journal
        self.pending = collections.defaultdict(collections.deque)
        for row, obj in zip(rows, objects, strict=True):
            self.pending[id(obj)].append(row["position"])
        self.active = {}
        self.objects = objects
        self.fixtures = FixtureCursor(rows)

    def startTest(self, test):
        position = self.pending[id(test)].popleft()
        self.fixtures.start(position)
        self.active[id(test)] = position
        self.journal.emit({"event": "start", "position": position, "monotonic_ns": time.monotonic_ns()})
        super().startTest(test)

    def stopTest(self, test):
        self.fixtures.stop(self.active[id(test)])
        self.journal.emit(
            {"event": "stop", "position": self.active.pop(id(test)), "monotonic_ns": time.monotonic_ns()}
        )
        super().stopTest(test)

    def outcome(self, test, kind, detail=""):
        position = self.active.get(id(getattr(test, "test_case", test)))
        if position is None:
            before = self.fixtures.cursor
            correlation = self.fixtures.fixture(test.id(), kind)
            for i in range(before, self.fixtures.cursor):
                position = self.pending[id(self.objects[i])].popleft()
                if position != self.fixtures.rows[i]["position"]:
                    raise RunnerError("Blocked occurrence identity differs")
            self.journal.emit(
                {"event": "fixture", "id": test.id(), "kind": kind, "detail": detail, **correlation}
            )
        else:
            self.journal.emit({"event": "outcome", "position": position, "kind": kind, "detail": detail})

    def addSuccess(self, test):
        self.outcome(test, "success")
        super().addSuccess(test)

    def addError(self, test, err):
        self.outcome(test, "error", self._exc_info_to_string(err, test))
        super().addError(test, err)

    def addFailure(self, test, err):
        self.outcome(test, "failure", self._exc_info_to_string(err, test))
        super().addFailure(test, err)

    def addSkip(self, test, reason):
        self.outcome(test, "skip", reason)
        super().addSkip(test, reason)

    def addExpectedFailure(self, test, err):
        self.outcome(test, "expected_failure", self._exc_info_to_string(err, test))
        super().addExpectedFailure(test, err)

    def addUnexpectedSuccess(self, test):
        self.outcome(test, "unexpected_success")
        super().addUnexpectedSuccess(test)

    def addSubTest(self, test, subtest, err):
        kind = "subtest_success" if err is None else "subtest_failure"
        self.outcome(test, kind, str(subtest) if err is None else self._exc_info_to_string(err, test))
        super().addSubTest(test, subtest, err)


def worker(root, request_path, index, evidence):
    request = strict(request_path.read_bytes())
    suite, rows, objects, errors = discover(root)
    assignments = assign(suite, rows, request["jobs"])
    if canonical(request) != canonical(
        {
            "version": VERSION,
            "jobs": request["jobs"],
            "source": source(root),
            "rows": rows,
            "assignments": assignments,
            "errors": errors,
            "evidence_limit": request["evidence_limit"],
        }
    ):
        raise RunnerError("Worker source or discovery differs")
    if (
        type(request["evidence_limit"]) is not int
        or not 0 < request["evidence_limit"] <= EVIDENCE_BYTES
        or not 0 <= index < request["jobs"]
    ):
        raise RunnerError("Invalid worker bounds")
    groups = assignments[index]
    chosen = [(r, o) for r, o in zip(rows, objects, strict=True) if r["position"][0] in groups]
    journal = Journal(evidence, request["evidence_limit"])
    try:
        journal.emit({"event": "header", "version": VERSION, "worker": index, "request": digest(request)})
        result = unittest.TextTestRunner(
            verbosity=2,
            resultclass=lambda *args, **kw: Result(
                *args,
                **kw,
                journal=journal,
                rows=[x[0] for x in chosen],
                objects=[x[1] for x in chosen],
            ),
        ).run(unittest.TestSuite([child for i, child in enumerate(suite) if i in groups]))
        if source(root) != request["source"]:
            raise RunnerError("Worker source changed")
        journal.emit({"event": "end", "tests_run": result.testsRun, "successful": result.wasSuccessful()})
        return 0 if result.wasSuccessful() and not errors else 1
    finally:
        journal.stream.close()


def reconcile(request, index, path, exit_status):
    if type(request.get("version")) is not int or request["version"] != VERSION:
        raise RunnerError("Unsupported result protocol version")
    if path.stat().st_size > request["evidence_limit"]:
        raise RunnerError("Structured evidence overflow")
    records = [strict(line) for line in path.read_bytes().splitlines()]
    header = {"event": "header", "version": VERSION, "worker": index, "request": digest(request)}
    if not records or canonical(records[0]) != canonical(header):
        raise RunnerError("Worker header differs")
    end = records[-1]
    if (
        set(end) != {"event", "tests_run", "successful"}
        or end["event"] != "end"
        or type(end["tests_run"]) is not int
        or type(end["successful"]) is not bool
    ):
        raise RunnerError("Incomplete worker trailer")
    expected = {
        tuple(r["position"]): r for r in request["rows"] if r["position"][0] in request["assignments"][index]
    }
    started, stopped, outcomes, fixtures = set(), set(), collections.defaultdict(list), []
    active = None
    order = []
    timings = {}
    last_time = 0
    lifecycle = FixtureCursor(list(expected.values()))
    allowed = {
        "success",
        "failure",
        "error",
        "skip",
        "expected_failure",
        "unexpected_success",
        "subtest_success",
        "subtest_failure",
    }
    for record in records[1:-1]:
        event = record.get("event")
        if event == "fixture":
            if set(record) != {"event", "id", "kind", "detail", "interval", "sequence"}:
                raise RunnerError("Invalid fixture result")
            if type(record["id"]) is not str or type(record["detail"]) is not str:
                raise RunnerError("Invalid fixture identity")
            # Null must not select the capture-only omitted-argument path.
            if record["interval"] is None or record["sequence"] is None:
                raise RunnerError("Missing fixture correlation")
            lifecycle.fixture(record["id"], record["kind"], record["interval"], record["sequence"])
            fixtures.append(record)
            continue
        required = {"event", "position"} | ({"kind", "detail"} if event == "outcome" else {"monotonic_ns"})
        if set(record) != required or event not in {"start", "stop", "outcome"}:
            raise RunnerError("Unknown result event")
        position = record["position"]
        if type(position) is not list or any(type(i) is not int for i in position):
            raise RunnerError("Invalid occurrence position")
        key = tuple(position)
        if key not in expected:
            raise RunnerError("Foreign occurrence")
        if event in {"start", "stop"}:
            stamp = record["monotonic_ns"]
            if type(stamp) is not int or stamp < last_time:
                raise RunnerError("Invalid lifecycle clock")
            last_time = stamp
            timings.setdefault(key, {})[event] = stamp
        if event == "start":
            lifecycle.start(position)
            if key in started or active is not None:
                raise RunnerError("Duplicate or overlapping start")
            started.add(key)
            order.append(key)
            active = key
        elif event == "stop":
            if key != active or key in stopped or not outcomes[key]:
                raise RunnerError("Incomplete or duplicate stop")
            lifecycle.stop(position)
            stopped.add(key)
            active = None
        else:
            if key != active or record["kind"] not in allowed or type(record["detail"]) is not str:
                raise RunnerError("Invalid occurrence outcome")
            if outcomes[key] and any(
                x["kind"] not in {"subtest_success", "subtest_failure", "skip"} for x in outcomes[key]
            ):
                # unittest may report a method failure followed by a teardown error.
                if record["kind"] != "error":
                    raise RunnerError("Duplicate terminal outcome")
            outcomes[key].append({"kind": record["kind"], "detail": record["detail"]})
    if active is not None or started != stopped or end["tests_run"] != len(started):
        raise RunnerError("Incomplete test lifecycle")
    if order != [key for key in expected if key in started]:
        raise RunnerError("Occurrence order differs")
    for values in outcomes.values():
        if not any(o["kind"] != "subtest_success" for o in values):
            raise RunnerError("Missing terminal outcome")
    missing = set(expected) - stopped
    skipped = {
        tuple(lifecycle.rows[i]["position"]) for i, kind in lifecycle.blocked.items() if kind == "skip"
    }
    bad = any(
        o["kind"] in {"failure", "error", "unexpected_success", "subtest_failure"}
        for values in outcomes.values()
        for o in values
    ) or any(f["kind"] == "error" for f in fixtures)
    successful = not bad and not (missing - skipped)
    if end["successful"] != (not bad) or exit_status != (
        0 if end["successful"] and not request["errors"] else 1
    ):
        raise RunnerError("Worker exit or success disagrees")
    return {
        "worker": index,
        "exit_status": exit_status,
        "successful": successful and exit_status == 0,
        "occurrences": [
            {
                **row,
                "outcomes": outcomes.get(key, []),
                "timing_ns": timings.get(key, {}),
                "state": "completed"
                if key in stopped
                else "fixture_skip"
                if key in skipped
                else "incomplete",
            }
            for key, row in expected.items()
        ],
        "fixtures": fixtures,
    }


def terminate(processes):
    for process in processes:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    until = time.monotonic() + 0.2
    for process in processes:
        try:
            process.wait(timeout=max(0.001, until - time.monotonic()))
        except subprocess.TimeoutExpired:
            pass
    for process in processes:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()


def run(root, jobs=2, *, output=None, seconds=SECONDS, text_limit=TEXT_BYTES, evidence_limit=EVIDENCE_BYTES):
    if type(jobs) is not int or jobs not in (1, 2):
        raise RunnerError("Worker count must be 1 or 2")
    if (
        type(seconds) not in (int, float)
        or not math.isfinite(seconds)
        or not 0 < seconds <= SECONDS
        or type(text_limit) is not int
        or not 0 < text_limit <= TEXT_BYTES
        or type(evidence_limit) is not int
        or not 0 < evidence_limit <= EVIDENCE_BYTES
    ):
        raise RunnerError("Invalid execution bounds")
    if not all(hasattr(signal, name) for name in ("setitimer", "pthread_sigmask")) or not hasattr(
        os, "killpg"
    ):
        raise RunnerError("Process-group supervision unavailable")
    output = Path(output) if output else Path(tempfile.mkdtemp(prefix="agentic-check-"))
    output.mkdir(parents=True, exist_ok=True)
    print(f"Workflow evidence: {output}", flush=True)
    started = time.monotonic()
    import resource

    usage_before = resource.getrusage(resource.RUSAGE_CHILDREN)
    processes, logs, sizes = [], [], [0] * jobs
    selector = selectors.DefaultSelector()
    summary = {"version": VERSION, "jobs": jobs, "successful": False, "workers": [], "error": None}
    old_handlers = {s: signal.getsignal(s) for s in (signal.SIGALRM, signal.SIGTERM, signal.SIGINT)}

    def interrupted(signum, frame):
        raise RunnerError("Runner deadline or interruption")

    try:
        for sig in old_handlers:
            signal.signal(sig, interrupted)
        signal.setitimer(signal.ITIMER_REAL, seconds)
        identity = source(root)
        suite, rows, _, errors = discover(root)
        if source(root) != identity:
            raise RunnerError("Source changed during discovery")
        request = {
            "version": VERSION,
            "jobs": jobs,
            "source": identity,
            "rows": rows,
            "assignments": assign(suite, rows, jobs),
            "errors": errors,
            "evidence_limit": evidence_limit,
        }
        request_path = output / "request.json"
        request_path.write_bytes(canonical(request))
        summary["request_digest"] = digest(request)
        for index in range(jobs):
            temporary = output / f"tmp-{index}"
            temporary.mkdir()
            log = (output / f"worker-{index}.log").open("xb")
            logs.append(log)
            # Defer cancellation until the new group is registered for cleanup.
            mask = signal.pthread_sigmask(signal.SIG_BLOCK, set(old_handlers))
            try:
                process = subprocess.Popen(
                    [
                        sys.executable,
                        "-B",
                        str(Path(__file__).resolve()),
                        "--worker",
                        str(index),
                        str(root),
                        str(request_path),
                        str(output / f"worker-{index}.jsonl"),
                    ],
                    cwd=root,
                    env={**os.environ, "TMPDIR": str(temporary)},
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
                processes.append(process)
                selector.register(process.stdout, selectors.EVENT_READ, index)
            finally:
                signal.pthread_sigmask(signal.SIG_SETMASK, mask)
        while selector.get_map() or any(p.poll() is None for p in processes):
            if time.monotonic() - started >= seconds:
                raise RunnerError("Runner deadline")
            for key, _ in selector.select(0.05):
                index = key.data
                raw = os.read(key.fileobj.fileno(), 65536)
                if not raw:
                    selector.unregister(key.fileobj)
                    key.fileobj.close()
                    continue
                room = max(0, text_limit - sizes[index])
                logs[index].write(raw[:room])
                sizes[index] += len(raw)
                if sizes[index] > text_limit:
                    raise RunnerError("Text output overflow; retained prefix is incomplete")
            for index in range(len(processes)):
                path = output / f"worker-{index}.jsonl"
                if path.exists() and path.stat().st_size > evidence_limit:
                    raise RunnerError("Structured evidence overflow")
        for index, process in enumerate(processes):
            summary["workers"].append(
                reconcile(request, index, output / f"worker-{index}.jsonl", process.wait())
            )
        if source(root) != identity:
            raise RunnerError("Source changed during execution")
        summary["successful"] = all(w["successful"] for w in summary["workers"]) and not errors
        summary["occurrences"] = len(rows)
    except (RunnerError, OSError, ValueError, TypeError, KeyError, IndexError) as exc:
        summary["error"] = str(exc)
    finally:
        # Always reap groups, including children that outlive an exited worker.
        signal.setitimer(signal.ITIMER_REAL, 0)
        for sig in old_handlers:
            signal.signal(sig, signal.SIG_IGN)
        terminate(processes)
        selector.close()
        for log in logs:
            log.close()
        for sig, handler in old_handlers.items():
            signal.signal(sig, handler)
        usage = resource.getrusage(resource.RUSAGE_CHILDREN)
        summary["child_user_seconds"] = usage.ru_utime - usage_before.ru_utime
        summary["child_system_seconds"] = usage.ru_stime - usage_before.ru_stime
        summary["child_peak_rss_native_units"] = usage.ru_maxrss
        summary["elapsed_seconds"] = time.monotonic() - started
        summary["process_exits"] = [p.returncode for p in processes]
        (output / "summary.json").write_bytes(canonical(summary))
        for index in range(len(logs)):
            print(f"--- workflow worker {index} ---", flush=True)
            with (output / f"worker-{index}.log").open("rb") as stream:
                while raw := stream.read(65536):
                    sys.stdout.buffer.write(raw)
            sys.stdout.flush()
        print(
            f"Workflow aggregate: jobs={jobs} occurrences={summary.get('occurrences', 'incomplete')} successful={summary['successful']}",
            flush=True,
        )
        if summary["error"]:
            print(summary["error"], file=sys.stderr)
    return 0 if summary["successful"] else 1


if __name__ == "__main__":
    if len(sys.argv) != 6 or sys.argv[1] != "--worker" or sys.argv[2] not in ("0", "1"):
        raise SystemExit("Internal worker arguments required")
    signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGALRM, signal.SIGTERM, signal.SIGINT})
    raise SystemExit(worker(Path(sys.argv[3]), Path(sys.argv[4]), int(sys.argv[2]), Path(sys.argv[5])))
