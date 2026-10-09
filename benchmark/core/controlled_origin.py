"""Auditable bounded concurrent origin; independent service and byte truth."""

import ipaddress
import threading
import time
from collections import Counter, deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .truth import LEGACY, digest, encode, require, workload

SCENARIOS = ("steady", "drop-recovery", "mixed-sizes", "balanced", "skewed", "overload", "sparse-interrupted")
RESEARCH_SCENARIOS = SCENARIOS + ("transient-overload", "sustained-overload", "oscillation", "sparse", "baseline-drift", "recovery")


class ServiceModel:
    """FIFO bounded queue, nonpreemptive capacity steps, on an origin-only clock."""

    def __init__(self, schedule, queue_bound, emit, *, limits=LEGACY):
        self.limits = limits
        require(type(queue_bound) is int and 0 <= queue_bound <= 32, "queue bound must be 0..32")
        require(isinstance(schedule, list) and 1 <= len(schedule) <= 16, "invalid service schedule")
        previous = -1
        for at, slots in schedule:
            require(type(at) in (int, float) and 0 <= at <= limits.acquisition_seconds and at > previous, "invalid step time")
            require(type(slots) is int and 1 <= slots <= 16, "service slots must be 1..16")
            previous = at
        require(schedule[0][0] == 0, "service schedule must start at zero")
        self.schedule, self.queue_bound, self.emit = schedule, queue_bound, emit
        self.position, self.now, self.slots = 0, 0, schedule[0][1]
        self.queue, self.active, self.states = deque(), set(), {}
        self.stopped = False

    def event(self, phase, **fields):
        self.emit(
            {
                "phase": phase,
                "origin_elapsed_s": self.now,
                "active": len(self.active),
                "queued": len(self.queue),
                "slots": self.slots,
                **fields,
            }
        )

    def advance(self, now):
        require(type(now) in (int, float) and self.now <= now <= 300, "invalid origin clock")
        self.now = now
        while self.position < len(self.schedule) and self.schedule[self.position][0] <= now:
            planned, self.slots = self.schedule[self.position]
            self.position += 1
            self.event("capacity", planned_elapsed_s=planned)
        self.dispatch()

    def dispatch(self):
        while not self.stopped and self.queue and len(self.active) < self.slots:
            request = self.queue.popleft()
            self.active.add(request)
            self.states[request] = "service"
            self.event("service_start", request_id=request)

    def arrive(self, now, request, path):
        self.advance(now)
        require(request not in self.states, "duplicate origin request identity")
        self.event("arrival", request_id=request, path=path)
        if self.stopped or (len(self.active) >= self.slots and len(self.queue) >= self.queue_bound):
            self.states[request] = "rejected"
            self.event("admission", request_id=request, accepted=False)
        else:
            self.states[request] = "queued"
            self.queue.append(request)
            self.event("admission", request_id=request, accepted=True)
            self.dispatch()
        return self.states[request]

    def finish(self, now, request, **fields):
        self.advance(now)
        require(request in self.active, "finish requires an active service request")
        self.active.remove(request)
        self.states[request] = "finished"
        self.event("service_end", request_id=request, **fields)
        self.dispatch()

    def stop(self, now):
        self.stopped = True
        self.advance(now)
        while self.queue:
            request = self.queue.popleft()
            self.states[request] = "cancelled"
            self.event("queue_cancel", request_id=request)


def scenario(name, payloads, *, rows=128, research_workload=None):
    """Generate immutable policies before any request, without consulting clients."""
    limits = workload(research_workload)
    require(name in (SCENARIOS if research_workload is None else RESEARCH_SCENARIOS), "unknown controlled scenario")
    require(type(rows) is int and 1 <= rows <= limits.max_rows, "scenario rows exceed finite workload")
    require(set(payloads) >= {"JPEG", "PNG"}, "JPEG/PNG originals required")
    kinds = sorted(payloads) if name == "mixed-sizes" else ["JPEG", "PNG"]
    objects = {
        f"/objects/{i}/same.jpg": {
            "payload": payloads[kind],
            "service_s": 0.03,
            "responses": [{"status": 200}],
        }
        for i, kind in enumerate(kinds)
    }
    if name == "mixed-sizes":
        for spec in objects.values():
            spec["service_s"] = 0.02 + min(0.2, len(spec["payload"]) / 1_000_000)
    if name == "overload":
        for i, spec in enumerate(objects.values()):
            spec["responses"] = [{"status": 429 if i % 2 else 503, "retry_after": "0.1"}, {"status": 200}]
    if name == "sparse-interrupted":
        rows = min(rows, 8)
        next(iter(objects.values()))["responses"] = [{"status": 200, "truncate": True}]
        for spec in objects.values():
            spec["service_s"] = 0.5
    paths = list(objects)
    assignments = []
    for i in range(rows):
        origin = (i % 2 if name == "balanced" else int(i % 10 == 0)) if name in ("balanced", "skewed") else 0
        assignments.append({"origin": origin, "path": paths[i % len(paths)]})
    require(
        sum(len(objects[row["path"]]["payload"]) for row in assignments) <= limits.max_payload_bytes,
        "scenario exceeds finite expected row payload",
    )
    result = {
        "schema": "flowdc-origin-scenario-v1",
        "name": name,
        "objects": objects,
        "assignments": assignments,
        "origins": 2 if name in ("balanced", "skewed") else 1,
        "schedule": [[0, 4], [0.6, 1], [1.2, 4]] if name == "drop-recovery" else [[0, 4]],
        "queue_bound": 2 if name == "overload" else 16,
        "queue_rejection_status": 503,
        "queue_retry_after": "0.1",
        "clock_anchor": "first request arrival, separately at each origin",
    }
    if research_workload is not None:
        result.update(schema="flowdc-origin-scenario-v2", workload=limits.record())
        # Fixed stimulus definitions, frozen before controller outcomes. Small
        # engineering runs may end early; qualification must check realization.
        result["schedule"] = ([[0, 4], [15, 1], [35, 4]] if name == "drop-recovery"
                              else [[0, 4], [10, 1], [20, 4], [30, 1], [40, 4]] if name == "oscillation"
                              else [[0, 1]] if name == "sparse" else [[0, 4]])
        result["overload_windows"] = ([[10, 11]] if name == "transient-overload"
                                      else [[10, 300]] if name == "sustained-overload"
                                      else [[10, 15]] if name == "recovery" else [])
        result["queue_bound"] = 16
        for spec in objects.values():
            if name != "mixed-sizes":
                spec["service_s"] = .4 if name == "sparse" else .04
            spec["tail_s"] = .02
            spec["service_drift_per_s"] = .001 if name == "baseline-drift" else 0
    return result


def public_scenario(plan):
    objects = {
        path: {
            **{k: v for k, v in spec.items() if k != "payload"},
            "bytes": len(spec["payload"]),
            "sha256": digest(spec["payload"]),
        }
        for path, spec in plan["objects"].items()
    }
    return {**plan, "objects": objects}


class ControlledOrigin:
    """Fresh server per independent run; no cross-machine epoch arithmetic."""

    def __init__(self, directory, plan, *, instrument=True, bind="127.0.0.1", port=0, guest=False):
        address = ipaddress.ip_address(bind)
        require(
            bind == "127.0.0.1"
            or (
                guest is True
                and address.version == 4
                and address.is_private
                and not address.is_unspecified
                and not address.is_loopback
            ),
            "guest origin requires an explicit private IPv4 binding",
        )
        self.bind = bind
        require(type(instrument) is bool, "instrumentation flag must be boolean")
        for spec in plan["objects"].values():
            require(0 < spec["service_s"] <= 1, "object service time must be in (0,1]")
        self.plan, self.instrument = plan, instrument
        self.condition, self.stop_event = threading.Condition(), threading.Event()
        self.anchor, self.requests, self.responses = None, 0, 0
        self.failure = None
        self.counts, self.statuses, self.events = Counter(), Counter(), []
        self.log = (directory / "origin.jsonl").open("xb")
        self.model = ServiceModel(plan["schedule"], plan["queue_bound"], self.emit,
                                  limits=workload(plan.get("workload", {}).get("name") or plan.get("research_workload")))
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def setup(self):
                super().setup()
                self.connection.settimeout(5)

            def do_GET(self):
                with owner.condition:
                    if owner.anchor is None:
                        owner.anchor = time.monotonic()
                    owner.requests += 1
                    request = owner.requests
                    owner.counts[self.path] += 1
                    ordinal = owner.counts[self.path]
                    owner.model.arrive(owner.elapsed(), request, self.path)
                    while owner.model.states[request] == "queued":
                        owner.condition.wait(timeout=0.05)
                    service = owner.model.states[request] == "service"
                spec = plan["objects"].get(self.path)
                policy = (
                    {"status": 404}
                    if spec is None
                    else spec["responses"][min(ordinal - 1, len(spec["responses"]) - 1)]
                )
                if not service:
                    policy = {
                        "status": plan["queue_rejection_status"],
                        "retry_after": plan["queue_retry_after"],
                    }
                service_s = (min(1, spec["service_s"] + spec.get("service_drift_per_s", 0) * owner.elapsed())
                             if spec else 0)
                overload_stimulus = service and any(start <= owner.elapsed() < end
                    for start, end in plan.get("overload_windows", []))
                if service and overload_stimulus:
                    policy = {"status": 429 if request % 2 else 503, "retry_after": "0.1"}
                if service and spec and owner.stop_event.wait(service_s):
                    policy = {"status": 503}
                status, sent, disconnected = policy["status"], 0, False
                payload = spec["payload"] if status == 200 and spec is not None else b""
                try:
                    self.send_response(status)
                    self.send_header("Content-Length", str(len(payload)))
                    if "retry_after" in policy:
                        self.send_header("Retry-After", policy["retry_after"])
                    if "location" in policy:
                        self.send_header("Location", policy["location"])
                    self.end_headers()
                    content = payload[: len(payload) // 2] if policy.get("truncate") else payload
                    if content and spec.get("tail_s", 0):
                        self.wfile.write(content[:1])
                        self.wfile.flush()
                        if owner.stop_event.wait(spec["tail_s"]):
                            raise TimeoutError("origin stopped during body")
                        self.wfile.write(content[1:])
                    else:
                        self.wfile.write(content)
                    self.wfile.flush()
                    sent = len(content)
                except (BrokenPipeError, ConnectionResetError, TimeoutError):
                    disconnected, sent = True, None
                finally:
                    with owner.condition:
                        owner.responses += 1
                        owner.statuses[status] += 1
                        fields = {"status": status, "body_bytes_written": sent, "disconnected": disconnected,
                                  "service_duration_s": service_s if service else None,
                                  "overload_stimulus": overload_stimulus}
                        if service:
                            owner.model.finish(owner.elapsed(), request, **fields)
                        owner.emit(
                            {
                                "phase": "response",
                                "origin_elapsed_s": owner.elapsed(),
                                "request_id": request,
                                "path": self.path,
                                "path_attempt": ordinal,
                                **fields,
                            }
                        )
                        owner.condition.notify_all()

            def log_message(self, *args):
                pass

        try:
            self.server = ThreadingHTTPServer((bind, port), Handler)
        except BaseException:
            self.log.close()
            raise
        self.server.daemon_threads = False
        self.server_thread = threading.Thread(
            target=self.server.serve_forever, kwargs={"poll_interval": 0.05}
        )
        self.clock_thread = threading.Thread(target=self.tick)

    def elapsed(self):
        return 0 if self.anchor is None else time.monotonic() - self.anchor

    def emit(self, record):
        if self.instrument:
            event = {"sequence": len(self.events) + 1, "origin_monotonic_ns": time.monotonic_ns(), **record}
            self.events.append(event)
            try:
                self.log.write(encode(event))
                self.log.flush()
            except OSError as exc:
                self.failure = exc
                raise

    def tick(self):
        while not self.stop_event.wait(0.01):
            with self.condition:
                try:
                    if self.anchor is not None:
                        self.model.advance(self.elapsed())
                        self.condition.notify_all()
                except Exception as exc:
                    self.failure = exc
                    self.stop_without_events()
                    return

    def stop_without_events(self):
        self.model.stopped = True
        for request in self.model.queue:
            self.model.states[request] = "cancelled"
        self.model.queue.clear()
        self.stop_event.set()
        self.condition.notify_all()

    def __enter__(self):
        self.server_thread.start()
        self.clock_thread.start()
        return self

    def __exit__(self, *args):
        with self.condition:
            try:
                self.model.stop(self.elapsed())
            except Exception as exc:
                self.failure = self.failure or exc
            finally:
                self.stop_without_events()
        self.server.shutdown()
        self.server.server_close()
        self.server_thread.join(timeout=5)
        self.clock_thread.join(timeout=5)
        self.log.close()
        require(
            not self.server_thread.is_alive() and not self.clock_thread.is_alive(), "origin cleanup failed"
        )
        require(self.failure is None, "origin instrumentation/model failed; retained evidence is incomplete")

    @property
    def base_url(self):
        return f"http://{self.bind}:{self.server.server_port}"

    def snapshot(self):
        with self.condition:
            return {
                "requests": self.requests,
                "responses": self.responses,
                "attempts_by_path": dict(self.counts),
                "statuses": dict(self.statuses),
                "active_service": len(self.model.active),
                "queued": len(self.model.queue),
                "instrumented": self.instrument,
                "attribution": "aggregate origin/path work; duplicate-URL row attribution unavailable",
            }


def audit_events(events, snapshot):
    """Independent replay of origin admission/service invariants and final accounting."""
    if not snapshot["instrumented"]:
        return {"status": "not_collected", "reason": "instrumentation-off calibration"}
    arrivals, admitted, active, finished, responses = set(), set(), set(), set(), set()
    queue, slots, previous_time = deque(), None, -1
    peak_open_requests = 0
    for sequence, event in enumerate(events, 1):
        require(
            event["sequence"] == sequence and event["origin_monotonic_ns"] >= previous_time,
            "origin event ordering invalid",
        )
        previous_time = event["origin_monotonic_ns"]
        request, phase = event.get("request_id"), event["phase"]
        if phase == "capacity":
            slots = event["slots"]
            require(
                event["origin_elapsed_s"] >= event["planned_elapsed_s"],
                "capacity realized before its schedule",
            )
        elif phase == "arrival":
            require(request not in arrivals, "duplicate arrival")
            arrivals.add(request)
            peak_open_requests = max(peak_open_requests, len(arrivals - responses))
        elif phase == "admission":
            require(request in arrivals and request not in admitted, "admission without unique arrival")
            admitted.add(request)
            if event["accepted"]:
                queue.append(request)
        elif phase == "service_start":
            require(
                queue and queue.popleft() == request and request not in active,
                "service violates admitted FIFO",
            )
            active.add(request)
            require(slots is not None and len(active) <= slots, "new service exceeds current origin capacity")
        elif phase == "service_end":
            require(request in active and request not in finished, "service end without active work")
            active.remove(request)
            finished.add(request)
        elif phase == "queue_cancel":
            require(request in queue, "cancellation without queued work")
            queue.remove(request)
        elif phase == "response":
            require(
                request in arrivals
                and request not in responses
                and request not in active
                and request not in queue,
                "response before service closure or duplicate response",
            )
            responses.add(request)
        else:
            raise ValueError("unexpected origin event phase")
    require(
        not active and not queue and arrivals == admitted == responses, "unaccounted origin request/service"
    )
    require(len(arrivals) == snapshot["requests"] == snapshot["responses"], "origin counter mismatch")
    return {
        "status": "verified",
        "requests": len(arrivals),
        "completed_services": len(finished),
        "peak_open_requests": peak_open_requests,
        "unclosed_requests": len(arrivals - responses),
    }
