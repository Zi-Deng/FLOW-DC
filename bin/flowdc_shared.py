"""Authenticated manager authority and fail-closed acquisition clients."""

import asyncio
import dataclasses
import ipaddress
import socket
import ssl
import time
from pathlib import Path
from uuid import uuid4

import aiohttp
from aiohttp import web
from flowdc_methods import ControllerManager, method_record
from flowdc_shared_state import (
    HEX,
    SCHEMA,
    UUID,
    Ledger,
    digest,
    encode,
    finite,
    origin,
    read_private,
    require,
    write_private,
)
from flowdc_staging import DOWNLOAD_FILES
from yarl import URL

MODULES = DOWNLOAD_FILES
RPC_TIMEOUT_S = 3.0
BACKPRESSURE_DELAYS_S = (0.05, 0.1, 0.2)


class SharedControlError(RuntimeError):
    """A control-channel failure must never enable independent worker capacity."""


def runtime_binding(config):
    excluded = {
        "input_path",
        "output_folder",
        "force_overwrite",
        "resume",
        "reconcile",
        "shared_control_file",
    }
    settings = {key: value for key, value in dataclasses.asdict(config).items() if key not in excluded}
    return {
        "source_sha256": digest(
            encode({name: digest(Path(__file__).with_name(name).read_bytes()) for name in MODULES})
        ),
        "config_sha256": digest(encode(settings)),
        "method": method_record(config)["id"],
    }


def protected_endpoint(endpoint):
    url = URL(endpoint)
    require(
        url.user is None and not url.query and not url.fragment and url.path in ("", "/"),
        "invalid shared-control endpoint",
    )
    if url.scheme == "http":
        require(
            url.raw_host is not None and ipaddress.ip_address(url.raw_host).is_loopback,
            "plaintext control must use loopback or an authenticated SSH forward",
        )
    else:
        require(
            url.scheme == "https" and url.raw_host is not None, "verified HTTPS control endpoint required"
        )
    return str(url).rstrip("/")


def descriptor(config):
    record = read_private(config.shared_control_file)
    require(
        isinstance(record, dict)
        and {"schema", "endpoint", "binding", "epoch", "credential"}
        <= set(record)
        <= {"schema", "endpoint", "binding", "epoch", "credential", "ca_pem"}
        and record["schema"] == SCHEMA,
        "invalid private control descriptor",
    )
    protected_endpoint(record["endpoint"])
    require(
        isinstance(record["binding"], dict)
        and set(record["binding"]) == {"run_id", "source_sha256", "config_sha256", "method"}
        and isinstance(record["binding"]["run_id"], str)
        and UUID.fullmatch(record["binding"]["run_id"])
        and isinstance(record["credential"], str)
        and HEX.fullmatch(record["credential"]),
        "invalid shared identity/credential",
    )
    require(
        {key: record["binding"].get(key) for key in runtime_binding(config)} == runtime_binding(config),
        "shared source/config/method mismatch",
    )
    require(type(record["epoch"]) is int and record["epoch"] >= 1, "invalid control epoch")
    if "ca_pem" in record:
        require(
            record["endpoint"].startswith("https://")
            and isinstance(record["ca_pem"], str)
            and len(record["ca_pem"].encode()) <= 16384,
            "invalid explicit control trust",
        )
        control_tls_context(record)
    return record


def control_tls_context(description):
    if "ca_pem" not in description:
        return True  # aiohttp default certificate/hostname verification
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_verify_locations(cadata=description["ca_pem"])
    return context


def observation(value):
    expected = {
        "status",
        "retry_after",
        "ttfb",
        "body_bytes",
        "latency_eligible",
        "body_complete",
        "response_complete",
        "is_conn_error",
        "is_local_error",
        "is_unknown_error",
        "reason",
    }
    require(isinstance(value, dict) and set(value) == expected, "invalid completion fields")
    require(
        value["status"] is None or type(value["status"]) is int and 100 <= value["status"] <= 599,
        "invalid status",
    )
    for key in ("retry_after", "ttfb"):
        require(value[key] is None or finite(value[key]), "invalid observation duration")
    require(
        type(value["body_bytes"]) is int and 0 <= value["body_bytes"] <= 64 * 1024 * 1024, "invalid body size"
    )
    require(
        all(
            type(value[key]) is bool
            for key in (
                "latency_eligible",
                "body_complete",
                "response_complete",
                "is_conn_error",
                "is_local_error",
                "is_unknown_error",
            )
        ),
        "invalid observation flags",
    )
    require(
        value["reason"] in ("final", "redirect", "cancelled", "transport_closed"), "invalid completion reason"
    )
    if value["latency_eligible"]:
        require(
            value["body_complete"]
            and value["status"] == 200
            and value["body_bytes"] > 0
            and value["ttfb"] is not None
            and value["ttfb"] > 0,
            "unsupported latency confidence",
        )
    return value


class Authority:
    """Serialized policy transitions and durable admission on the manager clock."""

    def __init__(self, directory, config, *, run_id=None, reopen=False):
        from download_batch import AdaptiveSemaphore, PAARCController, normalize_config

        self.config = normalize_config(config)
        require(
            config.control_method is not None and config.enable_paarc,
            "shared authority requires an explicit method",
        )
        binding = {"run_id": run_id or uuid4().hex, **runtime_binding(self.config)}
        self.ledger = Ledger(directory, binding, reopen=reopen)
        self.controllers = ControllerManager(self.config, AdaptiveSemaphore, PAARCController, self.emit)
        self.lock = asyncio.Lock()
        self.failure = None
        self.runner = self.tick_task = None
        self.endpoint = None
        self.next_step = {}
        self.registered_origins = set()
        self.connections = 0

    def emit(self, record):
        from flowdc_integrity import json_value

        with self.ledger.change("control", trajectory=json_value(record)):
            pass

    async def controller(self, url):
        require(
            len(self.controllers.controllers) < 32 or origin(url) in self.registered_origins,
            "origin bound exceeded",
        )
        controller = await self.controllers.get_controller(url)
        if origin(url) not in self.registered_origins:
            self.ledger.configure_origin(url, controller.semaphore.limit)
            self.registered_origins.add(origin(url))
        return controller

    def enroll(self, scope_id, rows, path, *, attempts=2, ca_pem=None):
        require(self.endpoint is not None, "start manager before enrolling a client")
        token = self.ledger.enroll(scope_id, rows, attempts=attempts)
        state = self.ledger.current()
        write_private(
            path,
            {
                "schema": SCHEMA,
                "endpoint": self.endpoint,
                "binding": state["binding"],
                "epoch": state["epoch"],
                "credential": token,
                **({"ca_pem": ca_pem} if ca_pem is not None else {}),
            },
        )

    async def request(self, token, message):
        require(
            isinstance(message, dict)
            and set(message) == {"schema", "binding", "client_id", "operation", "arguments"}
            and message["schema"] == SCHEMA,
            "invalid control envelope",
        )
        state = self.ledger.current()
        require(message["binding"] == state["binding"] and self.failure is None, "session unavailable")
        scope = self.ledger.authenticate(token)
        client = message["client_id"]
        args, operation = message["arguments"], message["operation"]
        fields = {
            "connect": {"worker_id", "epoch"},
            "heartbeat": set(),
            "close": set(),
            "acquire": {"row_id", "request_id", "url"},
            "dispatch": {"permit_id", "epoch"},
            "headers": {"permit_id", "status", "retry_after"},
            "complete": {"permit_id", "observation"},
        }
        require(
            operation in fields and isinstance(args, dict) and set(args) == fields[operation],
            "invalid control operation",
        )
        if operation == "dispatch":
            # Preserve base PAARC pacing, then recheck epoch/embargo atomically.
            permit = self.ledger.permit(state, scope, client, args["permit_id"])
            controller = await self.controller(permit["url"])
            if controller.smoother is not None:
                await controller.smoother.acquire()
        async with self.lock:
            if operation == "connect":
                return self.ledger.connect(scope, client, **args)
            if operation == "heartbeat":
                return self.ledger.heartbeat(scope, client)
            if operation == "close":
                return self.ledger.close_client(scope, client)
            if operation == "acquire":
                current = self.ledger.current()
                self.ledger.client(current, scope, client, active=True)
                require(args["row_id"] in current["scopes"][scope]["rows"], "row outside task scope")
                await self.controller(args["url"])
                return self.ledger.acquire(scope, client, **args)
            if operation == "dispatch":
                return self.ledger.dispatch(scope, client, **args)
            if operation == "headers":
                return self.ledger.headers(scope, client, **args)
            value = observation(args["observation"])
            permit = self.ledger.permit(self.ledger.current(), scope, client, args["permit_id"])
            result = self.ledger.complete(scope, client, args["permit_id"], value)
            if not result["duplicate"]:
                controller = await self.controller(permit["url"])
                await controller.metrics.record(
                    status_code=value["status"],
                    ttfb=value["ttfb"],
                    bytes_downloaded=value["body_bytes"],
                    retry_after_sec=value["retry_after"],
                    latency_eligible=value["latency_eligible"],
                    is_conn_error=value["is_conn_error"],
                    is_local_error=value["is_local_error"],
                    is_unknown_error=value["is_unknown_error"],
                    acquisition_success=value["body_complete"] and not value["is_local_error"],
                    dispatch_at=permit["dispatch_at"],
                )
            return result

    async def tick(self):
        try:
            while True:
                await asyncio.sleep(0.05)
                async with self.lock:
                    self.ledger.expire_clients()
                    if self.ledger.current()["phase"] != "open":
                        continue
                    for key, controller in self.controllers.controllers.items():
                        url = origin(str(URL.build(scheme=key[0], host=key[1], port=key[2])))
                        now = time.monotonic()
                        if now < self.next_step.get(url, 0):
                            continue
                        controller.semaphore._inflight = len(
                            self.ledger.outstanding(self.ledger.current(), url)
                        )
                        snap = await controller.step_interval()
                        self.ledger.configure_origin(url, controller.semaphore.limit)
                        self.next_step[url] = now + controller._calculate_control_interval(snap)
        except asyncio.CancelledError:
            raise
        except Exception:
            self.failure = SharedControlError("manager policy unavailable")
            self.ledger.fence("policy_failure")

    async def handle(self, request):
        if self.connections >= 64:
            return web.json_response({"error": "backpressure"}, status=429)
        self.connections += 1
        try:
            auth = request.headers.get("Authorization", "")
            require(auth.startswith("Bearer "), "authentication refused")
            self.ledger.authenticate(auth[7:])
            message = await request.json()
            require(len(encode(message)) <= 32768, "control message bound exceeded")
            result = await self.request(auth[7:], message)
            return web.json_response(result)
        except (ValueError, KeyError, TypeError):
            return web.json_response({"error": "control request refused"}, status=403)
        except Exception:
            self.failure = SharedControlError("manager storage unavailable")
            return web.json_response({"error": "control unavailable"}, status=503)
        finally:
            self.connections -= 1

    async def start(self, *, host="127.0.0.1", port=0, ssl_context=None, endpoint=None):
        require(
            ipaddress.ip_address(host).is_loopback or ssl_context is not None, "remote control requires TLS"
        )
        app = web.Application(client_max_size=32768)
        app.router.add_post("/control", self.handle)
        self.runner = web.AppRunner(app, access_log=None, shutdown_timeout=3)
        await self.runner.setup()
        site = web.TCPSite(self.runner, host, port, ssl_context=ssl_context)
        await site.start()
        actual_port = site._server.sockets[0].getsockname()[1]
        self.endpoint = protected_endpoint(
            endpoint or str(URL.build(scheme="https" if ssl_context else "http", host=host, port=actual_port))
        )
        self.tick_task = asyncio.create_task(self.tick())
        return self.endpoint

    async def stop(self):
        if self.tick_task is not None:
            self.tick_task.cancel()
            await asyncio.gather(self.tick_task, return_exceptions=True)
        self.ledger.fence("manager_stop")
        if self.runner is not None:
            await self.runner.cleanup()


class SharedClient:
    def __init__(self, config, emit):
        self.description = descriptor(config)
        self.client_id = uuid4().hex
        self.worker_id = socket.gethostname()
        self.emit = emit
        self.failure = None
        self.session = self.heartbeat_task = None
        self.active_tasks = set()
        self.pending = set()
        self.messages = asyncio.Semaphore(4)

    def check_health(self):
        if self.failure is not None:
            raise SharedControlError("shared manager unavailable; acquisition stopped") from None

    def fail(self):
        self.failure = SharedControlError("shared manager unavailable; acquisition stopped")
        for task in self.active_tasks:
            if task is not asyncio.current_task():
                task.cancel()

    async def rpc(self, operation, **arguments):
        self.check_health()
        message = {
            "schema": SCHEMA,
            "binding": self.description["binding"],
            "client_id": self.client_id,
            "operation": operation,
            "arguments": arguments,
        }
        try:
            # One deadline includes local queueing, replies and safe backpressure
            # retries. A lost/ambiguous acknowledgement is never retried here.
            result, retries = await asyncio.wait_for(self.exchange(message), RPC_TIMEOUT_S)
            self.emit(
                {
                    "shared_event": operation,
                    "session_run_id": self.description["binding"]["run_id"],
                    "task_attempt_id": self.client_id,
                    "worker_id": self.worker_id,
                    "arguments": arguments,
                    "receipt": result,
                    "control_backpressure_retries": retries,
                }
            )
            return result
        except asyncio.CancelledError:
            raise
        except Exception:
            self.fail()
            raise SharedControlError("shared manager unavailable; acquisition stopped") from None

    async def exchange(self, message):
        import json

        for retries in range(len(BACKPRESSURE_DELAYS_S) + 1):
            self.check_health()
            async with self.messages:
                async with self.session.post(
                    self.description["endpoint"] + "/control",
                    json=message,
                    headers={"Authorization": "Bearer " + self.description["credential"]},
                    allow_redirects=False,
                ) as response:
                    status = response.status
                    raw = bytearray()
                    while True:
                        chunk = await response.content.read(32769 - len(raw))
                        if not chunk:
                            break
                        raw.extend(chunk)
                        require(len(raw) <= 32768, "oversize control response")
                    result = json.loads(raw)
            if status == 200:
                return result, retries
            # Only this authority's pre-execution refusal is safe to replay.
            # In particular 503 can follow a partially committed operation.
            require(status == 429 and result == {"error": "backpressure"}, "control request failed")
            self.emit(
                {
                    "shared_event": "backpressure",
                    "session_run_id": self.description["binding"]["run_id"],
                    "task_attempt_id": self.client_id,
                    "worker_id": self.worker_id,
                    "operation": message["operation"],
                    "refusal_number": retries + 1,
                }
            )
            require(retries < len(BACKPRESSURE_DELAYS_S), "control backpressure exhausted")
            await asyncio.sleep(BACKPRESSURE_DELAYS_S[retries])

    async def start(self):
        # A separate session prevents origin redirects from receiving credentials.
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=RPC_TIMEOUT_S),
            connector=aiohttp.TCPConnector(limit=4, ssl=control_tls_context(self.description)),
        )
        try:
            result = await self.rpc("connect", worker_id=self.worker_id, epoch=self.description["epoch"])
            require(result["binding"] == self.description["binding"], "manager binding mismatch")
        except BaseException:
            await self.session.close()
            raise
        self.heartbeat_task = asyncio.create_task(self.heartbeat())

    async def heartbeat(self):
        try:
            while True:
                await asyncio.sleep(0.5)
                await self.rpc("heartbeat")
        except asyncio.CancelledError:
            raise
        except SharedControlError:
            self.fail()

    def attempt(self, row_id):
        return RemoteAttempt(self, row_id)

    async def close(self):
        if self.heartbeat_task is not None:
            self.heartbeat_task.cancel()
            await asyncio.gather(self.heartbeat_task, return_exceptions=True)
        try:
            if self.session is not None and self.failure is None and not self.pending:
                await self.rpc("close")
        finally:
            if self.session is not None:
                await self.session.close()
                self.session = None


class RemoteAttempt:
    def __init__(self, client, row_id):
        self.client, self.row_id = client, row_id
        self.permit = self.headers_seen = None
        self.task = asyncio.current_task()
        client.active_tasks.add(self.task)

    async def dispatch(self, url):
        require(self.permit is None, "implicit HTTP replay lacks a fresh permit")
        request_id = uuid4().hex
        while True:
            self.client.check_health()
            result = await self.client.rpc("acquire", row_id=self.row_id, request_id=request_id, url=url)
            if result["state"] == "issued":
                self.permit = result["permit_id"]
                self.client.pending.add(self.permit)
                break
            require(result["state"] == "wait", "request replay cannot dispatch twice")
            await asyncio.sleep(0.05)
        while True:
            result = await self.client.rpc(
                "dispatch", permit_id=self.permit, epoch=self.client.description["epoch"]
            )
            if result["state"] == "dispatched":
                return
            require(result["state"] == "wait", "invalid dispatch receipt")
            await asyncio.sleep(0.05)

    async def headers(self, status, retry_after):
        require(self.permit is not None, "response without a manager permit")
        self.headers_seen = {"status": status, "retry_after": retry_after}
        await self.client.rpc("headers", permit_id=self.permit, **self.headers_seen)

    async def finish_hop(self, trace, reason):
        if self.permit is None:
            return
        kind = trace.get("failure_kind")
        value = {
            "status": None,
            "retry_after": None,
            **(self.headers_seen or {}),
            "ttfb": trace.get("ttfb") if reason == "final" else None,
            "body_bytes": trace.get("observed_response_body_bytes", 0) if reason == "final" else 0,
            "latency_eligible": bool(trace.get("latency_eligible")) if reason == "final" else False,
            "body_complete": trace.get("body_completed_at") is not None if reason == "final" else False,
            "response_complete": bool(trace.get("remote_response_complete"))
            or (reason == "final" and trace.get("body_completed_at") is not None),
            "is_conn_error": kind == "transport",
            "is_local_error": kind in ("local", "admission"),
            "is_unknown_error": kind == "unknown",
            "reason": reason,
        }
        await self.client.rpc("complete", permit_id=self.permit, observation=value)
        self.client.pending.discard(self.permit)
        self.permit = self.headers_seen = None

    async def close(self, trace):
        try:
            await self.finish_hop(trace, "cancelled")
        finally:
            self.client.active_tasks.discard(self.task)
