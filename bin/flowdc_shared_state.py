"""Durable aggregate admission ledger. Timeouts never release uncertain permits."""

import contextlib
import fcntl
import hashlib
import json
import math
import os
import re
import secrets
import sqlite3
import stat
import time
from collections.abc import MutableMapping
from pathlib import Path

from flowdc_methods import origin_key

SCHEMA = "flowdc-shared-admission-v1"
HEX = re.compile(r"[0-9a-f]{64}")
UUID = re.compile(r"[0-9a-f]{32}")
MAX_PERMITS = 16384
MAX_CLIENTS = 64
MAX_EVENTS = 262144


def require(condition, message):
    if not condition:
        raise ValueError(message)


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(value).hexdigest()


def origin(url):
    scheme, host, port = origin_key(url)
    return f"{scheme}://{'[' + host + ']' if ':' in host else host}:{port}"


def finite(value, low=0, high=1e9):
    return type(value) in (int, float) and math.isfinite(value) and low <= value <= high


def private_file(path, *, create=False, writable=True):
    """Open only a current-owner regular private file without following symlinks."""
    flags = (os.O_RDWR if writable else os.O_RDONLY) | os.O_NOFOLLOW
    if create:
        flags |= os.O_CREAT | os.O_EXCL
    fd = os.open(path, flags, 0o600)
    info = os.fstat(fd)
    if not (stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid() and not info.st_mode & 0o077):
        os.close(fd)
        raise ValueError("shared-control file must be owner-private and regular")
    return fd


def write_private(path, value):
    fd = private_file(path, create=True)
    with os.fdopen(fd, "wb") as stream:
        stream.write(encode(value))
        stream.flush()
        os.fsync(stream.fileno())
    sync_directory(Path(path).parent)


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def read_private(path):
    fd = private_file(path, writable=False)
    with os.fdopen(fd, "rb") as stream:
        raw = stream.read(32769)
    require(len(raw) <= 32768, "oversize private control descriptor")
    return json.loads(raw)


class PermitRows(MutableMapping):
    """Lazy transactional permit rows; completed history is never rewritten/scanned
    by the hot admission path. Each accessed mutable row is flushed atomically
    with the session state and event; reads outside change() cannot persist edits.
    """

    def __init__(self, db):
        self.db, self.loaded, self.original = db, {}, {}

    def __getitem__(self, key):
        if key not in self.loaded:
            found = self.db.execute("SELECT value FROM permits WHERE id=?", (key,)).fetchone()
            if found is None:
                raise KeyError(key)
            self.original[key] = found[0]
            self.loaded[key] = json.loads(found[0])
        return self.loaded[key]

    def __setitem__(self, key, value):
        if key not in self.original:
            found = self.db.execute("SELECT value FROM permits WHERE id=?", (key,)).fetchone()
            self.original[key] = found[0] if found is not None else None
        self.loaded[key] = value

    def __delitem__(self, key):
        raise ValueError("retained permit deletion is forbidden")

    def __iter__(self):
        existing = {row[0] for row in self.db.execute("SELECT id FROM permits")}
        return iter(existing | self.loaded.keys())

    def __len__(self):
        return self.db.execute("SELECT COUNT(*) FROM permits").fetchone()[0] + sum(
            raw is None for raw in self.original.values()
        )

    def outstanding(self, key=None, client_id=None):
        # No transaction adds/releases a permit before checking this predicate.
        conditions, args = ["state NOT IN ('complete','quiescent')"], []
        for name, value in (("origin", key), ("client_id", client_id)):
            if value is not None:
                conditions.append(name + "=?")
                args.append(value)
        return [
            json.loads(row[0])
            for row in self.db.execute("SELECT value FROM permits WHERE " + " AND ".join(conditions), args)
        ]

    def flush(self):
        for key, value in self.loaded.items():
            raw = encode(value).decode()
            if raw == self.original[key]:
                continue
            self.db.execute(
                "INSERT INTO permits(id,state,origin,client_id,value) VALUES(?,?,?,?,?) "
                "ON CONFLICT(id) DO UPDATE SET state=excluded.state,origin=excluded.origin,"
                "client_id=excluded.client_id,value=excluded.value",
                (key, value["state"], value["origin"], value["client_id"], raw),
            )


class Ledger:
    """One owner/SQLite transaction stream per run, bounded retained identities.

    Reopening fences all new admission. Old completions remain accountable; only
    acknowledged closure of every client and permit permits a fresh epoch. There
    is deliberately no lease-expiry, heartbeat-based recycling or force-reset API.
    """

    def __init__(self, directory, binding, *, reopen=False, clock=time.monotonic, fault=None):
        self.clock, self.fault = clock, fault or (lambda _: None)
        self.directory = Path(directory).absolute()
        if not reopen:
            self.directory.mkdir(mode=0o700, parents=False, exist_ok=False)
        info = self.directory.lstat()
        require(
            stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid() and not info.st_mode & 0o077,
            "shared ledger directory must be owner-private",
        )
        require(
            isinstance(binding, dict)
            and set(binding) == {"run_id", "source_sha256", "config_sha256", "method"}
            and UUID.fullmatch(binding["run_id"])
            and HEX.fullmatch(binding["source_sha256"])
            and HEX.fullmatch(binding["config_sha256"]),
            "invalid shared session binding",
        )
        self.lock = private_file(self.directory / "owner.lock", create=not reopen)
        self.db = None
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            database = self.directory / "ledger.sqlite"
            fd = private_file(database, create=not reopen)
            os.close(fd)
            self.db = sqlite3.connect(database, isolation_level=None)
            self.db.execute("PRAGMA journal_mode=DELETE")
            self.db.execute("PRAGMA synchronous=FULL")
            if not reopen:
                self.db.executescript(
                    "CREATE TABLE state (id INTEGER PRIMARY KEY CHECK(id=1), value TEXT NOT NULL);"
                    "CREATE TABLE events (seq INTEGER PRIMARY KEY, value TEXT NOT NULL);"
                    "CREATE TABLE permits (id TEXT PRIMARY KEY, state TEXT NOT NULL,"
                    "origin TEXT NOT NULL, client_id TEXT NOT NULL, value TEXT NOT NULL);"
                    "CREATE INDEX active_origin ON permits(origin) WHERE state NOT IN ('complete','quiescent');"
                    "CREATE INDEX active_client ON permits(client_id) WHERE state NOT IN ('complete','quiescent');"
                    "CREATE INDEX active_all ON permits(state) WHERE state NOT IN ('complete','quiescent');"
                )
                initial = {
                    "schema": SCHEMA,
                    "storage_schema": "normalized-permits-v1",
                    "binding": binding,
                    "epoch": 1,
                    "phase": "open",
                    "scopes": {},
                    "clients": {},
                    "origins": {},
                }
                self.db.execute("INSERT INTO state VALUES(1,?)", (encode(initial).decode(),))
                sync_directory(self.directory)
                sync_directory(self.directory.parent)
            current = self.current()
            require(current["schema"] == SCHEMA and current["binding"] == binding, "ledger binding mismatch")
            if reopen:
                with self.change("restart_fence") as state:
                    state["phase"] = "fenced"
                    # Monotonic epochs cannot be restored across process/host boots.
                    # Embargoes are not cleared; recovery requires full quiescence.
                    for entry in state["origins"].values():
                        entry["embargo_until"] = self.clock() + entry["embargo_delay_max"]
        except BaseException:
            self.close()
            raise

    def close(self):
        if self.db is not None:
            self.db.close()
            self.db = None
        if self.lock is not None:
            os.close(self.lock)
            self.lock = None

    def snapshot(self):
        state = self.current()
        state["permits"] = dict(state["permits"])
        return state

    def current(self):
        state = json.loads(self.db.execute("SELECT value FROM state WHERE id=1").fetchone()[0])
        require(
            state.get("storage_schema") == "normalized-permits-v1",
            "unsupported development ledger storage; retain it and start a distinct run",
        )
        state["permits"] = PermitRows(self.db)
        return state

    @contextlib.contextmanager
    def change(self, action, **fields):
        last = self.db.execute("SELECT COALESCE(MAX(seq),0) FROM events").fetchone()[0]
        require(last < MAX_EVENTS, "shared event journal bound exceeded; admission refused")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            state = self.current()
            yield state
            event = {"action": action, "manager_monotonic_s": self.clock(), "epoch": state["epoch"], **fields}
            state["permits"].flush()
            session = {key: value for key, value in state.items() if key != "permits"}
            self.db.execute("UPDATE state SET value=? WHERE id=1", (encode(session).decode(),))
            self.db.execute("INSERT INTO events(value) VALUES(?)", (encode(event).decode(),))
            self.fault("before_commit")
            self.db.execute("COMMIT")
        except BaseException:
            if self.db.in_transaction:
                self.db.execute("ROLLBACK")
            raise

    def events(self):
        return [
            {"sequence": seq, **json.loads(raw)}
            for seq, raw in self.db.execute("SELECT seq,value FROM events ORDER BY seq")
        ]

    def enroll(self, scope_id, rows, *, attempts=2):
        require(
            UUID.fullmatch(scope_id) and type(attempts) is int and 1 <= attempts <= 4, "invalid task scope"
        )
        require(
            isinstance(rows, list)
            and 1 <= len(rows) <= 256
            and len(set(rows)) == len(rows)
            and all(isinstance(row, str) and HEX.fullmatch(row) for row in rows),
            "invalid scope rows",
        )
        token = secrets.token_hex(32)
        with self.change("enroll", scope_id=scope_id) as state:
            require(
                state["phase"] == "open" and scope_id not in state["scopes"],
                "scope collision or fenced manager",
            )
            require(len(state["scopes"]) < MAX_CLIENTS, "scope bound exceeded")
            all_rows = {
                row
                for scope in state["scopes"].values()
                if scope["epoch"] == state["epoch"]
                for row in scope["rows"]
            }
            require(
                not all_rows.intersection(rows) and len(all_rows) + len(rows) <= 256,
                "overlapping or excessive session rows",
            )
            state["scopes"][scope_id] = {
                "token_sha256": digest(token.encode()),
                "rows": rows,
                "attempts": attempts,
                "epoch": state["epoch"],
            }
        return token

    def authenticate(self, token):
        require(isinstance(token, str) and HEX.fullmatch(token), "authentication refused")
        wanted = digest(token.encode())
        for identifier, scope in self.current()["scopes"].items():
            if secrets.compare_digest(scope["token_sha256"], wanted):
                return identifier
        raise ValueError("authentication refused")

    def client(self, state, scope, client_id, *, active=False):
        client = state["clients"].get(client_id)
        require(client is not None and client["scope"] == scope, "client identity refused")
        if active:
            require(
                state["phase"] == "open" and client["status"] == "open" and client["epoch"] == state["epoch"],
                "client or manager fenced",
            )
        return client

    def connect(self, scope, client_id, worker_id, epoch):
        require(
            UUID.fullmatch(client_id)
            and isinstance(worker_id, str)
            and re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", worker_id),
            "invalid client identity",
        )
        with self.change("connect", client_id=client_id, worker_id=worker_id, scope_id=scope) as state:
            require(
                scope in state["scopes"] and state["phase"] == "open" and epoch == state["epoch"],
                "session epoch fenced",
            )
            require(state["scopes"][scope]["epoch"] == epoch, "credential belongs to a fenced epoch")
            if client_id in state["clients"]:
                client = self.client(state, scope, client_id, active=True)
                require(client["worker_id"] == worker_id, "worker identity reassignment")
            else:
                used = sum(client["scope"] == scope for client in state["clients"].values())
                require(
                    used < state["scopes"][scope]["attempts"] and len(state["clients"]) < MAX_CLIENTS,
                    "task replay bound exceeded",
                )
                state["clients"][client_id] = {
                    "scope": scope,
                    "worker_id": worker_id,
                    "epoch": epoch,
                    "status": "open",
                    "last_seen": self.clock(),
                }
        return {"epoch": epoch, "binding": self.current()["binding"]}

    def heartbeat(self, scope, client_id):
        with self.change("heartbeat", client_id=client_id) as state:
            client = self.client(state, scope, client_id, active=True)
            client["last_seen"] = self.clock()
        return {"status": "open"}

    def expire_clients(self, age=5):
        require(finite(age, 1, 60), "invalid heartbeat age")
        now = self.clock()
        current = self.current()
        stale = [
            key
            for key, value in current["clients"].items()
            if value["status"] == "open" and now - value["last_seen"] > age
        ]
        if stale:
            with self.change("uncertain_clients", client_ids=stale) as state:
                for key in stale:
                    state["clients"][key]["status"] = "uncertain"
        return stale  # No permit or capacity is released.

    def configure_origin(self, url, limit):
        key = origin(url)
        require(type(limit) is int and 1 <= limit <= 10000, "invalid aggregate limit")
        with self.change("limit", origin=key, limit=limit) as state:
            entry = state["origins"].setdefault(
                key, {"limit": limit, "embargo_until": 0, "embargo_delay_max": 0}
            )
            entry["limit"] = limit

    @staticmethod
    def outstanding(state, key=None, client_id=None):
        if isinstance(state["permits"], PermitRows):
            return state["permits"].outstanding(key, client_id)
        return [
            p
            for p in state["permits"].values()
            if p["state"] not in ("complete", "quiescent")
            and (key is None or p["origin"] == key)
            and (client_id is None or p["client_id"] == client_id)
        ]

    def acquire(self, scope, client_id, row_id, request_id, url):
        require(
            isinstance(url, str) and len(url) <= 16384 and UUID.fullmatch(request_id),
            "invalid request identity",
        )
        key = origin(url)
        permit_id = digest(encode([client_id, request_id]))
        current = self.current()
        self.client(current, scope, client_id, active=True)
        require(row_id in current["scopes"][scope]["rows"], "row outside task scope")
        existing = current["permits"].get(permit_id)
        if existing is not None:
            require(existing["row_id"] == row_id and existing["url"] == url, "conflicting request replay")
            return {"permit_id": permit_id, "state": existing["state"], "epoch": existing["epoch"]}
        entry = current["origins"].get(key)
        require(entry is not None, "origin controller unavailable")
        if self.clock() < entry["embargo_until"] or len(self.outstanding(current, key)) >= entry["limit"]:
            return {"state": "wait"}
        require(
            len(current["permits"]) < MAX_PERMITS
            and len(self.outstanding(current, client_id=client_id)) < 256,
            "permit bound exceeded",
        )
        with self.change(
            "acquire", permit_id=permit_id, client_id=client_id, row_id=row_id, origin=key
        ) as state:
            client = self.client(state, scope, client_id, active=True)
            state["permits"][permit_id] = {
                "permit_id": permit_id,
                "client_id": client_id,
                "worker_id": client["worker_id"],
                "task_attempt_id": client_id,
                "row_id": row_id,
                "request_id": request_id,
                "url": url,
                "origin": key,
                "epoch": state["epoch"],
                "state": "issued",
                "issued_at": self.clock(),
                "dispatch_at": None,
                "headers": None,
                "completion": None,
            }
        return {"permit_id": permit_id, "state": "issued", "epoch": current["epoch"]}

    def permit(self, state, scope, client_id, permit_id):
        self.client(state, scope, client_id)
        permit = state["permits"].get(permit_id)
        require(permit is not None and permit["client_id"] == client_id, "permit identity refused")
        return permit

    def dispatch(self, scope, client_id, permit_id, epoch):
        state = self.current()
        self.client(state, scope, client_id, active=True)
        permit = self.permit(state, scope, client_id, permit_id)
        require(epoch == state["epoch"] == permit["epoch"], "stale dispatch epoch")
        require(permit["state"] == "issued", "permit already dispatched or complete")
        if self.clock() < state["origins"][permit["origin"]]["embargo_until"]:
            return {"state": "wait"}
        with self.change("dispatch", permit_id=permit_id, client_id=client_id) as state:
            state["permits"][permit_id].update(state="dispatched", dispatch_at=self.clock())
        return {"state": "dispatched", "epoch": epoch}

    def headers(self, scope, client_id, permit_id, status, retry_after):
        require(
            type(status) is int and 100 <= status <= 599 and (retry_after is None or finite(retry_after)),
            "invalid response headers",
        )
        value = {"status": status, "retry_after": retry_after}
        state = self.current()
        permit = self.permit(state, scope, client_id, permit_id)
        if permit["headers"] is not None:
            require(permit["headers"] == value, "conflicting header replay")
            return {"duplicate": True}
        require(
            permit["dispatch_at"] is not None and permit["state"] in ("dispatched", "quiescent"),
            "headers require dispatched work",
        )
        with self.change("headers", permit_id=permit_id, observation=value) as state:
            state["permits"][permit_id]["headers"] = value
            if retry_after is not None:
                entry = state["origins"][permit["origin"]]
                entry["embargo_until"] = max(entry["embargo_until"], self.clock() + retry_after)
                entry["embargo_delay_max"] = max(entry["embargo_delay_max"], retry_after)
        return {"duplicate": False}

    def complete(self, scope, client_id, permit_id, observation):
        require(isinstance(observation, dict) and len(encode(observation)) <= 4096, "invalid completion")
        state = self.current()
        permit = self.permit(state, scope, client_id, permit_id)
        if permit["completion"] is not None:
            require(permit["completion"] == observation, "conflicting completion replay")
            return {"duplicate": True}
        with self.change("complete", permit_id=permit_id, observation=observation) as state:
            status, delay = observation.get("status"), observation.get("retry_after")
            if status is not None:
                require(
                    type(status) is int and 100 <= status <= 599 and (delay is None or finite(delay)),
                    "invalid completion headers",
                )
                headers = {"status": status, "retry_after": delay}
                require(permit["dispatch_at"] is not None, "response requires dispatched work")
                if permit["headers"] is not None:
                    require(permit["headers"] == headers, "conflicting completion headers")
                else:
                    state["permits"][permit_id]["headers"] = headers
                    if delay is not None:
                        entry = state["origins"][permit["origin"]]
                        entry["embargo_until"] = max(entry["embargo_until"], self.clock() + delay)
                        entry["embargo_delay_max"] = max(entry["embargo_delay_max"], delay)
            state["permits"][permit_id].update(
                state="complete", completion=observation, completed_at=self.clock()
            )
        # An owner-proven stopped attempt may deliver a late observation. Retain
        # it, but never feed a different epoch's controller or release twice.
        return {"duplicate": permit["state"] == "quiescent", "late": permit["state"] == "quiescent"}

    def prove_quiescent(self, client_id, proof):
        """Owner-only evidence receipt; NOT exposed on the worker RPC surface.

        Callers must have observed a source-bound task's actual process exit or
        proven every process in their own local worker tree stopped. Cancellation
        requests, heartbeat loss, TTL expiry and user-supplied booleans are not
        such proof. Preserve unknown request work separately from completion.
        """
        state = self.current()
        require(
            isinstance(proof, dict)
            and set(proof) == {"kind", "run_id", "source_sha256", "client_id", "evidence_sha256"},
            "invalid quiescence proof",
        )
        require(
            proof["kind"] in ("native_task_exit", "owned_process_tree_exit")
            and proof["run_id"] == state["binding"]["run_id"]
            and proof["source_sha256"] == state["binding"]["source_sha256"]
            and proof["client_id"] == client_id
            and isinstance(proof["evidence_sha256"], str)
            and HEX.fullmatch(proof["evidence_sha256"]),
            "unbound quiescence proof",
        )
        require(client_id in state["clients"], "unknown quiescent client")
        previous = state["clients"][client_id].get("quiescence_proof")
        if previous is not None:
            require(previous == proof, "conflicting quiescence proof")
            return {"duplicate": True}
        with self.change("prove_quiescent", client_id=client_id, proof=proof) as state:
            for permit in self.outstanding(state, client_id=client_id):
                state["permits"][permit["permit_id"]].update(state="quiescent", quiescence_proof=proof)
            state["clients"][client_id].update(status="closed", quiescence_proof=proof)
        return {"duplicate": False}

    def close_client(self, scope, client_id):
        with self.change("close_client", client_id=client_id) as state:
            client = self.client(state, scope, client_id)
            require(
                not self.outstanding(state, client_id=client_id), "uncertain work prevents client closure"
            )
            client["status"] = "closed"
        return {"status": "closed"}

    def fence(self, reason):
        with self.change("fence", reason=reason) as state:
            state["phase"] = "fenced"

    def recover_closed_epoch(self):
        """Owner-only recovery after acknowledgements, never based on elapsed time."""
        with self.change("recover_closed_epoch") as state:
            require(
                state["phase"] == "fenced" and not self.outstanding(state),
                "uncertain permits prevent recovery",
            )
            require(
                all(client["status"] == "closed" for client in state["clients"].values()),
                "unclosed clients prevent recovery",
            )
            state["epoch"] += 1
            state["phase"] = "open"
            # All old client work has acknowledged closure; new controller state
            # and credentials must be enrolled explicitly for this epoch.
            # Preserve conservative embargoes across epoch changes. New policy
            # instances replace limits when their origins are first requested.
        return self.current()["epoch"]

    def export(self):
        state = self.snapshot()
        for scope in state["scopes"].values():
            scope.pop("token_sha256")
        return {"state": state, "events": self.events()}
