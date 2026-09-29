"""Explicit bounded TaskVine/shared-origin profile for retained research artifacts.

The historical TaskvineFLOWDC route remains separate. This profile requires a
known-truth catalog and a hashed portable environment; it never activates VMs.
"""

import asyncio
import dataclasses
import hashlib
import json
import os
import re
import secrets
import ssl
import sys
import time
from pathlib import Path
from uuid import uuid4

import polars as pl
from download_batch import Config, normalize_config
from flowdc_shared import Authority, protected_endpoint
from flowdc_shared_state import Ledger, private_file
from flowdc_staging import WORKER_FILES
from flowdc_vine_cohort import dispatch_audit
from flowdc_vine_cohort import validate as validate_cohort
from flowdc_vine_native import RUNTIME, NativeManager
from flowdc_vine_protocol import digest, parse, require, unpack_return, write_new

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from benchmark.core.truth import PROVENANCE, Truth, initial_outcomes, partition_truth  # noqa: E402
from benchmark.core.verifier import verify_native  # noqa: E402

PROFILE = "shared-origin-v1"
ROOT = Path(__file__).resolve().parent


def file_digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def validate_config(value):
    require(isinstance(value, dict), "distributed config must be an object")
    mandatory = {
        "distributed_profile",
        "original_manifest",
        "catalog",
        "output_directory",
        "environment_archive",
        "environment_sha256",
        "workers",
        "download",
    }
    optional = {
        "deadline_s",
        "task_deadline_s",
        "max_attempts",
        "port_number",
        "control_tls",
        "native_password_file",
        "local_worker_binary",
    }
    require(mandatory <= set(value) <= mandatory | optional, "invalid distributed config fields")
    require(value["distributed_profile"] == PROFILE, "unsupported distributed profile")
    config = {"deadline_s": 180, "task_deadline_s": 120, "max_attempts": 1, "port_number": 0, **value}
    require(type(config["workers"]) is int and config["workers"] in (1, 2, 4), "workers must be 1/2/4")
    require(type(config["max_attempts"]) is int and 1 <= config["max_attempts"] <= 4, "invalid attempt bound")
    require(
        type(config["deadline_s"]) is int and 10 <= config["deadline_s"] <= 180,
        "manager deadline must be 10..180 seconds",
    )
    require(
        type(config["task_deadline_s"]) is int and 1 <= config["task_deadline_s"] < config["deadline_s"],
        "task deadline must be positive and less than manager deadline",
    )
    require(type(config["port_number"]) is int and 0 <= config["port_number"] <= 65535, "invalid port")
    require(
        isinstance(config["environment_sha256"], str)
        and re.fullmatch(r"[0-9a-f]{64}", config["environment_sha256"]),
        "environment hash required",
    )
    if "control_tls" in config:
        tls = config["control_tls"]
        require(
            isinstance(tls, dict)
            and set(tls) == {"host", "port", "endpoint", "certfile", "keyfile", "ca_file", "ca_sha256"},
            "invalid TLS control configuration",
        )
        protected_endpoint(tls["endpoint"])
        require(
            tls["endpoint"].startswith("https://") and type(tls["port"]) is int and 1 <= tls["port"] <= 65535,
            "remote control requires verified HTTPS",
        )
    options = config["download"]
    require(
        isinstance(options, dict)
        and options.get("control_method")
        in ("paarc-base-v2", "gradient-candidate-v1", "fixed-v1", "ratio-v1"),
        "explicit versioned method required",
    )
    forbidden = {
        "input_path",
        "output_folder",
        "force_overwrite",
        "resume",
        "reconcile",
        "shared_control_file",
        "research_profile",
        "create_tar",
        "create_overview",
        "enable_paarc",
    }
    require(not forbidden.intersection(options), "profile-owned download fields cannot be overridden")
    download = normalize_config(
        Config(
            input_path="partition.parquet",
            output_folder="return/native",
            shared_control_file="control-private.json",
            research_profile=True,
            create_tar=True,
            create_overview=True,
            enable_paarc=True,
            **options,
        )
    )
    require(download.url_col == "url", "research profile requires url column")
    return config, download


def prepare(value):
    """Pure validation, including metadata and package bytes, before any mutation."""
    config, download = validate_config(value)
    truth = Truth.load(config["original_manifest"], parse(Path(config["catalog"]).read_bytes()))
    package = Path(config["environment_archive"]).resolve()
    require(
        package.is_file() and file_digest(package) == config["environment_sha256"],
        "environment hash mismatch",
    )
    output = Path(config["output_directory"]).absolute()
    require(not output.exists() and not output.is_symlink(), "refusing output-directory collision")
    require(all(not p.is_symlink() for p in output.parents), "symlink output parent")
    if "control_tls" in config:
        tls = config["control_tls"]
        raw = Path(tls["ca_file"]).read_bytes()
        require(len(raw) <= 16384 and digest(raw) == tls["ca_sha256"], "control CA hash mismatch")
        from flowdc_shared import control_tls_context

        control_tls_context({"ca_pem": raw.decode("ascii")})
    sources = {name: (ROOT / name).read_bytes() for name in WORKER_FILES}
    return config, download, truth, package, output, sources


class Reconciler:
    """Credit only independent verification, once per original logical row."""

    def __init__(self, truth):
        self.truth, self.rows = truth, initial_outcomes(truth)
        self.attempts, self.returns, self.errors = {}, [], []

    def accept(self, directory, archive, spec, native, clients):
        record = {"scope_id": spec["scope_id"], "native": native, "accepted": False}
        self.returns.append(record)
        try:
            identity, receipt, bundle_hash = unpack_return(archive, directory)
            attempt = identity["attempt_id"]
            require(
                isinstance(attempt, str) and re.fullmatch(r"[0-9a-f]{32}", attempt),
                "invalid physical attempt",
            )
            expected = {
                "schema": "flowdc-vine-attempt-v1",
                "scope_id": spec["scope_id"],
                "binding": spec["binding"],
                "partition_sha256": spec["partition_sha256"],
                "row_ids": spec["row_ids"],
                "source_files": spec["files"],
                "environment_sha256": spec["environment_sha256"],
            }
            require(all(identity.get(k) == v for k, v in expected.items()), "returned task identity mismatch")
            require(
                receipt["schema"] == "flowdc-vine-receipt-v1"
                and receipt["attempt_id"] == attempt
                and receipt["scope_id"] == spec["scope_id"],
                "receipt identity mismatch",
            )
            require(
                attempt in clients and clients[attempt]["scope"] == spec["scope_id"],
                "unenrolled returned execution",
            )
            old = self.attempts.get(attempt)
            require(old is None or old == bundle_hash, "conflicting return for physical attempt")
            record.update(attempt_id=attempt, bundle_sha256=bundle_hash, receipt=receipt, identity=identity)
            if old is not None:
                record.update(accepted=True, duplicate=True)
                return record
            self.attempts[attempt] = bundle_hash
            verified = verify_native(
                "flowdc", Path(directory) / "native", partition_truth(self.truth, spec["row_ids"])
            )
            write_new(Path(directory) / "verification.json", verified)
            record.update(verification=verified, accepted=verified["artifacts_valid"], duplicate=False)
            require(
                {row["row_id"] for row in verified["rows"]} == set(spec["row_ids"]),
                "return row membership mismatch",
            )
            for row in verified["rows"]:
                current = self.rows[row["row_id"]]
                if current["disposition"] == "verified":
                    # Both attempts were checked against the same independent bytes/metadata.
                    if row["disposition"] == "verified":
                        require(current["useful_bytes"] == row["useful_bytes"], "conflicting useful credit")
                elif row["disposition"] == "verified" or current["disposition"] == "missing":
                    self.rows[row["row_id"]] = row
            if not verified["artifacts_valid"]:
                self.errors.append("invalid required native artifacts")
        except (ValueError, KeyError, TypeError, OSError) as exc:
            record["error_type"] = type(exc).__name__
            self.errors.append(str(exc))
        return record

    def summary(self):
        rows = list(self.rows.values())
        count = sum(row["disposition"] == "verified" for row in rows)
        return {
            "original_rows": self.truth["original_rows"],
            "verified_rows": count,
            "useful_bytes": sum(row["useful_bytes"] for row in rows),
            "verified_coverage": count / self.truth["original_rows"],
            "rows": rows,
            "returns": self.returns,
            "errors": self.errors,
        }


def dispatch_records(run_info):
    """Native RUNNING means dispatch, not proof of child execution or HTTP work."""
    records = []
    for path in sorted(Path(run_info).rglob("transactions")):
        for line in path.read_text(errors="replace").splitlines():
            fields = line.split()
            if len(fields) >= 6 and fields[2] == "TASK" and fields[4] == "RUNNING":
                records.append(
                    {
                        "task_id": int(fields[3]),
                        "worker_id": fields[5],
                        "raw": line,
                        "log": str(path.relative_to(run_info)),
                    }
                )
    return records


async def run(value, *, ready=None, pulse=None, engineering_fault=None, owned_cohort=None):
    """Native manager entrypoint. Hooks only instrument bounded local fixtures."""
    import ndcctools.taskvine as vine

    config, download, truth, package, root, sources = prepare(value)
    owned_cohort = validate_cohort(owned_cohort, config["workers"])
    require(vine.cvine.vine_version_string() == RUNTIME, "shared profile requires TaskVine " + RUNTIME)
    require(
        engineering_fault in (None, "partial-artifact", "preconnect-pause", "sandbox-exhaustion", "forsaken"),
        "unknown engineering fault",
    )
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    write_new(root / "cohort.json", owned_cohort)
    eligible = truth.write(root / "fixture")
    write_new(
        root / "effective.json", {**config, "download": dataclasses.asdict(download), "runtime": RUNTIME}
    )
    source_dir = root / "source"
    source_dir.mkdir()
    for name, raw in sources.items():
        (source_dir / name).write_bytes(raw)
    frame = pl.read_parquet(eligible)
    password = (
        Path(config["native_password_file"]) if "native_password_file" in config else root / "native-password"
    )
    if "native_password_file" in config:
        fd = private_file(password, writable=False)
        with os.fdopen(fd, "rb") as stream:
            require(
                re.fullmatch(rb"[0-9a-f]{64}\n", stream.read(66)) is not None,
                "invalid run-private native credential",
            )
    else:
        with password.open("x") as stream:
            os.chmod(password, 0o600)
            stream.write(secrets.token_hex(32) + "\n")
    authority = Authority(root / "authority", download)
    reconciler = Reconciler(truth.record)
    tasks, returns = {}, []
    manager = None
    status, error_type = "failed", None
    native_cleanup = None
    started = time.monotonic_ns()
    verified_ns = 0
    try:
        if "control_tls" in config:
            tls = config["control_tls"]
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.minimum_version = ssl.TLSVersion.TLSv1_2
            fd = private_file(tls["keyfile"], writable=False)
            os.close(fd)
            context.load_cert_chain(tls["certfile"], tls["keyfile"])
            await authority.start(
                host=tls["host"], port=tls["port"], ssl_context=context, endpoint=tls["endpoint"]
            )
        else:
            await authority.start()
        manager = NativeManager(
            {
                "port": config["port_number"],
                "root": str(root),
                "password": str(password),
                "package": str(package),
            }
        )
        await manager.start()
        for i in range(min(config["workers"], frame.height)):
            directory = root / f"partition-{i}"
            directory.mkdir(mode=0o700)
            part = frame[i :: config["workers"]]
            part.write_parquet(directory / "partition.parquet")
            ids, scope = sorted(part[PROVENANCE[3]].to_list()), uuid4().hex
            ca_pem = (
                Path(config["control_tls"]["ca_file"]).read_text("ascii") if "control_tls" in config else None
            )
            authority.enroll(
                scope, ids, directory / "control-private.json", attempts=config["max_attempts"], ca_pem=ca_pem
            )
            spec = {
                "schema": "flowdc-vine-task-v1",
                "control_ca_sha256": digest(ca_pem.encode("ascii")) if ca_pem is not None else None,
                "scope_id": scope,
                "row_ids": ids,
                "binding": authority.ledger.current()["binding"],
                "files": {name: digest(raw) for name, raw in sources.items()},
                "download": dataclasses.asdict(download),
                "environment_sha256": config["environment_sha256"],
                "partition_sha256": file_digest(directory / "partition.parquet"),
                "deadline_s": config["task_deadline_s"],
                "engineering_fault": engineering_fault if i == 0 else None,
            }
            write_new(directory / "task.json", spec)
            response = await manager.call(
                "submit",
                directory=str(directory),
                spec=spec,
                feature=owned_cohort["slots"][i]["feature"],
            )
            identifier = response["task_id"]
            tasks[identifier] = (directory, spec)
            write_new(directory / "submission.json", {"native_task_id": identifier, "scope_id": scope})
        if ready:
            await ready(manager, authority, password)
        pending = set(tasks)
        while pending:
            if (time.monotonic_ns() - started) / 1e9 >= config["deadline_s"]:
                status = "timed_out"
                break
            if pulse:
                await pulse(manager, authority, tasks)
            native = await manager.call("wait")
            if native is None:
                continue
            identifier = native["task_id"]
            require(identifier in pending, "unexpected native task return")
            pending.remove(identifier)
            directory, spec = tasks[identifier]
            stdout = native.pop("stdout")
            write_new(directory / "native.json", native)
            (directory / "native-stdout.txt").write_text(stdout)
            before = time.monotonic_ns()
            returns.append(
                reconciler.accept(
                    directory / "returned",
                    directory / "return.tar",
                    spec,
                    native,
                    authority.ledger.current()["clients"],
                )
            )
            returned = returns[-1]
            if "identity" in returned and native["result"] == "success":
                # The packaged worker executes acquisition in this one process.
                # Successful native receipt proves that specific process exited;
                # it says nothing about earlier lost attempts of the same task.
                attempt = returned["identity"]["attempt_id"]
                authority.ledger.prove_quiescent(
                    attempt,
                    {
                        "kind": "native_task_exit",
                        "client_id": attempt,
                        "run_id": spec["binding"]["run_id"],
                        "source_sha256": spec["binding"]["source_sha256"],
                        "evidence_sha256": digest(
                            (directory / "native.json").read_bytes() + returned["bundle_sha256"].encode()
                        ),
                    },
                )
            verified_ns += time.monotonic_ns() - before
        else:
            status = "returned"
    except BaseException as exc:
        error_type = type(exc).__name__
        status = "interrupted" if isinstance(exc, (KeyboardInterrupt, asyncio.CancelledError)) else "failed"
    finally:
        # Cancel/shutdown are requests, never evidence that uncertain HTTP stopped.
        if manager is not None:
            native_cleanup = await manager.close()
            write_new(root / "native-cleanup.json", native_cleanup)
        await authority.stop()
        exported = authority.ledger.export()
        write_new(root / "control.json", exported)
        authority.ledger.close()
    before = time.monotonic_ns()
    summary = reconciler.summary()
    dispatches = dispatch_records(root / "run-info")
    bound = dispatch_audit(owned_cohort, tasks, dispatches)
    summary.update(
        schema="flowdc-distributed-result-v1",
        method=download.control_method,
        binding=exported["state"]["binding"],
        environment_sha256=config["environment_sha256"],
        status=status,
        error_type=error_type,
        dispatches=dispatches,
        native_dispatch_bound=bound,
        acquisition_attempts=exported["state"]["clients"],
        attempt_accounting="native dispatches and admitted clients are distinct; pre-connect execution may be unobserved",
        acquisition_complete=status == "returned"
        and len(returns) == len(tasks)
        and all(
            r["accepted"] and r["native"]["successful"] and r.get("receipt", {}).get("status") == "returned"
            for r in returns
        )
        and all(row["disposition"] in ("verified", "skipped") for row in summary["rows"])
        and not summary["errors"],
    )
    control_complete, native_complete = control_closure(exported["state"], native_cleanup)
    summary.update(
        control_complete=control_complete,
        native_complete=native_complete,
        run_complete=summary["acquisition_complete"]
        and control_complete
        and native_complete
        and bound["within_bound"],
    )
    write_new(root / "outcomes.json", summary)
    # Required reconciliation/index closure belongs to this boundary; rendering does not.
    parsed = parse((root / "outcomes.json").read_bytes())
    require(parsed == summary, "closed distributed index mismatch")
    end = time.monotonic_ns()
    timing = {
        "end_to_end_ns": end - started,
        "verification_ns": verified_ns + end - before,
        "boundary": "manager creation through native receipt and closed verified outcome index",
    }
    write_new(root / "timing.json", timing)
    return {**summary, **timing}


def control_closure(state, native_cleanup):
    control_complete = not Ledger.outstanding(state) and all(
        client["status"] == "closed" for client in state["clients"].values()
    )
    native_complete = (
        isinstance(native_cleanup, dict)
        and type(native_cleanup.get("native_manager_exit")) is int
        and native_cleanup["native_manager_exit"] == 0
        and native_cleanup.get("shutdown_receipt") is True
    )
    return control_complete, native_complete


def cli(value, *, dry_run=False, owned_cohort=None):
    if dry_run:
        config, _, truth, _, _, _ = prepare(value)
        print(
            json.dumps(
                {
                    "profile": PROFILE,
                    "original_rows": truth.record["original_rows"],
                    "workers": config["workers"],
                    "runtime": RUNTIME,
                    "submitted": False,
                }
            )
        )
        return 0
    if owned_cohort is None:
        require(
            "local_worker_binary" in value, "use an owned local worker binary or the prepared guest workflow"
        )
        result = asyncio.run(run_local(value))
    else:
        require(owned_cohort["owner"] == "prepared-guest-service-v1", "unexpected CLI cohort owner")
        result = asyncio.run(run(value, owned_cohort=owned_cohort))
    print(
        json.dumps(
            {
                key: result[key]
                for key in ("status", "run_complete", "original_rows", "verified_rows", "useful_bytes")
            }
        )
    )
    return 0 if result["run_complete"] else 2


async def run_local(value):
    """Maintained CLI owns every local worker; no external worker/factory profile."""
    import subprocess

    from flowdc_vine_cohort import cohort
    from flowdc_vine_ownership import OwnedWorkers

    config, _, _, _, root, _ = prepare(value)
    require("control_tls" not in config, "local owned workers require loopback control")
    executable = Path(config["local_worker_binary"])
    require(executable.is_absolute() and executable.is_file(), "absolute official worker binary required")
    version = subprocess.run([str(executable), "--version"], capture_output=True, timeout=5, check=True)
    require(RUNTIME in (version.stdout + version.stderr).decode().split(), "worker runtime mismatch")
    directory = root.with_name(root.name + "-workers")
    require(not directory.exists() and not directory.is_symlink(), "owned worker output collision")
    directory.mkdir(mode=0o700, parents=True)
    owned = OwnedWorkers(directory, executable, config["workers"])
    plan = cohort(config["workers"])
    owned.bind_cohort(plan)
    write_new(directory / "runtime.json", {"version": RUNTIME, "binary_sha256": file_digest(executable)})
    watch = asyncio.create_task(owned.watch())

    async def ready(manager, authority, password):
        for slot in plan["slots"]:
            owned.start(manager.port, password, slot["feature"])

    try:
        return await run(value, ready=ready, owned_cohort=plan)
    finally:
        try:
            write_new(directory / "cleanup.json", await owned.close())
        finally:
            owned.stopping = True
            await watch
