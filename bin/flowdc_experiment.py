#!/usr/bin/env python3
"""Prepare, explicitly execute and recover bounded three-role FLOW-DC experiments."""

import argparse
import os
import signal
import sys
import tarfile
import time
from pathlib import Path
from uuid import uuid4

import flowdc_ops as ops
from flowdc_experiment_artifacts import bundle, members
from flowdc_experiment_data import (
    LIMIT,
    ROLES,
    ExperimentError,
    Store,
    case_config,
    digest,
    encode,
    fields,
    parquet_rows,
    parse,
    read_file,
    require,
    run_id,
    specification,
)
from flowdc_experiment_fixture import generate
from flowdc_experiment_process import CANCEL_CHECK, check_cancel, execute
from flowdc_experiment_source import SourceError, read_source
from flowdc_experiment_transport import Controller, Transport, binding, clean, ready, remaining, ssh_preflight
from flowdc_pilot_journal import Journal


def prepare(path):
    raw_spec = read_file(path, 262144, private=True)
    spec = specification(parse(raw_spec))
    record = Journal(spec["state_root"]).read()
    require(record["registration_id"] == spec["registration_id"], "registration_changed")
    source, files = read_source(spec["source"]["repository"], spec["source"]["revision"])
    cases = []
    originals = {}
    for case in spec["cases"]:
        raw = read_file(case["config"], 262144)
        config = case_config(parse(raw))
        cases.append({"name": case["name"], "config": config})
        originals[case["name"]] = raw
        files[f"configs/{case['name']}.json"] = encode(config)
    if spec["fixture"]:
        require(
            len(cases) == 2 and {case["config"]["enable_paarc"] for case in cases} == {False, True},
            "fixture_requires_both_paarc_modes",
        )
        require(all(case["config"].get("url_col", "url") == "url" for case in cases), "fixture_url_column")
        generated, partitions = generate(
            record["access"]["interfaces"]["origin"]["fixed_ip"], [case["name"] for case in cases]
        )
        files.update(generated)
    else:
        partitions = {case["name"]: [] for case in cases}
        for index, part in enumerate(spec["partitions"]):
            raw = read_file(part["path"])
            filename = f"part-{index:03}.parquet"
            for case in cases:
                require(
                    parquet_rows(raw, case["config"].get("url_col", "url")) == part["rows"],
                    "partition_row_count_mismatch",
                )
                files[f"inputs/{case['name']}/{filename}"] = raw
                selected = {"name": filename, "rows": part["rows"]}
                if "expected_sha256" in part:
                    selected["expected_sha256"] = part["expected_sha256"]
                partitions[case["name"]].append(selected)
    helper = Path(__file__).with_name("flowdc_experiment_guest.py").read_bytes()
    files["guest.py"] = helper
    files["guest.json"] = encode(
        {
            "python": spec["guest"]["python"],
            "worker": spec["guest"]["worker"],
            "service_mode": spec["guest"]["service_mode"],
            "bounds": spec["bounds"],
            "cases": [c["name"] for c in cases],
            "addresses": {role: record["access"]["interfaces"][role]["fixed_ip"] for role in ROLES},
        }
    )
    require(sum(map(len, files.values())) < LIMIT - 1048576, "staging_size_limit")
    archive = bundle(files)
    require(len(archive) <= LIMIT, "staging_size_limit")
    manifest = {
        "schema_version": 1,
        "spec": spec,
        "source": source,
        "binding": binding(record),
        "known_hosts_sha256": ssh_preflight(spec["ssh"], binding(record)),
        "cases": cases,
        "partitions": partitions,
        "files": {},
        "bundle_sha256": digest(archive),
        "original_spec_sha256": digest(raw_spec),
        "original_configs_sha256": {name: digest(raw) for name, raw in originals.items()},
    }
    store = Store(spec["state_root"])
    with store.lock():
        selected = store.create()
        # A failed prepare deliberately preserves its partial directory. It has no
        # activation intent, cannot execute, and never overwrites a previous run.
        for index, (path, content) in enumerate(sorted(files.items())):
            local = "guest.py" if path == "guest.py" else f"staged-{index:04}.bin"
            store.write(selected, local, content)
            manifest["files"][path] = {"local": local, "bytes": len(content), "sha256": digest(content)}
        for name, raw in originals.items():
            store.write(selected, f"original-case-{name}.json", raw)
        store.write(selected, "original-spec.json", raw_spec)
        store.write(selected, "bundle.tar", archive)
        manifest["run_id"] = selected
        raw = encode(manifest)
        store.write(selected, "manifest.json", raw)
        store.write(
            selected,
            "state.json",
            encode(
                {
                    "schema_version": 1,
                    "run_id": selected,
                    "manifest_sha256": digest(raw),
                    "binding": manifest["binding"],
                    "phase": "prepared",
                    "activation_intent": False,
                    "workload": "not_run",
                    "cleanup": "not_requested",
                    "guest_cleanup": "not_requested",
                    "errors": [],
                    "collected": {},
                    "services": [],
                    "addresses": None,
                    "events": [{"phase": "prepared", "utc": time.time()}],
                }
            ),
        )
    return {"run_id": selected, "phase": "prepared", "source_commit": source["commit"]}


def load(store, selected, *, verify=True):
    state = store.json(selected, "state.json")
    require(state.get("schema_version") == 1 and state.get("run_id") == selected, "invalid_run_state")
    raw = store.read(selected, "manifest.json")
    require(digest(raw) == state.get("manifest_sha256"), "manifest_changed")
    manifest = parse(raw)
    require(
        manifest.get("schema_version") == 1
        and manifest.get("run_id") == selected
        and manifest.get("binding") == state.get("binding"),
        "invalid_manifest",
    )
    specification(manifest["spec"])
    require(manifest["spec"]["state_root"] == str(store.root), "state_root_changed")
    require(manifest["spec"]["registration_id"] == manifest["binding"]["registration_id"], "invalid_manifest")
    if verify:
        require(
            digest(store.read(selected, "original-spec.json")) == manifest["original_spec_sha256"],
            "staged_content_changed",
        )
        for name, expected in manifest["original_configs_sha256"].items():
            require(
                digest(store.read(selected, f"original-case-{name}.json")) == expected,
                "staged_content_changed",
            )
        for item in manifest["files"].values():
            fields(item, ("local", "bytes", "sha256"))
            require(Path(item["local"]).name == item["local"], "invalid_manifest")
            content = store.read(selected, item["local"])
            require(
                len(content) == item["bytes"] and digest(content) == item["sha256"], "staged_content_changed"
            )
        archive = store.read(selected, "bundle.tar")
        require(digest(archive) == manifest["bundle_sha256"], "staged_content_changed")
        contents = members(archive, LIMIT)
        require(set(contents) == set(manifest["files"]), "bundle_manifest_mismatch")
        require(
            all(digest(contents[path]) == item["sha256"] for path, item in manifest["files"].items()),
            "bundle_manifest_mismatch",
        )
    return manifest, state


def public(state):
    return {key: state[key] for key in ("run_id", "phase", "workload", "cleanup", "guest_cleanup", "errors")}


def save(store, selected, state, phase=None):
    if phase:
        state["phase"] = phase
        state["events"].append({"phase": phase, "utc": time.time()})
    store.save(selected, "state.json", state)


def error_code(exc):
    if isinstance(exc, ops.OpsError):
        return exc.code
    if isinstance(exc, (ExperimentError, SourceError)):
        return str(exc)
    if isinstance(exc, KeyboardInterrupt):
        return "experiment_interrupted"
    return "experiment_operation_failed"


def collect_outputs(store, selected, manifest, state, transport, deadline):
    maximum = manifest["spec"]["bounds"]["output_bytes"]
    failures = []

    def acquire(name, role, case="all"):
        previous = state["collected"].get(name)
        if previous and previous["valid"]:
            raw = store.read(selected, previous["file"], maximum)
            require(digest(raw) == previous["sha256"], "collected_content_changed")
            return raw
        require(transport is not None, "guest_collection_unavailable")
        raw = transport.call(
            role, "collect", case, seconds=deadline - time.monotonic(), maximum=maximum, extra=(maximum,)
        )
        filename = f"artifacts-{name}-{uuid4().hex}.tar"
        # New snapshots preserve previous partial evidence instead of overwriting it.
        store.write(selected, filename, raw)
        state["collected"][name] = {"file": filename, "sha256": digest(raw), "valid": False}
        save(store, selected, state)
        return raw

    def validate(raw, kind, **expected):
        code, response = execute(
            [sys.executable, str(Path(__file__).with_name("flowdc_experiment_artifacts.py"))],
            seconds=deadline - time.monotonic(),
            maximum=LIMIT,
            data=encode({"kind": kind, "maximum": maximum, **expected}) + raw,
        )
        value = parse(response)
        if code:
            raise ExperimentError(value.get("error", "artifact_validation_failed"))
        return value["result"]

    for case in manifest["cases"]:
        name = case["name"]
        try:
            raw = acquire(name, "manager", name)
            reports = validate(raw, "case", case=case, partitions=manifest["partitions"][name])
            store.save(selected, f"validation-{name}.json", reports)
            state["collected"][name]["valid"] = True
            save(store, selected, state)
        except Exception as exc:
            failures.append(error_code(exc))
    for case in manifest["cases"]:
        key = case["name"] + "-worker"
        try:
            worker_raw = acquire(key, "worker", case["name"])
            validate(worker_raw, "worker")
            state["collected"][key]["valid"] = True
        except Exception as exc:
            failures.append(error_code(exc))
    if manifest["spec"]["fixture"]:
        try:
            raw = acquire("origin", "origin")
            report = validate(
                raw,
                "origin",
                cases=manifest["cases"],
                worker=manifest["binding"]["access"]["interfaces"]["worker"]["fixed_ip"],
            )
            store.save(selected, "validation-origin.json", report)
            state["collected"]["origin"]["valid"] = True
        except Exception as exc:
            failures.append(error_code(exc))
    state["workload"] = "failed" if failures else "passed"
    state["errors"] = list(dict.fromkeys(state["errors"] + failures))
    save(store, selected, state)


def cleanup(store, selected, manifest, state, controller, transport=None):
    deadline = time.monotonic() + manifest["spec"]["bounds"]["stop_seconds"]
    try:
        save(store, selected, state, "stopping")
    except Exception:
        state["errors"].append("cleanup_record_write_failed")
    try:
        # Request cloud stop BEFORE trying any guest operation. Failure or hanging
        # guest collection cannot delay this bounded independent cleanup path.
        controller.call("stop", seconds=min(10, deadline - time.monotonic()))
    except Exception as exc:
        state["errors"].append(error_code(exc))
    guest_ok = all(service.get("stopped") for service in state["services"])
    if transport is not None:
        guest_ok = True
        # Cloud stop has already been requested. Share half the remaining stop
        # window among outstanding services, reserving the rest for cloud proof.
        now = time.monotonic()
        guest_deadline = now + max(0, deadline - now) / 2
        outstanding = [s for s in reversed(state["services"]) if not s.get("stopped")]
        for index, service in enumerate(outstanding):
            try:
                transport.call(
                    service["role"],
                    "stop",
                    service["case"],
                    seconds=(guest_deadline - time.monotonic()) / (len(outstanding) - index),
                )
                service["stopped"] = True
            except Exception:
                guest_ok = False
    state["guest_cleanup"] = "verified" if guest_ok else "uncertain"
    state["cleanup"] = "uncertain"
    while time.monotonic() < deadline:
        try:
            value = controller.call("status", seconds=min(10, deadline - time.monotonic()))
            if clean(value):
                state["cleanup"] = "verified"
                break
        except Exception as exc:
            if error_code(exc) not in state["errors"]:
                state["errors"].append(error_code(exc))
        time.sleep(min(1, max(0, deadline - time.monotonic())))
    complete = state["cleanup"] == "verified" and state["guest_cleanup"] == "verified"
    save(store, selected, state, "finished" if complete else "cleanup_incomplete")
    if complete:
        store.release(selected)


def run(store, selected, manifest, state):
    require(state["phase"] == "prepared" and not state["activation_intent"], "run_cannot_be_replayed")
    require(store.owner() in (None, selected), "another_experiment_incomplete")
    controller = Controller(store.root, manifest["binding"])
    bounds = manifest["spec"]["bounds"]
    controller.preflight(bounds["window_seconds"])
    require(
        ssh_preflight(manifest["spec"]["ssh"], manifest["binding"]) == manifest["known_hosts_sha256"],
        "enrolled_host_keys_changed",
    )
    store.claim(selected)
    transport = None

    def cancellation():
        try:
            request = store.json(selected, "cancel.json")
        except FileNotFoundError:
            return
        require(
            request == {"run_id": selected, "manifest_sha256": state["manifest_sha256"]},
            "invalid_cancellation",
        )
        raise ExperimentError("cancellation_requested")

    cancel_token = CANCEL_CHECK.set(cancellation)
    old_handlers = {}

    def interrupted(signum, frame):
        # A second signal must not interrupt the bounded cleanup attempt.
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, signal.SIG_IGN)
        raise KeyboardInterrupt

    for sig in (signal.SIGINT, signal.SIGTERM):
        old_handlers[sig] = signal.signal(sig, interrupted)
    try:
        check_cancel()
        state["activation_intent"] = True
        save(store, selected, state, "activation_intent")
        start = time.monotonic()
        work_end = start + bounds["stop_after_seconds"] - bounds["collect_seconds"]
        controller.call(
            "start", seconds=min(10, work_end - time.monotonic()), window=bounds["window_seconds"]
        )
        while True:
            value = controller.call("status", seconds=min(10, work_end - time.monotonic()))
            require(not value.get("checkpoint"), "controller_checkpoint")
            if ready(value):
                break
            require(time.monotonic() < work_end, "startup_deadline_expired")
            time.sleep(1)
        work_end = min(work_end, time.monotonic() + remaining(value) - bounds["collect_seconds"])
        require(
            work_end - time.monotonic()
            >= bounds["deployment_min_seconds"] + len(manifest["cases"]) * bounds["min_case_seconds"],
            "insufficient_work_time",
        )
        addresses = controller.addresses(min(bounds["phase_seconds"], work_end - time.monotonic()))
        state["addresses"] = addresses
        save(store, selected, state, "deploying")
        transport = Transport(store, selected, manifest, addresses)
        for role in ROLES:
            environment = transport.call(
                role,
                "probe",
                seconds=min(
                    bounds["phase_seconds"],
                    work_end - time.monotonic() - len(manifest["cases"]) * bounds["min_case_seconds"],
                ),
                extra=(
                    manifest["spec"]["guest"]["worker"],
                    bounds["disk_mb"],
                    manifest["spec"]["guest"]["service_mode"],
                ),
            )
            store.write(selected, f"environment-{role}.json", encode(environment))
        raw = store.read(selected, "bundle.tar")
        for role in ROLES:
            transport.call(
                role,
                "deploy",
                seconds=min(
                    bounds["phase_seconds"],
                    work_end - time.monotonic() - len(manifest["cases"]) * bounds["min_case_seconds"],
                ),
                data=raw,
                extra=(manifest["bundle_sha256"], LIMIT),
            )

        def launch(role, case, seconds):
            # Lost service-start acknowledgements remain owned cleanup obligations.
            state["services"].append({"role": role, "case": case})
            save(store, selected, state)
            transport.call(
                role,
                "launch",
                case,
                seconds=min(20, work_end - time.monotonic()),
                extra=(max(1, int(seconds)),),
            )

        if manifest["spec"]["fixture"]:
            launch("origin", "all", work_end - time.monotonic() + bounds["collect_seconds"])
        save(store, selected, state, "running")
        for index, case in enumerate(manifest["cases"]):
            value = controller.call("status", seconds=min(10, work_end - time.monotonic()))
            require(ready(value), "controller_not_active")
            work_end = min(work_end, time.monotonic() + remaining(value) - bounds["collect_seconds"])
            case_budget = min(
                bounds["phase_seconds"], (work_end - time.monotonic()) / (len(manifest["cases"]) - index)
            )
            require(case_budget >= bounds["min_case_seconds"], "insufficient_work_time")
            end = time.monotonic() + case_budget
            launch("manager", case["name"], end - time.monotonic())
            launch("worker", case["name"], end - time.monotonic())
            while True:
                require(time.monotonic() < end, "case_deadline_expired")
                result = transport.call(
                    "manager", "status", case["name"], seconds=min(10, end - time.monotonic())
                )
                if result.get("ActiveState") in ("inactive", "failed"):
                    require(
                        result.get("Result") == "success" and result.get("ExecMainStatus") == "0",
                        "manager_failed",
                    )
                    next(
                        s for s in state["services"] if s["role"] == "manager" and s["case"] == case["name"]
                    )["stopped"] = True
                    save(store, selected, state)
                    break
                require(time.monotonic() < end, "case_deadline_expired")
                time.sleep(min(1, max(0, end - time.monotonic())))
            require(time.monotonic() < work_end, "work_deadline_expired")
            transport.call("worker", "stop", case["name"], seconds=min(10, work_end - time.monotonic()))
            next(s for s in state["services"] if s["role"] == "worker" and s["case"] == case["name"])[
                "stopped"
            ] = True
            save(store, selected, state)
        if manifest["spec"]["fixture"]:
            transport.call(
                "origin", "stop", seconds=min(10, start + bounds["stop_after_seconds"] - time.monotonic())
            )
            next(s for s in state["services"] if s["role"] == "origin")["stopped"] = True
            save(store, selected, state)
        save(store, selected, state, "collecting")
        collect_outputs(
            store,
            selected,
            manifest,
            state,
            transport,
            min(start + bounds["stop_after_seconds"], time.monotonic() + bounds["collect_seconds"]),
        )
    except BaseException as exc:
        state["errors"].append(error_code(exc))
        state["workload"] = "failed"
        save(store, selected, state, "interrupted_or_failed")
        if transport is not None and error_code(exc) not in (
            "cancellation_requested",
            "experiment_interrupted",
        ):
            try:
                collect_outputs(
                    store,
                    selected,
                    manifest,
                    state,
                    transport,
                    min(start + bounds["stop_after_seconds"], time.monotonic() + bounds["collect_seconds"]),
                )
            except Exception:
                pass
    finally:
        CANCEL_CHECK.reset(cancel_token)
        for sig in old_handlers:
            signal.signal(sig, signal.SIG_IGN)
        try:
            cleanup(store, selected, manifest, state, controller, transport)
        finally:
            for sig, handler in old_handlers.items():
                signal.signal(sig, handler)
    return public(state)


def cancel_active(store, selected):
    # The running process retains the registration lock. This per-run request is
    # an atomic mailbox, never a PID signal or an unscoped controller stop.
    manifest, state = load(store, selected, verify=False)
    require(store.owner() == selected, "another_experiment_incomplete")
    store.save(selected, "cancel.json", {"run_id": selected, "manifest_sha256": state["manifest_sha256"]})
    deadline = time.monotonic() + min(30, manifest["spec"]["bounds"]["stop_seconds"] + 5)
    while time.monotonic() < deadline:
        state = store.json(selected, "state.json")
        if state["phase"] in ("finished", "cleanup_incomplete", "cancelled_before_activation"):
            return public(state)
        time.sleep(0.1)
    value = public(state)
    value["cancellation_requested"] = True
    return value


def operate(args):
    if args.command == "prepare":
        return prepare(args.spec)
    selected = run_id(args.run_id)
    store = Store(args.state_root)
    if args.command == "status":
        _, state = load(store, selected)
        return public(state)
    try:
        with store.lock():
            return operate_locked(args, store, selected)
    except ExperimentError as exc:
        if args.command == "stop" and str(exc) == "experiment_busy":
            return cancel_active(store, selected)
        raise


def operate_locked(args, store, selected):
    manifest, state = load(store, selected, verify=args.command != "stop")
    if args.command == "run":
        return run(store, selected, manifest, state)
    if args.command == "stop" and not state["activation_intent"]:
        require(not state["services"], "invalid_run_state")
        if store.owner() == selected:
            store.release(selected)
        state.update(cleanup="verified", guest_cleanup="verified")
        save(store, selected, state, "cancelled_before_activation")
        return public(state)
    require(state["activation_intent"], "experiment_not_started")
    if args.command == "stop" and state["cleanup"] == "verified" and state["guest_cleanup"] == "verified":
        return public(state)
    require(store.owner() in (selected, None), "another_experiment_incomplete")
    if args.command == "stop":
        store.claim(selected)
    required = [case["name"] for case in manifest["cases"]] + [
        case["name"] + "-worker" for case in manifest["cases"]
    ]
    if manifest["spec"]["fixture"]:
        required.append("origin")
    if args.command == "collect" and all(state["collected"].get(key, {}).get("valid") for key in required):
        collect_outputs(
            store,
            selected,
            manifest,
            state,
            None,
            time.monotonic() + manifest["spec"]["bounds"]["collect_seconds"],
        )
        return public(state)
    controller = Controller(store.root, manifest["binding"])
    transport = None
    if state["addresses"] is not None:
        try:
            transport = Transport(store, selected, manifest, state["addresses"])
        except Exception:
            if args.command != "stop":
                raise
    if args.command == "stop":
        cleanup(store, selected, manifest, state, controller, transport)
    else:
        collect_outputs(
            store,
            selected,
            manifest,
            state,
            transport,
            time.monotonic() + manifest["spec"]["bounds"]["collect_seconds"],
        )
    return public(state)


class ArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        raise ExperimentError("invalid_arguments")


def main(argv=None):
    parser = ArgumentParser(description=__doc__, allow_abbrev=False)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare_parser = commands.add_parser(
        "prepare", help="Stage a new offline run; never activate VMs.", allow_abbrev=False
    )
    prepare_parser.add_argument("--spec", required=True)
    for action in ("run", "status", "collect", "stop"):
        command = commands.add_parser(action, allow_abbrev=False)
        command.add_argument("--state-root", required=True)
        command.add_argument("--run-id", required=True)
    try:
        args = parser.parse_args(argv)
    except ExperimentError:
        print(
            encode(
                {"schema_version": 1, "operation": "unknown", "status": "error", "error": "invalid_arguments"}
            ).decode(),
            end="",
        )
        return 2
    mask = os.umask(0o077)
    try:
        value = operate(args)
        successful = (
            args.command in ("prepare", "status")
            or (
                args.command == "stop"
                and value["cleanup"] == "verified"
                and value["guest_cleanup"] == "verified"
            )
            or (
                value["workload"] == "passed"
                and value["cleanup"] == "verified"
                and value["guest_cleanup"] == "verified"
                and not value["errors"]
            )
        )
        print(
            encode(
                {
                    "schema_version": 1,
                    "operation": args.command,
                    "status": "ok" if successful else "incomplete",
                    "data": value,
                }
            ).decode(),
            end="",
        )
        return 0 if successful else 3
    except (
        ExperimentError,
        SourceError,
        ops.OpsError,
        OSError,
        ValueError,
        KeyError,
        TypeError,
        tarfile.TarError,
    ) as exc:
        print(
            encode(
                {"schema_version": 1, "operation": args.command, "status": "error", "error": error_code(exc)}
            ).decode(),
            end="",
        )
        return 2
    finally:
        os.umask(mask)


if __name__ == "__main__":
    sys.exit(main())
