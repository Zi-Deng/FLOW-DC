"""Prepared bounded guest bridge to the versioned shared-origin TaskVine path."""

import asyncio
import base64
import json
import math
import re
import signal
import sys
from pathlib import Path
from urllib.parse import urlsplit

from flowdc_experiment_data import fields, guest_path, read_file, require
from flowdc_vine_protocol import digest, encode
from flowdc_research_profile import workload

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from benchmark.core.truth import Truth  # noqa: E402


def validate(value):
    fields(
        value,
        ("manifest", "catalog", "origin_plan", "environment_archive", "environment_sha256", "control_tls"),
        ("research_workload",),
    )
    workload(value.get("research_workload"))
    for key in ("manifest", "catalog", "origin_plan", "environment_archive"):
        guest_path(value[key])
    require(isinstance(value["environment_sha256"], str) and len(value["environment_sha256"]) == 64)
    tls = fields(
        value["control_tls"], ("host", "port", "endpoint", "certfile", "keyfile", "ca_file", "ca_sha256")
    )
    for key in ("certfile", "keyfile", "ca_file"):
        guest_path(tls[key])
    require(isinstance(tls["ca_sha256"], str) and re.fullmatch(r"[0-9a-f]{64}", tls["ca_sha256"]))
    endpoint = urlsplit(tls["endpoint"])
    require(
        endpoint.scheme == "https"
        and endpoint.hostname == tls["host"]
        and endpoint.port == tls["port"]
        and not endpoint.username
        and not endpoint.password
        and not endpoint.query
        and not endpoint.fragment,
        "verified_remote_control_required",
    )
    require(type(tls["port"]) is int and 1024 <= tls["port"] <= 65535)
    return value


def origin_plan(raw):
    plan = json.loads(raw)
    fields(
        plan,
        ("schema", "name", "schedule", "queue_bound", "assignments", "objects"),
        ("queue_rejection_status", "queue_retry_after", "research_workload", "overload_windows"),
    )
    plan.setdefault("queue_rejection_status", 503)
    plan.setdefault("queue_retry_after", "0.1")
    require(type(plan["queue_rejection_status"]) is int and plan["queue_rejection_status"] in (429, 503))
    require(
        isinstance(plan["queue_retry_after"], str)
        and re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", plan["queue_retry_after"])
        and float(plan["queue_retry_after"]) <= 5
    )
    require(plan["schema"] in ("flowdc-guest-origin-v1", "flowdc-guest-origin-v3"))
    limits = workload(plan.get("research_workload"))
    require((plan["schema"] == "flowdc-guest-origin-v1") == ("research_workload" not in plan))
    require(1 <= len(plan["objects"]) <= limits.max_rows and 1 <= len(plan["assignments"]) <= limits.max_rows)
    from benchmark.core.controlled_origin import ServiceModel

    ServiceModel(plan["schedule"], plan["queue_bound"], lambda event: None, limits=limits)
    require(all(path in plan["objects"] for path in plan["assignments"]))
    plan["schema"] = "flowdc-origin-scenario-v1" if "research_workload" not in plan else "flowdc-origin-scenario-v3"
    if "research_workload" in plan:
        plan["workload"] = limits.record()
    windows = plan.get("overload_windows", [])
    require(isinstance(windows, list) and len(windows) <= 16)
    for window in windows:
        require(isinstance(window, list) and len(window) == 2
                and all(type(x) in (int, float) and math.isfinite(x) for x in window)
                and 0 <= window[0] < window[1] <= limits.acquisition_seconds, "invalid overload window")
    total = 0
    for path, value in plan["objects"].items():
        require(path.startswith("/") and ".." not in path.split("/") and len(path) <= 2048)
        fields(value, ("payload_base64", "service_s", "responses"), ("tail_s", "service_drift_per_s"))
        value["payload"] = base64.b64decode(value.pop("payload_base64"), validate=True)
        total += len(value["payload"])
        require(0 < len(value["payload"]) <= limits.max_object_bytes and total <= limits.max_payload_bytes)
        require(type(value["service_s"]) in (int, float) and math.isfinite(value["service_s"]) and 0 < value["service_s"] <= 1)
        for key, upper in (("tail_s", 1), ("service_drift_per_s", .01)):
            require(type(value.get(key, 0)) in (int, float) and math.isfinite(value.get(key, 0))
                    and 0 <= value.get(key, 0) <= upper, "invalid origin delay")
        require(isinstance(value["responses"], list) and 1 <= len(value["responses"]) <= 4)
        for response in value["responses"]:
            fields(response, ("status",), ("retry_after", "truncate"))
            require(type(response["status"]) is int and response["status"] in (200, 404, 429, 503))
            require(type(response.get("truncate", False)) is bool)
            require(
                isinstance(response.get("retry_after", "0"), str)
                and re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", response.get("retry_after", "0"))
                and float(response.get("retry_after", "0")) <= 5
            )
    require(sum(len(plan["objects"][p]["payload"]) for p in plan["assignments"]) <= limits.max_payload_bytes)
    return plan


def prepare(value, record):
    value = validate(value)
    require(
        value["control_tls"]["host"] == record["access"]["interfaces"]["manager"]["fixed_ip"],
        "control_endpoint_mapping_mismatch",
    )
    raw = read_file(value["manifest"])
    catalog = json.loads(read_file(value["catalog"]))
    truth = Truth.load(value["manifest"], catalog, research_workload=value.get("research_workload"))
    require(raw == truth.raw_manifest, "research_manifest_changed")
    plan_raw = read_file(value["origin_plan"])
    plan = origin_plan(plan_raw)
    require(plan.get("research_workload") == value.get("research_workload"), "origin_workload_mismatch")
    origin = record["access"]["interfaces"]["origin"]["fixed_ip"]
    expected = {
        f"http://{origin}:18080{path}": {"bytes": len(item["payload"]), "sha256": digest(item["payload"])}
        for path, item in plan["objects"].items()
    }
    require(catalog == expected, "independent_origin_catalog_mismatch")
    files = {
        "research/original.parquet": raw,
        "research/catalog.json": encode(catalog),
        "research/origin.json": plan_raw,
    }
    return files, truth.record


def run_case(root, case):
    from flowdc_vine import cli

    settings = json.loads((root / "guest.json").read_bytes())
    options = json.loads((root / "configs" / f"{case}.json").read_bytes())
    require(
        options.pop("enable_paarc") is True and options.get("control_method") is not None,
        "explicit_shared_method_required",
    )
    options.pop("url_col", None)
    selected = settings["distributed"]
    require(options.get("research_workload") == selected.get("research_workload"), "case_workload_mismatch")
    config = {
        "distributed_profile": "shared-origin-v1",
        "original_manifest": str(root / "research/original.parquet"),
        "catalog": str(root / "research/catalog.json"),
        "output_directory": str(root / "results" / case / "distributed"),
        "environment_archive": selected["environment_archive"],
        "environment_sha256": selected["environment_sha256"],
        "control_tls": selected["control_tls"],
        "native_password_file": str(root / "native-password"),
        "workers": len(settings["addresses"]) - 2,
        "download": options,
        "port_number": 9123,
        "deadline_s": min(workload(selected.get("research_workload")).acquisition_seconds, settings["bounds"]["phase_seconds"]),
        "task_deadline_s": min(workload(selected.get("research_workload")).acquisition_seconds - 30, settings["bounds"]["phase_seconds"] - 30),
        "max_attempts": 1,
    }
    require(config["deadline_s"] > config["task_deadline_s"], "research_guest_deadline_too_small")
    from flowdc_vine_cohort import validate as validate_cohort

    selected_cohort = validate_cohort(settings["worker_cohorts"][case], config["workers"])
    require(selected_cohort["owner"] == "prepared-guest-service-v1", "guest cohort owner required")
    raise SystemExit(cli(config, owned_cohort=selected_cohort))


def serve_origin(root, case):
    from benchmark.core.controlled_origin import ControlledOrigin

    settings = json.loads((root / "guest.json").read_bytes())
    plan = origin_plan((root / "research/origin.json").read_bytes())

    async def serve():
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(signum, stop.set)
        with ControlledOrigin(
            root / "results" / case, plan, bind=settings["addresses"]["origin"], port=18080, guest=True
        ):
            await stop.wait()

    asyncio.run(serve())


def verify_worker_launch(value, cohort, role, case, slot):
    from flowdc_vine_cohort import validate as validate_cohort

    validate_cohort(cohort, len(cohort["slots"]))
    require(
        cohort["owner"] == "prepared-guest-service-v1"
        and value["schema"] == "flowdc-owned-worker-launch-v1"
        and value["cohort"] == cohort
        and value["role"] == role
        and value["case"] == case
        and value["feature"] == cohort["slots"][slot]["feature"]
        and value["single_shot"] is True
        and value["restart"] == "no"
        and type(value["launch_index"]) is int
        and value["launch_index"] == 0,
        "owned_worker_launch_mismatch",
    )


def verify_return(files, case, expected_truth, expected_source, expected_environment, expected_cohort=None):
    """Reverify retained manager returns independently after guest collection."""
    from tempfile import TemporaryDirectory

    from flowdc_vine import Reconciler, control_closure
    from flowdc_vine_protocol import parse

    prefix = "distributed/"
    truth = parse(files[prefix + "fixture/truth.json"])
    require(truth == expected_truth, "returned_independent_truth_changed")
    claimed = parse(files[prefix + "outcomes.json"])
    require(
        isinstance(claimed.get("returns"), list) and len(claimed["returns"]) <= 4, "returned_partition_bound"
    )
    control = parse(files[prefix + "control.json"])
    require(
        all(control_closure(control["state"], parse(files[prefix + "native-cleanup.json"]))),
        "distributed_control_or_native_incomplete",
    )
    require(
        claimed["schema"] == "flowdc-distributed-result-v1"
        and claimed["method"] == case["config"]["control_method"]
        and control["state"]["binding"].get("method") == claimed["method"],
        "distributed_method_mismatch",
    )
    with TemporaryDirectory() as directory:
        parent = Path(directory)
        if expected_cohort is not None:
            from flowdc_vine import dispatch_records
            from flowdc_vine_cohort import dispatch_audit

            require(parse(files[prefix + "cohort.json"]) == expected_cohort, "native_cohort_changed")
            submissions = [
                parse(files[name])["native_task_id"]
                for name in sorted(files)
                if name.startswith(prefix + "partition-") and name.endswith("/submission.json")
            ]
            require(
                len(submissions)
                == min(len(expected_cohort["slots"]), sum(row["eligible"] for row in truth["rows"])),
                "missing_native_submission",
            )
            require(len(submissions) == len(set(submissions)), "duplicate_native_task")
            for name, raw in files.items():
                if name.startswith(prefix + "run-info/") and name.endswith("/transactions"):
                    target = parent / name
                    require(target.resolve().is_relative_to(parent.resolve()), "unsafe_dispatch_log_path")
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes(raw)
            actual = dispatch_records(parent / prefix / "run-info")
            audit = dispatch_audit(expected_cohort, submissions, actual)
            require(
                actual == claimed["dispatches"]
                and audit == claimed["native_dispatch_bound"]
                and audit["within_bound"],
                "native_dispatch_bound_mismatch",
            )
            require(all(item["dispatches"] > 0 for item in audit["tasks"]), "missing_native_dispatch")
        reconciler = Reconciler(truth)
        for returned in claimed["returns"]:
            identifier = returned["scope_id"]
            require(
                isinstance(identifier, str) and re.fullmatch(r"[0-9a-f]{32}", identifier),
                "unsafe_returned_scope",
            )
            matches = [
                name
                for name in files
                if name.startswith(prefix + "partition-")
                and name.endswith("/task.json")
                and parse(files[name])["scope_id"] == identifier
            ]
            require(len(matches) == 1, "returned_partition_identity_mismatch")
            name = matches[0]
            specification = parse(files[name])
            if expected_cohort is not None:
                partition = name.removesuffix("task.json")
                submission = parse(files[partition + "submission.json"])
                native = parse(files[partition + "native.json"])
                require(
                    submission["scope_id"] == identifier
                    and submission["native_task_id"] == native["task_id"]
                    and native == returned["native"],
                    "native_return_receipt_mismatch",
                )
            require(
                specification["files"] == expected_source
                and specification["environment_sha256"] == expected_environment,
                "returned_source_environment_changed",
            )
            require(
                specification["binding"] == control["state"]["binding"], "returned_control_binding_changed"
            )
            archive = parent / (identifier + ".tar")
            archive.write_bytes(files[name.removesuffix("task.json") + "return.tar"])
            reconciler.accept(
                parent / identifier,
                archive,
                parse(files[name]),
                returned["native"],
                control["state"]["clients"],
            )
        result = reconciler.summary()
        require(
            not result["errors"] and result["rows"] == claimed["rows"],
            "distributed_collection_verification_failed",
        )
        require(
            claimed["run_complete"] is True
            and all(row["disposition"] in ("verified", "skipped") for row in result["rows"])
            and all(
                item["accepted"]
                and item["native"].get("successful") is True
                and type(item["native"].get("exit_code")) is int
                and item["native"]["exit_code"] == 0
                and item.get("receipt", {}).get("status") == "returned"
                for item in result["returns"]
            ),
            "distributed_run_incomplete",
        )
    return {
        "original_rows": result["original_rows"],
        "verified_rows": result["verified_rows"],
        "useful_bytes": result["useful_bytes"],
        "method": claimed["method"],
        "end_to_end_ns": parse(files[prefix + "timing.json"])["end_to_end_ns"],
    }
