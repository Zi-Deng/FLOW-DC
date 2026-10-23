"""Finite single-downloader public pilot using existing supervised accounting and trust."""

import argparse
import datetime as dt
import hashlib
import json
import math
import os
import subprocess
import sys
import tarfile
import time
from pathlib import Path
from uuid import uuid4

from . import cloud, storage
from .common import file_hash, lock, read_json, require, source_identity, utc, write_new
from .onset_cloud import exposure, local_status, ready
from .public_batch import LIMITS


def enrolled_topology(source, operator):
    Journal, _, _, _ = cloud.imports(source)
    current = Journal(operator["state"]).read()
    selected = {
        v["role"]: v["id"] for v in current["spec"]["vms"] if v["role"] in ("manager", "worker", "origin")
    }
    require(
        set(selected) == {"manager", "worker", "origin"} and selected["worker"] == operator["worker_id"],
        "supported three-role topology required",
    )
    return selected


def prepare(study, source, previous, packet, *, limits=LIMITS, new_storage=False):
    study, source, previous, packet = map(lambda p: Path(p).resolve(), (study, source, previous, packet))
    require(not packet.exists(), "new deployment directory required")
    require(
        not subprocess.check_output(["git", "status", "--porcelain"], cwd=source, text=True).strip(),
        "commit qualified source before staging",
    )
    protocol = read_json(study / "protocol.json")
    require(protocol["source"] == source_identity(source), "prepared source identity changed")
    old = read_json(previous / "operator.json")
    packet.mkdir(parents=True, mode=0o700)
    campaign = str(uuid4())
    with (packet / "source.tar").open("xb") as out:
        subprocess.run(["git", "archive", "HEAD"], cwd=source, stdout=out, check=True, timeout=60)
    write_new(
        packet / "deployment.json",
        dict(
            campaign_id=campaign,
            source=protocol["source"],
            limits=limits,
            selected=enrolled_topology(source, old),
            registered_rate_total_per_hour=32,
            source_archive_sha256=file_hash(packet / "source.tar"),
            protocol_sha256=file_hash(study / "protocol.json"),
            authority="Maintainer approved finite public study and execution before final PR",
        ),
    )
    write_new(
        packet / "operator.json",
        {
            **old,
            "study": str(study),
            "source": str(source),
            "guest_root": (
                "/mnt/flowdc-external-" + campaign
                if new_storage
                else old["guest_root"] + "/public-" + campaign
            ),
            "previous_packet": str(previous),
        },
    )
    write_new(packet / "ssh-identity.json", read_json(previous / "ssh-identity.json"))
    if not new_storage:
        write_new(
            packet / "retained-volume.json",
            dict(
                volume=read_json(previous / "volume.json"),
                mount=read_json(previous / "volume-mount.json"),
                root=old["guest_root"],
            ),
        )
    requirements = source / "benchmark/requirements-tools-linux-py312.lock"
    wheels = packet / "wheels"
    wheels.mkdir()
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "--isolated",
            "download",
            "--require-hashes",
            "--only-binary=:all:",
            "--platform",
            "manylinux_2_28_x86_64",
            "--platform",
            "manylinux2014_x86_64",
            "--python-version",
            "312",
            "--implementation",
            "cp",
            "--abi",
            "cp312",
            "-r",
            str(requirements),
            "-d",
            str(wheels),
        ],
        check=True,
        timeout=300,
    )
    with tarfile.open(packet / "inputs.tar", "x") as archive:
        for path in sorted(study.rglob("*")):
            require(not path.is_symlink(), "symlink in private input")
            if path.is_file() or path.is_dir():
                archive.add(path, arcname="study/" + str(path.relative_to(study)), recursive=False)
        for path in wheels.iterdir():
            archive.add(path, arcname="wheels/" + path.name, recursive=False)
        archive.add(requirements, arcname="requirements.txt")
    write_new(
        packet / "payload.json",
        dict(
            inputs_sha256=file_hash(packet / "inputs.tar"),
            wheels={p.name: file_hash(p) for p in wheels.iterdir()},
        ),
    )
    return read_json(packet / "deployment.json")


def qualification(packet):
    proof = read_json(packet / "qualification.json")
    deployment = read_json(packet / "deployment.json")
    head = deployment["source"]["head"]
    require(proof["head"] == head, "qualification head changed")
    review = read_json(proof["review_file"])
    report = read_json(proof["report_file"])
    require(
        file_hash(proof["review_file"]) == proof["review_sha256"]
        and file_hash(proof["report_file"]) == proof["report_sha256"],
        "review receipt changed",
    )
    reviewed = review["head"]
    if reviewed != head:
        # The maintainer caps this task at two reviews. A bounded final repair
        # retains its actual reviewed SHA, exact delta and current-head CI.
        op = read_json(packet / "operator.json")
        require(
            proof.get("reviewed_head") == reviewed
            and proof.get("final_head") == head
            and proof.get("post_review_repair") is True,
            "undisclosed post-review source change",
        )
        require(
            subprocess.run(
                ["git", "merge-base", "--is-ancestor", reviewed, head], cwd=op["source"]
            ).returncode
            == 0,
            "review is not an ancestor",
        )
        delta = subprocess.check_output(
            ["git", "diff", "--no-ext-diff", "--no-textconv", reviewed, head], cwd=op["source"]
        )
        require(
            hashlib.sha256(delta).hexdigest() == proof.get("post_review_delta_sha256"),
            "post-review repair delta changed",
        )
    require(review["status"] == "completed", "completed independent static review required")
    material = {i for i, f in enumerate(report["findings"]) if f["severity"] in ("P0", "P1", "P2")}
    if material:
        # The maintainer permits two reviews/two repairs, not an automatic third
        # review. Preserve the original findings and disclose the exact final
        # repair delta above; never relabel it as a reviewed final head.
        require(
            reviewed != head and proof.get("post_review_repair") is True,
            "material findings need an explicit post-review repair",
        )
        path = proof["post_review_disposition_file"]
        require(file_hash(path) == proof["post_review_disposition_sha256"], "repair disposition changed")
        dispositions = read_json(path)
        require(
            dispositions["reviewed_head"] == reviewed and dispositions["final_head"] == head,
            "repair disposition identity differs",
        )
        resolved = dispositions["resolved"]
        require(
            {d["finding"] for d in resolved} == material
            and len(resolved) == len(material)
            and all(d["status"] == "repaired" and d["validation"] and d["resolution"] for d in resolved),
            "material findings remain unresolved",
        )
    names = set()
    for run in proof["ci_run_ids"]:
        remote = json.loads(
            subprocess.check_output(
                [
                    "gh",
                    "run",
                    "view",
                    str(run),
                    "--repo",
                    "Zi-Deng/FLOW-DC",
                    "--json",
                    "headSha,conclusion,name",
                ],
                text=True,
                timeout=30,
            )
        )
        require(
            remote["headSha"] == head and remote["conclusion"] == "success", "current-head hosted CI required"
        )
        names.add(remote["name"])
    require(
        len(proof["ci_run_ids"]) == 2 and names == {"FLOW-DC regression tests", "agentic workflow tests"},
        "both required CI workflows required",
    )


def authorize(packet, limits=LIMITS):
    op, deployment = read_json(packet / "operator.json"), read_json(packet / "deployment.json")
    Journal, allowance, binding, account_digest = cloud.imports(op["source"])
    journal = Journal(op["state"])
    import flowdc_ops as ops
    from flowdc_pilot_provider import Provider
    from flowdc_pilot_supervisor import sample_clock

    clock = sample_clock()  # Preserve the first preflight clock across failed read-only checks.
    clock_path = packet / "start-clock.json"
    if clock_path.exists():
        from flowdc_pilot import ClockSample

        earlier = read_json(clock_path)
        require(earlier["boot_id"] == clock.boot_id, "preflight controller reboot")
        clock = ClockSample(**earlier)
    else:
        write_new(clock_path, vars(clock))
    qualification(packet)
    require(source_identity(op["source"]) == deployment["source"], "deployment source changed")
    current = journal.read()
    Provider(ops.load_profile(current["profile_path"])).verify_idle(current)
    facts = read_json(op["allocation_file"])
    observed = dt.datetime.fromisoformat(
        (facts.get("observed_at") or facts["reported_at"]).replace("Z", "+00:00")
    )
    require(
        facts["allocation"] == "CIS240296"
        and facts["cpu_su"] >= limits["budget_su"]
        and 0 <= time.time() - observed.timestamp() <= 7 * 86400
        and time.time() + limits["wall_seconds"]
        < dt.datetime.fromisoformat(facts["expiry_date"]).replace(tzinfo=dt.UTC).timestamp(),
        "allocation facts insufficient or older than7days; no portal observation invented",
    )
    if (packet / "campaign.json").exists():
        require(
            (packet / "grant-application.json").exists()
            and read_json(packet / "grant-application.json")["returncode"] == 0,
            "interrupted grant; reconcile without replay",
        )
        record = read_json(packet / "campaign.json")
        require(
            set(record["baseline"]) == set(deployment["selected"].values()) and record["limits"] == limits,
            "campaign must account for actual selected VMs and current finite limits",
        )
        return journal, record
    rates_receipt = cloud.verify_rates(
        Provider(ops.load_profile(current["profile_path"])), current, deployment
    )
    rates = {row["id"]: row["su_per_hour"] for row in rates_receipt["servers"]}
    chosen = sorted(deployment["selected"].values())
    require(
        sum(rates.values()) * min(limits["wall_seconds"], limits["max_windows"] * limits["window_seconds"]) / 3600
        <= limits["budget_su"],
        "actual selected-VM forecast exceeds study budget",
    )
    baseline = {key: current["vms"][key]["account"]["consumed"] for key in chosen}
    cumulative = {
        key: max(
            math.ceil(allowance(current["vms"][key]["account"]).effective_limit) + 1,
            math.ceil(baseline[key]) + min(limits["wall_seconds"], limits["max_windows"] * limits["window_seconds"]),
        )
        for key in chosen
    }
    preview = sum((cumulative[key] - baseline[key]) * rates[key] / 3600 for key in chosen)
    require(preview <= max(1000, limits["budget_su"]), "preserved cumulative preview exceeds authorization ceiling")
    record = dict(
        campaign_id=deployment["campaign_id"],
        clock=vars(clock),
        baseline=baseline,
        rates=rates,
        limits=limits,
        allocation_basis=facts,
        preserved_cumulative_preview_su=preview,
        new_admission_limit_su=limits["budget_su"],
    )
    write_new(packet / "campaign.json", record)
    request = dict(
        schema="flowdc-campaign-authorization-v1",
        campaign_id=record["campaign_id"],
        registration_id=current["registration_id"],
        expected_binding_sha256=binding(current),
        expected_accounts_sha256=account_digest(current),
        selected_ids=chosen,
        cumulative_limits_seconds=cumulative,
        expires_at=dt.datetime.fromtimestamp(clock.utc + limits["wall_seconds"], dt.UTC).isoformat(),
        max_window_seconds=limits["window_seconds"],
        protocol_sha256=file_hash(packet / "deployment.json"),
        budget_sha256=file_hash(packet / "campaign.json"),
        budget_su=max(limits["budget_su"], math.ceil(preview)),
    )
    # Existing unused cumulative authority is preserved. This driver's finite
    # new-exposure limit still counts every selected VM.
    write_new(packet / "grant.json", request)
    path = cloud.operational_grant(packet, op)
    service = current["service"]
    call = subprocess.run(
        [
            service["interpreter"],
            "-E",
            "-s",
            "-B",
            str(Path(service["release"]) / "flowdc_ops.py"),
            "pilot",
            "campaign-apply",
            "--state-root",
            op["state"],
            "--grant",
            str(path),
        ],
        capture_output=True,
        timeout=180,
    )
    write_new(
        packet / "grant-application.json",
        dict(
            returncode=call.returncode,
            stdout=call.stdout.decode("utf8", "replace"),
            stderr=call.stderr.decode("utf8", "replace"),
        ),
    )
    require(call.returncode == 0, "campaign grant refused; no activation replay")
    return journal, record


def wall_left(record):
    from flowdc_pilot_supervisor import sample_clock

    now = sample_clock()
    before = record["clock"]
    require(now.boot_id == before["boot_id"], "controller reboot")
    return min(before["utc"] + 28800 - now.utc, before["boottime"] + 28800 - now.boottime)


def remote(packet, number, seconds, record, action="window"):
    op, deployment = read_json(packet / "operator.json"), read_json(packet / "deployment.json")
    root = op["guest_root"]
    installation = read_json(packet / "deployment-receipt.json")
    execution = dict(
        executor="jetstream2",
        vm_id=op["worker_id"],
        campaign_id=deployment["campaign_id"],
        deadline_utc=record["clock"]["utc"] + 28800,
        dispatch_deadline_utc=record["clock"]["utc"] + 19800,
        source_root=root + "/source",
    )
    setup = "import json,pathlib,socket,sys;sys.path.insert(0,sys.argv[3]);from benchmark.real_server_study.common import replace_derived;p=pathlib.Path(sys.argv[1]);v=json.loads(sys.argv[2]);v['hostname']=socket.gethostname();replace_derived(p/'execution.json',v)"
    cloud.ssh(
        packet,
        [installation["python"], "-c", setup, root + "/study", json.dumps(execution), root + "/source"],
        timeout=30,
    )
    command = [
        installation["python"],
        "-B",
        "-m",
        "benchmark.real_server_study.public_batch",
        "window",
        "--study",
        root + "/study",
        "--source",
        root + "/source",
        "--seconds",
        str(seconds),
    ]
    if action == "audit":
        command = [
            installation["python"],
            "-B",
            "-m",
            "benchmark.real_server_study.public_analysis",
            root + "/study",
        ]
    unit = f"flowdc-public-{deployment['campaign_id']}-{number}-{action}"
    wrapper = [
        "sudo",
        "-n",
        "systemd-run",
        "--quiet",
        "--wait",
        "--pipe",
        "--collect",
        "--unit",
        unit,
        "--property=RuntimeMaxSec=" + str(seconds + 10),
        "--property=TimeoutStopSec=15",
        "--property=KillMode=control-group",
        "--property=MemoryMax=24G",
        "--property=CPUQuota=400%",
        "--property=LimitNOFILE=4096",
        "--property=UMask=0077",
        "--uid=exouser",
        "--gid=exouser",
        "--setenv=POLARS_MAX_THREADS=4",
        "--setenv=OMP_NUM_THREADS=4",
        "--setenv=OPENBLAS_NUM_THREADS=4",
        "--working-directory=" + root + "/source",
        *command,
    ]
    return json.loads(cloud.ssh(packet, wrapper, timeout=seconds + 45))


def quiesce(packet, number):
    op, deployment = read_json(packet / "operator.json"), read_json(packet / "deployment.json")
    units = [f"flowdc-public-{deployment['campaign_id']}-{number}-{a}.service" for a in ("window", "audit")]
    script = """import json,subprocess,sys
for unit in sys.argv[1:]:
 p=subprocess.run(['systemctl','show','--property=ActiveState','--value',unit],capture_output=True,text=True,timeout=10)
 assert p.returncode in (0,4),'unit state unknown'
 if p.stdout.strip() not in ('','inactive','failed'):
  subprocess.run(['sudo','-n','systemctl','stop',unit],check=True,timeout=30)
 p=subprocess.run(['systemctl','show','--property=ActiveState','--value',unit],capture_output=True,text=True,timeout=10)
 assert p.returncode in (0,4) and p.stdout.strip() in ('','inactive','failed'),'owned process remains'
print(json.dumps({'quiescent':True,'units':sys.argv[1:]}))
"""
    return json.loads(cloud.ssh(packet, [op["guest_python"], "-c", script, *units], timeout=90))


def admit_window(record, actual_ids, spent, remaining_wall):
    require(
        set(actual_ids) == set(record["baseline"]) == set(record["rates"]),
        "activation selection differs from accounted VMs",
    )
    require(
        remaining_wall >= 3600 and spent + sum(record["rates"].values()) <= LIMITS["budget_su"],
        "finite time/SU admission exhausted",
    )


def run(packet):
    packet = Path(packet).resolve()
    os.umask(0o077)
    op = read_json(packet / "operator.json")
    from .public_analysis import report

    if (packet / "collected").exists():
        return report(packet / "collected")
    with lock(packet / "controller.lock"):
        journal, record = authorize(packet)
        from flowdc_experiment_transport import remaining

        terminal = False
        for number in range(LIMITS["max_windows"]):
            closed = packet / f"window-{number:02}.json"
            intent = packet / f"window-{number:02}-intent.json"
            if closed.exists():
                continue
            require(not intent.exists(), "unfinished window; reconcile instead of replacement activation")
            controller = cloud.controller_for(packet)
            from flowdc_topology import selected_ids

            admit_window(
                record, selected_ids(controller.expected), exposure(record, journal), wall_left(record)
            )
            controller.preflight(3600)
            write_new(intent, dict(utc=utc(), number=number))
            outcome = None
            try:
                controller.call("start", seconds=30, window=3600)
                ready(journal)
                cloud.refresh_ssh(packet, controller)
                cloud.wait_ssh(packet, controller, number)
                volume = read_json(packet / "retained-volume.json")
                cloud.ssh(
                    packet,
                    [
                        "sudo",
                        "-n",
                        "/usr/bin/python3",
                        "-c",
                        storage.MOUNT,
                        volume["volume"]["volume_id"],
                        volume["root"],
                        volume["mount"]["filesystem_uuid"],
                    ],
                    timeout=180,
                )
                if not (packet / "deployment-receipt.json").exists():
                    require(
                        not (packet / "staging-intent.json").exists(), "interrupted staging; no overwrite"
                    )
                    write_new(packet / "staging-intent.json", dict(utc=utc()))
                    cloud.ssh(packet, ["mkdir", "--", op["guest_root"]])
                    storage.stage(packet)
                dispatch_left = record["clock"]["utc"] + 19800 - time.time()
                seconds = int(
                    min(2800, remaining(local_status(journal)) - 60, dispatch_left, wall_left(record) - 900)
                )
                if seconds >= 270 and not terminal:
                    outcome = remote(packet, number, seconds, record)
                    terminal = outcome["status"] in ("complete", "stopped")
                    write_new(packet / f"window-{number:02}-guest.json", outcome)
                else:
                    terminal = True
                if terminal:
                    seconds = int(min(1200, remaining(local_status(journal)) - 120))
                    require(seconds >= 60, "audit/collection require separately bounded remaining window")
                    audited = remote(packet, number, seconds, record, "audit")
                    write_new(packet / f"window-{number:02}-audit.json", audited)
                    cloud.collect(
                        packet,
                        seconds=int(min(900, remaining(local_status(journal)) - 30)),
                        metadata_limit=LIMITS["metadata_bytes"],
                        public_metadata=True,
                    )
            except Exception as error:
                write_new(
                    packet / f"window-{number:02}-failure.json",
                    dict(utc=utc(), error=type(error).__name__ + ": " + str(error)),
                )
                # Diagnose once. Never replay an uncertain acquisition invocation.
                if (packet / "deployment-receipt.json").exists():
                    write_new(packet / f"window-{number:02}-quiescence.json", quiesce(packet, number))
                raise
            finally:
                cloud.cleanup(packet, controller, closed, outcome)
            if (packet / "collected").exists():
                result = report(packet / "collected")
                write_new(
                    packet / "complete.json",
                    dict(
                        utc=utc(),
                        windows=number + 1,
                        new_exposure_su=exposure(record, journal),
                        cleanup_complete=True,
                        report=result,
                    ),
                )
                return read_json(packet / "complete.json")
        raise ValueError("finite batch exhausted; preserve artifacts and cleanup, no automatic extension")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="command", required=True)
    a = sub.add_parser("prepare")
    for name in ("study", "source", "previous", "packet"):
        a.add_argument("--" + name, type=Path, required=True)
    r = sub.add_parser("run")
    r.add_argument("--packet", type=Path, required=True)
    args = p.parse_args()
    result = (
        prepare(args.study, args.source, args.previous, args.packet)
        if args.command == "prepare"
        else run(args.packet)
    )
    print(json.dumps(result))
