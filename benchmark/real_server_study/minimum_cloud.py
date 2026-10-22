"""One finite supervised campaign; three trusted guests, no registration rewrite."""

import argparse
import json
import os
import shlex
import shutil
import subprocess
import tarfile
import time
from pathlib import Path

from . import cloud, public_cloud, storage
from .common import file_hash, lock, read_json, require, utc, write_new
from .minimum_study import LIMITS
from .onset_cloud import exposure, local_status, ready

ROLES = ("manager", "worker", "origin")
VOLUMES = {"manager": 4096, "worker": 512, "origin": 512}
NATIVE = {
    "bin/vine_worker": "a970826a3557463181066fa021adee114da0b5a630bb2f151e768c20163694d4",
    "lib/python3.12/site-packages/ndcctools/taskvine/_cvine.so": "c7e5f6d35713491ee66cc4d277e864100b0f608c8c653ee96e7628bbc3c46851",
}


def ssh(packet, role, argv, timeout=30, data=None, stdin=None, stdout=subprocess.PIPE):
    require(role in ROLES, "unselected guest")
    command = ["ssh", "-F", cloud.ssh_configuration(packet), "--", role, shlex.join(list(map(str, argv)))]
    result = subprocess.run(
        command, input=data, stdin=stdin, stdout=stdout, stderr=subprocess.PIPE, timeout=timeout
    )
    if result.returncode:
        write_new(
            packet / f"ssh-error-{time.time_ns()}.json",
            dict(
                role=role,
                returncode=result.returncode,
                stderr=result.stderr[-8192:].decode("utf8", "replace"),
                utc=utc(),
            ),
        )
    require(result.returncode == 0, "guest operation failed; retain intent, do not replay")
    return result.stdout


def json_command(packet, role, script, *args, timeout=30):
    op = read_json(packet / "operator.json")
    return json.loads(
        ssh(packet, role, [op["guest_root"] + "/venv/bin/python", "-c", script, *args], timeout)
    )


def prepare(study, source, previous, packet):
    packet = Path(packet)
    result = public_cloud.prepare(study, source, previous, packet, limits=LIMITS, new_storage=True)
    for role in ROLES:
        directory = packet / "volumes" / role
        directory.mkdir(parents=True)
        write_new(directory / "deployment.json", dict(campaign_id=result["campaign_id"]))
    write_new(
        packet / "storage-plan.json",
        dict(gib=VOLUMES, native=NATIVE, owner="new campaign only; no existing volume formatted or erased"),
    )
    write_new(packet / "native-password", os.urandom(32).hex().encode())
    os.chmod(packet / "native-password", 0o600)
    return result


def stage(packet, role, profile):
    op, deployment = read_json(packet / "operator.json"), read_json(packet / "deployment.json")
    directory = packet / "volumes" / role
    receipt = storage.create_volume(directory, profile, deployment["selected"][role], size_gib=VOLUMES[role])
    mounted = directory / "volume-mount.json"
    mount = json.loads(
        ssh(
            packet,
            role,
            [
                "sudo",
                "-n",
                "/usr/bin/python3",
                "-c",
                storage.MOUNT,
                receipt["volume_id"],
                op["guest_root"],
                read_json(mounted)["filesystem_uuid"] if mounted.exists() else "NEW",
            ],
            180,
        )
    )
    if not mounted.exists():
        write_new(mounted, mount)
    installed = directory / "installation.json"
    if installed.exists():
        return read_json(installed)
    write_new(directory / "staging-intent.json", dict(utc=utc()))
    for name in ("source.tar", "inputs.tar", "native-password"):
        with (packet / name).open("rb") as stream:
            ssh(
                packet,
                role,
                [
                    "/usr/bin/python3",
                    "-c",
                    "import pathlib,shutil,sys; p=pathlib.Path(sys.argv[1]); f=p.open('xb'); shutil.copyfileobj(sys.stdin.buffer,f); f.close();p.chmod(0o600)",
                    op["guest_root"] + "/" + name,
                ],
                timeout=300,
                stdin=stream,
            )
    program = storage.STAGE.replace("'-m','venv',", "'-m','venv','--system-site-packages',")
    result = json.loads(
        ssh(
            packet,
            role,
            [
                op["guest_python"],
                "-c",
                program,
                op["guest_root"],
                deployment["source_archive_sha256"],
                read_json(packet / "payload.json")["inputs_sha256"],
                deployment["source"]["head"],
                op["guest_python"],
            ],
            timeout=360,
        )
    )
    # Reuse the previously qualified native runtime without modifying its bytes.
    native = json_command(
        packet,
        role,
        """import hashlib,json,pathlib,sys
import ndcctools.taskvine as vine
base=pathlib.Path(sys.argv[1]).parent.parent
expected=json.loads(sys.argv[2]); actual={p:hashlib.sha256((base/p).read_bytes()).hexdigest() for p in expected}
assert actual==expected and vine.__version__=='7.17.2','native runtime differs'
print(json.dumps({'version':vine.__version__,'digests':actual,'base':str(base)}))""",
        op["guest_python"],
        json.dumps(NATIVE),
    )
    result["native"] = native
    write_new(installed, result)
    return result


def unit(packet, role, name, command, seconds, cores=8, memory="24G"):
    root = read_json(packet / "operator.json")["guest_root"]
    ssh(
        packet,
        role,
        [
            "sudo",
            "-n",
            "systemd-run",
            "--quiet",
            "--collect",
            "--unit",
            name,
            "--property=RuntimeMaxSec=" + str(seconds),
            "--property=TimeoutStopSec=15",
            "--property=KillMode=control-group",
            "--property=Restart=no",
            "--property=MemoryMax=" + memory,
            "--property=CPUQuota=" + str(cores * 100) + "%",
            "--property=LimitNOFILE=8192",
            "--property=UMask=0077",
            "--uid=exouser",
            "--gid=exouser",
            "--property=StandardOutput=append:" + root + "/" + name + ".log",
            "--property=StandardError=append:" + root + "/" + name + ".log",
            "--setenv=POLARS_MAX_THREADS=8",
            "--setenv=OMP_NUM_THREADS=1",
            "--setenv=OPENBLAS_NUM_THREADS=1",
            "--working-directory=" + root + "/source",
            *command,
        ],
    )


def stop_unit(packet, role, name):
    return json.loads(
        ssh(
            packet,
            role,
            [
                "/usr/bin/python3",
                "-c",
                """import json,subprocess,sys
unit=sys.argv[1]+'.service'
r=subprocess.run(['systemctl','show',unit,'--property=ActiveState','--value'],capture_output=True,text=True,timeout=10)
assert r.returncode in (0,4)
if r.stdout.strip() not in ('','inactive','failed'):
 subprocess.run(['sudo','-n','systemctl','stop',unit],check=True,timeout=45)
r=subprocess.run(['systemctl','show',unit,'--property=ActiveState','--value'],capture_output=True,text=True,timeout=10)
assert r.returncode in (0,4) and r.stdout.strip() in ('','inactive','failed')
print(json.dumps({'unit':unit,'quiescent':True}))""",
                name,
            ],
            timeout=70,
        )
    )


def job(packet, cell, number, journal, record):
    op, deployment = read_json(packet / "operator.json"), read_json(packet / "deployment.json")
    root, campaign = op["guest_root"], deployment["campaign_id"]
    roles = ["worker", "origin"][: cell["workers"]]
    request = dict(
        cell=cell,
        guest_root=root,
        study=root + "/study",
        campaign_id=campaign,
        head=deployment["source"]["head"],
        roles=roles,
        vms=deployment["selected"],
    )
    local = packet / "jobs" / cell["run_id"]
    local.mkdir(parents=True)
    write_new(local / "intent.json", dict(utc=utc(), window=number, request=request))
    json_command(
        packet,
        "manager",
        """import json,pathlib,sys
root=pathlib.Path(sys.argv[1]);p=root/'requests';p.mkdir(exist_ok=True)
with (p/(sys.argv[2]+'.json')).open('x') as f:f.write(sys.argv[3])
print('{}')""",
        root,
        cell["run_id"],
        json.dumps(request),
    )
    name = "flowdc-min-" + campaign + "-" + cell["run_id"]
    owned, result = [], None
    seconds = cell["job_seconds"]
    require(0 < seconds <= LIMITS["job_seconds"], "invalid admitted job deadline")
    deadline = time.monotonic() + seconds + 30
    try:
        owned.append(("manager", name + "-m"))
        unit(
            packet,
            "manager",
            name + "-m",
            [
                root + "/venv/bin/python",
                "-B",
                "-m",
                "benchmark.real_server_study.minimum_native",
                "manager",
                "--request",
                root + "/requests/" + cell["run_id"] + ".json",
            ],
            seconds + 15,
            cores=16,
            memory="48G",
        )
        while True:
            state = json_command(
                packet,
                "manager",
                """import json,pathlib,sys
p=pathlib.Path(sys.argv[1])/'study/runs'/sys.argv[2]
print(json.dumps({'ready':(p/'ready.json').exists(),'record':json.loads((p/'record.json').read_text()) if (p/'record.json').exists() else None}))""",
                root,
                cell["run_id"],
            )
            if state["record"]:
                result = state["record"]
                break
            if state["ready"]:
                break
            require(time.monotonic() < deadline - 60, "manager readiness deadline")
            time.sleep(2)
        if result is None:
            manager_ip = journal.read()["access"]["interfaces"]["manager"]["fixed_ip"]
            native_worker = str(Path(op["guest_python"]).parent / "vine_worker")
            for role in roles:
                owned.append((role, name + "-" + role))
                ssh(packet, role, ["mkdir", "-p", "--", root + "/worker-sandboxes/" + cell["run_id"]])
                unit(
                    packet,
                    role,
                    name + "-" + role,
                    [
                        native_worker,
                        "--ssl",
                        "-P",
                        root + "/native-password",
                        "--feature",
                        campaign + "-" + role,
                        "--single-shot",
                        "--wall-time",
                        str(seconds),
                        "--cores",
                        "8",
                        "--memory",
                        "24576",
                        "--disk",
                        "300000",
                        "--workspace",
                        root + "/worker-sandboxes/" + cell["run_id"],
                        manager_ip,
                        "9123",
                    ],
                    seconds,
                )
            while result is None:
                require(time.monotonic() < deadline, "job completion deadline; preserve partial work")
                require(exposure(record, journal) < LIMITS["budget_su"], "compute budget exhausted")
                result = json_command(
                    packet,
                    "manager",
                    """import json,pathlib,sys
p=pathlib.Path(sys.argv[1])/'study/runs'/sys.argv[2]/'record.json'
print(p.read_text() if p.exists() else 'null')""",
                    root,
                    cell["run_id"],
                )
                if result is None:
                    time.sleep(15)
        write_new(local / "record.json", result)
    finally:
        receipts = []
        for role, service in reversed(owned):
            receipts.append(stop_unit(packet, role, service))
        write_new(local / "quiescence.json", receipts)
    if result["status"] == "complete":
        # Manager has independently verified each archive. Only now retire
        # duplicates, confined to this campaign/run; never older studies.
        for role in roles:
            proof = json_command(
                packet,
                "manager",
                """import pathlib,sys
print((pathlib.Path(sys.argv[1])/'study/runs'/sys.argv[2]/sys.argv[3]/'retention.json').read_text())""",
                root,
                cell["run_id"],
                role,
            )
            require(
                proof["verified"] and proof["vm_id"] == deployment["selected"][role],
                "retention acknowledgement differs",
            )
            receipt = json_command(
                packet,
                role,
                """import json,pathlib,shutil,sys
root=pathlib.Path(sys.argv[1]);run=sys.argv[2];role=sys.argv[3]
for p in (root/'jobs'/run/role,root/'worker-sandboxes'/run):
 assert p.is_relative_to(root) and not p.is_symlink()
 if p.exists():shutil.rmtree(p)
print(json.dumps({'deleted_verified_duplicate':run,'role':role}))""",
                root,
                cell["run_id"],
                role,
                timeout=180,
            )
            write_new(local / (role + "-duplicate-cleanup.json"), receipt)
        json_command(
            packet,
            "manager",
            """import json,pathlib,sys
p=pathlib.Path(sys.argv[1])/'study/runs'/sys.argv[2]
for role in json.loads(sys.argv[3]):
 assert json.loads((p/role/'retention.json').read_text())['verified']
 (p/role/'return.tar').unlink()
print('{}')""",
            root,
            cell["run_id"],
            json.dumps(roles),
        )
    return result


def collect(packet):
    root = read_json(packet / "operator.json")["guest_root"]
    write_new(packet / "collection-intent.json", dict(utc=utc(), offline_only=True))
    program = """import hashlib,json,pathlib,sys,tarfile,io
root=pathlib.Path(sys.argv[1]);study=root/'study';chosen=[]
for p in study.rglob('*'):
 if not p.is_file() or p.is_symlink():continue
 rel=str(p.relative_to(study))
 if rel.startswith(('pools/','runs/')):
  keep=(p.name in ('record.json','worker-record.json','attempt.json','config.json','flowdc-config.json','retention.json','verification.json','vine-result.json','input.parquet') or '/.flowdc/control/' in rel)
  if not keep:continue
 chosen.append(p)
assert sum(p.stat().st_size for p in chosen)<4*2**30
manifest={str(p.relative_to(study)):{'bytes':p.stat().st_size,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for p in chosen}
with tarfile.open(fileobj=sys.stdout.buffer,mode='w|') as archive:
 for p in chosen:archive.add(p,arcname=str(p.relative_to(study)),recursive=False)
 raw=json.dumps(manifest).encode();info=tarfile.TarInfo('transfer-manifest.json');info.size=len(raw);archive.addfile(info,io.BytesIO(raw))
"""
    require(
        shutil.disk_usage(packet).free > LIMITS["free_bytes"] + LIMITS["metadata_bytes"], "local free reserve"
    )
    with (packet / "metadata.tar").open("xb") as output:
        ssh(
            packet,
            "manager",
            [root + "/venv/bin/python", "-c", program, root],
            timeout=LIMITS["collection_seconds"],
            stdout=output,
        )
    require(
        (packet / "metadata.tar").stat().st_size <= LIMITS["metadata_bytes"] + 2**20, "collection ceiling"
    )
    destination = packet / "collected"
    destination.mkdir()
    with tarfile.open(packet / "metadata.tar") as archive:
        for item in archive:
            require(
                item.isfile() and not Path(item.name).is_absolute() and ".." not in Path(item.name).parts,
                "unsafe collected path",
            )
            archive.extract(item, destination, filter="data")
    manifest = read_json(destination / "transfer-manifest.json")
    require(
        {str(p.relative_to(destination)) for p in destination.rglob("*") if p.is_file()}
        == set(manifest) | {"transfer-manifest.json"},
        "collection inventory differs",
    )
    for name, expected in manifest.items():
        path = destination / name
        require(
            path.stat().st_size == expected["bytes"] and file_hash(path) == expected["sha256"],
            "collection digest differs",
        )
    (packet / "metadata.tar").unlink()
    write_new(packet / "collection-complete.json", dict(utc=utc(), files=len(manifest)))
    return len(manifest)


def run(packet):
    packet = Path(packet).resolve()
    os.umask(0o077)
    require(not (packet / "complete.json").exists(), "campaign already closed; no activation replay")
    op, deployment = read_json(packet / "operator.json"), read_json(packet / "deployment.json")
    require(deployment["limits"] == LIMITS, "frozen campaign limits differ")
    with lock(packet / "controller.lock"):
        journal, record = public_cloud.authorize(packet, limits=LIMITS)
        import flowdc_ops as ops
        from flowdc_experiment_transport import remaining

        profile = ops.load_profile(journal.read()["profile_path"])
        started = record["clock"]["utc"]
        terminal = False
        failure = None
        overhead_file = packet / "lifecycle-overhead.jsonl"
        overhead_history = (
            [json.loads(line)["seconds"] for line in overhead_file.read_text().splitlines()]
            if overhead_file.exists()
            else []
        )
        for number in range(LIMITS["max_windows"]):
            closed = packet / f"window-{number:02}.json"
            if closed.exists():
                continue
            intent = packet / f"window-{number:02}-intent.json"
            require(not intent.exists(), "unfinished window; no activation replay")
            controller = cloud.controller_for(packet)
            window_seconds = 3600
            full_window = (
                time.time() + 3600 < started + LIMITS["wall_seconds"]
                and exposure(record, journal) + 32 <= LIMITS["budget_su"]
            )
            if not full_window or number == LIMITS["max_windows"] - 1:
                terminal = True
                failure = failure or dict(
                    utc=utc(), error="finite acquisition envelope exhausted; metadata collection only"
                )
                window_seconds = 1800
                if (
                    (packet / "collection-complete.json").exists()
                    or time.time() + window_seconds >= started + LIMITS["wall_seconds"]
                    or exposure(record, journal) + 16 > LIMITS["budget_su"]
                    or not (packet / "volumes/manager/installation.json").exists()
                ):
                    value = dict(
                        utc=utc(),
                        status="blocked",
                        failure=failure,
                        windows=number,
                        collected=(packet / "collection-complete.json").exists(),
                        cleanup_complete=not any(
                            v["account"]["obligation"] or v["account"]["uncertain"]
                            for v in journal.read()["vms"].values()
                        ),
                        new_exposure_su=exposure(record, journal),
                    )
                    write_new(packet / "complete.json", value)
                    return value
            controller.preflight(window_seconds)
            write_new(intent, dict(utc=utc(), window=number))
            window_started = time.monotonic()

            def work_left():
                return min(
                    remaining(local_status(journal)),
                    window_started + window_seconds - LIMITS["cleanup_seconds"] - time.monotonic(),
                )

            outcome = None
            jobs_started = 0
            setup_seconds, one_time_install = 0.0, 0.0
            try:
                controller.call("start", seconds=30, window=window_seconds)
                ready(journal)
                cloud.refresh_ssh(packet, controller)
                write_new(packet / f"window-{number:02}-containment.json", controller.containment(120))
                for role in ROLES:
                    ssh(packet, role, ["/usr/bin/true"])
                    fresh = not (packet / "volumes" / role / "installation.json").exists()
                    stage_started = time.monotonic()
                    stage(packet, role, profile)
                    if fresh:
                        one_time_install += time.monotonic() - stage_started
                root = op["guest_root"]
                # Bounded sequential real-image probes run once, on a downloader.
                if not terminal:
                    gates = json_command(
                        packet,
                        "worker",
                        """import json,pathlib,socket,sys
    sys.path.insert(0,sys.argv[1]+'/source')
    from benchmark.real_server_study.common import replace_derived
    from benchmark.real_server_study.public_batch import host_gate
    root=pathlib.Path(sys.argv[1]);study=root/'study'
    replace_derived(study/'execution.json',dict(executor='jetstream2',vm_id=sys.argv[2],campaign_id=sys.argv[3],hostname=socket.gethostname(),deadline_utc=float(sys.argv[4]),source_root=str(root/'source')))
    print(json.dumps({source:host_gate(study,source) for source in ('inaturalist-open','openimages-cvdf')}))""",
                        root,
                        deployment["selected"]["worker"],
                        deployment["campaign_id"],
                        str(started + 86400),
                        timeout=300,
                    )
                    json_command(
                        packet,
                        "manager",
                        """import json,pathlib,sys
    root=pathlib.Path(sys.argv[1]);gates=json.loads(sys.argv[2])
    for source,value in gates.items():
     p=root/'study/gates'/(source+'.json')
     if not p.exists():p.write_text(json.dumps(value))
    print('{}')""",
                        root,
                        json.dumps(gates),
                    )
                setup_seconds = time.monotonic() - window_started - one_time_install
                while not terminal and work_left() > 180 and time.time() < started + 75600:
                    selection = json_command(
                        packet,
                        "manager",
                        """import json,sys,shutil
sys.path.insert(0,sys.argv[1]+'/source')
from benchmark.real_server_study.minimum_queue import next_cell
disk=shutil.disk_usage(sys.argv[1])
assert disk.free>192*2**30 and disk.used+192*2**30<3584*2**30,'retention/free-space admission'
print(json.dumps(next_cell(sys.argv[1]+'/study',float(sys.argv[2]),float(sys.argv[3]),sys.argv[4]=='True')))""",
                        root,
                        str(started + 86400 - time.time()),
                        str(max(120, setup_seconds, *overhead_history)),
                        str(bool(overhead_history)),
                        timeout=120,
                    )
                    if selection and selection.get("phase") == "observe_cleanup":
                        break
                    if selection is None:
                        terminal = True
                        break
                    if work_left() <= selection["job_seconds"] + 90:
                        break
                    # Conservative reservations also cover unknown failed bytes.
                    previous = [read_json(p) for p in (packet / "jobs").glob("*/record.json")]
                    require(
                        sum(g["requests"] for g in gates.values())
                        + sum(r["rows"] for r in previous)
                        + selection["rows"]
                        <= LIMITS["max_requests"],
                        "request budget",
                    )
                    require(
                        sum(g["probe_body_bytes"] for g in gates.values())
                        + sum(r.get("body_bytes", LIMITS["job_body_bytes"]) for r in previous)
                        + LIMITS["job_body_bytes"]
                        <= LIMITS["body_bytes"],
                        "response-body budget",
                    )
                    jobs_started += 1
                    outcome = job(packet, selection, number, journal, record)
                    if outcome["status"] != "complete":
                        terminal = True
                        break
                require(
                    terminal or jobs_started or one_time_install > 0,
                    "recurring setup leaves no room for the next job; stop jobless windows",
                )
                if terminal or time.time() >= started + 75600:
                    terminal = True
                    if work_left() > 900:
                        collect(packet)
            except Exception as error:
                failure = dict(utc=utc(), error=type(error).__name__ + ": " + str(error))
                write_new(packet / f"window-{number:02}-failure.json", failure)
                # A feasibility refusal ends acquisition, but retain all available
                # measurements. Collection never launches replacement downloads.
                terminal = True
                if (
                    (packet / "volumes/manager/installation.json").exists()
                    and not (packet / "collection-intent.json").exists()
                    and work_left() > 900
                ):
                    collect(packet)
            finally:
                cleanup_started = time.monotonic()
                cloud.cleanup(packet, controller, closed, outcome)
                recurring = setup_seconds + time.monotonic() - cleanup_started
                overhead_history.append(recurring)
                with overhead_file.open("a") as stream:
                    stream.write(
                        json.dumps(
                            dict(window=number, seconds=recurring, one_time_install_seconds=one_time_install)
                        )
                        + "\n"
                    )
                    stream.flush()
                    os.fsync(stream.fileno())
            if terminal:
                if (
                    not (packet / "collection-complete.json").exists()
                    and not (packet / "collection-intent.json").exists()
                    and all((packet / "volumes" / role / "installation.json").exists() for role in ROLES)
                    and not (packet / "separate-collection-window.json").exists()
                ):
                    write_new(
                        packet / "separate-collection-window.json",
                        dict(
                            utc=utc(),
                            next_window=number + 1,
                            scope="offline metadata only; no replacement image acquisition",
                        ),
                    )
                    continue
                value = dict(
                    utc=utc(),
                    status="blocked" if failure else "collected",
                    windows=number + 1,
                    cleanup_complete=True,
                    new_exposure_su=exposure(record, journal),
                    failure=failure,
                    collected=(packet / "collection-complete.json").exists(),
                )
                write_new(packet / "complete.json", value)
                return value
        value = dict(
            utc=utc(),
            status="blocked",
            failure="finite window count exhausted",
            collected=(packet / "collection-complete.json").exists(),
            cleanup_complete=True,
            windows=LIMITS["max_windows"],
            new_exposure_su=exposure(record, journal),
        )
        write_new(packet / "complete.json", value)
        return value


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    prep = sub.add_parser("prepare")
    for name in ("study", "source", "previous", "packet"):
        prep.add_argument("--" + name, type=Path, required=True)
    execute = sub.add_parser("run")
    execute.add_argument("--packet", type=Path, required=True)
    args = parser.parse_args()
    print(
        json.dumps(
            prepare(args.study, args.source, args.previous, args.packet)
            if args.command == "prepare"
            else run(args.packet)
        )
    )
