"""Finite eight-hour study bridge over the existing supervised VM lifecycle.

No PR/merge prerequisite for this explicitly authorized study exception. The
staged commit still needs exact-head hosted CI and bounded independent review.
"""

import argparse
import datetime as dt
import json
import math
import os
import subprocess
import time
from pathlib import Path
from uuid import uuid4

from . import ablation, cloud, onset_cloud, storage
from .common import (
    file_hash,
    lock,
    read_json,
    replace_derived,
    require,
    sha256,
    source_identity,
    utc,
    write_new,
)

# Two leases plus settlement/preflight margin. A deferred audit additionally
# needs 3600s admission after up to1200s settlement and fresh preflight.
FINALIZATION_RESERVE = 9000
NEXT_WINDOW_RESERVE = 4800


def prepare(study, source, previous, packet):
    study, source, previous, packet = map(lambda p: Path(p).resolve(), (study, source, previous, packet))
    ablation.check(study, source)
    require(
        not packet.exists()
        and not subprocess.check_output(
            ["git", "-C", str(source), "status", "--porcelain"], text=True
        ).strip(),
        "commit source; use new packet",
    )
    op = read_json(previous / "operator.json")
    Journal, _, _, _ = cloud.imports(source)
    current = Journal(op["state"]).read()
    require(
        current["desired"] == "idle"
        and not any(v["account"]["obligation"] or v["account"]["uncertain"] for v in current["vms"].values()),
        "prior cleanup unresolved",
    )
    selected = {
        v["role"]: v["id"] for v in current["spec"]["vms"] if v["role"] in ("manager", "worker", "origin")
    }
    require(set(selected) == {"manager", "worker", "origin"}, "trusted three-VM topology required")
    packet.mkdir(parents=True, mode=0o700)
    campaign = str(uuid4())
    identity = source_identity(source)
    with (packet / "source.tar").open("xb") as out:
        subprocess.run(["git", "-C", str(source), "archive", "HEAD"], stdout=out, check=True, timeout=60)
    write_new(
        packet / "deployment.json",
        dict(
            campaign_id=campaign,
            source=identity,
            selected=selected,
            source_archive_sha256=file_hash(packet / "source.tar"),
            protocol_sha256=file_hash(study / "protocol.json"),
            limits=ablation.LIMITS,
            registered_rate_total_per_hour=32,
            authority="Maintainer approved adaptive150 plan; execute before one final PR; no emails or agent merge.",
        ),
    )
    write_new(
        packet / "operator.json",
        {
            **op,
            "source": str(source),
            "study": str(study),
            "previous_packet": str(previous),
            "guest_root": "/mnt/flowdc-external-" + campaign,
            "branch": subprocess.check_output(
                ["git", "-C", str(source), "branch", "--show-current"], text=True
            ).strip(),
        },
    )
    write_new(packet / "ssh-identity.json", read_json(previous / "ssh-identity.json"))
    storage.prepare_payload(packet, study)
    return read_json(packet / "deployment.json")


def qualified(packet, deployment):
    proof = read_json(packet / "qualification.json")
    head = deployment["source"]["head"]
    reviewed = proof.get("reviewed_head", head)
    require(proof["head"] == head, "qualification source changed")
    review = Path(proof["review_directory"])
    meta = read_json(review / "review.json")
    report = read_json(review / "report.json")
    require(
        meta["repository"] == "Zi-Deng/FLOW-DC"
        and meta["head"] == reviewed
        and meta["status"] == "completed"
        and meta["report_sha256"] == file_hash(review / "report.json")
        and file_hash(review / "review.json") == proof["review_metadata_sha256"],
        "review qualification invalid",
    )
    material = {str(i) for i, f in enumerate(report["findings"]) if f["severity"] in ("P0", "P1", "P2")}
    if reviewed == head:
        require(not material, "material review findings remain")
    else:
        # Maintainer's two-review/two-repair policy: qualify the final bounded
        # repair delta explicitly, never relabel the old review as final-head.
        assessment = read_json(packet / "final-repair-assessment.json")
        source = read_json(packet / "operator.json")["source"]
        delta = subprocess.check_output(
            ["git", "-C", source, "diff", "--no-ext-diff", "--no-textconv", reviewed, head]
        )
        paths = set(
            subprocess.check_output(
                ["git", "-C", source, "diff", "--name-only", reviewed, head], text=True
            ).splitlines()
        )
        allowed = {
            "benchmark/real_server_study/ablation_cloud.py",
            "benchmark/real_server_study/ablation_collect.py",
            "benchmark/real_server_study/ablation_analysis.py",
            "tests/test_adaptive_ablation.py",
            "docs/research/ADAPTIVE-150.md",
        }
        require(
            meta["invocation"] == 2
            and assessment["review_invocations_used"] == 2
            and assessment["repair_rounds_used"] == 2
            and assessment["reviewed_head"] == reviewed
            and assessment["final_head"] == head
            and assessment["report_sha256"] == meta["report_sha256"]
            and assessment["delta_sha256"] == sha256(delta)
            and file_hash(packet / "final-repair-assessment.json") == proof["repair_assessment_sha256"]
            and set(assessment["material_dispositions"]) == material
            and all(v["status"] == "fixed" for v in assessment["material_dispositions"].values())
            and assessment["independent_final_head_review"] is False
            and bool(paths)
            and paths <= allowed,
            "bounded final repair assessment invalid",
        )
    observations = []
    for run_id in proof["ci_runs"]:
        run = json.loads(
            subprocess.check_output(
                [
                    "gh",
                    "run",
                    "view",
                    str(run_id),
                    "--repo",
                    "Zi-Deng/FLOW-DC",
                    "--json",
                    "headSha,conclusion,name,event",
                ],
                text=True,
                timeout=30,
            )
        )
        require(
            run["headSha"] == head and run["conclusion"] == "success" and run["event"] == "workflow_dispatch",
            "exact-head branch CI required",
        )
        observations.append(run)
    require(
        {r["name"] for r in observations} >= {"FLOW-DC regression tests", "agentic workflow tests"},
        "required CI missing",
    )
    replace_derived(
        packet / "qualification-observation.json",
        dict(
            utc=utc(),
            head=head,
            reviewed_head=reviewed,
            independent_final_head_review=reviewed == head,
            repair_assessment_sha256=proof.get("repair_assessment_sha256"),
            ci=observations,
            review=meta,
        ),
    )


def wall_left(record):
    from flowdc_pilot_supervisor import sample_clock

    now = sample_clock()
    clock = record["clock"]
    require(now.boot_id == clock["boot_id"], "controller reboot invalidates clock")
    return min(
        clock["utc"] + ablation.LIMITS["wall_seconds"] - now.utc,
        clock["boottime"] + ablation.LIMITS["wall_seconds"] - now.boottime,
    )


def authorize(packet):
    deployment, op = read_json(packet / "deployment.json"), read_json(packet / "operator.json")
    qualified(packet, deployment)
    require(
        source_identity(op["source"]) == deployment["source"]
        and not subprocess.check_output(
            ["git", "-C", op["source"], "status", "--porcelain"], text=True
        ).strip(),
        "prepared source changed",
    )
    Journal, allowance, binding, account_digest = cloud.imports(op["source"])
    import flowdc_ops as ops
    from flowdc_pilot_provider import Provider
    from flowdc_pilot_supervisor import sample_clock

    journal = Journal(op["state"])
    current = journal.read()
    facts = read_json(op["allocation_file"])
    cutoff = dt.datetime.fromisoformat(facts["expiry_date"]).replace(tzinfo=dt.UTC).timestamp()
    require(
        facts["allocation"] == "CIS240296" and facts["cpu_su"] >= 300 and time.time() + 32400 < cutoff,
        "allocation insufficient",
    )
    target = packet / "campaign.json"
    if target.exists():
        record = read_json(target)
        require(
            record["limits"] == ablation.LIMITS
            and record["protocol_sha256"] == deployment["protocol_sha256"],
            "campaign changed",
        )
        require(
            (packet / "grant-application.json").exists()
            and read_json(packet / "grant-application.json")["returncode"] == 0,
            "grant application uncertain; reconcile before activation",
        )
        return journal, record
    provider = Provider(ops.load_profile(current["profile_path"]))
    provider.verify_idle(current)
    rates = cloud.verify_rates(provider, current, deployment)
    chosen = sorted(deployment["selected"].values())
    clock = sample_clock()
    record = dict(
        campaign_id=deployment["campaign_id"],
        limits=ablation.LIMITS,
        clock=vars(clock),
        protocol_sha256=deployment["protocol_sha256"],
        allocation_basis=facts,
        baseline={k: current["vms"][k]["account"]["consumed"] for k in chosen},
        rates={r["id"]: r["su_per_hour"] for r in rates["servers"]},
    )
    limits = {
        k: max(
            math.ceil(allowance(current["vms"][k]["account"]).effective_limit) + 1,
            math.ceil(current["vms"][k]["account"]["consumed"]) + 28800,
        )
        for k in chosen
    }
    preview = sum((limits[k] - record["baseline"][k]) * record["rates"][k] / 3600 for k in chosen)
    require(preview <= 1000, "preserved historical cumulative authority exceeds1000SU")
    record.update(preserved_cumulative_preview_su=preview, new_admission_limit_su=300)
    write_new(target, record)
    write_new(packet / "rates.json", rates)
    request = dict(
        schema="flowdc-campaign-authorization-v1",
        campaign_id=record["campaign_id"],
        registration_id=current["registration_id"],
        expected_binding_sha256=binding(current),
        expected_accounts_sha256=account_digest(current),
        selected_ids=chosen,
        cumulative_limits_seconds=limits,
        expires_at=dt.datetime.fromtimestamp(clock.utc + 28800, dt.UTC).isoformat(),
        max_window_seconds=3600,
        protocol_sha256=file_hash(target),
        budget_sha256=file_hash(target),
        budget_su=1000,
    )
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
    require(call.returncode == 0, "grant refused; no activation")
    return journal, record


def remote(packet, number, action, seconds):
    op = read_json(packet / "operator.json")
    deployment = read_json(packet / "deployment.json")
    install = read_json(packet / "deployment-receipt.json")
    root = op["guest_root"]
    unit = f"flowdc-onset-{deployment['campaign_id']}-{number}-{action}"
    if action == "acquire":
        command = [
            install["python"],
            "-B",
            "-m",
            "benchmark.real_server_study.ablation_run",
            "window",
            "--study",
            root + "/study",
            "--source",
            root + "/source",
            "--campaign-id",
            deployment["campaign_id"],
            "--vm-id",
            op["worker_id"],
            "--seconds",
            str(seconds),
        ]
    else:
        script = "from pathlib import Path;import json,sys;from benchmark.real_server_study.ablation_analysis import audit;print(json.dumps(audit(Path(sys.argv[1]),Path(sys.argv[2]),int(sys.argv[3]))))"
        command = [install["python"], "-B", "-c", script, root + "/study", root + "/source", str(seconds)]
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
        "--property=RuntimeMaxSec=" + str(seconds + 15),
        "--property=TimeoutStopSec=30",
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
    return json.loads(cloud.ssh(packet, wrapper, timeout=seconds + 60))


def run(packet):
    from .ablation_analysis import report
    from .ablation_collect import collect

    packet = Path(packet).resolve()
    op = read_json(packet / "operator.json")
    deployment = read_json(packet / "deployment.json")
    Journal, _, _, _ = cloud.imports(op["source"])
    import flowdc_ops as ops
    from flowdc_experiment_transport import idle, remaining
    from flowdc_pilot_provider import Provider

    with lock(packet / "controller.lock"):
        if (packet / "collected").exists():
            return dict(status="collected", report=str(report(packet / "collected")), cloud_activation=False)
        if (packet / "collection.tar.gz").exists() and (packet / "collection.json").exists():
            # A local extraction retry requires no VM window or image request.
            record = read_json(packet / "campaign.json")
            seconds = min(900, wall_left(record) - 120)
            require(seconds > 0, "finite report/collection reserve exhausted")
            collect(packet, seconds)
            return dict(status="collected", report=str(report(packet / "collected")), cloud_activation=False)
        journal, record = authorize(packet)
        for number in range(ablation.LIMITS["max_windows"]):
            closed = packet / f"window-{number:02}.json"
            if closed.exists():
                require(read_json(closed)["cleaned"], "cleanup unresolved")
                continue
            require(
                not (packet / f"window-{number:02}-intent.json").exists(),
                "window uncertain; reconcile, no replacement",
            )
            require(
                wall_left(record) >= 3600 and onset_cloud.exposure(record, journal) + 32 <= 300,
                "finite window admission exhausted",
            )
            current = journal.read()
            require(
                all(
                    current["vms"][k]["account"]["campaign_id"] == record["campaign_id"]
                    for k in record["baseline"]
                ),
                "campaign binding changed",
            )
            # preflight refreshes stale idle observations and verifies enrolled
            # offload/network state; avoid a duplicate full inventory here.
            controller = cloud.controller_for(packet)
            controller.preflight(3600)
            write_new(
                packet / f"window-{number:02}-intent.json", dict(utc=utc(), campaign_id=record["campaign_id"])
            )
            outcome = None
            safe = True
            try:
                controller.call("start", seconds=30, window=3600)
                onset_cloud.ready(journal)
                cloud.refresh_ssh(packet, controller)
                cloud.wait_ssh(packet, controller, number)
                if not (packet / "volume.json").exists():
                    storage.create_volume(
                        packet, ops.load_profile(current["profile_path"]), op["worker_id"], size_gib=384
                    )
                storage.mount(packet, new=not (packet / "volume-mount.json").exists())
                if not (packet / "deployment-receipt.json").exists():
                    require(
                        not (packet / "staging-intent.json").exists(), "staging interrupted; no overwrite"
                    )
                    write_new(packet / "staging-intent.json", dict(utc=utc()))
                    storage.stage(packet)
                seconds = int(
                    min(
                        2820,
                        remaining(onset_cloud.local_status(journal)) - 90,
                        wall_left(record) - NEXT_WINDOW_RESERVE,
                    )
                )
                # Keep one full window available for audit/collection/cleanup.
                offline_only = (
                    (packet / "acquisition-ended.json").exists()
                    or number == 7
                    or wall_left(record) < FINALIZATION_RESERVE
                    or onset_cloud.exposure(record, journal) + 64 > 300
                )
                if offline_only:
                    outcome = dict(status="stopped", reason="offline-only finalization")
                elif seconds >= 120:
                    try:
                        outcome = remote(packet, number, "acquire", seconds)
                    except (OSError, ValueError, subprocess.SubprocessError) as error:
                        outcome = dict(
                            status="stopped",
                            reason="acquisition invocation failed",
                            error_type=type(error).__name__,
                        )
                        try:
                            write_new(
                                packet / f"window-{number:02}-quiescence.json",
                                onset_cloud.quiesce(packet, number),
                            )
                        except (OSError, ValueError, subprocess.SubprocessError):
                            safe = False
                    write_new(packet / f"window-{number:02}-guest.json", outcome)
                if outcome and outcome["status"] == "cooldown":
                    can_recover = (
                        number < 6
                        and outcome["until_utc"] - time.time() + FINALIZATION_RESERVE < wall_left(record)
                        and onset_cloud.exposure(record, journal) + 64 <= 300
                    )
                    if not can_recover:
                        write_new(
                            packet / "recovery-skipped.json",
                            dict(
                                utc=utc(),
                                reason="cooldown and finalization cannot fit",
                                requested_cooldown_until=outcome["until_utc"],
                            ),
                        )
                        outcome = dict(outcome, status="stopped", recovery="not attempted: finite reserve")
                if (
                    outcome
                    and outcome["status"] in ("complete", "stopped")
                    and not (packet / "acquisition-ended.json").exists()
                ):
                    write_new(packet / "acquisition-ended.json", outcome)
                if safe and outcome and outcome["status"] in ("complete", "stopped"):
                    previous_audits = sorted(packet.glob("window-*-audit.json"))
                    pending_audit = (
                        read_json(previous_audits[-1]).get("audit_pending", 0) > 0
                        if previous_audits
                        else True
                    )
                    seconds = int(
                        min(
                            1200 - onset_cloud.audit_spent(packet),
                            remaining(onset_cloud.local_status(journal)) - 1050,
                            wall_left(record) - 2100,
                        )
                    )
                    if seconds >= 60:
                        write_new(
                            packet / f"window-{number:02}-audit-intent.json", dict(utc=utc(), seconds=seconds)
                        )
                        audited = remote(packet, number, "audit", seconds)
                        write_new(packet / f"window-{number:02}-audit.json", audited)
                        pending_audit = audited.get("audit_pending", 0) > 0
                    # Finish resumable audit units in the next offline window
                    # before sealing collection, unless its finite budget is
                    # exhausted or this is already the final window.
                    continue_audit = (
                        pending_audit
                        and onset_cloud.audit_spent(packet) <= 1140
                        and number < 7
                        and wall_left(record) >= NEXT_WINDOW_RESERVE
                        and onset_cloud.exposure(record, journal) + 32 <= 300
                    )
                    if not continue_audit and remaining(onset_cloud.local_status(journal)) >= 180:
                        seconds = int(
                            min(
                                900,
                                remaining(onset_cloud.local_status(journal)) - 60,
                                wall_left(record) - 1200,
                            )
                        )
                        if seconds >= 60:
                            collect(packet, seconds)
            finally:
                controller.call("stop", seconds=30)
                deadline = time.monotonic() + 600
                while not idle(onset_cloud.local_status(journal)):
                    require(time.monotonic() < deadline, "cleanup unresolved; supervisor stays running")
                    time.sleep(5)
                Provider(ops.load_profile(journal.read()["profile_path"])).verify_idle(journal.read())
                write_new(
                    closed,
                    dict(
                        utc=utc(),
                        cleaned=True,
                        outcome=outcome,
                        new_exposure_su=onset_cloud.exposure(record, journal),
                        fresh_offload_and_network_verified=True,
                    ),
                )
            if (packet / "collected").exists():
                pdf = report(packet / "collected")
                write_new(
                    packet / "complete.json",
                    dict(
                        utc=utc(),
                        report=str(pdf),
                        windows_used=number + 1,
                        new_exposure_su=onset_cloud.exposure(record, journal),
                        cleanup_complete=True,
                        wall_seconds=28800 - wall_left(record),
                    ),
                )
                return read_json(packet / "complete.json")
            if outcome and outcome["status"] == "cooldown":
                wait = outcome["until_utc"] - time.time()
                while wait > 0:
                    time.sleep(min(30, wait))
                    wait = outcome["until_utc"] - time.time()
        raise ValueError("eight-window envelope exhausted; retain artifacts and report gaps")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("action", choices=["prepare", "run"])
    p.add_argument("--packet", type=Path, required=True)
    p.add_argument("--study", type=Path)
    p.add_argument("--source", type=Path)
    p.add_argument("--previous", type=Path)
    a = p.parse_args()
    os.umask(0o077)
    print(
        json.dumps(
            prepare(a.study, a.source, a.previous, a.packet) if a.action == "prepare" else run(a.packet)
        )
    )
