"""Guest execution: one immutable first attempt per identified run."""
import contextlib
import json
import os
import shutil
import signal
import subprocess
import sys
import time

import polars as pl

from .common import ARTIFACT_CAP, RESERVE, file_hash, lock, protocol_check, read_json, remaining, require, utc, write_new
from .design import activity_intervals, config, select_fixed
from .prepare import build_main, guest_check, read_lines, robots_permit


def terminate(proc):
    if proc.poll() is None:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(proc.pid, signal.SIGTERM)
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(proc.pid, signal.SIGKILL)
            proc.wait(timeout=5)


def records(study):
    result, gap = [], False
    for run in read_json(study / "schedule.json")["runs"]:
        directory = study / "runs" / run["run_id"]
        if directory.exists():
            require(not gap,"out-of-order attempt prevents continuation")
            require((directory/"record.json").exists(), "attempt without closed record; acquisition continuation refused")
            record = read_json(directory/"record.json")
            require(all(record[k] == v for k,v in run.items()), "record identity changed")
            result.append(record)
        else:
            gap=True
    return result


def check_inputs(study, run):
    if run["phase"] != "calibration":
        plan = read_json(study / "main-plan.json")
        require(plan["protocol_sha256"] == file_hash(study/"protocol.json"), "main protocol changed")
        name = f"slices/{run['slice']}.parquet"
        require(file_hash(study/name) == plan["files"][name], "main slice changed")
        for identity,digest in plan["calibration_records"].items():
            require(file_hash(study/"runs"/identity/"record.json") == digest, "calibration changed")
    if run["phase"] == "fixed":
        selected = read_json(study/"fixed-selection.json")
        for identity, hashes in selected["evidence"].items():
            for name,digest in hashes.items():
                require(file_hash(study/"runs"/identity/name) == digest, "adaptive selection evidence changed")


def compact_outputs(directory):
    """Drop loose duplicates only; authoritative archive and all journals stay."""
    native=directory/"native"
    index=read_json(native/"outcome-index.json")
    removed=[]
    for row in index["rows"]:
        if row["disposition"]!="verified":
            continue
        for field in ("payload","metadata"):
            name=row[field]
            require("/" not in name and name not in ("overview.json","outcome-index.json") and name not in (".",".."),"unsafe loose output name")
            path=native/name
            require(path.is_file() and not path.is_symlink(),"loose output changed before compaction")
            if field=="payload":
                require(file_hash(path)==row["payload_sha256"],"loose payload changed before compaction")
            path.unlink()
            removed.append(name)
    write_new(directory/"compaction.json",{"archive_sha256":file_hash(directory/"native.tar"),
               "removed_loose_duplicates":removed,"retained":"native.tar plus native/.flowdc, overview and outcome index"})


def execute(study, source, run):
    directory = study / "runs" / run["run_id"]
    directory.mkdir(parents=True)  # An existing directory consumes the attempt.
    binding = {**run, "utc":utc(), "protocol_sha256":file_hash(study/"protocol.json"),
               "slice_sha256":file_hash(study/"slices"/f"{run['slice']}.parquet"),
               "execution_sha256":file_hash(study/"execution.json")}
    for field,name in (("main_plan_sha256","main-plan.json"),("fixed_selection_sha256","fixed-selection.json")):
        if (study/name).exists():
            binding[field] = file_hash(study/name)
    write_new(directory/"attempt.json", binding)
    selected = read_json(study/"fixed-selection.json") if run["phase"] == "fixed" else None
    cfg = config(run, study, directory, selected)
    write_new(directory/"flowdc-config.json",cfg)
    urls = pl.read_parquet(cfg["input"])["url"].to_list()
    gate = read_json(study/"host-gate.json")
    if gate.get("robots_txt"):
        robots_permit(gate["robots_txt"], urls)
    write_new(directory/"execution.json",read_json(study/"execution.json"))
    command = [sys.executable,"-B","-m","benchmark.real_server_study.launch","--study",str(study),
               "--source",str(source),"--run-id",run["run_id"]]
    reason, fatal, cooldown, offset = None, False, 0, 0
    with (directory/"stdout.log").open("x") as out, (directory/"stderr.log").open("x") as err:
        started = time.monotonic()
        proc = subprocess.Popen(command,cwd=source,stdout=out,stderr=err,start_new_session=True)
        try:
            while proc.poll() is None:
                path = directory/"request-audit.jsonl"
                if path.exists():
                    with path.open() as stream:
                        stream.seek(offset)
                        while True:
                            at = stream.tell(); line = stream.readline()
                            if not line or not line.endswith("\n"):
                                offset = at; break
                            e = json.loads(line)
                            if e["event"] in ("stop","closed"):
                                reason = reason or e.get("reason") or e.get("stop_reason")
                                fatal = fatal or e.get("fatal",False)
                                cooldown = max(cooldown,e.get("cooldown_until_utc",0))
                if time.monotonic()-started >= 300 or remaining(study) < 180:
                    reason = reason or "deadline"
                if shutil.disk_usage(study).free < RESERVE:
                    reason = "storage_reserve"; fatal = True
                if reason:
                    terminate(proc); break
                time.sleep(.1)
        finally:
            terminate(proc)
        acquired = time.monotonic()
    verification, complete = {}, False
    verify = [sys.executable,"-B","-m","benchmark.real_server_study.verify","--run-dir",str(directory),
              "--source",str(source),"--slice",cfg["input"],"--started",str(started),"--ended",str(acquired)]
    with (directory/"verification.json").open("x") as out,(directory/"verification.log").open("x") as err:
        try:
            result = subprocess.run(verify,cwd=source,stdout=out,stderr=err,timeout=120)
            if result.returncode == 0:
                verification = read_json(directory/"verification.json")
                complete = proc.returncode == 0 and reason is None
        except subprocess.TimeoutExpired:
            reason = reason or "verification_timeout"
    ended = time.monotonic()
    events = read_lines(directory/"request-audit.jsonl") if (directory/"request-audit.jsonl").exists() else []
    for event in events:
        if event["event"] in ("stop","closed"):
            reason = reason or event.get("reason") or event.get("stop_reason")
            fatal = fatal or event.get("fatal",False)
            cooldown = max(cooldown,event.get("cooldown_until_utc",0))
    complete = complete and reason is None
    if reason in ("refusal_threshold","sustained_latency"):
        cooldown = max(cooldown,time.time()+600)
    observation = verification.pop("observations",{})
    campaign_stop=None
    if verification.get("decode_failures",0)>=.05*len(urls):
        fatal,campaign_stop=True,"content_anomalies"
    dead=verification.get("row_final_status",{})
    if dead.get("404",0)+dead.get("410",0)>=.2*len(urls):
        fatal,campaign_stop=True,"dead_link_threshold"
    intervals=activity_intervals(events,started,warmup=0)
    compact_started=time.monotonic()
    if complete:
        compact_outputs(directory)
    row = {**run,"started_monotonic":started,"acquired_monotonic":acquired,"ended_monotonic":ended,
           "elapsed_s":ended-started,"acquisition_s":acquired-started,"rows":len(urls),
           "returncode":proc.returncode,"complete":complete,"termination_reason":reason,
           "fatal":fatal,"cooldown_until_utc":cooldown,"verification":verification,"observations":observation,
           "campaign_stop_reason":campaign_stop,
           "client": {"peak_inflight":max([e["inflight"] for e in events if e["event"]=="dispatch"]+[0]),
                      "network_seconds_above_4":sum(b-a for a,b,n,_ in intervals if n>4),
                      "network_seconds_above_16":sum(b-a for a,b,n,_ in intervals if n>16),
                      "peak_dispatch_cap":max([e["cap"] for e in events if e["event"]=="dispatch" and e.get("cap") is not None]+[0])},
           "goodput_Bps":verification["credited_bytes"]/(ended-started) if complete else None,
           "coverage":verification["credited_rows"]/len(urls) if complete else None,
           "status":"complete" if complete else "stopped" if reason else "incomplete",
           "compaction_s":time.monotonic()-compact_started,
           "evidence":{p.name:file_hash(p) for p in directory.iterdir() if p.is_file()}}
    if complete:
        for name in ("native/.flowdc/final.json","native/.flowdc/owner.json","native/outcome-index.json","native/overview.json"):
            row["evidence"][name]=file_hash(directory/name)
    write_new(directory/"record.json",row)
    return row


def run_window(study, source):
    guest_check(study)
    protocol_check(study,source)
    if (study/"STOP.json").exists():
        return {"status":"stopped","runs":len(list((study/"runs").glob("*/record.json")))}
    with lock(study/"study.lock"):
        for _ in range(2):
            old = records(study)
            if old and (old[-1]["fatal"] or old[-1]["status"] == "incomplete" or
                        (len(old)>1 and all(r["termination_reason"]=="refusal_threshold" for r in old[-2:]))):
                write_new(study/"STOP.json",{"utc":utc(),"reason":"fatal, incomplete, or repeated refusal evidence"})
                return {"status":"stopped","runs":len(old)}
            if len(old) == 4 and not (study/"main-plan.json").exists():
                build_main(study)
            if len(old) == 16 and not (study/"fixed-selection.json").exists():
                selection = select_fixed(old,[read_lines(study/"runs"/r["run_id"]/"request-audit.jsonl") for r in old],
                                         read_json(study/"probe-summary.json")["ttfb_p50_s"])
                selection["evidence"] = {r["run_id"]:{name:file_hash(study/"runs"/r["run_id"]/name)
                    for name in ("record.json","request-audit.jsonl")} for r in old}
                write_new(study/"fixed-selection.json",selection)
            if len(old) == 28:
                return {"status":"complete","runs":28}
            wait = max([r["cooldown_until_utc"] for r in old]+[0])-time.time()
            if wait > 0:
                return {"status":"cooldown","until_utc":time.time()+wait,"runs":len(old)}
            if remaining(study) < 480:
                return {"status":"window_complete","runs":len(old)}
            require(shutil.disk_usage(study).free >= RESERVE, "storage reserve exhausted")
            retained = sum(p.stat().st_size for p in study.glob("runs/*/native.tar"))
            require(retained + 8*2**30 <= ARTIFACT_CAP, "artifact budget insufficient for another bounded run")
            run = read_json(study/"schedule.json")["runs"][len(old)]
            check_inputs(study,run)
            execute(study,source,run)
            # Always finish a phase in its own window so ordering gates are visible.
            if len(old)+1 in (4,16,28):
                break
            time.sleep(30)
    return {"status":"window_complete","runs":len(records(study))}
