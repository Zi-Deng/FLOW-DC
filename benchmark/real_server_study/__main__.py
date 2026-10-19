"""Explicit offline preparation, bounded guest phases, analysis and cloud bridge."""
import argparse
import asyncio
import json
import os
import socket
from pathlib import Path

from .common import protocol_check, read_json, remaining, require, terminal, write_new


def main():
    p=argparse.ArgumentParser(description=__doc__)
    sub=p.add_subparsers(dest="command",required=True)
    for name in ("prepare","guest-window","analyze","report"):
        cmd=sub.add_parser(name);cmd.add_argument("--study",required=True,type=Path)
        if name in ("prepare","guest-window"):
            cmd.add_argument("--source",required=True,type=Path)
        if name=="prepare":
            cmd.add_argument("--previous",required=True,type=Path)
        if name=="guest-window":
            cmd.add_argument("--campaign-id",required=True)
            cmd.add_argument("--vm-id",required=True)
            cmd.add_argument("--seconds",required=True,type=int)
            cmd.add_argument("--final-window",action="store_true")
    cloud=sub.add_parser("cloud-prepare")
    for name in ("source","packet","state","allocation","ssh-config","guest-python","study"):
        cloud.add_argument("--"+name,required=True)
    for name in ("cloud-bootstrap","cloud-run"):
        run=sub.add_parser(name);run.add_argument("--packet",required=True,type=Path)
        run.add_argument("--allocation",type=Path,help="Current allocation observation; defaults to the prepared private path")
    args=p.parse_args()
    os.umask(0o077)
    if args.command=="prepare":
        from .prepare import prepare
        prepare(args.study,args.source,args.previous)
    elif args.command=="guest-window":
        for name in ("POLARS_MAX_THREADS","OMP_NUM_THREADS","OPENBLAS_NUM_THREADS"):
            os.environ[name]="4"
        from .cloud import execution_receipt
        from .prepare import host_gate,probe
        from .runner import run_window
        from uuid import UUID
        require(480<=args.seconds<=1020,"finite guest window required")
        require(str(UUID(args.campaign_id))==args.campaign_id and str(UUID(args.vm_id))==args.vm_id,"invalid cloud identity")
        protocol_check(args.study,args.source)
        if (args.study/"execution.json").exists():
            old=read_json(args.study/"execution.json")
            require(old["campaign_id"]==args.campaign_id and old["vm_id"]==args.vm_id,"guest identity changed")
        receipt=execution_receipt({"campaign_id":args.campaign_id},{"worker_id":args.vm_id},args.seconds,socket.gethostname())
        # This is a current execution lease. Prior leases are copied into immutable run evidence.
        temporary=args.study/"execution.next.json"
        write_new(temporary,receipt);temporary.replace(args.study/"execution.json")
        os.sched_setaffinity(0,sorted(os.sched_getaffinity(0))[:4])
        if args.final_window and len(list((args.study/"runs").glob("*/record.json")))<28:
            terminal(args.study,"window_budget_exhausted")
        if not (args.study/"STOP.json").exists():
            try:
                if not (args.study/"host-gate.json").exists():
                    host_gate(args.study)
                if not (args.study/"probe-summary.json").exists():
                    if (args.study/"probe-results.jsonl").exists():
                        terminal(args.study,"probe_interrupted")
                    elif asyncio.run(probe(args.study))["passed"]:
                        print(json.dumps({"status":"window_complete","runs":0}));return
            except (OSError,ValueError) as error:
                terminal(args.study,"host_gate_failed",str(error))
        result=run_window(args.study,args.source)
        target=args.study/"analysis/results.json"
        if result["status"] in ("complete","stopped") and (not target.exists() or read_json(target).get("analysis_incomplete_runs",0)):
            if remaining(args.study)<930:
                print(json.dumps({"status":"analysis_pending","runs":result["runs"]}));return
            from .analyze import analyze
            analyzed=analyze(args.study,seconds=min(900,remaining(args.study)-30))
            if analyzed["analysis_incomplete_runs"]:
                result={"status":"analysis_pending","runs":result["runs"]}
        print(json.dumps(result))
    elif args.command in ("analyze","report"):
        from .analyze import analyze,report
        (analyze if args.command=="analyze" else report)(args.study)
    elif args.command=="cloud-prepare":
        from .cloud import prepare_packet
        from .storage import prepare_payload
        print(json.dumps(prepare_packet(args.source,args.packet,args.state,args.allocation,args.ssh_config,args.guest_python)))
        prepare_payload(Path(args.packet),Path(args.study))
    elif args.command=="cloud-bootstrap":
        from .cloud import bootstrap
        print(json.dumps(bootstrap(args.packet,args.allocation)))
    elif args.command=="cloud-run":
        from .cloud import run_windows
        print(json.dumps(run_windows(args.packet,args.allocation)))


if __name__=="__main__":
    main()
