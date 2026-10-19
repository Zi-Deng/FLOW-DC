"""Private deployment packet and existing supervised three-VM window bridge.

Preparation never activates guests. Execution requires a populated allocation,
reviewed merged source and explicitly applied finite campaign grant.
"""
import datetime as dt
import json
import math
import shlex
import subprocess
import sys
import time
from pathlib import Path
from uuid import uuid4

from .common import CAMPAIGN_SECONDS, MAX_WINDOWS, OPERATIONAL_SU, file_hash, read_json, require, source_identity, utc, write_new


def imports(source):
    sys.path.insert(0,str(Path(source)/"bin"))
    from flowdc_pilot_journal import Journal, allowance, allowance_binding_sha256
    from flowdc_campaign import account_digest
    return Journal,allowance,allowance_binding_sha256,account_digest


def capacity_plan():
    # One worst-case run per window, distinct bootstrap/analysis/collection,
    # plus five contingency windows. No extra scientific attempts are granted.
    required=28+3
    require(MAX_WINDOWS>=required+2 and MAX_WINDOWS*1800<=CAMPAIGN_SECONDS,"insufficient worst-case window capacity")
    require(300+300+180+30<=1020 and 32*CAMPAIGN_SECONDS/3600<=OPERATIONAL_SU,"execution/SU capacity insufficient")
    return {"scientific_runs":28,"core_windows":required,"contingency_windows":MAX_WINDOWS-required,
            "worst_case_run_seconds":810,"nominal_envelope_su":32*CAMPAIGN_SECONDS/3600}


def prepare_packet(source, directory, state, allocation, ssh_config, guest_python):
    source, directory, state = Path(source).resolve(),Path(directory).resolve(),Path(state).resolve()
    require(not directory.exists(), "deployment packet exists; preserve it")
    require(not subprocess.check_output(["git","-C",str(source),"status","--porcelain","--untracked-files=normal"],
            text=True,timeout=30).strip(), "deployment requires a clean committed source worktree")
    capacity=capacity_plan()
    Journal,allowance,binding,account_digest = imports(source)
    j=Journal(state).read()
    require(j["desired"]=="idle" and not any(v["account"]["obligation"] or v["account"]["uncertain"] for v in j["vms"].values()), "outstanding accounting/cleanup")
    selected={v["role"]:v for v in j["spec"]["vms"] if v["role"] in ("manager","worker","origin")}
    require(set(selected)=={"manager","worker","origin"}, "registered three-role topology required")
    facts=read_json(allocation)
    require(facts["allocation"]=="CIS240296" and facts["expiry_date"]=="2027-04-17", "allocation renewal facts missing")
    campaign=str(uuid4())
    rates={v["id"]:v["rate"]["su_per_hour"] for v in selected.values()}
    limits={key:max(allowance(j["vms"][key]["account"]).effective_limit+1,
                    math.ceil(allowance(j["vms"][key]["account"]).consumed)+CAMPAIGN_SECONDS) for key in rates}
    preview=sum((limits[k]-j["vms"][k]["account"]["consumed"])*rates[k]/3600 for k in rates)
    require(preview <= 1000, "existing-account preview exceeds selected 1000SU authorization bound")
    directory.mkdir(mode=0o700,parents=True)
    with (directory/"source.tar").open("xb") as output:
        subprocess.run(["git","-C",str(source),"archive","HEAD"],stdout=output,check=True,timeout=60)
    identity=source_identity(source)
    protocol={"schema":"flowdc-external-deployment-v1","campaign_id":campaign,"source":identity,
              "capacity_plan":capacity,
              "source_archive_sha256":file_hash(directory/"source.tar"),"selected":{k:v["id"] for k,v in selected.items()},
              "max_windows":MAX_WINDOWS,"max_window_seconds":1800,"deadline_seconds":CAMPAIGN_SECONDS,
              "operational_budget_su":OPERATIONAL_SU,"controller_authorization_budget_su":1000,
              "allocation":facts,"registered_rate_total_per_hour":sum(rates.values()),
              "conservative_grant_preview_su":preview,"storage_gib":256,"retention_cap_gib":160,
              "reserve_gib":40,"cleanup":"all registered guests freshly SHELVED_OFFLOADED; preserve volume/evidence"}
    protocol.update(storage_accounting="total campaign-volume filesystem use; includes loose outputs and journals",
                    metadata_transfer_cap_gib=8,local_cache_peak_gib=16)
    write_new(directory/"deployment.json",protocol)
    grant={"schema":"flowdc-campaign-authorization-v1","campaign_id":campaign,
           "registration_id":j["registration_id"],"expected_binding_sha256":binding(j),
           "expected_accounts_sha256":account_digest(j),"selected_ids":sorted(rates),
           "cumulative_limits_seconds":limits,
           # Populated only at application, after fresh inventory and source review.
           "expires_at":None,"max_window_seconds":1800,"protocol_sha256":file_hash(directory/"deployment.json"),
           "budget_sha256":file_hash(directory/"deployment.json"),"budget_su":1000}
    write_new(directory/"grant-template.json",grant)
    write_new(directory/"operator.json",{"state":str(state),"source":str(source),"ssh_config":str(Path(ssh_config).resolve()),
              "allocation_file":str(Path(allocation).resolve()),
              "guest_python":guest_python,"guest_root":"/mnt/flowdc-external-"+campaign,
              "worker_id":selected["worker"]["id"],"registration":j["registration_id"],"utc":utc(),
              "branch":subprocess.check_output(["git","-C",str(source),"branch","--show-current"],text=True).strip()})
    ssh_fields={}
    for line in Path(ssh_config).read_text().splitlines():
        fields=shlex.split(line)
        if len(fields)==2 and fields[0].lower() in ("identityfile","userknownhostsfile","user"):
            ssh_fields.setdefault(fields[0].lower(),fields[1])
    require(set(ssh_fields)=={"identityfile","userknownhostsfile","user"},"trusted SSH identity references unavailable")
    write_new(directory/"ssh-identity.json",{"identity_file":ssh_fields["identityfile"],"known_hosts":ssh_fields["userknownhostsfile"],
              "user":ssh_fields["user"],"known_hosts_sha256":file_hash(ssh_fields["userknownhostsfile"])})
    return protocol


def ssh_configuration(packet):
    current=packet/"current-ssh.conf"
    require(current.exists(),"fresh verified guest addresses required")
    require(file_hash(current)==(packet/"current-ssh.sha256").read_text().strip(),"prepared SSH routing changed")
    return str(current)


def refresh_ssh(packet,controller):
    from flowdc_experiment_transport import ssh_config,ssh_preflight
    op=read_json(packet/"operator.json")
    Journal,_,_,_=imports(op["source"])
    record=Journal(op["state"]).read()
    spec=read_json(packet/"ssh-identity.json")
    require(ssh_preflight({k:spec[k] for k in ("identity_file","known_hosts","user")},record)==spec["known_hosts_sha256"],"trusted host keys changed")
    configuration=ssh_config(spec,record,controller.addresses(120))
    next_path=packet/"current-ssh.next"
    write_new(next_path,configuration);next_path.replace(packet/"current-ssh.conf")
    next_hash=packet/"current-ssh.sha256.next"
    write_new(next_hash,(file_hash(packet/"current-ssh.conf")+"\n").encode());next_hash.replace(packet/"current-ssh.sha256")


def ssh(packet, arguments, timeout=30, data=None):
    operator=read_json(packet/"operator.json")
    require(Path(operator["ssh_config"]).is_file(), "trusted SSH config missing")
    # Exact role and argv, always quoted. No private key bytes or credential environment copied.
    result=subprocess.run(["ssh","-F",ssh_configuration(packet),"--","worker",shlex.join(arguments)],
                          input=data,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=timeout)
    if result.returncode:
        write_new(packet/f"guest-failure-{time.time_ns()}.json",{"returncode":result.returncode,
                  "stderr":result.stderr[-32768:].decode("utf8","replace"),"utc":utc()})
    require(result.returncode==0,"guest command failed; preserve private diagnostic, no replacement")
    return result.stdout


def allocation_gate(deployment):
    facts=deployment["allocation"]
    require(type(facts.get("cpu_su")) in (int,float) and facts["cpu_su"]>=1000,"current CPU SU balance missing/insufficient")
    observed=dt.datetime.fromisoformat((facts.get("observed_at") or facts["reported_at"]).replace("Z","+00:00"))
    require(observed.tzinfo is not None and 0<=time.time()-observed.timestamp()<=86400,"allocation observation stale")
    cutoff=dt.datetime.fromisoformat(facts["expiry_date"]).replace(tzinfo=dt.UTC).timestamp()
    require(time.time()+CAMPAIGN_SECONDS+3600<cutoff,"allocation cutoff does not cover campaign and cleanup")


def execution_receipt(deployment, operator, seconds, hostname):
    now=time.time()
    return {"executor":"jetstream2","campaign_id":deployment["campaign_id"],"vm_id":operator["worker_id"],
            "hostname":hostname,"deadline_utc":now+seconds,
            "deadline_monotonic":time.monotonic()+seconds,
            "boot_id":Path("/proc/sys/kernel/random/boot_id").read_text().strip()}


def merged_gate(packet,allocation=None):
    deployment,op=read_json(packet/"deployment.json"),read_json(packet/"operator.json")
    path=Path(allocation or op["allocation_file"])
    facts=read_json(path)
    require(all(facts[k]==deployment["allocation"][k] for k in ("allocation","expiry_date")),"allocation identity/renewal changed")
    allocation_gate({"allocation":facts})
    write_new(packet/f"allocation-observation-{time.time_ns()}.json",
              {"utc":utc(),"sha256":file_hash(path),"facts":facts})
    require(source_identity(op["source"])==deployment["source"],"prepared source changed")
    subprocess.run(["git","-C",op["source"],"fetch","origin","main"],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=60)
    head=deployment["source"]["head"]
    if subprocess.run(["git","-C",op["source"],"merge-base","--is-ancestor",head,"FETCH_HEAD"]).returncode:
        # Squash merge creates a different commit. Require the exact PR head to
        # be merged and every staged runtime path to match current main.
        prs=json.loads(subprocess.check_output(["gh","pr","list","--repo","Zi-Deng/FLOW-DC","--state","merged",
              "--head",op["branch"],"--json","headRefOid"],text=True,timeout=30))
        require(any(p["headRefOid"]==head for p in prs),"human merge required")
        require(subprocess.run(["git","-C",op["source"],"diff","--quiet",head,"FETCH_HEAD","--",
                *deployment["source"]["files"]]).returncode==0,"merged runtime differs from prepared source")
    return deployment,op


def controller_for(packet):
    deployment,op=read_json(packet/"deployment.json"),read_json(packet/"operator.json")
    Journal,_,_,_=imports(op["source"])
    from flowdc_experiment_transport import Controller
    from flowdc_topology import selection
    j=Journal(op["state"]).read()
    expected={**j,"selection":selection(j["spec"],[deployment["selected"]["worker"]])}
    return Controller(op["state"],expected)


def cleanup(packet,controller,path,outcome):
    from flowdc_experiment_transport import idle
    from flowdc_pilot_provider import Provider
    import flowdc_ops as ops
    controller.call("stop",seconds=30)
    end=time.monotonic()+600
    value=controller.call("status")
    # Durable settlement can outlive the narrow interval in which all six
    # journal observations are fresh. The provider proof below is still required.
    while not idle(value):
        require(time.monotonic()<end,"cleanup unresolved; keep supervisor running")
        time.sleep(1);value=controller.call("status")
    op=read_json(packet/"operator.json")
    Journal,_,_,_=imports(op["source"])
    current=Journal(op["state"]).read()
    Provider(ops.load_profile(current["profile_path"])).verify_idle(current)
    write_new(path,{"utc":utc(),"outcome":outcome,"cleaned":True,"cleanup":value,
                   "fresh_provider_verification":"offload and original networking verified after durable idle"})


def wait_active(controller):
    from flowdc_experiment_transport import ready
    deadline=time.monotonic()+300
    value=controller.call("status")
    while not ready(value):
        require(time.monotonic()<deadline,"activation timeout")
        time.sleep(1);value=controller.call("status")
    return value


def verify_rates(provider,record,deployment):
    """Fresh flavor identity must support the retained per-VM rate assumptions."""
    import flowdc_ops as ops
    rows=[]
    for vm in record["spec"]["vms"]:
        if vm["id"] not in deployment["selected"].values():continue
        with provider.step("preflight",vm["role"]):
            provider.context(record)
            value=provider.query("server",vm["id"])
            require(provider.validate_server(record,vm["id"],value)=="SHELVED_OFFLOADED","selected VM not offloaded")
            rate=vm["rate"]
            require(rate["verified"] and ops.reference_id(value["flavor"])==rate["flavor_id"],"selected flavor/rate changed")
            rows.append({"id":vm["id"],"flavor_id":rate["flavor_id"],"su_per_hour":rate["su_per_hour"]})
    require(len(rows)==3 and sum(v["su_per_hour"] for v in rows)==deployment["registered_rate_total_per_hour"]==32,
            "selected aggregate rate differs from frozen budget")
    return {"utc":utc(),"servers":rows}


AUDIT = r'''
import json,os,sys
from pathlib import Path
for name in ('POLARS_MAX_THREADS','OMP_NUM_THREADS','OPENBLAS_NUM_THREADS'):os.environ[name]='4'
os.sched_setaffinity(0,sorted(os.sched_getaffinity(0))[:4])
from benchmark.real_server_study.common import lock,read_json,remaining
from benchmark.real_server_study.analyze import analyze
study=Path(sys.argv[1]);seconds=min(900,int(sys.argv[2])-30,remaining(study)-30)
result={'status':'analysis_pending','runs':int(sys.argv[3])}
if seconds>0:
 with lock(study/'study.lock'):
  audited=analyze(study,seconds=seconds)
 if not audited['analysis_incomplete_runs']:
  result['status']='stopped' if (study/'STOP.json').exists() else 'complete'
  if (study/'STOP.json').exists():result['reason']=read_json(study/'STOP.json')['reason']
print(json.dumps(result))
'''


def guest(packet,controller,number,final=False):
    from flowdc_experiment_transport import remaining as window_remaining
    deployment,op=read_json(packet/"deployment.json"),read_json(packet/"operator.json")
    installation=read_json(packet/"deployment-receipt.json")
    # remaining() already excludes600s cleanup plus180s provider-action lead.
    # Reserve30s for SSH dispatch, rather than a second120s work-time deduction.
    allowed=min(1020,window_remaining(controller.call("status"))-30)
    require(allowed>=480,"guest lease insufficient")
    cmd=[installation["python"],"-B","-m","benchmark.real_server_study","guest-window",
         "--study",op["guest_root"]+"/study","--source",op["guest_root"]+"/source",
         "--campaign-id",deployment["campaign_id"],"--vm-id",op["worker_id"],"--seconds",str(int(allowed))]
    if final:cmd.append("--final-window")
    unit=f"flowdc-external-{deployment['campaign_id']}-{number}"
    remote=["sudo","-n","systemd-run","--quiet","--wait","--pipe","--collect","--unit",unit,
        "--property=RuntimeMaxSec="+str(int(allowed)),"--property=TimeoutStopSec=30","--property=KillMode=control-group",
        "--property=MemoryMax=24G","--property=CPUQuota=400%","--property=LimitNOFILE=4096","--property=UMask=0077",
        "--uid=exouser","--gid=exouser","--working-directory="+op["guest_root"]+"/source",*cmd]
    started=time.monotonic()
    outcome=json.loads(ssh(packet,remote,timeout=allowed+45))
    if outcome['status']=='analysis_pending':
        # The frozen guest's930s admission is often unreachable after cold setup.
        # Invoke the SAME offline auditor within the unextended original lease.
        budget=int(min(allowed-(time.monotonic()-started),window_remaining(controller.call('status'))-120))
        if budget>=60:
            audit=remote[:]
            audit[audit.index(unit)]=unit+'-audit'
            audit[audit.index('--property=RuntimeMaxSec='+str(int(allowed)))]=('--property=RuntimeMaxSec='+str(budget))
            audit=audit[:audit.index(installation['python'])]+[installation['python'],'-B','-c',AUDIT,
                op['guest_root']+'/study',str(budget),str(outcome['runs'])]
            outcome=json.loads(ssh(packet,audit,timeout=budget+30))
    return outcome


def operational_grant(packet,op):
    """Keep identical grant bytes outside Git, as the installed CLI requires."""
    import os
    import flowdc_ops as ops
    raw=(packet/"grant.json").read_bytes()
    campaign=ops.uuid_value(read_json(packet/"deployment.json")["campaign_id"])
    require(read_json(packet/"grant.json")["campaign_id"]==campaign,"grant campaign differs")
    directory=Path(op["state"])/"authorizations"
    name=campaign+".json"
    with ops.private_directory(directory,create=True) as parent:
        try:
            with ops.open_private_at(parent,name) as fd:
                with os.fdopen(os.dup(fd),"rb") as stream:existing=stream.read(len(raw)+1)
            require(existing==raw,"operational grant already exists with different bytes")
        except FileNotFoundError:
            fd=os.open(name,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600,dir_fd=parent)
            with os.fdopen(fd,"wb") as stream:
                stream.write(raw);stream.flush();os.fsync(stream.fileno())
            os.fsync(parent)
    return directory/name


def bootstrap(packet,allocation=None):
    """Explicitly apply finite allowance, activate one window, stage, probe, clean."""
    from .storage import create_volume,mount,stage
    packet=Path(packet).resolve()
    deployment,op=merged_gate(packet,allocation)
    require((packet/"payload.json").exists(),"offline pinned payload missing")
    require(not (packet/"clock.json").exists(),"bootstrap already attempted; inspect retained state")
    Journal,allowance,binding,account_digest=imports(op["source"])
    from flowdc_pilot_provider import Provider
    import flowdc_ops as ops
    journal=Journal(op["state"]);j=journal.read()
    provider=Provider(ops.load_profile(j["profile_path"]))
    provider.verify_idle(j)
    write_new(packet/"bootstrap-rates.json",verify_rates(provider,j,deployment))
    now=time.time()
    write_new(packet/"clock.json",{"utc_epoch":now,"monotonic":time.monotonic(),
              "boot_id":Path("/proc/sys/kernel/random/boot_id").read_text().strip()})
    chosen=list(deployment["selected"].values())
    write_new(packet/"account-baseline.json",{k:j["vms"][k]["account"]["consumed"] for k in chosen})
    request=read_json(packet/"grant-template.json")
    request.update(expected_binding_sha256=binding(j),expected_accounts_sha256=account_digest(j),
                   cumulative_limits_seconds={k:max(allowance(j["vms"][k]["account"]).effective_limit+1,
                       math.ceil(j["vms"][k]["account"]["consumed"])+CAMPAIGN_SECONDS) for k in chosen},
                   expires_at=dt.datetime.fromtimestamp(now+CAMPAIGN_SECONDS,dt.UTC).isoformat())
    write_new(packet/"grant.json",request)
    grant_path=operational_grant(packet,op)
    service=j["service"]
    command=[service["interpreter"],"-E","-s","-B",str(Path(service["release"])/"flowdc_ops.py"),
             "pilot","campaign-apply","--state-root",op["state"],"--grant",str(grant_path)]
    with (packet/"grant-application.json").open("xb") as out,(packet/"grant-application.stderr").open("xb") as err:
        result=subprocess.run(command,stdout=out,stderr=err,timeout=240)
    require(result.returncode==0,"campaign grant refused; no activation")
    controller=controller_for(packet);controller.preflight(1800)
    write_new(packet/"window-00-intent.json",{"utc":utc(),"purpose":"deployment and probe"})
    outcome=None
    try:
        controller.call("start",seconds=30,window=1800);wait_active(controller)
        refresh_ssh(packet,controller)
        create_volume(packet,ops.load_profile(j["profile_path"]),op["worker_id"])
        mount(packet,new=True)
        installation=stage(packet)
        require(installation["source_archive_sha256"]==deployment["source_archive_sha256"],"installation source mismatch")
        outcome=guest(packet,controller,0)
        if outcome["status"] in ("complete","stopped"):
            write_new(packet/"terminal.json",outcome)
    finally:
        cleanup(packet,controller,packet/"window-00.json",outcome)
    return outcome


def collect(packet,seconds=900):
    """Retry only a metadata transfer; closed transfers/extractions are reusable."""
    import contextlib
    import os
    import selectors
    import shutil
    import signal
    import tarfile
    packet=Path(packet)
    require(1<=seconds<=900,'collection deadline outside finite bound')
    target=packet/"collected"
    if target.exists():return target
    closed=packet/"return.tar"
    if not closed.exists():
        require(shutil.disk_usage(packet).free>=56*2**30,"metadata transfer/extraction would consume local reserve")
        op=read_json(packet/"operator.json")
        script="""import pathlib,sys,tarfile
root=pathlib.Path(sys.argv[1])
paths=list(root.glob('*'))+list(root.glob('analysis/*'))+list(root.glob('runs/*/*'))
with tarfile.open(fileobj=sys.stdout.buffer,mode='w|') as archive:
 for p in sorted(paths):
  if p.is_file() and not p.is_symlink() and p.suffix in ('.json','.jsonl','.log','.tex','.pdf'):
   archive.add(p,arcname=str(p.relative_to(root)),recursive=False)
"""
        command=[op["guest_python"],"-c",script,op["guest_root"]+"/study"]
        partial=packet/"return.partial"
        if partial.exists():
            require(partial.is_file() and not partial.is_symlink(),"unsafe partial transfer")
            partial.unlink()  # Regenerable transfer bytes; guest originals stay intact.
        with partial.open("xb") as out:
            proc=subprocess.Popen(["ssh","-F",ssh_configuration(packet),"--","worker",shlex.join(command)],
                                  stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,start_new_session=True)
            count,deadline=0,time.monotonic()+seconds
            try:
                with selectors.DefaultSelector() as selector:
                    selector.register(proc.stdout,selectors.EVENT_READ)
                    while selector.get_map():
                        require(time.monotonic()<deadline,"collection deadline")
                        for key,_ in selector.select(.2):
                            chunk=os.read(key.fd,min(1024*1024,8*2**30+1-count))
                            if not chunk:
                                selector.unregister(key.fileobj);continue
                            count+=len(chunk)
                            require(count<=8*2**30,"collection exceeds cache bound")
                            out.write(chunk)
                    while os.waitid(os.P_PID,proc.pid,os.WEXITED|os.WNOHANG|os.WNOWAIT) is None:
                        require(time.monotonic()<deadline,"collection exit deadline")
                        time.sleep(.01)
            finally:
                with contextlib.suppress(ProcessLookupError):os.killpg(proc.pid,signal.SIGKILL)
                code=proc.wait(timeout=5);proc.stdout.close()
        require(code==0,"collection failed; guest evidence preserved")
        partial.replace(closed)
    temporary=packet/"collected.partial"
    if temporary.exists():
        require(temporary.is_dir() and not temporary.is_symlink(),"unsafe partial extraction")
        shutil.rmtree(temporary)
    temporary.mkdir()
    with tarfile.open(closed,mode="r:") as archive:
        require(all(m.isfile() and not Path(m.name).is_absolute() and ".." not in Path(m.name).parts for m in archive.getmembers()),"unsafe collection")
        archive.extractall(temporary,filter="data")
    temporary.replace(target)
    return target


def run_windows(packet,allocation=None):
    """Continue unattempted runs only; all VM mutations remain supervisor-owned."""
    from .common import lock
    from .storage import mount
    packet=Path(packet).resolve()
    # Local report/extraction recovery never starts a VM or needs a new grant.
    if (packet/"terminal.json").exists() and ((packet/"collected").exists() or (packet/"return.tar").exists()):
        from .analyze import report
        report(collect(packet))
        return read_json(packet/"terminal.json")
    deployment,op=merged_gate(packet,allocation)
    require((packet/"deployment-receipt.json").exists(),"bootstrap not completed")
    Journal,_,_,_=imports(op["source"]);journal=Journal(op["state"])
    clock,baseline=read_json(packet/"clock.json"),read_json(packet/"account-baseline.json")
    require(clock["boot_id"]==Path("/proc/sys/kernel/random/boot_id").read_text().strip(),"control host reboot")
    with lock(packet/"controller.lock"):
        for number in range(1,MAX_WINDOWS):
            path=packet/f"window-{number:02}.json"
            if path.exists():
                require(read_json(path)["cleaned"],"previous cleanup unresolved")
                continue
            require(not (packet/f"window-{number:02}-intent.json").exists(),"unfinished window intent requires diagnosis")
            left=min(clock["utc_epoch"]+CAMPAIGN_SECONDS-time.time(),clock["monotonic"]+CAMPAIGN_SECONDS-time.monotonic())
            require(left>=1800,"whole window cannot fit before original deadline")
            j=journal.read();chosen=list(deployment["selected"].values())
            require(all(j["vms"][k]["account"].get("campaign_id")==deployment["campaign_id"] for k in chosen),"campaign binding changed")
            exposure=sum((j["vms"][v["id"]]["account"]["consumed"]-baseline[v["id"]])*v["rate"]["su_per_hour"]/3600
                         for v in j["spec"]["vms"] if v["id"] in chosen)
            require(exposure+32<=OPERATIONAL_SU,"operational SU budget exhausted")
            from flowdc_pilot_provider import Provider
            import flowdc_ops as ops
            write_new(packet/f"window-{number:02}-rates-{time.time_ns()}.json",verify_rates(Provider(ops.load_profile(j["profile_path"])),j,deployment))
            controller=controller_for(packet);controller.preflight(1800)
            write_new(packet/f"window-{number:02}-intent.json",{"utc":utc(),"campaign_id":deployment["campaign_id"]})
            outcome=None
            try:
                controller.call("start",seconds=30,window=1800);wait_active(controller)
                refresh_ssh(packet,controller)
                mount(packet)
                outcome=(read_json(packet/"terminal.json") if (packet/"terminal.json").exists() else
                         guest(packet,controller,number,final=number>=MAX_WINDOWS-3 or left<5400 or exposure+96>OPERATIONAL_SU))
                if outcome["status"]=="analysis_pending" and (number>=MAX_WINDOWS-2 or left<3600 or exposure+64>OPERATIONAL_SU):
                    outcome={"status":"stopped","reason":"analysis_budget_exhausted","runs":outcome["runs"]}
                if outcome["status"] in ("complete","stopped"):
                    if not (packet/"terminal.json").exists():write_new(packet/"terminal.json",outcome)
                    from flowdc_experiment_transport import remaining as window_remaining
                    seconds=int(min(900,window_remaining(controller.call("status"))-120))
                    if seconds>=60:collect(packet,seconds=seconds)
            finally:
                cleanup(packet,controller,path,outcome)
            if outcome and outcome["status"] in ("complete","stopped") and (packet/"collected").exists():
                from .analyze import report
                report(packet/"collected")
                return outcome
            if outcome and outcome["status"]=="cooldown":
                seconds=max(0,outcome["until_utc"]-time.time())
                require(seconds<clock["utc_epoch"]+CAMPAIGN_SECONDS-time.time(),"cooldown cannot fit")
                # Wait on the local control host while all guests are offloaded.
                until=time.monotonic()+seconds
                while time.monotonic()<until:time.sleep(min(30,until-time.monotonic()))
        raise ValueError("finite window envelope exhausted; no automatic additional attempts")
