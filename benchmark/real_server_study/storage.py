"""Campaign-owned volume and offline payload staging; never modify existing data."""
import json
import subprocess
import sys
import tarfile
import time
from pathlib import Path
from uuid import UUID

from .common import file_hash, read_json, require, utc, write_new

MOUNT = r'''
import json,os,pathlib,pwd,subprocess,sys
volume,root,expected=sys.argv[1:]
assert root.startswith('/mnt/flowdc-external-') and root.rsplit('-',5)[0]=='/mnt/flowdc-external'
prefix=volume[:20]
disks=[]
for block in pathlib.Path('/sys/class/block').iterdir():
 for p in (block/'serial',block/'device/serial'):
  if p.exists() and p.read_text().strip()==prefix:disks.append('/dev/'+block.name)
disks=list(set(disks));assert len(disks)==1,'volume serial not uniquely found'
device=disks[0]
assert int(subprocess.check_output(['blockdev','--getsize64',device]))>=256*2**30
value=subprocess.run(['blkid','-o','value','-s','UUID',device],capture_output=True,text=True)
if expected=='NEW':
 assert value.returncode==2 and not value.stdout.strip(),'new volume unexpectedly contains a filesystem'
 subprocess.run(['mkfs.ext4','-q',device],check=True,timeout=120)
 value=subprocess.run(['blkid','-o','value','-s','UUID',device],capture_output=True,text=True,check=True)
else:assert value.returncode==0 and value.stdout.strip()==expected,'filesystem UUID changed'
target=pathlib.Path(root);assert not target.is_symlink()
target.mkdir(mode=0o700,exist_ok=True)
mounted=subprocess.run(['findmnt','-n','-o','UUID','--mountpoint',root],capture_output=True,text=True)
if mounted.returncode==0:assert mounted.stdout.strip()==value.stdout.strip()
else:
 assert not any(target.iterdir()),'refusing to shadow existing files'
 subprocess.run(['mount',device,root],check=True,timeout=30)
uid=pwd.getpwnam('exouser').pw_uid;os.chown(root,uid,uid);os.chmod(root,0o700)
print(json.dumps({'volume_id':volume,'filesystem_uuid':value.stdout.strip(),'root':root}))
'''


def volume_call(profile, action, args):
    import flowdc_ops as ops
    require(action in ("show","create","attach"),"unsupported volume action")
    if action=="create":
        require(len(args)==1 and str(UUID(args[0]))==args[0],"invalid campaign identity")
    else:
        require(all(str(UUID(a))==a for a in args),"invalid volume/VM UUID")
    prefix=r'''set +x +v
set -eu -o pipefail
readonly flowdc_credential=$1 flowdc_client=$2 flowdc_action=$3
shift 3
case "$flowdc_action" in
 show) command=(volume show "$1" -f json);;
 create) command=(volume create --size 256 --property "flowdc_campaign=$1" "flowdc-external-$1" -f json);;
 attach) command=(server add volume "$1" "$2" -f json);;
 *) exit 64;;
esac
readonly -a command
'''
    script=prefix+ops.WRAPPER.split("readonly -a command\n",1)[1]
    ops.validate_client(profile["openstack_client"])
    with ops.private_file(profile["credential_file"]) as fd:
        code,raw=ops.run_bounded(["/bin/bash","-p","-c",script,"flowdc-volume",
             f"/proc/self/fd/{fd}",profile["openstack_client"],action,*args],timeout=60,pass_fds=(fd,),classify_errors=True)
    require(code==0,"volume operation failed; preserve intent, no automatic mutation replay")
    return ops.parse_json(raw) if raw.strip() else {}


def create_volume(packet, profile, worker_id):
    deployment=read_json(packet/"deployment.json")
    if (packet/"volume.json").exists():
        receipt=read_json(packet/"volume.json")
    else:
        require(not (packet/"volume-create-intent.json").exists(),"volume creation uncertain; reconcile its retained intent first")
        write_new(packet/"volume-create-intent.json",{"utc":utc(),"campaign_id":deployment["campaign_id"]})
        result=volume_call(profile,"create",[deployment["campaign_id"]])
        key=result.get("id") or result.get("ID")
        receipt={"volume_id":str(UUID(key)),"campaign_id":deployment["campaign_id"],"created_for_campaign":True}
        write_new(packet/"volume.json",receipt)
    volume=volume_call(profile,"show",[receipt["volume_id"]])
    properties=volume.get("properties",{})
    require(isinstance(properties,dict) and properties.get("flowdc_campaign")==deployment["campaign_id"],"volume ownership metadata mismatch")
    require(int(volume["size"])==256,"volume size mismatch")
    attachments=volume.get("attachments") or []
    if not attachments:
        deadline=time.monotonic()+120
        while volume.get("status")!="available":
            require(time.monotonic()<deadline and volume.get("status")!="error","volume not ready for attachment")
            time.sleep(1);volume=volume_call(profile,"show",[receipt["volume_id"]])
        require(not (packet/"volume-attach-intent.json").exists(),"volume attachment uncertain; reconcile before continuing")
        write_new(packet/"volume-attach-intent.json",{"utc":utc(),"volume":receipt["volume_id"],"worker":worker_id})
        volume_call(profile,"attach",[worker_id,receipt["volume_id"]])
    deadline=time.monotonic()+120
    while True:
        volume=volume_call(profile,"show",[receipt["volume_id"]])
        attachments=volume.get("attachments") or []
        if volume.get("status")=="in-use" and len(attachments)==1 and attachments[0].get("server_id")==worker_id:
            return receipt
        require(time.monotonic()<deadline,"volume attachment not observed complete")
        time.sleep(1)


def mount(packet, new=False):
    from .cloud import ssh
    op=read_json(packet/"operator.json")
    volume=read_json(packet/"volume.json")
    deployment=read_json(packet/"deployment.json")
    require(volume["campaign_id"]==deployment["campaign_id"] and op["guest_root"]=="/mnt/flowdc-external-"+deployment["campaign_id"],"volume mount binding changed")
    expected="NEW" if new else read_json(packet/"volume-mount.json")["filesystem_uuid"]
    result=json.loads(ssh(packet,["sudo","-n","/usr/bin/python3","-c",MOUNT,volume["volume_id"],op["guest_root"],expected],timeout=180))
    if new:
        write_new(packet/"volume-mount.json",result)
    return result


def prepare_payload(packet, study):
    """Download pinned binary wheels locally; no guest/volume mutation."""
    packet,study=Path(packet).resolve(),Path(study).resolve()
    op=read_json(packet/"operator.json")
    requirements=Path(op["source"])/"benchmark/real_server_study/requirements.txt"
    wheels=packet/"wheels";wheels.mkdir()
    subprocess.run([sys.executable,"-m","pip","--isolated","download","--only-binary=:all:",
                    "--platform","manylinux_2_28_x86_64","--platform","manylinux2014_x86_64",
                    "--python-version","312","--implementation","cp","--abi","cp312",
                    "--index-url","https://pypi.org/simple","-r",str(requirements),"-d",str(wheels)],check=True,timeout=300)
    with tarfile.open(packet/"inputs.tar","x") as archive:
        for path in sorted(study.rglob("*")):
            require(not path.is_symlink(),"symlink in prepared inputs")
            if path.is_file():archive.add(path,arcname="study/"+str(path.relative_to(study)),recursive=False)
        for path in wheels.iterdir():archive.add(path,arcname="wheels/"+path.name,recursive=False)
        archive.add(requirements,arcname="requirements.txt")
    write_new(packet/"payload.json",{"inputs_sha256":file_hash(packet/"inputs.tar"),
               "wheels":{p.name:file_hash(p) for p in wheels.iterdir()}})


STAGE = r'''
import hashlib,json,pathlib,subprocess,sys,tarfile
root=pathlib.Path(sys.argv[1]);source_sha,input_sha,head,base_python=sys.argv[2:]
for name,sha in [('source.tar',source_sha),('inputs.tar',input_sha)]:
 h=hashlib.sha256()
 with (root/name).open('rb') as stream:
  for chunk in iter(lambda:stream.read(1024*1024),b''):h.update(chunk)
 assert h.hexdigest()==sha,'transfer digest mismatch'
source=root/'source';assert not source.exists(),'source deployment already attempted'
source.mkdir()
with tarfile.open(root/'source.tar') as archive:archive.extractall(source,filter='data')
(source/'.flowdc-source-head').write_text(head+'\n')
with tarfile.open(root/'inputs.tar') as archive:archive.extractall(root,filter='data')
assert subprocess.check_output([base_python,'-c','import sys;print(sys.version_info[:2]==(3,12))'],text=True).strip()=='True'
subprocess.run([base_python,'-m','venv',str(root/'venv')],check=True,timeout=60)
python=str(root/'venv/bin/python')
subprocess.run([python,'-m','pip','--isolated','install','--no-index','--find-links',str(root/'wheels'),'-r',str(root/'requirements.txt')],check=True,stdout=subprocess.DEVNULL,timeout=180)
versions=subprocess.check_output([python,'-m','pip','freeze'],text=True,timeout=20)
print(json.dumps({'python':python,'versions':versions,'source_archive_sha256':source_sha,'inputs_sha256':input_sha}))
'''


def stage(packet):
    from .cloud import ssh,ssh_configuration
    packet=Path(packet)
    op,deployment,payload=read_json(packet/"operator.json"),read_json(packet/"deployment.json"),read_json(packet/"payload.json")
    # Only fixed filenames below, inside the exact campaign-owned mount.
    for name in ("source.tar","inputs.tar"):
        command=["/usr/bin/python3","-c",
                 "import pathlib,shutil,sys; p=pathlib.Path(sys.argv[1]); f=p.open('xb'); shutil.copyfileobj(sys.stdin.buffer,f); f.close()",
                 op["guest_root"]+"/"+name]
        # stdin is streamed from disk rather than buffering the payload in controller memory.
        with (packet/name).open("rb") as data:
            result=subprocess.run(["ssh","-F",ssh_configuration(packet),"--","worker",__import__('shlex').join(command)],
                                  stdin=data,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=180)
        require(result.returncode==0,"payload transfer failed; retain partial destination")
    result=json.loads(ssh(packet,[op["guest_python"],"-c",STAGE,op["guest_root"],deployment["source_archive_sha256"],
                    payload["inputs_sha256"],deployment["source"]["head"],op["guest_python"]],timeout=300))
    result["campaign_id"]=deployment["campaign_id"]
    write_new(packet/"deployment-receipt.json",result)
    return result
