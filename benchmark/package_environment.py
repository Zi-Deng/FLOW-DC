#!/usr/bin/env python3
"""Package the active task-private pinned environment without modifying its files."""

import argparse
import hashlib
import json
import os
import sys
import time
from pathlib import Path

import conda_pack
import ndcctools.taskvine as vine
from conda_pack.core import File
from ndcctools.poncho.package_create import _copy_run_in_env

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument(
    "--output", required=True, type=Path, help="New directory for archive, inventory and receipt"
)
args = parser.parse_args()
root = Path(__file__).resolve().parents[1]
prefix = Path(sys.prefix).resolve()
if not prefix.is_relative_to(root / ".agentic-local"):
    parser.error("run with the task-private environment Python under .agentic-local")
if vine.cvine.vine_version_string() != "7.17.2":
    parser.error("matching official TaskVine 7.17.2 required")
out = args.output.absolute()
out.mkdir(mode=0o700, parents=True, exist_ok=False)
started = time.monotonic_ns()
env = conda_pack.CondaEnv.from_prefix(str(prefix), ignore_missing_files=False, ignore_editable_packages=False)


def inventory():
    result = {}
    for f in env.files:
        p = Path(f.source)
        result[f.target] = (
            {"symlink": os.readlink(p)}
            if p.is_symlink()
            else {"sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
        )
    return result


before = inventory()
(out / "source-inventory.json").write_text(json.dumps(before, sort_keys=True) + "\n")
(out / "overlay/env/bin").mkdir(parents=True)
os.environ["PATH"] = str(prefix / "bin") + os.pathsep + os.environ["PATH"]
_copy_run_in_env(str(out / "overlay"))  # Upstream writes only this newly owned overlay.
wrappers = []
for name in ("run_in_env", "poncho_package_run"):
    p = out / "overlay/env/bin" / name
    wrappers.append(File(str(p), "bin/" + name, is_conda=False))
# Replace only the packaged launcher record; source files remain unchanged.
targets = {f.target for f in wrappers}
portable = conda_pack.CondaEnv(str(prefix), [f for f in env.files if f.target not in targets] + wrappers)
archive = out / "research-env.tar.gz"
portable.pack(output=str(archive), force=False, n_threads=2, compress_level=1)
after = inventory()
if before != after:
    raise RuntimeError("source environment changed during packing")
result = {
    "schema": "flowdc-portable-environment-preflight-v1",
    "files": len(portable.files),
    "source_inventory_sha256": hashlib.sha256((out / "source-inventory.json").read_bytes()).hexdigest(),
    "archive_sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
    "archive_bytes": archive.stat().st_size,
    "source_unchanged": True,
    "elapsed_ns": time.monotonic_ns() - started,
    "versions": {"conda_pack": conda_pack.__version__, "taskvine": vine.cvine.vine_version_string()},
    "conda_metadata_sha256": {
        p.name: hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted((prefix / "conda-meta").glob("*.json"))
    },
    "claim": "strict environment packaging only; relocation and native TaskVine integration still required",
}
(out / "result.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps(result))
