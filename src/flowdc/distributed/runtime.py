"""Bind native execution to the disclosed source patch and actual build artifacts."""

import hashlib
from importlib.resources import files
from pathlib import Path

from ..integrity import parse
from .protocol import require


def verify_runtime(vine, receipt_path, worker_binary=None):
    pin = parse(files("flowdc.distributed").joinpath("cctools.json").read_bytes())
    receipt = parse(Path(receipt_path).read_bytes())
    require(vine.__version__ == pin["release"], "TaskVine version differs from the runtime pin")
    require(
        receipt["source_revision"] == pin["revision"] and receipt["patch_sha256"] == pin["patch_sha256"],
        "runtime build does not identify the disclosed source patch",
    )
    paths = {
        "lib/python3.12/site-packages/ndcctools/taskvine/_cvine.so": Path(vine.__file__).parent / "_cvine.so"
    }
    if worker_binary:
        paths["bin/vine_worker"] = Path(worker_binary)
    hashes = {}
    for name, path in paths.items():
        with path.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
        require(receipt["binaries"].get(name) == digest, f"native build artifact differs: {name}")
        hashes[name] = digest
    return {
        "version": vine.__version__,
        "source_revision": pin["revision"],
        "patch_sha256": pin["patch_sha256"],
        "binaries": hashes,
        "scope": "Local manager binding and configured worker; remote workers require separate qualification",
    }
