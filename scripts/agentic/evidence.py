#!/usr/bin/env python3
"""Run an explicitly supplied validation command and bind its outputs to provenance."""

import argparse
import datetime as dt
import hashlib
import json
import subprocess
import sys
from pathlib import Path

from workflow import Repo, WorkflowError, positive, write_json


def file_record(path):
    path = Path(path)
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return {"path": str(path), "sha256": h.hexdigest(), "bytes": path.stat().st_size}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--issue", required=True)
    parser.add_argument("--pr", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--inputs", required=True)
    parser.add_argument("--environment-lock", required=True)
    parser.add_argument("--seed", type=int, action="append", default=[])
    parser.add_argument("--artifact", action="append", default=[])
    parser.add_argument("--output", required=True)
    parser.add_argument("--tier", choices=["V1", "V2", "V3", "V4"], required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    try:
        repo = Repo()
        command = args.command[1:] if args.command[:1] == ["--"] else args.command
        if not command:
            raise WorkflowError("Supply the exact command after --")
        if repo.git("status", "--porcelain"):
            raise WorkflowError("Evidence requires a clean committed starting state")
        output = Path(args.output).resolve()
        if output.exists():
            raise WorkflowError("Evidence manifest already exists; choose a new run path")
        before = repo.git("rev-parse", "HEAD")
        metadata = {
            "schema_version": 1,
            "git_commit": before,
            "dirty": False,
            "issue": positive(args.issue),
            "pull_request": positive(args.pr),
            "tier": args.tier,
            "config": file_record(args.config),
            "input_manifest": file_record(args.inputs),
            "environment_lock": file_record(args.environment_lock),
            "seeds": args.seed,
            "command_argv": command,
            "started_at_utc": dt.datetime.now(dt.UTC).isoformat(),
        }
        # No shell. This operation is intentionally human invoked and may spend compute.
        result = subprocess.run(command, cwd=repo.root, check=False)
        metadata.update(
            {
                "exit_code": result.returncode,
                "finished_at_utc": dt.datetime.now(dt.UTC).isoformat(),
                "head_after": repo.git("rev-parse", "HEAD"),
                "dirty_after": bool(repo.git("status", "--porcelain")),
                "artifacts": [],
            }
        )
        missing = []
        for item in args.artifact:
            if not Path(item).is_file():
                missing.append(item)
            else:
                metadata["artifacts"].append(file_record(item))
        metadata["missing_artifacts"] = missing
        metadata["valid_run"] = (
            result.returncode == 0
            and not missing
            and not metadata["dirty_after"]
            and before == metadata["head_after"]
        )
        output.parent.mkdir(parents=True, exist_ok=True)
        write_json(output, metadata)
        print(json.dumps({"manifest": str(output), "valid_run": metadata["valid_run"]}))
        return 0 if metadata["valid_run"] else 1
    except (WorkflowError, OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
