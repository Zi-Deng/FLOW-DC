#!/usr/bin/env python3
"""Optional offline check of the fixed offload child against an existing SDK.

Uses an explicit trusted system Python and disposable venv, with no installation.
Only the supplied SDK site-packages directory is added to that test environment.
Synthetic requests replace HTTP; socket connects are forbidden. Never loads a
production profile/OpenRC or executes the administration environment's interpreter.
Local interface enumeration is also synthetic; no host network facts are needed.
Normal unittest discovery does not require the optional SDK dependency.
"""

import argparse
import hashlib
import json
import os
import stat
import subprocess
import sys
import tempfile
import time
from pathlib import Path

PROJECT = "11111111-1111-4111-8111-111111111111"
VM = "22222222-2222-4222-8222-000000000001"

HOOK = r"""
import json
import socket
from pathlib import Path
from urllib.parse import urlsplit

def deny(*args, **kwargs):
    raise AssertionError("network forbidden in offline SDK fixture")

socket.socket.connect = deny
socket.socket.connect_ex = deny
socket.create_connection = deny
import psutil
# Connection checks local IPv6 availability during construction. Model an empty
# interface set instead of querying the host (which can require netlink access).
# This affects only test-local interface discovery, not SDK request dispatch.
psutil.net_if_addrs = lambda: {}
import requests

ROOT = Path(__file__).resolve().parents[3]
PROJECT = "11111111-1111-4111-8111-111111111111"
VM = "22222222-2222-4222-8222-000000000001"

def send(self, request, **kwargs):
    url = urlsplit(request.url)
    assert url.scheme == "https"
    assert url.netloc in ("cloud.example.test", "compute.example.test")
    path = url.path.rstrip("/")
    row = {"method": request.method, "path": path}
    if path.endswith("/action"):
        row["json"] = json.loads(request.body)
        row["microversion"] = request.headers.get("OpenStack-API-Version")
    with (ROOT / "requests.jsonl").open("a") as stream:
        stream.write(json.dumps(row) + "\n")
    response = requests.Response()
    response.request = request
    response.url = request.url
    response.status_code = 200
    response.headers["Content-Type"] = "application/json"
    if path == "/v3/auth/tokens" and request.method == "POST":
        assert url.netloc == "cloud.example.test"
        response.status_code = 201
        response.headers["X-Subject-Token"] = "synthetic-token"
        body = {"token": {"expires_at": "2080-01-01T00:00:00Z",
            "issued_at": "2026-01-01T00:00:00Z", "methods": ["application_credential"],
            "user": {"id": "fake-user", "name": "fake", "domain": {"id": "default"}},
            "project": {"id": PROJECT, "name": "fake", "domain": {"id": "default"}},
            "roles": [], "catalog": [{"id": "compute", "name": "nova", "type": "compute",
                "endpoints": [{"id": "public", "interface": "public",
                    "region": "test-region", "region_id": "test-region",
                    "url": "https://compute.example.test/v2.1"}]}]}}
    elif path in ("", "/v2.1") and request.method == "GET":
        version = {"id": "v2.1", "status": "CURRENT", "version": "2.99",
                   "min_version": "2.1", "updated": "2026-01-01T00:00:00Z",
                   "links": [{"rel": "self", "href": "https://compute.example.test/v2.1/"}]}
        body = {"version": version} if path else {"versions": [version]}
    elif path == "/v2.1/servers/" + VM and request.method == "GET":
        body = {"server": {"id": VM, "tenant_id": PROJECT, "status": "SHELVED", "name": "fake"}}
    elif path == "/v2.1/servers/" + VM + "/action" and request.method == "POST":
        response.status_code = json.loads((ROOT / "case.json").read_text())["status"]
        body = {} if response.status_code == 202 else {"error": {"message": "secret-canary"}}
    else:
        raise AssertionError("unexpected synthetic request")
    response._content = json.dumps(body).encode()
    return response

requests.Session.send = send

# Record only exception types/locations and missing fixed config keys from this
# entirely synthetic child, to distinguish constructor failure from HTTP refusal.
import re
import sys
def trace(frame, event, arg):
    if event == "exception" and frame.f_code.co_filename == "<string>":
        error = arg[1]
        row = {"function": frame.f_code.co_name, "line": frame.f_lineno,
               "type": type(error).__name__, "errno": getattr(error, "errno", None)}
        if isinstance(error, KeyError) and error.args and isinstance(error.args[0], str):
            if re.fullmatch("[a-z_]{1,64}", error.args[0]):
                row["missing_key"] = error.args[0]
        with (ROOT / "exceptions.jsonl").open("a") as stream:
            stream.write(json.dumps(row) + "\n")
    return trace
sys.settrace(trace)
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--sdk-site-packages", required=True, type=Path)
    parser.add_argument("--provider-source", type=Path, default=Path(__file__).resolve().parents[1] / "bin")
    parser.add_argument("--output", required=True, type=Path, help="New synthetic evidence JSON file.")
    args = parser.parse_args()
    library = args.sdk_site_packages.resolve(strict=True)
    if not library.is_dir() or "\n" in str(library) or "\r" in str(library):
        parser.error("SDK directory required")
    sys.path.insert(0, str(args.provider_source.resolve(strict=True)))
    import flowdc_ops as ops
    import flowdc_pilot_provider as provider

    interpreter = Path("/usr/bin/python3.12")
    with ops.private_directory(interpreter.parent, private=False) as parent:
        info = os.stat(interpreter.name, dir_fd=parent, follow_symlinks=False)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_mode & 0o022
            or info.st_uid not in (0, os.stat("/").st_uid)
        ):
            raise RuntimeError("trusted test interpreter required")
    os.umask(0o077)
    with tempfile.TemporaryDirectory(prefix="flowdc-real-sdk-test-") as directory:
        runtime = Path(directory) / "venv"
        subprocess.run(
            [str(interpreter), "-I", "-B", "-m", "venv", "--without-pip", str(runtime)],
            check=True,
            capture_output=True,
            timeout=15,
        )
        packages = runtime / "lib/python3.12/site-packages"
        # Use a unique hook, avoiding an unrelated system sitecustomize module.
        (packages / "installed_sdk.pth").write_text(str(library) + "\nimport flowdc_sdk_fixture\n")
        (packages / "flowdc_sdk_fixture.py").write_text(HOOK)
        rows = []
        for status, category in ((202, "ok"), (403, "permission"), (409, "conflict"), (503, "transient")):
            (runtime / "case.json").write_text(json.dumps({"status": status}))
            request_log = runtime / "requests.jsonl"
            request_log.unlink(missing_ok=True)
            exception_log = runtime / "exceptions.jsonl"
            exception_log.unlink(missing_ok=True)
            env = dict(
                ops.safe_environment(),
                OS_AUTH_URL="https://cloud.example.test/v3",
                OS_REGION_NAME="test-region",
                OS_AUTH_TYPE="v3applicationcredential",
                OS_APPLICATION_CREDENTIAL_ID="fake-id",
                OS_APPLICATION_CREDENTIAL_SECRET="synthetic-secret",
            )
            child = subprocess.run(
                [
                    str(runtime / "bin/python"),
                    "-I",
                    "-B",
                    "-c",
                    provider.OFFLOAD_PROGRAM,
                    "offload",
                    str(runtime),
                    PROJECT,
                    "test-region",
                    "https://cloud.example.test/v3",
                    VM,
                    str(time.monotonic() + 20),
                ],
                env=env,
                capture_output=True,
                timeout=25,
                text=True,
            )
            calls = (
                [json.loads(line) for line in request_log.read_text().splitlines()]
                if request_log.exists()
                else []
            )
            result = json.loads(child.stdout)
            mutations = [
                call for call in calls if call["method"] == "POST" and call["path"] != "/v3/auth/tokens"
            ]
            expected = [
                {
                    "method": "POST",
                    "path": "/v2.1/servers/" + VM + "/action",
                    "json": {"shelveOffload": None},
                    "microversion": "compute 2.1",
                }
            ]
            expected_code = (
                "offload_acknowledged"
                if status == 202
                else ("provider_permission_pending" if status == 403 else "provider_request_failed")
            )
            passed = (
                child.returncode == 0
                and not child.stderr
                and mutations == expected
                and result == {"code": expected_code, "category": category, "dispatch_possible": True}
                and "secret-canary" not in child.stdout
                and "synthetic-secret" not in child.stdout
            )
            rows.append(
                {
                    "simulated_status": status,
                    "passed": passed,
                    "child_exit": child.returncode,
                    "result": result,
                    "requests": calls,
                    "exceptions": [json.loads(line) for line in exception_log.read_text().splitlines()]
                    if exception_log.exists()
                    else [],
                }
            )
        evidence = {
            "fake_only": True,
            "local_interfaces": "synthetic_empty",
            "provider_sha256": hashlib.sha256(Path(provider.__file__).read_bytes()).hexdigest(),
            "rows": rows,
        }
        with args.output.open("x") as stream:
            json.dump(evidence, stream, indent=2)
            stream.write("\n")
        print(
            json.dumps(
                {
                    "fake_only": True,
                    "cases": [{"status": row["simulated_status"], "passed": row["passed"]} for row in rows],
                }
            )
        )
        return 0 if all(row["passed"] for row in rows) else 1


if __name__ == "__main__":
    raise SystemExit(main())
