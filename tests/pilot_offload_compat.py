#!/usr/bin/env python3
"""Offline reproduction for the installed OpenStackClient 10.3.0 offload no-op.

Run only with a separately established trusted administration venv's Python.
Do not execute an installed runtime that the path guard has refused. No installation, OpenRC,
profile, authentication, or network access is used. Not part of CI discovery.
"""

import argparse
import hashlib
import importlib.metadata
import io
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


def main():
    version = importlib.metadata.version("python-openstackclient")
    if version != "10.3.0":
        print(json.dumps({"checkpoint": "compatibility_version_required"}))
        return 3
    from openstackclient.compute.v2 import server

    results = []
    for initial in ("SHELVED", "ACTIVE"):
        state = SimpleNamespace(id="22222222-2222-4222-8222-000000000001", status=initial)
        actions = []

        def shelve(resource, actions=actions, state=state):
            actions.append("shelve")
            state.status = "SHELVED"

        compute = SimpleNamespace(
            find_server=lambda *a, state=state, **kw: state,
            get_server=lambda *a, state=state, **kw: state,
            shelve_server=shelve,
            shelve_offload_server=lambda resource, actions=actions: actions.append("offload"),
        )
        app = SimpleNamespace(client_manager=SimpleNamespace(compute=compute), stdout=io.StringIO())
        command = server.ShelveServer(app, None)
        with (
            patch("socket.socket", side_effect=AssertionError("network forbidden")),
            patch("socket.create_connection", side_effect=AssertionError("network forbidden")),
        ):
            command.take_action(argparse.Namespace(servers=[state.id], offload=True, wait=False))
        results.append({"initial_state": initial, "actions": actions})
    assert results == [
        {"initial_state": "SHELVED", "actions": []},
        {"initial_state": "ACTIVE", "actions": ["shelve", "offload"]},
    ]
    print(
        json.dumps(
            {
                "version": version,
                "source_sha256": hashlib.sha256(Path(server.__file__).read_bytes()).hexdigest(),
                "fake_only": True,
                "results": results,
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
