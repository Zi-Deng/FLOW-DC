"""V7 access-only snapshot with explicit verification of its exclusively owned store.

The frozen native reader still owns record/security validation. This handle is
valid only inside this context; it neither makes flock reentrant nor accepts a
caller-provided authentication assertion. Every verification re-reads the store.
"""

import contextlib
import copy
import json
import os
import tempfile
import time
from pathlib import Path

import claude_native_auth as auth
from workflow import WorkflowError

_KEY = object()


class OwnedSnapshot:
    def __init__(self, key, storage, registration, credentials, identity, env, timeout, started):
        if key is not _KEY:
            raise WorkflowError("Native snapshot must own its guarded store")
        self._storage = storage
        self._registration = registration
        self._receipt = storage.read("receipt.json")
        self._credentials = credentials
        self._identity = identity
        self.env = env
        self._timeout = timeout
        self._wall, self._monotonic = started
        self._active = True

    def recheck(self):
        if not self._active:
            raise WorkflowError("Native snapshot ownership has ended")
        delta = (time.time() - self._wall) - (time.monotonic() - self._monotonic)
        if not auth.finite(delta) or abs(delta) > 1:
            raise WorkflowError("Clock changed during native credential preparation")
        current, fresh, account = auth._load(self._storage, self._timeout)
        if self._storage.read("receipt.json") != self._receipt:
            raise WorkflowError("Paid-usage receipt changed during native preparation")
        if (current, fresh, account) != (self._registration, self._credentials, self._identity):
            raise WorkflowError("Native registration changed during preparation")
        for name, record in ((".credentials.json", fresh), (".claude.json", account)):
            with auth.store(Path(self.env["CLAUDE_CONFIG_DIR"]), create=True) as ephemeral:
                if ephemeral.read(name) != record:
                    raise WorkflowError("Native authentication snapshot changed before launch")

    def current_binding(self, timeout, window=None):
        self.recheck()
        if window is not None:
            from review_lifetime import _binding

            return _binding(self._storage, timeout, window, clock=time.time)
        registration, _, _ = auth._load(self._storage, timeout)
        return copy.deepcopy(registration["authentication"])

    def capability_lineage(self, observed, current, timeout):
        auth.validate_binding(observed)
        auth.validate_binding(current)
        self.current_binding(timeout)
        registration = self._registration
        live = registration["authentication"]

        def retained(binding):
            return binding == live or (
                binding in registration["lineage"]
                and binding["generation_id"] in registration["retained_capability_generations"]
            )

        # Imported publications can refer to the retained ancestor generation.
        # Both endpoints must have verified provenance in this *live* account.
        return retained(current) and retained(observed) and (current == live or current == observed)


def require(owned):
    if type(owned) is not OwnedSnapshot or not owned._active:
        raise WorkflowError("Expected an active owned native snapshot")
    return owned


@contextlib.contextmanager
def snapshot(policy, *, root=None):
    binding = auth.validate_binding(policy.get("authentication"))
    timeout = policy["budget"]["timeout_seconds"]
    with auth.store(root) as storage:
        registration, credentials, identity = auth._load(storage, timeout)
        if registration["authentication"] != binding:
            raise WorkflowError("Prepared credential generation changed; explicitly prepare a fresh packet")
        started = (time.time(), time.monotonic())
        with tempfile.TemporaryDirectory(prefix="agentic-native-auth-") as temporary:
            from review_claude import environment

            env = environment(Path(temporary))
            for name, record in ((".credentials.json", credentials), (".claude.json", identity)):
                fd = os.open(
                    Path(env["CLAUDE_CONFIG_DIR"]) / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
                )
                with os.fdopen(fd, "w") as stream:
                    json.dump(record, stream, allow_nan=False)
            owned = OwnedSnapshot(_KEY, storage, registration, credentials, identity, env, timeout, started)
            try:
                yield owned
            finally:
                owned._active = False
