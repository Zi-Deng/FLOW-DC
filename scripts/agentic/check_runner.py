"""Bounded process-isolated unittest execution with complete occurrence accounting."""

import collections
import hashlib
import json
import math
import os
import selectors
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

SECONDS = 840
TEXT_BYTES = 32 * 1024 * 1024
EVIDENCE_BYTES = 16 * 1024 * 1024
VERSION = 2

# Fixed historical scheduling estimates, never evidence of current execution.
_SCHEDULING_SEED = {
    "schema_version": 1,
    "algorithm": "fixture-group-lpt-ms-v1",
    "source_head": "44eeac999fc5b6f3fa0eeb6120c4f59db81a71bd",
    "source_map_sha256": "ea02975390c37852a874fc70de73debb4d426fa8dc0f952dd1911de7d17703ec",
    "provenance": {
        "request.json": "938cd3a9009bfdc1d99232dec4559e42c684904d7394354316f25d925d6fd045",
        "summary.json": "5845ff152e852782d982ce3e3c7729aeaa2a5f3d2dca8fc481c721fbf0ff5b81",
        "worker-0.jsonl": "18688336e730da2d4585112bf29856249c5f0c3250532a4dfbd574d1d06ba6f1",
        "worker-1.jsonl": "f83948aef0e803fae7dad9652cb075085789be3a1cb961c147a488ba9e3be490",
    },
    "entries": [
        {
            "identity_sha256": "b5f10901c0b8f10688ebac217dbc8dd50f987f675dade342215262966e379276",
            "weight_ms": 146,
        },
        {
            "identity_sha256": "6b1de550b24b2197f480d436f314e8644bf6bab3ec8312cb30f16f4fff591a85",
            "weight_ms": 4088,
        },
        {
            "identity_sha256": "510eda884fc2ecfbd8771f71a21ed0016c58cc882dee9688d16699ab21ffe867",
            "weight_ms": 25,
        },
        {
            "identity_sha256": "d32d00ced78adc78c33585092dd199bbd31adf21ec64c7b852f8ef7b26baa6e1",
            "weight_ms": 506,
        },
        {
            "identity_sha256": "7f373ab516bd0b9df4a15df22a1dffa14710d1d5ad816dd8eff6c482659297d0",
            "weight_ms": 1855,
        },
        {
            "identity_sha256": "6e7b62e314969ed4f7a4beb17538590aff85c11c90ef45e4c11b7b686efc92b2",
            "weight_ms": 150,
        },
        {
            "identity_sha256": "dfb38acf62d4bee41d441826732dc94bd2e7b377993e069917435c835428cab3",
            "weight_ms": 21,
        },
        {
            "identity_sha256": "27cfd4ebb271d8f09c098435f6a07f54730484d8eb2fcb0ba9eaa93318ba56f4",
            "weight_ms": 52,
        },
        {
            "identity_sha256": "8d9df67f27048beaa271c216326d8af45353896fe03dc019107f55065ef5ba75",
            "weight_ms": 4598,
        },
        {
            "identity_sha256": "07aca7f1731e9145f6d567355fdd1968c0e7ef76baf220d63ea03d3e0e704280",
            "weight_ms": 1482,
        },
        {
            "identity_sha256": "d92a1f7695f6795b3bc8b6b006d93087ef37d67d87414e710dcf0e4d54f0a231",
            "weight_ms": 2554,
        },
        {
            "identity_sha256": "ff592e51b6dc0370501be222022c87af9f0dd335b53a6dea4fc1b394d506541c",
            "weight_ms": 4334,
        },
        {
            "identity_sha256": "1ad79eb64d467fdb0dbe9a86747d9faaa29e2eeb6ebc6ca5965ed7dead08ca8d",
            "weight_ms": 60,
        },
        {
            "identity_sha256": "586303e8fd872b150a1d56a71754eb1c82ccc27f7d8d6b136585ab17016251ae",
            "weight_ms": 3128,
        },
        {
            "identity_sha256": "ce54e953528ea11947ae86ce4dfa49f84de3c64c412c598a86c07368abe571a4",
            "weight_ms": 1505,
        },
        {
            "identity_sha256": "94d98e01ec29293e0520af9d0484cbc2162240f6c31daea0c6efb90aa7f63682",
            "weight_ms": 14,
        },
        {
            "identity_sha256": "9fd00dee81bb0808de46dc5730a8273a2ff6849337af0bbdb01d66789983c82b",
            "weight_ms": 578,
        },
        {
            "identity_sha256": "969351a0fa3a736d822ffc6ad1981cf953a05676c2d38398f73b58a403f65a82",
            "weight_ms": 2637,
        },
        {
            "identity_sha256": "d09dd43f198c2a2559495bc8e5a97a975653f63f75435677e9cc4f84900cb181",
            "weight_ms": 3489,
        },
        {
            "identity_sha256": "65b4cee735a5cd453bd1f18e562234b435eeeeef85e2a000eef9f3d021bd87d4",
            "weight_ms": 6982,
        },
        {
            "identity_sha256": "faff5a98f41b5827bcde1ba77bd6933c619267d8689ba5dd26bb82695d072ee8",
            "weight_ms": 12647,
        },
        {
            "identity_sha256": "cb6cf50c508d0693efe81bf48b2e1efd0794de9a73d3497b427f69d7ce23de0c",
            "weight_ms": 23686,
        },
        {
            "identity_sha256": "14712a9775b3866f707fb07976d8993988cf2bc7f99fc48b71c2754151281d80",
            "weight_ms": 16006,
        },
        {
            "identity_sha256": "087a3f028e9f4c8660808e97853ae1c60bf248c046f8bcf95a4422556c9ea3b2",
            "weight_ms": 7,
        },
        {
            "identity_sha256": "44a4e6d55c1414648e43e200d4b073b6084c7f2423f65f6fca77d5900f22a6fd",
            "weight_ms": 379,
        },
        {
            "identity_sha256": "a2a46b436a265e25902ed37f5addfaadbc2f7adfe4b66b173ecaafd2981063b9",
            "weight_ms": 11,
        },
        {
            "identity_sha256": "c75b954f006a9a939809e69f263bbc8309072a91f063062c8cf4792310ab8191",
            "weight_ms": 2354,
        },
        {
            "identity_sha256": "9114dfbc5f2d83a36025c2e7f7088f1597f8dc8d9ec8b8ecf8ab184f01350723",
            "weight_ms": 3667,
        },
        {
            "identity_sha256": "80c001cb91377edb6d1df75c024b810b9dcb3b394fd354668cd9abe2dd368554",
            "weight_ms": 6182,
        },
        {
            "identity_sha256": "65e2e5400444b3dca4ff77efb9d8a45d9085a1e3f0ee31432647b04c9821b8ba",
            "weight_ms": 3,
        },
        {
            "identity_sha256": "77186bcc6f0ee85dc08a4ea7980b62f9d5515b69c82b9b68c1a3cc1233784480",
            "weight_ms": 2872,
        },
        {
            "identity_sha256": "d8ef4226d09635638f8bb0eb6fcf4a0980222070df174561d86feb979e9e83ca",
            "weight_ms": 4629,
        },
        {
            "identity_sha256": "b4b20641b1a4f58ac7d4350c9d5c6c14a93fdd00ccc49065705938da15cccb8d",
            "weight_ms": 875,
        },
        {
            "identity_sha256": "bb8e5737ac398192b085592a5dad23c4a2b2e63b96902191058f497757c4a338",
            "weight_ms": 3197,
        },
        {
            "identity_sha256": "907adbac1bb8a671e8474694d469edbddb15bd16446798fc686afb9c95b2fd6f",
            "weight_ms": 4,
        },
        {
            "identity_sha256": "2e0e0c4791a9b0171d1513119d178469031ce85bbcc371f279dbdca413daeecc",
            "weight_ms": 7152,
        },
        {
            "identity_sha256": "db9a6036465fc93745ca71a8f594633f53c723fbdac7e33e86f6d747f8d6f8ce",
            "weight_ms": 20819,
        },
        {
            "identity_sha256": "375a73a5451849a932662ce5d9d3c841874bd6727bdaac23ab6c1799e32f98ec",
            "weight_ms": 2884,
        },
        {
            "identity_sha256": "b98f04612dcb36635d392957709f4e4bafa5223dfbdf77a1213576cca4e1df77",
            "weight_ms": 14021,
        },
        {
            "identity_sha256": "4deafa2945d494286a2d945217778d65d87f0ca625c2aaa212b9a3f2e5c25a96",
            "weight_ms": 13123,
        },
        {
            "identity_sha256": "cef01b0e28af9f342db156a3770216d3ead342981518cced868f3c31e344d1c5",
            "weight_ms": 5604,
        },
        {
            "identity_sha256": "004d379ff616658488416f4995e1474f5b22e2ad22ff212cdc7faf85ad1add42",
            "weight_ms": 8192,
        },
        {
            "identity_sha256": "fc7aeab49ffd160397ea409815c5193cf578e98fb176d52e5ac6e6a8d0a7c8ff",
            "weight_ms": 11853,
        },
        {
            "identity_sha256": "43a2d2f56f3f07c36bb16dce933fd037ad4a3e87a0700c33e88f17441efb06d4",
            "weight_ms": 17281,
        },
        {
            "identity_sha256": "628646d90eb12d079096d318515a81bc5338f69c2de240a3acec5aa4c213c46c",
            "weight_ms": 13428,
        },
        {
            "identity_sha256": "0d638ad96e3ce7399597a85eba53df39c50dae10944bd647569641dcef7a877a",
            "weight_ms": 20597,
        },
        {
            "identity_sha256": "4ab47436a466b0caba5ccc7a4aa9f56c1b7bb1007a664f46bbb3701b18f6da30",
            "weight_ms": 37896,
        },
        {
            "identity_sha256": "dbec2c631a5bf7c8606afaf56fd9ae6f7b073778c8bc637fd41faaab02fbfe25",
            "weight_ms": 2728,
        },
        {
            "identity_sha256": "b10ecd70ae17c575b6f06e1f24c4bbfc9400114237356d21e74e354172129c3e",
            "weight_ms": 9459,
        },
        {
            "identity_sha256": "ba0eb7df125fdf50aafc95f918218ff04515a8d755ebb6c79a51ea0df17a641c",
            "weight_ms": 73056,
        },
        {
            "identity_sha256": "11276419f95d53ccaf89d57309a33522fa83ecaf451e64b1a1aa3fe9f69688b8",
            "weight_ms": 14909,
        },
        {
            "identity_sha256": "5116bd36bf946c149a8814e36089f8a888456d41994f3a67fafc62bf01517ec3",
            "weight_ms": 68231,
        },
        {
            "identity_sha256": "1c86a1dfb44d214d0e7aae11315af4fa9f0763fbfae209ef95b330197dbe33e8",
            "weight_ms": 142,
        },
        {
            "identity_sha256": "2d81e0ba3ffe5ee2637e5bb76023b421fe6e09c01d8149494ff95e6b543db0bc",
            "weight_ms": 57572,
        },
        {
            "identity_sha256": "489999bc94b26f88f9468bfbe8966993f4820a91e8d63fec06804147bf02d6c4",
            "weight_ms": 4971,
        },
        {
            "identity_sha256": "7f572fd8c4e6acc741128dc47ac7b1be8a0b1d4a1db546050567ee8a8ebec688",
            "weight_ms": 853,
        },
        {
            "identity_sha256": "40c8b3acb33d83afa9a78e29d0bf120fc30a8612756fcf684d8eaa89b437cd09",
            "weight_ms": 1961,
        },
        {
            "identity_sha256": "39e770ca29b38b4e9fada37a0b4c7310c8627429f25ce227e19f01415fa33fc4",
            "weight_ms": 524,
        },
        {
            "identity_sha256": "f14c42eb9bfc040e9fa739c222204e8c2df67dade83cb2573daffec3742ea69f",
            "weight_ms": 1889,
        },
        {
            "identity_sha256": "e0c6298ef83f4bd62eb0b44769ca9be5dd00a01dba75c471ee48427eaa1e3d11",
            "weight_ms": 1474,
        },
        {
            "identity_sha256": "8dd45d4e2b105b0cd61b5af043e62a7716c95f337be2bdc209ecd4965065503a",
            "weight_ms": 2471,
        },
        {
            "identity_sha256": "b782c2fa7ef16821e1a9ab4713c4e80f8c38a69303236357fcc332b50088aa50",
            "weight_ms": 2221,
        },
        {
            "identity_sha256": "c5a0782f09b64a1a822036e99838056ad78439fccafe5ac11aad26c4ace7e8c6",
            "weight_ms": 20,
        },
        {
            "identity_sha256": "a2d557fafb2c1efc1df7f63b0bee0689c4679fde42343eaf6da4ae1a8b1e3a9b",
            "weight_ms": 2746,
        },
        {
            "identity_sha256": "4e747b9ecd225343fecba59b1d818656d0334e2bc392b704260542a28a3c4a05",
            "weight_ms": 7,
        },
        {
            "identity_sha256": "23c23e3d7142feb6da5d14c532444bd47e5e533e3da2ff872f3d79e4269d155c",
            "weight_ms": 318,
        },
        {
            "identity_sha256": "58645eb34e36065f73b5f56110e8ae79eb3fc4333246f56de3d02a94a97342be",
            "weight_ms": 286,
        },
        {
            "identity_sha256": "191bdd45cde7a7f8d4b5a690359f9bfa1e1a04e54e92206f3d3b354144ac4397",
            "weight_ms": 2155,
        },
        {
            "identity_sha256": "d75eb86e25d8d8e460e4153c778708e2fa196808a754b81a1c816ac53471ea5d",
            "weight_ms": 934,
        },
        {
            "identity_sha256": "d16eb382dd269d6ab958bea8c52b3435659a3936c0cadde0d86d87796d53b9b7",
            "weight_ms": 3380,
        },
        {
            "identity_sha256": "0d7776bd43456b8479cc8f20effe20d5d56a7b18bb60411f85bb66a3cc6ea85f",
            "weight_ms": 402,
        },
    ],
}
_SEED_DIGEST = "1bc1a7df04b831d4104c82432124785875936b76762dcdf27d350380f2da4338"


class RunnerError(Exception):
    """An incomplete or inconsistent gate, never a successful fallback."""


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def strict(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise RunnerError("Duplicate protocol key")
            result[key] = value
        return result

    return json.loads(raw, object_pairs_hook=pairs, parse_constant=lambda _: _invalid())


def _invalid():
    raise RunnerError("Invalid protocol constant")


def source(root):
    files = {}
    for name in (
        "scripts/agentic",
        "tests/agentic",
        ".agentic",
        ".agents/skills",
        ".github",
        "docs/agent-workflow",
    ):
        directory = root / name
        for path in sorted(directory.rglob("*")):
            if any(p in {"__pycache__", ".ruff_cache"} for p in path.parts):
                continue
            if path.is_symlink():
                raise RunnerError("Source symlink")
            if path.is_file():
                files[str(path.relative_to(root))] = hashlib.sha256(path.read_bytes()).hexdigest()
    for name in ("AGENTS.md", "Makefile", "scripts/check_repository.py"):
        path = root / name
        if path.exists():
            files[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    # An installed disposable tree need not be a Git repository.
    checkout = None
    if (root / ".git").exists():
        checkout = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=root, timeout=10, text=True
        ).strip()
        tracked = subprocess.check_output(["git", "ls-files", "-z"], cwd=root, timeout=10)
        for raw in tracked.split(b"\0"):
            if not raw:
                continue
            name = os.fsdecode(raw)
            path = root / name
            if path.is_symlink() or not path.is_file():
                raise RunnerError("Missing or symlinked tracked source")
            files[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    return {"checkout": checkout, "files": files}


def discover(root):
    sys.path.insert(0, str(root / "scripts/agentic"))
    loader = unittest.TestLoader()
    suite = loader.discover(str(root / "tests/agentic"))
    rows, objects = [], []

    def visit(node, position):
        if type(node) is unittest.TestSuite:
            for index, child in enumerate(node):
                visit(child, position + [index])
        elif (
            isinstance(node, unittest.TestCase)
            and type(node).run is unittest.TestCase.run
            and type(node).__call__ is unittest.TestCase.__call__
        ):
            rows.append(
                {
                    "position": position,
                    "id": node.id(),
                    "module": type(node).__module__,
                    "class": type(node).__module__ + "." + type(node).__qualname__,
                }
            )
            objects.append(node)
        else:
            raise RunnerError("Unsupported custom suite or test runner")

    visit(suite, [])
    if not rows:
        raise RunnerError("Empty test discovery")
    return suite, rows, objects, loader.errors


def intervals(suite, rows):
    spans = {}
    for row in rows:
        group = row["position"][0]
        a, b = spans.get(row["module"], (group, group))
        spans[row["module"]] = min(a, group), max(b, group)
    intervals = []
    for a, b in sorted([*spans.values(), *((i, i) for i, _ in enumerate(suite))]):
        if intervals and a <= intervals[-1][1]:
            intervals[-1][1] = max(b, intervals[-1][1])
        else:
            intervals.append([a, b])
    return intervals


def assign(suite, rows, jobs, weights=None):
    if type(jobs) is not int or jobs not in (1, 2):
        raise RunnerError("Worker count must be 1 or 2")
    counts = collections.Counter(r["position"][0] for r in rows)
    if weights is None:
        weights = [counts[i] for i, _ in enumerate(suite)]
    if (
        type(weights) is not list
        or len(weights) != len(list(suite))
        or any(type(w) is not int or w < 0 or (w == 0) != (counts[i] == 0) for i, w in enumerate(weights))
    ):
        raise RunnerError("Invalid complete group weights")
    loads, selected = [0] * jobs, [[] for _ in range(jobs)]
    for a, b in sorted(intervals(suite, rows), key=lambda p: (-sum(weights[p[0] : p[1] + 1]), p[0])):
        worker = min(range(jobs), key=lambda i: (loads[i], i))
        selected[worker].extend(range(a, b + 1))
        loads[worker] += sum(weights[a : b + 1])
    return [sorted(groups) for groups in selected]


def scheduling_seed():
    """Validate the complete literal; malformed configuration is never a miss."""
    seed = _SCHEDULING_SEED

    def hex_string(value, length):
        return type(value) is str and len(value) == length and all(c in "0123456789abcdef" for c in value)

    if (
        type(seed) is not dict
        or set(seed)
        != {"schema_version", "algorithm", "source_head", "source_map_sha256", "provenance", "entries"}
        or type(seed["schema_version"]) is not int
        or seed["schema_version"] != 1
        or seed["algorithm"] != "fixture-group-lpt-ms-v1"
        or not hex_string(seed["source_head"], 40)
        or not hex_string(seed["source_map_sha256"], 64)
        or type(seed["provenance"]) is not dict
        or set(seed["provenance"]) != {"request.json", "summary.json", "worker-0.jsonl", "worker-1.jsonl"}
        or any(not hex_string(v, 64) for v in seed["provenance"].values())
        or type(seed["entries"]) is not list
        or len(seed["entries"]) != 71
    ):
        raise RunnerError("Invalid scheduling seed descriptor")
    entries = {}
    for entry in seed["entries"]:
        if (
            type(entry) is not dict
            or set(entry) != {"identity_sha256", "weight_ms"}
            or not hex_string(entry["identity_sha256"], 64)
            or entry["identity_sha256"] in entries
            or type(entry["weight_ms"]) is not int
            or not 1 <= entry["weight_ms"] <= 840000
        ):
            raise RunnerError("Invalid scheduling seed entry")
        entries[entry["identity_sha256"]] = entry["weight_ms"]
    if digest(seed) != _SEED_DIGEST:
        raise RunnerError("Scheduling seed provenance differs")
    return entries


def assignment_policy(root, suite, rows, objects, identity, jobs):
    """Fresh discovery/source determines estimates independently in every process."""
    entries = scheduling_seed()
    grouped = [[] for _ in suite]
    origins = [dict() for _ in suite]
    unsupported = set()
    for row, obj in zip(rows, objects, strict=True):
        group = row["position"][0]
        grouped[group].append({**row, "position": row["position"][1:]})
        module_name = type(obj).__module__
        if module_name != row["module"]:
            raise RunnerError("Discovered defining module differs")
        origin = getattr(sys.modules.get(module_name), "__file__", None)
        try:
            path = Path(origin) if type(origin) is str else None
            if path is None or not path.is_absolute():
                raise ValueError("Unsupported origin")
            relative = str(path.relative_to(root))
            if ".." in path.parts or relative not in identity["files"]:
                raise ValueError("Origin outside current source")
            origins[group][relative] = identity["files"][relative]
        except ValueError:
            unsupported.add(group)
    groups = []
    for i, group_rows in enumerate(grouped):
        fingerprint = (
            None if i in unsupported else digest({"rows": group_rows, "defining_sources": origins[i]})
        )
        matched = bool(group_rows) and fingerprint in entries
        reason = (
            "matched"
            if matched
            else "empty"
            if not group_rows
            else "unsupported-origin"
            if i in unsupported
            else "unmatched"
        )
        groups.append(
            {
                "fingerprint": fingerprint,
                "reason": reason,
                "weight_ms": entries[fingerprint] if matched else 1000 * len(group_rows),
            }
        )
    weights = [g["weight_ms"] for g in groups]
    selected = assign(suite, rows, jobs, weights)
    policy = {
        "schema_version": 1,
        "algorithm": _SCHEDULING_SEED["algorithm"],
        "seed_digest": _SEED_DIGEST,
        "provenance": {k: v for k, v in _SCHEDULING_SEED.items() if k != "entries"},
        "groups": groups,
        "intervals": intervals(suite, rows),
        "estimated_load_ms": [sum(weights[i] for i in indices) for indices in selected],
    }
    return policy, selected


class Journal:
    def __init__(self, path, limit):
        self.stream = path.open("xb")
        self.limit = limit
        self.size = 0

    def emit(self, value):
        raw = canonical(value) + b"\n"
        if self.size + len(raw) > self.limit:
            raise RunnerError("Structured evidence overflow")
        self.stream.write(raw)
        self.stream.flush()
        self.size += len(raw)


class FixtureCursor:
    """Correlate callbacks with contiguous runs in this worker's occurrence order."""

    def __init__(self, rows):
        self.rows = rows
        self.cursor = 0
        self.active = None
        self.sequence = 0
        self.last_fixture = None
        self.blocked = {}
        self.failed_setups = set()
        self.runs = {}
        for field, suffix in (("module", "Module"), ("class", "Class")):
            start = 0
            while start < len(rows):
                end = start + 1
                while end < len(rows) and rows[end][field] == rows[start][field]:
                    end += 1
                for prefix in ("setUp", "tearDown"):
                    name = f"{prefix}{suffix} ({rows[start][field]})"
                    self.runs.setdefault(name, []).append((start, end))
                start = end

    def start(self, position):
        if (
            self.active is not None
            or self.cursor >= len(self.rows)
            or position != self.rows[self.cursor]["position"]
        ):
            raise RunnerError("Occurrence skips an unaccounted lifecycle")
        self.active = position
        self.last_fixture = None

    def stop(self, position):
        if position != self.active:
            raise RunnerError("Occurrence stop differs")
        self.active = None
        self.cursor += 1
        self.last_fixture = None

    def fixture(self, name, kind, interval=None, sequence=None):
        if (
            self.active is not None
            or type(kind) is not str
            or kind not in {"skip", "error"}
            or name not in self.runs
        ):
            raise RunnerError("Invalid fixture boundary")
        setup = name.startswith("setUp")
        # Multiple cleanup exceptions belong to the same actual fixture attempt.
        if self.last_fixture and self.last_fixture[0] == name:
            span = self.last_fixture[1]
        else:
            matches = [p for p in self.runs[name] if p[0 if setup else 1] == self.cursor]
            if len(matches) != 1:
                raise RunnerError("Fixture is outside its occurrence interval")
            span = matches[0]
        start, end = span
        if interval is not None and (
            type(interval) is not list
            or len(interval) != 2
            or any(type(i) is not int for i in interval)
            or tuple(interval) != span
        ):
            raise RunnerError("Fixture interval differs")
        if sequence is not None and (type(sequence) is not int or sequence != self.sequence):
            raise RunnerError("Fixture callback sequence differs")
        if setup:
            for i in range(start, end):
                prior = self.blocked.get(i)
                self.blocked[i] = "error" if kind == "error" or prior == "error" else "skip"
            self.failed_setups.add((name, span))
            self.cursor = end
        else:
            setup_name = name.replace("tearDown", "setUp", 1)
            if (setup_name, span) in self.failed_setups or any(
                n.startswith("setUpModule") and a <= start and end <= b for n, (a, b) in self.failed_setups
            ):
                raise RunnerError("Teardown follows failed setup")
        value = {"interval": [start, end], "sequence": self.sequence}
        self.sequence += 1
        self.last_fixture = (name, span)
        return value


class Result(unittest.TextTestResult):
    def __init__(self, *args, journal, rows, objects, **kwargs):
        super().__init__(*args, **kwargs)
        self.journal = journal
        self.pending = collections.defaultdict(collections.deque)
        for row, obj in zip(rows, objects, strict=True):
            self.pending[id(obj)].append(row["position"])
        self.active = {}
        self.objects = objects
        self.fixtures = FixtureCursor(rows)

    def startTest(self, test):
        position = self.pending[id(test)].popleft()
        self.fixtures.start(position)
        self.active[id(test)] = position
        self.journal.emit({"event": "start", "position": position, "monotonic_ns": time.monotonic_ns()})
        super().startTest(test)

    def stopTest(self, test):
        self.fixtures.stop(self.active[id(test)])
        self.journal.emit(
            {"event": "stop", "position": self.active.pop(id(test)), "monotonic_ns": time.monotonic_ns()}
        )
        super().stopTest(test)

    def outcome(self, test, kind, detail=""):
        position = self.active.get(id(getattr(test, "test_case", test)))
        if position is None:
            before = self.fixtures.cursor
            correlation = self.fixtures.fixture(test.id(), kind)
            for i in range(before, self.fixtures.cursor):
                position = self.pending[id(self.objects[i])].popleft()
                if position != self.fixtures.rows[i]["position"]:
                    raise RunnerError("Blocked occurrence identity differs")
            self.journal.emit(
                {"event": "fixture", "id": test.id(), "kind": kind, "detail": detail, **correlation}
            )
        else:
            self.journal.emit({"event": "outcome", "position": position, "kind": kind, "detail": detail})

    def addSuccess(self, test):
        self.outcome(test, "success")
        super().addSuccess(test)

    def addError(self, test, err):
        self.outcome(test, "error", self._exc_info_to_string(err, test))
        super().addError(test, err)

    def addFailure(self, test, err):
        self.outcome(test, "failure", self._exc_info_to_string(err, test))
        super().addFailure(test, err)

    def addSkip(self, test, reason):
        self.outcome(test, "skip", reason)
        super().addSkip(test, reason)

    def addExpectedFailure(self, test, err):
        self.outcome(test, "expected_failure", self._exc_info_to_string(err, test))
        super().addExpectedFailure(test, err)

    def addUnexpectedSuccess(self, test):
        self.outcome(test, "unexpected_success")
        super().addUnexpectedSuccess(test)

    def addSubTest(self, test, subtest, err):
        kind = "subtest_success" if err is None else "subtest_failure"
        self.outcome(test, kind, str(subtest) if err is None else self._exc_info_to_string(err, test))
        super().addSubTest(test, subtest, err)


SUITE_PROFILE = "issue31-suite1800-v1"
SUITE_SECONDS = 1800
SUITE_VERSION = 3


def execution_limits(profile, seconds, text_limit, evidence_limit):
    if (
        type(profile) is not str
        or profile != SUITE_PROFILE
        or type(seconds) not in (int, float)
        or not math.isfinite(seconds)
        or not 0 < seconds <= SUITE_SECONDS
        or type(text_limit) is not int
        or not 0 < text_limit <= TEXT_BYTES
        or type(evidence_limit) is not int
        or not 0 < evidence_limit <= EVIDENCE_BYTES
    ):
        raise RunnerError("Invalid suite execution limits")
    return dict(
        schema_version=1,
        profile=SUITE_PROFILE,
        cap_seconds=SUITE_SECONDS,
        seconds=seconds,
        text_limit=text_limit,
        evidence_limit=evidence_limit,
    )


def request_limits(request):
    version = request.get("version")
    if type(version) is not int or version not in (VERSION, SUITE_VERSION):
        raise RunnerError("Unsupported result protocol version")
    if version == VERSION:
        if "execution_limits" in request:
            raise RunnerError("Legacy request cannot carry suite limits")
        return {}
    limits = request.get("execution_limits")
    if (
        type(limits) is not dict
        or set(limits)
        != {"schema_version", "profile", "cap_seconds", "seconds", "text_limit", "evidence_limit"}
        or type(limits["schema_version"]) is not int
        or limits["schema_version"] != 1
    ):
        raise RunnerError("Invalid suite limit descriptor")
    expected = execution_limits(
        limits["profile"], limits["seconds"], limits["text_limit"], limits["evidence_limit"]
    )
    if canonical(limits) != canonical(expected) or limits["evidence_limit"] != request["evidence_limit"]:
        raise RunnerError("Suite limit descriptor differs")
    return {"execution_limits": expected}


def worker(root, request_path, index, evidence):
    request = strict(request_path.read_bytes())
    limits = request_limits(request)
    suite, rows, objects, errors = discover(root)
    identity = source(root)
    policy, assignments = assignment_policy(root, suite, rows, objects, identity, request["jobs"])
    if canonical(request) != canonical(
        {
            "version": request["version"],
            **limits,
            "jobs": request["jobs"],
            "source": identity,
            "assignment_policy": policy,
            "rows": rows,
            "assignments": assignments,
            "errors": errors,
            "evidence_limit": request["evidence_limit"],
        }
    ):
        raise RunnerError("Worker source or discovery differs")
    if (
        type(request["evidence_limit"]) is not int
        or not 0 < request["evidence_limit"] <= EVIDENCE_BYTES
        or not 0 <= index < request["jobs"]
    ):
        raise RunnerError("Invalid worker bounds")
    groups = assignments[index]
    chosen = [(r, o) for r, o in zip(rows, objects, strict=True) if r["position"][0] in groups]
    journal = Journal(evidence, request["evidence_limit"])
    try:
        journal.emit(
            {"event": "header", "version": request["version"], "worker": index, "request": digest(request)}
        )
        result = unittest.TextTestRunner(
            verbosity=2,
            resultclass=lambda *args, **kw: Result(
                *args,
                **kw,
                journal=journal,
                rows=[x[0] for x in chosen],
                objects=[x[1] for x in chosen],
            ),
        ).run(unittest.TestSuite([child for i, child in enumerate(suite) if i in groups]))
        if source(root) != request["source"]:
            raise RunnerError("Worker source changed")
        journal.emit({"event": "end", "tests_run": result.testsRun, "successful": result.wasSuccessful()})
        return 0 if result.wasSuccessful() and not errors else 1
    finally:
        journal.stream.close()


def reconcile(request, index, path, exit_status):
    request_limits(request)
    if path.stat().st_size > request["evidence_limit"]:
        raise RunnerError("Structured evidence overflow")
    records = [strict(line) for line in path.read_bytes().splitlines()]
    header = {"event": "header", "version": request["version"], "worker": index, "request": digest(request)}
    if not records or canonical(records[0]) != canonical(header):
        raise RunnerError("Worker header differs")
    end = records[-1]
    if (
        set(end) != {"event", "tests_run", "successful"}
        or end["event"] != "end"
        or type(end["tests_run"]) is not int
        or type(end["successful"]) is not bool
    ):
        raise RunnerError("Incomplete worker trailer")
    expected = {
        tuple(r["position"]): r for r in request["rows"] if r["position"][0] in request["assignments"][index]
    }
    started, stopped, outcomes, fixtures = set(), set(), collections.defaultdict(list), []
    active = None
    order = []
    timings = {}
    last_time = 0
    lifecycle = FixtureCursor(list(expected.values()))
    allowed = {
        "success",
        "failure",
        "error",
        "skip",
        "expected_failure",
        "unexpected_success",
        "subtest_success",
        "subtest_failure",
    }
    for record in records[1:-1]:
        event = record.get("event")
        if event == "fixture":
            if set(record) != {"event", "id", "kind", "detail", "interval", "sequence"}:
                raise RunnerError("Invalid fixture result")
            if type(record["id"]) is not str or type(record["detail"]) is not str:
                raise RunnerError("Invalid fixture identity")
            # Null must not select the capture-only omitted-argument path.
            if record["interval"] is None or record["sequence"] is None:
                raise RunnerError("Missing fixture correlation")
            lifecycle.fixture(record["id"], record["kind"], record["interval"], record["sequence"])
            fixtures.append(record)
            continue
        required = {"event", "position"} | ({"kind", "detail"} if event == "outcome" else {"monotonic_ns"})
        if set(record) != required or event not in {"start", "stop", "outcome"}:
            raise RunnerError("Unknown result event")
        position = record["position"]
        if type(position) is not list or any(type(i) is not int for i in position):
            raise RunnerError("Invalid occurrence position")
        key = tuple(position)
        if key not in expected:
            raise RunnerError("Foreign occurrence")
        if event in {"start", "stop"}:
            stamp = record["monotonic_ns"]
            if type(stamp) is not int or stamp < last_time:
                raise RunnerError("Invalid lifecycle clock")
            last_time = stamp
            timings.setdefault(key, {})[event] = stamp
        if event == "start":
            lifecycle.start(position)
            if key in started or active is not None:
                raise RunnerError("Duplicate or overlapping start")
            started.add(key)
            order.append(key)
            active = key
        elif event == "stop":
            if key != active or key in stopped or not outcomes[key]:
                raise RunnerError("Incomplete or duplicate stop")
            lifecycle.stop(position)
            stopped.add(key)
            active = None
        else:
            if key != active or record["kind"] not in allowed or type(record["detail"]) is not str:
                raise RunnerError("Invalid occurrence outcome")
            if outcomes[key] and any(
                x["kind"] not in {"subtest_success", "subtest_failure", "skip"} for x in outcomes[key]
            ):
                # unittest may report a method failure followed by a teardown error.
                if record["kind"] != "error":
                    raise RunnerError("Duplicate terminal outcome")
            outcomes[key].append({"kind": record["kind"], "detail": record["detail"]})
    if active is not None or started != stopped or end["tests_run"] != len(started):
        raise RunnerError("Incomplete test lifecycle")
    if order != [key for key in expected if key in started]:
        raise RunnerError("Occurrence order differs")
    for values in outcomes.values():
        if not any(o["kind"] != "subtest_success" for o in values):
            raise RunnerError("Missing terminal outcome")
    missing = set(expected) - stopped
    skipped = {
        tuple(lifecycle.rows[i]["position"]) for i, kind in lifecycle.blocked.items() if kind == "skip"
    }
    bad = any(
        o["kind"] in {"failure", "error", "unexpected_success", "subtest_failure"}
        for values in outcomes.values()
        for o in values
    ) or any(f["kind"] == "error" for f in fixtures)
    successful = not bad and not (missing - skipped)
    if end["successful"] != (not bad) or exit_status != (
        0 if end["successful"] and not request["errors"] else 1
    ):
        raise RunnerError("Worker exit or success disagrees")
    return {
        "worker": index,
        "exit_status": exit_status,
        "successful": successful and exit_status == 0,
        "occurrences": [
            {
                **row,
                "outcomes": outcomes.get(key, []),
                "timing_ns": timings.get(key, {}),
                "state": "completed"
                if key in stopped
                else "fixture_skip"
                if key in skipped
                else "incomplete",
            }
            for key, row in expected.items()
        ],
        "fixtures": fixtures,
    }


def terminate(processes):
    for process in processes:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    until = time.monotonic() + 0.2
    for process in processes:
        try:
            process.wait(timeout=max(0.001, until - time.monotonic()))
        except subprocess.TimeoutExpired:
            pass
    for process in processes:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()


def run(
    root,
    jobs=2,
    *,
    output=None,
    seconds=SECONDS,
    text_limit=TEXT_BYTES,
    evidence_limit=EVIDENCE_BYTES,
    suite_profile=None,
):
    if type(jobs) is not int or jobs not in (1, 2):
        raise RunnerError("Worker count must be 1 or 2")
    limits = (
        {}
        if suite_profile is None
        else {"execution_limits": execution_limits(suite_profile, seconds, text_limit, evidence_limit)}
    )
    version = SUITE_VERSION if limits else VERSION
    if (
        type(seconds) not in (int, float)
        or not math.isfinite(seconds)
        or not 0 < seconds <= (SUITE_SECONDS if limits else SECONDS)
        or type(text_limit) is not int
        or not 0 < text_limit <= TEXT_BYTES
        or type(evidence_limit) is not int
        or not 0 < evidence_limit <= EVIDENCE_BYTES
    ):
        raise RunnerError("Invalid execution bounds")
    if not all(hasattr(signal, name) for name in ("setitimer", "pthread_sigmask")) or not hasattr(
        os, "killpg"
    ):
        raise RunnerError("Process-group supervision unavailable")
    output = Path(output) if output else Path(tempfile.mkdtemp(prefix="agentic-check-"))
    output.mkdir(parents=True, exist_ok=True)
    print(f"Workflow evidence: {output}", flush=True)
    started = time.monotonic()
    import resource

    usage_before = resource.getrusage(resource.RUSAGE_CHILDREN)
    processes, logs, sizes = [], [], [0] * jobs
    selector = selectors.DefaultSelector()
    summary = {"version": version, **limits, "jobs": jobs, "successful": False, "workers": [], "error": None}
    old_handlers = {s: signal.getsignal(s) for s in (signal.SIGALRM, signal.SIGTERM, signal.SIGINT)}

    def interrupted(signum, frame):
        raise RunnerError("Runner deadline or interruption")

    try:
        for sig in old_handlers:
            signal.signal(sig, interrupted)
        signal.setitimer(signal.ITIMER_REAL, seconds)
        identity = source(root)
        suite, rows, objects, errors = discover(root)
        if source(root) != identity:
            raise RunnerError("Source changed during discovery")
        policy, assignments = assignment_policy(root, suite, rows, objects, identity, jobs)
        summary["assignment_policy"] = policy
        request = {
            "version": version,
            **limits,
            "jobs": jobs,
            "source": identity,
            "rows": rows,
            "assignments": assignments,
            "assignment_policy": policy,
            "errors": errors,
            "evidence_limit": evidence_limit,
        }
        request_path = output / "request.json"
        request_path.write_bytes(canonical(request))
        summary["request_digest"] = digest(request)
        for index in range(jobs):
            temporary = output / f"tmp-{index}"
            temporary.mkdir()
            log = (output / f"worker-{index}.log").open("xb")
            logs.append(log)
            # Defer cancellation until the new group is registered for cleanup.
            mask = signal.pthread_sigmask(signal.SIG_BLOCK, set(old_handlers))
            try:
                process = subprocess.Popen(
                    [
                        sys.executable,
                        "-B",
                        str(Path(__file__).resolve()),
                        "--worker",
                        str(index),
                        str(root),
                        str(request_path),
                        str(output / f"worker-{index}.jsonl"),
                    ],
                    cwd=root,
                    env={**os.environ, "TMPDIR": str(temporary)},
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
                processes.append(process)
                selector.register(process.stdout, selectors.EVENT_READ, index)
            finally:
                signal.pthread_sigmask(signal.SIG_SETMASK, mask)
        while selector.get_map() or any(p.poll() is None for p in processes):
            if time.monotonic() - started >= seconds:
                raise RunnerError("Runner deadline")
            for key, _ in selector.select(0.05):
                index = key.data
                raw = os.read(key.fileobj.fileno(), 65536)
                if not raw:
                    selector.unregister(key.fileobj)
                    key.fileobj.close()
                    continue
                room = max(0, text_limit - sizes[index])
                logs[index].write(raw[:room])
                sizes[index] += len(raw)
                if sizes[index] > text_limit:
                    raise RunnerError("Text output overflow; retained prefix is incomplete")
            for index in range(len(processes)):
                path = output / f"worker-{index}.jsonl"
                if path.exists() and path.stat().st_size > evidence_limit:
                    raise RunnerError("Structured evidence overflow")
        for index, process in enumerate(processes):
            summary["workers"].append(
                reconcile(request, index, output / f"worker-{index}.jsonl", process.wait())
            )
        if source(root) != identity:
            raise RunnerError("Source changed during execution")
        summary["successful"] = all(w["successful"] for w in summary["workers"]) and not errors
        summary["occurrences"] = len(rows)
    except (RunnerError, OSError, ValueError, TypeError, KeyError, IndexError) as exc:
        summary["error"] = str(exc)
    finally:
        # Always reap groups, including children that outlive an exited worker.
        signal.setitimer(signal.ITIMER_REAL, 0)
        for sig in old_handlers:
            signal.signal(sig, signal.SIG_IGN)
        terminate(processes)
        selector.close()
        for log in logs:
            log.close()
        for sig, handler in old_handlers.items():
            signal.signal(sig, handler)
        usage = resource.getrusage(resource.RUSAGE_CHILDREN)
        summary["child_user_seconds"] = usage.ru_utime - usage_before.ru_utime
        summary["child_system_seconds"] = usage.ru_stime - usage_before.ru_stime
        summary["child_peak_rss_native_units"] = usage.ru_maxrss
        summary["elapsed_seconds"] = time.monotonic() - started
        summary["process_exits"] = [p.returncode for p in processes]
        (output / "summary.json").write_bytes(canonical(summary))
        for index in range(len(logs)):
            print(f"--- workflow worker {index} ---", flush=True)
            with (output / f"worker-{index}.log").open("rb") as stream:
                while raw := stream.read(65536):
                    sys.stdout.buffer.write(raw)
            sys.stdout.flush()
        print(
            f"Workflow aggregate: jobs={jobs} occurrences={summary.get('occurrences', 'incomplete')} successful={summary['successful']}",
            flush=True,
        )
        if summary["error"]:
            print(summary["error"], file=sys.stderr)
    return 0 if summary["successful"] else 1


if __name__ == "__main__":
    if len(sys.argv) != 6 or sys.argv[1] != "--worker" or sys.argv[2] not in ("0", "1"):
        raise SystemExit("Internal worker arguments required")
    signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGALRM, signal.SIGTERM, signal.SIGINT})
    raise SystemExit(worker(Path(sys.argv[3]), Path(sys.argv[4]), int(sys.argv[2]), Path(sys.argv[5])))
