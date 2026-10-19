"""Finite contracts for the single-client external concurrency study."""
import contextlib
import fcntl
import hashlib
import io
import json
import math
import os
import ssl
import subprocess
import time
import urllib.request
import warnings
from pathlib import Path
from urllib.parse import urlsplit

from PIL import Image

HOST = "moth.tbn.org.tw"
PREFIX = "https://moth.tbn.org.tw/images/"
PROFILE = "bounded-external-v1"
METHODS = ("ratio-v1", "gradient-candidate-v1")
OPTIONS = {"fixed-v1": {}, "ratio-v1": {"ratio_buffer_fraction": .3, "ratio_headroom": 1, "sample_window_s": 1.5},
           "gradient-candidate-v1": {"gradient_tau_s": .25, "queue_tau_s": .25, "sample_min": 3, "sample_window_s": 1.5}}
IMAGE_PIXELS = 89_478_485
MAX_OBJECT = 64 * 2**20
RESERVE = 40 * 2**30
ARTIFACT_CAP = 160 * 2**30
CORE = ("bin/download_batch.py", "bin/single_download.py", "bin/flowdc_methods.py", "bin/flowdc_integrity.py")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def file_hash(path, deadline=None):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            if deadline is not None and time.monotonic()>deadline:
                raise TimeoutError("analysis evidence audit deadline")
            h.update(chunk)
    return h.hexdigest()


def encode(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def read_json(path):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, "duplicate JSON key")
            result[key] = value
        return result
    return json.loads(Path(path).read_bytes(), object_pairs_hook=pairs,
                      parse_constant=lambda _: require(False, "nonfinite JSON"))


def write_new(path, value):
    raw = value if isinstance(value, bytes) else encode(value)
    with Path(path).open("xb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())


def utc():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


@contextlib.contextmanager
def lock(path):
    with Path(path).open("a") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


def checked_url(url):
    p = urlsplit(url)
    require(p.scheme == "https" and p.hostname == HOST and p.port in (None, 443)
            and not p.username and not p.password and not p.query and not p.fragment
            and not any(c.isspace() for c in url) and url.startswith(PREFIX), "URL outside moth study")
    return url


def tls_context(ignore_time=False):
    context = ssl.create_default_context()
    if ignore_time:
        context.verify_flags |= 0x200000  # NO_CHECK_TIME: certificate/CRL validity periods only.
    require(context.check_hostname and context.verify_mode == ssl.CERT_REQUIRED, "unverified TLS")
    return context


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def opener(ignore_time=False):
    return urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect(),
                                      urllib.request.HTTPSHandler(context=tls_context(ignore_time)))


def bounded_read(stream, maximum=MAX_OBJECT):
    raw = stream.read(maximum + 1)
    require(len(raw) <= maximum, "body exceeds byte limit")
    return raw


def decode_image(raw):
    Image.MAX_IMAGE_PIXELS = IMAGE_PIXELS
    with warnings.catch_warnings():
        warnings.simplefilter("error", Image.DecompressionBombWarning)
        with Image.open(io.BytesIO(raw)) as image:
            image.verify()
        with Image.open(io.BytesIO(raw)) as image:
            image.load()


def source_identity(source):
    source = Path(source).resolve()
    head = ((source / ".flowdc-source-head").read_text().strip() if (source / ".flowdc-source-head").exists() else
            subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip())
    return {"head": head, "files": {str(p.relative_to(source)): file_hash(p) for p in
            sorted((source / "benchmark/real_server_study").glob("*.py"))
            + [source / p for p in (*CORE, "bin/flowdc_research_profile.py", "benchmark/core/truth.py", "benchmark/core/verifier.py",
                                  "benchmark/real_server_study/requirements.txt")]}}


def protocol_check(study, source):
    study = Path(study)
    p = read_json(study / "protocol.json")
    require(p["schema"] == "flowdc-external-concurrency-v1", "unsupported study")
    require(p["source"] == source_identity(source), "frozen source changed")
    for name, digest in p["files"].items():
        require(not Path(name).is_absolute() and ".." not in Path(name).parts, "unsafe protocol path")
        require(file_hash(study / name) == digest, "frozen input changed: " + name)
    return p


def remaining(study):
    p = read_json(Path(study) / "execution.json")
    require(p["boot_id"] == Path("/proc/sys/kernel/random/boot_id").read_text().strip(), "guest reboot invalidates clock")
    return min(p["deadline_utc"] - time.time(), p["deadline_monotonic"] - time.monotonic())


def terminal(study, reason, detail=""):
    study=Path(study)
    if not (study/"STOP.json").exists():
        write_new(study/"STOP.json",{"utc":utc(),"reason":reason,"detail":detail})
    return {"status":"stopped","runs":len(list((study/"runs").glob("*/attempt.json")))}


def finite(value):
    return type(value) in (int, float) and math.isfinite(value)
