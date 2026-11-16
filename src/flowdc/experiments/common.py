"""Shared, finite experiment primitives; no operational credentials are loaded."""

import hashlib
import math
import ssl
from pathlib import Path

from ..integrity import encode, parse


def require(condition, message):
    if not condition:
        raise ValueError(message)


def finite(value):
    return type(value) in (int, float) and math.isfinite(value)


def checked_url(url):
    from .guard import checked_url as validate

    return validate(url, "taibif-moth")


def tls_context(ignore_time=False):
    context = ssl.create_default_context()
    if ignore_time:
        # Historical moth protocol exception; chain and hostname checks remain enabled.
        context.verify_flags |= 0x200000
    require(context.check_hostname and context.verify_mode == ssl.CERT_REQUIRED, "TLS verification disabled")
    return context


def file_hash(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def read_json(path):
    return parse(Path(path).read_bytes())


def write_new(path, value):
    with Path(path).open("xb") as stream:
        stream.write(encode(value))


def sha256(value):
    return hashlib.sha256(value).hexdigest()
