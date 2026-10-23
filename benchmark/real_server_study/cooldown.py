"""Optional user-requested study pauses, independent of server Retry-After."""

import time

from .common import finite, require


def user_seconds(protocol):
    """Only set this protocol field when the user explicitly requests a pause."""
    value = protocol.get("user_cooldown_seconds", 0)
    require(finite(value) and value >= 0,
            "user_cooldown_seconds must be a finite nonnegative number")
    return value


def deadline(seconds):
    seconds = user_seconds({"user_cooldown_seconds": seconds})
    return time.time() + seconds if seconds else 0
