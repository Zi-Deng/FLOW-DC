"""Finite, versioned research budgets shared by all acquisition boundaries.

The legacy fixture profile remains available for interpreting retained v1
records. New larger runs must select bounded-research-v2 explicitly.
"""
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class Workload:
    name: str
    max_rows: int
    max_payload_bytes: int
    max_metadata_bytes: int
    max_artifact_bytes: int
    max_return_bytes: int
    acquisition_seconds: int
    max_files: int
    max_permits: int
    max_events: int
    max_row_metadata_bytes: int = 64 * 1024
    max_attempts: int = 4
    max_clients: int = 64
    max_object_bytes: int = 64 * 2**20

    def record(self):
        return asdict(self)


LEGACY = Workload("bounded-fixture-v1", 256, 64 * 2**20, 16 * 2**20,
                  128 * 2**20, 192 * 2**20, 180, 8192, 16384, 262144)
RESEARCH = Workload("bounded-research-v2", 32768, 512 * 2**20, 32 * 2**20,
                    2 * 2**30, 2 * 2**30, 300, 524288, 131072, 2097152)


def workload(name=None):
    if name is None or name == LEGACY.name:
        return LEGACY
    if name == RESEARCH.name:
        return RESEARCH
    raise ValueError("unknown finite research workload profile")


def from_record(record):
    """Records carry the complete limits; a name cannot silently change them."""
    if not isinstance(record, dict):
        raise ValueError("missing finite workload record")
    result = workload(record.get("name"))
    if record != result.record():
        raise ValueError("workload limits changed")
    return result


class BodyBudget:
    """Event-loop-owned finite observed bytes, including unsuccessful attempts."""

    def __init__(self, limits):
        self.limits = limits
        self.observed_bytes = 0

    def charge(self, count, object_bytes):
        if (object_bytes > self.limits.max_object_bytes
                or self.observed_bytes + count > self.limits.max_payload_bytes * self.limits.max_attempts):
            raise ValueError("observed HTTP body exceeds finite research workload")
        self.observed_bytes += count
