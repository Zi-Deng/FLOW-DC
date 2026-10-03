"""Explicit synthetic authorizations; these never authorize a real account."""

import time
from pathlib import Path

from test_workflow import review

# isort: split
import review_batch as batch
from tasks import digest


def limits():
    return dict(
        requests=100,
        kind="ai-credits",
        cost="100",
        seconds=6000,
        unit_cost="1",
        unit_seconds=60,
        max_report_bytes=50000,
        max_integration_bytes=5000000,
    )


def authorize(repo, directory, bounds):
    preview = batch.preview(directory, bounds)
    return {
        "name": "synthetic-test-only",
        "preview_digest": digest(preview),
        "harness_commit": repo.git("rev-parse", "HEAD"),
        "harness_files": {"scripts/agentic/review_batch.py": review.digest(Path(batch.__file__))},
        "expires_at": time.time() + bounds["seconds"] + 100,
    }


def select(repo, directory, bounds):
    if review.verify_packet(directory).get("kind") == "batch-parent":
        return batch.select(directory, bounds)
    return batch.select(directory, bounds, authorize(repo, directory, bounds))
