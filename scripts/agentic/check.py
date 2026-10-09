#!/usr/bin/env python3
"""Run the one current workflow regression suite within a finite deadline."""

import os
import sys
from pathlib import Path

from providers import capture

ROOT = Path(__file__).resolve().parents[2]


def main():
    result, raw = capture(
        [sys.executable, "-B", "-m", "unittest", "discover", "-s", "tests/agentic", "-v"],
        cwd=ROOT,
        env=os.environ.copy(),
        seconds=120,
        output_limit=2_000_000,
        include_stderr=True,
    )
    sys.stdout.buffer.write(raw)
    if result["reason"]:
        print(f"Workflow suite stopped: {result['reason']}", file=sys.stderr)
        return 1
    return result["exit_status"]


if __name__ == "__main__":
    sys.exit(main())
