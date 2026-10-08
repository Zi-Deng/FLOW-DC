#!/usr/bin/env python3
"""Dependency-free validation of the portable workflow and its regression suite."""

import argparse
import ast
import json
import sys
from pathlib import Path

import check_runner

ROOT = Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jobs", type=int, choices=(1, 2), default=2)
    parser.add_argument("--suite-profile", choices=(check_runner.SUITE_PROFILE,))
    args = parser.parse_args()
    for directory in [ROOT / "scripts/agentic", ROOT / "tests/agentic"]:
        for path in directory.glob("*.py"):
            ast.parse(path.read_text(), filename=str(path))
    json.loads((ROOT / ".agentic/config.json").read_text())
    if args.suite_profile is not None:
        return check_runner.run(
            ROOT, args.jobs, suite_profile=args.suite_profile, seconds=check_runner.SUITE_SECONDS
        )
    return check_runner.run(ROOT, args.jobs)


if __name__ == "__main__":
    sys.exit(main())
