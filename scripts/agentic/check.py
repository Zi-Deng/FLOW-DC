#!/usr/bin/env python3
"""Dependency-free validation of the portable workflow and its regression suite."""

import ast
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def main():
    for directory in [ROOT / "scripts/agentic", ROOT / "tests/agentic"]:
        for path in directory.glob("*.py"):
            ast.parse(path.read_text(), filename=str(path))
    json.loads((ROOT / ".agentic/config.json").read_text())
    result = subprocess.run(
        [sys.executable, "-B", "-m", "unittest", "discover", "-s", "tests/agentic", "-v"],
        cwd=ROOT,
        check=False,
    )
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
