#!/usr/bin/env python3
"""Install a built wheel outside the checkout and run the core-only local demo."""

import argparse
import os
import subprocess
import sys
import tempfile
import venv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheel", type=Path)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="flowdc-wheel-check-") as temporary:
        root = Path(temporary)
        wheels = root / "wheels"
        if args.wheel:
            wheel = args.wheel.resolve()
        else:
            subprocess.run(
                [sys.executable, "-m", "build", "--wheel", "--outdir", str(wheels)], cwd=ROOT, check=True
            )
            wheel = next(wheels.glob("flow_dc-*.whl"))
        environment = root / "environment"
        venv.EnvBuilder(with_pip=True).create(environment)
        python = environment / "bin/python"
        clean = {k: v for k, v in os.environ.items() if k not in ["PYTHONPATH", "PYTHONHOME", "VIRTUAL_ENV"]}
        subprocess.run([str(python), "-m", "pip", "install", str(wheel)], cwd=root, env=clean, check=True)
        subprocess.run(
            [
                str(python),
                "-I",
                "-c",
                "import flowdc; from pathlib import Path; from importlib.metadata import version; "
                "assert 'site-packages' in str(Path(flowdc.__file__)); "
                "assert version('flow-dc') == flowdc.__version__; print(flowdc.__version__)",
            ],
            cwd=root,
            env=clean,
            check=True,
        )
        reported = subprocess.check_output(
            [str(python), "-m", "flowdc", "--version"], cwd=root, env=clean, text=True
        )
        installed = subprocess.check_output(
            [str(python), "-c", "import flowdc; print(flowdc.__version__)"], cwd=root, env=clean, text=True
        )
        assert reported.strip() == "FLOW-DC " + installed.strip(), "CLI/package version mismatch"
        subprocess.run(
            [str(python), str(ROOT / "examples/local_demo.py"), "--output-dir", str(root / "demo")],
            cwd=root,
            env=clean,
            check=True,
        )
        subprocess.run(
            [str(python), "-m", "flowdc", "verify", "--output-dir", str(root / "demo")],
            cwd=root,
            env=clean,
            check=True,
        )
    print("Built wheel installed and acquired/verified a loopback dataset outside the checkout.")


if __name__ == "__main__":
    main()
