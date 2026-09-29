"""Public, non-secret local package/source identity for retained research runs."""

import importlib.metadata
import platform
import subprocess
import sys
from pathlib import Path

from .truth import digest, encode


def environment_record(root):
    distributions = []
    for distribution in importlib.metadata.distributions():
        files = {}
        for item in distribution.files or []:
            path = Path(distribution.locate_file(item))
            if path.is_file() and path.suffix != ".pyc":
                # Package contents, including extension binaries. Do not publish
                # direct_url.json, which can contain private source credentials.
                if path.name == "direct_url.json":
                    continue
                files[str(item)] = digest(path.read_bytes())
        distributions.append(
            {
                "name": distribution.metadata["Name"],
                "version": distribution.version,
                "installed_files_sha256": files,
                "files_manifest_sha256": digest(encode(files)),
            }
        )
    sources = {}
    for directory in ("bin", "benchmark/core"):
        for path in sorted((root / directory).glob("*.py")):
            sources[str(path.relative_to(root))] = digest(path.read_bytes())
    sources["benchmark/known_truth.py"] = digest((root / "benchmark/known_truth.py").read_bytes())
    return {
        "python_version": sys.version,
        "platform": platform.platform(),
        "python_binary_sha256": digest(Path(sys.executable).read_bytes()),
        "distributions": sorted(distributions, key=lambda d: d["name"].lower()),
        "source_files_sha256": sources,
        "git_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
        "git_dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=root)),
        "provenance_scope": "installed content hashes; package-index origins require retained pip install report",
    }
