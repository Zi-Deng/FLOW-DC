#!/usr/bin/env python3
"""Compare the Python decision engine with the unchanged pinned Java class."""

import argparse
import csv
import hashlib
import io
import json
import math
import subprocess
import sys
import tempfile
import urllib.request
from dataclasses import asdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bin"))
from flowdc_gradient2 import Gradient2, Gradient2Config  # noqa: E402

REFERENCE = ROOT / "third_party/netflix-gradient2"
CASES = ROOT / "tests/fixtures/gradient2/cases.json"
SLF4J_URL = "https://repo.maven.apache.org/maven2/org/slf4j/slf4j-api/1.7.32/slf4j-api-1.7.32.jar"
SLF4J_SHA256 = "3624f8474c1af46d75f98bc097d7864a323c81b3808aa43689a6e1c601c027be"
PARAMETERS = tuple(asdict(Gradient2Config()))


def observations():
    for case in json.loads(CASES.read_text()):
        config = Gradient2Config(**case["options"])
        index = 0
        for delay, inflight, drop, count in case["segments"]:
            for _ in range(count):
                yield case["id"], index, config, delay, inflight, drop
                index += 1


def compare(raw):
    rows = list(csv.DictReader(io.StringIO(raw.decode())))
    inputs = list(observations())
    if len(rows) != len(inputs):
        raise AssertionError("Reference omitted or added observations")
    engine = None
    scenario = None
    for row, (name, index, config, delay, inflight, drop) in zip(rows, inputs, strict=True):
        if name != scenario:
            engine = Gradient2(config)
            scenario = name
        assert row["scenario"] == name and int(row["index"]) == index
        assert int(row["delay_ns"]) == delay and int(row["inflight"]) == inflight
        assert row["did_drop"] == str(drop).lower()
        for parameter, value in asdict(config).items():
            assert float(row[parameter]) == value, parameter
        assert engine.sample(delay, inflight, drop) == int(row["limit"]), (name, index, "limit")
        assert engine.last_delay == int(row["last_delay_ns"])
        # IEEE double serialization; limit is exact, state permits only tiny roundoff.
        assert math.isclose(
            engine.estimated_limit, float(row["estimated_limit"]), rel_tol=1e-15, abs_tol=1e-12
        ), (name, index, "estimate")
        assert math.isclose(engine.long_delay, float(row["long_delay_ns"]), rel_tol=1e-15, abs_tol=1e-9), (
            name,
            index,
            "average",
        )
    return len(rows)


def reference(java, javac, jar=None):
    provenance = json.loads((REFERENCE / "provenance.json").read_text())
    for item in provenance["files"]:
        assert hashlib.sha256((REFERENCE / item["path"]).read_bytes()).hexdigest() == item["sha256"]
    with tempfile.TemporaryDirectory(prefix="flowdc-gradient2-") as temporary:
        workspace = Path(temporary)
        if jar is None:
            jar = workspace / "slf4j.jar"
            with urllib.request.urlopen(SLF4J_URL, timeout=30) as response:
                data = response.read(100_000)
            jar.write_bytes(data)
        jar = Path(jar).resolve()
        assert hashlib.sha256(jar.read_bytes()).hexdigest() == SLF4J_SHA256, "Wrong pinned SLF4J artifact"
        classes = workspace / "classes"
        classes.mkdir()
        sources = sorted((REFERENCE / "src").rglob("*.java")) + [REFERENCE / "ReferenceTrace.java"]
        subprocess.run(
            [
                javac,
                "--release",
                "8",
                "-encoding",
                "UTF-8",
                "-cp",
                str(jar),
                "-d",
                str(classes),
                *map(str, sources),
            ],
            check=True,
            capture_output=True,
            timeout=60,
        )
        lines = []
        for name, index, config, delay, inflight, drop in observations():
            lines.append(
                ",".join(
                    map(str, [name, index, delay, inflight, str(drop).lower(), *asdict(config).values()])
                )
            )
        return subprocess.run(
            [java, "-cp", f"{classes}:{jar}", "ReferenceTrace"],
            input=("\n".join(lines) + "\n").encode(),
            check=True,
            capture_output=True,
            timeout=60,
        ).stdout


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--java", default="java")
    parser.add_argument("--javac", default="javac")
    parser.add_argument("--slf4j-jar", type=Path)
    parser.add_argument(
        "--write-fixture", action="store_true", help="Intentional reference fixture regeneration"
    )
    args = parser.parse_args()
    raw = reference(args.java, args.javac, args.slf4j_jar)
    count = compare(raw)
    fixture = CASES.with_name("reference.csv")
    if args.write_fixture:
        fixture.write_bytes(raw)
    else:
        assert fixture.read_bytes() == raw, "Retained reference fixture changed"
    print(
        json.dumps(
            {
                "status": "pass",
                "observations": count,
                "scenarios": len(json.loads(CASES.read_text())),
                "upstream_revision": json.loads((REFERENCE / "provenance.json").read_text())["revision"],
                "reference_sha256": hashlib.sha256(raw).hexdigest(),
                "limits": "Decision-engine equivalence on declared traces; no acquisition integration or efficacy evidence",
            }
        )
    )


if __name__ == "__main__":
    main()
