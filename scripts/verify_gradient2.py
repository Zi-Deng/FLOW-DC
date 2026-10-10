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


def check(condition, context):
    if not condition:
        raise AssertionError(context)


def observations(*, campaign=False):
    for case in json.loads(CASES.read_text()):
        config = Gradient2Config(**case["options"])
        index = 0
        for delay, inflight, drop, count in case["segments"]:
            for _ in range(count):
                yield case["id"], index, config, delay, inflight, drop
                index += 1
    if campaign:
        for queue in (0, 1, 2, 4):
            for smoothing in (.1, .2):
                config = Gradient2Config(initial_limit=4, min_limit=2, max_limit=16,
                                         queue_size=queue, smoothing=smoothing)
                name = f"campaign-q{queue}-s{smoothing}"
                # Include averaging warmup, a long changed phase, recovery,
                # application-limited updates and ignored drop flags.
                for index, delay in enumerate([20_000_000] * 20 + [200_000_000] * 950 + [20_000_000] * 950):
                    yield name, index, config, delay, 1 if index % 31 == 0 else 16, index % 17 == 0


def compare(raw, *, campaign=False):
    rows = list(csv.DictReader(io.StringIO(raw.decode())))
    inputs = list(observations(campaign=campaign))
    if len(rows) != len(inputs):
        raise AssertionError("Reference omitted or added observations")
    engine = None
    scenario = None
    for row, (name, index, config, delay, inflight, drop) in zip(rows, inputs, strict=True):
        if name != scenario:
            engine = Gradient2(config)
            scenario = name
        check(row["scenario"] == name and int(row["index"]) == index, "Reference verification mismatch")
        check(
            int(row["delay_ns"]) == delay and int(row["inflight"]) == inflight,
            "Reference verification mismatch",
        )
        check(row["did_drop"] == str(drop).lower(), "Reference verification mismatch")
        for parameter, value in asdict(config).items():
            check(float(row[parameter]) == value, parameter)
        check(engine.sample(delay, inflight, drop) == int(row["limit"]), (name, index, "limit"))
        check(engine.last_delay == int(row["last_delay_ns"]), "Reference verification mismatch")
        # IEEE double serialization; limit is exact, state permits only tiny roundoff.
        check(
            math.isclose(engine.estimated_limit, float(row["estimated_limit"]), rel_tol=1e-15, abs_tol=1e-12),
            (name, index, "estimate"),
        )
        check(
            math.isclose(engine.long_delay, float(row["long_delay_ns"]), rel_tol=1e-15, abs_tol=1e-9),
            (
                name,
                index,
                "average",
            ),
        )
    return len(rows)


def reference(java, javac, jar=None, *, campaign=False):
    provenance = json.loads((REFERENCE / "provenance.json").read_text())
    for item in provenance["files"]:
        check(
            hashlib.sha256((REFERENCE / item["path"]).read_bytes()).hexdigest() == item["sha256"],
            "Reference verification mismatch",
        )
    with tempfile.TemporaryDirectory(prefix="flowdc-gradient2-") as temporary:
        workspace = Path(temporary)
        if jar is None:
            jar = workspace / "slf4j.jar"
            with urllib.request.urlopen(SLF4J_URL, timeout=30) as response:
                data = response.read(100_000)
            jar.write_bytes(data)
        jar = Path(jar).resolve()
        check(hashlib.sha256(jar.read_bytes()).hexdigest() == SLF4J_SHA256, "Wrong pinned SLF4J artifact")
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
        for name, index, config, delay, inflight, drop in observations(campaign=campaign):
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
    parser.add_argument("--campaign", action="store_true", help="Also validate all eight campaign parameter configurations")
    parser.add_argument("--save-reference", type=Path, help="Retain fresh Java output at a new path")
    parser.add_argument(
        "--write-fixture", action="store_true", help="Intentional reference fixture regeneration"
    )
    args = parser.parse_args()
    check(not (args.campaign and args.write_fixture), "Campaign traces must not replace the historical fixture")
    raw = reference(args.java, args.javac, args.slf4j_jar, campaign=args.campaign)
    count = compare(raw, campaign=args.campaign)
    fixture = CASES.with_name("reference.csv")
    if args.write_fixture:
        fixture.write_bytes(raw)
    elif not args.campaign:
        check(fixture.read_bytes() == raw, "Retained reference fixture changed")
    if args.save_reference:
        with args.save_reference.open("xb") as stream:
            stream.write(raw)
    print(
        json.dumps(
            {
                "status": "pass",
                "observations": count,
                "scenarios": len(json.loads(CASES.read_text())) + (8 if args.campaign else 0),
                "upstream_revision": json.loads((REFERENCE / "provenance.json").read_text())["revision"],
                "reference_sha256": hashlib.sha256(raw).hexdigest(),
                "limits": "Decision-engine equivalence on declared traces; no acquisition integration or efficacy evidence",
            }
        )
    )


if __name__ == "__main__":
    main()
