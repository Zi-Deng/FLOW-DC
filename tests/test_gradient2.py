"""Pinned Java decisions, real boundaries and rejection without state mutation."""

import hashlib
import importlib.util
import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bin"))
from flowdc_gradient2 import (  # noqa: E402
    MAX_DELAY_NS,
    UPSTREAM_REVISION,
    Gradient2,
    Gradient2Config,
    nanoseconds,
)

spec = importlib.util.spec_from_file_location("verify_gradient2", ROOT / "scripts/verify_gradient2.py")
reference = importlib.util.module_from_spec(spec)
spec.loader.exec_module(reference)


class Gradient2Tests(unittest.TestCase):
    def test_actual_java_fixture_decisions_and_state(self):
        raw = (ROOT / "tests/fixtures/gradient2/reference.csv").read_bytes()
        self.assertEqual(
            hashlib.sha256(raw).hexdigest(),
            "f40d71086fedf3df3d6326906418338356737e4f9597eee32261147475556d05",
        )
        self.assertEqual(reference.compare(raw), 2006)

    def test_pinned_upstream_source_and_license(self):
        root = ROOT / "third_party/netflix-gradient2"
        manifest = json.loads((root / "provenance.json").read_text())
        self.assertEqual(manifest["revision"], UPSTREAM_REVISION)
        for item in manifest["files"]:
            self.assertEqual(hashlib.sha256((root / item["path"]).read_bytes()).hexdigest(), item["sha256"])
        self.assertIn("Apache License", (root / "LICENSE").read_text())

    def test_fractional_limit_utilization_gate_and_sparse_hold(self):
        engine = Gradient2()
        self.assertEqual(engine.limit, 20)
        self.assertEqual(engine.state()["observations"], 0)
        self.assertEqual(engine.sample(100, 10), 20)
        self.assertAlmostEqual(engine.estimated_limit, 20.8)
        self.assertEqual(engine.sample(200, 10), 20)
        self.assertAlmostEqual(engine.estimated_limit, 20.8)
        self.assertEqual(engine.long_delay, 150)
        self.assertEqual(engine.reason, "application_limited")

    def test_strict_decay_boundary_and_warmup_sum(self):
        engine = Gradient2(Gradient2Config(min_limit=1, smoothing=0))
        engine.sample(300, 20)
        engine.sample(100, 20)
        self.assertEqual(engine.long_delay, 200)  # Exactly 2x: no decay.
        engine.sample(50, 20)
        self.assertEqual(engine.long_delay, 142.5)
        engine.sample(100, 20)
        self.assertEqual(engine.long_delay, 137.5)  # Raw sum, not previously decayed value.

    def test_drop_flag_ignored(self):
        left, right = Gradient2(), Gradient2()
        for delay in [100] * 10 + [10000] * 20 + [10] * 20:
            self.assertEqual(left.sample(delay, 200, False), right.sample(delay, 200, True))
            self.assertEqual(left.state(), right.state())

    def test_invalid_observations_fail_before_state_change(self):
        engine = Gradient2()
        before = engine.state()
        for args in [
            (0, 20),
            (True, 20),
            (1.5, 20),
            (MAX_DELAY_NS + 1, 20),
            (1, -1),
            (1, True),
            (1, 10001),
            (1, 20, "false"),
        ]:
            with self.subTest(args=args), self.assertRaises(ValueError):
                engine.sample(*args)
            self.assertEqual(engine.state(), before)

    def test_finite_parameters(self):
        for options in [
            {"initial_limit": 1},
            {"long_window": 0},
            {"max_limit": 10001},
            {"queue_size": -1},
            {"smoothing": float("nan")},
            {"smoothing": 2},
            {"rtt_tolerance": 0.9},
            {"rtt_tolerance": 10**1000},
            {"min_limit": True},
        ]:
            with self.subTest(options=options), self.assertRaises(ValueError):
                Gradient2Config(**options)
        with self.assertRaises(ValueError):
            Gradient2(False)
        self.assertEqual(Gradient2Config(smoothing=0, queue_size=0).smoothing, 0)

    def test_seconds_conversion_and_bounds(self):
        self.assertEqual(nanoseconds(0.0123456789), 12345678)
        self.assertEqual(nanoseconds(1e-9), 1)
        for value in [0, -1, 1e-10, True, "1", float("inf"), float("nan"), 10**1000]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                nanoseconds(value)


if __name__ == "__main__":
    unittest.main()
