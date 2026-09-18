"""Deterministic accounting tests; no credentials, network or cloud mutation."""

import importlib.util
import sys
import unittest
from dataclasses import asdict
from pathlib import Path

SPEC = importlib.util.spec_from_file_location(
    "flowdc_pilot", Path(__file__).resolve().parents[1] / "bin/flowdc_pilot.py"
)
pilot = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = pilot
SPEC.loader.exec_module(pilot)


def clock(seconds=0, *, boot="test-boot", wall=None):
    return pilot.ClockSample(boot, seconds, 100000 + (seconds if wall is None else wall))


class AccountingTests(unittest.TestCase):
    def start(self, **kwargs):
        return pilot.Allowance().activation_intent(clock(), window_seconds=7200, **kwargs)

    def test_intent_starts_charging_before_any_provider_result(self):
        account = self.start().account(clock(20))
        self.assertTrue(account.obligation)
        self.assertEqual(account.consumed, 20)

    def test_only_confirmed_offload_settles_and_retries_keep_consumption(self):
        account = self.start()
        for state in ("ACTIVE", "SHUTOFF", "SHELVED", "UNKNOWN", "ERROR", None):
            with self.subTest(state=state):
                self.assertTrue(account.observe(clock(100), state=state).obligation)
        account = account.observe(clock(100), state="SHELVED_OFFLOADED")
        self.assertFalse(account.obligation)
        self.assertEqual(account.remaining, 7100)
        account = account.activation_intent(clock(900), window_seconds=7100)
        self.assertEqual(account.account(clock(1000)).consumed, 200)
        self.assertTrue(account.shutdown_due(clock(7400)))

    def test_duplicate_start_does_not_extend_deadline(self):
        account = self.start()
        with self.assertRaisesRegex(pilot.AccountingError, "outstanding_obligation"):
            account.activation_intent(clock(500), window_seconds=7200)
        self.assertEqual(account.shutdown_at_consumed, 6600)

    def test_shutdown_reserve_and_inspection_window(self):
        account = self.start()
        self.assertFalse(account.shutdown_due(clock(6599)))
        self.assertTrue(account.shutdown_due(clock(6600)))
        inspection = pilot.Allowance().activation_intent(clock(), window_seconds=1800, inspection=True)
        self.assertFalse(inspection.shutdown_due(clock(1199)))
        self.assertTrue(inspection.shutdown_due(clock(1200)))
        with self.assertRaises(pilot.AccountingError):
            pilot.Allowance().activation_intent(clock(), window_seconds=1801, inspection=True)

    def test_delayed_offload_counts_beyond_allowance(self):
        account = self.start().observe(clock(7300), state="SHELVED")
        self.assertEqual(account.consumed, 7300)
        self.assertTrue(account.shutdown_due(clock(7300)))
        account = account.observe(clock(7400), state="SHELVED_OFFLOADED")
        self.assertEqual(account.consumed, 7400)
        self.assertEqual(account.remaining, 0)
        with self.assertRaises(pilot.AccountingError):
            account.activation_intent(clock(7500), window_seconds=601)

    def test_same_boot_restart_restores_obligation_from_serialized_state(self):
        saved = asdict(self.start().account(clock(50)))
        saved["sample"] = pilot.ClockSample(**saved["sample"])
        restored = pilot.Allowance(**saved).account(clock(100))
        self.assertEqual(restored.consumed, 100)
        self.assertEqual(restored.shutdown_at_consumed, 6600)

    def test_clock_ambiguity_requires_cleanup_and_never_replenishes(self):
        for now in (clock(20, boot="new-boot"), clock(20, wall=10), clock(20, wall=40), clock(5)):
            with self.subTest(now=now):
                account = self.start().account(clock(10)).account(now)
                self.assertTrue(account.uncertain)
                self.assertEqual(account.remaining, 0)
                self.assertTrue(account.shutdown_due(now))
                account = account.observe(now, state="SHELVED_OFFLOADED")
                with self.assertRaisesRegex(pilot.AccountingError, "ambiguous_history"):
                    account.activation_intent(now, window_seconds=601)

    def test_small_clock_drift_charges_larger_elapsed_interval(self):
        self.assertEqual(self.start().account(clock(10, wall=12)).consumed, 12)
        self.assertEqual(self.start().account(clock(12, wall=10)).consumed, 12)

    def test_idle_time_is_not_charged(self):
        account = pilot.Allowance(consumed=100)
        self.assertEqual(account.account(clock(99999)), account)

    def test_missing_history_and_invalid_values_are_rejected(self):
        for fields in (
            {"obligation": True},
            {"sample": clock()},
            {"limit": 7201},
            {"consumed": -1},
            {"consumed": float("nan")},
            {"limit": True},
            {"obligation": 1},
            {"uncertain": "false"},
        ):
            with self.subTest(fields=fields), self.assertRaises(pilot.AccountingError):
                pilot.Allowance(**fields)
        for window in (600, 0, -1, 7201, float("inf"), float("nan"), True):
            with self.subTest(window=window), self.assertRaises(pilot.AccountingError):
                pilot.Allowance().activation_intent(clock(), window_seconds=window)


if __name__ == "__main__":
    unittest.main()
