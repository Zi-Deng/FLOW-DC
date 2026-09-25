"""Finite allowance request boundaries; private fixtures only, no provider calls."""

import copy
import hashlib
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bin"))

import flowdc_ops as ops
import flowdc_pilot_journal as journal


class GrantRequestTests(unittest.TestCase):
    def request(self):
        return {
            "schema_version": 1,
            "grant_id": "11111111-1111-4111-8111-111111111111",
            "registration_id": "22222222-2222-4222-8222-222222222222",
            "expected_binding_sha256": "a" * 64,
            "expected_limits_seconds": {"manager": 3608, "worker": 3595, "origin": 3602},
            "additional_seconds": 600,
        }

    def test_valid_request_is_not_rewritten_and_digest_is_canonical(self):
        value = self.request()
        before = copy.deepcopy(value)
        self.assertEqual(journal.validate_grant_request(value), before)
        self.assertEqual(value, before)
        self.assertEqual(
            journal.grant_request_digest(value),
            hashlib.sha256(journal.encode(value).encode()).hexdigest(),
        )
        self.assertEqual(
            journal.grant_request_digest(value),
            journal.grant_request_digest(dict(reversed(list(value.items())))),
        )

    def test_strict_fields_types_and_bounds(self):
        cases = []
        for key in self.request():
            value = self.request()
            del value[key]
            cases.append(value)
        for key, invalids in {
            "schema_version": [True, 1.0, 2, None],
            "grant_id": [None, 12, "secret-marker"],
            "registration_id": [False, "secret-marker"],
            "expected_binding_sha256": [None, "A" * 64, "a" * 63, "g" * 64],
            "additional_seconds": [True, False, 0, -1, 1801, 1.5, "600", None, float("inf"), float("nan")],
            "expected_limits_seconds": [None, [], {}, {"manager": 3000, "worker": 3000}],
        }.items():
            for invalid in invalids:
                value = self.request()
                value[key] = invalid
                cases.append(value)
        value = self.request()
        value["unexpected"] = "secret-marker"
        cases.append(value)
        for role in ("manager", "worker", "origin", "extra"):
            for invalid in (True, 0, -1, 7201, 3600.0, "3600", None):
                value = self.request()
                value["expected_limits_seconds"][role] = invalid
                cases.append(value)
        for value in cases:
            with self.subTest(value=value):
                with self.assertRaises(ops.OpsError) as raised:
                    journal.validate_grant_request(value)
                self.assertEqual(raised.exception.code, "invalid_allowance_grant")
                self.assertNotIn("secret-marker", str(raised.exception))

    def test_equal_addition_and_lifetime_ceiling(self):
        for addition in (1, 600, 1800):
            value = self.request()
            value["additional_seconds"] = addition
            value["expected_limits_seconds"] = dict.fromkeys(("manager", "worker", "origin"), 7200 - addition)
            journal.validate_grant_request(value)
            for role in value["expected_limits_seconds"]:
                overflow = copy.deepcopy(value)
                overflow["expected_limits_seconds"][role] += 1
                with self.assertRaises(ops.OpsError):
                    journal.validate_grant_request(overflow)

    def test_binding_pins_only_the_approved_registration_fields(self):
        record = {
            "registration_id": self.request()["registration_id"],
            "profile_path": "/tmp/private/profile.json",
            "spec": {"vms": []},
            "access": {"interfaces": {}},
            "service": {"digest": "b" * 64},
            "heartbeat": None,
            "vms": {"account": {"consumed": 12}},
        }
        before = journal.allowance_binding_sha256(record)
        for key in ("registration_id", "profile_path", "spec", "access", "service"):
            changed = copy.deepcopy(record)
            changed[key] = "changed"
            self.assertNotEqual(journal.allowance_binding_sha256(changed), before)
        record["heartbeat"] = {"utc": 123}
        record["vms"]["account"]["consumed"] = 13
        self.assertEqual(journal.allowance_binding_sha256(record), before)

    def test_private_file_and_strict_json_boundaries(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "grant.json"
            path.write_text(json.dumps(self.request()))
            path.chmod(0o600)
            self.assertEqual(journal.load_grant_request(str(path)), self.request())
            path.chmod(0o644)
            with self.assertRaises(ops.OpsError):
                journal.load_grant_request(str(path))
            path.chmod(0o600)
            for name, link in (("symlink.json", os.symlink), ("hardlink.json", os.link)):
                alias = Path(directory) / name
                link(path, alias)
                with self.assertRaises(ops.OpsError):
                    journal.load_grant_request(str(alias))
                alias.unlink()
            for raw in (
                '{"schema_version":1,"schema_version":1}',
                '{"additional_seconds":NaN}',
                " " * (ops.MAX_BYTES + 1),
            ):
                path.write_text(raw)
                with self.assertRaises(ops.OpsError):
                    journal.load_grant_request(str(path))


if __name__ == "__main__":
    unittest.main()
