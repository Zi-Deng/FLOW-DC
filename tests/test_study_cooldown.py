"""Offline checks for user-selected pauses and server-directed delays."""

import json
import tempfile
import unittest
from email.utils import formatdate
from pathlib import Path
from unittest.mock import patch

from benchmark.real_server_study import ablation, cooldown, onset, telemetry
from benchmark.real_server_study.common import write_new
from benchmark.real_server_study.minimum_queue import source_available
from benchmark.real_server_study.public_guard import PublicPolicy
from benchmark.real_server_study.public_metrics import partial_transport


class CooldownTests(unittest.TestCase):
    def test_no_default_pause_on_refusal_or_latency(self):
        self.assertEqual(cooldown.user_seconds({}), 0)
        for policy_type in (telemetry.StopPolicy, onset.Policy, ablation.Policy, PublicPolicy):
            with self.subTest(policy=policy_type.__name__):
                policy = policy_type(.1)
                policy.header(0, 403)
                self.assertTrue(policy.reason)
                self.assertEqual(policy.cooldown_until, 0)
                self.assertEqual(policy.server_retry_after_until, 0)
        policy = PublicPolicy(.1)
        policy.tick(0)
        for bucket in range(3):
            for i in range(20):
                policy.complete(10 * bucket + i / 10, 1)
            policy.tick(10 * (bucket + 1))
        self.assertEqual(policy.reason, "sustained_latency")
        self.assertEqual(policy.cooldown_until, 0)

    def test_opt_in_does_not_extend_on_every_subsequent_response(self):
        with patch.object(cooldown.time, "time", return_value=1000) as clock:
            policy = PublicPolicy(.1, cooldown_seconds=60)
            policy.header(0, 503)
            self.assertEqual(policy.cooldown_until, 1060)
            clock.return_value = 1010
            policy.header(1, 503)
            self.assertEqual(policy.cooldown_until, 1060)

    def test_retry_after_is_separate_and_longer_deadline_wins(self):
        with patch.object(cooldown.time, "time", return_value=1000):
            for header, seconds, expected in (("90", 0, 1090), (formatdate(1120, usegmt=True), 60, 1120),
                                              ("30", 60, 1060)):
                with self.subTest(header=header, seconds=seconds):
                    policy = PublicPolicy(.1, cooldown_seconds=seconds)
                    policy.header(0, 503, header)
                    self.assertEqual(policy.cooldown_until, expected)
                    self.assertEqual(policy.user_cooldown_seconds, seconds)
                    self.assertGreater(policy.server_retry_after_until, 1000)

    def test_invalid_user_setting_rejected_before_preparation(self):
        from benchmark.real_server_study.minimum_study import prepare
        for value in (-1, float("inf"), float("nan"), True, "60", None):
            with self.subTest(value=value), tempfile.TemporaryDirectory() as directory:
                study = Path(directory) / "study"
                with self.assertRaisesRegex(ValueError, "user_cooldown_seconds"):
                    prepare(study, directory, {}, {}, {}, user_cooldown_seconds=value)
                self.assertFalse(study.exists())

    def test_partial_audit_carries_server_deadline_without_inventing_one(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "request-audit.jsonl"
            path.write_text(json.dumps(dict(event="headers", status=503)) + "\n")
            self.assertEqual(partial_transport(directory, 100)["server_retry_after_until_utc"], 0)
            with path.open("a") as stream:
                stream.write(json.dumps(dict(event="headers", status=503,
                                             server_retry_after_until_utc=1120)) + "\n")
            self.assertEqual(partial_transport(directory, 100)["server_retry_after_until_utc"], 1120)

    def test_source_suspension_has_no_default_wait_and_never_rewrites_evidence(self):
        for seconds, server, expected in ((0, 0, 1000), (60, 0, 1060), (0, 1120, 1120), (60, 1120, 1120)):
            with (self.subTest(seconds=seconds, server=server), tempfile.TemporaryDirectory() as directory,
                  patch.object(cooldown.time, "time", return_value=1000) as clock):
                study = Path(directory)
                (study / "gates").mkdir()
                write_new(study / "protocol.json", dict(user_cooldown_seconds=seconds,
                    provenance={"openimages-cvdf": dict(media_rights_qualified=True, access_qualified=True)}))
                history = {"first": dict(run_id="first", source="openimages-cvdf", status="stopped", stop_scope="source",
                                        stop_evidence=[dict(server_retry_after_until_utc=server)])}
                self.assertFalse(source_available(study, "openimages-cvdf", history))
                path = study / "gates/openimages-cvdf-disposition.json"
                original = path.read_bytes()
                record = json.loads(original)
                self.assertEqual(record["earliest_reconsideration_utc"], expected)
                self.assertEqual(record["server_retry_after_until_utc"], server)
                clock.return_value = 2000
                self.assertFalse(source_available(study, "openimages-cvdf", history))
                self.assertEqual(path.read_bytes(), original)


if __name__ == "__main__":
    unittest.main()
