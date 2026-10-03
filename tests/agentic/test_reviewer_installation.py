"""Pinned artifact verification and export; local synthetic artifacts only."""

import hashlib
import io
import json
import subprocess
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from test_workflow import SOURCE, install, workflow

# isort: split
import install_tool
import review_cli


class ReviewerInstallationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.binary = self.root / "claude"
        self.binary.write_bytes(b"synthetic executable, never run")
        spec = dict(review_cli.PROVIDERS["claude-code"]["cli"])
        spec["binary_sha256"] = hashlib.sha256(self.binary.read_bytes()).hexdigest()
        self.manifest = self.root / "manifest.json"
        self.manifest.write_text(
            json.dumps(
                {"version": "2.1.282", "platforms": {"linux-x64": {"checksum": spec["binary_sha256"]}}}
            )
        )
        spec["manifest_sha256"] = hashlib.sha256(self.manifest.read_bytes()).hexdigest()
        self.spec = spec
        self.signature = self.root / "manifest.json.sig"
        self.signature.write_bytes(b"synthetic signature")
        self.key = self.root / "key.asc"
        self.key.write_bytes(b"synthetic public key")

    def verify(self, status=None):
        def run(args, **kwargs):
            self.assertIn("--no-options", args)
            home = Path(args[args.index("--homedir") + 1])
            self.assertNotEqual(home, Path.home())
            self.assertEqual(home.stat().st_mode & 0o777, 0o700)
            self.assertEqual(kwargs["env"]["HOME"], str(home))
            self.assertNotIn("GNUPGHOME", kwargs["env"])
            return subprocess.CompletedProcess(
                args,
                0,
                status
                if status is not None
                else b"[GNUPG:] VALIDSIG 31DDDE24DDFAB679F42D7BD2BAA929FF1A7ECACE fixture\n",
                b"",
            )

        with (
            patch.dict(review_cli.PROVIDERS["claude-code"], cli=self.spec),
            patch.object(review_cli, "supported_platform"),
            patch.object(review_cli.shutil, "which", return_value="/usr/bin/gpg"),
            patch.object(review_cli.subprocess, "run", side_effect=run),
        ):
            return review_cli.verify_claude(self.binary, self.manifest, self.signature, self.key)

    def test_signed_pinned_identity_returns_absolute_binary(self):
        self.assertEqual(self.verify(), str(self.binary))

    def test_wrong_signature_key_or_ambiguous_signatures_are_rejected(self):
        for value in [
            b"",
            b"[GNUPG:] VALIDSIG wrong-key fixture\n",
            b"[GNUPG:] VALIDSIG 31DDDE24DDFAB679F42D7BD2BAA929FF1A7ECACE fixture\n" * 2,
        ]:
            with self.subTest(status=value[:20]), self.assertRaises(workflow.WorkflowError):
                self.verify(value)

    def test_tampered_binary_manifest_and_symlink_are_rejected(self):
        self.binary.write_bytes(b"changed executable")
        with self.assertRaisesRegex(workflow.WorkflowError, "digest"):
            self.verify()
        self.binary.unlink()
        self.binary.symlink_to(self.key)
        with self.assertRaises(workflow.WorkflowError):
            self.verify()
        self.binary.unlink()
        self.binary.write_bytes(b"synthetic executable, never run")
        self.manifest.write_text("{}")
        with self.assertRaisesRegex(workflow.WorkflowError, "digest"):
            self.verify()

    def test_unknown_platform_is_refused(self):
        with patch.object(review_cli.platform, "system", return_value="Darwin"):
            with self.assertRaises(workflow.WorkflowError):
                review_cli.supported_platform()

    def test_install_refuses_existing_destination_without_network_or_replacement(self):
        for provider, filename in [("claude-code", "claude"), ("copilot", "copilot")]:
            folder = self.root / provider
            folder.mkdir()
            existing = folder / filename
            existing.write_bytes(b"user installation")
            with (
                patch.object(review_cli, "supported_platform"),
                patch.object(install_tool, "download") as download,
            ):
                with self.assertRaisesRegex(workflow.WorkflowError, "Destination exists"):
                    install_tool.install(provider, folder)
                download.assert_not_called()
            self.assertEqual(existing.read_bytes(), b"user installation")

    def test_hosted_route_remains_explicit_copilot_and_disabled_by_default(self):
        text = (SOURCE / ".github/workflows/copilot-review.yml").read_text()
        self.assertIn("AGENTIC_COPILOT_ACTIONS_ENABLED == 'true'", text)
        self.assertIn("--review-provider copilot --review-model claude-opus-5 --review-effort default", text)
        self.assertIn("register-reviewer copilot", text)
        self.assertNotIn("CLAUDE_CODE_OAUTH_TOKEN", text)
        self.assertNotIn("claude-review-token", text)

    def test_copilot_binary_is_checked_against_pinned_archive_member(self):
        binary = self.root / "copilot"
        binary.write_bytes(b"fixture copilot")
        archive = self.root / "copilot.tar.gz"
        with tarfile.open(archive, "w:gz") as bundle:
            member = tarfile.TarInfo("copilot")
            member.size = len(binary.read_bytes())
            bundle.addfile(member, io.BytesIO(binary.read_bytes()))
        spec = {
            **review_cli.PROVIDERS["copilot"]["cli"],
            "archive_sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
        }
        with (
            patch.dict(review_cli.PROVIDERS["copilot"], cli=spec),
            patch.object(review_cli, "supported_platform"),
        ):
            self.assertEqual(review_cli.verify_copilot(binary, archive), str(binary))
            binary.write_bytes(b"tampered")
            with self.assertRaises(workflow.WorkflowError):
                review_cli.verify_copilot(binary, archive)

    def test_export_contains_both_providers_and_frozen_history_but_no_private_state(self):
        target = self.root / "export"
        result = install.install(SOURCE, target, apply=True)
        self.assertTrue(result["applied"])
        for name in (
            "review_claude",
            "review_copilot",
            "review_policy",
            "review_cli",
            "claude_credentials",
            "claude_telemetry",
            "claude_telemetry_v1",
            "claude_telemetry_v2",
            "claude_telemetry_v3",
            "review_diagnostics",
            "diagnostic_recovery",
            "diagnostic_recovery_v5",
            "diagnostic_recovery_v6",
            "review_coverage_v1",
            "review_coverage_v2",
            "review_telemetry_v2",
        ):
            self.assertTrue((target / "scripts/agentic" / (name + ".py")).is_file())
        self.assertFalse((target / ".agentic-local").exists())
        self.assertFalse(list(target.rglob("*review-token*")))
