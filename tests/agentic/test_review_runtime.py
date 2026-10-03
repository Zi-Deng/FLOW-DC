"""Bound invariant work without caching across mutable packet validations."""

import json
import os
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from unittest.mock import patch

from test_workflow import review, workflow

# isort: split
import review_navigation as navigation
from tasks import digest


class RuntimeTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.packet = self.directory / "packet"
        self.packet.mkdir()
        self.raw = ("short\r\n" * 400 + "\u03bb" * 9000 + "\nlast").encode()
        (self.packet / "source.txt").write_bytes(self.raw)
        (self.packet / "a-dir").mkdir()
        (self.packet / "a-dir/z.txt").write_bytes(b"nested\n")
        (self.packet / "a-dir.txt").write_bytes(b"sibling\n")
        inventory = [
            {
                "id": f"{i:024x}",
                "kind": "test",
                "path": "source.py",
                "revision": "head",
                "artifact": "source.txt",
                "start_line": 1,
                "end_line": 1,
                "bytes": 7,
                "links": [],
            }
            for i in range(100)
        ]
        (self.packet / "required-material.json").write_text(
            json.dumps({"schema_version": 2, "required": inventory}) + "\n"
        )
        self.unit = {
            "required_ids": [],
            "context_ids": [r["id"] for r in inventory],
            "navigation_ids": [r["id"] for r in inventory[:10]],
        }

    def test_render_matches_base_bytes_and_hoists_whole_artifact_hash(self):
        with patch.object(navigation, "sha", wraps=navigation.sha) as hashed:
            files = navigation.render(self.packet, self.unit, 10000)
        self.assertEqual(
            digest({name: body.hex() for name, body in files.items()}),
            "a5dab880498fc4c648a68885401642d58981d2b12af0f9df23ee6606523ed06c",
        )
        # One hash for the windows and one independent catalog read, not one
        # whole-file hash per returned window. Range hashes remain required.
        self.assertLessEqual(sum(call.args[0] == self.raw for call in hashed.call_args_list), 2)
        self.assertGreater(len(navigation.windows(self.packet, "source.txt")), 4)

    def test_render_prunes_generated_tree_and_builds_preferred_membership_once(self):
        binding = navigation.materialize(self.packet, self.unit, 10000)
        expected = navigation.render(self.packet, self.unit, 10000)
        visits = []
        scan = os.scandir

        def tracked(path):
            visits.append(Path(path))
            return scan(path)

        class Preferred(list):
            iterations = 0

            def __iter__(self):
                self.iterations += 1
                return super().__iter__()

        preferred = Preferred(self.unit["navigation_ids"])
        unit = {**self.unit, "navigation_ids": preferred}
        with patch.object(os, "scandir", side_effect=tracked):
            self.assertEqual(navigation.render(self.packet, unit, 10000), expected)
        self.assertFalse([p for p in visits if p.is_relative_to(self.packet / "navigation")])
        self.assertEqual(preferred.iterations, 1)
        navigation.validate(self.packet, self.unit, 10000, binding)
        generated = next((self.packet / "navigation").rglob("*.jsonl"))
        generated.write_bytes(generated.read_bytes()[:-1])
        with self.assertRaises(workflow.WorkflowError):
            navigation.validate(self.packet, self.unit, 10000, binding)

    def test_packet_validation_visits_once_and_rechecks_all_files_links_and_mutations(self):
        navigation.materialize(self.packet, self.unit, 10000)
        files = {
            str(p.relative_to(self.packet)): review.digest(p) for p in self.packet.rglob("*") if p.is_file()
        }
        metadata = {"schema_version": 3, "files": files}
        (self.directory / "metadata.json").write_text(json.dumps(metadata))
        visits = Counter()
        scan = os.scandir

        def tracked(path):
            visits[Path(path)] += 1
            return scan(path)

        with (
            patch.object(os, "scandir", side_effect=tracked),
            patch.object(review, "digest", wraps=review.digest) as hashed,
        ):
            self.assertEqual(review.verify_packet(self.directory), metadata)
        self.assertEqual(len(hashed.call_args_list), len(files))
        self.assertTrue(all(count == 1 for count in visits.values()), visits)
        target = self.packet / "source.txt"
        target.write_bytes(self.raw[:-1])
        with self.assertRaises(workflow.WorkflowError):
            review.verify_packet(self.directory)
        target.write_bytes(self.raw)
        self.assertEqual(review.verify_packet(self.directory), metadata)
        target.unlink()
        with self.assertRaises(workflow.WorkflowError):
            review.verify_packet(self.directory)
        target.write_bytes(self.raw)
        for name, destination in [
            ("file-link", target),
            ("directory-link", self.packet / "a-dir"),
            ("dangling", self.packet / "absent"),
        ]:
            link = self.packet / name
            link.symlink_to(destination)
            # Include a file link's exact bytes in the manifest: only the link
            # check can reject it, rather than merely an unexpected pathname.
            linked_metadata = {**metadata, "files": {**files}}
            if destination.is_file():
                linked_metadata["files"][name] = review.digest(destination)
            (self.directory / "metadata.json").write_text(json.dumps(linked_metadata))
            with self.assertRaises(workflow.WorkflowError):
                review.verify_packet(self.directory)
            link.unlink()
            (self.directory / "metadata.json").write_text(json.dumps(metadata))
        self.assertEqual(review.verify_packet(self.directory), metadata)
