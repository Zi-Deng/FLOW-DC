"""Packet traversal retains fresh content checks and rejects root/descendant links."""

import os
from unittest.mock import patch

from test_workflow import GitFixture, review, workflow


class PacketTreeTests(GitFixture):
    def setUp(self):
        super().setUp()
        self.commit_task()
        self.directory = review.prepare(self.repo, 31, 12, 1234)
        self.packet = self.directory / "packet"

    def test_packet_root_link_is_rejected_even_when_every_file_hash_matches(self):
        original = review.verify_packet(self.directory)
        self.assertTrue(original["files"])
        other = self.parent / "external-packet"
        self.packet.rename(other)
        self.packet.symlink_to(other, target_is_directory=True)
        with self.assertRaises(workflow.WorkflowError):
            review.verify_packet(self.directory)

    def test_same_size_same_timestamp_changes_are_not_cached(self):
        review.verify_packet(self.directory)
        path = self.packet / "capability/fixture.txt"
        stat = path.stat()
        raw = path.read_bytes()
        path.write_bytes(b"X" + raw[1:])
        os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns))
        with self.assertRaises(workflow.WorkflowError):
            review.verify_packet(self.directory)
        path.write_bytes(raw)
        os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns))
        self.assertTrue(review.verify_packet(self.directory)["files"])

    def test_descendant_link_never_reads_its_target(self):
        external = self.parent / "outside.txt"
        external.write_text("harmless synthetic outside content")
        link = self.packet / "unexpected-link.txt"
        link.symlink_to(external)
        original = review.digest

        def checked(path):
            self.assertNotEqual(path, link, "must reject link before reading target")
            return original(path)

        with patch.object(review, "digest", side_effect=checked):
            with self.assertRaises(workflow.WorkflowError):
                review.verify_packet(self.directory)
