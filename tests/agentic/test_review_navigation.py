"""Bounded navigation retains exact public context and immutable old assignments."""

import json
from unittest.mock import patch

from test_workflow import GitFixture, git, review, workflow

# isort: split
import review_batch as batch
import review_prompt
from batch_fixtures import limits, select


class NavigationTests(GitFixture):
    def prepare(self):
        self.commit_task()
        self.reviews = [
            {
                "id": 101,
                "html_url": "https://github.com/example/project/pull/31#pullrequestreview-101",
                "body": "Check scripts/agentic/alpha.py against its test and acceptance criterion.",
            }
        ]
        for name in ("alpha", "beta"):
            for path in (f"scripts/agentic/{name}.py", f"tests/agentic/test_{name}.py"):
                target = self.task_path / path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text("value = 1\n" * 30)
        git(self.task_path, "add", ".")
        git(self.task_path, "commit", "-m", "two source and test families")
        self.head = git(self.task_path, "rev-parse", "HEAD")
        self.pr_data["head"]["sha"] = self.head
        git(self.task_path, "push", "origin", "HEAD:refs/pull/31/head")
        return review.prepare(self.repo, 31, 12, 1234)

    def test_source_test_families_are_not_hash_interleaved(self):
        directory = self.prepare()
        inventory = json.loads((directory / "packet/required-material.json").read_bytes())["required"]
        lookup = {r["id"]: r for r in inventory}
        families = []
        for unit in batch.plan(directory)["units"][:-1]:
            paths = {lookup[key]["path"] for key in unit["required_ids"]}
            names = {name for name in ("alpha", "beta") if any(name in p for p in paths)}
            if names:
                self.assertEqual(len(names), 1, paths)
                families.append(paths)
                context = [lookup[key] for key in unit["navigation_ids"]]
                self.assertTrue(any(row["kind"] == "acceptance" for row in context))
                if "alpha" in names:
                    self.assertTrue(any(row["kind"] == "finding" for row in context))
        ids = [key for unit in batch.plan(directory)["units"] for key in unit["required_ids"]]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertEqual(set(ids), set(lookup))
        for name in ("alpha", "beta"):
            self.assertTrue(
                any(
                    f"scripts/agentic/{name}.py" in p and f"tests/agentic/test_{name}.py" in p
                    for p in families
                )
            )

    def test_native_prompt_uses_bounded_navigation_and_frozen_report_ceiling(self):
        directory = self.prepare()
        bounds = {**limits(), "max_report_bytes": 10000}
        select(self.repo, directory, bounds)

        def inspect(repo, child, **kwargs):
            meta = review.verify_packet(child)
            prompt = review_prompt.native(child, meta)
            self.assertIn("under 10000 UTF-8 bytes", prompt)
            self.assertNotIn("under 50000", prompt)
            self.assertIn("navigation/START.txt", prompt)
            self.assertIn("offset", prompt)
            self.assertTrue((child / "packet/navigation/START.txt").is_file())
            # Even rehashing a corrupted copy in owner-writable metadata cannot
            # substitute it for deterministic navigation from original bytes.
            page = child / "packet/navigation/START.txt"
            page.write_bytes(page.read_bytes()[:-1])
            meta["files"]["navigation/START.txt"] = review.digest(page)
            from tasks import atomic_json

            atomic_json(child / "metadata.json", meta)
            with self.assertRaisesRegex(workflow.WorkflowError, "Navigation"):
                batch.dispatch_timeout(repo, child, meta, kwargs["dispatch_context"])
            raise workflow.WorkflowError("stop synthetic dispatch")

        with (
            patch.object(review, "review", side_effect=inspect),
            self.assertRaisesRegex(workflow.WorkflowError, "stop synthetic"),
        ):
            batch.execute(self.repo, directory)

    def test_context_windows_are_lossless_bounded_and_validate_missing_changed_bytes(self):
        import review_navigation as navigation

        directory = self.prepare()
        packet = directory / "packet"
        # Actual large-context failure shape, plus Unicode, CRLF and no terminal LF.
        raw = ("row\r\n" * 100000 + "\u03bb" * 12000).encode("utf-8")
        (packet / "findings-and-dispositions.txt").write_bytes(raw)
        unit = {"required_ids": [], "context_ids": []}
        binding = navigation.materialize(packet, unit, 10000)
        navigation.validate(packet, unit, 10000, binding)
        files = list((packet / "navigation").rglob("*"))
        self.assertTrue(all(p.stat().st_size <= 16000 for p in files if p.is_file()))
        windows = navigation.windows(packet, "findings-and-dispositions.txt")
        restored = b""
        for window in windows:
            start, end = window["start_byte"], window["end_byte"]
            self.assertEqual(start, len(restored))
            read = window["read"]
            if window["representation"] == "original":
                lines = raw.split(b"\n")
                selected = b"\n".join(lines[read["offset"] - 1 : read["offset"] - 1 + read["limit"]]) + b"\n"
                self.assertEqual(selected, raw[start:end])
                restored += selected
            else:
                _, copies = navigation.artifact_windows(packet, "findings-and-dispositions.txt")
                selected = b"".join(
                    json.loads(row)[2].encode("utf-8") for row in copies[read["file_path"]].splitlines()
                )
                self.assertEqual(selected, raw[start:end])
                restored += selected
            self.assertLessEqual(read["limit"], 120)
        self.assertEqual(restored, raw)
        original = (packet / "findings-and-dispositions.txt").read_bytes()
        for change in (b"changed", original[:-1]):
            (packet / "findings-and-dispositions.txt").write_bytes(change)
            with self.assertRaises(workflow.WorkflowError):
                navigation.validate(packet, unit, 10000, binding)
        (packet / "findings-and-dispositions.txt").write_bytes(original)
        page = next(p for p in files if p.is_file() and p.name != "START.txt")
        page.unlink()
        with self.assertRaises(workflow.WorkflowError):
            navigation.validate(packet, unit, 10000, binding)

    def test_navigation_refuses_oversized_lines_storage_and_changed_required_links(self):
        import review_navigation as navigation

        directory = self.prepare()
        packet = directory / "packet"
        unit = batch.plan(directory)["units"][0]
        binding = navigation.materialize(packet, unit, 10000)
        changed = {**unit, "required_ids": unit["required_ids"][1:]}
        with self.assertRaises(workflow.WorkflowError):
            navigation.validate(packet, changed, 10000, binding)
        with self.assertRaises(workflow.WorkflowError):
            navigation.validate(packet, unit, 9999, binding)
        (packet / "oversized.txt").write_bytes(b"x" * 65537)
        with self.assertRaises(workflow.WorkflowError):
            navigation.materialize(packet, unit, 10000)
        (packet / "oversized.txt").unlink()
        with patch.object(navigation, "MAX_NAVIGATION_BYTES", 100), self.assertRaises(workflow.WorkflowError):
            navigation.materialize(packet, unit, 10000)

    def test_saved_plan5_retains_exact_assignment_and_incomplete_assessment(self):
        directory = self.prepare()
        original = batch.plan
        with patch.object(batch, "plan", side_effect=lambda path, version=5: original(path, version=5)):
            select(self.repo, directory, limits())
            with patch.object(review, "review", side_effect=workflow.WorkflowError("interrupted")):
                with self.assertRaises(workflow.WorkflowError):
                    batch.execute(self.repo, directory)
            before = review.qualification(directory)
        self.assertEqual(batch.load(directory)["schema_version"], 5)
        self.assertEqual(review.qualification(directory), before)
        child = directory / "units/unit-0000"
        self.assertNotIn("navigation", review.verify_packet(child)["batch_unit"])
        with patch.object(review, "review") as provider:
            batch.execute(self.repo, directory, recover_only=True)
            with self.assertRaisesRegex(workflow.WorkflowError, "durably stopped"):
                batch.execute(self.repo, directory, resume=True)
            provider.assert_not_called()
