"""Frozen history must recover exact bytes without acquiring dispatch or readiness."""

import json
import shutil
from unittest.mock import patch

from test_workflow import GitFixture, review, workflow

# isort: split

import review_batch_v4 as batch
import review_coverage as current_coverage
import review_coverage_issue31_v3 as coverage
import review_coverage_v5 as coverage5
import review_issue31_v3 as historical
from review_fixtures import events
from tasks import atomic_json, digest


class CompatibilityTests(GitFixture):
    def setUp(self):
        super().setUp()
        self.commit_task()

    def historical_parent(self, version):
        directory = review.prepare(self.repo, 31, 12, 1234)
        meta = review.verify_packet(directory)
        for key in ("review_policy", "selection_sources", "kind"):
            meta.pop(key, None)
        meta["schema_version"] = 3
        atomic_json(directory / "metadata.json", meta)
        planned = {**batch.plan(directory, version=version), "budget": batch.budget(100, 100, 6000, 1, 60)}
        atomic_json(directory / "batch.json", planned)
        meta.update(schema_version=4, batch_sha256=digest(planned))
        atomic_json(directory / "metadata.json", meta)
        unit = planned["units"][0]
        row = {"unit": unit["id"], "credits": 1, "seconds": 60}
        atomic_json(
            directory / "batch-state.json",
            {
                "schema_version": 1,
                "batch_sha256": digest(planned),
                "started": 1000,
                "deadline": 7000,
                "reservations": [row],
            },
        )
        child = batch.unit_path(directory, unit)
        child.mkdir(parents=True)
        shutil.copytree(directory / "packet", child / "packet")
        assignment = {
            "batch_sha256": digest(planned),
            "unit": unit,
            "required_ids": unit["required_ids"],
            "dependencies": {},
        }
        if version >= 4:
            assignment["publication_version"] = 2
        if version >= 2:
            items = coverage.read_json(child / "packet/required-material.json")["required"]
            assignment["inspection_suggestions"] = batch.inspection_suggestions(
                child / "packet", [i for i in items if i["id"] in unit["required_ids"]], version=version
            )
        atomic_json(child / "packet/assignment.json", assignment)
        cm = {key: value for key, value in meta.items() if key != "batch_sha256"}
        cm.update(
            schema_version=3,
            batch_unit=assignment,
            files={
                str(p.relative_to(child / "packet")): review.digest(p)
                for p in (child / "packet").rglob("*")
                if p.is_file()
            },
            config={**meta["config"], "review_timeout_seconds": 60, "review_max_ai_credits": 1},
        )
        atomic_json(child / "metadata.json", cm)
        raw = "\n".join(json.dumps(e) for e in events(child / "packet"))
        body, diagnostics = coverage.parse_events(
            raw, child / "packet", child / "packet", version="1.0.83", usage={"totalNanoAiu": 100}
        )
        historical.save_result(child, cm, body, diagnostics, "1.0.83")
        return directory, child, body

    def test_all_historical_plan_versions_exact_capture_and_publication_recover(self):
        for version in (1, 2, 3, 4):
            with self.subTest(version=version):
                parent, child, body = self.historical_parent(version)
                capture = (child / "review-capture.json").read_bytes()
                (child / "review-result.json").unlink()
                with patch.object(review, "review", side_effect=AssertionError("No inference")):
                    historical.recover_review(self.repo, child)
                self.assertEqual((child / "review.md").read_bytes(), body.encode())
                self.assertEqual((child / "review-capture.json").read_bytes(), capture)
                batch.finalize(parent)
                before = {
                    str(p.relative_to(parent)): p.read_bytes() for p in parent.rglob("*") if p.is_file()
                }
                expected = historical.qualification(parent)
                self.assertEqual(review.qualification(parent), expected)
                self.assertEqual(review.publication_body(parent), historical.publication_body(parent))
                self.assertEqual(review.publication_body(child), historical.publication_body(child))
                self.assertFalse(review.coverage_ready(parent))
                self.assertFalse(review.coverage_ready(child))
                with self.assertRaises(workflow.WorkflowError):
                    review.qualification(parent, require=True)
                with self.assertRaisesRegex(workflow.WorkflowError, "recovery-only"):
                    batch.execute(self.repo, parent, resume=True)
                self.assertEqual(
                    {str(p.relative_to(parent)): p.read_bytes() for p in parent.rglob("*") if p.is_file()},
                    before,
                )
                publications = []
                original_api = self.repo.api

                def github(
                    endpoint, data=None, *, original_api=original_api, publications=publications, **kwargs
                ):
                    if endpoint != "pulls/31/reviews":
                        return original_api(endpoint, data=data, **kwargs)
                    if data is None:
                        return publications
                    value = {**data, "id": len(publications) + 1, "state": "COMMENTED", "html_url": "fixture"}
                    publications.append(value)
                    return value

                with patch.object(self.repo, "api", side_effect=github):
                    review.publish(self.repo, parent)
                    self.assertEqual(len(publications), 2)
                    self.assertTrue(review.verify_publication(self.repo, parent)["exact_match"])
                    publications[0]["body"] += "changed child"
                    with self.assertRaises(workflow.WorkflowError):
                        review.verify_publication(self.repo, parent)
                (child / "review.md").write_bytes(body.encode() + b" ")
                with self.assertRaises(workflow.WorkflowError):
                    review.qualification(parent)

    def test_detached_child_does_not_select_favorable_evaluator(self):
        parent, child, _ = self.historical_parent(4)
        historical.recover_review(self.repo, child)
        detached = self.parent / "detached"
        shutil.copytree(child, detached)
        with self.assertRaises(workflow.WorkflowError):
            review.qualification(detached)

    def test_schema5_capture_uses_merged_frozen_evaluator_not_new_coverage(self):
        directory = review.prepare(self.repo, 31, 12, 1234)
        meta = review.verify_packet(directory)
        meta["schema_version"] = 5
        meta.pop("kind")
        atomic_json(directory / "metadata.json", meta)
        packet = directory / "packet"
        raw = "\n".join(json.dumps(e) for e in events(packet))
        body, diagnostics = coverage5.parse_events(
            raw, packet, packet, version="1.0.83", usage={"totalNanoAiu": 100}
        )
        expected = coverage5.assess(packet, body, diagnostics, meta["review_policy"])
        with patch.object(current_coverage, "assess", side_effect=AssertionError("No new interpretation")):
            review.save_result(directory, meta, body, diagnostics, "1.0.83")
            capture = (directory / "review-capture.json").read_bytes()
            (directory / "review-result.json").unlink()
            review.recover_review(self.repo, directory)
            self.assertEqual(review.qualification(directory), expected)
            self.assertEqual((directory / "review-capture.json").read_bytes(), capture)
