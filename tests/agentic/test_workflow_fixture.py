"""Keep generic contracts bounded; exercise live policy bytes explicitly too."""

import json
import shutil

from test_workflow import SOURCE, GitFixture, git, review, workflow

# isort: split
import review_batch
import review_navigation


class FixtureContractTests(GitFixture):
    def test_generic_contract_has_no_incidental_live_documentation(self):
        policies = self.root / "docs/agent-workflow"
        self.assertEqual(sorted(p.name for p in policies.iterdir()), ["REVIEW.md", "domain-review.md"])
        self.assertLess(sum(p.stat().st_size for p in policies.iterdir()), 2000)
        self.assertLess((self.root / "AGENTS.md").stat().st_size, 1000)
        self.commit_task()
        directory = review.prepare(self.repo, 31, 12, 1234)
        inventory = json.loads((directory / "packet/required-material.json").read_bytes())["required"]
        assigned = [key for unit in review_batch.plan(directory)["units"] for key in unit["required_ids"]]
        self.assertCountEqual(assigned, [item["id"] for item in inventory])
        self.assertEqual(len(assigned), len(set(assigned)))
        self.assertEqual(
            {item["artifact"] for item in inventory if item["kind"] == "policy"},
            {"repository-policy.txt", "review-policy.txt", "domain-policy.txt"},
        )

    def test_live_policies_and_available_docs_are_exact_and_mutations_refuse(self):
        # This test deliberately uses the whole current documentation tree. The
        # generic lifecycle tests do not need to repeat this expanding contract.
        shutil.copytree(SOURCE / "docs/agent-workflow", self.root / "docs/agent-workflow", dirs_exist_ok=True)
        shutil.copyfile(SOURCE / "AGENTS.md", self.root / "AGENTS.md")
        git(self.root, "add", ".")
        git(self.root, "commit", "-m", "actual workflow policies")
        self.base = git(self.root, "rev-parse", "HEAD")
        git(self.root, "push", "origin", "trunk")
        self.commit_task()
        directory = review.prepare(self.repo, 31, 12, 1234)
        packet = directory / "packet"
        inventory = json.loads((packet / "required-material.json").read_bytes())["required"]
        sources = json.loads((packet / "source-index.json").read_bytes())
        by_path = {item["path"]: item for item in sources}
        for path in sorted((SOURCE / "docs/agent-workflow").rglob("*.md")):
            relative = str(path.relative_to(SOURCE))
            self.assertNotIn("omitted", by_path[relative])
            self.assertEqual((packet / by_path[relative]["snapshot"]).read_bytes(), path.read_bytes())
        for original, artifact in (
            ("AGENTS.md", "repository-policy.txt"),
            ("docs/agent-workflow/REVIEW.md", "review-policy.txt"),
            ("docs/agent-workflow/domain-review.md", "domain-policy.txt"),
        ):
            raw = (SOURCE / original).read_bytes()
            self.assertEqual((packet / artifact).read_bytes(), raw)
            covered = {
                number
                for item in inventory
                if item["artifact"] == artifact
                for number in range(item["start_line"], item["end_line"] + 1)
            }
            self.assertEqual(covered, set(range(1, len(raw.splitlines()) + 1)))

        def mutations_refuse(validate):
            # Recompute from current bytes, including an available document
            # outside this unit's required ranges; no memoized qualification.
            validate()
            for artifact in ("review-policy.txt", by_path["docs/agent-workflow/PROVIDERS.md"]["snapshot"]):
                path = packet / artifact
                before = path.read_bytes()
                path.write_bytes(before + b"changed\n")
                try:
                    with self.assertRaises(workflow.WorkflowError):
                        validate()
                finally:
                    path.write_bytes(before)
                validate()

        mutations_refuse(lambda: review.verify_packet(directory))
        unit = review_batch.plan(directory)["units"][0]
        binding = review_navigation.materialize(packet, unit, 10000)
        mutations_refuse(lambda: review_navigation.validate(packet, unit, 10000, binding))
