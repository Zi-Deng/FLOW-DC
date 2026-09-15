"""Exercise destructive boundaries using real temporary Git repositories."""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / "scripts/agentic"))
import install  # noqa: E402
import review  # noqa: E402
import workflow  # noqa: E402


def git(path, *args):
    return workflow.run(["git", "-C", path, *args]).stdout.strip()


class GitFixture(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="agentic-test-")
        self.addCleanup(self.temp.cleanup)
        self.parent = Path(self.temp.name)
        self.root = self.parent / "project with spaces"
        self.root.mkdir()
        self.remote = self.parent / "origin.git"
        git(self.parent, "init", "--bare", "--initial-branch=trunk", self.remote)
        git(self.root, "init", "--initial-branch=trunk")
        git(self.root, "config", "user.email", "test@example.invalid")
        git(self.root, "config", "user.name", "Workflow Test")
        git(self.root, "config", "commit.gpgsign", "false")
        git(self.root, "config", "core.hooksPath", "/dev/null")
        (self.root / "code.py").write_text("value = 1\n")
        (self.root / ".gitignore").write_text("/memory/\n/.agentic-local/\n*.cache\n")
        for item in [".agentic", ".github/agents", "docs/agent-workflow"]:
            if (SOURCE / item).exists():
                shutil.copytree(SOURCE / item, self.root / item)
        shutil.copyfile(SOURCE / "AGENTS.md", self.root / "AGENTS.md")
        # Generic orchestration fixtures use one synthetic check. Production check
        # names are validated separately against the actual workflow definitions.
        config_path = self.root / ".agentic/config.json"
        config = json.loads(config_path.read_text())
        config["required_checks"] = ["quality"]
        config_path.write_text(json.dumps(config))
        # Minimal policy text keeps this fixture independent of documentation wording.
        for name in ["REVIEW.md", "domain-review.md"]:
            p = self.root / "docs/agent-workflow" / name
            p.parent.mkdir(parents=True, exist_ok=True)
            if not p.exists():
                p.write_text("Static review only.\n")
        git(self.root, "add", ".")
        git(self.root, "commit", "-m", "baseline")
        git(self.root, "remote", "add", "origin", self.remote)
        git(self.root, "push", "-u", "origin", "trunk")
        self.repo = workflow.Repo(self.root)
        self.repo._info = {
            "nameWithOwner": "example/project",
            "defaultBranchRef": {"name": "trunk"},
            "isPrivate": False,
        }
        self.base = git(self.root, "rev-parse", "HEAD")
        self.issue = {
            "state": "open",
            "title": "Correct value",
            "url": "https://api.github.com/repos/example/project/issues/12",
        }
        self.repo.api = self.api
        self.reviews = []
        self.posts = []
        self.pr_data = None
        self.environ = patch.dict(os.environ)
        self.environ.start()
        self.addCleanup(self.environ.stop)
        os.environ.pop("WT_ROOT", None)
        # These fixtures simulate coordinator operations even when their caller
        # is a managed executor. Dedicated tests explicitly restore the guard.
        os.environ.pop("AGENTIC_EXECUTOR_ROLE", None)

    def api(self, suffix, *, data=None, **kwargs):
        if data is not None:
            self.posts.append((suffix, data))
            return {"html_url": "https://github.com/example/project/pull/31#review-1"}
        if suffix == "issues/12":
            return self.issue
        if suffix == "issues/comments/1234":
            return {"issue_url": self.issue["url"], "body": "Approved plan"}
        if suffix == "pulls/31":
            return self.pr_data
        if suffix.endswith("/reviews"):
            return self.reviews
        return []

    def task(self):
        data = workflow.new_task(self.repo, "12", "correct-value")
        self.task_path = Path(data["worktree"])
        return data

    def commit_task(self):
        self.task()
        (self.task_path / "code.py").write_text("value = 2\n")
        git(self.task_path, "add", "code.py")
        git(self.task_path, "commit", "-m", "change")
        self.head = git(self.task_path, "rev-parse", "HEAD")
        git(self.task_path, "push", "origin", "HEAD:refs/pull/31/head")
        self.pr_data = {
            "state": "open",
            "merged": False,
            "draft": False,
            "mergeable": True,
            "head": {
                "sha": self.head,
                "ref": "issue-12-correct-value",
                "repo": {"full_name": self.repo.name},
            },
            "base": {"sha": self.base, "ref": "trunk"},
        }

    def merged(self):
        self.commit_task()
        git(self.root, "merge", "--squash", "issue-12-correct-value")
        git(self.root, "commit", "-m", "squash change")
        self.pr_data["merged"] = True
        self.pr_data["state"] = "closed"


class WorktreeTests(GitFixture):
    def test_new_task_creates_isolated_pushed_branch_from_non_main_default(self):
        result = self.task()
        self.assertEqual(git(self.root, "branch", "--show-current"), "trunk")
        self.assertEqual(git(self.task_path, "rev-parse", "HEAD"), self.base)
        self.assertEqual(
            git(self.task_path, "rev-parse", "--abbrev-ref", "@{u}"), "origin/issue-12-correct-value"
        )
        self.assertEqual(Path(result["worktree"]).parent, self.root.parent / (self.root.name + "-worktrees"))
        (self.task_path / "code.py").write_text("isolated\n")
        self.assertEqual((self.root / "code.py").read_text(), "value = 1\n")

    def test_dirty_main_is_preserved(self):
        (self.root / "code.py").write_text("unsaved work\n")
        with self.assertRaisesRegex(workflow.WorkflowError, "dirty"):
            self.task()
        self.assertEqual((self.root / "code.py").read_text(), "unsaved work\n")

    def test_closed_issue_and_pr_number_are_refused(self):
        for change in [{"state": "closed"}, {"state": "open", "pull_request": {}}]:
            self.issue.update(change)
            with self.assertRaisesRegex(workflow.WorkflowError, "open issue"):
                self.task()

    def test_path_traversal_and_shell_metacharacters_are_refused(self):
        for number, slug in [
            ("0", "thing"),
            ("-1", "thing"),
            ("12", "../x"),
            ("12", "x;touch-pwned"),
            ("12", "a/b"),
        ]:
            with self.assertRaises(workflow.WorkflowError):
                workflow.new_task(self.repo, number, slug)

    def test_duplicate_worktree_is_refused(self):
        self.task()
        with self.assertRaisesRegex(workflow.WorkflowError, "already exists"):
            self.task()

    def test_existing_local_branch_is_reused(self):
        git(self.root, "branch", "issue-12-correct-value")
        self.task()
        self.assertEqual(git(self.task_path, "rev-parse", "HEAD"), self.base)

    def test_existing_remote_branch_is_reused(self):
        git(self.root, "push", "origin", "trunk:issue-12-correct-value")
        self.task()
        self.assertEqual(git(self.task_path, "rev-parse", "HEAD"), self.base)

    def test_nested_worktree_root_is_refused(self):
        os.environ["WT_ROOT"] = str(self.root / "nested")
        with self.assertRaisesRegex(workflow.WorkflowError, "outside"):
            self.task()

    def test_cleanup_requires_merge(self):
        self.commit_task()
        with self.assertRaisesRegex(workflow.WorkflowError, "MERGED"):
            workflow.cleanup_task(self.repo, 31)
        self.assertTrue(self.task_path.exists())

    def test_squash_cleanup_removes_only_verified_branch_and_is_repeatable(self):
        self.merged()
        self.assertNotEqual(git(self.root, "rev-parse", "HEAD"), self.head)
        workflow.cleanup_task(self.repo, 31)
        self.assertFalse(self.task_path.exists())
        self.assertNotIn("issue-12-correct-value", git(self.root, "branch", "--list"))
        workflow.cleanup_task(self.repo, 31)

    def test_cleanup_preserves_post_merge_local_commits(self):
        self.merged()
        (self.task_path / "later.py").write_text("value = 3\n")
        git(self.task_path, "add", ".")
        git(self.task_path, "commit", "-m", "unmerged follow-up")
        with self.assertRaisesRegex(workflow.WorkflowError, "tip differs"):
            workflow.cleanup_task(self.repo, 31)
        self.assertTrue((self.task_path / "later.py").exists())

    def test_cleanup_preserves_ignored_data(self):
        self.merged()
        (self.task_path / "valuable.cache").write_text("experiment result")
        with self.assertRaisesRegex(workflow.WorkflowError, "ignored"):
            workflow.cleanup_task(self.repo, 31)
        self.assertTrue((self.task_path / "valuable.cache").exists())

    def test_cleanup_preserves_dirty_worktree(self):
        self.merged()
        (self.task_path / "code.py").write_text("unsaved")
        with self.assertRaisesRegex(workflow.WorkflowError, "changed"):
            workflow.cleanup_task(self.repo, 31)

    def test_cleanup_rejects_fork_and_wrong_base(self):
        self.merged()
        self.pr_data["head"]["repo"]["full_name"] = "outsider/project"
        with self.assertRaisesRegex(workflow.WorkflowError, "fork"):
            workflow.cleanup_task(self.repo, 31)
        self.pr_data["head"]["repo"]["full_name"] = self.repo.name
        self.pr_data["base"]["ref"] = "release"
        with self.assertRaisesRegex(workflow.WorkflowError, "default branch"):
            workflow.cleanup_task(self.repo, 31)

    def test_cleanup_rejects_moved_worktree(self):
        self.merged()
        git(self.root, "worktree", "move", self.task_path, self.parent / "unexpected")
        with self.assertRaisesRegex(workflow.WorkflowError, "unexpected"):
            workflow.cleanup_task(self.repo, 31)

    def test_main_operation_refuses_task_checkout(self):
        self.task()
        other = workflow.Repo(self.task_path)
        other._info = self.repo.info
        with self.assertRaisesRegex(workflow.WorkflowError, "main checkout"):
            other.assert_main()

    def test_ruleset_uses_discovered_branch_and_concrete_checks(self):
        data = workflow.ruleset(self.repo, ["quality"])
        self.assertEqual(data["conditions"]["ref_name"]["include"], ["refs/heads/trunk"])
        self.assertEqual(data["bypass_actors"], [])
        self.assertEqual(data["rules"][3]["parameters"]["required_approving_review_count"], 0)

    def test_merge_preflight_rejects_stale_head_without_merging(self):
        self.commit_task()
        with self.assertRaisesRegex(workflow.WorkflowError, "head changed"):
            workflow.merge_preflight(self.repo, 31, self.base)

    def test_merge_preflight_requires_recorded_review(self):
        self.commit_task()
        with self.assertRaisesRegex(workflow.WorkflowError, "No published review"):
            workflow.merge_preflight(self.repo, 31, self.head)

    def test_merge_preflight_rejects_no_checks_or_skipped_checks(self):
        self.commit_task()
        self.reviews = [{"commit_id": self.head, "state": "COMMENTED"}]
        original = workflow.run
        for checks in [[], [{"name": "quality", "bucket": "skipping", "state": "SKIPPED"}]]:

            def fake(args, checks=checks, **kwargs):
                if args[:3] == ["gh", "pr", "checks"]:
                    return subprocess.CompletedProcess(args, 0, json.dumps(checks), "")
                return original(args, **kwargs)

            with patch.object(workflow, "run", side_effect=fake), self.assertRaises(workflow.WorkflowError):
                workflow.merge_preflight(self.repo, 31, self.head)

    def test_merge_preflight_emits_pinned_command_only(self):
        self.commit_task()
        self.reviews = [{"commit_id": self.head, "state": "COMMENTED"}]
        original = workflow.run

        def fake(args, **kwargs):
            if args[:3] == ["gh", "pr", "checks"]:
                return subprocess.CompletedProcess(
                    args, 0, json.dumps([{"name": "quality", "bucket": "pass"}]), ""
                )
            return original(args, **kwargs)

        with patch.object(workflow, "run", side_effect=fake):
            result = workflow.merge_preflight(self.repo, 31, self.head)
        self.assertIn("--match-head-commit " + self.head, result["command"])
        self.assertFalse(self.pr_data["merged"])


class ReviewTests(GitFixture):
    def packet(self):
        self.commit_task()
        return review.prepare(self.repo, 31, 12, 1234)

    def test_packet_records_exact_diff_plan_and_source_mapping(self):
        directory = self.packet()
        meta = review.verify_packet(directory)
        self.assertEqual(meta["head_sha"], self.head)
        self.assertEqual(meta["requested_model"], workflow.configuration(self.root)["copilot_model"])
        self.assertIn("+value = 2", (directory / "packet/diff.txt").read_text())
        context = json.loads((directory / "packet/context.json").read_text())
        self.assertEqual(context["designated_plan_comment"]["body"], "Approved plan")
        index = json.loads((directory / "packet/source-index.json").read_text())
        source = next(i for i in index if i["path"] == "code.py")
        self.assertEqual((directory / "packet" / source["snapshot"]).read_text(), "value = 2\n")
        self.assertFalse((directory / "packet/source/AGENTS.md").exists())

    def test_changed_packet_is_rejected(self):
        directory = self.packet()
        (directory / "packet/diff.txt").write_text("tampered")
        with self.assertRaisesRegex(workflow.WorkflowError, "changed"):
            review.verify_packet(directory)

    def test_unlimited_default_accepts_diff_above_old_cap(self):
        self.commit_task()
        (self.task_path / "large.txt").write_text("é" * 160_000 + "\n", encoding="utf-8")
        git(self.task_path, "add", "large.txt")
        git(self.task_path, "commit", "-m", "large text fixture")
        self.pr_data["head"]["sha"] = git(self.task_path, "rev-parse", "HEAD")
        git(self.task_path, "push", "origin", "HEAD:refs/pull/31/head")
        directory = review.prepare(self.repo, 31, 12, 1234)
        self.assertGreater((directory / "packet/diff.txt").stat().st_size, 300_000)
        index = json.loads((directory / "packet/source-index.json").read_text())
        self.assertIn("omitted", next(item for item in index if item["path"] == "large.txt"))

    def test_opt_in_diff_cap_is_inclusive_and_counts_utf8_bytes(self):
        self.commit_task()
        (self.task_path / "code.py").write_text('value = "é"\n', encoding="utf-8")
        git(self.task_path, "commit", "-am", "unicode diff fixture")
        self.pr_data["head"]["sha"] = git(self.task_path, "rev-parse", "HEAD")
        git(self.task_path, "push", "origin", "HEAD:refs/pull/31/head")
        packet = review.prepare(self.repo, 31, 12, 1234)
        diff = (packet / "packet/diff.txt").read_text()
        byte_count = len(diff.encode("utf-8"))
        self.assertGreater(byte_count, len(diff))
        cfg = workflow.configuration(self.root)
        for cap, accepted in [(byte_count, True), (byte_count - 1, False)]:
            cfg["max_diff_bytes"] = cap
            with patch.object(review, "configuration", return_value=cfg):
                if accepted:
                    self.assertTrue(review.prepare(self.repo, 31, 12, 1234).is_dir())
                else:
                    with self.assertRaisesRegex(workflow.WorkflowError, "max_diff_bytes"):
                        review.prepare(self.repo, 31, 12, 1234)

    def test_unlimited_diff_still_rejects_empty_change(self):
        self.commit_task()
        self.pr_data["head"]["sha"] = self.base
        with self.assertRaisesRegex(workflow.WorkflowError, "Diff is empty"):
            review.prepare(self.repo, 31, 12, 1234)

    def test_symlinks_are_never_dereferenced(self):
        (self.root / "secret-link.py").symlink_to("/etc/passwd")
        git(self.root, "add", "secret-link.py")
        git(self.root, "commit", "-m", "link fixture")
        index = review.snapshot(
            self.repo,
            git(self.root, "rev-parse", "HEAD"),
            self.parent / "snapshot",
            workflow.configuration(self.root),
        )
        self.assertIn("symlink", next(i for i in index if i["path"] == "secret-link.py")["omitted"])

    def test_private_path_diff_is_rejected_before_packet_creation(self):
        self.commit_task()
        (self.task_path / "memory").mkdir()
        (self.task_path / "memory/private.md").write_text("private")
        git(self.task_path, "add", "-f", "memory/private.md")
        git(self.task_path, "commit", "-m", "private fixture")
        self.pr_data["head"]["sha"] = git(self.task_path, "rev-parse", "HEAD")
        git(self.task_path, "push", "origin", "HEAD:refs/pull/31/head")
        with self.assertRaisesRegex(workflow.WorkflowError, "private/data"):
            review.prepare(self.repo, 31, 12, 1234)

    def test_review_rejects_stale_head(self):
        directory = self.packet()
        self.pr_data["head"]["sha"] = "a" * 40
        with self.assertRaisesRegex(workflow.WorkflowError, "changed"):
            review.review(self.repo, directory)

    def test_publish_is_comment_bound_to_sha_and_idempotent(self):
        directory = self.packet()
        (directory / "review.md").write_text("No material findings supported.\n")
        meta = review.verify_packet(directory)
        meta["review_sha256"] = review.digest(directory / "review.md")
        workflow.write_json(directory / "metadata.json", meta)
        review.publish(self.repo, directory)
        body = self.posts[0][1]
        self.assertEqual(body["event"], "COMMENT")
        self.assertEqual(body["commit_id"], self.head)
        self.reviews.append({"body": body["body"], "html_url": "existing"})
        self.assertEqual(review.publish(self.repo, directory), {"existing_review": "existing"})
        self.assertEqual(len(self.posts), 1)

    def test_copilot_run_has_fresh_state_and_read_only_tool_allowlist(self):
        packet_config = workflow.configuration(self.root)
        with patch.object(review, "configuration", return_value=packet_config):
            directory = self.packet()
        requested_model = packet_config["copilot_model"]
        original = review.run
        observed = []

        def fake(args, **kwargs):
            if args[0] != "copilot":
                return original(args, **kwargs)
            if args[1] == "--help":
                return subprocess.CompletedProcess(
                    args,
                    0,
                    "--available-tools --no-custom-instructions --disable-builtin-mcps --no-remote-export --no-ask-user --usage-output-file --max-ai-credits",
                    "",
                )
            if args[1] == "--version":
                return subprocess.CompletedProcess(args, 0, "Copilot test double\n", "")
            observed.append((args, kwargs))
            self.assertFalse(Path(kwargs["cwd"]).is_relative_to(self.root))
            self.assertNotIn("GH_TOKEN", kwargs["env"])
            self.assertNotIn("COPILOT_PROVIDER_BASE_URL", kwargs["env"])
            settings = json.loads((Path(kwargs["env"]["COPILOT_HOME"]) / "settings.json").read_text())
            self.assertTrue(settings["disableAllHooks"])
            return subprocess.CompletedProcess(
                args, 0, "No material findings supported by this review.\n", ""
            )

        with (
            patch.dict(
                os.environ,
                {
                    "COPILOT_GITHUB_TOKEN": "fake-test-token",
                    "COPILOT_PROVIDER_BASE_URL": "https://invalid.test",
                },
            ),
            patch.object(review, "run", side_effect=fake),
            patch.object(
                review,
                "configuration",
                side_effect=AssertionError("Run must use the model and budgets frozen in the review packet"),
            ),
        ):
            report = review.review(self.repo, directory)
        self.assertTrue(report.exists())
        argv = observed[0][0]
        self.assertIn("--available-tools=view,grep,glob", argv)
        self.assertNotIn("--allow-all", argv)
        self.assertNotIn("--continue", argv)
        self.assertEqual(argv[argv.index("--model") + 1], requested_model)
        self.assertEqual(
            argv[argv.index("--max-ai-credits") + 1],
            str(packet_config["review_max_ai_credits"]),
        )
        self.assertIn(f"Requested model: `{requested_model}`", report.read_text())


class InstallerTests(unittest.TestCase):
    def test_parent_component_cannot_redirect_install_into_copied_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            copied_source = parent / "source"
            install.install(SOURCE, copied_source, True)
            manifest = copied_source / ".agentic/template-origin.json"
            manifest.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "source": "agentic-github-template",
                        "files": {"retained-fixture-record": "unchanged"},
                    }
                )
            )
            previous = manifest.read_bytes()
            sibling = parent / "outside"
            sibling.mkdir()
            target = sibling / ".." / copied_source.name
            self.assertFalse(target.is_relative_to(copied_source))
            for apply in (False, True):
                with self.subTest(apply=apply):
                    with self.assertRaisesRegex(workflow.WorkflowError, "components"):
                        install.install(copied_source, target, apply)
                    self.assertEqual(manifest.read_bytes(), previous)
                    self.assertEqual(list(sibling.iterdir()), [])

    def test_preview_writes_nothing_and_conflicts_abort_before_copy(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "new"
            result = install.install(SOURCE, target)
            self.assertFalse(result["applied"])
            self.assertFalse(target.exists())
            target.mkdir()
            (target / "AGENTS.md").write_text("existing policy\n")
            with self.assertRaisesRegex(workflow.WorkflowError, "before any writes"):
                install.install(SOURCE, target, True)
            self.assertFalse((target / ".agentic").exists())
            self.assertEqual((target / "AGENTS.md").read_text(), "existing policy\n")

    def test_install_is_repeatable_and_omits_private_memory(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "new"
            install.install(SOURCE, target, True)
            result = install.install(SOURCE, target, True)
            self.assertEqual(result["new_files"], [])
            self.assertFalse((target / "memory").exists())
            self.assertFalse((target / "README.md").exists())
            self.assertTrue((target / "scripts/agentic/workflow.py").exists())
            self.assertTrue((target / "scripts/finish-task.sh").exists())
            self.assertEqual(len(list((target / ".agents/skills").glob("*/SKILL.md"))), 8)
            self.assertFalse((target / ".agentic-local").exists())

    def test_installer_rejects_non_directory_ancestor_before_writes(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "new"
            target.mkdir()
            (target / "scripts").write_text("existing user file")
            with self.assertRaisesRegex(workflow.WorkflowError, "before any writes"):
                install.install(SOURCE, target, True)
            self.assertFalse((target / ".agentic").exists())
            self.assertEqual((target / "scripts").read_text(), "existing user file")

    def test_installer_preserves_unrecognized_origin_manifest(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "new"
            (target / ".agentic").mkdir(parents=True)
            manifest = target / ".agentic/template-origin.json"
            manifest.write_text('{"private_user_record": true}')
            with self.assertRaisesRegex(workflow.WorkflowError, "before any writes"):
                install.install(SOURCE, target, True)
            self.assertFalse((target / "scripts").exists())
            self.assertEqual(json.loads(manifest.read_text()), {"private_user_record": True})


if __name__ == "__main__":
    unittest.main()
