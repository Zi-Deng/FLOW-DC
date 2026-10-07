"""Finite local catalog/window software fixtures; no readiness or native calls."""

import copy
import hashlib
import json
import subprocess
import tempfile
import unittest
from pathlib import Path

import review_batch_windows_v1 as windows
from tasks import digest
from workflow import WorkflowError


def catalog(count=2):
    rows = []
    for index in range(count + 1):
        cross = index == count
        rows.append(
            {
                "id": f"{index:024x}",
                "family": "integration" if cross else f"family-{index:02d}",
                "artifact": f"source/{index}.txt",
                "start_line": 1,
                "end_line": 1,
                "bytes": 2,
                "sha256": hashlib.sha256(b"x\n").hexdigest(),
                "kind": "cross-boundary" if cross else "source",
            }
        )
    ids = {i["id"] for i in rows}
    units = [
        {
            "id": i["family"],
            "required_ids": [i["id"]],
            "context_ids": windows.context_reference(ids, [i["id"]]),
        }
        for i in rows
    ]
    return {
        "binding": {"source": "a" * 64, "context": "b" * 64},
        "items": rows,
        "components": units[:-1],
        "integration": units[-1],
        "files": {i["artifact"]: i["sha256"] for i in rows},
    }


def children(plan, window):
    return [
        {
            "unit": unit,
            "binding": plan["catalog_digest"],
            "claim": digest([unit, "claim"]),
            "report": digest([unit, "report"]),
            "publication": digest([unit, "publication"]),
            "execution": digest([unit, "execution"]),
            "capture": digest([unit, "capture"]),
            "observer": digest([unit, "observer"]),
            "usage": {
                "status": "observed",
                "counters": {"duration_ms": 1000, "estimated_usd": 0.1},
                "models": {},
            },
        }
        for group in plan["schedule"]["windows"][: window + 1]
        for unit in group
    ]


class WindowTests(unittest.TestCase):
    def test_complete_maximum_schedule_and_smaller_whole_funding(self):
        plan = windows.plan_catalog(catalog(48))
        schedule = plan["schedule"]
        self.assertEqual(schedule["processes"], 49)
        self.assertEqual(schedule["reference_usd"], 490)
        self.assertEqual(schedule["active_seconds"], 94980)
        self.assertEqual(schedule["pause_seconds"], 16200)
        self.assertEqual(schedule["wall_seconds"], 111180)
        self.assertEqual(schedule["window_seconds"], [11700] + [10800] * 7 + [2100, 9180])
        self.assertEqual(windows.schedule(["one"])["processes"], 2)
        self.assertEqual(plan, windows.validate_plan(copy.deepcopy(plan)))
        for unit in plan["catalog"]["components"]:
            self.assertEqual(len(windows.surrounding_ids(plan["catalog"], unit)), 48)

    def test_missing_duplicate_wrong_owner_context_and_family(self):
        good = catalog()
        mutations = []
        value = copy.deepcopy(good)
        value["components"][0]["required_ids"] = []
        mutations.append(value)
        value = copy.deepcopy(good)
        value["components"][0]["required_ids"] += value["components"][1]["required_ids"]
        mutations.append(value)
        value = copy.deepcopy(good)
        value["components"][0]["context_ids"] = []
        mutations.append(value)
        value = copy.deepcopy(good)
        value["items"][1]["family"] = value["items"][0]["family"]
        mutations.append(value)
        value = copy.deepcopy(good)
        value["items"][0]["kind"] = "cross-boundary"
        mutations.append(value)
        value = copy.deepcopy(good)
        value["items"][0]["bytes"] = True
        mutations.append(value)
        value = copy.deepcopy(good)
        value["items"][0]["sha256"] = "unknown"
        mutations.append(value)
        for value in mutations:
            with self.subTest(value=value), self.assertRaises(WorkflowError):
                windows.validate_catalog(value)

    def test_bounds_types_funding_and_unfunded_suffix(self):
        for values in (
            [],
            ["b", "a"],
            ["a", "a"],
            ["integration"],
            [True],
            [f"u-{n:02d}" for n in range(49)],
        ):
            with self.subTest(values=values), self.assertRaises(WorkflowError):
                windows.schedule(values)
        original = windows.plan_catalog(catalog())
        for key, value in [
            ("processes", 1),
            ("reference_usd", 0),
            ("paid_extra_usd", True),
            ("window_seconds", [100]),
        ]:
            plan = copy.deepcopy(original)
            plan["schedule"][key] = value
            with self.subTest(key=key), self.assertRaises(WorkflowError):
                windows.validate_plan(plan)
        for size, lines in ((500001, 1), (2, 9001)):
            value = catalog()
            value["items"][0].update(bytes=size, end_line=lines)
            with self.assertRaises(WorkflowError):
                windows.validate_catalog(value)

    def test_known_complete_pause_and_exact_resume_replay(self):
        plan = windows.plan_catalog(catalog(7))
        application = windows.application(plan, 1000, 2000, qualification_finished=1500)
        records = children(plan, 0)
        pause = windows.seal(plan, application, 0, records, 2100)
        resumed = windows.resume(plan, application, pause, records, 2200)
        self.assertEqual(resumed["window"], 1)
        self.assertEqual(resumed["required_seconds"], 2100)
        self.assertEqual(pause["remaining_reference_usd"], 20)
        self.assertEqual(pause["children"], records)
        self.assertEqual(pause["remaining"], ["family-06", "integration", "final-validation"])
        for now in (2099, 3901, float("nan"), True):
            with self.subTest(now=now), self.assertRaises(WorkflowError):
                windows.resume(plan, application, pause, records, now)

    def test_unknown_partial_copied_and_changed_publication(self):
        plan = windows.plan_catalog(catalog(7))
        applied = windows.application(plan, 1000, 2000, qualification_finished=1500)
        records = children(plan, 0)
        paused = windows.seal(plan, applied, 0, records, 2100)
        for key, value in [
            ("publication", None),
            ("capture", "changed"),
            ("binding", "f" * 64),
            ("usage", {"status": "unknown"}),
        ]:
            changed = copy.deepcopy(records)
            changed[0][key] = value
            with self.subTest(key=key), self.assertRaises(WorkflowError):
                windows.seal(plan, applied, 0, changed, 2100)
        changed = copy.deepcopy(records)
        changed[0]["report"] = "f" * 64
        with self.assertRaises(WorkflowError):
            windows.resume(plan, applied, paused, changed, 2200)
        with self.assertRaises(WorkflowError):
            windows.seal(plan, applied, 0, records[:-1], 2100)
        changed = copy.deepcopy(paused)
        changed["remaining"] = []
        with self.assertRaises(WorkflowError):
            windows.resume(plan, applied, changed, records, 2200)

    def test_exclusive_and_torn_application_never_reused(self):
        plan = windows.plan_catalog(catalog())
        applied = windows.application(plan, 1000, 2000, qualification_finished=1500)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            windows.claim(root, plan, applied)
            self.assertEqual(windows.load(root), (plan, applied))
            with self.assertRaises(WorkflowError):
                windows.claim(root, plan, applied)
            before = (root / "windows-application.json").read_bytes()
            (root / "windows-plan.json").write_text("{")
            with self.assertRaises((WorkflowError, ValueError)):
                windows.load(root)
            self.assertEqual((root / "windows-application.json").read_bytes(), before)

    def test_git_material_ranges_hashes_and_projection_refusal(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            subprocess.run(["git", "init", "-q", str(root)], check=True)
            (root / "a.py").write_text("value = 1\n")
            subprocess.run(["git", "-C", str(root), "add", "a.py"], check=True)
            subprocess.run(
                [
                    "git",
                    "-C",
                    str(root),
                    "-c",
                    "user.name=Fixture",
                    "-c",
                    "user.email=fixture@example.invalid",
                    "commit",
                    "-qm",
                    "fixture",
                ],
                check=True,
            )
            raw = subprocess.check_output(["git", "-C", str(root), "show", "HEAD:a.py"])
            packet = root / "packet"
            packet.mkdir()
            (packet / "source.txt").write_bytes(raw)
            (packet / "cross.txt").write_text("integration\n")
            inventory = [
                {
                    "id": "1" * 24,
                    "path": "scripts/agentic/review.py",
                    "artifact": "source.txt",
                    "kind": "source",
                    "start_line": 1,
                    "end_line": 1,
                    "bytes": len(raw),
                },
                {
                    "id": "2" * 24,
                    "path": "cross",
                    "artifact": "cross.txt",
                    "kind": "cross-boundary",
                    "start_line": 1,
                    "end_line": 1,
                    "bytes": 12,
                },
            ]
            result = windows.partition(packet, inventory, {"source": digest(raw.decode())})
            self.assertEqual(result["components"][0]["id"], "review-storage")
            self.assertEqual(result["items"][0]["sha256"], hashlib.sha256(raw).hexdigest())
            inventory[0]["projection"] = {"schema_version": True}
            with self.assertRaises(WorkflowError):
                windows.partition(packet, inventory, {"source": "a" * 64})
            del inventory[0]["projection"]
            (packet / "source.txt").write_bytes(raw + b"changed\n")
            inventory[0]["bytes"] += 8
            with self.assertRaises(WorkflowError):
                windows.partition(packet, inventory, {"source": "a" * 64})

    def test_actual_runner_journals_and_fresh_external_ci_adapter(self):
        from types import SimpleNamespace
        from unittest.mock import patch

        import check_runner
        import ci_evidence
        import install

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "repository"
            root.mkdir()
            evidence = Path(temp) / "evidence"
            evidence.mkdir()
            testdir = root / "tests/agentic"
            testdir.mkdir(parents=True)
            (testdir / "test_tiny.py").write_text(
                "import unittest\nclass Tiny(unittest.TestCase):\n    def test_positive(self):\n        self.assertEqual(2 + 2, 4)\n"
            )
            for relative in install.PATHS:
                target = root / relative
                if relative in {
                    ".agentic",
                    ".agents/skills",
                    "scripts/agentic",
                    "tests/agentic",
                    "docs/agent-workflow",
                }:
                    target.mkdir(parents=True, exist_ok=True)
                elif not target.exists():
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_text("fixture\n")
            subprocess.run(["git", "init", "-q", str(root)], check=True)
            subprocess.run(["git", "-C", str(root), "add", "."], check=True)
            subprocess.run(
                [
                    "git",
                    "-C",
                    str(root),
                    "-c",
                    "user.name=Fixture",
                    "-c",
                    "user.email=fixture@example.invalid",
                    "commit",
                    "-qm",
                    "tiny",
                ],
                check=True,
            )
            head = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
            for name, jobs in [("serial", 1), ("parallel", 2)]:
                self.assertEqual(check_runner.run(root, jobs, output=evidence / name), 0)
            installed_root = evidence / "installed-root"
            install.install(root, installed_root, apply=True)
            import sys

            subprocess.run(
                [
                    sys.executable,
                    "-B",
                    "-c",
                    "import sys; from pathlib import Path; import check_runner; raise SystemExit(check_runner.run(Path(sys.argv[1]), 1, output=Path(sys.argv[2])))",
                    str(installed_root),
                    str(evidence / "installed"),
                ],
                check=True,
            )
            (evidence / "log.txt").write_text("Synthetic other-command receipt; no real final gates\n")
            source = check_runner.source(root)
            commands = {
                "serial": "python3 -B scripts/agentic/check.py --jobs 1",
                "parallel": "make check-agentic",
                "full": "make check",
                "clean": "make check-clean",
                "installed": "installed-full-suite",
                "lint": "ruff check",
                "format": "ruff format --check",
                "repository": "python3 -B scripts/check_repository.py",
            }
            value = {
                "head": head,
                "source": source,
                "commands": {
                    name: {
                        "command": command,
                        "exit_status": 0,
                        "artifacts": {
                            "log.txt": hashlib.sha256((evidence / "log.txt").read_bytes()).hexdigest()
                        },
                    }
                    for name, command in commands.items()
                },
                "installed_files": {
                    str(p): hashlib.sha256((root / p).read_bytes()).hexdigest() for p in install.payload(root)
                },
            }
            (evidence / "full-checks.json").write_text(json.dumps(value))
            receipts = [
                {
                    "check": name,
                    "state": "observed",
                    "run_attempt": 1,
                    "test_status": "success",
                    "clean_status": "success",
                    "pr_head_sha": head,
                    "pr_base_sha": head,
                    "tested_checkout_sha": "c" * 40,
                }
                for name in ("flowdc-tests", "agentic-quality")
            ]
            repo = SimpleNamespace(root=root, api=lambda *a, **k: [])
            meta = {"head_sha": head, "base_sha": head}
            with patch.object(ci_evidence, "collect", return_value=receipts):
                result = windows.full_checks(repo, evidence, meta)
                self.assertEqual(result["source"], digest(source))
                private = installed_root / "memory/private.txt"
                private.parent.mkdir()
                private.write_text("synthetic forbidden private payload")
                with self.assertRaisesRegex(WorkflowError, "extra/private"):
                    windows.full_checks(repo, evidence, meta)
                private.unlink()
                damaged = installed_root / "tests/agentic/test_tiny.py"
                original_installed = damaged.read_bytes()
                damaged.write_bytes(original_installed + b"# changed\n")
                with self.assertRaisesRegex(WorkflowError, "payload changed"):
                    windows.full_checks(repo, evidence, meta)
                damaged.write_bytes(original_installed)
                receipts[0]["run_attempt"] = 2
                with self.assertRaises(WorkflowError):
                    windows.full_checks(repo, evidence, meta)
                receipts[0]["run_attempt"] = 1
                request = evidence / "parallel/request.json"
                old = request.read_bytes()
                changed = json.loads(old)
                changed["rows"] = []
                request.write_text(json.dumps(changed))
                with self.assertRaises(WorkflowError):
                    windows.full_checks(repo, evidence, meta)
                request.write_bytes(old)
                (testdir / "test_tiny.py").write_text("changed\n")
                with self.assertRaises(WorkflowError):
                    windows.full_checks(repo, evidence, meta)

    def test_whole_integration_reports_dependencies_and_all_rows(self):
        import review_report_material_v1 as material
        from test_review_report_material_v1 import dependency, report

        plan = windows.plan_catalog(catalog())
        reports = {u["id"]: report(multiline=True) for u in plan["catalog"]["components"]}
        dependencies = {name: dependency(raw) for name, raw in reports.items()}
        result = windows.integration_reports(plan, reports, dependencies)
        for item in result["items"]:
            raw = result["files"][item["path"]]
            projected = result["files"][item["artifact"]]
            material.verify(raw, projected, item, dependencies[Path(item["path"]).stem])
            self.assertEqual(item["end_line"], 79)
            self.assertEqual(raw, reports[Path(item["path"]).stem])
            with self.assertRaises(ValueError):
                material.verify(
                    raw, projected.split(b"\n", 1)[1], item, dependencies[Path(item["path"]).stem]
                )
        for values in ({}, dict(reversed(list(reports.items())))):
            with self.assertRaises(WorkflowError):
                windows.integration_reports(plan, values, dependencies)
        changed = copy.deepcopy(dependencies)
        changed[next(iter(changed))]["review_sha256"] = "f" * 64
        with self.assertRaises(ValueError):
            windows.integration_reports(plan, reports, changed)
        with self.assertRaises(ValueError):
            windows.integration_reports(plan, reports, dependencies, existing_projection_bytes=2000000)

    def test_concurrent_exclusive_application_and_default_closed_prefix(self):
        from concurrent.futures import ThreadPoolExecutor

        plan = windows.plan_catalog(catalog())
        applied = windows.application(plan, 1000, 2000, qualification_finished=1500)
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)

            def attempt():
                try:
                    windows.claim(directory, plan, applied)
                    return True
                except WorkflowError:
                    return False

            with ThreadPoolExecutor(max_workers=2) as executor:
                results = list(executor.map(lambda _: attempt(), range(2)))
            self.assertEqual(sorted(results), [False, True])
            with self.assertRaisesRegex(WorkflowError, "prefix adapter"):
                windows.replay_prefix(None, directory, plan, 0)

    def test_durable_pause_resume_seals_and_lineage_failure(self):
        # Qualification/publication and owned auth are explicit doubles here.
        # These journal tests do not claim a real owned execution or readiness.
        from types import SimpleNamespace
        from unittest.mock import patch

        import claude_owned_auth
        from claude_fixtures import AUTHENTICATION

        plan = windows.plan_catalog(catalog(7))
        applied = windows.application(plan, 1000, 2000, qualification_finished=1500)
        calls = []
        owned = SimpleNamespace(
            current_binding=lambda *args: calls.append(args) or copy.deepcopy(AUTHENTICATION),
            capability_lineage=lambda *args: True,
        )
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            windows.claim(directory, plan, applied)
            with (
                patch.object(windows, "current_plan"),
                patch.object(windows, "replay_prefix", return_value=children(plan, 0)),
                patch.object(claude_owned_auth, "require", return_value=owned),
            ):
                first = windows.pause(None, directory, owned=owned, now=2100)
                with self.assertRaises(WorkflowError):
                    windows.pause(None, directory, owned=owned, now=2101)
                owned.capability_lineage = lambda *args: False
                with self.assertRaisesRegex(WorkflowError, "lineage"):
                    windows.resume_window(None, directory, owned=owned, now=2200)
                self.assertEqual(len(windows.journal(directory, plan, applied)), 1)
                owned.capability_lineage = lambda *args: True
                second = windows.resume_window(None, directory, owned=owned, now=2200)
                self.assertEqual(second["previous"], digest(first))
                self.assertIn((900, 1740), calls)
                with self.assertRaises(WorkflowError):
                    windows.resume_window(None, directory, owned=owned, now=2201)
                self.assertEqual(len(windows.journal(directory, plan, applied)), 2)
                for invalid_clock in (2199, 4000):
                    with self.assertRaisesRegex(WorkflowError, "clock rollback or allocation overrun"):
                        windows.pause(None, directory, owned=owned, now=invalid_clock)
                (directory / "window-transitions/02.json").write_text("{")
                with self.assertRaises((WorkflowError, ValueError)):
                    windows.journal(directory, plan, applied)

    def test_catalog_rebuilds_real_git_and_refuses_saved_context_or_source(self):
        from types import SimpleNamespace
        from unittest.mock import patch

        import reporting_activation_v6 as activation
        import review
        import review_packet
        import tasks
        import workflow
        from test_reporting_qualification_v6 import policy

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "repo"
            root.mkdir()
            directory = Path(temp) / "catalog"
            directory.mkdir()
            main = Path(temp) / "control"
            (main / ".agentic-local/tasks").mkdir(parents=True)
            (main / ".agentic-local/tasks/issue-31.json").write_text(
                json.dumps({"v6_catalog": str(directory)})
            )
            schema = Path(".agentic/schemas/review-report.json").read_bytes()
            for name, raw in {
                "AGENTS.md": b"Local policy\n",
                "docs/agent-workflow/REVIEW.md": b"Review all material\n",
                ".agentic/domain.md": b"No scientific validation\n",
                ".agentic/schemas/review-report.json": schema,
                "scripts/agentic/example.py": b"value = 1\n",
            }.items():
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(raw)
            subprocess.run(["git", "init", "-q", str(root)], check=True)

            def git(*args):
                return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()

            def commit():
                git("add", ".")
                git(
                    "-c",
                    "user.name=Fixture",
                    "-c",
                    "user.email=fixture@example.invalid",
                    "commit",
                    "-qm",
                    "fixture",
                )
                return git("rev-parse", "HEAD")

            base = commit()
            (root / "scripts/agentic/example.py").write_text("value = 2\n")
            head = commit()
            issue = {
                "number": 31,
                "url": "https://api.github.com/repos/example/test/issues/31",
                "state": "open",
                "title": "fixture",
                "body": "## Acceptance\nInspect the complete source.\n",
            }
            proposal = {
                "id": activation.CONTRACT["plan_comment"],
                "body": "Keep all obligations.\n",
                "issue_url": issue["url"],
            }
            pr = {"state": "open", "head": {"sha": head}, "base": {"sha": base}}
            context = {
                "issue": issue,
                "designated_plan_comment": proposal,
                "pull_request": pr,
                "reviews": [],
                "inline_comments": [],
                "pr_comments": [
                    {"id": 42, "body": "Original complete public response for scripts/agentic/example.py\n"}
                ],
                "issue_comments": [],
                "check_runs": [],
                "commit_statuses": [],
                "hosted_receipts": [],
            }
            endpoints = {
                "issues/31": issue,
                f"issues/comments/{proposal['id']}": proposal,
                "pulls/32": pr,
                "pulls/32/reviews": context["reviews"],
                "pulls/32/comments": context["inline_comments"],
                "issues/32/comments": context["pr_comments"],
                "issues/31/comments": context["issue_comments"],
            }
            repo = SimpleNamespace(
                root=root,
                main=main,
                name="example/test",
                git=git,
                api=lambda endpoint, **kwargs: copy.deepcopy(endpoints[endpoint]),
            )
            repo.pr = lambda number: copy.deepcopy(pr)
            cfg = {
                "max_source_file_bytes": 100000,
                "max_snapshot_bytes": 1000000,
                "domain_rubric": ".agentic/domain.md",
                "required_checks": ["flowdc-tests", "agentic-quality"],
            }
            packet = directory / "packet"
            packet.mkdir()
            head_index = review.snapshot(repo, head, packet / "source", cfg)
            base_index = review.snapshot(repo, base, packet / "base-source", cfg)
            for name, value in [
                ("source-index.json", head_index),
                ("base-source-index.json", base_index),
                ("context.json", context),
            ]:
                (packet / name).write_text(json.dumps(value))
            (packet / "diff.txt").write_text(
                subprocess.check_output(
                    [
                        "git",
                        "-C",
                        str(root),
                        "diff",
                        "--no-ext-diff",
                        "--no-textconv",
                        "--no-renames",
                        base,
                        head,
                    ],
                    text=True,
                )
            )
            for source, target in [
                ("AGENTS.md", "repository-policy.txt"),
                ("docs/agent-workflow/REVIEW.md", "review-policy.txt"),
                (cfg["domain_rubric"], "domain-policy.txt"),
                (".agentic/schemas/review-report.json", "report-schema.json"),
            ]:
                (packet / target).write_bytes((root / source).read_bytes())
            review_packet.build(
                repo, packet, head, base, head_index, base_index, context, cfg, provider="claude-code"
            )
            selected = policy()
            meta = {
                "schema_version": 7,
                "kind": "batch-parent",
                "issue": 31,
                "pr": 32,
                "repository": repo.name,
                "plan_comment": proposal["id"],
                "head_sha": head,
                "base_sha": base,
                "merge_base_sha": base,
                "review_policy": selected,
                "requested_model": selected["model"],
                "config": cfg,
            }

            def save_meta():
                meta["files"] = {
                    str(p.relative_to(packet)): hashlib.sha256(p.read_bytes()).hexdigest()
                    for p in packet.rglob("*")
                    if p.is_file()
                }
                (directory / "metadata.json").write_text(json.dumps(meta))

            save_meta()
            # Only external authority and final gate receipts are simulated in
            # this adapter fixture. Git snapshots, packet verification and complete
            # inventory regeneration remain real; full_checks has its own test.
            with (
                patch.object(activation, "authorization", return_value={}),
                patch.object(workflow, "configuration", return_value=cfg),
                patch.object(
                    activation, "CONTRACT_DIGEST", digest(tasks.issue_contract(repo, 31, proposal["id"]))
                ),
                patch.object(
                    windows,
                    "full_checks",
                    return_value={"local": "a" * 64, "hosted": "b" * 64, "source": "c" * 64},
                ),
            ):
                planned = windows.catalog(repo, plan_only=True)
                self.assertEqual(planned["schema_version"], 9)
                self.assertTrue(
                    any(i["artifact"].startswith("whole-responses/") for i in planned["catalog"]["items"])
                )
                self.assertEqual(
                    len(
                        [
                            i
                            for i in planned["catalog"]["items"]
                            if i["artifact"].startswith("final-guidance/")
                        ]
                    ),
                    4,
                )
                supplied = windows.catalog(repo)
                self.assertEqual(supplied["dependencies"]["assignments"], planned["catalog_digest"])
                required = packet / "required-material.json"
                original_required = required.read_bytes()
                value = json.loads(original_required)
                value["required"] = value["required"][1:]
                required.write_text(json.dumps(value))
                save_meta()
                with self.assertRaisesRegex(WorkflowError, "regenerated obligations"):
                    windows.catalog(repo)
                required.write_bytes(original_required)
                save_meta()
                changed = copy.deepcopy(context)
                changed["issue"]["body"] = "Changed contract"
                (packet / "context.json").write_text(json.dumps(changed))
                save_meta()
                with self.assertRaisesRegex(WorkflowError, "saved issue/plan"):
                    windows.catalog(repo)
                (packet / "context.json").write_text(json.dumps(context))
                save_meta()
                (root / "scripts/agentic/example.py").write_text("value = 3\n")
                with self.assertRaisesRegex(WorkflowError, "dirty or stale"):
                    windows.catalog(repo)

    def test_full_item_catalog_keeps_all_context_within_record_bound(self):
        from claude_reporting import _json_bytes

        value = catalog(48)
        for index, unit in enumerate(value["components"]):
            original = value["items"][index]
            for offset in range(34):
                item = {**original, "id": f"{100 + index * 34 + offset:024x}"}
                value["items"].append(item)
                unit["required_ids"].append(item["id"])
        ids = {i["id"] for i in value["items"]}
        for unit in value["components"] + [value["integration"]]:
            unit["context_ids"] = windows.context_reference(ids, unit["required_ids"])
        plan = windows.plan_catalog(value)
        self.assertEqual(len(ids), 1681)
        self.assertLess(len(_json_bytes(plan, 2000000)), 2000000)
        expanded = copy.deepcopy(plan)
        for unit in expanded["catalog"]["components"] + [expanded["catalog"]["integration"]]:
            context = sorted(ids - set(unit["required_ids"]))
            self.assertEqual(windows.surrounding_ids(value, unit), context)
            unit["context_ids"] = context
        # The unshared representation really exceeds the frozen serialization
        # bound; the fix preserves exactly the same sets without raising it.
        self.assertGreater(len(json.dumps(expanded, separators=(",", ":")).encode()), 2000000)
        with self.assertRaisesRegex(ValueError, "text_limit_or_type"):
            _json_bytes(expanded, 2000000)


class PreparationTests(unittest.TestCase):
    """Real packets/global claims with explicit external gate and owned-auth doubles."""

    def setUp(self):
        from contextlib import ExitStack
        from types import SimpleNamespace
        from unittest.mock import patch

        import claude_owned_auth
        import reporting_activation_v6 as activation
        import reporting_admission_v6 as admission
        import review
        from test_capacity_native_v1 import policy

        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.repo = SimpleNamespace(root=self.root, main=self.root, name="example/test")
        subprocess.run(["git", "init", "-q", str(self.root)], check=True)
        (self.root / "source.py").write_text("value = 1\n")
        subprocess.run(["git", "-C", str(self.root), "add", "."], check=True)
        subprocess.run(
            [
                "git",
                "-C",
                str(self.root),
                "-c",
                "user.name=Fixture",
                "-c",
                "user.email=fixture@example.invalid",
                "commit",
                "-qm",
                "fixture",
            ],
            check=True,
        )
        head = subprocess.check_output(["git", "-C", str(self.root), "rev-parse", "HEAD"], text=True).strip()
        self.packet = self.root / "export-source"
        self.packet.mkdir()
        schema = Path(".agentic/schemas/review-report.json").read_bytes()
        (self.packet / "report-schema.json").write_bytes(schema)
        (self.packet / "source.txt").write_bytes((self.root / "source.py").read_bytes())
        (self.packet / "cross.txt").write_bytes(b"Inspect interface\n")
        inventory = [
            {
                "id": "1" * 24,
                "path": "scripts/agentic/review.py",
                "kind": "source",
                "artifact": "source.txt",
                "start_line": 1,
                "end_line": 1,
                "bytes": 10,
            },
            {
                "id": "2" * 24,
                "path": "cross",
                "kind": "cross-boundary",
                "artifact": "cross.txt",
                "start_line": 1,
                "end_line": 1,
                "bytes": 18,
            },
        ]
        (self.packet / "required-material.json").write_text(
            json.dumps({"schema_version": 2, "required": inventory})
        )
        self.policy = policy()
        identity = {
            "repository": self.repo.name,
            "pr": 32,
            "issue": 31,
            "plan_comment": 6035844223,
            "head_sha": head,
            "base_sha": head,
            "merge_base_sha": head,
        }
        binding = {
            **{
                key: "b" * 64
                for key in ("local", "hosted", "source", "authorization", "context", "inventory")
            },
            "identity": digest(identity),
            "contract": "a" * 64,
            "policy": digest(self.policy),
        }
        self.plan = windows.plan_catalog(windows.partition(self.packet, inventory, binding))
        self.meta = {
            **identity,
            "schema_version": 7,
            "kind": "single",
            "requested_model": self.policy["model"],
            "review_policy": self.policy,
            "config": {},
            "files": windows.packet_hashes(self.packet),
        }
        self.grant = {"binding": {"harness": {"head": head}}}
        self.last = {"finished": 9500}
        self.evidence = {
            "schema_version": 6,
            "grant_digest": digest(self.grant),
            "binding_digest": "b" * 64,
            "outcomes": {str(n): digest(self.last) if n == 23 else "c" * 64 for n in range(20, 24)},
            "empirical_receipt": {"synthetic": "external qualification double; no actual readiness"},
            "authentication": self.policy["authentication"],
        }
        self.calls = []
        self.owned = SimpleNamespace(
            current_binding=lambda *args: self.calls.append(args) or self.policy["authentication"],
            recheck=lambda: None,
        )
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(patch("time.time", return_value=10000))
        self.stack.enter_context(patch("time.monotonic", return_value=500))
        self.stack.enter_context(patch.object(claude_owned_auth, "require", return_value=self.owned))
        self.admission = self.stack.enter_context(
            patch.object(admission, "check", side_effect=lambda *a, **k: copy.deepcopy(self.evidence))
        )
        self.stack.enter_context(
            patch.object(activation, "load", return_value=(self.grant, {"applied_at": 8000}))
        )
        self.stack.enter_context(patch.object(activation, "outcome", return_value=self.last))

        def export(repo, *, packet_target=None, plan_only=False):
            import shutil

            if packet_target is not None:
                shutil.copytree(self.packet, packet_target)
                return {"plan": copy.deepcopy(self.plan), "metadata": copy.deepcopy(self.meta)}
            return self.plan if plan_only else {"dependencies": {"assignments": self.plan["catalog_digest"]}}

        self.catalog = self.stack.enter_context(patch.object(windows, "catalog", side_effect=export))
        self.auth = {
            "name": "synthetic-full-review",
            "plan_digest": digest(self.plan),
            "policy_digest": digest(self.policy),
            "funding": copy.deepcopy(self.plan["schedule"]),
            "expires_at": 10000 + 129600,
        }
        self.target = self.root / "prepared"
        self.review = review

    def select(self, target=None):
        import review_batch

        return review_batch.select(
            target or self.target, None, self.auth, version=9, repo=self.repo, owned_auth=self.owned
        )

    def test_real_preparation_claims_load_and_assigned_surrounding_access(self):
        import review_batch
        import review_claims

        batch = self.select()
        self.assertEqual(review_batch.load(self.target), batch)
        self.assertIn((900, self.plan["schedule"]["window_seconds"][0] - 360), self.calls)
        unit = self.plan["catalog"]["components"][0]
        child = review_batch.prepare_unit(
            self.target, batch, unit, None, repo=self.repo, owned_auth=self.owned
        )
        meta = windows.verify_child(self.repo, child)
        self.assertEqual(meta["schema_version"], 7)
        self.assertEqual(meta["batch_unit"]["required_ids"], unit["required_ids"])
        self.assertEqual(meta["batch_unit"]["context_ids"], ["2" * 24])
        self.assertEqual((child / "packet/cross.txt").read_bytes(), b"Inspect interface\n")
        claims = windows.read(self.target / "batch-claims.json")
        for candidate in self.plan["catalog"]["components"] + [self.plan["catalog"]["integration"]]:
            review_claims.verify(self.repo, batch, candidate, claims[candidate["id"]])
        with self.assertRaisesRegex(WorkflowError, "owned runtime"):
            self.review.run_review(self.repo, child)
        with self.assertRaises(WorkflowError):
            windows.prepare_child(self.repo, self.target, unit["id"], owned_auth=self.owned)

    def test_scope_and_whole_funding_fail_before_admission(self):
        self.auth["funding"]["processes"] = 1
        with self.assertRaises(WorkflowError):
            self.select()
        self.admission.assert_not_called()
        self.assertFalse(self.target.exists())
        self.auth["funding"] = copy.deepcopy(self.plan["schedule"])
        self.catalog.side_effect = WorkflowError("actual source/gates/context changed")
        with self.assertRaisesRegex(WorkflowError, "source/gates/context"):
            self.select()
        self.admission.assert_not_called()

    def test_unqualified_receipt_and_full_window_failure_leave_no_claim(self):
        self.evidence["empirical_receipt"] = None
        with self.assertRaises(WorkflowError):
            self.select()
        self.assertFalse(self.target.exists())
        self.evidence["empirical_receipt"] = {"synthetic": True}

        def expired(*args):
            raise WorkflowError("full credential/paid receipt window unavailable")

        self.owned.current_binding = expired
        with self.assertRaisesRegex(WorkflowError, "receipt window"):
            self.select()
        self.assertFalse(self.target.exists())

    def test_competing_and_torn_claims_are_consumed_without_replacement(self):
        self.select()
        with self.assertRaises(WorkflowError):
            self.select()
        second = self.root / "renamed"
        with self.assertRaisesRegex(WorkflowError, "already claimed"):
            self.select(second)
        self.assertTrue((second / "windows-application.json").exists())
        with self.assertRaises(WorkflowError):
            self.select(second)
        (self.target / "batch-claims.json").write_text("{")
        with self.assertRaises((WorkflowError, ValueError)):
            windows.load_preparation(self.target)

    def test_child_tamper_and_missing_global_claim_are_not_storage_success(self):
        import review_claims

        self.select()
        unit = self.plan["catalog"]["components"][0]
        child = windows.prepare_child(self.repo, self.target, unit["id"], owned_auth=self.owned)
        path = child / "packet/cross.txt"
        original = path.read_bytes()
        path.write_bytes(b"omitted context\n")
        with self.assertRaises(WorkflowError):
            windows.verify_child(self.repo, child)
        path.write_bytes(original)
        claims = windows.read(self.target / "batch-claims.json")
        key = claims[unit["id"]]["claim"]["keys"][0]
        (review_claims.root(self.repo) / key / "0001.json").write_text("{}")
        with self.assertRaises(WorkflowError):
            windows.verify_child(self.repo, child)

    def test_integration_and_changed_public_context_remain_closed(self):
        self.select()
        with self.assertRaisesRegex(WorkflowError, "current complete window"):
            windows.prepare_child(self.repo, self.target, "integration", owned_auth=self.owned)
        self.catalog.side_effect = WorkflowError("public findings/dispositions/context changed")
        with self.assertRaisesRegex(WorkflowError, "public findings"):
            windows.prepare_child(
                self.repo, self.target, self.plan["catalog"]["components"][0]["id"], owned_auth=self.owned
            )

    def test_independent_head_base_contract_owner_and_clock_mutations(self):
        self.select()
        path = self.target / "batch.json"
        metadata = self.target / "metadata.json"
        original, meta_original = path.read_bytes(), metadata.read_bytes()
        for key in ("head_sha", "base_sha", "merge_base_sha", "repository", "plan_comment"):
            changed = json.loads(original)
            meta = json.loads(meta_original)
            changed["binding"][key] = "f" * 40 if key.endswith("sha") else "wrong"
            meta[key] = changed["binding"][key]
            meta["batch_sha256"] = digest(changed)
            path.write_text(json.dumps(changed))
            metadata.write_text(json.dumps(meta))
            with self.subTest(key=key), self.assertRaises(WorkflowError):
                windows.load_preparation(self.target)
        path.write_bytes(original)
        metadata.write_bytes(meta_original)
        unit = self.plan["catalog"]["components"][0]
        child = windows.prepare_child(self.repo, self.target, unit["id"], owned_auth=self.owned)
        timing = child / "batch-preparation.json"
        saved = timing.read_bytes()
        for key, value in (
            ("started", 11000),
            ("finished", 12000),
            ("local_deadline", 12000),
            ("action_deadline", 999999),
            ("monotonic_seconds", -1),
            ("schema_version", True),
        ):
            record = json.loads(saved)
            record[key] = value
            timing.write_text(json.dumps(record))
            with self.subTest(key=key), self.assertRaises(WorkflowError):
                windows.verify_child(self.repo, child)
        timing.write_bytes(saved)
        assignment = child / "packet/assignment.json"
        changed = json.loads(assignment.read_bytes())
        changed["required_ids"] = ["2" * 24]
        assignment.write_text(json.dumps(changed))
        meta = json.loads((child / "metadata.json").read_bytes())
        meta["batch_unit"] = changed
        meta["files"]["assignment.json"] = self.review.digest(assignment)
        (child / "metadata.json").write_text(json.dumps(meta))
        with self.assertRaises(WorkflowError):
            windows.verify_child(self.repo, child)


class CatalogExportTests(unittest.TestCase):
    def test_actual_git_catalog_exports_every_bound_artifact_and_added_range(self):
        from unittest.mock import patch

        actual_catalog = windows.catalog
        exports = []
        # Reuse the cheap real-Git adapter scenario, including its source/context
        # mutations. Only authority/final external receipts are fixture doubles.
        with tempfile.TemporaryDirectory() as temp:

            def export_and_compare(repo, **kwargs):
                result = actual_catalog(repo, **kwargs)
                target = Path(temp) / str(len(exports))
                exported = actual_catalog(repo, packet_target=target)
                self.assertEqual(windows.packet_hashes(target), exported["plan"]["catalog"]["files"])
                inventory = json.loads((target / "required-material.json").read_bytes())["required"]
                self.assertEqual(
                    {row["id"] for row in inventory},
                    {row["id"] for row in exported["plan"]["catalog"]["items"]},
                )
                self.assertTrue(any(row["artifact"].startswith("whole-responses/") for row in inventory))
                self.assertEqual(sum(row["artifact"].startswith("final-guidance/") for row in inventory), 4)
                for row in inventory:
                    self.assertTrue((target / row["artifact"]).is_file())
                exports.append(target)
                return result

            with patch.object(windows, "catalog", side_effect=export_and_compare):
                WindowTests(
                    "test_catalog_rebuilds_real_git_and_refuses_saved_context_or_source"
                ).test_catalog_rebuilds_real_git_and_refuses_saved_context_or_source()
            self.assertGreaterEqual(len(exports), 2)


class PreparationRouteTests(unittest.TestCase):
    def test_legacy_defaults_and_exact_batch9_routes_do_not_alias(self):
        from unittest.mock import patch

        import review_batch
        import review_batch_v7

        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            (directory / "metadata.json").write_text(json.dumps({"schema_version": 7}))
            with patch.object(review_batch_v7, "plan", return_value="legacy") as old:
                self.assertEqual(review_batch.plan(directory), "legacy")
                old.assert_called_once_with(directory, version=7)
            with patch.object(review_batch_v7, "select", return_value="legacy") as old:
                self.assertEqual(review_batch.select(directory, {"old": True}), "legacy")
                old.assert_called_once_with(directory, {"old": True}, None)
            with patch.object(review_batch_v7, "load", return_value="legacy") as old:
                self.assertEqual(review_batch.load(directory), "legacy")
                old.assert_called_once_with(directory)
            with patch.object(review_batch_v7, "prepare_unit", return_value="legacy") as old:
                self.assertEqual(
                    review_batch.prepare_unit(directory, {"schema_version": 7}, {}, {}), "legacy"
                )
                old.assert_called_once_with(directory, {"schema_version": 7}, {}, {})
            for version in (True, 8, "9"):
                with self.subTest(version=version), self.assertRaises(WorkflowError):
                    review_batch.select(directory, None, {}, version=version, repo=object())
            (directory / "metadata.json").write_text(json.dumps({"schema_version": 7, "batch_version": 9}))
            with self.assertRaisesRegex(WorkflowError, "owned execution/recovery"):
                review_batch.execute(None, directory)

    def test_plan9_requires_actual_designation_and_repo_adapter(self):
        from types import SimpleNamespace
        from unittest.mock import patch

        import review_batch

        with tempfile.TemporaryDirectory() as temp:
            main = Path(temp)
            designated = main / "catalog"
            (main / ".agentic-local/tasks").mkdir(parents=True)
            (main / ".agentic-local/tasks/issue-31.json").write_text(
                json.dumps({"v6_catalog": str(designated)})
            )
            repo = SimpleNamespace(main=main)
            actual = windows.plan_catalog(catalog())
            with patch.object(windows, "catalog", return_value=actual) as adapter:
                self.assertEqual(review_batch.plan(designated, version=9, repo=repo), actual)
                adapter.assert_called_once_with(repo, plan_only=True)
                with self.assertRaises(WorkflowError):
                    review_batch.plan(main / "undeclared", version=9, repo=repo)
                with self.assertRaises(WorkflowError):
                    review_batch.plan(designated, version=9)
