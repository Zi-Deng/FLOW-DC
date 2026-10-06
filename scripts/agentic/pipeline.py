"""Coordinator PR publication, feedback and bounded independent review."""

from __future__ import annotations

import re
import subprocess

import review as independent
import review_batch
import review_policy
from tasks import (
    TaskStore,
    body_text,
    digest,
    operation_key,
    plain_path,
    publish_once,
    verify_contract,
    workspace,
)
from workflow import WorkflowError, configuration, positive, run, sha


def issue_for_pr(repo, number):
    pr = repo.pr(positive(number))
    head = pr["head"]
    if not head.get("repo") or head["repo"]["full_name"] != repo.name:
        raise WorkflowError("Managed tasks require a same-repository PR")
    match = re.fullmatch(r"issue-([1-9][0-9]*)-[a-z0-9]+(?:-[a-z0-9]+)*", head["ref"])
    if not match:
        raise WorkflowError("PR does not use an issue task branch")
    return positive(match[1])


def current_task_pr(repo, state, number=None):
    number = positive(number if number is not None else state.get("pr"))
    if state.get("pr") is not None and positive(state["pr"]) != number:
        raise WorkflowError("Task is already bound to another PR")
    pr = repo.pr(number)
    head = pr["head"]
    if (
        not head.get("repo")
        or head["repo"]["full_name"] != repo.name
        or head["ref"] != state["workspace"]["branch"]
        or pr["base"]["ref"] != repo.base
    ):
        raise WorkflowError("PR repository, branch or base differs from the registered task")
    if pr.get("state") != "open" or pr.get("merged"):
        raise WorkflowError("This operation requires an open, unmerged task PR")
    sha(head["sha"])
    sha(pr["base"]["sha"])
    return pr


def bind_state_pr(repo, state, number):
    pr = current_task_pr(repo, state, number)
    state["pr"] = positive(number)
    state["pr_url"] = pr.get("html_url", f"https://github.com/{repo.name}/pull/{number}")
    return pr


def bind_pr(repo, number, pr_number):
    number, pr_number = positive(number), positive(pr_number)
    store = TaskStore(repo)
    with store.locked(f"issue-{number}") as state:
        verify_contract(repo, state)
        workspace(repo, state)
        if issue_for_pr(repo, pr_number) != number:
            raise WorkflowError("PR issue branch differs from the requested task")
        pr = bind_state_pr(repo, state, pr_number)
        store.save(state)
        return {"pr": pr_number, "url": state["pr_url"], "head_sha": pr["head"]["sha"]}


def publish_pr(repo, number, title, body_file, retry_confirmed_absent=False):
    number = positive(number)
    text = body_text(body_file)
    if not title.strip() or len(title) > 256:
        raise WorkflowError("PR title must contain 1–256 characters")
    if not re.search(rf"(?im)^Fixes #{number}\s*$", text):
        raise WorkflowError(f"PR body requires its own line: Fixes #{number}")
    store = TaskStore(repo)
    with store.locked(f"issue-{number}") as state, repo.lock():
        verify_contract(repo, state)
        path = workspace(repo, state)
        if run(["git", "-C", path, "status", "--porcelain"]).stdout:
            raise WorkflowError("Commit intended task changes before publishing the PR")
        branch = state["workspace"]["branch"]
        listing = f"pulls?state=all&head={repo.name.split('/')[0]}:{branch}&per_page=100"
        if state.get("pr"):
            current_task_pr(repo, state)
        else:
            candidates = repo.api(listing, paginate=True)
            opened = [item for item in candidates if item["state"] == "open"]
            if len(opened) > 1:
                raise WorkflowError("Multiple open PRs match the task branch")
            if opened:
                bind_state_pr(repo, state, opened[0]["number"])
                store.save(state)
            elif candidates:
                raise WorkflowError("A closed PR already used this task branch; reconcile its state")
        head = sha(run(["git", "-C", path, "rev-parse", "HEAD"]).stdout.strip())
        repo.fetch(f"refs/heads/{repo.base}")
        base = sha(repo.git("rev-parse", "FETCH_HEAD"))
        if int(repo.git("rev-list", "--count", f"{base}..{head}")) == 0:
            raise WorkflowError("A draft PR requires a coherent commit ahead of its base")
        run(["git", "-C", path, "push", "-u", "origin", branch])
        observed = run(["git", "-C", path, "ls-remote", "--heads", "origin", f"refs/heads/{branch}"])
        if observed.stdout.split() != [head, f"refs/heads/{branch}"]:
            raise WorkflowError("Remote branch changed during publication")
        verify_contract(repo, state)
        state.pop("finish", None)
        if not state.get("pr"):
            result = publish_once(
                store,
                state,
                "pr-create",
                "pulls",
                listing,
                {"title": title, "body": text, "head": branch, "base": repo.base, "draft": True},
                retry_confirmed_absent=retry_confirmed_absent,
            )
            bind_state_pr(repo, state, result["number"])
            store.save(state)
        pr = current_task_pr(repo, state)
        if pr["head"]["sha"] != head:
            raise WorkflowError("GitHub has not confirmed the pushed head; retry publication")
        desired = {"title": title, "body": text}
        state["pr_update"] = {
            "digest": digest(desired),
            "head_sha": head,
            "phase": "inflight",
        }
        store.save(state)
        if any(pr.get(key) != value for key, value in desired.items()):
            try:
                repo.api(f"pulls/{state['pr']}", data=desired, method="PATCH")
            except (WorkflowError, OSError, ValueError, subprocess.SubprocessError):
                # PATCH targets a saved ID. Observe the actual outcome before retrying.
                observed_pr = current_task_pr(repo, state)
                if any(observed_pr.get(key) != value for key, value in desired.items()):
                    raise WorkflowError("PR update remains incomplete; retry the same publication") from None
        pr = current_task_pr(repo, state)
        if pr["head"]["sha"] != head or any(pr.get(key) != value for key, value in desired.items()):
            raise WorkflowError("PR changed during publication; reconcile and retry")
        state["pr_update"]["phase"] = "published"
        store.save(state)
        return {"pr": state["pr"], "url": state["pr_url"], "head_sha": head, "updated": True}


def collect_feedback(repo, pr_number):
    pr_number = positive(pr_number)
    return {
        "reviews": repo.api(f"pulls/{pr_number}/reviews?per_page=100", paginate=True),
        "inline": repo.api(f"pulls/{pr_number}/comments?per_page=100", paginate=True),
        "conversation": repo.api(f"issues/{pr_number}/comments?per_page=100", paginate=True),
    }


def relevant_records(feedback):
    """Assess published history, including old-head findings, without assuming defects."""
    result = {}
    for surface, prefix in (("reviews", "review"), ("inline", "inline")):
        for item in feedback[surface]:
            if surface == "reviews":
                relevant = item.get("state") != "PENDING"
            else:
                relevant = True
            if relevant:
                identifier = f"{prefix}:{positive(item['id'])}"
                if identifier in result:
                    raise WorkflowError("Feedback pagination returned a duplicate record")
                normalized = {
                    key: item.get(key)
                    for key in (
                        "id",
                        "body",
                        "commit_id",
                        "original_commit_id",
                        "state",
                        "path",
                        "line",
                        "original_line",
                        "updated_at",
                        "in_reply_to_id",
                    )
                }
                result[identifier] = {
                    "digest": digest(normalized),
                    "url": item.get("html_url"),
                    "record": item,
                }
    return result


def pr_digest(pr):
    return digest(
        {
            "title": pr.get("title"),
            "body": pr.get("body"),
            "head": pr["head"]["sha"],
            "base": pr["base"]["sha"],
            "head_ref": pr["head"]["ref"],
            "base_ref": pr["base"]["ref"],
        }
    )


def feedback(repo, number):
    number = positive(number)
    store = TaskStore(repo)
    with store.locked(f"issue-{number}") as state:
        verify_contract(repo, state)
        pr = current_task_pr(repo, state)
        contents = collect_feedback(repo, state["pr"])
        return {
            "pr": state["pr"],
            "head_sha": pr["head"]["sha"],
            "base_sha": pr["base"]["sha"],
            "records": relevant_records(contents),
            "feedback_digest": digest(contents),
            "pr_digest": pr_digest(pr),
            "feedback": contents,
        }


def respond(repo, number, key, body_file, retry_confirmed_absent=False):
    number = positive(number)
    operation_key(key)
    store = TaskStore(repo)
    with store.locked(f"issue-{number}") as state:
        verify_contract(repo, state)
        current_task_pr(repo, state)
        pr_number = state["pr"]
        return publish_once(
            store,
            state,
            "response-" + key,
            f"issues/{pr_number}/comments",
            f"issues/{pr_number}/comments?per_page=100",
            {"body": body_text(body_file)},
            retry_confirmed_absent=retry_confirmed_absent,
        )


def report_record(repo, state, round_record):
    directory = plain_path(round_record["directory"])
    allowed = plain_path(repo.main / ".agentic-local/reviews")
    if not directory.is_relative_to(allowed):
        raise WorkflowError("Review record is outside the canonical review directory")
    if not round_record.get("run_attempted"):
        raise WorkflowError("The designated report was not run by this pipeline")
    meta = independent.verify_packet(directory)
    expected = {
        "repository": repo.name,
        "pr": state["pr"],
        "issue": state["approval"]["issue"],
        "plan_comment": state["approval"]["plan_comment"],
        "head_sha": round_record["head_sha"],
        "base_sha": round_record["base_sha"],
        "requested_model": round_record.get("review_policy", {}).get("model", "claude-opus-5"),
    }
    if any(meta.get(key) != value for key, value in expected.items()):
        raise WorkflowError("Review metadata differs from the registered pipeline round")
    if round_record["contract_digest"] != digest(state["approval"]["contract"]):
        raise WorkflowError("Review used a superseded contract")
    if meta.get("schema_version") in {5, 6, 7} and (
        meta.get("review_policy") != round_record.get("review_policy")
        or digest(meta["review_policy"]) != round_record.get("review_policy_digest")
    ):
        raise WorkflowError("Review provider policy differs from the registered immutable round")
    if meta.get("kind") == "batch-parent":
        planned = review_batch.load(directory)
        if round_record.get("batch_sha256") != digest(planned) or round_record.get(
            "authorization_digest"
        ) != digest(planned["authorization"]):
            raise WorkflowError("Batch plan or authorization differs from the registered round")
    report = plain_path(directory / "review.md")
    if (
        not report.is_file()
        or (meta.get("kind") != "batch-parent" and not meta.get(independent.version_field(meta)))
        or independent.digest(report) != meta.get("review_sha256")
    ):
        raise WorkflowError("Pipeline review is incomplete or its report changed")
    return meta, independent.publication_body(directory)


def published_report(repo, state, round_record):
    meta, expected_body = report_record(repo, state, round_record)
    records = repo.api(f"pulls/{state['pr']}/reviews", paginate=True)
    matching = [
        item
        for item in records
        if item.get("body") == expected_body
        and item.get("commit_id") == meta["head_sha"]
        and item.get("state") == "COMMENTED"
    ]
    if len(matching) > 1:
        raise WorkflowError("Multiple published reviews match this pipeline round")
    if matching and meta.get("kind") == "batch-parent":
        review_batch.verify_unit_publications(repo, round_record["directory"], complete_only=False)
    return matching[0] if matching else None


def validate_designated(repo, state):
    verify_contract(repo, state)
    pr = current_task_pr(repo, state)
    designated = state.get("designated_review")
    if not designated:
        raise WorkflowError("No designated published pipeline review exists")
    if (
        designated["head_sha"] != pr["head"]["sha"]
        or designated["base_sha"] != pr["base"]["sha"]
        or designated["contract_digest"] != digest(state["approval"]["contract"])
    ):
        raise WorkflowError("Designated review is stale for the current head, base or contract")
    matching = [
        record
        for record in state.get("review_rounds", [])
        if record["directory"] == designated["directory"]
        and record.get("status") == "published"
        and record.get("run_attempted")
    ]
    if len(matching) != 1:
        raise WorkflowError("Designated review has no unique completed pipeline round")
    independent.qualification(designated["directory"], require=True)
    published = published_report(repo, state, matching[0])
    if not published or positive(published["id"]) != designated["review_id"]:
        raise WorkflowError("Designated GitHub review is missing or changed")
    independent.current_pr(repo, state["pr"], designated["head_sha"], designated["base_sha"])
    return {"pr": pr, "review": published, "designation": designated}


def review_task(
    repo,
    number,
    execute=False,
    publish=False,
    fresh=False,
    continue_reason=None,
    approved_continuation=False,
    retry_confirmed_absent=False,
    prior_review=None,
    batch=False,
    batch_limits=None,
    batch_resume=False,
    review_provider=None,
    review_model=None,
    review_effort=None,
    batch_authorization=None,
    reporting=None,
    batch_successor=None,
):
    if batch_successor is not None and not (
        batch and fresh and approved_continuation and (continue_reason or "").strip()
    ):
        raise WorkflowError("Prepared successor requires --batch --fresh and explicit reason/continuation")
    if batch_limits is not None and not batch:
        raise WorkflowError("Budget flags require explicit --batch selection")
    if batch_resume and not (batch and execute):
        raise WorkflowError("Batch resume requires --batch and --execute")
    number = positive(number)
    store = TaskStore(repo)
    with store.locked(f"issue-{number}") as state:
        contract = verify_contract(repo, state)
        pr = current_task_pr(repo, state)
        rounds = state.setdefault("review_rounds", [])
        binding = {
            "head_sha": pr["head"]["sha"],
            "base_sha": pr["base"]["sha"],
            "contract_digest": digest(contract),
        }
        previous = rounds[-1] if rounds else None
        same_head = previous and all(previous.get(k) == v for k, v in binding.items())
        overrides = any(
            value is not None for value in (review_provider, review_model, review_effort, reporting)
        )
        legacy_recovery = same_head and not fresh and not overrides and "review_policy" not in previous
        if (
            same_head
            and not fresh
            and not overrides
            and previous.get("run_attempted")
            and previous.get("review_policy")
        ):
            # Recovery and publication are bound to the attempted packet, even if
            # the operator subsequently changes the default or saved selection.
            selection = {"policy": review_policy.validate_policy(previous["review_policy"])}
        elif not legacy_recovery:
            selection = review_policy.resolve(
                repo,
                configuration(repo.root),
                review_provider=review_provider,
                review_model=review_model,
                review_effort=review_effort,
            )
            from claude_native_auth import bind

            selection["policy"] = bind(selection["policy"])
            from claude_reporting_policy import build
            from claude_reporting_policy import selection as reporting_selection

            reporting = reporting_selection(selection["policy"], configuration(repo.root), reporting)
            if reporting is not None:
                selection["policy"] = build(selection["policy"], **reporting)
        if not legacy_recovery:
            binding["review_policy_digest"] = digest(selection["policy"])
            if (
                same_head
                and not fresh
                and previous.get("review_policy_digest") != binding["review_policy_digest"]
            ):
                raise WorkflowError("Review provider/model/effort/budget changed; explicitly prepare --fresh")
        reuse = previous and not fresh and all(previous.get(k) == v for k, v in binding.items())
        record = previous if reuse else None
        needs_run = execute and (record is None or not record.get("run_attempted"))
        attempted = sum(bool(item.get("run_attempted")) for item in rounds)
        if (
            needs_run
            and attempted >= 1
            and (not approved_continuation or not (continue_reason or "").strip())
        ):
            raise WorkflowError("Further review requires a stated reason and explicit continuation")
        if approved_continuation and not (continue_reason or "").strip():
            raise WorkflowError("Explicit continuation requires a concrete reason")
        if record is None:
            if batch_successor is not None:
                import review_continuation

                directory = plain_path(batch_successor)
                proposal = review_continuation.validate(directory)
                if (
                    not same_head
                    or proposal is None
                    or plain_path(proposal["snapshot"]["ancestor"]) != plain_path(previous["directory"])
                ):
                    raise WorkflowError("Successor does not extend the exact designated stopped round")
                if independent.verify_packet(directory).get("kind") != "single":
                    raise WorkflowError("Managed successor must be unselected and unattempted")
            else:
                directory = independent.prepare(
                    repo,
                    state["pr"],
                    number,
                    state["approval"]["plan_comment"],
                    expected_head=binding["head_sha"],
                    prior_review=prior_review,
                    review_provider=review_provider,
                    review_model=review_model,
                    review_effort=review_effort,
                    reporting=reporting,
                )
            if independent.verify_packet(directory)["review_policy"] != selection["policy"]:
                raise WorkflowError("Review selection changed during preparation; prepare a fresh packet")
            record = {
                **binding,
                "review_policy": selection["policy"],
                "directory": str(directory),
                "status": "prepared",
                "run_attempted": False,
            }
            rounds.append(record)
            store.save(state)
        is_batch = independent.verify_packet(record["directory"]).get("kind") == "batch-parent"
        if batch and record["run_attempted"] and not is_batch:
            raise WorkflowError("Cannot switch an attempted single review into batch mode")
        if batch and is_batch and batch_limits is not None:
            review_batch.select(record["directory"], batch_limits, batch_authorization)
        if is_batch and not batch:
            raise WorkflowError("Existing batch requires explicit --batch selection")
        if batch and not record["run_attempted"] and execute:
            if batch_limits is None:
                raise WorkflowError("Explicit aggregate and unit budgets are required for batch execution")
            review_batch.select(record["directory"], batch_limits, batch_authorization)
        if batch_resume and not record["run_attempted"]:
            raise WorkflowError("There is no attempted batch to resume")
        if execute and not record["run_attempted"]:
            if batch:
                planned = review_batch.load(record["directory"])
                record.update(
                    batch_sha256=digest(planned),
                    authorization_digest=digest(planned["authorization"]),
                )
            record.update(
                status="running",
                run_attempted=True,
                continuation={
                    "approved": approved_continuation,
                    "reason": continue_reason,
                },
            )
            store.save(state)
            try:
                if batch:
                    review_batch.execute(repo, record["directory"])
                else:
                    independent.review(repo, record["directory"])
                report_record(repo, state, record)
            except BaseException:
                record["status"] = "incomplete"
                store.save(state)
                raise
            record["coverage_qualified"] = independent.coverage_ready(record["directory"])
            record["status"] = "reviewed" if record["coverage_qualified"] else "reviewed-incomplete"
            store.save(state)
        elif execute and record.get("run_attempted"):
            # Recover a completed report after interruption, but never rerun an
            # uncertain model invocation in this directory.
            if batch_resume:
                review_batch.execute(repo, record["directory"], resume=True)
            else:
                independent.recover_review(repo, record["directory"])
            report_record(repo, state, record)
            if record["status"] not in {"publishing", "published", "published-incomplete"}:
                record["coverage_qualified"] = independent.coverage_ready(record["directory"])
                record["status"] = "reviewed" if record["coverage_qualified"] else "reviewed-incomplete"
                store.save(state)
        if publish:
            report_record(repo, state, record)
            observed = published_report(repo, state, record)
            if not observed:
                if record["status"] == "publishing" and not retry_confirmed_absent:
                    raise WorkflowError("Review publication is ambiguous; investigate before retrying")
                record["status"] = "publishing"
                store.save(state)
                try:
                    independent.publish(repo, record["directory"])
                except (WorkflowError, OSError, ValueError, subprocess.SubprocessError):
                    observed = published_report(repo, state, record)
                    if not observed:
                        raise WorkflowError("Review publication remains incomplete") from None
                observed = published_report(repo, state, record)
            if not observed:
                raise WorkflowError("Published pipeline review is not yet observable")
            independent.current_pr(repo, state["pr"], binding["head_sha"], binding["base_sha"])
            qualified = independent.coverage_ready(record["directory"])
            record["coverage_qualified"] = qualified
            record["status"] = "published" if qualified else "published-incomplete"
            state.pop("designated_review", None)
            if qualified:
                state["designated_review"] = {
                    **binding,
                    "directory": record["directory"],
                    "review_id": positive(observed["id"]),
                    "url": observed["html_url"],
                }
            state.pop("finish", None)
            store.save(state)
        return {
            "batch_preview": review_batch.preview(record["directory"], batch_limits)
            if batch and not execute
            else None,
            "pr": state["pr"],
            "directory": record["directory"],
            "status": record["status"],
            "incomplete": record.get("coverage_qualified") is False,
            "coverage_qualified": record.get("coverage_qualified"),
            "model": record.get("review_policy", {}).get("model", "claude-opus-5"),
            "review_policy": record.get("review_policy"),
            "attempted_rounds": sum(bool(item.get("run_attempted")) for item in rounds),
            "designated_review": state.get("designated_review"),
        }


def add_commands(sub):
    bind = sub.add_parser("bind-pr")
    bind.add_argument("issue")
    bind.add_argument("pr")
    publication = sub.add_parser("publish-pr")
    publication.add_argument("issue")
    publication.add_argument("--title", required=True)
    publication.add_argument("--body-file", required=True)
    publication.add_argument("--retry-confirmed-absent", action="store_true")
    feedback_parser = sub.add_parser("feedback")
    feedback_parser.add_argument("issue")
    response = sub.add_parser("respond")
    response.add_argument("issue")
    response.add_argument("--key", required=True)
    response.add_argument("--body-file", required=True)
    response.add_argument("--retry-confirmed-absent", action="store_true")
    review_parser = sub.add_parser("task-review")
    review_parser.add_argument("issue")
    for flag in ("execute", "publish", "fresh", "approved-continuation", "retry-confirmed-absent"):
        review_parser.add_argument("--" + flag, action="store_true")
    review_parser.add_argument("--continue-reason")
    review_parser.add_argument("--prior-review")
    review_parser.add_argument(
        "--batch", action="store_true", help="Preview or explicitly execute bounded review units"
    )
    review_parser.add_argument(
        "--batch-resume", action="store_true", help="Resume only never-started eligible units"
    )
    for name, kind in (
        ("requests", int),
        ("kind", str),
        ("cost", str),
        ("seconds", int),
        ("unit-cost", str),
        ("unit-seconds", int),
        ("max-report-bytes", int),
        ("max-integration-bytes", int),
    ):
        review_parser.add_argument("--batch-" + name, type=kind)
    review_parser.add_argument(
        "--batch-authorization", help="Named authorization JSON bound to final executable preview"
    )
    review_policy.add_arguments(review_parser)
    review_parser.add_argument(
        "--batch-successor", help="Prepared exact same-revision continuation directory"
    )
    review_parser.add_argument("--reporting-policy", help="JSON with explicit max_turns and retention limits")


def dispatch(repo, args):
    if args.command == "bind-pr":
        return bind_pr(repo, args.issue, args.pr)
    if args.command == "publish-pr":
        return publish_pr(repo, args.issue, args.title, args.body_file, args.retry_confirmed_absent)
    if args.command == "feedback":
        return feedback(repo, args.issue)
    if args.command == "respond":
        return respond(repo, args.issue, args.key, args.body_file, args.retry_confirmed_absent)
    if args.command == "task-review":
        from claude_reporting_policy import read_selection

        return review_task(
            repo,
            args.issue,
            args.execute,
            args.publish,
            args.fresh,
            args.continue_reason,
            args.approved_continuation,
            args.retry_confirmed_absent,
            args.prior_review,
            args.batch,
            review_batch.arguments_budget(args)
            if any(review_batch.arguments_budget(args).values())
            else None,
            args.batch_resume,
            args.review_provider,
            args.review_model,
            args.review_effort,
            independent.coverage.read_json(plain_path(args.batch_authorization))
            if args.batch_authorization
            else None,
            read_selection(args.reporting_policy),
            args.batch_successor,
        )
    raise WorkflowError("Unknown pipeline operation")
