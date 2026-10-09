"""Record the actual hosted checkout; no custom qualification/replay stack."""

import argparse
import os
import subprocess

from workflow import WorkflowError, write_json


def record(output, check, test_status, clean_status):
    if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted":
        raise WorkflowError("Execution receipt requires the GitHub-hosted CI boundary")
    value = {
        "repository": os.environ["GITHUB_REPOSITORY"],
        "check": check,
        "pr_head_sha": os.environ["REVIEW_HEAD_SHA"],
        "pr_base_sha": os.environ.get("REVIEW_BASE_SHA"),
        "tested_checkout_sha": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "run_id": int(os.environ["GITHUB_RUN_ID"]),
        "run_attempt": int(os.environ["GITHUB_RUN_ATTEMPT"]),
        "test_status": test_status,
        "clean_status": clean_status,
        "scope": "Executable software checks; no model or scientific validation.",
    }
    write_json(output, value)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--output", required=True)
    p.add_argument("--check", choices=["flowdc-tests", "agentic-quality"], required=True)
    p.add_argument("--test-status", required=True)
    p.add_argument("--clean-status", required=True)
    a = p.parse_args()
    record(a.output, a.check, a.test_status, a.clean_status)
