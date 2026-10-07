"""Separate finite V6 CLI. Legacy CLI and all historical recovery stay unchanged."""

import argparse
import json
import sys

import claude_owned_auth
import reporting_activation_v6 as activation
import reporting_admission_v6 as admission
import reporting_diagnostic_v6 as diagnostic
from workflow import Repo, WorkflowError


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    sub = result.add_subparsers(dest="command", required=True)
    preview = sub.add_parser("preview")
    preview.add_argument("--policy", required=True)
    preview.add_argument("--name", required=True)
    preview.add_argument("--tested-head", required=True)
    apply = sub.add_parser("apply")
    apply.add_argument("--preview", required=True)
    apply.add_argument("--preview-digest", required=True)
    for operation in ("prepare", "run", "recover"):
        command = sub.add_parser(operation)
        command.add_argument("--number", type=int, choices=(20, 21, 22, 23), required=True)
    status = sub.add_parser("status")
    status.add_argument("--capacity", action="store_true")
    return result


def dispatch(repo, args):
    repo.assert_main()
    if args.command == "recover":
        return diagnostic.recover(repo, number=args.number)
    if args.command in {"prepare", "run"}:
        return getattr(diagnostic, args.command)(repo, number=args.number)
    # A production catalog/full-gate adapter is a prerequisite, not a CLI flag.
    diagnostic.catalog(repo)
    if args.command == "preview":
        policy = activation.read(args.policy)
    elif args.command == "apply":
        proposal = activation.read(args.preview)
        policy = proposal["grant"]["binding"]["policy"]
    else:
        policy = activation.load(repo)[0]["binding"]["policy"]
    with claude_owned_auth.snapshot(policy) as owned:
        if args.command == "preview":
            return activation.preview(
                repo, policy, name=args.name, tested_head=args.tested_head, owned_auth=owned
            )
        if args.command == "apply":
            return activation.apply(repo, proposal, preview_digest=args.preview_digest, owned_auth=owned)
        if args.command == "status":
            return admission.check(repo, owned_auth=owned, capacity_required=args.capacity)
    raise WorkflowError("Unknown V6 reporting command")


def main():
    try:
        value = dispatch(Repo(), parser().parse_args())
        print(json.dumps(str(value) if not isinstance(value, dict) else value, indent=2, sort_keys=True))
    except (WorkflowError, OSError, ValueError, KeyError) as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2) from None


if __name__ == "__main__":
    main()
