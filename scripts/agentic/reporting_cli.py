#!/usr/bin/env python3
"""Explicit coordinator-only reporting activation operations; never automatic fan-out."""

import argparse
import json
import sys

import reporting_activation_v2 as activation
import reporting_admission
import reporting_diagnostic_v2 as reporting_diagnostic
from workflow import Repo, WorkflowError


def parser():
    value = argparse.ArgumentParser(description=__doc__)
    commands = value.add_subparsers(dest="command", required=True)
    preview = commands.add_parser("preview", help="Read-only exact finite grant preview")
    preview.add_argument("--policy", required=True, help="Exact authenticated schema-2 policy JSON")
    preview.add_argument("--name", required=True)
    preview.add_argument("--tested-head", required=True)
    preview.add_argument("--expires-at", required=True, type=float)
    apply = commands.add_parser("apply", help="Consume the single explicit prospective authorization")
    apply.add_argument("--preview", required=True)
    apply.add_argument("--preview-digest", required=True)
    for name in ("prepare", "run", "recover"):
        command = commands.add_parser(name)
        command.add_argument("--number", required=True, type=int, choices=(12, 13))
    legacy = commands.add_parser("recover-v1", help="Storage-only old 10/11 recovery; never retry")
    legacy.add_argument("--number", required=True, type=int, choices=(10, 11))
    status = commands.add_parser("status", help="Re-evaluate both actual purposes; no inference")
    status.add_argument("--policy", required=True)
    return value


def dispatch(repo, args):
    repo.assert_main()
    if args.command == "recover-v1":
        import reporting_diagnostic as legacy

        return legacy.recover(repo, number=args.number)
    if args.command == "preview":
        return activation.preview(
            repo,
            activation.read(args.policy),
            name=args.name,
            tested_head=args.tested_head,
            expires_at=args.expires_at,
        )
    if args.command == "apply":
        return activation.apply(repo, activation.read(args.preview), preview_digest=args.preview_digest)
    if args.command == "prepare":
        return {"directory": str(reporting_diagnostic.prepare(repo, number=args.number))}
    if args.command == "run":
        return reporting_diagnostic.run(repo, number=args.number)
    if args.command == "recover":
        return reporting_diagnostic.recover(repo, number=args.number)
    if args.command == "status":
        return reporting_admission.check(repo, activation.read(args.policy))
    raise WorkflowError("Unknown reporting operation")


def main():
    args = parser().parse_args()
    try:
        print(json.dumps(dispatch(Repo(), args), indent=2, ensure_ascii=False))
        return 0
    except (WorkflowError, OSError, ValueError, KeyError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
