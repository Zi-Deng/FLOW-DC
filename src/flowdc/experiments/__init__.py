"""Reproducible protocols and retained first-attempt analysis."""

import argparse
import json

from .common import read_json


def main(argv=None):
    parser = argparse.ArgumentParser(prog="flowdc experiment")
    sub = parser.add_subparsers(dest="command", required=True)
    prepare = sub.add_parser("prepare")
    prepare.add_argument("--protocol", required=True)
    prepare.add_argument("--input-path", required=True)
    prepare.add_argument("--output-dir", required=True)
    prepare.add_argument("--exclude-study", help="Exclude all URLs frozen in an earlier calibration study")
    run = sub.add_parser("run")
    run.add_argument("--study", required=True)
    run.add_argument("--taskvine-config")
    run.add_argument("--comparator-python")
    run.add_argument("--limit", type=int)
    for name in ["verify", "analyze", "select"]:
        command = sub.add_parser(name)
        command.add_argument("--study", required=True)
    args = parser.parse_args(argv)
    if args.command == "prepare":
        from .protocol import prepare

        result = prepare(args.protocol, args.input_path, args.output_dir, args.exclude_study)
    else:
        from . import runner

        if args.command == "run":
            result = runner.run(
                args.study,
                taskvine=read_json(args.taskvine_config) if args.taskvine_config else None,
                comparator_python=args.comparator_python,
                limit=args.limit,
            )
        else:
            result = getattr(runner, args.command)(args.study)
    print(json.dumps(result, indent=2, allow_nan=False))
    if args.command == "run":
        return 0 if all(r["status"] == "complete" for r in result) else 1
    return 0
