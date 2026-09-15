#!/usr/bin/env bash
# Human entrypoint. Agents prepare this command; they do not execute it.
set -euo pipefail
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
exec python3 "$script_dir/agentic/finish.py" "$@"
