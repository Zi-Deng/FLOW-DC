# Setup

Require Python3.12+, Git and GitHub CLI. Create `.venv-agentic` and install `requirements-dev.txt`. Run `make check` once. Install/login to Claude Code separately in the dedicated profile described in [PROVIDERS.md](PROVIDERS.md). Copilot is optional; switching is explicit.

`python3 scripts/agentic/install.py TARGET` copies the one current workflow into a new project and refuses existing files. Reconcile existing project configuration intentionally. Do not install old adapters, datasets, private memory or model output. Existing projects keep their own product tests and branch protection.
