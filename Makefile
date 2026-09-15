PYTHON ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
RUFF ?= .venv/bin/ruff

.PHONY: check test lint check-clean
check: lint test
	$(PYTHON) scripts/check_repository.py

lint:
	$(RUFF) check scripts/agentic scripts/check_repository.py tests/agentic
	$(RUFF) format --check scripts/agentic scripts/check_repository.py tests/agentic

test:
	$(PYTHON) -B scripts/agentic/check.py

check-clean:
	git diff --check
	$(PYTHON) -c 'import subprocess; s=subprocess.check_output(["git","status","--porcelain"], text=True); print(s, end=""); raise SystemExit(bool(s))'
