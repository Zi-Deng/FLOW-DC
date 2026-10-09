PYTHON ?= $(if $(wildcard .venv-agentic/bin/python),.venv-agentic/bin/python,python3)
RUFF ?= $(if $(wildcard .venv-agentic/bin/ruff),.venv-agentic/bin/ruff,ruff)

.PHONY: check test test-flowdc test-agentic check-agentic lint check-clean
check: test-flowdc check-agentic

check-agentic: lint test-agentic
	$(PYTHON) -B scripts/check_repository.py

lint:
	$(RUFF) check scripts/agentic scripts/check_repository.py tests/agentic
	$(RUFF) format --check scripts/agentic scripts/check_repository.py tests/agentic

test: test-flowdc test-agentic

test-flowdc:
	$(PYTHON) -B -m unittest discover -s tests -v

test-agentic:
	$(PYTHON) -B scripts/agentic/check.py

check-clean:
	git diff --check
	$(PYTHON) -c 'import subprocess; s=subprocess.check_output(["git","status","--porcelain"], text=True); print(s, end=""); raise SystemExit(bool(s))'
