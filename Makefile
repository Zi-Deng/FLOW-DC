PYTHON ?= python3
RUFF ?= $(PYTHON) -m ruff
.PHONY: check test test-flowdc test-agentic check-agentic lint check-clean paper package
check: lint test-flowdc test-agentic
	$(PYTHON) -B scripts/check_repository.py

test: test-flowdc
test-flowdc:
	$(PYTHON) -B -m unittest discover -s tests -t . -v

test-agentic:
	$(PYTHON) -B scripts/agentic/check.py

check-agentic: test-agentic
	$(RUFF) check scripts/agentic tests/agentic scripts/check_repository.py
	$(RUFF) format --check scripts/agentic tests/agentic scripts/check_repository.py
	$(PYTHON) -B scripts/check_repository.py

lint:
	$(RUFF) check src scripts tests paper/scripts
	$(RUFF) format --check src scripts tests paper/scripts

paper:
	$(MAKE) -C paper all PYTHON=$(abspath $(shell command -v $(PYTHON)))

package:
	$(PYTHON) -m build

check-clean:
	git diff --check
	$(PYTHON) -c 'import subprocess; s=subprocess.check_output(["git","status","--porcelain"],text=True); print(s,end=""); raise SystemExit(bool(s))'
