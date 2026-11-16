# Development

Use Linux and Python 3.12. Create a dedicated environment and install the project:

```sh
python -m pip install -e '.[research,dev]'
make check
```

Keep changes within clear subsystem interfaces. Configuration uses typed dataclasses,
CLI parsing contains no acquisition logic, and experimental results retain their
input identities and failure dispositions. Prefer focused behavioral tests to tests
that merely repeat implementation details.

The controller oracle contains 560 transitions frozen from the recorded v2 source.
Changing policy equations or priorities is a method change, not a naming refactor.
HTTP, interrupted publication, archive verification and output ownership tests are
necessary when changing acquisition behavior.

`make package` builds a wheel/sdist. Test the wheel from outside the checkout with
`scripts/check_package.py`. `make paper` builds current scientific artifacts;
`make -C paper reproduce` checks source-only reproduction and the source ZIP.

Set `FLOWDC_NATIVE_PYTHON`, `FLOWDC_NATIVE_WORKER` and `FLOWDC_NATIVE_RECEIPT` to a qualified TaskVine 7.17.2
installation for real local worker tests. These launch only owned local processes;
cloud resources require separate authorization. The operational suite uses fake
providers and temporary state. It does not change the installed supervisor.

Operational runtime modules preserve source-bound release identities and are excluded
from cosmetic formatting. Their existing tests contain immediately consumed loop
closures. New portable code is covered by Ruff lint and formatting.

Use the [issue/worktree workflow](docs/agent-workflow/OPERATING-GUIDE.md) and retain
its bounded independent review and human merge handoff. Private evidence and runtime
installation paths must stay outside commits and reviewer snapshots.
