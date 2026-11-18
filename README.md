# FLOW-DC

FLOW-DC turns URL manifests into verifiable image datasets. It combines asynchronous
HTTP transfer, per-host adaptive concurrency, original-row accounting and optional
TaskVine distribution. It is designed for dataset construction on workstations,
research clusters and heterogeneous academic resources.

**PAARC** is the default request controller. It combines a baseline-relative delay
budget with a normalized excess-delay rate to adapt per-host concurrency.
**PAARC-Ratio** is an alternative controller that regulates concurrency through
a delay ratio within the same acquisition pipeline. Their signals
are application-level response delays, not transport RTT or measured server queues.

## Start locally

Requires Linux and Python 3.12+. Install the core package:

```sh
python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install .
flowdc --help
python examples/local_demo.py
```

The demo uses a local HTTP server and writes a small verified dataset under
`build/demo-download`. It makes no public-host requests.

For a real manifest, provide a Parquet or CSV file containing a URL column:

```sh
flowdc download --input-path images.parquet --output-dir runs/images \
  --url-column url --label-column species --method paarc
flowdc verify --output-dir runs/images
```

Excel and XML readers are optional: `python -m pip install '.[formats]'`.
Inputs must fit in memory together with row metadata and acquisition buffers;
the current index is not an out-of-core datastore.

## Configuration and output

Use `flowdc download --config examples/download.json` after replacing the example
manifest’s placeholder URL with your own inputs. The local demo above supplies a
working loopback example. Explicit CLI options override JSON values. Unknown fields are rejected. Both controllers start at four requests,
with a minimum of two and a default per-host ceiling of 192. Change the ceiling with
`--c-max`; use `--method paarc-ratio` for the ratio controller.

Outputs include payloads and row metadata, an `outcome-index.json`, an overview,
an integrity journal under `.flowdc/`, and a verified uncompressed archive by default.
Duplicate URLs retain separate original-row identities. Invalid rows are recorded;
failed or interrupted work is never silently credited as successful output.

```sh
flowdc resume --output-dir runs/images
flowdc reconcile --output-dir runs/images
flowdc partition --input-path images.parquet --output-dir runs/partitions --groups 8
```

Resume verifies source/configuration identity and remaining attempt budgets before
sending requests. Reconciliation is offline. Existing output requires confirmation
before replacement; `--force` explicitly authorizes replacement. Earlier-version runs
require their original runtime for recovery; no automatic ownership migration occurs.
See [configuration](docs/configuration.md), [integrity](docs/integrity.md) and the
[interface migration guide](docs/migration.md).

## Distributed execution

The TaskVine backend runs the same packaged downloader on independent workers,
splits an explicit aggregate request budget, returns archives and verifies their
original-row accounting on the manager. It stages one source artifact per revision.

```sh
flowdc download --config examples/download.json --backend taskvine \
  --taskvine-config examples/taskvine.json
```

The example requires paths to a qualified runtime. Native TaskVine is installed
separately; it is not a dependency of a local download. See the
[TaskVine guide](docs/distributed.md) for local workers, portable environments,
trusted remote workers and failure/cleanup semantics. Optional
[Jetstream2 operations](docs/jetstream2/README.md) use `flowdc ops` and retain
explicit resource enrollment and cumulative accounting.

## Experiments and paper

The [experiment guide](experiments/README.md) defines frozen input blocks, explicit
method configurations, first-attempt retention and independent verification.
img2dataset 1.47.0 runs in a separate pinned environment. Analytical and comparator
dependencies are deliberately isolated.

The [manuscript](paper/README.md) rebuilds from versioned sources and compact measured
inputs without private workspace files or image acquisition:

```sh
python -m pip install '.[research]'
make paper
```

TeX Live and Poppler are required. PDFs, standalone LaTeX, source ZIP and validation
receipts appear under `build/paper/`.

The retained results establish complete short-job CVDF output coverage and useful
TaiBIF acquisition with explicit byte and timing accounting. One matched TaiBIF block
reports 9.32/9.64 MB/s for PAARC-Ratio/PAARC. On the short CVDF comparison, img2dataset
is faster: 12.45 MB/s versus 10.91/10.70 MB/s. These observations support practical
acquisition and expose adaptation's startup cost; they do not establish universal
superiority, sustained high concurrency or general scaling. The
[packaged measurements](paper/data/measured.json) and supplement retain incomplete
outcomes and the limits of these conclusions.

## Find the implementation

| Location | Purpose |
|---|---|
| `src/flowdc` | Downloader, controllers, integrity, distributed execution and experiments |
| `src/flowdc/ops` | Optional operations and immutable service-release modules |
| `examples` | Small manifests, configurations and the local demo |
| `experiments` | Protocols and comparator environment |
| `paper` | Current manuscript and reproducibility inputs |
| `tests` | Acquisition, controller, experiment, native and fake-provider regressions |
| `docs` | Architecture and operating contracts |

Start with the [architecture](docs/architecture.md) and
[contributor guide](CONTRIBUTING.md). The existing [development workflow](docs/agent-workflow/OPERATING-GUIDE.md)
remains available. License: [Apache-2.0](LICENSE); third-party runtime material and
scientific datasets retain their own terms, described in [NOTICE](NOTICE).
