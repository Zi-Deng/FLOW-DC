# Architecture

FLOW-DC separates request admission from output credit. An HTTP response can inform
control only after its eligible body completes; it earns useful bytes only after
storage and independent output verification succeed.

```mermaid
flowchart LR
    A[URL manifest] --> B[Identity and partitioning]
    B --> C[Local runner or TaskVine workers]
    C --> D[Per-origin PAARC controller]
    D --> E[HTTP and Retry-After admission]
    E --> F[Atomic payload and metadata publication]
    F --> G[Row journal and verified archive]
    G --> H[Independent experiment verification]
```

| Module | Responsibility |
|---|---|
| `config`, `cli` | Typed defaults, strict configuration, CLI precedence |
| `manifests`, `partition`, `naming` | Original-row identity and deterministic output planning |
| `controllers`, `admission` | Pure ratio/gradient transitions, observation freshness and permits |
| `http`, `downloader` | Request timing, redirects, retries and bounded worker scheduling |
| `integrity`, `verification`, `reports` | No-follow storage, journals, reconciliation and byte accounting |
| `distributed` | Package staging, owned native workers and manager-side verification |
| `experiments` | Frozen protocols, isolated comparator, request observations and analysis |
| `ops` | Optional resource lifecycle and cumulative accounting |

PAARC-Grad and PAARC-Ratio correspond to the historical selectors
`gradient-candidate-v2` and `ratio-v2`. Their transition oracle records the original
v2 implementation revision. Renaming the public interface does not change their
priority rules. No other controller implementation is retained.

Each distributed worker owns a local controller. The configured aggregate ceiling
is divided across workers; there is no shared global adaptive authority. Returned
files do not inherit worker-local ownership for resume. The manager independently
checks row identities, archive membership, hashes and original metadata.

The operational release files are intentionally standalone: their content-bound
installation and historical release validation are different from portable package
installation. Importing the product does not install services or contact a provider.

Packaging follows PyPA’s [source layout](https://packaging.python.org/en/latest/discussions/src-layout-vs-flat-layout/)
and [project configuration](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/) guidance.
A wheel is installed and exercised outside the checkout to verify that packaging
includes the runtime files.
