"""Declarative maintained acquisition dependency closure, also used by guest bundles."""

DOWNLOAD_FILES = (
    "download_batch.py",
    "single_download.py",
    "flowdc_integrity.py",
    "flowdc_methods.py",
    "flowdc_shared_state.py",
    "flowdc_shared.py",
    "flowdc_staging.py",
)
WORKER_FILES = DOWNLOAD_FILES + ("flowdc_vine_worker.py", "flowdc_vine_protocol.py")
SOURCE_PATHS = (
    ("bin/TaskvineFLOWDC.py", "bin/flowdc_vine.py", "bin/flowdc_vine_native.py")
    + tuple("bin/" + name for name in WORKER_FILES)
    + (
        "benchmark/__init__.py",
        "benchmark/core/__init__.py",
        "benchmark/core/truth.py",
        "benchmark/core/verifier.py",
        "benchmark/core/controlled_origin.py",
        "bin/flowdc_experiment_research.py",
        "bin/flowdc_experiment_data.py",
        "bin/flowdc_ops.py",
        "bin/flowdc_topology.py",
    )
)
HISTORICAL_REQUIRED = frozenset(("bin/TaskvineFLOWDC.py", "bin/download_batch.py", "bin/single_download.py"))
