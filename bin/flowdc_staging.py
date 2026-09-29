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
SOURCE_PATHS = ("bin/TaskvineFLOWDC.py",) + tuple("bin/" + name for name in DOWNLOAD_FILES)
HISTORICAL_REQUIRED = frozenset(("bin/TaskvineFLOWDC.py", "bin/download_batch.py", "bin/single_download.py"))
