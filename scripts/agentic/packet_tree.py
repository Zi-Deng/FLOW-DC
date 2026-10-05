"""Fresh packet traversal without repeated ancestor reconstruction or status checks."""

import os
from pathlib import Path

from tasks import plain_path
from workflow import WorkflowError


def files(root, *, exclude_root_directories=(), exclude_root_files=()):
    """Enumerate all regular files, rejecting observed links and special entries.

    DirEntry uses the current directory scan's type information. No names, content
    or validation results survive this call. Callers still read/hash every file.
    This does not provide an atomic snapshot against concurrent owner writes.
    """
    root = plain_path(root)
    pending = [(str(root), "")]
    while pending:
        directory, prefix = pending.pop()
        with os.scandir(directory) as entries:
            for entry in entries:
                if entry.is_symlink():
                    raise WorkflowError("Packet tree contains a symlink")
                if entry.is_dir(follow_symlinks=False):
                    if prefix or entry.name not in exclude_root_directories:
                        pending.append((entry.path, prefix + entry.name + "/"))
                elif entry.is_file(follow_symlinks=False):
                    if prefix or entry.name not in exclude_root_files:
                        yield prefix + entry.name, Path(entry.path)
                else:
                    raise WorkflowError("Packet tree contains a nonregular entry")
