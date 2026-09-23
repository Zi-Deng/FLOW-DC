"""Frozen v1 connection guard from 32b6ad44 (before maintenance support).

Exercise the legacy schema exclusion without requiring Git history in shallow CI.
The request/read operations under test are unchanged; this is the version-sensitive
connection implementation that old clients use for every read and mutation.
"""

import sqlite3
from contextlib import contextmanager

import flowdc_ops as ops
from flowdc_pilot_journal import DB_NAME, failure, private_lock

JOURNAL_VERSION = 1


@contextmanager
def connection(self):
    with ops.private_directory(self.root) as parent, private_lock(parent, "journal-io.lock"):
        # Serialize inspection with every local SQLite connection, including
        # reads: rollback journals may be unlinked during a concurrent commit.
        # Do not relax any metadata checks or retry arbitrary unsafe paths.
        for name in (DB_NAME, DB_NAME + "-journal", DB_NAME + "-wal", DB_NAME + "-shm"):
            ops.inspect_child(parent, name, required=name == DB_NAME)
        # The descriptor anchors the private directory for SQLite's auxiliary files.
        connection = None
        try:
            connection = sqlite3.connect(
                f"file:/proc/self/fd/{parent}/{DB_NAME}?mode=rw", uri=True, timeout=2
            )
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("PRAGMA trusted_schema=OFF")
            if connection.execute("PRAGMA user_version").fetchone()[0] != JOURNAL_VERSION:
                raise failure("unsupported_journal")
            yield connection
        except sqlite3.Error:
            raise failure("journal_unavailable_or_corrupt") from None
        finally:
            if connection is not None:
                connection.close()
