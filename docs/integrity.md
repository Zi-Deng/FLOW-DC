# Output integrity

Every original manifest position has a stable hash-derived row ID. Duplicate URLs
remain distinct rows. The denominator obeys
`original_rows = verified + failed + skipped + unattempted`.

The `.flowdc/` journal binds an output directory's device/inode, source manifest,
configuration and acquisition source fingerprints. Atomic JSON records, retained
staging and hard-link publication support recovery after process interruption.
They do not claim host-power-loss durability or distributed locking.

No-follow directory traversal prevents symlinks from redirecting writes. Payloads,
metadata and archives are checked against committed lengths, hashes and original
row identity. A same-content pre-existing file is still unowned. Output planning
rejects reserved names, aliases and directory-prefix collisions before HTTP.

`flowdc verify` reopens committed files and checks the published archive without
changing completion records. `flowdc reconcile` can recover eligible ready records
and republishes the row index/reports. `flowdc resume` reconciles first and sends
only eligible unresolved requests with remaining attempts. Local storage failures
are not congestion observations. Verified rows are not redownloaded.

Whole-output replacement requires confirmation or explicit `--force`; an active or
malformed managed journal is never an invitation to erase it. Do not move or copy a
run directory and expect its ownership binding to remain resumable. Historical
runs require their original runtime for recovery. Schema-2 historical output can be checked with `flowdc verify` in read-only mode;
this preserves its recorded field names and source identities. It cannot become
resumable under the package implementation. Archive membership verification is
also available independently of a relocated run journal.

The downloader's overview records its acquisition interval. Scientific goodput is
computed by the experiment runner over the complete job through independent
verification; the two timing boundaries must not be interchanged.
