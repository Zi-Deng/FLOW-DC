# Output integrity and reconciliation (schema 2)

This contract applies to `download_batch.py` and `download_batch_gradient.py`.
They share one acquisition/publication/reconciliation runner. The gradient controller
equations and tuning are unchanged. The separate multithread/GBIF helper and simulated
UI retain their existing behavior; they do not produce schema-2 recovery evidence.

## Capacity boundary

The current implementation supports in-memory manifests: the input bytes, Polars
frames, Python row/provenance objects, parsed ownership JSON and reconciled index must
fit in available process memory together with acquisition buffers. Stamping and
validation visit every original row; reopening and reconciliation repeat full-index
work. Partitioning also stamps the whole source before splitting it. Row count alone
is not a sufficient capacity bound because original metadata is retained in full.
This increment has no validated numeric maximum or 40M-row throughput/memory claim.
The historical partitioner scale description does not certify the schema-2 path.
It is not an out-of-core index; avoid inputs whose full representation cannot fit.
The Polars clone used to retain original columns is a cheap shared-data clone, but
the Python row materialization and JSON indexes do allocate whole-manifest objects.

## Identity and row outcomes

Before URL validation or ordering, the loader hashes the original manifest bytes and
assigns each original position an ID: SHA-256 of canonical JSON `[manifest_sha256,
position]` plus newline. Duplicate URLs and identical content remain separate requested
rows. An input `__key__` is retained as `__flowdc_external_key__`; ordinary identifier
columns remain original metadata. Null, blank and invalid URLs are skipped, including
all-invalid and empty manifests. Unsafe output names are failed rows.

Prepared partitions carry `__flowdc_manifest__`, `__flowdc_position__`,
`__flowdc_source_rows__`, `__flowdc_row_id__` and `__flowdc_row_digest__`. Partial
provenance, inconsistent positions/counts/digests, duplicate IDs and conflicting internal
keys fail before destructive output handling. These hashes detect inconsistency; they
are not signatures authenticating a supplied manifest. The run also records the exact
partition file SHA-256. `SplitParquet.py` stamps before grouping and preserves original
columns; its optional derived host column is `__flowdc_partition_host__`. All three
grouping modes retain invalid rows. Empty input emits an empty partition.

The index enforces `original_rows == verified + failed + skipped + unattempted`.
Each row appears once. Attempts live separately under that row ID and consume the
original `max_retry_attempts` budget (including the first attempt). An intent or an
interrupted attempt directory counts as started work, with uncertain execution where
necessary; it never becomes an invented zero-attempt/unattempted outcome. A rejection
before an attempt, such as an unowned destination, is explicitly failed with zero
intents. Only no-start evidence permits `unattempted`.
Recorded rejections are terminal: removing the original conflict does not erase the
rejection or make the row retryable. Recorded local/unknown attempt failures are also
terminal even if their physical cause was transient. Preserve the old run and its
verified rows; reacquire with the original input/configuration in a new absent output
directory, without recovery/overwrite flags. That creates a distinct run and may
redownload previously verified rows. It requires neither deleting the old run nor
manually editing journals. No in-place reopening of terminal failures is supported.
If attempt-directory or intent publication fails without a durable row rejection,
acquisition aborts before HTTP and leaves the final marker incomplete. There is no
in-memory retry of an unchanged persisted budget. An interrupted attempt directory
still consumes one attempt and reconciles as failed with uncertain execution;
failure before that directory exists cannot have dispatched HTTP. Repair storage
before explicitly resuming; failure to record evidence is never a complete run.

## Containment and publication

`row_id` filenames use the immutable ID and original URL suffix. Sequential and URL
pattern interfaces remain available. Before HTTP, planning detects payload/metadata
aliases, directory-prefix conflicts and reserved `.flowdc`, `overview.json`, and
`outcome-index.json` destinations. All affected rows fail on a legacy naming collision.
Labels and path components reject dot/dot-dot, absolute paths, encoded separators and
unsafe components. Directory-descriptor traversal uses no-follow filesystem checks;
symlinks cannot redirect publication. A same-content pre-existing file is still unowned.
This includes every ancestor of the absolute output path, not only destinations
inside it. A symlinked home/scratch/mount alias must be replaced with its real path
in the configuration. The no-follow open error names the offending component.

Directories created through the integrity layer (including new ancestors, class
directories and staging) use `0700`; the maintained entrypoint's output root itself
uses ordinary umask-derived mode. Staged and published files, sidecars and exports
use `0600` (further restricted by umask). Archive members use `0600`.
This owner-only artifact policy differs from the
former umask-derived shared access. Existing ancestor permissions are unchanged.
Same-user readers can consume the output; cross-user/group storage and workers need
a separately arranged export/access policy. No automatic permission expansion occurs.

Each run owns `.flowdc/owner.json`, bound to the output directory device/inode, a random
run ID, original manifest/rows, effective configuration and runtime source SHA-256s.
An exclusive advisory lock prevents cooperating concurrent writers. Per-row publication:

1. Create the numbered attempt directory and atomically publish its intent.
2. Exclusively write/close staged payload and metadata on the output filesystem.
3. Reopen and verify lengths, SHA-256s, row identity and original metadata; publish the
   `ready.json` evidence.
4. Hard-link each verified component to its contained destination without replacement.
5. Write the row completion record last.

Staged files remain as ownership evidence. Recovery accepts an already-published
component only if it is the same owned staged inode and its digest/length still match.
Atomic JSON records may leave recognizable `.writing-*` tails; they are not parsed as
completed records. A valid ready record can finish publication after interruption.
Malformed evidence, changed components or conflicting destinations remain failures
with files preserved. Recorded terminal local failures are not silently recovered into
success without an already valid commit. A transient lookup error after commitment
can produce a failed in-memory attempt and failure record; reconciliation independently
reopens and verifies the committed components and may then mark the row verified.
Attempt observations remain distinct from the final row disposition and are retained.
Ordinary transient HTTP failures retain retry eligibility.

This is logical commitment recoverable from process interruption on a local POSIX
filesystem supporting hard links and advisory locks. It is not two-file atomic rename,
host-power-loss durability, a distributed lock, or protection against another process
deliberately modifying owned inodes during validation. There is no automatic staging
garbage collection. Preserve the complete run directory; moving/copying it changes the
ownership boundary and is not supported for resume.

## Offline reconciliation and explicit resume

`--output DIR --reconcile` opens only existing owned state, verifies or recovers eligible
publications, and writes a deterministic `outcome-index.json`. It does not need the input
file, instantiate an HTTP session or redownload a row. Reconciliation invalidates any
previous final completion marker before revalidating the result. Repeated reconciliation
does not duplicate rows, byte credit or attempts.
The invoking base/gradient variant must match recorded ownership. A cross-entrypoint
request fails before reconciliation invalidates the final marker or changes reports;
use the original entrypoint even though offline mode needs no HTTP configuration.

`--resume` reconciles first, then dispatches only eligible unresolved rows with remaining
attempt budget. Verified rows are not redownloaded. Original manifest bytes, validated
parent provenance, effective acquisition configuration, controller variant and runtime
source hashes must match. Mismatch fails closed without deleting output. Thus resuming
after a source/config change is intentionally rejected; offline inspection remains
available. A malformed ownership record is not an invitation to overwrite or guess.

Gradient controller counters are not persisted across invocations. Resume reports
therefore set `gradient_summary=null` and
`gradient_summary_scope="unavailable_across_resume"`, for both zero-work and partial
resumes. They do not present the new controller set as whole-run measurements.
Offline reconciliation carries forward a matching run's prior summary and scope;
it does not make new controller observations. Earlier report versions remain in the
owned export history.

`--force`/`force_overwrite` and interactive overwrite consent retain their previous
meaning for the selected output directory, after input and ownership checks. They
cannot be combined with recovery. A consented replacement removes only verified owned
external exports; unowned external archive/report collisions fail and are preserved.
No automatic adoption of historical folders or journals from other schema versions
occurs. Keep schema-1 outputs and their historical interpretation intact.

## Result boundaries, archives and reports

`--research_profile` selects original decoded downloaded payload bytes, per-row metadata,
row-ID filenames, an uncompressed WebDataset archive and overviews. Each invocation
produces one archive/shard; prepared partitions produce one each. Useful final research
output requires the closed verified archive and reconciled outcome index. Compression
and resizing of image content are not introduced. Ordinary WebDataset tar mode uses
the archive boundary; ImageFolder and `--no_tar` use the labeled `committed_local_files`
boundary. Optional archives can still be produced for ImageFolder.
ImageFolder consumers must exclude `.flowdc/` from class-directory discovery and
select payloads rather than JSON sidecars. The standalone legacy save helper does
not create that run directory; its tuple interface remains compatible.

Archives contain only committed payloads, metadata, the exact outcome index and an
overview of the local-files stage. They exclude staging and unrelated files. The writer
reopens archives and checks exact unique membership, regular-file types, row identities,
metadata, lengths and SHA-256s, including missing/extra/duplicate members, truncated tar
terminators and corrupt gzip trailers. The maintained `create_tar` compatibility helper
uses the same rules for owned directories; its historical unowned-folder mode remains.
For an already finalized managed directory, the helper raises before reconciliation
or archive publication, preserving the final record and report hash binding. Use the
CLI `--reconcile` operation to revalidate and republish the entire result instead.

Archive and report exports have their own staged inode ownership history. A foreign
file is never replaced merely because its name/content matches. The final run record
`.flowdc/final.json` is written **after** required reports. Overview reports identify its
expected SHA-256 and set their `run_complete` and `useful_final_payload_bytes` to null;
consumers must read and match the final record. The archive's earlier overview describes
the loose stage and cannot certify its own archive. Report/archive failure or a missing
or mismatched last record cannot establish completion. A completed run has no failed or
unattempted rows; intentionally skipped invalid URLs remain explicitly counted. A run
with failures may retain verified partial artifacts and corresponding useful bytes.

| Schema-2 field | Meaning |
| --- | --- |
| `verified_payload_bytes` | Integer sum of payload bytes for verified local rows, with each requested row counted once. |
| `unique_content_bytes` | Integer bytes counted once per verified payload SHA-256. No implicit download deduplication. |
| `artifact_file_bytes` | Logical sizes of committed payload and metadata files plus the published archive when present; excludes staging, journals and reports. This is not physical disk allocation. |
| `observed_response_body_bytes` | Measured decoded application-body bytes across attempts, including failed attempts where observed. Not network wire bytes; `observed_bytes_complete=false` identifies incomplete observation. |
| `useful_final_payload_bytes` | In the last completion record, payload bytes crossing the declared boundary; zero when that boundary fails. Pending reports leave it null. |
| `successful_downloads` | Compatibility count of verified local rows, not proof of final run/archive completion. |
| `downloaded_mb`, `avg_speed_MBps` | Decimal display fields; never the source for scientific integer byte accounting. Resume/reconcile throughput and elapsed time are null. |

The benchmark adapter uses schema-2 integer local payload bytes and labels completion
metadata separately. Schema-2 reports missing valid integer bytes are rejected with
the offending path and field. Reports with unavailable elapsed time (resume/reconcile)
are also rejected, since the current benchmark result schema requires measured timing;
no zero duration or derived throughput is invented. Its schema-1 conversion remains
historical. The legacy benchmark field `throughput_mbps` uses MiB/s, consistently with
the schema-1 and img2dataset adapters. Schema-2 derives it from integer payload bytes
divided by 1,048,576 and elapsed seconds, ignoring `downloaded_mb`; extra metrics name
`throughput_unit="MiB/s"` and `throughput_bytes_divisor=1048576`. This does not rewrite
historical results or establish benchmark fairness. Experiment artifact validation supports both schemas, checks partition/parent
IDs and validates new archive members. Source packaging and TaskVine sandbox staging
include `flowdc_integrity.py`.
The existing experimental cloud upload path still requires compressed output; this
increment does not add distributed gradient control or deploy a research profile.

Completed nonempty HTTP bodies with valid timing remain latency-eligible even when
local publication/stat/metadata fails. Such attempts receive zero useful output credit,
a disjoint local-failure count and no invented overload. Empty/incomplete/invalid timing
observations remain ineligible. See [HTTP measurement](HTTP-MEASUREMENT.md).

Rollback is a source revert while preserving new outputs; older code must not resume
or reinterpret the new journal. Validation establishes exercised software invariants,
not performance, manuscript efficacy, distributed fairness or merge readiness.
