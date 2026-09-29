"""Reproducible synthetic fixture bytes; no generated datasets belong in Git."""

import io
import struct
import zlib

from flowdc_experiment_data import ExperimentError, digest
from flowdc_integrity import stamp_frame


def png(index):
    def chunk(kind, raw):
        return struct.pack("!I", len(raw)) + kind + raw + struct.pack("!I", zlib.crc32(kind + raw))

    rows = b"".join(b"\0" + bytes((index * 3 % 256, y, index * 7 % 256)) * 128 for y in range(128))
    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack("!2I5B", 128, 128, 8, 2, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(rows, 9))
        + chunk(b"IEND", b"")
    )


def generate(address, cases, *, preserve_identity=False):
    try:
        import polars as pl
    except ImportError:
        raise ExperimentError("local_polars_required") from None
    images = {f"images/{i}.png": png(i) for i in range(64)}
    files, partitions = dict(images), {}
    for case in cases:
        parts = []
        parent = pl.DataFrame({"url": [f"http://{address}:8000/{case}/{i}.png" for i in range(64)]})
        original = io.BytesIO()
        parent.write_parquet(original)
        if preserve_identity:
            parent = stamp_frame(parent, digest(original.getvalue()))
        for group in range(2):
            indices = range(group * 32, (group + 1) * 32)
            buffer = io.BytesIO()
            parent.slice(group * 32, 32).write_parquet(buffer)
            filename = f"part-{group:03}.parquet"
            path = f"inputs/{case}/{filename}"
            files[path] = buffer.getvalue()
            parts.append(
                {
                    "name": filename,
                    "rows": 32,
                    "manifest_sha256": digest(files[path]),
                    **({"row_ids": parent.slice(group * 32, 32)["__key__"].to_list()} if preserve_identity else {}),
                    "expected_sha256": [digest(images[f"images/{i}.png"]) for i in indices],
                }
            )
        partitions[case] = parts
    return files, partitions
