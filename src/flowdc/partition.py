"""Partition manifests without losing original row identity or invalid rows."""

from pathlib import Path
from urllib.parse import urlsplit

import polars as pl

from . import integrity
from .http import load_input_file


def partition_manifest(
    input_path, output_dir, *, groups=1, url_column="url", method="host", input_format=None
):
    source = Path(input_path)
    raw = source.read_bytes()
    frame = load_input_file(str(source), input_format)
    if source.read_bytes() != raw:
        raise ValueError("manifest changed while reading")
    if url_column not in frame.columns:
        raise ValueError(f"missing URL column: {url_column}")
    frame = integrity.stamp_frame(frame, integrity.digest(raw))
    return partition_frame(frame, output_dir, groups=groups, url_column=url_column, method=method)


def partition_frame(frame, output_dir, *, groups=1, url_column="url", method="host"):
    """Write a previously validated snapshot without rereading a mutable manifest."""
    if type(groups) is not int or groups < 1:
        raise ValueError("groups must be a positive integer")
    output = Path(output_dir)
    if output.exists() or output.is_symlink():
        raise FileExistsError(f"Partition destination exists: {output}")
    bins = [[] for _ in range(groups)]
    if method == "host":
        hosts = {}
        for row in frame.iter_rows(named=True):
            value = row.get(url_column)
            try:
                host = urlsplit(value.strip()).netloc.lower() if isinstance(value, str) else ""
            except ValueError:
                host = ""
            hosts.setdefault(host, []).append(row)
        for _, rows in sorted(hosts.items(), key=lambda item: (-len(item[1]), item[0])):
            bins[min(range(groups), key=lambda i: (len(bins[i]), i))].extend(rows)
    elif method == "rows":
        for i, row in enumerate(frame.iter_rows(named=True)):
            bins[i % groups].append(row)
    else:
        raise ValueError("partition method must be host or rows")
    with integrity.Files(output.absolute().parent, create=True):
        pass
    output.mkdir()
    paths = []
    for i, rows in enumerate(bins):
        part = pl.DataFrame(rows, schema=frame.schema) if rows else frame.head(0)
        path = output / f"part-{i:04d}.parquet"
        part.write_parquet(path)
        paths.append(path)
    return paths
