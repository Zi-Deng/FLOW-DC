"""Bounded official metadata preparation. Never requests image payloads."""

import argparse
import csv
import gzip
import hashlib
import io
import time
import urllib.request
from pathlib import Path

import polars as pl

from .common import file_hash, require, utc, write_new
from .public_guard import NoRedirect

BASE = "https://inaturalist-open-data.s3.amazonaws.com/"


class PrefixReader:
    def __init__(self, response, deadline, maximum):
        self.response, self.deadline, self.maximum = response, deadline, maximum
        self.bytes, self.digest = 0, hashlib.sha256()

    def read(self, count=-1):
        require(time.monotonic() < self.deadline, "metadata preparation deadline")
        require(count >= 0, "unbounded compressed metadata read")
        data = self.response.read(count)
        self.bytes += len(data)
        require(self.bytes <= self.maximum, "metadata compressed-byte ceiling")
        self.digest.update(data)
        return data


def inaturalist(destination, count=2000000):
    destination = Path(destination)
    require(not destination.exists() and 1 <= count <= 2000000, "new bounded population required")
    destination.mkdir(parents=True, mode=0o700)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    observers = {}
    deadline = time.monotonic() + 1800
    receipts = {}
    with opener.open(BASE + "observers.csv.gz", timeout=30) as response:
        raw = PrefixReader(response, deadline, 128 * 2**20)
        with io.TextIOWrapper(gzip.GzipFile(fileobj=raw), encoding="utf8", newline="") as text:
            for row in csv.DictReader(text, delimiter="\t"):
                observers[row["observer_id"]] = row["name"] or row["login"]
        receipts["observers"] = dict(
            url=BASE + "observers.csv.gz",
            headers=dict(response.headers),
            consumed_bytes=raw.bytes,
            sha256=raw.digest.hexdigest(),
        )
    rows, scanned, duplicate_photos, seen = [], 0, 0, set()
    with opener.open(BASE + "photos.csv.gz", timeout=30) as response:
        raw = PrefixReader(response, deadline, 3 * 2**30)
        with io.TextIOWrapper(gzip.GzipFile(fileobj=raw), encoding="utf8", newline="") as text:
            for row in csv.DictReader(text, delimiter="\t"):
                scanned += 1
                require(scanned <= 25000000, "metadata prefix row ceiling")
                if row["license"] not in ("CC0", "CC-BY") or row["extension"] not in ("jpg", "jpeg"):
                    continue
                author = observers.get(row["observer_id"])
                if not author:
                    continue
                photo = row["photo_id"]
                if photo in seen:
                    duplicate_photos += 1
                    continue
                seen.add(photo)
                rows.append(
                    dict(
                        url=BASE + f"photos/{photo}/original.{row['extension']}",
                        record_id=photo,
                        author=author,
                        license=row["license"],
                        landing_url="https://www.inaturalist.org/photos/" + photo,
                        observer_id=row["observer_id"],
                        observation_uuid=row["observation_uuid"],
                    )
                )
                if len(rows) == count:
                    break
        receipts["photos"] = dict(
            url=BASE + "photos.csv.gz",
            headers=dict(response.headers),
            compressed_prefix_bytes=raw.bytes,
            compressed_prefix_sha256=raw.digest.hexdigest(),
            scanned_rows=scanned,
            duplicate_photo_ids_excluded=duplicate_photos,
            whole_object_downloaded=False,
        )
    require(len(rows) == count, "insufficient license-qualified metadata cohort")
    frame = pl.DataFrame(rows)
    require(frame["url"].n_unique() == frame.height, "duplicate photo identity")
    path = destination / "population.parquet"
    frame.write_parquet(path)
    write_new(
        destination / "provenance.json",
        dict(
            source="inaturalist-open",
            utc=utc(),
            rows=frame.height,
            population_sha256=file_hash(path),
            media_rights_qualified=True,
            metadata=receipts,
            rendition="original, publisher max2048px",
            selection="First 2000000 eligible CC0/CC-BY JPEG records in a bounded publisher metadata prefix; randomized into disjoint blocks before any HTTP outcomes. Not a representative sample of all iNaturalist.",
            rights_source="https://github.com/inaturalist/inaturalist-open-data",
            traffic_basis="Official public open-data S3 bulk-image route; bounded study, no unlimited-traffic assumption.",
        ),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("destination", type=Path)
    parser.add_argument("--rows", type=int, default=2000000)
    args = parser.parse_args()
    inaturalist(args.destination, args.rows)
