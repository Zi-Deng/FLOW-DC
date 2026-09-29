"""Versioned local output protocol. Completion records commit verified components.

Atomic JSON records and retained staging support process-interruption recovery,
not host-power-loss durability. No operation in this module performs HTTP calls.
All filesystem traversal uses directory descriptors and rejects symlinks.
"""

import contextlib
import dataclasses
import datetime
import fcntl
import hashlib
import io
import json
import math
import os
import re
import stat
import tarfile
from collections import Counter
from pathlib import Path
from urllib.parse import unquote, urlsplit
from uuid import uuid4

SCHEMA = 2
INTERNAL = ".flowdc"
PROVENANCE = (
    "__flowdc_manifest__",
    "__flowdc_position__",
    "__flowdc_source_rows__",
    "__flowdc_row_id__",
    "__flowdc_row_digest__",
)
EXTERNAL_KEY = "__flowdc_external_key__"
PARTITION_HOST = "__flowdc_partition_host__"
RESERVED = {INTERNAL, "overview.json", "outcome-index.json"}
HEX = re.compile(r"[0-9a-f]{64}\Z")


class IntegrityError(ValueError):
    """Invalid or conflicting local evidence; preserve files and fail closed."""


def require(condition, message):
    if not condition:
        raise IntegrityError(message)


def json_value(value):
    if isinstance(value, dict):
        return {str(k): json_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_value(v) for v in value]
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, bytes):
        return {"encoding": "hex", "value": value.hex()}
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def encode(value):
    return (
        json.dumps(
            json_value(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
        )
        + "\n"
    ).encode()


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def parse(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, "duplicate JSON key")
            result[key] = value
        return result

    try:
        return json.loads(
            raw, object_pairs_hook=pairs, parse_constant=lambda _: require(False, "nonfinite JSON")
        )
    except (ValueError, UnicodeError) as exc:
        raise IntegrityError("malformed or truncated JSON evidence") from exc


def row_identity(manifest, position):
    return digest(encode([manifest, position]))


def row_metadata(row):
    return json_value(
        {k: v for k, v in row.items() if k not in PROVENANCE and k not in ("__key__", PARTITION_HOST)}
    )


def stamp_frame(frame, manifest):
    """Stamp before filtering/partitioning; validate complete supplied provenance.

    Source identity is SHA-256 of the original manifest bytes. Supplied partitions
    carry that parent's identity and position plus a digest of the original row.
    Digests detect inconsistent provenance; they do not authenticate an author.
    """
    import polars as pl

    require(isinstance(manifest, str) and HEX.fullmatch(manifest), "invalid manifest identity")
    columns = set(frame.columns)
    present = columns.intersection(PROVENANCE)
    require(not present or present == set(PROVENANCE), "partial supplied provenance")
    require(
        present or not columns.intersection({EXTERNAL_KEY, PARTITION_HOST}),
        "reserved provenance without parent identity",
    )
    require(
        not {c for c in columns if c.startswith("__flowdc_")}
        - set(PROVENANCE)
        - {EXTERNAL_KEY, PARTITION_HOST},
        "unknown provenance column",
    )
    if not present and "__key__" in columns:
        require(EXTERNAL_KEY not in columns, "conflicting external key")
        frame = frame.rename({"__key__": EXTERNAL_KEY})
    rows, ids, parents = [], set(), {}
    for index, row in enumerate(frame.iter_rows(named=True)):
        if present:
            source, position, count, key, fingerprint = (row[p] for p in PROVENANCE)
            require(isinstance(source, str) and HEX.fullmatch(source), "invalid supplied manifest")
            require(
                type(position) is int and type(count) is int and 0 <= position < count,
                "invalid supplied position/count",
            )
            require(source not in parents or parents[source] == count, "conflicting parent count")
            parents[source] = count
            require(key == row_identity(source, position), "conflicting supplied row identity")
            require(fingerprint == digest(encode(row_metadata(row))), "conflicting supplied row metadata")
            require("__key__" not in row or row["__key__"] == key, "conflicting internal key")
        else:
            source, position, count = manifest, index, frame.height
            key = row_identity(source, position)
            fingerprint = digest(encode(row_metadata(row)))
        require(key not in ids, "duplicate internal row identity")
        ids.add(key)
        row.update(zip(PROVENANCE, (source, position, count, key, fingerprint), strict=True))
        row["__key__"] = key
        rows.append(row)
    if rows:
        return pl.DataFrame(rows, infer_schema_length=None)
    # Empty manifests are legitimate runs and retain their input schema.
    return frame.with_columns(
        *[
            pl.Series(name, [], dtype=pl.Int64 if name in PROVENANCE[1:3] else pl.String)
            for name in (*PROVENANCE, "__key__")
        ]
    )


def effective_config(cfg):
    excluded = {"input_path", "output_folder", "force_overwrite", "resume", "reconcile"}
    modules = ["download_batch.py", "single_download.py", "flowdc_integrity.py"]
    modules.append("flowdc_methods.py")
    variant = "gradient" if hasattr(cfg, "gradient_threshold") else "base"
    if variant == "gradient":
        modules.append("download_batch_gradient.py")
    return {
        "implementation": variant,
        "source_sha256": {name: digest(Path(__file__).with_name(name).read_bytes()) for name in modules},
        "values": json_value({k: v for k, v in dataclasses.asdict(cfg).items() if k not in excluded}),
    }


def valid_url(value):
    if not isinstance(value, str) or not value.strip() or any(c.isspace() for c in value.strip()):
        return False
    try:
        parsed = urlsplit(value.strip())
        return parsed.scheme in ("http", "https") and bool(parsed.hostname) and parsed.port != 0
    except ValueError:
        return False


def component(value):
    require(isinstance(value, str) and value and value not in (".", ".."), "unsafe path component")
    decoded = value
    while True:
        require(
            not any(c in decoded for c in ("/", "\\", "\0")) and decoded not in (".", ".."),
            "unsafe or encoded path separator",
        )
        new = unquote(decoded)
        if new == decoded:
            break
        decoded = new
    require(len(value.encode()) <= 240 and not any(ord(c) < 32 for c in value), "invalid path component")
    return value


def relative(path):
    require(isinstance(path, str) and not path.startswith("/"), "absolute output path")
    parts = path.split("/")
    for part in parts:
        component(part)
    return parts


class Files:
    """An anchored, no-symlink filesystem view. Never follows parent symlinks."""

    def __init__(self, root, *, create=False):
        path = Path(os.path.abspath(root))
        self.path = path
        fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
        try:
            for part in path.parts[1:]:
                if create:
                    try:
                        os.mkdir(part, 0o700, dir_fd=fd)
                    except FileExistsError:
                        pass
                new = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                os.close(fd)
                fd = new
            self.fd = fd
        except BaseException:
            os.close(fd)
            raise

    def close(self):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    @contextlib.contextmanager
    def parent(self, path, *, create=False):
        parts = relative(path)
        fd = os.dup(self.fd)
        try:
            for part in parts[:-1]:
                if create:
                    try:
                        os.mkdir(part, 0o700, dir_fd=fd)
                    except FileExistsError:
                        pass
                new = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                os.close(fd)
                fd = new
            yield fd, parts[-1]
        finally:
            os.close(fd)

    def info(self, path):
        with self.parent(path) as (fd, name):
            return os.stat(name, dir_fd=fd, follow_symlinks=False)

    def exists(self, path):
        try:
            self.info(path)
            return True
        except FileNotFoundError:
            return False

    def mkdir(self, path):
        with self.parent(path, create=True) as (fd, name):
            os.mkdir(name, 0o700, dir_fd=fd)

    def list(self, path):
        with self.parent(path) as (fd, name):
            child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            try:
                return sorted(os.listdir(child))
            finally:
                os.close(child)

    def open(self, path, *, write=False):
        with self.parent(path, create=write) as (fd, name):
            flags = os.O_NOFOLLOW | (os.O_WRONLY | os.O_CREAT | os.O_EXCL if write else os.O_RDONLY)
            child = os.open(name, flags, 0o600, dir_fd=fd)
            try:
                require(stat.S_ISREG(os.fstat(child).st_mode), "non-regular artifact")
                return os.fdopen(child, "wb" if write else "rb")
            except BaseException:
                os.close(child)
                raise

    def read(self, path):
        with self.open(path) as stream:
            return stream.read()

    def write(self, path, raw):
        with self.open(path, write=True) as stream:
            stream.write(raw)

    def link(self, source, target, target_fs=None):
        destination = target_fs or self
        with self.parent(source) as (src_fd, src), destination.parent(target, create=True) as (dst_fd, dst):
            require(
                stat.S_ISREG(os.stat(src, dir_fd=src_fd, follow_symlinks=False).st_mode),
                "invalid staged component",
            )
            os.link(src, dst, src_dir_fd=src_fd, dst_dir_fd=dst_fd, follow_symlinks=False)

    def unlink(self, path):
        with self.parent(path) as (fd, name):
            os.unlink(name, dir_fd=fd)

    def json(self, path):
        return parse(self.read(path))

    def atomic(self, path, value, *, replace=False):
        raw = encode(value)
        temporary = path + ".writing-" + uuid4().hex
        self.write(temporary, raw)
        # A torn temporary is retained as evidence, never parsed as a record.
        with self.parent(temporary) as (srcfd, src), self.parent(path) as (dstfd, dst):
            if replace:
                if self.exists(path):
                    require(stat.S_ISREG(self.info(path).st_mode), "unsafe record destination")
                os.replace(src, dst, src_dir_fd=srcfd, dst_dir_fd=dstfd)
            else:
                os.link(src, dst, src_dir_fd=srcfd, dst_dir_fd=dstfd, follow_symlinks=False)
                os.unlink(src, dir_fd=srcfd)

    def verify(self, path, length, sha256):
        with self.open(path) as stream:
            require(os.fstat(stream.fileno()).st_size == length, "artifact size mismatch")
            hashed = hashlib.sha256()
            count = 0
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                hashed.update(chunk)
                count += len(chunk)
            require(count == length and hashed.hexdigest() == sha256, "artifact digest mismatch")

    def same(self, path, other, other_fs=None):
        a, b = self.info(path), (other_fs or self).info(other)
        return (
            stat.S_ISREG(a.st_mode)
            and stat.S_ISREG(b.st_mode)
            and (a.st_dev, a.st_ino) == (b.st_dev, b.st_ino)
        )


def safe_save(content, file_path):
    path = Path(os.path.abspath(file_path))
    with Files(path.parent, create=True) as fs:
        fs.write(component(path.name), content)


def plan_rows(frame, cfg, render):
    rows, destinations = [], {}
    for number, row in enumerate(frame.iter_rows(named=True), 1):
        key = row["__flowdc_row_id__"]
        url = row.get(cfg.url_col)
        item = {
            "row_id": key,
            "source_manifest": row[PROVENANCE[0]],
            "source_position": row[PROVENANCE[1]],
            "source_rows": row[PROVENANCE[2]],
            "row_digest": row[PROVENANCE[4]],
            "row": row_metadata(row),
            "url": json_value(url.strip() if isinstance(url, str) else url),
            "payload": None,
            "metadata": None,
            "initial_disposition": None,
            "error": None,
        }
        rows.append(item)
        if not valid_url(url):
            item.update(initial_disposition="skipped", error="invalid or empty HTTP(S) URL")
            continue
        try:
            ext = Path(urlsplit(url.strip()).path).suffix or ".jpg"
            component(ext)
            if cfg.naming_mode == "row_id":
                filename = key + (".payload.json" if ext.lower() == ".json" else ext)
            elif cfg.naming_mode == "sequential":
                filename = f"{number:08d}" + ext
            else:
                filename = render(cfg.file_name_pattern or "{segment[-2]}", url.strip(), key)
            component(filename)
            prefix = ""
            if cfg.output_format == "imagefolder":
                label = row.get(cfg.label_col) if cfg.label_col else None
                label = "output" if label is None else str(label)
                component(label)
                label = label.replace("'", "").replace('"', "").replace(" ", "_")
                component(label)
                require(label not in RESERVED, "reserved output directory")
                prefix = label + "/"
            payload = prefix + filename
            metadata = prefix + (
                key + ".json" if cfg.naming_mode == "row_id" else str(Path(filename).with_suffix(".json"))
            )
            for path in (payload, metadata):
                require(relative(path)[0] not in RESERVED, "reserved output destination")
                destinations.setdefault(path, []).append(item)
            item.update(payload=payload, metadata=metadata)
        except (ValueError, OSError) as exc:
            item.update(initial_disposition="failed", error=str(exc))
    # A file/directory prefix conflict is a collision as well as exact aliases.
    paths = sorted(destinations)
    for i, path in enumerate(paths):
        conflicts = list(destinations[path])
        for other in paths[i + 1 :]:
            if other.startswith(path + "/"):
                conflicts.extend(destinations[other])
            elif other > path + "/\uffff":
                break
        if len(conflicts) > 1:
            for row in conflicts:
                row.update(initial_disposition="failed", error="output destination collision: " + path)
    return rows


class RunStore:
    def __init__(self, root, *, manifest=None, config=None, rows=None, fault=None):
        self.fs = Files(root)
        self.root = self.fs.path
        self.fault = fault or (lambda _step: None)
        self.lock = None
        try:
            if rows is not None:
                self.fs.mkdir(INTERNAL)
                for directory in ("attempts", "commits", "exports", "rejections"):
                    self.fs.mkdir(f"{INTERNAL}/{directory}")
            self.lock = self.fs.open(f"{INTERNAL}/lock", write=rows is not None)
            fcntl.flock(self.lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            if rows is not None:
                root_stat = os.fstat(self.fs.fd)
                owner = {
                    "schema_version": SCHEMA,
                    "run_id": uuid4().hex,
                    "root": [root_stat.st_dev, root_stat.st_ino],
                    "root_path": str(self.root),
                    "manifest": manifest,
                    "config": config,
                    "rows": rows,
                }
                self.fs.atomic(f"{INTERNAL}/owner.json", owner)
            self.owner = self.fs.json(f"{INTERNAL}/owner.json")
            self._validate_owner()
            if manifest is not None:
                require(self.owner["manifest"] == manifest, "resume manifest mismatch")
            if config is not None:
                require(self.owner["config"] == config, "resume acquisition configuration mismatch")
            self.rows = {row["row_id"]: row for row in self.owner["rows"]}
        except BaseException:
            self.close()
            raise

    def _validate_owner(self):
        owner = self.owner
        require(
            isinstance(owner, dict) and owner.get("schema_version") == SCHEMA, "unsupported ownership schema"
        )
        require(
            isinstance(owner.get("run_id"), str) and re.fullmatch(r"[0-9a-f]{32}", owner["run_id"]),
            "invalid run ownership",
        )
        info = os.fstat(self.fs.fd)
        require(owner.get("root") == [info.st_dev, info.st_ino], "output directory ownership mismatch")
        require(owner.get("root_path") == str(self.root), "output directory ownership path mismatch")
        require(
            isinstance(owner.get("manifest"), dict) and HEX.fullmatch(owner["manifest"].get("sha256", "")),
            "invalid manifest evidence",
        )
        require(
            isinstance(owner.get("config"), dict) and isinstance(owner["config"].get("values"), dict),
            "invalid configuration evidence",
        )
        require(
            type(owner["config"]["values"].get("max_retry_attempts")) is int
            and owner["config"]["values"]["max_retry_attempts"] > 0,
            "invalid owned retry budget",
        )
        require(owner["config"].get("implementation") in ("base", "gradient"), "invalid owned implementation")
        sources = owner["config"].get("source_sha256")
        expected_sources = {"download_batch.py", "single_download.py", "flowdc_integrity.py"}
        if "control_method" in owner["config"]["values"]:
            expected_sources.add("flowdc_methods.py")
        if owner["config"]["implementation"] == "gradient":
            expected_sources.add("download_batch_gradient.py")
        require(
            isinstance(sources, dict)
            and set(sources) == expected_sources
            and all(isinstance(value, str) and HEX.fullmatch(value) for value in sources.values()),
            "invalid source provenance",
        )
        require(isinstance(owner.get("rows"), list), "invalid row evidence")
        ids, paths, parents = set(), set(), {}
        for row in owner["rows"]:
            require(isinstance(row, dict), "invalid owned row")
            source, pos, count = (
                row.get("source_manifest"),
                row.get("source_position"),
                row.get("source_rows"),
            )
            require(
                isinstance(source, str)
                and HEX.fullmatch(source)
                and type(pos) is int
                and type(count) is int
                and 0 <= pos < count,
                "invalid owned provenance",
            )
            key = row.get("row_id")
            require(key == row_identity(source, pos) and key not in ids, "conflicting owned identity")
            ids.add(key)
            require(source not in parents or parents[source] == count, "conflicting owned parent counts")
            parents[source] = count
            require(isinstance(row.get("row"), dict), "invalid owned metadata")
            require(row.get("row_digest") == digest(encode(row.get("row"))), "owned metadata mismatch")
            original_url = row["row"].get(owner["config"]["values"].get("url_col"))
            require(
                row.get("url") == (original_url.strip() if isinstance(original_url, str) else original_url),
                "owned URL mismatch",
            )
            require(row.get("initial_disposition") in (None, "skipped", "failed"), "invalid initial outcome")
            for field in ("payload", "metadata"):
                if row["initial_disposition"] is None:
                    require(
                        isinstance(row.get(field), str) and row[field] not in paths,
                        "invalid or duplicate owned destination",
                    )
                    paths.add(row[field])
                if row.get(field) is not None:
                    require(relative(row[field])[0] not in RESERVED, "reserved owned destination")
        require(owner["manifest"].get("original_rows") == len(ids), "owned denominator mismatch")

    def close(self):
        if self.lock is not None:
            self.lock.close()
            self.lock = None
        self.fs.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def event(self, step):
        self.fault(step)

    def attempts(self, key):
        path = f"{INTERNAL}/attempts/{key}"
        if not self.fs.exists(path):
            return []
        names = self.fs.list(path)
        require(all(re.fullmatch(r"[1-9][0-9]*", name) for name in names), "invalid attempt journal")
        numbers = sorted(map(int, names))
        require(numbers == list(range(1, len(numbers) + 1)), "noncontiguous attempt journal")
        return [f"{path}/{n}" for n in numbers]

    def begin(self, key):
        row = self.rows[key]
        require(row["initial_disposition"] is None, "row is not eligible")
        require(not self.fs.exists(f"{INTERNAL}/commits/{key}.json"), "row already committed")
        attempts = self.attempts(key)
        require(
            len(attempts) < self.owner["config"]["values"]["max_retry_attempts"], "attempt budget exhausted"
        )
        try:
            for path in (row["payload"], row["metadata"]):
                require(not self.fs.exists(path), "unowned or incomplete existing target: " + path)
        except (OSError, ValueError) as exc:
            self.fs.atomic(f"{INTERNAL}/rejections/{key}.json", {"error": str(exc)}, replace=True)
            raise
        number = len(attempts) + 1
        directory = f"{INTERNAL}/attempts/{key}/{number}"
        self.fs.mkdir(directory)
        intent = {"run_id": self.owner["run_id"], "row_id": key, "attempt": number}
        self.fs.atomic(directory + "/intent.json", intent)
        self.event("intent")
        return directory

    def intent(self, directory, key):
        expected = {
            "run_id": self.owner["run_id"],
            "row_id": key,
            "attempt": int(directory.rsplit("/", 1)[1]),
        }
        require(self.fs.json(directory + "/intent.json") == expected, "invalid attempt intent")
        return expected

    def metadata(self, key, size, sha256):
        row = self.rows[key]
        return {
            "schema_version": SCHEMA,
            "key": key,
            "row_id": key,
            "url": row["url"],
            "class_name": row["row"].get(self.owner["config"]["values"].get("label_col")),
            "source_manifest": row["source_manifest"],
            "source_position": row["source_position"],
            "source_rows": row["source_rows"],
            "row": row["row"],
            "payload": row["payload"],
            "payload_bytes": size,
            "payload_sha256": sha256,
        }

    def publish(self, directory, key, content):
        intent = self.intent(directory, key)
        row = self.rows[key]
        self.fs.write(directory + "/payload", content)
        self.event("payload_staged")
        size, sha256 = len(content), digest(content)
        self.fs.verify(directory + "/payload", size, sha256)
        raw_metadata = encode(self.metadata(key, size, sha256))
        self.fs.write(directory + "/metadata", raw_metadata)
        self.event("metadata_staged")
        record = {
            **intent,
            "payload": row["payload"],
            "metadata": row["metadata"],
            "payload_bytes": size,
            "payload_sha256": sha256,
            "metadata_bytes": len(raw_metadata),
            "metadata_sha256": digest(raw_metadata),
        }
        self.fs.verify(directory + "/metadata", len(raw_metadata), digest(raw_metadata))
        self.fs.atomic(directory + "/ready.json", record)
        self.event("ready")
        self.complete(directory, key, record)
        return str(self.root / row["payload"])

    def validate_record(self, directory, key, record):
        intent = self.intent(directory, key)
        require(
            isinstance(record, dict) and all(record.get(k) == v for k, v in intent.items()),
            "completion identity mismatch",
        )
        row = self.rows[key]
        require(
            record.get("payload") == row["payload"] and record.get("metadata") == row["metadata"],
            "completion path mismatch",
        )
        for field in ("payload", "metadata"):
            length, sha = record.get(field + "_bytes"), record.get(field + "_sha256")
            require(
                type(length) is int and length >= 0 and isinstance(sha, str) and HEX.fullmatch(sha),
                "invalid component evidence",
            )
            self.fs.verify(directory + "/" + field, length, sha)
        require(
            self.fs.json(directory + "/metadata")
            == self.metadata(key, record["payload_bytes"], record["payload_sha256"]),
            "row metadata mismatch",
        )

    def complete(self, directory, key, record):
        self.validate_record(directory, key, record)
        for field in ("payload", "metadata"):
            source, destination = directory + "/" + field, record[field]
            if self.fs.exists(destination):
                require(self.fs.same(source, destination), "publication conflict: " + destination)
                self.fs.verify(destination, record[field + "_bytes"], record[field + "_sha256"])
            else:
                self.fs.link(source, destination)
            self.event(field + "_published")
        commit = f"{INTERNAL}/commits/{key}.json"
        if self.fs.exists(commit):
            require(self.fs.json(commit) == record, "conflicting completion record")
        else:
            self.fs.atomic(commit, record)
        self.event("committed")

    def fail(
        self,
        directory,
        key,
        *,
        error,
        status=None,
        retryable=False,
        observed_bytes=0,
        latency_eligible=False,
        body_complete=False,
    ):
        intent = self.intent(directory, key)
        self.fs.atomic(
            directory + "/result.json",
            {
                **intent,
                "error": str(error),
                "status": status,
                "retryable": bool(retryable),
                "observed_response_body_bytes": observed_bytes,
                "latency_eligible": bool(latency_eligible),
                "body_complete": bool(body_complete),
            },
        )

    def reconcile(self, *, recover=True):
        # The final result is valid only after the CLI has also checked archives
        # and published its reports. A fresh reconciliation invalidates it first.
        self.fs.atomic(
            f"{INTERNAL}/final.json", {"run_complete": False, "run_id": self.owner["run_id"]}, replace=True
        )
        for namespace in ("attempts", "commits", "rejections"):
            for name in self.fs.list(f"{INTERNAL}/{namespace}"):
                key = name if namespace == "attempts" else name.split(".json", 1)[0]
                require(key in self.rows, "journal contains an unknown row identity")
                if namespace != "attempts":
                    require(
                        name == key + ".json"
                        or re.fullmatch(re.escape(key) + r"\.json\.writing-[0-9a-f]{32}", name),
                        "invalid row journal filename",
                    )
        outcomes = []
        for key, row in self.rows.items():
            result = {
                "row_id": key,
                "source_manifest": row["source_manifest"],
                "source_position": row["source_position"],
                "disposition": row["initial_disposition"] or "unattempted",
                "error": row["error"],
                "attempt_intents": 0,
                "attempt_information_uncertain": False,
                "retryable": False,
                "observed_response_body_bytes": 0,
                "observed_bytes_complete": True,
                "payload_bytes": 0,
                "payload_sha256": None,
                "payload": row["payload"],
                "metadata": row["metadata"],
            }
            attempts = self.attempts(key)
            rejection = f"{INTERNAL}/rejections/{key}.json"
            if self.fs.exists(rejection):
                result.update(disposition="failed", error=self.fs.json(rejection)["error"])
            result["attempt_intents"] = len(attempts)
            commit_path = f"{INTERNAL}/commits/{key}.json"
            committed = self.fs.json(commit_path) if self.fs.exists(commit_path) else None
            for directory in attempts:
                try:
                    self.intent(directory, key)
                    ready_path = directory + "/ready.json"
                    ready = self.fs.json(ready_path) if self.fs.exists(ready_path) else None
                    failure_path = directory + "/result.json"
                    failure = self.fs.json(failure_path) if self.fs.exists(failure_path) else None
                    if ready is not None:
                        self.validate_record(directory, key, ready)
                        result["observed_response_body_bytes"] += ready["payload_bytes"]
                        if committed is None and recover and failure is None:
                            self.complete(directory, key, ready)
                            committed = ready
                    elif failure is not None:
                        require(
                            all(failure.get(k) == v for k, v in self.intent(directory, key).items()),
                            "failure identity mismatch",
                        )
                        measured = failure.get("observed_response_body_bytes")
                        require(
                            type(measured) is int
                            and measured >= 0
                            and type(failure.get("retryable")) is bool,
                            "invalid failure accounting",
                        )
                        result["observed_response_body_bytes"] += measured
                        result["observed_bytes_complete"] &= failure.get("body_complete") is True
                    else:
                        result["attempt_information_uncertain"] = True
                        result["observed_bytes_complete"] = False
                    if failure is not None:
                        result.update(
                            disposition="failed",
                            error=failure["error"],
                            status_code=failure["status"],
                            retryable=failure["retryable"],
                        )
                    elif committed is None:
                        result.update(
                            disposition="failed",
                            error="interrupted or unresolved attempt",
                            retryable=ready is None,
                        )
                        result["attempt_information_uncertain"] = True
                except (OSError, ValueError, KeyError, TypeError) as exc:
                    result.update(
                        disposition="failed",
                        error="invalid or conflicting attempt evidence: " + str(exc),
                        attempt_information_uncertain=True,
                        retryable=False,
                        observed_bytes_complete=False,
                    )
                    committed = None
                    break
            if committed is not None:
                try:
                    n = committed["attempt"]
                    require(type(n) is int and 1 <= n <= len(attempts), "completion has no intent")
                    directory = attempts[n - 1]
                    self.validate_record(directory, key, committed)
                    for field in ("payload", "metadata"):
                        require(
                            self.fs.same(directory + "/" + field, committed[field]),
                            "committed artifact ownership changed",
                        )
                        self.fs.verify(
                            committed[field], committed[field + "_bytes"], committed[field + "_sha256"]
                        )
                    result.update(
                        disposition="verified",
                        error=None,
                        retryable=False,
                        payload_bytes=committed["payload_bytes"],
                        payload_sha256=committed["payload_sha256"],
                        metadata_bytes=committed["metadata_bytes"],
                        metadata_sha256=committed["metadata_sha256"],
                        status_code=200,
                    )
                except (OSError, ValueError, KeyError, TypeError) as exc:
                    result.update(
                        disposition="failed", error="invalid committed output: " + str(exc), retryable=False
                    )
            result["remaining_attempts"] = max(
                0, self.owner["config"]["values"]["max_retry_attempts"] - len(attempts)
            )
            outcomes.append(result)
        counts = Counter(row["disposition"] for row in outcomes)
        require(sum(counts.values()) == len(self.rows), "outcome denominator mismatch")
        verified = [row for row in outcomes if row["disposition"] == "verified"]
        unique = {row["payload_sha256"]: row["payload_bytes"] for row in verified}
        snapshot = {
            "schema_version": SCHEMA,
            "run_id": self.owner["run_id"],
            "manifest": self.owner["manifest"],
            "original_rows": len(self.rows),
            "counts": {k: counts[k] for k in ("verified", "failed", "skipped", "unattempted")},
            "verified_payload_bytes": sum(row["payload_bytes"] for row in verified),
            "unique_content_bytes": sum(unique.values()),
            "artifact_file_bytes": sum(row["payload_bytes"] + row["metadata_bytes"] for row in verified),
            "observed_response_body_bytes": sum(row["observed_response_body_bytes"] for row in outcomes),
            "observed_bytes_complete": all(row["observed_bytes_complete"] for row in outcomes),
            "rows": outcomes,
        }
        self.fs.atomic(f"{INTERNAL}/outcomes.json", snapshot, replace=True)
        return snapshot

    def eligible(self, snapshot):
        return [
            row["row_id"]
            for row in snapshot["rows"]
            if row["remaining_attempts"] > 0
            and (row["disposition"] == "unattempted" or (row["disposition"] == "failed" and row["retryable"]))
        ]

    def remove_owned_exports(self):
        """Called only after explicit whole-output overwrite consent."""
        path = f"{INTERNAL}/exports.json"
        if not self.fs.exists(path):
            return
        index = self.fs.json(path)
        with Files(self.root.parent) as parent:
            selected = []
            for key, history in index.items():
                if not key.startswith("external:"):
                    continue
                name = key.removeprefix("external:")
                require(
                    name in {self.root.name + suffix for suffix in ("_overview.json", ".tar", ".tar.gz")},
                    "invalid external ownership",
                )
                if parent.exists(name):
                    matches = [item for item in history if self.fs.same(item["stage"], name, parent)]
                    require(len(matches) == 1, "unowned external artifact conflict")
                    parent.verify(name, matches[0]["bytes"], matches[0]["sha256"])
                    selected.append((name, matches[0]["stage"]))
            for name, stage in selected:
                require(self.fs.same(stage, name, parent), "external artifact changed")
                parent.unlink(name)

    def emit(self, name, raw, *, external=False):
        stage = f"{INTERNAL}/exports/{uuid4().hex}"
        self.fs.write(stage, raw)
        return self.emit_staged(name, stage, len(raw), digest(raw), external=external)

    def emit_staged(self, name, stage, size, sha256, *, external=False):
        """Replace only verified owned links; persist export intent before linking.

        Retained stage inodes establish ownership after interruption. Matching
        bytes alone never establish ownership of a pre-existing destination.
        """
        component(name)
        self.fs.verify(stage, size, sha256)
        key = ("external:" if external else "internal:") + name
        index_path = f"{INTERNAL}/exports.json"
        index = self.fs.json(index_path) if self.fs.exists(index_path) else {}
        require(isinstance(index, dict), "invalid export ownership")
        history = index.get(key, [])
        with Files(self.root.parent if external else self.root) as destination:
            previous = None
            if destination.exists(name):
                matches = [
                    record
                    for record in history
                    if self.fs.exists(record["stage"]) and self.fs.same(record["stage"], name, destination)
                ]
                require(len(matches) == 1, "unowned export destination: " + name)
                previous = matches[0]
                destination.verify(name, previous["bytes"], previous["sha256"])
                if previous["sha256"] == sha256 and previous["bytes"] == size:
                    return str(destination.path / name)
            record = {"stage": stage, "bytes": size, "sha256": sha256}
            index[key] = history + [record]
            self.fs.atomic(index_path, index, replace=True)
            self.event("export_intent")
            if destination.exists(name):
                require(
                    previous is not None and self.fs.same(previous["stage"], name, destination),
                    "export changed during publication",
                )
                destination.unlink(name)
            self.fs.link(stage, name, destination)
            self.event("export_published")
            return str(destination.path / name)

    def make_archive(self, snapshot, report, *, compress):
        # Stream only committed records into an owned same-filesystem stage.
        # Memory is bounded by the manifest/index and one member, not the archive.
        stage = f"{INTERNAL}/exports/{uuid4().hex}"
        with self.fs.open(stage, write=True) as stream:
            with tarfile.open(
                fileobj=stream, mode="w:gz" if compress else "w", format=tarfile.PAX_FORMAT
            ) as archive:
                for row in snapshot["rows"]:
                    if row["disposition"] != "verified":
                        continue
                    for field in ("payload", "metadata"):
                        self.fs.verify(row[field], row[field + "_bytes"], row[field + "_sha256"])
                        info = tarfile.TarInfo(self.root.name + "/" + row[field])
                        info.size, info.mode, info.mtime = row[field + "_bytes"], 0o600, 0
                        with self.fs.open(row[field]) as member:
                            archive.addfile(info, member)
                for name, content in (
                    ("outcome-index.json", encode(snapshot)),
                    ("overview.json", encode(report)),
                ):
                    info = tarfile.TarInfo(self.root.name + "/" + name)
                    info.size, info.mode, info.mtime = len(content), 0o600, 0
                    archive.addfile(info, io.BytesIO(content))
        with self.fs.open(stage) as stream:
            verify_archive(stream, snapshot, self.root.name)
            stream.seek(0)
            hashed = hashlib.sha256()
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                hashed.update(chunk)
            size = os.fstat(stream.fileno()).st_size
        sha256 = hashed.hexdigest()
        name = self.root.name + (".tar.gz" if compress else ".tar")
        path = self.emit_staged(name, stage, size, sha256, external=True)
        with Files(self.root.parent) as parent, parent.open(name) as stream:
            verify_archive(stream, snapshot, self.root.name)
        return path, size, sha256


def verify_archive(raw, snapshot, prefix):
    """Verify exact membership, types, unique names, metadata, lengths and digests."""
    expected = {prefix + "/outcome-index.json": encode(snapshot)}
    for row in snapshot["rows"]:
        if row["disposition"] == "verified":
            for field in ("payload", "metadata"):
                name = prefix + "/" + row[field]
                require(name not in expected, "duplicate row member")
                expected[name] = (row[field + "_bytes"], row[field + "_sha256"])
    expected[prefix + "/overview.json"] = None
    source = io.BytesIO(raw) if isinstance(raw, bytes) else raw
    source.seek(0)
    compressed = source.read(2) == b"\x1f\x8b"
    source.seek(0)
    seen, end = set(), 0
    with tarfile.open(fileobj=source, mode="r:*") as archive:
        for item in archive:
            require(
                item.isfile() and item.name in expected and item.name not in seen,
                "unexpected, duplicate or unsafe archive member",
            )
            seen.add(item.name)
            end = max(end, item.offset_data + ((item.size + 511) // 512) * 512)
            stream = archive.extractfile(item)
            content = stream.read()
            require(len(content) == item.size, "truncated archive member")
            specification = expected[item.name]
            if isinstance(specification, tuple):
                require((len(content), digest(content)) == specification, "archive component mismatch")
                if item.name.endswith(".json"):
                    matching = [r for r in snapshot["rows"] if prefix + "/" + str(r["metadata"]) == item.name]
                    if matching:
                        metadata = parse(content)
                        row = matching[0]
                        require(
                            metadata.get("row_id") == row["row_id"]
                            and metadata.get("payload_sha256") == row["payload_sha256"],
                            "archive row metadata mismatch",
                        )
            elif specification is not None:
                require(content == specification, "archive outcome index mismatch")
            else:
                require(isinstance(parse(content), dict), "invalid archived overview")
        require(seen == set(expected), "missing archive members")
    # tarfile accepts missing terminators and ignores trailing bytes. Verify the
    # uncompressed tail in bounded chunks; gzip also verifies its CRC/trailer.
    source.seek(0)
    import gzip

    tail_stream = gzip.GzipFile(fileobj=source) if compressed else source
    try:
        position = 0
        for chunk in iter(lambda: tail_stream.read(1024 * 1024), b""):
            if position + len(chunk) > end:
                require(not any(chunk[max(0, end - position) :]), "nonzero archive tail")
            position += len(chunk)
        require(position % 512 == 0 and position - end >= 1024, "truncated archive terminator")
    finally:
        if compressed:
            tail_stream.close()
    return True
