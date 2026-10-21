"""Bounded lossless metadata collection, using one file per sealed run."""

import hashlib
import io
import json
import shlex
import shutil
import subprocess
import tarfile
import time
from pathlib import Path

from . import ablation, cloud
from .common import file_hash, read_json, replace_derived, require, write_new


def pack(study):
    from .ablation_analysis import closed_archive, seal

    study = Path(study)
    target = study / "collection.tar.gz"
    receipt = study / "collection.json"
    previous = closed_archive(target, receipt)
    if previous:
        return previous
    partial = target.with_name(target.name + ".partial")
    paths = [p for p in study.iterdir() if p.is_file() and p.suffix in (".json", ".jsonl", ".parquet")]
    paths.extend(p for p in (study / "slices").glob("*.parquet"))
    paths.extend(p for p in (study / "analysis").rglob("*") if p.is_file())
    for parent in (study / "runs", study / "controlled"):
        for directory in parent.glob("*"):
            if not directory.is_dir():
                continue
            bundle = directory / "metadata.tar.gz"
            seal(directory)  # Validate/recover the closed bundle after quiescence.
            paths.append(bundle)
            paths.append(directory / "metadata-bundle.json")
            if (directory / "record.json").exists():
                paths.append(directory / "record.json")
    for directory in [study / "health", *study.glob("recovery-[0-9]")]:
        if directory.exists():
            paths.extend(p for p in directory.rglob("*") if p.is_file() and p.suffix in (".json", ".jsonl"))
    manifest = {}
    with tarfile.open(partial, "x:gz", compresslevel=1) as archive:
        for p in sorted(set(paths)):
            require(not p.is_symlink(), "metadata symlink")
            name = str(p.relative_to(study))
            manifest[name] = dict(bytes=p.stat().st_size, sha256=file_hash(p))
            archive.add(p, arcname=name, recursive=False)
        raw = json.dumps(manifest, sort_keys=True).encode()
        m = tarfile.TarInfo("metadata-manifest.json")
        m.size = len(raw)
        archive.addfile(m, io.BytesIO(raw))
    require(partial.stat().st_size <= ablation.LIMITS["metadata_bytes"], "compressed metadata bound exceeded")
    value = dict(bytes=partial.stat().st_size, sha256=file_hash(partial), files=len(manifest))
    replace_derived(receipt, value)
    partial.replace(target)
    return value


def extract(archive, destination, maximum, *, deadline=None, max_members=2000000):
    seen, size = set(), 0
    with tarfile.open(archive, "r|*") as stream:
        for member in stream:
            require(deadline is None or time.monotonic() < deadline, "collection deadline")
            require(
                member.isfile()
                and not Path(member.name).is_absolute()
                and ".." not in Path(member.name).parts
                and len(Path(member.name).parts) <= 32
                and len(member.name.encode()) <= 4096,
                "unsafe metadata",
            )
            size += member.size
            require(
                member.name not in seen and len(seen) < max_members and size <= maximum,
                "metadata expansion bound",
            )
            seen.add(member.name)
            stream.extract(member, destination, filter="data")
    manifest = (
        read_json(destination / "metadata-manifest.json")
        if (destination / "metadata-manifest.json").exists()
        else read_json(destination / "bundle-manifest.json")
    )
    actual = {str(p.relative_to(destination)) for p in destination.rglob("*") if p.is_file()} - {
        "metadata-manifest.json",
        "bundle-manifest.json",
    }
    require(actual == set(manifest), "metadata membership mismatch")
    for name, spec in manifest.items():
        require(deadline is None or time.monotonic() < deadline, "collection deadline")
        p = destination / name
        require(
            p.stat().st_size == spec["bytes"] and file_hash(p, deadline) == spec["sha256"],
            "metadata digest mismatch",
        )


def inspect_bundle(archive, maximum, *, deadline=None):
    """Verify every sealed member without materializing millions of tiny files."""
    actual, manifest, total = {}, None, 0

    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, "duplicate manifest key")
            result[key] = value
        return result

    with tarfile.open(archive, "r|*") as stream:
        for member in stream:
            require(deadline is None or time.monotonic() < deadline, "collection deadline")
            require(
                member.isfile()
                and not Path(member.name).is_absolute()
                and ".." not in Path(member.name).parts,
                "unsafe metadata",
            )
            total += member.size
            require(total <= maximum and len(actual) < 2000000, "metadata expansion bound")
            content = stream.extractfile(member)
            if member.name == "bundle-manifest.json":
                require(manifest is None, "duplicate bundle manifest")
                manifest = json.loads(content.read(), object_pairs_hook=unique_object)
                continue
            require(member.name not in actual, "duplicate metadata member")
            digest = hashlib.sha256()
            size = 0
            while chunk := content.read(1024 * 1024):
                require(deadline is None or time.monotonic() < deadline, "collection deadline")
                size += len(chunk)
                digest.update(chunk)
            require(size == member.size, "truncated metadata member")
            actual[member.name] = dict(bytes=size, sha256=digest.hexdigest())
    require(isinstance(manifest, dict) and set(actual) == set(manifest), "metadata membership mismatch")
    for name, value in manifest.items():
        require(
            isinstance(value, dict) and type(value.get("bytes")) is int and value == actual[name],
            "metadata digest mismatch",
        )
    return dict(
        bytes=total,
        files=len(actual),
        archive_sha256=file_hash(archive, deadline),
        journals="retained losslessly in metadata.tar.gz; extract on demand",
    )


def collection_spent(packet):
    total = 0
    for path in Path(packet).glob("collection-attempt-*-intent.json"):
        closed = path.with_name(path.name.replace("-intent", ""))
        total += read_json(closed)["elapsed_s"] if closed.exists() else read_json(path)["seconds"]
    return total


def collect(packet, seconds=900):
    packet = Path(packet)
    if (packet / "collected").exists():
        return packet / "collected"
    seconds = min(seconds, 900 - collection_spent(packet))
    require(seconds > 0, "aggregate collection budget exhausted")
    index = len(list(packet.glob("collection-attempt-*-intent.json")))
    require(index < 8, "finite collection attempts exhausted")
    write_new(packet / f"collection-attempt-{index:02}-intent.json", dict(seconds=seconds))
    began = time.monotonic()
    try:
        return _collect(packet, seconds)
    finally:
        write_new(packet / f"collection-attempt-{index:02}.json", dict(elapsed_s=time.monotonic() - began))


def _collect(packet, seconds):
    packet = Path(packet)
    target = packet / "collected"
    if target.exists():
        return target
    # 40GiB reserve +4GiB compressed +8GiB outer files +4GiB allocation margin.
    require(shutil.disk_usage(packet).free >= 56 * 2**30, "collection would consume local reserve")
    op = read_json(packet / "operator.json")
    receipt = read_json(packet / "deployment-receipt.json")
    root = op["guest_root"]
    began = time.monotonic()
    deadline = began + seconds
    require(0 < seconds <= 900, "collection budget")
    path = packet / "collection.json"
    if not path.exists():
        script = (
            "import json,sys;sys.path.insert(0," + repr(root + "/source") + ");"
            "from benchmark.real_server_study.ablation_collect import pack;print(json.dumps(pack(sys.argv[1])))"
        )
        raw = cloud.ssh(
            packet, [receipt["python"], "-B", "-c", script, root + "/study"], timeout=min(600, seconds - 30)
        )
        write_new(path, json.loads(raw))
    spec = read_json(path)
    require(spec["bytes"] <= ablation.LIMITS["metadata_bytes"], "metadata bound")
    archive = packet / "collection.tar.gz"
    if not archive.exists():
        partial = packet / "collection.partial"
        if partial.exists():
            partial.unlink()  # Regenerable transfer, guest evidence is unchanged.
        script = "import shutil,sys;shutil.copyfileobj(open(sys.argv[1],'rb'),sys.stdout.buffer)"
        command = [
            "ssh",
            "-F",
            cloud.ssh_configuration(packet),
            "--",
            "worker",
            shlex.join([op["guest_python"], "-c", script, root + "/study/collection.tar.gz"]),
        ]
        with partial.open("xb") as out:
            result = subprocess.run(
                command,
                stdout=out,
                stderr=subprocess.DEVNULL,
                timeout=max(1, seconds - (time.monotonic() - began)),
            )
        require(
            result.returncode == 0
            and partial.stat().st_size == spec["bytes"]
            and file_hash(partial, deadline) == spec["sha256"],
            "collection transfer incomplete",
        )
        partial.replace(archive)
    else:
        require(
            archive.stat().st_size == spec["bytes"] and file_hash(archive, deadline) == spec["sha256"],
            "retained collection changed",
        )
    partial = packet / "collected.partial"
    if partial.exists():
        require(partial.is_dir() and not partial.is_symlink(), "unsafe extraction temporary")
        shutil.rmtree(partial)  # Derived from the verified local archive above.
    partial.mkdir()
    try:
        extract(archive, partial, 8 * 2**30, deadline=deadline, max_members=4096)
        expanded = 0
        for bundle in sorted(partial.glob("*/**/metadata.tar.gz")):
            inspection = inspect_bundle(bundle, 512 * 2**20, deadline=deadline)
            receipt = read_json(bundle.parent / "metadata-bundle.json")
            require(
                inspection["archive_sha256"] == receipt["sha256"]
                and bundle.stat().st_size == receipt["bytes"],
                "sealed metadata changed",
            )
            expanded += inspection["bytes"]
            require(expanded <= 24 * 2**30, "total metadata expansion bound")
            replace_derived(bundle.parent / "metadata-inspection.json", inspection)
    except BaseException:
        shutil.rmtree(partial)
        raise
    partial.replace(target)
    return target
