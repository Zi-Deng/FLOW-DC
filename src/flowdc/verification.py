"""Offline verification; this module never creates an HTTP session."""

from pathlib import Path

from . import integrity


def verify_output(output_dir):
    root = Path(output_dir)
    with integrity.RunStore(root, allow_historical=True) as store:
        before = store.fs.json(".flowdc/final.json") if store.fs.exists(".flowdc/final.json") else None
        # Reconciliation reopens committed files. Keep completion claims explicit.
        index = store.reconcile(recover=False, publish=False)
        if store.fs.exists("outcome-index.json"):
            integrity.require(index == store.fs.json("outcome-index.json"), "committed row index changed")
        archive = before.get("archive") if before else None
        if archive:
            path = Path(archive["path"])
            if not path.is_file() or path.is_symlink():
                raise integrity.IntegrityError("missing or unsafe archive")
            with path.open("rb") as stream:
                import hashlib

                digest = hashlib.file_digest(stream, "sha256").hexdigest()
            integrity.require(
                path.stat().st_size == archive["bytes"] and digest == archive["sha256"], "archive changed"
            )
            with path.open("rb") as stream:
                integrity.verify_archive(stream, index, root.name)
        return {
            "verified": True,
            "original_rows": index["original_rows"],
            "counts": index["counts"],
            "verified_payload_bytes": index["verified_payload_bytes"],
            "archive_verified": bool(archive),
        }
