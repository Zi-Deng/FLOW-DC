"""Deterministic payload names; row IDs preserve duplicate requested rows."""

from __future__ import annotations

import asyncio
import os
import re
from pathlib import Path
from urllib.parse import urlsplit

from .http import extract_extension


def _sanitize_filename(name: str, max_len: int = 180) -> str:
    """Sanitize a string for use as a filename."""
    name = name.strip().replace(os.sep, "_")
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("._")
    if not name:
        name = "file"
    return name[:max_len]


class SequentialNamer:
    """Stable sequential filename generator."""

    def __init__(self):
        self._lock = asyncio.Lock()
        self._counter = 0
        self._map: dict[str, str] = {}

    async def filename_for(self, key: str, url: str) -> str:
        """Get filename for a key, creating if needed."""
        async with self._lock:
            if key in self._map:
                return self._map[key]

            _, ext = extract_extension(str(url))
            if not ext:
                ext = ".jpg"

            self._counter += 1
            fn = f"{self._counter:08d}{ext}"
            self._map[key] = fn
            return fn


def render_filename(pattern: str, url: str, key: str) -> str:
    """Render filename from pattern and URL.

    Available template variables:
        segment: List of URL path segments (non-empty parts)
        segment_last: Last segment (usually the filename)
        segment_parent: Second-to-last segment (often an ID)
        segment_first: First segment
        ext: File extension from URL (e.g., '.jpg')
        key: Sanitized row key/identifier

    Example patterns:
        '{segment_parent}' - Use parent directory name (e.g., photo ID)
        '{segment_last}' - Use original filename
        '{key}_{segment_parent}' - Combine key with parent segment
    """
    seg = [s for s in urlsplit(url).path.split("/") if s]
    ext = Path(urlsplit(url).path).suffix or ""
    safe_key = _sanitize_filename(key)

    # Pre-compute common segment accessors for template use
    # (Python's str.format() doesn't support negative indexing like {segment[-2]})
    env = {
        "segment": seg,
        "ext": ext,
        "key": safe_key,
        # Convenience accessors (handle empty lists gracefully)
        "segment_last": seg[-1] if seg else "",
        "segment_parent": seg[-2] if len(seg) >= 2 else (seg[-1] if seg else ""),
        "segment_first": seg[0] if seg else "",
    }

    try:
        stem = pattern.format(**env)
    except Exception:
        stem = safe_key

    stem = _sanitize_filename(stem)

    if ext and not stem.endswith(ext):
        stem = f"{stem}{ext}"
    if not Path(stem).suffix:
        stem = f"{stem}.jpg"

    return stem
