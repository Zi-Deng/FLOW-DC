#!/usr/bin/env python3
"""
FLOW-DC Single Download Module

Modular download functions for downloading individual URLs.
Handles format-specific logic, file naming, and output formats.

This module is used by download_batch.py and provides:
- load_input_file(): Multi-format data loader (parquet, csv, excel, xml)
- download_single(): Core async download function
- extract_extension(): URL extension extraction
- Output format handlers (imagefolder, webdataset)
"""

import os
import json
import asyncio
import math
import time
import weakref
import aiohttp
import polars as pl
from contextvars import ContextVar
from datetime import timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse, unquote
from http import HTTPStatus
from typing import Optional, Tuple, List
from yarl import URL
from flowdc_integrity import component, safe_save


# Shared with both asynchronous batch variants.
HTTP_TRACE_CTX: ContextVar[dict | None] = ContextVar("HTTP_TRACE_CTX", default=None)
OUTPUT_CTX: ContextVar[tuple | None] = ContextVar("OUTPUT_CTX", default=None)
HTTP_MEASUREMENT_VERSION = "3-output-independent-latency"


def http_authority(url) -> tuple[str, int]:
    """Normalized hostname and effective port; schemes share an explicit port.

    Default HTTP/HTTPS ports (80/443) remain distinct. Userinfo and paths are not
    part of the key. YARL supplies the same IDNA/IP normalization as aiohttp.
    """
    parsed = URL(url)
    if parsed.scheme not in ("http", "https") or parsed.raw_host is None:
        raise ValueError("HTTP(S) authority required")
    return parsed.raw_host.lower(), parsed.port


def parse_retry_after(value: Optional[str], wall_time: Optional[float] = None) -> Optional[float]:
    """Seconds until admission, accepting finite nonnegative floats for compatibility.

    RFC delay-seconds are integers; fractional/exponent forms retain the former
    float parser's behavior. HTTP dates are converted against wall time only here.
    """
    if value is None:
        return None
    try:
        delay = float(value)
    except (ValueError, TypeError, OverflowError):
        try:
            date = parsedate_to_datetime(value)
            if date.tzinfo is None:
                date = date.replace(tzinfo=timezone.utc)
            delay = max(0.0, date.timestamp() - (time.time() if wall_time is None else wall_time))
        except (ValueError, TypeError, OverflowError, OSError):
            return None
    return delay if math.isfinite(delay) and delay >= 0 else None


class RetryAfterGate:
    """One event-loop/session's authority embargoes, independent of PAARC.

    Updates contain no await, so concurrent responses cannot lose an extension.
    Sleeps hold no lock and always recheck the current deadline. In-flight requests
    are not recalled. This is neither a cross-process nor a cross-VM gate.
    """

    def __init__(self, *, clock=time.monotonic, wall_clock=time.time, sleep=asyncio.sleep):
        self._clock = clock
        self._wall_clock = wall_clock
        self._sleep = sleep
        self._deadlines: dict[tuple[str, int], float] = {}

    def observe(self, url, value: Optional[str]) -> Optional[float]:
        delay = parse_retry_after(value, self._wall_clock())
        if delay is None:
            return None
        deadline = self._clock() + delay
        if not math.isfinite(deadline):
            return None
        authority = http_authority(url)
        self._deadlines[authority] = max(self._deadlines.get(authority, 0.0), deadline)
        return delay

    async def wait(self, url) -> None:
        authority = http_authority(url)
        while True:
            remaining = self._deadlines.get(authority, 0.0) - self._clock()
            if remaining <= 0:
                self._deadlines.pop(authority, None)
                return
            await self._sleep(remaining)


_SESSION_GATES = weakref.WeakKeyDictionary()


def session_http_gate(session: aiohttp.ClientSession) -> RetryAfterGate:
    """Get the single shared gate and install dispatch hooks once per session.

    ClientSession exposes its trace_configs list. A helper caller's session may
    omit our config; append a frozen config before its first request is created.
    Existing configs and already-dispatched requests remain intact.
    """
    gate = _SESSION_GATES.get(session)
    if gate is None:
        gate = RetryAfterGate()
        _SESSION_GATES[session] = gate
    if not any(isinstance(trace, HTTPTraceConfig) for trace in session.trace_configs):
        trace = HTTPTraceConfig()
        trace.freeze()
        session.trace_configs.append(trace)
    return gate


class HTTPTraceConfig(aiohttp.TraceConfig):
    """Observe headers/redirects and recheck admission at the send boundary."""

    def __init__(self):
        super().__init__()
        self.on_request_start.append(self._start)
        self.on_request_headers_sent.append(self._dispatch)
        self.on_request_redirect.append(self._redirect)
        self.on_request_end.append(self._end)

    async def _start(self, session, ctx, params):
        ctx.measurement = HTTP_TRACE_CTX.get()

    async def _admit(self, session, ctx, url):
        if ctx.measurement is not None:
            ctx.measurement.update(phase="admission", feedback_url=str(url))
        await session_http_gate(session).wait(url)
        if ctx.measurement is not None:
            ctx.measurement["phase"] = "request"

    async def _dispatch(self, session, ctx, params):
        # aiohttp calls this before writing request headers, after connector/DNS
        # waits, and again for each redirect hop. A new embargo must win here.
        await self._admit(session, ctx, params.url)
        if ctx.measurement is not None and ctx.measurement.get("dispatch_check") is not None:
            ctx.measurement["dispatch_check"]()
        if ctx.measurement is not None:
            now = time.monotonic()
            ctx.measurement["t0"] = now
            ctx.measurement["final_url"] = str(params.url)
            ctx.measurement.setdefault("hops", []).append({
                "url": str(params.url), "dispatch_at": now,
            })

    def _headers(self, session, ctx, response, *, final):
        # A 3xx without Location reaches both redirect and end callbacks. Observe
        # its header once; rereading a delay-seconds value would extend it twice.
        if getattr(ctx, "observed_response", None) is response:
            now, retry_after = ctx.observed_headers
        else:
            now = time.monotonic()
            retry_after = session_http_gate(session).observe(response.url, response.headers.get("Retry-After"))
            ctx.observed_response = response
            ctx.observed_headers = now, retry_after
        if ctx.measurement is not None:
            d = ctx.measurement
            if d.get("hops"):
                d["hops"][-1].update(headers_at=now, status=response.status, retry_after=retry_after)
            if final:
                d.update(final_headers_at=now, final_url=str(response.url), retry_after=retry_after)
        return retry_after

    async def _redirect(self, session, ctx, params):
        response = params.response
        retry_after = self._headers(session, ctx, response, final=False)
        location = response.headers.get("Location") or response.headers.get("URI")
        if location is None:
            return  # aiohttp returns this 3xx as a final response.

        # aiohttp normally releases the redirect response after this callback.
        # Release it before our waits so timeout/cancellation cannot strand its
        # connection. Redirect bodies are not acquisition bodies.
        response.release()
        if retry_after is not None:
            # RFC 9110 10.2.3 also delays this chain's follow-up, even to another
            # authority. Do not install the source's embargo on the destination.
            await self._admit(session, ctx, response.url)

        if ctx.measurement is not None and ctx.measurement.get("redirect_admit") is not None:
            # Resolve only for admission; aiohttp still owns redirect validation,
            # limits and auth/cookie handling. Match its public requote setting.
            try:
                destination = URL(location, encoded=not session.requote_redirect_url)
                if not destination.scheme:
                    destination = response.url.join(destination)
                http_authority(destination)
            except ValueError:
                return  # aiohttp will report the invalid redirect.
            ctx.measurement.update(phase="admission", feedback_url=str(destination))
            await ctx.measurement["redirect_admit"](str(destination))
            await self._admit(session, ctx, destination)

    async def _end(self, session, ctx, params):
        self._headers(session, ctx, params.response, final=True)


def sanitize_class_name(class_name) -> str:
    """
    Clean class name for filesystem compatibility.

    Args:
        class_name: Raw class name from data (can be None)

    Returns:
        Sanitized class name safe for filesystem, or "output" if None
    """
    if class_name is None:
        return "output"
    if isinstance(class_name, float) and class_name != class_name:  # NaN check
        return "unknown"
    return str(class_name).replace("'", "").replace('"', "").replace(" ", "_").replace("/", "_")


def extract_extension(url: str) -> Tuple[str, str]:
    """
    Extract file extension from URL, handling query parameters.

    Args:
        url: Image URL

    Returns:
        Tuple of (base_url, extension) where extension includes the dot
    """
    # Parse URL to handle query parameters properly
    parsed = urlparse(str(url))
    clean_path = parsed.path
    base_url, original_ext = os.path.splitext(clean_path)

    # If no extension in path, check if URL has one
    if not original_ext:
        # Try to get extension from full URL (before query params)
        full_base, full_ext = os.path.splitext(str(url).split('?')[0])
        if full_ext:
            original_ext = full_ext

    return base_url, original_ext


def determine_file_path(output_folder: str, output_format: str, class_name: Optional[str], filename: str) -> str:
    """
    Determine the full file path based on output format.

    Args:
        output_folder: Base output folder
        output_format: 'imagefolder' or 'webdataset'
        class_name: Class/label name (can be None, will use "output" for imagefolder)
        filename: Generated filename

    Returns:
        Full file path
    """
    if output_format == "imagefolder":
        # Use "output" folder if class_name is None
        folder_name = class_name if class_name is not None else "output"
        return os.path.join(output_folder, folder_name, filename)
    elif output_format == "webdataset":
        return os.path.join(output_folder, filename)
    else:
        # Default to imagefolder
        folder_name = class_name if class_name is not None else "output"
        return os.path.join(output_folder, folder_name, filename)


def save_imagefolder(content: bytes, file_path: str, key: str, image_url: str,
                     class_name: str, total_bytes: List[int]) -> Tuple[bool, Optional[str]]:
    """
    Save content for imagefolder format.

    Args:
        content: File content bytes
        file_path: Full path to save file
        key: Row key/identifier
        image_url: Original image URL
        class_name: Class name
        total_bytes: List to append file size to

    Returns:
        Tuple of (success: bool, error: str or None)
    """
    try:
        safe_save(content, file_path)
        file_size = os.path.getsize(file_path)
        total_bytes.append(file_size)
        return True, None
    except Exception as e:
        return False, str(e)


def save_webdataset(content: bytes, file_path: str, key: str, image_url: str,
                    class_name: Optional[str], total_bytes: List[int]) -> Tuple[bool, Optional[str]]:
    """
    Save content for webdataset format (includes JSON metadata).

    Args:
        content: File content bytes
        file_path: Full path to save file
        key: Row key/identifier
        image_url: Original image URL
        class_name: Class name (can be None, will be omitted from JSON if None)
        total_bytes: List to append file size to

    Returns:
        Tuple of (success: bool, error: str or None)
    """
    try:
        json_path = file_path.rsplit('.', 1)[0] + ".json"
        if os.path.lexists(json_path):
            raise FileExistsError("Metadata destination already exists")
        if json_path == file_path:
            raise ValueError("Payload and metadata destinations collide")
        safe_save(content, file_path)
        file_size = os.path.getsize(file_path)
        # Create JSON metadata file
        json_path = file_path.rsplit('.', 1)[0] + ".json"
        metadata = {
            'key': key,
            'url': image_url,
        }
        # Only include class_name if it's not None
        if class_name is not None:
            metadata['class_name'] = class_name

        safe_save(json.dumps(metadata).encode(), json_path)

        total_bytes.append(file_size)
        return True, None
    except Exception as e:
        return False, str(e)


async def download_via_http_get(
    session: aiohttp.ClientSession,
    url: str,
    timeout: int
) -> Tuple[Optional[bytes], Optional[int], Optional[str], Optional[float]]:
    """Return (body, status, error, retry_after), preserving the helper interface.

    A positive total timeout includes admission, connection, all redirect hops and
    body reads. Zero/negative retains aiohttp's unbounded setting. Timing uses
    time.monotonic; t0/ttfb refer only to the final dispatched hop, excluding prior
    redirects and admission/connector waits. See docs/research/HTTP-MEASUREMENT.md.
    """
    measurement = HTTP_TRACE_CTX.get()
    if measurement is None:
        measurement = {}
    measurement.update(
        attempt_started_at=time.monotonic(), t0=None, final_headers_at=None,
        first_body_byte_at=None, body_completed_at=None, ttfb=None, hops=[],
        latency_eligible=False, failure_kind=None, retry_after=None, feedback_url=url,
        observed_response_body_bytes=0,
    )
    token = HTTP_TRACE_CTX.set(measurement)

    async def fetch():
        measurement["phase"] = "admission"
        await session_http_gate(session).wait(url)
        measurement["phase"] = "request"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
            retry_after = measurement["retry_after"]
            if response.status == 200:
                measurement["phase"] = "body"
                first = await response.content.read(1)
                measurement["observed_response_body_bytes"] = len(first)
                if first:
                    measurement["first_body_byte_at"] = time.monotonic()
                tail = await response.content.read()
                measurement["observed_response_body_bytes"] += len(tail)
                content = first + tail
                measurement["body_completed_at"] = time.monotonic()
                if first and measurement["t0"] is not None:
                    measurement["ttfb"] = measurement["first_body_byte_at"] - measurement["t0"]
                    measurement["latency_eligible"] = math.isfinite(measurement["ttfb"]) and measurement["ttfb"] > 0
                return content, response.status, None, retry_after
            measurement["failure_kind"] = "http"
            try:
                status_name = HTTPStatus(response.status).phrase
            except ValueError:
                status_name = "Unknown"
            return None, response.status, f"HTTP {response.status}: {status_name}", retry_after

    try:
        # wait_for also bounds admission before aiohttp's own timer is entered,
        # and preserves the existing product's Python 3.10 compatibility.
        return await asyncio.wait_for(fetch(), timeout if timeout > 0 else None)
    except asyncio.TimeoutError:
        measurement.update(ttfb=None, latency_eligible=False)
        measurement["failure_kind"] = "admission" if measurement.get("phase") == "admission" else "transport"
        return None, 408, "Request Timeout", None
    except aiohttp.ClientError as e:
        measurement.update(ttfb=None, latency_eligible=False)
        measurement["failure_kind"] = "transport"
        return None, None, f"Connection Error: {str(e)}", None
    except Exception as e:
        measurement.update(ttfb=None, latency_eligible=False)
        # An unexpected acquisition exception does not establish either output
        # failure or remote overload. Preserve that uncertainty for accounting.
        measurement["failure_kind"] = "unknown"
        return None, None, f"Error: {str(e)}", None
    except asyncio.CancelledError:
        measurement.update(ttfb=None, latency_eligible=False, failure_kind="cancelled")
        raise
    finally:
        HTTP_TRACE_CTX.reset(token)


def load_input_file(file_path: str, file_format: Optional[str] = None) -> pl.DataFrame:
    """
    Load input file in various formats using Polars.

    Args:
        file_path: Path to input file
        file_format: Optional format hint ('parquet', 'csv', 'excel', 'xml')
                    If None, inferred from file extension

    Returns:
        Polars DataFrame

    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If format is unsupported
        Exception: If file reading fails
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Input file {file_path} not found")

    # Determine format from extension if not provided
    if file_format is None:
        if file_path.endswith(".parquet"):
            file_format = "parquet"
        elif file_path.endswith(".csv") or file_path.endswith(".txt"):
            file_format = "csv"
        elif file_path.endswith(".xlsx") or file_path.endswith(".xls"):
            file_format = "excel"
        elif file_path.endswith(".xml"):
            file_format = "xml"
        else:
            raise ValueError(f"Could not determine file format from extension: {file_path}")

    try:
        if file_format == "parquet":
            return pl.read_parquet(file_path)
        elif file_format == "csv":
            return pl.read_csv(file_path)
        elif file_format == "excel":
            return pl.read_excel(file_path)
        elif file_format == "xml":
            # Polars doesn't have native XML support, use pandas as fallback
            import pandas as pd
            pdf = pd.read_xml(file_path)
            return pl.from_pandas(pdf)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
    except Exception as e:
        raise Exception(f"Failed to read input file {file_path}: {e}")


async def download_single(
    # Core identifiers
    url: str,
    key: str,
    class_name: Optional[str],
    # Output config
    output_folder: str,
    output_format: str,
    # Network/session
    session: aiohttp.ClientSession,
    timeout: int,
    # Filename (optional - if not provided, derived from URL)
    filename: Optional[str] = None,
    # Tracking (optional)
    total_bytes: Optional[List[int]] = None
) -> Tuple[str, Optional[str], str, Optional[str], Optional[int], Optional[float]]:
    """
    Download a single URL and save it according to output format.

    This is the core download function used by download_batch.py. It handles:
    - URL validation
    - File naming (from provided filename or URL)
    - HTTP GET download
    - Output format-specific saving (imagefolder or webdataset)

    Args:
        url: URL to download
        key: Row key/identifier for tracking
        class_name: Class/label name (will be sanitized, can be None)
        output_folder: Base output folder
        output_format: 'imagefolder' or 'webdataset'
        session: aiohttp ClientSession
        timeout: Request timeout in seconds
        filename: Optional pre-generated filename (if None, derived from URL)
        total_bytes: Optional list to track downloaded file sizes

    Returns:
        Tuple of (key, file_path, class_name, error, status_code, retry_after_sec)
        - key: Row identifier (for matching with input)
        - file_path: Full path to saved file (or None if error)
        - class_name: Sanitized class name
        - error: Error message (or None if success)
        - status_code: HTTP status code (or None if connection error)
        - retry_after_sec: Retry-After header value if present (for 429 responses)
    """
    # Reject unsafe labels before sanitization can hide an escape or collision.
    try:
        if class_name is not None:
            component(str(class_name))
        if filename is not None:
            component(filename)
    except ValueError as exc:
        return key, None, class_name, str(exc), None, None
    # Sanitize class name
    class_name = sanitize_class_name(class_name)

    # Validate URL
    if url is None or not str(url).strip():
        return key, None, class_name, "Invalid or empty URL", None, None

    url = str(url).strip()

    # Determine filename
    if filename is None:
        # Extract extension and generate filename from URL
        base_url, original_ext = extract_extension(url)

        # Determine extension (use .jpg if no extension found)
        if not original_ext:
            filename = f"{base_url.split('/')[-1]}.jpg"
        else:
            filename = f"{base_url.split('/')[-1]}{original_ext}"

    # Determine file path
    file_path = determine_file_path(output_folder, output_format, class_name, filename)
    try:
        component(filename)
    except (OSError, ValueError) as exc:
        return key, None, class_name, str(exc), None, None

    # Download content
    content, status_code, error, retry_after = await download_via_http_get(session, url, timeout)

    # Handle download failure
    if content is None:
        return key, file_path, class_name, error, status_code, retry_after

    # Initialize tracking list if needed
    if total_bytes is None:
        total_bytes = []

    # Save based on output format
    publication = OUTPUT_CTX.get()
    if publication is not None:
        store, directory, row_id = publication
        try:
            file_path = store.publish(directory, row_id, content)
            total_bytes.append(len(content))
            success, save_error = True, None
        except (OSError, ValueError) as exc:
            success, save_error = False, str(exc)
    elif output_format == "imagefolder":
        success, save_error = save_imagefolder(content, file_path, key, url, class_name, total_bytes)
    elif output_format == "webdataset":
        success, save_error = save_webdataset(content, file_path, key, url, class_name, total_bytes)
    else:
        # Default to imagefolder
        success, save_error = save_imagefolder(content, file_path, key, url, class_name, total_bytes)

    if success:
        return key, file_path, class_name, None, status_code, retry_after
    else:
        measurement = HTTP_TRACE_CTX.get()
        if measurement is not None:
            # A completed body observation remains valid when local publication
            # fails. Saved-output success and useful bytes are separate outcomes.
            measurement["failure_kind"] = "local"
        return key, file_path, class_name, save_error, status_code, retry_after


# Standalone main for testing single URL download
async def main_single():
    """
    Standalone main function for testing single URL download.
    Can be called directly for debugging/testing.
    """
    # Test URL (use a reliable test image)
    url = "https://httpbin.org/image/jpeg"
    output_folder = "files/output/test"
    output_format = "imagefolder"
    class_name = "test_class"
    timeout = 30

    # Create output directory
    os.makedirs(output_folder, exist_ok=True)

    # Create session
    async with aiohttp.ClientSession() as session:
        result = await download_single(
            url=url,
            key="test_key_001",
            class_name=class_name,
            output_folder=output_folder,
            output_format=output_format,
            session=session,
            timeout=timeout,
            filename="test_image.jpg"
        )

        key, file_path, class_name, error, status_code, retry_after = result

        if error:
            print(f"Error: {error} (Status: {status_code})")
        else:
            print(f"Success: Saved to {file_path}")
            print(f"  Key: {key}")
            print(f"  Class: {class_name}")
            print(f"  Status: {status_code}")


if __name__ == "__main__":
    asyncio.run(main_single())
