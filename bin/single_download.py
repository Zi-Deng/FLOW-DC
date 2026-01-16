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
import aiohttp
import polars as pl
from urllib.parse import urlparse, unquote
from http import HTTPStatus
from typing import Optional, Tuple, List


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
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'wb') as f:
            f.write(content)
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
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'wb') as f:
            f.write(content)
        file_size = os.path.getsize(file_path)
        total_bytes.append(file_size)

        # Create JSON metadata file
        json_path = file_path.rsplit('.', 1)[0] + ".json"
        metadata = {
            'key': key,
            'url': image_url,
        }
        # Only include class_name if it's not None
        if class_name is not None:
            metadata['class_name'] = class_name

        with open(json_path, 'w') as f:
            json.dump(metadata, f)

        return True, None
    except Exception as e:
        return False, str(e)


async def download_via_http_get(
    session: aiohttp.ClientSession,
    url: str,
    timeout: int
) -> Tuple[Optional[bytes], Optional[int], Optional[str], Optional[float]]:
    """
    Download content via standard HTTP GET request.

    Args:
        session: aiohttp ClientSession
        url: URL to download
        timeout: Request timeout in seconds

    Returns:
        Tuple of (content: bytes, status_code: int, error: str, retry_after: float)
    """
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
            # Extract Retry-After header if present
            retry_after = None
            if 'Retry-After' in response.headers:
                try:
                    retry_after = float(response.headers['Retry-After'])
                except ValueError:
                    pass

            if response.status == 200:
                content = await response.read()
                return content, response.status, None, retry_after
            else:
                try:
                    status_name = HTTPStatus(response.status).phrase
                except ValueError:
                    status_name = "Unknown"
                return None, response.status, f"HTTP {response.status}: {status_name}", retry_after

    except asyncio.TimeoutError:
        return None, 408, "Request Timeout", None
    except aiohttp.ClientError as e:
        return None, None, f"Connection Error: {str(e)}", None
    except Exception as e:
        return None, None, f"Error: {str(e)}", None


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

    # Download content
    content, status_code, error, retry_after = await download_via_http_get(session, url, timeout)

    # Handle download failure
    if content is None:
        return key, file_path, class_name, error, status_code, retry_after

    # Initialize tracking list if needed
    if total_bytes is None:
        total_bytes = []

    # Save based on output format
    if output_format == "imagefolder":
        success, save_error = save_imagefolder(content, file_path, key, url, class_name, total_bytes)
    elif output_format == "webdataset":
        success, save_error = save_webdataset(content, file_path, key, url, class_name, total_bytes)
    else:
        # Default to imagefolder
        success, save_error = save_imagefolder(content, file_path, key, url, class_name, total_bytes)

    if success:
        return key, file_path, class_name, None, status_code, retry_after
    else:
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
