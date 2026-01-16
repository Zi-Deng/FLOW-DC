#!/usr/bin/env python3
"""
Modular download script for downloading a single URL.
Handles format-specific logic, file naming, and download methods.
"""

import os
import json
import math
import polars as pl
import aiohttp
import asyncio
from urllib.parse import urlparse, unquote
from http import HTTPStatus
import re
import hashlib


def sanitize_class_name(class_name):
    """
    Clean class name for filesystem compatibility.

    Args:
        class_name: Raw class name from data (can be None)

    Returns:
        Sanitized class name safe for filesystem, or "output" if None
    """
    if class_name is None:
        return "output"
    # Check for NaN (works for both float NaN and None)
    if isinstance(class_name, float) and math.isnan(class_name):
        return "unknown"
    return str(class_name).replace("'", "").replace('"', "").replace(" ", "_").replace("/", "_")


def extract_extension(url):
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


def determine_file_path(output_folder, output_format, class_name, filename):
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


def save_imagefolder(content, file_path, key, image_url, class_name, total_bytes):
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
        Tuple of (success: bool, error: str)
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

def save_hf_parquet(content, file_path, total_bytes):
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'wb') as f:
            f.write(content)
        file_size = os.path.getsize(file_path)
        total_bytes.append(file_size)
        return True, None
    except Exception as e:
        return False, str(e)


def save_webdataset(content, file_path, key, image_url, class_name, total_bytes):
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
        Tuple of (success: bool, error: str)
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
            'image_url': image_url,
        }
        # Only include class_name if it's not None
        if class_name is not None:
            metadata['class_name'] = class_name
        
        with open(json_path, 'w') as f:
            json.dump(metadata, f)
        
        return True, None
    except Exception as e:
        return False, str(e)


async def download_via_http_get(session, url, timeout):
    """
    Download content via standard HTTP GET request.

    Args:
        session: aiohttp ClientSession
        url: URL to download
        timeout: Request timeout in seconds

    Returns:
        Tuple of (content: bytes, status_code: int, error: str, retry_after_sec: float|None)
        retry_after_sec is extracted from Retry-After header on 429 responses
    """
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
            if response.status == 200:
                content = await response.read()
                return content, response.status, None, None
            else:
                try:
                    status_name = HTTPStatus(response.status).phrase
                except ValueError:
                    status_name = "Unknown"

                # Extract Retry-After header if present (for 429 responses)
                retry_after_sec = None
                if response.status == 429:
                    retry_after = response.headers.get("Retry-After")
                    if retry_after:
                        try:
                            # Retry-After can be seconds (int) or HTTP-date
                            retry_after_sec = float(retry_after)
                        except ValueError:
                            # Could be HTTP-date format, ignore for now
                            pass

                return None, response.status, f"HTTP Error: {status_name}", retry_after_sec
    except asyncio.TimeoutError as e:
        return None, 408, str(e), None
    except Exception as e:
        return None, None, str(e), None

async def download_hf_parquet(session, url, timeout):
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
            if response.status == 200:
                content = await response.read()
            else:
                try:
                    status_name = HTTPStatus(response.status).phrase
                except ValueError:
                    status_name = "Unknown"
                return None, response.status, f"HTTP Error: {status_name}"
    except asyncio.TimeoutError as e:
        return None, 408, str(e)
    except Exception as e:
        return None, None, str(e)


async def download_single(
    # Core identifiers
    url, key, class_name,
    # Output config
    output_folder, output_format,
    # Network/session
    session, timeout,
    filename=None,
    # Rate limiting (optional)
    token_bucket=None, enable_rate_limiting=False,
    # Tracking
    total_bytes=None
):
    """
    Download a single URL and save it according to output format.

    Args:
        url: Image URL to download
        key: Row key/identifier
        class_name: Class/label name (will be sanitized)
        output_folder: Base output folder
        output_format: 'imagefolder' or 'webdataset'
        session: aiohttp ClientSession
        timeout: Request timeout in seconds
        file_name_pattern: Pattern for filename generation (default: "{segment[-2]}")
        filename: Optional pre-generated filename (if provided, skips URL-based generation)
        download_method: Download method ('http_get', 'hf_api', 'aws_api')
        token_bucket: Optional TokenBucket for rate limiting
        enable_rate_limiting: Whether rate limiting is enabled
        total_bytes: Optional list to track file sizes

    Returns:
        Tuple of (key, file_path, class_name, error, status_code, retry_after_sec)
        - key: Row identifier
        - file_path: Full path to saved file (or None if error)
        - class_name: Sanitized class name
        - error: Error message (or None if success)
        - status_code: HTTP status code (or None if other error)
        - retry_after_sec: Retry-After header value in seconds (for 429 responses)
    """
    #########################################################
    # Sanitize class name
    #########################################################
    class_name = sanitize_class_name(class_name)
    
    #########################################################
    # Validate URL
    #########################################################
    if url is None or (isinstance(url, float) and math.isnan(url)) or not str(url).strip():
        return key, None, class_name, "Invalid image URL", None, None

    url = str(url).strip()
    

    #########################################################
    # FILE NAMING
    #########################################################
    # Use provided filename or generate from URL
    if filename is not None:
        # Use the provided filename directly
        pass
    else:
        # Extract extension
        base_url, original_ext = extract_extension(url)
        
        # Determine extension (use .png if no extension found)
        if not original_ext:
            filename = f"{base_url.split('/')[-1]}.png"
        else:
            filename = f"{base_url.split('/')[-1]}{original_ext}"
    
    #########################################################
    # Determine file path
    #########################################################
    file_path = determine_file_path(output_folder, output_format, class_name, filename)

    
    #########################################################
    # Acquire rate limit token if needed
    #########################################################
    if token_bucket and enable_rate_limiting:
        await token_bucket.acquire()

    #########################################################
    # DOWNLOAD
    #########################################################
    content, status_code, error, retry_after_sec = await download_via_http_get(session, url, timeout)

    #########################################################
    # Handle download result
    #########################################################
    if error or content is None:
        return key, file_path, class_name, error, status_code, retry_after_sec

    #########################################################
    # Save based on output format
    #########################################################
    if total_bytes is None:
        total_bytes = []

    if output_format == "imagefolder":
        success, save_error = save_imagefolder(content, file_path, key, url, class_name, total_bytes)
    elif output_format == "webdataset":
        success, save_error = save_webdataset(content, file_path, key, url, class_name, total_bytes)
    else:
        # Default to imagefolder
        success, save_error = save_imagefolder(content, file_path, key, url, class_name, total_bytes)

    if success:
        return key, file_path, class_name, None, status_code, None
    else:
        return key, file_path, class_name, save_error, status_code, None

def load_input_file(file_path, file_format=None):
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


# Standalone main for testing single URL download
async def main_single():
    """
    Standalone main function for testing single URL download.
    Can be called directly for debugging/testing.
    """
    #url = "https://api.gbif.org/v1/image/unsafe/https%3A%2F%2Fmoth.tbn.org.tw%2Fimages%2Ftwmoth003%2Ftwmoth012846.jpg"
    url = "https://huggingface.co/datasets/ILSVRC/imagenet-1k/resolve/main/data/test-00000-of-00028.parquet"
    output = "test_output"
    output_format = "imagefolder"
    class_name = "moth"
    timeout = 30
    file_name_pattern = "{segment[-1]}"
    download_method = "http_get"
    
    # Create session
    async with aiohttp.ClientSession() as session:
        result = await download_single(
            url=url,
            key="test_key",
            class_name=class_name,
            output_folder=output,
            output_format=output_format,
            session=session,
            timeout=timeout,
        )

        key, file_path, class_name, error, status_code, retry_after = result

        if error:
            print(f"Error: {error} (Status: {status_code})")
            if retry_after:
                print(f"Retry-After: {retry_after}s")
        else:
            print(f"Success: Saved to {file_path}")


if __name__ == "__main__":
    asyncio.run(main_single())

