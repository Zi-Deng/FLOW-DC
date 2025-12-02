#!/usr/bin/env python3
"""
Modular download script for downloading a single URL.
Handles format-specific logic, file naming, and download methods.
"""

import os
import json
import pandas as pd
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
        class_name: Raw class name from data
        
    Returns:
        Sanitized class name safe for filesystem
    """
    if pd.isna(class_name):
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


# def generate_filename_from_pattern(image_url, base_url, original_ext, pattern="{segment[-2]}"):
#     """
#     Generate filename from URL using a safe template pattern.
    
#     Available placeholders:
#         {segment[n]}  - URL path segment at index n (supports negative indexing)
#         {segment[-1]} - Last segment (filename)
#         {segment[-2]} - Second to last segment
#         {hash}        - MD5 hash of full URL
#         {hash[:8]}    - First 8 chars of hash
#         {uuid}        - Extract UUID-like pattern from URL
#         {path}        - Full sanitized path
        
#     Args:
#         image_url: Full image URL
#         base_url: URL without extension (from os.path.splitext)
#         original_ext: File extension (e.g., '.jpg')
#         pattern: Template string with placeholders
        
#     Returns:
#         Generated filename with extension
#     """
#     # Parse URL and get segments
#     parsed = urlparse(image_url)
#     segments = [s for s in parsed.path.split('/') if s]  # Remove empty strings
    
#     # Prepare safe variables for substitution
#     variables = {
#         'hash': hashlib.md5(image_url.encode()).hexdigest(),
#         'uuid': extract_uuid_from_url(image_url),
#         'path': '_'.join(segments) if segments else 'file',
#     }
    
#     # Process the pattern
#     result = pattern
    
#     # Handle {segment[index]} patterns with regex
#     def replace_segment(match):
#         index_str = match.group(1)
#         try:
#             index = int(index_str)
#             if -len(segments) <= index < len(segments):
#                 return segments[index]
#             else:
#                 # Index out of range, use fallback
#                 return segments[-1] if segments else 'file'
#         except (ValueError, IndexError):
#             return 'file'
    
#     # Replace {segment[n]} patterns
#     result = re.sub(r'\{segment\[(-?\d+)\]\}', replace_segment, result)
    
#     # Handle slicing like {hash[:8]}
#     def replace_slice(match):
#         var_name = match.group(1)
#         slice_part = match.group(2)
        
#         if var_name not in variables:
#             return ''
        
#         value = variables[var_name]
        
#         # Parse slice notation
#         if slice_part:
#             try:
#                 # Handle [:n] or [n:] or [n:m]
#                 slice_match = re.match(r'\[(\d*):(\d*)\]', slice_part)
#                 if slice_match:
#                     start = int(slice_match.group(1)) if slice_match.group(1) else None
#                     end = int(slice_match.group(2)) if slice_match.group(2) else None
#                     return value[start:end]
#             except:
#                 return value
#         return value
    
#     # Replace {variable[:n]} patterns
#     result = re.sub(r'\{(\w+)(\[[\d:]*\])?\}', replace_slice, result)
    
#     # Sanitize the result (remove invalid filename characters)
#     result = sanitize_filename(result)
    
#     # Ensure we have a valid filename
#     if not result or result == '.':
#         result = hashlib.md5(image_url.encode()).hexdigest()[:12]
    
#     return f"{result}{original_ext}"


# def extract_uuid_from_url(url):
#     """Extract UUID-like pattern from URL (8+ alphanumeric chars)."""
#     # Look for UUID patterns (like 'a1b2c3d4' or longer)
#     matches = re.findall(r'[a-f0-9]{8,}', url.lower())
#     return matches[0] if matches else hashlib.md5(url.encode()).hexdigest()[:12]


# def sanitize_filename(filename):
    """Remove or replace invalid filename characters."""
    # Replace invalid characters with underscore
    filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    # Remove control characters
    filename = ''.join(c for c in filename if ord(c) >= 32)
    # Collapse multiple underscores
    filename = re.sub(r'_+', '_', filename)
    # Remove leading/trailing dots and underscores
    filename = filename.strip('._')
    # Limit length to 200 chars
    filename = filename[:200]
    return filename


def determine_file_path(output_folder, output_format, class_name, filename):
    """
    Determine the full file path based on output format.
    
    Args:
        output_folder: Base output folder
        output_format: 'imagefolder' or 'webdataset'
        class_name: Class/label name
        filename: Generated filename
        
    Returns:
        Full file path
    """
    if output_format == "imagefolder":
        return os.path.join(output_folder, class_name, filename)
    elif output_format == "webdataset":
        return os.path.join(output_folder, filename)
    else:
        # Default to imagefolder
        return os.path.join(output_folder, class_name, filename)


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


def save_webdataset(content, file_path, key, image_url, class_name, total_bytes):
    """
    Save content for webdataset format (includes JSON metadata).
    
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
        
        # Create JSON metadata file
        json_path = file_path.rsplit('.', 1)[0] + ".json"
        with open(json_path, 'w') as f:
            json.dump({
                'key': key,
                'image_url': image_url,
                'class_name': class_name,
            }, f)
        
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
        Tuple of (content: bytes, status_code: int, error: str)
    """
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
            if response.status == 200:
                content = await response.read()
                return content, response.status, None
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
        Tuple of (key, file_path, class_name, error, status_code)
        - key: Row identifier
        - file_path: Full path to saved file (or None if error)
        - class_name: Sanitized class name
        - error: Error message (or None if success)
        - status_code: HTTP status code (or None if other error)
    """
    # Sanitize class name
    class_name = sanitize_class_name(class_name)
    
    # Validate URL
    if pd.isna(url) or not str(url).strip():
        return key, None, class_name, "Invalid image URL", None
    
    url = str(url).strip()
    

    # FILE NAMING
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
    
    # Determine file path
    file_path = determine_file_path(output_folder, output_format, class_name, filename)

    
    # Acquire rate limit token if needed
    if token_bucket and enable_rate_limiting:
        await token_bucket.acquire()

    # DOWNLOAD
    content, status_code, error = await download_via_http_get(session, url, timeout)

    # Handle download result
    if error or content is None:
        return key, file_path, class_name, error, status_code
    
    # Save based on output format
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
        return key, file_path, class_name, None, status_code
    else:
        return key, file_path, class_name, save_error, status_code

def load_input_file(file_path, file_format=None):
    """
    Load input file in various formats.
    
    Args:
        file_path: Path to input file
        file_format: Optional format hint ('parquet', 'csv', 'excel', 'xml')
                    If None, inferred from file extension
        
    Returns:
        pandas DataFrame
        
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
            return pd.read_parquet(file_path)
        elif file_format == "csv":
            return pd.read_csv(file_path)
        elif file_format == "excel":
            return pd.read_excel(file_path)
        elif file_format == "xml":
            return pd.read_xml(file_path)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
    except Exception as e:
        raise Exception(f"Failed to read input file {file_path}: {e}")


def extract_url_and_label(row, url_col, label_col):
    """
    Extract URL and label from a dataframe row.
    
    Args:
        row: pandas Series (dataframe row)
        url_col: Column name containing URLs
        label_col: Column name containing labels
        
    Returns:
        Tuple of (url, label, key)
    """
    key = row.name
    url = row[url_col] if url_col in row else None
    label = row[label_col] if label_col in row else None
    return url, label, key


# Standalone main for testing single URL download
async def main_single():
    """
    Standalone main function for testing single URL download.
    Can be called directly for debugging/testing.
    """
    # import argparse
    
    # parser = argparse.ArgumentParser(description="Download a single URL")
    # parser.add_argument("--url", type=str, required=True, help="URL to download")
    # parser.add_argument("--output", type=str, required=True, help="Output folder")
    # parser.add_argument("--output_format", type=str, default="imagefolder", choices=["imagefolder", "webdataset"])
    # parser.add_argument("--class_name", type=str, default="test", help="Class name")
    # parser.add_argument("--timeout", type=int, default=30, help="Timeout in seconds")
    # parser.add_argument("--file_name_pattern", type=str, default="{segment[-2]}", help="Filename pattern")
    # parser.add_argument("--download_method", type=str, default="http_get", choices=["http_get", "hf_api", "aws_api"])
    
    # args = parser.parse_args()

    url = "https://api.gbif.org/v1/image/unsafe/https%3A%2F%2Fmoth.tbn.org.tw%2Fimages%2Ftwmoth003%2Ftwmoth012846.jpg"
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
            file_name_pattern=file_name_pattern,
            download_method=download_method
        )
        
        key, file_path, class_name, error, status_code = result
        
        if error:
            print(f"Error: {error} (Status: {status_code})")
        else:
            print(f"Success: Saved to {file_path}")


if __name__ == "__main__":
    asyncio.run(main_single())

