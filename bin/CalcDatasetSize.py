#!/usr/bin/env python
"""
FLOW-DC Dataset Size Calculator

Estimates the total download size of a dataset by querying URLs for Content-Length headers.
Useful for planning storage requirements before running batch downloads.

Features:
- Async HTTP HEAD requests for efficient size estimation
- Handles URLs without extensions by checking Content-Type
- Tries alternative file extensions when HEAD fails
- Supports both single parquet files and directories

Usage:
    # Estimate size for a directory of partitions:
    python CalcDatasetSize.py --directory files/input/partitions --url_column url

    # Estimate size for a single parquet file:
    python CalcDatasetSize.py --input files/input/dataset.parquet --url_column photo_url

    # Sample a subset of URLs for faster estimation:
    python CalcDatasetSize.py --directory files/input/partitions --url_column url --sample 1000
"""

import os
import argparse
import asyncio
import aiohttp
import pandas as pd
from urllib.parse import urlsplit, urlunsplit
from typing import Optional


def parse_args():
    parser = argparse.ArgumentParser(
        description='FLOW-DC Dataset Size Calculator - Estimate download size from URL headers',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python CalcDatasetSize.py --directory files/input/partitions --url_column url
  python CalcDatasetSize.py --input files/input/dataset.parquet --url_column photo_url --sample 5000
"""
    )
    parser.add_argument(
        '--directory',
        type=str,
        help='Input directory containing parquet files'
    )
    parser.add_argument(
        '--input',
        type=str,
        help='Single input parquet/csv file'
    )
    parser.add_argument(
        '--url_column',
        type=str,
        default='url',
        help='URL column name (default: url)'
    )
    parser.add_argument(
        '--sample',
        type=int,
        default=None,
        help='Sample N URLs instead of checking all (faster estimation)'
    )
    parser.add_argument(
        '--concurrent',
        type=int,
        default=100,
        help='Number of concurrent requests (default: 100)'
    )
    parser.add_argument(
        '--timeout',
        type=int,
        default=10,
        help='Request timeout in seconds (default: 10)'
    )
    return parser.parse_args()


async def estimate_image_size(
    session: aiohttp.ClientSession,
    url: str,
    timeout: int
) -> Optional[float]:
    """
    Estimate file size from URL using HEAD request.

    Returns:
        Size in KB or None if unable to determine
    """
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout)

        # First try HEAD request
        async with session.head(url, allow_redirects=True, timeout=client_timeout) as response:
            if response.status == 200 and 'Content-Length' in response.headers:
                size_bytes = int(response.headers['Content-Length'])
                return size_bytes / 1024

        # If HEAD fails, try GET with Range header
        async with session.get(
            url,
            headers={'Range': 'bytes=0-1023'},
            allow_redirects=True,
            timeout=client_timeout
        ) as response:
            if response.status in (200, 206) and 'Content-Range' in response.headers:
                content_range = response.headers['Content-Range']
                # Format: "bytes 0-1023/total_size"
                total_size = int(content_range.split('/')[-1])
                return total_size / 1024

        return None
    except Exception:
        return None


async def handle_url_without_extension(
    session: aiohttp.ClientSession,
    url: str,
    timeout: int
) -> Optional[float]:
    """
    Handle URLs that may not have file extensions.
    Checks Content-Type to verify it's an image.
    """
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout)

        async with session.head(url, allow_redirects=True, timeout=client_timeout) as response:
            if response.status == 200:
                content_type = response.headers.get('Content-Type', '')
                if 'image' in content_type and 'Content-Length' in response.headers:
                    size_bytes = int(response.headers['Content-Length'])
                    return size_bytes / 1024

        # Try GET with Range header
        async with session.get(
            url,
            headers={'Range': 'bytes=0-1023'},
            allow_redirects=True,
            timeout=client_timeout
        ) as response:
            if response.status in (200, 206) and 'Content-Range' in response.headers:
                content_range = response.headers['Content-Range']
                total_size = int(content_range.split('/')[-1])
                return total_size / 1024

        return None
    except Exception:
        return None


async def try_different_extensions(
    session: aiohttp.ClientSession,
    url: str,
    timeout: int,
    unresolved_urls: list
) -> float:
    """
    Try to get file size, attempting different extensions if needed.

    Returns:
        Size in KB or 0 if unable to determine
    """
    # First try the URL as-is
    size_kb = await estimate_image_size(session, url, timeout)
    if size_kb is not None:
        return size_kb

    # Try different extensions
    extensions = ['.JPG', '.jpeg', '.JPEG', '.jpg', '.png', '.gif', '.webp', '.tiff']
    url_parts = urlsplit(url)

    for ext in extensions:
        if '.' in url_parts.path:
            base, _ = url_parts.path.rsplit('.', 1)
        else:
            base = url_parts.path

        new_url = urlunsplit((
            url_parts.scheme,
            url_parts.netloc,
            f"{base}{ext}",
            url_parts.query,
            url_parts.fragment
        ))

        size_kb = await estimate_image_size(session, new_url, timeout)
        if size_kb is not None:
            return size_kb

    # Try handling as URL without extension
    size_kb = await handle_url_without_extension(session, url, timeout)
    if size_kb is not None:
        return size_kb

    unresolved_urls.append(url)
    return 0


async def get_total_size(
    urls: list,
    concurrent: int,
    timeout: int,
    show_progress: bool = True
) -> tuple[float, float, float, int]:
    """
    Calculate total size of all URLs.

    Returns:
        Tuple of (total_kb, total_mb, total_gb, unresolved_count)
    """
    unresolved_urls = []
    semaphore = asyncio.Semaphore(concurrent)

    async def bounded_request(session, url):
        async with semaphore:
            return await try_different_extensions(session, url, timeout, unresolved_urls)

    connector = aiohttp.TCPConnector(limit=concurrent)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [bounded_request(session, url) for url in urls]

        if show_progress:
            from tqdm.asyncio import tqdm_asyncio
            sizes_kb = await tqdm_asyncio.gather(*tasks, desc="Estimating sizes")
        else:
            sizes_kb = await asyncio.gather(*tasks)

    total_size_kb = sum(sizes_kb)
    total_size_mb = total_size_kb / 1024
    total_size_gb = total_size_mb / 1024

    return total_size_kb, total_size_mb, total_size_gb, len(unresolved_urls)


def load_urls_from_directory(directory: str, url_column: str) -> pd.DataFrame:
    """Load all parquet files from a directory into a single DataFrame."""
    df = pd.DataFrame()

    for filename in sorted(os.listdir(directory)):
        if filename.endswith(".parquet"):
            filepath = os.path.join(directory, filename)
            try:
                temp_df = pd.read_parquet(filepath)
                df = pd.concat([df, temp_df], ignore_index=True)
                print(f"  Loaded {filename}: {len(temp_df):,} rows")
            except Exception as e:
                print(f"  Error reading {filepath}: {e}")

    return df


def load_urls_from_file(filepath: str, url_column: str) -> pd.DataFrame:
    """Load URLs from a single file."""
    if filepath.endswith('.parquet'):
        return pd.read_parquet(filepath)
    elif filepath.endswith('.csv'):
        return pd.read_csv(filepath)
    else:
        raise ValueError(f"Unsupported file format: {filepath}")


async def main():
    args = parse_args()

    print("=" * 60)
    print("FLOW-DC Dataset Size Calculator")
    print("=" * 60)

    # Load data
    if args.directory:
        if not os.path.exists(args.directory):
            print(f"Error: Directory '{args.directory}' not found")
            return
        print(f"\nLoading from directory: {args.directory}")
        df = load_urls_from_directory(args.directory, args.url_column)

    elif args.input:
        if not os.path.exists(args.input):
            print(f"Error: File '{args.input}' not found")
            return
        print(f"\nLoading from file: {args.input}")
        df = load_urls_from_file(args.input, args.url_column)

    else:
        print("Error: Must specify either --directory or --input")
        return

    # Validate
    if df.empty:
        print("Error: No data loaded")
        return

    if args.url_column not in df.columns:
        print(f"Error: URL column '{args.url_column}' not found")
        print(f"Available columns: {list(df.columns)}")
        return

    df = df.reset_index(drop=True)
    total_urls = len(df)
    print(f"\nTotal URLs: {total_urls:,}")

    # Sample if requested
    urls = df[args.url_column].dropna().tolist()
    sample_size = len(urls)

    if args.sample and args.sample < len(urls):
        import random
        urls = random.sample(urls, args.sample)
        sample_size = len(urls)
        print(f"Sampling {sample_size:,} URLs for estimation")

    # Calculate sizes
    print(f"\nEstimating sizes with {args.concurrent} concurrent requests...")
    total_kb, total_mb, total_gb, unresolved = await get_total_size(
        urls,
        args.concurrent,
        args.timeout
    )

    # Results
    print("\n" + "=" * 60)
    print("Results")
    print("=" * 60)

    if args.sample and args.sample < total_urls:
        # Extrapolate from sample
        scale_factor = total_urls / sample_size
        est_total_gb = total_gb * scale_factor
        print(f"Sample size:      {sample_size:,} URLs")
        print(f"Sample total:     {total_mb:.2f} MB ({total_gb:.2f} GB)")
        print(f"Unresolved URLs:  {unresolved:,} ({100*unresolved/sample_size:.1f}%)")
        print(f"\nExtrapolated for full dataset ({total_urls:,} URLs):")
        print(f"Estimated total:  {est_total_gb * 1024:.2f} MB ({est_total_gb:.2f} GB)")
    else:
        print(f"Total URLs:       {total_urls:,}")
        print(f"Total size:       {total_mb:.2f} MB ({total_gb:.2f} GB)")
        print(f"Unresolved URLs:  {unresolved:,} ({100*unresolved/total_urls:.1f}%)")
        print(f"Average per file: {total_kb/max(1, total_urls-unresolved):.1f} KB")

    print("=" * 60)


if __name__ == '__main__':
    asyncio.run(main())
