#!/usr/bin/env python3
"""
Probe and correct URL extensions in parquet files.
Uses async HEAD requests with high concurrency for maximum speed.

Features:
- Batch processing for large datasets (millions of URLs)
- Checkpointing for resume capability
- Configurable concurrency and timeout
"""
import argparse
import asyncio
import json
import os
import re
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Set, Tuple

import aiohttp
import polars as pl
from tqdm import tqdm
from tqdm.asyncio import tqdm as atqdm


@dataclass
class Config:
    """Configuration for probe_extensions."""
    input_path: str
    output_path: str
    url_col: str = "url"
    concurrency: int = 1500
    timeout: int = 10
    batch_size: int = 500_000
    checkpoint_dir: Optional[str] = None
    extensions: List[str] = field(default_factory=lambda: ["jpeg", "jpg", "png", "gif"])


def parse_args() -> Config:
    """Parse command line arguments or JSON config file."""
    p = argparse.ArgumentParser(
        description="Probe and correct URL extensions in parquet files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python probe_extensions.py --config probe_extensions.json
  python probe_extensions.py --input urls.parquet --output urls_corrected.parquet

For large datasets (millions of URLs), use batch_size and checkpoint_dir:
  {
    "input": "large.parquet",
    "output": "large_corrected.parquet",
    "batch_size": 500000,
    "checkpoint_dir": "checkpoints/"
  }
"""
    )

    p.add_argument("--config", type=str, help="Path to JSON config file")
    p.add_argument("--input", dest="input_path", type=str, help="Input parquet file")
    p.add_argument("--output", dest="output_path", type=str, help="Output parquet file")
    p.add_argument("--url", dest="url_col", type=str, default="url", help="URL column name")
    p.add_argument("--concurrency", type=int, default=1500, help="Concurrent requests")
    p.add_argument("--timeout", type=int, default=10, help="Request timeout in seconds")
    p.add_argument("--batch_size", type=int, default=500_000, help="URLs per batch")
    p.add_argument("--checkpoint_dir", type=str, default=None, help="Checkpoint directory for resume")
    p.add_argument("--extensions", type=str, nargs="+",
                   default=["jpeg", "jpg", "png", "gif"],
                   help="Extensions to try (in order)")

    args = p.parse_args()

    # Load from JSON config if provided
    if args.config:
        cfg_path = Path(args.config)
        with cfg_path.open("r") as f:
            data = json.load(f)

        return Config(
            input_path=data.get("input", ""),
            output_path=data.get("output", ""),
            url_col=data.get("url", "url"),
            concurrency=int(data.get("concurrency", 1500)),
            timeout=int(data.get("timeout", 10)),
            batch_size=int(data.get("batch_size", 500_000)),
            checkpoint_dir=data.get("checkpoint_dir"),
            extensions=data.get("extensions", ["jpeg", "jpg", "png", "gif"]),
        )

    # Validate required args
    if not args.input_path or not args.output_path:
        p.error("--input and --output are required unless --config is provided")

    return Config(
        input_path=args.input_path,
        output_path=args.output_path,
        url_col=args.url_col,
        concurrency=args.concurrency,
        timeout=args.timeout,
        batch_size=args.batch_size,
        checkpoint_dir=args.checkpoint_dir,
        extensions=args.extensions,
    )


def get_completed_batches(checkpoint_dir: str) -> Set[int]:
    """Return set of completed batch indices from checkpoint directory."""
    completed = set()
    if not checkpoint_dir or not os.path.exists(checkpoint_dir):
        return completed

    for fname in os.listdir(checkpoint_dir):
        if fname.startswith("batch_") and fname.endswith(".parquet"):
            try:
                batch_idx = int(fname[6:10])
                completed.add(batch_idx)
            except ValueError:
                pass

    return completed


def save_batch_result(checkpoint_dir: str, batch_idx: int, start_idx: int,
                      results: List[Tuple[str, Optional[str], str]]) -> None:
    """Save batch results to checkpoint file."""
    os.makedirs(checkpoint_dir, exist_ok=True)

    data = {
        "idx": list(range(start_idx, start_idx + len(results))),
        "corrected_url": [r[0] for r in results],
        "extension": [r[1] for r in results],
        "status": [r[2] for r in results],
    }
    df = pl.DataFrame(data)
    df.write_parquet(os.path.join(checkpoint_dir, f"batch_{batch_idx:04d}.parquet"))


def load_all_checkpoints(checkpoint_dir: str, total_urls: int) -> Optional[List[Tuple[str, Optional[str], str]]]:
    """Load and merge all checkpoint files in order."""
    if not checkpoint_dir or not os.path.exists(checkpoint_dir):
        return None

    # Find all batch files
    batch_files = sorted([
        f for f in os.listdir(checkpoint_dir)
        if f.startswith("batch_") and f.endswith(".parquet")
    ])

    if not batch_files:
        return None

    # Load and concatenate
    dfs = []
    for fname in batch_files:
        df = pl.read_parquet(os.path.join(checkpoint_dir, fname))
        dfs.append(df)

    merged = pl.concat(dfs).sort("idx")

    # Verify we have all results
    if len(merged) != total_urls:
        return None

    return list(zip(
        merged["corrected_url"].to_list(),
        merged["extension"].to_list(),
        merged["status"].to_list()
    ))


async def find_working_extension(
    session: aiohttp.ClientSession,
    url: str,
    extensions: List[str],
    semaphore: asyncio.Semaphore,
    timeout: int
) -> Tuple[str, Optional[str], str]:
    """
    Check current extension first, then try alternatives.

    Returns:
        Tuple of (final_url, working_extension, status)
        status: "unchanged" | "corrected" | "not_found" | "no_extension"
    """
    # Extract base URL and current extension
    match = re.match(r'(.+)\.(\w+)$', url)
    if not match:
        return url, None, "no_extension"

    base_url, current_ext = match.groups()
    current_ext = current_ext.lower()

    async with semaphore:
        # Try current extension first
        try:
            async with session.head(url, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                if resp.status == 200:
                    return url, current_ext, "unchanged"
        except Exception:
            pass

        # Try alternative extensions
        for ext in extensions:
            if ext.lower() == current_ext:
                continue  # Already tried
            new_url = f"{base_url}.{ext}"
            try:
                async with session.head(new_url, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                    if resp.status == 200:
                        return new_url, ext, "corrected"
            except Exception:
                pass

    return url, None, "not_found"


async def process_batch(
    session: aiohttp.ClientSession,
    urls: List[str],
    config: Config,
    semaphore: asyncio.Semaphore,
    batch_idx: int,
    num_batches: int
) -> List[Tuple[str, Optional[str], str]]:
    """Process a batch of URLs concurrently."""
    tasks = [
        find_working_extension(session, url, config.extensions, semaphore, config.timeout)
        for url in urls
    ]
    results = await atqdm.gather(*tasks, desc=f"Batch {batch_idx+1}/{num_batches}")
    return results


async def main() -> None:
    config = parse_args()
    start_time = time.time()

    print(f"Loading {config.input_path}...")
    df = pl.read_parquet(config.input_path)
    total_urls = len(df)
    print(f"Loaded {total_urls:,} URLs")

    urls = df[config.url_col].to_list()

    # Calculate number of batches
    num_batches = (total_urls + config.batch_size - 1) // config.batch_size
    print(f"\nProcessing in {num_batches} batches of up to {config.batch_size:,} URLs each")
    print(f"Concurrency: {config.concurrency}")
    print(f"Extensions: {config.extensions}")

    # Check for completed batches (resume support)
    completed_batches = set()
    if config.checkpoint_dir:
        completed_batches = get_completed_batches(config.checkpoint_dir)
        if completed_batches:
            print(f"Found {len(completed_batches)} completed batches (resuming)")

    # Check if all batches complete
    if len(completed_batches) == num_batches:
        print("\nAll batches already complete, loading results...")
        all_results = load_all_checkpoints(config.checkpoint_dir, total_urls)
        if all_results:
            # Skip to final merge
            pass
        else:
            print("Error loading checkpoints, reprocessing...")
            completed_batches = set()

    # Process remaining batches
    all_results = []
    if len(completed_batches) < num_batches:
        semaphore = asyncio.Semaphore(config.concurrency)
        connector = aiohttp.TCPConnector(
            limit=config.concurrency + 100,
            limit_per_host=config.concurrency,
            ttl_dns_cache=300,
            enable_cleanup_closed=True
        )

        async with aiohttp.ClientSession(connector=connector) as session:
            for batch_idx in range(num_batches):
                start_idx = batch_idx * config.batch_size
                end_idx = min(start_idx + config.batch_size, total_urls)
                batch_urls = urls[start_idx:end_idx]

                if batch_idx in completed_batches:
                    print(f"Batch {batch_idx+1}/{num_batches}: Skipped (already complete)")
                    continue

                batch_start = time.time()
                results = await process_batch(
                    session, batch_urls, config, semaphore, batch_idx, num_batches
                )
                batch_time = time.time() - batch_start
                rate = len(batch_urls) / batch_time

                print(f"Batch {batch_idx+1}/{num_batches}: {len(batch_urls):,} URLs in {batch_time:.1f}s ({rate:.0f} URLs/sec)")

                # Save checkpoint
                if config.checkpoint_dir:
                    save_batch_result(config.checkpoint_dir, batch_idx, start_idx, results)

        # Load all results from checkpoints
        if config.checkpoint_dir:
            print("\nLoading all checkpoint results...")
            all_results = load_all_checkpoints(config.checkpoint_dir, total_urls)
        else:
            # If no checkpointing, we need to store results in memory (not recommended for large datasets)
            print("\nWarning: No checkpoint_dir specified. Results stored in memory only.")
    else:
        all_results = load_all_checkpoints(config.checkpoint_dir, total_urls)

    if not all_results:
        print("Error: Failed to collect all results")
        return

    # Collect statistics
    status_counter = Counter()
    ext_counter = Counter()
    corrected_urls = []

    for final_url, ext, status in all_results:
        corrected_urls.append(final_url)
        status_counter[status] += 1
        if ext:
            ext_counter[ext] += 1

    # Update dataframe
    df = df.with_columns(pl.Series(config.url_col, corrected_urls))

    # Report results
    elapsed = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"Results ({elapsed/60:.1f} minutes total):")
    print(f"  Unchanged (already correct): {status_counter['unchanged']:,} ({100*status_counter['unchanged']/total_urls:.1f}%)")
    print(f"  Corrected (extension fixed): {status_counter['corrected']:,} ({100*status_counter['corrected']/total_urls:.1f}%)")
    print(f"  Not found (no working ext):  {status_counter['not_found']:,} ({100*status_counter['not_found']/total_urls:.1f}%)")
    if status_counter['no_extension']:
        print(f"  No extension in URL:         {status_counter['no_extension']:,}")

    print(f"\nExtension distribution (working URLs):")
    for ext, count in ext_counter.most_common():
        print(f"  .{ext}: {count:,}")

    # Save corrected parquet
    print(f"\nSaving to {config.output_path}...")
    df.write_parquet(config.output_path)
    print(f"Done! Saved {total_urls:,} URLs")


if __name__ == "__main__":
    asyncio.run(main())
