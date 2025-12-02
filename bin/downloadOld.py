#!/usr/bin/env python3

import pandas as pd
import os
import sys
import aiohttp
import asyncio
import tarfile
import time
import mimetypes
import argparse
import json
from pathlib import Path 
from tqdm.asyncio import tqdm
import signal
import shutil
from http import HTTPStatus

# Global flag for graceful shutdown
shutdown_flag = False

def signal_handler(sig, frame):
    global shutdown_flag
    print("\nReceived interrupt signal. Shutting down gracefully...")
    shutdown_flag = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def parse_args():
    """
    Parse user inputs from arguments using argparse.
    """
    parser = argparse.ArgumentParser(description="Download images asynchronously, track bandwidth, and tar the output folder.")

    # JSON configuration file option
    parser.add_argument("--config", type=str, help="Path to JSON configuration file. If specified, all other arguments are ignored.")
    ############################################################
    parser.add_argument("--input", type=str, help="Path to the input CSV or Parquet file.")
    parser.add_argument("--input_format", type=str, default="parquet", help="Input file format (default: parquet).")
    parser.add_argument("--url", type=str, default="photo_url", help="Column name containing the image URLs.")
    parser.add_argument("--label", type=str, default="taxon_name", help="Column name containing the class names.")

    parser.add_argument("--output", type=str, help="Path to the output tar file (e.g., 'images.tar.gz').")
    parser.add_argument("--output_format", type=str, default="imagefolder", help="Output file format (default: tar).")

    parser.add_argument("--concurrent_downloads", type=int, default=1000, help="Number of concurrent downloads (default: 1000).")
    parser.add_argument("--timeout", type=int, default=30, help="Download timeout in seconds (default: 30).")

    parser.add_argument("--rate_limit", type=float, default=100.0, help="Initial rate limit in requests per second (default: 100.0).")
    parser.add_argument("--rate_capacity", type=int, default=200, help="Token bucket capacity (default: 200).")
    parser.add_argument("--enable_rate_limiting", action="store_true", help="Enable token bucket rate limiting.")
    parser.add_argument("--max_retry_attempts", type=int, default=3, help="Maximum retry attempts for 429 errors (default: 3).")
    parser.add_argument("--create_overview", action="store_true", default=True, help="Create JSON overview file (default: True).")
    parser.add_argument("--croissant", type=str, default="no_croissant", choices=["no_croissant", "basic_croissant", "comprehensive_croissant"], help="Generate Croissant metadata: no_croissant (default), basic_croissant, or comprehensive_croissant.")
    
    # Croissant metadata optional fields
    parser.add_argument("--dataset_name", type=str, help="Name for the dataset (default: output folder name).")
    parser.add_argument("--dataset_description", type=str, help="Custom description of the dataset.")
    parser.add_argument("--dataset_license", type=str, default="Unspecified", help="Dataset license (e.g., CC-BY-4.0, MIT, Apache-2.0).")
    parser.add_argument("--dataset_creator", type=str, help="Dataset creator name or organization.")
    parser.add_argument("--dataset_url", type=str, help="URL/homepage for the dataset.")
    parser.add_argument("--dataset_version", type=str, default="1.0.0", help="Dataset version (default: 1.0.0).")
    parser.add_argument("--dataset_citation", type=str, help="Citation information for the dataset.")
    parser.add_argument("--dataset_keywords", type=str, help="Comma-separated keywords for the dataset.")

    args = parser.parse_args()

    if args.config:
        args = load_json_config(args.config)
    else:
        # Validate required arguments when not using JSON config
        if not args.input:
            parser.error("--input is required when not using --config")
        if not args.output:
            parser.error("--output is required when not using --config")
        return args

    return args

def load_json_config(config_path):
    """
    Load configuration from JSON file and return as argparse.Namespace object.
    """
    try:
        with open(config_path, 'r') as f:
            config_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: Configuration file '{config_path}' not found.")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in configuration file '{config_path}': {e}")
        sys.exit(1)

    # Define required fields and defaults
    required_fields = ['input', 'output']
    defaults = {
        'url': 'photo_url',
        'label': 'taxon_name',
        'input_format': 'parquet',
        'output_format': 'imagefolder',
        'concurrent_downloads': 1000,
        'timeout': 30,
        'rate_limit': 100.0,
        'rate_capacity': 200,
        'enable_rate_limiting': False,
        'max_retry_attempts': 0,
        'create_overview': True,
        'croissant': 'no_croissant',
        'dataset_name': None,
        'dataset_description': None,
        'dataset_license': 'Unspecified',
        'dataset_creator': None,
        'dataset_url': None,
        'dataset_version': '1.0.0',
        'dataset_citation': None,
        'dataset_keywords': None,
    }
    
    # Check required fields
    for field in required_fields:
        if field not in config_data:
            print(f"Error: Required field '{field}' not found in JSON configuration.")
            sys.exit(1)

    # Apply defaults for missing optional fields
    for field, default_value in defaults.items():
        if field not in config_data:
            config_data[field] = default_value

    # Validate data types
    type_validators = {
        'input': str,
        'output': str,
        'input_format': str,
        'output_format': str,
        'concurrent_downloads': int,
        'timeout': int,
        'rate_limit': (int, float),
        'rate_capacity': int,
        'enable_rate_limiting': bool,
        'max_retry_attempts': int,
    }
    for field, expected_type in type_validators.items():
        if not isinstance(config_data[field], expected_type):
            print(f"Error: Field '{field}' has invalid type. Expected {expected_type}, got {type(config_data[field])}.")
            sys.exit(1)
    
    # Convert to argparse.Namespace for compatibility
    return argparse.Namespace(**config_data)

class TokenBucket:
    def __init__(self, rate, capacity):
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.last_refill = time.time()

    async def acquire(self):
        """
        Wait until a token is available and consume it.
        """
        while True:
            self._refill()
            if self.tokens >= 1:
                self.tokens -= 1
                return
            await asyncio.sleep(0.01) 
    # Wait a short time before checking again

    def _refill(self):
        """
        Refill the bucket with tokens based on the elapsed time.
        """
        now = time.time()
        elapsed = now - self.last_refill
        new_tokens = elapsed * self.rate
        self.tokens = min(self.capacity, self.tokens + new_tokens)
        self.last_refill = now

    def adjust_rate(self, new_rate, reason=""):
        new_rate = max(1, new_rate)  # Minimum 1 request per second
        if abs(new_rate - self.rate) > 0.1:  # Only adjust if significant change
            reason_str = f" ({reason})" if reason else ""
            print(f"[Rate Limit] {self.rate:.2f} -> {new_rate:.2f} req/sec{reason_str}")
            self.rate = new_rate

    def get_rate(self):
        return self.rate

async def gradually_increase_rate(
    # Rate limiting
    token_bucket, max_rate, 
    # Timing
    interval=5
):
    """
    Gradually increase the rate limit over time to recover from rate limiting.
    """
    while True:
        await asyncio.sleep(interval)
        current_rate = token_bucket.get_rate()
        if current_rate < max_rate:
            new_rate = min(max_rate, current_rate * 1.2)  # Increase by 20%
            print(f"[Gradual Recovery] Increasing rate: {current_rate:.2f} -> {new_rate:.2f} req/sec")
            token_bucket.adjust_rate(new_rate, "rate increase")
        else:
            print(f"[Gradual Recovery] At maximum rate: {current_rate:.2f} req/sec")
            pass

def save_and_track(
    # Content and format
    content, output_format,
    # Identifiers and metadata
    key, image_url, class_name,
    # Output config
    file_path, total_bytes
):
    """
    Save content to file and track progress.
    """
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'wb') as f:
            f.write(content)
        file_size = os.path.getsize(file_path)
        total_bytes.append(file_size)

        if output_format == "webdataset":
            json_path = file_path.split(".")[0] + ".json"
            with open(json_path, 'w') as f:
                json.dump({
                    'key': key,
                    'image_url': image_url,
                    'class_name': class_name,
                }, f)
    
        return True, None
    except Exception as e:
        print(f"Error saving file {file_path}: {e}")
        return False, str(e)

async def download_with_extension(
    # Core identifiers
    key, image_url, original_ext, class_name,
    # Network/session
    session, timeout,
    # Output config
    base_url, output_folder, output_format,
    # Rate limiting
    token_bucket, enable_rate_limiting, total_bytes
):
    file_name = f"{base_url.split('/')[-2]}{original_ext}"

    if output_format == "imagefolder":
        file_path = os.path.join(output_folder, class_name, file_name)
    if output_format == "webdataset":
        file_path = os.path.join(output_folder, file_name)
    
    try:
        if token_bucket and enable_rate_limiting:
            await token_bucket.acquire()

        async with session.get(image_url, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
            if response.status == 200:
                content = await response.read()
                success, error = save_and_track(content, output_format, key, image_url, class_name, file_path, total_bytes)
                if success:
                    return key, file_path, class_name, None, response.status
                else:
                    return key, file_path, class_name, error, response.status
            else:
                try:
                    status_name = HTTPStatus(response.status).phrase
                except ValueError:
                    status_name = "Unknown"
                return key, file_path, class_name, f"HTTP Error: {status_name}", response.status
    except asyncio.TimeoutError as e:
        return key, file_path, class_name, str(e), 408
    except Exception as e:
        return key, file_path, class_name, str(e), None

async def download_no_extension(
    # Core identifiers
    key, image_url, class_name,
    # Network/session
    session, timeout,
    # Output config
    base_url, output_folder, output_format,
    # Rate limiting
    token_bucket, enable_rate_limiting, total_bytes
):
    ext = ".png"
    file_name = f"{base_url.split('/')[-2]}{ext}"

    if output_format == "imagefolder":
        file_path = os.path.join(output_folder, class_name, file_name)
    if output_format == "webdataset":
        file_path = os.path.join(output_folder, file_name)
    try:
        if token_bucket and enable_rate_limiting:
            await token_bucket.acquire()
        
        async with session.get(image_url, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
            if response.status == 200:
                content = await response.read()

                success, error = save_and_track(content, output_format, key, image_url, class_name, file_path, total_bytes)

                if success:
                    return key, file_path, class_name, None, response.status
                else:
                    return key, file_path, class_name, error, response.status
            else:
                try:
                    status_name = HTTPStatus(response.status).phrase
                except ValueError:
                    status_name = "Unknown"
                return key, file_path, class_name, f"HTTP Error: {status_name}", response.status
    except asyncio.TimeoutError as e:
        return key, file_path, class_name, str(e), 408
    except Exception as e:
        return key, file_path, class_name, str(e), None

async def download_image(
    # Data row and concurrency
    row, semaphore,
    # Column names
    url_col, class_col,
    # Rate limiting
    token_bucket, enable_rate_limiting,
    # Network/session
    session, timeout,
    # Output config
    total_bytes, output_format, output_folder
):
    global shutdown_flag
    if shutdown_flag:
        return row.name, None, None, "Shutdown requested"

    async with semaphore:
        key, image_url = row.name, row[url_col]

    if pd.isna(image_url) or not str(image_url).strip():
        return key, None, None, "Invalid image URL"
    
    class_name = str(row[class_col]).replace("'", "").replace('"', "").replace(" ", "_").replace("/", "_")
    base_url, original_ext = os.path.splitext(str(image_url))

    if not original_ext:
        return await download_no_extension(
            key,
            image_url,
            class_name,
            session,
            timeout,
            base_url,
            output_folder,
            output_format,
            token_bucket,
            enable_rate_limiting,
            total_bytes
        )
    else:
        return await download_with_extension(
            key,
            image_url,
            original_ext,
            class_name,
            session,
            timeout,
            base_url,
            output_folder,
            output_format,
            token_bucket,
            enable_rate_limiting,
            total_bytes
        )

async def download_batch(
    # Network/session
    session, concurrent_downloads,
    # Data batch
    df_batch,
    # Output config
    output_folder, output_format,
    # Column names
    url_col, class_col,
    # Tracking and limits
    total_bytes, timeout,
    # Rate limiting
    token_bucket, enable_rate_limiting,
    # Retry control
    attempt_number=1
):
    """
    Download batch of images
    """
    global shutdown_flag
    semaphore = asyncio.Semaphore(concurrent_downloads)

    tasks = [
        download_image(
            row,                    # row
            semaphore,              # semaphore
            url_col,                # url_col
            class_col,              # class_col
            token_bucket,           # token_bucket
            enable_rate_limiting,   # enable_rate_limiting
            session,                # session
            timeout,                # timeout
            total_bytes,            # total_bytes
            output_format,          # output_format
            output_folder,          # output_folder
        )
        for _, row in df_batch.iterrows()
    ]

    error_details = []
    retry_rows =[]
    successful_downloads = 0

    print(f"\n--- Attempt #{attempt_number} - Processing {len(tasks)} images ---")
    if enable_rate_limiting and token_bucket:
        print(f"Current rate limit: {token_bucket.get_rate():.2f} req/sec")
    
    try:
        for future in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc=f"Downloading (attempt {attempt_number})"):
            if shutdown_flag:
                print("\nShutting down gracefully...")
                break

            key, file_name, class_name, error, status_code = await future

            if error:
                error_details.append({
                    'key': key,
                    'file_name': file_name,
                    'class_name': class_name,
                    'error': error,
                    'status_code': status_code,
                })

                is_429_error = status_code == 429 or "429" in str(error)
                is_timeout_error = status_code == 504 or "504" in str(error)
                is_server_error = status_code >= 500 or "500" in str(error)

                if is_429_error or is_timeout_error or is_server_error:
                    original_row = df_batch.loc[df_batch.index==key]
                    if not original_row.empty:
                        retry_rows.append(original_row.to_dict(orient='records')[0])

                if token_bucket and enable_rate_limiting:
                    if is_429_error or is_timeout_error or is_server_error:
                        new_rate = token_bucket.get_rate() * 0.5
                        token_bucket.adjust_rate(new_rate, "rate reduction")
                
                if not (is_429_error or is_timeout_error or is_server_error):
                    print(f"\n[Error] Key: {key}, Error: {error}, Status Code: {status_code}")
            else:
                successful_downloads += 1
    except KeyboardInterrupt:
        print("Download interrupted by user")
        shutdown_flag = True

    return successful_downloads, error_details, retry_rows


def validate_and_clean(
    # Input/Output paths
    input, output_folder,
    # Column names
    url_col, class_col
):
    if not os.path.exists(input):
        print(f"Error: Input file {input} not found")
        sys.exit(1)
    
    if os.path.exists(output_folder):
        print(f"Error: Output folder {output_folder} already exists")
        sys.exit(1)
        #shutil.rmtree(output_folder)

    try:
        if input.endswith(".csv") or input.endswith(".txt"):
            df = pd.read_csv(input)
        elif input.endswith(".parquet"):
            df = pd.read_parquet(input)
        elif input.endswith(".xml"):
            df = pd.read_xml(input)
        else:
            print(f"Error: Unsupported input file format {input}")
            sys.exit(1)
    except Exception as e:
        print(f"Error: Failed to read input file {input}: {e}")
        sys.exit(1)

    if url_col not in df.columns:
        print(f"Error: URL column {url_col} not found in input file {input}")
        sys.exit(1)

    initial_count = len(df)
    df = df.dropna(subset=[url_col])
    filtered_count = len(df)
    if filtered_count < initial_count:
        print(f"Filtered out {initial_count - filtered_count} rows with missing URLs")
    
    if filtered_count == 0:
        print("No valid URLs found in input file")
        sys.exit(1)
    
    return df, filtered_count

def create_json_overview(
        # Input/Output config
        input, input_format, output_path, output_format,
        # Column names
        url_col, class_col,
        # Download settings
        concurrent_downloads, timeout,
        # Results tracking
        error_details, successful_downloads, total_bytes, total_time, total_errors, total_downloaded, filtered_count,
        # Rate limiting
        rate_limit, rate_capacity, token_bucket=None, enable_rate_limiting=False,
        # Retry control
        max_retry_attempts=3
):
    overview_data = {
        "script_inputs": {
            "input": input,
            "input_format": input_format,
            "output": output_path,
            "output_format": output_format,
            "url_column": url_col,
            "label_column": class_col,
            "concurrent_downloads": concurrent_downloads,
            "timeout": timeout,
            "rate_limiting_enabled": enable_rate_limiting,
            "final_rate_limit": token_bucket.get_rate() if token_bucket else None,
            "rate_limit": rate_limit,
            "rate_capacity": rate_capacity,
            "max_retry_attempts": max_retry_attempts,
        },
        "download_summary": {
            "total_records_processed": filtered_count,
            "successful_downloads": successful_downloads,
            "failed_downloads": total_errors,
            "success_rate_percent": round((successful_downloads/(successful_downloads+total_errors)*100), 2) if (successful_downloads + total_errors) > 0 else 0,
            "total_data_mb": round(total_downloaded / 1e6, 2) if total_downloaded > 0 else 0,
            "total_time_seconds": round(total_time, 2),
            "average_speed_mbps": round((total_downloaded / total_time) / 1e6, 2) if total_time > 0 and total_downloaded > 0 else 0
        },
        "error_breakdown": {},
        "execution_info": {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "shutdown_requested": shutdown_flag
        }
    }
    if total_errors > 0:
        error_counts = {}
        for error_info in error_details:
            error_type = error_info['error']
            if error_type in error_counts:
                error_counts[error_type] += 1
            else:
                error_counts[error_type] = 1
        overview_data["error_breakdown"] = error_counts

    json_filename = os.path.splitext(output_path)[0] + "_overview.json"
    try:
        with open(json_filename, 'w') as json_file:
            json.dump(overview_data, json_file, indent=2)
        print(f"Created overview file: {json_filename}")
    except Exception as e:
        print(f"Warning: Could not create overview JSON file: {e}")

    return None

def create_tar_file(
    # Output paths
    output_path,  # This is the FOLDER containing images
    # Results tracking
    successful_downloads, total_errors
):
    tar_filename = output_path + ".tar.gz"  # Generate tar name
    
    if successful_downloads > 0 and not shutdown_flag and os.path.exists(output_path):
        try:
            print(f"\nCreating tar archive: {tar_filename}")
            with tarfile.open(tar_filename, "w:gz") as tar:
                tar.add(output_path, arcname=os.path.basename(output_path))
                
            full_path = Path(tar_filename).resolve()
            tar_size = os.path.getsize(tar_filename)
            print(f"Created tar archive: {full_path} ({tar_size / 1e6:.2f} MB)")          
        except Exception as e:
            print(f"Error creating tar archive: {e}")
            sys.exit(1)
    else:
        if shutdown_flag:
            print("Shutdown was requested, skipping tar creation")
        elif successful_downloads == 0:
            print("No successful downloads, skipping tar creation")
        sys.exit(1 if total_errors > 0 else 0)
    
    return None

def create_croissant_metadata(
    # Output paths
    output_folder, output_format,
    # Mode
    croissant_mode,
    # Dataset statistics
    successful_downloads, filtered_count, class_col, url_col,
    # Timing and size
    download_time, total_downloaded,
    # Optional metadata
    dataset_name=None, dataset_description=None, dataset_license=None,
    dataset_creator=None, dataset_url=None, dataset_version=None,
    dataset_citation=None, dataset_keywords=None
):
    """
    Generate Croissant metadata for the output dataset.
    
    Args:
        output_folder: Path to the folder containing downloaded images
        output_format: Format of output ('imagefolder' or 'webdataset')
        croissant_mode: Mode of metadata ('basic_croissant' or 'comprehensive_croissant')
        successful_downloads: Number of successfully downloaded images
        filtered_count: Total number of images attempted
        class_col: Name of the class/label column
        url_col: Name of the URL column
        download_time: Time taken for downloads in seconds
        total_downloaded: Total bytes downloaded
        dataset_name: Optional custom dataset name
        dataset_description: Optional custom dataset description
        dataset_license: Optional license information
        dataset_creator: Optional creator name or organization
        dataset_url: Optional dataset URL/homepage
        dataset_version: Optional version string
        dataset_citation: Optional citation information
        dataset_keywords: Optional comma-separated keywords
    """
    if not os.path.exists(output_folder):
        print(f"Warning: Output folder {output_folder} does not exist. Skipping Croissant metadata generation.")
        return None
    
    try:
        # Scan dataset structure
        dataset_name = dataset_name or os.path.basename(output_folder)
        tar_filename = output_folder + ".tar.gz"
        
        # Collect class information for imagefolder format
        classes = []
        class_counts = {}
        total_size = 0
        
        if output_format == "imagefolder" and os.path.exists(output_folder):
            # Scan subdirectories for classes
            for class_dir in os.listdir(output_folder):
                class_path = os.path.join(output_folder, class_dir)
                if os.path.isdir(class_path):
                    classes.append(class_dir)
                    # Count images in this class
                    image_files = [f for f in os.listdir(class_path) if os.path.isfile(os.path.join(class_path, f))]
                    class_counts[class_dir] = len(image_files)
                    # Add to total size
                    for img_file in image_files:
                        img_path = os.path.join(class_path, img_file)
                        total_size += os.path.getsize(img_path)
        
        # Build basic metadata structure
        # Use custom description if provided, otherwise generate default
        if dataset_description:
            description = dataset_description
        else:
            description = f"Image dataset downloaded and organized by {class_col}. Contains {successful_downloads} successfully downloaded images across {len(classes)} classes."
        
        metadata = {
            "@context": "https://schema.org/",
            "@type": "Dataset",
            "name": dataset_name,
            "description": description,
            "datePublished": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "distribution": []
        }
        
        # Add optional fields if provided
        if dataset_url:
            metadata["url"] = dataset_url
        
        if dataset_version:
            metadata["version"] = dataset_version
        
        # Add tar file distribution if it exists
        if os.path.exists(tar_filename):
            tar_size = os.path.getsize(tar_filename)
            metadata["distribution"].append({
                "@type": "DataDownload",
                "encodingFormat": "application/gzip",
                "contentUrl": Path(tar_filename).resolve().as_uri(),
                "contentSize": tar_size
            })
        
        # Add folder distribution
        metadata["distribution"].append({
            "@type": "DataDownload",
            "encodingFormat": "application/x-directory",
            "contentUrl": Path(output_folder).resolve().as_uri(),
            "contentSize": total_size if total_size > 0 else total_downloaded
        })
        
        # Basic recordSet structure
        if classes:
            metadata["recordSet"] = {
                "@type": "ml:RecordSet",
                "name": "images",
                "description": f"Image records organized by {class_col}",
                "field": [
                    {
                        "@type": "ml:Field",
                        "name": "image",
                        "description": "The image file",
                        "dataType": "ml:Image"
                    },
                    {
                        "@type": "ml:Field",
                        "name": class_col,
                        "description": "The class label",
                        "dataType": "sc:Text"
                    }
                ]
            }
        
        # Add comprehensive metadata if requested
        if croissant_mode == "comprehensive_croissant":
            # Use provided license or default
            metadata["license"] = dataset_license or "Unspecified"
            
            # Add creator if provided
            if dataset_creator:
                # Detect if it's likely an organization (contains certain keywords)
                is_org = any(word in dataset_creator.lower() for word in ['university', 'institute', 'lab', 'laboratory', 'company', 'corporation', 'inc', 'ltd'])
                metadata["creator"] = [
                    {
                        "@type": "Organization" if is_org else "Person",
                        "name": dataset_creator
                    }
                ]
            
            # Use provided keywords or generate default
            if dataset_keywords:
                keywords = [k.strip() for k in dataset_keywords.split(",")]
                metadata["keywords"] = keywords
            else:
                metadata["keywords"] = ["images", "classification", class_col]
            
            # Add detailed class information
            if classes:
                metadata["about"] = f"Image classification dataset with {len(classes)} classes: {', '.join(sorted(classes)[:10])}" + ("..." if len(classes) > 10 else "")
                
                # Add class distribution
                metadata["variableMeasured"] = [
                    {
                        "@type": "PropertyValue",
                        "name": "class_distribution",
                        "value": class_counts
                    }
                ]
            
            # Add provenance information
            metadata["temporalCoverage"] = time.strftime("%Y-%m-%d", time.gmtime())
            
            # Add quality metrics
            metadata["measurementTechnique"] = "Automated download and organization"
            
            if filtered_count > 0:
                metadata["qualityMeasurement"] = {
                    "@type": "PropertyValue",
                    "name": "download_success_rate",
                    "value": round((successful_downloads / filtered_count) * 100, 2),
                    "unitText": "percent"
                }
            
            # Add performance metrics
            if download_time > 0 and total_downloaded > 0:
                metadata["contentSize"] = total_downloaded
                metadata["performanceMetrics"] = {
                    "downloadTime": round(download_time, 2),
                    "downloadSpeed": round((total_downloaded / download_time) / 1e6, 2),
                    "downloadSpeedUnit": "MB/s"
                }
            
            # Add citation if provided
            if dataset_citation:
                metadata["citeAs"] = dataset_citation
            
            # Enhanced recordSet for comprehensive mode
            if classes and "recordSet" in metadata:
                metadata["recordSet"]["field"].extend([
                    {
                        "@type": "ml:Field",
                        "name": "file_path",
                        "description": "Relative path to the image file",
                        "dataType": "sc:Text"
                    },
                    {
                        "@type": "ml:Field",
                        "name": "file_size",
                        "description": "Size of the image file in bytes",
                        "dataType": "sc:Integer"
                    }
                ])
                
                metadata["recordSet"]["totalRecords"] = successful_downloads
                metadata["recordSet"]["classes"] = sorted(classes)
        
        # Save metadata to file
        croissant_filename = os.path.splitext(output_folder)[0] + "_croissant.json"
        with open(croissant_filename, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        print(f"Created Croissant metadata file: {croissant_filename}")
        
    except Exception as e:
        print(f"Warning: Could not create Croissant metadata file: {e}")
    
    return None

async def main():
    global shutdown_flag

    args = parse_args()

    # Display configuration source
    if hasattr(args, 'config') and args.config:
        print(f"Using JSON configuration from: {args.config}")
    else:
        print("Using command-line arguments")

    input = args.input
    input_format = args.input_format
    url_col = args.url
    class_col = args.label
    output = args.output
    output_format = args.output_format
    concurrent_downloads = args.concurrent_downloads
    timeout = args.timeout
    rate_limit = args.rate_limit
    rate_capacity = args.rate_capacity
    enable_rate_limiting = args.enable_rate_limiting
    max_retry_attempts = args.max_retry_attempts
    create_overview = args.create_overview
    croissant_mode = args.croissant
    
    # Extract Croissant metadata arguments
    dataset_name = args.dataset_name
    dataset_description = args.dataset_description
    dataset_license = args.dataset_license
    dataset_creator = args.dataset_creator
    dataset_url = args.dataset_url
    dataset_version = args.dataset_version
    dataset_citation = args.dataset_citation
    dataset_keywords = args.dataset_keywords
    
    # Validate and clean input data
    df, filtered_count = validate_and_clean(input, output, url_col, class_col)

    token_bucket = None
    if enable_rate_limiting:
        token_bucket = TokenBucket(rate=rate_limit, capacity=rate_capacity)
        print(f"Processing {filtered_count} images with {concurrent_downloads} concurrent downloads and {rate_limit:.1f} req/s rate limit")
    else:
        print(f"Processing {filtered_count} images with {concurrent_downloads} concurrent downloads (no rate limiting)")

    # Create output folder
    total_bytes = []
    download_start_time = time.monotonic()

    connector = aiohttp.TCPConnector(
        limit=int(concurrent_downloads * 1.1),
        ttl_dns_cache=300,
        use_dns_cache=True
    )

    recovery_task = None
    if enable_rate_limiting and token_bucket:
        print(f"Starting rate increase (target: {concurrent_downloads*1.1:.1f} req/sec, interval: 5s)")
        recovery_task = asyncio.create_task(
            gradually_increase_rate(token_bucket, concurrent_downloads*1.1, interval=5)
        )

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=aiohttp.ClientTimeout(total=timeout*2),  # Overall session timeout
        headers={'User-Agent': 'FLOW-DC-ImageDownloader/1.0'}
    ) as session:
        all_error_details = []
        total_successful_downloads = 0
        current_df = df.copy()
        attempt = 1

        while attempt <= max_retry_attempts and not current_df.empty and not shutdown_flag:
            successful_downloads, error_details, retry_rows = await download_batch(
                session, concurrent_downloads,
                current_df,
                output, output_format,
                url_col, class_col,
                total_bytes, timeout,
                token_bucket, enable_rate_limiting,
                attempt
            )

            total_successful_downloads += successful_downloads
            all_error_details.extend(error_details)

            count_retry_errors = len(retry_rows)
            non_retry_errors = len(error_details) - count_retry_errors

            print(f"\nAttempt #{attempt} Results:")
            print(f"  - Successful downloads: {successful_downloads}")
            print(f"  - Retry errors: {count_retry_errors}")
            print(f"  - Other errors: {non_retry_errors}")

            if retry_rows and attempt < max_retry_attempts and not shutdown_flag:
                current_df = pd.DataFrame(retry_rows)
                attempt += 1
                print(f"\nWaiting 2.0 seconds before retry attempt...")
                await asyncio.sleep(2.0)
            else:
                break

        # Final results
        if retry_rows and attempt > max_retry_attempts:
            print(f"\nReached maximum retry attempts ({max_retry_attempts}). {len(retry_rows)} items with retry errors will not be retried.")
        elif not retry_rows:
            print(f"\nAll downloads completed successfully or no retry errors remaining.")

    if recovery_task:
        recovery_task.cancel()

    # Use the aggregated results
    successful_downloads = total_successful_downloads
    error_details = all_error_details

    download_end_time = time.monotonic()
    download_time = download_end_time - download_start_time
    total_downloaded = sum(total_bytes)  # Total bytes downloaded
    total_errors = len(error_details)

    # Print download summary
    print(f"\nDownload Summary:")
    print(f"  - Successful downloads: {successful_downloads}")
    print(f"  - Failed downloads: {total_errors}")
    if successful_downloads + total_errors > 0:
        print(f"  - Success rate: {(successful_downloads/(successful_downloads+total_errors)*100):.1f}%")
    
    if download_time > 0 and total_downloaded > 0:
        avg_speed = total_downloaded / download_time  # Bytes per second
        print(f"  - Total Data: {total_downloaded / 1e6:.2f} MB")
        print(f"  - Download Time: {download_time:.2f} sec")
        print(f"  - Avg Download Speed: {avg_speed / 1e6:.2f} MB/s")
    else:
        print("  - No successful downloads to compute bandwidth statistics.")
    
    # Display detailed error breakdown
    if total_errors > 0:
        print(f"\nError Breakdown:")
        error_counts = {}
        for error_info in error_details:
            error_type = error_info['error']
            if error_type in error_counts:
                error_counts[error_type] += 1
            else:
                error_counts[error_type] = 1
        
        for error_type, count in sorted(error_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"  - {error_type}: {count} occurrences")

    # Conditionally create Croissant metadata file
    if croissant_mode != "no_croissant":
        create_croissant_metadata(
            output, output_format,
            croissant_mode,
            successful_downloads, filtered_count, class_col, url_col,
            download_time, total_downloaded,
            dataset_name, dataset_description, dataset_license,
            dataset_creator, dataset_url, dataset_version,
            dataset_citation, dataset_keywords
        )
    else:
        print("Skipped creating Croissant metadata file")

    # # START TIMING - Tar creation
    tar_start_time = time.monotonic()
    
    # # Create tar file if we have successful downloads
    # create_tar_file(
    #     output,
    #     successful_downloads,
    #     total_errors
    # )
    
    # # END TIMING - Tar creation
    tar_end_time = time.monotonic()
    tar_time = tar_end_time - tar_start_time
    
    # Print tar creation time (only if tar was created)
    if successful_downloads > 0 and not shutdown_flag:
        print(f"\nTar Creation:")
        print(f"  - Time: {tar_time:.2f} sec")
    
    # Total time
    total_time = download_time + tar_time
    print(f"\nTotal Time: {total_time:.2f} sec")
    print(f"  - Download phase: {download_time:.2f} sec ({(download_time/total_time*100):.1f}%)")
    print(f"  - Tar creation phase: {tar_time:.2f} sec ({(tar_time/total_time*100):.1f}%)")

    # Conditionally create overview JSON file
    if create_overview:
        create_json_overview(
            input, input_format, output, output_format,
            url_col, class_col,
            concurrent_downloads, timeout,
            error_details, successful_downloads, total_bytes, download_time, total_errors, total_downloaded, filtered_count,
            rate_limit, rate_capacity, token_bucket, enable_rate_limiting, max_retry_attempts
        )
    else:
        print("\nSkipped creating JSON overview file")
    

if __name__ == "__main__":
    asyncio.run(main()) 


