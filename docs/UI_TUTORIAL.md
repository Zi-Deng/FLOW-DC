# FLOW-DC Web UI Tutorial

A comprehensive guide to using the FLOW-DC NiceGUI web interface for configuring and monitoring dataset downloads.

## Table of Contents

1. [Installation](#installation)
2. [Quick Start](#quick-start)
3. [Interface Overview](#interface-overview)
4. [Configuration Guide](#configuration-guide)
5. [Running Downloads](#running-downloads)
6. [Monitoring Jobs](#monitoring-jobs)
7. [Troubleshooting](#troubleshooting)

---

## Installation

### Prerequisites

- Python 3.10+
- Existing FLOW-DC environment

### Install NiceGUI

Add NiceGUI to your FLOW-DC environment:

```bash
conda activate FLOW-DC
pip install nicegui
```

### Verify Installation

```bash
python -c "import nicegui; print('NiceGUI installed successfully')"
```

---

## Quick Start

### 1. Launch the UI

```bash
cd /path/to/FLOW-DC
python bin/ui_app.py
```

### 2. Open in Browser

The UI will automatically open in your default browser at:
```
http://localhost:8080
```

### 3. Configure and Run

1. Enter your input file path (e.g., `files/input/urls.parquet`)
2. Enter your output folder path (e.g., `files/output/my_dataset`)
3. Adjust settings as needed
4. Click **Start Download**

---

## Interface Overview

The FLOW-DC UI is organized into several sections:

```
+------------------------------------------------------------------+
|  FLOW-DC Header                                    [Dark] [Light] |
+------------------------------------------------------------------+
|                                                                   |
|  +---------------------------+  +-----------------------------+   |
|  |   Configuration Panel     |  |     Job Status Panel        |   |
|  |                           |  |                             |   |
|  |  [Basic] [Advanced] [Rate]|  |  [Start]  [Stop]            |   |
|  |                           |  |                             |   |
|  |  Input File: [________]   |  |  Status: RUNNING            |   |
|  |  Output:     [________]   |  |  Elapsed: 00:05:32          |   |
|  |  URL Column: [________]   |  |  PolicyBBR: PROBE_BW        |   |
|  |                           |  |                             |   |
|  |  [Load Config]            |  |  Progress: [=========>] 45% |   |
|  +---------------------------+  |                             |   |
|                                 |  Successful: 4,521          |   |
|                                 |  Failed: 12                 |   |
|                                 |  Rate: 125.3 req/s          |   |
|                                 +-----------------------------+   |
|                                                                   |
|  +--------------------------------------------------------------+ |
|  |   Logs Panel                                                 | |
|  |   [12:34:56] Starting download job...                        | |
|  |   [12:34:57] Loaded 10,000 URLs                              | |
|  |   [12:35:02] Progress: 100/10000 (1.0%) - Rate: 98.2 req/s   | |
|  +--------------------------------------------------------------+ |
+------------------------------------------------------------------+
```

---

## Configuration Guide

### Basic Tab

| Field | Description | Example |
|-------|-------------|---------|
| **Input File** | Path to your URL list file | `files/input/gbif_urls.parquet` |
| **Input Format** | File format (auto-detected from extension) | `parquet`, `csv`, `excel`, `xml` |
| **Output Folder** | Where downloaded files will be saved | `files/output/gbif_dataset` |
| **Output Format** | How to organize downloaded files | `imagefolder`, `webdataset` |
| **URL Column** | Column name containing URLs | `photo_url`, `url`, `image_url` |
| **Label Column** | Column for class labels (optional) | `taxon_name`, `class`, `label` |

### Advanced Tab

| Field | Description | Default |
|-------|-------------|---------|
| **Concurrent Downloads** | Maximum parallel downloads | 256 |
| **Timeout (sec)** | Request timeout in seconds | 30 |
| **Max Retries** | Retry attempts for failed downloads | 3 |
| **Naming Mode** | `sequential` (00001.jpg) or `url_based` | sequential |
| **Create TAR** | Create .tar.gz archive when complete | false |
| **Create Overview** | Generate JSON statistics report | true |

### Rate Control Tab

| Field | Description | Default |
|-------|-------------|---------|
| **Enable PolicyBBR** | Use adaptive rate limiting | true |
| **Initial Rate** | Starting requests per second | 100.0 |
| **Min Rate** | Minimum rate floor | 1.0 |
| **Max Rate** | Maximum rate ceiling | 10,000.0 |
| **Initial Concurrency** | Per-host starting concurrency | 16 |
| **Max Concurrency** | Per-host concurrency cap | 256 |
| **Control Interval** | Seconds between rate adjustments | 5.0 |

---

## Running Downloads

### Starting a Job

1. **Configure Settings**: Fill in all required fields (Input File, Output Folder)
2. **Click Start Download**: The green "Start Download" button begins the job
3. **Monitor Progress**: Watch real-time statistics in the Status panel

### Stopping a Job

- Click the red **Stop** button to gracefully cancel
- The job will finish current downloads and save progress

### Loading Saved Configurations

1. Enter the path to a JSON config file in the text box
2. Click **Load** to populate all fields
3. Modify settings as needed
4. Start the download

Example config file path:
```
files/config/gbif.json
```

---

## Monitoring Jobs

### Status Indicators

| Indicator | Meaning |
|-----------|---------|
| **IDLE** | No job running |
| **RUNNING** | Download in progress |
| **COMPLETED** | Job finished |

### PolicyBBR States

| State | Description |
|-------|-------------|
| **STARTUP** | Discovering optimal rate (exponential growth) |
| **DRAIN** | Settling at discovered ceiling |
| **PROBE_BW** | Steady state with periodic probing |
| **PROBE_RTT** | Refreshing latency baseline |
| **BACKOFF** | Retreating after rate limit hit |

### Statistics Explained

| Metric | Description |
|--------|-------------|
| **Successful** | Downloads completed successfully |
| **Failed** | Downloads that failed after all retries |
| **Rate (req/s)** | Current requests per second |
| **Throughput (MB/s)** | Data transfer rate |

### Log Messages

The log panel shows:
- Job start/stop events
- Progress updates (every 100 URLs)
- Rate adjustments from PolicyBBR
- Error summaries
- Final statistics

---

## Troubleshooting

### UI Won't Start

```bash
# Check if port 8080 is in use
lsof -i :8080

# Try a different port
python bin/ui_app.py  # Edit ui.run(port=8081) in the file
```

### Configuration Not Loading

- Verify the JSON file path is correct
- Check JSON syntax is valid
- Ensure all required fields exist in the JSON

### Downloads Not Starting

1. **Check Input File**: Verify the path exists and is readable
2. **Check Output Folder**: Ensure parent directory exists
3. **Check URL Column**: Make sure the column name matches your data

### Slow Performance

1. Reduce **Concurrent Downloads** if hitting memory limits
2. Enable **PolicyBBR Rate Control** to avoid server throttling
3. Increase **Timeout** for slow servers

### Rate Limiting (429 Errors)

If you see many 429 errors in logs:

1. Enable **PolicyBBR Rate Control** (if not already)
2. Lower **Initial Rate** to start more conservatively
3. Lower **Max Rate** to set a ceiling
4. The system will automatically back off and find optimal rate

---

## Keyboard Shortcuts

| Key | Action |
|-----|--------|
| `Ctrl+R` | Refresh page |
| `Ctrl+Shift+I` | Open developer tools |

---

## Tips for Large Downloads

1. **Start with a small test**: Try 100-1000 URLs first
2. **Monitor PolicyBBR state**: Healthy downloads stay in PROBE_BW
3. **Check logs regularly**: Watch for error patterns
4. **Use sequential naming**: More predictable for large datasets
5. **Enable overview JSON**: Useful for post-download analysis

---

## Next Steps

- Read `UI_BUILD_GUIDE.md` to understand how the UI was built
- Modify `ui_app.py` to add custom features
- Integrate with actual download_batch.py for production use
