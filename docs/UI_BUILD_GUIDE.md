# FLOW-DC UI Build Guide

A step-by-step documentation of how the NiceGUI frontend was designed and built for FLOW-DC.

## Table of Contents

1. [Design Decisions](#design-decisions)
2. [Project Structure](#project-structure)
3. [Step-by-Step Build Process](#step-by-step-build-process)
4. [Code Architecture](#code-architecture)
5. [Extending the UI](#extending-the-ui)

---

## Design Decisions

### Why NiceGUI?

After evaluating Streamlit, Gradio, and Reflex, NiceGUI was chosen for FLOW-DC because:

| Requirement | NiceGUI Solution |
|-------------|------------------|
| Async architecture (aiohttp) | Built on FastAPI + asyncio |
| Long-running jobs (hours) | Background tasks + `ui.timer()` |
| Real-time progress updates | Native async support without WebSocket complexity |
| Configuration forms | Full form components with data binding |
| Integration with existing code | Direct Python imports work seamlessly |

### UI Design Principles

1. **Single Page Application**: All functionality on one page for simplicity
2. **Two-Column Layout**: Configuration (left) and Status (right)
3. **Tabbed Configuration**: Organized by complexity (Basic → Advanced → Rate Control)
4. **Real-Time Updates**: 500ms refresh interval for live statistics
5. **Dark Mode Default**: Easier on the eyes for long monitoring sessions

---

## Project Structure

```
FLOW-DC/
├── bin/
│   ├── ui_app.py              # Main NiceGUI application
│   ├── download_batch.py      # Core download logic
│   └── single_download.py     # Single URL download functions
├── docs/
│   ├── UI_TUTORIAL.md         # User guide
│   └── UI_BUILD_GUIDE.md      # This file
└── files/
    └── config/                # JSON configuration files
```

---

## Step-by-Step Build Process

### Step 1: Set Up the Application Shell

First, create the basic NiceGUI application structure:

```python
#!/usr/bin/env python3
from nicegui import ui, app

@ui.page('/')
def main_page():
    ui.label('FLOW-DC').classes('text-2xl font-bold')

if __name__ in {"__main__", "__mp_main__"}:
    ui.run(title='FLOW-DC', port=8080)
```

**Why this structure?**
- `@ui.page('/')` defines the main route
- `__mp_main__` check handles NiceGUI's multiprocessing reload

### Step 2: Define State Management Classes

Create dataclasses to manage application state:

```python
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any

@dataclass
class JobStatus:
    """Tracks the current state of a download job."""
    is_running: bool = False
    start_time: Optional[float] = None
    total_urls: int = 0
    completed: int = 0
    successful: int = 0
    failed: int = 0
    current_rate: float = 0.0
    bytes_downloaded: int = 0
    log_messages: List[str] = field(default_factory=list)
    policy_state: str = "IDLE"
```

**Design choice**: Using `@dataclass` provides:
- Automatic `__init__` generation
- Clean attribute access
- Easy serialization if needed later

### Step 3: Add Computed Properties

Add derived values as properties:

```python
@property
def progress(self) -> float:
    if self.total_urls == 0:
        return 0.0
    return (self.completed / self.total_urls) * 100

@property
def elapsed_time(self) -> str:
    if self.start_time is None:
        return "00:00:00"
    elapsed = time.time() - self.start_time
    hours, remainder = divmod(int(elapsed), 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

@property
def throughput_mbps(self) -> float:
    if self.start_time is None:
        return 0.0
    elapsed = time.time() - self.start_time
    if elapsed == 0:
        return 0.0
    return (self.bytes_downloaded / 1_000_000) / elapsed
```

**Why properties?**
- Computed on access, always up-to-date
- Encapsulates calculation logic
- Clean API for UI components

### Step 4: Create Configuration State

```python
class UIState:
    """Manages UI state and configuration."""

    def __init__(self):
        self.config = {
            "input_path": "",
            "input_format": "parquet",
            "output_folder": "",
            # ... more defaults
        }

    def load_config_file(self, filepath: str) -> bool:
        """Load configuration from JSON file."""
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
            # Map JSON keys to config keys
            # ...
            return True
        except Exception:
            return False
```

**Design choice**: Separate class for UI state allows:
- Clean separation from job status
- Easy config file loading/saving
- Potential for multiple config profiles

### Step 5: Build the Header

```python
def create_header():
    """Create the application header."""
    with ui.header().classes('items-center justify-between'):
        with ui.row().classes('items-center gap-4'):
            ui.icon('cloud_download', size='lg').classes('text-white')
            ui.label('FLOW-DC').classes('text-2xl font-bold text-white')

        with ui.row().classes('items-center gap-2'):
            ui.button(icon='dark_mode', on_click=lambda: ui.dark_mode(True))
            ui.button(icon='light_mode', on_click=lambda: ui.dark_mode(False))
```

**NiceGUI patterns used**:
- `with` context managers for nested layouts
- `.classes()` for Tailwind CSS styling
- Lambda functions for simple callbacks

### Step 6: Create the Configuration Panel with Tabs

```python
def create_config_panel():
    with ui.card().classes('w-full'):
        ui.label('Configuration').classes('text-xl font-bold mb-4')

        with ui.tabs().classes('w-full') as tabs:
            basic_tab = ui.tab('Basic')
            advanced_tab = ui.tab('Advanced')
            rate_tab = ui.tab('Rate Control')

        with ui.tab_panels(tabs, value=basic_tab).classes('w-full'):
            with ui.tab_panel(basic_tab):
                # Basic settings inputs...
                ui.input(
                    label='Input File',
                    placeholder='/path/to/urls.parquet'
                ).bind_value(ui_state.config, 'input_path')
```

**Key technique: Data Binding**
```python
.bind_value(ui_state.config, 'input_path')
```
This creates two-way binding:
- UI changes update `ui_state.config['input_path']`
- Code changes to the dict update the UI

### Step 7: Implement Real-Time Status Updates

```python
def create_status_panel():
    with ui.card().classes('w-full'):
        # Create labels that will be updated
        status_label = ui.label('IDLE').classes('text-2xl font-bold')
        progress_bar = ui.linear_progress(value=0)

        # Update function called by timer
        def update_status():
            status_label.text = 'RUNNING' if job_status.is_running else 'IDLE'
            progress_bar.value = job_status.progress / 100

        # Timer triggers updates every 500ms
        ui.timer(0.5, update_status)
```

**Why `ui.timer()`?**
- Simpler than WebSockets for this use case
- Works with NiceGUI's async architecture
- 500ms provides smooth updates without excessive overhead

### Step 8: Create the Log Panel

```python
def create_log_panel():
    with ui.card().classes('w-full'):
        ui.label('Logs').classes('text-xl font-bold mb-4')

        log_area = ui.log(max_lines=100).classes('w-full h-64')

        def update_logs():
            while len(job_status.log_messages) > 0:
                msg = job_status.log_messages.pop(0)
                log_area.push(msg)

        ui.timer(0.5, update_logs)
```

**Design choice**: Message queue pattern
- Job adds messages to `log_messages` list
- Timer pops and displays them
- Decouples log production from display

### Step 9: Implement Job Control Functions

```python
async def run_download_job():
    """Run the download job asynchronously."""
    global job_status, shutdown_requested

    job_status = JobStatus()
    job_status.is_running = True
    job_status.start_time = time.time()

    try:
        # In production: call actual download functions
        # For prototype: simulate progress
        while job_status.completed < job_status.total_urls:
            if shutdown_requested:
                break
            await asyncio.sleep(0.1)
            # Update progress...

    except asyncio.CancelledError:
        job_status.log_messages.append("Job cancelled")
    finally:
        job_status.is_running = False

async def start_job():
    """Start the download job."""
    global job_task
    if job_status.is_running:
        ui.notify("Job already running!", type="warning")
        return
    job_task = asyncio.create_task(run_download_job())

async def stop_job():
    """Stop the running job."""
    global shutdown_requested
    shutdown_requested = True
    if job_task:
        job_task.cancel()
```

**Async pattern**:
- `asyncio.create_task()` runs job in background
- `shutdown_requested` flag for graceful stop
- `CancelledError` handling for immediate stop

### Step 10: Assemble the Main Page

```python
@ui.page('/')
def main_page():
    ui.dark_mode(True)  # Default to dark mode

    create_header()

    with ui.column().classes('w-full max-w-6xl mx-auto p-4 gap-4'):
        # Two-column layout
        with ui.row().classes('w-full gap-4'):
            with ui.column().classes('flex-grow'):
                create_config_panel()
            with ui.column().classes('flex-grow'):
                create_status_panel()

        # Full-width log panel
        create_log_panel()
```

**Layout strategy**:
- `max-w-6xl mx-auto`: Centered, max 1152px wide
- `flex-grow`: Columns expand equally
- Modular functions for each section

---

## Code Architecture

### Component Hierarchy

```
main_page()
├── create_header()
│   └── Dark/Light mode toggles
├── create_config_panel()
│   ├── Basic Tab
│   │   ├── Input/Output fields
│   │   └── Column mapping
│   ├── Advanced Tab
│   │   ├── Download settings
│   │   └── Output options
│   └── Rate Control Tab
│       ├── PolicyBBR toggle
│       └── Rate parameters
├── create_status_panel()
│   ├── Control buttons
│   ├── Status indicators
│   ├── Progress bar
│   └── Statistics cards
└── create_log_panel()
    └── Scrolling log area
```

### Data Flow

```
User Input → ui_state.config (dict) → start_job() → run_download_job()
                                                           ↓
UI Display ← ui.timer(update) ← job_status (dataclass) ←──┘
```

### Global State

| Variable | Type | Purpose |
|----------|------|---------|
| `ui_state` | UIState | Configuration and settings |
| `job_status` | JobStatus | Current job metrics |
| `job_task` | asyncio.Task | Running job handle |
| `shutdown_requested` | bool | Graceful stop flag |

---

## Extending the UI

### Adding a New Configuration Field

1. Add to `DEFAULT_CONFIG`:
```python
DEFAULT_CONFIG = {
    # ...existing fields...
    "my_new_field": "default_value",
}
```

2. Add UI input in appropriate tab:
```python
ui.input(
    label='My New Field',
    value=ui_state.config['my_new_field']
).bind_value(ui_state.config, 'my_new_field')
```

3. Update `load_config_file()` and `save_config_file()` methods

### Adding a New Status Metric

1. Add to `JobStatus` dataclass:
```python
@dataclass
class JobStatus:
    # ...existing fields...
    my_metric: float = 0.0
```

2. Add display in `create_status_panel()`:
```python
with ui.card().classes('flex-grow'):
    metric_label = ui.label('0.0').classes('text-3xl font-bold')
    ui.label('My Metric').classes('text-sm text-gray-500')
```

3. Update in `update_status()`:
```python
metric_label.text = f'{job_status.my_metric:.2f}'
```

### Integrating with Actual Download Code

Replace the simulated `run_download_job()` with:

```python
async def run_download_job():
    global job_status

    # Import actual download functions
    from download_batch import Config, validate_and_load, main as download_main

    # Build Config from ui_state
    cfg = Config(
        input_path=ui_state.config['input_path'],
        output_folder=ui_state.config['output_folder'],
        # ... map all fields
    )

    # Run actual download
    # Note: Would need to modify download_batch.py to:
    # 1. Accept a Config object directly
    # 2. Yield progress updates
    # 3. Check shutdown flag
```

### Adding Charts/Graphs

NiceGUI supports Plotly and other charting libraries:

```python
from nicegui import ui
import plotly.graph_objects as go

def create_throughput_chart():
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=[], y=[], mode='lines', name='Throughput'))
    chart = ui.plotly(fig).classes('w-full h-64')

    def update_chart():
        # Append new data point
        fig.data[0].x = list(fig.data[0].x) + [time.time()]
        fig.data[0].y = list(fig.data[0].y) + [job_status.throughput_mbps]
        chart.update()

    ui.timer(1.0, update_chart)
```

---

## Summary

The FLOW-DC NiceGUI frontend was built by:

1. **Choosing NiceGUI** for its async-first architecture matching FLOW-DC's aiohttp base
2. **Designing state management** with dataclasses for clean data flow
3. **Building modular components** for maintainability
4. **Using `ui.timer()`** for real-time updates without WebSocket complexity
5. **Leveraging data binding** for automatic UI↔state synchronization

The result is a ~500-line prototype that provides:
- Full configuration management
- Real-time job monitoring
- Log viewing
- Easy extensibility for production integration
