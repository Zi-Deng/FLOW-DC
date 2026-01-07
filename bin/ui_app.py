#!/usr/bin/env python3
"""
FLOW-DC Web UI - NiceGUI Frontend

A web-based interface for configuring and monitoring FLOW-DC batch downloads.
Built with NiceGUI for seamless async integration with the existing codebase.
"""

import asyncio
import json
import os
import sys
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional, Dict, Any, List
from datetime import datetime

from nicegui import ui, app

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

# -----------------------------------------
# Job State Management
# -----------------------------------------

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
    errors: List[Dict[str, Any]] = field(default_factory=list)
    log_messages: List[str] = field(default_factory=list)
    policy_state: str = "IDLE"

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

    @property
    def success_rate(self) -> float:
        if self.completed == 0:
            return 0.0
        return (self.successful / self.completed) * 100


# Global job status
job_status = JobStatus()
job_task: Optional[asyncio.Task] = None
shutdown_requested = False


# -----------------------------------------
# Configuration Defaults
# -----------------------------------------

DEFAULT_CONFIG = {
    "input_path": "",
    "input_format": "parquet",
    "output_folder": "",
    "output_format": "imagefolder",
    "url_col": "photo_url",
    "label_col": "",
    "concurrent_downloads": 256,
    "timeout_sec": 30,
    "enable_polite_controller": True,
    "initial_rate": 100.0,
    "min_rate": 1.0,
    "max_rate": 10000.0,
    "per_host_conc_init": 16,
    "per_host_conc_cap": 256,
    "control_interval_sec": 5.0,
    "max_retry_attempts": 3,
    "naming_mode": "sequential",
    "create_tar": False,
    "create_overview": True,
}


# -----------------------------------------
# UI State
# -----------------------------------------

class UIState:
    """Manages UI state and configuration."""

    def __init__(self):
        self.config = DEFAULT_CONFIG.copy()
        self.dark_mode = True

    def load_config_file(self, filepath: str) -> bool:
        """Load configuration from JSON file."""
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)

            # Map JSON keys to config keys
            mapping = {
                "input": "input_path",
                "output": "output_folder",
                "url": "url_col",
                "label": "label_col",
                "timeout": "timeout_sec",
                "control_interval_sec": "control_interval_sec",
            }

            for json_key, config_key in mapping.items():
                if json_key in data:
                    self.config[config_key] = data[json_key]

            # Direct mappings
            direct_keys = [
                "input_format", "output_format", "concurrent_downloads",
                "enable_polite_controller", "initial_rate", "min_rate",
                "max_rate", "per_host_conc_init", "per_host_conc_cap",
                "control_interval_sec", "max_retry_attempts", "naming_mode",
                "create_tar", "create_overview"
            ]
            for key in direct_keys:
                if key in data:
                    self.config[key] = data[key]

            return True
        except Exception as e:
            return False

    def save_config_file(self, filepath: str) -> bool:
        """Save configuration to JSON file."""
        try:
            # Map config keys back to JSON format
            data = {
                "input": self.config["input_path"],
                "input_format": self.config["input_format"],
                "output": self.config["output_folder"],
                "output_format": self.config["output_format"],
                "url": self.config["url_col"],
                "label": self.config["label_col"] if self.config["label_col"] else None,
                "concurrent_downloads": self.config["concurrent_downloads"],
                "timeout": self.config["timeout_sec"],
                "enable_polite_controller": self.config["enable_polite_controller"],
                "initial_rate": self.config["initial_rate"],
                "min_rate": self.config["min_rate"],
                "max_rate": self.config["max_rate"],
                "per_host_conc_init": self.config["per_host_conc_init"],
                "per_host_conc_cap": self.config["per_host_conc_cap"],
                "control_interval_sec": self.config["control_interval_sec"],
                "max_retry_attempts": self.config["max_retry_attempts"],
                "naming_mode": self.config["naming_mode"],
                "create_tar": self.config["create_tar"],
                "create_overview": self.config["create_overview"],
            }

            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2)
            return True
        except Exception as e:
            return False


ui_state = UIState()


# -----------------------------------------
# Download Job Runner (Simulated)
# -----------------------------------------

async def run_download_job():
    """
    Run the download job. This is a simulation for the UI prototype.
    In production, this would import and call the actual download_batch functions.
    """
    global job_status, shutdown_requested

    job_status = JobStatus()
    job_status.is_running = True
    job_status.start_time = time.time()
    job_status.policy_state = "STARTUP"

    # Simulate loading URLs
    job_status.total_urls = 1000  # In production: len(df) from validate_and_load()
    job_status.log_messages.append(f"[{datetime.now().strftime('%H:%M:%S')}] Starting download job...")
    job_status.log_messages.append(f"[{datetime.now().strftime('%H:%M:%S')}] Loaded {job_status.total_urls} URLs")
    job_status.log_messages.append(f"[{datetime.now().strftime('%H:%M:%S')}] Output: {ui_state.config['output_folder']}")

    # Simulate download progress
    try:
        while job_status.completed < job_status.total_urls and not shutdown_requested:
            await asyncio.sleep(0.1)  # Simulate work

            # Simulate batch completion
            batch_size = min(10, job_status.total_urls - job_status.completed)
            job_status.completed += batch_size

            # Simulate success/failure (95% success rate)
            import random
            successes = int(batch_size * 0.95)
            job_status.successful += successes
            job_status.failed += (batch_size - successes)

            # Simulate bytes downloaded (avg 100KB per image)
            job_status.bytes_downloaded += batch_size * 100_000

            # Simulate rate changes
            job_status.current_rate = random.uniform(80, 150)

            # Simulate policy state transitions
            progress = job_status.progress
            if progress < 10:
                job_status.policy_state = "STARTUP"
            elif progress < 20:
                job_status.policy_state = "DRAIN"
            elif progress < 90:
                job_status.policy_state = "PROBE_BW"
            else:
                job_status.policy_state = "PROBE_BW"

            # Add occasional log messages
            if job_status.completed % 100 == 0:
                job_status.log_messages.append(
                    f"[{datetime.now().strftime('%H:%M:%S')}] Progress: {job_status.completed}/{job_status.total_urls} "
                    f"({job_status.progress:.1f}%) - Rate: {job_status.current_rate:.1f} req/s"
                )

        # Job completed
        job_status.log_messages.append(f"[{datetime.now().strftime('%H:%M:%S')}] Download complete!")
        job_status.log_messages.append(
            f"[{datetime.now().strftime('%H:%M:%S')}] Final: {job_status.successful} successful, "
            f"{job_status.failed} failed ({job_status.success_rate:.1f}% success rate)"
        )

    except asyncio.CancelledError:
        job_status.log_messages.append(f"[{datetime.now().strftime('%H:%M:%S')}] Job cancelled by user")
    finally:
        job_status.is_running = False
        job_status.policy_state = "IDLE"
        shutdown_requested = False


async def start_job():
    """Start the download job."""
    global job_task, shutdown_requested

    if job_status.is_running:
        ui.notify("Job already running!", type="warning")
        return

    # Validate required fields
    if not ui_state.config["input_path"]:
        ui.notify("Please specify an input file", type="negative")
        return
    if not ui_state.config["output_folder"]:
        ui.notify("Please specify an output folder", type="negative")
        return

    shutdown_requested = False
    job_task = asyncio.create_task(run_download_job())
    ui.notify("Download job started!", type="positive")


async def stop_job():
    """Stop the running job."""
    global shutdown_requested, job_task

    if not job_status.is_running:
        ui.notify("No job is running", type="warning")
        return

    shutdown_requested = True
    if job_task:
        job_task.cancel()
    ui.notify("Stopping job...", type="warning")


# -----------------------------------------
# UI Components
# -----------------------------------------

def create_header():
    """Create the application header."""
    with ui.header().classes('items-center justify-between'):
        with ui.row().classes('items-center gap-4'):
            ui.icon('cloud_download', size='lg').classes('text-white')
            ui.label('FLOW-DC').classes('text-2xl font-bold text-white')
            ui.label('Distributed Dataset Downloader').classes('text-sm text-gray-300')

        with ui.row().classes('items-center gap-2'):
            ui.button(icon='dark_mode', on_click=lambda: ui.dark_mode(True)).props('flat color=white')
            ui.button(icon='light_mode', on_click=lambda: ui.dark_mode(False)).props('flat color=white')


def create_config_panel():
    """Create the configuration panel."""
    with ui.card().classes('w-full'):
        ui.label('Configuration').classes('text-xl font-bold mb-4')

        with ui.tabs().classes('w-full') as tabs:
            basic_tab = ui.tab('Basic')
            advanced_tab = ui.tab('Advanced')
            rate_tab = ui.tab('Rate Control')

        with ui.tab_panels(tabs, value=basic_tab).classes('w-full'):
            # Basic Settings Tab
            with ui.tab_panel(basic_tab):
                with ui.column().classes('w-full gap-4'):
                    # Input/Output Section
                    ui.label('Input / Output').classes('text-lg font-semibold')

                    with ui.row().classes('w-full gap-4'):
                        ui.input(
                            label='Input File',
                            placeholder='/path/to/urls.parquet',
                            value=ui_state.config['input_path']
                        ).classes('flex-grow').bind_value(ui_state.config, 'input_path')

                        ui.select(
                            label='Format',
                            options=['parquet', 'csv', 'excel', 'xml'],
                            value=ui_state.config['input_format']
                        ).classes('w-32').bind_value(ui_state.config, 'input_format')

                    with ui.row().classes('w-full gap-4'):
                        ui.input(
                            label='Output Folder',
                            placeholder='/path/to/output',
                            value=ui_state.config['output_folder']
                        ).classes('flex-grow').bind_value(ui_state.config, 'output_folder')

                        ui.select(
                            label='Format',
                            options=['imagefolder', 'webdataset'],
                            value=ui_state.config['output_format']
                        ).classes('w-32').bind_value(ui_state.config, 'output_format')

                    # Column Mapping
                    ui.label('Column Mapping').classes('text-lg font-semibold mt-4')

                    with ui.row().classes('w-full gap-4'):
                        ui.input(
                            label='URL Column',
                            value=ui_state.config['url_col']
                        ).classes('flex-grow').bind_value(ui_state.config, 'url_col')

                        ui.input(
                            label='Label Column (optional)',
                            value=ui_state.config['label_col']
                        ).classes('flex-grow').bind_value(ui_state.config, 'label_col')

            # Advanced Settings Tab
            with ui.tab_panel(advanced_tab):
                with ui.column().classes('w-full gap-4'):
                    ui.label('Download Settings').classes('text-lg font-semibold')

                    with ui.row().classes('w-full gap-4'):
                        ui.number(
                            label='Concurrent Downloads',
                            value=ui_state.config['concurrent_downloads'],
                            min=1, max=10000
                        ).classes('flex-grow').bind_value(ui_state.config, 'concurrent_downloads')

                        ui.number(
                            label='Timeout (sec)',
                            value=ui_state.config['timeout_sec'],
                            min=1, max=600
                        ).classes('flex-grow').bind_value(ui_state.config, 'timeout_sec')

                        ui.number(
                            label='Max Retries',
                            value=ui_state.config['max_retry_attempts'],
                            min=0, max=10
                        ).classes('flex-grow').bind_value(ui_state.config, 'max_retry_attempts')

                    ui.label('Output Options').classes('text-lg font-semibold mt-4')

                    with ui.row().classes('w-full gap-4'):
                        ui.select(
                            label='Naming Mode',
                            options=['sequential', 'url_based'],
                            value=ui_state.config['naming_mode']
                        ).classes('flex-grow').bind_value(ui_state.config, 'naming_mode')

                        ui.checkbox(
                            'Create TAR archive',
                            value=ui_state.config['create_tar']
                        ).bind_value(ui_state.config, 'create_tar')

                        ui.checkbox(
                            'Create Overview JSON',
                            value=ui_state.config['create_overview']
                        ).bind_value(ui_state.config, 'create_overview')

            # Rate Control Tab
            with ui.tab_panel(rate_tab):
                with ui.column().classes('w-full gap-4'):
                    ui.checkbox(
                        'Enable PolicyBBR Rate Control',
                        value=ui_state.config['enable_polite_controller']
                    ).bind_value(ui_state.config, 'enable_polite_controller')

                    ui.label('Rate Limits').classes('text-lg font-semibold mt-2')

                    with ui.row().classes('w-full gap-4'):
                        ui.number(
                            label='Initial Rate (req/s)',
                            value=ui_state.config['initial_rate'],
                            min=1, max=100000
                        ).classes('flex-grow').bind_value(ui_state.config, 'initial_rate')

                        ui.number(
                            label='Min Rate (req/s)',
                            value=ui_state.config['min_rate'],
                            min=0.1, max=1000
                        ).classes('flex-grow').bind_value(ui_state.config, 'min_rate')

                        ui.number(
                            label='Max Rate (req/s)',
                            value=ui_state.config['max_rate'],
                            min=1, max=100000
                        ).classes('flex-grow').bind_value(ui_state.config, 'max_rate')

                    ui.label('Per-Host Concurrency').classes('text-lg font-semibold mt-4')

                    with ui.row().classes('w-full gap-4'):
                        ui.number(
                            label='Initial Concurrency',
                            value=ui_state.config['per_host_conc_init'],
                            min=1, max=1000
                        ).classes('flex-grow').bind_value(ui_state.config, 'per_host_conc_init')

                        ui.number(
                            label='Max Concurrency',
                            value=ui_state.config['per_host_conc_cap'],
                            min=1, max=10000
                        ).classes('flex-grow').bind_value(ui_state.config, 'per_host_conc_cap')

                        ui.number(
                            label='Control Interval (sec)',
                            value=ui_state.config['control_interval_sec'],
                            min=0.1, max=60
                        ).classes('flex-grow').bind_value(ui_state.config, 'control_interval_sec')

        # Config file actions
        with ui.row().classes('w-full gap-4 mt-4'):
            async def load_config():
                result = await app.native.main_window.create_file_dialog(
                    allow_multiple=False,
                    file_types=[('JSON files', '*.json')]
                )
                if result:
                    if ui_state.load_config_file(result[0]):
                        ui.notify('Configuration loaded!', type='positive')
                    else:
                        ui.notify('Failed to load configuration', type='negative')

            ui.button('Load Config', icon='folder_open', on_click=lambda: ui.notify('Use file path input below')).props('outline')

            config_path_input = ui.input(placeholder='Config file path...').classes('flex-grow')
            ui.button('Load', on_click=lambda: (
                ui_state.load_config_file(config_path_input.value) and ui.notify('Loaded!', type='positive')
            ) if config_path_input.value else ui.notify('Enter path first', type='warning')).props('outline')


def create_status_panel():
    """Create the job status and monitoring panel."""
    with ui.card().classes('w-full'):
        ui.label('Job Status').classes('text-xl font-bold mb-4')

        # Control buttons
        with ui.row().classes('w-full gap-4 mb-4'):
            ui.button('Start Download', icon='play_arrow', on_click=start_job).props('color=positive')
            ui.button('Stop', icon='stop', on_click=stop_job).props('color=negative')

        # Status indicators
        with ui.row().classes('w-full gap-8 mb-4'):
            with ui.column().classes('items-center'):
                status_label = ui.label('IDLE').classes('text-2xl font-bold')
                ui.label('Status').classes('text-sm text-gray-500')

            with ui.column().classes('items-center'):
                elapsed_label = ui.label('00:00:00').classes('text-2xl font-bold font-mono')
                ui.label('Elapsed').classes('text-sm text-gray-500')

            with ui.column().classes('items-center'):
                policy_label = ui.label('IDLE').classes('text-2xl font-bold')
                ui.label('PolicyBBR State').classes('text-sm text-gray-500')

        # Progress bar
        progress_bar = ui.linear_progress(value=0, show_value=False).classes('w-full')
        progress_text = ui.label('0 / 0 URLs (0.0%)').classes('text-center w-full')

        # Statistics cards
        with ui.row().classes('w-full gap-4 mt-4'):
            with ui.card().classes('flex-grow'):
                successful_label = ui.label('0').classes('text-3xl font-bold text-green-500')
                ui.label('Successful').classes('text-sm text-gray-500')

            with ui.card().classes('flex-grow'):
                failed_label = ui.label('0').classes('text-3xl font-bold text-red-500')
                ui.label('Failed').classes('text-sm text-gray-500')

            with ui.card().classes('flex-grow'):
                rate_label = ui.label('0.0').classes('text-3xl font-bold text-blue-500')
                ui.label('Rate (req/s)').classes('text-sm text-gray-500')

            with ui.card().classes('flex-grow'):
                throughput_label = ui.label('0.0').classes('text-3xl font-bold text-purple-500')
                ui.label('Throughput (MB/s)').classes('text-sm text-gray-500')

        # Update function
        def update_status():
            if job_status.is_running:
                status_label.text = 'RUNNING'
                status_label.classes('text-2xl font-bold text-green-500', remove='text-yellow-500 text-gray-500')
            elif job_status.completed > 0:
                status_label.text = 'COMPLETED'
                status_label.classes('text-2xl font-bold text-blue-500', remove='text-green-500 text-gray-500')
            else:
                status_label.text = 'IDLE'
                status_label.classes('text-2xl font-bold text-gray-500', remove='text-green-500 text-blue-500')

            elapsed_label.text = job_status.elapsed_time
            policy_label.text = job_status.policy_state

            progress_bar.value = job_status.progress / 100
            progress_text.text = f'{job_status.completed} / {job_status.total_urls} URLs ({job_status.progress:.1f}%)'

            successful_label.text = str(job_status.successful)
            failed_label.text = str(job_status.failed)
            rate_label.text = f'{job_status.current_rate:.1f}'
            throughput_label.text = f'{job_status.throughput_mbps:.2f}'

        # Timer for real-time updates
        ui.timer(0.5, update_status)


def create_log_panel():
    """Create the log output panel."""
    with ui.card().classes('w-full'):
        ui.label('Logs').classes('text-xl font-bold mb-4')

        log_area = ui.log(max_lines=100).classes('w-full h-64')

        def update_logs():
            while len(job_status.log_messages) > 0:
                msg = job_status.log_messages.pop(0)
                log_area.push(msg)

        ui.timer(0.5, update_logs)

        with ui.row().classes('w-full gap-2 mt-2'):
            ui.button('Clear Logs', icon='delete', on_click=lambda: log_area.clear()).props('outline size=sm')


# -----------------------------------------
# Main Application
# -----------------------------------------

@ui.page('/')
def main_page():
    """Main application page."""
    ui.dark_mode(True)

    create_header()

    with ui.column().classes('w-full max-w-6xl mx-auto p-4 gap-4'):
        # Title section
        with ui.row().classes('w-full items-center justify-between mb-4'):
            with ui.column():
                ui.label('Download Dashboard').classes('text-3xl font-bold')
                ui.label('Configure and monitor your dataset downloads').classes('text-gray-500')

        # Main content in two columns
        with ui.row().classes('w-full gap-4'):
            # Left column - Configuration
            with ui.column().classes('flex-grow'):
                create_config_panel()

            # Right column - Status
            with ui.column().classes('flex-grow'):
                create_status_panel()

        # Full width log panel
        create_log_panel()

        # Footer
        with ui.row().classes('w-full justify-center mt-4'):
            ui.label('FLOW-DC - Flexible Large-scale Orchestrated Workflow for Data Collection').classes('text-sm text-gray-500')


# -----------------------------------------
# Entry Point
# -----------------------------------------

if __name__ in {"__main__", "__mp_main__"}:
    ui.run(
        title='FLOW-DC',
        port=8080,
        reload=True,
        dark=True,
    )
