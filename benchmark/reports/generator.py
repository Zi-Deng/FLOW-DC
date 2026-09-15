"""Report generation for benchmark results."""

import json
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from benchmark.core.metrics import BenchmarkReport


class ReportGenerator:
    """Generates JSON and HTML reports from benchmark results."""

    def __init__(self, output_dir: Path):
        """Initialize generator.

        Args:
            output_dir: Directory to write reports to
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Setup Jinja2 environment
        template_dir = Path(__file__).parent / "templates"
        self.jinja_env = Environment(
            loader=FileSystemLoader(str(template_dir)),
            autoescape=True,
        )

    def generate_json(self, report: BenchmarkReport, filename: str = "report.json") -> Path:
        """Generate JSON report.

        Args:
            report: Benchmark report data
            filename: Output filename

        Returns:
            Path to generated file
        """
        output_path = self.output_dir / filename
        with open(output_path, "w") as f:
            json.dump(report.to_dict(), f, indent=2)
        return output_path

    def generate_html(self, report: BenchmarkReport, filename: str = "report.html") -> Path:
        """Generate HTML report with charts.

        Args:
            report: Benchmark report data
            filename: Output filename

        Returns:
            Path to generated file
        """
        template = self.jinja_env.get_template("report.html.j2")

        # Prepare data for charts
        chart_data = self._prepare_chart_data(report)

        html_content = template.render(
            report=report.to_dict(),
            chart_data=json.dumps(chart_data),
            metadata=report.to_dict()["metadata"],
        )

        output_path = self.output_dir / filename
        with open(output_path, "w") as f:
            f.write(html_content)
        return output_path

    def _prepare_chart_data(self, report: BenchmarkReport) -> dict:
        """Prepare data structures for Chart.js visualizations."""
        data = {
            "throughput": {"labels": [], "datasets": []},
            "success_rate": {"labels": [], "datasets": []},
            "cpu_usage": {"labels": [], "datasets": []},
            "memory_usage": {"labels": [], "datasets": []},
        }

        # Use concurrency levels as labels
        data["throughput"]["labels"] = [str(c) for c in report.concurrency_levels]
        data["success_rate"]["labels"] = [str(c) for c in report.concurrency_levels]
        data["cpu_usage"]["labels"] = [str(c) for c in report.concurrency_levels]
        data["memory_usage"]["labels"] = [str(c) for c in report.concurrency_levels]

        # Colors for different tools
        colors = {
            "flowdc_paarc_enabled": {"bg": "rgba(54, 162, 235, 0.5)", "border": "rgb(54, 162, 235)"},
            "flowdc_paarc_disabled": {"bg": "rgba(255, 159, 64, 0.5)", "border": "rgb(255, 159, 64)"},
            "img2dataset_default": {"bg": "rgba(255, 99, 132, 0.5)", "border": "rgb(255, 99, 132)"},
        }

        # Build datasets for each tool
        for tool_key, concurrency_results in report.results.items():
            color = colors.get(tool_key, {"bg": "rgba(128, 128, 128, 0.5)", "border": "rgb(128, 128, 128)"})

            throughput_data = []
            success_data = []
            cpu_data = []
            memory_data = []

            for conc in report.concurrency_levels:
                if conc in concurrency_results:
                    agg = concurrency_results[conc]
                    throughput_data.append(agg.avg_throughput_mbps)
                    success_data.append(agg.avg_success_rate)
                    cpu_data.append(agg.avg_cpu_percent)
                    memory_data.append(agg.avg_memory_mb)
                else:
                    throughput_data.append(None)
                    success_data.append(None)
                    cpu_data.append(None)
                    memory_data.append(None)

            # Add to chart datasets
            label = tool_key.replace("_", " ").title()

            data["throughput"]["datasets"].append({
                "label": label,
                "data": throughput_data,
                "backgroundColor": color["bg"],
                "borderColor": color["border"],
                "borderWidth": 2,
            })

            data["success_rate"]["datasets"].append({
                "label": label,
                "data": success_data,
                "backgroundColor": color["bg"],
                "borderColor": color["border"],
                "borderWidth": 2,
            })

            data["cpu_usage"]["datasets"].append({
                "label": label,
                "data": cpu_data,
                "backgroundColor": color["bg"],
                "borderColor": color["border"],
                "borderWidth": 2,
            })

            data["memory_usage"]["datasets"].append({
                "label": label,
                "data": memory_data,
                "backgroundColor": color["bg"],
                "borderColor": color["border"],
                "borderWidth": 2,
            })

        return data

    def generate_all(self, report: BenchmarkReport) -> dict[str, Path]:
        """Generate all report formats.

        Args:
            report: Benchmark report data

        Returns:
            Dict mapping format to output path
        """
        return {
            "json": self.generate_json(report),
            "html": self.generate_html(report),
        }
