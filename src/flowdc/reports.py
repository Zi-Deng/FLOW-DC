"""Human-readable summaries derived from the committed row index."""

from collections import Counter
from dataclasses import asdict

from .http import HTTP_MEASUREMENT_VERSION


def generate_overview_report(*, cfg, df_total, outcomes, elapsed_sec):
    successful = [o for o in outcomes.values() if o.success]
    failed = [o for o in outcomes.values() if not o.success]
    payload_bytes = sum(o.bytes_downloaded for o in successful)
    errors = Counter((o.status_code, o.error) for o in failed)
    return {
        "version": "3.0.0",
        "http_measurement": {
            "version": HTTP_MEASUREMENT_VERSION,
            "controller_latency": "final-hop dispatch to first nonempty body read; eligible completed responses",
            "clock": "time.monotonic",
            "retry_after_scope": "session-local hostname/effective-port authority including redirects",
        },
        "configuration": asdict(cfg),
        "summary": {
            "total_urls": df_total,
            "successful_downloads": len(successful),
            "failed_downloads": len(failed),
            "unattempted_or_cancelled_urls": max(0, df_total - len(outcomes)),
            "success_rate_percent": round(100 * len(successful) / df_total, 2) if df_total else 0.0,
            "downloaded_mb": payload_bytes / 1e6,
            "elapsed_sec": elapsed_sec,
            "avg_speed_MBps": payload_bytes / elapsed_sec / 1e6 if elapsed_sec else 0.0,
            "tar_path": None,
        },
        "error_breakdown": [
            {"status_code": status, "error": error, "count": count}
            for (status, error), count in errors.items()
        ],
    }
