"""Run-stop classification and one finite source health check; never a retry."""

import time
import urllib.error
import urllib.request
import urllib.robotparser
from pathlib import Path

import polars as pl

from .common import decode_image, read_json, require, utc, write_new
from .prepare import guest_check
from .public_guard import MAX_OBJECT, UA, NoRedirect, PublicPolicy, checked_url
from .common import tls_context


def stop_scope(record):
    if record["status"] == "complete":
        return None
    if record.get("worker_closed") is False:
        return "campaign"
    if record.get("stop_scope"):
        require(record["stop_scope"] in ("run", "source", "campaign"), "unknown stop scope")
        return record["stop_scope"]
    evidence = record.get("stop_evidence", [record.get("transport", {})])
    statuses = {str(k) for e in evidence for k, n in e.get("status_counts", {}).items() if n}
    reasons = {r for e in evidence for r in e.get("stop_reasons", [])}
    if statuses.intersection({"401", "403", "429"}) or any(s.startswith("3") for s in statuses):
        return "source"
    if reasons.intersection({"robots_header", "tls_failure", "dead_link_fraction", "transport_fraction"}):
        return "source"
    if "503" in statuses:
        return "run"
    # A verified worker closure can be a bounded native/resource failure. An
    # unverified manager/task failure cannot authorize the next acquisition.
    return "run" if record.get("worker_closed") else "campaign"


def recovery_probe(study, source, after_run):
    study = Path(study)
    require(Path(after_run).name == after_run and after_run not in (".", ".."), "unsafe recovery identity")
    guest_check(study)
    path = study / "gates" / ("recovery-" + after_run + ".json")
    if path.exists():
        return read_json(path)
    intent = path.with_suffix(".intent.json")
    result = dict(source=source, after_run=after_run, status="failed", requests=0,
                  probe_body_bytes=0, started_utc=utc(), observations=[])
    if intent.exists():
        result.update(reason="interrupted recovery probe; no replay", requests=4,
                      probe_body_bytes=4 * MAX_OBJECT, body_basis="conservative unclosed reservation")
        write_new(path, result)
        return result
    gate = read_json(study / "gates" / (source + ".json"))
    urls = pl.read_parquet(study / "pools" / (source + ".parquet"))["url"].head(4).to_list()
    require(len(urls) == 4, "four fixed health rows required")
    write_new(intent, dict(source=source, after_run=after_run, urls=urls, utc=utc()))
    policy = PublicPolicy(1, latency_stop=False)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect(),
        urllib.request.HTTPSHandler(context=tls_context(gate["ignore_time"])))
    robots = urllib.robotparser.RobotFileParser()
    robots.parse(gate.get("robots_txt", "").splitlines())
    deadline = time.monotonic() + 150
    try:
        for url in urls:
            checked_url(url, source)
            require(not gate.get("robots_txt") or robots.can_fetch(UA, url), "recovery robots refusal")
            require(time.monotonic() < deadline, "recovery deadline")
            result["requests"] += 1
            response = None
            try:
                response = opener.open(urllib.request.Request(url, headers={"User-Agent": UA}),
                                       timeout=min(30, max(1, deadline - time.monotonic())))
            except urllib.error.HTTPError as error:
                response = error
            try:
                policy.inspect(response.headers)
                policy.header(time.monotonic(), response.code, response.headers.get("Retry-After"))
                result["observations"].append(dict(url=url, status=response.code,
                    retry_after=response.headers.get("Retry-After")))
                require(not policy.reason and response.code == 200, "recovery source response refused")
                body = response.read(MAX_OBJECT + 1)
                require(len(body) <= MAX_OBJECT, "oversized recovery object")
                result["probe_body_bytes"] += len(body)
                decode_image(body)
            finally:
                response.close()
        result["status"] = "passed"
    except Exception as error:
        result.update(reason=type(error).__name__ + ": " + str(error),
                      probe_body_bytes=4 * MAX_OBJECT, body_basis="conservative failed probe reservation")
    result.update(closed_utc=utc(), server_retry_after_until_utc=policy.server_retry_after_until)
    write_new(path, result)
    return result
