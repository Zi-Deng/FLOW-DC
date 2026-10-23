"""Offline population preparation and explicitly guest-only health probing."""
import asyncio
import json
import random
import socket
import ssl
import statistics
import time
import urllib.error
import urllib.request
import urllib.robotparser
from pathlib import Path

import polars as pl

from .common import (HOST, MAX_OBJECT, PREFIX, OPTIONS, NoRedirect, bounded_read, checked_url, decode_image,
                     file_hash, opener, read_json, remaining, require, source_identity, terminal, tls_context, utc, write_new)
from .design import schedule, slice_size
from .cooldown import user_seconds

UA = "FLOW-DC/2.0 PAARC/2.0"


def robots_permit(text, urls):
    parser = urllib.robotparser.RobotFileParser()
    parser.parse(text.splitlines())
    matched = [e for e in parser.entries if e.applies_to(UA)]
    if not matched and parser.default_entry:
        matched = [parser.default_entry]
    require(len(matched) <= 1, "unsupported combined robots groups")
    for entry in matched:
        require(not entry.delay and not entry.req_rate, "robots imposes pacing")
        require(not any("*" in urllib.parse.unquote(r.path) or "$" in r.path for r in entry.rulelines), "unsupported robots pattern")
    require(all(parser.can_fetch(UA, url) for url in urls), "robots disallows selected paths")


def prepare(study, source, previous, *, user_cooldown_seconds=0):
    cooldown = user_seconds({"user_cooldown_seconds": user_cooldown_seconds})
    study, source, previous = Path(study).resolve(), Path(source).resolve(), Path(previous).resolve()
    require(not study.exists(), "study already exists; never replace first attempts")
    original = read_json(previous / "population-record.json")
    require(file_hash(previous / "population.parquet") == original["population_sha256"], "old population changed")
    # All planned old slices were actually requested; nevertheless use actual audit/probe URLs.
    seen = {read_json_line["url"] for read_json_line in read_lines(previous / "probe-results.jsonl")}
    old_hashes={"probe-results.jsonl":file_hash(previous/"probe-results.jsonl")}
    for path in previous.glob("runs/*/request-audit.jsonl"):
        require(file_hash(path)==read_json(path.parent/"record.json")["evidence_sha256"]["request-audit.jsonl"], "old request evidence changed")
        old_hashes[str(path.relative_to(previous))]=file_hash(path)
        seen.update(e["url"] for e in read_lines(path) if e["event"] == "dispatch")
    frame = pl.read_parquet(previous / "population.parquet").filter(~pl.col("url").is_in(list(seen)))
    require(frame["url"].n_unique() == frame.height and frame.height > 4*8192+24*8192+300, "population insufficient")
    for url in frame["url"]:
        checked_url(url)
    study.mkdir(parents=True, mode=0o700)
    (study / "slices").mkdir()
    rng = random.Random(20261019)
    indices = rng.sample(range(frame.height), 300+4*8192)
    probe = frame[indices[:300]]
    probe.write_parquet(study / "probe.parquet")
    remaining = frame.filter(~pl.col("url").is_in(probe["url"]))
    calibration_urls = []
    runs = schedule()
    for i, run in enumerate(runs[:4]):
        chosen = frame[indices[300+i*8192:300+(i+1)*8192]]
        chosen.write_parquet(study / "slices" / f"{run['slice']}.parquet")
        calibration_urls.extend(chosen["url"].to_list())
    remaining.filter(~pl.col("url").is_in(calibration_urls)).write_parquet(study / "main-population.parquet")
    write_new(study / "schedule.json", {"runs": runs, "seed": 20261019, "order": "all adaptive before all Fixed"})
    names = ["schedule.json", "probe.parquet", "main-population.parquet"] + [f"slices/{r['slice']}.parquet" for r in runs[:4]]
    write_new(study / "protocol.json", {"schema": "flowdc-external-concurrency-v1", "utc": utc(),
              "user_cooldown_seconds": cooldown,
              "source": source_identity(source), "options": OPTIONS, "previous_population": original,
              "old_request_evidence":old_hashes,
              "excluded_old_request_urls": len(seen), "files": {name: file_hash(study/name) for name in names},
              "attempts_per_row": 1, "max_concurrency": 256, "main_repetitions": 6,
              "allocation": "renewed to2027-04-17; exact current balance/cutoff supplied separately"})


def read_lines(path):
    with Path(path).open() as stream:
        return [json.loads(line) for line in stream]


def guest_check(study):
    receipt = read_json(Path(study) / "execution.json")
    require(receipt["executor"] == "jetstream2" and receipt["vm_id"] and receipt["campaign_id"], "guest execution receipt required")
    require(socket.gethostname() == receipt["hostname"], "wrong guest")
    require(time.time() < receipt["deadline_utc"], "campaign expired")
    # Self-declared UUID/hostname is insufficient: refuse accidental local runs.
    metadata=urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    with metadata.open("http://169.254.169.254/openstack/latest/meta_data.json",timeout=3) as response:
        identity=json.loads(bounded_read(response,65536))
    require(identity.get("uuid")==receipt["vm_id"],"cloud metadata UUID mismatch; local acquisition refused")


def host_gate(study):
    guest_check(study)
    ignore = False
    try:
        with socket.create_connection((HOST, 443), timeout=20) as raw:
            with tls_context().wrap_socket(raw, server_hostname=HOST):
                pass
    except ssl.SSLCertVerificationError as error:
        require(error.verify_code in (9, 10), "TLS error is not certificate validity period")
        ignore = True
    with socket.create_connection((HOST, 443), timeout=20) as raw:
        with tls_context(ignore).wrap_socket(raw, server_hostname=HOST) as tls:
            cert, peer = tls.getpeercert(), tls.getpeername()
    gate = dict(utc=utc(), ignore_time=ignore, chain_and_hostname_verified=True, certificate=cert,
                peer=peer, resolved_addresses=sorted({r[4][0] for r in socket.getaddrinfo(HOST,443)}))
    try:
        with opener(ignore).open(urllib.request.Request(f"https://{HOST}/robots.txt", headers={"User-Agent": UA}), timeout=20) as response:
            gate.update(robots_status=response.status, robots_txt=bounded_read(response, 512*1024).decode("utf8", "replace"))
        robots_permit(gate["robots_txt"], [PREFIX+"twmoth142/twmoth709671.jpg"])
    except urllib.error.HTTPError as error:
        require(error.code == 404, "robots access unavailable/refused")
        gate["robots_status"] = 404
    write_new(Path(study) / "host-gate.json", gate)


async def probe(study):
    import aiohttp
    guest_check(study)
    gate = read_json(study / "host-gate.json")
    rows = pl.read_parquet(study / "probe.parquet")["url"].to_list()
    if gate.get("robots_txt"):
        robots_permit(gate["robots_txt"], rows)
    trace = aiohttp.TraceConfig()
    async def dispatch(session, ctx, params):
        ctx.trace_request_ctx["sent"] = time.monotonic()
    trace.on_request_headers_sent.append(dispatch)
    results, reason, transport_errors = [], None, 0
    with (study / "probe-results.jsonl").open("x") as log:
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=1, ssl=tls_context(gate["ignore_time"])),
                    trace_configs=[trace], headers={"User-Agent": UA}, trust_env=False) as session:
            require(aiohttp.__version__ == "3.13.3", "pinned probe runtime required")
            session._retry_connection = False
            for url in rows:
                if remaining(study)<45:
                    reason="probe_deadline";break
                checked_url(url)
                timing, body = {}, bytearray()
                entry=dict(url=url,status=None,bytes=0,valid=False,ttfb_s=None,error=None)
                try:
                    async with session.get(url, allow_redirects=False, timeout=aiohttp.ClientTimeout(total=30), trace_request_ctx=timing) as response:
                        entry.update(status=response.status,retry_after=response.headers.get("Retry-After"))
                        if response.status in (401,403,429) or response.status>=500:
                            reason="probe_host_refusal"
                        else:
                            first = await response.content.read(1)
                            entry["ttfb_s"] = time.monotonic()-timing["sent"] if first else None
                            body.extend(first)
                            async for chunk in response.content.iter_chunked(65536):
                                body.extend(chunk)
                                require(len(body) <= MAX_OBJECT, "probe object too large")
                            if response.status == 200:
                                try:
                                    decode_image(body);entry["valid"]=True
                                except Exception:
                                    pass
                except (aiohttp.ClientError,asyncio.TimeoutError,ValueError) as error:
                    entry["error"]=f"{type(error).__name__}: {error}"
                    transport_errors+=1
                    if isinstance(error,aiohttp.ClientSSLError) or transport_errors>=5 or isinstance(error,ValueError):
                        reason="probe_transport_or_content_failure"
                entry["bytes"]=len(body)
                results.append(entry)
                log.write(json.dumps(entry)+"\n");log.flush()
                if reason:break
                await asyncio.sleep(1)
    good = [r for r in results if r["valid"]]
    passed=len(results)==300 and len(good)>=270 and reason is None
    summary=dict(passed=passed,reason=reason or (None if passed else "probe_coverage"),requests=len(results),valid=len(good),
                 mean_bytes=statistics.mean(r["bytes"] for r in good) if good else None,
                 ttfb_p50_s=statistics.median(r["ttfb_s"] for r in good) if good else None)
    write_new(study/"probe-summary.json",summary)
    if not passed:terminal(study,summary["reason"])
    return summary


def build_main(study):
    records = [read_json(study/"runs"/r["run_id"]/"record.json") for r in read_json(study/"schedule.json")["runs"][:4]]
    frame = pl.read_parquet(study / "main-population.parquet")
    plan = slice_size(records, read_json(study / "probe-summary.json")["mean_bytes"], frame.height)
    n = plan["rows_per_main_run"]
    rng = random.Random(20261020)
    chosen = frame[sorted(rng.sample(range(frame.height), n*24))].sort("url")
    bins = [[] for _ in range(24)]
    for at in range(0, chosen.height, 24):
        destinations = list(range(24)); rng.shuffle(destinations)
        for offset, dest in enumerate(destinations):
            bins[dest].append(at+offset)
    files = {}
    for run, indices in zip(read_json(study / "schedule.json")["runs"][4:], bins):
        rng.shuffle(indices)
        name = f"slices/{run['slice']}.parquet"
        chosen[indices].write_parquet(study / name)
        files[name] = file_hash(study / name)
    write_new(study / "main-plan.json", {**plan, "protocol_sha256": file_hash(study/"protocol.json"),
               "files": files, "calibration_records": {r["run_id"]:file_hash(study/"runs"/r["run_id"]/"record.json") for r in records}})
