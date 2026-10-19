"""Prospective allocation and deterministic late Fixed selection."""
import math
import random
import statistics

from .common import METHODS, OPTIONS, PROFILE, finite, require


def schedule(seed=20261019):
    rng = random.Random(seed)
    runs = []
    for phase, blocks in (("calibration", 2), ("adaptive", 6), ("fixed", 6)):
        arms = list(METHODS) if phase != "fixed" else ["fixed-low", "fixed-high"]
        orders = [arms[:], arms[::-1]] * (blocks // 2)
        rng.shuffle(orders)
        for block, order in enumerate(orders, 1):
            for position, arm in enumerate(order, 1):
                runs.append(dict(run_id=f"{phase}-{block:02}-{position}-{arm}", phase=phase,
                                 block=block, position=position, arm=arm,
                                 method="fixed-v1" if phase == "fixed" else arm,
                                 slice=f"{len(runs):02}"))
    return runs


def config(run, study, run_dir, selected=None):
    concurrency = selected[run["arm"]] if run["phase"] == "fixed" else None
    return dict(input=str(study / "slices" / f"{run['slice']}.parquet"), input_format="parquet",
                output=str(run_dir / "native"), output_format="webdataset", url="url", label=None,
                concurrent_downloads=256, timeout=30, enable_paarc=True,
                control_method=run["method"], method_options=OPTIONS[run["method"]],
                C_min=concurrency or 2, C_init=concurrency or 4, C_max=concurrency or 256,
                max_retry_attempts=1, research_profile=True, research_workload=PROFILE,
                create_tar=True, compress_tar=False, create_overview=True, force_overwrite=False)


def slice_size(calibration, mean_bytes, available_rows):
    require(len(calibration) == 4 and all(r["complete"] for r in calibration), "four complete calibrations required")
    require(finite(mean_bytes) and mean_bytes > 0, "invalid size estimate")
    rate = max(r["verification"]["credited_rows"] / r["acquisition_s"] for r in calibration)
    require(rate > 0, "calibration rate unavailable")
    wanted = max(8192, math.ceil(120 * rate / 256) * 256)
    upper = min(32768, available_rows // 24, math.floor(4 * 2**30 / (1.25 * mean_bytes))) // 256 * 256
    # Measured costs constrain size prospectively, before any main outcome.
    for name,maximum in (("verification_s",300),("finalization_s",180)):
        per_row=max(r.get(name,0)/r.get("rows",8192) for r in calibration)
        if per_row>0:
            upper=min(upper,math.floor((maximum-30)/(3*per_row)/256)*256)
    rows = min(wanted, upper)
    require(rows >= 8192 and rows/rate >= 60, "population/bytes cannot support sustained main observations")
    return {"rows_per_main_run": rows, "target_rows": wanted, "predicted_fastest_acquisition_s": rows/rate,
            "fastest_calibration_credited_rows_per_s": rate, "expected_bytes_per_run": rows*mean_bytes}


def windows(events, baseline, started):
    """Ten-second bins; samples measure actual network activity, not allowed cap."""
    require(finite(baseline) and baseline > 0, "invalid probe baseline")
    bins = {}
    for e in events:
        at = e["monotonic_s"] - started
        if at < 10:
            continue
        b = bins.setdefault(int(at // 10), {"samples": [], "headers": [], "ends": []})
        field = {"sample": "samples", "headers": "headers", "network_end": "ends"}.get(e["event"])
        if field=="ends" and not e.get("dispatched",True):
            continue
        if field:
            b[field].append(e)
    intervals=activity_intervals(events,started)
    result = []
    for i, b in sorted(bins.items()):
        samples, ends, headers = b["samples"], b["ends"], b["headers"]
        delays = [e["ttfb_s"] for e in ends if e.get("ttfb_s") is not None]
        eligible = len(samples) >= 9 and sum(e["backlog"] > 0 for e in samples) / len(samples) >= .9 and len(ends) >= 100
        clean = eligible and delays and not any(e["status"] in (429, 503) for e in headers)
        clean = bool(clean and sum(e.get("failure_kind") == "transport" for e in ends)/len(ends) < .01
                     and statistics.median(delays) <= 1.5*baseline)
        weighted=[(count,max(0,min(end,started+10*(i+1))-max(begin,started+10*i)))
                  for begin,end,count,backlogged in intervals if backlogged]
        result.append(dict(bin=i, clean=clean, eligible=eligible,
                           median_inflight=weighted_quantile(weighted,.5) if any(w>0 for _,w in weighted) else 0))
    return result


def activity_intervals(events,started,warmup=10):
    previous,active,backlogged=started,0,False
    out=[]
    for e in sorted(events,key=lambda e:e["monotonic_s"]):
        now=e["monotonic_s"]
        if now>max(previous,started+warmup):
            out.append((max(previous,started+warmup),now,active,backlogged))
        if "inflight" in e:
            active=e["inflight"]
            require(type(active) is int and 0<=active<=256,"invalid observed concurrency")
        if e["event"]=="sample":
            backlogged=e["backlog"]>0
        previous=now
    return out


def weighted_quantile(values,q):
    values=sorted((v,w) for v,w in values if w>0)
    require(values and 0<q<=1,"no positive exposure for quantile")
    target=q*sum(w for _,w in values);total=0
    for value,weight in values:
        total+=weight
        if total>=target:
            return value
    return values[-1][0]


def select_fixed(records, audits, baseline):
    require(len(records) == 16 and len(audits) == 16, "all four calibration and twelve adaptive first attempts required")
    require(sum(r["phase"]=="calibration" for r in records)==4 and sum(r["phase"]=="adaptive" for r in records)==12,
            "selection phase counts changed")
    clean, exposure, signals = [], [], []
    for record, events in zip(records, audits):
        require(record["phase"] in ("calibration", "adaptive"), "Fixed evidence cannot select Fixed")
        for w in windows(events, baseline, record["started_monotonic"]):
            if w["clean"] and w["median_inflight"] > 0:
                clean.append(w["median_inflight"])
        exposure.extend((count,end-begin) for begin,end,count,backlogged in activity_intervals(events,record["started_monotonic"]) if backlogged)
        dispatch = {e["token"]: e for e in events if e["event"] == "dispatch"}
        for e in events:
            if e["event"] == "headers" and e["status"] == 429:
                require(e["token"] in dispatch and finite(dispatch[e["token"]].get("cap")), "429 missing dispatch-time cap")
                signals.append(dispatch[e["token"]]["cap"])
    require(clean and exposure, "no qualifying clean adaptive window for Fixed-low")
    c_clean, signal = max(clean), min(signals) if signals else None
    low = max(1, math.floor(.8 * min(c_clean, signal if signal is not None else c_clean)))
    q95 = weighted_quantile(exposure,.95)
    high = min(256, max(low+1, signal if signal is not None else math.ceil(1.5*q95)))
    require(1 <= low < high <= 256, "Fixed settings not distinct or outside bounds")
    return {"fixed-low": low, "fixed-high": high, "C_clean": c_clean, "S": signal, "Q95": q95,
            "high_interpretation": "observed-429 setting" if signals else "stress candidate; rate limiting unestablished"}
