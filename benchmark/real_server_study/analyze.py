"""Offline analysis; randomized adaptive inference and descriptive late Fixed."""
import itertools
import math
import time
from pathlib import Path

import numpy as np
from scipy import stats

from .common import file_hash, read_json, require, replace_derived, write_new


def summary(values, alpha=.05):
    x = np.asarray(values,dtype=float)
    require(np.isfinite(x).all(), "nonfinite statistical input")
    if not len(x):
        return dict(n=0,mean=None,ci=None,p=None)
    mean = float(x.mean())
    if len(x)<2:
        return dict(n=1,mean=mean,ci=None,p=None)
    se = float(x.std(ddof=1)/math.sqrt(len(x)))
    half = float(stats.t.ppf(1-alpha/2,len(x)-1))*se
    p = float(2*stats.t.sf(abs(mean)/se,len(x)-1)) if se else 0.0 if mean else 1.0
    return dict(n=len(x),mean=mean,ci=[mean-half,mean+half],p=p)


def randomization(values):
    # Historical output key exact_randomization_p denotes this 64-sign-flip
    # sensitivity. It does not enumerate the balanced 3/3 order assignment.
    require(len(values) == 6, "paired sign-flip sensitivity requires six complete pairs")
    observed = abs(sum(values))
    null = [abs(sum(s*d for s,d in zip(signs,values))) for signs in itertools.product((-1,1),repeat=6)]
    return sum(v >= observed-1e-9 for v in null)/len(null)


def lower(values):
    s = summary(values)
    # One-sided alpha .025 equals the lower endpoint of an ordinary 95% t interval.
    return s["ci"][0] if s["ci"] else None


def audit_record(directory,r,study,deadline):
    for name,digest in r["evidence"].items():
        require(not Path(name).is_absolute() and ".." not in Path(name).parts and
                file_hash(directory/name,deadline)==digest,"retained evidence changed")
    if r["complete"]:
        binding=read_json(directory/"attempt.json")
        require(binding["protocol_sha256"]==file_hash(study/"protocol.json",deadline),"attempt protocol binding changed")
        for field,name in (("main_plan_sha256","main-plan.json"),("fixed_selection_sha256","fixed-selection.json")):
            if field in binding:require(binding[field]==file_hash(study/name,deadline),"attempt selection binding changed")
        receipt=read_json(directory/"verification.json")
        require(receipt.pop("observations")==r["observations"] and receipt==r["verification"],"verification receipt and results differ")
        overview=read_json(directory/"native/overview.json")
        for spec in overview["http_observations"]["files"]+overview["control_trajectories"]:
            name=spec["path"]
            require(not Path(name).is_absolute() and ".." not in Path(name).parts and
                    file_hash(directory/"native"/name,deadline)==spec["sha256"],"retained observations/trace changed")
        require(r["elapsed_s"] == r["ended_monotonic"]-r["started_monotonic"] and
                r["goodput_Bps"] == r["verification"]["credited_bytes"]/r["elapsed_s"] and
                r["coverage"] == r["verification"]["credited_rows"]/r["rows"], "primary arithmetic mismatch")


def analyze(study,seconds=None):
    study = Path(study)
    deadline=time.monotonic()+seconds if seconds is not None else None
    scheduled = read_json(study/"schedule.json")["runs"]
    rows, hashes = [], {}
    for run in scheduled:
        directory = study/"runs"/run["run_id"]
        record_path=directory/"record.json"
        if not record_path.exists() and (directory/"incomplete-record.json").exists():
            record_path=directory/"incomplete-record.json"
        if not record_path.exists():
            rows.append({**run,"status":"attempted_without_record" if directory.exists() else "unrun","complete":False})
            continue
        r = read_json(record_path)
        require(all(r[k] == v for k,v in run.items()), "scheduled run changed")
        hashes[run["run_id"]] = {"file":record_path.name,"sha256":file_hash(record_path)}
        try:
            audit_record(directory,r,study,deadline)
        except TimeoutError:
            rows.append({**run,"complete":False,"status":"analysis_incomplete","goodput_Bps":None,"coverage":None})
            continue
        rows.append(r)
    by = {(r["block"],r["arm"]):r for r in rows if r["phase"]=="adaptive" and r["complete"]}
    pairs = [(by[(b,"gradient-candidate-v1")],by[(b,"ratio-v1")]) for b in range(1,7)
             if (b,"gradient-candidate-v1") in by and (b,"ratio-v1") in by]
    diffs = [a["goodput_Bps"]-b["goodput_Bps"] for a,b in pairs]
    coverage = [a["coverage"]-b["coverage"]+.01 for a,b in pairs]
    latency = []
    for a,b in pairs:
        x,y = a["observations"]["eligible_ttfb_s"],b["observations"]["eligible_ttfb_s"]
        if min(len(x),len(y)) >= 100:
            latency.append(float(1.1*np.percentile(y,95)-np.percentile(x,95)))
    estimate = summary(diffs)
    cov,lat = lower(coverage),lower(latency) if len(latency)==len(pairs) else None
    result = {"protocol_sha256":file_hash(study/"protocol.json"),"run_record_hashes":hashes,"runs":rows,
              "primary": {"contrast":"Gradient minus Ratio","paired_blocks":len(pairs),"difference_Bps":estimate,
                "exact_randomization_p":randomization(diffs) if len(diffs)==6 else None,
                "coverage_margin_lower":cov,"latency_margin_lower_s":lat,
                "claim_supported":bool(len(pairs)==6 and estimate["mean"]>0 and estimate["p"]<.05 and
                    cov is not None and cov>=0 and lat is not None and lat>=0)},
              "fixed_comparison":"Later phase, data-selected settings; descriptive comparisons against adaptive arms only",
              "planned_runs":28,"recorded_runs":len(hashes),"complete_runs":sum(r["complete"] for r in rows),
              "analysis_incomplete_runs":sum(r["status"]=="analysis_incomplete" for r in rows)}
    (study/"analysis").mkdir(exist_ok=True)
    target=study/"analysis/results.json"
    if target.exists():
        old=read_json(target)
        if old.get("analysis_incomplete_runs",0):
            replace_derived(target,result)
        else:
            require(old==result,"completed reanalysis differs; preserve the existing result and diagnose")
    else:
        write_new(target,result)
    return result


def report(study):
    """Readable private LaTeX/PDF from already-audited results; no new analysis."""
    import subprocess
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    study=Path(study)
    result=read_json(study/"analysis/results.json")
    out=study/"analysis"
    labels={"ratio-v1":"Ratio","gradient-candidate-v1":"Gradient","fixed-low":"Fixed-low","fixed-high":"Fixed-high"}
    colors=("#B27E35","#405D8C","#267B71","#8B5268")
    def escape(value):
        mapping={"\\":r"\textbackslash{}","_":r"\_","%":r"\%","&":r"\&","#":r"\#","{":r"\{","}":r"\}"}
        return "".join(mapping.get(c,c) for c in str(value))
    def number(value,scale=1):
        return "unavailable" if value is None else f"{value/scale:.4f}"
    fig,ax=plt.subplots(figsize=(6.4,2.7),layout="constrained")
    main=[r for r in result["runs"] if r["phase"]!="calibration"]
    for (arm,label),color in zip(labels.items(),colors):
        points=[(i+1,r["goodput_Bps"]/1e6) for i,r in enumerate(main) if r["arm"]==arm and r["complete"]]
        if points:ax.plot(*zip(*points),"o-",label=label,color=color,markersize=4)
    ax.axvline(12.5,color="#777777",linestyle="--",linewidth=1)
    ax.set(xlabel="Main run position: adaptive first, Fixed last",ylabel="Verified goodput (MB/s)",xlim=(.5,24.5))
    if ax.get_legend_handles_labels()[0]:ax.legend(frameon=False,ncol=2,fontsize=8)
    ax.grid(alpha=.15);fig.savefig(out/"goodput.pdf");plt.close(fig)
    g=result["primary"]["difference_Bps"];ci=g["ci"]
    lines=[r"\documentclass[11pt]{article}",r"\usepackage[margin=0.8in]{geometry}",
      r"\usepackage[T1]{fontenc}\usepackage{newtxtext,newtxmath,booktabs,longtable,graphicx,xcolor,hyperref}",
      r"\definecolor{navy}{HTML}{18394A}\hypersetup{colorlinks=true,linkcolor=navy,urlcolor=navy}",
      r"\setlength{\parindent}{0pt}\setlength{\parskip}{6pt}",r"\begin{document}",
      r"\title{\textcolor{navy}{FLOW-DC}\\Higher-concurrency real-host study}\author{Private research evidence report}\date{\today}\maketitle",
      r"\section{What was done}",
      "Ratio and Gradient started at four, with minimum two and a safety ceiling of 256, supported by 256 downloader tasks. "
      "Their decision rules and selected options were unchanged. Four adaptive calibration runs preceded six paired adaptive blocks. "
      "Six later Fixed-low/high blocks used settings selected deterministically from adaptive traces. The planned total was 28 runs plus a 300-URL health probe.",
      f"There are {result['recorded_runs']} recorded first attempts and {result['complete_runs']} fully accounted complete runs. "
      "The inventory below retains stopped, incomplete and unrun cells. A completed process or HTTP 200 alone does not establish decoded-image credit.",
      f"Evidence auditing was incomplete for {result['analysis_incomplete_runs']} runs. Those runs receive no complete-run credit or inferred zero goodput.",
      r"\section{Principal comparison}",
      f"Gradient minus Ratio has {g['n']} complete paired blocks. The mean verified-goodput difference is {number(g['mean'],1e6)} decimal MB/s. "
      +("The ordinary 95\\% paired interval is ["+number(ci[0],1e6)+", "+number(ci[1],1e6)+"] MB/s. " if ci else "An interval is unavailable. ")+
      f"The primary two-sided paired-test p value is {number(g['p'])}; planned 64-sign-flip sensitivity p is {number(result['primary']['exact_randomization_p'])}.",
      "The sign-flip sensitivity assumes paired sign exchangeability. It does not enumerate the 20 assignments permitted by exactly balanced method order; it must not be described as that design's exact randomization test.",
      "The predeclared positive claim is "+("supported" if result['primary']['claim_supported'] else "not supported")+". "
      "It additionally requires coverage-loss and p95-delay safeguards; nonsignificance does not establish equivalence.",
      r"\begin{center}\includegraphics[width=\linewidth]{goodput.pdf}\end{center}",
      "Points show complete main runs only. Missing/stopped attempts remain in the inventory. The dashed line separates execution phases; comparisons against later Fixed arms are descriptive and confounded with time.",
      r"\section{Method summaries}\begin{tabular}{lrrrr}\toprule Arm & Complete & Mean MB/s & Coverage & Peak in-flight\\\midrule"]
    for arm,label in labels.items():
        runs=[r for r in main if r['arm']==arm and r['complete']]
        mean=lambda key:float(np.mean([r[key] for r in runs])) if runs else None
        peak=max(r.get('client',{}).get('peak_inflight',0) for r in runs) if runs else "unavailable"
        lines.append(f"{label} & {len(runs)}/6 & {number(mean('goodput_Bps'),1e6)} & {number(mean('coverage'))} & {peak} "+r"\\")
    lines += [r"\bottomrule\end{tabular}",r"\section{Fixed selection}"]
    if (study/"fixed-selection.json").exists():
        selected=read_json(study/"fixed-selection.json")
        lines.append(f"Selected Fixed-low: {selected['fixed-low']}; Fixed-high: {selected['fixed-high']}. "+escape(selected['high_interpretation'])+".")
    else:lines.append("Fixed selection has not been completed. No supported Fixed setting is inferred.")
    if (study/"host-gate.json").exists() and read_json(study/"host-gate.json")["ignore_time"]:
        lines.append("For the moth host only, certificate/CRL validity-period checks were suppressed; cryptographic trust-chain and hostname checks remained enabled. No global unverified TLS context was used.")
    lines += [r"\section{Every planned first attempt}\small\begin{longtable}{llrr}\toprule Arm & Status & Block & MB/s\\\midrule\endhead"]
    for r in result['runs']:
        value=number(r.get('goodput_Bps'),1e6)
        lines.append(f"{labels.get(r['arm'],escape(r['arm']))} & {escape(r['status'])} & {r['block']} & {value} "+r"\\")
    lines += [r"\bottomrule\end{longtable}\normalsize",r"\section{Interpretation and limits}",
      "The endpoint is independently verified decoded original-row bytes divided by launch-through-verification elapsed time. "
      "Original row denominators include known failures. Unknown evidence is not silently assigned successful completion or imputed into complete-run intervals. "
      "Application-observed first-body delay is not packet RTT. Common Retry-After/admission guards remain enabled even in Fixed.",
      "Report actual concurrency and any binding 256 ceiling, client bottleneck, short acquisition, refusal or content failure. "
      "An observed 429 is explicit rate-limit behavior; 503, transport errors and latency changes are separate signals. "
      "The selected high-load setting does not prove a configured server quota. One host/client/time window and distinct sampled URLs limit generality; server caches and background load are uncontrolled.",
      "Raw archives, native journals, hashes, protocol and attempt records remain private. This report is derived from retained results and is separate from the October17 study. "
      "It establishes neither distributed scaling nor independent-human reproduction nor manuscript submission readiness.",r"\end{document}"]
    next_path=out/"report.next.tex"
    write_new(next_path,"\n".join(lines).encode());next_path.replace(out/"report.tex")
    with (out/"latex.log").open("wb") as log:
        for _ in range(2):
            subprocess.run(["pdflatex","-interaction=nonstopmode","-halt-on-error","report.tex"],cwd=out,stdout=log,stderr=log,check=True,timeout=60)
