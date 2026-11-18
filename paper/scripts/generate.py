#!/usr/bin/env python3
"""Rebuild every numerical figure and diagram without network or private paths."""

import hashlib
import json
import math
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch
from scipy import stats

ROOT = Path(__file__).resolve().parents[1]
DATA, FIG = ROOT / "data", ROOT / "figures"
METHODS = ["img2dataset", "PAARC", "PAARC-Ratio"]
COLORS = {"PAARC-Ratio": "#2265bd", "PAARC": "#cc682d", "img2dataset": "#68737b"}
plt.rcParams.update(
    {
        "font.family": "DejaVu Sans",
        "font.size": 9,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "pdf.fonttype": 42,
        "ps.fonttype": 42,
        "savefig.bbox": "tight",
    }
)
MANIFEST = []


def save(fig, name, kind, inputs, claim):
    path = FIG / f"{name}.pdf"
    fig.savefig(path, metadata={"CreationDate": None, "ModDate": None})
    plt.close(fig)
    MANIFEST.append(
        dict(
            file=str(path.relative_to(ROOT)),
            evidence_kind=kind,
            inputs=inputs,
            claim=claim,
            sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        )
    )


def box(ax, xy, size, text, color="#222222", fill="white", fontsize=9):
    x, y = xy
    w, h = size
    ax.add_patch(
        FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.012,rounding_size=.025", ec=color, fc=fill, lw=1.3)
    )
    ax.text(x + w / 2, y + h / 2, text, ha="center", va="center", fontsize=fontsize, color="#172b3a")


def arrow(ax, a, b, text=None, dashed=False, rad=0):
    ax.add_patch(
        FancyArrowPatch(
            a,
            b,
            arrowstyle="-|>",
            mutation_scale=12,
            lw=1.25,
            color="#3e4c59",
            linestyle="--" if dashed else "-",
            connectionstyle=f"arc3,rad={rad}",
        )
    )
    if text:
        ax.text((a[0] + b[0]) / 2, (a[1] + b[1]) / 2 + 0.025, text, ha="center", fontsize=7.5)


def canvas(size=(7.1, 2.5)):
    fig, ax = plt.subplots(figsize=size)
    ax.set_xlim(-0.025, 1.025)
    ax.set_ylim(0, 1)
    ax.axis("off")
    return fig, ax


def diagrams():
    fig, ax = canvas((7.1, 2.1))
    for x, name in [
        (0.015, "STARTUP\nDiscover bandwidth"),
        (0.265, "DRAIN\nReduce inflight"),
        (0.515, "PROBE BW\nProbe and cruise"),
        (0.765, "PROBE RTT\nRefresh baseline"),
    ]:
        box(ax, (x, 0.38), (0.21, 0.32), name, "#2265bd", fontsize=8)
    for a, b in [(0.225, 0.265), (0.475, 0.515), (0.725, 0.765)]:
        arrow(ax, (a, 0.54), (b, 0.54))
    arrow(ax, (0.86, 0.36), (0.62, 0.36), rad=-0.15)
    ax.text(
        0.5,
        0.12,
        "Transport-layer model: bandwidth × propagation delay → inflight data",
        ha="center",
        fontsize=9,
    )
    save(
        fig,
        "bbr-phases",
        "conceptual",
        ["scripts/generate.py"],
        "Original BBR phases; not PAARC state equivalence",
    )

    fig, ax = canvas((3.45, 1.9))
    box(ax, (0.26, 0.70), (0.48, 0.23), "TaskVine manager", "#2265bd", fontsize=8.5)
    for x in [0.02, 0.35, 0.68]:
        box(ax, (x, 0.12), (0.29, 0.24), "Worker" if x != 0.68 else "Worker N", fontsize=8.5)
        arrow(ax, (0.5, 0.68), (x + 0.145, 0.38))
    ax.text(0.5, 0.025, "Declared inputs / outputs", ha="center", fontsize=8)
    save(fig, "taskvine", "conceptual", ["scripts/generate.py"], "Manager-worker topology")

    fig, ax = canvas((7.1, 2.5))
    for x, s in [(0.12, "INPUT"), (0.44, "COORDINATION"), (0.79, "EXECUTION")]:
        ax.text(x, 0.97, s, ha="center", va="top", fontsize=10, color="#67717a", weight="bold")
    box(ax, (0.01, 0.56), (0.23, 0.22), "Dataset manifest\nURLs + metadata")
    box(ax, (0.32, 0.56), (0.25, 0.22), "Manager\nTaskVine scheduler", "#2265bd")
    box(ax, (0.34, 0.19), (0.21, 0.20), "Partition manifests")
    box(ax, (0.70, 0.69), (0.28, 0.17), "Public remote hosts")
    box(ax, (0.69, 0.37), (0.29, 0.22), "Workers\nPAARC admission", fontsize=8.5)
    box(ax, (0.71, 0.05), (0.25, 0.17), "Verified output archives", fontsize=8.5)
    arrow(ax, (0.25, 0.67), (0.31, 0.67))
    arrow(ax, (0.445, 0.54), (0.445, 0.41))
    arrow(ax, (0.56, 0.29), (0.68, 0.43), "Tasks")
    arrow(ax, (0.83, 0.60), (0.83, 0.67))
    ax.text(0.86, 0.635, "HTTP", fontsize=7.5)
    arrow(ax, (0.83, 0.35), (0.83, 0.24))
    arrow(ax, (0.69, 0.54), (0.58, 0.64), "Summary", True)
    save(
        fig, "architecture", "conceptual", ["scripts/generate.py"], "Current FLOW-DC acquisition architecture"
    )

    fig, ax = canvas((3.45, 2.9))
    labels = [
        "Validate original manifest rows",
        "Per-host admission + delay feedback",
        "HTTP transfer → atomic write",
        "Row ledger + output finalization",
        "Archive verification + summary",
    ]
    for i, label in enumerate(labels):
        y = 0.82 - i * 0.18
        box(ax, (0.05, y), (0.90, 0.12), label, "#2265bd" if i == 1 else "#333333", fontsize=8.5)
        if i < 4:
            arrow(ax, (0.5, y - 0.006), (0.5, y - 0.055))
    save(fig, "worker", "conceptual", ["scripts/generate.py"], "Worker pipeline and integrity boundary")

    fig, ax = canvas((7.1, 2.35))
    box(ax, (0.015, 0.61), (0.18, 0.23), "Initialize\nEligible samples", fontsize=8.5)
    box(ax, (0.27, 0.61), (0.19, 0.23), "Explore\nUtilized startup", "#2265bd")
    box(ax, (0.55, 0.61), (0.19, 0.23), "Regulate\nDelay budget + rate", "#2265bd")
    box(ax, (0.77, 0.14), (0.21, 0.23), "Probe baseline\nDrain, then refresh")
    box(ax, (0.30, 0.14), (0.24, 0.23), "Recover / hold\nPriority safeguards")
    arrow(ax, (0.20, 0.72), (0.26, 0.72))
    arrow(ax, (0.47, 0.72), (0.54, 0.72), "Budget", rad=0)
    arrow(ax, (0.74, 0.69), (0.87, 0.39), "Age", rad=-0.15)
    arrow(ax, (0.77, 0.18), (0.46, 0.59), rad=0.20)
    arrow(ax, (0.36, 0.60), (0.39, 0.39))
    arrow(ax, (0.63, 0.60), (0.50, 0.39))
    arrow(ax, (0.29, 0.25), (0.27, 0.60), rad=-0.3)
    ax.text(
        0.5,
        0.025,
        "Overload and stale evidence take precedence over positive growth.",
        ha="center",
        fontsize=8,
    )
    save(
        fig,
        "paarc-phases",
        "conceptual",
        ["scripts/generate.py"],
        "Logical PAARC phases; implementation uses state variables and priority branches",
    )


def plots(m, author):
    fig, ax = plt.subplots(figsize=(7.1, 2.7), layout="constrained")
    supplied = {r["concurrency_limit"]: r for r in author["conditions"]}
    groups = [
        ("img2dataset\n96", "img2dataset", supplied[96]["goodput_MBps"]),
        (
            "img2dataset\n192",
            "img2dataset",
            [r["goodput_Bps"] / 1e6 for r in m["cvdf"] if r["label"] == "img2dataset"],
        ),
        ("img2dataset\n384", "img2dataset", None),
        (
            "PAARC-Ratio\n192 cap",
            "PAARC-Ratio",
            [r["goodput_Bps"] / 1e6 for r in m["cvdf"] if r["label"] == "PAARC-Ratio"],
        ),
        (
            "PAARC\n192 cap",
            "PAARC",
            [r["goodput_Bps"] / 1e6 for r in m["cvdf"] if r["label"] == "PAARC"],
        ),
    ]
    for j, (_, method, values) in enumerate(groups):
        if values is None:
            continue
        vals = np.array(values)
        ax.bar(j, vals.mean(), color=COLORS[method], width=0.65)
        ax.plot(j + np.linspace(-0.12, 0.12, len(vals)), vals, "o", ms=3.5, color="#151515")
        ax.text(j, max(vals) + 0.28, f"{vals.mean():.2f}", ha="center", va="bottom", fontsize=9)
    ax.text(
        2,
        0.48,
        "N/A\n5/5 stopped",
        transform=ax.get_xaxis_transform(),
        ha="center",
        va="center",
        fontsize=9,
        color="#96382c",
        bbox=dict(boxstyle="round,pad=.35", facecolor="#fff7f4", edgecolor="#bd786b"),
    )
    ax.set(
        xticks=range(len(groups)),
        xticklabels=[g[0] for g in groups],
        ylim=(0, 14.5),
        ylabel="Verified goodput (MB/s)",
    )
    ax.tick_params(axis="x", labelsize=8.5)
    ax.grid(axis="y", alpha=0.15)
    ax.set_axisbelow(True)
    save(
        fig,
        "cvdf-comparison",
        "measured-and-author-reported",
        ["data/measured.json", "data/author-concurrency.json"],
        "Consolidated CVDF configuration display; machine difference disclosed in caption; statistical samples remain separate; stopped is N/A, never zero",
    )

    fig, axes = plt.subplots(1, 2, figsize=(7.1, 2.6), layout="constrained")
    labels = [r["label"] for r in m["taibif_timing"]]
    for i, r in enumerate(m["taibif_timing"]):
        net = r["network_span_s"]
        native = r["native_seconds"]
        post = r["postprocessing_seconds"]
        left = 0
        for val, label, col in [
            (net, "HTTP span", "#2265bd"),
            (native - net, "Other native time", "#b7c8dc"),
            (post, "Postprocessing / verification", "#e0e4e7"),
        ]:
            axes[0].barh(i, val, left=left, color=col, label=label if i == 0 else None)
            left += val
        axes[1].bar(
            i - 0.18,
            r["full_http_interval_MBps"],
            0.34,
            color="#b7c8dc",
            label="HTTP span" if i == 0 else None,
        )
        axes[1].bar(
            i + 0.18,
            r["end_to_end_MBps"],
            0.34,
            color=COLORS[r["label"]],
            label="End to end" if i == 0 else None,
        )
    axes[0].set_yticks(range(2), labels, fontsize=8)
    axes[0].invert_yaxis()
    axes[0].set_xlabel("Elapsed time (s)")
    axes[0].legend(fontsize=7, loc="lower center", bbox_to_anchor=(0.5, 1.01))
    axes[1].set_xticks(range(2), labels, fontsize=8)
    axes[1].set_ylabel("Verified bytes / interval (MB/s)")
    axes[1].legend(fontsize=8)
    axes[1].grid(axis="y", alpha=0.15)
    save(
        fig,
        "taibif-timing",
        "measured",
        ["data/measured.json"],
        "Complete validation block 2; time-bounded acquisition",
    )


def tables(m, author):
    summaries = {}
    lines = []
    for method in METHODS:
        rows = [r for r in m["cvdf"] if r["label"] == method]
        s = {
            k: float(np.mean([r[k] for r in rows]))
            for k in ["goodput_Bps", "elapsed_s", "verification_s", "mean_active"]
        }
        summaries[method] = s
        lines.append(
            f"{method} & 192 & {s['goodput_Bps'] / 1e6:.2f} & {s['elapsed_s']:.2f} & 100 & {s['mean_active']:.2f}"
            + r" \\"
        )
    supplied = {r["concurrency_limit"]: r for r in author["conditions"]}
    author_mean = float(np.mean(supplied[96]["goodput_MBps"]))
    lines.insert(0, f"img2dataset & 96 & {author_mean:.2f} & N/A & N/A & N/A" + r" \\")
    lines.insert(2, r"img2dataset & 384 & N/A & N/A & N/A & $\approx 2^{\ddagger}$ \\")
    (ROOT / "sections/cvdf-table.tex").write_text("\n".join(lines) + "\n")
    contrasts = []
    ref = {int(r["index"]): r for r in m["cvdf"] if r["label"] == METHODS[0]}
    for method in METHODS[1:]:
        vals = np.array(
            [
                math.log(r["goodput_Bps"] / ref[int(r["index"])]["goodput_Bps"])
                for r in m["cvdf"]
                if r["label"] == method
            ]
        )
        half = float(stats.t.ppf(0.975, 3) * stats.sem(vals))
        contrasts.append(
            dict(
                method=method,
                geometric_ratio=float(math.exp(vals.mean())),
                lower=float(math.exp(vals.mean() - half)),
                upper=float(math.exp(vals.mean() + half)),
                n=4,
            )
        )
    (DATA / "summaries.json").write_text(
        json.dumps(
            dict(
                cvdf=summaries,
                cvdf_contrasts=contrasts,
                author_concurrency=dict(
                    c96_mean_MBps=author_mean,
                    c96_n=len(supplied[96]["goodput_MBps"]),
                    c384_stopped=supplied[384]["reported_run_count"],
                    c384_goodput_MBps=None,
                    pooled_with_matched_comparison=False,
                ),
            ),
            indent=2,
        )
        + "\n"
    )


def engineering_status():
    data = json.loads((DATA / "engineering-campaign.json").read_text())
    records = data["records"]
    complete = [r for r in records if r["status"] == "complete"]
    stopped = len(records) - len(complete)
    interval = data["admission_interval"]
    coverage = (
        "All completed jobs achieved 100\\% original-row coverage."
        if all(r["coverage"] == 1 for r in complete)
        else "Completed-job coverage is shown in the engineering table."
    )
    text = (
        f"The retained engineering inventory contains {len(records)} attempts: {len(complete)} completed and {stopped} stopped. "
        f"{coverage} The stopped first attempt and its explicitly identified engineering correction remain separate records. "
        "No long calibration or evaluation result is inferred from these checks. The planned long comparison remained unstarted at campaign closure. "
        f"The engineering admission model required at least {interval['minimum_common_rows']:,} common rows for a 600-s HTTP interval in the fastest one-worker iNaturalist configuration, "
        f"whereas its most restrictive native-processing constraint admitted at most {interval['maximum_common_rows']:,} rows. "
        "Thus no common row count satisfied the frozen admission rule. These bounds are model estimates from short runs, not measured limits of the public service.\n"
    )
    (ROOT / "sections/engineering-status.tex").write_text(text)
    measured = json.loads((DATA / "measured.json").read_text())
    values = {}
    for method, label in [("img2dataset", "Tool"), ("PAARC-Ratio", "Ratio"), ("PAARC", "PAARC")]:
        rows = [r for r in measured["cvdf"] if r["label"] == method]
        values[f"CVDF{label}Goodput"] = f"{np.mean([r['goodput_Bps'] for r in rows]) / 1e6:.2f}"
        values[f"CVDF{label}Time"] = f"{np.mean([r['elapsed_s'] for r in rows]):.2f}"
    for r in measured["taibif_timing"]:
        label = "Ratio" if r["label"] == "PAARC-Ratio" else "PAARC"
        values[f"TaiBIF{label}Goodput"] = f"{r['end_to_end_MBps']:.2f}"
    (ROOT / "sections/numbers.tex").write_text(
        "".join("\\newcommand{\\" + k + "}{" + v + "}\n" for k, v in values.items())
    )


def main():
    FIG.mkdir(exist_ok=True)
    measured = json.loads((DATA / "measured.json").read_text())
    author = json.loads((DATA / "author-concurrency.json").read_text())
    diagrams()
    plots(measured, author)
    tables(measured, author)
    engineering_status()
    from engineering_campaign import generate

    MANIFEST.extend(generate())
    (FIG / "manifest.json").write_text(json.dumps(MANIFEST, indent=2) + "\n")
    print(f"Rebuilt {len(MANIFEST)} figures; no network or acquisition calls.")


if __name__ == "__main__":
    main()
