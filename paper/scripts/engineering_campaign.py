"""Render engineering measurements; do not treat them as efficacy replicates."""

import hashlib
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
ARMS = [
    ("paarc", None, "PAARC"),
    ("paarc-ratio", None, "PAARC-Ratio"),
    ("img2dataset-1.47.0", [1, 32], "img2dataset (1,32)"),
    ("img2dataset-1.47.0", [1, 192], "img2dataset (1,192)"),
    ("img2dataset-1.47.0", [8, 24], "img2dataset (8,24)"),
]
CONDITIONS = [
    ("inaturalist-open", 1, "iNaturalist / one worker"),
    ("inaturalist-open", 2, "iNaturalist / two workers"),
    ("openimages-cvdf", 1, "Open Images / one worker"),
]


def generate():
    path = ROOT / "data/engineering-campaign.json"
    data = json.loads(path.read_text())
    assert data["collected_analysis_verified"] is True
    records = data["records"]
    assert all(r["phase"] == "engineering" for r in records)
    complete = [r for r in records if r["status"] == "complete"]
    for r in complete:
        assert np.isclose(r["goodput_Bps"], r["credited_bytes"] / r["elapsed_s"])
        assert len(r["participants"]) == r["workers"]
    table = []
    fig, axes = plt.subplots(1, 3, figsize=(7.1, 2.9), layout="constrained")
    colors = ["#cc682d", "#2265bd", "#68737b", "#89949a", "#b2b9bd"]
    for ax, (source, workers, title) in zip(axes, CONDITIONS, strict=True):
        for i, (method, setting, label) in enumerate(ARMS):
            selected = [
                r
                for r in complete
                if r["source"] == source
                and r["workers"] == workers
                and r["method"] == method
                and (setting is None or r["tool_setting"] == setting)
            ]
            by_size = {r["rows"]: r for r in selected}
            assert len(by_size) == len(selected), "Do not silently select among repeated complete attempts"
            r = by_size.get(2048)
            if r:
                ax.bar(i, r["http_span_s"], color=colors[i], width=0.65)
                ax.bar(
                    i, r["elapsed_s"] - r["http_span_s"], bottom=r["http_span_s"], color="#e0e4e7", width=0.65
                )
            else:
                ax.text(i, 0, "Missing", rotation=90, va="bottom", ha="center", fontsize=6)
            fields = ["iNat" if source == "inaturalist-open" else "CVDF", str(workers), label]
            fields += [f"{by_size[n]['http_span_s']:.1f}" if n in by_size else "---" for n in (512, 2048)]
            fields += [f"{r['elapsed_s']:.1f}" if r else "---", f"{100 * r['coverage']:.2f}" if r else "---"]
            table.append(" & ".join(fields) + r" \\")
        ax.set_title(title, fontsize=8)
        ax.set_xticks(range(5), ["PAARC", "Ratio", "I2D\n1,32", "I2D\n1,192", "I2D\n8,24"], fontsize=7)
        ax.set_ylim(bottom=0)
        ax.grid(axis="y", alpha=0.18)
        ax.set_axisbelow(True)
    axes[0].set_ylabel("Manager elapsed time (s)")
    fig.text(
        0.5,
        -0.02,
        "2,048-row engineering checks: color = HTTP span; gray = remaining manager interval",
        ha="center",
        fontsize=7,
    )
    dest = ROOT / "figures/engineering-duration.pdf"
    fig.savefig(dest, bbox_inches="tight", metadata={"CreationDate": None, "ModDate": None})
    plt.close(fig)
    (ROOT / "sections/engineering-table.tex").write_text("\n".join(table) + "\n")
    return [
        dict(
            file=str(dest.relative_to(ROOT)),
            evidence_kind="measured-engineering",
            inputs=["data/engineering-campaign.json", "scripts/engineering_campaign.py"],
            claim="Execution and sizing observations, not replicated long-run efficacy",
            sha256=hashlib.sha256(dest.read_bytes()).hexdigest(),
        )
    ]


if __name__ == "__main__":
    generate()
