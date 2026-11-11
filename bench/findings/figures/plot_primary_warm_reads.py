#!/usr/bin/env python3
"""Render a historical CI figure; read existing judged output, never run a benchmark.

Requires Matplotlib 3.10.7. Supply the retained readtiming.json explicitly.
The input hash is checked before parsing; source JSON is never copied to output.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics
import tempfile

os.environ.setdefault("MPLCONFIGDIR", str(Path(tempfile.gettempdir()) / "toposhift-figure-mplcache"))
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.ticker import FixedLocator, NullLocator, FuncFormatter

SOURCE_SHA256 = "7cd52971cb96ef255a9355186f2076e3e6512486cf895f20d1a179ba687c2f35"
COMMIT = "e85ba569626ede3e920814b7ba915ba525d68b1b"
RUN = "39769897468"
AGES = ("now", "3h", "9h", "1d")
GROUPS = ("service<- hot-records neighbors", "node<- hot-records neighbors")
CANDIDATES = ("L/k64a2l5m", "M/crdb1", "L/off")
LABELS = ("L with checkpoints", "M · per-edge versioning", "L without checkpoints")
COLORS = ("#2166ac", "#ad5d13", "#5a6e44")
MARKERS = ("D", "s", "^")


def load(path):
    raw = path.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == SOURCE_SHA256, "Input does not match the recorded judge output"
    data = json.loads(raw)
    assert data["Commit"] == COMMIT and data["RunID"] == RUN
    assert data["Window"] == 7 and data["Family"] == "rmae"
    arch, = [a for a in data["Arches"] if a["Arch"] == "aarch64"]
    assert arch["CPUModels"] == ["Neoverse-N2"]
    assert arch["Jobs"] == {c: 5 for c in CANDIDATES}
    cells = [c for c in arch["Cells"] if c["Primary"]]
    assert len(cells) == 8
    assert {(c["Group"], c["Age"]) for c in cells} == {(g, a) for g in GROUPS for a in AGES}
    result = {}
    for cell in cells:
        assert cell["Statistic"] == "largest" and cell["Queries"] == 10
        scope, = cell["Scopes"]
        assert scope["Scope"] == "Neoverse-N2" and not scope["NotReported"]
        values = {c["Name"]: c for c in scope["Candidates"]}
        assert set(values) == set(CANDIDATES)
        for c in values.values():
            jobs = c["Jobs"]
            assert len(jobs) == 5 and sorted(j["Rep"] for j in jobs) == [1, 2, 3, 4, 5]
            assert not c["Partial"] and c["LeftOut"] == 0
            assert all(not j["Partial"] and j["Warm"] == j["Queries"] == 10 for j in jobs)
            samples = [j["Value"]["Ns"] for j in jobs]
            assert (min(samples), statistics.median(samples), max(samples)) == tuple(c[k]["Ns"] for k in ("Min", "Median", "Max"))
        result[(cell["Group"], cell["Age"])] = values
    return data, result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, default=Path(__file__).parent)
    args = parser.parse_args()
    data, cells = load(args.input)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 11,
                         "svg.fonttype": "none", "svg.hashsalt": "toposhift-ci-39769897468"})
    fig, axes = plt.subplots(2, 1, figsize=(10.2, 9.8), sharex=True)
    fig.subplots_adjust(left=0.20, right=0.97, top=0.78, bottom=0.15, hspace=0.45)
    fig.suptitle("Primary warm reverse-neighbor query timings", x=0.055, y=0.975, ha="left", fontsize=17, fontweight="medium")
    fig.text(0.055, 0.935, "CI 39769897468 · seven-day synthetic spike stores · aarch64 / Neoverse-N2", fontsize=11)
    fig.text(0.055, 0.907, "Not serve or MCP · warm cache only · report-only", fontsize=11, fontweight="medium")
    legend = [Line2D([0], [0], color=c, marker=m, linewidth=1.6, markersize=6, label=n)
              for c, m, n in zip(COLORS, MARKERS, LABELS)]
    fig.legend(handles=legend, loc="upper left", bbox_to_anchor=(0.045, 0.88), frameon=False, ncol=3, fontsize=10)
    fig.text(0.055, 0.812, "Five small points = five job values; large marker = median; capped line = min–max.", fontsize=10)
    for ax, group, title in zip(axes, GROUPS, ("Service reverse-neighbor reads", "Node reverse-neighbor reads")):
        for row, age in enumerate(AGES):
            for index, (candidate, color, marker) in enumerate(zip(CANDIDATES, COLORS, MARKERS)):
                c = cells[(group, age)][candidate]
                y = row + (index - 1) * 0.20
                low, mid, high = [c[k]["Ns"] / 1000 for k in ("Min", "Median", "Max")]
                ax.errorbar(mid, y, xerr=[[mid - low], [high - mid]], fmt=marker,
                            color=color, markersize=6, capsize=3.5, linewidth=1.5, zorder=3)
                for job in c["Jobs"]:
                    # Vertical spacing only separates repetitions; it encodes no measure.
                    jitter = (job["Rep"] - 3) * 0.022
                    ax.scatter(job["Value"]["Ns"] / 1000, y + jitter, s=12,
                               facecolors="white", edgecolors=color, linewidths=0.8, zorder=4)
        ax.set_xscale("log")
        ax.set_xlim(35, 45000)
        ax.set_ylim(3.5, -0.5)
        ax.set_yticks(range(4), ("now", "3 hours back", "9 hours back", "1 day back"))
        ax.set_title(title, loc="left", fontsize=12, fontweight="medium", pad=10)
        ax.set_ylabel("Query age", labelpad=14)
        ax.grid(axis="x", color="#d6d6d6", linewidth=0.6)
        ax.set_axisbelow(True)
        ax.xaxis.set_major_locator(FixedLocator([50, 100, 300, 1000, 3000, 10000, 30000]))
        ax.xaxis.set_minor_locator(NullLocator())
        ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x:,.0f}"))
        ax.spines[["top", "right", "left"]].set_visible(False)
        ax.spines["bottom"].set_color("#999999")
        ax.tick_params(axis="y", length=0)
    axes[-1].set_xlabel("Query time (µs) — logarithmic scale", labelpad=12)
    fig.text(0.055, 0.07, "All eight pre-registered primary cells shown. These are job-level statistics, not p99 or confidence intervals.", fontsize=10)
    fig.text(0.055, 0.043, "Secondary exceptions, the control cell and the full measurement scope must accompany this figure; see caption.", fontsize=10)
    stem = args.out_dir / "primary-warm-reverse-reads"
    fig.savefig(stem.with_suffix(".png"), dpi=180, facecolor="white", metadata={"Title": "Historical CI primary warm reverse-neighbor read timings"})
    fig.savefig(stem.with_suffix(".svg"), facecolor="white", metadata={"Date": None})
    fig.savefig(stem.with_suffix(".pdf"), facecolor="white", metadata={"CreationDate": None, "ModDate": None})
    plt.close(fig)
    provenance = {"source_file": "readtiming.json", "source_sha256": SOURCE_SHA256,
                  "commit": COMMIT, "ci_run": RUN, "ci_url": f"https://github.com/lotannauo/toposhift/actions/runs/{RUN}",
                  "rules_version": data["RulesVersion"], "rules_digest": data["RulesDigest"],
                  "source_plan_sha256": "47a9455198362119829b30e4c75f003c513ace26aac60b82a12646a10c2723c1",
                  "stream_digest": "93d02559ff842f1300afbc41c9bf8ea45cce6439c176b0d34dee156c659f9317",
                  "queries_digest": "efffb645a078861c37b972b3ba07cff5e27a147292d86e644467213063fc525d",
                  "arch": "aarch64", "cpu_model": "Neoverse-N2", "primary_cells": 8, "candidates": list(CANDIDATES),
                  "jobs_per_candidate_per_cell": 5, "unit_conversion": "JSON nanoseconds divided by 1000 to microseconds",
                  "statistics": "Existing judge job values, Min, Median and Max only; no new statistic or benchmark run",
                  "matplotlib": matplotlib.__version__, "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                  "outputs": {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in [stem.with_suffix(s) for s in (".png", ".svg", ".pdf")]}}
    (args.out_dir / "provenance.json").write_text(json.dumps(provenance, indent=2) + "\n")
    print("Generated PNG, SVG and PDF from 8 primary cells × 3 candidates × 5 verified historical job values.")


if __name__ == "__main__":
    main()
