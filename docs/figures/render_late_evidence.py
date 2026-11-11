#!/usr/bin/env python3
"""Draw an illustrative watch-mode history; run no query or experiment.

K1/K2 denote symbolic sequence tokens. The required observations and
policies remain in one store history. Requires Matplotlib 3.10.7.
"""
import argparse
from pathlib import Path
import os
import tempfile
os.environ.setdefault("MPLCONFIGDIR", str(Path(tempfile.gettempdir()) / "toposhift-figure-mplcache"))
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", type=Path, required=True)
    args = parser.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 13.5,
                         "svg.fonttype": "none", "svg.hashsalt": "toposhift-readme-round2"})
    # A conceptual narrative, not a UI screenshot or measured response.
    fig,ax=plt.subplots(figsize=(11,5.3));fig.subplots_adjust(left=0,right=1,bottom=0,top=1);ax.set_xlim(0,11);ax.set_ylim(0,5.3);ax.axis('off')
    ink='#16324a';muted='#566a79';blue='#2f6fb0';orange='#b46b16';purple='#7b62a3'
    ax.text(.35,4.91,'Same past. New evidence.',fontsize=23,color=ink,weight='medium')
    ax.text(.35,4.49,'Question: what was on node-2 at 03:07?',fontsize=16,color=ink)
    # Shared baseline: cards are explanatory scenes, not product UI.
    for x,w,face,edge in [(.35,3.0,'#edf4fc','#bed0e1'),(3.92,3.15,'#fff6e9','#e7ceaa'),(7.65,3.0,'#f1edf8','#cfc3df')]:
     ax.add_patch(FancyBboxPatch((x,1.35),w,2.62,boxstyle='round,pad=.02,rounding_size=.12',linewidth=1.0,edgecolor=edge,facecolor=face))
    ax.text(.58,3.62,'First answer',fontsize=16,color=blue,weight='medium')
    ax.text(.58,3.26,'Knowledge token K₁',fontsize=12.5,color=muted)
    ax.text(.58,2.73,'Observed at 03:00:',fontsize=12.5,color=ink)
    ax.text(.58,2.35,'pod-a → node-2',fontsize=16,color=ink)
    ax.text(.58,1.73,'pod-a is included',fontsize=13.5,color=blue,weight='medium')
    ax.text(4.16,3.62,'Late evidence arrives',fontsize=15,color=orange,weight='medium')
    ax.text(4.16,3.12,'pod-a was deleted',fontsize=14,color=ink)
    ax.text(4.16,2.70,'Reported time: 03:05',fontsize=13,color=ink)
    ax.text(4.16,2.14,'Received after the first query',fontsize=11.5,color=muted)
    ax.text(4.16,1.70,'Placed back in event time',fontsize=12,color=orange)
    ax.text(7.89,3.62,'Latest answer',fontsize=16,color=purple,weight='medium')
    ax.text(7.89,3.26,'Knowledge token K₂',fontsize=12.5,color=muted)
    ax.text(7.89,2.73,'Deletion now visible:',fontsize=12.5,color=ink)
    ax.text(7.89,2.35,'pod-a ended at 03:05',fontsize=14,color=ink)
    ax.text(7.89,1.73,'pod-a is excluded',fontsize=13.5,color=purple,weight='medium')
    for left,right in [(3.42,3.84),(7.15,7.57)]:
     ax.add_patch(FancyArrowPatch((left,2.65),(right,2.65),arrowstyle='-|>',mutation_scale=15,color='#748593',linewidth=1.4))
    ax.text(.35,.82,'Repeat the question with K₁: the first answer is preserved.',fontsize=14,color=ink)
    ax.text(.35,.36,'Illustrative watch-mode observations · same store history and policy · required records retained',fontsize=10.8,color=muted)
    for suffix in ['png','svg']:
     fig.savefig(args.out_dir/f'late-evidence.{suffix}',dpi=150,facecolor='white',metadata={'Date':None} if suffix=='svg' else None)
    plt.close(fig)


if __name__ == "__main__":
    main()
