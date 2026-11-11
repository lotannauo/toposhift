#!/usr/bin/env python3
"""Render the README layout from the same checked historical CI output.

Requires Matplotlib 3.10.7. This changes presentation, never measurements.
The sibling loader validates the pinned input, cells, jobs and statistics.
"""
import argparse
from pathlib import Path
import plot_primary_warm_reads as source
from matplotlib.lines import Line2D
from matplotlib.ticker import FixedLocator, NullLocator, FuncFormatter
import matplotlib.pyplot as plt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    args = parser.parse_args()
    _, cells = source.load(args.input)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 13.5,
                         "svg.fonttype": "none", "svg.hashsalt": "toposhift-readme-round2"})
    base_height=8.8
    extra_title_space=.24
    height=base_height+extra_title_space
    scale_y=base_height/height
    fig,axes=plt.subplots(2,1,figsize=(10.2,height),sharex=True)
    fig.subplots_adjust(left=.20,right=.975,top=.73*scale_y,bottom=.17*scale_y,hspace=.45)
    fig.suptitle('Primary warm reverse-neighbor query timings',x=.045,y=1-(1-.975)*scale_y,ha='left',fontsize=20)
    ci_label=fig.text(.045,.93*scale_y,'CI 39769897468 · seven-day synthetic spike stores',fontsize=13)
    scope_label=fig.text(.045,.893*scale_y,'aarch64 / Neoverse-N2 · warm cache · not serve or MCP · report-only',fontsize=12.5)
    legend=[Line2D([0],[0],color=c,marker=m,linewidth=1.8,markersize=7,label=n) for c,m,n in zip(source.COLORS,source.MARKERS,source.LABELS)]
    legend_artist=fig.legend(handles=legend,loc='upper left',bbox_to_anchor=(.035,.865*scale_y),frameon=False,ncol=3,fontsize=12.3,columnspacing=1.3,handlelength=1.7)
    key_label=fig.text(.045,.788*scale_y,'Small points: five job values. Large marker: median. Capped line: min–max.',fontsize=12)
    for ax,group,title in zip(axes,source.GROUPS,['Service reverse-neighbor reads','Node reverse-neighbor reads']):
     for row,age in enumerate(source.AGES):
      for index,(candidate,color,marker) in enumerate(zip(source.CANDIDATES,source.COLORS,source.MARKERS)):
       c=cells[(group,age)][candidate];y=row+(index-1)*.20
       low,mid,high=[c[k]['Ns']/1000 for k in ('Min','Median','Max')]
       ax.errorbar(mid,y,xerr=[[mid-low],[high-mid]],fmt=marker,color=color,markersize=7,capsize=4,linewidth=1.6,zorder=3)
       for j in c['Jobs']:
        ax.scatter(j['Value']['Ns']/1000,y+(j['Rep']-3)*.025,s=17,facecolor='white',edgecolor=color,linewidth=.9,zorder=4)
     ax.set_xscale('log');ax.set_xlim(35,45000);ax.set_ylim(3.5,-.5)
     ax.set_yticks(range(4),['now','3 hours back','9 hours back','1 day back'])
     ax.set_title(title,loc='left',fontsize=15,pad=12)
     ax.set_ylabel('Query age',labelpad=16,fontsize=13)
     ax.grid(axis='x',color='#d6dfe5',linewidth=.7);ax.set_axisbelow(True)
     ax.xaxis.set_major_locator(FixedLocator([50,100,300,1000,3000,10000,30000]));ax.xaxis.set_minor_locator(NullLocator());ax.xaxis.set_major_formatter(FuncFormatter(lambda x,_:f'{x:,.0f}'))
     ax.spines[['top','right','left']].set_visible(False);ax.spines['bottom'].set_color('#999999');ax.tick_params(axis='y',length=0,labelsize=13)
     ax.tick_params(axis='x',labelsize=12)
    axes[-1].set_xlabel('Query time (µs) — logarithmic scale',labelpad=12,fontsize=13)
    fig.text(.045,.075*scale_y,'All eight primary cells · job-level summaries, not request p99 or confidence intervals',fontsize=11.3)
    fig.text(.045,.040*scale_y,'Control, secondary outcomes and full measurement scope accompany this figure; see caption.',fontsize=10.7)
    # Check the actual text bounds, including title descenders, before exporting.
    fig.canvas.draw()
    renderer=fig.canvas.get_renderer()
    header_artists=[fig._suptitle,ci_label,scope_label,legend_artist,key_label]
    header_boxes=[artist.get_window_extent(renderer) for artist in header_artists]
    header_gaps=[upper.y0-lower.y1 for upper,lower in zip(header_boxes,header_boxes[1:])]
    assert all(gap>=8 for gap in header_gaps),header_gaps
    for suffix in ['png','svg']:
     fig.savefig(args.out_dir/f'primary-warm-reverse-reads-readme.{suffix}',dpi=150,facecolor='white',metadata={'Date':None} if suffix=='svg' else None)
    plt.close(fig)


if __name__ == "__main__":
    main()
