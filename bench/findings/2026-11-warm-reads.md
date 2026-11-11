# Primary warm reverse-neighbor reads

This compares benchmark implementations of a per-entity history log (L) and per-edge versioning (M). Each reverse-neighbor query asks which entities pointed at a node or service at the requested instant. The cache already held the data needed for these reads.

![Historical CI query timings for all eight registered primary aarch64 cells. All three candidates show five job values and a median with min–max range; the horizontal axis is logarithmic microseconds. This is a seven-day synthetic spike experiment, not serve or MCP.](figures/primary-warm-reverse-reads.svg)

**Figure caption.** CI warm query timings for all eight pre-registered primary reverse-neighbor cells on a full seven-day synthetic store, aarch64 Neoverse-N2. L with checkpoints is `L/k64a2l5m`; M per-edge versioning is `M/crdb1`; L without checkpoints is `L/off`. Small points show the five existing job values, large markers their median, and capped lines their min–max. Points can overlap where values are close. The horizontal axis is logarithmic and measured in microseconds. No ratio, confidence interval, speedup headline or adoption verdict is inferred from the plot.

Per job and query, the source statistic is the median of 31 timed asks after three warm-ups. Each primary cell's job value is the largest of those per-query medians over its hottest-ten group. The plotted range is variation between five jobs, **not a p99 or an uncertainty interval over all individual requests**. Query age is the requested instant's lookback in the retained seven-day history.

**Measurement label:** CI timing, [run 39769897468](https://github.com/lotannauo/toposhift/actions/runs/39769897468), [commit e85ba569626ede3e920814b7ba915ba525d68b1b](https://github.com/lotannauo/toposhift/tree/e85ba569626ede3e920814b7ba915ba525d68b1b), aarch64, Neoverse-N2, warm block cache of 256 MiB (zero misses in the timed asks), five jobs per candidate, three warm-up and 31 timed asks per query, one reader. Times include the engine's counter reporting with the recorder muted; answer hashing is excluded; GOGC is default and collections are counted. Spike engine, full seven-day store, not canonical; synthetic `rmae` stream, preset `ci`, seed `1`; G1's pinned queries; report-only.

**Not serve:** these are the spike engine's layouts L and M through the benchmark engine contract. They are not measurements of `pebblestore` under serve's boot key or of the MCP path. The stores use `sync=false`, without `-canonical-layout`; table boundaries vary between jobs and are part of the spread. No Go memory limit is set. Only warm-cache questions are shown. There is no comparison with P2b, cold reads, a thirty-day timed store or native Postgres.

## Values shown

For accessibility and checking, each cell below gives **median [minimum–maximum] in µs**, rounded to three significant figures. Every cell contains five jobs per candidate; the plot uses the source nanosecond values converted to µs.

| Primary cell and age | L with checkpoints | M per-edge versioning | L without checkpoints |
|---|---:|---:|---:|
| service ← · now | 58.1 [57.3–58.5] | 16,400 [16,400–16,400] | 30,900 [30,100–32,500] |
| service ← · 3h | 51.2 [50.5–51.5] | 17,300 [17,200–17,400] | 30,500 [30,000–33,000] |
| service ← · 9h | 50.1 [49.6–50.6] | 18,900 [18,800–19,000] | 29,700 [29,200–31,600] |
| service ← · 1d | 51.3 [50.9–52.0] | 22,300 [22,100–22,400] | 27,700 [27,100–29,500] |
| node ← · now | 132 [129–139] | 1,470 [1,470–1,480] | 6,810 [6,720–6,880] |
| node ← · 3h | 143 [140–148] | 1,660 [1,650–1,670] | 6,690 [6,630–6,750] |
| node ← · 9h | 130 [129–133] | 1,780 [1,750–1,780] | 6,450 [6,400–6,500] |
| node ← · 1d | 127 [124–132] | 2,010 [1,990–2,010] | 5,920 [5,870–5,950] |

## Control and secondary outcomes

The registered control, node-reverse median neighbors at now, is not one of the eight primary cells. On the same CI aarch64 scope its median [min–max] values in µs were: `L/k64a2l5m` 10.9 [10.8–11.1], `M/crdb1` 89.7 [89.7–90.2], `L/off` 488 [487–490]. It does not enter the primary noise statistic.

The primary selection does not imply a universal layout result. In 49 secondary aarch64 cells, all five checkpointed-L job values lay above all five M values: 17 host-reverse, 17 node-forward L1, 13 service-reverse L3 and two dead-node existence cells. One secondary service-reverse hot-records window cell also had all five checkpointed-L values above L/off. The recorded reading explains these outcomes with the corresponding lower decided raw work; they are not a general claim that M reads faster. They must remain beside the primary results when this figure is published.

## Source and reproduction

The figure reads the retained judged output `readtiming.json`, SHA-256 `7cd52971cb96ef255a9355186f2076e3e6512486cf895f20d1a179ba687c2f35`. The rendering script refuses a different hash, commit, run, architecture, primary set, job count, partial/warm state or inconsistent min/median/max. It makes no network request and runs no engine query. This hash check establishes retained-file identity; it is not independent replication of the experiment.

[Back to the findings synthesis](2026-11-update.md).

The source rules are version `1`, digest `e76692c37dda16ce4a1127ae85604120c399e7808f952ab9bcd8779eabfbdcec`. The plan SHA-256 is `47a9455198362119829b30e4c75f003c513ace26aac60b82a12646a10c2723c1`; stream digest `93d02559ff842f1300afbc41c9bf8ea45cce6439c176b0d34dee156c659f9317`; query digest `efffb645a078861c37b972b3ba07cff5e27a147292d86e644467213063fc525d`.

Render existing evidence with Python and Matplotlib 3.10.7:

```sh
python bench/findings/figures/plot_primary_warm_reads.py --input /path/to/readtiming.json --out-dir /tmp/toposhift-warm-read-figure
```

The README uses a display variant with the same checked input, cells and job values, separate service/node panels, and larger labels. To reproduce that presentation:

```sh
python bench/findings/figures/plot_readme_warm_reads.py --input /path/to/readtiming.json --out-dir /tmp/toposhift-readme-warm-reads
```

The historical CI run is public. Its original downloaded artifacts and the retained judge output are not bundled with this figure, so the script is a rendering recipe for a holder of that exact output, not a claim of complete public reproduction. The rendering script copies no raw JSON and can export PNG, SVG and PDF with source-identity metadata. Only the SVG is included here.
