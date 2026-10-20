# toposhift: what was measured, and what was not

This report sets out what was measured and compared while building toposhift's store, and what was not. It is not an official benchmark publication, and it makes no claim that toposhift is ready for release.

## How to read this report

Every measured figure carries one of two labels, as `bench/findings/README.md` requires:

- `CI run <id>`: a number from a GitHub-hosted run, read from the judge's printed output. Every timing and memory figure in this report carries this label; no rate is quoted.
- `local, deterministic`: a count made on a laptop that is a pure function of the commit, the input hashes and the flags, quoted only after two runs gave the same digest. It is given with the commit, the input sha256 and the command that reproduces it.

Where the labels are given:
- §2 and §5 (each of its two parts) carry a label block, and §6 and §8 open with one, that applies to every figure in them.
- §3 gives the label of each block of figures (one CI run each) in the block's heading; the figures in §3's explanation of G2 and in its "What this shows" carry their run id inline. §1, the Summary and §10 give a label, or a pointer to a section's label, beside each figure.
- §4 and the Terms quote no measured figure: the limits of Q1 to Q5 (250 ms, 50 ms, 2 h, 3000 records/s, 512 MB) are thresholds, not results.
- §7 describes correctness checks. The outcomes of the read-work run carry §2's label; the G0 checks run beside the CI timings come from local counters runs that carry neither label, so their outcomes are not quoted; the test suites are described, not measured.
- §9 describes one run observed once, by its configuration and the checks that passed. It is not a benchmark and quotes no measured figure: its version numbers (k3s v1.36.4, chart 0.38.6) and durations (20 m, 6 h) are settings, not results.
- Hashes, commits, dates, thresholds and settings are not figures and carry no label.

Raw outputs are not committed, except the evaluation's results file in `bench/eval/results`. The figures are quoted in the text.

## Terms

- **Work** is a count of what a read did: seeks, internal steps and decoded entries. It is not a time. No read in this report was timed, so no read is said to take more or less time than another.
- **Read budget**: an absolute limit on a read's work, per entity read and per item answered. A **ratio** is a cell's work divided by its budget, so 0.47 means 47 % of the budget and 1,705.04 means 1,705.04 times it.
- **"N times fewer"** means that the other side holds N times as many (bytes or records).
- **Layout L** is toposhift's: a log per entity, laid out for time locality, with derived checkpoints. **Layout M** is one MVCC key per edge, in the CockroachDB key format.
- **Prefix:** the range of keys that holds one subject's records, an entity's or one end of an edge's; each edge record is stored under two keys. A **pinned prefix** is one the measurement plan chose to read (the busiest, by records or by extensions, and the median prefixes of each population, chosen from the 2-day window).
- **Projected store:** a store into which only the records touching a pinned prefix are written, instead of the whole workload. For layout L, a pinned prefix is expected to hold the same records as on a full store; for layout M, a scan may cross other subjects' keys that a projection omits, so its counts may understate a full store's. Neither was verified.
- **Cell, decided, projected value.** A cell is one population (for example `service<-`, the reads of what points at a service), one statistic, one query and age, and one counter. The service-reverse and node-reverse populations have 10 cells per counter: the largest over their busiest ("hot-records") pinned prefixes and the median over their median ones, each for five queries (a batch read now, and neighbour reads now, 3 h, 9 h and 1 d back). The other four populations have 12 or 15, adding prefixes busiest by extensions ("hot-extensions"), and for node existence a read of a dead node. Cells on seeks and on steps plus decoded entries are **decided** (they can pass or fail); cells on block counters of a projected store are reported and **not decided**. A cell's **projected value** is its ratio scaled to a doubled history (§2).
- **Layers** (`internal/catalog`): L0 the physical fabric (racks, switches, ports), kept; L1 host placement (hosts and zones); L2 workload placement (nodes, pods, containers, processes); L3 logical dependencies between services. "Service-reverse from layer 3" (`service<-L3`) is the service-reverse population at L3; `service<-` without a suffix is the one at L2.
- **Gates of the spike's rules (version 5):** **G0**, every answer equals the reference engine's (counted as wrong, wrong before compaction, unstable); **G1**, read work fits its budget at 30 days and at a doubled history; **G2**, the retention plus the slowness of the first 100 batches after it; **G3**, the 99th-percentile batch commit; **G4**, table bytes per record after a full compaction.
- **Limits of the shipped store's retention (Q1-Q5):** **Q1**, the longest batch inside a retention window takes at most 250 ms; **Q2**, the p99 batch commit inside windows is at most the larger of twice the same outside windows and 50 ms; **Q3**, with 30 days kept, the longest rewrite takes at most 2 h at a load of at least 3000 records/s; **Q4**, the batches inside windows sustain at least 3000 records/s; **Q5**, the peak live heap is at most that of the same store without checkpoints plus 512 MB.
- **Judge:** a program that reads a run's outputs and prints its verdicts under a fixed rule: `spikebench timing` and `spikebench g1` for the spike's gates, `rocksbench judge` for Rule R, and the evaluation's scorer for §6.
- **Spike engine and shipped store.** The spike engine is the benchmark code in `bench/spike` that built each layout on Pebble to choose one (and, for Rule R, on RocksDB). The shipped store is `internal/store/pebblestore`, which `toposhift serve` uses; it retains in the background.
- **Rule R:** the registered comparison of layout L on RocksDB with layout L on Pebble (§8).
- **Candidate names.** `L/k64a2l5m` is layout L with checkpoints due after 64 records (k64), scaled by alpha 2 (a2), at an instant 5 minutes behind the newest record (l5m, the shipped default); `l1ns` is a lag of 1 ns; `L/off` is layout L with no checkpoints; `M/crdb1` is layout M on CockroachDB's columnar key schema. `RL` and `RL4` are layout L on RocksDB (§8).
- **Registered:** written, with its thresholds and void conditions, in a dated registration log before any figure existed. The log is kept outside the repository; this report quotes what it needs from it.
- **Censored hour:** one hour cut from a longer trace, so objects alive at its start or end have their intervals cut there.
- **Heartbeat, TTL, extension:** a producer re-asserts each live object at every sampling interval (its heartbeat); each record promises the object for a TTL, three intervals by default; the coalescer extends a run with each sighting and starts a new run once it is half its TTL old.
- **Sync off:** commits do not sync the write-ahead log to disk.
- **Boot key:** the attribute the shipped store's default lifecycle policy uses to tell a host's boots apart (`topo.host.boot.id`). While a store has one, an entity's existence is folded from its whole prefix and the store places no checkpoints on entity prefixes (pods, nodes and every other entity), so checkpoints sit only on edge prefixes.
- **Oracle Cloud VM:** a development machine whose terms forbid publishing benchmarks; nothing from it is quoted.

## What this does not show

- **Production behaviour.** The storage figures come from one hour (the first, MSMetricsUpdate files 0-1) of one production trace, placement only. Every other figure comes from synthetic or simulated workloads: the generator's synthetic families (one preset and one seed for read work), a kwok-simulated cluster, recorded fault scenarios (ITBench-Lite), or a demo application driven by a scripted loader.
- **Read times.** Read claims are counts of work on projected stores. No read was timed.
- **The shipped store's timing.** Every timed figure is from the spike engine that chose the layout (layouts L and M on Pebble and, in §8 only, layout L on RocksDB), with synchronous retention. The shipped store, which retains in the background, has no CI measurement yet.
- **Timings from a laptop.** Laptop figures are deterministic counts only. Timings, rates and memory come only from GitHub-hosted runners.
- **Comparisons that were not made.** Snapshots, Merkle or prolly trees (as in Dolt), RocksDB user-defined timestamps, time-series databases, temporal graph databases and Postgres range types were not measured, and nothing here compares toposhift with any of them.
- **Release readiness.** Nothing here says that a release is ready, or that any default is final.

## Summary

toposhift keeps a Kubernetes topology as a per-entity append-only log (layout L), with derived checkpoints. On a synthetic 30-day workload, per-edge MVCC (layout M, `M/crdb1`) does more read work than its budget allows on reverse reads, at hot and median prefixes. On layer-2 service-reverse reads (`service<-`), its steps and decoded entries reach up to 1,705.04 times the read budget, and on node-reverse reads (`node<-`) up to 60.89 times. Layout L with checkpoints stays within the budget in every decided cell: at most 0.47 of it on service-reverse reads and 0.74 on node-reverse reads, at the shipped checkpoint lag of 5 minutes (the default). Layout L without checkpoints, the registered control, has the same G1 count (internal steps plus decoded entries) as M in every decided cell of one population, `service<-` steps and decoded entries, though the two counts are made up differently; they are equal query by query in all 92 service-reverse queries that G1 judges in each window, and on the other 30, range reads that no G1 cell includes, M/crdb1 counts more (§2). So, as registered before the run, on that population it is the checkpoints, not the key layout, that keep the work within budget. The attribution covers no other population, and rests on projected stores and one synthetic seed. These are counts of work (`local, deterministic`; projected stores; synthetic workload, preset ci, seed 1; commit `fefd99ff79b1dc55f37f05e752b8754e553df0ed`; the full label is in §2). No read was timed.

On CI hardware at 7 days, with synchronous retention on the spike engine, M's G2 (the retention plus the slowness of the 100 batches after it, largest over the repetitions) was 12m58.55s (aarch64) and 17m0.962s (x86_64), against 46.979s and 43.592s for L with checkpoints at a 1 ns lag, a lag that does not ship (`CI run 37859480901`, 3 repetitions). G2 is not the retention time itself: M's retention alone took at most 34.77s and 40.82s, against 46.86s and 43.50s for that L candidate, and M's G2 comes from the batches after the retention (§3). In the 5-repetition run at 7 days (`CI run 37875443885`) the same L candidate's G2 reached 1m8.439s on x86_64 and L without checkpoints 1m3.568s, both above 60 s: synchronous retention fit 60 s for neither L candidate there, so the judge set the budget to 2x the best, 2m7.136s, which both fit (M was not built in that run). At 30 days it fit 60 s for neither on either architecture (`CI run 37877783029`). In the 3-repetition run (`CI run 37859480901`) M stored fewer bytes per record, 31.2, against 37.3 for L without checkpoints and 41.6 for L with them (L's figure includes its checkpoints; why M's keys take fewer bytes was not measured; the judge does not judge M's figure). The same L candidate (checkpoints at a 1 ns lag) is over its own write gate, G3, on aarch64 at 2, 7 and 30 days (§3). M's answers were not checked on CI, so its 7-day G3 and G4 are not judged, and it was never built at 30 days on CI (§3, §10).

On engines (§8, `CI run 38908320193`): with layout L at the shipped lag, RocksDB was worse than Pebble on G2 on aarch64, the deciding architecture (on x86_64, neither), so Pebble stands and the store stays pure Go. The deciding gate was G2 with synchronous retention on the spike engine; the verdict is about the engine under layout L, not about the shipped store's writer, whose timing is not shown here.

On accuracy (§6, `local, deterministic`): the blast-radius verdict is "mixed". The time-aware walk's recall, 0.672, equals that of the union graph, which ignores when edges existed, so time-aware walking added nothing measurable on this dataset.

On production-shaped data, as registered: "On one censored hour of the Alibaba v2022 placement, the coalesced log takes 1.93 times fewer bytes than storing every sample (~2x), for 1.93 times fewer records; change-only sources approach I (up to 2x I's records with deletes)." I is the change-only lower bound: one record per maximal interval. The "up to 2x" is argued, not measured: a source that writes one record when an object appears and another when it is deleted writes up to two records per interval. The sentence is at 60 s samples; at 300 s samples the coalesced log takes 1.63 times fewer bytes for 1.71 times fewer records. Both are bytes of the shipped store. A write-once sorted Parquet file of every sample takes 4.98 times fewer bytes than the coalesced store, though it takes no append or late event without a rewrite, and its read cost was not measured (`local, deterministic`; the full label is in §5). The claim is about 2x fewer bytes than storing every sample in the same store, not about being smaller than every alternative.

## 1. What was compared, and what was not

| A skeptic asks | Compared? | What exists | Where |
|---|---|---|---|
| Why not periodic snapshots? | **No.** Argued in unpublished design notes, not measured. The nearest arm is the every-sample store (S), which is per-sample storage, not snapshots | On one Alibaba hour, the every-sample store takes 1.93 times the coalesced log's bytes (`local, deterministic`) | §5 |
| Why not one MVCC key per edge (CockroachDB key format)? | **Yes.** Layout M (`M/crdb1`) against L: read work (`local, deterministic`) and write and retention (CI, 7 days only) | Read work and write timings | §2, §3 |
| Why not RocksDB user-defined timestamps? | **No.** Rule R compared RocksDB with Pebble as the engine under layout L, and lists user-defined timestamps among what it cannot show | Pebble stands; user-defined timestamps not measured | §8 |
| Why not a Merkle or prolly store (Dolt)? | **No.** Argued, not measured | None | §10 |
| Why not a time-series database? | **No** | None | §10 |
| Why not a temporal property graph (Raphtory, XTDB, SurrealDB)? | **No.** Listed, not run | None | §10 |
| Why not Postgres `tstzrange` with GiST and a recursive CTE? | **No.** Planned as the first baseline after this report, timed on CI only | None | §10 |
| Why not a sorted columnar archive (Parquet)? | **Yes, for bytes only.** A sorted Parquet file written once by DuckDB: "a write-once sorted file: no appends or late events without a rewrite; read cost not measured" | Takes 4.98 times fewer bytes than the coalesced log (60 s cadence) and 4.04 times fewer (300 s) (`local, deterministic`); read cost not measured | §5, §10 |

So the evidence supports a narrower thesis than "beats snapshots, Merkle trees and per-edge versioning". On one synthetic seed and projected stores, per-entity logs with checkpoints keep read work within budget where per-edge MVCC does not, at a measured write and storage cost, and on one population (`service<-` steps and decoded entries) the attribution registered before the run puts that on the checkpoints, not on the per-entity layout alone. On storage, the measured claim is about 2x fewer bytes than storing every sample in the same store, not fewer than every alternative: the write-once Parquet archive takes fewer bytes. The other alternatives in the table were argued, not measured.

## 2. Read work

The read-work gate, G1, asks two things. Does the work of a read (seeks, steps and decoded entries) fit an absolute budget at 30 days of retained history? Does it still fit if that history doubled? For each cell (a population, a statistic, an age and a counter), the **ratio** is the work divided by that cell's read budget at 30 days. The **projected** value scales the ratio by the work's growth, fitted over 2, 7, 14 and 30 days, to a doubled history. A cell fails when its projected value is above 1. Every figure in this section is a multiple of that cell's own read budget; no figure compares one candidate's work with another's.

The budget for steps and decoded entries is 128 per entity read plus 5 per answer item; for seeks it is 8 plus 4 per item. It was set on 2026-10-05 (commit `dbae325d2bc0a9c79a9ea29a414b54a07a0c280a`), before any build at a retained window and before these runs, but after untimed three-day runs had shown counters.

**Label for every figure in this section:** `local, deterministic`; counts of work, no timing; projected stores; synthetic workload (preset ci, seed 1).
- Commit `fefd99ff79b1dc55f37f05e752b8754e553df0ed`.
- `spikebench` sha256 `051a29dd7e7e6d92a31f4531f7a76e7ecce731c80f3af68d725efa98ecb51ca2`, built in `bench` with `CGO_ENABLED=0 go build -trimpath ./cmd/spikebench` at that commit with go1.27.2 for darwin/arm64; the binary's sha256 holds only for that toolchain and platform (§12 says what other platforms can compare).
- `pins.json` sha256 `2fb4d2576a8d4ed7c5f32e2d536990203ac41d664937efe24d28437acc4a9ab4`.
- `plan.json` sha256: R2 `defe022c7952792f365cd151b1c9eac52fbb5ca440fbcf5ea27a0a4ac7adb4bd`, R7 `c99ddb834d902a547775922dd1d8398e670e3e7257b65a9ce899e6caac51dc29`, R14 `5f3dcd1cdfdbb3e32d653d7c3c308f2aaead2e55ccaf1286f692067fa5e70040`, R30 `32d97528d0734b4318633b757b96997460a143a30dae486364e8d97c80ec05b8`.
- Outputs: `g1.txt` sha256 `829e2fd74d8ba154e90d3ceacfad7b4af7bf185b364182ddfeb436651acaf044`; `g1.json` sha256 `529ac10191c1107babb458167386cc1b9e1edc860ead887ac352e81c24a05b57`.
- Reproduce: the commands in §12. The scripts that ran them here (not published) had sha256 `fe327500fea389e1dd337de821ba26fe509732b9bdfb1e7399d55a5fdd3003c2` (`p1-plans.sh`), `0ba45705a1a78fd88b426056504e3ac184548bbdc7e87b80812ef0eb080ebf6e` (`p1-run.sh`) and `2c52d06d6ab7a4f43b20d86ccd64b878f02d7f4bf2ae9d513115f68f507c7bb9` (`p1-build.sh`, which is not in the registered list of hashes, so its hash is for reference only).
- Two runs, each from its own pins and plans (byte-identical between them), gave byte-identical `g1.txt` and `g1.json`.

The candidates, at 2, 7, 14 and 30 days:
- **L/k64a2l5m:** layout L with checkpoints at the shipped lag of 5 minutes (the default).
- **L/k64a2l1ns:** layout L with checkpoints at a 1 ns lag.
- **L/off:** the registered control, layout L without checkpoints. It differs from the checkpointed L candidates only in checkpoints, and separates what the checkpoints do from what the key layout does. Its failure was expected. The registration disclosed it before the run and needs it for the attribution below. It is reported here, not counted against layout L with checkpoints.
- **M/crdb1:** per-edge MVCC, CockroachDB-style keys.

**Verdicts** (rules version 5, digest `ea47515f5ec1`; 142 decided cells per candidate):

| candidate | G1 | decided cells failing |
|---|---|---|
| L/k64a2l5m | passes | 0 of 142 |
| L/k64a2l1ns | passes | 0 of 142 |
| L/off (control, no checkpoints) | fails | 59 of 142 |
| M/crdb1 | fails | 26 of 142 |

**Steps and decoded entries,** service-reverse and node-reverse reads (10 decided cells per population: hot and median prefixes; multiples of each cell's read budget). The largest projected value is over the population's decided cells, and need not be in the cell with the largest ratio:

| candidate | service-reverse: ratio | largest projected | failing | node-reverse: ratio | largest projected | failing |
|---|---|---|---|---|---|---|
| L/k64a2l5m | 0.17-0.47 | 0.47 | 0 of 10 | 0.20-0.74 | 0.74 | 0 of 10 |
| L/k64a2l1ns | 0.18-0.34 | 0.34 | 0 of 10 | 0.30-0.90 | 0.93 | 0 of 10 |
| L/off | 11.81-1,705.04 | 3,391.07 | 10 of 10 | 78.65-292.46 | 584.56 | 10 of 10 |
| M/crdb1 | 11.81-1,705.04 | 3,391.07 | 10 of 10 | 15.89-60.89 | 111.64 | 10 of 10 |

**Seeks,** the same populations:

| candidate | service-reverse: ratio | failing | node-reverse: ratio | failing |
|---|---|---|---|---|
| L/k64a2l5m, L/k64a2l1ns, L/off (identical) | 0.0043-0.0049 | 0 of 10 | 0.0021-0.031 | 0 of 10 |
| M/crdb1 | 0.0043-46.67 | 3 of 10 | 0.0021-2.12 | 3 of 10 |

What this shows:
- **Layout L with checkpoints fits the budget in every decided cell,** at the shipped lag and at 1 ns.
- **On service-reverse steps and decoded entries, L without checkpoints (L/off) has the same G1 count (internal steps plus decoded entries) as M/crdb1 in every decided cell,** equal in value, ratio and projected value. As registered before the run, the service-reverse gap is therefore attributed to checkpoints, not to the key layout. The scope of that attribution: one population (`service<-`), one counter (steps plus decoded entries), projected stores, one synthetic seed.
- The counts are made up differently. L/off's count is one internal step per record plus the baseline entries it decodes. M/crdb1's is internal steps only, about two per version because its values are kept apart, and its user-level steps are 0.50 to 0.55 of them. The equality holds for every cell's statistic, and query by query for all 92 service-reverse queries that G1 judges in each window. The other 30 service-reverse queries are range reads (window 1 h and 1 d) that no G1 cell includes; on each of them M/crdb1's count is higher than L/off's. Counted by user-level steps, M/crdb1 would still be about 6 to 940 times the budget in these cells, against at most 0.47 (L/k64a2l5m) and 0.34 (L/k64a2l1ns) for the checkpointed L candidates.
- The attribution covers service-reverse steps and decoded entries only. On seeks, L/off does the same work as the checkpointed L candidates, and only M/crdb1 fails. On node-reverse steps and decoded entries, L/off (78.65 to 292.46 times the budget) and M/crdb1 (15.89 to 60.89 times) both fail, are not equal, and no attribution is made.
- **Without checkpoints, layout L fails more cells than M does.** L/off's 59 failing cells are all on steps and decoded entries: service-reverse 10, node-reverse 10, and 39 in four populations that M/crdb1 passes (host-reverse 12, node to layer 1 12, service-reverse from layer 3 12, node existence 3). M/crdb1's 26 are all in the service-reverse and node-reverse populations.
- **No answer differed** from the reference engine, and none was unstable, for any candidate at 2, 7, 14 or 30 days, in both runs. The 134 reads at "now" were also checked before compaction, with none wrong.
- Block-counter cells are projected, not decided, and support no claim.

Limits of this section:
- **A reproduction, not new evidence.** These counts reproduce an earlier local run on the same plans, whose outcome was disclosed before this run was registered. This run adds a new binary, the canonical layout and a determinism check. It does not confirm the result independently.
- **The budget is not independent of the candidates.** It is shaped on layout L's checkpoint bound, and it was set before these runs, but after untimed three-day runs had shown counters. M/crdb1's failing cells on steps and decoded entries are 11.81 to 1,705.04 times the budget on service-reverse reads and 15.89 to 60.89 times on node-reverse reads, and its 6 failing seek cells 1.02 to 46.67 times. How M would fare under another budget was not measured. L's margins are thin: L/k64a2l1ns projects to 0.93 on node-reverse reads, a margin of 7 %.
- **One synthetic stream:** preset ci, seed 1. No other seed or real trace was read for G1. The "hot" prefixes are pinned from the 2-day window.
- **Projected stores.** Only the records touching a pinned prefix are written. L's pass assumes that a pinned prefix holds the same records as on a full store; that was not verified. M's counts may understate a full store's, because an M scan may cross other subjects' keys that a projection omits; that was not verified either.
- **Settled stores.** The stores were read after retention had settled and after compaction, not mid-compaction.
- The projection extrapolates a four-point fit over 2 to 30 days, as registered.

§3 gives L/k64a2l5m's write figures at 30 days from `CI run 38908320193`. Each half of that pairing keeps its own label, and this section supplies no timing or memory figure to it.

## 3. Write and retention

Every row carries one description: **spike engine, synchronous retention, Sync off.** The builds settle each retention, do not rest after one, and are not rewritten canonical. The workload is the synthetic family rmae. The first three runs below are judged by the spike's timing judge, rules version 5 (digest `ea47515f5ec1`), and their L candidate with checkpoints is L/k64a2l1ns (lag 1 ns). The last block, `CI run 38908320193`, is at the shipped lag (L/k64a2l5m) and is printed by Rule R's judge.

The spike judge's gates, each figure as it prints it:
- G2: the largest, over the repetitions, of the retention plus the slowness of the first 100 batches after it. It is not the retention time alone. The budget is 60 s on an architecture where at least one layout of the judge's reference set (L/off and L/k64a2l1ns; M did not join it) fits 60 s; where none does, the budget there is 2x the best of them. So in `CI run 37859480901`, on x86_64, L/k64a2l1ns fit 60 s (43.592s), the budget stayed 60 s, and L/off's 1m4.72s and M's 17m0.962s are over it. In `CI run 37875443885`, on x86_64, neither L candidate fit 60 s (best 1m3.568s), so the budget became 2m7.136s, and L/off's 1m3.568s and L/k64a2l1ns's 1m8.439s are within it, though both are above 60 s.
- G3: the median, over the repetitions, of the p99 batch commit, within 2x the best median.
- G4: the median table bytes per record, within 2x the best median.

Rule R's judge prints, for each candidate, the median over the repetitions; per build, its G2 is the largest retention plus the excess of the 100 batches after it. It compares engines and applies none of the spike judge's limits.

**7 days, `CI run 37859480901`** (3 repetitions per architecture, revision `eed10127c9bd62a6a08fea1da21ee5cf1704cc2b`, plan digest a2538a274e74 as the judge prints it, 39,291,769 records):

| candidate | G2 aarch64 / x86_64 | G3 aarch64 / x86_64 | G4 |
|---|---|---|---|
| L/k64a2l1ns | 46.979s / 43.592s | 18.874368ms **over** (limit 14.680064ms) / 16.777216ms | 41.6 |
| L/off | 37.342s / 1m4.72s **over** | 7.340032ms / 10.48576ms | 37.3 |
| M/crdb1 | 12m58.55s **over** / 17m0.962s **over** | 4.02653184s / 3.758096384s | 31.2 |

M/crdb1's answers (G0) are not established in this run, because no counters run read its plan, so its G3 and G4 rows are not judged; its G2 is judged, and is over on both architectures. Its retention alone took 34.77s and 40.82s at the longest (largest of 3), against 46.86s and 43.50s for L/k64a2l1ns and 37.33s and 1m4.71s for L/off, rounded from the judge's diagnostics. Its G2 comes from the batches after the retention: the longest first batch after a retention, median of 3, was 9.17s and 9.24s. No other way of collecting M's old versions (such as RocksDB user-defined timestamps) was measured.

**7 days, `CI run 37875443885`** (5 repetitions, same revision):
- L/k64a2l1ns: G2 40.548s / 1m8.439s (x86_64 within the 2x-best budget of 2m7.136s, above 60 s); G3 18.874368ms **over** / 23.068672ms (within its limit of 23.068672ms); G4 41.6.
- L/off: G2 37.625s / 1m3.568s (x86_64 within the same budget, above 60 s); G3 7.340032ms / 11.534336ms; G4 37.3.
- The judge's finding: at 7 days on x86_64, synchronous retention fits 60 s for no layout of its reference set.
- The same run at 2 days: L/k64a2l1ns's G3 on aarch64 is 20.97152ms, **over** its limit of 12.582912ms.

**30 days, `CI run 37877783029`** (5 repetitions, revision `6debad4eebf3c9218ce12555c92c1610ae9c89e2`, plan digest ccd7ec4bdef4, 154,179,890 records, Go memory limit 11 GiB; no layout fits 60 s here, so the G2 budget is 2x the best: 3m41.702s on aarch64 and 5m18.776s on x86_64):

| candidate | G2 aarch64 / x86_64 | G3 aarch64 / x86_64 | G4 | checkpoint bytes |
|---|---|---|---|---|
| L/k64a2l1ns | 2m2.04s / 3m15.674s (not judged: noise) | 18.874368ms **over** (limit 15.72864ms) / 27.262976ms (not judged: noise) | 43.0 | 7.8 % (limit 20 %) |
| L/off | 1m50.851s / 2m39.388s (not judged: noise) | 7.86432ms / 12.582912ms (not judged: noise) | 38.5 | - |

Notes on the 30-day run:
- The judge's finding, on both architectures: synchronous retention fits 60 s for no layout of its reference set at 30 days. M was not built at 30 days.
- G0 was checked for both candidates from local counters runs read by the judge; those runs carry neither label, so the outcome is not quoted (§7).
- x86_64 pools four or five CPU models across these runs. Where L/off's own values spread by more than 1.5x, the judge prints "not judged".

**30 days at the shipped lag, `CI run 38908320193`** (Rule R's dispatch B, its Pebble arms, built by the spike engine; medians of 5 repetitions as the judge prints them; revision `c43a2922adb3cd1d86560e54820a78882cc0d149`, plan digest ccd7ec4bdef4, 154,179,890 records):

| candidate | G2 aarch64 / x86_64 | G3 aarch64 / x86_64 | G4 | peak RSS aarch64 / x86_64 |
|---|---|---|---|---|
| L/k64a2l5m | 2m15.178s / 3m8.827s | 23.068672ms / 29.360128ms | 43.2 | 9206 MiB / 9292 MiB |
| L/off | 1m32.147s / 2m26.881s | 8.388608ms / 13.631488ms | 38.5 | 3356 MiB / 3412 MiB |

Rule R's judge applies no G2 budget and no G3 gate, so these rows are not judged against the spike's limits. They are the first CI write figures for L at the shipped lag. These G2 figures are medians; the spike judge's G2 above is the largest over the repetitions. The largest single build's G2 for L/k64a2l5m was 2m53.253s (aarch64) and 3m33.194s (x86_64), in the same run. The largest single build's peak RSS for L/k64a2l5m was 11255 MiB (aarch64), in the same run.

What this shows:
- At 7 days, M's G2 (retention plus the slowness after it) was 12m58.55s and 17m0.962s, where L with checkpoints at the non-shipped 1 ns lag had 40.548s to 1m8.439s (`CI run 37859480901`, `CI run 37875443885`). M's retention alone was not longer than L's; the difference is in the batches after it. M stores fewer bytes per record.
- L with checkpoints at a 1 ns lag is over its write gate G3 on aarch64 at 2, 7 and 30 days. At the shipped lag no G3 gate was applied.
- Synchronous retention fit 60 s for neither L candidate at 30 days (`CI run 37877783029`), nor on x86_64 at 7 days in `CI run 37875443885` (in `CI run 37859480901`, L/k64a2l1ns fit at 43.592s). The shipped store retains in the background, which these runs do not measure (§4). M was never built at 30 days, so there is no 30-day comparison with M.

## 4. The shipped store

Every timed figure in §3 is from the spike engine with synchronous retention. The shipped store retains in the background, and its retention is held to five limits, Q1 to Q5 (see Terms). No figure of the shipped store's retention is quoted in this report.

There is no CI evidence for the shipped store's background retention. So, as the registered fallback says, **v0.1.0 is released with retention off by default**, and `--retain on` stays available (see the repository README, "Retention").

A CI measurement of the shipped store is planned after this report. It is reported against Q1 to Q5 but decides nothing by itself; this report will add its result when it exists.

## 5. Storage

**Input.** Alibaba Cluster Trace Program, cluster-trace-microservices-v2022 (https://github.com/alibaba/clusterdata/tree/master/cluster-trace-microservices-v2022), described in S. Luo et al., "The Power of Prediction: Microservice Auto Scaling via Workload Learning", ACM SoCC 2022. Used for research and study under the program's stated terms; no trace data is redistributed here. The slice measured is one hour: the first, MSMetricsUpdate files 0-1. Only aggregate counts, bytes, ratios and hashes are reported: no row, no id, no per-service table. The program's README asks users to tell the maintainers of publications that use the trace; this report has not been announced, and they have not been told. These figures belong to this report only, as a study of the store; they are not product claims.

**Label for every figure in this section:** `local, deterministic`; counts and canonical table bytes; one hour (the first, MSMetricsUpdate files 0-1) of the Alibaba cluster-trace-microservices-v2022.
- Commit `ac76dbfbc407484dc1f500e55ae893f2c6b79acc`.
- The archive files (arm A) were written by DuckDB v1.5.6 (binary sha256 `7d15b2aaf6be05212ada5f99fe79e0b83e63b7ed91e7e422c0b925278e9e0c39`), which is not part of the commit, from `bench/cmd/activitybytes/testdata/archive.sql` (sha256 `530a8f9bec4726938459e5fc0eaf83e6e087aa62e1b7ceafb3d772826356ad72`).
- Inputs: the trace archives the reducer reads (the MSMetricsUpdate and CallGraph tables). The placement measured here comes from the MSMetricsUpdate archives; the reducer also reads the hour's CallGraph archives and writes call tables from them, which no figure in this report uses. Their hashes are listed because they are inputs of the same reduction. All are checked against a `SHA256SUMS` whose sha256 is `6c7fa73b7c92c4fecd5a199b7ba73d41d459ee9e9a02ad6aafdfce6a98f27b6d`. That file is the block below, line for line and in this order, with the paths relative to the directory that holds `MSMetricsUpdate/` and `CallGraph/`:

  ```text
  df1fff9628d9e993440b2826912aa37e71d37cbddf9b9249389b2b71afbad2f9  ./CallGraph/CallGraph_0.tar.gz
  09c1b0939b0537c6705291df13fc269230f5ff0b1e7963e13b3e5200c1b76176  ./CallGraph/CallGraph_1.tar.gz
  8805d61125987489f3252e0ed0ade6c5ddf5ab38ed610558a3c8da1c1719d2de  ./CallGraph/CallGraph_10.tar.gz
  7498d79d4788e22cddfce386e3e19ea925379b1a42cce24a7a763e25e6903136  ./CallGraph/CallGraph_11.tar.gz
  924d41a4eea784c8d8f6443d1d849317dc02631a735dddb17fefb4195ff489c3  ./CallGraph/CallGraph_12.tar.gz
  20474dc2d9973d3e5a9559c267f1c9f587581d999bbb248fea4ab22c796f4e71  ./CallGraph/CallGraph_13.tar.gz
  1450150e3ec9753f5d1f970f8ff1fbd5d2f1f9eddd976090d8a0f1b945b05782  ./CallGraph/CallGraph_14.tar.gz
  73b8c17d78066b82abee4f3c2fb6526b60ee455a312e39b52e806fbfbff8937d  ./CallGraph/CallGraph_15.tar.gz
  325f0ee15310ea33270d092213e75e8918c7231cdb0019ef98e814c4661a9c90  ./CallGraph/CallGraph_16.tar.gz
  c9512ba80f54df526781514368f4a3818c62caa1d5bbe77036327b26b167defc  ./CallGraph/CallGraph_17.tar.gz
  4d59084cc8c708b3ddf038b8f79983ed172d1915ac2798db8c377c8062cb1c70  ./CallGraph/CallGraph_18.tar.gz
  9ed4ed7e43e6c5abfd184d21bcb96dbc267605dff20a8b49d3c2a01eb9affb79  ./CallGraph/CallGraph_19.tar.gz
  aa8e56392d8b74eb8fbccd7fdf6172b45ae081072f2902150c784cf09bd3cc11  ./CallGraph/CallGraph_2.tar.gz
  2c2b3af818090e735e5f455a668eb539d932ca31d8e6e33fb0534a42bb1a22ed  ./CallGraph/CallGraph_3.tar.gz
  3c73aa78d1c6597ab3254c883dcbd2b433af03ca8940387f0173b9f6bb3b4952  ./CallGraph/CallGraph_4.tar.gz
  075ef2c066ac3ff594f94d12570409cbdac8d766c88bbf860963c0cdf69cd47c  ./CallGraph/CallGraph_5.tar.gz
  71ba9dd30fb86ef93c1ec377fd80872c2a6a58f107fb6d4e4352ac2fee61675c  ./CallGraph/CallGraph_6.tar.gz
  b310128192b5bf248943ef56061ad48e3faee5da534d4ed7374034b0c04f31b6  ./CallGraph/CallGraph_7.tar.gz
  558a0927328b6186b897963284c4186617e1c82d6bdd8c6dd20a22162d79247e  ./CallGraph/CallGraph_8.tar.gz
  0f5ec27c5eb22cea830578c50e57c687ebdc769b21bcdbd3e1fa9846355a2d74  ./CallGraph/CallGraph_9.tar.gz
  479e0cda9fdd8270aad077c334994e7933794b7833cddf0a4597706008558613  ./MSMetricsUpdate/MSMetricsUpdate_0.tar.gz
  efa8cffc800d75329cd07f0cdf3782483d24169dc62ec35afe4ece4040b97e2c  ./MSMetricsUpdate/MSMetricsUpdate_1.tar.gz
  ```

- The activity files reduced from them, each with its content digest and file sha256 (the file sha256 can differ between runs of identical content; the content digest does not):

  | stream | content digest | file sha256 |
  |---|---|---|
  | 60 s, every sample (S) | `sha256:03d20ad52f6449bf3f2180c68056c2451161707bcb84dd0ef9fdc090afffcd00` | `559b5c4bd0fb9e1dfe2dbcef7d6cd5a26086539f25bbdb8e996b70ac108d360f` |
  | 60 s, coalesced (C) | `sha256:d2a61b1c5093958a8895da07769f151f0370bec7154b5e92cec739ae60206910` | `ee08d026778b251e153f10fe2e7669937340e1fc9b70432d21176d442e3398bf` |
  | 60 s, maximal intervals (I) | `sha256:939c217577bdcd2ffe46e674fe636e56ffccfad6ba82a01c018d4cd80917a184` | `9bb9498fbef1ab486314740a89280a1a81c848c2d7724f28c8acacf1f1bed16d` |
  | 300 s, every sample (S) | `sha256:71e20a8aa9911f996cd5178a9b61a6c7c5b048cb3a828885c152b75c4481344f` | `398196ca11d0fdfcc4164d9da3921ca4ced5139882ed2aa9b6a2eb5070208577` |
  | 300 s, coalesced (C) | `sha256:90bf6113b7957d2596cf4d9b2a8997bc92a7425cec32a3a0824567820c6eb86e` | `7d7aae572ed5cf2d1fdfb6716c9d277fb43a1ffaa4b109d2af52737136d8f99d` |
  | 300 s, maximal intervals (I) | `sha256:b16f16d2a2cdf221c0b26f891f1145aceb5d5ec20c09765b6877768539fe0332` | `61955d0e75f944426a5d94274c817e30e44549ff21f7063ac855cead91bddd15` |
  | 60 s, coalesced, TTL x15 (C-T15) | `sha256:78d4ea93d7de210392873f1bfcf2c8162bb40e098c662b9b8d25f6a52b40470b` | `892d49159fbe906cd903fd03bbbeb847641b07c1157969add71198723a0cd138` |

- Archive files (arm A), sha256 equal in both copies: 60 s from S `b15e0e23ea4b728789e218a2070f3e6ec27c8f4325128fcbb15c052a0a35fd28`; 300 s from S `a154d6a43b108be6d9f45201ce923d2e99d54a10dd2691f2ea64b1bec352b742`; 60 s from C `122e31dcd465dee9904a1c0a79960345e455402047d72e9ceb21da5d8821d6ff`.
- Reproduce: the commands in §12. The scripts that ran them here (not published) had sha256 `53ab27b3a0b88b6611f3f22dd2d9c6b6154176bd88f4627ca6c86bd3697a1b17` (`p3-build.sh`) and `817dbf51a6b062122a7985e2372da2488127338a1b10a41d8d620255c05b708c` (`p3-run.sh`). The Go binaries were built with go1.27.2 for darwin/arm64.
- Every reduction, every store load and every archive file was made twice, and each pair is equal (content digests; data digests, table bytes, keys, records and checkpoints; the archive files by sha256).

What was stored. Placement only (pods, nodes and pod-on-node edges), sampled every 60 s and every 300 s. The arms:
- **S:** every asserted sample, with no coalescer.
- **C:** the shipped coalescer's records, the stream toposhift stores.
- **I:** one record per maximal interval, a derived change-only lower bound. It is not what any real source stores. A watch source that writes one record when an object appears and another when it is deleted writes up to 2x I's records (argued, not measured).
- **S-off, C-off (store labels S0, C0):** S and C with checkpoints switched off altogether. With the default lifecycle policy's boot key (see Terms), the store places no checkpoints on entity prefixes, such as pods and nodes, anyway, so what is switched off is the checkpoints on edge (pod-on-node) prefixes.
- **C-T15:** C with a TTL of 15 sampling intervals (900 s) in place of the shipped 3 (180 s).
- **A:** a sorted Parquet file written once by DuckDB from the S stream (and, as a secondary arm, from the C stream): "a write-once sorted file: no appends or late events without a rewrite; read cost not measured."

Each store is the shipped store with its default options, Sync off, loaded in batches of 1,000, compacted and rewritten canonical (every table in the bottom level), with no retention. An empty store takes 873 bytes; it is subtracted from no arm.

Asserted samples: 57,827,320 at 60 s and 11,566,626 at 300 s.

| store arm | records | canonical table bytes | bytes per record | bytes per asserted sample |
|---|---:|---:|---:|---:|
| 60 s, S (every sample) | 57,827,320 | 2,021,619,333 | 34.96 | 34.96 |
| 60 s, C (coalesced) | 29,889,037 | 1,049,534,966 | 35.11 | 18.15 |
| 60 s, I (lower bound) | 971,472 | 58,382,736 | 60.10 | 1.01 |
| 60 s, S-off | 57,827,320 | 1,918,484,263 | 33.18 | 33.18 |
| 60 s, C-off | 29,889,037 | 997,934,611 | 33.39 | 17.26 |
| 60 s, C-T15 | 8,682,677 | 331,799,908 | 38.21 | 5.74 |
| 300 s, S (every sample) | 11,566,626 | 430,918,195 | 37.26 | 37.26 |
| 300 s, C (coalesced) | 6,750,557 | 264,054,763 | 39.12 | 22.83 |
| 300 s, I (lower bound) | 967,603 | 58,160,578 | 60.11 | 5.03 |

| archive arm (A) | rows | file bytes | bytes per row | bytes per asserted sample |
|---|---:|---:|---:|---:|
| 60 s, from S | 57,827,320 | 210,580,108 | 3.64 | 3.64 |
| 300 s, from S | 11,566,626 | 65,323,071 | 5.65 | 5.65 |
| 60 s, from C (secondary) | 29,889,037 | 123,687,523 | 4.14 | 2.14 |

These bytes per record are not comparable with CI's G4 of 37-43 bytes per record for layout L at 7 and 30 days (§3; `CI run 37859480901`, `CI run 37875443885`, `CI run 37877783029` and `CI run 38908320193`): another engine (the shipped store, not the spike's), the boot key (no checkpoints on entity prefixes such as pods and nodes), canonical tables (G4 is read after compaction), and other data.

**The storage sentence,** as registered: "On one censored hour of the Alibaba v2022 placement, the coalesced log takes 1.93 times fewer bytes than storing every sample (~2x), for 1.93 times fewer records; change-only sources approach I (up to 2x I's records with deletes)." That is at 60 s samples; at 300 s samples the coalesced log takes 1.63 times fewer bytes for 1.71 times fewer records. Both are bytes of the shipped store. A write-once sorted Parquet file of every sample takes 4.98 times fewer bytes than the coalesced store (see R3 below and §10).

| ratio | what it compares | value |
|---|---|---:|
| R1 (primary) | bytes of S over bytes of C, 60 s | 1.93 |
| R1' | the same, 300 s (for 1.71 times fewer records) | 1.63 |
| R1off | the same with checkpoints off, 60 s | 1.92 |
| R2 | bytes of C over bytes of I, 60 s / 300 s | 17.98 / 4.54 |
| R3 | bytes of the sorted file of S over bytes of C, 60 s / 300 s | 0.20 / 0.25 |
| R4 | bytes of C-T15 over bytes of C, 60 s | 0.32 |

What the ratios say, as registered:
- **R1** is inside the band registered before the run (1.7 to 2.3), so "~2x" may be said.
- **The ~2x comes from the product's settings as much as from the data.** With a TTL of three intervals and extension at half the TTL, a new run starts at every second refresh, so a steady heartbeat's record ratio cannot exceed about 2; the observed record ratio, 1.935, is 97 % of that ceiling. What this measures is that bytes follow records: 1.926 times fewer bytes for 1.935 times fewer records.
- **R1off** differs from R1 by under the registered 15 % of R1, so nothing is attributed to checkpoints.
- **R2** is the cost of heartbeat semantics over the change-only lower bound on one censored hour: the coalesced log holds 17.98 times the bytes of I at 60 s and 4.54 times at 300 s. The gap is a chosen trade: runs are bounded at half their TTL, so that a stopped object ends on time and checkpoints survive. R2 is for a one-hour window. It is expected to grow with longer windows, because I adds one record per new maximal interval, while C keeps adding records for as long as an object lives; that is argued, not measured.
- **R3 < 1 at both cadences.** A write-once sorted file takes 4.98 times fewer bytes than the coalesced log at 60 s and 4.04 times fewer at 300 s. It takes no append or late event without a rewrite, and no read cost was measured for it. This is reported, not hidden, and not counted against the read claim. The file keeps every column of the records, but not what the store adds for appends and point-in-time reads: the second key of each edge record, the checkpoints and the key structure reads use. It is sorted in one total order (layer, subject kind, source, target, relation, producer, event time, sequence) and zstd-compressed, which favours its size; no other order or codec was tried.
- **R4, the TTL trade:** at 60 s, a 900 s TTL in place of 180 s keeps 0.32 of the bytes for 3.44 times fewer records. Ends come up to 450 s early (up to 90 s at a 180 s TTL), and a vanished object stays asserted up to 900 s after its last sample instead of 180 s.
- Bounded point-in-time reads rest on §2 (synthetic), not on this section.
- The hour is censored at both ends, so these are not lifetimes. §10 item 12 lists what else this does not show.

### Record count on a cluster-shaped capture

**Label:** `local, deterministic`; counts.
- The sequencer and store code at commit `c2dcd6c713651f9d43e1d4b34cbcee46313b2d11`; a counting tool kept outside the repository, built against that commit with go1.27.2 for darwin/arm64 (binary sha256 `c363513f56b2983f50a1545e67734d09e80b3bfd0343a6c53fb4d86a1781aba1`).
- Input: a capture of a kwok cluster (simulated nodes, no real kubelets), sha256 `3afe7027285da051f5b9cb74bdbbf60fd167626826ffca66cc910ad6a217c718`; the verdict's reference counts, sha256 `26e471c8234e488c652812d4c7f48ba7fcc979098368603b65d8518dc012e93f`.
- Two runs gave the same verdict file (sha256 `426b1ebe60779cdfb405b97e7a7f0688def937021b68ab2e39932a7b4e22186f`) and the same counts.
- Command: in §12. The counting tool and the capture are kept outside the repository, so a stranger cannot re-run this count today.

The capture was re-exported through the shipped sequencer at that commit, with 5-minute pulls, and counted over a 3-hour window of steady pods (2026-10-15T06:55:26Z to 09:55:26Z). The objects producer (k8sobjects) wrote a mean of 1,003.6 records per pod-day over 660 pods, a per-day figure scaled from that 3-hour window. The count registered before the run, per steady pod-day, is (2 + 2C + 2A) x 144 records, for a pod with C containers and A addresses named by that producer. Every class of pod, gated and control, in both producers, came in at 1.000 of that count. This checks that the writer's address claims cost exactly what was predicted. It is not compared here with the synthetic stream's records per pod, because no labelled count of that exists.

## 6. Accuracy

**Label:** `local, deterministic`.
- Commit `c43a2922adb3cd1d86560e54820a78882cc0d149`.
- Dataset ITBench-Lite (IBM Research, Apache-2.0), https://huggingface.co/datasets/ibm-research/ITBench-Lite at revision `d0916b08ba421ce5e672e9ad68aa947d938dfef0`.
- Inputs, by sha256: table A `e506d84d31c1677a3239315dad1d90146a5708c7d08c30c7db9f839c5f24f902`; table B `0e7b9fdf7eaad3a9e5685aeebe087b16f6cc32f40fea7135fe43f15228eed0db`; scored services `8f8d1e05165e27aa2a20ce0804668fd55669882dc429bcd461b8ce93ed06d4d2`; trace-coverage probe `cf1c50be920c00dcf5789f0b5380d75c07bc7dc753fe7fc38a88419232bfd767`.
- Results: `bench/eval/results/itbench-lite-d0916b08.json`, sha256 `8acdafea5214ad7b63df5a4e3044f91dad98051ac3617e853533eace3cc2e441`, equal to the hash logged before the results were read. A second run of the same command gave a results file with the same sha256.
- Command: §12.
- Bootstrap: 10,000 resamples, seed 20261015, paired 95 % percentile intervals.
- The acceptance rule was fixed on 2026-10-15, before any case was scored.

Headline stratum: service level, call-graph faults, the time-aware walk W, 25 cases (24 scorable).

| measure | value [95 % interval] |
|---|---|
| precision | 0.429 [0.337, 0.524] |
| recall | 0.672 [0.518, 0.821] |
| F1 | 0.523 [0.420, 0.621] |
| lift | 3.562 [2.840, 4.508] |
| F1 minus the one-hop baseline | 0.150 [0.020, 0.256] |

The one-hop baseline (B_1hop) has F1 0.373 [0.260, 0.504].

**Verdict: mixed.** A "good" verdict needs a recall of at least 0.80. The scorer reports the union graph's recall, 0.672, as the ceiling: B_union is reverse reachability over every edge in the window, ignoring when, and W walks the same edges with the time constraint added, so it cannot reach more (argued from how the two are built; the scorer reports the ceiling and does not judge it). With the ceiling at 0.672, "good" is out of reach on this dataset.

W's headline numbers equal B_union's exactly, so on this dataset time-aware walking added nothing measurable over the union graph. The other strata:
- Co-residency: 2 cases, cannot say.
- Pod-level strata: cannot say.
- The co-residency predictor scored on call-graph faults: poor (F1 0.201).

## 7. Correctness

- **Answers beside the CI runs:** the timing judge checks G0 for L/k64a2l1ns and L/off at 2, 7 and 30 days (`CI run 37875443885`, `CI run 37877783029`). G0 is not a CI measurement: it comes from local counters runs on the same plans, which the judge reads beside the CI builds. Those runs carry neither label, so their outcomes are not quoted. M/crdb1's G0 is not established on CI.
- **Answers in the read-work run** (§2, whose label applies): no answer differed from the reference engine, and none was unstable, for L/k64a2l5m, L/k64a2l1ns, L/off and M/crdb1 at 2, 7, 14 and 30 days, in both runs; the 134 reads at "now" were also checked before compaction, with none wrong.
- **Conformance against a reference engine:** every spike layout is checked against a reference engine "with no indexes, no cleverness and no persistence" (`bench/spike/oracle/oracle.go`). The suite tests its own power with what its own description calls "about fifty deliberately broken engines" (approximate; this report did not count them), each of which must be caught by the check written for it (`bench/README.md`). The product store has its own suite with broken stores (`internal/store/storetest`).
- **Seeded simulation of ingest:** each seed draws a small cluster with churn and a schedule of faults (restarts and crashes, refused or failed writes, silences, duplicates, drops, late backlogs and shuffled requests) on the path a request takes through the ingest sequencer into the reference store. It is compared with a fault-free control and with the simulated cluster, and every eighth seed is run twice, and the two runs must give the same digest (`internal/ingest/sequencer/dst_test.go`). It found two behaviours that are now accepted: flushed run extensions written about 14 minutes after a run started, which can delete later checkpoints on a shared prefix; and a failed flush write that can leave a reference absent for a bounded time, read as a liveness expiry. It does not yet run on the Pebble store.
- **Background retention:** in the store's tests, a background pass leaves the same data as a synchronous one on seeded streams (`TestBackgroundAndSynchronousKeyspacesEqual` in `internal/store/pebblestore/settle_bg_internal_test.go`) and on random histories whose writes fall at random chunk boundaries (`TestABackgroundPassIsTheSynchronousOneWhateverTheInterleaving` in `retain_async_oracle_internal_test.go`). Other tests cover a close during a settle and the reopen after it (`TestCloseDuringSettleThenReopen`), and a crash during a pass and its resume (`retain_async_internal_test.go`).

## 8. Engine choice: RocksDB under layout L

**Label:** `CI run 38908320193`.

Rule R version 1 was registered on 2026-10-15 (registration text sha256 `5e3a5b17be8ab3d7bd00d61e79395e0d8c55e62ea9f11ff7bae640e6af7880a0`), before any timed RocksDB run. It compares layout L on Pebble v2.1.7 with layout L on RocksDB 11.8.1, with a zero tuning budget for both. The verdict is on `k64a2l5m` at 30 days; aarch64 decides and x86_64 guards the direction.

Dispatch B, `CI run 38908320193`: 30 days, revision `c43a2922adb3cd1d86560e54820a78882cc0d149`, plan digest ccd7ec4bdef4, 154,179,890 records, 869 queries, 50 builds, 5 repetitions per candidate and architecture. RL is layout L on RocksDB; RL4 is the registered sensitivity arm, RL with RocksDB's file sizes matched to Pebble's below the base level. Each figure is RocksDB's median against Pebble's. A side is better or worse only by more than the limit, the larger of x1.10 and the noise floor. aarch64 is Neoverse-N2; x86_64 pools AMD EPYC 7763, 9V45, 9V74 and Intel Xeon Platinum 8573C.

**Verdict: Pebble stands.** RL/k64a2l5m is worse on G2. So the store stays pure Go. The deciding gate is G2, the retention plus the slowness after it, with synchronous retention, on the spike engine. The verdict is about the engine under layout L; it says nothing about the shipped store's writer, whose timing is not shown in this report (§4).

| gate | aarch64, RL/k64a2l5m (RocksDB against Pebble, ratio, limit) | aarch64 outcome | aarch64, RL4/k64a2l5m | x86_64, RL / RL4 |
|---|---|---|---|---|
| G2 | 4m9.535s against 2m15.178s, x1.846, limit x1.456 | **worse** | 4m16.817s, x1.900, worse | neither (x1.310, limit x1.747 / x1.513, limit x1.628) |
| G3 | 27.262976ms against 23.068672ms, x1.182, limit x1.400 | neither | 27.262976ms, x1.182, neither | neither (x1.286, limit x1.429 / x1.143, limit x1.385) |
| G4 | 39.8 against 43.2, x0.921, limit x1.100 | neither | 39.8, x0.921, neither | neither (x0.921, limit x1.100 / x0.921, limit x1.100) |
| peak RSS | 8140 MiB against 9206 MiB, x0.884, limit x1.241 | neither | 8332 MiB, x0.905, neither | neither (x0.942, limit x1.100 / x0.914, limit x1.132) |

RocksDB is worse on G2 on aarch64, and neither better nor worse on G3, G4 and peak RSS. On x86_64 every gate is "neither". Without checkpoints (RL/off, not part of the verdict), on aarch64: G3 6.815744ms against 8.388608ms, x0.812, floor x1.154, better; G2 3m31.442s against 1m32.147s, x2.295, floor x1.440, worse; G4 and peak RSS neither.

Dispatch A, `CI run 38906579866` (2 and 7 days; 84 jobs, 0 failed), has no 30-day builds and decides nothing; its figures are not quoted here. A 30-day pilot of RL/k64a2l5m (run 38825117403, one repetition per architecture) decided nothing and is not quoted. A first attempt of dispatch A (run 38848250631) is void: 64 of its 80 builds failed in the workflow's collect step, a defect fixed before A ran.

Rule R cannot show: read times, synced commits, the shipped store's retainer and writer state, RocksDB driven from C++, RocksDB built with its own defaults, versions other than those named, any change to Pebble's tuning after registration, the heaviest family, real traces, or user-defined timestamps.

## 9. The live cluster (observed run log)

This section gives a run's configuration and the checks that passed, nothing more. It is not a benchmark, it carries neither figure label, and it quotes no figure; no time is given; the 20 m expiry is a configured value.

What ran, on 2026-10-17:
- A k3s v1.36.4 cluster of three nodes (one server, two agents), each an OrbStack container on one Mac.
- The OpenTelemetry demo, trimmed (chart 0.38.6), driven by a scripted loader.
- The shipped collector configurations: `deploy/otel-collector/toposhift.yaml` with its trace-metrics opt-in on, and `deploy/otel-collector/tracemetrics.yaml` with its one placeholder set. The trace collector also wrote a capture file and its own metrics, for a registered capture (below).
- `toposhift serve` built from commit `ac76dbfbc407484dc1f500e55ae893f2c6b79acc` (binary sha256 `aa6fa7ded6d7eefb49225d305c1bb6f1ccd743d23d4eea01fb6ac2814eb1298b`), with `--retain on`, `--instance-ttl 6h` (how long a service instance lives after its last span count; 6 h) and `--dependency-expiry 20m` (how long a service and a `depends_on` edge live after their last service-graph sighting; 20 minutes here in place of its default of 6 h). The 20-minute expiry is a labelled perturbation: every service and `depends_on` end in this store carries it.

The run was registered before its first line (version 3, registration text sha256 `1e37b383c27f6b6bdab00092a4520b60321d75581aa3d2749b1ff9ddbfd04cdd`). Its registered purpose is a capture of the service-graph metrics over two days or more, for a later rule; that capture is read only after it ends, and nothing from it is quoted here. The checks below read toposhift's own answers during the run.

Erratum to the registration: it says no store existed when it was appended. One did. The halted first start had created the store, and it still holds the records of that halted earlier cluster (its nodes and their pods, ended by liveness expiry). `serve` was restarted on the same store with the same binary and flags. No check's window overlaps those records, and the registered capture never reads the store.

Checks, each passed:
- **C2.** `/status` reported retention on, set by the flag; resume on; the node agent on.
- **C3.** A pod resolved from its uid. At that instant its neighbours were its node (`scheduled_on`) and its container (`part_of`), and one address edge was withheld from the answer and counted in it.
- **C5.** After a rollout restart of one deployment, `what_changed` over the old and new pods listed the old pod as deleted and the new one as created, derived, with names.
- **C6.** One agent node was stopped and later restarted. `blast_radius` of that node over a window around the stop returned its pods and their containers, reached through `scheduled_on` and then `part_of`, with no truncation and no service. The taint eviction issued the pods' deletes once the node's toleration had passed, and the replacement pods were created on the other agent at once. The evicted pods' objects were removed, and their reaches ended, only after the restarted kubelet confirmed termination. So the ends mark when the objects were removed, not when the pods were evicted, and between the two both the old and the replacement pods appear alive in the graph. "No service" proves nothing here: none of the services on that node sets `service.instance.id`, so no instance edge existed to carry the failure.
- **C7.** `/status` for the cluster's metrics: not silent; counters non-zero only where expected; no refusal counted.
- **C8.** `neighbors_at` on the shipping service: `depends_on` forward to quote and reverse from checkout, derived.
- **C10, C11.** The quote service was scaled to zero, and restored inside its registered window. `what_changed` over shipping and quote showed the `shipping depends_on quote` edge ended with the end reason `liveness_expiry`, at last sighting + the labelled 20 m expiry. The quote service itself did not end: quote sets `service.instance.id`, and its instance keeps it alive under the 6 h instance TTL. The checkout to payment and checkout to email edges, which the scale-down also stopped, were not read.
- **C12.** After the restore, the `shipping depends_on quote` edge appeared again, with the new quote instance's edge.
- **C13.** `find_entities` found the demo's pods in their namespace, and a service by part of its name.

Limits:
- **Instance ids.** In the demo as configured, only ad, cart and quote set `service.instance.id`. The other services exist only as service-graph entries, with the 20 m lifetime and no link to a pod or a node, so a walk from pods to services reaches only those three.
- **The 20 m expiry.** The ends show the mechanism (last sighting plus the expiry), not production timing.
- **One small cluster on one machine,** a scripted loader, and one scale-down of one service on one day. Nothing here speaks to scale.
- **The path.** The trace collector reached the cluster collector through a host port on the server node, not through the shipped in-cluster Service path.

## 10. Known shortcomings

The repository README lists what the product does not do yet. This list is what the evidence does not show, and repeats the README where they meet.

1. **No read was timed.** Read claims are counts of work on synthetic, projected stores.
2. **Snapshots, Merkle or prolly trees (Dolt), RocksDB user-defined timestamps, time-series databases, temporal graph databases and Postgres range types were not measured.** Postgres is the first planned baseline.
3. **M stores fewer bytes per record:** 31.2, against 37.3 for L without checkpoints and 41.6 for L with checkpoints at a 1 ns lag, whose figure includes its checkpoints (`CI run 37859480901`; CI's G4, not comparable with §5's bytes per record). Why M's keys take fewer bytes was not measured.
4. **Layout L without checkpoints fails G1** in 59 of 142 decided cells, more than M's 26 (`local, deterministic`; label in §2). Layout L passes only with its checkpoints. On service-reverse steps and decoded entries, L/off has exactly M/crdb1's G1 count (internal steps plus decoded entries) in every decided cell (query by query in every query G1 judges), so, as registered before the run, that gap is attributed to checkpoints, not to the key layout. The attribution covers no other population, and rests on projected stores and one seed.
5. **The read-work result is a reproduction on one seed.** It repeats an earlier local run on the same plans, whose outcome was known before the run. The read budget is shaped on layout L's checkpoint bound, and L's margins are thin (0.93 projected on node-reverse reads at a 1 ns lag). L's pass on projected stores assumes a pinned prefix holds the same records as on a full store, which was not verified (§2).
6. **L with checkpoints at a 1 ns lag is over its write gate (G3) on aarch64** at 2, 7 and 30 days. At the shipped lag no G3 gate was applied (§3).
7. **Synchronous retention fit 60 s for neither L candidate at 30 days** on CI (`CI run 37877783029`), nor on x86_64 at 7 days (`CI run 37875443885`). M was not built at 30 days.
8. **The shipped store has no CI evidence** for its background retention, and **v0.1.0 is released with retention off by default.**
9. **Every timed figure is the spike engine's,** with synchronous retention and Sync off, on a synthetic workload: layouts L and M on Pebble, built by `spikebench`, and, in §8 only, layout L on RocksDB, built by `rocksbench`. `CI run 37859480901`, `CI run 37875443885` and `CI run 37877783029` have L at a checkpoint lag of 1 ns; `CI run 38908320193` has it at the shipped lag. The heaviest family (ph5m-rmae, with 5-minute pod heartbeats) was not timed. x86_64 G2 and G3 at 30 days are not judged because of noise.
10. **M was never built at 30 days on CI,** and M's G0 was not established on CI (in the read-work run, on projected stores, no answer differed at 2 to 30 days; `local, deterministic`, label in §2).
11. **Blast-radius results are "mixed".** Recall is 0.672, equal to the time-ignoring union graph, and co-residency could not be judged (2 cases) (`local, deterministic`; label in §6).
12. **Alibaba is one censored hour, placement only, and the coalesced log is not the smallest way to keep it.** The storage claim is about 2x fewer bytes than storing every sample in the same store, not fewer than every alternative (`local, deterministic`; label in §5):
    - A write-once sorted Parquet file of every sample takes 4.98 times fewer bytes than the coalesced log at 60 s and 4.04 times fewer at 300 s. The same kind of file made from the coalesced stream takes 8.49 times fewer bytes than the store holding the same records (reported, not a registered ratio). Such a file takes no append or late event without a rewrite, and its read cost was not measured. This is reported, not hidden, and not counted against the read claim.
    - The ~2x is bounded by the product's settings: with a TTL of three intervals and extension at half the TTL, a steady heartbeat's record ratio cannot exceed about 2, and the observed 1.935 is 97 % of that. What the measurement adds is that bytes follow records.
    - Each coalesced record takes slightly more bytes than a raw sample: 35.11 against 34.96 bytes per record at 60 s. So bytes fall a little less than records (1.63 times fewer bytes for 1.71 times fewer records at 300 s).
    - The change-only lower bound I has the highest bytes per record (60.10 at 60 s), and no checkpoints.
    - R2 is for a one-hour window; that it grows with longer windows is argued, not measured (§5).
    - Canonical table bytes count only the data, every table in the bottom level. A running store also carries the extra copies its compaction levels hold, its write-ahead log and obsolete files, so its disk use is expected to be larger; that was not measured.
    - These bytes per record are not comparable with CI's G4 (§3).
13. **The synthetic workload may write fewer records per pod than a cluster-shaped stream.** On a kwok capture, both producers re-assert every pod, container and edge at each pull, which the synthetic stream does not; no labelled count compares the two. Record volume at production scale was not measured; only a model exists, and it is not quoted.
14. **Read work was measured with retention on,** at 30 days of retained history and its projected doubling. Range answers on a long-lived store cost more as it ages, because an entity's history is folded from the start of retention; with retention off, which v0.1.0 will have by default, that start does not move.
15. **Some read-cost gaps remain:** checkpoint holes left by late records are not repaired; translator resets can still raise read work in watch-only mode; native sidecars are not read.
16. **The memory the shipped writer needs at scale is not measured on CI.** The spike's checkpointing writer at the shipped lag had a median peak RSS of 9206 MiB (aarch64) and 9292 MiB (x86_64) at 30 days, against 3356 MiB and 3412 MiB without checkpoints, and 11255 MiB in its largest build (`CI run 38908320193`). The shipped store's writer is built differently (a compact state instead of the spike's per-prefix map), and no memory figure of it is quoted.
17. **The live cluster is one observed run, not a benchmark** (§9). In the demo as configured, only ad, cart and quote set an instance id.
18. **A stranger cannot re-run everything from the repository today.** The registration log, the local run scripts, the evaluation's probe file, the kwok capture and its counting tool, the counters runs that give G0, and the judges' printed outputs are not in it. CI artifacts are kept for 90 days after each run.

## 11. Methodology

- **Pre-registration:** every measurement quoted here was registered in a dated log, kept outside the repository, with its commit, inputs, thresholds and void conditions, before its figures existed; the read-work run reproduces an earlier run whose outcome was known (§2). Nothing quoted was re-run or re-thresholded after its figure was seen without a new registered version.
- **Frozen rules:** the timed judge and the read-work judge both print rules digest `ea47515f5ec1` (version 5).
- **Counters decide, and timing settles only what counters leave open:**
  - Timings, rates and memory come only from GitHub-hosted runners, quoted with a run id.
  - Laptop figures are quoted only as deterministic counts that gave the same digest on two runs.
  - Nothing from the Oracle Cloud VM is quoted.
- **Canonical stores** for any block or byte counter quoted as `local, deterministic`: rewritten once in key order into bottom-level 64 MiB tables. A running store is expected to count more (§10 item 12); that was not measured. Block counters of a projected store are reported and not decided.
- **Noise:** x86_64 pools CPU models only with at least 3 repetitions, and a cell is not judged where the reference varies by more than 1.5x. aarch64 is one CPU model (Neoverse-N2).
- **Raw outputs are not committed,** except the evaluation's results file. The report quotes them in its text.

## 12. How to reproduce

What a stranger can reproduce today:

| figure | reproducible from the repository and public data? | what is missing |
|---|---|---|
| CI timings (§3, §8) | Only while GitHub keeps the runs' artifacts (90 days after each run); re-dispatching gives new timings, not these | The artifacts once expired; the local counters runs that give G0 |
| Rule R verdict (§8) | Yes, by judging the downloaded artifacts again, while they are kept | RocksDB 11.8.1 installed to build the judge |
| Read work (§2) | Yes, with the commands below | Nothing; only the binary's sha256 is platform-bound |
| Storage (§5) | Yes, with the trace fetched from its program's repository | The full set of load commands is listed below; the scripts that ran them are not published |
| Record count on a kwok capture (§5) | No | The capture and the counting tool are kept outside the repository |
| Accuracy (§6) | Not the score stage | The trace-coverage probe file is not in the repository |

Variables used below: `$OUT` is an empty directory outside the checkout for binaries and outputs; `$DIR` is an empty directory for one read-work run; `$TRACE` is the directory that holds the trace's `MSMetricsUpdate/` and `CallGraph/` folders. The binary hashes of §2 and §5 hold only for go1.27.2 building for darwin/arm64; the Rule R judge's binary hash depends on the machine and the RocksDB library it was built with. On another platform, compare the output digests instead (pins, plans, `g1.txt` and `g1.json`, content and data digests, archive files); they are expected not to depend on the platform, but this was not checked on another platform.

- **CI timings (§3):**
  1. `gh run download <id>`. GitHub keeps these runs' artifacts for 90 days after each run.
  2. Then `spikebench timing -in <artifacts> -counters <counters runs> -join-from <first timed run's artifacts> -git <checkout>`, at the run's revision (`bench/README.md`, "timing"): `eed10127c9bd62a6a08fea1da21ee5cf1704cc2b` for runs 37859480901 and 37875443885, `6debad4eebf3c9218ce12555c92c1610ae9c89e2` for run 37877783029. The counters runs are local runs on the same plans; they give G0, and they are not published, so a stranger has to make their own.
- **Rule R (§8 and the last block of §3):** the dispatches, at commit `c43a2922adb3cd1d86560e54820a78882cc0d149`, were:
  ```sh
  # A, run 38906579866 (2 and 7 days)
  gh workflow run bench.yml --ref main -f family=rmae -f windows=2,7 \
    -f candidates=L/off,L/k64a2l5m,RL/off,RL/k64a2l5m -f reps=5 -f arches=both -f pins_window=2 \
    -f sync=false -f metrics_every= -f go_mem_limit= -f retention_mode=sync -f retain_flush_dels=default \
    -f read=true -f max_parallel=11 -f expected_sha=c43a2922adb3cd1d86560e54820a78882cc0d149
  # B, run 38908320193 (30 days, the verdict)
  gh workflow run bench.yml --ref main -f family=rmae -f windows=30 \
    -f candidates=L/off,L/k64a2l5m,RL/off,RL/k64a2l5m,RL4/k64a2l5m -f reps=5 -f arches=both -f pins_window=2 \
    -f sync=false -f metrics_every= -f go_mem_limit=11GiB -f retention_mode=sync -f retain_flush_dels=default \
    -f read=true -f max_parallel=11 -f expected_sha=c43a2922adb3cd1d86560e54820a78882cc0d149
  ```
  To judge B again: download its artifacts and plans into a directory of their own (A and B are never pooled), build the judge at that commit with RocksDB 11.8.1 installed (`bench/rocksdb/scripts/build-rocksdb.sh`), and run it:
  ```sh
  gh run download 38908320193 -D B
  cd bench/rocksdb && ./scripts/cgoenv.sh go build -tags grocksdb_no_link,pebblegozstd -o "$OUT/rocksbench" ./cmd/rocksbench
  "$OUT/rocksbench" judge B
  ```
  The judge's printed output for B, quoted in §8, has sha256 `7d1cb63af3689679605e6e5ef0b3d93f5c00b35f1676eb4458678f48e64c71f0`; the judge binary that printed it had sha256 `a47ae35bad800d7baad319d47e31fcb148500bf33e1cf5e21572aebfa1267d1b`. It ended with status 0, with no refusal and no void build.
- **Accuracy (§6):**
  1. Fetch ITBench-Lite at revision `d0916b08ba421ce5e672e9ad68aa947d938dfef0` from https://huggingface.co/datasets/ibm-research/ITBench-Lite, and point `-data` at the folder that holds the `Scenario-N` folders (`snapshots/sre/v0.2-B96DF826-4BB2-4B62-97AB-6D84254C53D7` at that revision), with each scenario's traces export fetched.
  2. At commit `c43a2922adb3cd1d86560e54820a78882cc0d149`, from `bench`, run `CGO_ENABLED=0 go run ./cmd/evalblast -source itbench -data <ITBench-Lite dir> -out <dir>` with `-stage truth`, then with `-stage map -table-a <sha>`, then with `-stage pool`, each with the same `-source`, `-data` and `-out`, and check each printed sha256 against §6.
  3. Then, with the trace-coverage probe file (sha256 `cf1c50be920c00dcf5789f0b5380d75c07bc7dc753fe7fc38a88419232bfd767`; not in the repository, §10 item 18), run:
     ```sh
     cd bench && CGO_ENABLED=0 go run ./cmd/evalblast -source itbench -data <ITBench-Lite dir> -out <dir> -stage score \
       -table-a e506d84d31c1677a3239315dad1d90146a5708c7d08c30c7db9f839c5f24f902 \
       -table-b 0e7b9fdf7eaad3a9e5685aeebe087b16f6cc32f40fea7135fe43f15228eed0db \
       -services 8f8d1e05165e27aa2a20ce0804668fd55669882dc429bcd461b8ce93ed06d4d2 \
       -probe <probe.tsv> -probe-sha256 cf1c50be920c00dcf5789f0b5380d75c07bc7dc753fe7fc38a88419232bfd767
     ```
  4. Compare the sha256 of `<dir>/itbench-lite-d0916b08.json` with `8acdafea5214ad7b63df5a4e3044f91dad98051ac3617e853533eace3cc2e441`, the committed `bench/eval/results/itbench-lite-d0916b08.json`.
- **Read work (§2):** at commit `fefd99ff79b1dc55f37f05e752b8754e553df0ed`, in a clean checkout. These are the commands the scripts `p1-build.sh`, `p1-plans.sh` and `p1-run.sh` (hashes in §2) run, once per run:
  ```sh
  cd bench && CGO_ENABLED=0 go build -trimpath -o "$OUT/spikebench" ./cmd/spikebench
  "$OUT/spikebench" pins -out "$DIR" -window 2 -preset ci -run-max-age 30m
  for d in 2 7 14 30; do
    "$OUT/spikebench" plan -out "$DIR/R$d" -window "$d" -pins "$DIR/pins.json" -preset ci -run-max-age 30m
  done
  "$OUT/spikebench" windows -out "$DIR" -windows 2,7,14,30 -preset ci -run-max-age 30m \
    -candidates L/k64a2l5m,L/k64a2l1ns,L/off,M/crdb1 -rest-after-retention -canonical-layout
  ```
  Compare the sha256 of `$DIR/pins.json`, the four `$DIR/R*/plan.json`, `$DIR/g1.txt` and `$DIR/g1.json` with §2's, then read `$DIR/g1.txt`.
- **Storage (§5):** fetch the trace yourself from the program's repository (it is not mirrored here), write §5's block of archive hashes to `SHA256SUMS` beside the archives, and check them with `shasum -a 256 -c SHA256SUMS`; the file's own sha256 should then be §5's. Then, at commit `ac76dbfbc407484dc1f500e55ae893f2c6b79acc`, in a clean checkout, with DuckDB v1.5.6 (sha256 in §5). These are the commands the scripts `p3-build.sh` and `p3-run.sh` (hashes in §5) run, each reduction, load and archive twice:
  ```sh
  cd bench
  for c in alibabareduce activitybytes dbhash; do CGO_ENABLED=0 go build -trimpath -o "$OUT/bin/$c" "./cmd/$c"; done
  cp cmd/activitybytes/testdata/archive.sql "$OUT/bin/archive.sql"
  "$OUT/bin/alibabareduce" -in "$TRACE" -max-file-bytes 0 -out "$OUT/reduce-t3" -arms s,i
  "$OUT/bin/alibabareduce" -in "$TRACE" -max-file-bytes 0 -out "$OUT/reduce-t15" -ttl-factor 15
  D=$OUT/reduce-t3/out
  T=$OUT/reduce-t15/out
  AB=("$OUT/bin/activitybytes" -out "$OUT/stores")
  "${AB[@]}" -label E -header -empty                                   # the empty store
  "${AB[@]}" -label C60/S     -in "$D/placement_c60_s.parquet"  -expect-records 57827320 -samples 57827320 -checkpoints default
  "${AB[@]}" -label C60/C     -in "$D/placement_c60.parquet"    -expect-records 29889037 -samples 57827320 -checkpoints default
  "${AB[@]}" -label C60/I     -in "$D/placement_c60_i.parquet"  -expect-records 971472   -samples 57827320 -checkpoints default
  "${AB[@]}" -label C60/S0    -in "$D/placement_c60_s.parquet"  -expect-records 57827320 -samples 57827320 -checkpoints off
  "${AB[@]}" -label C60/C0    -in "$D/placement_c60.parquet"    -expect-records 29889037 -samples 57827320 -checkpoints off
  "${AB[@]}" -label C300/S    -in "$D/placement_c300_s.parquet" -expect-records 11566626 -samples 11566626 -checkpoints default
  "${AB[@]}" -label C300/C    -in "$D/placement_c300.parquet"   -expect-records 6750557  -samples 11566626 -checkpoints default
  "${AB[@]}" -label C300/I    -in "$D/placement_c300_i.parquet" -expect-records 967603   -samples 11566626 -checkpoints default
  "${AB[@]}" -label C60/C-T15 -in "$T/placement_c60.parquet"    -expect-records 8682677  -samples 57827320 -checkpoints default
  mkdir -p "$OUT/archive" "$OUT/duck-tmp"
  for f in c60_s c300_s c60; do
    duckdb -c "SET VARIABLE src = '$D/placement_$f.parquet'" -c "SET VARIABLE dst = '$OUT/archive/$f.parquet'" \
      -c "SET VARIABLE tmp = '$OUT/duck-tmp'" -c ".read $OUT/bin/archive.sql"
  done
  ```
  These are the loads and archives that were run, each twice (the scripts also wrap each step in resource limits and state files, none of which changes an output). A stranger can compare their own two loads of each arm with each other (data digest, table bytes, keys, records and checkpoints), each content digest with §5's, and each archive file's sha256 with §5's.
- **Record count on a kwok capture (§5):** with the counting tool (kept outside the repository, built against commit `c2dcd6c713651f9d43e1d4b34cbcee46313b2d11`; binary sha256 in §5) as `$A`, the capture as `$R/out/real-5m.jsonl`, and an empty output directory `$O` (the names `real-5m.jsonl` and `kwokreal` are the capture's labels; the cluster is kwok-simulated):
  ```sh
  $A export-capture -out $O/x5 -pull-interval 5m -cluster-name kwokreal $R/out/real-5m.jsonl > $O/export.txt
  $A inspect $O/x5/activity.parquet > $O/inspect.txt
  $A count -from 2026-10-15T06:55:26Z -to 2026-10-15T09:55:26Z -refresh 5m -out $O/count5.json $O/x5/activity.parquet > $O/count.txt
  python3 -I o1verdict.py $O > $O/verdict.txt
  ```
  Compare the sha256 of `$O/verdict.txt` with §5's.
