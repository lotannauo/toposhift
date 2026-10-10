# bench

A separate Go module for everything that must not be linked into `toposhift`:
the storage-engine spike now, and later the baselines (RocksDB, Dolt,
SurrealDB, XTDB) and the benchmark drivers.

It is a separate module because it carries a `replace` of the root module to
`../`, which the root `go.mod` must never have (a `replace` breaks
`go install github.com/lotannauo/toposhift/cmd/toposhift@latest`). Import-path
based `internal/` visibility still lets it use the root module's `internal/`
packages. There is no committed `go.work`.

```sh
mise run bench:test        # the full tests, Pebble's invariant checks on, cgo off
mise run bench:test:race   # the trimmed tests under the race detector (go test -short)
mise run bench:test:deep   # the full tests under the race detector, in random order
mise run bench:lint    # lint
mise run bench:vuln    # govulncheck on this module's dependencies
mise run bench:purego  # the Pebble code builds and runs with CGO disabled
mise run check         # between edits: format and lint checks, bench:test, root tests without -race
mise run ci            # before committing: includes the bench tasks
mise run ci:deep       # what a push to main and the nightly run add: everything under -race
```

`check`, `ci` and the tasks above other than `bench:test:deep` reuse a package's
result when its test binary and the inputs it read are unchanged, so after an edit
only the packages it reaches rerun (pebblelog alone: about 15 s on 10 cores). A
comment-only change that moves no line leaves every binary the same and reruns
nothing; a comment that adds or removes a line changes the line numbers compiled
into its package, and so reruns it and everything that imports it.

## spike

- `spike/engine`: the narrow interface a storage candidate implements, and the
  record it stores. Not the product's `Store`.
- `spike/workload`: a deterministic generator of churn (late records, same-second
  ties, outages, heartbeat runs, incompressible payloads), with options for the shapes
  of real churn and presets of cluster size (see below).
- `spike/oracle`: the reference engine, built on the lifecycle specification.
- `spike/conformance`: the conformance test every candidate must pass, which
  checks a candidate against the oracle on generated workloads.
- `spike/pebblekv`: what the Pebble-backed candidates share: the numeric ids that
  keys carry (pinned by a golden file), the value codec, how a database is opened,
  and the liveness rule applied to the versions a read finds. The plumbing (opening and
  tuning, the value codec, the ids, the meta keys, the settle after a retention, the
  iterator recording) is the root module's `internal/store/pebblekv`, so the
  benchmarks measure the code the product store runs on; this package adds the
  cockroachkvs layout and what only a measurement needs (the waits, the snapshot,
  the description of a database, the canonical rewrite).
- `spike/pebblemvcc`: layout M, per-edge MVCC versions on Pebble's `cockroachkvs`
  (see its package documentation for the key layout, the read and the retention).
- `spike/candidates`: the list of variants a measurement runs, each with the name it goes
  by in results (`M/crdb1`, `M/crdb1+filter`, `M/default`, `M/default+filter`, `L/off`,
  `L/k64a4l1ns`, and any other checkpoint policy as `L/k<K>a<alpha>[l<lag>]`), so that the
  variant measured is the variant checked: every one of them passes the conformance
  checks at the benchmark settings, and a name is checked to be what it says in the
  tables it writes.
- `spike/pebblelog`: layout L, a log per entity, direction and layer in Pebble's
  bytewise key order, newest first, with the retention baseline (see its package
  documentation). With checkpoints off a read replays the history older than the
  instant; with them on, interleaved checkpoints let it stop early.
- `spike/rootlog`: the same layout L built through the root module's store
  (`internal/store/pebblestore`), so that what the spike measured can be repeated on
  the code the product runs. Its candidates are `Lroot/off` and `Lroot/k<K>a<alpha>[l<lag>]`
  (the grammar of `L/...`), found by `candidates.Lookup` and not in the default set. The
  store is opened with the runner's database configuration unchanged, no boot key in the
  lifecycle policy (a boot key changes the timings and the read counters) and the
  checkpoint policy stated outright, and its database is given the waits, the snapshot
  and the canonical rewrite through `pebblekv.Wrap`. The manifests and results of an
  `Lroot` build and an `L` build of one plan list the same counters, with the store's few
  extra names, and the data keys the two leave (`dbhash -data`) are byte for byte equal;
  the gates do not compare the two with each other, since the names differ.

### The workload's shapes of real churn

A store is judged on the stream it is given, so the generator has options for what
the small test streams leave out. Each is off by default, and a config that does
not set it gets exactly the stream it always got (a golden digest per config in
`workload/golden_test.go` fails if it changes):

- `FreshIdentities`: every pod, service instance and container is a new identity each
  time the pod is created, and a reschedule is a deletion and a creation on the new
  node (Kubernetes never moves a pod). Without it a pod slot keeps one identity for
  ever, so a busy node only ever meets as many distinct peers as there are slots,
  which favours any layout that keeps one key per peer. `Generator.Entities` lists
  every entity that has ever existed, and the conformance test probes the dead ones
  too.
- `MaxPodsPerNode`: at most that many pods on a node (Kubernetes' default is 110).
  With a Zipf-skewed choice of node the busiest holds a fifth or more of all pods
  otherwise.
- `PodHeartbeatInterval`: the cluster-level collector re-asserts every live pod's
  placement each interval, and nothing withdraws it when the pod moves, so it expires.
  The busiest node's reverse prefix then holds a live run for each pod, each extended
  on every refresh, and each extension is a record in the past that invalidates any
  checkpoint written after the run began.
- `BacklogEvery`, `BacklogMeanDelay`, `BacklogSpan`: a producer's pipeline backs up
  for a span and drains in order. Independent per-record lateness (`LateProbability`)
  reorders one producer's own records, which real lateness does not; the two models
  are exclusive.
- `ExtendTTLFraction`: a run is re-asserted at most once per that share of its own
  TTL (the design's rule is half), where `ExtendEvery` is one interval for every run.

`workload.CI()`, `Week()` and `Month()` (three, ten and thirty-one days of
`workload.Cluster`) put these together for a cluster of about 400 nodes and 20,000
pods at about 2.6 million records a day. They are starting points, not
measurements of a real cluster. Before measuring anything on them, look at what they
amount to:

```sh
cd bench && go run ./cmd/streamstats -preset ci
```

It prints records a day by kind, the share that arrives late, how many dead entities
sit behind each live one, and for each kind of prefix (a layer, an entity type and a
side) how many distinct peers it meets, how many it holds at the end, how many records
it holds within the last 2, 7 and 14 days (over the prefixes that hold any), and how many of its records are in the past
of the newest in it (what invalidates a later checkpoint). Three days take about 15 s
and 1 GB (measured). It keeps a small entry per prefix and fresh identities make
millions of prefixes, so expect memory to grow about in proportion to the days: roughly
3 GB for ten days and 10 GB for a month. Its live degree and its late share are
approximate under independent lateness (a record more than an hour late can revive an
edge).

Not yet modeled: heartbeats still fire for every node on the same second (a burst a
timed write test would want spread out), and an outage ending does not release a burst
of backed-up records.

### What a measurement reads

An engine reports what it does through the optional parts of `engine/hooks.go`, so a
runner can read it without knowing Pebble:

- **Per read**, to the `Recorder`, under `read.<op>.<name>` for each of `neighbors`,
  `batch`, `alive` and `window`, from the statistics Pebble keeps on the iterator
  (`pebblekv.RecordIter`): `reads` (a count) and samples of `block_bytes`,
  `block_bytes_cached`, `block_read_ns`, `points`, `key_bytes`, `value_bytes`, `seeks`,
  `steps`, `internal_seeks`, `internal_steps`, `covered_by_tombstones`,
  `separated_values` and `separated_value_bytes_fetched`. They are in one unit for every
  layout; the layouts' own counters (versions or records stepped over) are not, because
  each means something different by a step. The number that matters most is
  `internal_steps` against `steps`: the iterator steps over every stored version of a key
  to show the caller one, so five versions of each of a hundred keys is 100 steps and 500
  inner steps. Block bytes depend on what has been compacted when, so they are only
  comparable after `CompactAll`.
- **`Quiescer`**: `Quiesce` waits until nothing is flushing or compacting, Pebble has
  loaded the statistics of every table (they hold its tombstones and garbage estimates, and
  the compactions that drop tombstones are picked when they arrive; Pebble's own "nothing
  queued" is true while a job is still loading them), no file is marked for compaction
  (unless automatic compactions are off) and all of that has held for a second; `CompactAll`
  compacts everything into as few tables as Pebble will and then waits for rest. `Settle`
  is the quick version the conformance test uses between rounds.
- **`Statser`**: flat counters from `pebblekv.Snapshot`: flushes, compactions, read
  compactions, tables and bytes per level, read amplification, the garbage point and range
  tombstones still hold, cache hits and misses, and the bytes written in, flushed and
  compacted (write amplification is `(bytes_flushed + bytes_compacted) / bytes_in`).
  `Size` is the bytes on disk including the log and files not yet deleted; the size to
  compare is `live_table_bytes` after `CompactAll`.
- **`LayerSizer`** and **`Breakdowner`**: Pebble's estimate of the table bytes of each
  layer, and a scan that divides the logical bytes of every data key and value into
  parts that add up to all of them: `observe`, `extension` (a run re-asserted with a
  later Through) and `delete`, each without its payload; the payload by the direction it
  is stored under (`payload forward`, `payload reverse`, `payload entity`); and for layout
  L `checkpoint` and `baseline`. The payload parts are checked against the payloads of the
  stream that was written.
- **`Describer`**: the settings the engine was opened with and, read back from its tables,
  the key schema and block-property collectors they were written with (every table also
  carries Pebble's own `obsolete-key` one), so a result says what it was measured on.

A measurement of a built database opens it with `Config.DisableAutoCompactions` and
`Config.DisableReadCompactions`, so the tables do not change under the reads being timed;
the first is checked by its effect and the second by the options it comes to (read-triggered
compactions do not fire at the scale of a test). `KV.CloseClean` flushes before closing, and
`KV.RecoveredBytes` is what opening wrote to tables from the log, which a measurement
refuses to be anything but zero.

### Comparing candidates: spikebench

`cmd/spikebench` builds the candidates on one stream, asks each the same questions and
sets what the reads cost side by side. It reads **counters, not clocks**: Pebble's
iterator statistics for every read, the bytes each layout holds, what retention did. A
counter repeats from one run to the next (every query is asked twice and one whose cost
differs is flagged), so it can choose a design locally; a timing is only a confirmation
and comes later, from CI hardware. Three counters are known not to repeat and are never
decided on as they are: the bytes a read was served from the block cache; the allocations of a read, which depend on the process (they are the fewest of three runs after two to fill the pools, never required to repeat, and read only above their floor); and, for the batched reads
of layout M, the number of points Pebble reports with a value in a value block (a few
more or fewer from one pass to the next; unexplained).

```sh
cd bench && CGO_ENABLED=0 go build -o ~/spikebench/bin/spikebench ./cmd/spikebench
~/spikebench/bin/spikebench run -preset ci -out ~/spikebench/ci   # plan; build and read each candidate; report
```

The steps are separate processes so that one engine's heap and block cache never share a
process with another's, and each can be run alone:

| Step | What it does |
| --- | --- |
| `plan -preset P -out D` | Reads the workload twice with no store. The first pass finds the prefixes to ask about (the busiest of each class by records and by run extensions, and some around the middle, among those that exist at the end); the second feeds the reference engine only the records that touch them and records its answer to every query. `D/plan.json` holds the stream's digest, the queries and the answers, and nothing about the machine, so one spec makes one plan. A group of queries whose answers are mostly empty is an error: candidates would agree on nothing. |
| `build -candidate C -out D` | Writes the planned stream to `C` in batches of the planned size, dropping the records before the horizon as a store would refuse them, moving the horizon as planned, and checks the token after every batch. It compacts everything, records what the database holds in `D/<C>/manifest.json`, and opens the closed database read-only as `read` will, checking it as `read` checks it. Its block cache holds the plan's size of blocks besides the memtables, which Pebble charges to the same cache: without that room a cache no larger than the memtables (the plan's 64 MiB against 64 MiB memtables) held no block at all, and every read of a retention or a checkpoint went to the file. After each retention, with `-rest-after-retention` (on by default for an `-untimed` build, off otherwise, and recorded in the manifest), the build waits until the database is at rest: the deletions a retention writes otherwise pile up in memory behind a writer that runs ahead of the compactions, and a first batch after a retention that follows the wait does not carry the compactions' catch-up, so the report does not compare that figure of a rested build. What a build records of its history before the final compaction (the counts of flushes and compactions, the bytes compacted, the shape of the tables at the end of the stream and the block bytes of the reads before compacting) depends on how fast the writer ran against the compactions, and is not repeated exactly from one build to the next; nor, for some candidates, are the table boundaries `CompactAll` leaves, and with them the block counters of the reads (a few percent on the queries that differ). With `-canonical-layout` (off by default, and recorded in the manifest as `canonical_layout`) the build then rewrites the compacted data once, in key order, into bottom-level tables cut at the first new key prefix past the target file size, written with the candidate's own comparer, key schema, collectors and table format and ingested in place of the old ones: the tables, and the blocks every read after the compaction loads, are then a function of the data, while the answers and the engine's counts do not change, and nor does what a read steps over, except where the compacted tables still held tombstones, which the rewrite drops (a read then steps over fewer). The bytes of the tables, the blocks a read loads and where a value is kept (in place or in a value block) do change, so the report and the windows refuse to compare a build with it and one without. With `-sync` (off by default) every batch is committed with a sync of the write-ahead log to the disk, as production does, which changes no stored byte and changes every commit's time; it is recorded in the manifest as `sync`, and the report and the windows refuse to compare a build with it and one without, so the timing of a checkpoint's second commit is measured in a build of its own while the gates stay on builds without it. With `-metrics-every D` (off by default, and at least `1s` otherwise) the build appends, every `D` and at each change of phase, a line of the database's own statistics (compaction debt, memtables, level 0, tables waiting for their statistics, the block cache) and of the Go runtime's memory to `metrics.jsonl` in the candidate's directory, to find what grows while a build runs; the interval is recorded in the manifest as `metrics_every`, and the report refuses to compare builds that sampled differently, because reading the statistics takes the database's metrics lock for a moment. `run` and `windows` take `-rest-after-retention`, `-canonical-layout`, `-sync` and `-metrics-every` too, with the same defaults, and pass their values to every build they start, so a family is built alike; one that resumes over a candidate whose manifest says otherwise stops before it builds anything. A build that does not come to the planned stream, fails a write or a retention, counts a failed checkpoint, or whose database changed after it was measured writes no manifest, so it cannot be read; its directory is left as it was and has to be removed before the candidate is built again. |
| `read -candidate C -out D` | Opens the built database in a new process **read-only** (nothing is written, flushed or compacted, no background work runs, the directory is as it was, and it is refused unless its tables and its last sequence number are the build's), with the same absolute block cache for every candidate, asks every query twice, compares each answer with the reference engine's, asks each twice more with the block cache emptied (the tables stay open) for the blocks a first read needs, counts its allocations, and writes the cost of each query to `D/<C>/results.json`. One that answers anything differently from the reference engine fails the step (its results are written all the same). |
| `report -out D` | Sets the candidates side by side (all that have been read, or the ones named), and fails if they cannot be compared; the full tables are always in `D/report.txt`. The first lines say whether the results can be compared at all: one plan, one stream, one binary, the same Pebble options (apart from the comparer, key schema and collectors, which are what a candidate is), every answer the reference engine's. If not, it says so first. |

What is asked: forward and reverse neighbors and `Alive` as of now, an hour, three hours,
nine hours and a day back (a read that far back needs that much stream behind it), and an old snapshot (read at the instant of its token, which is before any record from the token on takes effect, so that a pipeline backed up at the token does not make every heartbeat prefix look lapsed; with only what it saw); `Alive` an hour after the end
(`dead`: every refresh of a heartbeating node has lapsed by then, so the store has to establish that no record holds,
which a read of a live entity, stopping at the first record that does, never shows; a plan in which one is alive is refused,
and a stream whose nodes are not refreshed has none); windows of the last hour and the last day; and a batch of
the chosen prefixes together. The queries are chosen from the stream, not from any
candidate, and are asked of live prefixes only. Reads that would be empty by
construction are left out: a refreshed edge is one run, and all its refreshes carry the
instant it started, so the window of a heartbeat prefix is empty for an old run and measures the restart for a new one.

After the tables the report says how the counters of the rules put every pair of candidates in order in every cell (a group of queries at an age; a candidate's value in it is its mean over the queries), and lists the **mixed cells**, the ones in which the counters disagree, with a digest. The list is a function of the counters alone: it is what timing on CI hardware settles, and it is fixed before anything is timed.

The rules the counters will be read against (the gates, the slope that separates bounded
from linear growth, the factors) are in `runner.Rules`, with their digest pinned in
`runner/stream_test.go` next to the digests of each preset's spec; a change to either is
made on purpose. The values nobody has chosen yet are listed in `Rules.Placeholders`; setting one
changes the rules' digest and nothing built or read, because the rules are applied to the counters
afterwards, so no plan is stale for it.

#### Windows of retained history: G1

G1 asks whether the work of a read on the busiest prefixes stays within a budget as the
history a prefix retains grows, and whether it still would if the retention were doubled.
A store that has been shrunk to a window flatters layout L (shrinking folds a long run's
extensions into the baseline), so every window is a store that has **always** held it:

```sh
~/spikebench/bin/spikebench windows -preset ci -windows 2,7,14,30 \
    -candidates L/off,L/k64a4l1ns,M/crdb1 -out ~/spikebench/g1   # pins; each window; G1
~/spikebench/bin/spikebench g1 -out ~/spikebench/g1              # judge G1 again from what is there
```

- `pins` chooses once, from the stream of the shortest window, the busiest and the median hub
  prefixes of each class (a node's, a service's, a host's: entities that exist from the start and
  are never replaced; a pod's own prefix is not pinned, because a pod that exists at the end of a
  short stream does not exist in a longer one). `pins.json` holds them with a digest that every plan
  records, so every window asks the same questions of the same prefixes.
- A window of R days (`-window R`, `runner.WindowSpec`) is a stream of 2R + 1.5 days with a daily
  retention that keeps R from day R + 1 on; the stream ends twelve hours after the last. The
  long-lived runs, which all begin together at the start, then restart every R + 1 days, so at the end
  the oldest are R + 0.5 days old in every window: the worst case, and the same phase for every R.
- With `-pins` the stream of a run is the **projection** of the full one on the pins: only the records
  that touch a pinned entity are written to a store, in the same order and with the same sequence
  numbers, and the retention moves when the whole batch says (so the coalescer restarts runs at the
  same instants as in the full stream). A test checks that a projected store gives the answers, and the
  logical counters (seeks, steps, entries decoded), of a store given the whole stream. The block
  counters of a projection depend on the depth of an index that is shallower than a full store's,
  in level and in slope, and differ from a full store's in either direction: the cells of G1 on them
  are reported and not decided (marked ?) until the offset against a full build is recorded.
- With `-full` as well as `-pins` (`windows -full`, `plan -window R -pins P -full`), every store is given the
  whole stream and asked the pins' queries: a family of full stores read at the pinned prefixes, with the
  answers of the projected family, whose block counters G1 decides and whose other gates are judged. The plan
  holds the reference engine's records of the pinned entities only, as a projected plan does, and not the
  analyzer of every prefix that a plan without pins needs (about 6 GB at 7 days).
  The other gates (stall, commit, bytes per record, checkpoint share) are those of a full build and
  are not judged on a projection; only G0, the answers, is.
- The plan of a window generates the stream twice and holds the reference engine's records of the
  pinned entities in memory: at the `ci` preset a window of 14 days takes about 3 minutes and 6 GB, and
  one of 30 days about 8 minutes and 11 GB (measured on a Mac; the builds themselves are small).
- `g1` applies the rule in `runner.Rules` to every pinned query at the instants of the rules (now, three
  and nine hours back, a day back, and `dead`): each query's ratio to its budget (per entity read, and per
  item of the reference answer in that window; cold bytes in blocks of the run) in each window, the
  population's statistic of it in each window (its largest, or the median for the median population), and
  the statistic at the target window multiplied by the headroom raised to the growth, the least-squares
  slope of the statistic's logarithm against the window's over every window (none when the statistic's work
  at the target is at or below the counter's floor). A population passes if that is at most 1, and a
  candidate passes G1 if every population of every class does at every instant of the rules; windows whose
  plans do not ask one are judged on the others and no pass is shown. The slopes between the last two
  windows and between the first and third, the class the second gives, and the largest fit of one query's
  own work (flagged at or above the linear slope) are printed as diagnostics. `g1.txt` has the verdicts and
  every cell, and `g1.json` the same for tools.

Scenario flags, all of which leave the presets unchanged unless given: `-extend every` (every refresh
re-asserted, the control) or `-extend 0.5` (a share of the TTL), `-pod-heartbeat 5m` or `off`,
`-run-max-age 2h` (the ingest coalescer continues a run that reaches this age with a new one, for every
refreshed run whatever its layer: a bound on how far a layout that keeps an extension at its run's start
has to walk back; a run whose extension interval, half its TTL, is at least the bound is never extended), `-payload-pad 456`
(random bytes added to every payload from a source of their own, so the stream is the same records with
longer payloads), `-events-per-second` and `-cache-mb` (the block cache is the same absolute size in every
run, 64 MiB unless given). A checkpoint candidate is named by its policy, any of them: `L/k64a4l1h30m` has a
checkpoint 1 h 30 min behind the newest record of its prefix, which with `-run-max-age 1h` and an extension
interval of 30 min is behind every run that can still be extended (a lag is written without the units that
are zero at its end).

What a build records besides the tables: how long each batch, each retention and the first batch after
each took (informational: a timing of the machine that built it), the reads of "now" at the end of the build
before anything was compacted (a labelled cell, outside the verdicts, with each answer checked), and, for
layout L with checkpoints, the bytes of checkpoints written. The report prints the other gates (G0 answers,
G2 retention and the slowness after it, G3 commit, G4 bytes per record) and the share of the bytes written that
went to checkpoints.

**G2 and G3 since rules version 5.** G2 judges what a retention costs the writer: the retention's own time plus,
for each of the first 100 batches written after it, how much that batch took over the build's median commit
(never less than zero); a build's value is the largest of these over its retentions, and it passes within the
stall budget. The first batch after a retention is no longer judged as a ratio to the best candidate: it is the
first term of that sum, and the report prints it, with the verdict the former rule gave, as a diagnostic, as it
does the retention alone and the time a retention spent settling. G3 is the 99th-percentile commit alone. A build
records the batches after each retention (`post_retention_batches` in its description), and every layout settles
a retention's tombstones inside the retention (`settle_tombstones`); builds with and without it are never
compared, and a build that rested after each retention, or did not record those batches, is shown and not judged
on G2. The local report judges the absolute budget only; the relative fallback for a window where no layout fits
it belongs to the judgement of the timed builds. The rules' log of 2026-10-08 has the rule in full.

`streamstats` prints three models of what a deadline index would cost: every extension rewriting its
entry, and an index bucketed by the hour and by the day, which is written only when a run begins, lapses
or moves its deadline into another bucket.

`dbhash <db>...` prints, for each Pebble database it opens read-only, the number of keys, how many are
layout-L records, checkpoints and baselines, and a SHA-256 of every key and value, so two stores built from
the same stream can be compared byte for byte. With `-data` it digests only layout L's data keys (those whose
first byte is a layer, 1 to 4) and leaves out the meta keys, which are the part a change of the store's own
bookkeeping may change on purpose; `TestStoredBytesAreUnchanged` in `spike/pebblelog` freezes the same digest
for fixed streams. `-tails` reports how many keys a whole-prefix read of each data prefix reads and how many a
read that stops at the newest checkpoint and record does. It is for layout L: layout M's keys use another
comparer, which the command does not install.

**A binary that a result may come from** is built with `CGO_ENABLED=0` (the block cache is
then on the Go heap, as `toposhift` ships), without `-race` or the `invariants` tag, from
a clean tree of the repository with its version control stamp (a binary built with
`-buildvcs=false` has none, and cannot be told from one built from a dirty tree). `spikebench` refuses to build or read otherwise; `-untimed` allows it for a
validation run (an instrumented build of a full-size stream, say) and its results are
marked, and the report refuses to compare them. The output directory must be outside the
repository: `bench/results/` and `*.jsonl` under `bench/` are ignored, and `check:no-results` fails if any of
them, or a `plan.json`, `manifest.json`, `results.json` or `report.txt` under `bench/`, is tracked.

Size: the three-day `ci` preset is 7.9 million records and about 300 MB of tables per candidate (a database directory is larger while it holds a log and files not yet deleted), so about 1.8 GB for the six. The analyzer of the first pass keeps every prefix of the stream, about 1 GB for three days, more for `week` and `month`. How long a build takes is not a result and is not recorded.

**What the counters do not tell you yet.** The report prints every counter and chooses none, because they disagree:

- A layout that seeks to every key (M) and one that walks a run (L) are each cheap in the unit the other pays in. On a busy node's prefix a day back, M made many seeks to L's one, and loaded far more block bytes, while making fewer steps. "Steps" alone, or "block bytes" alone, would pick a different winner.
- `block bytes` counts every block load, a cached block too, and again at every seek that reloads it, so for a layout that repositions the iterator at every key it measures the repositioning more than the data. Measured against the distinct blocks a read needs (`block_loads` and `cold_block_bytes`), layout M's gross block bytes on a busy node's prefix were about ten times its distinct bytes, which is what put it two orders of magnitude above layout L in gross bytes and about one in distinct ones; layout L's were the same in both. That was measured on a one-day run, where the read "a day back" is made at the first instant of the stream, before most runs have a history, so it shows how the two count blocks and not yet how a read grows with history; it is to be measured again on a run of two days or more. `block_loads` and `cold_block_bytes` are therefore the numbers a rule rests on: with the block cache emptied and the tables open, the blocks the read misses (index, filter, data and value blocks) and the compressed bytes of the index, filter and data blocks among them, so a block is counted once however often the iterator comes back to it.
- `steps` counts the calls the layout made. A skip with `NextPrefix` is one step in the layout and many underneath, in a counter nobody exposes.
- Everything is read after `CompactAll`, which leaves a few large tables: range deletions, tombstones and L0 are not in the picture, and an iterator crosses fewer levels than in a running store. The reads of "now" at the end of the build, before anything is compacted, are recorded and printed beside them (a labelled cell outside the verdicts); the block bytes right after a retention are not measured.
- One seed, until `-seed` is varied: a family of windows is one seed and one scenario: the pins record the scenario they were chosen from (the hubs are the same entities in every seed, so the pins of another seed would otherwise be taken for its busiest prefixes), and a plan with the pins of another scenario is refused. How a read grows with retained history is what the windows measure (see above); a window of R days is a stream of 2R + 1.5 days, and the projection that makes the long ones cheap has a shallower index than a full store, so its block counters are lower. The prefixes asked about are ranked by the records a store still holds after the final retention, not by the whole stream: on a heartbeat prefix the busiest over the whole stream is the one with an unbroken run, which a retention folds into the baseline.
- The workload is synthetic and every default of it is provisional until the Alibaba and kwok replays exist.

#### Timed builds on CI hardware: timing

G2, G3 and G4 are judged on builds made on GitHub-hosted runners by the `bench` workflow, which
uploads, for each job (one runner, window, candidate and repetition), an artifact with the build's
`manifest.json`, a `job.json` that says what the job was and what it ran on (revision, binary, CPU
model, run), and logs. `timing` reads the downloaded artifacts and the plans, and judges them offline:

```sh
~/spikebench/bin/spikebench timing -in ~/spikebench/ci/run-1 -counters ~/spikebench/scen5 \
    -join-from ~/spikebench/ci/first-run -git ~/Developer/toposhift -json ~/spikebench/timing.json
```

`-in` (repeatable, and arguments after the flags are more) are searched at any depth for artifacts and
`plan.json` files; `-counters` for counters runs, which give G0 on the same plan; `-join-from` for the artifacts of
the first timed run, from which it is decided whether M/crdb1 joins the reference set (by default the `-in` builds);
`-git` is a checkout of the repository, in which two read-only git commands check that the builds' revision is an
ancestor of `-ref` (default `origin/main`); `-json` also writes the verdicts for tools, outside the repository.
The rule is the one in the rules' log of 2026-10-07 and of 2026-10-08: builds without a sync of the log, without a
rest after a retention, not rewritten into the canonical layout and not sampling metrics, that settled each
retention's tombstones and recorded the batches after it as the rules count them; G2 takes the largest, over a
candidate's repetitions, of the retention plus the slowness after it, within the stall budget, or, on an architecture
at a window where no layout of the reference set fits, within the factor of the best (a finding says so, and that
the store must retain asynchronously before it ingests real data); G3 takes the median 99th-percentile commit over
the repetitions against the best median of the reference set, not decided where L/off's own commit varies over its
repetitions by more than a factor of 1.5; G4 the median bytes per record. The reference set is L/off and the
checkpoint policy G1 chose, plus M/crdb1 if its median commit at the 7-day window is more than 10% below L/off's on
either architecture, decided from `-join-from`, whose builds must be of the same family and pins and of the same
plan; a gate passes only if it passes on both architectures. A timing is compared only within one CPU model, or
pooled over models when every candidate compared has at least three repetitions, and is otherwise not comparable.
The first batch after a retention, the retention alone and the settling time are printed as diagnostics that do not
decide. A line is labelled `CI timing` only when every precondition holds (one revision, one binary per
architecture, the revision checked, the plan and the counters runs present, three repetitions, comparable
hardware); otherwise it says `not judged:` and why. A plan made under earlier rules is refused, and builds that did
not settle a retention's tombstones or did not record the batches after one are shown and not judged. The command reads files,
runs git only with `-git`, never touches the network and writes nothing but `-json`.

Builds may run under a Go memory limit (the workflow's `go_mem_limit` input, applied to the builds and not to the
plans), which is recorded in the manifest as `go_memory_limit` and in `job.json`; the judge and the report refuse a mix of builds under different limits.

### Adding a candidate

Implement `engine.Engine` and, in the candidate's own package, run the
conformance test against it:

```go
func TestConforms(t *testing.T) {
	conformance.Run(t, func(dir string) (engine.Engine, error) { return open(dir) })
}
```

The function opens `dir`, which is empty or holds what the same candidate left
when it was closed; it must come back exactly as it was (records, token and
retention horizon), because `Run` closes and reopens it along the way. An engine
with no files, or one that is not safe for concurrent use (a test double), runs
`conformance.RunSerial` instead, which leaves out the reopen and concurrent-read
checks. `oracle.OpenDurable` is the oracle with a file under `dir`: a reference
that persists, and what the harness tests itself against.

An engine may also implement the optional interfaces in `engine/hooks.go`:
`Settler` (flush and compact, which the conformance test calls between rounds so
reads are checked against files and not only memory), `LayerSizer` and
`Breakdowner`, and take an `engine.Recorder` at construction to report counts and
samples (block bytes read, replay length, checkpoint hits) without changing its
code for each measurement.

`Run` runs the reopen and concurrent-read checks one at a time first, and then the
workloads and the other scripted checks as parallel subtests (each opens its own
engine from the factory, so a factory must not share state between engines);
`RunSerial` keeps them serial. Because Go starts a parallel subtest only after the
function that created it returns, a caller must not `defer` teardown after `Run`:
it would run before the checks (`t.Cleanup` is fine).

The contract the test enforces, beyond answering reads like the oracle:

- Every read takes a `Scope`: one layer, and a snapshot token (`AsOf`) that makes
  it see only the records with `Seq <= AsOf` (`engine.Latest` sees everything).
  The records are never overwritten: `Window` returns every stored record,
  including those a later record of the same producer at the same instant
  overwrites in effect, because a query pinned to an earlier token sees them.
- `NeighborsBatch` answers many fingerprints at once, in any order, repeats
  allowed, results parallel to the input, from one snapshot.
- Reads may run concurrently with each other and with `Write`; `Write` and
  `Retain` are serialized by the caller. A read sees whole committed batches.
  `LastSeq` is the current token and moves only after a batch is visible.
- Any instant may be asked about, however far outside 1970 to 2262.
- `Write` takes records in strictly ascending `Seq`, above everything already
  written, and refuses a batch whole, storing nothing, if any record is invalid
  (`engine.Record.Validate`) or older than the retention horizon
  (`engine.ErrBeforeHorizon`).
- Every record of one subject carries the same layer. That is the caller's
  precondition; the oracle rejects a violation, engines are not required to.
- `Retain(horizon)`, called when the highest committed Seq is `L`, may discard
  history strictly before the horizon but must leave every answer for an instant
  at or after it and a token at or above `L` unchanged, `Window` included: every
  record at or after the horizon stays. Older answers are unspecified. The
  horizon only moves forward.
- Event times, and the deadline of a record with a TTL, must fit an int64 of Unix
  nanoseconds (1970 to 2262); a real store rejects what does not.
- The candidate must not keep or alter the slices it is handed.

Reads running alongside writes must each see one committed state:
`CheckConcurrentReads` has a writer commit batches and retain while readers read
as of the latest token, and every answer must equal the oracle's at some batch
boundary between the engine's token before the read and after it. An answer that
mixes the world before a batch with the world after it, or that sees a batch the
token had not reached, fails. (Moving pods between nodes in every batch is what
makes a torn read likely to show.) `CheckReopen` closes and reopens the engine
three times along a workload with retentions between, and compares every answer
to the oracle's after each.

After a retention the workload generator is told the horizon (`SetHorizon`): a
heartbeating run that began before it is continued by a new run at the refresh's
own event time, not extended at its start. Extending at the start is what the
coalescer did, and a store that has retained past that start refuses it, so every
long-lived heartbeat would silently die at its deadline. Where the extension
interval had absorbed refreshes that the old run's stored deadline does not reach
past, the new run is preceded by a record that carries the old one to it (the same
for a run that is replaced because its description changed), so a retention does
not split an interval of existence that the refreshes did not split. A run that
ends in a real silence still ends at its stored deadline, early by less than the
extension interval.

Beyond the random workloads, `Run` runs scripted checks on a fresh engine:
`CheckInstant` (several records of one producer at one instant, every token
between them, then a retention at that very instant; sequence numbers that
cross 2^32), `CheckProducers` (a reference belongs to the producer that made it:
one producer's delete does not end another's), `CheckRelations` (two relations
between one pair of entities are two edges, and a self-loop is read from both
sides; no random workload builds either), `CheckCheckpoint` (for an engine that
implements `engine.Checkpointer`: it writes checkpoints at instants of its own
choosing, which a workload on whole seconds never reaches, and probes records a
nanosecond before, at and after them, what a checkpoint depends on, a checkpoint
built on another, and a retention exactly at one), `CheckExtremes` (records,
deadlines and runs at both ends of the time range), `CheckReadContract` and
`CheckWriteContract`.

The random workloads include a second producer that confirms pod placements
(`ConfirmProbability`), sequence numbers that start just below 2^32 and 2^63
(`FirstSeq`), late records and retention.

### Test tiers

Pebble turns its own invariant checks on with the build tags `invariants` or `race`,
and it is the race detector, not the checks, that makes the tests slow (about 20 s
with the checks and 190 s with the race detector, on 10 cores). So there are two
tiers, and `mise run ci` runs both:

- `bench:test` is the full matrix with the checks on and cgo off (how `toposhift` is
  built): every workload and retention schedule, the random workloads, the reopen
  checks from real files. A guard test fails it if the checks are not compiled in.
  It sets `TOPOSHIFT_RAPID_CHECKS` to 40; a `-rapid.checks` on the command line wins.
- `bench:test:race` is `go test -race -short`: the tests that start goroutines (the
  concurrent-read check, which is the only goroutine code outside tests), the scripted
  and unit tests, and one workload per candidate. The concurrent-read check runs on
  the variants whose code it reaches differently: layout M's primary and
  `crdb1+filter` (the time filter off and on, which is what its own code branches on;
  the key schema is chosen inside Pebble) and layout L's primary (checkpoints on) and
  its piecewise-retention test (checkpoints off). The other variants run it in
  `bench:test` and, under the race detector, in `bench:test:deep`; leaving them out
  here cuts this tier by about two fifths (about 65 s to 37 s and a third less CPU time, with
  `GOMAXPROCS=4` on 10 cores). `conformance.Trimmed` and
  `conformance.SkipWhenTrimmed` are the one place that decides what `-short` drops, and
  a test that starts goroutines must not call the latter.
- `bench:test:deep` is the whole matrix under the race detector, in random order and
  never reused from an earlier run. It runs on every push to `main` and nightly,
  through `mise run ci:deep`.

CI runs `mise run ci` as four jobs on each architecture: `ci:static` (formatting,
lint, spelling, vulnerabilities, workflows, secrets), `ci:bench` (`bench:test`,
`bench:purego`), `ci:race` (the root module's tests under the race detector) and
`ci:benchrace` (`bench:test:race`). The required checks `ci (ubuntu-24.04)` and
`ci (ubuntu-24.04-arm)` pass only when all eight passed.

Without the environment variable the packages run `rapid` at their own defaults: 25 in
the conformance package, whose tests run the oracle and about fifty deliberately
broken engines, and 4 in the Pebble layouts, where each workload costs seconds under
the race detector; a deeper run is `go test ./spike/pebblemvcc -rapid.checks=100`. The
broken engines are the evidence that each check can fail: every one must be caught
by the check written for it, and by the random workloads too unless it says only a
scripted check can reach it. An honest engine that really discards history
(keeping everything at or after the horizon, and for each producer's reference
the newest record before it if still live) must pass.

Layout L's checkpoints are tested at the boundaries a whole-second workload never
reaches: variants with a lag of one nanosecond (the checkpoint instant lands on a
record's) and of whole seconds, a checkpoint after every write, and a scripted
check that writes them at chosen instants. Checkpoints are never pruned in this
spike (only a late record or a retention deletes one), so a prefix's checkpoints grow with the
policy's spacing; choosing the spacing and pruning is a measurement question.

Results written under `bench/results/` are git-ignored. Numbers measured on a
laptop or a VM choose between designs and are never published; published
performance numbers come only from CI hardware.
