# bench

A separate Go module for everything that must not be linked into `toposhift`:
the storage-engine spike now, and later the baselines (RocksDB, Dolt,
SurrealDB, XTDB) and the benchmark drivers.

It is a separate module because it carries a `replace` of the root module to
`../`, which the root `go.mod` must never have (a `replace` breaks
`go install github.com/lotannauo/toposhift/cmd/toposhift@latest`). Import-path
based `internal/` visibility still lets it use the root module's `internal/`
packages. There is no committed `go.work`.

The RocksDB baseline, `rocksdb/`, is a module of its own inside this one, because it
is built with cgo and a native RocksDB that this module's cgo-free builds and tests
must never see (`./...` here stops at its `go.mod`). It is layout L on RocksDB:
`rocksdb/rockslog` is `spike/pebblelog`'s code with each call it makes of Pebble made
through `rocksdb/rockskv`, which gives RocksDB Pebble's shapes, so the keys and values
are layout L's byte for byte. Its tests hold it to layout L's frozen store digests, to
the conformance harness and to layout L's own counters on every query of a plan, and
`rocksdb/cmd/rocksbench build` and `read` measure it on a plan made by `spikebench
plan`. Its tasks (`mise run bench:rocksdb:test`, `bench:rocksdb:test:race`,
`bench:rocksdb:lint`, `bench:rocksdb:vuln`) need RocksDB installed: the pinned
Homebrew library on a Mac, or `ROCKSDB_PREFIX`, `CGO_CFLAGS` and `CGO_LDFLAGS` for one
built elsewhere. Only its formatting check needs neither, and it runs in the static
shard.

The Postgres baseline, `postgres/`, is a module of its own as well, to keep a database
driver and a server to test against out of this module's graph. It is pure Go (build
and test it with `CGO_ENABLED=0`). It asks whether a Postgres 17 table of interval
ranges with GiST indexes answers the spike's reads as the reference engine does.
`postgres/pgmodel` needs no server: the planner that turns records into interval rows
(a re-fold of a subject from the start of the interval a late record lands in, with
retention baselines) and a `Model`, an in-memory engine with the SQL's read semantics,
which the spike's conformance test checks against the reference engine.
`postgres/pgschema` is the DDL as text, `postgres/pginput` reads an activity file as
engine records, and `postgres/g0` asks an engine every question of a plan and compares
the answers with the plan's (`go run ./cmd/g0model -plan plan.json`, from `postgres/`).
`postgres/pgengine` is an engine over a Postgres 17 server (pgx): the same planner's
row changes applied with set-based SQL, the reads with range types, GiST (index set G)
or a btree on the interval's end (set B), and the EXPLAIN counter parser and the check
that a server has the settings of `postgres/scripts/settings.conf`. `mise run
bench:postgres:test` runs the module's tests; the ones that need a server skip without
`TOPOSHIFT_PG_DSN`. To run them, start the server the settings are written for (a
container, which this script starts on a laptop and on CI alike) and stop it when done,
from `bench/`:

```sh
postgres/scripts/pg-server.sh start     # the pinned image, 127.0.0.1:55432, no password
TOPOSHIFT_PG_DSN='host=127.0.0.1 port=55432 user=postgres dbname=postgres sslmode=disable' \
  mise run bench:postgres:test:server   # the engine's tests, the server's included
postgres/scripts/pg-server.sh psql      # a psql in the container
postgres/scripts/pg-server.sh stop
```

The data is under `~/spikebench/pg/17` unless `TOPOSHIFT_PG_DATA` says otherwise, and a
Unix socket replaces the port when `TOPOSHIFT_PG_SOCKET_DIR` is set, as on CI
(`.github/workflows/postgres.yml`, which is not a required check).

The metrics-store baseline, `victoria/`, is a module of its own as well and shares nothing
with the Postgres one (pure Go: build and test it with `CGO_ENABLED=0`). It asks what a
PromQL-compatible metrics store (VictoriaMetrics single-node) holds and answers when the
spike's streams are written to it as samples, the way kube-state-metrics' placement series
are, and says what it cannot answer. Nothing in it needs a server, an image or a new third-party
module yet. `victoria/vmmodel` is the series model (`topo_entity`, `topo_edge`), the
converter from the every-sample stream (the generator with `CoalesceRuns` off) to samples
and staleness markers with its deterministic counts, the registered table of producers and
the window each is read with, the reference's existence intervals and the classification of
an instant near a change as a boundary instant (counted, never compared), the capability
table (a question the store cannot express is a row there, never a loss and never timed;
a question whose only holder asserts once is tallied apart as not modelled yet),
and the check that the coalesced and the every-sample stream describe the same existence.
`victoria/vmwrite` encodes remote-write requests by hand, `victoria/vmquery` rewrites the
spike's reads as instant queries with one term per producer, reads the answers as an
engine returns them and has a model of the server for those queries, against which a
plan's answers are checked without a server. Everything it says about the server's own
behaviour is marked as an unverified assumption in `vmmodel`'s documentation. Run its tests
with `cd victoria && CGO_ENABLED=0 go test ./... -p 2`; `mise run bench:victoria:test`,
`bench:victoria:test:race`, `bench:victoria:lint`, `bench:victoria:vuln` and
`bench:victoria:purego` are its tasks, in the CI shards.

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

## eval

- `eval/blastscore`: scores the blast-radius walk and the co-residency queries against
  labelled fault cases, with baselines (every entity, one hop, the union graph) and an
  acceptance rule fixed before anything is measured. It reads a store only, parses no
  dataset, and its results hold pooled numbers and numbers keyed by case id, never an
  entity name. Its package documentation records the schema of the labelled scenarios'
  files and the rule itself. These are accuracy numbers: it reports no timing.
- `eval/factload`: writes one scenario's facts into a store. Pods, nodes and containers
  go through the product's Kubernetes objects translator and the resume step. The
  product's service writers (`internal/ingest/svcgraph`, `internal/ingest/spancount`) read
  the service graph and span count connectors' metrics; the dataset holds spans and none of
  those metrics, so this loader pairs the spans itself, as the service graph connector
  does, with a TTL of its own (an evaluation parameter; the product's expiry is
  configuration). Feeding those writers with connector output computed from the spans is
  not done here.
- `eval/itbench`: reads ITBench-Lite (IBM Research, Apache-2.0, revision d0916b0) and
  turns each labelled scenario into a case and its facts. It reads the fetched files and
  copies nothing from them. Its one added dependency is `go.yaml.in/yaml/v3` (Apache-2.0
  and MIT, pure Go), which reads the labels.
- `cmd/evalblast`: runs the evaluation in stages with no default: `-stage truth`
  prints the table made from the labels alone, `-stage map` the table made from the
  Kubernetes objects, `-stage pool` the set of service names with a span in the trace data
  read, and `-stage score` scores. The first three print a sha256, and score
  needs the sha256 of both tables and of the pooled set, and the probe file of the traces
  exports' coverage with its sha256. All its output is written outside the repository;
  everything but the results file names entities of the dataset.

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
and comes from CI hardware (`read -timed`, below): the counters choose, and a timing only confirms. Three counters are known not to repeat and are never
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
longer payloads), `-producer-len 18` (every producer name padded to exactly that many bytes, 14 to 64, by
a "/" and filler after the name, characters that do not repeat within the name so that block compression does not hide the cost, so the stream is the same records with longer producer strings; it makes
another scenario, so `pins` and `plan` must be given the same value), `-events-per-second` and `-cache-mb` (the block cache is the same absolute size in every
run, 64 MiB unless given). A checkpoint candidate is named by its policy, any of them: `L/k64a4l1h30m` has a
checkpoint 1 h 30 min behind the newest record of its prefix, which with `-run-max-age 1h` and an extension
interval of 30 min is behind every run that can still be extended (a lag is written without the units that
are zero at its end).

A name may end in `r<N>` after the lag (`L/k64a2l1nsr4`, `L/k64a4l1h30mr4`; there is no `r0`, and the root store's `Lroot`
has none): a write that deletes checkpoints in a prefix, because a record arrives with an event time before them, then
puts back the newest N of them in each prefix, each at its own instant and in the write's own checkpoint commit, up to 32
in a write (`checkpoint.replaced` counts them, `checkpoint.replace_capped` the ones over either limit, and their bytes are
in `checkpoint.bytes_written` and not in `checkpoint.written`). The policy's spacing does not move: a put-back checkpoint is
not the last one written. A reader pinned to a token below the late record cannot use a put-back checkpoint and walks to an
older one, as it would after the deletion. The variant is a prototype of the spike's layout L: `Lookup` finds it, no default
set or sweep holds it, and every earlier name and every byte stored without it are unchanged. An `-untimed` build of layout L
also writes `tallies.json` beside `manifest.json` (`{"<prefix in hex>": [invalidated, replaced, capped], ...}`, only the
prefixes with a count above zero; the manifest keeps its shape). `spikebench gaps -out D -candidate C [-window 30]` opens a
built candidate read-only (`-out` is a window's directory, or the directory that holds `R<days>`) and, for each prefix of
the plan's `node<- hot-records neighbors` group, prints its checkpoints, the gaps between consecutive checkpoints in records
(median, 90th percentile by nearest rank, largest), the holes (gaps over 1.6 times the prefix's median, taking the upper
middle value of an even count; hole counts use the gaps between checkpoints only, and the earlier evidence's scan also counted the span down to the baseline, so the two are not comparable), for each 9 h and 1 d read the steps a model of the read walks and the gap it sits in as
a multiple of the median, and, if there is a tallies file, the prefix's counts; it also writes `gaps.json` beside the
manifest (`-json` says where else). The model counts the iterator's steps exactly on a canonical build (`-canonical-layout`);
on another it is a lower bound, and the tool says so. With a results file the model's steps are printed next to the read's
`internal_steps`. `g1 -out` judges every candidate under the directory, so the verdicts of the arms being judged are to be
read from a `g1` run made before an extra arm (such as one with a lag) is added to the directory, or that arm's line is
ignored.

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

`producercost` measures, on a local capture of the Kubernetes objects receiver, what a longer producer name
costs a store: it translates the capture with the real translator, coalesces it and writes it to the real
store with no record, with producer names of 18 bytes (the baseline) and with each length of `-lengths` (a comma
list from 14 to 64, not 18; default `32,64`: 64 is the longest the rule allows and not a typical name, so the
other lengths show the curve), compacts each to the bottom level and prints the record counts at each step,
the live table bytes, the bytes written and, for each length, the cost
`(live_N - live18) / (live18 - liveEmpty)`
(`go run ./cmd/producercost -capture <dir with run1.jsonl and run2.jsonl>`). The 2026-10-12 capture has 2,535
log records; the translator makes 656 records of them and the coalescer (5-minute pull setting) writes 615.
Its numbers are local and informational. It writes one small store, so its cost moves by about half a
percentage point with the exact bytes of the filler; the decision takes it and the bench measurement at scale
(R2, R7) together.

`streamstats` prints three models of what a deadline index would cost: every extension rewriting its
entry, and an index bucketed by the hour and by the day, which is written only when a run begins, lapses
or moves its deadline into another bucket.

`alibabareduce` reduces one hour of the Alibaba cluster trace v2022 (the `MSMetricsUpdate` and `CallGraph`
archives) to activity files, tables and a report: `go run ./cmd/alibabareduce -in ~/datasets/alibaba-v2022 -out
~/spikebench/alibaba-reduce/<date>`. It streams the archives with the standard library, counts every skipped row by
reason, and refuses an output directory inside a git work tree. The trace, and everything derived from it, stays
outside the repository; `cmd/alibabareduce/testdata` holds synthetic rows and `check.sql`, an independent check for
the DuckDB command line (never linked, never run in CI). Its numbers are local and informational.

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

#### Exploratory flags: -extend-fraction and -compression

Two flags let an exploratory run vary one setting of the product, so that its counts can be reproduced from a
commit. Neither changes a default: a run that names neither writes what it wrote before them.

`alibabareduce -extend-fraction F` sets the share of a placement run's TTL after which the coalescer re-asserts
the run, in (0, 1]; the default is 0.5, the product's. It applies to the C60 and C300 streams; the call edges and the fixture keep the product's coalescer. The sharded flush stands in
for the product's whole coalescer only where the extension interval and the rest of the TTL both exceed a flush
lead and a second; for a fraction and a TTL where it does not, that cadence runs through one whole coalescer, and
`-whole-coalescer` asks for that at any fraction (it writes the same stream with more memory). `-recency` counts,
at each sample, how many sampling intervals the stored end of the subject's run is behind it. The manifest's
`coalescer` is the configuration C60 and C300 ran with, `product_coalescer` the product's, each output's
`coalescer_flush` says whether its coalescer was split or whole, and each written output's `content_digest` is
the digest in its file's footer, so repeats are compared by it and not by the file's bytes.

`activitybytes -compression default|snappy|zstd|balanced` sets the block compression of the store's tables
(`pebblekv.Tuning.Compression`); the default is `default`, which sets nothing, and its row and result are those
of a run without the flag. Under any other value the result file names the compression. The store holds the same
keys and values under each; only the table bytes differ.

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

#### Timed reads on CI hardware: read -timed and readtiming

`read` counts what a query costs; `read -timed` also times it, after everything `read` does, and writes a
second file, `timings.json`, beside `results.json`. `results.json` is complete, and written, before the first timed
ask, and is the same file a read without `-timed` writes. Local timings have no use, so `run` and `windows` never
pass `-timed`, and the command prints counts only (`read L/off: 869 queries timed, 3 warm-ups and 31 timed asks each,
2 not warm`): no duration reaches a log, so nobody sees a figure before the registered reading.

What is timed is the engine's call for one query (`engine.Engine`'s `Neighbors`, `NeighborsBatch`, `Alive` or
`Window`), on a monotonic clock, and nothing else: the answer is hashed after the clock is read, and the recorder that
collects the engine's counters is muted for the whole timing, so a counter costs one atomic load and not a lock and a
map write. The engine's own counter reporting (the iterator's statistics) still runs inside the interval. The
collector is left as it is (`GOGC` is not changed, and the judge refuses a job that set it); the
collections that ran during a query's timed asks are counted in `timings.json`. Per query there are `-timed-warmup`
untimed asks (3) and then `-timed-reps` timed ones (31, odd, at least 3), one reader, in the plan's order, on the
open store after the cold passes, so "warm" means warm for that query. A query whose timed asks moved the engine's
warm counter (for Pebble, the block cache's misses: `pebble.block_cache_misses`) is not warm and is left out of its
cell. The plan's block cache is the store's; a timed dispatch reads at the product's 256 MiB (`plan -cache-mb 256`,
and its pins made at 256 MiB too, since the pins name the scenario they were chosen from, which has the cache in it;
the pinned prefixes, the queries and their expected answers do not depend on it).

The loop is written against `engine.Engine`, not Pebble. To time another engine, implement `engine.Engine` and a
`runner.WarmProbe` (a counter that grows when a read is served from outside the memory the engine was given), call
`runner.TimeQueries`, write a `timings.json` and the bench workflow's `job.json`, and load them into
`[]runner.TimedRead` (`Results` may be nil: the answers are then checked against the plan's by each query's `Digest`);
the judge and its rules are the same. Version 1 of the judge loads only the spike's artifacts (it needs `manifest.json`
and `results.json`).

`spikebench readtiming` judges the artifacts of a run offline, on rules of its own whose text and digest are
printed on its first line (a constant in `runner/readtiming.go`, a test pins the digest; the spike's rules version 5
and its digest are not touched):

```sh
spikebench readtiming -commit <sha> -plans <run>/artifacts/bench-plans-<family>-<id> -base L/k64a2l5m -jobs 5 \
    -primary 'service<- hot-records neighbors@now' -json readtiming.json <run>/artifacts
```

It reports and decides nothing. A candidate's cell (a group of the plan at an age) is never one number: it is the
job values in microseconds to three significant figures with their minimum, median and maximum, the spread (the largest
job value over the smallest), and, against `-base`, the ratio of the medians with the bound from the smallest
over the base's largest to the largest over the base's smallest. A job's value in a cell is the statistic G1 uses
over its warm queries' medians (the largest for the hottest prefixes, the lower median for the median group); a
job with a query left out is partial in that cell, and is printed apart and not counted. The collections that ran during the timed asks are printed as a diagnostic column ("collections counted", per job and per query where there was one, and per query in the JSON) and enter nothing. Jobs on a CPU model that no reported scope of a cell holds are named under the cell, and an architecture's summary line says how many there are and on which models; neither changes the exit status. A duration under a
microsecond is printed `< 1 µs`, and no ratio or spread is formed from it. It refuses, with exit status 3 and nothing
judged, jobs of more than one commit, run or attempt, the same job twice, more than one binary per architecture, an untimed read, a block
cache other than 256 MiB, other asks than 3, 31 and 1, `GOGC` or a memory limit, any answer that differs from the
reference engine's and fewer jobs than `-jobs`; a cell that cannot be reported (fewer than three counted jobs, or CPU
models that cannot be compared) is named and ends it with status 2. It reads files only, runs no git and no network,
and writes nothing but `-json`. A timing is quoted only from its output on the artifacts of a CI run, with the run id.

#### The store's retention gate: storegate

The root store (`Lroot/off` and `Lroot/<policy>`) is judged apart from the spike's gates, by five limits fixed
before anything is measured (rules version 3), which `spikebench storegate` prints with a digest of their text on every report
(the text is a constant in `runner/storegate.go`, and a test pins its digest, so a change to a limit shows in the
diff). The command never reads `runner.Rules`: the spike's rules version and digest do not depend on it.

```sh
spikebench build -candidate Lroot/off         -out D/R7 -metrics-every 10s -retention-mode sync
spikebench build -candidate Lroot/k64a2l1ns   -out D/R7 -metrics-every 10s -retention-mode sync
spikebench build -candidate Lroot/k64a2l1ns   -out D/R7w -sync -metrics-every 10s -retention-mode background -pace 4500   # over the plan of a window
spikebench build -candidate Lroot/k64a2l1ns   -out D/R7t -sync -metrics-every 10s -retention-mode background -pace 4500 -retain-flush-dels 24576
spikebench storegate D/R7                     # or the build directories themselves, or a directory of CI artifacts
spikebench storegate -commit <commit> D/R7w   # paced builds in the background are judged only as the builds of one commit
```

A window is the time from a retention's publication to the end of its rewrite; a batch that overlaps one, its
first and last instants included, is inside it. A batch that overlaps a settle (from the end of a rewrite to the
end of its settle) and no window is settling: it is counted in neither the inside nor the outside, so it is
neither the control nor Q2's baseline, and its longest and 99th percentile are reported. Nothing is written
during a synchronous settle.

| Row | Limit |
| --- | --- |
| Q1 | The longest batch inside a window takes at most 250 ms. The longest outside every window and settle is printed beside it as the control; a build over 250 ms with a control above 150 ms, or with no control, is not judged (rerun it), and a control never prevents a pass. The largest `retain.max_prefix_records` and `retain.chunk_hold_ns` of the build and the sum of `retain.touch_rewrites` (recorded by a background build only) are reported with it. |
| Q2 | The 99th percentile of a batch's commit inside windows is at most the larger of twice the same outside windows and 50 ms. A histogram bucket is within an eighth of the value it holds, so the inside is read at the top of its bucket and the outside at the bottom of its: neither reading lets a batch pass that the true values would not (and a value within an eighth under the limit may be refused). |
| Q3 | With 30 days kept, the longest rewrite (publication to the end of the rewrite, not of the settle nor the return of the call) takes at most 2 h, at a load outside windows of at least 3000 records/s. The load is printed; a build below it, or one that keeps fewer than 30 days, shows its figure and is not judged. The tombstones after the settle are reported, not judged. |
| Q4 | The records of the batches inside windows over the windows' total time plus the time of those batches that lies outside the windows are at least 3000 records/s (a big batch over a short window is not a quick writer). |
| Q5 | The peak live heap (`/gc/heap/live:bytes`, the field `heap_live` of `metrics.jsonl`, so the build needs `-metrics-every`) is at most that of `Lroot/off` of the same plan, architecture and repetition plus 512 MB, and the two builds must record the same `go_gc`, `go_memory_limit` and `metrics_every` (the sampled peak depends on the interval), and the baseline must itself be a build the gate judges, or the row is not judged; a separate row holds the peak resident set of the build process to 14 GB. MB and GB are 10^6 and 10^9 bytes: reading them as MiB and GiB would relax the limits by 4.9% and 7.4%. |

A build records, in its manifest, the windows (with the end of the rewrite and of the settle and the instant
the call returned), the batches inside and outside them (count, records and a histogram of their commit),
the time the stream took and the time the builder waited for the database to rest, the history the last
retention kept, the peak resident set (`getrusage`, so it covers the compaction after the stream too), and `go_gc`
and `retention_mode` among its descriptions. The load outside windows is the records of the batches outside every
window over the time the stream took, less the rests and the time the builder was away in a retention that did not
return until it was done.

`-retention-mode` (on `build`, `run` and `windows`, passed to the `Lroot` candidates only) accepts `sync`, which is
what no value means, and `background`; anything else is refused. In synchronous mode the writer is blocked for the
whole of each window, so no batch is written inside one: Q1, Q2 and Q4 are then not judged, with the longest time the
writer was blocked in the reason, and Q3 and Q5 are judged, as in rules version 1. In background mode a retention
returns when its horizons are published and the build goes on writing while the store rewrites and then settles (a
flush and a compaction of the key ranges of the layers it moved, with no lock held). The store reports the end of
each phase of each retention to the build as it happens, so the windows are the store's own; a retention that a later
one replaces before it ends is marked replaced and ends at the later one's publication. The build waits for the last
retention, its settle included, before it reads, sizes or compacts anything.

**Rules version 2: a build in the background.** Q3 assumes the product calls Retain at most once a day per store;
its bound is one twelfth of that. A background build of a window of W days is judged on its paced part only:
`-pace 4500` holds the writer to 4,500 records/s on a fixed wall-clock schedule (open-loop: it writes without
sleeping when behind, never runs ahead) from stream day D = 2W + 1.5 - ceil(W/2) (11.5 for 7 days, 46.5 for 30), and
the days before are written unpaced. Only a build with `-retention-mode background` is paced, and its plan must be a
window plan (`-window`; `windows` makes one for each of its windows). Q1 to Q4 are judged on the windows published at
or after D; a batch that starts before D is unpaced, and one that overlaps an unjudged window or its settle is
excluded, and neither is inside, settling or outside; Q1's control, Q2's outside and Q3's load are the paced batches
only. A window a later retention replaced, or still open when the stream ended, ends there. Q3 takes the rewrite time
of each judged pass, or for a replaced or unfinished pass the least it took: it fails if any is over 2 h, is not
judged if some pass has only a lower bound, and otherwise passes; its load is over the paced writing time, from D to
the end of the stream, and Q4's time ends at the end of the stream. A build that is unpaced, or paced at another rate,
prints its values and is not judged; one paced from another day than D is refused. Q5 compares builds of the same
retention mode, sync setting, pacing rate, pacing start and retention flush threshold (rules version 3), and the repetitions of a candidate on an architecture
must share them, or the set is refused (status 3, naming the values); a baseline that differs leaves Q5 not judged.
The manifest records `pace_records_per_second` (0: unpaced) and `pace_from_day`, and the report prints, for each
build in the background, how it was paced, how its batches divided, the number of inside batches Q2 is taken over,
and for each pass the time of its rewrite (or its lower bound), of its settle, and whether the settle reached its
deadline, with the Q3 note of the rules. The spike's timing judge refuses a paced build.

**The retention flush threshold.** `-retain-flush-dels N` (on `build`, `run` and `windows`) sets the count of
unflushed range deletions at which the root store's retention flushes (`pebblestore.Options.RetainFlushDels`, a
field for measurements that the product never sets), for a measurement that compares thresholds. It is accepted only
with `-retention-mode background` and for the `Lroot` candidates, and refused before anything is built otherwise; 0,
the default, is the store's own (`pebblestore.DefaultRetainFlushDels`, 32,768, provisional), and a negative count is
refused. Every `Lroot` manifest records the threshold its store used as `retain_flush_dels`, the default's number
for 0; a manifest without the key was built before it was recorded and reads "not recorded", never the default. A
run or `windows` refuses to resume over a build at another threshold, the report and `windows` refuse a family of
two, the gate refuses a candidate's repetitions at two (or one recorded and one not) and leaves Q5 not judged against
a baseline at another, and the spike's timing judge refuses a build at any threshold but its binary's default.

**What the gate reports of the judged part.** Beside the whole build's figures, the report gives each build's
threshold and, apart, the figures of the part the gate judges (the passes published from the start of the pacing,
every pass of a build that was not paced, and the stream's time from then to its end) and of the rest: for
`retain.chunk_open_ns`, `retain.chunk_hold_ns` and `retain.chunk_prefixes` over the chunks of those passes, each
pass's last chunk left out (a pass a later retention replaced keeps it), the count, least, 50th, 90th and 99th
percentiles, largest and mean, and the exact share of chunks whose opening took more than 10 ms; the flushes started,
put off and refused; the write stalls, counted from the database's count at the start of the pacing to the end of the
stream; and the peak sublevels of level 0 over the metrics lines of that part, which can miss a peak between two
lines. For each retaining build in the background (not `Lroot/off`) it then prints the retention-chunk criteria C1 to
C5 below, each met or not, and whether the build meets them. None of this is a row of the gate or changes a verdict.

**Where the builds come from.** The repetitions of a candidate on an architecture must also come from one plan, and
either all from the bench workflow (with its `job.json`) or all built locally; the workflow's must come from one run
and from its first attempt (`run_attempt` 1), since re-running some of an arm's jobs is not a redispatch. Paced builds
in the background are judged only with `-commit <commit>`, the frozen commit: every build's manifest revision, and
its job's commit if it has a `job.json`, must be it. A local build has no job and is checked by its revision alone.
Any of these refusals ends `storegate` with status 3.

Repetitions of a candidate on an architecture are one row, the worst of those that could be judged, marked
`(partial)` when some could not be; a build that is untimed, not through the root store, without the gate's record or,
in a synchronous build, with a window that never ended is listed under `NOT JUDGED` with its reason. Only a build
through the root store records the gate's trace. It reads files and writes nothing.

Exit status of `storegate`:

| Status | Meaning |
| --- | --- |
| 0 | Nothing is over a limit, and every row the builds' mode has something to judge was judged. Rows that cannot be judged by construction (Q1, Q2 and Q4 of a synchronous retention) do not count against it. |
| 1 | A row is over its limit (this wins when rows are also not judged). |
| 2 | A row that the builds' mode should judge was not judged (synchronous: Q3 and both Q5 rows; background: all six; Q1 withheld by its control counts): a run in which nothing could be judged is never a pass. A local build that keeps fewer than 30 days, or has no heap samples, ends with 2 though its report is complete. |
| 3 | An error: no build was given, a flag is unknown, a build could not be read, the repetitions of a candidate differ in a setting or in where they came from, or the builds are not all of the commit `-commit` names (or paced builds in the background were given without it). Nothing is judged, and it is not taken for a verdict. |

##### Registered procedures (dated 2026-10-12)

These texts were registered on 2026-10-12, before any background build was judged on CI, and are given here as
registered. They sit outside the gate's rules text, so its digest does not cover them; the version-3 entry of the gate's rules log
in `runner/storegate.go` records them, with the incomplete-builds text, which they refer to. Commits are written
`<commit>`.

**Retention-chunk criteria.**

> **Retention-chunk criteria** (registered 2026-10-12, before any CI build of the retain-chunk lane). They are derived from the design: a chunk's clock charges its iterator's opening up to chunkTime/2. So C1 limits how often that cap binds, and C2 limits what a chunk holds when it does. A change of chunkTime (20 ms) or of `retainBytes` (4 MiB) reopens these criteria.
>
> **Where.** Judged only on CI hardware, on the 7-day background arm of the store gate's dispatch (sync=true, `-pace 4500` from day 11.5), for each retaining build (not the Lroot/off baselines).
>
> **The judged part.** It is the gate's own:
> - **chunks:** those of passes whose windows the gate judges (publication at or after D), with each pass's last chunk left out. A pass replaced by a later retention keeps its last chunk, which ended on time or bytes;
> - **stalls and deferrals:** those that begin between the start of pacing and the end of the stream;
> - **statistics:** taken over all such chunks of the build pooled, never per pass.
>
> **A build meets the criteria if:**
> - **C1 (opening).** At most 10% of its judged chunks have `retain.chunk_open_ns` > chunkTime/2 (10 ms). This is an exact count of chunks over the bound divided by the count of judged chunks, not a histogram quantile.
> - **C2 (hold).** The 99th percentile of `retain.chunk_hold_ns` over its judged chunks, read as the upper bound of its histogram bucket, is at most 1.5 × chunkTime (30 ms).
> - **C3 (progress).** The 50th percentile of `retain.chunk_prefixes` over its judged chunks, read as the lower bound of its bucket, is at least 100.
> - **C4 (stalls).** Its write stalls (Pebble's `WriteStallBegin`) in the judged part number 0. They are summed over every opening of the database in the build: for each opening, the count at the end of the stream (or at its close) less the count at the start of pacing (0 for an opening after it).
> - **C5 (flushes).** In the judged part, `retain.flush_deferred` = 0 and `retain.flush_errors` = 0.
> - **The gate's verdicts.** Its Q1, Q2 and Q4 each pass or are not judged.
>
> **Reported, not judged:**
> - for `chunk_open_ns`, `chunk_hold_ns` and `chunk_prefixes` over the judged chunks: count, min, p50, p90, p99, max and mean, with C1's exact fraction;
> - `retain.flushes`;
> - the peak L0 sublevels in the judged part, sampled per metrics line, so it can miss a peak between lines;
> - the same figures for the unjudged part (unpaced days and unjudged windows);
> - the number of passes and chunks judged.
>
> A build with no judged chunk does not meet C1–C3.

**The threshold ladder.**

> **Threshold ladder** (the one tuning of `retainFlushDels`; never repeated under this registration).
> - **The rungs.** `retainFlushDels` takes one of 32,768, 24,576 and 16,384, set per build through a bench-only input and recorded in its manifest. Builds of different rungs are never pooled into one verdict. Each rung is one 7-day background dispatch, as pre-registered for that arm: 3 repetitions per architecture, both architectures, the Lroot/off baselines included. All rungs run from the same frozen commit.
> - **The order.** The rungs are tried from the largest. A rung qualifies only if **every** retaining build of it, on every architecture, meets every criterion of Text 1. The first rung that qualifies is chosen; no smaller rung is dispatched. That is the largest qualifying rung.
> - **Incomplete builds.** A rung with an incomplete build is treated as an arm is under the incomplete-builds text (registered 2026-10-12): at most one whole redispatch of that rung, unchanged.
> - **The chosen value:**
>   - it becomes `defaultRetainFlushDels`, which is also the zero-config product default (`DefaultOptions` and `toposhift serve` use `BenchTuning`), and is frozen;
>   - its 7-day dispatch is the 7-day arm's verdict, and the other rungs' dispatches are reported beside it;
>   - the 30-day arm, and its dispatch condition, use that value and that 7-day dispatch only.
>   - If the 30-day arm fails any criterion of Text 1, the choice stops. There is no step down to a smaller rung: the next lever is registered anew in a new, logged version.
> - **If no rung qualifies:**
>   - nothing is chosen, and 32,768 stays the provisional default;
>   - the 7-day arm has not passed under this registration, and the pre-registered fallback applies (the release ships with retention off by default);
>   - the next lever is registered anew before any further dispatch. The named candidates, in order:
>     - (1) open and close an iterator outside the store's lock just before each background chunk and right after a flush starts, so the in-lock opening is cheap [unmeasured];
>     - (2) a shorter chunkTime, which also reopens Text 1.
>   - Pebble option changes (compaction concurrency, the L0 stop threshold) are deferred and are not candidates under this registration.
> - **The delay.** `BenchTuning.FlushDelayDeleteRange` is 0 in every rung. The `Tuning` field stays, defaulting to off.

("Text 1" is the retention-chunk criteria above.)

The ladder, step by step:

1. **Freeze.** The owner tags the frozen commit `gate-freeze-YYYYMMDD`, signed, and records the tag and its commit in
   the gate's log. A signed tag is annotated, so `git rev-parse gate-freeze-YYYYMMDD` gives the tag object, not the
   commit: `<commit>` below, recorded in the log and passed as `expected_sha` in the smoke and in every rung, is
   `git rev-parse gate-freeze-YYYYMMDD^{commit}`. The bench workflow accepts a dispatch from `refs/heads/main` or from a tag matching
   `^refs/tags/gate-freeze-[0-9]{8}$` whose commit is an ancestor of `main`, and checks that every job runs
   `expected_sha`.
2. **Smoke.** One dispatch from the tag with the command of step 3 (with `-f expected_sha=<commit>`) but
   `-f candidates=Lroot/off,Lroot/k64a2l1ns,Lroot/k64a2l5m -f reps=1` (6 jobs), at `retain_flush_dels=32768`, to prove
   the wiring: under the incomplete-builds text a failed build fails its arm. The tag came before the verdict of Rule L
   (the registered lag rule), so the smoke covers both lag candidates. Rule L passed: the default checkpoint lag is
   5 minutes, and the candidate of the dispatches equals the shipped default. The smoke is logged as "smoke, values
   seen", is never judged, and is excluded from every pool by its run ID.
3. **Dispatch the rung 32,768**, with the pre-registered values:

   ```sh
   gh workflow run bench.yml --ref gate-freeze-YYYYMMDD \
     -f family=rmae -f windows=7 -f candidates=Lroot/off,<candidate> \
     -f reps=3 -f arches=both -f pins_window=2 -f sync=true \
     -f metrics_every=10s -f go_mem_limit=11GiB -f max_parallel=12 \
     -f retention_mode=background -f retain_flush_dels=32768 \
     -f expected_sha=<commit>
   ```

   `<candidate>` is `Lroot/k64a2l5m`, which is the default lag since Rule L passed. Without `metrics_every`, Q5 has no
   live heap samples and the level 0 peak of the judged part cannot be read, so the workflow refuses a background
   dispatch with none. The pace (4,500 records/s) follows from the retention mode.
4. **Judge it offline** with `spikebench` built from `<commit>`: `spikebench storegate -commit <commit>` over the
   dispatch's artifacts. Read C1 to C5 and Q1, Q2 and Q4 for every retaining build.
5. **If every retaining build qualifies,** choose that rung and stop. Otherwise dispatch 24,576, then 16,384, with
   the same command and only `retain_flush_dels` changed.
6. **The chosen value** becomes `defaultRetainFlushDels` in a later change, after the 30-day arm, as the ladder says.
7. **Record** each dispatch (run ID, ref, commit, inputs, plan digests, outcome) in the gate's log before the next,
   and compare its plan digests with those of the local quiet builds.
8. **While a rung runs,** 12 of the account's 20 concurrent jobs are taken, so pull-request CI queues behind it.

**Reopen thresholds.**

> **Reopen thresholds.** Let T be the nominal wall time of a paced stream day: the plan's records at or after D ÷ 4,500 per second ÷ the number of stream days after D. The median wall time between consecutive publications of judged passes is reported beside it, as is the paced wall time against the paced records ÷ 4,500 per second. Let δ be the settle deadline the build ran with, which is the dispatch's. Reopen if any of these holds:
> (a) of the judged passes that have a settle (the report does not print "none"), more than half reached their deadline; the last pass counts by its printed flag;
> (b) any judged pass's rewrite time, or the lower bound of a judged pass that has only a lower bound, exceeds T/2;
> (c) any judged pass other than the last was overlapped by the next retention: its rewrite was replaced (the report shows "rewrite at least"), or its settle did not end by itself before the next publication (in the gate trace, its settle ends at the next publication and did not reach its deadline; in the report, its publication + rewrite + settle equals the next publication to within 2 ms).
> The criterion "the settles' total time exceeds 25% of the paced writing time" is withdrawn: a settle is capped at δ and there is one pass per stream day, so at δ = 120 s and T ≈ 555 s the share cannot exceed about 22%, which also caps the share of paced batches a settle removes from Q1 and Q2. T/2 leaves a runner about twice as slow as the local machine before its passes outgrow the stream day. Each judged pass's busy share except the last, (rewrite + settle) / time to the next publication, is reported for information and is not a threshold. Separately, if the unpaced part exceeds 1.5 h locally, this is reported as a coarse alarm and does not by itself block the dispatch; the dispatch condition decides. Every quiet local 30-day build run under this rules version is read for these thresholds, and any one that trips them reopens; a build voided by the pre-registered definition of quiet is recorded with the reason and not read. Logged: written after the void local run of 2026-10-11/12, whose values were seen.

Quiet, as registered: the local 30-day and 7-day builds run at normal priority on AC power, with nothing else of ours
running. A sampler records the foreign CPU once a minute through the whole build, unpaced and paced parts alike: the
host's busy CPU (the `CPU usage` line of `top` times the 10 cores), minus the CPU of the build's own process tree,
minus kernel_task's. Before each build, once nothing of ours has run for 5 minutes, it records 10 samples with
nothing of ours running; F0 is their median, and the limit is L = F0 + 1.0 core. A build is not started if
F0 + 1.0 + D exceeds 9 cores, where D is the 95th percentile of the build tree's CPU over the unpaced part of the
smoke build that checks this rule. A build is void if, from its fifth sample to its end, the mean of the last five
foreign-CPU samples exceeds L, or two samples are received more than 3 minutes apart. A void build is stopped,
recorded with its samples and not read. A smoke build over its limit gets one more smoke after a fix, and then the
runs stop.

**Dispatch condition for the 30-day arm.**

> **Dispatch condition for the 30-day arm (fixed before the 7-day jobs report).** Before the 7-day arm is dispatched, run a quiet local 30-day and a quiet local 7-day background `sync=true` build, and record for each its unpaced time and that time's two segments: from the start of the build to pass 1's publication, and from there to the start of pacing. Every such local build that is run is recorded, and the estimate is computed for every recorded pair of a local 30-day and a local 7-day build; the largest is used. For each architecture, take the 7-day arm's slowest retaining build (not an Lroot/off baseline), and estimate the 30-day build's step time as the sum of:
> - U, the larger of: its unpaced time × (local 30-day unpaced time ÷ local 7-day unpaced time); and the sum, over the two segments, of its segment time × (local 30-day segment time ÷ local 7-day segment time);
> - (the 30-day plan's records at or after D ÷ 4,500 per second) × the larger of: its paced wall time ÷ (its paced records ÷ 4,500 per second); and 4,500 ÷ R, where R is (the 30-day plan's records from the stream day of pass 1's retention to D ÷ the local 30-day build's second-segment time) × (the local 7-day build's second-segment time ÷ the slowest 7-day CI build's second-segment time); and 1;
> - 0.15 h for the step's tail.
>
> The local builds, both arms' dispatches and the plans are built from one commit on main after the retention-chunk fix. Record the estimate and every input. Dispatch only if every architecture's estimate is at most 5.0 h, with `arches` = both; otherwise do not dispatch under this rules version; an arm not dispatched has not passed, and the pre-registered fallback applies.

The single-commit order:

1. the retention-chunk change merges;
2. the change that gives the workflow its dispatch inputs merges;
3. that commit is frozen (tagged `gate-freeze-YYYYMMDD`);
4. the quiet local 30-day build and the quiet local 7-day build;
5. the 7-day dispatch;
6. the estimate;
7. the 30-day dispatch, on that commit.

The dispatches run from the tag at the frozen commit, so later merges to main do not affect them. A change that must be
included forces a re-freeze and new local builds.

#### Diagnostic builds on CI runners: diag

`.github/workflows/diag.yml` builds the profiling and table-shape diagnostics on
GitHub-hosted runners, so that they cost no time on a local machine. It is dispatched
by hand, needs no frozen tag and decides nothing: nothing it produces is a judged arm,
and each of its runs is left out of every pool by its run id. The judged arms stay on
`bench.yml`.

A dispatch builds one candidate in two to four configurations on the runners it names
(`ubuntu-24.04-arm`, `ubuntu-24.04`, `macos-15`), each configuration two or three times
on each runner. The first configuration is the control (A) and the rest are variants
(B), given as a JSON list of `{name, build_flags, plan_flags}`:

```sh
gh workflow run diag.yml --ref main -f label=p5 -f base_sha=<commit of main> \
  -f candidate=Lroot/k64a2l5m -f runners=ubuntu-24.04-arm \
  -f configs='[{"name":"control","build_flags":"-profile-windows","plan_flags":""},
               {"name":"cache2g","build_flags":"-profile-windows","plan_flags":"-cache-mb=2048"}]'
```

The other inputs default to the local command, `spikebench build -candidate
Lroot/k64a2l5m -sync -metrics-every 10s -retention-mode background -pace 4500`, on a
7-day window. Build flags are words from a list that `bench/scripts/diag-dispatch-check.sh`
checks, which a test runs with each input it must refuse: `-profile-windows`,
`-disable-delete-only`, `-lbase-target-mb=N`, `-target-sizes-mb=N,N,...` and
`-read-ahead=N`; plan flags are `-cache-mb=N`. A 30-day window is refused, since its
build may not fit the 6 hours of a job.

- **Reading a pair.** The repetitions of the control are the noise floor of a runner
  label (the A/A pair). A variant is read only where it differs from the control by more
  than the control's repetitions differ from each other (the A/B pair), only within one
  CPU model (the report table flags a label whose jobs ran on more than one), and only
  against a control on the same label. Absolute times, and the share of any step that
  waits for the disk or the scheduler, differ between a runner and a local machine in
  kind; ratios and counts are more stable. Never compare a runner's figure with a
  local one, or one runner label with another.
- **`GOMAXPROCS`.** Every build runs with `GOMAXPROCS=10`, set explicitly: Pebble cuts
  its block cache into 4 x `GOMAXPROCS` shards, so this keeps the 40 shards of a
  10-core machine. On a runner with 3 or 4 CPUs it means more runnable goroutines than
  CPUs, so a figure from a diag run is not comparable with one from `bench.yml`, whose
  builds run at the default. Each job also times 1,000 durable 4 KiB writes before it builds and records
  the 50th and 99th percentile.
- **The binary.** It is built from `base_sha`, a commit of main. A flag that is not on
  main yet is carried by a patch on that commit: `patch_b64` (the output of `git diff
  --binary --full-index <base> <commit>`, gzip-compressed and base64-encoded, under about
  60 KB) or a secret gist with that text in a file named `patch.b64`, with its
  `patch_sha256` and the `tree` the patched checkout must have. The run refuses unless
  it does, and `job.json` records the base, the tree, the patch and the binary's sha256.
  A build of a patch is dispatched from a branch named `verify`, which points at a
  commit of main and is never given a commit of its own: a patch is code, and code in a
  job can write to the cache of the ref it runs on, so its caches must be ones that
  nothing on main reads. Such a result is a diagnostic and nothing more. Before a
  registration cites a figure, the flag lands on main and `base_sha` alone names the
  binary.
- **Approval.** The setup job runs in the environment `diag-approval`, which has a
  required reviewer, so a dispatch starts only when it is approved in the Actions page.
  Nothing else of the run starts before that.
- **What a patched build can reach.** A patch is code, and a patched program runs before
  the job's later steps, so it can rewrite the scripts that collect and summarise its
  own output: the `job.json` and the summary of such a job are not evidence of anything
  but what the artifacts hold. Nothing secret is in reach: the job has no secret and a
  read-only token, and the checkout keeps no credential.
- **macOS.** `macos-15` has 3 CPUs and 7 GB of memory, so a dispatch that names it sets
  `macos_low_rss`, to confirm that its configurations stay under about 6 GB.
- **What it keeps.** The plans (generated in the run, one per distinct plan flags, with
  their sha256 in the job summary), and for each build `profiles/`, `index.json`,
  `metrics.jsonl`, `manifest.json`, `job.json`, the sync probe and the logs (never
  `db/`) for 30 days. The report job writes one table of the whole dispatch.

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

CI runs `mise run ci` as six jobs on each architecture: `ci:static` (formatting,
lint, spelling, vulnerabilities, workflows, secrets), `ci:bench` (`bench:test`,
`bench:purego`), `ci:raceheavy` and `ci:racerest` (the root module's tests under the
race detector, the slowest packages apart from the rest) and `ci:benchraceheavy` and
`ci:benchracerest` (`bench:test:race` the same way; `internal/raceshards` names each
module's slowest packages). The required checks `ci (ubuntu-24.04)` and
`ci (ubuntu-24.04-arm)` pass only when all twelve passed.

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
performance numbers come only from CI hardware. The deterministic counts the
findings report may quote are described in `findings/README.md`.

### Known failures of the CI tiers

Each entry says what is known and what is not, and is dated.

- **A concurrent read in `spike/pebblemvcc` matched no committed state once (2026-10-21).**
  `TestConforms/crdb1/concurrent_reads` failed in the `verify bench (ubuntu-24.04)` job of
  `verify` run 39549744035, attempt 1 (a patch on a commit of `main`): "a concurrent read
  matched no committed state" for a batched read of every node's pods, between two equal
  tokens. The same job rerun passed, and every other job of that attempt passed. The cause
  is not found: whether the reader of layout M returned a view that was never committed, or
  the check judged a read of an instant near a retention horizon wrongly, is not known.
  `pebblemvcc` is a candidate of this spike and is not in the product build. A second
  occurrence in any run is the reason to run the check many times under `-race` and decide
  between fixing the reader and fixing the check.
- **`deep (ubuntu-24.04)` failed on slow hosts because of the demo tests' fixed deadlines
  (2026-10-17 to 2026-10-21; fixed in #199).** `TestDemoNoServePrintsTheBanner`,
  `TestDemoEndToEnd` and `TestDemoOnATakenPort` in `cmd/toposhift` failed with `context
  deadline exceeded`: the demo builds its cluster in memory before it serves, and with
  every package running under the race detector at once on a 4-vCPU runner that took
  longer than the deadlines (28.37 s in nightly 39400829188). Three of the four failed runs
  read (39039943774, 39365679498, 39400829188) were on `AMD EPYC 7763` hosts; the fourth,
  push run 39559075805, was on an `AMD EPYC 9V74` host, so a slow host, not one CPU model,
  is the condition. #199 gives these tests four times the deadline under the race
  detector; what they assert is unchanged. The nightly 39576583196, which contains #199,
  passed this job on an `AMD EPYC 7763` host: one green run on the host class that failed
  most, not proof that it is closed. A `deep (ubuntu-24.04)` failure on a commit that
  contains #199 is a new failure to read, not a rerun.
- **`internal/coalesce` `TestWithNothingAbsorbedTheCoalescedStreamFoldsLikeTheRawOne`
  failed once in the `deep` job of push run 39636383439 (2026-10-21;
  `-rapid.seed=2047047424587645809`).** The cause was a counting error in the property,
  not in the coalescer: a byte-identical confirmation repeated at one instant was counted
  as absorbed. The property now counts such a repeat separately and still requires the
  coalesced stream to fold exactly like the raw one. The shipped coalescer and v0.1.0 are
  unaffected.
