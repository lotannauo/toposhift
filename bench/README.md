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
  and the liveness rule applied to the versions a read finds.
- `spike/pebblemvcc`: layout M, per-edge MVCC versions on Pebble's `cockroachkvs`
  (see its package documentation for the key layout, the read and the retention).
- `spike/candidates`: the list of variants a measurement runs, each with the name it goes
  by in results (`M/crdb1`, `M/crdb1+filter`, `M/default`, `M/default+filter`, `L/off`,
  `L/k64a4`, and any other checkpoint policy as `L/k<K>a<alpha>[l<lag>]`), so that the
  variant measured is the variant checked: every one of them passes the conformance
  checks at the benchmark settings, and a name is checked to be what it says in the
  tables it writes.
- `spike/pebblelog`: layout L, a log per entity, direction and layer in Pebble's
  bytewise key order, newest first, with the retention baseline (see its package
  documentation). With checkpoints off a read replays the history older than the
  instant; with them on, interleaved checkpoints let it stop early.

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
- **`Quiescer`**: `Quiesce` waits until nothing is flushing or compacting, the estimates
  of every table's garbage are collected, no file is marked for compaction (unless
  automatic compactions are off) and all of that has held for a second; `CompactAll`
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
and comes later, from CI hardware. Two counters are known not to repeat and are never
decided on: the bytes a read was served from the block cache, and, for the batched reads
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
| `build -candidate C -out D` | Writes the planned stream to `C` in batches of the planned size, dropping the records before the horizon as a store would refuse them, moving the horizon as planned, and checks the token after every batch. It compacts everything and records what the database holds in `D/<C>/manifest.json`. A build that does not come to the planned stream, fails a write or a retention, or counts a failed checkpoint writes no manifest, so it cannot be read; its directory is left as it was and has to be removed before the candidate is built again. |
| `read -candidate C -out D` | Opens the built database in a new process, with the compactions held still and the same absolute block cache for every candidate, refuses one that did any work when opened, asks every query twice, compares each answer with the reference engine's, and writes the cost of each query to `D/<C>/results.json`. A candidate that flushes or compacts while being read is refused, and one that answers anything differently from the reference engine fails the step (its results are written all the same). |
| `report -out D` | Sets the candidates side by side (all that have been read, or the ones named), and fails if they cannot be compared; the full tables are always in `D/report.txt`. The first lines say whether the results can be compared at all: one plan, one stream, one binary, the same Pebble options (apart from the comparer, key schema and collectors, which are what a candidate is), every answer the reference engine's. If not, it says so first. |

What is asked: forward and reverse neighbors and `Alive` as of now, an hour back, a day
back, and an old snapshot (read at the instant of its token, with only what it saw); windows of the last hour and the last day; and a batch of
the chosen prefixes together. The queries are chosen from the stream, not from any
candidate, and are asked of live prefixes only. Reads that would be empty by
construction are left out: a refreshed edge is one run, and all its refreshes carry the
instant it started, so the window of a heartbeat prefix is empty for an old run and measures the restart for a new one.

The rules the counters will be read against (the gates, the slope that separates bounded
from linear growth, the factors) are in `runner.Rules`, with their digest pinned in
`runner/stream_test.go` next to the digests of each preset's spec; a change to either is
made on purpose. The values nobody has chosen yet are listed in `Rules.Placeholders`; setting one
changes the rules' digest and nothing built or read, because the rules are applied to the counters
afterwards, so no plan is stale for it.

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
- `block bytes` counts every block load, a cached block too, and again at every seek that reloads it. Two key schemas of M made the same seeks and steps and loaded block bytes two orders of magnitude apart for the same reads, so it measures how the iterator is repositioned as much as the data a read needs. A count of distinct blocks is the number to add before a rule rests on it.
- `steps` counts the calls the layout made. A skip with `NextPrefix` is one step in the layout and many underneath, in a counter nobody exposes.
- Everything is read after `CompactAll`, which leaves a few large tables: range deletions, tombstones and L0 are not in the picture, and an iterator crosses fewer levels than in a running store.
- One seed and one retention: how a read grows with retained history is not in a single build. The default spec retains once, half a day before the end, and a refreshed prefix's current run is as old as the last retention, so what L pays on one is the run's extensions since then (and of every run that began after the horizon). The prefixes asked about are ranked by the records a store still holds after the final retention, not by the whole stream: on a heartbeat prefix the busiest over the whole stream is the one with an unbroken run, which a retention folds into the baseline. The sweep over retained windows is a separate build per window.
- The workload is synthetic and every default of it is provisional until the Alibaba and kwok replays exist.

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

CI runs `mise run ci` as three jobs on each architecture: `ci:static` (formatting,
lint, spelling, vulnerabilities, workflows, secrets), `ci:bench` (`bench:test`,
`bench:purego`) and `ci:race` (the root tests and `bench:test:race`). The required
checks `ci (ubuntu-24.04)` and `ci (ubuntu-24.04-arm)` pass only when all six passed.

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
