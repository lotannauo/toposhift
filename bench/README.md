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
mise run ci            # includes the bench tasks
```

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
long-lived heartbeat would silently die at its deadline.

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
  and unit tests, and one workload per candidate. `conformance.Trimmed` and
  `conformance.SkipWhenTrimmed` are the one place that decides what `-short` drops, and
  a test that starts goroutines must not call the latter.
- `bench:test:deep` is the whole matrix under the race detector, in random order. It
  runs on every push to `main` and nightly, through `mise run ci:deep`.

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
