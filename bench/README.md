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
mise run bench:test   # tests with the race detector
mise run bench:lint   # lint
mise run ci           # includes the bench tasks
```

## spike

- `spike/engine`: the narrow interface a storage candidate implements, and the
  record it stores. Not the product's `Store`.
- `spike/workload`: a deterministic generator of churn (late records, same-second
  ties, outages, heartbeat runs, incompressible payloads).
- `spike/oracle`: the reference engine, built on the lifecycle specification.
- `spike/conformance`: the conformance test every candidate must pass, which
  checks a candidate against the oracle on generated workloads.

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
one producer's delete does not end another's), `CheckExtremes` (records,
deadlines and runs at both ends of the time range), `CheckReadContract` and
`CheckWriteContract`.

The random workloads include a second producer that confirms pod placements
(`ConfirmProbability`), sequence numbers that start just below 2^32 and 2^63
(`FirstSeq`), late records and retention. The conformance package's own tests
run `rapid` for 25 checks (a `-rapid.checks` on the command line wins), because
they run it against the oracle and about fifty deliberately broken engines under
the race detector; a candidate's own package gets rapid's default of 100. The
broken engines are the evidence that each check can fail: every one must be caught
by the check written for it, and by the random workloads too unless it says only a
scripted check can reach it. An honest engine that really discards history
(keeping everything at or after the horizon, and for each producer's reference
the newest record before it if still live) must pass.

Results written under `bench/results/` are git-ignored. Numbers measured on a
laptop or a VM choose between designs and are never published; published
performance numbers come only from CI hardware.
