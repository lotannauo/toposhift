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
they run it against the oracle and about forty deliberately broken engines under
the race detector; a candidate's own package gets rapid's default of 100. The
broken engines are the evidence that each check can fail: every one must be caught
by the check written for it, and by the random workloads too unless it says only a
scripted check can reach it. An honest engine that really discards history
(keeping everything at or after the horizon, and for each producer's reference
the newest record before it if still live) must pass.

Results written under `bench/results/` are git-ignored. Numbers measured on a
laptop or a VM choose between designs and are never published; published
performance numbers come only from CI hardware.
