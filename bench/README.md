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

- `Write` takes records in strictly ascending `Seq`, above everything already
  written, and refuses a batch whole, storing nothing, if any record is invalid
  (`engine.Record.Validate`) or older than the retention horizon
  (`engine.ErrBeforeHorizon`).
- `Retain(horizon)` may discard history before the horizon but must leave every
  answer at or after it unchanged. The horizon only moves forward.
- Event times, and the deadline of a record with a TTL, must fit an int64 of Unix
  nanoseconds (1970 to 2262); a real store rejects what does not.
- The candidate must not keep or alter the slices it is handed.

Results written under `bench/results/` are git-ignored. Numbers measured on a
laptop or a VM choose between designs and are never published; published
performance numbers come only from CI hardware.
