# Guidance for agents and contributors

toposhift is a temporal topology graph for infrastructure. Read `README.md` for the pitch and `docs/adr/` for the decisions that bind you. The design plan is maintained outside this repository; ADRs are the durable record.

## Commands

```sh
mise install        # install pinned tools
mise run ci         # everything CI runs
mise run fmt        # format Go code
mise run test       # go test -race
mise run hooks      # install git hooks
```

## Rules most likely to be broken by a locally sensible change

- Once an ADR exists for a decision, the ADR wins. When reality contradicts an ADR, write a new ADR that supersedes it. Never silently rewrite earlier reasoning.
- The fact log stores only producer-asserted facts and attributed store-authored events. Checkpoints, decay, co-residency, `same_as` projections and blast-radius sets are derived, labeled as derived, and never written back as facts.
- Co-residency and every path of length two or more are validated by interval overlap, never by walking a union graph.
- Prefer "unattributed" over "guessed" in identity and IP ownership. Exact identity matching only. Clone collisions are loud errors.
- Late events are diffed against the record just before them in event time, never against current state, and invalidate later checkpoints.
- Both time axes (event time and ingest time) on every record. Checkpoints carry the fold-logic version and the ingest sequence number they were built through.
- `toposhift` stays pure Go and boots with zero config (ADR 0004). CGO (RocksDB) and GPL (Raphtory) code lives only in separate benchmark builds.
- Identity encoding changes require updated golden vectors and an ADR.
- Published performance numbers come only from CI hardware, never from the Oracle Cloud VM, whose terms forbid publishing benchmarks.

## Repository conventions

- Go module path: `github.com/lotannauo/toposhift`. Import grouping: standard library, third party, then this module (enforced by `mise run fmt`).
- Workflows pin actions to full commit SHAs. Do not replace a SHA with a tag.
- No secrets in the repository. Secrets come from the environment.
- Commits are signed. Changes reach `main` through pull requests, squash-merged.
- Do not edit git configuration. Do not force-push or skip hooks unless the repository owner asks.
