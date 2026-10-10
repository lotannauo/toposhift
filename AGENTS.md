# Guidance for agents and contributors

toposhift is a temporal topology graph for infrastructure. Read `README.md` for the pitch. Architecture decision records live in `docs/adr/`: that folder is local and untracked, present only in the owner's checkout and never on GitHub. The design plan is maintained outside this repository; ADRs are the durable record.

## Commands

```sh
mise install        # install pinned tools
mise run check      # between edits: format and lint checks, tests without the race detector
mise run ci         # before committing: everything CI runs on a pull request
mise run ci:deep    # the full race-detector run CI does after a push to main and nightly
mise run fmt        # format Go code
mise run test       # go test -race
mise run hooks      # install git hooks
```

`check` and `ci` reuse a package's test result unless its test binary or an input the test read changed, so they rerun only what an edit reaches. A comment-only change that moves no line leaves every binary the same and reruns no test; one that adds or removes a line changes the line numbers compiled into its package and reruns the packages that import it. `ci:deep` always reruns everything. CI runs `ci` as four shards (`ci:static`, `ci:bench`, `ci:race` for the root module's race tests, `ci:benchrace` for the bench module's); add a new check to a shard, never to `ci` itself (`check:ci-shards` fails otherwise).

## Rules most likely to be broken by a locally sensible change

- Once an ADR exists for a decision, the ADR wins. When reality contradicts an ADR, write a new ADR that supersedes it. Never silently rewrite earlier reasoning.
- The fact log stores only producer-asserted facts and attributed store-authored events. Checkpoints, decay, co-residency, `same_as` projections and blast-radius sets are derived, labeled as derived, and never written back as facts.
- Co-residency and every path of length two or more are validated by interval overlap, never by walking a union graph.
- Prefer "unattributed" over "guessed" in identity and IP ownership. Exact identity matching only. Clone collisions are loud errors.
- Late events are diffed against the record just before them in event time, never against current state, and invalidate later checkpoints.
- Both time axes (event time and ingest time) on every record. Checkpoints carry the fold-logic version and the ingest sequence number they were built through.
- `toposhift` stays pure Go and boots with zero config. CGO (RocksDB) and GPL (Raphtory) code lives only in separate benchmark builds.
- Identity encoding changes require updated golden vectors and an ADR.
- Published performance numbers come only from CI hardware, never from the Oracle Cloud VM, whose terms forbid publishing benchmarks.

## Repository conventions

- Go module path: `github.com/lotannauo/toposhift`. Import grouping: standard library, third party, then this module (enforced by `mise run fmt`).
- Workflows pin actions to full commit SHAs. Do not replace a SHA with a tag.
- No secrets in the repository. Secrets come from the environment.
- Commits are signed. Changes reach `main` through pull requests, squash-merged.
- Do not edit git configuration. Do not force-push or skip hooks unless the repository owner asks.

## Handoff conditions for agents

- Commits are signed with the owner's Touch ID, so an unattended agent cannot sign. Leave changes uncommitted and ask the owner to commit.
- Never add or remove keys on the GitHub account.
- Work only in the owner's local checkout: `docs/adr/` is untracked, so a fresh clone cannot see it.
