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

`check` and `ci` reuse a package's test result unless its test binary or an input the test read changed, so they rerun only what an edit reaches. A comment-only change that moves no line leaves every binary the same and reruns no test; one that adds or removes a line changes the line numbers compiled into its package and reruns the packages that import it. `ci:deep` always reruns everything. CI runs `ci` as six shards (`ci:static`, `ci:bench`, `ci:raceheavy` and `ci:racerest` for the root module's race tests, `ci:benchraceheavy` and `ci:benchracerest` for the bench module's, each pair split by package in `internal/raceshards`); add a new check to a shard, never to `ci` itself (`check:ci-shards` fails otherwise). `rocksdb.yml` is the one exception: it builds the pinned RocksDB and runs the cgo module `bench/rocksdb`'s lint, vulnerability scan and tests (`mise run bench:rocksdb:*`, which need RocksDB installed), outside the shards and not as a required check, because those checks need cgo and a native library that no shard has; `mise run ci` does not run them.

## Rules most likely to be broken by a locally sensible change

- Once an ADR exists for a decision, the ADR wins. When reality contradicts an ADR, write a new ADR that supersedes it. Never silently rewrite earlier reasoning.
- The fact log stores only producer-asserted facts and attributed store-authored events. Checkpoints, decay, co-residency, `same_as` projections and blast-radius sets are derived, labeled as derived, and never written back as facts.
- Co-residency and every path of length two or more are validated by interval overlap, never by walking a union graph.
- Prefer "unattributed" over "guessed" in identity and IP ownership. Exact identity matching only. Clone collisions are loud errors.
- Late events are diffed against the record just before them in event time, never against current state, and invalidate later checkpoints.
- Both time axes (event time and ingest time) on every record. Checkpoints carry the fold-logic version and the ingest sequence number they were built through.
- `toposhift` stays pure Go and boots with zero config. CGO (RocksDB) and GPL (Raphtory) code lives only in separate benchmark builds (RocksDB in the nested module `bench/rocksdb`, never in `go.mod` or `bench/go.mod`).
- Identity encoding changes require updated golden vectors and an ADR.
- Published performance numbers (timings, rates, memory) come only from CI hardware, never from the Oracle Cloud VM, whose terms forbid publishing benchmarks. A deterministic count (a pure function of the commit, the input hashes and the flags, with the same digest on a second run) may be quoted from a laptop in the findings report under `bench/findings`, labelled `local, deterministic` and given with its commit, input hash and reproduction command; raw outputs stay uncommitted, except the evaluation's results file in `bench/eval/results`.

## Repository conventions

- Go module path: `github.com/lotannauo/toposhift`. Import grouping: standard library, third party, then this module (enforced by `mise run fmt`).
- Workflows pin actions to full commit SHAs. Do not replace a SHA with a tag.
- No secrets in the repository. Secrets come from the environment.
- Every commit pushed to a branch on GitHub is signed; GitHub refuses unsigned commits on every branch. Changes reach `main` through pull requests, squash-merged.
- A release made while retention ships off by default has its own branch `release-vX.Y.Z`, pushed once by the owner together with its tag `vX.Y.Z` and never updated: one signed commit on a commit of `main` that changes only the retention default and the test that pins it. No branch other than `release-*` and `verify` is pushed outside a pull request.
- Do not edit git configuration. Do not force-push or skip hooks unless the repository owner asks.

## Handoff conditions for agents

- Commits are signed with the owner's Touch ID, so an unattended agent cannot sign. Lane agents never commit: leave changes uncommitted and ask the owner to commit.
- Only the owner's main session commits, and only through the owner's landing script. It may make local, unsigned work-in-progress commits that are never pushed; work reaches GitHub only as one signed commit per lane.
- Never add or remove keys on the GitHub account.
- Work only in the owner's local checkout: `docs/adr/` is untracked, so a fresh clone cannot see it.
- Verification runs: a lane agent may dispatch `verify.yml` from the `verify` branch (`gh workflow run verify.yml --ref verify -f label=<lane> -f base_sha=<sha> -f tree=<sha> -f patch_sha256=<hex> -f patch_b64=<text>`) for the tree its own lane shipped, to run CI's shards on a patch on GitHub's runners with no commit, and may read the result. That is the only workflow an agent dispatches: never `bench.yml` or `diag.yml`, never another ref, and an agent never creates, moves or deletes a ref (the `verify` branch is the owner's). The patch is the lane's own diff against a commit of main, and the tree named is the shipped tree.
