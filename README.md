# toposhift

A temporal topology graph for infrastructure. For each entity (a pod, a node, a container, a host, a service) it keeps a log of what producers asserted about it, with the time each thing happened and the time toposhift learned of it. Whatever it works out from that log, such as a blast radius or the pods that shared a node, is computed when you ask, labelled as derived, and never written back as a fact.

Today it answers these questions, read-only, over MCP: which entities have a name containing some text, at an instant or over a window; what an entity is connected to at an instant; the blast radius of a failing entity over a window (from a host, the nodes on it and everything placed on them; from a node, the pods, containers and service instances placed on it; from a service, the services that call it, directly or through others); which pods shared a node, or a host, with a pod; the context of a service over a window: the services one call away, as callers or callees, and the services that shared a pod, a node or a host with it; what changed in a set of entities between two instants; when an entity existed and why each span ended; the id of an entity from its identifying attributes; and the schema, which says how each relation carries failure.

## Status

Pre-release: there is no release yet, and v0.1.0 will be the first. `toposhift serve` takes in what an OpenTelemetry collector reads from a Kubernetes cluster (pods, nodes and containers; hosts, through the optional node agents; services and the calls between them, through the optional trace pipeline) and keeps it in an on-disk store, with no configuration. It serves the read-only tools on loopback. `toposhift demo` serves the same tools from a made-up cluster, with nothing to set up. Interfaces, answers and the store format may change between releases.

## Try it without a cluster

You need git and Go 1.27.2 or newer; any Go from 1.21 downloads it by itself unless `GOTOOLCHAIN` is set to `local`.

```sh
git clone https://github.com/lotannauo/toposhift
cd toposhift
CGO_ENABLED=0 go build -trimpath -o bin/toposhift ./cmd/toposhift
./bin/toposhift demo
```

`demo` builds a synthetic Kubernetes cluster in memory, prints a banner on stderr, and serves the tools at `http://127.0.0.1:7070/mcp` (loopback only, nothing written to disk) until you stop it. Nothing in it is real: its history runs from 2026-10-01T00:00:00Z to 2026-10-01T06:00:00Z, and the banner says what happens in it and prints three questions to try. At 2026-10-01T03:00:00Z the node `node-2` stops being ready.

In a second terminal, ask three questions. The server keeps no session, so each is a single `tools/call` request. Send both headers: the server answers 415 to a request whose `Content-Type` is not `application/json`, and 400 to one whose `Accept` does not admit both `application/json` and `text/event-stream` (curl's default, `*/*`, admits both; a request with no `Accept` at all is refused).

First, from a name to an id: which node named `node-2` existed one minute before it failed?

```sh
curl -s -X POST http://127.0.0.1:7070/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"find_entities","arguments":{"type":"k8s.node","name":"node-2","at":"2026-10-01T02:59:00Z"}}}'
```

Its id is `k8s.node:3c5919a8fa3541cd06c41e864b40a22f`. Second, what was it connected to then?

```sh
curl -s -X POST http://127.0.0.1:7070/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"neighbors_at","arguments":{"entity":"k8s.node:3c5919a8fa3541cd06c41e864b40a22f","at":"2026-10-01T02:59:00Z"}}}'
```

Third, its blast radius over the first half hour of the failure:

```sh
curl -s -X POST http://127.0.0.1:7070/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"blast_radius","arguments":{"entity":"k8s.node:3c5919a8fa3541cd06c41e864b40a22f","from":"2026-10-01T03:00:00Z","to":"2026-10-01T03:30:00Z"}}}'
```

In each reply, `result.structuredContent` is the answer and `result.content[0].text` is the same answer as text. `"derived": true` marks an answer computed from the records rather than read from them, and `token` names the state of the store it was read at: pass it back as `asof` to read the same state again. If you have `jq`, add `| jq .result.structuredContent` to a command to read it. The tools describe themselves; ask `describe_schema` for the entity and relation types and the words every answer uses.

To connect an MCP client, use either transport:

- Over standard input and output: a client that starts a command runs the binary and speaks to it over its standard input and output. Give it the command `/absolute/path/to/bin/toposhift` and the arguments `demo --stdio`.
- Over HTTP: give it the URL `http://127.0.0.1:7070/mcp`.

`./bin/toposhift demo help` lists the rest.

## Run it on a cluster

```sh
./bin/toposhift serve
curl http://127.0.0.1:7070/healthz
```

`serve` needs no configuration. It listens on `127.0.0.1:7070`, keeps its data under `$HOME/.local/share/toposhift` (`--data-dir` or `TOPOSHIFT_DATA_DIR` names another directory), takes OpenTelemetry logs at `POST /v1/logs` and metrics at `POST /v1/metrics`, serves the read-only tools at `POST /mcp`, and reports its state at `GET /status`. `./bin/toposhift serve -h` lists the flags. A ready-made collector configuration, with its README, is in [deploy/otel-collector](deploy/otel-collector).

The tools are served only when `serve` listens on loopback, because the server has no authentication yet. A collector inside a cluster cannot reach loopback, and the collector README tells it to set `TOPOSHIFT_LISTEN` to an address the cluster can reach. So today one `serve` either takes ingest from a remote collector or serves the tools, not both, unless the collector reaches the machine's loopback: a collector on the same machine, or a forwarded port (whatever reaches the forward reaches every route). With an address that is not loopback, `serve` says at start that the tools are off.

## Install

```sh
CGO_ENABLED=0 go install github.com/lotannauo/toposhift/cmd/toposhift@latest
```

The documented install builds without cgo. A cgo build links Pebble's optional C zstd, which the store does not use (block compression is Pebble's default, Snappy).

## Build from source

Tool versions are pinned in `mise.toml`.

```sh
mise install
mise run build
```

`mise run build` writes `bin/toposhift`, without cgo. If mise cannot install a tool on your platform, build with the `go build` line under "Try it without a cluster". To contribute, see [CONTRIBUTING.md](CONTRIBUTING.md).

## Retention

`toposhift serve` can discard old history once a day: L2 (pods, nodes) keeps 30 days, L1 (hosts) 90 days, and L0 is kept. `--retain on` or `TOPOSHIFT_RETAIN=on` turns it on, and `off` turns it off; the flag wins over the variable.

The default depends on the build:

- **Releases made before background retention passes its latency check on CI hardware** ship with retention **off** by default, and v0.1.0 is the first. `go install …@v0.1.0` installs it, and once v0.1.0 is tagged `@latest` installs the newest release, which has retention off while it is one of these releases. `serve` then keeps every record it is sent, so its store grows for as long as it runs, and reads of objects with a long history cost more over time. Background retention has not yet passed that check, so these releases do not turn it on by default. A checkout of a release branch (`release-*`), once one exists, builds with retention off too.
- **Builds of `main`** (`go install …@main`, or `mise run build` in a checkout of main) keep retention **on** by default.

When retention is off by default and `serve` has a data directory, `serve` logs a warning at start that says how to turn it on. `GET /status` reports where the mode came from, for example `"retention": {"mode": "off", "set_by": "default"}`. A store that ran without retention loses what is older than each layer's window at its first retention, once the producers' clocks agree with the server's.

## Known shortcomings

What toposhift does not do yet:

- There is no authentication. The tools are served on loopback only, and the ingest routes accept whatever reaches them.
- One `serve` either takes ingest from a remote collector or serves the tools, not both (see "Run it on a cluster").
- There is no UI and no HTTP query API besides MCP.
- A call is stopped after 30 seconds with no partial answer; `service_context` on a large service over a long window can reach it.
- Range answers on a long-lived store cost more as it ages, because an entity's history is folded from the start of retention.
- `find_entities` checks at most 256 ids a call and says how to continue.
- A producer's text that contains an endpoint's id is not printed: an answer that would print it is refused, not shown.
- A container has no namespace under `serve`'s ranks, so a namespace filter finds no container.
- The demo deletes evicted pods at once.
- Checkpoint holes left by late records are not repaired.
- Translator resets can still raise read work in watch-only mode.
- Native sidecars are not read.
- The memory the shipped writer needs at scale is not measured on CI.

The list of what the evidence does not show arrives with the findings report.

## License

Apache-2.0. See [LICENSE](LICENSE).
