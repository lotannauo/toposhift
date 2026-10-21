# toposhift

toposhift keeps the history of how a Kubernetes cluster's pods, nodes, containers, hosts and services were connected, and answers questions about a past instant or window over MCP.

Every record keeps when it happened and when toposhift learned of it. Anything computed from records, such as a blast radius, is labelled derived and never stored as fact.

**Status: early.** There is no authentication, so the tools are served on loopback only. Interfaces, answers and the store format may change between releases. See [Known shortcomings](#known-shortcomings) and [what was measured](bench/findings/REPORT.md).

[Try it](#try-it-without-a-cluster) · [Run it](#run-it-on-a-cluster) · [Tools](#what-it-answers) · [Retention](#retention) · [Shortcomings](#known-shortcomings) · [Docs](docs/README.md) · [Report](bench/findings/REPORT.md)

## What it answers
Tool | Answers
--- | ---
`find_entities` | Entities whose name contains some text
`neighbors_at` | What an entity is connected to at an instant
`blast_radius` | What a failure could have reached over a window
`co_residents` | Pods that shared a node or host with a pod
`service_context` | A service's callers, callees and co-residents
`what_changed` | What changed between two instants
`entity_history` | When an entity existed, and why spans ended
`resolve_entity` | An entity's id, from its attributes
`describe_schema` | The entity and relation types

`"derived": true` marks a computed answer. `token` names the store state read; pass it as `asof` to read it again. See [docs/mcp.md](docs/mcp.md).

## Try it without a cluster
You need git, curl and Go 1.27.3 or newer; any Go from 1.21 fetches it unless `GOTOOLCHAIN` is `local`.
```sh
git clone https://github.com/lotannauo/toposhift
cd toposhift
CGO_ENABLED=0 go build -trimpath -o bin/toposhift ./cmd/toposhift
./bin/toposhift demo
```
`demo` serves a synthetic cluster from memory at `http://127.0.0.1:7070/mcp` (loopback only) and prints three questions on stderr. Its history runs from 2026-10-01T00:00:00Z to 2026-10-01T06:00:00Z; at 2026-10-01T03:00:00Z the node `node-2` stops being ready.

In a second terminal, ask three questions. First: which node named `node-2` existed one minute before it failed?
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

In a reply, `result.structuredContent` is the answer and `result.content[0].text` is its text twin. With `jq`, add `| jq .result.structuredContent`.

To connect an MCP client, give it this command:
```json
{"mcpServers":{"toposhift":{"command":"/absolute/path/to/bin/toposhift","args":["demo","--stdio"]}}}
```
An HTTP client uses the URL above. Try asking: what was node-2 connected to a minute before it failed?

Every request needs both headers: a 415 means `Content-Type`, a 400 means `Accept`. If port 7070 is taken, run `./bin/toposhift demo --listen 127.0.0.1:7071` and change the URLs. `./bin/toposhift demo help` lists the rest.

## Run it on a cluster
```sh
./bin/toposhift serve
curl http://127.0.0.1:7070/healthz
```
- `serve` needs no configuration: it listens on `127.0.0.1:7070`, keeps data under `$HOME/.local/share/toposhift`, takes OpenTelemetry logs and metrics, and serves the tools at `/mcp`. Collectors: [deploy/otel-collector](deploy/otel-collector).
- A collector in a cluster needs `--listen` (or `TOPOSHIFT_LISTEN`) on a reachable address, which turns the tools off: one `serve` takes remote ingest or serves the tools, not both ([why](docs/operate.md#loopback-and-the-tools)).
- A service call or instance ends 6 hours after its last sighting by default ([details](docs/operate.md#services-and-their-calls)).

## Install
```sh
CGO_ENABLED=0 go install github.com/lotannauo/toposhift/cmd/toposhift@latest
```
Or, in a checkout, `mise install && mise run build` writes `bin/toposhift` ([builds](docs/operate.md#builds)).

## Retention
`toposhift serve` can discard old history once a day: L2 and L3 (pods, nodes, containers, services) keep 30 days, L1 (hosts) 90, and L0 is kept. `--retain on` or `off`, or `TOPOSHIFT_RETAIN`, chooses; the flag wins.
- **Releases**, from v0.1.0 on, ship with retention **off** until background retention passes its latency check on CI hardware, which it has not yet done. The store then grows as long as `serve` runs. A `release-*` branch builds the same way.
- **Builds of `main`** (`go install …@main`, or `mise run build` in a checkout) have retention **on**. `go install …@latest` builds the newest release, v0.1.0 so far, which has retention off.
- `GET /status` says which mode is in force and what set it; on a release, for example, `"retention":{"mode":"off","set_by":"default"}`. More: [retention](docs/operate.md#retention).

## Known shortcomings
- No authentication; ingest routes accept whatever reaches them.
- One `serve` takes remote ingest or serves the tools, not both.
- No UI or HTTP query API besides MCP.
- A call stops after 30 seconds, with no partial answer.
- Range answers that reach a host cost more as the host's history grows (in v0.1.0, any entity's).

[All of them](docs/limits.md). In the report: [what the evidence does not show](bench/findings/REPORT.md#what-this-does-not-show) and [its shortcomings](bench/findings/REPORT.md#10-known-shortcomings).

## Documentation
Read | To learn
--- | ---
[docs/mcp.md](docs/mcp.md) | Tools and transports
[docs/how-it-works.md](docs/how-it-works.md) | Facts, derived answers
[docs/operate.md](docs/operate.md) | Running `serve`
[docs/limits.md](docs/limits.md) | Not done yet
[deploy/otel-collector](deploy/otel-collector) | Collectors
[bench/findings/REPORT.md](bench/findings/REPORT.md) | What was measured, and what was not

## Contributing, security, license
[CONTRIBUTING.md](CONTRIBUTING.md); contributors and coding agents start with [AGENTS.md](AGENTS.md), then [docs/](docs/README.md). [SECURITY.md](SECURITY.md). Apache-2.0: [LICENSE](LICENSE).
