# The MCP tools

This page is for anyone who connects an MCP client, or a script, to toposhift. The [README](../README.md) shows three calls end to end; this page lists everything a caller can rely on.

## Transports

| Transport | How | Served by |
|---|---|---|
| HTTP | `POST /mcp`, stateless: no session, one `tools/call` request per call | `toposhift serve`, `toposhift demo` |
| Standard input and output | One JSON-RPC message per line; nothing else is written to the output | `toposhift demo --stdio`, `toposhift mcp` |

The HTTP tools are served only on a loopback listener, because the server has no authentication. `serve` on any other address turns them off. See [Loopback and the tools](operate.md#loopback-and-the-tools).

`toposhift demo` serves a synthetic cluster from memory.

`toposhift mcp --data-dir DIR/store` serves one client over standard input and output from a store that `toposhift replay` filled and no other process holds open. A running server keeps its store in the `store` directory of its data directory, so stop the server and give `DIR/store`, not `DIR`. `toposhift mcp help` and `toposhift replay help` say the rest.

## Headers

Every HTTP request needs both of these:

- `Content-Type: application/json`. Any other value is answered 415.
- An `Accept` that admits both `application/json` and `text/event-stream`. Any other value is answered 400, and so is a request with no `Accept` at all.

curl's default `Accept`, `*/*`, admits both. The examples in the README send both explicitly.

## Tools

To list them, send `{"jsonrpc":"2.0","id":1,"method":"tools/list"}` with the two headers. There are nine. Times are RFC 3339 with a zone. A window `[from, to)` includes `from` and excludes `to` (`what_changed` includes both instants). Every tool that reads the store also takes `asof` (see [Reading an answer](#reading-an-answer)).

| Tool | Question | Arguments |
|---|---|---|
| `find_entities` | Which pods, nodes, containers or services have a name containing some text, at an instant or over a window? | `type` (required: `k8s.pod`, `k8s.node`, `container` or `service`); `name`; `namespace`; `at`, or `from` and `to`; `after`; `limit` |
| `neighbors_at` | What is an entity connected to at an instant? Each edge counts only while both its ends exist. | `entity`, `at` (required); `direction` (`forward`, `reverse` or `both`) |
| `blast_radius` | What could a failure of an entity have reached over a window, by the recorded relations? | `entity` (required); `at`, or `from` and `to`; `depth`; `limit` |
| `co_residents` | Which pods shared a node, or a host through two nodes, with a pod? | `pod`, `from`, `to` (required); `node_only`; `limit` |
| `service_context` | Which services call a service or are called by it (one hop), and which shared a pod, a node or a host with it? | `service`, `from`, `to` (required); `node_only`; `limit` |
| `what_changed` | What was created, deleted or changed in a set of entities between two instants, and what ended each disappearance? | `scope` (a list of ids), `from`, `to` (required); `limit` |
| `entity_history` | When did an entity exist, and why did each span end? | `entity`, `from`, `to` (required); `limit` |
| `resolve_entity` | What is the id of an entity with exactly these identifying attributes? | `type`, `attributes` (required); `at` |
| `describe_schema` | What are the entity and relation types, how does failure travel along each relation, and what do the words in an answer mean? | none |

A few details that change how you call them:

- `blast_radius` starts from a host, a node or a service. From a host it reaches the nodes on it and everything placed on them. From a node it reaches the pods, containers and service instances placed on it. From a service it reaches the services that call it, directly or through others.

  Each member says when it was reached, by which edges, and how each edge ended. The answer is derived.
- `co_residents` and `service_context` pair entities only where their intervals overlap. A placement or a mapping that cannot be told is listed as unattributed, never guessed.
- `find_entities` compares names in lower case. Hosts have no name and endpoints are never listed. To go from a name to an id, call it; to compute an id from identifying attributes, call `resolve_entity`, which never searches.
- `neighbors_at` takes `direction`; the default is `both`.
- `describe_schema` reads no store. Ask it for the words an answer uses, such as `fact`, `derived`, `stable`, `flapping` and the end reasons.

## Reading an answer

A reply is a JSON-RPC response. `result.structuredContent` is the answer and `result.content[0].text` is the same answer as text. `result.isError` is true when the call was refused, and the text says why.

- `"derived": true` marks an answer computed from the records rather than read from them. A blast radius, a co-resident and a service context are derived.
- `token` names the state of the store the answer was read at. Pass it back as `asof` to read the same state again, for example to continue a long answer or to compare two questions at one moment.

## Limits that shape calls

- A call is stopped after 30 seconds, with no partial answer. A long window on a large service can reach it; a shorter window, or `node_only`, reads less.
- `find_entities` checks at most 256 ids a call, in id order. When it stops early, the answer says so and gives `next_after`: pass it as `after`, with the same `asof`, to continue.
- `limit` is 1 to 200 for `find_entities` and 1 to 500 for the other tools, and defaults to 50.
- A name, a namespace or a description that is too long or holds a control or format character is not printed. It is null, and the answer says it was not printed.
- An answer that would print an endpoint's id, which reveals an address, is refused unless the request named that endpoint. Elsewhere such an edge is withheld and counted.
- On a store with retention on, a window that starts before a layer's horizon is refused, and the refusal says from when a window does not need it. See [Retention](operate.md#retention).

## Client setup

For a client that starts a command, give it the binary and the arguments. Use the absolute path of the `toposhift` you built:

```json
{"mcpServers":{"toposhift":{"command":"/absolute/path/to/bin/toposhift","args":["demo","--stdio"]}}}
```

For a client that speaks HTTP, give it `http://127.0.0.1:7070/mcp` while `demo` or `serve` runs there. If the port is taken, start `demo` with `--listen 127.0.0.1:7071` and use that address.

A first question to ask: what was node-2 connected to one minute before it stopped being ready?
