# The temporal subgraph API

This page is for anyone who reads toposhift from a program that runs on the same machine, such as a browser page that toposhift serves, and wants the whole history of a few entities in one answer rather than one instant at a time. The [MCP tools](mcp.md) answer a question with a list; the routes on this page answer it with a temporal subgraph: the entities and relations the question reaches, when each existed over a window, and what changed when.

The routes are served on the loopback listener of `toposhift serve` and `toposhift demo`, beside `POST /mcp`, and nowhere else. Every answer is derived and is read at one token.

## Routes

Each route takes `POST` with a JSON body, and answers 200 with a JSON object.

| Route | Body | The subgraph of |
|---|---|---|
| `/api/v1/subgraph/scope` | `scope` (a list of 1 to 256 ids), `from`, `to` | the entities of the scope and every relation with an end among them, one hop out |
| `/api/v1/subgraph/co_residents` | `pod`, `from`, `to`; `node_only` | the pods that shared a node, or a host through two nodes, with the pod |
| `/api/v1/subgraph/service_context` | `service`, `from`, `to`; `node_only` | the service's callers and callees (one `depends_on` hop) and the services that shared a pod, a node or a host with it |
| `/api/v1/subgraph/blast_radius` | `entity`, `from`, `to`; `depth` (1 to 6, default 6) | what a failure of the entity could have reached |

Every body may also hold `asof`, a token as a decimal string. The questions, the fields and their sentences are those of the MCP tools `what_changed` (for `scope`), `co_residents`, `service_context` and `blast_radius`, described in [The MCP tools](mcp.md#tools), with these differences: there is no `limit` and no `at`, because a subgraph is never cut and is always a window `[from, to)`; and `service_context` is always the exploratory one, as the tool is.

A request must be:

- `Content-Type: application/json`, which any other value is answered 415 for. No `Accept` is needed.
- at most 64 KiB, which a larger body is answered 413 for.
- one JSON object with only the fields of its route. An unknown field, a field of the wrong type, anything after the object, or no body is answered 400.
- times in RFC 3339 with a zone, with `from` before `to`; ids as `type:` and 32 lower-case hex digits, for a type of the catalog.

For example, against `toposhift demo`, whose banner prints the id of node-2:

```sh
curl -s http://127.0.0.1:7070/api/v1/subgraph/blast_radius \
  -H 'Content-Type: application/json' \
  -d '{"entity":"<the id the banner gives node-2>","from":"2026-10-01T02:55:00Z","to":"2026-10-01T03:30:00Z"}'
```

## The answer

```json
{
  "token": "4211",
  "derived": true,
  "question": "scope",
  "window": {"from": "2026-10-01T02:55:00Z", "to": "2026-10-01T03:30:00Z"},
  "policy": {"interval": 1, "rank": "287b7cdf8e61e161", "names": 2, "withhold": 1},
  "incomplete": false,
  "names": [{"id": "host:…", "type": "host", "name": null}, {"id": "k8s.node:…", "type": "k8s.node", "name": "node-2", "name_from": "k8sobjects/…"}],
  "counts": {"withheld": {"entities": 0, "edges": 1, "derived": 0, "quarantined_entities": 0, "quarantined_edges": 0}},
  "notes": ["Withheld because each is or names an endpoint the request did not name (an endpoint's id reveals its address): 1 edge. Name the endpoint, by its id, to see it.", "Pass asof=4211 to see the same world, until the next retention (at most a day) or a restart with other ranks."],
  "document": {"format": {"major": 1, "minor": 0}, "basis": "derived", "token": "4211", "…": "the subgraph"}
}
```

- `token` is the one token every read of the answer used, the names' included, and equals `document.token`. A token is a decimal string, so no client rounds it. Pass it back as `asof` to read the same world again.
- `derived` is true, and `document.basis` says `derived` once, for the whole subgraph. Nothing in an answer is a `fact`.
- `names[i]` names `document.entities[i]`, in the same order, as the tools name the same entity: `name` is read from the description of the highest-ranked producer at the last instant of the window, or the first if the entity is not there at the last, and is `null` for a host, an endpoint or a service instance, which are unnamed, or when the entity was not described, or when the name is not printed (more than 253 bytes, or a control or format character). `name_from` is the producer, and `name_note` says why a name is missing when it is for a fixed reason. `policy.rank` is a digest of the producers' authority order, which decides which description wins; `names` and `withhold` are the versions of the rules that read names and withhold addresses.
- `incomplete` is true when a cap of the question's query cut what it read, for example the nodes a co-residency search was allowed to read. Each cut is told in `notes`. The subgraph itself is never cut, and an answer is never partial.
- `counts.withheld` counts what was taken out of the subgraph because it is, or names, an endpoint the request did not name (below).

## The document

`document` is the JSON form of the wire format's temporal subgraph, `format` 1.0, with the same content as the binary form. It holds:

- `from` and `to`, the window as nanoseconds since the epoch, and `token`: each a decimal string, as is every other 64-bit integer, so that no reader squeezes one into a float.
- `policy`, the versions of every rule the subgraph was derived under, and `query`, the question as the server understood it: its `kind`, its `seeds`, every option its policy used, ascending by name, and `asof`, which is `latest` when the request named no token.
- `entities`, the dictionary: each is a `fingerprint`, `type:hash`, and `boundary`, which says the entity is named by an element and is not a subject of the question. Everything else refers to an entity by its id and to an element by its index.
- `elements`: one for each entity, edge or derived fact the question reaches. Its `key` says what it is about; `subject` says `asserted` (a producer said it) or `derived` (it was worked out), and `rule` says by which rule; `class` and `transitions` say how it behaved in the window (`stable`, `appeared`, `disappeared`, `transient`, `flapping`); `pieces` are the spans it held, each with `start` and `end` in nanoseconds after `from`, whether it began before the window, why it ended (`end_reason`, `unknown` when it reaches the end of the window) and which producers asserted it; `paths` and leg `ends` say how a derived element was reached and how each edge of a path ended.
- `keyframes` and `deltas`: the elements present at some instants, and each appearance or disappearance of an element between them, with the instants as nanoseconds after `from`. A client scrubs by applying deltas to a keyframe, forward or backward; each delta says what the element was before and after.
- `quarantine`, the entities and edges whose existence cannot be told because of a clone collision, and, in `sections`, what the question's caps left out and what it would not guess.

`subject: asserted` says what the element is about and not how sure toposhift is of it: it is not a `fact` label, and every element of a subgraph is read from a window and so is derived.

## What is withheld

An endpoint's id is as revealing as its address, so an answer holds one only if the request named that endpoint, by its id. Every other endpoint, and every element that names one, is taken out of the subgraph, counted in `counts.withheld` and told in a note; the subgraph is rebuilt without them, so its keyframes, deltas and indexes stay consistent. An answer that would still hold an unnamed endpoint's id anywhere in its bytes is refused whole, with a sentence that carries no id (500, `internal`). The same rule covers every sentence and log line. A request can still learn whether a guessed endpoint id exists, by naming it.

Names pass the print rule of the tools, so a name that is not printed is `null`. A quarantined entity's cause prints the boot ids of a collision, as the tools do.

A client must show names and ids as text and never as markup: a name is a producer's text.

## Errors

A refusal is `{"error": {"code": "...", "message": "..."}}`, with `Cache-Control: no-store`. The message is the sentence the MCP tools give for the same cause, or a fixed one that echoes nothing the request sent.

| Status | `code` | When |
|---|---|---|
| 400 | `invalid_request` | The body is not one JSON object with the fields of the route |
| 400 | `invalid_argument` | A field is wrong: an id, a time, a window, a depth, a scope of more than 256 ids, or a result that names more than 16,384 entities |
| 403 | | The connection is not on loopback, the `Host` is not `localhost` or a loopback address, or a browser says the request is from another site |
| 405 | `method_not_allowed` | The method is not `POST` |
| 409 | `future_token` | `asof` is ahead of the store |
| 410 | `before_horizon` | The window starts before a layer's retention horizon, or needs a description that is no longer kept |
| 413 | `request_too_large` | The body is over 64 KiB |
| 415 | `unsupported_media_type` | The content type is not `application/json` |
| 422 | `answer_too_large` | The answer is over 16 MiB; narrow the window or the scope |
| 500 | `internal` | The last check refused the answer, or anything else; the cause is logged, without ids |
| 501 | `no_range_reads` | The store does not support range reads |
| 503 | `busy` | Both slots are taken for more than 5 seconds; retry after the `Retry-After` seconds |
| 503 | `unavailable` | No store is attached yet; the sentence says why |
| 503 | `shutting_down` | The server is stopping |
| 504 | `deadline` | The call took longer than 30 seconds |

A client that goes away is answered with nothing.

## How it differs from the tools

- The routes and the tools share the same two slots, the same 30-second deadline, the same sentence before a store is attached and the same stop: a page that scrubs while a client asks tools can be told busy sooner.
- A token ahead of the store is refused, with 409, and never clamped to the store's latest as the tools do: a store restored from a backup reuses token numbers, and a client that pinned one must not be shown another world under it.
- An answer that is too large is refused, never cut.
- The names travel beside the document, in the answer, because the format of the document does not carry names.

## Who can call

The routes are mounted only when `serve` listens on loopback, and `demo` serves only on loopback. The handler does not rely on that alone:

- the address the connection arrived on must be a loopback address;
- the `Host` header must name `localhost` or a loopback address, which refuses a page on another site whose name a browser was made to resolve to this machine;
- the standard library's cross-origin protection refuses a request a browser says is cross-site;
- the routes take `POST` and `application/json` only, which a page on another site cannot send without a preflight that the server never answers, and no `Access-Control-` header is ever written, so a page can use them only from the origin that serves them.

Every response says `Cache-Control: no-store`, `X-Content-Type-Options: nosniff` and `Referrer-Policy: no-referrer`; the server sets no cookie and keeps no session.

The package also offers the routes without these three protections, for a listener that puts its own checks in front of them; `serve` and `demo` do not use it.

Loopback is not authentication: any process on the machine can read the topology, and so can anything that reaches a forwarded loopback port (`kubectl port-forward`, `ssh -L`), which can also send any `Host` header. A route serves the whole one-hop topology of a scope in one call. See [Loopback and the tools](operate.md#loopback-and-the-tools).
