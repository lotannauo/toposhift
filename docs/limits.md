# What toposhift does not do yet

This page is for anyone deciding whether toposhift fits their use. It lists what the product does not do yet; the [README](../README.md) keeps the five that matter most to a first user.

What the evidence does not show is a separate list, in the findings report: [What this does not show](../bench/findings/REPORT.md#what-this-does-not-show) and [10. Known shortcomings](../bench/findings/REPORT.md#10-known-shortcomings).

- There is no authentication. The tools and the subgraph routes are served on loopback only, and the ingest routes accept whatever reaches them.
- One `serve` either takes ingest from a remote collector or serves the tools, not both (see [Run it on a cluster](../README.md#run-it-on-a-cluster) and [Loopback and the tools](operate.md#loopback-and-the-tools)).
- There is no UI. The only HTTP query routes besides MCP are the four of the [subgraph API](api.md), each answering one fixed question; there is no query language and no way to serve them off loopback.
- A call is stopped after 30 seconds with no partial answer; `service_context` on a large service over a long window can reach it.
- Range answers that reach a host cost more as the host's history grows (in v0.1.0, any entity's), because a host's history is folded from the start of retention. Seed walks of other entities stop at a checkpoint, so they are bounded, except across a checkpoint hole (below).
- A release made before background retention passes its latency check on CI hardware, such as v0.1.0, ships with retention off, so its store keeps all history and grows for as long as `serve` runs unless `--retain on` is given. Answers stay correct; reads of long-lived objects cost more as the store ages, and turning retention on later discards what is older than each layer's window at its first pass ([retention](operate.md#retention)).
- `find_entities` checks at most 256 ids a call and says how to continue.
- A producer's text that contains an endpoint's id is not printed: an answer that would print it is refused, not shown.
- A container has no namespace under `serve`'s ranks, so a namespace filter finds no container.
- The demo deletes evicted pods at once.
- Checkpoint holes left by late records are not repaired.
- Translator resets can still raise read work in watch-only mode.
- Native sidecars are not read.
- The memory the shipped writer needs at scale is not measured on CI.
- The recency `service_context` gives a caller or callee cannot show a silence shorter than half the expiry (3 hours at the default expiry of 6 hours), and it shows only for dependencies; entities and placements have none.
