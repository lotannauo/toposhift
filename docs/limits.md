# What toposhift does not do yet

This page is for anyone deciding whether toposhift fits their use. It lists what the product does not do yet; the [README](../README.md) keeps the five that matter most to a first user.

What the evidence does not show is a separate list, in the findings report: [What this does not show](../bench/findings/REPORT.md#what-this-does-not-show) and [10. Known shortcomings](../bench/findings/REPORT.md#10-known-shortcomings).

- There is no authentication. The tools are served on loopback only, and the ingest routes accept whatever reaches them.
- One `serve` either takes ingest from a remote collector or serves the tools, not both (see [Run it on a cluster](../README.md#run-it-on-a-cluster) and [Loopback and the tools](operate.md#loopback-and-the-tools)).
- There is no UI and no HTTP query API besides MCP.
- A call is stopped after 30 seconds with no partial answer; `service_context` on a large service over a long window can reach it.
- Range answers that reach a host cost more as the host's history grows (in v0.1.0, any entity's), because a host's history is folded from the start of retention. Seed walks of other entities stop at a checkpoint, so they are bounded, except across a checkpoint hole (below).
- `find_entities` checks at most 256 ids a call and says how to continue.
- A producer's text that contains an endpoint's id is not printed: an answer that would print it is refused, not shown.
- A container has no namespace under `serve`'s ranks, so a namespace filter finds no container.
- The demo deletes evicted pods at once.
- Checkpoint holes left by late records are not repaired.
- Translator resets can still raise read work in watch-only mode.
- Native sidecars are not read.
- The memory the shipped writer needs at scale is not measured on CI.
