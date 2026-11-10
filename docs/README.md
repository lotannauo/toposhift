# Documentation

Start with the [README](../README.md) for the pitch and a demo. Then choose a path below. The public guides stand alone; the project's private design records are not needed to follow them.

- **Try and query:** connect a client with [MCP](mcp.md), or ask the four fixed questions through the [JSON API](api.md). Read [how answers are derived](how-it-works.md) to interpret them.
- **Operate and collect:** start with [running toposhift](operate.md), then [set up the collectors](../deploy/otel-collector/README.md). Check the [current limits](limits.md) before deployment.
- **Understand and contribute:** start with the [system overview](architecture.md) and [how answers are derived](how-it-works.md), then the [findings report](../bench/findings/REPORT.md) for the measured evidence and its limits. Contributor instructions are below.

## Reference map

| Page | Read it to learn |
|---|---|
| [architecture.md](architecture.md) | How collection, durable writes, derived queries and read modes fit together, and which integrations remain unwired |
| [mcp.md](mcp.md) | The transports, the two required headers, each tool's arguments, how to read an answer, and how to set up a client |
| [api.md](api.md) | The four routes that answer a question with a temporal subgraph as JSON, how to read the answer, and what is withheld |
| [how-it-works.md](how-it-works.md) | What is a fact and what is derived, the two time axes, layers and relations, late records, and checkpoints |
| [operate.md](operate.md) | The defaults and routes of `serve`, the loopback rule, retention, services and their calls, and builds |
| [limits.md](limits.md) | What toposhift does not do yet |
| [../deploy/otel-collector/README.md](../deploy/otel-collector/README.md) | How to set up the collectors that feed `serve` |
| [../bench/findings/REPORT.md](../bench/findings/REPORT.md) | What was measured and compared, what was not, and each figure's provenance |

Contributors start with [CONTRIBUTING.md](../CONTRIBUTING.md) and [AGENTS.md](../AGENTS.md).
