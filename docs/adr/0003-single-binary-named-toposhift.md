# 3. One binary, named toposhift

Date: 2026-09-30

## Status

Accepted

## Context

The project needs a server (ingest and query) and client operations (query, replay recorded events, inspect the store). Tools that split these conventionally name the daemon with a `d` suffix and ship a separate client, such as `tailscaled` and `tailscale`. Tools that ship one binary usually give it the project name, such as `vault`, `consul` and `cockroach`.

An earlier draft called the binary `topod`. The repository was then named `toposhift`.

## Decision

There is one executable, `toposhift`, with subcommands: `serve`, `query`, `replay` and `version`. It is installed with `go install github.com/lotannauo/toposhift/cmd/toposhift@latest`. There is no separate client binary.

## Consequences

- One artifact to build, sign, release and document; the name matches the repository.
- Server and client code ship together, so they never drift apart in version.
- If a separate daemon and client are ever needed, the daemon becomes `toposhiftd` and the client keeps the name `toposhift`. That would need a superseding ADR.
