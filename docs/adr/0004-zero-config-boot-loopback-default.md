# 4. Zero-config boot, loopback by default

Date: 2026-09-30

## Status

Accepted

## Context

A new user, a CI job and a test should be able to start toposhift with no setup and have it work. Configuration that is mandatory turns every first run into a debugging session, and a server that binds all interfaces by default is reachable from the network before anyone has configured authentication.

## Decision

`toposhift serve`, given no flags, no environment variables and no config file, must start, accept requests and shut down cleanly. A test builds the real binary and starts it with an empty environment to enforce this.

The default listen address is `127.0.0.1:7070`. Binding a non-loopback address is possible through `--listen` or `TOPOSHIFT_LISTEN`, and logs a warning while the server has no authentication.

Configuration layers, lowest to highest precedence: built-in defaults, YAML file, environment, flags. Unknown keys in a YAML file are errors. Secrets come only from the environment, never from flags or files. Only defaults, environment and flags exist today; the YAML layer arrives with the first setting that needs it.

Optional infrastructure (object storage such as R2, the OpenTelemetry Collector) is never required to boot.

## Consequences

- The first-run experience is one command.
- Every new feature must have a working default, or be optional. A feature that cannot have one needs its own ADR.
- Tests that start the binary use the default port and skip themselves when it is taken, so they run in CI and on clean machines.
