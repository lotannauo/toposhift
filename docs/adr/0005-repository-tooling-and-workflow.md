# 5. Repository tooling and workflow

Date: 2026-09-30

## Status

Accepted

## Context

The repository is public from its first commit, so its supply chain and history are part of the product. Local development and CI must not drift apart, and a change should be checkable before it is merged.

## Decision

- **Tool versions** are pinned in `mise.toml`. CI installs the same versions. Go tools that live in the module (govulncheck) are pinned with `tool` directives in `go.mod` and run with `go tool`.
- **Tasks** are `mise run` tasks. `mise run ci` runs exactly what the CI job runs. There is no Makefile.
- **Formatting** is gofumpt and goimports through `golangci-lint fmt`; CI fails on any difference.
- **Workflows** pin every third-party action to a full commit SHA with the version in a comment, default to read-only permissions, and are linted by actionlint and zizmor.
- **Commits** are signed with SSH keys held in the Mac's Secure Enclave. The `main` branch requires signed commits.
- **Merging** happens through pull requests with required checks, squash only, using the pull request title as the commit message. Force pushes and deletion of `main` are blocked.
- **Dependencies** are updated by Renovate, which waits a few days after a release before proposing it.
- **Go module** has no `replace` directives, so `go install ...@latest` works. CI enforces this.

## Consequences

- A clean clone plus `mise install && mise run ci` reproduces CI.
- Supply-chain attacks through moved action tags or freshly published malicious versions are harder.
- Pinned versions need regular updates, which Renovate automates; reviewing them is a recurring cost.
- Git hooks (lefthook) are a convenience; CI is the enforcement.
