# Contributing

The project is at an early stage and the design is still moving, so please open an issue to discuss a change before writing a large pull request.

## Setup

```sh
mise install
mise run hooks
mise run ci
```

If mise cannot install a tool on your platform, install it with your package manager at the version pinned in `mise.toml`.

## Pull requests

- Keep each pull request to one change.
- Run `mise run ci` first; it runs exactly what CI runs.
- Commits should be signed. Pull requests are squash-merged, and the pull request title becomes the commit message, so write it as one.
- Read `AGENTS.md` for the rules that are easiest to break by accident.
