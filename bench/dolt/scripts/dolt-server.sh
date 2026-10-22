#!/usr/bin/env bash
# dolt-server.sh runs the Dolt server the Dolt baseline is tested and measured against, in a
# container, with the settings of server.yaml. It is the one way the server is started, on a
# laptop and on CI, so that one code path is tested.
#
# usage: dolt-server.sh start | stop | sql [dolt sql arguments...] | image
#
#   start  pull the image if it is not there, start the container, and wait until the
#          server answers with the registered version
#   stop   stop the container (a clean shutdown) and remove it; the data stays
#   sql    run `dolt sql` in the container, against the running server
#   image  print the image reference (tag and digest) the server is started from
#
# The server has no password: the port is published on loopback only, and whoever can reach
# it is trusted. The container sends nothing out: usage reporting is switched off in its
# global configuration, and start refuses a server for which it is not.
#
# Environment:
#   TOPOSHIFT_DOLT_DATA    the data directory (default ~/spikebench/dolt/2.4.2); created if
#                          absent. Never put it inside a git work tree.
#   TOPOSHIFT_DOLT_PORT    the port on 127.0.0.1 (default 33306)
#   TOPOSHIFT_DOLT_NAME    the container's name (default toposhift-dolt)
#   TOPOSHIFT_DOLT_MEMORY  a memory limit for the container, as docker takes it (default none)
set -euo pipefail

# The image: the tag says what it is, the digest of the multi-architecture index is what is
# pulled. Never a tag alone.
image='dolthub/dolt-sql-server:2.4.2@sha256:30c7e4fa8d3dd46be9728d7a5c341ceb2ffce7e42b484ef91c999c4c707525e9'
version='2.4.2'

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
config="$here/server.yaml"
name="${TOPOSHIFT_DOLT_NAME:-toposhift-dolt}"
port="${TOPOSHIFT_DOLT_PORT:-33306}"
data="${TOPOSHIFT_DOLT_DATA:-$HOME/spikebench/dolt/$version}"
memory="${TOPOSHIFT_DOLT_MEMORY:-}"

die() {
  echo "dolt-server.sh: $*" >&2
  exit 1
}

start() {
  command -v docker >/dev/null || die "docker is not installed"
  case "$data" in
    /*) ;;
    *) die "TOPOSHIFT_DOLT_DATA must be an absolute path, not '$data'" ;;
  esac
  case "$port" in
    '' | *[!0-9]*) die "TOPOSHIFT_DOLT_PORT must be a number, not '$port'" ;;
  esac
  [ -f "$config" ] || die "$config is missing"
  if docker container inspect "$name" >/dev/null 2>&1; then
    die "the container $name already exists; run 'dolt-server.sh stop' first"
  fi
  mkdir -p "$data"
  if git -C "$data" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    die "$data is inside a git work tree; the data directory must not be"
  fi
  # Pull only what is not already there, so that a laptop that has the image never reaches
  # for the network.
  if ! docker image inspect "$image" >/dev/null 2>&1; then
    docker pull --quiet "$image" >/dev/null
  fi

  local -a limits=()
  [ -z "$memory" ] || limits=(--memory "$memory" --memory-swap "$memory")

  # The image's entrypoint reads server configuration from /etc/dolt/servercfg.d and the
  # global dolt configuration from /etc/dolt/doltcfg.d, and creates root with the host it is
  # given, so that the published port can be used.
  docker run --detach --name "$name" "${limits[@]}" \
    -e DOLT_ROOT_HOST='%' \
    -p "127.0.0.1:$port:3306" \
    -v "$data:/var/lib/dolt" \
    -v "$config:/etc/dolt/servercfg.d/server.yaml:ro" \
    -v "$here/global.json:/etc/dolt/doltcfg.d/global.json:ro" \
    "$image" >/dev/null

  # The entrypoint says so when the server is up and its configuration is in place; asking
  # `dolt sql` earlier would open the data directory beside the server that is starting.
  local i got
  for ((i = 0; i < 300; i++)); do
    if docker logs "$name" 2>&1 | grep -q 'Dolt init process done. Ready for connections.'; then
      got="$(docker exec "$name" dolt sql --result-format csv --query 'SELECT dolt_version()' | tail -n 1)"
      if [ "$got" = "$version" ]; then
        check_reporting
        return 0
      fi
      docker logs "$name" >&2 || true
      die "the server is not Dolt $version: it said '$got'"
    fi
    if [ "$(docker container inspect --format '{{.State.Running}}' "$name")" != true ]; then
      docker logs "$name" >&2 || true
      die "the server exited while starting"
    fi
    sleep 0.5
  done
  docker logs "$name" >&2 || true
  die "the server did not answer within 150 seconds"
}

# check_reporting refuses a server that has not switched usage reporting off.
check_reporting() {
  local got
  got="$(docker exec "$name" dolt config --global --get metrics.disabled 2>/dev/null || true)"
  [ "$got" = true ] || die "usage reporting is not switched off in the container (metrics.disabled is '$got')"
}

stop() {
  command -v docker >/dev/null || die "docker is not installed"
  if docker container inspect "$name" >/dev/null 2>&1; then
    # A stop signal makes the server shut down cleanly; allow it time to flush.
    docker stop --time 120 "$name" >/dev/null
    docker rm "$name" >/dev/null
  fi
}

run_sql() {
  command -v docker >/dev/null || die "docker is not installed"
  local -a tty=(-i)
  if [ -t 0 ] && [ -t 1 ]; then tty=(-it); fi
  exec docker exec "${tty[@]}" "$name" dolt sql "$@"
}

case "${1:-}" in
  start) [ $# -eq 1 ] || die "start takes no arguments"; start ;;
  stop) [ $# -eq 1 ] || die "stop takes no arguments"; stop ;;
  sql) shift; run_sql "$@" ;;
  image) [ $# -eq 1 ] || die "image takes no arguments"; echo "$image" ;;
  *) die "usage: dolt-server.sh start | stop | sql [dolt sql arguments...] | image" ;;
esac
