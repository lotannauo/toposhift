#!/usr/bin/env bash
# vm-server.sh runs the VictoriaMetrics server the metrics-store baseline is tested and
# measured against, in a container, with the flags of flags.conf. It is the one way the
# server is started, on a laptop and on CI, so that one code path is tested.
#
# usage: vm-server.sh start | stop | url | flags
#
#   start  pull the image if it is not there, start the container, and wait until the
#          server answers its health check
#   stop   stop the container (a clean shutdown) and remove it; the data stays
#   url    print the address the server is reached at
#   flags  print the flags of flags.conf as the server is given them, one per line
#
# The server has no authentication: it is published on the loopback of the host only.
#
# Environment:
#   TOPOSHIFT_VM_DATA    the data directory (default ~/spikebench/vm/data); created if
#                        absent. Never put it inside a git work tree.
#   TOPOSHIFT_VM_PORT    the port on 127.0.0.1 (default 8428)
#   TOPOSHIFT_VM_NAME    the container's name (default toposhift-vm)
#   TOPOSHIFT_VM_MEMORY  if set, a hard memory limit for the container (for example 4g),
#                        so that a laptop that shares its container runtime with other
#                        work never gives the server more than it can spare. The server
#                        sizes its caches from the limit it sees, so a run with a limit
#                        and a run without one are different regimes: CI sets none.
set -euo pipefail

# The image: the tag says what it is, the digest of the multi-architecture index is what
# is pulled. Never a tag alone.
image='victoriametrics/victoria-metrics:v1.153.0@sha256:5eff7af5341e401471002f58d106d399e614a62f3d240f2dbc21901e49eed5dd'

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
flagfile="$here/flags.conf"
name="${TOPOSHIFT_VM_NAME:-toposhift-vm}"
port="${TOPOSHIFT_VM_PORT:-8428}"
data="${TOPOSHIFT_VM_DATA:-$HOME/spikebench/vm/data}"
memory="${TOPOSHIFT_VM_MEMORY:-}"

die() {
  echo "vm-server.sh: $*" >&2
  exit 1
}

# trim sets REPLY to its argument without leading and trailing blanks.
trim() {
  REPLY="$1"
  REPLY="${REPLY#"${REPLY%%[![:space:]]*}"}"
  REPLY="${REPLY%"${REPLY##*[![:space:]]}"}"
}

# server_flags prints one -name=value flag per line of flags.conf, one word per line.
server_flags() {
  local line body key value
  while IFS= read -r line || [ -n "$line" ]; do
    trim "${line%%#*}"
    body="$REPLY"
    [ -n "$body" ] || continue
    case "$body" in
      *=*) ;;
      *) die "$flagfile: '$body' is not name = value" ;;
    esac
    trim "${body%%=*}"
    key="$REPLY"
    trim "${body#*=}"
    value="$REPLY"
    [ -n "$key" ] && [ -n "$value" ] || die "$flagfile: '$body' has an empty name or value"
    printf -- '-%s=%s\n' "$key" "$value"
  done <"$flagfile"
}

start() {
  command -v docker >/dev/null || die "docker is not installed"
  case "$data" in
    /*) ;;
    *) die "TOPOSHIFT_VM_DATA must be an absolute path, not '$data'" ;;
  esac
  case "$port" in
    '' | *[!0-9]*) die "TOPOSHIFT_VM_PORT must be a number, not '$port'" ;;
  esac
  if docker container inspect "$name" >/dev/null 2>&1; then
    die "the container $name already exists; run 'vm-server.sh stop' first"
  fi
  # Pull only what is not already there, so that a laptop that has the image never
  # reaches for the network, and run with --pull=never so that nothing else can.
  if ! docker image inspect "$image" >/dev/null 2>&1; then
    docker pull --quiet "$image" >/dev/null
  fi
  mkdir -p "$data"

  local -a flags=()
  while IFS= read -r word; do
    flags+=("$word")
  done < <(server_flags)

  local -a limit=()
  if [ -n "$memory" ]; then
    # The same size for memory and swap: no swap.
    limit=(--memory "$memory" --memory-swap "$memory")
  fi

  docker run --detach --pull=never --name "$name" \
    ${limit[@]+"${limit[@]}"} \
    -v "$data:/victoria-metrics-data" \
    -p "127.0.0.1:$port:8428" \
    "$image" \
    -storageDataPath=/victoria-metrics-data \
    -httpListenAddr=:8428 \
    "${flags[@]}" >/dev/null

  local i
  for ((i = 0; i < 240; i++)); do
    if curl --silent --fail --max-time 2 "http://127.0.0.1:$port/health" >/dev/null 2>&1; then
      return 0
    fi
    if [ "$(docker container inspect --format '{{.State.Running}}' "$name")" != true ]; then
      docker logs "$name" >&2 || true
      die "the server exited while starting"
    fi
    sleep 0.5
  done
  docker logs "$name" >&2 || true
  die "the server did not answer its health check within 120 seconds"
}

stop() {
  command -v docker >/dev/null || die "docker is not installed"
  if docker container inspect "$name" >/dev/null 2>&1; then
    # A stop signal makes the server flush and shut down cleanly; allow it time.
    docker stop --time 120 "$name" >/dev/null
    docker rm "$name" >/dev/null
  fi
}

case "${1:-}" in
  start) [ $# -eq 1 ] || die "start takes no arguments"; start ;;
  stop) [ $# -eq 1 ] || die "stop takes no arguments"; stop ;;
  url) [ $# -eq 1 ] || die "url takes no arguments"; echo "http://127.0.0.1:$port" ;;
  flags) [ $# -eq 1 ] || die "flags takes no arguments"; server_flags ;;
  *) die "usage: vm-server.sh start | stop | url | flags" ;;
esac
