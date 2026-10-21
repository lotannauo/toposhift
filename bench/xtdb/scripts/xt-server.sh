#!/usr/bin/env bash
# xt-server.sh runs the XTDB node the XTDB baseline is tested and measured against, in a
# container, with the configuration of xtdb.yaml. It is the one way the node is started,
# on a laptop and on CI, so that one code path is tested.
#
# usage: xt-server.sh start | stop | psql [psql arguments...]
#
#   start  pull the image if it is not there, start the container, and wait until
#          the node accepts connections
#   stop   stop the container (a clean shutdown) and remove it; the data stays
#   psql   run the host's psql against the node, stopping at the first error
#
# The node has no password: it is published on loopback only, and trusts whoever can
# reach it.
#
# Environment:
#   TOPOSHIFT_XTDB_DATA         the directory the node keeps its log and storage in
#                               (default ~/spikebench/xtdb/data); created if absent. It
#                               must not be inside a git work tree.
#   TOPOSHIFT_XTDB_PORT         the port of the Postgres wire protocol on 127.0.0.1
#                               (default 55433)
#   TOPOSHIFT_XTDB_HEALTH_PORT  the port of the health endpoint on 127.0.0.1 (default 55480)
#   TOPOSHIFT_XTDB_NAME         the container's name (default toposhift-xtdb)
#   TOPOSHIFT_XTDB_JAVA_OPTS    the JVM options (default: the registered ones below). Only
#                               for a local run of tests that probe behaviour, on a machine
#                               too small for the registered sizes; nothing measured or
#                               registered may come from a node started with another value.
#   TOPOSHIFT_XTDB_CONFIG       the node configuration file (default xtdb.yaml beside this
#                               script), for the same use and under the same rule.
set -euo pipefail

# The image: the tag says what it is, the digest of the multi-architecture index is
# what is pulled. Never a tag alone.
image='ghcr.io/xtdb/xtdb:2.1.0@sha256:15b6aa9d37d1e1a4e21748788db9275a57b87c339e5eb66d86376cf1e6899816'

# The JVM's heap and the memory it may map outside it (the node's buffers and caches),
# which with the JVM's own use is about 9 GB. Registered with the configuration.
registered_jvm_options='-Xmx4g -XX:MaxDirectMemorySize=4g'
jvm_options="${TOPOSHIFT_XTDB_JAVA_OPTS:-$registered_jvm_options}"

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
config="${TOPOSHIFT_XTDB_CONFIG:-$here/xtdb.yaml}"
name="${TOPOSHIFT_XTDB_NAME:-toposhift-xtdb}"
port="${TOPOSHIFT_XTDB_PORT:-55433}"
health_port="${TOPOSHIFT_XTDB_HEALTH_PORT:-55480}"
data="${TOPOSHIFT_XTDB_DATA:-$HOME/spikebench/xtdb/data}"

die() {
  echo "xt-server.sh: $*" >&2
  exit 1
}

# in_work_tree reports whether a path, which may not exist yet, is inside a git work
# tree: it asks about the nearest directory that exists.
in_work_tree() {
  local dir="$1"
  while [ ! -d "$dir" ]; do dir="$(dirname "$dir")"; done
  command -v git >/dev/null && git -C "$dir" rev-parse --is-inside-work-tree >/dev/null 2>&1
}

start() {
  command -v docker >/dev/null || die "docker is not installed"
  command -v curl >/dev/null || die "curl is not installed"
  case "$data" in
    /*) ;;
    *) die "TOPOSHIFT_XTDB_DATA must be an absolute path, not '$data'" ;;
  esac
  if in_work_tree "$data"; then
    die "TOPOSHIFT_XTDB_DATA is inside a git work tree: $data"
  fi
  [ -f "$config" ] || die "$config is missing"
  if [ "$jvm_options" != "$registered_jvm_options" ] || [ "$config" != "$here/xtdb.yaml" ]; then
    echo "xt-server.sh: NOT the registered configuration (JVM options '$jvm_options', config $config): for probes only" >&2
  fi
  if docker container inspect "$name" >/dev/null 2>&1; then
    die "the container $name already exists; run 'xt-server.sh stop' first"
  fi
  # Pull only what is not already there, so that a laptop that has the image never
  # reaches for the network.
  if ! docker image inspect "$image" >/dev/null 2>&1; then
    docker pull --quiet "$image" >/dev/null
  fi

  mkdir -p "$data"
  # The node runs as a user of its own (uid 20000), which must be able to write.
  chmod 0777 "$data"

  docker run --detach --name "$name" \
    -e "JDK_JAVA_OPTIONS=$jvm_options" \
    -v "$data:/var/lib/xtdb" \
    -v "$config:/config/xtdb.yaml:ro" \
    -p "127.0.0.1:$port:5432" \
    -p "127.0.0.1:$health_port:8080" \
    "$image" -f /config/xtdb.yaml >/dev/null

  # Started means it has caught up with its log, which a new node does at once and a
  # node over an old log does after replaying it.
  local i
  for ((i = 0; i < 600; i++)); do
    if curl --silent --fail --max-time 2 "http://127.0.0.1:$health_port/healthz/started" >/dev/null 2>&1; then
      return 0
    fi
    if [ "$(docker container inspect --format '{{.State.Running}}' "$name")" != true ]; then
      docker logs "$name" >&2 || true
      die "the node exited while starting"
    fi
    sleep 0.5
  done
  docker logs "$name" >&2 || true
  die "the node did not start within 300 seconds"
}

stop() {
  command -v docker >/dev/null || die "docker is not installed"
  if docker container inspect "$name" >/dev/null 2>&1; then
    # A stop signal makes the node shut down cleanly; allow it time to finish a block.
    docker stop --time 120 "$name" >/dev/null
    docker rm "$name" >/dev/null
  fi
}

run_psql() {
  command -v psql >/dev/null || die "psql is not installed"
  exec psql --set ON_ERROR_STOP=1 "host=127.0.0.1 port=$port user=xtdb dbname=xtdb" "$@"
}

case "${1:-}" in
  start) [ $# -eq 1 ] || die "start takes no arguments"; start ;;
  stop) [ $# -eq 1 ] || die "stop takes no arguments"; stop ;;
  psql) shift; run_psql "$@" ;;
  *) die "usage: xt-server.sh start | stop | psql [psql arguments...]" ;;
esac
