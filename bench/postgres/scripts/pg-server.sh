#!/usr/bin/env bash
# pg-server.sh runs the Postgres server the Postgres baseline is tested and measured
# against, in a container, with the settings of settings.conf. It is the one way the
# server is started, on a laptop and on CI, so that one code path is tested.
#
# usage: pg-server.sh start | stop | psql [psql arguments...]
#
#   start  pull the image if it is not there, start the container, and wait until
#          the server accepts connections with the settings in force
#   stop   stop the container (a clean shutdown) and remove it; the data stays
#   psql   run psql in the container, as the superuser, stopping at the first error
#
# The server has no password: it listens on loopback only (a laptop) or on a Unix
# socket (CI), and trusts whoever can reach it.
#
# Environment:
#   TOPOSHIFT_PG_DATA        the data directory (default ~/spikebench/pg/17); created
#                            if absent. Never put it inside a git work tree.
#   TOPOSHIFT_PG_SOCKET_DIR  if set, the server listens on a Unix socket in this
#                            directory and on no port, as on CI. Otherwise it is
#                            published on 127.0.0.1:TOPOSHIFT_PG_PORT.
#   TOPOSHIFT_PG_PORT        the port on 127.0.0.1 (default 55432)
#   TOPOSHIFT_PG_NAME        the container's name (default toposhift-pg)
set -euo pipefail

# The image: the tag says what it is, the digest of the multi-architecture index is
# what is pulled. Never a tag alone.
image='postgres:17.11-trixie@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3'

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
settings="$here/settings.conf"
name="${TOPOSHIFT_PG_NAME:-toposhift-pg}"
port="${TOPOSHIFT_PG_PORT:-55432}"
data="${TOPOSHIFT_PG_DATA:-$HOME/spikebench/pg/17}"
socket="${TOPOSHIFT_PG_SOCKET_DIR:-}"

die() {
  echo "pg-server.sh: $*" >&2
  exit 1
}

# trim sets REPLY to its argument without leading and trailing blanks.
trim() {
  REPLY="$1"
  REPLY="${REPLY#"${REPLY%%[![:space:]]*}"}"
  REPLY="${REPLY%"${REPLY##*[![:space:]]}"}"
}

# server_flags prints one -c flag per setting of settings.conf, one word per line.
server_flags() {
  local line body key value
  while IFS= read -r line || [ -n "$line" ]; do
    trim "${line%%#*}"
    body="$REPLY"
    [ -n "$body" ] || continue
    case "$body" in
      *=*) ;;
      *) die "$settings: '$body' is not name = value" ;;
    esac
    trim "${body%%=*}"
    key="$REPLY"
    trim "${body#*=}"
    value="$REPLY"
    [ -n "$key" ] && [ -n "$value" ] || die "$settings: '$body' has an empty name or value"
    printf '%s\n%s=%s\n' -c "$key" "$value"
  done <"$settings"
}

start() {
  command -v docker >/dev/null || die "docker is not installed"
  case "$data" in
    /*) ;;
    *) die "TOPOSHIFT_PG_DATA must be an absolute path, not '$data'" ;;
  esac
  case "$socket" in
    '' | /*) ;;
    *) die "TOPOSHIFT_PG_SOCKET_DIR must be an absolute path, not '$socket'" ;;
  esac
  if docker container inspect "$name" >/dev/null 2>&1; then
    die "the container $name already exists; run 'pg-server.sh stop' first"
  fi
  # Pull only what is not already there, so that a laptop that has the image never
  # reaches for the network.
  if ! docker image inspect "$image" >/dev/null 2>&1; then
    docker pull --quiet "$image" >/dev/null
  fi

  local fresh=0
  [ -f "$data/PG_VERSION" ] || fresh=1
  mkdir -p "$data"

  local -a flags=()
  while IFS= read -r word; do
    flags+=("$word")
  done < <(server_flags)

  # How the server is reached: options of docker run, and the address it listens on.
  local -a reach listen
  if [ -n "$socket" ]; then
    mkdir -p "$socket"
    # The server runs as a user of its own, which must be able to create its socket.
    chmod 0777 "$socket"
    reach=(-v "$socket:/var/run/postgresql")
    listen=(-c "listen_addresses=")
  else
    reach=(-p "127.0.0.1:$port:5432")
    listen=(-c "listen_addresses=*")
  fi

  # The settings follow 'postgres', so the image's entrypoint runs the server with
  # them (and starts its temporary initialisation server with them too).
  docker run --detach --name "$name" --shm-size=1g \
    -e POSTGRES_HOST_AUTH_METHOD=trust \
    -v "$data:/var/lib/postgresql/data" \
    "${reach[@]}" \
    "$image" postgres "${flags[@]}" "${listen[@]}" >/dev/null

  # The entrypoint starts a temporary server to initialise a new data directory and
  # then the real one, and each says it is ready: wait for the last.
  local want=1 i ready
  [ "$fresh" = 1 ] && want=2
  for ((i = 0; i < 300; i++)); do
    ready="$(docker logs "$name" 2>&1 | grep -c 'database system is ready to accept connections' || true)"
    if [ "$ready" -ge "$want" ] && docker exec "$name" pg_isready --quiet --username postgres; then
      return 0
    fi
    if [ "$(docker container inspect --format '{{.State.Running}}' "$name")" != true ]; then
      docker logs "$name" >&2 || true
      die "the server exited while starting"
    fi
    sleep 0.5
  done
  docker logs "$name" >&2 || true
  die "the server did not accept connections within 150 seconds"
}

stop() {
  command -v docker >/dev/null || die "docker is not installed"
  if docker container inspect "$name" >/dev/null 2>&1; then
    # A stop signal makes the server shut down cleanly; allow it time to checkpoint.
    docker stop --time 120 "$name" >/dev/null
    docker rm "$name" >/dev/null
  fi
}

run_psql() {
  command -v docker >/dev/null || die "docker is not installed"
  local -a tty=(-i)
  if [ -t 0 ] && [ -t 1 ]; then tty=(-it); fi
  exec docker exec "${tty[@]}" "$name" psql --username postgres --set ON_ERROR_STOP=1 "$@"
}

case "${1:-}" in
  start) [ $# -eq 1 ] || die "start takes no arguments"; start ;;
  stop) [ $# -eq 1 ] || die "stop takes no arguments"; stop ;;
  psql) shift; run_psql "$@" ;;
  *) die "usage: pg-server.sh start | stop | psql [psql arguments...]" ;;
esac
