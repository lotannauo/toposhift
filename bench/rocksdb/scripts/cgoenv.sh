#!/bin/sh
# cgoenv.sh runs a command with the cgo settings the RocksDB module is built with:
# cgo on, and RocksDB's headers and library under ROCKSDB_PREFIX (default
# /opt/homebrew, where the pinned Homebrew library is). CGO_CFLAGS and CGO_LDFLAGS
# already in the environment win, so a CI job that builds RocksDB elsewhere sets them.
#
# The link flags are given here, not by the binding (which is built with the
# grocksdb_no_link tag): its default asks for -lstdc++, which macOS's SDK does not
# have. On macOS the shared library brings its own dependencies; elsewhere the
# library is static and its compression libraries and C++ runtime are named.
#
# usage: scripts/cgoenv.sh <command> [args...]
set -eu
prefix="${ROCKSDB_PREFIX:-/opt/homebrew}"
if [ "$(uname -s)" = Darwin ]; then
  ldflags="-L$prefix/lib -lrocksdb"
else
  ldflags="-L$prefix/lib -lrocksdb -lstdc++ -lm -ldl -lpthread -lz -lsnappy -llz4 -lzstd"
fi
export CGO_ENABLED=1
export CGO_CFLAGS="${CGO_CFLAGS:--I$prefix/include}"
export CGO_LDFLAGS="${CGO_LDFLAGS:-$ldflags}"
exec "$@"
