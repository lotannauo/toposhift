#!/usr/bin/env bash
# Installs the pinned Node for the arm64 runner and puts it first on the PATH of the
# steps after it. The tarball is downloaded from nodejs.org and checked against the
# sha256 that nodejs.org publishes in SHASUMS256.txt for it, written here as a
# literal. The Node of the runner image is never used.
#
#   bash install-node.sh
#
# It reads RUNNER_TEMP and GITHUB_PATH, which a runner sets.
set -euo pipefail

version="v24.21.0"
tarball="node-${version}-linux-arm64.tar.xz"
sha256="6ad1325edbdb5649c379b75a237147a666c95d4f9ae8d340fef2d1575d289ad2"

dir="${RUNNER_TEMP:?}/node"
mkdir -p "$dir"
curl --fail --silent --show-error --location --retry 3 --output "$dir/$tarball" "https://nodejs.org/dist/${version}/${tarball}"
echo "${sha256}  ${dir}/${tarball}" | sha256sum --check --strict -
tar --extract --xz --file "$dir/$tarball" --directory "$dir"
bin="$dir/node-${version}-linux-arm64/bin"
got="$("$bin/node" --version)"
if [ "$got" != "$version" ]; then
  echo "::error::the unpacked node is $got, want $version" >&2
  exit 1
fi
echo "$bin" >>"${GITHUB_PATH:?}"
