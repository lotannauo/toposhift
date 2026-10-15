#!/usr/bin/env bash
# build-rocksdb.sh builds the pinned RocksDB from source, as a static library, and
# installs it with its headers under a prefix, for the RocksDB module (layout L on
# RocksDB) to be built and tested against. CI runs it on Linux (x86-64 and arm64)
# when its cache holds no library built by this exact script; it also builds on
# macOS.
#
# usage: build-rocksdb.sh [--tarball FILE] [--jobs N] PREFIX
#        build-rocksdb.sh --version     prints the pinned RocksDB version
#
#   PREFIX          where the library is installed. It must not exist yet, and its
#                   parent directory must. Nothing is written there unless the
#                   whole build succeeds: the result is assembled elsewhere and
#                   moved into place as the last step.
#   --tarball FILE  use this copy of the pinned source archive instead of
#                   downloading it. It is checked against the same sha256.
#   --jobs N        compile with N jobs (1 to 64). The default is the machine's
#                   processor count, at most 8.
#
# What PREFIX holds afterwards:
#
#   PREFIX/include/rocksdb/...          the public headers (c.h is the one cgo uses)
#   PREFIX/lib/librocksdb.a             the static library
#   PREFIX/lib/rocksdb-build.stamp      how the library was built (below)
#   PREFIX/lib/cmake/rocksdb/...        CMake's package files (unused here)
#   PREFIX/lib/pkgconfig/rocksdb.pc     pkg-config's file (unused here)
#
# The stamp is the record of the build: one key=value per line, in a fixed order,
# with no time in it, so two builds by this script of the same source with the
# same compiler write the same stamp. Its keys are rocksdb_version,
# source_sha256, script_sha256, build_type, portable, rtti, shared, compression,
# bzip2, gflags, liburing, jemalloc, cxx, cmake and host.
#
# Choices, each recorded in the stamp:
#
#   - The source is the release archive of the pinned tag, checked against a
#     pinned sha256 before anything is unpacked. A different archive is refused.
#   - portable=1: compiled for the architecture's baseline instruction set, not
#     the build machine's (-march=native). A cached library is then safe on every
#     runner of that architecture, whichever processor model it lands on, and it
#     matches the Go side, which is compiled for the baseline too. Homebrew's
#     library is built the same way.
#   - rtti on, as Homebrew's library is, so CI and a Mac link the same kind of
#     library.
#   - Compression: snappy, lz4, zstd and zlib, from the system (Ubuntu's -dev
#     packages on CI, Homebrew on a Mac). bzip2 is off: no layout selects it, and
#     scripts/cgoenv.sh's Linux link line does not name it. Homebrew's library has
#     bzip2 and gflags on, so it links two libraries this one does not; what a
#     database writes does not depend on either.
#   - gflags, io_uring and jemalloc are off: gflags serves only RocksDB's own
#     tools, which are not built; io_uring and jemalloc would change the link line
#     and the allocator, and the comparison uses the system allocator.
#   - Static only, and position-independent, so cgo can link it into any binary.
#   - Warnings are not errors, so a newer compiler's new warning does not stop a
#     build. They do not change the code compiled.
#
# On macOS the build works, but scripts/cgoenv.sh's macOS link line expects
# Homebrew's shared library; to use a library from this script there, set
# CGO_LDFLAGS yourself (the static library needs -lc++ and the four compression
# libraries).
#
# The CMAKE environment variable names the cmake to use (default: cmake on PATH).
#
# The script writes only to a scratch directory it makes with mktemp, which it
# removes when it exits, and, at the end, to PREFIX.
set -euo pipefail

readonly version=11.8.1
readonly url="https://github.com/facebook/rocksdb/archive/refs/tags/v${version}.tar.gz"
readonly sha256=618d9726a7cb1cf4ce034f4cdca49de98aa64867dda06b91371a791ae8921aff
readonly max_jobs=8
# The baseline instruction set of the architecture (see the header).
readonly portable=1

die() {
  echo "build-rocksdb.sh: $*" >&2
  exit 1
}

usage() {
  echo "usage: build-rocksdb.sh [--tarball FILE] [--jobs N] PREFIX" >&2
  echo "       build-rocksdb.sh --version" >&2
  exit 2
}

sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum -- "$1" | awk '{ print $1 }'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 -- "$1" | awk '{ print $1 }'
  else
    die "neither sha256sum nor shasum is installed"
  fi
}

cores() {
  local n=""
  if command -v nproc >/dev/null 2>&1; then
    n="$(nproc)"
  elif command -v sysctl >/dev/null 2>&1; then
    n="$(sysctl -n hw.ncpu)"
  fi
  case "$n" in
    '' | *[!0-9]*) n=1 ;;
  esac
  if [ "$n" -lt 1 ]; then n=1; fi
  if [ "$n" -gt "$max_jobs" ]; then n="$max_jobs"; fi
  echo "$n"
}

tarball=""
jobs=""
prefix=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --version)
      [ "$#" -eq 1 ] || usage
      echo "$version"
      exit 0
      ;;
    --tarball)
      [ "$#" -ge 2 ] || usage
      tarball="$2"
      shift 2
      ;;
    --jobs)
      [ "$#" -ge 2 ] || usage
      jobs="$2"
      shift 2
      ;;
    -h | --help) usage ;;
    -*) echo "build-rocksdb.sh: unknown option $1" >&2; usage ;;
    *)
      [ -z "$prefix" ] || usage
      prefix="$1"
      shift
      ;;
  esac
done
[ -n "$prefix" ] || usage

if [ -n "$jobs" ]; then
  case "$jobs" in
    '' | *[!0-9]*) die "--jobs must be a whole number from 1 to 64, not '$jobs'" ;;
  esac
  if [ "$jobs" -lt 1 ] || [ "$jobs" -gt 64 ]; then
    die "--jobs must be a whole number from 1 to 64, not '$jobs'"
  fi
else
  jobs="$(cores)"
fi

# The prefix is made absolute, because CMake records it in the files it installs
# and the staged copy is found by appending it to the scratch directory.
parent="$(dirname -- "$prefix")"
[ -d "$parent" ] || die "the parent of the prefix, $parent, does not exist"
parent="$(cd -- "$parent" && pwd -P)"
prefix="$parent/$(basename -- "$prefix")"
case "$(basename -- "$prefix")" in
  . | .. | /) die "the prefix must name a new directory, not $prefix" ;;
esac
if [ -e "$prefix" ] || [ -L "$prefix" ]; then
  die "$prefix already exists; this script only installs into a new directory"
fi

if [ -n "$tarball" ]; then
  [ -f "$tarball" ] || die "the tarball $tarball is not a file"
  tarball_dir="$(dirname -- "$tarball")"
  tarball_dir="$(cd -- "$tarball_dir" && pwd -P)"
  tarball="$tarball_dir/$(basename -- "$tarball")"
fi

cmake="${CMAKE:-cmake}"
for tool in "$cmake" tar awk; do
  command -v "$tool" >/dev/null 2>&1 || die "$tool is not installed"
done
if [ -z "$tarball" ]; then
  command -v curl >/dev/null 2>&1 || die "curl is not installed (or pass --tarball)"
fi

script_path="${BASH_SOURCE[0]}"
script_sha256="$(sha256_of "$script_path")"

work=""
cleanup() {
  if [ -n "$work" ] && [ -d "$work" ]; then
    rm -rf -- "$work"
  fi
}
trap cleanup EXIT
work="$(mktemp -d "${TMPDIR:-/tmp}/build-rocksdb.XXXXXX")"

archive="$work/rocksdb-${version}.tar.gz"
if [ -n "$tarball" ]; then
  cp -- "$tarball" "$archive"
else
  echo "build-rocksdb.sh: downloading $url"
  curl --fail --silent --show-error --location \
    --proto '=https' --proto-redir '=https' --tlsv1.2 \
    --retry 3 --retry-delay 5 \
    --output "$archive" -- "$url"
fi

got="$(sha256_of "$archive")"
if [ "$got" != "$sha256" ]; then
  die "the RocksDB $version archive has sha256 $got, not the pinned $sha256; refusing it"
fi
echo "build-rocksdb.sh: the archive matches the pinned sha256 $sha256"

src="$work/src"
build="$work/build"
stage="$work/stage"
mkdir -- "$src" "$build" "$stage"
tar -xzf "$archive" -C "$src" --strip-components=1
[ -f "$src/CMakeLists.txt" ] || die "the archive holds no CMakeLists.txt at its top"

"$cmake" -S "$src" -B "$build" \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX="$prefix" \
  -DCMAKE_INSTALL_LIBDIR=lib \
  -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
  -DPORTABLE="$portable" \
  -DUSE_RTTI=ON \
  -DFAIL_ON_WARNINGS=OFF \
  -DROCKSDB_BUILD_SHARED=OFF \
  -DWITH_SNAPPY=ON \
  -DWITH_LZ4=ON \
  -DWITH_ZSTD=ON \
  -DWITH_ZLIB=ON \
  -DWITH_BZ2=OFF \
  -DWITH_GFLAGS=OFF \
  -DWITH_LIBURING=OFF \
  -DWITH_JEMALLOC=OFF \
  -DWITH_TESTS=OFF \
  -DWITH_BENCHMARK_TOOLS=OFF \
  -DWITH_TOOLS=OFF \
  -DWITH_CORE_TOOLS=OFF \
  -DWITH_TRACE_TOOLS=OFF

echo "build-rocksdb.sh: compiling with $jobs jobs"
"$cmake" --build "$build" --parallel "$jobs"
DESTDIR="$stage" "$cmake" --install "$build"

out="$stage$prefix"
[ -f "$out/lib/librocksdb.a" ] || die "the build installed no lib/librocksdb.a"
[ -f "$out/include/rocksdb/c.h" ] || die "the build installed no include/rocksdb/c.h"
installed="$(awk '
  $1 == "#define" && $2 == "ROCKSDB_MAJOR" { major = $3 }
  $1 == "#define" && $2 == "ROCKSDB_MINOR" { minor = $3 }
  $1 == "#define" && $2 == "ROCKSDB_PATCH" { patch = $3 }
  END { print major "." minor "." patch }
' "$out/include/rocksdb/version.h")"
[ "$installed" = "$version" ] || die "the installed headers are RocksDB $installed, not $version"

cxx="$(awk -F= '/^CMAKE_CXX_COMPILER:/ { print $2; exit }' "$build/CMakeCache.txt")"
[ -n "$cxx" ] || die "CMake recorded no C++ compiler"
# Each is its own assignment, so a command that fails stops the script.
cxx_version="$("$cxx" --version | awk 'NR == 1')"
cmake_version="$("$cmake" --version | awk 'NR == 1')"
host_os="$(uname -s)"
host_arch="$(uname -m)"
{
  echo "rocksdb_version=$version"
  echo "source_sha256=$sha256"
  echo "script_sha256=$script_sha256"
  echo "build_type=Release"
  echo "portable=$portable"
  echo "rtti=on"
  echo "shared=off"
  echo "compression=snappy,lz4,zstd,zlib"
  echo "bzip2=off"
  echo "gflags=off"
  echo "liburing=off"
  echo "jemalloc=off"
  echo "cxx=$cxx $cxx_version"
  echo "cmake=$cmake_version"
  echo "host=$host_os $host_arch"
} > "$out/lib/rocksdb-build.stamp"

# Checked again, because the build took a while: mv would move the library
# into an existing directory rather than replace it.
if [ -e "$prefix" ] || [ -L "$prefix" ]; then
  die "$prefix appeared during the build; leaving it alone"
fi
mv -- "$out" "$prefix"
echo "build-rocksdb.sh: installed RocksDB $version under $prefix"
cat -- "$prefix/lib/rocksdb-build.stamp"
