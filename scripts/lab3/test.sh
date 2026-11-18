#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
build=${NANOFS_BUILD_DIR:-"$root/build"}
timeout=${CTEST_TIMEOUT:-120}

cmake -S "$root" -B "$build" \
  -DNANOFS_STAGE_B=ON \
  -DNANOFS_LAB2B=ON \
  -DNANOFS_REQUIRE_FUSE=OFF \
  -DNANORAFT_LAB3=ON
cmake --build "$build" -j4
ctest --test-dir "$build" \
  -R '^(Raft|Kv|FileStorage)' \
  --output-on-failure --no-tests=error --timeout "$timeout"

# CTest exercises the in-process Raft suites. This smoke test also checks the
# real nanoraftd processes, RPC redirects and a majority surviving one node.
BUILD="$build" bash "$root/nanoraft/test/kv_smoke.sh"
