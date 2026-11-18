#!/usr/bin/env bash
# Integration smoke for nanoraftd: start a 3-node cluster on localhost, exercise
# kv_put/kv_get/kv_scan (with leader redirect), then kill a node and confirm the
# cluster still commits. Run manually — not part of ctest.
#
# usage: BUILD=<build-dir> nanoraft/test/kv_smoke.sh
set -u
set -o pipefail

BUILD="${BUILD:-build}"
D="$BUILD/nanoraft"
RAFTD="$D/nanoraftd"
KVCTL="$D/nanoraft_kvctl"
[ -x "$RAFTD" ] || { echo "missing $RAFTD (build nanoraftd first)"; exit 2; }
[ -x "$KVCTL" ] || { echo "missing $KVCTL (build nanoraft_kvctl first)"; exit 2; }

PEERS="127.0.0.1:19001,127.0.0.1:19002,127.0.0.1:19003"
TMP="$(mktemp -d)"
PIDS=()
STEP=0
CURRENT_STEP="initialization"
LAST_CMD=""
LAST_STATUS=0
LAST_STDOUT=""
LAST_STDERR=""

cleanup() {
  for p in "${PIDS[@]:-}"; do
    kill "$p" 2>/dev/null || true
    wait "$p" 2>/dev/null || true
  done
  rm -rf "$TMP"
}
trap cleanup EXIT

start_node() {  # id port
  "$RAFTD" --id "$1" --peers "$PEERS" --port "$2" --storage "$TMP/n$1.bin" >"$TMP/n$1.log" 2>&1 &
  PIDS[$1]=$!
}

begin_step() {
  STEP=$((STEP + 1))
  CURRENT_STEP="$1"
  LAST_CMD=""
  LAST_STATUS=0
  LAST_STDOUT=""
  LAST_STDERR=""
  printf '\n[%02d] BEGIN %s\n' "$STEP" "$CURRENT_STEP"
}

pass_step() {
  printf '[%02d] PASS  %s\n' "$STEP" "$CURRENT_STEP"
}

# Run kvctl while retaining both streams. Keeping the last command and its
# output makes a failure point to the exact RPC operation instead of only
# saying that a generic smoke phase failed.
run_kvctl() {
  printf -v LAST_CMD '%q ' "$@"
  LAST_CMD="${LAST_CMD% }"
  "$@" >"$TMP/last.stdout" 2>"$TMP/last.stderr"
  LAST_STATUS=$?
  LAST_STDOUT="$(cat "$TMP/last.stdout")"
  LAST_STDERR="$(cat "$TMP/last.stderr")"
  return "$LAST_STATUS"
}

fail() {
  echo
  echo "FAIL at step [$STEP] $CURRENT_STEP"
  echo "reason: $1"
  if [ -n "$LAST_CMD" ]; then
    echo "command: $LAST_CMD"
    echo "command exit: $LAST_STATUS"
    echo "command stdout: ${LAST_STDOUT:-<empty>}"
    echo "command stderr: ${LAST_STDERR:-<empty>}"
  fi
  echo "--- node logs ---"
  for log in "$TMP"/n*.log; do
    [ -f "$log" ] || continue
    echo "--- $(basename "$log") ---"
    cat "$log"
  done
  exit 1
}

begin_step "start 3-node cluster (allow election to settle)"
start_node 0 19001; start_node 1 19002; start_node 2 19003
sleep 2   # election
for i in 0 1 2; do
  kill -0 "${PIDS[$i]}" 2>/dev/null || fail "node $i exited before the cluster became ready"
done
pass_step

begin_step "put k1=v1"
run_kvctl "$KVCTL" --peers "$PEERS" --client c1 --seq 1 put k1 v1 \
  || fail "put k1 command failed"
pass_step

begin_step "get k1 and verify v1"
run_kvctl "$KVCTL" --peers "$PEERS" get k1 || fail "get k1 command failed"
got="$LAST_STDOUT"
[ "$got" = "v1" ] || fail "expected get k1 to return v1, got '$got'"
echo "result: $got"
pass_step

begin_step "put k2=v2"
run_kvctl "$KVCTL" --peers "$PEERS" --client c1 --seq 2 put k2 v2 \
  || fail "put k2 command failed"
pass_step

begin_step "scan prefix k and verify k1,k2"
run_kvctl "$KVCTL" --peers "$PEERS" scan k || fail "scan k command failed"
rows="$(printf '%s\n' "$LAST_STDOUT" | sort | tr '\n' ' ')"
echo "result: $rows"
[ "$rows" = "k1=v1 k2=v2 " ] || fail "expected scan to return 'k1=v1 k2=v2 ', got '$rows'"
pass_step

begin_step "get k1 through entry point node 0 (leader redirect)"
run_kvctl "$KVCTL" --peers "$PEERS" --start 0 get k1 \
  || fail "get k1 via node 0 command failed"
g="$LAST_STDOUT"
[ "$g" = "v1" ] || fail "node 0 entry returned '$g' (want v1)"
pass_step

begin_step "get k1 through entry point node 1 (leader redirect)"
run_kvctl "$KVCTL" --peers "$PEERS" --start 1 get k1 \
  || fail "get k1 via node 1 command failed"
g="$LAST_STDOUT"
[ "$g" = "v1" ] || fail "node 1 entry returned '$g' (want v1)"
pass_step

begin_step "get k1 through entry point node 2 (leader redirect)"
run_kvctl "$KVCTL" --peers "$PEERS" --start 2 get k1 \
  || fail "get k1 via node 2 command failed"
g="$LAST_STDOUT"
[ "$g" = "v1" ] || fail "node 2 entry returned '$g' (want v1)"
pass_step

begin_step "kill node 0 and wait for majority failover"
kill "${PIDS[0]}" 2>/dev/null || fail "could not kill node 0"
wait "${PIDS[0]}" 2>/dev/null || true
unset 'PIDS[0]'
sleep 2   # re-election if needed
pass_step

begin_step "put k3=v3 after node 0 stopped"
run_kvctl "$KVCTL" --peers "$PEERS" --client c1 --seq 3 put k3 v3 \
  || fail "put k3 after kill command failed"
pass_step

begin_step "get k3 after node 0 stopped and verify v3"
run_kvctl "$KVCTL" --peers "$PEERS" get k3 || fail "get k3 after kill command failed"
g3="$LAST_STDOUT"
[ "$g3" = "v3" ] || fail "expected get k3 to return v3, got '$g3'"
echo "result: $g3"
pass_step

begin_step "get k1 after node 0 stopped and verify v1"
run_kvctl "$KVCTL" --peers "$PEERS" get k1 || fail "get k1 after kill command failed"
g1="$LAST_STDOUT"
[ "$g1" = "v1" ] || fail "expected k1 to remain v1, got '$g1'"
echo "result: $g1"
pass_step

echo
echo "PASS: all $STEP smoke steps completed"
