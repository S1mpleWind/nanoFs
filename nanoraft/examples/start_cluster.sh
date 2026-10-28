#!/usr/bin/env bash
# Start the 3-node nanoraftd cluster for the KV demo (kill the coder,
# attach a new window). Storage, logs and pids live under DATA, so the cluster
# outlives this script and a restarted node recovers from its log.
# Companion: stop_cluster.sh.
#
# usage: BUILD=<build-dir> nanoraft/examples/start_cluster.sh [DATA]
#        DATA defaults to /tmp/nanoraft-demo
set -u

BUILD="${BUILD:-build}"
RAFTD="$BUILD/nanoraft/nanoraftd"
DATA="${1:-/tmp/nanoraft-demo}"
PEERS="127.0.0.1:9000,127.0.0.1:9001,127.0.0.1:9002"

[ -x "$RAFTD" ] || { echo "missing $RAFTD (build first, see docs/lab3/lab3-tasks.md)"; exit 2; }
if [ -f "$DATA/pids" ] && kill -0 $(cat "$DATA/pids") 2>/dev/null; then
  echo "cluster already running; stop it first: nanoraft/examples/stop_cluster.sh $DATA"
  exit 1
fi

mkdir -p "$DATA"
: > "$DATA/pids"
for i in 0 1 2; do
  "$RAFTD" --id "$i" --peers "$PEERS" --port "900$i" --storage "$DATA/n$i.bin" \
    > "$DATA/n$i.log" 2>&1 &
  echo $! >> "$DATA/pids"
  echo "node $i: pid $! (log: $DATA/n$i.log)"
done
sleep 2   # election

# Point nanoCoder at the cluster: merge a "raft" key into the config in the
# current directory (other keys — model/base_url/api_key — are kept).
python3 - "$PEERS" <<'EOF'
import json, os, sys
path = "nanocoder.config.json"
cfg = json.load(open(path)) if os.path.exists(path) else {}
cfg["raft"] = sys.argv[1]
json.dump(cfg, open(path, "w"), indent=2)
EOF

echo "cluster up: $PEERS"
echo "wrote \"raft\" into ./nanocoder.config.json (other keys kept)"
echo "chat: uv run python -m nanocoder.tui --session S1"
