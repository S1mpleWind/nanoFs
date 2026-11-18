#!/usr/bin/env bash
# Stop the demo cluster started by start_cluster.sh: all nodes, or a single
# node id to demonstrate failover (the surviving majority keeps committing).
# Storage is kept so a restarted cluster recovers the session; rm -rf DATA
# to wipe it.
#
# usage: nanoraft/examples/stop_cluster.sh [DATA] [node-id]
#        DATA defaults to /tmp/nanoraft-demo
set -u

DATA="${1:-/tmp/nanoraft-demo}"
[ -f "$DATA/pids" ] || { echo "no pidfile in $DATA (cluster not running?)"; exit 1; }

if [ $# -ge 2 ]; then
  pid=$(sed -n "$(( $2 + 1 ))p" "$DATA/pids")
  [ -n "$pid" ] || { echo "no node $2 in $DATA/pids"; exit 1; }
  kill "$pid" 2>/dev/null && echo "killed node $2 (pid $pid); the majority carries on"
else
  kill $(cat "$DATA/pids") 2>/dev/null
  rm -f "$DATA/pids"
  echo "cluster stopped (storage kept in $DATA; rm -rf $DATA to wipe)"
fi
