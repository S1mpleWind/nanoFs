#!/bin/sh
set -eu

[ "$#" -eq 1 ] || { echo "usage: $0 STUDENT_ID" >&2; exit 2; }
case "$1" in *[!A-Za-z0-9_-]*|'') echo "invalid student id" >&2; exit 2;; esac

student_id="$1"
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$root"
archive="handin-lab3-$student_id.tgz"
[ ! -e "$archive" ] || {
  echo "archive exists: $archive (rename it before resubmitting)" >&2
  exit 1
}

tar -czf "$archive" nanoraft/src/raft.cc
printf 'Created %s\n' "$root/$archive"
