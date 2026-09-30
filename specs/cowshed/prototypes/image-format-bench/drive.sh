#!/bin/sh
# Run every candidate x variant sequentially. Wrap the whole script in whatever keeps the host otherwise quiet.
set -u
P=${PROTO_ROOT:?set PROTO_ROOT}
HERE=$(cd "$(dirname "$0")" && pwd)
for v in clones noclones; do
  for f in A B C; do
    echo "=== $f $v $(date +%T)"
    python3 "$HERE/run.py" "$f" "$v" || echo "FAILED $f $v rc=$?"
  done
done
for step in "fmt.py A" "fmt.py B" "fmt.py C" "iocmp.py" "cap.py A" "cap.py C" "run.py A inclone" "createfrom.py A"; do
  echo "=== $step $(date +%T)"
  # shellcheck disable=SC2086
  python3 "$HERE"/$step || echo "FAILED $step rc=$?"
done
echo "=== done $(date +%T)"
