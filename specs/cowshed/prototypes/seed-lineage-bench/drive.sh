#!/bin/sh
# Run the seed-lineage prototype end to end. Wrap it in whatever keeps the host otherwise quiet.
# The recorded results/ came from one run at load ~20-30 on an 18-core host.
set -eu
P=${PROTO_ROOT:?set PROTO_ROOT to a scratch directory on the volume that holds cowshed images}
HERE=$(cd "$(dirname "$0")" && pwd)
cc -O2 -o "$P/xtool" "$HERE/../image-format-bench/xtool.c"
cd "$HERE"
python3 lineage.py
python3 large.py
python3 rewrite.py
rm -f "$P"/seed_*.asif "$P/xtool"
