#!/bin/sh
# Regenerates src/wire-fixtures.json, the napi wire corpus cowshed-core serializes (cowshed:wire-fixtures).
#
# cowshed:lint reads the corpus while this runs. Written in place (the render, then biome formatting
# over it), a reader saw a truncated file or the unformatted render. Both steps write into a staging
# directory under tmp/ (git-ignored, so Nx and biome never see it) on the same file system as src/,
# and one rename(2) publishes the finished file: a reader sees the old corpus or the new one.
set -eu
corpus=src/wire-fixtures.json
mkdir -p tmp
staging=$(mktemp -d tmp/wire-fixtures.XXXXXX)
trap 'rm -rf "$staging"' EXIT
COWSHED_WIRE_FIXTURES="$PWD/$staging/rendered.json" cargo --frozen test -p cowshed-napi --quiet the_committed_wire_corpus_is_what_core_serializes
biome format --stdin-file-path="$corpus" <"$staging/rendered.json" >"$staging/wire-fixtures.json"
mv -f "$staging/wire-fixtures.json" "$corpus"
