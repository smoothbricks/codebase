#!/usr/bin/env bash
# macOS-only Linux compile gate. Linux `nx lint` already compiles that cfg arm.
# Nx caches cargo-lint-cross on CARGO_INPUTS; a hit is a prior real
# `cargo clippy --target x86_64-unknown-linux-gnu` and never enters linux-cross.
# A miss falls through to `bun run check:linux`, which populates that cache.

cd "$(git rev-parse --show-toplevel)"
TOOLING="$PWD/tooling"

export PATH="$("$TOOLING/direnv/repo-path")"

set -e -o pipefail

case "$(uname -s)" in
  Darwin) ;;
  *) exit 0 ;;
esac

if nx run-many -t cargo-lint-cross; then
  exit 0
fi

bun run check:linux
