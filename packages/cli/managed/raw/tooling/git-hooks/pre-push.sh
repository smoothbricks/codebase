#!/usr/bin/env bash
# macOS-only Linux compile gate. Linux `nx lint` already compiles that cfg arm.
# Nx caches cargo-lint-cross; a hit must not enter the 0.4 GiB linux-cross shell.

cd "$(git rev-parse --show-toplevel)"
TOOLING="$PWD/tooling"

export PATH="$("$TOOLING/direnv/repo-path")"

set -e -o pipefail

case "$(uname -s)" in
  Darwin) ;;
  *) exit 0 ;;
esac

# Cache hit never invokes cargo, so the missing cross gcc does not matter.
# Cache miss without the compiler fails the target; check:linux enters linux-cross.
if nx run-many -t cargo-lint-cross; then
  exit 0
fi

bun run check:linux
