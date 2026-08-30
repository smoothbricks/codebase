#!/usr/bin/env bash
# macOS-only Linux compile gate. Linux `nx lint` already compiles that cfg arm.
#
# This hook never enters linux-cross: devenv + clippy is minutes, which is what
# `bun run check:linux` is for. Nx cache hits never execute cargo, so they are
# safe without the cross gcc. A miss fails immediately rather than compiling.

cd "$(git rev-parse --show-toplevel)"
TOOLING="$PWD/tooling"

export PATH="$("$TOOLING/direnv/repo-path")"

set -e -o pipefail

case "$(uname -s)" in
  Darwin) ;;
  *) exit 0 ;;
esac

# A cache miss must not spawn a real cargo: without linux-cross gcc that sits in
# cc-rs for a minute, and with it this hook would compile for several minutes.
probe=$(mktemp -d)
trap 'rm -rf "$probe"' EXIT
printf '#!/bin/sh\nexit 75\n' >"$probe/cargo"
chmod +x "$probe/cargo"

if PATH="$probe:$PATH" nx run-many -t cargo-lint-cross; then
  exit 0
fi

echo "Linux cargo lint is not in the Nx cache. Run: bun run check:linux" >&2
exit 1
