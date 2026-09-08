#!/usr/bin/env bash
# macOS-only Linux compile gate. Everything in tooling depends only on
# devenv.nix packages, so this hook must work without devenv on PATH and
# therefore never enters a profile itself: it is a quick cache check only.
# Nx caches cargo-lint-cross on CARGO_INPUTS; a hit is a prior real
# `cargo clippy --target x86_64-unknown-linux-gnu` and passes with no toolchain.
# A miss means the check never ran for this tree — run `bun run check:linux`
# (which enters the linux-cross profile properly), wait for it to pass,
# then push again.

cd "$(git rev-parse --show-toplevel)"
TOOLING="$PWD/tooling"

export PATH="$("$TOOLING/direnv/repo-path")"

case "$(uname -s)" in
  Darwin) ;;
  *) exit 0 ;;
esac

if nx run-many -t cargo-lint-cross; then
  exit 0
fi

cat >&2 <<'EOF'
pre-push: cargo-lint-cross is not cached for this tree, and this hook never
enters a devenv profile itself. Run `bun run check:linux`, wait for it to
pass, then push again.
EOF
exit 1
