#!/usr/bin/env bash
# macOS-only Linux compile gate. Everything in tooling depends only on
# devenv.nix packages, so this hook must also work without devenv on PATH —
# but it may not pretend a check ran that never did. Nx task hashes can
# differ between the bare shell and the linux-cross profile, so a bare miss
# proves nothing on its own. Policy, in order:
# 1. Bare `nx run-many -t cargo-lint-cross`: a hit is a prior real
#    `cargo clippy --target x86_64-unknown-linux-gnu`; pass with no toolchain.
# 2. If a Nix-built devenv is on PATH, `bun run check:linux` enters the
#    linux-cross profile properly and its result decides the push.
# 3. Otherwise refuse with the recovery command instead of a toolchain error

# A Nix-built devenv binary (under /nix/store), skipping the repo wrapper
# scripts that also answer to this name but cannot enter a profile alone.
has_nix_devenv() {
  while IFS= read -r candidate; do
    case "$(realpath "$candidate" 2>/dev/null)" in
      /nix/store/*) return 0 ;;
    esac
  done < <(which -a devenv 2>/dev/null)
  return 1
}

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

if has_nix_devenv; then
  exec bun run check:linux
fi

cat >&2 <<'EOF'
pre-push: cargo-lint-cross is not cached for this tree and no Nix-built
devenv is on PATH to enter the linux-cross profile. Run `bun run check:linux`
from a shell with nix on PATH, wait for it to pass, then push again.
EOF
exit 1
