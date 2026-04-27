#!/bin/bash
set -euo pipefail

NIX_STORE_NAR="${NIX_STORE_NAR:-/tmp/nix-store.nar}"
nix_store_cmd="/nix/var/nix/profiles/default/bin/nix-store"

add_repo_paths() {
  local root
  root="$(cd ../.. && pwd)"
  {
    echo "$root/tooling"
    echo "$root/node_modules/.bin"
    echo "$root/tooling/direnv/.devenv/profile/bin"
  } >> "$GITHUB_PATH"
}

restore_nix_store() {
  if [ -f "$NIX_STORE_NAR" ]; then
    sudo "$nix_store_cmd" --import --quiet < "$NIX_STORE_NAR"
  else
    echo "No NAR file found, skipping import"
  fi
}

install_devenv() {
  sudo mkdir -p /nix/var/nix/gcroots/ci
  if ! command -v devenv >/dev/null 2>&1; then
    nix profile install --accept-flake-config nixpkgs#devenv
  fi
  echo "$HOME/.nix-profile/bin" >> "$GITHUB_PATH"
  sudo ln -sf "$HOME/.nix-profile" /nix/var/nix/gcroots/ci/profile
}

build_devenv_shell() {
  devenv shell --verbose -- date
  add_repo_paths
}

case "${1:-}" in
  restore-store) restore_nix_store ;;
  install-devenv) install_devenv ;;
  build-shell) build_devenv_shell ;;
  *)
    echo "Usage: $0 {restore-store|install-devenv|build-shell}" >&2
    exit 1
    ;;
esac
