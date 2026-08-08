#!/usr/bin/env bash
set -euo pipefail

NIX_STORE_NAR="${NIX_STORE_NAR:-/tmp/nix-store.nar}"
nix_store_cmd="/nix/var/nix/profiles/default/bin/nix-store"
DEVENV_FLAKE="${DEVENV_FLAKE:-github:cachix/devenv}"
# Resolve from this script's location, not the caller's cwd. GitHub Actions
# runs this from tooling/direnv today, but direct cwd-changing helpers are easy
# to misuse and break on repeated calls.
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
# Host CI supplies its bind-mounted cache through the runner profile. Ephemeral
# runners must use the same repository path restored by cache-ttsc-plugins.
TTSC_CACHE_DIR="${TTSC_CACHE_DIR:-$repo_root/.cache/ttsc}"

clear_devenv_cache_state() {
  rm -rf "$repo_root/tooling/direnv/.devenv" "$repo_root/tooling/direnv/.direnv"
}

add_repo_paths() {
  "$repo_root/tooling/direnv/repo-path" --github-path
}

restore_nix_store() {
  if [ -s "$NIX_STORE_NAR" ]; then
    if ! sudo "$nix_store_cmd" --import --quiet < "$NIX_STORE_NAR"; then
      # .devenv/.direnv contain absolute /nix/store references. If importing the
      # matching store closure fails, clearing them prevents incoherent restores.
      clear_devenv_cache_state
      exit 1
    fi
    # The restored NAR is only the import source. Cleanup exports a fresh NAR for
    # the next cache save, so remove this workspace scratch file before release
    # steps run git cleanliness checks.
    rm -f "$NIX_STORE_NAR"
  else
    echo "No NAR file found; clearing devenv cache state"
    clear_devenv_cache_state
  fi
}

install_devenv() {
  # Shared host /nix/store is the package cache. Image may already provide
  # devenv from github:cachix/devenv; otherwise profile-add the same flake
  # (links store paths; re-fetch only when missing).
  if command -v devenv >/dev/null 2>&1; then
    echo "using existing devenv: $(command -v devenv) ($(devenv version 2>/dev/null || true))"
  else
    echo "nix profile add ${DEVENV_FLAKE}"
    nix profile add --accept-flake-config "$DEVENV_FLAKE"
  fi
  if [ -d "$HOME/.nix-profile/bin" ]; then
    echo "$HOME/.nix-profile/bin" >> "${GITHUB_PATH:-/dev/null}"
  fi
  if [ -n "$TTSC_CACHE_DIR" ]; then
    mkdir -p "$TTSC_CACHE_DIR"
  fi
}

build_devenv_shell() {
  devenv shell --verbose -- date
  # enterShell exports these for the bootstrap itself; persist them for the
  # independent workflow step shells that run after this composite action.
  if [ -n "${GITHUB_ENV:-}" ]; then
    echo "TTSC_TSGO_BINARY=$repo_root/node_modules/@typescript/native/bin/tsc" >> "$GITHUB_ENV"
    # Only re-export when the runner (or a prior step) already set the path.
    if [ -n "$TTSC_CACHE_DIR" ]; then
      echo "TTSC_CACHE_DIR=$TTSC_CACHE_DIR" >> "$GITHUB_ENV"
    fi
  fi
  # Add repo-local tools only after the shell exists; cleanup steps use an
  # explicit PATH because failures before this point must still refresh caches.
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
