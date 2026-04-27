#!/usr/bin/env bash
set -e

pushd "$(dirname "$0")" > /dev/null
  cd ..
  TOOLING="$(pwd)"
  cd "$TOOLING/.."
  WORKSPACE_ROOT="$(pwd)"
popd > /dev/null

GIT_DIR="$(git rev-parse --git-dir)"

link_hook() {
  local name="$1"
  local source="$TOOLING/git-hooks/$name.sh"
  local target="$GIT_DIR/hooks/$name"

  [ "$(readlink "$target" 2>/dev/null || true)" = "$source" ] || {
    echo "[!] Linking $name hook in ${GIT_DIR}"
    rm -f "$target"
    ln -vs "$source" "$target"
    echo
  }
}

pushd "$(git rev-parse --show-toplevel)" > /dev/null || {
  echo "Error: Not in a git repository"
  exit 1
}
  git config --local include.path "$TOOLING/workspace.gitconfig"
  link_hook pre-commit
  link_hook commit-msg
popd > /dev/null
