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
# A separate name on purpose: assigning to TTSC_CACHE_DIR itself would mutate
# the (possibly exported) environment that persist_devenv_environment later
# fingerprints as the pre-shell baseline.
ttsc_cache_dir_default="${TTSC_CACHE_DIR:-$repo_root/.cache/ttsc}"

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
    echo "using existing devenv: $(command -v devenv) ($(devenv version))"
  else
    echo "nix profile add ${DEVENV_FLAKE}"
    nix profile add --accept-flake-config "$DEVENV_FLAKE"
  fi
  if [ -d "$HOME/.nix-profile/bin" ]; then
    echo "$HOME/.nix-profile/bin" >> "${GITHUB_PATH:-/dev/null}"
  fi
  if [ -n "$ttsc_cache_dir_default" ]; then
    mkdir -p "$ttsc_cache_dir_default"
  fi
}

build_devenv_shell() {
  devenv shell --verbose -- date
  persist_devenv_environment
  # Add repo-local tools only after the shell exists; cleanup steps use an
  # explicit PATH because failures before this point must still refresh caches.
  add_repo_paths
}

# Workflow steps run their commands directly — never through `devenv shell` —
# so nothing the shell hooks export survives this composite action on its own.
# Persisting a hand-picked list here proved to be whack-a-mole: the TTSC
# variables were copied while LD_LIBRARY_PATH was not, and Bun-spawned native
# bindings dlopen-failed on NixOS runners (Bun has no Nix RUNPATH, unlike the
# patched Node, so it resolves libstdc++ only through that variable). Instead,
# capture the exported environment the devenv shell actually produces and
# persist everything it added or changed, minus what belongs to the runner
# step itself or to shell-session bookkeeping.
persist_devenv_environment() {
  [ -n "${GITHUB_ENV:-}" ] || return 0
  local name entry value captured bkey persisted=""
  # Baseline: this step's own exported variables, before shell influence.
  # Stored as smoo_baseline_<name> pseudo-map entries (printf -v), not a
  # bash-4 associative array — macOS runners still ship bash 3.2. compgen -e
  # only lists names that are valid identifiers, so the composed variable
  # name is always valid too.
  while IFS= read -r name; do
    printf -v "smoo_baseline_$name" '%s' "${!name}"
  done < <(compgen -e)
  # NUL separators keep multi-line values intact, and compgen -e lists
  # exported variables only (no BASH_FUNC_* noise). bash is the one
  # interpreter every devenv shell provides, so this needs no coreutils.
  # The capture writes to a file, NOT stdout: enterShell hooks print their
  # own progress to stdout and would corrupt an inline capture.
  captured="$(mktemp)"
  SMOO_ENV_CAPTURE_OUT="$captured" devenv shell -- bash -c \
    'while IFS= read -r n; do printf "%s=%s\0" "$n" "${!n}"; done < <(compgen -e) > "$SMOO_ENV_CAPTURE_OUT"'
  while IFS= read -r -d '' entry; do
    name="${entry%%=*}"
    value="${entry#*=}"
    case "$name" in
      # The runner step owns these; PATH persistence is add_repo_paths' job.
      PATH | PWD | OLDPWD | SHLVL | SHELL | HOME | USER | LOGNAME | HOSTNAME | TERM | TMPDIR | TMP | TEMP | _ | PS1) continue ;;
      GITHUB_* | RUNNER_* | ACTIONS_* | CI) continue ;;
      # Shell-session bookkeeping, meaningful only inside the shell itself.
      DIRENV_* | DEVENV_* | NIX_* | BASH* | XDG_* | IN_NIX_SHELL | SMOO_ENV_CAPTURE_OUT) continue ;;
    esac
    bkey="smoo_baseline_$name"
    if [ "${!bkey+set}" = set ] && [ "${!bkey}" = "$value" ]; then
      continue
    fi
    if [ "${value#*$'\n'}" != "$value" ]; then
      # GITHUB_ENV heredoc form for multi-line values; the delimiter only has
      # to never appear as a full line inside the value.
      printf '%s<<__SMOO_DEVENV_ENV__\n%s\n__SMOO_DEVENV_ENV__\n' "$name" "$value" >> "$GITHUB_ENV"
    else
      printf '%s=%s\n' "$name" "$value" >> "$GITHUB_ENV"
    fi
    persisted="$persisted $name"
  done < "$captured"
  rm -f "$captured"
  echo "devenv environment persisted for later steps:${persisted:- (none)}"
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
