# Managed by `smoo monorepo`. Sourced from the workspace root by devenv.smoo.nix's
# enterShell prologue, before nx-socket-dir.sh rebinds NX_WORKSPACE_ROOT_PATH,
# and by the git hooks, which run outside that shell:
#
#   . tooling/direnv/ttsc-env.sh
#
# Exports ttsc's environment for this workspace: TTSC_TSGO_BINARY and
# TTSC_TSGO_SOURCE, the pinned native TypeScript compiler and its Go module
# source that devenv.smoo.nix places in this checkout's devenv profile
# (<checkout>/tooling/direnv/.devenv/profile/bin/tsc and .../profile/tsc), so
# shells and git hooks outside the shell always select this checkout's own
# compiler without reading inherited SMOO_* variables; TTSC_CACHE_DIR, where
# ttsc keeps the plugin binaries it compiles (<checkout>/.cache/ttsc-plugins,
# the path a repository's ttsc plugin build target declares as its output); and
# TTSC_GO_CACHE_DIR, the Go build cache that compiles them.
#
# The Go build cache is the host's, $HOME/.cache/ttsc/go-build, not the
# checkout's. Compiling a plugin is almost all Go compilation, and Go keys each
# object by its content, never by the checkout (GOFLAGS=-trimpath keeps paths
# out), so one cache serves every checkout's builds of the plugins' shared Go
# dependencies, while each plugin host's own packages still compile per
# checkout. Measured on six plugins, a second workspace wrote 498 Go cache
# files against 2058 in the first (341 s against 728 s). Kept per checkout,
# every fork compiled what its main had not, about 90 s and 3 GiB each. It is a
# directory of ttsc's own, so "ttsc clean" empties it and never the host's
# GOCACHE. A repository shares it into its cowshed sandboxes by declaring it in
# main's .cowshed.toml, [caches] home = [".cache/ttsc"]; without that
# declaration each sandbox's private HOME holds a cache of its own.
#
# This checkout is the directory the script is sourced from: devenv.smoo.nix's
# enterShell enters the workspace root first, and each git hook enters
# "git rev-parse --show-toplevel". Sourced from any other directory, $PWD names
# no checkout, and the plugin cache below would land beside that directory
# instead: a shell in HOME would create ~/.cache/ttsc-plugins. That directory is
# refused.
if [ ! -f "$PWD/tooling/direnv/ttsc-env.sh" ]; then
  echo "ttsc-env.sh: not a workspace root (no tooling/direnv/ttsc-env.sh): $PWD; source it from the checkout root" >&2
  return 1
fi
# The compiler is always this checkout's own. An inherited cache directory is
# the host's choice and stays (CI points it at a cache shared across runs),
# unless the environment was bound for another workspace: an inherited
# NX_WORKSPACE_ROOT_PATH naming another directory says so, the evidence
# nx-socket-dir.sh drops that workspace's Nx state on. Its ttsc caches are then
# dropped too, and this workspace takes its own. A hook inherits whatever
# environment ran "git"; run from a shell bound to another checkout, every
# commit here compiled ttsc plugins into that checkout's cache, and failed
# outright once that checkout's cache was gone.
smoo_ttsc_checkout="$(pwd -P)"
if [ -n "${NX_WORKSPACE_ROOT_PATH:-}" ] &&
  [ "$(cd "$NX_WORKSPACE_ROOT_PATH" >/dev/null 2>&1 && pwd -P)" != "$smoo_ttsc_checkout" ]; then
  unset TTSC_CACHE_DIR TTSC_GO_CACHE_DIR
fi
unset smoo_ttsc_checkout
export TTSC_TSGO_BINARY="$PWD/tooling/direnv/.devenv/profile/bin/tsc"
if [ ! -x "$TTSC_TSGO_BINARY" ]; then
  echo "ttsc-env.sh: checkout profile/bin/tsc is unavailable: $TTSC_TSGO_BINARY" >&2
  return 1
fi
export TTSC_TSGO_SOURCE="$PWD/tooling/direnv/.devenv/profile/tsc"
if [ ! -d "$TTSC_TSGO_SOURCE" ]; then
  echo "ttsc-env.sh: checkout profile/tsc is unavailable: $TTSC_TSGO_SOURCE" >&2
  return 1
fi
export TTSC_CACHE_DIR="${TTSC_CACHE_DIR:-$PWD/.cache/ttsc-plugins}"
if [ -z "${TTSC_GO_CACHE_DIR:-}" ]; then
  if [ -z "${HOME:-}" ]; then
    echo "ttsc-env.sh: HOME is unset, so nothing names this host's ttsc Go build cache; export HOME or TTSC_GO_CACHE_DIR" >&2
    return 1
  fi
  TTSC_GO_CACHE_DIR="$HOME/.cache/ttsc/go-build"
fi
export TTSC_GO_CACHE_DIR
mkdir -p "$TTSC_GO_CACHE_DIR"
# The plugins ttsc builds are pure Go; with cgo off neither ttsc's cache key nor
# the build asks the C toolchain. An inherited value stays.
export CGO_ENABLED="${CGO_ENABLED:-0}"
