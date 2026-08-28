# Managed by `smoo monorepo`. Repo-owned configuration belongs in devenv.nix,
# which imports this module:
#
#   imports = [./devenv.smoo.nix];
#
# This is the SmoothBricks shell contract — the wiring that pairs with the other
# managed files (tooling/direnv/repo-path, setup-environment.ts, the CI actions)
# and must stay identical across repositories. `enterShell` is `types.lines`, so
# this module's prologue and epilogue merge around whatever devenv.nix adds:
# mkBefore establishes the workspace root, PATH, and toolchain before any
# project step runs, and mkAfter restores the caller's directory last.
{
  lib,
  pkgs,
  ...
}: {
  # Nx otherwise defaults to three workers. Scale to the cores available in each
  # developer shell or CI runner; explicit --parallel flags still take precedence.
  env.NX_PARALLEL = "100%";

  enterShell = lib.mkMerge [
    (lib.mkBefore ''
      cd "$DEVENV_ROOT/../.."
      # PATH order: most-specific → least-specific (the same list the git hooks use).
      export PATH="$("$PWD/tooling/direnv/repo-path")"
      # ttsc drives the native TypeScript 7 binary while Nx imports the TypeScript 6 API.
      export TTSC_TSGO_BINARY="$PWD/node_modules/@typescript/native/bin/tsc"
      # Content-keyed native plugin binaries + GOCACHE. Keep outside node_modules so
      # dependency installs/caches stay lean; CI restores .cache/ttsc/plugins only.
      export TTSC_CACHE_DIR="''${TTSC_CACHE_DIR:-$PWD/.cache/ttsc}"
      mkdir -p "$TTSC_CACHE_DIR"
      # enter-shell.ts chdirs to the workspace root and runs setup-environment.ts; a
      # failed bootstrap aborts shell entry instead of yielding a half-working shell.
      bun "$DEVENV_ROOT/enter-shell.ts" || exit $?

      # Wrap rustc only where a compile cache is actually owned.
      #
      # A cowshed workspace is a short-lived fork whose target/ arrived by clone,
      # and the host daemon behind ~/.cowshed/caches serves every workspace on the
      # machine, so a shared cache is the right trade there. The socket path is a
      # fixed convention rather than something cowshed injects: a client that finds
      # no daemon compiles uncached, which is why the workspace shell exports the
      # endpoint unconditionally. Main is the one long-lived checkout you keep
      # rebuilding — incrementality beats the cache there, and sccache refuses
      # incremental compilations outright, so main stays unwrapped. A CI runner that
      # owns a persistent SCCACHE_DIR opts in the same way; a machine with neither
      # would otherwise grow a store nobody ever reclaims.
      #
      # SCCACHE_BASEDIR_CWD=1 activates the patched sccache from the repository's
      # nixpkgs overlay, which keys path-bearing hash inputs relative to the request
      # cwd so workspaces share one cache at any mount path; crates that compile
      # env!("CARGO_MANIFEST_DIR") into their output fail closed.
      if [ "$(sed -n 's/.*"role": *"\([a-z]*\)".*/\1/p' .cowshed/workspace.json 2>/dev/null)" = "workspace" ]; then
        export SCCACHE_DIR="''${SCCACHE_DIR:-$HOME/.cowshed/caches/sccache}"
        export SCCACHE_SERVER_UDS="''${SCCACHE_SERVER_UDS:-$HOME/.cowshed/sccache.sock}"
        export RUSTC_WRAPPER=sccache
        export SCCACHE_BASEDIR_CWD=1
        export CARGO_INCREMENTAL=0
      elif [ -n "''${SCCACHE_DIR:-}" ]; then
        export RUSTC_WRAPPER=sccache
        export SCCACHE_BASEDIR_CWD=1
        export CARGO_INCREMENTAL=0
      fi

      # On Darwin, remove nix CC/CXX so xcodebuild finds Xcode's clang (it supports
      # -index-store-path); bun/node native addons find compilers through node-gyp.
      ${lib.optionalString pkgs.stdenv.isDarwin "unset CC CXX"}
    '')
    (lib.mkAfter ''
      # The devenv wrapper runs from tooling/direnv; return the shell to wherever
      # the caller invoked it.
      if [ -n "$DEVENV_SHELL_PWD" ]; then
        cd "$DEVENV_SHELL_PWD"
      fi
    '')
  ];
}
