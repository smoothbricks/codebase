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
#
# Every explanation lives out here in Nix comments. Hook bodies are exported as
# shell text, so a comment inside one becomes part of an environment variable.
{
  lib,
  pkgs,
  ...
}: {
  # Nx otherwise defaults to three workers. Scale to the cores available in each
  # developer shell or CI runner; explicit --parallel flags still take precedence.
  env.NX_PARALLEL = "100%";

  enterShell = lib.mkMerge [
    # Prologue, in order:
    #
    # 1. The devenv wrapper runs from tooling/direnv; every later step expects the
    #    workspace root.
    # 2. PATH order is most-specific → least-specific, the same list the git hooks
    #    use, so a hook and a shell resolve one binary the same way.
    # 3. ttsc drives the native TypeScript 7 binary while Nx imports the TypeScript
    #    6 API, so the two must be named separately.
    # 4. TTSC_CACHE_DIR holds content-keyed native plugin binaries and GOCACHE.
    #    It stays outside node_modules so dependency installs and caches stay lean,
    #    and an inherited value wins because CI restores .cache/ttsc/plugins into a
    #    path it chooses.
    # 5. enter-shell.ts chdirs to the workspace root and runs setup-environment.ts;
    #    a failed bootstrap aborts shell entry instead of yielding a half-working
    #    shell.
    # 6. rustc is wrapped wherever a compile cache is owned, and CARGO_INCREMENTAL
    #    is deliberately left unset. sccache only refuses when that variable is
    #    set: with it unset, cargo keeps incremental for local dev units (sccache
    #    reports them non-cacheable and forwards them, which costs nothing because
    #    an agent's own edit is novel input that could never hit) while every unit
    #    cargo compiles non-incrementally — registry dependencies, `incremental =
    #    false` profiles, release and platform builds — goes through the cache and
    #    can hit work another workspace already did. Setting CARGO_INCREMENTAL=0
    #    bought nothing and surrendered the inner loop.
    #
    #    A cowshed workspace is a short-lived fork whose target/ arrived by clone,
    #    and one host daemon serves every workspace on the machine, so the shared
    #    store is the right trade. The socket path is a fixed convention rather
    #    than something cowshed injects: a client that finds no daemon compiles
    #    uncached, which is why the workspace shell exports the endpoint itself.
    #    Main and plain clones stay unwrapped — nothing owns a store there, and an
    #    unowned store is one nobody ever reclaims. A CI runner that owns a
    #    persistent SCCACHE_DIR opts in the same way. The endpoint is derived only
    #    inside the branch that uses it, so re-entering the shell cannot promote
    #    main into the wrapped branch. SCCACHE_BASEDIR_CWD=1 activates the patched
    #    sccache from the repository's nixpkgs overlay, which keys path-bearing
    #    hash inputs relative to the request cwd so workspaces share one cache at
    #    any mount path; crates that compile env!("CARGO_MANIFEST_DIR") into their
    #    output fail closed.
    # 7. On Darwin, drop nix CC/CXX so xcodebuild finds Xcode's clang (it supports
    #    -index-store-path); bun/node native addons find compilers through
    #    node-gyp.
    (lib.mkBefore ''
      cd "$DEVENV_ROOT/../.."
      export PATH="$("$PWD/tooling/direnv/repo-path")"
      export TTSC_TSGO_BINARY="$PWD/node_modules/@typescript/native/bin/tsc"
      export TTSC_CACHE_DIR="''${TTSC_CACHE_DIR:-$PWD/.cache/ttsc}"
      mkdir -p "$TTSC_CACHE_DIR"
      bun "$DEVENV_ROOT/enter-shell.ts" || exit $?

      if [ "$(sed -n 's/.*"role": *"\([a-z]*\)".*/\1/p' .cowshed/workspace.json 2>/dev/null)" = "workspace" ]; then
        export SCCACHE_DIR="''${SCCACHE_DIR:-$HOME/.cowshed/caches/sccache}"
        export SCCACHE_SERVER_UDS="''${SCCACHE_SERVER_UDS:-$HOME/.cowshed/sccache.sock}"
        export RUSTC_WRAPPER=sccache
        export SCCACHE_BASEDIR_CWD=1
      elif [ -n "''${SCCACHE_DIR:-}" ]; then
        export RUSTC_WRAPPER=sccache
        export SCCACHE_BASEDIR_CWD=1
      fi

      ${lib.optionalString pkgs.stdenv.isDarwin "unset CC CXX"}
    '')
    # Epilogue: the wrapper runs devenv from tooling/direnv, so return the shell
    # to wherever the caller invoked it. Last, after every project step.
    (lib.mkAfter ''
      if [ -n "$DEVENV_SHELL_PWD" ]; then
        cd "$DEVENV_SHELL_PWD"
      fi
    '')
  ];
}
