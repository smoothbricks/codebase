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

  # One toolchain for every repository and every workspace, pinned by devenv.lock
  # rather than by a rust-toolchain file. devenv resolves the channel through
  # rust-overlay, so a lock bump is the only thing that moves the compiler, and a
  # CI runner building this same shell gets the same rustc a laptop has.
  #
  # Nightly because the fleet needs `-Z` features (`-Zbuild-std`,
  # `panic=immediate-abort` for wasm artifacts) and there is no reason to keep a
  # second toolchain alongside for them. `version = "latest"` reads "newest
  # nightly the locked rust-overlay offers", not "whatever nightly exists today".
  #
  # `components` and `targets` are list options, so a repository adds its own
  # targets in devenv.nix and they merge with these; `channel` and `version` are
  # single-valued and belong to this module. A `rust-toolchain.toml` is NOT the
  # mechanism here: nix cargo ignores it unless devenv is pointed at it with
  # `languages.rust.toolchainFile`, so such a file is decoration that silently
  # disagrees with the shell.
  languages.rust = {
    enable = true;
    channel = "nightly";
    version = "latest";
    components = [
      "rustc"
      "cargo"
      "clippy"
      "rustfmt"
      "rust-analyzer"
      "rust-src"
    ];
  };

  # One Go for every repository, same reasoning as the Rust toolchain: a compiler
  # is part of a cache key, so two of them mean two caches. devenv pins it through
  # the lock.
  #
  # The rule, stated plainly, because it is an ownership boundary and not a
  # workaround: OUR code is built with the Go devenv provides. A dependency MAY
  # vendor its own SDK — ttsc does, inside its native package, with no override
  # for it — and that vendored SDK is an implementation detail of the dependency
  # in the same way a library bundling its own zlib is. Neither side may leak
  # GOROOT into the other. Whose Go is whose is therefore decided by which
  # binary is invoked, never by an ambient variable.
  #
  # This is why Go arrives via `packages` and NOT `languages.go.enable`: that
  # option exports GOROOT, and an exported GOROOT naming one patch release makes
  # the other side's build fail with `compile: version does not match go tool
  # version`. On PATH only, each toolchain resolves its own GOROOT from its own
  # binary and the two coexist by construction. Note the failure is a
  # tool-resolution fault, not a cache fault: Go keys build-cache entries by
  # toolchain, so two versions sharing one GOCACHE produce distinct keys rather
  # than corruption — sharing costs cache size, never correctness, which is what
  # keeps ttsc's plugin builds from costing a rebuild per checkout.
  #
  # Deliberately NOT pinned to whatever Go ttsc vendors. That coupling would make
  # our toolchain hostage to an npm package and is not even always satisfiable:
  # ttsc 0.28.1/0.28.2 vendor go1.26.6, which no nixpkgs channel packages at all.
  packages = [pkgs.go];

  enterShell = lib.mkMerge [
    # Prologue, in order:
    #
    # 1. The devenv wrapper runs from tooling/direnv; every later step expects the
    #    workspace root.
    # 2. PATH order is most-specific → least-specific, the same list the git hooks
    #    use, so a hook and a shell resolve one binary the same way.
    # 3. ttsc drives the native TypeScript 7 binary while Nx imports the TypeScript
    #    6 API, so the two must be named separately.
    # 4. On a cowshed host, Go and ttsc caches point at the shared store, so every
    #    workspace reads one warm cache instead of growing its own copy inside its
    #    image. Without one, ttsc stays in-tree for the CI cache action while Go
    #    keeps its normal per-user defaults. Both Go caches are content-addressed,
    #    so sharing them needs no patch — unlike Rust, Go keys on content and flags
    #    rather than on where the files live. An inherited value always wins so CI
    #    can place any of these caches itself. GOFLAGS carries -trimpath because it
    #    is Go's stable way to keep absolute build paths out of the artifact, and
    #    unlike Rust's trim-paths it costs no cache reuse.
    #
    #    ttsc's plugin builds use the Go it bundles; that is not overridable (it
    #    exposes only TTSC_CACHE_DIR and TTSC_TSGO_BINARY), so the repository's own
    #    Go modules and ttsc's plugin builds are two toolchains by construction.
    #    Placing both caches in the shared store is what keeps that from costing a
    #    rebuild per workspace.
    # 5. enter-shell.ts chdirs to the workspace root and runs setup-environment.ts;
    #    a failed bootstrap aborts shell entry instead of yielding a half-working
    #    shell.
    # 6. The wrapper follows the store, so a cowshed host wraps every checkout,
    #    including main. The store and the socket are cowshed's own, not this
    #    module's invention: cowshed-core/src/sandbox.rs defines them as
    #    /private/cowshed/caches/sccache and /private/cowshed/store/sccache.sock,
    #    its Seatbelt profile admits exactly that socket and denies binding it, and
    #    its supervisor exports the same pair into every sandboxed process. Naming
    #    any other path here does not create a second opinion, it creates a second
    #    cache — and a $HOME path is the worst of them, because $HOME is private
    #    per workspace inside the sandbox, so every client is sent to a store the
    #    daemon does not serve while the daemon's own store goes unread.
    #
    #    A client that finds no daemon compiles uncached, so the socket path is a
    #    fixed convention rather than a discovery. A CI runner that exports
    #    SCCACHE_DIR opts in the same way. A machine with no cowshed store stays
    #    unwrapped, so nothing grows a cache nobody reclaims. SCCACHE_BASEDIR_CWD=1
    #    activates the patched sccache from the repository's nixpkgs overlay, which
    #    keys path-bearing hash inputs relative to the request cwd so workspaces
    #    share one cache at any mount path; crates that compile
    #    env!("CARGO_MANIFEST_DIR") into their output fail closed.
    #
    #    CARGO_INCREMENTAL is not set here, so a shell outside cowshed keeps
    #    incremental for local dev units — sccache reports them non-cacheable and
    #    forwards them, which costs nothing because an agent's own edit is novel
    #    input that could never hit — while everything cargo compiles
    #    non-incrementally still goes through the cache. Under cowshed the choice
    #    is not this module's to make: the supervisor sets CARGO_INCREMENTAL=0 for
    #    every sandboxed process, so incremental is off wherever cowshed runs.
    # 7. GOROOT is unset rather than set, and this is the isolation boundary that
    #    makes the arrangement above correct rather than a workaround for it. Our
    #    Go comes from devenv; a dependency may vendor its own SDK; neither may
    #    leak GOROOT into the other. An inherited GOROOT names exactly one
    #    toolchain and is therefore wrong for at least one side, which surfaces as
    #    `compile: version does not match go tool version`. Absent the variable,
    #    every Go resolves its own GOROOT from its own binary, so which toolchain
    #    builds what is decided by which binary is invoked — the only thing that
    #    can actually be reasoned about.
    # 8. On Darwin, drop nix CC/CXX so xcodebuild finds Xcode's clang (it supports
    #    -index-store-path); bun/node native addons find compilers through
    #    node-gyp.
    (lib.mkBefore ''
      cd "$DEVENV_ROOT/../.."
      export PATH="$("$PWD/tooling/direnv/repo-path")"
      export TTSC_TSGO_BINARY="$PWD/node_modules/@typescript/native/bin/tsc"
      if [ -d "$HOME/.cowshed/caches" ]; then
        export TTSC_CACHE_DIR="''${TTSC_CACHE_DIR:-$HOME/.cowshed/caches/ttsc}"
        export GOCACHE="''${GOCACHE:-$HOME/.cowshed/caches/go/build}"
        export GOMODCACHE="''${GOMODCACHE:-$HOME/.cowshed/caches/go/mod}"
        mkdir -p "$TTSC_CACHE_DIR" "$GOCACHE" "$GOMODCACHE"
      else
        export TTSC_CACHE_DIR="''${TTSC_CACHE_DIR:-$PWD/.cache/ttsc}"
        mkdir -p "$TTSC_CACHE_DIR"
      fi
      export GOFLAGS="''${GOFLAGS:--trimpath}"
      unset GOROOT
      bun "$DEVENV_ROOT/enter-shell.ts" || exit $?

      if [ -n "''${SCCACHE_DIR:-}" ] || [ -d /private/cowshed/caches/sccache ]; then
        export SCCACHE_DIR="''${SCCACHE_DIR:-/private/cowshed/caches/sccache}"
        export SCCACHE_SERVER_UDS="''${SCCACHE_SERVER_UDS:-/private/cowshed/store/sccache.sock}"
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
