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
  inputs,
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
  # is part of a cache key, so two of them mean two caches.
  #
  # Pinned to the patch release ttsc vendors inside its native package, because
  # that SDK is not overridable — ttsc exposes only TTSC_CACHE_DIR and
  # TTSC_TSGO_BINARY — so matching it is the only way to have ONE Go rather than
  # ours plus a dependency's. `smoo monorepo check` enforces the match and fails
  # with both versions when they diverge, which is what makes this pin a
  # maintained invariant instead of a comment that rots.
  #
  # It needs its own input because the fleet's nixpkgs does not carry that patch:
  # devenv-nixpkgs rolling ships go 1.26.5 and its `go_1_27` is a release
  # candidate, while ttsc 0.28.3 vendors go1.26.7. Same idiom, and same reason,
  # as the dedicated sccache input — one leaf package from a nixpkgs that has it,
  # lock-pinned, prebuilt, rather than a fleet-wide bump that would move rustc,
  # bun and node too. Not every pairing is reachable: ttsc 0.28.1/0.28.2 vendor
  # go1.26.6, which NO channel packages, so when no revision offers the vendored
  # patch the ttsc pin is what moves.
  #
  # The ownership boundary this pin sits inside, unchanged by it: our code is
  # built with the Go devenv provides, a dependency may vendor its own SDK, and
  # neither may leak GOROOT into the other. Go therefore arrives via `packages`
  # and NOT `languages.go.enable`, which would export GOROOT; on PATH only, each
  # toolchain resolves its own GOROOT from its own binary. With the versions
  # matched that crossing cannot misfire at all, so the unset in enterShell is
  # belt-and-braces rather than the fix.
  # Node tracks the newest AWS Lambda managed runtime, currently nodejs26.x. It
  # lives here rather than in each repository's devenv.nix because it is a
  # fleet-wide fact: declared in three places, the next bump has to find all
  # three, and the one it misses re-splits the fleet.
  #
  # An explicit major, NOT `nodejs_latest`. A tool that participates in cache keys
  # and native-addon ABI must not be spelled "whatever is newest" in one
  # repository and a fixed major in the others — and the two spellings agreeing
  # today is precisely what makes the drift invisible, because the fleet looks
  # unified right up to the lock bump that splits it. A floating attribute is a
  # version that changes without a commit, so it cannot be reviewed or bisected.
  #
  # 26 also closes a split this axis caused once: 24.0.x declares URLPattern in a
  # way that TS2403-conflicts with lib.dom in consumers emitting declarations,
  # which forced a ~24.13.0 floor on one side; 26 is DOM-compatible.
  #
  # `@types/node`, `engines.node` and `packageManager` are derived from this pin
  # rather than maintained beside it — smoo's syncRootRuntimeVersions reads the
  # shell's Node major, so this line moves them.
  #
  # Reading which version this resolves to: devenv.lock has TWO nixpkgs nodes and
  # the obvious one is a decoy. `.nodes.nixpkgs` is not what `pkgs` is; the
  # authoritative path is `.nodes.root.inputs.nixpkgs`, which indirects to
  # `nixpkgs_2`.
  packages = [
    (import inputs.nixpkgs-go {inherit (pkgs.stdenv.hostPlatform) system;}).go
    pkgs.nodejs_26
  ];

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
    # 7. GOROOT is unset rather than set. With devenv's Go pinned to the patch
    #    release ttsc vendors, a GOROOT crossing cannot misfire on version at all,
    #    so this is belt-and-braces rather than the fix — it keeps the isolation
    #    boundary intact even while the two are momentarily out of step, e.g. after
    #    a ttsc bump and before the Go pin follows it. The boundary itself: our Go
    #    comes from devenv, a dependency may vendor its own SDK, and neither may
    #    leak GOROOT into the other. An inherited GOROOT names exactly one
    #    toolchain and so is wrong for at least one side, surfacing as `compile:
    #    version does not match go tool version`. Absent the variable, every Go
    #    resolves its own GOROOT from its own binary, which makes "whose Go is
    #    whose" a property of which binary is invoked — the only thing that can
    #    actually be reasoned about.
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
