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
  # sccache is deliberately absent from this shell. It used to export
  # RUSTC_WRAPPER=sccache as a BARE NAME, which PATH then resolved per project —
  # which is precisely how an unpatched binary from one profile came to serve
  # clients from another. sccache now belongs to cowshed: `cowshed setup
  # --sccache` builds packages/cowshed/nix/sccache, pins the result with a nix GC
  # root, and supervises that exact store path. A repository shell has no
  # business deciding which compiler cache a host runs.

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
    # CI validates on Linux, so a macOS shell carries Linux's std and can
    # type-check the arm CI compiles. rust-std alone reaches every crate whose
    # dependency graph is pure Rust; a dependency that compiles C for the target
    # — ring, openssl-sys, libgit2-sys, libz-sys — additionally needs a cross C
    # compiler, which is 0.4 GiB and so lives in the opt-in `linux-cross` profile
    # below rather than here, keeping it off every macOS shell and macOS runner.
    #
    # The reverse direction is not available at any price: type-checking an Apple
    # target from Linux needs an Apple SDK for those same C-building
    # dependencies, which is a licensing boundary rather than a missing package.
    targets = [
      "x86_64-unknown-linux-gnu"
    ];
  };

  # Opt-in Linux cross toolchain, activated by `devenv -P linux-cross` and driven
  # by the root `check:linux` script. devenv 2.2.3 has first-class profiles, so
  # this is one gated module in the shared file rather than a second config
  # directory that would have to re-import and re-pin everything here.
  #
  # It is a profile and NOT a default package because pkgsCross.gnu64's cc
  # closure is 0.4 GiB against a default shell closure of ~5.4 GiB — a 7% tax on
  # every shell entry and every CI cache restore, to serve one command that a
  # macOS laptop runs deliberately. Nothing on Darwin links a Linux object.
  #
  # Why a C compiler is needed at all, when rust-std above is already installed:
  # ring, openssl-sys, libgit2-sys and libz-sys compile C for the target from
  # their build scripts, and build scripts are compiled and run even under
  # `cargo check`, so std alone stops at `ToolNotFound: failed to find tool
  # "x86_64-linux-gnu-gcc"`. The `cc` crate probes triple-prefixed tool names,
  # which is precisely what this wrapper's bin/ exports.
  #
  # Each tool path is derived from the wrapper's own `targetPrefix` instead of
  # being written out, so a nixpkgs bump that renames the prefix carries these
  # with it rather than leaving four stale strings that fail at build-script time.
  #
  # This profile supplies the toolchain and nothing else. CARGO_BUILD_TARGET is
  # deliberately NOT set: the triple belongs to the Nx target that asks for it
  # (`cargo-lint-x64-linux` passes `--target` explicitly), not to the ambient
  # environment. An ambient triple would make the check silently host-local
  # whenever the profile was forgotten — reporting green having compiled macOS —
  # and that false green is the precise failure this whole profile exists to end.
  # With the flag on the target instead, running it outside this profile fails
  # loudly at `ToolNotFound` and a host lint can never be mistaken for a cross one.
  profiles.linux-cross.module = let
    crossCC = pkgs.pkgsCross.gnu64.stdenv.cc;
    tool = name: "${crossCC}/bin/${crossCC.targetPrefix}${name}";
  in {
    packages = [crossCC];
    env = {
      CC_x86_64_unknown_linux_gnu = tool "cc";
      CXX_x86_64_unknown_linux_gnu = tool "c++";
      AR_x86_64_unknown_linux_gnu = tool "ar";
      CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER = tool "cc";
    };
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
  # for any single-package pin — one leaf package from a nixpkgs that has it,
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
    pkgs.binaryen
    # Target-dir GC for the inferred cargo-sweep target: cargo never removes
    # superseded artifacts on its own (a busy workspace accumulated ~18k stale
    # variants per crate and 26 GB of junk before this existed), and a sweep
    # prunes them without touching the warm current-fingerprint surface.
    pkgs.cargo-sweep
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
    # 6. GOROOT is unset rather than set. With devenv's Go pinned to the patch
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
    # 7. On Darwin, drop nix CC/CXX so xcodebuild finds Xcode's clang (it supports
    #    -index-store-path); bun/node native addons find compilers through
    #    node-gyp. CC/CXX are what xcodebuild reads, which is why they go rather
    #    than being pointed somewhere else.
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
