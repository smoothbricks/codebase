# https://devenv.sh/basics/
{
  inputs,
  pkgs,
  lib,
  ...
}:
# https://devenv.sh/inputs/
let
  git-format-staged = inputs.git-format-staged.packages.${pkgs.stdenv.system}.default;
in {
  # The SmoothBricks shell contract: PATH, toolchain, compiler-cache policy, and
  # the enterShell prologue/epilogue every repo shares. Managed by `smoo monorepo`.
  imports = [./devenv.smoo.nix];

  # https://devenv.sh/overlays/
  overlays = [
    inputs.nixpkgs-overlay.overlays.default
    inputs.rust-overlay.overlays.default
  ];

  # https://devenv.sh/packages/
  packages =
    (with pkgs; [
      gnutar # Tarball inspection for package validation
      coreutils # Provides fmt for commit message wrapping
      git # Git hooks and repository inspection
      gh # GitHub Actions and release inspection
      # Bun.sh for javascript dependencies
      bun
      # The toolchain itself comes from ./devenv.smoo.nix (one nightly for every
      # repository, pinned by devenv.lock). Only this repository's extra targets
      # are declared here, below, via languages.rust.targets.
      just # Task runner for packages/columine (mirrors the lmao justfile)
      cargo-mutants # Mutation target inferred by @smoothbricks/nx-plugin
      # Go comes from ./devenv.smoo.nix (languages.go), one version for every
      # repository for the same reason as the Rust toolchain: a compiler is part
      # of a cache key. packages/lmao-ttsc/plugin builds against it.
      # Git hooks and formatters
      git-format-staged
      jq # Used in pre-commit hook and generally useful
      alejandra # Nix formatter
    ])
    # GARM/Linux CI: rustc needs host linker `cc` (-Zbuild-std, native crates).
    # N-API cross targets need raw Clang: Nix's cc-wrapper injects host include
    # paths ahead of the downloaded target sysroot.
    ++ lib.optionals pkgs.stdenv.isLinux [
      pkgs.stdenv.cc
      (lib.hiPrio pkgs.llvmPackages.clang-unwrapped)
      pkgs.rsync
      # openssl-src (vendored-openssl / git2) configure needs perl + make.
      pkgs.perl
      pkgs.gnumake
      pkgs.pkg-config
      pkgs.openssl
    ];

  # Nix's setup hooks replace generic CC/CXX after `env` is applied, leaving
  # cc-rs with bare clang and therefore no Nix libc include path.
  # cc-rs probes target-specific names before generic ones, so pin
  # the native Linux target to the wrapper at the precedence point it honors.
  # N-API cross targets use their own target-specific compiler variables.
  env = lib.optionalAttrs pkgs.stdenv.isLinux {
    CC_x86_64_unknown_linux_gnu = "${pkgs.stdenv.cc}/bin/cc";
    CXX_x86_64_unknown_linux_gnu = "${pkgs.stdenv.cc}/bin/c++";
  };

  # Use system Xcode for iOS simulator, signing, and instruments.
  # Nix Apple SDK is build-only — no simctl/simulator runtimes, and nix's
  # clang doesn't support -index-store-path which xcodebuild passes.
  # https://devenv.sh/recipes/macos/
  # https://github.com/cachix/devenv/issues/1674
  apple.sdk = null;

  # The managed module supplies fleet-wide WASM and Linux targets. Add only the
  # native release targets the current runner can actually build, so a laptop
  # does not fetch Linux std it never links.
  languages.rust.targets =
    lib.optionals pkgs.stdenv.isDarwin [
      "aarch64-apple-darwin"
      "x86_64-apple-darwin"
    ]
    ++ lib.optionals pkgs.stdenv.isLinux [
      "aarch64-unknown-linux-gnu"
    ];

  # The managed prologue bootstraps dependencies first; this repo-owned extension
  # then rebuilds the local Nx plugin when its sources are newer than its output.
  enterShell = ''
    bun "$DEVENV_ROOT/enter-shell.ts" || exit $?
  '';

  # https://devenv.sh/languages/
  # Python with pyarrow for Arrow IPC verification tests.
  # Must use languages.python instead of adding pythonEnv to packages because:
  # - Shells pass argv[0] as just "python" (not full path) when running via PATH
  # - Nix's python wrapper uses --inherit-argv0, passing this bare name to the real Python
  # - Python uses argv[0] to find its prefix/site-packages, fails with just "python"
  # - languages.python sets up shell hooks that ensure argv[0] contains the full path
  languages.python = {
    enable = true;
    package = pkgs.python314.withPackages (ps: [ps.pyarrow ps.pandas]);
  };

  # We're not using Devenv's pre-commit-hooks, because this repo's pre-commit hook
  # uses `git-format-staged` to format only the content that is about to be committed.
  # See https://devenv.sh/pre-commit-hooks/ for more details (uses Python pre-commit)

  # See full reference at https://devenv.sh/reference/options/
}
