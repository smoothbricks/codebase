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
  imports = [
    ./devenv.smoo.nix
    {
      # Apply after devenv assembles the pinned components: changing component
      # pins or PATH would create a second toolchain instead of removing its cc.
      options.languages.rust.toolchainPackage = lib.mkOption {
        apply = toolchain:
          if pkgs.stdenv.isDarwin
          then
            toolchain.overrideAttrs (previous: {
              propagatedBuildInputs = lib.remove pkgs.stdenv.cc (previous.propagatedBuildInputs or []);
              depsHostHostPropagated = lib.remove pkgs.stdenv.cc (previous.depsHostHostPropagated or []);
            })
          else toolchain;
      };
    }
  ];

  # https://devenv.sh/overlays/
  overlays = [
    # Import the live owner source: a flake input evaluates its overlay outside
    # devenv's file-read tracing, leaving generated pins absent from the eval cache.
    ((import ./nixpkgs-overlay/flake.nix).outputs {}).overlays.default
    inputs.rust-overlay.overlays.default
    # https://bugs.gnu.org/81060: tee 9.11 spins at close after a nonblocking write sets EAGAIN.
    # Keep stdenv's coreutils unchanged; only the shell selects the patched package.
    # Remove when nixpkgs coreutils includes this upstream fix.
    (_: previous: {
      coreutils-tee = previous.coreutils.overrideAttrs (old: {
        patches =
          (old.patches or [])
          ++ [
            (previous.fetchpatch {
              url = "https://github.com/coreutils/coreutils/commit/0d6fcb99d691d920961938e61c43478566ef626e.patch";
              includes = ["src/iopoll.c" "src/iopoll.h" "src/tee.c"];
              hash = "sha256-mtBtXvroe1whqHVGsxiWjQ7/3QyjSxXX0G515qaCD7E=";
            })
          ];
      });
    })
  ];

  # https://devenv.sh/packages/
  packages =
    (with pkgs; [
      gnutar # Tarball inspection for package validation
      coreutils-tee # Patched tee and fmt for commit message wrapping
      git # Git hooks and repository inspection
      gh # GitHub Actions and release inspection
      direnv # Cowshed .envrc activation; hosted CI has no ambient direnv
      # Bun.sh for javascript dependencies
      bun
      # The managed shell's uv installer is exercised against the real producer in
      # packages/cli's shell-entry tests; those tests must find uv on PATH in CI,
      # where no ambient profile provides it.
      uv
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
    ++ lib.optionals pkgs.stdenv.isDarwin [
      (pkgs.writeShellScriptBin "smoothbricks-macos-linker"
        (builtins.readFile ../../packages/cowshed/scripts/macos-linker.sh))
    ]
    # GARM/Linux CI: rustc needs host linker `cc` (-Zbuild-std, native crates).
    ++ lib.optionals pkgs.stdenv.isLinux [
      pkgs.stdenv.cc
      pkgs.rsync
      # openssl-src (vendored-openssl / git2) configure needs perl + make.
      pkgs.perl
      pkgs.gnumake
      pkgs.pkg-config
      pkgs.openssl
      # mkfs.ext4 for the job cgroup test's loop-mounted scratch: the build's own
      # ZFS storage reaches neither memory.current's page cache nor io.stat.
      pkgs.e2fsprogs
      pkgs.util-linux # ext4 mount/umount and the unprivileged test controller's setpriv
    ];

  # Nix's setup hooks replace generic CC/CXX after `env` is applied, leaving
  # cc-rs with bare clang and therefore no Nix libc include path.
  # cc-rs probes target-specific names before generic ones, so pin
  # the native Linux target to the wrapper at the precedence point it honors.
  # Linux artifacts build with zig through cargo-zigbuild, whose Nx executor
  # drops this pin for the triple it builds.
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

  # Rust enables devenv's C tools by default. Darwin uses the selected Apple
  # toolchain instead: neither that module nor the shell's own stdenv should
  # add Nix compiler/linker binaries to PATH. Linux keeps its existing tools.
  stdenv = lib.mkIf pkgs.stdenv.isDarwin pkgs.stdenvNoCC;
  languages.c = {
    enable = !pkgs.stdenv.isDarwin;
    lsp.enable = false;
    debugger = null;
  };

  # The managed module supplies fleet-wide WASM and Linux targets, including
  # every Linux artifact triple (its linux toolchain). Add only the macOS
  # release targets a Mac runner builds, so a Linux runner does not fetch an
  # Apple std it can never link.
  languages.rust.targets = lib.optionals pkgs.stdenv.isDarwin [
    "aarch64-apple-darwin"
    "x86_64-apple-darwin"
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
    # pyarrow only. The oracles import `pyarrow.ipc` and nothing else — grep the
    # whole repository and there is not one `import pandas`, nor a single .py
    # file: python here exists to be spawned by two Rust tests
    # (columine-arrow/tests/pyarrow_oracle.rs, lmao-arrow/tests/convert.rs).
    # pandas dragged 102.7 MB into every shell and every mac CI restore — itself
    # plus pytz, python-dateutil, tzdata and six — for an import nothing makes.
    # numpy stays: pyarrow propagates it.
    package = pkgs.python314.withPackages (ps: [ps.pyarrow]);
    # A language server is editor tooling, and nothing in this repository is
    # written in Python, so devenv's default pyright is 291.7 MB of pure tax on
    # every macOS CI store restore. It also drags nodejs 24 in alongside the
    # nodejs 26 this shell pins on purpose — a second Node major on one PATH is
    # exactly the ambiguity that fleet-wide pin exists to prevent. An editor
    # that wants pyright can install its own; a release runner cannot.
    lsp.enable = false;
  };

  # We're not using Devenv's pre-commit-hooks, because this repo's pre-commit hook
  # uses `git-format-staged` to format only the content that is about to be committed.
  # See https://devenv.sh/pre-commit-hooks/ for more details (uses Python pre-commit)

  # See full reference at https://devenv.sh/reference/options/
}
