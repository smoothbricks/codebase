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
  # https://devenv.sh/overlays/
  overlays = [
    inputs.nixpkgs-overlay.overlays.default
    inputs.rust-overlay.overlays.default
  ];

  # https://devenv.sh/packages/
  packages = with pkgs; [
    gnutar # Tarball inspection for package validation
    coreutils # Provides fmt for commit message wrapping
    git # Git hooks and repository inspection
    gh # GitHub Actions and release inspection
    # Pin Node.js version to match AWS Lambda runtime
    nodejs_24
    # Bun.sh for javascript dependencies
    bun
    # Rust toolchain for packages/cowshed and packages/lmao-rs: cargo, rustc,
    # clippy, rustfmt via rust-overlay stable, plus rust-src/rust-analyzer for
    # IDE and LSP use. Keep the WASM target on every system; add only the
    # native release targets that the current runner can build.
    (rust-bin.stable.latest.default.override {
      extensions = ["rust-src" "rust-analyzer"];
      targets =
        ["wasm32-unknown-unknown"]
        ++ lib.optionals pkgs.stdenv.isDarwin [
          "aarch64-apple-darwin"
          "x86_64-apple-darwin"
        ]
        ++ lib.optionals pkgs.stdenv.isLinux [
          "aarch64-unknown-linux-gnu"
        ];
    })
    # Nightly rides alongside stable as an explicit `cargo-nightly` shim, so
    # stable stays the default for every existing target. Only the wasm
    # artifact build uses it: -Zbuild-std + panic=immediate-abort strips the
    # fmt/panic machinery stable cannot remove (packages/columine justfile
    # `wasm`; same shim as AxE's devenv). rust-src is required by -Zbuild-std.
    (let
      nightly = rust-bin.nightly.latest.minimal.override {
        extensions = ["rust-src"];
      };
    in
      pkgs.writeShellScriptBin "cargo-nightly" ''
        export RUSTC="${nightly}/bin/rustc"
        exec "${nightly}/bin/cargo" "$@"
      '')
    just # Task runner for packages/columine (mirrors lmao-rs/axe justfiles)
    cargo-nextest # Rust test runner
    cargo-mutants # Mutation target inferred by @smoothbricks/nx-plugin
    # Go toolchain for packages/lmao-ttsc/plugin (ttsc transform plugin)
    go
    sccache # Shared Rust compile cache (cowshed cache layer 3)
    # Git hooks and formatters
    git-format-staged
    jq # Used in pre-commit hook and generally useful
    alejandra # Nix formatter
    awscli2 # For DynamoDB Local CLI testing/debugging
  ];

  # Use system Xcode for iOS simulator, signing, and instruments.
  # Nix Apple SDK is build-only — no simctl/simulator runtimes, and nix's
  # clang doesn't support -index-store-path which xcodebuild passes.
  # https://devenv.sh/recipes/macos/
  # https://github.com/cachix/devenv/issues/1674
  apple.sdk = null;

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

  # https://devenv.sh/processes/
  # DynamoDB Local for index store integration tests
  services.dynamodb-local.enable = true;
  # Default port 8000; available at http://127.0.0.1:8000 when `devenv up` is running

  # https://devenv.sh/scripts/#entershell
  # This runs when entering the devenv shell
  # - When using the devenv wrapper from tooling/, restore the original working directory
  #   (The wrapper runs devenv from tooling/direnv but we want the shell to start where the user was)

  # PATH order: most-specific → least-specific.
  # ttsc needs the native TypeScript 7 binary while Nx imports the TypeScript 6 API.
  # On Darwin, remove Nix CC/CXX so xcodebuild finds Xcode clang with -index-store-path support.
  # Bun/node native addons find compilers through node-gyp.
  enterShell = ''
    cd "$DEVENV_ROOT/../.."
    export PATH="$("$PWD/tooling/direnv/repo-path")"
    export TTSC_TSGO_BINARY="$PWD/node_modules/@typescript/native/bin/tsc"
    bun "$DEVENV_ROOT/enter-shell.ts" || exit $?

    ${lib.optionalString pkgs.stdenv.isDarwin "unset CC CXX"}

    if [ -n "$DEVENV_SHELL_PWD" ]; then
      cd "$DEVENV_SHELL_PWD"
    fi
  '';

  # See full reference at https://devenv.sh/reference/options/
}
