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
  zig =
    if pkgs.stdenv.isDarwin
    then inputs.zig.packages.${pkgs.stdenv.system}.brew."0.16.0"
    else inputs.zig.packages.${pkgs.stdenv.system}."0.16.0";
in {
  # https://devenv.sh/overlays/
  overlays = [
    inputs.nixpkgs-overlay.overlays.default
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
    # WASM and NAPI extensions
    zig
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
    package = pkgs.python312.withPackages (ps: [ps.pyarrow ps.pandas]);
  };

  # We're not using Devenv's pre-commit-hooks, because this repo's pre-commit hook
  # uses `git-format-staged` to format only the content that is about to be committed.
  # See https://devenv.sh/pre-commit-hooks/ for more details (uses Python pre-commit)

  # https://devenv.sh/processes/
  # DynamoDB Local for index store integration tests
  services.dynamodb-local.enable = true;
  # Default port 8000; available at http://127.0.0.1:8000 when `devenv up` is running

  # MinIO (S3-compatible) for BunS3Archive integration tests
  services.minio.enable = true;
  services.minio.buckets = ["test"];
  # S3 API on port 9000; web UI on 9001 when `devenv up` is running
  # Note: MinIO requires ~5% free disk. If tests fail with XMinioStorageFull, free disk space.

  # https://devenv.sh/scripts/#entershell
  # This runs when entering the devenv shell
  # - When using the devenv wrapper from tooling/, restore the original working directory
  #   (The wrapper runs devenv from tooling/direnv but we want the shell to start where the user was)

  # PATH order: most-specific → least-specific.
  #   1. tooling/     – the repo toolbox
  #   2. tooling/node_modules/.bin – toolbox installed deps
  #   3. node_modules/.bin – root workspace deps (Nx, biome, etc.)
  #   (devenv profile is already on $PATH by the time enterShell runs)
  enterShell = ''
     cd "$DEVENV_ROOT/../.."
     export PATH="$PWD/tooling:$PWD/tooling/node_modules/.bin:$PWD/node_modules/.bin:$PATH"
    bun "$DEVENV_ROOT/enter-shell.ts"

    # Unset nix CC/CXX so xcodebuild finds Xcode's clang (supports -index-store-path)
    # Zig has its own toolchain and doesn't use CC; bun/node native addons use node-gyp
    # which finds compilers via its own logic.
    unset CC CXX

    # S3 integration tests (BunS3Archive) - MinIO runs when devenv up
    export S3_TEST_ENDPOINT="http://127.0.0.1:9000"
    export S3_TEST_BUCKET="test"

    if [ -n "$DEVENV_SHELL_PWD" ]; then
      cd "$DEVENV_SHELL_PWD"
    fi
  '';

  # See full reference at https://devenv.sh/reference/options/
}
