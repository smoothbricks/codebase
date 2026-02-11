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
  ];

  # https://devenv.sh/packages/
  packages = with pkgs; [
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

  # Set up PATH first so setup script can find tools
  enterShell = ''
    cd "$DEVENV_ROOT/../.."
    export PATH="$PWD/tooling:$PWD/node_modules/.bin:$PATH"
    bun ${./setup-environment.ts}

    # S3 integration tests (BunS3Archive) - MinIO runs when devenv up
    export S3_TEST_ENDPOINT="http://127.0.0.1:9000"
    export S3_TEST_BUCKET="test"

    if [ -n "$DEVENV_SHELL_PWD" ]; then
      cd "$DEVENV_SHELL_PWD"
    fi
  '';

  # See full reference at https://devenv.sh/reference/options/
}
