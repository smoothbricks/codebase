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
  # processes.ping.exec = "ping example.com";

  # https://devenv.sh/scripts/#entershell
  # This runs when entering the devenv shell
  # - When using the devenv wrapper from tooling/, restore the original working directory
  #   (The wrapper runs devenv from tooling/direnv but we want the shell to start where the user was)

  # Set up PATH first so setup script can find tools
  enterShell = ''
    cd "$DEVENV_ROOT/../.."
    export PATH="$PWD/tooling:$PWD/node_modules/.bin:$PATH"
    bun ${./setup-environment.ts}

    if [ -n "$DEVENV_SHELL_PWD" ]; then
      cd "$DEVENV_SHELL_PWD"
    fi
  '';

  # See full reference at https://devenv.sh/reference/options/
}
