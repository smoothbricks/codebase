# https://devenv.sh/basics/
{
  inputs,
  pkgs,
  ...
}:
# https://devenv.sh/inputs/
let
  git-format-staged = inputs.git-format-staged.packages.${pkgs.stdenv.system}.default;
in {
  # https://devenv.sh/packages/
  packages = with pkgs;
    [
      # Pin Node.js version to match AWS Lambda runtime
      nodejs_22
      # Bun.sh for javascript dependencies
      bun
      # Git hooks and formatters
      git-format-staged
      jq # Used in pre-commit hook and generally useful
      alejandra # Nix formatter
    ]
    ++
    # On macOS, it's better to use the provided git as it uses the keychain for credentials storage.
    # Therefore, we only include git in the environment for non-macOS platforms.
    lib.optional (!pkgs.stdenv.isDarwin) [git];

  # https://devenv.sh/languages/
  # languages.nix.enable = true;

  # We're not using Devenv's pre-commit-hooks, because this repo's pre-commit hook
  # uses `git-format-staged` to format only the content that is about to be committed.
  # See https://devenv.sh/pre-commit-hooks/ for more details (uses Python pre-commit)

  # https://devenv.sh/processes/
  # processes.ping.exec = "ping example.com";

  # See full reference at https://devenv.sh/reference/options/
}
