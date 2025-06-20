#!/usr/bin/env bash
cd "$(git rev-parse --show-toplevel)" # Workspace root
TOOLING="$PWD/tooling"

# Add hook scripts, bun installed tools and Devenv to PATH:
export PATH="$TOOLING/git-hooks:$TOOLING:$PWD/node_modules/.bin:$TOOLING/direnv/.devenv/profile/bin:$PATH"

# Make an exclusion list to ignore files in submodules
SUBMODULE_EXCLUSIONS=$(git config --file .gitmodules --get-regexp '.path$' | awk '{ print "!" $2 "*" }' | tr '\n' ' ')

# Make bash abort on errors and output commands
set -e -o pipefail

# Run `eslint --fix` for files not supported by Biome, except for hidden files:
# - https://stackoverflow.com/questions/57947585/eslint-warning-file-ignored-by-default-use-a-negated-ignore-pattern
git-format-staged --verbose -f \
  "eslint-formatted.sh '{}'" \
   '!.*' '*.astro'

# Format all files that Biome supports, excluding git submodules:
git-format-staged --verbose -f \
  "biome check --files-ignore-unknown=true —use-editorconfig=true '--stdin-file-path={}' --fix" $SUBMODULE_EXCLUSIONS \
  '*.js' '*.ts' '*.jsx' '*.tsx' '*.json' '*.jsonc' '*.html' '*.css' '*.graphql' \

# Format other (Markdown) files with prettier:
git-format-staged --verbose -f \
  "prettier --ignore-unknown --stdin-filepath '{}'" $SUBMODULE_EXCLUSIONS \
  '!*.astro' '!*.js' '!*.ts' '!*.jsx' '!*.tsx' '!*.json' '!*.jsonc' '!*.html' '!*.css' '!*.graphql' \
  '*' # Positive patterns must be after negatives or it won't match!

# Nix
git-format-staged --verbose -f "alejandra" '*.nix'
