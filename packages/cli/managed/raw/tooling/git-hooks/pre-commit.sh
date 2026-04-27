#!/usr/bin/env bash
cd "$(git rev-parse --show-toplevel)"
TOOLING="$PWD/tooling"

export PATH="$TOOLING/git-hooks:$TOOLING:$PWD/node_modules/.bin:$TOOLING/direnv/.devenv/profile/bin:$PATH"

set -e -o pipefail

# Format exactly what is staged so the commit includes the formatter output.
git-format-staged

# Adding a workspace package is rare and easy to leave half-wired. Run the
# full monorepo validator only for newly staged package manifests; --fail-fast
# keeps pre-commit output focused on the first blocking setup issue.
if ! git diff --cached --quiet --diff-filter=A -- \
  'packages/*/package.json' \
  'targets/*/package.json'; then
  smoo monorepo validate --fail-fast
fi
