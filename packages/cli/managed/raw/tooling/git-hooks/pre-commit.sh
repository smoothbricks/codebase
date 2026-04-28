#!/usr/bin/env bash
cd "$(git rev-parse --show-toplevel)"
TOOLING="$PWD/tooling"

export PATH="$TOOLING/git-hooks:$TOOLING:$PWD/node_modules/.bin:$TOOLING/direnv/.devenv/profile/bin:$PATH"

set -e -o pipefail

# Format exactly what is staged so the commit includes the formatter output.
git-format-staged

# Adding a workspace package is rare and easy to leave half-wired. Run the
# full monorepo validator only for newly staged package manifests.
smoo monorepo validate --fail-fast --only-if-new-workspace-package
