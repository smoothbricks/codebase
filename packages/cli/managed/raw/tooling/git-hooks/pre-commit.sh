#!/usr/bin/env bash
cd "$(git rev-parse --show-toplevel)"
TOOLING="$PWD/tooling"

# PATH order: most-specific → least-specific.
#   1. git-hooks/   – hook-specific helper scripts
#   2. tooling/     – the repo toolbox (curated tools like the smoo wrapper)
#   3. tooling/node_modules/.bin – toolbox installed deps (@smoothbricks/cli)
#   4. node_modules/.bin – root workspace deps (Nx, biome, etc.)
#   5. devenv profile – upstream native/system tools
export PATH="$TOOLING/git-hooks:$TOOLING:$TOOLING/node_modules/.bin:$PWD/node_modules/.bin:$TOOLING/direnv/.devenv/profile/bin:$PATH"

set -e -o pipefail

# Format exactly what is staged so the commit includes the formatter output.
git-format-staged

# Adding a workspace package is rare and easy to leave half-wired. Run the
# full monorepo validator only for newly staged package manifests.
smoo monorepo validate --fail-fast --only-if-new-workspace-package
