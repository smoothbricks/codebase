#!/usr/bin/env bash
cd "$(git rev-parse --show-toplevel)"
TOOLING="$PWD/tooling"

export PATH="$("$TOOLING/direnv/repo-path")"

set -e -o pipefail

# Format exactly what is staged so the commit includes the formatter output.
git-format-staged

# Any `bun install` rewrites bun.lock workspace versions from package.json,
# reverting the stable-tag versions the release flow syncs (see
# packages/cli/src/monorepo/lockfile.ts). Self-heal here instead of failing a
# later commit on drift it didn't cause.
smoo monorepo sync-bun-lockfile-versions --stage

# Adding a workspace package is rare and easy to leave half-wired. Run the
# full monorepo validator only for newly staged package manifests.
smoo monorepo validate --fail-fast --only-if-new-workspace-package
