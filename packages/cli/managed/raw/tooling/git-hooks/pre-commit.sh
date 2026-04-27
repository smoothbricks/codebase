#!/usr/bin/env bash
cd "$(git rev-parse --show-toplevel)"
TOOLING="$PWD/tooling"

export PATH="$TOOLING/git-hooks:$TOOLING:$PWD/node_modules/.bin:$TOOLING/direnv/.devenv/profile/bin:$PATH"

set -e -o pipefail

git-format-staged
