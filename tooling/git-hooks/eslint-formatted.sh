#!/bin/sh
WORKSPACE_ROOT="$PWD" # Git executes this script from repository root
file="$1" # Path to lint + format
set -e -o pipefail

# Change working directory relative to formatted file, to support subdirectory eslint and prettier config:
cd "$(dirname $file)"

# Send eslint fixed output to stdout, report linting errors to stderr:
EFF_NO_LINK_RULES=true \
  eslint --output-file /dev/stderr \
    --format "$WORKSPACE_ROOT/.git/tooling/git-hooks/eslint-stdout.cjs" \
    --fix-dry-run \
    --stdin --stdin-filename "$file"
