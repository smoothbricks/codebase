#!/usr/bin/env bash

# Go to project root
cd "$DEVENV_ROOT/../.."

# Install dependencies first so node_modules/.bin tools are available
bun install --no-summary

# Add tooling and node_modules/.bin to PATH
export PATH="$PWD/tooling:$PWD/node_modules/.bin:$PATH"

# Make sure Biome is executable
chmod +x node_modules/@biomejs/cli-*/biome 2>/dev/null || true

# Update package.json with current versions from devenv
NODE_VERSION=$(node --version | sed 's/v//')
BUN_VERSION=$(bun --version)
CURRENT_NODE_ENGINE=$(jq -r '.engines.node // ""' package.json)
CURRENT_PKG_MANAGER=$(jq -r '.packageManager // ""' package.json)
EXPECTED_NODE_ENGINE=">=${NODE_VERSION%%.*}.0.0"
EXPECTED_PKG_MANAGER="bun@$BUN_VERSION"

if [[ "$CURRENT_NODE_ENGINE" != "$EXPECTED_NODE_ENGINE" ]] || [[ "$CURRENT_PKG_MANAGER" != "$EXPECTED_PKG_MANAGER" ]]; then
  # Update package.json and format with biome (now available via node_modules/.bin)
  jq --arg node "$EXPECTED_NODE_ENGINE" \
     --arg bun "$EXPECTED_PKG_MANAGER" \
     '.engines.node = $node | .packageManager = $bun' \
     package.json | biome format --stdin-file-path=package.json > package.json.tmp && mv package.json.tmp package.json
fi

# Apply workspace git configuration
if [ -f "$DEVENV_ROOT/apply-workspace-git-config.sh" ]; then
  "$DEVENV_ROOT/apply-workspace-git-config.sh"
fi