#!/bin/bash
#
# LMAO Transformer Demo
#
# This script:
# 1. Compiles demo-source.ts with the LMAO transformer
# 2. Shows the transformed code (with injected moduleMetadata and line numbers)
# 3. Runs the compiled code
# 4. Prints the Arrow table showing the injected data
#
# Usage: ./run-demo.sh

set -e

cd "$(dirname "$0")"

echo
echo "========================================"
echo "  LMAO TRANSFORMER DEMO"
echo "========================================"
echo
echo "This demo shows the transformer injecting:"
echo "  - moduleMetadata (gitSha, filePath, moduleName) into createModuleContext()"
echo "  - .line(N) after ctx.log.info/debug/warn/error()"
echo "  - .line(N) after ctx.ok() and ctx.err()"
echo "  - Line number as 3rd argument to ctx.span()"
echo

echo ">>> Step 1: Compiling demo-source.ts with LMAO transformer..."
echo
bun run compile.ts

echo
echo "========================================"
echo ">>> Step 2: Running compiled code..."
echo "========================================"
echo
bun run demo-compiled.js

echo
echo ">>> Demo complete!"
echo
