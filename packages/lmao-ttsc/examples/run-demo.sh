#!/bin/bash
#
# LMAO Transformer Demo
#
# Compiles demo-source.ts through Bun.build and the ttsc native plugin, then
# prints the transformed JavaScript. There is no classic TypeScript transformer.
#
# Usage: ./run-demo.sh

set -e

cd "$(dirname "$0")"

echo
echo "========================================"
echo "  LMAO TTSC BUN ADAPTER DEMO"
echo "========================================"
echo
echo "This demo shows the native tsgo transformer:"
echo "  - injecting package and source metadata into defineModule()"
echo "  - specializing child-span setup from compile-time capabilities"
echo "  - inlining tag, log, and result fluent writes"
echo "  - encoding literal log messages as Op-local u16 IDs"
echo
echo ">>> Compiling demo-source.ts through @ttsc/unplugin/bun..."
echo
bun run compile.ts

echo
echo "========================================"
echo ">>> Native transformer demo complete"
echo