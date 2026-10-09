#!/bin/sh
set -eu

# Nix's developer directory is not Apple's toolchain. An already-selected SDK
# needs no discovery; otherwise xcrun follows the host's Xcode selection.
unset DEVELOPER_DIR
if [ -z "${SDKROOT:-}" ]; then
  SDKROOT=$(xcrun --sdk macosx --show-sdk-path)
fi
exec /usr/bin/clang -isysroot "$SDKROOT" "$@"
