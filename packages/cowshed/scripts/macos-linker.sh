#!/bin/sh
set -eu

unset DEVELOPER_DIR SDKROOT
DEVELOPER_DIR=$(/usr/bin/xcode-select -p)
export DEVELOPER_DIR
sdk_root=$(/usr/bin/xcrun --sdk macosx --show-sdk-path)
exec /usr/bin/xcrun --sdk macosx clang -isysroot "$sdk_root" "$@"
