#!/bin/sh
# Managed by smoo. Native builds use the entered shell's compiler; foreign
# Linux builds enter its locked linux-cross profile. No registry bootstrap.
set -eu

if [ "$#" -lt 2 ]; then
  echo 'usage: napi-build.sh <target-triple> <napi-executable> [build-options...]' >&2
  exit 2
fi
target="$1"
napi="$(realpath "$(command -v "$2")")"
shift 2

case "$target:$(uname -s):$(uname -m)" in
  x86_64-unknown-linux-gnu:Linux:x86_64|aarch64-unknown-linux-gnu:Linux:aarch64|aarch64-unknown-linux-gnu:Linux:arm64)
    ;;
  *-unknown-linux-gnu:*)
    tooling_dir="$(CDPATH= cd "$(dirname "$0")" && pwd)"
    exec "$tooling_dir/devenv" -P linux-cross shell -- "$napi" build --target "$target" "$@"
    ;;
esac

exec "$napi" build --target "$target" "$@"
