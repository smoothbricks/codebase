#!/bin/sh
# Managed by smoo. Native builds use the entered shell's compiler; foreign
# Linux builds enter its locked linux-cross profile. No registry bootstrap.
set -eu

if [ "$#" -lt 2 ]; then
  echo 'usage: napi-build.sh <target-triple> <napi-executable|--identity> [build-options...]' >&2
  exit 2
fi
target="$1"
action="$2"
shift 2
host="$(uname -s):$(uname -m)"
mode="${SMOO_NAPI_TOOLCHAIN_MODE:-native}"

case "$target:$host" in
  x86_64-unknown-linux-gnu:Linux:x86_64|aarch64-unknown-linux-gnu:Linux:aarch64|aarch64-unknown-linux-gnu:Linux:arm64)
    ;;
  *-unknown-linux-gnu:*)
    mode=linux-cross
    ;;
esac

if [ "$action" = --identity ]; then
  printf '%s:%s\n' "$mode" "$host"
  exit 0
fi

napi="$(realpath "$(command -v "$action")")"
if [ "$mode" = linux-cross ] && [ "${SMOO_NAPI_TOOLCHAIN_MODE:-native}" != linux-cross ]; then
  tooling_dir="$(CDPATH= cd "$(dirname "$0")" && pwd)"
  exec "$tooling_dir/devenv" -P linux-cross shell -- "$napi" build --target "$target" "$@"
fi

exec "$napi" build --target "$target" "$@"
