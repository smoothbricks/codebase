#!/bin/sh
# The install that follows a land which changed cowshed: 'land-install.sh <repo> <before> <head>', run in main's devenv
# once the land's turn has ended. <repo> names the repository's land turn ('cowshed land-turn <repo>'); <before> and
# <head> are the commits main moved between. The gateway daemon and the cowshed an operator types are one installed
# artifact that only 'cowshed setup' replaces (specs/cowshed/05_gateway.md, "The installed cowshed"), and setup refuses
# to restart the gateway while any land holds its turn, the land that runs it included ("An install refuses while a land
# runs"). A land therefore cannot install from inside its own turn. In order:
#
# 1. Ask whether the land changed cowshed or a project it depends on (land-install.ts wanted). Nothing else happens
#    when it did not; when Nx cannot say, the install goes on.
# 2. Build the release CLI and stage it, as a land turn of its own: 'cowshed land-turn <repo>' queues behind the lands
#    ahead and holds main still while the build compiles, so no land's fast-forward or build races it. The CLI, the
#    Nx cache server bundle it installs with and the mbx flake its setup pins are copied to .cache/cowshed-release/<head>/,
#    laid out as a distribution (<package>/dist/bin/<platform>/cowshed, <package>/nix/mbx as the npm package ships it,
#    the server under <package>/node_modules). The debug build every land
#    replays into dist/bin replaces the release binary there; a stage no later build rewrites keeps the command this
#    script prints true until three newer stages exist. The turn ends with the stage.
# 3. With no turn held, ask the staged build's own doctor whether setup would raise an administrator dialog: its host
#    storage findings come from the plan setup executes. When it would, or when doctor cannot say, nothing is run
#    unattended.
# 4. Ask gateway status for the work an install refuses under: a land turn's holder, or another install holding the
#    install slot, which setup would wait for with no deadline. When there is any,
#    the install is deferred.
# 5. Run the staged build's setup once. A refusal it answers is told apart from a build it will not install by a fresh
#    status: work that began after the first read defers the install too.
#
# stdout carries only land-status lines, one per outcome: '<UTC time> <head> install attended|unverified|deferred|
# installed|failed|skipped ...' (land-install.ts, landStatusLine). Run as the declared after-land command, they are kept
# with every other word (stderr) in the land's records, lands/<head>.after<n>.log beside lands.log (specs/cowshed/
# 02_workspaces.md "A declared land", step 6): there whoever watches the install reads them. Nothing here waits for the
# host: an install it cannot take now is deferred with the command to rerun, and whoever sees the named work end
# reruns it.
# Nx and setup run through 'direnv exec <main>', whatever environment started the script: setup installs the PATH of the
# shell it runs in and its build-state discovery runs cargo, neither of which a pure environment has.
set -eu

here=$(cd "$(dirname "$0")" && pwd)
main=$(cd "$here/.." && pwd)
# Where setup looks for the Nx cache server bundle of a distribution's CLI (gateway_service.rs, NX_CACHE_SERVER_BUNDLE).
server=node_modules/@smoothbricks/nx-cache-server/dist/server/nx-cache-server.js

say() {
  echo "land-install: $*" >&2
}

# Runs a command in main's devenv.
devenv() {
  direnv exec "$main" "$@"
}
# One land-status line for $head on stdout: record KIND [WHY [COMMAND]].
record() {
  bun "$here/land-install.ts" record "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$head" "$@"
}

# The verdict of land-install.ts readiness over the staged build's gateway status in $readiness; the status's own exit
# is left in $status_status for the line that cannot read it.
gateway_readiness() {
  status_status=0
  status=$(cd / && "$binary" --json gateway status </dev/null) || status_status=$?
  readiness=$(bun "$here/land-install.ts" readiness "$status")
}

# Runs under the repository's land turn, so main stands still. Exit 3 means it said why in a status line of its own.
build_release() {
  stage=$(bun "$here/land-install.ts" stage-dir "$main" "$head")
  if [ -x "$stage/dist/bin/$platform/cowshed" ] && [ -f "$stage/$server" ] && [ -f "$stage/nix/mbx/flake.nix" ]; then
    say "the production build for $head is already staged in $stage"
    return 0
  fi
  say "building the production CLI ($target) and the Nx cache server bundle at main $(git -C "$main" rev-parse HEAD), for $head"
  build_status=0
  (cd "$main" && devenv bun nx run nx-cache-server:bundle --outputStyle=static-failures-only </dev/null &&
    devenv bun nx run "cowshed:$target:production" --outputStyle=static-failures-only </dev/null) >&2 || build_status=$?
  if [ "$build_status" -ne 0 ]; then
    record failed "the production build of cowshed:$target exited $build_status"
    return 3
  fi
  built=$main/packages/cowshed/dist/bin/$platform/cowshed
  bundle=$main/packages/nx-cache-server/dist/server/nx-cache-server.js
  if [ ! -x "$built" ] || [ ! -f "$bundle" ]; then
    record failed "the production build left no $built or no $bundle"
    return 3
  fi
  # A stage is whole or absent: it is assembled beside its name and renamed into it. This runs under the land turn, so a
  # .partial that a crashed run left is nobody's.
  root=$(dirname "$stage")
  mkdir -p "$root"
  rm -rf "$root"/*.partial "$stage"
  mkdir -p "$stage.partial/dist/bin/$platform" "$stage.partial/$(dirname "$server")" "$stage.partial/nix/mbx"
  cp -p "$built" "$stage.partial/dist/bin/$platform/cowshed"
  cp -p "$bundle" "$stage.partial/$server"
  cp -p "$main/packages/cowshed/nix/mbx/flake.nix" "$main/packages/cowshed/nix/mbx/flake.lock" "$stage.partial/nix/mbx/"
  mv "$stage.partial" "$stage"
  # Newest first by modification time; stale only ever names full commit ids, so no other entry of the root is touched.
  # shellcheck disable=SC2012
  for old in $(ls -1t "$root" | bun "$here/land-install.ts" stale 3); do
    rm -rf "${root:?}/$old"
  done
  say "staged the production build for $head in $stage"
}

if [ "$#" -ge 1 ] && [ "$1" = --build ]; then
  [ "$#" -eq 4 ] || {
    echo 'usage: land-install.sh --build <head> <target> <platform>' >&2
    exit 64
  }
  head=$2 target=$3 platform=$4
  build_release
  exit
fi

[ "$#" -eq 3 ] || {
  echo 'usage: land-install.sh <repo> <before> <head>' >&2
  exit 64
}
repo=$1 before=$2 head=$3
bun "$here/land-install.ts" commit "$before" "$head"

affected=$(cd "$main" && devenv bun nx show projects --affected --base="$before" --head="$head" --json </dev/null) || affected=
changed=$(git -C "$main" diff --name-only "$before" "$head") || changed=
wanted=$(bun "$here/land-install.ts" wanted "$affected" "$changed")
case $wanted in
no) exit 0 ;;
unknown) say "cannot tell whether $head changed cowshed; installing as if it did" ;;
esac

cli=$(bun "$here/land-install.ts" target "$(uname -s)" "$(uname -m)")
if [ "$cli" = unsupported ]; then
  record skipped "no release CLI of cowshed installs on $(uname -s) $(uname -m)"
  exit 0
fi
target=${cli% *}
platform=${cli#* }

turn_status=0
cowshed land-turn "$repo" --workspace post-land-install -- sh "$here/land-install.sh" --build "$head" "$target" "$platform" || turn_status=$?
case $turn_status in
0) ;;
3) exit 0 ;;
*)
  record failed "the land turn for the production build of $head exited $turn_status"
  exit 0
  ;;
esac

stage=$(bun "$here/land-install.ts" stage-dir "$main" "$head")
binary=$stage/dist/bin/$platform/cowshed
command=$(bun "$here/land-install.ts" command "$main" "$binary")
if [ ! -x "$binary" ]; then
  record skipped "no staged production build of $head at $binary"
  exit 0
fi

# Would setup raise an administrator dialog? Doctor exits 5 for an unhealthy host and still prints its report.
doctor_status=0
doctor=$(cd / && "$binary" --json doctor </dev/null) || doctor_status=$?
verdict=$(bun "$here/land-install.ts" prompt "$doctor")
case $verdict in
none) ;;
needed\ *)
  record attended "${verdict#needed }" "$command"
  exit 0
  ;;
*)
  record unverified "${verdict#unknown } (doctor exited $doctor_status)" "$command"
  exit 0
  ;;
esac

# What an install would refuse under: a land holding its repository's turn, an install holding the slot.
gateway_readiness
case $readiness in
ready) ;;
busy\ *)
  record deferred "${readiness#busy }" "$command"
  exit 0
  ;;
*)
  record unverified "${readiness#unknown } (gateway status exited $status_status)" "$command"
  exit 0
  ;;
esac

setup_status=0
setup_output=$(devenv "$binary" --json setup </dev/null) || setup_status=$?
outcome=$(bun "$here/land-install.ts" setup "$setup_status" "$setup_output")
case $outcome in
installed) record installed ;;
refused\ *)
  # Work that began after the status was read: name it from a fresh status, never from the words of the refusal.
  gateway_readiness
  case $readiness in
  busy\ *) record deferred "${readiness#busy }" "$command" ;;
  *) record failed "${outcome#refused }" ;;
  esac
  ;;
*) record failed "${outcome#failed }" ;;
esac
