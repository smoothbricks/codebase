#!/bin/sh
# The install that follows a land which changed cowshed: 'land-install.sh <before> <head>', run in main's devenv by the
# land's declared after commands once its turn has ended. <before> and <head> are the commits main moved between. The
# gateway daemon and the cowshed an operator types are one installed artifact that only 'cowshed setup' replaces
# (specs/cowshed/05_gateway.md, "The installed cowshed"), and setup refuses to restart the gateway while any land holds
# its turn, the land that runs it included ("An install refuses while a land runs"). A land therefore cannot install
# from inside its own turn, and nothing here takes a turn: a land never waits for its after commands (specs/cowshed/
# 02_workspaces.md "A declared land", step 6), and the build that used to hold one made the next land wait for it. In
# order:
#
# 1. Ask whether the land changed cowshed or a project it depends on (land-install.ts wanted). Nothing else happens
#    when it did not; when Nx cannot say, the install goes on.
# 2. When the stage for <head> is not already whole, judge readiness before building anything: the installed cowshed's
#    gateway status says whether a land holds its repository's turn or another install holds the install slot. When it
#    does, the install is deferred, and when the status cannot be read it is unverified; each names the command that
#    reruns this script, and nothing is built.
# 3. Under the release lock ('cowshed lock .cache/cowshed-release.lock'), build the release CLI in the stable build shed
#    'land-install' at <head>, through 'cowshed exec --host': the shed is the build's own tree, so main stands still, no
#    land's fast-forward races it and no land turn is held. It is forked once ('cowshed fork --ref') and re-pointed to
#    <head> by 'cowshed rebase --onto <head>' on every later install, which refuses a dirty tree by name; it is never
#    removed, so its build volume stays warm and every release build after the first is incremental, with no fork and
#    no rm per install. The CLI, the Nx cache server bundle it installs with and the mbx flake its setup pins are copied
#    from the shed to .cache/cowshed-release/<head>/, laid out as a distribution (<package>/dist/bin/<platform>/cowshed,
#    <package>/nix/mbx as the npm package ships it, the server under <package>/node_modules). The debug build every land
#    replays into dist/bin replaces the release binary there; a stage no later build rewrites keeps the command this
#    script prints true until three newer stages exist. The lock is held by this script's own re-entry,
#    'land-install.sh --under-lock <before> <head>', which is not a command to run by hand.
# 4. Ask the staged build's own doctor whether setup would raise an administrator dialog: its host storage findings
#    come from the plan setup executes. When it would, or when doctor cannot say, nothing is run unattended.
# 5. Judge readiness again, over the staged build's gateway status: work that began during the build defers the
#    install.
# 6. Run the staged build's setup once. A refusal it answers is told apart from a build it will not install by a fresh
#    status: work that began after the second read defers the install too.
#
# stdout carries only land-status lines, one per outcome: '<UTC time> <head> install attended|unverified|deferred|
# installed|failed|skipped ...' (land-install.ts, landStatusLine). Run as the declared after-land command, they are kept
# with every other word (stderr) in the land's records, lands/<head>.after<n>.log beside lands.log (specs/cowshed/
# 02_workspaces.md "A declared land", step 6): there whoever watches the install reads them. Nothing here waits for the
# host: an install it cannot take now is deferred with the command to rerun, and whoever sees the named work end
# reruns it. The one wait is the release lock, which orders installs as the install slot does.
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

# cowshed for main's project, whatever directory this runs from: a rerun starts wherever its operator is.
cs() {
  cowshed --project "$main" "$@"
}

# One land-status line for $head on stdout: record KIND [WHY [COMMAND]].
record() {
  bun "$here/land-install.ts" record "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$head" "$@"
}

# The verdict of land-install.ts readiness over the gateway status of the cowshed $1 names, in $readiness; the status's
# own exit is left in $status_status for the line that cannot read it.
gateway_readiness() {
  status_status=0
  status=$(cd / && "$1" --json gateway status </dev/null) || status_status=$?
  readiness=$(bun "$here/land-install.ts" readiness "$status")
}

# A stage is whole with its binary, the release record setup judges the binary by, the cache server bundle and the mbx
# flake its setup pins.
stage_whole() {
  [ -x "$stage/dist/bin/$platform/cowshed" ] && [ -f "$stage/dist/bin/$platform/cowshed.build" ] &&
    [ -f "$stage/$server" ] && [ -f "$stage/nix/mbx/flake.nix" ]
}

# Assembles the stage from the shed's build outputs $built and $bundle. A stage is whole or absent: it is assembled
# beside its name and renamed into it. The release lock is held, so a .partial found here is a crashed run's and
# removing every one of them is safe. It is chained with && because it runs where a failing command must stop it.
stage_release() {
  root=$(dirname "$stage") &&
    mkdir -p "$root" &&
    rm -rf "$root"/*.partial "$stage" &&
    mkdir -p "$stage.partial/dist/bin/$platform" "$stage.partial/$(dirname "$server")" "$stage.partial/nix/mbx" &&
    cp -p "$built" "$stage.partial/dist/bin/$platform/cowshed" &&
    cp -p "$built.build" "$stage.partial/dist/bin/$platform/cowshed.build" &&
    cp -p "$bundle" "$stage.partial/$server" &&
    cp -p "$shed/packages/cowshed/nix/mbx/flake.nix" "$shed/packages/cowshed/nix/mbx/flake.lock" "$stage.partial/nix/mbx/" &&
    mv "$stage.partial" "$stage" &&
    prune_stages
}

# Newest first by modification time; stale only ever names full commit ids, so no other entry of the root is touched.
prune_stages() {
  # shellcheck disable=SC2012
  for old in $(ls -1t "$root" | bun "$here/land-install.ts" stale 3); do
    rm -rf "${root:?}/$old"
  done
}

# Builds in the shed $ws, which stands at $head, and stages from it. Exit 3 means it said why in a status line of its
# own.
build_in_shed() {
  shed=$(cs -q path "$ws" </dev/null) || {
    record failed "cowshed path $ws exited $?"
    return 3
  }
  say "building the production CLI ($target) and the Nx cache server bundle for $head in $shed"
  build_status=0
  {
    cs exec "$ws" --host -- direnv exec . bun nx run nx-cache-server:bundle --outputStyle=static-failures-only </dev/null &&
      cs exec "$ws" --host -- direnv exec . bun nx run "cowshed:$target:production" --outputStyle=static-failures-only </dev/null
  } >&2 || build_status=$?
  if [ "$build_status" -ne 0 ]; then
    record failed "the production build of cowshed:$target exited $build_status"
    return 3
  fi
  built=$shed/packages/cowshed/dist/bin/$platform/cowshed
  bundle=$shed/packages/nx-cache-server/dist/server/nx-cache-server.js
  if [ ! -x "$built" ] || [ ! -f "$built.build" ] || [ ! -f "$bundle" ]; then
    record failed "the production build left no $built, no release record $built.build or no $bundle"
    return 3
  fi
  stage_status=0
  stage_release >&2 || stage_status=$?
  if [ "$stage_status" -ne 0 ]; then
    record failed "staging the production build of $head from $shed exited $stage_status"
    return 3
  fi
  say "staged the production build for $head in $stage"
}

# Points the stable build shed at $head, creating it on the first install, builds and stages in it. The shed is never
# removed. Exit 3 means it said why in a status line of its own.
build_release() {
  ws=land-install
  path_status=0
  cs -q path "$ws" </dev/null >/dev/null 2>&1 || path_status=$?
  case $path_status in
  0)
    say "re-pointing the shed $ws to $head"
    # stdout is the new head; stderr is the refusal, whose words (a dirty tree's paths among them) the record keeps.
    repoint_words=$(mktemp)
    repoint_status=0
    repointed=$(cs -q rebase "$ws" --onto "$head" </dev/null 2>"$repoint_words") || repoint_status=$?
    refusal=$(cat "$repoint_words")
    rm -f "$repoint_words"
    if [ "$repoint_status" -ne 0 ]; then
      record failed "the shed $ws cannot be re-pointed to $head (cowshed rebase exited $repoint_status): $refusal"
      return 3
    fi
    # A rebase onto a commit the shed is already past moves nothing, and the build would not be of $head.
    if [ "$repointed" != "$head" ]; then
      record failed "the shed $ws stands at $repointed after cowshed rebase --onto $head: it is never moved back to an older commit"
      return 3
    fi
    ;;
  3)
    say "forking the shed $ws at $head"
    fork_status=0
    cs fork "$ws" --ref "$head" </dev/null >&2 || fork_status=$?
    if [ "$fork_status" -ne 0 ]; then
      record failed "cowshed fork $ws --ref $head exited $fork_status"
      return 3
    fi
    ;;
  *)
    record failed "cowshed path $ws exited $path_status"
    return 3
    ;;
  esac
  build_in_shed
}

# The re-entry that holds the release lock ('--under-lock'): the part of an install that moves and builds in the shared
# shed, which is run by 'cowshed lock', never by hand.
under_lock=
if [ "${1:-}" = --under-lock ]; then
  under_lock=1
  shift
fi

[ "$#" -eq 2 ] || {
  echo 'usage: land-install.sh <before> <head>' >&2
  exit 64
}
before=$1 head=$2
bun "$here/land-install.ts" commit "$before" "$head"

if [ -z "$under_lock" ]; then
  affected=$(cd "$main" && devenv bun nx show projects --affected --base="$before" --head="$head" --json </dev/null) || affected=
  changed=$(git -C "$main" diff --name-only "$before" "$head") || changed=
  wanted=$(bun "$here/land-install.ts" wanted "$affected" "$changed")
  case $wanted in
  no) exit 0 ;;
  unknown) say "cannot tell whether $head changed cowshed; installing as if it did" ;;
  esac
fi

cli=$(bun "$here/land-install.ts" target "$(uname -s)" "$(uname -m)")
if [ "$cli" = unsupported ]; then
  record skipped "no release CLI of cowshed installs on $(uname -s) $(uname -m)"
  exit 0
fi
target=${cli% *}
platform=${cli#* }

stage=$(bun "$here/land-install.ts" stage-dir "$main" "$head")
binary=$stage/dist/bin/$platform/cowshed
command=$(bun "$here/land-install.ts" command "$main" "$binary")
rerun=$(bun "$here/land-install.ts" rerun "$main" "$before" "$head")

if [ -n "$under_lock" ]; then
  # An install ahead of this one in the lock may have staged $head while this one waited.
  if stage_whole; then
    say "the production build for $head is already staged in $stage"
    exit 0
  fi
  build_release
  exit 0
fi

if stage_whole; then
  say "the production build for $head is already staged in $stage"
else
  # A build the install would then be refused for is not started: what an install refuses under, a land holding its
  # repository's turn or an install holding the slot, is read before the build and again before setup.
  gateway_readiness cowshed
  case $readiness in
  ready) ;;
  busy\ *)
    record deferred "${readiness#busy }" "$rerun"
    exit 0
    ;;
  *)
    record unverified "${readiness#unknown } (gateway status exited $status_status)" "$rerun"
    exit 0
    ;;
  esac
  mkdir -p "$main/.cache"
  # WHY lock: after-jobs are separate processes, and once the install holds no land turn nothing orders two of them, so
  # no single owner of the shared build shed exists to message; a second install moving the shed under a running build
  # would build a tree that is neither head. The release lock orders installs as the install slot does ("The install
  # slot", specs/cowshed/05_gateway.md: a second installer waits in line), and only after-jobs take it, so no land waits
  # on it (specs/cowshed/02_workspaces.md "A declared land", step 6). It guards the shed's re-point, build and stage; what
  # runs under it starts no process that outlives it, since 'cowshed exec' jobs live in the workspace supervisor.
  lock_status=0
  cowshed lock "$main/.cache/cowshed-release.lock" -- sh "$here/land-install.sh" --under-lock "$before" "$head" </dev/null ||
    lock_status=$?
  case $lock_status in
  0) ;;
  3) exit 0 ;;
  *)
    record failed "the build under the release lock exited $lock_status"
    exit 0
    ;;
  esac
fi

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
gateway_readiness "$binary"
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
  gateway_readiness "$binary"
  case $readiness in
  busy\ *) record deferred "${readiness#busy }" "$command" ;;
  *) record failed "${outcome#refused }" ;;
  esac
  ;;
*) record failed "${outcome#failed }" ;;
esac
