#!/bin/sh
# The host half of a land: `land-host.sh <workspace> [cowshed land args...]`. A host that lands many workspaces runs
# this under one lock per repository, so lands gate one at a time; nothing here takes a lock. The steps:
#
# 1. Rebase <workspace> onto main's head. A conflict stops the land and leaves <workspace> as it was. Main can move
#    between reading its head and the rebase; cowshed fences on the head it was told, and a fence that fired for
#    that reason alone is retried against the newer head.
# 2. Stop <workspace>'s Nx daemon. Its file map can be stale across a rebase (fseventsd drops events), and a gate
#    that hashes a tree other than the rebased one proves nothing about it. The gate starts a fresh daemon.
# 3. Gate the rebased tree on the host: `nx run-many -t gate --nx-bail=false`. The root Nx graph owns
#    every project's lint/test/build and the host-e2e aggregate. Cached tasks replay unless their inputs changed.
#    The host, unlike the land sandbox, can run the real-APFS tests. Any nonzero gate exit stops the land.
#    A failure summary keeps unique tasks and exact diagnostics; the complete failed output is retained.
# 4. `cowshed land` fast-forwards main to exactly the gated commit and retires the workspace. If main moved
#    meanwhile, rebase and gate the new tree; this is a source fence, never a retry of a red gate.
set -eu

here=$(cd "$(dirname "$0")" && pwd)
cd "$here/.."

[ "$#" -ge 1 ] || {
  echo "usage: $0 <workspace> [cowshed land args...]" >&2
  exit 64
}
workspace=$1
shift
[ "$workspace" != main ] || {
  echo "land-host: main is not a workspace to land" >&2
  exit 64
}
main=$(cowshed path main)
checkout=$(cowshed path "$workspace")
started=$(date +%s)


say() {
  echo "land-host: $*" >&2
}

# Rebases <workspace> onto main's head and sets $base to that head and $head to the rebased commit.
rebase_onto_main() {
  while :; do
    base=$(git -C "$main" rev-parse HEAD)
    # The old range must be explicitly cut off, never replayed by this ordinary rebase.
    (cd "$main" && direnv exec "$main" bun "$here/land-history.ts" "$checkout" "$base") || return 1
    if outcome=$(cowshed --json rebase "$workspace" --expected-onto-head "$base"); then
      printf '%s\n' "$outcome" >&2
      head=$(git -C "$checkout" rev-parse HEAD)
      return 0
    fi
    printf '%s\n' "$outcome" >&2
    case $outcome in
    *'"reason":"ontoMoved"'*) say "main moved past $base before the rebase began; rebasing onto its new head" ;;
    *) return 1 ;;
    esac
  done
}

# direnv trusts an .envrc by its content. The operator approved main's; a workspace's is approved here only when it is
# byte-identical. One that differs (a change to the .envrc) runs only when the operator has already approved it by
# hand, so a gate never runs an environment nobody approved. direnv reports an approved .envrc as "Found RC allowed 0".
allow_envrc() {
  if cmp -s "$main/.envrc" "$checkout/.envrc"; then
    direnv allow "$checkout"
  elif ! (cd "$checkout" && direnv status) | grep -qx 'Found RC allowed 0'; then
    say "$checkout/.envrc differs from main's approved .envrc; review it and run 'direnv allow $checkout', or rebase"
    exit 1
  fi
}

# `nx <args>` in <workspace>'s own devenv. A gate takes no input, and a task that reads stdin must not eat a loop's.
workspace_nx() {
  (cd "$checkout" && direnv exec "$checkout" bun nx "$@" </dev/null)
}

# Replay only the final 64 KiB under a 10-second bound; a blocked reader never authorizes a land.
replay_gate_output() {
  timeout --kill-after=1s 10s tail -c 65536 "$1" >&2 || {
    say "could not replay the gate output from $1; the land stops; complete output: $1"
    exit 1
  }
}

# Gates exactly the rebased tree. Reporting never changes Nx's status or grants permission to land.
gate_workspace() {
  say "gating $head (rebased onto $base) on the host"
  workspace_nx daemon --stop >&2 || {
    say "could not stop $workspace's Nx daemon; a stale file map could gate a tree other than $head"
    exit 1
  }
  output=$(mktemp "${TMPDIR:-/tmp}/land-gate.XXXXXX")
  gate_status=0
  # The landing gate is never CPU-limited: it is the run that moves main, so it takes
  # the whole host (Main's rule, 2026-10-16). A capped gate measured about 1% of this
  # machine and took 40 minutes, and the resulting timeouts were misread as test
  # failures. Exploratory runs are the ones that get capped, by being stopped.
  workspace_nx run-many -t gate --parallel="$(sysctl -n hw.ncpu)" --nx-bail=false --outputStyle=static-failures-only >"$output" 2>&1 || gate_status=$?
  if [ "$gate_status" -ne 0 ]; then
    (cd "$checkout" && direnv exec "$checkout" bun "$here/land-summary.ts" "$output") >&2 || {
      say "failure summary unavailable; complete gate output: $output"
      replay_gate_output "$output"
    }
    say "gate exited $gate_status; the land stops; complete output: $output"
    exit "$gate_status"
  fi
  replay_gate_output "$output"
  rm "$output"
}

rebase_onto_main
rebased=$(date +%s)
allow_envrc
gate_workspace
gated=$(date +%s)
say "rebase $((rebased - started))s, gate $((gated - rebased))s"

while :; do
  if outcome=$(cd "$main" && cowshed --json land "$workspace" --expected-target-head "$base" --expected-source-head "$head" "$@"); then
    printf '%s\n' "$outcome" >&2
    exit 0
  fi
  printf '%s\n' "$outcome" >&2
  case $outcome in
  *'"reason":"targetMoved"'*) ;;
  *) exit 1 ;;
  esac
  say "main moved while the gate ran; rebasing onto its new head and gating again"
  rebase_onto_main
  allow_envrc
  gate_workspace
done
