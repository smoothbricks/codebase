#!/bin/sh
# The host half of a land: `land-host.sh <workspace> [cowshed land args...]`. A host that lands many workspaces runs
# this under one lock per repository, so lands gate one at a time; nothing here takes a lock. The steps:
#
# 1. Rebase <workspace> onto main's head. A conflict stops the land and leaves <workspace> as it was. Main can move
#    between reading its head and the rebase; cowshed fences on the head it was told, and a fence that fired for
#    that reason alone is retried against the newer head.
# 2. Pick the build volume the gate builds on (16_build_volumes.md, "Picking a land's build volume"): the workspace's
#    own, or a clone of main's latest seed, whichever leaves fewer Cargo compile seconds for the rebased tree
#    (tooling/land-pick.ts), reseeding only onto the seed it priced. A pick that cannot be priced is said and the gate
#    builds on the workspace's own volume; a reseed that fails is said, and the gate builds on whichever volume it left
#    linked: the pick decides where the gate starts, never whether the tree lands.
# 3. Stop <workspace>'s Nx daemon. Its file map can be stale across a rebase (fseventsd drops events), and a gate
#    that hashes a tree other than the rebased one proves nothing about it. The gate starts a fresh daemon.
# 4. Gate the rebased tree on the host: `nx run-many -t gate --nx-bail=false`. The root Nx graph owns
#    every project's lint/test/build and the host-e2e aggregate. Cached tasks replay unless their inputs changed.
#    The host, unlike the land sandbox, can run the real-APFS tests. Any nonzero gate exit stops the land.
#    Before the gate, the wiring check refuses what the rebased tree adds since main's head that no production code
#    reaches (AGENTS.md rule 7); a refusal stops the land.
#    A failure summary keeps unique tasks and exact diagnostics; the complete failed output is retained. The reporter
#    is main's native build of @smoothbricks/failure-summary (dist/bin/<platform>/smoo-nx-failures), which step 5
#    keeps current.
#    The gate runs as a cowshed gate job (`cowshed exec --class gate --host`): it is never CPU-capped, and its class's
#    priority outranks background execs, not every class (05_gateway.md, "Host scheduling", "Classes").
#    A red names, for each task the gate failed, the commits of the land that change a file Nx hashes for it or for a
#    task it waits for (tooling/land-attribution.ts); naming never changes the verdict. A red land's next attempt gates
#    whole again, and Nx's cache replays every task whose inputs did not move.
#    After the gate passes, `bun run check:linux` cross-lints the same tree for Linux: the pre-push hook refuses a
#    tree whose Linux cross check is not cached, so a land that skipped it would leave main unpushable.
#    The shipping seam then builds this host's production CLI and runs cowshed:release-check on those dist bytes.
#    The guard remains uncached and dependency-free; both phases report their wall time and refuse a red land.
#    After the gate and the Linux cross check pass, the gate run's task results are published to the
#    project's localhost Nx cache, the only writer to it (sheds only read it). The cache never blocks a land: task
#    hashes that cannot be read from the gate's run summary, or a publish that fails, are said and the land goes on.
# 5. `cowshed land` fast-forwards main to exactly the gated commit and retires the workspace. If main moved
#    meanwhile, rebase and gate the new tree; this is a source fence, never a retry of a red gate.
# 6. Build main's outputs at the landed commit (`nx run-many -t build` in main). Main's built products (the plugin's,
#    cowshed's and the CLI's dist) are what developer links in other checkouts run; until they are rebuilt they lag the
#    sources the land just moved main to, and those links refuse. Nx restores the outputs the gate cached for the same
#    tree, so this replays rather than compiles. A failed build is said with its status; the land stands.
set -eu

here=$(cd "$(dirname "$0")" && pwd)
cd "$here/.."

# The first argument is the workspace; land-decide.ts refuses a missing one and main itself (a tested call).
workspace=$(bun "$here/land-decide.ts" workspace "$@")
shift
main=$(cowshed path main)
checkout=$(cowshed path "$workspace")
# The failure reporter's platform directory, named as the native tool packages name dist/bin/<platform>.
case "$(uname -s)-$(uname -m)" in
Darwin-arm64) platform=darwin-arm64 ;;
Darwin-x86_64) platform=darwin-x64 ;;
Linux-aarch64 | Linux-arm64) platform=linux-arm64 ;;
Linux-x86_64) platform=linux-x64 ;;
*) platform="unsupported-$(uname -s)-$(uname -m)" ;;
esac
failure_summary="$here/../packages/failure-summary/dist/bin/$platform/smoo-nx-failures"
started=$(date +%s)


say() {
  echo "land-host: $*" >&2
}

# Rebases <workspace> onto main's head and sets $base to that head and $head to the rebased commit. $built_tree is the
# tree the checkout had before: the tree its build volume was last built at, by its own builds or the previous gate.
rebase_onto_main() {
  built_tree=$(git -C "$checkout" rev-parse 'HEAD^{tree}')
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
    verdict=$(bun "$here/land-decide.ts" after-rebase "$outcome")
    case $verdict in
    retry) say "main moved past $base before the rebase began; rebasing onto its new head" ;;
    *) return 1 ;;
    esac
  done
}

# direnv trusts an .envrc by its content. The operator approved main's; a workspace's is approved here only when it is
# byte-identical. One that differs (a change to the .envrc) runs only when the operator has already approved it by
# hand, so a gate never runs an environment nobody approved. direnv reports an approved .envrc as "Found RC allowed 0".
allow_envrc() {
  envrc_same=differs
  envrc_status=
  if cmp -s "$main/.envrc" "$checkout/.envrc"; then
    envrc_same=same
  else
    envrc_status=$(cd "$checkout" && direnv status) || envrc_status=
  fi
  verdict=$(bun "$here/land-decide.ts" envrc "$envrc_same" "$envrc_status")
  case $verdict in
  allow) direnv allow "$checkout" ;;
  approved) ;;
  *)
    say "$checkout/.envrc differs from main's approved .envrc; review it and run 'direnv allow $checkout', or rebase"
    exit 1
    ;;
  esac
}

# Points <workspace> at the build volume that leaves the gate least to compile for $head: its own, or a clone of main's
# latest seed. Only where the gate starts is chosen here. A preview that fails, a failed pricing and a seed that changed
# after it was priced are said, and the gate builds on the workspace's own volume. A reseed that fails is said with
# cowshed's status, and the gate builds on whichever volume it left linked: a reseed links its clone before it records
# and releases the old volume (16_build_volumes.md, "Workspace reseed").
pick_build_volume() {
  preview=$(cowshed --json reseed "$workspace" --from main --dry-run) || {
    status=$?
    printf '%s\n' "$preview" >&2
    case $(bun "$here/land-pick.ts" refusal "$preview") in
    not-found) say "the reseed preview found nothing to price; the gate builds on $workspace's own build volume" ;;
    *) say "the reseed preview failed (cowshed exited $status); the gate builds on $workspace's own build volume" ;;
    esac
    return 0
  }
  choice=$(cd "$checkout" && direnv exec "$checkout" bun "$here/land-pick.ts" price "$checkout" "$built_tree" "$preview" </dev/null) || {
    say "could not price $workspace's build volumes; the gate builds on its own"
    return 0
  }
  case $choice in
  "seed "*) priced=${choice#seed } ;;
  *) return 0 ;;
  esac
  # The reseed clones main's latest seed as it then stands, and a fork or land can publish another while the pick
  # prices: reseed only onto the seed that was priced.
  again=$(cowshed --json reseed "$workspace" --from main --dry-run) || {
    status=$?
    printf '%s\n' "$again" >&2
    say "the reseed preview before the reseed failed (cowshed exited $status); the gate builds on $workspace's own build volume"
    return 0
  }
  current=$(bun "$here/land-pick.ts" seed "$again") || current=
  if [ "$current" != "$priced" ]; then
    say "main's seed changed after it was priced (priced $priced, now ${current:-unreadable}); the gate builds on $workspace's own build volume"
    return 0
  fi
  say "reseeding $workspace from main's seed $priced: it leaves less to compile for $head"
  cowshed --json reseed "$workspace" --from main >&2 || {
    status=$?
    say "the reseed did not finish (cowshed exited $status); the gate builds on whichever build volume it left linked to $workspace"
  }
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

# <command...> as a gate job of <workspace>, at its root and in its own devenv, taking no input.
gate_job() {
  cowshed exec "$workspace" --class gate --host -- direnv exec "$checkout" "$@" </dev/null
}

# Names, for each task the gate failed, the commits of $base..$head that change a file Nx hashes for it or for a task
# it waits for. tooling/land-attribution.ts reads the failed tasks from the run summary the gate's Nx wrote (as a gate
# job, which names Nx's cache directory) and names the commits; the workspace's own tooling/land-inputs.mts lists the
# files, under node, from the Nx the gate ran. Reporting only: a step that fails is said, and the verdict stands.
attribute_failures() {
  failed=$(gate_job bun "$here/land-attribution.ts" failed --since "$began") || {
    say "could not read which tasks the gate failed (status $?), so no commit is named"
    return 0
  }
  if [ -z "$failed" ]; then
    say "the gate's run summary names no failed task, so no commit is named"
    return 0
  fi
  inputs=$(mktemp "${TMPDIR:-/tmp}/land-inputs.XXXXXX") || {
    say "could not name the commits that reach the failed tasks (status $?)"
    return 0
  }
  # Task ids hold no whitespace, so each failed one is one argument: the split is intended.
  # shellcheck disable=SC2086
  if gate_job node "$checkout/tooling/land-inputs.mts" gate $failed >"$inputs" &&
    attribution=$(printf '%s\n' "$failed" | (cd "$checkout" && bun "$here/land-attribution.ts" attribute --base "$base" --head "$head" --inputs "$inputs")); then
    printf '%s\n' "$attribution" | while IFS= read -r line; do say "$line"; done
  else
    say "could not name the commits that reach the failed tasks"
  fi
  rm "$inputs" || say "could not remove $inputs (status $?)"
  return 0
}

# Gates exactly the rebased tree. Reporting never changes Nx's status or grants permission to land.
gate_workspace() {
  say "gating $head (rebased onto $base) on the host"
  workspace_nx daemon --stop >&2 || {
    say "could not stop $workspace's Nx daemon; a stale file map could gate a tree other than $head"
    exit 1
  }
  # The shed's Cargo build-script records against the gate's shell, the shell main's seed is built in (build_main): a
  # variable they disagree on is one the shed's own shells carried, and this gate reruns every build script that read it
  # and rebuilds what depends on them (16_build_volumes.md, "One Cargo environment on every path"). Said, never a reason
  # to stop: drift costs the gate time, not correctness.
  (cd "$checkout" && direnv exec "$checkout" bun "$here/land-cargo-env.ts" "$checkout") >&2 ||
    say "comparing $workspace's Cargo build-script environment with the land shell failed (status $?); the land goes on"
  # Done means the production path calls what the change adds (AGENTS.md rule 7): the wiring check refuses each item
  # the rebased tree adds since $base that no production code reaches, by name and file:line. It runs before the gate
  # it would otherwise wait behind, as a gate job like the gate; a tree it already judged is a cache hit.
  say "checking that $head wires what it adds since $base"
  gate_job bun nx run @smoothbricks/codebase:wiring "--args=--base $base" --outputStyle=static-failures-only >&2 || {
    wiring_status=$?
    say "the wiring check exited $wiring_status; the land stops: wire or delete each item it names"
    exit "$wiring_status"
  }
  output=$(mktemp "${TMPDIR:-/tmp}/land-gate.XXXXXX")
  gate_status=0
  began=$(date +%s)
  # The landing gate is never CPU-capped: it is the run that moves main. It runs as a gate job, whose class's priority
  # outranks background execs. Its Nx run serves its own CPU pool, so its tasks' runners together stay within its
  # cores. Nothing here caps a width.
  gate_job bun nx run-many -t gate --parallel="$(sysctl -n hw.ncpu)" --nx-bail=false --outputStyle=static-failures-only >"$output" 2>&1 || gate_status=$?
  if [ "$gate_status" -ne 0 ]; then
    "$failure_summary" "$output" >&2 || {
      say "failure summary unavailable; complete gate output: $output"
      replay_gate_output "$output"
    }
    attribute_failures
    say "gate exited $gate_status; the land stops; complete output: $output"
    exit "$gate_status"
  fi
  replay_gate_output "$output"
  rm "$output"
  # The gate's task hashes are read now, straight after the gate, from the run summary the gate's own Nx began (one an
  # earlier invocation began is refused). The cache never blocks a land (05_gateway.md, "Publication"): hashes that
  # cannot be read are said, and the land goes on unpublished.
  hashes=$(mktemp "${TMPDIR:-/tmp}/land-hashes.XXXXXX")
  bun "$here/land-publish.ts" hashes "$checkout/.nx/cache/run.json" "$began" >"$hashes" || {
    say "could not read the gate's task hashes from $checkout/.nx/cache/run.json (status $?); the land goes on without publishing: the cache never blocks a land"
    rm "$hashes"
    hashes=
  }
  say "cross-linting $head for Linux"
  gate_job bun run check:linux >"$output" 2>&1 || {
    linux_status=$?
    [ -z "$hashes" ] || rm "$hashes"
    replay_gate_output "$output"
    say "Linux cross check exited $linux_status; the land stops; complete output: $output"
    exit "$linux_status"
  }
  rm "$output"
  shipping_started=$(date +%s)
  say "building the host production CLI for shipping verification"
  gate_job bun nx run cowshed:cli-native:production --outputStyle=static-failures-only >&2 || {
    shipping_status=$?
    say "host production CLI exited $shipping_status after $(($(date +%s) - shipping_started))s; the land stops"
    [ -z "$hashes" ] || rm "$hashes"
    exit "$shipping_status"
  }
  shipping_built=$(date +%s)
  say "checking the shipping CLI bytes with cowshed:release-check"
  gate_job bun nx run cowshed:release-check --outputStyle=static-failures-only >&2 || {
    shipping_status=$?
    say "shipping release-check exited $shipping_status after $(($(date +%s) - shipping_built))s; the land stops"
    [ -z "$hashes" ] || rm "$hashes"
    exit "$shipping_status"
  }
  shipping_checked=$(date +%s)
  say "shipping verification: producer $((shipping_built - shipping_started))s, release-check $((shipping_checked - shipping_built))s, total $((shipping_checked - shipping_started))s"
  [ -n "$hashes" ] || return 0
  # The gate and the Linux cross check passed, so the gate's task results are the land's to publish. The cache never
  # blocks a land: a failed publish is said, and the land goes on.
  gate_job bun "$here/land-publish.ts" publish "$checkout" "$hashes" >&2 || say "publishing the gate's Nx results failed (status $?); the land goes on: the cache never blocks a land"
  rm "$hashes"
}

# Builds main's outputs at the commit the land moved it to. The land already happened, so a failure is said, not undone.
build_main() {
  say "building main's outputs at $head"
  build_status=0
  (cd "$main" && direnv exec "$main" bun nx run-many -t build --outputStyle=static-failures-only </dev/null) >&2 || build_status=$?
  if [ "$build_status" -ne 0 ]; then
    say "main's build exited $build_status; the land stands, but main's built outputs lag its sources until it passes"
  fi
}
rebase_onto_main
rebased=$(date +%s)
allow_envrc
pick_build_volume
picked=$(date +%s)
gate_workspace
gated=$(date +%s)
say "rebase $((rebased - started))s, pick $((picked - rebased))s, gate $((gated - picked))s"

while :; do
  if outcome=$(cd "$main" && cowshed --json land "$workspace" --expected-target-head "$base" --expected-source-head "$head" "$@"); then
    printf '%s\n' "$outcome" >&2
    build_main
    exit 0
  fi
  printf '%s\n' "$outcome" >&2
  verdict=$(bun "$here/land-decide.ts" after-land "$outcome")
  case $verdict in
  gateAgain) ;;
  *) exit 1 ;;
  esac
  say "main moved while the gate ran; rebasing onto its new head and gating again"
  rebase_onto_main
  allow_envrc
  pick_build_volume
  gate_workspace
done
