#!/bin/sh
# The repository's declared land gate: main's .cowshed.toml names it as `[land] gate`, and `cowshed land <ws>` runs
# `sh tooling/land-host.sh --gate <ws>` in main's checkout and main's pure environment once it holds the land turn and
# has rebased <ws> onto main (specs/cowshed/02_workspaces.md "A declared land", step 4). The route around the gate is
# cowshed's: the turn, the rebase, the fast-forward (and the re-gate when main moved), retiring the workspace, the push
# and the after-land commands, main's build among them. This script checks the rebased tree and nothing else: it moves
# neither main nor the workspace, and its exit status is the gate's verdict. The steps:
#
# 1. Approve <workspace>'s .envrc for direnv only when it is main's approved one, or the operator approved it by hand.
# 2. Stop <workspace>'s Nx daemon. Its file map can be stale across a rebase (fseventsd drops events), and a gate
#    that hashes a tree other than the rebased one proves nothing about it. The gate job starts no daemon of its own
#    (every job runs with NX_DAEMON_START=never, specs/cowshed/04_sandbox.md), so after the stop it has no stale map.
# 3. Gate the rebased tree on the host: `nx run-many -t gate --nx-bail=false`. The root Nx graph owns
#    every project's lint/test/build and the host-e2e aggregate. Cached tasks replay unless their inputs changed.
#    The host, unlike the land sandbox, can run the real-APFS tests. Any nonzero gate exit is this script's exit.
#    Before the gate, the wiring check refuses what the rebased tree adds since the commit it was rebased onto that no
#    production code reaches (AGENTS.md rule 7); a refusal is this script's exit.
#    A failure summary keeps unique tasks and exact diagnostics; the complete failed output is retained. The reporter
#    is main's native build of @smoothbricks/failure-summary (dist/bin/<platform>/smoo-nx-failures), which the
#    declared after-land build keeps current.
#    The gate runs as a cowshed gate job (`cowshed exec --class gate --host`): it is never CPU-capped, and its class's
#    priority outranks background execs, not every class (05_gateway.md, "Host scheduling", "Classes").
#    A red names, for each task the gate failed, the commits of the land that change a file Nx hashes for it or for a
#    task it waits for (tooling/land-attribution.ts); naming never changes the verdict. A red land's next attempt gates
#    whole again, and Nx's cache replays every task whose inputs did not move.
#    After the gate passes, `bun run check:linux` cross-lints the same tree for Linux: the pre-push hook refuses a
#    tree whose Linux cross check is not cached, so a land that skipped it would leave main unpushable.
#    The shipping seam then builds this host's production CLI and runs cowshed:release-check on those dist bytes.
#    The guard remains uncached and dependency-free; both phases report their wall time and refuse a red land.
#    Once the wiring preflight, gate, Linux cross check and shipping seam all pass, each captured invocation's
#    succeeded results are published through smoo-nx-publish to the accepted Nx store. Sheds stay read-only.
#    Hashes are captured before the next invocation overwrites its summary; a red publishes nothing.
#    The cache never blocks a land: unreadable task hashes or a failed publish are said, and the land goes on.
#
# The gate builds on the workspace's own build volume. Choosing between it and a clone of main's latest seed
# (16_build_volumes.md, "Picking a land's build volume") is to move into `cowshed land`; until it does, a land whose
# own volume is far from the seed compiles more Cargo units than the pick would have left it.
set -eu

here=$(cd "$(dirname "$0")" && pwd)
cd "$here/.."

# The only call is `--gate <workspace>`. land-decide.ts refuses anything else with the usage line, and main itself (a
# tested call); a call without `--gate` reaches it with no arguments, which is usage.
case ${1-} in
--gate) shift ;;
*) set -- ;;
esac
workspace=$(bun "$here/land-decide.ts" workspace "$@")
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
head=$(git -C "$checkout" rev-parse HEAD)
# The commit <workspace> was rebased onto, where its own commits begin. Main may have gained a doc commit since: the
# fast-forward then refuses, and cowshed rebases onto it and gates again. The workspace holds no object main gained
# after the rebase, so main's objects are read beside its own (an alternate, never a fetch: this script moves neither).
base=$(GIT_ALTERNATE_OBJECT_DIRECTORIES="$(git -C "$main" rev-parse --absolute-git-dir)/objects" git -C "$checkout" merge-base HEAD "$(git -C "$main" rev-parse HEAD)")
started=$(date +%s)
publisher="$here/../packages/nx-plugin/dist/bin/smoo-nx-publish.js"


say() {
  echo "land-host: $*" >&2
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

# `nx <args>` in <workspace>'s own devenv. A gate takes no input, and a task that reads stdin must not eat the caller's.
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

# Retain only validated invocation lists in one gate-owned temporary namespace.
capture_hashes() {
  [ -n "$hashes_dir" ] || return 0
  capture_file=$hashes_dir/$1
  capture_since=$2
  capture_subject=$3
  "$publisher" hashes "$checkout/.nx/cache/run.json" "$capture_since" >"$capture_file" || {
    capture_status=$?
    say "could not read $capture_subject task hashes from $checkout/.nx/cache/run.json (status $capture_status); the land goes on without publishing: the cache never blocks a land"
    if [ -f "$capture_file" ]; then
      rm "$capture_file" || say "could not release failed hash capture $capture_file"
    fi
  }
}
# Gates exactly the rebased tree. Reporting never changes Nx's status or grants permission to land.
gate_workspace() {
  say "gating $head (rebased onto $base) on the host"
  workspace_nx daemon --stop >&2 || {
    say "could not stop $workspace's Nx daemon; a stale file map could gate a tree other than $head"
    exit 1
  }
  # The shed's Cargo build-script records against the gate's shell, the shell main's seed is built in (the declared
  # after-land build): a variable they disagree on is one the shed's own shells carried, and this gate reruns every
  # build script that read it and rebuilds what depends on them (16_build_volumes.md, "One Cargo environment on every
  # path"). Said, never a reason to stop: drift costs the gate time, not correctness.
  (cd "$checkout" && direnv exec "$checkout" bun "$here/land-cargo-env.ts" "$checkout") >&2 ||
    say "comparing $workspace's Cargo build-script environment with the land shell failed (status $?); the land goes on"
  # Done means the production path calls what the change adds (AGENTS.md rule 7): the wiring check refuses each item
  # the rebased tree adds since $base that no production code reaches, by name and file:line. It runs before the gate
  # it would otherwise wait behind, as a gate job like the gate; a tree it already judged is a cache hit.
  hashes_dir=$(mktemp -d "${TMPDIR:-/tmp}/land-hashes.XXXXXX") || {
    say "could not allocate Nx publication lists (status $?); the land goes on without publishing: the cache never blocks a land"
    hashes_dir=
  }
  if [ -n "$hashes_dir" ]; then
    trap 'if ! rm -r "$hashes_dir"; then say "could not release Nx publication lists in $hashes_dir"; fi' EXIT
  fi
  say "checking that $head wires what it adds since $base"
  wiring_began=$(date +%s)
  gate_job bun nx run @smoothbricks/codebase:wiring "--args=--base $base" --outputStyle=static-failures-only >&2 || {
    wiring_status=$?
    say "the wiring check exited $wiring_status; the land stops: wire or delete each item it names"
    exit "$wiring_status"
  }
  capture_hashes 00-wiring.json "$wiring_began" "the wiring check's"
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
  capture_hashes 01-gate.json "$began" "the gate's"
  gate_summary=$(cksum <"$checkout/.nx/cache/run.json" 2>/dev/null || :)
  cross_began=$(date +%s)
  say "cross-linting $head for Linux"
  gate_job bun run check:linux >"$output" 2>&1 || {
    linux_status=$?
    replay_gate_output "$output"
    say "Linux cross check exited $linux_status; the land stops; complete output: $output"
    exit "$linux_status"
  }
  rm "$output"
  # The pre-push hook asks Nx for cargo-lint-cross, the cross check's own run and not the gate's: main finds it cached
  # by adopting this workspace's build volume, which a held target refuses, and a green land is then unpushable. When
  # the cross check wrote a run summary of its own, its task hashes are read the way the gate's were and published too.
  if [ "$(cksum <"$checkout/.nx/cache/run.json" 2>/dev/null || :)" != "$gate_summary" ]; then
    capture_hashes 02-cross.json "$cross_began" "the Linux cross check's"
  fi
  shipping_started=$(date +%s)
  say "building the host production CLI for shipping verification"
  gate_job bun nx run cowshed:cli-native:production --outputStyle=static-failures-only >&2 || {
    shipping_status=$?
    say "host production CLI exited $shipping_status after $(($(date +%s) - shipping_started))s; the land stops"
    exit "$shipping_status"
  }
  shipping_built=$(date +%s)
  capture_hashes 03-shipping-producer.json "$shipping_started" "the shipping producer's"
  shipping_check_began=$(date +%s)
  say "checking the shipping CLI bytes with cowshed:release-check"
  gate_job bun nx run cowshed:release-check --outputStyle=static-failures-only >&2 || {
    shipping_status=$?
    say "shipping release-check exited $shipping_status after $(($(date +%s) - shipping_check_began))s; the land stops"
    exit "$shipping_status"
  }
  shipping_checked=$(date +%s)
  capture_hashes 04-shipping-check.json "$shipping_check_began" "the shipping guard's"
  say "shipping verification: producer $((shipping_built - shipping_started))s, release-check $((shipping_checked - shipping_check_began))s, total $((shipping_checked - shipping_started))s"
  # All checks passed; publication is a loud, nonblocking cache optimization.
  if [ -n "$hashes_dir" ]; then
    for list in "$hashes_dir"/*.json; do
      [ -f "$list" ] || continue
      gate_job "$publisher" publish "$checkout" "$list" >&2 || say "publishing Nx results failed (status $?); the land goes on: the cache never blocks a land"
    done
  fi
}

allow_envrc
gate_workspace
say "gate passed for $head in $(($(date +%s) - started))s"
