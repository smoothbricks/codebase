#!/bin/sh
# The host half of a land: `land-host.sh <workspace> [cowshed land args...]`. A host that lands many workspaces runs
# this under one lock per repository, so lands gate one at a time; nothing here takes a lock. The steps:
#
# 1. Rebase <workspace> onto main's head. A conflict stops the land and leaves <workspace> as it was. Main can move
#    between reading its head and the rebase; cowshed fences on the head it was told, and a fence that fired for
#    that reason alone is retried against the newer head.
# 2. Stop <workspace>'s Nx daemon. Its file map can be stale across a rebase (fseventsd drops events), and a gate
#    that hashes a tree other than the rebased one proves nothing about it. The gate starts a fresh daemon.
# 3. Gate the rebased tree on the host, in <workspace>: `nx run-many -t lint test build --nx-bail=false` over every
#    project. Tasks main already ran at the tree they share replay from the cache the rebase carried over, so only
#    what <workspace>'s change reaches runs. The gate runs on the host, not in `cowshed land --check`'s sandbox: some
#    tasks hash differently there, and the sandbox cannot host the real-APFS tests.
# 4. `cowshed land` fast-forwards main to exactly the commit step 3 gated, onto exactly the head step 1 rebased
#    onto. If main moved meanwhile the land is refused, and the script rebases and gates again; Nx replays what the
#    new commits did not touch.
#
# A failed gate is re-run once, and only when every failed task failed on a wall-clock bound alone: the Nx output
# names the failed tasks, and each one's bounded-exec record (@smoothbricks/nx-plugin README, "Bounded Test Targets")
# says the command outlived its ceiling, or every failing test failed on its runner's per-test timeout. An assertion,
# a build or lint failure, a wedged task, or a task with no record stops the land at once, and so does a timed-out
# test whose own output shows a panic or failed assertion: a test thread can panic and then hang until the bound, and
# nextest reports that test as a timeout. The records are this checkout's own and are cleared before each run, so a run
# that dies before writing one is never judged by the run before it. Nx's own `run.json` (what
# `smoo-nx-bound-failures` reads) is not used: this repository's patched Nx writes it to a per-user directory that every
# checkout shares, so another gate's run overwrites it. A re-run that fails only on bounds again lands only if the
# failures are proven to hide nothing (tooling/land-judge.ts):
#
# - Every task that failed is re-run alone with every test running: nextest stops a run at its first failed test, and
#   a per-test timeout is one, so `--no-fail-fast` runs the tests behind it. A task that passes then has nothing left
#   to tolerate. One that fails again must fail on per-test timeouts alone, and its report must account for every test
#   the runner started; a task that outlives its total ceiling even alone ran an unknown number of tests and stops the
#   land.
# - Nx does not run the tasks behind a failed task, and its output names them. Those tasks are run one target at a
#   time without their dependencies, in dependency order, under the same bound-only rule, so a tolerated failure never
#   skips a check.
#
# Each tolerated failure is appended to agent-todo/land-ledger.md and to the message of the commit that records it:
# one commit on top of the gated one, changing only the ledger, and that commit is what main fast-forwards to.
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

gate_targets="lint test build"
ledger=agent-todo/land-ledger.md
# The ledger lines of the failures this land tolerated, and the last run's verdict lines.
tolerated=""
verdicts=""

scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT

say() {
  echo "land-host: $*" >&2
}

# Rebases <workspace> onto main's head and sets $base to that head and $head to the rebased commit.
rebase_onto_main() {
  while :; do
    base=$(git -C "$main" rev-parse HEAD)
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

# The judge of tooling/land-judge.ts, main's own copy so a change cannot loosen the gate that lands it, in
# <workspace>'s devenv and Nx workspace.
judge_tool() {
  (cd "$checkout" && DIRENV_LOG_FORMAT= direnv exec "$checkout" bun "$here/land-judge.ts" "$@" </dev/null)
}

# workspace_nx, its output kept in file $1 and shown on stderr; $2 names the one task whose bounded-exec record the run
# replaces, or is empty for every task's (the records are the checkout's own, and a run that dies before it writes
# one must not be judged by the run before it). Succeeds when Nx did.
nx_captured() {
  captured=$1
  forgotten=$2
  shift 2
  # $forgotten is a task id or nothing.
  # shellcheck disable=SC2086
  judge_tool forget $forgotten || return 1
  : >"$scratch/status"
  { workspace_nx "$@" 2>&1 && echo 0 >"$scratch/status" || echo $? >"$scratch/status"; } | tee "$captured" >&2
  [ "$(cat "$scratch/status")" = 0 ]
}

# Prints the verdicts of the failed run whose Nx output is file $1 to stderr and keeps them in $verdicts; succeeds when
# the run failed and every failed task failed on a wall-clock bound alone. The facts are the run's own output, which
# names the failed tasks, and bounded-exec's record of each: a task with none, an assertion, a wedge, or a timeout
# behind a panic or failed assertion (nextest reports a test that panicked and then hung as a timeout) is `failed`.
bound_only() {
  verdicts=$(judge_tool failures "$1") || {
    printf '%s\n' "$verdicts" >&2
    return 1
  }
  printf '%s\n' "$verdicts" >&2
}

# gate <nx args...>: one gate invocation in <workspace>, re-run once when it failed on bounds alone. Returns 0 when
# it passed, 2 when it failed on bounds alone twice (the verdicts are in $verdicts, and nothing is tolerated yet),
# and 1 when it failed on anything else, which stops the land.
gate() {
  nx_captured "$scratch/gate.out" "" "$@" --outputStyle=static-failures-only && return 0
  bound_only "$scratch/gate.out" || {
    say "'nx $*' failed on more than a wall-clock bound; the land stops"
    return 1
  }
  say "'nx $*' failed only on wall-clock bounds; re-running it once (Nx replays every task that passed)"
  nx_captured "$scratch/gate.out" "" "$@" --outputStyle=static-failures-only && return 0
  bound_only "$scratch/gate.out" || {
    say "the re-run of 'nx $*' failed on more than a wall-clock bound; the land stops"
    return 1
  }
  say "the re-run of 'nx $*' failed only on wall-clock bounds again; the failed tasks must account for every test"
  return 2
}

# Re-runs every task in $verdicts alone with every test running, and tolerates it only as tooling/land-judge.ts
# proves: a failure on per-test timeouts alone, with a report holding every test the runner started. Each tolerated
# task adds a line to $tolerated. A task that passes this time has nothing left to tolerate.
cover_failed_tasks() {
  failed=$(printf '%s\n' "$verdicts" | sed -n 's/^bound  \([^ ]*\): .*$/\1/p')
  [ -n "$failed" ] || {
    say "no failed task to account for; the land stops"
    return 1
  }
  load=$(uptime | sed 's/.*load averages*: //')
  for task in $failed; do
    full=$(judge_tool rerun-args "$task") || {
      say "$task cannot be proven complete; the land stops"
      return 1
    }
    say "re-running $task alone with every test running: nx run $task $full"
    # $full is a list of words by construction.
    # shellcheck disable=SC2086
    if nx_captured "$scratch/cover.out" "$task" run "$task" $full --outputStyle=static-failures-only; then
      say "$task passed with every test running; nothing of it is tolerated"
      continue
    fi
    bound_only "$scratch/cover.out" || {
      say "$task failed on more than a wall-clock bound when every test ran; the land stops"
      return 1
    }
    [ "$(printf '%s\n' "$verdicts" | sed -n 's/^bound  \([^ ]*\): .*$/\1/p')" = "$task" ] || {
      say "the full re-run of $task failed on tasks other than $task; the land stops"
      return 1
    }
    line=$(judge_tool coverage "$task" "$scratch/cover.out") || {
      say "$task's bound hides tests nobody saw; the land stops"
      return 1
    }
    say "$line"
    tolerated="$tolerated$(printf '%s\n' "$line" | sed -n \
      "s/^covered \([^ ]*\): \(.*\)$/- $workspace · \1 · \2 · load $load · landed on the bound-only rule after the re-run and a full re-run (land-host)/p")
"
  done
}

# gate <nx args...>, then what a double bound failure must prove (cover_failed_tasks). The tasks behind a failed task
# are not this one's business: land_gate is for runs that cannot have any.
land_gate() {
  gate_status=0
  gate "$@" || gate_status=$?
  case $gate_status in
  0) return 0 ;;
  2) cover_failed_tasks ;;
  *) return 1 ;;
  esac
}

# Nx does not run the tasks behind a failed task, and says which they were in the output of the gate run. plan_skipped
# <run-many args...> orders them with the task graph of the same arguments and writes them to $scratch/skipped as
# `group TARGET PROJECTS` lines, from the last gate run's output.
plan_skipped() {
  say "reading the task graph for the tasks behind a failed task, which Nx did not run"
  workspace_nx "$@" --graph="$scratch/graph.json" >&2 || {
    say "could not read the task graph; the land stops"
    return 1
  }
  judge_tool skipped "$scratch/gate.out" --graph "$scratch/graph.json" >"$scratch/skipped" || {
    say "could not plan the tasks behind the failed ones; the land stops"
    return 1
  }
}

# Runs the groups of $scratch/skipped without their dependencies, in the order the plan gives them.
run_skipped() {
  while IFS=$(printf '\t') read -r kind target projects; do
    [ "$kind" = group ] || continue
    say "running $target for $projects, which a failed task kept Nx from running"
    land_gate run-many -t "$target" -p "$projects" --excludeTaskDependencies --nx-bail=false || return 1
  done <"$scratch/skipped"
}

# Appends $tolerated to the ledger and commits it on top of the gated commit, with the same lines in the message.
record_tolerated() {
  [ -n "$tolerated" ] || return 0
  mkdir -p "$(dirname "$checkout/$ledger")"
  printf '%s' "$tolerated" >>"$checkout/$ledger"
  git -C "$checkout" add -- "$ledger"
  git -C "$checkout" commit --quiet -m "docs(tooling): ledger $workspace's bound-only land

$workspace landed with these tasks failing on a wall-clock bound alone:

$tolerated" -- "$ledger"
  head=$(git -C "$checkout" rev-parse HEAD)
  say "$ledger records the tolerated tasks in $head"
  tolerated=""
}

# Gates the rebased tree and records what it tolerated. Sets nothing; stops the script when the land must stop.
gate_workspace() {
  say "gating $head (rebased onto $base) on the host"
  workspace_nx daemon --stop >&2 || {
    say "could not stop $workspace's Nx daemon; a stale file map could gate a tree other than $head"
    exit 1
  }
  gate_status=0
  gate run-many -t $gate_targets --nx-bail=false || gate_status=$?
  case $gate_status in
  0) ;;
  2)
    plan_skipped run-many -t $gate_targets || exit 1
    cover_failed_tasks || exit 1
    run_skipped || exit 1
    ;;
  *) exit 1 ;;
  esac
  record_tolerated
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
