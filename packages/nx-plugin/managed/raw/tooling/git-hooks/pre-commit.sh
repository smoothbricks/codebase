#!/usr/bin/env bash
cd "$(git rev-parse --show-toplevel)"
TOOLING="$PWD/tooling"

# A bare assignment, not `export PATH=$(…)`: export's status would hide repo-path's refusal of an unserved workspace.
PATH="$("$TOOLING/direnv/repo-path")" || exit $?
# ttsc compiles with this checkout's native TypeScript compiler into this
# checkout's plugin cache, which the dev shell names. A hook runs outside that
# shell, in whatever environment ran `git`; ttsc-env.sh names them the same way.
. "$TOOLING/direnv/ttsc-env.sh"

set -e -o pipefail

# A commit in cowshed's main checkout moves main's ref, which only a holder of
# the repository's ref lock may move. The in-image marker names the checkout's
# role (cowshed 01_storage.md "In-image marker"); a shed, a plain clone and CI
# have no main role and commit as before. In main the commit must run under a
# process that holds the ref lock and says so with
# SMOO_REF_LOCK_HOLDER=<pid>:<lock file>, where <pid> is the process that has
# <lock file> open (for lockf(1), the lockf process: its command does not hold
# the file). The hook proves both halves itself, before anything rewrites the
# index: <pid> is its ancestor and has <lock file> open. An inherited, stale or
# hand-set marker therefore does not pass. A fast-forward, merge or rebase runs
# no pre-commit hook and is never judged here.
refuse_main_commit() {
  {
    echo "pre-commit: refused: $1"
    echo "pre-commit: this is the main checkout. Commit to main only under its ref lock, through the repository's doc lane:"
    echo "pre-commit:   ~/Dev/.land-lock.sh <repo> git [-C <main>] commit -m '…' -- <paths>"
    echo "pre-commit: if the doc lane fails, report that failure instead of committing here. Code lands from a shed."
  } >&2
  exit 1
}

if [[ -f .cowshed/workspace.json && $(<.cowshed/workspace.json) =~ \"role\"[[:space:]]*:[[:space:]]*\"main\" ]]; then
  holder=${SMOO_REF_LOCK_HOLDER:-}
  [[ -n $holder ]] || refuse_main_commit 'SMOO_REF_LOCK_HOLDER is not set: this commit is not made under the ref lock'
  holder_pid=${holder%%:*}
  lock=${holder#*:}
  if [[ $holder != *:* || ! $holder_pid =~ ^[1-9][0-9]*$ || $lock != /* ]]; then
    refuse_main_commit "SMOO_REF_LOCK_HOLDER='$holder' is not <pid>:<absolute lock file>"
  fi
  [[ -e $lock ]] || refuse_main_commit "SMOO_REF_LOCK_HOLDER names $lock, which does not exist"

  # One read of the process table; a sparse indexed array keeps this bash 3 safe.
  table=$(ps -A -o pid= -o ppid=) || refuse_main_commit 'ps could not read the process table'
  parent_of=()
  while read -r pid ppid; do parent_of[pid]=$ppid; done <<<"$table"
  ancestor=${parent_of[$$]:-0}
  while ((ancestor > 1)) && [[ $ancestor != "$holder_pid" ]]; do ancestor=${parent_of[ancestor]:-0}; done
  if [[ $ancestor != "$holder_pid" ]] || ((ancestor <= 1)); then
    refuse_main_commit "SMOO_REF_LOCK_HOLDER names process $holder_pid, which is not an ancestor of this commit: the marker is stale or was set outside the lock holder"
  fi

  open=
  if [[ -d /proc/$holder_pid/fd ]]; then
    for fd in /proc/"$holder_pid"/fd/*; do
      if [[ $fd -ef $lock ]]; then
        open=$holder_pid
        break
      fi
    done
  else
    # lsof exits 1 when the process does not have the file open; its errors still reach stderr.
    open=$(lsof -t -a -p "$holder_pid" -- "$lock") || :
  fi
  [[ $open == "$holder_pid" ]] || refuse_main_commit "process $holder_pid does not hold $lock open, so it does not hold the ref lock"
fi

# Format the index git handed this hook, which is what the commit will contain:
# the staged content for a plain `git commit`, and a snapshot of the working
# tree copies of the named paths for `git commit --only -- <paths>`. It rewrites
# that index and the working tree file, and post-commit then drops the
# pre-format entry a partial commit strands in the real index.
git-format-staged

# Judge the formatted added lines from that same index, not unstaged edits.
smoo monorepo check-public-denylist --staged

# Adding a workspace package is rare and easy to leave half-wired. Run the
# full monorepo validator only for newly staged package manifests.
smoo monorepo validate --fail-fast --only-if-new-workspace-package
