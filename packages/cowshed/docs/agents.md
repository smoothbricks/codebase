# Driving cowshed from coding agents

cowshed's CLI is designed to be **self-driving**: an agent that can run shell commands can operate cowshed correctly
without reading this document, because every command narrates its effects and next steps on stderr. This document makes
the implicit contract explicit.

## The two-stream rule

- **Parse stdout.** It contains exactly one thing: the answer (a path, a name, TSV rows, or a `--json` envelope). Piping
  cowshed into `grep`, `jq`, `xargs`, or a variable never captures prose.
- **Read stderr.** All guidance lives there, in greppable, prefixed lines:
  - `cowshed: ...` — what just happened, or why something failed
  - `next: <command>` — concrete follow-up commands, valid to run as-is

An agent's loop is: capture stdout for data, scan stderr for `next:` lines when deciding what to do, and switch on the
exit code when something fails. Never parse prose out of stdout — there is none — and never expect data on stderr.

```sh
WS_PATH=$(cowshed new task-4821)          # stdout only: the mountpoint
# stderr (seen by the agent, not captured): cowshed: ... / next: cowshed exec task-4821 -- ...
```

Prefer `--json` when you want structure:

```sh
cowshed new task-4821 --json | jq -r .result.path
```

## Warm worktrees for Claude Code

Pattern: one cowshed workspace per agent task, instead of `git worktree add`. You get full isolation (the agent can
trash anything inside; the host is safe), warm caches (builds and installs are incremental from main's state), and O(1)
cleanup.

```sh
# spawn
WS=task-$RANDOM
WS_PATH=$(cowshed new "$WS")

# work — either cd into it (inherits your session's permissions)...
cd "$WS_PATH" && bun test

# ...or run sandboxed (recommended for autonomous work):
cowshed exec "$WS" -- bun install
cowshed exec "$WS" -- bun test
cowshed exec "$WS" -- cargo clippy --workspace

# ship & destroy
cowshed exec "$WS" -- git commit -am "implement feature"
cowshed push "$WS"
cowshed rm "$WS"
```

Notes for harness integration:

- `cowshed new` is fast (~250 ms) and safe to call per-task. Names must be unique per project; exit 4 means the name is
  taken — pick another, don't retry the same one.
- The workspace branch is `cowshed/<name>`; `cowshed push` delivers it to the main repo's refs, where a human (or a
  merge queue) takes over. Never push to main's checked-out branch.
- `cowshed rm` refuses (exit 4) if the branch was never pushed — that's your signal work would be lost. Push first or
  `--force` deliberately.
- If a task needs a _snapshot to retry from_, `cowshed checkpoint <ws> <label>` before the risky step and
  `cowshed restore <ws> <label>` on failure — both are milliseconds, and far cheaper than re-creating the workspace.
- Parallel exploration: `cowshed fork <ws> <ws>-alt` mid-task gives you two divergent futures from the same state. Forks
  start with a closed sandbox regardless of the source's grants.

## A complete task loop (script-grade)

Everything an autonomous harness needs, with exit-code handling:

```sh
#!/bin/sh
set -eu
TASK="$1"; WS="task-$TASK"

if ! WS_PATH=$(cowshed new "$WS" --json | jq -re .result.path); then
  case $? in
    4) echo "name collision: $WS already exists (stale task?)" >&2; exit 1 ;;
    5) cowshed doctor >&2; exit 1 ;;      # environment problem; doctor says what
    *) exit 1 ;;
  esac
fi

run() {
  cowshed exec "$WS" -- "$@"
  rc=$?
  if [ $rc -eq 6 ]; then
    # cowshed already printed the exact `next: cowshed grant ...` line on stderr.
    # Surface it upward; do NOT self-grant here.
    echo "NEEDS-GRANT: see stderr above" >&2
  fi
  return $rc
}

run bun install
run bun test
run git commit -am "task $TASK"

cowshed push "$WS" >/dev/null           # branch cowshed/task-$TASK now in main's repo
cowshed rm "$WS" >/dev/null
```

Points worth copying even if you don't copy the script: capture stdout only via `--json`+`jq`; branch on exit codes, not
output text; let exit 6 flow upward as a permission request; push before rm (rm exits 4 otherwise, which is your
unsaved-work safety net).

## Sandbox etiquette: start closed, earn grants

Every workspace starts closed: writes limited to its own volume, designated cache subtrees, and temp; network limited to
your own gateway listener (whose registry mirrors — npm, crates.io — are baseline policy, warm and credentialed, so
builds and installs work out of the box with zero grants). Third-party repositories arrive through
`cowshed repo clone <url>`, which the gateway mirrors on your behalf. Need a dev server? Bind it to `$PORT` — each
workspace owns a block of ports above its gateway base (`$COWSHED_PORT_BASE`), reachable from the host browser
container-style and guaranteed not to collide with sibling workspaces.

**Exit code 6 is not an error to retry — it is a request to negotiate.** When cowshed reports 6 it has authoritative
evidence of the denial (egress denials always — the gateway logged the decision; filesystem denials when the kernel
sandbox telemetry can be correlated) and prints the exact grant that would allow it:

```
$ cowshed exec task-4821 -- ./scripts/sync-fixtures.sh
cowshed: sandbox denied network-outbound fixtures.internal.example:443
cowshed: egress from 'task-4821' is limited to the gateway allowlist
next: cowshed grant task-4821 --egress fixtures.internal.example
```

The correct agent behavior on exit 6:

1. Do **not** silently self-grant if you hold the authority to; surface the `next:` line to the operator or the
   coordinating agent as a permission request.
2. Once granted, retry the _original_ command (filesystem grants apply from the next exec).
3. Treat grants as scoped to the task: they die with the workspace, and `cowshed revoke <ws> --all` returns to closed if
   you widened temporarily.

Not every denial is diagnosable: a deep child process blocked by the sandbox may surface only as its own nonzero exit,
passed through unchanged. If a command fails writing outside your volume or reaching the network and there was no exit
6, suspect the sandbox anyway — `cowshed doctor` shows recent gateway denials, and the troubleshooting guide has the
Seatbelt log incantation.

This is the same layered model jcode's swarm coordinator uses (see [jcode.md](jcode.md)): the supervisor holds grant
authority; workers run closed and ask.

## Concurrency and hygiene

- Workspaces are cheap; don't pool or reuse them across tasks. Fresh clone per task means reproducible starting state
  (whatever main was at that moment).
- Everything about a workspace is derived from disk — there is no daemon to desync from. If your harness crashes
  mid-task, `cowshed ls` still tells the truth, and `cowshed rm` cleans up completely.
- Run `cowshed ensure` at task start if your harness may outlive reboots; it is a no-op (~20 ms) when healthy and
  repairs mounts when not.
- `cowshed gc` is safe to run between tasks; it never touches live workspaces.

## What agents must not expect

- No writes to `$HOME`, no reading `~/.ssh`, `~/.aws`, keychains, or other projects — closed means closed, and
  `cowshed grant` is the only door.
- No direct internet. The gateway's mirrors make registry traffic invisible to you; arbitrary hosts need an egress
  grant. Credentials are injected upstream by the gateway — there are no tokens inside the workspace to find, and no
  `.env` files.
- No remote git. Your repo has exactly the local `host` remote (main) and whatever mirrors `cowshed repo clone` brought
  in; pushing to origin/GitHub is the coordinator's job, host-side.
- No mutating cowshed's own state files: markers are informational, grants live outside your volume.
