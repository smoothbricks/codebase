# Basic cowshed usage

cowshed replaces linked Git worktrees with warm, standalone, copy-on-write workspaces. The important mental model is:

```text
host
├── acme/api
│   ├── main                 one warm main image for this repository
│   ├── agent-auth           independent workspace
│   └── agent-cache          independent workspace
└── acme/web
    ├── main                 a different warm main image
    └── agent-homepage       independent workspace
```

A host may adopt any number of repositories. Each repository identity (`repo_id`) owns exactly one `main`, plus its own
workspace names, checkpoints, gateway sessions, and lifecycle state. `main` is repository-scoped, never host-global.

## How cowshed chooses a repository

Most commands do not need a `repo_id` argument. cowshed selects the repository from either:

1. `--project <git-root>`, when supplied; or
2. the current directory, by walking up to the enclosing standalone Git root and validating its cowshed binding.

The current directory may be the adopted main checkout, a nested directory inside it, or any cowshed workspace belonging
to that repository.

```sh
cd ~/src/api/packages/server/src
cowshed fork agent-auth
```

That command selects the repository bound to `~/src/api`, then clones **that repository's** warm `main` image.

For automation, be explicit so a changed process cwd cannot select the wrong repository:

```sh
cowshed fork agent-auth --project ~/src/api --json
cowshed fork agent-homepage --project ~/src/web --json
```

`--project` takes a Git path, not a `repo_id`. `--repo-id owner/repo` is an adoption-time identity choice for a
repository whose remotes cannot supply a usable identity.

## What `fork` clones

Within the selected repository:

```sh
cowshed fork <ws>
```

clones main by default, carrying tracked edits and untracked files warm. Name a source to fork another session:

```sh
cowshed fork agent-auth experiment
```

`--clean` discards tracked edits and nonignored untracked files. `--ref` implies clean and selects a revision in the
source:

```sh
cowshed fork reproduce-4821 --ref 8b31e9f
```

A fork never crosses repository boundaries. To select another repository, change cwd or pass `--project`.

Use `fork` when you want a named divergent future from an existing workspace:

```sh
cowshed fork agent-auth agent-auth-alt
```

## First-time setup for a repository

Commands below assume `cowshed` is on `PATH`.

Adopt an existing checkout once:

```sh
cd ~/src/api
cowshed adopt
```

A local-only repository with no usable remote needs an explicit stable identity:

```sh
cowshed adopt ~/src/local-tool --repo-id personal/local-tool
```

Adoption keeps the checkout at the same path but turns it into that repository's image-backed `main`. On macOS, the
first adoption on a host may request one administrator authorization while cowshed provisions its APFS storage.
Provisioning is batched under one authorization session; ordinary commands and background services never provision
storage or prompt.

Start the managed gateway once:

```sh
cowshed gateway start
cowshed gateway status --json
```

The gateway owns credentials, registry mirrors, and egress policy. Workspaces receive an isolated endpoint and opaque
workspace token; secrets are not copied into the workspace.

Every published workspace image carries its environment in `.cowshed/env`. Shell hooks are optional: cowshed neither
requires nor writes `.envrc` or `.envrc-local`, and a plain Git repository needs no shell-hook ignore rules. A project
that uses direnv interactively may add this line to its own hook:

```sh
source_env_if_exists .cowshed/env
```

This is direnv's standard optional-file loader. It is not needed for `cowshed exec`: the supervisor supplies workspace
values directly, whether or not the repository uses direnv. Existing repository hooks are cloned unchanged.

Until its supervisor first serves a workspace, its file sets neither `TMPDIR` nor `COWSHED_RUNTIME_LINK`, and the
checkout's build link may still name the volume of the checkout it was forked from. A fork interrupted before that start
is left so. A hook that needs either value never falls back to the caller's: the managed shell and hooks run
`cowshed exec <ws> -- true` once to serve such a checkout, then continue, and refuse by name only if that fails.

The sourced file exports workspace-local `COWSHED_*` values and is rewritten whenever cowshed rotates the workspace
token. After a reboot or manual eject, reattach explicitly:

```sh
cowshed attach
```

`attach` is safe to run repeatedly. It does not install dependencies or fetch code, and cowshed never writes direnv
trust records.

A project's shell is its own `.envrc`. A devenv project writes `use devenv` there, and `cowshed exec` activates it
through direnv inside the sandbox; cowshed has no devenv configuration or `devenv shell` path of its own.

## Daily human workflow

Create a workspace and capture its mount path:

```sh
WS=fix-auth
WS_PATH=$(cowshed fork "$WS")
cd "$WS_PATH"
```

The workspace is a standalone Git checkout. Normal editor and Git operations work there.

For a command that should run under cowshed's sandbox:

```sh
cowshed exec "$WS" -- bun test
cowshed exec "$WS" -- git status --porcelain
```

Under a workspace `.envrc`, the workspace supervisor keeps an activated shell and activates a fresh one only when an
input direnv recorded for it changes (11_shell.md). Processes already running keep the environment they launched with.

Before a risky operation, create a checkpoint:

```sh
cowshed checkpoint "$WS" before-refactor
```

Restore it if needed:

```sh
cowshed restore "$WS" before-refactor
```

Land the work, then remove the workspace:

```sh
cowshed land "$WS"
```

`land` retires the workspace once its commits are in `main`, so there is usually no separate `rm`. Removing a workspace
destroys the image its commits live in, so `rm` refuses until `main` contains the workspace's `HEAD`.

`--force` overrides _transient_ state only — a dirty tree, an in-progress merge, the workspace's own running jobs. It
never waives a process cowshed does not run holding the mount, and it does not authorize destroying commits. `--abandon`
does, and only that: it bundles the unlanded commits into `sessions/.trash` first and prints what it destroyed.

## Recommended coding-agent workflow

Use one fresh workspace per task. Give it a deterministic, repository-local name rather than pooling old workspaces.

```sh
PROJECT=~/src/api
WS=agent-4821

MOUNT=$(cowshed fork "$WS" --project "$PROJECT")
cowshed exec "$WS" --project "$PROJECT" -- bun install
cowshed exec "$WS" --project "$PROJECT" -- bun test
cowshed exec "$WS" --project "$PROJECT" -- git status --porcelain
```

Why agents should prefer `cowshed exec` over merely `cd`-ing into the mount:

- on macOS, the command runs with the workspace's Seatbelt authority;
- inherited host secrets and unsafe environment values are removed;
- cache, temp, gateway, and tool paths are selected by the controller;
- stdout and stderr retain the child process's byte and exit-code behavior;
- lifecycle identity is revalidated before execution.

A useful retry pattern:

```sh
cowshed checkpoint "$WS" before-migration --project "$PROJECT"
if ! cowshed exec "$WS" --project "$PROJECT" -- ./scripts/migrate; then
  cowshed restore "$WS" before-migration --project "$PROJECT"
fi
```

For parallel exploration, fork the current state instead of rebuilding it:

```sh
cowshed fork "$WS" "$WS-alt" --project "$PROJECT"
```

When the task is complete, land it:

```sh
cowshed land "$WS" --project "$PROJECT"
```

If `rm` reports a conflict, **do not reach for a flag**. The conflict is the data-loss fence: the workspace holds
commits that exist nowhere else, and the remedy it names — `cowshed land <ws>` — is the one that keeps them. `--force`
will not get past it, by design; `--abandon` will, and destroys them (recoverably, via the bundle it writes) on purpose.

### Lanes: one change built by several workspaces

When a change is too big for one workspace, give it a lane base and fork its units from that base. Each unit folds into
the lane base instead of main by Git alone, and the lane base lands into main once, gated, when the whole change is
done:

```sh
LANE=auth-rework

cowshed fork "$LANE" --project "$PROJECT"
cowshed fork "$LANE" "$LANE-schema" --project "$PROJECT"
cowshed fork "$LANE" "$LANE-api" --project "$PROJECT"

# each unit, when its work is done: Git alone, no check, no build-volume carry
cowshed rebase "$LANE-schema" --into "$LANE" --ungated --project "$PROJECT"
cowshed land "$LANE-schema" --into "$LANE" --ungated --project "$PROJECT"

# the lane, when every unit has landed into it:
cowshed rebase "$LANE" --project "$PROJECT"
cowshed land "$LANE" --check 'just verify' --project "$PROJECT"
```

`rebase --into` catches a unit up with what its lane-mates have already landed in the lane base, and `land --into` moves
the lane base's checked-out branch and retires the unit once the lane base holds its commits. Cowshed keeps no record of
the lane: `--into` names a workspace, and your script is what knows which units belong to it. If the lane base is
removed and recreated under the same name while units are in flight, their lands refuse it rather than land into a
workspace they were not forked from.

`--ungated` makes a unit's fold Git alone: the rebase computes no task hash and carries nothing, and the land runs no
check, initializes or carries no Nx cache, freezes no seed and adopts no build volume. The lane base keeps its build
volume and seed, and the unit's volume and seed are released when it retires. The lane is built once, by the lane base's
own gated land into main, whose `--check` is the batch gate for every unit. Main refuses `--ungated` (exit 2, naming
main): only a land into main gates. `--ungated` is also refused with `--check` or `--push-only` on land and with
`--gate` on rebase (exit 2). With `--no-retire`, an ungated land keeps the unit and collects nothing, since it released
no volume. Leave `--ungated` off when each unit must gate on its own, as in a stack whose units each land reviewed work:
a gated land into the lane base adopts the unit's build volume and refreshes the lane base's seed.

## Machine-readable operation

Every command keeps control output separate from guidance:

- stdout: one bare answer, TSV rows, raw child output, or one `--json` envelope;
- stderr: `cowshed:` explanations and `next:` follow-up commands;
- exit status: the stable cowshed error code, or the foreground child's exit code for `exec`.

Prefer `--json` in orchestration:

```sh
cowshed fork agent-4821 --project ~/src/api --json
```

```json
{
  "ok": true,
  "result": {
    "workspace": "agent-4821",
    "mount": "<mount-root>/acme/api/agent-4821",
    "baseCommit": "6f3a2c1000000000000000000000000000000000"
  }
}
```

Do not merge stderr into stdout before parsing the envelope. Read stderr separately: `next:` lines are deliberate
self-driving affordances, not noise.

Errors use the same frozen shape:

```json
{
  "ok": false,
  "error": {
    "code": "conflict",
    "message": "workspace agent-4821 head 9b2e77d… is not contained by main (main is at 6f3a2c1…)",
    "hint": "land the workspace: cowshed land agent-4821"
  }
}
```

Important exit codes:

| Code | Meaning             | Agent behavior                                          |
| ---: | ------------------- | ------------------------------------------------------- |
|    0 | success             | continue                                                |
|    1 | internal error      | stop and report the bug                                 |
|    2 | usage               | fix argv; do not retry unchanged                        |
|    3 | not found           | refresh repository/workspace selection                  |
|    4 | conflict            | preserve work or resolve the named state conflict       |
|    5 | environment missing | follow the emitted setup or repair hint                 |
|    6 | sandbox denied      | request the named authority; do not silently self-widen |
|    7 | integrity failure   | stop; do not select whichever copy looks newest         |

## Repository and workspace inspection

List only the workspaces in the repository selected by cwd:

```sh
cd ~/src/api
cowshed ls
```

Or select explicitly:

```sh
cowshed ls --project ~/src/web --json
```

Get a live mount path; a detached workspace is attached first:

```sh
cowshed path agent-auth --project ~/src/api
```

Inspect without attaching:

```sh
cowshed path agent-auth --project ~/src/api --no-attach
```

Suspend and resume without deleting the image:

```sh
cowshed detach agent-auth --project ~/src/api
cowshed attach agent-auth --project ~/src/api
```

## Main rollback

`main` is permanent during ordinary workspace operations. To undo adoption and restore the exact retained standalone
checkout, use the explicit reversible path:

```sh
cowshed rm main --restore --project ~/src/api
```

The command refuses dirty or conflicting state unless the documented force path is deliberately chosen. Successful
rollback removes only that repository's binding and controller storage; other adopted repositories and their `main`
images are unaffected.

## Command map

| Goal                                            | Command                                                                              |
| ----------------------------------------------- | ------------------------------------------------------------------------------------ |
| Adopt a repository                              | `cowshed adopt [path] [--repo-id owner/repo]`                                        |
| Create from selected repo's main                | `cowshed fork <ws>`                                                                  |
| Create from another workspace                   | `cowshed fork <src> <ws>`                                                            |
| Create clean at a revision                      | `cowshed fork [<src>] <ws> --ref <rev>`                                              |
| List selected repository                        | `cowshed ls`                                                                         |
| Resolve/attach a mount                          | `cowshed path <workspace>`                                                           |
| Run a sandboxed command                         | `cowshed exec <workspace> -- <argv...>`                                              |
| Checkpoint / restore                            | `cowshed checkpoint <workspace> [label]`, `cowshed restore <workspace> <label>`      |
| Detach / attach                                 | `cowshed detach <workspace>`, `cowshed attach <workspace>`                           |
| Preserve Git work                               | `cowshed push <workspace>`, `cowshed rebase <workspace>`, `cowshed land <workspace>` |
| Remove a workspace                              | `cowshed rm <workspace>`                                                             |
| Preview / run reclamation                       | `cowshed gc --dry-run`, `cowshed gc`                                                 |
| Replace rebuildable build state                 | `cowshed reseed <ws> [--from <target>] [--dry-run]` (default target: main)           |
| Clear a workspace's and the project's Nx caches | `cowshed cache clear [<ws>]` (not `nx reset`, which only removes the links)          |
| Make `fork` fast on an old main                 | `cowshed defrag main` (idle checkout; `doctor` warns `main-extents` when it is due)  |
| Diagnose host state                             | `cowshed doctor`                                                                     |
| Manage the gateway                              | `cowshed gateway start`, `status`, `stop`                                            |

`--project <git-root>`, `--json`, and `--quiet` are global options accepted by repository-scoped commands. Gateway
service commands are host-global and reject `--project`.

`reseed` discards live build-volume contents for a new clone of the target's current-incarnation latest existing seed.
It leaves source files, HEAD, index, dirty and untracked work untouched; missing incremental work rebuilds cold. It
never refreshes or deletes a seed: stale seeds are used as-is and an absent seed refuses. Run it while idle; jobs, land
holds, filesystem holders and unfinished lifecycle work refuse. Preview changes no build state and reports
allocated-byte estimates; execution reports removed-image allocation, not unique physical savings on APFS shared
extents. See [the CLI reference](cli.md) for JSON fields and full refusal/accounting rules.

## Related guides

- [CLI reference](cli.md) — complete command and output contracts
- [Coding agents](agents.md) — capability, job, artifact, and sandbox details
- [Gateway](gateway.md) — service lifecycle, mirrors, authentication, and egress
- [Troubleshooting](troubleshooting.md) — mount, sandbox, storage, and recovery diagnosis
