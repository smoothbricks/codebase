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
cowshed new agent-auth
```

That command selects the repository bound to `~/src/api`, then clones **that repository's** warm `main` image.

For automation, be explicit so a changed process cwd cannot select the wrong repository:

```sh
cowshed new agent-auth --project ~/src/api --json
cowshed new agent-homepage --project ~/src/web --json
```

`--project` takes a Git path, not a `repo_id`. `--repo-id owner/repo` is an adoption-time identity choice for a
repository whose remotes cannot supply a usable identity.

## What `new` clones

Within the selected repository:

```sh
cowshed new <name>
```

clones `main` by default. Use `--from` to use another workspace in the same repository as the copy-on-write source:

```sh
cowshed new experiment --from agent-auth
```

Use `--ref` when the new workspace's Git branch should start at a particular revision:

```sh
cowshed new reproduce-4821 --ref 8b31e9f
```

`--from` never crosses repository boundaries. To select another repository, change cwd or pass `--project`.

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

If the repository uses direnv, its `.envrc` can ask cowshed to heal mounts and emit the small environment contract:

```sh
eval "$(cowshed ensure --envrc)"
```

`ensure` is safe to run repeatedly. It does not install dependencies or fetch code.

## Daily human workflow

Create a workspace and capture its mount path:

```sh
WS=fix-auth
WS_PATH=$(cowshed new "$WS")
cd "$WS_PATH"
```

The workspace is a standalone Git checkout. Normal editor and Git operations work there.

For a command that should run under cowshed's sandbox:

```sh
cowshed exec "$WS" -- bun test
cowshed exec "$WS" -- git status --porcelain
```

Before a risky operation, create a checkpoint:

```sh
cowshed checkpoint "$WS" before-refactor
```

Restore it if needed:

```sh
cowshed restore "$WS" before-refactor
```

Preserve the branch, then remove the workspace:

```sh
cowshed push "$WS"
cowshed rm "$WS"
```

`rm` refuses when commits have not been preserved. Use `--force` only when discarding them is intentional.

## Recommended coding-agent workflow

Use one fresh workspace per task. Give it a deterministic, repository-local name rather than pooling old workspaces.

```sh
PROJECT=~/src/api
WS=agent-4821

MOUNT=$(cowshed new "$WS" --project "$PROJECT")
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

When the task is complete:

```sh
cowshed push "$WS" --project "$PROJECT"
cowshed rm "$WS" --project "$PROJECT"
```

If `rm` reports a conflict, do not automatically add `--force`: the conflict is the data-loss fence telling the agent to
preserve or explicitly discard its work.

## Machine-readable operation

Every command keeps control output separate from guidance:

- stdout: one bare answer, TSV rows, raw child output, or one `--json` envelope;
- stderr: `cowshed:` explanations and `next:` follow-up commands;
- exit status: the stable cowshed error code, or the foreground child's exit code for `exec`.

Prefer `--json` in orchestration:

```sh
cowshed new agent-4821 --project ~/src/api --json
```

```json
{
  "ok": true,
  "result": {
    "workspace": "agent-4821",
    "mount": "/Users/me/.cowshed/mnt/acme/api/agent-4821",
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
    "message": "workspace agent-4821 has unpublished changes",
    "hint": "push or land the workspace, or retry with: cowshed rm agent-4821 --force"
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
|    5 | environment missing | follow the emitted setup/healing hint                   |
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

| Goal                             | Command                                                                              |
| -------------------------------- | ------------------------------------------------------------------------------------ |
| Adopt a repository               | `cowshed adopt [path] [--repo-id owner/repo]`                                        |
| Create from selected repo's main | `cowshed new <name>`                                                                 |
| Create from another workspace    | `cowshed new <name> --from <workspace>`                                              |
| Fork current workspace state     | `cowshed fork <source> <destination>`                                                |
| List selected repository         | `cowshed ls`                                                                         |
| Resolve/attach a mount           | `cowshed path <workspace>`                                                           |
| Run a sandboxed command          | `cowshed exec <workspace> -- <argv...>`                                              |
| Checkpoint / restore             | `cowshed checkpoint <workspace> [label]`, `cowshed restore <workspace> <label>`      |
| Detach / attach                  | `cowshed detach <workspace>`, `cowshed attach <workspace>`                           |
| Preserve Git work                | `cowshed push <workspace>`, `cowshed rebase <workspace>`, `cowshed land <workspace>` |
| Remove a workspace               | `cowshed rm <workspace>`                                                             |
| Preview / run reclamation        | `cowshed gc --dry-run`, `cowshed gc`                                                 |
| Diagnose host state              | `cowshed doctor`                                                                     |
| Manage the gateway               | `cowshed gateway start`, `status`, `stop`                                            |

`--project <git-root>`, `--json`, and `--quiet` are global options accepted by repository-scoped commands. Gateway
service commands are host-global and reject `--project`.

## Related guides

- [CLI reference](cli.md) — complete command and output contracts
- [Coding agents](agents.md) — capability, job, artifact, and sandbox details
- [Gateway](gateway.md) — service lifecycle, mirrors, authentication, and egress
- [Troubleshooting](troubleshooting.md) — mount, sandbox, storage, and recovery diagnosis
