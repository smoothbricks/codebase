# Controller Git never runs unsandboxed

`specs/cowshed/04_sandbox.md:341-352` states the design: controller Git never runs with its cwd or config inside a
workspace unsandboxed, on any platform. It forces off `core.sshCommand`, `credential.helper` and `core.askPass` beside
hooks, fsmonitor and signing. Rebase, land and fetch run under the controller Git sandbox (GitDiscovery on macOS, the
Landlock controller Git sandbox on Linux), and a platform without that sandbox refuses the verbs with a typed error.

## What the code does

- `packages/cowshed/crates/cowshed-core/src/git.rs:2304` `git_command_at` passes `core.hooksPath=/dev/null`,
  `core.fsmonitor=false`, `commit.gpgsign=false`, `tag.gpgsign=false`. It does not force off `core.sshCommand`,
  `credential.helper` or `core.askPass`, so a workspace-writable `.git/config` can name a program the controller runs.
- `crates/cowshed-core/src/workspace_git_fetch.rs:228` and `crates/cowshed-gateway/src/repo_mirror.rs:594` build their
  own `-c` lists with the same gap.
- The GitDiscovery profile exists on macOS (`git.rs:2522`, `sandbox.rs:51`). Linux has no controller Git sandbox, and
  the verbs run unsandboxed there instead of refusing.

## Work

1. One hardened `-c` set, used by every controller Git spawn (core, fetch, gateway mirror), that adds
   `core.sshCommand=`, `credential.helper=` and `core.askPass=` to the existing keys.
2. On a platform without the controller Git sandbox, return a typed refusal from rebase, land and fetch before any Git
   process starts.
3. The Linux controller Git sandbox (Landlock, same write boundary as GitDiscovery) is the Linux substrate's work and
   replaces the refusal when it lands.

## Acceptance

- Extend `crates/cowshed-core/tests/workspace_git_escape.rs`: a workspace `.git/config` naming `core.sshCommand`,
  `credential.helper` or `core.askPass` as a marker-writing script runs nothing during fetch, land or rebase.
- On a build without the controller Git sandbox, the three verbs return the typed refusal and spawn no Git process.
