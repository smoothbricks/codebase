# cowshed in GitHub Actions

A cowshed workspace is what a CI job wants: a warm, isolated checkout with `.git`, `node_modules`, a hot `target/`, warm
Nix/devenv state, and every package cache already populated. The cowshed runner that would give every job one — a
self-hosted Linux host with a ZFS substrate, a `services.cowshed-runner` NixOS module, and job hooks that create and
destroy each job's workspace and dispatch every command through `cowshed exec` — does not exist yet. Its design is
`specs/cowshed/10_ci.md` (and `09_substrates.md` for ZFS). What ships today is the composite action below.

## The one factual constraint

GitHub-hosted runners on free/open-source plans **cannot boot a custom or NixOS image.** The custom-image feature only
ever existed on the (now discontinued) larger-runner preview, never on standard or OSS runners. So "our own image with
Nix and a cowshed store already available" necessarily means **self-hosted**.

## The composite action

`./.github/actions/smoothbricks-ci` prepares a job either way, chosen by `mode`:

```yaml
- name: 🧱 Setup environment
  id: cowshed
  uses: ./.github/actions/smoothbricks-ci
  with:
    mode: auto # auto | github | cowshed
```

- **`github`** runs the existing `setup-devenv` path: Nix install, caches, devenv build.
- **`cowshed`** requires `cowshed` on the runner's `PATH` and an adopted checkout. It creates a workspace named
  `ci-<run_id>` at the job's commit with `cowshed new`, publishes its mount as `steps.cowshed.outputs.workspace-path`,
  and exports `COWSHED_CI_WORKSPACE` and `COWSHED_CI_WORKSPACE_PATH`. A composite action cannot register a post-job
  hook, so reclaim the workspace with a final `if: always()` step running `cowshed rm "$COWSHED_CI_WORKSPACE"`.
- **`auto`** picks `cowshed` when the binary is on `PATH`, else `github`.

The action only provisions an environment and publishes a cwd. It does not wrap later steps: a step runs inside the
workspace's sandbox only if it invokes `cowshed exec` itself, so the action is never a security boundary, and a
self-hosted runner on a public repository needs the usual guardrails (approval for fork-PR workflows, ephemeral runners,
a dedicated runner group, never `pull_request_target` on untrusted code).

This repository's own CI does not use `cowshed` mode: its `Validate` job runs on Linux runners that have no cowshed
store (`specs/cowshed/08_testing.md`, "CI").
