# cowshed as a GitHub Actions runner

A cowshed workspace is exactly what a CI job wants: a warm, isolated checkout with `.git`, `node_modules`, a hot
`target/`, warm Nix/devenv state, and every package cache already populated — created in milliseconds and destroyed at
the end of the job. Running CI on self-hosted cowshed runners deletes most of a workflow's setup (Nix install, cache
restore, `bun install`) and moves your registry and git secrets off GitHub's servers into the runner host's gateway.

This is a Linux/ZFS story (see [zfs.md](zfs.md)) — instant clone, no attach, ephemeral by construction.

## The one factual constraint

GitHub-hosted runners on free/open-source plans **cannot boot a custom or NixOS image.** The custom-image feature only
ever existed on the (now discontinued) larger-runner preview, never on standard or OSS runners. So "our own image with
Nix and ZFS already available" necessarily means **self-hosted**. That's the point of cowshed runners, not a workaround.

## Runner host setup

The runner host is a Linux box with ZFS and multi-user Nix. The NixOS module registers ephemeral GitHub runners and
keeps a headless `main` workspace warm. Its repository binding records the chosen remote and the corresponding stable
`repo_id` (the remote's lowercase `owner/repo`); startup refuses a mismatch. Discovery may suggest a binding but never
silently creates one. Multiple identities are allowed with one primary, and a local-only repository requires an explicit
`repo_id`.

```nix
{
  services.cowshed-runner = {
    enable = true;
    repository = {
      remote = "https://github.com/smoothbricks/smoothbricks.git";
      repoId = "smoothbricks/smoothbricks";
      primary = true;
    };
    # Ephemeral runners: each registers, runs one job, deregisters.
    ephemeral = true;
    count = 4;
    labels = [ "self-hosted" "cowshed" "zfs" ];
    pool = "rpool";                 # cowshed datasets live under rpool/cowshed
    # The headless main workspace this host clones jobs from.
    main = {
      ref = "main";
      # Refreshed by the green main-branch job itself (see below).
    };
    # Gateway holds registry + git credentials host-side, out of GitHub.
    gateway.enable = true;
  };
}
```

Trusted project policy is `~/.cowshed/<owner>/<repo>/policy.json`, with `owner` and `repo` encoded as separate path-safe
components. Here `~` belongs to the trusted cowshed controller identity, not the ephemeral runner account. Policy is
created by trusted host bootstrap, owned by the controller, and unavailable to runner and job processes. A checkout,
workflow, discovery result, or job hook can neither mint repository identity nor rewrite policy; a missing or
inconsistent binding is a host-bootstrap error.

`services.cowshed-runner` also installs the ZFS mount helper and the gateway service, and points runner registration at
a `GITHUB_TOKEN`/app credential read from the host secret-service — the only place a token is needed.

### The warm-main flywheel

A cowshed runner's `main` workspace is the base every job clones. It stays warm because **the main-branch CI job
refreshes it**: on a green build of `main`, the job's final step promotes its own workspace state (or fast-forwards the
headless main and re-runs the warm targets). So the warmth CI produces is the warmth the next job inherits — the same
main-as-base convention cowshed uses everywhere, applied to the runner host. No template job, no nightly rebuild.

## How a job flows

1. **Register** — an ephemeral runner picks up the job (labels `[self-hosted, cowshed, zfs]`).
2. **Clone** — the runner's trusted job-start hook creates `ci-<run_id>-<attempt>` as a snapshot + clone of the headless
   main before any repository-controlled payload starts. The composite action does not create the confinement boundary.
3. **Intercept every command** — before the runner launches any repository-controlled command, including the first, the
   runner integration dispatches it through `cowshed exec` in that workspace. There is no direct-runner fallback. An
   action type is supported only when all repository-controlled processes it can launch are interceptable; otherwise the
   step is rejected before its payload starts.
4. **Destroy** — the trusted job-completed hook destroys the clone and origin snapshot regardless of job outcome. The
   ephemeral runner then deregisters. Host `cowshed gc` sweeps orphaned `ci-*` workspaces after crashes.

## What you delete from the workflow

Against the current `ci.yml`, cowshed mode removes:

- **`setup-devenv`** (Determinate Nix install, Nix store NAR restore, Cachix, devenv build) — the clone already has a
  built devenv shell.
- **`cache-nx` / `actions/cache`** for `.nx` — the workspace carries warm Nx state from main.
- **`cache-node-modules` / any `bun install`** — `node_modules` is materialized in the clone.
- **Most `secrets.*`** — registry auth lives in the host gateway, injected per-request by workspace identity; git
  credentials stay host-side entirely (workspace git is local-paths-only — the clone's `host` remote and gateway-fetched
  mirrors). Only `GITHUB_TOKEN` (for status/PR API) stays in the workflow.

What remains is the actual work: `smoo github-ci nx-smart --target build|lint|test`, now running against warm inputs.

## Using the composite action

The same job body runs on GitHub-hosted or cowshed runners — `mode: auto` picks cowshed when the runner has it and falls
back to GitHub's Nix setup otherwise:

```yaml
jobs:
  main:
    name: Validate
    # cowshed runners advertise these labels; ubuntu-latest is the fallback.
    runs-on: ${{ vars.CI_RUNNER || 'ubuntu-latest' }}
    steps:
      - uses: actions/checkout@v6.0.2
        with: { filter: blob:none, fetch-depth: 0 }

      - name: 🧱 Setup environment
        id: cowshed
        uses: ./.github/actions/smoothbricks-ci
        with:
          mode: auto # auto | github | cowshed

      # Unchanged: the same targets, warm on cowshed, cold-with-cache on GitHub.
      # On a cowshed runner, the runner integration intercepts each command and invokes cowshed exec.
      - run: smoo github-ci nx-smart --target build --name "Build" --step 6
      - run: smoo github-ci nx-smart --target lint  --name "Lint"  --step 7
      - run: smoo github-ci nx-smart --target test  --name "Unit Tests" --step 8
```

On a cowshed runner, `steps.cowshed.outputs.workspace-path` is the job-hook-created clone; on GitHub it is the plain
checkout, so downstream job bodies do not branch. Pin `mode: github` to force the hosted path (for example, to reproduce
a hosted-only failure), or `mode: cowshed` to fail loudly if a job was mis-routed to a runner without cowshed. The
action only provisions the environment and publishes a cwd; it cannot wrap later steps and is never credited as a
sandbox boundary.

## Public-repo security posture

Self-hosted runners on public repos are normally dangerous — a fork PR can run arbitrary code on your infrastructure.
cowshed makes it defensible, not risk-free, and the standard guardrails still apply:

- **Require approval for fork-PR workflows** (repo setting: _Require approval for all outside collaborators_). This is
  mandatory, not optional.
- **Ephemeral runners**: one job per runner, then deregister — no state carries from a malicious job to the next.
- **Universal interception for supported actions**: every repository-controlled command, including the first, runs via
  `cowshed exec`; an action type that cannot be intercepted completely is rejected before any payload starts.
- **Sandboxed commands**: Landlock confinement limits each command to its throwaway workspace and designated caches;
  egress is loopback-only through the gateway. Host secrets, cowshed state, trusted policy, and other jobs stay denied.
- **Runner-unit defense in depth**: the dedicated unprivileged runner unit is independently filesystem-confined away
  from cowshed state, bindings, `~/.cowshed/<owner>/<repo>/policy.json`, gateway credentials, and sockets, and its
  network is forced through the gateway. A narrow controller interface permits lifecycle and `cowshed exec` dispatch
  without exposing backing files. This remains mandatory even though `cowshed exec` is the repository-command boundary.
- **Secrets aren't on GitHub to leak**: registry and git credentials live only on the runner host. A compromised job
  reaches them only through the gateway, which scopes every request to the job workspace's egress grants and audits it
  (Arrow telemetry store, `cowshed audit`).
- **Dedicated runner group**: scope ephemeral runners to this repository, and never use `pull_request_target` to check
  out untrusted code.

Neither cwd relocation nor the composite action confines a process. The security claim depends on both universal runner
interception and the independent unit sandbox; the throwaway clone limits persistence but is not itself a host sandbox.
