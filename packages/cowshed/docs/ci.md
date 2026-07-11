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
keeps a headless `main` workspace warm:

```nix
{
  services.cowshed-runner = {
    enable = true;
    repo = "smoothbricks/smoothbricks";
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

`services.cowshed-runner` also installs the ZFS mount helper and the gateway service, and points runner registration at
a `GITHUB_TOKEN`/app credential read from the host secret-service — the only place a token is needed.

### The warm-main flywheel

A cowshed runner's `main` workspace is the base every job clones. It stays warm because **the main-branch CI job
refreshes it**: on a green build of `main`, the job's final step promotes its own workspace state (or fast-forwards the
headless main and re-runs the warm targets). So the warmth CI produces is the warmth the next job inherits — the same
main-as-base convention cowshed uses everywhere, applied to the runner host. No template job, no nightly rebuild.

## How a job flows

1. **Register** — an ephemeral runner picks up the job (labels `[self-hosted, cowshed, zfs]`).
2. **Clone** — the composite action runs `cowshed new ci-<run_id>`: snapshot + clone of the headless main, tens of
   milliseconds. The workspace already has Nix/devenv built, `node_modules` installed, caches warm.
3. **Steps run sandboxed** — build/lint/test execute via cowshed's exec pipeline inside the workspace: Landlock-confined
   filesystem, loopback-only egress through the gateway, the same grants and exit-code-6 contract as everywhere else.
4. **Destroy** — the ephemeral runner reaps every dataset under the job when it deregisters, so nothing persists between
   jobs. To reclaim immediately, add a trailing `if: ${{ always() }}` step running `cowshed rm ci-${{ github.run_id }}`
   (the composite action can't self-clean — composite actions have no post-job hook — and it prints this reminder on
   stderr). `cowshed gc` on the host sweeps any orphaned `ci-*` workspace and its origin snapshot as a backstop.

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

      - name: 🧱 Setup (cowshed clone or GitHub Nix)
        id: cowshed
        uses: ./.github/actions/smoothbricks-ci
        with:
          mode: auto # auto | github | cowshed

      # Unchanged: the same targets, warm on cowshed, cold-with-cache on GitHub.
      - run: smoo github-ci nx-smart --target build --name "Build" --step 6
      - run: smoo github-ci nx-smart --target lint  --name "Lint"  --step 7
      - run: smoo github-ci nx-smart --target test  --name "Unit Tests" --step 8

      # cowshed mode: reclaim the workspace now (no-op on GitHub — COWSHED_CI_WORKSPACE is unset).
      - name: 🧹 Destroy cowshed workspace
        if: ${{ always() && env.COWSHED_CI_WORKSPACE != '' }}
        run: cowshed rm "$COWSHED_CI_WORKSPACE"
```

On a cowshed runner, `steps.cowshed.outputs.workspace-path` is the mounted clone; on GitHub it is the plain checkout, so
downstream steps don't branch. Pin `mode: github` to force the hosted path (e.g. to reproduce a hosted-only failure), or
`mode: cowshed` to fail loudly if a job was mis-routed to a runner without cowshed.

## Public-repo security posture

Self-hosted runners on public repos are normally dangerous — a fork PR can run arbitrary code on your infrastructure.
cowshed makes it defensible, and the standard guardrails still apply:

- **Require approval for fork-PR workflows** (repo setting: _Require approval for all outside collaborators_). This is
  mandatory, not optional.
- **Ephemeral runners**: one job per runner, then deregister — no state carries from a malicious job to the next.
- **Sandboxed steps**: even within a job, build/test run Landlock-confined with loopback-only egress. A hostile PR
  cannot read the host's secrets (they're in the gateway, denied to the sandbox), cannot reach the network except
  through the gateway's allowlist, and cannot write outside its throwaway clone.
- **Secrets aren't on GitHub to leak**: registry and git credentials live only on the runner host. A compromised job
  reaches them only through the gateway, which scopes every request to the job workspace's egress grants and audits it
  (Arrow telemetry store, `cowshed audit`).

The clone-is-a-blast-radius property does the heavy lifting: the worst a job can do to the host is fill its throwaway
dataset, which `cowshed rm` reclaims.
