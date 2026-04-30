# Nx Plugin Policy Refactor Plan

## Backstory

CI unit-test stress runs started hanging after test output appeared complete. The first visible symptom looked like
`lmao:test` had finished all tests and then stalled before Nx printed a completion footer. We added targeted cleanup and
process diagnostics, switched stress output to streaming, and sampled live processes during GitHub Actions runs.

The process sampler eventually showed that the live process was not LMAO cleanup. It was `packages/cli` running
`bun test`, with nested fixture subprocesses under that test process. Streaming logs identified a concrete timed-out CLI
fixture test in `packages/cli/src/release/__tests__/fixture-repo.test.ts`:

- `pushes current release refs to a local bare remote and another clone can fetch them`

The same fixture file runs real `git` operations, clones local bare repos, and invokes nested Nx builds from temporary
fixture workspaces. `packages/cli/src/release/__tests__/helpers/fixture-repo.ts` symlinks the real repo `node_modules`
into those temp workspaces, so `nx` resolves to the real workspace dependency graph and binaries.

## What We Tried And Learned

### Bun Timeout Semantics

We dug into Bun test behavior and found that Bun's per-test timeout marks the test as failed, but does not cancel the
async JS promise/test body. If the timed-out body started subprocess work, that work can continue after Bun has already
advanced to later tests. This explains the cascade: a fixture test times out, Bun reports the timeout, but orphaned
async work can keep running and later tests can still spawn more `git`/Nx processes.

This ruled out simply increasing Bun's timeout. A larger timeout only delays failure and still does not provide process
ownership or cleanup.

### Nx `run-commands` Timeout

We inspected the installed Nx `22.7.1` executor implementation and schema:

- `node_modules/.bun/nx@22.7.1+f2228094d74d4aeb/node_modules/nx/dist/src/executors/run-commands/schema.json`
- `node_modules/.bun/nx@22.7.1+f2228094d74d4aeb/node_modules/nx/dist/src/executors/run-commands/run-commands.impl.js`
- `node_modules/.bun/nx@22.7.1+f2228094d74d4aeb/node_modules/nx/dist/src/executors/run-commands/running-tasks.js`

Findings:

- `nx:run-commands` has no real wall-clock `timeout` option.
- It does kill child processes when Nx receives signals, and it uses `tree-kill` for sibling process cleanup in some
  parallel failure cases.
- It does not enforce a per-task deadline.
- Because the schema allows additional properties and Nx can forward unknown options into commands, adding
  `options.timeout` is not a safe timeout mechanism. It may become a runner argument such as `bun test --timeout=...`,
  which is exactly the wrong layer.

### GNU `timeout`

We considered wrapping commands with GNU `timeout`, for example:

```sh
timeout --kill-after=10s 10m bun test
```

This is attractive because `tooling/direnv/devenv.nix` already includes `coreutils`, and
`packages/cli/src/monorepo/tool-validation.ts` already validates `coreutils` as required tooling. It is runner-agnostic
and works as an immediate defensive policy.

Weaknesses:

- It is a shell-prefix convention, not structured Nx config.
- It is less cross-platform outside devenv/Linux.
- Quoting and command-prefix parsing are easier to get subtly wrong.
- Diagnostics are generic (`124`, `137`, etc.) instead of Nx-aware.
- It still leaves test-target policy split between CLI validation and shell command strings.

### Other Nx Plugins

We broadened research beyond built-in Nx executors. We searched npm and GitHub for Nx plugins, command runners, test
executors, timeout support, and process runners. Plausible packages included:

- `@simonegianni/nx-nodejs-test-runner`
- `nx-uvu`
- `@nx-extend/e2e-runner`
- `nx-pm2-plugin`
- `@ns3/nx-serverless`
- plugin collections such as `nxkit/nxkit`, `ZachJW34/nx-plus`, `twittwer/nx-tools`, and others

Findings:

- Most packages are runner-specific or domain-specific.
- Runner-specific timeout options map to runner semantics, not external process-tree kill semantics.
- No maintained generic Nx command executor clearly advertised hard wall-clock timeout with process-tree kill.

This makes a first-party executor more defensible than adopting an external dependency.

## Why Move Nx Policy Into `@smoothbricks/nx-plugin`

`smoo monorepo validate` currently owns many policies that are actually Nx configuration policy. Examples live mostly in
`packages/cli/src/monorepo/package-policy.ts`:

- `nx.json` plugin defaults, including `@smoothbricks/nx-plugin` and `@nx/js/typescript` configuration.
- Nx target naming policy: no colon target names, tool-output names like `tsc-js`, `tsdown-js`, and `zig-wasm`.
- Aggregate `build` target semantics and build-output dependency patterns.
- `namedInputs` and `targetDefaults` policy.
- Package script to Nx target migration.
- `typecheck-tests` and `tsconfig.test.json` policy.
- Nx release configuration required by `smoo release`.
- The future bounded test-target policy.

Those invariants are about how SmoothBricks workspaces use Nx. They belong in the Nx plugin that provides inferred
targets, generators, version actions, and future executors. The CLI should orchestrate validation and present failures,
not duplicate Nx-specific normalization logic.

This also keeps one source of truth for three consumers:

- Nx generators for manual migration.
- `smoo monorepo validate --fix` for repo automation.
- `smoo monorepo validate` for policy checking.

## Bootstrap Constraint: CLI Runs Under Bun

The SmoothBricks CLI intentionally runs with Bun and should bootstrap immediately. It does not require every package to
be built before `smoo` can run. The existing repo does build `@smoothbricks/nx-plugin` during devenv entry, but CLI
startup must stay lightweight.

Current `@smoothbricks/nx-plugin` package import is safe because its root export points at `loader.js`:

```js
import { existsSync } from 'node:fs';

const realPluginPath = new URL('./dist/index.js', import.meta.url);

export const createNodesV2 = [
  '**/package.json',
  async (...args) => {
    if (!existsSync(realPluginPath)) {
      return [];
    }
    const plugin = await import(realPluginPath.href);
    return plugin.createNodesV2[1](...args);
  },
];

export default { createNodesV2 };
```

We ran a small Bun experiment:

```sh
bun -e "const plugin = await import('@smoothbricks/nx-plugin'); console.log(Object.keys(plugin).join(',')); console.log(typeof plugin.createNodesV2, typeof plugin.default);"
```

Result:

```text
createNodesV2,default
object object
```

We also imported built plugin files directly with Bun:

```sh
bun -e "const plugin = await import('./packages/nx-plugin/dist/index.js'); console.log(Object.keys(plugin).join(',')); console.log(Array.isArray(plugin.createNodesV2));"
bun -e "const generator = await import('./packages/nx-plugin/dist/generators/bun-test-tracing/generator.js'); console.log(typeof generator.default);"
```

Both imports worked.

The experiment means we do not need to avoid Nx generator/devkit APIs on principle. Nx generators are useful precisely
because they operate on a virtual filesystem and are easy to unit test against realistic workspace migrations. The only
bootstrap requirement is practical: whatever `smoo` imports must run correctly in a Bun process before the whole repo
has been rebuilt from scratch. That is a testable constraint, not a reason to avoid generators.

The policy implementation should therefore be written as generator-friendly code: a core normalizer that can operate on
a Tree-like filesystem for generator tests and on the real filesystem for `smoo monorepo validate --fix`. If that shared
implementation imports Nx devkit, we should prove it with an explicit Bun import test and keep the root plugin loader
lazy as it is today.

## Target Architecture

Add a policy layer to `@smoothbricks/nx-plugin` that is callable both by generators and by `smoo`.

Important ownership clarification: `package.json` is not the reason this belongs in CLI. `package.json` is one possible
storage location for Nx project configuration and one possible source of a legacy `scripts.test` command. The policy is
still Nx policy because the normalized result is an Nx `test` target and an Nx script alias. The plugin must own the
normalizer and support both package-backed and project-json-backed projects:

- `package.json#nx.targets.test`
- `project.json#targets.test`
- `package.json#scripts.test` migrated into whichever project config file owns that project

The CLI should only orchestrate `smoo monorepo validate` and call the plugin policy API. CLI-local package policy tests
may keep coverage for non-Nx package/release/workspace metadata, but Nx target tests should move to the plugin.

Conceptual API:

```ts
export interface NxPolicyIssue {
  path: string;
  message: string;
}

export interface NxPolicyResult {
  changed: boolean;
  issues: NxPolicyIssue[];
}

export function checkSmoothBricksNxPolicy(root: string): NxPolicyResult;

export function applySmoothBricksNxPolicy(root: string): NxPolicyResult;
```

The API should be split internally into focused policies:

```ts
export function checkBoundedTestTargets(root: string): NxPolicyResult;
export function applyBoundedTestTargets(root: string): NxPolicyResult;

export function checkNxWorkspaceConfig(root: string): NxPolicyResult;
export function applyNxWorkspaceConfig(root: string): NxPolicyResult;

export function checkNxReleaseConfig(root: string): NxPolicyResult;
export function applyNxReleaseConfig(root: string): NxPolicyResult;

export function checkPackageNxTargets(root: string): NxPolicyResult;
export function applyPackageNxTargets(root: string): NxPolicyResult;
```

Generators should be first-class wrappers around the same APIs. For example:

```sh
nx g @smoothbricks/nx-plugin:bounded-test-targets
```

should call the same policy implementation that `smoo monorepo validate --fix` uses. Human users can still use Nx
generator dry-runs, and `smoo` can call the same API directly instead of shelling out to Nx and parsing dry-run output.

## Bounded Command Executor

Add a first-party executor:

```json
{
  "executor": "@smoothbricks/nx-plugin:bounded-exec",
  "options": {
    "command": "bun test",
    "cwd": "{projectRoot}",
    "timeoutMs": 600000,
    "killAfterMs": 10000
  }
}
```

Executor requirements:

- Stream stdout/stderr like `nx:run-commands`.
- Preserve `cwd` and `env` options.
- Enforce a hard wall-clock deadline.
- On timeout, terminate the whole process tree with `tree-kill` instead of hand-rolled platform-specific cleanup.
- Send a graceful signal first, then force kill after `killAfterMs`.
- Forward Nx CLI positional args in the same spirit as `nx:run-commands`.
- Preserve normal parent-process signal behavior after cleaning up the child tree.
- Return structured failure output that includes command, cwd, elapsed time, timeout, and whether force kill was needed.
- Avoid runner-specific timeout semantics.

Policy requirement:

- Any `test` target must use `@smoothbricks/nx-plugin:bounded-exec` or an explicitly approved equivalent.
- Package scripts should delegate to Nx, for example:

```json
"test": "nx run cli:test --tui=false --outputStyle=stream"
```

The underlying Nx target is the bounded unit of work.

## What Moves From CLI To Nx Plugin

### Current In-Progress State

The first bounded-execution slice is implemented but the ownership refactor is incomplete:

- `@smoothbricks/nx-plugin:bounded-exec` exists and owns hard timeout/process-tree cleanup.
- `@smoothbricks/nx-plugin/bounded-test-policy` exists and is imported by CLI.
- `bounded-test-targets` generator exists, but currently writes package-json-backed targets only.
- `smoo monorepo validate` calls plugin-owned bounded test policy, but the surrounding Nx policy still mostly lives in
  `packages/cli/src/monorepo/package-policy.ts` and its tests.
- CLI had to learn current `nx show projects` JSON output so post-build policy validation could resolve real inferred
  target names.

This is intentionally not the final boundary. The next refactor must move Nx target/config policy and tests into the
plugin and make the policy project-config-aware.

### Move First

1. Bounded test-target policy.
2. Bounded command executor.
3. Generator for bounded test-target migration.
4. CLI integration that calls policy APIs from `smoo monorepo validate` and `--fix`.

### Move Next

Nx workspace configuration policy:

- `@smoothbricks/nx-plugin` presence in `nx.json.plugins`.
- `@nx/js/typescript` plugin configuration.
- `targetDefaults.build.cache` and `targetDefaults.build.outputs`.
- `namedInputs.production` and `namedInputs.sharedGlobals`.
- no colon target names in `targetDefaults`.

Nx package target policy:

- no colon names in `package.json nx.targets`.
- no colon names in `project.json targets`.
- migration from colon names to tool-output names.
- `dependsOn` rewriting and validation.
- removal of redundant noop `build` targets when inferred build exists.
- target naming conventions such as `tsc-js`, `tsdown-js`, and `zig-wasm`.
- package script to Nx target conversion for safe build/test/dev commands, writing targets to `project.json` when that
  is the project's owning config file and to `package.json#nx.targets` otherwise.

Nx release configuration policy:

- `release.projectsRelationship = "independent"`.
- `release.version.specifierSource = "conventional-commits"`.
- `release.version.currentVersionResolver = "git-tag"`.
- `release.version.fallbackCurrentVersionResolver = "disk"`.
- `release.version.versionActions = "@smoothbricks/nx-plugin/version-actions"`.
- no `release.version.preVersionCommand`.
- `release.releaseTag.pattern = "{projectName}@{version}"`.
- changelog settings needed by SmoothBricks release behavior.

### Consider Later

Typecheck-test policy:

- `tsconfig.test.json` creation/defaults.
- no `tsconfig.json` reference to `./tsconfig.test.json`.
- `typecheck-tests` no-emit semantics.
- runner detection for Bun/Vitest.

This overlaps Nx inferred targets and TypeScript package ergonomics, so it probably belongs in the plugin eventually,
but it should move after the bounded executor/policy path is proven.

## What Stays In CLI

The CLI should keep policies that are not specifically Nx behavior:

- Root command policy such as `lint`, `lint:fix`, `format:staged`, and `format:changed`.
- Tool/devenv validation such as `bun`, `git`, `coreutils`, `jq`, `gnutar`, and `git-format-staged`.
- Workspace dependency range policy such as `workspace:*`.
- Public npm package metadata policy such as `license`, `publishConfig.access`, repository metadata, export condition
  ordering, and `npm:public` tags.
- Commit message validation and scope formatting.
- Release orchestration behavior: durable npm/GitHub state checks, repair planning, pushing refs, publishing, and
  user-facing CLI output.

The rule of thumb: if the invariant describes valid Nx config, move it to the plugin. If it describes broader repository
tooling, package publishing, or CLI workflow behavior, keep it in CLI.

## Implementation Plan

This repo is greenfield. Do not preserve legacy target shapes or add compatibility layers. Move directly to the correct
architecture.

1. Add `@smoothbricks/nx-plugin:bounded-exec` executor.
2. Add focused executor tests for successful command, nonzero command, timeout failure, graceful kill, force kill, and
   output behavior.
3. Add a shared Nx policy normalizer in `@smoothbricks/nx-plugin` that works for generator Tree tests and real workspace
   application.
4. Add a `bounded-test-targets` generator that uses the shared normalizer.
5. Add a Bun import/bootstrap test proving `@smoothbricks/cli` can import the plugin policy API under Bun.
6. Make `smoo monorepo validate` call the plugin policy check API.
7. Make `smoo monorepo validate --fix` call the plugin policy apply API.
8. Replace all existing package `test` targets with the bounded executor.
9. Remove obsolete CLI-local bounded/test-target policy once the plugin API owns it.
10. Add a project-config abstraction in the plugin that can read/check/apply policy against both `project.json` and
    `package.json#nx.targets`.
11. Move package target policy tests from CLI to plugin, including project.json coverage.
12. Reduce CLI tests to orchestration checks proving `smoo` calls plugin policy APIs.
13. Move the rest of the Nx-specific policy from CLI to plugin in the same direct style, without compatibility shims.

Validate with:

```sh
nx lint cli
nx lint nx-plugin
nx test nx-plugin
nx test cli
smoo monorepo validate
```

Keep a top-level CI `timeout` around the whole stress command as an independent emergency guard, not as the primary
test-target policy.

## Risks And Mitigations

- Risk: importing plugin policy from the Bun CLI fails because Nx devkit/generator code assumes Node behavior that Bun
  does not provide.
  - Mitigation: add an explicit Bun import/bootstrap test. If the shared normalizer needs Nx devkit, that is acceptable
    only if the import test proves it works under Bun.

- Risk: custom process supervision is buggy.
  - Mitigation: keep `bounded-command` tiny, test timeout and kill behavior directly, and keep CI outer timeout during
    rollout.

- Risk: CLI and generator policy diverge.
  - Mitigation: generator is a wrapper over shared policy APIs only.

- Risk: changing every test target at once obscures failures.
  - Mitigation: migrate in one commit, keep package-policy tests precise, and run all package tests through Nx.

- Risk: bounded executor breaks Nx caching/output behavior.
  - Mitigation: match `nx:run-commands` options shape where possible and let existing target cache settings remain on
    the target config, not inside the executor.

## Success Criteria

- All package `test` targets are externally bounded by structured Nx executor config.
- `smoo monorepo validate` fails on unbounded test targets.
- `smoo monorepo validate --fix` migrates unbounded test targets.
- The CLI can import plugin policy APIs under Bun without building/running Nx generator internals.
- Existing generators and CLI validation share the same policy implementation.
- Projects using `project.json` get the same policy/migration behavior as package-json-backed projects.
- CI stress runs fail fast and cleanly on hung tests instead of hanging indefinitely.
