# The nx-plugin keys the targets it infers; a consumer's nx.json does not

`packages/nx-plugin` (inference in `src/index.ts`, the policy in `src/workspace-config-policy.ts`).

## What is measured

A consumer workspace holds 15 target-default rules in its `nx.json`, which it wrote itself on 2026-10-11, 2026-10-21 and
2026-11-10. This repository's own `nx.json` holds none of them. Thirteen say something that is true of every workspace
the plugin serves:

| Rules | Target default                                                                                | What the rule adds                                                                                                                                                | Why the consumer needed it                                                                                                                                                                           |
| ----- | --------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 7     | `*`, `cargo-test-*`, `tsc-js`, `test`, `lint`, each filtered by one of the plugin's executors | named inputs over the executor's module closure (`nxBoundedExec`, `nxTypescriptEmit`, `nxWranglerDeploy`, `nxAllVerdicts`, `nxNextestExtract`, `nxNextestConfig`) | `smoo.developerLinks` points the plugin at a checkout outside the workspace, where no file hash and no lockfile entry sees the code a task runs                                                      |
| 3     | `*`, `build`, `lint`, filtered by `nx:noop`                                                   | `{ "externalDependencies": [] }`                                                                                                                                  | Nx keys a task whose executor is not an `@nx/` one on every package in the lockfile, and a cached target with no inputs on every file of its project; five cargo aggregates hashed about 4,600 files |
| 3     | `tsc-js`, `typecheck`, `lint`, unfiltered                                                     | the ttsc transformer, every `tsconfig*.json` of the project, the dependencies' production files                                                                   | the inferred inputs name `tsconfig.lib.json` alone and no transformer, and a type-aware lint names no dependency; each gap replayed a wrong result from the cache                                    |

The other two are the consumer's own: its tests key the Rust artifacts they load, and its `build` default names its
inputs. Each rule carries its measurement in a `"//"` member beside it.

The consumer wrote them as Nx's list form, one entry per `filter.executor`. `applyBuildTargetDefault`
(`src/workspace-config-policy.ts:396-397`) reads `targetDefaults.build` through `getOrCreateRecord` (`:579-587`), which
replaces a value that is not a record: `smoo monorepo update` writes `"build": { "cache": true }` and both entries are
gone, with exit 0. `validateBuildTargetDefault` (`:296-299`) reports the same list as "targetDefaults.build.cache must
be true". That defect has its own unit (shed `ttsc-up`); it is named here because it is why the rules were not seen.

## What to change

1. The plugin sets on each target it infers the inputs that target needs, so no consumer states them:
   - a target that runs one of the plugin's executors keys that executor's module closure, by the files a linked
     checkout resolves, less the modules its `executors.json` record lists as output-neutral;
   - an `nx:noop` aggregate the plugin infers keys its own project configuration and no package of the lockfile;
   - the inferred `typecheck` and `tsc-js` key the file set of the tsconfig they compile, every `tsconfig*.json` of the
     project, the transformer that changes their output, and their dependencies' production files; the inferred `lint`
     keys its dependencies' production files.
2. The named inputs those rules use that hold only inside the consumer today (`nxPluginReach`, `nxPluginTypes`, and the
   scripts that compute them under the consumer's `tooling/`) move with them or are replaced by what the plugin already
   knows about its own package.
3. `smoo monorepo validate` refuses a consumer rule that repeats what the plugin infers, by name, and
   `smoo monorepo update` removes it, leaving the consumer's own entries and their `"//"` members untouched.

## Done when

- A workspace with a developer link to the plugin and no target-default rule of the thirteen: an edit to a module of the
  bounded-exec closure changes the hash of a task that executor runs, and an edit to an output-neutral module does not;
  an edit to a sibling `tsconfig.*.json` changes `typecheck` and `tsc-js`; a lockfile edit leaves a noop aggregate's
  hash alone. Each is a test over Nx's own hash, as the consumer's rules were proven.
- The consumer deletes its thirteen rules and its task hashes for an unchanged tree are explained entry by entry: the
  same inputs reach each task, from the plugin instead of from `nx.json`.
- This repository's own `nx.json` loses the `externalDependencies` entries of `build`, `cargo-test` and `cargo-lint`
  that say the same thing by hand.
