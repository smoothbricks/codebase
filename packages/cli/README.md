# @smoothbricks/cli

`@smoothbricks/cli` provides `smoo`, the SmoothBricks monorepo automation CLI. It is the control plane for shared CI,
release, Git hook, package metadata, and publish validation conventions across SmoothBricks-style repositories.

The tool is intentionally convention-over-configuration. SmoothBricks repos use [Nx], [Bun], [Nix], [devenv], and
[direnv], so `smoo` assumes those pieces exist instead of adding another local config file. Repos should be made correct
by running the mutating initialization path, then kept correct by the read-only validation path.

## Install

Add the CLI to the root workspace:

```bash
bun add -d @smoothbricks/cli
```

The package exposes a Bun-native executable:

```bash
smoo --help
```

SmoothBricks itself self-hosts before `dist` exists by using `tooling/smoo`, which imports `packages/cli/src/cli.ts`
directly. Published installs use the package binary in `bin/smoo`, which imports built JavaScript from `dist`.

## Core Commands

```bash
smoo monorepo init [--runtime-only] [--sync-runtime]
smoo monorepo validate [--fail-fast] [--only-if-new-workspace-package]
smoo monorepo update
smoo monorepo check
smoo monorepo diff
smoo monorepo validate-commit-msg <commit-msg-file>
smoo monorepo sync-bun-lockfile-versions
smoo monorepo list-release-packages [--fail-empty] [--github-output <path>]
smoo monorepo validate-public-tags
smoo monorepo setup-test-tracing (--all | --projects <projects>) [--dry-run]

smoo release npm-status
smoo release repair-pending [--dry-run]
smoo release version --bump <auto|patch|minor|major|prerelease> [--dry-run] [--github-output <path>]
smoo release publish --bump <auto|patch|minor|major|prerelease> [--dry-run]
smoo release retag-unpublished <tag...> [--to <ref>] [--push] [--dispatch] [--remote <remote>] [--branch <branch>] [--dry-run]
smoo release bootstrap-npm-packages [--dry-run] [--skip-login] [--package <name...>]
smoo release trust-publisher [--bootstrap] [--dry-run] [--skip-login] [--package <name...>]

smoo github-ci cleanup-cache
smoo github-ci nx-smart --target <target> --name <check-name> --step <number>
smoo github-ci nx-run-many --targets <targets> [--projects <projects>]
```

## Initialization

`smoo monorepo init` is the fix-first command. It mutates the repository toward the SmoothBricks convention instead of
only reporting drift.

It currently:

- Updates managed CI, release, hook, and formatting files. The publish workflow is only written when the repo has owned
  release packages.
- Ensures the local `tooling/smoo` source shim is executable when present.
- Synchronizes root runtime versions inside devenv, or when `--sync-runtime` is passed.
- Applies safe publish metadata defaults to `npm:public` packages without inferring package ownership.
- Normalizes internal workspace dependency ranges to `workspace:*`.
- Rewrites safe package scripts in packages with workspace dependencies so developer commands like `bun run test` and
  `bun run dev` stay available while delegating through Nx targets.
- `smoo monorepo validate --fix` creates/updates `tooling/package.json`, keeps `@smoothbricks/cli` there instead of the
  root workspace package, and fills required workspace/devenv tool declarations.
- Runs [`sherif --fix --select highest`][sherif] for broad monorepo package hygiene.
- Normalizes conditional export ordering so `types` comes first and `default` comes last.
- Adds `src` to package `files` when development-only exports intentionally point at source files.

The workspace dependency rule is generic. `smoo` does not know about individual package names such as `eslint-stdout`.
For every root or workspace `package.json`, if a dependency name matches an actual package in the same workspace, `smoo`
rewrites that range to `workspace:*`.

Packages with internal workspace dependencies also need Nx-aware scripts so dependent builds run before local commands.
For safe build, test, typecheck, benchmark, dev, and preview commands, `smoo monorepo validate --fix` moves the real
command into `package.json` `nx.targets.<target>.options.command`, sets `cwd` to `{projectRoot}`, and replaces the
script with an `nx run <project>:<target>` alias. Continuous commands such as `astro dev`, `vite dev`, and previews get
`--tui=false --outputStyle=stream` on the alias and `continuous: true` on the Nx target. Simple leading environment
assignments are moved into `nx.targets.<target>.options.env` so commands such as
`NODE_OPTIONS='--import=extensionless/register' astro dev` remain shell-independent.

The rewrite is intentionally conservative. `smoo` does not rewrite deploy, database, release, sync, subtree, publish, or
pack scripts, and it rejects Nx target commands that recurse through package scripts such as `bun run test`. The reason
is dependency correctness without hiding unsafe operational commands behind generated Nx targets: workspace-dependent
packages should get `^build` ordering for ordinary development commands, while publishing and deployment stay explicit.

`smoo monorepo init --runtime-only` only synchronizes root runtime versions. It is used from direnv setup so
`packageManager`, `engines.node`, and `@types/node` stay aligned with the active devenv shell without duplicating that
policy in the direnv script.

Bun types are also a root runtime policy. The root `@types/bun` version follows the exact `packageManager` Bun version,
while package manifests are not forced to depend on Bun just because a test tsconfig opts into Bun globals. If a package
does explicitly declare `@types/bun`, `sherif` can keep duplicate declarations consistent, but `smoo` owns the semantic
root `bun@x.y.z` to `@types/bun x.y.z` relationship.

## LMAO Test Tracing

`smoo monorepo setup-test-tracing` configures LMAO-backed Bun test tracing for workspace packages. It is a bulk wrapper
around the `@smoothbricks/nx-plugin:bun-test-tracing` generator, so the Nx plugin remains the single source of truth for
the files written.

Configure every workspace package:

```bash
smoo monorepo setup-test-tracing --all
```

Configure selected packages by Nx project name, package name, or package root:

```bash
smoo monorepo setup-test-tracing --projects cli,@smoothbricks/lmao,packages/nx-plugin
```

The command infers the op context module from each package's `package.json` `name`, assumes an `opContext` named export,
and imports `defineTestTracer` from `@smoothbricks/lmao/testing/bun`. Override those defaults when a repository uses a
different convention:

```bash
smoo monorepo setup-test-tracing --projects my-lib --op-context-export myOpContext
smoo monorepo setup-test-tracing --projects my-lib --tracer-module @scope/testing/bun
```

Use `--dry-run` to print the `nx g @smoothbricks/nx-plugin:bun-test-tracing ...` invocations without writing files.
After setup, run `smoo monorepo validate --fix` to apply the broader SmoothBricks monorepo policy.

## Validation

`smoo monorepo validate` is the read-only gate. It should pass in local shells and CI after packages have been built.

It checks:

- Managed file drift.
- Root package policy.
- Root Bun type version matches the Bun package manager version.
- Tooling policy: root `package.json` owns workspace-level tools like `nx`, `tooling/package.json` owns `smoo`, and
  `tooling/direnv/devenv.nix` owns shell-provided tools like `bun`, `git-format-staged`, and `fmt`.
- Nx release policy, including project package release tags, project-level GitHub Release changelogs, and the temporary
  Bun lockfile versionActions hook.
- `bun.lock` workspace versions match package manifests.
- Public package tag policy.
- Public package metadata.
- Workspace dependency ranges.
- Workspace-dependent package scripts delegate safe commands through Nx targets without recursive script runners.
- Nx target conventions and inferred-task setup.
- [`sherif`] package hygiene, with warnings treated as validation failures.
- Packed public package artifacts with [`publint`].
- Packed public package type resolution with the [`attw`][are-the-types-wrong] CLI.

The packed-package checks validate what [npm] users will install, not only the source tree. `smoo` packs each
`npm:public` package with [Bun], runs [`publint`] on the tarball, then runs [`attw`][are-the-types-wrong] on the same
tarball.

The [`attw`][are-the-types-wrong] check uses the `node16` profile and ignores the CJS-to-ESM warning. SmoothBricks
packages are [ESM]-first, so [CommonJS] consumers can use dynamic import. Node 10-only subpath failures are
intentionally ignored because [Node.js] 10 is not part of the supported package contract.

`smoo monorepo validate --only-if-new-workspace-package` first checks the staged git diff for newly added workspace
package manifests. If none are staged, it exits successfully without running the full validator. The generated
pre-commit hook uses this mode so adding a package rechecks conditional managed files, including whether the publish
workflow is now required, without making every commit pay for full validation.

## Publishable Packages

Publishability is declared with an [Nx] tag:

```json
{
  "nx": {
    "tags": ["npm:public"]
  }
}
```

This tag is the source of truth for public npm metadata and publish artifact validation. Release selection adds one more
convention: a package is released by the current repository only when its `repository.url` exactly matches the root
`package.json` `repository.url`. Equivalent-but-different spellings, such as `git+https` vs SSH for the same GitHub
repo, fail validation because ownership should be explicit and visually obvious. This lets a workspace mirror public
packages from another repository without publishing them from the mirror.

Rules:

- `npm:public` packages must not be `private: true`.
- `private: true` packages must not have `npm:public`.
- Public packages must define license metadata.
- Public packages must publish with `publishConfig.access = "public"`.
- Public packages must define `repository.type`, `repository.url`, and `repository.directory`.
- Public packages must define `files`.
- Public library packages must define `types`.
- Public packages must define either `exports` or `bin`.

Owned public packages may inherit the root license when the root license is not `UNLICENSED`. Mirrored public packages
must carry their own license. `smoo monorepo init` does not copy the root `repository.url` into packages; a missing
package `repository.url` is a validation failure so new packages must consciously choose whether they are owned by the
current repository or mirrored from another one. Init still sets `publishConfig.access = "public"`, repository type,
repository directory, export ordering, and source-file publish entries when those can be derived safely.

`smoo monorepo list-release-packages` prints the comma-separated Nx project names for packages that are both
`npm:public` and owned by the current repository. Release commands, trusted-publisher setup, and the managed publish
workflow use this owned release package list instead of every public package in the workspace. smoo keeps both names in
release metadata: Nx commands, GitHub Release tags, and git release tags use `projectName`, while npm publish checks and
tarball validation use the real package `name`.

For [GitHub Actions], `smoo monorepo list-release-packages --fail-empty --github-output "$GITHUB_OUTPUT"` appends the
`projects=<nx-project-list>` output expected by the managed publish workflow and fails with a clear error when no owned
release packages exist.

`smoo release npm-status` shows whether each owned release package's current `name@version` already exists on npm. It is
an npm registry check, not a full release workflow status check.

`smoo release version --bump auto` first selects direct release candidates, then delegates versioning to [Nx]. Direct
candidates are owned public packages with package-local changes that can affect published users: files matched by the
package's resolved Nx `build`/`production` inputs, packaged assets listed in `package.json` `files`, package metadata
docs such as README/LICENSE/CHANGELOG, or user-visible `package.json` fields such as `exports`, `bin`, `types`,
`dependencies`, `peerDependencies`, and `publishConfig`. Test-only and local automation changes such as `scripts`, `nx`,
`devDependencies`, and `tsconfig.test.json` do not select a package by themselves.

Downstream dependency bumps are intentionally left to Nx release. If package A is selected and bumped, Nx may also bump
public package B when B depends on A, even when B has no direct file changes. `smoo` should not pre-expand direct
candidates to downstream dependents because that would duplicate Nx's dependency graph and can over-select packages.

## Nx Conventions

`smoo` keeps Nx target names predictable and separates tool work from aggregate workflows.

Official Nx plugins own targets they already know how to infer. For example, `@nx/js/typescript` owns TypeScript library
builds, so a package `tsconfig.lib.json` produces the concrete target `tsc-js`.

`@smoothbricks/nx-plugin` only fills SmoothBricks convention gaps that official plugins do not provide. Today that means
Bun test typechecking, non-TypeScript build-tool steps, and aggregate targets.

Concrete targets use `{tool}-{output}` names and describe the tool that runs and the artifact or purpose it produces:

- `tsc-js` comes from the official TypeScript plugin and runs `tsc` for package JavaScript/declaration output.
- Packages that run `bun test` must have `tsconfig.test.json`. Bun executes tests without typechecking, so smoo creates
  a no-emit `typecheck-tests` target from that config and wires it into validation. Other test runners may own their own
  typecheck path.
- Test tsconfigs are validation configs, not TypeScript build-mode projects. They must use `noEmit`, must not set
  `composite: true`, and package root `tsconfig.json` must not reference `./tsconfig.test.json`. The inferred
  `typecheck-tests` target runs `tsc --noEmit -p tsconfig.test.json` after `build` instead.
- Non-TypeScript build steps come from explicit tool configuration. For example, a package `build.zig` must expose named
  `b.step("name", ...)` entries; each non-reserved step becomes a `zig-name` target such as `zig-wasm`.
- `build` is an aggregate. It exists only when there is at least one concrete build target such as `tsc-js`,
  `tsdown-js`, or another tool-output target, and it depends on output-family wildcards such as `*-js`, `*-web`,
  `*-html`, `*-css`, `*-ios`, `*-android`, `*-native`, `*-napi`, `*-bun`, and `*-wasm` instead of duplicating commands.
- `lint` is an aggregate validation target. It is not a formatting target.

Explicit Nx target names must not contain `:`. Nx already uses colon syntax at the CLI boundary:
`project:target:configuration`. Allowing target names like `build:wasm` makes command parsing and package-script aliases
look like configurations, and it prevents a clean split between concrete tool-output targets and aggregate targets.

Use tool-output names for concrete targets, such as `tsc-js`, `tsdown-js`, and `zig-wasm`. Use `build` and `lint` only
as aggregate targets. Package scripts may still use developer-friendly colon names, for example `build:wasm`, but those
scripts should delegate to unambiguous Nx targets such as `nx run pkg:zig-wasm`.

## Managed Files

`smoo monorepo update` writes the managed files into a repository.

Managed files include:

- [`tooling/git-hooks/git-format-staged.yml`][git-format-staged]
- Git hook scripts under `tooling/git-hooks`
- [direnv]/[GitHub Actions] bootstrap scripts under `tooling/direnv`
- [GitHub Actions] workflows under `.github/workflows`
- Local composite [GitHub Actions] under `.github/actions`

When a managed target is a symlink, `smoo` leaves it alone. SmoothBricks uses symlinks back to `packages/cli/managed` so
changes to the CLI package are tested immediately. Downstream repos receive ordinary committed copies.

The publish workflow is conditional. Repositories with no owned release packages skip `.github/workflows/publish.yml` in
`init`, `check`, and `diff`; adding a new owned package makes the workflow required on the next validation run.

Use:

```bash
smoo monorepo update
smoo monorepo check
smoo monorepo diff
```

`check` fails when a managed file is missing or stale. `diff` reports drift without writing files.

## Formatting And Git Hooks

The root `lint:fix` script runs [`git-format-staged`][git-format-staged] with
`--config tooling/git-hooks/git-format-staged.yml --unstaged`. The formatter config intentionally excludes `bun.lock`.

The generated pre-commit hook runs the same formatter path from the repository root with `tooling`, `node_modules/.bin`,
and the [devenv] profile on `PATH`.

After formatting, the hook runs `smoo monorepo validate --fail-fast --only-if-new-workspace-package`. This keeps normal
commits fast while still catching incomplete package setup and conditional managed-file drift when a new workspace
package manifest is staged.

The generated commit-msg hook delegates conventional commit validation to:

```bash
smoo monorepo validate-commit-msg --fix <commit-msg-file>
```

This keeps hook behavior consistent with CI and avoids duplicating commit message parsing in shell. With `--fix`, smoo
wraps prose body paragraphs through `fmt -w 72` while preserving fenced code blocks, quoted markdown, indented blocks,
bullets, trailers, URLs, and comment lines.

Conventional commit scopes should use Nx project names. For packages in the same npm scope as the root package, smoo
requires `package.json` `nx.name` to be the unscoped package name, such as `cli` for `@smoothbricks/cli`, so subjects
like `fix(cli): repair release notes` map cleanly to Nx Release.

## GitHub Actions

The generated [GitHub Actions] workflows keep readable YAML and named top-level steps, while larger logic lives in
`smoo` commands, post-checkout composite actions, or the small pre-smoo bootstrap script. Checkout stays inline in each
workflow because repository-local composite actions do not exist until `actions/checkout` has populated the working
tree.

CI uses explicit lint, test, and build phases. The publish workflow does the same after versioning so GitHub output
stays readable and validation happens on the exact release commit.

CI status deeplinks depend on [GitHub Actions]' top-level job step anchors. The generated CI workflow keeps `# Step N`
comments next to each top-level step, and the `smoo github-ci nx-smart --step <number>` values for lint, test, and build
must stay synchronized with those comments. Composite action internals do not change the top-level step numbers.

Managed CI setup is split across local composite actions:

- `setup-devenv` installs [Nix], restores the Nix cache segment, imports the store NAR, restores `.devenv`/`.direnv`
  only after an exact Nix cache hit, enables [Cachix], installs [devenv], restores `node_modules`, and builds the shell.
- `save-nix-devenv` runs under `always()`, calls `smoo github-ci cleanup-cache`, and explicitly saves cache segments
  only when cleanup reports `cache-ready=true`.
- `cache-nix-devenv` is the shared restore/save primitive for the separate `nix` and `devenv` cache segments.

The cache split is intentional. The Nix segment contains the profile, Nix state, and exported store NAR. It is large and
keyed by the expensive shell closure inputs. The `.devenv`/`.direnv` segment is small, but it contains absolute
`/nix/store` pointers, so restoring it without the exact matching Nix store can leave metadata pointing at missing store
paths.

`smoo github-ci cleanup-cache` prepares the Nix segment for saving. It verifies and repairs the store, scans `.devenv`,
`.direnv`, and `~/.nix-profile` for embedded `/nix/store/...` references, protects the live paths with temporary GC
roots, runs garbage collection, and exports the resulting closure to `NIX_STORE_NAR`. The command writes
`cache-ready=true` or `cache-ready=false` to `GITHUB_OUTPUT` so the save action can avoid uploading incomplete caches.

The bootstrap script is intentionally small. It only handles work required before `smoo` can run in GitHub Actions:

- Restore Nix store cache state, clearing `.devenv` and `.direnv` if the matching NAR is missing or fails to import.
- Install `devenv`.
- Build the devenv shell and add repo-local tooling to `GITHUB_PATH`.

## Releases

Release commands wrap [Nx Release][nx-release] but keep SmoothBricks policy in one place.

Versioning:

- `--bump auto` first filters owned release packages to package-local candidates, then lets [Nx Release][nx-release]
  derive the semver bump from [Conventional Commits]. A tagged package is an auto candidate only when files under its
  package root changed since its current `projectName@version` release tag. An untagged package is an auto candidate
  only when its package root has git history. Root-only changes, workflow edits, lockfile-only churn, and other
  workspace-global changes may still affect Nx tasks, but they do not make unrelated package artifacts releasable.
- `--bump patch|minor|major|prerelease` forces the release specifier for the full owned release package set. Forced
  bumps intentionally bypass the package-local auto filter.
- Release packages are discovered from `npm:public` packages whose `repository.url` exactly matches the root package.
- [Nx Release][nx-release] config must use `currentVersionResolver: "git-tag"` with
  `fallbackCurrentVersionResolver: "disk"`. Conventional-commit versioning requires git tags as the primary source,
  while the disk fallback supports initial releases before package tags exist.
- [Nx Release][nx-release] config must use `versionActions: "@smoothbricks/cli/nx-version-actions"`. This wraps Nx's JS
  version actions and temporarily syncs `bun.lock` workspace versions after Nx runs `bun install --lockfile-only`.
- Same-org scoped packages must define short `package.json` `nx.name` values, for example `@smoothbricks/money` uses
  `"nx": { "name": "money" }`. This lets Nx Release understand commit scopes like `fix(money): ...` without requiring
  the npm org in every commit subject.
- Nx project names and npm package names are different release identities. Nx project filters, workflow `projects=`
  outputs, build/lint/test validation, project changelog lookup, GitHub Release tags, and durable git release tags use
  `projectName`. npm publish checks and tarball validation use package `name`.
- [Nx Release][nx-release] `preVersionCommand` is intentionally not used. smoo builds exactly the packages that still
  need npm publish immediately before packing them, while the managed workflow separately builds, lints, tests, and
  validates newly created release commits.
- `smoo release repair-pending` runs before the normal publish flow. It repairs older remote release tags whose npm
  package version or GitHub Release is missing, while leaving the current `HEAD` release target to `version` and
  `publish`.
- `smoo release version` selects the current release target before validation by running [Nx Release][nx-release]
  versioning. npm registry state is not used to decide whether versioning should run. If `HEAD` is already a release
  target, versioning returns `mode=none` and leaves idempotent completion to `smoo release publish`.
- `smoo release version --github-output "$GITHUB_OUTPUT"` appends `mode=new|none` and `projects=<comma-list>`. The
  managed publish workflow uses `mode != "none"` to build, lint, test, and validate exactly the commit that
  `smoo release publish` will publish. `projects` is a comma-separated Nx project-name list, not an npm package-name
  list. The validation and publish step names include the selected mode so the [GitHub Actions] run shows whether it is
  creating a new release or recording a no-op.
- Explicit bumps are mandatory when `HEAD` is not already a release target: after pending releases are repaired,
  `bump=patch|minor|major|prerelease` must make Nx create a new release commit. smoo fails if Nx returns without moving
  `HEAD`; `auto` may no-op when there are no releasable conventional commits.
- `--dry-run` previews versioning and completion without pushing refs, publishing npm packages, or writing GitHub
  Releases.
- The `nx-version-actions` hook is a temporary Bun workaround. Bun currently leaves `bun.lock` workspace versions stale
  after package manifest bumps, and `bun pm pack` rewrites `workspace:*` dependencies using those stale lockfile
  versions. Keep the hook until supported Bun versions resolve these issues:
  [oven-sh/bun#18906](https://github.com/oven-sh/bun/issues/18906),
  [oven-sh/bun#20477](https://github.com/oven-sh/bun/issues/20477), and
  [oven-sh/bun#20829](https://github.com/oven-sh/bun/issues/20829).
- Package release tags must use the Nx project name and version, for example `nx-plugin@0.0.2`. smoo derives release
  package/version pairs from that tag shape and maps project names back to npm package names before checking npm state.
- `smoo release retag-unpublished <tag...>` is a break-glass recovery command for the case where Nx already committed a
  version bump but npm publish failed before the package version became durable. It moves exact owned release tags to
  `HEAD` by default without bumping package manifests again. It refuses to move a tag when `package@version` already
  exists on npm, when the GitHub Release exists, or when the target ref's package manifest does not contain the tagged
  version. Pass `--push` to update remote tags with `--force-with-lease`; pass `--dispatch` to also start `publish.yml`
  with `bump=auto`. Dispatch validates that the target ref is already the remote branch head so the workflow will
  publish the same commit that was retagged.

### Repair Process

`repair-pending` is tag-driven, not history-driven. It starts from fetched remote release tags because those tags are
the durable record that a package version was selected for release. It only checks npm and GitHub Release state to
decide which tags still need work; it does not walk normal commits looking for release-shaped changes.

1. Collect owned release tags from the fetched remote tag set, sorted newest-first by annotated tag `creatordate`. Only
   tags matching owned Nx project release names are considered, and each tag is peeled to the commit it releases.
2. Classify each owned release tag before grouping by commit. A tag needs npm repair when `package@version` is missing
   from npm, and it needs GitHub repair when the GitHub Release for that tag is missing. Tags needing neither are
   filtered out immediately.
3. Group only repair-needed tags by peeled commit. Empty commits disappear because their tags were already complete.
   Exclude `HEAD` because the current release target is handled by `smoo release version` and `smoo release publish`,
   not by the older-release repair loop.
4. Sort the remaining repair commits oldest-to-newest. Only after this sorted non-HEAD repair list exists does smoo
   start checking out commits.
5. For each repair commit, check out the commit once and load that checkout's direnv environment once. If any grouped
   tag still needs npm publish, run `nx run-many -t build --projects=<comma-separated npm-missing Nx projects>` once,
   then publish those packages using the npm dist-tag implied by each package version. If the commit only needs GitHub
   Releases, skip the build. Finally, create the missing GitHub Releases for the grouped tags that need them.

Pending release state should be a suffix of the release-target timeline because `repair-pending` runs before every
publish. Once a complete release target is reached, older targets are assumed complete; an observed gap in repair state
violates the workflow invariant and should fail loudly instead of silently repairing history out of order.

Publishing:

- `prerelease` publishes with npm dist-tag `next`.
- Stable bumps publish with npm dist-tag `latest`.
- `smoo release publish` pushes missing branch/tag refs, publishes missing npm versions, creates or updates GitHub
  Releases, and writes a GitHub Step Summary. Already published npm versions are skipped, so reruns after auth or
  network failures retry only the package versions npm does not have yet.
- npm registry state gates publish idempotency only. It decides which already-versioned package tarballs still need to
  be published during a real release retry, not whether versioning should run or whether the workflow has a release to
  publish.
- Before npm publish, smoo runs `nx run-many -t build --projects=<comma-separated npm-missing Nx projects>` for exactly
  the packages whose `name@version` is not on npm yet. Nx cache makes this cheap when the managed workflow already built
  the same projects, and it keeps reruns self-sufficient when repairing a previously selected `HEAD` release target.
- Publish uses `bun pm pack` to create package tarballs, then publishes those tarballs with latest npm CLI and
  `--provenance`. Each package uses the npm dist-tag implied by its own version (`next` for prereleases, `latest` for
  stable versions). Bun pack resolves internal `workspace:*` dependency ranges to real versions in the tarball manifest;
  smoo fails before publish if a packed manifest still contains `workspace:` or if an internal dependency does not match
  the current workspace package version.
- [npm CLI][npm] owns publish authentication. Packages use [trusted publishing][npm-trusted-publishing] with [GitHub
  Actions OIDC][github-actions-oidc] from the workflow's `id-token: write` permission. Package names must exist on npm
  before CI publish runs; use `smoo release trust-publisher --bootstrap` locally to publish `0.0.0-bootstrap.0` under
  the `bootstrap` dist-tag for new package names before configuring trust.
- `smoo release bootstrap-npm-packages` scans owned `npm:public` release packages missing from npm, runs
  `npm login --auth-type=web` through `nix shell nixpkgs#nodejs_latest` unless `--skip-login` is passed, and publishes a
  minimal placeholder package with `--access public --tag bootstrap`. It supports `--dry-run` and `--package <name...>`
  for targeted bootstraps.
- `smoo release trust-publisher` configures [npm trusted publishing][npm-trusted-publishing] for every owned release
  package. It uses the root `package.json` `repository.url` as the GitHub `owner/repo`, uses `publish.yml` as the
  trusted workflow, and runs `npm trust` through `nix shell nixpkgs#nodejs_latest` because the Lambda-pinned Node 24/npm
  toolchain may lag the npm CLI feature. It does not run a separate `npm login` before trust setup; `npm trust list` and
  `npm trust github` own authentication so npm can offer the 5-minute trust/publish challenge bypass. Pass
  `--package <name...>` to target specific owned packages. Pass `--bootstrap` to create missing npm package names first,
  then configure trusted publishing in the same command. With `--bootstrap`, `--skip-login` only skips the placeholder
  publish login. Existing matching trusted publishers are skipped via `npm trust list <package> --json`.

GitHub Releases:

- `smoo release publish` delegates to `nx release changelog` once per owned package whose current package release tag is
  at `HEAD`, passing that package's version explicitly for independent releases after npm publish succeeds.
- Nx project changelogs are configured to create or update GitHub Releases, not local changelog files.
- Generated release notes are package-scoped Conventional Commit changelogs. Init defaults author rendering and GitHub
  username lookup on, while validation allows repos to override those render options.

The release flow is designed to be rerun after partial failure. Nx owns local version/tag behavior, while smoo derives
durable completion state from the remote branch, release tags, npm registry versions, and GitHub Releases. Repeated
Publish runs converge without self-spawning another workflow run.

## Why This Shape

The important design goal is one source of truth per convention:

- [Nx] `npm:public` tags decide what has a public npm package contract.
- Matching root/package `repository.url` values decide which public packages are released by the current repo.
- Managed files decide what generated CI and hooks should look like.
- Root package metadata provides defaults only for owned public packages.
- Actual workspace package names decide which dependency ranges become `workspace:*`.
- Package manifests decide Bun lockfile workspace versions until Bun stops leaving them stale during releases.
- [`sherif`] handles broad package hygiene.
- [`publint`] and [`attw`][are-the-types-wrong] validate real packed artifacts.

This keeps `smoo` small where external tools already do the job, but keeps SmoothBricks-specific policy native where
generic tools do not know the repo contract. In particular, `sherif` is useful for package hygiene, but it does not know
SmoothBricks publish metadata, release tags, generated workflow files, or Nx release policy. Those remain `smoo`
conventions.

## Local Verification

Typical verification after changing `smoo`:

```bash
nx typecheck @smoothbricks/cli
nx lint @smoothbricks/cli
smoo monorepo validate
```

If validating packages with Zig build steps from outside the devenv shell, add Zig explicitly:

```bash
nix shell nixpkgs#zig -c nx run-many -t build --projects=<public-projects>
```

## Links

[are-the-types-wrong]: https://github.com/arethetypeswrong/arethetypeswrong.github.io/tree/main/packages/cli
[Bun]: https://bun.sh/
[Cachix]: https://www.cachix.org/
[CommonJS]: https://nodejs.org/api/modules.html
[Conventional Commits]: https://www.conventionalcommits.org/
[devenv]: https://devenv.sh/
[direnv]: https://direnv.net/
[ESM]: https://nodejs.org/api/esm.html
[git-format-staged]: https://github.com/smoothbricks/git-format-staged
[GitHub Actions]: https://docs.github.com/actions
[github-actions-oidc]:
  https://docs.github.com/actions/deployment/security-hardening-your-deployments/about-security-hardening-with-openid-connect
[Nix]: https://nixos.org/
[Node.js]: https://nodejs.org/
[npm]: https://www.npmjs.com/
[npm-trusted-publishing]: https://docs.npmjs.com/trusted-publishers
[Nx]: https://nx.dev/
[nx-release]: https://nx.dev/features/manage-releases
[`publint`]: https://publint.dev/
[`sherif`]: https://github.com/QuiiBz/sherif
