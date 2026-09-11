# @smoothbricks/cli

`@smoothbricks/cli` provides `smoo`, the SmoothBricks monorepo automation CLI. It is the control plane for shared CI,
release, Git hook, package metadata, and publish validation conventions across SmoothBricks-style repositories.

The tool is intentionally convention-over-configuration. SmoothBricks repos use [Nx], [Bun], [Nix], and [devenv]
(activated natively, or through [direnv] in repos that have not migrated), so `smoo` assumes those pieces exist instead
of adding another local config file. Repos should be made correct by running the mutating initialization path, then kept
correct by the read-only validation path.

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
smoo release version --bump <auto|patch|minor|major|prerelease> [--projects <projects|all>] [--dry-run] [--github-output <path>]
smoo release publish --bump <auto|patch|minor|major|prerelease> [--dry-run]
smoo release retag-unpublished <tag...> [--to <ref>] [--push] [--dispatch] [--remote <remote>] [--branch <branch>] [--dry-run]
smoo release bootstrap-npm-packages [--dry-run] [--skip-login] [--package <name...>]
smoo release trust-publisher [--bootstrap] [--dry-run] [--skip-login] [--package <name...>]

smoo secrets status [-R <owner/name|remote>] [--env <environment>] [--json]
smoo secrets set [name] [-R <owner/name|remote>] [--env <environment>]
smoo secrets sync [-R <owner/name|remote>] [--env <environment>]
smoo secrets run <group> <command...>

smoo github-ci nx-smart --target <target> [--name <check-name>] [--step <number>] [--mode <auto|affected|run-many>] [--stage <stage>]
smoo github-ci nx-run-many --targets <targets> [--projects <projects>] [--collect-outputs <directory>]
smoo github-ci nx-deploy [--stage <stage>] [--mode <auto|affected|run-many>] [--select-tag <tag>] [--verify]
smoo github-ci apply-outputs <directories...> --source-sha <sha>
smoo github-ci dispatch-workflow --workflow <workflow> --ref <ref>
smoo github-ci ensure-pull-request --head <branch> --base <branch> --title <title> --body <body>
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
an explicit output style on the alias and `continuous: true` on the Nx target. Astro/Vite dev servers use
`--outputStyle=dynamic-legacy`; other continuous targets use `--outputStyle=stream`. Simple leading environment
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

`@smoothbricks/nx-plugin` owns transformer-aware TypeScript targets because this workspace compiles through `ttsc`.
Configuring `@nx/js/typescript` would run `tsc`/`tsgo` directly and bypass the Typia and LMAO transformers. A package
`tsconfig.lib.json` therefore produces the concrete `tsc-js` target from the SmoothBricks plugin. The same plugin also
infers Bun test typechecking, Cargo workspace targets, and aggregate targets.

Concrete targets use `{tool}-{output}` names and describe the tool that runs and the artifact or purpose it produces:

- `tsc-js` comes from `@smoothbricks/nx-plugin` and runs `ttsc` for package JavaScript/declaration output.
- Packages that run `bun test` must have `tsconfig.test.json`. Bun executes tests without typechecking, so smoo creates
  a no-emit `typecheck-tests` target from that config and wires it into validation. Other test runners may own their own
  typecheck path.
- Test tsconfigs are validation configs, not TypeScript build-mode projects. They must use `noEmit`, must not set
  `composite: true`, and package root `tsconfig.json` must not reference `./tsconfig.test.json`. The inferred
  `typecheck-tests` target runs `ttsc --noEmit -p tsconfig.test.json` after `build` instead.
- A neighboring workspace-root `Cargo.toml` provides Cargo test, lint, mutation, and benchmark targets. Workspaces with
  `cdylib` member crates also receive the cacheable `cargo-wasm` output target.
- `build` is an aggregate. It exists only when there is at least one concrete build target such as `tsc-js`,
  `tsdown-js`, or another tool-output target, and it depends on output-family wildcards such as `*-js`, `*-web`,
  `*-html`, `*-css`, `*-ios`, `*-android`, `*-native`, `*-napi`, `*-bun`, and `*-wasm` instead of duplicating commands.
- `lint` is an aggregate validation target. It is not a formatting target.

The root `@typescript/native` dependency follows TypeScript's documented side-by-side pattern: it aliases TypeScript 7
and supplies the native compiler used by `ttsc`. Because `ttsc` resolves only the unscoped package by default, the
managed devenv shell sets `TTSC_TSGO_BINARY` to `node_modules/@typescript/native/bin/tsc`; the GitHub setup action
persists that absolute path through `GITHUB_ENV`. Nx and other JavaScript tooling still require the full TypeScript
compiler API, so workspace `typescript` stays on TypeScript 6. After install, setup-environment forces Bun's shared
`.bun/node_modules/typescript` hoist onto that API package so Nx does not load `@typescript/native` (TS7) via the store
([oven-sh/bun#33834](https://github.com/oven-sh/bun/issues/33834)). Both dependencies and the environment binding are
required: TypeScript 7 under the unscoped name breaks Nx API calls such as `readConfigFile`.

Explicit Nx target names must not contain `:`. Nx already uses colon syntax at the CLI boundary:
`project:target:configuration`. Allowing target names like `build:wasm` makes command parsing and package-script aliases
look like configurations, and it prevents a clean split between concrete tool-output targets and aggregate targets.

Use tool-output names for concrete targets, such as `tsc-js`, `tsdown-js`, and `cargo-wasm`. Use `build` and `lint` only
as aggregate targets. Package scripts may still use developer-friendly colon names, for example `build:wasm`, but those
scripts should delegate to unambiguous Nx targets such as `nx run pkg:cargo-wasm`.

## Package Structure

Workspace packages keep all TypeScript sources — and especially all test files — under a single root: `src/`.

```
packages/<name>/
  src/
    foo.ts
    foo.test.ts          # unit tests: colocated with the module they defend
    __tests__/           # cross-module/integration TS tests, still under src/
  crates/*/tests/        # Rust integration tests (cargo-owned convention, unaffected)
  scripts/               # non-shipped tooling; never *.test.ts
  dist/, target/         # generated output, never scanned
```

The test-location rule is not tidiness; it is what makes the generated tooling provably cover every test. The convention
is one _root_, not one path: `tsconfig.test.json` includes are generated as `src/**` patterns, and the bounded
`bun test` targets run with `cwd <package>/src` — both because `bun test <arg>` treats the argument as a filter over a
scan rooted at the cwd (scanning from the package root would walk a Rust package's entire cargo `target/` tree, tens of
seconds per run), and because a second test root would have to be threaded through every one of those consumers forever.
A test file outside `src/` is therefore neither typechecked nor executed, silently. `smoo monorepo validate` fails on
any `*.test.ts` / `*.spec.ts(x)` outside `src/` so that gap cannot reappear.

Within the single root, separation still exists where it matters: unit tests sit next to their modules, integration
tests live in `src/__tests__/`, and Rust crates keep cargo's own `tests/` directories, which the rule deliberately
ignores.

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

The generated publish workflow is canonical Prettier YAML. Running the repository formatter over
`.github/workflows/publish.yml` is byte-stable and does not create managed-file drift.

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

The generated pre-push hook runs only on macOS. Linux `nx lint` already compiles the Linux `cfg(target_os)` arm; Darwin
does not. The hook is a probe: it runs `env -u CC_x86_64_unknown_linux_gnu nx run-many -t cargo-lint-cross` and nothing
else. Nx caches that target on the Cargo inputs, so a hit is a prior real Linux clippy and the push proceeds. A miss
refuses the push and names `bun run check:linux` — the hook never enters linux-cross and never starts a compile, so what
it costs is one Nx graph construction rather than an unbounded clippy. `bun run check:linux` is
`tooling/devenv -P linux-cross shell --` around that same Nx target; it is the only place the gate actually runs, so it
is deliberately not quiet. `git push --no-verify` skips the hook.

The toolchain variable is unset for the probe on purpose. The target's own command reads it to decide whether the cross
toolchain is present, so a push made from inside an already-entered linux-cross shell would otherwise fall through into
a real multi-minute compile — the thing the probe exists to avoid. It is not a declared input of the target, so
unsetting it cannot change the task hash: a warm entry still hits. Measured both ways, including with the variable set
to a nonexistent compiler.

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

- `setup-devenv` detects the runner kind first. An ephemeral runner installs a single-user [Nix], restores `/nix` itself
  from the Actions cache — store paths and the Nix database, as files, so nothing is imported — enables [Cachix],
  installs [devenv] at `devenv.lock`'s rev, restores `node_modules` and the ttsc plugins, and builds the shell. A
  host-nix runner (`NIX_REMOTE=daemon` with `/var/cache/ci`) skips the install and every store cache: it already has the
  store and keeps its caches on the shared bind.
- `save-nix-devenv` runs under `always()` and saves the `.devenv`/`.direnv` eval-cache segment when setup missed it and
  the shell produced `nix-eval-cache.db`. The store cache saves itself in `setup-devenv`'s post phase, which collects
  garbage down to the live closure before uploading.
- `cache-nix-devenv` is the shared restore/save primitive for the `.devenv`/`.direnv` segment.

The cache split is intentional. The store segment is large and keyed by the expensive shell closure inputs
(`devenv.yaml`, `devenv.nix`, `devenv.lock`), and it carries store content only — never the Nix profiles, which exist to
be gcroots: restoring a previous run's copies over a freshly installed Nix de-roots the running Nix binary, and the post
phase then collects it. The `.devenv`/`.direnv` segment is small, but it holds absolute `/nix/store` pointers, so it
restores on every ephemeral runner and devenv checks that the shell's derivation and output still exist on an eval-cache
hit, re-evaluating when garbage collection removed them.

The bootstrap script is intentionally small. It only handles work required before `smoo` can run in GitHub Actions:

- Install `devenv`, held to `devenv.lock`'s rev on an ephemeral runner.
- Build the devenv shell and add repo-local tooling to `GITHUB_PATH`.

### Deploy configuration (`package.json` → `smoo.github`)

- `pushBranches`: the first entry is the branch whose pushes deploy the staging stage (default `main`).
- `environments.staging` / `environments.production`: GitHub Environments put on the validate + e2e jobs and on the
  production-on-push job respectively. The staging Environment goes on Validate for every run of a deploying repo, pull
  requests included, so a staging Environment with required reviewers would gate every pull request's Validate.
- `deploySecrets`: extra secrets for deploy steps, as a map of env var name → repository secret name, rendered
  `NAME: ${{ secrets.SECRET }}` (GitHub forbids `GITHUB_`-prefixed secret names, so the two may differ). The keys
  `CLOUDFLARE_API_TOKEN` / `CLOUDFLARE_ACCOUNT_ID` replace the default Cloudflare mapping.
- `e2eSecrets`: the same map shape for the e2e-deployment step only.
- `previewUrls`: required URL templates for pull-request stages (`{stage}` is replaced, and required in every template).
  The first one becomes the GitHub deployment URL, all are listed in the step summary. There is no default: a
  pull-request deploy with selected projects and no `previewUrls` fails instead of inventing a hostname, while a stage
  with no selected projects still skips quietly. `SMOO_PREVIEW_ZONE` is no longer read; move its host into `previewUrls`
  (for example, a zone that used to build `https://app.<stage>.<zone>` becomes `["https://app.{stage}.<zone>"]` with the
  zone's real hostname in place of `<zone>`).

Cloudflare deploys and cleanups need `CLOUDFLARE_ACCOUNT_ID` and `CLOUDFLARE_API_TOKEN`. The token needs Workers
Scripts, Workers KV, R2, and D1 write, plus Zone DNS and Workers Routes write for preview hostnames. A token without D1
access makes cleanup refuse before it deletes anything rather than half-clean a stage.

A wrong value type anywhere in `smoo.github` fails `smoo monorepo update` and `smoo github-ci nx-deploy` with the
offending path, rather than silently falling back to the defaults.

#### What CI deploys: the deploy tags

A `deploy` target says a project CAN be deployed. Four Nx tags say whether CI does, and they are the only thing that
says so: the generated workflows and `smoo github-ci nx-deploy` read one rule, so a repository cannot render a deploy
job that deploys nothing, nor deploy a project no job announced.

- `stage-deploy-target`: deployed on every stage — pull-request previews, staging, and production.
- `staging-deploy-target`: deployed on staging only, for infrastructure the pull-request stages share.
- `permanent-deploy-target`: excluded from every stage deploy; deployed outside the stage flow.
- `production-push-deploy-target`: also deployed by the generated production-on-push job (below).

An untagged `deploy` target is deployable by hand — `nx run <project>:deploy --stage=pr42` — and invisible to CI. A
repository where no project carries one of these tags gets no deploy step, no `deployments: write` permission, no
Cloudflare credentials and no `pr-preview-cleanup.yml`, however many wrangler manifests its packages hold: a published
library ships one to document a Durable Object binding for its consumers, and that manifest looks exactly like a
deployable worker's.

Tag a project `production-push-deploy-target` to have it deployed to production by a generated `deploy-production` job
that runs after Validate and the e2e job succeed on a push to the staging push branch
(`smoo github-ci nx-deploy --stage production --select-tag production-push-deploy-target`). The project must also carry
`stage-deploy-target`, otherwise `--select-tag` finds nothing and the job logs
`No run-many deploy projects; skipping production.`

#### Ordering one deploy after another

When a project's deploy calls into what another project deploys — a site that signs in to its stage's backend — say so
with an ordinary Nx edge on the deploying project:

```json
{ "nx": { "targets": { "deploy": { "dependsOn": ["...", "app-backend:deploy"] } } } }
```

Nx then orders it, at any depth: `site:deploy` after `app-backend:deploy` after `db:deploy` all resolve inside the one
`nx run-many` that `smoo github-ci nx-deploy` already issues, and `nx deploy site --stage=…` run by hand gets the same
order. Keep the leading `"..."`: it expands the `deploy-build` edge the plugin infers (below), which a bare list would
drop.

What makes the edge safe is that a deploy is never cached and always cheap when there is nothing to do:

- `@smoothbricks/nx-plugin` gives the `deploy` it infers `cache: false`. An Nx cache hit on a deploy means "we once
  uploaded this hash", which a rollback silently falsifies — the workspace is unchanged, so the hash is unchanged, so a
  cached deploy would report success while the previous version keeps serving.
- `smoo wrangler deploy-stage` reads live state first. If the version tagged with this task's hash is already the one
  serving 100% of traffic, it returns `remote-cache-hit` after two API calls: no upload, no migration, no traffic shift.
- The expensive, purely file-derived half belongs in a sibling `deploy-build` target (build the artifact, register it,
  refresh what the build needs). The plugin gives that one `cache: true` and makes `deploy` depend on it, so a redundant
  deploy costs two API calls rather than a rebuild.
- A deploy does not finish when Cloudflare accepts the traffic shift; it finishes when the new version is the one being
  served. `deploy-stage` polls `wrangler deployments status` until the tag it activated is live, and fails with what it
  expected and what it saw if that never happens. Pass `--version-endpoint <url>` to also require an endpoint served by
  the worker — whose trimmed response body is the running version tag — to answer with it. Without that wait, an edge
  onto a deploy orders nothing: the step returns while the edge still serves the old code.

**The policy caveat, plainly.** A cross-project edge is part of the graph, not of the selection: a run that selects only
the dependent project will still evaluate the dependency's `deploy`. When the dependency is already at that hash, that
evaluation is a no-op — two API calls. But it is not inert: if the dependency is intentionally behind (its production
deploy is being held back, say), a run that selects only the dependent project **will advance the dependency to the hash
the current workspace produces**. If you need a project to be deployable without touching what it depends on, do not add
the edge; sequence those two deploys as separate CI jobs instead.

`smoo wrangler deployed-version --stage <stage>` prints the tag serving all traffic for the project's worker on that
stage, so an operator can check the same fact the deploy checks. It caches its answer briefly under Nx's workspace data
directory; that cache is a convenience for repeated queries only. Nothing that decides whether to deploy reads it, and
it refuses — rather than answering `unknown` — without credentials, on an API error, or while traffic is split between
two versions.

Pushes to the staging push branch queue behind a running workflow instead of canceling it, so a newer push never cancels
a production deployment mid-flight. Pull requests and other branches keep canceling superseded runs. The e2e and
production jobs repeat the Cargo credential and sibling-source preflight before SetupDevenv, so their `--step` anchors
shift with the configuration instead of staying fixed.

### Private dependency configuration (`package.json` → `smoo.github.cargoCredentials`)

The private git origins Cargo fetches from, and the secret that reads each one:

```json
{
  "smoo": {
    "github": {
      "cargoCredentials": {
        "gitOrigins": [
          {
            "origin": "https://git.example.net",
            "tokenEnv": "SOURCE_READ_TOKEN",
            "internalMirror": "http://10.89.0.1:3000",
            "sshOrigins": ["ssh://forgejo@forge.example.net:2223/", "ssh://forge.example.net:2223/"]
          }
        ],
        "registryTokenEnvs": ["CARGO_REGISTRIES_EXAMPLE_TOKEN"]
      }
    }
  }
}
```

- `origin`: the credential-free https origin, and `tokenEnv` the repository secret that reads it. The generated
  credential helper answers for that host alone, reading the token from the environment when git calls it.
- `internalMirror`: the address managed runners reach the same forge at, such as a container-bridge address. Managed CI
  rewrites the origin prefix onto it with `url.<mirror>.insteadOf` and answers the same credential for the mirror's
  host, because git hands helpers the rewritten URL.
- `sshOrigins`: the SSH spellings of that same forge, as a lockfile pins them (`Cargo.toml` git dependencies, uv
  sources). Each one gets its own `insteadOf` line onto the mirror. git matches `insteadOf` values as literal URL
  prefixes and derives no spelling from another, so a forge pinned as `ssh://forgejo@host:2223/` and as
  `ssh://host:2223/` needs both declared; a runner holding only the mirror's read token and no SSH key would otherwise
  fetch nothing. The declarations stay credential-free — the rewrite happens before transport, so an SSH pin nobody
  rewrote fails loudly instead of collecting a token — and each requires `internalMirror`, since an SSH spelling is a
  rewrite source and nothing else.
- `registryTokenEnvs`: `CARGO_REGISTRIES_<NAME>_TOKEN` secrets Cargo's own credential provider reads for private
  registries.

### Remote cache configuration (`package.json` → `smoo.remoteCache`)

One Nx self-hosted remote cache shared by every runner and every developer shell:

```json
{
  "smoo": {
    "remoteCache": {
      "server": "https://nx-cache.example.net",
      "internalServer": "http://10.89.0.1:8765",
      "tokenSecret": "NX_REMOTE_CACHE_TOKEN"
    }
  }
}
```

- `server`: the origin every developer machine reaches. Credential-free, no path, and no trailing slash — Nx appends
  `/v1/cache/<hash>`, so a trailing slash asks for a doubled-slash route that answers 404 forever. Such a declaration is
  refused at render time rather than trimmed, because managed CI and the developer shell read the same field.
- `internalServer`: the origin managed runners reach instead, such as a container-bridge address. Same declaration as a
  git origin's `internalMirror` — it says this repository's runners sit inside that network — so every generated job
  takes it while shells outside keep `server`. Omitted means CI uses `server` too.
- `tokenSecret`: the repository secret holding the cache token, and the variable name a developer shell resolves locally
  (an ambient value, or a `smoo.secrets` entry). A read-only token is the honest choice for an untrusted context: it
  reads the cache and cannot publish into it.

Both generated workflows put `NX_SELF_HOSTED_REMOTE_CACHE_SERVER` and `NX_SELF_HOSTED_REMOTE_CACHE_ACCESS_TOKEN` in
every job env that runs Nx. The pair is emitted whole or not at all: Nx enables its cache on a nonempty server alone and
accepts only 200 or 404 from it, so a server it cannot authenticate to fails every task on 401 instead of missing
quietly. That is also why a declared cache puts the same-repository gate on Validate that a private dependency install
does — a fork pull request receives no secrets, and a job with half the pair would fail everything.

Developer shells get the pair from the managed `tooling/direnv/secret-references.ts`, which the managed devenv
`enterShell` runs and `eval`s: it prints the two exports when the declared token has a value, prints nothing when it has
none (with the reason on stderr), and never replaces a server the environment already carries, so a CI job keeps the
internal address its own runners reach.

`tokenSecret` may name a `smoo.secrets` entry, and then it is in group `nx-cache`: shell entry never resolves it, so an
unreachable secret provider loses the cache rather than the install. `op read` being unavailable in a sandboxed
workspace therefore costs the cache, not the shell. See the section below for what a group is.

### Local secret commands and groups (`package.json` → `smoo.secrets`)

```json
{
  "smoo": {
    "secrets": {
      "SMOO_TOKEN": { "command": ["op", "read", "op://vault/smoo/token"] },
      "ACME_NPM_TOKEN": { "command": ["op", "read", "op://vault/registry/token"] },
      "DEPLOY_TOKEN": { "command": ["op", "read", "op://vault/deploy/token"], "group": "deploy" }
    }
  }
}
```

Each entry names a command whose stdout is the value. An existing nonempty environment value always wins, and CI never
runs these commands at all: there, every variable comes from the job's secret store.

A group says which operation resolves the credential, and shell entry — every direnv reload, every
`devenv shell -- <command>` — resolves the `shell` group and nothing else. A provider authorises per requesting process
lineage, so a command placed at shell entry is a credential prompt on every reload; a credential only one deliberate
command needs belongs to that command:

```bash
smoo secrets run registry bun install
smoo secrets run registry bun add -d @acme/sdk
smoo secrets run nx-cache nx build app
```

The group is DERIVED from declarations the repository already carries, so almost nothing declares one:

- `registry` — a variable `.npmrc` interpolates as `${VAR}`. An installed checkout contacts no registry, so shell entry
  defers it; a request that does contact one is what resolves it.
- `nx-cache` — the variable `smoo.remoteCache.tokenSecret` names.
- `shell` — everything else, resolved at shell entry because that is when it is needed.

`group` on an entry overrides the derivation, and any label the command line can name is a group: nothing about
`registry` or `nx-cache` is privileged in smoo, so a repository may declare `deploy` and run it the same way. An
override is stated in `smoo secrets status`, because a silent one is how the next reader loses an hour.

`smoo secrets run` with no group refuses and lists the groups this repository declares with the secrets in each. It
resolves exactly one group — a run for `registry` never triggers the cache token's provider command — and passes the
values in the child's environment only: never in argv, which `ps` shows to every user on the machine, never in a file,
and never on stdout. Everything after the group reaches the child verbatim, flags included, and the child's exit status
is reproduced, including death by signal as 128+signum.

Both contexts that cannot run a provider command refuse by name instead of promising a command that would not work
there: CI says to inject the variable from the secret store, and a cowshed workspace says to enroll registry credentials
through the gateway.

## Releases

Release commands wrap [Nx Release][nx-release] but keep SmoothBricks policy in one place.

Versioning:

- `--bump auto` first filters owned release packages to package-local candidates, then lets [Nx Release][nx-release]
  derive the semver bump from [Conventional Commits]. A tagged package is an auto candidate only when files under its
  package root changed since its current `projectName@version` release tag. An untagged package is an auto candidate
  only when its package root has git history and its current version is stable. Root-only changes, workflow edits,
  lockfile-only churn, untagged next-prerelease preparation commits, and other workspace-global changes may still affect
  Nx tasks, but they do not make unrelated package artifacts releasable.
- `--bump patch|minor|major|prerelease` forces the release specifier only; it never widens the release set. Package
  selection comes from `--projects`: blank selects the package-local changed set (the same filter `--bump auto` uses),
  whatever the bump mode is. `--projects <a,b,...>` releases exactly those owned Nx projects without change detection,
  and `--projects all` is the deliberate whole-fleet opt-in that versions every owned release package.
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
- An explicit bump with a non-empty selection is mandatory progress when `HEAD` is not already a release target: after
  pending releases are repaired, `bump=patch|minor|major|prerelease` must make Nx create a new release commit, and smoo
  fails if Nx returns without moving `HEAD`. Every bump mode may no-op with `mode=none` when the selection is empty: no
  package-local changes for blank `--projects`, or no owned release packages at all for `--projects all`.
- `--dry-run` previews versioning and completion without pushing refs, publishing npm packages, or writing GitHub
  Releases.
- The pack path maps unpublished `-next` lock entries to the last stable tag because `bun pm pack` resolves
  `workspace:*` from `bun.lock`. Supported Buns keep the lockfile fresh on install
  ([18906](https://github.com/oven-sh/bun/issues/18906), [20477](https://github.com/oven-sh/bun/issues/20477),
  [20829](https://github.com/oven-sh/bun/issues/20829)), so no staleness repair remains.
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
- After a successful non-dry-run stable release at the branch tip, `smoo release publish` runs an untagged
  `prerelease --preid next` version bump for the released stable packages and pushes that branch commit. This prepares
  the codebase for the next development prerelease without creating prerelease release tags or publishing npm packages.
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
- npmjs accepts `--provenance` only from GitHub-hosted runners (`RUNNER_ENVIRONMENT=github-hosted`). On GitHub Actions,
  smoo refuses a public provenance publish when `RUNNER_ENVIRONMENT=self-hosted` before calling npm, so the failure
  names runner policy instead of trusted-publishing credentials. Private Forgejo publication does not use provenance.
  Local public publishes (no `GITHUB_ACTIONS`) are unchanged.
- [npm CLI][npm] owns publish authentication. Packages use [trusted publishing][npm-trusted-publishing] with [GitHub
  Actions OIDC][github-actions-oidc] from the workflow's `id-token: write` permission. Package names must exist on npm
  before CI publish runs; use `smoo release trust-publisher --bootstrap` locally to publish `0.0.0-bootstrap.0` under
  the `bootstrap` dist-tag for new package names before configuring trust.
- `smoo release bootstrap-npm-packages` scans owned `npm:public` release packages missing from npm, runs
  `npm login --auth-type=web` unless `--skip-login` is passed, and publishes a minimal placeholder package with
  `--access public --tag bootstrap`. It supports `--dry-run` and `--package <name...>` for targeted bootstraps.
- `smoo release trust-publisher` configures [npm trusted publishing][npm-trusted-publishing] for every owned release
  package. It uses the root `package.json` `repository.url` as the GitHub `owner/repo`, uses `publish.yml` as the
  trusted workflow, grants that workflow npm's `--allow-publish` permission, and runs `npm trust` from PATH, where the
  devenv-pinned Node supplies an npm new enough for the feature. It does not pre-login: `npm trust list` and
  `npm trust github` own normal authentication so npm can offer the 5-minute trust/publish challenge bypass. If
  `npm trust list` denies access, smoo reports the active npm identity and package owners, opens
  `npm login --auth-type=web` once so the operator can switch accounts, then retries the lookup. A second denial fails
  instead of looping. Pass `--package <name...>` to target specific owned packages. Pass `--bootstrap` to create missing
  npm package names first, then configure trusted publishing in the same command. With `--bootstrap`, `--skip-login`
  only skips the placeholder publish login. Existing matching trusted publishers are skipped via
  `npm trust list <package> --json`.

GitHub Releases:

- `smoo release publish` delegates to `nx release changelog` once per owned package whose current package release tag is
  at `HEAD`, passing that package's version explicitly for independent releases after npm publish succeeds.
- Nx project changelogs are configured to create or update GitHub Releases, not local changelog files.
- Generated release notes are package-scoped Conventional Commit changelogs. Init defaults author rendering and GitHub
  username lookup on, while validation allows repos to override those render options.

The release flow is designed to be rerun after partial failure. Nx owns local version/tag behavior, while smoo derives
durable completion state from the remote branch, release tags, npm registry versions, and GitHub Releases. Repeated
Publish runs converge without self-spawning another workflow run.

## Wrangler Commands

### `smoo wrangler deploy-stage --stage <stage> [--config <path>]`

Without `--config`, deploys the project's own source configuration with `--env <stage>`; a `prN` stage is derived from
`[env.staging]`. The source configuration is discovered in wrangler's own precedence — `wrangler.jsonc`,
`wrangler.json`, `wrangler.toml` — and the two JSON spellings are read with the JSONC parser wrangler uses for them, so
comments and trailing commas are fine. A project carrying more than one of those files is refused by name: wrangler
would read the first and ignore the rest without saying so, and that is how a repo ends up deploying a config nobody is
editing.

Both formats parse into one model, so a stage derives identically from either: the same plan, the same resources, the
same refusals. The committed file is never rewritten. A `prN` stage's derived configuration is written beside it as a
temporary `.wrangler.smoo-*.json` that the deploy deletes afterwards — JSON whichever format the source was, because
wrangler selects its parser by extension and a file read once by a machine has no reader for comments.

With `--config <path>` the target is a build-generated, env-block-free `wrangler.json` (what the Cloudflare Vite and
Astro adapters emit). `staging` and `production` deploy it as-is, with no `--env` flag. A `prN` stage treats it as the
staging template and derives a copy beside it: worker name (`<base>-prN` from a `-staging` name), routes and vars by
hostname label (hosts without a `staging` label are pinned to staging and dropped; a template whose routes are all
pinned is refused, since the stage would deploy unrouted), KV namespaces created by title, R2 buckets, D1 databases
created by name with their migrations applied, `services` bindings and rate limits. Cleanup (`cleanup-pr`) removes every
resource carrying the `prN` segment, D1 included.

- `--config` deploys ignore `CLOUDFLARE_ENV`. There is no `--env` flag for a flat config, so wrangler would otherwise
  fall back to that variable and rename the worker after it.
- The secrets manifest (`.dev.vars.example`) and the temporary secrets file come from the working directory, not from
  beside the `--config` file.
- Secrets are gated per stage. `smoo.wrangler.secretStages` in the project's `package.json` maps a declared secret name
  to the stages that require it, and the mapping cuts both ways: a listed stage REFUSES to deploy when that secret has
  no value in the environment and none on the Worker, and an unlisted stage never RECEIVES it even when the deploying
  shell exports one. `preview` matches any `prN`. A name absent from the map is required by every stage.
- The refusal happens before any Cloudflare mutation and names every missing secret at once, so a first deploy reports
  the whole list instead of one name per attempt. Values never appear in a message or in argv.
- Both directions are load-bearing, and neither is a lint. `--secrets-file` applies additively — wrangler deletes
  nothing it is not told about — so a secret introduced after a stage's first deploy would otherwise never arrive and
  the Worker would keep serving without it, silently. And a token scoped to preview stages, exported by a shell that
  also deploys production, would otherwise install a test-only capability into production.
- Migrations run only for the D1 bindings that declare a `migrations_dir`.
- For a `prN` stage, an R2 bucket or D1 database whose name has no exact `staging` segment is refused, before any
  Cloudflare resource is created: reusing the name verbatim would share staging's data with the pull request.
- A non-wildcard route gets no DNS record from this command; the stage's wildcard record must already exist.
- D1 migrations are auto-confirmed: the command captures wrangler's output, so wrangler sees a non-interactive session
  and answers its own "apply migrations?" prompt with yes. Point it only at a stage you mean to migrate.

## Why This Shape

The important design goal is one source of truth per convention:

- [Nx] `npm:public` tags decide what has a public npm package contract.
- Matching root/package `repository.url` values decide which public packages are released by the current repo.
- Managed files decide what generated CI and hooks should look like.
- Root package metadata provides defaults only for owned public packages.
- Actual workspace package names decide which dependency ranges become `workspace:*`.
- Package manifests decide Bun lockfile workspace versions; the pre-publish pack maps unpublished `-next` entries to the
  last stable tag.
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
