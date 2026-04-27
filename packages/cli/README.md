# @smoothbricks/cli

`@smoothbricks/cli` provides `smoo`, the SmoothBricks monorepo automation CLI. It is the control plane for shared CI,
release, Git hook, package metadata, and publish validation conventions across SmoothBricks-style repositories.

The tool is intentionally convention-over-configuration. SmoothBricks repos use Nx, Bun, Nix, devenv, and direnv, so
`smoo` assumes those pieces exist instead of adding another local config file. Repos should be made correct by running
the mutating initialization path, then kept correct by the read-only validation path.

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
smoo monorepo validate
smoo monorepo update
smoo monorepo check
smoo monorepo diff
smoo monorepo validate-commit-msg <commit-msg-file>
smoo monorepo sync-bun-lockfile-versions
smoo monorepo list-public-projects
smoo monorepo validate-public-tags
smoo monorepo release-state

smoo release version --bump <auto|patch|minor|major|prerelease> [--dry-run]
smoo release publish --bump <auto|patch|minor|major|prerelease> [--dry-run]
smoo release github-release --tags <tags> --bump <auto|patch|minor|major|prerelease>

smoo github-ci cleanup-cache
smoo github-ci nx-smart --target <target> --name <check-name> --step <number>
smoo github-ci nx-run-many --targets <targets> [--projects <projects>]
```

## Initialization

`smoo monorepo init` is the fix-first command. It mutates the repository toward the SmoothBricks convention instead of
only reporting drift.

It currently:

- Updates managed CI, release, hook, and formatting files.
- Ensures the local `tooling/smoo` source shim is executable when present.
- Synchronizes root runtime versions inside devenv, or when `--sync-runtime` is passed.
- Applies publish metadata defaults to `npm:public` packages.
- Normalizes internal workspace dependency ranges to `workspace:*`.
- Runs `sherif --fix --select highest` for broad monorepo package hygiene.
- Normalizes conditional export ordering so `types` comes first and `default` comes last.
- Adds `src` to package `files` when development-only exports intentionally point at source files.

The workspace dependency rule is generic. `smoo` does not know about individual package names such as `eslint-stdout`.
For every root or workspace `package.json`, if a dependency name matches an actual package in the same workspace, `smoo`
rewrites that range to `workspace:*`.

`smoo monorepo init --runtime-only` only synchronizes root runtime versions. It is used from direnv setup so
`packageManager`, `engines.node`, and `@types/node` stay aligned with the active devenv shell without duplicating that
policy in the direnv script.

Bun types are also a root runtime policy. The root `@types/bun` version follows the exact `packageManager` Bun version,
while package manifests are not forced to depend on Bun just because a test tsconfig opts into Bun globals. If a package
does explicitly declare `@types/bun`, `sherif` can keep duplicate declarations consistent, but `smoo` owns the semantic
root `bun@x.y.z` to `@types/bun x.y.z` relationship.

## Validation

`smoo monorepo validate` is the read-only gate. It should pass in local shells and CI after packages have been built.

It checks:

- Managed file drift.
- Root package policy.
- Root Bun type version matches the Bun package manager version.
- Nx release policy.
- Public package tag policy.
- Public package metadata.
- Workspace dependency ranges.
- `sherif` package hygiene.
- Packed public package artifacts with `publint`.
- Packed public package type resolution with the `attw` CLI.

The packed-package checks validate what npm users will install, not only the source tree. `smoo` packs each `npm:public`
package with Bun, runs `publint` on the tarball, then runs `attw` on the same tarball.

The `attw` check uses the `node16` profile and ignores the CJS-to-ESM warning. SmoothBricks packages are ESM-first, so
CommonJS consumers can use dynamic import. Node 10-only subpath failures are intentionally ignored because Node 10 is
not part of the supported package contract.

## Publishable Packages

Publishability is declared with an Nx tag:

```json
{
  "nx": {
    "tags": ["npm:public"]
  }
}
```

This tag is the source of truth for release selection, metadata validation, and publish artifact validation. `smoo`
never relies on a second hardcoded package list.

Rules:

- Publishable packages must not be private.
- Private packages must not have `npm:public`.
- Every non-private workspace package must have `npm:public`.
- Public packages must define license metadata.
- Public packages must publish with `publishConfig.access = "public"`.
- Public package repositories must point at the root repo and include `repository.directory`.
- Public packages must define `files`.
- Public library packages must define `types`.
- Public packages must define either `exports` or `bin`.

`smoo monorepo init` fixes the metadata that can be derived from the root package. For example, it copies the root
license and repository URL, then sets each package's `repository.directory` from its workspace path.

## Managed Files

`smoo monorepo update` writes the managed files into a repository.

Managed files include:

- `.git-format-staged.yml`
- Git hook scripts under `tooling/git-hooks`
- direnv/GitHub Actions bootstrap scripts under `tooling/direnv`
- GitHub Actions workflows under `.github/workflows`
- Local composite GitHub Actions under `.github/actions`

When a managed target is a symlink, `smoo` leaves it alone. SmoothBricks uses symlinks back to `packages/cli/managed` so
changes to the CLI package are tested immediately. Downstream repos receive ordinary committed copies.

Use:

```bash
smoo monorepo update
smoo monorepo check
smoo monorepo diff
```

`check` fails when a managed file is missing or stale. `diff` reports drift without writing files.

## Git Hooks

The generated pre-commit hook runs `git-format-staged` from the repository root with `tooling`, `node_modules/.bin`, and
the devenv profile on `PATH`.

The generated commit-msg hook delegates conventional commit validation to:

```bash
smoo monorepo validate-commit-msg <commit-msg-file>
```

This keeps hook behavior consistent with CI and avoids duplicating commit message parsing in shell.

## GitHub Actions

The generated workflows keep readable YAML and named steps, while larger logic lives in `smoo` commands or the small
pre-smoo bootstrap script.

CI uses explicit lint, test, and build phases. The publish workflow does the same before running release commands so
GitHub output remains readable even though Nx release also has its own `preVersionCommand` safety net.

The bootstrap script is intentionally small. It only handles work required before `smoo` can run in GitHub Actions:

- Restore Nix store cache state.
- Install `devenv`.
- Build the devenv shell and add repo-local tooling to `GITHUB_PATH`.

## Releases

Release commands wrap Nx release but keep SmoothBricks policy in one place.

Versioning:

- `--bump auto` uses Nx conventional-commit versioning.
- `--bump patch|minor|major|prerelease` forces the release specifier.
- Release projects are discovered from `npm:public` packages.

Publishing:

- `prerelease` publishes with npm dist-tag `next`.
- Stable bumps publish with npm dist-tag `latest`.
- Conflicting explicit dist-tags are rejected.

GitHub Releases:

- `smoo release github-release` creates or updates releases for the selected tags.
- `latest` status follows the derived npm dist-tag.

The release flow is designed to be rerun after partial failure. It checks the current package versions and tags so a
rerun can resume publishing instead of blindly bumping again.

## Why This Shape

The important design goal is one source of truth per convention:

- Nx `npm:public` tags decide what is publishable.
- Managed files decide what generated CI and hooks should look like.
- Root package metadata provides defaults for public packages.
- Actual workspace package names decide which dependency ranges become `workspace:*`.
- `sherif` handles broad package hygiene.
- `publint` and `attw` validate real packed artifacts.

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
