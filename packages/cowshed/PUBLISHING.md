# Publishing Cowshed

## Goal

Publish one `@smoothbricks/cowshed` npm package containing every native binary the package claims to support. The
package must never be published from a single host build: a local or ordinary Nx build intentionally produces only the
current platform's binary.

The initial supported target set is:

| Runtime           | Rust target                 | Package artifact                    |
| ----------------- | --------------------------- | ----------------------------------- |
| macOS arm64       | `aarch64-apple-darwin`      | `dist/cowshed.darwin-arm64.node`    |
| macOS x64         | `x86_64-apple-darwin`       | `dist/cowshed.darwin-x64.node`      |
| Linux arm64 glibc | `aarch64-unknown-linux-gnu` | `dist/cowshed.linux-arm64-gnu.node` |
| Linux x64 glibc   | `x86_64-unknown-linux-gnu`  | `dist/cowshed.linux-x64-gnu.node`   |

Windows is not in this matrix because the public Cowshed API transfers Unix file descriptors and the N-API crate uses
`std::os::fd`. Linux musl is not in the initial contract. Adding either platform requires runtime support, a loader
case, a native smoke test on that runtime, and a required release artifact; it must not be represented as supported
before those exist.

## Package and loader contract

`packages/cowshed/package.json` owns the NAPI-RS binary name and target list. `@napi-rs/cli` turns Cargo's
platform-specific `cdylib` into the correctly named `.node` file; no repository script renames Cargo outputs.

The normal local build remains:

```sh
nx run cowshed:cargo-napi
```

It invokes `napi build --platform` and creates exactly the current host artifact. `packages/cowshed/src/native.ts`
derives the required filename from `process.platform` and `process.arch`, then loads that file. `COWSHED_NODE_PATH`
remains the explicit development/test override. There is no universal `dist/cowshed.node` fallback because such a
filename hides which platform produced the package.

All four release artifacts deliberately live in the same `dist/` directory and are included by the existing package
`files: ["dist"]` rule. Do not adopt NAPI-RS's optional-dependency/per-platform-package template: this package's release
contract is one npm tarball containing the complete matrix.

## Generic Nx target contract

Smoo must not know about Cowshed, NAPI-RS, Cargo, or particular package names. Platform work is selected by Nx target
name globs and declared target outputs.

Use output-family target names:

- `*-macos` for outputs that require a macOS runner;
- `*-ios` for Apple mobile outputs that require a macOS runner;
- `*-linux` for supplemental Linux-only release outputs;
- future runner families add their own suffix and target glob without changing package-specific workflow code.

Platform-only families are not dependencies of the ordinary aggregate `build` target. In particular, move `*-ios` out of
`BUILD_OUTPUT_DEPENDENCIES` when the platform producer is introduced and do not add `*-macos` there. Otherwise the
generic Linux `nx build` graph would try to execute Apple targets.

Cowshed uses the same convention as any other package:

- `napi-arm64-macos` builds `cowshed.darwin-arm64.node`;
- `napi-x64-macos` builds `cowshed.darwin-x64.node`;
- `napi-arm64-linux` builds the supplemental Linux arm64 artifact;
- the existing `cargo-napi` target remains the current-host build used by ordinary Linux build and test.

Each platform target invokes `napi build --target <rust-target>` directly and writes to a target-specific staging
directory. Every target declares precise Nx `inputs` and `outputs`; two targets never own the same output path.

Nx accepts project patterns but its `-t/--targets` argument is a list of exact target names. Extend
`smoo github-ci nx-run-many` so target arguments containing globs are expanded from resolved `nx show project --json`
metadata. Smoo groups the matched project/target pairs by exact target name and invokes Nx only for projects that own
that target. This works for any executor and any package type.

Add an output-collection mode to the same command. After successful execution it copies only the matched targets'
declared outputs into a staging tree while preserving workspace-relative paths and writes a manifest containing the
project, target, source SHA, output paths, sizes, and SHA-256 checksums. GitHub Actions uploads that staging tree; it
does not guess `dist/` paths or native filename conventions.

## GitHub Actions design

`.github/workflows/ci.yml` remains the generic Linux CI template. It contains no Cowshed name and no macOS job.

The generated publish workflow gains generic platform producer jobs. During `smoo monorepo init`, the existing managed
file context scans resolved Nx targets:

- no target matching a platform family: render the current single-platform publish workflow;
- one or more targets matching `*-macos` or `*-ios`: render a macOS producer;
- one or more targets matching `*-linux`: include them in the Linux release-candidate producer;
- future platform families use the same target-discovery and artifact contract.

The publish workflow generator receives discovered platform families, not package names. A Rust addon, Swift package,
Electron bundle, or any other package participates by defining a matching Nx target with declared outputs.

### Two-stage publish graph

The publish run has this job graph:

```text
linux-release-candidate ─┐
                         ├──> publish-on-linux
macos-platform ──────────┘
```

The two producers run in parallel. `publish-on-linux` is the only job that combines their output and the only job
allowed to publish. Repositories without macOS target matches omit `macos-platform` and keep the current shape.

### Stage 1: Linux release candidate

The Linux producer owns the existing publish workflow through validation:

1. check out the dispatch SHA;
2. run the existing Linux `setup-devenv` action;
3. build the self-hosted `smoo`;
4. repair pending releases;
5. create the local release commit and tags;
6. run selected projects' ordinary build, lint, test, and monorepo validation;
7. expand and run any discovered `*-linux` release target globs;
8. collect the declared Linux platform outputs.

It uploads immutable artifacts for this workflow run:

- a Git bundle containing the validated local release commit and release tags;
- validated workspace outputs needed by `smoo release publish`;
- the collected Linux platform-output tree;
- the generated target/output/checksum manifest.

The Git bundle is required because release versioning creates a local commit and tags before validation and deliberately
does not push them. The final job reconstructs that exact state rather than re-running versioning or checking out an
unvalidated remote commit.

### Stage 1: generic macOS platform producer

Use one pinned macOS runner and execute all matching targets sequentially or through Nx's bounded scheduler:

```sh
smoo github-ci nx-run-many --targets '*-macos,*-ios' --collect-outputs .smoo/platform/macos
```

The job does only:

1. check out the dispatch SHA;
2. run the same `./.github/actions/setup-devenv` used on Linux;
3. expand and execute the macOS target globs;
4. collect and verify the matched targets' declared outputs;
5. upload the platform staging tree and manifest;
6. run `./.github/actions/save-nix-devenv` in `always()`.

`setup-devenv` is already platform-aware. `DeterminateSystems/determinate-nix-action` installs Nix for the macOS runner,
and devenv evaluates `devenv.nix` for the runner's Darwin system. It does not download or reuse Linux binaries on macOS.
The Darwin evaluation selects `pkgs.stdenv.isDarwin` branches, including the system Xcode SDK configuration.

The platform job does not run aggregate `build`, `lint`, `test`, affected calculation, or unrelated package targets.
Only target names matching its configured platform globs may appear in its Nx task graph. Smoo validation should reject
a platform target whose dependency closure escapes that platform family.

For Cowshed, the resolved matches are `napi-arm64-macos` and `napi-x64-macos`; Smoo itself remains unaware of those
names and of their `.node` outputs. The Darwin Rust toolchain closure must include both Apple Rust targets.

The macOS job builds the workflow's dispatch SHA while the parallel Linux job creates a metadata-only release commit.
Release versioning must therefore remain metadata-only. If platform compilation ever depends on generated version state
or embeds the release commit SHA, versioning must become a preceding stage and all producers must build that commit.

For `mode: none` and dry runs, the macOS producer may perform an unnecessary build because it cannot consume the
parallel Linux job's output. That is the deliberate cost of retaining a two-stage graph.

### Stage 2: publish on Linux

When macOS targets exist, the final job has:

```yaml
needs: [linux-release-candidate, macos-platform]
runs-on: ubuntu-latest
```

It:

1. checks out the dispatch SHA;
2. restores the validated local release branch and tags from the Linux Git bundle;
3. runs Linux `setup-devenv` against that release candidate;
4. restores the validated workspace outputs;
5. downloads every platform artifact from its `needs` producers in this workflow run;
6. validates manifests, checksums, source SHA, ownership, and output-path collisions;
7. overlays the workspace-relative platform output trees;
8. runs packed-package validation and inspects the resulting tarballs;
9. runs `smoo release publish` through the existing trusted-publisher/OIDC path.

The final job does not rebuild platform targets. It never downloads “latest successful” artifacts from another run.
Package-specific packed validation may impose stronger requirements; Cowshed requires its four exact `.node` filenames
once each.

Before Cowshed publication is enabled:

1. remove `"private": true` from `packages/cowshed/package.json`;
2. add the repository's `npm:public` Nx tag;
3. add the intended `publishConfig` and release ownership metadata;
4. include Cowshed in the existing `smoo` release-project selection.

## Generic CI target opt-out

Use an Nx project tag, not a package name in the workflow:

```json
{
  "nx": {
    "tags": ["ci:skip:test"]
  }
}
```

`smoo github-ci nx-smart --target <target>` automatically adds:

```text
--exclude=tag:ci:skip:<target>
```

Thus the unchanged generic Unit Tests step excludes every project tagged `ci:skip:test`; build and lint still run. Other
targets can use the same contract, for example `ci:skip:lint`. `smoo monorepo validate` rejects malformed skip tags and
tags naming targets the project does not have.

The release workflow's explicit `nx-run-many` validation does not honor CI skip tags unless requested: a package cannot
silently opt out of release validation. Add `ci:skip:test` to Cowshed while its published native artifact contract is
incomplete, then remove it when the multi-platform publish path becomes required.

## Cache boundaries

Do not use Nx cache as the transport between operating systems. Platform target outputs move through immutable GitHub
Actions artifacts and manifests.

Use each cache for its actual purpose:

| Data                         | Linux producer              | macOS producer              | Cross-OS sharing         |
| ---------------------------- | --------------------------- | --------------------------- | ------------------------ |
| Nix store and devenv closure | Linux restore/save          | Darwin restore/save         | never                    |
| `node_modules`               | Linux restore               | Darwin restore              | never                    |
| `.nx/cache` in ordinary CI   | Linux namespace             | Darwin namespace if used    | never                    |
| `.nx/cache` release outputs  | do not restore              | do not restore              | never                    |
| compiler objects             | platform-specific `sccache` | platform-specific `sccache` | never                    |
| final target outputs         | upload artifact             | upload artifact             | merge in final Linux job |

The current publish workflow intentionally does not restore persisted Nx outputs, so a release is built from source in
that run. Preserve that rule for every platform producer. Ordinary CI may continue to use Nx cache.

The generic cache actions currently key on `runner.os`, which already prevents Linux Nix/devenv and `node_modules`
entries from reaching macOS. Add `runner.arch` before mixing x64 and arm64 runners of the same OS. Update both the
managed templates and their generated copies:

- `packages/cli/managed/templates/github/actions/cache-nix-devenv/action.yml`;
- `packages/cli/managed/templates/github/actions/cache-node-modules/action.yml`;
- `packages/cli/managed/templates/github/actions/cache-nx/action.yml`;
- the generated `.github/actions/` copies;
- the Nx save key emitted by `packages/cli/src/monorepo/ci-workflow.ts`.

Keys use an OS/architecture prefix such as `${{ runner.os }}-${{ runner.arch }}`. Nix derivations and binary
dependencies are restored only into a matching system.

Nix/devenv caching restores the platform's toolchain closure, not Cargo compilation products. Configure the
already-installed `sccache` as Cargo's `RUSTC_WRAPPER` with an OS-, architecture-, compiler-, target-, and lock-aware
namespace. Do not cache Cargo `target/` wholesale.

## Required release gates

Publishing remains disabled until all of these are enforced:

- generic CI excludes Cowshed tests through `ci:skip:test`, without a Cowshed workflow condition;
- the publish generator discovers platform jobs solely from Nx target globs;
- repositories without platform targets retain the current publish workflow behavior;
- the Linux candidate validates the exact local release commit it bundles;
- the macOS producer runs only target/dependency graphs matching the macOS families;
- macOS setup resolves a Darwin Nix/devenv closure, never Linux binaries;
- all four Cowshed target builds succeed from a clean checkout;
- runnable target artifacts pass native loading smoke tests;
- every collected output is declared by its Nx target and covered by the manifest;
- the final Linux job accepts artifacts only from its `needs` producers in the same run;
- output ownership, checksums, source SHA, and collisions are validated before overlay;
- the assembled Cowshed tarball contains all four native binaries exactly once;
- installing that tarball on every supported runtime loads the expected filename without `COWSHED_NODE_PATH`;
- deleting or replacing any required artifact makes assembly fail;
- `ci:skip:test` is removed from Cowshed when the publish path becomes required.
