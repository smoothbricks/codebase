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

## Nx target shape

Keep `cargo-napi` as the local host build. Add explicit release configurations for:

- `darwin-arm64`
- `darwin-x64`
- `linux-arm64-gnu`
- `linux-x64-gnu`

Each configuration passes `--target <rust-target>` directly to `napi build` and writes to its own staging directory, for
example `native-artifacts/darwin-arm64/`. The release jobs never let two target configurations write to the same output
directory. The Linux publisher copies the verified files into `packages/cowshed/dist/` only after every producer has
completed.

`cargo-napi` has no `dependsOn`, so a native producer can run:

```sh
nx run cowshed:cargo-napi:darwin-arm64
nx run cowshed:cargo-napi:darwin-x64
```

without running `build`, `lint`, `test`, or any other workspace target.

The target inputs cover Rust sources, Cargo manifests and lock/config files, `package.json`, `bun.lock`, and the
devenv/Rust toolchain inputs. The explicit configuration and command are part of the Nx task hash. Release outputs are
still transferred between jobs as workflow artifacts, not by restoring another runner's Nx cache.

## GitHub Actions design

`.github/workflows/ci.yml` remains the generic Linux CI template. It gets no Cowshed-specific job, condition, or
exclusion. The normal Linux Cowshed build and test already load the Linux platform artifact and should continue to run
there.

The native fan-out belongs only to the generated publish workflow. Add a generic optional release-artifact producer
facility to the publish workflow generator; repositories without configured producers retain the current single-job
workflow. This repository configures one macOS producer for Cowshed. Do not add a Cowshed branch to the generic CI
workflow generator.

### Two-stage publish graph

The publish run has this job graph:

```text
linux-release-candidate ─┐
                         ├──> publish-on-linux
macos-native ────────────┘
```

`linux-release-candidate` and `macos-native` run in parallel. `publish-on-linux` is the only job that combines their
outputs and is the only job allowed to publish.

This is a two-stage graph even though it contains three jobs: two parallel producers, followed by one consumer.

### Stage 1: Linux release candidate

The Linux producer owns the existing publish workflow through validation:

1. check out the dispatch SHA;
2. set up devenv;
3. build the self-hosted `smoo`;
4. repair pending releases;
5. create the local release commit and tags;
6. run the selected projects' build, lint, test, and monorepo validation;
7. build or collect the Linux x64 glibc artifact;
8. build Linux arm64 glibc with its explicit target configuration when arm64 is part of the release contract.

It then uploads immutable artifacts for this workflow run:

- a Git bundle containing the validated local release commit and release tags;
- the validated workspace build outputs needed by `smoo release publish`;
- `cowshed.linux-x64-gnu.node`;
- `cowshed.linux-arm64-gnu.node`;
- a manifest containing filenames, target triples, source SHA, sizes, and SHA-256 checksums.

The Git bundle is required because release versioning intentionally creates a local commit and tags before validation
and does not push them. The final Linux job reconstructs that exact validated release state rather than re-running
versioning or checking out an unvalidated remote commit.

### Stage 1: macOS native producer

Use one pinned macOS runner and build both Apple targets sequentially. A single job avoids paying for devenv setup
twice, avoids concurrent cache-save races, and lets Xcode cross-compile between Apple arm64 and x64.

The macOS job does only:

1. check out the dispatch SHA;
2. run `./.github/actions/setup-devenv`;
3. run `nx run cowshed:cargo-napi:darwin-arm64`;
4. run `nx run cowshed:cargo-napi:darwin-x64`;
5. verify the two exact filenames, Mach-O architectures, dynamic libraries, sizes, and SHA-256 checksums;
6. smoke-test the artifact matching the runner architecture;
7. upload `cowshed-native-macos` containing the two `.node` files and their manifest;
8. run `./.github/actions/save-nix-devenv` in `always()`.

It does not run Nx `build`, `lint`, `test`, affected calculation, or any workspace-wide target. It does not restore or
save `.nx/cache` during a release; both native binaries are release outputs and are rebuilt from source.

The Rust toolchain in `tooling/direnv/devenv.nix` must include `aarch64-apple-darwin` and `x86_64-apple-darwin` on
Darwin. Use the system Xcode SDK already selected by `apple.sdk = null`. Do not compile Apple binaries on Linux.

The macOS job builds the workflow's dispatch SHA while the parallel Linux job creates a metadata-only release commit.
The release implementation must enforce that versioning changes only release metadata. If native compilation ever
depends on the generated release commit or embeds its SHA, this two-stage graph is no longer valid; versioning must
become a preceding stage and both producers must build that commit.

For `mode: none` and dry runs, the macOS producer may perform an unnecessary build because it cannot consume the
parallel Linux job's version output. That is the deliberate cost of retaining a two-stage graph.

### Stage 2: publish on Linux

The final job has:

```yaml
needs: [linux-release-candidate, macos-native]
runs-on: ubuntu-latest
```

It performs these steps:

1. check out the dispatch SHA;
2. download the Linux candidate Git bundle and restore its local release branch and tags;
3. set up devenv from the restored release candidate;
4. restore the validated workspace build outputs;
5. download the Linux and macOS native artifacts from this workflow run;
6. verify both manifests, all checksums, the source SHA, and the exact required filename set;
7. copy the four files into a clean `packages/cowshed/dist/`;
8. run packed-package validation and inspect the resulting tarball;
9. run `smoo release publish` through the existing trusted-publisher/OIDC path.

The final job does not run `nx build cowshed`: doing so would create another host binary and contaminate the assembled
release. It consumes only artifacts named by its `needs` jobs in the same workflow run; it never downloads “latest
successful” output from another run.

Before publication is enabled:

1. remove `"private": true` from `packages/cowshed/package.json`;
2. add the repository's `npm:public` Nx tag;
3. add the intended `publishConfig` and release ownership metadata;
4. include Cowshed in the existing `smoo` release-project selection.

## Generic CI remains unchanged

Do not add `--exclude cowshed` to `ci.yml` or its generator. Cowshed's Linux native build and three Bun integration
tests pass on the ordinary Linux path, so there is no remaining reason to skip the project. Multi-platform assembly is a
release concern, not a reason to weaken the generic validation template.

## Cache boundaries

Do not share Nx caches between Linux and macOS. A native task's output is platform-specific, and the current publish
workflow intentionally does not restore persisted Nx outputs so a release is built from the versioned source in that
run. Preserve that release-integrity rule for both producer jobs.

Use each cache for its actual purpose:

| Data                         | Linux producer  | macOS producer  | Cross-OS sharing                |
| ---------------------------- | --------------- | --------------- | ------------------------------- |
| Nix store and devenv closure | restore/save    | restore/save    | never                           |
| `node_modules`               | restore         | restore         | never                           |
| `.nx/cache` release outputs  | do not restore  | do not restore  | never                           |
| Cargo compiler objects       | `sccache`       | `sccache`       | never                           |
| final `.node` files          | upload artifact | upload artifact | combine only in final Linux job |

The generic cache actions currently key only on `runner.os`. Before adding an arm64 runner, update both their managed
templates and generated copies to include `runner.arch`:

- `packages/cli/managed/templates/github/actions/cache-nix-devenv/action.yml`;
- `packages/cli/managed/templates/github/actions/cache-node-modules/action.yml`;
- `packages/cli/managed/templates/github/actions/cache-nx/action.yml`;
- the generated `.github/actions/` copies;
- the Nx save key emitted by `packages/cli/src/monorepo/ci-workflow.ts`.

Keys use an OS/architecture prefix such as `${{ runner.os }}-${{ runner.arch }}`. An x64 Nix profile or Bun install must
never be restored onto an arm64 runner merely because both report `macOS`.

Nix/devenv caching restores the toolchain closure, not Cargo compilation products. Configure the already-installed
`sccache` as Cargo's `RUSTC_WRAPPER` and give its persistent cache an OS-, architecture-, Rust-toolchain-, target-, and
Cargo-lock-aware namespace. Do not cache Cargo `target/` wholesale.

For ordinary non-release CI, Linux may continue restoring and saving its Linux Nx cache. If a separate non-release macOS
preflight is added later, it gets an independent macOS/architecture Nx cache. Neither cache is used to transfer native
binaries to publishing; `actions/upload-artifact` and `actions/download-artifact` are the transfer mechanism.

## Required release gates

Publishing remains disabled until all of these are enforced:

- the generic Linux CI workflow remains unchanged and green;
- the Linux candidate validates the exact local release commit it bundles;
- the macOS job runs only the two explicit native Nx configurations;
- all four target builds succeed from a clean checkout;
- runnable target artifacts pass a native loading smoke test;
- every artifact's format, architecture, dynamic libraries, size, source SHA, and checksum are verified;
- the final Linux job accepts artifacts only from its two `needs` producers in the same run;
- the assembled tarball contains all four binaries exactly once;
- the tarball's JavaScript and declarations pass packed-package validation;
- installing the tarball on macOS arm64, macOS x64, Linux arm64 glibc, and Linux x64 glibc loads the expected filename
  without `COWSHED_NODE_PATH`;
- deleting or replacing any one artifact makes assembly fail.
