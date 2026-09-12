# StateBus packed-consumer contract

Run `nx run statebus-core:verify-packages`. The target first builds all six StateBus packages plus LMAO/codec dependencies, then runs Bun's real
`pm pack` for each one. It inspects their public metadata, JavaScript/declaration exports and rewritten inter-package
versions, then installs the unmodified tarballs into two temporary consumers outside the workspace: Bun's isolated
and hoisted linkers. Each consumer passes strict declaration checking with `skipLibCheck: false` and runs under both
Node and Bun with the real QueryClient and memory navigation adapter. Browser primitives have a separate Chromium gate.

Every StateBus entry must resolve inside its consumer's `node_modules`, have the expected version and match the actual
packed entry bytes. No workspace-source links, development exports or TypeScript path mappings are used. The disposable
consumer's overrides map unpublished internal prereleases transitively to those same tarballs; packed semver edges are
checked before that mapping. These overrides are verification-only, not changes to published package dependencies.

Direct third-party versions come from the repository's frozen install, but Bun constructs fresh consumer dependency
graphs. The check requires a populated package cache or registry access. Each graph is reinstalled with a frozen
consumer lockfile. Consumer installs disable global-store links and lifecycle scripts. Nothing is published, merged,
or installed into the user's project. The temporary consumers are removed even on failure.

The tarballs, consumer lockfiles and checksummed result manifest are written to `.cache/statebus-packages/`. A stale
success manifest is removed before a rerun; the new manifest is written only after both consumers pass.

## Consumer project

`consumer/` is an ordinary TypeScript project, not text templates. `platform/` covers the six-package public exports,
legacy declaration merging and negative platform contracts. `composed/` contains the inventory library, generated
codecs, integration scenarios, failure/replay cases and compile-negative API contracts. Each has a checked-in
`tsconfig.json`; neither inherits the workspace's source aliases or development export conditions.

The verifier copies the authored project unchanged to each temporary installation, then compiles and runs it against
the installed package exports. Local `dist`, `node_modules` and `.cache` directories are excluded from copying.
The nested `statebus-consumer` Nx project owns these imports so they cannot introduce a dependency from core back
to React or the loader packages. `nx lint statebus-consumer` checks the real TypeScript with Biome;
`nx test statebus-consumer` delegates to the same built-package verification target. Both programs use strict
public-declaration checks with unused-local checks and `noEmitOnError`. See `../CONSUMER.md` for the API contract.

`StateBus package contracts` runs this target on pull requests and main. It also runs the exact-interest Mitata target
under Node/V8 and Bun in both orders, checking semantic equivalence and retaining individual raw samples. These are
phase diagnostics, not a measured zero-allocation or browser-frame-time certificate; unavailable instrumentation is
explicitly recorded. See `../benchmarks/EXACT-INTEREST.md` for the measurement boundaries.

The CI artifact retains tarballs, consumer lockfiles, result metadata, benchmark samples, and tracked source/revision.
It contains no dependency caches, credentials, `.git`, environment files, or runner home directories.

## Platform ambient contracts

All six production tsconfigs use `types: []`. Core, data-loader, navigation-core,
and the TanStack bridge select `ES2022` plus `WebWorker` for standard host APIs
(`AbortSignal`, timers, microtasks and console), without declaring Node/Bun or
DOM-only `Window`/`Document` globals. This selects declarations only; it does not
require a worker runtime. The React and browser adapters explicitly select DOM
libraries instead. Test-runner types remain confined to test tsconfigs.

The package check asserts the empty production ambient-type allowlist. Both
isolated and hoisted packed consumers also compile negative contracts for
`Buffer`, `process`, `Bun`, `require` and `__dirname` with the real transitive
exports. Reintroducing those globals fails validation rather than silently
weakening platform neutrality. Declaration checking remains strict.

The separate composed scenario program explicitly enables Node test-harness types
for its assertions and execution adapters. It does not share a TypeScript program
with the platform-neutral consumer, so it cannot mask those negative contracts.
