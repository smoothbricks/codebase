# StateBus packed-consumer contract

Run `nx run statebus-core:verify-packages`. The target first builds all six StateBus packages, then runs Bun's real
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
success manifest is removed before a rerun; the new manifest is written only after both consumers pass. The fixture
is stored as text because its declaration merging belongs to the consumer, not this core's source schema or Nx graph.
It is copied to `consumer.ts` and fully typechecked on every run. Explicit Nx dependencies build the package set.

`StateBus package contracts` runs this target on pull requests and main. It also runs the exact-interest Mitata target
under Node/V8 and Bun in both orders, checking semantic equivalence and retaining individual raw samples. These are
phase diagnostics, not a measured zero-allocation or browser-frame-time certificate; unavailable instrumentation is
explicitly recorded. See `../benchmarks/EXACT-INTEREST.md` for the measurement boundaries.

The CI artifact retains tarballs, consumer lockfiles, result metadata, benchmark samples, and tracked source/revision.
It contains no dependency caches, credentials, `.git`, environment files, or runner home directories.
