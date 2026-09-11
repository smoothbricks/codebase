# StateBus packed-consumer contract

Run `nx run statebus-core:verify-packages`. The target first builds all six StateBus packages, then runs Bun's real
`pm pack` for each one. It extracts those tarballs into a temporary consumer outside the workspace, verifies public
metadata, JavaScript/declaration exports and rewritten inter-package versions, and compiles a strict consumer against
the extracted declarations. The compiled consumer runs under both Node and Bun with the real QueryClient and memory
navigation adapter. Browser navigation primitives have their separate Chromium gate.

Every StateBus package resolves from its extracted tarball, never workspace source or development exports. Third-party
dependencies are linked from the already-verified frozen repository installation; this is an offline packaging contract,
not a claim of a fresh registry install. No package is published. The tarballs and a checksummed result manifest are
written to `.cache/statebus-packages/`; the temporary consumer is removed even on failure.

The fixture is stored as text because its declaration merging belongs to the temporary consumer, not this core's
source schema or Nx dependency graph. It is copied to `consumer.ts` and fully typechecked during every verification.
The target's explicit dependency list, rather than accidental fixture imports, builds the package set.

`StateBus package contracts` runs the same Nx target on pull requests and main. Its artifact retains the tested tarballs,
result manifest and tracked-source snapshot/revision for reproducible verification. It contains no dependency caches,
runner credentials, `.git`, environment files, or runner home directories.
