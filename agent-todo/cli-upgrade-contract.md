# smoo workspace reconciliation: implementation gaps

Governing contract: `specs/cli/01_workspace_reconciliation.md`, “Operations”, “Update resolves one release manifest”,
“Dirty workspaces and resumable installs”, and “Acceptance”. The package's shipped path is described in
`packages/cli/docs/workspace-reconciliation.md`.

## Installed-toolset synchronization

- Spec: `specs/cli/01_workspace_reconciliation.md:8-11` separates offline, deterministic `monorepo sync` from upgrade
  resolution and installation.
- Code: `packages/cli/src/cli.ts:129-132` registers `monorepo update`, which calls `updateManagedFiles`; no separate
  `monorepo sync` is registered.
- Work: expose installed-toolset render-and-apply as `monorepo sync`, with no release resolution, installation of new
  versions or network requests. Name both commands in `packages/cli/README.md`, “Managed Files”.

## One compatible release and new-CLI generation

- Spec: `specs/cli/01_workspace_reconciliation.md:13-25` records one compatible release manifest, installs the selected
  toolset, re-executes its CLI through the resolved path and checks the rendered fixed point. Linked workspace source
  remains linked.
- Code: `packages/cli/src/monorepo/index.ts:115-137` renders first, applies policies and runs `bun install` last. It
  resolves no upgrade manifest and continues in the installed generator.
- Work: define the manifest's publication and implement the specified resolve, stage, install and explicit-path
  re-execution sequence; retain `workspace:` links rather than implicitly replacing source with registry packages.

## Dirty workspaces and resumable installs

- Spec: `specs/cli/01_workspace_reconciliation.md:27-32` defines snapshot/conflict handling, preserves user edits and
  records selected versions before network operations so a failed installation resumes the same release.
- Code: `updateManagedFiles` has no upgrade snapshot/conflict policy or selected-release resume record.
- Work: implement those safeguards around the upgrade, without discarding user edits or re-resolving a failed install.

## Published-package fixtures

- Spec: `specs/cli/01_workspace_reconciliation.md:34-38`, “Acceptance”, requires published-tarball fixtures for old-CLI
  upgrades, distinct compatible CLI/plugin versions, migrations, linked CLI source, dirty extensions, install resume and
  an unchanged second synchronization.
- Code: the CLI has no fixture suite for that upgrade contract.
- Work: add the specified published-package fixtures; source-import tests alone do not satisfy the contract.

## A declared link brings a second instance of the dependencies the linked package shares

- Spec: `specs/cli/01_workspace_reconciliation.md` §Declared developer link pins (`:115`) and `packages/cli/README.md`,
  "Declared Links": a pin holds "source, built outputs and dependencies together". Neither says what happens to a
  dependency both the linked package and its consumer install.
- Code: a developer link replaces the consumer's `node_modules/<package>` with a symlink to the provider checkout
  (`packages/nx-plugin/managed/raw/tooling/direnv/developer-links.ts`). Node, Bun and tsc resolve a module's
  dependencies from its real path, so the linked package brings the provider's copy of each of its dependencies;
  `developer-links.ts` uses `realpathSync` only for confinement, snapshot identity and dangling checks, and nothing
  compares a shared dependency's two real paths.
- Observed with a consumer linked to `lmao`, `arrow-builder`, `time` and `duration`: unpatched registry packages at
  equal versions resolve to one path through Bun's global store; `@uwdata/flechette@2.7.0`, patched byte-identically in
  both repositories and so materialized per checkout, `tslib@2.8.1` and the peer-resolved `typia@15.2.1` resolve to two
  paths; `@smoothbricks/validation` is the provider's workspace package on one side and published 0.1.14 on the other.
  The consumer's typecheck failed with TS2883 (an inferred type that cannot be named without a reference into the
  provider's copy of flechette), and at run time an Arrow table built by lmao was not an instance of the consumer's
  `Table`. The consumer's repair was a hand-written link for flechette to a path inside the provider's install.
- Work: when a declared link is applied, compare the provider's and the consumer's real path for each runtime and peer
  dependency of the linked package. Equal: nothing to do. Same name, version and bytes (compare the package census, as
  the snapshot code does): bind the consumer's entries to the provider's copy with a receipt like any declared link, and
  undo it when the link goes. Different version or bytes: name the dependency and both versions at link time, and state
  in the spec whether that refuses or reports, with the reason. A sealed snapshot link (`snapshot: { targets }`) answers
  the same question for the copies it seals.
- Red first: a fixture consumer and provider that both install a patched dependency at one version resolve it to one
  real path after the link is applied; with different versions the link names the dependency and both versions; removing
  the declaration restores the consumer's own copy.

## A typecheck keeps diagnostics from before a linked package changed

- Code: the inferred `typecheck` target runs tsc with its build-info file (`dist-types/tsconfig.lib.tsbuildinfo`). When
  only the target of a `node_modules` symlink changes, no source file's bytes change, so tsc reuses the diagnostics the
  build info holds.
- Observed: after the link repair above, the consumer's typecheck still reported TS2883 from the failed run, on the same
  tree; with a fresh `--tsBuildInfoFile` it reported nothing.
- Work: a changed module resolution invalidates the stored diagnostics of the files it reaches (the resolved real paths
  are an input of the task and of tsc's reuse), so a link change cannot leave a project red or green on state from
  before it.
- Red first: a project typechecks red against one linked target, the link moves to a target that fixes it, and the next
  typecheck is green without deleting the build info.
