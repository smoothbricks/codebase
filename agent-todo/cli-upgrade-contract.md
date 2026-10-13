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
