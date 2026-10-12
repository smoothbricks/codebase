# Workspace reconciliation

`smoo monorepo update` calls `updateManagedFiles` in the installed CLI. It performs these steps in that process:

1. Render and apply managed files with `applyManagedFiles(root, 'update')`.
2. Apply monorepo, Nx release, public-package and workspace-dependency defaults; apply tool configuration and Cargo
   feature-unification defaults; synchronize root runtime pins and Bun lockfile versions.
3. Run `bun install --no-summary` through `keepDeveloperLinks`. Nx is relinked to the lockfile-selected package; other
   developer links are preserved.

The command renders before installation. It does not resolve a release manifest or re-execute the newly installed CLI,
and there is no separate `smoo monorepo sync` command.

The [workspace reconciliation specification](../../../specs/cli/01_workspace_reconciliation.md) defines synchronization
and coherent upgrades. The [implementation gap list](../../../agent-todo/cli-upgrade-contract.md) records the work
between this path and that contract.
