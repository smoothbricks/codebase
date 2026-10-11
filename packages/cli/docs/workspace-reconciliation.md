# Workspace reconciliation: one upgrade contract, two operations

A repository's managed files, tool dependencies and runtime pins are reconciled by the toolset that renders them. A
process that renders managed files and only then installs a newer CLI has not upgraded anything coherently: the files
came from the old generator, the plugin follows its own dependency policy, and runtime synchronization may read a
profile realized before the Nix edits. So reconciliation is two operations with one contract.

- `smoo monorepo sync` reconciles the currently installed, pinned toolset. It is deterministic and offline: it resolves
  nothing and installs nothing new.
- `smoo monorepo update` resolves an upgrade, installs the compatible toolset, and then synchronizes using that new
  toolset.

## Update resolves one release manifest

An update resolves a release manifest once. The manifest records the compatible CLI and plugin versions, the tested Nx
ranges, the configuration schema version, the template version, and the ordered migrations. The CLI and the plugin may
have different package versions; compatibility is the pair, never an assumption that their semvers match.

The update stages the selected package and toolchain changes, installs them, and invokes the newly installed CLI through
its explicit resolved path. Generation never continues in the old process, and never rediscovers an older global binary
through `PATH`. The new CLI reads the graph with the new plugin, renders, validates, and checks the fixed point before
it applies the reviewed workspace changes.

Smoothbricks itself keeps `workspace:` source development: an update never replaces linked source development with
registry packages implicitly.

## Dirty workspaces and resumable installs

A staging worktree is one mechanism, never the only one: a dirty downstream workspace has a defined snapshot and
conflict policy, and an update never discards user edits. The update records the exact selected versions before any
network operation, and a failed install resumes with those versions rather than resolving a different release halfway
through.

## Acceptance

The fixtures run from published tarballs, not only workspace source imports, and cover: an old published CLI upgrading a
downstream repository; distinct compatible CLI and plugin versions; multi-version migrations; a linked CLI; dirty local
extensions; a failed installation that resumes; and a second synchronization that changes nothing.
