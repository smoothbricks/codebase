# smoo: `monorepo sync` and a coherent `monorepo update`

`packages/cli/docs/workspace-reconciliation.md` states the upgrade contract: `smoo monorepo sync` reconciles the
installed, pinned toolset offline; `smoo monorepo update` resolves one release manifest, installs the compatible CLI and
plugin pair, re-invokes the newly installed CLI through its resolved path, and only then renders, validates, checks the
fixed point and applies.

## What the code does

- `packages/cli/src/cli.ts:129-132`: `monorepo update` calls `updateManagedFiles`. There is no `monorepo sync` command
  (the only `sync` commands are `monorepo sync-bun-lockfile-versions` at `:167` and `secrets sync` at `:626`).
- `packages/cli/src/monorepo/index.ts:115-136`: `updateManagedFiles` renders the managed files first (`:116`), applies
  the package and tool policies (`:117-133`), and installs last (`:134-135`). The running process stays the old
  generator, and no release manifest is resolved or recorded.

## Work

1. Add `smoo monorepo sync`: today's render-and-apply with the installed toolset, with no resolution and no install of
   new versions. It must make no network request.
2. Define the release manifest (CLI and plugin versions, tested Nx ranges, configuration schema version, template
   version, ordered migrations) and where a release publishes it.
3. Rework `monorepo update`: resolve the manifest once, record the selected versions before any network operation, stage
   and install them, then run the installed CLI's `monorepo sync` through its explicit resolved path, never through
   `PATH`. A failed install resumes from the recorded versions.
4. Keep `workspace:` links in Smoothbricks itself; an update never replaces a linked package with a registry package.
5. Define the dirty-workspace snapshot and conflict policy; an update never discards user edits.
6. Update `packages/cli/README.md` "Managed Files" to name both commands.

## Acceptance

The fixtures in `workspace-reconciliation.md` "Acceptance", run from published tarballs: an old published CLI upgrading
a downstream repository; distinct compatible CLI and plugin versions; multi-version migrations; a linked CLI; dirty
local extensions; a failed installation that resumes with the same versions; and a second `sync` that changes nothing.
