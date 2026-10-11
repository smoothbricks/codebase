# Nx managed install status

Live task-status wiring gap for developer links to Nx. Public wording is in `packages/cli/README.md` (developer links,
Nx paragraph) and `packages/nx-plugin/managed/raw/tooling/direnv/developer-links.ts` (header).

## Current behaviour of an ordinary local setup invocation (measured in setup-environment.ts)

- `bunInstaller.isCurrent` is true only when the install stamp matches, the TypeScript API package resolves, and
  `hasStaleDeveloperLinks(projectRoot, ['nx'])` is false: no dangling developer link and no live link to `nx`.
- An ordinary local setup invocation runs `bun install` only when `isCurrent` is false. It relinks `nx` through
  `keepDeveloperLinks(..., { relink: ['nx'] })`, then records the stamp.
- An ordinary local setup invocation with current bindings runs no `bun install`. It only reports developer links.

## Gap

- The devenv `smoo:install` task is `execIfModified` over the inputs listed in
  `packages/nx-plugin/managed/raw/tooling/direnv/devenv.smoo.nix` (lines 382-400). Devenv starts no installer for an
  unchanged checkout (same file, lines 349-350). A new external Nx link in the root's or a workspace member's
  `node_modules` changes none of those inputs, so the alias is not repaired until setup runs by some other route.
- `smoo:developer-links` has status `setup --links --check` (`devenv.smoo.nix` lines 411-418). `setup-environment.ts`
  lines 139-144 describe it as covering the packages `smoo.developerLinks` declares. `nx` is refused as a declaration
  (README, Nx paragraph), so this status does not cover an Nx alias.

## Acceptance for the wiring

- An ordinary devenv entry repairs a new external root or workspace-member Nx alias.
- The next entry, with nothing changed, installs nothing.
- A genuine sibling developer link survives that repair.

Not implemented. This note records the requirement only.
