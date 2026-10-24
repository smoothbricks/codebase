# Monorepo tooling: the remaining Bun catalog gaps

A consumer monorepo keeps one root catalog (Bun: `"workspaces": {"packages": [...], "catalog": {"dep": "1.2.3"}}`,
members write `"dep": "catalog:"`) so a dependency pinned identically in many members has one authority. The nx-plugin
policies read the object form of `workspaces` through `workspacePackagePatterns`, and smoo's root tool, Bun-types,
runtime-types and ttsc-patch policies judge and repair the version Bun installs through `dependencyVersion` /
`setDependencyVersion` (catalog resolution in `packages/nx-plugin/src/workspace-manifest.ts`). Specs are silent on
catalogs. The items below remain; each names the code, the evidence and the acceptance. They are independent.

## No check keeps a catalogued dependency single-sourced

- Evidence: sherif 1.13.0 (what `validatePackageHygiene` runs, `monorepo/package-hygiene.ts:43`) reports nothing for a
  member pinning `^3.0.0` beside a root catalog entry `^4.0.0` that other members use through `catalog:`; its `--fix`
  leaves both. `validateWorkspaceDependencies` (`package-policy.ts:700-755`) checks only workspace names.
- Work: when the root defines a catalog, a member that names a catalogued dependency with anything but its catalog
  reference fails `validate` with the member path, field and catalog entry; `--fix` rewrites it to `catalog:`.
- Acceptance: the mismatch above fails `validate` and `--fix` converges it.

## Nx Bun lockfile parser does not resolve `catalog:`

- Code: the patched Nx 23.2.1 `lock-file/bun-parser.js` `processSingleDependency` (patch:
  `packages/nx-plugin/managed/raw/patches/nx@23.2.1.patch`) skips `workspace:` and passes `catalog:` to
  `findResolvedVersion`; `findBestVersionMatch` then falls back to the first candidate. Nx's own `BunCatalogManager`
  (`utils/catalog/bun-manager.js`) reads `workspaces.catalog` but only the pnpm parser and pruning call it.
- Status: not reproduced. With one resolved version per package the edge is right; with several (a nested older copy)
  the external edge may point at the wrong one.
- Work: reproduce with a lockfile holding two versions of a catalogued package; if the edge is wrong, resolve the
  catalog spec through `BunCatalogManager` before `findResolvedVersion` in the patch.
- Acceptance: the project graph's `npm:` edge for a `catalog:` dependency names the catalog's version.

## Packed-manifest guard

- `packReleaseTarball` relies on `bun pm pack`, which resolves `catalog:` from `bun.lock` (probe with Bun 1.4.3: the
  packed manifest carries the catalog range; without a lockfile pack fails loudly). The post-pack guard
  (`release/pack.ts:294`) and `packed-manifest.ts:41-45` match `workspace:`/`link:`/`file:` only.
- Work: add `catalog:` to the unresolved-protocol guard so a packed `catalog:` refuses publication.
- Acceptance: a tarball whose manifest still says `catalog:` fails the guard.
