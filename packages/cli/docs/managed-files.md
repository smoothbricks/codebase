# Managed monorepo files

`@smoothbricks/nx-plugin:managed-files` is the generator used by `smoo monorepo update`, `check`, and `diff`.
The workspace policy registers it as a global sync generator, so `nx sync` and `nx sync:check` use the same implementation.

The plugin owns the templates and workflow renderers. Repository manifests are read from Nx's `Tree`; inferred target
information comes from the resolved Nx graph passed at the boundary. `getProjects(Tree)` is used for workspace manifest
locations, not as a substitute for plugin-inferred targets. No generator installs dependencies or resolves secrets.

The content-ownership and context-derivation functions are pure. Generation stages changes in `Tree`. Updates flush those
changes with Nx; checks inspect them without writing. There is no serialized plan, second change journal, or rollback
system. A failed apply may leave a partial diff. Correct the failure and rerun, or use Git to restore the intended state.
Smoo never resets a working tree automatically.

Repository-owned tails and inline sections retain their existing markers. An orphaned marker or lost/ambiguous anchor is
an error, not permission to discard local content. Files disabled by repository capabilities are left untouched.
Matching in-workspace source symlinks are preserved; dangling, escaping, or drifted links are reported. A small filesystem
boundary inspects links and executable bits because Nx Tree does not expose that metadata. File contents come from Tree.

Tests cover pure content rules, generator output with `createTreeWithEmptyWorkspace()`, and real filesystem boundaries.
The idempotence integration test applies the first result, starts a fresh Tree, and requires the second generation to have
no changes. CLI check, diff, and update use the same staging function. The existing Bun registry-install contracts remain
separate process tests; generator tests never require registry authentication.

The macOS release build and unit-test jobs still use the existing devenv environment. Moving generation into Nx does not
remove release validation or introduce another build environment.
