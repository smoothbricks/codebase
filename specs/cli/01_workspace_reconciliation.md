# Workspace reconciliation

A repository's managed files, tool dependencies and runtime pins are reconciled by the toolset that renders them.
Reconciliation separates synchronization of the installed toolset from installation of a compatible upgrade.

## Self-hosted shell bootstrap

SmoothBricks' PATH launcher imports the built CLI from `packages/cli/dist`, exactly as the published binary does. It
loads no source transformer and has no source fallback. Shell entry, not each CLI invocation, owns missing or stale
outputs: after the install and developer-link tasks, `tooling/direnv/enter-shell.ts` emits the Nx plugin, CLI and their
required `workspace:*` runtime entries, dependency first and once per package. The cowshed dependency supplies only its
native-free `process-lineage` entry through its independent `tsc --build tsconfig.lineage.json --force` producer, not
its full API or that API's dependencies. The forced build repairs missing public outputs even when incremental
build-info still exists; a current bootstrap marker skips the compiler entirely. Other packages use
`ttsc -p tsconfig.lib.json --emit`. Bootstrap invokes neither Nx nor smoo, so it needs no project graph or pre-existing
CLI build.

The environment-capture pass (`DEVENV_SKIP_TASKS`) builds nothing. The entry proper finishes bootstrap before runtime
pin synchronization can call `smoo monorepo init --runtime-only`. Install, postinstall and developer-link setup run
their raw scripts, not the CLI. The existing `node_modules/.cache/smoo/dist-verified` marker gates emit; absent or empty
`dist`, missing markers or newer source/config files rebuild. The lineage entry specifically requires both its public
`dist/lineage/process-lineage.d.ts` and JavaScript file: a non-empty full-API `dist/ts` cannot stand in for it.

Only an emit of the plugin's dependency closure that changed a member's output bytes invalidates the plugin's daemon
record and project graph. The entry digests each closure member's output tree (paths, kinds, bytes and link text) before
and after its emit, so a source that only gained a newer mtime (a checkout, a same-byte restore, a formatter's identical
rewrite) recompiles and keeps them. A CLI-only rebuild preserves them, and every build preserves Nx task results and
their database.

That invalidation destroys what every Nx run of the checkout in flight reads, a gate's included, so it is a destructive
operation with the holder check in front of it (../cowshed/02_workspaces.md, "Holders cowshed does not run"). Before it
emits the closure, an entry in a checkout with `.cowshed/workspace.json` runs `cowshed tree-holders` over the directory
`.nx/workspace-data` resolves to, exempting the daemon its `d/server-process.json` names, as clearing a checkout's Nx
state does (../cowshed/16_build_volumes.md). Another holder, or a census that cannot be taken, refuses the closure's
emit, marker and invalidation together, and the shell still enters. The refusal also holds back every stale package
whose workspace dependency closure reaches a held member, the CLI among them: it compiles against their dist
(`noEmitOnError`), so a change to both, as a rebase onto a main that changed the plugin and the CLI brings, would fail
its emit and stop the entry. Packages outside that closure rebuild. Stderr names the packages held back and up to ten
holders by their census lines, counting the rest, or the census's failure and its cause. The held packages stay stale,
so the next entry that finds no other holder rebuilds them. An emit kept without its invalidation would recompile to the
same bytes at that entry and never invalidate. A checkout without that record runs no gate and takes no census. The
census is the same from a sandboxed entry: run inside `cowshed exec`, it names a host process holding a file there
(measured 2026-10-30), so a gate on the host refuses a sandboxed entry's rebuild as it refuses a host entry's.

For managed uv projects, install-task status joins the existing input stamps with a live environment-relocation witness
instead of relying only on the task's file-digest cache. Normal project commands inherit `UV_NO_SYNC=1`; the managed
installer's explicit sync is the sole writer of editable mappings and normalizes them before publication
([Python/uv relocation](../cowshed/03_caches.md)). A fork enters installed and imports its own tree.

## Operations

- `smoo monorepo sync` reconciles the installed, pinned toolset. It is deterministic and offline: it resolves nothing
  and installs nothing new.
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

## Declared developer link pins

`smoo.developerLinks` selects the package code a local consumer executes. The stable form is
`{ "directory": "../provider/packages/api", "snapshot": "<64 lowercase hex digits>" }`: the ID of a sealed snapshot, not
a Git ref or a provider directory to materialize. The existing snapshot census and producer provenance pin source,
outputs and dependencies together. Resolving a pin reads the snapshot from the consumer's own `.cache/linked-snapshots`
store; a checkout whose store lacks it imports it from the store of its project's main checkout, the link anchor its
`.cowshed/workspace.json` `projectRoot` names: it copies that sealed snapshot into a temporary entry of its own store,
verifies its census and identity there and renames it into place, then binds it as a local one. A snapshot is
content-addressed, so the import moves only bytes the ID already pins. Resolving verifies that census and identity and
requires the receipt's package name to match the declaration. It never captures, builds, fetches from anywhere but the
main checkout's store, or falls back to a live directory or the installed version.

Capture must not fabricate outputs. A producer receipt records a target's actual successful completion, outputs and
their hashes; a target that cannot prove its outputs refuses rather than inventing an empty output set. The provider's
existing `producerReceipts.targets` list includes `cli:build` and `nx-plugin:build` so their real completions can vouch
for these pins. The existing `lmao:rust-dist`, `lmao:cargo-wasm` and `lmao:cargo-thread-ffi` targets keep the same
production and verification rules.

The directory only locates the provider for an informational HEAD comparison. A changed HEAD reports the captured and
current commits as a stale pin; an absent or unreadable provider reports that freshness cannot be inspected. Both
continue successfully with the same verified snapshot. Shell entry's `smoo:developer-links` status reports freshness
without requesting an install, and postinstall restores the same pin after Bun replaces the alias. Missing, corrupt,
malformed and wrong-package pins refuse with a nonzero result before any alias changes.

The consumer deliberately captures and reviews a new snapshot with the existing snapshot machinery, then changes the
declared ID to move the pin. Moving the provider branch alone never moves it. The snapshot store travels with a cowshed
build volume, and a checkout forked before its main captured a pin imports it from main's store on its first resolve, so
one capture in main serves every checkout of the project. A pin that neither the checkout's store nor its main's holds
refuses, naming both stores; it is not reconstructed from today's provider. Existing manual-link ownership and receipt
rules still apply.

The directory-string form remains an explicit live link. The existing
`{ directory, snapshot: { targets, artifactModules } }` form remains an explicit capture of current, attested provider
bytes on each entry. Both forms keep their existing behavior; neither is a fixed pin. CI still uses lockfile-published
packages and does not apply developer links. Managed-file drift checks remain independent: pinning changes their input,
not which differences they accept.

A live link runs whatever its checkout last built, so linking one whose build does not describe its sources refuses,
names each such build unit and its cached Nx rebuild, and leaves every alias unchanged. Packages resolved from Bun's
store are not judged. A producer writes `.build-inputs.json` into each output directory it owns: the SHA-256 of every
input it read and, where it knows them, the Git pathspecs whose every match is an input. The record is part of the
target's Nx outputs, so a cache hit restores the record that describes the restored outputs. A unit with a record is
current while every recorded input keeps its bytes and no matching source was added; a rewrite with the same bytes
changes nothing. A unit without one is judged by modification time, and its refusal says that this also counts a file
rewritten with its own bytes. Producer receipts above stay the proof for pins: they bind a clean HEAD and live outside
Nx outputs, so neither a dirty working tree nor a cache hit can vouch through them.

The records judged are those beside the outputs the checkout runs. Writers add optional `producer` provenance to the
existing version 1 record from the actual Nx task environment: its project, target and optional configuration. Shell
entry inventories source-owned package `.cache` receipts and published receipts, without following cache symlinks or
entering build-volume `target` or `node_modules` trees. It selects the current package's producer with no configuration
or the manifest target's named `defaultConfiguration`, but never `production`. Declared development CLIs under
`.cache/bin/<platform>` and inferred addons under `.cache/native-dev/<platform>` are therefore covered without a
hand-normalized shadow graph, an output registry, an inferred-target list or starting Nx. A refusal names that exact
producer target and configuration as its cached rebuild: the package's `build` need not reach it. Foreign projects,
other hosts, nondefault/production records and obsolete native records without provenance do not select a unit, and
their surrounding trees are not judged. The TypeScript emit's directory is an independent boundary and still accepts a
standalone bootstrap record without producer provenance; an enclosing obsolete `dist` record cannot hide the nested
emit's record or its missing-record timestamp fallback. Normal published units without records retain their existing
timestamp fallback. These currentness checks run at shell entry, in the `smoo:developer-links` status and again in the
prologue's question below, never per artifact execution.

Shell entry fails closed on that refusal. Once devenv's tasks have run, the managed prologue plans the declared links
again (`setup-environment.ts --links --refusal`) and stops the entry with the planner's reason. The question is asked of
the providers at that entry and no verdict of an earlier entry is kept, so the entry after the provider is rebuilt
succeeds with no step between. The environment-capture pass (`DEVENV_SKIP_TASKS`) runs before the tasks that repair and
asks nothing, and a manifest that declares no links starts no process for the question. Only the planner's refusal stops
entry: a failed install, or a link the tasks could not bring current, still loads the shell so the tools to repair it
stay available.

## Acceptance

The fixtures run from published tarballs, not only workspace source imports, and cover: an old published CLI upgrading a
downstream repository; distinct compatible CLI and plugin versions; multi-version migrations; a linked CLI; dirty local
extensions; a failed installation that resumes; and a second synchronization that changes nothing.
