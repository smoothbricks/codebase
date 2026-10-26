# Workspace reconciliation

A repository's managed files, tool dependencies and runtime pins are reconciled by the toolset that renders them.
Reconciliation separates synchronization of the installed toolset from installation of a compatible upgrade.

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
`{ "directory": "../provider/packages/api", "snapshot": "<64 lowercase hex digits>" }`: the ID of a sealed snapshot in
the consumer's `.cache/linked-snapshots` store, not a Git ref or a provider directory to materialize. The existing
snapshot census and producer provenance pin source, outputs and dependencies together. Resolving a pin verifies that
census and identity and requires the receipt's package name to match the declaration. It never captures, builds,
fetches, or falls back to a live directory or the installed version.

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
build volume; a new consumer needs the named sealed snapshot in its own store. It does not reconstruct a missing pin
from today's provider. Existing manual-link ownership and receipt rules still apply.

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
timestamp fallback. These currentness checks run once at shell entry, never per artifact execution.

## Acceptance

The fixtures run from published tarballs, not only workspace source imports, and cover: an old published CLI upgrading a
downstream repository; distinct compatible CLI and plugin versions; multi-version migrations; a linked CLI; dirty local
extensions; a failed installation that resumes; and a second synchronization that changes nothing.
