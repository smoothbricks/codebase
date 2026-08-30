# cowshed-core/metadata+repository+checkout

Scope: `packages/cowshed/crates/cowshed-core/src/metadata.rs` (1824),
`packages/cowshed/crates/cowshed-core/src/repository.rs` (1233), `packages/cowshed/crates/cowshed-core/src/checkout.rs`
(484). Doctrine: BYPRODUCT-ENGINEERING.md, PERFORMANCE-HANDBOOK §4.1 / §7.7 / §7.10bb, handbook `04-mechanisms.md` +
`05-memory-toolkit.md`. Targeted greps: version constants, port-block constants, `repository.json` /
`checkout-layout.json`, RepoId validators, uuid/serde_json, TS `workspaceIncarnation`.

## Summary

- Two readers of `checkout-layout.json` disagree on absence: `load_checkout_layout` writes `DirectMount`;
  `StorageLayout::checkout_layout` infers (and swallows non-NotFound read errors). Live divergence.
- `MetadataError::InvalidPath` Display always says "has no file name"; every production construction is "path is not
  absolute". The error is a lie.
- `RepoId` grammar is SSOT in `repository.rs` and restated in cowshed-gateway; `config.rs` / `sim_broker.rs` already
  accept uppercase / `_`-leading components that `RepoId::parse` rejects.
- On-disk JSON has one I/O pair (`read_json`/`write_json`) but four record kinds share `METADATA_VERSION` and
  `repository.json` has a second `BINDING_VERSION` (both `1`). GrantSet is flattened on write and hand-restated on read.
- `uuid` is unused in this slice. `WorkspaceIncarnation` is 32 lowercase hex (nonce), not a UUID type and not a content
  hash. `serde_json` is load-bearing; do not shell it out.
- Port-block range `40960–49151` / size `16` is defined here and restated in `cowshed-gateway` `config.rs`;
  `PortBlock::new` does not apply the range.
- Project path filenames: only `repository.json` is a named constant; `checkout-layout.json` and siblings are string
  literals.
- Copies in this slice are once-per-open/parse, not a hot loop. Do not inflate them.

## Findings

### F1 — HIGH — SSOT — Two checkout-layout readers, two absence policies

Evidence: `packages/cowshed/crates/cowshed-core/src/checkout.rs:198-210`

```
pub fn load_checkout_layout(path: &Path) -> Result<CheckoutLayout, MetadataError> {
    match read_json::<CheckoutLayoutRecord>(path) {
        Ok(record) => {
            record.validate()?;
            Ok(record.checkout_layout)
        }
        Err(MetadataError::Io { source, .. }) if source.kind() == io::ErrorKind::NotFound => {
            let layout = CheckoutLayout::DirectMount;
            write_json(path, &CheckoutLayoutRecord::new(layout))?;
            Ok(layout)
        }
        Err(error) => Err(error),
    }
}
```

Other reader (targeted, not owned): `packages/cowshed/crates/cowshed-core/src/storage/mod.rs:294-312` —
`if let Ok(record) = read_json::<CheckoutLayoutRecord>(…)` then infer Symlink from `mnt/…/main` existing, else error if
a main image exists, else default DirectMount, and **never write**. Any `read_json` failure other than success
(including `Json`) falls through to inference. Call site in this crate:
`packages/cowshed/crates/cowshed-core/src/gateway_inventory.rs:1196` calls `load_checkout_layout` during binding
discovery, so a missing record is published as DirectMount before the inferring reader can run. Problem: one durable
fact, two functions, already-disagreeing absence semantics. A symlink-layout project whose `checkout-layout.json` is
missing (deleted, never written, mid-adopt crash) is stamped DirectMount by inventory and later believed.
`StorageLayout::checkout_layout` would have inferred Symlink if the mount dir exists, or failed closed if a main image
exists. Fix: delete `load_checkout_layout`'s write-on-read. One function owns the record: fail closed on malformed JSON;
on NotFound either (a) infer exactly as `StorageLayout::checkout_layout` does and then `write_json` that answer, or (b)
require the record and stop inferring. Prefer (a) with a single function in `metadata`/`checkout`, called by both
inventory and `StorageLayout`. Cost/Risk: `gateway_inventory.rs` and `storage/mod.rs` must share the one path; the
legacy DirectMount materialize test in `checkout.rs:435-447` must be rewritten against the chosen policy.

### F2 — HIGH — STRUCTURE — `InvalidPath` Display is false on every path that produces it

Evidence: `packages/cowshed/crates/cowshed-core/src/metadata.rs:54` and `:124-126`

```
    InvalidPath(PathBuf),
…
            Self::InvalidPath(path) => {
                write!(f, "metadata path has no file name: {}", path.display())
            }
```

Constructions: `packages/cowshed/crates/cowshed-core/src/checkout.rs:47-48` and `:116-117`

```
        if !project_root.is_absolute() {
            return Err(MetadataError::InvalidPath(project_root.to_owned()));
        }
```

`packages/cowshed/crates/cowshed-core/src/metadata.rs:1064-1065`

```
            if !info.project_root.is_absolute() {
                return Err(MetadataError::InvalidPath(info.project_root.clone()));
```

No callsite constructs `InvalidPath` because a path lacks a file name. The test at `checkout.rs:365-372` only matches
the variant, so the lie cannot go red (§7.10bb). Problem: operators (and `error.to_string()` bridges) are told the path
has no file name when the fact is "not absolute". Cantrill: the system does not tell the truth about itself. Fix: split
or rename. `InvalidPath { path, reason: &'static str }` matching `SandboxError::InvalidPath` /
`HostConfigError::InvalidPath` already in this crate, with `reason: "path is not absolute"`. Delete the "no file name"
sentence. Pin the reason in the checkout relative-root test. Cost/Risk: Display string is a contract if anything matches
the current text; grep showed no such matcher. Variant shape change hits every `matches!(… InvalidPath(_))`.

### F3 — HIGH — SSOT — `RepoId` grammar restated in gateway; copies already disagree

Evidence (SSOT): `packages/cowshed/crates/cowshed-core/src/repository.rs:107-131`

```
fn validate_identity_component(value: &str, component: RepoIdComponent) -> Result<(), RepoIdError> {
    …
    if !first.is_ascii_lowercase() && !first.is_ascii_digit() {
        return Err(RepoIdError::InvalidComponent { component });
    }
    if !value
        .as_bytes()
        .iter()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"._-".contains(byte))
```

Diverged copy: `packages/cowshed/crates/cowshed-gateway/src/config.rs:221-228`

```
fn validate_repo_id(value: &str) -> Result<(), ConfigError> {
    let (owner, name) = value.split_once('/').ok_or(ConfigError::InvalidRepoId)?;
    if name.contains('/') || matches!(owner, "." | "..") || matches!(name, "." | "..") {
        return Err(ConfigError::InvalidRepoId);
    }
    validate_identifier("repo_id", owner).map_err(|_| ConfigError::InvalidRepoId)?;
    validate_identifier("repo_id", name).map_err(|_| ConfigError::InvalidRepoId)?;
```

`validate_identifier` at `config.rs:210-215` allows `is_ascii_alphanumeric` (uppercase) and a first byte of `_` / `-` /
`.`. Core rejects `Acme/widget` and `_acme/widget` (`repository.rs:823-837`). Closer restatement (still a second copy):
`packages/cowshed/crates/cowshed-gateway/src/repo_mirror.rs:1300-1323` (comment admits the mirror). Third copy:
`packages/cowshed/crates/cowshed-gateway/src/sim_broker.rs:857-865` (same uppercase hole as config). Problem: live
admission mismatch. Gateway can bind a session whose `repo_id` core will not parse. The comment in `repo_mirror.rs`
already names the failure mode. Fix: `RepoId` stays the single source. Gateway must call `RepoId::parse` (or a
`cowshed-core` identity crate the gateway is allowed to depend on). Delete the three hand copies. If the
gateway-below-core constraint is load-bearing, extract `cowshed-identity` with `RepoId` only — do not keep a comment
promising the copies stay identical. Cost/Risk: gateway crate graph; every `validate_repo_id` callsite. Uppercase IDs
currently accepted by gateway become errors (correct).

### F4 — MEDIUM — SSOT — macOS port-block range defined twice; `PortBlock` ignores it

Evidence: `packages/cowshed/crates/cowshed-core/src/metadata.rs:13-16,813-818`

```
pub const PORT_BLOCK_SIZE: u16 = 16;
pub const MACOS_PORT_BLOCK_MIN: u16 = 40_960;
pub const MACOS_PORT_BLOCK_MAX: u16 = 49_151;
pub const MACOS_PORT_BLOCK_LAST_BASE: u16 = MACOS_PORT_BLOCK_MAX - PORT_BLOCK_SIZE + 1;
…
    pub fn new(base: u16, size: u16) -> Result<Self, MetadataError> {
        if size == PORT_BLOCK_SIZE && base.checked_add(size - 1).is_some() {
            Ok(Self { base, size })
```

`GrantSet::validate(Platform::Macos)` at `:922-929` only calls `block.validate()`, so base `0` is a valid macOS grant.
Tests pin that: `metadata.rs:1667-1693` (`PortBlock::new(0, PORT_BLOCK_SIZE)` then
`macos.validate(Platform::Macos).unwrap()`). Restatement: `packages/cowshed/crates/cowshed-gateway/src/config.rs:20-22`
(`MACOS_PORT_MIN`/`MAX`/`MACOS_PORT_BLOCK_SIZE`) and the error text at `config.rs:638`
(`"macOS gateway base must reserve 16 ports within 40960-49151"`). Problem: the range constants in this file are unused
by the type they sit next to. Allocator code in `runtime/project.rs` uses them; persisted metadata and gateway config do
not share a check. A sidecar with `portBlock.base = 0` is valid here and invalid there. Fix: `PortBlock::new` stays
geometry-only (size 16, no overflow). `GrantSet::validate(Platform::Macos)` requires `base` in
`MACOS_PORT_BLOCK_MIN..=MACOS_PORT_BLOCK_LAST_BASE` and aligned. Gateway imports those constants (or a
`PortBlock::macos(base)` constructor) and deletes its copies. Update the base-0 test: it is a Linux-shaped block, not a
macOS one. Cost/Risk: any fixture that persisted `base: 0` on macOS (the metadata unit test does). Gateway config tests.

### F5 — MEDIUM — SSOT — `DetachedWorkspaceMetadata` serialize/deserialize are two schemas

Evidence: serialize side `packages/cowshed/crates/cowshed-core/src/metadata.rs:975-990`

```
pub struct DetachedWorkspaceMetadata {
    …
    #[serde(flatten)]
    pub grants: GrantSet,
    …
}
```

Deserialize wire `packages/cowshed/crates/cowshed-core/src/metadata.rs:992-1018` restates every `GrantSet` field
(`revision`, `port_block`, `read`, `write`, `egress`, `repos`, `sim`) plus the outer keys, then rebuilds `GrantSet` at
`:1036-1044`. `GrantSet` itself is `packages/cowshed/crates/cowshed-core/src/metadata.rs:892-908`. Problem: write path
is `flatten GrantSet`; read path is a hand-maintained field list behind `deny_unknown_fields`. Adding a grant field
updates `GrantSet` + Serialize automatically and then fails Deserialize (unknown field) or, if someone removes
`deny_unknown_fields`, silently drops it. The frozen-spelling tests catch a miss only if they are updated in the same
change. Fix: deserialize with the same struct. Put `#[serde(flatten)] grants: GrantSet` on one type; keep
`deny_unknown_fields` on `GrantSet` and the outer struct (serde flatten + deny_unknown_fields needs a check — if that
combination is illegal, deserialize `GrantSet` as a internally-tagged subtree, not a second field list).
`publication_state` default stays on the one type (`#[serde(default = "active_publication_state")]`). Cost/Risk: wire
tests `detached_metadata_round_trip_preserves_frozen_spelling` and
`legacy_v1_sidecar_without_publication_state_reopens_as_active` are the oracle; they must stay byte-equal.

### F6 — MEDIUM — SSOT — Four JSON kinds share `METADATA_VERSION`; binding has a second `1`

Evidence: `packages/cowshed/crates/cowshed-core/src/metadata.rs:12`

```
pub const METADATA_VERSION: u32 = 1;
```

Used as the version field of `CheckoutLayoutRecord` (`:469,475-479`), `SlotBindingsRecord` (`:565,571-575`),
`WorkspaceMarker` (`:757-761`), `DetachedWorkspaceMetadata` (`:1056-1060`). Independent:
`packages/cowshed/crates/cowshed-core/src/repository.rs:10,400-401`

```
const BINDING_VERSION: u32 = 1;
…
        if self.version != BINDING_VERSION {
```

Problem: bumping the sidecar schema to 2 also invalidates every `checkout-layout.json` and `slot-bindings.json` that
still say `1`, even if those records did not change. `repository.json` already proved these are different documents (its
own constant). Having two constants both equal to `1` is fine; sharing one constant across unrelated documents is not.
Fix: `MARKER_VERSION`, `SIDECAR_VERSION`, `CHECKOUT_LAYOUT_VERSION`, `SLOT_BINDINGS_VERSION` (or a per-type associated
const). Keep `BINDING_VERSION`. Today they all stay `1`; the names are the SSOT. Cost/Risk: every
`version: METADATA_VERSION` construction and the proptest that rejects `version != METADATA_VERSION` for marker+sidecar
only (`metadata.rs:1448-1462`) — that proptest already does not cover layout/slot records.

### F7 — MEDIUM — SSOT — Project path filenames are one constant and five literals

Evidence: `packages/cowshed/crates/cowshed-core/src/repository.rs:13,697-704`

```
pub const REPOSITORY_BINDING_FILE: &str = "repository.json";
…
            repository_binding: checked_join(&project_root, [REPOSITORY_BINDING_FILE])?,
            checkout_layout: checked_join(&project_root, ["checkout-layout.json"])?,
            slot_bindings: checked_join(&project_root, ["slot-bindings.json"])?,
            policy: checked_join(&project_root, ["policy.json"])?,
            sessions: checked_join(&project_root, ["sessions"])?,
            checkpoints: checked_join(&project_root, ["checkpoints"])?,
            quarantine: checked_join(&project_root, ["quarantine"])?,
            waivers: checked_join(&project_root, ["waivers.json"])?,
```

`checkout.rs:10` restates the layout filename in a comment; tests join `"checkout-layout.json"` again
(`checkout.rs:437,452,470`). Problem: `REPOSITORY_BINDING_FILE` exists because discovery keys on the name. The other
names are the same kind of fact. A second spelling of `checkout-layout.json` is how F1's two readers already drifted.
Fix: named `pub const`s next to `REPOSITORY_BINDING_FILE`; `ProjectPaths` and tests use only those.
`load_checkout_layout` takes `ProjectPaths` (or the const), not a free `Path`. Cost/Risk: path-construction tests in
`repository.rs:1131-1149` pin the joined strings; update them to the constants.

### F8 — MEDIUM — STRUCTURE — Layout/slot records skip validate-on-deserialize

Evidence: `CheckoutLayoutRecord` derives Deserialize with no validate
(`packages/cowshed/crates/cowshed-core/src/metadata.rs:459-464`). `SlotBindingsRecord` same (`:555-560`). Callers must
remember `record.validate()` / `into_bindings()`. Contrast: `WorkspaceMarker` (`:730-751`), `DetachedWorkspaceMetadata`
(`:1021-1050`), `RepositoryBinding` (`:617-638`) all validate inside `Deserialize`. `load_checkout_layout` does call
`validate` (`checkout.rs:200-202`). A `read_json::<CheckoutLayoutRecord>` that forgets it accepts `version: 2`. Problem:
two patterns for the same invariant ("unsupported version is not a value"). The derived path makes an illegal version
representable after load. Fix: custom `Deserialize` on `CheckoutLayoutRecord` and `SlotBindingsRecord` that calls the
existing `validate`/`into_bindings`. Then `load_checkout_layout` can drop the extra `validate()` (keep the NotFound
branch). Cost/Risk: any test that deserializes a bad version as `Ok` then checks `validate()` Err must move the err to
`from_value`.

### F9 — LOW — STRUCTURE — `BoundIdentity` Deserialize is a field-for-field copy

Evidence: `packages/cowshed/crates/cowshed-core/src/repository.rs:370-377` (the type) and `:641-662`

```
impl<'de> Deserialize<'de> for BoundIdentity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct RawIdentity {
            repo_id: RepoId,
            remote_name: Option<String>,
            remote_url: Option<String>,
            primary: bool,
        }
        let raw = RawIdentity::deserialize(deserializer)?;
        Ok(Self { repo_id: raw.repo_id, remote_name: raw.remote_name, remote_url: raw.remote_url, primary: raw.primary })
    }
}
```

No validation, no default, no deny_unknown_fields (the outer `RepositoryBinding` raw type also lacks
`deny_unknown_fields` — `repository.rs:622-629`). Problem: weightless code. Unknown fields on a binding identity are
accepted; marker/sidecar deny them. Fix: `#[derive(Deserialize)]` on `BoundIdentity` with `deny_unknown_fields`. Keep
validate-on-load on `RepositoryBinding` only. Cost/Risk: a binding JSON with extra identity keys starts failing
(correct, matches marker/sidecar).

### F10 — LOW — TESTS — Guards on serde/Display strings, not on the typed refusal

Evidence: `packages/cowshed/crates/cowshed-core/src/metadata.rs:1296-1300`

```
        let error = serde_json::from_value::<DetachedWorkspaceMetadata>(malformed).unwrap_err();
        assert!(
            error.to_string().contains("unknown variant `published`"),
            "{error}"
        );
```

and `metadata.rs:1564-1607` (`metadata_errors_expose_stable_messages_and_causes`) asserting exact `Display` text
including `"metadata I/O failed for /metadata.json: disk unavailable"`. Problem: §7.10bb. A deserializer that accepted
`"published"` as Active would still fail the contains() only because the string vanished; a rename of serde's wording
fails a still-correct reject. The I/O Display test cannot distinguish F2's lie from a real message. Fix:
`from_value::<DetachedWorkspaceMetadata>` is `is_err()` plus a typed round-trip that `PublicationState` has exactly
`Active | PendingFence`. Keep one Display test per variant only if CLI/docs pin the sentence; otherwise assert
`matches!(err, MetadataError::Json { .. })`. Cost/Risk: none.

## Cross-slice questions

- `packages/cowshed/crates/cowshed-core/src/storage/mod.rs:294-312` (`StorageLayout::checkout_layout`):
  `if let Ok(record) = read_json` swallows `MetadataError::Json` and `UnsupportedVersion` and then infers. That is
  fail-open on a corrupt record. F1's fix needs this function to die or to call the one remaining reader. Owner:
  storage/lifecycle slice.
- `packages/cowshed/crates/cowshed-core/src/storage/apfs.rs:127-134` (`UuidIncarnationSource`): mints
  `WorkspaceIncarnation` via `uuid::Uuid::new_v4().simple()`. This slice's type only requires 32 lowercase hex
  (`metadata.rs:395-405`) and tests use `00000000000000000000000000000001`, which is not RFC 4122 v4.
  Content-hash-as-index does not apply: incarnation is a generation nonce, not a digest of the image (clone gets a new
  incarnation and inherits lineage). If mint stays random, store `[u8; 16]` and hex only at the JSON edge; do not pull
  `uuid` into this module. Owner: storage/apfs slice.
- `packages/cowshed/crates/cowshed-core/Cargo.toml:33` `uuid = { version = "1", features = ["serde", "v4"] }`: unused by
  these three files; used by fsio/git/runtime/storage. Dep-bloat of `uuid` is a crate-level question, not this slice.
  `serde`/`serde_json`/`thiserror` are load-bearing here (`read_json`/`write_json`; `RepositoryBinding` errors). Do not
  replace JSON with `jq`. The crate-level `url` dep is unused by `normalize_remote_url` (hand-rolled on purpose:
  scp-like `git@host:owner/repo.git` is not a WHATWG URL) — keep the hand-roll; `url` crate justification belongs to
  whoever imports it.
- `packages/cowshed/crates/cowshed-gateway/src/{config,repo_mirror,sim_broker}.rs` RepoId copies: F3. Gateway-below-core
  constraint is their claim; identity crate vs import is their decision.
- `packages/cowshed/src/types.ts:43-47` restates `workspaceIncarnation: string` against `WorkspaceIncarnation`. Owner:
  Rust/TS DTO slice. This slice's JSON field name is the Rust serde camelCase of the newtype; TS should import or
  generate, not re-spell.
- `packages/cowshed/crates/cowshed-core/src/api/dto.rs:114-128` `GitOid` is 40|64 hex. `WorkspaceMarker.base_commit` /
  `WorkspaceInfoSnapshot.base_commit` are free `String` and tests use `"8f31c2d"`. Not a bug if abbreviated SHAs are the
  on-disk fact; do not silently switch to `GitOid` without a version bump (F6). Owner: api/dto slice.

## Non-findings (checked, clean)

- Single JSON I/O: `read_json` / `write_json` / `publish` → `fsio::publish_private_file`. No second serializer in this
  slice. Pretty-print + trailing newline is once-per-write durable spelling; not a hot path (§4.1 regime: controller
  metadata, not a probe loop).
- `serde_json` earns its weight: typed deny-unknown records, atomic publish, frozen-spelling tests. Shell-out to
  `jq`/`python` would lose error typing and `0600` publish.
- `uuid` crate: not referenced by these three files. `WorkspaceIncarnation` is not a UUID.
- `ImageCapacity` holds bytes; unit letters resolve once. No restated GB/GiB table.
- `GRANTS_SIDECAR_SUFFIX` / `sidecar_path` / `image_from_sidecar_path` are one pair of inverses. Callers use the const.
- `WORKSPACE_MARKER_PATH` lives in `storage/mod.rs` and checkout imports it; not restated.
- Marker vs sidecar overlapping facts (`repo_id`, `project_root`) are the dual-record design `checkout.rs` exists to
  move together. Not accidental duplication.
- `RepoId::parse` `format!("{owner}/{repo}")`, `WorkspaceIncarnation(String)`, `clone()` on rewrite, `canonicalize` in
  `resolves_to`: once per open/move/observation. Not findings.
- `WorkspaceMarker::read_from` / `DetachedWorkspaceMetadata::read_for_image` re-validate after `Deserialize` already did
  (L7 evaporating work) — once per open, note only.
- `OwnedRepoIds::new`/`from_parts` do not re-check disjointness; `RepositoryBinding::validate` does. Public constructors
  can represent overlap; callers in this slice go through the binding.
- `normalize_remote_url` is the one Git-remote → identity function; tests pin typed errors. No `url` crate, correctly.
- `relink_checkout` is unix-only without `cfg`; cowshed targets macos/linux. Fine.
- No `unwrap`/`expect` on operational paths in non-test code. Test `expect("clock")` / `expect("temp directory")` only.
- No `unsafe`. No 5k-line file. No function over ~100 lines in non-test code (`RepositoryBinding::validate` is the
  longest, ~67 lines).
- Checkout tests use typed `CheckoutLayout` / `MetadataError` / path equality, except they do not pin `InvalidPath`
  Display (F2). Repository tests assert typed `RepoIdError`/`BindingError`/`PathLayoutError`. Marker/sidecar proptests
  round-trip `serde_json::Value` (frozen spelling is the contract, not a rendered-string substitute).
- `CheckoutLayoutRecord` / sidecar / marker `deny_unknown_fields` on the types that have it. Binding identity does not
  (F9).
