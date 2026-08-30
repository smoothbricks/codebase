//! Opcode-dispatch audit against frozen ABI snapshots (tripwire).
//!
//! Born from the 0x81 incident: `BATCH_STRUCT_MAP_UPSERT_FIRST` was declared
//! and dispatched in the cutover snapshot but silently missing from the Rust
//! length table, both dispatch arms, and both registries — the faithful
//! unknown-byte skip then misparsed programs without any test failing. The
//! fixture freezes what the registries declared and dispatched, and this audit
//! asserts the Rust side covers exactly that set (and vice versa, modulo an
//! explicit allowlist). A deliberate ABI change edits the fixture in the same
//! commit that changes the dispatch.
//!
//! Rust-side enum-arm harvest keeps a sanity floor pinned to today's count so
//! parser rot (a formatting change making the scan return nothing) fails loudly.

use columine_types::abi_registry_fixture::{
    DISPATCHED_OPCODE_BYTES, FLAT_UNDO_OPS, TYPES_OPCODE_REGISTRY,
};
use columine_types::audit_parser::{arm_names, enum_decls, norm, read_source};
use columine_types::types::Opcode;
use std::collections::{BTreeMap, BTreeSet};

const MANIFEST: &str = env!("CARGO_MANIFEST_DIR");

/// Rust-covered bytes legitimately absent from the frozen dispatch, each with
/// the reason it is allowed. Empty today — the sets are equal.
const RUST_EXTRA_ALLOWLIST: &[(u8, &str)] = &[];

/// Frozen bytes legitimately absent from the Rust dispatch. Empty today; an
/// entry here must name the follow-up that removes it.
const FROZEN_EXTRA_ALLOWLIST: &[(u8, &str)] = &[];

fn frozen_dispatched() -> BTreeSet<u8> {
    DISPATCHED_OPCODE_BYTES.iter().copied().collect()
}

fn rust_covered() -> BTreeSet<u8> {
    let rust_vm = read_source(MANIFEST, "src/vm.rs");
    let rust_types = read_source(MANIFEST, "../columine-types/src/types.rs");
    let rust_decls = enum_decls(&rust_types, "pub enum Opcode");
    assert!(
        rust_decls.len() >= 57,
        "types.rs Opcode decl harvest rotted: got {}",
        rust_decls.len()
    );
    let mut covered = BTreeSet::new();
    for (name, byte) in rust_decls {
        let needle = format!("Opcode::{name}");
        if rust_vm.matches(&needle).count() >= 2 {
            covered.insert(byte);
        }
    }
    assert!(
        covered.len() >= 55,
        "vm.rs Opcode coverage harvest rotted: only {} registry arms",
        covered.len()
    );
    covered
}

fn hex(set: impl IntoIterator<Item = u8>) -> Vec<String> {
    set.into_iter().map(|b| format!("{b:#04x}")).collect()
}

/// The 0x81-class tripwire: every byte the frozen dispatch handled must be
/// covered by an `Opcode` arm in the Rust dispatch.
#[test]
fn every_frozen_dispatched_byte_is_rust_covered() {
    let frozen = frozen_dispatched();
    let rust = rust_covered();
    let allow: BTreeSet<u8> = FROZEN_EXTRA_ALLOWLIST.iter().map(|(b, _)| *b).collect();
    let missing: Vec<u8> = frozen
        .difference(&rust)
        .copied()
        .filter(|b| !allow.contains(b))
        .collect();
    assert!(
        missing.is_empty(),
        "the frozen ABI dispatches opcode byte(s) the Rust side does not \
         cover: {:?} — port the arm (length table + dispatch + registries) \
         with a regression pin, the way 0x81/BATCH_STRUCT_MAP_UPSERT_FIRST \
         was fixed (c36adb3fe)",
        hex(missing)
    );
    for (b, reason) in FROZEN_EXTRA_ALLOWLIST {
        assert!(
            frozen.contains(b) && !rust.contains(b),
            "stale FROZEN_EXTRA_ALLOWLIST entry {b:#04x} ({reason}) — remove it"
        );
    }
}

/// Reverse direction: the Rust dispatch must not invent opcodes the frozen
/// ABI does not handle without editing the fixture deliberately.
#[test]
fn every_rust_covered_byte_is_frozen_dispatched() {
    let frozen = frozen_dispatched();
    let rust = rust_covered();
    let allow: BTreeSet<u8> = RUST_EXTRA_ALLOWLIST.iter().map(|(b, _)| *b).collect();
    let extra: Vec<u8> = rust
        .difference(&frozen)
        .copied()
        .filter(|b| !allow.contains(b))
        .collect();
    assert!(
        extra.is_empty(),
        "Rust covers opcode byte(s) outside the frozen ABI: {:?} — a \
         deliberate ABI extension edits abi_registry_fixture in this commit",
        hex(extra)
    );
    for (b, reason) in RUST_EXTRA_ALLOWLIST {
        assert!(
            rust.contains(b) && !frozen.contains(b),
            "stale RUST_EXTRA_ALLOWLIST entry {b:#04x} ({reason}) — remove it"
        );
    }
}

/// Every frozen dispatched byte must decode through `Opcode::from_u8` to a
/// variant whose name matches the frozen declaration (registry completeness
/// — 0x81 was also missing here).
#[test]
fn every_frozen_dispatched_byte_decodes_in_types_registry() {
    let by_byte: BTreeMap<u8, &str> = TYPES_OPCODE_REGISTRY
        .iter()
        .map(|(n, b)| (*b, *n))
        .collect();
    for byte in DISPATCHED_OPCODE_BYTES {
        let Some(frozen_name) = by_byte.get(byte) else {
            panic!("fixture dispatches {byte:#04x} but no frozen registry declares it");
        };
        let rust = Opcode::from_u8(*byte).unwrap_or_else(|| {
            panic!("Opcode::from_u8({byte:#04x}) is None but the frozen ABI dispatches it as {frozen_name}")
        });
        let rust_name = norm(&format!("{rust:?}"));
        assert_eq!(
            &rust_name, frozen_name,
            "discriminant {byte:#04x} names disagree: frozen {frozen_name} vs Rust {rust_name}"
        );
    }
}

/// FlatUndoOp: the undo-entry wire ops. The frozen ABI set must remain a
/// subset (the fixture never shrinks or renumbers), post-parity extensions are
/// the explicit allowlist below, and the Rust rollback/decode dispatch must
/// cover every declared op.
#[test]
fn flat_undo_op_registry_and_rollback_arms_match_fixture() {
    // WHY an allowlist: the wire contract is ours to evolve post-cutover,
    // but every extension must be named here deliberately — an unlisted new
    // op fails the audit exactly like a dropped frozen op.
    const POST_PARITY_EXTENSIONS: &[(&str, u8)] = &[
        // Scalar writes were un-journaled in the frozen ABI; journaled
        // deliberately at the post-parity sweep.
        ("ScalarUpdate", 14),
        // Variable nested state snapshots journal exact bytes without widening
        // the fixed undo-entry ABI.
        ("StateBytes", 15),
    ];

    let rust_undo = read_source(MANIFEST, "src/undo_log.rs");
    let rust_vm = read_source(MANIFEST, "src/vm.rs");

    let mut frozen: BTreeMap<String, u8> = FLAT_UNDO_OPS
        .iter()
        .map(|(n, b)| (n.to_string(), *b))
        .collect();
    let rust_decls: BTreeMap<String, u8> = enum_decls(&rust_undo, "pub enum FlatUndoOp")
        .into_iter()
        .map(|(n, b)| (norm(&n), b))
        .collect();
    assert!(
        frozen.len() >= 13,
        "frozen FlatUndoOp fixture rotted: {}",
        frozen.len()
    );
    for (name, byte) in POST_PARITY_EXTENSIONS {
        assert!(
            !frozen.values().any(|b| b == byte),
            "extension {name} reuses a frozen ABI byte {byte:#04x}"
        );
        frozen.insert(norm(name), *byte);
    }
    assert_eq!(
        frozen, rust_decls,
        "FlatUndoOp declarations diverged from frozen ABI + named extensions"
    );

    let declared_names: BTreeSet<String> = frozen.keys().cloned().collect();
    let mut rust_arms = BTreeSet::new();
    for src in [&rust_undo, &rust_vm] {
        for name in arm_names(src, "FlatUndoOp::") {
            let n = norm(&name);
            if frozen.contains_key(&n) {
                rust_arms.insert(n);
            }
        }
    }
    assert_eq!(
        rust_arms, declared_names,
        "Rust rollback/decode arms do not cover every declared FlatUndoOp"
    );
}
