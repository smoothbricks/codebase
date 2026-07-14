//! Opcode-dispatch audit against the frozen Zig ABI (tripwire).
//!
//! Born from the 0x81 incident: `BATCH_STRUCT_MAP_UPSERT_FIRST` was declared
//! and dispatched in Zig but silently missing from the Rust length table,
//! both dispatch arms, and both registries — the faithful unknown-byte skip
//! then misparsed programs without any test failing. The Zig sources are
//! deleted (cutover); `zig_abi_fixture` freezes what they declared and
//! dispatched, and this audit asserts the Rust side covers exactly that set
//! (and vice versa, modulo an explicit allowlist). A deliberate ABI change
//! edits the fixture in the same commit that changes the dispatch.
//!
//! Rust-side harvests keep sanity FLOORS pinned to today's counts so parser
//! rot (a formatting change making a scan return nothing) fails loudly.

use columine_types::types::Opcode;
use columine_types::zig_abi_fixture::{FLAT_UNDO_OPS, TYPES_ZIG_OPCODES, ZIG_DISPATCHED_BYTES};
use columine_types::zig_audit::{arm_bytes, arm_names, enum_decls, norm, read_source};
use std::collections::{BTreeMap, BTreeSet};

const MANIFEST: &str = env!("CARGO_MANIFEST_DIR");

/// Rust-covered bytes legitimately absent from the frozen Zig dispatch,
/// each with the reason it is allowed. Empty today — the sets are equal.
const RUST_EXTRA_ALLOWLIST: &[(u8, &str)] = &[];

/// Frozen-Zig bytes legitimately absent from the Rust dispatch. Empty
/// today; an entry here must name the follow-up that removes it.
const ZIG_EXTRA_ALLOWLIST: &[(u8, &str)] = &[];

fn zig_dispatched() -> BTreeSet<u8> {
    ZIG_DISPATCHED_BYTES.iter().copied().collect()
}

fn rust_covered() -> BTreeSet<u8> {
    let rust_vm = read_source(MANIFEST, "src/vm.rs");
    let rust_types = read_source(MANIFEST, "../columine-types/src/types.rs");
    let rust_decls: BTreeMap<String, u8> = enum_decls(&rust_types, "pub enum Opcode")
        .into_iter()
        .map(|(n, b)| (norm(&n), b))
        .collect();
    assert!(
        rust_decls.len() >= 57,
        "types.rs Opcode decl harvest rotted: got {}",
        rust_decls.len()
    );
    let raw = arm_bytes(&rust_vm);
    assert!(
        raw.len() >= 25,
        "vm.rs raw-arm harvest rotted: {}",
        raw.len()
    );
    let mut covered = raw;
    let mut top_count = 0usize;
    for name in arm_names(&rust_vm, "Opcode::") {
        if let Some(byte) = rust_decls.get(&norm(&name)) {
            covered.insert(*byte);
            top_count += 1;
        }
    }
    assert!(
        top_count >= 30,
        "vm.rs Opcode-arm harvest rotted: only {top_count} registry arms"
    );
    covered
}

fn hex(set: impl IntoIterator<Item = u8>) -> Vec<String> {
    set.into_iter().map(|b| format!("{b:#04x}")).collect()
}

/// The 0x81-class tripwire: every byte the frozen Zig dispatch handled must
/// be covered by the Rust dispatch (registry arm or raw-byte arm).
#[test]
fn every_zig_dispatched_byte_is_rust_covered() {
    let zig = zig_dispatched();
    let rust = rust_covered();
    let allow: BTreeSet<u8> = ZIG_EXTRA_ALLOWLIST.iter().map(|(b, _)| *b).collect();
    let missing: Vec<u8> = zig
        .difference(&rust)
        .copied()
        .filter(|b| !allow.contains(b))
        .collect();
    assert!(
        missing.is_empty(),
        "the frozen Zig ABI dispatches opcode byte(s) the Rust side does not \
         cover: {:?} — port the arm (length table + dispatch + registries) \
         with a regression pin, the way 0x81/BATCH_STRUCT_MAP_UPSERT_FIRST \
         was fixed (c36adb3fe)",
        hex(missing)
    );
    for (b, reason) in ZIG_EXTRA_ALLOWLIST {
        assert!(
            zig.contains(b) && !rust.contains(b),
            "stale ZIG_EXTRA_ALLOWLIST entry {b:#04x} ({reason}) — remove it"
        );
    }
}

/// Reverse direction: the Rust dispatch must not invent opcodes the frozen
/// ABI does not handle without editing the fixture deliberately.
#[test]
fn every_rust_covered_byte_is_zig_dispatched() {
    let zig = zig_dispatched();
    let rust = rust_covered();
    let allow: BTreeSet<u8> = RUST_EXTRA_ALLOWLIST.iter().map(|(b, _)| *b).collect();
    let extra: Vec<u8> = rust
        .difference(&zig)
        .copied()
        .filter(|b| !allow.contains(b))
        .collect();
    assert!(
        extra.is_empty(),
        "Rust covers opcode byte(s) outside the frozen Zig ABI: {:?} — a \
         deliberate ABI extension edits zig_abi_fixture in this commit",
        hex(extra)
    );
    for (b, reason) in RUST_EXTRA_ALLOWLIST {
        assert!(
            rust.contains(b) && !zig.contains(b),
            "stale RUST_EXTRA_ALLOWLIST entry {b:#04x} ({reason}) — remove it"
        );
    }
}

/// Every frozen dispatched byte must decode through `Opcode::from_u8` to a
/// variant whose name matches the frozen declaration (registry completeness
/// — 0x81 was also missing here).
#[test]
fn every_zig_dispatched_byte_decodes_in_types_registry() {
    let by_byte: BTreeMap<u8, &str> = TYPES_ZIG_OPCODES.iter().map(|(n, b)| (*b, *n)).collect();
    for byte in ZIG_DISPATCHED_BYTES {
        let Some(zig_name) = by_byte.get(byte) else {
            panic!("fixture dispatches {byte:#04x} but no frozen registry declares it");
        };
        let rust = Opcode::from_u8(*byte).unwrap_or_else(|| {
            panic!("Opcode::from_u8({byte:#04x}) is None but the frozen ABI dispatches it as {zig_name}")
        });
        let rust_name = norm(&format!("{rust:?}"));
        assert_eq!(
            &rust_name, zig_name,
            "discriminant {byte:#04x} names disagree: frozen {zig_name} vs Rust {rust_name}"
        );
    }
}

/// FlatUndoOp: the undo-entry wire ops. The frozen Zig-ABI set must remain a
/// subset (the tombstone never shrinks or renumbers), post-parity extensions
/// are the explicit allowlist below, and the Rust rollback/decode dispatch
/// must cover every declared op.
#[test]
fn flat_undo_op_registry_and_rollback_arms_match_fixture() {
    // WHY an allowlist: the wire contract is ours to evolve post-cutover,
    // but every extension must be named here deliberately — an unlisted new
    // op fails the audit exactly like a dropped frozen op.
    const POST_PARITY_EXTENSIONS: &[(&str, u8)] = &[
        // Scalar writes were un-journaled in the frozen Zig ABI; journaled
        // deliberately at the post-parity sweep.
        ("ScalarUpdate", 14),
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
