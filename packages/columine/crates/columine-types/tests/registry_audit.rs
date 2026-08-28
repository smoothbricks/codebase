//! Registry-declaration audit against frozen ABI snapshots (tripwire; see the
//! WHY on `columine_types::audit_parser` and the 0x81 incident it answers).
//!
//! The snapshots were captured at the port cutover; `abi_registry_fixture`
//! preserves their ground truth. Each Rust registry is audited against its
//! corresponding frozen declaration set — separately on purpose: the types
//! and opcodes registries intentionally differ (types has the Nested family,
//! opcodes does not), and that distinction is pinned truth. A deliberate ABI
//! change edits the fixture and registry in the same commit.

use columine_types::abi_registry_fixture::{OPCODES_OPCODE_REGISTRY, TYPES_OPCODE_REGISTRY};
use columine_types::audit_parser::{enum_decls, norm, read_source};
use columine_types::types::Opcode as TypesOpcode;
use std::collections::BTreeMap;

const MANIFEST: &str = env!("CARGO_MANIFEST_DIR");

fn decls(src: &str, header: &str, floor: usize, what: &str) -> BTreeMap<String, u8> {
    let map: BTreeMap<String, u8> = enum_decls(src, header)
        .into_iter()
        .map(|(n, b)| (norm(&n), b))
        .collect();
    assert!(
        map.len() >= floor,
        "{what} harvest rotted: {} (< {floor})",
        map.len()
    );
    map
}

fn fixture(pairs: &[(&str, u8)]) -> BTreeMap<String, u8> {
    pairs.iter().map(|(n, b)| (n.to_string(), *b)).collect()
}

#[test]
fn types_rs_registry_matches_fixture() {
    let rust = read_source(MANIFEST, "src/types.rs");
    let rust_decls = decls(&rust, "pub enum Opcode", 55, "types.rs Opcode");
    assert_eq!(
        fixture(TYPES_OPCODE_REGISTRY),
        rust_decls,
        "types.rs Opcode registry diverged from the frozen ABI snapshot — \
         if this change is deliberate, update abi_registry_fixture in this commit"
    );
}

#[test]
fn opcodes_rs_registry_matches_fixture() {
    let rust = read_source(MANIFEST, "src/opcodes.rs");
    let rust_decls = decls(&rust, "pub enum Opcode", 50, "opcodes.rs Opcode");
    assert_eq!(
        fixture(OPCODES_OPCODE_REGISTRY),
        rust_decls,
        "opcodes.rs Opcode registry diverged from the frozen ABI snapshot — \
         if this change is deliberate, update abi_registry_fixture in this commit"
    );
}

/// `Opcode::from_u8` must agree with the enum declarations exactly — a
/// variant added to the enum but forgotten in `from_u8` (or vice versa) is
/// the same silent-skip class as 0x81.
#[test]
fn types_rs_from_u8_matches_declarations() {
    let rust = read_source(MANIFEST, "src/types.rs");
    let declared = decls(&rust, "pub enum Opcode", 55, "types.rs Opcode");
    let mut decoded = BTreeMap::new();
    for byte in 0..=255u8 {
        if let Some(op) = TypesOpcode::from_u8(byte) {
            decoded.insert(norm(&format!("{op:?}")), byte);
        }
    }
    assert_eq!(
        declared, decoded,
        "types.rs Opcode::from_u8 diverged from the enum declarations"
    );
}
