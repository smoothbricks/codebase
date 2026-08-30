//! Registry-declaration audit against the frozen ABI snapshot (tripwire; see
//! the WHY on `columine_types::audit_parser` and the 0x81 incident it
//! answers).
//!
//! The snapshot was captured at the port cutover; `abi_registry_fixture`
//! preserves its ground truth. There is one `Opcode` registry, in
//! `src/types.rs`, and it is audited both against that snapshot and against
//! its own `from_u8`. A deliberate ABI change edits the fixture and the
//! registry in the same commit.

use columine_types::abi_registry_fixture::{
    AGG_TYPES, DURATION_UNITS, ERROR_CODES, SLOT_TYPES, STRUCT_FIELD_TYPES, TYPES_OPCODE_REGISTRY,
};
use columine_types::audit_parser::{enum_decls, norm, read_source};
use columine_types::types::{
    AggType, DurationUnit, ErrorCode, Opcode as TypesOpcode, SlotType, StructFieldType,
};
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

/// Decode every byte in the domain and compare the resulting tag set with the
/// frozen one. Set equality in both directions is what makes this go red:
/// adding a variant introduces a decoded tag the snapshot lacks, renumbering
/// one moves it, and dropping `from_u8` coverage removes it.
fn audit_u8_registry<T: std::fmt::Debug>(
    what: &str,
    frozen: &[(&str, u8)],
    decode: impl Fn(u8) -> Option<T>,
) {
    let decoded: BTreeMap<String, u8> = (0..=u8::MAX)
        .filter_map(|byte| decode(byte).map(|tag| (norm(&format!("{tag:?}")), byte)))
        .collect();
    assert_eq!(
        fixture(frozen),
        decoded,
        "{what} diverged from the frozen ABI snapshot — if this change is \
         deliberate, update abi_registry_fixture in this commit"
    );
}

#[test]
fn slot_type_registry_matches_fixture() {
    audit_u8_registry("SlotType", SLOT_TYPES, SlotType::from_u8);
}

#[test]
fn agg_type_registry_matches_fixture() {
    audit_u8_registry("AggType", AGG_TYPES, AggType::from_u8);
}

#[test]
fn struct_field_type_registry_matches_fixture() {
    audit_u8_registry(
        "StructFieldType",
        STRUCT_FIELD_TYPES,
        StructFieldType::from_u8,
    );
}

#[test]
fn duration_unit_registry_matches_fixture() {
    audit_u8_registry("DurationUnit", DURATION_UNITS, DurationUnit::from_u8);
}

/// `ErrorCode` is a `u32` status word, so its domain is audited over the
/// declared range plus a margin past the last variant rather than 0..=255.
#[test]
fn error_code_registry_matches_fixture() {
    let decoded: BTreeMap<String, u32> = (0..64_u32)
        .filter_map(|value| {
            ErrorCode::from_u32(value).map(|code| (norm(&format!("{code:?}")), value))
        })
        .collect();
    let frozen: BTreeMap<String, u32> = ERROR_CODES
        .iter()
        .map(|(name, value)| ((*name).to_owned(), *value))
        .collect();
    assert_eq!(
        frozen, decoded,
        "ErrorCode diverged from the frozen ABI snapshot — if this change is \
         deliberate, update abi_registry_fixture in this commit"
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
