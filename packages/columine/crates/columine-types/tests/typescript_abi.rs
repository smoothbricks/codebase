//! Audits the TypeScript host's ABI tables against this crate.
//!
//! `src/types.ts` and `src/wasm-backend.ts` restate the numbers the wasm
//! artifact actually returns, and their comments still pointed at a `vm.zig`
//! that no longer exists. They had drifted: `SlotType` was missing
//! `NESTED = 9`, and the slot-meta field offsets were bare literals inside
//! expressions.
//!
//! Rust is the source: it is what `columine.wasm` returns. Rather than a
//! generator — a build-order edge from this crate into the TypeScript package,
//! for tables a human edits by hand anyway — the audit harvests the TypeScript
//! declarations and compares them with what the Rust decoders accept, in both
//! directions. Adding a Rust variant without the TypeScript member, or
//! renumbering either side, fails here.
//!
//! Every harvest is paired with a floor, so a formatting change that makes the
//! scan return nothing fails the test instead of silently weakening it.

use columine_types::audit_parser::{enum_decls, norm};
use columine_types::types::{
    AggType, DurationUnit, EVICTION_ENTRY_SIZE, ErrorCode, Opcode, PROGRAM_HASH_PREFIX,
    PROGRAM_MAGIC, ProgramHeader, SLOT_META_SIZE, STATE_HEADER_SIZE, SlotMetaOffset, SlotType,
    StructFieldType,
};
use std::collections::BTreeMap;

const TYPES_TS: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../src/types.ts");
const WASM_BACKEND_TS: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../src/wasm-backend.ts");

fn read(path: &str) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|error| panic!("read {path}: {error}"))
}

/// Harvest a TypeScript `export enum <name> { NAME = <value>, ... }` block.
fn ts_enum(source: &str, name: &str, floor: usize) -> BTreeMap<String, u8> {
    let harvested: BTreeMap<String, u8> = enum_decls(source, &format!("export enum {name} "))
        .into_iter()
        .map(|(member, value)| (norm(&member), value))
        .collect();
    assert!(
        harvested.len() >= floor,
        "types.ts {name} harvest rotted: {} members (< {floor})",
        harvested.len()
    );
    harvested
}

/// Decode the whole `u8` domain through a Rust decoder into a normalized set.
fn rust_u8_tags<T: std::fmt::Debug>(decode: impl Fn(u8) -> Option<T>) -> BTreeMap<String, u8> {
    (0..=u8::MAX)
        .filter_map(|byte| decode(byte).map(|tag| (norm(&format!("{tag:?}")), byte)))
        .collect()
}

/// Harvest a `const NAME = <number>;` declaration.
fn ts_const(source: &str, name: &str) -> u64 {
    let needle = format!("{name} = ");
    let start = source
        .find(&needle)
        .unwrap_or_else(|| panic!("{name} not declared in the TypeScript source"));
    let rest = &source[start + needle.len()..];
    let literal: String = rest
        .chars()
        .take_while(|c| c.is_ascii_hexdigit() || *c == 'x' || *c == '_')
        .filter(|c| *c != '_')
        .collect();
    if let Some(hex) = literal.strip_prefix("0x") {
        u64::from_str_radix(hex, 16)
            .unwrap_or_else(|error| panic!("{name} is not a hex literal: {error}"))
    } else {
        literal
            .parse()
            .unwrap_or_else(|error| panic!("{name} is not a decimal literal: {error}"))
    }
}

#[test]
fn typescript_opcode_enum_matches_rust() {
    let harvested = ts_enum(&read(TYPES_TS), "Opcode", 55);
    assert_eq!(
        rust_u8_tags(Opcode::from_u8),
        harvested,
        "types.ts Opcode diverged from columine_types::Opcode"
    );
}

#[test]
fn typescript_slot_type_enum_matches_rust() {
    let harvested = ts_enum(&read(TYPES_TS), "SlotType", 10);
    assert_eq!(
        rust_u8_tags(SlotType::from_u8),
        harvested,
        "types.ts SlotType diverged from columine_types::SlotType"
    );
}

#[test]
fn typescript_agg_type_enum_matches_rust() {
    let harvested = ts_enum(&read(TYPES_TS), "AggType", 11);
    assert_eq!(
        rust_u8_tags(AggType::from_u8),
        harvested,
        "types.ts AggType diverged from columine_types::AggType"
    );
}

#[test]
fn typescript_struct_field_type_enum_matches_rust() {
    let harvested = ts_enum(&read(TYPES_TS), "StructFieldType", 10);
    assert_eq!(
        rust_u8_tags(StructFieldType::from_u8),
        harvested,
        "types.ts StructFieldType diverged from columine_types::StructFieldType"
    );
}

/// `TtlStartOf` is the host's name for `DurationUnit`: same wire byte, same
/// variants.
#[test]
fn typescript_ttl_start_of_matches_rust_duration_unit() {
    let harvested = ts_enum(&read(TYPES_TS), "TtlStartOf", 9);
    assert_eq!(
        rust_u8_tags(DurationUnit::from_u8),
        harvested,
        "types.ts TtlStartOf diverged from columine_types::DurationUnit"
    );
}

#[test]
fn typescript_error_code_enum_matches_rust() {
    let harvested = ts_enum(&read(TYPES_TS), "ErrorCode", 9);
    let rust: BTreeMap<String, u8> = (0..64_u32)
        .filter_map(|value| {
            ErrorCode::from_u32(value).map(|code| {
                (
                    norm(&format!("{code:?}")),
                    u8::try_from(value).expect("declared error codes fit a byte"),
                )
            })
        })
        .collect();
    assert_eq!(
        rust, harvested,
        "types.ts ErrorCode diverged from columine_types::ErrorCode — \
         vmErrorCode() throws on a status it cannot name"
    );
}

#[test]
fn typescript_program_constants_match_rust() {
    let source = read(TYPES_TS);
    assert_eq!(ts_const(&source, "PROGRAM_MAGIC"), u64::from(PROGRAM_MAGIC));
    assert_eq!(
        ts_const(&source, "HEADER_SIZE"),
        ProgramHeader::WIRE_SIZE as u64
    );
    assert_eq!(
        ts_const(&source, "PROGRAM_HASH_PREFIX"),
        u64::from(PROGRAM_HASH_PREFIX)
    );
}

/// The host reads slot metadata out of raw state bytes, so every offset it
/// indexes with is this crate's layout. These were bare literals inside the
/// indexing expressions; naming them is what makes them auditable.
#[test]
fn typescript_state_layout_constants_match_rust() {
    let source = read(WASM_BACKEND_TS);
    assert_eq!(
        ts_const(&source, "STATE_HEADER_SIZE"),
        u64::from(STATE_HEADER_SIZE)
    );
    assert_eq!(
        ts_const(&source, "SLOT_META_SIZE"),
        u64::from(SLOT_META_SIZE)
    );
    assert_eq!(
        ts_const(&source, "SLOT_META_TYPE_FLAGS_OFFSET"),
        u64::from(SlotMetaOffset::TYPE_FLAGS)
    );
    assert_eq!(
        ts_const(&source, "SLOT_META_EVICTED_BUFFER_OFFSET"),
        u64::from(SlotMetaOffset::EVICTED_BUFFER_OFFSET)
    );
    assert_eq!(
        ts_const(&source, "SLOT_META_EVICTED_COUNT_OFFSET"),
        u64::from(SlotMetaOffset::EVICTED_COUNT)
    );
    assert_eq!(
        ts_const(&source, "EVICTION_ENTRY_SIZE"),
        u64::from(EVICTION_ENTRY_SIZE)
    );
}
