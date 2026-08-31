//! Harvests Compact/Result ABI numbers from `parse-backend.ts` and compares
//! them with the Rust constants the wasm decoder actually uses.
//!
//! Generation would add a build-order edge from this crate into the TypeScript
//! package. The existing columine-types audit is the same shape: harvest the
//! host table, fail if a Rust number moves and TS does not.

use columine_arrow::{
    ArrowType, MAX_EVENTS_PER_BATCH, MAX_SCHEMA_FIELDS, MAX_VALUE_BYTES, MIN_ARROW_OUTPUT_CAPACITY,
};
use columine_event_processor::{
    COMPACT_ABI_VERSION, COMPACT_BATCH_MAGIC, COMPACT_DESCRIPTOR_SIZE, COMPACT_DIAGNOSTIC_STAGE,
    COMPACT_HEADER_SIZE, CreateFailure, RESULT_HEADER_SIZE, ResultCode,
};

const PARSE_BACKEND_TS: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../src/parse-backend.ts");

fn read() -> String {
    std::fs::read_to_string(PARSE_BACKEND_TS)
        .unwrap_or_else(|error| panic!("read parse-backend.ts: {error}"))
}

fn ts_const(source: &str, name: &str) -> u64 {
    let needle = format!("{name} = ");
    let start = source
        .find(&needle)
        .unwrap_or_else(|| panic!("{name} not declared in parse-backend.ts"));
    let rest = &source[start + needle.len()..];
    let expr: String = rest
        .chars()
        .take_while(|c| *c != ';' && *c != '\n')
        .collect();
    let expr = expr.trim();
    if expr.contains('*') {
        return expr.split('*').map(|part| parse_int(part.trim())).product();
    }
    parse_int(expr)
}

fn parse_int(literal: &str) -> u64 {
    let compact: String = literal.chars().filter(|c| *c != '_').collect();
    if let Some(hex) = compact.strip_prefix("0x") {
        u64::from_str_radix(hex, 16)
            .unwrap_or_else(|error| panic!("{literal} is not a hex literal: {error}"))
    } else {
        compact
            .parse()
            .unwrap_or_else(|error| panic!("{literal} is not a decimal literal: {error}"))
    }
}

fn ts_kind_tag(source: &str, kind: &str) -> u8 {
    let block_start = source
        .find("const COMPACT_KIND_TAG = {")
        .expect("COMPACT_KIND_TAG not declared");
    let block = &source[block_start..];
    let needle = format!("{kind}: ");
    let start = block
        .find(&needle)
        .unwrap_or_else(|| panic!("COMPACT_KIND_TAG.{kind} missing"));
    let rest = &block[start + needle.len()..];
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits
        .parse()
        .unwrap_or_else(|error| panic!("COMPACT_KIND_TAG.{kind}: {error}"))
}

/// Read the `EP_CREATE_FAILURE` decoder table, which is keyed by ABI code:
/// find the line naming `code`, then parse the hex key that precedes it.
fn ts_create_failure(source: &str, code: &str) -> u32 {
    let block_start = source
        .find("const EP_CREATE_FAILURE = {")
        .expect("EP_CREATE_FAILURE not declared");
    let block = &source[block_start..];
    let needle = format!(": '{code}',");
    let end = block
        .find(&needle)
        .unwrap_or_else(|| panic!("EP_CREATE_FAILURE.{code} missing"));
    let key = block[..end].rsplit('\n').next().unwrap_or_default().trim();
    u32::try_from(parse_int(key))
        .unwrap_or_else(|error| panic!("EP_CREATE_FAILURE.{code}: {error}"))
}

#[test]
fn parse_backend_ts_compact_abi_matches_rust() {
    let source = read();
    assert_eq!(
        ts_const(&source, "MAX_EVENTS_PER_BATCH"),
        u64::from(MAX_EVENTS_PER_BATCH)
    );
    assert_eq!(ts_const(&source, "MAX_FIELDS"), MAX_SCHEMA_FIELDS as u64);
    assert_eq!(
        ts_const(&source, "MAX_VARIABLE_DATA_BYTES"),
        u64::from(MAX_VALUE_BYTES)
    );
    assert_eq!(
        ts_const(&source, "MIN_COMPACT_ARROW_CAPACITY"),
        MIN_ARROW_OUTPUT_CAPACITY as u64
    );
    assert_eq!(
        ts_const(&source, "WASM_OUTPUT_HEADER_SIZE"),
        RESULT_HEADER_SIZE as u64
    );
    assert_eq!(
        ts_const(&source, "COMPACT_MAGIC"),
        u64::from(COMPACT_BATCH_MAGIC)
    );
    assert_eq!(
        ts_const(&source, "COMPACT_VERSION"),
        u64::from(COMPACT_ABI_VERSION)
    );
    assert_eq!(
        ts_const(&source, "COMPACT_HEADER_SIZE"),
        COMPACT_HEADER_SIZE as u64
    );
    assert_eq!(
        ts_const(&source, "COMPACT_DESCRIPTOR_SIZE"),
        COMPACT_DESCRIPTOR_SIZE as u64
    );
    assert_eq!(ts_const(&source, "RESULT_OK"), ResultCode::Ok as u64);
    assert_eq!(ts_kind_tag(&source, "null"), ArrowType::Null as u8);
    assert_eq!(ts_kind_tag(&source, "u32"), ArrowType::Int32 as u8);
    assert_eq!(ts_kind_tag(&source, "f64"), ArrowType::Float64 as u8);
    assert_eq!(ts_kind_tag(&source, "binary"), ArrowType::Binary as u8);
    assert_eq!(ts_kind_tag(&source, "utf8"), ArrowType::Utf8 as u8);
    assert_eq!(ts_kind_tag(&source, "bool"), ArrowType::Bool as u8);
    assert_eq!(ts_kind_tag(&source, "i64"), ArrowType::Int64 as u8);
}

/// The host decodes these codes into distinct diagnostics, so a code that
/// moves on one side only puts the wrong cause in the error message.
#[test]
fn parse_backend_ts_create_failures_match_rust() {
    let source = read();
    for (name, failure) in [
        ("BAD_REQUEST", CreateFailure::BadRequest),
        ("CAPACITY", CreateFailure::Capacity),
        ("SCHEMA_MESSAGE", CreateFailure::SchemaMessage),
        ("SCHEMA_TOO_MANY_FIELDS", CreateFailure::SchemaTooManyFields),
        ("SCHEMA_FIELD_METADATA", CreateFailure::SchemaFieldMetadata),
        ("SCHEMA_FIELD_COUNT", CreateFailure::SchemaFieldCount),
        ("SCHEMA_TYPE_MISMATCH", CreateFailure::SchemaTypeMismatch),
        ("SCHEMA_NULLABILITY", CreateFailure::SchemaNullability),
        ("SCHEMA_FIELD_NAMES", CreateFailure::SchemaFieldNames),
        ("INIT", CreateFailure::Init),
        ("HANDLES_EXHAUSTED", CreateFailure::HandlesExhausted),
    ] {
        assert_eq!(ts_create_failure(&source, name), failure as u32, "{name}");
    }
}

#[test]
fn compact_diagnostic_stage_is_not_column() {
    // COLUMN is 4. Compact used to share that byte; a compact BAD_HEADER
    // then decoded as INVALID_JSON.
    assert_eq!(COMPACT_DIAGNOSTIC_STAGE, 6);
}
