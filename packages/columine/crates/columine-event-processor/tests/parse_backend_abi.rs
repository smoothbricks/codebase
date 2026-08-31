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
    COMPACT_HEADER_SIZE, CreateFailure, RESULT_HEADER_SIZE, ResultCode, WASM_EVENT_CAPACITY,
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

/// The TypeScript `COMPACT_KIND_TAG` key for one plane.
///
/// Wildcard-free ON PURPOSE, and that is the whole point of this harness.
/// The audit used to list the tags by hand, which is how a Rust change could
/// narrow tag 1 to a signed plane while TypeScript kept calling it unsigned
/// and nothing failed. Adding an `ArrowType` variant without naming its
/// TypeScript kind here does not fail an assertion — it fails to COMPILE.
fn ts_kind(plane: ArrowType) -> &'static str {
    match plane {
        ArrowType::Null => "null",
        ArrowType::Int32 => "i32",
        ArrowType::Float64 => "f64",
        ArrowType::Binary => "binary",
        ArrowType::Utf8 => "utf8",
        ArrowType::Bool => "bool",
        ArrowType::Int64 => "i64",
        ArrowType::Int8 => "i8",
        ArrowType::Int16 => "i16",
        ArrowType::UInt8 => "u8",
        ArrowType::UInt16 => "u16",
        ArrowType::UInt32 => "u32",
        ArrowType::UInt64 => "u64",
        ArrowType::Float16 => "f16",
        ArrowType::Float32 => "f32",
        ArrowType::Decimal128 => "decimal128",
        ArrowType::Decimal256 => "decimal256",
        ArrowType::LargeBinary => "largeBinary",
        ArrowType::LargeUtf8 => "largeUtf8",
        ArrowType::FixedSizeBinary => "fixedSizeBinary",
        ArrowType::IntervalYearMonth => "intervalYearMonth",
        ArrowType::IntervalDayTime => "intervalDayTime",
        ArrowType::IntervalMonthDayNano => "intervalMonthDayNano",
    }
}

/// The TypeScript `EP_CREATE_FAILURE` name for one handle-creation failure.
/// Wildcard-free for the same reason as [`ts_kind`].
fn ts_create_failure_name(failure: CreateFailure) -> &'static str {
    match failure {
        CreateFailure::BadRequest => "BAD_REQUEST",
        CreateFailure::Capacity => "CAPACITY",
        CreateFailure::SchemaMessage => "SCHEMA_MESSAGE",
        CreateFailure::SchemaTooManyFields => "SCHEMA_TOO_MANY_FIELDS",
        CreateFailure::SchemaFieldMetadata => "SCHEMA_FIELD_METADATA",
        CreateFailure::SchemaFieldCount => "SCHEMA_FIELD_COUNT",
        CreateFailure::SchemaTypeMismatch => "SCHEMA_TYPE_MISMATCH",
        CreateFailure::SchemaNullability => "SCHEMA_NULLABILITY",
        CreateFailure::SchemaFieldNames => "SCHEMA_FIELD_NAMES",
        CreateFailure::Init => "INIT",
        CreateFailure::HandlesExhausted => "HANDLES_EXHAUSTED",
    }
}

/// Every `key: <decimal>,` entry of the `COMPACT_KIND_TAG` object literal, in
/// source order.
///
/// A line inside the braces that is not an entry PANICS rather than being
/// skipped: an audit that silently ignores what it cannot read is an audit
/// that passes when the table is broken.
fn ts_kind_tags(source: &str) -> Vec<(String, u8)> {
    const OPENER: &str = "const COMPACT_KIND_TAG = {";
    let block_start = source.find(OPENER).expect("COMPACT_KIND_TAG not declared");
    let body_start = block_start + OPENER.len();
    let body_end = body_start
        + source[body_start..]
            .find('}')
            .expect("COMPACT_KIND_TAG is not closed");
    source[body_start..body_end]
        .lines()
        .filter_map(|line| {
            let line = line.trim().trim_end_matches(',');
            if line.is_empty() {
                return None;
            }
            let (key, value) = line
                .split_once(':')
                .unwrap_or_else(|| panic!("COMPACT_KIND_TAG entry is not `key: number`: {line}"));
            let tag = value.trim().parse::<u8>().unwrap_or_else(|error| {
                panic!("COMPACT_KIND_TAG.{key} is not a u8 literal: {error}")
            });
            Some((key.trim().to_owned(), tag))
        })
        .collect()
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
    // Distinct quantities: a rowCount ceiling and an allocation size. Sharing
    // one number made every handle allocate the whole 65536-row column plane.
    assert_eq!(
        ts_const(&source, "EP_EVENT_CAPACITY"),
        u64::from(WASM_EVENT_CAPACITY)
    );
    assert_ne!(WASM_EVENT_CAPACITY, MAX_EVENTS_PER_BATCH);
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
}

/// The plane table is the seam between the two implementations, so the audit
/// is set equality and not a hand-written list of assertions.
///
/// Adding a plane on ONE side only fails here: a Rust variant with no
/// TypeScript entry, or a TypeScript entry Rust does not know about, both
/// break the comparison. The Rust half is additionally compile-checked by
/// `ts_kind`, which has no wildcard arm.
#[test]
fn parse_backend_ts_kind_table_is_exactly_the_rust_plane_table() {
    let source = read();
    let declared = ts_kind_tags(&source);
    let expected: Vec<(String, u8)> = ArrowType::ALL
        .iter()
        .map(|plane| (ts_kind(*plane).to_owned(), *plane as u8))
        .collect();
    assert_eq!(
        declared, expected,
        "COMPACT_KIND_TAG must be exactly the Rust plane table, in tag order"
    );

    // Two planes claiming one TypeScript kind would make the comparison above
    // pass while the host decoded one of them as the other.
    let mut names: Vec<&str> = ArrowType::ALL.iter().map(|plane| ts_kind(*plane)).collect();
    let plane_count = names.len();
    names.sort_unstable();
    names.dedup();
    assert_eq!(
        names.len(),
        plane_count,
        "two planes share a TypeScript kind"
    );

    // The host's bounds check must be a DERIVED maximum. It used to be
    // `tag > COMPACT_KIND_TAG.i64`, which silently rejected every plane
    // appended after Int64 — the exact failure this table is meant to prevent.
    assert!(
        source.contains("COMPACT_MAX_KIND_TAG"),
        "parse-backend.ts must derive a maximum plane tag"
    );
    assert!(
        !source.contains("tag > COMPACT_KIND_TAG."),
        "the plane bounds check must not name one plane's tag as the maximum"
    );
    let highest = declared
        .iter()
        .map(|(_, tag)| *tag)
        .max()
        .expect("at least one plane");
    assert_eq!(
        usize::from(highest) + 1,
        plane_count,
        "plane tags must be a gapless block from zero"
    );
}

/// The host decodes these codes into distinct diagnostics, so a code that
/// moves on one side only puts the wrong cause in the error message.
///
/// Exhaustive the same way the plane table is: a twelfth `CreateFailure`
/// cannot be added without naming it in `ts_create_failure_name`, or this does
/// not compile.
#[test]
fn parse_backend_ts_create_failures_match_rust() {
    let source = read();
    for (index, failure) in CreateFailure::ALL.iter().enumerate() {
        // One contiguous block from 0x8000_0001, so a variant missing from
        // `ALL` shortens the block instead of hiding.
        assert_eq!(*failure as u32, 0x8000_0001 + index as u32);
        assert_eq!(
            ts_create_failure(&source, ts_create_failure_name(*failure)),
            *failure as u32,
            "{failure:?}"
        );
    }

    // And the TypeScript table declares no code Rust does not raise.
    let declared = source
        .split("const EP_CREATE_FAILURE = {")
        .nth(1)
        .expect("EP_CREATE_FAILURE not declared")
        .split('}')
        .next()
        .expect("EP_CREATE_FAILURE is not closed")
        .lines()
        .filter(|line| line.contains(": '"))
        .count();
    assert_eq!(declared, CreateFailure::ALL.len());
}

#[test]
fn compact_diagnostic_stage_is_not_column() {
    // COLUMN is 4. Compact used to share that byte; a compact BAD_HEADER
    // then decoded as INVALID_JSON.
    assert_eq!(COMPACT_DIAGNOSTIC_STAGE, 6);
}
