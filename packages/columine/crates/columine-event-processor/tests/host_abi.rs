//! Generates `src/event-processor-abi.generated.ts` from the Rust definitions of the event-processor ABI.
//!
//! The TypeScript host binds the wasm exports with the numbers the native decoder reads: the plane tags and the
//! typed arrays that carry each plane, the field descriptor's byte layout, the limits, the status and
//! creation-failure codes, and the caller-built batch header. Each is rendered here from the Rust item that
//! defines it and committed, so importing it needs no wasm and no host number is typed by hand. A Rust change
//! that is not regenerated fails here; `UPDATE_GENERATED=1` rewrites the file, as
//! `columine-types/tests/counts_fold_capacities.rs` does for the counts capacities.
//!
//! The design is `specs/columine/10-physical-vocabulary.md` section 9.

use columine_arrow::{
    ArrowType, CELL_ENCODING_KEY, DictionarySlot, FIELD_DESCRIPTOR_BYTES, FIELD_IDENTITY_KEY,
    IndexWidth, MAX_EVENTS_PER_BATCH, MAX_FIXED_SIZE_BINARY_WIDTH, MAX_SCHEMA_FIELDS,
    MAX_SCHEMA_MESSAGE_BYTES, MAX_VALUE_BYTES, MIN_ARROW_OUTPUT_CAPACITY, PlaneKind,
    SignalSchemaField, TIME_UNITS, TIMESTAMP_ZONE, TimeUnit, time_unit_code,
};
use columine_event_processor::{
    COMPACT_ABI_VERSION, COMPACT_BATCH_MAGIC, COMPACT_DESCRIPTOR_SIZE, COMPACT_DIAGNOSTIC_STAGE,
    COMPACT_HEADER_SIZE, CreateFailure, InputFormat, RESULT_HEADER_SIZE, ResultCode,
    WASM_EVENT_CAPACITY, diagnostic_stage,
};

//#region smoo/columine!n/vocab-host-table

/// The generated file as committed, compiled in: Nx keys the test on a file a literal `include_str!` names
/// (packages/nx-plugin/README.md), not on one the test opens at run time.
const COMMITTED: &str = include_str!("../../../src/event-processor-abi.generated.ts");

/// Where `UPDATE_GENERATED=1` writes the file, read at run time: a workspace runs the test binary it
/// inherited from the checkout that compiled it, and must write its own tree.
fn generated_path() -> String {
    let manifest = std::env::var("CARGO_MANIFEST_DIR")
        .expect("cargo sets CARGO_MANIFEST_DIR for the tests it runs");
    format!("{manifest}/../../src/event-processor-abi.generated.ts")
}

/// The formatter's line width. An object that fits it is rendered on one line and any other one entry per
/// line; the formatter keeps either shape, so the committed file is stable under it.
const LINE_WIDTH: usize = 120;

fn wide(value: usize) -> u64 {
    u64::try_from(value).expect("a usize fits in a u64")
}

/// A 32-bit code in the grouped hexadecimal the Rust source writes it in.
fn hex32(value: u32) -> String {
    format!("0x{:04x}_{:04x}", value >> 16, value & 0xffff)
}

fn quoted(text: &str) -> String {
    assert!(
        !text.contains(['\'', '\\', '\n']),
        "{text:?} cannot be a TypeScript string literal without escapes"
    );
    format!("'{text}'")
}

/// `head{ key: value, … }tail` at `indent`: on one line when that fits the line width, one entry per
/// line otherwise.
fn object(indent: usize, head: &str, entries: &[(&str, String)], tail: &str) -> String {
    let pad = " ".repeat(indent);
    let inline = entries
        .iter()
        .map(|(key, value)| format!("{key}: {value}"))
        .collect::<Vec<_>>()
        .join(", ");
    let one_line = format!("{pad}{head}{{ {inline} }}{tail}");
    if one_line.chars().count() <= LINE_WIDTH {
        return one_line;
    }
    let body: String = entries
        .iter()
        .map(|(key, value)| format!("{pad}  {key}: {value},\n"))
        .collect();
    format!("{pad}{head}{{\n{body}{pad}}}{tail}")
}

/// A JSDoc comment, ending in a newline.
fn doc(lines: &[&str]) -> String {
    if let [only] = lines {
        return format!("/** {only} */\n");
    }
    let body: String = lines.iter().map(|line| format!(" * {line}\n")).collect();
    format!("/**\n{body} */\n")
}

/// `export const NAME = value;`
fn constant(name: &str, value: impl std::fmt::Display) -> String {
    format!("export const {name} = {value};")
}

fn carrier(name: Option<&str>) -> String {
    name.map_or_else(|| "null".to_owned(), quoted)
}

/// A field of `plane`, nullable. `FixedSizeBinary` needs a width to have a layout.
fn field_of(plane: ArrowType) -> SignalSchemaField {
    if plane == ArrowType::FixedSizeBinary {
        SignalSchemaField::new_fixed_size_binary(1, true)
    } else {
        SignalSchemaField::new(plane, true)
    }
}

/// The typed array a plane's data buffer is read through, `None` for a plane with no values.
fn data_carrier(plane: ArrowType, kind: PlaneKind) -> Option<&'static str> {
    match kind {
        PlaneKind::Empty => None,
        PlaneKind::Bool => Some("Uint8Array"),
        PlaneKind::SignedInt { width } => Some(match width {
            1 => "Int8Array",
            2 => "Int16Array",
            4 => "Int32Array",
            8 => "BigInt64Array",
            other => panic!("no typed array carries a {other}-byte signed integer"),
        }),
        PlaneKind::UnsignedInt { width } => Some(match width {
            1 => "Uint8Array",
            2 => "Uint16Array",
            4 => "Uint32Array",
            8 => "BigUint64Array",
            other => panic!("no typed array carries a {other}-byte unsigned integer"),
        }),
        // The language has no half-precision array, so the half-width plane is carried as raw 16-bit words.
        PlaneKind::Float { width } => Some(match width {
            2 => "Uint16Array",
            4 => "Float32Array",
            8 => "Float64Array",
            other => panic!("no typed array carries a {other}-byte float"),
        }),
        PlaneKind::FixedBytes { .. } => Some(if plane == ArrowType::IntervalDayTime {
            // Two little-endian i32 per row (days, then milliseconds): its elements are scalars of the
            // host's Int32Array, not opaque bytes like the other fixed-size planes.
            "Int32Array"
        } else {
            "Uint8Array"
        }),
        PlaneKind::Text { .. } | PlaneKind::Bytes { .. } => Some("Uint8Array"),
    }
}

/// The typed array a plane's offsets buffer is read through, `None` for a plane without one.
fn offsets_carrier(kind: PlaneKind) -> Option<&'static str> {
    kind.offset_width().map(|width| match width {
        4 => "Uint32Array",
        8 => "BigInt64Array",
        other => panic!("no typed array carries {other}-byte offsets"),
    })
}

fn plane_row(plane: ArrowType) -> String {
    let kind = field_of(plane).value_kind();
    let validity = (kind != PlaneKind::Empty).then_some("Uint8Array");
    let fields = [
        ("kind", quoted(plane.ts_kind())),
        ("variant", quoted(&format!("{plane:?}"))),
        ("tag", (plane as u8).to_string()),
        ("data", carrier(data_carrier(plane, kind))),
        ("offsets", carrier(offsets_carrier(kind))),
        ("validity", carrier(validity)),
    ];
    object(2, "", &fields, ",")
}

fn plane_table() -> String {
    let rows: String = ArrowType::ALL
        .iter()
        .map(|plane| format!("{}\n", plane_row(*plane)))
        .collect();
    format!("export const ARROW_PLANES = [\n{rows}] as const;")
}

/// Where each field of a descriptor sits in its wire bytes.
struct DescriptorLayout {
    plane: usize,
    nullable: usize,
    type_param: usize,
    dictionary: usize,
    index_plane: usize,
    reserved: usize,
}

/// A 16-bit value whose two bytes differ from each other and from zero.
const PROBE: u16 = 0x0301;

fn changed_bytes(base: SignalSchemaField, probe: SignalSchemaField) -> Vec<usize> {
    let (base, probe) = (base.to_wire(), probe.to_wire());
    (0..FIELD_DESCRIPTOR_BYTES)
        .filter(|&index| base[index] != probe[index])
        .collect()
}

/// The one byte `probe` changes against `base`, which must hold `expected` in `probe`.
fn byte_offset(
    base: SignalSchemaField,
    probe: SignalSchemaField,
    expected: u8,
    field: &str,
) -> usize {
    let changed = changed_bytes(base, probe);
    let &[offset] = changed.as_slice() else {
        panic!("{field}: probing changed bytes {changed:?}, expected exactly one");
    };
    assert_eq!(probe.to_wire()[offset], expected, "{field} byte");
    offset
}

/// The two bytes `probe` changes against `base`, which must hold `PROBE` little-endian in `probe`.
fn le16_offset(base: SignalSchemaField, probe: SignalSchemaField, field: &str) -> usize {
    let changed = changed_bytes(base, probe);
    let &[low, high] = changed.as_slice() else {
        panic!("{field}: probing changed bytes {changed:?}, expected exactly two");
    };
    assert_eq!(high, low + 1, "{field} is two adjacent bytes");
    let wire = probe.to_wire();
    assert_eq!(
        u16::from_le_bytes([wire[low], wire[high]]),
        PROBE,
        "{field} is a little-endian u16"
    );
    low
}

/// Encodes probe descriptors with [`SignalSchemaField::to_wire`] and locates the bytes that change, so the
/// encoder is the only place the layout is written.
fn descriptor_layout() -> DescriptorLayout {
    let int32 = SignalSchemaField::new(ArrowType::Int32, false);
    let float64 = SignalSchemaField::new(ArrowType::Float64, false);
    let nullable = SignalSchemaField::new(ArrowType::Int32, true);
    let plane = byte_offset(int32, float64, float64.arrow_type as u8, "plane");
    let nullable = byte_offset(int32, nullable, nullable.nullable, "nullable");
    let type_param = le16_offset(
        SignalSchemaField::new_fixed_size_binary(0, false),
        SignalSchemaField::new_fixed_size_binary(PROBE, false),
        "typeParam",
    );
    let schema_dictionary = |position: u16| {
        SignalSchemaField::new_dictionary(DictionarySlot::Schema(position), IndexWidth::U8, false)
    };
    let dictionary = le16_offset(
        schema_dictionary(0),
        schema_dictionary(PROBE - DictionarySlot::Schema(0).wire()),
        "dictionary",
    );
    let stream_dictionary =
        |width: IndexWidth| SignalSchemaField::new_dictionary(DictionarySlot::Stream, width, false);
    let index_plane = byte_offset(
        stream_dictionary(IndexWidth::U8),
        stream_dictionary(IndexWidth::U16),
        IndexWidth::U16.plane() as u8,
        "indexPlane",
    );
    let described = [
        plane,
        nullable,
        type_param,
        type_param + 1,
        dictionary,
        dictionary + 1,
        index_plane,
    ];
    let undescribed: Vec<usize> = (0..FIELD_DESCRIPTOR_BYTES)
        .filter(|index| !described.contains(index))
        .collect();
    let &[reserved] = undescribed.as_slice() else {
        panic!(
            "descriptor bytes {undescribed:?} belong to no field, expected exactly one reserved byte"
        );
    };
    DescriptorLayout {
        plane,
        nullable,
        type_param,
        dictionary,
        index_plane,
        reserved,
    }
}

fn time_unit_name(unit: TimeUnit) -> &'static str {
    match unit {
        TimeUnit::Second => "s",
        TimeUnit::Millisecond => "ms",
        TimeUnit::Microsecond => "us",
        TimeUnit::Nanosecond => "ns",
    }
}

/// Names the variants of an ABI enum once. The list and the `match` come from the same tokens, and the
/// `match` has no wildcard arm, so a variant added to the enum does not compile until it is named here.
macro_rules! named_variants {
    ($ty:ident, $all:ident, $name_of:ident { $($variant:ident => $name:literal,)+ }) => {
        const $all: &[$ty] = &[$($ty::$variant),+];

        fn $name_of(value: $ty) -> &'static str {
            match value {
                $($ty::$variant => $name),+
            }
        }
    };
}

named_variants!(ResultCode, RESULT_CODES, result_code_name {
    Ok => "OK",
    InvalidHandle => "INVALID_HANDLE",
    ParseError => "PARSE_ERROR",
    EncodeError => "ENCODE_ERROR",
    OutputTooSmall => "OUTPUT_TOO_SMALL",
    InvalidFormat => "INVALID_FORMAT",
    InvalidInput => "INVALID_INPUT",
    SchemaMismatch => "SCHEMA_MISMATCH",
    MemoryExhausted => "MEMORY_EXHAUSTED",
});

named_variants!(InputFormat, INPUT_FORMATS, input_format_name {
    Json => "JSON",
    Msgpack => "MSGPACK",
    ArrowPassthrough => "ARROW_PASSTHROUGH",
    MsgpackStream => "MSGPACK_STREAM",
});

/// The name of one handle-creation failure. `CreateFailure::ALL` lists the variants, and this `match` has no
/// wildcard arm, so a variant added to the enum does not compile until it is named here.
fn create_failure_name(failure: CreateFailure) -> &'static str {
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
        CreateFailure::MemoryExhausted => "MEMORY_EXHAUSTED",
        CreateFailure::SchemaTooLarge => "SCHEMA_TOO_LARGE",
        CreateFailure::SchemaDictionary => "SCHEMA_DICTIONARY",
        CreateFailure::SchemaFieldIdentity => "SCHEMA_FIELD_IDENTITY",
    }
}

fn render() -> String {
    let layout = descriptor_layout();
    let mut blocks: Vec<String> = vec![
        "// Generated from the Rust definitions of the event-processor ABI (crates/columine-arrow/src/schema.rs:
// ArrowType, SignalSchemaField, DictionarySlot, IndexWidth, the time units and the limits;
// crates/columine-event-processor/src/lib.rs and compact.rs: ResultCode, InputFormat, CreateFailure and the compact
// batch constants) by crates/columine-event-processor/tests/host_abi.rs; rerun that test with UPDATE_GENERATED=1
// instead of editing."
            .to_owned(),
    ];

    blocks.push(
        doc(&[
            "Every Arrow physical plane, ascending by tag: the `ArrowType` table of columine-arrow. `data`, `offsets` and",
            "`validity` name the typed array that carries each buffer of the plane, derived from its layout.",
        ]) + &plane_table(),
    );

    blocks.push(
        doc(&["Bytes of one field descriptor."])
            + &constant("FIELD_DESCRIPTOR_BYTES", FIELD_DESCRIPTOR_BYTES),
    );
    blocks.push(
        doc(&[
            "Byte offset of each field of a field descriptor, found by encoding probe descriptors with",
            "`SignalSchemaField::to_wire`. `typeParam` and `dictionary` are little-endian u16; `reserved` is zero.",
        ]) + &object(
            0,
            "export const FIELD_DESCRIPTOR = ",
            &[
                ("plane", layout.plane.to_string()),
                ("nullable", layout.nullable.to_string()),
                ("typeParam", layout.type_param.to_string()),
                ("dictionary", layout.dictionary.to_string()),
                ("indexPlane", layout.index_plane.to_string()),
                ("reserved", layout.reserved.to_string()),
            ],
            " as const;",
        ),
    );

    let units: Vec<(&str, String)> = TIME_UNITS
        .iter()
        .map(|unit| (time_unit_name(*unit), time_unit_code(*unit).to_string()))
        .collect();
    blocks.push(
        doc(&["The `typeParam` of a Timestamp or Duration plane: the unit its values count."])
            + &object(0, "export const TIME_UNIT = ", &units, " as const;"),
    );
    blocks.push(
        doc(&["The time zone every Timestamp column declares."])
            + &constant("TIMESTAMP_ZONE", quoted(TIMESTAMP_ZONE)),
    );

    let plain = SignalSchemaField::new(ArrowType::Utf8, false).to_wire();
    let none = u16::from_le_bytes([plain[layout.dictionary], plain[layout.dictionary + 1]]);
    assert_eq!(
        DictionarySlot::from_wire(none),
        None,
        "a plain column's dictionary value names no slot"
    );
    blocks.push(
        doc(&[
            "The `dictionary` of a descriptor: `none` for a plain column, `stream` for the schema's stream dictionary,",
            "and `firstSchema + k` for schema dictionary `k` of the creation call's dictionary table.",
        ]) + &object(
            0,
            "export const DICTIONARY_SLOT = ",
            &[
                ("none", none.to_string()),
                ("stream", DictionarySlot::Stream.wire().to_string()),
                ("firstSchema", DictionarySlot::Schema(0).wire().to_string()),
            ],
            " as const;",
        ),
    );

    // The planes an index width names, from the plane table: a width added to `IndexWidth` is picked up.
    let index_planes: Vec<String> = ArrowType::ALL
        .iter()
        .filter_map(|plane| {
            let width = IndexWidth::from_plane(*plane)?;
            assert_eq!(width.plane(), *plane, "an index width names its own plane");
            Some((*plane as u8).to_string())
        })
        .collect();
    blocks.push(
        doc(&[
            "The planes a dictionary-encoded field stores its indexes in: its `indexPlane`, and the plane of the",
            "column a caller supplies for it.",
        ]) + &constant(
            "DICTIONARY_INDEX_PLANES",
            format!("[{}] as const", index_planes.join(", ")),
        ),
    );

    blocks.push(
        doc(&["Arrow field metadata key under which a column names the declared fields it holds."])
            + &constant("FIELD_IDENTITY_KEY", quoted(FIELD_IDENTITY_KEY)),
    );
    blocks.push(
        doc(&[
            "Arrow field metadata key under which a Binary column names the encoding of its cells.",
        ]) + &constant("CELL_ENCODING_KEY", quoted(CELL_ENCODING_KEY)),
    );

    let limits: [(&str, &str, u64); 8] = [
        (
            "MAX_SCHEMA_FIELDS",
            "Physical Arrow fields in one flattened schema.",
            wide(MAX_SCHEMA_FIELDS),
        ),
        (
            "MAX_EVENTS_PER_BATCH",
            "Ceiling for a batch's row count: a validation limit, nothing is allocated for it.",
            u64::from(MAX_EVENTS_PER_BATCH),
        ),
        (
            "MAX_VALUE_BYTES",
            "Variable-width data bytes one column holds.",
            u64::from(MAX_VALUE_BYTES),
        ),
        (
            "MAX_SCHEMA_MESSAGE_BYTES",
            "Longest Arrow IPC Schema message a handle accepts.",
            wide(MAX_SCHEMA_MESSAGE_BYTES),
        ),
        (
            "MAX_FIXED_SIZE_BINARY_WIDTH",
            "Widest FixedSizeBinary field, in bytes per row.",
            u64::from(MAX_FIXED_SIZE_BINARY_WIDTH),
        ),
        (
            "MIN_ARROW_OUTPUT_CAPACITY",
            "Fewest Arrow output bytes the encoder is given, however short the stream.",
            wide(MIN_ARROW_OUTPUT_CAPACITY),
        ),
        (
            "WASM_EVENT_CAPACITY",
            "Event capacity of every `ep_create_*` handle: an allocation size, not the row ceiling.",
            u64::from(WASM_EVENT_CAPACITY),
        ),
        (
            "RESULT_HEADER_SIZE",
            "Bytes of the result header that precede the Arrow bytes of every output.",
            wide(RESULT_HEADER_SIZE),
        ),
    ];
    for (name, meaning, value) in limits {
        blocks.push(doc(&[meaning]) + &constant(name, value));
    }

    blocks.push(
        doc(&[
            "The caller-built batch header (`CPB1`): its magic, version, header and descriptor sizes, and the",
            "diagnostic stage its refusals report.",
        ]) + &object(
            0,
            "export const COMPACT = ",
            &[
                ("magic", hex32(COMPACT_BATCH_MAGIC)),
                ("version", COMPACT_ABI_VERSION.to_string()),
                ("headerSize", COMPACT_HEADER_SIZE.to_string()),
                ("descriptorSize", COMPACT_DESCRIPTOR_SIZE.to_string()),
                ("diagnosticStage", COMPACT_DIAGNOSTIC_STAGE.to_string()),
            ],
            " as const;",
        ),
    );

    let results: Vec<(&str, String)> = RESULT_CODES
        .iter()
        .map(|code| (result_code_name(*code), (*code as u32).to_string()))
        .collect();
    blocks.push(
        doc(&["Status codes of the `ep_*` exports, which also head every result."])
            + &object(0, "export const RESULT_CODE = ", &results, " as const;"),
    );

    let formats: Vec<(&str, String)> = INPUT_FORMATS
        .iter()
        .map(|input| (input_format_name(*input), (*input as u8).to_string()))
        .collect();
    blocks.push(
        doc(&["Input formats `ep_create_log_entry` reads."])
            + &object(0, "export const INPUT_FORMAT = ", &formats, " as const;"),
    );

    let failures: Vec<(&str, String)> = CreateFailure::ALL
        .iter()
        .map(|failure| (create_failure_name(*failure), hex32(*failure as u32)))
        .collect();
    blocks.push(
        doc(&[
            "Why an `ep_create_*` call produced no handle. A handle is a small positive integer, so any other return",
            "value is one of these codes, read as unsigned.",
        ]) + &object(0, "export const CREATE_FAILURE = ", &failures, " as const;"),
    );

    blocks.join("\n\n") + "\n"
}

#[test]
fn typescript_abi_table_is_generated_from_the_native_definitions() {
    let rendered = render();
    if std::env::var("UPDATE_GENERATED").as_deref() == Ok("1") {
        if COMMITTED != rendered {
            std::fs::write(generated_path(), &rendered).expect("write the generated ABI table");
        }
        return;
    }
    assert_eq!(
        COMMITTED, rendered,
        "src/event-processor-abi.generated.ts is not generated from the Rust ABI definitions; rerun with UPDATE_GENERATED=1"
    );
}

//#endregion smoo/columine!n/vocab-host-table

//#region smoo/columine!n/vocab-host-table

/// Two planes claiming one TypeScript kind would let the host decode one as the other. The enum cannot
/// name a variant twice, but the kind names can collide.
#[test]
fn two_planes_never_share_a_typescript_kind() {
    let mut kinds: Vec<&str> = ArrowType::ALL.iter().map(|plane| plane.ts_kind()).collect();
    kinds.sort_unstable();
    kinds.dedup();
    assert_eq!(kinds.len(), ArrowType::ALL.len());
}

/// The wasm and every persisted fixture index by tag, so a hole means a plane was deleted rather than
/// appended, and `ALL` listing them out of order means the table is not ascending.
#[test]
fn plane_tags_are_a_gapless_block_from_zero() {
    for (index, plane) in ArrowType::ALL.iter().enumerate() {
        assert_eq!(usize::from(*plane as u8), index, "{plane:?}");
    }
}

/// One contiguous block, so a variant missing from `ALL` shortens the block instead of hiding.
#[test]
fn creation_failure_codes_are_one_contiguous_block() {
    let first = CreateFailure::ALL[0] as u32;
    for (index, failure) in CreateFailure::ALL.iter().enumerate() {
        let index = u32::try_from(index).expect("a handful of failures");
        assert_eq!(*failure as u32, first + index, "{failure:?}");
    }
}

/// Distinct quantities: a row ceiling and an allocation size. Sharing one number made every handle
/// allocate the whole 65536-row column plane.
#[test]
fn the_handle_allocation_is_not_the_row_ceiling() {
    assert_ne!(WASM_EVENT_CAPACITY, MAX_EVENTS_PER_BATCH);
}

/// A compact `BAD_HEADER` that shared the column stage's byte decoded as an invalid JSON document.
#[test]
fn compact_diagnostic_stage_is_not_column() {
    assert_ne!(COMPACT_DIAGNOSTIC_STAGE, diagnostic_stage::COLUMN);
}

//#endregion smoo/columine!n/vocab-host-table
