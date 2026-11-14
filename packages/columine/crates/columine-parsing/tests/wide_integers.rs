//! An integer anywhere in `-(2^63) ..= 2^64 - 1` reaches a dynamic cell's
//! seam exactly through the value view, on both lanes and in both payload
//! spellings, whether the wire spells it as a MessagePack integer marker, the
//! bigint extension or a JSON integer literal; an unsigned 64-bit column
//! takes the same values (specs/columine/10-physical-vocabulary.md §7, §8).

use columine_parsing::json_extractor::{ExtractionDiagnostic, extract_json_events};
use columine_parsing::msgpack_extractor::extract_msgpack_events_with_projections;
use columine_parsing::validate::{NumberValue, ValueKind, ValueView};
use columine_parsing::{
    ArrowType, CellLayout, CellWriter, ColumnValue, DynamicColumns, ExtractionConfig,
    FrameProjections, SignalSchemaField, build_extraction_config_with_semantic_schemas, read_cell,
};
use columine_types::category::NoCategories;
use columine_types::cells::{
    CellRefusal, CellTarget, RecordRow, TEXT_HASH_BYTES, TextPlace, ValueCells,
};

//#region smoo/columine!n/vocab-value-view
const ENCODING: &str = "test-cells";

/// The test embedder: a member cell is the value's kind and the exact
/// number its view answers.
struct NumberCells;

impl ValueCells for NumberCells {
    fn encoding(&self) -> &'static str {
        ENCODING
    }

    fn prepare(&mut self, _target: CellTarget<'_>) -> Result<(), CellRefusal> {
        Ok(())
    }

    fn member(
        &mut self,
        _column: u32,
        value: &dyn ValueView,
        out: &mut Vec<u8>,
    ) -> Result<(), CellRefusal> {
        let number = match value.as_number() {
            Some(NumberValue::Signed(number)) => format!("i {number}"),
            Some(NumberValue::Unsigned(number)) => format!("u {number}"),
            Some(NumberValue::Float(number)) => format!("f {number}"),
            None => "none".to_owned(),
        };
        let kind = match value.kind() {
            ValueKind::Number => "number",
            ValueKind::BigInt => "bigint",
            other => other.as_str(),
        };
        out.extend(format!("{kind} {number}").bytes());
        Ok(())
    }

    fn begin_record(&mut self) {}

    fn put(&mut self, _row: &RecordRow<'_>, _value: &dyn ValueView) -> Result<(), CellRefusal> {
        Err(CellRefusal::Unsupported)
    }

    fn finish_record(&mut self, _out: &mut Vec<u8>) -> Result<bool, CellRefusal> {
        Ok(false)
    }

    fn begin_undeclared(&mut self) {}

    fn put_undeclared(&mut self, _path: &str, _value: &dyn ValueView) -> Result<(), CellRefusal> {
        Err(CellRefusal::Unsupported)
    }

    fn finish_undeclared(&mut self, _out: &mut Vec<u8>) -> Result<bool, CellRefusal> {
        Ok(false)
    }

    fn text(
        &mut self,
        _hash_column: u32,
        _value: &str,
        _hash: &mut [u8; TEXT_HASH_BYTES],
    ) -> Result<TextPlace, CellRefusal> {
        Err(CellRefusal::Unsupported)
    }

    fn text_by_hash(
        &mut self,
        _hash_column: u32,
        _hash: &[u8; TEXT_HASH_BYTES],
    ) -> Result<(), CellRefusal> {
        Err(CellRefusal::Unsupported)
    }
}

/// `type`, the dynamic member cell `value.data` and the unsigned 64-bit
/// column `value.count`.
fn config() -> ([SignalSchemaField; 3], ExtractionConfig) {
    let fields = [
        SignalSchemaField::new(ArrowType::Utf8, true),
        SignalSchemaField::new(ArrowType::Binary, true),
        SignalSchemaField::new(ArrowType::UInt64, true),
    ];
    let config = build_extraction_config_with_semantic_schemas(
        &fields,
        &["type", "value.data", "value.count"],
        &[],
        None,
        None,
        CellLayout {
            cell_metadata: &[None, Some(ENCODING.to_owned()), None],
            dynamic_columns: &[1],
            encoding: Some(ENCODING),
            ..CellLayout::default()
        },
    )
    .unwrap();
    (fields, config)
}

/// Every row's `column`, as text: \`-\` is the empty cell.
fn column_text(columns: &DynamicColumns, rows: usize, column: usize) -> Vec<String> {
    (0..rows)
        .map(|row| match read_cell(columns, column, row) {
            None => "-".to_owned(),
            Some(ColumnValue::Binary(bytes)) => String::from_utf8_lossy(&bytes).into_owned(),
            Some(ColumnValue::UInt(number)) => number.to_string(),
            Some(other) => format!("{other:?}"),
        })
        .collect()
}

fn json(input: &[u8], column: usize) -> Vec<String> {
    let (fields, config) = config();
    let mut columns = DynamicColumns::new(&fields, 16).expect("column plane fits");
    let rows = extract_json_events(
        input,
        &config,
        &mut columns,
        &mut [0; 256],
        &mut ExtractionDiagnostic::default(),
        &mut NoCategories,
        &mut CellWriter::Embedder(&mut NumberCells),
        &mut Vec::new(),
    )
    .expect("the JSON batch is accepted");
    column_text(&columns, rows, column)
}

fn msgpack(input: &[u8], column: usize) -> Vec<String> {
    let (fields, config) = config();
    let mut columns = DynamicColumns::new(&fields, 16).expect("column plane fits");
    let rows = extract_msgpack_events_with_projections(
        input,
        &config,
        &mut columns,
        &mut [0; 256],
        false,
        FrameProjections::default(),
        &mut NoCategories,
        &mut CellWriter::Embedder(&mut NumberCells),
        &mut Vec::new(),
    )
    .expect("the MessagePack batch is accepted");
    column_text(&columns, rows, column)
}

/// The MessagePack spellings of the test integers.
const UINT64_2_63: &[u8] = &[0xcf, 0x80, 0, 0, 0, 0, 0, 0, 0];
const UINT64_MAX: &[u8] = &[0xcf, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff];
const INT64_MIN: &[u8] = &[0xd3, 0x80, 0, 0, 0, 0, 0, 0, 0];
/// The bigint extension (type 20: a sign byte, then the big-endian magnitude).
const BIGINT_2_63: &[u8] = &[0xc7, 9, 20, 0, 0x80, 0, 0, 0, 0, 0, 0, 0];
const BIGINT_U64_MAX: &[u8] = &[
    0xc7, 9, 20, 0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
];
const BIGINT_I64_MIN: &[u8] = &[0xc7, 9, 20, 1, 0x80, 0, 0, 0, 0, 0, 0, 0];
const BIGINT_2_64: &[u8] = &[0xc7, 10, 20, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0];

/// One MessagePack event per value, each spelled nested and then flat under
/// \`member\`.
fn msgpack_events(member: &str, values: &[&[u8]]) -> Vec<u8> {
    let mut out = vec![0xdc];
    out.extend(u16::try_from(values.len() * 2).unwrap().to_be_bytes());
    let text = |out: &mut Vec<u8>, text: &str| {
        out.push(0xa0 | u8::try_from(text.len()).unwrap());
        out.extend(text.as_bytes());
    };
    for value in values {
        out.push(0x82);
        text(&mut out, "type");
        text(&mut out, "n");
        text(&mut out, "value");
        out.push(0x81);
        text(&mut out, member);
        out.extend(*value);
        out.push(0x82);
        text(&mut out, "type");
        text(&mut out, "n");
        text(&mut out, &format!("value.{member}"));
        out.extend(*value);
    }
    out
}

/// One JSON event per literal, each spelled nested and then flat under
/// \`member\`.
fn json_events(member: &str, literals: &[&str]) -> Vec<u8> {
    let events: Vec<String> = literals
        .iter()
        .flat_map(|literal| {
            [
                format!(r#"{{"type":"n","value":{{"{member}":{literal}}}}}"#),
                format!(r#"{{"type":"n","value.{member}":{literal}}}"#),
            ]
        })
        .collect();
    format!("[{}]", events.join(",")).into_bytes()
}

/// Each expected cell twice: once per spelling.
fn twice(cells: &[&str]) -> Vec<String> {
    cells
        .iter()
        .flat_map(|cell| [cell.to_string(), cell.to_string()])
        .collect()
}

#[test]
fn a_dynamic_member_reads_back_every_64_bit_integer_exactly_on_both_lanes() {
    assert_eq!(
        msgpack(
            &msgpack_events(
                "data",
                &[
                    UINT64_2_63,
                    UINT64_MAX,
                    INT64_MIN,
                    BIGINT_2_63,
                    BIGINT_U64_MAX,
                    BIGINT_I64_MIN
                ]
            ),
            1
        ),
        twice(&[
            "number u 9223372036854775808",
            "number u 18446744073709551615",
            "number i -9223372036854775808",
            "bigint u 9223372036854775808",
            "bigint u 18446744073709551615",
            "bigint i -9223372036854775808",
        ]),
        "MessagePack: integer markers, then the bigint extension"
    );
    assert_eq!(
        json(
            &json_events(
                "data",
                &[
                    "9223372036854775808",
                    "18446744073709551615",
                    "-9223372036854775808"
                ]
            ),
            1
        ),
        twice(&[
            "number u 9223372036854775808",
            "number u 18446744073709551615",
            "number i -9223372036854775808",
        ]),
        "JSON integer literals"
    );
}

#[test]
fn the_bigint_extension_past_64_bits_still_answers_no_number() {
    assert_eq!(
        msgpack(&msgpack_events("data", &[BIGINT_2_64]), 1),
        twice(&["bigint none"])
    );
}

#[test]
fn an_unsigned_64_bit_column_takes_every_unsigned_64_bit_integer_on_both_lanes() {
    assert_eq!(
        msgpack(
            &msgpack_events(
                "count",
                &[UINT64_2_63, UINT64_MAX, BIGINT_2_63, BIGINT_U64_MAX]
            ),
            2
        ),
        twice(&[
            "9223372036854775808",
            "18446744073709551615",
            "9223372036854775808",
            "18446744073709551615",
        ])
    );
    assert_eq!(
        json(
            &json_events("count", &["9223372036854775808", "18446744073709551615"]),
            2
        ),
        twice(&["9223372036854775808", "18446744073709551615"])
    );
}
//#endregion smoo/columine!n/vocab-value-view
