//! A row of the MessagePack lane may hold a cell by its column's ordinal
//! (specs/columine/10-physical-vocabulary.md §7 "A row may hold a cell by its
//! column's ordinal"): the cell is written as a member's whose row names the
//! column is, and no row, name or schema is read.

use columine_arrow::{DictionarySlot, IndexWidth, SchemaDictionary, TextColumn};
use columine_parsing::json_extractor::ExtractionError;
use columine_parsing::msgpack_extractor::extract_msgpack_events_with_projections;
use columine_parsing::validate::ValueView;
use columine_parsing::{
    ArrowType, CellLayout, CellWriter, ColumnValue, DynamicColumns, ExtractionConfig,
    FrameProjections, PayloadDiscriminator, SignalSchemaField,
    build_extraction_config_with_semantic_schemas, read_cell,
};
use columine_types::category::NoCategories;
use columine_types::cells::{
    CellRefusal, CellTarget, RecordRow, TEXT_HASH_BYTES, TextPlace, ValueCells,
};

//#region smoo/columine!n/vocab-ordinal-cells
const ENCODING: &str = "test-cells";

/// The test embedder: a member's cell is `cell:` and its text, a text's hash
/// is its length in every byte, and `keep` is kept inline.
struct Cells;

impl ValueCells for Cells {
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
        out.extend(b"cell:");
        out.extend(value.as_str().ok_or(CellRefusal::Unsupported)?.as_bytes());
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
        value: &str,
        hash: &mut [u8; TEXT_HASH_BYTES],
    ) -> Result<TextPlace, CellRefusal> {
        *hash = [u8::try_from(value.len()).unwrap(); TEXT_HASH_BYTES];
        Ok(if value == "keep" {
            TextPlace::Inline
        } else {
            TextPlace::HashOnly
        })
    }

    fn text_by_hash(
        &mut self,
        _hash_column: u32,
        _hash: &[u8; TEXT_HASH_BYTES],
    ) -> Result<(), CellRefusal> {
        Ok(())
    }

    fn text_frame(
        &mut self,
        _hash_column: u32,
        _bytes: &[u8],
        _hash: &mut [u8; TEXT_HASH_BYTES],
        _inline: &mut Vec<u8>,
    ) -> Result<TextPlace, CellRefusal> {
        Err(CellRefusal::Unsupported)
    }
}

const NAMES: [&str; 12] = [
    "type",
    "value.n",
    "value.flag",
    "value.ratio",
    "value.kind",
    "value.note",
    "note_inline",
    "value.cell",
    "value.raw",
    "value.label",
    "value.$undeclared",
    "event_value_present.value.n",
];
const N: u8 = 1;
const FLAG: u8 = 2;
const RATIO: u8 = 3;
const KIND: u8 = 4;
const NOTE: u8 = 5;
const NOTE_INLINE: u8 = 6;
const CELL: u8 = 7;
const RAW: u8 = 8;
const LABEL: u8 = 9;
const CARRIER: u8 = 10;
const N_PRESENT: u8 = 11;

fn fields() -> [SignalSchemaField; 12] {
    let plain = |plane| SignalSchemaField::new(plane, true);
    [
        plain(ArrowType::Utf8),
        plain(ArrowType::Int64),
        plain(ArrowType::Bool),
        plain(ArrowType::Float64),
        SignalSchemaField::new_dictionary(DictionarySlot::Schema(0), IndexWidth::U8, true),
        SignalSchemaField::new_fixed_size_binary(32, true),
        plain(ArrowType::Utf8),
        plain(ArrowType::Binary),
        plain(ArrowType::Binary),
        plain(ArrowType::Utf8),
        plain(ArrowType::Binary),
        plain(ArrowType::Bool),
    ]
}

/// A processor of the columns above: with a discriminator it has no
/// placement row at all, so no member of any type has a column.
fn create(discriminated: bool) -> ExtractionConfig {
    let mut cell_metadata = vec![None; NAMES.len()];
    cell_metadata[usize::from(CELL)] = Some(ENCODING.to_owned());
    cell_metadata[usize::from(CARRIER)] = Some(ENCODING.to_owned());
    build_extraction_config_with_semantic_schemas(
        &fields(),
        &NAMES,
        &[SchemaDictionary {
            slot: DictionarySlot::Schema(0),
            arrow_ids: vec![0],
            values: vec![Some("a".to_owned()), Some("b".to_owned())],
        }],
        None,
        discriminated
            .then(|| PayloadDiscriminator {
                key: "type".into(),
                members: Vec::new(),
            })
            .as_ref(),
        CellLayout {
            cell_metadata: &cell_metadata,
            text_columns: &[TextColumn {
                hash: usize::from(NOTE),
                inline: Some(usize::from(NOTE_INLINE)),
            }],
            encoding: Some(ENCODING),
            ..CellLayout::default()
        },
    )
    .expect("every kind of cell")
}

/// A key of a row's map.
enum K<'a> {
    Str(&'a str),
    /// A positive fixint.
    Ord(u8),
    /// The same number as a `uint 8`.
    Ord8(u8),
    /// A `uint 64`.
    Ord64(u64),
    /// The negative fixint -1.
    Negative,
}

/// A MessagePack value of the test rows.
enum V<'a> {
    Nil,
    True,
    Int(u8),
    Float(f64),
    Str(&'a str),
    Bin(&'a [u8]),
    Map(Vec<(K<'a>, V<'a>)>),
}

fn short(len: usize) -> u8 {
    u8::try_from(len).ok().filter(|len| *len < 16).unwrap()
}

fn text(out: &mut Vec<u8>, text: &str) {
    out.push(0xa0 | short(text.len()));
    out.extend(text.as_bytes());
}

fn value(out: &mut Vec<u8>, v: &V<'_>) {
    match v {
        V::Nil => out.push(0xc0),
        V::True => out.push(0xc3),
        V::Int(n) => {
            assert!(*n < 0x80, "a positive fixint");
            out.push(*n);
        }
        V::Float(n) => {
            out.push(0xcb);
            out.extend(n.to_be_bytes());
        }
        V::Str(s) => text(out, s),
        V::Bin(bytes) => {
            out.extend([0xc4, short(bytes.len())]);
            out.extend(*bytes);
        }
        V::Map(entries) => {
            out.push(0x80 | short(entries.len()));
            for (key, v) in entries {
                match key {
                    K::Str(name) => text(out, name),
                    K::Ord(n) => {
                        assert!(*n < 0x80, "a positive fixint");
                        out.push(*n);
                    }
                    K::Ord8(n) => out.extend([0xcc, *n]),
                    K::Ord64(n) => {
                        out.push(0xcf);
                        out.extend(n.to_be_bytes());
                    }
                    K::Negative => out.push(0xff),
                }
                value(out, v);
            }
        }
    }
}

type Cell = Option<ColumnValue<'static>>;

/// The cells of columns `N` to `LABEL` in every row the lane stores from
/// `rows`, each row the members after its `type`.
fn store(
    config: &ExtractionConfig,
    rows: Vec<Vec<(K<'_>, V<'_>)>>,
) -> Result<Vec<[Cell; 9]>, ExtractionError> {
    let mut batch = vec![0x90 | short(rows.len())];
    for row in rows {
        let mut members = vec![(K::Str("type"), V::Str("order"))];
        members.extend(row);
        value(&mut batch, &V::Map(members));
    }
    let fields = fields();
    let mut columns = DynamicColumns::new(&fields, 8).expect("column plane fits");
    let stored = extract_msgpack_events_with_projections(
        &batch,
        config,
        &mut columns,
        &mut [0; 64],
        false,
        FrameProjections::default(),
        &mut NoCategories,
        &mut CellWriter::Embedder(&mut Cells),
        &mut Vec::new(),
    )?;
    Ok((0..stored)
        .map(|row| {
            [N, FLAG, RATIO, KIND, NOTE, NOTE_INLINE, CELL, RAW, LABEL]
                .map(|column| read_cell(&columns, usize::from(column), row))
        })
        .collect())
}

fn utf8(text: &str) -> Cell {
    Some(ColumnValue::Utf8(text.to_owned().into()))
}

fn bytes(bytes: &[u8]) -> Cell {
    Some(ColumnValue::Binary(bytes.to_vec().into()))
}

fn hash(byte: u8) -> Cell {
    Some(ColumnValue::FixedBytes(vec![byte; TEXT_HASH_BYTES].into()))
}

#[test]
fn a_pre_placed_row_lands_every_kind_of_cell_whether_or_not_a_row_names_its_column() {
    for discriminated in [false, true] {
        let stored = store(
            &create(discriminated),
            vec![
                vec![
                    (K::Ord(N), V::Int(7)),
                    (K::Ord(FLAG), V::True),
                    (K::Ord(RATIO), V::Float(1.5)),
                    (K::Ord(KIND), V::Str("b")),
                    (K::Ord(NOTE), V::Str("keep")),
                    (K::Ord(CELL), V::Str("x")),
                    (K::Ord8(RAW), V::Bin(&[1, 2])),
                    (K::Ord64(u64::from(LABEL)), V::Str("hi")),
                ],
                vec![(K::Ord(N), V::Nil), (K::Ord(NOTE), V::Str("dropped"))],
            ],
        );
        assert_eq!(
            stored,
            Ok(vec![
                [
                    Some(ColumnValue::Int(7)),
                    Some(ColumnValue::Bool(true)),
                    Some(ColumnValue::Float(1.5)),
                    Some(ColumnValue::UInt(1)),
                    hash(4),
                    utf8("keep"),
                    bytes(b"cell:x"),
                    bytes(&[1, 2]),
                    utf8("hi"),
                ],
                [None, None, None, None, hash(7), None, None, None, None],
            ]),
            "discriminated {discriminated}: every cell, then a nil, a text kept by hash and missing ordinals"
        );
    }
}

#[test]
fn a_cell_by_ordinal_takes_its_columns_coercion() {
    let config = create(true);
    assert_eq!(
        store(&config, vec![vec![(K::Ord(N), V::Str("seven"))]]),
        Err(ExtractionError::InvalidFieldType),
        "a text in an integer column"
    );
    assert_eq!(
        store(&config, vec![vec![(K::Ord(RAW), V::Str("text"))]]),
        Err(ExtractionError::InvalidFieldType),
        "a text in a raw cell"
    );
}

#[test]
fn an_ordinal_that_names_no_column_a_member_is_written_to_is_refused_by_its_number() {
    let config = create(true);
    for ordinal in [
        u64::try_from(NAMES.len()).unwrap(),
        u64::MAX,
        u64::from(NOTE_INLINE),
        u64::from(CARRIER),
        u64::from(N_PRESENT),
    ] {
        assert_eq!(
            store(&config, vec![vec![(K::Ord64(ordinal), V::Str("x"))]]),
            Err(ExtractionError::UnknownColumn { ordinal }),
        );
    }
}

#[test]
fn a_column_is_written_once_in_a_row_that_holds_an_ordinal() {
    // With no discriminator the key `value.n` reaches the column of its name.
    let config = create(false);
    for (row, why) in [
        (
            vec![(K::Ord(N), V::Int(7)), (K::Ord8(N), V::Int(8))],
            "two ordinals",
        ),
        (
            vec![(K::Ord(N), V::Int(7)), (K::Str("value.n"), V::Int(8))],
            "an ordinal, then a member placed there",
        ),
        (
            vec![(K::Str("value.n"), V::Int(8)), (K::Ord(N), V::Int(7))],
            "a member placed there, then an ordinal",
        ),
    ] {
        assert_eq!(
            store(&config, vec![row]),
            Err(ExtractionError::ColumnWrittenTwice {
                ordinal: usize::from(N)
            }),
            "{why}"
        );
    }
}

#[test]
fn an_ordinal_is_a_top_level_key_of_the_flat_spelling_and_an_unsigned_integer() {
    let config = create(false);
    let nested = || V::Map(vec![(K::Str("label"), V::Str("hi"))]);
    for (row, why) in [
        (
            vec![(K::Ord(N), V::Int(7)), (K::Str("value"), nested())],
            "an ordinal before a nested `value`",
        ),
        (
            vec![(K::Str("value"), nested()), (K::Ord(N), V::Int(7))],
            "an ordinal after a nested `value`",
        ),
        (
            vec![(K::Str("value"), V::Map(vec![(K::Ord(N), V::Int(7))]))],
            "an integer key inside `value`",
        ),
        (vec![(K::Negative, V::Int(7))], "a negative integer key"),
    ] {
        assert_eq!(
            store(&config, vec![row]),
            Err(ExtractionError::InvalidJson),
            "{why}"
        );
    }
}
//#endregion smoo/columine!n/vocab-ordinal-cells
