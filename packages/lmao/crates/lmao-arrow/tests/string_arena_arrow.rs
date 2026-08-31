//! Independent pair checks for string-column Arrow output.
//!
//! Category and text are different schema strategies. A string column that is
//! *nearly* right — dictionary keys off by one, UTF-8 offsets shifted after a
//! realloc, static templates copied into the values buffer — is the defect that
//! surfaces months later in a query. These tests decode every cell.

use arrow_array::cast::AsArray;
use arrow_array::types::UInt32Type;
use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::DataType;
use lmao_arrow::{StableVocabularyCatalog, convert_thread_buffer};
use lmao_core::{
    ColumnValue, FieldMeta, FieldStrategy, SYSTEM_COLUMN_COUNT, TextInput, ThreadSpanBuffer,
    TraceId,
};

static FIELDS: &[FieldMeta] = &[
    FieldMeta::new("label", FieldStrategy::Category),
    FieldMeta::new("note", FieldStrategy::Text),
];

const CATEGORY: u16 = SYSTEM_COLUMN_COUNT as u16;
const TEXT: u16 = CATEGORY + 1;

fn trace() -> TraceId {
    TraceId::new("arena-arrow").unwrap()
}

fn empty_catalog() -> StableVocabularyCatalog<'static> {
    StableVocabularyCatalog::EMPTY
}

fn batch(buffer: &mut ThreadSpanBuffer) -> RecordBatch {
    let rows = buffer.row_count();
    convert_thread_buffer(buffer, &empty_catalog(), 0, rows, 99).unwrap()
}

fn decoded_utf8(column: &dyn Array, row: usize) -> Option<&str> {
    if !column.is_valid(row) {
        return None;
    }
    match column.data_type() {
        DataType::Utf8 => Some(column.as_string::<i32>().value(row)),
        DataType::Dictionary(key, value)
            if **key == DataType::UInt32 && **value == DataType::Utf8 =>
        {
            let dictionary = column.as_dictionary::<UInt32Type>();
            let values = dictionary
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("utf8 dictionary values");
            Some(values.value(dictionary.keys().value(row) as usize))
        }
        other => panic!("string column has unexpected type {other:?}"),
    }
}

fn populate(values: &[(&str, &str)]) -> ThreadSpanBuffer {
    let mut buffer = ThreadSpanBuffer::new(7, 64, FIELDS);
    for (i, (label, note)) in values.iter().enumerate() {
        let span = buffer
            .open_span(trace(), 0, 0, TextInput::Static("span"), i as i64, 1)
            .unwrap();
        let label_id = buffer.intern(label).unwrap();
        let note_id = buffer.intern(note).unwrap();
        buffer
            .write_tag(span, CATEGORY, ColumnValue::Text(label_id))
            .unwrap();
        buffer
            .write_tag(span, TEXT, ColumnValue::Text(note_id))
            .unwrap();
        buffer.end_ok(span, i as i64 + 1).unwrap();
    }
    buffer
}

#[test]
fn category_and_text_decode_to_the_written_strings() {
    // Insertion order is deliberately not sorted, so a dictionary that stores
    // first-seen keys against sorted values cannot hide behind a single-value
    // column.
    let written = [
        ("zebra", "error: café exploded"),
        ("apple", "日本語"),
        ("zebra", "error: café exploded"),
        ("mango", ""),
        ("apple", "🚀"),
        ("mango", "appears-once"),
    ];
    let mut buffer = populate(&written);
    let output = batch(&mut buffer);
    let category = output.column_by_name("label").expect("category column");
    let text = output.column_by_name("note").expect("text column");

    assert!(
        matches!(
            category.data_type(),
            DataType::Dictionary(key, value)
                if **key == DataType::UInt32 && **value == DataType::Utf8
        ),
        "S.category must stay dictionary-encoded, got {:?}",
        category.data_type()
    );

    for (i, (label, note)) in written.iter().enumerate() {
        let start = i * 2;
        assert_eq!(decoded_utf8(category.as_ref(), start), Some(*label));
        assert_eq!(decoded_utf8(text.as_ref(), start), Some(*note));
        assert_eq!(
            decoded_utf8(category.as_ref(), start + 1),
            None,
            "completion rows are not tagged"
        );
        assert_eq!(decoded_utf8(text.as_ref(), start + 1), None);
    }
}

#[test]
fn empty_repeated_unique_and_multibyte_round_trip() {
    let written = [
        ("", ""),
        ("same", "same"),
        ("same", "same"),
        ("unique-a", "naïve — 日本語 — 𝄞"),
    ];
    let mut buffer = populate(&written);
    let output = batch(&mut buffer);
    let category = output.column_by_name("label").unwrap();
    let text = output.column_by_name("note").unwrap();
    for (i, (label, note)) in written.iter().enumerate() {
        let start = i * 2;
        assert_eq!(decoded_utf8(category.as_ref(), start), Some(*label));
        assert_eq!(decoded_utf8(text.as_ref(), start), Some(*note));
    }
}
