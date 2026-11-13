//! What the physical vocabulary promises that `columine-arrow` alone can show
//! (`specs/columine/10-physical-vocabulary.md` §10): the Timestamp and Duration
//! planes, schema dictionaries, the stream dictionary and its deltas, and the
//! buffer budget of a wide schema.
//!
//! Every test drives the public surface and reads the stream back with
//! arrow-ipc's own `StreamReader`, so the assertions are on what a reader sees
//! and on the refusal a caller gets, never on the writer's internals.

use std::io::Cursor;

use arrow_array::types::{ArrowDictionaryKeyType, UInt8Type, UInt32Type};
use arrow_array::{DictionaryArray, RecordBatch, StringArray};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::{MessageHeader, root_as_message};
use arrow_schema::{DataType, Field, TimeUnit};
use columine_arrow::{
    ArrowType, DictionarySlot, DictionaryValues, DynamicColumn, DynamicColumns,
    DynamicSchemaConfig, EOS_MARKER, IndexWidth, IpcError, MAX_SCHEMA_FIELDS,
    MIN_ARROW_OUTPUT_CAPACITY, MetadataStorage, SchemaDictionaryTable, SchemaError, SchemaParts,
    SignalSchemaField, StreamDictionaryDelta, TIME_UNITS, dictionary_batch_len,
    logical_schema_ipc_bytes, required_arrow_ipc_len, write_arrow_ipc_from_borrowed_columns,
    write_arrow_ipc_from_dynamic_columns,
};

mod common;

use common::schema_message_declaring;

// -- Shared construction and reading ---------------------------------------

/// A `Dictionary(index, Utf8)` logical type.
fn dictionary_type(index: DataType) -> DataType {
    DataType::Dictionary(Box::new(index), Box::new(DataType::Utf8))
}

/// A nullable dictionary-encoded Arrow field.
fn kind(name: &str, index: DataType) -> Field {
    Field::new(name, dictionary_type(index), true)
}

/// A nullable encoded descriptor.
fn encoded(slot: DictionarySlot, index: IndexWidth) -> SignalSchemaField {
    SignalSchemaField::new_dictionary(slot, index, true)
}

/// Dictionary values, every entry present.
fn texts(values: &[&str]) -> Vec<Option<String>> {
    values
        .iter()
        .map(|value| Some((*value).to_owned()))
        .collect()
}

fn parts<'a>(message: &'a [u8], dictionaries: SchemaDictionaryTable<'a>) -> SchemaParts<'a> {
    SchemaParts {
        schema_message: message,
        dictionaries,
        ..SchemaParts::default()
    }
}

/// The schema `logical` describes, created over `fields` and the schema
/// dictionaries `dictionaries`.
fn create(
    fields: &[SignalSchemaField],
    logical: Vec<Field>,
    dictionaries: &[Vec<Option<String>>],
) -> Result<DynamicSchemaConfig, SchemaError> {
    let message = logical_schema_ipc_bytes(logical).expect("a Schema message encodes");
    DynamicSchemaConfig::from_parts(
        fields,
        parts(&message, SchemaDictionaryTable::Typed(dictionaries)),
    )
}

/// One column of `field`'s index plane, a row per entry; `None` is a null row.
fn index_column(field: SignalSchemaField, rows: &[Option<u64>]) -> DynamicColumns {
    let mut columns = DynamicColumns::new(&[field], u32::try_from(rows.len()).unwrap())
        .expect("column plane fits");
    for row in rows {
        assert!(columns.begin_row());
        match row {
            Some(index) => columns
                .append_uint(0, *index)
                .expect("an index its plane holds"),
            None => columns.append_null(0).expect("a null cell"),
        }
        columns.end_row();
    }
    columns
}

/// One stream: the preamble, the stream dictionary's values when the batch
/// brought any, the record batch and the end-of-stream marker.
fn emit(
    columns: &DynamicColumns,
    fields: &[SignalSchemaField],
    config: &DynamicSchemaConfig,
    stream_dictionary: Option<StreamDictionaryDelta<'_>>,
) -> Result<Vec<u8>, IpcError> {
    let mut metadata = MetadataStorage::for_fields(fields).expect("size metadata");
    let mut output = vec![0u8; 1 << 20];
    let written = write_arrow_ipc_from_dynamic_columns(
        columns,
        config,
        &mut output,
        &mut metadata,
        &[],
        stream_dictionary,
    )?;
    output.truncate(written);
    Ok(output)
}

/// The only record batch of a stream, as arrow-ipc reads it.
fn read_batch(stream: &[u8]) -> RecordBatch {
    let mut reader =
        StreamReader::try_new(Cursor::new(stream), None).expect("arrow-ipc reads the stream");
    let batch = reader
        .next()
        .expect("one record batch")
        .expect("a valid record batch");
    assert!(reader.next().is_none(), "exactly one record batch");
    batch
}

fn dictionary_column<K: ArrowDictionaryKeyType>(
    batch: &RecordBatch,
    index: usize,
) -> &DictionaryArray<K> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<DictionaryArray<K>>()
        .unwrap_or_else(|| {
            panic!(
                "column {index} is {:?}, not a dictionary of the expected index type",
                batch.schema().field(index).data_type()
            )
        })
}

/// A dictionary array's values by ordinal and each row's key, `None` for a
/// null row.
fn dictionary_of<K: ArrowDictionaryKeyType>(
    array: &DictionaryArray<K>,
) -> (Vec<Option<String>>, Vec<Option<usize>>) {
    let values = array
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("a Utf8 dictionary");
    (
        values
            .iter()
            .map(|value| value.map(str::to_owned))
            .collect(),
        array.keys_iter().collect(),
    )
}

/// One continuation-prefixed message of a stream.
struct Message<'a> {
    header: MessageHeader,
    metadata: &'a [u8],
    body: &'a [u8],
    /// Where the next message starts.
    end: usize,
}

fn message_at(stream: &[u8], at: usize) -> Message<'_> {
    assert_eq!(
        &stream[at..at + 4],
        &[0xFF; 4],
        "a continuation marker starts the message at {at}"
    );
    let metadata_len =
        u32::from_le_bytes(stream[at + 4..at + 8].try_into().expect("four bytes")) as usize;
    let metadata = &stream[at + 8..at + 8 + metadata_len];
    let message = root_as_message(metadata).expect("a valid message");
    let body_at = at + 8 + metadata_len;
    let body_len = usize::try_from(message.bodyLength()).expect("a body length");
    Message {
        header: message.header_type(),
        metadata,
        body: &stream[body_at..body_at + body_len],
        end: body_at + body_len,
    }
}

/// A DictionaryBatch message, decoded from its own bytes.
struct Dictionary {
    id: i64,
    is_delta: bool,
    values: Vec<Option<String>>,
    /// Where the next message starts.
    end: usize,
}

fn dictionary_message_at(stream: &[u8], at: usize) -> Dictionary {
    let message = message_at(stream, at);
    assert_eq!(
        message.header,
        MessageHeader::DictionaryBatch,
        "a dictionary message at {at}"
    );
    let root = root_as_message(message.metadata).expect("a valid message");
    let batch = root
        .header_as_dictionary_batch()
        .expect("a DictionaryBatch header");
    let data = batch.data().expect("the dictionary's values");
    let buffers: Vec<&[u8]> = data
        .buffers()
        .expect("buffer descriptors")
        .iter()
        .map(|buffer| {
            let start = usize::try_from(buffer.offset()).expect("an offset");
            let length = usize::try_from(buffer.length()).expect("a length");
            &message.body[start..start + length]
        })
        .collect();
    assert_eq!(buffers.len(), 3, "validity, offsets and data");
    let (validity, offsets, bytes) = (buffers[0], buffers[1], buffers[2]);
    let offsets: Vec<usize> = offsets
        .as_chunks::<4>()
        .0
        .iter()
        .map(|chunk| u32::from_le_bytes(*chunk) as usize)
        .collect();
    let count = usize::try_from(data.length()).expect("a length");
    assert_eq!(offsets.len(), count + 1, "n + 1 offsets");
    // An empty validity buffer says every entry is valid.
    let values = (0..count)
        .map(|index| {
            let valid = validity.is_empty() || validity[index / 8] & (1 << (index % 8)) != 0;
            valid.then(|| {
                String::from_utf8(bytes[offsets[index]..offsets[index + 1]].to_vec())
                    .expect("UTF-8 text")
            })
        })
        .collect();
    Dictionary {
        id: batch.id(),
        is_delta: batch.isDelta(),
        values,
        end: message.end,
    }
}

//#region smoo/columine!n/vocab-temporal-planes

fn temporal_config(
    field: SignalSchemaField,
    logical: DataType,
) -> Result<DynamicSchemaConfig, SchemaError> {
    create(&[field], vec![Field::new("t", logical, true)], &[])
}

#[test]
fn a_timestamp_or_duration_is_refused_on_the_int64_tag_and_created_on_its_own() {
    let int64 = SignalSchemaField::new(ArrowType::Int64, true);
    let mismatch = Some(SchemaError::TypeMismatch { field_index: 0 });
    for unit in TIME_UNITS {
        let instant = DataType::Timestamp(unit, Some("UTC".into()));
        let span = DataType::Duration(unit);
        let timestamp = SignalSchemaField::new_timestamp(unit, true);
        let duration = SignalSchemaField::new_duration(unit, true);

        // Neither is admitted on the Int64 tag: its unit is the descriptor's.
        assert_eq!(
            temporal_config(int64, instant.clone()).err(),
            mismatch,
            "{unit:?} instant on Int64"
        );
        assert_eq!(
            temporal_config(int64, span.clone()).err(),
            mismatch,
            "{unit:?} duration on Int64"
        );

        // Each is created under its own tag and keeps its logical type.
        let config = temporal_config(timestamp, instant.clone())
            .unwrap_or_else(|error| panic!("{unit:?} instant: {error:?}"));
        assert_eq!(config.logical_types, std::slice::from_ref(&instant));
        assert_eq!(config.field_metadata[0].arrow_type, ArrowType::Timestamp);
        let config = temporal_config(duration, span.clone())
            .unwrap_or_else(|error| panic!("{unit:?} duration: {error:?}"));
        assert_eq!(config.logical_types, std::slice::from_ref(&span));
        assert_eq!(config.field_metadata[0].arrow_type, ArrowType::Duration);

        // A plain integer is neither, and the two tags are not each other's.
        assert_eq!(temporal_config(timestamp, DataType::Int64).err(), mismatch);
        assert_eq!(temporal_config(duration, DataType::Int64).err(), mismatch);
        assert_eq!(temporal_config(timestamp, span.clone()).err(), mismatch);
        assert_eq!(temporal_config(duration, instant.clone()).err(), mismatch);

        // An instant is UTC: a zone-less timestamp and one in another zone
        // have no plane under tag 23, or any other.
        for zone in [None, Some("Europe/Amsterdam")] {
            let zoned = DataType::Timestamp(unit, zone.map(Into::into));
            assert_eq!(
                temporal_config(timestamp, zoned.clone()).err(),
                mismatch,
                "{unit:?} in {zone:?}"
            );
            assert_eq!(temporal_config(int64, zoned).err(), mismatch);
        }
    }

    // A unit that differs between the descriptor and the Schema message is a
    // type mismatch, for every pair of units.
    for descriptor in TIME_UNITS {
        for declared in TIME_UNITS {
            if descriptor == declared {
                continue;
            }
            assert_eq!(
                temporal_config(
                    SignalSchemaField::new_timestamp(descriptor, true),
                    DataType::Timestamp(declared, Some("UTC".into())),
                )
                .err(),
                mismatch,
                "timestamp {descriptor:?} over {declared:?}"
            );
            assert_eq!(
                temporal_config(
                    SignalSchemaField::new_duration(descriptor, true),
                    DataType::Duration(declared),
                )
                .err(),
                mismatch,
                "duration {descriptor:?} over {declared:?}"
            );
        }
    }
}

#[test]
fn a_temporal_descriptor_names_a_unit_the_plane_has_and_no_other_plane_carries_one() {
    let message = logical_schema_ipc_bytes(vec![Field::new(
        "t",
        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        true,
    )])
    .expect("a Schema message encodes");
    let invalid = Some(SchemaError::InvalidFieldMetadata { field_index: 0 });

    // Unit code 4 is past nanoseconds.
    let past_nanoseconds = SignalSchemaField {
        type_param: 4,
        ..SignalSchemaField::new_timestamp(TimeUnit::Nanosecond, true)
    };
    assert_eq!(
        DynamicSchemaConfig::new(&message, &[past_nanoseconds]).err(),
        invalid
    );
    // The Int64 plane has no unit.
    let unit_on_int64 = SignalSchemaField {
        type_param: 3,
        ..SignalSchemaField::new(ArrowType::Int64, true)
    };
    assert_eq!(
        DynamicSchemaConfig::new(&message, &[unit_on_int64]).err(),
        invalid
    );

    // The same through the untrusted descriptor bytes.
    let nanoseconds = [23, 1, 3, 0, 0, 0, 0, 0];
    let config = DynamicSchemaConfig::from_wire(&message, &nanoseconds).expect("tag 23, unit 3");
    assert_eq!(
        config.field_metadata,
        [SignalSchemaField::new_timestamp(TimeUnit::Nanosecond, true)]
    );
    for raw in [[23, 1, 4, 0, 0, 0, 0, 0], [6, 1, 3, 0, 0, 0, 0, 0]] {
        assert_eq!(
            DynamicSchemaConfig::from_wire(&message, &raw).err(),
            invalid,
            "{raw:?}"
        );
    }
}

//#endregion smoo/columine!n/vocab-temporal-planes

//#region smoo/columine!n/vocab-enum-dictionary

#[test]
fn a_schema_dictionary_column_reads_back_as_a_dictionary_of_its_declared_values() {
    let field = encoded(DictionarySlot::Schema(0), IndexWidth::U8);
    let table = [texts(&["open", "closed"])];
    let config = create(&[field], vec![kind("kind", DataType::UInt8)], &table)
        .expect("a schema dictionary column is created");

    let dictionary = config
        .dictionary(DictionarySlot::Schema(0))
        .expect("the schema dictionary is bound");
    assert_eq!(dictionary.values, table[0]);
    assert_eq!(dictionary.arrow_ids, [0]);
    assert!(config.dictionary(DictionarySlot::Stream).is_none());

    let columns = index_column(field, &[Some(0), Some(1), None, Some(0)]);
    let stream = emit(&columns, &[field], &config, None).expect("the stream is written");
    // The preamble, the Schema message and its dictionary, heads the stream.
    assert_eq!(
        &stream[..config.schema_message_size()],
        &config.schema_bytes[..]
    );

    let batch = read_batch(&stream);
    assert_eq!(
        batch.schema().field(0).data_type(),
        &dictionary_type(DataType::UInt8)
    );
    let (values, keys) = dictionary_of(dictionary_column::<UInt8Type>(&batch, 0));
    assert_eq!(values, texts(&["open", "closed"]));
    assert_eq!(keys, [Some(0), Some(1), None, Some(0)]);
}

#[test]
fn a_dictionary_with_a_skipped_ordinal_keeps_a_null_entry() {
    let field = encoded(DictionarySlot::Schema(0), IndexWidth::U8);
    let table = [vec![
        Some("open".to_owned()),
        None,
        Some("closed".to_owned()),
    ]];
    let config = create(&[field], vec![kind("kind", DataType::UInt8)], &table)
        .expect("a dictionary that skips an ordinal is created");
    assert_eq!(
        config.dictionary(DictionarySlot::Schema(0)).unwrap().values,
        table[0]
    );

    let columns = index_column(field, &[Some(2), Some(0)]);
    let stream = emit(&columns, &[field], &config, None).expect("the stream is written");

    // The message the preamble holds says ordinal 1 is null, not an empty text.
    let message = dictionary_message_at(&stream, config.schema_message().len());
    assert!(!message.is_delta);
    assert_eq!(message.values, table[0]);

    let batch = read_batch(&stream);
    let (values, keys) = dictionary_of(dictionary_column::<UInt8Type>(&batch, 0));
    assert_eq!(values, table[0]);
    assert_eq!(keys, [Some(2), Some(0)]);
}

#[test]
fn two_fields_of_one_slot_with_separate_arrow_ids_each_get_the_dictionary() {
    let field = encoded(DictionarySlot::Schema(0), IndexWidth::U8);
    let fields = [field, field];
    let table = [texts(&["open", "closed"])];
    // Arrow's writer numbers the two fields' dictionaries 0 and 1.
    let config = create(
        &fields,
        vec![kind("a", DataType::UInt8), kind("b", DataType::UInt8)],
        &table,
    )
    .expect("two encoded fields over one slot are created");
    let dictionary = config.dictionary(DictionarySlot::Schema(0)).unwrap();
    assert_eq!(dictionary.arrow_ids, [0, 1]);

    // The dictionary's message is written once per id.
    let one_message = dictionary_batch_len(table[0].as_slice()).unwrap();
    assert_eq!(
        config.schema_message_size(),
        config.schema_message().len() + 2 * one_message
    );

    let mut columns = DynamicColumns::new(&fields, 2).expect("column plane fits");
    assert!(columns.begin_row());
    columns.append_uint(0, 0).unwrap();
    columns.append_uint(1, 1).unwrap();
    columns.end_row();
    assert!(columns.begin_row());
    columns.append_uint(0, 1).unwrap();
    columns.append_null(1).unwrap();
    columns.end_row();
    let stream = emit(&columns, &fields, &config, None).expect("the stream is written");

    let first = dictionary_message_at(&stream, config.schema_message().len());
    let second = dictionary_message_at(&stream, first.end);
    assert_eq!([first.id, second.id], [0, 1]);
    assert_eq!(first.values, table[0]);
    assert_eq!(second.values, table[0]);

    let batch = read_batch(&stream);
    let (values, keys) = dictionary_of(dictionary_column::<UInt8Type>(&batch, 0));
    assert_eq!(values, table[0]);
    assert_eq!(keys, [Some(0), Some(1)]);
    let (values, keys) = dictionary_of(dictionary_column::<UInt8Type>(&batch, 1));
    assert_eq!(values, table[0]);
    assert_eq!(keys, [Some(1), None]);
}

#[test]
fn a_field_naming_a_missing_dictionary_or_too_narrow_an_index_is_refused() {
    let mismatch = |field_index| Some(SchemaError::DictionaryMismatch { field_index });
    let many = |count: usize| -> Vec<Option<String>> {
        (0..count).map(|index| Some(format!("v{index}"))).collect()
    };

    // An entry the table lacks, at the position of the field that names it.
    assert_eq!(
        create(
            &[encoded(DictionarySlot::Schema(1), IndexWidth::U8)],
            vec![kind("kind", DataType::UInt8)],
            &[texts(&["open"])],
        )
        .err(),
        mismatch(0)
    );
    assert_eq!(
        create(
            &[encoded(DictionarySlot::Schema(0), IndexWidth::U8)],
            vec![kind("kind", DataType::UInt8)],
            &[],
        )
        .err(),
        mismatch(0)
    );
    assert_eq!(
        create(
            &[
                SignalSchemaField::new(ArrowType::Utf8, true),
                encoded(DictionarySlot::Schema(1), IndexWidth::U8),
            ],
            vec![
                Field::new("text", DataType::Utf8, true),
                kind("kind", DataType::UInt8),
            ],
            &[texts(&["open"])],
        )
        .err(),
        mismatch(1)
    );

    // An index plane too narrow for the table's values: a byte indexes 256.
    let narrow = |index: IndexWidth, index_type: DataType, count: usize| {
        create(
            &[encoded(DictionarySlot::Schema(0), index)],
            vec![kind("kind", index_type)],
            &[many(count)],
        )
    };
    assert!(narrow(IndexWidth::U8, DataType::UInt8, 256).is_ok());
    assert_eq!(
        narrow(IndexWidth::U8, DataType::UInt8, 257).err(),
        mismatch(0)
    );
    assert!(narrow(IndexWidth::U16, DataType::UInt16, 257).is_ok());

    // Each field's own width is judged against the slot's values.
    assert_eq!(
        create(
            &[
                encoded(DictionarySlot::Schema(0), IndexWidth::U16),
                encoded(DictionarySlot::Schema(0), IndexWidth::U8),
            ],
            vec![
                kind("wide", DataType::UInt16),
                kind("narrow", DataType::UInt8)
            ],
            &[many(300)],
        )
        .err(),
        mismatch(1)
    );
}

#[test]
fn a_dictionary_no_field_names_is_refused() {
    let invalid = Some(SchemaError::InvalidDictionaryTable);
    let first = encoded(DictionarySlot::Schema(0), IndexWidth::U8);

    // A table beside a schema with no encoded field.
    assert_eq!(
        create(
            &[SignalSchemaField::new(ArrowType::Utf8, true)],
            vec![Field::new("text", DataType::Utf8, true)],
            &[texts(&["open"])],
        )
        .err(),
        invalid
    );
    // A table longer than the entries its fields name.
    assert_eq!(
        create(
            &[first],
            vec![kind("kind", DataType::UInt8)],
            &[texts(&["open"]), texts(&["closed"])],
        )
        .err(),
        invalid
    );
    // Entry 1 is named and entry 0 is not.
    assert_eq!(
        create(
            &[encoded(DictionarySlot::Schema(1), IndexWidth::U8)],
            vec![kind("kind", DataType::UInt8)],
            &[texts(&["open"]), texts(&["closed"])],
        )
        .err(),
        invalid
    );
    // The stream dictionary has no table entry.
    assert_eq!(
        create(
            &[encoded(DictionarySlot::Stream, IndexWidth::U32)],
            vec![kind("kind", DataType::UInt32)],
            &[texts(&["open"])],
        )
        .err(),
        invalid
    );

    // Every entry named is a schema.
    create(
        &[first, encoded(DictionarySlot::Schema(1), IndexWidth::U8)],
        vec![kind("a", DataType::UInt8), kind("b", DataType::UInt8)],
        &[texts(&["open"]), texts(&["closed"])],
    )
    .expect("every table entry is named by a field");
}

#[test]
fn two_fields_sharing_one_arrow_id_get_the_dictionary_once() {
    let field = encoded(DictionarySlot::Schema(0), IndexWidth::U8);
    let fields = [field, field];
    let table = [texts(&["open", "closed"])];
    let message = schema_message_declaring(&[("a", Some(0)), ("b", Some(0))]);
    let config = DynamicSchemaConfig::from_parts(
        &fields,
        parts(&message, SchemaDictionaryTable::Typed(&table)),
    )
    .expect("two fields over one Arrow id are created");
    assert_eq!(
        config
            .dictionary(DictionarySlot::Schema(0))
            .unwrap()
            .arrow_ids,
        [0]
    );

    // The dictionary is written once for its one id, not once per field.
    let one_message = dictionary_batch_len(table[0].as_slice()).unwrap();
    assert_eq!(
        config.schema_message_size(),
        config.schema_message().len() + one_message
    );

    let mut columns = DynamicColumns::new(&fields, 2).expect("column plane fits");
    assert!(columns.begin_row());
    columns.append_uint(0, 0).unwrap();
    columns.append_uint(1, 1).unwrap();
    columns.end_row();
    assert!(columns.begin_row());
    columns.append_uint(0, 1).unwrap();
    columns.append_null(1).unwrap();
    columns.end_row();
    let stream = emit(&columns, &fields, &config, None).expect("the stream is written");

    let only = dictionary_message_at(&stream, config.schema_message().len());
    assert_eq!(only.id, 0);
    assert_eq!(only.values, table[0]);
    assert_eq!(
        message_at(&stream, only.end).header,
        MessageHeader::RecordBatch,
        "no second dictionary message"
    );

    let batch = read_batch(&stream);
    let (values, keys) = dictionary_of(dictionary_column::<UInt8Type>(&batch, 0));
    assert_eq!(values, table[0]);
    assert_eq!(keys, [Some(0), Some(1)]);
    let (values, keys) = dictionary_of(dictionary_column::<UInt8Type>(&batch, 1));
    assert_eq!(values, table[0]);
    assert_eq!(keys, [Some(1), None]);
}

#[test]
fn one_arrow_id_under_two_slots_is_refused() {
    let tables = [texts(&["open"]), texts(&["closed"])];
    let message_with =
        |ids: [i64; 2]| schema_message_declaring(&[("a", Some(ids[0])), ("b", Some(ids[1]))]);
    let create_with =
        |ids: [i64; 2], fields: [SignalSchemaField; 2], tables: &[Vec<Option<String>>]| {
            DynamicSchemaConfig::from_parts(
                &fields,
                parts(&message_with(ids), SchemaDictionaryTable::Typed(tables)),
            )
            .err()
        };
    let schema_0 = encoded(DictionarySlot::Schema(0), IndexWidth::U8);
    let schema_1 = encoded(DictionarySlot::Schema(1), IndexWidth::U8);
    let stream = encoded(DictionarySlot::Stream, IndexWidth::U8);

    // The same two slots over two ids are fine: it is the shared id that is refused.
    assert_eq!(create_with([0, 1], [schema_0, schema_1], &tables), None);

    // Fields of two slots declaring one id are not: one Arrow dictionary
    // cannot be both slots'.
    let mismatch = Some(SchemaError::DictionaryMismatch { field_index: 1 });
    assert_eq!(create_with([0, 0], [schema_0, schema_1], &tables), mismatch);
    assert_eq!(
        create_with([0, 0], [schema_0, stream], &tables[..1]),
        mismatch
    );
    assert_eq!(
        create_with([0, 0], [stream, schema_0], &tables[..1]),
        mismatch
    );
}

/// The wire form of a dictionary table: `[count u32]`, then per dictionary
/// `[values u32]`, then per value `[length u32][bytes]`, `0xFFFF_FFFF` for a
/// null entry.
fn wire_table(dictionaries: &[Vec<Option<&str>>]) -> Vec<u8> {
    let word = |value: usize| u32::try_from(value).unwrap().to_le_bytes();
    let mut bytes = word(dictionaries.len()).to_vec();
    for values in dictionaries {
        bytes.extend_from_slice(&word(values.len()));
        for value in values {
            match value {
                Some(text) => {
                    bytes.extend_from_slice(&word(text.len()));
                    bytes.extend_from_slice(text.as_bytes());
                }
                None => bytes.extend_from_slice(&u32::MAX.to_le_bytes()),
            }
        }
    }
    bytes
}

#[test]
fn the_wire_dictionary_table_decodes_the_same_dictionaries_as_the_typed_one_and_refuses_a_truncated_one()
 {
    let fields = [
        encoded(DictionarySlot::Schema(0), IndexWidth::U8),
        encoded(DictionarySlot::Schema(1), IndexWidth::U16),
    ];
    let message = logical_schema_ipc_bytes(vec![
        kind("a", DataType::UInt8),
        kind("b", DataType::UInt16),
    ])
    .expect("a Schema message encodes");
    let typed_table = [
        vec![Some("open".to_owned()), None, Some("closed".to_owned())],
        texts(&["x", "αβ"]),
    ];
    let wire = wire_table(&[
        vec![Some("open"), None, Some("closed")],
        vec![Some("x"), Some("αβ")],
    ]);

    let typed = DynamicSchemaConfig::from_parts(
        &fields,
        parts(&message, SchemaDictionaryTable::Typed(&typed_table)),
    )
    .expect("the typed table is accepted");
    let decoded = DynamicSchemaConfig::from_parts(
        &fields,
        parts(&message, SchemaDictionaryTable::Wire(&wire)),
    )
    .expect("the wire table is accepted");
    assert_eq!(decoded, typed);

    // The same through the untrusted descriptor bytes.
    let descriptors: Vec<u8> = fields.iter().flat_map(|field| field.to_wire()).collect();
    let from_bytes = DynamicSchemaConfig::from_wire_parts(
        &descriptors,
        parts(&message, SchemaDictionaryTable::Wire(&wire)),
    )
    .expect("the descriptor and dictionary tables are accepted");
    assert_eq!(from_bytes, typed);

    // A table cut anywhere short of its end is refused.
    for length in 1..wire.len() {
        assert_eq!(
            DynamicSchemaConfig::from_parts(
                &fields,
                parts(&message, SchemaDictionaryTable::Wire(&wire[..length])),
            )
            .err(),
            Some(SchemaError::InvalidDictionaryTable),
            "a table cut at {length} of {} bytes",
            wire.len()
        );
    }
}

#[test]
fn a_wire_dictionary_table_with_trailing_bytes_or_invalid_text_is_refused() {
    let field = encoded(DictionarySlot::Schema(0), IndexWidth::U8);
    let message = logical_schema_ipc_bytes(vec![kind("kind", DataType::UInt8)])
        .expect("a Schema message encodes");
    let refused = |table: &[u8]| {
        DynamicSchemaConfig::from_parts(
            &[field],
            parts(&message, SchemaDictionaryTable::Wire(table)),
        )
        .err()
    };

    let wire = wire_table(&[vec![Some("open")]]);
    assert_eq!(refused(&wire), None);
    let mut trailing = wire.clone();
    trailing.push(0);
    assert_eq!(
        refused(&trailing),
        Some(SchemaError::InvalidDictionaryTable)
    );

    // One dictionary of one value of one byte, which is not UTF-8.
    let mut not_text = Vec::new();
    for word in [1u32, 1, 1] {
        not_text.extend_from_slice(&word.to_le_bytes());
    }
    not_text.push(0xFF);
    assert_eq!(
        refused(&not_text),
        Some(SchemaError::InvalidDictionaryTable)
    );
}

//#endregion smoo/columine!n/vocab-enum-dictionary

//#region smoo/columine!n/vocab-stream-dictionary

/// The stream dictionary's values as one message carries them.
struct StreamValues(Vec<String>);

impl StreamValues {
    fn new(values: &[&str]) -> Self {
        Self(values.iter().map(|value| (*value).to_owned()).collect())
    }
}

impl DictionaryValues for StreamValues {
    fn len(&self) -> u32 {
        u32::try_from(self.0.len()).expect("a test dictionary is small")
    }

    fn value(&self, index: u32) -> Option<&[u8]> {
        let index = usize::try_from(index).ok()?;
        self.0.get(index).map(|text| text.as_bytes())
    }
}

/// A one-column schema whose column indexes the stream dictionary.
fn stream_schema() -> (DynamicSchemaConfig, SignalSchemaField) {
    let field = encoded(DictionarySlot::Stream, IndexWidth::U32);
    let config = create(&[field], vec![kind("kind", DataType::UInt32)], &[])
        .expect("a stream dictionary column is created");
    (config, field)
}

/// The rows of a stream with one record batch, as their texts.
fn row_texts(stream: &[u8]) -> Vec<Option<String>> {
    let batch = read_batch(stream);
    let (values, keys) = dictionary_of(dictionary_column::<UInt32Type>(&batch, 0));
    keys.into_iter()
        .map(|key| key.and_then(|key| values[key].clone()))
        .collect()
}

/// Three batches through one dictionary: the first defines it with two
/// values, the second adds one, the third adds none.
fn three_streams() -> (DynamicSchemaConfig, [Vec<u8>; 3]) {
    let (config, field) = stream_schema();
    let fields = [field];
    let first_values = StreamValues::new(&["open", "closed"]);
    let first = emit(
        &index_column(field, &[Some(0), Some(1), Some(0)]),
        &fields,
        &config,
        Some(StreamDictionaryDelta {
            values: &first_values,
            is_delta: false,
        }),
    )
    .expect("the first stream is written");
    let added = StreamValues::new(&["pending"]);
    let second = emit(
        &index_column(field, &[Some(2), Some(0)]),
        &fields,
        &config,
        Some(StreamDictionaryDelta {
            values: &added,
            is_delta: true,
        }),
    )
    .expect("the second stream is written");
    let third = emit(
        &index_column(field, &[Some(1), Some(2), None]),
        &fields,
        &config,
        None,
    )
    .expect("the third stream is written");
    (config, [first, second, third])
}

#[test]
fn the_stream_dictionary_is_defined_once_and_then_grows_by_deltas() {
    let (config, [first, second, third]) = three_streams();
    // No schema dictionary: the preamble is the Schema message alone.
    let preamble = config.schema_message_size();
    assert_eq!(preamble, config.schema_message().len());
    assert_eq!(
        config.dictionary(DictionarySlot::Stream).unwrap().arrow_ids,
        [0]
    );

    // The first stream defines the dictionary and reads standalone.
    let defined = dictionary_message_at(&first, preamble);
    assert!(!defined.is_delta);
    assert_eq!(defined.id, 0);
    assert_eq!(defined.values, texts(&["open", "closed"]));
    assert_eq!(
        message_at(&first, defined.end).header,
        MessageHeader::RecordBatch
    );
    assert_eq!(row_texts(&first), texts(&["open", "closed", "open"]));

    // The second carries exactly what it added, as one delta, then its batch.
    let delta = dictionary_message_at(&second, preamble);
    assert!(delta.is_delta);
    assert_eq!(delta.id, 0);
    assert_eq!(delta.values, texts(&["pending"]));
    let record_batch = message_at(&second, delta.end);
    assert_eq!(record_batch.header, MessageHeader::RecordBatch);
    assert_eq!(&second[record_batch.end..], &EOS_MARKER);

    // The third added nothing and carries no dictionary message.
    let record_batch = message_at(&third, preamble);
    assert_eq!(record_batch.header, MessageHeader::RecordBatch);
    assert_eq!(&third[record_batch.end..], &EOS_MARKER);

    // A reader given the image so far, then the batch, reads each batch's
    // texts: the preamble, the definition, the delta and the batch.
    let through_second = [
        &second[..preamble],
        &first[preamble..defined.end],
        &second[preamble..],
    ]
    .concat();
    assert_eq!(row_texts(&through_second), texts(&["pending", "open"]));
    let through_third = [
        &second[..preamble],
        &first[preamble..defined.end],
        &second[preamble..delta.end],
        &third[preamble..],
    ]
    .concat();
    assert_eq!(
        row_texts(&through_third),
        vec![Some("closed".to_owned()), Some("pending".to_owned()), None]
    );
}

#[test]
fn a_reader_that_starts_late_is_first_given_the_image_as_one_dictionary_message() {
    let (config, field) = stream_schema();
    let image = StreamValues::new(&["open", "closed", "pending"]);
    let stream = emit(
        &index_column(field, &[Some(1), Some(2), None]),
        &[field],
        &config,
        Some(StreamDictionaryDelta {
            values: &image,
            is_delta: false,
        }),
    )
    .expect("the stream is written");
    assert_eq!(
        row_texts(&stream),
        vec![Some("closed".to_owned()), Some("pending".to_owned()), None]
    );
}

/// The columns of `columns` as a caller builds a batch from borrowed buffers.
fn borrowed_column(
    columns: &DynamicColumns,
    field: SignalSchemaField,
    index: usize,
) -> Result<DynamicColumn<'_>, IpcError> {
    let storage = columns.columns.get(index).ok_or(IpcError::InvalidColumn)?;
    Ok(DynamicColumn::fixed(
        u32::try_from(index).map_err(|_| IpcError::InvalidColumn)?,
        field,
        Some(storage.validity_bytes(columns.count)),
        storage.value_bytes(columns.count),
    ))
}

#[test]
fn the_exact_length_of_a_stream_counts_its_stream_dictionary_messages() {
    let (config, field) = stream_schema();
    let columns = index_column(field, &[Some(0), Some(1)]);
    let added = StreamValues::new(&["open", "closed"]);
    let delta = StreamDictionaryDelta {
        values: &added,
        is_delta: false,
    };

    let without = required_arrow_ipc_len(
        &config,
        |index| borrowed_column(&columns, field, index),
        &[],
        None,
    )
    .unwrap();
    let with = required_arrow_ipc_len(
        &config,
        |index| borrowed_column(&columns, field, index),
        &[],
        Some(delta),
    )
    .unwrap();
    assert_eq!(with - without, dictionary_batch_len(&added).unwrap());

    let mut output = vec![0u8; with.max(MIN_ARROW_OUTPUT_CAPACITY)];
    let mut metadata = MetadataStorage::for_fields(&[field]).unwrap();
    let written = write_arrow_ipc_from_borrowed_columns(
        columns.count,
        &config,
        &mut output,
        &mut metadata,
        |index| borrowed_column(&columns, field, index),
        |_| 0,
        &[],
        Some(delta),
    )
    .unwrap();
    assert_eq!(written, with);
}

#[test]
fn stream_values_for_a_schema_without_a_stream_dictionary_are_refused() {
    let values = StreamValues::new(&["open"]);
    let delta = Some(StreamDictionaryDelta {
        values: &values,
        is_delta: false,
    });

    // A schema with no dictionary, and one whose only dictionary is fixed.
    let plain = SignalSchemaField::new(ArrowType::Utf8, true);
    let plain_config = create(
        &[plain],
        vec![Field::new("text", DataType::Utf8, true)],
        &[],
    )
    .expect("a plain schema is created");
    let mut plain_columns = DynamicColumns::new(&[plain], 1).expect("column plane fits");
    assert!(plain_columns.begin_row());
    plain_columns.append_variable(0, b"open").unwrap();
    plain_columns.end_row();

    let fixed = encoded(DictionarySlot::Schema(0), IndexWidth::U8);
    let fixed_config = create(
        &[fixed],
        vec![kind("kind", DataType::UInt8)],
        &[texts(&["open"])],
    )
    .expect("a schema dictionary column is created");
    let fixed_columns = index_column(fixed, &[Some(0)]);

    for (config, field, columns) in [
        (&plain_config, plain, &plain_columns),
        (&fixed_config, fixed, &fixed_columns),
    ] {
        let mut metadata = MetadataStorage::for_fields(&[field]).unwrap();
        let mut output = vec![0u8; 1 << 16];
        assert_eq!(
            write_arrow_ipc_from_dynamic_columns(
                columns,
                config,
                &mut output,
                &mut metadata,
                &[],
                delta,
            ),
            Err(IpcError::InvalidColumn)
        );
        assert!(
            output.iter().all(|byte| *byte == 0),
            "a refused batch writes nothing"
        );
        // The same columns are written when no stream values come with them.
        assert!(
            write_arrow_ipc_from_dynamic_columns(
                columns,
                config,
                &mut output,
                &mut metadata,
                &[],
                None,
            )
            .is_ok()
        );
    }
}

//#endregion smoo/columine!n/vocab-stream-dictionary

//#region smoo/columine!n/vocab-buffer-budget

#[test]
fn a_schema_of_512_dictionary_columns_over_one_dictionary_is_created_and_encoded() {
    let field = encoded(DictionarySlot::Schema(0), IndexWidth::U8);
    let fields = vec![field; MAX_SCHEMA_FIELDS];
    let logical: Vec<Field> = (0..MAX_SCHEMA_FIELDS)
        .map(|index| kind(&format!("c{index}"), DataType::UInt8))
        .collect();
    let table = [texts(&["open", "closed"])];
    let config = create(&fields, logical, &table).expect("512 encoded columns are created");
    assert_eq!(config.field_metadata.len(), MAX_SCHEMA_FIELDS);

    // Arrow's writer numbers each field's dictionary, and the one dictionary
    // is written once per id.
    let dictionary = config.dictionary(DictionarySlot::Schema(0)).unwrap();
    assert_eq!(dictionary.arrow_ids.len(), MAX_SCHEMA_FIELDS);
    let one_message = dictionary_batch_len(table[0].as_slice()).unwrap();
    assert_eq!(
        config.schema_message_size(),
        config.schema_message().len() + MAX_SCHEMA_FIELDS * one_message
    );

    // The budget is one node and two buffers (validity, indexes) per field:
    // the dictionary's values are not any field's buffers.
    assert_eq!(
        usize::try_from(config.compute_buffer_count()).unwrap(),
        2 * MAX_SCHEMA_FIELDS
    );
    let storage = MetadataStorage::for_fields(&fields).expect("size metadata");
    assert_eq!(storage.field_nodes.len(), MAX_SCHEMA_FIELDS);
    assert_eq!(storage.buffer_descs.len(), 2 * MAX_SCHEMA_FIELDS);

    let mut columns = DynamicColumns::new(&fields, 2).expect("column plane fits");
    for key in [0u64, 1] {
        assert!(columns.begin_row());
        for column in 0..MAX_SCHEMA_FIELDS {
            columns
                .append_uint(u32::try_from(column).unwrap(), key)
                .unwrap();
        }
        columns.end_row();
    }
    let stream = emit(&columns, &fields, &config, None).expect("the stream is written");

    let batch = read_batch(&stream);
    assert_eq!(batch.num_columns(), MAX_SCHEMA_FIELDS);
    for index in 0..MAX_SCHEMA_FIELDS {
        let (values, keys) = dictionary_of(dictionary_column::<UInt8Type>(&batch, index));
        assert_eq!(values, table[0], "column {index}");
        assert_eq!(keys, [Some(0), Some(1)], "column {index}");
    }
}

//#endregion smoo/columine!n/vocab-buffer-budget
