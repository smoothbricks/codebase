//! lmao's trace schema as a reader of sealed segments sees it, and the exact-schema oracle every
//! span kind's test holds its segments to (13_telemetry.md, "Process-tree spans"). Shared by the
//! test binaries that read job and process spans (`#[path]`-included, not a crate).
//!
//! The columns a test passes the oracle are written from the spec's tables with the spec's type
//! names, never derived from the declaration under test: an oracle that passes shows the
//! declaration says what the spec says, and a segment that gains or loses a column is named.

use std::path::Path;

use arrow_array::cast::AsArray as _;
use arrow_array::{Array as _, ArrowPrimitiveType, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};

/// One custom column as the spec declares it: its name, its spec type and whether it may hold
/// nulls. The spec types are `u16`, `u32`, `u64`, `i32`, `i64`, `utf8`, `S.category`, `S.text` and
/// `S.enum`.
pub(crate) type Column = (&'static str, &'static str, bool);

fn dictionary(key: DataType) -> DataType {
    DataType::Dictionary(Box::new(key), Box::new(DataType::Utf8))
}

/// How lmao's Arrow encoding carries a spec type.
fn arrow_type(spec_type: &str) -> DataType {
    match spec_type {
        "u16" => DataType::UInt16,
        "u32" => DataType::UInt32,
        "u64" => DataType::UInt64,
        "i32" => DataType::Int32,
        "i64" => DataType::Int64,
        "utf8" => DataType::Utf8,
        "S.category" | "S.text" => dictionary(DataType::UInt32),
        "S.enum" => dictionary(DataType::UInt16),
        other => panic!("`{other}` is no spec type"),
    }
}

/// `custom` as the Arrow fields lmao's encoding gives them: what a declaration of exactly those
/// columns generates.
pub(crate) fn fields(custom: &[Column]) -> Vec<Field> {
    custom
        .iter()
        .map(|(name, spec_type, nullable)| Field::new(*name, arrow_type(spec_type), *nullable))
        .collect()
}

/// lmao's twelve system columns, in order, as every trace segment opens.
fn system_fields() -> Vec<Field> {
    vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("trace_id", dictionary(DataType::UInt32), false),
        Field::new("thread_id", DataType::UInt64, false),
        Field::new("span_id", DataType::UInt32, false),
        Field::new("parent_thread_id", DataType::UInt64, true),
        Field::new("parent_span_id", DataType::UInt32, true),
        Field::new("entry_type", dictionary(DataType::UInt8), false),
        Field::new("message", dictionary(DataType::UInt32), true),
        Field::new("package_name", dictionary(DataType::UInt32), true),
        Field::new("package_file", dictionary(DataType::UInt32), true),
        Field::new("git_sha", dictionary(DataType::UInt32), true),
        Field::new("line", DataType::UInt32, false),
    ]
}

fn describe(fields: &[Field]) -> Vec<String> {
    fields
        .iter()
        .map(|field| {
            format!(
                "{}: {}{}",
                field.name(),
                field.data_type(),
                if field.is_nullable() { "?" } else { "" }
            )
        })
        .collect()
}

/// The exact-schema oracle: lmao's system columns, then exactly `custom`, each with its
/// declared encoding and nullability, and nothing else.
pub(crate) fn exact_schema(schema: &Schema, custom: &[Column]) -> Result<(), String> {
    let expected = system_fields()
        .into_iter()
        .chain(fields(custom))
        .collect::<Vec<_>>();
    let actual = schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "columns {:?}, expected {:?}",
            describe(&actual),
            describe(&expected)
        ))
    }
}

/// Every sealed segment under `telemetry` whose name starts with `prefix`, in the order its
/// writer sealed them: its name and its one batch. A segment under another prefix is a failure,
/// so a test reads exactly the kind of segment it names.
pub(crate) fn sealed_segments(telemetry: &Path, prefix: &str) -> Vec<(String, RecordBatch)> {
    let segments = sealed(telemetry);
    let strangers = segments
        .iter()
        .map(|(name, _)| name)
        .filter(|name| !name.starts_with(prefix))
        .collect::<Vec<_>>();
    assert!(
        strangers.is_empty(),
        "only sealed `{prefix}` segments: {strangers:?}"
    );
    segments
}

/// [`sealed_segments`] for a root that holds other kinds of segment too: a job's span and its
/// process spans seal into one root, in one writer's order, and a test reads one kind of them.
pub(crate) fn sealed_segments_among_kinds(
    telemetry: &Path,
    prefix: &str,
) -> Vec<(String, RecordBatch)> {
    let mut segments = sealed(telemetry);
    segments.retain(|(name, _)| name.starts_with(prefix));
    segments
}

/// Every segment sealed under `telemetry` (`trace_segment::read_sealed`), by name, which is its
/// writer's seal order: its name and its one batch. A torn or empty segment fails here, since no
/// test cuts the power.
fn sealed(telemetry: &Path) -> Vec<(String, RecordBatch)> {
    let sealed =
        cowshed_core::storage::trace_segment::read_sealed(telemetry).expect("read telemetry root");
    assert!(
        sealed.skipped.is_empty(),
        "torn segments: {:?}",
        sealed.skipped
    );
    let mut segments: Vec<(String, RecordBatch)> = sealed
        .segments
        .into_iter()
        .map(|segment| {
            let name = segment.name().to_owned();
            let [batch] = <[RecordBatch; 1]>::try_from(segment.batches)
                .unwrap_or_else(|batches| panic!("{name} holds one batch, not {}", batches.len()));
            (name, batch)
        })
        .collect();
    segments.sort_by(|left, right| left.0.cmp(&right.0));
    segments
}

/// The string `column` holds at `row` as a reader of lmao's schema reads it, whether the column is
/// plain `Utf8` or a dictionary of any key width; `None` for a null cell.
pub(crate) fn text_cell(batch: &RecordBatch, column: &str, row: usize) -> Option<String> {
    let array = batch
        .column_by_name(column)
        .unwrap_or_else(|| panic!("no column {column}"));
    if array.is_null(row) {
        return None;
    }
    Some(match array.data_type() {
        DataType::Utf8 => array.as_string::<i32>().value(row).to_owned(),
        DataType::Dictionary(..) => {
            let dictionary = array.as_any_dictionary();
            let key = dictionary.normalized_keys()[row];
            dictionary.values().as_string::<i32>().value(key).to_owned()
        }
        other => panic!("{column} is {other}, not a string column"),
    })
}

/// The number `column` holds at `row`; `None` for a null cell.
pub(crate) fn number_cell<T: ArrowPrimitiveType>(
    batch: &RecordBatch,
    column: &str,
    row: usize,
) -> Option<T::Native> {
    let array = batch
        .column_by_name(column)
        .unwrap_or_else(|| panic!("no column {column}"));
    let values = array.as_primitive::<T>();
    values.is_valid(row).then(|| values.value(row))
}
