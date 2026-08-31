//! In-process backend: hand-rolled scan over flushed RecordBatches.
//!
//! Every column read is typed and fallible. A column of an unexpected type, a
//! selector naming a column no batch carries, or a query against zero batches is an
//! `Err` — never "no rows matched". A count that silently becomes 0 makes `never()`
//! report that an event never appeared and `all_children_of()` report that every child
//! is correctly parented, so a coerced error here answers the caller's assertion with
//! its PASS value (minigraf PERFORMANCE-HANDBOOK §7.10f).

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Float64Type, Int64Type, TimestampNanosecondType, UInt8Type, UInt32Type, UInt64Type,
};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{ArrowError, DataType};

use crate::{ColumnValue, Selector, TraceQuery};

/// Span identity columns. Span ids are per-thread counters, so thread identity is part
/// of the key, not decoration: without it a child whose `parent_span_id` collides with
/// a same-numbered span on another thread reads as correctly parented.
const IDENTITY_COLUMNS: [&str; 5] = [
    "trace_id",
    "thread_id",
    "span_id",
    "parent_thread_id",
    "parent_span_id",
];

pub struct ArrowTraceQuery {
    batches: Vec<RecordBatch>,
}

/// A span's full identity within a trace.
type SpanKey = (String, u64, u64);

fn type_error(column: &str, want: &str, got: &DataType) -> ArrowError {
    ArrowError::SchemaError(format!("column {column} must be {want}, found {got}"))
}

fn selector_columns(selector: &Selector) -> Vec<&str> {
    let mut columns = Vec::with_capacity(selector.constraints.len() + 1);
    if selector.template.is_some() {
        columns.push("message");
    }
    columns.extend(selector.constraints.iter().map(|(name, _)| name.as_str()));
    columns
}

/// A span id or thread id widened from 32 to 64 unsigned bits is lossless, so both are
/// accepted; a signed or narrower column is not an identity this reader can trust.
fn reads_as_u64(data_type: &DataType) -> bool {
    matches!(data_type, DataType::UInt32 | DataType::UInt64)
}

fn reads_as_i64(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int64 | DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None)
    )
}

fn reads_as_str(data_type: &DataType) -> bool {
    match data_type {
        DataType::Utf8 => true,
        DataType::Dictionary(key, value) => {
            **value == DataType::Utf8 && matches!(**key, DataType::UInt8 | DataType::UInt32)
        }
        _ => false,
    }
}

fn addresses_column(want: &ColumnValue, data_type: &DataType) -> bool {
    match want {
        ColumnValue::Str(_) => reads_as_str(data_type),
        ColumnValue::U64(_) => reads_as_u64(data_type),
        ColumnValue::I64(_) => reads_as_i64(data_type),
        ColumnValue::F64(_) => matches!(data_type, DataType::Float64),
        ColumnValue::Bool(_) => matches!(data_type, DataType::Boolean),
    }
}

fn wanted_type_name(want: &ColumnValue) -> &'static str {
    match want {
        ColumnValue::Str(_) => "Utf8 or a Utf8 dictionary",
        ColumnValue::U64(_) => "UInt32 or UInt64",
        ColumnValue::I64(_) => "Int64 or timestamp[ns]",
        ColumnValue::F64(_) => "Float64",
        ColumnValue::Bool(_) => "Boolean",
    }
}

impl ArrowTraceQuery {
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        Self { batches }
    }

    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    /// Errors when a needed column is absent from any batch, and when there are no
    /// batches at all: with no schema to resolve against, answering 0 would be a
    /// vacuous pass for `never()` rather than a fact about the trace.
    fn require_columns(&self, columns: &[&str]) -> Result<(), ArrowError> {
        if self.batches.is_empty() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "no batches to query: cannot resolve columns [{}]",
                columns.join(", ")
            )));
        }
        for batch in &self.batches {
            let schema = batch.schema();
            for column in columns {
                schema.index_of(column)?;
            }
        }
        Ok(())
    }

    /// Validates presence AND type before any row is read. Checking lazily per row
    /// would leave a mistyped column undetected whenever no row reaches it — the
    /// selector would be answered from a schema the reader cannot actually interpret
    /// (§7.7: validate once, then trust).
    fn validate_selector(&self, selector: &Selector) -> Result<(), ArrowError> {
        self.require_columns(&selector_columns(selector))?;
        for batch in &self.batches {
            let schema = batch.schema();
            if selector.template.is_some() {
                let data_type = schema.field(schema.index_of("message")?).data_type();
                if !reads_as_str(data_type) {
                    return Err(type_error(
                        "message",
                        "Utf8 or a Utf8 dictionary",
                        data_type,
                    ));
                }
            }
            for (name, want) in &selector.constraints {
                let data_type = schema.field(schema.index_of(name)?).data_type();
                if !addresses_column(want, data_type) {
                    return Err(type_error(name, wanted_type_name(want), data_type));
                }
            }
        }
        Ok(())
    }

    /// Same up-front discipline for the identity columns `all_children_of` reads.
    fn require_identity_columns(&self) -> Result<(), ArrowError> {
        self.require_columns(&IDENTITY_COLUMNS)?;
        for batch in &self.batches {
            let schema = batch.schema();
            for column in IDENTITY_COLUMNS {
                let data_type = schema.field(schema.index_of(column)?).data_type();
                let ok = if column == "trace_id" {
                    reads_as_str(data_type)
                } else {
                    reads_as_u64(data_type)
                };
                if !ok {
                    let want = if column == "trace_id" {
                        "Utf8 or a Utf8 dictionary"
                    } else {
                        "UInt32 or UInt64"
                    };
                    return Err(type_error(column, want, data_type));
                }
            }
        }
        Ok(())
    }

    /// Visits every row matching `selector`, propagating the first column-read error.
    fn visit_matching<F>(&self, selector: &Selector, mut visit: F) -> Result<(), ArrowError>
    where
        F: FnMut(&RecordBatch, usize) -> Result<(), ArrowError>,
    {
        for batch in &self.batches {
            for row in 0..batch.num_rows() {
                if row_matches(batch, row, selector)? {
                    visit(batch, row)?;
                }
            }
        }
        Ok(())
    }

    fn span_key(batch: &RecordBatch, row: usize) -> Result<Option<SpanKey>, ArrowError> {
        let (Some(trace), Some(thread), Some(span)) = (
            str_at(batch, "trace_id", row)?,
            u64_at(batch, "thread_id", row)?,
            u64_at(batch, "span_id", row)?,
        ) else {
            return Ok(None);
        };
        Ok(Some((trace.to_owned(), thread, span)))
    }

    fn parent_key(batch: &RecordBatch, row: usize) -> Result<Option<SpanKey>, ArrowError> {
        let (Some(trace), Some(thread), Some(span)) = (
            str_at(batch, "trace_id", row)?,
            u64_at(batch, "parent_thread_id", row)?,
            u64_at(batch, "parent_span_id", row)?,
        ) else {
            return Ok(None);
        };
        Ok(Some((trace.to_owned(), thread, span)))
    }
}

fn str_at<'a>(
    batch: &'a RecordBatch,
    name: &str,
    row: usize,
) -> Result<Option<&'a str>, ArrowError> {
    let idx = batch.schema_ref().index_of(name)?;
    let column = batch.column(idx);
    if column.is_null(row) {
        return Ok(None);
    }
    match column.data_type() {
        DataType::Dictionary(key, value) if **value == DataType::Utf8 => match **key {
            DataType::UInt8 => {
                let dictionary = column.as_dictionary::<UInt8Type>();
                let values = dictionary
                    .values()
                    .as_string_opt::<i32>()
                    .ok_or_else(|| type_error(name, "a Utf8 dictionary", column.data_type()))?;
                Ok(Some(values.value(dictionary.keys().value(row) as usize)))
            }
            DataType::UInt32 => {
                let dictionary = column.as_dictionary::<UInt32Type>();
                let values = dictionary
                    .values()
                    .as_string_opt::<i32>()
                    .ok_or_else(|| type_error(name, "a Utf8 dictionary", column.data_type()))?;
                Ok(Some(values.value(dictionary.keys().value(row) as usize)))
            }
            _ => Err(type_error(
                name,
                "a dictionary keyed by UInt8 or UInt32",
                column.data_type(),
            )),
        },
        DataType::Dictionary(_, _) => {
            Err(type_error(name, "a Utf8 dictionary", column.data_type()))
        }
        DataType::Utf8 => Ok(Some(column.as_string::<i32>().value(row))),
        other => Err(type_error(name, "Utf8 or a Utf8 dictionary", other)),
    }
}

fn u64_at(batch: &RecordBatch, name: &str, row: usize) -> Result<Option<u64>, ArrowError> {
    let idx = batch.schema().index_of(name)?;
    let col = batch.column(idx);
    if col.is_null(row) {
        return Ok(None);
    }
    match col.data_type() {
        DataType::UInt64 => Ok(Some(col.as_primitive::<UInt64Type>().value(row))),
        DataType::UInt32 => Ok(Some(u64::from(col.as_primitive::<UInt32Type>().value(row)))),
        other => Err(type_error(name, "UInt32 or UInt64", other)),
    }
}

fn i64_at(batch: &RecordBatch, name: &str, row: usize) -> Result<Option<i64>, ArrowError> {
    let idx = batch.schema().index_of(name)?;
    let col = batch.column(idx);
    if col.is_null(row) {
        return Ok(None);
    }
    match col.data_type() {
        DataType::Int64 => Ok(Some(col.as_primitive::<Int64Type>().value(row))),
        DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None) => Ok(Some(
            col.as_primitive::<TimestampNanosecondType>().value(row),
        )),
        other => Err(type_error(name, "Int64 or timestamp[ns]", other)),
    }
}

fn f64_at(batch: &RecordBatch, name: &str, row: usize) -> Result<Option<f64>, ArrowError> {
    let idx = batch.schema().index_of(name)?;
    let col = batch.column(idx);
    if col.is_null(row) {
        return Ok(None);
    }
    match col.data_type() {
        DataType::Float64 => Ok(Some(col.as_primitive::<Float64Type>().value(row))),
        other => Err(type_error(name, "Float64", other)),
    }
}

fn bool_at(batch: &RecordBatch, name: &str, row: usize) -> Result<Option<bool>, ArrowError> {
    let idx = batch.schema().index_of(name)?;
    let col = batch.column(idx);
    if col.is_null(row) {
        return Ok(None);
    }
    match col.data_type() {
        DataType::Boolean => Ok(Some(col.as_boolean().value(row))),
        other => Err(type_error(name, "Boolean", other)),
    }
}

/// A constraint whose value type cannot address the column is a malformed selector,
/// not an empty result — the typed accessors above report it as an error.
fn column_equals(
    batch: &RecordBatch,
    name: &str,
    row: usize,
    want: &ColumnValue,
) -> Result<bool, ArrowError> {
    Ok(match want {
        ColumnValue::Str(s) => str_at(batch, name, row)? == Some(s.as_str()),
        ColumnValue::U64(v) => u64_at(batch, name, row)? == Some(*v),
        ColumnValue::I64(v) => i64_at(batch, name, row)? == Some(*v),
        ColumnValue::F64(v) => f64_at(batch, name, row)? == Some(*v),
        ColumnValue::Bool(v) => bool_at(batch, name, row)? == Some(*v),
    })
}

fn row_matches(batch: &RecordBatch, row: usize, selector: &Selector) -> Result<bool, ArrowError> {
    if let Some(template) = &selector.template
        && str_at(batch, "message", row)? != Some(template.as_str())
    {
        return Ok(false);
    }
    for (name, want) in &selector.constraints {
        if !column_equals(batch, name, row, want)? {
            return Ok(false);
        }
    }
    Ok(true)
}

impl TraceQuery for ArrowTraceQuery {
    type Error = ArrowError;

    fn count(&self, selector: &Selector) -> Result<usize, Self::Error> {
        self.validate_selector(selector)?;
        let mut matched = 0usize;
        self.visit_matching(selector, |_, _| {
            matched += 1;
            Ok(())
        })?;
        Ok(matched)
    }

    fn all_children_of(&self, child: &Selector, parent: &Selector) -> Result<bool, Self::Error> {
        self.validate_selector(child)?;
        self.validate_selector(parent)?;
        self.require_identity_columns()?;

        let mut parents: std::collections::HashSet<SpanKey> = std::collections::HashSet::new();
        self.visit_matching(parent, |batch, row| {
            if let Some(key) = Self::span_key(batch, row)? {
                parents.insert(key);
            }
            Ok(())
        })?;

        let mut all_parented = true;
        self.visit_matching(child, |batch, row| {
            if all_parented {
                all_parented =
                    Self::parent_key(batch, row)?.is_some_and(|key| parents.contains(&key));
            }
            Ok(())
        })?;
        Ok(all_parented)
    }
}
