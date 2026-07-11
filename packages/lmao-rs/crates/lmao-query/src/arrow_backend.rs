//! In-process backend: hand-rolled scan over flushed RecordBatches (zero heavy deps;
//! the `datafusion` feature adds a SQL engine over the same batches).

use arrow_array::cast::AsArray;
use arrow_array::types::{Int64Type, UInt8Type, UInt32Type, UInt64Type};
use arrow_array::{Array, RecordBatch};

use crate::{ColumnValue, Selector, TraceQuery};

pub struct ArrowTraceQuery {
    batches: Vec<RecordBatch>,
}

impl ArrowTraceQuery {
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        Self { batches }
    }

    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    fn matching_rows<'a>(
        &'a self,
        selector: &'a Selector,
    ) -> impl Iterator<Item = (&'a RecordBatch, usize)> + 'a {
        self.batches.iter().flat_map(move |batch| {
            (0..batch.num_rows())
                .filter_map(move |row| row_matches(batch, row, selector).then_some((batch, row)))
        })
    }
}

fn dict_str_value(batch: &RecordBatch, name: &str, row: usize) -> Option<String> {
    let idx = batch.schema().index_of(name).ok()?;
    let col = batch.column(idx);
    match col.data_type() {
        arrow_schema::DataType::Dictionary(k, _) => {
            let values = match **k {
                arrow_schema::DataType::UInt8 => {
                    let d = col.as_dictionary::<UInt8Type>();
                    if d.keys().is_null(row) {
                        return None;
                    }
                    let key = d.keys().value(row) as usize;
                    d.values().as_string::<i32>().value(key).to_string()
                }
                arrow_schema::DataType::UInt32 => {
                    let d = col.as_dictionary::<UInt32Type>();
                    if d.keys().is_null(row) {
                        return None;
                    }
                    let key = d.keys().value(row) as usize;
                    d.values().as_string::<i32>().value(key).to_string()
                }
                _ => return None,
            };
            Some(values)
        }
        arrow_schema::DataType::Utf8 => {
            let s = col.as_string::<i32>();
            (!s.is_null(row)).then(|| s.value(row).to_string())
        }
        _ => None,
    }
}

fn column_equals(batch: &RecordBatch, name: &str, row: usize, want: &ColumnValue) -> bool {
    let Ok(idx) = batch.schema().index_of(name) else {
        return false;
    };
    let col = batch.column(idx);
    if col.is_null(row) {
        return false;
    }
    match want {
        ColumnValue::Str(s) => dict_str_value(batch, name, row).as_deref() == Some(s.as_str()),
        ColumnValue::U64(v) => match col.data_type() {
            arrow_schema::DataType::UInt64 => col.as_primitive::<UInt64Type>().value(row) == *v,
            arrow_schema::DataType::UInt32 => {
                col.as_primitive::<UInt32Type>().value(row) as u64 == *v
            }
            _ => false,
        },
        ColumnValue::I64(v) => match col.data_type() {
            arrow_schema::DataType::Int64 => col.as_primitive::<Int64Type>().value(row) == *v,
            _ => false,
        },
        ColumnValue::F64(v) => match col.data_type() {
            arrow_schema::DataType::Float64 => {
                col.as_primitive::<arrow_array::types::Float64Type>()
                    .value(row)
                    == *v
            }
            _ => false,
        },
        ColumnValue::Bool(v) => col
            .as_boolean_opt()
            .is_some_and(|b| !b.is_null(row) && b.value(row) == *v),
    }
}

fn row_matches(batch: &RecordBatch, row: usize, selector: &Selector) -> bool {
    if let Some(template) = &selector.template
        && dict_str_value(batch, "message", row).as_deref() != Some(template.as_str())
    {
        return false;
    }
    selector
        .constraints
        .iter()
        .all(|(name, want)| column_equals(batch, name, row, want))
}

impl TraceQuery for ArrowTraceQuery {
    fn count(&self, selector: &Selector) -> usize {
        self.matching_rows(selector).count()
    }

    fn all_children_of(&self, child: &Selector, parent: &Selector) -> bool {
        // Parent identity set: (trace_id, span_id) of rows matching `parent`.
        let parents: std::collections::HashSet<(String, u64)> = self
            .matching_rows(parent)
            .filter_map(|(batch, row)| {
                let trace = dict_str_value(batch, "trace_id", row)?;
                let span = batch
                    .column(batch.schema().index_of("span_id").ok()?)
                    .as_primitive::<UInt32Type>()
                    .value(row) as u64;
                Some((trace, span))
            })
            .collect();

        self.matching_rows(child).all(|(batch, row)| {
            let Some(trace) = dict_str_value(batch, "trace_id", row) else {
                return false;
            };
            let Ok(idx) = batch.schema().index_of("parent_span_id") else {
                return false;
            };
            let col = batch.column(idx).as_primitive::<UInt32Type>();
            !col.is_null(row) && parents.contains(&(trace, col.value(row) as u64))
        })
    }
}
