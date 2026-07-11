//! SQLite backend matching the TS `SQLiteTracer` table shape
//! (`packages/lmao/src/lib/sqlite/sqlite-common.ts`, `.trace-results.db`):
//!
//! ```sql
//! CREATE TABLE spans (
//!   trace_id TEXT NOT NULL,
//!   span_id INTEGER NOT NULL,
//!   parent_span_id INTEGER NOT NULL,   -- 0 for roots (TS convention)
//!   row_index INTEGER NOT NULL,
//!   entry_type INTEGER NOT NULL,
//!   timestamp_ns INTEGER NOT NULL,
//!   message TEXT,
//!   PRIMARY KEY (trace_id, span_id, row_index)
//! );
//! ```
//!
//! User schema columns are added dynamically via `ALTER TABLE` (sqlite-writer.ts);
//! this backend queries whatever columns exist, so those work transparently.

use rusqlite::{Connection, params};

use arrow_array::cast::AsArray;
use arrow_array::types::{Int64Type, UInt8Type, UInt32Type};
use arrow_array::{Array, RecordBatch};

use crate::{ColumnValue, Selector, TraceQuery};

pub const SPANS_DDL: &str = "
  CREATE TABLE IF NOT EXISTS spans (
    trace_id TEXT NOT NULL,
    span_id INTEGER NOT NULL,
    parent_span_id INTEGER NOT NULL,
    row_index INTEGER NOT NULL,
    entry_type INTEGER NOT NULL,
    timestamp_ns INTEGER NOT NULL,
    message TEXT,
    PRIMARY KEY (trace_id, span_id, row_index)
  );
  CREATE INDEX IF NOT EXISTS idx_spans_trace ON spans(trace_id);
  CREATE INDEX IF NOT EXISTS idx_spans_parent ON spans(trace_id, parent_span_id);
";

pub struct SqliteTraceQuery {
    conn: Connection,
}

impl SqliteTraceQuery {
    pub fn open_in_memory() -> rusqlite::Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(SPANS_DDL)?;
        Ok(Self { conn })
    }

    pub fn open(path: &std::path::Path) -> rusqlite::Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute_batch(SPANS_DDL)?;
        Ok(Self { conn })
    }

    pub fn connection(&self) -> &Connection {
        &self.conn
    }

    /// Load flushed RecordBatches (lmao-arrow `trace_schema`) into the spans table.
    /// `row_index` restarts per (trace_id, span_id), matching the TS writer.
    pub fn load_batches(&mut self, batches: &[RecordBatch]) -> rusqlite::Result<()> {
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO spans (trace_id, span_id, parent_span_id, row_index, entry_type, timestamp_ns, message)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            )?;
            let mut row_indices: std::collections::HashMap<(String, u32), i64> =
                std::collections::HashMap::new();
            for batch in batches {
                let ts = batch.column(0).as_primitive::<Int64Type>();
                let trace = batch.column(1).as_dictionary::<UInt32Type>();
                let trace_values = trace.values().as_string::<i32>();
                let span_ids = batch.column(3).as_primitive::<UInt32Type>();
                let parent_span = batch.column(5).as_primitive::<UInt32Type>();
                let entry = batch.column(6).as_dictionary::<UInt8Type>();
                let message = batch.column(7).as_dictionary::<UInt32Type>();
                let message_values = message.values().as_string::<i32>();
                for row in 0..batch.num_rows() {
                    let trace_id = trace_values.value(trace.keys().value(row) as usize);
                    let span_id = span_ids.value(row);
                    let row_index = row_indices
                        .entry((trace_id.to_string(), span_id))
                        .and_modify(|i| *i += 1)
                        .or_insert(0);
                    // TS convention: roots store parent_span_id = 0 (NOT NULL column).
                    let parent = if parent_span.is_null(row) {
                        0i64
                    } else {
                        parent_span.value(row) as i64
                    };
                    // entry_type stored as the numeric discriminant (dict key + 1).
                    let entry_type = entry.keys().value(row) as i64 + 1;
                    let msg: Option<&str> = (!message.keys().is_null(row))
                        .then(|| message_values.value(message.keys().value(row) as usize));
                    stmt.execute(params![
                        trace_id,
                        span_id,
                        parent,
                        *row_index,
                        entry_type,
                        ts.value(row),
                        msg
                    ])?;
                }
            }
        }
        tx.commit()
    }

    fn where_clause(selector: &Selector) -> (String, Vec<rusqlite::types::Value>) {
        let mut clauses = Vec::new();
        let mut params: Vec<rusqlite::types::Value> = Vec::new();
        if let Some(t) = &selector.template {
            clauses.push("message = ?".to_string());
            params.push(t.clone().into());
        }
        for (name, value) in &selector.constraints {
            // Column names come from test/assertion code, not user input, but quote
            // defensively anyway.
            let quoted = format!("\"{}\"", name.replace('"', "\"\""));
            clauses.push(format!("{quoted} = ?"));
            params.push(match value {
                ColumnValue::U64(v) => (*v as i64).into(),
                ColumnValue::I64(v) => (*v).into(),
                ColumnValue::F64(v) => (*v).into(),
                ColumnValue::Str(s) => s.clone().into(),
                ColumnValue::Bool(b) => (*b as i64).into(),
            });
        }
        let where_sql = if clauses.is_empty() {
            "1=1".to_string()
        } else {
            clauses.join(" AND ")
        };
        (where_sql, params)
    }

    fn query_count(&self, sql: &str, params: &[rusqlite::types::Value]) -> usize {
        self.conn
            .query_row(sql, rusqlite::params_from_iter(params.iter()), |r| {
                r.get::<_, i64>(0)
            })
            .unwrap_or(0) as usize
    }
}

impl TraceQuery for SqliteTraceQuery {
    fn count(&self, selector: &Selector) -> usize {
        let (where_sql, params) = Self::where_clause(selector);
        self.query_count(
            &format!("SELECT COUNT(*) FROM spans WHERE {where_sql}"),
            &params,
        )
    }

    fn all_children_of(&self, child: &Selector, parent: &Selector) -> bool {
        let (child_where, mut params) = Self::where_clause(child);
        let (parent_where, parent_params) = Self::where_clause(parent);
        params.extend(parent_params);
        // Count child rows whose (trace_id, parent_span_id) is NOT a span with a
        // parent-matching row; zero violations = true.
        let sql = format!(
            "SELECT COUNT(*) FROM spans c WHERE {child_where} AND NOT EXISTS (
               SELECT 1 FROM spans p
               WHERE p.trace_id = c.trace_id AND p.span_id = c.parent_span_id
                 AND {parent_where})",
        );
        // Rebind: child params first, then parent params (order matches SQL).
        self.query_count(&sql, &params) == 0
    }
}
