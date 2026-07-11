//! DataFusion backend: SQL over the same in-process RecordBatches.
//!
//! This is the Rust equivalent of the planned DuckDB query engine
//! (`specs/lmao/02_query_engine.md`) for the in-process case: zero-copy over Arrow,
//! full SQL for exploratory queries, while [`crate::ArrowTraceQuery`] stays the
//! dependency-light default for assertions.

use std::sync::Arc;

use arrow_array::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;

use crate::{ColumnValue, Selector, TraceQuery};

pub struct DataFusionTraceQuery {
    ctx: SessionContext,
    rt: tokio::runtime::Runtime,
}

impl DataFusionTraceQuery {
    pub fn new(batches: Vec<RecordBatch>) -> datafusion::error::Result<Self> {
        let ctx = SessionContext::new();
        if let Some(first) = batches.first() {
            let table = MemTable::try_new(first.schema(), vec![batches.clone()])?;
            ctx.register_table("spans", Arc::new(table))?;
        }
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio current-thread runtime");
        Ok(Self { ctx, rt })
    }

    /// Run arbitrary SQL over the `spans` table (exploratory surface; assertions
    /// should go through [`TraceQuery`]).
    pub fn sql_count(&self, sql: &str) -> datafusion::error::Result<usize> {
        self.rt.block_on(async {
            let df = self.ctx.sql(sql).await?;
            let rows: usize = df.collect().await?.iter().map(|b| b.num_rows()).sum();
            Ok(rows)
        })
    }

    fn where_clause(selector: &Selector) -> String {
        let mut clauses = Vec::new();
        if let Some(t) = &selector.template {
            clauses.push(format!("message = '{}'", escape(t)));
        }
        for (name, value) in &selector.constraints {
            let quoted = format!("\"{}\"", name.replace('"', "\"\""));
            let literal = match value {
                ColumnValue::U64(v) => v.to_string(),
                ColumnValue::I64(v) => v.to_string(),
                ColumnValue::F64(v) => v.to_string(),
                ColumnValue::Str(s) => format!("'{}'", escape(s)),
                ColumnValue::Bool(b) => b.to_string(),
            };
            clauses.push(format!("{quoted} = {literal}"));
        }
        if clauses.is_empty() {
            "1=1".to_string()
        } else {
            clauses.join(" AND ")
        }
    }
}

fn escape(s: &str) -> String {
    s.replace('\'', "''")
}

impl TraceQuery for DataFusionTraceQuery {
    fn count(&self, selector: &Selector) -> usize {
        let where_sql = Self::where_clause(selector);
        self.sql_count(&format!("SELECT * FROM spans WHERE {where_sql}"))
            .unwrap_or(0)
    }

    fn all_children_of(&self, child: &Selector, parent: &Selector) -> bool {
        let child_where = Self::where_clause(child);
        let parent_where = Self::where_clause(parent);
        let sql = format!(
            "SELECT * FROM spans c WHERE {child_where} AND NOT EXISTS (
               SELECT 1 FROM spans p
               WHERE p.trace_id = c.trace_id AND p.span_id = c.parent_span_id
                 AND {parent_where})",
        );
        // Unqualified columns inside the subquery bind to `p`, outer ones to `c`,
        // mirroring the SQLite backend's scoping.
        self.sql_count(&sql).map(|n| n == 0).unwrap_or(false)
    }
}
