//! # lmao-query
//!
//! Trace assertion/query layer per `AxE/specs/sim/08-trace-testing.md`:
//!
//! - "The query layer must not care which tracer produced the table" — the same
//!   [`Selector`] runs against in-process Arrow RecordBatches ([`ArrowTraceQuery`],
//!   always available; DataFusion SQL under the `datafusion` feature) and SQLite
//!   (`.trace-results.db` / `SQLiteTracer` parity, `sqlite` feature).
//! - Assertions select by TEMPLATE/name + typed columns, never rendered text.
//! - Negative assertions ("this event never appears") are first-class.
//! - Ordering asserts use span parentage, not wall-clock across threads:
//!   [`TraceQuery::all_children_of`] checks `(trace_id, parent_span_id)` linkage.
//! - Planned sugar: a Tracetest-style selector language (`span[name="..." attr:x=y]`)
//!   compiling to the same predicate — thin layer, not built yet.

mod arrow_backend;
#[cfg(feature = "datafusion")]
pub mod datafusion_backend;
#[cfg(feature = "sqlite")]
pub mod sqlite_backend;

pub use arrow_backend::ArrowTraceQuery;

/// A predicate over trace rows. Structured (template + typed column constraints),
/// deliberately NOT a rendered-message matcher.
#[derive(Debug, Clone, Default)]
pub struct Selector {
    /// Dictionary-encoded template / span name to match (`message` column).
    /// `None` = any message.
    pub template: Option<String>,
    /// Typed column equality constraints (column name → value).
    pub constraints: Vec<(String, ColumnValue)>,
}

impl Selector {
    pub fn template(t: impl Into<String>) -> Self {
        Self {
            template: Some(t.into()),
            constraints: Vec::new(),
        }
    }

    pub fn with(mut self, column: impl Into<String>, value: impl Into<ColumnValue>) -> Self {
        self.constraints.push((column.into(), value.into()));
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnValue {
    U64(u64),
    I64(i64),
    F64(f64),
    Str(String),
    Bool(bool),
}

impl From<u64> for ColumnValue {
    fn from(v: u64) -> Self {
        Self::U64(v)
    }
}
impl From<i64> for ColumnValue {
    fn from(v: i64) -> Self {
        Self::I64(v)
    }
}
impl From<&str> for ColumnValue {
    fn from(v: &str) -> Self {
        Self::Str(v.to_string())
    }
}
impl From<bool> for ColumnValue {
    fn from(v: bool) -> Self {
        Self::Bool(v)
    }
}

/// Tracer-agnostic query surface (same selector, any backend).
pub trait TraceQuery {
    /// Rows matching the selector (count is enough for most assertions).
    fn count(&self, selector: &Selector) -> usize;

    /// Negative assertion helper: true iff NO row matches. Note (08-trace-testing):
    /// proves absence of an EMITTED event, not absence of underlying work.
    fn never(&self, selector: &Selector) -> bool {
        self.count(selector) == 0
    }

    /// Causal-ordering assertion: every row matching `child` belongs to a span whose
    /// `(trace_id, parent_span_id)` points at a span with a row matching `parent`.
    /// Vacuously true when nothing matches `child` — pair with a positive `count`.
    fn all_children_of(&self, child: &Selector, parent: &Selector) -> bool;
}
