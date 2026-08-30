//! Differential oracle for [`ArrowTraceQuery`].
//!
//! `ArrowTraceQuery` is the only backend, so nothing else in the tree can catch it
//! being wrong. This file is the independent arm.
//!
//! **Structure, and why it is this shape.** Both arms render ONE [`SpanSpec`] list:
//! the Arrow arm through `convert_span_trees` into a `RecordBatch` scanned by the
//! production code, the model arm into a flat `Vec<ModelRow>` evaluated by a naive
//! implementation that is correct by inspection. The model is NEVER derived from the
//! batch — an oracle built by reading the artifact under test is wrong in exactly the
//! ways that artifact is wrong.
//!
//! **Error parity is asserted, not just value parity** (handbook §7.10f). The previous
//! oracle's SQLite arm ended its count in `.unwrap_or(0)`, so a failed query returned
//! 0 rows, which made `never()` report "this event never appears" and
//! `all_children_of()` report "every child is correctly parented". A value-only
//! comparison cannot see that: it needs an arm that distinguishes `Err` from `0`.
//! Nothing in this file may coerce an error into a value.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use lmao_arrow::{ENTRY_TYPE_NAMES, MockSpan, StableVocabularyCatalog, convert_span_trees};
use lmao_core::{SpanIdentity, TraceId};
use lmao_query::{ArrowTraceQuery, ColumnValue, Selector, TraceQuery};
use proptest::prelude::*;

/// Packed row header entry types (`(vocabulary_id << 8) | entry_type`, id 0 = dynamic).
const ENTRY_SPAN_START: u32 = 1;
const ENTRY_SPAN_OK: u32 = 2;
const ENTRY_INFO: u32 = 8;

fn entry_name(entry_type: u32) -> &'static str {
    ENTRY_TYPE_NAMES[entry_type as usize]
}

// ---------------------------------------------------------------------------
// The specification both arms render
// ---------------------------------------------------------------------------

/// One logical span. `parent` names another spec's `(thread_id, span_id)`; that spec
/// MUST appear earlier in the list, which is what makes the forest well-founded.
#[derive(Debug, Clone)]
struct SpanSpec {
    trace: String,
    thread_id: u64,
    span_id: u32,
    parent: Option<(u64, u32)>,
    name: String,
    logs: Vec<String>,
}

impl SpanSpec {
    fn root(trace: &str, thread_id: u64, span_id: u32, name: &str) -> Self {
        Self {
            trace: trace.to_string(),
            thread_id,
            span_id,
            parent: None,
            name: name.to_string(),
            logs: Vec::new(),
        }
    }

    fn child_of(mut self, parent: (u64, u32)) -> Self {
        self.parent = Some(parent);
        self
    }

    fn with_logs(mut self, logs: &[&str]) -> Self {
        self.logs = logs.iter().map(|l| (*l).to_string()).collect();
        self
    }

    /// Row shape: span-start carrying the name, span-ok carrying no message, then one
    /// info row per log template. Matches what the conversion emits per buffer.
    fn row_headers(&self) -> (Vec<u32>, Vec<Option<String>>) {
        let mut headers = vec![ENTRY_SPAN_START, ENTRY_SPAN_OK];
        let mut messages = vec![Some(self.name.clone()), None];
        for log in &self.logs {
            headers.push(ENTRY_INFO);
            messages.push(Some(log.clone()));
        }
        (headers, messages)
    }
}

// ---------------------------------------------------------------------------
// Arm A: the production Arrow scan
// ---------------------------------------------------------------------------

fn build_batch(specs: &[SpanSpec]) -> RecordBatch {
    let mut identities: HashMap<(u64, u32), Arc<SpanIdentity>> = HashMap::new();
    for spec in specs {
        let parent = spec.parent.map(|key| {
            identities
                .get(&key)
                .expect("a spec's parent must appear before it")
                .clone()
        });
        identities.insert(
            (spec.thread_id, spec.span_id),
            Arc::new(SpanIdentity {
                thread_id: spec.thread_id,
                span_id: spec.span_id,
                trace_id: TraceId::new(spec.trace.as_str()).expect("valid trace id"),
                parent,
            }),
        );
    }

    let mut nodes: Vec<Option<MockSpan>> = specs
        .iter()
        .map(|spec| {
            let (packed_headers, messages) = spec.row_headers();
            Some(MockSpan {
                identity: identities[&(spec.thread_id, spec.span_id)].clone(),
                timestamps: (0..packed_headers.len() as i64).collect(),
                packed_headers,
                messages,
                overflow: None,
                children: Vec::new(),
            })
        })
        .collect();

    let index: HashMap<(u64, u32), usize> = specs
        .iter()
        .enumerate()
        .map(|(i, s)| ((s.thread_id, s.span_id), i))
        .collect();

    // Descending: a child is fully assembled before its parent is reached, and a
    // parent's index is always lower than its child's, so its slot is still occupied.
    for i in (0..specs.len()).rev() {
        if let Some(parent_key) = specs[i].parent {
            let node = nodes[i].take().expect("each node is moved at most once");
            let parent = index[&parent_key];
            nodes[parent]
                .as_mut()
                .expect("parent precedes child")
                .children
                .push(node);
        }
    }

    let roots: Vec<MockSpan> = nodes.into_iter().flatten().collect();
    convert_span_trees(&roots, &StableVocabularyCatalog::EMPTY).expect("fixture converts")
}

// ---------------------------------------------------------------------------
// Arm B: the naive model
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct ModelRow {
    timestamp: i64,
    trace_id: String,
    thread_id: u64,
    span_id: u32,
    parent_thread_id: Option<u64>,
    parent_span_id: Option<u32>,
    entry_type: &'static str,
    message: Option<String>,
    line: u32,
}

/// The model's error channel. A selector naming a column the model does not carry, or
/// constraining one with a value of the wrong type, is an error — never zero matching
/// rows. Both arms must agree on WHICH selectors are answerable at all.
#[derive(Debug, PartialEq, Eq)]
enum ModelError {
    UnknownColumn(String),
    TypeMismatch(String),
}

impl std::fmt::Display for ModelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownColumn(c) => write!(f, "unknown column {c}"),
            Self::TypeMismatch(c) => write!(f, "column {c} constrained by the wrong type"),
        }
    }
}

#[derive(Debug)]
struct ModelQuery {
    rows: Vec<ModelRow>,
}

impl ModelQuery {
    const COLUMNS: [&'static str; 12] = [
        "timestamp",
        "trace_id",
        "thread_id",
        "span_id",
        "parent_thread_id",
        "parent_span_id",
        "entry_type",
        "package_name",
        "package_file",
        "git_sha",
        "message",
        "line",
    ];

    fn new(specs: &[SpanSpec]) -> Self {
        let parents: HashMap<(u64, u32), (u64, u32)> = specs
            .iter()
            .filter_map(|s| s.parent.map(|p| ((s.thread_id, s.span_id), p)))
            .collect();
        let mut rows = Vec::new();
        for spec in specs {
            let (headers, messages) = spec.row_headers();
            let parent = parents.get(&(spec.thread_id, spec.span_id)).copied();
            for (row, (entry, message)) in headers.into_iter().zip(messages).enumerate() {
                rows.push(ModelRow {
                    timestamp: row as i64,
                    trace_id: spec.trace.clone(),
                    thread_id: spec.thread_id,
                    span_id: spec.span_id,
                    parent_thread_id: parent.map(|(t, _)| t),
                    parent_span_id: parent.map(|(_, s)| s),
                    entry_type: entry_name(entry),
                    message,
                    line: 0,
                });
            }
        }
        Self { rows }
    }

    fn validate(&self, selector: &Selector) -> Result<(), ModelError> {
        for (name, _) in &selector.constraints {
            if !Self::COLUMNS.contains(&name.as_str()) {
                return Err(ModelError::UnknownColumn(name.clone()));
            }
        }
        Ok(())
    }

    fn cell_equals(row: &ModelRow, name: &str, want: &ColumnValue) -> Result<bool, ModelError> {
        Ok(match (name, want) {
            ("timestamp", ColumnValue::I64(v)) => row.timestamp == *v,
            ("trace_id", ColumnValue::Str(s)) => row.trace_id == *s,
            ("thread_id", ColumnValue::U64(v)) => row.thread_id == *v,
            ("span_id", ColumnValue::U64(v)) => u64::from(row.span_id) == *v,
            ("parent_thread_id", ColumnValue::U64(v)) => row.parent_thread_id == Some(*v),
            ("parent_span_id", ColumnValue::U64(v)) => {
                row.parent_span_id.map(u64::from) == Some(*v)
            }
            ("entry_type", ColumnValue::Str(s)) => row.entry_type == s.as_str(),
            ("package_name" | "package_file" | "git_sha", ColumnValue::Str(_)) => false,
            ("message", ColumnValue::Str(s)) => row.message.as_deref() == Some(s.as_str()),
            ("line", ColumnValue::U64(v)) => u64::from(row.line) == *v,
            // A value type that cannot address the column is a malformed selector, so
            // it is an error rather than a silent non-match.
            _ => return Err(ModelError::TypeMismatch(name.to_string())),
        })
    }

    fn row_matches(row: &ModelRow, selector: &Selector) -> Result<bool, ModelError> {
        if let Some(template) = &selector.template
            && row.message.as_deref() != Some(template.as_str())
        {
            return Ok(false);
        }
        for (name, want) in &selector.constraints {
            if !Self::cell_equals(row, name, want)? {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

impl TraceQuery for ModelQuery {
    type Error = ModelError;

    fn count(&self, selector: &Selector) -> Result<usize, Self::Error> {
        self.validate(selector)?;
        let mut matched = 0usize;
        for row in &self.rows {
            if Self::row_matches(row, selector)? {
                matched += 1;
            }
        }
        Ok(matched)
    }

    fn all_children_of(&self, child: &Selector, parent: &Selector) -> Result<bool, Self::Error> {
        self.validate(child)?;
        self.validate(parent)?;
        // FULL span identity. Span ids are per-thread counters, so `(trace_id, span_id)`
        // alone conflates same-numbered spans on different threads.
        let mut parents: HashSet<(&str, u64, u32)> = HashSet::new();
        for row in &self.rows {
            if Self::row_matches(row, parent)? {
                parents.insert((row.trace_id.as_str(), row.thread_id, row.span_id));
            }
        }
        for row in &self.rows {
            if !Self::row_matches(row, child)? {
                continue;
            }
            let parented = match (row.parent_thread_id, row.parent_span_id) {
                (Some(thread), Some(span)) => {
                    parents.contains(&(row.trace_id.as_str(), thread, span))
                }
                _ => false,
            };
            if !parented {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

// ---------------------------------------------------------------------------
// Regression tests: one per confirmed defect, named for the mechanism
// ---------------------------------------------------------------------------

/// Span ids are per-thread counters, so two spans in one trace routinely share a
/// span_id across threads. Parentage keyed on `(trace_id, span_id)` cannot tell the
/// child's real parent from the same-numbered span on another thread, and answers the
/// causal assertion with its PASS value.
#[test]
fn cross_thread_span_id_collision_is_not_parentage() {
    let specs = vec![
        SpanSpec::root("t", 1, 5, "real-parent"),
        SpanSpec::root("t", 2, 5, "impostor"),
        SpanSpec::root("t", 1, 9, "child").child_of((1, 5)),
    ];
    let arrow = ArrowTraceQuery::new(vec![build_batch(&specs)]);
    let model = ModelQuery::new(&specs);

    let child = Selector::template("child");
    let impostor = Selector::template("impostor");
    let real = Selector::template("real-parent");

    // Positive control: the real parent must still satisfy the assertion, so a fix
    // that simply always answers false cannot pass this test.
    assert!(
        model.all_children_of(&child, &real).unwrap(),
        "model: the thread-1 root is the child's parent"
    );
    assert!(
        arrow.all_children_of(&child, &real).unwrap(),
        "arrow: the thread-1 root is the child's parent"
    );

    assert!(
        !model.all_children_of(&child, &impostor).unwrap(),
        "model: a span on another thread is not the parent"
    );
    assert!(
        !arrow.all_children_of(&child, &impostor).unwrap(),
        "arrow: cross-thread span_id collision reported as parentage"
    );
}

/// `validate_selector` iterates the batches, so with none it validates nothing and an
/// unknown column becomes zero matching rows instead of an error — the same
/// wrong-success shape as `unwrap_or(0)`, reached through an empty collection.
#[test]
fn unknown_column_errors_with_no_batches() {
    let arrow = ArrowTraceQuery::new(Vec::new());
    let selector = Selector::default().with("missing_column", 1_u64);

    let error = arrow
        .count(&selector)
        .expect_err("an unknown column is an error whether or not any batch exists");
    assert!(error.to_string().contains("missing_column"), "{error}");
}

/// `require_columns` checks presence, not type, so a mistyped identity column used to
/// reach `as_primitive::<UInt32Type>()` and ABORT THE PROCESS. A schema the reader
/// cannot interpret is an operational failure and belongs in the error channel.
///
/// `Int64` is the interesting case rather than `UInt64`: widening an unsigned span id
/// from 32 to 64 bits is lossless and is deliberately accepted, whereas a signed
/// column is not a span id the reader can trust.
#[test]
fn wrong_column_type_errors_rather_than_panicking() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("thread_id", DataType::UInt64, false),
        // The trace schema declares UInt32 for both span id columns.
        Field::new("span_id", DataType::Int64, false),
        Field::new("parent_thread_id", DataType::UInt64, true),
        Field::new("parent_span_id", DataType::UInt64, true),
        Field::new("message", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["t"])),
            Arc::new(UInt64Array::from(vec![1_u64])),
            Arc::new(Int64Array::from(vec![9_i64])),
            Arc::new(UInt64Array::from(vec![Some(1_u64)])),
            Arc::new(UInt64Array::from(vec![Some(5_u64)])),
            Arc::new(StringArray::from(vec![Some("child")])),
        ],
    )
    .expect("fixture batch");

    let arrow = ArrowTraceQuery::new(vec![batch]);
    let error = arrow
        .all_children_of(&Selector::template("child"), &Selector::template("parent"))
        .expect_err("a span_id column of the wrong type is a query error");
    assert!(error.to_string().contains("span_id"), "{error}");
}

/// The trace schema uses UInt32 span ids, so a `RecordBatch` built by the conversion
/// must remain answerable — guards the type check above against over-tightening.
#[test]
fn declared_schema_types_remain_answerable() {
    let specs = vec![
        SpanSpec::root("t", 1, 1, "root").with_logs(&["hit {k}"]),
        SpanSpec::root("t", 1, 2, "leaf").child_of((1, 1)),
    ];
    let arrow = ArrowTraceQuery::new(vec![build_batch(&specs)]);

    assert_eq!(arrow.count(&Selector::template("hit {k}")).unwrap(), 1);
    assert!(
        arrow
            .all_children_of(&Selector::template("leaf"), &Selector::template("root"))
            .unwrap()
    );
}

/// A constraint whose value type cannot address the column matched nothing, which is
/// the same wrong-success shape one level down: the selector is malformed, so `never()`
/// would report the event absent. Both arms must refuse it.
#[test]
fn type_mismatched_constraint_errors_on_both_arms() {
    let specs = vec![SpanSpec::root("t", 1, 1, "root")];
    let arrow = ArrowTraceQuery::new(vec![build_batch(&specs)]);
    let model = ModelQuery::new(&specs);
    // `timestamp` is Int64; a U64 constraint cannot address it.
    let selector = Selector::default().with("timestamp", 0_u64);

    let error = arrow
        .count(&selector)
        .expect_err("a U64 constraint on an Int64 column is a malformed selector");
    assert!(error.to_string().contains("timestamp"), "{error}");
    assert!(model.count(&selector).is_err(), "model must refuse it too");
}

// ---------------------------------------------------------------------------
// Property: the two arms agree, on values AND on errors
// ---------------------------------------------------------------------------

const TRACES: [&str; 2] = ["trace-a", "trace-b"];
const NAMES: [&str; 3] = ["handle-request", "db-query", "render"];
const LOGS: [&str; 2] = ["user {id} loaded", "cache {key} hit"];

/// Small pools for threads and span ids so ids collide across threads — the shape the
/// cross-thread defect lives in — while `(thread_id, span_id)` stays unique.
fn spec_forest() -> impl Strategy<Value = Vec<SpanSpec>> {
    prop::collection::vec(
        (
            0..TRACES.len(),
            1..4u64,
            1..4u32,
            0..NAMES.len(),
            0..=LOGS.len(),
            prop::option::of(0..8usize),
        ),
        1..8,
    )
    .prop_map(|raw| {
        let mut specs: Vec<SpanSpec> = Vec::new();
        let mut used: HashSet<(u64, u32)> = HashSet::new();
        for (trace, thread_id, span_id, name, log_count, parent_pick) in raw {
            if !used.insert((thread_id, span_id)) {
                continue;
            }
            let mut spec = SpanSpec::root(TRACES[trace], thread_id, span_id, NAMES[name])
                .with_logs(&LOGS[..log_count]);
            // A parent must already exist; a child inherits its parent's trace so the
            // linkage is meaningful.
            if let Some(pick) = parent_pick
                && !specs.is_empty()
            {
                let parent = &specs[pick % specs.len()];
                let key = (parent.thread_id, parent.span_id);
                if key != (thread_id, span_id) {
                    spec.trace = parent.trace.clone();
                    spec = spec.child_of(key);
                }
            }
            specs.push(spec);
        }
        specs
    })
}

fn selector_strategy() -> impl Strategy<Value = Selector> {
    prop_oneof![
        (0..NAMES.len()).prop_map(|i| Selector::template(NAMES[i])),
        (0..LOGS.len()).prop_map(|i| Selector::template(LOGS[i])),
        Just(Selector::template("never emitted")),
        (0..TRACES.len()).prop_map(|i| Selector::default().with("trace_id", TRACES[i])),
        (1..4u64).prop_map(|t| Selector::default().with("thread_id", t)),
        (1..4u64).prop_map(|s| Selector::default().with("span_id", s)),
        (1..4u64).prop_map(|s| Selector::default().with("parent_span_id", s)),
        (1..4u64).prop_map(|t| Selector::default().with("parent_thread_id", t)),
        Just(Selector::default().with("entry_type", "span-start")),
        Just(Selector::default().with("entry_type", "info")),
        (0..3i64).prop_map(|ts| Selector::default().with("timestamp", ts)),
        (0..NAMES.len(), 1..4u64)
            .prop_map(|(i, t)| Selector::template(NAMES[i]).with("thread_id", t)),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// `count` and `never` agree between the production scan and the model.
    #[test]
    fn count_agrees_with_the_model(specs in spec_forest(), selector in selector_strategy()) {
        let arrow = ArrowTraceQuery::new(vec![build_batch(&specs)]);
        let model = ModelQuery::new(&specs);

        let arrow_count = arrow.count(&selector).expect("modelled columns are valid");
        let model_count = model.count(&selector).expect("modelled columns are valid");
        prop_assert_eq!(arrow_count, model_count, "count for {:?}", selector);
        prop_assert_eq!(
            arrow.never(&selector).expect("valid"),
            model.never(&selector).expect("valid"),
            "never for {:?}",
            selector
        );
    }

    /// `all_children_of` agrees, including where a span id repeats across threads.
    #[test]
    fn parentage_agrees_with_the_model(
        specs in spec_forest(),
        child in selector_strategy(),
        parent in selector_strategy(),
    ) {
        let arrow = ArrowTraceQuery::new(vec![build_batch(&specs)]);
        let model = ModelQuery::new(&specs);

        prop_assert_eq!(
            arrow.all_children_of(&child, &parent).expect("valid"),
            model.all_children_of(&child, &parent).expect("valid"),
            "all_children_of({:?}, {:?})",
            child,
            parent
        );
    }

    /// Error parity. An unknown column must be an error on BOTH arms: an arm that
    /// answers 0 where the other answers `Err` is the wrong-success channel this
    /// oracle exists to detect, and it is invisible to a value-only comparison.
    #[test]
    fn unknown_columns_error_on_both_arms(specs in spec_forest(), selector in selector_strategy()) {
        let arrow = ArrowTraceQuery::new(vec![build_batch(&specs)]);
        let model = ModelQuery::new(&specs);
        let poisoned = selector.clone().with("no_such_column", 1_u64);

        prop_assert!(
            arrow.count(&poisoned).is_err(),
            "arrow scan answered a selector naming an absent column"
        );
        prop_assert!(
            model.count(&poisoned).is_err(),
            "model answered a selector naming an absent column"
        );
        prop_assert!(arrow.never(&poisoned).is_err(), "never must not swallow the error");
        prop_assert!(
            arrow.all_children_of(&poisoned, &selector).is_err(),
            "all_children_of must not swallow the error"
        );
    }
}
