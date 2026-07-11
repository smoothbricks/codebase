//! Backend parity: the SAME selector must produce the SAME answer against the
//! in-process Arrow scan, SQLite (`SQLiteTracer` shape), and DataFusion — the
//! "query layer must not care which tracer produced the table" rule from
//! `AxE/specs/sim/08-trace-testing.md`.

use std::sync::Arc;

use arrow_array::RecordBatch;
use lmao_arrow::{MockSpan, convert_span_trees};
use lmao_core::{SpanIdentity, TraceId};
use lmao_query::{ArrowTraceQuery, Selector, TraceQuery};

/// Two traces; trace A has a root span (id 1) with a child (id 2); templates repeat.
fn fixture_batch() -> RecordBatch {
    let trace_a = TraceId::new("trace-a").unwrap();
    let root_a = Arc::new(SpanIdentity {
        thread_id: 7,
        span_id: 1,
        trace_id: trace_a.clone(),
        parent: None,
    });
    let child_a = Arc::new(SpanIdentity {
        thread_id: 7,
        span_id: 2,
        trace_id: trace_a,
        parent: Some(root_a.clone()),
    });
    let root_b = Arc::new(SpanIdentity {
        thread_id: 9,
        span_id: 3,
        trace_id: TraceId::new("trace-b").unwrap(),
        parent: None,
    });

    let span = |id: Arc<SpanIdentity>, name: &str, logs: &[&str]| MockSpan {
        identity: id,
        timestamps: (0..(2 + logs.len()) as i64).collect(),
        entry_types: [1u8, 2]
            .into_iter()
            .chain(std::iter::repeat_n(5, logs.len()))
            .collect(),
        messages: [Some(name.to_string()), None]
            .into_iter()
            .chain(logs.iter().map(|l| Some(l.to_string())))
            .collect(),
        overflow: None,
        children: vec![],
    };

    let mut root = span(root_a, "handle-request", &["user {id} loaded", "cache {key} hit"]);
    root.children.push(span(
        child_a,
        "db-query",
        &["cache {key} hit", "rows {n} returned"],
    ));
    let other = span(root_b, "handle-request", &["user {id} loaded"]);
    convert_span_trees(&[root, other]).unwrap()
}

fn selectors() -> Vec<(&'static str, Selector)> {
    vec![
        ("by-template", Selector::template("cache {key} hit")),
        ("template+span", Selector::template("cache {key} hit").with("span_id", 2u64)),
        ("by-trace", Selector::default().with("trace_id", "trace-a")),
        ("absent", Selector::template("never-logged {x}")),
        ("by-entry-type-span-start", Selector::template("handle-request")),
        ("timestamp", Selector::default().with("timestamp_ns", 0i64)),
    ]
}

/// The Arrow backend uses lmao-arrow column names; SQLite/DataFusion-over-sqlite-shape
/// use the SQLiteTracer names. Translate the two columns that differ.
fn arrow_flavored(s: &Selector) -> Selector {
    let mut out = s.clone();
    for (name, _) in out.constraints.iter_mut() {
        if name == "timestamp_ns" {
            *name = "timestamp".to_string();
        }
    }
    out
}

#[test]
fn arrow_scan_answers_the_fixture() {
    let q = ArrowTraceQuery::new(vec![fixture_batch()]);
    assert_eq!(q.count(&Selector::template("cache {key} hit")), 2);
    assert_eq!(q.count(&Selector::template("handle-request")), 2);
    assert!(q.never(&Selector::template("never-logged {x}")));
    // db-query rows are children of handle-request spans.
    assert!(q.all_children_of(
        &Selector::template("db-query"),
        &Selector::template("handle-request"),
    ));
    // handle-request roots are NOT children of db-query.
    assert!(!q.all_children_of(
        &Selector::template("handle-request"),
        &Selector::template("db-query"),
    ));
}

#[cfg(feature = "sqlite")]
#[test]
fn sqlite_backend_parity() {
    use lmao_query::sqlite_backend::SqliteTraceQuery;

    let batch = fixture_batch();
    let arrow = ArrowTraceQuery::new(vec![batch.clone()]);
    let mut sqlite = SqliteTraceQuery::open_in_memory().unwrap();
    sqlite.load_batches(&[batch]).unwrap();

    for (name, sel) in selectors() {
        assert_eq!(
            arrow.count(&arrow_flavored(&sel)),
            sqlite.count(&sel),
            "count parity failed for selector {name}"
        );
        assert_eq!(
            arrow.never(&arrow_flavored(&sel)),
            sqlite.never(&sel),
            "never parity failed for selector {name}"
        );
    }
    assert_eq!(
        arrow.all_children_of(
            &Selector::template("db-query"),
            &Selector::template("handle-request")
        ),
        sqlite.all_children_of(
            &Selector::template("db-query"),
            &Selector::template("handle-request")
        ),
    );
}

#[cfg(feature = "datafusion")]
#[test]
fn datafusion_backend_parity() {
    use lmao_query::datafusion_backend::DataFusionTraceQuery;

    let batch = fixture_batch();
    let arrow = ArrowTraceQuery::new(vec![batch.clone()]);
    let df = DataFusionTraceQuery::new(vec![batch]).unwrap();

    for (name, sel) in selectors() {
        // DataFusion queries the Arrow table directly → arrow column names.
        let sel = arrow_flavored(&sel);
        assert_eq!(
            arrow.count(&sel),
            df.count(&sel),
            "count parity failed for selector {name}"
        );
    }
    assert_eq!(
        arrow.all_children_of(
            &Selector::template("db-query"),
            &Selector::template("handle-request")
        ),
        df.all_children_of(
            &Selector::template("db-query"),
            &Selector::template("handle-request")
        ),
    );
}
