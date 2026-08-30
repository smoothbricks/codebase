//! Arrow scan answers: typed `count` / `never` / `all_children_of` over the
//! in-process RecordBatch fixture, plus stable-vs-dynamic vocabulary parity.

use std::sync::Arc;

use arrow_array::RecordBatch;
use lmao_arrow::{
    MockSpan, StableVocabularyCatalog, StableVocabularyEntry, StableVocabularyKind,
    build_trace_chunk_envelope, convert_span_trees,
};
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
        packed_headers: [1u32, 2]
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

    let mut root = span(
        root_a,
        "handle-request",
        &["user {id} loaded", "cache {key} hit"],
    );
    root.children.push(span(
        child_a,
        "db-query",
        &["cache {key} hit", "rows {n} returned"],
    ));
    let other = span(root_b, "handle-request", &["user {id} loaded"]);
    let empty_catalog = StableVocabularyCatalog::EMPTY;
    convert_span_trees(&[root, other], &empty_catalog).unwrap()
}

#[test]
fn arrow_scan_answers_the_fixture() {
    let q = ArrowTraceQuery::new(vec![fixture_batch()]);
    assert_eq!(
        q.count(&Selector::template("cache {key} hit"))
            .expect("valid template selector"),
        2
    );
    assert_eq!(
        q.count(&Selector::template("handle-request"))
            .expect("valid template selector"),
        2
    );
    assert!(
        q.never(&Selector::template("never-logged {x}"))
            .expect("valid template selector")
    );
    // db-query rows are children of handle-request spans.
    assert!(
        q.all_children_of(
            &Selector::template("db-query"),
            &Selector::template("handle-request"),
        )
        .expect("valid parentage selectors")
    );
    // handle-request roots are NOT children of db-query.
    assert!(
        !q.all_children_of(
            &Selector::template("handle-request"),
            &Selector::template("db-query"),
        )
        .expect("valid parentage selectors")
    );
}

#[test]
fn unknown_column_is_a_query_error() {
    let q = ArrowTraceQuery::new(vec![fixture_batch()]);
    let selector = Selector::default().with("missing_column", 1_u64);

    let error = q
        .count(&selector)
        .expect_err("a selector column absent from every batch must fail");
    assert!(error.to_string().contains("missing_column"), "{error}");
}

#[test]
fn stable_and_dynamic_vocabulary_have_query_and_archive_parity() {
    const STATIC_LOG_ID: u32 = 0x0055_7011;
    const STATIC_SPAN_ID: u32 = 0x00A0_3022;
    static ENTRIES: [StableVocabularyEntry<'static>; 2] = [
        StableVocabularyEntry {
            id: STATIC_LOG_ID,
            kind: StableVocabularyKind::LogTemplate,
            value: "search {term}",
        },
        StableVocabularyEntry {
            id: STATIC_SPAN_ID,
            kind: StableVocabularyKind::SpanName,
            value: "query-span",
        },
    ];
    // Value order is query-span, search {term}, the reverse of stable-ID order.
    static VALUE_ORDER: [u32; 2] = [1, 0];

    let identity = || {
        Arc::new(SpanIdentity {
            thread_id: 31,
            span_id: 7,
            trace_id: TraceId::new("vocabulary-parity").unwrap(),
            parent: None,
        })
    };
    let dynamic = MockSpan {
        identity: identity(),
        timestamps: vec![10, 20, 30],
        packed_headers: vec![1, 2, 8],
        messages: vec![
            Some("query-span".into()),
            None,
            Some("search {term}".into()),
        ],
        overflow: None,
        children: vec![],
    };
    let static_rows = MockSpan {
        identity: identity(),
        timestamps: vec![10, 20, 30],
        packed_headers: vec![(STATIC_SPAN_ID << 8) | 1, 2, (STATIC_LOG_ID << 8) | 8],
        messages: vec![None, None, None],
        overflow: None,
        children: vec![],
    };
    let catalog = StableVocabularyCatalog::try_new(&ENTRIES, &VALUE_ORDER).unwrap();
    let empty_catalog = StableVocabularyCatalog::EMPTY;
    let dynamic_batch = convert_span_trees(&[dynamic], &empty_catalog).unwrap();
    let static_batch = convert_span_trees(&[static_rows], &catalog).unwrap();
    let dynamic_query = ArrowTraceQuery::new(vec![dynamic_batch.clone()]);
    let static_query = ArrowTraceQuery::new(vec![static_batch.clone()]);

    for (name, selector, expected) in [
        ("log template", Selector::template("search {term}"), 1),
        ("span name", Selector::template("query-span"), 1),
        ("absent", Selector::template("never emitted"), 0),
    ] {
        assert_eq!(
            dynamic_query
                .count(&selector)
                .expect("valid dynamic selector"),
            expected,
            "dynamic {name}"
        );
        assert_eq!(
            static_query.count(&selector).expect("valid static selector"),
            expected,
            "static {name}"
        );
    }
    assert_eq!(
        build_trace_chunk_envelope("archive://fixture", &dynamic_batch),
        build_trace_chunk_envelope("archive://fixture", &static_batch),
        "archive identity and bounds are independent of vocabulary encoding",
    );
}
