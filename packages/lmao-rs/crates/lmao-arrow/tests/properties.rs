//! Determinism properties for the Arrow conversion layer.

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::Array;
use arrow_array::cast::AsArray;
use arrow_array::types::UInt32Type;
use lmao_arrow::{ColumnDictionary, MockSpan, build_trace_chunk_envelope, convert_span_trees};
use lmao_arrow::{
    VOCABULARY_DENSE_INDICES, VOCABULARY_IDS, VOCABULARY_VALUES, static_vocabulary_dictionary,
};
use lmao_core::{SpanIdentity, TraceId};
use proptest::prelude::*;

proptest! {
    /// Sorted-dictionary determinism: any permutation of the same value multiset
    /// finalizes to the identical dictionary. This is the foundation of AxE's
    /// "same seed → bit-identical trace bytes" requirement at the encoding layer.
    #[test]
    fn dictionary_is_permutation_invariant(
        mut values in prop::collection::vec("[a-z]{0,12}", 0..100),
        seed in any::<u64>(),
    ) {
        let mut d1 = ColumnDictionary::default();
        for v in &values {
            d1.observe(v);
        }
        // Finalize to owned strings before shuffling (keys borrow from `values`).
        let f1 = d1.finalize();
        // Deterministic pseudo-shuffle from the seed (no rand dep).
        let len = values.len();
        if len > 1 {
            let mut s = seed | 1;
            for i in (1..len).rev() {
                s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                values.swap(i, (s as usize) % (i + 1));
            }
        }
        let mut d2 = ColumnDictionary::default();
        for v in &values {
            d2.observe(v);
        }
        prop_assert_eq!(f1, d2.finalize());
    }
}

/// Deterministic tree generator: `n` events spread over root + child + overflow,
/// message templates drawn from a small pool (dictionary-shaped, like real traces).
fn build_tree(n: usize, trace: &str) -> MockSpan {
    let trace_id = TraceId::new(trace).unwrap();
    let root_id = Arc::new(SpanIdentity {
        thread_id: 0xDEAD_BEEF_0000_0001,
        span_id: 1,
        trace_id: trace_id.clone(),
        parent: None,
    });
    let child_id = Arc::new(SpanIdentity {
        thread_id: 0xDEAD_BEEF_0000_0001,
        span_id: 2,
        trace_id,
        parent: Some(root_id.clone()),
    });
    let templates = ["user {id} created", "cache {key} hit", "retry {n}"];
    let row = |i: usize| {
        (
            1_700_000_000_000_000_000 + i as i64,
            [8u32, 7, 9, 10][i % 4], // info/debug/warn/error
            Some(templates[i % templates.len()].to_string()),
        )
    };
    let mut child = MockSpan {
        identity: child_id,
        timestamps: vec![10],
        packed_headers: vec![1],
        messages: vec![Some("child-span".into())],
        overflow: None,
        children: vec![],
    };
    let mut root = MockSpan {
        identity: root_id,
        timestamps: vec![0, 1],
        packed_headers: vec![1, 2],
        messages: vec![Some("root-span".into()), None],
        overflow: None,
        children: vec![],
    };
    // Split events across the child's overflow chain to exercise chain contiguity.
    let mut overflow = MockSpan {
        identity: child.identity.clone(),
        timestamps: vec![],
        packed_headers: vec![],
        messages: vec![],
        overflow: None,
        children: vec![],
    };
    for i in 0..n {
        let (ts, et, msg) = row(i);
        let target = if i % 2 == 0 {
            &mut child
        } else {
            &mut overflow
        };
        target.timestamps.push(ts);
        target.packed_headers.push(et);
        target.messages.push(msg);
    }
    if !overflow.timestamps.is_empty() {
        child.overflow = Some(Box::new(overflow));
    }
    root.children.push(child);
    root
}

fn ipc_bytes(batch: &arrow_array::RecordBatch) -> Vec<u8> {
    let mut out = Vec::new();
    {
        let mut w = arrow_ipc::writer::StreamWriter::try_new(&mut out, &batch.schema()).unwrap();
        w.write(batch).unwrap();
        w.finish().unwrap();
    }
    out
}

const DENSE_ZERO_LOG_ID: u32 = 15_317_875;
const OTHER_STATIC_LOG_ID: u32 = 9_474_871;

fn static_ordinal_values() -> Vec<&'static str> {
    VOCABULARY_DENSE_INDICES
        .iter()
        .map(|dense| VOCABULARY_VALUES[*dense as usize])
        .collect()
}

fn static_ordinal_for_value(value: &str) -> u32 {
    VOCABULARY_DENSE_INDICES
        .iter()
        .position(|dense| VOCABULARY_VALUES[*dense as usize] == value)
        .expect("fixture value is registered") as u32
}

fn packed(entry_type: u8, vocabulary_id: u32) -> u32 {
    (vocabulary_id << 8) | u32::from(entry_type)
}

fn dictionary_span(trace: &str, span_id: u32, rows: &[(i64, u32, Option<String>)]) -> MockSpan {
    MockSpan {
        identity: Arc::new(SpanIdentity {
            thread_id: 0xABCD,
            span_id,
            trace_id: TraceId::new(trace).unwrap(),
            parent: None,
        }),
        timestamps: rows.iter().map(|row| row.0).collect(),
        packed_headers: rows.iter().map(|row| row.1).collect(),
        messages: rows.iter().map(|row| row.2.clone()).collect(),
        overflow: None,
        children: vec![],
    }
}

fn message_dictionary(
    batch: &arrow_array::RecordBatch,
) -> (&arrow_array::UInt32Array, &arrow_array::StringArray) {
    let message = batch.column(7).as_dictionary::<UInt32Type>();
    (message.keys(), message.values().as_string::<i32>())
}

#[test]
fn message_dictionary_reuses_static_prefix_and_appends_first_seen_dynamic_suffix() {
    let static_only = dictionary_span(
        "static-only",
        1,
        &[
            (1, packed(8, DENSE_ZERO_LOG_ID), None),
            (2, packed(8, OTHER_STATIC_LOG_ID), None),
            (3, 2, None),
        ],
    );
    let static_batch = convert_span_trees(&[static_only]).unwrap();
    let (static_keys, static_values) = message_dictionary(&static_batch);
    assert_eq!(
        static_values.len(),
        VOCABULARY_VALUES.len(),
        "static-only rows add no suffix"
    );
    assert!(
        std::ptr::eq(static_values, static_vocabulary_dictionary().as_ref()),
        "static-only conversion reuses the cached dictionary allocation",
    );
    assert_eq!(
        (0..static_values.len())
            .map(|index| static_values.value(index))
            .collect::<Vec<_>>(),
        static_ordinal_values(),
    );
    assert_eq!(
        static_keys.value(0),
        VOCABULARY_IDS.binary_search(&DENSE_ZERO_LOG_ID).unwrap() as u32,
    );
    assert_eq!(
        static_keys.value(1),
        VOCABULARY_IDS.binary_search(&OTHER_STATIC_LOG_ID).unwrap() as u32,
    );
    assert!(static_keys.is_null(2));

    let mut root = dictionary_span(
        "mixed-tree",
        10,
        &[
            (10, packed(8, OTHER_STATIC_LOG_ID), None),
            (11, 8, Some("dynamic-z".into())),
        ],
    );
    root.overflow = Some(Box::new(dictionary_span(
        "mixed-tree",
        10,
        &[
            (12, 8, Some("dynamic-a".into())),
            (13, 8, Some("dynamic-z".into())),
            (14, 2, None),
        ],
    )));
    let mut child = dictionary_span(
        "mixed-tree",
        11,
        &[
            (15, packed(8, DENSE_ZERO_LOG_ID), None),
            (16, 8, Some("dynamic-child".into())),
        ],
    );
    child.identity = Arc::new(SpanIdentity {
        thread_id: 0xABCD,
        span_id: 11,
        trace_id: root.identity.trace_id.clone(),
        parent: Some(root.identity.clone()),
    });
    root.children.push(child);

    let mixed_batch = convert_span_trees(&[root]).unwrap();
    let (keys, values) = message_dictionary(&mixed_batch);
    assert_eq!(
        (VOCABULARY_VALUES.len()..values.len())
            .map(|index| values.value(index))
            .collect::<Vec<_>>(),
        ["dynamic-z", "dynamic-a", "dynamic-child"],
        "dynamic suffix follows first encounter across overflow and child rows",
    );
    let suffix = VOCABULARY_VALUES.len() as u32;
    let other_ordinal = VOCABULARY_IDS.binary_search(&OTHER_STATIC_LOG_ID).unwrap() as u32;
    let zero_ordinal = VOCABULARY_IDS.binary_search(&DENSE_ZERO_LOG_ID).unwrap() as u32;
    assert_eq!(
        (0..keys.len())
            .map(|row| (!keys.is_null(row)).then(|| keys.value(row)))
            .collect::<Vec<_>>(),
        [
            Some(other_ordinal),
            Some(suffix),
            Some(suffix + 1),
            Some(suffix),
            None,
            Some(zero_ordinal),
            Some(suffix + 2),
        ],
    );
    assert_eq!(
        (0..keys.len())
            .map(|row| (!keys.is_null(row)).then(|| values.value(keys.value(row) as usize)))
            .collect::<Vec<_>>(),
        [
            Some("literal braces: {ok} for {region}"),
            Some("dynamic-z"),
            Some("dynamic-a"),
            Some("dynamic-z"),
            None,
            Some("No items to validate"),
            Some("dynamic-child"),
        ],
    );
}

fn dictionary_row_strategy() -> impl Strategy<Value = (u32, Option<String>, Option<String>)> {
    prop_oneof![
        Just((2, None, None)),
        Just((
            packed(8, DENSE_ZERO_LOG_ID),
            None,
            Some("No items to validate".into())
        )),
        Just((
            packed(8, OTHER_STATIC_LOG_ID),
            None,
            Some("literal braces: {ok} for {region}".into())
        )),
        "dynamic-[a-z]{0,8}".prop_map(|message| (8, Some(message.clone()), Some(message))),
    ]
}

proptest! {
    #[test]
    fn randomized_message_rows_preserve_exact_values_and_canonical_indices(
        rows in prop::collection::vec(dictionary_row_strategy(), 0..100),
        root_rows in 0usize..100,
        overflow_rows in 0usize..100,
    ) {
        let root_end = root_rows.min(rows.len());
        let overflow_end = (root_end + overflow_rows).min(rows.len());
        let to_source_rows = |slice: &[(u32, Option<String>, Option<String>)], base: i64| {
            slice.iter().enumerate().map(|(index, (header, raw, _))| {
                (base + index as i64, *header, raw.clone())
            }).collect::<Vec<_>>()
        };
        let mut root = dictionary_span("property-tree", 1, &to_source_rows(&rows[..root_end], 0));
        if overflow_end > root_end {
            root.overflow = Some(Box::new(dictionary_span(
                "property-tree",
                1,
                &to_source_rows(&rows[root_end..overflow_end], root_end as i64),
            )));
        }
        if overflow_end < rows.len() {
            let mut child = dictionary_span(
                "property-tree",
                2,
                &to_source_rows(&rows[overflow_end..], overflow_end as i64),
            );
            child.identity = Arc::new(SpanIdentity {
                thread_id: 0xABCD,
                span_id: 2,
                trace_id: root.identity.trace_id.clone(),
                parent: Some(root.identity.clone()),
            });
            root.children.push(child);
        }

        let batch = convert_span_trees(&[root]).unwrap();
        let (keys, values) = message_dictionary(&batch);
        prop_assert_eq!(batch.num_rows(), rows.len());
        let mut seen = HashSet::new();
        let expected_suffix = rows.iter().filter_map(|(_, raw, _)| raw.as_deref()).filter(|value| {
            static_vocabulary_dictionary().iter().flatten().all(|static_value| static_value != *value)
                && seen.insert(*value)
        }).collect::<Vec<_>>();
        prop_assert_eq!(values.len(), VOCABULARY_VALUES.len() + expected_suffix.len());
        for (offset, expected) in expected_suffix.iter().enumerate() {
            prop_assert_eq!(values.value(VOCABULARY_VALUES.len() + offset), *expected);
        }
        for (row, (_, _, expected)) in rows.iter().enumerate() {
            match expected {
                None => prop_assert!(keys.is_null(row)),
                Some(expected) => {
                    prop_assert!(!keys.is_null(row));
                    prop_assert_eq!(values.value(keys.value(row) as usize), expected);
                    if expected == "No items to validate" {
                        prop_assert_eq!(keys.value(row), static_ordinal_for_value(expected));
                    }
                }
            }
        }
    }
}

proptest! {
    /// Identical input event sequences serialize to bit-identical IPC bytes
    /// (AxE H-SIM-4 style trace-byte identity).
    #[test]
    fn identical_trees_serialize_identically(n in 0usize..50) {
        let a = convert_span_trees(&[build_tree(n, "trace-a")]).unwrap();
        let b = convert_span_trees(&[build_tree(n, "trace-a")]).unwrap();
        prop_assert_eq!(ipc_bytes(&a), ipc_bytes(&b));
    }

    /// Conversion is lossless for row count and per-row (timestamp, entry_type),
    /// and pre-order/overflow contiguity holds: rows of one logical span (buffer +
    /// overflow chain) are adjacent in the batch.
    #[test]
    fn conversion_preserves_rows(n in 0usize..80) {
        use arrow_array::cast::AsArray;
        use lmao_arrow::{SpanSource, walk_pre_order};

        let tree = build_tree(n, "trace-rows");
        let mut expected: Vec<(i64, u8)> = Vec::new();
        walk_pre_order(std::slice::from_ref(&tree), &mut |b: &MockSpan| {
            for row in 0..b.row_count() {
                expected.push((b.timestamp(row), b.packed_header(row) as u8));
            }
        });

        let batch = convert_span_trees(std::slice::from_ref(&tree)).unwrap();
        prop_assert_eq!(batch.num_rows(), expected.len());
        let ts = batch.column(0).as_primitive::<arrow_array::types::Int64Type>();
        let et = batch.column(6).as_dictionary::<arrow_array::types::UInt8Type>();
        for (row, (want_ts, want_et)) in expected.iter().enumerate() {
            prop_assert_eq!(ts.value(row), *want_ts);
            // Dictionary key is discriminant − 1.
            prop_assert_eq!(et.keys().value(row), want_et - 1);
        }
    }

    /// Envelope identity is a pure function of content (same batch → same chunk_id;
    /// different file_ref → different chunk_id).
    #[test]
    fn envelope_is_deterministic(n in 0usize..30) {
        let batch = convert_span_trees(&[build_tree(n, "trace-env")]).unwrap();
        let e1 = build_trace_chunk_envelope("s3://bucket/chunk-1", &batch);
        let e2 = build_trace_chunk_envelope("s3://bucket/chunk-1", &batch);
        let e3 = build_trace_chunk_envelope("s3://bucket/chunk-2", &batch);
        prop_assert_eq!(&e1, &e2);
        prop_assert_ne!(e1.chunk_id, e3.chunk_id);
    }
}
