//! Determinism properties for the Arrow conversion layer.

use std::sync::Arc;

use lmao_arrow::{ColumnDictionary, MockSpan, build_trace_chunk_envelope, convert_span_trees};
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
            5u8 + (i % 4) as u8, // info/debug/warn/error
            Some(templates[i % templates.len()].to_string()),
        )
    };
    let mut child = MockSpan {
        identity: child_id,
        timestamps: vec![10],
        entry_types: vec![1],
        messages: vec![Some("child-span".into())],
        overflow: None,
        children: vec![],
    };
    let mut root = MockSpan {
        identity: root_id,
        timestamps: vec![0, 1],
        entry_types: vec![1, 2],
        messages: vec![Some("root-span".into()), None],
        overflow: None,
        children: vec![],
    };
    // Split events across the child's overflow chain to exercise chain contiguity.
    let mut overflow = MockSpan {
        identity: child.identity.clone(),
        timestamps: vec![],
        entry_types: vec![],
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
        target.entry_types.push(et);
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
                expected.push((b.timestamp(row), b.entry_type(row)));
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
