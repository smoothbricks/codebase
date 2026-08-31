//! Integration tests: real `lmao-core::SpanBuffer` input, archive primitives, and
//! cross-implementation validation of the emitted Arrow IPC via pyarrow (adapted
//! from an independent pyarrow verifier).

use std::sync::Arc;

use arrow_array::Array;
use arrow_array::cast::AsArray;
use arrow_array::types::{TimestampNanosecondType, UInt32Type};
use arrow_schema::{DataType, TimeUnit};
use lmao_arrow::{
    PartitionCardinality, StableVocabularyCatalog, convert_span_trees,
    inspect_partition_cardinality, split_chunk_by_partition, write_ipc_stream,
};
use lmao_core::{Clock, EntryType, SourceMetadata, SpanBuffer, SpanIdentity, TraceAnchor, TraceId};

struct TickClock(std::sync::atomic::AtomicU64);
impl Clock for TickClock {
    fn wall_nanos(&self) -> i64 {
        1_700_000_000_000_000_000
    }
    fn monotonic_nanos(&self) -> u64 {
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
}

fn real_root(trace: &str, span_id: u32, logs: usize) -> SpanBuffer {
    let clock = TickClock(std::sync::atomic::AtomicU64::new(0));
    let anchor = TraceAnchor::capture(&clock);
    let identity = Arc::new(SpanIdentity {
        thread_id: 7,
        span_id,
        trace_id: TraceId::new(trace).unwrap(),
        parent: None,
    });
    let mut buf = SpanBuffer::start_dynamic(identity.clone(), 8, "root-op".into(), &anchor, &clock);
    buf.set_source(SourceMetadata {
        package_name: "lmao-arrow-fixture",
        package_file: "src/fixture.rs",
        git_sha: Some("deadbeef"),
        line: 41,
    });
    for i in 0..logs {
        buf.append_dynamic(
            EntryType::Info,
            Some("log {i}".into()),
            100 + i as u32,
            &anchor,
            &clock,
        );
    }
    buf.end_ok(&anchor, &clock);

    let child_identity = Arc::new(SpanIdentity {
        thread_id: 7,
        span_id: span_id + 100,
        trace_id: identity.trace_id.clone(),
        parent: Some(identity),
    });
    let mut child =
        SpanBuffer::start_dynamic(child_identity, 8, "child-op".into(), &anchor, &clock);
    child.end_ok(&anchor, &clock);
    buf.add_child(child);
    buf
}

#[test]
fn converts_core_span_buffers_with_overflow() {
    // 20 logs in a capacity-8 buffer forces overflow chaining.
    let roots = [real_root("trace-x", 1, 20), real_root("trace-y", 2, 3)];
    let empty_catalog = StableVocabularyCatalog::EMPTY;
    let batch = convert_span_trees(&roots, &empty_catalog).unwrap();
    // Root 1: 2 fixed rows + 20 logs + child (2 rows); root 2: 2 + 3 + 2.
    assert_eq!(batch.num_rows(), 24 + 7);
    // Entry-type dictionary keys are the ABI discriminants; slot 0 is intentionally empty.
    let et = batch
        .column(6)
        .as_dictionary::<arrow_array::types::UInt8Type>();
    assert_eq!(et.keys().value(1), EntryType::SpanOk.as_u8());
    // TickClock timestamps: anchor consumes tick 0, row 0 = wall + 1.
    let ts = batch.column(0).as_primitive::<TimestampNanosecondType>();
    assert_eq!(ts.value(0), 1_700_000_000_000_000_001);
    assert!(
        ts.value(2) > ts.value(0),
        "log rows stamped after span-start"
    );
    // Row 0 message is the span name; log rows carry templates + line numbers.
    let msg = batch
        .column(10)
        .as_dictionary::<arrow_array::types::UInt32Type>();
    let msg_values = msg.values().as_string::<i32>();
    assert_eq!(msg_values.value(msg.keys().value(0) as usize), "root-op");
    assert_eq!(msg_values.value(msg.keys().value(2) as usize), "log {i}");
    assert!(msg.keys().is_null(1), "completion row has no template");
    let lines = batch.column(11).as_primitive::<UInt32Type>();
    assert_eq!(lines.value(0), 41, "callsite line on row 0");
    assert_eq!(lines.value(2), 100, "dynamic append line on first log row");
    // Children were walked: the child span's name appears after the root's rows.
    let child_name_key = (0..batch.num_rows()).find(|r| {
        !msg.keys().is_null(*r) && msg_values.value(msg.keys().value(*r) as usize) == "child-op"
    });
    assert!(child_name_key.is_some(), "child span rows present in batch");
    assert_eq!(
        inspect_partition_cardinality(&batch),
        PartitionCardinality::Mixed
    );

    let parts = split_chunk_by_partition(&batch);
    assert_eq!(parts.len(), 2);
    let total: usize = parts.iter().map(|(_, rows)| rows.len()).sum();
    assert_eq!(total, batch.num_rows());
    // Contiguity: each partition's row indices are one dense run (pre-order keeps a
    // span's rows adjacent, and each root here is one trace).
    for (_, rows) in &parts {
        for pair in rows.windows(2) {
            assert_eq!(pair[1], pair[0] + 1);
        }
    }
}

#[test]
fn single_trace_is_single_partition() {
    let empty_catalog = StableVocabularyCatalog::EMPTY;
    let batch = convert_span_trees(&[real_root("only-trace", 1, 4)], &empty_catalog).unwrap();
    assert_eq!(
        inspect_partition_cardinality(&batch),
        PartitionCardinality::Single
    );
}

/// Cross-implementation check: pyarrow must be able to read our IPC bytes and agree
/// on the row count and schema. The development shell provisions pyarrow specifically
/// for this test, so a missing interpreter is a broken test environment, not a skip.
#[test]
fn pyarrow_reads_our_ipc() {
    let probe = std::process::Command::new("python3")
        .args(["-c", "import pyarrow"])
        .output()
        .expect("python3 must be available for the mandatory pyarrow oracle");
    assert!(
        probe.status.success(),
        "pyarrow must be installed for the mandatory IPC oracle: {}",
        String::from_utf8_lossy(&probe.stderr)
    );

    let empty_catalog = StableVocabularyCatalog::EMPTY;
    let batch = convert_span_trees(&[real_root("pyarrow-trace", 1, 10)], &empty_catalog).unwrap();
    let dir = std::env::temp_dir().join("lmao-pyarrow-verify");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("chunk.arrow");
    let expected_schema = [
        ("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None)),
        (
            "trace_id",
            DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
        ),
        ("thread_id", DataType::UInt64),
        ("span_id", DataType::UInt32),
        ("parent_thread_id", DataType::UInt64),
        ("parent_span_id", DataType::UInt32),
        (
            "entry_type",
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
        ),
        (
            "package_name",
            DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
        ),
        (
            "package_file",
            DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
        ),
        (
            "git_sha",
            DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
        ),
        (
            "message",
            DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
        ),
        ("line", DataType::UInt32),
    ];
    let schema = batch.schema();
    for (index, (name, data_type)) in expected_schema.iter().enumerate() {
        assert_eq!(schema.field(index).name(), name);
        assert_eq!(schema.field(index).data_type(), data_type);
    }

    let mut file = std::fs::File::create(&path).unwrap();
    write_ipc_stream(&mut file, &batch).unwrap();

    let script = format!(
        "import pyarrow.ipc as ipc\nt = ipc.open_stream('{}').read_all()\nprint(t.num_rows)",
        path.display()
    );
    let out = std::process::Command::new("python3")
        .args(["-c", &script])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "pyarrow failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8(out.stdout).unwrap();
    assert_eq!(stdout.trim(), batch.num_rows().to_string());
}
