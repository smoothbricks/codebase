//! Integration tests: real `lmao-core::SpanBuffer` input, archive primitives, and
//! cross-implementation validation of the emitted Arrow IPC via pyarrow (adapted
//! from `AxE/packages/axe-runtime/scripts/pyarrow-verify.py`).

use std::sync::Arc;

use arrow_array::Array;
use arrow_array::cast::AsArray;
use arrow_array::types::{Int64Type, UInt32Type};
use lmao_arrow::{
    PartitionCardinality, convert_span_trees, inspect_partition_cardinality,
    split_chunk_by_partition,
};
use lmao_core::{Clock, EntryType, SpanBuffer, SpanIdentity, TraceAnchor, TraceId};

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
    let mut buf = SpanBuffer::start(identity.clone(), 8, &anchor, &clock);
    buf.set_name("root-op");
    buf.set_callsite("src/fixture.rs", 41);
    for i in 0..logs {
        buf.append_msg(EntryType::Info, "log {i}", 100 + i as u32, &anchor, &clock);
    }
    buf.end_ok(&anchor, &clock);

    let child_identity = Arc::new(SpanIdentity {
        thread_id: 7,
        span_id: span_id + 100,
        trace_id: identity.trace_id.clone(),
        parent: Some(identity),
    });
    let mut child = SpanBuffer::start(child_identity, 8, &anchor, &clock);
    child.set_name("child-op");
    child.end_ok(&anchor, &clock);
    buf.add_child(child);
    buf
}

#[test]
fn converts_core_span_buffers_with_overflow() {
    // 20 logs in a capacity-8 buffer forces overflow chaining.
    let roots = [real_root("trace-x", 1, 20), real_root("trace-y", 2, 3)];
    let batch = convert_span_trees(&roots).unwrap();
    // Root 1: 2 fixed rows + 20 logs + child (2 rows); root 2: 2 + 3 + 2.
    assert_eq!(batch.num_rows(), 24 + 7);
    // Row 1 of each span is a completion entry (span-ok, discriminant 2 → key 1).
    let et = batch
        .column(6)
        .as_dictionary::<arrow_array::types::UInt8Type>();
    assert_eq!(et.keys().value(1), 1);
    // TickClock timestamps: anchor consumes tick 0, row 0 = wall + 1.
    let ts = batch.column(0).as_primitive::<Int64Type>();
    assert_eq!(ts.value(0), 1_700_000_000_000_000_001);
    assert!(ts.value(2) > ts.value(0), "log rows stamped after span-start");
    // Row 0 message is the span name; log rows carry templates + line numbers.
    let msg = batch
        .column(7)
        .as_dictionary::<arrow_array::types::UInt32Type>();
    let msg_values = msg.values().as_string::<i32>();
    assert_eq!(msg_values.value(msg.keys().value(0) as usize), "root-op");
    assert_eq!(msg_values.value(msg.keys().value(2) as usize), "log {i}");
    assert!(msg.keys().is_null(1), "completion row has no template");
    let lines = batch.column(8).as_primitive::<UInt32Type>();
    assert_eq!(lines.value(0), 41, "callsite line on row 0");
    assert_eq!(lines.value(2), 100, "append_msg line on first log row");
    // Children were walked: the child span's name appears after the root's rows.
    let child_name_key = (0..batch.num_rows())
        .find(|r| !msg.keys().is_null(*r) && msg_values.value(msg.keys().value(*r) as usize) == "child-op");
    assert!(child_name_key.is_some(), "child span rows present in batch");
    assert_eq!(inspect_partition_cardinality(&batch), PartitionCardinality::Mixed);

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
    let batch = convert_span_trees(&[real_root("only-trace", 1, 4)]).unwrap();
    assert_eq!(
        inspect_partition_cardinality(&batch),
        PartitionCardinality::Single
    );
}

/// Cross-implementation check: pyarrow must be able to read our IPC bytes and agree
/// on row count + schema field names. Skips (with a note) when python3/pyarrow is
/// unavailable — the arrow-rs roundtrip in properties.rs still covers self-consistency.
#[test]
fn pyarrow_reads_our_ipc() {
    let probe = std::process::Command::new("python3")
        .args(["-c", "import pyarrow"])
        .output();
    if !probe.map(|o| o.status.success()).unwrap_or(false) {
        eprintln!("SKIP: python3/pyarrow not available; relying on arrow-rs roundtrip");
        return;
    }

    let batch = convert_span_trees(&[real_root("pyarrow-trace", 1, 10)]).unwrap();
    let dir = std::env::temp_dir().join("lmao-rs-pyarrow-verify");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("chunk.arrow");
    let file = std::fs::File::create(&path).unwrap();
    let mut w = arrow_ipc::writer::StreamWriter::try_new(file, &batch.schema()).unwrap();
    w.write(&batch).unwrap();
    w.finish().unwrap();

    let script = format!(
        "import pyarrow.ipc as ipc\nt = ipc.open_stream('{}').read_all()\nprint(t.num_rows)\nprint(','.join(t.schema.names))",
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
    let mut lines = stdout.lines();
    assert_eq!(lines.next().unwrap(), batch.num_rows().to_string());
    assert_eq!(
        lines.next().unwrap(),
        "timestamp,trace_id,thread_id,span_id,parent_thread_id,parent_span_id,entry_type,message,line_number"
    );
}
