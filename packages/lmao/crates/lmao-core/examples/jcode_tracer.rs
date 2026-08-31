//! How a host like jcode adopts lmao: schema definition, spans wrapping ops
//! inside async tokio tasks, and an interval flush loop.
//!
//! Run: `cargo run -p lmao-core --example jcode_tracer`
//!
//! ## Identity under tokio (the decision, also documented in `context.rs`)
//!
//! `span_id` counters are thread-local; identity is captured ONCE at span
//! creation, so `(thread_id, span_id)` is a unique label even when the tokio
//! scheduler migrates the task to another worker thread mid-span. No task-local
//! machinery is needed — uniqueness never depended on staying put.
//!
//! ## Flush model shown here
//!
//! Workers run ops inside spans and push finished root buffers to an mpsc
//! channel (in jcode: the session actor owns this, one tracer per session).
//! A flush task drains on an interval — in production it hands the batch to
//! `lmao-arrow::convert_span_trees` for ONE RecordBatch per flush (`01k`);
//! here it prints a summary so the example has no cross-crate dependency on
//! the in-progress arrow crate.

use lmao_core::{
    Clock, EntryType, SpanBuffer, SystemClock, TextInput, TraceContext, TraceId, Transient,
};
use lmao_macros::{define_log_schema, log, span_with_retry};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

define_log_schema!(pub ToolCallSchema {
    duration_ms: number,
    tokens: uint64,
    cache_hit: boolean,
    tool_name: category,
    detail: text,
    outcome: enum["ok", "err", "timeout"],
});

#[tokio::main(worker_threads = 4)]
async fn main() {
    let clock: Arc<dyn Clock> = Arc::new(SystemClock::new());
    let (tx, mut rx) = mpsc::channel::<SpanBuffer>(256);

    // In jcode, thread_id comes from the process-level entropy seam once.
    let thread_id = 0x0001_C0DE_u64;

    // ---- Worker tasks: each turn/tool-call runs inside a span ----
    let mut workers = Vec::new();
    for worker in 0..4u64 {
        let clock = clock.clone();
        let tx = tx.clone();
        workers.push(tokio::spawn(async move {
            let trace = TraceContext::new(
                TraceId::new(format!("session-{worker}")).unwrap(),
                thread_id,
                clock,
            );
            for call in 0..25 {
                let (out, buf) = span_with_retry!(
                    trace,
                    "tool-call",
                    None,
                    64,
                    |_delay_ms| { /* tokio::time::sleep in real async retry */ },
                    |ctx| {
                        log!(ctx, EntryType::Info, "invoking {tool} with {args}");
                        if call % 10 == 9 {
                            Err(Transient::fixed("transient provider error", 2, 5))
                        } else {
                            log!(ctx, EntryType::Debug, "tool returned {bytes} bytes");
                            Ok(call)
                        }
                    },
                );
                let _ = out; // jcode: surface Err to the turn loop
                for span in buf {
                    let mut traced = ToolCallSchema::from_span(span);
                    let tool_name = format!("tool-{}", call % 5);
                    traced
                        .tag_tool_name(TextInput::Dynamic(&tool_name))
                        .tag_cache_hit(call % 3 == 0)
                        .tag_duration_ms(1.5 * (call as f64 + 1.0))
                        .tag_tokens(128 * call);
                    traced.tag_outcome(0).expect("known outcome variant");
                    tx.send(traced.into_span())
                        .await
                        .expect("flush channel open");
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        }));
    }
    drop(tx);

    // ---- Interval flush loop (jcode: background persistence actor) ----
    let flusher = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(50));
        let mut pending: Vec<SpanBuffer> = Vec::new();
        let (mut flushes, mut spans, mut entries) = (0u32, 0usize, 0usize);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if !pending.is_empty() {
                        // Production: lmao_arrow::convert_span_trees(
                        //     &pending,
                        //     &lmao_arrow::StableVocabularyCatalog::EMPTY,
                        // ) -> RecordBatch
                        flushes += 1;
                        spans += pending.len();
                        entries += pending.iter().map(|b| b.write_index()).sum::<usize>();
                        pending.clear();
                    }
                }
                buf = rx.recv() => match buf {
                    Some(b) => pending.push(b),
                    None => {
                        spans += pending.len();
                        entries += pending.iter().map(|b| b.write_index()).sum::<usize>();
                        if !pending.is_empty() { flushes += 1; }
                        break (flushes, spans, entries);
                    }
                }
            }
        }
    });

    for w in workers {
        w.await.unwrap();
    }
    let (flushes, spans, entries) = flusher.await.unwrap();
    println!("flushed {spans} spans / {entries} entries in {flushes} interval batches");
    assert!(spans >= 100, "every tool call produced at least one span");
    println!(
        "enum dictionary (compile-time, zero flush work): {:?}",
        ToolCallSchema::OUTCOME_VALUES
    );
}
