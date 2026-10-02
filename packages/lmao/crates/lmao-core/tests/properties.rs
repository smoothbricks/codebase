//! Property tests for the core row-layout and tuning invariants.
//! TDD driver set: red tests here define behavior before implementation.

use lmao_core::clock::{Clock, TraceAnchor};
use lmao_core::tuning::{CapacityRatchet, MAX_CAPACITY, MIN_CAPACITY};
use lmao_core::{EntryType, SpanBuffer, SpanIdentity, TraceId};
use proptest::prelude::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

struct TickClock(AtomicU64);
impl Clock for TickClock {
    fn wall_nanos(&self) -> i64 {
        1_700_000_000_000_000_000
    }
    fn monotonic_nanos(&self) -> u64 {
        self.0.fetch_add(1, Ordering::Relaxed)
    }
}

fn test_span(capacity: usize) -> (SpanBuffer, TraceAnchor, TickClock) {
    let clock = TickClock(AtomicU64::new(0));
    let anchor = TraceAnchor::capture(&clock);
    let identity = Arc::new(SpanIdentity {
        thread_id: 7,
        // Nonzero: zero is the reserved "no parent" sentinel (see
        // `identity::successor`), so no real span may carry it.
        span_id: 1,
        trace_id: TraceId::new("prop-test-trace").unwrap(),
        parent: None,
    });
    let buf = SpanBuffer::start_dynamic(identity, capacity, "span".into(), &anchor, &clock);
    (buf, anchor, clock)
}

proptest! {
    /// Row 1 is ALWAYS a valid completion entry, at every point in a span's life
    /// (01b: pre-initialized span-exception, overwritten by ok/err — never absent).
    #[test]
    fn row1_is_always_a_completion_entry(
        capacity_exp in 3usize..=10,
        complete in prop::option::of(prop::bool::ANY),
    ) {
        let (mut buf, anchor, clock) = test_span(1 << capacity_exp);
        prop_assert_eq!(buf.entry_type_at(0), Some(EntryType::SpanStart));
        prop_assert!(buf.entry_type_at(1).unwrap().is_completion());

        match complete {
            Some(true) => buf.end_ok(&anchor, &clock),
            Some(false) => buf.end_err(&anchor, &clock),
            None => {} // abandoned span: row 1 must still be valid (span-exception)
        }
        prop_assert!(buf.entry_type_at(1).unwrap().is_completion());
        // duration = ts[1] - ts[0] is always computable and non-negative under a
        // monotonic clock.
        prop_assert!(buf.duration_nanos() >= 0);
    }

    /// Ratchet capacity is always a power of two within [MIN, MAX], for any
    /// workload sequence.
    #[test]
    fn ratchet_stays_bounded_power_of_two(
        initial_exp in 3usize..=10,
        writes in prop::collection::vec(0u64..5_000, 0..500),
    ) {
        let mut r = CapacityRatchet::new(1 << initial_exp);
        for w in writes {
            r.record_span(w);
            let c = r.capacity();
            prop_assert!(c.is_power_of_two());
            prop_assert!((MIN_CAPACITY..=MAX_CAPACITY).contains(&c));
        }
    }
}

proptest! {
    /// Appends land at monotonically increasing rows starting at 2, and overflow
    /// chaining preserves every entry in order. Per 01b4/01b5, overflow buffers
    /// carry NO span-start/completion rows (they share the root's identity), so
    /// only the head buffer subtracts the 2 reserved rows when counting.
    #[test]
    fn appends_are_ordered_and_lossless(
        capacity_exp in 3usize..=6,
        n_appends in 0usize..200,
    ) {
        let (mut buf, anchor, clock) = test_span(1 << capacity_exp);
        for i in 0..n_appends {
            let row = buf.append_dynamic(EntryType::Info, None, 0, &anchor, &clock);
            if i + 2 < buf.capacity() {
                prop_assert_eq!(row, i + 2);
            }
        }
        // Count entries across the overflow chain: none lost. Head buffer reserves
        // rows 0-1; overflow buffers are all-data rows.
        let mut total = buf.write_index().saturating_sub(2);
        let mut cur = buf.overflow();
        while let Some(b) = cur {
            total += b.write_index();
            cur = b.overflow();
        }
        prop_assert_eq!(total, n_appends);
    }
}

// --- Properties added with the full lmao-core implementation ---

use lmao_core::{F64Column, StrColumn, TextInput, TraceContext};

proptest! {
    /// Lazy column memory accounting: bytes are 0 until first touch, then exactly
    /// the fixed-capacity footprint, and NEVER change again (no realloc, `01b1`).
    #[test]
    fn lazy_column_memory_accounting(
        capacity_exp in 3usize..=10,
        touches in prop::collection::vec(any::<u16>(), 0..64),
    ) {
        let cap = 1 << capacity_exp;
        let mut num = F64Column::new();
        let mut s = StrColumn::new();
        prop_assert_eq!(num.allocated_bytes(), 0);
        prop_assert_eq!(s.allocated_bytes(), 0);
        let mut expected_num = 0;
        let mut expected_str = 0;
        for t in &touches {
            let row = *t as usize & (cap - 1);
            num.set(row, cap, 1.0);
            s.set(row, cap, "static-template");
            if expected_num == 0 {
                expected_num = num.allocated_bytes();
                expected_str = s.allocated_bytes();
                prop_assert!(expected_num > 0);
                prop_assert!(expected_str > 0);
            }
            prop_assert_eq!(num.allocated_bytes(), expected_num);
            prop_assert_eq!(s.allocated_bytes(), expected_str);
        }
        if touches.is_empty() {
            prop_assert_eq!(num.allocated_bytes(), 0);
            prop_assert_eq!(s.allocated_bytes(), 0);
        }
    }

    /// Ratchet convergence: under a STEADY workload the capacity converges to a
    /// stable fixed point and stops moving (no oscillation, `01b2`).
    #[test]
    fn ratchet_converges_on_steady_workload(
        initial_exp in 3usize..=10,
        writes_per_span in 0u64..3_000,
    ) {
        let mut r = CapacityRatchet::new(1 << initial_exp);
        // Long steady phase: capacity must reach a fixed point...
        for _ in 0..2_000 {
            r.record_span(writes_per_span);
        }
        let settled = r.capacity();
        // ...and stay there for the rest of the workload.
        for _ in 0..2_000 {
            r.record_span(writes_per_span);
            prop_assert_eq!(r.capacity(), settled, "ratchet oscillated after settling");
        }
    }
}

/// Serialize every observable row of a buffer tree to bytes (timestamps LE,
/// entry types, line numbers, messages, names) — the determinism witness.
fn buffer_bytes(buf: &lmao_core::SpanBuffer) -> Vec<u8> {
    let mut out = Vec::new();
    let mut stack = vec![buf];
    while let Some(b) = stack.pop() {
        out.extend_from_slice(&b.identity.thread_id.to_le_bytes());
        out.extend_from_slice(&b.identity.span_id.to_le_bytes());
        out.extend_from_slice(b.identity.trace_id.as_str().as_bytes());
        for row in 0..b.capacity() {
            out.extend_from_slice(&b.timestamp_at(row).unwrap_or(0).to_le_bytes());
            out.extend_from_slice(&b.packed_header_at(row).unwrap_or(0).to_le_bytes());
            out.extend_from_slice(&b.line_at(row).to_le_bytes());
            if let Some(m) = b.dynamic_message_at(row) {
                out.extend_from_slice(m.as_bytes());
            }
            out.push(0xFE);
        }
        if let Some(o) = b.overflow() {
            stack.push(o);
        }
        for c in b.children() {
            stack.push(c);
        }
    }
    out
}

/// One deterministic run of the op sequence (executed on a fresh thread so the
/// thread-local span_id counter starts at 0).
fn run(capacity_exp: usize, ops: &[(u8, u16)]) -> lmao_core::SpanBuffer {
    let clock = TickClock(AtomicU64::new(0));
    let trace = TraceContext::new(
        TraceId::new("determinism").unwrap(),
        42,
        std::sync::Arc::new(clock),
    );
    let (_, buf) = trace.__span(
        TextInput::Static("root"),
        None,
        1 << capacity_exp,
        lmao_core::SourceMetadata::UNATTRIBUTED,
        |ctx| {
            for (op, val) in ops {
                match op {
                    0 => {
                        ctx.__log(EntryType::Info, "op {v}", *val as u32);
                    }
                    1 => {
                        ctx.__log(EntryType::Warn, "warn {v}", *val as u32);
                    }
                    2 => {
                        ctx.append(EntryType::BufferWrites);
                    }
                    _ => {
                        ctx.__child(
                            TextInput::Static("kid"),
                            8,
                            lmao_core::SourceMetadata::UNATTRIBUTED,
                            |c| {
                                c.__log(EntryType::Debug, "child {v}", *val as u32);
                                Ok::<_, ()>(())
                            },
                        )?;
                    }
                }
            }
            Ok::<_, ()>(())
        },
    );
    buf
}

proptest! {
    /// Deterministic scheduler shape: identical input sequences under a fixed Clock produce
    /// BIT-IDENTICAL buffer contents (same bytes, both runs).
    #[test]
    fn identical_inputs_produce_bit_identical_buffers(
        capacity_exp in 3usize..=6,
        ops in prop::collection::vec((0u8..4, any::<u16>()), 0..120),
    ) {
        // span_id comes from a thread-local counter, so each run executes on a
        // FRESH thread where the counter starts at 0 — identical between runs.
        let ops2 = ops.clone();
        let a = std::thread::spawn(move || buffer_bytes(&run(capacity_exp, &ops)))
            .join()
            .unwrap();
        let b = std::thread::spawn(move || buffer_bytes(&run(capacity_exp, &ops2)))
            .join()
            .unwrap();
        prop_assert_eq!(a, b, "same inputs + fixed clock must yield bit-identical buffers");
    }
}

// ── Thread row store: a span id a writer holds ───────────────────────────────

use lmao_core::ThreadSpanBuffer;

/// One call a host makes on a thread row store; `Log` and `End` pick one of
/// the spans still open.
#[derive(Clone, Debug)]
enum StoreStep {
    Open,
    Log(usize),
    End(usize),
    /// A streaming flush's release: the open spans move to the front.
    Flush,
    Reset,
}

fn store_step() -> impl Strategy<Value = StoreStep> {
    prop_oneof![
        4 => Just(StoreStep::Open),
        3 => any::<usize>().prop_map(StoreStep::Log),
        3 => any::<usize>().prop_map(StoreStep::End),
        1 => Just(StoreStep::Flush),
        1 => Just(StoreStep::Reset),
    ]
}

proptest! {
    /// A foreign writer keeps the span id a receipt named and asks the store
    /// where that span's rows are now (`start_row`). Whatever the host does in
    /// between — open, log, end, a flush that moves the open spans' rows
    /// (`retain_open`), a reset — a held id resolves to that span's own start
    /// row while the store holds it and to nothing once released: never to
    /// another span's rows.
    #[test]
    fn a_held_span_id_resolves_to_its_own_rows_or_to_none(
        steps in prop::collection::vec(store_step(), 1..64),
    ) {
        let mut store = ThreadSpanBuffer::new(7, 8, &[]);
        let trace = TraceId::new("prop-store-trace").unwrap();
        let mut issued = std::collections::HashSet::new();
        let mut open: Vec<u32> = Vec::new();
        let mut ended: Vec<u32> = Vec::new();
        let mut released: Vec<u32> = Vec::new();
        for (clock, step) in (1_i64..).zip(steps) {
            match step {
                StoreStep::Open => {
                    let id = store.open_span(trace.clone(), 0, 0, "s".into(), clock, 1).unwrap();
                    prop_assert!(issued.insert(id), "span id {} was issued again", id);
                    open.push(id);
                }
                StoreStep::Log(pick) if !open.is_empty() => {
                    let id = open[pick % open.len()];
                    store.append_log(id, EntryType::Info, Some("row".into()), 2, clock).unwrap();
                }
                StoreStep::End(pick) if !open.is_empty() => {
                    let id = open.swap_remove(pick % open.len());
                    store.end_ok(id, clock).unwrap();
                    ended.push(id);
                }
                StoreStep::Log(_) | StoreStep::End(_) => {}
                StoreStep::Flush => {
                    store.retain_open();
                    released.append(&mut ended);
                }
                StoreStep::Reset => {
                    store.reset();
                    released.append(&mut open);
                    released.append(&mut ended);
                }
            }
            for &id in open.iter().chain(&ended) {
                let at = store.start_row(id).and_then(|row| store.span_id_at(row));
                prop_assert_eq!(at, Some(id), "held span {} does not resolve to its own row", id);
            }
            for &id in &released {
                prop_assert_eq!(store.start_row(id), None, "released span {} still resolves", id);
            }
        }
    }
}
