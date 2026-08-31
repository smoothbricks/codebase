//! Warmed-path allocation census for ThreadSpanBuffer.
//!
//! open / end / log / tag must allocate zero heap after warmup. This binary
//! has its own counting global allocator so it cannot be silenced by a sibling
//! test file.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use lmao_core::{
    ColumnValue, EntryType, FieldMeta, FieldStrategy, TextInput, ThreadSpanBuffer, TraceId,
};

struct CountingAlloc;

std::thread_local! {
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = ALLOCATIONS.try_with(|c| c.set(c.get() + 1));
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

fn allocations() -> u64 {
    ALLOCATIONS.with(|c| c.get())
}

static FIELDS: &[FieldMeta] = &[
    FieldMeta::new("answer", FieldStrategy::Number),
    FieldMeta::new("label", FieldStrategy::Category),
];

fn trace() -> TraceId {
    TraceId::new("alloc-thread").unwrap()
}

#[test]
fn open_end_log_tag_are_alloc_free_after_warmup() {
    let mut buffer = ThreadSpanBuffer::new(7, 64, FIELDS);
    let warmup_name = TextInput::Static("warmup");
    let warmup_log = TextInput::Static("warmup-log");
    let hot_name = TextInput::Static("hot");
    let hot_log = TextInput::Static("hot-log");
    let id = trace();

    // Warm: one span, first-touch of the message column, first-touch of the
    // numeric attribute column, one completion. HashMaps get their first inserts.
    let warm = buffer
        .open_span(id.clone(), 0, 0, warmup_name, 1, 0)
        .unwrap();
    buffer
        .write_tag(warm, 12, ColumnValue::Number(0.0))
        .unwrap();
    buffer
        .append_log(warm, EntryType::Info, Some(warmup_log), 0, 2)
        .unwrap();
    buffer.end_ok(warm, 3).unwrap();

    let before = allocations();
    let span = buffer.open_span(id, 0, 0, hot_name, 4, 0).unwrap();
    let after_open = allocations();
    buffer
        .write_tag(span, 12, ColumnValue::Number(1.0))
        .unwrap();
    let after_tag = allocations();
    buffer
        .append_log(span, EntryType::Info, Some(hot_log), 1, 5)
        .unwrap();
    let after_log = allocations();
    buffer.end_ok(span, 6).unwrap();
    let after_end = allocations();

    let open = after_open - before;
    let tag = after_tag - after_open;
    let log = after_log - after_tag;
    let end = after_end - after_log;
    assert_eq!(
        (open, tag, log, end),
        (0, 0, 0, 0),
        "post-warmup allocations (open, tag, log, end) must all be zero"
    );
}
