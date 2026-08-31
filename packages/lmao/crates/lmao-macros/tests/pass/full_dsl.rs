use std::sync::Arc;

use lmao_core::{Clock, FieldStrategy, SpanBuffer, SpanIdentity, TextInput, TraceAnchor, TraceId};
use lmao_macros::define_log_schema;

define_log_schema!(pub FullSchema {
    latency: number,
    count: uint64,
    hit: boolean,
    route: category,
    detail: text,
    method: enum["GET", "POST"],
});

struct FixedClock;

impl Clock for FixedClock {
    fn wall_nanos(&self) -> i64 {
        0
    }

    fn monotonic_nanos(&self) -> u64 {
        0
    }
}

fn main() {
    let clock = FixedClock;
    let anchor = TraceAnchor::capture(&clock);
    let identity = Arc::new(SpanIdentity {
        thread_id: 1,
        span_id: 1,
        trace_id: TraceId::new("macro-fixture").unwrap(),
        parent: None,
    });
    let span =
        SpanBuffer::start_dynamic(identity, 8, TextInput::Static("fixture"), &anchor, &clock);
    let mut schema = FullSchema::from_span(span);
    schema
        .tag_latency(1.0)
        .tag_count(2)
        .tag_hit(true)
        .tag_route(TextInput::Static("route"))
        .tag_detail(TextInput::Static("detail"));
    schema.tag_method(1).unwrap();
    assert_eq!(schema.get_latency(0), Some(1.0));
    assert_eq!(schema.get_count(0), Some(2));
    assert_eq!(schema.get_hit(0), Some(true));
    assert_eq!(schema.get_route(0), Some("route"));
    assert_eq!(schema.get_detail(0), Some("detail"));
    assert_eq!(schema.get_method(0), Some("POST"));
    assert!(schema.set_method(0, 2).is_err());
    assert_eq!(FullSchema::METHOD_VALUES, &["GET", "POST"]);
    assert_eq!(FullSchema::FIELD_META[3].strategy, FieldStrategy::Category);
    assert_eq!(FullSchema::FIELD_META[4].strategy, FieldStrategy::Text);
    let _span = schema.into_span();
}
