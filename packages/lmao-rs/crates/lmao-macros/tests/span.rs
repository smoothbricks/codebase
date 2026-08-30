use std::sync::Arc;

use lmao_core::{Clock, SpanContext, TraceContext, TraceId};
use lmao_macros::span;

struct FixedClock;

impl Clock for FixedClock {
    fn wall_nanos(&self) -> i64 {
        1
    }

    fn monotonic_nanos(&self) -> u64 {
        1
    }
}

fn root_body(_context: &mut SpanContext<'_>) -> Result<i32, ()> {
    Ok(42)
}

fn child_body(_context: &mut SpanContext<'_>) -> Result<(), ()> {
    Ok(())
}

#[test]
fn span_macro_injects_callsite_and_supports_parent_form() {
    let trace = TraceContext::new(TraceId::new("macro-span").unwrap(), 7, Arc::new(FixedClock));
    let (result, root) = span!(trace, "root", root_body);
    assert_eq!(result, Ok(42));
    assert_eq!(root.capacity(), lmao_core::DEFAULT_CAPACITY);
    let source = root.source().expect("span! injects source attribution");
    assert_eq!(source.package_name, "lmao-macros");
    assert!(source.package_file.ends_with("tests/span.rs"));
    assert!(source.line > 0);

    let parent = root.identity.clone();
    let (result, child) = span!(trace, parent.clone(), "child", child_body);
    assert_eq!(result, Ok(()));
    assert!(child.identity.is_child_of(&parent));
}
