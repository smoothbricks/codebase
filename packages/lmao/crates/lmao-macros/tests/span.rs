use std::sync::Arc;

use lmao_core::{Clock, EntryType, SpanContext, TraceContext, TraceId, Transient};
use lmao_macros::{child, log, span, span_with_retry};

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
    let git_sha = source
        .git_sha
        .expect("git checkout records a last-touch commit");
    assert_eq!(git_sha.len(), 40);
    assert!(git_sha.bytes().all(|byte| byte.is_ascii_hexdigit()));

    let parent = root.identity.clone();
    let (result, child) = span!(trace, parent.clone(), "child", child_body);
    assert_eq!(result, Ok(()));
    assert!(child.identity.is_child_of(&parent));
}

#[test]
fn log_child_and_retry_macros_inject_provenance() {
    let trace = TraceContext::new(TraceId::new("macro-tree").unwrap(), 7, Arc::new(FixedClock));
    let (result, root) = span!(trace, "root", |context| {
        log!(context, EntryType::Info, "root log");
        child!(context, "child", |child_context| {
            log!(child_context, EntryType::Debug, "child log");
            Ok::<_, ()>(())
        })?;
        Ok::<_, ()>(())
    });
    assert_eq!(result, Ok(()));
    assert_eq!(
        root.line_at(2),
        root.source().expect("root source").line + 1
    );
    let child = &root.children()[0];
    assert!(child.source().is_some());
    assert!(child.line_at(2) > 0);

    let mut attempt = 0u32;
    let (result, attempts) = span_with_retry!(trace, "retry", None, 8, |_| {}, |_| {
        attempt += 1;
        if attempt == 1 {
            Err(Transient::fixed("again", 2, 0))
        } else {
            Ok(7)
        }
    },);
    assert_eq!(result, Ok(7));
    assert_eq!(attempts.len(), 2);
    assert!(attempts.iter().all(|span| span.source().is_some()));
}
