//! Op/span context pattern, per `specs/lmao/01c_context_flow_and_op_wrappers.md`
//! and `01l_op_context_pattern.md`.
//!
//! [`TraceContext`] is the per-trace root: trace id + timestamp anchor + clock.
//! [`SpanContext`] owns one span's buffer during execution and records the
//! completion (row 1) from the `Result` the body returns. Panic safety is free:
//! row 1 is pre-armed `span-exception` at buffer creation, so an unwinding body
//! leaves a valid completion behind.
//!
//! ## Async/tokio identity decision (documented for jcode adoption)
//!
//! `span_id` counters are THREAD-local ([`crate::identity::next_span_id`]). Under
//! a multi-threaded tokio runtime a task may migrate between polls, but identity
//! is captured ONCE at span creation — `(thread_id, span_id)` is a unique label,
//! not a thread-affinity claim, so migration cannot cause collisions (the pair was
//! unique the moment it was minted; counters never hand out the same value twice
//! on one thread). Task-local counters were rejected: they'd need a runtime dep in
//! the core crate and buy nothing — the spec's definition ("a span is a unit of
//! work within a single thread of execution", `01b4`) maps to a poll-segment, and
//! cross-thread parentage is explicitly supported.

use crate::arena::TextInput;
use crate::buffer::{SourceMetadata, SpanBuffer};
use crate::clock::{Clock, TraceAnchor};
use crate::entry_type::EntryType;
use crate::identity::{SpanIdentity, TraceId, next_span_id};
use crate::result::{SpanOutcome, Transient};
use crate::scope::{ScopeEntry, SpanScope};
use std::sync::Arc;

/// Per-trace root: shared time reference + identity factory.
pub struct TraceContext {
    pub trace_id: TraceId,
    pub anchor: TraceAnchor,
    clock: Arc<dyn Clock>,
    thread_id: u64,
}

impl TraceContext {
    /// `thread_id` comes from the host's entropy seam once per process/worker
    /// (`01b4`); pass it in rather than generating here so a simulation can seed it.
    pub fn new(trace_id: TraceId, thread_id: u64, clock: Arc<dyn Clock>) -> Self {
        let anchor = TraceAnchor::capture(&*clock);
        Self {
            trace_id,
            anchor,
            clock,
            thread_id,
        }
    }

    pub fn clock(&self) -> &dyn Clock {
        &*self.clock
    }

    /// Anchored epoch-nanos timestamp.
    #[inline]
    pub fn now(&self) -> i64 {
        self.anchor.timestamp(&*self.clock)
    }

    /// Mint an identity for a new span (root when `parent` is `None`).
    pub fn identity(&self, parent: Option<Arc<SpanIdentity>>) -> Arc<SpanIdentity> {
        Arc::new(SpanIdentity {
            thread_id: self.thread_id,
            span_id: next_span_id(),
            trace_id: self.trace_id.clone(),
            parent,
        })
    }

    /// Start a span and hand its context to `f`; row 1 is completed from the
    /// returned `Result`. Returns `(body result, finished buffer)` — the caller
    /// attaches the buffer to its parent or hands it to the flush pipeline.
    #[doc(hidden)]
    pub fn __span<T, E>(
        &self,
        name: TextInput<'_>,
        parent: Option<Arc<SpanIdentity>>,
        capacity: usize,
        source: SourceMetadata,
        f: impl FnOnce(&mut SpanContext<'_>) -> Result<T, E>,
    ) -> (Result<T, E>, SpanBuffer) {
        let mut ctx = SpanContext::start(self, self.identity(parent), capacity, name);
        ctx.set_source(source);
        let out = f(&mut ctx);
        let buf = ctx.finish(match &out {
            Ok(_) => SpanOutcome::Ok,
            Err(_) => SpanOutcome::Err,
        });
        (out, buf)
    }

    /// [`Self::span`] with the `01l` retry pattern: the policy rides on the error
    /// value ([`Transient`]); each retry gets a FRESH span buffer (attempts are
    /// separate spans sharing the parent) and `sleep` receives the policy delay —
    /// inject `std::thread::sleep`, `tokio::time::sleep` via a bridge, or a no-op
    /// in a deterministic simulation.
    #[doc(hidden)]
    pub fn __span_with_retry<T, E>(
        &self,
        name: TextInput<'_>,
        parent: Option<Arc<SpanIdentity>>,
        capacity: usize,
        source: SourceMetadata,
        mut sleep: impl FnMut(u64),
        mut f: impl FnMut(&mut SpanContext<'_>) -> Result<T, Transient<E>>,
    ) -> (Result<T, Transient<E>>, Vec<SpanBuffer>) {
        let mut attempts = Vec::new();
        let mut attempt = 0u32;
        loop {
            let (out, buf) = self.__span(name, parent.clone(), capacity, source, &mut f);
            attempts.push(buf);
            match out {
                Ok(v) => return (Ok(v), attempts),
                Err(t) => {
                    if attempt + 1 >= t.policy.max_attempts() {
                        return (Err(t), attempts);
                    }
                    sleep(t.policy.delay_ms(attempt));
                    attempt += 1;
                }
            }
        }
    }
}

/// One executing span: buffer + trace back-reference, with the `{span, log, tag,
/// ok, err}` surface. Attribute (`tag_*`/typed log value) writers are generated
/// per schema by `lmao_macros::define_log_schema!` on a wrapper struct; this core
/// context covers the schema-independent system columns.
pub struct SpanContext<'t> {
    trace: &'t TraceContext,
    buf: SpanBuffer,
}

impl<'t> SpanContext<'t> {
    pub fn start(
        trace: &'t TraceContext,
        identity: Arc<SpanIdentity>,
        capacity: usize,
        name: TextInput<'_>,
    ) -> Self {
        let buf = SpanBuffer::start_dynamic(identity, capacity, name, &trace.anchor, trace.clock());
        Self { trace, buf }
    }

    /// Record the source attribution injected by `span!`.
    pub fn set_source(&mut self, source: SourceMetadata) {
        self.buf.set_source(source);
    }

    /// Append a log entry with its format-string template. Returns the row for
    /// schema-generated typed-value writers to target.
    #[doc(hidden)]
    #[inline]
    pub fn __log(&mut self, level: EntryType, template: &'static str, line: u32) -> usize {
        debug_assert!(matches!(
            level,
            EntryType::Trace
                | EntryType::Debug
                | EntryType::Info
                | EntryType::Warn
                | EntryType::Error
        ));
        self.buf.append_dynamic(
            level,
            Some(TextInput::Static(template)),
            line,
            &self.trace.anchor,
            self.trace.clock(),
        )
    }

    /// Append a metric/flag entry (no template).
    #[inline]
    pub fn append(&mut self, entry: EntryType) -> usize {
        self.buf
            .append_dynamic(entry, None, 0, &self.trace.anchor, self.trace.clock())
    }

    /// The buffer being written — schema wrappers use this plus the row indices
    /// returned by the log macro to place typed values.
    pub fn buffer_mut(&mut self) -> &mut SpanBuffer {
        &mut self.buf
    }

    pub fn identity(&self) -> Arc<SpanIdentity> {
        self.buf.identity.clone()
    }

    /// Merge scope attributes into this span (`01i`): `Some(value)` sets a field,
    /// `None` clears it, and a field the update does not name is left alone.
    ///
    /// Scope applies to EVERY row of this span that has no direct write to the same
    /// column, and is inherited by children created after this call. Children
    /// created BEFORE it keep the snapshot they captured — the update replaces this
    /// span's scope value, it never mutates the one already handed out.
    ///
    /// Use schema-generated `tag_*` row-0 writes for values that describe only the
    /// span's entry point, and scope for context that should appear on every row and
    /// every descendant.
    pub fn set_scope(&mut self, update: &[ScopeEntry]) {
        self.buf.set_scope(update);
    }

    /// Read-only view of the current scope (`01i`'s `ctx.scope`).
    pub fn scope(&self) -> Option<&SpanScope> {
        self.buf.scope()
    }

    /// Nest a child span (buffer attached to this span's children).
    ///
    /// The child inherits this span's scope AT CREATION, by reference: one `Arc`
    /// refcount bump, no copy, and immune to any later `set_scope` here. That is
    /// `01i`'s snapshot semantics, and it is why the child is built here rather than
    /// delegated to [`TraceContext::span`] — the scope has to be in place before the
    /// body runs, since the body may create grandchildren.
    #[doc(hidden)]
    pub fn __child<T, E>(
        &mut self,
        name: TextInput<'_>,
        capacity: usize,
        source: SourceMetadata,
        f: impl FnOnce(&mut SpanContext<'_>) -> Result<T, E>,
    ) -> Result<T, E> {
        let identity = self.trace.identity(Some(self.identity()));
        let mut ctx = SpanContext::start(self.trace, identity, capacity, name);
        ctx.set_source(source);
        ctx.buf.inherit_scope(self.buf.scope_handle());
        let out = f(&mut ctx);
        let child = ctx.finish(match &out {
            Ok(_) => SpanOutcome::Ok,
            Err(_) => SpanOutcome::Err,
        });
        self.buf.add_child(child);
        out
    }

    /// Complete row 1 and release the buffer.
    pub fn finish(mut self, outcome: SpanOutcome) -> SpanBuffer {
        match outcome {
            SpanOutcome::Ok => self.buf.end_ok(&self.trace.anchor, self.trace.clock()),
            SpanOutcome::Err => self.buf.end_err(&self.trace.anchor, self.trace.clock()),
            // Row 1 is already pre-armed span-exception; leave it untouched so the
            // creation-time timestamp marks where execution stopped being observed.
            SpanOutcome::Exception => {}
        }
        self.buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

    fn trace() -> TraceContext {
        TraceContext::new(
            TraceId::new("ctx-test").unwrap(),
            7,
            Arc::new(TickClock(AtomicU64::new(0))),
        )
    }

    #[test]
    fn span_completes_ok_and_err_from_result() {
        let t = trace();
        let (out, buf) = t.__span(
            TextInput::Static("op-a"),
            None,
            8,
            SourceMetadata::UNATTRIBUTED,
            |ctx| {
                ctx.__log(EntryType::Info, "step {n}", 42);
                Ok::<_, ()>(1)
            },
        );
        assert_eq!(out, Ok(1));
        assert_eq!(buf.entry_type_at(1), Some(EntryType::SpanOk));
        assert_eq!(buf.dynamic_name(), Some("op-a"));
        assert_eq!(buf.dynamic_message_at(2), Some("step {n}"));
        assert_eq!(buf.line_at(2), 42);

        let (out, buf) = t.__span(
            TextInput::Static("op-b"),
            None,
            8,
            SourceMetadata::UNATTRIBUTED,
            |_| Err::<(), _>("boom"),
        );
        assert!(out.is_err());
        assert_eq!(buf.entry_type_at(1), Some(EntryType::SpanErr));
    }

    #[test]
    fn children_nest_and_share_trace_id() {
        let t = trace();
        let (_, buf) = t.__span(
            TextInput::Static("parent"),
            None,
            8,
            SourceMetadata::UNATTRIBUTED,
            |ctx| {
                ctx.__child(
                    TextInput::Static("kid"),
                    8,
                    SourceMetadata::UNATTRIBUTED,
                    |_| Ok::<_, ()>(()),
                )
            },
        );
        assert_eq!(buf.children().len(), 1);
        let kid = &buf.children()[0];
        assert!(kid.identity.is_child_of(&buf.identity));
        assert_eq!(kid.identity.trace_id, buf.identity.trace_id);
    }

    #[test]
    fn retry_policy_on_error_drives_attempts() {
        let t = trace();
        let mut calls = 0;
        let mut slept = Vec::new();
        let (out, attempts) = t.__span_with_retry(
            TextInput::Static("flaky"),
            None,
            8,
            SourceMetadata::UNATTRIBUTED,
            |ms| slept.push(ms),
            |_| {
                calls += 1;
                if calls < 3 {
                    Err(Transient::fixed("try again", 5, 10))
                } else {
                    Ok(calls)
                }
            },
        );
        assert_eq!(out, Ok(3));
        assert_eq!(attempts.len(), 3, "each attempt is its own span");
        assert_eq!(slept, vec![10, 10]);
        // Failed attempts recorded span-err, final one span-ok.
        assert_eq!(attempts[0].entry_type_at(1), Some(EntryType::SpanErr));
        assert_eq!(attempts[2].entry_type_at(1), Some(EntryType::SpanOk));
    }

    #[test]
    fn retry_exhaustion_returns_last_error() {
        let t = trace();
        let (out, attempts) = t.__span_with_retry(
            TextInput::Static("doomed"),
            None,
            8,
            SourceMetadata::UNATTRIBUTED,
            |_| {},
            |_| Err::<(), _>(Transient::fixed("nope", 2, 1)),
        );
        assert!(out.is_err());
        assert_eq!(attempts.len(), 2);
    }
}
