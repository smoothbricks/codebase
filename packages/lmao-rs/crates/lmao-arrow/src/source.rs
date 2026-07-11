//! The conversion input seam.
//!
//! `lmao-core`'s `SpanBuffer` currently exposes system columns only (timestamps +
//! entry types); message templates and schema attribute columns arrive with the
//! `lmao-macros` generated buffers. [`SpanSource`] abstracts what conversion needs so
//! this crate is not blocked on (and not coupled to) that codegen: core's buffer gets
//! a minimal impl today, macro-generated buffers implement the same trait later.
//!
//! `MockSpan` is the test/bench implementation — it carries messages and children,
//! which core's `SpanBuffer` cannot yet construct (no child-attach API; noted as a
//! core gap).

use lmao_core::{SpanBuffer, SpanIdentity};

/// One node in a span tree, as seen by the Arrow conversion walk (`01k`).
pub trait SpanSource {
    fn identity(&self) -> &SpanIdentity;
    /// Number of valid rows in THIS buffer (not counting overflow/children).
    fn row_count(&self) -> usize;
    fn timestamp(&self, row: usize) -> i64;
    fn entry_type(&self, row: usize) -> u8;
    /// Format-string template / span name / flag name for the row (`01f`: the
    /// `message` column is dictionary-encoded and NEVER interpolated).
    fn message(&self, _row: usize) -> Option<&str> {
        None
    }
    /// Overflow continuation (same identity), yielded immediately after this buffer
    /// so one logical span's rows stay contiguous (`01k`).
    fn overflow(&self) -> Option<&Self>;
    /// Child spans, walked depth-first pre-order after this buffer's chain.
    fn children(&self) -> &[Self]
    where
        Self: Sized;
}

impl SpanSource for SpanBuffer {
    fn identity(&self) -> &SpanIdentity {
        &self.identity
    }

    fn row_count(&self) -> usize {
        self.write_index()
    }

    fn timestamp(&self, row: usize) -> i64 {
        self.timestamp_at(row).unwrap_or(0)
    }

    fn entry_type(&self, row: usize) -> u8 {
        self.entry_type_at(row).map(|e| e.as_u8()).unwrap_or(0)
    }

    fn overflow(&self) -> Option<&Self> {
        self.overflow()
    }

    fn children(&self) -> &[Self] {
        self.children()
    }
}

/// Depth-first PRE-ORDER walk over root buffers: each buffer, then its overflow
/// chain contiguously, then its children recursively (`01k`: parent before children,
/// same-branch rows adjacent for Parquet locality and streaming reconstruction).
pub fn walk_pre_order<'a, S: SpanSource, F: FnMut(&'a S)>(roots: &'a [S], f: &mut F) {
    for root in roots {
        walk_node(root, f);
    }
}

fn walk_node<'a, S: SpanSource, F: FnMut(&'a S)>(node: &'a S, f: &mut F) {
    let mut buffer = Some(node);
    while let Some(b) = buffer {
        f(b);
        buffer = b.overflow();
    }
    for child in node.children() {
        walk_node(child, f);
    }
}

/// Owned test/bench span tree with messages and children. Public because criterion
/// benches and downstream integration tests need to build realistic trees while
/// `lmao-core::SpanBuffer` lacks child construction.
#[derive(Debug)]
pub struct MockSpan {
    pub identity: std::sync::Arc<SpanIdentity>,
    pub timestamps: Vec<i64>,
    pub entry_types: Vec<u8>,
    /// Parallel to rows; `None` = null message.
    pub messages: Vec<Option<String>>,
    pub overflow: Option<Box<MockSpan>>,
    pub children: Vec<MockSpan>,
}

impl SpanSource for MockSpan {
    fn identity(&self) -> &SpanIdentity {
        &self.identity
    }

    fn row_count(&self) -> usize {
        self.timestamps.len()
    }

    fn timestamp(&self, row: usize) -> i64 {
        self.timestamps[row]
    }

    fn entry_type(&self, row: usize) -> u8 {
        self.entry_types[row]
    }

    fn message(&self, row: usize) -> Option<&str> {
        self.messages.get(row).and_then(|m| m.as_deref())
    }

    fn overflow(&self) -> Option<&Self> {
        self.overflow.as_deref()
    }

    fn children(&self) -> &[Self] {
        &self.children
    }
}
