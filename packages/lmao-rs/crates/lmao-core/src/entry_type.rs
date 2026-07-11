//! The 23 entry types, per `specs/lmao/01h_entry_types_and_logging_primitives.md`
//! and `specs/lmao/01f_arrow_table_structure.md` ("Metrics as Structured Logs").
//!
//! Everything — user logs AND internal metrics — flows through the same table and
//! flush path as one dense `entry_type` column. Discriminants 1..=4 MUST match the
//! Zig allocator constants in `packages/lmao/src/lib/wasm/allocator.zig`
//! (`ENTRY_TYPE_SPAN_START` etc.) — they are written into shared memory by both sides.

/// Dense entry-type discriminant stored in the `entry_type` u8 column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum EntryType {
    // --- Span lifecycle (4) — discriminants shared with allocator.zig ---
    SpanStart = 1,
    SpanOk = 2,
    SpanErr = 3,
    SpanException = 4,

    // --- Log levels (4) ---
    Info = 5,
    Debug = 6,
    Warn = 7,
    Error = 8,

    // --- Feature flags (2), per 01p_feature_flags.md ---
    FfAccess = 9,
    FfUsage = 10,

    // --- Metrics (13), per 01n_op_and_buffer_metrics.md ---
    PeriodStart = 11,
    OpInvocations = 12,
    OpErrors = 13,
    OpExceptions = 14,
    OpDurationTotal = 15,
    OpDurationOk = 16,
    OpDurationErr = 17,
    OpDurationMin = 18,
    OpDurationMax = 19,
    BufferWrites = 20,
    BufferOverflowWrites = 21,
    BufferCreated = 22,
    BufferOverflows = 23,
}

impl EntryType {
    pub const COUNT: usize = 23;

    /// A completion entry is what row 1 of every span buffer must always hold
    /// (`01b_columnar_buffer_architecture.md`: row 1 is pre-initialized to
    /// `SpanException` at creation for exception safety, overwritten by ok/err).
    #[inline]
    pub const fn is_completion(self) -> bool {
        matches!(self, Self::SpanOk | Self::SpanErr | Self::SpanException)
    }

    #[inline]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    pub const fn from_u8(v: u8) -> Option<Self> {
        if v >= 1 && v <= 23 {
            // SAFETY: repr(u8), contiguous discriminants 1..=23 asserted above.
            Some(unsafe { core::mem::transmute::<u8, EntryType>(v) })
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_all_23() {
        for v in 1u8..=23 {
            assert_eq!(EntryType::from_u8(v).unwrap().as_u8(), v);
        }
        assert!(EntryType::from_u8(0).is_none());
        assert!(EntryType::from_u8(24).is_none());
    }

    #[test]
    fn zig_abi_constants_match() {
        // Must match ENTRY_TYPE_* in packages/lmao/src/lib/wasm/allocator.zig.
        assert_eq!(EntryType::SpanStart.as_u8(), 1);
        assert_eq!(EntryType::SpanOk.as_u8(), 2);
        assert_eq!(EntryType::SpanErr.as_u8(), 3);
        assert_eq!(EntryType::SpanException.as_u8(), 4);
    }
}
