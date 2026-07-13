//! The 24 entry types, aligned exactly with the TypeScript runtime mapping.
//!
//! Everything — user logs AND internal metrics — flows through the same table and
//! flush path as one dense entry-type lane in the packed row header. Discriminants
//! 1..=4 MUST match the Zig allocator constants in
//! `packages/lmao/src/lib/wasm/allocator.zig`.

/// Dense entry-type discriminant stored in the low byte of the packed row header.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum EntryType {
    SpanStart = 1,
    SpanOk = 2,
    SpanErr = 3,
    SpanException = 4,
    SpanRetry = 5,
    Trace = 6,
    Debug = 7,
    Info = 8,
    Warn = 9,
    Error = 10,
    FfAccess = 11,
    FfUsage = 12,
    PeriodStart = 13,
    OpInvocations = 14,
    OpErrors = 15,
    OpExceptions = 16,
    OpDurationTotal = 17,
    OpDurationOk = 18,
    OpDurationErr = 19,
    OpDurationMin = 20,
    OpDurationMax = 21,
    BufferWrites = 22,
    BufferSpans = 23,
    BufferCapacity = 24,
}

impl EntryType {
    pub const COUNT: usize = 24;

    /// A completion entry is what row 1 of every span buffer must always hold.
    #[inline]
    pub const fn is_completion(self) -> bool {
        matches!(self, Self::SpanOk | Self::SpanErr | Self::SpanException)
    }

    #[inline]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    pub const fn from_u8(v: u8) -> Option<Self> {
        if v >= 1 && v <= 24 {
            // SAFETY: repr(u8), contiguous discriminants 1..=24 asserted above.
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
    fn roundtrip_all_24() {
        for v in 1u8..=24 {
            assert_eq!(EntryType::from_u8(v).unwrap().as_u8(), v);
        }
        assert!(EntryType::from_u8(0).is_none());
        assert!(EntryType::from_u8(25).is_none());
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
