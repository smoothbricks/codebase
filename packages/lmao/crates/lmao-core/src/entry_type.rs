//! The entry types, aligned exactly with the TypeScript runtime mapping.
//!
//! Everything — user logs AND internal metrics — flows through the same table and
//! flush path as one dense entry-type lane in the packed row header. Discriminants
//! 1..=4 MUST remain stable for span lifecycle entries consumed by the WASM and
//! TypeScript ABI.
include!(concat!(env!("OUT_DIR"), "/entry_type.rs"));

impl EntryType {
    /// A completion entry is what row 1 of every span buffer must always hold.
    #[inline]
    pub const fn is_completion(self) -> bool {
        matches!(self, Self::SpanOk | Self::SpanErr | Self::SpanException)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_table_matches_discriminants_and_names() {
        for (index, entry_type) in EntryType::ALL.into_iter().enumerate() {
            let discriminant = u8::try_from(index + 1).unwrap();
            assert_eq!(entry_type.as_u8(), discriminant);
            assert_eq!(EntryType::from_u8(discriminant), Some(entry_type));
            assert_eq!(
                EntryType::NAMES[usize::from(discriminant)],
                entry_type.name()
            );
        }
        assert_eq!(EntryType::NAMES[0], "");
        assert!(EntryType::from_u8(0).is_none());
        assert!(EntryType::from_u8(EntryType::COUNT as u8 + 1).is_none());
    }
}
