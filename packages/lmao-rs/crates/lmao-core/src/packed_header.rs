//! Packed native row headers: low 8 bits are [`EntryType`], high 24 bits are a
//! manifest-global vocabulary ID.

use crate::EntryType;
use core::fmt;

/// Largest value representable by the packed header's unsigned 24-bit ID lane.
pub const MAX_VOCABULARY_ID: u32 = 0x00ff_ffff;
const VOCABULARY_SHIFT: u32 = 8;
const ENTRY_TYPE_MASK: u32 = 0xff;

/// A validated, nonzero vocabulary ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct VocabularyId(u32);

impl VocabularyId {
    #[inline]
    pub fn new(value: u32) -> Result<Self, InvalidVocabularyId> {
        Self::try_from(value)
    }

    #[inline]
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// A value which cannot be represented as a nonzero u24 vocabulary ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidVocabularyId(pub u32);

impl fmt::Display for InvalidVocabularyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "vocabulary ID must be in 1..={MAX_VOCABULARY_ID}, got {}",
            self.0
        )
    }
}

impl std::error::Error for InvalidVocabularyId {}

impl TryFrom<u32> for VocabularyId {
    type Error = InvalidVocabularyId;

    #[inline]
    fn try_from(value: u32) -> Result<Self, Self::Error> {
        if (1..=MAX_VOCABULARY_ID).contains(&value) {
            Ok(Self(value))
        } else {
            Err(InvalidVocabularyId(value))
        }
    }
}

impl From<VocabularyId> for u32 {
    #[inline]
    fn from(value: VocabularyId) -> Self {
        value.get()
    }
}

/// A nonzero vocabulary ID was supplied for a row kind that cannot be static.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StaticVocabularyNotAllowed(pub EntryType);

impl fmt::Display for StaticVocabularyNotAllowed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "entry type {:?} cannot carry a static vocabulary ID",
            self.0
        )
    }
}

impl std::error::Error for StaticVocabularyNotAllowed {}

/// Pack a dynamic/lifecycle row. The vocabulary lane is always zero.
#[inline]
pub const fn pack_dynamic(entry_type: EntryType) -> u32 {
    entry_type.as_u8() as u32
}

/// Pack a static row after validating that its entry kind permits a vocabulary ID.
#[inline]
pub fn pack_static(
    entry_type: EntryType,
    vocabulary_id: VocabularyId,
) -> Result<u32, StaticVocabularyNotAllowed> {
    if !supports_static_vocabulary(entry_type) {
        return Err(StaticVocabularyNotAllowed(entry_type));
    }
    Ok((vocabulary_id.get() << VOCABULARY_SHIFT) | entry_type.as_u8() as u32)
}

/// Whether a row kind may carry a nonzero vocabulary ID.
#[inline]
pub const fn supports_static_vocabulary(entry_type: EntryType) -> bool {
    matches!(
        entry_type,
        EntryType::SpanStart
            | EntryType::Trace
            | EntryType::Debug
            | EntryType::Info
            | EntryType::Warn
            | EntryType::Error
    )
}

#[inline]
pub const fn entry_type_from_header(header: u32) -> Option<EntryType> {
    EntryType::from_u8((header & ENTRY_TYPE_MASK) as u8)
}

#[inline]
pub fn vocabulary_id_from_header(header: u32) -> Option<VocabularyId> {
    let value = header >> VOCABULARY_SHIFT;
    VocabularyId::try_from(value).ok()
}
