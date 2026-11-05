//! Row-clock and stored-expiry declarations (specs/columine/01-stored-row-sweep.md §2).
use columine_types::types::{ErrorCode, ProgramSections};

/// `TMP1`, framed as a VM-owned program extension.
pub const TEMPORAL_MAGIC: u32 = 0x3150_4d54;
const ENTRY_BYTES: usize = 24;
/// No companion: evaluate the static predicate on changed stored keys.
pub const NO_COMPANION: u32 = u32::MAX;

/// A stored expiry's compile-time classification; its predicate contains no clock reads.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Expiry {
    pub slot: u32,
    pub predicate: u32,
    pub companion: u32,
    pub field: u8,
    pub micros: bool,
    pub age: i64,
}

/// A borrowed, validated temporal program table. Entries are strictly ordered by owner slot.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Temporal<'a> {
    pub clock: u32,
    entries: &'a [u8],
}

fn word(bytes: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(bytes[at..at + 4].try_into().expect("validated table word"))
}
impl<'a> Temporal<'a> {
    pub fn parse(sections: &ProgramSections<'a>) -> Result<Option<Self>, ErrorCode> {
        let tables = sections.tables().ok_or(ErrorCode::InvalidProgram)?;
        let Some(bytes) = tables.extension(TEMPORAL_MAGIC) else {
            return Ok(None);
        };
        if bytes.len() < 8 {
            return Err(ErrorCode::InvalidProgram);
        }
        let count = usize::try_from(word(bytes, 4)).map_err(|_| ErrorCode::InvalidProgram)?;
        if count
            .checked_mul(ENTRY_BYTES)
            .and_then(|n| n.checked_add(8))
            != Some(bytes.len())
        {
            return Err(ErrorCode::InvalidProgram);
        }
        let table = Self {
            clock: word(bytes, 0),
            entries: &bytes[8..],
        };
        let mut previous = None;
        for entry in table.iter() {
            if previous.is_some_and(|slot| slot >= entry.slot) {
                return Err(ErrorCode::InvalidProgram);
            }
            previous = Some(entry.slot);
        }
        for entry in table.entries.as_chunks::<ENTRY_BYTES>().0 {
            if entry[13] > 1 || entry[14] != 0 || entry[15] != 0 {
                return Err(ErrorCode::InvalidProgram);
            }
        }
        Ok(Some(table))
    }
    fn entry(&self, index: usize) -> Expiry {
        let bytes = &self.entries[index * ENTRY_BYTES..(index + 1) * ENTRY_BYTES];
        Expiry {
            slot: word(bytes, 0),
            predicate: word(bytes, 4),
            companion: word(bytes, 8),
            field: bytes[12],
            micros: bytes[13] == 1,
            age: i64::from_le_bytes(bytes[16..24].try_into().expect("validated deadline age")),
        }
    }
    pub fn iter(&self) -> impl ExactSizeIterator<Item = Expiry> + '_ {
        (0..self.entries.len() / ENTRY_BYTES).map(|index| self.entry(index))
    }
    pub fn get(&self, slot: u32) -> Option<Expiry> {
        let mut lo = 0;
        let mut hi = self.entries.len() / ENTRY_BYTES;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let entry = self.entry(mid);
            match entry.slot.cmp(&slot) {
                core::cmp::Ordering::Less => lo = mid + 1,
                core::cmp::Ordering::Greater => hi = mid,
                core::cmp::Ordering::Equal => return Some(entry),
            }
        }
        None
    }
}
