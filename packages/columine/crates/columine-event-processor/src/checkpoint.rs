//! Checkpoint serialization for dedup state (`dedup/checkpoint.zig`).
//!
//! Byte layout is the cross-session contract (TS persists these blobs):
//! `[header 36B][bloom bits][pad8][meta 8B][pad8][stats 24B][pad8]`.
//! Note the header is NOT padded before the bloom bits (bloom_offset = 36);
//! `requiredSize`'s comment in the Zig narrates a different layout but
//! computes the same total — serialize() is the truth ported here.

use crate::bloom::{CollisionPolicy, DedupState};

pub const CHECKPOINT_MAGIC: u32 = 0x4348_4B50; // "CHKP"
pub const CHECKPOINT_VERSION: u8 = 1;

const HEADER_SIZE: usize = 36;
const META_SIZE: usize = 8;
const STATS_SIZE: usize = 24;

fn align_to_8(offset: usize) -> usize {
    (offset + 7) & !7usize
}

/// Serialize dedup state; returns bytes written or None when the output is
/// too small (`checkpoint.zig serialize`).
pub fn serialize(state: &DedupState, output: &mut [u8]) -> Option<usize> {
    let mut offset = HEADER_SIZE;

    // Bloom filter bits (unpadded start at 36 — layout contract).
    let bloom_offset = offset;
    let bloom_len = state.bloom.bits.len();
    if offset + bloom_len > output.len() {
        return None;
    }
    output[offset..offset + bloom_len].copy_from_slice(&state.bloom.bits);
    offset = align_to_8(offset + bloom_len);

    // Metadata: capacity u32 | hash_count u8 | policy u8 | reserved [2]u8.
    let meta_offset = offset;
    if offset + META_SIZE > output.len() {
        return None;
    }
    output[offset..offset + 4].copy_from_slice(&state.bloom.capacity.to_le_bytes());
    output[offset + 4] = state.bloom.hash_count;
    output[offset + 5] = state.policy as u8;
    output[offset + 6..offset + 8].fill(0);
    offset = align_to_8(offset + META_SIZE);

    // Stats: total_added u64 | duplicates_detected u64 | total_events u64.
    let stats_offset = offset;
    if offset + STATS_SIZE > output.len() {
        return None;
    }
    output[offset..offset + 8].copy_from_slice(&state.bloom.total_added.to_le_bytes());
    output[offset + 8..offset + 16].copy_from_slice(&state.duplicates_detected.to_le_bytes());
    output[offset + 16..offset + 24].copy_from_slice(&state.total_events.to_le_bytes());
    offset = align_to_8(offset + STATS_SIZE);

    // Header: magic u32 | version u8 | flags u8 | reserved1 u16 |
    // bloom_offset u32 | bloom_len u32 | meta_offset u32 | stats_offset u32 |
    // total_size u32 | reserved2 [8]u8.
    output[0..4].copy_from_slice(&CHECKPOINT_MAGIC.to_le_bytes());
    output[4] = CHECKPOINT_VERSION;
    output[5] = 0;
    output[6..8].fill(0);
    output[8..12].copy_from_slice(&(bloom_offset as u32).to_le_bytes());
    output[12..16].copy_from_slice(&(bloom_len as u32).to_le_bytes());
    output[16..20].copy_from_slice(&(meta_offset as u32).to_le_bytes());
    output[20..24].copy_from_slice(&(stats_offset as u32).to_le_bytes());
    output[24..28].copy_from_slice(&(offset as u32).to_le_bytes());
    output[28..36].fill(0);

    Some(offset)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeserializeError {
    InvalidCheckpoint,
    InvalidMagic,
    UnsupportedVersion,
    SizeMismatch,
    OutOfMemory,
}

fn read_u32(input: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_le_bytes(input.get(at..at + 4)?.try_into().ok()?))
}

fn read_u64(input: &[u8], at: usize) -> Option<u64> {
    Some(u64::from_le_bytes(input.get(at..at + 8)?.try_into().ok()?))
}

/// Deserialize checkpoint bytes (`checkpoint.zig deserialize`). The restored
/// bloom geometry must match a fresh filter of the checkpointed capacity —
/// a bit-length mismatch is `SizeMismatch`, not a resize.
pub fn deserialize(input: &[u8]) -> Result<DedupState, DeserializeError> {
    if input.len() < HEADER_SIZE {
        return Err(DeserializeError::InvalidCheckpoint);
    }
    let magic = read_u32(input, 0).ok_or(DeserializeError::InvalidCheckpoint)?;
    if magic != CHECKPOINT_MAGIC {
        return Err(DeserializeError::InvalidMagic);
    }
    if input[4] != CHECKPOINT_VERSION {
        return Err(DeserializeError::UnsupportedVersion);
    }
    let bloom_offset = read_u32(input, 8).ok_or(DeserializeError::InvalidCheckpoint)? as usize;
    let bloom_len = read_u32(input, 12).ok_or(DeserializeError::InvalidCheckpoint)? as usize;
    let meta_offset = read_u32(input, 16).ok_or(DeserializeError::InvalidCheckpoint)? as usize;
    let stats_offset = read_u32(input, 20).ok_or(DeserializeError::InvalidCheckpoint)? as usize;

    let capacity = read_u32(input, meta_offset).ok_or(DeserializeError::InvalidCheckpoint)?;
    let policy = CollisionPolicy::from_u8(
        *input
            .get(meta_offset + 5)
            .ok_or(DeserializeError::InvalidCheckpoint)?,
    )
    .ok_or(DeserializeError::InvalidCheckpoint)?;

    let total_added = read_u64(input, stats_offset).ok_or(DeserializeError::InvalidCheckpoint)?;
    let duplicates_detected =
        read_u64(input, stats_offset + 8).ok_or(DeserializeError::InvalidCheckpoint)?;
    let total_events =
        read_u64(input, stats_offset + 16).ok_or(DeserializeError::InvalidCheckpoint)?;

    let mut state = DedupState::new(capacity, policy);
    if bloom_len != state.bloom.bits.len() {
        return Err(DeserializeError::SizeMismatch);
    }
    let bits = input
        .get(bloom_offset..bloom_offset + bloom_len)
        .ok_or(DeserializeError::InvalidCheckpoint)?;
    state.bloom.bits.copy_from_slice(bits);
    state.bloom.total_added = total_added;
    state.duplicates_detected = duplicates_detected;
    state.total_events = total_events;
    Ok(state)
}

/// Required checkpoint size for a bloom filter of `bloom_bytes`
/// (`checkpoint.zig requiredSize`).
pub fn required_size(bloom_bytes: usize) -> usize {
    let mut size = HEADER_SIZE + bloom_bytes;
    size = align_to_8(size);
    size += META_SIZE;
    size = align_to_8(size);
    size += STATS_SIZE;
    align_to_8(size)
}

#[cfg(test)]
mod tests {
    use super::*;

    // test "checkpoint round-trip"
    #[test]
    fn round_trip() {
        let mut state = DedupState::new(1000, CollisionPolicy::Latest);
        state.should_process(b"event-001");
        state.should_process(b"event-002");
        state.should_process(b"event-001"); // duplicate

        let mut buffer = [0u8; 4096];
        let size = serialize(&state, &mut buffer).unwrap();
        let restored = deserialize(&buffer[..size]).unwrap();

        assert_eq!(restored.total_events, state.total_events);
        assert_eq!(restored.duplicates_detected, state.duplicates_detected);
        assert_eq!(restored.bloom.total_added, state.bloom.total_added);
        assert_eq!(restored.policy, state.policy);
        assert!(restored.bloom.maybe_contains(b"event-001"));
        assert!(restored.bloom.maybe_contains(b"event-002"));
    }

    // test "checkpoint header size" / "meta size" / "stats size" — the Zig
    // pins @sizeOf of extern structs; the Rust writer uses explicit offsets,
    // so the layout is pinned by a serialized byte-image instead.
    #[test]
    fn serialized_layout_pinned() {
        let state = DedupState::new(10, CollisionPolicy::Discard); // 64-byte bloom
        let mut buffer = [0u8; 512];
        let size = serialize(&state, &mut buffer).unwrap();

        // Header fields.
        assert_eq!(&buffer[0..4], &CHECKPOINT_MAGIC.to_le_bytes());
        assert_eq!(buffer[4], CHECKPOINT_VERSION);
        assert_eq!(read_u32(&buffer, 8).unwrap(), 36); // bloom_offset (unpadded!)
        assert_eq!(read_u32(&buffer, 12).unwrap(), 64); // bloom_len
        assert_eq!(read_u32(&buffer, 16).unwrap(), 104); // meta at align8(36+64)
        assert_eq!(read_u32(&buffer, 20).unwrap(), 112); // stats at align8(104+8)
        assert_eq!(read_u32(&buffer, 24).unwrap(), 136); // total
        assert_eq!(size, 136);
        // Meta bytes.
        assert_eq!(read_u32(&buffer, 104).unwrap(), 10); // capacity
        assert_eq!(buffer[104 + 5], CollisionPolicy::Discard as u8);
    }

    // test "invalid magic rejected"
    #[test]
    fn invalid_magic() {
        let buffer = [0u8; 64];
        assert_eq!(
            deserialize(&buffer).unwrap_err(),
            DeserializeError::InvalidMagic
        );
    }

    // test "required size calculation"
    #[test]
    fn required_size_calculation() {
        // 36 + 64 = 100 -> 104; +8 = 112; +24 = 136.
        assert_eq!(required_size(64), 136);
        // And it matches what serialize actually produces (pinned above).
    }

    #[test]
    fn version_and_size_mismatch() {
        let state = DedupState::new(10, CollisionPolicy::Latest);
        let mut buffer = [0u8; 512];
        let size = serialize(&state, &mut buffer).unwrap();

        let mut wrong_version = buffer;
        wrong_version[4] = 2;
        assert_eq!(
            deserialize(&wrong_version[..size]).unwrap_err(),
            DeserializeError::UnsupportedVersion
        );

        // Corrupt bloom_len -> geometry mismatch for the same capacity.
        let mut wrong_len = buffer;
        wrong_len[12..16].copy_from_slice(&63u32.to_le_bytes());
        assert_eq!(
            deserialize(&wrong_len[..size]).unwrap_err(),
            DeserializeError::SizeMismatch
        );
    }
}
