//! The proof-buffer layout (ports `timestamp_proof_layout.zig` 1:1).
//!
//! One buffer, three regions, all unaligned little-endian:
//! `[timestamps: i64 × capacity][entry_types: u8 × capacity][write_index: u32]`.
//! Rows 0 and 1 are the span slots (start + end); `write_log_entry` appends
//! from the persisted write index. The Zig wrote through unaligned pointers;
//! Rust writes explicit LE bytes, so no alignment contract exists at all.

pub const ENTRY_TYPE_SPAN_START: u8 = 1;
pub const ENTRY_TYPE_SPAN_OK: u8 = 2;
pub const ENTRY_TYPE_SPAN_ERR: u8 = 3;
pub const ENTRY_TYPE_SPAN_EXCEPTION: u8 = 4;

#[inline]
fn write_timestamp(buf: &mut [u8], row: u32, timestamp_nanos: i64) {
    let off = row as usize * 8;
    buf[off..off + 8].copy_from_slice(&timestamp_nanos.to_le_bytes());
}

#[inline]
fn write_entry_type(buf: &mut [u8], capacity: u32, row: u32, entry_type: u8) {
    buf[capacity as usize * 8 + row as usize] = entry_type;
}

#[inline]
fn write_index_offset(capacity: u32) -> usize {
    capacity as usize * 9
}

/// `writeSpanStart`: row 0 = start; row 1 is pre-armed as SPAN_EXCEPTION with
/// timestamp 0 so a crash between start and end is visible in the proof data.
pub fn write_span_start(buf: &mut [u8], capacity: u32, timestamp_nanos: i64) {
    write_timestamp(buf, 0, timestamp_nanos);
    write_entry_type(buf, capacity, 0, ENTRY_TYPE_SPAN_START);
    write_timestamp(buf, 1, 0);
    write_entry_type(buf, capacity, 1, ENTRY_TYPE_SPAN_EXCEPTION);
    let off = write_index_offset(capacity);
    buf[off..off + 4].copy_from_slice(&2u32.to_le_bytes());
}

/// `writeSpanEnd`: overwrite the pre-armed row 1.
pub fn write_span_end(buf: &mut [u8], capacity: u32, entry_type: u8, timestamp_nanos: i64) {
    write_entry_type(buf, capacity, 1, entry_type);
    write_timestamp(buf, 1, timestamp_nanos);
}

/// `writeLogEntry`: append at the persisted write index; returns the row.
pub fn write_log_entry(buf: &mut [u8], capacity: u32, entry_type: u8, timestamp_nanos: i64) -> u32 {
    let off = write_index_offset(capacity);
    let row = u32::from_le_bytes(buf[off..off + 4].try_into().expect("write-index slot"));
    write_timestamp(buf, row, timestamp_nanos);
    write_entry_type(buf, capacity, row, entry_type);
    buf[off..off + 4].copy_from_slice(&(row + 1).to_le_bytes());
    row
}

/// Buffer size for a capacity (the proof harness allocates this).
pub fn buffer_len(capacity: u32) -> usize {
    capacity as usize * 9 + 4
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read_ts(buf: &[u8], row: u32) -> i64 {
        let off = row as usize * 8;
        i64::from_le_bytes(buf[off..off + 8].try_into().unwrap())
    }

    #[test]
    fn span_start_arms_exception_row() {
        let mut buf = vec![0u8; buffer_len(16)];
        write_span_start(&mut buf, 16, 1_234_567_890);
        assert_eq!(read_ts(&buf, 0), 1_234_567_890);
        assert_eq!(buf[16 * 8], ENTRY_TYPE_SPAN_START);
        assert_eq!(read_ts(&buf, 1), 0);
        assert_eq!(buf[16 * 8 + 1], ENTRY_TYPE_SPAN_EXCEPTION);
        // write index persisted as 2
        assert_eq!(
            u32::from_le_bytes(buf[16 * 9..16 * 9 + 4].try_into().unwrap()),
            2
        );
    }

    #[test]
    fn span_end_overwrites_row_one() {
        let mut buf = vec![0u8; buffer_len(16)];
        write_span_start(&mut buf, 16, 10);
        write_span_end(&mut buf, 16, ENTRY_TYPE_SPAN_OK, 99);
        assert_eq!(read_ts(&buf, 1), 99);
        assert_eq!(buf[16 * 8 + 1], ENTRY_TYPE_SPAN_OK);
    }

    #[test]
    fn log_entries_append_from_persisted_index() {
        let mut buf = vec![0u8; buffer_len(8)];
        write_span_start(&mut buf, 8, 1);
        assert_eq!(write_log_entry(&mut buf, 8, 7, 100), 2);
        assert_eq!(write_log_entry(&mut buf, 8, 8, 200), 3);
        assert_eq!(read_ts(&buf, 3), 200);
        assert_eq!(buf[8 * 8 + 3], 8);
    }
}
