//! Physical undo/delta wire entries. All mutation families restore actual
//! bounded bytes; no logical inverse can rewrite unrelated allocated cells.

/// Physical record discriminant in exported undo/delta/fork segments.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FlatUndoOp {
    /// Restore `pad1` (1..=8) prior bytes at absolute state offset `key`
    /// from `aux`'s little-endian bytes without changing layout.
    StateBytes = 15,
}

impl FlatUndoOp {
    pub const fn from_u8(value: u8) -> Option<Self> {
        match value {
            15 => Some(Self::StateBytes),
            _ => None,
        }
    }
}

/// One serialized physical record. Layout: op@0, zero@1, byte length@2,
/// zero@3, absolute offset@4, zero@8, zero@12, payload@16; size 24.
/// The explicit wire codec, not the Rust memory layout, defines the ABI.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FlatUndoEntry {
    pub op: FlatUndoOp,
    /// Reserved zero lane.
    pub slot: u32,
    /// Physical payload length, 1..=8.
    pub pad1: u8,
    /// Reserved zero lane.
    pub pad2: u8,
    /// Absolute state offset.
    pub key: u32,
    /// Reserved zero lane.
    pub prev_value: u32,
    /// Prior or subsequent bytes in little-endian order.
    pub aux: u64,
}

/// Serialized size of one undo entry.
pub const FLAT_UNDO_ENTRY_SIZE: u32 = 24;

/// One undo/redo pair occupying 48 serialized bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FlatDeltaEntry {
    pub undo: FlatUndoEntry,
    pub redo: FlatUndoEntry,
}

/// Serialized size of a delta pair.
pub const FLAT_DELTA_ENTRY_SIZE: u32 = 48;

impl FlatUndoEntry {
    /// Serialize with the extern-struct layout (see type doc). Byte 1 is
    /// written as zero.
    pub fn write_to(&self, out: &mut [u8; FLAT_UNDO_ENTRY_SIZE as usize]) {
        out.fill(0);
        out[0] = self.op as u8;
        out[2] = self.pad1;
        out[3] = self.pad2;
        out[4..8].copy_from_slice(&self.key.to_le_bytes());
        out[8..12].copy_from_slice(&self.prev_value.to_le_bytes());
        out[12..16].copy_from_slice(&self.slot.to_le_bytes());
        out[16..24].copy_from_slice(&self.aux.to_le_bytes());
    }

    /// Deserialize; `None` if the op byte is not a `FlatUndoOp` (a corrupt
    /// or foreign segment — an operational condition for segment consumers,
    /// not a programmer bug).
    pub fn read_from(buf: &[u8; FLAT_UNDO_ENTRY_SIZE as usize]) -> Option<Self> {
        let op = FlatUndoOp::from_u8(buf[0])?;
        if !(1..=8).contains(&buf[2])
            || buf[1] != 0
            || buf[3] != 0
            || buf[8..16].iter().any(|byte| *byte != 0)
        {
            return None;
        }
        Some(Self {
            op,
            slot: u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]),
            pad1: buf[2],
            pad2: buf[3],
            key: u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]),
            prev_value: u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]),
            aux: u64::from_le_bytes([
                buf[16], buf[17], buf[18], buf[19], buf[20], buf[21], buf[22], buf[23],
            ]),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The serialized layout is the contract: op@0, zero@1, `_pad1`@2,
    /// `_pad2`@3, key@4 LE, prev_value@8 LE, slot@12 LE, aux@16 LE.
    #[test]
    fn flat_undo_entry_serialized_layout_is_pinned() {
        let entry = FlatUndoEntry {
            op: FlatUndoOp::StateBytes,
            slot: 0,
            pad1: 3,
            pad2: 0,
            key: 0x11223344,
            prev_value: 0,
            aux: 0x99aabbccddeeff00,
        };
        let mut buf = [0xa5u8; 24];
        entry.write_to(&mut buf);
        assert_eq!(buf[0], 15);
        assert_eq!(buf[1], 0);
        assert_eq!(buf[2], 3);
        assert_eq!(buf[3], 0);
        assert_eq!(&buf[4..8], &0x11223344u32.to_le_bytes());
        assert_eq!(&buf[8..12], &[0; 4]);
        assert_eq!(&buf[12..16], &[0; 4]);
        assert_eq!(&buf[16..24], &0x99aabbccddeeff00u64.to_le_bytes());
        assert_eq!(FlatUndoEntry::read_from(&buf), Some(entry));
        assert_eq!(FLAT_UNDO_ENTRY_SIZE, 24);
        assert_eq!(FLAT_DELTA_ENTRY_SIZE, 48);
    }

    /// Logical encodings are not a second accepted ABI.
    #[test]
    fn flat_undo_op_discriminants_match_contract() {
        assert_eq!(FlatUndoOp::StateBytes as u8, 15);
        for opcode in 0..15 {
            assert_eq!(FlatUndoOp::from_u8(opcode), None);
        }
        assert_eq!(FlatUndoEntry::read_from(&[0u8; 24]), None); // 0 invalid
        assert_eq!(
            FlatUndoEntry::read_from(&{
                let mut b = [0u8; 24];
                b[0] = 16;
                b
            }),
            None
        ); // beyond the enum
    }
}
