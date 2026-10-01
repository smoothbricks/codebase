//! Index operands: unsigned LEB128.
//!
//! Every program operand that names an input column, a state slot, or a
//! count of matched event types is an *index*: an unsigned LEB128 encoding
//! of a `u32`. Seven value bits per byte, low group first, the high bit set
//! on every byte but the last. Values below 128 take one byte, below 16 384
//! two, and no value takes more than [`MAX_INDEX_LEN`] bytes, so the number
//! of columns, slots or signals a program addresses is bounded by `u32`, not
//! by its operand width.
//!
//! WHY LEB128 and not one width chosen per program: a program's widest index
//! would set the width of every index it holds, so one program past 255
//! columns would pay a second byte on every operand, including the slots and
//! low columns that dominate its reduce section; LEB128 pays the second byte
//! only on the operands past 127. Decoding is one compare per operand on the
//! one-byte path, and the reduce section decodes each instruction once per
//! batch (or once per element of a `FOR_EACH` body), never per cell.
//!
//! Only the canonical (shortest) encoding is accepted: a program has exactly
//! one byte image per meaning, so its content hash identifies it. A trailing
//! zero group or a value past `u32::MAX` is not an index.
//!
//! Every other operand — opcode bytes, type and flag bytes, struct field
//! ordinals and their counts, capacities, lengths and type ids — keeps the
//! fixed width its opcode documents.

/// The longest index encoding: five groups cover 35 bits, the last group
/// carrying the top four bits of a `u32`.
pub const MAX_INDEX_LEN: usize = 5;

/// Encoded length of `value` as an index.
pub const fn index_len(value: u32) -> usize {
    match value {
        0..0x80 => 1,
        0x80..0x4000 => 2,
        0x4000..0x20_0000 => 3,
        0x20_0000..0x1000_0000 => 4,
        _ => 5,
    }
}

/// An index's canonical encoding, for an encoder to append.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EncodedIndex {
    bytes: [u8; MAX_INDEX_LEN],
    len: u8,
}

impl EncodedIndex {
    pub const fn as_bytes(&self) -> &[u8] {
        self.bytes.split_at(self.len as usize).0
    }
}

impl AsRef<[u8]> for EncodedIndex {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

/// Encode `value` as an index.
pub const fn encode_index(value: u32) -> EncodedIndex {
    let len = index_len(value);
    let mut bytes = [0u8; MAX_INDEX_LEN];
    let mut rest = value;
    let mut i = 0;
    while i < len {
        let group = (rest & 0x7f) as u8;
        rest >>= 7;
        bytes[i] = if i + 1 < len { group | 0x80 } else { group };
        i += 1;
    }
    EncodedIndex {
        bytes,
        len: len as u8,
    }
}

/// Decode the index at `code[at..]`, returning its value and the position
/// after it. `None` for a truncated, overlong or out-of-range encoding.
#[inline(always)]
pub fn read_index(code: &[u8], at: usize) -> Option<(u32, usize)> {
    let first = *code.get(at)?;
    if first < 0x80 {
        return Some((u32::from(first), at + 1));
    }
    read_index_multibyte(code, at)
}

#[cold]
fn read_index_multibyte(code: &[u8], at: usize) -> Option<(u32, usize)> {
    let mut value = 0u32;
    for i in 0..MAX_INDEX_LEN {
        let byte = *code.get(at.checked_add(i)?)?;
        let group = u32::from(byte & 0x7f);
        // The fifth group holds bits 28..32; anything above them is past u32.
        if i == MAX_INDEX_LEN - 1 && group > 0x0f {
            return None;
        }
        value |= group << (7 * i);
        if byte & 0x80 == 0 {
            // A zero last group after the first byte is an overlong encoding.
            if i > 0 && byte == 0 {
                return None;
            }
            return Some((value, at + i + 1));
        }
    }
    None
}

/// One instruction's operands, read in wire order. Every read answers `None`
/// past the end of the code or on a malformed index, so a decoder states its
/// layout once and refuses a short or malformed instruction by construction.
#[derive(Clone, Copy, Debug)]
pub struct Operands<'a> {
    code: &'a [u8],
    at: usize,
}

impl<'a> Operands<'a> {
    /// Read `code` from position `at` (the first operand byte).
    pub const fn at(code: &'a [u8], at: usize) -> Self {
        Self { code, at }
    }

    /// Position of the next unread byte.
    pub const fn pos(&self) -> usize {
        self.at
    }

    /// A column, slot or count index.
    #[inline(always)]
    pub fn index(&mut self) -> Option<u32> {
        let (value, next) = read_index(self.code, self.at)?;
        self.at = next;
        Some(value)
    }

    /// One fixed-width byte operand.
    #[inline(always)]
    pub fn byte(&mut self) -> Option<u8> {
        let value = *self.code.get(self.at)?;
        self.at += 1;
        Some(value)
    }

    /// A little-endian `u16`.
    pub fn u16(&mut self) -> Option<u16> {
        let bytes = self.bytes(2)?;
        Some(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    /// A little-endian `u32`.
    pub fn u32(&mut self) -> Option<u32> {
        let bytes = self.bytes(4)?;
        Some(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    /// A little-endian `f32`.
    pub fn f32(&mut self) -> Option<f32> {
        self.u32().map(f32::from_bits)
    }

    /// The next `len` bytes, unread.
    pub fn bytes(&mut self, len: usize) -> Option<&'a [u8]> {
        let end = self.at.checked_add(len)?;
        let bytes = self.code.get(self.at..end)?;
        self.at = end;
        Some(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_length_boundary_round_trips_at_its_length() {
        for (value, len) in [
            (0, 1),
            (0x7f, 1),
            (0x80, 2),
            (0x3fff, 2),
            (0x4000, 3),
            (0x1f_ffff, 3),
            (0x20_0000, 4),
            (0x0fff_ffff, 4),
            (0x1000_0000, 5),
            (u32::MAX, 5),
        ] {
            let encoded = encode_index(value);
            assert_eq!(
                (encoded.as_bytes().len(), index_len(value)),
                (len, len),
                "{value:#x}"
            );
            assert_eq!(
                read_index(encoded.as_bytes(), 0),
                Some((value, len)),
                "{value:#x}"
            );
        }
        assert_eq!(encode_index(300).as_bytes(), [0xac, 0x02]);
    }

    #[test]
    fn refuses_truncated_overlong_and_out_of_range_encodings() {
        assert_eq!(read_index(&[], 0), None);
        assert_eq!(read_index(&[0x80], 0), None);
        // 0 and 1 spelled in two bytes: one meaning, one byte image.
        assert_eq!(read_index(&[0x80, 0x00], 0), None);
        assert_eq!(read_index(&[0x81, 0x00], 0), None);
        // A fifth group above the four bits u32 has left.
        assert_eq!(read_index(&[0xff, 0xff, 0xff, 0xff, 0x1f], 0), None);
        assert_eq!(read_index(&[0x80, 0x80, 0x80, 0x80, 0x80, 0x01], 0), None);
    }

    #[test]
    fn operands_read_in_wire_order_and_refuse_past_the_end() {
        let code = [7, 0xac, 0x02, 0x34, 0x12, 9];
        let mut operands = Operands::at(&code, 0);
        assert_eq!(operands.byte(), Some(7));
        assert_eq!(operands.index(), Some(300));
        assert_eq!(operands.u16(), Some(0x1234));
        assert_eq!(operands.pos(), 5);
        assert_eq!(operands.u16(), None);
        assert_eq!(operands.index(), Some(9));
        assert_eq!(operands.byte(), None);
    }
}
