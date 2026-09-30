//! Schema-attribute storage for one [`crate::ThreadSpanBuffer`] block, laid out
//! so a foreign writer can share it.
//!
//! Every attribute of a block lives in ONE fixed allocation that never moves
//! for the block's lifetime. A JavaScript lane holds TypedArray views over the
//! same bytes (a linear-memory view on Wasm, an external `ArrayBuffer` in a host
//! runtime that embeds the row store) and writes cells with plain stores; the
//! Rust writer and the Arrow converter read and write the same words. Nothing is
//! copied between the two languages: there is one copy of each value, and both
//! sides name it.
//!
//! # Layout (an ABI: the TypeScript lane computes the same offsets)
//!
//! The allocation is `field_count * stride(capacity)` little-endian `u64` words,
//! where `stride(capacity) = capacity + ceil(capacity / 64)`. Field `i` (the
//! schema attribute at ordinal `SYSTEM_COLUMN_COUNT + i`) owns words
//! `[i * stride, (i + 1) * stride)`:
//!
//! - words `[0, capacity)`: one value cell per row;
//! - words `[capacity, stride)`: the validity bitmap, bit `r` set when row `r`
//!   holds a value. Read as bytes, that is bit `r & 7` of byte `r >> 3` — the
//!   form a `Uint8Array` writer uses — because the words are little-endian.
//!
//! A cell's encoding follows the field's strategy: `number` is the `f64` bit
//! pattern, `uint64` the value, `boolean` 0 or 1, `category`/`text` the store's
//! 1-based intern ordinal, `enum` the variant index. The last three fit the low
//! 32 bits, so a 32-bit writer stores the low half of the word and leaves the
//! high half as the zero it was initialized to.
//!
//! # Why atomics
//!
//! The words are [`AtomicU64`] because memory another runtime writes is not
//! memory Rust may assume unchanged between its own accesses. Every access is
//! `Relaxed`, which compiles to a plain load or store on every target this
//! crate ships to; the type states the sharing rather than buying ordering.
//! Writers never run concurrently with each other or with a reader: the owner
//! of the row store sequences them.

use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(target_endian = "big")]
compile_error!("attribute cells are a little-endian shared layout");

/// Words one field occupies at `capacity` rows: the value cells plus the
/// validity bitmap.
#[inline]
#[must_use]
pub const fn stride(capacity: usize) -> usize {
    capacity + capacity.div_ceil(64)
}

/// One block's attribute cells.
#[derive(Debug)]
pub struct AttributeCells {
    words: Box<[AtomicU64]>,
    capacity: usize,
}

impl AttributeCells {
    /// Zeroed cells for `field_count` attributes at `capacity` rows.
    #[must_use]
    pub fn new(field_count: usize, capacity: usize) -> Self {
        let words = (0..field_count * stride(capacity))
            .map(|_| AtomicU64::new(0))
            .collect();
        Self { words, capacity }
    }

    #[inline]
    fn value_index(&self, field: usize, row: usize) -> usize {
        debug_assert!(row < self.capacity, "row outside the block");
        field * stride(self.capacity) + row
    }

    #[inline]
    fn validity(&self, field: usize, row: usize) -> (&AtomicU64, u64) {
        let index = field * stride(self.capacity) + self.capacity + row / 64;
        (&self.words[index], 1u64 << (row % 64))
    }

    /// Store `cell` at `row` and mark it valid. Two stores: the value, then the
    /// validity word.
    #[inline]
    pub fn set(&self, field: usize, row: usize, cell: u64) {
        self.words[self.value_index(field, row)].store(cell, Ordering::Relaxed);
        let (word, bit) = self.validity(field, row);
        word.store(word.load(Ordering::Relaxed) | bit, Ordering::Relaxed);
    }

    /// The cell at `row`, or `None` when no writer marked it valid.
    #[inline]
    #[must_use]
    pub fn get(&self, field: usize, row: usize) -> Option<u64> {
        let (word, bit) = self.validity(field, row);
        (word.load(Ordering::Relaxed) & bit != 0)
            .then(|| self.words[self.value_index(field, row)].load(Ordering::Relaxed))
    }

    /// Fill every row of `start..end` that holds no value with `cell`,
    /// returning how many were filled. Direct writes always win.
    pub fn fill_unset(&self, field: usize, start: usize, end: usize, cell: u64) -> usize {
        let mut filled = 0;
        for row in start..end {
            let (word, bit) = self.validity(field, row);
            let bits = word.load(Ordering::Relaxed);
            if bits & bit == 0 {
                self.words[self.value_index(field, row)].store(cell, Ordering::Relaxed);
                word.store(bits | bit, Ordering::Relaxed);
                filled += 1;
            }
        }
        filled
    }

    /// Copy every field's cell and validity from `from` in `source` to `to` in
    /// `self`. `source` may be `self`.
    pub fn copy_row(&self, to: usize, source: &Self, from: usize) {
        let fields = self.words.len() / stride(self.capacity);
        for field in 0..fields {
            let (from_word, from_bit) = source.validity(field, from);
            let (to_word, to_bit) = self.validity(field, to);
            let valid = from_word.load(Ordering::Relaxed) & from_bit != 0;
            let cell = source.words[source.value_index(field, from)].load(Ordering::Relaxed);
            self.words[self.value_index(field, to)].store(cell, Ordering::Relaxed);
            let bits = to_word.load(Ordering::Relaxed);
            to_word.store(
                if valid { bits | to_bit } else { bits & !to_bit },
                Ordering::Relaxed,
            );
        }
    }

    /// Mark rows `start..capacity` empty in every field. Value cells keep their
    /// bytes; validity is the only authority on whether a row holds a value.
    pub fn clear_from(&self, start: usize) {
        let fields = self.words.len() / stride(self.capacity);
        for field in 0..fields {
            for row in start..self.capacity {
                let (word, bit) = self.validity(field, row);
                word.store(word.load(Ordering::Relaxed) & !bit, Ordering::Relaxed);
            }
        }
    }

    /// Base address of the cells, for a foreign writer's view.
    #[must_use]
    pub fn as_ptr(&self) -> *mut u64 {
        // `AtomicU64` has the size and alignment of `u64`, and its interior
        // mutability is what makes writing through this pointer sound.
        self.words.as_ptr().cast::<u64>().cast_mut()
    }

    /// Size of the allocation in bytes.
    #[must_use]
    pub fn byte_len(&self) -> usize {
        self.words.len() * size_of::<u64>()
    }

    /// Whether the block carries any attribute storage at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.words.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_byte_writer_and_the_word_reader_agree_on_validity() {
        let cells = AttributeCells::new(2, 128);
        // A Uint8Array writer marks row 77 of field 1 valid and stores 9 in its
        // low 32 bits, exactly as the TypeScript lane does.
        let base = cells.as_ptr().cast::<u8>();
        let field = stride(128);
        // SAFETY: the offsets are inside the allocation this test just made.
        unsafe {
            *base.add((field + 128) * 8 + (77 >> 3)) |= 1 << (77 & 7);
            *base.cast::<u32>().add((field + 77) * 2) = 9;
        }
        assert_eq!(cells.get(1, 77), Some(9));
        assert_eq!(cells.get(1, 76), None);
        assert_eq!(cells.get(0, 77), None);
    }

    #[test]
    fn fill_leaves_direct_writes_alone_and_clear_empties_the_tail() {
        let cells = AttributeCells::new(1, 8);
        cells.set(0, 3, 30);
        assert_eq!(cells.fill_unset(0, 2, 6, 7), 3);
        assert_eq!(cells.get(0, 3), Some(30));
        assert_eq!(cells.get(0, 5), Some(7));
        cells.clear_from(4);
        assert_eq!(cells.get(0, 5), None);
        assert_eq!(cells.get(0, 3), Some(30));
    }

    #[test]
    fn a_copied_row_carries_absence_as_well_as_values() {
        let cells = AttributeCells::new(2, 8);
        cells.set(0, 6, 1);
        cells.set(1, 0, 2);
        cells.copy_row(0, &cells, 6);
        assert_eq!(cells.get(0, 0), Some(1));
        assert_eq!(cells.get(1, 0), None);
    }
}
