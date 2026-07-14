//! Little-endian raw-state access helpers shared by the VM-core modules.
//!
//! The Zig side reads and writes state bytes through pointer casts that are
//! valid because every multi-byte field in the state layout happens to be
//! naturally aligned. Rust references to underaligned data are UB, so this
//! crate never forms a reference into the state buffer at all: every access
//! is an explicit LE byte copy. That is byte-for-byte identical on the LE
//! targets the VM ships on, and also correct on BE hosts.
//!
//! The accessors are `#[inline(always)]` (they compile to a bounds check plus
//! one load/store, and the interpreter hot loops call them per event), so the
//! failure path must not be duplicated at every inline site: all of them
//! funnel out-of-bounds into the one `#[cold]` panic below instead of
//! carrying per-site `expect` strings. An OOB offset here is a programmer
//! bug (the layout math is the contract), so panicking is correct.

#[cold]
#[inline(never)]
fn oob() -> ! {
    columine_types::die!("state buffer access out of bounds")
}

macro_rules! le_accessors {
    ($read:ident, $write:ident, $ty:ty, $n:literal) => {
        #[inline(always)]
        pub fn $read(buf: &[u8], off: u32) -> $ty {
            let off = off as usize;
            match buf.get(off..).and_then(|s| s.first_chunk::<$n>()) {
                Some(&arr) => <$ty>::from_le_bytes(arr),
                None => oob(),
            }
        }

        #[inline(always)]
        pub fn $write(buf: &mut [u8], off: u32, value: $ty) {
            let off = off as usize;
            match buf.get_mut(off..).and_then(|s| s.first_chunk_mut::<$n>()) {
                Some(arr) => *arr = value.to_le_bytes(),
                None => oob(),
            }
        }
    };
}

le_accessors!(read_u16, write_u16, u16, 2);
le_accessors!(read_u32, write_u32, u32, 4);
le_accessors!(read_f32, write_f32, f32, 4);
le_accessors!(read_f64, write_f64, f64, 8);
le_accessors!(read_u64, write_u64, u64, 8);
le_accessors!(read_i64, write_i64, i64, 8);

/// Fill `count` consecutive u32 cells starting at `off` with `value`.
pub fn fill_u32(buf: &mut [u8], off: u32, count: u32, value: u32) {
    let bytes = value.to_le_bytes();
    let start = off as usize;
    for i in 0..count as usize {
        buf[start + i * 4..start + i * 4 + 4].copy_from_slice(&bytes);
    }
}

/// Fill `count` consecutive f64 cells starting at `off` with `value`.
pub fn fill_f64(buf: &mut [u8], off: u32, count: u32, value: f64) {
    let bytes = value.to_le_bytes();
    let start = off as usize;
    for i in 0..count as usize {
        buf[start + i * 8..start + i * 8 + 8].copy_from_slice(&bytes);
    }
}

/// Zero the byte range `[off, off + len)`.
#[inline(always)]
pub fn zero(buf: &mut [u8], off: u32, len: u32) {
    let start = off as usize;
    match buf.get_mut(start..start + len as usize) {
        Some(s) => s.fill(0),
        None => oob(),
    }
}

/// `memcpy` within one buffer is not needed by slice 1; cross-buffer copy is.
#[inline(always)]
pub fn copy(dst: &mut [u8], dst_off: u32, src: &[u8], src_off: u32, len: u32) {
    let d = dst_off as usize;
    let s = src_off as usize;
    let (Some(dst), Some(src)) = (
        dst.get_mut(d..d + len as usize),
        src.get(s..s + len as usize),
    ) else {
        oob()
    };
    dst.copy_from_slice(src);
}

/// Same-buffer copy (vm.zig `@memcpy` between two regions of the state; the
/// Zig call requires non-overlap and callers guarantee it — `copy_within`
/// handles overlap safely anyway).
#[inline(always)]
pub fn copy_within(buf: &mut [u8], src_off: u32, dst_off: u32, len: u32) {
    buf.copy_within(src_off as usize..(src_off + len) as usize, dst_off as usize);
}
