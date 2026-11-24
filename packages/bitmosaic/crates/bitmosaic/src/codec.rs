//! Low-level wire codecs, bit-select, and reciprocal arithmetic.
//!
//! Leaf module: depends only on [`crate::admission`], breaking the former
//! cycle between `ef`, `wire`, and `patch`.

use core::fmt;

use crate::admission::{AdmissionClass, AdmissionError};

/// Why a write into a fixed-capacity slot was refused. Nothing in the slot
/// is touched on any of these.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PatchError {
    /// The string needs `needed` bytes and the slot holds fewer. Grow the
    /// slot to at least `needed` and retry.
    Capacity { needed: usize },
    /// The slot does not begin with a well-formed native u32 image. Initialise
    /// it with `Bitmosaic::write_into_slice` of the set it should hold (the
    /// empty set included) before patching.
    Malformed,
    /// `adds` or `removes` is not strictly ascending. Sort and dedup the
    /// batch; the patch is one merge pass and needs the order.
    Unsorted,
}

impl fmt::Display for PatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PatchError::Capacity { needed } => write!(
                f,
                "the native image needs {needed} bytes and the slot holds fewer; grow the slot to at least {needed} bytes and retry"
            ),
            PatchError::Malformed => f.write_str(
                "the slot does not begin with a well-formed native u32 image; initialise it with Bitmosaic::write_into_slice before patching",
            ),
            PatchError::Unsorted => f.write_str(
                "adds and removes must each be strictly ascending; sort and dedup the batch before patching",
            ),
        }
    }
}

impl std::error::Error for PatchError {}

// ── unaligned little-endian reads ────────────────────────────────────────
//
// Mapped planes carry no alignment guarantee, so every scalar field is read
// through `from_le_bytes`. On aarch64 and x86-64 that compiles to one load.

#[inline(always)]
pub(crate) fn read_u16_at(bytes: &[u8], at: usize) -> u16 {
    let mut buf = [0u8; 2];
    buf.copy_from_slice(&bytes[at..at + 2]);
    u16::from_le_bytes(buf)
}

#[inline(always)]
pub(crate) fn read_u32_at(bytes: &[u8], at: usize) -> u32 {
    let mut buf = [0u8; 4];
    buf.copy_from_slice(&bytes[at..at + 4]);
    u32::from_le_bytes(buf)
}

#[inline(always)]
pub(crate) fn read_u64_at(bytes: &[u8], at: usize) -> u64 {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes[at..at + 8]);
    u64::from_le_bytes(buf)
}

#[inline(always)]
pub(crate) fn read_u24_at(bytes: &[u8], at: usize) -> u32 {
    u32::from(bytes[at]) | (u32::from(bytes[at + 1]) << 8) | (u32::from(bytes[at + 2]) << 16)
}

/// Minimum byte width `W(x)` in `1..=4` needed to encode `x` little-endian (§2).
#[inline(always)]
pub(crate) const fn width_of(x: u32) -> usize {
    1 + (x >= 256) as usize + (x >= 65_536) as usize + (x >= 16_777_216) as usize
}

/// Read a little-endian unsigned integer of `width` bytes (`1..=4`) without
/// reading past `bytes[at + width]`.
#[inline(always)]
pub(crate) fn read_uint_le(bytes: &[u8], at: usize, width: usize) -> u32 {
    match width {
        1 => u32::from(bytes[at]),
        2 => u32::from(read_u16_at(bytes, at)),
        3 => read_u24_at(bytes, at),
        4 => read_u32_at(bytes, at),
        _ => unreachable!("width must be in 1..=4"),
    }
}

/// Write the low `width` bytes (`1..=4`) of `value` in little-endian order.
#[inline(always)]
pub(crate) fn write_uint_le(out: &mut [u8], at: usize, value: u32, width: usize) {
    let le = value.to_le_bytes();
    out[at..at + width].copy_from_slice(&le[..width]);
}

// ── canonical ULEB128 ────────────────────────────────────────────────────

// Queries that inspect framing without certifying it retain nullable probe semantics;
// every parser uses the same canonical ULEB reader as full admission.
pub(crate) fn read_uleb(bytes: &[u8], at: &mut usize, max: u32) -> Option<u32> {
    read_uleb_checked(bytes, at, max, AdmissionClass::Count).ok()
}

pub(crate) fn read_uleb_checked(
    bytes: &[u8],
    at: &mut usize,
    max: u32,
    class: AdmissionClass,
) -> Result<u32, AdmissionError> {
    let start = *at;
    let mut value = 0u32;
    for index in 0..5 {
        let Some(&byte) = bytes.get(*at) else {
            return Err(AdmissionError::new(
                class,
                start,
                "unsigned LEB128 is truncated",
            ));
        };
        *at += 1;
        if index == 4 && byte > 0x0f {
            return Err(AdmissionError::new(
                class,
                start,
                "unsigned LEB128 overflows u32",
            ));
        }
        value |= u32::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {
            if value > max {
                return Err(AdmissionError::new(
                    class,
                    start,
                    "unsigned LEB128 exceeds its declared bound",
                ));
            }
            if index != 0 && byte == 0 {
                return Err(AdmissionError::new(
                    class,
                    start,
                    "unsigned LEB128 is not shortest form",
                ));
            }
            return Ok(value);
        }
    }
    Err(AdmissionError::new(
        class,
        start,
        "unsigned LEB128 has no terminator",
    ))
}

pub(crate) const fn uleb_len(value: u32) -> usize {
    ((32 - (value | 1).leading_zeros()) as usize).div_ceil(7)
}

pub(crate) fn write_uleb(out: &mut [u8], at: &mut usize, mut value: u32) {
    loop {
        let low = (value & 0x7f) as u8;
        value >>= 7;
        out[*at] = low | if value == 0 { 0 } else { 0x80 };
        *at += 1;
        if value == 0 {
            break;
        }
    }
}

// ── SWAR broadword select64 ──────────────────────────────────────────────

/// Zero-indexed bit position (`0..64`) of the `rank`-th set bit in `word`.
///
/// Precondition: `rank < word.count_ones()`. Branchless SWAR broadword
/// implementation: parallel 2/4/8-bit popcounts, byte-level prefix scan via
/// `trailing_zeros`, then three branchless halving steps (nibble, pair, bit)
/// using the already-computed sub-counts. Zero memory loads, O(1) latency.
#[inline(always)]
pub(crate) fn select64(word: u64, rank: u32) -> u32 {
    debug_assert!(rank < word.count_ones(), "select64 rank out of bounds");
    let s2 = word - ((word >> 1) & 0x5555_5555_5555_5555);
    let s4 = (s2 & 0x3333_3333_3333_3333) + ((s2 >> 2) & 0x3333_3333_3333_3333);
    let s8 = ((s4 + (s4 >> 4)) & 0x0F0F_0F0F_0F0F_0F0F).wrapping_mul(0x0101_0101_0101_0101);
    let threshold = (u64::from(rank) + 1).wrapping_mul(0x0101_0101_0101_0101);
    let gt = ((s8 | 0x8080_8080_8080_8080) - threshold) & 0x8080_8080_8080_8080;
    let mut pos = gt.trailing_zeros() & !7;
    let mut rem = rank - (((s8 << 8) >> pos) & 0xFF) as u32;

    let low4 = ((s4 >> pos) & 0xF) as u32;
    let go4 = u32::from(rem >= low4);
    rem -= low4 * go4;
    pos += 4 * go4;

    let low2 = ((s2 >> pos) & 0x3) as u32;
    let go2 = u32::from(rem >= low2);
    rem -= low2 * go2;
    pos += 2 * go2;

    let low1 = ((word >> pos) & 1) as u32;
    pos + u32::from(rem >= low1)
}

// ── Lemire reciprocal division (Stride & Cone) ───────────────────────────

/// Precomputed `ceil(2^32 / s)` for `s in 2..256` (fits in `u32`; `s <= 1`
/// has `magic = 1u64 << 32`). Eliminates hardware division even on single-shot
/// borrowed `CView::Stride` opens for all small strides.
const STRIDE_MAGIC_LUT: [u32; 256] = {
    let mut table = [0u32; 256];
    let mut s = 2usize;
    while s < 256 {
        table[s] = (1u64 << 32).div_ceil(s as u64) as u32;
        s += 1;
    }
    table
};

/// `ceil(2^32 / stride.max(1))`: exact 32-bit reciprocal for `u16` deltas.
///
/// With `delta < 2^16`, the accumulated reciprocal error
/// `delta * ((-2^32) mod stride)` is at most `65535 * 65534 < 2^32`, so
/// `(delta * magic) >> 32 == delta / stride` holds identically.
#[inline(always)]
pub(crate) fn stride_magic(stride: u16) -> u64 {
    if stride <= 1 {
        1u64 << 32
    } else if (stride as usize) < STRIDE_MAGIC_LUT.len() {
        u64::from(STRIDE_MAGIC_LUT[stride as usize])
    } else {
        (1u64 << 32).div_ceil(u64::from(stride))
    }
}

/// `floor(delta / stride)` from a reciprocal produced by [`stride_magic`].
#[inline(always)]
pub(crate) fn stride_quotient(delta: u32, magic: u64) -> u32 {
    ((u64::from(delta) * magic) >> 32) as u32
}

/// Initial 15-bit lower-bound reciprocal seed for [`cone_inv_scale`]:
/// `LUT[idx] = floor(2^21 / (65 + idx))`, guaranteeing `m32_hi * (LUT[idx] << 16) <= 2^62`.
const CONE_RECIP_LUT: [u16; 64] = {
    let mut table = [0u16; 64];
    let mut i = 0usize;
    while i < 64 {
        table[i] = ((1u64 << 21) / (65 + i as u64)) as u16;
        i += 1;
    }
    table
};

/// Division-free lower-bound approximation of `floor(2^64 / scale)` for
/// `scale >= 1u64 << 32`, or `0` sentinel for smaller scales (which cannot
/// occur on a valid ascending Cone).
///
/// Uses a 64-entry LUT seed plus two `u64` Newton-Raphson iterations (~23 bits
/// of accuracy, strictly below `2^64 / scale`), so [`cone_guess`] recovers the
/// exact quotient `((delta as u64) << 32) / scale` with one `u64` multiply and
/// one verify compare — zero hardware divisions and zero 128-bit arithmetic.
#[inline(always)]
pub(crate) fn cone_inv_scale(scale: u64) -> u64 {
    if scale < (1u64 << 32) {
        return 0;
    }
    let lz = scale.leading_zeros();
    let m = scale << lz;
    let m32_hi = (m >> 32) + 1;
    let idx = ((m >> 57) & 63) as usize;
    let x0 = u64::from(CONE_RECIP_LUT[idx]) << 16;
    let err1 = (1u64 << 62) - m32_hi * x0;
    let x1 = x0 + ((x0 * (err1 >> 31)) >> 31);
    let err2 = (1u64 << 62) - m32_hi * x1;
    let x2 = x1 + ((x1 * (err2 >> 25)) >> 37);
    if lz == 31 { x2 << 1 } else { x2 >> (30 - lz) }
}

/// Exact `floor((u64::from(delta) << 32) / scale)` (or `0` when `scale == 0`)
/// using the reciprocal from [`cone_inv_scale`].
#[inline(always)]
pub(crate) fn cone_guess(delta: u16, scale: u64, inv_scale: u64) -> u64 {
    if inv_scale != 0 {
        let d = u64::from(delta);
        let q0 = (d * inv_scale) >> 32;
        q0 + u64::from((q0 + 1) * scale <= (d << 32))
    } else {
        (u64::from(delta) << 32).checked_div(scale).unwrap_or(0)
    }
}

/// Branchless counted bisection over an ascending cone window `lo..hi`:
/// returns the first index in `lo..=hi` whose value is `>= v`.
#[inline(always)]
pub(crate) fn cone_window_rank(lo: usize, hi: usize, v: u16, at: impl Fn(usize) -> u16) -> usize {
    let mut base = lo;
    let mut len = hi.saturating_sub(lo);
    while len > 4 {
        let half = len / 2;
        base = if at(base + half - 1) < v {
            base + half
        } else {
            base
        };
        len -= half;
    }
    let mut count = 0usize;
    for i in base..base + len {
        count += usize::from(at(i) < v);
    }
    base + count
}

#[cfg(test)]
mod tests {
    use super::{
        cone_guess, cone_inv_scale, cone_window_rank, select64, stride_magic, stride_quotient,
    };

    fn naive_select64(mut word: u64, rank: u32) -> u32 {
        for _ in 0..rank {
            word &= word - 1;
        }
        word.trailing_zeros()
    }

    fn xorshift(state: &mut u64) -> u64 {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        *state
    }

    #[test]
    fn select64_matches_naive_exhaustively_on_edges_and_random() {
        // Single-bit words.
        for bit in 0..64u32 {
            assert_eq!(select64(1u64 << bit, 0), bit);
        }
        // All-ones word, every rank.
        for rank in 0..64u32 {
            assert_eq!(select64(u64::MAX, rank), rank);
        }
        // Every 8-bit pattern in every byte lane.
        for lane in 0..8u32 {
            for byte in 1..=255u64 {
                let word = byte << (lane * 8);
                for rank in 0..word.count_ones() {
                    assert_eq!(
                        select64(word, rank),
                        naive_select64(word, rank),
                        "word={word:#018x} rank={rank}"
                    );
                }
            }
        }
        // Random words across all popcounts.
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        for _ in 0..4_096 {
            let word = xorshift(&mut state);
            let ones = word.count_ones();
            for rank in 0..ones {
                assert_eq!(
                    select64(word, rank),
                    naive_select64(word, rank),
                    "word={word:#018x} rank={rank}"
                );
            }
        }
    }

    #[test]
    fn stride_quotient_matches_division_across_domain() {
        let strides: Vec<u16> = (1..=300u16)
            .chain([511, 512, 513, 1024, 4096, 32767, 32768, 65534, 65535])
            .collect();
        for stride in strides {
            let magic = stride_magic(stride);
            for delta in 0..=65535u32 {
                assert_eq!(
                    stride_quotient(delta, magic),
                    delta / u32::from(stride),
                    "stride={stride} delta={delta}"
                );
            }
        }
    }

    #[test]
    fn cone_guess_matches_exact_division() {
        let mut scales: Vec<u64> = vec![
            0,
            1,
            (1u64 << 32) - 1,
            1u64 << 32,
            (1u64 << 32) + 1,
            (1u64 << 33) - 1,
            1u64 << 33,
            (1u64 << 48) - 1,
            1u64 << 48,
            (65535u64 << 32),
            1u64 << 63,
            u64::MAX - 1,
            u64::MAX,
        ];
        // Real cone_fit scales: (span << 32) / (n - 1).
        for n in [2u64, 3, 7, 16, 64, 255, 512, 1024] {
            for span in [n - 1, n, n * 3, 32_768, 65_535] {
                if span <= 65_535 {
                    scales.push((span << 32) / (n - 1));
                }
            }
        }
        let mut state = 0xC0DE_5CA1_E123_4567u64;
        for _ in 0..512 {
            let r = xorshift(&mut state);
            let s = (1u64 << 32) | (r >> (r & 31));
            scales.push(s);
        }
        let deltas: Vec<u16> = (0..=256u16)
            .chain([
                511, 512, 1023, 1024, 4096, 16384, 32767, 32768, 65534, 65535,
            ])
            .collect();
        for scale in scales {
            let inv = cone_inv_scale(scale);
            if scale >= (1u64 << 32) {
                assert_ne!(
                    inv, 0,
                    "valid cone scale {scale:#x} must produce nonzero inv"
                );
            }
            for &d in &deltas {
                let want = (u64::from(d) << 32).checked_div(scale).unwrap_or(0);
                assert_eq!(
                    cone_guess(d, scale, inv),
                    want,
                    "scale={scale:#x} inv={inv:#x} delta={d}"
                );
            }
        }
    }

    #[test]
    fn cone_window_rank_matches_partition_point() {
        let mut state = 0xBEEF_CAFE_0123_4567u64;
        for len in 0..=140usize {
            let mut vals: Vec<u16> = Vec::with_capacity(len);
            let mut cur = 10u16;
            for _ in 0..len {
                vals.push(cur);
                cur = cur.saturating_add(1 + (xorshift(&mut state) % 13) as u16);
            }
            let offset = 7usize;
            let mut padded = vec![0u16; offset];
            padded.extend_from_slice(&vals);
            padded.extend_from_slice(&[u16::MAX; 5]);
            for probe in vals
                .iter()
                .flat_map(|&m| [m.saturating_sub(1), m, m.saturating_add(1)])
                .chain([0, u16::MAX])
            {
                let got = cone_window_rank(offset, offset + len, probe, |i| padded[i]);
                let want = offset + vals.partition_point(|&m| m < probe);
                assert_eq!(got, want, "len={len} probe={probe}");
            }
        }
    }
}
