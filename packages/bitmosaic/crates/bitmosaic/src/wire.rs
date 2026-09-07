//! Canonical byte string for a frozen [`Bitmosaic`], and the borrowed view that
//! reads it without decoding.
//!
//! The owned forest is a build-time shape: three `Box`ed planes plus a
//! `Box<[u64; 1024]>` per dense chunk. Nothing frozen can be reached from a
//! mapped page through it, so a reader would have to materialise one
//! instance per key. This module is the other half — one contiguous string
//! a segment can hold, and [`BitmosaicView`] over it, which owns zero bytes and
//! allocates on no read path.
//!
//! Two things are decided HERE rather than in the ladder, because both are
//! properties of the byte string and not of the in-memory shape:
//!
//! - **The root arm.** A forest pays a 10-byte directory record per occupied
//!   chunk. On sparse data that record is the whole cost: `u13m/n100` puts
//!   100 values in 81 chunks, so 810 of 1,064 bytes are directory. The
//!   encoder prices an Elias-Fano plane over the same set in closed form and
//!   publishes it only when it is strictly smaller — the same discipline the
//!   per-chunk ladder already applies, one level up.
//! - **Span-tight `Words`.** The owned container is a whole 8 KB chunk
//!   because staging mutates it. Frozen, only the occupied word window is
//!   worth writing, and an intersection then touches only the overlap of two
//!   windows instead of 16 KB.
//!
//! Cardinality is NOT stored per container. The directory's exclusive-prefix
//! plane already answers it by subtraction, so `Stride` costs 4 payload
//! bytes, `Cone` 11 + n, and `Array` exactly 2n.

use crate::ef::{self, EfLayout, EfRange, EfView};
use crate::patch::PatchError;
use crate::{
    BLOCK_WORDS, Bitmosaic, Bitmosaic64, CHUNK_WORDS, Container, DIR_STRIDE, DRIVE_RATIO_KEYS,
    DRIVE_RATIO_MEMBERS, Range, SUMMARY_WORDS, charge_key_positions, charge_linear,
    drive_from_small, gallop_drive, merge_and_array_bytes, select_group, stride_and_closed,
};

/// Bytes identifying a native bitmap image.
pub const IMAGE_ID_LEN: usize = 4;
pub const EMPTY_U32_IMAGE: [u8; IMAGE_ID_LEN] = *b"BMS\x0c";
pub const EMPTY_U64_IMAGE: [u8; IMAGE_ID_LEN] = *b"BMS\x0d";
const FORMAT_VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KeyWidth {
    U32,
    U64,
}

/// Bounded framing proof; body topology is checked by the corresponding view.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ImageHeader {
    pub width: KeyWidth,
    pub encoded_len: usize,
    root: u8,
    body_offset: usize,
}

/// Check identification and canonical length framing without scanning the body.
pub fn image_header(bytes: &[u8]) -> Option<ImageHeader> {
    let id = bytes.get(..IMAGE_ID_LEN)?;
    if &id[..3] != b"BMS" || id[3] >> 3 != FORMAT_VERSION {
        return None;
    }
    let mode = id[3] & 7;
    if matches!(mode, 3 | 6 | 7) {
        return None;
    }
    let width = if mode & 1 == 0 {
        KeyWidth::U32
    } else {
        KeyWidth::U64
    };
    let mut body_offset = IMAGE_ID_LEN;
    let encoded_len = if mode & 4 != 0 {
        IMAGE_ID_LEN
    } else {
        let body_bytes = read_uleb(bytes, &mut body_offset, u32::MAX)? as usize;
        let total = body_offset.checked_add(body_bytes)?;
        if total > u32::MAX as usize || total > bytes.len() {
            return None;
        }
        total
    };
    Some(ImageHeader {
        width,
        encoded_len,
        root: if mode & 2 == 0 { ROOT_FOREST } else { ROOT_EF },
        body_offset,
    })
}

pub(crate) fn read_uleb(bytes: &[u8], at: &mut usize, max: u32) -> Option<u32> {
    let mut value = 0u32;
    for index in 0..5 {
        let byte = *bytes.get(*at)?;
        *at += 1;
        if index == 4 && byte > 0x0f {
            return None;
        }
        value |= u32::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {
            return (value <= max && (index == 0 || byte != 0)).then_some(value);
        }
    }
    None
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

pub(crate) fn frame_len(body_bytes: usize) -> usize {
    let body = u32::try_from(body_bytes).expect("native body exceeds u32 offset domain");
    let total = IMAGE_ID_LEN + uleb_len(body) + body_bytes;
    assert!(
        total <= u32::MAX as usize,
        "native image exceeds u32 offset domain"
    );
    total
}

pub(crate) fn write_frame(out: &mut [u8], width: KeyWidth, root: u8, body_bytes: usize) -> usize {
    out[..3].copy_from_slice(b"BMS");
    out[3] =
        (FORMAT_VERSION << 3) | ((root == ROOT_EF) as u8) << 1 | (width == KeyWidth::U64) as u8;
    let mut at = IMAGE_ID_LEN;
    write_uleb(
        out,
        &mut at,
        u32::try_from(body_bytes).expect("native body exceeds u32"),
    );
    at
}

pub(crate) fn write_kind(bytes: &mut [u8], start: usize, index: usize, kind: u32) {
    let bit = index * 3;
    let at = start + bit / 8;
    let shift = bit % 8;
    bytes[at] = (bytes[at] & !(7u8 << shift)) | ((kind as u8) << shift);
    if shift > 5 {
        let width = shift - 5;
        bytes[at + 1] = (bytes[at + 1] & !((1u8 << width) - 1)) | ((kind as u8) >> (8 - shift));
    }
}

fn read_kind(bytes: &[u8], start: usize, index: usize) -> u32 {
    let bit = index * 3;
    let at = start + bit / 8;
    let shift = bit % 8;
    let mut value = u32::from(bytes[at]) >> shift;
    if shift > 5 {
        value |= u32::from(bytes[at + 1]) << (8 - shift);
    }
    value & 7
}

pub(crate) const ROOT_FOREST: u8 = 0;
pub(crate) const ROOT_EF: u8 = 1;

pub(crate) const K_RUNS: u32 = 4;
pub(crate) const K_WORDS: u32 = 0;
pub(crate) const K_STRIDE: u32 = 1;
pub(crate) const K_CONE: u32 = 2;
pub(crate) const K_ARRAY: u32 = 3;

/// Below this the branchless key walk is two cache lines and beats any
/// hashed probe; above it, binary search starts paying log2(C) dependent
/// loads and the tagged slot plane replaces them with one.
const SLOT_MIN_CHUNKS: u32 = 64;

// ── unaligned little-endian reads ────────────────────────────────────────
//
// Mapped planes carry no alignment guarantee, so every scalar field is read
// through `from_le_bytes`. On aarch64 and x86-64 that is one load; the kernels
// that need real alignment ask for it explicitly via `align_to` and fall back
// to scalar when the mapping does not provide it.

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

// ── geometry ─────────────────────────────────────────────────────────────

/// Directory plane offsets for a forest of `chunks` chunks. Derived, never
/// stored: five offsets in the header would cost 20 bytes to say what
/// `chunks` already says.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Geometry {
    pub(crate) keys_at: usize,
    pub(crate) before_at: usize,
    pub(crate) desc_at: usize,
    pub(crate) kinds_at: usize,
    pub(crate) slot_at: usize,
    pub(crate) slot_bits: u8,
    pub(crate) payload_at: usize,
}

impl Geometry {
    pub(crate) fn new(chunks: u32, slot_bits: u8, keys_at: usize) -> Geometry {
        let c = chunks as usize;
        let before_at = keys_at + 2 * c;
        let desc_at = before_at + 4 * c.saturating_sub(1);
        let kinds_at = desc_at + 4 * c.saturating_sub(1);
        let slot_at = kinds_at + (3 * c).div_ceil(8);
        let slot_bytes = if slot_bits == 0 {
            0
        } else {
            8usize << slot_bits
        };
        Geometry {
            keys_at,
            kinds_at,
            before_at,
            desc_at,
            slot_at,
            slot_bits,
            payload_at: slot_at + slot_bytes,
        }
    }
}

/// Slot-plane size for a key set, or 0 when it would buy nothing.
///
/// Two refusals, and both are the same principle: never spend bytes on a
/// structure the arithmetic already answers.
///
/// - **Small forests.** 64 keys are two cache lines; the branchless walk
///   beats any hashed probe and costs no bytes at all.
/// - **Contiguous keys.** `locate`'s predict-verify guess is EXACT for a
///   forest spanning an unbroken key range, so the slot plane is never
///   consulted. Measured: `stride1000/n20000` occupies 306 contiguous
///   chunks, where an unconditional plane was 4,096 of 8,416 wire bytes —
///   half the string, to accelerate a probe that already resolves in one
///   load.
fn slot_bits_for(keys: &[u16]) -> u8 {
    match keys {
        [] => 0,
        [first, .., last] => slot_bits_for_span(keys.len() as u32, *first, *last),
        [_] => slot_bits_for_span(1, keys[0], keys[0]),
    }
}

/// The closed form behind [`slot_bits_for`], over the three numbers it
/// actually depends on. The reader recomputes it from the directory it has
/// already validated and REQUIRES header equality: the slot-plane width is
/// derived, never trusted, so a forged header byte can neither overflow the
/// geometry shift nor point probes at payload bytes.
pub(crate) fn slot_bits_for_span(chunks: u32, first: u16, last: u16) -> u8 {
    let n = chunks as usize;
    if n <= SLOT_MIN_CHUNKS as usize {
        return 0;
    }
    // Keys are strictly ascending, so an exact end-to-end span proves the
    // run is unbroken.
    if usize::from(last - first) == n - 1 {
        return 0;
    }
    (n * 8 / 7 + 1).next_power_of_two().trailing_zeros() as u8
}

/// Fibonacci-hashed slot for a chunk key.
///
/// CORRECTNESS INVARIANT, and it is the reason there is no verify step: a
/// slot packs the FULL 16-bit chunk key into a 32-bit tag field, so a tag
/// match is key equality, not a fingerprint agreement. `slot >> 32 == key`
/// therefore decides membership outright — one load on a hit, one load on a
/// miss, zero dependent loads either way.
///
/// This holds only because the key domain is 16 bits and the tag field is
/// 32. A tag NARROWER than its key domain collides, and then the tag is an
/// accelerator in front of a verifying load and never a replacement for
/// one. Widen the key, or narrow the tag, and the verify must come back.
#[inline(always)]
fn slot_of(key: u16, slot_bits: u8) -> usize {
    ((u64::from(key).wrapping_mul(0x9E37_79B9_7F4A_7C15)) >> (64 - slot_bits)) as usize
}

// ── span-tight Words geometry ────────────────────────────────────────────

/// Occupied word window of a staged chunk, snapped out to whole 8-word
/// summary blocks so block indices stay exact in absolute chunk coordinates
/// and the AND steering needs no remapping — plus the EXACT first and last
/// member, which the same scan already has in hand.
///
/// The window is block-snapped and therefore loose by up to 511 positions at
/// each end; `min`/`max` are tight to the bit. That difference is the whole
/// value of the refuter: a seek landing in the snapped slack is decided by a
/// compare instead of a directory walk.
///
/// An all-zero chunk cannot be published (cardinality comes from the
/// directory and a member always exists), but if one ever reached here the
/// bounds widen to the full domain rather than collapse to `0..=0` — a
/// refuter that fails open costs a walk, one that fails closed loses members.
pub(crate) fn word_window(words: &[u64; CHUNK_WORDS]) -> (usize, usize, u16, u16) {
    let (Some(first), Some(last)) = (
        words.iter().position(|w| *w != 0),
        words.iter().rposition(|w| *w != 0),
    ) else {
        return (0, BLOCK_WORDS, 0, u16::MAX);
    };
    (
        first / BLOCK_WORDS * BLOCK_WORDS,
        (last / BLOCK_WORDS + 1) * BLOCK_WORDS,
        (first * 64) as u16 + words[first].trailing_zeros() as u16,
        (last * 64) as u16 + 63 - words[last].leading_zeros() as u16,
    )
}

/// Payload bytes of a `Words` chunk from its window: the 24-byte head, the
/// window's words, and one `u16` directory entry per `DIR_STRIDE` words.
#[inline]
pub(crate) fn words_payload_len(window_words: usize) -> usize {
    WORDS_AT + window_words * 8 + (window_words / DIR_STRIDE - 1) * 2
}

fn payload_len(container: &Container) -> usize {
    match container {
        Container::Stride { .. } => 4,
        Container::Cone { residuals, .. } => 11 + residuals.len(),
        Container::Array(values) => 2 * values.len(),
        Container::Runs { runs, .. } => runs_payload_len(runs.len()),
        Container::Words { words, .. } => {
            let (lo, hi, _, _) = word_window(words);
            words_payload_len(hi - lo)
        }
    }
}

// ── payload emitters ─────────────────────────────────────────────────────
//
// One emitter per kind, over the primitive parts rather than the owned
// `Container`, so the seal-time writer and the in-place patch produce the
// same bytes from the same code. Each writes exactly its payload length at
// `out[0..]` and derives nothing from anything but its arguments.

#[inline]
pub(crate) fn emit_stride(out: &mut [u8], first: u16, stride: u16) {
    out[..2].copy_from_slice(&first.to_le_bytes());
    out[2..4].copy_from_slice(&stride.to_le_bytes());
}

#[inline]
pub(crate) fn emit_cone(out: &mut [u8], first: u16, scale: u64, eps: u8, residuals: &[i8]) {
    out[..2].copy_from_slice(&first.to_le_bytes());
    out[2] = eps;
    out[3..11].copy_from_slice(&scale.to_le_bytes());
    for (slot, r) in out[11..11 + residuals.len()].iter_mut().zip(residuals) {
        *slot = *r as u8;
    }
}

pub(crate) fn runs_payload_len(count: usize) -> usize {
    uleb_len((count - 1) as u32) + 4 * count + 2 * count.saturating_sub(2)
}

pub(crate) fn emit_runs(out: &mut [u8], runs: &[crate::Run]) {
    let mut at = 0;
    write_uleb(out, &mut at, (runs.len() - 1) as u32);
    for run in runs {
        out[at..at + 2].copy_from_slice(&run.start.to_le_bytes());
        out[at + 2..at + 4].copy_from_slice(&run.end.to_le_bytes());
        at += 4;
    }
    for run in runs.iter().skip(1).take(runs.len().saturating_sub(2)) {
        out[at..at + 2].copy_from_slice(&run.before.to_le_bytes());
        at += 2;
    }
}

#[inline]
pub(crate) fn emit_array(out: &mut [u8], values: &[u16]) {
    for (slot, v) in out.as_chunks_mut::<2>().0.iter_mut().zip(values) {
        *slot = v.to_le_bytes();
    }
}

/// The `Words` payload from the plane alone: block summary, window, the
/// min/max refuter, the window's words, and the rank directory over them.
/// Writes exactly [`words_payload_len`] of the plane's window.
pub(crate) fn emit_words(out: &mut [u8], words: &[u64; CHUNK_WORDS]) {
    let (lo, hi, min, max) = word_window(words);
    let mut summary = [0u64; SUMMARY_WORDS];
    for (w, word) in words.iter().enumerate() {
        if *word != 0 {
            let block = w / BLOCK_WORDS;
            summary[block / 64] |= 1u64 << (block % 64);
        }
    }
    for (i, s) in summary.iter().enumerate() {
        out[8 * i..8 * i + 8].copy_from_slice(&s.to_le_bytes());
    }
    out[16..18].copy_from_slice(&(lo as u16).to_le_bytes());
    out[18..20].copy_from_slice(&((hi - lo) as u16).to_le_bytes());
    // The refuter, in the 4 bytes that used to be alignment pad: exact min
    // and max member, derived from this chunk's own words during the scan
    // that already located the window. Content-only, so byte-determinism is
    // unaffected (E7).
    out[BOUNDS_AT..BOUNDS_AT + 2].copy_from_slice(&min.to_le_bytes());
    out[BOUNDS_AT + 2..BOUNDS_AT + 4].copy_from_slice(&max.to_le_bytes());
    let wc = hi - lo;
    let (plane, dir) = out[WORDS_AT..words_payload_len(wc)].split_at_mut(wc * 8);
    for (slot, w) in plane.as_chunks_mut::<8>().0.iter_mut().zip(&words[lo..hi]) {
        *slot = w.to_le_bytes();
    }
    // Directory rebuilt over the window: entry g is the popcount of the
    // window's words before group g, so `rank` inside the window is one
    // load plus at most DIR_STRIDE - 1 popcounts.
    let mut running = words[lo..lo + DIR_STRIDE]
        .iter()
        .map(|word| word.count_ones())
        .sum::<u32>();
    for (slot, group) in dir
        .as_chunks_mut::<2>()
        .0
        .iter_mut()
        .zip(words[lo + DIR_STRIDE..hi].as_chunks::<DIR_STRIDE>().0)
    {
        *slot = (running as u16).to_le_bytes();
        running += group.iter().map(|x| x.count_ones()).sum::<u32>();
    }
}

fn write_payload(container: &Container, out: &mut [u8]) {
    match container {
        Container::Runs { runs, .. } => emit_runs(out, runs),
        Container::Stride { first, stride, .. } => emit_stride(out, *first, *stride),
        Container::Cone {
            first,
            scale,
            eps,
            residuals,
        } => emit_cone(out, *first, *scale, *eps, residuals),
        Container::Array(values) => emit_array(out, values),
        Container::Words { words, .. } => emit_words(out, words),
    }
}

fn kind_of(container: &Container) -> u32 {
    match container {
        Container::Words { .. } => K_WORDS,
        Container::Stride { .. } => K_STRIDE,
        Container::Cone { .. } => K_CONE,
        Container::Runs { .. } => K_RUNS,
        Container::Array(_) => K_ARRAY,
    }
}

// ── directory and header writers ─────────────────────────────────────────

/// One chunk's directory record: the key, the cardinality, the kind tag and
/// the payload offset the writer assigned it.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ChunkRow {
    pub(crate) key: u16,
    pub(crate) card: u32,
    pub(crate) kind: u32,
    pub(crate) at: usize,
}

/// Write the key, exclusive-prefix, descriptor and (when the geometry has
/// one) slot planes of a forest over `plane`, whose first `payload_at`
/// bytes past the header are ZERO on entry — the slot plane's holes are its
/// empty slots. Returns the cardinality total, which is the header's `len`.
pub(crate) fn write_directory(
    plane: &mut [u8],
    geometry: Geometry,
    rows: impl Iterator<Item = ChunkRow>,
) -> u64 {
    let slot_bits = geometry.slot_bits;
    let mask = if slot_bits == 0 {
        0
    } else {
        (1usize << slot_bits) - 1
    };
    let mut before = 0u64;
    for (i, row) in rows.enumerate() {
        plane[geometry.keys_at + 2 * i..][..2].copy_from_slice(&row.key.to_le_bytes());
        if i != 0 {
            plane[geometry.before_at + 4 * (i - 1)..][..4]
                .copy_from_slice(&(before as u32).to_le_bytes());
            plane[geometry.desc_at + 4 * (i - 1)..][..4]
                .copy_from_slice(&((row.at - geometry.payload_at) as u32).to_le_bytes());
        }
        write_kind(plane, geometry.kinds_at, i, row.kind);
        if slot_bits != 0 {
            let mut s = slot_of(row.key, slot_bits);
            while read_u64_at(plane, geometry.slot_at + s * 8) != 0 {
                s = (s + 1) & mask;
            }
            plane[geometry.slot_at + s * 8..][..8]
                .copy_from_slice(&((u64::from(row.key) << 32) | (i as u64 + 1)).to_le_bytes());
        }
        before += u64::from(row.card);
    }
    before
}

pub(crate) fn forest_header_len(len: u64, chunks: u32) -> usize {
    uleb_len((len - 1) as u32) + uleb_len(chunks - 1)
}

pub(crate) fn write_forest_header(
    head: &mut [u8],
    slot_bits: u8,
    len: u64,
    chunks: u32,
    body_bytes: usize,
) -> Geometry {
    if len == 0 {
        head[..IMAGE_ID_LEN].copy_from_slice(&EMPTY_U32_IMAGE);
        return Geometry::new(0, 0, IMAGE_ID_LEN);
    }
    let mut at = write_frame(head, KeyWidth::U32, ROOT_FOREST, body_bytes);
    write_uleb(head, &mut at, (len - 1) as u32);
    write_uleb(head, &mut at, chunks - 1);
    Geometry::new(chunks, slot_bits, at)
}

pub(crate) fn ef_header_len(len: u64, base: u32, span: u32) -> usize {
    uleb_len((len - 1) as u32) + uleb_len(base) + uleb_len(span)
}

pub(crate) fn write_ef_header(
    head: &mut [u8],
    len: u64,
    body_bytes: usize,
    base: u32,
    span: u32,
) -> usize {
    let mut at = write_frame(head, KeyWidth::U32, ROOT_EF, body_bytes);
    write_uleb(head, &mut at, (len - 1) as u32);
    write_uleb(head, &mut at, base);
    write_uleb(head, &mut at, span);
    at
}

// ── encode ───────────────────────────────────────────────────────────────

/// The root the ladder chose for a set and the exact byte length of the
/// string it publishes — decided once, then shared by the length query and
/// every writer.
#[derive(Clone, Copy, Debug)]
enum Plan {
    Forest {
        body_bytes: usize,
    },
    Ef {
        layout: EfLayout,
        base: u32,
        span: u32,
        body_bytes: usize,
    },
}

impl Plan {
    fn body_bytes(self) -> usize {
        match self {
            Self::Forest { body_bytes } | Self::Ef { body_bytes, .. } => body_bytes,
        }
    }

    fn root(self) -> u8 {
        match self {
            Self::Forest { .. } => ROOT_FOREST,
            Self::Ef { .. } => ROOT_EF,
        }
    }

    fn total(self) -> usize {
        if self.body_bytes() == 0 {
            IMAGE_ID_LEN
        } else {
            frame_len(self.body_bytes())
        }
    }
}

impl Bitmosaic {
    /// Canonical bytes for this set: the string [`BitmosaicView::open`] reads.
    ///
    /// Runs at freeze time, so it allocates the output buffer; nothing it
    /// produces allocates again when read.
    pub fn to_bytes(&self) -> Vec<u8> {
        let plan = self.plan();
        let mut out = vec![0u8; plan.total()];
        self.write_plan(plan, &mut out);
        out
    }

    /// Append the canonical bytes to `out` — for building a segment plane in
    /// one buffer rather than one allocation per key.
    pub fn write_into(&self, out: &mut Vec<u8>) {
        let plan = self.plan();
        let start = out.len();
        out.resize(start + plan.total(), 0);
        self.write_plan(plan, &mut out[start..]);
    }

    /// Write the canonical bytes at `out[0..]`, returning how many were
    /// written — [`Bitmosaic::serialized_len`] exactly. For a fixed-capacity
    /// slot: a freshly frozen set lands without a `Vec` between it and the
    /// bytes that hold it. Refuses, touching nothing, when `out` is shorter
    /// than the string; the caller grows the slot and retries.
    pub fn write_into_slice(&self, out: &mut [u8]) -> Result<usize, PatchError> {
        let plan = self.plan();
        let needed = plan.total();
        let Some(out) = out.get_mut(..needed) else {
            return Err(PatchError::Capacity { needed });
        };
        self.write_plan(plan, out);
        Ok(needed)
    }

    /// Exact length of [`Bitmosaic::to_bytes`] without producing it.
    pub fn serialized_len(&self) -> usize {
        self.plan().total()
    }

    /// The mutable form: the forest root whatever the Elias-Fano arm would
    /// cost. A slot that is patched keeps the root it starts with, and a
    /// forest is the root whose patches touch only the chunks a batch
    /// names, so a slot starts as one — see [`crate::patch`]. Same
    /// contract as [`Bitmosaic::write_into_slice`] otherwise.
    pub fn write_forest_into_slice(&self, out: &mut [u8]) -> Result<usize, PatchError> {
        let needed = self.forest_bytes();
        let Some(out) = out.get_mut(..needed) else {
            return Err(PatchError::Capacity { needed });
        };
        self.write_forest(out);
        Ok(needed)
    }

    /// Exact length of the forest form ([`Bitmosaic::write_forest_into_slice`]).
    pub fn forest_len(&self) -> usize {
        self.forest_bytes()
    }

    /// The forest form as bytes — the oracle side of the mutable contract.
    pub fn to_forest_bytes(&self) -> Vec<u8> {
        let mut out = vec![0u8; self.forest_bytes()];
        self.write_forest(&mut out);
        out
    }

    /// The standalone frame and u64 children share this exact body writer.
    fn write_plan(&self, plan: Plan, out: &mut [u8]) {
        if self.is_empty() {
            out[..IMAGE_ID_LEN].copy_from_slice(&EMPTY_U32_IMAGE);
            return;
        }
        let at = write_frame(out, KeyWidth::U32, plan.root(), plan.body_bytes());
        self.write_body(plan, &mut out[at..]);
    }

    fn forest_body_bytes(&self) -> usize {
        if self.is_empty() {
            return 0;
        }
        let geometry = Geometry::new(
            self.chunk_count(),
            slot_bits_for(self.keys()),
            forest_header_len(self.len(), self.chunk_count()),
        );
        self.containers()
            .iter()
            .fold(geometry.payload_at, |at, c| at + payload_len(c))
    }

    fn forest_bytes(&self) -> usize {
        Plan::Forest {
            body_bytes: self.forest_body_bytes(),
        }
        .total()
    }

    /// EF wins only on strictly smaller complete framed size.
    fn plan(&self) -> Plan {
        let forest = Plan::Forest {
            body_bytes: self.forest_body_bytes(),
        };
        let n = self.len();
        if n >= 2
            && let (Some(base), Some(last)) = (self.select(0), self.select(n - 1))
            && let Some(layout) = EfLayout::new(n, u64::from(last - base))
        {
            let span = last - base;
            let candidate = Plan::Ef {
                layout,
                base,
                span,
                body_bytes: ef_header_len(n, base, span) + layout.payload_bytes(),
            };
            if candidate.total() < forest.total() {
                return candidate;
            }
        }
        forest
    }

    fn write_forest(&self, out: &mut [u8]) {
        self.write_plan(
            Plan::Forest {
                body_bytes: self.forest_body_bytes(),
            },
            out,
        );
    }

    fn write_body(&self, plan: Plan, out: &mut [u8]) {
        let mut at = 0;
        write_uleb(out, &mut at, (self.len() - 1) as u32);
        match plan {
            Plan::Ef {
                layout, base, span, ..
            } => {
                write_uleb(out, &mut at, base);
                write_uleb(out, &mut at, span);
                ef::encode_into(self.iter(), base, layout, &mut out[at..]);
            }
            Plan::Forest { .. } => {
                let chunks = self.chunk_count();
                write_uleb(out, &mut at, chunks - 1);
                let geometry = Geometry::new(chunks, slot_bits_for(self.keys()), at);
                out[at..geometry.payload_at].fill(0);
                at = geometry.payload_at;
                let rows = self
                    .keys()
                    .iter()
                    .zip(self.containers())
                    .map(|(&key, container)| {
                        let row = ChunkRow {
                            key,
                            card: container.cardinality(),
                            kind: kind_of(container),
                            at,
                        };
                        at += payload_len(container);
                        row
                    });
                write_directory(out, geometry, rows);
                debug_assert_eq!(at, out.len());
                at = geometry.payload_at;
                for container in self.containers() {
                    let len = payload_len(container);
                    write_payload(container, &mut out[at..at + len]);
                    at += len;
                }
            }
        }
    }
}

impl Bitmosaic64 {
    /// Canonical directory over tightly adjacent, unframed u32 bodies.
    pub fn to_bytes(&self) -> Vec<u8> {
        let n = self.forests().len();
        if n == 0 {
            return EMPTY_U64_IMAGE.to_vec();
        }
        let count = u32::try_from(n - 1).expect("native u64 directory exceeds offset domain");
        let plans: Vec<_> = self.forests().iter().map(Bitmosaic::plan).collect();
        let local = Geometry64::new(n, uleb_len(count))
            .expect("native u64 directory exceeds offset domain");
        let body_bytes = plans
            .iter()
            .try_fold(local.payload_at, |at, plan| {
                at.checked_add(plan.body_bytes())
            })
            .expect("native u64 image exceeds offset domain");
        let mut out = vec![0; frame_len(body_bytes)];
        let mut at = write_frame(&mut out, KeyWidth::U64, ROOT_FOREST, body_bytes);
        write_uleb(&mut out, &mut at, count);
        let geometry = Geometry64::new(n, at).expect("native u64 directory exceeds offset domain");
        let mut before = 0u64;
        at = geometry.payload_at;
        for (i, ((&high, forest), plan)) in self
            .highs()
            .iter()
            .zip(self.forests())
            .zip(plans)
            .enumerate()
        {
            out[geometry.keys_at + 4 * i..][..4].copy_from_slice(&high.to_le_bytes());
            if i != 0 && i + 1 != n {
                out[geometry.before_at + 8 * (i - 1)..][..8].copy_from_slice(&before.to_le_bytes());
            }
            if i != 0 {
                out[geometry.offsets_at + 4 * (i - 1)..][..4]
                    .copy_from_slice(&((at - geometry.payload_at) as u32).to_le_bytes());
            }
            out[geometry.roots_at + i / 8] |= plan.root() << (i % 8);
            forest.write_body(plan, &mut out[at..at + plan.body_bytes()]);
            at += plan.body_bytes();
            before = before
                .checked_add(forest.len())
                .expect("native u64 cardinality overflow");
        }
        out
    }
}

#[derive(Clone, Copy, Debug)]
struct Geometry64 {
    keys_at: usize,
    before_at: usize,
    offsets_at: usize,
    roots_at: usize,
    payload_at: usize,
}

impl Geometry64 {
    fn new(n: usize, keys_at: usize) -> Option<Self> {
        let before_at = keys_at.checked_add(n.checked_mul(4)?)?;
        let offsets_at = before_at.checked_add(n.saturating_sub(2).checked_mul(8)?)?;
        let roots_at = offsets_at.checked_add(n.saturating_sub(1).checked_mul(4)?)?;
        let payload_at = roots_at.checked_add(n.div_ceil(8))?;
        Some(Self {
            keys_at,
            before_at,
            offsets_at,
            roots_at,
            payload_at,
        })
    }
}

// ── borrowed containers ──────────────────────────────────────────────────

/// Byte offset of the word plane inside a `Words` payload: a 16-byte
/// summary, `base_word` and `word_count` as `u16`, then the min/max refuter
/// as two more `u16` — which is also what puts the words on an 8-byte
/// boundary, so the refuter is carried for free.
pub(crate) const WORDS_AT: usize = 24;

// The refuter instrument's seams. Without `bench-internals` these compile to
// a constant `true` and to nothing, so the shipped seek carries no load, no
// branch and no counter — see `crate::refuter` for why that matters here.
#[cfg(any(test, feature = "bench-internals"))]
#[inline(always)]
fn refuter_armed() -> bool {
    crate::refuter::armed()
}

#[cfg(not(any(test, feature = "bench-internals")))]
#[inline(always)]
fn refuter_armed() -> bool {
    true
}

/// Charge one refuter counter, named by its static. A macro rather than a
/// function because the counters do not EXIST without the feature, so a
/// function signature mentioning them could not be written in a shipped
/// build; the `let _` arm keeps the argument type-checked either way.
#[cfg(any(test, feature = "bench-internals"))]
macro_rules! charge {
    ($counter:ident, $n:expr) => {
        crate::refuter::bump(&crate::refuter::$counter, $n)
    };
}

#[cfg(not(any(test, feature = "bench-internals")))]
macro_rules! charge {
    ($counter:ident, $n:expr) => {
        let _: u64 = $n;
    };
}

/// Offset of the min/max refuter within the `Words` payload. It sits inside
/// the same 24-byte head that [`CView::open`] already reads, hence on a line
/// the probe has paid for; the word and directory planes it refutes start at
/// [`WORDS_AT`] and run to the end of the payload.
pub(crate) const BOUNDS_AT: usize = 20;

#[derive(Clone, Copy, Debug)]
pub(crate) enum CView<'a> {
    /// The payload is held whole and its three planes are derived on
    /// demand. `contains` needs one word and nothing else, so eagerly
    /// loading the 16-byte summary and building the word and directory
    /// subslices would be per-probe work for two ops that do not run.
    Words {
        payload: &'a [u8],
        base_word: u32,
        word_count: u32,
        len: u32,
    },
    Stride {
        first: u16,
        stride: u16,
        len: u16,
    },
    Cone {
        first: u16,
        scale: u64,
        eps: u8,
        residuals: &'a [u8],
    },
    Array(&'a [u8]),
    Runs(RunsView<'a>),
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RunsView<'a> {
    bounds: &'a [u8],
    prefixes: &'a [u8],
    len: u32,
}

impl<'a> RunsView<'a> {
    fn open(bytes: &'a [u8], len: u32) -> Option<Self> {
        let mut at = 0;
        let count = read_uleb(bytes, &mut at, 32_767)? as usize + 1;
        if bytes.len() != runs_payload_len(count) {
            return None;
        }
        let bounds = bytes.get(at..at + 4 * count)?;
        let view = Self {
            bounds,
            prefixes: &bytes[at + 4 * count..],
            len,
        };
        let mut before = 0u32;
        for i in 0..count {
            let start = view.start(i);
            let end = view.end(i);
            if start > end || (i != 0 && view.end(i - 1) + 1 >= start) {
                return None;
            }
            if i != 0
                && i + 1 != count
                && u32::from(read_u16_at(view.prefixes, 2 * (i - 1))) != before
            {
                return None;
            }
            before = before.checked_add(end - start + 1)?;
        }
        (before == len).then_some(view)
    }

    /// After attachment proved the count/extent, invert its exact size instead
    /// of decoding the count or revalidating records on every member probe.
    fn from_validated(bytes: &'a [u8], len: u32) -> Self {
        let size = bytes.len();
        let header = if size <= runs_payload_len(128) {
            1
        } else if size <= runs_payload_len(16_384) {
            2
        } else {
            3
        };
        let count = if size <= 9 {
            (size - 1) / 4
        } else {
            (size + 4 - header) / 6
        };
        let (bounds, prefixes) = bytes[header..].split_at(4 * count);
        Self {
            bounds,
            prefixes,
            len,
        }
    }

    pub(crate) fn count(self) -> usize {
        self.bounds.len() / 4
    }
    pub(crate) fn start(self, i: usize) -> u32 {
        u32::from(read_u16_at(self.bounds, 4 * i))
    }
    pub(crate) fn end(self, i: usize) -> u32 {
        u32::from(read_u16_at(self.bounds, 4 * i + 2))
    }
    fn before(self, i: usize) -> u32 {
        if i == 0 {
            0
        } else if i + 1 == self.count() {
            self.len - (self.end(i) - self.start(i) + 1)
        } else {
            u32::from(read_u16_at(self.prefixes, 2 * (i - 1)))
        }
    }
    fn lower_bound(self, from: usize, value: u16) -> usize {
        let (mut lo, mut hi) = (from, self.count());
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.end(mid) < u32::from(value) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }
    fn contains(self, value: u16) -> bool {
        let i = self.lower_bound(0, value);
        i < self.count() && self.start(i) <= u32::from(value)
    }
    fn rank_below(self, value: u16) -> u32 {
        let i = self.lower_bound(0, value);
        if i == self.count() {
            self.len
        } else {
            self.before(i) + u32::from(value).saturating_sub(self.start(i))
        }
    }
    fn select(self, rank: u32) -> u16 {
        let i = select_group(self.count(), self.len, rank, |i| self.before(i));
        (self.start(i) + rank - self.before(i)) as u16
    }
}

fn and_runs_view(runs: RunsView<'_>, other: &CView<'_>) -> u32 {
    match other {
        CView::Runs(right) => crate::run_overlap_count(
            runs.count(),
            |i| (runs.start(i), runs.end(i) + 1),
            right.count(),
            |i| (right.start(i), right.end(i) + 1),
        ),
        CView::Stride { first, stride, len } => (0..runs.count())
            .map(|i| {
                crate::progression_in_span(
                    runs.start(i),
                    runs.end(i) + 1,
                    u32::from(*first),
                    u32::from(*stride),
                    u32::from(*len) + 1,
                )
            })
            .sum(),
        CView::Words { len, .. } => (0..runs.count())
            .map(|i| {
                let after = if runs.end(i) == 65_535 {
                    *len
                } else {
                    other.rank_below((runs.end(i) + 1) as u16)
                };
                after - other.rank_below(runs.start(i) as u16)
            })
            .sum(),
        CView::Array(values) => crate::runs_sequence_count(
            runs.count(),
            |i| (runs.start(i) as u16, runs.end(i) as u16),
            values.len() / 2,
            |i| read_u16_at(values, 2 * i),
        ),
        CView::Cone {
            first,
            scale,
            residuals,
            ..
        } => crate::runs_sequence_count(
            runs.count(),
            |i| (runs.start(i) as u16, runs.end(i) as u16),
            residuals.len(),
            |i| cone_value(*first, *scale, residuals, i),
        ),
    }
}

impl<'a> CView<'a> {
    /// `card` comes from the directory's prefix plane, which is why no
    /// container stores its own length.
    pub(crate) fn open(rest: &'a [u8], kind: u32, card: u32) -> Option<CView<'a>> {
        // An occupied chunk always holds a member; a zero here is a directory
        // that disagrees with itself, and the stride arm's `card - 1` would
        // otherwise be the first thing to notice.
        if card == 0 || card > 65_536 {
            return None;
        }
        let view = match kind {
            K_STRIDE => {
                let first = read_u16_at(rest.get(..4)?, 0);
                let stride = read_u16_at(rest, 2);
                if stride == 0 || u32::from(first) + (card - 1) * u32::from(stride) > 65_535 {
                    return None;
                }
                CView::Stride {
                    first,
                    stride,
                    len: (card - 1) as u16,
                }
            }
            K_CONE => {
                let head = rest.get(..11 + card as usize)?;
                let scale = read_u64_at(head, 3);
                // Every probe multiplies an index below `card` by the model
                // scale; a scale that overflows that product is refused here
                // so no probe carries the check.
                u64::from(card).checked_mul(scale)?;
                CView::Cone {
                    first: read_u16_at(head, 0),
                    eps: head[2],
                    scale,
                    residuals: &rest[11..11 + card as usize],
                }
            }
            K_RUNS => CView::Runs(RunsView::open(rest, card)?),
            K_ARRAY => CView::Array(rest.get(..2 * card as usize)?),
            K_WORDS => {
                let head = rest.get(..WORDS_AT)?;
                let base_word = u32::from(read_u16_at(head, 16));
                let word_count = u32::from(read_u16_at(head, 18));
                let wc = word_count as usize;
                // The window is whole summary blocks inside the chunk: the
                // select walk and the block summary index by that shape.
                if wc == 0
                    || !wc.is_multiple_of(BLOCK_WORDS)
                    || !(base_word as usize).is_multiple_of(BLOCK_WORDS)
                    || base_word as usize + wc > CHUNK_WORDS
                {
                    return None;
                }
                CView::Words {
                    payload: rest.get(..words_payload_len(wc))?,
                    base_word,
                    word_count,
                    len: card,
                }
            }
            _ => return None,
        };
        (view.payload_len() == rest.len()).then_some(view)
    }

    /// Query construction reads only the selected payload's query fields.
    /// Attachment has already proved its tag, extent and structural invariants.
    fn from_validated(payload: &'a [u8], kind: u32, card: u32) -> Self {
        match kind {
            K_WORDS => Self::Words {
                payload,
                base_word: u32::from(read_u16_at(payload, 16)),
                word_count: u32::from(read_u16_at(payload, 18)),
                len: card,
            },
            K_STRIDE => Self::Stride {
                first: read_u16_at(payload, 0),
                stride: read_u16_at(payload, 2),
                len: (card - 1) as u16,
            },
            K_CONE => Self::Cone {
                first: read_u16_at(payload, 0),
                scale: read_u64_at(payload, 3),
                eps: payload[2],
                residuals: &payload[11..],
            },
            K_ARRAY => Self::Array(payload),
            K_RUNS => Self::Runs(RunsView::from_validated(payload, card)),
            _ => unreachable!("attachment proved the container tag"),
        }
    }

    pub(crate) fn payload_len(self) -> usize {
        match self {
            Self::Runs(runs) => runs_payload_len(runs.count()),
            Self::Stride { .. } => 4,
            Self::Cone { residuals, .. } => 11 + residuals.len(),
            Self::Array(values) => values.len(),
            Self::Words { payload, .. } => payload.len(),
        }
    }

    /// Content verification run once by [`BitmosaicView::open_verified`] so
    /// that every probe afterwards is arithmetic over bytes that agree with
    /// each other: members are real `u16`s in strictly ascending order, and
    /// the planes a `Words` chunk derives from its words — directory, block
    /// summary, member bounds — say what the words say. Same-crate bytes
    /// always pass; this is the reader's half of the "refused rather than
    /// trusted" contract for everyone else's, priced once per plane.
    fn verify(&self, card: u32) -> bool {
        match self {
            CView::Runs(_) => true,
            CView::Stride { first, stride, len } => {
                let (first, stride, len) = (u32::from(*first), u32::from(*stride), u32::from(*len));
                (len == 0 || stride != 0) && first + stride * len <= u32::from(u16::MAX)
            }
            CView::Cone {
                first,
                scale,
                residuals,
                ..
            } => {
                // `open` proved `card * scale` fits, so every `i * scale` does.
                let mut prev: Option<u16> = None;
                for (i, r) in residuals.iter().enumerate() {
                    let raw = i64::from(*first)
                        + ((i as u64 * *scale) >> 32) as i64
                        + i64::from(*r as i8);
                    if !(0..=i64::from(u16::MAX)).contains(&raw) {
                        return false;
                    }
                    let v = raw as u16;
                    if prev.is_some_and(|p| p >= v) {
                        return false;
                    }
                    prev = Some(v);
                }
                true
            }
            CView::Array(values) => {
                let mut prev: Option<u16> = None;
                for i in 0..values.len() / 2 {
                    let v = read_u16_at(values, 2 * i);
                    if prev.is_some_and(|p| p >= v) {
                        return false;
                    }
                    prev = Some(v);
                }
                true
            }
            CView::Words {
                payload,
                base_word,
                word_count,
                ..
            } => {
                let (base, wc) = (*base_word as usize, *word_count as usize);
                let summary = Self::summary(payload);
                let (mut total, mut min, mut max) = (0u32, None, None);
                for w in 0..wc {
                    if w.is_multiple_of(DIR_STRIDE)
                        && u32::from(Self::dir_entry(payload, *word_count, w / DIR_STRIDE)) != total
                    {
                        return false;
                    }
                    let word = read_u64_at(payload, WORDS_AT + w * 8);
                    if word != 0 {
                        let at = (base + w) * 64;
                        min.get_or_insert(at + word.trailing_zeros() as usize);
                        max = Some(at + 63 - word.leading_zeros() as usize);
                    }
                    total += word.count_ones();
                }
                if total != card {
                    return false;
                }
                for block in 0..CHUNK_WORDS / BLOCK_WORDS {
                    let first = block * BLOCK_WORDS;
                    let occupied = first >= base
                        && first < base + wc
                        && (0..BLOCK_WORDS)
                            .any(|k| read_u64_at(payload, WORDS_AT + (first - base + k) * 8) != 0);
                    if (summary[block / 64] >> (block % 64) & 1 == 1) != occupied {
                        return false;
                    }
                }
                let (lo, hi) = Self::member_bounds(payload);
                min == Some(usize::from(lo)) && max == Some(usize::from(hi))
            }
        }
    }

    #[inline(always)]
    pub(crate) fn cardinality(&self) -> u32 {
        match self {
            CView::Runs(runs) => runs.len,
            CView::Stride { len, .. } => u32::from(*len) + 1,
            CView::Cone { residuals, .. } => residuals.len() as u32,
            CView::Array(values) => values.len() as u32 / 2,
            CView::Words { len, .. } => *len,
        }
    }

    /// A word of the chunk in ABSOLUTE chunk coordinates. Outside the
    /// span-tight window there are no members, so the word is zero — the
    /// window boundary is data, not an error, and `contains` needs no
    /// separate range test.
    #[inline(always)]
    fn word(&self, absolute: usize) -> u64 {
        match self {
            CView::Words {
                payload,
                base_word,
                word_count,
                ..
            } => match absolute.checked_sub(*base_word as usize) {
                Some(at) if at < *word_count as usize => read_u64_at(payload, WORDS_AT + at * 8),
                _ => 0,
            },
            _ => 0,
        }
    }

    /// Cumulative popcount before directory group `g`, counted within the
    /// window.
    #[inline(always)]
    fn dir_entry(payload: &[u8], word_count: u32, g: usize) -> u16 {
        if g == 0 {
            0
        } else {
            read_u16_at(payload, WORDS_AT + word_count as usize * 8 + (g - 1) * 2)
        }
    }

    /// The 8-word block occupancy summary, in ABSOLUTE block coordinates —
    /// read only by the intersection kernel, never by a probe.
    #[inline(always)]
    fn summary(payload: &[u8]) -> [u64; SUMMARY_WORDS] {
        [read_u64_at(payload, 0), read_u64_at(payload, 8)]
    }

    /// The word plane alone, for the SIMD intersection kernel.
    #[inline(always)]
    fn words(payload: &'a [u8], word_count: u32) -> &'a [u8] {
        &payload[WORDS_AT..WORDS_AT + word_count as usize * 8]
    }

    #[inline(always)]
    fn contains(&self, v: u16) -> bool {
        match self {
            CView::Runs(runs) => runs.contains(v),
            CView::Stride { first, stride, len } => {
                let (first, stride) = (u32::from(*first), u32::from(*stride));
                let v = u32::from(v);
                if v < first || stride == 0 {
                    return v == first;
                }
                let q = (v - first) / stride;
                q <= u32::from(*len) && first + q * stride == v
            }
            CView::Cone {
                first,
                scale,
                eps,
                residuals,
            } => {
                let (lo, hi) = cone_window(*first, *scale, *eps, residuals.len(), v);
                (lo..hi).any(|i| cone_value(*first, *scale, residuals, i) == v)
            }
            CView::Array(values) => array_search(values, v).is_ok(),
            CView::Words { .. } => self.word(usize::from(v >> 6)) >> (v & 63) & 1 == 1,
        }
    }

    #[inline(always)]
    fn rank_below(&self, v: u16) -> u32 {
        match self {
            CView::Runs(runs) => runs.rank_below(v),
            CView::Stride { first, stride, len } => {
                let (first, stride) = (u32::from(*first), u32::from(*stride));
                let v = u32::from(v);
                if v <= first {
                    0
                // A zero stride holds one value; a nonzero divisor makes the
                // checked division identical to the raw form it replaces.
                } else {
                    (v - 1 - first)
                        .checked_div(stride.max(1))
                        .map_or(1, |steps| steps + 1)
                        .min(u32::from(*len) + 1)
                }
            }
            CView::Cone {
                first,
                scale,
                eps,
                residuals,
            } => {
                let (lo, hi) = cone_window(*first, *scale, *eps, residuals.len(), v);
                let mut at = lo;
                while at < hi && cone_value(*first, *scale, residuals, at) < v {
                    at += 1;
                }
                at as u32
            }
            CView::Array(values) => match array_search(values, v) {
                Ok(at) | Err(at) => at as u32,
            },
            CView::Words {
                payload,
                base_word,
                word_count,
                len,
            } => {
                let word = usize::from(v >> 6);
                let base = *base_word as usize;
                // Below the window: nothing precedes `v`. Above it: every
                // member does. The window is snapped to whole summary
                // blocks, so both ends are exact.
                if word < base {
                    return 0;
                }
                if word >= base + *word_count as usize {
                    return *len;
                }
                let group = (word - base) / DIR_STRIDE;
                let mut count = u32::from(Self::dir_entry(payload, *word_count, group));
                for w in base + group * DIR_STRIDE..word {
                    count += self.word(w).count_ones();
                }
                // The refuter's subject, published once per call: one
                // directory entry plus the words this walk touched. Both
                // planes start at `WORDS_AT`, i.e. off the head line the
                // open already paid for.
                charge!(RANK_WORDS, (2 + word - (base + group * DIR_STRIDE)) as u64);
                count + (self.word(word) & ((1u64 << (v & 63)) - 1)).count_ones()
            }
        }
    }

    /// Exact `(min, max)` member of a `Words` chunk, read from the head line.
    #[inline(always)]
    fn member_bounds(payload: &[u8]) -> (u16, u16) {
        (
            read_u16_at(payload, BOUNDS_AT),
            read_u16_at(payload, BOUNDS_AT + 2),
        )
    }

    /// Rank of the first member `>= v`, or `None` when every member is below
    /// `v` and the caller should move to the next chunk.
    ///
    /// The refuter arm: a `Words` chunk answers both terminal cases from the
    /// head line the open already loaded, so a seek that skips the chunk — or
    /// lands at or before its first member — touches neither the word plane
    /// nor the directory plane. Only a seek that genuinely lands INSIDE the
    /// chunk pays [`Self::rank_below`], and it is the only one that must.
    #[inline(always)]
    fn seek_rank(&self, v: u16) -> Option<u32> {
        if let CView::Words { payload, .. } = self
            && refuter_armed()
        {
            let (min, max) = Self::member_bounds(payload);
            if v > max {
                charge!(SKIPPED, 1);
                return None;
            }
            if v <= min {
                charge!(HEADED, 1);
                return Some(0);
            }
            charge!(WALKED, 1);
        }
        let rank = self.rank_below(v);
        (rank < self.cardinality()).then_some(rank)
    }

    #[inline(always)]
    pub(crate) fn select(&self, k: u32) -> u16 {
        match self {
            CView::Runs(runs) => runs.select(k),
            CView::Stride { first, stride, .. } => {
                (u32::from(*first) + k * u32::from(*stride)) as u16
            }
            CView::Cone {
                first,
                scale,
                residuals,
                ..
            } => cone_value(*first, *scale, residuals, k as usize),
            CView::Array(values) => read_u16_at(values, k as usize * 2),
            CView::Words {
                payload,
                base_word,
                word_count,
                len,
            } => {
                let groups = *word_count as usize / DIR_STRIDE;
                let g = select_group(groups, *len, k, |i| {
                    u32::from(Self::dir_entry(payload, *word_count, i))
                });
                // Both bounds are for an UNVERIFIED payload whose directory
                // may disagree with its words: the subtraction saturates and
                // the walk ends at the window instead of spinning on the
                // zero words past it. Verified planes never take either arm.
                let mut remaining =
                    k.saturating_sub(u32::from(Self::dir_entry(payload, *word_count, g)));
                let mut w = *base_word as usize + g * DIR_STRIDE;
                let end = *base_word as usize + *word_count as usize;
                while w < end {
                    let bits = self.word(w);
                    let ones = bits.count_ones();
                    if remaining < ones {
                        let mut word = bits;
                        for _ in 0..remaining {
                            word &= word - 1;
                        }
                        return ((w as u32) * 64 + word.trailing_zeros()) as u16;
                    }
                    remaining -= ones;
                    w += 1;
                }
                (end as u32 * 64 - 1) as u16
            }
        }
    }

    fn for_each(&self, mut f: impl FnMut(u16)) {
        match self {
            CView::Runs(runs) => {
                for i in 0..runs.count() {
                    for v in runs.start(i)..=runs.end(i) {
                        f(v as u16);
                    }
                }
            }
            CView::Stride { first, stride, len } => {
                let mut v = u32::from(*first);
                for _ in 0..=u32::from(*len) {
                    f(v as u16);
                    v += u32::from(*stride).max(1);
                }
            }
            CView::Cone {
                first,
                scale,
                residuals,
                ..
            } => {
                for i in 0..residuals.len() {
                    f(cone_value(*first, *scale, residuals, i));
                }
            }
            CView::Array(values) => {
                for k in 0..values.len() / 2 {
                    f(read_u16_at(values, k * 2));
                }
            }
            CView::Words {
                payload,
                base_word,
                word_count,
                ..
            } => {
                for w in 0..*word_count as usize {
                    let mut bits = read_u64_at(payload, WORDS_AT + w * 8);
                    let base = (*base_word as usize + w) as u32 * 64;
                    while bits != 0 {
                        f((base + bits.trailing_zeros()) as u16);
                        bits &= bits - 1;
                    }
                }
            }
        }
    }
}

#[inline]
pub(crate) fn cone_value(first: u16, scale: u64, residuals: &[u8], i: usize) -> u16 {
    (i64::from(first) + ((i as u64 * scale) >> 32) as i64 + i64::from(residuals[i] as i8)) as u16
}

/// The eps bound makes the candidate window a constant-width slice around
/// the model's prediction — the reason a cone probe never searches.
///
/// Clamped at BOTH ends. For a probe above the chunk's last member the
/// prediction runs off the residual plane, and an unclamped start would be
/// returned verbatim as the rank.
#[inline(always)]
fn cone_window(first: u16, scale: u64, eps: u8, n: usize, v: u16) -> (usize, usize) {
    let delta = (u64::from(v).saturating_sub(u64::from(first))) << 32;
    // Zero scale means "no model": predict 0 (checked_div's None arm).
    let guess = delta.checked_div(scale).unwrap_or(0) as i64;
    let margin = 2 + i64::from(eps);
    (
        (guess - margin).max(0).min(n as i64) as usize,
        ((guess + margin + 1) as usize).min(n),
    )
}

/// Binary search over an unaligned little-endian `u16` plane.
#[inline(always)]
fn array_search(values: &[u8], v: u16) -> Result<usize, usize> {
    let (mut lo, mut hi) = (0usize, values.len() / 2);
    while lo < hi {
        let mid = (lo + hi) / 2;
        match read_u16_at(values, mid * 2).cmp(&v) {
            core::cmp::Ordering::Less => lo = mid + 1,
            core::cmp::Ordering::Greater => hi = mid,
            core::cmp::Ordering::Equal => return Ok(mid),
        }
    }
    Err(lo)
}

// ── borrowed AND ─────────────────────────────────────────────────────────

fn and_count(a: &CView<'_>, b: &CView<'_>) -> u32 {
    match (a, b) {
        (CView::Runs(runs), other) | (other, CView::Runs(runs)) => and_runs_view(*runs, other),
        // Closed form, zero data touched.
        (
            CView::Stride {
                first: a0,
                stride: s1,
                len: n1,
            },
            CView::Stride {
                first: b0,
                stride: s2,
                len: n2,
            },
        ) => stride_and_closed(
            u32::from(*a0),
            u32::from((*s1).max(1)),
            u32::from(*n1) + 1,
            u32::from(*b0),
            u32::from((*s2).max(1)),
            u32::from(*n2) + 1,
        ),
        (
            CView::Words {
                payload: pa,
                base_word: ba,
                word_count: ca,
                len: la,
            },
            CView::Words {
                payload: pb,
                base_word: bb,
                word_count: cb,
                len: lb,
            },
        ) => words_and(
            CView::words(pa, *ca),
            *ba,
            *la,
            &CView::summary(pa),
            CView::words(pb, *cb),
            *bb,
            *lb,
            &CView::summary(pb),
        ),
        (CView::Array(x), CView::Array(y)) => {
            let (na, nb) = (x.len() / 2, y.len() / 2);
            // Both skewed probes and the balanced vector kernel read LE bytes at any alignment.
            if drive_from_small(na, nb, DRIVE_RATIO_MEMBERS) {
                return gallop_drive(na, |i| read_u16_at(x, i * 2), nb, |i| read_u16_at(y, i * 2));
            }
            if drive_from_small(nb, na, DRIVE_RATIO_MEMBERS) {
                return gallop_drive(nb, |i| read_u16_at(y, i * 2), na, |i| read_u16_at(x, i * 2));
            }
            merge_and_array_bytes(x, y)
        }
        // Enumerate the smaller side, probe the larger: the probe is O(1)
        // for every arm except Array, and Array x Array is handled above.
        _ => {
            let mut count = 0u32;
            if a.cardinality() <= b.cardinality() {
                a.for_each(|v| count += u32::from(b.contains(v)));
                charge_linear(a.cardinality() as usize);
            } else {
                b.for_each(|v| count += u32::from(a.contains(v)));
                charge_linear(b.cardinality() as usize);
            }
            count
        }
    }
}
/// Chunk-pair AND driven from the sparser mapped forest.
///
/// [`and_count`] is symmetric in its operands, so orienting the walk by
/// chunk count alone cannot change the total.
fn forest_and_driven(small: &Forest<'_>, large: &Forest<'_>) -> u64 {
    let (ns, nl) = (small.chunks as usize, large.chunks as usize);
    let mut count = 0u64;
    crate::gallop_walk(
        ns,
        |i| small.key(i),
        nl,
        |j| large.key(j),
        |i, j| {
            if let (Some(ca), Some(cb)) = (small.container(i), large.container(j)) {
                count += u64::from(and_count(&ca, &cb));
            }
        },
    );
    count
}

/// Word-AND over the OVERLAP of two span-tight windows.
///
/// The windows are snapped to whole summary blocks, so `sa & sb` already
/// excludes every block outside the overlap — the steering loop needs no
/// range clamp, and the dense sweep runs over the intersected window only.
/// Two chunks occupying disjoint halves of their key space touch nothing.
#[allow(clippy::too_many_arguments)]
fn words_and(
    wa: &[u8],
    ba: u32,
    la: u32,
    sa: &[u64; SUMMARY_WORDS],
    wb: &[u8],
    bb: u32,
    lb: u32,
    sb: &[u64; SUMMARY_WORDS],
) -> u32 {
    let lo = ba.max(bb) as usize;
    let hi = (ba as usize + wa.len() / 8).min(bb as usize + wb.len() / 8);
    if lo >= hi {
        return 0;
    }
    let (oa, ob) = ((lo - ba as usize) * 8, (lo - bb as usize) * 8);
    let (xa, xb) = (
        &wa[oa..(hi - ba as usize) * 8],
        &wb[ob..(hi - bb as usize) * 8],
    );
    let dense = la as usize + lb as usize >= (hi - lo) * 32;
    let live = if dense {
        u32::MAX
    } else {
        (sa[0] & sb[0]).count_ones() + (sa[1] & sb[1]).count_ones()
    };
    if live >= 32 {
        // The kernel takes bytes, so a mapped plane feeds it directly - no
        // alignment-dependent `&[u64]` cast and no scalar arm when it fails.
        return crate::words_and_count([xa, xb])
            .try_into()
            .unwrap_or(u32::MAX);
    }
    let mut count = 0u32;
    for half in 0..SUMMARY_WORDS {
        let mut mask = sa[half] & sb[half];
        while mask != 0 {
            let block = half * 64 + mask.trailing_zeros() as usize;
            mask &= mask - 1;
            let base = block * BLOCK_WORDS;
            if base < lo || base >= hi {
                continue;
            }
            for w in 0..BLOCK_WORDS {
                let at = (base - lo + w) * 8;
                count += (read_u64_at(xa, at) & read_u64_at(xb, at)).count_ones();
            }
        }
    }
    count
}

// ── the view ─────────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug)]
pub(crate) struct Forest<'a> {
    pub(crate) bytes: &'a [u8],
    pub(crate) len: u64,
    pub(crate) chunks: u32,
    pub(crate) geometry: Geometry,
}

impl<'a> Forest<'a> {
    #[inline(always)]
    pub(crate) fn key(&self, i: usize) -> u16 {
        read_u16_at(self.bytes, self.geometry.keys_at + 2 * i)
    }

    #[inline(always)]
    pub(crate) fn before(&self, i: usize) -> u32 {
        if i == 0 {
            0
        } else {
            read_u32_at(self.bytes, self.geometry.before_at + 4 * (i - 1))
        }
    }

    #[inline(always)]
    pub(crate) fn after(&self, i: usize) -> u64 {
        if i + 1 == self.chunks as usize {
            self.len
        } else {
            u64::from(self.before(i + 1))
        }
    }

    pub(crate) fn payload_at(&self, i: usize) -> usize {
        self.geometry.payload_at
            + if i == 0 {
                0
            } else {
                read_u32_at(self.bytes, self.geometry.desc_at + 4 * (i - 1)) as usize
            }
    }

    pub(crate) fn container(&self, i: usize) -> Option<CView<'a>> {
        self.container_with::<false>(i)
    }

    fn container_with<const CHECK: bool>(&self, i: usize) -> Option<CView<'a>> {
        let at = self.payload_at(i);
        let end = if i + 1 == self.chunks as usize {
            self.bytes.len()
        } else {
            self.payload_at(i + 1)
        };
        let card = u32::try_from(self.after(i).checked_sub(u64::from(self.before(i)))?).ok()?;
        let payload = self.bytes.get(at..end)?;
        let kind = read_kind(self.bytes, self.geometry.kinds_at, i);
        if CHECK {
            CView::open(payload, kind, card)
        } else {
            Some(CView::from_validated(payload, kind, card))
        }
    }

    /// Chunk index for `key`, or `None`.
    ///
    /// Predict-verify first: for a forest whose keys are contiguous — every
    /// set spanning an unbroken key range — the guess is exact and the probe
    /// is ONE load for any forest size. The tagged slot plane is the
    /// fallback, and it is also one load: the fingerprint is the full key,
    /// so a hit needs no verifying load and a miss needs none at all.
    #[inline(always)]
    pub(crate) fn locate(&self, key: u16) -> Option<usize> {
        let n = self.chunks as usize;
        if n == 0 {
            return None;
        }
        let first = self.key(0);
        let guess = usize::from(key.wrapping_sub(first)).min(n - 1);
        if self.key(guess) == key {
            return Some(guess);
        }
        if key < first {
            return None;
        }
        if self.geometry.slot_bits != 0 {
            let mask = (1usize << self.geometry.slot_bits) - 1;
            let mut s = slot_of(key, self.geometry.slot_bits);
            loop {
                let slot = read_u64_at(self.bytes, self.geometry.slot_at + s * 8);
                if slot == 0 {
                    return None;
                }
                if (slot >> 32) as u16 == key {
                    // The index is the LOW 32 bits; the tag occupies the
                    // high half of the same word.
                    return Some((slot as u32) as usize - 1);
                }
                s = (s + 1) & mask;
            }
        }
        let at = self.walk(key);
        (at < n && self.key(at) == key).then_some(at)
    }

    /// Insertion point for `key` — the answer `rank` needs on a miss.
    #[inline(always)]
    fn walk(&self, key: u16) -> usize {
        let n = self.chunks as usize;
        if n <= 64 {
            let mut at = 0usize;
            for i in 0..n {
                at += usize::from(self.key(i) < key);
            }
            return at;
        }
        let (mut lo, mut hi) = (0usize, n);
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.key(mid) < key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// The slot plane must map EVERY key to its own chunk and nothing else:
    /// each occupied slot names a chunk whose key is the slot's tag, exactly
    /// `chunks` slots are occupied, and a probe from each key's home slot
    /// reaches its slot before it meets a hole. With all three, `locate`'s
    /// no-verify tag compare (see [`slot_of`]) is exact for any key, and a
    /// probe can never step onto payload bytes. One pass over the plane and
    /// one bounded probe per key, at open only.
    fn verify_slot_plane(&self) -> bool {
        let bits = self.geometry.slot_bits;
        if bits == 0 {
            return true;
        }
        let n = self.chunks as usize;
        let slots = 1usize << bits;
        let mask = slots - 1;
        let mut occupied = 0usize;
        for s in 0..slots {
            let slot = read_u64_at(self.bytes, self.geometry.slot_at + s * 8);
            if slot == 0 {
                continue;
            }
            let (tag, at) = (slot >> 32, slot as u32 as usize);
            if tag > u64::from(u16::MAX) || at == 0 || at > n || u64::from(self.key(at - 1)) != tag
            {
                return false;
            }
            occupied += 1;
        }
        if occupied != n {
            return false;
        }
        // Keys are distinct, so a slot tagged with this key is this key's
        // slot; the probe must find it inside one lap and before any hole.
        (0..n).all(|i| {
            let key = self.key(i);
            let mut s = slot_of(key, bits);
            for _ in 0..slots {
                let slot = read_u64_at(self.bytes, self.geometry.slot_at + s * 8);
                if slot == 0 {
                    return false;
                }
                if (slot >> 32) as u16 == key {
                    return true;
                }
                s = (s + 1) & mask;
            }
            false
        })
    }

    /// Chunk holding the `k`-th member, with the count before it.
    #[inline(always)]
    fn chunk_of_rank(&self, k: u64) -> (usize, u32) {
        let n = self.chunks as usize;
        if n <= 16 {
            let mut at = 0usize;
            while at + 1 < n && u64::from(self.before(at + 1)) <= k {
                at += 1;
            }
            return (at, self.before(at));
        }
        let (mut lo, mut hi) = (0usize, n);
        while lo < hi {
            let mid = (lo + hi) / 2;
            if u64::from(self.before(mid)) <= k {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        (lo - 1, self.before(lo - 1))
    }
}

/// Zero-copy reader over [`Bitmosaic::to_bytes`].
///
/// Owns nothing: every accessor is arithmetic over the borrowed string. The
/// two roots — a container forest and an Elias-Fano plane — answer the same
/// four questions, and which one a set was published as is a byte-count
/// decision made at freeze, not a caller's concern.
#[derive(Clone, Copy, Debug)]
pub struct BitmosaicView<'a> {
    pub(crate) root: Root<'a>,
    encoded_len: usize,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum Root<'a> {
    Forest(Forest<'a>),
    Ef(EfView<'a>),
}

impl<'a> BitmosaicView<'a> {
    /// Validate the structure and attach. `None` on any disagreement
    /// between the header, the byte length, the directory, and the closed
    /// forms the writer derived them from (the slot-plane width, the slot
    /// plane itself, the prefix plane, the chunk count). After this every
    /// index below is in range by construction, `locate` is exact, and no
    /// read path can panic or read outside the string — same-crate bytes
    /// always pass, damaged bytes are refused or answered from what they
    /// hold, never trusted past their own bounds.
    ///
    /// The cost is the directory walk this reader has always paid, which
    /// is what lets a query open a mapped plane per probe. Container
    /// PAYLOADS are not read here: a lie inside one (a Words directory that
    /// disagrees with its words, an unsorted Array) changes answers but
    /// never bounds. A trust boundary that must refuse those too opens with
    /// [`Self::open_verified`], once, and keeps the view.
    pub fn open(bytes: &'a [u8]) -> Option<BitmosaicView<'a>> {
        Self::open_with(bytes, false)
    }

    /// [`Self::open`] plus one pass over every container payload and the
    /// Elias-Fano plane: members are real `u16`s in strictly ascending
    /// order, model arithmetic cannot overflow, and every derived plane —
    /// Words directory, block summary, member bounds, Elias-Fano samples —
    /// says what the members say. After this every answer is the writer's.
    /// Linear in the string, allocation-free; for bytes that crossed a
    /// trust boundary, and once per plane rather than per probe.
    pub fn open_verified(bytes: &'a [u8]) -> Option<BitmosaicView<'a>> {
        Self::open_with(bytes, true)
    }

    fn open_with(bytes: &'a [u8], verify: bool) -> Option<BitmosaicView<'a>> {
        let header = image_header(bytes)?;
        if header.width != KeyWidth::U32 {
            return None;
        }
        let bytes = &bytes[..header.encoded_len];
        if header.encoded_len == IMAGE_ID_LEN {
            return Some(Self {
                root: Root::Forest(Forest {
                    bytes,
                    len: 0,
                    chunks: 0,
                    geometry: Geometry::new(0, 0, IMAGE_ID_LEN),
                }),
                encoded_len: IMAGE_ID_LEN,
            });
        }
        Self::open_body::<true>(bytes, header.body_offset, header.root, verify)
    }

    /// Attach one body, whether framed standalone or bounded by a u64 directory.
    fn open_body<const CHECK: bool>(
        bytes: &'a [u8],
        mut at: usize,
        root: u8,
        verify: bool,
    ) -> Option<Self> {
        let len = u64::from(read_uleb(bytes, &mut at, u32::MAX)?) + 1;
        let root = match root {
            ROOT_EF => {
                let base = read_uleb(bytes, &mut at, u32::MAX)?;
                let span = read_uleb(bytes, &mut at, u32::MAX)?;
                let ef = EfView::new(bytes.get(at..)?, base, span, len)?;
                if verify && !ef.verify() {
                    return None;
                }
                Root::Ef(ef)
            }
            ROOT_FOREST => {
                let chunks = read_uleb(bytes, &mut at, u32::from(u16::MAX))? + 1;
                let c = chunks as usize;
                let keys = bytes.get(at..at.checked_add(2 * c)?)?;
                let first = read_u16_at(keys, 0);
                let last = read_u16_at(keys, 2 * (c - 1));
                if last < first {
                    return None;
                }
                let geometry = Geometry::new(chunks, slot_bits_for_span(chunks, first, last), at);
                bytes.get(..geometry.payload_at)?;
                let used = (3 * c) % 8;
                if used != 0 && bytes[geometry.kinds_at + 3 * c / 8] >> used != 0 {
                    return None;
                }
                let forest = Forest {
                    bytes,
                    len,
                    chunks,
                    geometry,
                };
                if CHECK {
                    for i in 0..c {
                        if i != 0 && forest.key(i) <= forest.key(i - 1) {
                            return None;
                        }
                        let card = u32::try_from(
                            forest.after(i).checked_sub(u64::from(forest.before(i)))?,
                        )
                        .ok()?;
                        let container = forest.container_with::<true>(i)?;
                        if container.cardinality() != card || (verify && !container.verify(card)) {
                            return None;
                        }
                    }
                    if !forest.verify_slot_plane() {
                        return None;
                    }
                }
                Root::Forest(forest)
            }
            _ => return None,
        };
        Some(Self {
            root,
            encoded_len: bytes.len(),
        })
    }

    /// Encoded extent, distinct from the set's cardinality.
    pub fn serialized_len(&self) -> usize {
        self.encoded_len
    }

    #[inline(always)]
    pub fn len(&self) -> u64 {
        match &self.root {
            Root::Forest(f) => f.len,
            Root::Ef(e) => e.len(),
        }
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// True when this plane was published as an Elias-Fano root rather than
    /// a container forest — the ladder's verdict, readable without decoding.
    pub fn is_elias_fano(&self) -> bool {
        matches!(self.root, Root::Ef(_))
    }

    /// True when this plane carries a tagged slot table for key lookup.
    ///
    /// Observability, not policy: a sweep that never activates the slot
    /// path cannot certify it, and the gate that decides it — over 64
    /// chunks AND a broken key run — is invisible from the outside.
    pub fn has_slot_plane(&self) -> bool {
        matches!(self.root, Root::Forest(f) if f.geometry.slot_bits != 0)
    }

    #[inline]
    pub fn contains(&self, value: u32) -> bool {
        match &self.root {
            Root::Ef(e) => e.contains(value),
            Root::Forest(f) => {
                let Some(at) = f.locate((value >> 16) as u16) else {
                    return false;
                };
                f.container(at)
                    .is_some_and(|c| c.contains((value & 0xFFFF) as u16))
            }
        }
    }

    /// Members strictly below `value`.
    #[inline]
    pub fn rank(&self, value: u32) -> u64 {
        match &self.root {
            Root::Ef(e) => e.rank(value),
            Root::Forest(f) => {
                let key = (value >> 16) as u16;
                match f.locate(key) {
                    Some(at) => {
                        u64::from(f.before(at))
                            + f.container(at)
                                .map_or(0, |c| c.rank_below((value & 0xFFFF) as u16))
                                as u64
                    }
                    None => {
                        let at = f.walk(key);
                        if at == 0 { 0 } else { f.after(at - 1) }
                    }
                }
            }
        }
    }

    /// `k`-th member ascending.
    #[inline]
    pub fn select(&self, k: u64) -> Option<u32> {
        match &self.root {
            Root::Ef(e) => e.select(k),
            Root::Forest(f) => {
                if k >= f.len {
                    return None;
                }
                let (at, before) = f.chunk_of_rank(k);
                let low = f.container(at)?.select((k - u64::from(before)) as u32);
                Some((u32::from(f.key(at)) << 16) | u32::from(low))
            }
        }
    }

    /// Fused AND cardinality between two mapped planes. Allocation-free on
    /// every arm, including the mixed-root ones.
    pub fn and_len(&self, other: &BitmosaicView<'_>) -> u64 {
        match (&self.root, &other.root) {
            (Root::Forest(a), Root::Forest(b)) => {
                let (na, nb) = (a.chunks as usize, b.chunks as usize);
                // Same chooser as the owned forest: a sorted u16 key plane
                // whose merge is scalar, so it crosses over an order of
                // magnitude earlier than the member kernels do.
                if drive_from_small(na, nb, DRIVE_RATIO_KEYS) {
                    return forest_and_driven(a, b);
                }
                if drive_from_small(nb, na, DRIVE_RATIO_KEYS) {
                    return forest_and_driven(b, a);
                }
                let mut count = 0u64;
                let (mut i, mut j) = (0usize, 0usize);
                while i < na && j < nb {
                    let (ka, kb) = (a.key(i), b.key(j));
                    if ka == kb {
                        if let (Some(ca), Some(cb)) = (a.container(i), b.container(j)) {
                            count += u64::from(and_count(&ca, &cb));
                        }
                        i += 1;
                        j += 1;
                    } else if ka < kb {
                        i += 1;
                    } else {
                        j += 1;
                    }
                }
                charge_key_positions(i + j);
                count
            }
            // An EF root is a sparse set by construction — it was published
            // only because the forest was mostly directory — so enumerating
            // it against the other side's O(1) probe is the cheap direction.
            _ => {
                let mut count = 0u64;
                if self.len() <= other.len() {
                    self.for_each(|v| count += u64::from(other.contains(v)));
                } else {
                    other.for_each(|v| count += u64::from(self.contains(v)));
                }
                count
            }
        }
    }

    /// Write the intersection into a caller-owned buffer, ascending, and
    /// return how many values were written.
    ///
    /// The output layout is a closed form known BEFORE the first result
    /// exists: [`BitmosaicView::and_len`] is a popcount over mapped bytes and
    /// materialises nothing, so a caller sizes `dest` exactly and this
    /// writes into it. No arena, no growth, no append counter — and because
    /// each writer's slot is arithmetic, a partitioned caller needs no
    /// synchronisation between them.
    ///
    /// `dest` shorter than the intersection is a caller sizing error, not a
    /// truncation mode: the surplus is dropped and `debug_assert` fires.
    pub fn and_into(&self, other: &BitmosaicView<'_>, dest: &mut [u32]) -> usize {
        // Enumerate the smaller side against the larger side's O(1) probe.
        let (small, large) = if self.len() <= other.len() {
            (self, other)
        } else {
            (other, self)
        };
        let (mut written, mut hits) = (0usize, 0usize);
        small.for_each(|v| {
            if large.contains(v) {
                hits += 1;
                if let Some(slot) = dest.get_mut(written) {
                    *slot = v;
                    written += 1;
                }
            }
        });
        debug_assert_eq!(hits, written, "dest must be sized from and_len");
        written
    }

    /// Ascending walk. Allocation-free; `Words` streams whole bit-words
    /// rather than re-selecting per element.
    pub fn for_each(&self, mut f: impl FnMut(u32)) {
        match &self.root {
            Root::Ef(e) => e.for_each(f),
            Root::Forest(forest) => {
                for i in 0..forest.chunks as usize {
                    let high = u32::from(forest.key(i)) << 16;
                    if let Some(c) = forest.container(i) {
                        c.for_each(|v| f(high | u32::from(v)));
                    }
                }
            }
        }
    }

    /// `Some((first, stride, len))` when the whole set is one arithmetic
    /// progression.
    ///
    /// The refusal path reads the DIRECTORY ONLY — one `u32` per chunk — and
    /// returns on the first non-`Stride` kind without faulting a payload
    /// page. On the accepting path it reads 4 payload bytes per chunk. For a
    /// dense-ordinal domain, which is a literal stride-1 progression, that
    /// makes `rank`/`select` pure arithmetic over two loads total.
    pub fn as_arithmetic(&self) -> Option<(u32, u32, u64)> {
        let Root::Forest(f) = &self.root else {
            return None;
        };
        for i in 0..f.chunks as usize {
            if read_kind(f.bytes, f.geometry.kinds_at, i) != K_STRIDE {
                return None;
            }
        }
        let mut expect: Option<(u32, u32, u64, u32)> = None;
        for i in 0..f.chunks as usize {
            let CView::Stride { first, stride, len } = f.container(i)? else {
                return None;
            };
            let base = u32::from(f.key(i)) << 16;
            let (cf, cs, cn) = (
                base + u32::from(first),
                u32::from(stride.max(1)),
                u64::from(len) + 1,
            );
            match &mut expect {
                None => expect = Some((cf, cs, cn, cf + (cn as u32 - 1) * cs)),
                Some((_, s, n, last)) => {
                    if cs != *s || cf != *last + *s {
                        return None;
                    }
                    *n += cn;
                    *last = cf + (cn as u32 - 1) * cs;
                }
            }
        }
        expect.map(|(first, stride, n, _)| (first, stride, n))
    }

    /// An ascending cursor over every member, positioned at the first.
    ///
    /// [`Self::for_each`] pushes; this pulls. A merge needs to advance ONE
    /// side after comparing two heads, which internal iteration cannot
    /// express — see [`crate::Range`].
    #[inline]
    pub fn range(&self) -> BitmosaicViewRange<'a> {
        BitmosaicViewRange::new(*self)
    }
}

/// Zero-copy reader over [`Bitmosaic64::to_bytes`]: a prefix directory over
/// per-forest [`BitmosaicView`]s.
#[derive(Clone, Copy, Debug)]
pub struct Bitmosaic64View<'a> {
    bytes: &'a [u8],
    forests: u32,
    len: u64,
    geometry: Geometry64,
    last_before: u64,
    single: Option<BitmosaicView<'a>>,
}

impl<'a> Bitmosaic64View<'a> {
    pub fn open(bytes: &'a [u8]) -> Option<Bitmosaic64View<'a>> {
        Self::open_with(bytes, false)
    }

    pub fn open_verified(bytes: &'a [u8]) -> Option<Bitmosaic64View<'a>> {
        Self::open_with(bytes, true)
    }

    fn open_with(bytes: &'a [u8], verify: bool) -> Option<Self> {
        let header = image_header(bytes)?;
        if header.width != KeyWidth::U64 {
            return None;
        }
        let bytes = &bytes[..header.encoded_len];
        let mut at = header.body_offset;
        let forests = if header.encoded_len == IMAGE_ID_LEN {
            0
        } else {
            read_uleb(bytes, &mut at, u32::MAX)?.checked_add(1)?
        };
        let n = forests as usize;
        let geometry = Geometry64::new(n, at)?;
        bytes.get(..geometry.payload_at)?;
        if !n.is_multiple_of(8) && bytes[geometry.roots_at + n / 8] >> (n % 8) != 0 {
            return None;
        }
        let mut view = Self {
            bytes,
            forests,
            len: 0,
            geometry,
            last_before: 0,
            single: None,
        };
        for i in 0..n {
            if i != 0 && view.high(i) <= view.high(i - 1) {
                return None;
            }
            if i != 0 && i + 1 != n && view.before(i) != view.len {
                return None;
            }
            let child = view.forest_with(i, verify)?;
            if child.is_empty() {
                return None;
            }
            view.last_before = view.len;
            view.len = view.len.checked_add(child.len())?;
            if n == 1 {
                view.single = Some(child);
            }
        }
        Some(view)
    }

    pub fn serialized_len(&self) -> usize {
        self.bytes.len()
    }

    #[inline(always)]
    fn high(&self, i: usize) -> u32 {
        read_u32_at(self.bytes, self.geometry.keys_at + 4 * i)
    }

    #[inline(always)]
    fn before(&self, i: usize) -> u64 {
        if i == 0 {
            0
        } else if i + 1 == self.forests as usize {
            self.last_before
        } else {
            read_u64_at(self.bytes, self.geometry.before_at + 8 * (i - 1))
        }
    }

    fn child_at(&self, i: usize) -> usize {
        self.geometry.payload_at
            + if i == 0 {
                0
            } else {
                read_u32_at(self.bytes, self.geometry.offsets_at + 4 * (i - 1)) as usize
            }
    }

    fn forest_with(&self, i: usize, verify: bool) -> Option<BitmosaicView<'a>> {
        let at = self.child_at(i);
        let end = if i + 1 == self.forests as usize {
            self.bytes.len()
        } else {
            self.child_at(i + 1)
        };
        let root = (self.bytes[self.geometry.roots_at + i / 8] >> (i % 8)) & 1;
        BitmosaicView::open_body::<true>(self.bytes.get(at..end)?, 0, root, verify)
    }

    fn forest(&self, i: usize) -> Option<BitmosaicView<'a>> {
        if let Some(child) = self.single {
            return (i == 0).then_some(child);
        }
        let at = self.child_at(i);
        let end = if i + 1 == self.forests as usize {
            self.bytes.len()
        } else {
            self.child_at(i + 1)
        };
        let root = (self.bytes[self.geometry.roots_at + i / 8] >> (i % 8)) & 1;
        BitmosaicView::open_body::<false>(self.bytes.get(at..end)?, 0, root, false)
    }

    #[inline(always)]
    pub fn len(&self) -> u64 {
        self.len
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Predict-verify over the prefix plane: dense-ordinal populations have
    /// exactly one forest (high32 = 0), so this resolves in one load.
    #[inline(always)]
    fn locate(&self, high: u32) -> Option<usize> {
        let n = self.forests as usize;
        if n == 0 {
            return None;
        }
        let first = self.high(0);
        let guess = (high.wrapping_sub(first) as usize).min(n - 1);
        if self.high(guess) == high {
            return Some(guess);
        }
        if high < first {
            return None;
        }
        (0..n).find(|&i| self.high(i) == high)
    }

    /// Insertion point for `high`: occupied forests strictly below it.
    ///
    /// The miss half of [`Self::locate`], which a cursor seek needs and a
    /// probe does not. Linear for the same reason [`Self::rank`]'s miss path
    /// is: the directory holds one entry per occupied high-32 region, so it
    /// is short by construction and never assumed sorted.
    #[inline]
    fn walk(&self, high: u32) -> usize {
        (0..self.forests as usize)
            .filter(|&i| self.high(i) < high)
            .count()
    }

    #[inline]
    pub fn contains(&self, value: u64) -> bool {
        self.locate((value >> 32) as u32)
            .and_then(|i| self.forest(i))
            .is_some_and(|f| f.contains(value as u32))
    }

    #[inline]
    pub fn rank(&self, value: u64) -> u64 {
        let high = (value >> 32) as u32;
        match self.locate(high) {
            Some(i) => self.before(i) + self.forest(i).map_or(0, |f| f.rank(value as u32)),
            None => {
                let forests = self.forests as usize;
                let at = (0..forests).filter(|&i| self.high(i) < high).count();
                if at == 0 {
                    0
                } else if at == forests {
                    self.len
                } else {
                    self.before(at)
                }
            }
        }
    }

    #[inline]
    pub fn select(&self, k: u64) -> Option<u64> {
        if k >= self.len {
            return None;
        }
        let n = self.forests as usize;
        let mut at = 0usize;
        while at + 1 < n && self.before(at + 1) <= k {
            at += 1;
        }
        let low = self.forest(at)?.select(k - self.before(at))?;
        Some((u64::from(self.high(at)) << 32) | u64::from(low))
    }

    pub fn and_len(&self, other: &Bitmosaic64View<'_>) -> u64 {
        let mut count = 0u64;
        let (mut i, mut j) = (0usize, 0usize);
        while i < self.forests as usize && j < other.forests as usize {
            let (ha, hb) = (self.high(i), other.high(j));
            if ha == hb {
                if let (Some(a), Some(b)) = (self.forest(i), other.forest(j)) {
                    count += a.and_len(&b);
                }
                i += 1;
                j += 1;
            } else if ha < hb {
                i += 1;
            } else {
                j += 1;
            }
        }
        count
    }

    /// Ascending walk over every member, high-32 prefixes restored.
    ///
    /// The wire tier's u64 root had no walk at all, mirroring the gap on the
    /// owned [`Bitmosaic64`](crate::Bitmosaic64). Internal iteration rather than a
    /// range, because a view's container is decoded per chunk into a `CView`
    /// and the wire tier has exactly one walk shape — see
    /// [`BitmosaicView::for_each`], which this delegates to per forest.
    pub fn for_each(&self, mut f: impl FnMut(u64)) {
        for i in 0..self.forests as usize {
            let high = u64::from(self.high(i)) << 32;
            if let Some(forest) = self.forest(i) {
                forest.for_each(|v| f(high | u64::from(v)));
            }
        }
    }

    /// Whole-set progression over the u64 domain, directory-only on refusal.
    pub fn as_arithmetic(&self) -> Option<(u64, u64, u64)> {
        if self.forests != 1 || self.high(0) != 0 {
            return None;
        }
        self.forest(0)?
            .as_arithmetic()
            .map(|(f, s, n)| (u64::from(f), u64::from(s), n))
    }

    /// An ascending cursor over every member, high-32 prefixes restored.
    ///
    /// The pull counterpart of [`Self::for_each`], and the surface the four
    /// lazy set adaptors ([`crate::AndRange`] and friends) compose over —
    /// so the wire tier gets intersection, union, difference and symmetric
    /// difference over MAPPED BYTES without materialising either side.
    #[inline]
    pub fn range(&self) -> Bitmosaic64ViewRange<'a> {
        Bitmosaic64ViewRange::new(*self)
    }
}

// ── borrowed cursors ─────────────────────────────────────────────────────
//
// The owned tier grew the full `Range` protocol — and with it the four lazy
// adaptors — while the wire tier kept only `for_each`. That split is the
// wrong way round: the owned forest is a build-time shape, and the borrowed
// view is what a durable consumer actually holds over a mapped page. A
// zero-copy reader could therefore push every member but never pull one,
// which rules out every merge: leapfrog joins, k-way intersection, and all
// four adaptors, none of which can drive a callback. These three cursors
// close that gap at the same three tiers the owned side already has
// (container, forest, u64 forest-of-forests).

/// Ascending cursor over ONE chunk of a borrowed forest.
///
/// The wire twin of `ContainerRange`, and `Words` is again the arm that
/// matters: it carries the next word index and the unconsumed bits of the
/// current one, so a walk costs one load per 64 positions instead of a
/// [`CView::select`] that re-enters `select_group` per element.
///
/// Unlike the owned container this holds the payload bytes rather than a
/// `&[u64; CHUNK_WORDS]`, because a frozen `Words` payload is span-tight:
/// the cursor's load count is the occupied window, never the whole chunk.
#[derive(Clone, Copy, Debug)]
enum CRange<'a> {
    Words {
        payload: &'a [u8],
        base_word: u32,
        /// Index of the next word to load, RELATIVE to `base_word`; `bits`
        /// belongs to `word - 1`.
        word: u32,
        word_count: u32,
        /// Unconsumed set bits of the word at `word - 1`. Non-zero whenever
        /// `remaining > 0` — the idempotent-`front` invariant.
        bits: u64,
        remaining: u32,
    },
    Stride {
        next: u32,
        stride: u32,
        remaining: u32,
    },
    Cone {
        first: u16,
        scale: u64,
        residuals: &'a [u8],
        at: usize,
    },
    Array {
        values: &'a [u8],
        at: usize,
    },
    Runs {
        view: RunsView<'a>,
        at: usize,
        next: u32,
        remaining: u32,
    },
}

impl<'a> CRange<'a> {
    /// The exhausted cursor. Also the placeholder a forest walk installs
    /// before it opens its first chunk.
    const DONE: CRange<'a> = CRange::Array { values: &[], at: 0 };

    #[inline]
    fn open(view: CView<'a>) -> CRange<'a> {
        CRange::at_rank(view, 0)
    }

    /// Unconsumed members, the head included.
    ///
    /// What the forest tier subtracts from its own running count when this
    /// cursor advances by more than one — a relative seek moves an unknown
    /// number of members, and the forest's `remaining` is the ordinal every
    /// consumer reads its position from.
    #[inline(always)]
    fn remaining(&self) -> u32 {
        match self {
            CRange::Runs { remaining, .. }
            | CRange::Words { remaining, .. }
            | CRange::Stride { remaining, .. } => *remaining,
            CRange::Cone { residuals, at, .. } => {
                (residuals.len() - *at.min(&residuals.len())) as u32
            }
            CRange::Array { values, at } => {
                (values.len() / 2 - *at.min(&(values.len() / 2))) as u32
            }
        }
    }

    /// Position at chunk-local rank `k`; `k >= cardinality` is exhausted.
    ///
    /// The ONE `select` a seek pays, and it pays it once per jump rather
    /// than once per element — which is the distinction G213/G213.1 drew
    /// between the primitives that win and `select` in a loop.
    fn at_rank(view: CView<'a>, k: u32) -> CRange<'a> {
        if k >= view.cardinality() {
            return CRange::DONE;
        }
        match view {
            CView::Runs(view) => {
                let at = select_group(view.count(), view.len, k, |i| view.before(i));
                CRange::Runs {
                    view,
                    at,
                    next: view.start(at) + k - view.before(at),
                    remaining: view.len - k,
                }
            }
            CView::Stride { first, stride, len } => {
                let stride = u32::from(stride).max(1);
                CRange::Stride {
                    next: u32::from(first) + k * stride,
                    stride,
                    remaining: u32::from(len) + 1 - k,
                }
            }
            CView::Cone {
                first,
                scale,
                residuals,
                ..
            } => CRange::Cone {
                first,
                scale,
                residuals,
                at: k as usize,
            },
            CView::Array(values) => CRange::Array {
                values,
                at: k as usize,
            },
            CView::Words {
                payload,
                base_word,
                word_count,
                len,
            } => {
                let v = view.select(k);
                // A member always lies inside the span-tight window, so the
                // subtraction cannot underflow; fail closed rather than
                // trust that against a corrupt plane.
                let Some(w) = u32::from(v >> 6).checked_sub(base_word) else {
                    return CRange::DONE;
                };
                if w >= word_count {
                    return CRange::DONE;
                }
                CRange::Words {
                    payload,
                    base_word,
                    word: w + 1,
                    word_count,
                    bits: read_u64_at(payload, WORDS_AT + w as usize * 8)
                        & !((1u64 << (v & 63)) - 1),
                    remaining: len - k,
                }
            }
        }
    }
}

impl Range for CRange<'_> {
    type Item = u16;

    #[inline(always)]
    fn empty(&self) -> bool {
        match self {
            CRange::Runs { remaining, .. }
            | CRange::Words { remaining, .. }
            | CRange::Stride { remaining, .. } => *remaining == 0,
            CRange::Cone { residuals, at, .. } => *at >= residuals.len(),
            CRange::Array { values, at } => *at * 2 >= values.len(),
        }
    }

    #[inline(always)]
    fn front(&self) -> u16 {
        debug_assert!(!Range::empty(self), "front on an empty chunk range");
        match self {
            // `bits` belongs to the word at `word - 1`, relative to the
            // window base.
            CRange::Words {
                base_word,
                word,
                bits,
                ..
            } => {
                ((base_word.wrapping_add(*word).wrapping_sub(1)) * 64 + bits.trailing_zeros())
                    as u16
            }
            CRange::Runs { next, .. } | CRange::Stride { next, .. } => *next as u16,
            CRange::Cone {
                first,
                scale,
                residuals,
                at,
            } => cone_value(*first, *scale, residuals, *at),
            CRange::Array { values, at } => read_u16_at(values, *at * 2),
        }
    }

    #[inline(always)]
    fn pop_front(&mut self) {
        debug_assert!(!Range::empty(self), "pop_front on an empty chunk range");
        match self {
            CRange::Runs {
                view,
                at,
                next,
                remaining,
            } => {
                *remaining -= 1;
                *next += 1;
                if *remaining != 0 && *next > view.end(*at) {
                    *at += 1;
                    *next = view.start(*at);
                }
            }
            CRange::Words {
                payload,
                word,
                word_count,
                bits,
                remaining,
                ..
            } => {
                *bits &= *bits - 1;
                *remaining -= 1;
                // The exhaustion test lives inside the refill: it is reached
                // only when the current word runs dry, not on every element.
                while *bits == 0 {
                    if *remaining == 0 || *word >= *word_count {
                        *remaining = 0;
                        return;
                    }
                    *bits = read_u64_at(payload, WORDS_AT + *word as usize * 8);
                    *word += 1;
                }
            }
            CRange::Stride {
                next,
                stride,
                remaining,
            } => {
                *next += *stride;
                *remaining -= 1;
            }
            CRange::Cone { at, .. } | CRange::Array { at, .. } => *at += 1,
        }
    }

    /// Advance IN PLACE to the first member at or after `target`.
    ///
    /// Relative, and that is the whole point. [`CRange::at_rank`] repositions
    /// this cursor ABSOLUTELY — `rank_below` over the word plane, then a
    /// `select` back down the directory — which is the right shape for a jump
    /// that clears whole chunks and the wrong one for closing a three-member
    /// gap, where it costs tens of times a `pop_front` and buys nothing. An
    /// aligner that seeks unconditionally therefore LOSES on low-skew pairs,
    /// measured at 3.65x on the 54k FTS pair (G520). A cursor that advances
    /// from where it stands has no such cliff: the cost is the distance
    /// travelled in words, members or a closed form, never a re-descent.
    #[inline]
    fn seek(&mut self, target: u16) {
        if Range::empty(self) || Range::front(self) >= target {
            return;
        }
        match self {
            CRange::Runs {
                view,
                at,
                next,
                remaining,
            } => {
                let old_rank = view.before(*at) + *next - view.start(*at);
                *at = view.lower_bound(*at, target);
                if *at == view.count() {
                    *remaining = 0;
                    return;
                }
                *next = u32::from(target).max(view.start(*at));
                let new_rank = view.before(*at) + *next - view.start(*at);
                *remaining -= new_rank - old_rank;
            }
            CRange::Words {
                payload,
                base_word,
                word,
                word_count,
                bits,
                remaining,
            } => {
                // `bits` belongs to the absolute word `base_word + word - 1`.
                let here = base_word.wrapping_add(*word).wrapping_sub(1);
                let goal = u32::from(target >> 6);
                let mask = !((1u64 << (target & 63)) - 1);
                if goal == here {
                    *remaining = remaining.saturating_sub((*bits & !mask).count_ones());
                    *bits &= mask;
                } else {
                    // The refuter's MAX half short-circuits the walk below:
                    // a target above the chunk's last member exhausts this
                    // cursor from the head line alone, touching neither the
                    // word plane nor the directory. The MIN half has no role
                    // here — a cursor that is standing somewhere already
                    // holds a TIGHTER lower bound than the rider does, which
                    // is why only the absolute `CView::seek_rank` consults
                    // both.
                    //
                    // UNCHARGED, deliberately. `RefuterCensus` counts CHUNK
                    // VERDICTS reached by the absolute reposition, and a
                    // bound check on the chunk a cursor is already standing
                    // in is not one of those: charging it would double-count
                    // the census AND put a thread-local write on one arm of
                    // the aligner A/B that a shipped build compiles away, so
                    // the instrument would price the arm it is measuring.
                    if refuter_armed() {
                        let (_, max) = CView::member_bounds(payload);
                        if target > max {
                            *remaining = 0;
                            return;
                        }
                    }
                    // Leave the current word, then skip whole words. Each
                    // skipped word costs one load and one popcount, so the
                    // walk is bounded by the DISTANCE and not by the members.
                    *remaining = remaining.saturating_sub(bits.count_ones());
                    *bits = 0;
                    let Some(rel) = goal.checked_sub(*base_word) else {
                        return;
                    };
                    if rel >= *word_count {
                        *remaining = 0;
                        return;
                    }
                    while *word < rel {
                        let skipped = read_u64_at(payload, WORDS_AT + *word as usize * 8);
                        *remaining = remaining.saturating_sub(skipped.count_ones());
                        *word += 1;
                    }
                    let landing = read_u64_at(payload, WORDS_AT + rel as usize * 8);
                    *remaining = remaining.saturating_sub((landing & !mask).count_ones());
                    *bits = landing & mask;
                    *word = rel + 1;
                }
                // Same refill the step path runs, for the same reason: the
                // head must be materialised before `front` is asked for it.
                while *bits == 0 {
                    if *remaining == 0 || *word >= *word_count {
                        *remaining = 0;
                        return;
                    }
                    *bits = read_u64_at(payload, WORDS_AT + *word as usize * 8);
                    *word += 1;
                }
            }
            CRange::Stride {
                next,
                stride,
                remaining,
            } => {
                // Closed form: no words, no members, no directory.
                let steps = (u32::from(target) - *next).div_ceil(*stride);
                if steps >= *remaining {
                    *remaining = 0;
                    return;
                }
                *next += steps * *stride;
                *remaining -= steps;
            }
            CRange::Cone {
                first,
                scale,
                residuals,
                at,
            } => {
                // Ascending by construction, and `cone_value` is O(1) at any
                // index, so the landing ordinal is a bisection of the tail.
                let mut lo = *at;
                let mut hi = residuals.len();
                while lo < hi {
                    let mid = lo + (hi - lo) / 2;
                    if cone_value(*first, *scale, residuals, mid) < target {
                        lo = mid + 1;
                    } else {
                        hi = mid;
                    }
                }
                *at = lo;
            }
            CRange::Array { values, at } => {
                let mut lo = *at;
                let mut hi = values.len() / 2;
                while lo < hi {
                    let mid = lo + (hi - lo) / 2;
                    if read_u16_at(values, mid * 2) < target {
                        lo = mid + 1;
                    } else {
                        hi = mid;
                    }
                }
                *at = lo;
            }
        }
    }
}

/// Ascending cursor over a borrowed [`BitmosaicView`], produced by
/// [`BitmosaicView::range`].
///
/// Both roots are driven: a container forest walks chunk by chunk, and an
/// Elias-Fano root walks its high plane in one pass. Which one a set was
/// published as is a byte-count decision made at freeze, and a cursor is
/// no more a caller's business than a probe is.
///
/// Whole-set consumers should prefer [`BitmosaicView::for_each`], which
/// resolves the arm once per chunk; this cursor exists for merges, which
/// must advance one side at a time and cannot use internal iteration.
#[derive(Clone, Copy, Debug)]
pub struct BitmosaicViewRange<'a> {
    root: ViewCursor<'a>,
    remaining: u64,
}

#[derive(Clone, Copy, Debug)]
enum ViewCursor<'a> {
    Forest {
        forest: Forest<'a>,
        /// Next chunk to open; `inner` is the one currently open.
        chunk: u32,
        /// Chunk key of `inner`, shifted into place.
        high: u32,
        inner: CRange<'a>,
    },
    Ef(EfRange<'a>),
    /// No source at all — an empty plane, or the placeholder a reposition
    /// installs before opening a chunk.
    Done,
}

impl<'a> BitmosaicViewRange<'a> {
    /// The exhausted cursor, for a caller that needs a positioned value
    /// before it has a plane to position it on.
    #[inline]
    pub(crate) const fn done() -> BitmosaicViewRange<'a> {
        BitmosaicViewRange {
            root: ViewCursor::Done,
            remaining: 0,
        }
    }

    fn new(view: BitmosaicView<'a>) -> BitmosaicViewRange<'a> {
        let mut range = match view.root {
            Root::Ef(ef) => {
                // The cursor's own count, not the header's: an UNVERIFIED
                // plane whose first select runs dry starts exhausted.
                let range = ef.range();
                BitmosaicViewRange {
                    remaining: range.remaining(),
                    root: ViewCursor::Ef(range),
                }
            }
            Root::Forest(forest) => BitmosaicViewRange {
                remaining: forest.len,
                root: ViewCursor::Forest {
                    forest,
                    chunk: 0,
                    high: 0,
                    inner: CRange::DONE,
                },
            },
        };
        range.open_next();
        range
    }

    /// Members not yet consumed, the head included.
    ///
    /// At every non-empty position `view.len() - range.remaining()` is the
    /// zero-based ordinal of [`Range::front`], so a consumer that needs the
    /// rank of a member it is standing on pays no second descent.
    #[inline(always)]
    pub fn remaining(&self) -> u64 {
        self.remaining
    }

    /// Establish "`inner` is non-empty unless the whole range is".
    ///
    /// A frozen chunk is never empty and `BitmosaicView::open` already proved
    /// every directory entry resolves, so this opens exactly one container
    /// per call; the loop and the `None` arm are fail-closed guards.
    #[inline(always)]
    fn open_next(&mut self) {
        if self.remaining == 0 {
            return;
        }
        let ViewCursor::Forest {
            forest,
            chunk,
            high,
            inner,
        } = &mut self.root
        else {
            return;
        };
        while Range::empty(inner) {
            let at = *chunk as usize;
            let Some(container) = (at < forest.chunks as usize)
                .then(|| forest.container(at))
                .flatten()
            else {
                self.remaining = 0;
                return;
            };
            *high = u32::from(forest.key(at)) << 16;
            *inner = CRange::open(container);
            *chunk += 1;
        }
    }

    /// Advance to the first member at or after `value`; never moves back.
    ///
    /// TWO shapes, because a monotone cursor's seek serves two callers. A
    /// target inside the chunk already open advances that chunk's cursor IN
    /// PLACE, so closing a short gap costs a word walk, a bisection or a
    /// closed form — never a re-descent. A target beyond it walks the
    /// directory and pays at most one chunk-local `rank_below`, which is what
    /// makes a long leapfrog cost the distance travelled rather than the
    /// members skipped. A `Words` chunk the seek jumps clean over, or lands
    /// at the head of, costs no `rank_below` at all: its min/max refuter
    /// settles both from the head line.
    ///
    /// The in-place arm is not an optimisation of the absolute one, it is the
    /// arm that makes this seek usable by a MERGE. `bitmosaic::AndRange`'s
    /// leapfrog aligner calls seek on every head gap, and gaps in a
    /// low-skew pair are a few members wide: absolute repositioning alone
    /// measured 3.65x SLOWER than stepping on the 54k FTS pair (G520).
    pub fn seek(&mut self, value: u32) {
        if self.remaining == 0 || Range::front(self) >= value {
            return;
        }
        self.remaining = match &mut self.root {
            ViewCursor::Done => 0,
            ViewCursor::Ef(ef) => {
                ef.seek(value);
                ef.remaining()
            }
            ViewCursor::Forest {
                forest,
                chunk,
                high,
                inner,
            } => {
                if value >> 16 == *high >> 16 {
                    let before = inner.remaining();
                    Range::seek(inner, (value & 0xFFFF) as u16);
                    self.remaining - u64::from(before - inner.remaining())
                } else {
                    seek_forest(forest, chunk, high, inner, value)
                }
            }
        };
        self.open_next();
    }
}

/// Reposition a forest cursor at the first member `>= value`, returning the
/// members left including the new head.
///
/// Leaves `inner` either positioned, or empty with `chunk` naming the next
/// chunk to open — which is exactly the state `open_next` finishes.
fn seek_forest<'a>(
    forest: &Forest<'a>,
    chunk: &mut u32,
    high: &mut u32,
    inner: &mut CRange<'a>,
    value: u32,
) -> u64 {
    let n = forest.chunks as usize;
    let key = (value >> 16) as u16;
    let mut at = forest.walk(key);
    if at < n && forest.key(at) == key {
        let Some(container) = forest.container(at) else {
            *inner = CRange::DONE;
            return 0;
        };
        if let Some(rank) = container.seek_rank((value & 0xFFFF) as u16) {
            *high = u32::from(key) << 16;
            *inner = CRange::at_rank(container, rank);
            *chunk = at as u32 + 1;
            return forest.len - u64::from(forest.before(at)) - u64::from(rank);
        }
        // Every member of this chunk is below `value`; resume at the next.
        // For a `Words` chunk that verdict came from the head line alone.
        at += 1;
    }
    *inner = CRange::DONE;
    if at >= n {
        return 0;
    }
    *chunk = at as u32;
    forest.len - u64::from(forest.before(at))
}

impl Range for BitmosaicViewRange<'_> {
    type Item = u32;

    #[inline(always)]
    fn empty(&self) -> bool {
        self.remaining == 0
    }

    #[inline(always)]
    fn front(&self) -> u32 {
        debug_assert!(!Range::empty(self), "front on an empty range");
        match &self.root {
            ViewCursor::Forest { high, inner, .. } => *high | u32::from(Range::front(inner)),
            ViewCursor::Ef(ef) => ef.front(),
            ViewCursor::Done => 0,
        }
    }

    #[inline(always)]
    fn pop_front(&mut self) {
        debug_assert!(!Range::empty(self), "pop_front on an empty range");
        match &mut self.root {
            ViewCursor::Forest { inner, .. } => Range::pop_front(inner),
            ViewCursor::Ef(ef) => {
                ef.pop_front();
                // The plane's own count wins over the header's: an
                // UNVERIFIED plane that ran dry early ends the range here.
                if ef.empty() {
                    self.remaining = 0;
                    return;
                }
            }
            ViewCursor::Done => return,
        }
        self.remaining -= 1;
        self.open_next();
    }

    /// The inherent [`BitmosaicViewRange::seek`] — a directory walk plus at
    /// most one chunk-local `rank_below`. Published through the protocol so
    /// the lazy adaptors reach it: [`crate::AndRange`] is generic over
    /// [`Range`] and could only ever call the three methods the trait
    /// declared, which is the whole of its former cardinality blindness.
    #[inline(always)]
    fn seek(&mut self, target: u32) {
        BitmosaicViewRange::seek(self, target);
    }
}

impl Iterator for BitmosaicViewRange<'_> {
    type Item = u32;

    #[inline(always)]
    fn next(&mut self) -> Option<u32> {
        if Range::empty(self) {
            return None;
        }
        let value = Range::front(self);
        Range::pop_front(self);
        Some(value)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = usize::try_from(self.remaining).unwrap_or(usize::MAX);
        (n, Some(n))
    }
}

/// Ascending cursor over a borrowed [`Bitmosaic64View`], produced by
/// [`Bitmosaic64View::range`]: a walk over the high-32 directory with an
/// [`BitmosaicViewRange`] inside it and the prefix restored.
#[derive(Clone, Copy, Debug)]
pub struct Bitmosaic64ViewRange<'a> {
    view: Bitmosaic64View<'a>,
    /// Next forest to open; `inner` is the one currently open.
    forest: u32,
    /// High-32 prefix of `inner`, shifted into place.
    high: u64,
    inner: BitmosaicViewRange<'a>,
    remaining: u64,
}

impl<'a> Bitmosaic64ViewRange<'a> {
    fn new(view: Bitmosaic64View<'a>) -> Bitmosaic64ViewRange<'a> {
        let mut range = Bitmosaic64ViewRange {
            view,
            forest: 0,
            high: 0,
            inner: BitmosaicViewRange::done(),
            remaining: view.len,
        };
        range.open_next();
        range
    }

    /// Members not yet consumed, the head included. As with
    /// [`BitmosaicViewRange::remaining`], `view.len() - remaining()` is the
    /// head's zero-based ordinal.
    #[inline(always)]
    pub fn remaining(&self) -> u64 {
        self.remaining
    }

    #[inline(always)]
    fn open_next(&mut self) {
        while self.remaining != 0 && Range::empty(&self.inner) {
            let at = self.forest as usize;
            let Some(forest) = (at < self.view.forests as usize)
                .then(|| self.view.forest(at))
                .flatten()
            else {
                self.remaining = 0;
                return;
            };
            self.high = u64::from(self.view.high(at)) << 32;
            self.inner = forest.range();
            self.forest += 1;
        }
    }

    /// Advance to the first member at or after `value`; never moves back.
    ///
    /// Same two shapes as [`BitmosaicViewRange::seek`], one tier up. A target
    /// inside the high-32 forest already open advances that forest's cursor
    /// in place; anything further walks the high-32 directory and opens a
    /// fresh one. The in-place arm is what a merge needs: an intersection
    /// whose two sides live in ONE forest — every `u32`-domain set, which is
    /// every FTS posting list — would otherwise re-walk the directory and
    /// re-`select` for every head gap it closes.
    pub fn seek(&mut self, value: u64) {
        if self.remaining == 0 || Range::front(self) >= value {
            return;
        }
        let n = self.view.forests as usize;
        let high = (value >> 32) as u32;
        if u64::from(high) << 32 == self.high {
            let before = self.inner.remaining();
            self.inner.seek(value as u32);
            self.remaining -= before - self.inner.remaining();
            self.open_next();
            return;
        }
        let mut at = self.view.walk(high);
        if at < n && self.view.high(at) == high {
            if let Some(forest) = self.view.forest(at) {
                let mut inner = forest.range();
                inner.seek(value as u32);
                if !Range::empty(&inner) {
                    self.high = u64::from(high) << 32;
                    self.remaining =
                        self.view.len - self.view.before(at) - (forest.len() - inner.remaining());
                    self.inner = inner;
                    self.forest = at as u32 + 1;
                    return;
                }
            }
            // Every member of this forest is below `value`.
            at += 1;
        }
        self.inner = BitmosaicViewRange::done();
        if at >= n {
            self.remaining = 0;
            return;
        }
        self.remaining = self.view.len - self.view.before(at);
        self.forest = at as u32;
        self.open_next();
    }
}

impl Range for Bitmosaic64ViewRange<'_> {
    type Item = u64;

    #[inline(always)]
    fn empty(&self) -> bool {
        self.remaining == 0
    }

    #[inline(always)]
    fn front(&self) -> u64 {
        debug_assert!(!Range::empty(self), "front on an empty range");
        self.high | u64::from(Range::front(&self.inner))
    }

    #[inline(always)]
    fn pop_front(&mut self) {
        debug_assert!(!Range::empty(self), "pop_front on an empty range");
        Range::pop_front(&mut self.inner);
        self.remaining -= 1;
        self.open_next();
    }

    /// The inherent [`Bitmosaic64ViewRange::seek`] — a high-32 directory walk
    /// into one forest's own seek. This is the cursor `ptmcart`'s FTS
    /// postings intersect through, so it is the one whose absence from the
    /// protocol made the two-token AND emit path step `|A| + |B|` times.
    #[inline(always)]
    fn seek(&mut self, target: u64) {
        Bitmosaic64ViewRange::seek(self, target);
    }
}

impl Iterator for Bitmosaic64ViewRange<'_> {
    type Item = u64;

    #[inline(always)]
    fn next(&mut self) -> Option<u64> {
        if Range::empty(self) {
            return None;
        }
        let value = Range::front(self);
        Range::pop_front(self);
        Some(value)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = usize::try_from(self.remaining).unwrap_or(usize::MAX);
        (n, Some(n))
    }
}

// ── the Words min/max refuter ────────────────────────────────────────────
//
// In-crate because the assertions are about BYTES AT AN OFFSET and about
// which read path answered. The differential suite in `tests/wire_view.rs`
// proves the view still agrees with its oracles; what it structurally cannot
// prove is that `seek` consults the refuter at all — a `seek_rank` whose
// `Words` arm were deleted would pass every oracle in that file, because
// `rank_below` returns the same answer more slowly. The substitution witness
// below is the half that fails when the routing is removed.
#[cfg(test)]
mod refuter {
    use super::*;
    use crate::Bitmosaic;

    /// One chunk that ladders to `Words`, with slack at BOTH ends of the
    /// block-snapped window: the first member is 100 (the window snaps back
    /// to position 0) and the last sits inside its final block. That slack is
    /// precisely the region where `rank_below`'s window guards do not fire
    /// and the refuter does.
    ///
    /// DENSE, and inset from both ends of its chunk. Density is not a
    /// stylistic choice: `Words` is only published when it undercuts every
    /// other arm, and at low cardinality an Elias-Fano root or a `Cone` wins
    /// on bytes, which would hand this module a chunk with no refuter in it.
    /// Four positions in every five across `5_000..60_000` puts `Words` well
    /// under both, and leaves several hundred positions of block-snapped
    /// slack at each end for the refuter to be tighter than.
    /// Sized up front: `filter` erases `size_hint`'s lower bound, so a bare
    /// `collect` would start at capacity 0 and double its way up. The count
    /// is a closed form of the band and the keep ratio.
    fn gapped_chunk() -> Vec<u32> {
        const LO: u32 = 5_000;
        const HI: u32 = 60_000;
        const KEEP: u32 = 5;
        let mut values = Vec::with_capacity(((HI - LO) - (HI - LO) / KEEP) as usize);
        values.extend((LO..HI).filter(|v| v % KEEP != 0));
        values
    }

    fn words_payload(bytes: &[u8]) -> (usize, u32) {
        let view = BitmosaicView::open(bytes).expect("open");
        let Root::Forest(forest) = view.root else {
            panic!("expected a forest root")
        };
        let Some(CView::Words { payload, len, .. }) = forest.container(0) else {
            panic!("expected a Words chunk")
        };
        (payload.as_ptr() as usize - bytes.as_ptr() as usize, len)
    }

    #[test]
    fn refuter_is_the_exact_member_bounds_and_is_content_determined() {
        let values = gapped_chunk();
        let bytes = Bitmosaic::from_sorted(values.iter().copied()).to_bytes();
        // E7: the rider is derived from the block's members and nothing else,
        // so a second freeze of the same content is byte-identical.
        let again = Bitmosaic::from_sorted(values.iter().copied()).to_bytes();
        assert_eq!(bytes, again, "refuter made the encoding non-deterministic");

        let (at, card) = words_payload(&bytes);
        assert_eq!(card, values.len() as u32);
        let (min, max) = CView::member_bounds(&bytes[at..]);
        assert_eq!(u32::from(min), values[0], "min is not the first member");
        assert_eq!(
            u32::from(max),
            *values.last().unwrap(),
            "max is not the last"
        );
        // The bytes really are the old pad, not extra payload: they sit in
        // the 4 bytes before the word plane, and the head is still 24 B.
        assert_eq!(BOUNDS_AT + 4, WORDS_AT);
        assert_ne!(
            read_u32_at(&bytes[at..], BOUNDS_AT),
            0,
            "the pad still reads as zero - the rider was not written"
        );
    }

    /// The refuter is TIGHTER than the block-snapped window it rides beside.
    /// If it were not, `rank_below`'s existing guards would already answer
    /// every case the refuter claims and the rider would buy nothing.
    #[test]
    fn refuter_is_tighter_than_the_snapped_window() {
        let bytes = Bitmosaic::from_sorted(gapped_chunk().iter().copied()).to_bytes();
        let (at, _) = words_payload(&bytes);
        let payload = &bytes[at..];
        let (min, max) = CView::member_bounds(payload);
        let base_word = u32::from(read_u16_at(payload, 16));
        let word_count = u32::from(read_u16_at(payload, 18));
        let window_hi = (base_word + word_count) * 64;
        assert!(
            u32::from(max) + 1 < window_hi,
            "no slack above the last member: max {max} vs window end {window_hi}"
        );
        assert!(min >= (base_word * 64) as u16);
    }

    /// A SECOND `Words` chunk above the first, so a seek can be made to
    /// enter a chunk the cursor is not standing in.
    ///
    /// Both refuter halves are consulted, but by different seek shapes, and
    /// one fixture cannot witness both: an in-place advance inside the open
    /// chunk needs no `min` (its own head is a tighter lower bound), while a
    /// cross-chunk jump resolves an absolute rank and needs both.
    fn two_gapped_chunks() -> Vec<u32> {
        let mut values = gapped_chunk();
        values.extend((70_000..125_000).filter(|v| v % 5 != 0));
        values
    }

    fn words_payload_at(bytes: &[u8], index: usize) -> usize {
        let view = BitmosaicView::open(bytes).expect("open");
        let Root::Forest(forest) = view.root else {
            panic!("expected a forest root")
        };
        let Some(CView::Words { payload, .. }) = forest.container(index) else {
            panic!("expected a Words chunk at {index}")
        };
        payload.as_ptr() as usize - bytes.as_ptr() as usize
    }

    /// PH-A3b routing witness. Corrupting ONLY the refuter must change what
    /// `seek` returns; if `seek` fell back to `rank_below` the corrupted
    /// bytes would be inert and this test would fail. The corruption is a
    /// lie the read path is entitled to believe — that is the point — which
    /// is why the lying planes go through `open` (structural checks only)
    /// and not `open_verified`, which would refuse a refuter that disagrees
    /// with its words before `seek` could be observed believing it.
    ///
    /// Three cases, because `seek` has two shapes and the rider serves them
    /// differently. The IN-PLACE advance inside the open chunk consults the
    /// `max` half only; the CROSS-CHUNK jump resolves an absolute rank and
    /// consults both. Witnessing only the first would let the absolute arm's
    /// routing rot; witnessing only the second would let the in-place arm
    /// walk the whole word plane to discover what the head line already said.
    #[test]
    fn seek_reads_the_refuter_and_not_the_word_plane() {
        let values = gapped_chunk();
        let bytes = Bitmosaic::from_sorted(values.iter().copied()).to_bytes();
        let (at, _) = words_payload(&bytes);
        let probe = values[values.len() / 2];

        let honest = {
            let view = BitmosaicView::open(&bytes).expect("open");
            let mut cursor = view.range();
            cursor.seek(probe);
            cursor.next()
        };
        assert_eq!(honest, Some(probe), "the honest seek missed its member");

        // IN-PLACE arm. Claim the chunk ends BELOW the probe: the cursor is
        // already standing in this chunk, so the advance is relative, and the
        // falsified max must exhaust it from the head line — with one chunk
        // in the forest the walk ends.
        let mut lying = bytes.clone();
        lying[at + BOUNDS_AT + 2..at + BOUNDS_AT + 4]
            .copy_from_slice(&(probe as u16 - 1).to_le_bytes());
        let view = BitmosaicView::open(&lying).expect("open");
        let mut cursor = view.range();
        cursor.seek(probe);
        assert_eq!(
            cursor.next(),
            None,
            "in-place seek ignored a falsified max - refuter unrouted"
        );

        // CROSS-CHUNK arm. The cursor stands in chunk 0 and seeks into chunk
        // 1, which is the shape that resolves an absolute rank.
        let values = two_gapped_chunks();
        let bytes = Bitmosaic::from_sorted(values.iter().copied()).to_bytes();
        let upper: Vec<u32> = values.iter().copied().filter(|v| *v >= 65_536).collect();
        let at = words_payload_at(&bytes, 1);
        let probe = upper[upper.len() / 2];
        let local = (probe & 0xFFFF) as u16;

        let honest = {
            let view = BitmosaicView::open(&bytes).expect("open");
            let mut cursor = view.range();
            cursor.seek(probe);
            cursor.next()
        };
        assert_eq!(honest, Some(probe), "the honest cross-chunk seek missed");

        // Claim chunk 1 STARTS at or above the probe: the refuter must hand
        // back rank 0, i.e. that chunk's real first member.
        let mut lying = bytes.clone();
        lying[at + BOUNDS_AT..at + BOUNDS_AT + 2].copy_from_slice(&local.to_le_bytes());
        let view = BitmosaicView::open(&lying).expect("open");
        let mut cursor = view.range();
        cursor.seek(probe);
        assert_eq!(
            cursor.next(),
            Some(upper[0]),
            "cross-chunk seek ignored a falsified min - refuter unrouted"
        );

        // Claim chunk 1 ends BELOW the probe: the refuter must skip it, and
        // with no chunk above it the walk ends.
        let mut lying = bytes.clone();
        lying[at + BOUNDS_AT + 2..at + BOUNDS_AT + 4].copy_from_slice(&(local - 1).to_le_bytes());
        let view = BitmosaicView::open(&lying).expect("open");
        let mut cursor = view.range();
        cursor.seek(probe);
        assert_eq!(
            cursor.next(),
            None,
            "cross-chunk seek ignored a falsified max - refuter unrouted"
        );
    }
}
