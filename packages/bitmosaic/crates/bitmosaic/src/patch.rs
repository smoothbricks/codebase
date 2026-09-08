//! In-place patching of a native bitmosaic u32 image.
//!
//! A frozen set lives in a fixed-capacity slot as its canonical bytes, and
//! the slot's writes are batches: add these values, remove those. The
//! seal-once model would restage the whole set for every batch, which is
//! O(members) for a one-element write. This module edits the string where
//! it lies: only the chunks a batch touches are opened into a word plane,
//! re-laddered and re-emitted; every other chunk stays bytes and moves as
//! one rigid block if a neighbour's footprint changed.
//!
//! A patch keeps the string's root arm. The forest-or-Elias-Fano decision
//! is a byte-optimality choice made when a plane is SEALED; a slot that
//! mutates picks its root once — the forest, whose patches touch only the
//! chunks a batch names ([`crate::Bitmosaic::write_forest_into_slice`]) — and
//! a patch that could flip the root would have to price the other arm on
//! every batch, which for an Elias-Fano root is a walk over the whole set,
//! to save bytes no mutating caller asked for. The one exception is forced
//! by representability: an Elias-Fano root left with fewer than two members
//! becomes a forest, the only root that holds them.
//!
//! Within its root the result is BYTE-EQUAL to a fresh encoding of the
//! surviving set. That is not a convention but a consequence: the per-chunk
//! ladder is [`crate::choose_arm`], the same function freeze decides
//! through, and the payload emitters and the directory writer are the
//! seal-time writer's own.
//!
//! Nothing here allocates on a warm [`PatchScratch`]: the plane, the member
//! buffer and the residual buffer are fixed; the record and byte buffers
//! keep their high-water mark, and [`PatchScratch::with_capacity`] sizes
//! them up front from the slot capacity so the steady path never grows
//! them at all.
//!
//! The slot is never left half-written. Everything a patch needs to know
//! before it commits — the new length, whether the slot can hold it — is
//! computed into the scratch first; a refusal returns with the string
//! exactly as it was.

use core::fmt;

use crate::ef::{self, EfLayout, EfView, SAMPLE_STRIDE};
use crate::wire::{
    BOUNDS_AT, BitmosaicView, CView, ChunkRow, EMPTY_U32_IMAGE, Forest, Geometry, IMAGE_ID_LEN,
    K_ARRAY, K_CONE, K_RUNS, K_STRIDE, K_WORDS, KeyWidth, Root, WORDS_AT, cone_value,
    ef_header_len, emit_array, emit_cone, emit_runs, emit_stride, emit_words, forest_header_len,
    frame_len, image_header, read_u16_at, read_u32_at, read_u64_at, runs_payload_len,
    slot_bits_for_span, uleb_len, word_window, words_payload_len, write_directory, write_ef_header,
    write_forest_header, write_kind,
};
use crate::{
    ARRAY_MAX, Arm, BLOCK_WORDS, CHUNK_WORDS, CONE_MAX_LEN, DIR_STRIDE, SUMMARY_WORDS, choose_arm,
    cone_residual,
};

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

/// What a patch did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PatchReport {
    /// Values from `adds` that were NOT already members.
    pub added: u32,
    /// Values from `removes` that WERE members.
    pub removed: u32,
    /// Cardinality after the patch.
    pub len: u64,
    /// Bytes the string occupies at the head of the slot after the patch.
    pub serialized_len: usize,
}

/// Where a patch reports, value by value, what it changed: the index into
/// `adds` of every value that was NOT already a member, and the index into
/// `removes` of every value that WAS one (a value both added and removed in
/// one batch is new on the add side and held on the remove side — adds land
/// first). The forest root answers from the edit itself — the old bit a
/// plane or word edit overwrites — so a caller that needs the per-value
/// answer pays no second probe; see [`patch_witnessed`].
pub trait Witness {
    /// `true` for a witness that records: a root that cannot answer as a
    /// byproduct probes for it, and skips the probes when nothing listens.
    const LISTENS: bool;
    fn add_new(&mut self, i: usize);
    fn remove_held(&mut self, i: usize);
    /// Every add in `range` was new — a whole word run landing on empty
    /// bits, which a batch of fresh values is made of; the default is the
    /// per-value form.
    #[inline(always)]
    fn adds_new(&mut self, range: core::ops::Range<usize>) {
        for i in range {
            self.add_new(i);
        }
    }
    /// Every remove in `range` was held.
    #[inline(always)]
    fn removes_held(&mut self, range: core::ops::Range<usize>) {
        for i in range {
            self.remove_held(i);
        }
    }
}

impl Witness for () {
    const LISTENS: bool = false;
    #[inline(always)]
    fn add_new(&mut self, _i: usize) {}
    #[inline(always)]
    fn remove_held(&mut self, _i: usize) {}
    #[inline(always)]
    fn adds_new(&mut self, _range: core::ops::Range<usize>) {}
    #[inline(always)]
    fn removes_held(&mut self, _range: core::ops::Range<usize>) {}
}

/// Set bits `lo..hi` of a bit plane, whole words at a time.
#[inline]
fn set_bits(plane: &mut [u64], lo: usize, hi: usize) {
    if lo >= hi {
        return;
    }
    let (w0, w1) = (lo / 64, (hi - 1) / 64);
    let head = u64::MAX << (lo % 64);
    let tail = u64::MAX >> (63 - (hi - 1) % 64);
    if w0 == w1 {
        plane[w0] |= head & tail;
    } else {
        plane[w0] |= head;
        for word in &mut plane[w0 + 1..w1] {
            *word = u64::MAX;
        }
        plane[w1] |= tail;
    }
}

/// A [`Witness`] over two caller-owned bit planes, one bit per batch value:
/// bit `i` of `adds_new` is set when `adds[i]` was new, bit `i` of
/// `removes_held` when `removes[i]` was held. Both planes are cleared when
/// the outcome is built, so a patch that changes nothing leaves them zero.
pub struct BatchOutcome<'a> {
    adds_new: &'a mut [u64],
    removes_held: &'a mut [u64],
}

impl<'a> BatchOutcome<'a> {
    /// `adds_new` holds at least `adds.len().div_ceil(64)` words and
    /// `removes_held` at least `removes.len().div_ceil(64)`; a shorter
    /// plane is a caller bug and panics at the first bit past its end.
    pub fn new(adds_new: &'a mut [u64], removes_held: &'a mut [u64]) -> Self {
        adds_new.fill(0);
        removes_held.fill(0);
        BatchOutcome {
            adds_new,
            removes_held,
        }
    }

    #[inline(always)]
    pub fn add_was_new(&self, i: usize) -> bool {
        self.adds_new[i / 64] >> (i % 64) & 1 == 1
    }

    #[inline(always)]
    pub fn remove_was_held(&self, i: usize) -> bool {
        self.removes_held[i / 64] >> (i % 64) & 1 == 1
    }
}

impl Witness for BatchOutcome<'_> {
    const LISTENS: bool = true;
    #[inline(always)]
    fn add_new(&mut self, i: usize) {
        self.adds_new[i / 64] |= 1u64 << (i % 64);
    }
    #[inline(always)]
    fn remove_held(&mut self, i: usize) {
        self.removes_held[i / 64] |= 1u64 << (i % 64);
    }
    #[inline(always)]
    fn adds_new(&mut self, range: core::ops::Range<usize>) {
        set_bits(self.adds_new, range.start, range.end);
    }
    #[inline(always)]
    fn removes_held(&mut self, range: core::ops::Range<usize>) {
        set_bits(self.removes_held, range.start, range.end);
    }
}

/// One chunk of a forest being rebuilt: where its payload bytes come from
/// and what its directory record says.
#[derive(Clone, Copy, Debug)]
struct Row {
    key: u16,
    card: u32,
    kind: u32,
    len: u32,
    /// Payload source: the old string when `staged` is false, the scratch's
    /// staged region when true.
    staged: bool,
    src: u32,
}

/// The patched form of one touched chunk: re-laddered and emitted into the
/// staged region, or — a `Words` chunk that stays `Words` on the same
/// window — edited where it lies, in which case `edits` names the batch
/// ranges to apply and `len` is the old payload length.
#[derive(Clone, Copy, Debug)]
struct Fresh {
    kind: u32,
    card: u32,
    len: u32,
    staged: bool,
    staged_at: u32,
    /// `adds[a0..a1]` and `removes[r0..r1]` of this chunk.
    edits: (u32, u32, u32, u32),
    /// For a `Words` chunk edited where it lies: the window the result
    /// occupies, `(base_word, word_count)`, which may extend the old one.
    window: (u16, u16),
}

/// A chunk of the old string: directory index, payload length,
/// cardinality and payload offset.
#[derive(Clone, Copy, Debug)]
struct Old {
    index: u32,
    len: u32,
    card: u32,
    at: u32,
}

/// A touched chunk: what it was in the old string, what it becomes.
#[derive(Clone, Copy, Debug)]
struct Touched {
    key: u16,
    /// `None` for a chunk the batch creates.
    old: Option<Old>,
    /// `None` when the chunk emptied.
    fresh: Option<Fresh>,
}

/// Reusable working memory for [`patch`]. Keep one per writer and reuse it
/// across batches: on the steady path it allocates nothing.
pub struct PatchScratch {
    plane: Box<[u64; CHUNK_WORDS]>,
    /// Exactly `ARRAY_MAX` deep: the ladder only needs members in hand
    /// while an Array or Cone is a candidate.
    members: Vec<u16>,
    runs: Vec<crate::Run>,
    residuals: Box<[i8; CONE_MAX_LEN]>,
    touched: Vec<Touched>,
    rows: Vec<Row>,
    /// Tightly packed, re-emitted payloads of touched chunks.
    staged: Vec<u8>,
    /// A whole rebuilt string, when the patch cannot be applied in place.
    build: Vec<u8>,
    /// Per directory group, the change in popcount a `Words` in-place edit
    /// made: the directory is then one prefix sum, not a rebuild.
    group_delta: Box<[i32; CHUNK_WORDS / DIR_STRIDE]>,
}

impl Default for PatchScratch {
    fn default() -> Self {
        Self::new()
    }
}

impl PatchScratch {
    pub fn new() -> Self {
        PatchScratch {
            plane: Box::new([0u64; CHUNK_WORDS]),
            members: Vec::with_capacity(ARRAY_MAX),
            runs: Vec::with_capacity((crate::WORDS_HEAP_BYTES - 1) / 6),
            residuals: Box::new([0i8; CONE_MAX_LEN]),
            touched: Vec::new(),
            rows: Vec::new(),
            staged: Vec::new(),
            build: Vec::new(),
            group_delta: Box::new([0; CHUNK_WORDS / DIR_STRIDE]),
        }
    }

    /// Pre-size the growable buffers for a slot of `slot_bytes` capacity and
    /// batches touching up to `touched_chunks` distinct chunks, so the first
    /// patch is already on the steady path. Both bounds are closed forms of
    /// the caller's own contract: a rebuilt string never exceeds the slot
    /// that must hold it, a chunk row costs the string at least 10
    /// directory bytes, and a batch cannot touch more chunks than it has
    /// values.
    pub fn with_capacity(slot_bytes: usize, touched_chunks: usize) -> Self {
        let mut scratch = Self::new();
        scratch.touched.reserve(touched_chunks);
        scratch.rows.reserve(touched_chunks + slot_bytes / 10);
        scratch.staged.reserve(slot_bytes);
        scratch.build.reserve(slot_bytes);
        scratch
    }
}

/// Grow a retained buffer to at least `len` bytes. Bytes already there are
/// kept as they are: a caller writes every byte it will read back, so the
/// buffer is never cleared or zeroed on the steady path.
#[inline]
fn grow(buffer: &mut Vec<u8>, len: usize) {
    if buffer.len() < len {
        buffer.resize(len, 0);
    }
}

fn strictly_ascending(values: &[u32]) -> bool {
    values.windows(2).all(|w| w[0] < w[1])
}

/// Patch the native u32 image at the head of `slot`: the set becomes
/// `(old ∪ adds) \ removes`. `adds` and `removes` are each strictly
/// ascending. `slot.len()` is the capacity; the string occupies its first
/// `PatchReport::serialized_len` bytes afterwards.
///
/// The root arm is kept. On a forest root only the chunks the batch
/// touches are re-laddered; with the chunk set unchanged the edit is in
/// place — a touched chunk's payload is rewritten and the untouched tail
/// moved once — and otherwise the string is rebuilt into the scratch and
/// copied back. An Elias-Fano root is spliced at word granularity, or
/// re-encoded from a merge of its cursor with the batch when its layout
/// constants move.
///
/// Refusals leave the slot untouched: [`PatchError::Capacity`] names the
/// length the patched string needs, [`PatchError::Malformed`] a slot that
/// holds no string, [`PatchError::Unsorted`] a batch out of order.
pub fn patch(
    slot: &mut [u8],
    adds: &[u32],
    removes: &[u32],
    scratch: &mut PatchScratch,
) -> Result<PatchReport, PatchError> {
    patch_witnessed(slot, adds, removes, scratch, &mut ())
}

/// [`patch`] that also reports, through `witness`, which adds were new and
/// which removes were held — the per-value form of the report's counts.
/// On a forest root every answer is the byproduct of the edit that decides
/// it (the old bit a plane or word edit overwrites); an Elias-Fano root,
/// which is not a slot's mutating form, probes the old plane once per
/// value before it is spliced. The witness is meaningful only on `Ok`: a
/// refusal may have reported part of a batch before refusing.
pub fn patch_witnessed<W: Witness>(
    slot: &mut [u8],
    adds: &[u32],
    removes: &[u32],
    scratch: &mut PatchScratch,
    witness: &mut W,
) -> Result<PatchReport, PatchError> {
    if !strictly_ascending(adds) || !strictly_ascending(removes) {
        return Err(PatchError::Unsorted);
    }
    let header = image_header(slot).ok_or(PatchError::Malformed)?;
    if header.width != KeyWidth::U32 {
        return Err(PatchError::Malformed);
    }
    if slot[3] & 2 != 0 {
        patch_ef(slot, adds, removes, scratch, witness)
    } else {
        patch_forest(slot, adds, removes, scratch, witness)
    }
}

// ── staging one chunk ────────────────────────────────────────────────────

/// Load a frozen container into the plane, which is zero on entry.
fn stage(plane: &mut [u64; CHUNK_WORDS], container: CView<'_>) {
    match container {
        CView::Runs(runs) => {
            for i in 0..runs.count() {
                fill_range(plane, runs.start(i) as usize, runs.end(i) as usize + 1);
            }
        }
        CView::Words {
            payload,
            base_word,
            word_count,
            ..
        } => {
            let base = base_word as usize;
            for w in 0..word_count as usize {
                plane[base + w] = read_u64_at(payload, WORDS_AT + w * 8);
            }
        }
        CView::Array(values) => {
            for i in 0..values.len() / 2 {
                set_bit(plane, read_u16_at(values, 2 * i));
            }
        }
        CView::Stride { first, stride, len } => {
            let n = usize::from(len) + 1;
            if stride == 1 {
                fill_range(plane, usize::from(first), usize::from(first) + n);
            } else {
                let (first, stride) = (usize::from(first), usize::from(stride));
                for i in 0..n {
                    set_bit(plane, (first + i * stride) as u16);
                }
            }
        }
        CView::Cone {
            first,
            scale,
            residuals,
            ..
        } => {
            for i in 0..residuals.len() {
                set_bit(plane, cone_value(first, scale, residuals, i));
            }
        }
    }
}

#[inline(always)]
fn set_bit(plane: &mut [u64; CHUNK_WORDS], v: u16) {
    plane[usize::from(v >> 6)] |= 1u64 << (v & 63);
}

/// Set every bit in `lo..hi` (chunk positions, `hi <= 65_536`).
fn fill_range(plane: &mut [u64; CHUNK_WORDS], lo: usize, hi: usize) {
    if lo >= hi {
        return;
    }
    let (wl, wh) = (lo / 64, (hi - 1) / 64);
    let head = u64::MAX << (lo % 64);
    let tail = u64::MAX >> (63 - (hi - 1) % 64);
    if wl == wh {
        plane[wl] |= head & tail;
        return;
    }
    plane[wl] |= head;
    plane[wl + 1..wh].fill(u64::MAX);
    plane[wh] |= tail;
}

/// Apply one chunk's adds and removes to the plane, returning how many
/// were fresh and how many were held.
fn apply<W: Witness>(
    plane: &mut [u64; CHUNK_WORDS],
    adds: &[u32],
    removes: &[u32],
    (a0, r0): (usize, usize),
    witness: &mut W,
) -> (u32, u32) {
    let (mut added, mut removed) = (0u32, 0u32);
    for (i, &v) in adds.iter().enumerate() {
        let low = (v & 0xFFFF) as u16;
        let word = &mut plane[usize::from(low >> 6)];
        let mask = 1u64 << (low & 63);
        if *word & mask == 0 {
            added += 1;
            witness.add_new(a0 + i);
        }
        *word |= mask;
    }
    for (i, &v) in removes.iter().enumerate() {
        let low = (v & 0xFFFF) as u16;
        let word = &mut plane[usize::from(low >> 6)];
        let mask = 1u64 << (low & 63);
        if *word & mask != 0 {
            removed += 1;
            witness.remove_held(r0 + i);
        }
        *word &= !mask;
    }
    (added, removed)
}

/// Merge one chunk's adds and removes with its old `Array` payload (`u16`
/// little-endian members, ascending) into `members`, ascending, as a
/// sequence: no plane is zeroed, staged or extracted for a chunk that is
/// a sequence before and can only be one after. Returns how many adds were
/// fresh and how many removes were held, and tells the witness which;
/// adds land first, removes act on the result, exactly as [`apply`].
fn merge_array<W: Witness>(
    old: &[u8],
    adds: &[u32],
    removes: &[u32],
    (a0, r0): (usize, usize),
    witness: &mut W,
    members: &mut Vec<u16>,
) -> (u32, u32) {
    members.clear();
    let old_len = old.len() / 2;
    let (mut o, mut a, mut r) = (0usize, 0usize, 0usize);
    let (mut added, mut removed) = (0u32, 0u32);
    loop {
        let member = (o < old_len).then(|| read_u16_at(old, 2 * o));
        let add = adds.get(a).map(|v| (*v & 0xFFFF) as u16);
        let candidate = match (member, add) {
            (None, None) => break,
            (Some(m), Some(v)) if v == m => {
                a += 1;
                o += 1;
                m
            }
            (Some(m), Some(v)) if v > m => {
                o += 1;
                m
            }
            (Some(m), None) => {
                o += 1;
                m
            }
            (_, Some(v)) => {
                witness.add_new(a0 + a);
                added += 1;
                a += 1;
                v
            }
        };
        // Removes below the candidate match nothing: every member left is
        // at or above it.
        while r < removes.len() && ((removes[r] & 0xFFFF) as u16) < candidate {
            r += 1;
        }
        if r < removes.len() && (removes[r] & 0xFFFF) as u16 == candidate {
            witness.remove_held(r0 + r);
            removed += 1;
            r += 1;
            continue;
        }
        members.push(candidate);
    }
    (added, removed)
}

/// Dense finalization shares the owned chooser without extracting members.
fn dense_arm(plane: &[u64; CHUNK_WORDS], first: usize, n: usize) -> Arm {
    crate::choose_dense_arm(plane, first, n)
}

/// Extract the plane's members into `members` (cleared first).
fn extract(plane: &[u64; CHUNK_WORDS], members: &mut Vec<u16>) {
    members.clear();
    for (w, &word) in plane.iter().enumerate() {
        let mut bits = word;
        while bits != 0 {
            members.push((w * 64 + bits.trailing_zeros() as usize) as u16);
            bits &= bits - 1;
        }
    }
}

fn kind_of(arm: Arm) -> u32 {
    match arm {
        Arm::Runs { .. } => K_RUNS,
        Arm::Words => K_WORDS,
        Arm::Stride { .. } => K_STRIDE,
        Arm::Cone { .. } => K_CONE,
        Arm::Array => K_ARRAY,
    }
}

/// Ladder the plane (holding `card >= 1` members) and emit the chosen
/// payload into the retained staged region.
fn finish(scratch: &mut PatchScratch, card: u32) -> Fresh {
    let n = card as usize;
    let plane = &*scratch.plane;
    let arm = if n <= ARRAY_MAX {
        extract(plane, &mut scratch.members);
        choose_arm(&scratch.members, true)
    } else {
        let (_, _, first, _) = word_window(plane);
        dense_arm(plane, usize::from(first), n)
    };
    if matches!(arm, Arm::Runs { .. }) {
        prepare_runs(scratch, card);
    }
    emit_arm(scratch, arm, card)
}

/// Ladder `scratch.members` (ascending, `1..=ARRAY_MAX` of them) and emit
/// the chosen payload: the member form of [`finish`], for a chunk merged
/// as a sequence rather than staged on the plane. Array costs at most
/// `2 * ARRAY_MAX` bytes, less than Words, so omitting the unbacked Words
/// candidate preserves the plane path's verdict and bytes.
fn finish_members(scratch: &mut PatchScratch, card: u32) -> Fresh {
    debug_assert_eq!(scratch.members.len(), card as usize);
    debug_assert!((1..=ARRAY_MAX).contains(&scratch.members.len()));
    debug_assert!(scratch.members.windows(2).all(|pair| pair[0] < pair[1]));
    let arm = choose_arm(&scratch.members, false);
    if matches!(arm, Arm::Runs { .. }) {
        prepare_runs(scratch, card);
    }
    emit_arm(scratch, arm, card)
}

fn prepare_runs(scratch: &mut PatchScratch, card: u32) {
    scratch.runs.clear();
    if card as usize <= ARRAY_MAX {
        let mut begin = 0;
        for i in 1..=scratch.members.len() {
            if i == scratch.members.len()
                || u32::from(scratch.members[i]) != u32::from(scratch.members[i - 1]) + 1
            {
                scratch.runs.push(crate::Run {
                    start: scratch.members[begin],
                    end: scratch.members[i - 1],
                    before: begin as u16,
                });
                begin = i;
            }
        }
    } else {
        let mut before = 0u32;
        crate::for_each_word_run(&scratch.plane, |start, end| {
            scratch.runs.push(crate::Run {
                start: start as u16,
                end: (end - 1) as u16,
                before: before as u16,
            });
            before += end - start;
        });
    }
}

/// Membership witnesses and surviving maximal intervals come from one sweep.
fn merge_interval_events<W: Witness>(
    container: CView<'_>,
    adds: &[u32],
    removes: &[u32],
    (a0, r0): (usize, usize),
    scratch: &mut PatchScratch,
    witness: &mut W,
) -> (u32, u32, Option<Fresh>) {
    let count = match container {
        CView::Runs(runs) => runs.count(),
        CView::Stride { stride: 1, .. } => 1,
        _ => unreachable!("interval sweep requires an interval container"),
    };
    let bounds = |i| match container {
        CView::Runs(runs) => (runs.start(i), runs.end(i) + 1),
        CView::Stride { first, len, .. } => {
            (u32::from(first), u32::from(first) + u32::from(len) + 1)
        }
        _ => unreachable!("interval sweep requires an interval container"),
    };
    scratch.runs.clear();
    let (mut dense, mut len, mut added, mut removed) = (false, 0u32, 0u32, 0u32);
    let (mut i, mut a, mut r) = (0, 0, 0);
    let mut left = bounds(0).0;
    let mut emit = |start, end| {
        crate::append_interval(
            &mut scratch.runs,
            &mut scratch.plane,
            &mut dense,
            &mut len,
            start,
            end,
        )
    };
    while a < adds.len() || r < removes.len() {
        let add = adds.get(a).map_or(65_536, |value| value & 0xffff);
        let remove = removes.get(r).map_or(65_536, |value| value & 0xffff);
        let value = add.min(remove);
        while i < count && bounds(i).1 <= value {
            emit(left, bounds(i).1);
            i += 1;
            if i < count {
                left = bounds(i).0;
            }
        }
        let old = i < count && left <= value;
        if old {
            emit(left, value);
            left = value + 1;
            if left == bounds(i).1 {
                i += 1;
                if i < count {
                    left = bounds(i).0;
                }
            }
        }
        let is_add = add == value;
        let is_remove = remove == value;
        if is_add {
            if !old {
                witness.add_new(a0 + a);
                added += 1;
            }
            a += 1;
        }
        if is_remove {
            if old || is_add {
                witness.remove_held(r0 + r);
                removed += 1;
            }
            r += 1;
        } else if old || is_add {
            emit(value, value + 1);
        }
    }
    while i < count {
        emit(left, bounds(i).1);
        i += 1;
        if i < count {
            left = bounds(i).0;
        }
    }
    let fresh = if len == 0 {
        None
    } else if dense {
        Some(finish(scratch, len))
    } else if len as usize <= ARRAY_MAX {
        scratch.members.clear();
        for run in &scratch.runs {
            scratch.members.extend(run.start..=run.end);
        }
        Some(finish_members(scratch, len))
    } else {
        let arm = if scratch.runs.len() == 1 {
            Arm::Stride {
                first: scratch.runs[0].start,
                stride: 1,
            }
        } else {
            crate::choose_candidates(
                len as usize,
                Some((crate::WORDS_HEAP_BYTES, Arm::Words)),
                None,
                None,
                scratch.runs.len(),
            )
        };
        Some(emit_arm(scratch, arm, len))
    };
    (added, removed, fresh)
}

/// Emit `arm` for the chunk staged in `scratch` — the plane for `Words`,
/// retained members/runs for the sparse arms — into the staged region.
fn emit_arm(scratch: &mut PatchScratch, arm: Arm, card: u32) -> Fresh {
    let n = card as usize;
    let plane = &*scratch.plane;
    let len = match arm {
        Arm::Runs { count } => runs_payload_len(count),
        Arm::Stride { .. } => 4,
        Arm::Cone { .. } => 11 + n,
        Arm::Array => 2 * n,
        Arm::Words => {
            let (lo, hi, _, _) = word_window(plane);
            words_payload_len(hi - lo)
        }
    };
    let at = scratch.staged.len();
    scratch.staged.resize(at + len, 0);
    let out = &mut scratch.staged[at..at + len];
    match arm {
        Arm::Runs { count } => {
            debug_assert_eq!(scratch.runs.len(), count);
            emit_runs(out, &scratch.runs);
        }
        Arm::Stride { first, stride } => emit_stride(out, first, stride),
        Arm::Cone { first, scale, eps } => {
            for (i, (slot, &v)) in scratch
                .residuals
                .iter_mut()
                .zip(&scratch.members)
                .enumerate()
            {
                *slot = cone_residual(first, scale, i, v);
            }
            emit_cone(out, first, scale, eps, &scratch.residuals[..n]);
        }
        Arm::Array => emit_array(out, &scratch.members),
        Arm::Words => emit_words(out, plane),
    }
    Fresh {
        kind: kind_of(arm),
        card,
        len: len as u32,
        staged: true,
        staged_at: at as u32,
        edits: (0, 0, 0, 0),
        window: (0, 0),
    }
}

/// Payload length of a frozen container, from its view.
fn view_len(container: CView<'_>) -> u32 {
    container.payload_len() as u32
}

fn view_kind(container: CView<'_>) -> u32 {
    match container {
        CView::Runs(_) => K_RUNS,
        CView::Stride { .. } => K_STRIDE,
        CView::Cone { .. } => K_CONE,
        CView::Array(_) => K_ARRAY,
        CView::Words { .. } => K_WORDS,
    }
}

/// Where chunk `i`'s payload begins.
#[inline]
fn payload_at(bytes: &[u8], geometry: Geometry, i: usize) -> usize {
    geometry.payload_at
        + if i == 0 {
            0
        } else {
            read_u32_at(bytes, geometry.desc_at + 4 * (i - 1)) as usize
        }
}

// ── the batch, grouped by chunk ──────────────────────────────────────────

/// Walk `adds` and `removes` together by chunk key: each step yields a key
/// and the index ranges of both batches that fall in it.
struct ByChunk<'a> {
    adds: &'a [u32],
    removes: &'a [u32],
    a: usize,
    r: usize,
}

impl ByChunk<'_> {
    fn new<'a>(adds: &'a [u32], removes: &'a [u32]) -> ByChunk<'a> {
        ByChunk {
            adds,
            removes,
            a: 0,
            r: 0,
        }
    }
}

impl Iterator for ByChunk<'_> {
    type Item = (u16, core::ops::Range<usize>, core::ops::Range<usize>);

    fn next(&mut self) -> Option<Self::Item> {
        let (adds, removes) = (&self.adds[self.a..], &self.removes[self.r..]);
        let key = match (adds.first(), removes.first()) {
            (None, None) => return None,
            (Some(a), None) => a >> 16,
            (None, Some(r)) => r >> 16,
            (Some(a), Some(r)) => (a >> 16).min(r >> 16),
        };
        let na = adds.partition_point(|v| v >> 16 <= key);
        let nr = removes.partition_point(|v| v >> 16 <= key);
        let (a, r) = (self.a, self.r);
        self.a += na;
        self.r += nr;
        Some((key as u16, a..a + na, r..r + nr))
    }
}

// ── Words, edited where it lies ──────────────────────────────────────────
//
// A `Words` chunk that stays `Words` on the same window after its edits is
// the common case for a dense set, and everything a bit flip changes in
// its payload is local: the word, its block's summary bit, the directory
// entries after its group (a prefix sum of per-group deltas), and the
// min/max refuter when the flip is at an edge. Re-laddering it would copy
// 8 KB in and 8.7 KB out to arrive at the same bytes.

/// One chunk's slice of the batch, with the indices its first add and
/// first remove have in the whole batch, so a witness is told where a
/// value sits in what the caller passed.
#[derive(Clone, Copy)]
struct ChunkBatch<'a> {
    adds: &'a [u32],
    removes: &'a [u32],
    a0: usize,
    r0: usize,
}

/// One chunk's batch grouped by word: `(word, mask, count)` for every word
/// the values touch, ascending. The values ascend, so a word's values are
/// one run, and a word is read and written once for all of them; `count`
/// is the run's length, so a consumer indexing the batch advances by it.
struct WordRuns<'a> {
    values: &'a [u32],
}

impl Iterator for WordRuns<'_> {
    type Item = (usize, u64, usize);

    #[inline(always)]
    fn next(&mut self) -> Option<(usize, u64, usize)> {
        let (&first, rest) = self.values.split_first()?;
        let w = usize::from((first & 0xFFFF) as u16 >> 6);
        let mut mask = 1u64 << (first & 63);
        let mut n = 0usize;
        for &v in rest {
            if usize::from((v & 0xFFFF) as u16 >> 6) != w {
                break;
            }
            mask |= 1u64 << (v & 63);
            n += 1;
        }
        self.values = &rest[n..];
        Some((w, mask, n + 1))
    }
}

/// Read-only trial of the in-place edit for a `Words` chunk with window
/// `[base_word, base_word + word_count)` and `card` members: the window
/// the result occupies when the chunk stays `Words` there — the old window
/// grown to the first surviving add below or above it, or shrunk to the
/// first block a remove leaves occupied — and the result stays above
/// `ARRAY_MAX` members and is not an arithmetic progression: exactly when
/// the ladder would publish `Words` over that window. `None` sends the
/// chunk down the general path.
///
/// A window that grows on one end while it shrinks on the other is `None`
/// too: the edit grows the window before it runs (the adds need their
/// words) and shrinks it after (the words must be zero first), so that
/// edit would pass through a footprint larger than both the old payload
/// and the new one, and the in-place walk sizes a chunk's transient by
/// exactly those two.
///
/// The trial walks the batch only when it must. `card - removes` above
/// `ARRAY_MAX` already proves the result dense, and a progression is
/// refuted from its first gap; the exact count is taken only to confirm
/// one, which a dense chunk with a hole never reaches. The edit itself
/// reports the counts.
fn words_in_place_plan(
    payload: &[u8],
    base_word: u32,
    word_count: u32,
    card: u32,
    adds: &[u32],
    removes: &[u32],
) -> Option<((u16, u16), u32, u32)> {
    let (lo, wc) = (base_word as usize, word_count as usize);
    let word_of = |v: u32| usize::from((v & 0xffff) as u16 >> 6);
    let word = |w: usize| {
        if w >= lo && w < lo + wc {
            read_u64_at(payload, WORDS_AT + (w - lo) * 8)
        } else {
            0
        }
    };
    let start = adds.first().map_or(lo, |v| lo.min(word_of(*v)));
    let end = adds
        .last()
        .map_or(lo + wc, |v| (lo + wc).max(word_of(*v) + 1));
    let mut add_masks = WordRuns { values: adds }.peekable();
    let mut remove_masks = WordRuns { values: removes }.peekable();
    let (mut added, mut removed, mut runs, mut previous) = (0u32, 0u32, 0usize, 0u64);
    let (mut first, mut last) = (None, 0usize);
    for w in start..end {
        while remove_masks.peek().is_some_and(|(at, _, _)| *at < w) {
            remove_masks.next();
        }
        let add = if add_masks.peek().is_some_and(|(at, _, _)| *at == w) {
            add_masks.next().expect("peeked add mask").1
        } else {
            0
        };
        let remove = if remove_masks.peek().is_some_and(|(at, _, _)| *at == w) {
            remove_masks.next().expect("peeked remove mask").1
        } else {
            0
        };
        let old = word(w);
        added += (add & !old).count_ones();
        removed += (remove & (old | add)).count_ones();
        let result = (old | add) & !remove;
        runs += (result & !((result << 1) | previous)).count_ones() as usize;
        previous = result >> 63;
        if result != 0 {
            first.get_or_insert(w * 64 + result.trailing_zeros() as usize);
            last = w * 64 + 63 - result.leading_zeros() as usize;
        }
    }
    let n = (card + added - removed) as usize;
    if n <= ARRAY_MAX {
        return None;
    }
    let first = first?;
    let mask_in = |values: &[u32], w: usize| {
        let at = values.partition_point(|v| word_of(*v) < w);
        values[at..]
            .iter()
            .take_while(|v| word_of(**v) == w)
            .fold(0u64, |mask, value| mask | 1u64 << (value & 63))
    };
    let verdict = crate::choose_dense_facts(first, last, n, runs, |value| {
        let w = value / 64;
        ((word(w) | mask_in(adds, w)) & !mask_in(removes, w)) >> (value % 64) & 1 != 0
    });
    if !matches!(verdict, Arm::Words) {
        return None;
    }
    let new_lo = first / (64 * BLOCK_WORDS) * BLOCK_WORDS;
    let new_hi = (last / (64 * BLOCK_WORDS) + 1) * BLOCK_WORDS;
    let grows = new_lo < lo || new_hi > lo + wc;
    let shrinks = new_lo > lo || new_hi < lo + wc;
    if grows && shrinks {
        return None;
    }
    Some(((new_lo as u16, (new_hi - new_lo) as u16), added, removed))
}

/// Move a `Words` payload from window `(lo, wc)` to the enclosing window
/// `(lo2, wc2)` inside `payload`, which holds room for the larger one. The
/// words shift up by the prepended words and the new words are zero; the
/// directory moves past them, its entries unchanged — the prepended words
/// hold nothing — and the entries for the appended groups all read the
/// whole cardinality. The summary keys blocks by absolute number, so it
/// stands; the refuter is the edit's to fix.
fn words_grow(
    payload: &mut [u8],
    card: u32,
    (lo, wc): (usize, usize),
    (lo2, wc2): (usize, usize),
    dir: &mut Vec<u16>,
) {
    debug_assert!(lo2 <= lo && lo2 + wc2 >= lo + wc);
    let (groups, groups2) = (wc / DIR_STRIDE, wc2 / DIR_STRIDE);
    dir.clear();
    dir.push(0);
    dir.extend((1..groups).map(|g| read_u16_at(payload, WORDS_AT + wc * 8 + 2 * (g - 1))));
    let prepended = lo - lo2;
    payload.copy_within(WORDS_AT..WORDS_AT + wc * 8, WORDS_AT + prepended * 8);
    payload[WORDS_AT..WORDS_AT + prepended * 8].fill(0);
    payload[WORDS_AT + (prepended + wc) * 8..WORDS_AT + wc2 * 8].fill(0);
    let dir_at = WORDS_AT + wc2 * 8;
    let (entries, _) = payload[dir_at..dir_at + 2 * (groups2 - 1)].as_chunks_mut::<2>();
    let lead = prepended / DIR_STRIDE;
    for (g, entry) in entries.iter_mut().enumerate() {
        let g = g + 1;
        *entry = if g < lead {
            0
        } else if g < lead + groups {
            dir[g - lead]
        } else {
            card as u16
        }
        .to_le_bytes();
    }
    payload[16..18].copy_from_slice(&(lo2 as u16).to_le_bytes());
    payload[18..20].copy_from_slice(&(wc2 as u16).to_le_bytes());
}

/// Move an edited `Words` payload from window `(lo, wc)` to the enclosed
/// window `(lo2, wc2)`, whose dropped end blocks the edit left all zero.
/// The words move down as one block and the directory follows them with
/// its entries unchanged: entry `g` is the popcount before group `g`, and
/// the dropped leading words contribute nothing to it. The summary keys
/// blocks by absolute number and the edit already cleared the emptied
/// ones; the refuter is exact from the edit. No arithmetic, two moves.
fn words_shrink(payload: &mut [u8], (lo, wc): (usize, usize), (lo2, wc2): (usize, usize)) {
    debug_assert!(lo2 >= lo && lo2 + wc2 <= lo + wc);
    let lead = lo2 - lo;
    payload.copy_within(WORDS_AT + lead * 8..WORDS_AT + (lead + wc2) * 8, WORDS_AT);
    let dir_at = WORDS_AT + wc * 8 + 2 * (lead / DIR_STRIDE);
    payload.copy_within(
        dir_at..dir_at + 2 * (wc2 / DIR_STRIDE - 1),
        WORDS_AT + wc2 * 8,
    );
    payload[16..18].copy_from_slice(&(lo2 as u16).to_le_bytes());
    payload[18..20].copy_from_slice(&(wc2 as u16).to_le_bytes());
}

/// Apply the edits `words_in_place_plan` accepted to the payload itself,
/// reporting how many adds were fresh and how many removes were held. The
/// bytes afterwards are what `emit_words` would produce for the same
/// members: the words, the touched blocks' summary bits, the directory
/// (each entry moved by the popcount change of the groups before it) and
/// the exact min/max refuter. `payload` covers the larger of the old and
/// the new payload: a window that grows is grown before the edit, one
/// that shrinks is shrunk after it.
fn words_edit_in_place<W: Witness>(
    payload: &mut [u8],
    card: u32,
    window: (u16, u16),
    batch: ChunkBatch<'_>,
    witness: &mut W,
    scratch_dir: &mut Vec<u16>,
    group_delta: &mut [i32],
) -> (u32, u32) {
    let ChunkBatch {
        adds,
        removes,
        a0,
        r0,
    } = batch;
    let old = (
        usize::from(read_u16_at(payload, 16)),
        usize::from(read_u16_at(payload, 18)),
    );
    let new = (usize::from(window.0), usize::from(window.1));
    let grows = new.0 < old.0 || new.0 + new.1 > old.0 + old.1;
    if grows {
        words_grow(payload, card, old, new, scratch_dir);
    }
    // The window the edit runs on: the grown one, or the old one that a
    // shrink cuts down once its end blocks are zero.
    let (lo, wc) = if grows { new } else { old };
    let groups = wc / DIR_STRIDE;
    let group_delta = &mut group_delta[..groups];
    group_delta.fill(0);
    // A block a fresh bit lands in is occupied, no scan needed; only a
    // block a remove zeroed a word of might have emptied, and only those
    // are scanned.
    let mut filled_blocks = [0u64; SUMMARY_WORDS];
    let mut zeroed_blocks = [0u64; SUMMARY_WORDS];
    let (mut min_add, mut max_add) = (None, None);
    let mut first_group = usize::MAX;
    let (mut added, mut removed) = (0u32, 0u32);
    // The runs consume the batches in order, so each value's index is the
    // cursor the witness pass advances alongside the run it belongs to.
    let (mut ia, mut ir) = (0usize, 0usize);
    for (w, mask, n) in (WordRuns { values: adds }) {
        let at = WORDS_AT + (w - lo) * 8;
        let word = read_u64_at(payload, at);
        let fresh = mask & !word;
        if W::LISTENS {
            // A run landing entirely on empty bits is the common shape of
            // a batch of fresh values and is reported as one range; only a
            // run that hit held bits is walked value by value.
            if fresh == mask {
                witness.adds_new(a0 + ia..a0 + ia + n);
            } else if fresh != 0 {
                for (k, &v) in adds[ia..ia + n].iter().enumerate() {
                    if fresh >> (v & 63) & 1 == 1 {
                        witness.add_new(a0 + ia + k);
                    }
                }
            }
            ia += n;
        }
        if fresh != 0 {
            payload[at..at + 8].copy_from_slice(&(word | mask).to_le_bytes());
            let g = (w - lo) / DIR_STRIDE;
            let count = fresh.count_ones();
            added += count;
            group_delta[g] += count as i32;
            first_group = first_group.min(g);
            filled_blocks[w / BLOCK_WORDS / 64] |= 1u64 << (w / BLOCK_WORDS % 64);
            min_add.get_or_insert(w * 64 + fresh.trailing_zeros() as usize);
            max_add = Some(w * 64 + 63 - fresh.leading_zeros() as usize);
        }
    }
    for (w, mask, n) in (WordRuns { values: removes }) {
        let held = if w < lo || w >= lo + wc {
            0
        } else {
            mask & read_u64_at(payload, WORDS_AT + (w - lo) * 8)
        };
        if W::LISTENS {
            if held == mask {
                witness.removes_held(r0 + ir..r0 + ir + n);
            } else if held != 0 {
                for (k, &v) in removes[ir..ir + n].iter().enumerate() {
                    if held >> (v & 63) & 1 == 1 {
                        witness.remove_held(r0 + ir + k);
                    }
                }
            }
            ir += n;
        }
        if held != 0 {
            let at = WORDS_AT + (w - lo) * 8;
            let word = read_u64_at(payload, at);
            let left = word & !mask;
            payload[at..at + 8].copy_from_slice(&left.to_le_bytes());
            let g = (w - lo) / DIR_STRIDE;
            let count = held.count_ones();
            removed += count;
            group_delta[g] -= count as i32;
            first_group = first_group.min(g);
            if left == 0 {
                zeroed_blocks[w / BLOCK_WORDS / 64] |= 1u64 << (w / BLOCK_WORDS % 64);
            }
        }
    }
    // Directory: entry g is the popcount before group g, so every entry
    // past the first changed group moves by the running sum of the changes
    // before it. One pass from there, branchless: the loads and stores are
    // independent across entries and only the running sum is carried.
    let dir_at = WORDS_AT + wc * 8;
    // `first_group` is unset when no bit changed: every add was held and
    // every remove absent. Nothing moves then.
    if first_group != usize::MAX && first_group + 1 < groups {
        let (entries, _) =
            payload[dir_at + 2 * first_group..dir_at + 2 * (groups - 1)].as_chunks_mut::<2>();
        let mut running = 0i32;
        for (entry, delta) in entries.iter_mut().zip(&group_delta[first_group..]) {
            running += delta;
            *entry = u16::from_le_bytes(*entry)
                .wrapping_add(running as u16)
                .to_le_bytes();
        }
    }
    // Summary: filled blocks are set outright; a zeroed block is scanned.
    for s in 0..SUMMARY_WORDS {
        let mut summary = read_u64_at(payload, 8 * s) | filled_blocks[s];
        let mut blocks = zeroed_blocks[s] & !filled_blocks[s];
        while blocks != 0 {
            let block = s * 64 + blocks.trailing_zeros() as usize;
            blocks &= blocks - 1;
            let occupied = (0..BLOCK_WORDS).any(|k| {
                let w = block * BLOCK_WORDS + k;
                read_u64_at(payload, WORDS_AT + (w - lo) * 8) != 0
            });
            if !occupied {
                summary &= !(1u64 << (block % 64));
            }
        }
        payload[8 * s..8 * s + 8].copy_from_slice(&summary.to_le_bytes());
    }
    // The refuter: exact smallest and largest member.
    let word = |w: usize| read_u64_at(payload, WORDS_AT + (w - lo) * 8);
    let has = |v: usize| word(v >> 6) >> (v & 63) & 1 == 1;
    let old_min = usize::from(read_u16_at(payload, BOUNDS_AT));
    let old_max = usize::from(read_u16_at(payload, BOUNDS_AT + 2));
    let min = match min_add {
        Some(a) if a < old_min => a,
        _ if has(old_min) => old_min,
        _ => {
            let mut w = old_min >> 6;
            while word(w) == 0 {
                w += 1;
            }
            w * 64 + word(w).trailing_zeros() as usize
        }
    };
    let max = match max_add {
        Some(a) if a > old_max => a,
        _ if has(old_max) => old_max,
        _ => {
            let mut w = old_max >> 6;
            while word(w) == 0 {
                w -= 1;
            }
            w * 64 + 63 - word(w).leading_zeros() as usize
        }
    };
    payload[BOUNDS_AT..BOUNDS_AT + 2].copy_from_slice(&(min as u16).to_le_bytes());
    payload[BOUNDS_AT + 2..BOUNDS_AT + 4].copy_from_slice(&(max as u16).to_le_bytes());
    if !grows && new != old {
        words_shrink(payload, old, new);
    }
    (added, removed)
}

// ── the forest root ──────────────────────────────────────────────────────

/// Everything pass 1 decides about a forest patch, computed while the old
/// string is still borrowed and consumed once it is not. `added`,
/// `removed` and `len` count the re-laddered chunks at first; the in-place
/// edits add theirs once they have run.
#[derive(Clone, Copy, Debug)]
struct ForestPlan {
    chunks: usize,
    geometry: Geometry,
    added: u32,
    removed: u32,
    len: u64,
    forest_total: usize,
    in_place: bool,
}

fn patch_forest<W: Witness>(
    slot: &mut [u8],
    adds: &[u32],
    removes: &[u32],
    scratch: &mut PatchScratch,
    witness: &mut W,
) -> Result<PatchReport, PatchError> {
    let (plan, old_total, old_len) = {
        let view = BitmosaicView::open(slot).ok_or(PatchError::Malformed)?;
        let Root::Forest(forest) = view.root else {
            return Err(PatchError::Malformed);
        };
        let old_total = view.serialized_len();
        (
            plan_forest(&forest, adds, removes, scratch, slot.len(), witness),
            old_total,
            view.len(),
        )
    };
    let Some(mut plan) = plan else {
        return Ok(PatchReport {
            added: 0,
            removed: 0,
            len: old_len,
            serialized_len: old_total,
        });
    };
    let needed = plan.forest_total;
    if needed > slot.len() {
        return Err(PatchError::Capacity { needed });
    }
    // Chunks edited where they lie are edited in the slot once the tail
    // has moved, or — for a rebuild, which reads the slot until the whole
    // string is assembled — in the staged region, from where the rebuild
    // copies them as bytes like any other row.
    let (added, removed) = if plan.in_place {
        apply_in_place(slot, &plan, adds, removes, scratch, witness)
    } else {
        stage_words_edits(slot, adds, removes, scratch, witness)
    };
    plan.added += added;
    plan.removed += removed;
    plan.len = plan.len + u64::from(plan.added) - u64::from(plan.removed);
    if plan.in_place {
        let body_bytes =
            needed - (plan.geometry.keys_at - forest_header_len(plan.len, plan.chunks as u32));
        write_forest_header(
            slot,
            plan.geometry.slot_bits,
            plan.len,
            plan.chunks as u32,
            body_bytes,
        );
    } else {
        refresh_rows(scratch);
        assemble(slot, scratch, needed, plan.len);
        slot[..needed].copy_from_slice(&scratch.build[..needed]);
    }
    Ok(PatchReport {
        added: plan.added,
        removed: plan.removed,
        len: plan.len,
        serialized_len: needed,
    })
}

/// The rebuild path's in-place edits: each chunk edited where it lies is
/// copied from the old string into the staged region on a fresh footprint
/// of its result length and edited there, and its `Fresh` then names that
/// copy. Returns the counts the edits reported.
fn stage_words_edits<W: Witness>(
    slot: &[u8],
    adds: &[u32],
    removes: &[u32],
    scratch: &mut PatchScratch,
    witness: &mut W,
) -> (u32, u32) {
    let (mut added, mut removed) = (0u32, 0u32);
    let PatchScratch {
        touched,
        staged,
        members,
        group_delta,
        ..
    } = scratch;
    for t in touched.iter_mut() {
        if let (Some(old), Some(fresh)) = (t.old, t.fresh.as_mut())
            && !fresh.staged
        {
            let (at, len, new_len) = (old.at as usize, old.len as usize, fresh.len as usize);
            let staged_at = staged.len();
            // The edit runs on the larger of the two payloads; the row
            // copies only the new length.
            let room = len.max(new_len);
            staged.resize(staged_at + room, 0);
            staged[staged_at..staged_at + len].copy_from_slice(&slot[at..at + len]);
            let (a0, a1, r0, r1) = fresh.edits;
            let (a, r) = words_edit_in_place(
                &mut staged[staged_at..staged_at + room],
                old.card,
                fresh.window,
                ChunkBatch {
                    adds: &adds[a0 as usize..a1 as usize],
                    removes: &removes[r0 as usize..r1 as usize],
                    a0: a0 as usize,
                    r0: r0 as usize,
                },
                witness,
                members,
                &mut group_delta[..],
            );
            fresh.card = old.card + a - r;
            fresh.staged = true;
            fresh.staged_at = staged_at as u32;
            added += a;
            removed += r;
        }
    }
    (added, removed)
}

/// Carry what the staged edits decided — the cardinality, and that the
/// payload now lives in the staged region — into the rows laid out for
/// their chunks. Rows and touched chunks both ascend by key, so this is
/// one merge walk.
fn refresh_rows(scratch: &mut PatchScratch) {
    let mut rows = scratch.rows.iter_mut();
    for t in &scratch.touched {
        if let (Some(_), Some(fresh)) = (t.old, t.fresh)
            && fresh.kind == K_WORDS
            && fresh.window != (0, 0)
        {
            let row = rows
                .by_ref()
                .find(|row| row.key == t.key)
                .expect("an in-place chunk keeps its row");
            row.card = fresh.card;
            row.staged = true;
            row.src = fresh.staged_at;
        }
    }
}

/// Pass 1: re-ladder every touched chunk into the staged region and price
/// both roots. The slot is not written. `None` when the batch is empty.
/// `capacity` is the slot's length: the in-place edit is routed only when
/// its transient footprint fits, and the caller compares the final length
/// against the same bound.
fn plan_forest<W: Witness>(
    forest: &Forest<'_>,
    adds: &[u32],
    removes: &[u32],
    scratch: &mut PatchScratch,
    capacity: usize,
    witness: &mut W,
) -> Option<ForestPlan> {
    let chunks = forest.chunks as usize;
    let old_total = forest.bytes.len();
    scratch.touched.clear();
    scratch.staged.clear();
    let (mut added, mut removed) = (0u32, 0u32);
    let mut chunk_set_changed = false;

    for (key, add_range, remove_range) in ByChunk::new(adds, removes) {
        let (chunk_adds, chunk_removes) =
            (&adds[add_range.clone()], &removes[remove_range.clone()]);
        let located = forest.locate(key).map(|i| {
            let container = forest.container(i).expect("open validated every chunk");
            (
                Old {
                    index: i as u32,
                    len: view_len(container),
                    card: container.cardinality(),
                    at: payload_at(forest.bytes, forest.geometry, i) as u32,
                },
                container,
            )
        });
        // A Words chunk that stays Words on its window is edited in place,
        // after the capacity verdict; its cardinality is the edit's to
        // report and stands provisional until then.
        if let Some((
            old,
            CView::Words {
                payload,
                base_word,
                word_count,
                len,
            },
        )) = located
            && let Some((window, a, r)) = words_in_place_plan(
                payload,
                base_word,
                word_count,
                len,
                chunk_adds,
                chunk_removes,
            )
        {
            scratch.touched.push(Touched {
                key,
                old: Some(old),
                fresh: Some(Fresh {
                    kind: K_WORDS,
                    card: len + a - r,
                    len: words_payload_len(usize::from(window.1)) as u32,
                    staged: false,
                    staged_at: 0,
                    edits: (
                        add_range.start as u32,
                        add_range.end as u32,
                        remove_range.start as u32,
                        remove_range.end as u32,
                    ),
                    window,
                }),
            });
            continue;
        }
        if let Some((old, container @ (CView::Runs(_) | CView::Stride { stride: 1, .. }))) = located
        {
            let (a, r, fresh) = merge_interval_events(
                container,
                chunk_adds,
                chunk_removes,
                (add_range.start, remove_range.start),
                scratch,
                witness,
            );
            added += a;
            removed += r;
            chunk_set_changed |= fresh.is_none();
            scratch.touched.push(Touched {
                key,
                old: Some(old),
                fresh,
            });
            continue;
        }
        // A chunk that is a sequence — an `Array`, or absent — and whose
        // result cannot exceed the Array bound is merged as a sequence:
        // the plane would be zeroed, staged, applied and extracted to
        // arrive at the same members.
        let sequence = match located {
            None => Some(&[][..]),
            Some((_, CView::Array(values))) => Some(values),
            Some(_) => None,
        };
        if let Some(values) = sequence
            && values.len() / 2 + chunk_adds.len() <= ARRAY_MAX
        {
            let (a, r) = merge_array(
                values,
                chunk_adds,
                chunk_removes,
                (add_range.start, remove_range.start),
                witness,
                &mut scratch.members,
            );
            added += a;
            removed += r;
            let old = located.map(|(old, _)| old);
            let card = old.map_or(0, |o| o.card) + a - r;
            let fresh = (card > 0).then(|| finish_members(scratch, card));
            if old.is_none() && fresh.is_none() {
                continue;
            }
            chunk_set_changed |= old.is_some() != fresh.is_some();
            scratch.touched.push(Touched { key, old, fresh });
            continue;
        }
        scratch.plane.fill(0);
        let old = located.map(|(old, container)| {
            stage(&mut scratch.plane, container);
            old
        });
        let (a, r) = apply(
            &mut scratch.plane,
            chunk_adds,
            chunk_removes,
            (add_range.start, remove_range.start),
            witness,
        );
        added += a;
        removed += r;
        let card = old.map_or(0, |o| o.card) + a - r;
        let fresh = (card > 0).then(|| finish(scratch, card));
        // A chunk absent before and after — removes of values never held,
        // or adds taken back in the same batch — is not a touched chunk.
        if old.is_none() && fresh.is_none() {
            continue;
        }
        chunk_set_changed |= old.is_some() != fresh.is_some();
        scratch.touched.push(Touched { key, old, fresh });
    }
    if scratch.touched.is_empty() {
        return None;
    }

    let final_len = scratch.touched.iter().fold(forest.len, |len, touched| {
        len + u64::from(touched.fresh.map_or(0, |fresh| fresh.card))
            - u64::from(touched.old.map_or(0, |old| old.card))
    });
    let count_width_changed = final_len == 0
        || forest.len == 0
        || uleb_len((final_len - 1) as u32) != uleb_len((forest.len - 1) as u32);
    // Layout of the patched forest. In place needs the chunk set unchanged;
    // otherwise the geometry moves and the string is rebuilt from rows.
    let (forest_total, in_place) = if chunk_set_changed || count_width_changed {
        (layout_rows(forest, scratch), false)
    } else {
        let mut total = old_total;
        let mut moved = 0usize;
        // The in-place walk is descending, so when it reaches a chunk the
        // string already holds every later chunk's change: its tail move
        // needs `old_total` plus the sum of the deltas behind it, and the
        // largest of those partial sums is the walk's transient peak. The
        // final length bounds nothing here — a lower chunk that shrinks
        // gives its bytes back only after a higher one has taken its own.
        let (mut running, mut peak) = (0isize, 0isize);
        for t in scratch.touched.iter().rev() {
            let old = t.old.expect("chunk set unchanged");
            let fresh = t.fresh.expect("chunk set unchanged");
            let (was, now) = (old.len as usize, fresh.len as usize);
            running += now as isize - was as isize;
            peak = peak.max(running);
            if was != now {
                moved += old_total - old.at as usize;
            }
        }
        total = (total as isize + running) as usize;
        // Each changed footprint moves the tail once; past two copies of
        // the string, rebuilding it is the cheaper traffic. A transient
        // peak past the slot takes the same route rather than a refusal:
        // the rebuild needs only the final length, so `Capacity` keeps
        // naming the length the patched string needs and never a length
        // the slot would hold for one copy — and the rebuild's two copies
        // of the string are exactly the traffic bound the in-place walk
        // is already allowed to reach.
        let old_prefix = forest.geometry.keys_at - forest_header_len(forest.len, forest.chunks);
        let body_bytes = total - old_prefix;
        let framed_total = frame_len(body_bytes);
        let in_place = framed_total == total
            && moved <= 2 * old_total
            && old_total as isize + peak <= capacity as isize;
        if !in_place {
            (layout_rows(forest, scratch), false)
        } else {
            (total, true)
        }
    };
    Some(ForestPlan {
        chunks,
        geometry: forest.geometry,
        added,
        removed,
        len: forest.len,
        forest_total,
        in_place,
    })
}

/// Rows of the patched forest — old chunks with touched ones replaced,
/// dropped ones gone, new ones merged in. Returns the forest's total
/// length.
fn layout_rows(forest: &Forest<'_>, scratch: &mut PatchScratch) -> usize {
    scratch.rows.clear();
    let chunks = forest.chunks as usize;
    let mut t = 0usize;
    let mut i = 0usize;
    loop {
        let touched = scratch.touched.get(t).copied();
        let old_key = (i < chunks).then(|| forest.key(i));
        match (old_key, touched) {
            (None, None) => break,
            (Some(key), Some(tc)) if tc.key == key => {
                push_fresh(&mut scratch.rows, tc);
                t += 1;
                i += 1;
            }
            (Some(key), Some(tc)) if tc.key > key => {
                push_old(&mut scratch.rows, forest, i);
                i += 1;
            }
            (Some(_), None) => {
                push_old(&mut scratch.rows, forest, i);
                i += 1;
            }
            (_, Some(tc)) => {
                push_fresh(&mut scratch.rows, tc);
                t += 1;
            }
        }
    }
    rows_total(&scratch.rows)
}

fn push_old(rows: &mut Vec<Row>, forest: &Forest<'_>, i: usize) {
    let c = forest.container(i).expect("open validated every chunk");
    rows.push(Row {
        key: forest.key(i),
        card: c.cardinality(),
        kind: view_kind(c),
        len: view_len(c),
        staged: false,
        src: payload_at(forest.bytes, forest.geometry, i) as u32,
    });
}

fn push_fresh(rows: &mut Vec<Row>, t: Touched) {
    if let Some(f) = t.fresh {
        rows.push(Row {
            key: t.key,
            card: f.card,
            kind: f.kind,
            len: f.len,
            staged: f.staged,
            src: if f.staged {
                f.staged_at
            } else {
                t.old.expect("an in-place chunk is an old chunk").at
            },
        });
    }
}

/// The string's total length for these rows.
fn rows_total(rows: &[Row]) -> usize {
    rows.iter().fold(geometry_of(rows).payload_at, |at, row| {
        at + row.len as usize
    })
}

fn geometry_of(rows: &[Row]) -> Geometry {
    let chunks = rows.len() as u32;
    let slot_bits = match rows {
        [] => 0,
        [first, .., last] => slot_bits_for_span(chunks, first.key, last.key),
        [only] => slot_bits_for_span(1, only.key, only.key),
    };
    if rows.is_empty() {
        return Geometry::new(0, 0, IMAGE_ID_LEN);
    }
    let len = rows.iter().map(|row| u64::from(row.card)).sum();
    let body_header = forest_header_len(len, chunks);
    let body_geometry = Geometry::new(chunks, slot_bits, body_header);
    let body_bytes = rows
        .iter()
        .fold(body_geometry.payload_at, |at, row| at + row.len as usize);
    Geometry::new(
        chunks,
        slot_bits,
        frame_len(body_bytes) - body_bytes + body_header,
    )
}

/// Build the forest described by the rows into `scratch.build`, payloads
/// copied from the old string in `slot` or from the staged region.
fn assemble(slot: &[u8], scratch: &mut PatchScratch, total: usize, len: u64) {
    let geometry = geometry_of(&scratch.rows);
    let build = &mut scratch.build;
    grow(build, total);
    if len == 0 {
        build[..IMAGE_ID_LEN].copy_from_slice(&EMPTY_U32_IMAGE);
        return;
    }
    // The directory's holes are its empty slots; the payload region is
    // written completely. The two are written through
    // disjoint halves: the rows' payload copies run as the directory writer
    // pulls each row, one pass over both.
    let (directory, payloads) = build[..total].split_at_mut(geometry.payload_at);
    directory.fill(0);
    let mut at = geometry.payload_at;
    let staged = &scratch.staged;
    let rows = scratch.rows.iter().map(|row| {
        let end = at + row.len as usize;
        let src = row.src as usize..row.src as usize + row.len as usize;
        payloads[at - geometry.payload_at..end - geometry.payload_at]
            .copy_from_slice(if row.staged { &staged[src] } else { &slot[src] });
        let start = at;
        at = end;
        ChunkRow {
            key: row.key,
            card: row.card,
            kind: row.kind,
            at: start,
        }
    });
    let counted = write_directory(directory, geometry, rows);
    debug_assert_eq!(counted, len);
    debug_assert_eq!(at, total);
    write_forest_header(
        build,
        geometry.slot_bits,
        len,
        scratch.rows.len() as u32,
        total - (geometry.keys_at - forest_header_len(len, scratch.rows.len() as u32)),
    );
}

/// The in-place edit: touched chunks in descending directory order, each
/// rewriting its payload and moving the tail past it once by the change in
/// its footprint. Descending, because the tail a chunk moves holds the
/// chunks already finished and nothing this chunk still needs to read.
/// Returns the counts the chunks edited where they lie reported; the
/// header is the caller's to finish.
fn apply_in_place<W: Witness>(
    slot: &mut [u8],
    plan: &ForestPlan,
    adds: &[u32],
    removes: &[u32],
    scratch: &mut PatchScratch,
    witness: &mut W,
) -> (u32, u32) {
    let chunks = plan.chunks;
    let geometry = plan.geometry;
    let mut running_total = image_header(slot).expect("validated frame").encoded_len;
    let (mut added, mut removed) = (0u32, 0u32);
    let PatchScratch {
        touched,
        staged: staged_region,
        members,
        group_delta,
        ..
    } = scratch;
    for t in touched.iter_mut().rev() {
        let old = t.old.expect("in place: chunk set unchanged");
        let fresh = t.fresh.as_mut().expect("in place: chunk set unchanged");
        let i = old.index as usize;
        let at = old.at as usize;
        let last = i + 1 == chunks;
        let (was, now) = (old.len as usize, fresh.len as usize);
        let delta = now as isize - was as isize;
        let new_len = fresh.len as usize;
        // The tail moves once. A chunk edited where it lies needs the old
        // bytes until the edit has run and the new footprint once it has:
        // a window that grows moves the tail first, one that shrinks
        // moves it after — either way the edit works inside the larger
        // of the two payloads and the string never exceeds `old_total`
        // plus the deltas already behind this chunk, the peak the plan
        // bounded.
        let move_tail = |slot: &mut [u8], running_total: usize| -> usize {
            if last {
                at + new_len
            } else if delta != 0 {
                slot.copy_within(at + was..running_total, at + now);
                (running_total as isize + delta) as usize
            } else {
                running_total
            }
        };
        if fresh.staged {
            running_total = move_tail(slot, running_total);
            let staged = fresh.staged_at as usize;
            slot[at..at + new_len].copy_from_slice(&staged_region[staged..staged + new_len]);
        } else {
            if delta > 0 {
                running_total = move_tail(slot, running_total);
            }
            let (a0, a1, r0, r1) = fresh.edits;
            let room = (old.len as usize).max(new_len);
            let (a, r) = words_edit_in_place(
                &mut slot[at..at + room],
                old.card,
                fresh.window,
                ChunkBatch {
                    adds: &adds[a0 as usize..a1 as usize],
                    removes: &removes[r0 as usize..r1 as usize],
                    a0: a0 as usize,
                    r0: r0 as usize,
                },
                witness,
                members,
                &mut group_delta[..],
            );
            fresh.card = old.card + a - r;
            added += a;
            removed += r;
            if delta <= 0 {
                running_total = move_tail(slot, running_total);
            }
        }
        slot[at + new_len..at + now].fill(0);

        // Its own record: the payload stays where it was, the kind may
        // change. The shift the tail took is applied to the later records
        // in one forward pass below, not once per touched chunk.
        write_kind(slot, geometry.kinds_at, i, fresh.kind);
    }
    debug_assert_eq!(running_total, plan.forest_total);
    // Every record after a touched chunk moves by the footprint change and
    // the cardinality change of the touched chunks before it: one pass
    // over the directory with two running sums, ascending like `touched`.
    let (mut shift, mut dcard) = (0isize, 0u32);
    let mut t = touched.iter().peekable();
    let first = touched[0].old.expect("in place").index as usize;
    for j in first..chunks {
        if shift != 0 && j != 0 {
            let d = geometry.desc_at + 4 * (j - 1);
            let offset = read_u32_at(slot, d);
            let moved = (offset as isize + shift) as u32;
            slot[d..d + 4].copy_from_slice(&moved.to_le_bytes());
        }
        if dcard != 0 && j != 0 {
            let b = geometry.before_at + 4 * (j - 1);
            let before = read_u32_at(slot, b).wrapping_add(dcard);
            slot[b..b + 4].copy_from_slice(&before.to_le_bytes());
        }
        if let Some(tc) = t.peek()
            && tc.old.expect("in place").index as usize == j
        {
            let old = tc.old.expect("in place");
            let fresh = tc.fresh.expect("in place");
            let (was, now) = (old.len as usize, fresh.len as usize);
            shift += now as isize - was as isize;
            dcard = dcard.wrapping_add(fresh.card.wrapping_sub(old.card));
            t.next();
        }
    }
    (added, removed)
}

// ── the Elias-Fano root ──────────────────────────────────────────────────

/// `(old ∪ adds) \ removes`, ascending, over any ascending member source.
/// Counts what it folds in as it goes.
#[derive(Clone)]
struct Merge<'a, I: Iterator<Item = u32>> {
    old: I,
    head: Option<u32>,
    adds: &'a [u32],
    removes: &'a [u32],
    /// Adds that were not already members, and removes that were.
    added: u32,
    removed: u32,
}

impl<'a, I: Iterator<Item = u32>> Merge<'a, I> {
    fn new(mut old: I, adds: &'a [u32], removes: &'a [u32]) -> Self {
        let head = old.next();
        Merge {
            old,
            head,
            adds,
            removes,
            added: 0,
            removed: 0,
        }
    }
}

impl<I: Iterator<Item = u32>> Iterator for Merge<'_, I> {
    type Item = u32;

    fn next(&mut self) -> Option<u32> {
        loop {
            let (candidate, was_member) = match (self.head, self.adds.first()) {
                (None, None) => return None,
                (Some(h), Some(&a)) if a < h => {
                    self.adds = &self.adds[1..];
                    (a, false)
                }
                (Some(h), Some(&a)) if a == h => {
                    self.adds = &self.adds[1..];
                    self.head = self.old.next();
                    (h, true)
                }
                (Some(h), _) => {
                    self.head = self.old.next();
                    (h, true)
                }
                (None, Some(&a)) => {
                    self.adds = &self.adds[1..];
                    (a, false)
                }
            };
            // Removes are ascending like the candidates: drop the prefix
            // below the candidate, then test the head.
            let skip = self.removes.partition_point(|r| *r < candidate);
            self.removes = &self.removes[skip..];
            // Adds land first, removes act on the result: a value both
            // added and removed counts once on each side, as the forest
            // path's plane edits count it.
            self.added += u32::from(!was_member);
            if self.removes.first() == Some(&candidate) {
                self.removes = &self.removes[1..];
                self.removed += 1;
                continue;
            }
            return Some(candidate);
        }
    }
}

fn patch_ef<W: Witness>(
    slot: &mut [u8],
    adds: &[u32],
    removes: &[u32],
    scratch: &mut PatchScratch,
    witness: &mut W,
) -> Result<PatchReport, PatchError> {
    let (needed, report) = {
        let view = BitmosaicView::open(slot).ok_or(PatchError::Malformed)?;
        let Root::Ef(ef) = view.root else {
            return Err(PatchError::Malformed);
        };
        // The splice and the re-encode both count what they fold in but
        // consume the batch bucket by bucket; the per-value answer is a
        // probe of the old plane, taken only when something listens.
        if W::LISTENS {
            for (i, &a) in adds.iter().enumerate() {
                if !ef.contains(a) {
                    witness.add_new(i);
                }
            }
            for (i, &r) in removes.iter().enumerate() {
                if ef.contains(r) || adds.binary_search(&r).is_ok() {
                    witness.remove_held(i);
                }
            }
        }
        match splice_ef(slot, ef, adds, removes, scratch) {
            Some(done) => done?,
            None => reencode_ef(slot, Merge::new(view.range(), adds, removes), scratch)?,
        }
    };
    slot[..needed].copy_from_slice(&scratch.build[..needed]);
    Ok(report)
}

// ── the Elias-Fano splice ────────────────────────────────────────────────
//
// An Elias-Fano plane is one global structure: member `k` in bucket `h`
// is the one-bit at position `h + k`, and its low field sits at index
// `k`. A batch therefore inserts and deletes single bits in the high plane
// and single fields in the low plane, at positions that ascend with the
// values. The splice is one pass over each plane at word granularity: the
// runs between edits are copied with a running bit shift, the edits are
// resolved inside their buckets, and the sample tables are a byproduct of
// the words as they are emitted. Nothing is decoded member by member.
//
// The pass keeps the layout's constants — `base`, `span`, `low_bits` — so
// a batch that moves the smallest or largest member, or a cardinality
// that changes the low width, takes the member-stream rebuild instead.

/// Bit-granular writer over a byte region with an accumulator word. A run
/// copy first tops the accumulator up to a whole word from the source, so
/// the rest of the run is one gathered load and one store per 64 bits
/// with no accumulator shifts at all.
struct BitSink<'a> {
    out: &'a mut [u8],
    word: usize,
    acc: u64,
    filled: u32,
}

impl<'a> BitSink<'a> {
    fn new(out: &'a mut [u8]) -> Self {
        BitSink {
            out,
            word: 0,
            acc: 0,
            filled: 0,
        }
    }

    /// Append the low `n` bits of `bits` (`n <= 64`, higher bits clear).
    #[inline(always)]
    fn push(&mut self, bits: u64, n: u32) {
        if n == 64 {
            return self.push_word(bits);
        }
        if n == 0 {
            return;
        }
        let filled = self.filled;
        if filled + n < 64 {
            self.acc |= bits << filled;
            self.filled = filled + n;
            return;
        }
        self.flush(self.acc | (bits << filled));
        let spill = filled + n - 64;
        self.acc = if spill == 0 { 0 } else { bits >> (64 - filled) };
        self.filled = spill;
    }

    /// Append 64 bits.
    #[inline(always)]
    fn push_word(&mut self, bits: u64) {
        let filled = self.filled;
        if filled == 0 {
            self.flush(bits);
            return;
        }
        self.flush(self.acc | (bits << filled));
        self.acc = bits >> (64 - filled);
    }

    /// Append `len` bits of `src` starting at bit `from`. `src` must hold
    /// at least 8 bytes past the run's last byte, which every plane of an
    /// Elias-Fano payload does (the low plane's pad and the sample tables
    /// follow it).
    fn copy(&mut self, src: &[u8], mut from: u64, mut len: u64) {
        // Top the accumulator up to a whole word so the body needs none.
        if self.filled != 0 {
            let head = u64::from(64 - self.filled);
            if len < head {
                let word = read_bits(src, from) & ((1u64 << len) - 1);
                self.push(word, len as u32);
                return;
            }
            self.push(read_bits(src, from) & ((1u64 << head) - 1), head as u32);
            from += head;
            len -= head;
        }
        let mut byte = (from / 8) as usize;
        let shift = (from % 8) as u32;
        while len >= 64 {
            let lo = read_u64_at(src, byte);
            let word = if shift == 0 {
                lo
            } else {
                (lo >> shift) | (read_u64_at(src, byte + 8) << (64 - shift))
            };
            self.flush(word);
            byte += 8;
            len -= 64;
        }
        if len > 0 {
            let word = read_bits(src, u64::from(shift) + byte as u64 * 8) & ((1u64 << len) - 1);
            self.push(word, len as u32);
        }
    }

    #[inline(always)]
    fn flush(&mut self, word: u64) {
        let at = self.word * 8;
        self.out[at..at + 8].copy_from_slice(&word.to_le_bytes());
        self.word += 1;
    }

    /// Flush the partial word; returns the bits written.
    fn finish(mut self) -> u64 {
        let filled = self.filled;
        if filled > 0 {
            self.flush(self.acc);
        }
        (self.word as u64 - u64::from(filled > 0)) * 64 + u64::from(filled)
    }
}

/// The sample tables of a high plane, from its words: the position of
/// every `SAMPLE_STRIDE`-th one and zero. One popcount per word; a word
/// holds at most 64 of either, so it crosses at most one boundary of each
/// kind and the in-word select runs only there.
fn build_samples(plane: &[u8], high_bits: u64, ones_out: &mut [u8], zeros_out: &mut [u8]) {
    let stride = u64::from(SAMPLE_STRIDE);
    let (mut ones, mut zeros) = (0u64, 0u64);
    let words = high_bits.div_ceil(64) as usize;
    for w in 0..words {
        let base = w as u64 * 64;
        let live = (high_bits - base).min(64);
        let mask = if live == 64 {
            u64::MAX
        } else {
            (1u64 << live) - 1
        };
        let word = read_u64_at(plane, w * 8) & mask;
        let word_ones = u64::from(word.count_ones());
        let word_zeros = live - word_ones;
        let next = ((ones + stride - 1) & !(stride - 1)).max(stride);
        if next < ones + word_ones {
            let at = ((next / stride - 1) * 4) as usize;
            let p = base + select_in_word(word, (next - ones) as u32);
            ones_out[at..at + 4].copy_from_slice(&(p as u32).to_le_bytes());
        }
        let next = (zeros + stride - 1) & !(stride - 1);
        if next < zeros + word_zeros {
            let at = ((next / stride) * 4) as usize;
            let p = base + select_in_word(!word & mask, (next - zeros) as u32);
            zeros_out[at..at + 4].copy_from_slice(&(p as u32).to_le_bytes());
        }
        ones += word_ones;
        zeros += word_zeros;
    }
}

/// Position of the `r`-th set bit of `word` (`r < popcount`).
#[inline(always)]
fn select_in_word(mut word: u64, r: u32) -> u64 {
    for _ in 0..r {
        word &= word - 1;
    }
    u64::from(word.trailing_zeros())
}

/// 64 bits of `src` from bit `from`, zero past the end.
#[inline(always)]
fn read_bits(src: &[u8], from: u64) -> u64 {
    let byte = (from / 8) as usize;
    let shift = (from % 8) as u32;
    let lo = read_u64_padded(src, byte);
    if shift == 0 {
        lo
    } else {
        (lo >> shift) | (read_u64_padded(src, byte + 8) << (64 - shift))
    }
}

#[inline(always)]
fn read_u64_padded(src: &[u8], at: usize) -> u64 {
    if at + 8 <= src.len() {
        return read_u64_at(src, at);
    }
    let mut buf = [0u8; 8];
    if at < src.len() {
        buf[..src.len() - at].copy_from_slice(&src[at..]);
    }
    u64::from_le_bytes(buf)
}

/// One bucket's worth of edits: the batch values sharing high part `h`,
/// as `(low, is_add)` in ascending order.
struct BucketEdits<'a> {
    adds: &'a [u32],
    removes: &'a [u32],
    base: u32,
    low_bits: u8,
}

impl<'a> BucketEdits<'a> {
    #[inline(always)]
    fn high(&self, v: u32) -> u64 {
        u64::from(v - self.base) >> self.low_bits
    }

    #[inline(always)]
    fn low(&self, v: u32) -> u64 {
        u64::from(v - self.base) & ((1u64 << self.low_bits) - 1)
    }

    /// The next bucket with an edit, or `None`.
    fn next_bucket(&self) -> Option<u64> {
        match (self.adds.first(), self.removes.first()) {
            (None, None) => None,
            (Some(&a), None) => Some(self.high(a)),
            (None, Some(&r)) => Some(self.high(r)),
            (Some(&a), Some(&r)) => Some(self.high(a).min(self.high(r))),
        }
    }

    /// Take the edits of bucket `h` off the front. Linear: `h` is the
    /// smallest bucket with an edit, so its edits are a prefix of both
    /// slices, and a bucket holds a handful.
    fn take(&mut self, h: u64) -> (&'a [u32], &'a [u32]) {
        let na = self.adds.iter().take_while(|v| self.high(**v) <= h).count();
        let nr = self
            .removes
            .iter()
            .take_while(|v| self.high(**v) <= h)
            .count();
        let (adds, rest) = self.adds.split_at(na);
        self.adds = rest;
        let (removes, rest) = self.removes.split_at(nr);
        self.removes = rest;
        (adds, removes)
    }
}

/// Sequential reader over the old high plane: the bit position `p`, the
/// rank `k` at it, and the bucket `z` it is in. Advancing to a bucket
/// copies the bits on the way and counts the zeros in the words it copies,
/// so no bucket start is ever looked up — the copy the splice must do
/// anyway is the walk.
struct HighWalk<'s> {
    ef: EfView<'s>,
    src: &'s [u8],
    high_bits: u64,
    p: u64,
    k: u64,
    z: u64,
}

/// Buckets ahead beyond which a bucket start is looked up through the zero
/// samples and the run copied plain, rather than counted word by word: a
/// lookup costs a sample load and a short scan, a counted word a popcount
/// and a compare, and a bucket is under three bits on average.
const WALK_JUMP: u64 = 256;

impl HighWalk<'_> {
    /// Copy through the terminator of bucket `h - 1`, leaving `p` on the
    /// first bit of bucket `h`. Past the old plane's end, buckets are
    /// empty and their terminators are pushed outright.
    fn advance_to_bucket(&mut self, h: u64, sink: &mut BitSink<'_>) {
        if self.z >= h {
            return;
        }
        let old_zeros = self.ef.layout.zeros;
        if h >= old_zeros {
            // Everything left of the old plane is kept, then the buckets
            // past its end.
            sink.copy(self.src, self.p, self.high_bits - self.p);
            self.k = self.ef.len;
            self.p = self.high_bits;
            let mut left = h - old_zeros.max(self.z);
            while left > 0 {
                let n = left.min(64) as u32;
                sink.push(0, n);
                left -= u64::from(n);
            }
            self.z = h;
            return;
        }
        if h - self.z >= WALK_JUMP {
            let p_h = self.ef.select_zero(h - 1) + 1;
            sink.copy(self.src, self.p, p_h - self.p);
            self.p = p_h;
            self.k = p_h - h;
            self.z = h;
            return;
        }
        let mut remaining = h - self.z;
        // Whole words while the zeros they hold do not reach the target:
        // the loads are the copy's own and the zero count is a popcount.
        let byte_shift = (self.p % 8) as u32;
        while self.p + 64 <= self.high_bits {
            let byte = (self.p / 8) as usize;
            let lo = read_u64_at(self.src, byte);
            let word = if byte_shift == 0 {
                lo
            } else {
                (lo >> byte_shift) | (read_u64_at(self.src, byte + 8) << (64 - byte_shift))
            };
            let zeros = u64::from((!word).count_ones());
            if zeros >= remaining {
                break;
            }
            sink.push_word(word);
            self.p += 64;
            self.z += zeros;
            self.k += 64 - zeros;
            remaining -= zeros;
        }
        // The word holding the target zero, or the plane's ragged end.
        loop {
            let avail = self.high_bits.saturating_sub(self.p).min(64);
            if avail == 0 {
                // Beyond the old plane: `remaining` empty buckets.
                while remaining > 0 {
                    let n = remaining.min(64) as u32;
                    sink.push(0, n);
                    remaining -= u64::from(n);
                }
                self.z = h;
                return;
            }
            let mask = if avail == 64 {
                u64::MAX
            } else {
                (1u64 << avail) - 1
            };
            let word = read_bits(self.src, self.p) & mask;
            let zeros = u64::from((!word & mask).count_ones());
            if zeros < remaining {
                sink.push(word, avail as u32);
                self.p += avail;
                self.z += zeros;
                self.k += avail - zeros;
                remaining -= zeros;
                continue;
            }
            let end = select_in_word(!word & mask, (remaining - 1) as u32) + 1;
            let keep = if end == 64 {
                u64::MAX
            } else {
                (1u64 << end) - 1
            };
            sink.push(word & keep, end as u32);
            self.p += end;
            self.k += end - remaining;
            self.z = h;
            return;
        }
    }
}

/// The splice. `None` when the batch needs the member-stream re-encode
/// (the smallest member moves, the low width changes, or the result is
/// too small for the root); otherwise the rebuilt string's length and
/// report, in `scratch.build`.
fn splice_ef(
    slot: &[u8],
    ef: EfView<'_>,
    adds: &[u32],
    removes: &[u32],
    scratch: &mut PatchScratch,
) -> Option<Result<(usize, PatchReport), PatchError>> {
    let (base, span, n, layout) = (ef.base, ef.span, ef.len, ef.layout);
    let last = base + span;
    let b = layout.low_bits;
    // The smallest member is the origin of every field; moving it rewrites
    // them all. The largest only bounds the bucket count, which the walk
    // extends or truncates.
    if adds.first().is_some_and(|&a| a < base) || removes.binary_search(&base).is_ok() {
        return None;
    }
    // A remove outside the set's range holds nothing; the splice walks
    // only values the plane can place.
    let removes =
        &removes[removes.partition_point(|&r| r < base)..removes.partition_point(|&r| r <= last)];
    if adds.is_empty() && removes.is_empty() {
        let total = image_header(slot)?.encoded_len;
        scratch.build.clear();
        scratch.build.extend_from_slice(&slot[..total]);
        return Some(Ok((
            total,
            PatchReport {
                added: 0,
                removed: 0,
                len: n,
                serialized_len: total,
            },
        )));
    }
    let new_last = last_after(ef, adds, removes);
    let new_span = new_last - base;
    let new_zeros = (u64::from(new_span) >> b) + 1;

    // Output regions. The high plane goes straight into the build at its
    // final offset; only the low plane is staged because its final offset
    // depends on the cardinality the pass is about to count.
    let n_max = n + adds.len() as u64;
    let high_words_max = (new_zeros + n_max).div_ceil(64) as usize;
    let low_bytes_max = ((n_max * u64::from(b)).div_ceil(8) as usize).div_ceil(8) * 8 + 16;
    // Grow-only buffers: every byte the result reads is written below, so
    // nothing is zeroed here.
    let build = &mut scratch.build;
    grow(build, high_words_max * 8 + 8);
    let staged = &mut scratch.staged;
    grow(staged, low_bytes_max);
    let low_out = &mut staged[..];

    let src = ef.bytes;
    let low_src = &src[layout.low_at()..];
    let mut high = BitSink::new(build);
    let mut low = BitSink::new(low_out);
    let mut walk = HighWalk {
        ef,
        src,
        high_bits: layout.high_bits,
        p: 0,
        k: 0,
        z: 0,
    };
    // Low fields copied so far: synced to the walk's rank before each
    // bucket's edits and at the end, one run per sync.
    let mut k_low = 0u64;
    let mut edits = BucketEdits {
        adds,
        removes,
        base,
        low_bits: b,
    };
    let (mut added, mut removed) = (0u32, 0u32);
    while let Some(h) = edits.next_bucket() {
        if h >= new_zeros {
            // Buckets past the new largest member hold no survivor and no
            // add; their removes are counted and nothing is emitted.
            removed += edits.removes.iter().filter(|&&r| ef.contains(r)).count() as u32;
            break;
        }
        let (bucket_adds, bucket_removes) = edits.take(h);
        walk.advance_to_bucket(h, &mut high);
        if b != 0 {
            low.copy(
                low_src,
                k_low * u64::from(b),
                (walk.k - k_low) * u64::from(b),
            );
        }
        k_low = walk.k;
        // Inside the bucket: merge the old members with the edits by low.
        let (mut ia, mut ir) = (0usize, 0usize);
        loop {
            let member = (walk.k < n && ef.bit(walk.p)).then(|| ef.low(walk.k));
            let add = bucket_adds.get(ia).map(|&v| (v, edits.low(v)));
            let remove = bucket_removes.get(ir).map(|&v| (v, edits.low(v)));
            match (member, add) {
                (None, None) => break,
                // A fresh add below every remaining member.
                (m, Some((_, a))) if m.is_none_or(|m| a < m) => {
                    // Removes below the add match no member either — every
                    // member left is above the add — so they are spent
                    // before the add's own remove can be seen.
                    while bucket_removes.get(ir).is_some_and(|&r| edits.low(r) < a) {
                        ir += 1;
                    }
                    let remove = bucket_removes.get(ir).map(|&v| (v, edits.low(v)));
                    match remove {
                        Some((_, r)) if r == a => {
                            // Added and removed in one batch: counts on
                            // both sides, lands nowhere.
                            ir += 1;
                            added += 1;
                            removed += 1;
                        }
                        _ => {
                            high.push(1, 1);
                            low.push(a, u32::from(b));
                            added += 1;
                        }
                    }
                    ia += 1;
                }
                (Some(m), add) => {
                    // The add of a value already held is a no-op.
                    if add.is_some_and(|(_, a)| a == m) {
                        ia += 1;
                    }
                    match remove {
                        Some((_, r)) if r < m => {
                            // A remove of a value not held.
                            ir += 1;
                            continue;
                        }
                        Some((_, r)) if r == m => {
                            ir += 1;
                            removed += 1;
                        }
                        _ => {
                            high.push(1, 1);
                            if b != 0 {
                                low.push(m, u32::from(b));
                            }
                        }
                    }
                    walk.p += 1;
                    walk.k += 1;
                    k_low = walk.k;
                }
                (None, Some(_)) => unreachable!("an add with no member left takes the first arm"),
            }
        }
    }
    // Every member past the last edit is kept: through the last new
    // bucket's terminator, and no further — buckets the largest member
    // vacated end here.
    walk.advance_to_bucket(new_zeros, &mut high);
    if b != 0 {
        low.copy(
            low_src,
            k_low * u64::from(b),
            (walk.k - k_low) * u64::from(b),
        );
    }
    let high_bits = high.finish();
    low.finish();

    let n_new = n + u64::from(added) - u64::from(removed);
    debug_assert_eq!(high_bits, new_zeros + n_new);
    if n_new < 2 {
        return None;
    }
    let new_layout = EfLayout::new(n_new, u64::from(new_span))?;
    if new_layout.low_bits != b {
        return None;
    }
    debug_assert_eq!(new_layout.zeros, new_zeros);
    debug_assert_eq!(new_layout.high_words, high_bits.div_ceil(64) as usize);

    let body_bytes = ef_header_len(n_new, base, new_span) + new_layout.payload_bytes();
    let ef_total = frame_len(body_bytes);
    if ef_total > slot.len() {
        return Some(Err(PatchError::Capacity { needed: ef_total }));
    }

    // Assemble: the staged low plane lands at its offset, the sample
    // tables are derived from the high plane in place.
    let build = &mut scratch.build;
    grow(build, ef_total);
    let payload_at = ef_total - new_layout.payload_bytes();
    build.copy_within(0..new_layout.high_words * 8, payload_at);
    let payload = &mut build[payload_at..ef_total];
    let low_out = &scratch.staged;
    let low_at = new_layout.low_at();
    payload[low_at..low_at + new_layout.low_bytes]
        .copy_from_slice(&low_out[..new_layout.low_bytes]);
    let (high_plane, tables) = payload.split_at_mut(new_layout.ones_at());
    let (ones_samples, zero_samples) = tables.split_at_mut(new_layout.ones_samples * 4);
    build_samples(
        high_plane,
        high_bits,
        ones_samples,
        &mut zero_samples[..new_layout.zero_samples * 4],
    );
    write_ef_header(build, n_new, body_bytes, base, new_span);
    Some(Ok((
        ef_total,
        PatchReport {
            added,
            removed,
            len: n_new,
            serialized_len: ef_total,
        },
    )))
}

/// Largest member after the batch: the largest add, unless a surviving old
/// member is larger — old members are walked down from the top past the
/// removed ones, which is as many `select`s as removes at the top.
fn last_after(ef: EfView<'_>, adds: &[u32], removes: &[u32]) -> u32 {
    let mut candidate = adds.last().copied();
    let mut j = ef.len;
    let mut removes = removes;
    while j > 0 {
        let v = ef.select(j - 1).expect("rank in range");
        if candidate.is_some_and(|c| c > v) {
            break;
        }
        match removes.binary_search(&v) {
            Ok(at) => {
                removes = &removes[..at];
                j -= 1;
            }
            Err(_) => {
                candidate = Some(v);
                break;
            }
        }
    }
    candidate.expect("a non-empty set has a largest member")
}

/// Re-encode an Elias-Fano root from an ascending member stream into
/// `scratch.build`: the path when the batch moves a layout constant. Two
/// passes over the stream — count and bound, then encode — because the
/// layout is a function of the count. Fewer than two survivors cannot be
/// an Elias-Fano plane and become a forest.
fn reencode_ef<'a, I: Iterator<Item = u32> + Clone>(
    slot: &[u8],
    members: Merge<'a, I>,
    scratch: &mut PatchScratch,
) -> Result<(usize, PatchReport), PatchError> {
    let mut walk = members.clone();
    let (mut n, mut base, mut last) = (0u64, 0u32, 0u32);
    for v in &mut walk {
        if n == 0 {
            base = v;
        }
        last = v;
        n += 1;
    }
    let (added, removed) = (walk.added, walk.removed);
    let Some(layout) = (n >= 2)
        .then(|| EfLayout::new(n, u64::from(last - base)))
        .flatten()
    else {
        return rebuild_forest_from_members(slot, members, scratch);
    };
    let body_bytes = ef_header_len(n, base, last - base) + layout.payload_bytes();
    let needed = frame_len(body_bytes);
    if needed > slot.len() {
        return Err(PatchError::Capacity { needed });
    }
    let build = &mut scratch.build;
    grow(build, needed);
    let payload_at = write_ef_header(build, n, body_bytes, base, last - base);
    ef::encode_into(members, base, layout, &mut build[payload_at..needed]);
    Ok((
        needed,
        PatchReport {
            added,
            removed,
            len: n,
            serialized_len: needed,
        },
    ))
}

/// Rebuild a FOREST into `scratch.build` from an ascending member stream,
/// re-laddering every chunk. Returns the string's length and the report;
/// the caller copies `build` into the slot.
fn rebuild_forest_from_members<'a, I: Iterator<Item = u32> + Clone>(
    slot: &[u8],
    members: Merge<'a, I>,
    scratch: &mut PatchScratch,
) -> Result<(usize, PatchReport), PatchError> {
    scratch.rows.clear();
    scratch.staged.clear();

    let mut n = 0u64;
    let mut current: Option<u16> = None;
    let mut card = 0u32;
    let mut walk = members;
    for v in &mut walk {
        n += 1;
        let key = (v >> 16) as u16;
        if current != Some(key) {
            if let Some(k) = current {
                let fresh = finish(scratch, card);
                push_fresh(
                    &mut scratch.rows,
                    Touched {
                        key: k,
                        old: None,
                        fresh: Some(fresh),
                    },
                );
            }
            scratch.plane.fill(0);
            current = Some(key);
            card = 0;
        }
        set_bit(&mut scratch.plane, (v & 0xFFFF) as u16);
        card += 1;
    }
    if let Some(k) = current {
        let fresh = finish(scratch, card);
        push_fresh(
            &mut scratch.rows,
            Touched {
                key: k,
                old: None,
                fresh: Some(fresh),
            },
        );
    }
    let (added, removed) = (walk.added, walk.removed);
    let needed = rows_total(&scratch.rows);
    if needed > slot.len() {
        return Err(PatchError::Capacity { needed });
    }
    assemble(slot, scratch, needed, n);
    Ok((
        needed,
        PatchReport {
            added,
            removed,
            len: n,
            serialized_len: needed,
        },
    ))
}
