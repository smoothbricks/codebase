//! The canonical check: is this admitted image the one the writer selects
//! for its set?
//!
//! Structural and verifying admission prove an image well formed and its
//! derived planes true. They do not prove it is the image the selection
//! rules give: an Array where the members are a Stride reads as the same
//! set from other bytes. A consumer that hashes or compares images needs
//! one byte string a set, so verified admission ends here, with the
//! writer's own choosers run over the members each chunk holds:
//!
//! - every directory chunk is in the arm the ladder picks for its members;
//! - one chunk at key zero is a directory only when no small form is as
//!   short, and a small image is the small form its set selects;
//! - under [`Root::Sealed`], the root is the sealed writer's: Elias-Fano
//!   exactly when strictly shorter than the forest family.
//!
//! It is one more pass over the members, with two 8 KiB arrays on the
//! stack and no allocation, and it runs only under verified admission:
//! `open` and every probe are untouched.

use crate::ef::{self, EfLayout};
use crate::patch::{PatchError, PatchScratch, stage};
use crate::small::{SmallSet, SmallSink, read_small_tag, small_count, small_form, small_tag};
use crate::wire::{
    BitmosaicView, CView, EMPTY_U32_IMAGE, Forest, Geometry, Root as ViewRoot, ef_header_len,
    forest_header_len, frame_len, runs_payload_len, slot_bits_for_span, word_window,
    words_payload_len, write_ef_header,
};
use crate::{ARRAY_MAX, AdmissionClass, AdmissionError, Arm, CHUNK_WORDS, choose_arm};

/// Which of the writer's images of a set pass.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Root {
    /// The sealed image or the forest-family one a patched slot holds.
    Either,
    /// The sealed image alone.
    Sealed,
}

fn refuse(offset: usize, what: &'static str) -> AdmissionError {
    AdmissionError::new(AdmissionClass::Canonical, offset, what)
}

/// A staged chunk's last member before it has one: its successor is no
/// `u16`, so the first member always opens a run.
const NO_MEMBER: u32 = u32::MAX - 1;
/// The members of one chunk, staged for the ladder: as a list while there
/// are at most [`ARRAY_MAX`], as a plane beyond.
struct Staged {
    values: [u16; ARRAY_MAX],
    plane: [u64; CHUNK_WORDS],
    card: usize,
    runs: usize,
    last: u32,
}

impl Staged {
    fn new() -> Self {
        Self {
            values: [0; ARRAY_MAX],
            plane: [0; CHUNK_WORDS],
            card: 0,
            runs: 0,
            last: NO_MEMBER,
        }
    }

    fn clear(&mut self) {
        if self.card > ARRAY_MAX {
            self.plane.fill(0);
        }
        (self.card, self.runs, self.last) = (0, 0, NO_MEMBER);
    }

    /// Take the next member, ascending.
    #[inline(always)]
    fn push(&mut self, value: u16) {
        self.runs += usize::from(self.last.wrapping_add(1) != u32::from(value));
        self.last = u32::from(value);
        if self.card < ARRAY_MAX {
            self.values[self.card] = value;
        } else {
            if self.card == ARRAY_MAX {
                for &held in &self.values {
                    self.plane[usize::from(held) / 64] |= 1 << (held % 64);
                }
            }
            self.plane[usize::from(value) / 64] |= 1 << (value % 64);
        }
        self.card += 1;
    }

    /// The arm the ladder picks for the staged members, and its payload
    /// length.
    fn arm(&self) -> (Arm, usize) {
        let n = self.card;
        let arm = if n <= ARRAY_MAX {
            choose_arm(&self.values[..n], false)
        } else {
            crate::choose_dense_arm(&self.plane, usize::from(self.values[0]), n)
        };
        let len = match arm {
            Arm::Runs { count } => runs_payload_len(count),
            Arm::Stride { .. } => 4,
            Arm::Cone { .. } => 11 + n,
            Arm::Array => 2 * n,
            Arm::Words => {
                let (lo, hi, _, _) = word_window(&self.plane);
                words_payload_len(hi - lo)
            }
        };
        (arm, len)
    }

    fn first(&self) -> u16 {
        self.values[0]
    }
}

/// The runs of ascending members, and whether two or more of them are one
/// arithmetic progression.
pub(crate) fn runs_and_stride(members: impl Iterator<Item = u32>) -> (usize, bool) {
    let (mut runs, mut even, mut previous, mut step, mut seen) = (0usize, true, 0u32, 0u32, 0u32);
    for member in members {
        if seen == 0 {
            runs = 1;
        } else {
            let gap = member.wrapping_sub(previous);
            runs += usize::from(gap != 1);
            if seen == 1 {
                step = gap;
            } else {
                even &= gap == step;
            }
        }
        previous = member;
        seen += 1;
    }
    (runs, even && seen >= 2)
}

/// A floor under the directory image of one chunk at key zero holding
/// `card` members (at most 4,096) in `runs` runs: its seven bytes of
/// framing and the cheapest payload any arm the ladder could pick has. One
/// member is an Array of two bytes; a progression is a Stride of four;
/// otherwise the ladder picks among Cone at `11 + card`, Array at
/// `2 * card` and Runs, and the floor takes the least of the three as if
/// the Cone fitted.
fn directory_floor(card: u32, runs: usize, stride: bool) -> usize {
    let n = card as usize;
    7 + if n == 1 {
        2
    } else if stride {
        4
    } else {
        // Runs costs at least four bytes a run.
        (11 + n).min(2 * n).min(runs.saturating_mul(4))
    }
}

/// The small form the selection rule gives a set of one chunk at key zero,
/// or `None` for its directory. `payload` is the ladder: the payload
/// length of the arm it picks for the members. It is asked only when the
/// shortest small form is not below [`directory_floor`]: below it the small
/// form is shorter than every directory the set could have, whatever the
/// ladder says. More than 2,048 members fit no small form but an interval
/// list, which is shorter than any directory of its members (four bytes a
/// run, where Runs costs more and every other arm a byte a member).
pub(crate) fn small_choice(
    first: u16,
    last: u16,
    card: u32,
    runs: usize,
    stride: bool,
    payload: impl FnOnce() -> usize,
) -> Option<(u32, usize)> {
    let shortest = small_form(usize::MAX, first, last, card, runs)?;
    if card > 2048 || shortest.1 < directory_floor(card, runs, stride) {
        return Some(shortest);
    }
    small_form(
        directory_len(1, 0, 0, u64::from(card), payload()),
        first,
        last,
        card,
        runs,
    )
}
/// The length of a directory image of `chunks` chunks from key `first` to
/// key `last`, `members` in all, whose payloads total `payloads` bytes.
pub(crate) fn directory_len(
    chunks: u32,
    first: u16,
    last: u16,
    members: u64,
    payloads: usize,
) -> usize {
    let header = forest_header_len(members, chunks);
    let geometry = Geometry::new(chunks, slot_bits_for_span(chunks, first, last), header);
    frame_len(geometry.payload_at + payloads)
}

/// The length of the sealed Elias-Fano image of `n` members from `first` to
/// `last`, when the set has one.
fn elias_fano_len(n: u64, first: u32, last: u32) -> Option<usize> {
    if n < 2 {
        return None;
    }
    let layout = EfLayout::new(n, u64::from(last - first))?;
    Some(frame_len(
        ef_header_len(n, first, last - first) + layout.payload_bytes(),
    ))
}

/// The payload length of the arm the ladder picks for at most 2,048
/// ascending members below 65,536. Its own cold function: the members are
/// staged on 4 KiB of stack, which the paths that never ask do not zero.
#[cold]
#[inline(never)]
fn ladder_len(members: impl Iterator<Item = u32>) -> usize {
    let mut values = [0u16; 2048];
    let mut at = 0;
    for (slot, member) in values.iter_mut().zip(members) {
        *slot = member as u16;
        at += 1;
    }
    arm_len(&values[..at])
}
/// The forest family's length for the `n` ascending members `walk` hands
/// over, from `first` to `last`: chunk by chunk through the ladder.
#[inline(never)]
fn forest_family_len(
    n: u64,
    first: u32,
    last: u32,
    walk: impl FnOnce(&mut dyn FnMut(u32)),
) -> usize {
    let mut staged = Staged::new();
    let (mut chunks, mut payloads, mut key) = (0u32, 0usize, 0u16);
    let mut only_small = None;
    let mut close = |staged: &mut Staged, key: u16| {
        let (_, len) = staged.arm();
        payloads += len;
        chunks += 1;
        if chunks == 1 && key == 0 {
            only_small = Some((staged.first(), staged.last, staged.card, staged.runs));
        }
        staged.clear();
    };
    walk(&mut |value| {
        let high = (value >> 16) as u16;
        if staged.card != 0 && high != key {
            close(&mut staged, key);
        }
        key = high;
        staged.push(value as u16);
    });
    close(&mut staged, key);
    let directory = directory_len(
        chunks,
        (first >> 16) as u16,
        (last >> 16) as u16,
        n,
        payloads,
    );
    match only_small {
        Some((first, last, card, runs)) if chunks == 1 => {
            small_form(directory, first, last as u16, card as u32, runs)
                .map_or(directory, |(_, len)| 1 + len)
        }
        _ => directory,
    }
}

// ── writing the sealed image from an ascending slice ─────────────────────

/// The shortest Elias-Fano image: its control, body length, three header
/// varints and one high-plane word.
const ELIAS_FANO_MIN: usize = 13;

/// What [`write_image_into`] writes for a set.
enum Sealed {
    Empty,
    Small { kind: u32, payload: usize },
    EliasFano { layout: EfLayout, body: usize },
    Directory { total: usize },
}

/// The small form of ascending members that all lie below 65,536, if the
/// forest family writes one ([`small_choice`]).
fn small_of(members: &[u32]) -> Option<(u32, usize)> {
    let (first, last) = (*members.first()?, *members.last()?);
    if last > 0xFFFF {
        return None;
    }
    let (runs, stride) = runs_and_stride(members.iter().copied());
    small_choice(
        first as u16,
        last as u16,
        members.len() as u32,
        runs,
        stride,
        || ladder_len(members.iter().copied()),
    )
}

fn sealed(members: &[u32]) -> Sealed {
    let (Some(&first), Some(&last)) = (members.first(), members.last()) else {
        return Sealed::Empty;
    };
    let n = members.len() as u64;
    let small = small_of(members);
    let forest = match small {
        Some((_, payload)) => 1 + payload,
        None => forest_family_len(n, first, last, |f| members.iter().copied().for_each(f)),
    };
    if forest >= ELIAS_FANO_MIN
        && n >= 2
        && let Some(layout) = EfLayout::new(n, u64::from(last - first))
    {
        let body = ef_header_len(n, first, last - first) + layout.payload_bytes();
        if frame_len(body) < forest {
            return Sealed::EliasFano { layout, body };
        }
    }
    match small {
        Some((kind, payload)) => Sealed::Small { kind, payload },
        None => Sealed::Directory { total: forest },
    }
}

/// The length of the sealed image of the set of `ascending`, which is
/// strictly ascending: what [`write_image_into`] writes and
/// [`crate::Bitmosaic::to_bytes`] would. No allocation; a set that is not a
/// small image is laddered chunk by chunk on 16 KiB of stack.
pub fn image_len(ascending: &[u32]) -> usize {
    match sealed(ascending) {
        Sealed::Empty => EMPTY_U32_IMAGE.len(),
        Sealed::Small { payload, .. } => 1 + payload,
        Sealed::EliasFano { body, .. } => frame_len(body),
        Sealed::Directory { total } => total,
    }
}

/// Write the sealed image of the set of `ascending` at the head of `out`
/// and return its length: byte-equal to
/// `Bitmosaic::from_sorted(ascending).to_bytes()`, without building the
/// set. A small image is one pass over the members and allocates nothing,
/// and neither does an Elias-Fano one; a directory is laid out through
/// `scratch`, which a writer keeps and which allocates nothing once it has
/// grown to its work.
///
/// [`PatchError::Unsorted`] for members that do not ascend strictly;
/// [`PatchError::Capacity`] names the bytes `out` needs, and leaves it
/// untouched.
pub fn write_image_into(
    ascending: &[u32],
    out: &mut [u8],
    scratch: &mut PatchScratch,
) -> Result<usize, PatchError> {
    if !ascending.windows(2).all(|pair| pair[0] < pair[1]) {
        return Err(PatchError::Unsorted);
    }
    let plan = sealed(ascending);
    let needed = match plan {
        Sealed::Empty => EMPTY_U32_IMAGE.len(),
        Sealed::Small { payload, .. } => 1 + payload,
        Sealed::EliasFano { body, .. } => frame_len(body),
        Sealed::Directory { total } => total,
    };
    let Some(image) = out.get_mut(..needed) else {
        return Err(PatchError::Capacity { needed });
    };
    match plan {
        Sealed::Empty => image.copy_from_slice(&EMPTY_U32_IMAGE),
        Sealed::Small { kind, payload } => {
            image[0] = small_tag(kind, small_count(kind, payload));
            let mut sink = SmallSink::new(&mut image[1..], kind, ascending[0] as u16);
            for &member in ascending {
                sink.push(member as u16);
            }
        }
        Sealed::EliasFano { layout, body } => {
            let (first, last) = (ascending[0], ascending[ascending.len() - 1]);
            let at = write_ef_header(image, ascending.len() as u64, body, first, last - first);
            ef::encode_into(ascending.iter().copied(), first, layout, &mut image[at..]);
        }
        Sealed::Directory { .. } => {
            // The forest family's image of a directory is what a patch of
            // the empty slot by the whole set leaves.
            image[0] = EMPTY_U32_IMAGE[0];
            crate::patch::patch(image, ascending, &[], scratch)?;
        }
    }
    Ok(needed)
}
/// Check an admitted, verified u32 image. `framed` is false for a u64
/// directory's child, which carries no body length: lengths are compared as
/// standalone images either way.
pub(crate) fn check(
    view: &BitmosaicView<'_>,
    root: Root,
    framed: bool,
) -> Result<(), AdmissionError> {
    let n = view.len();
    if n == 0 {
        return Ok(());
    }
    let (first, last) = (view.select(0).unwrap_or(0), view.select(n - 1).unwrap_or(0));
    let forest_family = match &view.root {
        ViewRoot::Ef(ef) => {
            if root == Root::Either {
                return Ok(());
            }
            let forest = forest_family_len(n, first, last, |f| ef.for_each(f));
            let own = elias_fano_len(n, first, last).unwrap_or(usize::MAX);
            if own >= forest {
                return Err(refuse(
                    0,
                    "an Elias-Fano root is not strictly shorter than the forest family's image",
                ));
            }
            return Ok(());
        }
        ViewRoot::Forest(forest) => forest,
    };
    let standalone = if framed || forest_family.chunks == 0 {
        view.serialized_len()
    } else {
        frame_len(view.serialized_len() - 1)
    };
    if forest_family.chunks == 0 {
        small(forest_family.bytes)?;
    } else {
        directory(forest_family, standalone)?;
    }
    if root == Root::Sealed
        && elias_fano_len(n, first, last).is_some_and(|sealed| sealed < standalone)
    {
        return Err(refuse(
            0,
            "the set's Elias-Fano image is strictly shorter than this one",
        ));
    }
    Ok(())
}

/// The payload length of the arm the ladder picks for `values`, at most
/// [`ARRAY_MAX`] ascending members.
pub(crate) fn arm_len(values: &[u16]) -> usize {
    match choose_arm(values, false) {
        Arm::Runs { count } => runs_payload_len(count),
        Arm::Stride { .. } => 4,
        Arm::Cone { .. } => 11 + values.len(),
        Arm::Array | Arm::Words => 2 * values.len(),
    }
}

/// A small image is the small form its set selects.
fn small(image: &[u8]) -> Result<(), AdmissionError> {
    let Some(set) = SmallSet::of(image) else {
        return Ok(());
    };
    let Some((kind, _)) = image.first().and_then(|&tag| read_small_tag(tag)) else {
        return Ok(());
    };
    let card = set.cardinality();
    let (first, last) = (
        set.select(0).unwrap_or(0) as u16,
        set.select(card - 1).unwrap_or(0) as u16,
    );
    // The members are staged for the ladder only where a directory could
    // be as short as the small form ([`small_choice`]).
    let (runs, stride) = runs_and_stride(crate::SmallView::new(set).iter());
    let chosen = small_choice(first, last, card, runs, stride, || {
        ladder_len(crate::SmallView::new(set).iter())
    });
    if chosen != Some((kind, image.len() - 1)) {
        return Err(refuse(
            0,
            "the set selects another form than this small one",
        ));
    }
    Ok(())
}

/// Every chunk of a directory is in its ladder arm, with the parameters the
/// writer derives, and a directory of one chunk at key zero has no small
/// form as short.
fn directory(forest: &Forest<'_>, standalone: usize) -> Result<(), AdmissionError> {
    let mut staged = None;
    for i in 0..forest.chunks as usize {
        let Some(chunk) = forest.container(i) else {
            continue;
        };
        let card = chunk.cardinality() as usize;
        let at = forest.payload_at(i);
        let other_kind = || refuse(at, "the chunk's members select another container kind");
        // The chunk's runs where its view says them without a walk.
        let runs = match chunk {
            // Runs is the ladder's pick from its counts alone when no other
            // arm can cost as little: Cone costs 16 and a byte a member
            // whatever its fit, Array two bytes a member, and two or more
            // runs that are not all singletons are no Stride.
            CView::Runs(runs) => {
                let r = runs.count();
                let canonical = if card <= ARRAY_MAX {
                    r >= 2 && 6 * r < 2 * card && (6 * r < 16 + card || card > 1024)
                } else {
                    r >= 2 && 6 * r < crate::WORDS_HEAP_BYTES
                };
                canonical.then_some(r)
            }
            // Any two or more members with equal gaps are a Stride.
            CView::Stride { stride, .. } => {
                if card < 2 {
                    return Err(other_kind());
                }
                Some(if stride == 1 { 1 } else { card })
            }
            // One member is an Array.
            CView::Array(_) if card == 1 => Some(1),
            _ => None,
        };
        let (first, last, runs) = match runs {
            Some(runs) => (chunk.select(0), chunk.select(card as u32 - 1), runs),
            None => {
                // The members, through the ladder.
                let staged = staged.get_or_insert_with(Staged::new);
                staged.clear();
                if card <= ARRAY_MAX {
                    chunk.for_each(|value| staged.push(value));
                } else {
                    // Past the list: the plane, by the patch's own decoder.
                    staged.plane.fill(0);
                    stage(&mut staged.plane, chunk);
                    staged.card = card;
                    staged.values[0] = chunk.select(0);
                    staged.runs = usize::MAX;
                    staged.last = u32::from(chunk.select(card as u32 - 1));
                }
                let (arm, _) = staged.arm();
                let same = match (arm, chunk) {
                    (
                        Arm::Cone { scale, eps, .. },
                        CView::Cone {
                            scale: held_scale,
                            eps: held_eps,
                            ..
                        },
                    ) => {
                        if scale != held_scale || eps != held_eps {
                            return Err(refuse(
                                at + 2,
                                "the Cone's scale or eps is not the one its members give",
                            ));
                        }
                        true
                    }
                    (Arm::Array, CView::Array(_))
                    | (Arm::Words, CView::Words { .. })
                    | (Arm::Runs { .. }, CView::Runs(_)) => true,
                    _ => false,
                };
                if !same {
                    return Err(other_kind());
                }
                (staged.first(), staged.last as u16, staged.runs)
            }
        };
        if forest.chunks == 1
            && forest.key(0) == 0
            && small_form(standalone, first, last, card as u32, runs).is_some()
        {
            return Err(refuse(
                0,
                "one chunk at key zero has a small form no longer than its directory",
            ));
        }
    }
    Ok(())
}
