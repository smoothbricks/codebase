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

use crate::ef::EfLayout;
use crate::patch::{kind_of, stage};
use crate::small::{SmallSet, read_small_tag, small_form, small_payload_len};
use crate::wire::{
    BitmosaicView, CView, Forest, Geometry, K_WORDS, Root as ViewRoot, ef_header_len,
    forest_header_len, frame_len, runs_payload_len, slot_bits_for_span, word_window,
    words_payload_len,
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

    /// The arm the ladder picks for the staged members, as its kind and
    /// payload length.
    fn arm(&self) -> (u32, usize) {
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
        (kind_of(arm), len)
    }

    fn first(&self) -> u16 {
        self.values[0]
    }
}

/// The length of a directory image of `chunks` chunks from key `first` to
/// key `last`, `members` in all, whose payloads total `payloads` bytes.
fn directory_len(chunks: u32, first: u16, last: u16, members: u64, payloads: usize) -> usize {
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
    let mut staged = Staged::new();
    let (first, last) = (view.select(0).unwrap_or(0), view.select(n - 1).unwrap_or(0));
    let forest_family = match &view.root {
        ViewRoot::Ef(ef) => {
            if root == Root::Either {
                return Ok(());
            }
            // The forest family's length for this set, chunk by chunk.
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
            ef.for_each(|value| {
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
            let forest = match only_small {
                Some((first, last, card, runs)) if chunks == 1 => {
                    small_form(directory, first, last as u16, card as u32, runs)
                        .map_or(directory, |(_, len)| 1 + len)
                }
                _ => directory,
            };
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
        small(forest_family.bytes, &mut staged)?;
    } else {
        directory(forest_family, standalone, &mut staged)?;
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

/// A small image is the small form its set selects.
fn small(image: &[u8], staged: &mut Staged) -> Result<(), AdmissionError> {
    let Some(set) = SmallSet::of(image) else {
        return Ok(());
    };
    let Some((kind, count)) = image.first().and_then(|&tag| read_small_tag(tag)) else {
        return Ok(());
    };
    let card = set.cardinality();
    let (first, last) = (
        set.select(0).unwrap_or(0) as u16,
        set.select(card - 1).unwrap_or(0) as u16,
    );
    // The directory this set would otherwise be. An interval list of more
    // than 256 members is shorter than any directory of them: its payload
    // is four bytes a run, where Runs costs more and every other arm at
    // least one byte a member.
    let (runs, directory) = if let SmallSet::Spans(spans) = set
        && card as usize > 256
    {
        (spans.len() / 4, usize::MAX)
    } else {
        set.for_each(|value| staged.push(value as u16));
        let (_, payload) = staged.arm();
        (
            staged.runs,
            directory_len(1, 0, 0, u64::from(card), payload),
        )
    };
    if small_form(directory, first, last, card, runs)
        != Some((kind, small_payload_len(kind, count)))
    {
        return Err(refuse(
            0,
            "the set selects another form than this small one",
        ));
    }
    Ok(())
}

/// Every chunk of a directory is in its ladder arm, and a directory of one
/// chunk at key zero has no small form as short.
fn directory(
    forest: &Forest<'_>,
    standalone: usize,
    staged: &mut Staged,
) -> Result<(), AdmissionError> {
    for i in 0..forest.chunks as usize {
        let Some(chunk) = forest.container(i) else {
            continue;
        };
        let card = chunk.cardinality() as usize;
        let at = forest.payload_at(i);
        staged.clear();
        let kind = if card <= ARRAY_MAX {
            chunk.for_each(|value| staged.push(value));
            staged.arm().0
        } else {
            // Past the list: the plane, staged by the patch's own decoder.
            staged.plane.fill(0);
            stage(&mut staged.plane, chunk);
            staged.card = card;
            staged.values[0] = chunk.select(0);
            staged.arm().0
        };
        let held = match chunk {
            CView::Words { .. } => K_WORDS,
            CView::Stride { .. } => crate::wire::K_STRIDE,
            CView::Cone { .. } => crate::wire::K_CONE,
            CView::Array(_) => crate::wire::K_ARRAY,
            CView::Runs(_) => crate::wire::K_RUNS,
        };
        if kind != held {
            return Err(refuse(
                at,
                "the chunk's members select another container kind",
            ));
        }
        if forest.chunks == 1
            && forest.key(0) == 0
            && card <= ARRAY_MAX
            && small_form(
                standalone,
                staged.first(),
                staged.last as u16,
                card as u32,
                staged.runs,
            )
            .is_some()
        {
            return Err(refuse(
                0,
                "one chunk at key zero has a small form no longer than its directory",
            ));
        }
    }
    Ok(())
}
