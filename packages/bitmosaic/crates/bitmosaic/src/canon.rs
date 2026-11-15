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
use crate::patch::stage;
use crate::small::{SmallSet, read_small_tag, small_form};
use crate::wire::{
    BitmosaicView, CView, Forest, Geometry, Root as ViewRoot, ef_header_len, forest_header_len,
    frame_len, runs_payload_len, slot_bits_for_span, word_window, words_payload_len,
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
    let (first, last) = (view.select(0).unwrap_or(0), view.select(n - 1).unwrap_or(0));
    let forest_family = match &view.root {
        ViewRoot::Ef(ef) => {
            if root == Root::Either {
                return Ok(());
            }
            // The forest family's length for this set, chunk by chunk.
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
fn arm_len(values: &[u16]) -> usize {
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
    // The directory this set would otherwise be, and its runs. An interval
    // list of more than 256 members is shorter than any directory of them:
    // its payload is four bytes a run, where Runs costs more and every
    // other arm at least one byte a member.
    let (mut runs, mut next) = (0usize, u32::MAX);
    let mut count_runs = |value: u32| {
        runs += usize::from(value != next);
        next = value + 1;
    };
    let payload = if let SmallSet::Spans(spans) = set
        && card > 256
    {
        runs = spans.len() / 4;
        usize::MAX / 2
    } else if card <= 256 {
        let mut values = [0u16; 256];
        let mut at = 0;
        set.for_each(|value| {
            count_runs(value);
            values[at] = value as u16;
            at += 1;
        });
        arm_len(&values[..at])
    } else {
        // A wide window of up to 2,048 members.
        let mut values = [0u16; 2048];
        let mut at = 0;
        set.for_each(|value| {
            count_runs(value);
            if let Some(slot) = values.get_mut(at) {
                *slot = value as u16;
                at += 1;
            }
        });
        arm_len(&values[..at])
    };
    let directory = if payload == usize::MAX / 2 {
        usize::MAX
    } else {
        directory_len(1, 0, 0, u64::from(card), payload)
    };
    if small_form(directory, first, last, card, runs) != Some((kind, image.len() - 1)) {
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
