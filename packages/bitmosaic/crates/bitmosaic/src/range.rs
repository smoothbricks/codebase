//! Ranges in Alexandrescu's D sense: `empty` / `front` / `pop_front` as
//! three SEPARATE primitives, rather than one fused `next() -> Option<T>`.
//!
//! # Why not `Iterator` alone
//!
//! Rust's `Iterator` fuses "is there another element", "what is it" and
//! "consume it" into a single call. That fusion is free for a consumer that
//! wants every element in order, and it is exactly wrong for a **merge**,
//! which is the shape every set operation in this crate has: compare two
//! heads and advance only ONE of them.
//!
//! Expressed over `Iterator` that needs `Peekable`, which buffers an
//! `Option<T>` per source and adds a branch and a store per step — and the
//! buffer holds *state the cursor already had*, copied. With `front`
//! idempotent the merge body is the plain three-way compare the
//! `PERFORMANCE-HANDBOOK` §7.4 measured at 0.68 ns/iter against 1.66 for the
//! branchless form:
//!
//! ```text
//! while !a.empty() && !b.empty() {
//!     let (x, y) = (a.front(), b.front());
//!     if x < y { a.pop_front() } else if y < x { b.pop_front() } else { .. }
//! }
//! ```
//!
//! `examples/g217_ranges.rs` races that body against the identical merge
//! over `Peekable`, both arms in ONE binary, and publishes the ratio with
//! the like-arm null floor measured in the same run. No absolute ns lives
//! in this file: an absolute is a property of the code AND the machine's
//! state at that moment, and this box is shared.
//!
//! A range is also a VALUE — [`Range`] requires `Copy` — so [`Range::save`]
//! is a register copy of a positioned cursor. That is the operation a
//! galloping/exponential search needs (probe ahead from a checkpoint,
//! abandon the probe for free) and the one `Iterator` cannot express at all,
//! because `Iterator` is a `&mut` protocol over an opaque state machine.
//!
//! # What this deletes
//!
//! Before this module, every allocation-free case was hand-written per
//! *operation* × *consumption shape*: `Bitmosaic::and_len` counts, `Bitmosaic64`
//! got no equivalent, `BitmosaicView::and_into` fills a caller buffer, and
//! `or`/`xor`/`andnot` had no alloc-free form at any shape — they built a
//! whole new `Bitmosaic`. Ranges make that an N + M problem: four lazy adaptors
//! ([`AndRange`], [`OrRange`], [`XorRange`], [`AndNotRange`]) compose over
//! anything implementing the protocol, which is how the u64 tier gets all
//! four without a line of per-tier code.
//!
//! # Two surfaces, no collision
//!
//! Every range here implements BOTH [`Range`] and [`Iterator`]. That is only
//! possible because `Range` names nothing `Iterator` names: a type carrying
//! `Range::fold` and `Iterator::fold` makes `r.fold(..)` an ambiguous-method
//! error at the call site. So the protocol trait is four methods, and
//! iteration — including the specialised `fold` that resolves the container
//! arm once — lives on the `Iterator` impls.
//!
//! That split is load-bearing, not cosmetic. Pulling re-loads the container
//! discriminant per element where `fold` resolves it once, and `g217`
//! measures the gap as a large multiple on the cheap arms — `Array` worst,
//! because there the per-element work `fold` deletes IS the whole cost. A
//! consumer that wants the whole range (`count`, `sum`, `collect`,
//! `extend`) reaches `fold` and pays the loop price; a merge must pull, and
//! pays the cursor price to advance one side at a time. Both are the right
//! trade for their caller, and neither is free.
//!
//! # The fit criterion still binds
//!
//! Settled by measurement (G213/G213.1): **rank, contains and `and_len`
//! win; `select` in a loop does not.** Every range here ADVANCES A CURSOR.
//! None of them calls `select`, at any tier. Nor does any of them replace
//! `and_len`: `g217` panel 2 measures the fused counting kernel at orders of
//! magnitude ahead of counting a lazy intersection on the arms with a
//! closed-form or word-popcount kernel, because it never looks at a member.
//! The ranges add a consumption shape; they retire nothing.

use crate::{Bitmosaic, Bitmosaic64, CHUNK_WORDS, Container};

// ─────────────────────────────────────────────────────────────────────────
// The protocol
// ─────────────────────────────────────────────────────────────────────────

/// An ascending input range: three primitives, separately callable.
///
/// The contract, and it is what the adaptors below rely on:
///
/// - `front` has the precondition `!empty()`, and is **idempotent**: calling
///   it twice without an intervening `pop_front` returns the same value and
///   observes nothing. Implementations therefore keep the head materialised
///   rather than computing it on demand — see [`ContainerRange::words`],
///   which normalises to a non-zero word so `front` is a `trailing_zeros`
///   over state already in a register.
/// - `pop_front` has the precondition `!empty()` and advances by exactly one.
/// - Elements are yielded in **strictly ascending** order. The set adaptors
///   are merges and are wrong without it.
///
/// `Copy` is a supertrait rather than a bound on `save`, because a cheap
/// snapshot is the protocol's distinguishing property and not an optional
/// extra. It also keeps the adaptors' `Copy` derivation honest: they hold
/// their sources by value.
pub trait Range: Copy {
    type Item: Copy + Ord;

    fn empty(&self) -> bool;

    /// The current element. Precondition: `!self.empty()`. Idempotent.
    fn front(&self) -> Self::Item;

    /// Advance past the current element. Precondition: `!self.empty()`.
    fn pop_front(&mut self);

    /// Advance to the first element at or after `target`; never moves back.
    ///
    /// The primitive a merge needs and `pop_front` cannot express. Without
    /// it a lazy intersection is CARDINALITY-BLIND by construction: it can
    /// only step, so it costs `|A| + |B|` cursor advances whatever the
    /// cardinalities are, where a source able to jump makes the same
    /// intersection cost the SMALLER side. Both wire cursors
    /// ([`crate::BitmosaicViewRange`], [`crate::Bitmosaic64ViewRange`]) have
    /// carried an inherent `seek` — a directory walk plus at most one
    /// chunk-local `rank_below` — since the views were written, and
    /// [`AndRange`] could not reach it because the protocol had three
    /// methods and none of them was this one.
    ///
    /// The default is the stepping loop, so a source that cannot jump is
    /// correct without writing anything, and an override is a pure
    /// acceleration of a settled contract rather than a new one.
    ///
    /// Postcondition: `empty()`, or `front() >= target`. A `target` at or
    /// below the current head is a no-op — the cursor is monotone and a seek
    /// never rewinds it.
    #[inline]
    fn seek(&mut self, target: Self::Item) {
        while !self.empty() && self.front() < target {
            self.pop_front();
        }
    }

    /// A copy of this cursor, positioned exactly where this one is.
    ///
    /// D's `save`. A range is a value, so checkpointing one is a copy of a
    /// few registers: no allocation, no `Rc`, no re-traversal.
    #[inline]
    fn save(&self) -> Self {
        *self
    }
}

// ─────────────────────────────────────────────────────────────────────────
// The Iterator bridge
// ─────────────────────────────────────────────────────────────────────────

/// `Iterator` over any [`Range`]: the three primitives fused back together.
///
/// Every range in THIS crate implements `Iterator` directly, with a
/// specialised `fold`, so none of them needs this. It exists for a FOREIGN
/// `Range` — one written against the protocol by a consumer — which would
/// otherwise have to hand-write an `Iterator` impl to reach `collect`, and
/// could not be fed to the adaptors at all, since those need both surfaces.
///
/// So this forwards BOTH: `Iterator` by fusing the primitives, and `Range`
/// by delegating them. `RangeIter<ForeignRange>` therefore satisfies the
/// adaptors' `Range + Iterator` bound, which is what makes "the adaptors
/// compose over anything implementing the protocol" true rather than
/// aspirational.
///
/// Being generic it can only offer the pull loop, where the concrete ranges
/// carry a `fold` that resolves the container arm once; prefer them.
#[derive(Clone, Copy, Debug)]
pub struct RangeIter<R>(pub R);

impl<R: Range> RangeIter<R> {
    #[inline]
    pub fn new(range: R) -> Self {
        RangeIter(range)
    }

    /// The underlying range, positioned where this bridge is.
    #[inline]
    pub fn into_inner(self) -> R {
        self.0
    }
}

impl<R: Range> Iterator for RangeIter<R> {
    type Item = R::Item;

    #[inline]
    fn next(&mut self) -> Option<R::Item> {
        if self.0.empty() {
            return None;
        }
        let v = self.0.front();
        self.0.pop_front();
        Some(v)
    }
}

impl<R: Range> Range for RangeIter<R> {
    type Item = R::Item;

    #[inline(always)]
    fn empty(&self) -> bool {
        self.0.empty()
    }

    #[inline(always)]
    fn front(&self) -> R::Item {
        self.0.front()
    }

    #[inline(always)]
    fn pop_front(&mut self) {
        self.0.pop_front();
    }

    /// Forwarded, not defaulted. The bridge's whole job is that a FOREIGN
    /// range reaches the adaptors unchanged, and a bridge that answered
    /// `seek` with the stepping default would silently discard whatever
    /// accelerated seek the consumer wrote — a false null of exactly the
    /// shape this protocol addition exists to remove. The seek oracle's
    /// negative half catches it: it plants a wrong foreign `seek` and
    /// requires the leapfrog arm's answer to change.
    #[inline(always)]
    fn seek(&mut self, target: R::Item) {
        self.0.seek(target);
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Source: one container
// ─────────────────────────────────────────────────────────────────────────

/// Ascending cursor over one container's members, produced by
/// `Container::range`. The crate's ONLY container cursor.
///
/// `Words` is the arm that matters: it holds the index of the next word to
/// load and the unconsumed set bits of the current one, so a traversal costs
/// one load per 64 positions. Driving the same traversal through `select`
/// would re-enter `select_group` and rescan from a directory group boundary
/// for every element — `O(n · DIR_STRIDE · 64)` bit inspections for a
/// sequence available in `O(n)`. That relation is pinned as an EQUALITY by
/// `a_words_walk_costs_its_span_where_select_costs_its_cardinality`, which
/// is load-immune where a timing is not.
///
/// The other three arms were already `O(1)` per element under `select`; they
/// carry a cursor here so the forest tiers have one shape to drive.
///
/// `Copy`, so `save` is a copy of at most four machine words. The `Array`
/// arm holds a slice rather than a `slice::Iter` for exactly that reason —
/// `slice::Iter` is `Clone` but not `Copy`, and `split_first` compiles to
/// the same instruction sequence.
#[derive(Clone, Copy)]
pub(crate) enum ContainerRange<'a> {
    Words {
        words: &'a [u64; CHUNK_WORDS],
        /// Index of the next word to load; `bits` belongs to `word - 1`.
        word: usize,
        /// Unconsumed set bits of the word at `word - 1`. Non-zero whenever
        /// `remaining > 0` — the invariant that makes `front` a pure read.
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
        residuals: &'a [i8],
        at: usize,
    },
    Array(&'a [u16]),
    Runs {
        runs: &'a [crate::Run],
        at: usize,
        next: u32,
        remaining: u32,
    },
}

impl<'a> ContainerRange<'a> {
    /// Cursor over a word bitmap, normalised so `bits` already holds the
    /// first unconsumed word.
    ///
    /// The normalisation is what buys an idempotent `front`. A lazily
    /// refilling cursor has states where `remaining > 0` and `bits == 0`, so
    /// its head is only knowable by mutating — which is `Peekable` again.
    /// Doing the refill in `pop_front` instead moves the same work one step
    /// earlier and costs nothing: the loop is bounded by the occupied span
    /// either way, and it stops at `remaining == 0` rather than scanning the
    /// chunk's empty tail.
    #[inline]
    pub(crate) fn words(words: &'a [u64; CHUNK_WORDS], len: u32) -> Self {
        let (mut word, mut bits) = (0usize, 0u64);
        if len != 0 {
            while bits == 0 && word < CHUNK_WORDS {
                bits = words[word];
                word += 1;
            }
        }
        ContainerRange::Words {
            words,
            word,
            bits,
            remaining: len,
        }
    }

    #[inline(always)]
    fn remaining(&self) -> usize {
        match self {
            ContainerRange::Runs { remaining, .. }
            | ContainerRange::Words { remaining, .. }
            | ContainerRange::Stride { remaining, .. } => *remaining as usize,
            ContainerRange::Cone { residuals, at, .. } => residuals.len() - *at,
            ContainerRange::Array(values) => values.len(),
        }
    }
}

impl Range for ContainerRange<'_> {
    type Item = u16;

    #[inline(always)]
    fn empty(&self) -> bool {
        match self {
            ContainerRange::Runs { remaining, .. }
            | ContainerRange::Words { remaining, .. }
            | ContainerRange::Stride { remaining, .. } => *remaining == 0,
            ContainerRange::Cone { residuals, at, .. } => *at >= residuals.len(),
            ContainerRange::Array(values) => values.is_empty(),
        }
    }

    #[inline(always)]
    fn front(&self) -> u16 {
        debug_assert!(!self.empty(), "front on an empty range");
        match self {
            // `bits` belongs to word - 1 and is non-zero while remaining > 0.
            ContainerRange::Words { word, bits, .. } => {
                ((*word as u32).wrapping_sub(1).wrapping_mul(64) + bits.trailing_zeros()) as u16
            }
            ContainerRange::Runs { next, .. } | ContainerRange::Stride { next, .. } => *next as u16,
            ContainerRange::Cone {
                first,
                scale,
                residuals,
                at,
            } => Container::cone_value(*first, *scale, residuals, *at),
            ContainerRange::Array(values) => values[0],
        }
    }

    #[inline(always)]
    fn pop_front(&mut self) {
        debug_assert!(!self.empty(), "pop_front on an empty range");
        match self {
            ContainerRange::Runs {
                runs,
                at,
                next,
                remaining,
            } => {
                *remaining -= 1;
                *next += 1;
                if *remaining != 0 && *next > u32::from(runs[*at].end) {
                    *at += 1;
                    *next = u32::from(runs[*at].start);
                }
            }
            ContainerRange::Words {
                words,
                word,
                bits,
                remaining,
            } => {
                *bits &= *bits - 1;
                *remaining -= 1;
                // The exhaustion test lives INSIDE the refill, not beside
                // it. Both are predictable and not-taken 63 times in 64, but
                // an unconditional one is still a compare and a branch on
                // EVERY element, where this one is reached only when the
                // current word runs dry. Exhausted, it leaves `word` at the
                // last member's word — so the cursor's load count is the
                // occupied span and never the chunk's empty tail, which is
                // the equality `a_words_walk_costs_its_span_...` pins.
                while *bits == 0 {
                    // `remaining` is the freeze-time popcount of `words`, so
                    // a set bit exists below CHUNK_WORDS; the bound is a
                    // constant compare that keeps the load in safe code.
                    if *remaining == 0 || *word >= CHUNK_WORDS {
                        *remaining = 0;
                        return;
                    }
                    *bits = words[*word];
                    *word += 1;
                }
            }
            ContainerRange::Stride {
                next,
                stride,
                remaining,
            } => {
                *next += *stride;
                *remaining -= 1;
            }
            ContainerRange::Cone { at, .. } => *at += 1,
            ContainerRange::Array(values) => *values = &values[1..],
        }
    }

    fn seek(&mut self, target: u16) {
        if self.empty() || self.front() >= target {
            return;
        }
        match self {
            Self::Runs {
                runs,
                at,
                next,
                remaining,
            } => {
                let old_rank = u32::from(runs[*at].before) + *next - u32::from(runs[*at].start);
                *at += runs[*at..].partition_point(|run| run.end < target);
                if *at == runs.len() {
                    *remaining = 0;
                    return;
                }
                *next = u32::from(target.max(runs[*at].start));
                let new_rank = u32::from(runs[*at].before) + *next - u32::from(runs[*at].start);
                *remaining -= new_rank - old_rank;
            }
            Self::Stride {
                next,
                stride,
                remaining,
            } => {
                let skip = (u32::from(target) - *next)
                    .div_ceil(*stride)
                    .min(*remaining);
                *next += skip * *stride;
                *remaining -= skip;
            }
            Self::Array(values) => {
                *values = &values[values.partition_point(|value| *value < target)..];
            }
            _ => {
                while !self.empty() && self.front() < target {
                    self.pop_front();
                }
            }
        }
    }
}

impl Iterator for ContainerRange<'_> {
    type Item = u16;

    #[inline(always)]
    fn next(&mut self) -> Option<u16> {
        if Range::empty(self) {
            return None;
        }
        let v = Range::front(self);
        Range::pop_front(self);
        Some(v)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = self.remaining();
        (n, Some(n))
    }

    /// Internal iteration: resolve the arm once, then run a straight loop.
    ///
    /// This is not a convenience override. Pulling through `next` re-loads
    /// the discriminant and spills the cursor to the stack for every single
    /// element; `g217` panel 1 races the two and the gap is a large multiple
    /// on the arms whose per-element work is smallest. `count`, `sum`,
    /// `collect` and `Extend` all route here, so a consumer that takes the
    /// whole container pays the loop price while a consumer that merges
    /// keeps a real cursor.
    #[inline]
    fn fold<B, F: FnMut(B, u16) -> B>(self, init: B, mut f: F) -> B {
        let mut acc = init;
        match self {
            ContainerRange::Runs {
                runs,
                at,
                next,
                remaining,
            } => {
                if remaining == 0 {
                    return acc;
                }
                for (i, run) in runs[at..].iter().enumerate() {
                    let start = if i == 0 { next } else { u32::from(run.start) };
                    for value in start..=u32::from(run.end) {
                        acc = f(acc, value as u16);
                    }
                }
                acc
            }
            ContainerRange::Words {
                words,
                mut word,
                mut bits,
                remaining,
            } => {
                if remaining == 0 {
                    return acc;
                }
                // `bits` belongs to word - 1.
                let mut base = (word as u32).wrapping_sub(1).wrapping_mul(64);
                loop {
                    while bits != 0 {
                        acc = f(acc, (base + bits.trailing_zeros()) as u16);
                        bits &= bits - 1;
                    }
                    if word >= CHUNK_WORDS {
                        return acc;
                    }
                    bits = words[word];
                    base = word as u32 * 64;
                    word += 1;
                }
            }
            ContainerRange::Stride {
                mut next,
                stride,
                remaining,
            } => {
                for _ in 0..remaining {
                    acc = f(acc, next as u16);
                    next += stride;
                }
                acc
            }
            ContainerRange::Cone {
                first,
                scale,
                residuals,
                at,
            } => {
                for i in at..residuals.len() {
                    acc = f(acc, Container::cone_value(first, scale, residuals, i));
                }
                acc
            }
            ContainerRange::Array(values) => values.iter().fold(acc, |a, &v| f(a, v)),
        }
    }
}

impl ExactSizeIterator for ContainerRange<'_> {
    #[inline]
    fn len(&self) -> usize {
        self.remaining()
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Source: a whole u32 forest
// ─────────────────────────────────────────────────────────────────────────

/// Ascending range over every member of an [`Bitmosaic`], produced by
/// [`Bitmosaic::range`] (and by [`Bitmosaic::iter`], which is the same cursor).
///
/// Exact-sized: the cumulative plane already answers the count in O(1), so a
/// consumer collecting the walk reserves once instead of growing. A
/// `flat_map` chain cannot report that — its lower bound is 0 until the
/// outer iterator is exhausted — and the count is a byproduct the forest is
/// holding either way.
///
/// The exact remaining length also preserves the current ordinal for free.
/// At every non-empty position, `set.len() as usize - range.len()` is the
/// zero-based ordinal of `range.front()`. Read it before `pop_front`: no
/// [`Bitmosaic::rank`] call or second directory descent is needed.
#[derive(Clone, Copy)]
pub struct ForestRange<'a> {
    /// Chunks NOT yet opened; `inner` is the one currently open.
    keys: &'a [u16],
    containers: &'a [Container],
    /// Chunk key of `inner`, shifted into place.
    high: u32,
    inner: ContainerRange<'a>,
    remaining: usize,
}

impl<'a> ForestRange<'a> {
    #[inline]
    pub(crate) fn new(forest: &'a Bitmosaic) -> Self {
        let mut range = ForestRange {
            keys: forest.keys(),
            containers: forest.containers(),
            high: 0,
            inner: ContainerRange::Array(&[]),
            remaining: forest.len() as usize,
        };
        range.open_next();
        range
    }

    /// Establish "`inner` is non-empty unless the whole range is": the
    /// forest-tier half of the idempotent-`front` invariant.
    ///
    /// A frozen chunk is never empty, so this opens exactly one container
    /// per call in practice; the loop is the guard, not the expectation.
    ///
    /// `inline(always)`: at plain `#[inline]`, fat LTO left this out of line
    /// and put a `bl` in the caller of every `Bitmosaic::iter()`.
    #[inline(always)]
    fn open_next(&mut self) {
        while Range::empty(&self.inner) {
            // keys and containers are parallel planes of equal length, so
            // one running dry ends the walk.
            let (Some((&key, keys)), Some((container, containers))) =
                (self.keys.split_first(), self.containers.split_first())
            else {
                return;
            };
            self.keys = keys;
            self.containers = containers;
            self.high = u32::from(key) << 16;
            self.inner = container.range();
        }
    }
}

impl Range for ForestRange<'_> {
    type Item = u32;

    #[inline(always)]
    fn empty(&self) -> bool {
        self.remaining == 0
    }

    #[inline(always)]
    fn front(&self) -> u32 {
        debug_assert!(!Range::empty(self), "front on an empty range");
        self.high | u32::from(Range::front(&self.inner))
    }

    #[inline(always)]
    fn pop_front(&mut self) {
        debug_assert!(!Range::empty(self), "pop_front on an empty range");
        Range::pop_front(&mut self.inner);
        self.remaining -= 1;
        self.open_next();
    }

    fn seek(&mut self, target: u32) {
        if self.empty() || self.front() >= target {
            return;
        }
        let high = target & 0xffff_0000;
        while self.remaining != 0 && self.high < high {
            self.remaining -= self.inner.remaining();
            self.inner = ContainerRange::Array(&[]);
            self.open_next();
        }
        if self.remaining != 0 && self.high == high {
            let before = self.inner.remaining();
            self.inner.seek(target as u16);
            self.remaining -= before - self.inner.remaining();
            self.open_next();
        }
    }
}

impl Iterator for ForestRange<'_> {
    type Item = u32;

    #[inline(always)]
    fn next(&mut self) -> Option<u32> {
        if Range::empty(self) {
            return None;
        }
        let v = Range::front(self);
        Range::pop_front(self);
        Some(v)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }

    /// Internal iteration, for the same reason [`ContainerRange::fold`] has
    /// one: the per-chunk arm is resolved once and the walk becomes a loop
    /// per container instead of a state machine per element.
    #[inline]
    fn fold<B, F: FnMut(B, u32) -> B>(self, init: B, mut f: F) -> B {
        let ForestRange {
            keys,
            containers,
            high,
            inner,
            ..
        } = self;
        let mut acc = inner.fold(init, |a, v| f(a, high | u32::from(v)));
        for (&key, container) in keys.iter().zip(containers) {
            let high = u32::from(key) << 16;
            acc = container
                .range()
                .fold(acc, |a, v| f(a, high | u32::from(v)));
        }
        acc
    }
}

impl ExactSizeIterator for ForestRange<'_> {
    #[inline]
    fn len(&self) -> usize {
        self.remaining
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Source: the u64 tier
// ─────────────────────────────────────────────────────────────────────────

/// Ascending range over every member of an [`Bitmosaic64`], produced by
/// [`Bitmosaic64::range`]: a walk over the high-32 plane with a [`ForestRange`]
/// inside it and the prefix restored.
///
/// The u64 tier had no walk at all before this — no `iter`, no `for_each` —
/// which blocked every u64 consumer at the point where it wanted the values
/// rather than a count. It is one more tier of the same cursor pattern,
/// which is the point of having the pattern: `Bitmosaic64` gets the four lazy
/// set operations from the same adaptors, rather than the four hand-written
/// per-operation kernels the u32 tier needed.
///
/// As with [`ForestRange`], the current zero-based ordinal is
/// `set.len() as usize - range.len()` at every non-empty position. The
/// remaining count is cursor state, so reading it before `pop_front` does
/// not call [`Bitmosaic64::rank`] or descend a directory.
#[derive(Clone, Copy)]
pub struct Forest64Range<'a> {
    highs: &'a [u32],
    forests: &'a [Bitmosaic],
    /// High-32 prefix of `inner`, shifted into place.
    high: u64,
    inner: ForestRange<'a>,
    remaining: usize,
}

impl<'a> Forest64Range<'a> {
    #[inline]
    pub(crate) fn new(set: &'a Bitmosaic64) -> Self {
        let mut range = Forest64Range {
            highs: set.highs(),
            forests: set.forests(),
            high: 0,
            // An empty ForestRange borrowing nothing; `open_next` replaces
            // it immediately when the tier holds any member at all.
            inner: ForestRange {
                keys: &[],
                containers: &[],
                high: 0,
                inner: ContainerRange::Array(&[]),
                remaining: 0,
            },
            remaining: set.len() as usize,
        };
        range.open_next();
        range
    }

    #[inline(always)]
    fn open_next(&mut self) {
        while Range::empty(&self.inner) {
            let (Some((&high, highs)), Some((forest, forests))) =
                (self.highs.split_first(), self.forests.split_first())
            else {
                return;
            };
            self.highs = highs;
            self.forests = forests;
            self.high = u64::from(high) << 32;
            self.inner = ForestRange::new(forest);
        }
    }
}

impl Range for Forest64Range<'_> {
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

    fn seek(&mut self, target: u64) {
        if self.empty() || self.front() >= target {
            return;
        }
        let high = target & 0xffff_ffff_0000_0000;
        while self.remaining != 0 && self.high < high {
            self.remaining -= self.inner.remaining;
            self.inner.remaining = 0;
            self.open_next();
        }
        if self.remaining != 0 && self.high == high {
            let before = self.inner.remaining;
            self.inner.seek(target as u32);
            self.remaining -= before - self.inner.remaining;
            self.open_next();
        }
    }
}

impl Iterator for Forest64Range<'_> {
    type Item = u64;

    #[inline(always)]
    fn next(&mut self) -> Option<u64> {
        if Range::empty(self) {
            return None;
        }
        let v = Range::front(self);
        Range::pop_front(self);
        Some(v)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }

    #[inline]
    fn fold<B, F: FnMut(B, u64) -> B>(self, init: B, mut f: F) -> B {
        let Forest64Range {
            highs,
            forests,
            high,
            inner,
            ..
        } = self;
        let mut acc = inner.fold(init, |a, v| f(a, high | u64::from(v)));
        for (&h, forest) in highs.iter().zip(forests) {
            let high = u64::from(h) << 32;
            acc = ForestRange::new(forest).fold(acc, |a, v| f(a, high | u64::from(v)));
        }
        acc
    }
}

impl ExactSizeIterator for Forest64Range<'_> {
    #[inline]
    fn len(&self) -> usize {
        self.remaining
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Lazy set adaptors
// ─────────────────────────────────────────────────────────────────────────
//
// Each wraps its sources BY VALUE and allocates nothing, ever. Each holds
// its sources ALIGNED — positioned so `front` is a pure read of state the
// sources already hold — which is what makes the protocol pay: the alignment
// step IS the merge, and it runs in `pop_front` where the work belongs, not
// in a peek buffer consulted per query.
//
// The bound is `Range + Iterator` on both sources: `Range` for the merge
// primitives, `Iterator` for `size_hint`, which the adaptors combine into a
// bound on their own output so `Vec::extend` reserves something sane.

/// Which source the union/symmetric-difference head came from.
///
/// Cached rather than recomputed. `front` and `pop_front` are separately
/// callable by contract, so without the tag a union compares its two heads
/// TWICE per element — once to answer `front`, once to decide who advances.
/// One `u8` tag makes `front` a dispatch and `pop_front` a dispatch plus one
/// compare to re-decide.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Side {
    Left,
    Right,
    Both,
    Done,
}

/// Lazy intersection: the values in both sources.
///
/// Aligned invariant: either a source is exhausted, or both heads are equal.
/// No side tag — the head is always `a.front()`.
///
/// # The two alignment kernels, and why both stay
///
/// `LEAPFROG` selects how the aligner closes a gap between the two heads.
/// It is a const parameter, so each arm monomorphizes to its own kernel with
/// no runtime branch, and both live in ONE binary — which is what lets a
/// caller interleave them per sample against a shared A/A floor instead of
/// comparing two builds (handbook §4.5), and what keeps the displaced kernel
/// measurable forever (§7.10c).
///
/// - `true` ([`AndRange::new`], the default): close the gap with
///   [`Range::seek`]. A source that can jump — both wire cursors can — makes
///   the intersection cost the SMALLER side's cardinality plus the distance
///   travelled, which is the "drive-from-smaller-side galloping" the count
///   kernel `and_len` has had since G261.
/// - `false` ([`AndRange::lockstep`]): close the gap one `pop_front` at a
///   time. The CONTROL, and what this adaptor did unconditionally while
///   CAMPAIGN-10X §14 recorded it as "cardinality-blind": stepping costs
///   `|A| + |B| - |A ∩ B|` cursor advances whatever the skew is, and no
///   call-site cardinality test can change that, because the blindness is
///   in the ALIGNER and not in the argument order.
///
/// Both arms yield byte-identical sequences by construction: `seek(target)`
/// and "step while `front() < target`" have the same postcondition, and the
/// default `seek` IS that loop. The differential oracle in `tests/range.rs`
/// holds them to it, with a planted divergence that must be caught.
#[derive(Clone, Copy, Debug)]
pub struct AndRange<A, B, const LEAPFROG: bool = true> {
    a: A,
    b: B,
}

impl<T, A, B, const LEAPFROG: bool> AndRange<A, B, LEAPFROG>
where
    T: Copy + Ord,
    A: Range<Item = T> + Iterator<Item = T>,
    B: Range<Item = T> + Iterator<Item = T>,
{
    /// Construct and establish the aligned invariant. Shared by both arms;
    /// a const parameter has no inference source at a bare `AndRange::new`
    /// path, so each arm's public constructor lives on its own concrete
    /// impl below and pins `LEAPFROG` by being there.
    #[inline]
    fn aligned(a: A, b: B) -> Self {
        let mut range = AndRange { a, b };
        range.align();
        range
    }

    /// Borrow both aligned source cursors without advancing them.
    ///
    /// While this intersection is non-empty, both source heads equal
    /// [`Range::front`] and their remaining lengths preserve both source
    /// ordinals. Inspect them before [`Range::pop_front`], which advances and
    /// re-aligns both cursors to the next match.
    #[inline]
    pub fn sources(&self) -> (&A, &B) {
        (&self.a, &self.b)
    }

    /// The three-way compare, branchy on purpose (handbook §7.4: 0.68
    /// ns/iter against 1.66 for the branchless form on this shape).
    ///
    /// `LEAPFROG` is a compile-time constant, so the arm below is folded and
    /// neither kernel pays a test for the other's existence.
    #[inline]
    fn align(&mut self) {
        while !self.a.empty() && !self.b.empty() {
            let (x, y) = (self.a.front(), self.b.front());
            if x < y {
                if LEAPFROG {
                    self.a.seek(y);
                } else {
                    self.a.pop_front();
                }
            } else if y < x {
                if LEAPFROG {
                    self.b.seek(x);
                } else {
                    self.b.pop_front();
                }
            } else {
                return;
            }
        }
    }
}

impl<T, A, B> AndRange<A, B, true>
where
    T: Copy + Ord,
    A: Range<Item = T> + Iterator<Item = T>,
    B: Range<Item = T> + Iterator<Item = T>,
{
    /// The leapfrog arm: close a head gap with [`Range::seek`], so the
    /// intersection costs the smaller side wherever the sources can jump.
    ///
    /// There is no unnamed constructor. Which aligner runs is the whole
    /// content of this adaptor's measured A/B, and a `new` that silently
    /// meant one of them is how a call site ends up unable to say which
    /// kernel produced its number.
    #[inline]
    pub fn leapfrog(a: A, b: B) -> Self {
        Self::aligned(a, b)
    }
}

impl<T, A, B> AndRange<A, B, false>
where
    T: Copy + Ord,
    A: Range<Item = T> + Iterator<Item = T>,
    B: Range<Item = T> + Iterator<Item = T>,
{
    /// The stepping control arm, named explicitly so a measurement can hold
    /// it beside [`AndRange::new`] in one process.
    #[inline]
    pub fn lockstep(a: A, b: B) -> Self {
        Self::aligned(a, b)
    }
}

impl<T, A, B, const LEAPFROG: bool> Range for AndRange<A, B, LEAPFROG>
where
    T: Copy + Ord,
    A: Range<Item = T> + Iterator<Item = T>,
    B: Range<Item = T> + Iterator<Item = T>,
{
    type Item = T;

    #[inline(always)]
    fn empty(&self) -> bool {
        self.a.empty() || self.b.empty()
    }

    #[inline(always)]
    fn front(&self) -> T {
        debug_assert!(!Range::empty(self), "front on an empty range");
        self.a.front()
    }

    #[inline(always)]
    fn pop_front(&mut self) {
        debug_assert!(!Range::empty(self), "pop_front on an empty range");
        self.a.pop_front();
        self.b.pop_front();
        self.align();
    }

    /// Seeking an intersection is seeking BOTH sources and re-aligning: an
    /// element of `A ∩ B` at or after `target` is at or after `target` in
    /// each source, so neither seek can skip a member of the result.
    ///
    /// Overridden only on the leapfrog arm, because it is the composition
    /// that makes a THREE-term intersection — an `AndRange` over an
    /// `AndRange` — leapfrog through its inner node instead of stepping the
    /// pair back into the blindness the outer node just escaped. The control
    /// arm keeps the protocol's stepping default, so it stays exactly the
    /// kernel it is the control for.
    #[inline]
    fn seek(&mut self, target: T) {
        if !LEAPFROG {
            while !Range::empty(self) && self.a.front() < target {
                Range::pop_front(self);
            }
            return;
        }
        if Range::empty(self) || self.a.front() >= target {
            return;
        }
        self.a.seek(target);
        self.b.seek(target);
        self.align();
    }
}

impl<T, A, B, const LEAPFROG: bool> Iterator for AndRange<A, B, LEAPFROG>
where
    T: Copy + Ord,
    A: Range<Item = T> + Iterator<Item = T>,
    B: Range<Item = T> + Iterator<Item = T>,
{
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<T> {
        if Range::empty(self) {
            return None;
        }
        let v = self.a.front();
        Range::pop_front(self);
        Some(v)
    }

    /// No lower bound: two non-empty sets can be disjoint. The upper bound
    /// is the smaller side, which is what a `collect` should reserve.
    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, min_upper(self.a.size_hint().1, self.b.size_hint().1))
    }
}

/// Lazy union: the values in either source, each once.
#[derive(Clone, Copy, Debug)]
pub struct OrRange<A, B> {
    a: A,
    b: B,
    side: Side,
}

impl<T, A, B> OrRange<A, B>
where
    T: Copy + Ord,
    A: Range<Item = T> + Iterator<Item = T>,
    B: Range<Item = T> + Iterator<Item = T>,
{
    #[inline]
    pub fn new(a: A, b: B) -> Self {
        let side = Self::decide(&a, &b);
        OrRange { a, b, side }
    }

    /// Borrow both source cursors without advancing them.
    ///
    /// At least one source head equals [`Range::front`]; both do when the
    /// current value is shared. A source whose head is greater than the
    /// current value exposes an insertion position, not the yielded value's
    /// ordinal. Inspect the cursors before [`Range::pop_front`].
    #[inline]
    pub fn sources(&self) -> (&A, &B) {
        (&self.a, &self.b)
    }

    #[inline(always)]
    fn decide(a: &A, b: &B) -> Side {
        match (a.empty(), b.empty()) {
            (true, true) => Side::Done,
            (true, false) => Side::Right,
            (false, true) => Side::Left,
            (false, false) => {
                let (x, y) = (a.front(), b.front());
                if x < y {
                    Side::Left
                } else if y < x {
                    Side::Right
                } else {
                    Side::Both
                }
            }
        }
    }

    /// Advance whichever source(s) produced the current head, then re-decide.
    #[inline(always)]
    fn step(&mut self) {
        match self.side {
            Side::Left => self.a.pop_front(),
            Side::Right => self.b.pop_front(),
            Side::Both => {
                // A shared value is yielded once, so both advance.
                self.a.pop_front();
                self.b.pop_front();
            }
            Side::Done => return,
        }
        self.side = Self::decide(&self.a, &self.b);
    }
}

impl<T, A, B> Range for OrRange<A, B>
where
    T: Copy + Ord,
    A: Range<Item = T> + Iterator<Item = T>,
    B: Range<Item = T> + Iterator<Item = T>,
{
    type Item = T;

    #[inline(always)]
    fn empty(&self) -> bool {
        self.side == Side::Done
    }

    #[inline(always)]
    fn front(&self) -> T {
        debug_assert!(!Range::empty(self), "front on an empty range");
        match self.side {
            Side::Right => self.b.front(),
            _ => self.a.front(),
        }
    }

    #[inline(always)]
    fn pop_front(&mut self) {
        debug_assert!(!Range::empty(self), "pop_front on an empty range");
        self.step();
    }
}

impl<T, A, B> Iterator for OrRange<A, B>
where
    T: Copy + Ord,
    A: Range<Item = T> + Iterator<Item = T>,
    B: Range<Item = T> + Iterator<Item = T>,
{
    type Item = T;

    /// One tag dispatch per element: read the head from the side the tag
    /// names, advance that side, re-decide. No head comparison happens
    /// twice.
    #[inline]
    fn next(&mut self) -> Option<T> {
        let v = match self.side {
            Side::Done => return None,
            Side::Right => self.b.front(),
            _ => self.a.front(),
        };
        self.step();
        Some(v)
    }

    /// At least the larger side (a subset relation is the best case), at
    /// most both (disjoint).
    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let ((alo, ahi), (blo, bhi)) = (self.a.size_hint(), self.b.size_hint());
        (alo.max(blo), sum_upper(ahi, bhi))
    }
}

/// Lazy difference: the values in `a` and not in `b`.
///
/// Aligned invariant: either `a` is exhausted, or `a.front()` is absent from
/// the remainder of `b`. No side tag — the head is always `a.front()`.
#[derive(Clone, Copy, Debug)]
pub struct AndNotRange<A, B> {
    a: A,
    b: B,
}

impl<T, A, B> AndNotRange<A, B>
where
    T: Copy + Ord,
    A: Range<Item = T> + Iterator<Item = T>,
    B: Range<Item = T> + Iterator<Item = T>,
{
    #[inline]
    pub fn new(a: A, b: B) -> Self {
        let mut range = AndNotRange { a, b };
        range.align();
        range
    }

    /// Borrow both aligned source cursors without advancing them.
    ///
    /// The left source is positioned on [`Range::front`]. The right source
    /// is exhausted or positioned on its first value greater than the
    /// current output, so its consumed count is an insertion position rather
    /// than a matched ordinal. Inspect both before [`Range::pop_front`].
    #[inline]
    pub fn sources(&self) -> (&A, &B) {
        (&self.a, &self.b)
    }

    #[inline]
    fn align(&mut self) {
        while !self.a.empty() && !self.b.empty() {
            let (x, y) = (self.a.front(), self.b.front());
            if y < x {
                self.b.pop_front();
            } else if y == x {
                // Both sources ascend strictly, so this `b` value can never
                // match again: drop it with the `a` value it killed.
                self.a.pop_front();
                self.b.pop_front();
            } else {
                return;
            }
        }
    }
}

impl<T, A, B> Range for AndNotRange<A, B>
where
    T: Copy + Ord,
    A: Range<Item = T> + Iterator<Item = T>,
    B: Range<Item = T> + Iterator<Item = T>,
{
    type Item = T;

    #[inline(always)]
    fn empty(&self) -> bool {
        self.a.empty()
    }

    #[inline(always)]
    fn front(&self) -> T {
        debug_assert!(!Range::empty(self), "front on an empty range");
        self.a.front()
    }

    #[inline(always)]
    fn pop_front(&mut self) {
        debug_assert!(!Range::empty(self), "pop_front on an empty range");
        self.a.pop_front();
        self.align();
    }
}

impl<T, A, B> Iterator for AndNotRange<A, B>
where
    T: Copy + Ord,
    A: Range<Item = T> + Iterator<Item = T>,
    B: Range<Item = T> + Iterator<Item = T>,
{
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<T> {
        if self.a.empty() {
            return None;
        }
        let v = self.a.front();
        Range::pop_front(self);
        Some(v)
    }

    /// At most all of `a`; at least `a` minus everything `b` could hold.
    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let ((alo, ahi), (_, bhi)) = (self.a.size_hint(), self.b.size_hint());
        (bhi.map_or(0, |n| alo.saturating_sub(n)), ahi)
    }
}

/// Lazy symmetric difference: the values in exactly one source.
#[derive(Clone, Copy, Debug)]
pub struct XorRange<A, B> {
    a: A,
    b: B,
    side: Side,
}

impl<T, A, B> XorRange<A, B>
where
    T: Copy + Ord,
    A: Range<Item = T> + Iterator<Item = T>,
    B: Range<Item = T> + Iterator<Item = T>,
{
    #[inline]
    pub fn new(a: A, b: B) -> Self {
        let mut range = XorRange {
            a,
            b,
            side: Side::Done,
        };
        range.decide();
        range
    }

    /// Borrow both source cursors without advancing them.
    ///
    /// Exactly one source head equals [`Range::front`]; the other is
    /// exhausted or positioned on a greater value. Only the producing
    /// source's consumed count is the yielded value's ordinal. Inspect the
    /// cursors before [`Range::pop_front`].
    #[inline]
    pub fn sources(&self) -> (&A, &B) {
        (&self.a, &self.b)
    }

    /// Skip the values both sources hold, then name the side holding the
    /// smaller head. `Side::Both` never survives here — that is the
    /// difference from [`OrRange`], and it is why this one needs `&mut`.
    #[inline(always)]
    fn decide(&mut self) {
        self.side = loop {
            match (self.a.empty(), self.b.empty()) {
                (true, true) => break Side::Done,
                (true, false) => break Side::Right,
                (false, true) => break Side::Left,
                (false, false) => {
                    let (x, y) = (self.a.front(), self.b.front());
                    if x < y {
                        break Side::Left;
                    } else if y < x {
                        break Side::Right;
                    }
                    self.a.pop_front();
                    self.b.pop_front();
                }
            }
        };
    }

    #[inline(always)]
    fn step(&mut self) {
        match self.side {
            Side::Left => self.a.pop_front(),
            Side::Right => self.b.pop_front(),
            // Unreachable by the invariant; costs nothing to be total.
            Side::Both | Side::Done => return,
        }
        self.decide();
    }
}

impl<T, A, B> Range for XorRange<A, B>
where
    T: Copy + Ord,
    A: Range<Item = T> + Iterator<Item = T>,
    B: Range<Item = T> + Iterator<Item = T>,
{
    type Item = T;

    #[inline(always)]
    fn empty(&self) -> bool {
        self.side == Side::Done
    }

    #[inline(always)]
    fn front(&self) -> T {
        debug_assert!(!Range::empty(self), "front on an empty range");
        match self.side {
            Side::Right => self.b.front(),
            _ => self.a.front(),
        }
    }

    #[inline(always)]
    fn pop_front(&mut self) {
        debug_assert!(!Range::empty(self), "pop_front on an empty range");
        self.step();
    }
}

impl<T, A, B> Iterator for XorRange<A, B>
where
    T: Copy + Ord,
    A: Range<Item = T> + Iterator<Item = T>,
    B: Range<Item = T> + Iterator<Item = T>,
{
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<T> {
        let v = match self.side {
            Side::Done => return None,
            Side::Right => self.b.front(),
            _ => self.a.front(),
        };
        self.step();
        Some(v)
    }

    /// No lower bound: two equal sets XOR to nothing.
    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, sum_upper(self.a.size_hint().1, self.b.size_hint().1))
    }
}

#[inline]
fn min_upper(a: Option<usize>, b: Option<usize>) -> Option<usize> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.min(y)),
        (Some(x), None) | (None, Some(x)) => Some(x),
        (None, None) => None,
    }
}

#[inline]
fn sum_upper(a: Option<usize>, b: Option<usize>) -> Option<usize> {
    a.zip(b).map(|(x, y)| x.saturating_add(y))
}
