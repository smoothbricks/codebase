//! Minimal RoaringFormatSpec bitmap for the wasm artifact. It implements the
//! exact surface used by `bitmap_ops` while keeping the shipped binary small.
//! The serializer follows the portable Roaring format (cookies 12346/12347).
//!
//! Container form between mutations is whatever the mutation history left
//! (an array promotes to a bitset when a 4097th value arrives, a bitset stays
//! a bitset while values drain, a run stays a run). `optimize` erases that
//! history: it re-ladders every container from its contents alone — runs when
//! `n_runs*4 < cardinality*2` (≤ 4096 values) or `n_runs*4 < 8192` (more),
//! else array/bitset by cardinality — so the bytes written after `optimize`
//! are a pure function of the set. Every store path calls `optimize` first,
//! which is what lets a snapshot round-trip and a `remove`-then-`insert`
//! reproduce the original image byte for byte.
//!
//! The `roaring` crate remains a DEV-dependency only: differential proptests
//! in `tests/bitmap.rs` use it as the read/write oracle.

use core::fmt;

const SERIAL_COOKIE_NO_RUNCONTAINER: u32 = 12346;
const SERIAL_COOKIE: u32 = 12347;
const NO_OFFSET_THRESHOLD: usize = 4;
const BITSET_SIZE_BYTES: usize = 8192;
const ARRAY_MAX_CARDINALITY: usize = 4096;

/// Deserialize failure — the only error the VM paths observe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidFormat;

impl fmt::Display for InvalidFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid roaring format")
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Container {
    /// Sorted unique low-16 values (≤ 4096 entries).
    Array(Vec<u16>),
    /// 65536-bit set + cached cardinality.
    Bitset(Box<[u64; 1024]>, u32),
    /// Sorted non-overlapping (start, length-1) pairs.
    Run(Vec<(u16, u16)>),
}

impl Container {
    fn cardinality(&self) -> u32 {
        match self {
            Container::Array(v) => v.len() as u32,
            Container::Bitset(_, card) => *card,
            Container::Run(runs) => runs.iter().map(|&(_, l)| u32::from(l) + 1).sum(),
        }
    }

    fn contains(&self, low: u16) -> bool {
        match self {
            Container::Array(v) => v.binary_search(&low).is_ok(),
            Container::Bitset(words, _) => words[usize::from(low) / 64] & (1u64 << (low % 64)) != 0,
            Container::Run(runs) => runs
                .binary_search_by(|&(start, len)| {
                    if low < start {
                        core::cmp::Ordering::Greater
                    } else if u32::from(low) > u32::from(start) + u32::from(len) {
                        core::cmp::Ordering::Less
                    } else {
                        core::cmp::Ordering::Equal
                    }
                })
                .is_ok(),
        }
    }

    /// Add one value; returns whether it was newly added. The answer must be
    /// exact for every form: delta reporting derives `changed` from it, and a
    /// duplicate that reported `true` would emit an undo record for a
    /// mutation that never happened. A full array therefore promotes only when
    /// the value is genuinely new, so the bitset form starts at 4097 values —
    /// the same boundary `from_words` and `optimize` use.
    fn add(&mut self, low: u16) -> bool {
        match self {
            Container::Array(v) => {
                let Err(pos) = v.binary_search(&low) else {
                    return false;
                };
                if v.len() >= ARRAY_MAX_CARDINALITY {
                    let mut words = Box::new([0u64; 1024]);
                    for &x in v.iter() {
                        words[usize::from(x) / 64] |= 1u64 << (x % 64);
                    }
                    words[usize::from(low) / 64] |= 1u64 << (low % 64);
                    *self = Container::Bitset(words, v.len() as u32 + 1);
                    return true;
                }
                v.insert(pos, low);
                true
            }
            Container::Bitset(words, card) => {
                let w = &mut words[usize::from(low) / 64];
                let bit = 1u64 << (low % 64);
                if *w & bit != 0 {
                    return false;
                }
                *w |= bit;
                *card += 1;
                true
            }
            Container::Run(runs) => {
                // Find insertion point among runs.
                let idx = runs.partition_point(|&(start, _)| start <= low);
                if idx > 0 {
                    let (start, len) = runs[idx - 1];
                    let end = u32::from(start) + u32::from(len);
                    if u32::from(low) <= end {
                        return false; // already inside a run
                    }
                    if u32::from(low) == end + 1 {
                        // extend previous run; possibly merge with next
                        runs[idx - 1].1 += 1;
                        if idx < runs.len() && u32::from(runs[idx].0) == u32::from(low) + 1 {
                            let (_, nlen) = runs.remove(idx);
                            runs[idx - 1].1 += nlen + 1;
                        }
                        return true;
                    }
                }
                if idx < runs.len() && u32::from(runs[idx].0) == u32::from(low) + 1 {
                    runs[idx].0 = low;
                    runs[idx].1 += 1;
                    return true;
                }
                runs.insert(idx, (low, 0));
                true
            }
        }
    }

    /// Remove one value; returns whether it was present. The form is kept —
    /// demoting a drained bitset here would re-promote on the next add, and
    /// `optimize` re-ladders every container once before it is serialized.
    fn remove(&mut self, low: u16) -> bool {
        match self {
            Container::Array(v) => match v.binary_search(&low) {
                Ok(pos) => {
                    v.remove(pos);
                    true
                }
                Err(_) => false,
            },
            Container::Bitset(words, card) => {
                let w = &mut words[usize::from(low) / 64];
                let bit = 1u64 << (low % 64);
                if *w & bit == 0 {
                    return false;
                }
                *w &= !bit;
                *card -= 1;
                true
            }
            Container::Run(runs) => {
                let idx = runs.partition_point(|&(start, _)| start <= low);
                if idx == 0 {
                    return false;
                }
                let (start, len) = runs[idx - 1];
                let end = u32::from(start) + u32::from(len);
                if u32::from(low) > end {
                    return false;
                }
                if start == low && len == 0 {
                    runs.remove(idx - 1);
                } else if start == low {
                    runs[idx - 1] = (low + 1, len - 1);
                } else if u32::from(low) == end {
                    runs[idx - 1].1 -= 1;
                } else {
                    // split
                    let left_len = low - start - 1;
                    let right_start = low + 1;
                    let right_len = (end - u32::from(low) - 1) as u16;
                    runs[idx - 1] = (start, left_len);
                    runs.insert(idx, (right_start, right_len));
                }
                true
            }
        }
    }

    /// Materialize as a bitset word array (algebra scratch domain).
    fn to_words(&self) -> Box<[u64; 1024]> {
        let mut words = Box::new([0u64; 1024]);
        match self {
            Container::Array(v) => {
                for &x in v {
                    words[usize::from(x) / 64] |= 1u64 << (x % 64);
                }
            }
            Container::Bitset(w, _) => words.copy_from_slice(&w[..]),
            Container::Run(runs) => {
                for &(start, len) in runs {
                    for x in u32::from(start)..=u32::from(start) + u32::from(len) {
                        words[(x / 64) as usize] |= 1u64 << (x % 64);
                    }
                }
            }
        }
        words
    }

    /// Build the canonical fresh container for a cardinality (rawr algebra
    /// results: array ≤4096, else bitset; runs only via runOptimize).
    fn from_words(words: Box<[u64; 1024]>) -> Option<Container> {
        let card: u32 = words.iter().map(|w| w.count_ones()).sum();
        if card == 0 {
            return None;
        }
        if card as usize <= ARRAY_MAX_CARDINALITY {
            let mut v = Vec::with_capacity(card as usize);
            for (i, &w) in words.iter().enumerate() {
                let mut bits = w;
                while bits != 0 {
                    let tz = bits.trailing_zeros();
                    v.push((i as u32 * 64 + tz) as u16);
                    bits &= bits - 1;
                }
            }
            Some(Container::Array(v))
        } else {
            Some(Container::Bitset(words, card))
        }
    }

    /// Number of maximal runs of consecutive values, read off each form
    /// without expanding it.
    fn run_count(&self) -> u32 {
        match self {
            Container::Array(v) => {
                if v.is_empty() {
                    return 0;
                }
                1 + v.windows(2).filter(|w| w[1] != w[0] + 1).count() as u32
            }
            Container::Bitset(words, _) => {
                // Number of 0→1 transitions across the 65536-bit plane.
                let mut n = 0u32;
                let mut carry = 0u64; // bit 63 of the previous word
                for &w in words.iter() {
                    n += (w & !((w << 1) | carry)).count_ones();
                    carry = w >> 63;
                }
                n
            }
            Container::Run(runs) => runs.len() as u32,
        }
    }

    /// Re-ladder to the canonical form for the current contents (module doc).
    /// Deciding from `(cardinality, run_count)` alone is what makes the
    /// serialized image history-independent.
    fn canonicalize(&mut self) {
        let card = self.cardinality();
        let n_runs = self.run_count();
        let small = card as usize <= ARRAY_MAX_CARDINALITY;
        let want_run = if small {
            n_runs * 4 < card * 2
        } else {
            n_runs * 4 < BITSET_SIZE_BYTES as u32
        };
        if want_run {
            if !matches!(self, Container::Run(_)) {
                *self = Container::Run(self.to_runs(n_runs as usize));
            }
        } else if small {
            if !matches!(self, Container::Array(_)) {
                let mut v = Vec::with_capacity(card as usize);
                v.extend(self.iter_lows());
                *self = Container::Array(v);
            }
        } else if !matches!(self, Container::Bitset(..)) {
            *self = Container::Bitset(self.to_words(), card);
        }
    }

    /// Maximal runs of the contents; `n_runs` sizes the allocation exactly.
    fn to_runs(&self, n_runs: usize) -> Vec<(u16, u16)> {
        let mut runs: Vec<(u16, u16)> = Vec::with_capacity(n_runs);
        match self {
            Container::Run(existing) => runs.extend_from_slice(existing),
            Container::Array(v) => {
                let mut it = v.iter().copied();
                if let Some(first) = it.next() {
                    let (mut start, mut prev) = (first, first);
                    for x in it {
                        if x != prev + 1 {
                            runs.push((start, prev - start));
                            start = x;
                        }
                        prev = x;
                    }
                    runs.push((start, prev - start));
                }
            }
            Container::Bitset(words, _) => {
                // Per word, `starts` marks 0→1 edges and `ends` 1→0 edges
                // (the bit before a 0 or the plane end). Edges alternate
                // start, end, start, … across the whole plane, so a pending
                // start always pairs with the next end.
                let mut pending: Option<u16> = None;
                for (i, &w) in words.iter().enumerate() {
                    if w == 0 {
                        continue;
                    }
                    let carry_in = if i == 0 { 0 } else { words[i - 1] >> 63 };
                    let carry_out = if i + 1 < words.len() {
                        (words[i + 1] & 1) << 63
                    } else {
                        0
                    };
                    let mut starts = w & !((w << 1) | carry_in);
                    let mut ends = w & !((w >> 1) | carry_out);
                    let base = i as u32 * 64;
                    while starts != 0 || ends != 0 {
                        match pending {
                            None => {
                                let tz = starts.trailing_zeros();
                                starts &= starts - 1;
                                pending = Some((base + tz) as u16);
                            }
                            Some(start) => {
                                let tz = ends.trailing_zeros();
                                ends &= ends - 1;
                                let end = (base + tz) as u16;
                                runs.push((start, end - start));
                                pending = None;
                            }
                        }
                    }
                }
            }
        }
        runs
    }

    fn iter_lows(&self) -> LowIter<'_> {
        match self {
            Container::Array(v) => LowIter::Array(v.iter()),
            Container::Bitset(words, _) => LowIter::Bitset {
                words,
                word: 0,
                bits: words[0],
            },
            Container::Run(runs) => LowIter::Run {
                runs: runs.iter(),
                cur: 1,
                end: 0,
            },
        }
    }
}

/// Ascending low-16 cursor over one container. An enum rather than a boxed
/// trait object so iterating a bitmap allocates nothing per container.
enum LowIter<'a> {
    Array(core::slice::Iter<'a, u16>),
    Bitset {
        words: &'a [u64; 1024],
        word: usize,
        bits: u64,
    },
    /// `cur > end` means the current run is exhausted and the next run is
    /// fetched on demand.
    Run {
        runs: core::slice::Iter<'a, (u16, u16)>,
        cur: u32,
        end: u32,
    },
}

impl Iterator for LowIter<'_> {
    type Item = u16;

    fn next(&mut self) -> Option<u16> {
        match self {
            LowIter::Array(it) => it.next().copied(),
            LowIter::Bitset { words, word, bits } => {
                while *bits == 0 {
                    *word += 1;
                    if *word >= words.len() {
                        return None;
                    }
                    *bits = words[*word];
                }
                let value = (*word as u32 * 64 + bits.trailing_zeros()) as u16;
                *bits &= *bits - 1;
                Some(value)
            }
            LowIter::Run { runs, cur, end } => {
                if *cur > *end {
                    let &(start, len) = runs.next()?;
                    *cur = u32::from(start);
                    *end = u32::from(start) + u32::from(len);
                }
                let value = *cur as u16;
                *cur += 1;
                Some(value)
            }
        }
    }
}

/// Minimal roaring bitmap over u32 keys, portable-format compatible.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MiniRoaring {
    keys: Vec<u16>,
    containers: Vec<Container>,
}

impl MiniRoaring {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> u64 {
        self.containers
            .iter()
            .map(|c| u64::from(c.cardinality()))
            .sum()
    }

    pub fn is_empty(&self) -> bool {
        self.containers.is_empty()
    }

    pub fn contains(&self, value: u32) -> bool {
        let key = (value >> 16) as u16;
        match self.keys.binary_search(&key) {
            Ok(idx) => self.containers[idx].contains(value as u16),
            Err(_) => false,
        }
    }

    pub fn insert(&mut self, value: u32) -> bool {
        let key = (value >> 16) as u16;
        let low = value as u16;
        match self.keys.binary_search(&key) {
            Ok(idx) => self.containers[idx].add(low),
            Err(idx) => {
                self.keys.insert(idx, key);
                self.containers.insert(idx, Container::Array(vec![low]));
                true
            }
        }
    }

    pub fn remove(&mut self, value: u32) -> bool {
        let key = (value >> 16) as u16;
        match self.keys.binary_search(&key) {
            Ok(idx) => {
                let removed = self.containers[idx].remove(value as u16);
                if removed && self.containers[idx].cardinality() == 0 {
                    self.keys.remove(idx);
                    self.containers.remove(idx);
                }
                removed
            }
            Err(_) => false,
        }
    }

    /// Re-ladder every container to its canonical form (module doc). Every
    /// store path calls this before serializing; the bytes that follow are
    /// then a pure function of the set.
    pub fn optimize(&mut self) {
        for c in &mut self.containers {
            c.canonicalize();
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        self.keys
            .iter()
            .zip(&self.containers)
            .flat_map(|(&key, c)| {
                c.iter_lows()
                    .map(move |low| (u32::from(key) << 16) | u32::from(low))
            })
    }

    pub fn is_disjoint(&self, other: &Self) -> bool {
        self.intersection_len(other) == 0
    }

    pub fn intersection_len(&self, other: &Self) -> u64 {
        let mut total = 0u64;
        for (idx, &key) in self.keys.iter().enumerate() {
            if let Ok(oidx) = other.keys.binary_search(&key) {
                let a = self.containers[idx].to_words();
                let b = other.containers[oidx].to_words();
                total += a
                    .iter()
                    .zip(b.iter())
                    .map(|(x, y)| (x & y).count_ones() as u64)
                    .sum::<u64>();
            }
        }
        total
    }

    fn algebra(
        &self,
        other: &Self,
        op: fn(u64, u64) -> u64,
        keep_left_only: bool,
        keep_right_only: bool,
    ) -> Self {
        let mut keys = Vec::new();
        let mut containers = Vec::new();
        let (mut i, mut j) = (0usize, 0usize);
        while i < self.keys.len() || j < other.keys.len() {
            let take_left =
                j >= other.keys.len() || (i < self.keys.len() && self.keys[i] < other.keys[j]);
            let take_right =
                i >= self.keys.len() || (j < other.keys.len() && other.keys[j] < self.keys[i]);
            if take_left {
                if keep_left_only {
                    keys.push(self.keys[i]);
                    containers.push(self.containers[i].clone());
                }
                i += 1;
            } else if take_right {
                if keep_right_only {
                    keys.push(other.keys[j]);
                    containers.push(other.containers[j].clone());
                }
                j += 1;
            } else {
                let a = self.containers[i].to_words();
                let b = other.containers[j].to_words();
                let mut out = Box::new([0u64; 1024]);
                for k in 0..1024 {
                    out[k] = op(a[k], b[k]);
                }
                if let Some(c) = Container::from_words(out) {
                    keys.push(self.keys[i]);
                    containers.push(c);
                }
                i += 1;
                j += 1;
            }
        }
        MiniRoaring { keys, containers }
    }

    // ---------------------------------------------------------------------
    // Portable RoaringFormatSpec serialization
    // ---------------------------------------------------------------------

    pub fn serialized_size(&self) -> usize {
        if self.containers.is_empty() {
            return 8;
        }
        let has_runs = self
            .containers
            .iter()
            .any(|c| matches!(c, Container::Run(_)));
        let n = self.containers.len();
        let mut size = if has_runs { 4 + n.div_ceil(8) } else { 8 };
        size += n * 4; // descriptive header
        if !has_runs || n >= NO_OFFSET_THRESHOLD {
            size += n * 4; // offset header
        }
        for c in &self.containers {
            size += match c {
                Container::Array(v) => v.len() * 2,
                Container::Bitset(..) => BITSET_SIZE_BYTES,
                Container::Run(runs) => 2 + runs.len() * 4,
            };
        }
        size
    }

    pub fn serialize_into(&self, out: &mut Vec<u8>) -> Result<(), InvalidFormat> {
        if self.containers.is_empty() {
            out.extend_from_slice(&SERIAL_COOKIE_NO_RUNCONTAINER.to_le_bytes());
            out.extend_from_slice(&0u32.to_le_bytes());
            return Ok(());
        }
        let n = self.containers.len();
        let has_runs = self
            .containers
            .iter()
            .any(|c| matches!(c, Container::Run(_)));
        let base = out.len();

        if has_runs {
            let cookie = SERIAL_COOKIE | (((n - 1) as u32) << 16);
            out.extend_from_slice(&cookie.to_le_bytes());
            let mut bitset = vec![0u8; n.div_ceil(8)];
            for (i, c) in self.containers.iter().enumerate() {
                if matches!(c, Container::Run(_)) {
                    bitset[i / 8] |= 1 << (i % 8);
                }
            }
            out.extend_from_slice(&bitset);
        } else {
            out.extend_from_slice(&SERIAL_COOKIE_NO_RUNCONTAINER.to_le_bytes());
            out.extend_from_slice(&(n as u32).to_le_bytes());
        }

        for (key, c) in self.keys.iter().zip(&self.containers) {
            out.extend_from_slice(&key.to_le_bytes());
            out.extend_from_slice(&((c.cardinality() - 1) as u16).to_le_bytes());
        }

        if !has_runs || n >= NO_OFFSET_THRESHOLD {
            // Absolute offsets from buffer start.
            let mut offset = (out.len() - base + n * 4) as u32;
            for c in &self.containers {
                out.extend_from_slice(&offset.to_le_bytes());
                offset += match c {
                    Container::Array(v) => (v.len() * 2) as u32,
                    Container::Bitset(..) => BITSET_SIZE_BYTES as u32,
                    Container::Run(runs) => (2 + runs.len() * 4) as u32,
                };
            }
        }

        for c in &self.containers {
            match c {
                Container::Array(v) => {
                    for &x in v {
                        out.extend_from_slice(&x.to_le_bytes());
                    }
                }
                Container::Bitset(words, _) => {
                    for &w in words.iter() {
                        out.extend_from_slice(&w.to_le_bytes());
                    }
                }
                Container::Run(runs) => {
                    out.extend_from_slice(&(runs.len() as u16).to_le_bytes());
                    for &(start, len) in runs {
                        out.extend_from_slice(&start.to_le_bytes());
                        out.extend_from_slice(&len.to_le_bytes());
                    }
                }
            }
        }
        Ok(())
    }

    pub fn deserialize_from(data: &[u8]) -> Result<Self, InvalidFormat> {
        let mut r = Reader { data, pos: 0 };
        let cookie = r.u32()?;
        let (n, run_flags): (usize, Option<Vec<u8>>) = if cookie & 0xFFFF == SERIAL_COOKIE {
            let n = ((cookie >> 16) & 0xFFFF) as usize + 1;
            let flags = r.bytes(n.div_ceil(8))?.to_vec();
            (n, Some(flags))
        } else if cookie == SERIAL_COOKIE_NO_RUNCONTAINER {
            let n = r.u32()? as usize;
            (n, None)
        } else {
            return Err(InvalidFormat);
        };

        if n == 0 {
            return Ok(Self::new());
        }
        if n > 65536 {
            return Err(InvalidFormat);
        }

        let mut keys = Vec::with_capacity(n);
        let mut cards = Vec::with_capacity(n);
        for _ in 0..n {
            keys.push(r.u16()?);
            cards.push(u32::from(r.u16()?) + 1);
        }
        // Keys must be strictly increasing.
        if keys.windows(2).any(|w| w[0] >= w[1]) {
            return Err(InvalidFormat);
        }

        let has_offsets = run_flags.is_none() || n >= NO_OFFSET_THRESHOLD;
        if has_offsets {
            r.bytes(n * 4)?; // sequential read: offsets are redundant
        }

        let is_run = |i: usize| {
            run_flags
                .as_ref()
                .is_some_and(|f| f[i / 8] & (1 << (i % 8)) != 0)
        };

        let mut containers = Vec::with_capacity(n);
        for (i, &card) in cards.iter().enumerate() {
            let c = if is_run(i) {
                let n_runs = usize::from(r.u16()?);
                let mut runs = Vec::with_capacity(n_runs);
                let mut total = 0u32;
                let mut prev_end: i64 = -2;
                for _ in 0..n_runs {
                    let start = r.u16()?;
                    let len = r.u16()?;
                    if i64::from(start) <= prev_end {
                        return Err(InvalidFormat);
                    }
                    prev_end = i64::from(start) + i64::from(len);
                    if prev_end > 0xFFFF {
                        return Err(InvalidFormat);
                    }
                    total += u32::from(len) + 1;
                    runs.push((start, len));
                }
                if total != card {
                    return Err(InvalidFormat);
                }
                Container::Run(runs)
            } else if card as usize <= ARRAY_MAX_CARDINALITY {
                let raw = r.bytes(card as usize * 2)?;
                let mut v = Vec::with_capacity(card as usize);
                for chunk in raw.as_chunks::<2>().0 {
                    v.push(u16::from_le_bytes(*chunk));
                }
                if v.windows(2).any(|w| w[0] >= w[1]) {
                    return Err(InvalidFormat);
                }
                Container::Array(v)
            } else {
                let raw = r.bytes(BITSET_SIZE_BYTES)?;
                let mut words = Box::new([0u64; 1024]);
                for (k, chunk) in raw.as_chunks::<8>().0.iter().enumerate() {
                    words[k] = u64::from_le_bytes(*chunk);
                }
                let actual: u32 = words.iter().map(|w| w.count_ones()).sum();
                if actual != card {
                    return Err(InvalidFormat);
                }
                Container::Bitset(words, card)
            };
            containers.push(c);
        }

        Ok(MiniRoaring { keys, containers })
    }
}
/// Validate every container of a portable image and sum the header
/// cardinalities. This is the full-image check the in-place readers skip.
fn scan_serialized(data: &[u8]) -> Result<u32, InvalidFormat> {
    let mut reader = Reader { data, pos: 0 };
    let cookie = reader.u32()?;
    let (container_count, run_flags) = if cookie & 0xFFFF == SERIAL_COOKIE {
        let count = ((cookie >> 16) & 0xFFFF) as usize + 1;
        (count, Some(reader.bytes(count.div_ceil(8))?))
    } else if cookie == SERIAL_COOKIE_NO_RUNCONTAINER {
        (reader.u32()? as usize, None)
    } else {
        return Err(InvalidFormat);
    };
    if container_count > 65536 {
        return Err(InvalidFormat);
    }

    let headers_start = reader.pos;
    let mut previous_key = None;
    let mut cardinality = 0u64;
    for _ in 0..container_count {
        let key = reader.u16()?;
        if previous_key.is_some_and(|previous| previous >= key) {
            return Err(InvalidFormat);
        }
        previous_key = Some(key);
        cardinality += u64::from(reader.u16()?) + 1;
    }
    if run_flags.is_none() || container_count >= NO_OFFSET_THRESHOLD {
        reader.bytes(container_count.checked_mul(4).ok_or(InvalidFormat)?)?;
    }

    for index in 0..container_count {
        let header = headers_start + index * 4;
        let card = u32::from(u16::from_le_bytes([data[header + 2], data[header + 3]])) + 1;
        let is_run = run_flags.is_some_and(|flags| flags[index / 8] & (1 << (index % 8)) != 0);

        if is_run {
            let run_count = usize::from(reader.u16()?);
            let mut total = 0u32;
            let mut previous_end = None;
            for _ in 0..run_count {
                let start = reader.u16()?;
                let length = reader.u16()?;
                let end = u32::from(start) + u32::from(length);
                if end > u32::from(u16::MAX)
                    || previous_end.is_some_and(|previous| u32::from(start) <= previous)
                {
                    return Err(InvalidFormat);
                }
                previous_end = Some(end);
                total += u32::from(length) + 1;
            }
            if total != card {
                return Err(InvalidFormat);
            }
        } else if card as usize <= ARRAY_MAX_CARDINALITY {
            let mut previous = None;
            for _ in 0..card {
                let low = reader.u16()?;
                if previous.is_some_and(|value| value >= low) {
                    return Err(InvalidFormat);
                }
                previous = Some(low);
            }
        } else {
            let raw = reader.bytes(BITSET_SIZE_BYTES)?;
            let actual: u32 = raw
                .as_chunks::<8>()
                .0
                .iter()
                .map(|chunk| u64::from_le_bytes(*chunk).count_ones())
                .sum();
            if actual != card {
                return Err(InvalidFormat);
            }
        }
    }
    Ok(cardinality.min(u64::from(u32::MAX)) as u32)
}

impl MiniRoaring {
    /// Validate portable bytes and read cardinality without materializing
    /// containers.
    pub fn len_bytes(data: &[u8]) -> Result<u32, InvalidFormat> {
        scan_serialized(data)
    }

    /// Probe one value in a portable image in place: the container is found
    /// by binary search over the key headers and addressed through the offset
    /// table, so the cost is the container's own lookup, not the image's size.
    /// Every read is bounds-checked; a truncated image is `InvalidFormat`.
    pub fn contains_bytes(data: &[u8], value: u32) -> Result<bool, InvalidFormat> {
        let image = Image::parse(data)?;
        let Some(index) = image.find_key((value >> 16) as u16) else {
            return Ok(false);
        };
        let low = value as u16;
        Ok(match image.container(index)? {
            ContainerView::Array(raw) => raw
                .as_chunks::<2>()
                .0
                .binary_search_by_key(&low, |chunk| u16::from_le_bytes(*chunk))
                .is_ok(),
            ContainerView::Bitset(raw) => {
                let word = usize::from(low) / 64 * 8;
                let word = u64::from_le_bytes(
                    raw[word..word + 8]
                        .try_into()
                        .expect("bitset words are 8 bytes wide"),
                );
                word & (1u64 << (low % 64)) != 0
            }
            ContainerView::Run(raw) => raw
                .as_chunks::<4>()
                .0
                .binary_search_by(|chunk| {
                    let start = u16::from_le_bytes([chunk[0], chunk[1]]);
                    let len = u16::from_le_bytes([chunk[2], chunk[3]]);
                    if low < start {
                        core::cmp::Ordering::Greater
                    } else if u32::from(low) > u32::from(start) + u32::from(len) {
                        core::cmp::Ordering::Less
                    } else {
                        core::cmp::Ordering::Equal
                    }
                })
                .is_ok(),
        })
    }

    /// The `rank`-th smallest value of a portable image, read in place: the
    /// container is located by summing header cardinalities and the value is
    /// picked inside it by index, run arithmetic, or word popcount. `None` when
    /// `rank` is at or past the cardinality.
    pub fn select_bytes(data: &[u8], rank: u32) -> Result<Option<u32>, InvalidFormat> {
        let image = Image::parse(data)?;
        let mut remaining = rank;
        for index in 0..image.count {
            let cardinality = image.cardinality(index);
            if remaining >= cardinality {
                remaining -= cardinality;
                continue;
            }
            let high = u32::from(image.key(index)) << 16;
            let low = match image.container(index)? {
                ContainerView::Array(raw) => raw
                    .as_chunks::<2>()
                    .0
                    .get(remaining as usize)
                    .map(|chunk| u16::from_le_bytes(*chunk)),
                ContainerView::Bitset(raw) => {
                    let mut found = None;
                    for (word_index, chunk) in raw.as_chunks::<8>().0.iter().enumerate() {
                        let mut word = u64::from_le_bytes(*chunk);
                        let ones = word.count_ones();
                        if remaining >= ones {
                            remaining -= ones;
                            continue;
                        }
                        for _ in 0..remaining {
                            word &= word - 1;
                        }
                        found = Some((word_index * 64) as u16 + word.trailing_zeros() as u16);
                        break;
                    }
                    found
                }
                ContainerView::Run(raw) => {
                    let mut found = None;
                    for chunk in raw.as_chunks::<4>().0 {
                        let start = u16::from_le_bytes([chunk[0], chunk[1]]);
                        let len = u32::from(u16::from_le_bytes([chunk[2], chunk[3]])) + 1;
                        if remaining >= len {
                            remaining -= len;
                            continue;
                        }
                        found = Some(start + remaining as u16);
                        break;
                    }
                    found
                }
            };
            // A header cardinality the container does not back is a malformed
            // image, not a missing rank.
            return low
                .map(|low| Some(high | u32::from(low)))
                .ok_or(InvalidFormat);
        }
        Ok(None)
    }
}

/// A portable image addressed in place: the header table located once, then
/// any container reached by index without materializing the others. The
/// offset table is the index when the format carries one; the run-cookie
/// layout omits it below [`NO_OFFSET_THRESHOLD`] containers, and those few are
/// walked.
struct Image<'a> {
    data: &'a [u8],
    count: usize,
    run_flags: Option<&'a [u8]>,
    headers: usize,
    offsets: Option<usize>,
    first_container: usize,
}

enum ContainerView<'a> {
    /// Sorted little-endian `u16` values.
    Array(&'a [u8]),
    /// 1024 little-endian `u64` words.
    Bitset(&'a [u8]),
    /// Little-endian `(start, length-1)` pairs, the count already consumed.
    Run(&'a [u8]),
}

impl<'a> Image<'a> {
    fn parse(data: &'a [u8]) -> Result<Self, InvalidFormat> {
        let mut reader = Reader { data, pos: 0 };
        let cookie = reader.u32()?;
        let (count, run_flags) = if cookie & 0xFFFF == SERIAL_COOKIE {
            let count = ((cookie >> 16) & 0xFFFF) as usize + 1;
            (count, Some(reader.bytes(count.div_ceil(8))?))
        } else if cookie == SERIAL_COOKIE_NO_RUNCONTAINER {
            (reader.u32()? as usize, None)
        } else {
            return Err(InvalidFormat);
        };
        if count > 65536 {
            return Err(InvalidFormat);
        }
        let headers = reader.pos;
        reader.bytes(count * 4)?;
        let offsets = if run_flags.is_none() || count >= NO_OFFSET_THRESHOLD {
            let offsets = reader.pos;
            reader.bytes(count * 4)?;
            Some(offsets)
        } else {
            None
        };
        Ok(Image {
            data,
            count,
            run_flags,
            headers,
            offsets,
            first_container: reader.pos,
        })
    }

    fn header_u16(&self, index: usize, at: usize) -> u16 {
        let pos = self.headers + index * 4 + at;
        u16::from_le_bytes([self.data[pos], self.data[pos + 1]])
    }

    fn key(&self, index: usize) -> u16 {
        self.header_u16(index, 0)
    }

    fn cardinality(&self, index: usize) -> u32 {
        u32::from(self.header_u16(index, 2)) + 1
    }

    fn is_run(&self, index: usize) -> bool {
        self.run_flags
            .is_some_and(|flags| flags[index / 8] & (1 << (index % 8)) != 0)
    }

    /// Index of the container keyed `key`; the header keys are sorted.
    fn find_key(&self, key: u16) -> Option<usize> {
        let (mut lo, mut hi) = (0usize, self.count);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match self.key(mid).cmp(&key) {
                core::cmp::Ordering::Less => lo = mid + 1,
                core::cmp::Ordering::Greater => hi = mid,
                core::cmp::Ordering::Equal => return Some(mid),
            }
        }
        None
    }

    fn container(&self, index: usize) -> Result<ContainerView<'a>, InvalidFormat> {
        let start = match self.offsets {
            Some(offsets) => {
                let pos = offsets + index * 4;
                u32::from_le_bytes([
                    self.data[pos],
                    self.data[pos + 1],
                    self.data[pos + 2],
                    self.data[pos + 3],
                ]) as usize
            }
            None => {
                let mut pos = self.first_container;
                for earlier in 0..index {
                    pos = pos
                        .checked_add(self.container_size(earlier, pos)?)
                        .ok_or(InvalidFormat)?;
                }
                pos
            }
        };
        let mut reader = Reader {
            data: self.data,
            pos: start,
        };
        let cardinality = self.cardinality(index) as usize;
        Ok(if self.is_run(index) {
            let runs = usize::from(reader.u16()?);
            ContainerView::Run(reader.bytes(runs * 4)?)
        } else if cardinality <= ARRAY_MAX_CARDINALITY {
            ContainerView::Array(reader.bytes(cardinality * 2)?)
        } else {
            ContainerView::Bitset(reader.bytes(BITSET_SIZE_BYTES)?)
        })
    }

    /// Byte size of the container starting at `pos`, for the walk an
    /// offset-less image needs.
    fn container_size(&self, index: usize, pos: usize) -> Result<usize, InvalidFormat> {
        let cardinality = self.cardinality(index) as usize;
        Ok(if self.is_run(index) {
            let mut reader = Reader {
                data: self.data,
                pos,
            };
            2 + usize::from(reader.u16()?) * 4
        } else if cardinality <= ARRAY_MAX_CARDINALITY {
            cardinality * 2
        } else {
            BITSET_SIZE_BYTES
        })
    }
}

struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn bytes(&mut self, n: usize) -> Result<&'a [u8], InvalidFormat> {
        let end = self.pos.checked_add(n).ok_or(InvalidFormat)?;
        if end > self.data.len() {
            return Err(InvalidFormat);
        }
        let s = &self.data[self.pos..end];
        self.pos = end;
        Ok(s)
    }
    fn u16(&mut self) -> Result<u16, InvalidFormat> {
        let b = self.bytes(2)?;
        Ok(u16::from_le_bytes([b[0], b[1]]))
    }
    fn u32(&mut self) -> Result<u32, InvalidFormat> {
        let b = self.bytes(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }
}

impl core::ops::BitAnd for MiniRoaring {
    type Output = MiniRoaring;
    fn bitand(self, rhs: MiniRoaring) -> MiniRoaring {
        self.algebra(&rhs, |a, b| a & b, false, false)
    }
}

impl core::ops::BitOr for MiniRoaring {
    type Output = MiniRoaring;
    fn bitor(self, rhs: MiniRoaring) -> MiniRoaring {
        self.algebra(&rhs, |a, b| a | b, true, true)
    }
}

impl core::ops::Sub for MiniRoaring {
    type Output = MiniRoaring;
    fn sub(self, rhs: MiniRoaring) -> MiniRoaring {
        self.algebra(&rhs, |a, b| a & !b, true, false)
    }
}

impl core::ops::BitXor for MiniRoaring {
    type Output = MiniRoaring;
    fn bitxor(self, rhs: MiniRoaring) -> MiniRoaring {
        self.algebra(&rhs, |a, b| a ^ b, true, true)
    }
}

impl core::ops::BitAndAssign<&MiniRoaring> for MiniRoaring {
    fn bitand_assign(&mut self, rhs: &MiniRoaring) {
        *self = self.algebra(rhs, |a, b| a & b, false, false);
    }
}

impl core::ops::BitOrAssign<&MiniRoaring> for MiniRoaring {
    fn bitor_assign(&mut self, rhs: &MiniRoaring) {
        *self = self.algebra(rhs, |a, b| a | b, true, true);
    }
}

impl core::ops::SubAssign<&MiniRoaring> for MiniRoaring {
    fn sub_assign(&mut self, rhs: &MiniRoaring) {
        *self = self.algebra(rhs, |a, b| a & !b, true, false);
    }
}

impl core::ops::BitXorAssign<&MiniRoaring> for MiniRoaring {
    fn bitxor_assign(&mut self, rhs: &MiniRoaring) {
        *self = self.algebra(rhs, |a, b| a ^ b, true, true);
    }
}
