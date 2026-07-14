//! Minimal RoaringFormatSpec bitmap — replaces the `roaring` crate in the
//! shipped artifact (35.3K of the wasm size budget) with exactly the surface
//! `bitmap_ops` uses. Byte-format compatible with rawr (the Zig side's
//! roaring library): the serializer mirrors rawr `serialize.zig` layout
//! byte-for-byte and the container-choice rules mirror rawr's semantics —
//! array→bitset promotion when a 4096-full array receives an add
//! (rawr `bitmap.zig:394` addToContainer), NO bitset→array demotion on
//! remove (rawr `bitmap.zig:463`), run containers stay runs under
//! add/remove, and `optimize` applies rawr `optimize.zig:11` runOptimize:
//! array→run when `n_runs*4 < cardinality*2`, bitset→run when
//! `n_runs*4 < 8192`, runs never demote.
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

    /// rawr addToContainer: array promotes to bitset when full; run splices.
    /// Returns whether the value was newly added (rawr's full-array branch
    /// returns true even for duplicates — preserved, it is observable through
    /// cached cardinality only when the array was exactly full).
    fn add(&mut self, low: u16) -> bool {
        match self {
            Container::Array(v) => {
                if v.len() >= ARRAY_MAX_CARDINALITY {
                    let mut words = Box::new([0u64; 1024]);
                    for &x in v.iter() {
                        words[usize::from(x) / 64] |= 1u64 << (x % 64);
                    }
                    let mut card = v.len() as u32;
                    let w = &mut words[usize::from(low) / 64];
                    let bit = 1u64 << (low % 64);
                    if *w & bit == 0 {
                        *w |= bit;
                        card += 1;
                    }
                    *self = Container::Bitset(words, card);
                    // rawr returns true unconditionally on this path.
                    return true;
                }
                match v.binary_search(&low) {
                    Ok(_) => false,
                    Err(pos) => {
                        v.insert(pos, low);
                        true
                    }
                }
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
                let mut idx = runs.partition_point(|&(start, _)| start <= low);
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
                let _ = &mut idx;
                true
            }
        }
    }

    /// rawr removeFromContainer: value removal with no type demotion.
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

    /// rawr optimize.zig runOptimize for one container.
    fn run_optimize(&mut self) {
        let n_runs = match self {
            Container::Array(v) => {
                if v.is_empty() {
                    return;
                }
                let mut n = 1u32;
                for w in v.windows(2) {
                    if w[1] != w[0] + 1 {
                        n += 1;
                    }
                }
                if n * 4 < v.len() as u32 * 2 {
                    n
                } else {
                    return;
                }
            }
            Container::Bitset(words, _) => {
                // countRunsInBitset: number of 0→1 transitions.
                let mut n = 0u32;
                let mut prev = 0u64; // bit 63 of previous word
                for &w in words.iter() {
                    // starts = bits set in w whose predecessor bit is 0
                    let shifted = (w << 1) | prev;
                    n += (w & !shifted).count_ones();
                    prev = w >> 63;
                }
                if n * 4 < BITSET_SIZE_BYTES as u32 {
                    n
                } else {
                    return;
                }
            }
            Container::Run(_) => return,
        };
        let _ = n_runs;
        // Convert current contents to runs.
        let lows: Vec<u16> = self.iter_lows().collect();
        let mut runs: Vec<(u16, u16)> = Vec::new();
        let mut it = lows.into_iter();
        if let Some(first) = it.next() {
            let mut start = first;
            let mut prev = first;
            for x in it {
                if x == prev + 1 {
                    prev = x;
                } else {
                    runs.push((start, prev - start));
                    start = x;
                    prev = x;
                }
            }
            runs.push((start, prev - start));
        }
        *self = Container::Run(runs);
    }

    fn iter_lows(&self) -> Box<dyn Iterator<Item = u16> + '_> {
        match self {
            Container::Array(v) => Box::new(v.iter().copied()),
            Container::Bitset(words, _) => {
                Box::new(words.iter().enumerate().flat_map(|(i, &w)| {
                    let mut bits = w;
                    core::iter::from_fn(move || {
                        if bits == 0 {
                            return None;
                        }
                        let tz = bits.trailing_zeros();
                        bits &= bits - 1;
                        Some((i as u32 * 64 + tz) as u16)
                    })
                }))
            }
            Container::Run(runs) => Box::new(runs.iter().flat_map(|&(start, len)| {
                (u32::from(start)..=u32::from(start) + u32::from(len)).map(|x| x as u16)
            })),
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

    /// rawr runOptimize — call before serializing (mirrors bitmapStore).
    pub fn optimize(&mut self) {
        for c in &mut self.containers {
            c.run_optimize();
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
    // Portable RoaringFormatSpec (mirrors rawr serialize.zig)
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
                for chunk in raw.chunks_exact(2) {
                    v.push(u16::from_le_bytes([chunk[0], chunk[1]]));
                }
                if v.windows(2).any(|w| w[0] >= w[1]) {
                    return Err(InvalidFormat);
                }
                Container::Array(v)
            } else {
                let raw = r.bytes(BITSET_SIZE_BYTES)?;
                let mut words = Box::new([0u64; 1024]);
                for (k, chunk) in raw.chunks_exact(8).enumerate() {
                    words[k] = match chunk.first_chunk::<8>() {
                        Some(&arr) => u64::from_le_bytes(arr),
                        // chunks_exact(8) guarantees the width.
                        None => columine_types::die!("chunks_exact(8) yielded a short chunk"),
                    };
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
