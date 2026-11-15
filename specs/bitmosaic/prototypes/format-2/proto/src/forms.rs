//! The candidate forms of bitmosaic format 2, each read where its bytes lie.
//!
//! Bitmap  `[0x80 | n][n bytes]`                     bit j of byte i is the member 8i + j
//! Single  `[0x20 | wa-1][first: wa bytes]`
//! List    `[dd kkk aa][first: wa bytes][k-1 deltas from first: wd bytes each]`, k = kkk + 1 in 2..=8
//! Blocks  `[0x02][n-1 varint][first varint]` then, for n <= 128, `[span varint][block]`; otherwise
//!         `[last: u32] x B`, `[end: u32] x B`, the blocks. A block of c members over 0..=span is nothing
//!         (one run), a bitmap of span + 1 bits, or Elias-Fano planes (high, then low), whichever the closed
//!         formulas make shortest.
//! Spans   `[0x60][r][first varint][2r-1 boundaries: u16, relative to first]`  (measured only)
//! RunEf   `[0x61][r][first varint][span varint][Elias-Fano block over the 2r boundaries]`  (measured only)
//!
//! No read leaves the image: a word load near the end is clipped to the bytes that remain.

pub const BLOCK: usize = 128;

#[inline(always)]
pub fn word_at(b: &[u8], at: usize) -> u64 {
    if at + 8 <= b.len() {
        u64::from_le_bytes(b[at..at + 8].try_into().unwrap())
    } else {
        let mut w = [0u8; 8];
        if at < b.len() {
            let rest = &b[at..];
            w[..rest.len()].copy_from_slice(rest);
        }
        u64::from_le_bytes(w)
    }
}

#[inline(always)]
fn u32_at(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(b[at..at + 4].try_into().unwrap())
}

/// The bytes a value needs, at least one.
#[inline(always)]
pub fn nbytes(x: u32) -> usize {
    (((32 - x.leading_zeros()) as usize) + 7) / 8 + usize::from(x == 0)
}

#[inline(always)]
fn field(b: &[u8], at: usize, width: usize) -> u32 {
    (word_at(b, at) & (u64::MAX >> (64 - 8 * width))) as u32
}

pub fn put_varint(out: &mut Vec<u8>, mut x: u32) {
    while x >= 128 {
        out.push(x as u8 | 0x80);
        x >>= 7;
    }
    out.push(x as u8);
}

#[inline(always)]
fn varint(b: &[u8], at: &mut usize) -> u32 {
    let mut x = 0u32;
    let mut shift = 0;
    loop {
        let byte = b[*at];
        *at += 1;
        x |= u32::from(byte & 0x7f) << shift;
        if byte < 0x80 {
            return x;
        }
        shift += 7;
    }
}

pub fn varint_len(x: u32) -> usize {
    ((32 - (x | 1).leading_zeros()) as usize + 6) / 7
}

/// Position of the `k`-th set bit of `word` (`k` below its popcount): the byte by prefix counts (one multiply),
/// the bit by clearing `k'` low set bits of that byte. No table, no bit-deposit instruction.
#[inline(always)]
pub fn select_in_word(word: u64, k: u32) -> u32 {
    const ONES: u64 = 0x0101_0101_0101_0101;
    const HIGH: u64 = 0x8080_8080_8080_8080;
    let mut c = word - ((word >> 1) & 0x5555_5555_5555_5555);
    c = (c & 0x3333_3333_3333_3333) + ((c >> 2) & 0x3333_3333_3333_3333);
    c = (c + (c >> 4)) & 0x0f0f_0f0f_0f0f_0f0f;
    let through = c.wrapping_mul(ONES);
    let passed = (((u64::from(k) * ONES) | HIGH) - through) & HIGH;
    let byte = passed.count_ones().min(7);
    let before = ((through << 8) >> (8 * byte)) as u32 & 0xff;
    let mut bits = (word >> (8 * byte)) as u32 & 0xff;
    for _ in 0..k - before {
        bits &= bits - 1;
    }
    8 * byte + bits.trailing_zeros()
}

/// The low `n` bits set, `n` in 0..=64.
#[inline(always)]
fn mask_below(n: usize) -> u64 {
    if n >= 64 { u64::MAX } else { (1u64 << n) - 1 }
}

// ── Bitmap ───────────────────────────────────────────────────────────────────────────────────────
pub fn bitmap_len(s: &[u32]) -> Option<usize> {
    let n = (*s.last()? >> 3) as usize + 1;
    (n <= 127).then_some(1 + n)
}
pub fn bitmap_write(s: &[u32], out: &mut Vec<u8>) {
    let n = s.last().map_or(0, |&b| (b >> 3) as usize + 1);
    let at = out.len();
    out.push(0x80 | n as u8);
    out.resize(at + 1 + n, 0);
    for &v in s {
        out[at + 1 + (v >> 3) as usize] |= 1 << (v & 7);
    }
}
#[inline(always)]
pub fn bitmap_contains(img: &[u8], v: u32) -> bool {
    let n = (img[0] & 0x7f) as usize;
    let i = (v >> 3) as usize;
    i < n && (img[1 + i] >> (v & 7)) & 1 != 0
}
#[inline(always)]
pub fn bitmap_rank(img: &[u8], v: u32) -> u32 {
    let n = (img[0] & 0x7f) as usize;
    let b = &img[1..1 + n];
    let v = (v as usize).min(8 * n);
    if n <= 16 {
        // two words, each masked to the positions below v: no loop, no branch on v
        let lo = word_at(b, 0) & mask_below(v.min(64));
        let hi = word_at(b, 8) & mask_below(v.saturating_sub(64));
        return lo.count_ones() + hi.count_ones();
    }
    let mut r = 0;
    let mut at = 0;
    while at + 64 <= v {
        r += word_at(b, at / 8).count_ones();
        at += 64;
    }
    if v > at {
        r += (word_at(b, at / 8) & ((1u64 << (v - at)) - 1)).count_ones();
    }
    r
}
#[inline(always)]
pub fn bitmap_count(img: &[u8]) -> u32 {
    let n = (img[0] & 0x7f) as usize;
    let b = &img[1..1 + n];
    (0..n)
        .step_by(8)
        .map(|at| word_at(b, at).count_ones())
        .sum()
}
#[inline(always)]
pub fn bitmap_select(img: &[u8], mut k: u32) -> u32 {
    let n = (img[0] & 0x7f) as usize;
    let b = &img[1..1 + n];
    let mut at = 0;
    loop {
        let w = word_at(b, at);
        let c = w.count_ones();
        if k < c {
            return 8 * at as u32 + select_in_word(w, k);
        }
        k -= c;
        at += 8;
    }
}
#[inline(always)]
pub fn bitmap_walk(img: &[u8], mut f: impl FnMut(u32)) {
    let n = (img[0] & 0x7f) as usize;
    let b = &img[1..1 + n];
    for at in (0..n).step_by(8) {
        let mut w = word_at(b, at);
        while w != 0 {
            f(8 * at as u32 + w.trailing_zeros());
            w &= w - 1;
        }
    }
}
#[inline(always)]
pub fn bitmap_and_len(a: &[u8], b: &[u8]) -> u32 {
    let n = ((a[0] & 0x7f) as usize).min((b[0] & 0x7f) as usize);
    let (a, b) = (&a[1..1 + n], &b[1..1 + n]);
    (0..n)
        .step_by(8)
        .map(|at| (word_at(a, at) & word_at(b, at)).count_ones())
        .sum()
}
/// Add `v` to the bitmap in `slot` (image at its start, zeros after); true when it was new.
#[inline(always)]
pub fn bitmap_add(slot: &mut [u8], v: u32) -> bool {
    let n = (slot[0] & 0x7f) as usize;
    let i = (v >> 3) as usize;
    let bit = 1u8 << (v & 7);
    let was = slot[1 + i] & bit != 0;
    slot[1 + i] |= bit;
    slot[0] = 0x80 | n.max(i + 1) as u8;
    !was
}
/// Remove `v`; true when it was held. The image is trimmed to its last nonzero byte.
#[inline(always)]
pub fn bitmap_remove(slot: &mut [u8], v: u32) -> bool {
    let mut n = (slot[0] & 0x7f) as usize;
    let i = (v >> 3) as usize;
    if i >= n {
        return false;
    }
    let bit = 1u8 << (v & 7);
    let was = slot[1 + i] & bit != 0;
    slot[1 + i] &= !bit;
    while n > 0 && slot[n] == 0 {
        n -= 1;
    }
    slot[0] = 0x80 | n as u8;
    was
}

// ── Single and List ─────────────────────────────────────────────────────────────────────────────
pub fn list_len(s: &[u32]) -> Option<usize> {
    let k = s.len();
    (1..=8)
        .contains(&k)
        .then(|| 1 + nbytes(s[0]) + (k - 1) * nbytes(s[k - 1] - s[0]))
}
pub fn list_write(s: &[u32], out: &mut Vec<u8>) {
    let k = s.len();
    let wa = nbytes(s[0]);
    let wd = nbytes(s[k - 1] - s[0]);
    out.push(if k == 1 {
        0x20 | (wa - 1) as u8
    } else {
        ((wd - 1) << 5 | (k - 1) << 2 | (wa - 1)) as u8
    });
    out.extend_from_slice(&s[0].to_le_bytes()[..wa]);
    for &v in &s[1..] {
        out.extend_from_slice(&(v - s[0]).to_le_bytes()[..wd]);
    }
}
/// (k, wa, wd, first) of a Single or List tag.
#[inline(always)]
fn list_head(img: &[u8]) -> (usize, usize, usize, u32) {
    let t = img[0] as usize;
    let wa = (t & 3) + 1;
    (((t >> 2) & 7) + 1, wa, (t >> 5) + 1, field(img, 1, wa))
}
/// (deltas below `d`, whether some delta is `d`) over the k - 1 deltas, each read in its exact width.
#[inline(always)]
fn list_scan(img: &[u8], k: usize, wa: usize, wd: usize, d: u32) -> (u32, bool) {
    let p = &img[1 + wa..1 + wa + (k - 1) * wd];
    let mut below = 0;
    let mut hit = false;
    match wd {
        1 => p.iter().for_each(|&x| {
            below += u32::from(u32::from(x) < d);
            hit |= u32::from(x) == d;
        }),
        2 => p.as_chunks::<2>().0.iter().for_each(|&x| {
            let x = u32::from(u16::from_le_bytes(x));
            below += u32::from(x < d);
            hit |= x == d;
        }),
        3 => p.as_chunks::<3>().0.iter().for_each(|&x| {
            let x = u32::from_le_bytes([x[0], x[1], x[2], 0]);
            below += u32::from(x < d);
            hit |= x == d;
        }),
        _ => p.as_chunks::<4>().0.iter().for_each(|&x| {
            let x = u32::from_le_bytes(x);
            below += u32::from(x < d);
            hit |= x == d;
        }),
    }
    (below, hit)
}
#[inline(always)]
pub fn list_rank(img: &[u8], v: u32) -> u32 {
    let (k, wa, wd, first) = list_head(img);
    if v <= first {
        return 0;
    }
    1 + list_scan(img, k, wa, wd, v - first).0
}
#[inline(always)]
pub fn list_contains(img: &[u8], v: u32) -> bool {
    let (k, wa, wd, first) = list_head(img);
    v >= first && (v == first || list_scan(img, k, wa, wd, v - first).1)
}
#[inline(always)]
pub fn list_select(img: &[u8], j: u32) -> u32 {
    let (_, wa, wd, first) = list_head(img);
    if j == 0 {
        first
    } else {
        first + field(img, 1 + wa + (j as usize - 1) * wd, wd)
    }
}
#[inline(always)]
pub fn list_count(img: &[u8]) -> u32 {
    ((img[0] as u32 >> 2) & 7) + 1
}
#[inline(always)]
pub fn list_walk(img: &[u8], mut f: impl FnMut(u32)) {
    let (k, wa, wd, first) = list_head(img);
    f(first);
    for i in 0..k - 1 {
        f(first + field(img, 1 + wa + i * wd, wd));
    }
}

// ── Elias-Fano block ────────────────────────────────────────────────────────────────────────────
/// The low width of `c` values in `0..=span` (the crate's closed form).
#[inline(always)]
pub fn ef_low(c: u32, span: u32) -> u32 {
    if span <= c {
        return 0;
    }
    let mut low = (31 - span.leading_zeros()) - (31 - c.leading_zeros());
    if low != 0 && (span >> low) < c {
        low -= 1;
    }
    low
}
#[derive(Clone, Copy)]
pub enum Shape {
    Run,
    Bits,
    Ef { low: u32, high_bytes: usize },
}
/// The block's form and byte length, from its count and span alone.
#[inline(always)]
pub fn block_shape(c: u32, span: u32) -> (Shape, usize) {
    if span + 1 == c {
        return (Shape::Run, 0);
    }
    let low = ef_low(c, span);
    let high_bytes = (((span >> low) + 1 + c) as usize).div_ceil(8);
    let ef = high_bytes + (c as usize * low as usize).div_ceil(8);
    let bits = (span as usize + 8) / 8;
    if bits <= ef {
        (Shape::Bits, bits)
    } else {
        (Shape::Ef { low, high_bytes }, ef)
    }
}
pub fn block_write(d: &[u32], span: u32, out: &mut Vec<u8>) {
    let c = d.len() as u32;
    let (shape, len) = block_shape(c, span);
    let at = out.len();
    out.resize(at + len, 0);
    match shape {
        Shape::Run => {}
        Shape::Bits => {
            for &x in d {
                out[at + (x >> 3) as usize] |= 1 << (x & 7);
            }
        }
        Shape::Ef { low, high_bytes } => {
            let low_at = at + high_bytes;
            for (j, &x) in d.iter().enumerate() {
                let h = (x >> low) as usize + j;
                out[at + h / 8] |= 1 << (h % 8);
                let bit = j * low as usize;
                let mut y = u64::from(x & ((1u32 << low) - 1)) << (bit % 8);
                let mut p = low_at + bit / 8;
                while y != 0 {
                    out[p] |= y as u8;
                    y >>= 8;
                    p += 1;
                }
            }
        }
    }
}
#[inline(always)]
fn low_at(b: &[u8], base: usize, low: u32, j: usize) -> u32 {
    let bit = j * low as usize;
    ((word_at(b, base + bit / 8) >> (bit % 8)) as u32) & ((1u32 << low) - 1)
}
/// Position of zero number `k` in the plane at `at`.
#[inline(always)]
fn select_zero(b: &[u8], at: usize, mut k: u32) -> usize {
    let mut i = 0;
    loop {
        let w = !word_at(b, at + i);
        let c = w.count_ones();
        if k < c {
            return 8 * i + select_in_word(w, k) as usize;
        }
        k -= c;
        i += 8;
    }
}
#[inline(always)]
fn select_one(b: &[u8], at: usize, mut k: u32) -> usize {
    let mut i = 0;
    loop {
        let w = word_at(b, at + i);
        let c = w.count_ones();
        if k < c {
            return 8 * i + select_in_word(w, k) as usize;
        }
        k -= c;
        i += 8;
    }
}
#[inline(always)]
fn bit(b: &[u8], at: usize, i: usize) -> bool {
    (b[at + i / 8] >> (i % 8)) & 1 != 0
}
/// (members of the block below `d`, whether `d` is a member). `d <= span`.
#[inline(always)]
pub fn block_rank(b: &[u8], at: usize, c: u32, span: u32, d: u32) -> (u32, bool) {
    match block_shape(c, span).0 {
        Shape::Run => (d, true),
        Shape::Bits => {
            let mut r = 0;
            let mut i = 0;
            while i + 64 <= d as usize {
                r += word_at(b, at + i / 8).count_ones();
                i += 64;
            }
            let w = word_at(b, at + i / 8) >> (d as usize - i);
            // bits below d in this word: shift the rest away
            let below = word_at(b, at + i / 8) & !(u64::MAX << (d as usize - i));
            (r + below.count_ones(), w & 1 != 0)
        }
        Shape::Ef { low, high_bytes } => {
            let h = d >> low;
            let dl = d & ((1u32 << low) - 1);
            let start = if h == 0 {
                0
            } else {
                select_zero(b, at, h - 1) + 1
            };
            // the bucket's members are the ones from `start` to the next zero
            let run = (!(word_at(b, at + start / 8) >> (start % 8))).trailing_zeros() as usize;
            let j = start - h as usize;
            let mut t = 0;
            while t < run {
                let x = low_at(b, at + high_bytes, low, j + t);
                if x >= dl {
                    return ((j + t) as u32, x == dl);
                }
                t += 1;
            }
            // a bucket longer than the word's rest: the slow tail (never taken by a bucket under 57 members)
            let mut p = start + run;
            let mut j = j + run;
            let end = 8 * high_bytes;
            while run >= 57 && p < end && bit(b, at, p) {
                let x = low_at(b, at + high_bytes, low, j);
                if x >= dl {
                    return (j as u32, x == dl);
                }
                j += 1;
                p += 1;
            }
            (j as u32, false)
        }
    }
}
#[inline(always)]
pub fn block_select(b: &[u8], at: usize, c: u32, span: u32, j: u32) -> u32 {
    match block_shape(c, span).0 {
        Shape::Run => j,
        Shape::Bits => select_one(b, at, j) as u32,
        Shape::Ef { low, high_bytes } => {
            let p = select_one(b, at, j) as u32;
            ((p - j) << low) | low_at(b, at + high_bytes, low, j as usize)
        }
    }
}
#[inline(always)]
pub fn block_walk(b: &[u8], at: usize, c: u32, span: u32, lo: u32, f: &mut impl FnMut(u32)) {
    match block_shape(c, span).0 {
        Shape::Run => (0..c).for_each(|j| f(lo + j)),
        Shape::Bits => {
            let n = (span as usize + 8) / 8;
            for i in (0..n).step_by(8) {
                let mut w = word_at(b, at + i);
                if i + 8 > n {
                    w &= u64::MAX >> (64 - 8 * (n - i));
                }
                while w != 0 {
                    f(lo + 8 * i as u32 + w.trailing_zeros());
                    w &= w - 1;
                }
            }
        }
        Shape::Ef { low, high_bytes } => {
            let mut j = 0u32;
            for i in (0..high_bytes).step_by(8) {
                let mut w = word_at(b, at + i);
                if i + 8 > high_bytes {
                    w &= u64::MAX >> (64 - 8 * (high_bytes - i));
                }
                while w != 0 {
                    let p = 8 * i as u32 + w.trailing_zeros();
                    f(lo + (((p - j) << low) | low_at(b, at + high_bytes, low, j as usize)));
                    j += 1;
                    w &= w - 1;
                }
            }
        }
    }
}

// ── Blocks ──────────────────────────────────────────────────────────────────────────────────────
pub fn blocks_len(s: &[u32]) -> usize {
    let n = s.len();
    let head = 1 + varint_len(n as u32 - 1) + varint_len(s[0]);
    if n <= BLOCK {
        return head + varint_len(s[n - 1] - s[0]) + block_shape(n as u32, s[n - 1] - s[0]).1;
    }
    let mut z = head;
    let mut lo = s[0];
    for blk in s.chunks(BLOCK) {
        let last = blk[blk.len() - 1];
        z += 8 + block_shape(blk.len() as u32, last - lo).1;
        lo = last + 1;
    }
    z
}
pub fn blocks_write(s: &[u32], out: &mut Vec<u8>) {
    let n = s.len();
    out.push(0x02);
    put_varint(out, n as u32 - 1);
    put_varint(out, s[0]);
    let mut d = [0u32; BLOCK];
    if n <= BLOCK {
        put_varint(out, s[n - 1] - s[0]);
        for (x, &v) in d.iter_mut().zip(s) {
            *x = v - s[0];
        }
        block_write(&d[..n], s[n - 1] - s[0], out);
        return;
    }
    let blocks = n.div_ceil(BLOCK);
    let table = out.len();
    out.resize(table + 8 * blocks, 0);
    let payload = out.len();
    let mut lo = s[0];
    for (i, blk) in s.chunks(BLOCK).enumerate() {
        let last = blk[blk.len() - 1];
        for (x, &v) in d.iter_mut().zip(blk) {
            *x = v - lo;
        }
        block_write(&d[..blk.len()], last - lo, out);
        out[table + 4 * i..table + 4 * i + 4].copy_from_slice(&last.to_le_bytes());
        let end = (out.len() - payload) as u32;
        out[table + 4 * (blocks + i)..table + 4 * (blocks + i) + 4]
            .copy_from_slice(&end.to_le_bytes());
        lo = last + 1;
    }
}
/// A Blocks image opened: its header decoded once, nothing else read.
#[derive(Clone, Copy)]
pub struct Blocks<'a> {
    b: &'a [u8],
    pub n: u32,
    first: u32,
    /// For one block: its span. Otherwise unused.
    span: u32,
    /// Offset of the `last` table (or of the single block).
    table: usize,
    blocks: usize,
}
impl<'a> Blocks<'a> {
    #[inline(always)]
    pub fn open(b: &'a [u8]) -> Self {
        let mut at = 1;
        let n = varint(b, &mut at) + 1;
        let first = varint(b, &mut at);
        if n as usize <= BLOCK {
            let span = varint(b, &mut at);
            Self {
                b,
                n,
                first,
                span,
                table: at,
                blocks: 1,
            }
        } else {
            Self {
                b,
                n,
                first,
                span: 0,
                table: at,
                blocks: (n as usize).div_ceil(BLOCK),
            }
        }
    }
    /// The first block whose last member is at least `v`, or `blocks`.
    #[inline(always)]
    fn find(&self, v: u32) -> usize {
        let (mut lo, mut len) = (0usize, self.blocks);
        while len > 1 {
            let half = len / 2;
            lo += usize::from(u32_at(self.b, self.table + 4 * (lo + half - 1)) < v) * half;
            len -= half;
        }
        lo + usize::from(u32_at(self.b, self.table + 4 * lo) < v)
    }
    /// (byte offset, count, span, lowest value) of block `i`.
    #[inline(always)]
    fn block(&self, i: usize) -> (usize, u32, u32, u32) {
        if self.blocks == 1 {
            return (self.table, self.n, self.span, self.first);
        }
        let payload = self.table + 8 * self.blocks;
        let (at, lo) = if i == 0 {
            (payload, self.first)
        } else {
            (
                payload + u32_at(self.b, self.table + 4 * (self.blocks + i - 1)) as usize,
                u32_at(self.b, self.table + 4 * (i - 1)) + 1,
            )
        };
        let c = if i + 1 == self.blocks {
            self.n - (BLOCK * i) as u32
        } else {
            BLOCK as u32
        };
        (at, c, u32_at(self.b, self.table + 4 * i) - lo, lo)
    }
    /// (members below `v`, whether `v` is one).
    #[inline(always)]
    pub fn rank_holds(&self, v: u32) -> (u32, bool) {
        let i = if self.blocks == 1 {
            usize::from(v > self.first.wrapping_add(self.span))
        } else {
            self.find(v)
        };
        if i == self.blocks {
            return (self.n, false);
        }
        let (at, c, span, lo) = self.block(i);
        if v < lo {
            return ((BLOCK * i) as u32, false);
        }
        let (r, hit) = block_rank(self.b, at, c, span, v - lo);
        ((BLOCK * i) as u32 + r, hit)
    }
    #[inline(always)]
    pub fn select(&self, k: u32) -> u32 {
        let i = k as usize / BLOCK;
        let (at, c, span, lo) = self.block(i);
        lo + block_select(self.b, at, c, span, k - (BLOCK * i) as u32)
    }
    /// The least member at or above `v`, with its rank; None past the last.
    #[inline(always)]
    pub fn seek(&self, v: u32) -> Option<(u32, u32)> {
        let (r, hit) = self.rank_holds(v);
        if hit {
            Some((v, r))
        } else if r < self.n {
            Some((self.select(r), r))
        } else {
            None
        }
    }
    #[inline(always)]
    pub fn walk(&self, mut f: impl FnMut(u32)) {
        for i in 0..self.blocks {
            let (at, c, span, lo) = self.block(i);
            block_walk(self.b, at, c, span, lo, &mut f);
        }
    }
    #[inline(always)]
    fn decode(&self, i: usize, out: &mut [u32; BLOCK]) -> usize {
        let (at, c, span, lo) = self.block(i);
        let mut n = 0;
        block_walk(self.b, at, c, span, lo, &mut |v| {
            out[n] = v;
            n += 1;
        });
        n
    }
    #[inline(always)]
    fn last(&self, i: usize) -> u32 {
        if self.blocks == 1 {
            self.first + self.span
        } else {
            u32_at(self.b, self.table + 4 * i)
        }
    }
    #[inline(always)]
    fn lowest(&self, i: usize) -> u32 {
        if i == 0 {
            self.first
        } else {
            u32_at(self.b, self.table + 4 * (i - 1)) + 1
        }
    }
    /// Members shared with `other`: blocks whose ranges miss each other are skipped by the block table alone;
    /// two blocks that overlap are decoded once each and merged.
    pub fn and_len(&self, other: &Blocks<'_>) -> u32 {
        let (mut i, mut j) = (0, 0);
        let (mut da, mut db) = ([0u32; BLOCK], [0u32; BLOCK]);
        let (mut na, mut nb) = (0, 0);
        let (mut ia, mut ib) = (usize::MAX, usize::MAX);
        let mut count = 0;
        while i < self.blocks && j < other.blocks {
            if self.last(i) < other.lowest(j) {
                // gallop: the first block of self that can reach other's block
                i = if self.blocks == 1 {
                    1
                } else {
                    self.find(other.lowest(j))
                };
                continue;
            }
            if other.last(j) < self.lowest(i) {
                j = if other.blocks == 1 {
                    1
                } else {
                    other.find(self.lowest(i))
                };
                continue;
            }
            if ia != i {
                na = self.decode(i, &mut da);
                ia = i;
            }
            if ib != j {
                nb = other.decode(j, &mut db);
                ib = j;
            }
            let (mut x, mut y) = (0, 0);
            while x < na && y < nb {
                let (a, b) = (da[x], db[y]);
                count += u32::from(a == b);
                x += usize::from(a <= b);
                y += usize::from(b <= a);
            }
            if self.last(i) <= other.last(j) {
                i += 1;
            } else {
                j += 1;
            }
        }
        count
    }
}

// ── Spans and RunEf (measured, not proposed) ────────────────────────────────────────────────────
pub fn runs_of(s: &[u32]) -> Vec<(u32, u32)> {
    let mut out = Vec::new();
    let mut st = s[0];
    let mut p = s[0];
    for &v in &s[1..] {
        if v != p + 1 {
            out.push((st, p));
            st = v;
        }
        p = v;
    }
    out.push((st, p));
    out
}
pub fn spans_write(s: &[u32], out: &mut Vec<u8>) -> bool {
    let rs = runs_of(s);
    if rs.len() > 32 || s[s.len() - 1] - s[0] >= 65535 {
        return false;
    }
    out.push(0x60);
    out.push(rs.len() as u8);
    put_varint(out, s[0]);
    for (i, &(st, e)) in rs.iter().enumerate() {
        if i > 0 {
            out.extend_from_slice(&((st - s[0]) as u16).to_le_bytes());
        }
        out.extend_from_slice(&((e + 1 - s[0]) as u16).to_le_bytes());
    }
    true
}
/// Members below `v`: the clamped distance of `v` into every interval, no branch on the data.
#[inline(always)]
pub fn spans_rank_holds(img: &[u8], v: u32) -> (u32, bool) {
    let r = img[1] as usize;
    let mut at = 2;
    let first = varint(img, &mut at);
    let d = v.saturating_sub(first).min(65535);
    let mut start = 0u32;
    let mut rank = 0;
    let mut hit = false;
    for i in 0..r {
        if i > 0 {
            start = u32::from(u16::from_le_bytes([img[at], img[at + 1]]));
            at += 2;
        }
        let end = u32::from(u16::from_le_bytes([img[at], img[at + 1]]));
        at += 2;
        rank += d.clamp(start, end) - start;
        hit |= (d >= start) & (d < end);
    }
    (rank, hit & (v >= first))
}
pub fn runef_write(s: &[u32], out: &mut Vec<u8>) -> bool {
    let rs = runs_of(s);
    if rs.len() > 32 {
        return false;
    }
    let mut d = Vec::new();
    for &(st, e) in &rs {
        d.push(st - s[0]);
        d.push(e + 1 - s[0]);
    }
    let span = *d.last().unwrap();
    out.push(0x61);
    out.push(rs.len() as u8);
    put_varint(out, s[0]);
    put_varint(out, span);
    block_write(&d, span, out);
    true
}
/// Rank through the boundaries: every boundary is read once (their count is the tag's), then the same
/// clamped distances. Without stored prefix counts this is the only way a boundary code answers rank.
#[inline(always)]
pub fn runef_rank_holds(img: &[u8], v: u32) -> (u32, bool) {
    let r = img[1] as usize;
    let mut at = 2;
    let first = varint(img, &mut at);
    let span = varint(img, &mut at);
    let d = v.saturating_sub(first).min(span);
    let mut bounds = [0u32; 64];
    let mut n = 0;
    block_walk(img, at, 2 * r as u32, span, 0, &mut |x| {
        bounds[n] = x;
        n += 1;
    });
    let mut rank = 0;
    let mut hit = false;
    for i in 0..r {
        let (start, end) = (bounds[2 * i], bounds[2 * i + 1]);
        rank += d.clamp(start, end) - start;
        hit |= (d >= start) & (d < end);
    }
    (rank, hit & (v >= first))
}
/// The same with the answer taken from the boundary rank's parity (contains only).
#[inline(always)]
pub fn runef_contains(img: &[u8], v: u32) -> bool {
    let r = img[1] as u32;
    let mut at = 2;
    let first = varint(img, &mut at);
    let span = varint(img, &mut at);
    if v < first || v - first >= span {
        return false;
    }
    // boundaries at or below d: rank(d + 1); odd means inside an interval
    let (below, hit) = block_rank(img, at, 2 * r, span, v - first);
    (below + u32::from(hit)) & 1 == 1
}
