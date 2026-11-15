//! Fixed-work cells for the small image's reads, three ways:
//!
//! - `view`: through [`BitmosaicView`], which reaches a small image by one
//!   call;
//! - `small`: through [`SmallView`], inline on the image bytes;
//! - `page`: on a decoded copy (a base and four words), the comparison the
//!   in-place reader is held against. The copy exists here only.
//!
//! ```sh
//! cargo build --locked -p bitmosaic --profile bench --example small_gate
//! /usr/bin/time -l target/release/examples/small_gate window small contains 100000
//! ```
//!
//! `small_gate <shape> <view|small|page> <op> <iterations>`; an operation
//! with the suffix `_sorted` probes in ascending order. As with
//! `format_gate`, a cell's cost is its count at N iterations less its count
//! at zero, divided by N.

use std::hint::black_box;

use bitmosaic::{Bitmosaic, BitmosaicView, SmallView};

fn xorshift(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

/// The set of a shape and the set it is intersected with.
fn shape(name: &str) -> (Vec<u32>, Vec<u32>) {
    match name {
        // List8: four ordinals of 83.
        "list8" => (vec![7, 17, 40, 82], vec![3, 17, 41, 82]),
        // Window: thirteen ordinals of 70.
        "window" => (
            vec![1, 3, 9, 14, 20, 25, 33, 40, 47, 55, 60, 66, 69],
            vec![0, 3, 8, 14, 21, 25, 30, 40, 48, 55, 61, 66, 68],
        ),
        // Window: a hundred of 250, all four words.
        "window-wide" => (
            (0..250).filter(|v| v % 5 < 2).collect(),
            (0..250).filter(|v| v % 3 == 0 || v % 7 == 0).collect(),
        ),
        // Window at: a dozen ordinals from 1,000.
        "window-at" => (
            (0..12).map(|i| 1_000 + 7 * i + i % 2).collect(),
            (0..12).map(|i| 1_003 + 5 * i + i % 3).collect(),
        ),
        // List16: eight members spread over a chunk.
        "list16" => (
            (0..8).map(|i| 500 + 900 * i * i).collect(),
            (0..8).map(|i| 500 + 900 * (i + 2) * (i + 2)).collect(),
        ),
        // Spans: three intervals.
        "spans" => (
            (100..400).chain(1_000..1_200).chain(9_000..9_050).collect(),
            (300..1_100).chain(9_010..9_020).collect(),
        ),
        other => panic!("unknown shape {other}"),
    }
}

/// A decoded copy of a set within 256 positions: what "decode at open"
/// would hold.
#[derive(Clone, Copy)]
struct Page {
    base: u32,
    words: [u64; 4],
}

impl Page {
    fn decode(view: &SmallView<'_>) -> Option<Page> {
        let base = view.select(0)? & !63;
        let mut page = Page {
            base,
            words: [0; 4],
        };
        let mut fits = true;
        view.for_each(|value| {
            let bit = (value - base) as usize;
            if bit < 256 {
                page.words[bit / 64] |= 1 << (bit % 64);
            } else {
                fits = false;
            }
        });
        fits.then_some(page)
    }

    #[inline(always)]
    fn contains(&self, value: u32) -> bool {
        let bit = value.wrapping_sub(self.base);
        bit < 256 && self.words[(bit >> 6) as usize] >> (bit & 63) & 1 != 0
    }

    #[inline(always)]
    fn rank(&self, value: u32) -> u32 {
        let bit = value.saturating_sub(self.base).min(256) as usize;
        let mut count = 0;
        for word in &self.words[..bit / 64] {
            count += word.count_ones();
        }
        if !bit.is_multiple_of(64) {
            count += (self.words[bit / 64] & ((1u64 << (bit % 64)) - 1)).count_ones();
        }
        count
    }

    #[inline(always)]
    fn and_len(&self, other: &Page) -> u32 {
        if self.base != other.base {
            return 0;
        }
        (0..4)
            .map(|i| (self.words[i] & other.words[i]).count_ones())
            .sum()
    }

    #[inline(always)]
    fn walk(&self, mut f: impl FnMut(u32)) {
        for (i, &word) in self.words.iter().enumerate() {
            let mut bits = word;
            while bits != 0 {
                f(self.base + 64 * i as u32 + bits.trailing_zeros());
                bits &= bits - 1;
            }
        }
    }
}

struct Cell<'a> {
    image: &'a [u8],
    view: BitmosaicView<'a>,
    other: BitmosaicView<'a>,
    small: SmallView<'a>,
    small_other: SmallView<'a>,
    page: Option<Page>,
    page_other: Option<Page>,
    probes: &'a [u32],
    turn: std::cell::Cell<usize>,
    sorted: &'a [u32],
    ranks: &'a [u32],
}

const PROBES: usize = 1024;
const PROBE_TURNS: usize = 64;

impl Cell<'_> {
    fn probes(&self) -> &[u32] {
        let turn = self.turn.get();
        self.turn.set((turn + 1) % PROBE_TURNS);
        &self.probes[turn * PROBES..(turn + 1) * PROBES]
    }
}

macro_rules! probe_cell {
    ($name:ident, $probes:ident, |$cell:ident, $p:ident| $body:expr) => {
        #[inline(never)]
        fn $name($cell: &Cell<'_>) -> u64 {
            let probes = probe_cell!(@probes $cell, $probes);
            let mut sum = 0u64;
            for &$p in probes {
                let $p = black_box($p);
                sum += u64::from($body);
            }
            sum
        }
    };
    (@probes $cell:ident, random) => {
        $cell.probes()
    };
    (@probes $cell:ident, sorted) => {
        $cell.sorted
    };
    (@probes $cell:ident, ranks) => {
        $cell.ranks
    };
}

probe_cell!(view_contains, random, |cell, p| black_box(&cell.view)
    .contains(p));
probe_cell!(view_contains_sorted, sorted, |cell, p| black_box(
    &cell.view
)
.contains(p));
probe_cell!(view_rank, random, |cell, p| black_box(&cell.view).rank(p)
    as u32);
probe_cell!(
    view_rank_sorted,
    sorted,
    |cell, p| black_box(&cell.view).rank(p) as u32
);
probe_cell!(view_select, ranks, |cell, k| black_box(&cell.view)
    .select(u64::from(k))
    .unwrap_or(0));
probe_cell!(
    view_and_rank,
    random,
    |cell, p| black_box(&cell.view).and_rank(&cell.other, p) as u32
);
probe_cell!(small_contains, random, |cell, p| black_box(&cell.small)
    .contains(p));
probe_cell!(small_contains_sorted, sorted, |cell, p| black_box(
    &cell.small
)
.contains(p));
probe_cell!(small_rank, random, |cell, p| black_box(&cell.small).rank(p));
probe_cell!(small_rank_sorted, sorted, |cell, p| black_box(&cell.small)
    .rank(p));
probe_cell!(small_select, ranks, |cell, k| black_box(&cell.small)
    .select(k)
    .unwrap_or(0));
probe_cell!(small_and_rank, random, |cell, p| black_box(&cell.small)
    .and_rank(&cell.small_other, p));
probe_cell!(page_contains, random, |cell, p| black_box(&cell.page)
    .as_ref()
    .is_some_and(|page| page.contains(p)));
probe_cell!(page_contains_sorted, sorted, |cell, p| black_box(
    &cell.page
)
.as_ref()
.is_some_and(|page| page.contains(p)));
probe_cell!(page_rank, random, |cell, p| black_box(&cell.page)
    .as_ref()
    .map_or(0, |page| page.rank(p)));
probe_cell!(page_rank_sorted, sorted, |cell, p| black_box(&cell.page)
    .as_ref()
    .map_or(0, |page| page.rank(p)));

#[inline(never)]
fn view_open(cell: &Cell<'_>) -> u64 {
    BitmosaicView::open(black_box(cell.image)).map_or(0, |view| view.serialized_len() as u64)
}

#[inline(never)]
fn view_verify(cell: &Cell<'_>) -> u64 {
    u64::from(BitmosaicView::open_verified(black_box(cell.image)).is_ok())
}

#[inline(never)]
fn view_canonical(cell: &Cell<'_>) -> u64 {
    u64::from(BitmosaicView::open_canonical(black_box(cell.image)).is_ok())
}

#[inline(never)]
fn small_open(cell: &Cell<'_>) -> u64 {
    u64::from(SmallView::open(black_box(cell.image)).is_some())
}

#[inline(never)]
fn page_open(cell: &Cell<'_>) -> u64 {
    SmallView::open(black_box(cell.image))
        .and_then(|small| Page::decode(&small))
        .map_or(0, |page| u64::from(page.base) + 1)
}

#[inline(never)]
fn view_walk(cell: &Cell<'_>) -> u64 {
    let mut sum = 0u64;
    black_box(&cell.view).for_each(|value| sum += u64::from(value));
    sum
}

#[inline(never)]
fn view_iter(cell: &Cell<'_>) -> u64 {
    black_box(&cell.view).range().map(u64::from).sum()
}

#[inline(never)]
fn small_walk(cell: &Cell<'_>) -> u64 {
    let mut sum = 0u64;
    black_box(&cell.small).for_each(|value| sum += u64::from(value));
    sum
}

#[inline(never)]
fn small_iter(cell: &Cell<'_>) -> u64 {
    black_box(&cell.small).iter().map(u64::from).sum()
}

#[inline(never)]
fn page_walk(cell: &Cell<'_>) -> u64 {
    let mut sum = 0u64;
    if let Some(page) = black_box(&cell.page) {
        page.walk(|value| sum += u64::from(value));
    }
    sum
}

#[inline(never)]
fn view_and_len(cell: &Cell<'_>) -> u64 {
    black_box(&cell.view).and_len(black_box(&cell.other))
}

#[inline(never)]
fn small_and_len(cell: &Cell<'_>) -> u64 {
    u64::from(black_box(&cell.small).and_len(black_box(&cell.small_other)))
}

#[inline(never)]
fn page_and_len(cell: &Cell<'_>) -> u64 {
    match (black_box(&cell.page), black_box(&cell.page_other)) {
        (Some(a), Some(b)) => u64::from(a.and_len(b)),
        _ => 0,
    }
}

/// The measuring hook: given the iterations, the operations an iteration
/// makes and the iteration itself, it runs them and returns their sum.
pub type Measure<'a> = dyn FnMut(u64, u64, &mut dyn FnMut() -> u64) -> u64 + 'a;
/// Run the cell the arguments name; `measure` is `format_gate`'s hook.
pub fn gate(measure: &mut Measure<'_>) {
    let args: Vec<String> = std::env::args().collect();
    let [_, shape_name, reader, op, iterations, ..] = args.as_slice() else {
        panic!("usage: small_gate <shape> <view|small|page> <op> <iterations>");
    };
    let iterations: u64 = iterations.parse().expect("an iteration count");
    let (va, vb) = shape(shape_name);
    let image = Bitmosaic::from_sorted(va.iter().copied()).to_forest_bytes();
    let other = Bitmosaic::from_sorted(vb.iter().copied()).to_forest_bytes();
    let small = SmallView::open(&image).expect("the shape is a small image");
    let small_other = SmallView::open(&other).expect("the other shape is a small image");
    let mut state = 0x5a11_5e75_u64 ^ va.len() as u64;
    let top = u64::from(va[va.len() - 1]) + 2;
    let probes: Vec<u32> = (0..PROBES * PROBE_TURNS)
        .map(|i| {
            let r = xorshift(&mut state);
            if i % 2 == 0 {
                va[(r % va.len() as u64) as usize]
            } else {
                (r % top) as u32
            }
        })
        .collect();
    let mut sorted = probes[..PROBES].to_vec();
    sorted.sort_unstable();
    let ranks: Vec<u32> = (0..PROBES)
        .map(|_| (xorshift(&mut state) % va.len() as u64) as u32)
        .collect();
    let cell = Cell {
        image: &image,
        view: BitmosaicView::open(&image).expect("attach"),
        other: BitmosaicView::open(&other).expect("attach the other"),
        small,
        small_other,
        page: Page::decode(&small),
        page_other: Page::decode(&small_other),
        probes: &probes,
        turn: std::cell::Cell::new(0),
        sorted: &sorted,
        ranks: &ranks,
    };
    let run: fn(&Cell<'_>) -> u64 = match (reader.as_str(), op.as_str()) {
        ("view", "open") => view_open,
        ("view", "verify") => view_verify,
        ("view", "canonical") => view_canonical,
        ("view", "contains") => view_contains,
        ("view", "contains_sorted") => view_contains_sorted,
        ("view", "rank") => view_rank,
        ("view", "rank_sorted") => view_rank_sorted,
        ("view", "select") => view_select,
        ("view", "walk") => view_walk,
        ("view", "iter") => view_iter,
        ("view", "and_len") => view_and_len,
        ("view", "and_rank") => view_and_rank,
        ("small", "open") => small_open,
        ("small", "contains") => small_contains,
        ("small", "contains_sorted") => small_contains_sorted,
        ("small", "rank") => small_rank,
        ("small", "rank_sorted") => small_rank_sorted,
        ("small", "select") => small_select,
        ("small", "walk") => small_walk,
        ("small", "iter") => small_iter,
        ("small", "and_len") => small_and_len,
        ("small", "and_rank") => small_and_rank,
        ("page", "open") => page_open,
        ("page", "contains") => page_contains,
        ("page", "contains_sorted") => page_contains_sorted,
        ("page", "rank") => page_rank,
        ("page", "rank_sorted") => page_rank_sorted,
        ("page", "walk") => page_walk,
        ("page", "and_len") => page_and_len,
        other => panic!("unknown cell {other:?}"),
    };
    let operations = match op.as_str() {
        "walk" | "iter" => va.len(),
        "open" | "verify" | "canonical" | "and_len" => 1,
        _ => PROBES,
    } as u64;
    let sum = measure(iterations, operations, &mut || run(black_box(&cell)));
    println!("{shape_name} {reader} {op} x{iterations}: {sum}");
}

#[allow(dead_code)]
fn main() {
    gate(&mut |iterations, _, body| {
        let mut sum = 0u64;
        for _ in 0..iterations {
            sum = sum.wrapping_add(body());
        }
        sum
    });
}
