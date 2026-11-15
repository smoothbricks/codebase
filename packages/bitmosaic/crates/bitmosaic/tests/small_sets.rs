//! What a small set costs as a native image, beside a plain bitmap of its
//! universe, a plain bitmap of its span, and the information-theoretic
//! floor. `nx run bitmosaic:test` checks the bounds; the table itself is
//! printed by the test (`--nocapture`).

use bitmosaic::{Bitmosaic, BitmosaicView};

fn xorshift(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

/// `k` distinct members of `0..n`, ascending: a partial shuffle of the universe.
fn draw(k: usize, n: u32, state: &mut u64) -> Vec<u32> {
    let mut pool: Vec<u32> = (0..n).collect();
    for i in 0..k {
        let j = i + (xorshift(state) % (u64::from(n) - i as u64)) as usize;
        pool.swap(i, j);
    }
    pool.truncate(k);
    pool.sort_unstable();
    pool
}

/// `k` members spread evenly over `0..n`, the first and the last ordinal included.
fn spread(k: u32, n: u32) -> Vec<u32> {
    (0..k).map(|i| i * (n - 1) / (k - 1)).collect()
}

/// Bytes needed to name one `k`-subset of an `n`-universe.
fn floor_bytes(k: usize, n: u32) -> usize {
    let bits: f64 = (1..=k)
        .map(|i| ((f64::from(n) - k as f64 + i as f64) / i as f64).log2())
        .sum();
    (bits / 8.0).ceil() as usize
}

/// The sealed image's length and the slot (forest-family) image's, after
/// both are admitted and read back.
fn image_len(members: &[u32]) -> (usize, usize) {
    let set = Bitmosaic::from_sorted(members.iter().copied());
    let (image, slot) = (set.to_bytes(), set.to_forest_bytes());
    for bytes in [&image, &slot] {
        let view = BitmosaicView::open_verified(bytes).expect("a written image is admitted");
        assert_eq!(view.serialized_len(), bytes.len());
        assert!(view.range().eq(members.iter().copied()));
    }
    (image.len(), slot.len())
}

/// A plain bitmap from the set's first member to its last.
fn span_bitmap(members: &[u32]) -> usize {
    (members[members.len() - 1] - members[0] + 1).div_ceil(8) as usize
}

//#region smoo/bitmosaic!n/small-set-sizes
/// The rows a consumer measured on the first native format, with the size
/// each then had and the size it may have now.
#[test]
fn the_measured_ordinal_sets_cost_at_most_their_stated_bytes() {
    /// A set: its name, universe, members, bytes in the first native format and bytes allowed now.
    type Measured = (&'static str, u32, Vec<u32>, usize, usize);
    let rows: Vec<Measured> = vec![
        ("service ordinals, 4", 129, vec![0, 1, 2, 10], 18, 8),
        ("service ordinals, 13", 129, spread(13, 129), 25, 17),
        ("service ordinals, 20", 129, spread(20, 129), 25, 20),
        // 83 spread over 129 ordinals: a bitmap of the span alone is 17.
        ("service ordinals, 83", 129, spread(83, 129), 44, 20),
        ("service ordinals, 70", 129, spread(70, 129), 52, 20),
        ("schema ordinals, 4", 83, spread(4, 83), 18, 8),
        ("schema ordinals, 13", 70, spread(13, 70), 24, 13),
        ("schema ordinals, 20", 83, spread(20, 83), 23, 15),
        ("schema ordinals, all 83", 83, (0..83).collect(), 14, 6),
        ("sparse, 4", 1_000, spread(4, 1_000), 18, 18),
        ("sparse, 13", 1_000, spread(13, 1_000), 31, 31),
    ];
    println!(
        "set                      members  universe  before  after  universe-bitmap  span-bitmap  floor"
    );
    let mut over = Vec::new();
    for (name, universe, members, before, allowed) in &rows {
        let (image, slot) = image_len(members);
        assert_eq!(image, slot, "{name}: a small set seals as its slot image");
        let span = span_bitmap(members);
        println!(
            "{name:<24} {:>7}  {universe:>8}  {before:>6}  {image:>5}  {:>15}  {span:>11}  {:>5}",
            members.len(),
            universe.div_ceil(8),
            floor_bytes(members.len(), *universe),
        );
        // Never more than three bytes above a plain bitmap of the span.
        if image > *allowed || image > span + 3 {
            over.push((*name, image));
        }
    }
    assert!(over.is_empty(), "rows over their stated size: {over:?}");
}

struct Row {
    k: usize,
    n: u32,
    /// Largest image over the draws this row may cost, when the format states one.
    at_most: Option<usize>,
}

const DRAWS: usize = 256;

#[test]
fn a_small_set_costs_its_members_and_not_its_framing() {
    let mut rows = vec![
        Row {
            k: 1,
            n: 83,
            at_most: Some(2),
        },
        Row {
            k: 2,
            n: 83,
            at_most: Some(3),
        },
        Row {
            k: 4,
            n: 83,
            at_most: Some(5),
        },
        Row {
            k: 13,
            n: 70,
            at_most: Some(11),
        },
        Row {
            k: 20,
            n: 83,
            at_most: Some(13),
        },
        Row {
            k: 40,
            n: 83,
            at_most: Some(13),
        },
        Row {
            k: 83,
            n: 83,
            at_most: Some(6),
        },
    ];
    for n in [256, 4_096, 65_536] {
        for k in [1, 2, 4, 16, 64] {
            rows.push(Row {
                k,
                n,
                at_most: (k == 1).then_some(3),
            });
        }
    }
    let mut state = 0x5EED_5E75_0000_0001u64;
    println!("members  universe  image(min..max)  slot-form(max)  plain-bitmap  floor");
    let mut over = Vec::new();
    for row in rows {
        let (mut min, mut max, mut slot_max) = (usize::MAX, 0usize, 0usize);
        for _ in 0..DRAWS {
            let members = draw(row.k, row.n, &mut state);
            let (image, slot) = image_len(&members);
            min = min.min(image);
            max = max.max(image);
            slot_max = slot_max.max(slot);
            // Within 256 positions a set never costs more than three bytes
            // above the bitmap of its span.
            if row.n <= 256 && image > span_bitmap(&members) + 3 {
                over.push((row.k, row.n, image, None));
            }
        }
        println!(
            "{:>7}  {:>8}  {:>6}..{:<7}  {:>14}  {:>12}  {:>5}",
            row.k,
            row.n,
            min,
            max,
            slot_max,
            row.n.div_ceil(8),
            floor_bytes(row.k, row.n),
        );
        if row.at_most.is_some_and(|bound| max > bound) {
            over.push((row.k, row.n, max, row.at_most));
        }
    }
    for (name, members, bound) in [
        ("run 0..200", (0..200).collect::<Vec<u32>>(), 6),
        ("run 1000..1200", (1_000..1_200).collect(), 6),
        ("full chunk 0..65536", (0..65_536).collect(), 7),
    ] {
        let (image, _) = image_len(&members);
        println!("{name}: {image} bytes");
        if image > bound {
            over.push((members.len(), members.len() as u32, image, Some(bound)));
        }
    }
    assert!(
        over.is_empty(),
        "rows over their stated size (k, n, max, bound): {over:?}"
    );
}
//#endregion smoo/bitmosaic!n/small-set-sizes
