//! Read, write, patch and intersection costs of the candidate forms against the crate's own, per operation:
//! instructions, cycles, branch mispredicts (kpc window; run through the dtrace trampoline).
//!
//! `proto2 <data dir> [filter]`. Every image is checked against its member list before anything is measured.
//! Probes are 65,536 random (set, value) pairs in a shuffled order, half of them members.
mod forms;
mod pmc;
// The crate the candidates are measured against: the repository's own, or with the feature `small-forms` an
// implementation of small forms named in Cargo.toml (then the repository's crate is measured beside it).
use forms::*;
#[cfg(feature = "small-forms")]
use small as cr;
use std::hint::black_box;
#[cfg(not(feature = "small-forms"))]
use v1 as cr;

const EVENTS: &[&str] = &[
    "FIXED_CYCLES",
    "FIXED_INSTRUCTIONS",
    "BRANCH_MISPRED_NONSPEC",
    "INST_BRANCH",
];
const PROBES: usize = 65_536;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

struct Bench {
    pmu: Option<pmc::Pmu>,
    filter: String,
}
impl Bench {
    /// `body` runs one round of `ops` operations; the cell is the least of five windows.
    fn cell(&mut self, name: &str, ops: usize, rounds: usize, mut body: impl FnMut() -> u64) {
        if !name.contains(&self.filter) {
            return;
        }
        let mut sum = 0u64;
        for _ in 0..2 {
            sum = sum.wrapping_add(body());
        }
        let Some(pmu) = &self.pmu else {
            println!("{name:58} (no pmu) {sum}");
            return;
        };
        let mut best = [u64::MAX; 4];
        for _ in 0..5 {
            let before = pmu.read().expect("read");
            for _ in 0..rounds {
                sum = sum.wrapping_add(body());
            }
            let after = pmu.read().expect("read");
            for i in 0..4 {
                best[i] = best[i].min(after[i].wrapping_sub(before[i]));
            }
        }
        let per = |i: usize| best[i] as f64 / (rounds * ops) as f64;
        println!(
            "{name:58} ins {:9.1}  cyc {:8.1}  miss {:6.3}  br {:7.1}  # {sum}",
            per(1),
            per(0),
            per(2),
            per(3)
        );
    }
}

fn load(path: &str, skip: usize, keep: impl Fn(&[u32]) -> bool, cap: usize) -> Vec<Vec<u32>> {
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{path}: {e}"));
    let mut out = Vec::new();
    for line in text.lines() {
        let s: Vec<u32> = line
            .split_whitespace()
            .skip(skip)
            .map(|x| x.parse().unwrap())
            .collect();
        if !s.is_empty() && keep(&s) {
            out.push(s);
            if out.len() == cap {
                break;
            }
        }
    }
    out
}

/// Images of one form, concatenated; `at[i]..at[i + 1]` is set i's.
struct Images {
    bytes: Vec<u8>,
    at: Vec<usize>,
}
impl Images {
    fn build(sets: &[Vec<u32>], mut write: impl FnMut(&[u32], &mut Vec<u8>)) -> Self {
        let mut bytes = Vec::new();
        let mut at = vec![0];
        for s in sets {
            write(s, &mut bytes);
            at.push(bytes.len());
        }
        Self { bytes, at }
    }
    #[inline(always)]
    fn get(&self, i: usize) -> &[u8] {
        &self.bytes[self.at[i]..self.at[i + 1]]
    }
    fn mean(&self) -> f64 {
        self.bytes.len() as f64 / (self.at.len() - 1) as f64
    }
}

/// (set index, probe value, expected rank, expected membership, a rank to select)
type Probe = (u32, u32, u32, bool, u32);
fn probes(sets: &[Vec<u32>], universe: u32, rng: &mut Rng) -> Vec<Probe> {
    (0..PROBES)
        .map(|_| {
            let i = rng.below(sets.len() as u64) as usize;
            let s = &sets[i];
            let v = if rng.next() & 1 == 0 {
                s[rng.below(s.len() as u64) as usize]
            } else {
                rng.below(u64::from(universe)) as u32
            };
            let r = s.partition_point(|&x| x < v);
            (
                i as u32,
                v,
                r as u32,
                s.get(r) == Some(&v),
                rng.below(s.len() as u64) as u32,
            )
        })
        .collect()
}

/// The point reads of one form over one workload: contains, rank, select, with their answers checked first.
#[allow(clippy::too_many_arguments)]
fn reads(
    b: &mut Bench,
    label: &str,
    ps: &[Probe],
    contains: impl Fn(usize, u32) -> bool,
    rank: impl Fn(usize, u32) -> u32,
    select: impl Fn(usize, u32) -> u32,
    sets: &[Vec<u32>],
) {
    for &(i, v, r, hit, k) in ps {
        assert_eq!(
            contains(i as usize, v),
            hit,
            "{label} contains set {i} value {v}"
        );
        assert_eq!(rank(i as usize, v), r, "{label} rank set {i} value {v}");
        assert_eq!(
            select(i as usize, k),
            sets[i as usize][k as usize],
            "{label} select set {i} rank {k}"
        );
    }
    b.cell(&format!("{label} contains"), PROBES, 20, || {
        ps.iter()
            .map(|&(i, v, ..)| u64::from(contains(black_box(i as usize), v)))
            .sum()
    });
    b.cell(&format!("{label} rank"), PROBES, 20, || {
        ps.iter()
            .map(|&(i, v, ..)| u64::from(rank(black_box(i as usize), v)))
            .sum()
    });
    b.cell(&format!("{label} select"), PROBES, 20, || {
        ps.iter()
            .map(|&(i, .., k)| u64::from(select(black_box(i as usize), k)))
            .sum()
    });
}

fn walk_cell(
    b: &mut Bench,
    label: &str,
    sets: &[Vec<u32>],
    walk: impl Fn(usize, &mut dyn FnMut(u32)),
) {
    let members: usize = sets.iter().map(Vec::len).sum();
    for (i, s) in sets.iter().enumerate() {
        let mut got = Vec::new();
        walk(i, &mut |v| got.push(v));
        assert_eq!(&got, s, "{label} walk set {i}");
    }
    let rounds = (4_000_000 / members).max(1);
    b.cell(
        &format!("{label} walk (per member)"),
        members,
        rounds,
        || {
            let mut sum = 0u64;
            for i in 0..sets.len() {
                walk(black_box(i), &mut |v| sum = sum.wrapping_add(u64::from(v)));
            }
            sum
        },
    );
}

fn crate_images(sets: &[Vec<u32>], forest: bool) -> Images {
    Images::build(sets, |s, out| {
        let set = cr::Bitmosaic::from_sorted(s.iter().copied());
        out.extend_from_slice(&if forest {
            set.to_forest_bytes()
        } else {
            set.to_bytes()
        });
    })
}
#[cfg(feature = "small-forms")]
fn v1_images(sets: &[Vec<u32>]) -> Images {
    Images::build(sets, |s, out| {
        out.extend_from_slice(&v1::Bitmosaic::from_sorted(s.iter().copied()).to_bytes())
    })
}

/// The crate's reader on the same probes (views opened once, outside the window: generous to it).
fn crate_reads(
    b: &mut Bench,
    label: &str,
    sets: &[Vec<u32>],
    ps: &[Probe],
    forest: bool,
    walks: bool,
) {
    let imgs = crate_images(sets, forest);
    let views: Vec<cr::BitmosaicView> = (0..sets.len())
        .map(|i| cr::BitmosaicView::open(imgs.get(i)).unwrap())
        .collect();
    println!("# {label}: {:.2} B a set", imgs.mean());
    reads(
        b,
        label,
        ps,
        |i, v| views[i].contains(v),
        |i, v| views[i].rank(v) as u32,
        |i, k| views[i].select(u64::from(k)).unwrap(),
        sets,
    );
    if walks {
        walk_cell(b, label, sets, |i, f| views[i].for_each(f));
    }
}
#[cfg(feature = "small-forms")]
fn v1_reads(b: &mut Bench, label: &str, sets: &[Vec<u32>], ps: &[Probe]) {
    let imgs = v1_images(sets);
    let views: Vec<v1::BitmosaicView> = (0..sets.len())
        .map(|i| v1::BitmosaicView::open(imgs.get(i)).unwrap())
        .collect();
    println!("# {label}: {:.2} B a set", imgs.mean());
    reads(
        b,
        label,
        ps,
        |i, v| views[i].contains(v),
        |i, v| views[i].rank(v) as u32,
        |i, k| views[i].select(u64::from(k)).unwrap(),
        sets,
    );
    walk_cell(b, label, sets, |i, f| views[i].for_each(f));
}

fn null_cell(b: &mut Bench, label: &str, ps: &[Probe]) {
    b.cell(
        &format!("{label} (the probe loop alone)"),
        PROBES,
        20,
        || {
            ps.iter()
                .map(|&(i, v, ..)| u64::from(black_box(i)) + u64::from(v))
                .sum()
        },
    );
}

fn blocks_cells(b: &mut Bench, label: &str, sets: &[Vec<u32>], ps: &[Probe]) -> Images {
    let imgs = Images::build(sets, blocks_write);
    for (i, s) in sets.iter().enumerate() {
        assert_eq!(imgs.get(i).len(), blocks_len(s), "blocks length formula");
    }
    println!("# {label}: {:.2} B a set", imgs.mean());
    reads(
        b,
        label,
        ps,
        |i, v| Blocks::open(imgs.get(i)).rank_holds(v).1,
        |i, v| Blocks::open(imgs.get(i)).rank_holds(v).0,
        |i, k| Blocks::open(imgs.get(i)).select(k),
        sets,
    );
    walk_cell(b, label, sets, |i, f| Blocks::open(imgs.get(i)).walk(f));
    imgs
}

fn tiny(b: &mut Bench, name: &str, sets: &[Vec<u32>], universe: u32, rng: &mut Rng) {
    println!(
        "\n## {name}: {} sets, {:.1} members a set",
        sets.len(),
        sets.iter().map(Vec::len).sum::<usize>() as f64 / sets.len() as f64
    );
    let ps = probes(sets, universe, rng);
    null_cell(b, name, &ps);
    if sets.iter().all(|s| s.len() <= 8) {
        let imgs = Images::build(sets, list_write);
        for (i, s) in sets.iter().enumerate() {
            assert_eq!(imgs.get(i).len(), list_len(s).unwrap());
        }
        println!("# {name} List: {:.2} B a set", imgs.mean());
        let label = format!("{name} List");
        reads(
            b,
            &label,
            &ps,
            |i, v| list_contains(imgs.get(i), v),
            |i, v| list_rank(imgs.get(i), v),
            |i, k| list_select(imgs.get(i), k),
            sets,
        );
        walk_cell(b, &label, sets, |i, f| list_walk(imgs.get(i), f));
        let mut out = Vec::with_capacity(64);
        b.cell(&format!("{label} write (per set)"), sets.len(), 20, || {
            let mut sum = 0u64;
            for s in sets {
                out.clear();
                list_write(black_box(s), &mut out);
                sum += out.len() as u64;
            }
            sum
        });
    }
    let multi: Vec<Vec<u32>> = sets.iter().filter(|s| s.len() >= 2).cloned().collect();
    if !multi.is_empty() {
        let ps2 = probes(&multi, universe, rng);
        let label = format!("{name} Blocks (sets of 2 or more: {})", multi.len());
        blocks_cells(b, &label, &multi, &ps2);
        let mut out = Vec::with_capacity(1024);
        b.cell(&format!("{label} write (per set)"), multi.len(), 20, || {
            let mut sum = 0u64;
            for s in &multi {
                out.clear();
                blocks_write(black_box(s), &mut out);
                sum += out.len() as u64;
            }
            sum
        });
    }
    crate_reads(b, &format!("{name} crate sealed"), sets, &ps, false, true);
    #[cfg(feature = "small-forms")]
    {
        v1_reads(b, &format!("{name} main v1"), sets, &ps);
        let mut out = [0u8; 4096];
        let mut scratch = cr::PatchScratch::try_new().unwrap();
        b.cell(
            &format!("{name} crate write_image_into (per set)"),
            sets.len(),
            20,
            || {
                sets.iter()
                    .map(|s| {
                        cr::write_image_into(black_box(s), &mut out, &mut scratch).unwrap() as u64
                    })
                    .sum()
            },
        );
    }
}

fn frames(b: &mut Bench, sets: &[Vec<u32>], rng: &mut Rng) {
    let name = "frames";
    println!(
        "\n## {name}: {} sets, {:.1} members a set",
        sets.len(),
        sets.iter().map(Vec::len).sum::<usize>() as f64 / sets.len() as f64
    );
    let ps = probes(sets, 96, rng);
    null_cell(b, name, &ps);
    let imgs = Images::build(sets, bitmap_write);
    println!("# {name} Bitmap: {:.2} B a set", imgs.mean());
    let label = format!("{name} Bitmap");
    reads(
        b,
        &label,
        &ps,
        |i, v| bitmap_contains(imgs.get(i), v),
        |i, v| bitmap_rank(imgs.get(i), v),
        |i, k| bitmap_select(imgs.get(i), k),
        sets,
    );
    walk_cell(b, &label, sets, |i, f| bitmap_walk(imgs.get(i), f));
    for (i, s) in sets.iter().enumerate() {
        assert_eq!(bitmap_count(imgs.get(i)) as usize, s.len());
    }
    b.cell(&format!("{label} len"), PROBES, 20, || {
        ps.iter()
            .map(|&(i, ..)| u64::from(bitmap_count(imgs.get(black_box(i as usize)))))
            .sum()
    });
    b.cell(&format!("{label} and_len (per pair)"), PROBES, 20, || {
        ps.iter()
            .map(|&(i, .., k)| {
                u64::from(bitmap_and_len(
                    imgs.get(i as usize),
                    imgs.get((i + k) as usize % sets.len()),
                ))
            })
            .sum()
    });
    let mut out = Vec::with_capacity(256);
    b.cell(&format!("{label} write (per set)"), sets.len(), 50, || {
        let mut sum = 0u64;
        for s in sets {
            out.clear();
            bitmap_write(black_box(s), &mut out);
            sum += out.len() as u64;
        }
        sum
    });
    // patch: add one absent value and remove it again, with the witness, in a 136-byte slot
    let mut slots: Vec<[u8; 136]> = sets
        .iter()
        .map(|s| {
            let mut slot = [0u8; 136];
            let mut v = Vec::new();
            bitmap_write(s, &mut v);
            slot[..v.len()].copy_from_slice(&v);
            slot
        })
        .collect();
    let absent: Vec<(u32, u32)> = ps.iter().filter(|p| !p.3).map(|p| (p.0, p.1)).collect();
    b.cell(
        &format!("{label} patch: add one, remove it (per pair)"),
        absent.len(),
        20,
        || {
            let mut sum = 0u64;
            for &(i, v) in &absent {
                let slot = &mut slots[i as usize];
                sum += u64::from(bitmap_add(slot, v)) + u64::from(bitmap_remove(slot, v));
            }
            sum
        },
    );
    for (slot, s) in slots.iter().zip(sets) {
        let mut v = Vec::new();
        bitmap_write(s, &mut v);
        assert_eq!(
            &slot[..v.len()],
            &v[..],
            "a patched slot is the written image again"
        );
    }
    crate_reads(b, &format!("{name} crate sealed"), sets, &ps, false, true);
    #[cfg(feature = "small-forms")]
    v1_reads(b, &format!("{name} main v1"), sets, &ps);
    let simgs = crate_images(sets, false);
    let views: Vec<cr::BitmosaicView> = (0..sets.len())
        .map(|i| cr::BitmosaicView::open(simgs.get(i)).unwrap())
        .collect();
    b.cell(
        &format!("{name} crate and_len (per pair)"),
        PROBES,
        20,
        || {
            ps.iter()
                .map(|&(i, .., k)| views[i as usize].and_len(&views[(i + k) as usize % sets.len()]))
                .sum()
        },
    );
    let mut scratch = cr::PatchScratch::try_new().unwrap();
    let mut sslots: Vec<[u8; 136]> = (0..sets.len())
        .map(|i| {
            let mut slot = [0u8; 136];
            let img = cr::Bitmosaic::from_sorted(sets[i].iter().copied()).to_forest_bytes();
            slot[..img.len()].copy_from_slice(&img);
            slot
        })
        .collect();
    b.cell(
        &format!("{name} crate patch: add one, remove it (per pair)"),
        absent.len(),
        5,
        || {
            let mut sum = 0u64;
            for &(i, v) in &absent {
                let slot = &mut sslots[i as usize];
                sum += cr::patch(slot, &[v], &[], &mut scratch).unwrap().len as u64;
                sum += cr::patch(slot, &[], &[v], &mut scratch).unwrap().len as u64;
            }
            sum
        },
    );
    #[cfg(feature = "small-forms")]
    {
        let mut out = [0u8; 4096];
        b.cell(
            &format!("{name} crate write_image_into (per set)"),
            sets.len(),
            50,
            || {
                sets.iter()
                    .map(|s| {
                        cr::write_image_into(black_box(s), &mut out, &mut scratch).unwrap() as u64
                    })
                    .sum()
            },
        );
    }
}

fn runs(b: &mut Bench, name: &str, sets: &[Vec<u32>], universe: u32, rng: &mut Rng) {
    println!(
        "\n## {name}: {} sets, {:.1} members and {:.1} runs a set",
        sets.len(),
        sets.iter().map(Vec::len).sum::<usize>() as f64 / sets.len() as f64,
        sets.iter().map(|s| runs_of(s).len()).sum::<usize>() as f64 / sets.len() as f64
    );
    let ps = probes(sets, universe, rng);
    null_cell(b, name, &ps);
    let spans = Images::build(sets, |s, out| assert!(spans_write(s, out)));
    let runef = Images::build(sets, |s, out| assert!(runef_write(s, out)));
    println!(
        "# {name} Spans16 with a base: {:.2} B a set; RunEf: {:.2} B a set",
        spans.mean(),
        runef.mean()
    );
    for &(i, v, r, hit, _) in &ps {
        assert_eq!(spans_rank_holds(spans.get(i as usize), v), (r, hit));
        assert_eq!(runef_rank_holds(runef.get(i as usize), v), (r, hit));
        assert_eq!(runef_contains(runef.get(i as usize), v), hit);
    }
    b.cell(
        &format!("{name} Spans contains+rank (no prefix counts)"),
        PROBES,
        20,
        || {
            ps.iter()
                .map(|&(i, v, ..)| {
                    u64::from(spans_rank_holds(spans.get(black_box(i as usize)), v).0)
                })
                .sum()
        },
    );
    b.cell(
        &format!("{name} RunEf rank (every boundary read)"),
        PROBES,
        20,
        || {
            ps.iter()
                .map(|&(i, v, ..)| {
                    u64::from(runef_rank_holds(runef.get(black_box(i as usize)), v).0)
                })
                .sum()
        },
    );
    b.cell(
        &format!("{name} RunEf contains (boundary parity)"),
        PROBES,
        20,
        || {
            ps.iter()
                .map(|&(i, v, ..)| u64::from(runef_contains(runef.get(black_box(i as usize)), v)))
                .sum()
        },
    );
    if sets.iter().all(|s| bitmap_len(s).is_some()) {
        let imgs = Images::build(sets, bitmap_write);
        println!("# {name} Bitmap: {:.2} B a set", imgs.mean());
        reads(
            b,
            &format!("{name} Bitmap"),
            &ps,
            |i, v| bitmap_contains(imgs.get(i), v),
            |i, v| bitmap_rank(imgs.get(i), v),
            |i, k| bitmap_select(imgs.get(i), k),
            sets,
        );
    }
    let multi: Vec<Vec<u32>> = sets.iter().filter(|s| s.len() >= 2).cloned().collect();
    let ps2 = probes(&multi, universe, rng);
    blocks_cells(b, &format!("{name} Blocks"), &multi, &ps2);
    crate_reads(b, &format!("{name} crate sealed"), sets, &ps, false, false);
}

fn large(
    b: &mut Bench,
    name: &str,
    sets: &[Vec<u32>],
    small: &[Vec<u32>],
    universe: u32,
    rng: &mut Rng,
) {
    println!(
        "\n## {name}: {} sets, {:.1} members a set",
        sets.len(),
        sets.iter().map(Vec::len).sum::<usize>() as f64 / sets.len() as f64
    );
    let ps = probes(sets, universe, rng);
    null_cell(b, name, &ps);
    let blocks = blocks_cells(b, &format!("{name} Blocks"), sets, &ps);
    crate_reads(b, &format!("{name} crate sealed"), sets, &ps, false, true);
    crate_reads(b, &format!("{name} crate directory"), sets, &ps, true, true);
    // seek: the least member at or above a probe
    let sealed = crate_images(sets, false);
    let forest = crate_images(sets, true);
    let sviews: Vec<cr::BitmosaicView> = (0..sets.len())
        .map(|i| cr::BitmosaicView::open(sealed.get(i)).unwrap())
        .collect();
    let fviews: Vec<cr::BitmosaicView> = (0..sets.len())
        .map(|i| cr::BitmosaicView::open(forest.get(i)).unwrap())
        .collect();
    for &(i, v, r, ..) in &ps {
        let want = sets[i as usize].get(r as usize).copied();
        assert_eq!(
            Blocks::open(blocks.get(i as usize)).seek(v).map(|x| x.0),
            want
        );
    }
    b.cell(&format!("{name} Blocks seek"), PROBES, 20, || {
        ps.iter()
            .map(|&(i, v, ..)| {
                u64::from(
                    Blocks::open(blocks.get(black_box(i as usize)))
                        .seek(v)
                        .map_or(0, |x| x.0),
                )
            })
            .sum()
    });
    for (label, views) in [("crate sealed", &sviews), ("crate directory", &fviews)] {
        b.cell(
            &format!("{name} {label} seek (a fresh cursor)"),
            PROBES,
            20,
            || {
                ps.iter()
                    .map(|&(i, v, ..)| {
                        let mut c = views[black_box(i as usize)].range();
                        c.seek(v);
                        u64::from(c.next().unwrap_or(0))
                    })
                    .sum()
            },
        );
    }
    // intersections: pairs of large sets, and a small list against a large set
    let pairs: Vec<(usize, usize)> = (0..256)
        .map(|_| {
            (
                rng.below(sets.len() as u64) as usize,
                rng.below(sets.len() as u64) as usize,
            )
        })
        .collect();
    let truth =
        |a: &[u32], c: &[u32]| a.iter().filter(|v| c.binary_search(v).is_ok()).count() as u64;
    let members: usize = pairs
        .iter()
        .map(|&(x, y)| sets[x].len().min(sets[y].len()))
        .sum();
    for &(x, y) in &pairs {
        let want = truth(&sets[x], &sets[y]);
        assert_eq!(
            u64::from(Blocks::open(blocks.get(x)).and_len(&Blocks::open(blocks.get(y)))),
            want
        );
        assert_eq!(sviews[x].and_len(&sviews[y]), want);
    }
    println!(
        "# {name} large x large: 256 pairs, {} members in the smaller sides",
        members
    );
    b.cell(
        &format!("{name} Blocks and_len large x large (per smaller-side member)"),
        members,
        3,
        || {
            pairs
                .iter()
                .map(|&(x, y)| {
                    u64::from(Blocks::open(blocks.get(x)).and_len(&Blocks::open(blocks.get(y))))
                })
                .sum()
        },
    );
    b.cell(
        &format!("{name} crate sealed and_len large x large"),
        members,
        3,
        || {
            pairs
                .iter()
                .map(|&(x, y)| sviews[x].and_len(&sviews[y]))
                .sum()
        },
    );
    b.cell(
        &format!("{name} crate directory and_len large x large"),
        members,
        3,
        || {
            pairs
                .iter()
                .map(|&(x, y)| fviews[x].and_len(&fviews[y]))
                .sum()
        },
    );
    let small_imgs = Images::build(small, list_write);
    let sp: Vec<(usize, usize)> = (0..PROBES)
        .map(|_| {
            (
                rng.below(small.len() as u64) as usize,
                rng.below(sets.len() as u64) as usize,
            )
        })
        .collect();
    let smembers: usize = sp.iter().map(|&(x, _)| small[x].len()).sum();
    b.cell(
        &format!("{name} List x Blocks and_len (per list member)"),
        smembers,
        5,
        || {
            let mut sum = 0u64;
            for &(x, y) in &sp {
                let big = Blocks::open(blocks.get(y));
                list_walk(small_imgs.get(x), |v| sum += u64::from(big.rank_holds(v).1));
            }
            sum
        },
    );
    let small_crate = crate_images(small, false);
    let small_views: Vec<cr::BitmosaicView> = (0..small.len())
        .map(|i| cr::BitmosaicView::open(small_crate.get(i)).unwrap())
        .collect();
    for (label, views) in [("crate sealed", &sviews), ("crate directory", &fviews)] {
        b.cell(
            &format!("{name} crate small x {label} and_len (per list member)"),
            smembers,
            5,
            || {
                sp.iter()
                    .map(|&(x, y)| small_views[x].and_len(&views[y]))
                    .sum()
            },
        );
    }
}

fn main() {
    let dir = std::env::args().nth(1).expect("data dir");
    let filter = std::env::args().nth(2).unwrap_or_default();
    let pmu = pmc::Pmu::open(EVENTS)
        .map_err(|e| println!("# no pmu: {e}"))
        .ok();
    let mut b = Bench { pmu, filter };
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let post = format!("{dir}/repo-postings.txt");
    let rows = 1 + load(&post, 0, |_| true, usize::MAX)
        .iter()
        .map(|s| s[s.len() - 1])
        .max()
        .unwrap_or(0);
    let tail = load(&post, 0, |s| s.len() <= 3, 20_000);
    tiny(&mut b, "postings 1-3", &tail, rows, &mut rng);
    tiny(
        &mut b,
        "postings 4-8",
        &load(&post, 0, |s| (4..=8).contains(&s.len()), 20_000),
        rows,
        &mut rng,
    );
    tiny(
        &mut b,
        "postings 9-32",
        &load(&post, 0, |s| (9..=32).contains(&s.len()), 20_000),
        rows,
        &mut rng,
    );
    tiny(
        &mut b,
        "postings 33-128",
        &load(&post, 0, |s| (33..=128).contains(&s.len()), 20_000),
        rows,
        &mut rng,
    );
    frames(
        &mut b,
        &load(&format!("{dir}/frames.txt"), 0, |_| true, usize::MAX),
        &mut rng,
    );
    runs(
        &mut b,
        "runs (present sets, best order)",
        &load(&format!("{dir}/present-best.txt"), 0, |_| true, usize::MAX),
        971,
        &mut rng,
    );
    runs(
        &mut b,
        "runs (actor planes, synthetic)",
        &load(&format!("{dir}/actor-planes.txt"), 0, |_| true, usize::MAX),
        320,
        &mut rng,
    );
    let big = load(&post, 0, |s| s.len() > 256, usize::MAX);
    large(&mut b, "postings >256 (text)", &big, &tail, rows, &mut rng);
    let parity = load(&format!("{dir}/postings-u32.txt"), 1, |_| true, usize::MAX);
    large(
        &mut b,
        "postings (parity corpus, 54k)",
        &parity,
        &tail,
        54_000,
        &mut rng,
    );
    let sparse: Vec<Vec<u32>> = load(&format!("{dir}/sparse-1e6.txt"), 0, |_| true, usize::MAX);
    large(
        &mut b,
        "sparse 1e6 of 2^32 (synthetic)",
        &sparse,
        &tail,
        u32::MAX,
        &mut rng,
    );
}
