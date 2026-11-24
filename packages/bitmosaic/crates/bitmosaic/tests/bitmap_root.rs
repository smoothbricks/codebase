//! Format 2's Bitmap root (specs/bitmosaic/01-format.md §4), held to the spec's vectors
//! (specs/bitmosaic/vectors/format-2.json), to the laws that speak of the form (specs/bitmosaic/laws) and to
//! its refusals (§11).

use std::collections::BTreeSet;

use bitmosaic::{
    AdmissionClass, BitmapView, Bitmosaic, Image, bitmap_len, bitmap_seal, bitmap_set,
};
use serde_json::Value;

const CORPUS: &str = include_str!("../../../../../specs/bitmosaic/vectors/format-2.json");

fn corpus() -> Value {
    serde_json::from_str(CORPUS).expect("the corpus is JSON")
}

fn hex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&text[at..at + 2], 16).expect("hex"))
        .collect()
}

fn members(row: &Value) -> Vec<u32> {
    row["members"]
        .as_array()
        .expect("members")
        .iter()
        .map(|v| u32::try_from(v.as_u64().expect("a member")).expect("a u32 member"))
        .collect()
}

/// The Bitmap of `set`, written the way a caller that holds no slice of members writes it.
fn written(set: &[u32]) -> Vec<u8> {
    let Some(&last) = set.last() else {
        return vec![0x80];
    };
    let mut image = vec![0u8; bitmap_len(last)];
    for &v in set {
        assert!(bitmap_set(&mut image, v), "member {v} fits");
    }
    assert_eq!(bitmap_seal(&mut image, last), Some(image.len()));
    image
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

/// Sets below 1,016 of every density, with the bytes a caller's buffer holds after the image.
fn sets(count: usize) -> Vec<Vec<u32>> {
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    (0..count)
        .map(|_| {
            let top = 1 + rng.next() % 1016;
            let keep = 1 + rng.next() % 16;
            let set: BTreeSet<u32> = (0..top)
                .filter(|_| rng.next() % 16 < keep)
                .map(|v| v as u32)
                .collect();
            set.into_iter().collect()
        })
        .filter(|set: &Vec<u32>| !set.is_empty())
        .collect()
}

//#region smoo/bitmosaic!n/bitmap-root
#[test]
fn every_bitmap_vector_is_written_and_read_back_byte_for_byte() {
    let corpus = corpus();
    let mut bitmaps = 0;
    for row in corpus["u32"].as_array().expect("u32 rows") {
        if row["form"] != "Bitmap" {
            continue;
        }
        bitmaps += 1;
        let set = members(row);
        let sealed = hex(row["sealed"].as_str().expect("sealed"));
        assert_eq!(written(&set), sealed, "the writer's bytes for {set:?}");
        let view = BitmapView::open(&sealed).expect("a vector is admitted");
        assert_eq!(view.extent(), sealed.len());
        assert_eq!(view.bytes(), &sealed[1..]);
        assert_eq!(view.iter().collect::<Vec<_>>(), set);
        assert_eq!(view.len() as usize, set.len());
        assert_eq!(view.is_empty(), set.is_empty());
    }
    assert!(bitmaps >= 6, "the corpus holds the Bitmap rows of spec §14");
}

#[test]
fn a_view_over_a_longer_slice_reads_only_its_image() {
    for set in sets(400) {
        let image = written(&set);
        let mut longer = image.clone();
        longer.extend_from_slice(&[0xFF; 11]);
        let (exact, long) = (
            BitmapView::open(&image).expect("admitted"),
            BitmapView::open(&longer).expect("admitted with bytes after it"),
        );
        assert_eq!(long.extent(), image.len());
        assert_eq!(long.bytes(), exact.bytes());
        assert_eq!(long.len(), exact.len());
        assert_eq!(long.iter().collect::<Vec<_>>(), set);
        for i in 0..18 {
            assert_eq!(long.word(i), exact.word(i), "word {i}");
        }
        for v in 0..1100 {
            assert_eq!(
                long.contains(v),
                set.binary_search(&v).is_ok(),
                "contains {v}"
            );
            assert_eq!(
                long.rank(v) as usize,
                set.partition_point(|&m| m < v),
                "rank {v}"
            );
        }
    }
}

#[test]
fn the_writer_refuses_what_the_form_cannot_hold() {
    let mut image = [0u8; 128];
    assert!(bitmap_set(&mut image, 1015));
    assert_eq!(bitmap_seal(&mut image, 1015), Some(128));
    assert_eq!(image[0], 0xFF);
    assert!(
        !bitmap_set(&mut image, 1016),
        "member 1,016 is past the form"
    );
    assert_eq!(bitmap_seal(&mut image, 1016), None);
    let mut short = [0u8; 2];
    assert!(bitmap_set(&mut short, 7));
    assert!(!bitmap_set(&mut short, 8), "its byte is past the buffer");
    assert_eq!(
        bitmap_seal(&mut short, 8),
        None,
        "the image is past the buffer"
    );
    assert_eq!(bitmap_seal(&mut short, 7), Some(2));
    assert_eq!(short, [0x81, 0x80]);
    let mut zero = [0u8; 4];
    assert!(bitmap_set(&mut zero, 3));
    assert_eq!(
        bitmap_seal(&mut zero, 20),
        None,
        "a seal never names a bitmap that ends in a zero byte"
    );
    assert_eq!(zero[0], 0, "a refused seal writes nothing");
}
//#endregion smoo/bitmosaic!n/bitmap-root

//#region smoo/bitmosaic!n/bitmap-direct-entry
#[test]
fn the_direct_entry_refuses_by_class_and_byte() {
    let refusal = |bytes: &[u8]| {
        let e = BitmapView::open(bytes).expect_err("refused");
        (e.class, e.offset)
    };
    assert_eq!(refusal(&[]), (AdmissionClass::Extent, 0));
    assert_eq!(
        refusal(&[0x20, 0x05]),
        (AdmissionClass::Kind, 0),
        "a Single is not a Bitmap"
    );
    assert_eq!(
        refusal(&[0x42, 0x4D, 0x53, 0x0C]),
        (AdmissionClass::Kind, 0),
        "nor is a format-1 image"
    );
    assert_eq!(
        refusal(&[0x81]),
        (AdmissionClass::Extent, 0),
        "one byte named, none present"
    );
    assert_eq!(refusal(&[0x83, 0x01, 0x01]), (AdmissionClass::Extent, 0));
    assert_eq!(
        refusal(&[0x81, 0x00]),
        (AdmissionClass::Extent, 1),
        "a last byte of zero"
    );
    assert_eq!(refusal(&[0x82, 0x01, 0x00]), (AdmissionClass::Extent, 2));
    assert!(BitmapView::open(&[0x80]).expect("the empty set").is_empty());
    assert_eq!(
        BitmapView::open(&[0x80, 0x00])
            .expect("bytes after it are not its own")
            .extent(),
        1
    );
}

#[test]
fn every_bitmap_refusal_of_the_corpus_is_refused_with_its_class() {
    let corpus = corpus();
    let mut seen = 0;
    for row in corpus["rejects"].as_array().expect("rejects") {
        let bytes = hex(row["bytes"].as_str().expect("bytes"));
        if bytes[0] < 0x80 {
            continue;
        }
        seen += 1;
        let e = BitmapView::open(&bytes).expect_err("a reject is refused");
        assert_eq!(
            e.class.as_str(),
            row["class"].as_str().expect("class"),
            "{bytes:02X?}"
        );
        let general = Image::open(&bytes).expect_err("and by the general entry");
        assert_eq!((general.class, general.offset), (e.class, e.offset));
    }
    assert_eq!(seen, 3, "the corpus's three Bitmap refusals");
}

#[test]
fn bytes_a_pass_admitted_open_without_the_last_byte_rule() {
    assert!(BitmapView::open_admitted(&[]).is_none());
    assert!(BitmapView::open_admitted(&[0x20, 0x05]).is_none());
    assert!(
        BitmapView::open_admitted(&[0x82, 0x01]).is_none(),
        "the extent is still checked"
    );
    let view =
        BitmapView::open_admitted(&[0x82, 0x05, 0x00, 0xFF]).expect("control and extent hold");
    assert_eq!(view.iter().collect::<Vec<_>>(), [0, 2]);
    assert_eq!((view.extent(), view.len(), view.rank(900)), (3, 2, 2));
}
//#endregion smoo/bitmosaic!n/bitmap-direct-entry

//#region smoo/bitmosaic!n/control-byte
#[test]
fn the_general_entry_tells_the_format_and_admits_every_format_2_root() {
    let corpus = corpus();
    for row in corpus["u32"].as_array().expect("u32 rows") {
        let sealed = hex(row["sealed"].as_str().expect("sealed"));
        let form = row["form"].as_str().expect("form");
        let want = members(row);
        match Image::open(&sealed).expect("every format-2 corpus row is admitted") {
            Image::Bitmap(view) => {
                assert_eq!(form, "Bitmap");
                assert_eq!(view.iter().collect::<Vec<_>>(), want);
            }
            Image::Single(view) => {
                assert_eq!(form, "Single");
                assert_eq!(view.range().collect::<Vec<_>>(), want);
            }
            Image::List(view) => {
                assert_eq!(form, "List");
                assert_eq!(view.range().collect::<Vec<_>>(), want);
            }
            Image::EliasFano(view) => {
                assert_eq!(form, "Elias-Fano");
                assert_eq!(view.range().collect::<Vec<_>>(), want);
            }
            Image::Directory(view) => {
                assert_eq!(form, "Directory");
                assert_eq!(view.range().collect::<Vec<_>>(), want);
            }
            other => panic!("{form} read as {other:?}"),
        }
    }
    let kind = |bytes: &[u8]| {
        let e = Image::open(bytes).expect_err("refused");
        assert_eq!((e.class, e.offset), (AdmissionClass::Kind, 0));
        e.what
    };
    assert!(kind(&[0x00]).contains("no image"));
    assert!(kind(&[0x41]).contains("later format"));
    assert!(kind(&[0x60, 0x05]).contains("unassigned"));
    assert_eq!(
        Image::open(&[]).expect_err("no byte").class,
        AdmissionClass::Extent
    );
}

#[test]
fn a_format_1_image_is_read_as_before() {
    let set = [5u32, 70_000, 140_000];
    let bytes = Bitmosaic::from_sorted(set).to_bytes();
    assert_eq!(&bytes[..3], b"BMS");
    match Image::open(&bytes).expect("format 1 is admitted") {
        Image::Format1(view) => {
            assert_eq!(view.len(), 3);
            assert!(view.contains(70_000) && !view.contains(6));
        }
        other => panic!("read as {other:?}"),
    }
    let wide = bitmosaic::Bitmosaic64::from_sorted([7u64, 1 << 40]).to_bytes();
    assert!(matches!(Image::open(&wide), Ok(Image::Format1Wide(view)) if view.len() == 2));
    let mut cut = bytes.clone();
    cut.truncate(bytes.len() - 1);
    assert!(Image::open(&cut).is_err(), "format 1's own refusals stand");
}
//#endregion smoo/bitmosaic!n/control-byte

//#region smoo/bitmosaic!n/bitmap-root
/// specs/bitmosaic/laws/one_image.bend, `a_bitmap_reads_back_as_its_set` and
/// `an_admitted_bitmap_is_the_written_one`: a trimmed bitmap is the only bitmap of its set, so the bytes a
/// reader admits are the bytes the writer writes. Every bitmap of one and of two bytes.
#[test]
fn law_one_image_an_admitted_bitmap_is_the_written_one() {
    for bits in 0u32..=0xFFFF {
        let [low, high, ..] = bits.to_le_bytes();
        for image in [vec![0x81, low], vec![0x82, low, high]] {
            let trimmed = image.last() != Some(&0);
            match BitmapView::open(&image) {
                Ok(view) => {
                    assert!(trimmed);
                    let set: Vec<u32> = view.iter().collect();
                    assert_eq!(written(&set), image, "the written image of {set:?}");
                }
                Err(e) => {
                    assert!(!trimmed);
                    assert_eq!(
                        (e.class, e.offset),
                        (AdmissionClass::Extent, image.len() - 1)
                    );
                }
            }
        }
    }
}

/// specs/bitmosaic/laws/bitmap_probe.bend, `whole_words_count_whole` and `a_mask_counts_inside_its_word`:
/// a rank is the population counts of the whole words below the position plus one masked word.
#[test]
fn law_bitmap_probe_rank_is_whole_words_and_one_masked_word() {
    for set in sets(300) {
        let image = written(&set);
        let view = BitmapView::open(&image).expect("admitted");
        for v in 0..=1016u32 {
            let whole: u32 = (0..v as usize / 64)
                .map(|i| view.word(i).count_ones())
                .sum();
            let inside = (view.word(v as usize / 64) & ((1u64 << (v % 64)) - 1)).count_ones();
            assert_eq!(view.rank(v), whole + inside);
            assert_eq!(view.rank(v) as usize, set.partition_point(|&m| m < v));
        }
    }
}

/// specs/bitmosaic/laws/bitmap_probe.bend, `and_holds_what_both_hold` and `and_goes_word_by_word`: the
/// members two bitmaps share are the set bits of their words ANDed, word by word, with no shift.
#[test]
fn law_bitmap_probe_and_goes_word_by_word() {
    let sets = sets(120);
    for pair in sets.windows(2) {
        let (a, b) = (written(&pair[0]), written(&pair[1]));
        let (a, b) = (
            BitmapView::open(&a).expect("a"),
            BitmapView::open(&b).expect("b"),
        );
        let shared: u32 = (0..16).map(|i| (a.word(i) & b.word(i)).count_ones()).sum();
        let both = pair[0]
            .iter()
            .filter(|v| pair[1].binary_search(v).is_ok())
            .count();
        assert_eq!(shared as usize, both);
    }
}

/// specs/bitmosaic/laws/bitmap_probe.bend, `rank_of_select` and `select_is_a_member`: the walk's `k`-th
/// answer is a member with exactly `k` members below it.
#[test]
fn law_bitmap_probe_the_kth_member_has_k_below_it() {
    for set in sets(200) {
        let image = written(&set);
        let view = BitmapView::open(&image).expect("admitted");
        for (k, member) in view.iter().enumerate() {
            assert!(view.contains(member));
            assert_eq!(view.rank(member) as usize, k);
        }
    }
}
//#endregion smoo/bitmosaic!n/bitmap-root
