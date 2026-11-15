//! The conformance vectors of the native format, as one JSON document.
//!
//! Every accepted case is written by the crate, admitted by its verifying
//! reader and read back member for member before it is recorded, and its
//! form (the root, and the container kind of every chunk) is checked
//! against the form the case names, so the document provably holds every
//! kind and both sides of every selection boundary. Every rejected case is
//! refused by the verifying reader with the recorded rule.
//!
//! A u32 case's `members` are ascending; an interval of three or more is
//! written as `[first, last]`, both inclusive. `image` is the sealed image
//! and `slot`, present where it differs, the forest-family image a patched
//! slot holds. u64 members are decimal strings.

use bitmosaic::{
    AdmissionClass, Bitmosaic, Bitmosaic64, Bitmosaic64View, BitmosaicView, EMPTY_U32_IMAGE,
    EMPTY_U64_IMAGE,
};

const WORDS: u8 = 0;
const STRIDE: u8 = 1;
const CONE: u8 = 2;
const ARRAY: u8 = 3;
const RUNS: u8 = 4;
const BYTES: u8 = 5;
const WINDOW: u8 = 6;
const WINDOW_WIDE: u8 = 7;
const KIND_NAMES: [&str; 8] = [
    "words",
    "stride",
    "cone",
    "array",
    "runs",
    "bytes",
    "window",
    "window-wide",
];

/// The forest-family form of a u32 image.
#[derive(Debug, PartialEq, Eq)]
enum Form {
    Empty,
    Lone(u8),
    Directory(Vec<u8>),
    EliasFano,
}

impl Form {
    fn name(&self) -> String {
        let kinds = |kinds: &[u8]| {
            kinds
                .iter()
                .map(|kind| KIND_NAMES[usize::from(*kind)])
                .collect::<Vec<_>>()
                .join(",")
        };
        match self {
            Form::Empty => "empty".to_owned(),
            Form::Lone(kind) => format!("lone:{}", kinds(&[*kind])),
            Form::Directory(chunks) => format!("directory:{}", kinds(chunks)),
            Form::EliasFano => "elias-fano".to_owned(),
        }
    }
}

fn uleb(bytes: &[u8], at: &mut usize) -> usize {
    let (mut value, mut shift) = (0usize, 0);
    loop {
        let byte = bytes[*at];
        *at += 1;
        value |= usize::from(byte & 0x7f) << shift;
        shift += 7;
        if byte & 0x80 == 0 {
            return value;
        }
    }
}

/// Read an image's form from its control byte and, for a directory, its
/// kind plane: the layout the README states, not the crate's reader.
fn form(image: &[u8]) -> Form {
    match image[0] {
        0x14 => Form::Empty,
        0x12 => Form::EliasFano,
        control if control & 0x80 != 0 => Form::Lone(control & 7),
        0x10 => {
            let mut at = 1;
            uleb(image, &mut at);
            uleb(image, &mut at);
            let chunks = uleb(image, &mut at) + 1;
            let kinds_at = at + 2 * chunks + 8 * (chunks - 1);
            Form::Directory(
                (0..chunks)
                    .map(|i| {
                        let bit = 3 * i;
                        let pair = u16::from(image[kinds_at + bit / 8])
                            | u16::from(*image.get(kinds_at + bit / 8 + 1).unwrap_or(&0)) << 8;
                        (pair >> (bit % 8) & 7) as u8
                    })
                    .collect(),
            )
        }
        control => panic!("control {control:#04x} is no u32 image"),
    }
}

/// Ascending members as JSON, intervals of three or more as `[first, last]`.
fn members_json(members: &[u32]) -> String {
    let mut out = Vec::new();
    let mut at = 0;
    for end in 1..=members.len() {
        if end < members.len() && members[end] == members[end - 1] + 1 {
            continue;
        }
        if end - at >= 3 {
            out.push(format!("[{},{}]", members[at], members[end - 1]));
        } else {
            out.extend(members[at..end].iter().map(u32::to_string));
        }
        at = end;
    }
    out.join(",")
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn xorshift(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

/// `n` distinct values below `below`, ascending: a partial shuffle.
fn scatter(n: usize, below: u32, seed: u64) -> Vec<u32> {
    let mut state = seed;
    let mut pool: Vec<u32> = (0..below).collect();
    for i in 0..n {
        let j = i + (xorshift(&mut state) % (u64::from(below) - i as u64)) as usize;
        pool.swap(i, j);
    }
    pool.truncate(n);
    pool.sort_unstable();
    pool
}

fn jittered(n: u32, step: u32) -> Vec<u32> {
    (0..n).map(|i| i * step + (i % 3) * 7).collect()
}

fn triples(runs: u32) -> Vec<u32> {
    (0..runs).flat_map(|i| i * 6..i * 6 + 3).collect()
}

fn in_chunk(key: u32, lows: impl IntoIterator<Item = u32>) -> Vec<u32> {
    lows.into_iter().map(|low| key << 16 | low).collect()
}

/// The u32 cases: a name, the members, and the form the slot image takes.
fn u32_cases() -> Vec<(&'static str, Vec<u32>, Form)> {
    let singletons = |keys: Vec<u32>| {
        keys.into_iter()
            .map(|key| key << 16 | 5)
            .collect::<Vec<_>>()
    };
    // A directory holds only the five arm kinds, whatever its chunks span.
    let every_kind: Vec<u32> = [
        in_chunk(0, [7, 40]),
        in_chunk(1, jittered(100, 500)),
        in_chunk(2, scatter(40, 65_536, 0x5eed_0006)),
        in_chunk(3, (0..4096).chain(32_768..36_864)),
        in_chunk(4, scatter(5_000, 9_000, 0x5eed_0005)),
        in_chunk(5, vec![9]),
    ]
    .concat();
    vec![
        ("empty", vec![], Form::Empty),
        ("singleton-zero", vec![0], Form::Lone(BYTES)),
        ("singleton-127", vec![127], Form::Lone(BYTES)),
        ("singleton-128-ties-array", vec![128], Form::Lone(ARRAY)),
        ("singleton-chunk-end", vec![65_535], Form::Lone(ARRAY)),
        (
            "singleton-second-chunk",
            vec![65_536],
            Form::Directory(vec![ARRAY]),
        ),
        (
            "singleton-max",
            vec![u32::MAX],
            Form::Directory(vec![ARRAY]),
        ),
        ("pair-near", vec![7, 40], Form::Lone(BYTES)),
        ("pair-span-255", vec![0, 255], Form::Lone(BYTES)),
        ("pair-span-256", vec![0, 256], Form::Lone(STRIDE)),
        ("four-of-83", vec![7, 17, 40, 82], Form::Lone(BYTES)),
        (
            "bytes-at-chunk-end",
            vec![65_280, 65_300, 65_535],
            Form::Lone(BYTES),
        ),
        ("four-of-129", vec![0, 1, 2, 10], Form::Lone(WINDOW)),
        (
            "progression-ties-keep-stride",
            vec![200, 300, 400],
            Form::Lone(STRIDE),
        ),
        (
            "progression-shorter-as-bytes",
            vec![0, 100, 200],
            Form::Lone(BYTES),
        ),
        (
            "bytes-window-tie-is-bytes",
            vec![0, 5, 16],
            Form::Lone(BYTES),
        ),
        (
            "window-shorter-than-bytes",
            vec![0, 5, 8],
            Form::Lone(WINDOW),
        ),
        (
            "interval-of-17-is-window",
            (0..17).collect(),
            Form::Lone(WINDOW),
        ),
        (
            "interval-of-18-is-stride",
            (0..18).collect(),
            Form::Lone(STRIDE),
        ),
        (
            "thirteen-of-70",
            vec![1, 3, 9, 14, 20, 25, 33, 40, 47, 55, 60, 66, 69],
            Form::Lone(WINDOW),
        ),
        (
            "window-of-16-bytes",
            (0..=128).filter(|v| v % 3 != 1).collect(),
            Form::Lone(WINDOW),
        ),
        (
            "window-of-17-bytes",
            (0..=136).filter(|v| v % 3 != 1).collect(),
            Form::Lone(WINDOW_WIDE),
        ),
        (
            "window-of-32-bytes",
            (0..=255).filter(|v| v % 3 != 0).collect(),
            Form::Lone(WINDOW_WIDE),
        ),
        ("every-ordinal-of-83", (0..83).collect(), Form::Lone(STRIDE)),
        ("run-of-200", (1_000..1_200).collect(), Form::Lone(STRIDE)),
        ("full-chunk", (0..65_536).collect(), Form::Lone(STRIDE)),
        (
            "progression-wide",
            (0..1_000).map(|i| i * 13).collect(),
            Form::Lone(STRIDE),
        ),
        (
            "array-count-14",
            scatter(14, 65_536, 0x5eed_000e),
            Form::Lone(ARRAY),
        ),
        (
            "array-count-15",
            scatter(15, 65_536, 0x5eed_000f),
            Form::Lone(ARRAY),
        ),
        (
            "array-count-270",
            scatter(270, 65_536, 0x5eed_010e),
            Form::Lone(ARRAY),
        ),
        (
            "array-count-271",
            scatter(271, 65_536, 0x5eed_010f),
            Form::Lone(ARRAY),
        ),
        (
            "array-count-4096",
            scatter(4_096, 65_536, 0x5eed_1000),
            Form::Lone(ARRAY),
        ),
        (
            "words-count-4097",
            scatter(4_097, 8_192, 0x5eed_1001),
            Form::Lone(WORDS),
        ),
        ("runs-1453-beat-words", triples(1_453), Form::Lone(RUNS)),
        ("runs-1454-lose-to-words", triples(1_454), Form::Lone(WORDS)),
        ("cone-ties-array-at-16", jittered(16, 500), Form::Lone(CONE)),
        (
            "array-beats-cone-at-15",
            jittered(15, 500),
            Form::Lone(ARRAY),
        ),
        ("cone-count-1024", jittered(1_024, 60), Form::Lone(CONE)),
        ("no-cone-above-1024", jittered(1_025, 60), Form::Lone(ARRAY)),
        (
            "two-runs",
            (0..4_096).chain(32_768..36_864).collect(),
            Form::Lone(RUNS),
        ),
        (
            "runs-tie-array-is-array",
            vec![0, 1, 2, 1_000, 1_001, 1_002],
            Form::Lone(ARRAY),
        ),
        (
            "runs-beat-array",
            vec![0, 1, 2, 1_000, 1_001, 1_002, 1_003],
            Form::Lone(RUNS),
        ),
        (
            "two-chunks",
            vec![1, 65_545],
            Form::Directory(vec![ARRAY; 2]),
        ),
        (
            "every-kind-in-a-directory",
            every_kind,
            Form::Directory(vec![STRIDE, CONE, ARRAY, RUNS, WORDS, ARRAY]),
        ),
        (
            "64-broken-keys-no-slot-plane",
            singletons((0..63).chain([70]).collect()),
            Form::Directory(vec![ARRAY; 64]),
        ),
        (
            "65-contiguous-keys-no-slot-plane",
            singletons((3..68).collect()),
            Form::Directory(vec![ARRAY; 65]),
        ),
        (
            "65-broken-keys-slot-plane",
            singletons((0..64).chain([70]).collect()),
            Form::Directory(vec![ARRAY; 65]),
        ),
        (
            "sparse-hundred-seals-as-elias-fano",
            scatter(100, 13_000_000, 0x5eed_0064),
            Form::Directory(vec![ARRAY; 0]),
        ),
    ]
}

fn u64_cases() -> Vec<(&'static str, Vec<u64>)> {
    let wide =
        |high: u64, lows: Vec<u32>| lows.into_iter().map(move |low| high << 32 | u64::from(low));
    vec![
        ("empty", vec![]),
        ("singleton-lone-child", vec![7]),
        ("singleton-max", vec![u64::MAX]),
        ("three-highs", vec![1, 1 << 32 | 3, 2 << 32 | 5]),
        (
            "children-of-every-root",
            wide(0, vec![7, 17, 40, 82])
                .chain(wide(9, scatter(100, 13_000_000, 0x5eed_0909)))
                .chain(wide(0xffff_ffff, vec![1, 65_545, 131_094]))
                .collect(),
        ),
    ]
}

fn set_u16(image: &mut [u8], at: usize, value: u16) {
    image[at..at + 2].copy_from_slice(&value.to_le_bytes());
}

/// Images the verifying reader refuses, with the rule each breaks. Offsets
/// follow the README's layouts.
fn rejects() -> Vec<(&'static str, u32, Vec<u8>, AdmissionClass)> {
    let image = |members: &[u32]| Bitmosaic::from_sorted(members.iter().copied()).to_forest_bytes();
    let altered = |members: &[u32], alter: &dyn Fn(&mut Vec<u8>)| {
        let mut bytes = image(members);
        alter(&mut bytes);
        bytes
    };
    let three_chunks = [1, 65_545, 131_094];
    let wide = Bitmosaic64::from_sorted([1, 1 << 32 | 3, 2 << 32 | 5]).to_bytes();
    let wide_altered = |alter: &dyn Fn(&mut Vec<u8>)| {
        let mut bytes = wide.clone();
        alter(&mut bytes);
        bytes
    };
    vec![
        ("no-bytes", 32, vec![], AdmissionClass::Extent),
        (
            "unassigned-control",
            32,
            vec![0x42, 0x4d, 0x53, 0x08],
            AdmissionClass::Kind,
        ),
        (
            "unassigned-root-mode",
            32,
            vec![0x13, 1, 0],
            AdmissionClass::Kind,
        ),
        (
            "u64-image-read-as-u32",
            32,
            EMPTY_U64_IMAGE.to_vec(),
            AdmissionClass::Kind,
        ),
        (
            "u32-image-read-as-u64",
            64,
            EMPTY_U32_IMAGE.to_vec(),
            AdmissionClass::Kind,
        ),
        (
            "lone-window-of-a-length-outside-its-kind",
            32,
            vec![0x80 | 1 << 3 | 7, 0, 0x01, 0x01],
            AdmissionClass::Extent,
        ),
        (
            "lone-array-truncated",
            32,
            vec![0x83, 7],
            AdmissionClass::Extent,
        ),
        (
            "lone-count-byte-missing",
            32,
            vec![0xf3],
            AdmissionClass::Extent,
        ),
        (
            "lone-word-count-not-shortest",
            32,
            altered(&[7], &|bytes| {
                bytes[0] = 0x80 | 15 << 3 | 3;
                bytes.splice(1..1, [0, 0]);
            }),
            AdmissionClass::Count,
        ),
        (
            "directory-of-one-chunk-at-key-zero",
            32,
            vec![0x10, 7, 0, 0, 0, 0, 3, 7, 0],
            AdmissionClass::Kind,
        ),
        (
            "body-length-not-shortest",
            32,
            altered(&three_chunks, &|bytes| {
                bytes[1] |= 0x80;
                bytes.insert(2, 0);
            }),
            AdmissionClass::Extent,
        ),
        (
            "body-truncated",
            32,
            altered(&three_chunks, &|bytes| {
                bytes.pop();
            }),
            AdmissionClass::Extent,
        ),
        (
            "directory-chunk-of-a-small-kind",
            32,
            altered(&three_chunks, &|bytes| bytes[4 + 6 + 16] |= 7),
            AdmissionClass::Kind,
        ),
        (
            "directory-kind-pad-bit",
            32,
            altered(&three_chunks, &|bytes| bytes[4 + 6 + 16 + 1] |= 0x80),
            AdmissionClass::BitPad,
        ),
        (
            "directory-keys-descend",
            32,
            altered(&three_chunks, &|bytes| set_u16(bytes, 4 + 2, 0)),
            AdmissionClass::MapOrder,
        ),
        (
            "directory-offset-past-payloads",
            32,
            altered(&three_chunks, &|bytes| set_u16(bytes, 4 + 6 + 8, 9_999)),
            AdmissionClass::Placement,
        ),
        (
            "directory-prefix-leaves-a-chunk-empty",
            32,
            altered(&three_chunks, &|bytes| bytes[4 + 6] = 0),
            AdmissionClass::Count,
        ),
        (
            "array-members-descend",
            32,
            altered(&[1, 900, 2_200], &|bytes| set_u16(bytes, 3, 1)),
            AdmissionClass::MapOrder,
        ),
        (
            "stride-step-zero",
            32,
            altered(&[2, 400, 798], &|bytes| set_u16(bytes, 3, 0)),
            AdmissionClass::PlaneRange,
        ),
        (
            "stride-leaves-chunk",
            32,
            altered(&[2, 400, 798], &|bytes| set_u16(bytes, 3, 40_000)),
            AdmissionClass::PlaneRange,
        ),
        (
            "bytes-offsets-descend",
            32,
            altered(&[7, 17, 40, 82], &|bytes| bytes[4] = 5),
            AdmissionClass::MapOrder,
        ),
        (
            "bytes-offset-zero",
            32,
            altered(&[7, 17, 40, 82], &|bytes| bytes[2] = 0),
            AdmissionClass::MapOrder,
        ),
        (
            "bytes-member-leaves-chunk",
            32,
            vec![0x80 | 1 << 3 | 5, 0xff, 0xff, 0x03, 1],
            AdmissionClass::PlaneRange,
        ),
        (
            "bytes-truncated",
            32,
            vec![0x80 | 1 << 3 | 5, 7],
            AdmissionClass::Extent,
        ),
        (
            "window-ends-in-empty-byte",
            32,
            vec![0x80 | 1 << 3 | 6, 0, 0x81, 0],
            AdmissionClass::Extent,
        ),
        (
            "window-member-leaves-chunk",
            32,
            vec![0x86, 0xff, 0xff, 0x03, 1],
            AdmissionClass::PlaneRange,
        ),
        (
            "runs-touch",
            32,
            altered(&[0, 1, 2, 1_000, 1_001, 1_002, 1_003], &|bytes| {
                set_u16(bytes, 6, 3)
            }),
            AdmissionClass::MapOrder,
        ),
        (
            "runs-count-differs",
            32,
            altered(&[0, 1, 2, 1_000, 1_001, 1_002, 1_003], &|bytes| {
                bytes[0] += 1 << 3
            }),
            AdmissionClass::Count,
        ),
        (
            "u64-child-is-empty-image",
            64,
            wide_altered(&|bytes| bytes[31] = 0x14),
            AdmissionClass::Kind,
        ),
        (
            "u64-keys-descend",
            64,
            wide_altered(&|bytes| bytes[3] = 2),
            AdmissionClass::MapOrder,
        ),
        (
            "u64-prefix-differs",
            64,
            wide_altered(&|bytes| bytes[15] = 2),
            AdmissionClass::Count,
        ),
        (
            "u64-offset-splits-a-child",
            64,
            wide_altered(&|bytes| bytes[23] = 3),
            AdmissionClass::Extent,
        ),
    ]
}

pub fn document() -> String {
    let mut out = String::from("{\n\"format\": \"bitmosaic native v2\",\n\"u32\": [\n");
    let cases = u32_cases();
    let mut lone_kinds = [false; 8];
    for (i, (name, members, expected)) in cases.iter().enumerate() {
        let set = Bitmosaic::from_sorted(members.iter().copied());
        let (image, slot) = (set.to_bytes(), set.to_forest_bytes());
        for bytes in [&image, &slot] {
            let view = BitmosaicView::open_verified(bytes)
                .unwrap_or_else(|error| panic!("{name}: a written image is refused: {error}"));
            assert_eq!(view.serialized_len(), bytes.len(), "{name}: extent");
            assert!(view.range().eq(members.iter().copied()), "{name}: members");
        }
        let slot_form = form(&slot);
        if *name == "sparse-hundred-seals-as-elias-fano" {
            assert_eq!(form(&image), Form::EliasFano, "{name}");
            assert!(matches!(slot_form, Form::Directory(_)), "{name}");
        } else {
            assert_eq!(&slot_form, expected, "{name}: slot form");
        }
        if *name == "65-broken-keys-slot-plane" {
            assert!(
                BitmosaicView::open(&slot).is_some_and(|view| view.has_slot_plane()),
                "{name}"
            );
        }
        if let Form::Lone(kind) = slot_form {
            lone_kinds[usize::from(kind)] = true;
        }
        let slot_field = if slot == image {
            String::new()
        } else {
            format!("\"slot\": \"{}\", ", hex(&slot))
        };
        out.push_str(&format!(
            "{{\"name\": \"{name}\", \"form\": \"{}\", \"image\": \"{}\", \"slot_form\": \"{}\", \
             {slot_field}\"members\": [{}]}}{}\n",
            form(&image).name(),
            hex(&image),
            slot_form.name(),
            members_json(members),
            if i + 1 == cases.len() { "" } else { "," },
        ));
    }
    assert_eq!(
        lone_kinds, [true; 8],
        "every kind stands alone in some case"
    );

    out.push_str("],\n\"u64\": [\n");
    let cases = u64_cases();
    for (i, (name, members)) in cases.iter().enumerate() {
        let image = Bitmosaic64::from_sorted(members.iter().copied()).to_bytes();
        let view = Bitmosaic64View::open_verified(&image)
            .unwrap_or_else(|error| panic!("{name}: a written image is refused: {error}"));
        assert_eq!(view.serialized_len(), image.len(), "{name}: extent");
        assert!(view.range().eq(members.iter().copied()), "{name}: members");
        let members: Vec<String> = members
            .iter()
            .map(|member| format!("\"{member}\""))
            .collect();
        out.push_str(&format!(
            "{{\"name\": \"{name}\", \"image\": \"{}\", \"members\": [{}]}}{}\n",
            hex(&image),
            members.join(","),
            if i + 1 == cases.len() { "" } else { "," },
        ));
    }

    out.push_str("],\n\"rejects\": [\n");
    let cases = rejects();
    for (i, (name, width, image, class)) in cases.iter().enumerate() {
        let refusal = match width {
            32 => BitmosaicView::open_verified(image).map(|_| ()),
            _ => Bitmosaic64View::open_verified(image).map(|_| ()),
        }
        .expect_err(name);
        assert_eq!(refusal.class, *class, "{name}: {refusal}");
        out.push_str(&format!(
            "{{\"name\": \"{name}\", \"width\": {width}, \"class\": \"{}\", \"image\": \"{}\"}}{}\n",
            class.as_str(),
            hex(image),
            if i + 1 == cases.len() { "" } else { "," },
        ));
    }
    out.push_str("]\n}\n");
    out
}
