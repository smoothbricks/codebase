//! The conformance vectors of the native format, as one JSON document.
//!
//! The generator holds its own writer and reader of the format, written
//! from the README's "Native v2" section and from nothing in the crate: the
//! selection rules (steps 1 to 6) pick each image, the byte builders write
//! it, and a decoder names its form. Every accepted case is the crate's
//! image, byte-equal to this writer's, and its form is the one the case
//! names. The sealed image (`image`) passes `open`, `open_verified` and
//! `open_canonical` and reads back member for member; where the slot image
//! a patch writes (`slot`, `to_forest_bytes`) differs, it passes `open` and
//! `open_verified` and `open_canonical` refuses it as `canonical`.
//!
//! Every rejected case is built by hand and refused with the recorded class
//! from the recorded entry on: `open` (structure; the stronger entries
//! refuse it too), `open_verified` (members and the writer's forms, `open`
//! accepting), or `open_canonical` alone (the sealed root choice).
//!
//! A u32 case's `members` are ascending; an interval of three or more is
//! written as `[first, last]`, both inclusive. u64 members are decimal
//! strings.

use bitmosaic::{AdmissionClass, Bitmosaic, Bitmosaic64, Bitmosaic64View, BitmosaicView};

// ── the format, from the README ─────────────────────────────────────────

const WORDS: u8 = 0;
const STRIDE: u8 = 1;
const CONE: u8 = 2;
const ARRAY: u8 = 3;
const RUNS: u8 = 4;
const KIND_NAMES: [&str; 5] = ["words", "stride", "cone", "array", "runs"];

const CONTROL_DIRECTORY: u8 = 0x10;
const CONTROL_U64: u8 = 0x11;
const CONTROL_ELIAS_FANO: u8 = 0x12;
const CONTROL_EMPTY: u8 = 0x14;
const CONTROL_EMPTY_U64: u8 = 0x15;

/// The small forms, by their `kk` (tag bits 5 to 7).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Small {
    List8 = 1,
    Window = 2,
    WindowAt = 3,
    List16 = 4,
    Spans = 5,
}

impl Small {
    const ALL: [Small; 5] = [
        Small::List8,
        Small::Window,
        Small::WindowAt,
        Small::List16,
        Small::Spans,
    ];

    fn of_tag(tag: u8) -> Option<Small> {
        Small::ALL.into_iter().find(|form| *form as u8 == tag >> 5)
    }

    fn name(self) -> &'static str {
        match self {
            Small::List8 => "list8",
            Small::Window => "window",
            Small::WindowAt => "window-at",
            Small::List16 => "list16",
            Small::Spans => "spans",
        }
    }
}

/// The form of a u32 image.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Form {
    Empty,
    Small(Small),
    Directory(Vec<u8>),
    EliasFano,
}

impl Form {
    fn name(&self) -> String {
        match self {
            Form::Empty => "empty".to_owned(),
            Form::Small(form) => format!("small:{}", form.name()),
            Form::Directory(kinds) => format!(
                "directory:{}",
                kinds
                    .iter()
                    .map(|kind| KIND_NAMES[usize::from(*kind)])
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            Form::EliasFano => "elias-fano".to_owned(),
        }
    }
}

fn put_varint(out: &mut Vec<u8>, value: u64) {
    let mut rest = value;
    for _ in 0..10 {
        if rest < 0x80 {
            out.push(rest as u8);
            return;
        }
        out.push(rest as u8 | 0x80);
        rest >>= 7;
    }
}

fn read_varint(bytes: &[u8], at: &mut usize) -> u64 {
    let mut value = 0;
    for shift in (0..64).step_by(7) {
        let byte = bytes[*at];
        *at += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            break;
        }
    }
    value
}

fn put_u16(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&u16::try_from(value).expect("a u16 field").to_le_bytes());
}

fn put_u32(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&u32::try_from(value).expect("a u32 field").to_le_bytes());
}

fn u16_at(bytes: &[u8], at: usize) -> u32 {
    u32::from(u16::from_le_bytes([bytes[at], bytes[at + 1]]))
}

fn u32_at(bytes: &[u8], at: usize) -> usize {
    u32::from_le_bytes(bytes[at..at + 4].try_into().expect("four bytes")) as usize
}

fn set_u16(bytes: &mut [u8], at: usize, value: u16) {
    bytes[at..at + 2].copy_from_slice(&value.to_le_bytes());
}

fn set_u32(bytes: &mut [u8], at: usize, value: u32) {
    bytes[at..at + 4].copy_from_slice(&value.to_le_bytes());
}

/// Control, `body_bytes`, body.
fn framed(control: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![control];
    put_varint(&mut out, body.len() as u64);
    out.extend_from_slice(body);
    out
}

/// Maximal intervals of ascending distinct values, both ends inclusive.
fn intervals(values: &[u32]) -> Vec<(u32, u32)> {
    let mut out: Vec<(u32, u32)> = Vec::new();
    for &value in values {
        match out.last_mut() {
            Some((_, end)) if u64::from(*end) + 1 == u64::from(value) => *end = value,
            _ => out.push((value, value)),
        }
    }
    out
}

/// The Cone model's scale over a chunk's values (step 1).
fn cone_scale(lows: &[u32]) -> u64 {
    let span = u128::from(lows[lows.len() - 1] - lows[0]);
    u64::try_from((span << 32) / (lows.len() as u128 - 1)).expect("a u64 scale")
}

/// Each value less its Cone model `first + floor(i * scale / 2^32)`.
fn cone_residuals(lows: &[u32]) -> Vec<i64> {
    let scale = u128::from(cone_scale(lows));
    lows.iter()
        .enumerate()
        .map(|(i, &value)| {
            i64::from(value) - i64::from(lows[0]) - ((i as u128 * scale) >> 32) as i64
        })
        .collect()
}

/// Step 1: the container kind of one chunk's values.
fn arm(lows: &[u32]) -> u8 {
    let n = lows.len();
    if n == 1 {
        return ARRAY;
    }
    if lows
        .windows(2)
        .all(|pair| pair[1] - pair[0] == lows[1] - lows[0])
    {
        return STRIDE;
    }
    let r = intervals(lows).len();
    if n > 4096 {
        return if 6 * r < 8720 { RUNS } else { WORDS };
    }
    let cone = (n <= 1024 && cone_residuals(lows).iter().all(|r| r.abs() <= 63)).then_some(16 + n);
    // The first of equal prices wins: Cone, then Array, then Runs.
    [(CONE, cone), (ARRAY, Some(2 * n)), (RUNS, Some(6 * r))]
        .into_iter()
        .filter_map(|(kind, price)| Some((kind, price?)))
        .min_by_key(|&(_, price)| price)
        .map(|(kind, _)| kind)
        .expect("Array always prices")
}

/// A container payload of `kind` over a chunk's values.
fn payload(kind: u8, lows: &[u32]) -> Vec<u8> {
    let mut out = Vec::new();
    let last = lows[lows.len() - 1];
    match kind {
        STRIDE => {
            put_u16(&mut out, lows[0]);
            put_u16(&mut out, if lows.len() > 1 { lows[1] - lows[0] } else { 1 });
        }
        CONE => {
            let residuals = cone_residuals(lows);
            put_u16(&mut out, lows[0]);
            let eps = residuals
                .iter()
                .map(|r| r.unsigned_abs())
                .max()
                .unwrap_or(0);
            out.push(u8::try_from(eps).expect("an eps byte"));
            out.extend_from_slice(&cone_scale(lows).to_le_bytes());
            out.extend(
                residuals
                    .iter()
                    .map(|&r| i8::try_from(r).expect("a residual byte") as u8),
            );
        }
        ARRAY => lows.iter().for_each(|&value| put_u16(&mut out, value)),
        RUNS => {
            let runs = intervals(lows);
            put_varint(&mut out, runs.len() as u64 - 1);
            for &(start, end) in &runs {
                put_u16(&mut out, start);
                put_u16(&mut out, end);
            }
            let mut before = 0;
            for (i, &(start, end)) in runs.iter().enumerate() {
                if i != 0 && i + 1 != runs.len() {
                    put_u16(&mut out, before);
                }
                before += end - start + 1;
            }
        }
        WORDS => {
            let base = lows[0] / 64 / 8 * 8;
            let w = (last / 64 / 8 + 1) * 8 - base;
            let mut words = vec![0u64; w as usize];
            let mut summary = [0u64; 2];
            for &value in lows {
                words[(value / 64 - base) as usize] |= 1 << (value % 64);
                summary[(value / 512 / 64) as usize] |= 1 << (value / 512 % 64);
            }
            summary
                .iter()
                .for_each(|word| out.extend_from_slice(&word.to_le_bytes()));
            put_u16(&mut out, base);
            put_u16(&mut out, w);
            put_u16(&mut out, lows[0]);
            put_u16(&mut out, last);
            words
                .iter()
                .for_each(|word| out.extend_from_slice(&word.to_le_bytes()));
            for g in 1..w as usize / 4 {
                put_u16(
                    &mut out,
                    words[..4 * g].iter().map(|word| word.count_ones()).sum(),
                );
            }
        }
        _ => unreachable!("kind {kind} has no payload"),
    }
    out
}

/// One chunk of a directory: its key, kind, cardinality and payload.
#[derive(Clone)]
struct Chunk {
    key: u32,
    kind: u8,
    card: u64,
    payload: Vec<u8>,
}

impl Chunk {
    fn of(key: u32, kind: u8, lows: &[u32]) -> Chunk {
        Chunk {
            key,
            kind,
            card: lows.len() as u64,
            payload: payload(kind, lows),
        }
    }
}

/// A u32 directory body over its chunks, in the order given.
fn directory_body(chunks: &[Chunk]) -> Vec<u8> {
    let c = chunks.len();
    let mut body = Vec::new();
    put_varint(
        &mut body,
        chunks.iter().map(|chunk| chunk.card).sum::<u64>() - 1,
    );
    put_varint(&mut body, c as u64 - 1);
    chunks
        .iter()
        .for_each(|chunk| put_u16(&mut body, chunk.key));
    let mut before = 0;
    for chunk in &chunks[..c - 1] {
        before += chunk.card;
        put_u32(&mut body, before);
    }
    let mut offset = 0;
    for chunk in &chunks[..c - 1] {
        offset += chunk.payload.len() as u64;
        put_u32(&mut body, offset);
    }
    let mut kinds = vec![0u8; (3 * c).div_ceil(8)];
    for (i, chunk) in chunks.iter().enumerate() {
        let (byte, shifted) = (3 * i / 8, u16::from(chunk.kind) << (3 * i % 8));
        kinds[byte] |= shifted as u8;
        if let Some(next) = kinds.get_mut(byte + 1) {
            *next |= (shifted >> 8) as u8;
        }
    }
    body.extend(kinds);
    let (first, last) = (chunks[0].key, chunks[c - 1].key);
    if c > 64 && last.wrapping_sub(first) != c as u32 - 1 {
        let bits = (8 * c / 7 + 1).next_power_of_two().trailing_zeros();
        let mut slots = vec![0u64; 1 << bits];
        for (i, chunk) in chunks.iter().enumerate() {
            let key = u64::from(chunk.key);
            let mut at = (key.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> (64 - bits)) as usize;
            for _ in 0..slots.len() {
                if slots[at] == 0 {
                    break;
                }
                at = (at + 1) % slots.len();
            }
            slots[at] = key << 32 | (i as u64 + 1);
        }
        slots
            .iter()
            .for_each(|slot| body.extend_from_slice(&slot.to_le_bytes()));
    }
    chunks
        .iter()
        .for_each(|chunk| body.extend_from_slice(&chunk.payload));
    body
}

fn directory(chunks: &[Chunk]) -> Vec<u8> {
    framed(CONTROL_DIRECTORY, &directory_body(chunks))
}

/// Step 3's candidates for one chunk at key zero, in their order, with
/// their payload lengths.
fn small_candidates(lows: &[u32]) -> [Option<(Small, usize)>; 5] {
    let (first, last) = (lows[0], lows[lows.len() - 1]);
    let (start, end) = (first / 8, last / 8);
    let (n, r) = (lows.len(), intervals(lows).len());
    [
        (end <= 31).then_some((Small::Window, end as usize + 1)),
        ((1..=255).contains(&start) && end - start <= 31)
            .then_some((Small::WindowAt, 2 + (end - start) as usize)),
        (r <= 32).then_some((Small::Spans, 4 * r)),
        (last <= 255 && n <= 32).then_some((Small::List8, n)),
        (n <= 32).then_some((Small::List16, 2 * n)),
    ]
}

/// The shortest candidate; the earlier one wins a tie.
fn small_choice(lows: &[u32]) -> Option<(Small, usize)> {
    small_candidates(lows)
        .into_iter()
        .flatten()
        .min_by_key(|&(_, len)| len)
}

/// A small image of `form` over a chunk-zero set, chosen or not.
fn small_image(form: Small, lows: &[u32]) -> Vec<u8> {
    let (first, last) = (lows[0], lows[lows.len() - 1]);
    let bitmap = |start: u32| {
        let mut bits = vec![0u8; (last / 8 - start + 1) as usize];
        for &value in lows {
            bits[(value / 8 - start) as usize] |= 1 << (value % 8);
        }
        bits
    };
    let (count, payload) = match form {
        Small::List8 => (
            lows.len(),
            lows.iter()
                .map(|&value| u8::try_from(value).expect("a List8 member"))
                .collect(),
        ),
        Small::Window => {
            let bits = bitmap(0);
            (bits.len(), bits)
        }
        Small::WindowAt => {
            let start = first / 8;
            let bits = bitmap(start);
            let start = u8::try_from(start).expect("a Window at start");
            (bits.len(), [vec![start], bits].concat())
        }
        Small::List16 => {
            let mut out = Vec::new();
            lows.iter().for_each(|&value| put_u16(&mut out, value));
            (lows.len(), out)
        }
        Small::Spans => {
            let spans = intervals(lows);
            let mut out = Vec::new();
            for (start, end) in &spans {
                put_u16(&mut out, *start);
                put_u16(&mut out, *end);
            }
            (spans.len(), out)
        }
    };
    assert!((1..=32).contains(&count), "a small count is 1 to 32");
    [vec![(form as u8) << 5 | (count - 1) as u8], payload].concat()
}

/// Members grouped by chunk key, as `(key, low halves)`.
fn chunks_of(members: &[u32]) -> Vec<(u32, Vec<u32>)> {
    let mut out: Vec<(u32, Vec<u32>)> = Vec::new();
    for &member in members {
        let (key, low) = (member >> 16, member & 0xffff);
        match out.last_mut() {
            Some((held, lows)) if *held == key => lows.push(low),
            _ => out.push((key, vec![low])),
        }
    }
    out
}

/// Steps 2 to 4: the forest-family image.
fn forest(members: &[u32]) -> Vec<u8> {
    if members.is_empty() {
        return vec![CONTROL_EMPTY];
    }
    let chunks = chunks_of(members);
    let directory = directory(
        &chunks
            .iter()
            .map(|(key, lows)| Chunk::of(*key, arm(lows), lows))
            .collect::<Vec<_>>(),
    );
    match chunks.as_slice() {
        [(0, lows)] => match small_choice(lows) {
            Some((form, len)) if 1 + len <= directory.len() => small_image(form, lows),
            _ => directory,
        },
        _ => directory,
    }
}

/// An Elias-Fano image of `base` plus each of `ds` (`ds[0] == 0`), with
/// where its planes lie.
struct EliasFano {
    image: Vec<u8>,
    high_at: usize,
    zeros_at: usize,
    high_bits: u64,
}

fn elias_fano_image(base: u64, ds: &[u64]) -> EliasFano {
    let n = ds.len() as u64;
    let span = ds[ds.len() - 1];
    let log2 = |value: u64| 63 - value.leading_zeros();
    let low = if span <= n {
        0
    } else {
        let low = log2(span) - log2(n);
        if low != 0 && span >> low < n {
            low - 1
        } else {
            low
        }
    };
    let buckets = (span >> low) + 1;
    let high_bits = buckets + n;
    let mut high = vec![0u64; high_bits.div_ceil(64) as usize];
    let mut lows = vec![0u8; (n * u64::from(low)).div_ceil(8) as usize];
    for (k, &d) in ds.iter().enumerate() {
        let bit = (d >> low) + k as u64;
        high[(bit / 64) as usize] |= 1 << (bit % 64);
        for b in 0..u64::from(low) {
            if d >> b & 1 != 0 {
                let at = k as u64 * u64::from(low) + b;
                lows[(at / 8) as usize] |= 1 << (at % 8);
            }
        }
    }
    let mut body = Vec::new();
    put_varint(&mut body, n - 1);
    put_varint(&mut body, base);
    put_varint(&mut body, span);
    let high_at = body.len();
    high.iter()
        .for_each(|word| body.extend_from_slice(&word.to_le_bytes()));
    body.extend(lows);
    for j in 1..=(n - 1) / 64 {
        let k = 64 * j;
        put_u32(&mut body, (ds[k as usize] >> low) + k);
    }
    let zeros_at = body.len();
    for j in 0..buckets.div_ceil(64) {
        let zero = 64 * j;
        put_u32(
            &mut body,
            zero + ds.iter().filter(|&&d| d >> low <= zero).count() as u64,
        );
    }
    let image = framed(CONTROL_ELIAS_FANO, &body);
    let frame = image.len() - body.len();
    EliasFano {
        image,
        high_at: frame + high_at,
        zeros_at: frame + zeros_at,
        high_bits,
    }
}

/// A u32 set's Elias-Fano image, when it has two members and its geometry
/// fits the u32 position domain.
fn elias_fano(members: &[u32]) -> Option<Vec<u8>> {
    if members.len() < 2 {
        return None;
    }
    let base = u64::from(members[0]);
    let ds: Vec<u64> = members
        .iter()
        .map(|&member| u64::from(member) - base)
        .collect();
    let ef = elias_fano_image(base, &ds);
    (ef.high_bits - 1 <= u64::from(u32::MAX)).then_some(ef.image)
}

/// Step 5: the sealed image.
fn sealed(members: &[u32]) -> Vec<u8> {
    let forest = forest(members);
    match elias_fano(members) {
        Some(ef) if ef.len() < forest.len() => ef,
        _ => forest,
    }
}

/// A u32 image as a u64 child: without its `body_bytes`.
fn unframed(image: &[u8]) -> Vec<u8> {
    if Small::of_tag(image[0]).is_some() {
        return image.to_vec();
    }
    let mut at = 1;
    read_varint(image, &mut at);
    [&image[..1], &image[at..]].concat()
}

/// A u64 directory over `(key, cardinality, child)`, in the order given.
fn wide_directory(children: &[(u32, u64, Vec<u8>)]) -> Vec<u8> {
    let m = children.len();
    let mut body = Vec::new();
    put_varint(&mut body, m as u64 - 1);
    children
        .iter()
        .for_each(|(key, ..)| body.extend_from_slice(&key.to_le_bytes()));
    let mut before = 0u64;
    for (i, (_, card, _)) in children.iter().enumerate() {
        if i != 0 && i + 1 != m {
            body.extend_from_slice(&before.to_le_bytes());
        }
        before += card;
    }
    let mut offset = 0;
    for (_, _, child) in &children[..m - 1] {
        offset += child.len() as u64;
        put_u32(&mut body, offset);
    }
    children
        .iter()
        .for_each(|(_, _, child)| body.extend_from_slice(child));
    framed(CONTROL_U64, &body)
}

/// Step 6: the u64 image, each child the sealed image of its low halves.
fn wide(members: &[u64]) -> Vec<u8> {
    if members.is_empty() {
        return vec![CONTROL_EMPTY_U64];
    }
    let mut groups: Vec<(u32, Vec<u32>)> = Vec::new();
    for &member in members {
        let (key, low) = ((member >> 32) as u32, member as u32);
        match groups.last_mut() {
            Some((held, lows)) if *held == key => lows.push(low),
            _ => groups.push((key, vec![low])),
        }
    }
    wide_directory(
        &groups
            .iter()
            .map(|(key, lows)| (*key, lows.len() as u64, unframed(&sealed(lows))))
            .collect::<Vec<_>>(),
    )
}

// ── reading forms back, from the README's layouts ───────────────────────

/// Where a u32 directory's planes lie.
struct Planes {
    chunks: usize,
    offsets_at: usize,
    kinds_at: usize,
    slots_at: usize,
    payload_at: usize,
}

impl Planes {
    fn of(image: &[u8], framed: bool) -> Planes {
        let mut at = 1;
        if framed {
            read_varint(image, &mut at);
        }
        read_varint(image, &mut at);
        let chunks = read_varint(image, &mut at) as usize + 1;
        let keys_at = at;
        let offsets_at = keys_at + 2 * chunks + 4 * (chunks - 1);
        let kinds_at = offsets_at + 4 * (chunks - 1);
        let slots_at = kinds_at + (3 * chunks).div_ceil(8);
        let span = u16_at(image, keys_at + 2 * (chunks - 1)).wrapping_sub(u16_at(image, keys_at));
        let slots = if chunks > 64 && span as usize != chunks - 1 {
            (8 * chunks / 7 + 1).next_power_of_two()
        } else {
            0
        };
        Planes {
            chunks,
            offsets_at,
            kinds_at,
            slots_at,
            payload_at: slots_at + 8 * slots,
        }
    }

    fn kind(&self, image: &[u8], i: usize) -> u8 {
        let at = self.kinds_at + 3 * i / 8;
        let pair = u16::from(image[at]) | u16::from(*image.get(at + 1).unwrap_or(&0)) << 8;
        (pair >> (3 * i % 8) & 7) as u8
    }
}

/// The form of a u32 image, or of a u64 child (`framed` false).
fn form_of(image: &[u8], framed: bool) -> Form {
    match image[0] {
        CONTROL_EMPTY => Form::Empty,
        CONTROL_ELIAS_FANO => Form::EliasFano,
        CONTROL_DIRECTORY => {
            let planes = Planes::of(image, framed);
            Form::Directory((0..planes.chunks).map(|i| planes.kind(image, i)).collect())
        }
        tag => Form::Small(
            Small::of_tag(tag).unwrap_or_else(|| panic!("control {tag:#04x} is no u32 image")),
        ),
    }
}

/// The members of a small image, decoded from its payload.
fn small_members(image: &[u8]) -> Vec<u32> {
    let form = Small::of_tag(image[0]).expect("a small tag");
    let count = usize::from(image[0] & 31) + 1;
    let payload = &image[1..];
    let bits = |start: u32, bytes: &[u8]| -> Vec<u32> {
        (0..8 * bytes.len())
            .filter(|bit| bytes[bit / 8] >> (bit % 8) & 1 != 0)
            .map(|bit| 8 * start + bit as u32)
            .collect()
    };
    match form {
        Small::List8 => payload[..count].iter().map(|&b| u32::from(b)).collect(),
        Small::Window => bits(0, &payload[..count]),
        Small::WindowAt => bits(u32::from(payload[0]), &payload[1..=count]),
        Small::List16 => (0..count).map(|i| u16_at(payload, 2 * i)).collect(),
        Small::Spans => (0..count)
            .flat_map(|i| u16_at(payload, 4 * i)..=u16_at(payload, 4 * i + 2))
            .collect(),
    }
}

/// The children of a u64 image, each without its `body_bytes`.
fn wide_children(image: &[u8]) -> Vec<&[u8]> {
    if image[0] == CONTROL_EMPTY_U64 {
        return Vec::new();
    }
    let mut at = 1;
    read_varint(image, &mut at);
    let m = read_varint(image, &mut at) as usize + 1;
    let offsets_at = at + 4 * m + 8 * m.saturating_sub(2);
    let children_at = offsets_at + 4 * (m - 1);
    let bound = |i: usize| match i {
        0 => 0,
        _ if i == m => image.len() - children_at,
        _ => u32_at(image, offsets_at + 4 * (i - 1)),
    };
    (0..m)
        .map(|i| &image[children_at + bound(i)..children_at + bound(i + 1)])
        .collect()
}

// ── sets ────────────────────────────────────────────────────────────────

/// Ascending members as JSON, intervals of three or more as `[first, last]`.
fn members_json(members: &[u32]) -> String {
    intervals(members)
        .into_iter()
        .map(|(first, last)| match last - first {
            0 => first.to_string(),
            1 => format!("{first},{last}"),
            _ => format!("[{first},{last}]"),
        })
        .collect::<Vec<_>>()
        .join(",")
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

fn interval(first: u32, last: u32) -> Vec<u32> {
    (first..=last).collect()
}

fn progression(first: u32, step: u32, n: u32) -> Vec<u32> {
    (0..n).map(|i| first + i * step).collect()
}

/// Two of every three values of `first..=last`: the ones not divisible by
/// three.
fn thinned(first: u32, last: u32) -> Vec<u32> {
    (first..=last).filter(|value| value % 3 != 0).collect()
}

fn jittered(n: u32, step: u32) -> Vec<u32> {
    (0..n).map(|i| i * step + (i % 3) * 7).collect()
}

/// `n` values in `r` runs of near-equal lengths, a run every `every` values.
fn even_runs(n: u32, r: u32, every: u32) -> Vec<u32> {
    (0..r)
        .flat_map(|j| j * every..j * every + (j + 1) * n / r - j * n / r)
        .collect()
}

/// `n` values in `r` runs of three and of one (one run of two when `n - r`
/// is odd), a run every six values.
fn sparse_runs(n: u32, r: u32) -> Vec<u32> {
    let threes = (n - r) / 2;
    (0..r)
        .flat_map(|j| {
            let len = match j {
                _ if j < threes => 3,
                _ if j == threes && (n - r) % 2 == 1 => 2,
                _ => 1,
            };
            6 * j..6 * j + len
        })
        .collect()
}

/// Twenty values every 500, the sixth moved by `residual`: a Cone of that
/// one residual, the model unchanged.
fn residual(residual: i32) -> Vec<u32> {
    (0..20)
        .map(|i| (i * 500 + if i == 5 { residual } else { 0 }) as u32)
        .collect()
}

fn in_chunk(key: u32, lows: impl IntoIterator<Item = u32>) -> Vec<u32> {
    lows.into_iter().map(|low| key << 16 | low).collect()
}

/// A Words chunk past the list limit: 4,097 values in 1,454 runs.
fn words_4097() -> Vec<u32> {
    sparse_runs(4_097, 1_454)
}

// ── accepted cases ──────────────────────────────────────────────────────

/// The u32 cases: a name, the members, and the form of the slot image.
fn u32_cases() -> Vec<(&'static str, Vec<u32>, Form)> {
    use Small::{List8, List16, Spans, Window, WindowAt};
    let small = Form::Small;
    let dir = |kinds: &[u8]| Form::Directory(kinds.to_vec());
    let singletons = |keys: Vec<u32>| {
        keys.into_iter()
            .map(|key| key << 16 | 5)
            .collect::<Vec<_>>()
    };
    let every_kind: Vec<u32> = [
        in_chunk(0, [7, 40]),
        in_chunk(1, jittered(100, 500)),
        in_chunk(2, scatter(40, 65_536, 0x5eed_0006)),
        in_chunk(3, (0..4096).chain(32_768..36_864)),
        in_chunk(4, words_4097()),
        in_chunk(5, [9]),
    ]
    .concat();
    let sparse = scatter(100, 13_000_000, 0x5eed_0064);
    let sparse_kinds = chunks_of(&sparse)
        .iter()
        .map(|(_, lows)| arm(lows))
        .collect();
    let list8_31: Vec<u32> = (0..30).map(|i| i * 8 + i % 3).chain([255]).collect();
    vec![
        ("empty", vec![], Form::Empty),
        // List8: count 1 to 31; at 32 members below 256 a Window is never longer.
        ("list8-count-1", vec![200], small(List8)),
        ("readme-list8", vec![7, 200], small(List8)),
        ("list8-count-31", list8_31, small(List8)),
        ("list8-up-to-255", vec![0, 255], small(List8)),
        ("list16-past-255", vec![0, 256], small(List16)),
        ("window-ties-list8", vec![0, 8], small(Window)),
        ("list8-shorter-than-window", vec![0, 16], small(List8)),
        (
            "spans-ties-list8",
            [interval(0, 3), interval(250, 253)].concat(),
            small(Spans),
        ),
        (
            "list8-shorter-than-spans",
            [interval(0, 2), interval(250, 253)].concat(),
            small(List8),
        ),
        (
            "list8-ties-stride-directory",
            progression(0, 25, 10),
            small(List8),
        ),
        (
            "stride-directory-one-shorter-than-list8",
            progression(0, 25, 11),
            dir(&[STRIDE]),
        ),
        // Window.
        ("window-count-1-at-zero", vec![0], small(Window)),
        ("readme-window-count-1", vec![7], small(Window)),
        ("readme-window", vec![0, 1, 2, 10], small(Window)),
        (
            "window-of-9-bytes",
            vec![1, 3, 9, 14, 20, 25, 33, 40, 47, 55, 60, 66, 69],
            small(Window),
        ),
        ("window-of-13-bytes", thinned(0, 100), small(Window)),
        ("window-count-32", thinned(0, 255), small(Window)),
        (
            "window-past-32-bytes-is-a-cone-directory",
            thinned(0, 263),
            dir(&[CONE]),
        ),
        ("window-ties-window-at", thinned(8, 63), small(Window)),
        (
            "window-at-shorter-than-window",
            thinned(16, 71),
            small(WindowAt),
        ),
        ("window-ties-spans", interval(0, 31), small(Window)),
        ("spans-shorter-than-window", interval(0, 32), small(Spans)),
        (
            "window-ties-stride-directory",
            progression(0, 2, 40),
            small(Window),
        ),
        (
            "stride-directory-one-shorter-than-window",
            progression(0, 2, 41),
            dir(&[STRIDE]),
        ),
        // Window at.
        ("window-at-start-255", vec![2040, 2042], small(WindowAt)),
        ("no-window-at-start-256", vec![2048, 2050], small(List16)),
        (
            "window-at-count-32-start-1",
            thinned(8, 263),
            small(WindowAt),
        ),
        (
            "window-at-past-32-bytes-is-a-cone-directory",
            thinned(8, 271),
            dir(&[CONE]),
        ),
        ("window-at-ties-spans", interval(16, 39), small(WindowAt)),
        (
            "spans-shorter-than-window-at",
            interval(16, 47),
            small(Spans),
        ),
        ("window-at-ties-list8", vec![16, 17, 24], small(WindowAt)),
        ("list8-shorter-than-window-at", vec![16, 24], small(List8)),
        ("window-at-ties-list16", vec![300, 316], small(WindowAt)),
        (
            "list16-shorter-than-window-at",
            vec![300, 324],
            small(List16),
        ),
        (
            "window-at-ties-stride-directory",
            progression(80, 2, 36),
            small(WindowAt),
        ),
        (
            "stride-directory-one-shorter-than-window-at",
            progression(80, 2, 40),
            dir(&[STRIDE]),
        ),
        // List16.
        ("list16-count-1", vec![65_535], small(List16)),
        ("readme-list16", vec![1000, 2000], small(List16)),
        (
            "list16-count-32",
            scatter(32, 65_536, 0x5eed_0020),
            small(List16),
        ),
        (
            "list16-ties-cone-directory",
            jittered(17, 500),
            small(List16),
        ),
        (
            "cone-directory-one-shorter-than-list16",
            jittered(18, 500),
            dir(&[CONE]),
        ),
        ("spans-ties-list16", vec![3000, 3001], small(Spans)),
        ("list16-shorter-than-spans", vec![3000, 3002], small(List16)),
        (
            "spans-shorter-than-list16",
            interval(3000, 3002),
            small(Spans),
        ),
        // Spans.
        ("readme-spans-count-1", interval(0, 82), small(Spans)),
        (
            "readme-two-intervals",
            (0..4096).chain(32_768..36_864).collect(),
            small(Spans),
        ),
        ("full-chunk-is-one-span", interval(0, 65_535), small(Spans)),
        ("spans-count-32", even_runs(320, 32, 2000), small(Spans)),
        (
            "spans-count-32-of-short-runs",
            even_runs(96, 32, 2000),
            small(Spans),
        ),
        (
            "spans-ties-cone-directory",
            even_runs(23, 10, 60),
            small(Spans),
        ),
        (
            "cone-directory-one-shorter-than-spans",
            even_runs(22, 10, 60),
            dir(&[CONE]),
        ),
        // One chunk at key zero with no small form as short.
        (
            "array-at-key-zero",
            scatter(33, 65_536, 0x5eed_0021),
            dir(&[ARRAY]),
        ),
        ("cone-at-key-zero", jittered(100, 500), dir(&[CONE])),
        ("runs-at-key-zero", even_runs(400, 40, 1000), dir(&[RUNS])),
        (
            "array-4096-in-1453-runs",
            sparse_runs(4_096, 1_453),
            dir(&[ARRAY]),
        ),
        (
            "runs-4097-in-1453-runs",
            sparse_runs(4_097, 1_453),
            dir(&[RUNS]),
        ),
        ("words-4097-in-1454-runs", words_4097(), dir(&[WORDS])),
        // Step 1 past key zero.
        ("singleton-second-chunk", vec![65_536], dir(&[ARRAY])),
        ("singleton-max", vec![u32::MAX], dir(&[ARRAY])),
        ("pair-is-stride", in_chunk(1, [5, 9]), dir(&[STRIDE])),
        (
            "progression-is-stride",
            in_chunk(1, progression(200, 100, 3)),
            dir(&[STRIDE]),
        ),
        (
            "cone-ties-array-at-16",
            in_chunk(1, jittered(16, 500)),
            dir(&[CONE]),
        ),
        (
            "array-beats-cone-at-15",
            in_chunk(1, jittered(15, 500)),
            dir(&[ARRAY]),
        ),
        ("cone-residual-63", in_chunk(1, residual(63)), dir(&[CONE])),
        (
            "cone-residual-64-is-array",
            in_chunk(1, residual(64)),
            dir(&[ARRAY]),
        ),
        (
            "cone-residual-minus-63",
            in_chunk(1, residual(-63)),
            dir(&[CONE]),
        ),
        (
            "cone-residual-minus-64-is-array",
            in_chunk(1, residual(-64)),
            dir(&[ARRAY]),
        ),
        (
            "cone-ties-runs",
            in_chunk(1, even_runs(20, 6, 10)),
            dir(&[CONE]),
        ),
        (
            "runs-beat-cone",
            in_chunk(1, even_runs(21, 6, 10)),
            dir(&[RUNS]),
        ),
        (
            "runs-tie-array-is-array",
            in_chunk(1, [0, 1, 2, 1000, 1001, 1002]),
            dir(&[ARRAY]),
        ),
        (
            "runs-beat-array",
            in_chunk(1, [0, 1, 2, 1000, 1001, 1002, 1003]),
            dir(&[RUNS]),
        ),
        (
            "cone-count-1024",
            in_chunk(1, even_runs(1_024, 180, 16)),
            dir(&[CONE]),
        ),
        (
            "no-cone-above-1024",
            in_chunk(1, even_runs(1_025, 180, 16)),
            dir(&[RUNS]),
        ),
        // Directories of several chunks.
        ("two-chunks", vec![1, 65_545], dir(&[ARRAY, ARRAY])),
        (
            "every-kind-in-a-directory",
            every_kind,
            dir(&[STRIDE, CONE, ARRAY, RUNS, WORDS, ARRAY]),
        ),
        (
            "64-broken-keys-no-slot-plane",
            singletons((0..63).chain([70]).collect()),
            dir(&[ARRAY; 64]),
        ),
        (
            "65-contiguous-keys-no-slot-plane",
            singletons((3..68).collect()),
            dir(&[ARRAY; 65]),
        ),
        (
            "65-broken-keys-slot-plane",
            singletons((0..64).chain([70]).collect()),
            dir(&[ARRAY; 65]),
        ),
        // Elias-Fano roots.
        (
            "sparse-hundred-seals-as-elias-fano",
            sparse,
            Form::Directory(sparse_kinds),
        ),
        (
            "key-zero-seals-as-elias-fano",
            scatter(300, 65_536, 0x5eed_012c),
            dir(&[ARRAY]),
        ),
    ]
}

/// The u64 cases: a name, the members, and the form of each child.
fn u64_cases() -> Vec<(&'static str, Vec<u64>, Vec<Form>)> {
    let wide =
        |high: u64, lows: Vec<u32>| lows.into_iter().map(move |low| high << 32 | u64::from(low));
    vec![
        ("empty", vec![], vec![]),
        (
            "singleton-small-child",
            vec![7],
            vec![Form::Small(Small::Window)],
        ),
        (
            "singleton-max",
            vec![u64::MAX],
            vec![Form::Directory(vec![ARRAY])],
        ),
        (
            "three-highs",
            vec![1, 1 << 32 | 3, 2 << 32 | 5],
            vec![Form::Small(Small::Window); 3],
        ),
        (
            "small-elias-fano-and-directory-children",
            wide(0, vec![7, 17, 40, 82])
                .chain(wide(9, scatter(100, 13_000_000, 0x5eed_0909)))
                .chain(wide(0xffff_ffff, vec![1, 65_545]))
                .collect(),
            vec![
                Form::Small(Small::List8),
                Form::EliasFano,
                Form::Directory(vec![ARRAY, ARRAY]),
            ],
        ),
    ]
}

fn read_back(name: &str, bytes: &[u8], view: BitmosaicView<'_>, members: &[u32]) {
    assert_eq!(view.serialized_len(), bytes.len(), "{name}: extent");
    assert!(view.range().eq(members.iter().copied()), "{name}: members");
}

const ALL_ENTRIES: &str = "[\"open\", \"open_verified\", \"open_canonical\"]";
const SLOT_ENTRIES: &str = "[\"open\", \"open_verified\"]";

fn u32_case(name: &str, members: &[u32], expected: &Form) -> String {
    let set = Bitmosaic::from_sorted(members.iter().copied());
    let (image, slot) = (set.to_bytes(), set.to_forest_bytes());
    assert_eq!(
        hex(&slot),
        hex(&forest(members)),
        "{name}: the crate's slot image is not the README's"
    );
    assert_eq!(
        hex(&image),
        hex(&sealed(members)),
        "{name}: the crate's sealed image is not the README's"
    );
    let slot_form = form_of(&slot, true);
    assert_eq!(&slot_form, expected, "{name}: slot form");
    match &slot_form {
        Form::Small(_) => assert_eq!(small_members(&slot), members, "{name}: small members"),
        Form::Directory(_) => {
            let planes = Planes::of(&slot, true);
            let view = BitmosaicView::open(&slot).unwrap_or_else(|| panic!("{name}: open"));
            assert_eq!(
                view.has_slot_plane(),
                planes.payload_at != planes.slots_at,
                "{name}: slot plane"
            );
        }
        _ => {}
    }
    let refused = |error| panic!("{name}: a written image is refused: {error}");
    read_back(
        name,
        &image,
        BitmosaicView::open(&image).unwrap_or_else(|| panic!("{name}: open")),
        members,
    );
    read_back(
        name,
        &image,
        BitmosaicView::open_verified(&image).unwrap_or_else(refused),
        members,
    );
    read_back(
        name,
        &image,
        BitmosaicView::open_canonical(&image).unwrap_or_else(refused),
        members,
    );
    let slot_fields = if slot == image {
        String::new()
    } else {
        read_back(
            name,
            &slot,
            BitmosaicView::open(&slot).unwrap_or_else(|| panic!("{name}: open")),
            members,
        );
        read_back(
            name,
            &slot,
            BitmosaicView::open_verified(&slot).unwrap_or_else(refused),
            members,
        );
        let refusal = BitmosaicView::open_canonical(&slot)
            .map(|_| ())
            .expect_err("open_canonical admits only the sealed image");
        assert_eq!(
            refusal.class,
            AdmissionClass::Canonical,
            "{name}: {refusal}"
        );
        format!(
            "\"slot\": \"{}\", \"slot_admits\": {SLOT_ENTRIES}, \"slot_canonical_refusal\": \"{}\", ",
            hex(&slot),
            refusal.class.as_str()
        )
    };
    format!(
        "{{\"name\": \"{name}\", \"form\": \"{}\", \"image\": \"{}\", \"admits\": {ALL_ENTRIES}, \
         \"slot_form\": \"{}\", {slot_fields}\"members\": [{}]}}",
        form_of(&image, true).name(),
        hex(&image),
        slot_form.name(),
        members_json(members),
    )
}

fn u64_case(name: &str, members: &[u64], expected: &[Form]) -> String {
    let image = Bitmosaic64::from_sorted(members.iter().copied()).to_bytes();
    assert_eq!(
        hex(&image),
        hex(&wide(members)),
        "{name}: the crate's image is not the README's"
    );
    for view in [
        Bitmosaic64View::open(&image).unwrap_or_else(|| panic!("{name}: open")),
        Bitmosaic64View::open_verified(&image)
            .unwrap_or_else(|error| panic!("{name}: a written image is refused: {error}")),
    ] {
        assert_eq!(view.serialized_len(), image.len(), "{name}: extent");
        assert!(view.range().eq(members.iter().copied()), "{name}: members");
    }
    let children: Vec<Form> = wide_children(&image)
        .into_iter()
        .map(|child| form_of(child, false))
        .collect();
    assert_eq!(children, expected, "{name}: child forms");
    let quoted = |items: Vec<String>| items.join(",");
    format!(
        "{{\"name\": \"{name}\", \"image\": \"{}\", \"admits\": {SLOT_ENTRIES}, \"children\": [{}], \
         \"members\": [{}]}}",
        hex(&image),
        quoted(
            children
                .iter()
                .map(|form| format!("\"{}\"", form.name()))
                .collect()
        ),
        quoted(
            members
                .iter()
                .map(|member| format!("\"{member}\""))
                .collect()
        ),
    )
}

// ── rejected cases ──────────────────────────────────────────────────────

/// The first admission entry that refuses an image; every stronger entry
/// refuses it with the same class.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Entry {
    Open,
    Verified,
    Canonical,
}

impl Entry {
    fn name(self) -> &'static str {
        match self {
            Entry::Open => "open",
            Entry::Verified => "open_verified",
            Entry::Canonical => "open_canonical",
        }
    }
}

type Reject = (&'static str, u32, Vec<u8>, AdmissionClass, Entry);

fn altered(image: &[u8], alter: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
    let mut bytes = image.to_vec();
    alter(&mut bytes);
    bytes
}

/// One chunk, its payload as given.
fn one_chunk(key: u32, kind: u8, card: u64, payload: Vec<u8>) -> Vec<u8> {
    directory(&[Chunk {
        key,
        kind,
        card,
        payload,
    }])
}

fn u16s(values: &[u32]) -> Vec<u8> {
    let mut out = Vec::new();
    values.iter().for_each(|&value| put_u16(&mut out, value));
    out
}

/// Images structure refuses, or whose members or derived planes lie.
fn structural_rejects() -> Vec<Reject> {
    use AdmissionClass as Class;
    let three = || {
        vec![
            Chunk::of(0, ARRAY, &[1]),
            Chunk::of(1, ARRAY, &[9]),
            Chunk::of(2, ARRAY, &[22]),
        ]
    };
    let three_image = directory(&three());
    let three_planes = Planes::of(&three_image, true);
    let with = |alter: &dyn Fn(&mut Vec<Chunk>)| {
        let mut chunks = three();
        alter(&mut chunks);
        directory(&chunks)
    };
    let words = directory(&[Chunk::of(1, WORDS, &[5, 77, 300])]);
    let words_at = Planes::of(&words, true).payload_at;
    // A step-one model whose members sit ten past it: with eps zero the
    // window of member 1 (value 11) is indices 9 to 13.
    let cone_window_misses = [
        vec![0, 0, 0],
        (1u64 << 32).to_le_bytes().to_vec(),
        vec![0, 10, 10, 10, 10, 10, 10, 10],
    ]
    .concat();
    let cone_descends = [
        vec![0, 0, 15],
        (10u64 << 32).to_le_bytes().to_vec(),
        vec![0, 15, (-15i8) as u8],
    ]
    .concat();
    let slotted = forest(
        &(0..64)
            .chain([70])
            .map(|key| key << 16 | 5)
            .collect::<Vec<_>>(),
    );
    let slotted_planes = Planes::of(&slotted, true);
    let ef_ds: Vec<u64> = (0..10).map(|i| i * i * 1000).collect();
    let ef = elias_fano_image(3, &ef_ds);
    let ef_swapped: Vec<u64> = [vec![1000, 0], ef_ds[2..].to_vec()].concat();
    let wide_three = wide(&[1, 1 << 32 | 3, 2 << 32 | 5]);
    // Control, `body_bytes`, child count, three keys: the prefix at 15,
    // the two offsets at 23.
    let (wide_before_at, wide_offsets_at) = (15, 23);
    vec![
        ("no-bytes", 32, vec![], Class::Extent, Entry::Open),
        (
            "unassigned-control",
            32,
            vec![0xc0],
            Class::Kind,
            Entry::Open,
        ),
        (
            "control-below-the-framed-range",
            32,
            vec![0x08],
            Class::Kind,
            Entry::Open,
        ),
        (
            "unassigned-root-mode",
            32,
            vec![0x13, 1, 0],
            Class::Kind,
            Entry::Open,
        ),
        (
            "u64-image-read-as-u32",
            32,
            vec![CONTROL_EMPTY_U64],
            Class::Kind,
            Entry::Open,
        ),
        (
            "u32-image-read-as-u64",
            64,
            vec![CONTROL_EMPTY],
            Class::Kind,
            Entry::Open,
        ),
        (
            "small-payload-truncated",
            32,
            vec![0x83, 0xe8, 0x03],
            Class::Extent,
            Entry::Open,
        ),
        (
            "window-ends-in-a-zero-byte",
            32,
            vec![0x41, 0x01, 0x00],
            Class::Extent,
            Entry::Open,
        ),
        (
            "window-at-starts-in-a-zero-byte",
            32,
            vec![0x61, 0x02, 0x00, 0x01],
            Class::Extent,
            Entry::Open,
        ),
        (
            "window-at-start-zero",
            32,
            vec![0x60, 0x00, 0x01],
            Class::Kind,
            Entry::Open,
        ),
        (
            "list8-descends",
            32,
            vec![0x22, 0x07, 0x05, 0x09],
            Class::MapOrder,
            Entry::Verified,
        ),
        (
            "list8-repeats",
            32,
            vec![0x21, 0x07, 0x07],
            Class::MapOrder,
            Entry::Verified,
        ),
        (
            "list16-descends",
            32,
            vec![0x81, 0xd0, 0x07, 0xe8, 0x03],
            Class::MapOrder,
            Entry::Verified,
        ),
        (
            "spans-touch",
            32,
            [vec![0xa1], u16s(&[0, 5, 6, 9])].concat(),
            Class::MapOrder,
            Entry::Verified,
        ),
        (
            "spans-overlap",
            32,
            [vec![0xa1], u16s(&[0, 5, 5, 9])].concat(),
            Class::MapOrder,
            Entry::Verified,
        ),
        (
            "span-ends-before-it-starts",
            32,
            [vec![0xa0], u16s(&[5, 3])].concat(),
            Class::MapOrder,
            Entry::Verified,
        ),
        (
            "body-length-not-shortest",
            32,
            altered(&three_image, |bytes| {
                bytes[1] |= 0x80;
                bytes.insert(2, 0);
            }),
            Class::Extent,
            Entry::Open,
        ),
        (
            "body-truncated",
            32,
            altered(&three_image, |bytes| {
                bytes.pop();
            }),
            Class::Extent,
            Entry::Open,
        ),
        (
            "cardinality-not-shortest",
            32,
            {
                let mut body = directory_body(&three());
                body[0] |= 0x80;
                body.insert(1, 0);
                framed(CONTROL_DIRECTORY, &body)
            },
            Class::Count,
            Entry::Open,
        ),
        (
            "directory-chunk-of-kind-5",
            32,
            with(&|chunks| chunks[0].kind = 5),
            Class::Kind,
            Entry::Open,
        ),
        (
            "directory-kind-pad-bit",
            32,
            altered(&three_image, |bytes| {
                bytes[three_planes.kinds_at + 1] |= 0x80
            }),
            Class::BitPad,
            Entry::Open,
        ),
        (
            "directory-keys-descend",
            32,
            with(&|chunks| chunks[0].key = 1),
            Class::MapOrder,
            Entry::Open,
        ),
        (
            "directory-offset-past-payloads",
            32,
            altered(&three_image, |bytes| {
                set_u32(bytes, three_planes.offsets_at, 9_999)
            }),
            Class::Placement,
            Entry::Open,
        ),
        (
            "directory-prefix-leaves-a-chunk-empty",
            32,
            with(&|chunks| chunks[0].card = 0),
            Class::Count,
            Entry::Open,
        ),
        (
            "slot-plane-entry-misplaced",
            32,
            altered(&slotted, |bytes| {
                let slot = |i: usize| slotted_planes.slots_at + 8 * i;
                let held = (0..)
                    .find(|&i| bytes[slot(i)..slot(i) + 8] != [0; 8])
                    .expect("a held slot");
                let empty = (held..)
                    .find(|&i| bytes[slot(i)..slot(i) + 8] == [0; 8])
                    .expect("an empty slot");
                let entry = bytes[slot(held)..slot(held) + 8].to_vec();
                bytes[slot(held)..slot(held) + 8].fill(0);
                bytes[slot(empty)..slot(empty) + 8].copy_from_slice(&entry);
            }),
            Class::Placement,
            Entry::Open,
        ),
        (
            "array-payload-past-its-members",
            32,
            one_chunk(1, ARRAY, 1, u16s(&[5, 6])),
            Class::Extent,
            Entry::Open,
        ),
        (
            "array-members-descend",
            32,
            one_chunk(1, ARRAY, 3, u16s(&[1, 900, 899])),
            Class::MapOrder,
            Entry::Verified,
        ),
        (
            "stride-step-zero",
            32,
            one_chunk(1, STRIDE, 3, u16s(&[2, 0])),
            Class::PlaneRange,
            Entry::Open,
        ),
        (
            "stride-leaves-its-chunk",
            32,
            one_chunk(1, STRIDE, 3, u16s(&[2, 40_000])),
            Class::PlaneRange,
            Entry::Open,
        ),
        (
            "cone-model-overflows",
            32,
            one_chunk(
                1,
                CONE,
                3,
                [vec![0, 0, 0], u64::MAX.to_le_bytes().to_vec(), vec![0; 3]].concat(),
            ),
            Class::PlaneRange,
            Entry::Open,
        ),
        (
            "cone-window-misses-a-member",
            32,
            one_chunk(1, CONE, 8, cone_window_misses),
            Class::PlaneRange,
            Entry::Verified,
        ),
        (
            "cone-members-descend",
            32,
            one_chunk(1, CONE, 3, cone_descends),
            Class::MapOrder,
            Entry::Verified,
        ),
        (
            "runs-touch",
            32,
            one_chunk(1, RUNS, 6, [vec![1], u16s(&[0, 2, 3, 5])].concat()),
            Class::MapOrder,
            Entry::Open,
        ),
        (
            "runs-count-differs",
            32,
            directory(&[Chunk {
                card: 8,
                ..Chunk::of(1, RUNS, &[0, 1, 2, 1000, 1001, 1002, 1003])
            }]),
            Class::Count,
            Entry::Open,
        ),
        (
            "words-window-not-whole-blocks",
            32,
            altered(&words, |bytes| set_u16(bytes, words_at + 16, 1)),
            Class::Count,
            Entry::Open,
        ),
        (
            "words-bounds-disagree",
            32,
            altered(&words, |bytes| set_u16(bytes, words_at + 20, 6)),
            Class::PlaneRange,
            Entry::Verified,
        ),
        (
            "words-rank-disagrees",
            32,
            altered(&words, |bytes| set_u16(bytes, words_at + 24 + 64, 0)),
            Class::PlaneRange,
            Entry::Verified,
        ),
        (
            "words-summary-disagrees",
            32,
            altered(&words, |bytes| bytes[words_at] |= 0x02),
            Class::PlaneRange,
            Entry::Verified,
        ),
        (
            "elias-fano-members-descend",
            32,
            elias_fano_image(3, &ef_swapped).image,
            Class::MapOrder,
            Entry::Verified,
        ),
        (
            "elias-fano-pad-bit",
            32,
            altered(&ef.image, |bytes| bytes[ef.high_at + 7] |= 0x80),
            Class::BitPad,
            Entry::Open,
        ),
        (
            "elias-fano-zero-sample-misplaced",
            32,
            altered(&ef.image, |bytes| bytes[ef.zeros_at] += 1),
            Class::Placement,
            Entry::Verified,
        ),
        (
            "elias-fano-past-u32",
            32,
            elias_fano_image(0xffff_fff0, &[0, 32]).image,
            Class::PlaneRange,
            Entry::Open,
        ),
        (
            "u64-child-is-the-empty-image",
            64,
            wide_directory(&[(0, 0, vec![CONTROL_EMPTY]), (1, 1, vec![0x40, 0x01])]),
            Class::Kind,
            Entry::Open,
        ),
        (
            "u64-keys-descend",
            64,
            wide_directory(&[(1, 1, vec![0x40, 0x02]), (0, 1, vec![0x40, 0x08])]),
            Class::MapOrder,
            Entry::Open,
        ),
        (
            "u64-prefix-differs",
            64,
            altered(&wide_three, |bytes| bytes[wide_before_at] = 2),
            Class::Count,
            Entry::Open,
        ),
        (
            "u64-offset-splits-a-child",
            64,
            altered(&wide_three, |bytes| bytes[wide_offsets_at] = 3),
            Class::Extent,
            Entry::Open,
        ),
    ]
}

/// Well-formed images that are not the writer's for their set: `open`
/// admits each, and the class is `canonical`.
fn canonical_rejects() -> Vec<Reject> {
    use Small::{List8, List16, Spans, Window, WindowAt};
    let kind = |kind: u8, key: u32, lows: &[u32]| directory(&[Chunk::of(key, kind, lows)]);
    let (one, two) = ([1, 65_545], [7, 40]);
    assert!(
        elias_fano(&one).expect("two members").len() >= forest(&one).len(),
        "the forest family is no longer"
    );
    let ef_shorter: Vec<u32> = (0..10).map(|i| i * 70_001 + 3).collect();
    assert_eq!(form_of(&sealed(&ef_shorter), true), Form::EliasFano);
    let wide_child = |child: Vec<u8>, card: u64| wide_directory(&[(9, card, child)]);
    let list8_of_32: Vec<u32> = (0..32).map(|i| i * 8 + i % 3).collect();
    let canonical = |name, image| (name, 32, image, AdmissionClass::Canonical, Entry::Verified);
    vec![
        // Step 1: each container kind where the members select another.
        canonical("array-of-a-stride", kind(ARRAY, 1, &[10, 20, 30])),
        canonical("stride-of-a-singleton", kind(STRIDE, 1, &[5])),
        canonical("array-where-cone-ties", kind(ARRAY, 1, &jittered(16, 500))),
        canonical(
            "cone-where-array-is-cheaper",
            kind(CONE, 1, &jittered(15, 500)),
        ),
        canonical("cone-with-a-residual-of-64", kind(CONE, 1, &residual(64))),
        canonical(
            "runs-where-array-ties",
            kind(RUNS, 1, &[0, 1, 2, 1000, 1001, 1002]),
        ),
        canonical(
            "array-where-runs-is-cheaper",
            kind(ARRAY, 1, &[0, 1, 2, 1000, 1001, 1002, 1003]),
        ),
        canonical("runs-of-one-interval", kind(RUNS, 1, &interval(0, 99))),
        canonical(
            "cone-where-runs-is-cheaper",
            kind(CONE, 1, &even_runs(21, 6, 10)),
        ),
        canonical("words-of-three-members", kind(WORDS, 1, &[5, 77, 300])),
        canonical(
            "cone-past-1024-members",
            kind(CONE, 1, &even_runs(1_025, 180, 16)),
        ),
        // Step 3: a directory of one chunk at key zero where a small form is no longer.
        canonical("directory-of-a-singleton-at-key-zero", kind(ARRAY, 0, &[7])),
        canonical(
            "stride-directory-where-list8-ties",
            kind(STRIDE, 0, &progression(0, 25, 10)),
        ),
        canonical(
            "cone-directory-where-list16-ties",
            kind(CONE, 0, &jittered(17, 500)),
        ),
        // Step 3: a small image in another form than the selected one.
        canonical("list8-where-window-ties", small_image(List8, &[0, 8])),
        canonical(
            "list8-of-32-where-window-ties",
            small_image(List8, &list8_of_32),
        ),
        canonical("list16-where-list8-fits", small_image(List16, &[7, 200])),
        canonical(
            "list16-where-spans-ties",
            small_image(List16, &[3000, 3001]),
        ),
        canonical(
            "spans-where-window-ties",
            small_image(Spans, &interval(0, 31)),
        ),
        canonical(
            "spans-where-window-is-shorter",
            small_image(Spans, &[0, 1, 2, 10]),
        ),
        canonical(
            "window-where-list8-is-shorter",
            small_image(Window, &[0, 16]),
        ),
        canonical(
            "window-at-start-1-where-window-ties",
            small_image(WindowAt, &thinned(8, 63)),
        ),
        canonical(
            "window-at-where-list16-is-shorter",
            small_image(WindowAt, &[300, 324]),
        ),
        // Step 3: a small image where the directory is strictly shorter.
        canonical(
            "list8-where-stride-directory-is-shorter",
            small_image(List8, &progression(0, 25, 11)),
        ),
        canonical(
            "window-where-stride-directory-is-shorter",
            small_image(Window, &progression(0, 2, 41)),
        ),
        canonical(
            "list16-where-cone-directory-is-shorter",
            small_image(List16, &jittered(18, 500)),
        ),
        // Step 5: an Elias-Fano root where the forest family is not longer.
        (
            "elias-fano-where-the-forest-is-shorter",
            32,
            elias_fano(&one).expect("two members"),
            AdmissionClass::Canonical,
            Entry::Canonical,
        ),
        (
            "elias-fano-of-a-small-set",
            32,
            elias_fano(&two).expect("two members"),
            AdmissionClass::Canonical,
            Entry::Canonical,
        ),
        // Step 6: a u64 child whose root is not the sealed one.
        (
            "u64-child-forest-where-elias-fano-is-shorter",
            64,
            wide_child(unframed(&forest(&ef_shorter)), 10),
            AdmissionClass::Canonical,
            Entry::Verified,
        ),
        (
            "u64-child-elias-fano-where-the-forest-is-shorter",
            64,
            wide_child(unframed(&elias_fano(&one).expect("two members")), 2),
            AdmissionClass::Canonical,
            Entry::Verified,
        ),
        (
            "u64-child-small-in-another-form",
            64,
            wide_child(small_image(List8, &[0, 8]), 2),
            AdmissionClass::Canonical,
            Entry::Verified,
        ),
        (
            "u64-child-directory-where-a-small-form-applies",
            64,
            wide_child(unframed(&kind(ARRAY, 0, &[7])), 1),
            AdmissionClass::Canonical,
            Entry::Verified,
        ),
    ]
}

fn reject(
    name: &str,
    width: u32,
    image: &[u8],
    class: AdmissionClass,
    entry: Entry,
) -> Result<String, String> {
    let (open, verified, canonical) = if width == 32 {
        (
            BitmosaicView::open(image).is_some(),
            BitmosaicView::open_verified(image).map(|_| ()),
            Some(BitmosaicView::open_canonical(image).map(|_| ())),
        )
    } else {
        (
            Bitmosaic64View::open(image).is_some(),
            Bitmosaic64View::open_verified(image).map(|_| ()),
            None,
        )
    };
    let observed =
        format!("open admits: {open}, open_verified: {verified:?}, open_canonical: {canonical:?}");
    // The first entry that refuses, and every stronger one with its class.
    let refused = match (open, &verified, &canonical) {
        (false, Err(error), None | Some(Err(_))) => Some((Entry::Open, error.class)),
        (true, Err(error), None | Some(Err(_))) => Some((Entry::Verified, error.class)),
        (true, Ok(()), Some(Err(error))) => Some((Entry::Canonical, error.class)),
        _ => None,
    };
    let stronger_agree = [verified.err(), canonical.and_then(Result::err)]
        .into_iter()
        .flatten()
        .all(|error| error.class == class);
    if refused != Some((entry, class)) || !stronger_agree {
        return Err(format!(
            "{name}: recorded {} {}; {observed}; image {}",
            entry.name(),
            class.as_str(),
            hex(image)
        ));
    }
    Ok(format!(
        "{{\"name\": \"{name}\", \"width\": {width}, \"class\": \"{}\", \"entry\": \"{}\", \"open_accepts\": {open}, \
         \"image\": \"{}\"}}",
        class.as_str(),
        entry.name(),
        hex(image),
    ))
}

// ── the document ────────────────────────────────────────────────────────

/// Every small form at count 1 and at its largest written count, every
/// container kind alone and all five in one directory, the slot plane
/// present and absent, and an Elias-Fano root.
fn assert_coverage(cases: &[(&'static str, Vec<u32>, Form)]) {
    let slots: Vec<Vec<u8>> = cases
        .iter()
        .map(|(_, members, _)| forest(members))
        .collect();
    for form in Small::ALL {
        let counts: Vec<u8> = slots
            .iter()
            .filter(|slot| Small::of_tag(slot[0]) == Some(form))
            .map(|slot| (slot[0] & 31) + 1)
            .collect();
        // 32 List8 members below 256 always fit a Window of at most 32 bytes.
        let most = if form == Small::List8 { 31 } else { 32 };
        assert!(
            counts.contains(&1) && counts.contains(&most),
            "{}: counts {counts:?}",
            form.name()
        );
    }
    for kind in [WORDS, STRIDE, CONE, ARRAY, RUNS] {
        assert!(
            cases
                .iter()
                .any(|(_, _, form)| *form == Form::Directory(vec![kind])),
            "{} alone",
            KIND_NAMES[usize::from(kind)]
        );
    }
    assert!(cases.iter().any(|(_, _, form)| matches!(form,
        Form::Directory(kinds) if [WORDS, STRIDE, CONE, ARRAY, RUNS].iter().all(|kind| kinds.contains(kind)))));
    let planes: Vec<bool> = slots
        .iter()
        .filter(|slot| slot[0] == CONTROL_DIRECTORY)
        .map(|slot| {
            let planes = Planes::of(slot, true);
            planes.chunks > 64 && planes.payload_at != planes.slots_at
        })
        .collect();
    assert!(planes.contains(&true), "a slot plane");
    assert!(
        cases
            .iter()
            .any(|(_, members, _)| form_of(&sealed(members), true) == Form::EliasFano)
    );
}

pub fn document() -> String {
    let cases = u32_cases();
    assert_coverage(&cases);
    let u32_lines: Vec<String> = cases
        .iter()
        .map(|(name, members, form)| u32_case(name, members, form))
        .collect();
    let u64_lines: Vec<String> = u64_cases()
        .iter()
        .map(|(name, members, children)| u64_case(name, members, children))
        .collect();
    let (reject_lines, mismatches): (Vec<_>, Vec<_>) = structural_rejects()
        .into_iter()
        .chain(canonical_rejects())
        .map(|(name, width, image, class, entry)| reject(name, width, &image, class, entry))
        .partition(Result::is_ok);
    let mismatches: Vec<String> = mismatches.into_iter().filter_map(Result::err).collect();
    assert!(
        mismatches.is_empty(),
        "rejects the crate treats otherwise:\n{}",
        mismatches.join("\n")
    );
    let reject_lines: Vec<String> = reject_lines.into_iter().filter_map(Result::ok).collect();
    format!(
        "{{\n\"format\": \"bitmosaic native v2\",\n\"u32\": [\n{}\n],\n\"u64\": [\n{}\n],\n\"rejects\": [\n{}\n]\n}}\n",
        u32_lines.join(",\n"),
        u64_lines.join(",\n"),
        reject_lines.join(",\n"),
    )
}
